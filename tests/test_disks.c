/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for disk class detection (/sys), durable checksummed DISK metadata, disk_init/disk_load/disk_bind,
 * `tsdb disk init|show` CLI, and crashfs power-loss durability (docs/design-cluster-data.md §15.1, D11).
 *
 *   ./build/test_disks [scratch dir]
 *   ./build/test_disks --mutant rotationalflip|bindtwice|disknosync [scratch dir]
 *   ./build/test_disks --child init-hdd|init-nvme|bind DIR ACKFILE [--mutant NAME]
 */
#define _GNU_SOURCE
#include "disk.h"
#include "chunk.h"
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#ifndef __APPLE__
#include <sys/sysmacros.h>
#endif
#include <sys/wait.h>
#include <unistd.h>

static int failures;
static int saved_err = -1;
static int cap_rd = -1;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32], CLI[PATH_MAX + 32], FAKESYS[PATH_MAX + 64];
static const char *mutant_name = NULL;

static void fail_at(const char *file, int line, const char *what)
{
    char buf[512];
    int n = snprintf(buf, sizeof buf, "%s:%d: CHECK failed: %s\n", file, line, what);
    if (write(saved_err >= 0 ? saved_err : 2, buf, (size_t)n) < 0)
        failures++;
    failures++;
}

#define CHECK(cond)                             \
    do {                                        \
        if (!(cond))                            \
            fail_at(__FILE__, __LINE__, #cond); \
    } while (0)

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void cap_begin(void)
{
    fflush(stderr);
    int pf[2];
    saved_err = dup(2);
    if (pipe(pf) == 0) {
        fcntl(pf[0], F_SETFL, O_NONBLOCK);
        fcntl(pf[0], F_SETFD, FD_CLOEXEC);
        fcntl(pf[1], F_SETFD, FD_CLOEXEC);
        cap_rd = pf[0];
        dup2(pf[1], 2);
        close(pf[1]);
    }
}

static void cap_end(char *out, size_t cap)
{
    fflush(stderr);
    if (saved_err >= 0) {
        dup2(saved_err, 2);
        close(saved_err);
        saved_err = -1;
    }
    size_t n = 0;
    if (cap_rd >= 0) {
        char junk[4096];
        bool room = out != NULL && cap > 0;
        for (;;) {
            ssize_t r = read(cap_rd, room ? out + n : junk, room ? cap - 1 - n : sizeof junk);
            if (r <= 0)
                break;
            if (room) {
                n += (size_t)r;
                room = (n + 1 < cap);
            }
        }
        close(cap_rd);
        cap_rd = -1;
    }
    if (out && cap > 0)
        out[n] = '\0';
}

static void fresh_dir(const char *name, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%s", S, name);
    CHECK(sh("rm -rf '%s' && mkdir -p '%s'", out, out) == 0);
}

static bool path_exists(const char *dir, const char *rel)
{
    char p[PATH_MAX + 128];
    snprintf(p, sizeof p, "%s/%s", dir, rel);
    return access(p, F_OK) == 0;
}

static void resolve_fakesys(const char *bindir)
{
    const char *env = getenv("TSDB_FAKE_SYS");
    if (env && *env && access(env, R_OK) == 0) {
        snprintf(FAKESYS, sizeof FAKESYS, "%s", env);
        return;
    }
    if (access("tests/data/fake-sys/dev/block", R_OK) == 0) {
        snprintf(FAKESYS, sizeof FAKESYS, "tests/data/fake-sys");
        return;
    }
    char cand[PATH_MAX + 64];
    snprintf(cand, sizeof cand, "%s/../tests/data/fake-sys", bindir);
    if (access(cand, R_OK) == 0) {
        snprintf(FAKESYS, sizeof FAKESYS, "%s", cand);
        return;
    }
    snprintf(FAKESYS, sizeof FAKESYS, "tests/data/fake-sys");
}

static int apply_mutant(const char *name)
{
#ifdef TSDB_TEST_HOOKS
    if (!name)
        return 0;
    if (strcmp(name, "rotationalflip") == 0) {
        disk_test_mutant = DISK_MUT_ROTATIONAL_FLIP;
        return 0;
    }
    if (strcmp(name, "bindtwice") == 0) {
        disk_test_mutant = DISK_MUT_BIND_TWICE;
        return 0;
    }
    if (strcmp(name, "disknosync") == 0) {
        disk_test_mutant = DISK_MUT_NOSYNC;
        return 0;
    }
#else
    (void)name;
#endif
    return -1;
}

/* ---------------------------------------------------------------- 1. /sys detection */

static void test_detect(void)
{
    const char *fakesys = FAKESYS;
    char why[512];
    int cls = DISK_CLASS_NONE;

    /* Accepted cases on fake-sys */
    CHECK(disk_detect_dev(259, 0, "/nvme0", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);
    CHECK(disk_detect_dev(259, 1, "/nvme0p1", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);
    CHECK(disk_detect_dev(8, 0, "/sda", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_SSD);
    CHECK(disk_detect_dev(8, 1, "/sda1", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_SSD);
    CHECK(disk_detect_dev(8, 16, "/sdb", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_HDD);
    CHECK(disk_detect_dev(8, 32, "/sdc", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_HDD);
    /* dm-crypt over NVMe partition */
    CHECK(disk_detect_dev(252, 0, "/dm0", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);
    /* md RAID1 over two HDDs */
    CHECK(disk_detect_dev(9, 0, "/md0", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_HDD);

    /* Native NVMe multipath (nvme_core.multipath=Y):
     * 259:20 / 259:21: subsys3 (collides with TCP ctrl nvme3), backed by PCIe nvme0 via multipath/ -> nvme */
    CHECK(disk_detect_dev(259, 20, "/nvme-mp-local", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);
    CHECK(disk_detect_dev(259, 21, "/nvme-mp-local-p1", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);
    /* 259:23: 5.x style subsys4 (no multipath/ subdir), controller link nvme0 (pcie) -> nvme */
    CHECK(disk_detect_dev(259, 23, "/nvme-mp-5x-local", fakesys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);

    /* Native NVMe multipath refused:
     * 259:22: subsys0 (collides with PCIe ctrl nvme0!), multipath/ has pcie nvme0 + tcp nvme1 -> REFUSED */
    why[0] = '\0';
    CHECK(disk_detect_dev(259, 22, "/nvme-mp-mixed", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);
    CHECK(strstr(why, "tcp") != NULL);

    /* 259:24: 5.x style subsys5 -> rdma nvme2 -> REFUSED */
    why[0] = '\0';
    CHECK(disk_detect_dev(259, 24, "/nvme-mp-rdma", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);
    CHECK(strstr(why, "rdma") != NULL);

    /* 259:25: subsys6 with zero controllers -> fail closed */
    why[0] = '\0';
    CHECK(disk_detect_dev(259, 25, "/nvme-mp-empty", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);

    /* Refused: dm-cache over mixed NVMe + HDD */
    why[0] = '\0';
    CHECK(disk_detect_dev(252, 1, "/dm1", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "devices of different kinds") != NULL);
    CHECK(strstr(why, "add :nvme, :ssd or :hdd") != NULL);

    /* Refused: loop device */
    why[0] = '\0';
    CHECK(disk_detect_dev(7, 0, "/loop0", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);
    CHECK(strstr(why, "add :nvme, :ssd or :hdd") != NULL);

    /* Refused: NVMe over TCP */
    why[0] = '\0';
    CHECK(disk_detect_dev(259, 10, "/nvme-tcp", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);
    CHECK(strstr(why, "tcp") != NULL);

    /* Refused: NVMe over RDMA */
    why[0] = '\0';
    CHECK(disk_detect_dev(259, 11, "/nvme-rdma", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);
    CHECK(strstr(why, "rdma") != NULL);

    /* Refused: btrfs / tmpfs / NFS / ZFS / overlay anonymous device number (major 0) */
    why[0] = '\0';
    CHECK(disk_detect_dev(0, 42, "/btrfs", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);
    CHECK(strstr(why, "add :nvme, :ssd or :hdd") != NULL);

    /* Refused: missing device */
    why[0] = '\0';
    CHECK(disk_detect_dev(8, 99, "/missing", fakesys, &cls, why, sizeof why) != 0);
    CHECK(strstr(why, "no local block device") != NULL);

    /* Refused: missing path */
    char nope[PATH_MAX + 32];
    snprintf(nope, sizeof nope, "%s/does-not-exist", S);
    CHECK(disk_detect(nope, fakesys, &cls, why, sizeof why) == TSDB_ERR_NOTFOUND);

    /* Exercise disk_detect(path, ...) and disk_init2(path, 0, ...) auto-detect end-to-end */
    char probe_dir[PATH_MAX + 32];
    fresh_dir("detect-real", probe_dir, sizeof probe_dir);
    struct stat st;
    CHECK(stat(probe_dir, &st) == 0);
    unsigned maj = (unsigned)major(st.st_dev), min = (unsigned)minor(st.st_dev);
    if (maj == 0) {
        /* Anonymous FS (e.g. tmpfs): disk_detect and disk_init2(class=0) must refuse and remove newly made dir */
        CHECK(disk_detect(probe_dir, fakesys, &cls, why, sizeof why) != 0);
        CHECK(strstr(why, "no local block device") != NULL);
        char autod[PATH_MAX + 32];
        snprintf(autod, sizeof autod, "%s/auto-anon-new", S);
        sh("rm -rf '%s'", autod);
        why[0] = '\0';
        CHECK(disk_init2(autod, DISK_CLASS_NONE, fakesys, false, NULL, why, sizeof why) == TSDB_ERR_INVALID);
        CHECK(strstr(why, "no local block device") != NULL);
        CHECK(access(autod, F_OK) != 0); /* newly created dir removed on refusal */
        /* Pre-existing empty dir stays intact on auto-detect refusal */
        CHECK(disk_init2(probe_dir, DISK_CLASS_NONE, fakesys, false, NULL, why, sizeof why) == TSDB_ERR_INVALID);
        CHECK(access(probe_dir, F_OK) == 0 && !path_exists(probe_dir, DISK_META_FILE));
    } else {
        /* Map scratch's real MAJ:MIN into a temporary fake-sys copy */
        char tmpsys[PATH_MAX + 32], linkp[PATH_MAX + 96], absfake[PATH_MAX];
        fresh_dir("tmpsys", tmpsys, sizeof tmpsys);
        CHECK(realpath(fakesys, absfake) != NULL);
        CHECK(sh("mkdir -p '%s/dev/block' && ln -s '%s/class' '%s/class' && ln -s '%s/devices' '%s/devices'",
                 tmpsys, absfake, tmpsys, absfake, tmpsys) == 0);
        snprintf(linkp, sizeof linkp, "%s/dev/block/%u:%u", tmpsys, maj, min);
        CHECK(symlink("../../devices/pci0000:00/0000:00:01.0/nvme/nvme0/nvme0n1", linkp) == 0);
        CHECK(disk_detect(probe_dir, tmpsys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_NVME);

        /* disk_init2 auto-detect accepted (new dir created & initialised as nvme) */
        char auto_ok[PATH_MAX + 32], auto_refuse[PATH_MAX + 32];
        snprintf(auto_ok, sizeof auto_ok, "%s/auto-ok-new", S);
        sh("rm -rf '%s'", auto_ok);
        disk_meta am, al;
        CHECK(disk_init2(auto_ok, DISK_CLASS_NONE, tmpsys, false, &am, why, sizeof why) == 0);
        CHECK(am.disk_class == DISK_CLASS_NVME && disk_load(auto_ok, &al) == 0 &&
              al.disk_class == DISK_CLASS_NVME && memcmp(al.store_id, am.store_id, 16) == 0);

        /* Switch mapping to md0 -> hdd */
        unlink(linkp);
        CHECK(symlink("../../devices/virtual/block/md0", linkp) == 0);
        CHECK(disk_detect(probe_dir, tmpsys, &cls, why, sizeof why) == 0 && cls == DISK_CLASS_HDD);

        /* Switch mapping to mixed dm-1 -> disk_init2(class=0) refuses and removes newly created dir */
        unlink(linkp);
        CHECK(symlink("../../devices/virtual/block/dm-1", linkp) == 0);
        snprintf(auto_refuse, sizeof auto_refuse, "%s/auto-refuse-new", S);
        sh("rm -rf '%s'", auto_refuse);
        why[0] = '\0';
        CHECK(disk_init2(auto_refuse, DISK_CLASS_NONE, tmpsys, false, NULL, why, sizeof why) == TSDB_ERR_INVALID);
        CHECK(strstr(why, "devices of different kinds") != NULL);
        CHECK(access(auto_refuse, F_OK) != 0);
        /* Pre-existing empty dir stays intact on auto-detect refusal */
        CHECK(disk_init2(probe_dir, DISK_CLASS_NONE, tmpsys, false, NULL, why, sizeof why) == TSDB_ERR_INVALID);
        CHECK(access(probe_dir, F_OK) == 0 && !path_exists(probe_dir, DISK_META_FILE));
    }

    printf("  class detection on fake-sys: NVMe (incl. native multipath), SSD, HDD, partitions, dm-crypt, md RAID1 "
           "accepted; mixed multipath/dm-cache, loop, NVMe-TCP/RDMA, anonymous/btrfs, missing refused\n");
}

/* ---------------------------------------------------------------- 2. init / load / bind */

static void test_init_load_bind(void)
{
    char d1[PATH_MAX + 32], d2[PATH_MAX + 32], d3[PATH_MAX + 32], d4[PATH_MAX + 32], log[4096], why[512];
    disk_meta m, loaded;

    /* NVMe init (real 100 fdatasyncs) */
    fresh_dir("init-nvme", d1, sizeof d1);
    CHECK(disk_init(d1, DISK_CLASS_NVME, NULL, &m) == 0);
    CHECK(m.version == DISK_META_VERSION);
    CHECK(m.disk_class == DISK_CLASS_NVME);
    CHECK(!m.bound && m.node_id == 0);
    CHECK(!path_exists(d1, "chunks"));
    CHECK(disk_load(d1, &loaded) == 0);
    CHECK(loaded.version == m.version && loaded.disk_class == m.disk_class &&
          loaded.sync_p50_us == m.sync_p50_us && !loaded.bound && loaded.node_id == 0 &&
          memcmp(loaded.store_id, m.store_id, 16) == 0 && memcmp(loaded.cluster_uuid, m.cluster_uuid, 16) == 0);

    /* Refused: second init on already-initialized directory */
    cap_begin();
    CHECK(disk_init(d1, DISK_CLASS_NVME, NULL, NULL) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "already initialised") != NULL);
    /* Original DISK unchanged */
    disk_meta after;
    CHECK(disk_load(d1, &after) == 0 && memcmp(after.store_id, m.store_id, 16) == 0);

    /* HDD init: creates <path>/chunks/ with same store_id under TSDB_EC */
    fresh_dir("init-hdd", d2, sizeof d2);
    CHECK(disk_init(d2, DISK_CLASS_HDD, NULL, &m) == 0);
    CHECK(m.disk_class == DISK_CLASS_HDD);
#ifdef TSDB_EC
    CHECK(path_exists(d2, "chunks/META"));
    {
        char cpath[PATH_MAX + 64];
        snprintf(cpath, sizeof cpath, "%s/chunks", d2);
        chunk_store *st = NULL;
        CHECK(chunk_store_open(cpath, m.store_id, &st) == 0 && st != NULL);
        if (st) {
            CHECK(memcmp(chunk_store_id(st), m.store_id, 16) == 0);
            chunk_store_close(st);
        }
    }
#endif

    /* Fast disk explicitly asked for chunks (disk_init2 want_chunks=true) */
    fresh_dir("init-ssd-chunks", d3, sizeof d3);
    why[0] = '\0';
#ifdef TSDB_EC
    CHECK(disk_init2(d3, DISK_CLASS_SSD, NULL, true, &m, why, sizeof why) == 0);
    CHECK(m.disk_class == DISK_CLASS_SSD);
    CHECK(path_exists(d3, "chunks/META"));
    {
        char cpath[PATH_MAX + 64];
        snprintf(cpath, sizeof cpath, "%s/chunks", d3);
        chunk_store *st = NULL;
        CHECK(chunk_store_open(cpath, m.store_id, &st) == 0 && st != NULL);
        if (st)
            chunk_store_close(st);
    }
#else
    /* NO_EC=1 build MUST refuse --chunks explicitly with a clear diagnostic */
    CHECK(disk_init2(d3, DISK_CLASS_SSD, NULL, true, &m, why, sizeof why) == TSDB_ERR_INVALID);
    CHECK(strstr(why, "not built with the erasure codec") != NULL);
    CHECK(!path_exists(d3, DISK_META_FILE));
#endif

    /* Refused: non-empty directory */
    fresh_dir("nonempty", d4, sizeof d4);
    CHECK(sh("touch '%s/stray.txt'", d4) == 0);
    cap_begin();
    CHECK(disk_init(d4, DISK_CLASS_SSD, NULL, NULL) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not empty") != NULL);
    CHECK(!path_exists(d4, DISK_META_FILE));

    /* Interrupted disk_init leftovers (DISK.probe.tmp, DISK.tmp, unbound chunks/META without DISK) are cleaned on retry;
     * real chunk data (chunks/s1) OR a bound chunks/META without DISK is STILL refused! */
    char dleft[PATH_MAX + 32], drealchunks[PATH_MAX + 32], dboundchunks[PATH_MAX + 32];
    fresh_dir("leftovers-ok", dleft, sizeof dleft);
    CHECK(sh("touch '%s/DISK.probe.tmp' '%s/DISK.tmp' && mkdir '%s/chunks' && touch '%s/chunks/META' '%s/chunks/META.tmp'",
             dleft, dleft, dleft, dleft, dleft) == 0);
    CHECK(disk_init(dleft, DISK_CLASS_HDD, NULL, &m) == 0);
    CHECK(disk_load(dleft, &loaded) == 0 && memcmp(loaded.store_id, m.store_id, 16) == 0);
    CHECK(!path_exists(dleft, "DISK.probe.tmp") && !path_exists(dleft, "DISK.tmp"));

    fresh_dir("leftovers-realchunks", drealchunks, sizeof drealchunks);
    CHECK(sh("mkdir -p '%s/chunks/s1' && touch '%s/chunks/META'", drealchunks, drealchunks) == 0);
    cap_begin();
    CHECK(disk_init(drealchunks, DISK_CLASS_HDD, NULL, NULL) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not empty") != NULL);
    CHECK(path_exists(drealchunks, "chunks/s1") && !path_exists(drealchunks, DISK_META_FILE));

    /* Bound empty chunk store (chunks/META with cluster=/node= owner, no volumes) MUST NOT be treated as leftover! */
    fresh_dir("leftovers-boundchunks", dboundchunks, sizeof dboundchunks);
    {
        char cpath[PATH_MAX + 64];
        snprintf(cpath, sizeof cpath, "%s/chunks", dboundchunks);
        chunk_store_owner ow = {.set = true, .node = 9};
        for (int i = 0; i < 16; i++)
            ow.cluster[i] = (uint8_t)(0x30 + i);
        uint8_t orig_sid[16];
        CHECK(chunk_store_init2(cpath, NULL, &ow, orig_sid) == 0);
        why[0] = '\0';
        CHECK(disk_init2(dboundchunks, DISK_CLASS_HDD, NULL, false, NULL, why, sizeof why) == TSDB_ERR_INVALID);
        CHECK(strstr(why, "bound chunk store") != NULL);
        CHECK(!path_exists(dboundchunks, DISK_META_FILE));
        /* Original bound chunk store is completely intact */
        chunk_store *st = NULL;
        CHECK(chunk_store_open(cpath, orig_sid, &st) == 0 && st != NULL);
        if (st) {
            chunk_store_owner got = {0};
            chunk_store_get_owner(st, &got);
            CHECK(got.set && got.node == 9 && memcmp(got.cluster, ow.cluster, 16) == 0);
            chunk_store_close(st);
        }
    }

    /* Non-blocking flock: busy disk directory returns TSDB_ERR_LOCKED immediately instead of hanging,
     * and MUST NOT unlink the lock holder's DISK.tmp / DISK.probe.tmp! */
    {
        char dbusy[PATH_MAX + 32];
        fresh_dir("busy-lock", dbusy, sizeof dbusy);
        int lfd = open(dbusy, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        CHECK(lfd >= 0 && flock(lfd, LOCK_EX | LOCK_NB) == 0);
        CHECK(sh("touch '%s/DISK.tmp' '%s/DISK.probe.tmp'", dbusy, dbusy) == 0);
        why[0] = '\0';
        CHECK(disk_init2(dbusy, DISK_CLASS_NVME, NULL, false, NULL, why, sizeof why) == TSDB_ERR_LOCKED);
        CHECK(strstr(why, "busy") != NULL);
        CHECK(path_exists(dbusy, "DISK.tmp") && path_exists(dbusy, "DISK.probe.tmp"));
        close(lfd);
        CHECK(disk_init(dbusy, DISK_CLASS_NVME, NULL, &m) == 0);

        /* Hold lock again during disk_bind */
        lfd = open(dbusy, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        CHECK(lfd >= 0 && flock(lfd, LOCK_EX | LOCK_NB) == 0);
        uint8_t bcl[16];
        for (int i = 0; i < 16; i++)
            bcl[i] = (uint8_t)(0x40 + i);
        cap_begin();
        CHECK(disk_bind(dbusy, bcl, 5) == TSDB_ERR_LOCKED);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "busy") != NULL);
        close(lfd);
        CHECK(disk_bind(dbusy, bcl, 5) == 0);
    }

    /* Refused: symlinked target path (with and without trailing slash) */
    char real_t[PATH_MAX + 32], sym_t[PATH_MAX + 32], sym_slash[PATH_MAX + 36];
    fresh_dir("sym-real", real_t, sizeof real_t);
    snprintf(sym_t, sizeof sym_t, "%s/sym-disk", S);
    unlink(sym_t);
    CHECK(symlink(real_t, sym_t) == 0);
    snprintf(sym_slash, sizeof sym_slash, "%s/", sym_t);
    cap_begin();
    CHECK(disk_init(sym_t, DISK_CLASS_NVME, NULL, NULL) != 0);
    CHECK(disk_init(sym_slash, DISK_CLASS_NVME, NULL, NULL) != 0);
    cap_end(NULL, 0);
    CHECK(!path_exists(real_t, DISK_META_FILE));

    /* disk_bind: write-once semantics */
    uint8_t cl1[16], cl2[16], zero16[16];
    memset(zero16, 0, sizeof zero16);
    for (int i = 0; i < 16; i++) {
        cl1[i] = (uint8_t)(0x20 + i);
        cl2[i] = (uint8_t)(0x90 + i);
    }
    /* Invalid bind args refused */
    CHECK(disk_bind(d1, zero16, 1) == TSDB_ERR_INVALID);
    CHECK(disk_bind(d1, cl1, 0) == TSDB_ERR_INVALID);
    /* First bind succeeds */
    CHECK(disk_bind(d1, cl1, 7) == 0);
    CHECK(disk_load(d1, &loaded) == 0 && loaded.bound && loaded.node_id == 7 &&
          memcmp(loaded.cluster_uuid, cl1, 16) == 0);
    /* Re-bind to SAME identity is idempotent */
    CHECK(disk_bind(d1, cl1, 7) == 0);
    /* Re-bind to DIFFERENT cluster refused */
    cap_begin();
    CHECK(disk_bind(d1, cl2, 7) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, d1) != NULL);
    /* Re-bind to DIFFERENT node refused */
    cap_begin();
    CHECK(disk_bind(d1, cl1, 8) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    /* Still holds original binding */
    CHECK(disk_load(d1, &loaded) == 0 && loaded.bound && loaded.node_id == 7 &&
          memcmp(loaded.cluster_uuid, cl1, 16) == 0);

    /* disk_rebind: explicit adoption rewrite OLD -> NEW (same cluster only) */
    {
        char dunb[PATH_MAX + 32];
        fresh_dir("rebind-unbound", dunb, sizeof dunb);
        CHECK(disk_init(dunb, DISK_CLASS_NVME, NULL, &m) == 0);
        cap_begin();
        CHECK(disk_rebind(dunb, cl1, 7, 19) == TSDB_ERR_INVALID); /* unbound refused */
        CHECK(disk_rebind(d1, zero16, 7, 19) == TSDB_ERR_INVALID);
        CHECK(disk_rebind(d1, cl1, 0, 19) == TSDB_ERR_INVALID);
        CHECK(disk_rebind(d1, cl1, 7, 0) == TSDB_ERR_INVALID);
        CHECK(disk_rebind(d1, cl1, 7, 7) == TSDB_ERR_INVALID);
        CHECK(disk_rebind(d1, cl2, 7, 19) == TSDB_ERR_INVALID);  /* foreign cluster refused */
        CHECK(disk_rebind(d1, cl1, 99, 19) == TSDB_ERR_INVALID); /* wrong old_node refused */
        cap_end(NULL, 0);
        /* Busy lock returns TSDB_ERR_LOCKED */
        int lfd = open(d1, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        CHECK(lfd >= 0 && flock(lfd, LOCK_EX | LOCK_NB) == 0);
        cap_begin();
        CHECK(disk_rebind(d1, cl1, 7, 19) == TSDB_ERR_LOCKED);
        cap_end(NULL, 0);
        close(lfd);
        /* Valid rebind 7 -> 19 preserves store_id, class, sync_p50_us, cluster_uuid */
        disk_meta before_rb;
        CHECK(disk_load(d1, &before_rb) == 0);
        CHECK(disk_rebind(d1, cl1, 7, 19) == 0);
        CHECK(disk_load(d1, &loaded) == 0 && loaded.bound && loaded.node_id == 19 &&
              memcmp(loaded.cluster_uuid, cl1, 16) == 0 &&
              memcmp(loaded.store_id, before_rb.store_id, 16) == 0 &&
              loaded.disk_class == before_rb.disk_class && loaded.sync_p50_us == before_rb.sync_p50_us);
        /* Idempotent retry 7 -> 19 succeeds */
        CHECK(disk_rebind(d1, cl1, 7, 19) == 0);
        /* Subsequent rebind from stale old node 7 -> 25 is refused */
        cap_begin();
        CHECK(disk_rebind(d1, cl1, 7, 25) == TSDB_ERR_INVALID);
        cap_end(NULL, 0);
    }

    /* Corrupt DISK detection: bit flip, oversize (73 bytes), truncation -> TSDB_ERR_CORRUPT naming path */
    char dcor[PATH_MAX + 32], fpath[PATH_MAX + 64];
    fresh_dir("corrupt", dcor, sizeof dcor);
    CHECK(disk_load(dcor, &loaded) == TSDB_ERR_NOTFOUND);
    CHECK(disk_init(dcor, DISK_CLASS_NVME, NULL, &m) == 0);
    snprintf(fpath, sizeof fpath, "%s/%s", dcor, DISK_META_FILE);
    /* Oversize file (73 bytes: valid 72-byte header + 1 trailing byte) exercises got == sizeof buf */
    int fd = open(fpath, O_RDWR | O_APPEND);
    CHECK(fd >= 0 && write(fd, "X", 1) == 1);
    if (fd >= 0)
        close(fd);
    cap_begin();
    CHECK(disk_load(dcor, &loaded) == TSDB_ERR_CORRUPT);
    cap_end(log, sizeof log);
    CHECK(strstr(log, dcor) != NULL);
    /* Back to exact size, flip a byte */
    CHECK(truncate(fpath, DISK_META_BYTES) == 0);
    CHECK(disk_load(dcor, &loaded) == 0);
    fd = open(fpath, O_RDWR);
    CHECK(fd >= 0);
    uint8_t b = 0;
    CHECK(pread(fd, &b, 1, 20) == 1);
    b ^= 0x01;
    CHECK(pwrite(fd, &b, 1, 20) == 1);
    close(fd);
    cap_begin();
    CHECK(disk_load(dcor, &loaded) == TSDB_ERR_CORRUPT);
    cap_end(log, sizeof log);
    CHECK(strstr(log, dcor) != NULL);
    /* Truncated file */
    CHECK(truncate(fpath, 20) == 0);
    cap_begin();
    CHECK(disk_load(dcor, &loaded) == TSDB_ERR_CORRUPT);
    cap_end(log, sizeof log);
    CHECK(strstr(log, dcor) != NULL);

#ifdef TSDB_TEST_HOOKS
    /* Fast-class high latency warning (> 2000 us warns on nvme/ssd, not on hdd) */
    char dw1[PATH_MAX + 32], dw2[PATH_MAX + 32];
    fresh_dir("warn-nvme", dw1, sizeof dw1);
    fresh_dir("warn-hdd", dw2, sizeof dw2);
    disk_test_sync_us = 3500;
    cap_begin();
    CHECK(disk_init(dw1, DISK_CLASS_NVME, NULL, &m) == 0 && m.sync_p50_us == 3500);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "warning:") != NULL && strstr(log, "3500") != NULL);

    cap_begin();
    CHECK(disk_init(dw2, DISK_CLASS_HDD, NULL, &m) == 0 && m.sync_p50_us == 3500);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "warning:") == NULL);
    disk_test_sync_us = 0;
#endif

    printf("  disk_init, disk_load, disk_bind, leftover recovery, corruption detection, chunks/ and sync warning ok\n");
}

/* ---------------------------------------------------------------- 3. CLI */

static void test_cli(void)
{
    if (access(CLI, X_OK) != 0) {
        printf("  CLI test: skipped (no %s)\n", CLI);
        return;
    }
    char d1[PATH_MAX + 32], outf[PATH_MAX + 32], buf[4096];
    snprintf(d1, sizeof d1, "%s/cli-disk1", S);
    snprintf(outf, sizeof outf, "%s/cli.out", S);
    sh("rm -rf '%s'", d1);

    /* tsdb disk init PATH:nvme (creates dir) */
    CHECK(sh("'%s' disk init '%s:nvme' >'%s' 2>&1", CLI, d1, outf) == 0);
    FILE *f = fopen(outf, "r");
    size_t n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    if (f)
        fclose(f);
    buf[n] = '\0';
    CHECK(strstr(buf, "class nvme") != NULL && strstr(buf, "sync_p50") != NULL);

    /* tsdb disk show PATH */
    CHECK(sh("'%s' disk show '%s' >'%s' 2>&1", CLI, d1, outf) == 0);
    f = fopen(outf, "r");
    n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    if (f)
        fclose(f);
    buf[n] = '\0';
    CHECK(strstr(buf, "class: nvme") != NULL && strstr(buf, "cluster: (unbound)") != NULL &&
          strstr(buf, "node: (unbound)") != NULL);

    /* Bind and re-show */
    uint8_t cl[16];
    for (int i = 0; i < 16; i++)
        cl[i] = (uint8_t)(0x50 + i);
    CHECK(disk_bind(d1, cl, 19) == 0);
    CHECK(sh("'%s' disk show '%s' >'%s' 2>&1", CLI, d1, outf) == 0);
    f = fopen(outf, "r");
    n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    if (f)
        fclose(f);
    buf[n] = '\0';
    CHECK(strstr(buf, "cluster: 505152535455565758595a5b5c5d5e5f") != NULL && strstr(buf, "node: 19") != NULL);

    /* --chunks CLI behavior */
    char dc[PATH_MAX + 32];
    snprintf(dc, sizeof dc, "%s/cli-chunks", S);
    sh("rm -rf '%s'", dc);
#ifdef TSDB_EC
    CHECK(sh("'%s' disk init '%s:ssd' --chunks >'%s' 2>&1", CLI, dc, outf) == 0);
    CHECK(path_exists(dc, "chunks/META"));
#else
    CHECK(sh("'%s' disk init '%s:ssd' --chunks >'%s' 2>&1", CLI, dc, outf) != 0);
    f = fopen(outf, "r");
    n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    if (f)
        fclose(f);
    buf[n] = '\0';
    CHECK(strstr(buf, "not built with the erasure codec") != NULL);
    CHECK(access(dc, F_OK) != 0);
#endif

    /* Refusals: second init, bad class, show missing */
    CHECK(sh("'%s' disk init '%s:nvme' >/dev/null 2>&1", CLI, d1) != 0);
    CHECK(sh("'%s' disk init '%s/bad:bogus' >/dev/null 2>&1", CLI, S) != 0);
    CHECK(sh("'%s' disk show '%s/no-such-disk' >/dev/null 2>&1", CLI, S) != 0);

    printf("  tsdb disk init and tsdb disk show CLI ok\n");
}

/* ---------------------------------------------------------------- 4. crashfs power loss */

static void write_ack(const char *ackfile, const char *text)
{
    int fd = open(ackfile, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd >= 0) {
        if (write(fd, text, strlen(text)) < 0)
            _exit(2);
        close(fd);
    }
}

static bool read_ack(const char *ackfile, char *buf, size_t cap)
{
    FILE *f = fopen(ackfile, "r");
    if (!f)
        return false;
    size_t n = fread(buf, 1, cap - 1, f);
    fclose(f);
    buf[n] = '\0';
    return n > 0;
}

static int run_child_ex(const char *mode, const char *root_dir, const char *target_dir, const char *ackfile,
                        const char *prob, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        const char *prev = getenv("ASAN_OPTIONS");
        snprintf(asan, sizeof asan, "%s%sverify_asan_link_order=0:detect_leaks=0",
                 prev && *prev ? prev : "", prev && *prev ? ":" : "");
        setenv("ASAN_OPTIONS", asan, 1);
        char w[16], sd[16];
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", root_dir, 1);
        setenv("CRASHFS_PROB", prob, 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        if (mutant_name)
            execl(SELF, "test_disks", "--child", mode, target_dir, ackfile, "--mutant", mutant_name, (char *)NULL);
        else
            execl(SELF, "test_disks", "--child", mode, target_dir, ackfile, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int run_child(const char *mode, const char *dir, const char *ackfile, int warmup, int seed)
{
    return run_child_ex(mode, dir, dir, ackfile, "1", warmup, seed);
}

static void child_entry(const char *mode, const char *dir, const char *ackfile)
{
#ifdef TSDB_TEST_HOOKS
    /* Use 2 probe syncs in the crashfs sweep child so we still exercise probe write/fdatasync/unlink
     * while stepping through every single filesystem op in milliseconds. */
    disk_test_sync_samples = 2;
#endif
    if (strcmp(mode, "init-hdd") == 0 || strcmp(mode, "init-nvme") == 0) {
        int cls = strcmp(mode, "init-hdd") == 0 ? DISK_CLASS_HDD : DISK_CLASS_NVME;
        disk_meta d;
        if (disk_init(dir, cls, NULL, &d) != 0)
            _exit(4);
        char hex[33];
        chunk_oid_hex(d.store_id, hex);
        write_ack(ackfile, hex);
        exit(0); /* exit() (not _exit): CRASHFS_AT_EXIT rolls back anything not synced! */
    }
    if (strcmp(mode, "bind") == 0) {
        uint8_t cl[16];
        for (int i = 0; i < 16; i++)
            cl[i] = (uint8_t)(0x70 + i);
        if (disk_bind(dir, cl, 42) != 0)
            _exit(4);
        write_ack(ackfile, "bound");
        exit(0);
    }
    if (strcmp(mode, "bind-idemp") == 0) {
        /* Simulate a prior process that renamed bound DISK into place and died BEFORE fsync(dirfd):
         * write bound DISK via tmp -> fsync -> renameat with NO dir fsync, then invoke idempotent
         * disk_bind(dir, cl, 42) and exit(0) so CRASHFS_AT_EXIT tests whether disk_bind synced it! */
        uint8_t cl[16];
        for (int i = 0; i < 16; i++)
            cl[i] = (uint8_t)(0x70 + i);
        disk_meta cur;
        if (disk_load(dir, &cur) != 0)
            _exit(4);
        memcpy(cur.cluster_uuid, cl, 16);
        cur.node_id = 42;
        cur.bound = true;
        uint8_t raw[DISK_META_BYTES];
        if (disk_meta_encode(&cur, raw) != 0)
            _exit(4);
        int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0)
            _exit(4);
        int fd = openat(dfd, "DISK.tmp", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0 || write(fd, raw, sizeof raw) != (ssize_t)sizeof raw || tsdb_fsync(fd) != 0 || close(fd) != 0 ||
            renameat(dfd, "DISK.tmp", dfd, DISK_META_FILE) != 0)
            _exit(4);
        close(dfd); /* deliberately NO fsync(dfd): rename is in page cache only */

        /* Now idempotent retry: must fsync DISK and dirfd before returning 0! */
        if (disk_bind(dir, cl, 42) != 0)
            _exit(4);
        write_ack(ackfile, "bound-idemp");
        exit(0);
    }
    if (strcmp(mode, "rebind") == 0) {
        uint8_t cl[16];
        for (int i = 0; i < 16; i++)
            cl[i] = (uint8_t)(0x70 + i);
        if (disk_rebind(dir, cl, 42, 99) != 0)
            _exit(4);
        write_ack(ackfile, "rebound");
        exit(0);
    }
    if (strcmp(mode, "rebind-idemp") == 0) {
        uint8_t cl[16];
        for (int i = 0; i < 16; i++)
            cl[i] = (uint8_t)(0x70 + i);
        disk_meta cur;
        if (disk_load(dir, &cur) != 0)
            _exit(4);
        cur.node_id = 99;
        uint8_t raw[DISK_META_BYTES];
        if (disk_meta_encode(&cur, raw) != 0)
            _exit(4);
        int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0)
            _exit(4);
        int fd = openat(dfd, "DISK.tmp", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0 || write(fd, raw, sizeof raw) != (ssize_t)sizeof raw || tsdb_fsync(fd) != 0 || close(fd) != 0 ||
            renameat(dfd, "DISK.tmp", dfd, DISK_META_FILE) != 0)
            _exit(4);
        close(dfd); /* deliberately NO fsync(dfd): rename is in page cache only */
        if (disk_rebind(dir, cl, 42, 99) != 0)
            _exit(4);
        write_ack(ackfile, "rebound-idemp");
        exit(0);
    }
    _exit(3);
}

static void sweep_init(const char *mode, int want_cls)
{
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32], ackbuf[128], msg[256];
    snprintf(ackf, sizeof ackf, "%s/pl-%s.ack", S, mode);
    int crashes = 0;
    bool finished = false;

    for (int k = 0; k < 300 && !finished; k++) {
        fresh_dir("pl-init", dir, sizeof dir);
        unlink(ackf);
        int code = run_child(mode, dir, ackf, k, k + 1);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += (code == 99);
        finished = (code == 0);

        bool acked = read_ack(ackf, ackbuf, sizeof ackbuf);
        disk_meta d;
        cap_begin();
        int rc = disk_load(dir, &d);
        cap_end(NULL, 0);

        if (acked) {
            if (rc != 0) {
                snprintf(msg, sizeof msg, "%s step %d: acknowledged init lost DISK (rc=%d)", mode, k, rc);
                fail_at(__FILE__, __LINE__, msg);
            } else {
                char hex[33];
                chunk_oid_hex(d.store_id, hex);
                CHECK(strcmp(hex, ackbuf) == 0 && d.disk_class == want_cls && !d.bound);
#ifdef TSDB_EC
                if (want_cls == DISK_CLASS_HDD) {
                    char cpath[PATH_MAX + 64];
                    snprintf(cpath, sizeof cpath, "%s/chunks", dir);
                    chunk_store *st = NULL;
                    CHECK(chunk_store_open(cpath, d.store_id, &st) == 0 && st != NULL);
                    if (st)
                        chunk_store_close(st);
                }
#endif
            }
        } else {
            /* Unacknowledged power loss: DISK must be absent or completely whole (never CORRUPT). */
            if (rc != TSDB_ERR_NOTFOUND && rc != 0) {
                snprintf(msg, sizeof msg, "%s step %d: torn DISK after power loss (rc=%d)", mode, k, rc);
                fail_at(__FILE__, __LINE__, msg);
            } else if (rc == 0) {
                CHECK(d.disk_class == want_cls && !d.bound);
#ifdef TSDB_EC
                if (want_cls == DISK_CLASS_HDD) {
                    char cpath[PATH_MAX + 64];
                    snprintf(cpath, sizeof cpath, "%s/chunks", dir);
                    chunk_store *st = NULL;
                    CHECK(chunk_store_open(cpath, d.store_id, &st) == 0 && st != NULL);
                    if (st)
                        chunk_store_close(st);
                }
#endif
            } else {
                /* DISK absent after mid-init crash: re-running disk_init MUST clean any leftovers and succeed! */
                disk_meta d2, d2l;
#ifdef TSDB_TEST_HOOKS
                disk_test_sync_samples = 2;
#endif
                int rrc = disk_init(dir, want_cls, NULL, &d2);
#ifdef TSDB_TEST_HOOKS
                disk_test_sync_samples = 0;
#endif
                if (rrc != 0) {
                    snprintf(msg, sizeof msg, "%s step %d: retry disk_init after crash failed (rc=%d)", mode, k, rrc);
                    fail_at(__FILE__, __LINE__, msg);
                } else {
                    CHECK(disk_load(dir, &d2l) == 0 && memcmp(d2l.store_id, d2.store_id, 16) == 0 &&
                          d2l.disk_class == want_cls && !d2l.bound);
#ifdef TSDB_EC
                    if (want_cls == DISK_CLASS_HDD) {
                        char cpath[PATH_MAX + 64];
                        snprintf(cpath, sizeof cpath, "%s/chunks", dir);
                        chunk_store *st = NULL;
                        CHECK(chunk_store_open(cpath, d2.store_id, &st) == 0 && st != NULL);
                        if (st)
                            chunk_store_close(st);
                    }
#endif
                }
            }
        }
    }
    CHECK(finished && crashes > 3);
}

static void sweep_bind(void)
{
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32], ackbuf[128], msg[256];
    snprintf(ackf, sizeof ackf, "%s/pl-bind.ack", S);
    uint8_t cl[16];
    for (int i = 0; i < 16; i++)
        cl[i] = (uint8_t)(0x70 + i);

    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 200 && !finished; k++) {
        fresh_dir("pl-bind", dir, sizeof dir);
        disk_meta orig;
#ifdef TSDB_TEST_HOOKS
        disk_test_sync_samples = 2;
#endif
        CHECK(disk_init(dir, DISK_CLASS_NVME, NULL, &orig) == 0);
#ifdef TSDB_TEST_HOOKS
        disk_test_sync_samples = 0;
#endif
        unlink(ackf);

        int code = run_child("bind", dir, ackf, k, k + 101);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += (code == 99);
        finished = (code == 0);

        bool acked = read_ack(ackf, ackbuf, sizeof ackbuf);
        disk_meta d;
        cap_begin();
        int rc = disk_load(dir, &d);
        cap_end(NULL, 0);

        if (rc != 0) {
            snprintf(msg, sizeof msg, "bind step %d: DISK missing or corrupt after power loss (rc=%d)", k, rc);
            fail_at(__FILE__, __LINE__, msg);
            continue;
        }
        CHECK(memcmp(d.store_id, orig.store_id, 16) == 0 && d.disk_class == orig.disk_class &&
              d.sync_p50_us == orig.sync_p50_us);
        if (acked) {
            if (!d.bound || d.node_id != 42 || memcmp(d.cluster_uuid, cl, 16) != 0) {
                snprintf(msg, sizeof msg, "bind step %d: acknowledged bind lost (bound=%d node=%u)", k, d.bound,
                         d.node_id);
                fail_at(__FILE__, __LINE__, msg);
            }
        } else {
            /* Either old unbound state intact or new bound state whole */
            if (d.bound) {
                CHECK(d.node_id == 42 && memcmp(d.cluster_uuid, cl, 16) == 0);
            } else {
                uint8_t z[16] = {0};
                CHECK(d.node_id == 0 && memcmp(d.cluster_uuid, z, 16) == 0);
            }
        }
        /* Re-running disk_bind after any crash must always succeed cleanly */
        CHECK(disk_bind(dir, cl, 42) == 0);
        CHECK(disk_load(dir, &d) == 0 && d.bound && d.node_id == 42 && memcmp(d.cluster_uuid, cl, 16) == 0);
    }
    CHECK(finished && crashes > 2);
}

/* Crashfs sweep where the target directory does NOT exist yet (exercises mkdir + sync_parent_dir). */
static void sweep_init_mkdir(void)
{
    char parent[PATH_MAX + 32], target[PATH_MAX + 64], ackf[PATH_MAX + 32], ackbuf[128], msg[256];
    snprintf(ackf, sizeof ackf, "%s/pl-init-mkdir.ack", S);
    int crashes = 0;
    bool finished = false;

    for (int k = 0; k < 300 && !finished; k++) {
        fresh_dir("pl-parent", parent, sizeof parent);
        snprintf(target, sizeof target, "%s/newdisk", parent);
        unlink(ackf);
        int code = run_child_ex("init-nvme", parent, target, ackf, "1", k, k + 201);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += (code == 99);
        finished = (code == 0);

        bool acked = read_ack(ackf, ackbuf, sizeof ackbuf);
        disk_meta d;
        cap_begin();
        int rc = disk_load(target, &d);
        cap_end(NULL, 0);

        if (acked) {
            if (rc != 0) {
                snprintf(msg, sizeof msg, "init-mkdir step %d: acknowledged init lost newdisk/DISK (rc=%d)", k, rc);
                fail_at(__FILE__, __LINE__, msg);
            } else {
                char hex[33];
                chunk_oid_hex(d.store_id, hex);
                CHECK(strcmp(hex, ackbuf) == 0 && d.disk_class == DISK_CLASS_NVME && !d.bound);
            }
        } else {
            if (rc != TSDB_ERR_NOTFOUND && rc != 0) {
                snprintf(msg, sizeof msg, "init-mkdir step %d: torn DISK after power loss (rc=%d)", k, rc);
                fail_at(__FILE__, __LINE__, msg);
            } else if (rc == TSDB_ERR_NOTFOUND) {
                disk_meta d2;
#ifdef TSDB_TEST_HOOKS
                disk_test_sync_samples = 2;
#endif
                CHECK(disk_init(target, DISK_CLASS_NVME, NULL, &d2) == 0);
#ifdef TSDB_TEST_HOOKS
                disk_test_sync_samples = 0;
#endif
                CHECK(disk_load(target, &d) == 0 && memcmp(d.store_id, d2.store_id, 16) == 0);
            }
        }
    }
    CHECK(finished && crashes > 3);
}

/* Idempotent disk_bind retry after an unsynced rename: must fsync DISK and dirfd before returning 0. */
static void test_bind_idemp_durability(void)
{
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32], ackbuf[64];
    snprintf(ackf, sizeof ackf, "%s/pl-bind-idemp.ack", S);
    fresh_dir("pl-bind-idemp", dir, sizeof dir);
    disk_meta orig;
#ifdef TSDB_TEST_HOOKS
    disk_test_sync_samples = 2;
#endif
    CHECK(disk_init(dir, DISK_CLASS_NVME, NULL, &orig) == 0);
#ifdef TSDB_TEST_HOOKS
    disk_test_sync_samples = 0;
#endif
    unlink(ackf);

    int code = run_child_ex("bind-idemp", dir, dir, ackf, "0", 0, 777);
    CHECK(code == 0);
    CHECK(read_ack(ackf, ackbuf, sizeof ackbuf));

    uint8_t cl[16];
    for (int i = 0; i < 16; i++)
        cl[i] = (uint8_t)(0x70 + i);
    disk_meta d;
    CHECK(disk_load(dir, &d) == 0);
    CHECK(memcmp(d.store_id, orig.store_id, 16) == 0);
    CHECK(d.bound && d.node_id == 42 && memcmp(d.cluster_uuid, cl, 16) == 0);
}

static void sweep_rebind(void)
{
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32], ackbuf[128], msg[256];
    snprintf(ackf, sizeof ackf, "%s/pl-rebind.ack", S);
    uint8_t cl[16];
    for (int i = 0; i < 16; i++)
        cl[i] = (uint8_t)(0x70 + i);

    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 200 && !finished; k++) {
        fresh_dir("pl-rebind", dir, sizeof dir);
        disk_meta orig;
#ifdef TSDB_TEST_HOOKS
        disk_test_sync_samples = 2;
#endif
        CHECK(disk_init(dir, DISK_CLASS_HDD, NULL, &orig) == 0);
#ifdef TSDB_TEST_HOOKS
        disk_test_sync_samples = 0;
#endif
        CHECK(disk_bind(dir, cl, 42) == 0);
        unlink(ackf);

        int code = run_child("rebind", dir, ackf, k, k + 301);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += (code == 99);
        finished = (code == 0);

        bool acked = read_ack(ackf, ackbuf, sizeof ackbuf);
        disk_meta d;
        cap_begin();
        int rc = disk_load(dir, &d);
        cap_end(NULL, 0);

        if (rc != 0) {
            snprintf(msg, sizeof msg, "rebind step %d: DISK missing or corrupt after power loss (rc=%d)", k, rc);
            fail_at(__FILE__, __LINE__, msg);
            continue;
        }
        CHECK(memcmp(d.store_id, orig.store_id, 16) == 0 && d.disk_class == orig.disk_class &&
              d.sync_p50_us == orig.sync_p50_us && d.bound && memcmp(d.cluster_uuid, cl, 16) == 0);
        if (acked) {
            if (d.node_id != 99) {
                snprintf(msg, sizeof msg, "rebind step %d: acknowledged rebind lost (node=%u)", k, d.node_id);
                fail_at(__FILE__, __LINE__, msg);
            }
        } else {
            CHECK(d.node_id == 42 || d.node_id == 99);
        }
        /* Retry after any crash must always leave DISK cleanly rebound to NEW (node 99) */
        CHECK(disk_rebind(dir, cl, 42, 99) == 0);
        CHECK(disk_load(dir, &d) == 0 && d.bound && d.node_id == 99 && memcmp(d.cluster_uuid, cl, 16) == 0);
    }
    CHECK(finished && crashes > 2);
}

static void test_rebind_idemp_durability(void)
{
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32], ackbuf[64];
    snprintf(ackf, sizeof ackf, "%s/pl-rebind-idemp.ack", S);
    fresh_dir("pl-rebind-idemp", dir, sizeof dir);
    disk_meta orig;
#ifdef TSDB_TEST_HOOKS
    disk_test_sync_samples = 2;
#endif
    CHECK(disk_init(dir, DISK_CLASS_NVME, NULL, &orig) == 0);
#ifdef TSDB_TEST_HOOKS
    disk_test_sync_samples = 0;
#endif
    uint8_t cl[16];
    for (int i = 0; i < 16; i++)
        cl[i] = (uint8_t)(0x70 + i);
    CHECK(disk_bind(dir, cl, 42) == 0);
    unlink(ackf);

    int code = run_child_ex("rebind-idemp", dir, dir, ackf, "0", 0, 888);
    CHECK(code == 0);
    CHECK(read_ack(ackf, ackbuf, sizeof ackbuf));

    disk_meta d;
    CHECK(disk_load(dir, &d) == 0);
    CHECK(memcmp(d.store_id, orig.store_id, 16) == 0);
    CHECK(d.bound && d.node_id == 99 && memcmp(d.cluster_uuid, cl, 16) == 0);
}

static void test_power_loss(void)
{
    if (access(SHIM, R_OK) != 0) {
        if (getenv("TSDB_REQUIRE_CRASHFS") && strcmp(getenv("TSDB_REQUIRE_CRASHFS"), "1") == 0) {
            fail_at(__FILE__, __LINE__, "TSDB_REQUIRE_CRASHFS=1 set but libcrashfs.so missing");
            return;
        }
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    sweep_init("init-hdd", DISK_CLASS_HDD);
    sweep_init("init-nvme", DISK_CLASS_NVME);
    sweep_init_mkdir();
    sweep_bind();
    test_bind_idemp_durability();
    sweep_rebind();
    test_rebind_idemp_durability();
    printf("  crashfs power loss (and retry) at every step of disk_init (hdd, nvme, mkdir), disk_bind, "
           "disk_rebind, and idempotent retries ok\n");
}

int main(int argc, char **argv)
{
    if (argc >= 5 && strcmp(argv[1], "--child") == 0) {
        if (argc >= 7 && strcmp(argv[5], "--mutant") == 0) {
            if (apply_mutant(argv[6]) != 0)
                _exit(5);
        }
        child_entry(argv[2], argv[3], argv[4]);
    }

    const char *base = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mutant_name = argv[++i];
            if (apply_mutant(mutant_name) != 0) {
                fprintf(stderr, "test_disks: unknown mutant '%s'\n", mutant_name);
                return 2;
            }
        } else if (argv[i][0] != '-') {
            base = argv[i];
        } else {
            fprintf(stderr, "usage: %s [--mutant rotationalflip|bindtwice|disknosync] [scratch]\n", argv[0]);
            return 2;
        }
    }

    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char dirbuf[PATH_MAX];
    snprintf(dirbuf, sizeof dirbuf, "%s", SELF);
    char *sl = strrchr(dirbuf, '/');
    if (sl)
        *sl = '\0';
    else
        snprintf(dirbuf, sizeof dirbuf, ".");
    snprintf(SHIM, sizeof SHIM, "%s/libcrashfs.so", dirbuf);
    snprintf(CLI, sizeof CLI, "%s/tsdb", dirbuf);
    resolve_fakesys(dirbuf);

    snprintf(S, sizeof S, "%s/test_disks_%ld", base, (long)getpid());
    if (sh("rm -rf '%s' && mkdir -p '%s'", S, S) != 0) {
        fprintf(stderr, "test_disks: cannot make scratch dir %s\n", S);
        return 2;
    }

    printf("test_disks%s%s:\n", mutant_name ? " --mutant " : "", mutant_name ? mutant_name : "");
    test_detect();
    test_init_load_bind();
    test_cli();
    test_power_loss();

    sh("rm -rf '%s'", S);
    if (failures) {
        fprintf(stderr, "test_disks: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_disks: ok\n");
    return 0;
}
