/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of the cluster volume manager (server/cvolfs.h; the rules are in the header of server/cvol.c).
 *
 * A new v<N> is made in <root>/v<N>.tmp: META (the root's format lines, volume=N, volume_uuid=, cluster=,
 * requires=cluster,replica), REPL (replfile_create: both slots hold the record's rid, generation 0 and no nonce, synced,
 * and the directory synced) and FNO, then the directory is synced, renamed to v<N> and the root synced. A crash leaves
 * v<N>.tmp or v<N>, never half of one: nothing in a tmp directory was ever relied on, so the next make removes it.
 *
 * REPL is read and written through src/replfile.c, whose identity fields are filled from what this manager knows:
 * volume_uuid and cluster_uuid from the record, node from the cluster thread's push, incarnation from this server run
 * (a random draw at cvol_start: the thread has no access to NODE's incarnation, and REPL only says which run saved
 * last), rid from the record, open_gen and open_nonce from this module. fence, led, floor, log_start and the history
 * stay as they are: nothing uses them before the replica driver is wired (r3a), so a torn load, whose values are
 * "unsure" (replfile.h), costs nothing here but the skip of a belt in the open generation, which replfile does.
 */
#define _GNU_SOURCE
#include "cvolfs.h"
#include "cvolset.h"
#include "snapinst.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../client/tsdb_crypto.h"
#include "../src/cvol.h"
#include "../src/replfile.h"
#include "tsdb.h"

int cvolfs_test_mutant;

void cvolfs_say(const char *fmt, ...)
{
    char line[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof line, fmt, ap);
    va_end(ap);
    fprintf(stderr, "tsdbd: volumes: %s\n", line);
}
#define say cvolfs_say

static void hex16(const uint8_t *b, char *out)
{
    for (int i = 0; i < 16; i++)
        snprintf(out + 2 * i, 3, "%02x", b[i]);
}

static int sync_dir(const char *d)
{
    int fd = open(d, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int rc = fsync(fd);
    close(fd);
    return rc;
}

/* a new file in `dir`, whole: written, fsynced; the caller syncs the directory */
static int write_new(const char *dir, const char *name, const char *txt)
{
    char p[4300];
    if ((size_t)snprintf(p, sizeof p, "%s/%s", dir, name) >= sizeof p) {
        errno = ENAMETOOLONG; /* never a cut name: that would be another file */
        return -1;
    }
    /* always a new inode: a file a failed write or sync left may have had its pages dropped */
    if (unlink(p) != 0 && errno != ENOENT)
        return -1;
    int fd = open(p, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;
    size_t len = strlen(txt);
    int rc = write(fd, txt, len) == (ssize_t)len && fsync(fd) == 0 ? 0 : -1;
    if (close(fd) != 0)
        rc = -1;
    return rc;
}

/* the root's format lines (version, partition_ms, granule_rows, partition_classes), so every volume has its layout */
static int root_format(const char *root, char *out, size_t cap)
{
    char p[4200], buf[4096];
    if ((size_t)snprintf(p, sizeof p, "%s/META", root) >= sizeof p)
        return -1;
    int fd = open(p, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0)
        return -1;
    buf[n] = 0;
    size_t o = 0;
    static const char *keys[] = {"version=", "partition_ms=", "granule_rows=", "partition_classes="};
    for (char *l = buf; *l;) {
        char *e = strchr(l, '\n');
        size_t ll = e ? (size_t)(e - l) : strlen(l);
        for (size_t k = 0; k < sizeof keys / sizeof keys[0]; k++)
            if (!strncmp(l, keys[k], strlen(keys[k])) && o + ll + 2 < cap) {
                memcpy(out + o, l, ll);
                o += ll;
                out[o++] = '\n';
            }
        l = e ? e + 1 : l + ll;
    }
    out[o] = 0;
    return strstr(out, "version=") ? 0 : -1;
}

static void fill_state(struct replfile_state *st, const struct cvolfs_rec *r)
{
    memset(st, 0, sizeof *st);
    memcpy(st->volume_uuid, r->uuid, 16);
    memcpy(st->cluster_uuid, r->cluster, 16);
    st->node = r->node;
    st->incarnation = r->incarnation;
    st->rid = r->rid;
}

/* makes <root>/v<N> whole (the header); 0 or -1 with a line said */
static int make_dir(const char *root, const struct cvolfs_rec *r, const char *vdir)
{
    char tmp[4300], meta[4096], u[33], c[33];
    snprintf(tmp, sizeof tmp, "%s.tmp", vdir);
    /* a crash may have left a tmp directory: every file in it (META, REPL, FNO and their tmps, whatever step the
     * crash came at), then itself; nothing in it was ever relied on */
    DIR *d = opendir(tmp);
    if (!d && errno != ENOENT) {
        say("volume %" PRIu32 ": cannot read the leftover %s: %s", r->number, tmp, strerror(errno));
        return -1;
    }
    for (struct dirent *e; d && (errno = 0, e = readdir(d));) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;
        char f[4600];
        snprintf(f, sizeof f, "%s/%s", tmp, e->d_name);
        if (unlink(f) != 0 && errno != ENOENT)
            break; /* rmdir names it below */
        errno = 0;
    }
    if (d && errno) { /* a readdir error is an error, never the end of the directory */
        say("volume %" PRIu32 ": cannot read the leftover %s: %s", r->number, tmp, strerror(errno));
        closedir(d);
        return -1;
    }
    if (d)
        closedir(d);
    if (rmdir(tmp) != 0 && errno != ENOENT) {
        say("volume %" PRIu32 ": cannot remove the leftover %s: %s", r->number, tmp, strerror(errno));
        return -1;
    }
    if (root_format(root, meta, sizeof meta - 256) != 0) {
        say("volume %" PRIu32 ": cannot read the format lines of %s/META", r->number, root);
        return -1;
    }
    if (r->rid == 0) {
        say("volume %" PRIu32 ": the record has no replica instance id", r->number);
        return -1;
    }
    hex16(r->uuid, u);
    hex16(r->cluster, c);
    size_t o = strlen(meta);
    snprintf(meta + o, sizeof meta - o, "volume=%" PRIu32 "\nvolume_uuid=%s\ncluster=%s\nrequires=cluster,replica\n",
             r->number, u, c);
    int dfd = -1, ok = mkdir(tmp, 0755) == 0 && write_new(tmp, "META", meta) == 0;
    if (ok) {
        struct replfile_state st;
        struct replfile *rf = NULL;
        fill_state(&st, r);
        dfd = open(tmp, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        /* generation 0, no nonce: the first open (cvolfs_prepare) saves generation 1 */
        int rc = dfd < 0 ? TSDB_ERR_IO : replfile_create(dfd, &st, true, &rf);
        if (rc != 0)
            errno = rc == TSDB_ERR_IO ? (errno ? errno : EIO) : EINVAL, ok = 0;
        replfile_close(rf);
        if (dfd >= 0)
            close(dfd);
    }
    if (!ok || tsdb_cvol_fno_create(tmp) != 0 || sync_dir(tmp) != 0 || rename(tmp, vdir) != 0 ||
        (cvolfs_test_mutant != CVOLFS_MUT_NODIRSYNC && sync_dir(root) != 0)) {
        say("volume %" PRIu32 ": cannot make %s: %s", r->number, vdir, strerror(errno));
        return -1;
    }
    say("volume %" PRIu32 " (%s): made %s, replica %016" PRIx64, r->number, r->name, vdir, r->rid);
    return 0;
}

int cvolfs_prepare(const char *root, const struct cvolfs_rec *r, struct cvolfs_open *out)
{
    char vdir[4200];
    if (snapinst_leftovers(root, r->number) != 0 || cvolset_prepare(root, r->number, r) != 0)
        return -1; /* R4: an install a crash interrupted, a directory the record no longer describes (lines said) */
    if ((size_t)snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, root, r->number) >= sizeof vdir) {
        say("volume %" PRIu32 ": the path of %s is too long", r->number, root);
        return -1;
    }
    struct stat sb;
    if (stat(vdir, &sb) != 0) {
        if (errno != ENOENT) {
            say("volume %" PRIu32 ": cannot look at %s: %s", r->number, vdir, strerror(errno));
            return -1;
        }
        if (make_dir(root, r, vdir) != 0)
            return -1;
    }
    int dfd = open(vdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) {
        say("volume %" PRIu32 ": cannot open %s: %s: the volume is not opened", r->number, vdir, strerror(errno));
        return -1;
    }
    struct replfile *rf = NULL;
    struct replfile_state st;
    bool torn = false;
    int lr = replfile_load(dfd, &rf, &st, &torn);
    close(dfd);
    if (lr != 0) {
        /* a directory made by this manager always has its REPL (made whole before the rename), so one without is
         * damage, and so is one with no valid slot (replfile_load says so too) */
        say("volume %" PRIu32 ": %s/REPL %s: the volume is not opened", r->number, vdir,
            lr == TSDB_ERR_NOTFOUND ? "is missing" : lr == TSDB_ERR_CORRUPT ? "has no valid slot" : "cannot be read");
        return -1;
    }
    int rc = -1;
    uint64_t draw = 0;
    if (memcmp(st.volume_uuid, r->uuid, 16) || memcmp(st.cluster_uuid, r->cluster, 16) || st.node != r->node) {
        say("volume %" PRIu32 ": %s/REPL belongs to another volume, cluster or node (node %" PRIu32 ", this server is %"
            PRIu32 "): not opened", r->number, vdir, st.node, r->node);
        goto out;
    }
    /* a torn load added a belt to open_gen (replfile_load): the checks below are about what the slot held */
    uint64_t raw = st.open_gen - (torn ? REPLFILE_OPEN_GEN_BELT : 0);
    if (torn) {
        say("volume %" PRIu32 ": %s/REPL was damaged (a torn save or rot): its fence, led and log start are not "
            "trusted, the open generation skips ahead", r->number, vdir);
        if (cvolfs_test_mutant == CVOLFS_MUT_TRUSTUNSURE)
            st.open_gen -= REPLFILE_OPEN_GEN_BELT;
    }
    uint64_t rid = st.rid, gen = st.open_gen, nonce = st.open_nonce;
    if (cvolfs_test_mutant == CVOLFS_MUT_SAMERID) /* a new instance at every open: replfile_save refuses it */
        rid = r->rid ^ (uint64_t)getpid() ^ 1;
    if (rid != r->rid) {
        say("volume %" PRIu32 ": %s holds replica %016" PRIx64 ", the metadata group names %016" PRIx64
            " for this server: another instance of the directory, not opened",
            r->number, vdir, rid, r->rid);
        if (cvolfs_test_mutant != CVOLFS_MUT_SAMERID)
            goto out;
        st.rid = rid;
    }
    /* The older slot of the current copy holds the open before the newest save: raw = rec - 1 (rot of the slot at rec),
     * rec - 1 - belt (the save of rec followed a torn load) or at least rec (a save after rec the group never heard of,
     * or a later save of the same open). Anything else is an older copy, whose slot rot has only made the belt hide.
     * A copy exactly one open behind with a damaged newest slot looks like rot of the current one: it is let in, and
     * reported unsure. */
    uint64_t rec = r->open_gen;
    bool back1 = rec >= 1 && raw == rec - 1;
    bool back1belt = rec >= 1 + REPLFILE_OPEN_GEN_BELT && raw == rec - 1 - REPLFILE_OPEN_GEN_BELT;
    if (raw < rec && !(torn && (back1 || back1belt || cvolfs_test_mutant == CVOLFS_MUT_TORNSTALE))) {
        say("volume %" PRIu32 ": %s is at open generation %" PRIu64 ", below the %" PRIu64
            " the metadata group recorded: an older copy of the directory, not opened",
            r->number, vdir, raw, rec);
        goto out;
    }
    /* the test switch's copy one open behind shows this directory's own last open, nonce included */
    uint64_t rec_nonce = r->stale_first && raw == rec ? nonce : r->open_nonce;
    if (raw == rec && rec > 0 && nonce != rec_nonce && cvolfs_test_mutant != CVOLFS_MUT_REPLNONCE) {
        say("volume %" PRIu32 ": %s saved open generation %" PRIu64 " in another open than the one the metadata group"
            " recorded (nonce %016" PRIx64 ", recorded %016" PRIx64 "): an older copy of the directory, not opened",
            r->number, vdir, raw, nonce, rec_nonce);
        goto out;
    }
    if (gen < rec) /* the belt did not reach the record (back1belt): the report must be above it */
        gen = rec;
    while (draw == 0 && tsdb_random(&draw, sizeof draw) == 0)
        ; /* 0 is a REPL's without one */
    st.open_gen = gen + 1;
    st.open_nonce = cvolfs_test_mutant == CVOLFS_MUT_NONONCE ? 0 : draw;
    st.incarnation = r->incarnation;
    if (draw == 0 || (rc = replfile_save(rf, &st)) != 0) {
        /* fail-stop for the volume: nothing about the file is known, the handle is dropped and never retried */
        say("volume %" PRIu32 ": cannot save open generation %" PRIu64 " in %s/REPL (%s): the volume is not opened",
            r->number, gen + 1, vdir, draw ? tsdb_strerror(rc) : "no randomness");
        rc = -1;
        goto out;
    }
    out->rid = st.rid;
    out->gen = gen + 1;
    out->nonce = draw;
    out->unsure = st.unsure;
    rc = 0;
out:
    replfile_close(rf);
    replfile_state_free(&st);
    return rc;
}
