/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The DIVERGED file (src/ecdiverge.[ch], R8 D8): its durability, its damage rules, its clearing, and tsdbd's one
 * answer for it. test_keep tests the flag through a settle's failed digest (with TSDB_EC); this test needs no codec:
 * it writes records with ec_diverge(EC_DIV_TEST) on a plain engine.
 *
 *   basic    records are appended once per (key, oid, why), read back by writers and readers, and set the flag
 *   damage   a bad record before a valid one is TSDB_ERR_CORRUPT (rw and read-only); a record copied to another
 *            offset is damage (the checksum is seeded with the offset); a torn tail opens and the next append cuts it;
 *            a DIVERGED that cannot be read (a directory in its place: EISDIR) is TSDB_ERR_IO, never "no records";
 *            a tail that a second read does not confirm (a misread page, the hook ecdiverge_test_misread) fails the
 *            open and is not cut
 *   eio      a failed write or fdatasync of DIVERGED (crashfs, CRASHFS_MODE=eio) fails the engine (fail-stop)
 *   power    power lost at every step of two appends (crashfs, torn writes): an acknowledged record is there after it
 *   preexist a DIVERGED whose name was never synced (its writer died between fdatasync and directory fsync), with
 *            records (the open trusts them: it must sync the name first) or empty (the first append must sync it):
 *            the power then fails; what the process acted on is still there
 *   cli      `volume diverged --clear` refuses when META cannot be read (it cannot tell a cluster volume) and when
 *            there is no META
 *   tsdbd    a single tsdbd with DIVERGED in the root and in volume 1: STATUS, the UNAVAILABLE reply (DIVERGED, the
 *            root found by the scan of a request without a volume) and /metrics name both; --clear offline, a
 *            restart: both serve again
 *
 * Mutants (src/ecdiverge.h; tools/mutants.sh requires each to fail it): nodirsync nofdatasync nofail

 * nocut nocorrupt seed0 notailconfirm readerrend metaopen.
 *
 * Usage: test_diverge [--mutant NAME] [--case NAME] [SCRATCH]   (TSDBD: the server, default ./build/tsdbd; the crashfs
 * shim is libcrashfs.so beside this binary; without it the crashfs cases say they are skipped)
 */
#define _GNU_SOURCE
#include "internal.h"
#include "ecdiverge.h"
#include "tsdb_client.h"
#include "harness.h"

#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

#define REC 48
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void fresh(const char *name, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, name);
    sh("rm -rf '%s'; mkdir -p '%s'", dir, dir);
}

static int open_eng(const char *dir, bool ro, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.read_only = ro;
    return eng_open(dir, &o, db);
}

static long fsize(const char *dir)
{
    char p[PATH_MAX + 64];
    struct stat st;
    snprintf(p, sizeof p, "%s/DIVERGED", dir);
    return stat(p, &st) == 0 ? (long)st.st_size : -1;
}

/* the number of records `volume diverged` lists (no engine), or its error */
static int listed(const char *dir)
{
    FILE *null = fopen("/dev/null", "w");
    int n = ecdiverge_cli(dir, false, null);
    if (null)
        fclose(null);
    return n;
}

/* a volume with `n` records (keys 1..n) made by ec_diverge, closed again */
static void with_records(const char *name, char *dir, size_t cap, int n)
{
    fresh(name, dir, cap);
    eng *db = NULL;
    CHECK(open_eng(dir, false, &db) == 0);
    for (int i = 1; db && i <= n; i++)
        CHECK(ec_diverge(db, i, NULL, EC_DIV_TEST) == 0);
    if (db)
        CHECK(eng_close(db) == 0);
    CHECK(fsize(dir) == (long)n * REC);
}

static int patch(const char *dir, long off, const void *bytes, size_t n)
{
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/DIVERGED", dir);
    int fd = open(p, O_RDWR);
    int ok = fd >= 0 && pwrite(fd, bytes, n, off) == (ssize_t)n;
    if (fd >= 0)
        close(fd);
    return ok ? 0 : -1;
}

static void case_basic(void)
{
    char dir[PATH_MAX + 32];
    fresh("basic", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, false, &db) == 0 && db);
    if (!db)
        return;
    CHECK(!atomic_load(&db->diverged) && fsize(dir) == -1);
    uint8_t oid[16] = {1, 2, 3};
    CHECK(ec_diverge(db, 7, oid, EC_DIV_TEST) == 0 && atomic_load(&db->diverged));
    CHECK(ec_diverge(db, 7, oid, EC_DIV_TEST) == 0 && fsize(dir) == REC); /* once per (key, oid, why) */
    CHECK(ec_diverge(db, 8, NULL, EC_DIV_TEST) == 0 && fsize(dir) == 2 * REC);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, false, &db) == 0 && db && atomic_load(&db->diverged));
    if (db) {
        CHECK(ec_diverge(db, 7, oid, EC_DIV_TEST) == 0 && fsize(dir) == 2 * REC); /* known from the file */
        CHECK(eng_close(db) == 0);
    }
    CHECK(open_eng(dir, true, &db) == 0 && db && atomic_load(&db->diverged)); /* a reader reads it too */
    if (db)
        CHECK(eng_close(db) == 0);
    CHECK(listed(dir) == 2);
    printf("  basic: appended once per record, read by writers and readers\n");
}

static void case_damage(void)
{
    char dir[PATH_MAX + 32];
    eng *db = NULL;
    /* a flipped byte in a record with a valid one after it: damage, never a torn tail to cut */
    with_records("dmg-mid", dir, sizeof dir, 3);
    uint8_t b = 0x77;
    CHECK(patch(dir, REC + 20, &b, 1) == 0);
    CHECK(open_eng(dir, false, &db) == TSDB_ERR_CORRUPT);
    CHECK(open_eng(dir, true, &db) == TSDB_ERR_CORRUPT);
    CHECK(listed(dir) == TSDB_ERR_CORRUPT && fsize(dir) == 3 * REC);
    /* record 0 copied over record 1: its checksum is seeded with offset 0, so at 48 it is damage */
    with_records("dmg-copy", dir, sizeof dir, 3);
    uint8_t r0[REC];
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/DIVERGED", dir);
    int fd = open(p, O_RDONLY);
    CHECK(fd >= 0 && pread(fd, r0, REC, 0) == REC);
    if (fd >= 0)
        close(fd);
    CHECK(patch(dir, REC, r0, REC) == 0);
    CHECK(open_eng(dir, false, &db) == TSDB_ERR_CORRUPT);
    /* a torn tail (a write the power cut): the open takes the records before it, the next append cuts it */
    with_records("dmg-torn", dir, sizeof dir, 2);
    uint8_t junk[100];
    memset(junk, 0xa5, sizeof junk);
    CHECK(patch(dir, 2 * REC, junk, sizeof junk) == 0);
    CHECK(open_eng(dir, false, &db) == 0 && db && atomic_load(&db->diverged));
    if (db) {
        CHECK(fsize(dir) == 2 * REC + 100); /* the open cuts nothing */
        CHECK(ec_diverge(db, 9, NULL, EC_DIV_TEST) == 0);
        CHECK(fsize(dir) == 3 * REC);
        CHECK(eng_close(db) == 0);
    }
    CHECK(listed(dir) == 3);
    /* a DIVERGED that cannot be read is an error, never "not diverged" */
    fresh("dmg-eisdir", dir, sizeof dir);
    CHECK(open_eng(dir, false, &db) == 0 && db);
    if (db)
        CHECK(eng_close(db) == 0);
    CHECK(sh("mkdir '%s/DIVERGED'", dir) == 0);
    CHECK(open_eng(dir, false, &db) == TSDB_ERR_IO);
    CHECK(open_eng(dir, true, &db) == TSDB_ERR_IO);
    CHECK(listed(dir) == TSDB_ERR_IO);
    /* a misread tail (the first read differs from the disk): the open fails, nothing is cut, a later open is fine */
    with_records("dmg-misread", dir, sizeof dir, 2);
    ecdiverge_test_misread = 1;
    int rc = open_eng(dir, false, &db);
    ecdiverge_test_misread = 0;
    CHECK(rc == TSDB_ERR_IO);
    if (rc == 0 && db) {
        CHECK(ec_diverge(db, 9, NULL, EC_DIV_TEST) == 0); /* what a cut would have cost: record 2 is gone */
        CHECK(eng_close(db) == 0);
    }
    CHECK(listed(dir) == 2 && fsize(dir) == 2 * REC);
    printf("  damage: CORRUPT before a valid record, a moved record is damage, a torn tail is cut by the next append, "
           "a read error and a misread tail fail the open\n");
}

/* ---------------------------------------------------------------- under crashfs */

static bool have_shim(void) { return access(SHIM, R_OK) == 0; }

/* runs this binary as `--child KIND DIR ACK` under the shim: power loss (or EIO) at tracked operation `warmup` (-1:
 * only at exit), seeded; extra: NULL-terminated name=value pairs */
static int run_child(const char *kind, const char *dir, const char *ack, int warmup, int seed, const char **extra)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", warmup >= 0 ? "1" : "0", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        for (int i = 0; extra && extra[i]; i += 2)
            setenv(extra[i], extra[i + 1], 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0 && !getenv("TEST_DIVERGE_VERBOSE")) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_diverge", "--child", kind, dir, ack, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void ack_line(const char *ack, const char *line)
{
    int fd = open(ack, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd >= 0) {
        if (write(fd, line, strlen(line)) < 0 || fdatasync(fd) != 0)
            _exit(5);
        close(fd);
    }
}

static int acked(const char *ack, const char *word)
{
    FILE *f = fopen(ack, "r");
    char line[64];
    int n = 0;
    while (f && fgets(line, sizeof line, f))
        if (!strncmp(line, word, strlen(word)))
            n++;
    if (f)
        fclose(f);
    return n;
}

/* the template of `n` records (keys 1..n) to plant: records do not name their directory */
static uint8_t tmpl[4 * REC];
static void make_template(void)
{
    char dir[PATH_MAX + 32], p[PATH_MAX + 64];
    with_records("template", dir, sizeof dir, 2);
    snprintf(p, sizeof p, "%s/DIVERGED", dir);
    int fd = open(p, O_RDONLY);
    CHECK(fd >= 0 && pread(fd, tmpl, 2 * REC, 0) == 2 * REC);
    if (fd >= 0)
        close(fd);
}

static int child_run(const char *kind, const char *dir, const char *ack)
{
    eng *db = NULL;
    if (!strcmp(kind, "eio")) { /* 0: the failed write failed the engine; 3: it did not; 4: no fault happened */
        if (open_eng(dir, false, &db) != 0)
            return 6;
        int rc = ec_diverge(db, 1, NULL, EC_DIV_TEST);
        bool failed = eng_check(db) != 0, flag = atomic_load(&db->diverged);
        eng_close(db);
        if (rc == 0)
            return 4;
        return failed && flag ? 0 : 3;
    }
    if (!strcmp(kind, "append")) { /* two records, each acknowledged after ec_diverge returned 0 */
        if (open_eng(dir, false, &db) != 0)
            return 6;
        for (int k = 1; k <= 2; k++)
            if (ec_diverge(db, k, NULL, EC_DIV_TEST) == 0)
                ack_line(ack, "rec\n");
        eng_close(db);
        ack_line(ack, "done\n");
        return 0;
    }
    if (!strcmp(kind, "preexist") || !strcmp(kind, "preempty")) {
        /* what a writer that died between its fdatasync and its directory fsync left: data synced, name not */
        char p[PATH_MAX + 64];
        snprintf(p, sizeof p, "%s/DIVERGED", dir);
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        bool full = !strcmp(kind, "preexist");
        if (fd < 0 || (full && write(fd, tmpl, 2 * REC) != 2 * REC) || fdatasync(fd) != 0)
            return 7;
        close(fd);
        if (open_eng(dir, false, &db) != 0)
            return 6;
        if (!full && ec_diverge(db, 1, NULL, EC_DIV_TEST) != 0)
            return 8;
        if (atomic_load(&db->diverged))
            ack_line(ack, "seen\n"); /* what a cluster would act on: deposed, out of the ISR */
        _exit(0); /* no close (it may sync the directory): the power fails at the exit (CRASHFS_AT_EXIT) */
    }
    return 2;
}

static void case_eio(void)
{
    if (!have_shim()) {
        printf("  eio: skipped (no %s)\n", SHIM);
        return;
    }
    const char *calls[] = {"pwrite", "fdatasync"};
    for (int i = 0; i < 2; i++) {
        char dir[PATH_MAX + 32], ack[PATH_MAX + 64], mark[PATH_MAX + 64];
        fresh(calls[i], dir, sizeof dir);
        snprintf(ack, sizeof ack, "%s.ack", dir);
        snprintf(mark, sizeof mark, "%s.mark", dir);
        unlink(mark);
        const char *extra[] = {"CRASHFS_MODE", "eio", "CRASHFS_EIO_ONLY", "DIVERGED", "CRASHFS_EIO_ON", calls[i],
                               "CRASHFS_MARK", mark, NULL};
        int code = run_child("eio", dir, ack, 0, 1, extra);
        if (code != 0)
            fprintf(stderr, "eio on %s: child exit %d (3: the engine did not fail; 4: no fault)\n", calls[i], code);
        CHECK(code == 0 && access(mark, F_OK) == 0);
    }
    printf("  eio: a failed write or fdatasync of DIVERGED fails the engine\n");
}

static void case_power(void)
{
    if (!have_shim()) {
        printf("  power: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX + 32], ack[PATH_MAX + 64];
    snprintf(ack, sizeof ack, "%s/power.ack", S);
    bool finished = false;
    int runs = 0, cut = 0;
    for (int k = 0; k < 2000 && !finished; k++) {
        fresh("power", dir, sizeof dir);
        unlink(ack);
        int code = run_child("append", dir, ack, k, k + 1, NULL);
        runs++;
        if (code != 0 && code != 99) {
            fprintf(stderr, "power k=%d: child exit %d\n", k, code);
            CHECK(0);
            break;
        }
        finished = acked(ack, "done") > 0;
        cut += code == 99;
        int acks = acked(ack, "rec");
        eng *db = NULL;
        int rc = open_eng(dir, false, &db);
        CHECK(rc == 0);
        int n = listed(dir);
        if (rc != 0 || n < acks || (db && atomic_load(&db->diverged) != (n > 0))) {
            fprintf(stderr, "power k=%d: open %d, %d records after %d acknowledged\n", k, rc, n, acks);
            CHECK(0);
            k = 1 << 20;
        }
        if (db)
            CHECK(eng_close(db) == 0);
    }
    CHECK(finished);
    printf("  power: %d runs (%d cut by a power loss), every acknowledged record survives\n", runs, cut);
}

static void case_preexist(void)
{
    if (!have_shim()) {
        printf("  preexist: skipped (no %s)\n", SHIM);
        return;
    }
    const char *kinds[] = {"preexist", "preempty"};
    for (int i = 0; i < 2; i++)
        for (int seed = 1; seed <= 8; seed++) {
            char dir[PATH_MAX + 32], ack[PATH_MAX + 64];
            fresh(kinds[i], dir, sizeof dir);
            /* the engine's own files first, outside the shim: only DIVERGED is new under it */
            eng *db = NULL;
            CHECK(open_eng(dir, false, &db) == 0);
            if (db)
                CHECK(eng_close(db) == 0);
            snprintf(ack, sizeof ack, "%s.ack", dir);
            unlink(ack);
            int code = run_child(kinds[i], dir, ack, -1, seed, NULL);
            CHECK(code == 0 && acked(ack, "seen") == 1);
            int rc = open_eng(dir, false, &db);
            CHECK(rc == 0 && db);
            if (db && !atomic_load(&db->diverged)) {
                fprintf(stderr, "%s seed %d: acted on DIVERGED, which a power loss then took away\n", kinds[i], seed);
                CHECK(0);
            }
            if (db)
                CHECK(eng_close(db) == 0);
        }
    printf("  preexist: a DIVERGED name left unsynced is synced before the flag is acted on\n");
}

static void case_cli(void)
{
    char dir[PATH_MAX + 32];
    FILE *null = fopen("/dev/null", "w");
    /* META unreadable (a directory in its place, which root cannot read either) */
    with_records("cli-meta", dir, sizeof dir, 1);
    CHECK(sh("cd '%s' && mv META META.x && mkdir META", dir) == 0);
    CHECK(ecdiverge_cli(dir, true, null) < 0 && fsize(dir) == REC);
    /* no META: not a database directory */
    CHECK(sh("rmdir '%s/META'", dir) == 0);
    CHECK(ecdiverge_cli(dir, true, null) < 0 && fsize(dir) == REC);
    CHECK(ecdiverge_cli(dir, false, null) == 1); /* listing needs no META */
    CHECK(sh("mv '%s/META.x' '%s/META'", dir, dir) == 0);
    CHECK(ecdiverge_cli(dir, true, null) == 1 && fsize(dir) == -1);
    if (null)
        fclose(null);
    printf("  cli: --clear refuses without a readable META, clears with one\n");
}

/* ---------------------------------------------------------------- tsdbd */

static void kill_pid(pid_t pid, int sig)
{
    if (pid > 0) /* never 0 or -1 (AGENTS.md) */
        kill(pid, sig);
}

static pid_t start_tsdbd(const char *root, int *port, int *mport)
{
    char pf[PATH_MAX + 64], follow[64];
    th_portfile(pf, sizeof pf, root);
    th_follow_me(follow, sizeof follow);
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    pid_t pid = fork();
    if (pid == 0) {
        const char *argv[24] = {"tsdbd", "--dir", root, "--port", "0", "--port-file", pf, "--no-auth",
                                "--metrics-port", "auto", follow};
        int n = 11;
        if (getenv("TSDBD_IO")) {
            argv[n++] = "--io";
            argv[n++] = getenv("TSDBD_IO");
        }
        argv[n] = NULL;
        execv(bin, (char *const *)argv);
        _exit(127);
    }
    *port = th_await(pf, pid, 30000);
    *mport = th_mport;
    unlink(pf);
    if (*port <= 0) {
        fputs(th_why, stderr);
        kill_pid(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        return -1;
    }
    return pid;
}

static void stop_tsdbd(pid_t pid)
{
    if (pid <= 0)
        return;
    kill_pid(pid, SIGTERM);
    int st = -1;
    CHECK(waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
}

static void metrics_get(int port, char *buf, size_t n)
{
    buf[0] = 0;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (fd < 0 || port <= 0 || connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        if (fd >= 0)
            close(fd);
        return;
    }
    struct timeval tv = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    static const char req[] = "GET /metrics HTTP/1.0\r\n\r\n";
    size_t got = 0;
    if (write(fd, req, sizeof req - 1) == (ssize_t)sizeof req - 1)
        for (ssize_t k; got < n - 1 && (k = read(fd, buf + got, n - 1 - got)) > 0;)
            got += (size_t)k;
    buf[got] = 0;
    close(fd);
}

/* the metric tsdb_volume_diverged of volume v: 0, 1, or -1 when it is not there */
static int metric_diverged(const char *text, unsigned v)
{
    char key[64];
    snprintf(key, sizeof key, "tsdb_volume_diverged{volume=\"%u\",", v);
    const char *p = strstr(text, key), *e = p ? strchr(p, '}') : NULL;
    return e && e[1] == ' ' ? atoi(e + 2) : -1;
}

static bool status_says(tsdbc *c, unsigned v, const char *health, const char *reason)
{
    char *text = NULL, want[128];
    snprintf(want, sizeof want, "\nvol.%u.health=%s\nvol.%u.reason=%s\n", v, health, v, reason);
    bool ok = c && tsdbc_status(c, &text) == 0 && text && strstr(text, want);
    if (!ok)
        fprintf(stderr, "STATUS lacks %s in:\n%s\n", want, text ? text : "(none)");
    free(text);
    return ok;
}

static void case_tsdbd(void)
{
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    if (access(bin, X_OK) != 0) {
        printf("  tsdbd: skipped (no %s)\n", bin);
        return;
    }
    char root[PATH_MAX + 32], vol[PATH_MAX + 32], mb[1 << 16];
    fresh("srv", root, sizeof root);
    snprintf(vol, sizeof vol, "%s/srv-v1", S);
    sh("rm -rf '%s'", vol);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *t = NULL;
    tsdb_sid sb = 0;
    uint32_t idx = 0;
    CHECK(tsdb_open(root, &o, &t) == 0 && t);
    if (t)
        tsdb_close(t);
    CHECK(tsdb_volume_add_dir(root, vol, &idx) == 0 && idx == 1);
    CHECK(tsdb_open(root, &o, &t) == 0 && t);
    if (t) {
        CHECK(tsdb_series_create(t, "b", TSDB_FLOAT32, vol, &sb) == 0);
        tsdb_close(t);
    }
    /* plant DIVERGED in both (as a settle's failed digest leaves it) */
    for (int i = 0; i < 2; i++) {
        char p[PATH_MAX + 64];
        snprintf(p, sizeof p, "%s/DIVERGED", i ? vol : root);
        FILE *f = fopen(p, "w");
        CHECK(f && fwrite(tmpl, 1, 2 * REC, f) == 2 * REC);
        if (f)
            fclose(f);
    }
    int port = 0, mport = 0;
    pid_t pid = start_tsdbd(root, &port, &mport);
    CHECK(pid > 0 && mport > 0);
    tsdbc *c = NULL;
    if (pid > 0 && tsdbc_connect_timeout("127.0.0.1", port, 5000, &c) == 0)
        tsdbc_set_retry(c, 0);
    CHECK(c);
    if (c) {
        CHECK(status_says(c, 0, "unavailable", "diverged"));
        CHECK(status_says(c, 1, "unavailable", "diverged"));
        int64_t ts = 1700000000000LL;
        float v = 1;
        tsdb_unavail ue[4];
        unsigned un = 0;
        CHECK(tsdbc_insert_series(c, sb, &ts, &v, 1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tsdbc_unavailable(c, ue, 4, &un) == 0 && un >= 1 && ue[0].reason == TSDB_WHY_DIVERGED &&
              ue[0].vol == 1);
        tsdb_sid sa = 0;
        CHECK(tsdbc_series_create(c, "a", TSDB_FLOAT32, NULL, &sa) == TSDB_ERR_UNAVAILABLE); /* the root */
        un = 0;
        CHECK(tsdbc_unavailable(c, ue, 4, &un) == 0 && un >= 1 && ue[0].reason == TSDB_WHY_DIVERGED);
        tsdbc_close(c);
    }
    metrics_get(mport, mb, sizeof mb);
    CHECK(metric_diverged(mb, 0) == 1 && metric_diverged(mb, 1) == 1);
    stop_tsdbd(pid);
    /* the operator's step, offline: both serve again */
    FILE *null = fopen("/dev/null", "w");
    CHECK(ecdiverge_cli(root, true, null) == 2 && ecdiverge_cli(vol, true, null) == 2);
    if (null)
        fclose(null);
    pid = start_tsdbd(root, &port, &mport);
    CHECK(pid > 0);
    c = NULL;
    if (pid > 0 && tsdbc_connect_timeout("127.0.0.1", port, 5000, &c) == 0)
        tsdbc_set_retry(c, 0);
    CHECK(c);
    if (c) {
        CHECK(status_says(c, 0, "available", "none"));
        CHECK(status_says(c, 1, "available", "none"));
        int64_t ts = 1700000000000LL;
        float v = 1;
        tsdb_sid sa = 0;
        CHECK(tsdbc_insert_series(c, sb, &ts, &v, 1) == 0);
        CHECK(tsdbc_series_create(c, "a", TSDB_FLOAT32, NULL, &sa) == 0);
        tsdbc_close(c);
    }
    metrics_get(mport, mb, sizeof mb);
    CHECK(metric_diverged(mb, 0) == 0 && metric_diverged(mb, 1) == 0);
    stop_tsdbd(pid);
    printf("  tsdbd: DIVERGED in the root and in a volume: STATUS, UNAVAILABLE and /metrics name both; --clear "
           "offline serves them again\n");
}

static const struct {
    const char *name;
    int m;
} MUTANTS[] = {{"nodirsync", ECDIV_MUT_NODIRSYNC},     {"nofdatasync", ECDIV_MUT_NOFDATASYNC},
               {"nofail", ECDIV_MUT_NOFAIL},
               {"nocut", ECDIV_MUT_NOCUT},             {"nocorrupt", ECDIV_MUT_NOCORRUPT},
               {"seed0", ECDIV_MUT_SEED0},             {"notailconfirm", ECDIV_MUT_NOTAILCONFIRM},
               {"readerrend", ECDIV_MUT_READERREND},   {"metaopen", ECDIV_MUT_METAOPEN}};

static int set_mutant(const char *m)
{
    for (size_t k = 0; k < sizeof MUTANTS / sizeof *MUTANTS; k++)
        if (!strcmp(m, MUTANTS[k].name))
            return ecdiverge_mutant = MUTANTS[k].m, 0;
    return -1;
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc >= 5 && !strcmp(argv[1], "--child")) {
        if (getenv("TEST_DIVERGE_MUTANT"))
            set_mutant(getenv("TEST_DIVERGE_MUTANT"));
        if (getenv("TEST_DIVERGE_TMPL")) { /* the template, handed over in a file */
            int fd = open(getenv("TEST_DIVERGE_TMPL"), O_RDONLY);
            if (fd < 0 || read(fd, tmpl, 2 * REC) != 2 * REC)
                return 9;
            close(fd);
        }
        return child_run(argv[2], argv[3], argv[4]);
    }
    const char *scratch = "/tmp", *mutant = NULL, *only = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else if (!strcmp(argv[i], "--case") && i + 1 < argc)
            only = argv[++i];
        else
            scratch = argv[i];
    }
    if (mutant && set_mutant(mutant) != 0) {
        fprintf(stderr, "unknown mutant %s\n", mutant);
        return 2;
    }
    if (mutant)
        setenv("TEST_DIVERGE_MUTANT", mutant, 1);
    char base[PATH_MAX];
    snprintf(base, sizeof base, "%s/test-diverge.XXXXXX", scratch);
    if (!mkdtemp(base)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", base);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    make_template();
    char tf[PATH_MAX + 32];
    snprintf(tf, sizeof tf, "%s/template.bin", S);
    FILE *f = fopen(tf, "w");
    CHECK(f && fwrite(tmpl, 1, 2 * REC, f) == 2 * REC);
    if (f)
        fclose(f);
    setenv("TEST_DIVERGE_TMPL", tf, 1);
    static const struct {
        const char *name;
        void (*fn)(void);
    } cases[] = {{"basic", case_basic}, {"damage", case_damage}, {"eio", case_eio},  {"power", case_power},
                 {"preexist", case_preexist}, {"cli", case_cli},  {"tsdbd", case_tsdbd}};
    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++)
        if (!only || strstr(only, cases[i].name)) /* --case a,b */

            cases[i].fn();
    if (!failures)
        sh("rm -rf '%s'", S);
    printf("test_diverge: %s\n", failures ? "FAILED" : "ok");
    return failures ? 1 : 0;
}
