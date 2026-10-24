/*
 * A Jepsen-style test of tsdbd: clients write and read through the network
 * while a nemesis crashes the server, and a checker verifies the history.
 *
 *   jepsen --dir DIR [--nemesis kill|power|pause|eio|none] [--seconds 20]
 *          [--clients 8] [--seed N] [--crashfs build/libcrashfs.so]
 *
 * Model. Each series is a set of keys (timestamps) holding the id of the write
 * that set them, last write wins. Every write has a unique id, stored as the
 * value, so the final state and every read say which write they come from.
 * Writes are batches of 1 to 3000 rows on random keys; the server is started
 * with small partitions and a small WAL threshold, so batches take both write
 * paths and many of them span several partitions (parts).
 *
 * Nemeses: kill (SIGKILL the server), power (the crashfs shim rolls the
 * directory back to what was fsync'd, then the server dies), pause
 * (SIGSTOP/SIGCONT), eio (the crashfs shim fails one write or sync with EIO
 * and from then on lies about that file's syncs, as Linux may after a failed
 * writeback; at exit it rolls back what was not made durable). After an EIO
 * on anything already relied upon (the WAL, MANIFEST, a dictionary, CATALOG,
 * the directory) the server must stop by itself with exit status 1
 * (fail-stop); after one on an unpublished part (tmp_*) or intent (txn_*) it
 * may stop or carry on, and then loses power (or, if idle, is stopped with
 * SIGTERM).
 * After each fault the server is restarted.
 *
 * Checks, with applied(w) = acknowledged, or found in the final state, or seen
 * by a read:
 *  1. durability and order: for every applied write w and each key k it wrote,
 *     the final value of k is not from a write that completed before w began;
 *  2. validity: a definitely failed write is never seen, and every value seen
 *     at a key was written there;
 *  3. atomic visibility: a read that sees w at one of its keys does not see,
 *     at another key of w inside its range, a value from a write that
 *     completed before w began (or nothing at all);
 *  4. no stale reads: a read that began after w was acknowledged sees, at w's
 *     keys inside its range, w or something not older than w.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define NSERIES 4
#define NKEYS 6000
#define T0 1700000000000LL
#define KEY_MS 2000      /* keys every 2 s: 6000 keys span 3.3 hours */
#define PARTITION_MS 3600000LL
#define MAX_OPS 2000000

typedef enum { ST_PENDING, ST_OK, ST_FAIL, ST_INFO } status;

typedef struct wop {
    int series;
    uint32_t *keys;
    uint32_t nkeys;
    int64_t invoke, complete; /* ns */
    status st;
} wop;

typedef struct rop {
    int series;
    uint32_t lo, hi;   /* key range [lo, hi) */
    uint32_t *seen;    /* write id per key in range, 0 = absent */
    int64_t invoke, complete;
    status st;
} rop;

static const char *dir, *crashfs, *nemesis = "kill";
static int nclients = 8, port;
static double seconds = 20;
static uint64_t seed = 1;
static uint32_t ids[NSERIES];

static wop *writes; /* indexed by write id; id 0 unused */
static atomic_uint nwrites = 1;
static rop *reads;
static atomic_uint nreads;
static atomic_bool stop;
static atomic_int server_pid;
static pthread_mutex_t server_lock = PTHREAD_MUTEX_INITIALIZER;
static int restarts;
static char markpath[PATH_MAX]; /* eio: crashfs writes the path of the file it failed here */
static int failstops, eio_problems;

static int64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static uint64_t mix(uint64_t x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    return x ^ (x >> 33);
}

/* ----------------------------------------------------------- the server */

static void stop_server(int sig);

static void start_server(bool faulty)
{
    char portstr[16], pms[32];
    snprintf(portstr, sizeof portstr, "%d", port);
    snprintf(pms, sizeof pms, "%lld", PARTITION_MS);
    unlink(markpath);
    pid_t pid = fork();
    if (pid == 0) {
        if (faulty && crashfs) {
            char sd[32];
            snprintf(sd, sizeof sd, "%llu", (unsigned long long)mix(seed + (uint64_t)restarts));
            setenv("LD_PRELOAD", crashfs, 1);
            setenv("CRASHFS_ROOT", dir, 1);
            setenv("CRASHFS_SEED", sd, 1);
            setenv("CRASHFS_PROB", "0.0005", 1);
            setenv("CRASHFS_WARMUP", "200", 1);
            if (!strcmp(nemesis, "eio")) {
                setenv("CRASHFS_MODE", "eio", 1);
                setenv("CRASHFS_MARK", markpath, 1);
                if (restarts % 2 == 0) /* every other server: the WAL, where an error must stop it */
                    setenv("CRASHFS_EIO_ONLY", "wal_", 1);
            }
        }
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0 && !getenv("JEPSEN_VERBOSE"))
            dup2(devnull, 2);
        execl(getenv("JEPSEN_TSDBD") ? getenv("JEPSEN_TSDBD") : "./build/tsdbd", "tsdbd", "--dir", dir, "--port", portstr,
              "--workers", "8", "--wal-max-batch-rows", "500", "--partition-ms", pms, getenv("TSDBD_IO") ? "--io" : NULL,
              getenv("TSDBD_IO"), (char *)NULL);
        _exit(127);
    }
    atomic_store(&server_pid, pid);
    if (getenv("JEPSEN_VERBOSE"))
        fprintf(stderr, "jepsen: started %d (%s)\n", (int)pid, faulty ? "faulty" : "clean");
    for (int i = 0; i < 300; i++) { /* until it accepts */
        tsdbc *c;
        if (tsdbc_connect("127.0.0.1", port, &c) == 0) {
            tsdbc_close(c);
            return;
        }
        int st;
        if (waitpid(pid, &st, WNOHANG) == pid) { /* crashed while starting (power loss at open) */
            if (getenv("JEPSEN_VERBOSE"))
                fprintf(stderr, "jepsen: %d died while starting\n", (int)pid);
            unlink(markpath); /* an EIO while opening: the clean server below has no fault */
            pid = fork();
            if (pid == 0) {
                execl(getenv("JEPSEN_TSDBD") ? getenv("JEPSEN_TSDBD") : "./build/tsdbd", "tsdbd", "--dir", dir, "--port",
                      portstr, "--workers", "8", "--wal-max-batch-rows", "500", "--partition-ms", pms,
                      getenv("TSDBD_IO") ? "--io" : NULL, getenv("TSDBD_IO"), (char *)NULL);
                _exit(127);
            }
            atomic_store(&server_pid, pid);
        }
        usleep(20000);
    }
    fprintf(stderr, "jepsen: the server does not come up\n");
    stop_server(SIGKILL);
    exit(2);
}

/* kill() with pid 0 or -1 signals the process group, or every process of the
 * user: with no server running, a nemesis must do nothing. */
static int signal_pid(pid_t pid, int sig) { return pid > 0 ? kill(pid, sig) : -1; }

static void stop_server(int sig)
{
    pid_t pid = atomic_load(&server_pid);
    if (getenv("JEPSEN_VERBOSE"))
        fprintf(stderr, "jepsen: stopping %d with signal %d\n", (int)pid, sig);
    if (pid > 0) {
        signal_pid(pid, sig);
        int st;
        waitpid(pid, &st, 0);
        atomic_store(&server_pid, 0);
    }
}

/* ------------------------------------------------------------ clients */

static tsdbc *connect_retry(void)
{
    for (;;) {
        tsdbc *c;
        if (tsdbc_connect("127.0.0.1", port, &c) == 0)
            return c;
        if (atomic_load(&stop))
            return NULL;
        usleep(10000);
    }
}

static void *client(void *arg)
{
    uint64_t r = mix(seed * 1000 + (uint64_t)(intptr_t)arg);
    tsdbc *c = connect_retry();
    while (c && !atomic_load(&stop)) {
        r = mix(r);
        int s = (int)(r % NSERIES);
        if ((r >> 8) % 10 < 6) { /* a write */
            uint32_t n = (r >> 16) % 100 < 85 ? 1 + (uint32_t)((r >> 24) % 20) : 400 + (uint32_t)((r >> 24) % 2600);
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= MAX_OPS)
                break;
            wop *w = &writes[id];
            w->series = s;
            w->keys = malloc(n * sizeof *w->keys);
            int64_t *ts = malloc(n * sizeof *ts);
            float *v = malloc(n * sizeof *v);
            /* distinct keys: a random start and stride */
            uint32_t start = (uint32_t)(mix(r + 1) % NKEYS), stride = 1 + (uint32_t)(mix(r + 2) % 7);
            uint32_t m = 0;
            for (uint32_t i = 0; i < n; i++) {
                uint32_t k = (start + i * stride) % NKEYS;
                bool dup = false;
                for (uint32_t j = 0; j < m && n <= 20; j++)
                    dup |= w->keys[j] == k;
                if (dup || (n > 20 && i * stride >= NKEYS))
                    continue;
                w->keys[m] = k;
                ts[m] = T0 + (int64_t)k * KEY_MS;
                v[m] = (float)id;
                m++;
            }
            w->nkeys = m;
            w->invoke = now_ns();
            int rc = tsdbc_insert_series(c, ids[s], ts, v, m);
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            free(ts);
            free(v);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) {
                tsdbc_close(c);
                c = connect_retry();
            }
        } else { /* a read of a key range */
            uint32_t idx = atomic_fetch_add(&nreads, 1);
            if (idx >= MAX_OPS)
                break;
            rop *q = &reads[idx];
            q->series = s;
            q->lo = (uint32_t)((r >> 12) % NKEYS);
            q->hi = q->lo + 1 + (uint32_t)((r >> 30) % 400);
            if (q->hi > NKEYS)
                q->hi = NKEYS;
            q->seen = calloc(q->hi - q->lo, sizeof *q->seen);
            tsdb_points p;
            q->invoke = now_ns();
            int rc = tsdbc_query_points(c, ids[s], T0 + (int64_t)q->lo * KEY_MS, T0 + (int64_t)q->hi * KEY_MS, &p);
            q->complete = now_ns();
            if (rc == 0) {
                q->st = ST_OK;
                for (size_t i = 0; i < p.n; i++) {
                    int64_t k = (p.ts[i] - T0) / KEY_MS;
                    if (k >= q->lo && k < q->hi && (p.ts[i] - T0) % KEY_MS == 0)
                        q->seen[k - q->lo] = (uint32_t)p.values[i];
                    else
                        q->seen[0] = UINT32_MAX; /* a row at no key: invalid */
                }
                free(p.ts);
                free(p.values);
            } else {
                q->st = ST_FAIL;
                tsdbc_close(c);
                c = connect_retry();
            }
        }
    }
    tsdbc_close(c);
    return NULL;
}

/* ------------------------------------------------------------- nemesis */

static void *nemesis_thread(void *arg)
{
    (void)arg;
    uint64_t r = mix(seed + 77);
    while (!atomic_load(&stop)) {
        r = mix(r);
        usleep(150000 + (useconds_t)(r % 600000));
        if (atomic_load(&stop))
            break;
        pthread_mutex_lock(&server_lock);
        if (!strcmp(nemesis, "kill")) {
            stop_server(SIGKILL);
            start_server(false);
            restarts++;
        } else if (!strcmp(nemesis, "power")) {
            pid_t pid = atomic_load(&server_pid);
            int st;
            if (waitpid(pid, &st, WNOHANG) != pid) { /* not crashed on its own yet: pull the plug */
                signal_pid(pid, SIGUSR2);
                for (int i = 0; i < 100 && waitpid(pid, &st, WNOHANG) != pid; i++)
                    usleep(10000);
                if (signal_pid(pid, 0) == 0) { /* idle, no file operation to crash in: plain kill */
                    signal_pid(pid, SIGKILL);
                    waitpid(pid, &st, 0);
                }
            }
            atomic_store(&server_pid, 0);
            start_server(true);
            restarts++;
        } else if (!strcmp(nemesis, "eio")) {
            pid_t pid = atomic_load(&server_pid);
            int st = 0;
            bool gone = waitpid(pid, &st, WNOHANG) == pid;
            if (!gone && access(markpath, F_OK) != 0) { /* the next write or sync fails */
                signal_pid(pid, SIGUSR2);
                for (int i = 0; i < 100 && access(markpath, F_OK) != 0; i++)
                    usleep(10000);
            }
            if (access(markpath, F_OK) == 0) {
                for (int i = 0; !gone && i < 100; i++) /* fail-stop takes about 220 ms */
                    if (!(gone = waitpid(pid, &st, WNOHANG) == pid))
                        usleep(10000);
                char failed[PATH_MAX] = "";
                FILE *m = fopen(markpath, "r");
                if (m) {
                    if (fgets(failed, sizeof failed, m))
                        failed[strcspn(failed, "\n")] = 0;
                    fclose(m);
                }
                const char *base = strrchr(failed, '/') ? strrchr(failed, '/') + 1 : failed;
                if (gone) {
                    failstops++;
                    if (!WIFEXITED(st) || WEXITSTATUS(st) != 1) {
                        eio_problems++;
                        fprintf(stderr, "  after EIO on %s the server ended with status %#x, not exit 1\n", failed, st);
                    }
                } else {
                    /* only an unpublished part (tmp_) or an intent whose name was never synced (txn_) fails
                     * just the call; the WAL, MANIFEST, the dictionaries, CATALOG and the directory are
                     * relied upon (class b in docs/design.md): fail-stop */
                    if (strncmp(base, "tmp_", 4) && strncmp(base, "txn_", 4)) {
                        eio_problems++;
                        fprintf(stderr, "  after EIO on %s the server is still running after 1 s\n", failed);
                    }
                    signal_pid(pid, SIGUSR2); /* a power loss at its next file operation */
                    for (int i = 0; i < 100 && waitpid(pid, &st, WNOHANG) != pid; i++)
                        usleep(10000);
                    if (signal_pid(pid, 0) == 0) { /* idle: stopped, the shim rolls back at exit */
                        signal_pid(pid, SIGTERM);
                        waitpid(pid, &st, 0);
                    }
                }
                atomic_store(&server_pid, 0);
                start_server(true);
                restarts++;
            } else if (gone) {
                eio_problems++;
                fprintf(stderr, "  the server ended (status %#x) without a fault\n", st);
                atomic_store(&server_pid, 0);
                start_server(true);
                restarts++;
            }
        } else if (!strcmp(nemesis, "pause")) {
            pid_t pid = atomic_load(&server_pid);
            signal_pid(pid, SIGSTOP);
            usleep(100000 + (useconds_t)(r % 300000));
            signal_pid(pid, SIGCONT);
        }
        pthread_mutex_unlock(&server_lock);
    }
    return NULL;
}

/* -------------------------------------------------------------- checker */

static int violations;

#define VIOLATION(...)                         \
    do {                                       \
        if (violations++ < 20)                 \
            fprintf(stderr, "  " __VA_ARGS__); \
    } while (0)

/* w1 completed before w2 began: w1 is strictly older */
static bool older(uint32_t w1, uint32_t w2) { return writes[w1].complete < writes[w2].invoke; }

static bool wrote_key(uint32_t w, uint32_t k)
{
    for (uint32_t i = 0; i < writes[w].nkeys; i++)
        if (writes[w].keys[i] == k)
            return true;
    return false;
}

static int check(uint32_t final[NSERIES][NKEYS])
{
    uint32_t nw = atomic_load(&nwrites), nr = atomic_load(&nreads);
    if (nw > MAX_OPS)
        nw = MAX_OPS;
    if (nr > MAX_OPS)
        nr = MAX_OPS;
    bool *applied = calloc(nw, sizeof *applied);
    /* applied: acknowledged, in the final state, or seen by a read */
    for (uint32_t w = 1; w < nw; w++)
        applied[w] = writes[w].st == ST_OK;
    for (int s = 0; s < NSERIES; s++)
        for (uint32_t k = 0; k < NKEYS; k++) {
            uint32_t w = final[s][k];
            if (!w)
                continue;
            if (w >= nw || writes[w].series != s || !wrote_key(w, k)) {
                VIOLATION("final: series %d key %u holds %u, which never wrote it\n", s, k, w);
                continue;
            }
            applied[w] = true;
        }
    for (uint32_t i = 0; i < nr; i++) {
        rop *q = &reads[i];
        if (q->st != ST_OK)
            continue;
        if (q->hi > q->lo && q->seen[0] == UINT32_MAX) {
            VIOLATION("read %u: a row at a timestamp no write used\n", i);
            continue;
        }
        for (uint32_t k = q->lo; k < q->hi; k++) {
            uint32_t w = q->seen[k - q->lo];
            if (!w)
                continue;
            if (w >= nw || writes[w].series != q->series || !wrote_key(w, k)) {
                VIOLATION("read %u: key %u shows %u, which never wrote it\n", i, k, w);
                continue;
            }
            applied[w] = true;
        }
    }
    /* 2. a definitely failed write was applied */
    for (uint32_t w = 1; w < nw; w++)
        if (applied[w] && writes[w].st == ST_FAIL)
            VIOLATION("write %u failed but was applied\n", w);
    /* 1. durability and order */
    uint64_t lost = 0;
    for (uint32_t w = 1; w < nw; w++) {
        if (!applied[w])
            continue;
        for (uint32_t i = 0; i < writes[w].nkeys; i++) {
            uint32_t k = writes[w].keys[i], f = final[writes[w].series][k];
            if (!f || older(f, w)) {
                if (lost++ < 10)
                    VIOLATION("write %u (%s) to series %d key %u lost: final holds %u\n", w,
                              writes[w].st == ST_OK ? "acknowledged" : "seen or partly kept", writes[w].series, k, f);
                else
                    violations++;
            }
        }
    }
    /* 3. atomic visibility: each distinct write seen by a read, checked once */
    uint32_t *distinct = malloc(400 * sizeof *distinct);
    for (uint32_t i = 0; i < nr; i++) {
        rop *q = &reads[i];
        if (q->st != ST_OK)
            continue;
        size_t nd = 0;
        for (uint32_t k = q->lo; k < q->hi; k++) {
            uint32_t w = q->seen[k - q->lo];
            if (!w || w >= nw)
                continue;
            bool known = false;
            for (size_t d = 0; d < nd && !known; d++)
                known = distinct[d] == w;
            if (!known)
                distinct[nd++] = w;
        }
        for (size_t d = 0; d < nd; d++) {
            uint32_t w = distinct[d];
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                uint32_t k2 = writes[w].keys[j];
                if (k2 < q->lo || k2 >= q->hi)
                    continue;
                uint32_t other = q->seen[k2 - q->lo];
                if (!other || (other != w && older(other, w))) {
                    VIOLATION("read %u [%lld..%lld] saw write %u (%u keys, %lld..%lld) but %u (%u keys, %lld..%lld) at "
                              "its key %u: torn\n",
                              i, (long long)q->invoke, (long long)q->complete, w, writes[w].nkeys,
                              (long long)writes[w].invoke, (long long)writes[w].complete, other,
                              other < nw ? writes[other].nkeys : 0, other < nw ? (long long)writes[other].invoke : 0LL,
                              other < nw ? (long long)writes[other].complete : 0LL, k2);
                }
            }
        }
    }
    free(distinct);

    /* 4. no stale reads. Per key: acknowledged writes by completion time, with
     * the running maximum of their start times. A read that began at t must
     * not show anything older than the newest-starting write completed by t. */
    typedef struct ack {
        int64_t complete, max_invoke;
    } ack;
    size_t total = 0;
    uint32_t *cnt = calloc((size_t)NSERIES * NKEYS + 1, sizeof *cnt);
    for (uint32_t w = 1; w < nw; w++)
        if (writes[w].st == ST_OK)
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                cnt[(size_t)writes[w].series * NKEYS + writes[w].keys[j] + 1]++;
                total++;
            }
    for (size_t x = 1; x <= (size_t)NSERIES * NKEYS; x++)
        cnt[x] += cnt[x - 1];
    ack *acks = malloc((total + 1) * sizeof *acks);
    uint32_t *fill = calloc((size_t)NSERIES * NKEYS, sizeof *fill);
    for (uint32_t w = 1; w < nw; w++)
        if (writes[w].st == ST_OK)
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                size_t slot = (size_t)writes[w].series * NKEYS + writes[w].keys[j];
                acks[cnt[slot] + fill[slot]++] = (ack){writes[w].complete, writes[w].invoke};
            }
    for (size_t slot = 0; slot < (size_t)NSERIES * NKEYS; slot++) {
        ack *a = acks + cnt[slot];
        size_t n = cnt[slot + 1] - cnt[slot];
        for (size_t x = 1; x < n; x++) /* insertion sort: few writes per key */
            for (size_t y = x; y > 0 && a[y - 1].complete > a[y].complete; y--) {
                ack t = a[y];
                a[y] = a[y - 1];
                a[y - 1] = t;
            }
        for (size_t x = 1; x < n; x++)
            if (a[x].max_invoke < a[x - 1].max_invoke)
                a[x].max_invoke = a[x - 1].max_invoke;
    }
    for (uint32_t i = 0; i < nr; i++) {
        rop *q = &reads[i];
        if (q->st != ST_OK)
            continue;
        for (uint32_t k = q->lo; k < q->hi; k++) {
            size_t slot = (size_t)q->series * NKEYS + k;
            ack *a = acks + cnt[slot];
            size_t n = cnt[slot + 1] - cnt[slot], lo = 0, hi = n;
            while (lo < hi) { /* acknowledged before the read began */
                size_t mid = (lo + hi) / 2;
                if (a[mid].complete < q->invoke)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            if (!lo)
                continue;
            uint32_t seen = q->seen[k - q->lo];
            if (!seen || (seen < nw && writes[seen].complete < a[lo - 1].max_invoke))
                VIOLATION("read %u shows %u at series %d key %u, older than an acknowledged write: stale\n", i, seen,
                          q->series, k);
        }
    }
    free(acks);
    free(fill);
    free(cnt);
    free(applied);
    return violations;
}

static void final_state(uint32_t final[NSERIES][NKEYS])
{
    /* not connect_retry(): that gives up once `stop` is set, which it is by now */
    tsdbc *c = NULL;
    for (int i = 0; i < 1000 && tsdbc_connect("127.0.0.1", port, &c) != 0; i++)
        usleep(10000);
    if (!c) {
        fprintf(stderr, "jepsen: cannot connect for the final read\n");
        stop_server(SIGTERM);
        exit(2);
    }
    for (int s = 0; s < NSERIES; s++) {
        memset(final[s], 0, sizeof final[s]);
        tsdb_points p;
        int rc = tsdbc_query_points(c, ids[s], INT64_MIN, INT64_MAX, &p);
        if (rc != 0) {
            fprintf(stderr, "jepsen: final read of series %d failed: %s\n", s, tsdbc_strerror(rc));
            int st;
            pid_t pid = atomic_load(&server_pid);
            usleep(200000);
            if (pid > 0 && waitpid(pid, &st, WNOHANG) == pid) {
                if (WIFSIGNALED(st))
                    fprintf(stderr, "jepsen: server %d was killed by signal %d\n", (int)pid, WTERMSIG(st));
                else
                    fprintf(stderr, "jepsen: server %d exited with %d\n", (int)pid, WEXITSTATUS(st));
                atomic_store(&server_pid, 0);
            } else {
                fprintf(stderr, "jepsen: server %d is still running\n", (int)pid);
            }
            stop_server(SIGTERM); /* no orphan server */
            exit(2);
        }
        for (size_t i = 0; i < p.n; i++) {
            int64_t k = (p.ts[i] - T0) / KEY_MS;
            if (k >= 0 && k < NKEYS)
                final[s][k] = (uint32_t)p.values[i];
        }
        free(p.ts);
        free(p.values);
    }
    tsdbc_close(c);
}

int main(int argc, char **argv)
{
    static struct option opts[] = {{"dir", required_argument, 0, 'd'},     {"nemesis", required_argument, 0, 'n'},
                                   {"seconds", required_argument, 0, 't'}, {"clients", required_argument, 0, 'c'},
                                   {"seed", required_argument, 0, 's'},    {"crashfs", required_argument, 0, 'f'},
                                   {0, 0, 0, 0}};
    int ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': dir = optarg; break;
        case 'n': nemesis = optarg; break;
        case 't': seconds = atof(optarg); break;
        case 'c': nclients = atoi(optarg); break;
        case 's': seed = strtoull(optarg, NULL, 10); break;
        case 'f': crashfs = optarg; break;
        default: return 2;
        }
    }
    bool shim = !strcmp(nemesis, "power") || !strcmp(nemesis, "eio");
    if (!dir || (shim && !crashfs)) {
        fprintf(stderr, "usage: jepsen --dir DIR [--nemesis kill|power|pause|eio|none] [--seconds S] [--clients N]\n"
                        "              [--seed N] [--crashfs build/libcrashfs.so]\n");
        return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    port = 10000 + (int)(getpid() % 20000); /* below the ephemeral range, where client sockets take ports */
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 2;
    snprintf(markpath, sizeof markpath, "%s.eio", dir);
    writes = calloc(MAX_OPS, sizeof *writes);
    reads = calloc(MAX_OPS, sizeof *reads);

    start_server(false); /* the series exist before any fault */
    tsdbc *c = connect_retry();
    for (int s = 0; s < NSERIES; s++) {
        char name[32];
        snprintf(name, sizeof name, "jepsen.%d", s);
        if (tsdbc_series(c, name, 1, &ids[s])) {
            fprintf(stderr, "jepsen: cannot create series\n");
            return 2;
        }
    }
    tsdbc_close(c);
    if (shim) { /* from here on, file operations may lose power or fail */
        pthread_mutex_lock(&server_lock);
        stop_server(SIGTERM);
        start_server(true);
        pthread_mutex_unlock(&server_lock);
    }

    pthread_t th[256], nem;
    for (int i = 0; i < nclients; i++)
        pthread_create(&th[i], NULL, client, (void *)(intptr_t)i);
    if (strcmp(nemesis, "none"))
        pthread_create(&nem, NULL, nemesis_thread, NULL);
    usleep((useconds_t)(seconds * 1e6));
    atomic_store(&stop, true);
    if (strcmp(nemesis, "none"))
        pthread_join(nem, NULL);
    for (int i = 0; i < nclients; i++)
        pthread_join(th[i], NULL);

    /* the final state, from a server without faults, and again after a clean restart */
    pthread_mutex_lock(&server_lock);
    pid_t pid = atomic_load(&server_pid);
    int st;
    if (pid > 0 && waitpid(pid, &st, WNOHANG) == pid)
        atomic_store(&server_pid, 0);
    stop_server(strcmp(nemesis, "eio") ? SIGKILL : SIGTERM); /* eio: the shim rolls back at a normal exit */
    start_server(false);
    static uint32_t final[NSERIES][NKEYS], again[NSERIES][NKEYS];
    final_state(final);
    stop_server(SIGTERM);
    start_server(false);
    final_state(again);
    stop_server(SIGTERM);
    pthread_mutex_unlock(&server_lock);

    uint32_t nw = atomic_load(&nwrites) - 1, nr = atomic_load(&nreads);
    unsigned ok = 0, info = 0, fail = 0, rok = 0;
    for (uint32_t w = 1; w <= nw && w < MAX_OPS; w++)
        ok += writes[w].st == ST_OK, info += writes[w].st == ST_INFO, fail += writes[w].st == ST_FAIL;
    for (uint32_t i = 0; i < nr && i < MAX_OPS; i++)
        rok += reads[i].st == ST_OK;
    printf("jepsen %s: %.0f s, %d clients, %d restarts; writes %u ok / %u indeterminate / %u failed, reads %u ok\n",
           nemesis, seconds, nclients, restarts, ok, info, fail, rok);
    if (memcmp(final, again, sizeof final) != 0)
        VIOLATION("the state changed across a clean restart\n");
    int v = check(final);
    if (!strcmp(nemesis, "eio")) {
        printf("eio: %d fail-stops\n", failstops);
        v += eio_problems;
    }
    unlink(markpath);
    if (system(cmd) != 0)
        return 2;
    if (v) {
        printf("FAILED: %d violations\n", v);
        return 1;
    }
    printf("valid\n");
    return 0;
}
