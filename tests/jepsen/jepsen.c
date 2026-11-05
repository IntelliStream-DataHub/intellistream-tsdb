/*
 * A Jepsen-style test of tsdbd: clients write and read through the network
 * while a nemesis crashes the server, and a checker verifies the history.
 *
 *   jepsen --dir DIR [--nemesis kill|power|pause|eio|misdirect|rot|none] [--seconds 20]
 *          [--clients 8] [--seed N] [--crashfs build/libcrashfs.so] [--workload kv|classes]
 *          [--verify-checksums] [--auth]
 *
 * Login (--auth): before the first server, the CLI (JEPSEN_TSDB, default ./build/tsdb) makes USERS in DIR: a
 * superuser, which tsdbd needs to start, and the user `kv` with read and write on prefix:jepsen., as which every
 * client and follower reader logs in, at every reconnect: restarts, USERS read again at each open (after a power
 * loss too) and logins after them are in the history. The servers run without --no-auth. Password files beside DIR.
 *
 * Model. Each series is a set of keys (timestamps) holding the id of the write
 * that set them, last write wins. Every write has a unique id, stored as the
 * value, so the final state and every read say which write they come from.
 * Writes are batches of 1 to 3000 rows on random keys; the server is started
 * with small partitions and a small WAL threshold, so batches take both write
 * paths and many of them span several partitions (parts).
 *
 * Nemeses: kill (SIGKILL the server), power (the crashfs shim rolls the
 * directory back to what was fsync'd, then the server dies; every other
 * server with CRASHFS_TEAR, which keeps torn tails, old-or-new sectors and a
 * random subset of unsynced directory operations), pause
 * (SIGSTOP/SIGCONT), eio (the crashfs shim fails one write or sync with EIO
 * and from then on lies about that file's syncs, as Linux may after a failed
 * writeback; at exit it rolls back what was not made durable). After an EIO
 * on anything already relied upon (the WAL, MANIFEST, a dictionary, a volume's META,
 * the directory) the server must stop by itself with exit status 1
 * (fail-stop); after one on an unpublished part (tmp_*) or intent (txn_*) it
 * may stop or carry on, and then loses power (or, if idle, is stopped with
 * SIGTERM).
 * After each fault the server is restarted.
 *
 * Damage: misdirect and rot. The claim: tsdb answers correctly or reports the damage (a query or write fails with
 * TSDB_ERR_CORRUPT, the open is refused, or the server stops), and never gives a silent wrong answer or loses an
 * acknowledged write without saying so. A run is a series of epochs (at most EPOCH_S each), each a child process
 * on a fresh directory and its own seed, since a database refused for damage stays refused. misdirect: the crashfs
 * shim in CRASHFS_MODE=misdirect sends a write or read of a database file (rate JEPSEN_MD_RATE, default 0.0003 per
 * call) to another offset or another file, and replaces a 512 or 4096 byte chunk of some mappings of a part with
 * one from elsewhere (a page misread after eviction); every fault is a line in DIR.eN.faults. With
 * --verify-checksums (tsdbd also gets --pin-index, which keeps each part's index as the open checked it) every
 * file is covered. Without it granule payloads and indexes damaged after the open are known to be unprotected: epochs
 * alternate between faults in everything but parts (which must hold) and in parts only (wrong answers there are
 * counted as known, a crash still fails the run). rot: while the server is down, 1-3 bits are flipped in a random
 * database file, or it is truncated; then the server starts again. Damage to the last record of the WAL or an
 * append-only dictionary looks like a torn append and is cut: writes lost that way, when the cut file is one the
 * nemesis damaged, are counted as "lost to a tail cut after damage", a known limit; every other loss is a
 * violation. The summary gives faults injected, detected, harmless and violations per kind of epoch.
 *
 * Workloads. kv: 4 series, keys every 2 s. classes: partition classes at work. 8 series whose rate changes every
 * few seconds of their timestamps (a row every 1, 3, 8 or 50 ms), written mostly near a frontier that moves
 * through time as the run goes (some series also write anywhere into the past), on a server with small
 * granules and partitions, flushes every 500 ms and idle merges after 1 s: fresh series get their class from
 * their first rows, a series that speeds up moves to a shorter class at a flush, one that slows down to a
 * longer one at the idle merge of its partition, while large inserts, replaces and deletes race those switches
 * and the nemesis interrupts them. Checks 1-4 as below, and at the end, on the database opened in-process:
 * classes did switch (at least one switch, parts in two classes), and after tsdb_optimize (one part per
 * partition) the parts hold exactly as many rows as the final state has keys: no (series, time) in two
 * partitions, whose copies a query would otherwise rank by block numbers that no merge keeps in order across
 * partitions.
 *
 * Power runs also start a follower: a second tsdbd with --follow over the same
 * directory, outside the shim, as a query server on another host would be. It
 * lives across every writer restart, so it tails whatever a power loss leaves
 * behind (torn and rolled back WAL tails, intents, half-written MANIFESTs).
 * Its reads may be stale, but each value they show must be valid (as in 2,
 * and from a write that had begun); it must not die, its reads must not keep
 * failing for FOLLOW_FAIL_S, and at the end it must catch up with the final
 * state.
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
 *     keys inside its range, w or something not older than w;
 *  5. real-time order through what a read saw: a read R that shows, at a key
 *     w covers, a value from a write that completed before w began saw w not
 *     yet in effect, so every write acknowledged before R began is older than
 *     w although the two overlap in time; 1 and 4 hold for those pairs too (a
 *     small write acknowledged while a large write was between its memtable
 *     flush and its publish once kept winning over it).
 * Deletes and replaces clear a key range of one series (a replace writes its
 * keys in that range in the same step). A key may then be absent where the
 * checks above want a value, if a delete covering it that did not definitely
 * fail could be ordered after that value (did not complete before it began,
 * and began before the read completed). An applied delete is a write to every
 * key of its range: 1 and 4 hold for it with "absent" as its value, and a read
 * that sees a replace shows nothing older than it in its range (3).
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../harness.h"

#define NSERIES 4        /* kv */
#define NKEYS 6000
#define T0 1700000000000LL
#define KEY_MS 2000      /* keys every 2 s: 6000 keys span 3.3 hours */
#define PARTITION_MS 3600000LL
#define MAX_SERIES 8
/* classes: base partitions of 2 s and granules of 512 rows, so a partition wants 4096 rows of a series: a row
 * every 3.4 ms or faster gives class 1 (14 s), up to 13.7 ms class 2 (56 s), slower class 3 (728 s); a
 * series speeding up from class 3 moves at a flush at a row every 6.8 ms or faster. */
#define CL_SERIES 8
#define CL_PARTITION_MS 2000
#define CL_GRANULE_ROWS 512
#define CL_SEG_MS 6000   /* a series keeps its rate for 6 s of timestamps */
#define CL_SPEED 2       /* ms of timestamps the frontier moves per ms of the run */
#define CL_NEAR 200      /* keys behind the frontier most writes go to */
#define MAX_OPS 2000000
#define MAX_FREADS 200000
#define FOLLOW_FAIL_S 10 /* longest run of failed follower reads */
#define FOLLOW_CATCHUP_S 20

typedef enum { ST_PENDING, ST_OK, ST_FAIL, ST_INFO } status;

enum { W_INSERT, W_DELETE, W_REPLACE };

typedef struct wop {
    int series;
    int kind;       /* W_*: a delete or replace also clears [lo, hi) of the series */
    uint32_t lo, hi;
    uint32_t *keys;
    uint32_t nkeys;
    int64_t invoke, complete; /* ns */
    status st;
} wop;

typedef struct rop {
    int series;
    uint32_t lo, hi;   /* key range [lo, hi) */
    uint32_t *seen;    /* write id per key in range, 0 = absent */
    bool stray;        /* a row at a timestamp of no key (kept apart: seen[] holds write ids only) */
    int64_t invoke, complete;
    status st;
} rop;

static const char *dir, *crashfs, *nemesis = "kill";
static int nclients = 8;
static atomic_int port; /* where the current server listens, 0 while none runs: tsdbd picks it, see start_server */
static double seconds = 20;
static uint64_t seed = 1;
static tsdb_sid ids[MAX_SERIES];
static bool classes;                /* --workload classes */
static bool auth;                   /* --auth: the servers require a login, the clients log in as KV_USER */
static char kv_pw[PATH_MAX];        /* KV_USER's password file, beside DIR */
#define KV_USER "kv"
static int nseries = NSERIES;
static uint32_t nkeys[MAX_SERIES];  /* keys of a series */
static size_t koff[MAX_SERIES + 1]; /* where its keys start in the checker's flat arrays */
static int64_t *kts[MAX_SERIES];    /* the timestamp of each key, increasing */
static int64_t run_start;           /* ns, when the clients start: the frontier moves from there */

static wop *writes; /* indexed by write id; id 0 unused */
static atomic_uint nwrites = 1;
static rop *reads;
static atomic_uint nreads;
static atomic_bool stop;
static atomic_int server_pid;
static pthread_mutex_t server_lock = PTHREAD_MUTEX_INITIALIZER;
static int restarts;
static char markpath[PATH_MAX]; /* eio: crashfs writes the path of the file it failed here */
static char slogpath[PATH_MAX]; /* the stderr of the server's latest start */

/* The last lines a file holds (a server's log), to stderr: why it did not come up. */
static void print_tail(const char *path)
{
    char b[4096];
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return;
    off_t end = lseek(fd, 0, SEEK_END), at = end > (off_t)sizeof b ? end - (off_t)sizeof b : 0;
    ssize_t n = pread(fd, b, sizeof b, at);
    close(fd);
    if (n > 0) {
        fprintf(stderr, "jepsen: its log (%s) ends:\n", path);
        fwrite(b, 1, (size_t)n, stderr);
    }
}
static int failstops, eio_problems;
static int lives_faulty, lives_clean, lives_died_starting; /* servers started under the nemesis, by kind */
static int death_problems;       /* power: a server that ended any way but a power loss (exit 99) or our kill */
static bool server_faulty;       /* the current server runs under the shim (set under server_lock or at start) */
static int fport;                   /* the follower, where tsdbd put it; 0 = none */
static atomic_int follower_pid;
static char flogpath[PATH_MAX];     /* its stderr */
static rop *freads;
static atomic_uint nfreads;
static int64_t ffail_longest;       /* ns, the longest run of failed follower reads */
/* misdirect and rot: damage the database; each epoch (a forked child) runs on a fresh directory and ends early once
 * the damage is persistent (even a server without faults refuses the directory or fails the final read) */
static bool md, rot;                /* --nemesis misdirect / rot */
static bool verify;                 /* --verify-checksums: tsdbd checks granule payloads */
static int epoch;
static const char *md_files = "all"; /* CRASHFS_MD_FILES of this epoch */
static char faultlog[PATH_MAX];     /* every injected fault, one line each (the shim's and rot's) */
static bool damaged;                /* the database is refused or unreadable: final state unknown */
static int refused_faulty, refused_clean, md_failstops, md_crashes, tail_cuts;
static atomic_int read_damage, write_damage; /* requests that failed with TSDB_ERR_CORRUPT or TSDB_ERR_IO */
static int loss_violations;         /* of the violations, those of checks 1, 3 and 4 (a write lost or not seen) */

#define CL_STR_(x) #x
#define CL_STR(x) CL_STR_(x)

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

/* -------------------------------------------------------------- keys */

/* Keys of a series are its timestamps in order. kv: every 2 s. classes: in segments of CL_SEG_MS, each with its
 * own spacing, in an order that differs per series, so each changes its rate several times in a run. */
static void make_keys(void)
{
    static const int spacing[] = {1, 50, 8, 1, 3, 50};
    int64_t span = (int64_t)(seconds * 1000) * CL_SPEED + 2 * CL_SEG_MS;
    for (int s = 0; s < nseries; s++) {
        size_t n = 0, cap = classes ? 1024 : NKEYS;
        kts[s] = malloc(cap * sizeof *kts[s]);
        for (int64_t t = T0; classes ? t < T0 + span : n < NKEYS;) {
            if (n == cap)
                kts[s] = realloc(kts[s], (cap *= 2) * sizeof *kts[s]);
            kts[s][n++] = t;
            t += classes ? spacing[((t - T0) / CL_SEG_MS + (uint64_t)s * 5 / 2) % 6] : KEY_MS;
        }
        nkeys[s] = (uint32_t)n;
        koff[s + 1] = koff[s] + n;
    }
}

/* [key_ts(lo), key_end(hi)) holds keys lo..hi-1 */
static int64_t key_ts(int s, uint32_t k) { return kts[s][k]; }
static int64_t key_end(int s, uint32_t k) { return k < nkeys[s] ? kts[s][k] : kts[s][nkeys[s] - 1] + 1; }

/* The key at ts, or -1 if no key has it. */
static int64_t ts_key(int s, int64_t ts)
{
    uint32_t lo = 0, hi = nkeys[s];
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (kts[s][mid] < ts)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < nkeys[s] && kts[s][lo] == ts ? (int64_t)lo : -1;
}

/* classes: keys below the frontier, which moves CL_SPEED ms of timestamps per ms of the run; kv: every key. */
static uint32_t front(int s)
{
    if (!classes)
        return nkeys[s];
    int64_t ft = T0 + (now_ns() - run_start) / 1000000 * CL_SPEED;
    uint32_t lo = 0, hi = nkeys[s];
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (kts[s][mid] < ft)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < 300 ? (nkeys[s] < 300 ? nkeys[s] : 300) : lo;
}

/* Where a key range starts. classes: mostly just behind the frontier (`near` of 100), else anywhere before it;
 * kv: anywhere, as x % NKEYS always did. */
static uint32_t pick_key(int s, uint64_t x, uint64_t y, int near)
{
    uint32_t f = front(s);
    if (!classes || (int)(y % 100) >= near)
        return (uint32_t)(x % f);
    uint32_t w = f < CL_NEAR ? f : CL_NEAR;
    return f - w + (uint32_t)(x % w);
}

/* The end of keys a write may use: a little past the frontier. */
static uint32_t key_limit(int s)
{
    uint32_t f = front(s);
    return !classes || f + 50 > nkeys[s] ? nkeys[s] : f + 50;
}

/* Series that write into the past as well (classes: the odd ones; their memtables then hold old rows too). */
static int near_pct(int s) { return s % 2 ? 70 : 97; }

/* ----------------------------------------------------------- the server */

static void stop_server(int sig);
/* No server runs: clients must not try the old port, which anything may take now. */
static void server_gone(void)
{
    atomic_store(&server_pid, 0);
    atomic_store(&port, 0);
}
static const char *tsdbd_path(void) { return getenv("JEPSEN_TSDBD") ? getenv("JEPSEN_TSDBD") : "./build/tsdbd"; }

static void note_death(int st, bool we_killed, pid_t pid);

static void start_server(bool faulty)
{
    char pms[32], pf[PATH_MAX];
    snprintf(pms, sizeof pms, "%lld", classes ? (long long)CL_PARTITION_MS : PARTITION_MS);
    atomic_store(&port, 0); /* until this server says where it listens, the clients have nowhere to go */
    for (int attempt = 0;; attempt++) {
        unlink(markpath); /* an EIO while opening: the clean server of the second attempt has no fault */
        th_portfile(pf, sizeof pf, dir);
        char follow[64];
        th_follow_me(follow, sizeof follow);
        pid_t pid = fork();
        if (pid == 0) {
            if (faulty && crashfs && attempt == 0) {
                char sd[32];
                snprintf(sd, sizeof sd, "%llu", (unsigned long long)mix(seed + (uint64_t)restarts));
                setenv("LD_PRELOAD", crashfs, 1);
                setenv("CRASHFS_ROOT", dir, 1);
                setenv("CRASHFS_SEED", sd, 1);
                setenv("CRASHFS_PROB", "0.0005", 1);
                setenv("CRASHFS_WARMUP", "200", 1);
                if (!strcmp(nemesis, "power") && restarts % 2 == 1)
                    setenv("CRASHFS_TEAR", "1", 1); /* every other server: keep what a real disk may keep */
                if (md) {
                    setenv("CRASHFS_MODE", "misdirect", 1);
                    /* low enough that a server lives a while between faults (0.002 ended most epochs at once) */
                    setenv("CRASHFS_MD_RATE", getenv("JEPSEN_MD_RATE") ? getenv("JEPSEN_MD_RATE") : "0.0003", 1);
                    setenv("CRASHFS_MD_FILES", md_files, 1);
                    setenv("CRASHFS_MD_LOG", faultlog, 1);
                }
                if (!strcmp(nemesis, "eio")) {
                    setenv("CRASHFS_MODE", "eio", 1);
                    setenv("CRASHFS_MARK", markpath, 1);
                    if (restarts % 2 == 0) /* every other server: the WAL, where an error must stop it */
                        setenv("CRASHFS_EIO_ONLY", "wal_", 1);
                }
            }
            if (!getenv("JEPSEN_VERBOSE")) { /* kept for a server that does not come up */
                int lfd = open(slogpath, O_WRONLY | O_CREAT | (md || rot ? O_APPEND : O_TRUNC), 0644);
                if (lfd < 0)
                    lfd = open("/dev/null", O_WRONLY);
                if (lfd >= 0)
                    dup2(lfd, 2);
            }
            const char *av[32] = {"tsdbd", "--dir", dir, "--port", "0", "--port-file", pf, "--workers", "8",
                                  "--wal-max-batch-rows", "500", "--partition-ms", pms};
            int ac = 13;
            av[ac++] = follow; /* a killed run leaves no server behind */
            if (!auth)
                av[ac++] = "--no-auth";
            if (classes) {
                av[ac++] = "--granule-rows", av[ac++] = CL_STR(CL_GRANULE_ROWS);
                av[ac++] = "--merge-idle-s", av[ac++] = "1";
                av[ac++] = "--flush-interval-ms", av[ac++] = "500";
            }
            if (getenv("TSDBD_IO"))
                av[ac++] = "--io", av[ac++] = getenv("TSDBD_IO");
            if (verify) /* both: payloads checked, and each part's index used as the open checked it */
                av[ac++] = "--verify-checksums", av[ac++] = "--pin-index";
            av[ac] = NULL;
            execv(tsdbd_path(), (char **)av);
            _exit(127);
        }
        atomic_store(&server_pid, pid);
        server_faulty = attempt == 0 && faulty && crashfs;
        if (faulty && crashfs && !server_faulty)
            lives_clean++;
        if (getenv("JEPSEN_VERBOSE"))
            fprintf(stderr, "jepsen: started %d (%s)\n", (int)pid, attempt == 0 && faulty ? "faulty" : "clean");
        int p = th_await(pf, pid, 30000);
        unlink(pf);
        if (p > 0) {
            atomic_store(&port, p);
            if (server_faulty)
                lives_faulty++;
            return;
        }
        if (p == TH_EXITED && (md || rot)) {
            /* refused: the open found damage (exit 1), or the faulty server stopped on damage it read; a signal
             * is a crash, never an answer to damage */
            server_gone();
            if (WIFSIGNALED(th_status)) {
                md_crashes++;
                fprintf(stderr, "  the %s server (pid %d) died of signal %d while opening\n",
                        server_faulty ? "faulty" : "clean", (int)pid, WTERMSIG(th_status));
                print_tail(slogpath);
            }
            if (server_faulty) { /* a transient fault, maybe: a clean server next */
                refused_faulty++;
                lives_died_starting++;
                continue;
            }
            refused_clean++;
            damaged = true; /* persistent: the epoch ends here */
            atomic_store(&stop, true);
            return;
        }
        if (p == TH_EXITED) {
            server_gone();
            if (faulty && crashfs && attempt == 0) { /* crashed while starting (power loss at open): a clean one */
                lives_died_starting++;
                if (!strcmp(nemesis, "power"))
                    note_death(th_status, false, pid);
                if (getenv("JEPSEN_VERBOSE"))
                    fprintf(stderr, "jepsen: %d died while starting\n", (int)pid);
                continue;
            }
        }
        fprintf(stderr, "jepsen: the server does not come up: %s", th_why);
        print_tail(slogpath);
        stop_server(SIGKILL);
        exit(2);
    }
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
        server_gone();
    }
    atomic_store(&port, 0);
}

/* ----------------------------------------------------------- the follower */

static void start_follower(void)
{
    char pf[PATH_MAX];
    th_portfile(pf, sizeof pf, dir);
    char follow[64];
    th_follow_me(follow, sizeof follow);
    pid_t pid = fork();
    if (pid == 0) { /* never under the shim: it plays a reader on another host */
        unsetenv("LD_PRELOAD");
        int log = open(flogpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log >= 0)
            dup2(log, 2);
        const char *av[16] = {"tsdbd", "--dir", dir, "--port", "0", "--port-file", pf, "--workers", "2", "--follow"};
        int ac = 10;
        av[ac++] = follow;
        if (!auth)
            av[ac++] = "--no-auth";
        if (getenv("TSDBD_IO"))
            av[ac++] = "--io", av[ac++] = getenv("TSDBD_IO");
        av[ac] = NULL;
        execv(tsdbd_path(), (char **)av);
        _exit(127);
    }
    atomic_store(&follower_pid, pid);
    int p = th_await(pf, pid, 30000);
    unlink(pf);
    if (p > 0) {
        fport = p;
        return;
    }
    fprintf(stderr, "jepsen: the follower does not come up (see %s): %s", flogpath, th_why);
    if (p != TH_EXITED) {
        signal_pid(pid, SIGKILL);
        waitpid(pid, NULL, 0);
    }
    atomic_store(&follower_pid, 0);
    stop_server(SIGKILL);
    exit(2);
}

/* Reads from the follower; any outcome but a failure is fine here, check_follower() judges the values. */
static void kill_follower(void) /* at exit(2): no orphan */
{
    pid_t pid = atomic_load(&follower_pid);
    if (pid > 0 && signal_pid(pid, SIGKILL) == 0)
        waitpid(pid, NULL, 0);
}

/* Connects to the server this harness started, and only to it: the port may have been given to another process
 * since that server died (the nemesis notices a crash only at its next tick), so after connecting the socket
 * has to belong to `pid`. A dead server holds no socket, which makes this fail until the next one is up. */
static int connect_own(int p, pid_t pid, tsdbc **c)
{
    tsdbc_options o = {.user = auth ? KV_USER : NULL, .password_file = auth ? kv_pw : NULL, .require_auth = auth};
    if (p <= 0 || pid <= 0 || tsdbc_connect_opts("127.0.0.1", p, &o, c) != 0)
        return -1;
    if (!th_owns_port(pid, p)) {
        tsdbc_close(*c);
        *c = NULL;
        return -1;
    }
    return 0;
}

static void *follower_reader(void *arg)
{
    (void)arg;
    uint64_t r = mix(seed * 7919 + 5);
    tsdbc *c = NULL;
    int64_t fail_since = 0;
    while (!atomic_load(&stop)) {
        bool ok = false;
        if (c || connect_own(fport, atomic_load(&follower_pid), &c) == 0) {
            uint32_t idx = atomic_fetch_add(&nfreads, 1);
            if (idx >= MAX_FREADS)
                break;
            rop *q = &freads[idx];
            r = mix(r);
            q->series = (int)(r % (uint64_t)nseries);
            q->lo = pick_key(q->series, r >> 12, r >> 50, 50);
            q->hi = q->lo + 1 + (uint32_t)((r >> 30) % 400);
            if (q->hi > nkeys[q->series])
                q->hi = nkeys[q->series];
            q->seen = calloc(q->hi - q->lo, sizeof *q->seen);
            tsdb_points p;
            q->invoke = now_ns();
            int rc = tsdbc_query_points(c, ids[q->series], key_ts(q->series, q->lo), key_end(q->series, q->hi), &p);
            q->complete = now_ns();
            if (rc == 0) {
                ok = true;
                q->st = ST_OK;
                for (size_t i = 0; i < p.n; i++) {
                    int64_t k = ts_key(q->series, p.ts[i]);
                    if (k >= q->lo && k < q->hi)
                        q->seen[k - q->lo] = (uint32_t)p.values[i];
                    else
                        q->stray = true;
                }
                free(p.ts);
                free(p.values);
            } else {
                q->st = ST_FAIL;
                if (getenv("JEPSEN_VERBOSE") && !fail_since)
                    fprintf(stderr, "jepsen: follower read failed: %s\n", tsdbc_strerror(rc));
                tsdbc_close(c);
                c = NULL;
            }
        } else {
            c = NULL;
        }
        int64_t now = now_ns();
        if (ok && fail_since) {
            if (now - fail_since > ffail_longest)
                ffail_longest = now - fail_since;
            fail_since = 0;
        } else if (!ok && !fail_since) {
            fail_since = now;
        }
        usleep(ok ? 2000 : 10000); /* a reader, not a load: MAX_FREADS lasts for minutes */
    }
    if (fail_since && now_ns() - fail_since > ffail_longest)
        ffail_longest = now_ns() - fail_since;
    if (c)
        tsdbc_close(c);
    return NULL;
}

/* ------------------------------------------------------------ clients */

static tsdbc *connect_retry(void)
{
    for (;;) {
        tsdbc *c;
        if (connect_own(atomic_load(&port), atomic_load(&server_pid), &c) == 0)
            return c;
        if (atomic_load(&stop))
            return NULL;
        usleep(2000); /* a server that crashes a few ms after it listens leaves little time to be found */
    }
}

static void *client(void *arg)
{
    uint64_t r = mix(seed * 1000 + (uint64_t)(intptr_t)arg);
    tsdbc *c = connect_retry();
    while (c && !atomic_load(&stop)) {
        r = mix(r);
        int s = (int)(r % (uint64_t)nseries);
        if ((r >> 8) % 100 < 6) { /* a delete or a replace of a key range */
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= MAX_OPS)
                break;
            wop *w = &writes[id];
            w->series = s;
            w->kind = (r >> 16) % 2 ? W_REPLACE : W_DELETE;
            w->lo = pick_key(s, mix(r + 3), mix(r + 6), 50);
            w->hi = w->lo + 1 + (uint32_t)(mix(r + 4) % 200);
            if (w->hi > key_limit(s))
                w->hi = key_limit(s) > w->lo ? key_limit(s) : w->lo + 1;
            uint32_t n = w->kind == W_REPLACE ? (w->hi - w->lo) : 0, stride = 1 + (uint32_t)(mix(r + 5) % 4), m = 0;
            w->keys = malloc((n ? n : 1) * sizeof *w->keys);
            int64_t *ts = malloc((n ? n : 1) * sizeof *ts);
            float *v = malloc((n ? n : 1) * sizeof *v);
            for (uint32_t k = w->lo; k < w->lo + n; k += stride) {
                w->keys[m] = k;
                ts[m] = key_ts(s, k);
                v[m++] = (float)id;
            }
            w->nkeys = m;
            tsdb_values col = {TSDB_FLOAT32, v, NULL};
            int64_t a = key_ts(s, w->lo), b = key_end(s, w->hi);
            w->invoke = now_ns();
            int rc = w->kind == W_REPLACE ? tsdbc_replace_range(c, ids[s], a, b, ts, &col, m)
                                          : tsdbc_delete_range(c, ids[s], a, b);
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            free(ts);
            free(v);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) {
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&write_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        } else if ((r >> 8) % 100 < 60) { /* a write */
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
            uint32_t lim = key_limit(s), stride = 1 + (uint32_t)(mix(r + 2) % 7);
            uint32_t start = pick_key(s, mix(r + 1), mix(r + 7), near_pct(s)), m = 0;
            if (classes && n > 20 && (mix(r + 7) % 100) < (uint64_t)near_pct(s)) /* large: ending at the frontier */
            {
                uint32_t span = n * stride, jitter = (uint32_t)(mix(r + 1) % CL_NEAR);
                start = span + jitter < lim ? lim - span - jitter : 0;
            }
            for (uint32_t i = 0; i < n; i++) {
                uint32_t k = (start + i * stride) % lim;
                bool dup = false;
                for (uint32_t j = 0; j < m && n <= 20; j++)
                    dup |= w->keys[j] == k;
                if (dup || (n > 20 && i * stride >= lim))
                    continue;
                w->keys[m] = k;
                ts[m] = key_ts(s, k);
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
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&write_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        } else { /* a read of a key range */
            uint32_t idx = atomic_fetch_add(&nreads, 1);
            if (idx >= MAX_OPS)
                break;
            rop *q = &reads[idx];
            q->series = s;
            q->lo = pick_key(s, r >> 12, r >> 50, 50);
            q->hi = q->lo + 1 + (uint32_t)((r >> 30) % 400);
            if (q->hi > nkeys[s])
                q->hi = nkeys[s];
            q->seen = calloc(q->hi - q->lo, sizeof *q->seen);
            tsdb_points p;
            q->invoke = now_ns();
            int rc = tsdbc_query_points(c, ids[s], key_ts(s, q->lo), key_end(s, q->hi), &p);
            q->complete = now_ns();
            if (rc == 0) {
                q->st = ST_OK;
                for (size_t i = 0; i < p.n; i++) {
                    int64_t k = ts_key(s, p.ts[i]);
                    if (k >= q->lo && k < q->hi)
                        q->seen[k - q->lo] = (uint32_t)p.values[i];
                    else
                        q->stray = true; /* a row at no key: invalid */
                }
                free(p.ts);
                free(p.values);
            } else {
                q->st = ST_FAIL;
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&read_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        }
    }
    tsdbc_close(c);
    return NULL;
}

/* ------------------------------------------------------------- nemesis */

/* Sleeps `us`; with a fault-injecting server (power) it ends early once the server has crashed by itself, which
 * the shim does a few ms after the listener is up (the fault is due after a fixed count of file operations).
 * Sleeping through it left the server dead for most of the run and the clients idle. The process is not reaped
 * here (WNOWAIT): the nemesis below does that. */
static void nap(useconds_t us, bool watch)
{
    for (useconds_t t = 0; t < us && !atomic_load(&stop); t += 5000) {
        pid_t pid = atomic_load(&server_pid);
        siginfo_t si;
        si.si_pid = 0;
        if (watch && pid > 0 && waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) == 0 && si.si_pid == pid)
            return;
        usleep(5000);
    }
}

/* How a power-nemesis server may end: crashfs's power loss is _exit(99); a plain kill of ours (an idle server);
 * SIGUSR2 on a server without the shim (the clean one after a failed start, which has no handler). Anything
 * else, a SIGSEGV or abort() in the server above all, is a crash bug that the restarts would otherwise hide. */
static void note_death(int st, bool we_killed, pid_t pid)
{
    bool ok = (WIFEXITED(st) && WEXITSTATUS(st) == 99 && server_faulty) ||
              (we_killed && WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL) ||
              (WIFSIGNALED(st) && WTERMSIG(st) == SIGUSR2 && !server_faulty);
    if (ok)
        return;
    death_problems++;
    fprintf(stderr, "  the %s server (pid %d) ended with status %#x, not a power loss (exit 99) or our kill\n",
            server_faulty ? "faulty" : "clean", (int)pid, st);
}

/* misdirect and rot: how a server ended. Exit 1 is a stop on damage it found (fail-stop, or an open refused);
 * exit 0 after our SIGTERM and our SIGKILL are ours; a signal of its own (SIGSEGV, SIGABRT...) is a crash, which
 * damage must never cause. */
static void md_death(int st, int sig, pid_t pid)
{
    if ((sig && WIFSIGNALED(st) && WTERMSIG(st) == sig) || (sig == SIGTERM && WIFEXITED(st) && WEXITSTATUS(st) == 0))
        return;
    if (WIFEXITED(st)) {
        md_failstops++;
        return;
    }
    md_crashes++;
    fprintf(stderr, "  the server (pid %d) ended with status %#x: a crash\n", (int)pid, st);
    print_tail(slogpath);
}

static void fault_note(const char *fmt, ...)
{
    FILE *f = fopen(faultlog, "a");
    if (!f)
        return;
    va_list ap;
    va_start(ap, fmt);
    fprintf(f, "jepsen restart %d: ", restarts);
    vfprintf(f, fmt, ap);
    fputc('\n', f);
    va_end(ap);
    fclose(f);
}

/* Files the engine cut as torn tails ("tsdb: NAME: cutting a torn tail"), from the server log since the last look:
 * at-rest damage to the last record of an append-only file looks the same, and the checker tells those apart. */
static char cut_files[64][256];
static int ncut;
static long slog_seen;

static void scan_cuts(void)
{
    FILE *f = fopen(slogpath, "r");
    if (!f)
        return;
    char line[1024];
    if (fseek(f, slog_seen, SEEK_SET) != 0)
        rewind(f);
    while (fgets(line, sizeof line, f)) {
        char *at = strstr(line, ": cutting a torn tail");
        if (strncmp(line, "tsdb: ", 6) || !at)
            continue;
        tail_cuts++;
        *at = 0;
        if (ncut < 64)
            snprintf(cut_files[ncut++], sizeof cut_files[0], "%.255s", line + 6);
        fault_note("the engine cut a torn tail of %s", line + 6);
    }
    slog_seen = ftell(f);
    fclose(f);
}

/* rot: damage one file of the stopped server's directory, as a disk may at rest: 1 to 3 flipped bits (a third of
 * them in the last 64 bytes, where the newest record of an append-only file is), a truncation at a random offset,
 * or a few bytes cut off the end. Each kind of file is as likely as any other, however many parts there are. */
static void rot_damage(uint64_t r)
{
    static const char *const kinds[] = {"wal_", ".part", "MANIFEST", "SERIES", "STATES", "CLASSES", "VOLUMES",
                                        "RETENTION", "txn_", "META"};
    char pick[PATH_MAX] = "";
    for (int tries = 0; tries < 8 && !pick[0]; tries++) {
        const char *kind = kinds[(r = mix(r)) % (sizeof kinds / sizeof *kinds)];
        if (kind[0] == '.' && !verify) /* granule payloads are checked only with verify_checksums (documented) */
            continue;
        DIR *d = opendir(dir);
        unsigned seen = 0;
        struct dirent *e;
        while (d && (e = readdir(d)) != NULL) {
            size_t n = strlen(e->d_name);
            bool match = kind[0] == '.' ? n > 5 && !strcmp(e->d_name + n - 5, ".part") && strncmp(e->d_name, "tmp_", 4)
                                        : !strncmp(e->d_name, kind, strlen(kind));
            if (match && (seen++, mix(r + seen) % seen == 0))
                snprintf(pick, sizeof pick, "%s/%s", dir, e->d_name);
        }
        if (d)
            closedir(d);
    }
    struct stat st;
    if (!pick[0] || stat(pick, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0)
        return;
    r = mix(r);
    if (r % 4 < 2) {
        int fd = open(pick, O_RDWR);
        int nflip = 1 + (int)((r >> 8) % 3);
        for (int i = 0; fd >= 0 && i < nflip; i++) {
            uint64_t x = mix(r + (uint64_t)i);
            off_t at = (x >> 20) % 3 == 0 && st.st_size > 64 ? st.st_size - 64 + (off_t)(x % 64) : (off_t)(x % (uint64_t)st.st_size);
            unsigned char b;
            if (pread(fd, &b, 1, at) != 1)
                continue;
            b ^= (unsigned char)(1u << ((x >> 8) % 8));
            if (pwrite(fd, &b, 1, at) == 1)
                fault_note("rot flip %s (%lld bytes) at %lld bit %d", pick, (long long)st.st_size, (long long)at,
                           (int)((x >> 8) % 8));
        }
        if (fd >= 0)
            close(fd);
    } else {
        off_t to = r % 4 == 2 ? (off_t)((r >> 8) % (uint64_t)st.st_size)
                              : st.st_size - 1 - (off_t)((r >> 8) % (uint64_t)(st.st_size < 16 ? st.st_size : 16));
        if (truncate(pick, to) == 0)
            fault_note("rot truncate %s from %lld to %lld", pick, (long long)st.st_size, (long long)to);
    }
}

static void *nemesis_thread(void *arg)
{
    (void)arg;
    uint64_t r = mix(seed + 77);
    while (!atomic_load(&stop)) {
        r = mix(r);
        nap(150000 + (useconds_t)(r % 600000), (!strcmp(nemesis, "power") && crashfs) || md);
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
            bool killed = false;
            if (waitpid(pid, &st, WNOHANG) != pid) { /* not crashed on its own yet: pull the plug */
                signal_pid(pid, SIGUSR2);
                for (int i = 0; i < 100 && waitpid(pid, &st, WNOHANG) != pid; i++)
                    usleep(10000);
                if (signal_pid(pid, 0) == 0) { /* idle, no file operation to crash in: plain kill */
                    signal_pid(pid, SIGKILL);
                    waitpid(pid, &st, 0);
                    killed = true;
                }
            }
            note_death(st, killed, pid);
            server_gone();
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
                     * just the call; the WAL, MANIFEST, the dictionaries, a volume's META and the directory are
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
                server_gone();
                start_server(true);
                restarts++;
            } else if (gone) {
                eio_problems++;
                fprintf(stderr, "  the server ended (status %#x) without a fault\n", st);
                server_gone();
                start_server(true);
                restarts++;
            }
        } else if (md || rot) { /* a restart (SIGKILL or SIGTERM), over damage the shim or rot put on disk */
            pid_t pid = atomic_load(&server_pid);
            int st = 0, sig = (r >> 20) % 2 ? SIGKILL : SIGTERM;
            if (pid > 0) {
                bool gone = waitpid(pid, &st, WNOHANG) == pid;
                if (!gone) {
                    signal_pid(pid, sig);
                    waitpid(pid, &st, 0);
                }
                md_death(st, gone ? 0 : sig, pid);
                server_gone();
                if (rot)
                    rot_damage(r);
                start_server(md); /* the shim's faults go on while it opens; rot's server has none */
                scan_cuts();
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

/* check 5: per write, a time before which it had not taken effect (at least its start) */
static int64_t *lb;

/* w1 is older than w2 in real time or, if acknowledged, by what a read saw (check 5) */
static bool older_seen(uint32_t w1, uint32_t w2)
{
    return older(w1, w2) || (writes[w1].st == ST_OK && writes[w1].complete < lb[w2]);
}

static bool wrote_key(uint32_t w, uint32_t k)
{
    for (uint32_t i = 0; i < writes[w].nkeys; i++)
        if (writes[w].keys[i] == k)
            return true;
    return false;
}

/* deletes covering each key (koff[series] + key), as offsets into dlist */
static uint32_t *dcnt, *dlist;

/* Whether a delete covering the key, that did not definitely fail, could be
 * ordered after something that began at `after` and before a read that
 * completed at `before`: then the key may be absent. */
static bool absent_ok(int series, uint32_t k, int64_t after, int64_t before)
{
    size_t slot = koff[series] + k;
    for (uint32_t i = dcnt[slot]; i < dcnt[slot + 1]; i++) {
        const wop *d = &writes[dlist[i]];
        if (d->st != ST_FAIL && d->complete >= after && d->invoke <= before)
            return true;
    }
    return false;
}

static int check(const uint32_t *final)
{
    uint32_t nw = atomic_load(&nwrites), nr = atomic_load(&nreads);
    if (nw > MAX_OPS)
        nw = MAX_OPS;
    if (nr > MAX_OPS)
        nr = MAX_OPS;
    bool *applied = calloc(nw, sizeof *applied);
    dcnt = calloc(koff[nseries] + 1, sizeof *dcnt);
    size_t ndel = 0;
    for (uint32_t w = 1; w < nw; w++)
        for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++, ndel++)
            dcnt[koff[writes[w].series] + k + 1]++;
    for (size_t x = 1; x <= koff[nseries]; x++)
        dcnt[x] += dcnt[x - 1];
    dlist = malloc((ndel + 1) * sizeof *dlist);
    uint32_t *dfill = calloc(koff[nseries], sizeof *dfill);
    for (uint32_t w = 1; w < nw; w++)
        for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
            size_t slot = koff[writes[w].series] + k;
            dlist[dcnt[slot] + dfill[slot]++] = w;
        }
    free(dfill);
    /* applied: acknowledged, in the final state, or seen by a read */
    for (uint32_t w = 1; w < nw; w++)
        applied[w] = writes[w].st == ST_OK;
    for (int s = 0; final && s < nseries; s++) /* no final state: the database was refused (damage detected) */
        for (uint32_t k = 0; k < nkeys[s]; k++) {
            uint32_t w = final[koff[s] + k];
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
        if (q->stray) {
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
    /* 5. real-time order through what a read saw. A read that began at t and shows, at a key that write w covers
     * (its keys; a delete's or replace's range), a value from a write that completed before w began saw w not yet in
     * effect (w overwrites that value, and no write can bring it back): w takes effect after t. So every write
     * acknowledged before t is older than w, although the two may overlap in time, and must not outlive it at a key
     * both cover. lb[w] is the latest such t (at least w's start); checks 1 and 4 compare acknowledged values with
     * it (older_seen). A small write acknowledged while a large write was between its memtable flush and its publish
     * once kept winning over it, which only this check sees: the two overlap in time. Follower reads may be stale
     * and do not count. */
    lb = malloc(nw * sizeof *lb);
    for (uint32_t w = 0; w < nw; w++)
        lb[w] = w ? writes[w].invoke : INT64_MIN;
    {
        /* per key, the writes that cover it, by start time */
        uint32_t *wc = calloc(koff[nseries] + 1, sizeof *wc), *wf = calloc(koff[nseries], sizeof *wf);
        size_t nwk = 0;
        for (uint32_t w = 1; w < nw; w++) {
            const wop *o = &writes[w];
            if (o->st == ST_FAIL)
                continue;
            if (o->kind == W_INSERT)
                for (uint32_t j = 0; j < o->nkeys; j++, nwk++)
                    wc[koff[o->series] + o->keys[j] + 1]++;
            else
                for (uint32_t k = o->lo; k < o->hi; k++, nwk++)
                    wc[koff[o->series] + k + 1]++;
        }
        for (size_t x = 1; x <= koff[nseries]; x++)
            wc[x] += wc[x - 1];
        uint32_t *wl = malloc((nwk + 1) * sizeof *wl);
        for (uint32_t w = 1; w < nw; w++) {
            const wop *o = &writes[w];
            if (o->st == ST_FAIL)
                continue;
            if (o->kind == W_INSERT)
                for (uint32_t j = 0; j < o->nkeys; j++) {
                    size_t slot = koff[o->series] + o->keys[j];
                    wl[wc[slot] + wf[slot]++] = w;
                }
            else
                for (uint32_t k = o->lo; k < o->hi; k++) {
                    size_t slot = koff[o->series] + k;
                    wl[wc[slot] + wf[slot]++] = w;
                }
        }
        for (size_t slot = 0; slot < koff[nseries]; slot++) { /* ids come nearly in start order */
            uint32_t *a = wl + wc[slot];
            size_t n = wc[slot + 1] - wc[slot];
            for (size_t x = 1; x < n; x++)
                for (size_t y = x; y > 0 && writes[a[y - 1]].invoke > writes[a[y]].invoke; y--) {
                    uint32_t t = a[y];
                    a[y] = a[y - 1];
                    a[y - 1] = t;
                }
        }
        for (uint32_t i = 0; i < nr; i++) {
            const rop *q = &reads[i];
            if (q->st != ST_OK)
                continue;
            for (uint32_t k = q->lo; k < q->hi; k++) {
                uint32_t x = q->seen[k - q->lo];
                if (!x || x >= nw)
                    continue;
                size_t slot = koff[q->series] + k;
                const uint32_t *a = wl + wc[slot];
                size_t n = wc[slot + 1] - wc[slot], lo = 0, hi = n;
                while (lo < hi) { /* the first write that began after x completed */
                    size_t mid = (lo + hi) / 2;
                    if (writes[a[mid]].invoke <= writes[x].complete)
                        lo = mid + 1;
                    else
                        hi = mid;
                }
                /* (a read that began after an acknowledged w completed and missed it is stale: check 4) */
                for (size_t j = lo; j < n && writes[a[j]].invoke < q->complete; j++)
                    if (a[j] != x && lb[a[j]] < q->invoke &&
                        (writes[a[j]].st != ST_OK || q->invoke < writes[a[j]].complete))
                        lb[a[j]] = q->invoke;
            }
        }
        free(wl);
        free(wf);
        free(wc);
    }
    /* 1. durability and order */
    int before_loss = violations;
    uint64_t lost = 0;
    for (uint32_t w = 1; final && w < nw; w++) {
        if (!applied[w])
            continue;
        for (uint32_t i = 0; i < writes[w].nkeys; i++) {
            uint32_t k = writes[w].keys[i], f = final[koff[writes[w].series] + k];
            if (f ? older_seen(f, w) : !absent_ok(writes[w].series, k, writes[w].invoke, INT64_MAX)) {
                if (lost++ < 10)
                    VIOLATION("write %u (%s, %u keys) to series %d key %u lost: final holds %u (%u keys)%s\n", w,
                              writes[w].st == ST_OK ? "acknowledged" : "seen or partly kept", writes[w].nkeys,
                              writes[w].series, k, f, f && f < nw ? writes[f].nkeys : 0,
                              f && !older(f, w) ? ", acknowledged before a read saw the write not yet in effect "
                                                  "(check 5)" : "");
                else
                    violations++;
            }
        }
    }
    /* 1. for deletes: nothing older than an applied delete survives in its range */
    for (uint32_t w = 1; final && w < nw; w++) {
        if (!applied[w] || writes[w].kind == W_INSERT)
            continue;
        for (uint32_t k = writes[w].lo; k < writes[w].hi; k++) {
            uint32_t f = final[koff[writes[w].series] + k];
            if (f && f < nw && older_seen(f, w)) {
                if (lost++ < 10)
                    VIOLATION("%s %u of series %d keys %u..%u: final holds %u at key %u, which completed before%s\n",
                              writes[w].kind == W_DELETE ? "delete" : "replace", w, writes[w].series, writes[w].lo,
                              writes[w].hi, f, k, older(f, w) ? "" : " a read saw the delete not yet in effect (check 5)");
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
                if (other >= nw)
                    continue; /* a value no write wrote: reported by check 1 */
                if (other ? other != w && older(other, w) : !absent_ok(q->series, k2, writes[w].invoke, q->complete)) {
                    VIOLATION("read %u [%lld..%lld] saw write %u (%u keys, %lld..%lld) but %u (%u keys, %lld..%lld) at "
                              "its key %u: torn\n",
                              i, (long long)q->invoke, (long long)q->complete, w, writes[w].nkeys,
                              (long long)writes[w].invoke, (long long)writes[w].complete, other,
                              other < nw ? writes[other].nkeys : 0, other < nw ? (long long)writes[other].invoke : 0LL,
                              other < nw ? (long long)writes[other].complete : 0LL, k2);
                }
            }
            /* a replace seen: its tombstone came with it */
            for (uint32_t k2 = writes[w].lo; writes[w].kind == W_REPLACE && k2 < writes[w].hi; k2++) {
                if (k2 < q->lo || k2 >= q->hi)
                    continue;
                uint32_t other = q->seen[k2 - q->lo];
                if (other && other < nw && other != w && older(other, w))
                    VIOLATION("read %u saw replace %u (keys %u..%u) but %u at key %u, which completed before it: torn\n",
                              i, w, writes[w].lo, writes[w].hi, other, k2);
            }
        }
    }
    free(distinct);

    /* 4. no stale reads. Per key: acknowledged writes by completion time, with
     * the running maximum of their start times. A read that began at t must
     * not show anything older than the newest-starting write completed by t. */
    typedef struct ack {
        int64_t complete, max_invoke, max_lb; /* max_lb: of lb (check 5), for acknowledged values */
    } ack;
    size_t total = 0;
    uint32_t *cnt = calloc(koff[nseries] + 1, sizeof *cnt);
    for (uint32_t w = 1; w < nw; w++)
        if (writes[w].st == ST_OK) {
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                cnt[koff[writes[w].series] + writes[w].keys[j] + 1]++;
                total++;
            }
            for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                cnt[koff[writes[w].series] + k + 1]++;
                total++;
            }
        }
    for (size_t x = 1; x <= koff[nseries]; x++)
        cnt[x] += cnt[x - 1];
    ack *acks = malloc((total + 1) * sizeof *acks);
    uint32_t *fill = calloc(koff[nseries], sizeof *fill);
    for (uint32_t w = 1; w < nw; w++)
        if (writes[w].st == ST_OK) {
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                size_t slot = koff[writes[w].series] + writes[w].keys[j];
                acks[cnt[slot] + fill[slot]++] = (ack){writes[w].complete, writes[w].invoke, lb[w]};
            }
            for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                size_t slot = koff[writes[w].series] + k;
                acks[cnt[slot] + fill[slot]++] = (ack){writes[w].complete, writes[w].invoke, lb[w]};
            }
        }
    for (size_t slot = 0; slot < koff[nseries]; slot++) {
        ack *a = acks + cnt[slot];
        size_t n = cnt[slot + 1] - cnt[slot];
        for (size_t x = 1; x < n; x++) /* insertion sort: few writes per key */
            for (size_t y = x; y > 0 && a[y - 1].complete > a[y].complete; y--) {
                ack t = a[y];
                a[y] = a[y - 1];
                a[y - 1] = t;
            }
        for (size_t x = 1; x < n; x++) {
            if (a[x].max_invoke < a[x - 1].max_invoke)
                a[x].max_invoke = a[x - 1].max_invoke;
            if (a[x].max_lb < a[x - 1].max_lb)
                a[x].max_lb = a[x - 1].max_lb;
        }
    }
    for (uint32_t i = 0; i < nr; i++) {
        rop *q = &reads[i];
        if (q->st != ST_OK)
            continue;
        for (uint32_t k = q->lo; k < q->hi; k++) {
            size_t slot = koff[q->series] + k;
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
            if (seen ? seen < nw && writes[seen].complete < a[lo - 1].max_invoke
                     : !absent_ok(q->series, k, a[lo - 1].max_invoke, q->complete))
                VIOLATION("read %u shows %u at series %d key %u, older than an acknowledged write: stale\n", i, seen,
                          q->series, k);
            else if (seen && seen < nw && writes[seen].st == ST_OK && writes[seen].complete < a[lo - 1].max_lb)
                VIOLATION("read %u shows %u at series %d key %u, acknowledged before a read saw a later acknowledged "
                          "write to it not yet in effect (check 5): stale\n", i, seen, q->series, k);
        }
    }
    free(acks);
    free(fill);
    free(cnt);
    loss_violations = violations - before_loss;
    free(applied);
    free(dcnt);
    free(dlist);
    free(lb);
    lb = NULL;
    return violations;
}

/* The follower's view of every series; nonzero if a read fails. */
static int follower_state(uint32_t *out)
{
    tsdbc *c;
    if (connect_own(fport, atomic_load(&follower_pid), &c) != 0)
        return -1;
    int rc = 0;
    memset(out, 0, koff[nseries] * sizeof *out);
    for (int s = 0; s < nseries && !rc; s++) {
        tsdb_points p;
        if ((rc = tsdbc_query_points(c, ids[s], INT64_MIN, INT64_MAX, &p)) != 0)
            break;
        for (size_t i = 0; i < p.n; i++) {
            int64_t k = ts_key(s, p.ts[i]);
            if (k >= 0)
                out[koff[s] + k] = (uint32_t)p.values[i];
            else
                out[koff[s]] = UINT32_MAX; /* a row at no key: never the writer's state */
        }
        free(p.ts);
        free(p.values);
    }
    tsdbc_close(c);
    return rc;
}

/* With the clean writer still up: the follower is alive, its reads were valid and did not keep failing, and it
 * catches up with `final`. Stops the follower. */
static int check_follower(const uint32_t *final)
{
    int before = violations;
    pid_t pid = atomic_load(&follower_pid);
    int st;
    if (waitpid(pid, &st, WNOHANG) == pid) {
        VIOLATION("the follower died (status %#x), see %s\n", st, flogpath);
        atomic_store(&follower_pid, 0);
        return violations - before;
    }
    uint32_t nw = atomic_load(&nwrites), nr = atomic_load(&nfreads);
    if (nw > MAX_OPS)
        nw = MAX_OPS;
    if (nr > MAX_FREADS)
        nr = MAX_FREADS;
    unsigned ok = 0;
    uint64_t values = 0;
    for (uint32_t i = 0; i < nr; i++) {
        rop *q = &freads[i];
        if (q->st != ST_OK)
            continue;
        ok++;
        for (uint32_t k = q->lo; k < q->hi; k++)
            values += q->seen[k - q->lo] != 0;
        if (q->stray) {
            VIOLATION("follower read %u: a row at a timestamp no write used\n", i);
            continue;
        }
        for (uint32_t k = q->lo; k < q->hi; k++) {
            uint32_t w = q->seen[k - q->lo];
            if (!w)
                continue;
            if (w >= nw || writes[w].series != q->series || !wrote_key(w, k))
                VIOLATION("follower read %u: key %u shows %u, which never wrote it\n", i, k, w);
            else if (writes[w].st == ST_FAIL)
                VIOLATION("follower read %u: key %u shows %u, which failed\n", i, k, w);
            else if (writes[w].invoke > q->complete)
                VIOLATION("follower read %u: key %u shows %u, which began after the read\n", i, k, w);
        }
    }
    printf("follower: %u reads, %u ok, %llu values, longest run of failures %.1f s\n", nr, ok,
           (unsigned long long)values, ffail_longest / 1e9);
    if (ffail_longest > FOLLOW_FAIL_S * 1000000000LL)
        VIOLATION("follower reads kept failing for %.1f s\n", ffail_longest / 1e9);
    uint32_t *view = calloc(koff[nseries], sizeof *view);
    int64_t deadline = now_ns() + FOLLOW_CATCHUP_S * 1000000000LL;
    int rc;
    bool same = false;
    while (!(same = (rc = follower_state(view)) == 0 && !memcmp(view, final, koff[nseries] * sizeof *view)) && now_ns() < deadline)
        usleep(100000);
    if (!same) {
        if (rc)
            VIOLATION("the follower cannot be read at the end: %s\n", rc < 0 ? "no connection" : tsdbc_strerror(rc));
        for (int s = 0; s < nseries && !rc; s++)
            for (uint32_t k = 0; k < nkeys[s]; k++)
                if (view[koff[s] + k] != final[koff[s] + k]) {
                    VIOLATION("the follower does not catch up in %d s: series %d key %u holds %u, the writer %u\n",
                              FOLLOW_CATCHUP_S, s, k, view[koff[s] + k], final[koff[s] + k]);
                    break;
                }
    }
    free(view);
    signal_pid(pid, SIGTERM);
    waitpid(pid, &st, 0);
    atomic_store(&follower_pid, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
        VIOLATION("the follower ended with status %#x at SIGTERM, see %s\n", st, flogpath);
    return violations - before;
}

static int stray_rows; /* rows at no key in a final read */

static int final_state(uint32_t *final)
{
    /* not connect_retry(): that gives up once `stop` is set, which it is by now */
    tsdbc *c = NULL;
    for (int i = 0; i < 1000 && connect_own(atomic_load(&port), atomic_load(&server_pid), &c) != 0; i++)
        usleep(10000);
    if (!c) {
        fprintf(stderr, "jepsen: cannot connect for the final read\n");
        stop_server(SIGTERM);
        exit(2);
    }
    memset(final, 0, koff[nseries] * sizeof *final);
    for (int s = 0; s < nseries; s++) {
        tsdb_points p;
        int rc = tsdbc_query_points(c, ids[s], INT64_MIN, INT64_MAX, &p);
        if (rc != 0 && (md || rot) && (rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_IO)) {
            fprintf(stderr, "  the final read of series %d failed: %s (damage detected)\n", s, tsdbc_strerror(rc));
            tsdbc_close(c);
            return rc;
        }
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
                server_gone();
            } else {
                fprintf(stderr, "jepsen: server %d is still running\n", (int)pid);
            }
            stop_server(SIGTERM); /* no orphan server */
            exit(2);
        }
        for (size_t i = 0; i < p.n; i++) {
            int64_t k = ts_key(s, p.ts[i]);
            if (k >= 0)
                final[koff[s] + k] = (uint32_t)p.values[i];
            else if (stray_rows++ < 5)
                fprintf(stderr, "  final: series %d has a row at %lld, which no write used\n", s, (long long)p.ts[i]);
        }
        free(p.ts);
        free(p.values);
    }
    tsdbc_close(c);
    return 0;
}

/* classes: on the database in-process, once no server runs. Classes switched (else the run tested nothing of them),
 * and after tsdb_optimize, one part per partition with the newest of each (series, time) in it, the parts hold
 * as many rows as the final state has keys and answer the final state: a (series, time) with rows in two
 * partitions would count twice. */
struct cls_tally {
    uint64_t rows, parts[4], crows[4];
};
static void tally_part(void *ctx, const tsdb_part_info *in)
{
    struct cls_tally *t = ctx;
    t->rows += in->rows;
    if (in->cls >= 0 && in->cls < 4)
        t->parts[in->cls]++, t->crows[in->cls] += in->rows;
}

static int check_classes(const uint32_t *final)
{
    int before = violations;
    /* CLASSES: 24-byte records [check][series][from][class][0]; from = INT64_MIN is a fresh series' decision */
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s/CLASSES", dir);
    unsigned switches = 0, fresh = 0;
    FILE *f = fopen(path, "rb");
    unsigned char rec[24];
    while (f && fread(rec, 1, sizeof rec, f) == sizeof rec) {
        int64_t from;
        memcpy(&from, rec + 8, 8);
        if (from == INT64_MIN)
            fresh++;
        else
            switches++;
    }
    if (f)
        fclose(f);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc) {
        VIOLATION("classes: cannot open the database in-process: %s\n", tsdb_strerror(rc));
        return violations - before;
    }
    int now[MAX_SERIES] = {0};
    for (int s = 0; s < nseries; s++)
        tsdb_series_class(db, ids[s], &now[s]);
    struct cls_tally t = {0};
    if ((rc = tsdb_optimize(db)) != 0)
        VIOLATION("classes: tsdb_optimize: %s\n", tsdb_strerror(rc));
    tsdb_foreach_part(db, tally_part, &t);
    uint64_t keys = 0;
    for (size_t x = 0; x < koff[nseries]; x++)
        keys += final[x] != 0;
    for (int s = 0; s < nseries; s++) {
        tsdb_points p = {0};
        if ((rc = tsdb_query_points(db, ids[s], INT64_MIN, INT64_MAX, &p)) != 0) {
            VIOLATION("classes: in-process read of series %d: %s\n", s, tsdb_strerror(rc));
            continue;
        }
        size_t same = 0;
        for (size_t i = 0; i < p.n; i++) {
            int64_t k = ts_key(s, p.ts[i]);
            same += k >= 0 && final[koff[s] + k] == (uint32_t)p.values[i];
        }
        size_t want = 0;
        for (uint32_t k = 0; k < nkeys[s]; k++)
            want += final[koff[s] + k] != 0;
        if (same != p.n || p.n != want)
            VIOLATION("classes: after tsdb_optimize series %d has %zu rows, %zu of them as in the final state of "
                      "%zu\n", s, p.n, same, want);
        tsdb_points_free(&p);
    }
    tsdb_close(db);
    unsigned used = 0;
    for (int c = 0; c < 4; c++)
        used += t.parts[c] > 0;
    printf("classes: %u switches, %u fresh decisions; series now in classes", switches, fresh);
    for (int s = 0; s < nseries; s++)
        printf(" %d", now[s]);
    printf("; after tsdb_optimize parts (rows) per class:");
    for (int c = 0; c < 4; c++)
        printf(" %llu (%llu)", (unsigned long long)t.parts[c], (unsigned long long)t.crows[c]);
    printf("\n");
    if (t.rows != keys)
        VIOLATION("classes: after tsdb_optimize the parts hold %llu rows, the final state %llu: rows of one (series, "
                  "time) in two partitions\n", (unsigned long long)t.rows, (unsigned long long)keys);
    if (switches == 0 || used < 2)
        VIOLATION("classes: %u switches, parts in %u classes: the workload tested no change of class\n", switches,
                  used);
    return violations - before;
}

/* --auth: `tsdb DIR user ...`, the CLI as an operator runs it before the first start (never under the shim). */
static int users_cli(const char *a1, const char *a2, const char *a3, const char *a4, const char *a5)
{
    const char *cli = getenv("JEPSEN_TSDB") ? getenv("JEPSEN_TSDB") : "./build/tsdb";
    pid_t pid = fork();
    if (pid == 0) {
        unsetenv("LD_PRELOAD");
        execl(cli, "tsdb", dir, "user", a1, a2, a3, a4, a5, (char *)NULL);
        _exit(127);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "jepsen: %s %s user %s %s ... failed\n", cli, dir, a1, a2);
        return 1;
    }
    return 0;
}

/* --auth: the superuser tsdbd needs to start, and KV_USER, with only what the workload does on its series. */
static int make_users(void)
{
    char root_pw[PATH_MAX];
    snprintf(root_pw, sizeof root_pw, "%s.root.pw", dir);
    snprintf(kv_pw, sizeof kv_pw, "%s.kv.pw", dir);
    const char *pw[2] = {root_pw, kv_pw};
    for (int i = 0; i < 2; i++) {
        FILE *f = fopen(pw[i], "w");
        if (!f || fprintf(f, "%s-%016llx\n", i ? "kv" : "root", (unsigned long long)mix(seed * 31 + (uint64_t)i)) < 0 ||
            fclose(f) != 0) {
            fprintf(stderr, "jepsen: cannot write %s\n", pw[i]);
            return 1;
        }
    }
    if (mkdir(dir, 0755) != 0 && errno != EEXIST)
        return 1;
    return users_cli("add", "root", "--superuser", "--password-file", root_pw) ||
           users_cli("add", KV_USER, "--password-file", kv_pw, NULL) ||
           users_cli("grant", KV_USER, "read,write", "prefix:jepsen.", NULL);
}

static int create_series(int from, int to)
{
    tsdbc *c = connect_retry();
    for (int s = from; s < to; s++) {
        char name[32];
        snprintf(name, sizeof name, "jepsen.%d", s);
        if (tsdbc_series(c, name, 1, &ids[s])) {
            fprintf(stderr, "jepsen: cannot create series\n");
            tsdbc_close(c);
            return 1;
        }
    }
    tsdbc_close(c);
    return 0;
}

/* ------------------------------------------------------- misdirect, rot */

#define EPOCH_S 8 /* longest epoch: long enough for several restarts, short enough for several epochs per run */

/* Whether the fault log names `name` (a file the engine cut a torn tail of) as the file a misdirected write was
 * meant for, or as one rot damaged: then losing its last record is the known limit (a damaged last record looks
 * like a torn append), not a bug. A misdirected read is transient and explains no cut. */
static bool cut_explained(const char *name)
{
    FILE *f = fopen(faultlog, "r");
    char line[3 * PATH_MAX], want[300];
    snprintf(want, sizeof want, "/%s ", name);
    bool yes = false;
    while (f && !yes && fgets(line, sizeof line, f)) {
        char *w = strstr(line, ": write "), *at = strstr(line, want);
        yes = at && ((w && at < strstr(line, " -> ")) || strstr(line, ": rot "));
    }
    if (f)
        fclose(f);
    return yes;
}

/* The end of an epoch: classify its violations and append its counts to DIR.stats for the parent. Returns the
 * violations that fail the run. */
static int epoch_report(int v, unsigned acked)
{
    int faults = 0, explained = 0;
    FILE *f = fopen(faultlog, "r");
    char line[3 * PATH_MAX];
    while (f && fgets(line, sizeof line, f))
        faults += strstr(line, "the engine cut") == NULL;
    if (f)
        fclose(f);
    for (int i = 0; i < ncut; i++)
        explained += cut_explained(cut_files[i]);
    int undetected = 0, tail_lost = 0, kept = v;
    if (md && !verify && !strcmp(md_files, "parts")) {
        undetected = v; /* granule payloads: unprotected without verify_checksums (documented) */
        kept = 0;
    } else if (v > 0 && v == loss_violations && explained) {
        tail_lost = v; /* writes lost with a last record that damage made look torn: the known limit */
        kept = 0;
    }
    kept += md_crashes; /* damage must never crash the server, checksums or not */
    int detected = refused_faulty + refused_clean + md_failstops + atomic_load(&read_damage) + atomic_load(&write_damage);
    char sp[PATH_MAX];
    snprintf(sp, sizeof sp, "%s.stats", dir);
    if ((f = fopen(sp, "a")) != NULL) {
        fprintf(f, "%d %s %d %d %d %d %d %d %d %d %d %d %d %d %d %u\n", epoch, md_files, faults, detected,
                refused_faulty, refused_clean, md_failstops, atomic_load(&read_damage), atomic_load(&write_damage),
                damaged, kept, undetected, tail_lost, tail_cuts, restarts, acked);
        fclose(f);
    }
    if (kept || undetected || tail_lost) {
        fprintf(stderr, "  epoch %d (seed %llu, files %s): %d violations%s%s, %d faults, %d tail cuts (%d explained):\n",
                epoch, (unsigned long long)seed, md_files, kept + undetected + tail_lost,
                undetected ? " in part payloads without verify_checksums (known)" : "",
                tail_lost ? " of writes lost to a tail cut after damage (known limit)" : "", faults, ncut, explained);
        int n = 0;
        if ((f = fopen(faultlog, "r")) != NULL) {
            while (fgets(line, sizeof line, f))
                if (n++ < 40)
                    fprintf(stderr, "    %s", line);
            fclose(f);
        }
        fprintf(stderr, "    (fault log %s, server log %s)\n", faultlog, slogpath);
    } else {
        unlink(faultlog);
    }
    return kept;
}

/* The parent of a misdirect or rot run: epochs, each a child forked from this still single-threaded process (so
 * with the model fresh) on a fresh directory and seed, until the run's time is up; then the counts. Returns -1 in
 * a child, which runs its epoch; the exit status in the parent. */
static int epochs(void)
{
    char sp[PATH_MAX];
    snprintf(sp, sizeof sp, "%s.stats", dir);
    unlink(sp);
    int64_t start = now_ns(), end = start + (int64_t)(seconds * 1e9);
    uint64_t seed0 = seed;
    int bad = 0, n = 0;
    for (int e = 0; now_ns() < end || (md && !verify && e < 2); e++) { /* without verify: both kinds of epoch */
        double left = (double)(end - now_ns()) / 1e9;
        fflush(NULL);
        pid_t pid = fork();
        if (pid < 0)
            return 2;
        if (pid == 0) {
            epoch = e;
            seed = mix(mix(seed0) + (uint64_t)e); /* seeds N and N+1 share no epochs */
            seconds = left > EPOCH_S ? EPOCH_S : left > 3 ? left : 3;
            md_files = md && !verify ? (e % 2 ? "parts" : "noparts") : "all";
            return -1;
        }
        int st;
        waitpid(pid, &st, 0);
        n++;
        int rc = WIFEXITED(st) ? WEXITSTATUS(st) : 2;
        if (rc == 2) {
            fprintf(stderr, "jepsen: epoch %d ended with status %#x\n", e, st);
            return 2;
        }
        bad |= rc != 0;
    }
    /* sums: [0] faults [1] detected [2] refused faulty [3] refused clean [4] fail-stops [5] read errors
     * [6] write errors [7] damaged [8] violations [9] undetected [10] tail losses [11] tail cuts [12] restarts
     * [13] acked; per kind of epoch for misdirect without verify */
    long sum[2][14] = {{0}}, harmless[2] = {0}, fdet[2] = {0}, fbad[2] = {0};
    int eps[2] = {0};
    FILE *f = fopen(sp, "r");
    char kind[16];
    int e;
    long x[14];
    while (f && fscanf(f, "%d %15s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld", &e, kind, &x[0], &x[1],
                       &x[2], &x[3], &x[4], &x[5], &x[6], &x[7], &x[8], &x[9], &x[10], &x[11], &x[12], &x[13]) == 16) {
        int k = !strcmp(kind, "parts");
        eps[k]++;
        for (int i = 0; i < 14; i++)
            sum[k][i] += x[i];
        if (x[8] || x[9] || x[10])
            fbad[k] += x[0];
        else if (x[1])
            fdet[k] += x[0];
        else
            harmless[k] += x[0];
    }
    if (f)
        fclose(f);
    unlink(sp);
    for (int k = 0; k < 2; k++) {
        if (!eps[k])
            continue;
        printf("jepsen %s%s%s: %.0f s, %d epochs, %ld restarts, %ld writes acknowledged\n", nemesis,
               verify ? " (verify_checksums)" : "", md && !verify ? (k ? ", faults in parts only" : ", faults in all but parts") : "",
               (double)(now_ns() - start) / 1e9, eps[k], sum[k][12], sum[k][13]);
        printf("  faults injected: %ld; in epochs that detected damage %ld, harmless (every check passed, nothing "
               "detected) %ld, in epochs that lost or got wrong answers (known limits included) %ld\n",
               sum[k][0], fdet[k], harmless[k], fbad[k]);
        printf("  detections: %ld (opens refused: %ld by the faulty server, %ld persistent; %ld stops on damage; "
               "%ld reads and %ld writes failed with CORRUPT); %ld epochs ended with the database refused\n",
               sum[k][1], sum[k][2], sum[k][3], sum[k][4], sum[k][5], sum[k][6], sum[k][7]);
        printf("  tail cuts: %ld; acknowledged writes lost to a tail cut after damage (known limit): %ld\n", sum[k][11],
               sum[k][10]);
        if (sum[k][9])
            printf("  wrong answers from part payloads without verify_checksums (known, documented): %ld\n", sum[k][9]);
        printf("  violations: %ld\n", sum[k][8]);
    }
    printf(bad ? "FAILED\n" : "valid\n");
    return bad ? 1 : 0;
}

int main(int argc, char **argv)
{
    static struct option opts[] = {{"dir", required_argument, 0, 'd'},     {"nemesis", required_argument, 0, 'n'},
                                   {"seconds", required_argument, 0, 't'}, {"clients", required_argument, 0, 'c'},
                                   {"seed", required_argument, 0, 's'},    {"crashfs", required_argument, 0, 'f'},
                                   {"workload", required_argument, 0, 'w'}, {"verify-checksums", no_argument, 0, 'V'},
                                   {"auth", no_argument, 0, 'A'},           {0, 0, 0, 0}};
    int ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': dir = optarg; break;
        case 'n': nemesis = optarg; break;
        case 't': seconds = atof(optarg); break;
        case 'c': nclients = atoi(optarg); break;
        case 's': seed = strtoull(optarg, NULL, 10); break;
        case 'f': crashfs = optarg; break;
        case 'w':
            if (strcmp(optarg, "kv") && strcmp(optarg, "classes"))
                return 2;
            classes = !strcmp(optarg, "classes");
            break;
        case 'V': verify = true; break;
        case 'A': auth = true; break;
        default: return 2;
        }
    }
    md = !strcmp(nemesis, "misdirect");
    rot = !strcmp(nemesis, "rot");
    bool shim = !strcmp(nemesis, "power") || !strcmp(nemesis, "eio") || md;
    if (!dir || (shim && !crashfs) || ((md || rot) && classes)) {
        fprintf(stderr, "usage: jepsen --dir DIR [--nemesis kill|power|pause|eio|misdirect|rot|none] [--seconds S]\n"
                        "              [--clients N] [--seed N] [--crashfs build/libcrashfs.so] [--workload kv|classes]\n"
                        "              [--verify-checksums]   (misdirect and rot: workload kv) [--auth]\n");
        return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    if (md || rot) {
        int rc = epochs(); /* the parent runs the epochs and returns here; each child goes on below */
        if (rc >= 0)
            return rc;
        snprintf(faultlog, sizeof faultlog, "%s.e%d.faults", dir, epoch);
        unlink(faultlog);
    }
    /* Two runs on one directory wipe and write each other's database, and the checker then reports the other run's
     * writes as lost acknowledged writes. The lock beside the directory (it survives the wipe) is held until exit. */
    char lockpath[1100];
    snprintf(lockpath, sizeof lockpath, "%s.lock", dir);
    int lockfd = open(lockpath, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (lockfd < 0 || flock(lockfd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr, "jepsen: %s is in use by another run (or cannot be locked: %s); use a --dir of your own\n",
                dir, strerror(errno));
        return 2;
    }
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 2;
    snprintf(markpath, sizeof markpath, "%s.eio", dir);
    if (md || rot)
        snprintf(slogpath, sizeof slogpath, "%s.e%d.server.log", dir, epoch);
    else
        snprintf(slogpath, sizeof slogpath, "%s.server.log", dir);
    unlink(slogpath);
    writes = calloc(MAX_OPS, sizeof *writes);
    reads = calloc(MAX_OPS, sizeof *reads);
    nseries = classes ? CL_SERIES : NSERIES;
    make_keys();
    if (auth && make_users())
        return 2;

    start_server(false); /* the series exist before any fault */
    /* classes under a shim: the series are created by the server under it (before the nemesis starts), so their
     * first rows get a fresh class there; the restart below decides every series that exists (class 0, with no
     * record), which left power and eio runs with no fresh decision at all. A series whose first flush brings
     * fewer than 64 rows is decided class 0 without a record: the summary counts the decisions recorded. */
    int early = classes && shim ? 0 : nseries;
    if (create_series(0, early))
        return 2;
    if (shim) { /* from here on, file operations may lose power or fail */
        pthread_mutex_lock(&server_lock);
        stop_server(SIGTERM);
        start_server(true);
        pthread_mutex_unlock(&server_lock);
        if (create_series(early, nseries)) /* before the nemesis starts: no fault yet */
            return 2;
    }

    pthread_t th[256], nem, fr;
    if (!strcmp(nemesis, "power")) {
        snprintf(flogpath, sizeof flogpath, "%s.follower.log", dir);
        freads = calloc(MAX_FREADS, sizeof *freads);
        atexit(kill_follower);
        start_follower();
        pthread_create(&fr, NULL, follower_reader, NULL);
    }
    run_start = now_ns();
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
    if (fport)
        pthread_join(fr, NULL);

    /* the final state, from a server without faults, and again after a clean restart */
    pthread_mutex_lock(&server_lock);
    pid_t pid = atomic_load(&server_pid);
    int st;
    if (pid > 0 && waitpid(pid, &st, WNOHANG) == pid) {
        if (md || rot)
            md_death(st, 0, pid);
        server_gone();
    }
    stop_server(strcmp(nemesis, "eio") ? SIGKILL : SIGTERM); /* eio: the shim rolls back at a normal exit */
    uint32_t *final = calloc(koff[nseries], sizeof *final), *again = calloc(koff[nseries], sizeof *again);
    if (!damaged) /* misdirect, rot: a database refused while the clients ran has no final state */
        start_server(false);
    if (!damaged && final_state(final) != 0)
        damaged = true;
    if (!damaged) {
        stop_server(SIGTERM);
        start_server(false);
    }
    if (!damaged && final_state(again) != 0)
        damaged = true;
    if (md || rot)
        scan_cuts();
    int fv = fport ? check_follower(again) : 0;
    stop_server(SIGTERM);
    pthread_mutex_unlock(&server_lock);

    uint32_t nw = atomic_load(&nwrites) - 1, nr = atomic_load(&nreads);
    unsigned ok = 0, info = 0, fail = 0, rok = 0, dels = 0, reps = 0;
    for (uint32_t w = 1; w <= nw && w < MAX_OPS; w++) {
        ok += writes[w].st == ST_OK, info += writes[w].st == ST_INFO, fail += writes[w].st == ST_FAIL;
        dels += writes[w].kind == W_DELETE && writes[w].st == ST_OK;
        reps += writes[w].kind == W_REPLACE && writes[w].st == ST_OK;
    }
    for (uint32_t i = 0; i < nr && i < MAX_OPS; i++)
        rok += reads[i].st == ST_OK;
    if (!(md || rot)) {
        printf("jepsen %s: %.0f s, %d clients, %d restarts; writes %u ok / %u indeterminate / %u failed, reads %u ok; "
               "%u deletes and %u replaces ok\n",
               nemesis, seconds, nclients, restarts, ok, info, fail, rok, dels, reps);
    }
    if (crashfs && !md)
        printf("servers under the shim: %d faulty, %d clean (fallback after a failed start), %d died while starting\n",
               lives_faulty, lives_clean, lives_died_starting);
    if (!damaged && memcmp(final, again, koff[nseries] * sizeof *final) != 0)
        VIOLATION("the state changed across a clean restart\n");
    if (stray_rows)
        VIOLATION("%d rows at timestamps no write used in the final reads\n", stray_rows);
    int v = check(damaged ? NULL : final); /* counts the follower's violations too */
    if (md || rot)
        v = epoch_report(v, ok);
    if (classes)
        v += check_classes(final);
    if (!strcmp(nemesis, "eio")) {
        printf("eio: %d fail-stops\n", failstops);
        v += eio_problems;
    }
    if (!strcmp(nemesis, "power")) {
        v += death_problems;
        /* the nemesis must keep killing: a server left dead until the next tick (the clients idle, 40 restarts in
         * 20 s instead of 300) tests far less, and passes every check above */
        if (crashfs && seconds >= 10 && restarts < (int)(seconds * 4)) {
            fprintf(stderr, "  only %d restarts in %.0f s (at least %d expected): the nemesis was idle\n", restarts,
                    seconds, (int)(seconds * 4));
            v++;
        }
    }
    unlink(markpath);
    if (!v)
        unlink(slogpath);
    if (fport && !fv)
        unlink(flogpath);
    if (system(cmd) != 0)
        return 2;
    if (v) {
        printf("FAILED: %d violations\n", v);
        return 1;
    }
    if (!(md || rot))
        printf("valid\n");
    return 0;
}
