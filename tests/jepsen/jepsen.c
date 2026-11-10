/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A Jepsen-style test of tsdbd: clients write and read through the network
 * while a nemesis crashes the server, and a checker verifies the history.
 *
 *   jepsen --dir DIR [--nemesis kill|power|pause|eio|misdirect|rot|outage|none] [--seconds 20]
 *          [--clients 8] [--seed N] [--crashfs build/libcrashfs.so] [--workload kv|classes]
 *          [--verify-checksums] [--auth] [--feed]
 *
 * Change feed (--feed; docs/design-cdc.md, step F1): before the first server the CLI (`tsdb DIR feed enable`) turns the
 * feed on, so every WAL segment the engine is done with is kept as fwal_<seq>.log instead of being unlinked. Before every
 * server start (the directory as the crash or the power loss left it) and after the final clean stop, tests/jepsen/
 * feedcheck.c checks what F2's reader relies on: every retained segment whole, numbers increasing, nothing above the floor
 * vanishing (a segment of the last check that is gone ends at or below the floor), the floor below the oldest kept number. The writes and reads are checked as without the flag: the feed adds no behaviour.
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
 * Outage (--outagefs build/liboutagefs.so; workload kv, no login): the series live in two volumes (series s in volume
 * s % 2), each a storage domain of tsdbd (--health-domain) under the outagefs shim, and a client works the series of
 * one volume only. The nemesis hangs one volume (past the DOWN threshold, which TSDBD_TEST_HEALTH scales to seconds,
 * or for a stall shorter than it), fails it with EIO, slows it, or kills the server (also while a volume hangs). The
 * claim (ha.md 15): the volume refuses with TSDB_ERR_UNAVAILABLE ("not applied": check 2 sees every such write as a
 * definite failure) while the server goes on, the other volume keeps at least 80% of its pace, the volume takes
 * writes again within 30 s of the heal by a recovery in place, and nothing acknowledged is lost (checks 1, 3, 4, 5
 * after each recovery and at the end). A server that ends by itself is a violation. Not covered yet: rollback (a
 * server that lost acknowledged writes: the model has no way to say which), a hung volume at open (no deadline, O5).
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
 * Nulls and QUERY_LATEST (kv, except under misdirect, rot and outage): a null write puts nulls at every key of a
 * range near the top of a series; for checks 1-5 it is a delete (reads show no value there). Clients also ask for
 * the newest row of every series (with or without nulls, sometimes before an end), and check_latest tests every
 * answer against what is certain: it is a row some write wrote and not a stale one (L1, L2), and no key above it,
 * before the end, holds a row an acknowledged write put there that nothing could have removed (L3). The final reads
 * then fix every series' rows, and the final server must answer the latest of each exactly (final_latest, also after
 * the clean restart, which builds the table anew). tests/jepsen/mutant-latest.sh plants stale table entries in
 * tsdbd: the check must fail them.
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
#include <math.h>
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

#include "../../src/health_rules.h"
#include "../harness.h"
#include "../outagefs_ctl.h"

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

/* W_NULL: nulls at every key of [lo, hi) (the keys list stays empty). For the checks it is a delete (reads show no
 * value there, and everything older in the range is hidden); the latest check tells it apart (a null is a row). */
enum { W_INSERT, W_DELETE, W_REPLACE, W_NULL };

typedef struct wop {
    int series;
    int kind;       /* W_*: a delete, replace or null write also clears [lo, hi) of the series */
    uint32_t lo, hi;
    uint32_t *keys;
    uint32_t nkeys;
    int64_t invoke, complete; /* ns */
    status st;
} wop;

/* QUERY_LATEST: the newest row of one series (a request asks for all of them: one lop each, the same times) */
typedef struct lop {
    int series;
    bool with_nulls;
    int64_t end;       /* rows before it: a key's timestamp, or INT64_MAX */
    int64_t invoke, complete;
    status st;
    int kind;          /* tsdb_latest_kind of the answer */
    int64_t ts;
    uint32_t val;      /* the write id the value names (kind VALUE) */
    int rstatus;       /* the record's status */
} lop;

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
static bool feed;                   /* --feed: the change feed is on (retained WAL segments), checked offline */
static int feed_checks;             /* directories checked offline */
static void feed_check(const char *when, bool crashed);
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
#define MAX_LATEST 1000000
static lop *lats;               /* kv without damage or outage: QUERY_LATEST answers, checked by check_latest */
static atomic_uint nlats;
static bool latest_on;
static atomic_bool stop;
static atomic_int server_pid;
static pthread_mutex_t server_lock = PTHREAD_MUTEX_INITIALIZER;
static int restarts;
/* The nemesis's own idleness (power nemesis): the server dies by itself a few ms after it is up (crashfs), and the
 * nemesis is meant to notice at once and restart it. death_thread stamps the first moment it sees the server exited
 * while the nemesis is napping (not inside its fault, where stopping and starting servers is the harness's time);
 * the nemesis records how long after that it woke up. What the machine does to the server (a slow start, a late
 * crash) is not in it, and a run under load differs only by the scheduling delay of a 5 ms poll. */
static _Atomic int64_t death_ts;
static atomic_bool nem_busy;
static double react_ms[16384];
static int nreact;
static bool idle_mutant;            /* --mutant-idle: a nemesis that sleeps through its naps; the check must fail */
static bool idle_mutant_long;       /* --mutant-longnap: naps of 5 s without watching, so it wakes too rarely to be judged
                                     * by a reaction time: the count floor must fail it */
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
/* outage: series s lives in volume s % 2; each volume is a storage domain of tsdbd (--health-domain) under the
 * outagefs shim (build/liboutagefs.so), which the nemesis hangs, fails and slows one volume at a time (ha.md 15.8) */
static bool outage;                 /* --nemesis outage */
static const char *outagefs_lib;    /* --outagefs */
static char vol_dir[2][PATH_MAX], octl[PATH_MAX]; /* the two volume directories; the shim's control socket */
static atomic_llong vol_ok[2], vol_last_ok[2];    /* acknowledged writes per volume; ns of the latest one's INVOKE */
static int outage_problems, outage_faults, outage_kills;
/* The heals the check judged: each is awaited until a write to the volume is acknowledged, or the bound passes. One a
 * run's end cuts short is counted, never silent. The nemesis stops starting faults at the end of the run's time
 * (nem_stop) and finishes the heal it is in before the clients stop, so the last heal is judged like the others. */
static atomic_bool nem_stop;
static int outage_heals, outage_judged, outage_flap_judged;
static double outage_worst_s, outage_worst_flap_s;
static char outage_keeppath[PATH_MAX + 64]; /* where a failed or close outage run's server log is kept */
static bool outage_near_miss; /* a heal took more than three quarters of its bound: the server log is kept */
static double healthy_ops, healthy_secs, healthy_base_ops; /* B's writes during A's outage, and what B did before */

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
static int64_t life_start_ns; /* outage: when the server that runs now began (its memory of DOWNs begins then) */

static void server_gone(void)
{
    life_start_ns = now_ns(); /* outage: the DOWNs the old server counted went with it */
    atomic_store(&server_pid, 0);
    atomic_store(&port, 0);
}
static const char *tsdbd_path(void) { return getenv("JEPSEN_TSDBD") ? getenv("JEPSEN_TSDBD") : "./build/tsdbd"; }

static void note_death(int st, bool we_killed, pid_t pid);

static void start_server(bool faulty)
{
    char pms[32], pf[PATH_MAX];
    if (feed)
        feed_check("before the server recovers it", true); /* the directory the last server left, torn or not */
    snprintf(pms, sizeof pms, "%lld", classes ? (long long)CL_PARTITION_MS : PARTITION_MS);
    atomic_store(&port, 0); /* until this server says where it listens, the clients have nowhere to go */
    for (int attempt = 0;; attempt++) {
        unlink(markpath); /* an EIO while opening: the clean server of the second attempt has no fault */
        th_portfile(pf, sizeof pf, dir);
        char follow[64];
        th_follow_me(follow, sizeof follow);
        pid_t pid = fork();
        if (pid == 0) {
            if (outage && faulty && outagefs_lib) { /* the shim: the nemesis takes a volume's directory away */
                char roots[2 * PATH_MAX + 2];
                snprintf(roots, sizeof roots, "%s:%s", vol_dir[0], vol_dir[1]);
                setenv("LD_PRELOAD", outagefs_lib, 1);
                setenv("OUTAGEFS_ROOTS", roots, 1);
                setenv("OUTAGEFS_CTL", octl, 1);
            }
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
            if (outage) /* thresholds of seconds, not tens of them: an outage of a few seconds is one */
                setenv("TSDBD_TEST_HEALTH", "down=2500,read=200,write=500,recover=1500,fast=400,spawn=60000", 1);
            if (!getenv("JEPSEN_VERBOSE")) { /* kept for a server that does not come up */
                int lfd = open(slogpath, O_WRONLY | O_CREAT | (md || rot || outage ? O_APPEND : O_TRUNC), 0644);
                if (lfd < 0)
                    lfd = open("/dev/null", O_WRONLY);
                if (lfd >= 0)
                    dup2(lfd, 2);
            }
            const char *av[40] = {"tsdbd", "--dir", dir, "--port", "0", "--port-file", pf, "--workers", "8",
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
            if (outage) {
                av[ac++] = "--health-domain", av[ac++] = vol_dir[0];
                av[ac++] = "--health-domain", av[ac++] = vol_dir[1];
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
    tsdbc_set_retry(*c, 0); /* the history records what the server answered, not what a retrying client made of it */
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
                        q->seen[k - q->lo] = isnan(p.values[i]) ? 0 : (uint32_t)p.values[i]; /* a null shows as absent */
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

/* outage: what a write to series s came to. UNAVAILABLE is "not applied" (a definite failure, checked by 2); a
 * client that gets it does not spin. */
static void outage_note(int s, int rc, int64_t invoke)
{
    if (!outage)
        return;
    if (rc == 0) {
        atomic_fetch_add(&vol_ok[s % 2], 1);
        /* the invoke time, not the ack: a call that was stuck in the hang and returns the moment it heals is a write
         * the old outage acknowledged, and says nothing of whether the volume takes new ones (it made the check
         * below pass at once in half the runs). The latest invoke among the acknowledged writes. */
        long long cur = atomic_load(&vol_last_ok[s % 2]);
        while (cur < invoke && !atomic_compare_exchange_weak(&vol_last_ok[s % 2], &cur, invoke))
            ;
    } else if (rc == TSDB_ERR_UNAVAILABLE)
        usleep(2000);
}

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
        if (outage) /* a client works one volume only: one stuck in a hung volume's calls must not hold back the rest */
            s = (s & ~1) | (int)((intptr_t)arg % 2);
        if ((r >> 8) % 100 < 6) { /* a delete or a replace of a key range */
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= MAX_OPS)
                break;
            wop *w = &writes[id];
            w->series = s;
            w->kind = (r >> 16) % 2 ? W_REPLACE : W_DELETE;
            w->lo = pick_key(s, mix(r + 3), mix(r + 6), 50);
            /* latest: a share of the replaces rewrite the top of the series and always write its last key, so that a
             * replace's row is the newest row of the series (what the latest answers must then show) */
            bool top = latest_on && w->kind == W_REPLACE && (mix(r + 11) % 100) < 30;
            if (top)
                w->lo = key_limit(s) - 1 - (uint32_t)(mix(r + 12) % (key_limit(s) < 60 ? key_limit(s) : 60));
            w->hi = w->lo + 1 + (uint32_t)(mix(r + 4) % 200);
            if (w->hi > key_limit(s) || top)
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
            if (top && w->keys[m - 1] != w->hi - 1) { /* m < n here: a stride above 1 left room */
                w->keys[m] = w->hi - 1;
                ts[m] = key_ts(s, w->hi - 1);
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
            outage_note(s, rc, w->invoke);
            free(ts);
            free(v);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) {
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&write_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        } else if (latest_on && (r >> 8) % 100 < 12) { /* nulls over a key range, near the top: the newest row's null */
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= MAX_OPS)
                break;
            wop *w = &writes[id];
            w->series = s;
            w->kind = W_NULL;
            uint32_t top = (r >> 20) % 100 < 70 ? 40 : nkeys[s];
            w->hi = nkeys[s] - (uint32_t)(mix(r + 3) % top);
            uint32_t len = 1 + (uint32_t)(mix(r + 4) % ((r >> 30) % 4 ? 6 : 120));
            w->lo = w->hi > len ? w->hi - len : 0;
            w->keys = malloc(sizeof *w->keys);
            w->nkeys = 0;
            uint32_t n = w->hi - w->lo;
            int64_t *ts = malloc(n * sizeof *ts);
            float *v = malloc(n * sizeof *v);
            for (uint32_t k = w->lo; k < w->hi; k++) {
                ts[k - w->lo] = key_ts(s, k);
                uint32_t nb = TSDB_NULL_F32_BITS;
                memcpy(&v[k - w->lo], &nb, sizeof nb);
            }
            w->invoke = now_ns();
            int rc = tsdbc_insert_series(c, ids[s], ts, v, n);
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            outage_note(s, rc, w->invoke);
            free(ts);
            free(v);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) {
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&write_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        } else if (latest_on && (r >> 8) % 100 < 20) { /* the newest row of every series, in one request */
            uint32_t base = atomic_fetch_add(&nlats, (uint32_t)nseries);
            if (base + (uint32_t)nseries > MAX_LATEST)
                break;
            bool wn = (r >> 14) % 2;
            int64_t end = (r >> 15) % 3 ? INT64_MAX : key_ts(s, (uint32_t)(mix(r + 9) % nkeys[s]));
            tsdb_latest_result out[MAX_SERIES];
            tsdb_latest_opts o = {.end = end, .ends = NULL, .with_nulls = wn, .threads = 0};
            int64_t t0 = now_ns();
            int rc = tsdbc_latest(c, ids, (size_t)nseries, &o, out);
            int64_t t1 = now_ns();
            for (int i = 0; i < nseries; i++) {
                lop *l = &lats[base + (uint32_t)i];
                l->series = i;
                l->with_nulls = wn;
                l->end = end;
                l->invoke = t0;
                l->complete = t1;
                l->st = rc == 0 ? ST_OK : ST_FAIL;
                if (rc == 0) {
                    l->rstatus = out[i].status;
                    l->kind = out[i].kind;
                    l->ts = out[i].ts;
                    l->val = out[i].kind == TSDB_LATEST_VALUE ? (uint32_t)out[i].v.f32 : 0;
                }
            }
            if (rc != 0) {
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&read_damage, 1);
                if (rc == TSDB_ERR_UNAVAILABLE) {
                    usleep(2000);
                } else {
                    tsdbc_close(c);
                    c = connect_retry();
                }
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
            if (latest_on && (mix(r + 8) % 100) < 25) /* the newest rows are what latest answers: write there often */
                start = lim - 1 - (uint32_t)(mix(r + 10) % 60);
            if (classes && n > 20 && (mix(r + 7) % 100) < (uint64_t)near_pct(s)) /* large: ending at the frontier */
            {
                uint32_t span = n * stride, jitter = (uint32_t)(mix(r + 1) % CL_NEAR);
                start = span + jitter < lim ? lim - span - jitter : 0;
            }
            /* latest: the top key of every odd series is written by replaces only (see below), so a replace's row is
             * often the newest row of its series, which inserts would otherwise cover within moments */
            bool reserved = latest_on && !classes && (s & 1);
            for (uint32_t i = 0; i < n; i++) {
                uint32_t k = (start + i * stride) % lim;
                bool dup = false;
                for (uint32_t j = 0; j < m && n <= 20; j++)
                    dup |= w->keys[j] == k;
                if (dup || (n > 20 && i * stride >= lim) || (reserved && k == lim - 1))
                    continue;
                w->keys[m] = k;
                ts[m] = key_ts(s, k);
                v[m] = (float)id;
                m++;
            }
            if (!m) { /* only the reserved key was drawn */
                w->keys[0] = lim - 2;
                ts[0] = key_ts(s, lim - 2);
                v[0] = (float)id;
                m = 1;
            }
            w->nkeys = m;
            w->invoke = now_ns();
            int rc = tsdbc_insert_series(c, ids[s], ts, v, m);
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            outage_note(s, rc, w->invoke);
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
                        q->seen[k - q->lo] = isnan(p.values[i]) ? 0 : (uint32_t)p.values[i]; /* a null shows as absent */
                    else
                        q->stray = true; /* a row at no key: invalid */
                }
                free(p.ts);
                free(p.values);
            } else {
                q->st = ST_FAIL;
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&read_damage, 1);
                if (rc == TSDB_ERR_UNAVAILABLE) { /* a volume is away: the connection is fine */
                    usleep(2000);
                } else {
                    tsdbc_close(c);
                    c = connect_retry();
                }
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
    for (useconds_t t = 0; t < us && !atomic_load(&stop) && !atomic_load(&nem_stop); t += 5000) {
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
    /* FNO is not here: what it protects (a log number is never handed out twice) is not visible to this workload, which
     * has no checker for it, so a flip there would only look like a pass. The damage to either slot, and the numbers
     * that skip ahead after it, are tests/test_fno.c's (test_slots, test_damaged_newest). */
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

/* ---- outage: one volume's directory goes away (hang, EIO, slow) and comes back, or the server is killed.
 *
 * What it checks beyond the model (the checker's 1-5 judge the history: an acknowledged write is never lost, a write
 * refused with UNAVAILABLE is never seen):
 *  - the server never ends by itself: a volume in a storage domain recovers in place (the root and a volume outside
 *    a domain still fail-stop, but this run has none of those faults);
 *  - within OUTAGE_RECOVER_S of the heal a write to the volume is acknowledged again;
 *  - while one volume is hung and DOWN, the other keeps at least 80% of the writes per second it did before (all
 *    the faults together, so that one slow second does not decide).
 * A hang is also ended by kill -9 now and then (the shim lives in the server: the next one opens a healthy
 * directory; opening a volume that is hung has no deadline of its own yet, O5). */
#define OUTAGE_RECOVER_S 30
/* A domain that went DOWN three times in ten minutes needs 60 s of good probes to leave DOWN (ha.md 15.2 "Hysteresis",
 * health.c: it is flapping, and recovering volumes into the next outage costs more than waiting). That is the
 * documented promise for such a volume, and a 300 s run reaches it all the time (the seed 1 and 2 runs of the
 * weekly CI failed on it: the volume was DOWN on purpose). Its bound is those 60 s plus OUTAGE_RECOVER_S. */
#define OUTAGE_FLAP_S (HEALTH_FLAP_NEED_MS / 1000) /* the server's own numbers (src/health_rules.h): one source */
#define OUTAGE_FLAP_WINDOW_S (HEALTH_FLAP_WINDOW_MS / 1000)
#define OUTAGE_FLAP_DOWNS HEALTH_FLAP_DOWNS
_Static_assert(OUTAGE_FLAP_S * 1000 == HEALTH_FLAP_NEED_MS && OUTAGE_FLAP_WINDOW_S * 1000 == HEALTH_FLAP_WINDOW_MS,
               "the flapping rule is whole seconds here");
#define OUTAGE_DOWN_S 5.0 /* DOWN is declared 2.5 s (the threshold here) plus a probe period or two after the hang
                           * starts, and the calls stuck before it are released then; the healthy volume is measured from here */

/* The faults that take a volume's domain DOWN (a hang past the threshold, eio: three probes fail), each at most once,
 * since the server that runs now began: the server's memory of DOWNs (health.c) goes with the process. A relapse from
 * RECOVERING is a DOWN too, and comes from a fault of this list as well, so the count is never below the server's. */
static int64_t down_faults[2][256];
static int n_down_faults[2];

static void outage_down_fault(int v)
{
    if (n_down_faults[v] < (int)(sizeof down_faults[v] / sizeof *down_faults[v]))
        down_faults[v][n_down_faults[v]++] = now_ns();
}

/* Is volume v's domain, healed now, one the health monitor holds DOWN for 60 s of good probes? */
static bool outage_flapping(int v)
{
    int64_t now = now_ns(), from = now - (int64_t)OUTAGE_FLAP_WINDOW_S * 1000000000LL;
    int n = 0;
    for (int i = 0; i < n_down_faults[v]; i++)
        if (down_faults[v][i] >= life_start_ns && down_faults[v][i] >= from)
            n++;
    return n >= OUTAGE_FLAP_DOWNS;
}

static void outage_ctl(const char *cmd, int v, const char *arg)
{
    char line[PATH_MAX + 64], out[256];
    snprintf(line, sizeof line, "%s %s%s%s", cmd, vol_dir[v], arg ? " " : "", arg ? arg : "");
    if (getenv("JEPSEN_OUTAGE_TRACE")) /* on the monotonic clock of the server's log lines */
        fprintf(stderr, "jepsen: [%.3f] %s\n", (double)now_ns() / 1e9, line);
    if (outagefs_cmd(octl, line, out, sizeof out)) {
        outage_problems++;
        fprintf(stderr, "  the shim refused '%s': %s\n", line, out);
    }
}

/* Waits until a write to volume v is acknowledged after `since` (ns). */
static void outage_await_recovery(int v, int64_t since, const char *what)
{
    int64_t t0 = now_ns();
    bool flap = outage_flapping(v);
    int bound = OUTAGE_RECOVER_S + (flap ? OUTAGE_FLAP_S : 0);
    outage_heals++;
    while (!atomic_load(&stop)) {
        if (atomic_load(&vol_last_ok[v]) > since) {
            double took = (double)(now_ns() - since) / 1e9;
            outage_judged++;
            if (flap) {
                outage_flap_judged++;
                outage_worst_flap_s = took > outage_worst_flap_s ? took : outage_worst_flap_s;
            } else {
                outage_worst_s = took > outage_worst_s ? took : outage_worst_s;
            }
            if (took > 0.75 * bound) {
                outage_near_miss = true;
                fprintf(stderr, "  volume %d: a write was acknowledged only %.1f s after the heal (bound %d s%s)\n", v,
                        took, bound, flap ? ", flapping" : "");
            }
            if (getenv("JEPSEN_OUTAGE_TRACE"))
                fprintf(stderr, "jepsen: [%.3f] volume %d takes a new write %.2f s after the heal (bound %d s%s)\n",
                        (double)now_ns() / 1e9, v, (double)(now_ns() - since) / 1e9, bound, flap ? ", flapping" : "");
            return;
        }
        if (now_ns() - t0 > bound * 1000000000LL) {
            outage_problems++;
            outage_judged++;
            fprintf(stderr, "  volume %d: no write acknowledged in %d s after the heal (%s%s)\n", v, bound, what,
                    flap ? "; its domain was flapping, 60 s of good probes included" : "");
            print_tail(slogpath); /* the server's side of it: the log of every start since the run began */
            return;
        }
        usleep(20000);
    }
    /* stop was set (a server that would not start, or the run ended another way): this heal has no verdict */
    fprintf(stderr, "  volume %d: the run ended before its heal was judged (%s)\n", v, what);
}

static void outage_tick(uint64_t r)
{
    pid_t pid = atomic_load(&server_pid);
    int st = 0;
    if (pid > 0 && waitpid(pid, &st, WNOHANG) == pid) {
        outage_problems++;
        fprintf(stderr, "  the server ended by itself (status %#x): a volume in a storage domain must not stop it\n",
                st);
        server_gone();
        start_server(true);
        restarts++;
        return;
    }
    int v = (int)((r >> 3) % 2), h = 1 - v, kind = (int)((r >> 8) % 7);
    char what[64];
    static const char *names[] = {"hang 8 s", "hang 8 s", "hang 1.2 s", "eio 2 s", "slow 40 ms 3 s",
                                  "hang 2 s and kill -9", "kill -9"};
    snprintf(what, sizeof what, "%s on volume %d", names[kind], v);
    if (getenv("JEPSEN_VERBOSE"))
        fprintf(stderr, "jepsen: outage: %s\n", what);
    int64_t b_t = now_ns();
    long long b_ok = atomic_load(&vol_ok[h]);
    if (kind < 5)
        usleep(800000); /* the healthy volume's pace before the fault */
    double base = (double)(atomic_load(&vol_ok[h]) - b_ok) * 1e9 / (double)(now_ns() - b_t);
    if (kind == 6 || kind == 5) {
        if (kind == 5) {
            outage_ctl("hang", v, NULL);
            usleep(2000000);
        }
        if (getenv("JEPSEN_OUTAGE_TRACE"))
            fprintf(stderr, "jepsen: [%.3f] kill -9 (%s)\n", (double)now_ns() / 1e9, what);
        stop_server(SIGKILL); /* the shim lives in the server: the hang ends with it, the next server opens a healthy directory */
        server_gone();
        start_server(true);
        restarts++;
        outage_kills++;
        return;
    }
    outage_faults++;
    if (kind <= 1 || kind == 3)
        outage_down_fault(v);
    if (kind <= 1) { /* past DOWN: the volume answers UNAVAILABLE, the other goes on at its pace */
        outage_ctl("hang", v, NULL);
        for (int i = 0; getenv("JEPSEN_VERBOSE") && i < 16; i++) { /* the pace of both volumes, per half second */
            long long a = atomic_load(&vol_ok[v]), b = atomic_load(&vol_ok[h]);
            usleep(500000);
            fprintf(stderr, "jepsen:   +%.1f s: hung volume %lld, healthy %lld writes\n", (i + 1) * 0.5,
                    (long long)atomic_load(&vol_ok[v]) - a, (long long)atomic_load(&vol_ok[h]) - b);
            if (i == 9)
                break;
        }
        if (!getenv("JEPSEN_VERBOSE"))
            usleep((useconds_t)(OUTAGE_DOWN_S * 1e6));
        int64_t t0 = now_ns();
        long long ok0 = atomic_load(&vol_ok[h]);
        usleep(3000000);
        double dt = (double)(now_ns() - t0) / 1e9;
        /* an outage that began shortly before the run's end lasts past it: the clients are gone by then (stop is set
         * and stays set), and the volume that "wrote nothing" was measured with nobody writing. Only a window in
         * which they ran all through counts (seed 2 of the gate failed at 67% for exactly this). */
        if (!atomic_load(&stop)) {
            healthy_ops += (double)(atomic_load(&vol_ok[h]) - ok0);
            healthy_secs += dt;
            healthy_base_ops += base * dt;
        }
    } else if (kind == 2) {
        outage_ctl("hang", v, NULL);
        usleep(1200000);
    } else if (kind == 3) {
        outage_ctl("eio", v, NULL);
        usleep(2000000);
    } else {
        outage_ctl("slow", v, "40");
        usleep(3000000);
    }
    outage_ctl("heal", v, NULL);
    outage_await_recovery(v, now_ns(), what);
}

static void *nemesis_thread(void *arg)
{
    (void)arg;
    uint64_t r = mix(seed + 77);
    while (!atomic_load(&stop) && !atomic_load(&nem_stop)) {
        r = mix(r);
        nap(idle_mutant_long ? 5000000 : 150000 + (useconds_t)(r % 600000),
            ((!strcmp(nemesis, "power") && crashfs) && !idle_mutant && !idle_mutant_long) || md);
        if (atomic_load(&stop) || atomic_load(&nem_stop))
            break;
        int64_t dts = atomic_exchange(&death_ts, 0);
        if (dts && nreact < (int)(sizeof react_ms / sizeof *react_ms))
            react_ms[nreact++] = (double)(now_ns() - dts) / 1e6;
        pthread_mutex_lock(&server_lock);
        atomic_store(&nem_busy, true);
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
            atomic_store(&death_ts, 0); /* a death seen while we were killing it is ours */
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
        } else if (outage) {
            outage_tick(r);
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
        atomic_store(&nem_busy, false);
        pthread_mutex_unlock(&server_lock);
    }
    return NULL;
}

/* Stamps the moment the server is first seen dead while the nemesis naps (see death_ts). */
static void *death_thread(void *arg)
{
    (void)arg;
    while (!atomic_load(&stop)) {
        usleep(2000);
        pid_t pid = atomic_load(&server_pid);
        siginfo_t si;
        si.si_pid = 0;
        if (pid > 0 && !atomic_load(&nem_busy) && atomic_load(&death_ts) == 0 &&
            waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) == 0 && si.si_pid == pid && !atomic_load(&nem_busy) &&
            atomic_load(&server_pid) == pid) {
            int64_t zero = 0;
            atomic_compare_exchange_strong(&death_ts, &zero, now_ns());
        }
    }
    return NULL;
}

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* -------------------------------------------------------------- checker */

static int violations;

#define VIOLATION(...)                         \
    do {                                       \
        if (violations++ < 20)                 \
            fprintf(stderr, "  " __VA_ARGS__); \
    } while (0)

int feedcheck_dir(const char *dir, bool crashed, char *why, size_t cap); /* tests/jepsen/feedcheck.c */

static void feed_check(const char *when, bool crashed)
{
    char why[512];
    feed_checks++;
    if (feedcheck_dir(dir, crashed, why, sizeof why) != 0)
        VIOLATION("feed, %s: %s\n", when, why);
    else if (!crashed && why[0])
        printf("feed, %s: %s\n", when, why);
}

/* the CLI as an operator runs it before the first start (never under the shim) */
static int feed_cli(bool enable)
{
    const char *cli = getenv("JEPSEN_TSDB") ? getenv("JEPSEN_TSDB") : "./build/tsdb";
    pid_t pid = fork();
    if (pid == 0) {
        unsetenv("LD_PRELOAD");
        /* short retention and a small size bound: the cleaner runs, and its floor is under every fault too */
        if (enable)
            execl(cli, "tsdb", dir, "feed", "enable", "--retention", "5s", "--max-bytes", "3M", (char *)NULL);
        else
            execl(cli, "tsdb", dir, "feed", "info", (char *)NULL);
        _exit(127);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "jepsen: %s %s feed %s failed\n", cli, dir, enable ? "enable" : "info");
        return 1;
    }
    return 0;
}

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

/* QUERY_LATEST answers (lats), against the writes. The model: a key holds a value row (an insert, or a replace that
 * wrote it), a null row (a null write), or nothing (a delete, or a replace that did not write it); the newest row of
 * a series is the highest key before `end` that holds a value (or, with nulls, any row). Per answer (series s,
 * invoked at I, completed at C), all of it only from what is certain:
 *  L1 the answer is real: a value is that of a write that wrote the key it names, began before C and did not
 *     definitely fail; a null names a key some null write covers (same conditions); the key is before `end`;
 *  L2 not stale at its own key: no write that was acknowledged before I and began after the value's write
 *     completed covers the key (it would have replaced the value, hidden or removed it); a null likewise against
 *     the null writes that could be its row;
 *  L3 nothing newer: no key between the answer and `end` (all keys below it when the answer is none) holds a row
 *     that an acknowledged write (completed before I) put there, unless a write that could be ordered after it and
 *     began before C removes it: deletes always, and replaces that did not write the key (one that did leaves a
 *     row there; the write itself is no remover); null writes too when the answer skips nulls. */
static bool latest_removes(int series, uint32_t k, uint32_t self, int64_t after, int64_t before, bool nulls_remove)
{
    size_t slot = koff[series] + k;
    for (uint32_t i = dcnt[slot]; i < dcnt[slot + 1]; i++) {
        const wop *d = &writes[dlist[i]];
        /* the write being checked is no remover of its own row, nor is a replace that wrote the key (it leaves a
         * row there, as new as itself): without this L3 never required a row that a replace wrote */
        if (dlist[i] == self || (d->kind == W_REPLACE && wrote_key(dlist[i], k)))
            continue;
        if ((nulls_remove || d->kind != W_NULL) && d->st != ST_FAIL && d->complete >= after && d->invoke <= before)
            return true;
    }
    return false;
}

/* What write w leaves at key k: 1 a value, 2 a null, 0 nothing. incover: w is in the cover list of k (an insert
 * there wrote it: no search of its keys, which a large insert has thousands of). */
static int row_of(const wop *w, uint32_t k, bool incover)
{
    if (w->kind == W_NULL)
        return k >= w->lo && k < w->hi ? 2 : 0;
    if (w->kind == W_DELETE)
        return 0;
    if (w->kind == W_INSERT && incover)
        return 1;
    for (uint32_t i = 0; i < w->nkeys; i++)
        if (w->keys[i] == k)
            return 1;
    return 0;
}

static int latest_ok_answers, latest_violations;

static void check_latest(uint32_t nw, const uint32_t *wc, const uint32_t *wl)
{
    uint32_t nl = atomic_load(&nlats);
    if (nl > MAX_LATEST)
        nl = MAX_LATEST;
    for (uint32_t i = 0; i < nl; i++) {
        const lop *l = &lats[i];
        if (l->st != ST_OK)
            continue;
        int s = l->series;
        latest_ok_answers++;
        int before = violations;
#define LVIOL(...)                                                                                                    \
    do {                                                                                                              \
        char lm_[400];                                                                                                \
        snprintf(lm_, sizeof lm_, __VA_ARGS__);                                                                       \
        VIOLATION("latest %u (series %d, with_nulls %d, end %lld, %lld..%lld): %s", i, s, l->with_nulls,              \
                  (long long)(l->end == INT64_MAX ? -1 : l->end), (long long)l->invoke, (long long)l->complete, lm_); \
    } while (0)
        if (l->rstatus != 0) {
            LVIOL("status %d for a series that exists and that the user may read\n", l->rstatus);
            goto next;
        }
        int64_t ak = -1; /* the answer's key */
        if (l->kind == TSDB_LATEST_NONE) {
            /* nothing: L3 below with every key */
        } else if (l->kind != TSDB_LATEST_VALUE && l->kind != TSDB_LATEST_NULL) {
            LVIOL("kind %d\n", l->kind);
            goto next;
        } else if (l->kind == TSDB_LATEST_NULL && !l->with_nulls) {
            LVIOL("a null without asking for nulls\n");
            goto next;
        } else if ((ak = ts_key(s, l->ts)) < 0 || l->ts >= l->end) {
            LVIOL("a row at %lld, which is no key or not before the end\n", (long long)l->ts);
            goto next;
        } else {
            uint32_t k = (uint32_t)ak;
            size_t slot = koff[s] + k, n = wc[slot + 1] - wc[slot];
            const uint32_t *cov = wl + wc[slot];
            if (l->kind == TSDB_LATEST_VALUE) {
                uint32_t v = l->val;
                if (v == 0 || v >= nw || writes[v].series != s || row_of(&writes[v], k, false) != 1 ||
                    writes[v].st == ST_FAIL || writes[v].invoke > l->complete) {
                    LVIOL("value %u at key %u: no such write, or it did not write that key, failed or began later\n", v,
                          k);
                    goto next;
                }
                for (size_t j = 0; j < n; j++) {
                    const wop *w = &writes[cov[j]];
                    if (cov[j] != v && w->st == ST_OK && w->complete < l->invoke && older(v, cov[j])) {
                        LVIOL("value %u at key %u, but write %u (%s) was acknowledged before the request and began "
                              "after %u completed: stale\n",
                              v, k, cov[j], w->kind == W_INSERT ? "insert" : w->kind == W_NULL ? "null" : "delete or replace", v);
                        break;
                    }
                }
            } else {
                bool any = false;
                for (size_t j = 0; j < n; j++)
                    if (row_of(&writes[cov[j]], k, true) == 2 && writes[cov[j]].st != ST_FAIL &&
                        writes[cov[j]].invoke <= l->complete)
                        any = true;
                if (!any) {
                    LVIOL("a null at key %u, which no null write could have put there\n", k);
                    goto next;
                }
                for (size_t j = 0; j < n; j++) {
                    const wop *w = &writes[cov[j]];
                    if (w->st != ST_OK || w->complete >= l->invoke)
                        continue;
                    bool all_older = true; /* every null write that could be the row is older than w */
                    for (size_t x = 0; x < n && all_older; x++)
                        if (row_of(&writes[cov[x]], k, true) == 2 && writes[cov[x]].st != ST_FAIL &&
                            writes[cov[x]].invoke <= l->complete && !older(cov[x], cov[j]))
                            all_older = false;
                    if (all_older && row_of(w, k, true) != 2) {
                        LVIOL("a null at key %u, but write %u was acknowledged before the request and began after "
                              "every null write of the key completed: stale\n", k, cov[j]);
                        break;
                    }
                }
            }
        }
        if (violations > before)
            goto next;
        /* L3: a row above the answer that must exist */
        uint32_t top = nkeys[s];
        while (top > 0 && key_ts(s, top - 1) >= l->end)
            top--;
        for (int64_t k = (int64_t)top - 1; k > ak; k--) {
            size_t slot = koff[s] + (size_t)k, n = wc[slot + 1] - wc[slot];
            const uint32_t *cov = wl + wc[slot];
            for (size_t j = 0; j < n; j++) {
                const wop *w = &writes[cov[j]];
                int row = row_of(w, (uint32_t)k, true);
                if (w->st != ST_OK || w->complete >= l->invoke || row == 0 || (row == 2 && !l->with_nulls))
                    continue;
                if (latest_removes(s, (uint32_t)k, cov[j], w->invoke, l->complete, !l->with_nulls))
                    continue;
                LVIOL("%s at key %lld (write %u, acknowledged before the request), newer than the answer (%s%s key "
                      "%lld): stale or missing\n", row == 1 ? "a value" : "a null", (long long)k, cov[j],
                      l->kind == TSDB_LATEST_NONE ? "none" : l->kind == TSDB_LATEST_NULL ? "null at" : "value at",
                      "", (long long)ak);
                break;
            }
            if (violations > before)
                break;
        }
#undef LVIOL
    next:
        if (violations > before)
            latest_violations++;
    }
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
    uint32_t *wc, *wl; /* per key, the writes that cover it (not failed), by start time: check_latest reads them too */
    {
        wc = calloc(koff[nseries] + 1, sizeof *wc);
        uint32_t *wf = calloc(koff[nseries], sizeof *wf);
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
        wl = malloc((nwk + 1) * sizeof *wl);
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
        free(wf);
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
    if (latest_on)
        check_latest(nw, wc, wl);
    free(wl);
    free(wc);
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
                out[koff[s] + k] = isnan(p.values[i]) ? 0 : (uint32_t)p.values[i];
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
static uint8_t *fnull; /* final_state: the keys that hold a null row */
static int latest_final_checked, latest_final_problems; /* the final latest answers against the final rows */

/* The final state, quiet: what QUERY_LATEST must say for every series, with and without nulls, and as of a few ends,
 * is worked out from the rows just read. The first server since the open's table build and the warmer. */
static void final_latest(tsdbc *c, const uint32_t *final)
{
    for (int variant = 0; variant < 8; variant++) {
        bool wn = variant & 1;
        int64_t end = variant < 4 ? INT64_MAX : key_ts(0, (uint32_t)(nkeys[0] * (uint32_t)(variant - 3) / 5));
        tsdb_latest_result out[MAX_SERIES];
        tsdb_latest_opts o = {.end = end, .ends = NULL, .with_nulls = wn, .threads = 0};
        int rc = tsdbc_latest(c, ids, (size_t)nseries, &o, out);
        if (rc != 0) {
            VIOLATION("final latest request failed: %s\n", tsdbc_strerror(rc));
            latest_final_problems++;
            return;
        }
        for (int s = 0; s < nseries; s++) {
            int64_t want_k = -1;
            for (int64_t k = (int64_t)nkeys[s] - 1; k >= 0 && want_k < 0; k--)
                if (key_ts(s, (uint32_t)k) < end && (final[koff[s] + k] || (wn && fnull[koff[s] + k])))
                    want_k = k;
            int want_kind = want_k < 0 ? TSDB_LATEST_NONE : final[koff[s] + want_k] ? TSDB_LATEST_VALUE : TSDB_LATEST_NULL;
            const tsdb_latest_result *a = &out[s];
            latest_final_checked++;
            bool bad = a->status != 0 || a->kind != want_kind ||
                       (want_k >= 0 && (a->ts != key_ts(s, (uint32_t)want_k) ||
                                        (want_kind == TSDB_LATEST_VALUE &&
                                         (uint32_t)a->v.f32 != final[koff[s] + want_k])));
            if (bad) {
                latest_final_problems++;
                VIOLATION("final latest of series %d (with_nulls %d, end %lld): status %d kind %d ts %lld value %u, "
                          "the rows say kind %d at key %lld (ts %lld, value %u)\n",
                          s, wn, (long long)end, a->status, a->kind, (long long)a->ts, (uint32_t)a->v.f32, want_kind,
                          (long long)want_k, want_k >= 0 ? (long long)key_ts(s, (uint32_t)want_k) : 0LL,
                          want_k >= 0 ? final[koff[s] + want_k] : 0);
            }
        }
    }
}

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
    memset(fnull, 0, koff[nseries]);
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
            if (k >= 0) {
                final[koff[s] + k] = isnan(p.values[i]) ? 0 : (uint32_t)p.values[i];
                fnull[koff[s] + k] = isnan(p.values[i]);
            }
            else if (stray_rows++ < 5)
                fprintf(stderr, "  final: series %d has a row at %lld, which no write used\n", s, (long long)p.ts[i]);
        }
        free(p.ts);
        free(p.values);
    }
    if (latest_on)
        final_latest(c, final);
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

/* outage: the two volumes, and each series made in its own (series s in volume s % 2) before the server starts:
 * tsdbd never makes a volume or a directory by itself, and the clients' tsdbc_series then finds the series. */
static int make_volumes(void)
{
    tsdb *db;
    tsdb_options o;
    tsdb_options_init(&o);
    o.create_volumes = 1;
    for (int i = 0; i < 2; i++) {
        char rp[PATH_MAX];
        if ((mkdir(vol_dir[i], 0755) != 0 && errno != EEXIST) || !realpath(vol_dir[i], rp)) {
            fprintf(stderr, "jepsen: cannot make %s: %s\n", vol_dir[i], strerror(errno));
            return 1;
        }
        snprintf(vol_dir[i], sizeof vol_dir[i], "%s", rp);
    }
    uint32_t vi;
    int rc = tsdb_open(dir, &o, &db);
    if (rc == 0) {
        rc = tsdb_volume_add(db, vol_dir[0], &vi) || tsdb_volume_add(db, vol_dir[1], &vi);
        for (int s = 0; !rc && s < nseries; s++) {
            char name[32];
            tsdb_sid id;
            snprintf(name, sizeof name, "jepsen.%d", s);
            rc = tsdb_series_at(db, name, vol_dir[s % 2], &id);
        }
        tsdb_close(db);
    }
    if (rc)
        fprintf(stderr, "jepsen: cannot make the volumes (%d)\n", rc);
    return rc != 0;
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
                                   {"feed", no_argument, 0, 'E'},
                                   {"auth", no_argument, 0, 'A'},           {"outagefs", required_argument, 0, 'o'},
                                   {"mutant-idle", no_argument, 0, 'I'},
                                   {"mutant-longnap", no_argument, 0, 'L'},
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
        case 'w':
            if (strcmp(optarg, "kv") && strcmp(optarg, "classes"))
                return 2;
            classes = !strcmp(optarg, "classes");
            break;
        case 'V': verify = true; break;
        case 'A': auth = true; break;
        case 'E': feed = true; break;
        case 'I': idle_mutant = true; break;
        case 'L': idle_mutant_long = true; break;
        case 'o': outagefs_lib = optarg; break;
        default: return 2;
        }
    }
    md = !strcmp(nemesis, "misdirect");
    rot = !strcmp(nemesis, "rot");
    outage = !strcmp(nemesis, "outage");
    bool shim = !strcmp(nemesis, "power") || !strcmp(nemesis, "eio") || md || outage;
    if (!dir || (shim && !outage && !crashfs) || (outage && (!outagefs_lib || classes || auth)) ||
        ((md || rot) && classes)) {
        fprintf(stderr, "usage: jepsen --dir DIR [--nemesis kill|power|pause|eio|misdirect|rot|outage|none] [--seconds S]\n"
                        "              [--clients N] [--seed N] [--crashfs build/libcrashfs.so] [--workload kv|classes]\n"
                        "              [--verify-checksums]   (misdirect and rot: workload kv) [--auth] [--feed]\n"
                        "              [--outagefs build/liboutagefs.so]   (outage: workload kv, no login)\n");
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
    char cmd[6 * PATH_MAX + 256];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (outage) { /* the volumes and the shim's socket beside the directory */
        for (int i = 0; i < 2; i++)
            snprintf(vol_dir[i], sizeof vol_dir[i], "%s.vol%c", dir, 'a' + i);
        snprintf(octl, sizeof octl, "%s.outagefs", dir);
        snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s' '%s' '%s' '%s.snap0' '%s.snap1'", dir, vol_dir[0], vol_dir[1],
                 octl, octl, octl);
    }
    if (system(cmd) != 0)
        return 2;
    snprintf(markpath, sizeof markpath, "%s.eio", dir);
    if (md || rot)
        snprintf(slogpath, sizeof slogpath, "%s.e%d.server.log", dir, epoch);
    else
        snprintf(slogpath, sizeof slogpath, "%s.server.log", dir);
    unlink(slogpath);
    if (outage) { /* a log kept by an earlier run of this directory and seed is not this run's */
        snprintf(outage_keeppath, sizeof outage_keeppath, "%s.outage-s%llu.server.log", dir, (unsigned long long)seed);
        unlink(outage_keeppath);
    }
    writes = calloc(MAX_OPS, sizeof *writes);
    reads = calloc(MAX_OPS, sizeof *reads);
    /* QUERY_LATEST: with nulls written and every answer checked against the acknowledged writes. Not where damage is
     * injected (a damaged part may decode to wrong rows, which the reads count as known) nor under outage (a volume is
     * away for seconds: its series answer UNAVAILABLE, which only the volume's own calls expect) nor in classes. */
    latest_on = !classes && !md && !rot && !outage;
    if (latest_on)
        lats = calloc(MAX_LATEST, sizeof *lats);
    nseries = classes ? CL_SERIES : NSERIES;
    make_keys();
    fnull = calloc(koff[nseries] + 1, 1);
    if (auth && make_users())
        return 2;
    if (outage && make_volumes())
        return 2;
    if (feed && (outage || md || rot || classes)) {
        fprintf(stderr, "jepsen: --feed goes with the kv workload under kill, power, pause and eio\n");
        return 2;
    }
    if (feed && ((mkdir(dir, 0755) != 0 && errno != EEXIST) || feed_cli(true)))
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
    pthread_t dth;
    bool dmon = !strcmp(nemesis, "power") && crashfs;
    if (strcmp(nemesis, "none"))
        pthread_create(&nem, NULL, nemesis_thread, NULL);
    if (dmon)
        pthread_create(&dth, NULL, death_thread, NULL);
    usleep((useconds_t)(seconds * 1e6));
    if (outage) { /* the clients go on until the heal the nemesis is in has been judged (up to its bound, 90 s) */
        atomic_store(&nem_stop, true);
        pthread_join(nem, NULL);
        atomic_store(&stop, true);
    } else {
        atomic_store(&stop, true);
        if (strcmp(nemesis, "none"))
            pthread_join(nem, NULL);
    }
    if (dmon)
        pthread_join(dth, NULL);
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
    if (feed && !damaged) {
        feed_check("after the final clean stop", false);
        feed_cli(false); /* what the cleaner did: its floor and what it kept */
    }
    pthread_mutex_unlock(&server_lock);

    uint32_t nw = atomic_load(&nwrites) - 1, nr = atomic_load(&nreads);
    unsigned ok = 0, info = 0, fail = 0, rok = 0, dels = 0, reps = 0, nulls_ok = 0;
    for (uint32_t w = 1; w <= nw && w < MAX_OPS; w++) {
        ok += writes[w].st == ST_OK, info += writes[w].st == ST_INFO, fail += writes[w].st == ST_FAIL;
        dels += writes[w].kind == W_DELETE && writes[w].st == ST_OK;
        reps += writes[w].kind == W_REPLACE && writes[w].st == ST_OK;
        nulls_ok += writes[w].kind == W_NULL && writes[w].st == ST_OK;
    }
    for (uint32_t i = 0; i < nr && i < MAX_OPS; i++)
        rok += reads[i].st == ST_OK;
    if (!(md || rot)) {
        printf("jepsen %s: %.0f s, %d clients, %d restarts; writes %u ok / %u indeterminate / %u failed, reads %u ok; "
               "%u deletes and %u replaces ok\n",
               nemesis, seconds, nclients, restarts, ok, info, fail, rok, dels, reps);
    }
    if (feed)
        printf("feed: %d directory checks (retained segments whole, numbers increasing, nothing lost above the floor)\n",
               feed_checks);
    if (crashfs && !md)
        printf("servers under the shim: %d faulty, %d clean (fallback after a failed start), %d died while starting\n",
               lives_faulty, lives_clean, lives_died_starting);
    if (!damaged && memcmp(final, again, koff[nseries] * sizeof *final) != 0)
        VIOLATION("the state changed across a clean restart\n");
    if (stray_rows)
        VIOLATION("%d rows at timestamps no write used in the final reads\n", stray_rows);
    int v = check(damaged ? NULL : final); /* counts the follower's violations too */
    if (latest_on) {
        printf("latest: %u null writes ok, %d answers checked, %d final answers checked, %d answers wrong\n", nulls_ok,
               latest_ok_answers, latest_final_checked, latest_violations + latest_final_problems);
        if (seconds >= 10 && latest_ok_answers == 0) {
            fprintf(stderr, "  no latest request was answered in %.0f s: the check tested nothing\n", seconds);
            v++;
        }
    }
    if (md || rot)
        v = epoch_report(v, ok);
    if (classes)
        v += check_classes(final);
    if (!strcmp(nemesis, "eio")) {
        printf("eio: %d fail-stops\n", failstops);
        v += eio_problems;
    }
    if (outage) {
        double rate = healthy_secs > 0 ? healthy_ops / healthy_secs : 0, base = healthy_secs > 0 ? healthy_base_ops / healthy_secs : 0;
        printf("outage: %d volume faults and %d kills; the other volume wrote %.0f/s during outages (%.0f/s before)\n",
               outage_faults, outage_kills, rate, base);
        if (healthy_secs > 5 && rate < 0.8 * base) {
            fprintf(stderr, "  the healthy volume kept %.0f%% of its writes per second during outages (at least 80%% expected)\n",
                    base > 0 ? 100 * rate / base : 0);
            v++;
        }
        if (seconds >= 30 && outage_faults < 2) {
            fprintf(stderr, "  only %d volume faults in %.0f s: the nemesis was idle\n", outage_faults, seconds);
            v++;
        }
        printf("outage heals: %d, %d judged (%d of a flapping domain; slowest %.1f s, slowest flapping %.1f s), "
               "%d not judged\n",
               outage_heals, outage_judged, outage_flap_judged, outage_worst_s, outage_worst_flap_s,
               outage_heals - outage_judged);
        v += outage_problems;
    }
    if (!strcmp(nemesis, "power")) {
        v += death_problems;
        /* the nemesis must keep killing: a server left dead until the next tick (the clients idle, 40 restarts in
         * 20 s instead of 300) tests far less, and passes every check above. Judged by the nemesis's own reaction
         * to a server that died by itself, not by a count of restarts: a count also falls when the machine is busy
         * and the servers start slowly (0 violations, "only 60 restarts in 20 s" under load). The median of
         * the reaction stays within a few polls of 5 ms whatever the load; a nemesis that sleeps through its naps
         * (150 to 750 ms) has a median of hundreds. */
        if (crashfs) {
            double med = 0, p90 = 0;
            if (nreact > 0) {
                qsort(react_ms, (size_t)nreact, sizeof *react_ms, cmp_double);
                med = react_ms[nreact / 2];
                p90 = react_ms[(int)((double)nreact * 0.9)];
            }
            printf("nemesis: %d server deaths seen while napping, reaction median %.0f ms, p90 %.0f ms (%d restarts)\n",
                   nreact, med, p90, restarts);
            /* a nemesis that wakes too rarely has too few reactions to judge: a nap is at most 0.75 s, so a
             * restart every 2 s is far below any honest run, and the busiest one had 60 restarts in 20 s */
            if ((double)restarts < seconds / 2) {
                fprintf(stderr, "  only %d restarts in %.0f s (at least %.0f expected): it was idle\n", restarts,
                        seconds, seconds / 2);
                v++;
            }
            if (nreact >= 5 && med > 100) {
                fprintf(stderr, "  the nemesis took a median of %.0f ms to notice a dead server (at most 100 expected): "
                        "it was idle\n", med);
                v++;
            }
        }
    }
    unlink(markpath);
    if (outage && (v || outage_near_miss)) {
        /* what a failed or a close run has to show: the server's side of it (the log of every start of the run), under
         * a name the next run does not unlink; CI uploads /tmp/tsdb-jepsen-ci*.log */
        if (rename(slogpath, outage_keeppath) == 0)
            fprintf(stderr, "  the server log of this run is kept in %s\n", outage_keeppath);
    } else if (!v && !getenv("JEPSEN_KEEP_LOG")) {
        unlink(slogpath);
    }
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
