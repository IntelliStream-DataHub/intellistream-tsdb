/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A Jepsen-style test of tsdbd: clients write and read through the network
 * while a nemesis crashes the server, and a checker verifies the history.
 *
 *   jepsen --dir DIR [--nemesis kill|power|pause|eio|misdirect|rot|outage|settle-steps|store-eio|store-stuck|
 *                               store-lost|salvage|fill|single-copy|none] [--seconds 20]
 *          [--clients 8] [--seed N] [--crashfs build/libcrashfs.so] [--outagefs build/liboutagefs.so]
 *          [--workload kv|classes] [--verify-checksums] [--auth] [--feed] [--tier N]
 *
 * Tier (--tier N; R7/R15, docs/design-cluster-data.md 10.2, §15.9): the database lives in DIR/db with N store
 * directories in DIR/tier/s0..s(N-1) configured via `tsdb DIR/db tier add` (`TIERS`), with timing overrides in
 * TSDB_TEST_TIER=grace_s=1,keep_s=5,eager=1 (TSDB_TEST_TIER_I_KNOW=1). Set JEPSEN_TIER_LEGACY=1 for the legacy
 * TSDB_TEST_TIER=k=..,m=..,dirs=.. path without TIERS. R15 tier nemeses (default 6 stores, 1 for single-copy, under
 * outagefs): `store-eio` (EIO on one store: reads decode around it, server stays up, heals), `store-stuck` (hung I/O
 * on one store: DOWN_STALLED, reads decode around it; kills `downnoread`), `store-lost` (wipe one store + live
 * `tsdb tier lost STORE --replace`: verifies pre-wipe live chunks rebuilt; kills `lostnoreplace`), `salvage` (wipe
 * m+1 stores, verify E15 TSDB_ERR_IO & stats before salvage, run `tsdb tier salvage`, verify young/surviving data and
 * live latest-table invalidation; kills `salvagelatest`), `fill` (outagefs `fill 92` on fast root: UNAVAILABLE FULL
 * refusals, reads go on, writes resume at 50%; kills `fullnoflag`), `single-copy` (1-store `--single-copy` 1+0 under
 * outagefs `rate`/`slow` and SIGKILL restarts). With the tier the workloads send more deletes, replaces and large
 * inserts (also into old keys, which a settle has taken or is taking), and the run ends with a settle check
 * (tier_check): `.ec` parts, `settled_` and `retired_*.ec` files in dbdir and the settle lines in the server logs
 * (JEPSEN_SETTLE_LINE, default "settled ") must reach a minimum (JEPSEN_MIN_SETTLES; 0 turns it off, for a tsdbd
 * without settles), so a run in which nothing settled fails: it tested nothing of the tier. The parts per partition
 * at the end are reported (settled parts count toward insert backpressure, finding 11). A kv or classes run with the
 * tier reads the database as it lies on disk: no follower (a follower refuses a tier volume, D7), and classes' final
 * "one part per partition" is only "no fewer rows than keys" (an `.ec` part is never merged).
 * The eio nemesis may hit a store file: a failed store stops that process's settles and nothing else (the server
 * goes on, reads go on), so a failure on a file under DIR/tier needs no fail-stop, and the minimum is 1.
 * settle-steps (needs --tier and --crashfs): every faulty tsdbd starts with TSDB_SETTLE_CRASH_AT=<step>[:n] (after-put,
 * after-ec-tmp, after-intent, between-renames, before-unlink) and CRASHFS_AT_EXIT=1 (no random power loss): it ends
 * at that step with exit 99, which the shim turns into a power cut (everything not synced is rolled back), the
 * nemesis restarts it with the next step, and a server that has not reached its step in SETTLE_WAIT_S is killed. A
 * random power cut seldom lands inside a swap that lasts milliseconds; this one always does.
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
 * random subset of unsynced directory operations; the others get the old bytes back where a durable sector was rewritten and
 * not synced, as a disk may, never zeros: round 13), pause
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
 * Not a wrong answer but the server itself (round 11; the same verdict, VIOLATION, under every nemesis and under none): a tsdbd that
 * ends by itself (a crash, an exit of its own; end_report: only what the harness sent, or the shim's power loss, or an EIO's fail-stop
 * is expected) or that stops answering (every request has a deadline, REQ_DEADLINE_DEFAULT_S) or whose stop never ends; a tsdbd that
 * will not start for a reason of its own (round 14: it exits 0, or exits with no line naming a failure of the machine, or is alive with no
 * port file and the machine not stalled: no_port_is_noted; the follower too) is a VIOLATION (exit 1), and only what the machine did (io_uring:
 * Cannot allocate memory, a stall: a thread of the server in D state for 10 s, this thread not scheduled for a second) is the environment
 * (exit 2). Liveness, how soon things
 * resume after a fault, is INCONCLUSIVE (exit 3) and never a violation.
 * Round 12 (what the harness looks at before it acts, and where it cannot see):
 *  - a server that is crashing when the harness kills it (its core dump keeps it from being a zombie, so a SIGKILL would turn the end
 *    into "killed by the harness") is told by /proc (CoreDumping, a fatal signal pending that the harness did not send) before every
 *    SIGKILL: crash_check. A crash in the microseconds between the kernel taking the signal off the queue and starting the dump, at the
 *    very moment of a kill, is not seen.
 *  - a request that misses its deadline while a thread of the server has been in D state (the kernel's: a file system that stalls) for
 *    10 s at a stretch is INCONCLUSIVE (kernel-stall), one with no such thread is a VIOLATION. A D state that is tsdbd's own looks the same
 *    from outside.
 *  - a server that has not listened yet is judged by start_server_run alone (round 14 reversed round 12's "never came up: exit 2"): see above.
 *  - under power and eio the last server, which the quiet tail leaves to the clients, is a clean one (no shim, so nothing ends it but the
 *    harness): a hung worker then reaches the deadline.
 *  - under misdirect and rot a server's end is a fail-stop on damage (not a violation) only as exit 1, with a log that names the damage, in an
 *    epoch where a fault was injected (a "tsdbd: storage failure, exiting" line); exit 0 of its own, or exit 1 without a fault, without that
 *    line or without damage in the log, is a violation.
 * The inherent limit: a hang or a self-stop (a SIGSTOP of its own) that a planned fault ends before the deadline (the next kill or plug, the
 * pause nemesis' SIGCONT, an outage that kills the server) cannot be told from slowness: a worker that hangs for good is found only while its
 * server lives past the deadline, which the quiet tail (every nemesis but outage, misdirect and rot) and a run with no nemesis give it.
 * tests/jepsen/mutant-crash.sh (make jepsen-selftest) plants each of these and requires the verdict.
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
#include <glob.h>
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
#ifdef __linux__
#include <sched.h>
#include <sys/prctl.h>
#endif
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../../src/health_rules.h"
#include "../../src/tiers.h"
#ifdef TSDB_EC
#include "../../src/eccache.h"
#include "../../src/tier.h"
#include "../../src/tierctl.h"
#endif
#include "../harness.h"
#include "../outagefs_ctl.h"
#include "outage_judge.h"

#define NSERIES 4        /* kv */
#define NKEYS 6000
#define T0 1700000000000LL
#define KEY_MS 2000      /* keys every 2 s: 6000 keys span 3.3 hours */
#define PARTITION_MS 3600000LL
#define WAL_MAX_BATCH_ROWS 500
#define WAL_MAX_BATCH_ROWS_STR "500"
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
static uint32_t max_ops = MAX_OPS; /* JEPSEN_MAX_OPS: a test hook of the selftest (the room for the history, never above MAX_OPS) */
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
/* What the nemesis did, and how long servers lay dead. Two sources, neither of them the nemesis's own look at the server:
 *  - the faults: counted where the nemesis issues one (nem_fault: a kill, a pause, an EIO the shim logged in its mark file, a
 *    plug the shim logged in its pull file, a volume taken away, a restart over damage), each with its time, so the
 *    run can be judged for the number of them and for their spread over the run;
 *  - the servers: a watcher thread (watcher) that polls the server pid every WATCH_POLL_MS (waitid with WNOWAIT: it reaps
 *    nothing) and stamps each death and each restart on its own clock. It counts the stretches with no server (from the
 *    poll that saw it gone to the poll that saw its successor, or to the run's end) and the time a server was up and
 *    listening. A nemesis that has stopped looking leaves a dead server unseen by itself and is seen by the watcher.
 * The nemesis thread touches the fault counters only; the watcher thread touches the watcher's numbers only; main reads
 * them after joining each. */
#define MAX_FAULT_TIMES 4096
static int nem_faults;                 /* faults the nemesis issued (nem_fault) */
static int64_t nem_fault_ns[MAX_FAULT_TIMES], nem_fault_end_ns[MAX_FAULT_TIMES]; /* when each was issued and when it was over (the first
                                       * MAX_FAULT_TIMES): a pulled plug is over when the server is dead, which takes seconds when the machine is busy */
static int pulls_issued;               /* power: SIGUSR2 sent to a live server; nem_faults counts those the shim logged */
static int eio_own;                    /* eio: marks the shim made by its own odds, with no SIGUSR2 of the nemesis before them: not faults of it
                                       * (unless the server went on and the nemesis pulled its plug: that act is one) */
static _Atomic int64_t nem_act_ns;     /* when the last fault was issued or over (0: none yet): the watcher's clock of the nemesis' activity */
static atomic_int nem_inprog;          /* faults being carried out now (a plug asked for and the server still alive, an EIO asked for and no mark yet) */
static int64_t nem_t0_ns;              /* when the nemesis started */
static bool nem_clock;                 /* the nemesis is running (the setup and the final checks are not its time) */
static int64_t nem_idle_ns;            /* power, kill, eio: time inside the fault step that was neither a server starting nor the wait for a pulled one to die
                                       * (kill and eio: the time before the signal); no CPU wait is taken off it (see the comment of nemesis_check) */
static _Atomic int64_t stop_ns;        /* when the run's time was up (0: not yet) */
static pthread_t watch_th;
static bool watch_started;
static atomic_bool watch_excused;      /* the checker's own work while the server is dead (feed_check): not the nemesis lying idle */
static int64_t watch_dead_total_ns, watch_dead_worst_ns, watch_live_ns; /* written by the watcher only */
static int64_t watch_free_ns;          /* the part of the listening time in which the nemesis was carrying no fault out (a plug pulled that
                                       * the server takes seconds to feel is the nemesis at work, not an opportunity it let go) */
static int64_t watch_step_worst_ns;    /* the longest stretch a server was listening with a fault step of the nemesis in progress (kill: it is over when the server
                                       * has died and a new one listens, so honest, none to speak of) */
static int64_t watch_tail_ns;          /* the listening time since the last fault, as the run's time was up */
static int64_t watch_stop_worst_ns;    /* the longest stretch a server lay stopped (SIGSTOP, state T in /proc), as the watcher saw it */
static int64_t watch_gap_worst_ns;     /* the longest stretch a server was listening with no fault issued or in progress */
static int64_t watch_live3_ns[3];      /* the same per third of the run (nem_third_start) */
static int watch_deaths, watch_restarts;
#define WATCH_POLL_MS 50
#define NEM_DEAD_MAX_MS 250   /* no server may lie dead longer than this, as the watcher sees it (to within two polls): the nemesis
                               * restarts at once (honest: 50 ms, one poll to the next, 100 to 150 ms in a few of 300 runs with a load average of 90) */
#define NEM_STOP_MAX_MS 1500  /* no server may lie stopped longer than this (the pause nemesis stops one for 100 to 400 ms): a nemesis that
                               * never continues it leaves every client waiting */
#define NEM_KILL_STEP_MAX_MS 400 /* kill: how long a server may be listening while a kill step is in progress before the time counts as free (not as
                               * the nemesis at work): the step is a SIGKILL and a start, in which a server listens for a poll or two (honest worst 200 ms in 96 runs, 106 ms with 48 busy loops: see nemesis_check) */
#define NEM_GAP_SHARE 0.1     /* the bound on a listening stretch with no fault: this share of the run, between NEM_GAP_MIN_S and NEM_GAP_MAX_S */
#define NEM_GAP_MIN_S 2.0
#define NEM_GAP_MAX_S 5.0
#define NEM_IDLE_SHARE 0.05   /* the idle time inside the fault steps stays below this share of the run */
#define KV_MIN_WRITES 100     /* the least acknowledged writes of a run of 10 s or more (see the kv coverage check) */
#define KV_MIN_DELREP 5       /* the least acknowledged deletes and replaces together */
#define NEM_MIN_FAULTS 3      /* the least faults of any run of 10 s or more */
#define EIO_STOP_WAIT_S 20    /* the longest an honest server takes to fail-stop after a file the engine relies on failed (EIO) */
#define PULL_WAIT_S 30        /* a server that has been sent SIGUSR2 and is still alive after this long is hung: a failure, unless it is
                               * dying (every thread in the kernel's exit: the teardown of a server with 47 threads and big mappings took more
                               * than 30 s on a machine with a load average of 90), which is given PULL_EXIT_WAIT_S */
#define PULL_EXIT_WAIT_S 300
static const char *nem_mutant;   /* --mutant-nemesis: nothing, once, burst:S, hole:FROM:TO (nothing between those seconds of the run), never, oncereact,
                                  * nap:MS (naps that do not look at the server), deadsleep:MS, busysleep:MS, eionosig (eio: never asks for the fault),
                                  * pausenocont (pause: never continues the server), killnoeffect (kill: signals nothing, restarts nothing),
                                  * pausenoeffect (pause: signals nothing), slow:MS (a nap longer by MS: works at a fraction of the honest rate),
                                  * lastpull (power: an honest nemesis that pulls its last plug after the run's time is up),
                                  * killslowstep:MS (kill: the step sleeps MS with the server live before it kills),
                                  * powerslowstep:MS, eioslowstep:MS (the same in the pull step of power and eio, the fault in progress),
                                  * blind:MS (naps that do not look at the server, as nap:MS, but no server is made to die in them: the shim's own
                                  * odds do that), and the ones that SPIN where the others sleep, so that no account of CPU waited for could tell them
                                  * from a machine that starves the thread: killspin:MS, powerspinstep:MS, eiospin:MS (the step spins MS with the server
                                  * live before its signal), pullspin:MS (power: spins MS after the plug, the server dying meanwhile, before it looks
                                  * for the death), waitspin:MS (the same, counted as the wait for the death), deadspin:MS (spins with the server dead
                                  * before the restart), norestart:K and norestartspin:K (power: no restart after K pulls, asleep or spinning, to the end
                                  * of the run): the check must fail all of these, whatever the load, with no excuse for a CPU wait; and the two the
                                  * documented lazy passes are measured with, stopafter:MS (asleep from MS into the run) and blindafter:S (blind naps
                                  * from S seconds on) */
static bool slow_create;            /* --mutant-slowcreate: waits 1 s before it creates the series (a server that crashed
                                     * by then must end the run at "cannot connect", not hang it: a self-test) */
static bool idle_mutant;            /* --mutant-idle: a nemesis that sleeps through its naps; the check must fail */
static bool idle_mutant_long;       /* --mutant-longnap: naps of 5 s without watching */
static int hang_problems;           /* power: a server that did not die within PULL_WAIT_S of SIGUSR2 */
static char markpath[PATH_MAX]; /* eio: crashfs writes the path of the file it failed here */
static char pullpath[PATH_MAX]; /* power: crashfs appends a line here for each power loss a SIGUSR2 asked for */
static char cutpath[PATH_MAX];  /* power: crashfs appends "<pid> <mono_ns>" per power loss */
static char slogpath[PATH_MAX]; /* the stderr of the server's latest start */
static int wal_sync_ms;         /* --wal-sync-ms N: passed to tsdbd; periodic WAL sync durability window */

typedef struct power_cut {
    pid_t pid;
    int64_t cut_ns;  /* monotonic time stamped by crashfs right before _exit(99) (or dead_ns if missing) */
    int64_t dead_ns; /* monotonic time recorded right after waitpid confirms exit 99 */
} power_cut;
static pthread_mutex_t cut_mu = PTHREAD_MUTEX_INITIALIZER;
static power_cut *reaped_cuts;
static size_t nreaped_cuts, cap_reaped_cuts;
static void record_power_death(pid_t pid, int64_t dead_ns);

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
/* What a process that will not die is doing: its state, and every thread's state and kernel wait (D: stuck in the kernel, S: waiting). */
static void print_proc_state(pid_t pid)
{
    char path[320], buf[1024];
    snprintf(path, sizeof path, "/proc/%d/status", (int)pid);
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "jepsen: pid %d has no /proc entry (errno %d)\n", (int)pid, errno);
        return;
    }
    while (fgets(buf, sizeof buf, f))
        if (!strncmp(buf, "State:", 6) || !strncmp(buf, "Threads:", 8) || !strncmp(buf, "SigPnd:", 7) || !strncmp(buf, "ShdPnd:", 7))
            fputs(buf, stderr);
    fclose(f);
    snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
    DIR *d = opendir(path);
    struct dirent *e;
    char seen[16][80]; /* the distinct (state, wait) pairs of the threads, with how many threads are in each */
    int nseen[16], nkinds = 0;
    while (d && (e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9')
            continue;
        char st[8] = "?", wc[64] = "";
        snprintf(path, sizeof path, "/proc/%d/task/%s/stat", (int)pid, e->d_name);
        if ((f = fopen(path, "r"))) {
            size_t n = fread(buf, 1, sizeof buf - 1, f);
            fclose(f);
            buf[n] = 0;
            const char *p = strrchr(buf, ')');
            if (p && p[1] == ' ')
                snprintf(st, sizeof st, "%c", p[2]);
        }
        snprintf(path, sizeof path, "/proc/%d/task/%s/wchan", (int)pid, e->d_name);
        if ((f = fopen(path, "r"))) {
            size_t n = fread(wc, 1, sizeof wc - 1, f);
            wc[n] = 0;
            fclose(f);
        }
        char kind[80];
        snprintf(kind, sizeof kind, "%s %s", st, wc);
        int k = 0;
        while (k < nkinds && strcmp(seen[k], kind))
            k++;
        if (k == nkinds && nkinds < 16) {
            snprintf(seen[nkinds], sizeof seen[0], "%s", kind);
            nseen[nkinds++] = 0;
        }
        if (k < nkinds)
            nseen[k]++;
    }
    for (int k = 0; k < nkinds; k++)
        fprintf(stderr, "jepsen:   %d threads: %s\n", nseen[k], seen[k]);
    if (d)
        closedir(d);
}
/* Whether process `pid` is in the middle of exiting (a thread waits in the kernel's do_exit, none is stopped): it is dying, however
 * slowly, and not hung. A hung one (SIGSTOP, a thread stuck in a file system call) is not. */
static bool proc_exiting(pid_t pid)
{
    char path[320], buf[64];
    snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
    DIR *d = opendir(path);
    struct dirent *e;
    bool exiting = false, stopped = false;
    while (d && (e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9')
            continue;
        snprintf(path, sizeof path, "/proc/%d/task/%s/wchan", (int)pid, e->d_name);
        FILE *f = fopen(path, "r");
        if (f) {
            size_t n = fread(buf, 1, sizeof buf - 1, f);
            buf[n] = 0;
            fclose(f);
            exiting |= strstr(buf, "do_exit") != NULL;
        }
        snprintf(path, sizeof path, "/proc/%d/task/%s/stat", (int)pid, e->d_name);
        if ((f = fopen(path, "r"))) {
            size_t n = fread(buf, 1, sizeof buf - 1, f);
            buf[n] = 0;
            fclose(f);
            const char *p = strrchr(buf, ')');
            stopped |= p && p[1] == ' ' && (p[2] == 'T' || p[2] == 't');
        }
    }
    if (d)
        closedir(d);
    return exiting && !stopped;
}
/* Whether a thread of process `pid` is stopped (SIGSTOP): a signal sent to it stays pending, the shim never sees it. */
static bool proc_stopped(pid_t pid)
{
    char path[320], buf[64];
    snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
    DIR *d = opendir(path);
    struct dirent *e;
    bool stopped = false;
    while (d && (e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9')
            continue;
        snprintf(path, sizeof path, "/proc/%d/task/%s/stat", (int)pid, e->d_name);
        FILE *f = fopen(path, "r");
        if (f) {
            size_t n = fread(buf, 1, sizeof buf - 1, f);
            buf[n] = 0;
            fclose(f);
            const char *p = strrchr(buf, ')');
            stopped |= p && p[1] == ' ' && (p[2] == 'T' || p[2] == 't');
        }
    }
    if (d)
        closedir(d);
    return stopped;
}
static int failstops, eio_problems;
static int lives_faulty, lives_clean, lives_died_starting; /* servers started under the nemesis, by kind */
static atomic_bool hung_flag;    /* a request got no answer within the deadline (req_done): the run is ending */
/* A server that has been forked and has not yet listened (start_server_run): if it ends, that function judges the end (a crash, damage,
 * or the environment: io_uring without memory, no port file in time), never the watcher, which would count a server that never came up
 * as a death and turn the harness error that follows (exit 2) into a verdict (exit 1; round 12, VERIFY11 (B)). */
static _Atomic bool server_starting;
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
/* --tier N: a directory tier of N stores (TIERS via `tsdb DIR tier add`, or legacy dirs= with JEPSEN_TIER_LEGACY=1).
 * R15 nemeses: store-eio, store-stuck, store-lost, salvage, fill, single-copy. */
static char dbdir[PATH_MAX];        /* DIR/db for TIERS mode (stores are siblings in DIR/tier), else DIR */
static int tier_n;
static bool tier_legacy;            /* JEPSEN_TIER_LEGACY=1: legacy TSDB_TEST_TIER dirs= path without TIERS */
static bool settle_steps;
static bool t_store_eio, t_store_stuck, t_store_lost, t_salvage, t_fill, t_single_copy, tier_nem;
static char tier_root[PATH_MAX];    /* DIR/tier, as an absolute path: the stores are tier_root/s<i> */
static char store_paths[16][PATH_MAX]; /* current canonical directory of each store slot 0..tier_n-1 */
static int store_gen[16];           /* replacement counter per store slot */
static bool store_replaced[16];     /* slot was replaced since the current server started (not in its OUTAGEFS_ROOTS) */
static int tier_nem_problems;       /* safety violations detected by R15 tier nemeses (exit 1) */
static int tier_liveness_issues;    /* timing/liveness misses in R15 tier nemeses (INCONCLUSIVE, exit 3) */
static char tier_liveness_msg[256];
static atomic_bool tier_harness_err; /* harness/setup/control failures in R15 tier nemeses (exit 2) */
static char tier_harness_msg[256];
static int tier_nem_rounds;         /* completed R15 tier fault rounds */
static atomic_llong fill_full_refusals; /* UNAVAILABLE WHY_FULL refusals observed by clients under `fill` */
static atomic_llong fill_writes_ok;     /* acknowledged writes under `fill` */
static atomic_llong fill_last_ok_ns;    /* invoke time of latest acknowledged write under `fill` */
static int salv_io_verified, salv_stats_verified, salv_parts_retired;
static int lost_rebuilds_verified;
#define MAX_SALV_EVENTS 32
typedef struct salv_event {
    int64_t lost_ns;              /* monotonic ns when 3 stores were marked LOST (settles paused from here) */
    int64_t salv_ns;              /* monotonic ns when `tsdb tier salvage` retired the lost .ec parts */
    int64_t done_ns;              /* monotonic ns when tsdbd restarted after replacing lost stores */
    int64_t parts[64];            /* partition keys whose .ec parts were retired by this salvage */
    int64_t lo[64];               /* inclusive timestamp start of each retired partition */
    int64_t hi[64];               /* exclusive timestamp end of each retired partition */
    int64_t settle_cutoff_ns[64]; /* monotonic ns cutoff from retired .ec settle_ms (never excuses younger writes) */
    int nparts;
} salv_event;
static salv_event salv_ev[MAX_SALV_EVENTS];
static int nsalv_ev;

static void tier_liveness(const char *fmt, ...)
{
    char buf[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    fprintf(stderr, "  [tier liveness] %s\n", buf);
    if (!tier_liveness_msg[0])
        snprintf(tier_liveness_msg, sizeof tier_liveness_msg, "%s", buf);
    tier_liveness_issues++;
}

static void tier_harness_fail(const char *fmt, ...)
{
    char buf[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    fprintf(stderr, "  [tier harness error] %s\n", buf);
    if (!tier_harness_msg[0])
        snprintf(tier_harness_msg, sizeof tier_harness_msg, "%s", buf);
    atomic_store(&tier_harness_err, true);
    atomic_store(&stop, true);
}

#ifdef TSDB_EC
static int64_t wall_ms_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}
#endif
static const char *const settle_step_names[] = {"after-put", "after-ec-tmp", "after-intent", "between-renames",
                                                "before-unlink"};
#define NSTEPS 5
#define SETTLE_WAIT_S 12            /* a server that has not reached its armed step by then is killed and restarted */
static int cur_step = -1;           /* the step the current server is armed with, -1: none */
static int64_t up_ns;               /* when the current server came up */
static int step_crashes[NSTEPS], step_unreached, step_armed[NSTEPS];
static int step_nocut;              /* servers that ended at their step with exit 99 but no power cut (a `_exit`) */
static int store_eios;              /* eio with the tier: faults that landed on a file of a store */
static char exitmark[PATH_MAX];     /* crashfs creates exitmark.<pid> when the process rolled back at its exit */
static bool verify;                 /* --verify-checksums: tsdbd checks granule payloads */
static int epoch;
static const char *md_files = "all"; /* CRASHFS_MD_FILES of this epoch */
static char faultlog[PATH_MAX];     /* every injected fault, one line each (the shim's and rot's) */
static int epoch_faults(void);
static bool log_names_damage(const char *path, long from, char *line, size_t n);
static long slog_from; /* where the latest server's own lines begin in slogpath (the epoch's log is appended to: an earlier server's lines are not this one's) */
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
static atomic_bool nem_stop, nem_done; /* nem_done: the nemesis thread has returned (main waits for it, bounded) */
/* The quiet tail (round 11): when the run's time is up the nemesis finishes the step it is in and goes quiet (nem_quiet), and the clients go on for QUIET_TAIL_S against the last server, which nothing faults any more. A server that hangs one of its
 * workers is cut short by the next kill or plug under a nemesis that restarts it every few hundred ms, and only a server that lives
 * long enough shows that a request is never answered: this one has to, whatever the nemesis (the request deadline does the rest). */
#define QUIET_TAIL_S 3
static atomic_bool nem_quiet;
static int outage_heals, outage_judged, outage_flap_judged, outage_slow_extended;
static double outage_worst_s, outage_worst_flap_s;
static char outage_keeppath[PATH_MAX + 64]; /* where a failed or close outage run's server log is kept */
static bool outage_near_miss; /* a heal took more than three quarters of its bound: the server log is kept */
static double healthy_ops, healthy_secs, healthy_base_ops; /* B's writes during A's outage, and what B did before */

/* A failed outage run keeps the server's side of it, under a name the next run does not unlink: every way the run can
 * end (the verdict, a server that does not come up, a final read that cannot connect) goes through here. */
static void outage_keep_log(void)
{
    if (outage && outage_keeppath[0] && rename(slogpath, outage_keeppath) == 0)
        fprintf(stderr, "  the server log of this run is kept in %s\n", outage_keeppath);
}

#define CL_STR_(x) #x
#define CL_STR(x) CL_STR_(x)

static int64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* The time a nemesis step spent on nothing the fault needs (a sleep, or a spin, with the server live before its signal or its kill: an
 * honest step does that in microseconds), from `t0` to now. It goes into nem_idle_ns, whose bound is NEM_IDLE_SHARE of the run. Nothing is
 * taken off for the time the thread waited for a CPU: a thread that spins on a machine that starves it waits as long as one that
 * is starved, and no account of the kernel's tells them apart. A machine that stalls the honest step fails the run as INCONCLUSIVE
 * (below), which the driver repeats with another seed. */
static void nem_idle_since(int64_t t0)
{
    nem_idle_ns += now_ns() - t0;
}

/* Two kinds of failure (the decision of 2026-11-14, test-trust round 9). A VIOLATION is a wrong answer of the system under test (lost,
 * resurrected or wrong data, a hung or crashed server, any check of the history): the run exits 1, and nobody repeats it. An INCONCLUSIVE
 * run is one whose own liveness or coverage check failed (the nemesis was too slow for its floor, a server lay dead too long, a gap with
 * no fault, a step that idled, the workload wrote or switched too little): the run tested less than it says, which says nothing about the
 * system under test. It exits 3 with the name of every check that failed and the numbers, and tests/jepsen/rerun.sh repeats it with the
 * next seed, twice at most; three in a row fail the gate with all three messages. A run that has both is a violation, and the run
 * always gets to say so: whatever the liveness checks found, main goes on to the final state and judges the whole history (every safety
 * check) before it picks the exit code: 1 if anything is violated, else 3 if a liveness or coverage check failed, else 0. */
static int inconclusive;
static char inc_checks[256];
static void inc_note(const char *check)
{
    inconclusive++;
    if (!strstr(inc_checks, check) && strlen(inc_checks) + strlen(check) + 3 < sizeof inc_checks)
        snprintf(inc_checks + strlen(inc_checks), sizeof inc_checks - strlen(inc_checks), "%s%s", inc_checks[0] ? ", " : "", check);
}
#define INCONCLUSIVE(check, ...)                           \
    do {                                                   \
        inc_note(check);                                   \
        fprintf(stderr, "  [inconclusive, check %s] ", check); \
        fprintf(stderr, __VA_ARGS__);                      \
    } while (0)

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
static int near_pct(int s) { return tier_n ? (s % 2 ? 40 : 75) : (s % 2 ? 70 : 97); } /* the tier: more into old keys */

/* ----------------------------------------------------------- the server */

static void stop_server(int sig);
/* No server runs: clients must not try the old port, which anything may take now. */
static int64_t life_start_ns; /* outage: when the server that runs now began (its memory of DOWNs begins then) */

/* The server is gone (reaped): no server until the next start_server. The dead time is the watcher's to measure. */
static void server_gone(void)
{
    life_start_ns = now_ns(); /* outage: the DOWNs the old server counted went with it */
    atomic_store(&server_pid, 0);
    atomic_store(&port, 0);
}

/* How the end of a server is judged (round 11; the user's scope of 2026-11-14: a server that dies other than by what the harness
 * did to it is a VIOLATION on the attempt where it died, under every nemesis and under none; the environment failing is not).
 * Every signal sent to a server is written down first (signal_pid), and every place that reaps one (reap_server) and the watcher,
 * which sees an end before the nemesis reaps it, ask end_report whether the end is one the harness caused or expects:
 *  - killed by a signal we sent to that pid, or exit 0 after our SIGTERM;
 *  - under the shim (power, eio): exit 99 (its power loss), and under eio exit 1 (the fail-stop on an EIO; the eio step
 *    judges whether the shim had failed a file);
 *  - misdirect and rot: judged by md_death.
 * Anything else is unplanned: a crash (SIGSEGV, SIGABRT...), an exit of its own, an end with no signal of ours. One end is
 * reported once (reported[]). A SIGKILL that we did not send is the one end that may be the machine's (the OOM killer) and
 * never tsdbd's own act: it is a harness error (exit 2), not a violation. */
#define END_TAB 1024
static pthread_mutex_t end_mu = PTHREAD_MUTEX_INITIALIZER;
static struct {
    pid_t pid;
    unsigned long long sigs; /* bit s: signal s was sent to pid */
} sent_tab[END_TAB];
static int sent_n;
static pid_t reported[END_TAB];
static int reported_n;
static int env_kills; /* a server killed by SIGKILL that the harness did not send */

static void note_sent(pid_t pid, int sig)
{
    if (pid <= 0 || sig <= 0 || sig >= 64)
        return;
    pthread_mutex_lock(&end_mu);
    int i = 0;
    while (i < sent_n && i < END_TAB && sent_tab[i].pid != pid)
        i++;
    if (i == sent_n || i >= END_TAB) {
        i = sent_n++ % END_TAB;
        sent_tab[i].pid = pid;
        sent_tab[i].sigs = 0;
    }
    sent_tab[i].sigs |= 1ULL << sig;
    pthread_mutex_unlock(&end_mu);
}
static bool sent_has(pid_t pid, int sig)
{
    bool r = false;
    if (sig <= 0 || sig >= 64)
        return false;
    pthread_mutex_lock(&end_mu);
    for (int i = 0; i < sent_n && i < END_TAB; i++)
        if (sent_tab[i].pid == pid)
            r = (sent_tab[i].sigs >> sig) & 1;
    pthread_mutex_unlock(&end_mu);
    return r;
}

static bool end_planned(pid_t pid, int st)
{
    if (md || rot)
        return true; /* md_death */
    bool shim_nem = !strcmp(nemesis, "power") || !strcmp(nemesis, "eio") || settle_steps; /* settle-steps: the hook's exit(99) after the shim's rollback */
    if (WIFSIGNALED(st)) /* a signal we sent, and no core: a SIGKILL cannot dump one, so a core is a crash that the kill met */
        return sent_has(pid, WTERMSIG(st)) && !WCOREDUMP(st);
    int c = WEXITSTATUS(st);
    if (c == 0 && sent_has(pid, SIGTERM))
        return true;
    if (server_faulty && shim_nem && (c == 99 || (c == 1 && !strcmp(nemesis, "eio"))))
        return true;
    return false;
}

static const char *end_text(int st, char *buf, size_t n)
{
    if (WIFSIGNALED(st))
        snprintf(buf, n, "signal %d (%s)%s", WTERMSIG(st), strsignal(WTERMSIG(st)), WCOREDUMP(st) ? ", core dumped" : "");
    else
        snprintf(buf, n, "exit status %d", WEXITSTATUS(st));
    return buf;
}

/* A server that fail-stops on ENOSPC while the file system of the test directory IS full (round 15, optional item 5): the machine's, not tsdb's: a
 * harness error (exit 2). Both must hold: the server's log (its latest start) ends with "No space left on device", and what is free on the
 * directory's file system is under 64 MB now (a quota, or the outage shim's injected ENOSPC, leaves room, and stays a verdict).
 * JEPSEN_FREE_BYTES overrides the free space (the selftest's hook). */
static bool fs_full_failstop(const char *logpath)
{
    long long avail;
    if (getenv("JEPSEN_FREE_BYTES"))
        avail = atoll(getenv("JEPSEN_FREE_BYTES"));
    else {
        struct statvfs sv;
        if (!dir || statvfs(dir, &sv) != 0)
            return false;
        avail = (long long)sv.f_bavail * (long long)sv.f_frsize;
    }
    if (avail >= (64LL << 20))
        return false;
    char b[8192];
    int fd = open(logpath, O_RDONLY);
    if (fd < 0)
        return false;
    off_t end = lseek(fd, 0, SEEK_END), at = end > (off_t)sizeof b - 1 ? end - ((off_t)sizeof b - 1) : 0;
    ssize_t n = pread(fd, b, sizeof b - 1, at);
    close(fd);
    if (n <= 0)
        return false;
    b[n] = 0;
    return strstr(b, "No space left on device") != NULL;
}

/* Judges the end of server `pid` (wait status `st`) and reports it, once, when the harness neither caused nor expected it. */
static bool end_report(pid_t pid, int st)
{
    if (pid <= 0 || end_planned(pid, st))
        return false;
    pthread_mutex_lock(&end_mu);
    for (int i = 0; i < reported_n && i < END_TAB; i++)
        if (reported[i] == pid) {
            pthread_mutex_unlock(&end_mu);
            return false;
        }
    reported[reported_n++ % END_TAB] = pid;
    pthread_mutex_unlock(&end_mu);
    char t[96];
    end_text(st, t, sizeof t);
    if (WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL) {
        env_kills++;
        fprintf(stderr, "jepsen: the %s server (pid %d) was killed by SIGKILL, which the harness did not send (the machine's "
                "out-of-memory killer?): the environment, not a verdict\n", server_faulty ? "faulty" : "clean", (int)pid);
        return true;
    }
    if (WIFEXITED(st) && WEXITSTATUS(st) == 1 && fs_full_failstop(slogpath)) {
        env_kills++;
        fprintf(stderr, "jepsen: the %s server (pid %d) exited with status 1 after \"No space left on device\", and the file system of %s has "
                "less than 64 MB free: it fail-stopped on a full disk, the environment, not a verdict\n", server_faulty ? "faulty" : "clean",
                (int)pid, dir);
        print_tail(slogpath);
        return true;
    }
    death_problems++;
    fprintf(stderr, "  the %s server (pid %d) ended with status %#x (%s) by itself: not a power loss (exit 99), not a signal or a "
            "stop of the harness, under the %s nemesis\n", server_faulty ? "faulty" : "clean", (int)pid, st, t, nemesis);
    print_tail(slogpath);
    return true;
}

/* The status of a server that has ended and not been reaped (it stays a zombie, so the status is still there), without reaping it. */
static bool end_status(pid_t pid, int *st)
{
    siginfo_t si;
    si.si_pid = 0;
    if (pid <= 0 || waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) != 0 || si.si_pid != pid)
        return false;
    *st = si.si_code == CLD_EXITED ? (si.si_status & 0xff) << 8 : (si.si_status & 0x7f) | (si.si_code == CLD_DUMPED ? 0x80 : 0);
    return true;
}

/* waitpid on the server, with its end judged: every reap of the server goes through here. */
static pid_t reap_server(pid_t pid, int *st, int opts)
{
    int s2 = 0;
    if (pid <= 0)
        return -1; /* waitpid(0) and waitpid(-1) would wait for any child */
    pid_t r = waitpid(pid, st ? st : &s2, opts);
    if (r == pid && pid > 0) {
        int s = st ? *st : s2;
        if (WIFEXITED(s) && WEXITSTATUS(s) == 99)
            record_power_death(pid, now_ns());
        end_report(pid, s);
    }
    return r;
}

/* A fault the nemesis issues, counted where it issues it. */
static void nem_fault_span(int64_t t, int64_t end)
{
    if (nem_faults < MAX_FAULT_TIMES) {
        nem_fault_ns[nem_faults] = t;
        nem_fault_end_ns[nem_faults] = end;
    }
    nem_faults++;
    atomic_store(&nem_act_ns, end > t ? end : t);
    if (getenv("JEPSEN_FAULT_TRACE")) /* when each fault was issued, seconds into the nemesis' run */
        fprintf(stderr, "jepsen: fault %d at %.3f s\n", nem_faults, (double)(t - nem_t0_ns) / 1e9);
}
static void nem_fault(void)
{
    int64_t t = now_ns();
    nem_fault_span(t, t);
}
/* The nemesis found a server dead by the shim's own odds and restarted it: its work, though not a fault of its own, so the
 * stretch it has been listening without one starts over (the floor counts only the faults it issued). A nemesis that has
 * stopped does not restart servers either: they stay dead, which has a bound of its own. */
static void nem_touch(void)
{
    atomic_store(&nem_act_ns, now_ns());
}
static const char *tsdbd_path(void) { return getenv("JEPSEN_TSDBD") ? getenv("JEPSEN_TSDBD") : "./build/tsdbd"; }

static void note_death(int st, bool we_killed, pid_t pid);

/* Whether the shim of server `pid` logged a power loss it was asked for with SIGUSR2: the only proof that a plug was pulled
 * (a server that was sent SIGUSR2 may die of the shim's own odds in the meantime, or the shim may ignore the signal). */
static bool pull_logged(pid_t pid)
{
    FILE *f = fopen(pullpath, "r");
    if (!f)
        return false;
    char line[128], want[48];
    snprintf(want, sizeof want, "pid %d power loss requested by SIGUSR2", (int)pid);
    bool ok = false;
    while (fgets(line, sizeof line, f))
        ok |= strncmp(line, want, strlen(want)) == 0;
    fclose(f);
    return ok;
}

/* The shim's fault rate per operation for the next server of the power and the eio nemesis (a power loss, an EIO). At 0.0005 a server on a fast file
 * system dies by itself within milliseconds, and the nemesis would find nothing to pull: so two servers in four run at
 * a rate low enough that they are alive when it comes to pull the plug (JEPSEN_PULL_PROB, 0.00003), and the others at the
 * high one (JEPSEN_CRASH_PROB, 0.0005: losses at random points, mostly early in a server's life, the recovery). */
static const char *crash_prob(void)
{
    const char *hi = getenv("JEPSEN_CRASH_PROB") ? getenv("JEPSEN_CRASH_PROB") : "0.0005";
    if ((strcmp(nemesis, "power") && strcmp(nemesis, "eio")) || (restarts / 2) % 2)
        return hi;
    return getenv("JEPSEN_PULL_PROB") ? getenv("JEPSEN_PULL_PROB") : "0.00003";
}

/* Whether a server's log (the stderr of its latest start) says that the open found damaged data: tsdb_open failed with
 * "corrupt data" or "I/O error" (tsdbd prints "tsdbd: DIR: <strerror>"), or the engine named a damaged file ("... is damaged ...",
 * "Damage is never silent"). Not an io_uring that would not start, not a port in use. The first such line goes to `line`. */
static bool log_says_damage(const char *path, long from, char *line, size_t n)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return false;
    if (from > 0 && fseek(f, from, SEEK_SET) != 0)
        rewind(f);
    char b[1024];
    bool hit = false;
    while (!hit && fgets(b, sizeof b, f)) {
        hit = strstr(b, ": corrupt data") || strstr(b, ": I/O error") || strstr(b, "is damaged") || strstr(b, " damaged: ") ||
              strstr(b, "(damaged") || strstr(b, "TSDB_ERR_CORRUPT");
        if (hit) {
            b[strcspn(b, "\n")] = 0;
            snprintf(line, n, "%.400s", b);
        }
    }
    fclose(f);
    return hit;
}

/* Whether the lines a server wrote (from `from` on) say that the machine, not the server, failed it: io_uring or a thread or a descriptor it
 * could not get, a port, a full disk, a limit. Such a start is a harness error (exit 2). */
static bool log_env_failure(const char *path, long from)
{
    static const char *const sig[] = {"io_uring", "Cannot allocate memory", "cannot start", "wake-up descriptor", "epoll:", "metrics thread",
                                      "cannot write", "Too many open files", "No space left", "Address already in use", "descriptor limit",
                                      "exit-with-parent", "Resource temporarily unavailable", NULL};
    FILE *f = fopen(path, "r");
    char b[1024];
    bool hit = false;
    if (f && from > 0 && fseek(f, from, SEEK_SET) != 0)
        rewind(f);
    while (f && !hit && fgets(b, sizeof b, f))
        for (int i = 0; sig[i] && !hit; i++)
            hit = strstr(b, sig[i]) != NULL;
    if (f)
        fclose(f);
    return hit;
}

/* The run ends here, where no server can be had (or one ended in a way that is a verdict): a crash or damage that was reported on
 * the way is a VIOLATION (exit 1), anything else a harness error (exit 2). The data directory goes, as at a normal end. */
static const char *rm_cmd; /* set by main: how the run's directories are removed */
__attribute__((noreturn)) static void bail(int code)
{
    if (code == 2 && death_problems > 0)
        code = 1;
    if (code == 1) {
        printf("FAILED: %d violations\n", death_problems > 0 ? death_problems : 1);
    }
    fflush(NULL);
    if (rm_cmd && !getenv("JEPSEN_KEEP") && system(rm_cmd) != 0) {
    }
    exit(code);
}

static void record_power_death(pid_t pid, int64_t dead_ns)
{
    if (pid <= 0)
        return;
    pthread_mutex_lock(&cut_mu);
    if (nreaped_cuts > 0 && reaped_cuts[nreaped_cuts - 1].pid == pid) {
        pthread_mutex_unlock(&cut_mu);
        return;
    }
    if (nreaped_cuts == cap_reaped_cuts) {
        size_t ncap = cap_reaped_cuts ? cap_reaped_cuts * 2 : 64;
        power_cut *na = realloc(reaped_cuts, ncap * sizeof *na);
        if (!na) {
            pthread_mutex_unlock(&cut_mu);
            fprintf(stderr, "jepsen: out of memory recording power cut\n");
            bail(2);
        }
        reaped_cuts = na;
        cap_reaped_cuts = ncap;
    }
    reaped_cuts[nreaped_cuts++] = (power_cut){.pid = pid, .cut_ns = dead_ns, .dead_ns = dead_ns};
    pthread_mutex_unlock(&cut_mu);
}

/* A server armed with a settle step ended with exit 99: that is a power cut only if crashfs rolled back in its
 * destructor, which runs on exit() and not on _exit(): the hook must leave through exit(99). Without the rollback the
 * step reached was a plain crash, and what it is meant to test (a cut inside the swap) never happened. */
static bool step_reached(int step, pid_t pid)
{
    char mp[PATH_MAX + 16];
    snprintf(mp, sizeof mp, "%s.%d", exitmark, (int)pid);
    if (access(mp, F_OK) == 0) {
        unlink(mp);
        step_crashes[step]++;
        return true;
    } else {
        step_nocut++;
        fprintf(stderr, "  step %s: the server ended with exit 99 and crashfs did not roll back (a _exit?)\n",
                settle_step_names[step]);
    }
    return false;
}

/* A server that does not come up (round 14; VERIFY13 finding 1). Two things are a verdict about tsdb, whatever the nemesis (kill, pause, power,
 * eio, outage, none, the login and feed variants, and the follower): a server that ENDS before it listens for a reason of its own (exit 0,
 * or a non-zero exit whose own lines, from this start's offset, name no failure of the machine: log_env_failure), and a server that is ALIVE
 * and wrote no port file in 30 s while the machine was not stalled (it hangs: pause(), a spin, a deadlock). Not a verdict: a server that
 * says what a machine out of memory says (io_uring: Cannot allocate memory), exit 127 (no binary), a server the machine killed, and a wait
 * that ends with the machine still stalling (a thread of the AWAITED process in D state, or this thread itself not scheduled, for a second:
 * liveness under load, a harness error, exit 2). A stall that ended before the wait ran out only lengthens it (await_watched): the server
 * that hangs after it is a verdict. A refusal on damage under misdirect and rot is judged before this, from the log. Every
 * message names the server's log and its last lines. */
static int64_t dstate_min_ns = 10000000000LL; /* a thread of the server in D state this long without a break is the kernel's stall (see req_done) */
#define AWAIT_SLICE_MS 250
static int open_wait_ms(void) /* how long a server may take to write its port file (JEPSEN_OPEN_WAIT_MS: the selftest's hung servers) */
{
    return getenv("JEPSEN_OPEN_WAIT_MS") && atoi(getenv("JEPSEN_OPEN_WAIT_MS")) > 0 ? atoi(getenv("JEPSEN_OPEN_WAIT_MS")) : 30000;
}
#define AWAIT_STARVED_MS 1000 /* a slice of the wait that took this much longer than it should have: the machine did not run this thread */
/* The sampler of D state for one process: the longest stretch, in ns, that a thread of `pid` has been in D state without a break, as of this
 * sample (struct dst_state keeps the record between samples: the global sampler's for the server that runs now, a wait's own for the process
 * it awaits). */
struct dst_state {
    pid_t cur;
    int nt;
    pid_t tids[256];
    int64_t since[256];
};
static int64_t dst_sample(struct dst_state *st, pid_t pid);
#define AWAIT_STALL_MIN_NS 1000000000LL /* a thread of the awaited server in D state this long, or this thread unscheduled this long, is a stall NOW */
#define AWAIT_CAP_FACTOR 4 /* a stall that goes on is waited out for this many times the wait, then it is the environment's (exit 2) */
/* Waits for the port file of `pid`. A wait does not end while the machine stalls, and the time it stalled does not count against the wait
 * (round 15, VERIFY14 findings 1 and 2): the stall is read from the process awaited (its own threads in D state for a second, or this thread
 * not scheduled for a second), at every slice. A server alive and not stalled when the wait runs out hangs: the caller's verdict. A stall still
 * there after AWAIT_CAP_FACTOR times the wait is the environment's: *stall_why says so (and only then; a stall that ended earlier leaves it
 * NULL, whatever it cost). */
static int await_watched(const char *pf, pid_t pid, int timeout_ms, const char **stall_why, char *whybuf, size_t wn)
{
    *stall_why = NULL;
    struct dst_state ds;
    memset(&ds, 0, sizeof ds);
    int64_t t0 = now_ns(), stalled_ns = 0, wait_ns = (int64_t)timeout_ms * 1000000LL;
    int p = TH_BAD;
    for (;;) {
        int64_t s0 = now_ns();
        p = th_await(pf, pid, AWAIT_SLICE_MS);
        int64_t s1 = now_ns(), el = (s1 - s0) / 1000000LL;
        if (p != TH_BAD || strstr(th_why, "names pid")) /* a port, an exit, or another process's port file: no more waiting helps */
            return p;
        int64_t d = dst_sample(&ds, pid);
        bool starved = el > AWAIT_SLICE_MS + AWAIT_STARVED_MS, in_d = d >= (dstate_min_ns < AWAIT_STALL_MIN_NS ? dstate_min_ns : AWAIT_STALL_MIN_NS);
        int64_t now = now_ns();
        if (starved || in_d)
            stalled_ns += now - s0; /* the machine did not run what this slice waits for */
        if (now - t0 - stalled_ns >= wait_ns || now - t0 >= wait_ns * AWAIT_CAP_FACTOR) {
            if (starved)
                snprintf(whybuf, wn, "this thread was not scheduled for %lld ms during the last slice of the wait", (long long)(el - AWAIT_SLICE_MS));
            else if (in_d)
                snprintf(whybuf, wn, "a thread of the server sat in D state (the kernel) for %.0f s, and still did at the end of the wait", (double)d / 1e9);
            if (starved || in_d)
                *stall_why = whybuf;
            /* the last slice's own reason is not the verdict's: what the wait saw is that no port file came */
            if (strstr(th_why, "wrote no"))
                snprintf(th_why, sizeof th_why, "harness: tsdbd (pid %d) wrote no %s in %lld s (%lld s of it stalled)\n", (int)pid, pf,
                         (long long)((now - t0) / 1000000000LL), (long long)(stalled_ns / 1000000000LL));
            return p;
        }
    }
}

/* The verdict on a server (the writer, or the follower) that did not listen. `p` is what the wait returned, `st` the wait status when it
 * ended. Returns when it is the environment (the caller prints the harness error); a verdict ends the run here (exit 1). */
static bool no_port_is_noted(pid_t pid, int p, int st, const char *who, const char *logpath, long from, const char *stall_why)
{
    if (p == TH_EXITED) {
        if (WIFSIGNALED(st) || (WIFEXITED(st) && WEXITSTATUS(st) == 127) || log_env_failure(logpath, from))
            return false; /* a signal (the crashes are judged by the caller), no binary, or a machine that failed the start */
        if (WIFEXITED(st) && WEXITSTATUS(st) == 0)
            fprintf(stderr, "  the %s (pid %d) exits with status 0 before it listens, nobody having asked it to: a server that stops by itself "
                    "(its log: %s)\n", who, (int)pid, logpath);
        else
            fprintf(stderr, "  the %s (pid %d) exits with status %d while opening, and its own log names neither damage nor a failure of the "
                    "environment: not a refusal on damage (its log: %s)\n", who, (int)pid, WIFEXITED(st) ? WEXITSTATUS(st) : -1, logpath);
    } else if (p == TH_BAD && strstr(th_why, "wrote no")) {
        if (stall_why)
            return false; /* the machine stalled during the wait: liveness under load, not a verdict (exit 2, the harness error) */
        fprintf(stderr, "  the %s (pid %d) is alive and wrote no port file in %d s, and the machine did not stall (no thread of it in D state "
                "at the end of the wait, this thread scheduled; the time it did stall was not counted): it hangs while opening (its log: %s)\n",
                who, (int)pid, open_wait_ms() / 1000, logpath);
        print_proc_state(pid);
    } else {
        return false; /* a port that is not the server's, another pid in the file: the environment */
    }
    death_problems++;
    print_tail(logpath);
    return true;
}

static void start_server_run(bool faulty)
{
    char pms[32], pf[PATH_MAX];
    if (feed) {
        atomic_store(&watch_excused, true); /* the checker's own work (tens of ms with thousands of segments) is not the nemesis lying idle */
        feed_check("before the server recovers it", true); /* the directory the last server left, torn or not */
        atomic_store(&watch_excused, false);
    }
    snprintf(pms, sizeof pms, "%lld", classes ? (long long)CL_PARTITION_MS : PARTITION_MS);
    atomic_store(&port, 0); /* until this server says where it listens, the clients have nowhere to go */
    for (int attempt = 0;; attempt++) {
        unlink(markpath); /* an EIO while opening: the clean server of the second attempt has no fault */
        unlink(pullpath);
        char arm[48] = "";
        cur_step = -1;
        if (settle_steps && faulty && crashfs && attempt == 0) { /* this server ends at a settle step, as a power cut */
            uint64_t q = mix(seed * 7919 + (uint64_t)restarts * 31 + 5);
            cur_step = (int)(q % NSTEPS);
            snprintf(arm, sizeof arm, "%s:%d", settle_step_names[cur_step], 1 + (int)((q >> 20) % 3));
            step_armed[cur_step]++;
        }
        th_portfile(pf, sizeof pf, dir);
        char follow[64];
        th_follow_me(follow, sizeof follow);
        { /* the log of an epoch is appended to (misdirect, rot, outage, keep): this server's lines begin where the file ends now */
            struct stat lsb;
            slog_from = (md || rot || outage || tier_n || getenv("JEPSEN_KEEP")) && !getenv("JEPSEN_VERBOSE") && stat(slogpath, &lsb) == 0 ? (long)lsb.st_size : 0;
        }
        pid_t pid = fork();
        if (pid == 0) {
            if (outage && faulty && outagefs_lib) { /* the shim: the nemesis takes a volume's directory away */
                char roots[2 * PATH_MAX + 2];
                snprintf(roots, sizeof roots, "%s:%s", vol_dir[0], vol_dir[1]);
                setenv("LD_PRELOAD", outagefs_lib, 1);
                setenv("OUTAGEFS_ROOTS", roots, 1);
                setenv("OUTAGEFS_CTL", octl, 1);
            } else if (tier_nem && faulty && outagefs_lib) { /* fast root + every store as its own outagefs root */
                char roots[18 * PATH_MAX];
                size_t at = (size_t)snprintf(roots, sizeof roots, "%s", dbdir);
                for (int i = 0; i < tier_n && at + strlen(store_paths[i]) + 2 < sizeof roots; i++)
                    at += (size_t)snprintf(roots + at, sizeof roots - at, ":%s", store_paths[i]);
                setenv("LD_PRELOAD", outagefs_lib, 1);
                setenv("OUTAGEFS_ROOTS", roots, 1);
                setenv("OUTAGEFS_CTL", octl, 1);
            }
            if (faulty && crashfs && attempt == 0) {
                char sd[32];
                snprintf(sd, sizeof sd, "%llu", (unsigned long long)mix(seed + (uint64_t)restarts));
                setenv("LD_PRELOAD", crashfs, 1);
                setenv("CRASHFS_ROOT", dir, 1);
                setenv("CRASHFS_PULL_FILE", pullpath, 1);
                setenv("CRASHFS_CUT_FILE", cutpath, 1);
                setenv("CRASHFS_SEED", sd, 1);
                setenv("CRASHFS_PROB", crash_prob(), 1);
                setenv("CRASHFS_WARMUP", "200", 1);
                if ((!strcmp(nemesis, "power") || settle_steps) && restarts % 2 == 1)
                    setenv("CRASHFS_TEAR", "1", 1); /* every other server: keep what a real disk may keep */
                if (arm[0]) { /* no random power loss: the settle step is the cut, and the shim rolls back at exit */
                    setenv("CRASHFS_PROB", "0", 1);
                    setenv("CRASHFS_AT_EXIT", "1", 1);
                    setenv("CRASHFS_EXIT_MARK", exitmark, 1);
                    setenv("TSDB_SETTLE_CRASH_AT", arm, 1);
                }
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
                    if (tier_n) { /* the WAL, any file, a file of a store (a store's failure stops no server) in turn */
                        if (restarts % 3 == 0)
                            setenv("CRASHFS_EIO_ONLY", "wal_", 1);
                        else if (restarts % 3 == 2)
                            setenv("CRASHFS_EIO_DIR", tier_root, 1);
                    } else if (restarts % 2 == 0) /* every other server: the WAL, where an error must stop it */
                        setenv("CRASHFS_EIO_ONLY", "wal_", 1);
                }
            }
            /* thresholds of seconds, not tens of them: an outage of a few seconds is one. fast=2000 (below down): a
             * write probe slower than fast_ms starts the 60 s of a flapping domain over, and on a loaded 4-CPU machine
             * probes of 400 to 1400 ms happen with no fault (the 10-seed runs, 6 of 20 failed at fast=400) */
            if (outage)
                setenv("TSDBD_TEST_HEALTH", "down=2500,read=200,write=500,recover=1500,fast=2000,spawn=60000", 1);
            if (tier_nem)
                setenv("TSDB_TEST_TIER_HEALTH", "stall_ms=1000,every_ms=200,ok_need=2,fast_ms=500", 1);
            if (!getenv("JEPSEN_VERBOSE")) { /* kept for a server that does not come up */
                bool keep = getenv("JEPSEN_KEEP") != NULL;
                int lfd = open(slogpath, O_WRONLY | O_CREAT | (md || rot || outage || keep || tier_n ? O_APPEND : O_TRUNC),
                               0644); /* the tier: tier_check counts the settle lines of every server of the run */
                if (lfd < 0)
                    lfd = open("/dev/null", O_WRONLY);
                else if (keep) { /* JEPSEN_KEEP: every server's log, each headed by when it started */
                    char h[128];
                    int hn = snprintf(h, sizeof h, "=== server %d at %.3f s, faulty %d, pid %d\n", restarts,
                                      (double)(now_ns() - run_start) / 1e9, faulty && crashfs && attempt == 0,
                                      (int)getpid());
                    if (write(lfd, h, (size_t)hn) < 0) {
                    }
                }
                if (lfd >= 0)
                    dup2(lfd, 2);
            }
            const char *av[48] = {"tsdbd", "--dir", dbdir, "--port", "0", "--port-file", pf, "--workers",
                                  t_store_stuck ? "20" : "8", "--wal-max-batch-rows", WAL_MAX_BATCH_ROWS_STR,
                                  "--partition-ms", pms};
            int ac = 13;
            av[ac++] = follow; /* a killed run leaves no server behind */
            if (!auth)
                av[ac++] = "--no-auth";
            char wsms[32];
            if (wal_sync_ms > 0) {
                snprintf(wsms, sizeof wsms, "%d", wal_sync_ms);
                av[ac++] = "--wal-sync-ms", av[ac++] = wsms;
            }
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
        atomic_store(&server_starting, true); /* before the pid: the watcher leaves the end of a server that is not up to this function */
        atomic_store(&server_pid, pid);
        server_faulty = attempt == 0 && faulty && crashfs;
        if (faulty && crashfs && !server_faulty)
            lives_clean++;
        if (getenv("JEPSEN_VERBOSE"))
            fprintf(stderr, "jepsen: started %d (%s)\n", (int)pid, attempt == 0 && faulty ? "faulty" : "clean");
        if (getenv("JEPSEN_START_AWAIT_DELAY_MS")) /* a test hook of the selftest: the watcher polls first, as it may on a machine that starves this thread */
            usleep((useconds_t)atoi(getenv("JEPSEN_START_AWAIT_DELAY_MS")) * 1000);
        const char *stall_why;
        char stall_buf[160];
        int p = await_watched(pf, pid, open_wait_ms(), &stall_why, stall_buf, sizeof stall_buf);
        unlink(pf);
        atomic_store(&server_starting, false);
        if (p > 0) {
            memset(store_replaced, 0, sizeof store_replaced);
            atomic_store(&port, p);
            up_ns = now_ns();
            if (server_faulty)
                lives_faulty++;
            return;
        }
        char dmg[512];
        if (p == TH_EXITED && (md || rot) &&
            (WIFSIGNALED(th_status) || (WIFEXITED(th_status) && WEXITSTATUS(th_status) == 1 && log_names_damage(slogpath, slog_from, dmg, sizeof dmg)))) {
            /* refused: the open found damage (exit 1, and its log names it), or the faulty server stopped on damage it read; a signal
             * is a crash, never an answer to damage. Any other end before it listened (exit 0, an exit that names no damage) is the
             * environment or a damage-free fault, judged below (round 12). */
            server_gone();
            if (WIFEXITED(th_status) && epoch_faults() == 0) { /* damage in a database that no fault touched */
                death_problems++;
                fprintf(stderr, "  the %s server (pid %d) refuses to open the database, and no fault was injected in this epoch: %s\n",
                        server_faulty ? "faulty" : "clean", (int)pid, dmg);
                print_tail(slogpath);
            }
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
            /* A server that ends before it listens (round 11): the end the shim plans for a faulty server (its power loss, exit 99; under
             * eio the fail-stop on the file it failed, exit 1, with its mark) is the fault, and a clean server follows. A crash is a
             * violation. An open that failed on damage (the log says CORRUPT, I/O error, damaged: the data a fault left is damaged) is
             * a violation too. Anything else that the machine did (io_uring: Cannot allocate memory, exit 127, a server killed by it) is the
             * environment: a harness error, exit 2; a server that ends for a reason of its own (exit 0, or exit with no such line) is a verdict
             * (no_port_is_noted, round 14), under every nemesis. */
            int st = th_status;
            if (WIFEXITED(st) && WEXITSTATUS(st) == 99)
                record_power_death(pid, now_ns());
            server_gone();
            bool shim0 = faulty && crashfs && attempt == 0;
            bool planned = shim0 && WIFEXITED(st) &&
                           (WEXITSTATUS(st) == 99 || (WEXITSTATUS(st) == 1 && !strcmp(nemesis, "eio") && access(markpath, F_OK) == 0));
            if (planned) { /* crashed while starting (power loss at open, an EIO at open): a clean one */
                lives_died_starting++;
                if (!strcmp(nemesis, "power") || settle_steps)
                    note_death(th_status, false, pid);
                if (settle_steps && cur_step >= 0 && WIFEXITED(th_status) && WEXITSTATUS(th_status) == 99 &&
                    step_reached(cur_step, pid))
                    nem_fault(); /* the settle at open (a recovery of one) reached its step */
                if (getenv("JEPSEN_VERBOSE"))
                    fprintf(stderr, "jepsen: %d died while starting\n", (int)pid);
                continue;
            }
            if (WIFSIGNALED(st) && WTERMSIG(st) != SIGKILL && WTERMSIG(st) != SIGTERM && WTERMSIG(st) != SIGUSR2) {
                end_report(pid, st); /* a crash while opening */
                print_tail(slogpath);
                if (shim0) {
                    lives_died_starting++;
                    continue;
                }
                bail(1);
            }
            char dl[512];
            if (WIFEXITED(st) && log_says_damage(slogpath, slog_from, dl, sizeof dl)) {
                fprintf(stderr, "  the server (pid %d, exit status %d) refuses to open the database after the faults: damaged data: %s\n",
                        (int)pid, WEXITSTATUS(st), dl);
                death_problems++;
                print_tail(slogpath);
                stop_server(SIGKILL);
                bail(1);
            }
        }
        /* round 14 (VERIFY13 finding 1): under every nemesis, a server that ends at open for a reason of its own, or hangs there with the machine
         * not stalled, is a verdict. Only what the machine did is a harness error. */
        if (no_port_is_noted(pid, p, th_status, "server", slogpath, slog_from, stall_why)) {
            stop_server(SIGKILL);
            outage_keep_log();
            bail(1);
        }
        fprintf(stderr, "jepsen: the server does not come up (the environment, not a verdict about tsdb)%s%s: %s", stall_why ? ", and ": "",
                stall_why ? stall_why : "", th_why);
        print_tail(slogpath);
        stop_server(SIGKILL);
        outage_keep_log();
        bail(2);
    }
}

static void start_server(bool faulty) { start_server_run(faulty); }

/* A server that is crashing when the harness kills it (round 12; VERIFY11 (A), the blocker). A process that dies of SIGABRT or
 * SIGSEGV is not a zombie while the kernel writes its core (systemd-coredump takes 0.1 to several seconds, longer under load; a
 * core file limit of 0 does not stop a pipe handler) or while it tears its address space down: waitpid finds nothing, the nemesis'
 * SIGKILL goes in, and the end becomes "killed by SIGKILL", which the harness sent: a crash hidden as the harness' own kill, passed
 * VALID. So before every SIGKILL of a process of the harness its /proc state is read: CoreDumping: 1, or a pending fatal signal
 * (SIGQUIT, SIGILL, SIGTRAP, SIGABRT, SIGBUS, SIGFPE, SIGSEGV, SIGXCPU, SIGXFSZ, SIGSYS) that the harness did not send, in any
 * thread or the process, is a crash: a violation at once, naming the signal when the end is still visible (a dump is given up to
 * CRASH_DUMP_WAIT_S to finish, which shows the signal in the wait status; the kill follows either way). What remains is the window
 * of microseconds between the kernel taking the fatal signal off the queue and setting CoreDumping: a crash in that window, at the
 * very moment of a kill, is not seen. */
#define CRASH_DUMP_WAIT_S 10
static bool fatal_pending_unsent(pid_t pid, int *sigp)
{
    const unsigned long long fatal = (1ULL << (SIGQUIT - 1)) | (1ULL << (SIGILL - 1)) | (1ULL << (SIGTRAP - 1)) |
                                     (1ULL << (SIGABRT - 1)) | (1ULL << (SIGBUS - 1)) | (1ULL << (SIGFPE - 1)) |
                                     (1ULL << (SIGSEGV - 1)) | (1ULL << (SIGXCPU - 1)) | (1ULL << (SIGXFSZ - 1)) |
                                     (1ULL << (SIGSYS - 1));
    char path[320], line[256];
    unsigned long long pend = 0, handled = 0; /* handled: caught or ignored (the shim's SIGBUS handler, for one): a pending one is no crash */
    bool dumping = false;
    snprintf(path, sizeof path, "/proc/%d/status", (int)pid);
    FILE *f = fopen(path, "r");
    if (!f)
        return false;
    while (fgets(line, sizeof line, f)) {
        unsigned long long v;
        if (!strncmp(line, "CoreDumping:", 12) && atoi(line + 12) == 1)
            dumping = true;
        else if ((!strncmp(line, "ShdPnd:", 7) || !strncmp(line, "SigPnd:", 7)) && sscanf(line + 7, "%llx", &v) == 1)
            pend |= v;
        else if ((!strncmp(line, "SigCgt:", 7) || !strncmp(line, "SigIgn:", 7)) && sscanf(line + 7, "%llx", &v) == 1)
            handled |= v;
    }
    fclose(f);
    snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
    DIR *d = opendir(path);
    struct dirent *e;
    while (d && (e = readdir(d))) { /* SigPnd of the other threads: the signal of abort() is queued to the thread that raised it */
        if (e->d_name[0] < '0' || e->d_name[0] > '9')
            continue;
        snprintf(path, sizeof path, "/proc/%d/task/%s/status", (int)pid, e->d_name);
        if (!(f = fopen(path, "r")))
            continue;
        while (fgets(line, sizeof line, f)) {
            unsigned long long v;
            if (!strncmp(line, "CoreDumping:", 12) && atoi(line + 12) == 1)
                dumping = true;
            else if (!strncmp(line, "SigPnd:", 7) && sscanf(line + 7, "%llx", &v) == 1)
                pend |= v;
        }
        fclose(f);
    }
    if (d)
        closedir(d);
    int s = 0;
    for (int n = 1; n < 64; n++)
        if (((pend & fatal & ~handled) >> (n - 1) & 1) && !sent_has(pid, n)) {
            s = n;
            break;
        }
    *sigp = s;
    return dumping || s;
}

static void crash_check(pid_t pid, int sig)
{
    int s = 0;
    if (sig != SIGKILL || pid <= 0 || !fatal_pending_unsent(pid, &s))
        return;
    pthread_mutex_lock(&end_mu);
    for (int i = 0; i < reported_n && i < END_TAB; i++)
        if (reported[i] == pid) {
            pthread_mutex_unlock(&end_mu);
            return;
        }
    reported[reported_n++ % END_TAB] = pid;
    pthread_mutex_unlock(&end_mu);
    int st = 0;
    bool seen = false;
    for (int64_t until = now_ns() + (int64_t)CRASH_DUMP_WAIT_S * 1000000000LL; now_ns() < until && !(seen = end_status(pid, &st));)
        usleep(5000); /* the dump finishing shows the signal the server died of */
    death_problems++;
    char t[96];
    if (seen && WIFSIGNALED(st))
        end_text(st, t, sizeof t);
    else if (s)
        snprintf(t, sizeof t, "signal %d (%s) on its way", s, strsignal(s));
    else
        snprintf(t, sizeof t, "writing a core dump; the signal is not visible any more");
    fprintf(stderr, "  the %s server (pid %d) was crashing when the harness killed it: it ended with %s (%s) by itself: not a "
            "power loss (exit 99), not a signal or a stop of the harness, under the %s nemesis\n",
            server_faulty ? "faulty" : "clean", (int)pid, seen ? "a status of its own" : "a core dump in progress", t, nemesis);
    print_tail(slogpath);
}

/* kill() with pid 0 or -1 signals the process group, or every process of the
 * user: with no server running, a nemesis must do nothing. */
static int signal_pid(pid_t pid, int sig)
{
    if (pid <= 0)
        return -1;
    crash_check(pid, sig); /* before note_sent: a crash is not ended by a kill that hides it */
    note_sent(pid, sig); /* before the signal: an end that the watcher sees first is already ours */
    return kill(pid, sig);
}

/* Stops the server with `sig` and reaps it; true when it was reaped, its wait status in *st (a kill is confirmed by the
 * server having died of that signal, never by the call having been made). */
#define STOP_TERM_S 120 /* the longest an honest server takes to stop cleanly after SIGTERM (a flush, the joins): far above what a loaded machine needs */
#define STOP_KILL_S 60  /* after SIGKILL: a process that is still there then is in the kernel's hands (D state), not ours */
/* Signals `pid` (the server or a server of the harness) with `sig` and reaps it, within a bound: true when it was reaped, its wait status in *st.
 * A server that does not stop within STOP_TERM_S of a SIGTERM (or any signal but SIGKILL) hangs: a VIOLATION, and it is killed. */
static bool stop_pid(pid_t pid, int sig, int *st)
{
    bool reaped = false;
    *st = 0;
    if (sig == SIGTERM && atomic_load(&hung_flag))
        sig = SIGKILL; /* a server that stopped answering has no clean stop to wait for */
    signal_pid(pid, sig);
    /* never an unbounded wait (round 11): a server whose clean stop waits for a hung worker never ends, and the harness that waits
     * for it never ends either. Past the bound it is a VIOLATION (the server hangs), and it is killed by its pid. */
    int64_t until = now_ns() + (sig == SIGKILL ? STOP_KILL_S : STOP_TERM_S) * 1000000000LL;
    for (int i = 0; !(reaped = reap_server(pid, st, WNOHANG) == pid) && now_ns() < until; i++) {
        usleep(i < 200 ? 250 : 5000);
        if (sig != SIGKILL && atomic_load(&hung_flag)) { /* a request met its deadline meanwhile (a hung worker): that is the verdict, no 120 s to wait for */
            sig = SIGKILL;
            signal_pid(pid, SIGKILL);
            until = now_ns() + STOP_KILL_S * 1000000000LL;
        }
    }
    if (!reaped && sig != SIGKILL) {
        death_problems++;
        fprintf(stderr, "  the server (pid %d) did not stop within %d s of signal %d: it hangs (its clean stop waits for something "
                "that never happens)\n", (int)pid, STOP_TERM_S, sig);
        print_proc_state(pid);
        print_tail(slogpath);
        signal_pid(pid, SIGKILL);
        until = now_ns() + STOP_KILL_S * 1000000000LL;
        for (int i = 0; !(reaped = reap_server(pid, st, WNOHANG) == pid) && now_ns() < until; i++)
            usleep(i < 200 ? 250 : 5000);
    }
    if (!reaped) {
        fprintf(stderr, "jepsen: the server (pid %d) is still there %d s after SIGKILL\n", (int)pid, STOP_KILL_S);
        bail(2);
    }
    return reaped;
}
static bool stop_server_st(int sig, int *st)
{
    pid_t pid = atomic_load(&server_pid);
    bool reaped = false;
    *st = 0;
    if (getenv("JEPSEN_VERBOSE"))
        fprintf(stderr, "jepsen: stopping %d with signal %d\n", (int)pid, sig);
    if (pid > 0) {
        reaped = stop_pid(pid, sig, st);
        server_gone();
    }
    atomic_store(&port, 0);
    return reaped;
}
static void stop_server(int sig)
{
    int st;
    stop_server_st(sig, &st);
}

/* A mutant that burns CPU where an honest nemesis sleeps or waits (--mutant-nemesis *spin*): on a machine that starves its thread, a spinner
 * has as much run delay as a thread that waits for a CPU, so the check takes no account of the delay (nem_idle_since). */
static void spin_ms(int ms)
{
    int64_t end = now_ns() + (int64_t)ms * 1000000LL;
    while (now_ns() < end)
        ;
}

/* The kill nemesis' fault: SIGKILL to the live server, which is a fault only once that server has died of it and a new one
 * listens (a different pid; a kill that reaches nothing, or a server that was dead already, is none). `sig` 0 is the mutant
 * whose kill has no effect (--mutant-nemesis killnoeffect: nothing is signalled, nothing restarted). */
static bool kill_and_restart(int sig, bool faulty)
{
    pid_t old = atomic_load(&server_pid);
    if (old <= 0) {
        start_server(faulty);
        return false;
    }
    int64_t issued = now_ns();
    int st = 0;
    /* a server that ended by itself before the kill (round 11) is reported by the reap, whatever the nemesis meant to do to it: a kill
     * that finds nothing alive is not a fault of the nemesis either (confirmed is false: the old server was not killed by it) */
    if (sig && reap_server(old, &st, WNOHANG) == old) {
        server_gone();
        start_server(faulty);
        nem_touch();
        return false;
    }
    atomic_fetch_add(&nem_inprog, 1);
    if (nem_mutant && !strncmp(nem_mutant, "killslowstep:", 13)) /* --mutant-nemesis killslowstep:MS: the step dawdles with the server live */
        usleep((useconds_t)atoi(nem_mutant + 13) * 1000);
    if (nem_mutant && !strncmp(nem_mutant, "killspin:", 9)) /* the same, spinning */
        spin_ms(atoi(nem_mutant + 9));
    nem_idle_since(issued); /* the time before the signal is the step's idle time, as the power step's */
    bool died = false;
    if (sig) {
        died = stop_server_st(sig, &st) && WIFSIGNALED(st) && WTERMSIG(st) == sig;
        start_server(faulty);
    }
    atomic_fetch_sub(&nem_inprog, 1);
    bool confirmed = died && atomic_load(&server_pid) > 0 && atomic_load(&server_pid) != old && atomic_load(&port) > 0;
    if (confirmed)
        nem_fault_span(issued, now_ns());
    return confirmed;
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
        const char *av[16] = {"tsdbd", "--dir", dbdir, "--port", "0", "--port-file", pf, "--workers", "2", "--follow"};
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
    const char *stall_why;
    char stall_buf[160];
    int p = await_watched(pf, pid, open_wait_ms(), &stall_why, stall_buf, sizeof stall_buf);
    unlink(pf);
    if (p > 0) {
        fport = p;
        return;
    }
    /* round 14 (VERIFY13 finding 1): the follower is a tsdbd too: one that ends at open for a reason of its own, or hangs there with the machine
     * not stalled, is a verdict; the log is its own (opened with O_TRUNC) */
    int fst = th_status;
    bool verdict = no_port_is_noted(pid, p, fst, "follower", flogpath, 0, stall_why);
    if (!verdict)
        fprintf(stderr, "jepsen: the follower does not come up (see %s)%s%s: %s", flogpath, stall_why ? ", and " : "", stall_why ? stall_why : "",
                th_why);
    if (p != TH_EXITED) {
        signal_pid(pid, SIGKILL);
        waitpid(pid, NULL, 0);
    }
    atomic_store(&follower_pid, 0);
    stop_server(SIGKILL);
    if (verdict)
        bail(1);
    exit(2);
}

/* Reads from the follower; any outcome but a failure is fine here, check_follower() judges the values. */
static void kill_follower(void) /* at exit(2): no orphan */
{
    pid_t pid = atomic_load(&follower_pid);
    if (pid > 0 && signal_pid(pid, SIGKILL) == 0)
        waitpid(pid, NULL, 0);
}

/* Every request of the harness has a deadline (round 11): a server that stops answering (a worker blocked for good, a deadlock) used
 * to hang the harness for ever, in recvfrom of a client that had no timeout, and the server of a harness that was then killed stayed
 * up. A request that gets no answer within req_deadline_s (the socket's receive timeout; JEPSEN_REQ_DEADLINE_S sets it for the
 * self-test) is a VIOLATION, "server stopped answering", naming the operation, and the run ends: the clients and the nemesis stop, the
 * server is killed by its pid and the final state is judged as for any run. The default is above the longest planned stop of a server
 * (a pause lasts under 0.4 s, an outage hang 8 s, a power loss or a kill ends the connection at once) and above five times the
 * slowest honest request measured under the heaviest load (the summary line says what this run's was: req_worst_ns). */
#define REQ_DEADLINE_DEFAULT_S 60
static int req_deadline_s = REQ_DEADLINE_DEFAULT_S;
static _Atomic int64_t req_worst_ns;  /* the slowest request that was answered (or failed) in this run */
static int stall_problems;            /* reported (once per run) */

/* A deadline miss while the kernel holds the server (round 12; VERIFY11 (B): at a load average of 599 the server's threads sat in D state on
 * the file system's journal, jbd2_log_wait_commit and folio_wait_bit_common, for over a minute, and the harness called it "server stopped
 * answering", a VIOLATION: liveness under load, not safety). The tasks of the server are sampled (dstate_thread, 4 times a second): a thread
 * that has been in D state (uninterruptible sleep) for dstate_min_ns without a break is the kernel's, not tsdbd's. A request that missed its
 * deadline while such a stretch is under way at that moment (a fresh look at the miss; a stretch that ended before it changes nothing: round 13,
 * VERIFY12) is a kernel I/O stall: no verdict, the request is an error like a refused one, the run goes on (the
 * checker judges the history, which has the request as indeterminate) and ends INCONCLUSIVE ("kernel-stall"); a stall that lasts
 * KSTALL_BOUND_S (JEPSEN_KSTALL_BOUND_S) from the first miss ends the run, also INCONCLUSIVE, with the server killed. A miss with no such
 * thread (a futex wait, a pause, a spin, a deadlock: tsdbd's own) stays a VIOLATION. Short D states are not enough: a server that hangs a
 * worker still fsyncs for the others, and its threads are in D for a few milliseconds at a time.
 * The limit: a D state that is tsdbd's own, a thread stuck in the kernel for a reason of its own making (a file system that never answers
 * the harness' volume), looks the same from outside; it is reported INCONCLUSIVE with the wait channels (print_proc_state) for a person to read. */
#define KSTALL_BOUND_S_DEFAULT 600
static _Atomic int64_t d_long_ns;      /* the last time a thread of the server was seen in D state for dstate_min_ns or more at a stretch */
static atomic_bool kstall_seen, kstall_over;
static int64_t kstall_first_ns;
static int kstall_misses, kstall_bound_s = KSTALL_BOUND_S_DEFAULT;
static atomic_bool dst_stop;

static int64_t dstate_scan(void);

static void req_done(const char *op, int rc, int64_t t0)
{
    int64_t d = now_ns() - t0, w = atomic_load(&req_worst_ns);
    if (rc != TSDB_ERR_IO && rc != TSDB_ERR_UNAVAILABLE) /* an answer: the honest worst the deadline is calibrated on */
        while (d > w && !atomic_compare_exchange_weak(&req_worst_ns, &w, d))
            ;
    /* only a D stretch still under way at this moment makes the miss the kernel's (VERIFY12: a stretch that ended before the miss, a
     * worker blocked in D for 40 s from the first second and a hung one that missed at 60 s, passed as a stall): a fresh scan decides */
    if (rc == TSDB_ERR_IO && d >= (int64_t)req_deadline_s * 950000000LL && dstate_scan() >= dstate_min_ns) {
        pid_t pid = atomic_load(&server_pid);
        if (!atomic_exchange(&kstall_seen, true)) {
            kstall_first_ns = now_ns();
            fprintf(stderr, "  [kernel I/O stall] %s got no answer in %.0f s (the deadline is %d s) while a thread of the server (pid %d) was in D state "
                    "for %.0f s or more: the kernel's, not the server's; no verdict, the run goes on (at most %d s of it)\n", op, (double)d / 1e9,
                    req_deadline_s, (int)pid, (double)dstate_min_ns / 1e9, kstall_bound_s);
            if (pid > 0)
                print_proc_state(pid);
        }
        kstall_misses++;
        return;
    }
    if (rc == TSDB_ERR_IO && d >= (int64_t)req_deadline_s * 950000000LL && !atomic_exchange(&hung_flag, true)) {
        pid_t pid = atomic_load(&server_pid);
        stall_problems++;
        fprintf(stderr, "  server stopped answering: %s got no answer in %.0f s (the deadline is %d s; server pid %d is %s)\n", op,
                (double)d / 1e9, req_deadline_s, (int)pid, pid > 0 && proc_stopped(pid) ? "stopped (SIGSTOP)" : "not answering");
        if (pid > 0)
            print_proc_state(pid);
        print_tail(slogpath);
        atomic_store(&stop, true); /* the clients and the nemesis end; main kills the server and judges what there is */
    }
}

/* The sampler of D state: four times a second, for the server that runs now (and once more at a deadline miss: dstate_scan is the one
 * place that reads the tasks, under dst_mu, so the thread's record of since-when and a miss's look at the same instant agree). It returns
 * the longest stretch, in ns, that a thread of the server has been in D state without a break, as of this scan. */
static pthread_mutex_t dst_mu = PTHREAD_MUTEX_INITIALIZER;
static int64_t dst_sample(struct dst_state *st, pid_t pid)
{
    enum { MAXT = 256 };
    int64_t now = now_ns(), longest = 0;
    if (pid != st->cur) {
        st->cur = pid;
        st->nt = 0;
    }
    if (pid > 0) {
        char path[320], b[512];
        snprintf(path, sizeof path, "/proc/%d/task", (int)pid);
        DIR *dd = opendir(path);
        struct dirent *e;
        pid_t seen[MAXT];
        int ns = 0;
        while (dd && (e = readdir(dd))) {
            if (e->d_name[0] < '0' || e->d_name[0] > '9')
                continue;
            snprintf(path, sizeof path, "/proc/%d/task/%s/stat", (int)pid, e->d_name);
            FILE *f = fopen(path, "r");
            if (!f)
                continue;
            size_t n = fread(b, 1, sizeof b - 1, f);
            fclose(f);
            b[n] = 0;
            const char *c = strrchr(b, ')');
            if (c && c[1] == ' ' && c[2] == 'D' && ns < MAXT)
                seen[ns++] = (pid_t)atoi(e->d_name);
        }
        if (dd)
            closedir(dd);
        /* the new record is built beside the old one: a thread's entry is found in the old list whatever order the others are in (VERIFY12:
         * compacting in place overwrote entries not yet searched, and a stretch that went on restarted whenever another thread entered D) */
        pid_t ntids[MAXT];
        int64_t nsince[MAXT];
        for (int i = 0; i < ns; i++) { /* a thread in D state now: since when */
            int j = 0;
            while (j < st->nt && st->tids[j] != seen[i])
                j++;
            int64_t sc = j < st->nt ? st->since[j] : now;
            if (sc < now && now - sc > longest)
                longest = now - sc;
            ntids[i] = seen[i];
            nsince[i] = sc;
        }
        memcpy(st->tids, ntids, (size_t)ns * sizeof st->tids[0]);
        memcpy(st->since, nsince, (size_t)ns * sizeof st->since[0]);
        st->nt = ns;
    } else {
        st->nt = 0;
    }
    return longest;
}

/* the server that runs now (server_pid); the one record of it, under dst_mu */
static int64_t dstate_scan(void)
{
    static struct dst_state gst;
    pthread_mutex_lock(&dst_mu);
    int64_t longest = dst_sample(&gst, atomic_load(&server_pid)), now = now_ns();
    if (longest >= dstate_min_ns)
        atomic_store(&d_long_ns, now);
    pthread_mutex_unlock(&dst_mu);
    return longest;
}

static void *dstate_thread(void *arg)
{
    (void)arg;
    while (!atomic_load(&dst_stop)) {
        usleep(250000);
        int64_t now = now_ns();
        dstate_scan();
        if (atomic_load(&kstall_seen) && !atomic_load(&kstall_over) && now - kstall_first_ns >= (int64_t)kstall_bound_s * 1000000000LL &&
            now - atomic_load(&d_long_ns) < 2000000000LL) { /* the stall has lasted its bound and still does: the run ends, INCONCLUSIVE */
            atomic_store(&kstall_over, true);
            fprintf(stderr, "  [kernel I/O stall] still there %d s after the first request it held up: the run ends here\n", kstall_bound_s);
            atomic_store(&hung_flag, true);
            atomic_store(&stop, true);
        }
    }
    return NULL;
}
#define REQ(op, expr) ({ int64_t t0_ = now_ns(); int r_ = (expr); req_done(op, r_, t0_); r_; })

/* Connects to the server this harness started, and only to it: the port may have been given to another process
 * since that server died (the nemesis notices a crash only at its next tick), so after connecting the socket
 * has to belong to `pid`. A dead server holds no socket, which makes this fail until the next one is up. */
static int connect_own_ms(int p, pid_t pid, tsdbc **c, int hello_ms)
{
    /* connect_ms bounds the TCP connect and the HELLO (tsdbc_connect_opts): 0 is 10 s, and a server whose workers are all hung answers no HELLO */
    tsdbc_options o = {.user = auth ? KV_USER : NULL, .password_file = auth ? kv_pw : NULL, .connect_ms = hello_ms, .require_auth = auth};
    if (p <= 0 || pid <= 0 || tsdbc_connect_opts("127.0.0.1", p, &o, c) != 0)
        return -1;
    if (!th_owns_port(pid, p)) {
        tsdbc_close(*c);
        *c = NULL;
        return -1;
    }
    tsdbc_set_retry(*c, 0); /* the history records what the server answered, not what a retrying client made of it */
    tsdbc_set_timeout(*c, req_deadline_s * 1000); /* a request that gets no answer ends in TSDB_ERR_IO: see REQ */
    return 0;
}

static int connect_own(int p, pid_t pid, tsdbc **c)
{
    return connect_own_ms(p, pid, c, 0);
}

/* A power loss lands at the server's next file operation, and a server that has no client left (the run's time is up while
 * the nemesis waits for a pulled plug to take effect: found once, a server "hung" 30 s after a SIGUSR2 sent in the last
 * moments, all its threads idle) makes none. This is the traffic: a row of a series of its own. A server that
 * is hung (SIGSTOP, a deadlock) does not answer it, within the timeout, and is still reported when the wait ends. */
static void poke_server(pid_t pid)
{
    tsdbc *c = NULL;
    if (connect_own_ms(atomic_load(&port), pid, &c, 2000) != 0) /* round 15 (VERIFY14 hang-power): a HELLO that took the default 10 s, once a second, stretched the wait for the plug to minutes */
        return;
    tsdbc_set_timeout(c, 2000);
    tsdb_sid id;
    if (tsdbc_series(c, "jepsen.poke", 1, &id) == 0) { /* a series of its own, that no check reads: a row of it is a WAL write at once */
        int64_t ts = (int64_t)time(NULL) * 1000 + (int64_t)(now_ns() / 1000000 % 1000);
        float v = 1.0f;
        tsdbc_insert_series(c, id, &ts, &v, 1);
    }
    tsdbc_close(c);
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

/* outage & fill: what a write to series s came to. UNAVAILABLE is "not applied" (a definite failure, checked by 2); a
 * client that gets it does not spin. Under `fill`, UNAVAILABLE carrying TSDB_WHY_FULL is counted in fill_full_refusals. */
static void outage_note(tsdbc *c, int s, int rc, int64_t invoke)
{
    if (outage) {
        if (rc == 0) {
            atomic_fetch_add(&vol_ok[s % 2], 1);
            /* the invoke time, not the ack: a call that was stuck in the hang and returns the moment it heals is a write
             * the old outage acknowledged, and says nothing of whether the volume takes new ones (it made the check
             * below pass at once in half the runs). The latest invoke among the acknowledged writes. */
            long long cur = atomic_load(&vol_last_ok[s % 2]);
            while (cur < invoke && !atomic_compare_exchange_weak(&vol_last_ok[s % 2], &cur, invoke))
                ;
        } else if (rc == TSDB_ERR_UNAVAILABLE) {
            usleep(2000);
        }
        return;
    }
    if (t_fill) {
        if (rc == 0) {
            atomic_fetch_add(&fill_writes_ok, 1);
            long long cur = atomic_load(&fill_last_ok_ns);
            while (cur < invoke && !atomic_compare_exchange_weak(&fill_last_ok_ns, &cur, invoke))
                ;
        } else if (rc == TSDB_ERR_UNAVAILABLE) {
            tsdbc_err_info ei;
            if (c && tsdbc_last_error(c, &ei) == TSDB_ERR_UNAVAILABLE && ei.has_body) {
                for (unsigned j = 0; j < ei.nunavail; j++) {
                    if (ei.unavail[j].reason == TSDB_WHY_FULL) {
                        atomic_fetch_add(&fill_full_refusals, 1);
                        break;
                    }
                }
            }
            usleep(2000);
        }
    }
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

/* Before the clients and the nemesis start, nobody else reaps the server: a server that died (a crash right after it
 * listened, the segv self-test) is a zombie that holds no socket, so connect_retry() would try for ever and a run
 * hung for 18 hours (found 2026-11-10: the self-test asleep in usleep with a zombie tsdbd). This one asks the child
 * whether it is gone, says how it ended, and gives up after SETUP_CONNECT_S. */
#define SETUP_CONNECT_S 60
static tsdbc *connect_setup(void)
{
    int64_t until = now_ns() + SETUP_CONNECT_S * 1000000000LL;
    for (;;) {
        tsdbc *c;
        pid_t pid = atomic_load(&server_pid);
        if (connect_own(atomic_load(&port), pid, &c) == 0)
            return c;
        int st;
        if (pid > 0 && reap_server(pid, &st, WNOHANG) == pid) {
            /* reap_server judged the end: a crash or an exit of its own is a violation (bail turns the 2 into 1) */
            fprintf(stderr, "jepsen: cannot connect to create the series: the server ended before the setup was done (status %#x)\n", st);
            print_tail(slogpath);
            outage_keep_log();
            server_gone();
            bail(2);
        }
        if (now_ns() > until) {
            fprintf(stderr, "jepsen: cannot connect to create the series: no connection to the server in %d s during the setup\n",
                    SETUP_CONNECT_S);
            print_tail(slogpath);
            outage_keep_log();
            stop_server(SIGKILL);
            bail(2);
        }
        usleep(2000);
    }
}

/* Self-test hook: a pause between the start of a server and the connection to it (the server of segv-tsdbd.sh is
 * dead by then, which is what the run must notice). */
static void setup_pause(void)
{
    const char *e = getenv("JEPSEN_SETUP_PAUSE_MS");
    if (e && atoi(e) > 0) { /* the watchdog's renewal signal ends a plain sleep early: sleep out the whole time */
        struct timespec ts = {atoi(e) / 1000, (atoi(e) % 1000) * 1000000L};
        while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
            ;
    }
}

/* A watchdog for the whole run: the budget is generous (every honest run ends within seconds + a few start-ups and
 * the final reads), and a run that is still going then is hung, whatever it waits for. It exits 124 with a message
 * (the servers follow it out: --exit-with-parent). JEPSEN_WATCHDOG_S sets the budget for the self-test. */
static const char *volatile jp_phase = "starting";
#define JP_PHASE(p) (jp_phase = (p))

#ifdef __linux__

/* One command with its output on stderr, at most 20 s (fork, exec and waitpid only: it runs in the watchdog's signal handler). */
static void jp_run(char *const argv[])
{
    pid_t c = fork();
    if (c == 0) {
        dup2(2, 1);
        execvp(argv[0], argv);
        _exit(127);
    }
    if (c < 0)
        return;
    for (int i = 0; i < 4000; i++) {
        int st;
        if (waitpid(c, &st, WNOHANG) == c)
            return;
        struct timespec ts = {0, 5000000};
        nanosleep(&ts, NULL);
    }
    kill(c, SIGKILL);
    waitpid(c, NULL, 0);
}

/* What the watchdog says besides the exit: the phase of the run and where the threads are (this process by gdb, which
 * the prctl in main lets attach; the server and the follower by their kernel wait channel). A hang reported as "still
 * running after N s" says nothing about what it waited for. */
static void kill_servers_now(void);
static void jp_hang_dump(void)
{
    static const char a[] = "\n--- jepsen at the hang: phase ", b[] = " ---\n";
    const char *ph = jp_phase;
    if (write(2, a, sizeof a - 1) < 0 || write(2, ph, strlen(ph)) < 0 || write(2, b, sizeof b - 1) < 0)
        return;
    char me[16] = "", ps[64] = "";
    size_t k = th_watch_itoa((int)getpid(), me), l = 0;
    me[k] = 0;
    l += th_watch_itoa((int)getpid(), ps + l);
    ps[l++] = ',';
    int sp = atomic_load(&server_pid), fp = atomic_load(&follower_pid);
    l += th_watch_itoa(sp > 0 ? sp : (int)getpid(), ps + l);
    ps[l++] = ',';
    l += th_watch_itoa(fp > 0 ? fp : (int)getpid(), ps + l);
    ps[l] = 0;
    char *const psv[] = {"ps", "-L", "-o", "pid,tid,stat,wchan:24,pcpu,comm", "-p", ps, NULL};
    jp_run(psv);
    char *const gdbv[] = {"gdb", "-p", me, "-batch", "-ex", "thread apply all bt 12", NULL};
    jp_run(gdbv);
    kill_servers_now(); /* the watchdog ends the harness: nothing it started stays up (a server's clean stop waits for its hung worker) */
}
#endif

/* Nothing outlives the harness (round 11): a tsdbd started with --exit-with-parent stops cleanly once its parent is gone, and a
 * clean stop waits for every worker, so a server with a hung worker stayed up after the harness was killed. So the harness stops
 * its own servers, by pid (the server's, the follower's), at every end it has: SIGTERM, then SIGKILL after a deadline; and the
 * signal handler of an outer kill (timeout, ^C) and the watchdog's hook SIGKILL them at once (kill() only: they run in a handler). */
static void kill_servers_now(void)
{
    pid_t sp = atomic_load(&server_pid), fp = atomic_load(&follower_pid);
    if (sp > 0)
        kill(sp, SIGKILL);
    if (fp > 0)
        kill(fp, SIGKILL);
}
static void on_outer_kill(int sig)
{
    kill_servers_now();
    signal(sig, SIG_DFL);
    raise(sig);
}
static void kill_servers_at_exit(void)
{
    pid_t pids[2] = {atomic_load(&server_pid), atomic_load(&follower_pid)};
    for (int i = 0; i < 2; i++)
        if (pids[i] > 0)
            kill(pids[i], SIGTERM);
    for (int w = 0; w < 400; w++) { /* up to 2 s for a clean stop */
        bool left = false;
        for (int i = 0; i < 2; i++)
            if (pids[i] > 0 && waitpid(pids[i], NULL, WNOHANG) != pids[i] && kill(pids[i], 0) == 0)
                left = true;
            else
                pids[i] = 0;
        if (!left)
            return;
        usleep(5000);
    }
    for (int i = 0; i < 2; i++)
        if (pids[i] > 0) {
            kill(pids[i], SIGKILL);
            waitpid(pids[i], NULL, 0);
        }
}

/* pthread_join with a deadline (round 11): a thread that does not end is a hang of the harness, and the run must end with a verdict,
 * not wait for it. On a miss: a violation naming the thread, the servers are killed by pid and the run ends here (exit 1). */
static void join_bounded(pthread_t t, const char *what, int secs)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += secs;
    if (pthread_timedjoin_np(t, NULL, &ts) == 0)
        return;
    fprintf(stderr, "  %s did not end within %d s: the server (or the harness) is hung\n", what, secs);
    print_proc_state(atomic_load(&server_pid));
    print_tail(slogpath);
    death_problems++;
    kill_servers_now();
    bail(1);
}

static void jepsen_watchdog(void)
{
#ifdef __linux__
    th_watch_hook = jp_hang_dump;
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
#endif
    long budget = (long)(seconds * 5) + 1200;
    const char *e = getenv("JEPSEN_WATCHDOG_S");
    if (e && atol(e) > 0)
        budget = atol(e);
    char name[64];
    snprintf(name, sizeof name, "jepsen --nemesis %s", nemesis);
    th_watchdog(name, budget);
}

/* Pacing (round 14). The history is kept in memory and checked after the run, and what the checker costs grows with the number of
 * operations (the arrays of MAX_OPS, the keys of every write, check_latest's covers of a hot key: quadratic). On a fast file system
 * (tmpfs: 860,000 writes in 20 s, against 17,000 on a disk that syncs) a run of 60 s took 14 minutes to check and 5.9 GB, and one of 120 s
 * reached MAX_OPS after 45 s, where every client stopped quietly (the rest of the run tested nothing) and the checker took 13 GB (the
 * OOM killer: the gate's selftest on /tmp). So the clients together do at most JEPSEN_OPS_PER_S operations a second (default 8000; 0: no
 * limit), and never more than 80% of MAX_OPS over the run's seconds: the fast file system is paced, a disk that syncs is not. A client
 * that falls behind does not catch up in a burst. A cap that is reached anyway is not quiet: see ops_cap_hit. */
static int64_t pace_ns; /* per client between two operations, 0: none */
static atomic_bool ops_cap_hit;

static void pace_init(void)
{
    double rate = 8000;
    const char *e = getenv("JEPSEN_OPS_PER_S");
    if (e)
        rate = atof(e);
    if (rate <= 0) {
        pace_ns = 0;
        return;
    }
    double cap_rate = 0.8 * max_ops / (seconds > 1 ? seconds : 1);
    if (rate > cap_rate)
        rate = cap_rate;
    pace_ns = (int64_t)(1e9 * nclients / rate);
}

static void *client(void *arg)
{
    uint64_t r = mix(seed * 1000 + (uint64_t)(intptr_t)arg);
    tsdbc *c = connect_retry();
    int64_t pace_next = now_ns();
    /* JEPSEN_CLIENT_NAP_MS: a test hook of the selftest, the clients of a machine that starves them (a run of the classes workload wrote 12 rows in 20 s
     * under 40 busy loops on 4 CPUs): every operation waits that long first, and the run, which tested nothing, must end INCONCLUSIVE */
    const char *cnap = getenv("JEPSEN_CLIENT_NAP_MS");
    while (c && !atomic_load(&stop)) {
        if (cnap)
            usleep((useconds_t)atoi(cnap) * 1000);
        if (pace_ns) {
            pace_next += pace_ns;
            int64_t d = pace_next - now_ns();
            if (d > 0)
                usleep((useconds_t)(d / 1000));
            else if (d < -1000000000LL)
                pace_next = now_ns();
        }
        r = mix(r);
        int s = (int)(r % (uint64_t)nseries);
        if (outage) /* a client works one volume only: one stuck in a hung volume's calls must not hold back the rest */
            s = (s & ~1) | (int)((intptr_t)arg % 2);
        const uint64_t td = tier_n ? 8 : 0; /* the tier: more deletes and replaces, which a settle must survive */
        if ((r >> 8) % 100 < 6 + td) { /* a delete or a replace of a key range */
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= max_ops) {
                atomic_store(&ops_cap_hit, true);
                break;
            }
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
            int rc = w->kind == W_REPLACE ? REQ("replace of a key range", tsdbc_replace_range(c, ids[s], a, b, ts, &col, m))
                                          : REQ("delete of a key range", tsdbc_delete_range(c, ids[s], a, b));
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            outage_note(c, s, rc, w->invoke);
            free(ts);
            free(v);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) {
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&write_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        } else if (latest_on && (r >> 8) % 100 < 12 + td) { /* nulls over a key range, near the top: the newest row's null */
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= max_ops) {
                atomic_store(&ops_cap_hit, true);
                break;
            }
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
            int rc = REQ("insert of nulls", tsdbc_insert_series(c, ids[s], ts, v, n));
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            outage_note(c, s, rc, w->invoke);
            free(ts);
            free(v);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) {
                if (rc == TSDB_ERR_CORRUPT)
                    atomic_fetch_add(&write_damage, 1);
                tsdbc_close(c);
                c = connect_retry();
            }
        } else if (latest_on && (r >> 8) % 100 < 20 + td) { /* the newest row of every series, in one request */
            uint32_t base = atomic_fetch_add(&nlats, (uint32_t)nseries);
            if (base + (uint32_t)nseries > MAX_LATEST) {
                atomic_store(&ops_cap_hit, true);
                break;
            }
            bool wn = (r >> 14) % 2;
            int64_t end = (r >> 15) % 3 ? INT64_MAX : key_ts(s, (uint32_t)(mix(r + 9) % nkeys[s]));
            tsdb_latest_result out[MAX_SERIES];
            tsdb_latest_opts o = {.end = end, .ends = NULL, .with_nulls = wn, .threads = 0};
            int64_t t0 = now_ns();
            int rc = REQ("latest", tsdbc_latest(c, ids, (size_t)nseries, &o, out));
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
        } else if ((r >> 8) % 100 < 60 + td) { /* a write */
            uint32_t n = (r >> 16) % 100 < (tier_n ? 65 : 85) ? 1 + (uint32_t)((r >> 24) % 20)
                                                               : 400 + (uint32_t)((r >> 24) % 2600);
            uint32_t id = atomic_fetch_add(&nwrites, 1);
            if (id >= max_ops) {
                atomic_store(&ops_cap_hit, true);
                break;
            }
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
            int rc = REQ("insert", tsdbc_insert_series(c, ids[s], ts, v, m));
            w->complete = now_ns();
            w->st = rc == 0 ? ST_OK : (rc == TSDB_ERR_IO || rc == TSDB_ERR_CORRUPT) ? ST_INFO : ST_FAIL;
            outage_note(c, s, rc, w->invoke);
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
            if (idx >= max_ops) {
                atomic_store(&ops_cap_hit, true);
                break;
            }
            rop *q = &reads[idx];
            q->series = s;
            q->lo = pick_key(s, r >> 12, r >> 50, 50);
            q->hi = q->lo + 1 + (uint32_t)((r >> 30) % 400);
            if (q->hi > nkeys[s])
                q->hi = nkeys[s];
            q->seen = calloc(q->hi - q->lo, sizeof *q->seen);
            tsdb_points p;
            q->invoke = now_ns();
            int rc = REQ("read", tsdbc_query_points(c, ids[s], key_ts(s, q->lo), key_end(s, q->hi), &p));
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

/* ------------------------------------------------------------- the watcher */

/* Where the j-th third of the run starts (j = 3: its end). */
static int64_t nem_third_start(int j) { return nem_t0_ns + (int64_t)(seconds * 1e9) * j / 3; }

/* A plug the nemesis pulled that the shim did not log (the server had died of its own odds just before the signal could reach
 * it): not a fault, but the nemesis was at work for that long, and a third of the run it was at work in is not one it left
 * idle. It counts for the spread of the run only, never for the floor (a shim that ignores the signal, a nemesis that
 * does not pull: no attempts, or no faults). */
static bool nem_att_third[3];
static void nem_attempt_span(int64_t t, int64_t end)
{
    for (int j = 0; j < 3; j++)
        if (t < nem_third_start(j + 1) && end >= nem_third_start(j))
            nem_att_third[j] = true;
}

/* Whether the server of server_pid is running: a child that has exited and not been reaped (WNOWAIT leaves it for the
 * nemesis), one that has been reaped (ECHILD) and no pid at all are not. Stopped (SIGSTOP) is running. */
static bool server_running(void)
{
    pid_t pid = atomic_load(&server_pid);
    if (pid <= 0)
        return false;
    siginfo_t si;
    si.si_pid = 0;
    if (waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) != 0)
        return false;
    return si.si_pid != pid;
}

/* Whether process `pid` is stopped by a signal (state T in /proc/PID/stat; t, a trace stop, is not). */
static bool pid_stopped(pid_t pid)
{
    if (pid <= 0)
        return false;
    char path[64], buf[512];
    snprintf(path, sizeof path, "/proc/%d/stat", (int)pid);
    FILE *f = fopen(path, "r");
    if (!f)
        return false;
    size_t n = fread(buf, 1, sizeof buf - 1, f);
    fclose(f);
    buf[n] = 0;
    const char *p = strrchr(buf, ')'); /* the name may hold anything, spaces and parentheses included */
    return p && p[1] == ' ' && p[2] == 'T';
}

/* A server lay dead from `since` to `tt`. The whole stretch counts: a machine that starves or stalls the nemesis thread (the CPU, memory
 * reclaim, a disk) makes an honest run lay a server dead for 450 to 650 ms, and the run is then INCONCLUSIVE and repeated; what the
 * nemesis spends on a spin in the stretch is no different from that on any account the kernel keeps, so none is subtracted. */
static void dead_stretch(int64_t since, int64_t tt)
{
    int64_t len = tt - since;
    watch_dead_total_ns += len;
    if (len > watch_dead_worst_ns)
        watch_dead_worst_ns = len;
}

/* An independent clock on the servers while the nemesis runs, until the run's time is up (stop_ns): every WATCH_POLL_MS it
 * stamps the death of a server (the first poll that finds none running) and the restart (the first that finds one), and
 * keeps the longest and the total of the stretches in between and the time a server was up and listening (port set, not
 * stopped). A server dead as the time ends is a stretch to stop_ns. A server lying stopped (SIGSTOP) is neither dead nor
 * listening: its stretches have a bound of their own. The longest time a server was listening with no fault issued or in
 * progress (the nemesis' activity is its own atomic stamp and counter, read here on the watcher's clock) is the bound on a
 * nemesis that has stopped working part of the way through. Nothing here depends on what the nemesis looks at or when. */
static void *watcher(void *arg)
{
    (void)arg;
    bool dead = false, prev_serving = false;
    int64_t since = 0, last = now_ns(), gap = 0, stop_since = 0, step_run = 0;
    for (;;) {
        usleep(WATCH_POLL_MS * 1000);
        int64_t t = now_ns(), lim = atomic_load(&stop_ns);
        bool end = lim && t >= lim;
        int64_t tt = end ? lim : t; /* what happens after the run's time is not the nemesis's */
        bool running = server_running();
        int64_t act = atomic_load(&nem_act_ns);
        bool fresh = act > last; /* a fault was issued, or carried out, since the last poll */
        if (atomic_load(&watch_excused)) {
            /* the checker's own time: not counted as listening. A fault issued inside it (a kill whose restart runs the checker)
             * still ends the stretch before it: left out, the stretches before each of them add up across every kill */
            if (fresh || atomic_load(&nem_inprog) > 0)
                gap = 0;
            if (dead)
                since = tt;
            prev_serving = false;
            last = tt;
        } else {
            /* a fault step in progress: the time a server listens in it is the nemesis at work, to its own end (a pulled plug, an EIO's
             * fail-stop), but a kill step is a SIGKILL and a start: the time a server lives in it past a bound is the nemesis
             * dawdling (killslowstep) and counts as free time, like the nap it is */
            bool inprog = atomic_load(&nem_inprog) > 0;
            if (!inprog)
                step_run = 0;
            else if (prev_serving && tt > last) {
                step_run += tt - last;
                if (step_run > watch_step_worst_ns)
                    watch_step_worst_ns = step_run;
            }
            if (inprog && !strcmp(nemesis, "kill") && step_run > NEM_KILL_STEP_MAX_MS * 1000000LL)
                inprog = false;
            if (prev_serving && tt > last) {
                watch_live_ns += tt - last;
                if (!inprog)
                    watch_free_ns += tt - last;
                for (int j = 0; j < 3; j++) { /* the part of [last, tt] inside the j-th third */
                    int64_t a = last > nem_third_start(j) ? last : nem_third_start(j), b = tt < nem_third_start(j + 1) ? tt : nem_third_start(j + 1);
                    if (b > a)
                        watch_live3_ns[j] += b - a;
                }
            }
            /* the time a server has been listening since the nemesis last issued a fault or was carrying one out */
            if (inprog)
                gap = 0;
            else if (fresh) {
                if (gap > 1500000000LL && getenv("JEPSEN_FAULT_TRACE")) /* a long stretch with no fault, ended at the fault of `act` */
                    fprintf(stderr, "jepsen: %.2f s listening with no fault, ended at %.3f s\n", (double)gap / 1e9, (double)(act - nem_t0_ns) / 1e9);
                gap = prev_serving && tt > act ? tt - act : 0;
            }
            else if (prev_serving && tt > last)
                gap += tt - last;
            if (gap > watch_gap_worst_ns)
                watch_gap_worst_ns = gap;
            bool stopped = running && pid_stopped(atomic_load(&server_pid));
            if (stopped && !stop_since) {
                stop_since = tt;
                if (!strcmp(nemesis, "pause")) { /* the pause nemesis' fault is confirmed by the stop itself, seen here */
                    nem_fault_span(tt, tt);
                    gap = 0;
                }
            } else if (!stopped && stop_since) {
                if (tt - stop_since > watch_stop_worst_ns)
                    watch_stop_worst_ns = tt - stop_since;
                stop_since = 0;
            }
            if (!running && !dead) {
                dead = true;
                since = tt;
                watch_deaths++;
                /* a server that ended and is not reaped yet: judged here, at once, whatever the nemesis does next (it may sleep for
                 * seconds, or be none at all); an end the nemesis reaps first is judged by its reap_server */
                int est;
                pid_t wp = atomic_load(&server_pid);
                if (!atomic_load(&server_starting) && end_status(wp, &est))
                    end_report(wp, est);
            } else if (running && dead) {
                dead = false;
                watch_restarts++;
                if (tt > since)
                    dead_stretch(since, tt);
            }
            prev_serving = running && !stopped && atomic_load(&port) > 0;
            last = tt;
        }
        if (end) {
            watch_tail_ns = gap; /* the listening time since the last fault, as the run's time was up */
            if (stop_since && lim > stop_since && lim - stop_since > watch_stop_worst_ns) /* stopped as the time was up */
                watch_stop_worst_ns = lim - stop_since;
            if (dead && lim > since) /* no server as the time was up: dead since its death, to the end of the run */
                dead_stretch(since, lim);
            return NULL;
        }
    }
}

/* The server of the quiet tail has no shim (round 12; VERIFY11 (A): a hung worker under power passed in 8 of 9 runs). A server under the
 * shim dies by its own odds (CRASHFS_PROB: a power loss or an EIO at a random file operation, most within milliseconds of its start), and a
 * server that dies in the tail takes with it every request that waits on a hung worker: the clients see a closed connection, not a
 * missing answer, and the hang never reaches the deadline. So when the nemesis is quiet the faulty server is stopped (SIGTERM; a server the
 * shim ended already is reaped) and a clean one, which nothing ends but the harness, takes the tail and answers or does not. Under the lock of
 * the server (the nemesis is not in a step). Judged like any end: our SIGTERM, or the shim's own planned exit. */
static void quiet_tail_server(void)
{
    pid_t pid = atomic_load(&server_pid);
    int st = 0;
    if (pid > 0 && reap_server(pid, &st, WNOHANG) == pid)
        server_gone();
    else if (pid > 0 && server_faulty)
        stop_server_st(SIGTERM, &st);
    else if (pid > 0)
        return; /* a clean server already (the faulty one died while starting): it is the tail's */
    start_server(false);
}

/* ------------------------------------------------------------- nemesis */

/* Sleeps `us`; with a fault-injecting server (power) it ends early once the server has crashed by itself, which
 * the shim does a few ms after the listener is up (the fault is due after a fixed count of file operations).
 * Sleeping through it left the server dead for most of the run and the clients idle. The process is not reaped
 * here (WNOWAIT): the nemesis below does that. */
static void nap(useconds_t us, bool watch)
{
    /* --mutant-idle, --mutant-longnap, --mutant-nemesis nap:MS: a server dies in a nap that does not watch it (what a real
     * run does by the shim's own odds, only not left to chance: 1 of ~20 loaded runs of idle, and 1 of 10 of nap:600, had no
     * death in a long nap): the plug is pulled at the start of every nap of 400 ms or more, so that the server lies dead for
     * what is left of it, far above the dead bound */
    if ((idle_mutant || idle_mutant_long || (nem_mutant && !strncmp(nem_mutant, "nap:", 4))) && !watch && us >= 400000 && atomic_load(&server_pid) > 1 &&
        !atomic_load(&nem_quiet)) /* a quiet nemesis does nothing, also a mutant: the tail's server is a clean one, which a SIGUSR2 kills */
        signal_pid(atomic_load(&server_pid), SIGUSR2);
    for (useconds_t t = 0; t < us && !atomic_load(&stop) && !atomic_load(&nem_stop) && !atomic_load(&nem_quiet); t += 5000) {
        pid_t pid = atomic_load(&server_pid);
        siginfo_t si;
        si.si_pid = 0;
        if (watch && pid > 0 && waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT) == 0 && si.si_pid == pid)
            return;
        usleep(5000);
    }
}

/* How a server may end under the power nemesis: crashfs's power loss is _exit(99); a plain kill of ours (an idle server);
 * SIGUSR2 on a server without the shim (the clean one after a failed start, which has no handler). Anything
 * else, a SIGSEGV or abort() in the server above all, is a crash bug that the restarts would otherwise hide. Since round 11
 * end_report judges every end of a server under every nemesis (see there); this stays as the call of the places that
 * knew a death before the nemesis' reap did. */
static void note_death(int st, bool we_killed, pid_t pid)
{
    (void)we_killed; /* the signals we sent are written down in signal_pid */
    end_report(pid, st);
}

/* The faults injected in this epoch (the shim's misdirected I/O and rot's damage, one line each in the fault log; a torn tail the engine
 * cut is not one), and whether a server's log names damage (a fail-stop or a refused open on corrupt data, a checksum, an I/O error). */
static int epoch_faults(void)
{
    int n = 0;
    char line[3 * PATH_MAX];
    FILE *f = fopen(faultlog, "r");
    while (f && fgets(line, sizeof line, f))
        n += strstr(line, "the engine cut") == NULL;
    if (f)
        fclose(f);
    return n;
}

/* tsdbd exits with status 1 for two reasons it says so for: the fail-stop (failwatch_main: "storage failure, exiting") and a close that
 * failed ("tsdbd: close: corrupt data": the clean stop after our SIGTERM found damage, which a misdirected write left). An exit 1 with
 * neither line is not one of them. */
static bool log_has_failstop(const char *path, long from)
{
    FILE *f = fopen(path, "r");
    char b[1024];
    bool hit = false;
    if (f && from > 0 && fseek(f, from, SEEK_SET) != 0)
        rewind(f);
    while (f && !hit && fgets(b, sizeof b, f))
        hit = strstr(b, "tsdbd: storage failure, exiting") != NULL || strstr(b, "tsdbd: close: ") != NULL;
    if (f)
        fclose(f);
    return hit;
}

static bool log_names_damage(const char *path, long from, char *line, size_t n)
{
    if (log_says_damage(path, from, line, n))
        return true;
    FILE *f = fopen(path, "r");
    char b[1024];
    bool hit = false;
    if (f && from > 0 && fseek(f, from, SEEK_SET) != 0)
        rewind(f);
    /* only what the engine and the fail-stop say, not tsdbd's start-up notes (the one about a file system that keeps no checksums
     * names "checksums" and "damaged" and is no damage) */
    while (f && !hit && fgets(b, sizeof b, f)) {
        hit = (!strncmp(b, "tsdb: ", 6) || !strncmp(b, "tsdbd: storage failure", 22)) &&
              (strcasestr(b, "corrupt") || strcasestr(b, "damaged") || strcasestr(b, "checksum") ||
               strcasestr(b, "input/output error") || strcasestr(b, "I/O error"));
        if (hit) {
            b[strcspn(b, "\n")] = 0;
            snprintf(line, n, "%.400s", b);
        }
    }
    if (f)
        fclose(f);
    return hit;
}

/* misdirect and rot: how a server ended. Ours: exit 0 after our SIGTERM, a death by the signal we sent. Expected: exit 1 with a log that
 * names the damage, in an epoch where a fault was injected (a fail-stop on what the fault left). Anything else is a violation (round 12): exit 0
 * of its own, exit 1 with no fault in the epoch or no damage in the log, another status; a signal of its own (SIGSEGV, SIGABRT...) is a
 * crash, which damage must never cause. */
static void md_death(int st, int sig, pid_t pid)
{
    if ((sig && WIFSIGNALED(st) && WTERMSIG(st) == sig) || (sig == SIGTERM && WIFEXITED(st) && WEXITSTATUS(st) == 0))
        return;
    char dl[512];
    if (WIFEXITED(st) && WEXITSTATUS(st) == 1 && epoch_faults() > 0 && log_has_failstop(slogpath, slog_from) && log_names_damage(slogpath, slog_from, dl, sizeof dl)) {
        md_failstops++; /* the one end that is expected: a fail-stop (tsdbd says "storage failure, exiting") whose log names damage, in an epoch where a fault was injected */
        return;
    }
    if (WIFEXITED(st)) { /* round 12: exit 0 of its own, or exit 1 with no fault in this epoch or no damage in its log, is not a fail-stop on damage */
        death_problems++;
        fprintf(stderr, "  the server (pid %d) ended with %s by itself: not a fail-stop on damage (that is exit status 1, with a log that names the "
                "damage, in an epoch where a fault was injected: %d so far)\n", (int)pid, st ? "exit status 1 or more" : "exit status 0",
                epoch_faults());
        print_tail(slogpath);
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
        DIR *d = opendir(dbdir);
        unsigned seen = 0;
        struct dirent *e;
        while (d && (e = readdir(d)) != NULL) {
            size_t n = strlen(e->d_name);
            bool match = kind[0] == '.' ? n > 5 && !strcmp(e->d_name + n - 5, ".part") && strncmp(e->d_name, "tmp_", 4)
                                        : !strncmp(e->d_name, kind, strlen(kind));
            if (match && (seen++, mix(r + seen) % seen == 0))
                snprintf(pick, sizeof pick, "%.3800s/%s", dbdir, e->d_name);
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
/* The most a healed volume is ever given, whatever the server logged: the flapping bound and one more 60 s, at most (a
 * slow write probe restarts the 60 s of good probes; a disk that slow for 60 s more is evidence, an endless run of
 * them is a stall). The nemesis's thread is joined within this plus the fault that precedes the heal and some slack. */
#define OUTAGE_HARD_S (OUTAGE_RECOVER_S + 2 * OUTAGE_FLAP_S)
#define OUTAGE_JOIN_S (OUTAGE_HARD_S + 60)
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

/* jepsen's own evidence of a slow disk: a thread of this process (the shim lives in the server, so nothing here is
 * hung or slowed by it) writes and fdatasyncs 4 KiB beside the volumes four times a second and keeps the ones that
 * took long. A slow write probe the server logs counts as the host's doing only if one of these overlaps it; a server
 * that reports every probe slow while the disk is fine (a planted stall in the health monitor) has none. */
#define SAMPLE_SLOW_MS 250
#define SAMPLE_KEEP 512
static struct {
    int64_t end_ns;
    int ms;
} samp[SAMPLE_KEEP];
static int samp_n;
static pthread_mutex_t samp_mu = PTHREAD_MUTEX_INITIALIZER;
static atomic_bool samp_stop;
static pthread_t samp_th;
static bool samp_on;

static void *disk_sampler(void *arg)
{
    (void)arg;
    char path[PATH_MAX + 32];
    snprintf(path, sizeof path, "%s.disk-sample", vol_dir[0]);
    int fd = open(path, O_WRONLY | O_CREAT, 0644);
    if (fd < 0)
        return NULL;
    char buf[4096];
    memset(buf, 0, sizeof buf);
    for (unsigned n = 0; !atomic_load(&samp_stop); n++) {
        buf[0] = (char)n;
        int64_t t0 = now_ns();
        if (pwrite(fd, buf, sizeof buf, 0) == (ssize_t)sizeof buf)
            (void)fdatasync(fd);
        int64_t t1 = now_ns();
        int ms = (int)((t1 - t0) / 1000000);
        if (ms >= SAMPLE_SLOW_MS) {
            pthread_mutex_lock(&samp_mu);
            samp[samp_n++ % SAMPLE_KEEP] = (typeof(samp[0])){t1, ms};
            pthread_mutex_unlock(&samp_mu);
        }
        usleep(250000);
    }
    close(fd);
    unlink(path);
    return NULL;
}

/* The slowest of jepsen's own samples that ended within [t - 8 s, t + 1 s] (t on the monotonic clock, in seconds): the
 * probe that was logged at t began up to its own duration before. 0 when none was slow. */
static int slow_sample_near(double t)
{
    int worst = 0;
    int64_t lo = (int64_t)((t - 8.0) * 1e9), hi = (int64_t)((t + 1.0) * 1e9);
    pthread_mutex_lock(&samp_mu);
    for (int i = 0; i < SAMPLE_KEEP && i < samp_n; i++)
        if (samp[i].end_ns >= lo && samp[i].end_ns <= hi && samp[i].ms > worst)
            worst = samp[i].ms;
    pthread_mutex_unlock(&samp_mu);
    return worst;
}

/* The "a write probe took N ms" lines of the server's log (health.c, src/health.c probe_done) for the domain `domain`
 * stamped at or after `since_s` (the monotonic clock in seconds, the one jepsen's trace uses too): their times, in
 * file order, at most `max`. Such a line says why a flapping domain is still DOWN: a probe that works but takes more
 * than fast_ms starts the 60 s of good probes over, and on a loaded machine the host's disk does that (a fdatasync
 * of 3 s, seen in the 10-seed runs). */
static int slow_probe_lines(const char *path, const char *domain, double since_s, double *out, int max)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return 0;
    char line[4096], pat[PATH_MAX + 32];
    snprintf(pat, sizeof pat, "storage domain %s: a write probe took", domain);
    int n = 0;
    while (n < max && fgets(line, sizeof line, f)) {
        double t;
        if (strncmp(line, "tsdb: [", 7) || sscanf(line + 7, "%lf]", &t) != 1 || !strstr(line, pat))
            continue;
        if (t >= since_s)
            out[n++] = t;
    }
    fclose(f);
    return n;
}

/* The latest such line, or 0 (the self-test of the scan: --scan-slow-probes). */
static double slow_probe_since(const char *path, const char *domain, double since_s)
{
    double t[256], last = 0;
    int n = slow_probe_lines(path, domain, since_s, t, 256);
    for (int i = 0; i < n; i++)
        if (t[i] > last)
            last = t[i];
    return last;
}

/* Whether the server reports a volume as failed for good: its TOPOLOGY says UNAVAILABLE with the reason DIVERGED (a reopen found the data
 * it had acknowledged gone, and tsdbd keeps the volume quiesced until it is restarted). Any other reason (NFS_DOWN, RECOVERING...) is the
 * outage still being handled: not a failure. */
static bool outage_volume_failed(void)
{
    tsdbc *c = NULL;
    if (connect_own(atomic_load(&port), atomic_load(&server_pid), &c) != 0)
        return false;
    tsdbc_set_timeout(c, 10000);
    tsdbc_topo *t = NULL;
    bool failed = false;
    if (tsdbc_topology(c, &t) == 0 && t) {
        for (size_t i = 0; i < t->nvols; i++)
            failed |= outage_entry_failed(t->vols[i].health, t->vols[i].reason);
        tsdbc_topology_free(t);
    }
    tsdbc_close(c);
    return failed;
}

/* Waits until a write to volume v is acknowledged after `since` (ns): within the bound, or (a flapping domain only)
 * within the bound counted from a slow write probe the server logged after the heal AND jepsen's own fdatasync was
 * slow then too, but never past OUTAGE_HARD_S after the heal. Past that the volume has failed its heal, and the
 * failure names the slow-probe lines the server wrote. */
static void outage_await_recovery(int v, int64_t since, const char *what)
{
    int64_t t0 = now_ns();
    bool flap = outage_flapping(v);
    int bound = OUTAGE_RECOVER_S + (flap ? OUTAGE_FLAP_S : 0);
    int64_t deadline = t0 + bound * 1000000000LL, hard = t0 + OUTAGE_HARD_S * 1000000000LL;
    bool extended = false, late = false;
    outage_heals++;
    while (!atomic_load(&stop)) {
        if (atomic_load(&vol_last_ok[v]) > since) {
            double took = (double)(now_ns() - since) / 1e9;
            outage_judged++;
            if (took > OUTAGE_HARD_S) { /* it came back, late: liveness */
                INCONCLUSIVE("outage-heal", "volume %d: a write was acknowledged only %.1f s after the heal (%s; at most %d s)\n", v,
                             took, what, OUTAGE_HARD_S);
                print_tail(slogpath);
                return;
            }
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
        if (now_ns() > deadline && deadline < hard && flap) {
            /* a probe the server logged as slow after the heal started the 60 s again, and a slow fdatasync of
             * jepsen's own at the same time says the disk was slow: the bound runs from there, up to the hard limit
             * (a real stall has no such line, and fails at the end of its bound as before) */
            double lines[256], best = 0;
            int n = slow_probe_lines(slogpath, vol_dir[v], (double)since / 1e9, lines, 256);
            for (int i = 0; i < n; i++) {
                int ms = slow_sample_near(lines[i]);
                if (ms > 0 && lines[i] > best)
                    best = lines[i];
                else if (ms == 0 && getenv("JEPSEN_OUTAGE_TRACE"))
                    fprintf(stderr, "jepsen: [%.3f] volume %d: a slow write probe logged at %.3f, no slow fdatasync of "
                            "jepsen's own near it: not a reason to wait\n", (double)now_ns() / 1e9, v, lines[i]);
            }
            if (best > 0 && (int64_t)(best * 1e9) + bound * 1000000000LL > deadline) {
                deadline = (int64_t)(best * 1e9) + bound * 1000000000LL;
                if (deadline > hard)
                    deadline = hard;
                if (!extended)
                    outage_slow_extended++;
                extended = true;
                fprintf(stderr, "  volume %d: a write probe took more than the fast limit at %.3f, and so did jepsen's own "
                        "fdatasync (%d ms): the bound runs from there (until %.0f s after the heal at the latest)\n", v,
                        best, slow_sample_near(best), (double)(deadline - t0) / 1e9);
            }
        }
        if (now_ns() > deadline) {
            /* How soon writes resume after a heal is liveness (round 11: on a machine at a load average of 200 the write probes took 7.5 s,
             * and an honest run failed on this): past the bound the run is INCONCLUSIVE, said once, and the wait goes on to the hard
             * limit. Only a volume that never comes back at all in that much longer time AND that the server itself reports as failed
             * (TOPOLOGY: UNAVAILABLE with the reason DIVERGED, a reopen that found lost data, which tsdbd keeps until it is restarted)
             * is a VIOLATION: that is data lost, not a slow disk. */
            if (now_ns() <= hard) {
                if (!late) {
                    late = true;
                    INCONCLUSIVE("outage-heal", "volume %d: no write acknowledged in %.0f s after the heal (%s%s): waiting up to %d s\n", v,
                                 (double)(deadline - t0) / 1e9, what, flap ? "; its domain was flapping" : "", OUTAGE_HARD_S);
                }
                usleep(20000);
                continue;
            }
            double lines[256];
            int n = slow_probe_lines(slogpath, vol_dir[v], (double)since / 1e9, lines, 256), ev = 0;
            for (int i = 0; i < n; i++)
                ev += slow_sample_near(lines[i]) > 0;
            outage_judged++;
            if (outage_volume_failed()) {
                outage_problems++;
                fprintf(stderr, "  volume %d: no write acknowledged in %d s after the heal (%s), and the server reports a volume as failed "
                        "(reason DIVERGED or LOST): it will not come back\n", v, OUTAGE_HARD_S, what);
            } else {
                INCONCLUSIVE("outage-heal", "volume %d: still no write acknowledged %d s after the heal (%s%s): the server reports no failure, "
                             "so this is a stall of the disk or the machine\n", v, OUTAGE_HARD_S, what,
                             extended ? ", after a bound extended by slow probes" : "");
            }
            if (n > 0) {
                fprintf(stderr, "  the server logged %d slow write probes of this domain since the heal (%d with a slow "
                        "fdatasync of jepsen's own near it), at", n, ev);
                for (int i = 0; i < n && i < 8; i++)
                    fprintf(stderr, " %.3f", lines[i]);
                fprintf(stderr, "%s\n", n > 8 ? " ..." : "");
            }
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
    if (pid > 0 && reap_server(pid, &st, WNOHANG) == pid) {
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
        int64_t kissued = now_ns();
        int kst = 0;
        bool kdied = atomic_load(&server_pid) > 0 && stop_server_st(SIGKILL, &kst) && WIFSIGNALED(kst); /* the shim lives in the server: the hang ends with it, the next server opens a healthy directory */
        server_gone();
        start_server(true);
        if (kdied) /* counted once the server has died of it and a new one listens */
            nem_fault_span(kissued, now_ns());
        restarts++;
        outage_kills++;
        return;
    }
    outage_faults++;
    nem_fault();
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

/* ---- R15 local tier nemeses (docs/design-cluster-data.md §15.9, E55):
 *   store-eio    EIO on one store (reads decode around it, settles pause and resume after heal, server never stops)
 *   store-stuck  hung I/O on one store (store goes DOWN_STALLED, reads decode around it without stalling; kills downnoread)
 *   store-lost   wipe one store and `tsdb tier lost --replace` under live load (rebuilt store holds chunks; kills lostnoreplace)
 *   salvage      wipe m+1 stores, verify E15 TSDB_ERR_IO + stats before salvage, `tsdb tier salvage`, replace & resume;
 *                plus post-run live `tsdb_tier_salvage` latest-table oracle check (kills salvagelatest)
 *   fill         drive fast root's statvfs fill to 92% (`UNAVAILABLE WHY_FULL` refusals, never lost writes; kills fullnoflag)
 *   single-copy  one store (`--single-copy`, m = 0, 1+0) with rate/slow/kill faults */

static const char *tsdb_cli_path(void) { return getenv("JEPSEN_TSDB") ? getenv("JEPSEN_TSDB") : "./build/tsdb"; }

/* Runs `tsdb` CLI with LD_PRELOAD unset; copies combined stdout+stderr into out[0..outcap) if non-NULL. */
static int tier_cli_run(char *const argv[], char *out, size_t outcap)
{
    int pfd[2] = {-1, -1};
    if (out && outcap > 0) {
        out[0] = 0;
        if (pipe(pfd) != 0)
            pfd[0] = pfd[1] = -1;
    }
    pid_t pid = fork();
    if (pid == 0) {
        unsetenv("LD_PRELOAD");
        unsetenv("OUTAGEFS_CTL");
        unsetenv("OUTAGEFS_ROOTS");
        if (pfd[1] >= 0) {
            dup2(pfd[1], 1);
            dup2(pfd[1], 2);
            close(pfd[0]);
            close(pfd[1]);
        }
        execv(tsdb_cli_path(), argv);
        _exit(127);
    }
    if (pfd[1] >= 0)
        close(pfd[1]);
    if (pfd[0] >= 0 && out && outcap > 0) {
        size_t got = 0;
        ssize_t n;
        while (got + 1 < outcap && (n = read(pfd[0], out + got, outcap - 1 - got)) > 0)
            got += (size_t)n;
        out[got] = 0;
        close(pfd[0]);
    }
    int st = 0;
    if (pid < 0 || waitpid(pid, &st, 0) != pid)
        return -1;
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static void rm_tree_dir(const char *p)
{
    DIR *d = opendir(p);
    if (!d)
        return;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;
        char q[PATH_MAX];
        int n = snprintf(q, sizeof q, "%s/%s", p, e->d_name);
        if (n <= 0 || (size_t)n >= sizeof q)
            continue;
        struct stat st;
        if (lstat(q, &st) == 0 && S_ISDIR(st.st_mode))
            rm_tree_dir(q);
        else
            unlink(q);
    }
    closedir(d);
    rmdir(p);
}

static int tier_outage_ctl(const char *cmd, const char *path, const char *arg)
{
    char line[PATH_MAX + 64], out[256];
    snprintf(line, sizeof line, "%s %s%s%s", cmd, path, arg ? " " : "", arg ? arg : "");
    if (getenv("JEPSEN_VERBOSE"))
        fprintf(stderr, "jepsen: tier outagefs: %s\n", line);
    int rc = outagefs_cmd(octl, line, out, sizeof out);
    if (rc != 0)
        fprintf(stderr, "  tier outagefs refused '%s': %s\n", line, out);
    return rc;
}

static bool tier_status_get(const char *key, char *val, size_t n)
{
    tsdbc *c = NULL;
    if (connect_own_ms(atomic_load(&port), atomic_load(&server_pid), &c, 3000) != 0)
        return false;
    tsdbc_set_timeout(c, 3000);
    char *txt = NULL;
    bool ok = false;
    if (tsdbc_status(c, &txt) == 0 && txt) {
        size_t kl = strlen(key);
        for (char *p = txt; *p;) {
            char *nl = strchr(p, '\n');
            size_t ll = nl ? (size_t)(nl - p) : strlen(p);
            if (ll > kl && strncmp(p, key, kl) == 0 && p[kl] == '=') {
                size_t vl = ll - kl - 1;
                if (vl >= n)
                    vl = n - 1;
                memcpy(val, p + kl + 1, vl);
                val[vl] = 0;
                ok = true;
                break;
            }
            p = nl ? nl + 1 : p + ll;
        }
        free(txt);
    }
    tsdbc_close(c);
    return ok;
}

static bool tier_wait_status(const char *key, const char *want, bool negate, int timeout_ms)
{
    int64_t end = now_ns() + (int64_t)timeout_ms * 1000000LL;
    char val[128];
    while (now_ns() < end && !atomic_load(&stop)) {
        if (tier_status_get(key, val, sizeof val)) {
            bool eq = strcmp(val, want) == 0;
            if (negate ? !eq : eq)
                return true;
        }
        usleep(50000);
    }
    return false;
}

static int count_ec_files(const char *d, int *settled_copies_out)
{
    int ec = 0, sc = 0;
    DIR *dp = opendir(d);
    struct dirent *e;
    while (dp && (e = readdir(dp)) != NULL) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] == 'p' && l > 3 && strcmp(e->d_name + l - 3, ".ec") == 0)
            ec++;
        else if (strncmp(e->d_name, "settled_", 8) == 0)
            sc++;
    }
    if (dp)
        closedir(dp);
    if (settled_copies_out)
        *settled_copies_out = sc;
    return ec;
}

static int list_ec_partitions(const char *d, int64_t *parts, int max)
{
    int np = 0;
    DIR *dp = opendir(d);
    struct dirent *e;
    while (dp && (e = readdir(dp)) != NULL) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
            continue;
        long long p = 0, lo = 0, hi = 0;
        unsigned lvl = 0;
        if (sscanf(e->d_name, "p%lld_%lld_%lld_%u", &p, &lo, &hi, &lvl) != 4)
            continue;
        bool dup = false;
        for (int i = 0; i < np && !dup; i++)
            dup = parts[i] == (int64_t)p;
        if (!dup && np < max)
            parts[np++] = (int64_t)p;
    }
    if (dp)
        closedir(dp);
    return np;
}

static void part_key_range(int64_t key, int64_t pms, int64_t *lo, int64_t *hi)
{
    static const int64_t mult[4] = {1, 7, 28, 364};
    int c = (int)(((key % 4) + 4) % 4);
    int64_t base = (key - c) / 4;
    *lo = base * pms;
    *hi = (base + mult[c]) * pms;
}

/* Reads the 16-byte OID from each live p*.ec file in `d` (trailer EC_TR_REC_OFF=16 -> record EC_REC_OID=8)
 * into lowercase 32-hex strings. */
static int read_ec_oids(const char *d, char oids[][33], int max)
{
    static const char hex[] = "0123456789abcdef";
    int n = 0;
    DIR *dp = opendir(d);
    struct dirent *e;
    while (dp && (e = readdir(dp)) != NULL && n < max) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
            continue;
        char fp[PATH_MAX + 256];
        int pn = snprintf(fp, sizeof fp, "%s/%s", d, e->d_name);
        if (pn <= 0 || (size_t)pn >= sizeof fp)
            continue;
        int fd = open(fp, O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            continue;
        struct stat st;
        if (fstat(fd, &st) == 0 && st.st_size >= 128) {
            uint8_t tr[64], ob[16];
            if (pread(fd, tr, 64, st.st_size - 64) == 64) {
                uint64_t rec_off = 0;
                memcpy(&rec_off, tr + 16, 8);
                if (rec_off + 24 <= (uint64_t)st.st_size && pread(fd, ob, 16, (off_t)(rec_off + 8)) == 16) {
                    for (int i = 0; i < 16; i++) {
                        oids[n][2 * i] = hex[ob[i] >> 4];
                        oids[n][2 * i + 1] = hex[ob[i] & 0xf];
                    }
                    oids[n][32] = 0;
                    n++;
                }
            }
        }
        close(fd);
    }
    if (dp)
        closedir(dp);
    return n;
}

static bool oid_in_list(const char *oid32, char list[][33], int n)
{
    for (int i = 0; i < n; i++)
        if (memcmp(oid32, list[i], 32) == 0)
            return true;
    return false;
}

static int count_store_chunks_dir(const char *store_dir, bool pos0_only)
{
    char s0[PATH_MAX + 16];
    snprintf(s0, sizeof s0, "%.4000s/s0", store_dir);
    DIR *dp = opendir(s0);
    if (!dp)
        return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(dp)) != NULL) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] != 'o' || l <= 7 || strcmp(e->d_name + l - 6, ".chunk") != 0)
            continue;
        if (pos0_only) {
            const char *dot = strchr(e->d_name, '.');
            if (!dot || atoi(dot + 1) != 0)
                continue;
        }
        n++;
    }
    closedir(dp);
    return n;
}

/* Picks a store slot holding at least one live .ec object's pos-0 data chunk (or any live chunk if !pos0_only). */
static int pick_tier_store(uint64_t r, bool pos0_only)
{
    char live[128][33];
    int nlive = read_ec_oids(dbdir, live, 128);
    int start = (int)((r + (uint64_t)tier_nem_rounds) % (uint64_t)tier_n);
    for (int pass = 0; pass < 2; pass++) {
        for (int off = 0; off < tier_n; off++) {
            int s = (start + off) % tier_n;
            char s0[PATH_MAX + 16];
            snprintf(s0, sizeof s0, "%.4000s/s0", store_paths[s]);
            DIR *dp = opendir(s0);
            if (!dp)
                continue;
            bool hit = false;
            struct dirent *e;
            while (!hit && (e = readdir(dp)) != NULL) {
                size_t l = strlen(e->d_name);
                if (e->d_name[0] != 'o' || l < 39 || strcmp(e->d_name + l - 6, ".chunk") != 0)
                    continue;
                const char *dot = strchr(e->d_name, '.');
                if (!dot || dot - (e->d_name + 1) != 32)
                    continue;
                if (pos0_only && pass == 0 && atoi(dot + 1) != 0)
                    continue;
                if (nlive == 0 || oid_in_list(e->d_name + 1, live, nlive))
                    hit = true;
            }
            closedir(dp);
            if (hit)
                return s;
        }
    }
    return start;
}

static bool tier_wait_ec(int timeout_ms, bool want_no_settled_copies)
{
    int64_t end = now_ns() + (int64_t)timeout_ms * 1000000LL;
    while (now_ns() < end && !atomic_load(&stop) && !atomic_load(&nem_stop) && !atomic_load(&nem_quiet)) {
        int sc = 0;
        int ec = count_ec_files(dbdir, &sc);
        if (ec > 0 && (!want_no_settled_copies || sc == 0))
            return true;
        usleep(100000);
    }
    return false;
}

/* Queries all kv series. Sets *liveness_out = true when failure is a connect failure or client timeout (liveness)
 * rather than an explicit server error reply. */
static int tier_query_all_series(int timeout_ms, bool *liveness_out)
{
    if (liveness_out)
        *liveness_out = false;
    tsdbc *c = NULL;
    if (connect_own_ms(atomic_load(&port), atomic_load(&server_pid), &c, timeout_ms) != 0) {
        if (liveness_out)
            *liveness_out = true;
        return TSDB_ERR_IO;
    }
    tsdbc_set_timeout(c, timeout_ms);
    int rc = 0;
    for (int s = 0; s < nseries && rc == 0; s++) {
        int64_t qt0 = now_ns();
        tsdb_points p = {0};
        rc = tsdbc_query_points(c, ids[s], key_ts(s, 0), key_end(s, nkeys[s]), &p);
        free(p.ts);
        free(p.values);
        if (rc == TSDB_ERR_IO && liveness_out && now_ns() - qt0 >= (int64_t)timeout_ms * 850000LL)
            *liveness_out = true;
    }
    tsdbc_close(c);
    return rc;
}

/* store-eio: EIO on one store -> goes DOWN_IOERR, reads decode around it, server stays up, heals back to UP. */
static void tier_store_eio_tick(uint64_t r)
{
    if (!tier_wait_ec(10000, false))
        return;
    int s = pick_tier_store(r, true);
    int64_t t0 = now_ns();
    atomic_fetch_add(&nem_inprog, 1);
    if (tier_outage_ctl("eio", store_paths[s], NULL) != 0) {
        tier_harness_fail("store-eio: outagefs eio failed on store %d (%s)", s, store_paths[s]);
        atomic_fetch_sub(&nem_inprog, 1);
        return;
    }
    /* Probe/read drives store s to DOWN_IOERR. */
    char skey[64];
    snprintf(skey, sizeof skey, "tier.store.%d.state", s);
    bool went_down = tier_wait_status(skey, "DOWN_IOERR", false, 5000);
    if (!server_running()) {
        tier_nem_problems++;
        fprintf(stderr, "  store-eio: server exited after EIO on store %d (%s): must stay up\n", s, store_paths[s]);
    } else {
        if (!went_down && !atomic_load(&stop)) {
            char cur[64] = "?";
            tier_status_get(skey, cur, sizeof cur);
            tier_liveness("store-eio: store %d did not reach DOWN_IOERR within 5 s (state=%s)", s, cur);
        }
        /* Reads MUST decode around the EIO store and succeed. */
        bool q_live = false;
        int qrc = tier_query_all_series(5000, &q_live);
        if (qrc != 0 && !atomic_load(&stop)) {
            if (q_live) {
                tier_liveness("store-eio: read timed out or lost connection while store %d was in EIO", s);
            } else {
                tier_nem_problems++;
                fprintf(stderr, "  store-eio: query failed (%s) while store %d was in EIO: must decode around it\n",
                        tsdbc_strerror(qrc), s);
            }
        }
        nap(1500000, false);
    }
    tier_outage_ctl("heal", store_paths[s], NULL);
    if (server_running() && !atomic_load(&stop)) {
        if (!tier_wait_status(skey, "UP", false, 10000) && !atomic_load(&stop)) {
            char cur[64] = "?";
            tier_status_get(skey, cur, sizeof cur);
            tier_liveness("store-eio: store %d did not recover to UP after heal (state=%s)", s, cur);
        }
    }
    atomic_fetch_sub(&nem_inprog, 1);
    nem_fault_span(t0, now_ns());
    tier_nem_rounds++;
    nap(1000000, false);
}

static char salv_cur_series[64];
static int64_t salv_cur_tbase;
static int64_t anchor_tbase;
static bool stuck_cold_ready;

static bool tier_wait_ec_for_ts(int64_t ts, int timeout_ms, bool want_no_settled_copies);

/* Picks the store slot holding pos 0 (the first data chunk) of the .ec object for partition containing `ts`. */
static int pick_pos0_store_for_ts(int64_t ts, uint64_t r)
{
    int64_t pms_val = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
    int64_t want_p = -1;
    int64_t parts[64];
    int np = list_ec_partitions(dbdir, parts, 64);
    for (int i = 0; i < np; i++) {
        int64_t lo = 0, hi = 0;
        part_key_range(parts[i], pms_val, &lo, &hi);
        if (ts >= lo && ts < hi) {
            want_p = parts[i];
            break;
        }
    }
    if (want_p >= 0) {
        char pfx[64];
        snprintf(pfx, sizeof pfx, "p%lld_", (long long)want_p);
        DIR *dp = opendir(dbdir);
        struct dirent *e;
        static const char hex[] = "0123456789abcdef";
        char oid[33] = "";
        while (dp && (e = readdir(dp)) != NULL) {
            size_t l = strlen(e->d_name);
            if (strncmp(e->d_name, pfx, strlen(pfx)) != 0 || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            char fp[PATH_MAX + 256];
            if (snprintf(fp, sizeof fp, "%.4000s/%s", dbdir, e->d_name) > 0) {
                int fd = open(fp, O_RDONLY | O_CLOEXEC);
                struct stat st;
                if (fd >= 0) {
                    if (fstat(fd, &st) == 0 && st.st_size >= 128) {
                        uint8_t tr[64], ob[16];
                        if (pread(fd, tr, 64, st.st_size - 64) == 64) {
                            uint64_t rec_off = 0;
                            memcpy(&rec_off, tr + 16, 8);
                            if (rec_off + 24 <= (uint64_t)st.st_size && pread(fd, ob, 16, (off_t)(rec_off + 8)) == 16) {
                                for (int i = 0; i < 16; i++) {
                                    oid[2 * i] = hex[ob[i] >> 4];
                                    oid[2 * i + 1] = hex[ob[i] & 0xf];
                                }
                                oid[32] = 0;
                            }
                        }
                    }
                    close(fd);
                }
            }
            if (oid[0])
                break;
        }
        if (dp)
            closedir(dp);
        if (oid[0]) {
            char fname[64];
            snprintf(fname, sizeof fname, "o%s.0.chunk", oid);
            for (int s = 0; s < tier_n; s++) {
                char cp[PATH_MAX + 96];
                snprintf(cp, sizeof cp, "%.4000s/s0/%s", store_paths[s], fname);
                if (access(cp, F_OK) == 0)
                    return s;
            }
        }
    }
    return pick_tier_store(r, true);
}

/* store-stuck: hung I/O on one store -> store goes DOWN_STALLED, reads decode around it without stalling, heals UP.
 * Kills `downnoread` (reads of an unread cold series whose data chunk lives on the hung store block and miss timeout). */
static void tier_store_stuck_tick(uint64_t r)
{
    if (stuck_cold_ready && anchor_tbase != 0) {
        if (!tier_wait_ec_for_ts(anchor_tbase, 12000, false))
            return;
    } else if (!tier_wait_ec(10000, false)) {
        return;
    }
    int s = (stuck_cold_ready && anchor_tbase != 0 && tier_nem_rounds < 8)
                ? pick_pos0_store_for_ts(anchor_tbase, r)
                : pick_tier_store(r, true);
    int64_t t0 = now_ns();
    atomic_fetch_add(&nem_inprog, 1);
    if (tier_outage_ctl("hang", store_paths[s], NULL) != 0) {
        tier_harness_fail("store-stuck: outagefs hang failed on store %d (%s)", s, store_paths[s]);
        atomic_fetch_sub(&nem_inprog, 1);
        return;
    }
    char skey[64];
    snprintf(skey, sizeof skey, "tier.store.%d.state", s);
    bool stalled = tier_wait_status(skey, "DOWN_STALLED", false, 6000);
    if (!stalled && !atomic_load(&stop)) {
        char cur[64] = "?";
        tier_status_get(skey, cur, sizeof cur);
        tier_liveness("store-stuck: store %d did not reach DOWN_STALLED within 6 s (state=%s)", s, cur);
    }
    if (stalled && !atomic_load(&stop)) {
        /* Query an unread cold series whose granule lives in pos 0 on the hung store (guaranteed eccache miss!)
         * plus all kv series: MUST decode around store s promptly without stalling on its hung fd. */
        int qrc = 0;
        if (stuck_cold_ready && anchor_tbase != 0 && tier_nem_rounds < 8) {
            int64_t pms_val = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
            char cname[64];
            snprintf(cname, sizeof cname, "jepsen.cold.%d", tier_nem_rounds);
            tsdbc *c = NULL;
            if (connect_own_ms(atomic_load(&port), atomic_load(&server_pid), &c, 3000) == 0 && c) {
                tsdbc_set_timeout(c, 3000);
                tsdb_sid csid = 0;
                if (tsdbc_series(c, cname, 0, &csid) == 0) {
                    tsdb_points p = {0};
                    qrc = tsdbc_query_points(c, csid, anchor_tbase, anchor_tbase + pms_val, &p);
                    free(p.ts);
                    free(p.values);
                }
                tsdbc_close(c);
            } else {
                qrc = TSDB_ERR_IO;
            }
        }
        if (qrc == 0)
            qrc = tier_query_all_series(3000, NULL);
        if (qrc != 0 && !atomic_load(&stop)) {
            tier_nem_problems++;
            fprintf(stderr,
                    "  store-stuck: read stalled or failed (%s) while store %d (%s) was DOWN_STALLED: did not "
                    "decode around it (downnoread?)\n",
                    tsdbc_strerror(qrc), s, store_paths[s]);
        }
    }
    tier_outage_ctl("heal", store_paths[s], NULL);
    if (server_running() && !atomic_load(&stop)) {
        if (!tier_wait_status(skey, "UP", false, 10000) && !atomic_load(&stop)) {
            char cur[64] = "?";
            tier_status_get(skey, cur, sizeof cur);
            tier_liveness("store-stuck: store %d did not recover to UP after heal (state=%s)", s, cur);
        }
    }
    atomic_fetch_sub(&nem_inprog, 1);
    nem_fault_span(t0, now_ns());
    tier_nem_rounds++;
    nap(1000000, false);
}

/* Writes a cold, never-overwritten level-1 .part (~320 KiB -> k'=4, m=2 across all 6 stores) while tsdbd is stopped.
 * Runs in a short-lived forked child so unsetenv("TSDB_TEST_TIER*") never mutates environ while client threads run.
 * If n_multi > 1, writes n_multi series named "<prefix>.0" .. "<prefix>.<n_multi-1>" into the same partition so
 * each store-stuck round can query a never-before-read series in data position 0 (guaranteed eccache miss). */
static int tier_write_cold_part_multi(const char *sname, int64_t t_base, int n_multi)
{
    int64_t pms_val = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
    pid_t pid = fork();
    if (pid < 0)
        return TSDB_ERR_IO;
    if (pid == 0) {
        unsetenv("TSDB_TEST_TIER");
        unsetenv("TSDB_TEST_TIER_I_KNOW");
        tsdb_options o;
        tsdb_options_init(&o);
        o.partition_ms = pms_val;
        o.merge_threads = 0;
        tsdb *db = NULL;
        int rc = tsdb_open(dbdir, &o, &db);
        if (rc == 0 && db) {
            int ns = n_multi > 1 ? n_multi : 1;
            if (ns > 64)
                ns = 64;
            /* Single flush >= 320 KiB incompressible float32 -> ONE .part > 256 KiB -> ONE 4+2 .ec spanning all 6 stores. */
            size_t total = pms_val >= 1000000 ? 80000 : 1000;
            size_t per_s = total / (size_t)ns;
            if (per_s < 10)
                per_s = 10;
            int64_t step = pms_val >= 1000000 ? 10 : 1;
            int64_t *ts = malloc(per_s * sizeof *ts);
            float *vs = malloc(per_s * sizeof *vs);
            if (!ts || !vs) {
                rc = TSDB_ERR_NOMEM;
            } else {
                tsdb_sid sids[64];
                for (int k = 0; k < ns && rc == 0; k++) {
                    char nm[96];
                    if (n_multi > 1)
                        snprintf(nm, sizeof nm, "%s.%d", sname, k);
                    else
                        snprintf(nm, sizeof nm, "%s", sname);
                    rc = tsdb_series(db, nm, &sids[k]);
                }
                for (int k = 0; k < ns && rc == 0; k++) {
                    for (size_t i = 0; i < per_s; i++) {
                        ts[i] = t_base + (int64_t)i * step;
                        uint64_t h = mix((uint64_t)i + (uint64_t)k * 100003ull + (uint64_t)t_base + seed);
                        vs[i] = 1.0f + (float)(h & 0x7fffffu) * 1e-4f;
                    }
                    rc = tsdb_insert_series(db, sids[k], ts, vs, per_s);
                }
                if (rc == 0)
                    rc = tsdb_flush(db);
                if (rc == 0)
                    rc = tsdb_optimize(db);
            }
            free(ts);
            free(vs);
            tsdb_close(db);
        }
        _exit(rc == 0 ? 0 : 1);
    }
    int st = 0;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR)
        ;
    return WIFEXITED(st) && WEXITSTATUS(st) == 0 ? 0 : TSDB_ERR_IO;
}

static int tier_write_cold_part(const char *sname, int64_t t_base)
{
    return tier_write_cold_part_multi(sname, t_base, 1);
}

/* Scans server log for `chunk backend: store <idx> (<path>) is UP: <N> chunks rebuilt`; returns max N found, or -1. */
static long long slog_store_rebuilt_chunks(const char *store_path)
{
    FILE *f = fopen(slogpath, "r");
    if (!f)
        return -1;
    char needle[PATH_MAX + 32];
    snprintf(needle, sizeof needle, "(%.4000s) is UP: ", store_path);
    long long best = -1;
    char line[4096];
    while (fgets(line, sizeof line, f)) {
        char *p = strstr(line, needle);
        if (!p)
            continue;
        p += strlen(needle);
        char *ep = NULL;
        long long n = strtoll(p, &ep, 10);
        if (ep != p && strstr(ep, " chunks rebuilt"))
            if (n > best)
                best = n;
    }
    fclose(f);
    return best;
}

static bool part_has_settled_copy(const char *d, int64_t part)
{
    char pfx[64];
    snprintf(pfx, sizeof pfx, "settled_p%lld_", (long long)part);
    size_t pl = strlen(pfx);
    DIR *dp = opendir(d);
    if (!dp)
        return false;
    bool hit = false;
    struct dirent *e;
    while (!hit && (e = readdir(dp)) != NULL)
        hit = strncmp(e->d_name, pfx, pl) == 0;
    closedir(dp);
    return hit;
}

static bool tier_wait_ec_for_ts(int64_t ts, int timeout_ms, bool want_salvage_ready)
{
    int64_t pms_val = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
    int64_t end = now_ns() + (int64_t)timeout_ms * 1000000LL;
    while (now_ns() < end && !atomic_load(&stop) && !atomic_load(&nem_stop) && !atomic_load(&nem_quiet)) {
        int64_t parts[64];
        int np = list_ec_partitions(dbdir, parts, 64);
        for (int i = 0; i < np; i++) {
            int64_t lo = 0, hi = 0;
            part_key_range(parts[i], pms_val, &lo, &hi);
            if (ts >= lo && ts < hi) {
                if (!want_salvage_ready || !part_has_settled_copy(dbdir, parts[i]))
                    return true;
                break;
            }
        }
        usleep(100000);
    }
    return false;
}

/* store-lost: wipe one store and run `tsdb DIR tier lost STORE --replace PATH` under live ingest/query load.
 * Verifies every pre-wipe live .ec chunk that belonged to the store was actually rebuilt onto the replacement store
 * (and serves reads with 2 other stores in EIO). Kills `lostnoreplace`. */
static void tier_store_lost_tick(uint64_t r)
{
    if (anchor_tbase != 0) {
        if (!tier_wait_ec_for_ts(anchor_tbase, 12000, false))
            return;
    } else if (!tier_wait_ec(10000, false)) {
        return;
    }
    int s = pick_tier_store(r, false);
    /* Snapshot exact chunk filenames on store s belonging to currently live .ec objects BEFORE wiping. */
    char live_before[128][33];
    int nlive_before = read_ec_oids(dbdir, live_before, 128);
    char orig_chunks[128][64];
    int norig = 0;
    {
        char s0[PATH_MAX + 16];
        snprintf(s0, sizeof s0, "%.4000s/s0", store_paths[s]);
        DIR *dp = opendir(s0);
        struct dirent *e;
        while (dp && (e = readdir(dp)) != NULL && norig < 128) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'o' || l < 39 || l >= sizeof orig_chunks[0] || strcmp(e->d_name + l - 6, ".chunk") != 0)
                continue;
            if (nlive_before == 0 || oid_in_list(e->d_name + 1, live_before, nlive_before))
                snprintf(orig_chunks[norig++], sizeof orig_chunks[0], "%s", e->d_name);
        }
        if (dp)
            closedir(dp);
    }
    if (norig == 0) {
        usleep(300000);
        return;
    }
    int64_t t0 = now_ns();
    atomic_fetch_add(&nem_inprog, 1);
    rm_tree_dir(store_paths[s]);

    char newp[PATH_MAX], newarg[PATH_MAX + 16], sname[16], out[2048];
    snprintf(newp, sizeof newp, "%.4000s/s%d_r%d", tier_root, s, ++store_gen[s]);
    snprintf(newarg, sizeof newarg, "%.4000s:hdd", newp);
    snprintf(sname, sizeof sname, "s%d", s);
    char *av[] = {"tsdb", dbdir, "tier", "lost", sname, "--replace", newarg, "--allow-shared-device", NULL};
    int rc = tier_cli_run(av, out, sizeof out);
    if (rc != 0) {
        tier_harness_fail("store-lost: `tsdb tier lost %s --replace %s` failed (rc %d): %.120s", sname, newarg, rc, out);
        atomic_fetch_sub(&nem_inprog, 1);
        return;
    }
    char canon[PATH_MAX];
    snprintf(store_paths[s], sizeof store_paths[s], "%.4090s", realpath(newp, canon) ? canon : newp);
    store_replaced[s] = true;

    /* Wait for live tsdbd's tier worker to finish rebuilding (REBUILDING gone AND state == UP). */
    char rb[PATH_MAX + 32], skey[64];
    snprintf(rb, sizeof rb, "%.4000s/REBUILDING", store_paths[s]);
    snprintf(skey, sizeof skey, "tier.store.%d.state", s);
    int64_t dl = now_ns() + 30000000000LL;
    bool done = false;
    while (now_ns() < dl && !atomic_load(&stop)) {
        char stv[64] = "";
        if (access(rb, F_OK) != 0 && tier_status_get(skey, stv, sizeof stv) && strcmp(stv, "UP") == 0) {
            done = true;
            break;
        }
        usleep(100000);
    }
    if (!done && !atomic_load(&stop)) {
        tier_liveness("store-lost: live tsdbd did not finish rebuilding store %d (%s) within 30 s", s, store_paths[s]);
    } else if (done) {
        /* Every pre-wipe chunk whose OID is STILL listed in a live .ec file MUST exist on the replacement store! */
        char live_after[128][33];
        int nlive_after = read_ec_oids(dbdir, live_after, 128);
        int checked = 0, missing = 0;
        char first_miss[64] = "";
        for (int k = 0; k < norig; k++) {
            if (!oid_in_list(orig_chunks[k] + 1, live_after, nlive_after))
                continue; /* retired/re-encoded during rebuild */
            checked++;
            char cp[PATH_MAX + 96];
            snprintf(cp, sizeof cp, "%.4000s/s0/%.63s", store_paths[s], orig_chunks[k]);
            if (access(cp, F_OK) != 0) {
                /* Re-check .ec liveness on a miss: an object may have retired and been GC'd since live_after was read. */
                char live_recheck[128][33];
                int nlive_recheck = read_ec_oids(dbdir, live_recheck, 128);
                if (!oid_in_list(orig_chunks[k] + 1, live_recheck, nlive_recheck)) {
                    checked--;
                    continue;
                }
                if (!first_miss[0])
                    snprintf(first_miss, sizeof first_miss, "%.63s", orig_chunks[k]);
                missing++;
            }
        }
        if (missing > 0) {
            tier_nem_problems++;
            fprintf(stderr,
                    "  store-lost: replacement store %d (%s) promoted UP missing %d of %d live pre-wipe chunks "
                    "(e.g. %s; lostnoreplace?)\n",
                    s, store_paths[s], missing, checked, first_miss);
        } else if (!getenv("JEPSEN_VERBOSE") && checked > 0 && slog_store_rebuilt_chunks(store_paths[s]) <= 0) {
            /* Even if background tierscrub patched missing chunks after promotion, the replacement rebuild itself
             * MUST have rebuilt >= 1 chunk before promoting the store UP! */
            tier_nem_problems++;
            fprintf(stderr,
                    "  store-lost: replacement store %d (%s) promoted UP with 0 chunks rebuilt by replacement "
                    "(had %d live pre-wipe chunks; lostnoreplace?)\n",
                    s, store_paths[s], checked);
        } else if (checked > 0) {
            lost_rebuilds_verified++;
        } else if (count_store_chunks_dir(store_paths[s], false) == 0 && count_ec_files(dbdir, NULL) > 0) {
            tier_nem_problems++;
            fprintf(stderr,
                    "  store-lost: replacement store %d (%s) promoted UP with 0 chunks rebuilt (had %d before wipe; "
                    "lostnoreplace?)\n",
                    s, store_paths[s], norig);
        } else {
            lost_rebuilds_verified++;
        }
        /* If outagefs is loaded, briefly take 2 OTHER unreplaced stores into EIO so reads MUST rely on rebuilt s. */
        if (outagefs_lib && tier_n >= 6 && !atomic_load(&stop)) {
            int o1 = -1, o2 = -1;
            for (int k = 1; k < tier_n; k++) {
                int cand = (s + k) % tier_n;
                if (!store_replaced[cand]) {
                    if (o1 < 0)
                        o1 = cand;
                    else if (o2 < 0) {
                        o2 = cand;
                        break;
                    }
                }
            }
            if (o1 >= 0 && o2 >= 0) {
                tier_outage_ctl("eio", store_paths[o1], NULL);
                tier_outage_ctl("eio", store_paths[o2], NULL);
                char k1[64], k2[64];
                snprintf(k1, sizeof k1, "tier.store.%d.state", o1);
                snprintf(k2, sizeof k2, "tier.store.%d.state", o2);
                tier_wait_status(k1, "DOWN_IOERR", false, 4000);
                tier_wait_status(k2, "DOWN_IOERR", false, 4000);
                bool q_live = false;
                int qrc = tier_query_all_series(5000, &q_live);
                if (qrc != 0 && !atomic_load(&stop)) {
                    if (q_live) {
                        tier_liveness("store-lost: read timed out or lost connection with stores (%d,%d) in EIO after "
                                      "rebuilding store %d", o1, o2, s);
                    } else {
                        tier_nem_problems++;
                        fprintf(stderr,
                                "  store-lost: query failed (%s) with 2 other stores (%d,%d) in EIO after rebuilding "
                                "store %d (lostnoreplace?)\n",
                                tsdbc_strerror(qrc), o1, o2, s);
                    }
                }
                tier_outage_ctl("heal", store_paths[o1], NULL);
                tier_outage_ctl("heal", store_paths[o2], NULL);
                tier_wait_status(k1, "UP", false, 8000);
                tier_wait_status(k2, "UP", false, 8000);
            }
        }
    }
    atomic_fetch_sub(&nem_inprog, 1);
    nem_fault_span(t0, now_ns());
    tier_nem_rounds++;
    nap(1500000, false);
}

#ifdef TSDB_EC
/* Scans store_paths[0..tier_n-1]/s0 for a LIVE .ec OID present on >= need stores, returning `need` distinct store indices. */
static bool pick_stores_for_oid(int *slots, int need)
{
    char live[128][33];
    int nlive = read_ec_oids(dbdir, live, 128);
    if (nlive == 0)
        return false;
    for (int oi = 0; oi < nlive; oi++) {
        int cnt = 0;
        for (int j = 0; j < tier_n && cnt < need; j++) {
            char j0[PATH_MAX + 16];
            snprintf(j0, sizeof j0, "%.4000s/s0", store_paths[j]);
            DIR *dj = opendir(j0);
            if (!dj)
                continue;
            struct dirent *ej;
            bool hit = false;
            while (!hit && (ej = readdir(dj)) != NULL)
                hit = ej->d_name[0] == 'o' && strncmp(ej->d_name + 1, live[oi], 32) == 0;
            closedir(dj);
            if (hit)
                slots[cnt++] = j;
        }
        if (cnt >= need)
            return true;
    }
    return false;
}

static int salvage_mark_stores_lost(const int *slots, int nslots)
{
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return -1;
    int vfd = openat(dfd, "VOLUMES", O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (vfd < 0) {
        close(dfd);
        return -1;
    }
    struct flock fl = {.l_type = F_WRLCK, .l_whence = SEEK_SET};
    int lrc;
    while ((lrc = fcntl(vfd, F_OFD_SETLKW, &fl)) != 0 && errno == EINTR)
        ;
    if (lrc != 0) {
        close(vfd);
        close(dfd);
        return -1;
    }
    tiers_view tv;
    tiers_view_init(&tv);
    int rc = tiers_load(dfd, true, true, &tv);
    for (int i = 0; rc == 0 && i < nslots; i++) {
        int idx = slots[i];
        if (idx < 0 || (size_t)idx >= tv.nstores) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        if (tv.stores[idx].state == TIERS_STATE_LOST)
            continue;
        const uint8_t *id = tv.stores[idx].state == TIERS_STATE_REPLACED_BY ? tv.stores[idx].replaced_by
                                                                            : tv.stores[idx].id;
        rc = tiers_append_state(dfd, true, id, TIERS_STATE_LOST, 0, false, NULL, NULL);
    }
    tiers_view_free(&tv);
    close(vfd);
    close(dfd);
    return rc;
}
/* Picks `need` (m+1 = 3 for 6 stores) distinct store indices that knock out > m chunks of at least one live .ec
 * object (including the cold 4+2 salvage partition when present) while avoiding triples that would lose a higher-block
 * .ec part above a surviving lower-block part in the same partition (which ec_swap_salvage refuses to retire alone). */
static bool pick_salvage_stores(int *slots, int need, uint64_t r)
{
    if (tier_n == 6 && need == 3) {
        typedef struct {
            int64_t part;
            long long min_block;
            unsigned store_mask;
            bool is_ec;
        } pitem;
        pitem items[256];
        int ni = 0;
        static const char hex[] = "0123456789abcdef";
        DIR *dp = opendir(dbdir);
        struct dirent *e;
        while (dp && (e = readdir(dp)) != NULL && ni < 256) {
            size_t l = strlen(e->d_name);
            bool is_ec = e->d_name[0] == 'p' && l > 3 && strcmp(e->d_name + l - 3, ".ec") == 0;
            bool is_pt = e->d_name[0] == 'p' && l > 5 && strcmp(e->d_name + l - 5, ".part") == 0;
            if (!is_ec && !is_pt)
                continue;
            long long p = 0, lo = 0, hi = 0;
            unsigned lvl = 0;
            if (sscanf(e->d_name, "p%lld_%lld_%lld_%u", &p, &lo, &hi, &lvl) != 4)
                continue;
            unsigned mask = 0;
            if (is_ec) {
                char fp[PATH_MAX + 256], oid[33] = "";
                if (snprintf(fp, sizeof fp, "%.4000s/%s", dbdir, e->d_name) > 0) {
                    int fd = open(fp, O_RDONLY | O_CLOEXEC);
                    struct stat st;
                    if (fd >= 0) {
                        if (fstat(fd, &st) == 0 && st.st_size >= 128) {
                            uint8_t tr[64], ob[16];
                            if (pread(fd, tr, 64, st.st_size - 64) == 64) {
                                uint64_t rec_off = 0;
                                memcpy(&rec_off, tr + 16, 8);
                                if (rec_off + 24 <= (uint64_t)st.st_size &&
                                    pread(fd, ob, 16, (off_t)(rec_off + 8)) == 16) {
                                    for (int i = 0; i < 16; i++) {
                                        oid[2 * i] = hex[ob[i] >> 4];
                                        oid[2 * i + 1] = hex[ob[i] & 0xf];
                                    }
                                    oid[32] = 0;
                                }
                            }
                        }
                        close(fd);
                    }
                }
                if (oid[0]) {
                    for (int j = 0; j < tier_n; j++) {
                        char j0[PATH_MAX + 16];
                        snprintf(j0, sizeof j0, "%.4000s/s0", store_paths[j]);
                        DIR *dj = opendir(j0);
                        struct dirent *ej;
                        bool hit = false;
                        while (dj && !hit && (ej = readdir(dj)) != NULL)
                            hit = ej->d_name[0] == 'o' && strncmp(ej->d_name + 1, oid, 32) == 0;
                        if (dj)
                            closedir(dj);
                        if (hit)
                            mask |= 1u << j;
                    }
                }
            }
            items[ni++] = (pitem){.part = (int64_t)p, .min_block = lo, .store_mask = mask, .is_ec = is_ec};
        }
        if (dp)
            closedir(dp);

        unsigned triples[20];
        int nt = 0;
        for (int a = 0; a < 6; a++)
            for (int b = a + 1; b < 6; b++)
                for (int c = b + 1; c < 6; c++)
                    triples[nt++] = (1u << a) | (1u << b) | (1u << c);

        int start = (int)((r + (uint64_t)tier_nem_rounds) % 20u);
        unsigned best_t = 0;
        int best_score = -1;
        for (int ti = 0; ti < nt; ti++) {
            unsigned T = triples[(start + ti) % nt];
            int lost_ec = 0;
            bool shadow = false;
            for (int i = 0; i < ni && !shadow; i++) {
                if (!items[i].is_ec || (items[i].store_mask & T) != T)
                    continue;
                lost_ec++;
                /* Would retiring items[i] leave a surviving lower-block part in the same partition? */
                for (int k = 0; k < ni; k++) {
                    if (items[k].part == items[i].part && items[k].min_block < items[i].min_block) {
                        if (!items[k].is_ec || (items[k].store_mask & T) != T) {
                            shadow = true;
                            break;
                        }
                    }
                }
            }
            if (!shadow && lost_ec > best_score) {
                best_score = lost_ec;
                best_t = T;
            }
        }
        if (best_score > 0) {
            int cnt = 0;
            for (int j = 0; j < 6; j++)
                if (best_t & (1u << j))
                    slots[cnt++] = j;
            return cnt == 3;
        }
        return false; /* skip this tick rather than falling back to a shadowing triple */
    }
    return pick_stores_for_oid(slots, need);
}
#endif

/* salvage: wipe m+1 (3) stores holding a settled object, declare them LOST, verify E15 TSDB_ERR_IO on reads and
 * statistics answering before salvage, stop tsdbd, run `tsdb DIR tier salvage`, replace lost stores, restart tsdbd. */
static void tier_salvage_tick(uint64_t r)
{
#ifndef TSDB_EC
    (void)r;
    return;
#else
    if (nsalv_ev >= MAX_SALV_EVENTS)
        return;
    /* Wait until the cold 4+2 salvage partition has settled into .ec AND its keep_s=1 settled_ copy has aged out. */
    if (salv_cur_series[0]) {
        if (!tier_wait_ec_for_ts(salv_cur_tbase, 14000, true))
            return;
    } else if (!tier_wait_ec(14000, true)) {
        return;
    }
    int need = (tier_n >= 6 ? 2 : 1) + 1; /* m + 1 = 3 for 6 stores */
    int slots[8];
    if (!pick_salvage_stores(slots, need, r)) {
        usleep(300000);
        return;
    }

    int64_t t0 = now_ns();
    atomic_fetch_add(&nem_inprog, 1);

    /* Wipe the m+1 stores and mark them LOST in TIERS. */
    for (int i = 0; i < need; i++)
        rm_tree_dir(store_paths[slots[i]]);
    if (salvage_mark_stores_lost(slots, need) != 0) {
        tier_harness_fail("salvage: failed to mark %d stores LOST in TIERS", need);
        atomic_fetch_sub(&nem_inprog, 1);
        return;
    }
    int64_t t_lost = now_ns();

    /* Wait for live tsdbd's tierctl (1 s poll) to mark all m+1 stores non-UP/LOST. */
    for (int i = 0; i < need; i++) {
        char skey[64];
        snprintf(skey, sizeof skey, "tier.store.%d.state", slots[i]);
        tier_wait_status(skey, "UP", true, 5000);
    }

    /* Before salvage: retry up to 5 s while tierctl's 1 s poll picks up TIERS_STATE_LOST (until then reads return
     * TSDB_ERR_UNAVAILABLE because the wiped stores look DOWN_IOERR). Once LOST, reads of lost .ec partitions MUST
     * return TSDB_ERR_IO (E15) while statistics still answer from the local .ec index! */
    int64_t pms_val = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
    int64_t pre_parts[64];
    int npre = list_ec_partitions(dbdir, pre_parts, 64);
    bool got_io = false, got_stats = false, probe_conn_ok = false, probe_timed_out = false;
    {
        tsdbc *c = NULL;
        if (connect_own_ms(atomic_load(&port), atomic_load(&server_pid), &c, 3000) == 0 && c) {
            probe_conn_ok = true;
            tsdbc_set_timeout(c, 5000);
            int64_t probe_dl = now_ns() + 5000000000LL;
            tsdb_sid ssid = 0;
            bool have_cold = salv_cur_series[0] && tsdbc_series(c, salv_cur_series, 0, &ssid) == 0;
            while (!got_io && now_ns() < probe_dl && !atomic_load(&stop)) {
                if (have_cold) {
                    int64_t qt0 = now_ns();
                    tsdb_points p = {0};
                    int qrc = tsdbc_query_points(c, ssid, salv_cur_tbase, salv_cur_tbase + pms_val, &p);
                    free(p.ts);
                    free(p.values);
                    if (qrc == TSDB_ERR_IO && now_ns() - qt0 >= 4250000000LL) {
                        probe_timed_out = true;
                        break;
                    }
                    if (qrc == TSDB_ERR_IO) {
                        got_io = true;
                        salv_io_verified++;
                        tsdb_bucket *bk = NULL;
                        size_t nbk = 0;
                        if (tsdbc_query_agg(c, ssid, salv_cur_tbase, salv_cur_tbase + pms_val, pms_val, &bk, &nbk) ==
                            0) {
                            got_stats = true;
                            salv_stats_verified++;
                        }
                        free(bk);
                        break;
                    }
                }
                for (int pi = 0; pi < npre && !got_io; pi++) {
                    int64_t a = 0, b = 0;
                    part_key_range(pre_parts[pi], pms_val, &a, &b);
                    for (int s = 0; s < nseries && !got_io; s++) {
                        int64_t qt0 = now_ns();
                        tsdb_points p = {0};
                        int qrc = tsdbc_query_points(c, ids[s], a, b, &p);
                        free(p.ts);
                        free(p.values);
                        if (qrc == TSDB_ERR_IO && now_ns() - qt0 >= 4250000000LL) {
                            probe_timed_out = true;
                            break;
                        }
                        if (qrc == TSDB_ERR_IO) {
                            got_io = true;
                            salv_io_verified++;
                            /* Verify statistics query STILL answers on the lost partition! */
                            tsdb_bucket *bk = NULL;
                            size_t nbk = 0;
                            if (tsdbc_query_agg(c, ids[s], a, b, b - a, &bk, &nbk) == 0) {
                                got_stats = true;
                                salv_stats_verified++;
                            }
                            free(bk);
                        }
                    }
                }
                if (probe_timed_out)
                    break;
                if (!got_io)
                    usleep(150000);
            }
            tsdbc_close(c);
        }
    }
    if (!got_io && !atomic_load(&stop)) {
        if (!probe_conn_ok || probe_timed_out) {
            tier_liveness("salvage: pre-salvage E15 probe %s before reading lost partition",
                          !probe_conn_ok ? "could not connect" : "timed out");
        } else {
            tier_nem_problems++;
            fprintf(stderr, "  salvage: expected TSDB_ERR_IO on lost partition before salvage, got none (%d .ec parts)\n",
                    npre);
        }
    } else if (got_io && !got_stats && !atomic_load(&stop)) {
        tier_nem_problems++;
        fprintf(stderr, "  salvage: statistics query failed on lost partition before salvage\n");
    }

    /* Let clients write young rows while 3 stores are LOST (settles paused -> must survive salvage intact!). */
    nap(500000, false);

    /* Stop tsdbd cleanly and run offline `tsdb DIR tier salvage` + replace the m+1 lost stores. */
    atomic_store(&watch_excused, true);
    stop_server(SIGTERM);
    int64_t t_salv = now_ns();

    /* Snapshot every live .ec file (name, partition, settle_ms from EC_REC_SETTLE_MS=88) before salvage so
     * salvaged_away only excuses rows settled into a retired .ec part, NEVER young rows written after settle. */
    typedef struct {
        char name[96];
        int64_t part;
        int64_t settle_ms;
    } pre_ec;
    pre_ec pec[256];
    int npec = 0;
    int ec_before = 0;
    {
        DIR *dp = opendir(dbdir);
        struct dirent *e;
        while (dp && (e = readdir(dp)) != NULL) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            ec_before++;
            long long p = 0, lo = 0, hi = 0;
            unsigned lvl = 0;
            if (sscanf(e->d_name, "p%lld_%lld_%lld_%u", &p, &lo, &hi, &lvl) != 4 || npec >= 256)
                continue;
            int64_t sms = 0;
            char fp[PATH_MAX + 256];
            if (snprintf(fp, sizeof fp, "%.4000s/%s", dbdir, e->d_name) > 0) {
                int fd = open(fp, O_RDONLY | O_CLOEXEC);
                struct stat st;
                if (fd >= 0) {
                    if (fstat(fd, &st) == 0 && st.st_size >= 160) {
                        uint8_t tr[64];
                        if (pread(fd, tr, 64, st.st_size - 64) == 64) {
                            uint64_t rec_off = 0;
                            memcpy(&rec_off, tr + 16, 8);
                            if (rec_off + 96 <= (uint64_t)st.st_size &&
                                pread(fd, &sms, 8, (off_t)(rec_off + 88)) != 8) /* EC_REC_SETTLE_MS = 88 */
                                sms = 0;
                        }
                    }
                    close(fd);
                }
            }
            snprintf(pec[npec].name, sizeof pec[npec].name, "%s", e->d_name);
            pec[npec].part = (int64_t)p;
            pec[npec].settle_ms = sms;
            npec++;
        }
        if (dp)
            closedir(dp);
    }
    int64_t wall_to_mono_ns = now_ns() - wall_ms_now() * 1000000LL;

    char out[4096];
    char *sav[] = {"tsdb", dbdir, "tier", "salvage", NULL};
    int src = tier_cli_run(sav, out, sizeof out);
    if (src != 0) {
        tier_nem_problems++;
        fprintf(stderr, "  salvage: `tsdb %s tier salvage` failed (rc %d): %s\n", dbdir, src, out);
    }
    int ec_after = count_ec_files(dbdir, NULL);
    int retired_now = ec_before > ec_after ? ec_before - ec_after : 0;
    salv_parts_retired += retired_now;

    salv_event *ev = &salv_ev[nsalv_ev];
    memset(ev, 0, sizeof *ev);
    ev->lost_ns = t_lost;
    ev->salv_ns = t_salv;
    for (int i = 0; i < npec; i++) {
        char fp[PATH_MAX + 128];
        snprintf(fp, sizeof fp, "%.4000s/%.95s", dbdir, pec[i].name);
        if (access(fp, F_OK) == 0)
            continue; /* survived salvage */
        /* Convert retired .ec's wall-clock settle_ms to monotonic ns (50 ms quantization slack, clamped to <= t_lost). */
        int64_t cut_ns = pec[i].settle_ms > 0 ? pec[i].settle_ms * 1000000LL + wall_to_mono_ns + 50000000LL : t_lost;
        if (cut_ns > t_lost)
            cut_ns = t_lost;
        int idx = -1;
        for (int k = 0; k < ev->nparts; k++)
            if (ev->parts[k] == pec[i].part) {
                idx = k;
                break;
            }
        if (idx < 0 && ev->nparts < 64) {
            idx = ev->nparts++;
            ev->parts[idx] = pec[i].part;
            part_key_range(pec[i].part, pms_val, &ev->lo[idx], &ev->hi[idx]);
            ev->settle_cutoff_ns[idx] = cut_ns;
        } else if (idx >= 0 && cut_ns > ev->settle_cutoff_ns[idx]) {
            ev->settle_cutoff_ns[idx] = cut_ns;
        }
    }
    if (retired_now == 0) {
        tier_nem_problems++;
        fprintf(stderr, "  salvage: `tsdb tier salvage` retired 0 .ec parts (output: %s)\n", out);
    }

    /* Replace the m+1 lost stores so tsdbd can reopen with full redundancy and resume settles. */
    for (int i = 0; i < need; i++) {
        int slot = slots[i];
        char newp[PATH_MAX], newarg[PATH_MAX + 16], sname[16], lout[1024];
        snprintf(newp, sizeof newp, "%.4000s/s%d_r%d", tier_root, slot, ++store_gen[slot]);
        snprintf(newarg, sizeof newarg, "%.4000s:hdd", newp);
        snprintf(sname, sizeof sname, "s%d", slot);
        char *lav[] = {"tsdb", dbdir, "tier", "lost", sname, "--replace", newarg, "--allow-shared-device", NULL};
        int lrc = tier_cli_run(lav, lout, sizeof lout);
        if (lrc != 0) {
            tier_harness_fail("salvage: `tsdb tier lost %s --replace %s` failed (rc %d): %.120s", sname, newarg, lrc,
                              lout);
        } else {
            char canon[PATH_MAX];
            snprintf(store_paths[slot], sizeof store_paths[slot], "%.4090s", realpath(newp, canon) ? canon : newp);
        }
    }
    memset(store_replaced, 0, sizeof store_replaced);
    /* Prepare a fresh unread 4+2 cold partition for the next salvage round while tsdbd is stopped. */
    snprintf(salv_cur_series, sizeof salv_cur_series, "jepsen.salv.%d", tier_nem_rounds + 1);
    salv_cur_tbase = (T0 / pms_val - 6 - tier_nem_rounds) * pms_val;
    if (tier_write_cold_part(salv_cur_series, salv_cur_tbase) != 0)
        tier_harness_fail("salvage: failed to prepare next cold partition %s", salv_cur_series);
    start_server(true);
    restarts++;
    ev->done_ns = now_ns();
    nsalv_ev++;
    atomic_store(&watch_excused, false);
    atomic_fetch_sub(&nem_inprog, 1);
    nem_fault_span(t0, now_ns());
    tier_nem_rounds++;
    nap(3000000, false);
#endif
}

/* fill: drive fast root's statvfs fill hook to 92% (>= young_high_pct 90%). Inserts must fail with UNAVAILABLE
 * TSDB_WHY_FULL (definite refusals, never lost writes), reads keep working, and writes resume once lowered to 50%.
 * Kills `fullnoflag`. */
static void tier_fill_tick(uint64_t r)
{
    (void)r;
    /* Let at least one settle run before the first fill cycle so tier_check sees settles > 0. */
    if (tier_nem_rounds == 0)
        tier_wait_ec(8000, false);
    if (atomic_load(&stop) || atomic_load(&nem_stop) || atomic_load(&nem_quiet))
        return;

    /* Create/resolve the probe series BEFORE setting fill to 92% so series creation cannot fail under FULL. */
    static tsdb_sid fill_probe_sid;
    if (fill_probe_sid == 0) {
        tsdbc *c = NULL;
        if (connect_own_ms(atomic_load(&port), atomic_load(&server_pid), &c, 3000) == 0 && c) {
            tsdbc_set_timeout(c, 5000);
            (void)tsdbc_series(c, "jepsen.fillprobe", 1, &fill_probe_sid);
            tsdbc_close(c);
        }
    }

    int64_t t0 = now_ns();
    atomic_fetch_add(&nem_inprog, 1);
    if (tier_outage_ctl("fill", dbdir, "92") != 0) {
        tier_harness_fail("fill: outagefs fill 92 failed on %s", dbdir);
        atomic_fetch_sub(&nem_inprog, 1);
        return;
    }

    /* Wait up to 3 s for tierfill's 1 s sampler to latch FULL. */
    bool latched = tier_wait_status("vol.0.full", "1", false, 3000);
    long long ref0 = atomic_load(&fill_full_refusals);
    nap(2000000, false);

    /* Direct insert probe while at 92% fill: MUST be refused with TSDB_ERR_UNAVAILABLE + TSDB_WHY_FULL! */
    bool probe_full = false;
    {
        tsdbc *c = NULL;
        if (connect_own_ms(atomic_load(&port), atomic_load(&server_pid), &c, 3000) == 0 && c) {
            tsdbc_set_timeout(c, 5000);
            tsdb_sid fsid = fill_probe_sid ? fill_probe_sid : ids[0];
            int64_t fts = T0 + 1000;
            float fv = 1.0f;
            int irc = tsdbc_insert_series(c, fsid, &fts, &fv, 1);
            if (irc == TSDB_ERR_UNAVAILABLE) {
                tsdbc_err_info ei;
                if (tsdbc_last_error(c, &ei) == TSDB_ERR_UNAVAILABLE && ei.has_body) {
                    for (unsigned j = 0; j < ei.nunavail; j++)
                        if (ei.unavail[j].reason == TSDB_WHY_FULL)
                            probe_full = true;
                }
            }
            /* Reads must still succeed while the volume is FULL. */
            tsdb_points p = {0};
            int qrc = tsdbc_query_points(c, ids[0], key_ts(0, 0), key_end(0, 200), &p);
            free(p.ts);
            free(p.values);
            if (qrc != 0 && !atomic_load(&stop)) {
                tier_nem_problems++;
                fprintf(stderr, "  fill: read failed (%s) while fast root was at 92%% fill\n", tsdbc_strerror(qrc));
            }
            tsdbc_close(c);
        }
    }
    long long ref_delta = atomic_load(&fill_full_refusals) - ref0;
    if (!atomic_load(&stop) && (!probe_full || (!latched && ref_delta == 0))) {
        tier_nem_problems++;
        fprintf(stderr,
                "  fill: volume at 92%% fill (>= young_high_pct 90%%) did not refuse inserts with UNAVAILABLE FULL "
                "(vol.0.full=%d, probe_full=%d, client WHY_FULL refusals=%lld; fullnoflag?)\n",
                latched ? 1 : 0, probe_full ? 1 : 0, ref_delta);
    }

    /* Lower fill to 50% (< 88% hysteresis clear) and verify writes resume cleanly. */
    if (tier_outage_ctl("fill", dbdir, "50") != 0)
        tier_harness_fail("fill: outagefs fill 50 failed on %s", dbdir);
    int64_t t_lower = now_ns();
    if (!atomic_load(&stop)) {
        bool resumed = false;
        int64_t dl = now_ns() + 15000000000LL;
        while (now_ns() < dl && !atomic_load(&stop)) {
            if (atomic_load(&fill_last_ok_ns) > t_lower) {
                resumed = true;
                break;
            }
            usleep(50000);
        }
        if (!resumed && !atomic_load(&stop))
            tier_liveness("fill: writes did not resume within 15 s after lowering fill to 50%%");
    }
    atomic_fetch_sub(&nem_inprog, 1);
    nem_fault_span(t0, now_ns());
    tier_nem_rounds++;
    nap(2000000, false);
}

/* single-copy: 1-store (--single-copy, m=0) tier under rate/slow throttling (when outagefs is loaded) and SIGKILL restarts. */
static void tier_single_copy_tick(uint64_t r)
{
    (void)r;
    if (tier_nem_rounds == 0)
        tier_wait_ec(8000, false);
    if (atomic_load(&stop) || atomic_load(&nem_stop) || atomic_load(&nem_quiet))
        return;
    if (outagefs_lib && (tier_nem_rounds % 2 == 0)) {
        int64_t t0 = now_ns();
        atomic_fetch_add(&nem_inprog, 1);
        tier_outage_ctl("rate", store_paths[0], "20");
        tier_outage_ctl("slow", store_paths[0], "5");
        nap(2000000, false);
        tier_outage_ctl("heal", store_paths[0], NULL);
        atomic_fetch_sub(&nem_inprog, 1);
        nem_fault_span(t0, now_ns());
    } else {
        kill_and_restart(SIGKILL, outagefs_lib != NULL);
        restarts++;
    }
    tier_nem_rounds++;
    nap(1500000, false);
}

static void *nemesis_thread(void *arg)
{
    (void)arg;
    int mut_pulls = 0, mut_iter = 0;
    uint64_t r = mix(seed + 77);
    bool late_done = false;
    while ((!atomic_load(&stop) && !atomic_load(&nem_stop)) || (nem_mutant && !strcmp(nem_mutant, "lastpull") && !late_done)) {
        r = mix(r);
        const char *mm = nem_mutant ? nem_mutant : "";
        /* --mutant-nemesis lastpull: an honest nemesis whose last plug is pulled after the run's time is up and the clients are
         * gone (half a second later: no traffic to bring the next file operation the power loss waits for); the harness gives
         * the server that operation itself (poke_server) */
        bool late = !strcmp(mm, "lastpull") && atomic_load(&stop);
        if (late)
            late_done = true;
        if (!strcmp(mm, "once") && mut_iter++ >= 1) { /* one tick of work, then asleep until the end */
            while (!atomic_load(&stop))
                usleep(5000);
            break;
        }
        if (!strncmp(mm, "burst:", 6) && now_ns() - nem_t0_ns > (int64_t)atoi(mm + 6) * 1000000000LL) { /* a burst of work, then asleep */
            while (!atomic_load(&stop))
                usleep(5000);
            break;
        }
        if (!strncmp(mm, "hole:", 5)) { /* hole:FROM:TO: asleep (the server up and listening) between those seconds of the run, then at work again */
            int64_t from = (int64_t)atoi(mm + 5) * 1000000000LL, to = (int64_t)atoi(strchr(mm + 5, ':') + 1) * 1000000000LL;
            if (now_ns() - nem_t0_ns >= from && now_ns() - nem_t0_ns < to) {
                while (!atomic_load(&stop) && now_ns() - nem_t0_ns < to)
                    usleep(5000);
                continue;
            }
        }
        if (!strncmp(mm, "stopafter:", 10) && now_ns() - nem_t0_ns > (int64_t)atoi(mm + 10) * 1000000LL) { /* stopafter:MS: at work, then asleep from MS into the run */
            while (!atomic_load(&stop))
                usleep(5000);
            break;
        }
        /* blindafter:S: at work, then from S seconds into the run naps that do not look at the server (as blind:MS, 1000 ms) */
        bool blind_now = !strncmp(mm, "blindafter:", 11) && now_ns() - nem_t0_ns > (int64_t)atoi(mm + 11) * 1000000000LL;
        useconds_t mnap = !strncmp(mm, "nap:", 4) || !strncmp(mm, "blind:", 6) ? (useconds_t)atoi(strchr(mm, ':') + 1) * 1000 : blind_now ? 1000000 : 0; /* naps that do not watch */
        useconds_t slow = !strncmp(mm, "slow:", 5) ? (useconds_t)atoi(mm + 5) * 1000 : 0; /* a nemesis that works at a fraction of the rate */
        if (late)
            usleep(500000);
        else
            nap(mnap ? mnap : idle_mutant_long ? 5000000 : 150000 + (useconds_t)(r % 600000) + slow,
                (((!strcmp(nemesis, "power") && crashfs) || settle_steps || !strcmp(nemesis, "eio")) && !idle_mutant && !idle_mutant_long && !mnap) ||
                    md);
        if (!late && (atomic_load(&stop) || atomic_load(&nem_stop)))
            break;
        if (!late && atomic_load(&nem_quiet)) { /* the quiet tail: no more faults, the last server is left to the clients until stop */
            usleep(5000);
            continue;
        }
        if (!strcmp(mm, "nothing")) /* wakes, does nothing: no plug, no reaping, no restart */
            continue;
        pthread_mutex_lock(&server_lock);
        if (!strcmp(nemesis, "kill")) {
            kill_and_restart(strcmp(mm, "killnoeffect") ? SIGKILL : 0, false); /* counted once the old server died of it and a new one listens */
            restarts++;
        } else if (!strcmp(nemesis, "power")) {
            pid_t pid = atomic_load(&server_pid);
            int st;
            bool killed = false;
            int64_t step0 = now_ns(), waited = 0; /* the step's time that is not a start or a wait for the pulled server to die */
            bool dead = reap_server(pid, &st, WNOHANG) == pid; /* it died by itself (the shim's own odds): nothing to pull */
            if (!dead && (!strcmp(mm, "never") || (!strcmp(mm, "oncereact") && mut_pulls >= 1))) {
                pthread_mutex_unlock(&server_lock); /* never pulls the plug: leaves a live server alone */
                continue;
            }
            if (!dead) { /* pull the plug: SIGUSR2 is a power loss at the server's next file operation */
                mut_pulls++;
                pulls_issued++;
                int64_t issued = now_ns();
                atomic_fetch_add(&nem_inprog, 1);
                if (!strncmp(mm, "powerslowstep:", 14)) /* the pull step dawdles with the server live and the fault in progress */
                    usleep((useconds_t)atoi(mm + 14) * 1000);
                if (!strncmp(mm, "powerspinstep:", 14)) /* the same, spinning */
                    spin_ms(atoi(mm + 14));
                signal_pid(pid, SIGUSR2);
                if (!strncmp(mm, "pullspin:", 9)) /* spins after the plug, as the server dies, before it looks for the death: idle time of the step */
                    spin_ms(atoi(mm + 9));
                bool gone = false;
                int64_t w0 = now_ns();
                int wait_s = PULL_WAIT_S;
                if (!strncmp(mm, "waitspin:", 9)) /* the same, but after the wait began: the step counts it as the wait for the death, and only the dead stretch is left to see it */
                    spin_ms(atoi(mm + 9));
                /* by the clock, never by the count of naps: a poke that waits for a hung server takes its time too */
                int64_t wend = w0 + (int64_t)wait_s * 1000000000LL;
                for (int i = 0; !gone && !(gone = reap_server(pid, &st, WNOHANG) == pid); i++) {
                    if (now_ns() >= wend) {
                        if (wait_s >= PULL_EXIT_WAIT_S || !proc_exiting(pid)) /* dying, slowly, is not hung */
                            break;
                        wait_s = PULL_EXIT_WAIT_S;
                        wend = w0 + (int64_t)wait_s * 1000000000LL;
                        if (getenv("JEPSEN_FAULT_TRACE"))
                            fprintf(stderr, "jepsen: the server is still exiting %d s after SIGUSR2, waiting on\n", PULL_WAIT_S);
                    }
                    usleep(10000);
                    if (atomic_load(&stop_ns) && now_ns() - atomic_load(&stop_ns) > 1000000000LL && i % 100 == 0)
                        poke_server(pid); /* the clients are gone: no traffic brings the file operation the power loss waits for */
                }
                atomic_fetch_sub(&nem_inprog, 1);
                waited += now_ns() - w0;
                if (gone && WIFEXITED(st) && WEXITSTATUS(st) == 99 && pull_logged(pid)) /* counted when the shim says the signal reached it and the server lost power (its exit 99), stamped when it was asked */
                    nem_fault_span(issued, now_ns());
                else if (gone)
                    nem_attempt_span(issued, now_ns());
                if (!gone) { /* the clients write all the time: a server that does not die has hung (a correctness failure) */
                    /* when the shim saw the signal (its line) or the server is stopped, the server was asked and did not die: hung. A
                     * live server whose shim never saw the signal (one that ignores it) shows nothing about the server: a plug that was
                     * not pulled, which the fault floor judges (inconclusive) */
                    bool hung = pull_logged(pid) || proc_stopped(pid);
                    if (hung)
                        hang_problems++;
                    fprintf(stderr, "  the server (pid %d) was still alive %d s after SIGUSR2 (a power loss at its next file "
                            "operation): %s\n", (int)pid, wait_s, hung ? "it is hung" : "the shim never saw the signal, not a plug");
                    print_tail(slogpath);
                    print_proc_state(pid);
                    errno = 0;
                    if (reap_server(pid, &st, WNOHANG) != pid)
                        fprintf(stderr, "jepsen: waitpid(%d) once more: not reaped (errno %d)\n", (int)pid, errno);
                    signal_pid(pid, SIGKILL);
                    reap_server(pid, &st, 0);
                    killed = true;
                }
            }
            note_death(st, killed, pid);
            server_gone();
            if (!strncmp(mm, "deadsleep:", 10)) /* --mutant-nemesis: sleeps with the server dead */
                usleep((useconds_t)atoi(mm + 10) * 1000);
            if (!strncmp(mm, "deadspin:", 9)) /* spins with the server dead before the restart */
                spin_ms(atoi(mm + 9));
            if ((!strncmp(mm, "norestart:", 10) && mut_pulls > atoi(mm + 10)) || (!strncmp(mm, "norestartspin:", 14) && mut_pulls > atoi(mm + 14))) {
                /* no restart after that many pulls, to the end of the run: the last servers lie dead (asleep, or spinning) */
                bool sp = !strncmp(mm, "norestartspin:", 14);
                while (!atomic_load(&stop)) {
                    if (sp)
                        spin_ms(5);
                    else
                        usleep(5000);
                }
            }
            int64_t s0 = now_ns();
            start_server(true);
            waited += now_ns() - s0;
            restarts++;
            nem_touch();
            if (!strncmp(mm, "busysleep:", 10)) /* sleeps inside the fault step with the new server up (and maybe dead) */
                usleep((useconds_t)atoi(mm + 10) * 1000);
            nem_idle_ns += now_ns() - step0 - waited;
        } else if (settle_steps) {
            /* the server ends by itself at its armed step (exit 99 after the shim's rollback); one that has not
             * reached it in SETTLE_WAIT_S has nothing to settle or settles too slowly: killed, the next arming
             * follows (counted, and a run in which most were unreached fails in tier_check) */
            pid_t pid = atomic_load(&server_pid);
            int st = 0;
            bool gone = reap_server(pid, &st, WNOHANG) == pid, killed = false;
            /* a server with no step (the clean one after an armed server died while starting) is replaced after a
             * few seconds and not counted as one that never reached a step */
            if (!gone && now_ns() - up_ns > (int64_t)(cur_step >= 0 ? SETTLE_WAIT_S : 3) * 1000000000LL) {
                signal_pid(pid, SIGKILL);
                reap_server(pid, &st, 0);
                gone = killed = true;
                step_unreached += cur_step >= 0;
                nem_attempt_span(up_ns, now_ns()); /* it was waiting for its step: at work, though no fault */
            }
            if (!gone)
                nem_touch(); /* an armed server is the fault still to come: the nemesis is at work while it waits */
            if (gone) {
                if (!killed && cur_step >= 0 && WIFEXITED(st) && WEXITSTATUS(st) == 99 && step_reached(cur_step, pid))
                    nem_fault_span(up_ns, now_ns()); /* a confirmed cut: the shim rolled back at the server's exit */
                note_death(st, killed, pid);
                server_gone();
                start_server(true);
                restarts++;
                nem_touch();
            }
        } else if (!strcmp(nemesis, "eio")) {
            pid_t pid = atomic_load(&server_pid);
            int st = 0;
            bool gone = reap_server(pid, &st, WNOHANG) == pid, sent = false;
            int64_t mark_seen_ns = 0, fault_t0 = 0; /* fault_t0: when the fault of this step was issued (0: none); it is over when the server has ended */
            if (!gone && !server_faulty) {
                /* the clean server after a faulty one failed to start (an EIO while opening) has no shim, so no
                 * handler: SIGUSR2 would kill it, which later read as "ended (status 0xc) without a fault". Crash
                 * it as the kill nemesis does and start a faulty one. */
                signal_pid(pid, SIGKILL);
                reap_server(pid, &st, 0);
                server_gone();
                start_server(true);
                restarts++;
                nem_touch();
                pthread_mutex_unlock(&server_lock);
                continue;
            }
            if (!gone && access(markpath, F_OK) != 0 && strcmp(mm, "eionosig")) { /* the next write or sync fails */
                int64_t step0 = now_ns();
                atomic_fetch_add(&nem_inprog, 1);
                if (!strncmp(mm, "eioslowstep:", 12)) /* the eio step dawdles with the server live and the fault in progress */
                    usleep((useconds_t)atoi(mm + 12) * 1000);
                if (!strncmp(mm, "eiospin:", 8)) /* the same, spinning */
                    spin_ms(atoi(mm + 8));
                fault_t0 = now_ns();
                nem_idle_since(step0); /* what it did before its signal is idle time (the wait for the server to stop is the fault's) */
                signal_pid(pid, SIGUSR2);
                sent = true;
                for (int i = 0; i < 100 && access(markpath, F_OK) != 0; i++)
                    usleep(10000);
                atomic_fetch_sub(&nem_inprog, 1);
                if (access(markpath, F_OK) != 0 && getenv("JEPSEN_FAULT_TRACE"))
                    fprintf(stderr, "jepsen: no mark 1 s after SIGUSR2, at %.3f s\n", (double)(now_ns() - nem_t0_ns) / 1e9);
            }
            if (access(markpath, F_OK) == 0) { /* the shim failed a file (its mark: on the nemesis's request or by its own odds) */
                /* a fault of the nemesis only when it asked for it before the mark appeared: the shim fails files by its own
                 * odds as well, and a nemesis that never sends SIGUSR2 would be credited with those */
                mark_seen_ns = now_ns();
                if (!sent) {
                    eio_own++;
                    if (getenv("JEPSEN_FAULT_TRACE"))
                        fprintf(stderr, "jepsen: shim's own EIO at %.3f s, server %s\n", (double)(now_ns() - nem_t0_ns) / 1e9, gone ? "gone" : "alive");
                }
                /* the shim creates the mark and writes its line a moment later: a reader that comes between (a shim thread that waits for a
                 * CPU for tens of ms) finds an empty name, which is no part (tmp_) and so a file the server must have stopped on: an honest
                 * run failed with "after EIO on  the server is still running after 20 s". The line is whole when it ends in a newline. */
                char failed[PATH_MAX] = "";
                for (int w = 0; w < 1000; w++) {
                    failed[0] = 0;
                    FILE *m = fopen(markpath, "r");
                    bool whole = false;
                    if (m) {
                        whole = fgets(failed, sizeof failed, m) && strchr(failed, '\n');
                        fclose(m);
                    }
                    if (whole)
                        break;
                    usleep(10000);
                }
                failed[strcspn(failed, "\n")] = 0;
                const char *base = strrchr(failed, '/') ? strrchr(failed, '/') + 1 : failed;
                /* A file the engine relies on (the WAL, MANIFEST, the dictionaries, a directory) must fail-stop the server. That takes
                 * about 220 ms on a quiet machine and seconds with 16 busy loops on 4 CPUs (an honest loaded run ended on a bound of
                 * 1 s: a harness bound that load breaks), so the wait for it is EIO_STOP_WAIT_S. An unpublished part (tmp_) or an intent
                 * (txn_) fails just the call and the server stays: a second is all it is given there. */
                bool in_store = tier_n && !strncmp(failed, tier_root, strlen(tier_root)) &&
                                failed[strlen(tier_root)] == '/';
                store_eios += in_store;
                bool survives = in_store || !strncmp(base, "tmp_", 4) || !strncmp(base, "txn_", 4); /* a store's failure stops no server */
                int64_t fs0 = now_ns();
                atomic_fetch_add(&nem_inprog, 1); /* the fault is still being carried out until the server has stopped */
                int stop_wait_s = EIO_STOP_WAIT_S;
                for (int i = 0; !gone && i < (survives ? 100 : stop_wait_s * 100); i++)
                    if (!(gone = reap_server(pid, &st, WNOHANG) == pid)) {
                        usleep(10000);
                        if (!survives && i == stop_wait_s * 100 - 1 && stop_wait_s < PULL_EXIT_WAIT_S && proc_exiting(pid))
                            stop_wait_s = PULL_EXIT_WAIT_S; /* exiting slowly, not still running */
                    }
                atomic_fetch_sub(&nem_inprog, 1);
                if (gone && getenv("JEPSEN_FAULT_TRACE") && now_ns() - fs0 > 500000000LL)
                    fprintf(stderr, "jepsen: the server took %.2f s to stop after the EIO\n", (double)(now_ns() - fs0) / 1e9);
                if (gone) {
                    failstops++;
                    /* An EIO the shim's own odds injected just after we looked for the mark, and then our SIGUSR2:
                     * to the shim a signal after the EIO means a power loss at the next file operation, which may
                     * come before the fail-stop's exit (99, 0x6300). That is the fault we sent, not the server's. */
                    bool ours = sent && WIFEXITED(st) && WEXITSTATUS(st) == 99;
                    if (!ours && (!WIFEXITED(st) || WEXITSTATUS(st) != 1)) {
                        eio_problems++;
                        fprintf(stderr, "  after EIO on %s the server ended with status %#x, not exit 1\n", failed, st);
                    } else if (!ours && in_store) { /* r7-tier: the store fails, the server and its reads go on */
                        eio_problems++;
                        fprintf(stderr, "  after EIO on %s, a file of a store, the server stopped: only the store is "
                                "to fail\n", failed);
                    }
                } else {
                    /* only an unpublished part (tmp_) or an intent whose name was never synced (txn_) fails
                     * just the call; the WAL, MANIFEST, the dictionaries, a volume's META and the directory are
                     * relied upon (class b in docs/design.md): fail-stop */
                    /* a file of a store: the store fails (no more puts and deletes, no more settles in this
                     * process, r7-tier) and reads go on: the server is not to stop */
                    if (!in_store && strncmp(base, "tmp_", 4) && strncmp(base, "txn_", 4)) {
                        eio_problems++;
                        fprintf(stderr, "  after EIO on %s the server is still running after %d s\n", failed, EIO_STOP_WAIT_S);
                    }
                    atomic_fetch_add(&nem_inprog, 1); /* the plug it pulls, and the server's end, are still the fault being carried out */
                    if (strcmp(mm, "eionosig")) {
                        /* the nemesis's own act on a server the shim's EIO did not stop: a fault of it, from the mark (one it
                         * asked for began at its signal) */
                        if (!sent)
                            fault_t0 = mark_seen_ns;
                        signal_pid(pid, SIGUSR2); /* a power loss at its next file operation */
                    }
                    for (int i = 0; i < 100 && reap_server(pid, &st, WNOHANG) != pid; i++)
                        usleep(10000);
                    if (signal_pid(pid, 0) == 0) { /* idle: stopped, the shim rolls back at exit */
                        stop_pid(pid, SIGTERM, &st);
                    }
                    atomic_fetch_sub(&nem_inprog, 1);
                }
                /* the fault is over when the server has ended, not when its signal was sent: under load that is up to
                 * seconds later (a span that ended at the signal left the third it was carried out in with no fault) */
                if (fault_t0)
                    nem_fault_span(fault_t0, now_ns());
                server_gone();
                start_server(true);
                restarts++;
                nem_touch();
            } else if (gone) {
                eio_problems++;
                fprintf(stderr, "  the server ended (status %#x) without a fault\n", st);
                server_gone();
                start_server(true);
                restarts++;
                nem_touch();
            }
        } else if (outage) {
            outage_tick(r);
        } else if (t_store_eio) {
            tier_store_eio_tick(r);
        } else if (t_store_stuck) {
            tier_store_stuck_tick(r);
        } else if (t_store_lost) {
            tier_store_lost_tick(r);
        } else if (t_salvage) {
            tier_salvage_tick(r);
        } else if (t_fill) {
            tier_fill_tick(r);
        } else if (t_single_copy) {
            tier_single_copy_tick(r);
        } else if (md || rot) { /* a restart (SIGKILL or SIGTERM), over damage the shim or rot put on disk */
            pid_t pid = atomic_load(&server_pid);
            int st = 0, sig = (r >> 20) % 2 ? SIGKILL : SIGTERM;
            if (pid > 0) {
                bool gone = reap_server(pid, &st, WNOHANG) == pid;
                if (!gone) {
                    int64_t issued = now_ns();
                    if (stop_pid(pid, sig, &st) && (WIFSIGNALED(st) || WIFEXITED(st))) /* counted once the server has ended of it */
                        nem_fault_span(issued, now_ns());
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
            int pst;
            /* a server that ended by itself, before the pause or after it (round 11), is reported by the reap and replaced: the run goes on
             * to judge its history */
            if (pid > 0 && reap_server(pid, &pst, WNOHANG) == pid) {
                server_gone();
                start_server(false);
                restarts++;
                nem_touch();
                pthread_mutex_unlock(&server_lock);
                continue;
            }
            /* the fault is the server lying stopped, which the watcher sees (state T in /proc) and counts: a SIGSTOP that stops
             * nothing (--mutant-nemesis pausenoeffect: signal 0) or one to a server stopped already is no fault */
            signal_pid(pid, strcmp(mm, "pausenoeffect") ? SIGSTOP : 0);
            usleep(100000 + (useconds_t)(r % 300000));
            if (strcmp(mm, "pausenocont")) /* --mutant-nemesis pausenocont: never continues it */
                signal_pid(pid, SIGCONT);
            if (strcmp(mm, "pausenocont") && pid > 0 && reap_server(pid, &pst, WNOHANG) == pid) { /* it died as it was continued */
                server_gone();
                start_server(false);
                restarts++;
                nem_touch();
            }
        }
        pthread_mutex_unlock(&server_lock);
    }
    atomic_store(&nem_done, true);
    return NULL;
}

/* The faults per second of free listening time (a server up and listening, none of the nemesis' faults being carried out: a
 * plug that the server takes seconds to feel, an EIO it takes seconds to stop on, a restart are its work, not time it let go)
 * that the run must have, per nemesis: half of the least an honest run made, quiet or with 16 busy loops on 4 CPUs, on tmpfs
 * (/tmp, the default JEPSEN_DIR here) or on disk (honest, in 8 runs of each kind: kill 2.04 to 2.34, pause 1.79 to 2.0, power 1.14 to 1.75, eio 0.86 to 1.7; the
 * outage nemesis' fault is one long step, 0.33 to 0.4 per listening second, a third of that is asked). A nemesis at half the honest
 * rate is what the check cannot tell from an honest one under load; one at a third of it, or less, fails.
 *
 * Calibration of every bound (2026-11-13, round 8: 16 full `make jepsen` runs, all valid: JEPSEN_DIR on /tmp, tmpfs, and on /var/tmp, ext4, quiet
 * (3 each), with 16 busy loops on the 4 CPUs of the run (3 each) and with 48 (3 on /var/tmp), both backends, kv and classes, login and feed, on a
 * machine whose load average was 45 to 197 (the other lanes of the calibration, other tracks' fuzzers and tests); plus 72 honest classes/power runs
 * on epoll (48 with 16 busy loops, 24 with 48) and 6 eio and 6 power runs of 20 s on /tmp with 16 busy loops, all valid; 4 more runs of `make jepsen`
 * were lost to the machine (tsdbd: io_uring: Cannot allocate memory, 3) and to a race in reading the shim's mark (1, fixed: see the eio step));
 * before them 12 eio, 6 kill and 6 power runs with 48 busy loops, 12 full runs and 300-second runs with a load average of 90), the honest worst
 * against the bound, margin in brackets:
 *   rate per free listening second, least:    kill 1.92 (floor 1.0, 1.9x)  pause 1.82 (0.9, 2.0x)  power 1.21 (0.55, 2.2x)  eio 0.85 (0.4, 2.1x)
 *   faults against the floor, least:          kill 1.86x  pause 1.92x  power 2.0x  eio 2.0x
 *   longest listening stretch with no fault:  0.80 s (pause), 0.77 s (power, eio), 0.76 s (kill) (bound 2.0 s for runs of 10 and 20 s, 2.5x)
 *   longest dead stretch (round 8, with a CPU wait taken off that round 9 no longer takes): 199 ms, in a kill run on /var/tmp with 16 busy loops at a load average of
 *     150 (bound 250 ms, 1.26x: the thinnest margin, and a server that waits for a CPU in the fork is not the nemesis' wait to excuse); 149 ms
 *     (power), 99 ms (eio) otherwise; raw, without the CPU wait taken off, the blocker run of round 7 had 453 ms
 *   longest stop: 413 ms (bound 1500 ms, 3.6x)
 *   longest listening stretch in a kill step: 491 ms with 16 busy loops, 1002 ms with 48 (it decides only when the time counts as free, 400 ms)
 *   idle time in the fault steps (the time before the signal in a kill, power or eio step): 0.00 s in every honest run (bound 5% of the run)
 * A nemesis that works at a third of the honest rate, at half of it (slow:MS), stops at three quarters of the run (hole:15:100) or
 * sees no server die in its naps (blind:MS) is failed by the selftest of the Makefile, 10 of 10 runs in each of the four conditions
 * (/tmp and /var/tmp, quiet and with 16 busy loops; round 8 on the code of that round, with the cases added in it: the kill, power
 * and eio steps that sleep 400 ms with the server live, the slow mark of the shim, a reader of a mapping the power loss cuts).
 * What passes, on purpose, and is not a failure of the check: (1) a nemesis at half the honest rate (slow:MS: the floor is half the honest
 * least, so such a run is at the floor and about one attempt in four passes (fails to fail): eio slow:1600 over 60 s passed 1 of 4 runs under load; at a
 * third of the rate, 0 of 16. A gate has three attempts (rerun.sh), so such a nemesis passes a gate far more often: 1-(3/4)^3, about 58%,
 * at half the rate; the selftest and the bounds therefore rest on the nemeses at a third of the rate and below); (2) a nemesis that stops working at nine tenths of a run (the gap bound is a tenth of the run, 2 s of 20), or at
 * four fifths of one of 10 s (the pause runs of make jepsen are 20 s for that reason); (3) any run whose fault count passes only because
 * restarts and pulls the nemesis waits for count as work, so a nemesis that always waits as long as the honest one under load can do is
 * not told from it; (4) a nemesis that does not look at the server in its naps (blind:MS, blindafter:S of the verifier) when the shim's own
 * odds are what must kill a server in one: the eio shim fails a file that kills the server in 4 to 8 naps of 20 s, the dead stretch it
 * leaves is what remains of the nap (uniform to 750 ms), and about half of the runs leave none above 250 ms (6 of 14 runs failed to fail;
 * a power nemesis, 100%: its plugs and the shim's odds kill 17 servers in 20 s), hence the selftest case with raised odds. A step of the
 * kill, power or eio nemesis that sleeps with the server live before its signal is NOT such a pass: that time is idle time of the step
 * (nem_idle_since; an honest step has none, 0.00 s in every run, and a sleep is no CPU wait), whose bound is 5% of the run, so a step that
 * dawdles 40 ms on average fails (killslowstep, powerslowstep and eioslowstep 200 to 3000 ms: 78 runs, none passed, quiet and with 16 busy loops).
 * A bound is never widened to make a mutant fail: honest runs come first.
 * Round 9 (what these checks are for). Two kinds of failure: a SAFETY anomaly (lost, resurrected or wrong data, a server that hangs or ends
 * any way but a power loss) is a VIOLATION, exit 1, never repeated. A failed LIVENESS or COVERAGE check of the test itself (every check below,
 * the classes workload's "0 switches" and the latest coverage) is INCONCLUSIVE, exit 3: the run tested too little, and says nothing about the
 * data. tests/jepsen/rerun.sh (make jepsen, the honest cases of the selftest) repeats it with the next seed, at most twice more; three in a
 * row fail the gate with all three messages, and every repeat is printed and counted. Rounds 7 and 8 each tightened or excused one timing
 * bound and each time an honest run was broken by a machine stall (a thread blocked 650 ms in reclaim, which no run-delay account sees; the
 * classes clients starved to 12 writes in 20 s), while the excuse (the kernel's run delay of the thread) let a nemesis that SPINS where it
 * should sleep pass under load. No account of the kernel's tells a spinner from a starved thread, so none is taken: no bound has an excuse
 * and a stall fails one attempt, never the gate. The spin mutants (killspin, powerspinstep, eiospin, pullspin, waitspin, deadspin,
 * norestartspin) and the sleeping ones fail all three attempts, quiet and loaded: the selftest requires that outcome, not just a non-zero
 * exit (expect.sh gate PATTERN: three INCONCLUSIVE and the named check). lastpull is an honest nemesis and must be valid. A server that
 * the shim never sent the signal to (CRASHFS_IGNORE_USR2) lives on: that is a plug not pulled (the floor), not a hung server.
 * Measured and caught, so not a pass: a blind power nemesis (naps that never look at the server, blind:1000) at the shim's default odds,
 * 16 of 16 runs (8 quiet, 8 with 16 busy loops on /tmp), by the dead stretch of 1 to 3 s its naps leave and by the floor; the odds of the
 * Makefile case are raised only to make the shim kill servers at all in 20 s. */
static double nem_rate(void)
{
    if (outage)
        return 0.11;
    if (tier_nem) /* multi-second store faults, rebuilds, salvages, fill cycles: sustained cadence across the run */
        return t_salvage ? 0.05 : 0.12;
    if (settle_steps) /* a server settles when it settles: tier_check judges the steps reached, the floor is one cut */
        return 0;
    if (!strcmp(nemesis, "kill"))
        return 1.0;
    if (!strcmp(nemesis, "pause"))
        return 0.9;
    if (!strcmp(nemesis, "power"))
        return 0.55;
    if (!strcmp(nemesis, "eio"))
        return 0.4;
    return 0.3;
}

static int cmp_size(const void *a, const void *b)
{
    size_t x = *(const size_t *)a, y = *(const size_t *)b;
    return (x > y) - (x < y);
}

/* The longest a server may be listening with no fault issued or in progress and no restart by the nemesis (honest: 0.65 to 0.85 s
 * for kill, pause and power, up to 1.25 s for eio, which restart servers that died of the shim's own odds): a tenth of the
 * run, at least 2 s and at most 5 s. The last stretch, up to the end of the run, counts: a nemesis that stops working at three
 * quarters of a 20 s run leaves 5 s of it, and one that stops at 90% is the limit of this check. */
static double nem_gap_max_s(void)
{
    double g = seconds * NEM_GAP_SHARE;
    return g < NEM_GAP_MIN_S ? NEM_GAP_MIN_S : g > NEM_GAP_MAX_S ? NEM_GAP_MAX_S : g;
}

/* Misdirect and rot: the epochs end early on damage, only the whole run's sum is judged, one fault per this many seconds. */
static int nem_every(void) { return 10; }

/* The run's verdict on the nemesis itself, from what it counted (nem_fault, each with its time) and what the watcher saw of
 * the servers, never from how the nemesis was seen to behave: a nemesis that did few faults for the time servers were up,
 * did them all in a part of the run, sleeps inside its fault step or leaves a server dead is idle, and the run tested far
 * less than it says. Returns the failures. */
static int nemesis_check(void)
{
    int bad = 0;
    double live = (double)watch_live_ns / 1e9; /* the time a server was up and listening: what the nemesis had to work on */
    int want = 0;
    if (seconds >= 10) {
        double w = (double)watch_free_ns / 1e9 * nem_rate();
        want = (int)w + (w > (int)w); /* rounded up */
        int least = outage ? NEM_MIN_FAULTS : 1; /* servers that take seconds to start leave little time to work in (slow-tsdbd.sh) */
        if (want < least)
            want = least;
    }
    int third[3] = {0, 0, 0};
    bool covered[3] = {false, false, false}; /* a fault issued in the third, or one still being carried out in it */
    for (int i = 0; i < nem_faults && i < MAX_FAULT_TIMES; i++)
        for (int j = 0; j < 3; j++) {
            third[j] += nem_fault_ns[i] >= nem_third_start(j) && nem_fault_ns[i] < nem_third_start(j + 1);
            if (nem_fault_ns[i] < nem_third_start(j + 1) && nem_fault_end_ns[i] >= nem_third_start(j))
                covered[j] = true;
        }
    for (int j = 0; j < 3; j++)
        covered[j] |= nem_att_third[j];
    /* a third in which a server was listening for less than half of it (servers that take seconds to start: the nemesis waits
     * for them) is not judged for its spread; the rest of the run's floor and the watcher's dead time still are */
    bool judged[3];
    for (int j = 0; j < 3; j++)
        judged[j] = watch_live3_ns[j] * 2 >= nem_third_start(j + 1) - nem_third_start(j);
    printf("nemesis %s: %d faults in %.0f s (%.1f s with a server listening, %.1f s of it with no fault being carried out; at least %d expected; by third of the run %d/%d/%d; "
           "%d plugs asked for), %d deaths and %d restarts seen, servers dead %.1f s in all, longest %.0f ms, %.2f s idle in its fault steps, "
           "longest listening stretch without a fault %.2f s, %.2f s at the end, longest stop %.0f ms, longest listening stretch in a fault step %.0f ms, %d marks of the shim's own odds not counted\n",
           nemesis, nem_faults, seconds, live, (double)watch_free_ns / 1e9, want, third[0], third[1], third[2], pulls_issued, watch_deaths, watch_restarts,
           (double)watch_dead_total_ns / 1e9, (double)watch_dead_worst_ns / 1e6, (double)nem_idle_ns / 1e9,
           (double)watch_gap_worst_ns / 1e9, (double)watch_tail_ns / 1e9, (double)watch_stop_worst_ns / 1e6, (double)watch_step_worst_ns / 1e6, eio_own);
    if (nem_faults < want) {
        INCONCLUSIVE("fault-floor", "the nemesis issued %d faults in %.0f s, %.1f s of it with a server listening (at least %d expected)%s: it was idle\n",
                     nem_faults, seconds, live, want, !strcmp(nemesis, "power") && pulls_issued > nem_faults ?
                     " (plugs it asked for that the shim did not log are not faults)" : "");
        bad++;
    }
    /* the faults must be spread over the run, not a burst at its start: one in each third (an outage fault is a step of many
     * seconds, a flapping domain's heal waits for a minute: there only the number counts) */
    if (seconds >= 10 && !outage && !t_salvage && !(nem_faults < want) && ((judged[0] && !covered[0]) || (judged[1] && !covered[1]) || (judged[2] && !covered[2]))) {
        INCONCLUSIVE("fault-spread", "the nemesis' faults (%d/%d/%d in the three thirds of the run) were not spread over it: it was idle for a part of it\n",
                     third[0], third[1], third[2]);
        bad++;
    }
    /* a nemesis that stops part of the way through leaves one fault in each third and a count above the floor, and the
     * stretch it leaves is a long one: the server listening and nothing issued or in progress (an outage fault is a step of
     * many seconds: there only the number counts) */
    double gap_max_s = nem_gap_max_s();
    if (seconds >= 10 && !outage && !tier_nem && (double)watch_gap_worst_ns / 1e9 > gap_max_s) {
        INCONCLUSIVE("gap", "a server was listening for %.1f s with no fault issued or in progress (at most %.1f s expected; %.1f s of it at "
                     "the end of the run): the nemesis was idle\n", (double)watch_gap_worst_ns / 1e9, gap_max_s, (double)watch_tail_ns / 1e9);
        bad++;
    }
    if (watch_stop_worst_ns > NEM_STOP_MAX_MS * 1000000LL) {
        INCONCLUSIVE("stop", "a server lay stopped (SIGSTOP) for %.0f ms (at most %d ms expected): the nemesis was idle or never "
                     "continued it\n", (double)watch_stop_worst_ns / 1e6, NEM_STOP_MAX_MS);
        bad++;
    }
    if (watch_dead_worst_ns > NEM_DEAD_MAX_MS * 1000000LL) {
        INCONCLUSIVE("dead-stretch", "a server lay dead for %.0f ms (at most %d ms expected, the watcher polls every %d ms): the nemesis was idle\n",
                     (double)watch_dead_worst_ns / 1e6, NEM_DEAD_MAX_MS, WATCH_POLL_MS);
        bad++;
    }
    if (seconds >= 10 && (double)nem_idle_ns > NEM_IDLE_SHARE * seconds * 1e9) {
        INCONCLUSIVE("idle-step", "the fault step took %.1f s of %.0f s besides starting servers and waiting for a pulled one to die (at "
                     "most %.0f%% expected): the nemesis was idle in it\n", (double)nem_idle_ns / 1e9, seconds, NEM_IDLE_SHARE * 100);
        bad++;
    }
    return bad;
}

/* The end of an INCONCLUSIVE run (exit 3, see inc_note): the checks that failed, and whether the history was judged. */
#define EXIT_INCONCLUSIVE 3
static int inconclusive_exit(bool history_checked)
{
    printf("INCONCLUSIVE (%d failed liveness or coverage checks: %s): the run tested less than it says; %s\n", inconclusive,
           inc_checks, history_checked ? "its history shows no violation" : "it ended before its history was judged");
    return EXIT_INCONCLUSIVE;
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
    if (feedcheck_dir(dbdir, crashed, why, sizeof why) != 0)
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
            execl(cli, "tsdb", dbdir, "feed", "enable", "--retention", "5s", "--max-bytes", "3M", (char *)NULL);
        else
            execl(cli, "tsdb", dbdir, "feed", "info", (char *)NULL);
        _exit(127);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "jepsen: %s %s feed %s failed\n", cli, dbdir, enable ? "enable" : "info");
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

/* Whether write w wrote key k. The keys of a write are a stride through the series (in no order): a write of many keys is
 * searched in a sorted copy made the first time it is asked (the checker is one thread), so a final state of thousands of
 * keys against a replace of thousands is not thousands of times thousands. */
static uint32_t **skeys; /* per write id, NULL until sorted; only for writes of more than WROTE_LINEAR keys */
#define WROTE_LINEAR 16
static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return x < y ? -1 : x > y;
}
static bool wrote_key(uint32_t w, uint32_t k)
{
    const wop *o = &writes[w];
    if (o->nkeys <= WROTE_LINEAR || !skeys) {
        for (uint32_t i = 0; i < o->nkeys; i++)
            if (o->keys[i] == k)
                return true;
        return false;
    }
    if (!skeys[w]) {
        skeys[w] = malloc(o->nkeys * sizeof **skeys);
        if (!skeys[w]) { /* no memory for the copy: the linear search is slow, not wrong */
            for (uint32_t i = 0; i < o->nkeys; i++)
                if (o->keys[i] == k)
                    return true;
            return false;
        }
        memcpy(skeys[w], o->keys, o->nkeys * sizeof **skeys);
        qsort(skeys[w], o->nkeys, sizeof **skeys, cmp_u32);
    }
    return bsearch(&k, skeys[w], o->nkeys, sizeof **skeys, cmp_u32) != NULL;
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
/* The writes that can remove a row at a key, indexed by key for latest_removes (built by check from dlist): per slot, the
 * ones that did not definitely fail, sorted by invoke, with the highest complete time of the entries up to each. [0]: deletes
 * and replaces that did not write the key (one that did leaves a row there as new as itself: no remover), [1]: null writes. */
static uint32_t *rcnt[2];  /* offsets per slot (koff[series] + key) */
static int64_t *rinv[2], *rpmax[2];
typedef struct rent { int64_t inv, comp; } rent;
static int cmp_rent(const void *a, const void *b)
{
    int64_t x = ((const rent *)a)->inv, y = ((const rent *)b)->inv;
    return x < y ? -1 : x > y;
}

static void build_removers(void)
{
    size_t nslots = koff[nseries];
    for (int kind = 0; kind < 2; kind++) {
        rcnt[kind] = calloc(nslots + 1, sizeof **rcnt);
        size_t total = 0;
        for (int pass = 0; pass < 2; pass++) {
            size_t cap = 0;
            rent *tmp = NULL;
            for (int s = 0; s < nseries; s++)
                for (uint32_t k = 0; k < nkeys[s]; k++) {
                    size_t slot = koff[s] + k, n = 0;
                    for (uint32_t i = dcnt[slot]; i < dcnt[slot + 1]; i++) {
                        const wop *d = &writes[dlist[i]];
                        if (d->st == ST_FAIL || (kind == 1) != (d->kind == W_NULL) ||
                            (d->kind == W_REPLACE && wrote_key(dlist[i], k)))
                            continue;
                        if (pass == 1) {
                            if (n == cap)
                                tmp = realloc(tmp, (cap = cap ? 2 * cap : 16) * sizeof *tmp);
                            tmp[n] = (rent){d->invoke, d->complete};
                        }
                        n++;
                    }
                    if (pass == 0) {
                        rcnt[kind][slot + 1] = (uint32_t)n;
                        continue;
                    }
                    qsort(tmp, n, sizeof *tmp, cmp_rent);
                    int64_t m = INT64_MIN;
                    for (size_t x = 0; x < n; x++) {
                        m = tmp[x].comp > m ? tmp[x].comp : m;
                        rinv[kind][rcnt[kind][slot] + x] = tmp[x].inv;
                        rpmax[kind][rcnt[kind][slot] + x] = m;
                    }
                }
            free(tmp);
            if (pass == 0) {
                for (size_t x = 1; x <= nslots; x++)
                    rcnt[kind][x] += rcnt[kind][x - 1];
                total = rcnt[kind][nslots];
                rinv[kind] = malloc((total + 1) * sizeof **rinv);
                rpmax[kind] = malloc((total + 1) * sizeof **rpmax);
            }
        }
    }
}

static void free_removers(void)
{
    for (int kind = 0; kind < 2; kind++) {
        free(rcnt[kind]);
        free(rinv[kind]);
        free(rpmax[kind]);
        rcnt[kind] = NULL, rinv[kind] = rpmax[kind] = NULL;
    }
}

static bool latest_removes(int series, uint32_t k, int64_t after, int64_t before, bool nulls_remove)
{
    size_t slot = koff[series] + k;
    for (int kind = 0; kind < (nulls_remove ? 2 : 1); kind++) {
        const int64_t *inv = rinv[kind] + rcnt[kind][slot];
        size_t lo = 0, hi = rcnt[kind][slot + 1] - rcnt[kind][slot]; /* the entries invoked at or before `before` */
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            if (inv[mid] <= before)
                lo = mid + 1;
            else
                hi = mid;
        }
        if (lo && rpmax[kind][rcnt[kind][slot] + lo - 1] >= after)
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
static bool *cuttable;
static bool *may_lose;
static int64_t *cut_at;

static int cmp_cut(const void *a, const void *b)
{
    int64_t x = ((const power_cut *)a)->dead_ns, y = ((const power_cut *)b)->dead_ns;
    return (x > y) - (x < y);
}

static void load_cuts(power_cut **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    struct file_cut {
        pid_t pid;
        int64_t cut_ns;
        bool used;
    } *fc = NULL;
    size_t nfc = 0, fcap = 0;
    FILE *f = fopen(cutpath, "r");
    if (f) {
        long long t;
        int pid;
        while (fscanf(f, "%d %lld", &pid, &t) == 2) {
            if (nfc == fcap) {
                size_t ncap = fcap ? fcap * 2 : 64;
                struct file_cut *na = realloc(fc, ncap * sizeof *na);
                if (!na) {
                    free(fc);
                    fclose(f);
                    fprintf(stderr, "jepsen: out of memory reading %s\n", cutpath);
                    bail(2);
                }
                fc = na;
                fcap = ncap;
            }
            fc[nfc++] = (struct file_cut){.pid = (pid_t)pid, .cut_ns = (int64_t)t, .used = false};
        }
        fclose(f);
    }
    pthread_mutex_lock(&cut_mu);
    size_t n = nreaped_cuts;
    power_cut *a = n ? malloc(n * sizeof *a) : NULL;
    if (n && !a) {
        pthread_mutex_unlock(&cut_mu);
        free(fc);
        fprintf(stderr, "jepsen: out of memory copying power cuts\n");
        bail(2);
    }
    if (n)
        memcpy(a, reaped_cuts, n * sizeof *a);
    pthread_mutex_unlock(&cut_mu);
    if (n > 1)
        qsort(a, n, sizeof *a, cmp_cut);
    for (size_t i = 0; i < n; i++) {
        int64_t prev_dead = i > 0 ? a[i - 1].dead_ns : INT64_MIN;
        for (size_t j = 0; j < nfc; j++) {
            if (!fc[j].used && fc[j].pid == a[i].pid && fc[j].cut_ns > prev_dead && fc[j].cut_ns <= a[i].dead_ns) {
                a[i].cut_ns = fc[j].cut_ns;
                fc[j].used = true;
                break;
            }
        }
    }
    free(fc);
    *out = a;
    *nout = n;
}

/* True if write starting at `w_invoke` to key `k` of series `s` may have been settled into a .ec part retired by a
 * `salvage` event at or before `at_ns` (INT64_MAX for final state). Bounded by each retired .ec part's settle_ms
 * (EC_REC_SETTLE_MS, mapped to monotonic settle_cutoff_ns): writes invoked at/before settle_ms in that partition are
 * excused (note: a row still in the memtable at settle_ms shares that window), while all writes invoked after settle_ms
 * — including young rows written between settle and wipe and all writes while stores are LOST — must survive salvage. */
static bool salvaged_away(int s, uint32_t k, int64_t w_invoke, int64_t at_ns)
{
    if (!nsalv_ev)
        return false;
    int64_t ts = key_ts(s, k);
    for (int e = 0; e < nsalv_ev; e++) {
        if (at_ns >= salv_ev[e].salv_ns) {
            for (int p = 0; p < salv_ev[e].nparts; p++)
                if (ts >= salv_ev[e].lo[p] && ts < salv_ev[e].hi[p] && w_invoke <= salv_ev[e].settle_cutoff_ns[p])
                    return true;
        }
    }
    return false;
}

static bool salvage_window_overlaps(int64_t invoke, int64_t complete)
{
    for (int e = 0; e < nsalv_ev; e++)
        if (complete >= salv_ev[e].lost_ns && invoke <= salv_ev[e].done_ns)
            return true;
    return false;
}

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
            if (t_salvage && l->rstatus == TSDB_ERR_IO && salvage_window_overlaps(l->invoke, l->complete))
                goto next; /* expected E15 TSDB_ERR_IO while m+1 stores were LOST before salvage */
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
                    if (cov[j] != v && w->st == ST_OK && w->complete < l->invoke && older(v, cov[j]) &&
                        !(may_lose && may_lose[cov[j]] && l->complete >= cut_at[cov[j]])) {
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
                    if (w->st != ST_OK || w->complete >= l->invoke ||
                        (may_lose && may_lose[cov[j]] && l->complete >= cut_at[cov[j]]))
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
            /* cov is sorted ascending by invoke; latest_removes and salvaged_away are monotone in w->invoke, so walking
             * backwards from the newest write with invoke < l->invoke checks the strictest bound first and breaks early. */
            size_t lo = 0, hi = n;
            while (lo < hi) {
                size_t mid = (lo + hi) / 2;
                if (writes[cov[mid]].invoke < l->invoke)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            for (size_t j = lo; j-- > 0;) {
                const wop *w = &writes[cov[j]];
                int row = row_of(w, (uint32_t)k, true);
                if (w->st != ST_OK || w->complete >= l->invoke || row == 0 || (row == 2 && !l->with_nulls) ||
                    (may_lose && may_lose[cov[j]] && l->complete >= cut_at[cov[j]]))
                    continue;
                if (latest_removes(s, (uint32_t)k, w->invoke, l->complete, !l->with_nulls))
                    break;
                if (salvaged_away(s, (uint32_t)k, w->invoke, l->complete))
                    break;
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

/* JEPSEN_KEEP: the history of a failed run as text, DIR.hist. Times in ms since the clients started.
 *   K series key ts            every key's timestamp
 *   W id series kind lo hi status invoke complete key...
 *   R id series lo hi status invoke complete (key=write)...   only keys with a value
 *   F series key write          the final state, nonzero only */
static void dump_history(const uint32_t *final)
{
    char p[PATH_MAX + 8];
    snprintf(p, sizeof p, "%s.hist", dir);
    FILE *f = fopen(p, "w");
    if (!f)
        return;
    uint32_t nw = atomic_load(&nwrites), nr = atomic_load(&nreads);
    nw = nw > max_ops ? max_ops : nw, nr = nr > max_ops ? max_ops : nr;
    for (int s = 0; s < nseries; s++)
        for (uint32_t k = 0; k < nkeys[s]; k++)
            fprintf(f, "K %d %u %lld\n", s, k, (long long)kts[s][k]);
    for (uint32_t w = 1; w < nw; w++) {
        wop *o = &writes[w];
        fprintf(f, "W %u %d %d %u %u %d %.3f %.3f", w, o->series, o->kind, o->lo, o->hi, (int)o->st,
                (double)(o->invoke - run_start) / 1e6, (double)(o->complete - run_start) / 1e6);
        for (uint32_t i = 0; i < o->nkeys; i++)
            fprintf(f, " %u", o->keys[i]);
        fputc('\n', f);
    }
    for (uint32_t i = 0; i < nr; i++) {
        rop *q = &reads[i];
        fprintf(f, "R %u %d %u %u %d %.3f %.3f", i, q->series, q->lo, q->hi, (int)q->st,
                (double)(q->invoke - run_start) / 1e6, (double)(q->complete - run_start) / 1e6);
        for (uint32_t k = q->lo; q->st == ST_OK && q->seen && k < q->hi; k++)
            if (q->seen[k - q->lo])
                fprintf(f, " %u=%u", k, q->seen[k - q->lo]);
        fputc('\n', f);
    }
    for (int s = 0; final && s < nseries; s++)
        for (uint32_t k = 0; k < nkeys[s]; k++)
            if (final[koff[s] + k])
                fprintf(f, "F %d %u %u\n", s, k, final[koff[s] + k]);
    fclose(f);
}

static int check(const uint32_t *final)
{
    uint32_t nw = atomic_load(&nwrites), nr = atomic_load(&nreads);
    if (nw > max_ops)
        nw = max_ops;
    if (nr > max_ops)
        nr = max_ops;
    bool *applied = calloc(nw, sizeof *applied);
    bool *in_final = calloc(nw, sizeof *in_final);
    bool *must_survive = calloc(nw, sizeof *must_survive);
    bool *seen_after_cut = calloc(nw, sizeof *seen_after_cut);
    int64_t *eff_time = malloc(nw * sizeof *eff_time);
    cuttable = calloc(nw, sizeof *cuttable);
    may_lose = calloc(nw, sizeof *may_lose);
    cut_at = malloc(nw * sizeof *cut_at);
    int64_t *dead_at = malloc(nw * sizeof *dead_at);
    for (uint32_t w = 0; w < nw; w++) {
        eff_time[w] = writes[w].st == ST_OK ? writes[w].complete : INT64_MAX;
        cut_at[w] = INT64_MAX;
        dead_at[w] = INT64_MAX;
    }
    skeys = calloc(nw, sizeof *skeys);
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
            in_final[w] = true;
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
            if (q->complete < eff_time[w])
                eff_time[w] = q->complete;
        }
    }
    power_cut *cuts = NULL;
    size_t ncuts = 0;
    int slack_ms = 0;
    uint64_t durable_before_cut = 0, window_acked = 0;
    if (wal_sync_ms > 0) {
        load_cuts(&cuts, &ncuts);
        slack_ms = wal_sync_ms / 2;
        if (slack_ms < 50)
            slack_ms = 50;
        if (slack_ms > 250)
            slack_ms = 250;
        int64_t win_ns = (int64_t)(wal_sync_ms + slack_ms) * 1000000LL;
        for (uint32_t w = 1; w < nw; w++) {
            if (eff_time[w] == INT64_MAX || ncuts == 0)
                continue;
            size_t lo = 0, hi = ncuts;
            while (lo < hi) {
                size_t mid = (lo + hi) / 2;
                if (cuts[mid].dead_ns < writes[w].invoke)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            if (lo < ncuts) {
                /* Only WAL-buffered ops (small inserts and nulls below WAL_MAX_BATCH_ROWS) may be lost in a power cut;
                 * deletes, replaces, and large inserts write and fsync parts directly before acknowledging. */
                bool wal_buffered = (writes[w].kind == W_INSERT && writes[w].nkeys < WAL_MAX_BATCH_ROWS) ||
                                    (writes[w].kind == W_NULL && (writes[w].hi - writes[w].lo) < WAL_MAX_BATCH_ROWS);
                if (wal_buffered && cuts[lo].cut_ns - eff_time[w] <= win_ns) {
                    cuttable[w] = true;
                    cut_at[w] = cuts[lo].cut_ns;
                    dead_at[w] = cuts[lo].dead_ns;
                    if (writes[w].st == ST_OK)
                        window_acked++;
                } else if (writes[w].st == ST_OK) {
                    durable_before_cut++;
                }
            }
        }
        for (uint32_t i = 0; i < nr; i++) {
            rop *q = &reads[i];
            if (q->st != ST_OK)
                continue;
            for (uint32_t k = q->lo; k < q->hi; k++) {
                uint32_t w = q->seen[k - q->lo];
                if (w && w < nw && cuttable[w] && q->invoke > dead_at[w])
                    seen_after_cut[w] = true;
            }
        }
    }
    for (uint32_t w = 1; w < nw; w++) {
        may_lose[w] = wal_sync_ms > 0 && cuttable[w] && !in_final[w] && !seen_after_cut[w];
        must_survive[w] = wal_sync_ms == 0 ? applied[w]
                                           : (in_final[w] || seen_after_cut[w] || (applied[w] && !cuttable[w]));
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
        if (!must_survive[w])
            continue;
        for (uint32_t i = 0; i < writes[w].nkeys; i++) {
            uint32_t k = writes[w].keys[i], f = final[koff[writes[w].series] + k];
            if (f ? older_seen(f, w)
                  : !absent_ok(writes[w].series, k, writes[w].invoke, INT64_MAX) &&
                        !salvaged_away(writes[w].series, k, writes[w].invoke, INT64_MAX)) {
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
        if (!must_survive[w] || writes[w].kind == W_INSERT)
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
    if (wal_sync_ms > 0) {
        uint64_t window_lost = 0;
        double worst_lost_ms = 0;
        for (uint32_t w = 1; final && w < nw; w++) {
            if (writes[w].st != ST_OK || !cuttable[w])
                continue;
            bool wlost = false;
            for (uint32_t i = 0; !wlost && i < writes[w].nkeys; i++) {
                uint32_t k = writes[w].keys[i], f = final[koff[writes[w].series] + k];
                if (f ? older_seen(f, w) : !absent_ok(writes[w].series, k, writes[w].invoke, INT64_MAX))
                    wlost = true;
            }
            for (uint32_t k = writes[w].lo; !wlost && writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                uint32_t f = final[koff[writes[w].series] + k];
                if (f && f < nw && older_seen(f, w))
                    wlost = true;
            }
            if (wlost) {
                window_lost++;
                double age = (double)(cut_at[w] - writes[w].complete) / 1e6;
                if (age > worst_lost_ms)
                    worst_lost_ms = age;
            }
        }
        printf("wal-sync-ms %d (slack %d ms): %zu cuts, %llu durable writes before cuts, %llu in window (%llu lost, oldest %.1f ms before cut)\n",
               wal_sync_ms, slack_ms, ncuts, (unsigned long long)durable_before_cut,
               (unsigned long long)window_acked, (unsigned long long)window_lost, worst_lost_ms);
        if (!strcmp(nemesis, "power") && seconds >= 10 && (ncuts == 0 || durable_before_cut == 0))
            INCONCLUSIVE("walsync-coverage", "wal-sync-ms %d: %zu power cuts and %llu durable writes before cuts in %.0f s: periodic sync was not tested across a cut\n",
                         wal_sync_ms, ncuts, (unsigned long long)durable_before_cut, seconds);
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
                if (other ? other != w && older(other, w)
                          : !absent_ok(q->series, k2, writes[w].invoke, q->complete) &&
                                !salvaged_away(q->series, k2, writes[w].invoke, q->complete)) {
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
        uint32_t max_w;
    } ack;
    typedef struct cack {
        int64_t complete, cut_at, max_cut;
        uint32_t w;
    } cack;
    size_t total = 0, ctotal = 0;
    uint32_t *cnt = calloc(koff[nseries] + 1, sizeof *cnt);
    uint32_t *ccnt = wal_sync_ms > 0 ? calloc(koff[nseries] + 1, sizeof *ccnt) : NULL;
    for (uint32_t w = 1; w < nw; w++) {
        if (writes[w].st != ST_OK)
            continue;
        if (!may_lose[w]) {
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                cnt[koff[writes[w].series] + writes[w].keys[j] + 1]++;
                total++;
            }
            for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                cnt[koff[writes[w].series] + k + 1]++;
                total++;
            }
        } else if (ccnt) {
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                ccnt[koff[writes[w].series] + writes[w].keys[j] + 1]++;
                ctotal++;
            }
            for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                ccnt[koff[writes[w].series] + k + 1]++;
                ctotal++;
            }
        }
    }
    for (size_t x = 1; x <= koff[nseries]; x++)
        cnt[x] += cnt[x - 1];
    ack *acks = malloc((total + 1) * sizeof *acks);
    uint32_t *fill = calloc(koff[nseries], sizeof *fill);
    cack *cacks = NULL;
    uint32_t *cfill = NULL;
    if (ccnt) {
        for (size_t x = 1; x <= koff[nseries]; x++)
            ccnt[x] += ccnt[x - 1];
        cacks = malloc((ctotal + 1) * sizeof *cacks);
        cfill = calloc(koff[nseries], sizeof *cfill);
    }
    for (uint32_t w = 1; w < nw; w++) {
        if (writes[w].st != ST_OK)
            continue;
        if (!may_lose[w]) {
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                size_t slot = koff[writes[w].series] + writes[w].keys[j];
                acks[cnt[slot] + fill[slot]++] = (ack){writes[w].complete, writes[w].invoke, lb[w], w};
            }
            for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                size_t slot = koff[writes[w].series] + k;
                acks[cnt[slot] + fill[slot]++] = (ack){writes[w].complete, writes[w].invoke, lb[w], w};
            }
        } else if (ccnt) {
            for (uint32_t j = 0; j < writes[w].nkeys; j++) {
                size_t slot = koff[writes[w].series] + writes[w].keys[j];
                cacks[ccnt[slot] + cfill[slot]++] = (cack){writes[w].complete, cut_at[w], cut_at[w], w};
            }
            for (uint32_t k = writes[w].lo; writes[w].kind != W_INSERT && k < writes[w].hi; k++) {
                size_t slot = koff[writes[w].series] + k;
                cacks[ccnt[slot] + cfill[slot]++] = (cack){writes[w].complete, cut_at[w], cut_at[w], w};
            }
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
            if (a[x].max_invoke < a[x - 1].max_invoke) {
                a[x].max_invoke = a[x - 1].max_invoke;
                a[x].max_w = a[x - 1].max_w;
            }
            if (a[x].max_lb < a[x - 1].max_lb)
                a[x].max_lb = a[x - 1].max_lb;
        }
        if (ccnt) {
            cack *ca = cacks + ccnt[slot];
            size_t cn = ccnt[slot + 1] - ccnt[slot];
            for (size_t x = 1; x < cn; x++)
                for (size_t y = x; y > 0 && ca[y - 1].complete > ca[y].complete; y--) {
                    cack t = ca[y];
                    ca[y] = ca[y - 1];
                    ca[y - 1] = t;
                }
            for (size_t x = 1; x < cn; x++)
                if (ca[x].max_cut < ca[x - 1].max_cut)
                    ca[x].max_cut = ca[x - 1].max_cut;
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
            int64_t req_inv = lo ? a[lo - 1].max_invoke : INT64_MIN;
            int64_t req_lb = lo ? a[lo - 1].max_lb : INT64_MIN;
            uint32_t req_w = lo ? a[lo - 1].max_w : 0;
            if (ccnt) {
                const cack *ca = cacks + ccnt[slot];
                size_t cn = ccnt[slot + 1] - ccnt[slot];
                if (cn > 0) {
                    size_t clo = 0, chi = cn;
                    while (clo < chi) {
                        size_t mid = (clo + chi) / 2;
                        if (ca[mid].complete < q->invoke)
                            clo = mid + 1;
                        else
                            chi = mid;
                    }
                    for (size_t j = clo; j-- > 0 && ca[j].max_cut > q->complete;) {
                        if (q->complete < ca[j].cut_at) {
                            uint32_t w = ca[j].w;
                            if (writes[w].invoke > req_inv) {
                                req_inv = writes[w].invoke;
                                req_w = w;
                            }
                            if (lb[w] > req_lb)
                                req_lb = lb[w];
                        }
                    }
                }
            }
            if (req_inv == INT64_MIN)
                continue;
            uint32_t seen = q->seen[k - q->lo];
            if (seen ? seen < nw && writes[seen].complete < req_inv
                     : !absent_ok(q->series, k, req_inv, q->complete) &&
                           !salvaged_away(q->series, k, req_inv, q->complete)) {
                char cut_info[64] = "";
                if (wal_sync_ms > 0 && req_w && ncuts > 0) {
                    for (size_t c = 0; c < ncuts; c++)
                        if (cuts[c].dead_ns >= writes[req_w].invoke) {
                            snprintf(cut_info, sizeof cut_info, " (%.1f ms before cut)",
                                     (double)(cuts[c].cut_ns - writes[req_w].complete) / 1e6);
                            break;
                        }
                }
                VIOLATION("read %u shows %u at series %d key %u, older than an acknowledged write %u%s: stale\n",
                          i, seen, q->series, k, req_w, cut_info);
            } else if (seen && seen < nw && writes[seen].st == ST_OK && writes[seen].complete < req_lb)
                VIOLATION("read %u shows %u at series %d key %u, acknowledged before a read saw a later acknowledged "
                          "write to it not yet in effect (check 5): stale\n", i, seen, q->series, k);
        }
    }
    free(acks);
    free(fill);
    free(cnt);
    free(cacks);
    free(cfill);
    free(ccnt);
    loss_violations = violations - before_loss;
    if (latest_on) {
        build_removers();
        check_latest(nw, wc, wl);
        free_removers();
    }
    free(wl);
    free(wc);
    free(applied);
    free(in_final);
    free(must_survive);
    free(seen_after_cut);
    free(eff_time);
    free(cuts);
    free(dead_at);
    free(cuttable);
    cuttable = NULL;
    free(may_lose);
    may_lose = NULL;
    free(cut_at);
    cut_at = NULL;
    free(reaped_cuts);
    reaped_cuts = NULL;
    nreaped_cuts = cap_reaped_cuts = 0;
    for (uint32_t w = 0; skeys && w < nw; w++)
        free(skeys[w]);
    free(skeys);
    skeys = NULL;
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
    if (nw > max_ops)
        nw = max_ops;
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
    bool fgone = false;
    for (int i = 0; !(fgone = waitpid(pid, &st, WNOHANG) == pid) && i < STOP_TERM_S * 200; i++) /* bounded: a hung follower is a violation, not a hang of the harness */
        usleep(5000);
    if (!fgone) {
        VIOLATION("the follower did not stop within %d s of SIGTERM, see %s\n", STOP_TERM_S, flogpath);
        signal_pid(pid, SIGKILL);
        waitpid(pid, &st, 0);
    }
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
        outage_keep_log();
        bail(2);
    }
    memset(final, 0, koff[nseries] * sizeof *final);
    memset(fnull, 0, koff[nseries]);
    for (int s = 0; s < nseries; s++) {
        tsdb_points p;
        int rc = REQ("final read", tsdbc_query_points(c, ids[s], INT64_MIN, INT64_MAX, &p));
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
            if (pid > 0 && reap_server(pid, &st, WNOHANG) == pid) {
                if (WIFSIGNALED(st))
                    fprintf(stderr, "jepsen: server %d was killed by signal %d\n", (int)pid, WTERMSIG(st));
                else
                    fprintf(stderr, "jepsen: server %d exited with %d\n", (int)pid, WEXITSTATUS(st));
                server_gone();
            } else {
                fprintf(stderr, "jepsen: server %d is still running\n", (int)pid);
                if (rc == TSDB_ERR_CORRUPT) { /* the server answers that what it holds is damaged: data corrupted by the run's faults */
                    death_problems++;
                    fprintf(stderr, "  the final read of series %d: the server says its data is damaged (%s)\n", s, tsdbc_strerror(rc));
                }
            }
            stop_server(SIGTERM); /* no orphan server */
            outage_keep_log();
            bail(2);
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
    uint64_t rows, ec_rows, parts[4], crows[4];
};
static void tally_part(void *ctx, const tsdb_part_info *in)
{
    struct cls_tally *t = ctx;
    t->rows += in->rows;
    size_t nl = in->name ? strlen(in->name) : 0;
    if (nl > 3 && !strcmp(in->name + nl - 3, ".ec"))
        t->ec_rows += in->rows;
    if (in->cls >= 0 && in->cls < 4)
        t->parts[in->cls]++, t->crows[in->cls] += in->rows;
}

/* The rows of every series that is not the workload's (the harness' own "jepsen.poke", written to a server that outlives its clients: round 13,
 * VERIFY12): the parts of a database hold them too, and the check of the parts counts the workload's keys only. */
struct other_rows {
    tsdb *db;
    uint64_t rows;
    int err;
};
static int other_series(void *ctx, tsdb_sid id)
{
    struct other_rows *o = ctx;
    for (int s = 0; s < nseries; s++)
        if (ids[s] == id)
            return 0;
    tsdb_points p = {0};
    if (tsdb_query_points(o->db, id, INT64_MIN, INT64_MAX, &p) != 0) {
        o->err++;
        return 0;
    }
    o->rows += p.n;
    tsdb_points_free(&p);
    return 0;
}

static int check_classes(const uint32_t *final)
{
    int before = violations;
    /* CLASSES: 24-byte records [check][series][from][class][0]; from = INT64_MIN is a fresh series' decision */
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%.4080s/CLASSES", dbdir);
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
    int rc = tsdb_open(dbdir, &o, &db);
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
    struct other_rows other = {.db = db};
    tsdb_series_foreach(db, other_series, &other);
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
    if (getenv("JEPSEN_TEST_PLANT_ROW")) /* a test hook of the selftest: one row too many in the parts, what a (series, time) in two partitions is */
        t.rows++;
    if (other.err)
        VIOLATION("classes: in-process read of a series that is not the workload's failed (%d)\n", other.err);
    /* With the tier an `.ec` part is never merged, so a newer part may overlap it and a row of one key sit in two
     * parts of one partition: every such duplicate has an older copy in an `.ec` part, so the rows beyond the final
     * state are at most those of the `.ec` parts. A (series, time) in two keys beyond that bound is still caught here;
     * below it only the open's check of every part against CLASSES (cls_check_parts) judges it: part info has no
     * per-series ranges. With no `.ec` part the equality holds as without the tier. */
    if (tier_n ? (t.rows < keys + other.rows || t.rows > keys + other.rows + t.ec_rows) : t.rows != keys + other.rows)
        VIOLATION("classes: after tsdb_optimize the parts hold %llu rows (%llu in .ec parts), the final state %llu (and %llu of series that are not the "
                  "workload's): rows of one (series, time) in two partitions\n", (unsigned long long)t.rows,
                  (unsigned long long)t.ec_rows, (unsigned long long)keys, (unsigned long long)other.rows);
    if (switches == 0 || used < 2) /* the workload did too little (a machine that starves the clients writes a dozen rows in 20 s): no violation */
        INCONCLUSIVE("classes-coverage", "classes: %u switches, parts in %u classes: the workload tested no change of class\n", switches,
                     used);
    return violations - before;
}

/* ---------------------------------------------------------------- the tier */

/* What a settle leaves in a database directory, and in the server logs. Counts the names of DIR (not below it: the
 * stores are in DIR/tier and hold chunks, not parts) and the lines of LOG that contain PAT, and the parts per
 * partition: p<partition>_<min>_<max>_<level>.part or .ec, a `settled_` or `retired_` prefix for what a settle or a
 * merge left. */
struct tier_part {
    int64_t partition;
    int kind; /* 0 part, 1 .ec, 2 settled_, 3 retired_ (any), 4 retired_*.ec */
};

static int cmp_tier_part(const void *a, const void *b)
{
    const struct tier_part *x = a, *y = b;
    return (x->partition > y->partition) - (x->partition < y->partition);
}

struct tier_counts {
    unsigned ec, settled, retired_ec, lines;
    size_t nparts, nlive; /* names counted per partition; those of them that are listed (a part or an .ec) */
    size_t nlivepart;     /* partitions with a listed part */
    struct tier_part *parts;
};

static bool ends_with(const char *s, const char *suf)
{
    size_t a = strlen(s), b = strlen(suf);
    return a >= b && !strcmp(s + a - b, suf);
}

static void tier_scan(const char *d, const char *log, const char *pat, struct tier_counts *c)
{
    memset(c, 0, sizeof *c);
    DIR *dp = opendir(d);
    size_t cap = 256;
    c->parts = malloc(cap * sizeof *c->parts);
    struct dirent *e;
    while (dp && (e = readdir(dp))) {
        const char *n = e->d_name;
        int kind;
        bool is_ec = ends_with(n, ".ec");
        if (!strncmp(n, "settled_", 8)) {
            n += 8;
            kind = 2;
            c->settled++;
        } else if (!strncmp(n, "retired_", 8)) {
            n += 8;
            kind = is_ec ? 4 : 3;
            c->retired_ec += is_ec;
        } else if (n[0] == 'p' && (is_ec || ends_with(n, ".part"))) {
            kind = is_ec;
            c->ec += is_ec;
        } else {
            continue;
        }
        long long part = 0, lo, hi;
        unsigned lvl;
        /* a retired_ name keeps its source's: retired_p<N>_..., or retired_<something>: only the first form counts */
        if (n[0] != 'p' || sscanf(n, "p%lld_%lld_%lld_%u", &part, &lo, &hi, &lvl) != 4)
            continue;
        if (c->nparts == cap)
            c->parts = realloc(c->parts, (cap *= 2) * sizeof *c->parts);
        c->parts[c->nparts++] = (struct tier_part){part, kind};
        c->nlive += kind <= 1;
    }
    if (dp)
        closedir(dp);
    FILE *f = log ? fopen(log, "r") : NULL;
    char line[2048];
    while (f && fgets(line, sizeof line, f))
        c->lines += strstr(line, pat) != NULL;
    if (f)
        fclose(f);
    qsort(c->parts, c->nparts, sizeof *c->parts, cmp_tier_part);
    for (size_t i = 0; i < c->nparts;) {
        size_t j = i;
        bool live = false;
        while (j < c->nparts && c->parts[j].partition == c->parts[i].partition)
            live |= c->parts[j++].kind <= 1;
        c->nlivepart += live;
        i = j;
    }
}

#ifdef TSDB_EC
typedef struct salv_race_ctx {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool armed;
    bool hit;
    bool release;
} salv_race_ctx;

static void salv_race_hook(void *arg, int at)
{
    if (at != LH_WARM_BEFORE_STORE)
        return;
    salv_race_ctx *c = arg;
    pthread_mutex_lock(&c->mu);
    if (c->armed && !c->hit) {
        c->hit = true;
        pthread_cond_broadcast(&c->cv);
        while (!c->release)
            pthread_cond_wait(&c->cv, &c->mu);
    }
    pthread_mutex_unlock(&c->mu);
}
#endif

/* Post-run live latest-table salvage check (`--nemesis salvage`): opens the clean database in-process with the latest
 * table enabled, writes and settles a top-timestamp partition on ids[0] & ids[1], pauses the warmer at
 * LH_WARM_BEFORE_STORE right after it computes the pre-salvage row from the .ec part, wipes & marks all stores LOST,
 * verifies E15 TSDB_ERR_IO on unread ids[1], runs live tsdb_tier_salvage(), releases the warmer, and checks tsdb_latest
 * against full-scan tsdb_query_points. Deterministically catches the exact original `salvagelatest` (skipped lgen bump). */
static void salvage_live_latest_check(void)
{
#ifdef TSDB_EC
    if (!t_salvage)
        return;
    setenv("TSDB_TEST_TIER", "grace_s=0,keep_s=0,eager=1", 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = PARTITION_MS;
    o.latest_cache = 16u << 20; /* 16 MiB: enough for page 0 of ltab (1 MiB per 65,536-series page) */
    tsdb *db = NULL;
    int rc = tsdb_open(dbdir, &o, &db);
    if (rc != 0 || !db) {
        tier_harness_fail("salvage live-latest: tsdb_open(%s) failed (%s)", dbdir, tsdb_strerror(rc));
        return;
    }
    /* Write two flushes of ids[0] and ids[1] into a brand-new top partition strictly newer than every kv key, optimize
     * so it merges to level 1, and wait for eager settle to move it into an .ec part. */
    int64_t t_young = T0 + 19LL * PARTITION_MS + 1000;
    int64_t t_top = T0 + 20LL * PARTITION_MS;
    int64_t p_top = -1;
    int64_t ts1 = t_top + 1000, ts2 = t_top + 2000;
    float vy = 888888.0f, v1 = 999991.0f, v2 = 999992.0f;
    if (tsdb_insert_series(db, ids[0], &ts1, &v1, 1) != 0 || tsdb_insert_series(db, ids[1], &ts1, &v1, 1) != 0 ||
        tsdb_flush(db) != 0 || tsdb_insert_series(db, ids[0], &ts2, &v2, 1) != 0 ||
        tsdb_insert_series(db, ids[1], &ts2, &v2, 1) != 0 || tsdb_flush(db) != 0 || tsdb_optimize(db) != 0) {
        tier_harness_fail("salvage live-latest: setup write/flush/optimize failed");
        tsdb_close(db);
        return;
    }
    bool settled = false;
    for (int w = 0; w < 100 && !settled; w++) {
        int64_t parts[64];
        int np = list_ec_partitions(dbdir, parts, 64);
        for (int i = 0; i < np; i++) {
            int64_t lo = 0, hi = 0;
            part_key_range(parts[i], PARTITION_MS, &lo, &hi);
            if (ts2 >= lo && ts2 < hi) {
                p_top = parts[i];
                settled = true;
                break;
            }
        }
        if (!settled)
            usleep(100000);
    }
    if (!settled) {
        tier_liveness("salvage live-latest: top partition for ts %lld did not settle into .ec within 10 s",
                      (long long)ts2);
        tsdb_close(db);
        return;
    }
    /* Write a surviving level-0 young .part on ids[0] at t_young (below p_top, strictly above all kv partitions):
     * eager settle never settles level-0 parts, so post-salvage tsdb_latest(ids[0]) finds t_young directly without
     * scanning older kv partitions (letting us salvage p_top alone cleanly via tsdb_tier_salvage(db, p_top, 0, 0)). */
    if (tsdb_insert_series(db, ids[0], &t_young, &vy, 1) != 0 || tsdb_flush(db) != 0) {
        tier_harness_fail("salvage live-latest: setup young-part write failed");
        tsdb_close(db);
        return;
    }
    /* Remove any settled_ copy of p_top so salvage retires the .ec part instead of restoring from settled_. */
    {
        char spfx[80];
        snprintf(spfx, sizeof spfx, "settled_p%lld_", (long long)p_top);
        DIR *dp = opendir(dbdir);
        struct dirent *e;
        while (dp && (e = readdir(dp)) != NULL) {
            if (!strncmp(e->d_name, spfx, strlen(spfx))) {
                char fp[PATH_MAX + 256];
                snprintf(fp, sizeof fp, "%.4000s/%s", dbdir, e->d_name);
                unlink(fp);
            }
        }
        if (dp)
            closedir(dp);
    }
    /* Drain any earlier warmer work so LH_WARM_BEFORE_STORE catches our targeted ids[0] warm below. */
    for (int ms = 0; ms < 5000; ms += 10) {
        tsdb_latest_stats lst;
        if (tsdb_get_latest_stats(db, 0, &lst) == 0 && lst.enabled && lst.unknown == 0)
            break;
        usleep(10000);
    }
    /* Arm warmer pause at LH_WARM_BEFORE_STORE, then write a NULL at ts2 on ids[0] so lt_fold marks ids[0] LE_UNKNOWN
     * and hints the warmer: it reads ts1 from p_top's .ec part and pauses right before storing into ltab! */
    salv_race_ctx rctx = {.mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER, .armed = true};
    tsdb_test_latest_hook(db, salv_race_hook, &rctx);
    float dummy = 0.0f;
    uint8_t valid0 = 0;
    tsdb_values ncol = {.type = TSDB_FLOAT32, .values = &dummy, .valid = &valid0};
    if (tsdb_insert_values(db, &ids[0], &ts2, &ncol, 1) != 0 || tsdb_flush(db) != 0) {
        tsdb_test_latest_hook(db, NULL, NULL);
        tier_harness_fail("salvage live-latest: null write to trigger warmer failed");
        tsdb_close(db);
        return;
    }
    struct timespec wt;
    clock_gettime(CLOCK_REALTIME, &wt);
    wt.tv_sec += 10;
    pthread_mutex_lock(&rctx.mu);
    while (!rctx.hit) {
        if (pthread_cond_timedwait(&rctx.cv, &rctx.mu, &wt) == ETIMEDOUT)
            break;
    }
    bool hook_hit = rctx.hit;
    pthread_mutex_unlock(&rctx.mu);
    if (!hook_hit) {
        tier_liveness("salvage live-latest: warmer did not reach LH_WARM_BEFORE_STORE within 10 s");
        tsdb_test_latest_hook(db, NULL, NULL);
        tsdb_close(db);
        return;
    }
    /* Mark ALL stores LOST (guaranteed > m positions lost for every .ec part regardless of k') and wake tierctl. */
    int all_slots[16];
    for (int i = 0; i < tier_n; i++) {
        all_slots[i] = i;
        rm_tree_dir(store_paths[i]);
    }
    if (salvage_mark_stores_lost(all_slots, tier_n) != 0) {
        pthread_mutex_lock(&rctx.mu);
        rctx.release = true;
        pthread_cond_broadcast(&rctx.cv);
        pthread_mutex_unlock(&rctx.mu);
        tsdb_test_latest_hook(db, NULL, NULL);
        tier_harness_fail("salvage live-latest: failed to mark stores LOST");
        tsdb_close(db);
        return;
    }
    tier *t = tier_find_by_dir(dbdir);
    if (t) {
        tierctl_wake(t);
        (void)tierctl_wait_idle(t, 10000);
        eccache_reopen_all(t);
        tier_put(t);
    }
    /* Verify read of unread ids[1] in p_top fails with TSDB_ERR_IO (E15). */
    bool io_seen = false;
    for (int w = 0; w < 50 && !io_seen; w++) {
        tsdb_points p = {0};
        int qrc = tsdb_query_points(db, ids[1], t_top, t_top + PARTITION_MS, &p);
        tsdb_points_free(&p);
        if (qrc == TSDB_ERR_IO)
            io_seen = true;
        else
            usleep(100000);
    }
    if (!io_seen) {
        pthread_mutex_lock(&rctx.mu);
        rctx.release = true;
        pthread_cond_broadcast(&rctx.cv);
        pthread_mutex_unlock(&rctx.mu);
        tsdb_test_latest_hook(db, NULL, NULL);
        VIOLATION("salvage live-latest: read of lost top partition did not return TSDB_ERR_IO\n");
        tsdb_close(db);
        return;
    }
    /* Run live tsdb_tier_salvage on p_top ONLY (isolated from any older kv partition layout) while warmer holds ts1! */
    rc = tsdb_tier_salvage(db, p_top, 0, 0);
    /* Release the paused warmer and wait for it to finish its store attempt (must be rejected by lgen bump). */
    pthread_mutex_lock(&rctx.mu);
    rctx.release = true;
    pthread_cond_broadcast(&rctx.cv);
    pthread_mutex_unlock(&rctx.mu);
    tsdb_test_latest_hook(db, NULL, NULL);
    for (int ms = 0; ms < 5000; ms += 10) {
        tsdb_latest_stats lst;
        if (tsdb_get_latest_stats(db, 0, &lst) == 0 && lst.enabled && lst.unknown == 0)
            break;
        usleep(10000);
    }
    if (rc != 0) {
        VIOLATION("salvage live-latest: tsdb_tier_salvage(partition %lld) failed (%s)\n", (long long)p_top,
                  tsdb_strerror(rc));
        tsdb_close(db);
        return;
    }
    /* Compare tsdb_latest (NULL opts -> ltab fast path) against full-scan [t_young, INT64_MAX): must return t_young! */
    tsdb_latest_result lr1 = {0};
    rc = tsdb_latest(db, &ids[0], 1, NULL, &lr1);
    if (rc != 0 || lr1.status != 0) {
        VIOLATION("salvage live-latest: post-salvage tsdb_latest failed (rc=%d status=%d)\n", rc, lr1.status);
    } else if (lr1.kind == TSDB_LATEST_VALUE && lr1.ts >= t_top) {
        VIOLATION("salvage live-latest: tsdb_latest returned salvaged row at ts %lld (salvagelatest?)\n",
                  (long long)lr1.ts);
    } else {
        tsdb_points p = {0};
        int qrc = tsdb_query_points(db, ids[0], t_young, INT64_MAX, &p);
        if (qrc == 0) {
            int64_t want_ts = INT64_MIN;
            for (size_t i = 0; i < p.n; i++)
                if (!isnan(p.values[i]) && p.ts[i] > want_ts)
                    want_ts = p.ts[i];
            if (want_ts != t_young || lr1.kind != TSDB_LATEST_VALUE || lr1.ts != want_ts) {
                VIOLATION("salvage live-latest: expected latest ts %lld, got kind=%d ts=%lld (salvagelatest?)\n",
                          (long long)want_ts, lr1.kind, (long long)lr1.ts);
            }
            tsdb_points_free(&p);
        }
    }
    tsdb_close(db);
#endif
}

/* At the end of a run with the tier: report, and fail a run in which nothing settled. */
static int tier_check(void)
{
    int before = violations;
    const char *pat = getenv("JEPSEN_SETTLE_LINE") ? getenv("JEPSEN_SETTLE_LINE") : "settled ";
    struct tier_counts c;
    tier_scan(dbdir, slogpath, pat, &c);
    /* the minimum: a few settles in any run of a minute (the eager policy settles 30 a minute at most, and a key
     * needs its merge round first); the eio nemesis fails a store now and then, and a failed store stops the settles of
     * that process until the next start, so there one is enough. */
    unsigned want = seconds >= 100 ? 5 : seconds >= 40 ? 3 : 1;
    if (!strcmp(nemesis, "eio") || tier_nem)
        want = 1;
    if (getenv("JEPSEN_MIN_SETTLES"))
        want = (unsigned)atoi(getenv("JEPSEN_MIN_SETTLES"));
    printf("tier: %d stores; at the end %u .ec, %u settled_ and %u retired_*.ec files; %u settle lines (\"%s\") in "
           "the server logs; at least %u wanted\n", tier_n, c.ec, c.settled, c.retired_ec, c.lines, pat, want);
    /* parts per partition: one entry per name that stands for a part of the partition (listed, or kept by a settle) */
    size_t nparts = 0, minp = SIZE_MAX, maxp = 0, worst_n = 0;
    int64_t worst = 0;
    size_t counts[65536], nc = 0;
    for (size_t i = 0; i < c.nparts;) {
        size_t j = i, live = 0;
        while (j < c.nparts && c.parts[j].partition == c.parts[i].partition)
            live += c.parts[j++].kind <= 1;
        if (live) {
            nparts++;
            if (live < minp)
                minp = live;
            if (live > maxp) {
                maxp = live;
                worst = c.parts[i].partition;
                worst_n = j - i;
            }
            if (nc < sizeof counts / sizeof *counts)
                counts[nc++] = live;
        }
        i = j;
    }
    if (nparts) {
        qsort(counts, nc, sizeof *counts, cmp_size);
        printf("tier: parts per partition at the end: %zu partitions, %zu listed parts (%u of them .ec), per partition "
               "min %zu, median %zu, max %zu (partition %lld: %zu listed, and %zu settled_ or retired_ names waiting "
               "out their keep time or a retire; DIR only: kv and classes have no volumes)\n",
               nparts, c.nlive, c.ec, minp, counts[nc / 2], maxp, (long long)worst, maxp, worst_n - maxp);
    } else {
        printf("tier: no part files at the end\n");
    }
    unsigned files = c.ec + c.settled + c.retired_ec;
    if (want > 0 && files == 0 && !t_salvage)
        VIOLATION("tier: no .ec, settled_ or retired_*.ec file at the end: nothing settled (or all of it was removed)\n");
    if (c.lines < want)
        VIOLATION("tier: %u settle lines (\"%s\") in the server logs, at least %u wanted in %.0f s: nothing, or too "
                  "little, settled\n", c.lines, pat, want, seconds);
    if (tier_nem) {
        printf("tier-nemesis %s: %d fault rounds, %d problems", nemesis, tier_nem_rounds, tier_nem_problems);
        if (t_fill)
            printf(", %lld UNAVAILABLE WHY_FULL refusals", (long long)atomic_load(&fill_full_refusals));
        if (t_store_lost)
            printf(", %d rebuilds verified", lost_rebuilds_verified);
        if (t_salvage)
            printf(", %d E15 IO / %d stats verified, %d .ec parts retired across %d salvage(s)", salv_io_verified,
                   salv_stats_verified, salv_parts_retired, nsalv_ev);
        printf("\n");
        if (seconds >= 10 && tier_nem_rounds == 0)
            INCONCLUSIVE("tier-nemesis-coverage", "tier nemesis %s completed 0 fault rounds in %.0f s\n", nemesis,
                         seconds);
    }
    if (tier_n && !strcmp(nemesis, "eio")) {
        printf("tier: %d EIOs landed on a file of a store\n", store_eios);
        unsigned min_eios = getenv("JEPSEN_MIN_STORE_EIOS") ? (unsigned)atoi(getenv("JEPSEN_MIN_STORE_EIOS")) : 1;
        if (want > 0 && seconds >= 20 && (unsigned)store_eios < min_eios)
            VIOLATION("tier: %d faults on a file of a store in %.0f s, at least %u wanted: the eio nemesis never hit "
                      "a store\n", store_eios, seconds, min_eios);
    }
    if (settle_steps) {
        unsigned crashes = 0;
        printf("settle-steps: servers armed / ended there:");
        for (int i = 0; i < NSTEPS; i++) {
            printf(" %s %d/%d", settle_step_names[i], step_armed[i], step_crashes[i]);
            crashes += (unsigned)step_crashes[i];
        }
        printf("; %d killed after %d s without reaching their step; %d ended with exit 99 and no power cut\n",
               step_unreached, SETTLE_WAIT_S, step_nocut);
        if (step_nocut)
            VIOLATION("settle-steps: %d servers ended at their step with exit 99 and crashfs did not roll back: the "
                      "hook must leave through exit(99), a _exit cuts no power\n", step_nocut);
        if (getenv("JEPSEN_MIN_SETTLES") == NULL || want > 0) {
            if (seconds >= 20 && crashes == 0)
                VIOLATION("settle-steps: no server ended at its armed step in %.0f s: the nemesis cut nothing\n", seconds);
            if (seconds >= 20 && (unsigned)step_unreached > crashes + 3)
                VIOLATION("settle-steps: %d servers never reached their step, %u did: too few settles to cut\n",
                          step_unreached, crashes);
        }
    }
    free(c.parts);
    return violations - before;
}

/* --auth: `tsdb DIR user ...`, the CLI as an operator runs it before the first start (never under the shim). */
static int users_cli(const char *a1, const char *a2, const char *a3, const char *a4, const char *a5)
{
    const char *cli = getenv("JEPSEN_TSDB") ? getenv("JEPSEN_TSDB") : "./build/tsdb";
    pid_t pid = fork();
    if (pid == 0) {
        unsetenv("LD_PRELOAD");
        execl(cli, "tsdb", dbdir, "user", a1, a2, a3, a4, a5, (char *)NULL);
        _exit(127);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "jepsen: %s %s user %s %s ... failed\n", cli, dbdir, a1, a2);
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
    if (mkdir(dbdir, 0755) != 0 && errno != EEXIST)
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
    int rc = tsdb_open(dbdir, &o, &db);
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
    if (from >= to)
        return 0;
    if (slow_create)
        sleep(1);
    setup_pause();
    tsdbc *c = connect_setup();
    for (int s = from; s < to; s++) {
        char name[32];
        snprintf(name, sizeof name, "jepsen.%d", s);
        if (REQ("create of a series", tsdbc_series(c, name, 1, &ids[s]))) {
            fprintf(stderr, "jepsen: cannot create series\n");
            tsdbc_close(c);
            pid_t pid = atomic_load(&server_pid); /* did the server end in the middle of it (a SIGSEGV)? say how */
            int st;
            for (int i = 0; pid > 0 && i < 100; i++) {
                if (reap_server(pid, &st, WNOHANG) == pid) {
                    note_death(st, false, pid);
                    fprintf(stderr, "jepsen: the server ended during the setup (status %#x)\n", st);
                    print_tail(slogpath);
                    server_gone();
                    break;
                }
                usleep(10000);
            }
            outage_keep_log();
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
                damaged, kept, undetected, tail_lost, tail_cuts, nem_faults, acked);
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
     * [6] write errors [7] damaged [8] violations [9] undetected [10] tail losses [11] tail cuts [12] faults the nemesis issued
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
        printf("jepsen %s%s%s: %.0f s, %d epochs, %ld faults issued by the nemesis, %ld writes acknowledged\n", nemesis,
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
    /* the nemesis must have done its work: restarts it caused, for the length of the run (epochs end early on damage, so
     * only the whole run's sum is judged) */
    long issued = sum[0][12] + sum[1][12];
    int want = seconds >= 10 ? (int)(seconds / nem_every()) : 0;
    if (issued < want) {
        INCONCLUSIVE("fault-floor", "the nemesis issued %ld faults in %.0f s (at least %d expected: one per %d s): it was idle\n", issued,
                     seconds, want, nem_every());
    }
    if (bad) {
        printf("FAILED\n");
        return 1;
    }
    if (inconclusive)
        return inconclusive_exit(true);
    printf("valid\n");
    return 0;
}

/* --tier N: sets up N store directories. By default (R15), builds TIERS in DIR/db using `tsdb DIR/db tier add` with
 * stores in DIR/tier/s0..s(N-1) (siblings of DIR/db so path-overlap checks pass while CRASHFS_ROOT=DIR covers both)
 * and passes timing keys only via TSDB_TEST_TIER. With JEPSEN_TIER_LEGACY=1, keeps the legacy dirs= path without TIERS. */
static int tier_setup(void)
{
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "jepsen: cannot make %s: %s\n", dir, strerror(errno));
        return 1;
    }
    char root_rp[PATH_MAX];
    if (!realpath(dir, root_rp)) {
        fprintf(stderr, "jepsen: cannot resolve %s: %s\n", dir, strerror(errno));
        return 1;
    }
    if (strlen(root_rp) + 16 >= sizeof tier_root) {
        fprintf(stderr, "jepsen: the path of %s is too long\n", dir);
        return 1;
    }
    snprintf(tier_root, sizeof tier_root, "%s/tier", root_rp);
    if (mkdir(tier_root, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "jepsen: cannot make %s: %s\n", tier_root, strerror(errno));
        return 1;
    }
    if (tier_legacy) {
        int m = tier_n / 3, k = tier_n - m;
        char dirs[16 * PATH_MAX], d[PATH_MAX + 64];
        size_t at = 0;
        for (int i = 0; i < tier_n; i++) {
            snprintf(d, sizeof d, "%.4000s/s%d", tier_root, i);
            if ((mkdir(d, 0755) != 0 && errno != EEXIST) || at + strlen(d) + 2 >= sizeof dirs) {
                fprintf(stderr, "jepsen: cannot make store %s: %s\n", d, strerror(errno));
                return 1;
            }
            snprintf(store_paths[i], sizeof store_paths[i], "%.4090s", d);
            at += (size_t)snprintf(dirs + at, sizeof dirs - at, "%s%s", i ? ":" : "", d);
        }
        char *spec = malloc(sizeof dirs + 256);
        const char *extra = getenv("JEPSEN_TIER_EXTRA");
        snprintf(spec, sizeof dirs + 256, "k=%d,m=%d,dirs=%s,grace_s=1,keep_s=5,eager=1%s%s", k, m, dirs,
                 extra && *extra ? "," : "", extra && *extra ? extra : "");
        setenv("TSDB_TEST_TIER", spec, 1);
        setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
        printf("tier (legacy): %d stores, k=%d m=%d, TSDB_TEST_TIER=%s\n", tier_n, k, m, spec);
        free(spec);
        return 0;
    }

    /* R15 TIERS path: db lives in DIR/db, stores in DIR/tier/s0..s(N-1), configured via `tsdb DIR/db tier add`. */
    snprintf(dbdir, sizeof dbdir, "%s/db", root_rp);
    if (mkdir(dbdir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "jepsen: cannot make %s: %s\n", dbdir, strerror(errno));
        return 1;
    }
    /* Initialize root META with the run's partition_ms before `tsdb tier add` (no TSDB_TEST_TIER set yet). */
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    {
        tsdb_options o;
        tsdb_options_init(&o);
        o.partition_ms = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
        tsdb *db = NULL;
        int rc = tsdb_open(dbdir, &o, &db);
        if (rc != 0 || !db) {
            fprintf(stderr, "jepsen: cannot initialize %s (%s)\n", dbdir, tsdb_strerror(rc));
            return 1;
        }
        tsdb_close(db);
    }
    for (int i = 0; i < tier_n; i++) {
        char d[PATH_MAX + 64], arg[PATH_MAX + 80], out[1024];
        snprintf(d, sizeof d, "%.4000s/s%d", tier_root, i);
        snprintf(arg, sizeof arg, "%.4000s:hdd", d);
        char *av[8];
        int ac = 0;
        av[ac++] = "tsdb";
        av[ac++] = dbdir;
        av[ac++] = "tier";
        av[ac++] = "add";
        av[ac++] = arg;
        av[ac++] = "--allow-shared-device";
        if (t_single_copy)
            av[ac++] = "--single-copy";
        av[ac] = NULL;
        int rc = tier_cli_run(av, out, sizeof out);
        if (rc != 0) {
            fprintf(stderr, "jepsen: `tsdb %s tier add %s` failed (rc %d): %s\n", dbdir, arg, rc, out);
            return 1;
        }
        char canon[PATH_MAX];
        snprintf(store_paths[i], sizeof store_paths[i], "%.4090s", realpath(d, canon) ? canon : d);
    }
    if (t_single_copy) {
        char out[1024];
        char *av[] = {"tsdb", dbdir, "tier", "set", "--redundancy", "1+0", NULL};
        int rc = tier_cli_run(av, out, sizeof out);
        if (rc != 0) {
            fprintf(stderr, "jepsen: `tsdb %s tier set --redundancy 1+0` failed (rc %d): %s\n", dbdir, rc, out);
            return 1;
        }
    }
    int64_t pms_val = classes ? (int64_t)CL_PARTITION_MS : PARTITION_MS;
    if (t_store_lost) {
        anchor_tbase = (T0 / pms_val - 4) * pms_val;
        int arc = tier_write_cold_part("jepsen.anchor", anchor_tbase);
        if (arc != 0) {
            fprintf(stderr, "jepsen: cannot write cold anchor part (%s)\n", tsdb_strerror(arc));
            return 1;
        }
    }
    if (t_store_stuck) {
        anchor_tbase = (T0 / pms_val - 4) * pms_val;
        int arc = tier_write_cold_part_multi("jepsen.cold", anchor_tbase, 32);
        if (arc != 0) {
            fprintf(stderr, "jepsen: cannot write cold stuck-probe part (%s)\n", tsdb_strerror(arc));
            return 1;
        }
        stuck_cold_ready = true;
    }
    if (t_salvage) {
        snprintf(salv_cur_series, sizeof salv_cur_series, "jepsen.salv.0");
        salv_cur_tbase = (T0 / pms_val - 5) * pms_val;
        int src = tier_write_cold_part(salv_cur_series, salv_cur_tbase);
        if (src != 0) {
            fprintf(stderr, "jepsen: cannot write cold salvage part (%s)\n", tsdb_strerror(src));
            return 1;
        }
    }
    char spec[512];
    const char *extra = getenv("JEPSEN_TIER_EXTRA");
    int keep = t_salvage ? 1 : 5;
    snprintf(spec, sizeof spec, "grace_s=1,keep_s=%d,eager=1%s%s", keep, extra && *extra ? "," : "",
             extra && *extra ? extra : "");
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    printf("tier (TIERS): %d store(s)%s in %s, TSDB_TEST_TIER=%s\n", tier_n, t_single_copy ? " (single-copy 1+0)" : "",
           tier_root, spec);
    return 0;
}

int main(int argc, char **argv)
{
    static struct option opts[] = {{"dir", required_argument, 0, 'd'},     {"nemesis", required_argument, 0, 'n'},
                                   {"seconds", required_argument, 0, 't'}, {"clients", required_argument, 0, 'c'},
                                   {"seed", required_argument, 0, 's'},    {"crashfs", required_argument, 0, 'f'},
                                   {"workload", required_argument, 0, 'w'}, {"verify-checksums", no_argument, 0, 'V'},
                                   {"feed", no_argument, 0, 'E'},           {"tier", required_argument, 0, 'T'},
                                   {"scan-tier", required_argument, 0, 'S'},
                                   {"auth", no_argument, 0, 'A'},           {"outagefs", required_argument, 0, 'o'},
                                   {"wal-sync-ms", required_argument, 0, 'W'},
                                   {"mutant-idle", no_argument, 0, 'I'},
                                   {"mutant-longnap", no_argument, 0, 'L'},
                                   {"mutant-nemesis", required_argument, 0, 'M'},
                                   {"mutant-slowcreate", no_argument, 0, 'C'},
                                   {"scan-slow-probes", required_argument, 0, 'P'},
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
        case 'T': tier_n = atoi(optarg); break;
        case 'S': { /* the self-test of tier_scan: DIR,LOG,PATTERN prints ec, settled, retired_ec, lines, partitions */
            char *a = strdup(optarg), *c2 = strrchr(a, ','), *c1 = c2 ? (*c2 = 0, strrchr(a, ',')) : NULL;
            if (!c1)
                return 2;
            *c1 = 0;
            struct tier_counts tc;
            tier_scan(a, c1 + 1, c2 + 1, &tc);
            printf("%u %u %u %u %zu %zu\n", tc.ec, tc.settled, tc.retired_ec, tc.lines, tc.nlivepart, tc.nlive);
            return 0;
        }
        case 'I': idle_mutant = true; break;
        case 'L': idle_mutant_long = true; break;
        case 'M': nem_mutant = optarg; break;
        case 'C': slow_create = true; break;
        case 'P': { /* the self-test of slow_probe_since: FILE,DOMAIN,SINCE prints the time of the latest line, or 0 */
            char *a = strdup(optarg), *c2 = strrchr(a, ','), *c1 = c2 ? (*c2 = 0, strrchr(a, ',')) : NULL;
            if (!c1)
                return 2;
            *c1 = 0;
            printf("%.3f\n", slow_probe_since(a, c1 + 1, atof(c2 + 1)));
            return 0;
        }
        case 'o': outagefs_lib = optarg; break;
        case 'W': {
            errno = 0;
            char *ep = NULL;
            long v = strtol(optarg, &ep, 10);
            if (errno || !ep || ep == optarg || *ep != '\0' || v < 0 || v > 3600000) {
                fprintf(stderr, "jepsen: invalid --wal-sync-ms '%s' (expected 0..3600000)\n", optarg);
                return 2;
            }
            wal_sync_ms = (int)v;
            break;
        }
        default: return 2;
        }
    }
    md = !strcmp(nemesis, "misdirect");
    rot = !strcmp(nemesis, "rot");
    outage = !strcmp(nemesis, "outage");
    settle_steps = !strcmp(nemesis, "settle-steps");
    t_store_eio = !strcmp(nemesis, "store-eio");
    t_store_stuck = !strcmp(nemesis, "store-stuck");
    t_store_lost = !strcmp(nemesis, "store-lost");
    t_salvage = !strcmp(nemesis, "salvage");
    t_fill = !strcmp(nemesis, "fill");
    t_single_copy = !strcmp(nemesis, "single-copy");
    tier_nem = t_store_eio || t_store_stuck || t_store_lost || t_salvage || t_fill || t_single_copy;
    tier_legacy = getenv("JEPSEN_TIER_LEGACY") && atoi(getenv("JEPSEN_TIER_LEGACY")) > 0;
    if (tier_nem && tier_n == 0)
        tier_n = t_single_copy ? 1 : 6;
    bool need_outagefs = outage || t_store_eio || t_store_stuck || t_fill;
    bool shim = !strcmp(nemesis, "power") || !strcmp(nemesis, "eio") || md || outage || settle_steps ||
                (tier_nem && outagefs_lib != NULL);
    bool bad_tier_n = tier_n < 0 || tier_n > 16 ||
                      (tier_n && ((t_single_copy ? tier_n != 1 : tier_n < 3) || outage || md || rot || verify)) ||
                      (tier_nem && (tier_legacy || classes));
    if (!dir || (shim && !outage && !tier_nem && !crashfs) || (need_outagefs && !outagefs_lib) ||
        (outage && (classes || auth)) || ((md || rot) && classes) || bad_tier_n || (settle_steps && !tier_n) ||
        wal_sync_ms < 0 || (wal_sync_ms > 0 && (classes || (strcmp(nemesis, "power") && strcmp(nemesis, "none"))))) {
        fprintf(stderr, "usage: jepsen --dir DIR [--nemesis kill|power|pause|eio|misdirect|rot|outage|settle-steps|\n"
                        "                                   store-eio|store-stuck|store-lost|salvage|fill|single-copy|none]\n"
                        "              [--seconds S] [--clients N] [--seed N] [--crashfs build/libcrashfs.so]\n"
                        "              [--workload kv|classes] [--verify-checksums]   (misdirect and rot: workload kv)\n"
                        "              [--wal-sync-ms MS]   (periodic WAL sync: workload kv under power or none)\n"
                        "              [--auth] [--feed] [--tier N]   (N stores, >= 3 or 1 for single-copy; not with outage,\n"
                        "              misdirect, rot or --verify-checksums; settle-steps needs it)\n"
                        "              [--outagefs build/liboutagefs.so]   (required for outage, store-eio, store-stuck, fill)\n");
        return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    {
        const char *rd = getenv("JEPSEN_REQ_DEADLINE_S");
        if (rd && atoi(rd) >= 2)
            req_deadline_s = atoi(rd);
        dstate_min_ns = (req_deadline_s >= 60 ? 10 : req_deadline_s / 6 > 1 ? req_deadline_s / 6 : 1) * 1000000000LL; /* 10 s at the default 60 */
        if (getenv("JEPSEN_KSTALL_BOUND_S") && atoi(getenv("JEPSEN_KSTALL_BOUND_S")) > 0)
            kstall_bound_s = atoi(getenv("JEPSEN_KSTALL_BOUND_S"));
        struct sigaction sa;
        memset(&sa, 0, sizeof sa);
        sa.sa_handler = on_outer_kill;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESETHAND;
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGHUP, &sa, NULL);
        atexit(kill_servers_at_exit);
    }
    if (md || rot) {
        jepsen_watchdog(); /* the parent: all the epochs */
        int rc = epochs(); /* the parent runs the epochs and returns here; each child goes on below */
        if (rc >= 0)
            return rc;
        snprintf(faultlog, sizeof faultlog, "%s.e%d.faults", dir, epoch);
        unlink(faultlog);
    }
    jepsen_watchdog(); /* a run without epochs, or one epoch: a timer is not inherited by a forked child */
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
    snprintf(dbdir, sizeof dbdir, "%s", dir);
    char cmd[6 * PATH_MAX + 256];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (outage) { /* the volumes and the shim's socket beside the directory */
        for (int i = 0; i < 2; i++)
            snprintf(vol_dir[i], sizeof vol_dir[i], "%s.vol%c", dir, 'a' + i);
        snprintf(octl, sizeof octl, "%s.outagefs", dir);
        snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s' '%s' '%s' '%s.snap0' '%s.snap1'", dir, vol_dir[0], vol_dir[1],
                 octl, octl, octl);
    } else if (tier_nem && outagefs_lib) {
        snprintf(octl, sizeof octl, "%s.outagefs", dir);
        snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'", dir, octl);
    }
    if (system(cmd) != 0)
        return 2;
    rm_cmd = cmd;
    snprintf(markpath, sizeof markpath, "%s.eio", dir);
    snprintf(pullpath, sizeof pullpath, "%s.pulls", dir);
    snprintf(cutpath, sizeof cutpath, "%s.cuts", dir);
    unlink(cutpath);
    snprintf(exitmark, sizeof exitmark, "%s.cut", dir);
    if (md || rot)
        snprintf(slogpath, sizeof slogpath, "%s.e%d.server.log", dir, epoch);
    else
        snprintf(slogpath, sizeof slogpath, "%s.server.log", dir);
    unlink(slogpath);
    if (outage) { /* a log kept by an earlier run of this directory and seed is not this run's */
        snprintf(outage_keeppath, sizeof outage_keeppath, "%s.outage-s%llu.server.log", dir, (unsigned long long)seed);
        unlink(outage_keeppath);
    }
    if (getenv("JEPSEN_MAX_OPS") && atol(getenv("JEPSEN_MAX_OPS")) > 1000 && atol(getenv("JEPSEN_MAX_OPS")) < MAX_OPS)
        max_ops = (uint32_t)atol(getenv("JEPSEN_MAX_OPS"));
    pace_init();
    writes = calloc(max_ops, sizeof *writes);
    reads = calloc(max_ops, sizeof *reads);
    /* QUERY_LATEST: with nulls written and every answer checked against the acknowledged writes. Not where damage is
     * injected (a damaged part may decode to wrong rows, which the reads count as known) nor under outage (a volume is
     * away for seconds: its series answer UNAVAILABLE, which only the volume's own calls expect) nor in classes. */
    latest_on = !classes && !md && !rot && !outage;
    if (latest_on)
        lats = calloc(MAX_LATEST, sizeof *lats);
    nseries = classes ? CL_SERIES : NSERIES;
    make_keys();
    fnull = calloc(koff[nseries] + 1, 1);
    if (tier_n && tier_setup()) /* before the CLI runs: every process of the run has the tier's variables */
        return 2;
    if (auth && make_users())
        return 2;
    if (outage && make_volumes())
        return 2;
    if (feed && (outage || md || rot || classes)) {
        fprintf(stderr, "jepsen: --feed goes with the kv workload under kill, power, pause and eio\n");
        return 2;
    }
    if (feed && ((mkdir(dbdir, 0755) != 0 && errno != EEXIST) || feed_cli(true)))
        return 2;

    start_server(false); /* the series exist before any fault */
    /* classes under a shim: the series are created by the server under it (before the nemesis starts), so their
     * first rows get a fresh class there; the restart below decides every series that exists (class 0, with no
     * record), which left power and eio runs with no fresh decision at all. A series whose first flush brings
     * fewer than 64 rows is decided class 0 without a record: the summary counts the decisions recorded. */
    int early = classes && shim ? 0 : nseries;
    JP_PHASE("setup: creating the series");
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
    if (!strcmp(nemesis, "power") && !tier_n) { /* a follower refuses a volume with a tier (D7) */
        snprintf(flogpath, sizeof flogpath, "%s.follower.log", dir);
        freads = calloc(MAX_FREADS, sizeof *freads);
        atexit(kill_follower);
        start_follower();
        pthread_create(&fr, NULL, follower_reader, NULL);
    }
    run_start = now_ns();
    JP_PHASE("running (clients and the nemesis)");
    for (int i = 0; i < nclients; i++)
        pthread_create(&th[i], NULL, client, (void *)(intptr_t)i);
    nem_clock = true;
    nem_t0_ns = now_ns();
    if (pthread_create(&watch_th, NULL, watcher, NULL) == 0)
        watch_started = true;
    pthread_t dst_th;
    bool dst_started = pthread_create(&dst_th, NULL, dstate_thread, NULL) == 0;
    if (outage) {
        samp_on = pthread_create(&samp_th, NULL, disk_sampler, NULL) == 0;
    }
    if (strcmp(nemesis, "none"))
        pthread_create(&nem, NULL, nemesis_thread, NULL);
    for (int64_t end = now_ns() + (int64_t)(seconds * 1e9); now_ns() < end && !atomic_load(&hung_flag);) /* a request that got no answer ends the run at once */
        usleep(20000);
    stop_ns = now_ns();
    if (!outage && !(md || rot) && !atomic_load(&hung_flag)) {
        JP_PHASE("the quiet tail");
        atomic_store(&nem_quiet, true);
        {   /* the nemesis finishes the step it is in (it holds server_lock for a step; bounded: a plug, a restart) */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 90;
            if (pthread_mutex_timedlock(&server_lock, &ts) == 0) {
                if (!strcmp(nemesis, "power") || !strcmp(nemesis, "eio") || settle_steps)
                    quiet_tail_server();
                pthread_mutex_unlock(&server_lock);
            }
        }
        for (int64_t end = now_ns() + QUIET_TAIL_S * 1000000000LL; now_ns() < end && !atomic_load(&hung_flag);)
            usleep(20000);
    }
    JP_PHASE("stopped: joining the nemesis");
    if (outage) { /* the clients go on until the heal the nemesis is in has been judged (up to its bound, 90 s) */
        atomic_store(&nem_stop, true);
        /* the heal it is in has a hard limit (OUTAGE_HARD_S) of its own; a nemesis still not done OUTAGE_JOIN_S after
         * the run's time is stuck somewhere else: the run fails there, with the heal unjudged, and does not wait on */
        int64_t join_by = now_ns() + (int64_t)OUTAGE_JOIN_S * 1000000000LL;
        while (!atomic_load(&nem_done) && now_ns() < join_by)
            usleep(20000);
        if (!atomic_load(&nem_done)) {
            outage_problems++;
            fprintf(stderr, "  the nemesis was still in its last heal or fault %d s after the run's time (the heal's limit is "
                    "%d s): it is cut short and counted as not judged\n", OUTAGE_JOIN_S, OUTAGE_HARD_S);
        }
        atomic_store(&stop, true);
        join_bounded(nem, "the nemesis", req_deadline_s + 200);
    } else {
        atomic_store(&stop, true);
        if (strcmp(nemesis, "none"))
            join_bounded(nem, "the nemesis", req_deadline_s + 200);
        /* an honest pause nemesis has continued the server before it returns; one that has not (it is judged for that: the
         * watcher saw the server stopped to the run's end) would leave every client waiting on it for good */
        if (!strcmp(nemesis, "pause") && pid_stopped(atomic_load(&server_pid)))
            signal_pid(atomic_load(&server_pid), SIGCONT);
    }
    JP_PHASE("stopped: joining the clients");
    for (int i = 0; i < nclients; i++)
        join_bounded(th[i], "a client", req_deadline_s + 30);
    JP_PHASE("stopped: joining the follower reader");
    if (fport)
        join_bounded(fr, "the follower reader", req_deadline_s + 30);
    if (watch_started)
        join_bounded(watch_th, "the watcher", 30); /* it ends at the first poll after stop_ns */
    atomic_store(&dst_stop, true);
    if (dst_started)
        join_bounded(dst_th, "the D-state sampler", 30);
    /* The nemesis is judged here, and the run goes on whatever the verdict: a lazy nemesis makes the run INCONCLUSIVE (exit 3, it
     * tested less than it says), but the final state and the whole history are always judged too, every safety check, and a loss
     * there is a VIOLATION (exit 1) on the attempt where it happened, never hidden behind a rerun (the user's scope, 2026-11-14:
     * the point of these runs is that no data is lost or corrupted, also under heavy load). A server that hangs or dies other than
     * by the planned power loss is counted in death_problems and hang_problems and ends the run in the same place (v). */
    if (strcmp(nemesis, "none") && !(md || rot))
        (void)nemesis_check();
    JP_PHASE("the final state");

    /* the final state, from a server without faults, and again after a clean restart */
    pthread_mutex_lock(&server_lock);
    pid_t pid = atomic_load(&server_pid);
    int st;
    if (pid > 0 && reap_server(pid, &st, WNOHANG) == pid) {
        if (md || rot)
            md_death(st, 0, pid);
        server_gone();
    }
    stop_server(!strcmp(nemesis, "eio") || wal_sync_ms > 0 || (tier_nem && !t_single_copy) ? SIGTERM : SIGKILL); /* eio: the shim rolls back at a normal exit; wal_sync_ms > 0: clean stop flushes unsynced buffer */
    if (atomic_load(&tier_harness_err)) {
        /* A tier nemesis harness step (outagefs ctl / TIERS write / CLI replace) failed before the final state:
         * exit 2 without judging a database that the harness itself left half-modified. */
        pthread_mutex_unlock(&server_lock);
        if (samp_on) {
            atomic_store(&samp_stop, true);
            join_bounded(samp_th, "the disk sampler", 30);
        }
        fprintf(stderr, "jepsen: harness error: %s\n",
                tier_harness_msg[0] ? tier_harness_msg : "tier nemesis setup/control failed");
        (void)system(cmd);
        return 2;
    }
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

    JP_PHASE("the checker");
    uint32_t nw = atomic_load(&nwrites) - 1, nr = atomic_load(&nreads);
    unsigned ok = 0, info = 0, fail = 0, rok = 0, dels = 0, reps = 0, nulls_ok = 0;
    for (uint32_t w = 1; w <= nw && w < max_ops; w++) {
        ok += writes[w].st == ST_OK, info += writes[w].st == ST_INFO, fail += writes[w].st == ST_FAIL;
        dels += writes[w].kind == W_DELETE && writes[w].st == ST_OK;
        reps += writes[w].kind == W_REPLACE && writes[w].st == ST_OK;
        nulls_ok += writes[w].kind == W_NULL && writes[w].st == ST_OK;
    }
    for (uint32_t i = 0; i < nr && i < max_ops; i++)
        rok += reads[i].st == ST_OK;
    if (!(md || rot)) {
        printf("jepsen %s: %.0f s, %d clients, %d restarts; writes %u ok / %u indeterminate / %u failed, reads %u ok; "
               "%u deletes and %u replaces ok\n",
               nemesis, seconds, nclients, restarts, ok, info, fail, rok, dels, reps);
        printf("requests: the slowest answered one took %.3f s (the deadline is %d s)\n", (double)atomic_load(&req_worst_ns) / 1e9,
               req_deadline_s);
    }
    if (atomic_load(&ops_cap_hit)) /* the clients stopped before the run did: what came after tested nothing (the pacing keeps an honest run below it) */
        INCONCLUSIVE("ops-cap", "a client ran out of room for its history (%u writes, %u reads, %u answers of latest: MAX_OPS and MAX_LATEST) "
                     "before the run ended; JEPSEN_OPS_PER_S paces the clients (now %lld ns between the operations of a client)\n",
                     nw, nr, atomic_load(&nlats), (long long)pace_ns);
    /* kv coverage (every workload but misdirect and rot, which count epochs): a run whose clients were starved wrote almost nothing
     * (8 acknowledged writes and no delete in 20 s, in a run with 40 busy loops on 4 CPUs), and a tsdbd that drops every 20th write
     * then passes it, because the bug never fired. Too few acknowledged writes, or too few of the deletes and replaces the workload
     * mixes in (6% of its operations), is INCONCLUSIVE: the history is judged all the same, and a loss in it is a violation. The
     * floors are far below what an honest run makes (least in about 1,000 gate runs: 1,622 writes and 162 deletes and replaces in 20 s, with
     * 16 busy loops on 4 CPUs on ext4; 24,600 and 2,500 on tmpfs; 8 and 0 only with 40 busy loops besides): 100 writes are five times
     * what a server that drops every 20th needs to lose one. */
    if (!(md || rot) && seconds >= 10) {
        if (ok < KV_MIN_WRITES) {
            INCONCLUSIVE("kv-coverage", "the clients made %u acknowledged writes in %.0f s (at least %d expected): the run tested almost nothing\n",
                         ok, seconds, KV_MIN_WRITES);
        } else if (dels + reps < KV_MIN_DELREP) {
            INCONCLUSIVE("kv-coverage", "the clients made %u acknowledged deletes and replaces in %.0f s (at least %d expected): the delete "
                         "path was not tested\n", dels + reps, seconds, KV_MIN_DELREP);
        }
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
            INCONCLUSIVE("latest-coverage", "no latest request was answered in %.0f s: the check tested nothing\n", seconds);
        }
    }
    if (md || rot)
        v = epoch_report(v, ok);
    if (classes)
        v += check_classes(final);
    if (tier_n)
        v += tier_check();
    if (t_salvage && !damaged) {
        int before_sl = violations;
        salvage_live_latest_check();
        v += violations - before_sl;
    }
    if (tier_nem) {
        v += tier_nem_problems;
        if (tier_liveness_issues > 0)
            INCONCLUSIVE("tier-liveness", "%d tier liveness miss(es) (first: %s)\n", tier_liveness_issues,
                         tier_liveness_msg);
    }
    if (!strcmp(nemesis, "eio")) {
        printf("eio: %d fail-stops\n", failstops);
        v += eio_problems;
    }
    if (outage) {
        double rate = healthy_secs > 0 ? healthy_ops / healthy_secs : 0, base = healthy_secs > 0 ? healthy_base_ops / healthy_secs : 0;
        printf("outage: %d volume faults and %d kills; the other volume wrote %.0f/s during outages (%.0f/s before)\n",
               outage_faults, outage_kills, rate, base);
        if (healthy_secs > 5 && rate < 0.8 * base) {
            /* speed, not data: a busy machine slows the healthy volume whatever the faulty one does, so it is a liveness check */
            INCONCLUSIVE("outage-throughput", "the healthy volume kept %.0f%% of its writes per second during outages (at least 80%% expected)\n",
                         base > 0 ? 100 * rate / base : 0);
        }
        printf("outage heals: %d, %d judged (%d of a flapping domain, %d of them held back by a slow write probe the server logged; "
               "slowest %.1f s, slowest flapping %.1f s), %d not judged\n",
               outage_heals, outage_judged, outage_flap_judged, outage_slow_extended, outage_worst_s,
               outage_worst_flap_s, outage_heals - outage_judged);
        v += outage_problems;
    }
    /* a server that ended by itself, under any nemesis or none (end_report), and under power one that would not die of the plug */
    v += death_problems + stall_problems + (!strcmp(nemesis, "power") ? hang_problems : 0);
    if (atomic_load(&kstall_seen))
        INCONCLUSIVE("kernel-stall", "%d requests got no answer within the %d s deadline while a thread of the server sat in D state (the kernel, not "
                     "tsdbd: a machine whose file system stalls) for %.0f s or more%s: liveness under load, not a verdict; the history is judged as always\n",
                     kstall_misses, req_deadline_s, (double)dstate_min_ns / 1e9, atomic_load(&kstall_over) ? ", and the stall outlasted its bound" : "");
    JP_PHASE("the cleanup (rm -rf of the directory)");
    unlink(markpath);
    unlink(pullpath);
    if (!v || !getenv("JEPSEN_KEEP"))
        unlink(cutpath);
    {
        glob_t g; /* exitmark.<pid> of servers that were stopped, not read */
        char pat[PATH_MAX + 8];
        snprintf(pat, sizeof pat, "%s.*", exitmark);
        if (glob(pat, 0, NULL, &g) == 0) {
            for (size_t i = 0; i < g.gl_pathc; i++)
                unlink(g.gl_pathv[i]);
            globfree(&g);
        }
    }
    if (samp_on) {
        atomic_store(&samp_stop, true);
        join_bounded(samp_th, "the disk sampler", 30);
    }
    if (outage && (v || outage_near_miss)) {
        /* what a failed or a close run has to show: the server's side of it (the log of every start of the run), under
         * a name the next run does not unlink; CI uploads /tmp/tsdb-jepsen-ci*.log */
        outage_keep_log();
    } else if (!v && !getenv("JEPSEN_KEEP_LOG")) {
        unlink(slogpath);
    }
    if (fport && !fv)
        unlink(flogpath);
    if (v && getenv("JEPSEN_KEEP")) { /* the directory, and the history as DIR.hist, for a diagnosis */
        dump_history(final);
        printf("kept %s, history %s.hist, server log %s\n", dir, dir, slogpath);
        printf("FAILED: %d violations\n", v);
        return 1;
    }
    if (system(cmd) != 0)
        return 2;
    if (v) {
        printf("FAILED: %d violations\n", v);
        return 1;
    }
    if (env_kills) { /* the machine killed the server (see end_report): nothing here says anything about tsdb */
        fprintf(stderr, "jepsen: harness error: a server was killed, or fail-stopped on a full disk, by something that was not the harness\n");
        return 2;
    }
    if (atomic_load(&tier_harness_err)) {
        fprintf(stderr, "jepsen: harness error: %s\n", tier_harness_msg[0] ? tier_harness_msg : "tier nemesis setup/control failed");
        return 2;
    }
    if (inconclusive)
        return inconclusive_exit(true);
    if (!(md || rot))
        printf("valid\n");
    return 0;
}
