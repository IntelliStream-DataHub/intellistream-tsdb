/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Crash points (docs/design-cluster.md 13.2, the midcommit nemesis): a named place in server code where the harness can
 * make the process end at once, as a power cut would, to see what recovery makes of the state it left.
 *
 *     crash_point(CP_NAME);
 *
 * is armed by the environment, read once per process: TSDB_CRASH_AT=name[:n] ends the process with `_exit(99)` at the
 * n-th time that point is passed (default 1), after one line on stderr ("tsdbd: crash point NAME (hit N): exiting
 * with status 99") that the harness counts. TSDB_CRASH_AT=name[:n]@path counts the passes only once the file `path`
 * exists (one stat per pass of the armed point until it does): a harness arms a process at its start and triggers the
 * point later, without the restart that would cost a replicated volume's leader its leadership (jepsen-cluster's last
 * fault on the leader of its replicated volume). 99 is crashfs's power loss too and no signal's (a SIGSEGV is a bug, and
 * jepsen tells the two apart). An unknown name ends the process with status 2 at the first crash_point(): a typo in a
 * harness must not pass as a point that never fired.
 *
 * Compiled into every build: with nothing armed a crash point costs one relaxed load. Server code only (libtsdb does
 * not link server/). The names are listed here, and a name a test arms must be in the list; the owner of the code
 * around a point places it and flips `placed` in tests/jepsen/jepsen_cluster.c's table.
 */
#ifndef TSDBD_CRASHPT_H
#define TSDBD_CRASHPT_H

#include <stdatomic.h>

#define CRASH_EXIT_STATUS 99

/* X(identifier, "name"): where it is placed. r3d and r3e fill the others in. */
#define CRASH_POINTS(X)                                                                                              \
    X(CP_CLUSTER_STARTED, "cluster-started")           /* tsdbd.c: right after cluster_start, before serving (placed) */ \
    X(CP_SERVING, "serving")                           /* tsdbd.c: after the port file: the node serves (placed) */ \
    X(CP_WAL_BEFORE_SEND, "wal-before-send")           /* leader: its WAL write is durable, nothing sent (r3d) */   \
    X(CP_ACKED_BEFORE_OK, "acked-before-ok")           /* leader: replicas acknowledged, the client has no OK (r3d) */ \
    X(CP_PREPARED_BEFORE_COMMIT, "prepared-before-commit") /* a large write: PREPARED sent, COMMIT not (r3d/r3e) */ \
    X(CP_CLASS_SWITCH, "class-switch")                 /* during a partition class switch (r3e) */           \
    X(CP_FENCE_SAVED, "fence-saved-before-reply")      /* a replica saved a FENCE in REPL, its reply not sent */ \
    X(CP_PROMOTE_BEFORE_EPOCH, "promote-before-epoch") /* named leader of e+1, its EPOCH marker not written */  \
    X(CP_DEPOSE_BEFORE_REOPEN, "depose-before-reopen") /* a deposed writer closed, not yet open as a replica */ \
    X(CP_COLD_BEFORE_APPLY, "cold-before-apply")       /* a replica's MK_COLD: checked, nothing done (r8h) */ \
    X(CP_COLD_MID_INTENT, "cold-mid-intent")           /* its intent durable, the .ec renames done, Y and R not */ \
    X(CP_COLD_AFTER_MARKER, "cold-after-marker")       /* a leader's settle: MK_COLD committed, the swap not run (r8g) */ \
    X(CP_SET_REMOVED, "set-removed")                   /* a removed replica's v<N> renamed .removed-, root synced (R4) */

enum crash_point_id {
#define X(id, name) id,
    CRASH_POINTS(X)
#undef X
    CRASH_POINT_COUNT
};

extern const char *const crash_point_names[CRASH_POINT_COUNT];
/* -1: the environment is not read yet, 0: nothing armed, 1: a point is armed */
extern _Atomic int crashpt_state;
void crashpt_slow(enum crash_point_id id);

/* crash_point(CP_X): a misspelt point is a compile error, not a point that never fires. */
static inline void crash_point(enum crash_point_id id)
{
    if (__builtin_expect(atomic_load_explicit(&crashpt_state, memory_order_relaxed) != 0, 0))
        crashpt_slow(id);
}

#endif
