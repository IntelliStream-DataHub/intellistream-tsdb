/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A received snapshot's install (snapinst.h, tsdbd's side; R4 plan D5, D6, D18; design-cluster.md 8.1).
 *
 * rsnap's worker hands a complete `v<N>.snap/` here (never the driver's thread, a link's or the cluster thread: the
 * install reads every byte). The volume is taken from the manager by cvol_install (server/cvol.c), which stops its
 * driver and closes its engine when one is open; only then is the old REPL read as `keep` (D6: the driver saves REPL
 * until it is stopped, and a FENCE saved between a read and the close would be lost). A snapshot whose P's epoch is
 * below the fence `keep` holds is not installed (the receiver is fenced above it: the new leader's notice asks again);
 * snap_install (src/snapshot.c) checks every file, writes META, FNO, REPL (keep's identity and promises, floor P) and
 * SNAPDONE and swaps the directories. The manager then opens the volume as it opens a new record: a new open
 * generation, REPLICA_OPEN waited for, the core opened from the installed REPL (log start, floor P, history).
 *
 * Crashes. snap-received (rsnap.c, every file in and synced, before the install) and snap-swapped (after the swap's
 * root fsync, before the old directory is removed): at the next start cvolfs_prepare runs snapinst_leftovers (snapinst.c)
 * before it looks at v<N>, and snap_leftovers finishes a swap whose SNAPDONE is there and removes a copy without it (asked again).
 *
 * Mutants (TSDB_REPLD_MUTANT): snapstalekeep (keep read
 * before the volume is closed), snapkeepdiverged (the old directory's DIVERGED copied into the new one); rsnap.c has
 * snapinstallstale (with the check after the close), cvol.c snapservebeforeopen; snapdropafter (a copy not installed
 * left for the worker's removal after the hand-back), snapnoendafter (P not compared with the stopped driver's log end),
 * snapnofence (with rsnap.c: a copy below the fence installed). Test hook: TSDB_REPLD_TEST=snapclosedelay=MS waits MS
 * before the close. Crash point snap-halfswap: between the swap's two renames (v<N>.old, v<N>.snap, no v<N>).
 */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/replfile.h"
#include "../src/snapshot.h"
#include "crashpt.h"
#include "cvol.h"
#include "repld.h"
#include "repld_int.h"
#include "snapinst.h"

/* TSDB_REPLD_TEST=snappowerat=K (test_cluster snappower, crashfs preloaded): after install step K (SNAP_INST_*) the
 * process asks crashfs for a power loss, which comes at the next tracked operation (K = 0: before the close) */
static int power_at = -1;

static void inst_hook(int at)
{
    if (at == power_at) {
        say("TEST: a power loss after install step %d (snappowerat)", at);
        kill(getpid(), SIGUSR2); /* process-directed: a thread that blocks it does not keep it pending */
    }
    if (at == SNAP_INST_SWAPPED)
        crash_point(CP_SNAP_SWAPPED);
    if (at == SNAP_INST_HALF)
        crash_point(CP_SNAP_HALFSWAP);
}

/* vdir's REPL into st: 1 read, 0 none there, <0 an error */
static int repl_read(const char *vdir, struct replfile_state *st)
{
    int dfd = open(vdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct stat sb;
    if (fstatat(dfd, "REPL", &sb, 0) != 0) {
        int e = errno;
        close(dfd);
        return e == ENOENT ? 0 : TSDB_ERR_IO;
    }
    struct replfile *rf = NULL;
    bool torn = false;
    int rc = replfile_load(dfd, &rf, st, &torn);
    close(dfd);
    if (rf)
        replfile_close(rf);
    return rc == 0 ? 1 : rc;
}

/* the whole of a small file (DIVERGED), malloc'd; NULL when missing */
static uint8_t *file_read(const char *path, size_t *n)
{
    FILE *f = fopen(path, "rb");
    *n = 0;
    if (!f)
        return NULL;
    uint8_t *b = NULL;
    size_t cap = 0;
    for (;;) {
        if (*n == cap) {
            uint8_t *x = realloc(b, cap = cap ? cap * 2 : 4096);
            if (!x)
                break;
            b = x;
        }
        size_t k = fread(b + *n, 1, cap - *n, f);
        if (!k)
            break;
        *n += k;
    }
    fclose(f);
    return b;
}

struct inst {
    const char *root, *staged;
    uint32_t number;
    uint64_t p, p_epoch;
    struct replfile_state pre; /* snapstalekeep: read before the close */
    int have_pre;              /* 1 read, -1 none there, 0 not read */
    struct snapinst_result *out;
};

static int inst_body(struct inst *in, const struct snap_ident *id, int had_engine, uint64_t log_end)
{
    char vdir[PATH_MAX];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, in->root, in->number);
    struct replfile_state keep = {0};
    int have;
    if (in->have_pre) {
        keep = in->pre;
        memset(&in->pre, 0, sizeof in->pre);
        have = in->have_pre > 0;
        in->have_pre = 0;
    } else {
        have = repl_read(vdir, &keep);
    }
    if (have < 0) {
        say("volume %" PRIu32 ": the REPL of %s does not read (%s): the snapshot is not installed", in->number, vdir,
            tsdb_strerror(have));
        return have;
    }
    in->out->keep_fence = have ? keep.fence : 0;
    if (have && keep.fence > in->p_epoch && G.mutant != RM_SNAPNOFENCE) {
        say("volume %" PRIu32 ": the snapshot of epoch %" PRIu64 " is not installed: this replica is fenced at %" PRIu64
            " (asked again)", in->number, in->p_epoch, keep.fence);
        replfile_state_free(&keep);
        in->out->stale = 1;
        return 1;
    }
    /* requirement 1 after the close: the driver ran on while the install waited for M.op and for its join, and a log
     * it extended past P from a new leader holds positions the copy would truncate (rsnap.c checked before) */
    if (had_engine && G.mutant != RM_SNAPINSTALLSTALE && G.mutant != RM_SNAPNOENDAFTER &&
        (log_end == REPLD_END_UNKNOWN || in->p <= log_end)) {
        if (log_end == REPLD_END_UNKNOWN)
            say("volume %" PRIu32 ": the snapshot is stale and not installed after the close: P %" PRIu64 ", the closed"
                " driver had no log to compare it with (asked again)", in->number, in->p);
        else
            say("volume %" PRIu32 ": the snapshot is stale and not installed after the close: P %" PRIu64 ", this"
                " replica's log ended at %" PRIu64 " when its driver stopped (asked again)", in->number, in->p, log_end);
        replfile_state_free(&keep);
        in->out->stale = 1;
        return 1;
    }
    size_t divn = 0;
    uint8_t *div = NULL;
    char divp[PATH_MAX + 16];
    snprintf(divp, sizeof divp, "%s/DIVERGED", vdir);
    if (G.mutant == RM_SNAPKEEPDIVERGED)
        div = file_read(divp, &divn);
    __atomic_store_n(&snap_inst_hook, inst_hook, __ATOMIC_RELEASE);
    int rc = snap_install(in->root, in->number, in->staged, id, have ? &keep : NULL);
    replfile_state_free(&keep);
    if (rc != 0) {
        say("volume %" PRIu32 ": the install of %s failed: %s (%s)", in->number, in->staged, tsdb_strerror(rc),
            had_engine ? "its engine was open" : "no engine was open");
        free(div);
        return rc;
    }
    if (div && divn) { /* planted: the diverged copy's flag carried into the fresh one */
        FILE *f = fopen(divp, "wb");
        if (f) {
            fwrite(div, 1, divn, f);
            fflush(f);
            fsync(fileno(f));
            fclose(f);
            say("MUTANT: volume %" PRIu32 ": DIVERGED carried into the installed copy (snapkeepdiverged)", in->number);
        }
    }
    free(div);
    struct replfile_state st = {0};
    int dfd = open(vdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    struct replfile *rf = NULL;
    bool torn = false;
    if (dfd >= 0 && replfile_load(dfd, &rf, &st, &torn) == 0) {
        in->out->fence = st.fence;
        in->out->open_gen = st.open_gen;
        replfile_close(rf);
        replfile_state_free(&st);
    }
    if (dfd >= 0)
        close(dfd);
    in->out->installed = 1;
    return 0;
}

/* inst_body, then what it did not install is removed here, before cvol_install hands the volume back: the prepare that
 * follows runs snap_leftovers on v<N>.snap, and a removal by rsnap's worker during it (exists, then an open that
 * fails) refused the volume until a restart (review F1). A copy with its SNAPDONE stays for snap_leftovers. */
static int inst_fn(void *ctx, const struct snap_ident *id, int had_engine, uint64_t log_end)
{
    struct inst *in = ctx;
    int rc = inst_body(in, id, had_engine, log_end);
    if (in->out->installed || G.mutant == RM_SNAPDROPAFTER)
        return rc;
    char done[PATH_MAX + 16];
    snprintf(done, sizeof done, "%s/" SNAP_DONE, in->staged);
    struct stat sb;
    if (lstat(done, &sb) == 0) {
        in->out->taken = 1; /* a swap for snap_leftovers to finish */
    } else if (lstat(in->staged, &sb) != 0 && errno == ENOENT) {
        in->out->taken = 1;
    } else if (snap_release(in->staged) == 0) {
        in->out->taken = 1;
    } else {
        say("volume %" PRIu32 ": %s could not be removed before the volume opens again (its prepare does)",
            in->number, in->staged);
        in->out->taken = 1; /* never removed by the worker after the hand-back: snap_leftovers alone owns it now */
    }
    return rc;
}

int snapinst_install(const char *root, uint32_t number, const char *staged, uint64_t p, uint64_t p_epoch,
                     struct snapinst_result *out)
{
    memset(out, 0, sizeof *out);
    struct inst in = {.root = root, .staged = staged, .number = number, .p = p, .p_epoch = p_epoch, .out = out};
    if (G.mutant == RM_SNAPSTALEKEEP) { /* planted: keep read while the driver may still save REPL */
        char vdir[PATH_MAX];
        snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, root, number);
        int h = repl_read(vdir, &in.pre);
        in.have_pre = h > 0 ? 1 : h == 0 ? -1 : 0;
        say("MUTANT: volume %" PRIu32 ": the old REPL read before the close (snapstalekeep)", number);
    }
#ifdef TSDB_TEST_HOOKS
    const char *te = getenv("TSDB_REPLD_TEST");
    const char *pa = te ? strstr(te, "snappowerat=") : NULL;
    if (pa)
        power_at = atoi(pa + strlen("snappowerat="));
    const char *cd = te ? strstr(te, "snapclosedelay=") : NULL;
    if (cd) {
        unsigned ms = (unsigned)strtoul(cd + strlen("snapclosedelay="), NULL, 10);
        say("TEST: volume %" PRIu32 ": the close for the install waits %u ms (snapclosedelay)", number, ms);
        usleep((useconds_t)ms * 1000);
    }
    if (power_at == 0)
        inst_hook(0);
#endif
    int rc = cvol_install(number, inst_fn, &in);
    if (in.have_pre > 0)
        replfile_state_free(&in.pre);
    return rc;
}
