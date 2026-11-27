/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the volume manager, the directory side (cvolset.h; R4 plan D12, D13). Linked into tsdbd and
 * into test_cvolfs, so it needs nothing of the cluster thread or the driver: the manager side (cvolset_record,
 * cvolset_close, the sweep) is server/cvolset_mgr.c.
 *
 * A directory the record does not describe is never opened and never silently made again:
 *   - v<N> whose REPL holds another rid than the record names for this server: set aside, `v<N>.stale-<rid>` (kept, a
 *     line names it), or `v<N>.lost-<rid>` when the manager saw a REPLICA_RESET to the record's rid (cvolset_note);
 *     cvolfs then makes a fresh v<N> for the record's rid. A server that was down during a REMOVE and is added again
 *     later starts fresh instead of being refused as "another instance".
 *   - v<N> missing while the group recorded an open of this replica (open_gen > 0): refused with a line naming
 *     `tsdb cluster replica reset`; an unmounted disk looks the same, so it is never made again by itself (D13).
 *   - v<N> of a committed removal: `v<N>.removed-<rid>`, root fsynced, deleted in the background (cvolset_setaside).
 * Every rename is followed by an fsync of the root before anything relies on it.
 */
#define _GNU_SOURCE /* nftw */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cvolset.h"
#include "cluster.h"
#include "../src/replfile.h"

#define say cvolfs_say

/* ---- the manager's notes (a reset seen, a removal committed); any thread */
#define NOTES 64
static struct {
    pthread_mutex_t mu;
    struct {
        uint32_t number;
        int kind;     /* CVS_RESET, CVS_UNNAMED (a committed removal) */
        uint64_t rid; /* RESET: the record's new rid */
    } n[NOTES];
    unsigned nn;
    char *fault_logged[128]; /* R9 (D6): v<N>.fault-<rid> paths already logged in this process */
    unsigned nfault_logged;
} N = {.mu = PTHREAD_MUTEX_INITIALIZER};

void cvolset_log_fault_once(uint32_t number, const char *path)
{
    int say_it = 0;
    pthread_mutex_lock(&N.mu);
    unsigned i = 0;
    while (i < N.nfault_logged && strcmp(N.fault_logged[i], path) != 0)
        i++;
    if (i == N.nfault_logged) {
        say_it = 1;
        if (N.nfault_logged < sizeof N.fault_logged / sizeof N.fault_logged[0]) {
            char *dup = strdup(path);
            if (dup)
                N.fault_logged[N.nfault_logged++] = dup;
        }
    }
    pthread_mutex_unlock(&N.mu);
    if (say_it)
        say("volume %" PRIu32 ": faulted replica directory %s left as evidence (never opened, never deleted "
            "automatically)", number, path);
}

void cvolset_note(uint32_t number, int kind, uint64_t rid)
{
    pthread_mutex_lock(&N.mu);
    unsigned i = 0;
    while (i < N.nn && !(N.n[i].number == number && N.n[i].kind == kind))
        i++;
    if (i == N.nn && N.nn < NOTES)
        N.nn++;
    if (i < N.nn)
        N.n[i].number = number, N.n[i].kind = kind, N.n[i].rid = rid;
    pthread_mutex_unlock(&N.mu);
}

int cvolset_noted(uint32_t number, int kind, uint64_t *rid, int take)
{
    int found = 0;
    pthread_mutex_lock(&N.mu);
    for (unsigned i = 0; i < N.nn; i++)
        if (N.n[i].number == number && N.n[i].kind == kind) {
            found = 1;
            if (rid)
                *rid = N.n[i].rid;
            if (take)
                N.n[i] = N.n[--N.nn];
            break;
        }
    pthread_mutex_unlock(&N.mu);
    return found;
}

/* built by r4-iface (not a stub): test_cvolfs `unnamed` */
int cvolset_unnamed(const struct cluster_vol_rec *recs, uint32_t n, uint32_t number)
{
    for (uint32_t i = 0; i < n; i++)
        if (recs[i].number == number)
            return 0;
    return !(n >= CLUSTER_REPORT_VOLS && number > recs[n - 1].number);
}

static int sync_root(const char *root)
{
    int fd = open(root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int rc = fsync(fd);
    close(fd);
    return rc;
}

static int rm_one(const char *p, const struct stat *sb, int flag, struct FTW *f)
{
    (void)sb, (void)f;
    return (flag == FTW_DP ? rmdir(p) : unlink(p)) != 0 && errno != ENOENT ? -1 : 0;
}

static void *rm_main(void *arg)
{
    char *p = arg;
    if (nftw(p, rm_one, 16, FTW_DEPTH | FTW_PHYS) != 0)
        say("cannot remove %s: %s (left for an operator)", p, strerror(errno));
    free(p);
    return NULL;
}

void cvolset_delete_bg(const char *path)
{
    char *p = strdup(path);
    pthread_t th;
    pthread_attr_t a;
    if (!p)
        return;
    pthread_attr_init(&a);
    pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&th, &a, rm_main, p) != 0)
        free(p); /* left: the next start's prepare finds it */
    pthread_attr_destroy(&a);
}

int cvolset_setaside(const char *root, uint32_t number, const char *kind, uint64_t rid, char *to, size_t cap)
{
    char from[4200];
    snprintf(from, sizeof from, "%s/v%" PRIu32, root, number);
    struct stat sb;
    for (unsigned k = 0; k < 1000; k++) {
        if (k == 0)
            snprintf(to, cap, "%s.%s-%016" PRIx64, from, kind, rid);
        else
            snprintf(to, cap, "%s.%s-%016" PRIx64 ".%u", from, kind, rid, k);
        if (lstat(to, &sb) != 0 && errno == ENOENT)
            break;
    }
    if (rename(from, to) != 0) {
        say("volume %" PRIu32 ": cannot rename %s to %s: %s", number, from, to, strerror(errno));
        return -1;
    }
    int nosync = cvolfs_test_mutant == CVOLFS_MUT_REMOVENODIRSYNC && !strcmp(kind, "removed");
    if (!nosync && sync_root(root) != 0) {
        say("volume %" PRIu32 ": cannot sync %s after renaming %s: %s", number, root, from, strerror(errno));
        return -1;
    }
    return 0;
}

/* the rid in v<N>/REPL: 0 read, -1 none or unreadable */
int cvolset_rid_of(const char *vdir, uint64_t *rid)
{
    int dfd = open(vdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return -1;
    struct replfile *rf = NULL;
    struct replfile_state st;
    bool torn = false;
    int rc = replfile_load(dfd, &rf, &st, &torn);
    close(dfd);
    if (rc != 0)
        return -1;
    *rid = st.rid;
    replfile_close(rf);
    replfile_state_free(&st);
    return 0;
}

int cvolset_prepare(const char *root, uint32_t number, const struct cvolfs_rec *r)
{
    char vdir[4200], to[4300];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, root, number);
    /* R9 (D6): recognize v<N>.fault-<rid> set aside for a faulted replica: never opened, never deleted automatically,
     * one clear log line naming the path. */
    {
        char pref[32];
        int plen = snprintf(pref, sizeof pref, "v%" PRIu32 ".fault-", number);
        DIR *d = opendir(root);
        if (d) {
            for (struct dirent *e; (e = readdir(d)) != NULL;) {
                if (plen > 0 && strncmp(e->d_name, pref, (size_t)plen) == 0) {
                    char fp[4400];
                    snprintf(fp, sizeof fp, "%s/%s", root, e->d_name);
                    cvolset_log_fault_once(number, fp);
                }
            }
            closedir(d);
        }
    }
    uint64_t want = 0;
    int reset = cvolset_noted(number, CVS_RESET, &want, 0) && want == r->rid;
    struct stat sb;
    if (lstat(vdir, &sb) != 0) {
        if (errno != ENOENT) {
            say("volume %" PRIu32 ": cannot look at %s: %s", number, vdir, strerror(errno));
            return -1;
        }
        if (reset)
            cvolset_noted(number, CVS_RESET, NULL, 1);
        if (r->open_gen > 0 && cvolfs_test_mutant != CVOLFS_MUT_MAKEMISSING) {
            say("volume %" PRIu32 ": %s is missing, while the metadata group recorded open generation %" PRIu64
                " of replica %016" PRIx64 " here: not made again by itself (an unmounted disk looks the same); "
                "`tsdb cluster replica reset PEER %" PRIu32 " %" PRIu32 "` makes a fresh replica",
                number, vdir, r->open_gen, r->rid, number, r->node);
            return -1;
        }
        return 0; /* a new replica: cvolfs makes it */
    }
    uint64_t rid = 0;
    if (cvolset_rid_of(vdir, &rid) != 0) {
        if (!reset)
            return 0; /* cvolfs names what is wrong with REPL and refuses the volume */
        rid = 0;      /* reset: its REPL was lost or damaged, which is why an operator reset it */
    } else if (rid == r->rid) {
        if (reset)
            cvolset_noted(number, CVS_RESET, NULL, 1);
        return 0;
    }
    /* A record that was opened here already (open_gen > 0) had its directory here: one of another rid in its place is
     * not stale but a wrong copy (a disk of another instance mounted, a restore), and a fresh one would hide the loss of
     * the recorded one. Only a replica the group never saw opened (an ADD after a REMOVE, a reset) replaces it; cvolfs
     * refuses this one as another instance, and `tsdb cluster replica reset` is the way out. */
    if (!reset && r->open_gen > 0)
        return 0;
    if (cvolfs_test_mutant == CVOLFS_MUT_STALEOPEN)
        return 0; /* planted: left in place (cvolfs refuses it as another instance) */
    const char *kind = reset && cvolfs_test_mutant != CVOLFS_MUT_LOSTASSTALE ? "lost" : "stale"; /* planted: stale */
    if (cvolset_setaside(root, number, kind, rid, to, sizeof to) != 0)
        return -1;
    if (reset)
        cvolset_noted(number, CVS_RESET, NULL, 1);
    say("volume %" PRIu32 ": %s held replica %016" PRIx64 ", the metadata group names %016" PRIx64 " for this server%s: "
        "set aside as %s (kept until an operator removes it), a fresh replica is made", number, vdir, rid, r->rid,
        reset ? " (a replica reset)" : "", to);
    return 0;
}
