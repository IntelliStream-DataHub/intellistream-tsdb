/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the volume manager, the manager side (cvolset.h; R4 plan D12, D13): what the manager does
 * when a push no longer names this server for a volume, or names it with another rid (REPLICA_RESET).
 *
 * A push that does not name this server (CVS_UNNAMED) is a hint only: the directory goes once the metadata copy
 * (cluster_copy_get: applied, so committed, entries only) holds the volume WITHOUT this server. Then the volume is
 * closed (its driver stopped, its engine closed: clients get NOTFOUND for it from then on, as for any volume this
 * server does not hold, or MOVED/forwarded by route_decide to the copy's leader), v<N> is renamed
 * v<N>.removed-<rid>, the root fsynced (crash point set-removed after it), and the directory deleted in the background.
 * A removed member is never in the ISR, so the ISR members hold every committed position (D12).
 *
 * A reset (CVS_RESET) is noted with the new rid and answered CVS_AGAIN: the manager closes what is open
 * (cvolset_close) and prepares again, where cvolset_prepare sets the old directory aside as v<N>.lost-<rid>.
 */
#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "cvolset.h"
#include "cluster.h"
#include "crashpt.h"
#include "cvol.h"
#include "meta.h"
#include "repld.h"

#define say cvolfs_say

static char g_roots[META_DISKS_MAX][PATH_MAX];
static int g_nroots;

/* 1 if the metadata copy holds volume `number` and does not name `self` among its replicas */
static int removed_here(uint32_t number)
{
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return 0;
    int gone = 0;
    const struct cluster_copy_vol *v = cluster_copy_vol(c, number);
    if (v && c->self) {
        gone = 1;
        for (uint32_t i = 0; i < v->nrep; i++)
            if (v->rep[i].node == c->self)
                gone = 0;
    }
    cluster_copy_put(c);
    return gone;
}

static void remove_dir(const char *root, uint32_t number)
{
    char vdir[4200], to[4300];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, root, number);
    uint64_t rid = 0;
    struct stat sb;
    if (stat(vdir, &sb) != 0)
        return; /* nothing here (never made, or set aside before a crash) */
    cvolset_rid_of(vdir, &rid);
    if (cvolset_setaside(root, number, "removed", rid, to, sizeof to) != 0)
        return; /* the line was said; the next start's sweep tries again */
    crash_point(CP_SET_REMOVED);
    say("volume %" PRIu32 ": this server's replica was removed from the replica set: %s renamed %s, deleted in the "
        "background", number, vdir, to);
    cvolset_delete_bg(to);
}

int cvolset_record(int change, uint32_t number, const struct cluster_vol_rec *r)
{
    if (change == CVS_RESET && r) {
        say("volume %" PRIu32 ": the metadata group names replica %016" PRIx64 " for this server (a replica reset): "
            "the old one is closed and set aside, a fresh one is made", number, r->rid);
        cvolset_note(number, CVS_RESET, r->rid);
        return CVS_AGAIN;
    }
    if (change == CVS_UNNAMED) {
        if (!removed_here(number)) {
            say("volume %" PRIu32 ": the latest push does not name this server, the metadata copy does not show a "
                "removal: kept", number);
            return 0;
        }
        cvolset_note(number, CVS_UNNAMED, 0);
        return CVS_AGAIN;
    }
    return 0;
}

int cvolset_close(uint32_t number)
{
    int removed = cvolset_noted(number, CVS_UNNAMED, NULL, 0);
    if (removed && cvolfs_test_mutant == CVOLFS_MUT_REMOVEDOPEN) {
        cvolset_noted(number, CVS_UNNAMED, NULL, 1);
        return 0; /* planted: forgotten by the manager, the driver and engine left serving */
    }
    if (repld_close(number) != 0) {
        say("volume %" PRIu32 ": its driver could not be closed: kept", number);
        return -1;
    }
    if (removed) {
        cvolset_noted(number, CVS_UNNAMED, NULL, 1);
        char vroot[PATH_MAX];
        if (cvol_vol_disk(number, vroot, sizeof vroot, NULL) == 0 && vroot[0]) {
            remove_dir(vroot, number);
        } else {
            for (int i = 0; i < g_nroots; i++)
                remove_dir(g_roots[i], number);
        }
    }
    return 0;
}

static void sweep_one(const char *root)
{
    DIR *d = opendir(root);
    if (!d)
        return;
    uint32_t nums[256], nn = 0;
    uint32_t tmps[256], nt = 0;
    for (struct dirent *e; (errno = 0, e = readdir(d));) {
        char *end;
        if (e->d_name[0] != 'v' || e->d_name[1] < '0' || e->d_name[1] > '9')
            continue;
        unsigned long v = strtoul(e->d_name + 1, &end, 10);
        if (*end == 0 && v <= UINT32_MAX && nn < 256)
            nums[nn++] = (uint32_t)v;
        else if (!strcmp(end, ".tmp") && v <= UINT32_MAX && nt < 256)
            tmps[nt++] = (uint32_t)v;
        else if (!strncmp(end, ".removed-", 9)) { /* a removal's background delete a stop cut short */
            char p[4400];
            snprintf(p, sizeof p, "%s/%s", root, e->d_name);
            cvolset_delete_bg(p);
        } else if (!strncmp(end, ".fault-", 7)) { /* R9 (D6): faulted replica kept as evidence, never deleted */
            char p[4400];
            snprintf(p, sizeof p, "%s/%s", root, e->d_name);
            cvolset_log_fault_once((uint32_t)v, p);
        }
    }
    closedir(d);
    for (uint32_t i = 0; i < nn; i++)
        if (removed_here(nums[i]) && cvolfs_test_mutant != CVOLFS_MUT_STALEOPEN)
            remove_dir(root, nums[i]);
    for (uint32_t i = 0; i < nt; i++)
        if (removed_here(tmps[i]))
            (void)cvolfs_clean_tmp(root, tmps[i]);
}

void cvolset_sweep_disks(const char *const *roots, int nroots)
{
    g_nroots = 0;
    for (int i = 0; i < nroots && g_nroots < META_DISKS_MAX; i++) {
        if (!roots[i] || !roots[i][0])
            continue;
        snprintf(g_roots[g_nroots], sizeof g_roots[g_nroots], "%s", roots[i]);
        g_nroots++;
    }
    for (int i = 0; i < g_nroots; i++)
        sweep_one(g_roots[i]);
}

void cvolset_sweep(const char *root)
{
    if (!root || !root[0])
        return;
    cvolset_sweep_disks(&root, 1);
}
