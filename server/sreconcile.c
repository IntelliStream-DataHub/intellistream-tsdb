/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 startup and return chunk reconciler (server/sfill.h; track r9-reconcile; R9 plan D15; design-cluster-data.md
 * §5.2).
 *
 * Compares local CHUNK_LIST against the volume leader's committed OBJLIST on every start and return: missing chunks
 * (degraded settles, fsck, replaced stores) are handed to sfill at their margin's priority, and retired chunks are
 * left to committed MK_GC.
 *
 * No-op stub until r9-reconcile fills it in.
 */
#include "sfill.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

/* Bounded in-memory ring of missing positions noted by degraded settles (E10); not persisted across restarts because
 * r9-reconcile rebuilds missing positions from committed .ec records' `positions` bitmaps on startup/return. */
#define SRECONCILE_MISSING_CAP 4096

struct sreconcile_missing {
    uint32_t volume;
    uint8_t oid[16];
    unsigned pos;
};

static pthread_mutex_t missing_mu = PTHREAD_MUTEX_INITIALIZER;
static struct sreconcile_missing missing_ring[SRECONCILE_MISSING_CAP];
static size_t missing_head;
static uint64_t missing_total;

int sreconcile_trigger(struct sfill *sf, uint32_t volume, uint32_t pos, uint64_t stripe_version)
{
    (void)sf;
    (void)volume;
    (void)pos;
    (void)stripe_version;
    return 0;
}

void sreconcile_on_start(struct sfill *sf)
{
    (void)sf;
}

void sreconcile_note_missing(uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    if (!oid)
        return;
    pthread_mutex_lock(&missing_mu);
    struct sreconcile_missing *m = &missing_ring[missing_head % SRECONCILE_MISSING_CAP];
    m->volume = volume;
    memcpy(m->oid, oid, 16);
    m->pos = pos;
    missing_head++;
    missing_total++;
    pthread_mutex_unlock(&missing_mu);
    char hex[33];
    for (int i = 0; i < 16; i++)
        snprintf(hex + 2 * i, 3, "%02x", oid[i]);
    fprintf(stderr, "tsdbd: volume %" PRIu32 ": reconcile noted missing position %u of object %s\n", volume, pos, hex);
}

uint64_t sreconcile_missing_count(void)
{
    pthread_mutex_lock(&missing_mu);
    uint64_t n = missing_total;
    pthread_mutex_unlock(&missing_mu);
    return n;
}
