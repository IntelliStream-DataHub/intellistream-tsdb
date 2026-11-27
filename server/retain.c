/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Per-server WAL retention budget for DOWN or held members (server/retain.h; R9 E29, PLAN D16).
 */
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "retain.h"

struct retain_slot {
    uint32_t vol;
    uint64_t excess;
    int active;
    int evicted;
};

static pthread_mutex_t ret_mu = PTHREAD_MUTEX_INITIALIZER; /* leaf mutex: never held across I/O or other locks */
static struct retain_slot ret_slots[CLUSTER_REPORT_VOLS];
static uint32_t ret_nslots;
static _Atomic uint64_t ret_total_excess;

#ifdef TSDB_TEST_HOOKS
static int ret_override_set;
static uint64_t ret_override_bytes;

static void parse_override(const char *env)
{
    if (!env)
        return;
    for (const char *p = env; p && *p;) {
        unsigned long long v = 0;
        if (sscanf(p, "retain_down_bytes=%llu", &v) == 1) {
            ret_override_set = 1;
            ret_override_bytes = (uint64_t)v;
        } else if (sscanf(p, "retain_down_kb=%llu", &v) == 1) {
            ret_override_set = 1;
            ret_override_bytes = (uint64_t)v << 10;
        } else if (sscanf(p, "retain_down_mb=%llu", &v) == 1) {
            ret_override_set = 1;
            ret_override_bytes = (uint64_t)v << 20;
        }
        p = strchr(p, ',');
        if (p)
            p++;
    }
}
#endif

void retain_init(void)
{
    pthread_mutex_lock(&ret_mu);
    ret_nslots = 0;
    atomic_store_explicit(&ret_total_excess, 0, memory_order_release);
#ifdef TSDB_TEST_HOOKS
    ret_override_set = 0;
    ret_override_bytes = 0;
    parse_override(getenv("TSDB_CLUSTER_TEST"));
    parse_override(getenv("TSDB_REPLD_TEST"));
#endif
    pthread_mutex_unlock(&ret_mu);
}

void retain_reset(void)
{
    pthread_mutex_lock(&ret_mu);
    ret_nslots = 0;
    atomic_store_explicit(&ret_total_excess, 0, memory_order_release);
    pthread_mutex_unlock(&ret_mu);
}

uint64_t retain_wall_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

uint64_t retain_budget_bytes(uint32_t repl_retain_down_gb)
{
    if (repl_retain_down_gb == 0)
        return 0;
#ifdef TSDB_TEST_HOOKS
    if (ret_override_set)
        return ret_override_bytes;
#endif
    return (uint64_t)repl_retain_down_gb << 30;
}

int retain_vol_wants(const struct cluster_vol_rec *rec, int self, const int *was_down, const uint64_t *peer_acked,
                     const int *peer_heard, int is_writer, uint64_t log_start, uint64_t wall_ms, int keep_after_out,
                     uint32_t *down_ids, uint32_t *ndown_out)
{
    uint32_t nd = 0;
    for (uint32_t i = 0; i < rec->nrep && i < META_REPLICAS_MAX; i++) {
        if ((int)i == self)
            continue;
        uint8_t st = rec->rep[i].state;
        uint64_t hold = rec->rep[i].hold_until_ms;
        int in_isr = 0;
        for (uint32_t j = 0; j < rec->nisr && j < META_REPLICAS_MAX; j++)
            if (rec->isr[j] == rec->rep[i].node)
                in_isr = 1;
        int held = st != META_OUT && st != META_DESTROYED && hold > wall_ms;
        int want = 0;
        if (st == META_DOWN)
            want = 1;
        else if (held && !in_isr)
            want = 1;
        else if (st == META_UP && was_down && was_down[i] && !in_isr)
            want = 1;
        else if (keep_after_out && st == META_OUT)
            want = 1;
        if (!want)
            continue;
        if (is_writer && peer_heard && peer_acked && peer_heard[i] && log_start > 1 && peer_acked[i] + 1 < log_start)
            continue; /* leader already dropped past what this peer needs: it must install a snapshot */
        if (down_ids)
            down_ids[nd] = rec->rep[i].node;
        nd++;
    }
    if (ndown_out)
        *ndown_out = nd;
    return nd > 0;
}

static struct retain_slot *slot_find_locked(uint32_t vol, int create)
{
    for (uint32_t i = 0; i < ret_nslots; i++)
        if (ret_slots[i].vol == vol)
            return &ret_slots[i];
    if (!create || ret_nslots >= CLUSTER_REPORT_VOLS)
        return NULL;
    struct retain_slot *s = &ret_slots[ret_nslots++];
    memset(s, 0, sizeof *s);
    s->vol = vol;
    return s;
}

static uint64_t recompute_sum_locked(void)
{
    uint64_t sum = 0;
    for (uint32_t i = 0; i < ret_nslots; i++)
        if (ret_slots[i].active && !ret_slots[i].evicted)
            sum += ret_slots[i].excess;
    return sum;
}

void retain_report(uint32_t vol, uint64_t excess, uint64_t budget_bytes, int active, int nocap, int *evicted_out,
                   uint64_t *rem_out, uint32_t *evicted_vols, uint32_t *nevicted, uint32_t evcap)
{
    uint32_t nev = 0;
    int ev = 0;
    uint64_t rem = 0;
    pthread_mutex_lock(&ret_mu);
    struct retain_slot *s = slot_find_locked(vol, active);
    if (!s) {
        ev = active && !nocap && budget_bytes == 0;
        pthread_mutex_unlock(&ret_mu);
        if (evicted_out)
            *evicted_out = ev;
        if (rem_out)
            *rem_out = 0;
        if (nevicted)
            *nevicted = 0;
        return;
    }
    if (!active) {
        s->active = 0;
        s->excess = 0;
        s->evicted = 0;
        atomic_store_explicit(&ret_total_excess, recompute_sum_locked(), memory_order_release);
        pthread_mutex_unlock(&ret_mu);
        if (evicted_out)
            *evicted_out = 0;
        if (rem_out)
            *rem_out = 0;
        if (nevicted)
            *nevicted = 0;
        return;
    }
    s->active = 1;
    if (budget_bytes == 0 && !nocap) {
        if (!s->evicted && evicted_vols && nev < evcap)
            evicted_vols[nev++] = s->vol;
        s->evicted = 1;
        s->excess = 0;
        atomic_store_explicit(&ret_total_excess, recompute_sum_locked(), memory_order_release);
        ev = 1;
        pthread_mutex_unlock(&ret_mu);
        if (evicted_out)
            *evicted_out = ev;
        if (rem_out)
            *rem_out = 0;
        if (nevicted)
            *nevicted = nev;
        return;
    }
    if (s->evicted && !nocap) {
        s->excess = 0;
        atomic_store_explicit(&ret_total_excess, recompute_sum_locked(), memory_order_release);
        pthread_mutex_unlock(&ret_mu);
        if (evicted_out)
            *evicted_out = 1;
        if (rem_out)
            *rem_out = 0;
        if (nevicted)
            *nevicted = 0;
        return;
    }
    s->excess = excess;
    uint64_t sum = recompute_sum_locked();
    if (!nocap) {
        while (sum > budget_bytes) {
            int best = -1;
            uint64_t best_ex = 0;
            for (uint32_t i = 0; i < ret_nslots; i++) {
                if (ret_slots[i].active && !ret_slots[i].evicted && ret_slots[i].excess > best_ex) {
                    best = (int)i;
                    best_ex = ret_slots[i].excess;
                }
            }
            if (best < 0)
                break;
            ret_slots[best].evicted = 1;
            sum -= ret_slots[best].excess;
            ret_slots[best].excess = 0;
            if (evicted_vols && nev < evcap)
                evicted_vols[nev++] = ret_slots[best].vol;
        }
    }
    atomic_store_explicit(&ret_total_excess, sum, memory_order_release);
    ev = s->evicted;
    if (!ev) {
        uint64_t others = sum >= s->excess ? sum - s->excess : 0;
        rem = budget_bytes > others ? budget_bytes - others : 0;
    }
    pthread_mutex_unlock(&ret_mu);
    if (evicted_out)
        *evicted_out = ev;
    if (rem_out)
        *rem_out = rem;
    if (nevicted)
        *nevicted = nev;
}

void retain_clear_evicted(uint32_t vol)
{
    pthread_mutex_lock(&ret_mu);
    struct retain_slot *s = slot_find_locked(vol, 0);
    if (s)
        s->evicted = 0;
    pthread_mutex_unlock(&ret_mu);
}

void retain_forget(uint32_t vol)
{
    pthread_mutex_lock(&ret_mu);
    for (uint32_t i = 0; i < ret_nslots; i++) {
        if (ret_slots[i].vol == vol) {
            ret_slots[i] = ret_slots[--ret_nslots];
            break;
        }
    }
    atomic_store_explicit(&ret_total_excess, recompute_sum_locked(), memory_order_release);
    pthread_mutex_unlock(&ret_mu);
}

uint64_t retain_total_excess(void)
{
    return atomic_load_explicit(&ret_total_excess, memory_order_acquire);
}
