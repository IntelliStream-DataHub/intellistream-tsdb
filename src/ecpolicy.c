/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * When a part settles (R7, r7-settle; design-cluster-data.md E5, §2.4; the plan's D11). The settle thread
 * (src/settle.c) asks ecpolicy_pick_locked once per engine and round; it names one key and the parts of it to settle.
 *
 *  - Class 0 (one base partition, a day by default): the key settles whole once its end (partition_range's hi) is
 *    grace_s old by eng_now_ms and no merge of it is selectable: the parts of a day at rest are merged into as few
 *    as the merge policy makes (an idle partition is merged whole, merge_idle_s), so the settle encodes final parts
 *    and never an input that a merge would rewrite a minute later (merges skip settled parts: src/ecskip.h).
 *    "Selectable" is what the merge threads will do: with merge_threads = 0 nothing is (D11: no merges never stops
 *    settling), and with merge_idle_s < 0 only a merge in flight is (no part is rewritten alone and no pair merged
 *    as idle; a pair that the size and age rule takes some day is not waited for: the key may settle as two objects
 *    where a merge would have made one).
 *  - Classes 1-3 (7, 28 or 364 base partitions): such a key takes rows for weeks, so it settles part by part, each
 *    once its newest row is grace_s old, and at most once per coarse_s, counted from the settle time in the newest
 *    .ec record of the key (the record is on disk: the pace survives a restart; never an mtime).
 *  - Eager (tier eager=1, jepsen's): a part settles once its newest row is grace_s old, but only after a merge round
 *    has run on it (level > 0) or above ECPOLICY_EAGER_MIN_BYTES, and at most ECPOLICY_EAGER_PER_MIN settles a minute
 *    per engine, so that a jepsen run settles often without settling every flush.
 * Never: a key a drop is retiring (drop_pending: partition_dropping), a key with a settling or damaged part, a key
 * whose settle failed in the last hour (ECPOLICY_BACKOFF_MS; STOPPED is not a failure), a part that is merging, has no
 * granules or was carried over from format 8 (it is rewritten alone first: src/merge.c rewrite_alone).
 *
 * Memtable rows do not block a settle on one server: a row that arrives for a settled key later is flushed into a
 * new .part above the .ec parts (block numbers grow, and the .ec part keeps its source's: D2), and last write wins
 * as before; nothing in the object has to change. A cluster (R8) settles the leader's parts while the replicas may
 * still hold memtable rows of the key that only a later flush lists, and it needs settle_force_s (a bound after which
 * the key is settled anyway) to keep a key that never quiets down from never settling.
 */
#include "settle.h"
#include "ecpart.h"
#include "ecskip.h"
#include "tier.h"

static bool failed_recently(const ecpolicy_state *ps, int64_t key, int64_t now)
{
    for (size_t i = 0; i < ps->nfail; i++)
        if (ps->fail[i].key == key && now - ps->fail[i].at_ms < ECPOLICY_BACKOFF_MS)
            return true;
    return false;
}

void ecpolicy_failed(eng *db, int64_t key, int64_t now_ms)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    if (!ps)
        return;
    pthread_mutex_lock(&ps->pmu);
    size_t i = 0;
    while (i < ps->nfail && ps->fail[i].key != key)
        i++;
    if (i == ps->nfail) {
        if (ps->nfail == ECPOLICY_FAILS) { /* forget the oldest */
            size_t o = 0;
            for (size_t j = 1; j < ps->nfail; j++)
                if (ps->fail[j].at_ms < ps->fail[o].at_ms)
                    o = j;
            i = o;
        } else {
            ps->nfail++;
        }
    }
    ps->fail[i].key = key;
    ps->fail[i].at_ms = now_ms;
    pthread_mutex_unlock(&ps->pmu);
}

void ecpolicy_settled(eng *db, int64_t now_ms)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    if (!ps)
        return;
    pthread_mutex_lock(&ps->pmu);
    if (now_ms - ps->eager_win_ms >= 60000 || now_ms < ps->eager_win_ms) {
        ps->eager_win_ms = now_ms;
        ps->eager_n = 0;
    }
    ps->eager_n++;
    pthread_mutex_unlock(&ps->pmu);
}

/* a part a settle may take (what eng_settle pins) */
static bool settleable(const part *p)
{
    return !p->merging && !p->settling && !p->dropped && !part_is_ec(p) && !atomic_load(&p->damaged) && p->ng > 0 &&
           p->hdr_len == sizeof(part_header) && !p->pending[0];
}

static bool mergeable(const part *p)
{
    return !p->merging && !atomic_load(&p->damaged) && !part_merge_barrier(p);
}

/* Whether the merge thread may still select something in parts[a, b) (select_merge_locked's rules, idle: a merge in
 * flight, a part it rewrites alone, or two neighbours it may merge) */
static bool merge_selectable(const eng *db, size_t a, size_t b)
{
    if (db->opt.merge_threads <= 0) /* no merge thread: nothing will be merged, and a merge in flight is a call's */
        return false;
    bool idle = db->opt.merge_idle_s >= 0; /* else select_merge_locked never rewrites alone nor merges as idle */
    for (size_t i = a; i < b; i++) {
        const part *p = db->parts[i];
        if (p->merging)
            return true;
        if (!idle)
            continue;
        bool alone = (p->level == 0 && p->ng > 0) || p->hdr_len != sizeof(part_header);
        if (alone && !part_no_rewrite(p) && !atomic_load(&p->damaged))
            return true;
        if (i + 1 < b && mergeable(p) && mergeable(db->parts[i + 1]) &&
            (double)p->map_len + (double)db->parts[i + 1]->map_len <= (double)db->opt.max_part_bytes)
            return true;
    }
    return false;
}

size_t ecpolicy_pick_locked(eng *db, int64_t now, int64_t *key, part **sel, size_t cap)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    const tier_cfg *cfg = db->tier ? tier_config(db->tier) : NULL;
    if (!ps || !cfg || !cap)
        return 0;
    int64_t grace = cfg->grace_s * 1000, coarse = cfg->coarse_s * 1000;
    pthread_mutex_lock(&ps->pmu);
    ecpolicy_state snap = *ps; /* the fail list and the eager count (pmu is a leaf: not held across the walk) */
    pthread_mutex_unlock(&ps->pmu);
    if (cfg->eager && now - snap.eager_win_ms < 60000 && now >= snap.eager_win_ms &&
        snap.eager_n >= ECPOLICY_EAGER_PER_MIN)
        return 0;
    for (size_t a = 0; a < db->nparts;) {
        size_t b = a + 1;
        int64_t k = db->parts[a]->partition;
        while (b < db->nparts && db->parts[b]->partition == k)
            b++;
        size_t lo_i = a, hi_i = b;
        a = b;
        if (partition_dropping(db, k) || failed_recently(&snap, k, now))
            continue;
        bool blocked = false;
        int64_t newest_settle = INT64_MIN;
        for (size_t i = lo_i; i < hi_i; i++) {
            const part *p = db->parts[i];
            blocked |= p->settling || atomic_load(&p->damaged);
            struct ecpart *e = atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
            if (e && !e->test_mark)
                newest_settle = MAX(newest_settle, e->rec.settle_ms);
        }
        if (blocked)
            continue;
        int cls = partition_class(db, k);
        int64_t plo, phi;
        partition_range(db, k, &plo, &phi);
        size_t n = 0;
        if (cfg->eager) {
            for (size_t i = lo_i; i < hi_i && n < cap; i++) {
                part *p = db->parts[i];
                if (settleable(p) && p->max_ts <= now - grace &&
                    (p->level > 0 || p->map_len > ECPOLICY_EAGER_MIN_BYTES))
                    sel[n++] = p;
            }
        } else if (cls == 0) {
            if (phi == INT64_MAX || now - phi < grace)
                continue;
            if (merge_selectable(db, lo_i, hi_i) && settle_mutant != SETTLE_MUT_BUSY)
                continue;
            for (size_t i = lo_i; i < hi_i && n < cap; i++)
                if (settleable(db->parts[i]))
                    sel[n++] = db->parts[i];
        } else {
            if (newest_settle != INT64_MIN && now - newest_settle < coarse)
                continue;
            for (size_t i = lo_i; i < hi_i && n < cap; i++) {
                part *p = db->parts[i];
                if (settleable(p) && (p->max_ts <= now - grace || settle_mutant == SETTLE_MUT_COARSEWHOLE))
                    sel[n++] = p;
            }
        }
        if (!n)
            continue;
        for (size_t i = 0; i < n; i++)
            part_ref(sel[i]);
        *key = k;
        return n;
    }
    return 0;
}
