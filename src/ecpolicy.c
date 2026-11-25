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
 *
 * On the leader of a cluster ec volume (R8, D21, r8g-cut; src/eccold.c the settle): whole keys only, of any class, once
 * the key's end is grace_s old by eng_now_ms (the leader's clock, which its NOW markers carry: E5) and no merge of it
 * is selectable; no per-part, coarse or eager settle; never an insert-written part (level 0, D18: it is rewritten alone
 * first); never a key some row of which expires within keep_s + repl_retain_s + 1 h (a replica lagging up to
 * repl_retain_s must not cross the expiry; repl_retain_s is the leader's keep cap, eng_leader_keep_cap, else 1800 s);
 * never on a diverged engine; and while the memtable may still hold rows of the key, not before the key's end is
 * grace_s + force_s old (the gate then flushes it). A settle that gave up (a position not UP) counts as failed: the
 * key waits an hour.
 */
#include "settle.h"
#include "eccold.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecskip.h"
#include "marker.h"
#include "tier.h"

#include <stdio.h>
#include <stdlib.h>

atomic_uint ecpolicy_objects_max = ECPOLICY_OBJECTS_MAX_DEFAULT;

static bool failed_recently(const ecpolicy_state *ps, int64_t key, int64_t now)
{
    for (size_t i = 0; i < ps->nfail; i++)
        if (ps->fail[i].key == key && now - ps->fail[i].at_ms < ECPOLICY_BACKOFF_MS)
            return true;
    return false;
}

static bool rfailed_recently(const ecpolicy_state *ps, int64_t key, int64_t now)
{
    for (size_t i = 0; i < ps->nrfail; i++)
        if (ps->rfail[i].key == key && now - ps->rfail[i].at_ms < ECPOLICY_BACKOFF_MS)
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

void ecpolicy_reencode_failed(eng *db, int64_t key, int64_t now_ms)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    if (!ps)
        return;
    pthread_mutex_lock(&ps->pmu);
    size_t i = 0;
    while (i < ps->nrfail && ps->rfail[i].key != key)
        i++;
    if (i == ps->nrfail) {
        if (ps->nrfail == ECPOLICY_FAILS) {
            size_t o = 0;
            for (size_t j = 1; j < ps->nrfail; j++)
                if (ps->rfail[j].at_ms < ps->rfail[o].at_ms)
                    o = j;
            i = o;
        } else {
            ps->nrfail++;
        }
    }
    ps->rfail[i].key = key;
    ps->rfail[i].at_ms = now_ms;
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

void ecpolicy_reencoded(eng *db, int64_t now_ms)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    if (!ps)
        return;
    pthread_mutex_lock(&ps->pmu);
    if (now_ms - ps->eager_rwin_ms >= 60000 || now_ms < ps->eager_rwin_ms) {
        ps->eager_rwin_ms = now_ms;
        ps->eager_rn = 0;
    }
    ps->eager_rn++;
    pthread_mutex_unlock(&ps->pmu);
}

/* a part a settle may take (what eng_settle pins) */
static bool settleable(const part *p)
{
    return !p->merging && !p->settling && !p->dropped && !part_is_ec(p) && !atomic_load(&p->damaged) && p->ng > 0 &&
           p->hdr_len == sizeof(part_header) && !p->pending[0];
}

/* A cluster leader's settle (R8, D21; eccold.c). Whether some row of parts[a, b) expires within margin of now (MK_NOW's
 * clock: eng_now_ms on a leader is what its NOW markers carry): a replica lagging up to repl_retain_s must not see the
 * key's rows expire while it still applies the settle. */
static bool expires_within(eng *db, size_t a, size_t b, int64_t now, int64_t margin)
{
    if (!atomic_load_explicit(&db->has_retention, memory_order_relaxed))
        return false;
    uint32_t gen = atomic_load(&db->ret_gen);
    for (size_t i = a; i < b; i++) {
        part *p = db->parts[i];
        if (p->ttl_gen != gen)
            ttl_compute_locked(db, p, gen);
        if (p->ttl_any != INT64_MAX && p->ttl_any - margin <= now)
            return true;
    }
    return false;
}

/* whether the memtable may still hold rows of [lo, hi] (db->lock held): a flush pending, or the active one's span */
static bool mt_holds_locked(eng *db, int64_t lo, int64_t hi)
{
    pthread_mutex_lock(&db->buf_lock);
    const memtable *m = db->active;
    bool any = db->nflushing > 0 || (m && m->rows > 0 && m->min_ts <= hi && m->max_ts >= lo);
    pthread_mutex_unlock(&db->buf_lock);
    return any;
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

bool ecpolicy_test_cap_hit;

size_t ecpolicy_pick_locked(eng *db, int64_t now, int64_t *key, part **sel, size_t cap)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    const tier_settings *cfg = db->tier ? tier_eng_settings(db) : NULL;
    if (!ps || !cfg || !cap)
        return 0;
    int64_t grace = cfg->grace_s * 1000, coarse = cfg->coarse_s * 1000;
    pthread_mutex_lock(&ps->pmu);
    ecpolicy_state snap = *ps; /* the fail list and the eager count (pmu is a leaf: not held across the walk) */
    pthread_mutex_unlock(&ps->pmu);
    /* R8: a cluster ec volume settles on its leader only (settle_engine asks), never on a diverged engine (item 9) */
    bool cluster = db->ec_cluster;
    if (cluster && atomic_load(&db->diverged))
        return 0;
    int64_t force = (cfg->force_s > 0 ? cfg->force_s : ECCOLD_FORCE_S_DEFAULT) * 1000;
    uint64_t cap_ms = atomic_load(&db->lead_cap_ms);
    int64_t retain = cap_ms ? (int64_t)cap_ms : ECCOLD_RETAIN_S_DEFAULT * 1000LL;
    int64_t margin = cfg->keep_s * 1000 + retain + 3600 * 1000LL;
    if (!cluster && cfg->eager && now - snap.eager_win_ms < 60000 && now >= snap.eager_win_ms &&
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
        if (cluster) { /* D21: whole keys of any class, no per-part, coarse or eager settle */
            if (phi == INT64_MAX || now - phi < grace)
                continue;
            if (merge_selectable(db, lo_i, hi_i) && settle_mutant != SETTLE_MUT_BUSY)
                continue;
            if (expires_within(db, lo_i, hi_i, now, margin))
                continue;
            /* the natural flush: the gate flushes the memtable only once force_s has passed */
            if (now - phi < grace + force && mt_holds_locked(db, plo, phi - 1))
                continue;
            /* the whole key or nothing: the parts it settles (I) and the rest of it (Y) must fit one marker; a key that
             * does not is said once and tried an hour later (the merges may make it smaller), never settled in part */
            size_t ni = 0, nall = 0;
            for (size_t i = lo_i; i < hi_i; i++) {
                const part *p = db->parts[i];
                nall += !part_is_ec(p);
                ni += settleable(db->parts[i]) && p->level > 0; /* D18: never an insert-written part */
            }
            if (ni > cap || nall > MK_FILES_MAX || ecpolicy_test_cap_hit) {
                if (ni) {
                    fprintf(stderr, "tsdb: %s: settle: key %lld has %zu parts to settle and %zu in all, more than one "
                                    "marker names (%zu, %d): tried again in an hour\n",
                            db->dir, (long long)k, ni, nall, cap, MK_FILES_MAX);
                    ecpolicy_failed(db, k, now);
                }
                continue;
            }
            for (size_t i = lo_i; i < hi_i; i++)
                if (settleable(db->parts[i]) && db->parts[i]->level > 0)
                    sel[n++] = db->parts[i];
        } else if (cfg->eager) {
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

/* ------------------------------------------------------------ R10 re-encode policy (E6) */

/* A part's full data bytes: an .ec part's map holds only its index, so its object_len bounds max_part_bytes. */
static uint64_t part_data_bytes(const part *p)
{
    struct ecpart *ep = atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
    return (ep && !ep->test_mark) ? ep->rec.object_len : (uint64_t)p->map_len;
}

/* Whether p is a real .ec part (not an in-memory eng_test_mark_ec flag whose file on disk is still .part). */
static bool part_is_real_ec(const part *p)
{
    struct ecpart *ep = atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
    return ep != NULL && !ep->test_mark;
}

/* Whether p may participate in a re-encode window: not busy/damaged/pending/test_mark, and if a .part, format 9 (v8
 * parts are rewritten alone first). Unlike settleable(), tombstone-only and empty .part files (ng == 0) are combinable. */
static bool reencodeable(const part *p)
{
    struct ecpart *ep = atomic_load_explicit(&((part *)p)->ec, memory_order_acquire);
    if (ep && ep->test_mark)
        return false;
    return !p->merging && !p->settling && !p->dropped && !atomic_load(&p->damaged) && !p->pending[0] &&
           (ep != NULL || p->hdr_len == sizeof(part_header));
}

/* Estimate rows of .ec part p hidden by the sorted/coalesced tombstones in ts, from the in-memory granule index
 * alone (never touches the chunk store). */
static uint64_t ec_hidden_rows_ts(const part *p, const tomb_set *ts)
{
    if (!p->ng || !ts->n)
        return 0;
    uint64_t hidden = 0;
    for (size_t ti = 0; ti < ts->n;) {
        uint32_t s = ts->t[ti].series;
        size_t tj = ti + 1;
        while (tj < ts->n && ts->t[tj].series == s)
            tj++;
        uint32_t ga = 0, gb = 0;
        gix_run(&p->ix, s, &ga, &gb);
        size_t tk = ti;
        for (uint32_t g = ga; g < gb && tk < tj; g++) {
            int64_t glo, ghi;
            gx_ts(p, g, &glo, &ghi);
            while (tk < tj && ts->t[tk].end <= glo)
                tk++;
            if (tk == tj || ts->t[tk].start > ghi)
                continue;
            uint32_t cnt = gx_count(p, g);
            uint64_t span = (uint64_t)ghi - (uint64_t)glo + 1;
            uint64_t ov = 0;
            for (size_t k = tk; k < tj && ts->t[k].start <= ghi; k++) {
                int64_t a = MAX(glo, ts->t[k].start);
                int64_t b = MIN(ghi, ts->t[k].end - 1);
                if (a <= b)
                    ov += (uint64_t)b - (uint64_t)a + 1;
            }
            if (ov >= span || span == 0)
                hidden += cnt;
            else if (ov > 0) {
                uint64_t est = (uint64_t)(((__uint128_t)cnt * ov) / span);
                hidden += est ? est : 1;
            }
        }
        ti = tj;
    }
    return MIN(hidden, p->rows);
}

/* Sum of part_data_bytes over parts[a .. b] inclusive; returns false if it exceeds max_bytes. */
static bool span_fits(part *const *parts, size_t a, size_t b, size_t limit, uint64_t max_bytes)
{
    if (b - a + 1 > limit)
        return false;
    uint64_t sum = 0;
    for (size_t i = a; i <= b; i++) {
        uint64_t sz = part_data_bytes(parts[i]);
        if (sz > max_bytes - sum)
            return false;
        sum += sz;
    }
    return true;
}

/* Evaluate all candidate start indices x in [g_lo, g_last] sharing the same right endpoint `end` in a single
 * right-to-left cumulative tomb_set pass (O(end - g_lo) instead of O(M^2)/O(M^3)). Sets *out_lo to the leftmost
 * triggering start index, matching a left-to-right scan. */
static bool eval_tomb_group(part *const *parts, size_t g_lo, size_t g_last, size_t end, size_t *out_lo)
{
    tomb_set ts = {0};
    bool dirty = false;
    uint64_t ec_rows = 0, hidden = 0;
    size_t pick_i = SIZE_MAX;
    for (size_t x = end; x-- > g_lo;) {
        if (x + 1 < end && parts[x + 1]->nt > 0) {
            if (tomb_set_add(&ts, parts[x + 1]->t, parts[x + 1]->nt) != 0) {
                tomb_set_free(&ts);
                return false;
            }
            dirty = true;
        }
        if (!part_is_ec(parts[x]))
            continue;
        ec_rows += parts[x]->rows;
        if (parts[x]->rows > 0 && ts.n > 0) {
            if (dirty) {
                tomb_set_finish(&ts);
                dirty = false;
            }
            hidden += ec_hidden_rows_ts(parts[x], &ts);
        }
        if (x <= g_last && parts[x]->rows > 0 && ec_rows > 0 &&
            (__uint128_t)hidden * 100 > (__uint128_t)ec_rows * ECPOLICY_TOMB_PCT)
            pick_i = x;
    }
    tomb_set_free(&ts);
    if (pick_i == SIZE_MAX)
        return false;
    *out_lo = pick_i;
    return true;
}

/* Scan a combinable run parts[0 .. len) for Trigger 2 using a two-pointer window bound and constant-end grouping. */
static bool pick_tomb_window(part *const *parts, size_t len, size_t limit, uint64_t max_bytes, size_t *out_lo,
                             size_t *out_hi)
{
    size_t last_tomb = 0;
    for (size_t i = len; i-- > 0;)
        if (parts[i]->nt > 0) {
            last_tomb = i;
            break;
        }
    if (last_tomb == 0)
        return false;
    size_t end = 0, tomb_cnt = 0;
    uint64_t sum = 0;
    size_t cur_end = 0, g_lo = SIZE_MAX, g_last = 0;
    for (size_t i = 0; i < last_tomb; i++) {
        if (end < i) {
            end = i;
            sum = 0;
            tomb_cnt = 0;
        }
        while (end < len && end - i < limit) {
            uint64_t sz = part_data_bytes(parts[end]);
            if (sz > max_bytes - sum)
                break;
            sum += sz;
            tomb_cnt += parts[end]->nt > 0;
            end++;
        }
        bool win_tombs = tomb_cnt > (size_t)(parts[i]->nt > 0);
        if (part_is_ec(parts[i]) && parts[i]->rows > 0 && end - i >= 2 && win_tombs) {
            if (g_lo == SIZE_MAX) {
                cur_end = end;
                g_lo = g_last = i;
            } else if (end == cur_end) {
                g_last = i;
            } else {
                size_t pick_i = 0;
                if (eval_tomb_group(parts, g_lo, g_last, cur_end, &pick_i)) {
                    *out_lo = pick_i;
                    *out_hi = cur_end;
                    return true;
                }
                cur_end = end;
                g_lo = g_last = i;
            }
        }
        if (end > i) {
            sum -= part_data_bytes(parts[i]);
            tomb_cnt -= parts[i]->nt > 0;
        }
    }
    if (g_lo != SIZE_MAX) {
        size_t pick_i = 0;
        if (eval_tomb_group(parts, g_lo, g_last, cur_end, &pick_i)) {
            *out_lo = pick_i;
            *out_hi = cur_end;
            return true;
        }
    }
    return false;
}

typedef struct {
    int64_t key;
    size_t off, len;
} reenc_run;

/* Whether a reencodeable run parts[r_lo .. r_hi) has any non-empty .ec part followed by a part with tombstones. */
static bool run_has_ec_tombs(part *const *parts, size_t r_lo, size_t r_hi)
{
    bool seen_ec = false;
    for (size_t i = r_lo; i < r_hi; i++) {
        if (seen_ec && parts[i]->nt > 0)
            return true;
        if (part_is_ec(parts[i]) && parts[i]->rows > 0)
            seen_ec = true;
    }
    return false;
}

size_t ecpolicy_pick_reencode(eng *db, int64_t now, int64_t *key, part **sel, size_t cap)
{
    ecpolicy_state *ps = ecpolicy_of(db);
    const tier_settings *cfg = db->tier ? tier_eng_settings(db) : NULL;
    if (!ps || !cfg || !cap || db->ec_cluster || db->cluster_req || db->replica_req || eng_jr(db) ||
        atomic_load(&db->diverged))
        return 0;
    pthread_mutex_lock(&ps->pmu);
    ecpolicy_state snap = *ps;
    pthread_mutex_unlock(&ps->pmu);
    if (cfg->eager && now - snap.eager_rwin_ms < 60000 && now >= snap.eager_rwin_ms &&
        snap.eager_rn >= ECPOLICY_EAGER_REENCODE_PER_MIN)
        return 0;
    unsigned obj_max = atomic_load(&ecpolicy_objects_max);
    if (obj_max == 0)
        obj_max = ECPOLICY_OBJECTS_MAX_DEFAULT;
    size_t limit = MIN(cap, (size_t)MAX_MERGE_PARTS);
    uint64_t max_bytes = db->opt.max_part_bytes;
    if (limit < 2 || max_bytes == 0)
        return 0;

    part **cand_parts = NULL;
    reenc_run *runs = NULL;
    size_t ncand = 0, nruns = 0;

    pthread_mutex_lock(&db->lock);
    for (size_t a = 0; a < db->nparts;) {
        size_t b = a + 1;
        int64_t k = db->parts[a]->partition;
        while (b < db->nparts && db->parts[b]->partition == k)
            b++;
        size_t lo_i = a, hi_i = b;
        a = b;
        if (hi_i - lo_i < 2 || partition_dropping(db, k) || rfailed_recently(&snap, k, now) ||
            eckeep_key_pending(db, k))
            continue;
        bool blocked = false, has_ec = false, has_tombs = false;
        for (size_t i = lo_i; i < hi_i; i++) {
            const part *p = db->parts[i];
            blocked |= p->settling || atomic_load(&p->damaged);
            has_ec |= part_is_real_ec(p);
            has_tombs |= p->nt > 0;
        }
        if (blocked || !has_ec)
            continue;

        /* Trigger 1 (E6): > ec_objects_max combinable .ec objects (only counting .ec objects that can combine with a
         * neighbour .ec inside max_part_bytes and MAX_MERGE_PARTS, so large objects never trigger forever). */
        size_t ncomb = 0, pick_lo = SIZE_MAX, pick_hi = 0;
        for (size_t r = lo_i; r < hi_i;) {
            while (r < hi_i && !reencodeable(db->parts[r]))
                r++;
            size_t r_lo = r;
            while (r < hi_i && reencodeable(db->parts[r]))
                r++;
            size_t r_hi = r;
            size_t prev_ec = SIZE_MAX;
            for (size_t i = r_lo; i < r_hi; i++) {
                if (!part_is_ec(db->parts[i]))
                    continue;
                size_t next_ec = SIZE_MAX;
                for (size_t j = i + 1; j < r_hi; j++)
                    if (part_is_ec(db->parts[j])) {
                        next_ec = j;
                        break;
                    }
                bool prev_ok = prev_ec != SIZE_MAX && span_fits(db->parts, prev_ec, i, limit, max_bytes);
                bool next_ok = next_ec != SIZE_MAX && span_fits(db->parts, i, next_ec, limit, max_bytes);
                if (prev_ok || next_ok)
                    ncomb++;
                if (pick_lo == SIZE_MAX && next_ok) {
                    /* Greedily extend rightward across combinable .ec and young .part files within the budget. */
                    uint64_t sum = 0;
                    for (size_t j = i; j <= next_ec; j++)
                        sum += part_data_bytes(db->parts[j]);
                    size_t end = next_ec + 1;
                    while (end < r_hi && end - i < limit) {
                        uint64_t sz = part_data_bytes(db->parts[end]);
                        if (sz > max_bytes - sum)
                            break;
                        sum += sz;
                        end++;
                    }
                    /* Also absorb contiguous young parts immediately before i if they fit. */
                    size_t start = i;
                    while (start > r_lo && end - start < limit) {
                        uint64_t sz = part_data_bytes(db->parts[start - 1]);
                        if (sz > max_bytes - sum)
                            break;
                        sum += sz;
                        start--;
                    }
                    pick_lo = start;
                    pick_hi = end;
                }
                prev_ec = i;
            }
            if (has_tombs && run_has_ec_tombs(db->parts, r_lo, r_hi)) {
                nruns++;
                ncand += r_hi - r_lo;
            }
        }
        if (ncomb > obj_max && pick_lo != SIZE_MAX) {
            size_t n = pick_hi - pick_lo;
            for (size_t i = 0; i < n; i++) {
                sel[i] = db->parts[pick_lo + i];
                part_ref(sel[i]);
            }
            pthread_mutex_unlock(&db->lock);
            *key = k;
            return n;
        }
    }

    /* Snapshot candidate runs for Trigger 2 so tombstone sorting and granule walks run outside db->lock. */
    size_t got_runs = 0, got_parts = 0;
    if (nruns > 0) {
        cand_parts = malloc(ncand * sizeof *cand_parts);
        runs = malloc(nruns * sizeof *runs);
        if (cand_parts && runs) {
            for (size_t a = 0; a < db->nparts && got_runs < nruns;) {
                size_t b = a + 1;
                int64_t k = db->parts[a]->partition;
                while (b < db->nparts && db->parts[b]->partition == k)
                    b++;
                size_t lo_i = a, hi_i = b;
                a = b;
                if (hi_i - lo_i < 2 || partition_dropping(db, k) || rfailed_recently(&snap, k, now) ||
                    eckeep_key_pending(db, k))
                    continue;
                bool blocked = false, has_ec = false, has_tombs = false;
                for (size_t i = lo_i; i < hi_i; i++) {
                    const part *p = db->parts[i];
                    blocked |= p->settling || atomic_load(&p->damaged);
                    has_ec |= part_is_real_ec(p);
                    has_tombs |= p->nt > 0;
                }
                if (blocked || !has_ec || !has_tombs)
                    continue;
                for (size_t r = lo_i; r < hi_i && got_runs < nruns;) {
                    while (r < hi_i && !reencodeable(db->parts[r]))
                        r++;
                    size_t r_lo = r;
                    while (r < hi_i && reencodeable(db->parts[r]))
                        r++;
                    size_t r_hi = r;
                    size_t rlen = r_hi - r_lo;
                    if (run_has_ec_tombs(db->parts, r_lo, r_hi) && got_parts + rlen <= ncand) {
                        runs[got_runs++] = (reenc_run){.key = k, .off = got_parts, .len = rlen};
                        for (size_t i = r_lo; i < r_hi; i++) {
                            part_ref(db->parts[i]);
                            cand_parts[got_parts++] = db->parts[i];
                        }
                    }
                }
            }
        }
    }
    pthread_mutex_unlock(&db->lock);

    /* Trigger 2 (E6): tombstones in newer combinable parts covering > 10% of combinable .ec objects' rows. */
    size_t picked = 0;
    for (size_t ri = 0; ri < got_runs; ri++) {
        size_t wlo = 0, whi = 0;
        if (pick_tomb_window(cand_parts + runs[ri].off, runs[ri].len, limit, max_bytes, &wlo, &whi)) {
            picked = whi - wlo;
            for (size_t i = 0; i < picked; i++) {
                sel[i] = cand_parts[runs[ri].off + wlo + i];
                cand_parts[runs[ri].off + wlo + i] = NULL; /* reference transferred to caller */
            }
            *key = runs[ri].key;
            break;
        }
    }
    for (size_t i = 0; i < got_parts; i++)
        if (cand_parts[i])
            part_unref(cand_parts[i]);
    free(cand_parts);
    free(runs);
    return picked;
}
