/*
 * Many series in one call (tsdb_query_multi). The caller is the conductor: it
 * hands results to the callback in index order. Helpers of the database's
 * scheduler run series ahead of it, at most W = 2 x threads past the head,
 * each through the unchanged single-series function, with the call's
 * statistics (cur_qstats) and the call's cut of every volume (cur_cuts), so
 * all series read one instant.
 *
 * A series ahead of the head must not take more memory than a small share:
 * it runs with a budget (cur_budget). Where a query would allocate more
 * (bucket accumulators or LTTB's buffer over multi_defer_bytes, more than
 * multi_defer_rows collected rows, LTTB over multi_defer_rows points) it gives up with
 * TSDB_ERR_DEFER, and the conductor runs it again at its turn, streaming
 * rows as a single request does. Finished results wait in a ring of W slots;
 * what they hold is charged to max_held and to the caller's budget hooks,
 * and when either refuses, the call goes on one series at a time at the head.
 *
 * The conductor works too: while the head runs on a helper it runs a later
 * series itself, and waits (on_wait) only when there is nothing to claim.
 * Locks: c->m is a leaf; the callback and the queries run without it.
 */
#include "internal.h"
#include "tsdb_calendar.h"
#include "tsdb_protocol.h"

#include <stdlib.h>

__thread const qbudget *cur_budget;
size_t multi_defer_bytes = 16u << 20;
size_t multi_defer_rows = 65536;
size_t multi_max_bounds = (TSDB_MAX_FRAME - 64) / 8;
void (*_Atomic multi_open_hook)(void);

#define MULTI_HELD_DEFAULT ((size_t)64 << 20)

enum { S_FREE, S_RUNNING, S_DONE, S_DEFERRED };

/* a result collected ahead of its turn (and the head's buckets) */
typedef struct mres {
    int status;
    size_t n, cap;
    int type; /* VALUES, LTTB: of the values, -1 before the first row */
    int64_t *ts;
    uint8_t *vals;
    uint8_t *valid; /* NULL while every row is known */
    void *buckets;
    size_t bytes;   /* what it holds, charged to held */
    bool charged;   /* bytes were taken from the caller's budget */
} mres;

typedef struct mslot {
    int state;
    mres r;
} mslot;

typedef struct mcall {
    tsdb *db;
    const tsdb_multi_query *q;
    const tsdb_sid *series;
    size_t n;
    const tsdb_multi_opts *mo;
    tsdb_multi_fn fn;
    void *ctx;
    qstats *qs;
    qcuts *cuts;
    pthread_mutex_t m;
    pthread_cond_t cv;
    mslot *slots;
    size_t W;
    size_t head, next; /* next: the first series nobody claimed */
    size_t held, max_held;
    bool headonly, stop;
    uint64_t rows_sent, max_rows; /* the conductor's */
    int bounds_rc; /* q's bounds checked once for the call (bounds_check), not by each series' query */
    sjob job;
} mcall;

/* one series being answered: streamed to fn (at the head) or collected into r */
typedef struct msink {
    mcall *c;
    size_t index;
    bool stream;
    mres *r;
    int cb_rc; /* why the sink stopped the query: TSDB_ERR_STOPPED, _TOO_MANY_BUCKETS or _DEFER */
} msink;

static size_t api_width(int type)
{
    switch (type) {
    case TSDB_BOOL:
        return 1;
    case TSDB_FLOAT64:
    case TSDB_INT64:
        return 8;
    default:
        return 4;
    }
}

static void mres_free(mres *r)
{
    free(r->ts);
    free(r->vals);
    free(r->valid);
    free(r->buckets);
    memset(r, 0, sizeof *r);
}

/* appends n validity bits (src NULL: all set) at bit `at` of dst */
static void bits_append(uint8_t *dst, size_t at, const uint8_t *src, size_t n)
{
    if (src && at % 8 == 0) {
        memcpy(dst + at / 8, src, (n + 7) / 8);
        return;
    }
    for (size_t i = 0; i < n; i++) {
        size_t k = at + i;
        if (!src || (src[i >> 3] >> (i & 7) & 1))
            dst[k >> 3] |= (uint8_t)(1u << (k & 7));
        else
            dst[k >> 3] &= (uint8_t) ~(1u << (k & 7));
    }
}

static int mres_append(mres *r, const int64_t *ts, const tsdb_values *v, size_t n)
{
    if (r->type < 0)
        r->type = v->type;
    size_t w = api_width(r->type);
    if (r->n + n > r->cap) {
        size_t cap = MAX(r->n + n, r->cap ? 2 * r->cap : 256);
        int64_t *t = realloc(r->ts, cap * sizeof *t);
        if (t)
            r->ts = t;
        uint8_t *x = realloc(r->vals, cap * w);
        if (x)
            r->vals = x;
        if (!t || !x)
            return TSDB_ERR_NOMEM;
        if (r->valid) {
            uint8_t *b = realloc(r->valid, (cap + 7) / 8);
            if (!b)
                return TSDB_ERR_NOMEM;
            memset(b + (r->cap + 7) / 8, 0, (cap + 7) / 8 - (r->cap + 7) / 8);
            r->valid = b;
        }
        r->cap = cap;
    }
    if (v->valid && !r->valid) { /* the first null: the rows so far were all known */
        r->valid = calloc((r->cap + 7) / 8, 1);
        if (!r->valid)
            return TSDB_ERR_NOMEM;
        bits_append(r->valid, 0, NULL, r->n);
    }
    if (r->valid)
        bits_append(r->valid, r->n, v->valid, n);
    memcpy(r->ts + r->n, ts, n * sizeof *ts);
    memcpy(r->vals + r->n * w, v->values, n * w);
    r->n += n;
    r->bytes = r->cap * (sizeof *r->ts + w) + (r->valid ? (r->cap + 7) / 8 : 0);
    return 0;
}

static uint64_t rows_of(const mres *r) { return r->status ? 0 : r->n; }

static int sink_values(void *arg, const int64_t *ts, const tsdb_values *v, size_t n)
{
    msink *k = arg;
    mcall *c = k->c;
    if (n == 0)
        return 0;
    if (!k->stream) {
        if (k->r->n + n > multi_defer_rows) {
            k->cb_rc = TSDB_ERR_DEFER;
            return 1;
        }
        int rc = mres_append(k->r, ts, v, n);
        if (rc)
            k->cb_rc = rc;
        return rc != 0;
    }
    if (c->max_rows && c->rows_sent + n > c->max_rows) {
        k->cb_rc = TSDB_ERR_TOO_MANY_BUCKETS;
        return 1;
    }
    c->rows_sent += n;
    tsdb_series_result r = {k->index, c->series[k->index], 0, 0, n, ts, *v, NULL};
    if (c->fn(c->ctx, &r)) {
        k->cb_rc = TSDB_ERR_STOPPED;
        return 1;
    }
    return 0;
}

static int first_ts(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)v;
    if (n)
        *(int64_t *)ctx = ts[0];
    return 0;
}

/* the bounds of q that serve series id over [s, e) (TSDB_MULTI_TRIM_*) */
static int trim_bounds(mcall *c, tsdb_sid id, int64_t s, int64_t e, const int64_t **b, size_t *nb)
{
    const tsdb_multi_query *q = c->q;
    int64_t lo = s, hi = e == INT64_MIN ? INT64_MIN : e - 1;
    int rc = 0;
    if (q->trim & TSDB_MULTI_TRIM_LO) {
        tsdb_query_opts first = {TSDB_ASC, 1};
        lo = INT64_MAX;
        rc = tsdb_query_values(c->db, id, s, e, &first, first_ts, &lo);
    }
    if (rc == 0 && (q->trim & TSDB_MULTI_TRIM_HI)) {
        tsdb_query_opts last = {TSDB_DESC, 1};
        hi = INT64_MIN;
        rc = tsdb_query_values(c->db, id, s, e, &last, first_ts, &hi);
    }
    *nb = 0;
    if (rc || lo > hi)
        return rc;
    /* the last bound at or before x, 0 when none is */
    size_t a = 0, z = q->nbounds;
    while (a < z) {
        size_t mid = a + (z - a) / 2;
        if (q->bounds[mid] <= lo)
            a = mid + 1;
        else
            z = mid;
    }
    size_t i0 = a ? a - 1 : 0;
    a = i0, z = q->nbounds;
    while (a < z) {
        size_t mid = a + (z - a) / 2;
        if (q->bounds[mid] <= hi)
            a = mid + 1;
        else
            z = mid;
    }
    size_t i1 = a ? a - 1 : 0;
    *b = q->bounds + i0;
    *nb = i1 - i0 + 1;
    /* as many local periods as one client request carries at most, as the clients check */
    size_t cap = q->kind == TSDB_MULTI_TWA ? multi_max_bounds - TSDB_TWA_HEADER / 8 : multi_max_bounds;
    return *nb > cap ? TSDB_ERR_TOO_MANY_BUCKETS : 0;
}

/* Answers series index i into k: VALUES and LTTB through sink_values, buckets into k->r. Returns its status. */
static int run_one(mcall *c, size_t i, msink *k)
{
    const tsdb_multi_query *q = c->q;
    tsdb_sid id = c->series[i];
    int64_t s = q->starts ? q->starts[i] : q->start, e = q->ends ? q->ends[i] : q->end;
    int64_t iv = q->intervals ? q->intervals[i] : q->interval_ms;
    mres *r = k->r;
    int rc;
    if (sid_malformed(id))
        return TSDB_ERR_INVALID; /* a truncated id: its status says so, not an empty answer */
    query_note_series(c->db, id); /* the hot-reads list: sampled per series, the call's counters are one update */
    if (q->kind == TSDB_MULTI_VALUES || q->kind == TSDB_MULTI_LTTB) {
        if (q->kind == TSDB_MULTI_VALUES)
            rc = tsdb_query_values(c->db, id, s, e, &q->opts, sink_values, k);
        else if (!k->stream && MIN(q->threshold, q->opts.limit ? q->opts.limit : q->threshold) > multi_defer_rows)
            rc = TSDB_ERR_DEFER;
        else
            rc = tsdb_query_lttb_ex(c->db, id, s, e, q->threshold, &q->opts, sink_values, k);
        return k->cb_rc ? k->cb_rc : rc;
    }
    size_t nb = 0;
    void *out = NULL;
    size_t row = q->kind == TSDB_MULTI_BUCKETS  ? sizeof(tsdb_bucket)
                 : q->kind == TSDB_MULTI_SPREAD ? sizeof(tsdb_spread_bucket)
                                                : sizeof(tsdb_twa_bucket);
    if (q->bounds && q->nbounds) {
        const int64_t *b = q->bounds;
        size_t n = q->nbounds;
        if (c->bounds_rc)
            return c->bounds_rc; /* each series answers INVALID, as its own query would */
        rc = q->trim ? trim_bounds(c, id, s, e, &b, &n) : 0;
        if (rc || n == 0)
            return rc;
        rc = query_bounds_of(c->db, q->kind, id, s, e, b, n, true, &q->opts, &q->twa, &out, &nb);
    } else if (q->group) {
        if (q->kind == TSDB_MULTI_BUCKETS)
            rc = tsdb_query_group_local(c->db, id, s, e, q->group, &q->opts, (tsdb_bucket **)&out, &nb);
        else if (q->kind == TSDB_MULTI_SPREAD)
            rc = tsdb_query_spread_group_local(c->db, id, s, e, q->group, &q->opts, (tsdb_spread_bucket **)&out,
                                               &nb);
        else
            rc = tsdb_query_twa_group_local(c->db, id, s, e, q->group, &q->twa, (tsdb_twa_bucket **)&out, &nb);
    } else {
        if (q->kind == TSDB_MULTI_BUCKETS)
            rc = tsdb_query_agg_ex(c->db, id, s, e, iv, &q->opts, (tsdb_bucket **)&out, &nb);
        else if (q->kind == TSDB_MULTI_SPREAD)
            rc = tsdb_query_spread(c->db, id, s, e, iv, &q->opts, (tsdb_spread_bucket **)&out, &nb);
        else
            rc = tsdb_query_twa(c->db, id, s, e, iv, &q->twa, (tsdb_twa_bucket **)&out, &nb);
    }
    if (rc) {
        free(out);
        return rc;
    }
    r->buckets = out;
    r->n = nb;
    r->bytes = nb * row;
    return 0;
}

/* hands a finished result to fn: one call with last = 1 */
static int deliver(mcall *c, size_t i, const mres *r)
{
    uint64_t n = rows_of(r);
    if (c->max_rows && c->rows_sent + n > c->max_rows)
        return TSDB_ERR_TOO_MANY_BUCKETS;
    c->rows_sent += n;
    tsdb_series_result x = {i, c->series[i], r->status, 1, (size_t)n, NULL, {0, NULL, NULL}, NULL};
    if (n) {
        x.ts = r->ts;
        x.values.type = (tsdb_type)(r->type < 0 ? 0 : r->type);
        x.values.values = r->vals;
        x.values.valid = r->valid;
        x.buckets = r->buckets;
    }
    return c->fn(c->ctx, &x) ? TSDB_ERR_STOPPED : 0;
}

/* the head, on the conductor without a budget: rows stream as in a single request */
static int run_head(mcall *c, size_t i)
{
    mres r = {.type = -1};
    msink k = {c, i, true, &r, 0};
    int st = run_one(c, i, &k);
    int rc;
    if (k.cb_rc == TSDB_ERR_STOPPED || k.cb_rc == TSDB_ERR_TOO_MANY_BUCKETS)
        rc = k.cb_rc; /* fn stopped the call, or max_rows did */
    else {
        r.status = st;
        if (st || r.buckets || (c->q->kind != TSDB_MULTI_VALUES && c->q->kind != TSDB_MULTI_LTTB))
            rc = deliver(c, i, &r);
        else { /* the rows went out already: the closing call */
            tsdb_series_result x = {i, c->series[i], 0, 1, 0, NULL, {0, NULL, NULL}, NULL};
            rc = c->fn(c->ctx, &x) ? TSDB_ERR_STOPPED : 0;
        }
    }
    mres_free(&r);
    return rc;
}

/* claims the next series to run ahead of the head (c->m held) */
static bool claim_ahead(mcall *c, size_t *i)
{
    if (c->stop || c->headonly || c->next >= c->n || c->next >= c->head + c->W || c->held >= c->max_held)
        return false;
    *i = c->next++;
    c->slots[*i % c->W].state = S_RUNNING;
    return true;
}

/* series i ahead of the head, with a budget; the result waits in its slot */
static void run_ahead(mcall *c, size_t i)
{
    mslot *sl = &c->slots[i % c->W];
    mres r = {.type = -1};
    msink k = {c, i, false, &r, 0};
    qbudget b = {multi_defer_bytes};
    const qbudget *saved = cur_budget;
    cur_budget = &b;
    int st = run_one(c, i, &k);
    cur_budget = saved;
    const tsdb_multi_opts *mo = c->mo;
    bool ok = st != TSDB_ERR_DEFER, refused = false;
    if (ok && st) { /* a failed series holds nothing: not counted in held, not charged to the budget */
        int type = r.type;
        mres_free(&r);
        r.type = type;
    }
    if (ok && r.bytes && mo && mo->budget_take && mo->budget_give) {
        if (mo->budget_take(mo->hook_ctx, r.bytes) == 0)
            r.charged = true;
        else
            ok = false, refused = true;
    }
    pthread_mutex_lock(&c->m);
    if (ok && c->held + r.bytes > c->max_held && c->held > 0)
        ok = false, refused = true; /* the head always fits: held is 0 when it waits for it */
    if (ok) {
        r.status = st;
        c->held += r.bytes;
        sl->r = r;
        sl->state = S_DONE;
    } else {
        sl->state = S_DEFERRED;
        if (refused)
            c->headonly = true; /* the memory for results ahead is gone: one series at a time */
    }
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->m);
    if (!ok && mo && mo->on_deferred)
        mo->on_deferred(mo->hook_ctx);
    if (!ok) {
        if (r.charged)
            mo->budget_give(mo->hook_ctx, r.bytes);
        mres_free(&r);
    }
}

static void task_fn(void *arg, int id)
{
    (void)id;
    mcall *c = arg;
    qstats *saved_qs = cur_qstats;
    qcuts *saved_cuts = cur_cuts;
    cur_qstats = c->qs;
    cur_cuts = c->cuts;
    pthread_mutex_lock(&c->m);
    size_t i;
    while (claim_ahead(c, &i)) {
        pthread_mutex_unlock(&c->m);
        run_ahead(c, i);
        int lv = sched_leave(); /* another request may need this helper, or the call was stopped */
        pthread_mutex_lock(&c->m);
        if (lv)
            break;
    }
    pthread_mutex_unlock(&c->m);
    cur_qstats = saved_qs;
    cur_cuts = saved_cuts;
}

static void slot_release(mcall *c, mslot *sl)
{
    const tsdb_multi_opts *mo = c->mo;
    c->held -= sl->r.bytes;
    if (sl->r.charged)
        mo->budget_give(mo->hook_ctx, sl->r.bytes);
    mres_free(&sl->r);
    sl->state = S_FREE;
}

static void reopen(mcall *c)
{
    pthread_mutex_lock(&c->m);
    bool more = !c->stop && !c->headonly && c->next < c->n && c->next < c->head + c->W && c->held < c->max_held;
    pthread_mutex_unlock(&c->m);
    if (more)
        sched_reopen(&c->job);
}

static int conduct(mcall *c)
{
    const tsdb_multi_opts *mo = c->mo;
    int rc = 0;
    for (;;) {
        if (mo && mo->cancelled && mo->cancelled(mo->hook_ctx)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        pthread_mutex_lock(&c->m);
        if (c->head == c->n) {
            pthread_mutex_unlock(&c->m);
            break;
        }
        size_t h = c->head;
        mslot *sl = &c->slots[h % c->W];
        if (sl->state == S_FREE || sl->state == S_DEFERRED) {
            if (sl->state == S_FREE && c->next == h)
                c->next++; /* nobody claimed it: head == next (but series 0, which the caller claimed at the start) */
            sl->state = S_RUNNING;
            pthread_mutex_unlock(&c->m);
            rc = run_head(c, h);
            pthread_mutex_lock(&c->m);
            sl->state = S_FREE;
            c->head++;
            pthread_mutex_unlock(&c->m);
        } else if (sl->state == S_DONE) {
            mres r = sl->r;
            pthread_mutex_unlock(&c->m);
            rc = deliver(c, h, &r);
            pthread_mutex_lock(&c->m);
            slot_release(c, sl);
            c->head++;
            pthread_mutex_unlock(&c->m);
        } else {
            size_t i;
            if (claim_ahead(c, &i)) { /* the head is on a helper: work on a later one meanwhile */
                pthread_mutex_unlock(&c->m);
                run_ahead(c, i);
                continue;
            }
            pthread_mutex_unlock(&c->m);
            if (mo && mo->on_wait)
                mo->on_wait(mo->hook_ctx, 1);
            pthread_mutex_lock(&c->m);
            while (sl->state == S_RUNNING)
                pthread_cond_wait(&c->cv, &c->m);
            pthread_mutex_unlock(&c->m);
            if (mo && mo->on_wait)
                mo->on_wait(mo->hook_ctx, -1);
            continue;
        }
        if (rc)
            break;
        reopen(c);
    }
    return rc;
}

static int clamp_threads(tsdb *db, const tsdb_multi_opts *mo)
{
    int rt = sched_request_threads(query_sched(db));
    int t = mo && mo->threads > 0 ? mo->threads : rt;
    return MAX(1, MIN(t, rt));
}

int tsdb_query_multi(tsdb *db, const tsdb_multi_query *q, const tsdb_sid *series, size_t n,
                     const tsdb_multi_opts *mo, tsdb_multi_fn fn, void *ctx)
{
    if (!db || !q || !fn || (n && !series) || q->kind < TSDB_MULTI_VALUES || q->kind > TSDB_MULTI_LTTB ||
        (q->nbounds && !q->bounds) || (q->trim & ~(TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI)))
        return TSDB_ERR_INVALID;
    if (n == 0)
        return 0;
    int threads = clamp_threads(db, mo);
    mcall c = {.db = db, .q = q, .series = series, .n = n, .mo = mo, .fn = fn, .ctx = ctx};
    /* once, not per series: 65,536 series over 33 M bounds (one frame of them) were 250 s of comparisons */
    c.bounds_rc = q->nbounds ? bounds_check(q->bounds, q->nbounds) : 0;
    c.W = 2 * (size_t)threads;
    c.max_held = mo && mo->max_held_bytes ? mo->max_held_bytes : MULTI_HELD_DEFAULT;
    c.max_rows = mo ? mo->max_rows : 0;
    c.slots = calloc(c.W, sizeof *c.slots);
    if (!c.slots)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&c.m, NULL);
    pthread_cond_init(&c.cv, NULL);
    /* one series alone reads one snapshot anyway, unless trimming reads it three times */
    qcuts cuts;
    bool cut = n > 1 || (q->trim && q->nbounds);
    if (cut)
        qcuts_init(&cuts);
    qstats *saved_qs = cur_qstats;
    qcuts *saved_cuts = cur_cuts;
    const qbudget *saved_budget = cur_budget;
    cur_budget = NULL;
    qrun qr;
    query_run_begin(db, &qr, threads);
    c.qs = &qr.q;
    c.cuts = cut ? &cuts : NULL;
    cur_cuts = c.cuts;
    /* The caller claims series 0 before any helper wakes. A helper that ran before the caller's first look at the
     * head would claim it as a series ahead: collected and held (a series over 65,536 rows computed and thrown
     * away, to run again at its turn), when the caller would stream it at once. Helpers start at series 1. */
    c.next = 1;
    /* helpers only run series ahead of the head: one series alone wakes none (a wakeup costs more than a
     * small query), though its own parallel work still has the group's cap */
    sched_open(query_sched(db), &c.job, (int)MIN((size_t)threads, n), task_fn, &c);
    void (*hook)(void) = atomic_load(&multi_open_hook);
    if (hook)
        hook();
    int rc = conduct(&c);
    if (rc) {
        pthread_mutex_lock(&c.m);
        c.stop = true;
        pthread_mutex_unlock(&c.m);
        sched_cancel(&qr.g); /* series in flight on helpers end at their next cancel point */
    }
    sched_close(&c.job);
    for (size_t i = 0; i < c.W; i++)
        if (c.slots[i].state == S_DONE)
            slot_release(&c, &c.slots[i]);
    query_run_end(&qr, -1, c.rows_sent);
    cur_qstats = saved_qs;
    cur_cuts = saved_cuts;
    cur_budget = saved_budget;
    if (cut)
        qcuts_free(&cuts);
    pthread_cond_destroy(&c.cv);
    pthread_mutex_destroy(&c.m);
    free(c.slots);
    return rc;
}

/* ------------------------------------------------------------ collect */

typedef struct mcollect {
    tsdb_series_result *out;
    mres cur; /* the rows of the series being streamed */
    size_t cur_index;
    bool have;
    size_t row;
    int rc;
} mcollect;

static int collect_fn(void *ctx, const tsdb_series_result *r)
{
    mcollect *k = ctx;
    if (!k->have || k->cur_index != r->index) {
        mres_free(&k->cur);
        k->cur.type = -1;
        k->cur_index = r->index;
        k->have = true;
    }
    if (r->n && r->ts) {
        int rc = mres_append(&k->cur, r->ts, &r->values, r->n);
        if (rc) {
            k->rc = rc;
            return 1;
        }
    }
    if (!r->last)
        return 0;
    tsdb_series_result *o = &k->out[r->index];
    o->index = r->index;
    o->series = r->series;
    o->status = r->status;
    o->last = 1;
    if (r->buckets && r->n) {
        void *b = malloc(r->n * k->row);
        if (!b) {
            k->rc = TSDB_ERR_NOMEM;
            return 1;
        }
        memcpy(b, r->buckets, r->n * k->row);
        o->buckets = b;
        o->n = r->n;
    } else if (k->cur.n) {
        o->n = k->cur.n;
        o->ts = k->cur.ts;
        o->values.type = (tsdb_type)k->cur.type;
        o->values.values = k->cur.vals;
        o->values.valid = k->cur.valid;
        memset(&k->cur, 0, sizeof k->cur); /* now the result's */
    }
    k->have = false;
    return 0;
}

int tsdb_query_multi_collect(tsdb *db, const tsdb_multi_query *q, const tsdb_sid *series, size_t n,
                             const tsdb_multi_opts *mo, tsdb_series_result **out)
{
    *out = NULL;
    if (!q)
        return TSDB_ERR_INVALID;
    mcollect k = {0};
    k.cur.type = -1;
    k.row = q->kind == TSDB_MULTI_BUCKETS  ? sizeof(tsdb_bucket)
            : q->kind == TSDB_MULTI_SPREAD ? sizeof(tsdb_spread_bucket)
                                           : sizeof(tsdb_twa_bucket);
    k.out = calloc(n ? n : 1, sizeof *k.out);
    if (!k.out)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++) {
        k.out[i].index = i;
        k.out[i].series = series ? series[i] : 0;
    }
    int rc = tsdb_query_multi(db, q, series, n, mo, collect_fn, &k);
    if (rc == TSDB_ERR_STOPPED && k.rc)
        rc = k.rc;
    mres_free(&k.cur);
    *out = k.out;
    return rc;
}

void tsdb_series_results_free(tsdb_series_result *r, size_t n)
{
    if (!r)
        return;
    for (size_t i = 0; i < n; i++) {
        free((void *)r[i].ts);
        free((void *)r[i].values.values);
        free((void *)r[i].values.valid);
        free((void *)r[i].buckets);
    }
    free(r);
}
