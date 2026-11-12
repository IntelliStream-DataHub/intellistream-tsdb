/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdb_query_group_local: grouping in a time zone for programs that embed
 * the database. The engine knows UTC only; this is the client's part (the
 * bounds of the local periods, tsdb_calendar_bounds) done in-process;
 * tsdb_query_spread_group_local and tsdb_query_twa_group_local likewise for
 * the spread and time-weighted averages.
 */
#include "internal.h"

#include "tsdb_calendar.h"

#include <stdlib.h>

static int local_range(tsdb *db, tsdb_sid series, int64_t start, int64_t end, int64_t *lo, int64_t *hi, bool *cut);

static int first_ts(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)v;
    if (n)
        *(int64_t *)ctx = ts[0];
    return 1; /* one row is all it takes */
}

/* Local periods are listed bound by bound, so a sparse series over a long
 * span (two rows years apart, grouped by the second) would list millions of
 * bounds for two buckets. They are made a window of about local_piece_bounds
 * periods at a time instead, each window queried on its own, and the windows
 * jump over time without rows: memory follows the buckets that have rows.
 * Windows end on period starts, so every period lies whole in one window and
 * the buckets, and the order and limit across windows, are those of one query
 * over every bound (only empty buckets, never returned, are skipped). */
size_t local_piece_bounds;
size_t local_twa_listed; /* bounds a local TWA has listed so far (once per window); tests */
#define LOCAL_MAX_BUCKETS (4u << 20) /* query.c MAX_BUCKETS */
#define TWA_LOCAL_MAX_BOUNDS ((4u << 20) + 2) /* twa.c: at most TWA_MAX_BUCKETS buckets between its data's ends */

/* Nominal length of `count` periods, in ms (0: too long to matter). */
static int64_t period_ms(const tsdb_group *g)
{
    static const int64_t len[] = {[TSDB_SECOND] = 1000,         [TSDB_MINUTE] = 60000,
                                  [TSDB_HOUR] = 3600000,        [TSDB_DAY] = 86400000,
                                  [TSDB_WEEK] = 7 * 86400000LL, [TSDB_MONTH] = 31 * 86400000LL,
                                  [TSDB_QUARTER] = 92 * 86400000LL, [TSDB_YEAR] = 366 * 86400000LL};
    if (g->unit < TSDB_SECOND || g->unit > TSDB_YEAR)
        return 0;
    int64_t r;
    if (__builtin_mul_overflow(len[g->unit], (int64_t)(g->count ? g->count : 1), &r))
        return 0;
    return r;
}

/* The bounds of a window of periods from t on (desc: up to t): [t, t + span), clipped to lim, or the window
 * reaching lim (*final). A window that is not final has at least two bounds, the last (desc: first) of which
 * begins the next window. */
static int window_bounds(const tsdb_group *g, bool desc, int64_t t, int64_t lim, int64_t **b, size_t *n,
                         bool *final)
{
    size_t k = local_piece_bounds ? local_piece_bounds : 65536;
    int64_t len = period_ms(g), span;
    if (len == 0 || __builtin_mul_overflow(len, (int64_t)k, &span))
        span = INT64_MAX;
    for (;;) {
        int64_t w;
        *final = desc ? __builtin_sub_overflow(t, span, &w) || w <= lim : __builtin_add_overflow(t, span, &w) ||
                                                                              w >= lim;
        if (*final)
            w = lim;
        int rc = desc ? tsdb_calendar_bounds(g->timezone, g->unit, g->count, w, t, b, n)
                      : tsdb_calendar_bounds(g->timezone, g->unit, g->count, t, w, b, n);
        if (rc || *final || *n >= 2)
            return rc;
        free(*b); /* periods longer than their nominal length: a wider window */
        span = span > INT64_MAX / 2 ? INT64_MAX : span * 2;
    }
}

/* Buckets (spread false: tsdb_bucket) or the spread (tsdb_spread_bucket) of local periods. */
static int group_local(tsdb *db, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                       const tsdb_query_opts *opts, bool spread, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (sid_malformed(series))
        return TSDB_ERR_INVALID;
    if (!group || !group->timezone || strcmp(group->timezone, "UTC") == 0)
        return spread ? tsdb_query_spread_group(db, series, start, end, group, opts, (tsdb_spread_bucket **)out, nout)
                      : tsdb_query_group_ex(db, series, start, end, group, opts, (tsdb_bucket **)out, nout);
    /* bounds are listed one by one: cut an open range to the rows there are */
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    tsdb_query_opts first = {TSDB_ASC, 1}, last = {TSDB_DESC, 1};
    int rc = tsdb_query_values(db, series, start, end, &first, first_ts, &lo);
    if (rc == 0 || rc == TSDB_ERR_STOPPED)
        rc = tsdb_query_values(db, series, start, end, &last, first_ts, &hi);
    if (rc && rc != TSDB_ERR_STOPPED)
        return rc;
    if (lo > hi)
        return 0; /* no rows */
    bool cut;
    if ((rc = local_range(db, series, start, end, &lo, &hi, &cut)))
        return rc;
    /* The windows keep the memory to the buckets with rows, but a query spans no more periods than one in UTC may
     * (query.c MAX_BUCKETS), counted at their nominal length. */
    int64_t len = period_ms(group);
    if (len > 0 && ((uint64_t)hi - (uint64_t)lo) / (uint64_t)len >= LOCAL_MAX_BUCKETS)
        return TSDB_ERR_TOO_MANY_BUCKETS;
    bool desc = opts && opts->order == TSDB_DESC;
    size_t limit = opts ? opts->limit : 0, size = spread ? sizeof(tsdb_spread_bucket) : sizeof(tsdb_bucket);
    /* asc: windows from the first row on, [qs, qe) the rows of one; desc: back from the last row */
    int64_t t = desc ? hi + 1 : lo, qs = start, qe = end;
    char *res = NULL;
    size_t nres = 0;
    for (;;) {
        int64_t *bounds;
        size_t n;
        bool final;
        rc = window_bounds(group, desc, t, desc ? lo : hi + 1, &bounds, &n, &final);
        if (rc)
            break;
        if (cut && n && (desc ? final : t == lo)) /* the first period: rows before the calendar's limit join it */
            bounds[0] = INT64_MIN;
        const int64_t *bp = bounds;
        if (!final && desc) /* the first period may begin before the window: the next window has it */
            qs = bounds[1], bp++, n--;
        else if (!final) /* the last period may end after the window: the next window has it */
            qe = bounds[--n];
        else if (desc)
            qs = start;
        else
            qe = end;
        tsdb_query_opts o = {desc ? TSDB_DESC : TSDB_ASC, limit ? limit - nres : 0};
        void *b = NULL;
        size_t nb = 0;
        rc = spread ? tsdb_query_spread_bounds(db, series, qs, qe, bp, n, &o, (tsdb_spread_bucket **)&b, &nb)
                    : tsdb_query_bounds(db, series, qs, qe, bp, n, &o, (tsdb_bucket **)&b, &nb);
        free(bounds);
        if (rc)
            break;
        if (!res) {
            res = b, nres = nb;
        } else if (nb) {
            char *r = realloc(res, (nres + nb) * size);
            if (!r) {
                free(b);
                rc = TSDB_ERR_NOMEM;
                break;
            }
            memcpy(r + nres * size, b, nb * size);
            res = r, nres += nb;
            free(b);
        } else {
            free(b);
        }
        if (final || (limit && nres >= limit))
            break;
        /* the next window begins at the next row: none in between, and no bucket */
        int64_t nx = desc ? INT64_MIN : INT64_MAX;
        rc = desc ? tsdb_query_values(db, series, start, qs, &last, first_ts, &nx)
                  : tsdb_query_values(db, series, qe, end, &first, first_ts, &nx);
        if (rc && rc != TSDB_ERR_STOPPED)
            break;
        rc = 0;
        if (desc ? nx == INT64_MIN || nx >= qs : nx == INT64_MAX || nx < qe)
            break; /* no more rows */
        if (desc)
            t = nx + 1, qe = qs;
        else
            t = nx, qs = qe;
    }
    if (rc) {
        free(res);
        return rc;
    }
    *out = res;
    *nout = nres;
    return 0;
}

int tsdb_query_group_local(tsdb *db, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                           const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    return group_local(db, series, start, end, group, opts, false, (void **)out, nout);
}

int tsdb_query_spread_group_local(tsdb *db, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                                  const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    return group_local(db, series, start, end, group, opts, true, (void **)out, nout);
}

/* The time of the newest (desc) or oldest reading in [start, end), if any. */
static int edge_ts(tsdb *db, tsdb_sid series, int64_t start, int64_t end, tsdb_order order, int64_t *ts, bool *found)
{
    int64_t t = INT64_MIN;
    tsdb_query_opts o = {order, 1};
    *found = false;
    if (start >= end)
        return 0;
    /* first_ts keeps INT64_MIN when there is no row; a row at INT64_MIN cannot be told apart, nor matter */
    int rc = tsdb_query_values(db, series, start, end, &o, first_ts, &t);
    if (rc && rc != TSDB_ERR_STOPPED)
        return rc;
    *found = rc == TSDB_ERR_STOPPED;
    *ts = t;
    return 0;
}

int tsdb_query_twa_group_local(tsdb *db, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                               const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (sid_malformed(series))
        return TSDB_ERR_INVALID;
    if (!group || !group->timezone || strcmp(group->timezone, "UTC") == 0)
        return tsdb_query_twa_group(db, series, start, end, group, opts, out, nout);
    tsdb_type type;
    int rc = tsdb_series_type(db, series, &type);
    if (rc)
        return rc;
    if (type == TSDB_STATE)
        return TSDB_ERR_INVALID;
    /* Segments cover time between the first and the last reading in range,
     * and beyond them up to start (end) when a reading before (after) the
     * range carries the value in (ends the last segment). */
    int64_t lo = 0, hi = 0, t;
    bool in, before, after;
    if ((rc = edge_ts(db, series, start, end, TSDB_ASC, &lo, &in)) ||
        (rc = edge_ts(db, series, INT64_MIN, start, TSDB_DESC, &t, &before)) ||
        (rc = edge_ts(db, series, end, INT64_MAX, TSDB_ASC, &t, &after)))
        return rc;
    if (in)
        rc = edge_ts(db, series, start, end, TSDB_DESC, &hi, &in);
    if (rc)
        return rc;
    if (before)
        lo = start;
    if (after)
        hi = end - 1;
    if (!in && !(before && after))
        return 0; /* nothing in range, and no segment crosses it */
    bool cut;
    if ((rc = local_range(db, series, start, end, &lo, &hi, &cut)))
        return rc;
    /* Segments fill every period they cross, so the bounds are listed in full, but no more of them than a TWA
     * answers (twa.c TWA_MAX_BUCKETS, by the span of its buckets): not 512 MB of bounds to be refused after. */
    /* Local periods are seldom longer than nominal (a day of 25 hours, a day repeated where a zone crossed the
     * date line), never four times over a whole span: one that long holds too many whatever the zone, refused
     * before any bound is listed. */
    int64_t len = period_ms(group);
    if (len > 0 && hi >= lo && ((uint64_t)hi - (uint64_t)lo) / 4 / (uint64_t)len > TWA_LOCAL_MAX_BOUNDS)
        return TSDB_ERR_TOO_MANY_BUCKETS;
    int64_t *bounds = NULL;
    t = lo;
    size_t n = 0, cap = 0;
    for (bool final = false; !final;) {
        int64_t *b;
        size_t m;
        if ((rc = window_bounds(group, false, t, hi + 1, &b, &m, &final)))
            break;
        if (!final)
            t = b[--m]; /* the next window begins with it */
        __atomic_fetch_add(&local_twa_listed, m, __ATOMIC_RELAXED);
        int64_t *r = bounds;
        if (n + m > TWA_LOCAL_MAX_BOUNDS) {
            r = NULL;
        } else if (n + m > cap) { /* doubling: an allocator that copies on realloc copies each bound twice at most */
            size_t c = cap * 2 > n + m ? cap * 2 : n + m;
            c = c < TWA_LOCAL_MAX_BOUNDS ? c : TWA_LOCAL_MAX_BOUNDS;
            if ((r = realloc(bounds, c * sizeof *bounds)))
                cap = c;
        }
        if (r)
            memcpy(r + n, b, m * sizeof *b), bounds = r, n += m;
        free(b);
        if (!r) {
            rc = n + m <= TWA_LOCAL_MAX_BOUNDS ? TSDB_ERR_NOMEM : TSDB_ERR_TOO_MANY_BUCKETS;
            break;
        }
    }
    if (rc == 0 && cut && n)
        bounds[0] = INT64_MIN; /* rows before the calendar's limit join the first period */
    if (rc == 0)
        rc = tsdb_query_twa_bounds(db, series, start, end, bounds, n, opts, out, nout);
    free(bounds);
    return rc;
}

/* Moves the edges lo and hi (inclusive) of a local grouping within the calendar's limits. The calendar places
 * instants within TSDB_CALENDAR_LIMIT_MS of 1970 only, and lists every period between: an edge beyond moves in to
 * the first (last) row within the limits, rows before them join the first period, which then begins at INT64_MIN
 * (*cut), and rows after them the last, which runs to the end anyway. One such row used to make every grouping over
 * an open range fail. */
static int local_range(tsdb *db, tsdb_sid series, int64_t start, int64_t end, int64_t *lo, int64_t *hi, bool *cut)
{
    const int64_t lim = TSDB_CALENDAR_LIMIT_MS;
    *cut = *lo < -lim;
    if (*cut || *lo >= lim || *hi < -lim || *hi >= lim) {
        int64_t ilo = 0, ihi = 0, s = start > -lim ? start : -lim, e = end < lim ? end : lim;
        bool inner = false;
        int rc = edge_ts(db, series, s, e, TSDB_ASC, &ilo, &inner);
        if (rc == 0 && inner)
            rc = edge_ts(db, series, s, e, TSDB_DESC, &ihi, &inner);
        if (rc)
            return rc;
        if (*lo < -lim || *lo >= lim)
            *lo = inner ? ilo : (*lo < -lim ? -lim : lim - 1);
        if (*hi < -lim || *hi >= lim)
            *hi = inner ? ihi : *lo;
        if (*hi < *lo)
            *hi = *lo;
    }
    return 0;
}
