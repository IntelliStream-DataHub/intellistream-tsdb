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

static int local_bounds(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group, int64_t lo,
                        int64_t hi, int64_t **bounds, size_t *n);

static int first_ts(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)v;
    if (n)
        *(int64_t *)ctx = ts[0];
    return 1; /* one row is all it takes */
}

/* Buckets (spread false: tsdb_bucket) or the spread (tsdb_spread_bucket) of local periods. */
static int group_local(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                       const tsdb_query_opts *opts, bool spread, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
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
    int64_t *bounds;
    size_t n;
    rc = local_bounds(db, series, start, end, group, lo, hi, &bounds, &n);
    if (rc == 0)
        rc = spread ? tsdb_query_spread_bounds(db, series, start, end, bounds, n, opts, (tsdb_spread_bucket **)out,
                                               nout)
                    : tsdb_query_bounds(db, series, start, end, bounds, n, opts, (tsdb_bucket **)out, nout);
    free(bounds);
    return rc;
}

int tsdb_query_group_local(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                           const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    return group_local(db, series, start, end, group, opts, false, (void **)out, nout);
}

int tsdb_query_spread_group_local(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                                  const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    return group_local(db, series, start, end, group, opts, true, (void **)out, nout);
}

/* The time of the newest (desc) or oldest reading in [start, end), if any. */
static int edge_ts(tsdb *db, uint32_t series, int64_t start, int64_t end, tsdb_order order, int64_t *ts, bool *found)
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

int tsdb_query_twa_group_local(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                               const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
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
    int64_t *bounds;
    size_t n;
    rc = local_bounds(db, series, start, end, group, lo, hi, &bounds, &n);
    if (rc == 0)
        rc = tsdb_query_twa_bounds(db, series, start, end, bounds, n, opts, out, nout);
    free(bounds);
    return rc;
}

/* The bounds of the local periods from lo to hi (inclusive). The calendar places instants within
 * TSDB_CALENDAR_LIMIT_MS of 1970 only, and lists every period between: an edge beyond moves in to the first (last)
 * row within the limits, rows before them join the first period, which then begins at INT64_MIN, and rows after
 * them the last, which runs to the end anyway. One such row used to make every grouping over an open range fail. */
static int local_bounds(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group, int64_t lo,
                        int64_t hi, int64_t **bounds, size_t *n)
{
    const int64_t lim = TSDB_CALENDAR_LIMIT_MS;
    bool cut = lo < -lim;
    if (cut || lo >= lim || hi < -lim || hi >= lim) {
        int64_t ilo = 0, ihi = 0, s = start > -lim ? start : -lim, e = end < lim ? end : lim;
        bool inner = false;
        int rc = edge_ts(db, series, s, e, TSDB_ASC, &ilo, &inner);
        if (rc == 0 && inner)
            rc = edge_ts(db, series, s, e, TSDB_DESC, &ihi, &inner);
        if (rc)
            return rc;
        if (lo < -lim || lo >= lim)
            lo = inner ? ilo : (lo < -lim ? -lim : lim - 1);
        if (hi < -lim || hi >= lim)
            hi = inner ? ihi : lo;
        if (hi < lo)
            hi = lo;
    }
    int rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi + 1, bounds, n);
    if (rc == 0 && cut && *n)
        (*bounds)[0] = INT64_MIN;
    return rc;
}
