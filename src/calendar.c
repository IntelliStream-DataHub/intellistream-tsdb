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
    rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi + 1, &bounds, &n);
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
    rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi + 1, &bounds, &n);
    if (rc == 0)
        rc = tsdb_query_twa_bounds(db, series, start, end, bounds, n, opts, out, nout);
    free(bounds);
    return rc;
}
