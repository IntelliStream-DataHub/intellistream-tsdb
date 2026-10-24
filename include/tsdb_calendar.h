/*
 * Calendar grouping in a time zone, done where the time zone is known: in the
 * client. The database stores and groups UTC only; for "per day in
 * Europe/Oslo" this computes the UTC instant each local day begins and the
 * database groups by those bounds (tsdb_query_bounds, or QUERY_BOUNDS over
 * the network).
 */
#ifndef TSDB_CALENDAR_H
#define TSDB_CALENDAR_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* The UTC start (ms) of every period of `count` units (0 means 1) that
 * overlaps [start, end), in `zone`: NULL or "UTC", a fixed offset ("+02:00",
 * "-0530"), or an IANA name such as "Europe/Oslo" (from /usr/share/zoneinfo,
 * or $TZDIR). Calendar units are local periods (a day across a DST change
 * lasts 23 or 25 hours); seconds, minutes and hours are fixed lengths aligned
 * in the zone's standard time. Multiples count from 1970 in local time, like
 * ClickHouse's toStartOfInterval. A period runs from its first instant to the
 * next period's. *bounds is malloc'd (free it); TSDB_ERR_INVALID for an
 * unknown zone, TSDB_ERR_TOO_MANY_BUCKETS beyond 64 M periods. */
int tsdb_calendar_bounds(const char *zone, tsdb_unit unit, uint32_t count, int64_t start, int64_t end,
                         int64_t **bounds, size_t *n);

/* Embedded: tsdb_query_group_ex with a time zone, done with the above and
 * tsdb_query_bounds. An unbounded range is cut to the series' first and last
 * rows first. */
int tsdb_query_group_local(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                           const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);

/* Embedded: tsdb_query_twa_group with a time zone, through tsdb_query_twa_bounds.
 * An unbounded range is cut to where segments can cover time: from the first
 * reading in [start, end) (start itself when a reading before it carries a
 * value in) to the last one (end when a later reading ends its segment). */
int tsdb_query_twa_group_local(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                               const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);

#ifdef __cplusplus
}
#endif

#endif
