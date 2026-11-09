/* SPDX-License-Identifier: AGPL-3.0-only */
/* tsdb as a library: write one day of readings and read them back.
 *
 *   build/embedded-example /tmp/tsdb-qs/embedded
 *
 * Needs no server. The database is a directory; it is created if it is missing. */
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsdb.h"
#include "tsdb_calendar.h"

#define N 1440 /* one reading a minute for a day */

/* Called with blocks of rows. We only count them and remember the first one. */
struct seen {
    size_t rows;
    int64_t first_ts;
    float first_value;
};

static int on_rows(void *ctx, const int64_t *ts, const float *values, size_t n)
{
    struct seen *s = ctx;
    if (s->rows == 0 && n > 0) {
        s->first_ts = ts[0];
        s->first_value = values[0];
    }
    s->rows += n;
    return 0; /* non-zero would stop early */
}

#define CHECK(call)                                                         \
    do {                                                                    \
        int err_ = (call);                                                  \
        if (err_ < 0) {                                                     \
            fprintf(stderr, "%s failed: %s\n", #call, tsdb_strerror(err_)); \
            rc = 1;                                                         \
            goto done;                                                      \
        }                                                                   \
    } while (0)

int main(int argc, char **argv)
{
    const char *dir = argc > 1 ? argv[1] : "/tmp/tsdb-qs/embedded";
    const int64_t start = 1717200000000; /* 2024-06-01 00:00:00 UTC, in milliseconds */
    const int64_t end = start + 86400000;

    int rc = 0;
    tsdb *db = NULL;
    tsdb_bucket *hours = NULL, *days = NULL;
    CHECK(tsdb_open(dir, NULL, &db)); /* on an error, CHECK prints it and jumps to done: below */

    tsdb_sid flow;
    CHECK(tsdb_series(db, "pump7.flow", &flow)); /* finds the series, or creates it */

    static int64_t times[N];
    static float values[N];
    for (int i = 0; i < N; i++) {
        times[i] = start + (int64_t)i * 60000;
        values[i] = 50.0f + 10.0f * sinf((float)i / 229.0f);
    }
    CHECK(tsdb_insert_series(db, flow, times, values, N)); /* on disk when it returns */
    printf("wrote %d rows\n", N);

    struct seen seen = {0};
    CHECK(tsdb_query_raw(db, flow, start, end, on_rows, &seen));
    printf("read %zu rows, the first: %" PRId64 " %.2f\n", seen.rows, seen.first_ts, seen.first_value);

    size_t nhours;
    CHECK(tsdb_query_agg(db, flow, start, end, 3600000, &hours, &nhours)); /* one summary per hour */
    if (nhours > 0) /* an empty time range gives no buckets */
        printf("%zu hours, the first average: %.2f\n", nhours, tsdb_bucket_avg(&hours[0]));

    size_t ndays;
    tsdb_group day = {TSDB_DAY, 1, "Europe/Oslo"};
    CHECK(tsdb_query_group_local(db, flow, start, end, &day, NULL, &days, &ndays)); /* one summary per Oslo day */
    for (size_t i = 0; i < ndays; i++)
        printf("Oslo day %zu: %" PRIu64 " rows, average %.2f\n", i + 1, days[i].count, tsdb_bucket_avg(&days[i]));

done:
    free(hours);
    free(days);
    if (db)
        tsdb_close(db);
    return rc;
}
