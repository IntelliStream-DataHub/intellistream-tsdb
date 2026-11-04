/* tsdb as a server: write one day of readings through tsdbd and read them back.
 *
 *   build/tsdb /tmp/tsdb-qs/server user add admin --superuser --password-file /tmp/tsdb-qs/password
 *   build/tsdbd --dir /tmp/tsdb-qs/server --port 17070 &
 *   build/client-example 127.0.0.1 17070 admin /tmp/tsdb-qs/password
 *
 * The last two arguments are the user and a file whose first line is the password. Leave them out for a
 * tsdbd started with --no-auth. The C client does not link the storage engine; it only talks to the server. */
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsdb_calendar.h"
#include "tsdb_client.h"

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
            fprintf(stderr, "%s failed: %s\n", #call, tsdbc_strerror(err_)); \
            rc = 1;                                                         \
            goto done;                                                      \
        }                                                                   \
    } while (0)

int main(int argc, char **argv)
{
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? atoi(argv[2]) : 7070;
    tsdbc_options login = {0};
    login.user = argc > 3 ? argv[3] : NULL;          /* NULL: no login */
    login.password_file = argc > 4 ? argv[4] : NULL; /* never the password itself: argv is visible in ps */
    const int64_t start = 1717200000000; /* 2024-06-01 00:00:00 UTC, in milliseconds */
    const int64_t end = start + 86400000;

    int rc = 0;
    tsdbc *c = NULL;
    tsdb_bucket *hours = NULL, *days = NULL;
    CHECK(tsdbc_connect_opts(host, port, &login, &c)); /* on an error, CHECK prints it and jumps to done: below */

    tsdb_sid flow;
    CHECK(tsdbc_series(c, "pump7.flow", 1, &flow)); /* 1: create the series if it is missing */

    static int64_t times[N];
    static float values[N];
    for (int i = 0; i < N; i++) {
        times[i] = start + (int64_t)i * 60000;
        values[i] = 50.0f + 10.0f * sinf((float)i / 229.0f);
    }
    CHECK(tsdbc_insert_series(c, flow, times, values, N)); /* the server has it on disk when this returns */
    printf("wrote %d rows\n", N);

    struct seen seen = {0};
    CHECK(tsdbc_query_raw(c, flow, start, end, on_rows, &seen));
    printf("read %zu rows, the first: %" PRId64 " %.2f\n", seen.rows, seen.first_ts, seen.first_value);

    size_t nhours;
    CHECK(tsdbc_query_agg(c, flow, start, end, 3600000, &hours, &nhours)); /* one summary per hour */
    if (nhours > 0) /* an empty time range gives no buckets */
        printf("%zu hours, the first average: %.2f\n", nhours, tsdb_bucket_avg(&hours[0]));

    size_t ndays;
    tsdb_group day = {TSDB_DAY, 1, "Europe/Oslo"};
    CHECK(tsdbc_query_group(c, flow, start, end, &day, NULL, &days, &ndays)); /* one summary per Oslo day */
    for (size_t i = 0; i < ndays; i++)
        printf("Oslo day %zu: %" PRIu64 " rows, average %.2f\n", i + 1, days[i].count, tsdb_bucket_avg(&days[i]));

done:
    free(hours);
    free(days);
    if (c)
        tsdbc_close(c);
    return rc;
}
