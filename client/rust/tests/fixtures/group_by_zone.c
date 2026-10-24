/*
 * What the C client's tsdbc_query_group returns for local days, ISO weeks
 * and 6-hour buckets in three zones, over rows every 30 minutes around the
 * 2024 DST changes: the expectation of tests/zones.rs (group_by_zone.txt).
 *
 *   cc -Iinclude -Iclient -o /tmp/group_by_zone client/rust/tests/fixtures/group_by_zone.c \
 *       build/libtsdbclient.a -lzstd -lpthread
 *   build/tsdbd --dir /tmp/gbz --port 17171 & /tmp/group_by_zone 17171 > client/rust/tests/fixtures/group_by_zone.txt
 *
 * Output: "zone unit count:" and (start,count) per bucket.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsdb_client.h"

int main(int argc, char **argv)
{
    tsdbc *c;
    uint32_t id;
    if (argc != 2 || tsdbc_connect("127.0.0.1", atoi(argv[1]), &c) || tsdbc_series(c, "oslo", 1, &id))
        return 1;
    /* 2024-03-29 to 04-02 and 2024-10-25 to 10-29 UTC, every 30 minutes */
    int64_t ts[400], spans[2][2] = {{1711670400000LL, 1712016000000LL}, {1729814400000LL, 1730160000000LL}};
    float v[400];
    size_t n = 0;
    for (int s = 0; s < 2; s++)
        for (int64_t t = spans[s][0]; t < spans[s][1]; t += 1800000) {
            ts[n] = t;
            v[n++] = 1.0f;
        }
    if (tsdbc_insert_series(c, id, ts, v, n))
        return 2;
    const char *zones[] = {"Europe/Oslo", "America/St_Johns", "Asia/Kathmandu"};
    tsdb_unit units[] = {TSDB_DAY, TSDB_WEEK, TSDB_HOUR};
    for (int z = 0; z < 3; z++)
        for (int u = 0; u < 3; u++) {
            tsdb_group g = {units[u], u == 2 ? 6 : 1, zones[z]};
            tsdb_bucket *b;
            size_t nb;
            if (tsdbc_query_group(c, id, INT64_MIN, INT64_MAX, &g, NULL, &b, &nb))
                return 3;
            printf("%s %d %u:", zones[z], (int)units[u], g.count);
            for (size_t i = 0; i < nb; i++)
                printf(" (%lld,%llu)", (long long)b[i].start, (unsigned long long)b[i].count);
            printf("\n");
            free(b);
        }
    tsdbc_close(c);
    return 0;
}
