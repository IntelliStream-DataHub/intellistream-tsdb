/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * RowBinary (u32 series, i64 ts ms, f32 value, as written by tsdb-bench --export) -> bulk formats.
 *   conv lp    IN OUT   line protocol for InfluxDB 2 and 3 (measurement sensors, tag series, ms)
 *   conv ilp32 IN OUT   the same for QuestDB's FLOAT column
 *   conv tdlp  IN OUT   ilp32 with the suffix f32, which makes TDengine's schemaless writer create a FLOAT column
 *   conv csv   IN OUT   series,timestamptz,value for TimescaleDB's COPY
 *   conv om    IN OUT   OpenMetrics text for Prometheus' promtool tsdb create-blocks-from openmetrics:
 *                       gauge sensors{series="N"}, timestamps in seconds with milliseconds
 *
 * Values must stay exactly the float32 of the input. InfluxDB and Prometheus have only float64, so lp and om
 * writes the float32's exact double value, as the shortest text that parses back to that double;
 * ilp32 and csv write the shortest text that parses back to the float32 ("21.9", not
 * "21.8999996"), which is smaller and what an exporter of float32 data would print.
 * Single threaded; the conversion is not part of any load time.
 */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#pragma pack(push, 1)
typedef struct { uint32_t s; int64_t t; float v; } row;
#pragma pack(pop)
int main(int argc, char **argv)
{
    if (argc != 4) return 2;
    int fd = open(argv[2], O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0 || st.st_size == 0) {
        fprintf(stderr, "%s: missing, empty or unreadable\n", argv[2]);
        return 1;
    }
    const row *r = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    if (r == MAP_FAILED) {
        perror(argv[2]);
        return 1;
    }
    size_t n = st.st_size / sizeof(row);
    FILE *o = fopen(argv[3], "w");
    static char obuf[1 << 22];
    setvbuf(o, obuf, _IOFBF, sizeof obuf);
    int mode = !strcmp(argv[1], "lp") ? 0 : !strcmp(argv[1], "ilp32") ? 1 : !strcmp(argv[1], "om") ? 3 : !strcmp(argv[1], "tdlp") ? 4 : 2;
    if (mode == 3) fputs("# TYPE sensors gauge\n", o);
    char vb[40];
    int64_t last_sec = INT64_MIN;
    char secbuf[32] = "";
    for (size_t i = 0; i < n; i++) {
        /* shortest text that parses back to the same value: as a double for lp and om, as a float otherwise */
        if (mode == 0 || mode == 3) {
            double d = r[i].v;
            for (int p = 9; p <= 17; p++) {
                snprintf(vb, sizeof vb, "%.*g", p, d);
                if (strtod(vb, NULL) == d) break;
            }
        } else {
            for (int p = 6; p <= 9; p++) {
                snprintf(vb, sizeof vb, "%.*g", p, (double)r[i].v);
                if (strtof(vb, NULL) == r[i].v) break;
            }
        }
        if (mode < 2 || mode == 4)
            fprintf(o, "sensors,series=%u value=%s%s %lld\n", r[i].s, vb, mode == 4 ? "f32" : "", (long long)r[i].t);
        else if (mode == 3)
            fprintf(o, "sensors{series=\"%u\"} %s %lld.%03lld\n", r[i].s, vb, (long long)(r[i].t / 1000),
                    (long long)(r[i].t % 1000));
        else {
            int64_t sec = r[i].t / 1000, ms = r[i].t % 1000; /* all timestamps are positive */
            if (sec != last_sec) {
                time_t tt = sec;
                struct tm tm;
                gmtime_r(&tt, &tm);
                strftime(secbuf, sizeof secbuf, "%Y-%m-%d %H:%M:%S", &tm);
                last_sec = sec;
            }
            fprintf(o, "%u,%s.%03lld+00,%s\n", r[i].s, secbuf, (long long)ms, vb);
        }
    }
    if (mode == 3) fputs("# EOF\n", o);
    fclose(o);
    return 0;
}
