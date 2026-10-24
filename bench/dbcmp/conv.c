/*
 * RowBinary (u32 series, i64 ts ms, f32 value, as written by tsdb-bench --export) -> bulk formats.
 *   conv lp    IN OUT   line protocol for InfluxDB 2 and 3 (measurement sensors, tag series, ms)
 *   conv ilp32 IN OUT   the same for QuestDB's FLOAT column
 *   conv csv   IN OUT   series,timestamptz,value for TimescaleDB's COPY
 *
 * Values must stay exactly the float32 of the input. InfluxDB has only float64 fields, so lp
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
    fstat(fd, &st);
    const row *r = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    size_t n = st.st_size / sizeof(row);
    FILE *o = fopen(argv[3], "w");
    static char obuf[1 << 22];
    setvbuf(o, obuf, _IOFBF, sizeof obuf);
    int mode = !strcmp(argv[1], "lp") ? 0 : !strcmp(argv[1], "ilp32") ? 1 : 2;
    char vb[40];
    int64_t last_sec = INT64_MIN;
    char secbuf[32] = "";
    for (size_t i = 0; i < n; i++) {
        /* shortest text that parses back to the same value: as a double for lp, as a float otherwise */
        if (mode == 0) {
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
        if (mode < 2)
            fprintf(o, "sensors,series=%u value=%s %lld\n", r[i].s, vb, (long long)r[i].t);
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
    fclose(o);
    return 0;
}
