/*
 * tsdbstat [settle|optimize]: tsdbd's storage statistics after a load (TSDB_HOST:TSDB_PORT).
 *   settle    polls until parts, merges and memtable rows have not changed for 3 s and prints
 *             when they last changed: the background work still running after the load
 *   optimize  times tsdbc_optimize (one part per partition), which is optional
 * It also prints a few series names, to confirm that ids match the file's series numbers.
 */
#include "tsdb_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }
static void show(tsdbc *c, const char *label)
{
    tsdb_stats s;
    tsdbc_stats(c, &s);
    printf("%s: parts %lu partitions %lu rows %lu bytes %lu (%.3f B/row) merges %lu memtable %lu\n", label, s.parts,
           s.partitions, s.rows, s.bytes, (double)s.bytes / s.rows, s.merges, s.memtable_rows);
}
int main(int argc, char **argv)
{
    tsdbc *c;
    const char *h = getenv("TSDB_HOST"), *p = getenv("TSDB_PORT");
    if (tsdbc_connect(h ? h : "127.0.0.1", p ? atoi(p) : 7070, &c)) return 1;
    show(c, "now");
    if (argc > 1 && !strcmp(argv[1], "settle")) {
        tsdb_stats a, b;
        tsdbc_stats(c, &a);
        double t0 = now(), last = t0;
        while (now() - last < 3) {
            usleep(50000);
            tsdbc_stats(c, &b);
            if (b.parts != a.parts || b.merges != a.merges || b.memtable_rows != a.memtable_rows) { last = now(); a = b; }
        }
        printf("settled %.2f s after the start of polling\n", last - t0);
        show(c, "settled");
    }
    if (argc > 1 && !strcmp(argv[1], "optimize")) {
        double t0 = now();
        int rc = tsdbc_optimize(c);
        printf("optimize: rc %d, %.2f s\n", rc, now() - t0);
        show(c, "optimized");
    }
    char name[64];
    for (uint32_t i = 0; i < 100; i += 33) { tsdbc_series_name(c, i, name, sizeof name); printf("id %u = %s\n", i, name); }
    tsdbc_close(c);
    return 0;
}
