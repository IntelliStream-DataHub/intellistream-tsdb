/*
 * The big buffers of a flush and of a memtable: what the other threads wait on (flush-tail).
 *
 * A memtable that sees a million series has a 40 MB array of per-series state, and mt_reserve grows it under buf_lock,
 * which every append and lookup takes. realloc and a memset of the new tail touched all of its pages there (9 ms with
 * the lock held, twice, right after a flush gave the appenders a new memtable); the array is now an anonymous mapping
 * (zero pages, grown by mremap), so a row for a high series id touches only its own page. And the large buffers of a
 * flush are handed back in slices (tsdb_free_big) so that munmap holds the mmap lock for nothing.
 *
 *   ./build/test_flush_tail
 *
 * With -DOLD_CODE (main's memtable.c and compat.h) the part about the free is left out, and the RSS check fails.
 */
#include "internal.h"
#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

/* resident memory of this process, bytes */
static long rss(void)
{
    FILE *f = fopen("/proc/self/statm", "r");
    long a = 0, b = 0;
    if (f) {
        if (fscanf(f, "%ld %ld", &a, &b) != 2)
            b = 0;
        fclose(f);
    }
    return b * sysconf(_SC_PAGESIZE);
}

static void add(memtable *m, uint32_t id, int64_t ts, double x)
{
    uint64_t slot;
    memcpy(&slot, &x, 8);
    CHECK(mt_add(m, NULL, id, &ts, &slot, 8, 1, 0, -1) == 0);
}

static int has(const memtable *m, uint32_t id, int64_t ts, double x)
{
    mem_run r = {0};
    int ok = 0;
    if (mt_collect(m, id, 8, INT64_MIN, INT64_MAX, UINT64_MAX, &r) == 0 && r.n == 1 && r.ts[0] == ts) {
        double y;
        memcpy(&y, r.v, 8);
        ok = y == x;
    }
    mem_run_free(&r);
    return ok;
}

static void test_series_array(void)
{
    memtable *m = mt_new(1);
    CHECK(m != NULL);
    add(m, 3, 100, 1.5);
    long r0 = rss();
    /* the first row of a memtable for a high id (what an appender sends right after a flush): the array grows to
     * 1,000,001 entries, 40 MB. The old code wrote every page of it under buf_lock */
    add(m, 1000000, 200, 2.5);
    long grown = rss() - r0;
    CHECK(grown < (8l << 20));
    if (grown >= (8l << 20))
        fprintf(stderr, "  the array's growth made %ld MB resident\n", grown >> 20);
    /* growing again keeps what was there (mremap moves it) and reads zero elsewhere */
    add(m, 3000000, 300, 3.5);
    CHECK(has(m, 3, 100, 1.5));
    CHECK(has(m, 1000000, 200, 2.5));
    CHECK(has(m, 3000000, 300, 3.5));
    mem_run r = {0};
    CHECK(mt_collect(m, 2000000, 8, INT64_MIN, INT64_MAX, UINT64_MAX, &r) == 0 && r.n == 0);
    mem_run_free(&r);
    CHECK(rss() - r0 < (8l << 20));
    mt_unref(m);
}

#ifndef OLD_CODE
/* tsdb_free_big zaps only the pages inside the block it is given: the neighbours and the block's partial pages stay */
static void test_free_big(void)
{
    size_t n = (size_t)24 << 20; /* above TSDB_FREE_BIG_MIN */
    unsigned char *a = malloc(n), *b = malloc(n), *c = malloc(n);
    CHECK(a && b && c);
    memset(a, 0xa5, n);
    memset(b, 0x5a, n);
    memset(c, 0xc3, n);
    long r0 = rss();
    tsdb_free_big(b, n);
    long freed = r0 - rss();
    for (size_t i = 0; i < n; i += 4093) {
        CHECK(a[i] == 0xa5);
        CHECK(c[i] == 0xc3);
    }
    CHECK(a[n - 1] == 0xa5 && c[0] == 0xc3);
    CHECK(freed > (12l << 20)); /* the block's pages went back */
    /* a block that starts inside a page and a short one: no crash, no damage */
    unsigned char *d = malloc((size_t)20 << 20);
    memset(d, 0x11, (size_t)20 << 20);
    tsdb_free_big(d, (size_t)20 << 20);
    tsdb_free_big(NULL, (size_t)64 << 20);
    tsdb_free_big(malloc(100), 100);
    CHECK(a[12345] == 0xa5 && c[12345] == 0xc3);
    free(a);
    free(c);
}
#endif

int main(void)
{
    test_series_array();
#ifndef OLD_CODE
    test_free_big();
#endif
    if (failures) {
        fprintf(stderr, "%d failures\n", failures);
        return 1;
    }
    printf("test_flush_tail: ok\n");
    return 0;
}
