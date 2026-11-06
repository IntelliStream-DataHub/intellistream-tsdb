/*
 * Calendar grouping against glibc: for each zone, the bucket of every row is
 * computed with localtime_r and every bucket start with mktime, and both must
 * match tsdb_query_group_local. The database knows UTC only; the zone is the
 * client side's (tsdb_calendar.h), which sends the periods' UTC bounds.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "portable.h"

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                    \
    do {                                                                                            \
        int rc_ = (expr);                                                                           \
        if (rc_ != 0) {                                                                             \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                                \
        }                                                                                           \
    } while (0)

static int64_t floordiv64(int64_t a, int64_t b) { return a / b - (a % b < 0); }

static int64_t days_from_civil(int64_t y, int m, int d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (unsigned)(m + (m > 2 ? -3 : 9)) + 2) / 5 + (unsigned)d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

/* reference bucket key and start, through glibc */
static int64_t ref_key(time_t t, tsdb_unit unit, int64_t count)
{
    struct tm tm;
    localtime_r(&t, &tm);
    int64_t y = tm.tm_year + 1900, day = days_from_civil(y, tm.tm_mon + 1, tm.tm_mday), idx;
    switch (unit) {
    case TSDB_DAY: idx = day; break;
    case TSDB_WEEK: idx = floordiv64(day + 3, 7); break;
    case TSDB_MONTH: idx = (y - 1970) * 12 + tm.tm_mon; break;
    case TSDB_QUARTER: idx = floordiv64((y - 1970) * 12 + tm.tm_mon, 3); break;
    default: idx = y - 1970; break;
    }
    return floordiv64(idx, count);
}

static time_t ref_start(int64_t key, tsdb_unit unit, int64_t count);

/* A period runs from its first instant to the next period's: a row in an hour
 * that comes back to the previous date (St. John's fell back from 00:01 to
 * 23:01 until 2011) stays in the period that had begun. */
static int64_t ref_bucket(time_t t, tsdb_unit unit, int64_t count)
{
    int64_t k = ref_key(t, unit, count);
    while (ref_start(k + 1, unit, count) <= t)
        k++;
    return k;
}

static time_t ref_start(int64_t key, tsdb_unit unit, int64_t count)
{
    int64_t idx = key * count, y, m = 1, d = 1;
    if (unit == TSDB_DAY || unit == TSDB_WEEK) {
        int64_t day = unit == TSDB_DAY ? idx : idx * 7 - 3;
        /* civil from days */
        int64_t z = day + 719468, era = (z >= 0 ? z : z - 146096) / 146097;
        unsigned doe = (unsigned)(z - era * 146097), yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
        d = doy - (153 * mp + 2) / 5 + 1;
        m = mp < 10 ? mp + 3 : mp - 9;
        y = (int64_t)yoe + era * 400 + (m <= 2);
    } else {
        int64_t month = unit == TSDB_MONTH ? idx : unit == TSDB_QUARTER ? idx * 3 : idx * 12;
        y = 1970 + floordiv64(month, 12);
        m = month - floordiv64(month, 12) * 12 + 1;
    }
    struct tm tm = {.tm_year = (int)(y - 1900), .tm_mon = (int)(m - 1), .tm_mday = (int)d, .tm_isdst = -1};
    time_t t = mktime(&tm);
    /* a local midnight that happens twice (a fallback just after it) begins the period the first time;
     * mktime may pick either reading, and fallbacks move clocks back by half an hour to two hours */
    static const int back[] = {7200, 3600, 1800};
    for (size_t i = 0; i < sizeof back / sizeof back[0]; i++) {
        time_t e = t - back[i];
        struct tm l;
        localtime_r(&e, &l);
        if (l.tm_year == tm.tm_year && l.tm_mon == tm.tm_mon && l.tm_mday == tm.tm_mday && l.tm_hour == 0 &&
            l.tm_min == 0 && l.tm_sec == 0)
            return e;
    }
    return t;
}

typedef struct rb {
    int64_t key;
    uint64_t count;
    double sum;
} rb;

extern size_t local_piece_bounds; /* src/calendar.c: local periods per window (0: the default) */
extern size_t local_twa_listed;   /* src/calendar.c: bounds local TWAs have listed so far */

static long maxrss_mb(void)
{
    struct rusage u;
    getrusage(RUSAGE_SELF, &u);
    return RU_MAXRSS_KB(u) / 1024;
}

/* Local grouping a few periods at a time gives the buckets of one window over every bound, bit for bit,
 * in both orders and with limits, for the buckets and the spread. */
static void same_in_windows(tsdb *db, tsdb_sid id, int64_t start, int64_t end, const tsdb_group *g)
{
    size_t lims[] = {0, 3}, wins[] = {2, 7};
    for (int desc = 0; desc < 2; desc++)
        for (size_t l = 0; l < 2; l++) {
            tsdb_query_opts o = {desc ? TSDB_DESC : TSDB_ASC, lims[l]};
            tsdb_bucket *a, *b;
            tsdb_spread_bucket *sa, *sb;
            size_t na, nb, nsa, nsb;
            local_piece_bounds = 0;
            OK(tsdb_query_group_local(db, id, start, end, g, &o, &a, &na));
            OK(tsdb_query_spread_group_local(db, id, start, end, g, &o, &sa, &nsa));
            CHECK(na > 0 && na == nsa && (!lims[l] || na <= lims[l]));
            for (size_t w = 0; w < 2; w++) {
                local_piece_bounds = wins[w];
                OK(tsdb_query_group_local(db, id, start, end, g, &o, &b, &nb));
                OK(tsdb_query_spread_group_local(db, id, start, end, g, &o, &sb, &nsb));
                if (nb != na || memcmp(a, b, na * sizeof *a) != 0 || nsb != nsa ||
                    memcmp(sa, sb, nsa * sizeof *sa) != 0)
                    fprintf(stderr, "  %s unit %d x%u desc %d limit %zu, windows of %zu: %zu buckets, want %zu\n",
                            g->timezone, g->unit, g->count, desc, lims[l], wins[w], nb, na);
                CHECK(nb == na && memcmp(a, b, na * sizeof *a) == 0);
                CHECK(nsb == nsa && memcmp(sa, sb, nsa * sizeof *sa) == 0);
                free(b);
                free(sb);
            }
            free(a);
            free(sa);
        }
    /* the TWA lists every bound, window after window: the same list as in one */
    tsdb_twa_bucket *ta, *tb;
    size_t nta, ntb;
    local_piece_bounds = 0;
    OK(tsdb_query_twa_group_local(db, id, start, end, g, NULL, &ta, &nta));
    for (size_t w = 0; w < 2; w++) {
        local_piece_bounds = wins[w];
        OK(tsdb_query_twa_group_local(db, id, start, end, g, NULL, &tb, &ntb));
        CHECK(ntb == nta && memcmp(ta, tb, nta * sizeof *ta) == 0);
        free(tb);
    }
    free(ta);
    local_piece_bounds = 0;
}

/* Two rows far apart, grouped by the local second: memory follows the two buckets, not the 4 million periods
 * between them (which listed 8 bytes of bounds each and then a dense array). 8, 60 or 100 million periods are more
 * than a query may span (4 Mi, as in UTC): refused from their nominal length, before any bound is listed. The TWA,
 * whose segments fill every period, is refused before it lists them, or for 8 million of them (too few to tell
 * from the nominal length) once it has listed one more than it answers. */
static void test_sparse_local(tsdb *db)
{
    int64_t t0 = days_from_civil(2024, 1, 1) * 86400000LL + 1500;
    int64_t gaps[] = {60000000LL * 1000, 100000000LL * 1000, 8000000LL * 1000, 4000000LL * 1000};
    for (int k = 0; k < 4; k++) {
        char name[32];
        snprintf(name, sizeof name, "sparse%d", k);
        tsdb_sid id;
        OK(tsdb_series(db, name, &id));
        int64_t tt[2] = {t0, t0 + gaps[k]};
        float vv[2] = {1, 2};
        OK(tsdb_insert_series(db, id, tt, vv, 2));
        tsdb_group g = {TSDB_SECOND, 1, "Europe/Oslo"};
        tsdb_query_opts newest = {TSDB_DESC, 1};
        tsdb_bucket *b;
        tsdb_spread_bucket *sb;
        tsdb_twa_bucket *tb;
        size_t nb;
        long before = maxrss_mb();
        size_t listed = local_twa_listed;
        if (gaps[k] / 1000 >= (4 << 20)) {
            CHECK(tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &b, &nb) ==
                  TSDB_ERR_TOO_MANY_BUCKETS);
            CHECK(tsdb_query_spread_group_local(db, id, t0 - 7, t0 + gaps[k] + 1, &g, NULL, &sb, &nb) ==
                  TSDB_ERR_TOO_MANY_BUCKETS);
            CHECK(tsdb_query_twa_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &tb, &nb) ==
                  TSDB_ERR_TOO_MANY_BUCKETS);
            /* refused from the nominal length before listing, unless the span is too short to tell */
            listed = local_twa_listed - listed;
            if (gaps[k] / 4 / 1000 > (4 << 20) + 2)
                CHECK(listed == 0);
            else
                CHECK(listed > (4 << 20));
        } else {
            OK(tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &b, &nb));
            CHECK(nb == 2 && b[0].start == t0 - 500 && b[1].start == tt[1] - 500 && b[0].count == 1 && b[1].sum == 2);
            free(b);
            OK(tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, &newest, &b, &nb));
            CHECK(nb == 1 && b[0].start == tt[1] - 500);
            free(b);
            OK(tsdb_query_spread_group_local(db, id, t0 - 7, t0 + gaps[k] + 1, &g, NULL, &sb, &nb));
            CHECK(nb == 2 && sb[1].b.start == tt[1] - 500 && sb[1].range == 0);
            free(sb);
        }
        long grew = maxrss_mb() - before;
        if (grew >= 128)
            fprintf(stderr, "  two rows %lld s apart by the local second: the queries took %ld MB\n",
                    (long long)(gaps[k] / 1000), grew);
        CHECK(grew < 128);
        /* windows of a few periods jump from row to row */
        same_in_windows(db, id, INT64_MIN, INT64_MAX, &(tsdb_group){TSDB_HOUR, 1, "Europe/Oslo"});
    }
}

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-group-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 256;
    o.partition_ms = 30LL * 86400000;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    test_sparse_local(db);

    /* sparse rows from 1945 to 2105 (Japan's DST 1948-51, China's 1986-91,
     * Moscow's offset changes), dense ones around DST changes */
    size_t cap = 500000, n = 0;
    int64_t *ts = malloc(cap * sizeof *ts);
    float *v = malloc(cap * sizeof *v);
    uint64_t r = 99;
    for (int64_t t = -788918400000LL; t < 4260211200000LL; t += 3LL * 3600000 + (int64_t)(r % 7200000)) {
        r = r * 6364136223846793005ull + 1442695040888963407ull;
        ts[n] = t;
        v[n] = (float)(n % 1000);
        n++;
    }
    for (int y = 1990; y <= 2095; y += 7) { /* every 10 minutes through March, April, October, November */
        int months[4] = {3, 4, 10, 11};
        for (int k = 0; k < 4; k++) {
            int64_t a = days_from_civil(y, months[k], 1) * 86400000LL;
            for (int64_t t = a; t < a + 30LL * 86400000 && n < cap; t += 600000) {
                ts[n] = t + 1;
                v[n] = 1.0f;
                n++;
            }
        }
    }
    /* sorted, so the reference below can group runs of equal buckets */
    for (size_t i = 1; i < n; i++) {
        int64_t t = ts[i];
        float x = v[i];
        size_t j = i;
        if (ts[j - 1] <= t)
            continue;
        size_t lo = 0, hi = i; /* binary search, then shift */
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            if (ts[mid] <= t)
                lo = mid + 1;
            else
                hi = mid;
        }
        memmove(ts + lo + 1, ts + lo, (i - lo) * sizeof *ts);
        memmove(v + lo + 1, v + lo, (i - lo) * sizeof *v);
        ts[lo] = t;
        v[lo] = x;
        (void)j;
    }
    OK(tsdb_insert_series(db, id, ts, v, n));
    OK(tsdb_flush(db));

    const char *zones[] = {"UTC", "Europe/Oslo", "America/New_York", "America/Los_Angeles", "Asia/Shanghai",
                           "Asia/Tokyo", "Europe/Moscow", "Asia/Kolkata", "Australia/Sydney", "America/St_Johns",
                           "+05:45"};
    const char *tzenv[] = {"UTC", "Europe/Oslo", "America/New_York", "America/Los_Angeles", "Asia/Shanghai",
                           "Asia/Tokyo", "Europe/Moscow", "Asia/Kolkata", "Australia/Sydney", "America/St_Johns",
                           "<+0545>-5:45"};
    tsdb_unit units[] = {TSDB_DAY, TSDB_WEEK, TSDB_MONTH, TSDB_QUARTER, TSDB_YEAR};
    int counts[] = {1, 2, 3};
    rb *ref = malloc(n * sizeof *ref);
    int checked = 0;
    for (size_t z = 0; z < sizeof zones / sizeof *zones; z++) {
        setenv("TZ", tzenv[z], 1);
        tzset();
        for (size_t u = 0; u < 5; u++) {
            for (size_t c = 0; c < 3; c++) {
                tsdb_group g = {units[u], (uint32_t)counts[c], zones[z]};
                tsdb_bucket *b;
                size_t nb;
                OK(tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &b, &nb));
                size_t nr = 0;
                for (size_t i = 0; i < n; i++) {
                    int64_t k = ref_bucket((time_t)floordiv64(ts[i], 1000), units[u], counts[c]);
                    size_t j = nr;
                    while (j > 0 && ref[j - 1].key > k)
                        j--;
                    if (j > 0 && ref[j - 1].key == k) {
                        ref[j - 1].count++;
                        ref[j - 1].sum += v[i];
                    } else {
                        memmove(ref + j + 1, ref + j, (nr - j) * sizeof *ref);
                        ref[j] = (rb){k, 1, v[i]};
                        nr++;
                    }
                }
                bool ok = nb == nr;
                for (size_t i = 0; ok && i < nb; i++) {
                    time_t start = ref_start(ref[i].key, units[u], counts[c]);
                    ok = b[i].count == ref[i].count && fabs(b[i].sum - ref[i].sum) < 1e-6 * fmax(1, ref[i].sum) &&
                         b[i].start == (int64_t)start * 1000;
                    if (!ok)
                        fprintf(stderr, "  %s unit %d x%d bucket %zu: got start %lld n=%llu, want %lld n=%llu\n",
                                zones[z], units[u], counts[c], i, (long long)b[i].start,
                                (unsigned long long)b[i].count, (long long)start * 1000,
                                (unsigned long long)ref[i].count);
                }
                if (!ok && nb != nr)
                    fprintf(stderr, "  %s unit %d x%d: %zu buckets, want %zu\n", zones[z], units[u], counts[c], nb, nr);
                CHECK(ok);
                checked++;
                free(b);
            }
        }
    }
    /* windows of local periods: the same buckets as one window over every bound */
    {
        const char *wz[] = {"Europe/Oslo", "America/New_York", "Australia/Sydney", "America/St_Johns", "+05:45"};
        int64_t y0 = days_from_civil(2018, 1, 1) * 86400000LL, y1 = days_from_civil(2019, 1, 1) * 86400000LL;
        for (size_t z = 0; z < 5; z++) {
            tsdb_group gs[] = {{TSDB_HOUR, 1, wz[z]}, {TSDB_MINUTE, 15, wz[z]}, {TSDB_DAY, 1, wz[z]},
                               {TSDB_DAY, 3, wz[z]}, {TSDB_WEEK, 1, wz[z]}, {TSDB_MONTH, 1, wz[z]}};
            /* not on period bounds; the hours and quarters over March and April only (DST changes, dense rows) */
            int64_t m0 = days_from_civil(2018, 3, 1) * 86400000LL + 45296789, m1 = m0 + 61LL * 86400000;
            for (size_t k = 0; k < sizeof gs / sizeof *gs; k++)
                same_in_windows(db, id, k < 2 ? m0 : y0 + 45296789, k < 2 ? m1 : y1 - 3600000 * 7, &gs[k]);
            same_in_windows(db, id, INT64_MIN, INT64_MAX, &(tsdb_group){TSDB_MONTH, 1, wz[z]});
            same_in_windows(db, id, INT64_MIN, INT64_MAX, &(tsdb_group){TSDB_YEAR, 2, wz[z]});
        }
    }
    /* sub-day units in a fixed-offset zone: multiples of the length in local time */
    {
        tsdb_group g = {TSDB_MINUTE, 15, "+05:45"};
        tsdb_bucket *b;
        size_t nb;
        /* 160 years of 15-minute buckets are 5.6 M, more than a query may span (4 Mi); 40 years are 1.4 M */
        CHECK(tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &b, &nb) == TSDB_ERR_TOO_MANY_BUCKETS);
        OK(tsdb_query_group_local(db, id, days_from_civil(1990, 1, 1) * 86400000LL,
                                  days_from_civil(2030, 1, 1) * 86400000LL, &g, NULL, &b, &nb));
        bool ok = nb > 0;
        for (size_t i = 0; i < nb; i++)
            ok &= floordiv64(b[i].start + (5 * 3600 + 45 * 60) * 1000LL, 900000) * 900000 == b[i].start + (5 * 3600 + 45 * 60) * 1000LL;
        CHECK(ok);
        free(b);
        tsdb_group h = {TSDB_HOUR, 6, NULL};
        OK(tsdb_query_group(db, id, INT64_MIN, INT64_MAX, &h, &b, &nb));
        uint64_t total = 0;
        for (size_t i = 0; i < nb; i++) {
            total += b[i].count;
            CHECK(b[i].start % (6 * 3600000LL) == 0);
        }
        CHECK(total == n);
        free(b);
    }
    /* a DST day in Oslo is 23 or 25 hours long */
    {
        tsdb_group g = {TSDB_DAY, 1, "Europe/Oslo"};
        tsdb_bucket *b;
        size_t nb;
        int64_t a = days_from_civil(2024, 3, 30) * 86400000LL, e = days_from_civil(2024, 4, 2) * 86400000LL;
        int64_t tt[3] = {a + 1, a + 86400000 + 1, a + 2 * 86400000 + 1};
        float vv[3] = {1, 1, 1};
        OK(tsdb_insert_series(db, id, tt, vv, 3));
        OK(tsdb_query_group_local(db, id, a - 86400000, e, &g, NULL, &b, &nb));
        bool found = false;
        for (size_t i = 0; i + 1 < nb; i++)
            if (b[i + 1].start - b[i].start == 23 * 3600000LL)
                found = true;
        CHECK(found);
        free(b);
    }
    tsdb_group bad = {TSDB_DAY, 1, "Not/AZone"};
    tsdb_bucket *b;
    size_t nb;
    CHECK(tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &bad, NULL, &b, &nb) == TSDB_ERR_INVALID);
    /* the database itself takes no zone: only UTC, or bounds from the client */
    tsdb_group oslo = {TSDB_DAY, 1, "Europe/Oslo"}, utc = {TSDB_DAY, 1, "UTC"};
    CHECK(tsdb_query_group(db, id, 0, 1, &oslo, &b, &nb) == TSDB_ERR_INVALID);
    OK(tsdb_query_group(db, id, 0, 86400000, &utc, &b, &nb));
    free(b);
    int64_t unordered[3] = {0, 2000, 1000}, ok_bounds[2] = {157766400000LL, 157766400000LL + 86400000};
    CHECK(tsdb_query_bounds(db, id, 0, 1, unordered, 3, NULL, &b, &nb) == TSDB_ERR_INVALID);
    CHECK(tsdb_query_bounds(db, id, 0, 1, ok_bounds, 0, NULL, &b, &nb) == TSDB_ERR_INVALID);
    /* rows before the first bound count nowhere; the last bucket runs on */
    OK(tsdb_query_bounds(db, id, INT64_MIN, INT64_MAX, ok_bounds, 2, NULL, &b, &nb));
    uint64_t after = 0;
    for (size_t i = 0; i < n; i++)
        after += ts[i] >= ok_bounds[1];
    CHECK(nb == 2 && b[0].start == ok_bounds[0] && b[1].start == ok_bounds[1] && b[1].count >= after);
    free(b);

    /* Regression: "+0530" was read as 530 hours (the +hh form parsed first), so
     * its days began 2 hours off. Fixed offsets in every spelling agree. */
    const char *offsets[][3] = {{"+0530", "+05:30", "+05:30:00"}, {"-0530", "-05:30", "-05:30:00"}};
    const int64_t midnight[2] = {(24 - 5) * 3600000LL - 30 * 60000LL, 5 * 3600000LL + 30 * 60000LL};
    for (int i = 0; i < 2; i++)
        for (int k = 0; k < 3; k++) {
            int64_t *bd;
            size_t nbd;
            OK(tsdb_calendar_bounds(offsets[i][k], TSDB_DAY, 1, 0, 10 * 86400000LL, &bd, &nbd));
            bool at_midnight = nbd >= 10;
            for (size_t j = 0; j < nbd; j++)
                at_midnight &= ((bd[j] % 86400000) + 86400000) % 86400000 == midnight[i];
            if (!at_midnight)
                printf("%s: days begin at %lld ms past UTC midnight\n", offsets[i][k],
                       nbd ? (long long)(((bd[0] % 86400000) + 86400000) % 86400000) : -1LL);
            CHECK(at_midnight);
            free(bd);
        }

    /* Regression: with a count near 2^32 the period holding a time before 1970
     * (local time: Oslo's 1970 begins at -3600000) begins billions of months
     * earlier; that bound overflowed in milliseconds (to one far in the future,
     * which dropped rows). It is INT64_MIN now. */
    const tsdb_unit huge_units[3] = {TSDB_MONTH, TSDB_QUARTER, TSDB_YEAR};
    for (int u = 0; u < 3; u++) {
        int64_t *bd;
        size_t nbd;
        OK(tsdb_calendar_bounds("Europe/Oslo", huge_units[u], UINT32_MAX, -10 * 365 * 86400000LL, -3600000, &bd, &nbd));
        CHECK(nbd == 1 && bd[0] == INT64_MIN);
        free(bd);
        OK(tsdb_calendar_bounds("UTC", huge_units[u], UINT32_MAX, -86400000LL, 86400000LL, &bd, &nbd));
        CHECK(nbd == 2 && bd[0] == INT64_MIN && bd[1] == 0);
        free(bd);
    }
    OK(tsdb_close(db));
    if (system(cmd) != 0)
        return 1;
    free(ts);
    free(v);
    free(ref);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("group tests passed (%d zone x unit x count combinations, %zu rows)\n", checked, n);
    return 0;
}
