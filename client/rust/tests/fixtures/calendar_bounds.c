/*
 * Prints the bounds tsdb_calendar_bounds (client/tsdb_calendar.c) gives for a
 * grid of zones, units, counts and ranges: the fixtures that
 * tests/calendar_fixtures.rs checks the Rust port against. Run
 * gen_calendar_fixtures.sh rather than this directly; it pins the zoneinfo.
 *
 * One line per case:
 *   zone unit count start end n fnv1a32 [bounds, when n <= 4]
 *   zone unit count start end ERR code
 * zone "-" is the empty name (UTC). The hash is 32-bit FNV-1a over the bounds
 * as little-endian i64. Seconds, minutes and hours depend on the zone only
 * through its standard offset, so they get the short ranges only. Cases of more than 100,000 periods are left out, to
 * keep the Rust test (and this program) quick; the errors beyond 64 Mi
 * periods stay in.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tsdb_calendar.h"

static const char *zones[] = {
    "-", "Z", "Etc/UTC", "+02:00", "-05:30", "+14:00",
    "Europe/Oslo", "Europe/London", "Europe/Dublin", "Europe/Lisbon", "Europe/Moscow", "Europe/Istanbul",
    "America/St_Johns", "America/Sao_Paulo", "America/Havana", "America/Santiago", "America/New_York",
    "America/Nuuk", "America/Asuncion", "America/Mexico_City", "America/Caracas", "America/Scoresbysund",
    "Australia/Lord_Howe", "Australia/Sydney", "Australia/Adelaide", "Australia/Eucla",
    "Asia/Kathmandu", "Asia/Kolkata", "Asia/Tehran", "Asia/Jerusalem", "Asia/Gaza", "Asia/Amman",
    "Asia/Beirut", "Asia/Shanghai", "Asia/Dhaka",
    "Africa/Casablanca", "Africa/Cairo", "Pacific/Apia", "Pacific/Kiritimati", "Pacific/Chatham",
    "Pacific/Honolulu", "Pacific/Kwajalein", "Antarctica/Troll", "Etc/GMT+5",
};

#define DAY 86400000LL
#define YEAR (365 * DAY + DAY / 4)

static int64_t civil_ms(int y, int m, int d) /* days_from_civil, in ms */
{
    int64_t yy = y - (m <= 2);
    int64_t era = (yy >= 0 ? yy : yy - 399) / 400;
    unsigned yoe = (unsigned)(yy - era * 400);
    unsigned doy = (153 * (unsigned)(m + (m > 2 ? -3 : 9)) + 2) / 5 + (unsigned)d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (era * 146097 + (int64_t)doe - 719468) * DAY;
}

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t next(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

/* shortest length of a unit, ms: an upper estimate of the periods in a range */
static const double shortest[] = {0, 1e3, 6e4, 3.6e6, 0.9 * DAY, 6.9 * DAY, 27 * DAY, 88 * DAY, 364 * DAY};

static void one(const char *zone, int unit, uint32_t count, int64_t start, int64_t end)
{
    double est = start < end ? ((double)end - (double)start) / (shortest[unit] * (count ? count : 1)) + 2 : 0;
    if (est > 100000 && est < 80e6)
        return; /* left out; from 80 M on, the error */
    int64_t *b;
    size_t n;
    int rc = tsdb_calendar_bounds(strcmp(zone, "-") ? zone : "", (tsdb_unit)unit, count, start, end, &b, &n);
    if (rc) {
        printf("%s %d %u %lld %lld ERR %d\n", zone, unit, count, (long long)start, (long long)end, rc);
        return;
    }
    if (n <= 100000) {
        uint32_t h = 0x811c9dc5u;
        for (size_t i = 0; i < n; i++)
            for (int k = 0; k < 8; k++) {
                h ^= (uint8_t)((uint64_t)b[i] >> (8 * k));
                h *= 0x01000193u;
            }
        printf("%s %d %u %lld %lld %zu %08x", zone, unit, count, (long long)start, (long long)end, n, h);
        for (size_t i = 0; n <= 4 && i < n; i++)
            printf(" %lld", (long long)b[i]);
        printf("\n");
    }
    free(b);
}

int main(void)
{
    static const uint32_t counts[] = {1, 2, 13, 100}, sub_day_counts[] = {1, 7, 100};
    const int64_t far = 100000 * YEAR;
    int64_t fixed[][2] = {
        {civil_ms(1900, 1, 1), civil_ms(2040, 1, 1)},                   /* decades */
        {civil_ms(1965, 6, 15) + 45296789, civil_ms(2005, 3, 1)},      /* odd milliseconds */
        {civil_ms(2037, 11, 1), civil_ms(2100, 1, 1)},                  /* the POSIX rules */
        {civil_ms(1800, 1, 1), civil_ms(1900, 1, 1) + 1},               /* before the first transition */
        {-1, 1}, {0, 1}, {-DAY - 1, -DAY}, {5, 5}, {7, 3},
        {-far, far},                                                    /* 200,000 years */
        {-7000000000000000000LL, 0},                                    /* beyond the limit */
    };
    size_t nfixed = sizeof fixed / sizeof fixed[0];
    for (size_t z = 0; z < sizeof zones / sizeof zones[0]; z++) {
        int64_t ranges[16][2];
        size_t nr = 0;
        for (size_t i = 0; i < nfixed; i++) {
            ranges[nr][0] = fixed[i][0];
            ranges[nr++][1] = fixed[i][1];
        }
        while (nr < 16) { /* random ranges from 1850 to 2100, up to 400 days long */
            int64_t s = civil_ms(1850, 1, 1) + (int64_t)(next() % (uint64_t)(250 * YEAR));
            ranges[nr][0] = s;
            ranges[nr++][1] = s + 1 + (int64_t)(next() % (uint64_t)(400 * DAY));
        }
        for (int unit = 1; unit <= 3; unit++)
            for (size_t c = 0; c < sizeof sub_day_counts / sizeof sub_day_counts[0]; c++)
                for (size_t r = 4; r < nr; r++)
                    if (r < 9 || r >= nfixed)
                        one(zones[z], unit, sub_day_counts[c], ranges[r][0], ranges[r][1]);
        for (int unit = 4; unit <= 8; unit++) {
            one(zones[z], unit, 0, ranges[1][0], ranges[1][1]); /* 0 counts as 1 */
            for (size_t c = 0; c < sizeof counts / sizeof counts[0]; c++)
                for (size_t r = 0; r < nr; r++)
                    one(zones[z], unit, counts[c], ranges[r][0], ranges[r][1]);
        }
        one(zones[z], 1, 1, ranges[0][0], ranges[0][1]); /* too many periods */
    }
    return 0;
}
