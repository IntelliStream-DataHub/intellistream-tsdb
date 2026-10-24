/*
 * Prints tsdb_calendar_bounds output (client/tsdb_calendar.c, the reference
 * definition of local calendar periods) as fixtures for CalendarBoundsTest,
 * which checks that the Java client computes the same bounds. From the
 * repository root:
 *
 *   cc -O2 -Iinclude -o build/calendar-fixtures client/java/src/test/c/calendar_fixtures.c \
 *       build/libtsdbclient.a -lzstd -lpthread
 *   build/calendar-fixtures | gzip -9n > client/java/src/test/resources/calendar-bounds.txt.gz
 *
 * With zone names as arguments it prints the same cases for those zones;
 * CalendarBoundsTest's main reads such output on stdin and lists where Java
 * differs (cd client/java; mvn test-compile first):
 *
 *   build/calendar-fixtures Europe/Oslo Asia/Tehran | java -cp \
 *       client/java/target/classes:client/java/target/test-classes:$JUNIT ai.intellistream.tsdb.CalendarBoundsTest
 *
 * ($JUNIT: the junit-jupiter-api and opentest4j jars from ~/.m2.)
 *
 * One line per case: zone unit count start end rc n, then the first bound and
 * the differences between consecutive bounds, run-length coded ("86400000*365").
 * The bounds come from the system's zoneinfo (or $TZDIR), whose version goes in
 * the header: Java's own tzdb can be older or newer.
 */
#include "tsdb_calendar.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const zones[] = {
    "UTC", "+05:30", "-03:30", "+14:00", "-12:00", "America/Toronto",
    "Europe/Oslo", "Europe/London", "Europe/Dublin", "Europe/Lisbon", "Europe/Moscow", "Europe/Amsterdam",
    "Europe/Istanbul", "America/St_Johns", "America/New_York", "America/Sao_Paulo", "America/Havana",
    "America/Santiago", "America/Asuncion", "America/Nuuk", "America/Caracas", "America/Mexico_City",
    "America/Scoresbysund", "America/Vancouver", "Atlantic/Azores", "Australia/Lord_Howe", "Australia/Adelaide",
    "Pacific/Chatham", "Pacific/Apia", "Pacific/Kiritimati", "Asia/Kathmandu", "Asia/Kolkata", "Asia/Tehran",
    "Asia/Jerusalem", "Asia/Dhaka", "Asia/Shanghai", "Africa/Casablanca", "Africa/Cairo", "Africa/Monrovia",
    "Antarctica/Troll",
};

static int64_t day_ms(int64_t y, unsigned m, unsigned d) /* civil date at 00:00 UTC */
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (era * 146097 + (int64_t)doe - 719468) * 86400000;
}

static int64_t year_ms(int64_t y) { return day_ms(y, 1, 1); }

static void one(const char *zone, int unit, uint32_t count, int64_t start, int64_t end)
{
    int64_t *b = NULL;
    size_t n = 0;
    int rc = tsdb_calendar_bounds(zone, (tsdb_unit)unit, count, start, end, &b, &n);
    printf("%s %d %" PRIu32 " %" PRId64 " %" PRId64 " %d %zu", zone, unit, count, start, end, rc, n);
    if (n)
        printf(" %" PRId64, b[0]);
    for (size_t i = 1; i < n;) {
        int64_t d = b[i] - b[i - 1];
        size_t r = 1;
        while (i + r < n && b[i + r] - b[i + r - 1] == d)
            r++;
        if (r > 1)
            printf(" %" PRId64 "*%zu", d, r);
        else
            printf(" %" PRId64, d);
        i += r;
    }
    printf("\n");
    free(b);
}

static uint64_t rng;

static uint64_t next(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static void cases(const char *zone)
{
    static const uint32_t days[] = {0, 1, 2, 3, 7, 10, 100}, weeks[] = {1, 2, 3, 52},
                          months[] = {1, 2, 3, 5, 7, 12, 13}, quarters[] = {1, 2, 5},
                          years[] = {1, 2, 3, 10, 100, 1000}, hours[] = {1, 2, 3, 5, 7, 24, 100},
                          minutes[] = {1, 7, 15, 45, 90}, seconds[] = {1, 7, 90};
    for (size_t i = 0; i < sizeof days / sizeof *days; i++)
        one(zone, TSDB_DAY, days[i], year_ms(1890) + 12345, year_ms(2110) - 777);
    for (size_t i = 0; i < sizeof weeks / sizeof *weeks; i++)
        one(zone, TSDB_WEEK, weeks[i], year_ms(1890) - 5, year_ms(2110) + 3);
    for (size_t i = 0; i < sizeof months / sizeof *months; i++)
        one(zone, TSDB_MONTH, months[i], year_ms(1800), year_ms(2200));
    for (size_t i = 0; i < sizeof quarters / sizeof *quarters; i++)
        one(zone, TSDB_QUARTER, quarters[i], year_ms(1800) + 1, year_ms(2200) - 1);
    for (size_t i = 0; i < sizeof years / sizeof *years; i++)
        one(zone, TSDB_YEAR, years[i], year_ms(1000), year_ms(3000));
    for (size_t i = 0; i < sizeof hours / sizeof *hours; i++)
        one(zone, TSDB_HOUR, hours[i], year_ms(1960) + 1, year_ms(2060));
    for (size_t i = 0; i < sizeof minutes / sizeof *minutes; i++)
        one(zone, TSDB_MINUTE, minutes[i], year_ms(2024), year_ms(2025));
    for (size_t i = 0; i < sizeof seconds / sizeof *seconds; i++) {
        one(zone, TSDB_SECOND, seconds[i], day_ms(2024, 3, 30), day_ms(2024, 4, 1));
        one(zone, TSDB_SECOND, seconds[i], day_ms(1980, 10, 25), day_ms(1980, 10, 27));
    }
    /* edges: empty and reversed ranges, one millisecond, a range beyond the limit, too many periods */
    const int64_t limit = 1000LL * 86400 * 366 * 200000;
    one(zone, TSDB_DAY, 1, 1000, 1000);
    one(zone, TSDB_DAY, 1, 1000, 999);
    one(zone, TSDB_DAY, 1, -1, 0);
    one(zone, TSDB_MONTH, 1, -limit - 1, 0);
    one(zone, TSDB_MONTH, 1, 0, limit + 1);
    one(zone, TSDB_YEAR, 1000, -limit, limit);
    one(zone, TSDB_YEAR, 7, year_ms(-3000), year_ms(5000));
    one(zone, TSDB_SECOND, 1, 0, 365LL * 86400000);
    /* unsigned counts beyond INT32_MAX (not so large that C's arithmetic overflows) */
    one(zone, TSDB_DAY, 3000000000u, year_ms(1900), year_ms(2100));
    one(zone, TSDB_HOUR, 4000000000u, year_ms(1900), year_ms(2100));
    one(zone, TSDB_WEEK, 2500000000u, year_ms(1900), year_ms(2100));
    /* random ranges: any unit, mostly small counts, from a millisecond to a few centuries long */
    rng = 0x9e3779b97f4a7c15ull;
    for (const char *p = zone; *p; p++)
        rng = (rng ^ (unsigned char)*p) * 0x100000001b3ull;
    for (int i = 0; i < 60; i++) {
        int unit = TSDB_SECOND + (int)(next() % 8);
        uint32_t count = next() % 4 ? (uint32_t)(next() % 13) : (uint32_t)(next() % 5000);
        int64_t start = year_ms(1850) + (int64_t)(next() % (uint64_t)(year_ms(2150) - year_ms(1850)));
        int64_t len = (int64_t)1 << (next() % 44); /* up to 557 years */
        len += (int64_t)(next() % (uint64_t)len);
        if (unit <= TSDB_MINUTE && len / (unit == TSDB_SECOND ? 1000 : 60000) / (count ? count : 1) > (8 << 20))
            len /= 1024; /* keep most of them in the bounds' limit */
        one(zone, unit, count, start, start + len);
    }
}

int main(int argc, char **argv)
{
    char path[512], line[128] = "";
    snprintf(path, sizeof path, "%s/tzdata.zi", getenv("TZDIR") ? getenv("TZDIR") : "/usr/share/zoneinfo");
    FILE *v = fopen(path, "r");
    if (v && fgets(line, sizeof line, v))
        line[strcspn(line, "\n")] = '\0';
    if (v)
        fclose(v);
    printf("# tsdb_calendar_bounds, system zoneinfo %s\n", *line ? line + 2 : "(unknown version)");
    printf("# zone unit count start end rc n [first delta[*repeat]...]\n");
    if (argc > 1)
        for (int i = 1; i < argc; i++)
            cases(argv[i]);
    else
        for (size_t i = 0; i < sizeof zones / sizeof *zones; i++)
            cases(zones[i]);
    return 0;
}
