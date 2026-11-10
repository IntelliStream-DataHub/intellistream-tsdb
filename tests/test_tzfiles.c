/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Time zone input the calendar must refuse or survive: crafted TZif files
 * under TZDIR, POSIX rules in their footers, fixed offsets and zone names.
 * The calendar's source is included, so the test sees its cache. Cases that
 * crashed or hung before their fix run in a child process with an alarm.
 *
 *   ./build/test_tzfiles [scratch dir]
 */
#define _GNU_SOURCE
#include "../client/tsdb_calendar.c"

#include <ftw.h>
#include <sys/wait.h>

static int failures;

#define CHECK(cond)                                                                          \
    do {                                                                                     \
        if (!(cond)) {                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);         \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)

#define MAX_CACHED_ZONES_TEST 1024

static char tzdir[512];

static void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24), p[1] = (uint8_t)(v >> 16), p[2] = (uint8_t)(v >> 8), p[3] = (uint8_t)v;
}

/* A TZif header: version ('\0', '2', ...) and the six counts (isutc isstd leap time type char). */
static size_t header(uint8_t *p, char version, const uint32_t c[6])
{
    memset(p, 0, 44);
    memcpy(p, "TZif", 4);
    p[4] = (uint8_t)version;
    for (int i = 0; i < 6; i++)
        put32(p + 20 + 4 * i, c[i]);
    return 44;
}

/* One ttinfo (UTC offset `off`, standard time) and its designation "XXX". */
static size_t one_type(uint8_t *p, int32_t off)
{
    put32(p, (uint32_t)off);
    p[4] = 0, p[5] = 0;
    memcpy(p + 6, "XXX", 4);
    return 10;
}

static void write_zone(const char *name, const uint8_t *buf, size_t len)
{
    char path[1024];
    snprintf(path, sizeof path, "%s/%s", tzdir, name);
    FILE *f = fopen(path, "wb");
    if (!f || fwrite(buf, 1, len, f) != len)
        exit(1);
    fclose(f);
}

/* A version 2 file with no transitions, offset 0, and the given footer rule. */
static void write_v2(const char *name, int32_t off, const char *footer)
{
    uint8_t buf[512];
    const uint32_t c[6] = {0, 0, 0, 0, 1, 4};
    size_t n = header(buf, '2', c);
    n += one_type(buf + n, off);
    n += header(buf + n, '2', c);
    n += one_type(buf + n, off);
    n += (size_t)snprintf((char *)buf + n, sizeof buf - n, "\n%s\n", footer);
    write_zone(name, buf, n);
}

static int day_bounds(const char *zone, int64_t start, int64_t end, int64_t *first, size_t *n)
{
    int64_t *b;
    int rc = tsdb_calendar_bounds(zone, TSDB_DAY, 1, start, end, &b, n);
    if (rc == 0) {
        *first = *n ? b[0] : 0;
        free(b);
    }
    return rc;
}

static bool in_child(void (*fn)(void))
{
    fflush(stderr);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(20);
        failures = 0;
        fn();
        _exit(failures ? 1 : 0);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid)
        return false;
    if (WIFSIGNALED(st))
        fprintf(stderr, "  child died of signal %d\n", WTERMSIG(st));
    return WIFEXITED(st) && WEXITSTATUS(st) == 0;
}

/* timecnt 0xffffffff was read as -1: the size check passed, and malloc((size_t)-1 + 1) was malloc(0) */
static void negative_counts(void)
{
    uint8_t buf[256] = {0};
    const uint32_t c[6] = {0, 0, 0, 0xffffffffu, 1, 4};
    header(buf, '\0', c);
    write_zone("Evil", buf, 44 + 64);
    int64_t first;
    size_t n;
    CHECK(day_bounds("Evil", 0, 86400000, &first, &n) < 0);
    /* every count at its largest, in the 64-bit copy of a version 2 file */
    const uint32_t ok[6] = {0, 0, 0, 0, 1, 4}, big[6] = {0xffffffffu, 0xffffffffu, 0xffffffffu, 0xffffffffu,
                                                            0xffffffffu, 0xffffffffu};
    size_t m = header(buf, '2', ok);
    m += one_type(buf + m, 0);
    m += header(buf + m, '2', big);
    write_zone("Evil2", buf, m + 64);
    CHECK(day_bounds("Evil2", 0, 86400000, &first, &n) < 0);
}

/* rule_utc stepped back from week 999999999 a week at a time, for every year */
static void rule_ranges(void)
{
    const char *bad[] = {"AAA0BBB,M3.999999999.0,M10.5.0", "AAA0BBB,M13.1.0,M10.5.0", "AAA0BBB,M0.1.0,M10.5.0",
                         "AAA0BBB,M3.0.0,M10.5.0", "AAA0BBB,M3.5.7,M10.5.0", "AAA0BBB,M3.5.-1,M10.5.0",
                         "AAA0BBB,M3.5.0/9999999999,M10.5.0", "AAA99999999999BBB,M3.5.0,M10.5.0"};
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        char name[32];
        snprintf(name, sizeof name, "Rule%zu", i);
        write_v2(name, 0, bad[i]);
        int64_t first;
        size_t n;
        /* ten years: a rule is evaluated for each */
        int rc = day_bounds(name, 0, 3650LL * 86400000, &first, &n);
        CHECK(rc == 0 && n == 3650 && first == 0); /* no usable rule: the fixed offset of the file */
    }
}

/* A version 1 file with one transition at `at` (UTC seconds) from offset `a` to offset `b`. */
static void write_jump(const char *name, int32_t at, int32_t a, int32_t b)
{
    uint8_t buf[128];
    const uint32_t c[6] = {0, 0, 0, 1, 2, 4};
    size_t n = header(buf, 0, c);
    put32(buf + n, (uint32_t)at), n += 4;
    buf[n++] = 1;
    put32(buf + n, (uint32_t)a), buf[n + 4] = 0, buf[n + 5] = 0, n += 6;
    put32(buf + n, (uint32_t)b), buf[n + 4] = 0, buf[n + 5] = 0, n += 6;
    memcpy(buf + n, "XXX", 4), n += 4;
    write_zone(name, buf, n);
}

/* Local time going back by more than the range: in 1867 Alaska went from +15:02 to -08:57, so the end of a
 * two-second range lay in an earlier local day than its start. The bounds were empty (the rows of the range fell
 * in no bucket), and with a jump of two days the count of bounds went negative and became a huge allocation. A
 * TZif offset beyond RFC 8536's -25..+26 hours is refused. */
static void backward_jumps(void)
{
    const int32_t at = 10 * 86400;
    const int64_t start = (int64_t)at * 1000 - 1000, end = (int64_t)at * 1000 + 1000;
    int64_t first;
    size_t n;
    write_jump("Back1", at, 14 * 3600, -12 * 3600);
    int rc = day_bounds("Back1", start, end, &first, &n);
    CHECK(rc == 0 && n >= 1 && first <= start);
    write_jump("Back2", at, 14 * 3600, -25 * 3600 + 1);
    rc = day_bounds("Back2", start, end, &first, &n);
    CHECK(rc == 0 && n >= 1 && first <= start);
    write_jump("Back2b", at, 26 * 3600 - 1, -25 * 3600 + 1);
    rc = day_bounds("Back2b", start, end, &first, &n);
    CHECK(rc == 0 && n >= 1 && first <= start);
    write_jump("Huge", at, 0, -40 * 3600);
    CHECK(day_bounds("Huge", start, end, &first, &n) < 0);
    /* the real one, where the system has it */
    char path[256];
    snprintf(path, sizeof path, "/usr/share/zoneinfo/America/Juneau");
    if (access(path, R_OK) == 0) {
        const char *tzd = getenv("TZDIR");
        char saved[512];
        snprintf(saved, sizeof saved, "%s", tzd ? tzd : "");
        unsetenv("TZDIR");
        const int64_t t = -3225223727LL * 1000; /* 1867-10-19 00:31:13 UTC */
        rc = day_bounds("America/Juneau", t - 1000, t + 1000, &first, &n);
        CHECK(rc == 0 && n >= 1 && first <= t - 1000);
        setenv("TZDIR", saved, 1);
    }
}

static void test_tzif(void)
{
    CHECK(in_child(negative_counts));
    CHECK(in_child(rule_ranges));
    CHECK(in_child(backward_jumps));
    /* a good rule still works: Oslo's, in a slim file. 2024-07-01 local midnight is 22:00 UTC the day before */
    write_v2("Slim", 3600, "CET-1CEST,M3.5.0,M10.5.0/3");
    int64_t first;
    size_t n;
    const int64_t jul1 = 1719792000000LL; /* 2024-07-01T00:00Z */
    CHECK(day_bounds("Slim", jul1, jul1 + 1, &first, &n) == 0 && n == 1 && first == jul1 - 2 * 3600000);
    const int64_t jan1 = 1704067200000LL; /* 2024-01-01T00:00Z */
    CHECK(day_bounds("Slim", jan1, jan1 + 1, &first, &n) == 0 && n == 1 && first == jan1 - 3600000);
}

static void fixed_offsets(void)
{
    const char *bad[] = {"+99999999999", "+999:00", "-9999999:99:99", "+27", "+26:01", "-27:00", "+1:2",
                         "+01:60", "+01:00:60", "+01:00:00:00", "+", "+:30", "+001:00:00x", "+1a"};
    int64_t first;
    size_t n;
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        int rc = day_bounds(bad[i], 0, 1, &first, &n);
        if (rc != TSDB_ERR_INVALID)
            fprintf(stderr, "  %s: %d\n", bad[i], rc);
        CHECK(rc == TSDB_ERR_INVALID);
    }
    /* a megabyte of leading zeros was a valid name, kept whole in the cache */
    size_t big = 1 << 20;
    char *name = malloc(big + 3);
    name[0] = '+';
    memset(name + 1, '0', big);
    name[big + 1] = '1';
    name[big + 2] = '\0';
    CHECK(day_bounds(name, 0, 1, &first, &n) == TSDB_ERR_INVALID);
    free(name);
    struct {
        const char *zone;
        int64_t first;
    } good[] = {{"+01:00", -3600000}, {"-0530", -66600000}, {"+05:30:00", -19800000},
                {"+14", -50400000},   {"-12:00", -43200000}, {"+0", 0}};
    for (size_t i = 0; i < sizeof good / sizeof *good; i++)
        CHECK(day_bounds(good[i].zone, 0, 1, &first, &n) == 0 && n == 1 && first == good[i].first);
}

static void zone_names(void)
{
    write_v2("Good", 3600, "AAA-1");
    mkdir(strcat(strcpy((char[600]){0}, tzdir), "/Etc"), 0755);
    write_v2("Etc/Twelve", -12 * 3600, "AAA12");
    int64_t first;
    size_t n;
    CHECK(day_bounds("Good", 0, 1, &first, &n) == 0 && first == -3600000);
    CHECK(day_bounds("Etc/Twelve", 0, 1, &first, &n) == 0 && first == -12 * 3600000LL);
    /* other spellings of one file were each a cache entry; a long one was cut at 512 bytes to another zone */
    char longname[600] = "";
    for (int i = 0; i < 240; i++)
        strcat(longname, "./");
    strcat(longname, "Good");
    const char *bad[] = {"./Good", "Etc//Twelve", "Etc/./Twelve", "Etc/Twelve/", "/Good", "../Good", "Go od",
                         "Good\n", "Etc/..", longname, ""};
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        if (!bad[i][0])
            continue; /* "" is UTC */
        int rc = day_bounds(bad[i], 0, 1, &first, &n);
        CHECK(rc == TSDB_ERR_INVALID);
    }
    char toolong[300];
    memset(toolong, 'A', sizeof toolong - 1);
    toolong[sizeof toolong - 1] = '\0';
    CHECK(day_bounds(toolong, 0, 1, &first, &n) == TSDB_ERR_INVALID);
}

static size_t cached_zones(void)
{
    size_t cached = 0;
    for (tz *z = cache; z; z = z->next)
        cached++;
    return cached;
}

/* distinct names that all load: fixed offsets never enter the cache, files stop at its limit, and zones past it
   still answer right */
static void cache_bound(void)
{
    int64_t first;
    size_t n;
    size_t loaded = 0, before = cached_zones();
    for (int sign = 0; sign < 2; sign++)
        for (int h = 0; h < 26; h++)
            for (int m = 0; m < 60; m++) {
                char zone[32];
                snprintf(zone, sizeof zone, "%c%02d:%02d", sign ? '-' : '+', h, m);
                int64_t off = (sign ? -1 : 1) * (h * 3600 + m * 60);
                int64_t want = (floor_div(off, 86400) * 86400 - off) * 1000;
                int rc = day_bounds(zone, 0, 1, &first, &n);
                CHECK(rc == 0 && n == 1 && first == want);
                loaded += rc == 0;
            }
    CHECK(loaded == 2 * 26 * 60);
    CHECK(cached_zones() == before);
    char dir[600];
    snprintf(dir, sizeof dir, "%s/Many", tzdir);
    mkdir(dir, 0755);
    loaded = 0;
    for (int i = 0; i < MAX_CACHED_ZONES_TEST + 100; i++) {
        char zone[32];
        snprintf(zone, sizeof zone, "Many/Z%d", i);
        char rule[16];
        snprintf(rule, sizeof rule, "AAA-%d", i % 24); /* the footer agrees with the one type */
        write_v2(zone, (i % 24) * 3600, rule);
        int rc = day_bounds(zone, 0, 1, &first, &n);
        CHECK(rc == 0 && n == 1 && first == -(int64_t)(i % 24) * 3600000);
        loaded += rc == 0;
    }
    CHECK(loaded == MAX_CACHED_ZONES_TEST + 100);
    CHECK(cached_zones() == MAX_CACHED_ZONES_TEST);
}

/* a TZDIR so long that "TZDIR/name" does not fit the path buffer (4096): refused. It was cut, and named another
   file: here "Trun" for "Trunc" (the slashes pad TZDIR without changing where it points) */
static void long_tzdir(void)
{
    write_v2("Trun", 5 * 3600, "AAA-5");
    write_v2("Trunc", 3 * 3600, "AAA-3");
    static char dir[4096];
    size_t l = (size_t)snprintf(dir, sizeof dir, "%s", tzdir);
    memset(dir + l, '/', 4090 - l); /* 4090 + "/Trunc" is 4097 bytes with its NUL: cut to 4095, "/Trun" */
    dir[4090] = '\0';
    char *saved = strdup(getenv("TZDIR"));
    setenv("TZDIR", dir, 1);
    int64_t first;
    size_t n;
    CHECK(day_bounds("Trunc", 0, 1, &first, &n) == TSDB_ERR_INVALID);
    setenv("TZDIR", saved, 1);
    free(saved);
    CHECK(day_bounds("Trunc", 0, 1, &first, &n) == 0 && first == -3 * 3600000LL);
}

static int rm_one(const char *p, const struct stat *s, int flag, struct FTW *f)
{
    (void)s, (void)flag, (void)f;
    return remove(p);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    snprintf(tzdir, sizeof tzdir, "%s/tzfiles_%d", base, (int)getpid());
    nftw(tzdir, rm_one, 16, FTW_DEPTH | FTW_PHYS);
    if (mkdir(tzdir, 0755) != 0) {
        perror(tzdir);
        return 1;
    }
    setenv("TZDIR", tzdir, 1);
    test_tzif();
    fixed_offsets();
    zone_names();
    cache_bound();
    long_tzdir();
    nftw(tzdir, rm_one, 16, FTW_DEPTH | FTW_PHYS);
    if (failures) {
        fprintf(stderr, "test_tzfiles: %d failures\n", failures);
        return 1;
    }
    printf("test_tzfiles: ok\n");
    return 0;
}
