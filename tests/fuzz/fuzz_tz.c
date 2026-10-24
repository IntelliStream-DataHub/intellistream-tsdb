/*
 * Time zones as the clients read them (client/tsdb_calendar.c, included
 * here so its static parsers can be called): a TZif file from TZDIR, a zone
 * name (a fixed offset or a file under the system's zoneinfo), or a POSIX TZ
 * rule as found in a TZif footer, each followed by the bounds of a range of
 * local periods. The range is kept to some ten thousand periods, so an input
 * is judged by its parsing and arithmetic, not by how much memory a valid
 * request for millions of bounds takes.
 *
 * Input: byte 0 picks the form, byte 1 the unit, byte 2 the count, bytes 3..10
 * the start, 11..14 the length of the range; the rest is the file, name or rule.
 */
#include "fuzz.h"

#include "../../client/tsdb_calendar.c"

static const char *D;

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("tz");
    atexit(fuzz_rm_at_exit);
    return 0;
}

/* about ten thousand periods of each unit, in ms */
static const int64_t span_max[] = {0, 10000LL * 1000, 10000LL * 60000, 10000LL * 3600000, 10000LL * 86400000,
                                   10000LL * 604800000, 10000LL * 2678400000, 10000LL * 8035200000,
                                   10000LL * 31622400000};

static int64_t get64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v = v << 8 | p[i];
    return (int64_t)v;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size < 15)
        return 0;
    int form = data[0] % 3;
    tsdb_unit unit = (tsdb_unit)(TSDB_SECOND + data[1] % (TSDB_YEAR - TSDB_SECOND + 1));
    uint32_t count = data[2] % 8;
    int64_t start = get64(data + 3) % TSDB_CALENDAR_LIMIT_MS;
    uint32_t len32 = (uint32_t)data[11] << 24 | (uint32_t)data[12] << 16 | (uint32_t)data[13] << 8 | data[14];
    int64_t span = (int64_t)((double)len32 / 4294967296.0 * (double)span_max[unit]) * (count ? count : 1);
    int64_t end = start + span;
    if (end > TSDB_CALENDAR_LIMIT_MS)
        end = TSDB_CALENDAR_LIMIT_MS;
    data += 15, size -= 15;

    tz *z = calloc(1, sizeof *z);
    int rc = 0;
    if (form == 0) {
        fuzz_put(D, "Fz", data, size);
        setenv("TZDIR", D, 1);
        rc = load_zone("Fz", z);
    } else {
        char *s = malloc(size + 1);
        memcpy(s, data, size);
        s[size] = 0;
        if (form == 1) {
            unsetenv("TZDIR");
            rc = load_zone(s, z);
        } else {
            parse_footer(z, s);
        }
        free(s);
    }
    if (rc == 0) {
        int64_t *b;
        size_t n;
        if (calendar_bounds(z, unit, count, start, end, TSDB_CALENDAR_LIMIT_MS, &b, &n) == 0)
            free(b);
    }
    tz_free(z);
    return 0;
}

static void seed(const char *dir, const char *name, int form, int unit, int64_t start, uint32_t len, const void *d,
                 size_t n)
{
    uint8_t *b = malloc(15 + n);
    b[0] = (uint8_t)form, b[1] = (uint8_t)(unit - TSDB_SECOND), b[2] = 1;
    for (int i = 0; i < 8; i++)
        b[3 + i] = (uint8_t)((uint64_t)start >> (56 - 8 * i));
    for (int i = 0; i < 4; i++)
        b[11 + i] = (uint8_t)(len >> (24 - 8 * i));
    memcpy(b + 15, d, n);
    fuzz_put(dir, name, b, 15 + n);
    free(b);
}

void fuzz_seeds(const char *dir)
{
    const int64_t y2024 = 1704067200000;
    size_t n;
    uint8_t *oslo = fuzz_get("/usr/share/zoneinfo/Europe", "Oslo", &n);
    if (oslo)
        seed(dir, "seed-tzif-oslo", 0, TSDB_DAY, y2024, 1u << 26, oslo, n);
    free(oslo);
    static const struct {
        const char *file, *s;
        int form, unit;
    } t[] = {
        {"seed-name-oslo", "Europe/Oslo", 1, TSDB_HOUR},
        {"seed-name-offset", "+05:30", 1, TSDB_DAY},
        {"seed-name-offset4", "-0930", 1, TSDB_MONTH},
        {"seed-rule-cet", "CET-1CEST,M3.5.0,M10.5.0/3", 2, TSDB_DAY},
        {"seed-rule-angle", "<+0330>-3:30", 2, TSDB_WEEK},
        {"seed-rule-ist", "IST-2IDT,M3.4.4/26,M10.5.0", 2, TSDB_HOUR},
        {"seed-rule-nz", "NZST-12NZDT,M9.5.0,M4.1.0/3", 2, TSDB_YEAR},
    };
    for (size_t i = 0; i < sizeof t / sizeof t[0]; i++)
        seed(dir, t[i].file, t[i].form, t[i].unit, y2024, 1u << 28, t[i].s, strlen(t[i].s));
}
