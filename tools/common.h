/* Helpers shared by the command line tools: DateTime64(3) parsing/printing and timing. */
#ifndef TSDB_TOOLS_COMMON_H
#define TSDB_TOOLS_COMMON_H

#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static inline double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static inline int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm). */
static inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

static inline int tools_days_in_month(int64_t y, int m)
{
    static const unsigned char dm[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m < 1 || m > 12)
        return 0; /* no such month: no day is valid */
    return dm[m - 1] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
}

/* Reads min..max decimal digits at *p into *out and advances *p; false if there are fewer or more. */
static inline bool tools_take_num(const char **p, int min, int max, int64_t *out)
{
    int64_t v = 0;
    int n = 0;
    while (isdigit((unsigned char)**p)) {
        if (++n > max)
            return false;
        v = v * 10 + (**p - '0');
        (*p)++;
    }
    *out = v;
    return n >= min;
}

/*
 * Accepts: integer milliseconds, "now", "min", "max",
 * "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS[.fff]", "YYYY-MM-DDTHH:MM:SS[.fff][Z]" (UTC),
 * and "now-1h" style offsets (units ms, s, m, h, d).
 */
static inline bool parse_interval(const char *s, int64_t *out);

static inline bool parse_time(const char *s, int64_t *out)
{
    while (isspace((unsigned char)*s))
        s++;
    if (strcmp(s, "min") == 0) {
        *out = INT64_MIN;
        return true;
    }
    if (strcmp(s, "max") == 0) {
        *out = INT64_MAX;
        return true;
    }
    if (strncmp(s, "now", 3) == 0) {
        int64_t t = now_ms(), d = 0;
        if (s[3] == '\0') {
            *out = t;
            return true;
        }
        /* a destructive command (drop-before, delete) must never act on a time that wrapped */
        if ((s[3] == '-' || s[3] == '+') && parse_interval(s + 4, &d))
            return !(s[3] == '-' ? __builtin_sub_overflow(t, d, out) : __builtin_add_overflow(t, d, out));
        return false;
    }
    /* A date: digits, then '-'. Every field has a bounded number of digits and a range, so nothing
     * overflows and a typo like 99:99:99 or 02-31 is refused instead of read as another time. */
    const char *p = s + (*s == '-');
    const char *q = p;
    while (isdigit((unsigned char)*q))
        q++;
    if (q > p && *q == '-') {
        int64_t y;
        int mo, d, h = 0, mi = 0, sec = 0, ms = 0;
        if (!tools_take_num(&p, 4, 6, &y) || *p++ != '-')
            return false;
        int64_t f;
        if (!tools_take_num(&p, 1, 2, &f) || (mo = (int)f) < 1 || mo > 12 || *p++ != '-' ||
            !tools_take_num(&p, 1, 2, &f) || (d = (int)f) < 1 || d > tools_days_in_month(y, mo))
            return false;
        if (*p == ' ' || *p == 'T') {
            p++;
            if (!tools_take_num(&p, 1, 2, &f) || (h = (int)f) > 23 || *p++ != ':' ||
                !tools_take_num(&p, 1, 2, &f) || (mi = (int)f) > 59 || *p++ != ':' ||
                !tools_take_num(&p, 1, 2, &f) || (sec = (int)f) > 59)
                return false;
            if (*p == '.') {
                int digits = 0;
                p++;
                while (isdigit((unsigned char)*p)) {
                    if (digits < 3) {
                        ms = ms * 10 + (*p - '0');
                        digits++;
                    }
                    p++;
                }
                while (digits++ < 3)
                    ms *= 10;
            }
            if (*p == 'Z')
                p++;
        }
        while (isspace((unsigned char)*p))
            p++;
        if (*p)
            return false;
        if (*s == '-')
            y = -y;
        /* |y| < 10^6 years is about 3.2e16 ms: far inside int64 */
        *out = ((days_from_civil(y, (unsigned)mo, (unsigned)d) * 24 + h) * 60 + mi) * 60000LL + sec * 1000LL + ms;
        return true;
    }
    char *end;
    errno = 0;
    long long v = strtoll(s, &end, 10);
    if (end == s || errno == ERANGE) /* strtoll clamps; a clamped time is not the one asked for */
        return false;
    while (isspace((unsigned char)*end))
        end++;
    if (*end)
        return false;
    *out = v;
    return true;
}

/* "1500", "500ms", "10s", "5m", "1h", "1d", "1w" -> milliseconds */
static inline bool parse_interval(const char *s, int64_t *out)
{
    char *end;
    double v = strtod(s, &end);
    if (end == s || !(v >= 0))
        return false;
    int64_t mult = 1;
    if (*end == '\0' || strcmp(end, "ms") == 0)
        mult = 1;
    else if (strcmp(end, "s") == 0)
        mult = 1000;
    else if (strcmp(end, "m") == 0)
        mult = 60000;
    else if (strcmp(end, "h") == 0)
        mult = 3600000;
    else if (strcmp(end, "d") == 0)
        mult = 86400000;
    else if (strcmp(end, "w") == 0)
        mult = 7 * 86400000LL;
    else
        return false;
    /* NaN, inf and anything past int64 are refused before the cast, which is undefined for them */
    double x = v * (double)mult;
    if (!(x < 9223372036854775808.0))
        return false;
    *out = (int64_t)x;
    return true;
}

/* Formats like ClickHouse DateTime64(3) in UTC: "2024-01-31 23:59:59.123". */
static inline void format_time(int64_t ms, char *buf, size_t len)
{
    if (ms == INT64_MIN || ms == INT64_MAX) { /* the ends of the time line, as parse_time reads them */
        snprintf(buf, len, "%s", ms == INT64_MIN ? "min" : "max");
        return;
    }
    int64_t secs = ms >= 0 ? ms / 1000 : -((-ms + 999) / 1000);
    int frac = (int)(ms - secs * 1000);
    int64_t days = secs >= 0 ? secs / 86400 : -((-secs + 86399) / 86400);
    int64_t rem = secs - days * 86400;
    /* civil_from_days */
    int64_t z = days + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y = (int64_t)yoe + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned d = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp < 10 ? mp + 3 : mp - 9;
    y += m <= 2;
    snprintf(buf, len, "%04" PRId64 "-%02u-%02u %02d:%02d:%02d.%03d", y, m, d, (int)(rem / 3600),
             (int)(rem / 60 % 60), (int)(rem % 60), frac);
}

#endif
