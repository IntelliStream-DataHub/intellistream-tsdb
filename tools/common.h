/* Helpers shared by the command line tools: DateTime64(3) parsing/printing and timing. */
#ifndef TSDB_TOOLS_COMMON_H
#define TSDB_TOOLS_COMMON_H

#define _GNU_SOURCE
#include <ctype.h>
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
        if ((s[3] == '-' || s[3] == '+') && parse_interval(s + 4, &d)) {
            *out = s[3] == '-' ? t - d : t + d;
            return true;
        }
        return false;
    }
    int y, mo, d, h = 0, mi = 0, sec = 0, n = 0;
    if (sscanf(s, "%d-%d-%d%n", &y, &mo, &d, &n) == 3 && n >= 8) {
        const char *p = s + n;
        int ms = 0;
        if (*p == ' ' || *p == 'T') {
            int m2 = 0;
            if (sscanf(p + 1, "%d:%d:%d%n", &h, &mi, &sec, &m2) != 3)
                return false;
            p += 1 + m2;
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
        if (*p || mo < 1 || mo > 12 || d < 1 || d > 31)
            return false;
        *out = ((days_from_civil(y, (unsigned)mo, (unsigned)d) * 24 + h) * 60 + mi) * 60000LL + sec * 1000LL + ms;
        return true;
    }
    char *end;
    long long v = strtoll(s, &end, 10);
    if (end == s)
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
    if (end == s || v < 0)
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
    *out = (int64_t)(v * (double)mult);
    return true;
}

/* Formats like ClickHouse DateTime64(3) in UTC: "2024-01-31 23:59:59.123". */
static inline void format_time(int64_t ms, char *buf, size_t len)
{
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
