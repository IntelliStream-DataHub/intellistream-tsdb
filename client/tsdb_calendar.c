/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Calendar periods in a time zone, as UTC bucket bounds (tsdb_calendar.h).
 *
 * The database itself only knows UTC. Grouping by local days, weeks or months
 * happens on the client side: this file turns "per day in Europe/Oslo" into
 * the UTC instants at which each local day begins (23 or 25 hours apart
 * around DST changes), and the database groups by those bounds.
 *
 * Zones: UTC, fixed offsets ("+02:00", "-0530") and IANA names read from the
 * system's zoneinfo (TZif, versions 1 to 4). Instants after the last
 * transition in the file follow the POSIX rule in its footer, as in
 * "CET-1CEST,M3.5.0,M10.5.0/3", so DST keeps working for future years with
 * "slim" zoneinfo files too. Loaded zones are cached for the life of the
 * process.
 */
#include "tsdb_calendar.h"

#include <ctype.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct tz tz;

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MAX_BOUNDS (64u << 20)

static inline int64_t floor_div(int64_t a, int64_t b) /* b > 0 */
{
    int64_t q = a / b;
    return (a % b < 0) ? q - 1 : q;
}

typedef struct posix_rule {
    int month, week, wday; /* Mm.w.d */
    int32_t secs;          /* local time of day of the change */
} posix_rule;

struct tz {
    char *name;
    int64_t *trans;  /* UTC seconds of each transition */
    int32_t *after;  /* offset (seconds east) from that transition on */
    size_t ntrans;
    int32_t before;  /* offset before the first transition */
    bool has_rule, rule_dst;
    int32_t std_off, dst_off;
    posix_rule start, end; /* DST start (in standard time) and end (in DST) */
    struct tz *next;
};

static pthread_mutex_t cache_lock = PTHREAD_MUTEX_INITIALIZER;
static tz *cache;
static size_t cache_n;
#define MAX_CACHED_ZONES 1024 /* about 600 zones exist, and some fixed offsets */
#define MAX_ZONE_NAME 255

static int64_t days_from_civil(int64_t y, unsigned m, unsigned d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

static void tz_civil(int64_t days, int64_t *y, int *m, int *d)
{
    int64_t z = days + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    *d = (int)(doy - (153 * mp + 2) / 5 + 1);
    *m = (int)(mp < 10 ? mp + 3 : mp - 9);
    *y = (int64_t)yoe + era * 400 + (*m <= 2);
}

static int64_t tz_days_from_civil(int64_t y, int m, int d) { return days_from_civil(y, (unsigned)m, (unsigned)d); }

/* ------------------------------------------------------------ POSIX rule */

static const char *parse_name(const char *p)
{
    if (*p == '<') {
        while (*p && *p != '>')
            p++;
        return *p ? p + 1 : NULL;
    }
    const char *s = p;
    while (isalpha((unsigned char)*p))
        p++;
    return p - s >= 3 ? p : NULL;
}

/* Two digits below 60 (minutes, seconds); NULL otherwise. */
static const char *parse_2digits(const char *p, int *v)
{
    if (!isdigit((unsigned char)p[0]) || !isdigit((unsigned char)p[1]) || isdigit((unsigned char)p[2]))
        return NULL;
    *v = (p[0] - '0') * 10 + (p[1] - '0');
    return *v < 60 ? p + 2 : NULL;
}

/* [+-]hh[:mm[:ss]] -> seconds. Hours have 1 to 3 digits up to 167 (RFC 8536 allows that much for the time of
 * a rule's change), minutes and seconds 2 digits below 60: anything else is no offset, never an overflow. */
static const char *parse_hms(const char *p, int32_t *out)
{
    int sign = 1;
    if (*p == '+' || *p == '-')
        sign = *p++ == '-' ? -1 : 1;
    int h = 0, m = 0, s = 0, digits = 0;
    while (isdigit((unsigned char)*p) && digits < 4) {
        h = h * 10 + (*p++ - '0');
        digits++;
    }
    if (digits == 0 || digits > 3 || h > 167)
        return NULL;
    if (*p == ':') {
        if (!(p = parse_2digits(p + 1, &m)))
            return NULL;
        if (*p == ':' && !(p = parse_2digits(p + 1, &s)))
            return NULL;
    }
    *out = sign * (h * 3600 + m * 60 + s);
    return p;
}

static const char *parse_rule(const char *p, posix_rule *r)
{
    if (*p != 'M')
        return NULL; /* the Jn and n forms are not used by current zones */
    if (sscanf(p, "M%d.%d.%d", &r->month, &r->week, &r->wday) != 3 || r->month < 1 || r->month > 12 ||
        r->week < 1 || r->week > 5 || r->wday < 0 || r->wday > 6)
        return NULL; /* rule_utc counts on these ranges */
    p++;
    while (*p && *p != '/' && *p != ',')
        p++;
    r->secs = 7200;
    if (*p == '/' && !(p = parse_hms(p + 1, &r->secs)))
        return NULL;
    return p;
}

static void parse_footer(tz *z, const char *s)
{
    const char *p = parse_name(s);
    int32_t off;
    if (!p || !(p = parse_hms(p, &off)))
        return;
    z->std_off = -off; /* POSIX offsets are west of UTC */
    z->has_rule = true;
    if (!*p)
        return; /* no DST */
    if (!(p = parse_name(p)))
        return;
    z->dst_off = z->std_off + 3600;
    if (*p && *p != ',') {
        if (!(p = parse_hms(p, &off)))
            return;
        z->dst_off = -off;
    }
    if (*p != ',' || !(p = parse_rule(p + 1, &z->start)) || *p != ',' || !parse_rule(p + 1, &z->end)) {
        z->has_rule = false;
        return;
    }
    z->rule_dst = true;
}

/* UTC second at which rule r fires in year y, given the offset in force before it. */
static int64_t rule_utc(const posix_rule *r, int64_t y, int32_t off_before)
{
    int64_t first = days_from_civil(y, (unsigned)r->month, 1);
    int wd_first = (int)((first % 7 + 11) % 7); /* 1970-01-01 was a Thursday (4) */
    int64_t day = first + (r->wday - wd_first + 7) % 7 + (int64_t)(r->week - 1) * 7;
    int64_t month_end = days_from_civil(r->month == 12 ? y + 1 : y, (unsigned)(r->month == 12 ? 1 : r->month + 1), 1);
    while (day >= month_end)
        day -= 7; /* week 5 means the last one */
    return day * 86400 + r->secs - off_before;
}

static int32_t rule_offset(const tz *z, int64_t t)
{
    if (!z->rule_dst)
        return z->std_off;
    int64_t y;
    int m, d;
    tz_civil(t >= 0 ? t / 86400 : -((-t + 86399) / 86400), &y, &m, &d);
    int64_t on = rule_utc(&z->start, y, z->std_off), off = rule_utc(&z->end, y, z->dst_off);
    bool dst = on < off ? (t >= on && t < off) : !(t >= off && t < on);
    return dst ? z->dst_off : z->std_off;
}

/* ----------------------------------------------------------------- TZif */

static int64_t be(const uint8_t *p, int n)
{
    uint64_t v = 0;
    for (int i = 0; i < n; i++)
        v = v << 8 | p[i];
    return n == 4 ? (int64_t)(int32_t)v : (int64_t)v;
}

/* A header count: unsigned, as RFC 8536 has it (be() would make 0xffffffff -1). */
static uint64_t be_count(const uint8_t *p) { return (uint64_t)p[0] << 24 | (uint64_t)p[1] << 16 | p[2] << 8 | p[3]; }

static int load_tzif(tz *z, const uint8_t *b, size_t len)
{
    if (len < 44 || memcmp(b, "TZif", 4) != 0)
        return TSDB_ERR_CORRUPT;
    int version = b[4] ? b[4] - '0' : 1;
    const uint8_t *h = b;
    int tsize = 4;
    for (int pass = 0; pass < 2; pass++) {
        if ((size_t)(h - b) + 44 > len)
            return TSDB_ERR_CORRUPT;
        /* below 2^32 each, so the sum cannot wrap in 64 bits; checked against what is left before any use */
        uint64_t isutc = be_count(h + 20), isstd = be_count(h + 24), leap = be_count(h + 28);
        uint64_t timecnt = be_count(h + 32), typecnt = be_count(h + 36), charcnt = be_count(h + 40);
        uint64_t body = timecnt * (uint64_t)tsize + timecnt + typecnt * 6 + charcnt + leap * (uint64_t)(tsize + 4) +
                        isstd + isutc;
        if (body > len - (size_t)(h - b) - 44 || typecnt == 0)
            return TSDB_ERR_CORRUPT;
        if (version >= 2 && pass == 0) { /* skip the 32-bit data: the 64-bit copy follows */
            h += 44 + body;
            tsize = 8;
            continue;
        }
        const uint8_t *tt = h + 44, *idx = tt + timecnt * tsize, *types = idx + timecnt;
        /* RFC 8536: offsets lie within -25 and +26 hours; beyond them the bounds arithmetic means nothing */
        for (uint64_t k = 0; k < typecnt; k++) {
            int32_t off = (int32_t)be(types + k * 6, 4);
            if (off < -89999 || off > 93599)
                return TSDB_ERR_CORRUPT;
        }
        z->ntrans = (size_t)timecnt;
        z->trans = malloc((z->ntrans + 1) * sizeof *z->trans);
        z->after = malloc((z->ntrans + 1) * sizeof *z->after);
        if (!z->trans || !z->after)
            return TSDB_ERR_NOMEM;
        for (size_t i = 0; i < z->ntrans; i++) {
            z->trans[i] = be(tt + i * (size_t)tsize, tsize);
            uint8_t k = idx[i];
            if (k >= typecnt)
                return TSDB_ERR_CORRUPT;
            z->after[i] = (int32_t)be(types + k * 6, 4);
        }
        /* before the first transition: the first standard type, or type 0 */
        z->before = (int32_t)be(types, 4);
        for (uint64_t k = 0; k < typecnt; k++)
            if (!types[k * 6 + 4]) {
                z->before = (int32_t)be(types + k * 6, 4);
                break;
            }
        if (version >= 2) {
            const uint8_t *foot = h + 44 + body;
            if (foot < b + len && *foot == '\n') {
                const uint8_t *e = memchr(foot + 1, '\n', (size_t)(b + len - foot - 1));
                if (e) {
                    char rule[128];
                    size_t n = MIN((size_t)(e - foot - 1), sizeof rule - 1);
                    memcpy(rule, foot + 1, n);
                    rule[n] = '\0';
                    parse_footer(z, rule);
                }
            }
        }
        return 0;
    }
    return 0;
}

/* A zone name as IANA spells them: [A-Za-z0-9_+-] components joined by single '/', no '.' (so no "..", no
 * "./"), short. Every name then names one file, and the cache holds one entry per zone, not per spelling. */
static bool zone_name_valid(const char *name)
{
    size_t n = strlen(name);
    if (n == 0 || n > MAX_ZONE_NAME || name[0] == '/' || name[n - 1] == '/' || strstr(name, "//"))
        return false;
    for (const char *p = name; *p; p++)
        if (!isalnum((unsigned char)*p) && !strchr("_+-/", *p))
            return false;
    return true;
}

static int load_zone(const char *name, tz *z)
{
    int32_t off;
    const char *p;
    if (!strcmp(name, "UTC") || !strcmp(name, "Z") || !strcmp(name, "GMT") || !strcmp(name, "Etc/UTC"))
        return 0;
    if (strlen(name) > MAX_ZONE_NAME)
        return TSDB_ERR_INVALID;
    /* +0530 before +hh[:mm[:ss]]: read as hours, "0530" would be 530 of them */
    if ((name[0] == '+' || name[0] == '-') && strlen(name) == 5 && strspn(name + 1, "0123456789") == 4) {
        char buf[8] = {name[0], name[1], name[2], ':', name[3], name[4], 0};
        if ((p = parse_hms(buf, &off)) && !*p) {
            z->before = off;
            return 0;
        }
    }
    if (name[0] == '+' || name[0] == '-') {
        /* real offsets lie within -12 and +14 hours; 26 leaves room and keeps the arithmetic far from overflow */
        if ((p = parse_hms(name, &off)) && !*p && strlen(name) <= 9 && off >= -26 * 3600 && off <= 26 * 3600) {
            z->before = off;
            return 0;
        }
        return TSDB_ERR_INVALID;
    }
    if (!zone_name_valid(name))
        return TSDB_ERR_INVALID;
    const char *dir = getenv("TZDIR") ? getenv("TZDIR") : "/usr/share/zoneinfo";
    char path[4096];
    int pl = snprintf(path, sizeof path, "%s/%s", dir, name);
    if (pl < 0 || (size_t)pl >= sizeof path)
        return TSDB_ERR_INVALID; /* cut, it would name another file */
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_NOTFOUND;
    struct stat st;
    uint8_t *buf = NULL;
    int rc = TSDB_ERR_IO;
    if (fstat(fd, &st) == 0 && st.st_size > 0 && st.st_size < (1 << 20) && (buf = malloc((size_t)st.st_size)) &&
        read(fd, buf, (size_t)st.st_size) == st.st_size)
        rc = load_tzif(z, buf, (size_t)st.st_size);
    free(buf);
    close(fd);
    return rc;
}

static void tz_free(tz *z)
{
    if (z) {
        free(z->name);
        free(z->trans);
        free(z->after);
        free(z);
    }
}

/* The zone called name, from the cache when it is there. Only zones read from a file are cached: each must name
 * a real file, so the cache holds at most one entry per file under TZDIR. Fixed offsets cost no I/O and are parsed
 * on every call (*owned, the caller frees the zone), so callers cannot fill the cache with "+00:00:01"... and push
 * the real zones out. A full cache does not grow either: such a zone is loaded for the call alone. */
static int tz_get(const char *name, const tz **out, bool *owned)
{
    if (!name || !*name)
        name = "UTC";
    *owned = false;
    if (name[0] == '+' || name[0] == '-') {
        tz *z = calloc(1, sizeof *z);
        int rc = z && (z->name = strdup(name)) ? load_zone(name, z) : TSDB_ERR_NOMEM;
        if (rc) {
            tz_free(z);
            return rc;
        }
        *out = z;
        *owned = true;
        return 0;
    }
    pthread_mutex_lock(&cache_lock);
    for (tz *z = cache; z; z = z->next)
        if (!strcmp(z->name, name)) {
            pthread_mutex_unlock(&cache_lock);
            *out = z;
            return 0;
        }
    tz *z = calloc(1, sizeof *z);
    int rc = z && (z->name = strdup(name)) ? load_zone(name, z) : TSDB_ERR_NOMEM;
    if (rc) {
        tz_free(z);
        pthread_mutex_unlock(&cache_lock);
        return rc;
    }
    if (cache_n < MAX_CACHED_ZONES) {
        z->next = cache;
        cache = z;
        cache_n++;
    } else {
        *owned = true;
    }
    pthread_mutex_unlock(&cache_lock);
    *out = z;
    return 0;
}

/* Offset in seconds east of UTC at UTC second t. */
static int32_t tz_offset(const tz *z, int64_t t)
{
    if (z->ntrans == 0 || t < z->trans[0])
        return z->ntrans == 0 && z->has_rule ? rule_offset(z, t) : z->before;
    if (t >= z->trans[z->ntrans - 1] && z->has_rule)
        return rule_offset(z, t);
    size_t lo = 0, hi = z->ntrans; /* last transition <= t */
    while (hi - lo > 1) {
        size_t mid = (lo + hi) / 2;
        if (z->trans[mid] <= t)
            lo = mid;
        else
            hi = mid;
    }
    return z->after[lo];
}

/* The UTC second at which local time `local` (seconds) begins. Repeated local
 * times (DST ends) give the first; skipped ones (DST starts) give the moment
 * the clock jumps past them. */
static int64_t tz_utc(const tz *z, int64_t local)
{
    int32_t a = tz_offset(z, local - 2 * 86400), b = tz_offset(z, local + 2 * 86400);
    int32_t lo_off = MIN(a, b), hi_off = MAX(a, b);
    /* the earliest consistent instant: a larger offset means an earlier instant */
    int64_t u = local - hi_off;
    if (tz_offset(z, u) == hi_off)
        return u;
    u = local - lo_off;
    if (tz_offset(z, u) == lo_off)
        return u;
    /* skipped: the transition lies in (local - hi_off, local - lo_off] */
    int64_t lo = local - hi_off, hi = local - lo_off;
    int32_t before = tz_offset(z, lo);
    while (hi - lo > 1) {
        int64_t mid = lo + (hi - lo) / 2;
        if (tz_offset(z, mid) == before)
            lo = mid;
        else
            hi = mid;
    }
    return hi;
}


static int32_t tz_fixed_offset(const tz *z, bool *fixed)
{
    *fixed = z->ntrans == 0 && !z->rule_dst;
    return z->has_rule ? z->std_off : z->before;
}

/* ------------------------------------------------------------------ bounds */

static const int64_t unit_ms[] = {0, 1000, 60000, 3600000};

/* index of the local calendar unit (days, ISO weeks, months, quarters, years since 1970) */
static int64_t cal_index(const tz *z, tsdb_unit unit, int64_t ts)
{
    int64_t s = floor_div(ts, 1000), local = s + tz_offset(z, s);
    int64_t day = floor_div(local, 86400);
    switch (unit) {
    case TSDB_DAY: return day;
    case TSDB_WEEK: return floor_div(day + 3, 7); /* 1970-01-01 was a Thursday */
    default: {
        int64_t y;
        int m, d;
        tz_civil(day, &y, &m, &d);
        int64_t month = (y - 1970) * 12 + (m - 1);
        return unit == TSDB_MONTH ? month : unit == TSDB_QUARTER ? floor_div(month, 3) : y - 1970;
    }
    }
}

/* local seconds at which calendar unit `idx` begins */
static int64_t cal_local_start(tsdb_unit unit, int64_t idx)
{
    switch (unit) {
    case TSDB_DAY: return idx * 86400;
    case TSDB_WEEK: return (idx * 7 - 3) * 86400;
    case TSDB_MONTH:
    case TSDB_QUARTER: {
        int64_t month = unit == TSDB_MONTH ? idx : idx * 3;
        return tz_days_from_civil(1970 + floor_div(month, 12), (int)(month - floor_div(month, 12) * 12) + 1, 1) * 86400;
    }
    default: return tz_days_from_civil(1970 + idx, 1, 1) * 86400;
    }
}

static int calendar_bounds(const tz *z, tsdb_unit unit, uint32_t count, int64_t start, int64_t end, int64_t limit,
                           int64_t **bounds, size_t *n);

int tsdb_calendar_bounds(const char *zone, tsdb_unit unit, uint32_t count, int64_t start, int64_t end,
                         int64_t **bounds, size_t *n)
{
    *bounds = NULL;
    *n = 0;
    if (unit < TSDB_SECOND || unit > TSDB_YEAR)
        return TSDB_ERR_INVALID;
    if (start >= end)
        return 0;
    /* beyond a few hundred thousand years the arithmetic below would overflow */
    const int64_t limit = TSDB_CALENDAR_LIMIT_MS;
    if (start < -limit || end > limit)
        return TSDB_ERR_INVALID;
    const tz *z;
    bool owned;
    int rc = tz_get(zone, &z, &owned);
    if (rc)
        return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_INVALID : rc;
    rc = calendar_bounds(z, unit, count, start, end, limit, bounds, n);
    if (owned)
        tz_free((tz *)z);
    return rc;
}

static int calendar_bounds(const tz *z, tsdb_unit unit, uint32_t count, int64_t start, int64_t end, int64_t limit,
                           int64_t **bounds, size_t *n)
{
    int64_t c = count ? count : 1, k0, k1, interval = 0, shift = 0;
    if (unit <= TSDB_HOUR) {
        bool fixed;
        interval = unit_ms[unit] * c;
        shift = (int64_t)tz_fixed_offset(z, &fixed) * 1000;
        k0 = floor_div(start + shift, interval);
        k1 = floor_div(end - 1 + shift, interval);
    } else {
        k0 = floor_div(cal_index(z, unit, start), c);
        k1 = floor_div(cal_index(z, unit, end - 1), c);
        /* local time can go back by a day (Alaska in 1867: +15:02 to -08:57), and the range's end then lies in
         * an earlier local day than its start: take every period from the lower to the higher */
        if (k1 < k0) {
            int64_t k = k0;
            k0 = k1, k1 = k;
        }
    }
    if (k1 - k0 + 1 > (int64_t)MAX_BOUNDS)
        return TSDB_ERR_TOO_MANY_BUCKETS;
    int64_t *b = malloc((size_t)(k1 - k0 + 1) * sizeof *b);
    if (!b)
        return TSDB_ERR_NOMEM;
    size_t m = 0;
    for (int64_t k = k0; k <= k1; k++) {
        int64_t t;
        if (unit <= TSDB_HOUR) {
            t = k * interval - shift;
        } else {
            /* with a huge count the first period can begin far before the range we
             * take (only the first: the others begin inside it); in ms it would
             * overflow, so it begins at the beginning of time */
            int64_t s = tz_utc(z, cal_local_start(unit, k * c));
            t = s < -limit / 1000 ? INT64_MIN : s * 1000;
        }
        if (m == 0 || t > b[m - 1]) /* bounds strictly increase */
            b[m++] = t;
    }
    *bounds = b;
    *n = m;
    return 0;
}
