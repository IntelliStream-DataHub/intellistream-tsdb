/*
 * Time zones for calendar grouping: UTC, fixed offsets ("+02:00", "-0530")
 * and IANA names read from the system's zoneinfo (TZif, versions 1 to 4).
 * Instants after the last transition in the file follow the POSIX rule in
 * its footer, as in "CET-1CEST,M3.5.0,M10.5.0/3", so DST keeps working for
 * future years with "slim" zoneinfo files too.
 *
 * Loaded zones are cached for the life of the process.
 */
#include "internal.h"

#include <ctype.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

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

static int64_t days_from_civil(int64_t y, unsigned m, unsigned d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

void tz_civil(int64_t days, int64_t *y, int *m, int *d)
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

int64_t tz_days_from_civil(int64_t y, int m, int d) { return days_from_civil(y, (unsigned)m, (unsigned)d); }

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

/* [+-]hh[:mm[:ss]] -> seconds */
static const char *parse_hms(const char *p, int32_t *out)
{
    int sign = 1;
    if (*p == '+' || *p == '-')
        sign = *p++ == '-' ? -1 : 1;
    if (!isdigit((unsigned char)*p))
        return NULL;
    int h = 0, m = 0, s = 0;
    while (isdigit((unsigned char)*p))
        h = h * 10 + (*p++ - '0');
    if (*p == ':') {
        p++;
        while (isdigit((unsigned char)*p))
            m = m * 10 + (*p++ - '0');
        if (*p == ':') {
            p++;
            while (isdigit((unsigned char)*p))
                s = s * 10 + (*p++ - '0');
        }
    }
    *out = sign * (h * 3600 + m * 60 + s);
    return p;
}

static const char *parse_rule(const char *p, posix_rule *r)
{
    if (*p != 'M')
        return NULL; /* the Jn and n forms are not used by current zones */
    if (sscanf(p, "M%d.%d.%d", &r->month, &r->week, &r->wday) != 3)
        return NULL;
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
        int64_t isutc = be(h + 20, 4), isstd = be(h + 24, 4), leap = be(h + 28, 4);
        int64_t timecnt = be(h + 32, 4), typecnt = be(h + 36, 4), charcnt = be(h + 40, 4);
        size_t body = (size_t)(timecnt * tsize + timecnt + typecnt * 6 + charcnt + leap * (tsize + 4) + isstd + isutc);
        if ((size_t)(h - b) + 44 + body > len || typecnt <= 0)
            return TSDB_ERR_CORRUPT;
        if (version >= 2 && pass == 0) { /* skip the 32-bit data: the 64-bit copy follows */
            h += 44 + body;
            tsize = 8;
            continue;
        }
        const uint8_t *tt = h + 44, *idx = tt + timecnt * tsize, *types = idx + timecnt;
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
        for (int64_t k = 0; k < typecnt; k++)
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

static int load_zone(const char *name, tz *z)
{
    int32_t off;
    const char *p;
    if (!strcmp(name, "UTC") || !strcmp(name, "Z") || !strcmp(name, "GMT") || !strcmp(name, "Etc/UTC"))
        return 0;
    if ((name[0] == '+' || name[0] == '-') && (p = parse_hms(name, &off)) && !*p) {
        z->before = off;
        return 0;
    }
    if ((name[0] == '+' || name[0] == '-') && strlen(name) == 5) { /* +0530 */
        char buf[8] = {name[0], name[1], name[2], ':', name[3], name[4], 0};
        if ((p = parse_hms(buf, &off)) && !*p) {
            z->before = off;
            return 0;
        }
    }
    if (strstr(name, "..") || name[0] == '/')
        return TSDB_ERR_INVALID;
    const char *dir = getenv("TZDIR") ? getenv("TZDIR") : "/usr/share/zoneinfo";
    char path[512];
    snprintf(path, sizeof path, "%s/%s", dir, name);
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

int tz_get(const char *name, const tz **out)
{
    if (!name || !*name)
        name = "UTC";
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
        if (z) {
            free(z->name);
            free(z->trans);
            free(z->after);
            free(z);
        }
        pthread_mutex_unlock(&cache_lock);
        return rc;
    }
    z->next = cache;
    cache = z;
    pthread_mutex_unlock(&cache_lock);
    *out = z;
    return 0;
}

/* Offset in seconds east of UTC at UTC second t. */
int32_t tz_offset(const tz *z, int64_t t)
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
int64_t tz_utc(const tz *z, int64_t local)
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

/* The first UTC second after t at which the offset changes (INT64_MAX if never). */
int64_t tz_next_transition(const tz *z, int64_t t)
{
    if (z->ntrans && t < z->trans[z->ntrans - 1]) {
        size_t lo = 0, hi = z->ntrans; /* first transition > t */
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            if (z->trans[mid] <= t)
                lo = mid + 1;
            else
                hi = mid;
        }
        return z->trans[lo];
    }
    if (!z->rule_dst)
        return INT64_MAX;
    int64_t y;
    int m, d;
    tz_civil(t >= 0 ? t / 86400 : -((-t + 86399) / 86400), &y, &m, &d);
    int64_t best = INT64_MAX;
    for (int64_t yy = y; yy <= y + 1; yy++) {
        int64_t a = rule_utc(&z->start, yy, z->std_off), b = rule_utc(&z->end, yy, z->dst_off);
        if (a > t && a < best)
            best = a;
        if (b > t && b < best)
            best = b;
    }
    return best;
}

int32_t tz_fixed_offset(const tz *z, bool *fixed)
{
    *fixed = z->ntrans == 0 && !z->rule_dst;
    return z->has_rule ? z->std_off : z->before;
}
