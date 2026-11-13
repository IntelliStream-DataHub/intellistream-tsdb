/* SPDX-License-Identifier: AGPL-3.0-only */
/* Command line client for a tsdb directory. */
#include "cfg.h"
#include "common.h"
#include "dump.h"
#include "tsdb.h"
#include "../client/tsdb_client.h"
#include "tsdb_calendar.h"

#include <errno.h>
#include <grp.h>
#include <limits.h>
#include <math.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef TSDB_BUILD_VERSION
#define TSDB_BUILD_VERSION "dev"
#endif

static void usage(void) __attribute__((noreturn));
static void usage(void)
{
    fprintf(stderr,
            "usage: tsdb <dir> <command> [args]\n"
            "       tsdb cluster secret|init|node add|status ... (tsdb cluster for the details)\n"
            "       tsdb --version\n"
            "\n"
            "  insert <series>                    read \"time,value\" lines from stdin\n"
            "  import [FILE]                      read \"series,time,value\" lines from FILE or stdin\n"
            "                                     (values by the series' type; null or empty: null;\n"
            "                                     new series are float32), or a stream of export:\n"
            "  export [-o FILE] [--series PATTERN]... [--from TIME] [--to TIME] [--zstd-level N] [--force]\n"
            "                                     write the series (all, or those matching a glob\n"
            "                                     PATTERN) with type, volume, retention and states to\n"
            "                                     FILE or stdout: what queries see, deletes applied;\n"
            "                                     read-only, also while tsdbd runs (one snapshot per\n"
            "                                     series). For backups and moving to a new version.\n"
            "                                     FILE is written as FILE.partial, then renamed; an\n"
            "                                     existing FILE.partial is refused unless --force\n"
            "  import [FILE] [--skip-existing|--merge|--replace] [--volume OLD=NEW]... [--volume-root OLD=NEW]...\n"
            "                [--create-volumes] [--no-retention] [--no-verify] [--batch-rows N]\n"
            "                                     read an export stream (checked first when FILE is a\n"
            "                                     file: a damaged one writes nothing). A series that\n"
            "                                     exists is refused, skipped, written into (last write\n"
            "                                     wins) or has the exported range replaced (FILE only);\n"
            "                                     --volume moves series of volume OLD to NEW (empty:\n"
            "                                     the database directory), --volume-root every volume\n"
            "                                     under OLD to the same place under NEW. Into an\n"
            "                                     existing database: a crash or a failure midway,\n"
            "                                     rerun it with --merge. Into a new database (missing\n"
            "                                     or empty directory): built in DIR.importing-PID,\n"
            "                                     synced once at the end, then renamed to DIR; an\n"
            "                                     interrupted import leaves no DIR (delete what the\n"
            "                                     next import names, then import again)\n"
            "  import --check [FILE]              verify an export stream without writing anything\n"
            "  query <series> <start> <end> [raw|count|sum|min|max|avg|all] [group] [--tz ZONE]\n"
            "        also range, stddev_pop, stddev_samp, var_pop, var_samp (stddev, var: the _pop ones)\n"
            "        [--desc] [--limit N]         newest first; at most N rows or buckets\n"
            "  query <series> <start> <end> lttb N  N points that keep the shape (for charts);\n"
            "                                     --desc and --limit as above\n"
            "  query <series> <start> <end> twa [group] [--linear] [--max-gap INTERVAL] [--tz ZONE]\n"
            "                                     time-weighted average: each value weighted by how\n"
            "                                     long it held (step; --linear: interpolated), not\n"
            "                                     across gaps over --max-gap; columns twa, area\n"
            "                                     (value x ms), covered_ms, count\n"
            "  create <series> [path] [--type T]  create a series of type T (float32, float64, int64,\n"
            "                                     bool, state), in volume <path> if given\n"
            "                                     (an existing volume: see volume add)\n"
            "  series                             list series (type, volume, partition class)\n"
            "  volume add <path>                  add a volume: a directory for series of their own\n"
            "                                     (e.g. one per disk or NFS mount); also while tsdbd runs\n"
            "  volume list                        list the volumes, the database directory first\n"
            "  upgrade                            carry a database closed cleanly by the previous release\n"
            "                                     (disk format 8) over to this one, in place; stop followers first\n"
            "  latest NAME... [--with-nulls] [--end TIME]\n"
            "                                     the newest value of each series, one line each: name, time,\n"
            "                                     value (no row: \\N \\N; with --with-nulls a null row: time \\N);\n"
            "                                     --end: only rows before TIME\n"
            "  feed enable [--retention AGE] [--max-bytes N[K|M|G|T]] [--min-free-pct N]\n"
            "                                     keep the WAL segments a flush no longer needs, for the change\n"
            "                                     feed (default 24h and 64G per volume, 10%% free space); not\n"
            "                                     beside a running tsdbd\n"
            "  feed disable                       stop keeping them and remove the kept segments\n"
            "  feed info                          per volume: on or off, feed id, limits, floor, kept segments\n"
            "  feed new-id                        a new feed id for every volume (after restoring a backup)\n"
            "  stats                              rows, parts, size, compression ratio, where the bytes go\n"
            "  stats series NAME...               rows, bytes, parts and range of each series\n"
            "  stats tenants [PREFIX...] [--volumes] [--tenants FILE]\n"
            "                                     rows and bytes per tenant: the series whose name starts with\n"
            "                                     a prefix (the longest wins; the rest are \"(none)\"); prefixes\n"
            "                                     from the arguments and FILE (one per line, # comments)\n"
            "  stats hot [reads|writes|bytes] [-k N]  the series with the most bytes per row (reads and writes\n"
            "                                     are counted by a running server, so they are empty here)\n"
            "  stats series|tenants|hot ... --server HOST:PORT [--login NAME [--login-password-file F]]\n"
            "                                     the same through a running tsdbd (no directory), as the user\n"
            "                                     who logs in: only the tenants and series that user may see;\n"
            "                                     tenants also takes --caller (what its own requests cost) and\n"
            "                                     --hot K (its hottest series); hot takes reads|writes|bytes -k N\n"
            "  parts                              list active parts (partition class and range)\n"
            "  optimize                           merge every partition into one part\n"
            "  drop-before <time>                 drop whole partitions ending before <time>\n"
            "  delete <series> <start> <end>      delete the series' values in [start, end)\n"
            "  retention <series> [<age>|off]     show or set how long the series keeps values\n"
            "                                     (e.g. 30d, 12h; off keeps them for ever)\n"
            "  user add|passwd|drop|alter|grant|revoke|list|compact ...\n"
            "                                     the users of tsdbd's login (USERS), also while tsdbd\n"
            "                                     runs; `tsdb <dir> user` for the details\n"
            "\n"
            "times: 1700000000000 (ms), 2024-01-31, \"2024-01-31 12:00:00.250\", 2024-01-31T12:00:00Z,\n"
            "       now, now-1h, min, max.  ranges are [start, end).\n"
            "groups: 500ms, 10s, 15m, 6h (fixed length); 1d, 1w, 1mo, 1q, 1y (calendar, in --tz,\n"
            "        e.g. Europe/Oslo; default UTC). Output is ClickHouse-style TSV, times in UTC;\n"
            "        names escaped (\\t, \\n, \\\\, other control bytes as \\xNN).\n"
            "root:   run as root on a <dir> another user owns, tsdb becomes that user and group first,\n"
            "        for every command (the owner could put links in <dir> that root would follow), and\n"
            "        opens the files it names (--password-file, -o, import FILE) as that user too.\n");
    exit(2);
}

int user_main(const char *dir, int argc, char **argv); /* tools/users-cli.c */
int cluster_cli(int argc, char **argv);                /* tools/cluster-cli.c */
int cli_connect(const char *server, const char *login, const char *login_pwfile, tsdbc **out); /* users-cli.c */

static void die(const char *what, int rc)
{
    fprintf(stderr, "tsdb: %s: %s\n", what, tsdb_strerror(rc));
    exit(1);
}

/* ------------------------------------------------------------------ import */

static const char *type_names[] = {"float32", "float64", "int64", "bool", "state"};

static int parse_type(const char *s)
{
    for (int t = 0; t < 5; t++)
        if (strcmp(s, type_names[t]) == 0)
            return t;
    return -1;
}

/* One batch per value type: a batch is written with one tsdb_insert_values. */
typedef struct batch {
    tsdb_type type;
    tsdb_sid *s;
    int64_t *t;
    uint8_t *v;     /* values of the type (bool: bytes) */
    uint8_t *valid; /* bitmap */
    bool nulls;
    size_t n, cap;
} batch;

static size_t api_width(tsdb_type t) { return t == TSDB_BOOL ? 1 : t == TSDB_FLOAT32 || t == TSDB_STATE ? 4 : 8; }

static void batch_flush(tsdb *db, batch *b, size_t *total)
{
    if (!b->n)
        return;
    tsdb_values col = {b->type, b->v, b->nulls ? b->valid : NULL};
    int rc = tsdb_insert_values(db, b->s, b->t, &col, b->n);
    if (rc)
        die("insert", rc);
    *total += b->n;
    b->n = 0;
    b->nulls = false;
}

static void batch_push(batch *b, tsdb_sid s, int64_t t, const void *v, bool known)
{
    if (b->n == b->cap) {
        b->cap = b->cap ? b->cap * 2 : 65536;
        b->s = realloc(b->s, b->cap * sizeof *b->s);
        b->t = realloc(b->t, b->cap * sizeof *b->t);
        b->v = realloc(b->v, b->cap * 8);
        b->valid = realloc(b->valid, b->cap / 8);
        if (!b->s || !b->t || !b->v || !b->valid)
            die("import", TSDB_ERR_NOMEM);
    }
    size_t w = api_width(b->type), i = b->n;
    b->s[i] = s;
    b->t[i] = t;
    memcpy(b->v + i * w, v, w);
    if (i % 8 == 0)
        b->valid[i / 8] = 0;
    b->valid[i / 8] |= (uint8_t)(known << (i % 8));
    b->nulls |= !known;
    b->n++;
}

/* Writes a name into TSV output escaped as ClickHouse does (\\t, \\n, \\\\, other control bytes as \\xNN):
 * names may hold any byte but NUL, and a raw tab, newline or ESC would forge rows or reach the terminal. */
static void put_name(FILE *f, const char *s)
{
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        if (*p == '\t')
            fputs("\\t", f);
        else if (*p == '\n')
            fputs("\\n", f);
        else if (*p == '\\')
            fputs("\\\\", f);
        else if (*p < 0x20 || *p == 0x7f)
            fprintf(f, "\\x%02x", *p);
        else
            fputc(*p, f);
    }
}

/* die() for a name read from an import file: escaped, so its bytes cannot drive the terminal. */
static void die_name(const char *name, int rc)
{
    fputs("tsdb: ", stderr);
    put_name(stderr, name);
    fprintf(stderr, ": %s\n", tsdb_strerror(rc));
    exit(1);
}

/* Parses a value of `type` into out (its API layout); false if it does not parse. */
static bool parse_value(tsdb *db, tsdb_sid series, tsdb_type type, const char *vs, uint8_t out[8], bool *known)
{
    *known = !(vs[0] == '\0' || strcmp(vs, "null") == 0 || strcmp(vs, "NULL") == 0 || strcmp(vs, "\\N") == 0);
    memset(out, 0, 8);
    if (!*known)
        return true;
    /* Out of range is refused, not clamped to the largest value or to inf, and the bit patterns reserved for
     * null are refused rather than stored as a null nobody wrote (as the Pulsar sink does). An explicit
     * "inf" is a value; "1e999" is an overflow. */
    char *end;
    errno = 0;
    switch (type) {
    case TSDB_FLOAT32: {
        float f = strtof(vs, &end);
        uint32_t bits;
        memcpy(&bits, &f, 4);
        memcpy(out, &f, 4);
        return end != vs && !*end && !(errno == ERANGE && isinf(f)) && bits != TSDB_NULL_F32_BITS;
    }
    case TSDB_FLOAT64: {
        double d = strtod(vs, &end);
        uint64_t bits;
        memcpy(&bits, &d, 8);
        memcpy(out, &d, 8);
        return end != vs && !*end && !(errno == ERANGE && isinf(d)) && bits != TSDB_NULL_F64_BITS;
    }
    case TSDB_INT64: {
        long long x = strtoll(vs, &end, 10);
        int64_t i = x;
        memcpy(out, &i, 8);
        return end != vs && !*end && errno != ERANGE && i != INT64_MIN;
    }
    case TSDB_BOOL:
        out[0] = strcmp(vs, "1") == 0 || strcmp(vs, "true") == 0;
        return out[0] || strcmp(vs, "0") == 0 || strcmp(vs, "false") == 0;
    default: {
        uint32_t code;
        int rc = tsdb_state(db, series, vs, &code);
        if (rc)
            die(vs, rc);
        memcpy(out, &code, 4);
        return true;
    }
    }
}

static int cmd_import(tsdb *db, const char *fixed_series, FILE *in)
{
    tsdb_sid fixed = 0;
    tsdb_type fixed_type = TSDB_FLOAT32;
    int rc;
    if (fixed_series && ((rc = tsdb_series(db, fixed_series, &fixed)) != 0 ||
                         (rc = tsdb_series_type(db, fixed, &fixed_type)) != 0))
        die(fixed_series, rc);
    batch b[5] = {{.type = TSDB_FLOAT32}, {.type = TSDB_FLOAT64}, {.type = TSDB_INT64}, {.type = TSDB_BOOL}, {.type = TSDB_STATE}};
    char *line = NULL;
    size_t cap = 0, lineno = 0, total = 0;
    ssize_t len;
    /* remember the last series name to avoid a dictionary lookup per line; a copy of its full length, so a
     * name is never mistaken for a longer one that shares its first bytes */
    char *last_name = NULL;
    size_t last_cap = 0;
    tsdb_sid last_id = 0;
    tsdb_type last_type = TSDB_FLOAT32;
    double t0 = now_s();
    while ((len = getline(&line, &cap, in)) > 0) {
        lineno++;
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0 || line[0] == '#')
            continue;
        char *fields[3];
        int nf = 0;
        char *p = line;
        fields[nf++] = p;
        while (*p && nf < 3) {
            if (*p == ',' || *p == '\t') {
                *p = '\0';
                fields[nf++] = p + 1;
            }
            p++;
        }
        int need = fixed_series ? 2 : 3;
        if (nf != need) {
            fprintf(stderr, "tsdb: line %zu: expected %d fields\n", lineno, need);
            return 1;
        }
        tsdb_sid id = fixed;
        tsdb_type type = fixed_type;
        if (!fixed_series) {
            if (!last_name || strcmp(fields[0], last_name) != 0) {
                if ((rc = tsdb_series(db, fields[0], &last_id)) != 0 ||
                    (rc = tsdb_series_type(db, last_id, &last_type)) != 0)
                    die_name(fields[0], rc);
                size_t n = strlen(fields[0]) + 1;
                if (n > last_cap) {
                    free(last_name);
                    if (!(last_name = malloc(last_cap = n)))
                        die("out of memory", TSDB_ERR_NOMEM);
                }
                memcpy(last_name, fields[0], n);
            }
            id = last_id;
            type = last_type;
        }
        int64_t t;
        uint8_t v[8];
        bool known;
        const char *ts = fields[need - 2], *vs = fields[need - 1];
        if (!parse_time(ts, &t) || !parse_value(db, id, type, vs, v, &known)) {
            fprintf(stderr, "tsdb: line %zu: cannot parse \"", lineno);
            put_name(stderr, ts);
            fputc(',', stderr);
            put_name(stderr, vs);
            fprintf(stderr, "\" as time, %s\n", type_names[type]);
            return 1;
        }
        batch_push(&b[type], id, t, v, known);
        if (b[type].n >= (1u << 20))
            batch_flush(db, &b[type], &total);
    }
    for (int k = 0; k < 5; k++) {
        batch_flush(db, &b[k], &total);
        free(b[k].s);
        free(b[k].t);
        free(b[k].v);
        free(b[k].valid);
    }
    double dt = now_s() - t0;
    fprintf(stderr, "inserted %zu rows in %.2f s (%.0f rows/s)\n", total, dt, total / (dt > 0 ? dt : 1));
    free(line);
    free(last_name);
    return 0;
}

/* ------------------------------------------------------------------- query */

typedef struct raw_out {
    FILE *f;
    size_t n;
    tsdb *db;
    tsdb_sid series;
} raw_out;

static int print_raw(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    raw_out *o = ctx;
    char tb[40];
    for (size_t i = 0; i < n; i++) {
        format_time(ts[i], tb, sizeof tb);
        if (col->valid && !((col->valid[i >> 3] >> (i & 7)) & 1)) {
            fprintf(o->f, "%s\t\\N\n", tb); /* null, as ClickHouse writes it in TSV */
            continue;
        }
        switch (col->type) {
        case TSDB_FLOAT32: fprintf(o->f, "%s\t%.9g\n", tb, ((const float *)col->values)[i]); break;
        case TSDB_FLOAT64: fprintf(o->f, "%s\t%.17g\n", tb, ((const double *)col->values)[i]); break;
        case TSDB_INT64: fprintf(o->f, "%s\t%" PRId64 "\n", tb, ((const int64_t *)col->values)[i]); break;
        case TSDB_BOOL: fprintf(o->f, "%s\t%s\n", tb, ((const uint8_t *)col->values)[i] ? "true" : "false"); break;
        default: {
            const char *name = tsdb_state_name(o->db, o->series, ((const uint32_t *)col->values)[i]);
            fprintf(o->f, "%s\t", tb);
            put_name(o->f, name ? name : "?");
            fputc('\n', o->f);
        }
        }
    }
    o->n += n;
    return 0;
}

/* 1234567 -> "1.23 million", ClickHouse style */
static void readable(double x, const char *unit, char *buf, size_t len)
{
    static const char *big[] = {"", " thousand", " million", " billion", " trillion"};
    int k = 0;
    while (x >= 1000 && k < 4) {
        x /= 1000;
        k++;
    }
    snprintf(buf, len, k ? "%.2f%s%s" : "%.0f%s%s", x, big[k], unit);
}

static void readable_bytes(double b, char *buf, size_t len)
{
    static const char *u[] = {"B", "KB", "MB", "GB", "TB"};
    int k = 0;
    while (b >= 1000 && k < 4) {
        b /= 1000;
        k++;
    }
    snprintf(buf, len, "%.2f %s", b, u[k]);
}

/* What the query did, as ClickHouse's client reports it, plus where the rows came from. */
static void print_stats(const char *what)
{
    tsdb_query_stats st;
    tsdb_last_query_stats(&st);
    double t = st.elapsed_s > 0 ? st.elapsed_s : 1e-9;
    char rows[48], bytes[32], rate[48], brate[32], read[32];
    readable((double)st.rows, " rows", rows, sizeof rows);
    readable_bytes((double)st.bytes, bytes, sizeof bytes);
    readable((double)st.rows / t, " rows/s", rate, sizeof rate);
    readable_bytes((double)st.bytes / t, brate, sizeof brate);
    readable_bytes((double)st.bytes_read, read, sizeof read);
    fprintf(stderr,
            "%" PRIu64 " %s in set. Elapsed: %.*f sec. Processed %s, %s (%s, %s/s)\n"
            "%" PRIu64 " parts, %" PRIu64 " granules (%" PRIu64 " from their statistics); %" PRIu64
            " rows decoded from %s, %" PRIu64 " in memory\n",
            st.result_rows, what, st.elapsed_s < 0.01 ? 6 : 3, st.elapsed_s, rows, bytes, rate, brate, st.parts, st.granules,
            st.granules_from_stats, st.rows_decoded, read, st.memtable_rows);
}

/* "15m", "6h", "1d", "1w", "1mo", "1q", "1y" -> a group; plain numbers and
 * "500ms" give a fixed interval in milliseconds instead. */
static bool parse_group(const char *s, tsdb_group *g, int64_t *interval_ms)
{
    char *end;
    errno = 0;
    long n = strtol(s, &end, 10);
    if (end == s || n <= 0)
        return false;
    static const struct {
        const char *suffix;
        tsdb_unit unit;
    } units[] = {{"s", TSDB_SECOND}, {"m", TSDB_MINUTE}, {"h", TSDB_HOUR},   {"d", TSDB_DAY},
                 {"w", TSDB_WEEK},   {"mo", TSDB_MONTH}, {"q", TSDB_QUARTER}, {"y", TSDB_YEAR}};
    for (size_t i = 0; i < sizeof units / sizeof *units; i++)
        if (strcmp(end, units[i].suffix) == 0) {
            if (errno == ERANGE || n > (long)UINT32_MAX) /* not cut to 32 bits: 4294967297h is not 1h */
                return false;
            g->unit = units[i].unit;
            g->count = (uint32_t)n;
            return true;
        }
    return parse_interval(s, interval_ms) && *interval_ms > 0;
}

static int query_twa(tsdb *db, tsdb_sid id, int64_t start, int64_t end, const tsdb_group *g, int64_t interval,
                     tsdb_twa_opts *tw, const tsdb_query_opts *q)
{
    tw->order = q->order;
    tw->limit = q->limit;
    tsdb_twa_bucket *b;
    size_t nb;
    int rc = g ? tsdb_query_twa_group_local(db, id, start, end, g, tw, &b, &nb)
               : tsdb_query_twa(db, id, start, end, interval, tw, &b, &nb);
    if (rc)
        die("query", rc);
    printf("time\ttwa\tarea\tcovered_ms\tcount\n");
    char tb[40], twa[32];
    for (size_t i = 0; i < nb; i++) {
        format_time(b[i].start, tb, sizeof tb);
        if (b[i].covered_ms)
            snprintf(twa, sizeof twa, "%.17g", b[i].twa);
        else
            snprintf(twa, sizeof twa, "\\N"); /* readings, but none held for any time */
        printf("%s\t%s\t%.17g\t%" PRId64 "\t%" PRIu64 "\n", tb, twa, b[i].area, b[i].covered_ms, b[i].count);
    }
    free(b);
    print_stats("buckets");
    return 0;
}

static int cmd_query(tsdb *db, int argc, char **argv)
{
    /* positional arguments, then --tz, --desc, --limit anywhere after them */
    const char *pos[5] = {0};
    int npos = 0;
    tsdb_query_opts q = {TSDB_ASC, 0};
    const char *zone = NULL;
    tsdb_twa_opts tw = {.method = TSDB_TWA_STEP, .order = TSDB_ASC};
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--linear") == 0)
            tw.method = TSDB_TWA_LINEAR;
        else if (strcmp(argv[i], "--max-gap") == 0 && i + 1 < argc) {
            if (!parse_interval(argv[++i], &tw.max_gap_ms) || tw.max_gap_ms < 0) {
                fprintf(stderr, "tsdb: cannot parse --max-gap \"%s\"\n", argv[i]);
                return 1;
            }
        } else if (strcmp(argv[i], "--desc") == 0)
            q.order = TSDB_DESC;
        else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
            /* digits only: "abc" would be 0 (no limit) and "-1" the largest number */
            const char *l = argv[++i];
            char *e;
            errno = 0;
            q.limit = isdigit((unsigned char)l[0]) ? strtoull(l, &e, 10) : 0;
            if (!isdigit((unsigned char)l[0]) || *e || errno == ERANGE || q.limit == 0) {
                fprintf(stderr, "tsdb: --limit wants a positive number, not \"%s\"\n", l);
                return 2;
            }
        }
        else if (strcmp(argv[i], "--tz") == 0 && i + 1 < argc)
            zone = argv[++i];
        else if (npos < 5)
            pos[npos++] = argv[i];
        else
            usage();
    }
    if (npos < 3)
        usage();
    tsdb_sid id;
    int rc = tsdb_series_lookup(db, pos[0], &id);
    if (rc == TSDB_ERR_NOTFOUND)
        return 0; /* unknown series: empty result */
    if (rc)
        die(pos[0], rc);
    int64_t start, end, interval = 0;
    if (!parse_time(pos[1], &start) || !parse_time(pos[2], &end)) {
        fprintf(stderr, "tsdb: cannot parse time range\n");
        return 1;
    }
    const char *mode = npos > 3 ? pos[3] : "raw";
    tsdb_group g = {0, 0, zone};
    bool grouped = false;
    if (npos > 4) {
        if (!parse_group(pos[4], &g, &interval)) {
            fprintf(stderr, "tsdb: cannot parse group \"%s\"\n", pos[4]);
            return 1;
        }
        grouped = g.unit != 0;
    }
    if (strcmp(mode, "raw") == 0) {
        raw_out o = {stdout, 0, db, id};
        if ((rc = tsdb_query_values(db, id, start, end, &q, print_raw, &o)) != 0)
            die("query", rc);
        print_stats("rows");
        return 0;
    }
    if (strcmp(mode, "lttb") == 0) { /* the points that keep the shape, printed like raw */
        char *e = NULL;
        /* digits only: strtoull would take "-1" as the largest number */
        unsigned long long n = npos > 4 && pos[4][0] >= '0' && pos[4][0] <= '9' ? strtoull(pos[4], &e, 10) : 0;
        if (!e || *e || n < 3) {
            fprintf(stderr, "tsdb: lttb needs a number of points, at least 3\n");
            return 1;
        }
        raw_out o = {stdout, 0, db, id};
        if ((rc = tsdb_query_lttb_ex(db, id, start, end, (size_t)n, &q, print_raw, &o)) != 0)
            die("query", rc);
        print_stats("rows");
        return 0;
    }
    if (strcmp(mode, "twa") == 0)
        return query_twa(db, id, start, end, grouped ? &g : NULL, interval, &tw, &q);
    /* the spread costs a second pass over the rows: asked for only by the modes that print it */
    static const char *const spread_modes[] = {"all", "range", "stddev", "stddev_pop", "stddev_samp",
                                               "var", "var_pop", "var_samp"};
    bool spread = false;
    for (size_t i = 0; i < sizeof spread_modes / sizeof *spread_modes; i++)
        spread = spread || strcmp(mode, spread_modes[i]) == 0;
    if (strcmp(mode, "stddev") == 0)
        mode = "stddev_pop";
    if (strcmp(mode, "var") == 0)
        mode = "var_pop";
    tsdb_spread_bucket *b = NULL;
    tsdb_bucket *pb = NULL;
    size_t nb = 0;
    if (spread) {
        if (grouped)
            rc = tsdb_query_spread_group_local(db, id, start, end, &g, &q, &b, &nb);
        else
            rc = tsdb_query_spread(db, id, start, end, interval, &q, &b, &nb);
    } else {
        if (grouped)
            rc = tsdb_query_group_local(db, id, start, end, &g, &q, &pb, &nb);
        else if (interval)
            rc = tsdb_query_agg_ex(db, id, start, end, interval, &q, &pb, &nb);
        else
            rc = tsdb_query_group_ex(db, id, start, end, NULL, &q, &pb, &nb);
        /* one loop below prints both */
        if (rc == 0 && (b = calloc(nb ? nb : 1, sizeof *b)) == NULL)
            rc = TSDB_ERR_NOMEM;
        for (size_t i = 0; rc == 0 && i < nb; i++)
            b[i].b = pb[i];
        free(pb);
    }
    if (rc)
        die("query", rc);
    tsdb_type type = TSDB_FLOAT32;
    tsdb_series_type(db, id, &type);
    bool all = strcmp(mode, "all") == 0, ints = type == TSDB_INT64 || type == TSDB_BOOL;
    if (all)
        printf("time\tcount\tsum\tmin\tmax\tavg\trange\tstddev_pop\tstddev_samp\n");
    char tb[40];
    for (size_t i = 0; i < nb; i++) {
        const tsdb_bucket *x = &b[i].b;
        const tsdb_spread_bucket *y = &b[i];
        format_time(x->start, tb, sizeof tb);
        char sum[32], mn[32], mx[32], rg[32], sp[32] = "";
        if (ints) { /* exact; the range of int64 values fits only in 64 unsigned bits */
            snprintf(sum, sizeof sum, "%" PRId64, x->isum);
            snprintf(mn, sizeof mn, "%" PRId64, x->imin);
            snprintf(mx, sizeof mx, "%" PRId64, x->imax);
            if (x->count)
                snprintf(rg, sizeof rg, "%" PRIu64, (uint64_t)x->imax - (uint64_t)x->imin);
            else
                snprintf(rg, sizeof rg, "%.17g", y->range);
        } else {
            snprintf(sum, sizeof sum, "%.17g", x->sum);
            snprintf(mn, sizeof mn, "%.17g", x->min);
            snprintf(mx, sizeof mx, "%.17g", x->max);
            snprintf(rg, sizeof rg, "%.17g", y->range);
        }
        if (strcmp(mode, "stddev_pop") == 0)
            snprintf(sp, sizeof sp, "%.9g", y->stddev_pop);
        else if (strcmp(mode, "stddev_samp") == 0)
            snprintf(sp, sizeof sp, "%.9g", y->stddev_samp);
        else if (strcmp(mode, "var_pop") == 0)
            snprintf(sp, sizeof sp, "%.9g", y->var_pop);
        else if (strcmp(mode, "var_samp") == 0)
            snprintf(sp, sizeof sp, "%.9g", y->var_samp);
        if (!x->count && !all) /* only nulls */
            snprintf(sum, sizeof sum, "\\N"), snprintf(mn, sizeof mn, "\\N"), snprintf(mx, sizeof mx, "\\N"),
                snprintf(rg, sizeof rg, "\\N"), snprintf(sp, sizeof sp, "\\N");
        if (all)
            printf("%s\t%" PRIu64 "\t%s\t%s\t%s\t%.9g\t%s\t%.9g\t%.9g\n", tb, x->count, sum, mn, mx,
                   tsdb_bucket_avg(x), rg, y->stddev_pop, y->stddev_samp);
        else if (strcmp(mode, "count") == 0)
            printf("%s\t%" PRIu64 "\n", tb, x->count);
        else if (strcmp(mode, "sum") == 0)
            printf("%s\t%s\n", tb, sum);
        else if (strcmp(mode, "min") == 0)
            printf("%s\t%s\n", tb, mn);
        else if (strcmp(mode, "max") == 0)
            printf("%s\t%s\n", tb, mx);
        else if (strcmp(mode, "avg") == 0)
            printf("%s\t%.9g\n", tb, tsdb_bucket_avg(x));
        else if (strcmp(mode, "range") == 0)
            printf("%s\t%s\n", tb, rg);
        else if (spread)
            printf("%s\t%s\n", tb, sp);
        else
            usage();
    }
    print_stats("buckets");
    free(b);
    return 0;
}

/* -------------------------------------------------------------- inspection */

static void print_part(void *ctx, const tsdb_part_info *p)
{
    (void)ctx;
    char a[40], b[40], s[40];
    format_time(p->min_ts, a, sizeof a);
    format_time(p->max_ts, b, sizeof b);
    format_time(p->start_ms, s, sizeof s);
    /* the class: partitions of 1, 7, 28 or 364 base partitions (tsdb_options.partition_classes) */
    printf("%-40s class %d from %s level %-3u %12" PRIu64 " rows %12" PRIu64 " bytes %8" PRIu64
           " granules  %s .. %s\n",
           p->name, p->cls, s, p->level, p->rows, p->bytes, p->granules, a, b);
}

static void human(double bytes, char *buf, size_t len)
{
    const char *u[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    int i = 0;
    while (bytes >= 1024 && i < 4) {
        bytes /= 1024;
        i++;
    }
    snprintf(buf, len, "%.2f %s", bytes, u[i]);
}

/* One line of `tsdb DIR series`; the ids come from tsdb_series_foreach, which does not assume they are dense. */
static int print_series(void *ctx, tsdb_sid id)
{
    tsdb *db = ctx;
    tsdb_type t = TSDB_FLOAT32;
    tsdb_series_type(db, id, &t);
    put_name(stdout, tsdb_series_name(db, id));
    printf("\t%s\t", type_names[t]);
    put_name(stdout, tsdb_series_path(db, id));
    int c = 0;
    tsdb_series_class(db, id, &c);
    printf("\tclass %d\n", c);
    return 0;
}

/* Run as root on a directory that another user owns (`sudo tsdb /var/lib/tsdb ...`), the CLI becomes that user and
 * group for good before it opens anything there. Whoever owns the directory (the service account, or whoever took
 * it over) can put symbolic links in it, and root would follow them: USERS, MANIFEST, the WAL, VOLUMES and the rest
 * would create, overwrite, chown or chmod files anywhere (secrel cli #1). As the owner, the CLI can do only what the
 * owner can, and what it makes belongs to the owner, as when tsdbd makes it. A directory that does not exist yet
 * counts as its nearest existing parent's (a new database, an import built beside it). A directory of root's: root
 * stays root (nothing to drop to). Exits when the change does not take, or root could come back. */
static void drop_root(const char *dir)
{
    if (geteuid() != 0)
        return;
    char p[PATH_MAX];
    if (strlen(dir) >= sizeof p)
        die(dir, TSDB_ERR_INVALID);
    snprintf(p, sizeof p, "%s", dir);
    struct stat st;
    while (stat(p, &st) != 0) {
        if (errno != ENOENT || strcmp(p, ".") == 0 || strcmp(p, "/") == 0) {
            fprintf(stderr, "tsdb: %s: %s\n", p, strerror(errno));
            exit(1);
        }
        char *s = strrchr(p, '/');
        if (!s)
            snprintf(p, sizeof p, ".");
        else if (s == p)
            p[1] = '\0';
        else
            *s = '\0';
    }
    if (st.st_uid == 0)
        return;
    uid_t uid = st.st_uid;
    gid_t gid = st.st_gid;
    const struct passwd *pw = getpwuid(uid);
    char name[64];
    snprintf(name, sizeof name, "%s", pw && pw->pw_name ? pw->pw_name : "no name");
    if (setgroups(1, &gid) != 0 || setgid(gid) != 0 || setuid(uid) != 0 || getuid() != uid || geteuid() != uid ||
        getgid() != gid || getegid() != gid || setuid(0) == 0 || seteuid(0) == 0) {
        fprintf(stderr, "tsdb: %s belongs to uid %u (%s): running as root, this must become that user and cannot "
                        "(%s); refused\n", p, (unsigned)uid, name, strerror(errno));
        exit(1);
    }
    fprintf(stderr, "tsdb: running as uid %u (%s), gid %u, the owner of %s, not as root\n", (unsigned)uid, name,
            (unsigned)gid, p);
}

/* `tsdb DIR stats`: the rows and parts as stored, and where the bytes go. The sweeper counts what the granule
 * indexes say (docs/design-stats.md): logical bytes are rows x (8 + the series' value width), exact; the 12 bytes a
 * row of a Float32 series takes is no longer assumed for every type. */
static int cmd_stats(tsdb *db)
{
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    int rc = tsdb_stats_settle(db, 600000);
    if (rc)
        die("stats", rc);
    tsdb_volume_stats tot;
    memset(&tot, 0, sizeof tot);
    for (uint32_t i = 0; i < tsdb_volume_count(db); i++) {
        tsdb_volume_stats v;
        if (tsdb_get_volume_stats(db, i, &v) != 0)
            continue;
        tot.logical_bytes += v.logical_bytes;
        tot.ts_bytes += v.ts_bytes;
        tot.value_bytes += v.value_bytes;
        tot.index_bytes += v.index_bytes;
        tot.other_bytes += v.other_bytes;
    }
    char sz[32], raw[32], ts[32], va[32], ix[32], ot[32];
    human((double)st.bytes, sz, sizeof sz);
    human((double)tot.logical_bytes, raw, sizeof raw);
    human((double)tot.ts_bytes, ts, sizeof ts);
    human((double)tot.value_bytes, va, sizeof va);
    human((double)tot.index_bytes, ix, sizeof ix);
    human((double)tot.other_bytes, ot, sizeof ot);
    printf("series      %" PRIu64 "\nrows        %" PRIu64 "\nparts       %" PRIu64 "\npartitions  %" PRIu64
           "\non disk     %s (%.3f bytes/row)\nuncompressed %s (8 bytes of time + the value, by type)\n"
           "ratio       %.1fx\n",
           tsdb_series_count(db), st.rows, st.parts, st.partitions, sz,
           st.rows ? (double)st.bytes / (double)st.rows : 0.0, raw,
           st.bytes ? (double)tot.logical_bytes / (double)st.bytes : 0.0);
    printf("  timestamps %s (%.2f bits/row)\n  values     %s (%.2f bits/row)\n  index      %s\n  other      %s\n"
           "simd        %s\n",
           ts, st.rows ? 8.0 * (double)tot.ts_bytes / (double)st.rows : 0.0, va,
           st.rows ? 8.0 * (double)tot.value_bytes / (double)st.rows : 0.0, ix, ot, tsdb_simd_level());
    return 0;
}

/* One line of `stats series`, from a local or a remote record. */
static void print_series_stat(const char *name, const tsdb_series_stat *s)
{
    char a[40] = "-", b[40] = "-", lw[40] = "-";
    if (s->status != 0)
        die(name, s->status);
    if (!(s->flags & TSDB_SSTAT_NO_RANGE)) {
        format_time(s->first_ts, a, sizeof a);
        format_time(s->last_ts, b, sizeof b);
    }
    if (s->last_write_ms)
        format_time(s->last_write_ms, lw, sizeof lw);
    put_name(stdout, name);
    printf("\t%s\tclass %d\t%" PRIu64 " rows (%" PRIu64 " in memtables, %" PRIu64 " nulls)\t%" PRIu64
           " bytes (%" PRIu64 " ts, %" PRIu64 " values, %" PRIu64 " index, %" PRIu64 " tombstones)\t%" PRIu64
           " parts, %" PRIu64 " partitions, %" PRIu64 " granules (%" PRIu64 " full)\t%s .. %s\twritten %s\n",
           type_names[s->type], s->cls, s->rows, s->memtable_rows, s->nulls,
           s->ts_bytes + s->value_bytes + s->index_bytes + s->tomb_bytes, s->ts_bytes, s->value_bytes,
           s->index_bytes, s->tomb_bytes, s->parts, s->partitions, s->granules, s->full_granules, a, b, lw);
}

/* `tsdb DIR stats series NAME...`: one line per series, from the granule index (rows and bytes as stored, which
 * include rows a newer write or a tombstone hides until a merge removes them). */
static int cmd_stats_series(tsdb *db, int n, char **names)
{
    if (n < 1)
        usage();
    tsdb_sid *ids = malloc((size_t)n * sizeof *ids);
    tsdb_series_stat *out = malloc((size_t)n * sizeof *out);
    if (!ids || !out)
        die("stats series", TSDB_ERR_NOMEM);
    for (int i = 0; i < n; i++) {
        int rc = tsdb_series_lookup(db, names[i], &ids[i]);
        if (rc)
            die(names[i], rc);
    }
    size_t done = 0;
    while (done < (size_t)n) {
        size_t d = 0, chunk = (size_t)n - done;
        int rc = tsdb_series_stats(db, ids + done, chunk > 65536 ? 65536 : chunk, 0, out + done, &d);
        if (rc)
            die("stats series", rc);
        done += d;
    }
    for (int i = 0; i < n; i++)
        print_series_stat(names[i], &out[i]);
    free(ids);
    free(out);
    return 0;
}

/* "20", "64G", "512M": a byte count with an optional binary suffix */
static bool parse_bytes(const char *s, uint64_t *out)
{
    char *end;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno || end == s || !v)
        return false;
    int shift = 0;
    if (*end == 'K' || *end == 'k')
        shift = 10, end++;
    else if (*end == 'M' || *end == 'm')
        shift = 20, end++;
    else if (*end == 'G' || *end == 'g')
        shift = 30, end++;
    else if (*end == 'T' || *end == 't')
        shift = 40, end++;
    if (*end || v > (UINT64_MAX >> shift))
        return false;
    *out = (uint64_t)v << shift;
    return true;
}

/* tsdb DIR feed enable|disable|info|new-id (src/feed.h): the writer open the verb needs fails with
 * TSDB_ERR_LOCKED beside tsdbd, which is the "refused beside a running writer" of design 2.13 */
static int cmd_feed(tsdb *db, int argc, char **argv)
{
    if (argc < 1)
        usage();
    const char *verb = argv[0];
    int rc;
    if (strcmp(verb, "enable") == 0) {
        tsdb_feed_options fo;
        memset(&fo, 0, sizeof fo);
        for (int i = 1; i < argc; i++) {
            int64_t ms;
            uint64_t n;
            if (strcmp(argv[i], "--retention") == 0 && i + 1 < argc) {
                if (!parse_interval(argv[++i], &ms) || ms < 1000 || ms / 1000 > UINT32_MAX)
                    usage();
                fo.retention_s = (uint32_t)(ms / 1000);
            } else if (strcmp(argv[i], "--max-bytes") == 0 && i + 1 < argc) {
                if (!parse_bytes(argv[++i], &fo.max_bytes))
                    usage();
            } else if (strcmp(argv[i], "--min-free-pct") == 0 && i + 1 < argc) {
                if (!parse_bytes(argv[++i], &n) || n > 90)
                    usage();
                fo.min_free_pct = (uint32_t)n;
            } else {
                usage();
            }
        }
        if ((rc = tsdb_feed_enable(db, &fo)) != 0)
            die("feed enable", rc);
    } else if (strcmp(verb, "disable") == 0 && argc == 1) {
        if ((rc = tsdb_feed_disable(db)) != 0)
            die("feed disable", rc);
    } else if (strcmp(verb, "new-id") == 0 && argc == 1) {
        if ((rc = tsdb_feed_new_id(db)) != 0)
            die("feed new-id", rc);
    } else if (strcmp(verb, "info") != 0 || argc != 1) {
        usage();
    }
    for (uint32_t v = 0; v < tsdb_volume_count(db); v++) {
        tsdb_feed_info fi;
        if ((rc = tsdb_feed_info_get(db, v, &fi)) != 0)
            die("feed info", rc);
        if (!fi.on) {
            printf("%u\t%s\toff\n", v, tsdb_volume_path(db, v));
            continue;
        }
        printf("%u\t%s\ton\tfeed_id %016" PRIx64 "\tretention %us\tmax_bytes %" PRIu64 "\tmin_free %u%%\t"
               "floor %" PRIu64 "\tkept %" PRIu64 " segments, %" PRIu64 " bytes\n",
               v, tsdb_volume_path(db, v), fi.feed_id, fi.retention_s, fi.max_bytes, fi.min_free_pct,
               fi.floor_event, fi.retained_segments, fi.retained_bytes);
    }
    return 0;
}

/* The tenant list of `stats tenants`: the PREFIX arguments and the lines of --tenants FILE. The strings live until
 * the process ends (tsdb_options.tenants copies them at the open, the printing reads the list back from the handle). */
static const char **tenant_list;
static size_t tenant_n;
static bool tenant_volumes;

static void tenant_add(const char *p)
{
    const char **n = realloc(tenant_list, (tenant_n + 1) * sizeof *n);
    char *c = strdup(p);
    if (!n || !c)
        die("stats tenants", TSDB_ERR_NOMEM);
    tenant_list = n;
    tenant_list[tenant_n++] = c;
}

static void tenant_file(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "tsdb: %s: %s\n", path, strerror(errno));
        exit(1);
    }
    char line[4096];
    while (fgets(line, sizeof line, f)) {
        size_t n = strlen(line);
        if (n && line[n - 1] != '\n' && !feof(f)) {
            fprintf(stderr, "tsdb: %s: a line longer than %zu bytes\n", path, sizeof line - 1);
            exit(1);
        }
        while (n && (line[n - 1] == '\n' || line[n - 1] == '\r'))
            line[--n] = 0;
        if (n && line[0] != '#')
            tenant_add(line);
    }
    fclose(f);
}

/* argv[4..): before the open, which needs the list */
static void tenant_args(int argc, char **argv)
{
    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--volumes") == 0)
            tenant_volumes = true;
        else if (strcmp(argv[i], "--tenants") == 0 && i + 1 < argc)
            tenant_file(argv[++i]);
        else if (argv[i][0] == '-' && argv[i][1] == '-')
            usage();
        else if (argv[i][0] == 0)
            usage();
        else
            tenant_add(argv[i]);
    }
}

static void print_tenant(const char *label, const tsdb_tenant_stats *t, const char *where)
{
    char lg[32], ph[32], rs[32];
    human((double)t->logical_bytes, lg, sizeof lg);
    human((double)(t->ts_bytes + t->value_bytes + t->index_bytes), ph, sizeof ph);
    human((double)t->resident_bytes, rs, sizeof rs);
    put_name(stdout, label);
    printf("\t%s\t%" PRIu64 " series\t%" PRIu64 " rows (%" PRIu64 " nulls)\t%s on disk (%s uncompressed)\t%" PRIu64
           " granules\t%" PRIu64 " active series (last hour)\t%s in the page cache (sampled)\n",
           where, t->series, t->rows, t->nulls, ph, lg, t->granules, t->active_series_prev, rs);
}

/* `tsdb DIR stats tenants`: one line per prefix of the list, then "(none)". The CLI runs the sweep itself first (the
 * numbers a running tsdbd keeps are in its process), so it takes as long as the index walk of every part. */
static int cmd_stats_tenants(tsdb *db)
{
    int rc = tsdb_stats_settle(db, 600000);
    if (rc)
        die("stats tenants", rc);
    uint32_t nv = tsdb_volume_count(db);
    for (size_t i = 0;; i++) {
        char pf[1024];
        int n = tsdb_tenant_prefix(db, i, pf, sizeof pf);
        if (n == TSDB_ERR_NOTFOUND)
            break;
        if (n < 0)
            die("stats tenants", n);
        for (uint32_t v = tenant_volumes ? 0 : TSDB_TENANT_VOLUME_SUM;; v++) {
            tsdb_tenant_stats t;
            char where[32] = "all";
            uint32_t vol = tenant_volumes ? v : TSDB_TENANT_VOLUME_SUM;
            if (tenant_volumes)
                snprintf(where, sizeof where, "volume %u", v);
            if ((rc = tsdb_tenant_stats_get(db, pf, vol, &t)) != 0)
                die("stats tenants", rc);
            print_tenant(pf, &t, where);
            if (t.flags & TSDB_TENANT_PARTIAL)
                fprintf(stderr, "tsdb: the numbers of %s are stale\n", pf);
            if (!tenant_volumes || v + 1 >= nv)
                break;
        }
    }
    for (uint32_t v = tenant_volumes ? 0 : TSDB_TENANT_VOLUME_SUM;; v++) {
        tsdb_tenant_stats t;
        char where[32] = "all";
        uint32_t vol = tenant_volumes ? v : TSDB_TENANT_VOLUME_SUM;
        if (tenant_volumes)
            snprintf(where, sizeof where, "volume %u", v);
        if ((rc = tsdb_tenant_stats_get(db, NULL, vol, &t)) != 0)
            die("stats tenants", rc);
        print_tenant("(none)", &t, where);
        if (!tenant_volumes || v + 1 >= nv)
            break;
    }
    return 0;
}

/* `tsdb DIR stats hot [reads|writes|bytes] [-k N]` */
static int cmd_stats_hot(tsdb *db, int argc, char **argv)
{
    int kind = TSDB_HOT_BYTES_PER_ROW;
    size_t k = 20;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "reads") == 0)
            kind = TSDB_HOT_READS;
        else if (strcmp(argv[i], "writes") == 0)
            kind = TSDB_HOT_WRITES;
        else if (strcmp(argv[i], "bytes") == 0)
            kind = TSDB_HOT_BYTES_PER_ROW;
        else if (strcmp(argv[i], "-k") == 0 && i + 1 < argc) {
            char *end;
            long v = strtol(argv[++i], &end, 10);
            if (*end || v < 1 || v > 256)
                usage();
            k = (size_t)v;
        } else {
            usage();
        }
    }
    int rc = tsdb_stats_settle(db, 600000);
    if (rc)
        die("stats hot", rc);
    tsdb_hot hot[256];
    size_t n = 0;
    uint64_t thr = 0;
    if ((rc = tsdb_hot_series(db, kind, hot, k, &n, &thr)) != 0)
        die("stats hot", rc);
    for (size_t i = 0; i < n; i++) {
        const char *name = tsdb_series_name(db, hot[i].id);
        put_name(stdout, name ? name : "?");
        if (kind == TSDB_HOT_BYTES_PER_ROW)
            printf("\t%.3f bytes/row\n", (double)hot[i].estimate / 1000.0);
        else
            printf("\t%" PRIu64 " (+- %" PRIu64 ")\n", hot[i].estimate, hot[i].error);
    }
    return 0;
}

/* `tsdb stats series|tenants|hot ... --server HOST:PORT [--login NAME [--login-password-file F]]`: the same numbers
 * from a running tsdbd (protocol 15), as the user who logs in, so a tenant admin needs no shell on the server. What
 * the user may not see is refused or left out by the server: the "(none)" tenant, the threshold of a hot list and
 * the hot lists of tenants the user does not administer. A password is never an argument. */
static int stats_online(const char *server, const char *login, const char *pwfile, const char *what, int n,
                        char **a)
{
    bool volumes = false, caller = false, tenants = strcmp(what, "tenants") == 0, hotcmd = strcmp(what, "hot") == 0;
    int kind = TSDB_HOT_BYTES_PER_ROW, hot = 0;
    long k = 20;
    const char **names = calloc((size_t)(n > 0 ? n : 1), sizeof *names);
    int nn = 0;
    if (!names)
        die("stats", TSDB_ERR_NOMEM);
    for (int i = 0; i < n; i++) {
        char *end;
        if (tenants && strcmp(a[i], "--volumes") == 0) {
            volumes = true;
        } else if (tenants && strcmp(a[i], "--caller") == 0) {
            caller = true;
        } else if ((tenants && strcmp(a[i], "--hot") == 0) || (hotcmd && strcmp(a[i], "-k") == 0)) {
            if (i + 1 >= n)
                usage();
            k = strtol(a[++i], &end, 10);
            if (*end || k < 1 || k > 256)
                usage();
            hot = 1;
        } else if (hotcmd && strcmp(a[i], "reads") == 0) {
            kind = TSDB_HOT_READS;
        } else if (hotcmd && strcmp(a[i], "writes") == 0) {
            kind = TSDB_HOT_WRITES;
        } else if (hotcmd && strcmp(a[i], "bytes") == 0) {
            kind = TSDB_HOT_BYTES_PER_ROW;
        } else if ((a[i][0] == '-' && a[i][1] == '-') || hotcmd || !a[i][0]) {
            usage();
        } else {
            names[nn++] = a[i];
        }
    }
    if (strcmp(what, "series") == 0 && nn < 1)
        usage();
    tsdbc *c;
    int rc = cli_connect(server, login, pwfile, &c);
    if (rc)
        exit(rc);
    if (strcmp(what, "series") == 0) {
        tsdb_sid *ids = malloc((size_t)nn * sizeof *ids);
        tsdb_series_stat *out = malloc((size_t)nn * sizeof *out);
        if (!ids || !out)
            die("stats series", TSDB_ERR_NOMEM);
        for (int i = 0; i < nn; i++)
            if ((rc = tsdbc_series(c, names[i], 0, &ids[i])) != 0)
                die(names[i], rc);
        if ((rc = tsdbc_series_stats(c, ids, (size_t)nn, 0, out)) != 0)
            die("stats series", rc);
        for (int i = 0; i < nn; i++)
            print_series_stat(names[i], &out[i]);
        free(ids);
        free(out);
        tsdbc_close(c);
        free(names);
        return 0;
    }
    tsdbc_tenant_result r;
    int flags = (volumes ? TSDB_TSTATS_VOLUMES : 0) | (caller ? TSDB_TSTATS_CALLER : 0) |
                ((hot || hotcmd) ? TSDB_TSTATS_HOT : 0);
    if ((rc = tsdbc_tenant_stats(c, names, (size_t)nn, flags, (unsigned)k, &r)) != 0)
        die(what, rc);
    static const char *const kn[] = {"?", "reads", "writes", "bytes"};
    for (size_t i = 0; i < r.nrows; i++) {
        const tsdbc_tenant_row *t = &r.rows[i];
        char where[32] = "all";
        const char *label = t->prefix_len ? t->prefix : "(none)";
        if (t->volume != TSDB_TENANT_VOLUME_SUM)
            snprintf(where, sizeof where, "volume %u", t->volume);
        if (t->status != 0)
            die(label, t->status);
        if (tenants) {
            print_tenant(label, &t->st, where);
            if (t->st.flags & TSDB_TENANT_PARTIAL)
                fprintf(stderr, "tsdb: the numbers of %s are stale\n", label);
        }
        if (tenants && t->has_caller) {
            const tsdbc_tenant_caller *q = &t->caller;
            put_name(stdout, label);
            printf("\tcaller\t%" PRIu64 " queries, %" PRIu64 " inserts, %" PRIu64 " deletes, %" PRIu64 " admin, %" PRIu64
                   " other\t%" PRIu64 " rows decoded, %" PRIu64 " from statistics, %" PRIu64 " from memtables\t%" PRIu64
                   " rows inserted\t%" PRIu64 " denied requests, %" PRIu64 " denied series, %" PRIu64
                   " failed logins\n",
                   q->requests_query, q->requests_insert, q->requests_delete, q->requests_admin, q->requests_other,
                   q->rows_decoded, q->rows_from_stats, q->rows_from_memtable, q->inserted_rows, q->denied_requests,
                   q->denied_series, q->failed_logins);
        }
        for (size_t j = 0; j < t->nhot; j++) {
            const tsdbc_hot *h = &t->hot[j];
            if (hotcmd && (h->kind != (uint8_t)kind || h->id == 0))
                continue;
            if (hotcmd) {
                char nm[4096];
                if (tsdbc_series_name(c, h->id, nm, sizeof nm) != 0)
                    snprintf(nm, sizeof nm, "#%" PRIu64, h->id);
                put_name(stdout, nm);
            } else {
                put_name(stdout, label);
                printf("\thot %s\t", h->kind < 4 ? kn[h->kind] : "?");
                if (h->id == 0)
                    printf("threshold");
                else
                    printf("series %" PRIu64, h->id);
            }
            if (h->kind == TSDB_HOT_BYTES_PER_ROW)
                printf("\t%.3f bytes/row\n", (double)h->estimate / 1000.0);
            else
                printf("\t%" PRIu64 " (+- %" PRIu64 ")\n", h->estimate, h->error);
        }
    }
    tsdbc_tenant_result_free(&r);
    tsdbc_close(c);
    free(names);
    return 0;
}

/* `tsdb DIR latest NAME... [--with-nulls] [--end TIME]`: tsdb_latest for the named series, one TSV line each. */
static int cmd_latest(tsdb *db, int argc, char **argv)
{
    tsdb_latest_opts lo = {.end = TSDB_TS_MAX};
    const char **names = malloc((size_t)(argc > 0 ? argc : 1) * sizeof *names);
    int n = 0;
    if (!names)
        die("latest", TSDB_ERR_NOMEM);
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--with-nulls") == 0) {
            lo.with_nulls = 1;
        } else if (strcmp(argv[i], "--end") == 0 && i + 1 < argc) {
            if (!parse_time(argv[++i], &lo.end)) {
                fprintf(stderr, "tsdb: cannot parse --end \"%s\"\n", argv[i]);
                free(names);
                return 2;
            }
        } else {
            names[n++] = argv[i];
        }
    }
    if (n < 1 || n > (1 << 20))
        usage();
    tsdb_sid *ids = malloc((size_t)n * sizeof *ids);
    tsdb_latest_result *out = malloc((size_t)n * sizeof *out);
    if (!ids || !out)
        die("latest", TSDB_ERR_NOMEM);
    for (int i = 0; i < n; i++) {
        int rc = tsdb_series_lookup(db, names[i], &ids[i]);
        if (rc)
            die(names[i], rc);
    }
    int rc = tsdb_latest(db, ids, (size_t)n, &lo, out);
    if (rc)
        die("latest", rc);
    for (int i = 0; i < n; i++) {
        const tsdb_latest_result *r = &out[i];
        char tb[40];
        if (r->status != 0)
            die(names[i], r->status);
        put_name(stdout, names[i]);
        if (r->kind == TSDB_LATEST_NONE) {
            fputs("\t\\N\t\\N\n", stdout);
            continue;
        }
        format_time(r->ts, tb, sizeof tb);
        printf("\t%s\t", tb);
        if (r->kind == TSDB_LATEST_NULL) {
            fputs("\\N\n", stdout);
            continue;
        }
        switch (r->type) {
        case TSDB_FLOAT32: printf("%.9g\n", (double)r->v.f32); break;
        case TSDB_FLOAT64: printf("%.17g\n", r->v.f64); break;
        case TSDB_INT64: printf("%" PRId64 "\n", r->v.i64); break;
        case TSDB_BOOL: printf("%s\n", r->v.b ? "true" : "false"); break;
        default: {
            const char *sn = tsdb_state_name(db, ids[i], r->v.state);
            put_name(stdout, sn ? sn : "?");
            putchar('\n');
        }
        }
    }
    free(names);
    free(ids);
    free(out);
    return 0;
}

/* Configuration file (docs/config.md): the settings of the commands that talk to a server, so that a login and a
 * cluster secret need not be typed each time. Only `tsdb user ... --server`, `tsdb stats ... --server` and `tsdb cluster` read it; the commands
 * on a directory take nothing from it. `tsdb --config FILE` names it, `--no-config` reads none, otherwise
 * $XDG_CONFIG_HOME/tsdb/tsdb.properties, ~/.config/tsdb/tsdb.properties or /etc/tsdb/tsdb.properties, the first that
 * exists. The command line wins: an option that is on it is not taken from the file. */
static const struct cfg_key cli_keys[] = {
    {"client.server", "server", CFG_STR, 0, NULL, "", "HOST:PORT of a running tsdbd: `tsdb user VERB ... ` and `tsdb stats series`, `tenants` and `hot` go "
                                                      "through it (even without --server) and log in as client.login."},
    {"client.login", "login", CFG_STR, 0, NULL, "", "The user to log in as."},
    {"client.login.password.file", "login-password-file", CFG_STR, 0, NULL, "",
     "A file with that user's password (mode 600). Passwords are never written in the configuration file itself."},
    {"cluster.secret.file", "secret-file", CFG_STR, 0, NULL, "", "The cluster's shared secret file, for `tsdb cluster`."},
    {NULL, "config", CFG_STR, 0, NULL, "", "Names the file itself (the first argument: `tsdb --config FILE ...`)."},
    {NULL, "no-config", CFG_STR, 0, NULL, "", "Reads no file."},
    {NULL, "check-config", CFG_STR, 0, NULL, "",
     "Checks the file and prints the effective settings (`tsdb --check-config`)."},
    {NULL, "config-keys", CFG_STR, 0, NULL, "", "Prints the table of keys (Markdown)."},
};
#define CLI_NKEYS (sizeof cli_keys / sizeof cli_keys[0])

/* 1: loaded into *c; 0: no file (none named, none in the usual places); exits with status 2 when one is wrong. */
/* 1: the file is there, 0: it is not (ENOENT, ENOTDIR); any other error is a file that exists and cannot be looked
 * at: not "no file", which would run on defaults without a word. */
static int cfg_exists(const char *path)
{
    struct stat st;
    if (stat(path, &st) == 0)
        return 1;
    if (errno != ENOENT && errno != ENOTDIR) {
        fprintf(stderr, "tsdb: %s: %s\n", path, strerror(errno));
        exit(2);
    }
    return 0;
}

static int cli_cfg_load(struct cfg *c, const char *path, int none)
{
    char buf[4096], err[1024];
    if (none)
        return 0;
    if (!path) {
        const char *x = getenv("XDG_CONFIG_HOME"), *h = getenv("HOME");
        if (x && *x == '/' && snprintf(buf, sizeof buf, "%s/tsdb/tsdb.properties", x) < (int)sizeof buf &&
            cfg_exists(buf))
            path = buf;
        else if (h && *h == '/' && snprintf(buf, sizeof buf, "%s/.config/tsdb/tsdb.properties", h) < (int)sizeof buf &&
                 cfg_exists(buf))
            path = buf;
        else if (cfg_exists("/etc/tsdb/tsdb.properties"))
            path = "/etc/tsdb/tsdb.properties";
        else
            return 0;
    }
    if (cfg_load_file(path, cli_keys, CLI_NKEYS, c, err, sizeof err) != 0) {
        fprintf(stderr, "tsdb: %s\n", err);
        exit(2);
    }
    return 1;
}

static int is_user_verb(const char *v)
{
    static const char *const verbs[] = {"add", "passwd", "drop", "alter", "grant", "revoke", "list", "compact", "whoami"};
    for (size_t i = 0; i < sizeof verbs / sizeof verbs[0]; i++)
        if (strcmp(v, verbs[i]) == 0)
            return 1;
    return 0;
}

/* argv with the options of the file that match prefix added at the end; *argc is the new count. */
static char **cli_with_cfg(const struct cfg *c, int *argc, char **argv, const char *prefix)
{
    size_t n;
    char **a = cfg_args(c, *argc, argv, prefix, &n);
    char **nv = a ? malloc((size_t)(*argc + 1 + (int)n) * sizeof *nv) : NULL;
    if (!nv) {
        fprintf(stderr, "tsdb: out of memory\n");
        exit(1);
    }
    memcpy(nv, argv, (size_t)*argc * sizeof *nv);
    memcpy(nv + *argc, a, n * sizeof *nv);
    nv[*argc + (int)n] = NULL;
    *argc += (int)n;
    free(a); /* the strings are the cfg's, which lives until the process ends */
    /* nv is not freed: the new argv is read until the command ends; the static keeps it reachable for LeakSanitizer */
    static char **volatile keep_nv;
    keep_nv = nv;
    (void)keep_nv;
    return nv;
}

int main(int argc, char **argv)
{
    const char *cfg_path = NULL;
    int no_config = 0;
    for (; argc > 1;) { /* leading --config FILE and --no-config */
        if (strcmp(argv[1], "--config") == 0 && argc > 2) {
            cfg_path = argv[2];
            argv[2] = argv[0];
            argv += 2, argc -= 2;
        } else if (strncmp(argv[1], "--config=", 9) == 0) {
            cfg_path = argv[1] + 9;
            argv[1] = argv[0];
            argv++, argc--;
        } else if (strcmp(argv[1], "--no-config") == 0) {
            no_config = 1;
            argv[1] = argv[0];
            argv++, argc--;
        } else {
            break;
        }
    }
    static struct cfg cfg; /* static: the argv built from it (cli_with_cfg) points into it until the process ends */
    int have_cfg = 0, cfg_tried = 0;
    if (cfg_path && !no_config) { /* a file that is named is checked whatever the command does with it */
        have_cfg = cli_cfg_load(&cfg, cfg_path, 0);
        cfg_tried = 1;
    }
    if (argc == 2 && strcmp(argv[1], "--config-keys") == 0) {
        cfg_print_markdown(stdout, cli_keys, CLI_NKEYS);
        return 0;
    }
    if (argc == 2 && strcmp(argv[1], "--check-config") == 0) {
        if (!cfg_tried)
            have_cfg = cli_cfg_load(&cfg, NULL, no_config);
        cfg_print_effective(stdout, have_cfg ? &cfg : NULL, cli_keys, CLI_NKEYS, 1, argv);
        return 0;
    }
    if (argc == 2 && (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)) {
        printf("tsdb %s (build %s)\n", tsdb_version(), TSDB_BUILD_VERSION);
        return 0;
    }
    /* `tsdb cluster VERB PEER ...`: a server's peer port, no directory. Before the argument count check:
     * `tsdb cluster` alone prints the cluster's commands, as the usage line promises. */
    if (argc >= 2 && strcmp(argv[1], "cluster") == 0) {
        if (!cfg_tried)
            have_cfg = cli_cfg_load(&cfg, NULL, no_config), cfg_tried = 1;
        if (have_cfg)
            argv = cli_with_cfg(&cfg, &argc, argv, "cluster.");
        return cluster_cli(argc - 1, argv + 1);
    }
    if (argc < 3)
        usage();
    if (strcmp(argv[1], "user") == 0) {
        int online = 0;
        for (int i = 3; i < argc; i++)
            if (strcmp(argv[i], "--server") == 0) /* `tsdb user VERB ... --server HOST:PORT`: no directory */
                online = 1;
        if (!cfg_tried)
            have_cfg = cli_cfg_load(&cfg, NULL, no_config), cfg_tried = 1;
        /* a server in the file makes `tsdb user VERB ...` the online form (no database is named "user" with one of
         * these verbs as its command: they are none of the commands on a directory) */
        for (size_t i = 0; have_cfg && !online && i < cfg.n; i++)
            online = strcmp(cfg.s[i].k->opt, "server") == 0 && is_user_verb(argv[2]);
        if (online) {
            if (have_cfg)
                argv = cli_with_cfg(&cfg, &argc, argv, "client.");
            return user_main(NULL, argc - 2, argv + 2);
        }
    }
    if (strcmp(argv[1], "stats") == 0 &&
        (strcmp(argv[2], "series") == 0 || strcmp(argv[2], "tenants") == 0 || strcmp(argv[2], "hot") == 0)) {
        /* `tsdb stats VERB ... --server HOST:PORT`: through a running tsdbd, no directory (no database is named
         * "stats" with one of these as its command: they are none of the commands on a directory) */
        int online = 0;
        for (int i = 3; i < argc; i++)
            if (strcmp(argv[i], "--server") == 0)
                online = 1;
        if (!cfg_tried)
            have_cfg = cli_cfg_load(&cfg, NULL, no_config), cfg_tried = 1;
        for (size_t i = 0; have_cfg && !online && i < cfg.n; i++)
            online = strcmp(cfg.s[i].k->opt, "server") == 0;
        if (online) {
            if (have_cfg)
                argv = cli_with_cfg(&cfg, &argc, argv, "client.");
            const char *server = NULL, *login = NULL, *pwfile = NULL;
            char **rest = malloc((size_t)argc * sizeof *rest);
            int nr = 0;
            if (!rest)
                die("stats", TSDB_ERR_NOMEM);
            for (int i = 3; i < argc; i++) {
                if (strcmp(argv[i], "--server") == 0 && i + 1 < argc)
                    server = argv[++i];
                else if (strcmp(argv[i], "--login") == 0 && i + 1 < argc)
                    login = argv[++i];
                else if ((strcmp(argv[i], "--login-password-file") == 0 || strcmp(argv[i], "--password-file") == 0) &&
                         i + 1 < argc)
                    pwfile = argv[++i];
                else if (strncmp(argv[i], "--password", 10) == 0 && strcmp(argv[i], "--password-file") != 0) {
                    fprintf(stderr, "tsdb: a password is never taken from the command line (ps shows it to every "
                                    "user):\n      use --login-password-file FILE, TSDB_LOGIN_PASSWORD or the "
                                    "prompt\n");
                    return 2;
                } else
                    rest[nr++] = argv[i];
            }
            if (!server || (pwfile && !login))
                usage();
            return stats_online(server, login, pwfile, argv[2], nr, rest);
        }
    }
    const char *dir = argv[1], *cmd = argv[2];
    drop_root(dir); /* before anything in dir is opened, by every command */
    if (strcmp(cmd, "upgrade") == 0) {
        if (argc != 3)
            usage();
        int rc = tsdb_upgrade(dir); /* the database is not opened: its files are of the previous format */
        if (rc < 0)
            die(dir, rc);
        if (rc == 0)
            printf("%s: nothing to upgrade (already disk format 9)\n", dir);
        else
            printf("%s: %d director%s carried over to disk format 9; parts keep their old headers until merges "
                   "rewrite them\n", dir, rc, rc == 1 ? "y" : "ies");
        return 0;
    }
    if (strcmp(cmd, "volume") == 0 && argc == 5 && strcmp(argv[3], "add") == 0) {
        /* without opening the database: a running tsdbd holds its lock, and opens the volume when a
         * request first names it */
        uint32_t index;
        int rc = tsdb_volume_add_dir(dir, argv[4], &index);
        if (rc == TSDB_ERR_INVALID && argv[4][0] != '/')
            fprintf(stderr, "tsdb: %s: a volume path must be absolute\n", argv[4]);
        if (rc)
            die(argv[4], rc);
        printf("%u\t%s\n", index, argv[4]);
        return 0;
    }
    if (strcmp(cmd, "volume") == 0 && (argc != 4 || strcmp(argv[3], "list") != 0))
        usage();
    if (strcmp(cmd, "user") == 0)
        return user_main(dir, argc - 3, argv + 3); /* USERS only: the engine is not opened */
    if (strcmp(cmd, "export") == 0)
        return dump_export_main(dir, argc - 3, argv + 3);
    FILE *csv = stdin;
    if (strcmp(cmd, "import") == 0) {
        int st = dump_import_main(dir, argc - 3, argv + 3, &csv);
        if (st >= 0)
            return st;
        if (!csv) {
            fprintf(stderr, "tsdb: import: %s\n", strerror(errno));
            return 1;
        }
    }
    bool writes = strcmp(cmd, "insert") == 0 || strcmp(cmd, "import") == 0 || strcmp(cmd, "optimize") == 0 ||
                  strcmp(cmd, "drop-before") == 0 || strcmp(cmd, "create") == 0 || strcmp(cmd, "delete") == 0 ||
                  (strcmp(cmd, "retention") == 0 && argc > 4) ||
                  (strcmp(cmd, "feed") == 0 && argc > 3 && strcmp(argv[3], "info") != 0);
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = !writes;
    o.merge_threads = 0; /* a short-lived process: merging happens in optimize or in the long-running writer */
    o.create_volumes = 0; /* volumes are added by "volume add", as for tsdbd */
    o.stats = 0;          /* "stats" settles the sweeper once; every other verb never starts it */
    o.stats_sweep_mb_s = -1; /* a one-shot walk of the indexes: no cap */
    if (strcmp(cmd, "stats") == 0 && argc >= 4 && strcmp(argv[3], "tenants") == 0) {
        tenant_args(argc, argv);
        o.tenants = tenant_list;
        o.ntenants = tenant_n;
    }
    if (!writes && access(dir, F_OK) != 0) {
        fprintf(stderr, "tsdb: %s does not exist\n", dir);
        return 1;
    }
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc)
        die(dir, rc);
    int ret = 0;
    if (strcmp(cmd, "insert") == 0) {
        if (argc < 4)
            usage();
        ret = cmd_import(db, argv[3], stdin);
    } else if (strcmp(cmd, "import") == 0) {
        ret = cmd_import(db, NULL, csv);
        if (csv != stdin)
            fclose(csv); /* the cookie stream of dump_import_main frees its read-ahead buffer */
    } else if (strcmp(cmd, "query") == 0) {
        ret = cmd_query(db, argc - 3, argv + 3);
    } else if (strcmp(cmd, "create") == 0) {
        const char *name = NULL, *path = NULL;
        int type = TSDB_FLOAT32;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) {
                if ((type = parse_type(argv[++i])) < 0)
                    usage();
            } else if (!name) {
                name = argv[i];
            } else if (!path) {
                path = argv[i];
            } else {
                usage();
            }
        }
        if (!name)
            usage();
        tsdb_sid id;
        if ((rc = tsdb_series_create(db, name, (tsdb_type)type, path, &id)) != 0)
            die(name, rc);
        printf("%" PRIu64 "\t", id);
        put_name(stdout, name);
        printf("\t%s\t", type_names[type]);
        put_name(stdout, tsdb_series_path(db, id));
        putchar('\n');
    } else if (strcmp(cmd, "series") == 0) {
        rc = tsdb_series_foreach(db, print_series, db);
        if (rc)
            die("series", rc);
    } else if (strcmp(cmd, "volume") == 0) {
        for (uint32_t i = 0; i < tsdb_volume_count(db); i++)
            printf("%u\t%s\n", i, tsdb_volume_path(db, i));
    } else if (strcmp(cmd, "latest") == 0) {
        rc = cmd_latest(db, argc - 3, argv + 3);
        if (rc)
            ret = rc;
    } else if (strcmp(cmd, "stats") == 0 && argc >= 4 && strcmp(argv[3], "series") == 0) {
        rc = cmd_stats_series(db, argc - 4, argv + 4);
    } else if (strcmp(cmd, "stats") == 0 && argc >= 4 && strcmp(argv[3], "tenants") == 0) {
        rc = cmd_stats_tenants(db);
    } else if (strcmp(cmd, "stats") == 0 && argc >= 4 && strcmp(argv[3], "hot") == 0) {
        rc = cmd_stats_hot(db, argc - 4, argv + 4);
    } else if (strcmp(cmd, "stats") == 0) {
        rc = cmd_stats(db);
    } else if (strcmp(cmd, "parts") == 0) {
        tsdb_foreach_part(db, print_part, NULL);
    } else if (strcmp(cmd, "optimize") == 0) {
        double t0 = now_s();
        if ((rc = tsdb_optimize(db)) != 0)
            die("optimize", rc);
        fprintf(stderr, "optimized in %.2f s\n", now_s() - t0);
    } else if (strcmp(cmd, "drop-before") == 0) {
        int64_t t;
        if (argc < 4 || !parse_time(argv[3], &t))
            usage();
        if ((rc = tsdb_drop_before(db, t)) != 0)
            die("drop-before", rc);
    } else if (strcmp(cmd, "delete") == 0) {
        int64_t a, b;
        tsdb_sid id;
        if (argc != 6 || !parse_time(argv[4], &a) || !parse_time(argv[5], &b))
            usage();
        if ((rc = tsdb_series_lookup(db, argv[3], &id)) != 0 || (rc = tsdb_delete_range(db, id, a, b)) != 0)
            die(argv[3], rc);
    } else if (strcmp(cmd, "feed") == 0) {
        ret = cmd_feed(db, argc - 3, argv + 3);
    } else if (strcmp(cmd, "retention") == 0) {
        tsdb_sid id;
        int64_t limit = 0;
        if (argc < 4 || argc > 5 || (argc == 5 && strcmp(argv[4], "off") != 0 &&
                                     (!parse_interval(argv[4], &limit) || limit <= 0)))
            usage();
        if ((rc = tsdb_series_lookup(db, argv[3], &id)) != 0)
            die(argv[3], rc);
        if (argc == 5 && (rc = tsdb_set_retention(db, id, limit)) != 0)
            die(argv[3], rc);
        if ((rc = tsdb_get_retention(db, id, &limit)) != 0)
            die(argv[3], rc);
        if (limit)
            printf("%s\t%" PRId64 " ms\n", argv[3], limit);
        else
            printf("%s\toff\n", argv[3]);
    } else {
        usage();
    }
    if ((rc = tsdb_close(db)) != 0)
        die("close", rc);
    return ret;
}
