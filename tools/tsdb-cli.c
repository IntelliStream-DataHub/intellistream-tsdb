/* Command line client for a tsdb directory. */
#include "common.h"
#include "dump.h"
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <math.h>
#include <unistd.h>

#ifndef TSDB_BUILD_VERSION
#define TSDB_BUILD_VERSION "dev"
#endif

static void usage(void)
{
    fprintf(stderr,
            "usage: tsdb <dir> <command> [args]\n"
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
            "  series                             list series (type and volume)\n"
            "  volume add <path>                  add a volume: a directory for series of their own\n"
            "                                     (e.g. one per disk or NFS mount); also while tsdbd runs\n"
            "  volume list                        list the volumes, the database directory first\n"
            "  stats                              rows, parts, size, compression ratio\n"
            "  parts                              list active parts\n"
            "  optimize                           merge every partition into one part\n"
            "  drop-before <time>                 drop whole partitions ending before <time>\n"
            "  delete <series> <start> <end>      delete the series' values in [start, end)\n"
            "  retention <series> [<age>|off]     show or set how long the series keeps values\n"
            "                                     (e.g. 30d, 12h; off keeps them for ever)\n"
            "\n"
            "times: 1700000000000 (ms), 2024-01-31, \"2024-01-31 12:00:00.250\", 2024-01-31T12:00:00Z,\n"
            "       now, now-1h, min, max.  ranges are [start, end).\n"
            "groups: 500ms, 10s, 15m, 6h (fixed length); 1d, 1w, 1mo, 1q, 1y (calendar, in --tz,\n"
            "        e.g. Europe/Oslo; default UTC). Output is ClickHouse-style TSV, times in UTC;\n"
            "        names escaped (\\t, \\n, \\\\, other control bytes as \\xNN).\n");
    exit(2);
}

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
    uint32_t *s;
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

static void batch_push(batch *b, uint32_t s, int64_t t, const void *v, bool known)
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
static bool parse_value(tsdb *db, uint32_t series, tsdb_type type, const char *vs, uint8_t out[8], bool *known)
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
    uint32_t fixed = 0;
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
    uint32_t last_id = 0;
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
        uint32_t id = fixed;
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
    uint32_t series;
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

static int query_twa(tsdb *db, uint32_t id, int64_t start, int64_t end, const tsdb_group *g, int64_t interval,
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
    uint32_t id;
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
    char a[40], b[40];
    format_time(p->min_ts, a, sizeof a);
    format_time(p->max_ts, b, sizeof b);
    printf("%-40s level %-3u %12" PRIu64 " rows %12" PRIu64 " bytes %8" PRIu64 " granules  %s .. %s\n", p->name,
           p->level, p->rows, p->bytes, p->granules, a, b);
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

int main(int argc, char **argv)
{
    if (argc == 2 && (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)) {
        printf("tsdb %s (build %s)\n", tsdb_version(), TSDB_BUILD_VERSION);
        return 0;
    }
    if (argc < 3)
        usage();
    const char *dir = argv[1], *cmd = argv[2];
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
                  (strcmp(cmd, "retention") == 0 && argc > 4);
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = !writes;
    o.merge_threads = 0; /* a short-lived process: merging happens in optimize or in the long-running writer */
    o.create_volumes = 0; /* volumes are added by "volume add", as for tsdbd */
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
        uint32_t id;
        if ((rc = tsdb_series_create(db, name, (tsdb_type)type, path, &id)) != 0)
            die(name, rc);
        printf("%u\t", id);
        put_name(stdout, name);
        printf("\t%s\t", type_names[type]);
        put_name(stdout, tsdb_series_path(db, id));
        putchar('\n');
    } else if (strcmp(cmd, "series") == 0) {
        for (uint32_t i = 0; i < tsdb_series_count(db); i++) {
            tsdb_type t = TSDB_FLOAT32;
            tsdb_series_type(db, i, &t);
            put_name(stdout, tsdb_series_name(db, i));
            printf("\t%s\t", type_names[t]);
            put_name(stdout, tsdb_series_path(db, i));
            putchar('\n');
        }
    } else if (strcmp(cmd, "volume") == 0) {
        for (uint32_t i = 0; i < tsdb_volume_count(db); i++)
            printf("%u\t%s\n", i, tsdb_volume_path(db, i));
    } else if (strcmp(cmd, "stats") == 0) {
        tsdb_stats st;
        tsdb_get_stats(db, &st);
        char sz[32], raw[32];
        human((double)st.bytes, sz, sizeof sz);
        human((double)st.rows * 12, raw, sizeof raw);
        printf("series      %u\nrows        %" PRIu64 "\nparts       %" PRIu64 "\npartitions  %" PRIu64
               "\non disk     %s (%.3f bytes/row)\nuncompressed %s (DateTime64(3) + Float32)\nratio       %.1fx\n"
               "simd        %s\n",
               tsdb_series_count(db), st.rows, st.parts, st.partitions, sz,
               st.rows ? (double)st.bytes / (double)st.rows : 0.0, raw,
               st.bytes ? (double)st.rows * 12 / (double)st.bytes : 0.0, tsdb_simd_level());
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
        uint32_t id;
        if (argc != 6 || !parse_time(argv[4], &a) || !parse_time(argv[5], &b))
            usage();
        if ((rc = tsdb_series_lookup(db, argv[3], &id)) != 0 || (rc = tsdb_delete_range(db, id, a, b)) != 0)
            die(argv[3], rc);
    } else if (strcmp(cmd, "retention") == 0) {
        uint32_t id;
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
