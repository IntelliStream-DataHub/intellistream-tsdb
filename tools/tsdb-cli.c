/* Command line client for a tsdb directory. */
#include "common.h"
#include "tsdb.h"

#include <unistd.h>

static void usage(void)
{
    fprintf(stderr,
            "usage: tsdb <dir> <command> [args]\n"
            "\n"
            "  insert <series>                    read \"time,value\" lines from stdin\n"
            "  import                             read \"series,time,value\" lines from stdin\n"
            "  query <series> <start> <end> [raw|count|sum|min|max|avg|all] [group] [--tz ZONE]\n"
            "        [--desc] [--limit N]         newest first; at most N rows or buckets\n"
            "  create <series> [path]             create a series, in volume <path> if given\n"
            "  series                             list series (and their volume)\n"
            "  stats                              rows, parts, size, compression ratio\n"
            "  parts                              list active parts\n"
            "  optimize                           merge every partition into one part\n"
            "  drop-before <time>                 drop whole partitions ending before <time>\n"
            "\n"
            "times: 1700000000000 (ms), 2024-01-31, \"2024-01-31 12:00:00.250\", 2024-01-31T12:00:00Z,\n"
            "       now, now-1h, min, max.  ranges are [start, end).\n"
            "groups: 500ms, 10s, 15m, 6h (fixed length); 1d, 1w, 1mo, 1q, 1y (calendar, in --tz,\n"
            "        e.g. Europe/Oslo; default UTC). Output is ClickHouse-style TSV, times in UTC.\n");
    exit(2);
}

static void die(const char *what, int rc)
{
    fprintf(stderr, "tsdb: %s: %s\n", what, tsdb_strerror(rc));
    exit(1);
}

/* ------------------------------------------------------------------ import */

typedef struct batch {
    uint32_t *s;
    int64_t *t;
    float *v;
    size_t n, cap;
} batch;

static void batch_push(batch *b, uint32_t s, int64_t t, float v)
{
    if (b->n == b->cap) {
        b->cap = b->cap ? b->cap * 2 : 65536;
        b->s = realloc(b->s, b->cap * sizeof *b->s);
        b->t = realloc(b->t, b->cap * sizeof *b->t);
        b->v = realloc(b->v, b->cap * sizeof *b->v);
        if (!b->s || !b->t || !b->v)
            die("import", TSDB_ERR_NOMEM);
    }
    b->s[b->n] = s;
    b->t[b->n] = t;
    b->v[b->n] = v;
    b->n++;
}

static int cmd_import(tsdb *db, const char *fixed_series)
{
    uint32_t fixed = 0;
    int rc;
    if (fixed_series && (rc = tsdb_series(db, fixed_series, &fixed)) != 0)
        die(fixed_series, rc);
    batch b = {0};
    char *line = NULL;
    size_t cap = 0, lineno = 0, total = 0;
    ssize_t len;
    /* remember the last series name to avoid a dictionary lookup per line */
    char last_name[1024] = "";
    uint32_t last_id = 0;
    double t0 = now_s();
    while ((len = getline(&line, &cap, stdin)) > 0) {
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
        if (!fixed_series) {
            if (strcmp(fields[0], last_name) != 0) {
                if ((rc = tsdb_series(db, fields[0], &last_id)) != 0)
                    die(fields[0], rc);
                snprintf(last_name, sizeof last_name, "%s", fields[0]);
            }
            id = last_id;
        }
        int64_t t;
        char *vend;
        const char *ts = fields[need - 2], *vs = fields[need - 1];
        float v = strtof(vs, &vend);
        if (!parse_time(ts, &t) || vend == vs) {
            fprintf(stderr, "tsdb: line %zu: cannot parse \"%s\"\n", lineno, ts);
            return 1;
        }
        batch_push(&b, id, t, v);
        if (b.n >= (1u << 20)) {
            if ((rc = tsdb_insert(db, b.s, b.t, b.v, b.n)) != 0)
                die("insert", rc);
            total += b.n;
            b.n = 0;
        }
    }
    if (b.n && (rc = tsdb_insert(db, b.s, b.t, b.v, b.n)) != 0)
        die("insert", rc);
    total += b.n;
    double dt = now_s() - t0;
    fprintf(stderr, "inserted %zu rows in %.2f s (%.0f rows/s)\n", total, dt, total / (dt > 0 ? dt : 1));
    free(line);
    free(b.s);
    free(b.t);
    free(b.v);
    return 0;
}

/* ------------------------------------------------------------------- query */

typedef struct raw_out {
    FILE *f;
    size_t n;
} raw_out;

static int print_raw(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    raw_out *o = ctx;
    char tb[40];
    for (size_t i = 0; i < n; i++) {
        format_time(ts[i], tb, sizeof tb);
        fprintf(o->f, "%s\t%.9g\n", tb, v[i]);
    }
    o->n += n;
    return 0;
}

/* "15m", "6h", "1d", "1w", "1mo", "1q", "1y" -> a group; plain numbers and
 * "500ms" give a fixed interval in milliseconds instead. */
static bool parse_group(const char *s, tsdb_group *g, int64_t *interval_ms)
{
    char *end;
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
            g->unit = units[i].unit;
            g->count = (uint32_t)n;
            return true;
        }
    return parse_interval(s, interval_ms) && *interval_ms > 0;
}

static int cmd_query(tsdb *db, int argc, char **argv)
{
    /* positional arguments, then --tz, --desc, --limit anywhere after them */
    const char *pos[5] = {0};
    int npos = 0;
    tsdb_query_opts q = {TSDB_ASC, 0};
    const char *zone = NULL;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--desc") == 0)
            q.order = TSDB_DESC;
        else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc)
            q.limit = strtoull(argv[++i], NULL, 10);
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
    double t0 = now_s();
    if (strcmp(mode, "raw") == 0) {
        raw_out o = {stdout, 0};
        if ((rc = tsdb_query_raw_ex(db, id, start, end, &q, print_raw, &o)) != 0)
            die("query", rc);
        fprintf(stderr, "%zu rows in %.3f ms\n", o.n, (now_s() - t0) * 1e3);
        return 0;
    }
    tsdb_bucket *b;
    size_t nb;
    if (grouped)
        rc = tsdb_query_group_ex(db, id, start, end, &g, &q, &b, &nb);
    else if (interval) {
        rc = tsdb_query_agg(db, id, start, end, interval, &b, &nb);
        if (rc == 0) { /* order and limit as tsdb_query_group_ex applies them */
            if (q.order == TSDB_DESC)
                for (size_t i = 0, j = nb ? nb - 1 : 0; i < j; i++, j--) {
                    tsdb_bucket x = b[i];
                    b[i] = b[j];
                    b[j] = x;
                }
            if (q.limit && nb > q.limit)
                nb = q.limit;
        }
    } else
        rc = tsdb_query_group_ex(db, id, start, end, NULL, &q, &b, &nb);
    if (rc)
        die("query", rc);
    double dt = now_s() - t0;
    bool all = strcmp(mode, "all") == 0;
    if (all)
        printf("time\tcount\tsum\tmin\tmax\tavg\n");
    char tb[40];
    for (size_t i = 0; i < nb; i++) {
        format_time(b[i].start, tb, sizeof tb);
        if (all)
            printf("%s\t%" PRIu64 "\t%.9g\t%.9g\t%.9g\t%.9g\n", tb, b[i].count, b[i].sum, b[i].min, b[i].max,
                   tsdb_bucket_avg(&b[i]));
        else if (strcmp(mode, "count") == 0)
            printf("%s\t%" PRIu64 "\n", tb, b[i].count);
        else if (strcmp(mode, "sum") == 0)
            printf("%s\t%.9g\n", tb, b[i].sum);
        else if (strcmp(mode, "min") == 0)
            printf("%s\t%.9g\n", tb, b[i].min);
        else if (strcmp(mode, "max") == 0)
            printf("%s\t%.9g\n", tb, b[i].max);
        else if (strcmp(mode, "avg") == 0)
            printf("%s\t%.9g\n", tb, tsdb_bucket_avg(&b[i]));
        else
            usage();
    }
    fprintf(stderr, "%zu buckets in %.3f ms\n", nb, dt * 1e3);
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
    if (argc < 3)
        usage();
    const char *dir = argv[1], *cmd = argv[2];
    bool writes = strcmp(cmd, "insert") == 0 || strcmp(cmd, "import") == 0 || strcmp(cmd, "optimize") == 0 ||
                  strcmp(cmd, "drop-before") == 0 || strcmp(cmd, "create") == 0;
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = !writes;
    o.merge_threads = 0; /* a short-lived process: merging happens in optimize or in the long-running writer */
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
        ret = cmd_import(db, argv[3]);
    } else if (strcmp(cmd, "import") == 0) {
        ret = cmd_import(db, NULL);
    } else if (strcmp(cmd, "query") == 0) {
        ret = cmd_query(db, argc - 3, argv + 3);
    } else if (strcmp(cmd, "create") == 0) {
        if (argc < 4)
            usage();
        uint32_t id;
        if ((rc = tsdb_series_at(db, argv[3], argc > 4 ? argv[4] : NULL, &id)) != 0)
            die(argv[3], rc);
        printf("%u\t%s\t%s\n", id, argv[3], tsdb_series_path(db, id));
    } else if (strcmp(cmd, "series") == 0) {
        for (uint32_t i = 0; i < tsdb_series_count(db); i++)
            printf("%s\t%s\n", tsdb_series_name(db, i), tsdb_series_path(db, i));
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
    } else {
        usage();
    }
    if ((rc = tsdb_close(db)) != 0)
        die("close", rc);
    return ret;
}
