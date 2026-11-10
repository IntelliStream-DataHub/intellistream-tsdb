/* SPDX-License-Identifier: AGPL-3.0-only */
/* The C client of the integration tests: libtsdbclient, as a user links it.
 *
 *   itc write <port> <writes-file>       apply a write script (see run.py)
 *   itc read  <port> <queries-file>      print the canonical answers
 *   itc load  <port> <series> <seed> [level]   pipelined load for the kill -9 scenario
 *   itc admin <port> flush|optimize
 *   itc ids   <port> <names-file>        print "name id" per line (C only: run.py's ids for reads by id)
 *
 * All four clients (C, Rust, Python, Java) speak this command line and print
 * byte-identical output for the same queries; run.py compares them. A query whose series is written
 * "#ID:TYPE" names it by id, with no lookup on the server: a tenant reading another's series by an id it got
 * elsewhere must be refused by the query itself, the check a lookup by name never reaches. */
#define _GNU_SOURCE
#include "tsdb_client.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void die(const char *what, int rc)
{
    fprintf(stderr, "itc: %s: %s\n", what, rc ? tsdbc_strerror(rc) : "");
    exit(1);
}

static const char *type_names[] = {"float32", "float64", "int64", "bool", "state"};

static int parse_type(const char *s)
{
    for (int t = 0; t < 5; t++)
        if (strcmp(s, type_names[t]) == 0)
            return t;
    fprintf(stderr, "itc: unknown type %s\n", s);
    exit(1);
}

/* ---- series by name, with their types ---- */
typedef struct sinfo {
    char name[128];
    tsdb_sid id;
    tsdb_type type;
} sinfo;
static sinfo infos[256];
static int ninfos;

static sinfo *find(tsdbc *c, const char *name, int create, tsdb_type type)
{
    for (int i = 0; i < ninfos; i++)
        if (strcmp(infos[i].name, name) == 0)
            return &infos[i];
    sinfo *s = &infos[ninfos];
    snprintf(s->name, sizeof s->name, "%s", name);
    int rc;
    if (!create && name[0] == '#') {
        char *colon = strchr(name, ':');
        if (!colon)
            die(name, 0);
        s->id = strtoull(name + 1, NULL, 10);
        s->type = parse_type(colon + 1);
    } else if (create) {
        if ((rc = tsdbc_series_create(c, name, type, NULL, &s->id)) != 0)
            die(name, rc);
        s->type = type;
    } else {
        if ((rc = tsdbc_series(c, name, 0, &s->id)) != 0)
            return NULL;
        if ((rc = tsdbc_series_type(c, s->id, &s->type)) != 0)
            die(name, rc);
    }
    ninfos++;
    return s;
}

/* ---- tokens ---- */
static size_t slot(tsdb_type t) { return t == TSDB_BOOL ? 1 : (t == TSDB_FLOAT32 || t == TSDB_STATE) ? 4 : 8; }

typedef struct column {
    tsdb_type type;
    size_t n, cap;
    int64_t *ts;
    tsdb_sid *sid;
    uint8_t *v;
    uint8_t *valid;
    int nulls;
} column;

static void col_push(column *c, int64_t ts, tsdb_sid sid, const void *v, int known)
{
    if (c->n == c->cap) {
        c->cap = c->cap ? c->cap * 2 : 1024;
        c->ts = realloc(c->ts, c->cap * 8);
        c->sid = realloc(c->sid, c->cap * sizeof *c->sid);
        c->v = realloc(c->v, c->cap * 8);
        c->valid = realloc(c->valid, c->cap / 8 + 8);
    }
    size_t i = c->n++, w = slot(c->type);
    c->ts[i] = ts;
    c->sid[i] = sid;
    memset(c->v + i * w, 0, w);
    if (known)
        memcpy(c->v + i * w, v, w);
    if (i % 8 == 0)
        c->valid[i / 8] = 0;
    if (known)
        c->valid[i / 8] |= (uint8_t)(1u << (i % 8));
    else
        c->nulls = 1;
}

static int parse_token(tsdbc *c, sinfo *s, const char *tok, uint8_t out[8])
{
    memset(out, 0, 8);
    if (strcmp(tok, "N") == 0)
        return 0;
    switch (s->type) {
    case TSDB_FLOAT32: {
        uint32_t b = (uint32_t)strtoul(tok, NULL, 16);
        memcpy(out, &b, 4);
        break;
    }
    case TSDB_FLOAT64: {
        uint64_t b = strtoull(tok, NULL, 16);
        memcpy(out, &b, 8);
        break;
    }
    case TSDB_INT64: {
        int64_t x = strtoll(tok, NULL, 10);
        memcpy(out, &x, 8);
        break;
    }
    case TSDB_BOOL: out[0] = tok[0] == '1'; break;
    default: {
        uint32_t code;
        int rc = tsdbc_state(c, s->id, tok, &code);
        if (rc)
            die(tok, rc);
        memcpy(out, &code, 4);
    }
    }
    return 1;
}

/* ---- write ---- */
static void do_write(tsdbc *c, const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f)
        die(path, TSDB_ERR_IO);
    char *line = NULL;
    size_t cap = 0;
    ssize_t len;
    int async_used = 0;
    while ((len = getline(&line, &cap, f)) > 0) {
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = 0;
        if (!len)
            continue;
        char a[200], b[200], flags[64];
        int level;
        size_t n;
        if (line[0] == 'S') {
            if (sscanf(line, "S %199s %199s", a, b) != 2)
                die("bad S line", 0);
            find(c, a, 1, (tsdb_type)parse_type(b));
            continue;
        }
        column col = {0};
        sinfo *one = NULL;
        int multi = line[0] == 'M';
        if (multi) {
            if (sscanf(line, "M %199s %d %63s %zu", a, &level, flags, &n) != 4)
                die("bad M line", 0);
            col.type = (tsdb_type)parse_type(a);
        } else {
            if (sscanf(line, "B %199s %d %63s %zu", a, &level, flags, &n) != 4)
                die("bad B line", 0);
            one = find(c, a, 0, 0);
            if (!one)
                die(a, TSDB_ERR_NOTFOUND);
            col.type = one->type;
        }
        for (size_t i = 0; i < n; i++) {
            if (getline(&line, &cap, f) <= 0)
                die("short write script", 0);
            char *save, *tok[3];
            int k = 0;
            for (char *p = strtok_r(line, " \t\r\n", &save); p && k < 3; p = strtok_r(NULL, " \t\r\n", &save))
                tok[k++] = p;
            if (k != (multi ? 3 : 2))
                die("bad row", 0);
            sinfo *s = multi ? find(c, tok[0], 0, 0) : one;
            if (!s)
                die("unknown series in M batch", TSDB_ERR_NOTFOUND);
            uint8_t v[8];
            int known = parse_token(c, s, tok[k - 1], v);
            col_push(&col, strtoll(tok[k - 2], NULL, 10), s->id, v, known);
        }
        tsdbc_set_compression(c, level);
        int is_async = strstr(flags, "async") != NULL, legacy = strstr(flags, "legacy") != NULL, rc;
        tsdb_values vals = {col.type, col.v, col.nulls ? col.valid : NULL};
        if (legacy) { /* float32 without nulls: the original array API */
            if (col.type != TSDB_FLOAT32 || col.nulls)
                die("legacy batch is not plain float32", 0);
            rc = multi ? (is_async ? tsdbc_insert_async : tsdbc_insert)(c, col.sid, col.ts, (const float *)col.v, n)
                       : (is_async ? tsdbc_insert_series_async : tsdbc_insert_series)(c, one->id, col.ts,
                                                                                      (const float *)col.v, n);
        } else if (multi)
            rc = (is_async ? tsdbc_insert_values_async : tsdbc_insert_values)(c, col.sid, col.ts, &vals, n);
        else
            rc = (is_async ? tsdbc_insert_series_values_async : tsdbc_insert_series_values)(c, one->id, col.ts, &vals,
                                                                                            n);
        if (rc)
            die("insert", rc);
        async_used |= is_async;
        free(col.ts);
        free(col.sid);
        free(col.v);
        free(col.valid);
    }
    int rc = tsdbc_wait(c);
    if (rc)
        die("wait", rc);
    (void)async_used;
    free(line);
    fclose(f);
}

/* ---- canonical output ---- */
static tsdbc *g_c;
static sinfo *g_s;
static char g_names[1024][64];
static uint32_t g_codes[1024];
static int g_nnames;

static const char *state_name(uint32_t code)
{
    for (int i = 0; i < g_nnames; i++)
        if (g_codes[i] == code)
            return g_names[i];
    char buf[64];
    int rc = tsdbc_state_name(g_c, g_s->id, code, buf, sizeof buf);
    if (rc)
        die("state_name", rc);
    snprintf(g_names[g_nnames], 64, "%s", buf);
    g_codes[g_nnames] = code;
    return g_names[g_nnames++];
}

static int emit_values(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    (void)ctx;
    for (size_t i = 0; i < n; i++) {
        printf("%" PRId64 "\t", ts[i]);
        if (col->valid && !((col->valid[i >> 3] >> (i & 7)) & 1)) {
            puts("N");
            continue;
        }
        switch (col->type) {
        case TSDB_FLOAT32: printf("%08x\n", ((const uint32_t *)col->values)[i]); break;
        case TSDB_FLOAT64: printf("%016" PRIx64 "\n", ((const uint64_t *)col->values)[i]); break;
        case TSDB_INT64: printf("%" PRId64 "\n", ((const int64_t *)col->values)[i]); break;
        case TSDB_BOOL: printf("%d\n", ((const uint8_t *)col->values)[i] ? 1 : 0); break;
        default: puts(state_name(((const uint32_t *)col->values)[i]));
        }
    }
    return 0;
}

static void emit_latest(const tsdb_latest_result *r)
{
    if (r->kind == TSDB_LATEST_NONE)
        return;
    printf("%" PRId64 "\t", r->ts);
    if (r->kind == TSDB_LATEST_NULL) {
        puts("N");
        return;
    }
    switch (r->type) {
    case TSDB_FLOAT32: {
        uint32_t b;
        memcpy(&b, &r->v.f32, 4);
        printf("%08x\n", b);
        break;
    }
    case TSDB_FLOAT64: {
        uint64_t b;
        memcpy(&b, &r->v.f64, 8);
        printf("%016" PRIx64 "\n", b);
        break;
    }
    case TSDB_INT64: printf("%" PRId64 "\n", r->v.i64); break;
    case TSDB_BOOL: printf("%d\n", r->v.b ? 1 : 0); break;
    default: puts(state_name(r->v.state));
    }
}

static int emit_f32(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ctx;
    for (size_t i = 0; i < n; i++) {
        uint32_t b;
        memcpy(&b, &v[i], 4);
        printf("%" PRId64 "\t%08x\n", ts[i], b);
    }
    return 0;
}

static uint64_t dbits(double d)
{
    uint64_t b;
    memcpy(&b, &d, 8);
    return b;
}

static void emit_buckets(const tsdb_bucket *b, size_t n)
{
    for (size_t i = 0; i < n; i++)
        printf("%" PRId64 "\t%" PRIu64 "\t%016" PRIx64 "\t%016" PRIx64 "\t%016" PRIx64 "\t%" PRId64 "\t%" PRId64
               "\t%" PRId64 "\n",
               b[i].start, b[i].count, dbits(b[i].sum), dbits(b[i].min), dbits(b[i].max), b[i].isum, b[i].imin,
               b[i].imax);
}

static int parse_unit(const char *s, tsdb_unit *u)
{
    static const char *names[] = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"};
    for (int i = 0; i < 8; i++)
        if (strcmp(s, names[i]) == 0) {
            *u = (tsdb_unit)(i + 1);
            return 1;
        }
    return 0;
}

static void do_read(tsdbc *c, const char *path)
{
    g_c = c;
    FILE *f = fopen(path, "r");
    if (!f)
        die(path, TSDB_ERR_IO);
    char *line = NULL;
    size_t cap = 0;
    ssize_t len;
    while ((len = getline(&line, &cap, f)) > 0) {
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = 0;
        if (!len)
            continue;
        printf("## %s\n", line);
        char kind[16], name[200], ord[16], unit[16], tz[64];
        int64_t start, end;
        char *save;
        char *tok[12];
        int k = 0;
        char *copy = strdup(line);
        for (char *p = strtok_r(copy, " ", &save); p && k < 12; p = strtok_r(NULL, " ", &save))
            tok[k++] = p;
        snprintf(kind, sizeof kind, "%s", tok[0]);
        snprintf(name, sizeof name, "%s", tok[1]);
        start = strtoll(tok[2], NULL, 10);
        end = strtoll(tok[3], NULL, 10);
        sinfo *s = find(c, name, 0, 0);
        if (!s) {
            puts("!!");
            free(copy);
            continue;
        }
        g_s = s;
        tsdb_query_opts o = {TSDB_ASC, 0};
        int rc = 0;
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if (strcmp(kind, "raw") == 0 || strcmp(kind, "f32raw") == 0) {
            snprintf(ord, sizeof ord, "%s", tok[4]);
            o.order = strcmp(ord, "desc") == 0 ? TSDB_DESC : TSDB_ASC;
            o.limit = strtoull(tok[5], NULL, 10);
            rc = strcmp(kind, "raw") == 0 ? tsdbc_query_values(c, s->id, start, end, &o, emit_values, NULL)
                                          : tsdbc_query_raw_ex(c, s->id, start, end, &o, emit_f32, NULL);
        } else if (strcmp(kind, "agg") == 0) {
            rc = tsdbc_query_agg(c, s->id, start, end, strtoll(tok[4], NULL, 10), &b, &nb);
        } else if (strcmp(kind, "grp") == 0) {
            snprintf(unit, sizeof unit, "%s", tok[4]);
            snprintf(tz, sizeof tz, "%s", tok[6]);
            snprintf(ord, sizeof ord, "%s", tok[7]);
            o.order = strcmp(ord, "desc") == 0 ? TSDB_DESC : TSDB_ASC;
            o.limit = strtoull(tok[8], NULL, 10);
            tsdb_group g = {0, (uint32_t)atoi(tok[5]), strcmp(tz, "-") ? tz : NULL};
            if (strcmp(unit, "none") == 0)
                rc = tsdbc_query_group(c, s->id, start, end, NULL, &o, &b, &nb);
            else if (!parse_unit(unit, &g.unit))
                die("unit", 0);
            else
                rc = tsdbc_query_group(c, s->id, start, end, &g, &o, &b, &nb);
        } else if (strcmp(kind, "bnd") == 0) {
            snprintf(ord, sizeof ord, "%s", tok[4]);
            o.order = strcmp(ord, "desc") == 0 ? TSDB_DESC : TSDB_ASC;
            o.limit = strtoull(tok[5], NULL, 10);
            int64_t bounds[4096];
            size_t m = 0;
            char *sv2;
            for (char *p = strtok_r(tok[6], ",", &sv2); p && m < 4096; p = strtok_r(NULL, ",", &sv2))
                bounds[m++] = strtoll(p, NULL, 10);
            rc = tsdbc_query_bounds(c, s->id, start, end, bounds, m, &o, &b, &nb);
        } else if (strcmp(kind, "latest") == 0) {
            /* latest NAME START END WITH_NULLS how: the newest row before END, by id or by name (see run.py) */
            tsdb_latest_opts lo = {.end = end, .with_nulls = atoi(tok[4]), .threads = 0};
            tsdb_latest_result r;
            const char *nm = name;
            if (strcmp(tok[5], "name") == 0 && name[0] != '#')
                rc = tsdbc_latest_by_name(c, &nm, 1, &lo, &r);
            else
                rc = tsdbc_latest(c, &s->id, 1, &lo, &r);
            if (!rc && r.status)
                rc = r.status;
            if (!rc)
                emit_latest(&r);
        } else
            die("unknown query kind", 0);
        if (rc)
            puts("!!");
        else if (b)
            emit_buckets(b, nb);
        free(b);
        free(copy);
    }
    free(line);
    fclose(f);
}

/* ---- ids of named series (as the superuser), for run.py's reads by id ---- */
static void do_ids(tsdbc *c, const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f)
        die(path, TSDB_ERR_IO);
    char name[256];
    while (fscanf(f, "%255s", name) == 1) {
        tsdb_sid id;
        int rc = tsdbc_series(c, name, 0, &id);
        if (rc)
            die(name, rc);
        printf("%s %" PRIu64 "\n", name, (uint64_t)id);
    }
    fclose(f);
}

/* ---- durability load ----
 * One series, `rows` timestamps 1000 ms apart. Pass 1 writes value base+i, pass 2 overwrites the same
 * timestamps with base+i+1000000. Batches of 200 rows are sent 8 at a time and then waited for together (the
 * pipeline drains every 8 batches, it is not a sliding window); after every tsdbc_wait that returns 0 the
 * batches since the last wait are acknowledged and printed as "ACK <pass> <first-batch> <last-batch>". */
#define LOAD_ROWS 1000000
#define LOAD_BATCH 200
#define LOAD_FLIGHT 8
static void do_load(tsdbc *c, const char *name, int level, int seed)
{
    tsdb_sid id;
    int rc = tsdbc_series_create(c, name, TSDB_INT64, NULL, &id);
    if (rc)
        die("series", rc);
    tsdbc_set_compression(c, level);
    printf("READY\n");
    fflush(stdout);
    int64_t ts[LOAD_BATCH], v[LOAD_BATCH];
    tsdb_values vals = {TSDB_INT64, v, NULL};
    for (int pass = 1; pass <= 2; pass++) {
        int nb = LOAD_ROWS / LOAD_BATCH, first = 0;
        for (int b = 0; b < nb; b++) {
            for (int i = 0; i < LOAD_BATCH; i++) {
                int r = b * LOAD_BATCH + i;
                ts[i] = 1700000000000LL + (int64_t)r * 1000;
                v[i] = (int64_t)seed * 100000000 + r + (pass == 2 ? 1000000 : 0);
            }
            if ((rc = tsdbc_insert_series_values_async(c, id, ts, &vals, LOAD_BATCH)) != 0)
                die("insert", rc);
            if ((b + 1) % LOAD_FLIGHT == 0 || b == nb - 1) {
                if ((rc = tsdbc_wait(c)) != 0)
                    die("wait", rc);
                printf("ACK %d %d %d\n", pass, first, b);
                fflush(stdout);
                first = b + 1;
            }
        }
        if (pass == 1) { /* wait for the driver: the clients start pass 2 together */
            char buf[16];
            printf("PASS1\n");
            fflush(stdout);
            if (!fgets(buf, sizeof buf, stdin))
                buf[0] = 0;
        }
    }
    printf("DONE\n");
    fflush(stdout);
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: itc write|read|load|admin|ids <port> <arg>...\n");
        return 2;
    }
    tsdbc *c;
    /* run.py --auth on names the user to log in as in the environment, as it does for every client program */
    tsdbc_options o = {.user = getenv("ITG_USER"), .password_file = getenv("ITG_PASSWORD_FILE")};
    o.require_auth = o.user != NULL;
    int rc = tsdbc_connect_opts("127.0.0.1", atoi(argv[2]), &o, &c);
    if (rc)
        die("connect", rc);
    if (strcmp(argv[1], "write") == 0)
        do_write(c, argv[3]);
    else if (strcmp(argv[1], "read") == 0)
        do_read(c, argv[3]);
    else if (strcmp(argv[1], "ids") == 0)
        do_ids(c, argv[3]);
    else if (strcmp(argv[1], "load") == 0 && argc >= 5)
        do_load(c, argv[3], argc > 5 ? atoi(argv[5]) : 1, atoi(argv[4]));
    else if (strcmp(argv[1], "admin") == 0) {
        rc = strcmp(argv[3], "flush") == 0 ? tsdbc_flush(c) : strcmp(argv[3], "optimize") == 0 ? tsdbc_optimize(c) : -1;
        if (rc)
            die("admin", rc);
    } else {
        fprintf(stderr, "itc: bad command\n");
        return 2;
    }
    rc = tsdbc_wait(c);
    tsdbc_close(c);
    return rc ? 1 : 0;
}
