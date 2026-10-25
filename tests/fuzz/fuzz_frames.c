/*
 * fuzz_frames: feeds arbitrary bytes to tsdbd's request path (parse() splits them into frames, handle() decodes
 * and runs each request against a real database) without sockets or threads, and checks the replies.
 *
 * It includes server/tsdbd.c itself, so the code under test is the server's own, static functions included;
 * the only differences are OUT_CAP (replies are drained after every request instead of by a send) and a guard
 * on volume paths (the fuzzer must not create directories outside its scratch directory).
 *
 *   fuzz_frames seeds DIR                       write a valid seed frame file per request form
 *   fuzz_frames replay [--dir D] FILE|DIR...    run each input once (make test: the committed corpus)
 *   fuzz_frames fuzz [--dir D] [--seconds N] [--out DIR] [--seed S] CORPUS...
 *       mutate the corpus for N seconds; inputs that reach new code go to --out, a failing one to
 *       --out/crash-* (the run then stops with status 1)
 *
 * Built with ASan and UBSan (make fuzz-frames). There is no clang here, so instead of libFuzzer it has its own
 * driver: GCC's -fsanitize-coverage=trace-pc calls __sanitizer_cov_trace_pc on every basic block of this file
 * (that is, of tsdbd.c), whose edges, counted in AFL's buckets, decide which inputs are kept. Mutations know the
 * frame layout: most keep each frame's length right, so they reach the handlers instead of the framing check.
 *
 * What counts as a failure: a sanitizer report, a crash, an input that runs longer than 20 s, and a reply
 * stream that is not well formed (each request answered by REPLY frames carrying its id, at least one).
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>

/* Volume paths come from the client: keep them inside the scratch directory (the server's own checks of
 * them belong to the volume code and its tests). */
static int fz_path_ok(const char *path) { return !path || (path[0] != '/' && !strstr(path, "..")); }
static int fz_series_at(tsdb *d, const char *name, const char *path, uint32_t *id)
{
    return fz_path_ok(path) ? tsdb_series_at(d, name, path, id) : TSDB_ERR_INVALID;
}
static int fz_series_create(tsdb *d, const char *name, tsdb_type t, const char *path, uint32_t *id)
{
    return fz_path_ok(path) ? tsdb_series_create(d, name, t, path, id) : TSDB_ERR_INVALID;
}
#define tsdb_series_at fz_series_at
#define tsdb_series_create fz_series_create
#define OUT_CAP SIZE_MAX
#define main tsdbd_main
#include "../../server/tsdbd.c"
#undef main
#undef tsdb_series_at
#undef tsdb_series_create

#include "../../client/tsdb_pack.h"

/* ------------------------------------------------------------ coverage */

#define MAP_SIZE (1u << 16)
static uint8_t cov_map[MAP_SIZE], virgin[MAP_SIZE];
static __thread uintptr_t cov_prev;
static int cov_on;

void __sanitizer_cov_trace_pc(void);
__attribute__((no_sanitize_coverage)) void __sanitizer_cov_trace_pc(void)
{
    if (!cov_on)
        return;
    uintptr_t pc = (uintptr_t)__builtin_return_address(0);
    pc = (pc >> 4) ^ (pc << 8);
    cov_map[(pc ^ cov_prev) & (MAP_SIZE - 1)]++;
    cov_prev = pc >> 1;
}

static uint8_t bucket(uint8_t n)
{
    return n == 0 ? 0 : n == 1 ? 1 : n == 2 ? 2 : n == 3 ? 4 : n < 8 ? 8 : n < 16 ? 16 : n < 32 ? 32 : n < 128 ? 64 : 128;
}

/* Whether the last run reached an edge (or a hit-count bucket of one) no earlier run did. */
__attribute__((no_sanitize_coverage)) static int cov_new(void)
{
    int fresh = 0;
    for (size_t i = 0; i < MAP_SIZE; i++) {
        if (!cov_map[i])
            continue;
        uint8_t b = bucket(cov_map[i]);
        if (b & ~virgin[i]) {
            virgin[i] |= b;
            fresh = 1;
        }
    }
    return fresh;
}

__attribute__((no_sanitize_coverage)) static size_t cov_edges(void)
{
    size_t n = 0;
    for (size_t i = 0; i < MAP_SIZE; i++)
        n += virgin[i] != 0;
    return n;
}

/* ----------------------------------------------------------- the server */

static char db_dir[4096];
static const char *crash_dir;
static int verbose; /* FUZZ_VERBOSE: each request's status (to check the seeds do what they should) */
static const uint8_t *cur_in;
static size_t cur_len;

static void fail_dump(void)
{
    if (!cur_in || !crash_dir)
        return;
    char p[4200];
    snprintf(p, sizeof p, "%s/crash-%ld-%d", crash_dir, (long)time(NULL), getpid());
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd >= 0) {
        if (write(fd, cur_in, cur_len) != (ssize_t)cur_len)
            perror("fuzz_frames: write crash input");
        close(fd);
        fprintf(stderr, "fuzz_frames: failing input saved as %s\n", p);
    }
}

static void on_fatal(int sig)
{
    fail_dump();
    signal(sig, SIG_DFL);
    if (sig == SIGALRM)
        fprintf(stderr, "fuzz_frames: an input ran longer than 20 s\n");
    raise(sig == SIGALRM ? SIGABRT : sig);
}

/* the sanitizers' runtime has it; a plain build (make test replays the corpus) does not */
extern void __sanitizer_set_death_callback(void (*cb)(void)) __attribute__((weak));

static void rm_rf(const char *path)
{
    char cmd[8400];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", path);
    if (system(cmd) != 0)
        fprintf(stderr, "fuzz_frames: could not remove %s\n", path);
}

/* A fresh database with a series of every type (ids 0-4) and a state name, so inputs find something. */
static void db_start(void)
{
    char d[4200];
    snprintf(d, sizeof d, "%s/db", db_dir);
    rm_rf(d);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.query_threads = 1;
    o.wal_sync_ms = -1;
    o.partition_ms = 3600000;
    o.granule_rows = 64;
    if (tsdb_open(d, &o, &db) != 0) {
        fprintf(stderr, "fuzz_frames: cannot open %s\n", d);
        exit(2);
    }
    static const char *names[] = {"f32", "f64", "i64", "bool", "state"};
    for (unsigned t = 0; t < 5; t++) {
        uint32_t id;
        tsdb_series_create(db, names[t], (tsdb_type)t, NULL, &id);
    }
    uint32_t code;
    tsdb_state(db, 4, "on", &code);
    int64_t ts[200];
    float v[200];
    for (int i = 0; i < 200; i++)
        ts[i] = 1000 * i, v[i] = (float)i;
    tsdb_insert_series(db, 0, ts, v, 200);
    tsdb_flush(db);
    tsdb_insert_series(db, 0, ts, v, 50); /* some rows in a part, some in the memtable */
}

static void db_stop(void)
{
    tsdb_close(db);
    db = NULL;
}

/* Checks the replies to request `seq` in out[0, len): REPLY frames with its id, at least one. */
static void check_replies(const uint8_t *out, size_t len, uint32_t seq, uint8_t op)
{
    size_t off = 0, frames = 0;
    while (off < len) {
        uint32_t flen, id;
        if (len - off < 4 + 9)
            goto bad;
        memcpy(&flen, out + off, 4);
        memcpy(&id, out + off + 4, 4);
        if (flen < 9 || flen > TSDB_MAX_FRAME || len - off - 4 < flen || id != seq || out[off + 8] != TSDB_OP_REPLY)
            goto bad;
        off += 4 + (size_t)flen;
        frames++;
    }
    if (frames)
        return;
bad:
    fprintf(stderr, "fuzz_frames: bad reply stream for op %u (%zu bytes, %zu frames)\n", op, len, frames);
    fail_dump();
    abort();
}

/* One input: what a client sends on one connection. Requests run in order, as a worker would run them. */
static void run_one(const uint8_t *in, size_t len)
{
    cur_in = in;
    cur_len = len;
    conn *c = calloc(1, sizeof *c);
    if (!c)
        abort();
    c->fd = -1;
    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->cv, NULL);
    /* in pieces, as recv would deliver them: the first piece is short, to go through partial frames */
    size_t first = len > 7 ? 7 : len, at = 0;
    uint32_t seq = 0;
    bool ok = true;
    while (at < len && ok) {
        size_t take = at == 0 ? first : len - at;
        if (buf_reserve(&c->in, take) != 0)
            abort();
        memcpy(c->in.p + c->in.len, in + at, take);
        c->in.len += take;
        at += take;
        ok = parse(c) == 0; /* 1: parked for memory, which only a large frame's partial header can be here */
        while (c->q_head) {
            req *r = c->q_head;
            c->q_head = r->next;
            if (!c->q_head)
                c->q_tail = NULL;
            c->q_bytes -= r->len;
            r->id = seq;
            handle(r);
            check_replies(c->out.p, c->out.len, seq, r->op);
            if (verbose) {
                int32_t st;
                memcpy(&st, c->out.p + 9, 4);
                fprintf(stderr, "  op %u: %u bytes, status %d, %zu reply bytes\n", r->op, (unsigned)r->len, st,
                        c->out.len);
            }
            c->out.len = 0;
            seq++;
            req_free(r);
        }
    }
    free(c->in.p);
    c->in = (buf){0};
    in_account(c);
    free(c->out.p);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    free(c);
    cur_in = NULL;
}

static void timed_run(const uint8_t *in, size_t len)
{
    alarm(20);
    run_one(in, len);
    alarm(0);
}

/* ---------------------------------------------------------------- seeds */

typedef struct fb {
    uint8_t *p;
    size_t len, cap;
} fb;

static void put(fb *b, const void *d, size_t n)
{
    if (b->len + n > b->cap) {
        b->cap = (b->len + n) * 2 + 64;
        b->p = realloc(b->p, b->cap);
        if (!b->p)
            abort();
    }
    memcpy(b->p + b->len, d, n);
    b->len += n;
}
static void p8(fb *b, uint8_t v) { put(b, &v, 1); }
static void p16(fb *b, uint16_t v) { put(b, &v, 2); }
static void p32(fb *b, uint32_t v) { put(b, &v, 4); }
static void p64(fb *b, int64_t v) { put(b, &v, 8); }
static void pstr16(fb *b, const char *s)
{
    p16(b, (uint16_t)strlen(s));
    put(b, s, strlen(s));
}

/* Appends frame (op, payload) to out. */
static void frame(fb *out, uint8_t op, const fb *pl)
{
    p32(out, (uint32_t)(5 + pl->len));
    p32(out, 1);
    p8(out, op);
    if (pl->len)
        put(out, pl->p, pl->len);
}

static void seed_write(const char *dir, const char *name, const fb *b)
{
    char p[4200];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "wb");
    if (!f || fwrite(b->p, 1, b->len, f) != b->len || fclose(f) != 0) {
        fprintf(stderr, "fuzz_frames: cannot write %s\n", p);
        exit(2);
    }
}

/* INSERT_VALUES or REPLACE_RANGE arrays for n rows of `type` (bitmap when valid), plus series when many */
static void values_body(fb *b, unsigned type, uint32_t n, bool valid, bool many)
{
    unsigned w = tsdb_wire_width(type);
    for (uint32_t i = 0; i < n; i++)
        p64(b, 5000 + 1000 * (int64_t)i);
    for (uint32_t i = 0; i < n; i++) {
        uint8_t v[8] = {0};
        if (type == 0) {
            float f = 1.5f * (float)i;
            memcpy(v, &f, 4);
        } else if (type == 1) {
            double d = 2.5 * i;
            memcpy(v, &d, 8);
        } else if (type == 2) {
            int64_t x = (int64_t)i * 7 - 3;
            memcpy(v, &x, 8);
        } else if (type == 3) {
            v[0] = i & 1;
        } else {
            v[0] = 1; /* the state code of "on" */
        }
        put(b, v, w);
    }
    if (valid)
        for (uint32_t i = 0; i < (n + 7) / 8; i++)
            p8(b, 0xb5);
    if (many) { /* b holds the payload from its start: pad to 4, then a series of this type per row */
        while (b->len % 4)
            p8(b, 0);
        for (uint32_t i = 0; i < n; i++)
            p32(b, type);
    }
}

static void values_header(fb *b, uint32_t n, uint32_t s, unsigned type, uint8_t flags)
{
    p32(b, n);
    p32(b, s);
    p8(b, (uint8_t)type);
    p8(b, flags);
    p16(b, 0);
    p32(b, 0);
}

static int cmd_seeds(const char *dir)
{
    mkdir(dir, 0755);
    fb f = {0}, p = {0};
#define SEED(name, op, BODY)                                                                                           \
    do {                                                                                                               \
        f.len = p.len = 0;                                                                                             \
        BODY;                                                                                                          \
        frame(&f, op, &p);                                                                                             \
        seed_write(dir, name, &f);                                                                                     \
    } while (0)
    SEED("hello", TSDB_OP_HELLO, p32(&p, TSDB_PROTOCOL_VERSION));
    SEED("hello-1", TSDB_OP_HELLO, p32(&p, 1));
    SEED("series-lookup", TSDB_OP_SERIES, (p8(&p, 0), pstr16(&p, "f32")));
    SEED("series-create", TSDB_OP_SERIES, (p8(&p, 1), pstr16(&p, "new")));
    SEED("series-path", TSDB_OP_SERIES, (p8(&p, 1), pstr16(&p, "vol"), pstr16(&p, "vol1")));
    SEED("series-name", TSDB_OP_SERIES_NAME, p32(&p, 2));
    SEED("insert", TSDB_OP_INSERT, ({
             p32(&p, 3);
             p32(&p, 0);
             for (int i = 0; i < 3; i++)
                 p64(&p, 300000 + i);
             for (int i = 0; i < 3; i++) {
                 float v = (float)i;
                 put(&p, &v, 4);
             }
         }));
    SEED("insert-many", TSDB_OP_INSERT, ({
             p32(&p, 2);
             p32(&p, TSDB_MANY_SERIES);
             p64(&p, 7);
             p64(&p, 8);
             float v[2] = {1, 2};
             put(&p, v, 8);
             p32(&p, 0);
             p32(&p, 0);
         }));
    SEED("query-raw", TSDB_OP_QUERY_RAW, (p32(&p, 0), p64(&p, 0), p64(&p, 1000000)));
    SEED("query-raw-ex", TSDB_OP_QUERY_RAW, (p32(&p, 0), p64(&p, 0), p64(&p, 1000000), p8(&p, 1), p64(&p, 10)));
    SEED("query-agg", TSDB_OP_QUERY_AGG, (p32(&p, 0), p64(&p, 0), p64(&p, 1000000), p64(&p, 10000)));
    SEED("query-group", TSDB_OP_QUERY_GROUP, ({
             p32(&p, 0);
             p64(&p, 0);
             p64(&p, 100000000);
             p8(&p, 1); /* a calendar unit */
             p32(&p, 1);
             p8(&p, 0);
             p64(&p, 0);
             pstr16(&p, "UTC");
         }));
    SEED("commit", TSDB_OP_COMMIT, (void)0);
    SEED("flush", TSDB_OP_FLUSH, (void)0);
    SEED("optimize", TSDB_OP_OPTIMIZE, (void)0);
    SEED("stats", TSDB_OP_STATS, (void)0);
    SEED("series-create-typed", TSDB_OP_SERIES_CREATE, (p8(&p, 2), pstr16(&p, "i"), p16(&p, 0)));
    SEED("series-create-path", TSDB_OP_SERIES_CREATE, (p8(&p, 1), pstr16(&p, "d"), pstr16(&p, "vol2")));
    SEED("series-type", TSDB_OP_SERIES_TYPE, p32(&p, 1));
    for (unsigned t = 0; t <= TSDB_STATE; t++) {
        char name[64];
        snprintf(name, sizeof name, "insert-values-%u", t);
        SEED(name, TSDB_OP_INSERT_VALUES, (values_header(&p, 5, t, t, 0), values_body(&p, t, 5, false, false)));
        snprintf(name, sizeof name, "insert-values-valid-many-%u", t);
        SEED(name, TSDB_OP_INSERT_VALUES,
             (values_header(&p, 9, TSDB_MANY_SERIES, t, TSDB_VALID), values_body(&p, t, 9, true, true)));
        snprintf(name, sizeof name, "replace-%u", t);
        SEED(name, TSDB_OP_REPLACE_RANGE, ({
                 values_header(&p, 3, t, t, TSDB_VALID);
                 p64(&p, 0);
                 p64(&p, 100000);
                 values_body(&p, t, 3, true, false);
             }));
        /* packed: the arrays reshaped and compressed, as the clients send them */
        for (int many = 0; many < 2; many++) {
            uint32_t n = 20;
            int64_t ts[20];
            uint8_t vals[20 * 8] = {0}, valid[3] = {0xff, 0x7f, 0x0f};
            uint32_t series[20];
            for (uint32_t i = 0; i < n; i++)
                ts[i] = 50000 + 10 * i, series[i] = t, vals[i * tsdb_wire_width(t)] = (uint8_t)(t == 4 ? 0 : i);
            size_t bound = tsdb_pack_raw_bound(n, t, 1, many);
            uint8_t *raw = malloc(bound), *z = malloc(ZSTD_compressBound(bound));
            size_t rl = tsdb_pack_reshape(n, t, ts, vals, valid, many ? series : NULL, raw);
            size_t zl = ZSTD_compress(z, ZSTD_compressBound(bound), raw, rl, 1);
            snprintf(name, sizeof name, "insert-packed-%u%s", t, many ? "-many" : "");
            SEED(name, TSDB_OP_INSERT_VALUES, ({
                     values_header(&p, n, many ? TSDB_MANY_SERIES : t, t, TSDB_PACKED | TSDB_VALID);
                     p32(&p, (uint32_t)rl);
                     put(&p, z, zl);
                 }));
            free(raw);
            free(z);
        }
    }
    SEED("query-values", TSDB_OP_QUERY_VALUES, (p32(&p, 1), p64(&p, 0), p64(&p, 1000000)));
    SEED("query-values-ex", TSDB_OP_QUERY_VALUES, (p32(&p, 2), p64(&p, 0), p64(&p, 1000000), p8(&p, 1), p64(&p, 3)));
    SEED("state", TSDB_OP_STATE, (p32(&p, 4), p8(&p, 1), pstr16(&p, "off")));
    SEED("state-lookup", TSDB_OP_STATE, (p32(&p, 4), p8(&p, 0), pstr16(&p, "on")));
    SEED("state-name", TSDB_OP_STATE_NAME, (p32(&p, 4), p32(&p, 1)));
    SEED("query-buckets", TSDB_OP_QUERY_BUCKETS, ({
             p32(&p, 0);
             p64(&p, 0);
             p64(&p, 1000000);
             p64(&p, 20000);
             p8(&p, 0);
             p32(&p, 0);
             p8(&p, 0);
             p64(&p, 0);
             p16(&p, 0);
         }));
    SEED("query-buckets-spread", TSDB_OP_QUERY_BUCKETS, ({
             p32(&p, 0);
             p64(&p, 0);
             p64(&p, 1000000);
             p64(&p, 0);
             p8(&p, 1);
             p32(&p, 1);
             p8(&p, 1);
             p64(&p, 2);
             pstr16(&p, "UTC");
             p8(&p, TSDB_SPREAD);
         }));
    SEED("last-stats", TSDB_OP_LAST_STATS, (void)0);
    SEED("query-bounds", TSDB_OP_QUERY_BOUNDS, ({
             p32(&p, 0);
             p64(&p, 0);
             p64(&p, 1000000);
             p8(&p, 0);
             p64(&p, 0);
             p32(&p, 3);
             p64(&p, 0);
             p64(&p, 50000);
             p64(&p, 100000);
         }));
    SEED("query-bounds-spread", TSDB_OP_QUERY_BOUNDS, ({
             p32(&p, 1);
             p64(&p, 0);
             p64(&p, 1000000);
             p8(&p, 1);
             p64(&p, 1);
             p32(&p, 2);
             p64(&p, 0);
             p64(&p, 50000);
             p8(&p, TSDB_SPREAD);
         }));
    SEED("query-lttb", TSDB_OP_QUERY_LTTB, (p32(&p, 0), p64(&p, 0), p64(&p, 1000000), p64(&p, 10)));
    SEED("query-lttb-ex", TSDB_OP_QUERY_LTTB,
         (p32(&p, 0), p64(&p, 0), p64(&p, 1000000), p64(&p, 10), p8(&p, 1), p64(&p, 4)));
    for (int form = 0; form < 3; form++) {
        char name[64];
        snprintf(name, sizeof name, "query-twa-%d", form);
        SEED(name, TSDB_OP_QUERY_TWA, ({
                 p32(&p, 0);
                 p64(&p, 0);
                 p64(&p, 1000000);
                 p8(&p, (uint8_t)(form & 1)); /* method */
                 p8(&p, 0);
                 p8(&p, form == 1 ? 1 : 0); /* unit */
                 p8(&p, 0);
                 p64(&p, 5000);
                 p64(&p, 0);
                 p64(&p, form == 0 ? 10000 : 0);
                 p32(&p, form == 1 ? 1 : 0);
                 p32(&p, form == 2 ? 2 : 0);
                 if (form == 2) {
                     p64(&p, 0);
                     p64(&p, 30000);
                 }
             }));
    }
    SEED("delete-range", TSDB_OP_DELETE_RANGE, (p32(&p, 0), p32(&p, 0), p64(&p, 10000), p64(&p, 20000)));
    SEED("set-retention", TSDB_OP_SET_RETENTION, (p32(&p, 1), p32(&p, 0), p64(&p, 86400000)));
    SEED("get-retention", TSDB_OP_GET_RETENTION, p32(&p, 1));
    /* a pipeline: several requests in one input */
    f.len = 0;
    p.len = 0;
    p32(&p, TSDB_PROTOCOL_VERSION);
    frame(&f, TSDB_OP_HELLO, &p);
    p.len = 0;
    values_header(&p, 4, 1, 1, 0);
    values_body(&p, 1, 4, false, false);
    frame(&f, TSDB_OP_INSERT_VALUES, &p);
    p.len = 0;
    p32(&p, 1), p64(&p, 0), p64(&p, 1000000);
    frame(&f, TSDB_OP_QUERY_VALUES, &p);
    frame(&f, TSDB_OP_LAST_STATS, &(fb){0});
    seed_write(dir, "pipeline", &f);
    free(f.p);
    free(p.p);
    return 0;
}

/* ------------------------------------------------------------ the corpus */

typedef struct input {
    uint8_t *p;
    size_t len;
} input;
static input *corpus;
static size_t ncorpus, corpus_cap;

static void corpus_add(const uint8_t *p, size_t len)
{
    if (ncorpus == corpus_cap) {
        corpus_cap = corpus_cap ? corpus_cap * 2 : 256;
        corpus = realloc(corpus, corpus_cap * sizeof *corpus);
        if (!corpus)
            abort();
    }
    uint8_t *c = malloc(len ? len : 1);
    if (!c)
        abort();
    memcpy(c, p, len);
    corpus[ncorpus++] = (input){c, len};
}

static int load_file(const char *path)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return -1;
    uint8_t *b = malloc(1 << 20);
    size_t n = b ? fread(b, 1, 1 << 20, f) : 0;
    fclose(f);
    if (b)
        corpus_add(b, n);
    free(b);
    return 0;
}

/* A fuzz run seeds from its earlier output as well; a crash-* input there is replayed by `replay`, not mutated. */
static bool skip_crashes;

static void load_path(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "fuzz_frames: %s: %s\n", path, strerror(errno));
        exit(2);
    }
    if (!S_ISDIR(st.st_mode)) {
        load_file(path);
        return;
    }
    struct dirent **list;
    int n = scandir(path, &list, NULL, alphasort);
    for (int i = 0; i < n; i++) {
        if (list[i]->d_name[0] != '.' && !(skip_crashes && !strncmp(list[i]->d_name, "crash-", 6))) {
            char p[4200];
            snprintf(p, sizeof p, "%s/%s", path, list[i]->d_name);
            if (stat(p, &st) == 0 && S_ISREG(st.st_mode))
                load_file(p);
        }
        free(list[i]);
    }
    free(list);
}

/* ------------------------------------------------------------ mutations */

static uint64_t rng_state = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}
static size_t below(size_t n) { return n ? (size_t)(rnd() % n) : 0; }

#define FZ_MAX_INPUT (64u << 10)

/* The frames of an input: offsets of their payloads (after the 9-byte header). Bytes after the last whole
 * frame are a tail. */
typedef struct fr {
    size_t at, len; /* payload */
} fr;

static size_t split(const uint8_t *p, size_t len, fr *out, size_t max)
{
    size_t n = 0, off = 0;
    while (n < max && len - off >= 9) {
        uint32_t flen;
        memcpy(&flen, p + off, 4);
        if (flen < 5 || len - off - 4 < flen)
            break;
        out[n++] = (fr){off + 9, flen - 5};
        off += 4 + flen;
    }
    return n;
}

static const int64_t interesting[] = {0,          1,          -1,         2,          3,          7,         8,
                                      16,         64,         255,        256,        4096,       65535,     65536,
                                      0x7fff,     0x8000,     0x7fffffff, 0x80000000, 0xffffffff, 0x100000000ll,
                                      INT64_MAX,  INT64_MIN,  INT64_MAX - 1, INT64_MIN + 1, 86400000, 3600000,
                                      (int64_t)TSDB_MAX_FRAME, (int64_t)TSDB_MAX_FRAME / 8, (int64_t)1 << 40};

/* Writes the length of the frame whose payload starts at `at` (n bytes). */
static void set_len(uint8_t *p, size_t at, size_t n)
{
    uint32_t l = (uint32_t)(n + 5);
    memcpy(p + at - 9, &l, 4);
}

/* Returns the new length of p (capacity FZ_MAX_INPUT). */
static size_t mutate(uint8_t *p, size_t len)
{
    fr frs[256];
    size_t nf = split(p, len, frs, 256);
    int rounds = 1 + (int)below(4);
    for (int k = 0; k < rounds; k++) {
        unsigned what = (unsigned)below(nf ? 14 : 3);
        if (what < 3 || !nf) { /* havoc on raw bytes, framing included */
            if (!len)
                return len;
            size_t i = below(len);
            if (what == 0)
                p[i] ^= (uint8_t)(1u << below(8));
            else if (what == 1)
                p[i] = (uint8_t)rnd();
            else if (len > 1)
                len = 1 + below(len); /* truncate */
            nf = split(p, len, frs, 256);
            continue;
        }
        fr *f = &frs[below(nf)];
        switch (what) {
        case 3:
        case 4: /* a byte of a payload */
            if (f->len)
                p[f->at + below(f->len)] ^= (uint8_t)(1u << below(8));
            break;
        case 5:
        case 6:
        case 7: { /* an interesting value at a field position (4- or 8-byte, aligned to the layouts' fields) */
            int64_t v = interesting[below(sizeof interesting / sizeof *interesting)];
            if (rnd() & 1)
                v = (int64_t)((uint64_t)v + below(3) - 1);
            size_t w = rnd() & 1 ? 8 : rnd() & 1 ? 4 : rnd() & 1 ? 2 : 1;
            if (f->len >= w) {
                size_t at = f->at + below(f->len - w + 1);
                memcpy(p + at, &v, w);
            }
            break;
        }
        case 8: /* the op */
            p[f->at - 1] = (uint8_t)below(32);
            break;
        case 9: { /* grow or shrink a payload, keeping its length right */
            size_t end = f->at + f->len;
            if (rnd() & 1 && f->len) {
                size_t cut = 1 + below(f->len < 16 ? f->len : 16);
                memmove(p + end - cut, p + end, len - end);
                len -= cut;
                set_len(p, f->at, f->len - cut);
            } else {
                size_t add = 1 + below(rnd() & 1 ? 8 : 64);
                if (len + add > FZ_MAX_INPUT)
                    break;
                memmove(p + end + add, p + end, len - end);
                for (size_t i = 0; i < add; i++)
                    p[end + i] = rnd() & 1 ? 0 : (uint8_t)rnd();
                len += add;
                set_len(p, f->at, f->len + add);
            }
            nf = split(p, len, frs, 256);
            break;
        }
        case 10: { /* duplicate a frame */
            size_t fl = 9 + f->len;
            if (len + fl > FZ_MAX_INPUT)
                break;
            memcpy(p + len, p + f->at - 9, fl);
            len += fl;
            nf = split(p, len, frs, 256);
            break;
        }
        case 11: { /* append a frame of another input */
            input *o = &corpus[below(ncorpus)];
            fr of[64];
            size_t on = split(o->p, o->len, of, 64);
            if (!on)
                break;
            fr *g = &of[below(on)];
            if (len + 9 + g->len > FZ_MAX_INPUT)
                break;
            memcpy(p + len, o->p + g->at - 9, 9 + g->len);
            len += 9 + g->len;
            nf = split(p, len, frs, 256);
            break;
        }
        case 12: /* a payload's bytes from another frame of the same op, spliced */
        case 13: { /* the count fields: n consistent or not with the payload */
            if (f->len >= 4) {
                uint32_t n = (uint32_t)below(f->len / 4 + 2);
                if (what == 13 && rnd() & 1)
                    n = (uint32_t)rnd();
                size_t at = f->at + (below(4) == 0 ? below(f->len - 3) : 0);
                memcpy(p + at, &n, 4);
            }
            break;
        }
        }
    }
    return len;
}

/* ----------------------------------------------------------------- main */

static void setup(const char *dir)
{
    if (dir)
        snprintf(db_dir, sizeof db_dir, "%s", dir);
    else
        snprintf(db_dir, sizeof db_dir, "/tmp/fuzz_frames.%d", getpid());
    mkdir(db_dir, 0755);
    if (chdir(db_dir) != 0) { /* relative volume paths land in it */
        perror("fuzz_frames: chdir");
        exit(2);
    }
    signal(SIGALRM, on_fatal);
    signal(SIGSEGV, on_fatal);
    signal(SIGABRT, on_fatal);
    signal(SIGPIPE, SIG_IGN);
    if (__sanitizer_set_death_callback)
        __sanitizer_set_death_callback(fail_dump);
    send_timeout_s = 1;
    db_start();
}

int main(int argc, char **argv)
{
    if (argc >= 3 && !strcmp(argv[1], "seeds"))
        return cmd_seeds(argv[2]);
    if (argc < 3 || (strcmp(argv[1], "replay") && strcmp(argv[1], "fuzz"))) {
        fprintf(stderr, "usage: fuzz_frames seeds DIR | replay [--dir D] PATH... | fuzz [--dir D] [--seconds N] "
                        "[--out DIR] [--seed S] CORPUS...\n");
        return 2;
    }
    bool fuzz = !strcmp(argv[1], "fuzz");
    const char *dir = NULL, *out = NULL;
    long seconds = 60;
    int i = 2;
    for (; i < argc && argv[i][0] == '-'; i++) {
        if (i + 1 >= argc)
            break;
        if (!strcmp(argv[i], "--dir"))
            dir = argv[++i];
        else if (!strcmp(argv[i], "--out"))
            out = argv[++i];
        else if (!strcmp(argv[i], "--seconds"))
            seconds = strtol(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--seed"))
            rng_state = strtoull(argv[++i], NULL, 0) | 1;
        else
            break;
    }
    skip_crashes = fuzz;
    for (; i < argc; i++)
        load_path(argv[i]);
    if (!ncorpus) {
        fprintf(stderr, "fuzz_frames: no inputs\n");
        return 2;
    }
    if (out) {
        mkdir(out, 0755);
        static char abs_out[4096];
        if (!realpath(out, abs_out)) {
            perror("fuzz_frames: --out");
            return 2;
        }
        crash_dir = abs_out;
    }
    char cwd[4096];
    if (!getcwd(cwd, sizeof cwd))
        return 2;
    setup(dir);
    if (!fuzz) {
        verbose = getenv("FUZZ_VERBOSE") != NULL;
        for (size_t k = 0; k < ncorpus; k++)
            timed_run(corpus[k].p, corpus[k].len);
        printf("fuzz_frames: %zu inputs replayed\n", ncorpus);
        db_stop();
        if (!dir)
            rm_rf(db_dir);
        return 0;
    }
    cov_on = 1;
    for (size_t k = 0; k < ncorpus; k++) {
        memset(cov_map, 0, sizeof cov_map);
        timed_run(corpus[k].p, corpus[k].len);
        cov_new();
    }
    printf("fuzz_frames: %zu seeds, %zu edges\n", ncorpus, cov_edges());
    uint8_t *buf = malloc(FZ_MAX_INPUT);
    time_t t0 = time(NULL), last = t0;
    uint64_t execs = 0, kept = 0;
    while (time(NULL) - t0 < seconds) {
        input *src = &corpus[below(ncorpus)];
        size_t len = src->len < FZ_MAX_INPUT ? src->len : FZ_MAX_INPUT;
        memcpy(buf, src->p, len);
        len = mutate(buf, len);
        memset(cov_map, 0, sizeof cov_map);
        timed_run(buf, len);
        execs++;
        if (cov_new()) {
            corpus_add(buf, len);
            kept++;
            if (out) {
                char p[4200];
                snprintf(p, sizeof p, "%s/in-%ld-%d-%06llu", crash_dir, (long)t0, getpid(), (unsigned long long)kept);
                int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
                if (fd >= 0) {
                    if (write(fd, buf, len) != (ssize_t)len)
                        perror("fuzz_frames: write");
                    close(fd);
                }
            }
        }
        if (execs % 5000 == 0) { /* the database grows with what inputs insert: start again now and then */
            db_stop();
            db_start();
        }
        if (time(NULL) - last >= 30) {
            last = time(NULL);
            printf("fuzz_frames: %llu execs, %zu inputs, %zu edges, %lld s\n", (unsigned long long)execs, ncorpus,
                   cov_edges(), (long long)(last - t0));
            fflush(stdout);
        }
    }
    printf("fuzz_frames: done: %llu execs, %zu inputs (%llu new), %zu edges\n", (unsigned long long)execs, ncorpus,
           (unsigned long long)kept, cov_edges());
    free(buf);
    db_stop();
    if (!dir)
        rm_rf(db_dir);
    (void)cwd;
    return 0;
}
