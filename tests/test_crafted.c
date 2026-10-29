/*
 * Crafted files and extreme inputs: what someone with write access to the
 * data directory (or a shared NFS mount) can put there, and inputs at the
 * edges of the API. The checksums are no authentication (xxh64, unkeyed), so
 * every case here recomputes them where the file has one: the code must
 * refuse the content, not rely on the hash. Each case that crashed before its
 * fix runs in a child process, so a crash is a failure, not the end of the run.
 *
 *   ./build/test_crafted [scratch dir]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;

#define CHECK(cond)                                                                          \
    do {                                                                                     \
        if (!(cond)) {                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);         \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)
#define OK(expr)                                                                             \
    do {                                                                                     \
        int rc_ = (expr);                                                                    \
        if (rc_ != 0) {                                                                      \
            fprintf(stderr, "%s:%d: %s returned %d\n", __FILE__, __LINE__, #expr, rc_);      \
            exit(1);                                                                         \
        }                                                                                    \
    } while (0)

static int rm_one(const char *p, const struct stat *s, int flag, struct FTW *f)
{
    (void)s, (void)flag, (void)f;
    return remove(p);
}
static void rm_rf(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

static char *slurp(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1);
    if (fread(buf, 1, (size_t)n, f) != (size_t)n) {
        free(buf);
        buf = NULL;
    }
    fclose(f);
    if (buf)
        buf[n] = '\0';
    *len = (size_t)n;
    return buf;
}

static void spit(const char *path, const void *buf, size_t len)
{
    FILE *f = fopen(path, "wb");
    if (!f || fwrite(buf, 1, len, f) != len) {
        fprintf(stderr, "cannot write %s\n", path);
        exit(1);
    }
    fclose(f);
}

static bool find_part(const char *dir, char *name, size_t len)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    bool found = false;
    while (d && (e = readdir(d)))
        if (e->d_name[0] == 'p' && strstr(e->d_name, ".part") && !found) {
            snprintf(name, len, "%s", e->d_name);
            found = true;
        }
    if (d)
        closedir(d);
    return found;
}

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64;
    o.fsync = 0;
    o.merge_threads = 0;
    o.flush_interval_ms = 1000000;
    return o;
}

/* Runs fn in a child: true when it exited 0 (a crash or a failed CHECK there is false). */
static bool in_child(void (*fn)(const char *), const char *arg)
{
    fflush(stderr);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60); /* a loop that never ends fails too */
        failures = 0;
        fn(arg);
        _exit(failures ? 1 : 0);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid)
        return false;
    if (WIFSIGNALED(st))
        fprintf(stderr, "  child died of signal %d\n", WTERMSIG(st));
    return WIFEXITED(st) && WEXITSTATUS(st) == 0;
}

/* A database with one series of 1000 rows in one part (and the MANIFEST listing it). */
static void make_db(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "s1", &id));
    int64_t ts[1000];
    float v[1000];
    for (int i = 0; i < 1000; i++) {
        ts[i] = 1700000000000LL + i * 1000;
        v[i] = (float)i;
    }
    OK(tsdb_insert_series(db, id, ts, v, 1000));
    OK(tsdb_flush(db));
    OK(tsdb_close(db));
}

/* ------------------------------------------------------------ MANIFEST */

/* MANIFEST with its "parts" line replaced and `extra` part lines added, checksum recomputed. */
static void craft_manifest(const char *dir, const char *parts_line, int extra, bool twice)
{
    char path[512];
    snprintf(path, sizeof path, "%s/MANIFEST", dir);
    size_t len;
    char *text = slurp(path, &len);
    CHECK(text != NULL);
    if (!text)
        return;
    size_t cap = len + 64 + (size_t)extra * 32;
    char *out = malloc(cap);
    size_t n = 0;
    for (char *line = text, *next; line && *line; line = next) {
        next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        if (!strncmp(line, "checksum ", 9))
            break;
        if (twice && !strncmp(line, "wal_active ", 11))
            continue; /* one header line fewer: the second count is read as a header too, not as a part name */
        if (!strncmp(line, "parts ", 6)) {
            n += (size_t)snprintf(out + n, cap - n, "%s\n", parts_line);
            if (twice)
                n += (size_t)snprintf(out + n, cap - n, "%s\n", parts_line);
            continue;
        }
        n += (size_t)snprintf(out + n, cap - n, "%s\n", line);
    }
    for (int i = 0; i < extra; i++)
        n += (size_t)snprintf(out + n, cap - n, "p0_%d_%d_0.part\n", i + 100, i + 100);
    n += (size_t)snprintf(out + n, cap - n, "checksum %016llx\n", (unsigned long long)xxh64(out, n, 0));
    spit(path, out, n);
    free(out);
    free(text);
}

static void open_follower_refused(const char *dir)
{
    tsdb_options o = opts();
    o.read_only = 1;
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    CHECK(rc == TSDB_ERR_CORRUPT);
    if (rc == 0)
        tsdb_close(db);
}

static void test_manifest(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_manifest", base);
    /* SIZE_MAX parts: calloc(SIZE_MAX + 1) was calloc(0), then a pointer per line was stored past it */
    make_db(dir);
    craft_manifest(dir, "parts 18446744073709551615", 2000, false);
    CHECK(in_child(open_follower_refused, dir));
    make_db(dir);
    craft_manifest(dir, "parts -1", 2000, false); /* %zu takes a sign */
    CHECK(in_child(open_follower_refused, dir));
    /* a second count would allocate again over the first array */
    make_db(dir);
    craft_manifest(dir, "parts 1", 0, true);
    CHECK(in_child(open_follower_refused, dir));
    rm_rf(dir);
}

/* --------------------------------------------------------------- parts */

static char part_dir[512];

static int count_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts, (void)v;
    *(long *)ctx += (long)n;
    return 0;
}

static void query_rows(const char *dir, int *rc, long *rows)
{
    tsdb_options o = opts();
    tsdb *db;
    uint32_t id;
    *rc = tsdb_open(dir, &o, &db);
    *rows = -1;
    if (*rc)
        return;
    OK(tsdb_series_lookup(db, "s1", &id));
    long n = 0;
    *rc = tsdb_query_raw(db, id, INT64_MIN, INT64_MAX, count_cb, &n);
    if (*rc == 0)
        *rows = n;
    tsdb_close(db);
}

static void open_refused(const char *dir)
{
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == TSDB_ERR_CORRUPT);
}

static void query_refused(const char *dir)
{
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == TSDB_ERR_CORRUPT);
}

/* The entries of the part's index (in buf, len bytes), decoded into a malloc'd array of f->ngranules. */
static granule *index_load(const uint8_t *buf, size_t len, part_footer *f)
{
    memcpy(f, buf + len - sizeof *f, sizeof *f);
    part pp;
    memset(&pp, 0, sizeof pp);
    granule *g = calloc(f->ngranules + 1, sizeof *g);
    if (!g || gix_parse(buf + f->index_off, (size_t)f->index_len, f->ngranules, &pp.ix) != 0) {
        free(g);
        return NULL;
    }
    pp.ng = f->ngranules;
    for (uint32_t i = 0; i < f->ngranules; i++)
        gx_load(&pp, i, GW_ALL, &g[i]);
    return g;
}

static void rehash_part(const char *path);

/* The part at path with its index replaced by g[0..n) (encoded as the writer does), the hash recomputed. */
static void index_store(const char *path, const granule *g, size_t n)
{
    size_t len;
    uint8_t *buf = (uint8_t *)slurp(path, &len);
    part_footer f;
    memcpy(&f, buf + len - sizeof f, sizeof f);
    uint8_t *ix;
    size_t ilen;
    CHECK(gix_encode(g, n, &ix, &ilen) == 0);
    size_t tail = len - (size_t)(f.index_off + f.index_len); /* trailer and footer */
    size_t nlen = (size_t)f.index_off + ilen + tail;
    uint8_t *out = malloc(nlen);
    memcpy(out, buf, (size_t)f.index_off);
    memcpy(out + f.index_off, ix, ilen);
    memcpy(out + f.index_off + ilen, buf + f.index_off + f.index_len, tail);
    f.index_len = ilen;
    f.ngranules = (uint32_t)n;
    memcpy(out + nlen - sizeof f, &f, sizeof f);
    spit(path, out, nlen);
    free(out);
    free(ix);
    free(buf);
    rehash_part(path);
}

/* The footer and index of the part, read and written back with a recomputed hash. */
static void rehash_part(const char *path)
{
    size_t len;
    char *buf = slurp(path, &len);
    part_footer f;
    memcpy(&f, buf + len - sizeof f, sizeof f);
    uint64_t index_end = f.index_off + f.index_len;
    uint64_t x = xxh64(buf, sizeof(part_header), 0);
    x = xxh64(buf + f.index_off, (size_t)f.index_len, x);
    size_t tlen = len - sizeof f - (size_t)index_end;
    if (tlen)
        x = xxh64(buf + index_end, tlen, x);
    f.index_hash = xxh64(&f, offsetof(part_footer, index_hash), x);
    memcpy(buf + len - sizeof f, &f, sizeof f);
    spit(path, buf, len);
    free(buf);
}

/* a second part in the crafted part's partition, after its rows, then a merge of the two */
static void merge_refused(const char *dir)
{
    tsdb_options o = opts();
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_lookup(db, "s1", &id));
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++) {
        ts[i] = 1700000000000LL + 2000000 + i * 1000;
        v[i] = (float)i;
    }
    OK(tsdb_insert_series(db, id, ts, v, 100));
    OK(tsdb_flush(db));
    CHECK(tsdb_optimize(db) == TSDB_ERR_CORRUPT); /* was TSDB_ERR_IO: write(2) from the unmapped address */
    tsdb_close(db);
}

/* One series of 40 rows in granules of 16, 16 and 8 rows (one part). */
static void make_db_3_granules(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = opts();
    o.granule_rows = 16;
    o.wal_max_batch_rows = 4; /* straight to a part */
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "s1", &id));
    int64_t ts[40];
    float v[40];
    for (int i = 0; i < 40; i++) {
        ts[i] = 1700000000000LL + i * 1000;
        v[i] = (float)(i * 3 % 17);
    }
    OK(tsdb_insert_series(db, id, ts, v, 40));
    OK(tsdb_close(db));
}

/* a crafted index may be refused or answered, but never overflow a buffer (ASan) */
static void query_safe(const char *dir)
{
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == 0 || rc == TSDB_ERR_CORRUPT);
}

static void test_parts(const char *base)
{
    snprintf(part_dir, sizeof part_dir, "%s/crafted_part", base);
    char name[256], path[1024];

    /* index_off near 2^64: index_end wrapped to 0, the check passed, and the hash read before the mapping
       (no hash needs forging: it crashed before the comparison) */
    make_db(part_dir);
    CHECK(find_part(part_dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", part_dir, name);
    size_t len;
    char *buf = slurp(path, &len);
    part_footer f;
    memcpy(&f, buf + len - sizeof f, sizeof f);
    f.index_len = 1ull << 32;
    f.index_off = (uint64_t)0 - f.index_len;
    memcpy(buf + len - sizeof f, &f, sizeof f);
    spit(path, buf, len);
    free(buf);
    CHECK(in_child(open_refused, part_dir));

    /* a granule's off + length wrapping below index_off, the index hash recomputed: the query read 8 GB before
       the mapping */
    make_db(part_dir);
    CHECK(find_part(part_dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", part_dir, name);
    buf = slurp(path, &len);
    granule *gi = index_load((const uint8_t *)buf, len, &f);
    CHECK(gi != NULL);
    free(buf);
    if (gi) {
        gi[0].ts_len = gi[0].val_len = 0xffffffffu;
        gi[0].off = (uint64_t)0 - ((1ull << 33) - 2) + 40;
        index_store(path, gi, f.ngranules);
        free(gi);
    }
    CHECK(in_child(query_refused, part_dir));
    /* the same granule in a merge, which copies whole granules without decoding them: it copied from 8 GB before
       the mapping (a shorter one goes through memcpy) */
    CHECK(in_child(merge_refused, part_dir));

    /* the 8-row granule first in the index (entries 0 and 2 swapped, hash recomputed): the raw query sizes its decode
       buffers by the largest granule it reads; sized by the first, it decoded 16 rows into room for 8 */
    make_db_3_granules(part_dir);
    CHECK(find_part(part_dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", part_dir, name);
    buf = slurp(path, &len);
    granule *gs = index_load((const uint8_t *)buf, len, &f);
    free(buf);
    CHECK(gs && f.ngranules == 3);
    if (gs && f.ngranules == 3) {
        CHECK(gs[0].count == 16 && gs[1].count == 16 && gs[2].count == 8);
        granule t = gs[0];
        gs[0] = gs[2];
        gs[2] = t;
        index_store(path, gs, 3);
    }
    free(gs);
    CHECK(in_child(query_safe, part_dir));
    rm_rf(part_dir);
}

/* ------------------------------------------------------------- intents */

static void write_intent(const char *dir, const char *lines, int count)
{
    char text[1024], path[512];
    int n = snprintf(text, sizeof text, "tsdb-intent 3 %d\n%s", count, lines);
    n += snprintf(text + n, sizeof text - (size_t)n, "end %016llx\n",
                  (unsigned long long)xxh64(text, (size_t)n, 0));
    snprintf(path, sizeof path, "%s/txn_99_99.intent", dir); /* a name parts_commit writes: others are not read */
    spit(path, text, (size_t)n);
}

/* An intent of two renames (the fewest a writer makes) in a database from make_db: the crafted one, whose tmp
   side exists, and one already done (the database's own part), so recovery rolls forward rather than back.
   Afterwards the database must still open with its rows, and a `fin` that did not exist must not appear. */
static void one_intent(const char *dir, const char *tmp, const char *fin)
{
    char path[1024], line[600];
    snprintf(path, sizeof path, "%s/%s", dir, fin);
    bool existed = access(path, F_OK) == 0;
    if (strchr(tmp, '/') == NULL) {
        snprintf(path, sizeof path, "%s/%s", dir, tmp);
        spit(path, "garbage", 7);
    }
    char part[256];
    CHECK(find_part(dir, part, sizeof part));
    snprintf(line, sizeof line, "%s %s\ntmp_7_7.part %s\n", tmp, fin, part);
    write_intent(dir, line, 2);
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == 0 && rows == 1000);
    snprintf(path, sizeof path, "%s/%s", dir, fin);
    CHECK(existed || access(path, F_OK) != 0);
}

/* --------------------------------------------------------------- the packed granule index */

/* the offset of field f's descriptor (its width byte) in index b; GF_N: the end of the descriptors */
static size_t desc_at(const uint8_t *b, int f)
{
    size_t pos = 0;
    while (b[pos++] & 0x80)
        ;
    for (int k = 0; k < f; k++) {
        uint8_t sk = b[pos + 1];
        pos += 2;
        while (b[pos++] & 0x80)
            ;
        if (sk & 128)
            while (b[pos++] & 0x80)
                ;
    }
    return pos;
}

static int ix_loads; /* indexes accepted and read entry by entry */

/* gix_parse of ix[0..len) for n entries, from a buffer of exactly len + 8 bytes (ASan sees anything beyond);
   when it is accepted, every entry is read through every accessor */
static int parse_copy(const uint8_t *ix, size_t len, uint32_t n)
{
    uint8_t *b = malloc(len + 8);
    memcpy(b, ix, len);
    memset(b + len, 0x5a, 8);
    part pp;
    memset(&pp, 0, sizeof pp);
    int rc = gix_parse(b, len, n, &pp.ix);
    if (rc == 0) {
        ix_loads++;
        pp.ng = n;
        for (uint32_t i = 0; i < n; i++) {
            granule g;
            gx_load(&pp, i, GW_ALL, &g);
            uint32_t a, e;
            gix_run(&pp.ix, gx_series(&pp, i), &a, &e);
            CHECK(a <= i && i < e && e <= n);
        }
    } else {
        CHECK(rc == TSDB_ERR_CORRUPT);
    }
    free(b);
    return rc;
}

/* the index of n entries over 3 series of float32 rows, as a part writer would make it */
static uint8_t *ix_build(uint32_t n, size_t *len)
{
    granule *g = calloc(n, sizeof *g);
    float v[64];
    int64_t ts[64];
    uint64_t off = sizeof(part_header);
    for (uint32_t k = 0; k < n; k++) {
        for (int i = 0; i < 64; i++) {
            ts[i] = 1700000000000LL + (int64_t)(k * 64 + (uint32_t)i) * 1000 + (i * 7919 % 13);
            v[i] = (float)((k * 31 + (uint32_t)i * 17) % 101) / 4;
        }
        granule_stats(TSDB_FLOAT32, v, 64, &g[k]);
        granule_twa(TSDB_FLOAT32, ts, v, 64, &g[k]);
        g[k].series = 3 + 5 * (k * 3 / n);
        g[k].count = 64;
        g[k].ts_min = ts[0];
        g[k].ts_max = ts[63];
        g[k].off = off;
        g[k].ts_len = 40 + k % 7;
        g[k].val_len = 90 + k * 13 % 29;
        g[k].crc = 0x9e3779b9u * (k + 1);
        off += g[k].ts_len + g[k].val_len;
    }
    uint8_t *ix = NULL;
    OK(gix_encode(g, n, &ix, len));
    free(g);
    return ix;
}

/* A damaged index whose hash was recomputed: every inconsistency is TSDB_ERR_CORRUPT, and an index that parses
   (mutated entry bits are just other values) never makes a reader load outside it plus the 8 bytes after it. */
static void test_index_damage(const char *base)
{
    static const uint32_t ns[] = {1, 2, 3, 5, 15, 40};
    int padded = 0;
    for (size_t t = 0; t < sizeof ns / sizeof *ns; t++) {
        uint32_t n = ns[t];
        size_t len;
        uint8_t *ix = ix_build(n, &len);
        uint8_t *m = malloc(len + 16);
        CHECK(parse_copy(ix, len, n) == 0);
        /* lengths: not a multiple of 8, cut short at every 8 bytes, 8 bytes of zeros more, another count */
        CHECK(parse_copy(ix, len - 4, n) == TSDB_ERR_CORRUPT);
        for (size_t k = 0; k < len; k += 8)
            CHECK(parse_copy(ix, k, n) == TSDB_ERR_CORRUPT);
        memcpy(m, ix, len);
        memset(m + len, 0, 8);
        CHECK(parse_copy(m, len + 8, n) == TSDB_ERR_CORRUPT);
        CHECK(parse_copy(ix, len, n - 1) == TSDB_ERR_CORRUPT);
        (void)parse_copy(ix, len, n + 1); /* fits only where the zero padding holds a whole entry */
        CHECK(parse_copy(ix, len, 0) == TSDB_ERR_CORRUPT);
        /* nruns: more than n, 0 */
        memcpy(m, ix, len);
        uint8_t nr = m[0];
        m[0] = (uint8_t)(n + 1);
        if (nr < 0x80 && n + 1 < 0x80)
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        m[0] = 0;
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        /* field descriptors */
        size_t dc = desc_at(ix, GF_COUNT), dm = desc_at(ix, GF_MIN), dh = desc_at(ix, GF_ISUMHI);
        memcpy(m, ix, len);
        m[dc] = 65; /* wider than 64 bits */
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        memcpy(m, ix, len);
        m[dc] = 64, m[dc + 1] = (uint8_t)((m[dc + 1] & ~63) | 1); /* shift + width beyond 64 */
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        memcpy(m, ix, len);
        m[dc + 1] |= 64; /* a float32 kind on an integer field */
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        memcpy(m, ix, len);
        m[dm + 1] |= 64 | 128; /* a float32 field with a slope */
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        memcpy(m, ix, len);
        m[dm + 1] = (uint8_t)((m[dm + 1] & ~63) | 64 | 20), m[dm] = 13; /* a float32 field beyond 32 bits */
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        CHECK(ix[dh] == 0); /* float32 rows: the upper word of the sums takes no bits */
        memcpy(m, ix, len);
        m[dh + 1] = (uint8_t)((m[dh + 1] & ~63) | 3); /* a shift without bits */
        CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        for (int f = 0; f < GF_N; f++) { /* every width one more and one less: refused or read in bounds */
            size_t d = desc_at(ix, f);
            for (int delta = -1; delta <= 1; delta += 2) {
                memcpy(m, ix, len);
                m[d] = (uint8_t)(m[d] + delta);
                (void)parse_copy(m, len, n);
            }
        }
        /* the runs: out of order, repeated, not starting at entry 0, a start beyond the entries */
        size_t r = (desc_at(ix, GF_N) + 3) & ~(size_t)3;
        uint32_t ser[3], st[3];
        memcpy(ser, ix + r, 4 * MIN(n, 3));
        if (n >= 3) {
            CHECK(ix[0] == 3);
            memcpy(st, ix + r + 12, 12);
            memcpy(m, ix, len);
            memcpy(m + r, &ser[1], 4), memcpy(m + r + 4, &ser[0], 4);
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
            memcpy(m, ix, len);
            memcpy(m + r + 4, &ser[0], 4);
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
            memcpy(m, ix, len);
            uint32_t one = 1;
            memcpy(m + r + 12, &one, 4);
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
            memcpy(m, ix, len);
            memcpy(m + r + 20, &n, 4);
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
            memcpy(m, ix, len);
            memcpy(m + r + 16, &st[2], 4); /* two runs starting at the same entry */
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
        }
        /* padding: between the descriptors and the runs, and after the entries */
        for (size_t p = desc_at(ix, GF_N); p < r; p++) {
            memcpy(m, ix, len);
            m[p] = 1;
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
            padded++;
        }
        part pp;
        memset(&pp, 0, sizeof pp);
        memcpy(m, ix, len);
        OK(gix_parse(m, len, n, &pp.ix));
        size_t end = (size_t)(pp.ix.e - m) + (size_t)n * pp.ix.stride;
        for (size_t p = end; p < len; p++) {
            memcpy(m, ix, len);
            m[p] = 0x80;
            CHECK(parse_copy(m, len, n) == TSDB_ERR_CORRUPT);
            padded++;
        }
        /* every byte before the entries, flipped in a few ways: refused, or read in bounds */
        static const uint8_t x[] = {0x01, 0x02, 0x10, 0x40, 0x80, 0xff};
        for (size_t p = 0; p < (size_t)(pp.ix.e - m); p++)
            for (size_t k = 0; k < sizeof x; k++) {
                memcpy(m, ix, len);
                m[p] ^= x[k];
                (void)parse_copy(m, len, n);
            }
        free(m);
        free(ix);
    }
    CHECK(padded > 0);
    CHECK(ix_loads > 0);

    /* the same in a part on disk, its hash recomputed: the open refuses it */
    snprintf(part_dir, sizeof part_dir, "%s/crafted_index", base);
    char name[256], path[1024];
    for (int k = 0; k < 6; k++) {
        make_db(part_dir);
        CHECK(find_part(part_dir, name, sizeof name));
        snprintf(path, sizeof path, "%s/%s", part_dir, name);
        size_t len;
        uint8_t *buf = (uint8_t *)slurp(path, &len);
        part_footer f;
        memcpy(&f, buf + len - sizeof f, sizeof f);
        uint8_t *ix = buf + f.index_off;
        switch (k) {
        case 0: f.index_len -= 8; break;                      /* the index's last 8 bytes taken as a trailer */
        case 1: f.index_off -= 8, f.index_len += 8; break;    /* starting 8 bytes early, in the last block */
        case 2: f.index_off += 4, f.index_len -= 8; break;    /* not 8-byte aligned */
        case 3: f.ngranules += 1; break;                      /* an entry more than the index holds */
        case 4: ix[desc_at(ix, GF_OFF)] = 65; break;          /* a field wider than 64 bits */
        default: f.index_len = 0, f.ngranules = 0; break;     /* no index at all, the bytes as a trailer */
        }
        memcpy(buf + len - sizeof f, &f, sizeof f);
        spit(path, buf, len);
        free(buf);
        rehash_part(path);
        if (!in_child(open_refused, part_dir)) {
            fprintf(stderr, "crafted index case %d was not refused cleanly\n", k);
            failures++;
        }
    }
    rm_rf(part_dir);
}

static void test_intents(const char *base)
{
    char dir[512], path[1024];
    snprintf(dir, sizeof dir, "%s/crafted_intent", base);

    /* an intent naming SERIES: garbage was renamed over it (CATALOG still knows the names, so only the bytes
       show it) */
    make_db(dir);
    snprintf(path, sizeof path, "%s/SERIES", dir);
    size_t n0, n1;
    char *before = slurp(path, &n0);
    one_intent(dir, "tmp_x", "SERIES");
    char *after = slurp(path, &n1);
    CHECK(before && after && n0 == n1 && memcmp(before, after, n0) == 0);
    free(before);
    free(after);

    /* any other file of the directory: a name that is no part */
    make_db(dir);
    snprintf(path, sizeof path, "%s/victim", dir);
    spit(path, "precious", 8);
    one_intent(dir, "tmp_x", "victim");
    after = slurp(path, &n1);
    CHECK(after && n1 == 8 && memcmp(after, "precious", 8) == 0);
    free(after);

    /* MANIFEST: the open must neither fail nor lose the part */
    make_db(dir);
    one_intent(dir, "tmp_1_2.part", "MANIFEST");

    /* names a writer never makes: something after ".part" on either side */
    make_db(dir);
    one_intent(dir, "tmp_1_2.partX", "p0_900_900_0.part");
    make_db(dir);
    one_intent(dir, "tmp_1_2.part", "p0_901_901_0.part.x");

    /* a tmp side that leaves the directory */
    make_db(dir);
    char sub[600];
    snprintf(sub, sizeof sub, "%s/tmp_", dir);
    mkdir(sub, 0755);
    snprintf(path, sizeof path, "%s/outside", base);
    spit(path, "outside", 7);
    char part[256], lines[512];
    CHECK(find_part(dir, part, sizeof part));
    snprintf(lines, sizeof lines, "tmp_/../../outside p0_900_900_0.part\ntmp_7_7.part %s\n", part);
    write_intent(dir, lines, 2);
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == 0 && rows == 1000);
    CHECK(access(path, F_OK) == 0); /* not pulled in */
    snprintf(path, sizeof path, "%s/p0_900_900_0.part", dir);
    CHECK(access(path, F_OK) != 0);
    snprintf(path, sizeof path, "%s/outside", base);
    unlink(path);
    rm_rf(dir);
}

/* ---------------------------------------------------------- dictionary */

#define DICT_MAX_NAMES ((1u << 31) - 1) /* as in series.c */

static void dict_full(const char *dir)
{
    int dfd = open(dir, O_DIRECTORY | O_RDONLY);
    series_dict d;
    OK(series_open(&d, dfd, "SERIES", false, false));
    uint32_t id;
    OK(series_get(&d, "a", true, TSDB_FLOAT32, &id));
    OK(series_get(&d, "b", true, TSDB_FLOAT32, &id));
    struct stat before, after;
    fstat(d.fd, &before);
    /* as if 2^31 - 1 names were there: one more is refused before its record is written (the table of twice
       as many slots wrapped its uint32 size at 2^30 names, and a record written first made every later open
       fail on it) */
    uint32_t real = atomic_load(&d.count);
    atomic_store(&d.count, DICT_MAX_NAMES);
    CHECK(series_get(&d, "c", true, TSDB_FLOAT32, &id) == TSDB_ERR_INVALID);
    fstat(d.fd, &after);
    CHECK(after.st_size == before.st_size);
    atomic_store(&d.count, real);
    /* existing names still resolve, and the seed is not the old public constant */
    CHECK(series_get(&d, "b", false, 0, &id) == 0 && id == 1);
    series_dict d2;
    int dfd2 = open(dir, O_DIRECTORY | O_RDONLY);
    OK(series_open(&d2, dfd2, "SERIES", true, false));
#ifndef MAIN_COMPAT /* builds against the code before the fix, to show the other checks fail there */
    CHECK(d2.seed != d.seed); /* random per open: 2^-64 to collide */
#endif
    CHECK(series_get(&d2, "a", false, 0, &id) == 0 && id == 0);
    /* and the table is placed by it: "a", the first name, sits in its own slot */
    CHECK(d.table[xxh64("a", 1, d.seed) & (d.tcap - 1)] == 1);
    /* a file that holds more names than a create may make is damage: refused at load, never stored past the end */
    OK(series_get(&d, "c", true, TSDB_FLOAT32, &id));
    uint32_t real2 = atomic_load(&d2.count);
    atomic_store(&d2.count, DICT_MAX_NAMES);
    CHECK(series_reload(&d2, dfd2) == TSDB_ERR_CORRUPT);
    atomic_store(&d2.count, real2);
    series_close(&d2);
    series_close(&d);
    close(dfd2);
    close(dfd);
}

static void test_dict(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_dict", base);
    rm_rf(dir);
    mkdir(dir, 0755);
    CHECK(in_child(dict_full, dir));
    rm_rf(dir);
}

/* ------------------------------------------------- extreme timestamps */

static void test_extremes(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_extremes", base);
    rm_rf(dir);
    tsdb_options o = opts();
    o.wal_max_batch_rows = 4; /* 3 rows: WAL and memtable; 5: straight to parts */
    tsdb *db;
    uint32_t a, b;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "b", &b));
    float v[5] = {1, 2, 3, 4, 5};
    /* a row at INT64_MAX: no half-open range reaches it, so it could never be read, counted or deleted */
    int64_t small[3] = {INT64_MIN, 1000, INT64_MAX};
    CHECK(tsdb_insert_series(db, a, small, v, 3) == TSDB_ERR_INVALID);
    CHECK(tsdb_append(db, a, INT64_MAX, 1) == TSDB_ERR_INVALID);
    int64_t sorted[5] = {INT64_MIN, 1000, 2000, 3000, INT64_MAX};
    CHECK(tsdb_insert_series(db, a, sorted, v, 5) == TSDB_ERR_INVALID);
    int64_t unsorted[5] = {3000, INT64_MAX, 1000, INT64_MIN, 2000};
    CHECK(tsdb_insert_series(db, a, unsorted, v, 5) == TSDB_ERR_INVALID);
    uint32_t ser[5] = {b, a, b, a, a};
    CHECK(tsdb_insert(db, ser, unsorted, v, 5) == TSDB_ERR_INVALID);
    uint32_t ser_sorted[5] = {a, a, a, b, b};
    int64_t ts_sorted[5] = {1, 2, INT64_MAX, 1, 2};
    CHECK(tsdb_insert(db, ser_sorted, ts_sorted, v, 5) == TSDB_ERR_INVALID);
    /* each series in time order, all in the last partition: the counting sort (bucket_write) takes it */
    uint32_t ser_bucket[5] = {b, a, b, a, a};
    int64_t ts_bucket[5] = {INT64_MAX - 4, INT64_MAX - 3, INT64_MAX - 2, INT64_MAX - 1, INT64_MAX};
    CHECK(tsdb_insert(db, ser_bucket, ts_bucket, v, 5) == TSDB_ERR_INVALID);
    tsdb_bucket *bk;
    size_t nb;
    OK(tsdb_query_group(db, a, INT64_MIN, INT64_MAX, NULL, &bk, &nb));
    CHECK(nb == 0 || bk[0].count == 0); /* all or nothing: none of the refused rows */
    free(bk);

    /* the extremes that are allowed: local grouping over an open range used to fail (TSDB_ERR_INVALID) once
       one row lay beyond the calendar's 200,000 years */
    int64_t ok3[3] = {INT64_MIN, 1000, INT64_MAX - 5};
    OK(tsdb_insert_series(db, a, ok3, v, 3));
    int64_t ok5[5] = {INT64_MIN, -5000, 1000, 86400000LL * 3, INT64_MAX - 1};
    OK(tsdb_insert_series(db, b, ok5, v, 5));
    tsdb_group g = {TSDB_DAY, 1, "+01:00"};
    for (int s = 0; s < 2; s++) {
        uint32_t id = s ? b : a;
        uint64_t want = s ? 5 : 3, total = 0;
        int rc = tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &bk, &nb);
        CHECK(rc == 0);
        for (size_t i = 0; rc == 0 && i < nb; i++)
            total += bk[i].count;
        CHECK(total == want);
        CHECK(rc != 0 || (nb > 0 && bk[0].start == INT64_MIN));
        free(bk);
        tsdb_spread_bucket *sb;
        CHECK(tsdb_query_spread_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &sb, &nb) == 0);
        free(sb);
        tsdb_twa_bucket *tb;
        CHECK(tsdb_query_twa_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &tb, &nb) == 0);
        free(tb);
    }
    /* the inner rows still have their own days: INT64_MIN joins the first, INT64_MAX - 1 the last (+01:00) */
    OK(tsdb_query_group_local(db, b, INT64_MIN, INT64_MAX, &g, NULL, &bk, &nb));
    CHECK(nb >= 2 && bk[0].start == INT64_MIN && bk[0].count == 3);
    CHECK(nb >= 2 && bk[nb - 1].start == 86400000LL * 3 - 3600000 && bk[nb - 1].count == 2);
    free(bk);
    /* only rows beyond the limit, on one side */
    uint32_t c;
    OK(tsdb_series(db, "c", &c));
    int64_t far[2] = {INT64_MAX - 10, INT64_MAX - 5};
    OK(tsdb_insert_series(db, c, far, v, 2));
    CHECK(tsdb_query_group_local(db, c, INT64_MIN, INT64_MAX, &g, NULL, &bk, &nb) == 0);
    CHECK(nb == 1 && bk[0].count == 2);
    free(bk);
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* A WAL that holds a row at INT64_MAX, as a build before the refusal could write it (or a crafted segment):
   replay and the flush at close keep it, as that build did, instead of failing every open. */
static int count_slots(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    (void)ts, (void)v;
    *(long *)ctx += (long)n;
    return 0;
}

static void wal_unreachable_write(const char *dir)
{
    tsdb_options o = opts();
    eng *db;
    uint32_t id;
    OK(eng_open(dir, &o, &db));
    OK(eng_series(db, "a", TSDB_FLOAT32, &id));
    int64_t ts[2] = {INT64_MAX, 1000};
    float v[2] = {1, 2};
    OK(mem_insert(db, NULL, id, ts, v, sizeof *v, 2)); /* past write_rows, which refuses it */
    _exit(failures ? 1 : 0); /* no close: the rows stay in the WAL only */
}

static void wal_unreachable_open(const char *dir)
{
    tsdb_options o = opts();
    eng *db;
    uint32_t id;
    for (int round = 0; round < 2; round++) { /* replay, flush at close; then from the part */
        int rc = eng_open(dir, &o, &db);
        CHECK(rc == 0);
        if (rc)
            return;
        OK(eng_series_lookup(db, "a", &id));
        long n = 0;
        OK(eng_query_raw_ex(db, id, INT64_MIN, INT64_MAX, false, 0, count_slots, &n));
        CHECK(n == 1);
        CHECK(eng_close(db) == 0);
    }
}

static void test_wal_unreachable(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_wal_max", base);
    rm_rf(dir);
    CHECK(in_child(wal_unreachable_write, dir));
    CHECK(in_child(wal_unreachable_open, dir));
    rm_rf(dir);
}

/* A patas header naming a ring slot before the first value: the decoder read
 * out[i - 1 - (i - 1 - ref) % RING] with an index that wrapped below zero. */
static void patas_ring(const char *unused)
{
    (void)unused;
    uint8_t blk[6 + BLK_SLACK] = {0x01, 0x02, 0x03, 0x04};
    uint16_t h = 5 << 9; /* ref 5 for the second value, no bytes, no shift */
    memcpy(blk + 4, &h, 2);
    uint32_t out[2];
    CHECK(patas_decode(blk, 6, 2, out) == TSDB_ERR_CORRUPT);
    exit(failures ? 1 : 0);
}

static void test_codecs(void)
{
    CHECK(in_child(patas_ring, NULL));
}

/* The part writer refuses rows out of (series, ts) order and writes nothing: a series run going back in time
 * (what a placement that read two class histories once wrote), a repeated timestamp, a series before the last,
 * within a granule and across granules. Before, it wrote them: queries binary-search the runs. */
static void test_writer_order(const char *base)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/order", base);
    tsdb_options o = opts();
    o.granule_rows = 4;
    eng *db;
    OK(eng_open(dir, &o, &db));
    static const struct {
        uint32_t s1;
        int64_t a[3];
        uint32_t s2;
        int64_t b[3];
    } cases[] = {
        {1, {10, 20, 30}, 1, {5, 6, 7}},     /* back in time across calls */
        {1, {10, 20, 30}, 1, {30, 31, 32}},  /* a repeated ts */
        {2, {10, 20, 30}, 1, {40, 50, 60}},  /* a series before the last */
        {1, {10, 30, 20}, 2, {1, 2, 3}},     /* within a granule */
    };
    float v[3] = {1, 2, 3};
    for (size_t c = 0; c < sizeof cases / sizeof *cases; c++) {
        part_writer w;
        OK(pw_begin(&w, db, 0, 0));
        int rc = pw_add(&w, cases[c].s1, TSDB_FLOAT32, cases[c].a, v, 3);
        if (rc == 0) {
            rc = pw_add(&w, cases[c].s2, TSDB_FLOAT32, cases[c].b, v, 3);
            part *p = NULL;
            if (rc == 0)
                rc = pw_finish(&w, 1, 1, 0, &p);
            else
                pw_abort(&w);
            if (p)
                part_unref(p);
        } else {
            pw_abort(&w);
        }
        CHECK(rc == TSDB_ERR_CORRUPT);
    }
    part_writer w; /* and a good one still goes through */
    OK(pw_begin(&w, db, 0, 0));
    int64_t t1[3] = {10, 20, 30}, t2[3] = {40, 50, 60};
    OK(pw_add(&w, 1, TSDB_FLOAT32, t1, v, 3));
    OK(pw_add(&w, 1, TSDB_FLOAT32, t2, v, 3));
    pw_abort(&w);
    CHECK(eng_close(db) == 0);
}

/* Day cuts in a merge: a full granule of a class-1 partition that spans a day edge (written by a build with
 * another GRANULE_CUT, or before the cut) must be re-cut by the merge, never copied as it is. */
static void test_daycut_merge(const char *base)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/daycut", base);
    tsdb_options o = opts();
    o.granule_rows = 4;
    eng *db;
    OK(eng_open(dir, &o, &db));
    uint32_t s;
    OK(eng_series(db, "a", TSDB_FLOAT32, &s));
    int64_t from = INT64_MIN;
    OK(eng_class_switch(db, s, 1, &from));
    const int64_t D = db->partition_ms;
    float v[4] = {1, 2, 3, 4};
    part *src[2];
    int64_t ta[4] = {D - 2, D - 1, D + 1, D + 2}, tb[4] = {3 * D, 3 * D + 1, 3 * D + 2, 3 * D + 3};
    for (int k = 0; k < 2; k++) {
        part_writer w;
        OK(pw_begin(&w, db, 1 /* class 1, base partition 0 */, 0));
        CHECK(w.cut_ms == D);
        w.cut_ms = 0; /* as an older build wrote it */
        OK(pw_add(&w, s, TSDB_FLOAT32, k ? tb : ta, v, 4));
        OK(pw_finish_pending(&w, 0, 0, 1, &src[k]));
        OK(parts_publish(db, &src[k], 1));
        src[k]->level = 1; /* as if a merge wrote it: only merged granules are copied as they are */
    }
    CHECK(src[0]->ng == 1 && floor_div(gx_ts_min(src[0], 0), D) != floor_div(gx_ts_max(src[0], 0), D));
    part *out = NULL;
    OK(merge_parts(db, src, 2, &out));
    CHECK(out != NULL);
    int spans = 0;
    for (uint32_t i = 0; out && i < out->ng; i++)
        spans += floor_div(gx_ts_min(out, i), D) != floor_div(gx_ts_max(out, i), D);
    if (spans)
        fprintf(stderr, "daycut merge: %d granule(s) of the merge output span a day edge\n", spans);
    CHECK(spans == 0);
    if (out)
        parts_discard(db, &out, 1);
    CHECK(eng_close(db) == 0);
}

/* A switch while tsdb_drop_before's renames are not yet durable: the switch sees no rows of the series (their part
 * is no longer listed) and writes its class from the beginning of time, synced. A power loss before the drop's
 * directory fsync brings the part back under its old name. Emulated: the state such a power loss leaves. */
static void test_drop_switch(const char *base)
{
    char dir[600], a[900], b[900];
    snprintf(dir, sizeof dir, "%s/dropsw", base);
    tsdb_options o = opts();
    eng *db;
    OK(eng_open(dir, &o, &db));
    uint32_t s;
    OK(eng_series(db, "a", TSDB_FLOAT32, &s));
    size_t n = 70000;
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    for (size_t i = 0; i < n; i++)
        ts[i] = (int64_t)i, v[i] = (float)i; /* 1 kHz: class 0, all in base partition 0 */
    OK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, n));
    int c = -1;
    OK(eng_series_class(db, s, &c));
    CHECK(c == 0);
    CHECK(db->nparts == 1);
    char name[256];
    snprintf(name, sizeof name, "%s", db->parts[0]->name);
    snprintf(a, sizeof a, "%s/%s", dir, name);
    snprintf(b, sizeof b, "%s/keep_%s", dir, name);
    char cmd[2000];
    snprintf(cmd, sizeof cmd, "cp '%s' '%s'", a, b);
    CHECK(system(cmd) == 0);
    OK(eng_drop_before(db, db->partition_ms));
    /* in the window between the drop's renames and its directory fsync: what cls_observe would do */
    OK(eng_class_switch(db, s, 3, NULL));
    OK(eng_series_class(db, s, &c));
    CHECK(c == 3);
    CHECK(eng_close(db) == 0);
    /* the power loss: the rename to retired_ never reached the disk, CLASSES (synced) did */
    CHECK(rename(b, a) == 0);
    snprintf(cmd, sizeof cmd, "rm -f '%s/retired_%s'", dir, name);
    CHECK(system(cmd) == 0);
    int rc = eng_open(dir, &o, &db);
    if (rc)
        fprintf(stderr, "dropsw: the reopen after the emulated power loss failed: %d\n", rc);
    CHECK(rc == 0);
    if (rc == 0)
        CHECK(eng_close(db) == 0);
    free(ts);
    free(v);
}

/* The open check decodes a granule that spans a stay in another class and skips rows up to where the history
 * changes next: that relies on ascending rows. A damaged granule whose rows stay within its bounds but go back in
 * time (here 2D + 123457, then D + 5, a row of class 0 in a class-1 partition) must fail the open, not hide that row. */
static void test_check_order(const char *base)
{
    char dir[600], path[900], name[256];
    snprintf(dir, sizeof dir, "%s/chkorder", base);
    tsdb_options o = opts();
    o.granule_rows = 4;
    eng *db;
    OK(eng_open(dir, &o, &db));
    uint32_t s;
    OK(eng_series(db, "a", TSDB_FLOAT32, &s));
    const int64_t D = db->partition_ms;
    int64_t t0 = INT64_MIN, t1 = D, t2 = 2 * D;
    OK(eng_class_switch(db, s, 1, &t0)); /* class 1, class 0 for the second day, then class 1 again */
    OK(eng_class_switch(db, s, 0, &t1));
    OK(eng_class_switch(db, s, 1, &t2));
    float v[4] = {1, 2, 3, 4};
    int64_t ts[4] = {D - 2, 2 * D + 123457, 2 * D + 45678901, 6 * D + 1234567}; /* irregular: room for the damage */
    part_writer w;
    part *pp;
    OK(pw_begin(&w, db, 1 /* class 1, base partition 0 */, 0));
    w.cut_ms = 0; /* one granule across the stay in class 0, as a build without day cuts wrote it */
    OK(pw_add(&w, s, TSDB_FLOAT32, ts, v, 4));
    OK(pw_finish_pending(&w, 0, 0, 1, &pp));
    OK(parts_publish(db, &pp, 1));
    CHECK(eng_close(db) == 0);
    OK(eng_open(dir, &o, &db)); /* intact: the check decodes the granule and passes */
    CHECK(eng_close(db) == 0);
    /* the damage: the third row goes back into the second day */
    CHECK(find_part(dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", dir, name);
    size_t len;
    uint8_t *buf = (uint8_t *)slurp(path, &len);
    part_footer f;
    memcpy(&f, buf + len - sizeof f, sizeof f);
    granule *gp = index_load(buf, len, &f);
    CHECK(gp && f.ngranules == 1);
    granule g = gp ? gp[0] : (granule){0};
    free(gp);
    int64_t bad[4] = {D - 2, 2 * D + 123457, D + 5, 6 * D + 1234567};
    uint8_t enc[4096];
    codec_ctx *cc = codec_new();
    size_t tl = block_encode_ts(cc, 1, bad, 4, enc);
    codec_free(cc);
    if (!(tl > 0 && tl <= g.ts_len))
        fprintf(stderr, "chkorder: the damaged block takes %zu bytes, the intact one %u\n", tl, g.ts_len);
    CHECK(tl > 0 && tl <= g.ts_len);
    if (tl > 0 && tl <= g.ts_len) {
        memmove(buf + g.off + tl, buf + g.off + g.ts_len, g.val_len);
        memcpy(buf + g.off, enc, tl);
        g.ts_len = (uint32_t)tl;
        spit(path, buf, len);
        index_store(path, &g, 1);
        int rc = eng_open(dir, &o, &db);
        if (rc == 0) {
            fprintf(stderr, "chkorder: the open took a granule with rows out of order\n");
            eng_close(db);
        }
        CHECK(rc == TSDB_ERR_CORRUPT);
    }
    free(buf);
}

int main(int argc, char **argv)
{
    /* a directory of this run's own: the fixed names below collided between worktrees testing at once */
    char base[400];
    snprintf(base, sizeof base, "%s/tsdb-crafted-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rm_rf(base);
    if (mkdir(base, 0755) != 0) {
        perror(base);
        return 1;
    }
    test_manifest(base);
    test_parts(base);
    test_index_damage(base);
    test_intents(base);
    test_dict(base);
    test_extremes(base);
    test_wal_unreachable(base);
    test_writer_order(base);
    test_daycut_merge(base);
    test_drop_switch(base);
    test_check_order(base);
    test_codecs();
    if (failures) {
        fprintf(stderr, "test_crafted: %d failures\n", failures);
        return 1;
    }
    rm_rf(base);
    printf("test_crafted: ok\n");
    return 0;
}
