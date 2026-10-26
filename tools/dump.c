/* tsdb export and import: a database as one self-describing stream (docs/export-format.md has the layout).
 *
 * It exists so that data outlives the on-disk format: before 1.0 a 0.9.x release may change the format, and a
 * database is then carried over by exporting it with the old tsdb and importing it with the new one. It also
 * serves backups and moving a database to other disks or hosts. Both directions use the public API only
 * (include/tsdb.h), so any version's binary can write a stream and any later one can read it.
 *
 * Export reads each series with one tsdb_query_values call, so a series is exported from one snapshot even while
 * a writer (tsdbd) keeps writing, and it holds only one chunk of rows in memory, whatever the database size. What
 * goes out is what queries see: deletes and replaces are applied, rows past their retention are gone, the last
 * write of a key wins. Values travel bit for bit (NaN payloads, -0.0, the int64 extremes); nulls stay nulls;
 * state codes travel with their names, since codes are per volume and differ in the target.
 *
 * Import checks every record's checksum and every field before it writes anything of it, and refuses a stream of
 * a newer version or with a record it does not know (unless the record is marked optional). From a file it reads
 * the stream twice: once to verify all of it, the second time to write, so a damaged file writes nothing. From a
 * pipe it cannot look ahead; each batch is then written atomically, and when the stream turns out damaged the
 * series being imported is removed again if the import created it, and the error says what was written. */
#include "common.h"
#include "dump.h"
#include "tsdb.h"

#include <fcntl.h>
#include <fnmatch.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zstd.h>
#if defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

#ifndef TSDB_BUILD_VERSION
#define TSDB_BUILD_VERSION "dev"
#endif
#ifndef TSDB_FORMAT_VERSION
#define TSDB_FORMAT_VERSION 0
#endif

#define DUMP_MAGIC "TSDBDUMP"
#define DUMP_VERSION 1            /* the newest stream version this tsdb writes and reads */
#define DUMP_HDR_MAX 65536        /* header payload bytes a reader accepts */
#define DUMP_PAYLOAD_MAX (64u << 20) /* record payload bytes a reader accepts */
#define DUMP_CHUNK_MAX (1u << 20) /* rows per chunk a reader accepts */
#define DUMP_CHUNK_ROWS 65536     /* rows per chunk this exporter writes */
#define DUMP_NAME_MAX 65536       /* bytes of a series, path or state name */

enum { REC_SERIES = 1, REC_STATES = 2, REC_CHUNK = 3, REC_END = 4, REC_TRAILER = 5 };
enum { REC_F_OPTIONAL = 1 };
enum { CHUNK_F_NULLS = 1 };
enum { COMP_NONE = 0, COMP_ZSTD = 1 };

static const char *const type_name[] = {"float32", "float64", "int64", "bool", "state"};
static const unsigned type_width[] = {4, 8, 8, 1, 4};

/* ------------------------------------------------------------------ CRC-32C (Castagnoli), as in iSCSI and ext4 */

static uint32_t crc_tab[8][256];

static void crc_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = c & 1 ? (c >> 1) ^ 0x82f63b78u : c >> 1;
        crc_tab[0][i] = c;
    }
    for (int t = 1; t < 8; t++)
        for (int i = 0; i < 256; i++)
            crc_tab[t][i] = (crc_tab[t - 1][i] >> 8) ^ crc_tab[0][crc_tab[t - 1][i] & 0xff];
}

static uint32_t crc32c(uint32_t crc, const void *data, size_t n)
{
    const uint8_t *p = data;
    crc = ~crc;
#if defined(__SSE4_2__) && defined(__x86_64__)
    for (; n >= 8; p += 8, n -= 8) {
        uint64_t w;
        memcpy(&w, p, 8);
        crc = (uint32_t)_mm_crc32_u64(crc, w);
    }
    for (; n; p++, n--)
        crc = _mm_crc32_u8(crc, *p);
#else
    for (; n >= 8; p += 8, n -= 8) {
        uint32_t lo = crc ^ ((uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24);
        crc = crc_tab[7][lo & 0xff] ^ crc_tab[6][(lo >> 8) & 0xff] ^ crc_tab[5][(lo >> 16) & 0xff] ^
              crc_tab[4][lo >> 24] ^ crc_tab[3][p[4]] ^ crc_tab[2][p[5]] ^ crc_tab[1][p[6]] ^ crc_tab[0][p[7]];
    }
    for (; n; p++, n--)
        crc = (crc >> 8) ^ crc_tab[0][(crc ^ *p) & 0xff];
#endif
    return ~crc;
}

/* ------------------------------------------------------------------ little-endian bytes */

typedef struct buf {
    uint8_t *p;
    size_t n, cap;
} buf;

static void buf_need(buf *b, size_t more)
{
    if (b->n + more <= b->cap)
        return;
    size_t cap = b->cap ? b->cap : 4096;
    while (cap < b->n + more)
        cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p) {
        fprintf(stderr, "tsdb: out of memory\n");
        exit(1);
    }
    b->p = p;
    b->cap = cap;
}

static void put_bytes(buf *b, const void *p, size_t n)
{
    buf_need(b, n);
    memcpy(b->p + b->n, p, n);
    b->n += n;
}

static void put_u8(buf *b, uint8_t v) { put_bytes(b, &v, 1); }

static void put_u16(buf *b, uint16_t v)
{
    uint8_t x[2] = {(uint8_t)v, (uint8_t)(v >> 8)};
    put_bytes(b, x, 2);
}

static void put_u32(buf *b, uint32_t v)
{
    uint8_t x[4];
    for (int i = 0; i < 4; i++)
        x[i] = (uint8_t)(v >> (8 * i));
    put_bytes(b, x, 4);
}

static void put_u64(buf *b, uint64_t v)
{
    uint8_t x[8];
    for (int i = 0; i < 8; i++)
        x[i] = (uint8_t)(v >> (8 * i));
    put_bytes(b, x, 8);
}

static void put_str32(buf *b, const char *s)
{
    size_t n = strlen(s);
    put_u32(b, (uint32_t)n);
    put_bytes(b, s, n);
}

static void put_varint(buf *b, uint64_t v)
{
    buf_need(b, 10);
    while (v >= 0x80) {
        b->p[b->n++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    b->p[b->n++] = (uint8_t)v;
}

static uint32_t get_u32(const uint8_t *p) { return p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }

static uint64_t get_u64(const uint8_t *p) { return get_u32(p) | (uint64_t)get_u32(p + 4) << 32; }

/* A bounds-checked reader of one payload. */
typedef struct cur {
    const uint8_t *p, *end;
    bool bad;
} cur;

static const uint8_t *take(cur *c, size_t n)
{
    if (c->bad || (size_t)(c->end - c->p) < n) {
        c->bad = true;
        return NULL;
    }
    const uint8_t *p = c->p;
    c->p += n;
    return p;
}

static uint8_t take_u8(cur *c)
{
    const uint8_t *p = take(c, 1);
    return p ? *p : 0;
}

static uint16_t take_u16(cur *c)
{
    const uint8_t *p = take(c, 2);
    return p ? (uint16_t)(p[0] | p[1] << 8) : 0;
}

static uint32_t take_u32(cur *c)
{
    const uint8_t *p = take(c, 4);
    return p ? get_u32(p) : 0;
}

static uint64_t take_u64(cur *c)
{
    const uint8_t *p = take(c, 8);
    return p ? get_u64(p) : 0;
}

/* A string of n bytes (no NUL inside), copied with a terminating NUL into *out (malloc'd). */
static bool take_str(cur *c, size_t n, char **out)
{
    const uint8_t *p = n > DUMP_NAME_MAX ? (c->bad = true, NULL) : take(c, n);
    if (!p || memchr(p, 0, n))
        return c->bad = true, false;
    char *s = malloc(n + 1);
    if (!s)
        return c->bad = true, false;
    memcpy(s, p, n);
    s[n] = 0;
    free(*out);
    *out = s;
    return true;
}

static bool take_varint(cur *c, uint64_t *v)
{
    uint64_t x = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        if (c->p >= c->end)
            return c->bad = true, false;
        uint8_t b = *c->p++;
        if (shift == 63 && b > 1)
            return c->bad = true, false; /* more than 64 bits */
        x |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) {
            *v = x;
            return true;
        }
    }
    return c->bad = true, false;
}

/* Names in messages: escaped like the CLI prints them, so a control byte cannot forge a line. */
static const char *esc(const char *s, char *out, size_t len)
{
    size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)s; *p && o + 5 < len; p++) {
        if (*p < 0x20 || *p == 0x7f || *p == '\\')
            o += (size_t)snprintf(out + o, len - o, *p == '\\' ? "\\\\" : "\\x%02x", *p);
        else
            out[o++] = (char)*p;
    }
    out[o] = 0;
    return out;
}

/* ------------------------------------------------------------------ export */

typedef struct exporter {
    tsdb *db;
    FILE *out;
    const char *out_name;
    ZSTD_CCtx *zc;
    int level;
    uint32_t seq;      /* records written */
    buf rec, raw;      /* a record payload, a chunk before compression */
    /* the chunk being filled */
    uint32_t series;
    tsdb_type type;
    size_t n;
    int64_t *ts;
    uint8_t *val; /* n * width bytes */
    uint8_t *valid;
    bool nulls;
    /* the state codes named so far in this series' stream */
    uint8_t *named;
    size_t named_cap; /* bits */
    uint64_t s_rows, s_nulls, t_rows, t_series, bytes;
    uint32_t s_chunks;
    bool failed;
} exporter;

static void ex_write(exporter *x, const void *p, size_t n)
{
    if (!x->failed && fwrite(p, 1, n, x->out) != n) {
        fprintf(stderr, "tsdb: export: writing %s: %s\n", x->out_name, strerror(errno));
        x->failed = true;
    }
    x->bytes += n;
}

static void ex_record(exporter *x, uint8_t kind, const buf *payload)
{
    uint8_t h[16] = {kind, 0, 0, 0};
    for (int i = 0; i < 4; i++) {
        h[4 + i] = (uint8_t)(payload->n >> (8 * i));
        h[8 + i] = (uint8_t)(x->seq >> (8 * i));
    }
    uint32_t crc = crc32c(crc32c(0, h, 12), payload->p, payload->n);
    for (int i = 0; i < 4; i++)
        h[12 + i] = (uint8_t)(crc >> (8 * i));
    ex_write(x, h, 16);
    ex_write(x, payload->p, payload->n);
    x->seq++;
}

/* Names every state code of the chunk the stream has not named yet in this series, in one STATES record before
 * the chunk. */
static int ex_states(exporter *x)
{
    x->rec.n = 0;
    uint32_t count = 0;
    put_u32(&x->rec, 0);
    const uint8_t *v = x->val;
    for (size_t i = 0; i < x->n; i++) {
        if (x->nulls && !(x->valid[i >> 3] >> (i & 7) & 1))
            continue;
        uint32_t code;
        memcpy(&code, v + 4 * i, 4);
        if (code < x->named_cap && x->named[code >> 3] >> (code & 7) & 1)
            continue;
        if (code >= x->named_cap) {
            size_t cap = x->named_cap ? x->named_cap : 1024;
            while (cap <= code)
                cap *= 2;
            uint8_t *p = realloc(x->named, cap / 8);
            if (!p)
                return TSDB_ERR_NOMEM;
            memset(p + x->named_cap / 8, 0, (cap - x->named_cap) / 8);
            x->named = p;
            x->named_cap = cap;
        }
        const char *name = tsdb_state_name(x->db, x->series, code);
        if (!name) {
            fprintf(stderr, "tsdb: export: series %u: state code %u has no name\n", x->series, code);
            return TSDB_ERR_CORRUPT;
        }
        x->named[code >> 3] |= (uint8_t)(1u << (code & 7));
        put_u32(&x->rec, code);
        put_str32(&x->rec, name);
        count++;
    }
    if (count) {
        for (int i = 0; i < 4; i++)
            x->rec.p[i] = (uint8_t)(count >> (8 * i));
        ex_record(x, REC_STATES, &x->rec);
    }
    return 0;
}

static int ex_chunk(exporter *x)
{
    if (!x->n)
        return 0;
    int rc;
    if (x->type == TSDB_STATE && (rc = ex_states(x)) != 0)
        return rc;
    /* raw: flags, timestamps (first zigzag, then deltas, varints), values in byte planes, validity bitmap */
    buf *r = &x->raw;
    r->n = 0;
    put_u8(r, x->nulls ? CHUNK_F_NULLS : 0);
    put_varint(r, ((uint64_t)x->ts[0] << 1) ^ (uint64_t)(x->ts[0] >> 63));
    for (size_t i = 1; i < x->n; i++)
        put_varint(r, (uint64_t)x->ts[i] - (uint64_t)x->ts[i - 1]);
    unsigned w = type_width[x->type];
    buf_need(r, x->n * w);
    for (unsigned k = 0; k < w; k++) {
        uint8_t *plane = r->p + r->n + k * x->n;
        for (size_t i = 0; i < x->n; i++)
            plane[i] = x->val[i * w + k];
    }
    r->n += x->n * w;
    if (x->nulls)
        put_bytes(r, x->valid, (x->n + 7) / 8);

    buf *p = &x->rec;
    p->n = 0;
    put_u32(p, (uint32_t)x->n);
    put_u32(p, (uint32_t)r->n);
    if (x->level > 0) {
        size_t bound = ZSTD_compressBound(r->n);
        put_u8(p, COMP_ZSTD);
        buf_need(p, bound);
        size_t z = ZSTD_compressCCtx(x->zc, p->p + p->n, bound, r->p, r->n, x->level);
        if (ZSTD_isError(z)) {
            fprintf(stderr, "tsdb: export: zstd: %s\n", ZSTD_getErrorName(z));
            return TSDB_ERR_IO;
        }
        p->n += z;
    } else {
        put_u8(p, COMP_NONE);
        put_bytes(p, r->p, r->n);
    }
    ex_record(x, REC_CHUNK, p);
    x->s_chunks++;
    x->n = 0;
    x->nulls = false;
    return x->failed ? TSDB_ERR_IO : 0;
}

static int ex_rows(void *arg, const int64_t *ts, const tsdb_values *v, size_t n)
{
    exporter *x = arg;
    unsigned w = type_width[x->type];
    for (size_t i = 0; i < n;) {
        size_t k = n - i < DUMP_CHUNK_ROWS - x->n ? n - i : DUMP_CHUNK_ROWS - x->n;
        memcpy(x->ts + x->n, ts + i, k * 8);
        memcpy(x->val + x->n * w, (const uint8_t *)v->values + i * w, k * w);
        for (size_t j = 0; j < k; j++) {
            size_t d = x->n + j;
            bool ok = !v->valid || (v->valid[(i + j) >> 3] >> ((i + j) & 7) & 1);
            if (d % 8 == 0)
                x->valid[d >> 3] = 0;
            x->valid[d >> 3] |= (uint8_t)(ok << (d & 7));
            if (!ok) {
                memset(x->val + d * w, 0, w); /* a null's value is unspecified: zero it, the same every time */
                x->nulls = true;
                x->s_nulls++;
            }
        }
        x->n += k;
        x->s_rows += k;
        i += k;
        if (x->n == DUMP_CHUNK_ROWS) {
            int rc = ex_chunk(x);
            if (rc)
                return rc;
        }
    }
    return 0;
}

static bool matches(const char *name, char **pat, int npat)
{
    if (npat == 0)
        return true;
    for (int i = 0; i < npat; i++)
        if (fnmatch(pat[i], name, 0) == 0)
            return true;
    return false;
}

static void export_usage(void)
{
    fprintf(stderr, "usage: tsdb <dir> export [-o FILE] [--series PATTERN]... [--from TIME] [--to TIME] "
                    "[--zstd-level N]\n");
    exit(2);
}

int dump_export_main(const char *dir, int argc, char **argv)
{
    const char *out_name = NULL;
    char **pat = calloc((size_t)argc + 1, sizeof *pat);
    int npat = 0, level = 1;
    int64_t from = TSDB_TS_MIN, to = TSDB_TS_MAX;
    for (int i = 0; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if ((strcmp(a, "-o") == 0 || strcmp(a, "--output") == 0) && v)
            out_name = argv[++i];
        else if (strcmp(a, "--series") == 0 && v)
            pat[npat++] = argv[++i];
        else if (strcmp(a, "--from") == 0 && v && parse_time(v, &from))
            i++;
        else if (strcmp(a, "--to") == 0 && v && parse_time(v, &to))
            i++;
        else if (strcmp(a, "--zstd-level") == 0 && v) {
            char *end;
            long l = strtol(v, &end, 10);
            if (*end || end == v || l < 0 || l > ZSTD_maxCLevel())
                export_usage();
            level = (int)l;
            i++;
        } else
            export_usage();
    }
    if (from >= to) {
        fprintf(stderr, "tsdb: export: --from must be before --to\n");
        free(pat);
        return 2;
    }
    crc_init();
    if (access(dir, F_OK) != 0) {
        fprintf(stderr, "tsdb: %s does not exist\n", dir);
        free(pat);
        return 1;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1; /* also while a writer (tsdbd) has it open: each series is read from one snapshot */
    o.merge_threads = 0;
    o.create_volumes = 0;
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc) {
        fprintf(stderr, "tsdb: %s: %s\n", dir, tsdb_strerror(rc));
        free(pat);
        return 1;
    }

    /* to a file: written as FILE.partial and renamed once complete and synced, so FILE is always whole */
    char *tmp = NULL;
    FILE *out = stdout;
    if (out_name && strcmp(out_name, "-") != 0) {
        if (asprintf(&tmp, "%s.partial", out_name) < 0 || !(out = fopen(tmp, "wb"))) {
            fprintf(stderr, "tsdb: export: %s: %s\n", tmp ? tmp : out_name, strerror(errno));
            tsdb_close(db);
            free(tmp);
            free(pat);
            return 1;
        }
    } else {
        out_name = "standard output";
        if (isatty(STDOUT_FILENO)) {
            fprintf(stderr, "tsdb: export: not writing a binary stream to a terminal; use -o FILE or a pipe\n");
            tsdb_close(db);
            free(pat);
            return 2;
        }
    }
    static char obuf[1 << 20];
    setvbuf(out, obuf, _IOFBF, sizeof obuf);

    exporter x = {.db = db, .out = out, .out_name = out_name, .level = level};
    x.zc = ZSTD_createCCtx();
    x.ts = malloc(DUMP_CHUNK_ROWS * 8);
    x.val = malloc(DUMP_CHUNK_ROWS * 8);
    x.valid = malloc(DUMP_CHUNK_ROWS / 8);
    if (!x.zc || !x.ts || !x.val || !x.valid) {
        fprintf(stderr, "tsdb: out of memory\n");
        if (tmp) {
            fclose(out);
            unlink(tmp);
        }
        tsdb_close(db);
        if (x.zc)
            ZSTD_freeCCtx(x.zc);
        free(x.ts);
        free(x.val);
        free(x.valid);
        free(tmp);
        free(pat);
        return 1;
    }
    double t0 = now_s();

    /* header */
    buf h = {0};
    put_bytes(&h, DUMP_MAGIC, 8);
    put_u32(&h, DUMP_VERSION);
    buf hp = {0};
    put_u32(&hp, TSDB_FORMAT_VERSION);
    put_u32(&hp, 0); /* flags */
    put_u64(&hp, (uint64_t)now_ms());
    put_u64(&hp, (uint64_t)from);
    put_u64(&hp, (uint64_t)to);
    put_u16(&hp, (uint16_t)strlen(TSDB_BUILD_VERSION));
    put_bytes(&hp, TSDB_BUILD_VERSION, strlen(TSDB_BUILD_VERSION));
    put_u32(&h, (uint32_t)hp.n);
    put_bytes(&h, hp.p, hp.n);
    put_u32(&h, crc32c(0, h.p, h.n));
    ex_write(&x, h.p, h.n);

    uint32_t count = tsdb_series_count(db);
    const char *root = tsdb_volume_path(db, 0);
    for (uint32_t id = 0; id < count && !rc && !x.failed; id++) {
        const char *name = tsdb_series_name(db, id), *path = tsdb_series_path(db, id);
        if (!name || !matches(name, pat, npat))
            continue;
        int64_t keep = 0;
        if ((rc = tsdb_series_type(db, id, &x.type)) != 0 || (rc = tsdb_get_retention(db, id, &keep)) != 0)
            break;
        buf *p = &x.rec;
        p->n = 0;
        put_str32(p, name);
        put_str32(p, path && root && strcmp(path, root) != 0 ? path : ""); /* "": the database directory */
        put_u8(p, (uint8_t)x.type);
        put_u64(p, (uint64_t)keep);
        ex_record(&x, REC_SERIES, p);
        x.series = id;
        x.n = 0;
        x.nulls = false;
        x.s_rows = x.s_nulls = 0;
        x.s_chunks = 0;
        if (x.named)
            memset(x.named, 0, x.named_cap / 8);
        rc = tsdb_query_values(db, id, from, to, NULL, ex_rows, &x);
        if (!rc)
            rc = ex_chunk(&x);
        if (rc)
            break;
        p->n = 0;
        put_u64(p, x.s_rows);
        put_u64(p, x.s_nulls);
        put_u32(p, x.s_chunks);
        ex_record(&x, REC_END, p);
        x.t_rows += x.s_rows;
        x.t_series++;
    }
    if (!rc && !x.failed) {
        buf *p = &x.rec;
        p->n = 0;
        put_u64(p, x.t_series);
        put_u64(p, x.t_rows);
        put_u64(p, x.seq);
        ex_record(&x, REC_TRAILER, p);
    }
    if (!rc && !x.failed && (fflush(out) != 0 || (tmp && fsync(fileno(out)) != 0))) {
        fprintf(stderr, "tsdb: export: writing %s: %s\n", out_name, strerror(errno));
        x.failed = true;
    }
    if (rc && rc != TSDB_ERR_IO)
        fprintf(stderr, "tsdb: export: %s\n", tsdb_strerror(rc));
    else if (rc)
        fprintf(stderr, "tsdb: export: reading the database: %s\n", tsdb_strerror(rc));
    bool ok = !rc && !x.failed;
    if (tmp) {
        if (fclose(out) != 0 && ok) {
            fprintf(stderr, "tsdb: export: closing %s: %s\n", tmp, strerror(errno));
            ok = false;
        }
        if (ok && rename(tmp, out_name) != 0) {
            fprintf(stderr, "tsdb: export: renaming %s to %s: %s\n", tmp, out_name, strerror(errno));
            ok = false;
        }
        if (ok) { /* the rename is durable once the directory is synced */
            const char *slash = strrchr(out_name, '/');
            char *d = slash ? strndup(out_name, (size_t)(slash - out_name) + (slash == out_name)) : strdup(".");
            int fd = d ? open(d, O_RDONLY | O_DIRECTORY) : -1;
            if (fd < 0 || fsync(fd) != 0) {
                fprintf(stderr, "tsdb: export: syncing the directory of %s: %s\n", out_name, strerror(errno));
                ok = false;
            }
            if (fd >= 0)
                close(fd);
            free(d);
        }
        if (!ok)
            unlink(tmp);
    }
    int crc = tsdb_close(db);
    if (crc && ok) {
        fprintf(stderr, "tsdb: close: %s\n", tsdb_strerror(crc));
        ok = false;
    }
    double dt = now_s() - t0;
    if (ok)
        fprintf(stderr, "exported %" PRIu64 " series, %" PRIu64 " rows, %" PRIu64 " bytes in %.2f s (%.1f M rows/s)\n",
                x.t_series, x.t_rows, x.bytes, dt, dt > 0 ? (double)x.t_rows / dt / 1e6 : 0.0);
    if (ok && npat && !x.t_series)
        fprintf(stderr, "tsdb: export: no series matched\n");
    ZSTD_freeCCtx(x.zc);
    free(x.ts);
    free(x.val);
    free(x.valid);
    free(x.named);
    free(x.rec.p);
    free(x.raw.p);
    free(h.p);
    free(hp.p);
    free(tmp);
    free(pat);
    return ok ? 0 : 1;
}

/* ------------------------------------------------------------------ import */

enum { EXIST_ERROR, EXIST_SKIP, EXIST_MERGE, EXIST_REPLACE };

typedef struct vmap {
    const char *from, *to;
} vmap;

typedef struct importer {
    tsdb *db;
    FILE *in;
    const char *in_name;
    bool apply; /* false: the verifying pass, which writes nothing */
    int existing;
    bool create_volumes;
    vmap *maps;
    int nmaps;
    size_t batch_rows;
    ZSTD_DCtx *zd;
    uint64_t off; /* bytes of the stream read */
    uint32_t seq;
    char err[1024];
    /* header */
    uint32_t version, src_format;
    int64_t from, to, created_ms;
    char *src_version;
    /* record */
    buf rec, raw;
    /* the series being read */
    bool in_series;
    char *name, *path;
    tsdb_type type;
    int64_t retention;
    uint32_t id;     /* in the target, when apply */
    bool skip;       /* existing, --skip-existing */
    bool created;    /* by this import */
    uint32_t *code;  /* source state code -> target code (0: not named yet) */
    size_t code_cap;
    int64_t last_ts;
    bool any_rows;
    int64_t min_written, max_written;
    bool written;
    uint64_t s_rows, s_nulls;
    uint32_t s_chunks;
    /* the batch to insert */
    size_t n;
    int64_t *ts;
    uint8_t *val;
    uint8_t *valid;
    bool nulls;
    /* totals */
    uint64_t t_series, t_rows, t_created, t_skipped, t_written_rows;
} importer;

#define IFAIL(im, ...) (snprintf((im)->err, sizeof (im)->err, __VA_ARGS__), -1)

static int im_read(importer *im, void *p, size_t n, const char *what)
{
    size_t got = fread(p, 1, n, im->in);
    im->off += got;
    if (got == n)
        return 0;
    if (ferror(im->in))
        return IFAIL(im, "reading %s: %s", what, strerror(errno));
    return IFAIL(im, "the stream ends at byte %" PRIu64 " inside %s: it is truncated", im->off, what);
}

static int im_header(importer *im, bool magic_read)
{
    uint8_t h[16];
    memcpy(h, DUMP_MAGIC, 8);
    if (!magic_read) {
        if (im_read(im, h, 8, "the header"))
            return -1;
        if (memcmp(h, DUMP_MAGIC, 8) != 0)
            return IFAIL(im, "not a tsdb export stream");
    }
    if (im_read(im, h + 8, 8, "the header"))
        return -1;
    im->version = get_u32(h + 8);
    uint32_t len = get_u32(h + 12);
    if (im->version == 0 || im->version > DUMP_VERSION)
        return IFAIL(im, "the stream has format version %u; this tsdb reads versions 1 to %u: import it with a "
                         "newer tsdb", im->version, DUMP_VERSION);
    if (len > DUMP_HDR_MAX || len < 34)
        return IFAIL(im, "the header is damaged (length %u)", len);
    buf_need(&im->rec, len + 4);
    if (im_read(im, im->rec.p, len + 4, "the header"))
        return -1;
    uint32_t crc = crc32c(crc32c(0, h, 16), im->rec.p, len);
    if (crc != get_u32(im->rec.p + len))
        return IFAIL(im, "the header's checksum does not match: the stream is damaged");
    cur c = {im->rec.p, im->rec.p + len, false};
    im->src_format = take_u32(&c);
    uint32_t flags = take_u32(&c);
    im->created_ms = (int64_t)take_u64(&c);
    im->from = (int64_t)take_u64(&c);
    im->to = (int64_t)take_u64(&c);
    uint16_t vl = take_u16(&c);
    take_str(&c, vl, &im->src_version);
    if (c.bad || im->from >= im->to)
        return IFAIL(im, "the header is damaged");
    if (flags)
        return IFAIL(im, "the stream uses features this tsdb does not know (header flags %#x): import it with a "
                         "newer tsdb", flags);
    /* fields after these are additions of a later version 1 writer that a reader may ignore */
    return 0;
}

static bool is_volume(tsdb *db, const char *path)
{
    for (uint32_t i = 0; i < tsdb_volume_count(db); i++)
        if (strcmp(tsdb_volume_path(db, i), path) == 0)
            return true;
    return false;
}

static int im_insert(importer *im)
{
    if (!im->n)
        return 0;
    if (im->apply && !im->skip) {
        tsdb_values v = {im->type, im->val, im->nulls ? im->valid : NULL};
        int rc = tsdb_insert_series_values(im->db, im->id, im->ts, &v, im->n);
        if (rc)
            return IFAIL(im, "writing series: %s", tsdb_strerror(rc));
        if (!im->written || im->ts[0] < im->min_written)
            im->min_written = im->ts[0];
        if (!im->written || im->ts[im->n - 1] > im->max_written)
            im->max_written = im->ts[im->n - 1];
        im->written = true;
        im->t_written_rows += im->n;
    }
    im->n = 0;
    im->nulls = false;
    return 0;
}

static int im_series(importer *im, cur *c)
{
    char e1[256], e2[256];
    uint32_t nl = take_u32(c);
    take_str(c, nl, &im->name);
    uint32_t pl = c->bad ? 0 : take_u32(c);
    take_str(c, pl, &im->path);
    uint8_t type = take_u8(c);
    im->retention = (int64_t)take_u64(c);
    if (c->bad || !im->name[0] || type > TSDB_STATE || im->retention < 0)
        return IFAIL(im, "record %u (a series) is damaged", im->seq);
    if (im->path[0] && im->path[0] != '/')
        return IFAIL(im, "record %u (a series) is damaged: volume path %s is not absolute", im->seq,
                     esc(im->path, e1, sizeof e1));
    im->type = (tsdb_type)type;
    im->in_series = true;
    im->skip = im->created = im->written = im->any_rows = false;
    im->s_rows = im->s_nulls = 0;
    im->s_chunks = 0;
    im->n = 0;
    im->nulls = false;
    if (im->code)
        memset(im->code, 0, im->code_cap * sizeof *im->code);
    const char *name = esc(im->name, e1, sizeof e1);
    if (!im->db)
        return 0;

    uint32_t id;
    int rc = tsdb_series_lookup(im->db, im->name, &id);
    if (rc == 0) {
        tsdb_type t;
        if ((rc = tsdb_series_type(im->db, id, &t)) != 0)
            return IFAIL(im, "series %s: %s", name, tsdb_strerror(rc));
        if (t != im->type)
            return IFAIL(im, "series %s exists as %s; the stream has it as %s", name, type_name[t],
                         type_name[im->type]);
        if (im->existing == EXIST_ERROR)
            return IFAIL(im, "series %s exists; pass --skip-existing, --merge or --replace", name);
        im->skip = im->existing == EXIST_SKIP;
        im->id = id;
        if (im->apply && im->existing == EXIST_REPLACE && (rc = tsdb_delete_range(im->db, id, im->from, im->to)) != 0)
            return IFAIL(im, "series %s: deleting the values it replaces: %s", name, tsdb_strerror(rc));
        if (im->apply && im->existing == EXIST_REPLACE && (rc = tsdb_set_retention(im->db, id, im->retention)) != 0)
            return IFAIL(im, "series %s: setting its retention: %s", name, tsdb_strerror(rc));
        return 0;
    }
    if (rc != TSDB_ERR_NOTFOUND)
        return IFAIL(im, "series %s: %s", name, tsdb_strerror(rc));
    const char *path = im->path;
    for (int i = 0; i < im->nmaps; i++)
        if (strcmp(im->maps[i].from, path) == 0) {
            path = im->maps[i].to;
            break;
        }
    if (path[0] && !is_volume(im->db, path) && !im->create_volumes)
        return IFAIL(im, "series %s lives in volume %s, which is not a volume of this database: add it (tsdb DIR "
                         "volume add PATH), map it to another (--volume %s=NEW, or =  for the database directory) "
                         "or pass --create-volumes", name, esc(path, e2, sizeof e2), esc(im->path, e2, sizeof e2));
    if (!im->apply)
        return 0;
    uint32_t vol;
    if (path[0] && !is_volume(im->db, path) && (rc = tsdb_volume_add(im->db, path, &vol)) != 0)
        return IFAIL(im, "adding volume %s: %s", esc(path, e2, sizeof e2), tsdb_strerror(rc));
    if ((rc = tsdb_series_create(im->db, im->name, im->type, path[0] ? path : NULL, &im->id)) != 0)
        return IFAIL(im, "creating series %s: %s", name, tsdb_strerror(rc));
    im->created = true;
    im->t_created++;
    if (im->retention && (rc = tsdb_set_retention(im->db, im->id, im->retention)) != 0)
        return IFAIL(im, "series %s: setting its retention: %s", name, tsdb_strerror(rc));
    return 0;
}

static int im_states(importer *im, cur *c)
{
    char e[256];
    if (!im->in_series || im->type != TSDB_STATE)
        return IFAIL(im, "record %u (state names) is out of place: the stream is damaged", im->seq);
    uint32_t count = take_u32(c);
    char *name = NULL;
    for (uint32_t i = 0; i < count && !c->bad; i++) {
        uint32_t code = take_u32(c), len = take_u32(c);
        if (c->bad || !take_str(c, len, &name) || code == 0 || !name[0])
            break;
        if (code >= im->code_cap) {
            size_t cap = im->code_cap ? im->code_cap : 1024;
            while (cap <= code)
                cap *= 2;
            uint32_t *p = realloc(im->code, cap * sizeof *p);
            if (!p) {
                free(name);
                return IFAIL(im, "out of memory");
            }
            memset(p + im->code_cap, 0, (cap - im->code_cap) * sizeof *p);
            im->code = p;
            im->code_cap = cap;
        }
        if (im->code[code]) {
            free(name);
            return IFAIL(im, "record %u names state code %u twice: the stream is damaged", im->seq, code);
        }
        uint32_t target = 1; /* the verifying pass only notes that the code is named */
        int rc;
        if (im->apply && !im->skip && (rc = tsdb_state(im->db, im->id, name, &target)) != 0) {
            (void)IFAIL(im, "state %s: %s", esc(name, e, sizeof e), tsdb_strerror(rc));
            free(name);
            return -1;
        }
        im->code[code] = target;
    }
    free(name);
    if (c->bad || c->p != c->end)
        return IFAIL(im, "record %u (state names) is damaged", im->seq);
    return 0;
}

static int im_chunk(importer *im, cur *c)
{
    if (!im->in_series)
        return IFAIL(im, "record %u (rows) is outside a series: the stream is damaged", im->seq);
    uint32_t rows = take_u32(c), raw_len = take_u32(c);
    uint8_t comp = take_u8(c);
    unsigned w = type_width[im->type];
    if (c->bad || rows == 0 || rows > DUMP_CHUNK_MAX || raw_len > 1 + (size_t)rows * (10 + w + 1))
        return IFAIL(im, "record %u (rows) is damaged", im->seq);
    const uint8_t *raw;
    if (comp == COMP_ZSTD) {
        buf_need(&im->raw, raw_len);
        size_t z = ZSTD_decompressDCtx(im->zd, im->raw.p, raw_len, c->p, (size_t)(c->end - c->p));
        if (ZSTD_isError(z) || z != raw_len)
            return IFAIL(im, "record %u (rows) does not decompress: the stream is damaged", im->seq);
        raw = im->raw.p;
    } else if (comp == COMP_NONE) {
        if ((size_t)(c->end - c->p) != raw_len)
            return IFAIL(im, "record %u (rows) is damaged", im->seq);
        raw = c->p;
    } else {
        return IFAIL(im, "record %u uses compression %u, which this tsdb does not know: import it with a newer tsdb",
                     im->seq, comp);
    }
    cur r = {raw, raw + raw_len, false};
    uint8_t flags = take_u8(&r);
    if (flags & ~CHUNK_F_NULLS)
        return IFAIL(im, "record %u (rows) uses features this tsdb does not know: import it with a newer tsdb",
                     im->seq);
    /* make room in the batch */
    if (im->n + rows > im->batch_rows && im_insert(im))
        return -1;
    int64_t *ts = im->ts + im->n;
    uint64_t z;
    for (uint32_t i = 0; i < rows; i++) {
        if (!take_varint(&r, &z))
            return IFAIL(im, "record %u (rows) is damaged", im->seq);
        int64_t t = i == 0 ? (int64_t)(z >> 1) ^ -(int64_t)(z & 1) : (int64_t)((uint64_t)ts[i - 1] + z);
        if ((i ? z == 0 || t <= ts[i - 1] : im->any_rows && t <= im->last_ts) || t < im->from || t >= im->to ||
            t == TSDB_TS_MAX)
            return IFAIL(im, "record %u (rows): times out of order or out of range: the stream is damaged", im->seq);
        ts[i] = t;
    }
    im->any_rows = true;
    im->last_ts = ts[rows - 1];
    const uint8_t *planes = take(&r, (size_t)rows * w);
    const uint8_t *bits = flags & CHUNK_F_NULLS ? take(&r, (rows + 7) / 8) : NULL;
    if (r.bad || r.p != r.end)
        return IFAIL(im, "record %u (rows) is damaged", im->seq);
    uint8_t *val = im->val + im->n * w;
    for (unsigned k = 0; k < w; k++)
        for (uint32_t i = 0; i < rows; i++)
            val[(size_t)i * w + k] = planes[(size_t)k * rows + i];
    uint64_t nulls = 0;
    for (uint32_t i = 0; i < rows; i++) {
        size_t d = im->n + i;
        bool ok = !bits || (bits[i >> 3] >> (i & 7) & 1);
        if (d % 8 == 0)
            im->valid[d >> 3] = 0;
        im->valid[d >> 3] |= (uint8_t)(ok << (d & 7));
        if (!ok) {
            nulls++;
            continue;
        }
        const uint8_t *v = val + (size_t)i * w;
        uint32_t u32;
        uint64_t u64;
        bool bad = false;
        switch (im->type) {
        case TSDB_FLOAT32:
            memcpy(&u32, v, 4);
            bad = u32 == TSDB_NULL_F32_BITS;
            break;
        case TSDB_FLOAT64:
            memcpy(&u64, v, 8);
            bad = u64 == TSDB_NULL_F64_BITS;
            break;
        case TSDB_INT64:
            memcpy(&u64, v, 8);
            bad = u64 == (uint64_t)INT64_MIN;
            break;
        case TSDB_BOOL:
            bad = *v > 1;
            break;
        case TSDB_STATE:
            memcpy(&u32, v, 4);
            if (u32 == 0 || u32 >= im->code_cap || !im->code[u32])
                return IFAIL(im, "record %u (rows) has a state code without a name: the stream is damaged",
                             im->seq);
            u32 = im->code[u32];
            memcpy((uint8_t *)v, &u32, 4);
            break;
        }
        if (bad)
            return IFAIL(im, "record %u (rows) holds a value reserved for null: the stream is damaged", im->seq);
    }
    /* the values were copied in the stream's byte order, little-endian, which is the host's (x86-64, arm64) */
    if (nulls)
        im->nulls = true;
    im->n += rows;
    im->s_rows += rows;
    im->s_nulls += nulls;
    im->s_chunks++;
    if (im->n >= im->batch_rows)
        return im_insert(im);
    return 0;
}

static int im_end(importer *im, cur *c)
{
    if (!im->in_series)
        return IFAIL(im, "record %u (end of series) is out of place: the stream is damaged", im->seq);
    uint64_t rows = take_u64(c), nulls = take_u64(c);
    uint32_t chunks = take_u32(c);
    if (c->bad || rows != im->s_rows || nulls != im->s_nulls || chunks != im->s_chunks)
        return IFAIL(im, "series %s: the stream says %" PRIu64 " rows, %" PRIu64 " nulls in %u chunks, but holds %"
                         PRIu64 ", %" PRIu64 " in %u: it is damaged", im->name, rows, nulls, chunks, im->s_rows,
                     im->s_nulls, im->s_chunks);
    if (im_insert(im))
        return -1;
    im->in_series = false;
    im->t_series++;
    im->t_rows += rows;
    if (im->skip)
        im->t_skipped++;
    return 0;
}

/* One pass over the stream (after its magic, when magic_read). */
static int im_pass(importer *im, bool magic_read)
{
    if (im_header(im, magic_read))
        return -1;
    im->seq = 0;
    im->in_series = false;
    im->t_series = im->t_rows = im->t_created = im->t_skipped = im->t_written_rows = 0;
    for (;;) {
        uint8_t h[16];
        if (im_read(im, h, 16, "a record header"))
            return -1;
        uint32_t len = get_u32(h + 4), seq = get_u32(h + 8);
        if (len > DUMP_PAYLOAD_MAX)
            return IFAIL(im, "record %u at byte %" PRIu64 " is damaged (length %u)", im->seq, im->off - 16, len);
        buf_need(&im->rec, len);
        if (im_read(im, im->rec.p, len, "a record"))
            return -1;
        if (crc32c(crc32c(0, h, 12), im->rec.p, len) != get_u32(h + 12))
            return IFAIL(im, "record %u at byte %" PRIu64 " fails its checksum: the stream is damaged", im->seq,
                         im->off - 16 - len);
        if (seq != im->seq || h[2] || h[3])
            return IFAIL(im, "record %u at byte %" PRIu64 " is out of sequence: the stream is damaged", im->seq,
                         im->off - 16 - len);
        cur c = {im->rec.p, im->rec.p + len, false};
        int rc = 0;
        switch (h[0]) {
        case REC_SERIES:
            if (im->in_series)
                return IFAIL(im, "series %s has no end record: the stream is damaged", im->name);
            rc = im_series(im, &c);
            break;
        case REC_STATES:
            rc = im_states(im, &c);
            break;
        case REC_CHUNK:
            rc = im_chunk(im, &c);
            break;
        case REC_END:
            rc = im_end(im, &c);
            break;
        case REC_TRAILER: {
            uint64_t series = take_u64(&c), rows = take_u64(&c), records = take_u64(&c);
            if (im->in_series)
                return IFAIL(im, "series %s has no end record: the stream is damaged", im->name);
            if (c.bad || series != im->t_series || rows != im->t_rows || records != im->seq)
                return IFAIL(im, "the trailer does not match the stream (%" PRIu64 " series, %" PRIu64
                                 " rows): it is damaged", series, rows);
            if (fgetc(im->in) != EOF)
                return IFAIL(im, "bytes follow the trailer at byte %" PRIu64 ": the stream is damaged", im->off);
            return 0;
        }
        default:
            if (!(h[1] & REC_F_OPTIONAL))
                return IFAIL(im, "record %u has kind %u, which this tsdb does not know: import it with a newer tsdb "
                                 "(the stream was written by tsdb %s)", im->seq, h[0], im->src_version);
        }
        if (rc)
            return -1;
        im->seq++;
    }
}

static void import_usage(void)
{
    fprintf(stderr, "usage: tsdb <dir> import [FILE|-] [--skip-existing|--merge|--replace] [--volume OLD=NEW]... "
                    "[--create-volumes] [--no-verify] [--batch-rows N]\n"
                    "       tsdb <dir> import --check [FILE|-]\n");
    exit(2);
}

/* The CSV import reads the input from its start: the bytes read to look for the magic come first. */
typedef struct prefixed {
    FILE *f;
    uint8_t head[16];
    size_t n, pos;
} prefixed;

static ssize_t prefixed_read(void *arg, char *out, size_t len)
{
    prefixed *p = arg;
    if (p->pos < p->n) {
        size_t k = p->n - p->pos < len ? p->n - p->pos : len;
        memcpy(out, p->head + p->pos, k);
        p->pos += k;
        return (ssize_t)k;
    }
    size_t k = fread(out, 1, len, p->f);
    return k == 0 && ferror(p->f) ? -1 : (ssize_t)k;
}

static int prefixed_close(void *arg)
{
    prefixed *p = arg;
    int rc = p->f != stdin ? fclose(p->f) : 0;
    free(p);
    return rc;
}

/* Everything the importer owns is freed by dump_import_main, so that no return path can leak it. */
static int import_run(const char *dir, int argc, char **argv, FILE **csv, importer *im)
{
    const char *in_name = NULL;
    bool check = false, verify = true, dump_opts = false;
    im->maps = calloc((size_t)argc + 1, sizeof *im->maps);
    if (!im->maps) {
        fprintf(stderr, "tsdb: out of memory\n");
        return 1;
    }
    for (int i = 0; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (a[0] == '-' && a[1])
            dump_opts = true;
        if (strcmp(a, "--check") == 0)
            check = true;
        else if (strcmp(a, "--skip-existing") == 0)
            im->existing = EXIST_SKIP;
        else if (strcmp(a, "--merge") == 0)
            im->existing = EXIST_MERGE;
        else if (strcmp(a, "--replace") == 0)
            im->existing = EXIST_REPLACE;
        else if (strcmp(a, "--create-volumes") == 0)
            im->create_volumes = true;
        else if (strcmp(a, "--no-verify") == 0)
            verify = false;
        else if (strcmp(a, "--volume") == 0 && v && strchr(v, '=')) {
            char *s = strdup(v), *eq = strchr(s, '=');
            *eq = 0;
            im->maps[im->nmaps++] = (vmap){s, eq + 1};
            i++;
        } else if (strcmp(a, "--batch-rows") == 0 && v) {
            char *end;
            unsigned long long n = strtoull(v, &end, 10);
            if (*end || end == v || n < 1 || n > 100000000)
                import_usage();
            im->batch_rows = (size_t)n;
            i++;
        } else if (!in_name && (a[0] != '-' || strcmp(a, "-") == 0)) {
            in_name = a;
        } else
            import_usage();
    }
    FILE *in = stdin;
    if (in_name && strcmp(in_name, "-") != 0) {
        if (!(in = fopen(in_name, "rb"))) {
            fprintf(stderr, "tsdb: import: %s: %s\n", in_name, strerror(errno));
            return 1;
        }
    } else {
        in_name = "standard input";
    }
    static char ibuf[1 << 20];
    setvbuf(in, ibuf, _IOFBF, sizeof ibuf);
    prefixed *pre = calloc(1, sizeof *pre);
    if (!pre) {
        fprintf(stderr, "tsdb: out of memory\n");
        if (in != stdin)
            fclose(in);
        return 1;
    }
    pre->f = in;
    /* 16 bytes: an export stream has NUL bytes there (its version), CSV text never has; so a stream whose magic
     * is damaged is refused rather than read as CSV */
    pre->n = fread(pre->head, 1, 16, in);
    if (pre->n < 8 || memcmp(pre->head, DUMP_MAGIC, 8) != 0) {
        if (check || dump_opts || memchr(pre->head, 0, pre->n)) {
            fprintf(stderr, "tsdb: import: %s is not a tsdb export stream%s\n", in_name,
                    check || !dump_opts ? (check ? "" : ", nor CSV text (a damaged stream?)")
                                        : " (the options are for export streams; CSV takes none)");
            free(pre);
            if (in != stdin)
                fclose(in);
            return 1;
        }
        *csv = fopencookie(pre, "r", (cookie_io_functions_t){.read = prefixed_read, .close = prefixed_close});
        return -1;
    }
    crc_init();
    struct stat st;
    bool seekable = fstat(fileno(in), &st) == 0 && S_ISREG(st.st_mode);
    /* up to 8 bytes after the magic were read too: a file seeks back to them, a pipe gets them in front */
    if (seekable) {
        free(pre);
        if (fseek(in, 8, SEEK_SET) != 0) {
            fprintf(stderr, "tsdb: import: %s: %s\n", in_name, strerror(errno));
            if (in != stdin)
                fclose(in);
            return 1;
        }
    } else {
        pre->pos = 8;
        in = fopencookie(pre, "r", (cookie_io_functions_t){.read = prefixed_read, .close = prefixed_close});
        if (!in) {
            fprintf(stderr, "tsdb: import: %s\n", strerror(errno));
            if (pre->f != stdin)
                fclose(pre->f);
            free(pre);
            return 1;
        }
    }
    im->in = in;
    im->in_name = in_name;
    im->off = 8;
    im->zd = ZSTD_createDCtx();
    double t0 = now_s();
    int ret = 1;
    tsdb *db = NULL; /* none with --check: every series is taken as new and every volume as present */
    if (!check) {
        tsdb_options o;
        tsdb_options_init(&o);
        o.create_volumes = 0; /* volumes only through --create-volumes */
        int rc = tsdb_open(dir, &o, &db);
        if (rc) {
            fprintf(stderr, "tsdb: %s: %s\n", dir, tsdb_strerror(rc));
            if (in != stdin)
                fclose(in);
            return 1;
        }
    }
    im->db = db;
    im->ts = malloc(im->batch_rows * 8 + DUMP_CHUNK_MAX * 8);
    im->val = malloc(im->batch_rows * 8 + DUMP_CHUNK_MAX * 8);
    im->valid = malloc((im->batch_rows + DUMP_CHUNK_MAX) / 8 + 1);
    if (!im->ts || !im->val || !im->valid || !im->zd) {
        fprintf(stderr, "tsdb: out of memory\n");
        if (db)
            tsdb_close(db);
        if (in != stdin)
            fclose(in);
        return 1;
    }
    char e[256];
    int rc = 0;
    if (check || (seekable && verify)) {
        im->apply = false;
        rc = im_pass(im, true);
        if (!rc && !check) {
            if (fseek(in, 0, SEEK_SET) != 0) {
                fprintf(stderr, "tsdb: import: %s: %s\n", in_name, strerror(errno));
                if (db)
                    tsdb_close(db);
                if (in != stdin)
                    fclose(in);
                return 1;
            }
            im->off = 0;
        }
    }
    if (!rc && !check) {
        im->apply = true;
        rc = im_pass(im, !(seekable && verify));
    }
    if (rc) {
        fprintf(stderr, "tsdb: import: %s: %s\n", in_name, im->err);
        if (im->apply) {
            if (im->in_series && im->created && im->written) {
                int drc = tsdb_delete_range(db, im->id, im->min_written, im->max_written + 1);
                fprintf(stderr, "tsdb: import: series %s, which this import created, %s\n", esc(im->name, e, sizeof e),
                        drc ? "could not be emptied again" : "was emptied again");
            } else if (im->in_series && im->written) {
                fprintf(stderr, "tsdb: import: series %s is partly imported\n", esc(im->name, e, sizeof e));
            }
            fprintf(stderr, "tsdb: import: %" PRIu64 " series before it were imported completely\n", im->t_series);
        } else if (!check) {
            fprintf(stderr, "tsdb: import: nothing was written\n");
        }
    } else {
        double dt = now_s() - t0;
        if (check)
            fprintf(stderr, "%s: a whole tsdb export stream, version %u, from tsdb %s (format %u): %" PRIu64
                            " series, %" PRIu64 " rows\n", in_name, im->version, im->src_version, im->src_format,
                    im->t_series, im->t_rows);
        else
            fprintf(stderr, "imported %" PRIu64 " series (%" PRIu64 " created, %" PRIu64 " skipped), %" PRIu64
                            " rows in %.2f s (%.1f M rows/s)\n", im->t_series, im->t_created, im->t_skipped,
                    im->t_written_rows, dt, dt > 0 ? (double)im->t_written_rows / dt / 1e6 : 0.0);
        ret = 0;
    }
    if (db) {
        int crc = tsdb_close(db);
        if (crc) {
            fprintf(stderr, "tsdb: close: %s\n", tsdb_strerror(crc));
            ret = 1;
        }
    }
    if (in != stdin)
        fclose(in);
    return ret;
}

int dump_import_main(const char *dir, int argc, char **argv, FILE **csv)
{
    importer im = {.existing = EXIST_ERROR, .batch_rows = 1 << 20};
    int ret = import_run(dir, argc, argv, csv, &im);
    if (im.zd)
        ZSTD_freeDCtx(im.zd);
    free(im.ts);
    free(im.val);
    free(im.valid);
    free(im.code);
    free(im.rec.p);
    free(im.raw.p);
    free(im.name);
    free(im.path);
    free(im.src_version);
    for (int i = 0; i < im.nmaps; i++)
        free((char *)im.maps[i].from);
    free(im.maps);
    return ret;
}
