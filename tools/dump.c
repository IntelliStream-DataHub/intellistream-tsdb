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

#include <dirent.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <limits.h>
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
#define DUMP_STATES_SPLIT (1u << 20) /* this exporter starts a new STATES record beyond this many bytes */

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
    if (payload->n > DUMP_PAYLOAD_MAX) { /* a reader refuses it: a bug here, never a stream that cannot be read */
        fprintf(stderr, "tsdb: export: record of %zu bytes exceeds the format's limit\n", payload->n);
        abort();
    }
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

static void ex_states_flush(exporter *x, uint32_t count)
{
    if (!count)
        return;
    for (int i = 0; i < 4; i++)
        x->rec.p[i] = (uint8_t)(count >> (8 * i));
    ex_record(x, REC_STATES, &x->rec);
}

/* Names every state code of the chunk the stream has not named yet in this series, in STATES records before the
 * chunk: as many as it takes to keep each under DUMP_STATES_SPLIT bytes (names may be up to 32 KiB, so the names of
 * one chunk could exceed the record limit). */
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
        if (strlen(name) > DUMP_NAME_MAX) {
            fprintf(stderr, "tsdb: export: series %u: state code %u has a name longer than the format allows\n",
                    x->series, code);
            return TSDB_ERR_INVALID;
        }
        if (count && x->rec.n + 8 + strlen(name) > DUMP_STATES_SPLIT) {
            ex_states_flush(x, count);
            x->rec.n = 0;
            count = 0;
            put_u32(&x->rec, 0);
        }
        x->named[code >> 3] |= (uint8_t)(1u << (code & 7));
        put_u32(&x->rec, code);
        put_str32(&x->rec, name);
        count++;
    }
    ex_states_flush(x, count);
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
                    "[--zstd-level N] [--force]\n");
    exit(2);
}

int dump_export_main(const char *dir, int argc, char **argv)
{
    const char *out_name = NULL;
    char **pat = calloc((size_t)argc + 1, sizeof *pat);
    int npat = 0, level = 1;
    bool force = false;
    int64_t from = TSDB_TS_MIN, to = TSDB_TS_MAX;
    for (int i = 0; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if ((strcmp(a, "-o") == 0 || strcmp(a, "--output") == 0) && v)
            out_name = argv[++i];
        else if (strcmp(a, "--force") == 0)
            force = true;
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
        /* an existing FILE.partial may be another export writing it now: not truncated without --force */
        int fd = -1;
        if (asprintf(&tmp, "%s.partial", out_name) < 0 ||
            (fd = open(tmp, O_WRONLY | O_CREAT | O_CLOEXEC | (force ? O_TRUNC : O_EXCL), 0644)) < 0 ||
            !(out = fdopen(fd, "wb"))) {
            if (errno == EEXIST)
                fprintf(stderr, "tsdb: export: %s exists: another export may be writing it, or one was "
                                "interrupted; remove it or pass --force\n", tmp);
            else
                fprintf(stderr, "tsdb: export: %s: %s\n", tmp ? tmp : out_name, strerror(errno));
            if (fd >= 0)
                close(fd);
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
        if (strlen(name) > DUMP_NAME_MAX || (path && strlen(path) > DUMP_NAME_MAX)) {
            fprintf(stderr, "tsdb: export: series %u: its name or volume path is longer than the format allows\n", id);
            rc = TSDB_ERR_INVALID;
            break;
        }
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
    char *from, *to;
} vmap;

/* The source's state codes of the series being read -> the target's: open addressing sized by the names the stream
 * gives, never by the code values (a stream may use any u32); a new generation empties it for the next series. */
typedef struct code_slot {
    uint32_t key, val, gen;
} code_slot;

/* The series names read so far, as two independent 64-bit hashes (16 bytes per series): a stream naming a series
 * twice is refused before anything is written. */
typedef struct name_slot {
    uint64_t a, b;
} name_slot;

typedef struct importer {
    tsdb *db;
    char root[PATH_MAX]; /* the target's directory, canonical */
    FILE *in;
    const char *in_name;
    bool apply;  /* false: the verifying pass, which writes nothing */
    bool report; /* this pass warns about rows past retention (the first pass only) */
    int existing;
    bool create_volumes, no_retention;
    vmap *maps, *roots;
    int nmaps, nroots;
    char **vols_ok; /* target volume paths checked already */
    int nvols_ok;
    size_t batch_rows;
    ZSTD_DCtx *zd;
    uint64_t off; /* bytes of the stream read */
    uint32_t seq;
    int64_t now_ms;
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
    char vpath[PATH_MAX]; /* its volume in the target */
    tsdb_type type;
    int64_t retention, keep; /* the stream's; the one that will apply in the target (0: none) */
    uint32_t id;             /* in the target, when apply */
    bool skip;               /* existing, --skip-existing */
    bool created;            /* by this import */
    bool replacing, deleted; /* existing, --replace; its range deleted already (it did not fit one batch) */
    code_slot *codes;
    size_t codes_cap, codes_n;
    uint32_t codes_gen;
    name_slot *names;
    size_t names_cap, names_n;
    int64_t last_ts;
    bool any_rows;
    int64_t min_written, max_written;
    bool written;
    uint64_t s_rows, s_nulls, s_expired;
    uint32_t s_chunks;
    /* the batch: rows of whole series of one type, then from cur0 those of the series being read */
    size_t n, cur0;
    tsdb_type btype;
    uint32_t *sid;
    int64_t *ts;
    uint8_t *val;
    uint8_t *valid;
    bool nulls;
    /* totals */
    uint64_t t_series, t_rows, t_created, t_skipped, t_written_rows, t_expired, t_expired_series;
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

/* --------------------------------------------------------- volume paths */

/* As the engine checks a volume path (tsdb_volume_add): absolute, no control characters, no . or .. */
static bool vpath_valid(const char *p)
{
    if (p[0] != '/' || strlen(p) >= PATH_MAX)
        return false;
    for (const unsigned char *s = (const unsigned char *)p; *s; s++)
        if (*s < 0x20 || *s == 0x7f)
            return false;
    for (const char *c = p; *c;) {
        while (*c == '/')
            c++;
        size_t n = strcspn(c, "/");
        if ((n == 1 && c[0] == '.') || (n == 2 && c[0] == '.' && c[1] == '.'))
            return false;
        c += n;
    }
    return true;
}

/* Repeated '/' collapsed, a trailing '/' dropped (in place): "/a//b/" and "/a/b" are the same volume. */
static char *vpath_norm(char *p)
{
    size_t n = 0;
    for (const char *s = p; *s; s++)
        if (*s != '/' || n == 0 || p[n - 1] != '/')
            p[n++] = *s;
    if (n > 1 && p[n - 1] == '/')
        n--;
    p[n] = 0;
    return p;
}

/* a is b or lies under it */
static bool vpath_within(const char *a, const char *b)
{
    size_t n = strlen(b);
    return (n == 1 && b[0] == '/') || (strncmp(a, b, n) == 0 && (a[n] == 0 || a[n] == '/'));
}

static bool is_volume(tsdb *db, const char *path)
{
    char canon[PATH_MAX];
    bool c = realpath(path, canon) != NULL;
    for (uint32_t i = 0; i < tsdb_volume_count(db); i++) {
        const char *v = tsdb_volume_path(db, i);
        if (strcmp(v, path) == 0 || (c && strcmp(v, canon) == 0))
            return true;
    }
    return false;
}

/* Whether --create-volumes can add `path` (valid, not a volume yet): what tsdb_volume_add would refuse, checked
 * before anything is written. NULL if it can, else why not. */
static const char *vpath_creatable(importer *im, const char *path)
{
    static const char *const tsdb_files[] = {"ID", "CATALOG", "VOLUMES", "ROOT_ID", "MANIFEST", "SERIES", "LOCK",
                                             "META"};
    char canon[PATH_MAX];
    struct stat st;
    if (stat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode))
            return "is not a directory";
        if (!realpath(path, canon))
            return strerror(errno);
        int fd = open(canon, O_RDONLY | O_DIRECTORY);
        if (fd < 0)
            return strerror(errno);
        bool has = false;
        for (size_t i = 0; i < sizeof tsdb_files / sizeof *tsdb_files && !has; i++)
            has = faccessat(fd, tsdb_files[i], F_OK, AT_SYMLINK_NOFOLLOW) == 0;
        close(fd);
        if (has)
            return "holds tsdb files (a volume or the directory of another database?)";
        if (access(canon, W_OK) != 0)
            return "is not writable";
    } else if (errno == ENOENT) { /* made one level deep: the parent must exist */
        char parent[PATH_MAX], pcanon[PATH_MAX];
        snprintf(parent, sizeof parent, "%s", path);
        char *slash = strrchr(parent, '/');
        const char *base = path + (slash - parent) + 1;
        slash[slash == parent] = 0; /* "/a" -> "/", "/a/b" -> "/a" */
        if (stat(parent, &st) != 0 || !S_ISDIR(st.st_mode))
            return "does not exist, nor does its parent directory (a volume is made one level deep only)";
        if (access(parent, W_OK) != 0)
            return "does not exist, and its parent directory is not writable";
        if (!realpath(parent, pcanon))
            return strerror(errno);
        if (snprintf(canon, sizeof canon, "%s/%s", strcmp(pcanon, "/") ? pcanon : "", base) >= (int)sizeof canon)
            return "is too long";
    } else {
        return strerror(errno);
    }
    if (vpath_within(canon, im->root) || vpath_within(im->root, canon))
        return "lies inside the database directory or contains it";
    for (uint32_t i = 1; i < tsdb_volume_count(im->db); i++) {
        const char *v = tsdb_volume_path(im->db, i);
        if (vpath_within(canon, v) || vpath_within(v, canon))
            return "lies inside another volume of this database or contains one";
    }
    return NULL;
}

/* The target volume of the series being read (--volume, then --volume-root, else its own path) into im->vpath,
 * checked: a valid path that is a volume of the target, or one --create-volumes can add. */
static int im_volume(importer *im, const char *name)
{
    char e1[256], e2[256];
    char src[PATH_MAX];
    snprintf(src, sizeof src, "%s", im->path);
    vpath_norm(src);
    const char *to = NULL;
    for (int i = 0; i < im->nmaps && !to; i++)
        if (strcmp(im->maps[i].from, src) == 0)
            to = im->maps[i].to;
    int root = -1; /* the longest --volume-root that holds the path */
    for (int i = 0; i < im->nroots && !to && src[0]; i++)
        if (vpath_within(src, im->roots[i].from) && (root < 0 || strlen(im->roots[i].from) > strlen(im->roots[root].from)))
            root = i;
    if (root >= 0) {
        size_t k = strlen(im->roots[root].from);
        const char *rest = src + (k == 1 ? 0 : k), *nw = strcmp(im->roots[root].to, "/") ? im->roots[root].to : "";
        if (snprintf(im->vpath, sizeof im->vpath, "%s%s", nw, rest[0] || nw[0] ? rest : "/") >= (int)sizeof im->vpath)
            return IFAIL(im, "series %s: volume %s mapped by --volume-root is too long", name,
                         esc(src, e1, sizeof e1));
        to = im->vpath;
    }
    if (to != im->vpath)
        snprintf(im->vpath, sizeof im->vpath, "%s", to ? to : src);
    vpath_norm(im->vpath);
    if (!im->vpath[0])
        return 0; /* the database directory */
    if (!vpath_valid(im->vpath))
        return IFAIL(im, "series %s: volume %s (from %s) is not a valid volume path: it must be absolute, without . "
                         "or .. components", name, esc(im->vpath, e1, sizeof e1), esc(src, e2, sizeof e2));
    if (!im->db)
        return 0; /* --check: volumes are the target's business */
    for (int i = 0; i < im->nvols_ok; i++)
        if (strcmp(im->vols_ok[i], im->vpath) == 0)
            return 0;
    if (!is_volume(im->db, im->vpath)) {
        if (!im->create_volumes)
            return IFAIL(im, "series %s lives in volume %s, which is not a volume of this database: map it "
                             "(--volume %s=NEW, --volume %s= for the database directory, or --volume-root OLD=NEW "
                             "for every volume under OLD), add it (tsdb DIR volume add PATH) or pass "
                             "--create-volumes", name, esc(im->vpath, e1, sizeof e1), esc(src, e2, sizeof e2),
                         e2);
        const char *why = vpath_creatable(im, im->vpath);
        if (why)
            return IFAIL(im, "series %s: volume %s %s: it cannot be added; map it elsewhere (--volume %s=NEW or "
                             "--volume-root OLD=NEW)", name, esc(im->vpath, e1, sizeof e1), why,
                         esc(src, e2, sizeof e2));
    }
    char **p = realloc(im->vols_ok, (size_t)(im->nvols_ok + 1) * sizeof *p);
    if (!p || !(p[im->nvols_ok] = strdup(im->vpath))) {
        if (p)
            im->vols_ok = p;
        return IFAIL(im, "out of memory");
    }
    im->vols_ok = p;
    im->nvols_ok++;
    return 0;
}

/* ------------------------------------------------- names and state codes */

static void name_hash(const char *s, uint64_t *a, uint64_t *b)
{
    uint64_t x = 0xcbf29ce484222325ull, y = strlen(s) * 0x9e3779b97f4a7c15ull;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        x = (x ^ *p) * 0x100000001b3ull;
        y = (y + *p + 1) * 0xbf58476d1ce4e5b9ull;
        y ^= y >> 31;
    }
    y ^= y >> 29;
    *a = x;
    *b = y | 1; /* never 0: an empty slot is {0, 0} */
}

/* 1: the stream named this series before; 0: it is new (and noted); -1: out of memory */
static int name_seen(importer *im, const char *name)
{
    if ((im->names_n + 1) * 2 > im->names_cap) {
        size_t cap = im->names_cap ? im->names_cap * 2 : 1024;
        name_slot *t = calloc(cap, sizeof *t);
        if (!t)
            return -1;
        for (size_t i = 0; i < im->names_cap; i++)
            if (im->names[i].b) {
                size_t j = im->names[i].a & (cap - 1);
                while (t[j].b)
                    j = (j + 1) & (cap - 1);
                t[j] = im->names[i];
            }
        free(im->names);
        im->names = t;
        im->names_cap = cap;
    }
    uint64_t a, b;
    name_hash(name, &a, &b);
    size_t j = a & (im->names_cap - 1);
    for (; im->names[j].b; j = (j + 1) & (im->names_cap - 1))
        if (im->names[j].a == a && im->names[j].b == b)
            return 1;
    im->names[j] = (name_slot){a, b};
    im->names_n++;
    return 0;
}

static size_t code_hash(uint32_t key, size_t cap) { return (size_t)(((uint64_t)key * 0x9e3779b97f4a7c15ull) >> 32) & (cap - 1); }

/* The slot of a source code in this series: with add, made (val 0) if missing; NULL if missing or out of memory. */
static uint32_t *code_slot_of(importer *im, uint32_t key, bool add)
{
    if (add && (im->codes_n + 1) * 2 > im->codes_cap) {
        size_t cap = im->codes_cap ? im->codes_cap * 2 : 64;
        code_slot *t = calloc(cap, sizeof *t);
        if (!t)
            return NULL;
        for (size_t i = 0; i < im->codes_cap; i++)
            if (im->codes[i].gen == im->codes_gen) {
                size_t j = code_hash(im->codes[i].key, cap);
                while (t[j].gen)
                    j = (j + 1) & (cap - 1);
                t[j] = im->codes[i];
                t[j].gen = 1;
            }
        free(im->codes);
        im->codes = t;
        im->codes_cap = cap;
        im->codes_gen = 1;
    }
    if (!im->codes_cap)
        return NULL;
    for (size_t j = code_hash(key, im->codes_cap);; j = (j + 1) & (im->codes_cap - 1)) {
        code_slot *s = &im->codes[j];
        if (s->gen != im->codes_gen) {
            if (!add)
                return NULL;
            *s = (code_slot){key, 0, im->codes_gen};
            im->codes_n++;
            return &s->val;
        }
        if (s->key == key)
            return &s->val;
    }
}

static void codes_clear(importer *im)
{
    im->codes_n = 0;
    if (++im->codes_gen == 0) { /* wrapped: slots of generation 0 would look current */
        if (im->codes)
            memset(im->codes, 0, im->codes_cap * sizeof *im->codes);
        im->codes_gen = 1;
    }
}

/* ------------------------------------------------------------ writing */

/* Writes the first n rows of the batch and drops them. */
static int im_write(importer *im, size_t n)
{
    if (!n)
        return 0;
    tsdb_values v = {im->btype, im->val, im->nulls ? im->valid : NULL};
    int rc = tsdb_insert_values(im->db, im->sid, im->ts, &v, n);
    if (rc)
        return IFAIL(im, "writing rows: %s", tsdb_strerror(rc));
    im->t_written_rows += n;
    return 0;
}

/* Writes the whole batch: the series before the current one and the current one's rows so far. */
static int im_insert(importer *im)
{
    char e[256];
    if (!im->n)
        return 0;
    if (!im->apply) {
        im->n = im->cur0 = 0;
        return 0;
    }
    if (im->in_series && im->replacing && !im->deleted && im->n > im->cur0) {
        /* more rows than one batch: the range is deleted first, so a crash before the import ends leaves it
         * partly written; rerunning the import (from the file) restores it */
        int rc = tsdb_delete_range(im->db, im->id, im->from, im->to);
        if (rc)
            return IFAIL(im, "series %s: deleting the values it replaces: %s", esc(im->name, e, sizeof e),
                         tsdb_strerror(rc));
        im->deleted = true;
    }
    if (im_write(im, im->n))
        return -1;
    if (im->in_series && im->n > im->cur0) {
        if (!im->written || im->ts[im->cur0] < im->min_written)
            im->min_written = im->ts[im->cur0];
        if (!im->written || im->ts[im->n - 1] > im->max_written)
            im->max_written = im->ts[im->n - 1];
        im->written = true;
    }
    im->n = im->cur0 = 0;
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
    const char *name = esc(im->name, e1, sizeof e1);
    int seen = name_seen(im, im->name);
    if (seen < 0)
        return IFAIL(im, "out of memory");
    if (seen)
        return IFAIL(im, "the stream holds series %s twice: it is damaged", name);
    /* a batch holds one value type */
    if (im->n && im->btype != (tsdb_type)type && im_insert(im))
        return -1;
    im->type = im->btype = (tsdb_type)type;
    im->cur0 = im->n;
    im->in_series = true;
    im->skip = im->created = im->written = im->any_rows = im->replacing = im->deleted = false;
    im->s_rows = im->s_nulls = im->s_expired = 0;
    im->s_chunks = 0;
    im->keep = im->no_retention ? 0 : im->retention;
    codes_clear(im);
    if (!im->db)
        return im_volume(im, name);

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
        im->id = id;
        im->skip = im->existing == EXIST_SKIP;
        if (im->skip) {
            im->keep = 0;
        } else if (im->existing == EXIST_MERGE) {
            int64_t keep = 0;
            im->keep = tsdb_get_retention(im->db, id, &keep) == 0 ? keep : 0;
        } else {
            im->replacing = true;
            /* the series' rows start a batch of their own, so that they can replace the range in one call */
            if (im->apply && im->n && im_insert(im))
                return -1;
            if (im->apply && !im->no_retention && (rc = tsdb_set_retention(im->db, id, im->retention)) != 0)
                return IFAIL(im, "series %s: setting its retention: %s", name, tsdb_strerror(rc));
        }
        return 0;
    }
    if (rc != TSDB_ERR_NOTFOUND)
        return IFAIL(im, "series %s: %s", name, tsdb_strerror(rc));
    if (im_volume(im, name))
        return -1;
    if (!im->apply)
        return 0;
    const char *path = im->vpath;
    uint32_t vol;
    if (path[0] && !is_volume(im->db, path) && (rc = tsdb_volume_add(im->db, path, &vol)) != 0)
        return IFAIL(im, "series %s: adding volume %s: %s", name, esc(path, e2, sizeof e2), tsdb_strerror(rc));
    if ((rc = tsdb_series_create(im->db, im->name, im->type, path[0] ? path : NULL, &im->id)) != 0)
        return IFAIL(im, "creating series %s: %s", name, tsdb_strerror(rc));
    im->created = true;
    im->t_created++;
    if (im->keep && (rc = tsdb_set_retention(im->db, im->id, im->keep)) != 0)
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
        uint32_t *slot = code_slot_of(im, code, true);
        if (!slot) {
            free(name);
            return IFAIL(im, "out of memory");
        }
        if (*slot) {
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
        *slot = target;
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
    int64_t cutoff = im->keep ? im->now_ms - im->keep : INT64_MIN;
    uint64_t z, expired = 0;
    for (uint32_t i = 0; i < rows; i++) {
        if (!take_varint(&r, &z))
            return IFAIL(im, "record %u (rows) is damaged", im->seq);
        int64_t t = i == 0 ? (int64_t)(z >> 1) ^ -(int64_t)(z & 1) : (int64_t)((uint64_t)ts[i - 1] + z);
        if ((i ? z == 0 || t <= ts[i - 1] : im->any_rows && t <= im->last_ts) || t < im->from || t >= im->to ||
            t == TSDB_TS_MAX)
            return IFAIL(im, "record %u (rows): times out of order or out of range: the stream is damaged", im->seq);
        ts[i] = t;
        expired += t < cutoff;
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
        im->sid[d] = im->id;
        if (!ok) {
            nulls++;
            continue;
        }
        const uint8_t *v = val + (size_t)i * w;
        uint32_t u32, *slot;
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
            if (u32 == 0 || !(slot = code_slot_of(im, u32, false)))
                return IFAIL(im, "record %u (rows) has a state code without a name: the stream is damaged",
                             im->seq);
            memcpy((uint8_t *)v, slot, 4);
            break;
        }
        if (bad)
            return IFAIL(im, "record %u (rows) holds a value reserved for null: the stream is damaged", im->seq);
    }
    /* the values were copied in the stream's byte order, little-endian, which is the host's (x86-64, arm64) */
    im->s_rows += rows;
    im->s_nulls += nulls;
    im->s_expired += expired;
    im->s_chunks++;
    if (!im->apply || im->skip)
        return 0; /* checked; nothing to write */
    if (nulls)
        im->nulls = true;
    im->n += rows;
    if (im->n >= im->batch_rows)
        return im_insert(im);
    return 0;
}

static int im_end(importer *im, cur *c)
{
    char e[256];
    if (!im->in_series)
        return IFAIL(im, "record %u (end of series) is out of place: the stream is damaged", im->seq);
    uint64_t rows = take_u64(c), nulls = take_u64(c);
    uint32_t chunks = take_u32(c);
    if (c->bad || c->p != c->end || rows != im->s_rows || nulls != im->s_nulls || chunks != im->s_chunks)
        return IFAIL(im, "series %s: the stream says %" PRIu64 " rows, %" PRIu64 " nulls in %u chunks, but holds %"
                         PRIu64 ", %" PRIu64 " in %u: it is damaged", esc(im->name, e, sizeof e), rows, nulls,
                     chunks, im->s_rows, im->s_nulls, im->s_chunks);
    if (im->apply && im->replacing && !im->deleted) { /* the whole series in one batch: replaced atomically */
        tsdb_values v = {im->type, im->val + im->cur0 * type_width[im->type], NULL};
        if (im->nulls) /* cur0 is 0 here: the batch was written before the series began */
            v.valid = im->valid;
        int rc = tsdb_replace_range(im->db, im->id, im->from, im->to, im->ts + im->cur0, &v, im->n - im->cur0);
        if (rc)
            return IFAIL(im, "series %s: replacing its values: %s", esc(im->name, e, sizeof e), tsdb_strerror(rc));
        im->t_written_rows += im->n - im->cur0;
        im->n = im->cur0;
        im->nulls = false;
    }
    if (im->report && im->s_expired) {
        if (im->t_expired_series < 20)
            fprintf(stderr, "tsdb: import: warning: series %s: %" PRIu64 " of its %" PRIu64 " rows are older than "
                            "its retention (%" PRId64 " ms) and will not be visible (--no-retention imports them "
                            "without the stream's retention)\n", esc(im->name, e, sizeof e), im->s_expired, rows,
                    im->keep);
        im->t_expired_series++;
        im->t_expired += im->s_expired;
    }
    im->in_series = false;
    im->cur0 = im->n; /* the series is whole: its rows in the batch may be written with the next ones */
    im->t_series++;
    im->t_rows += rows;
    if (im->skip)
        im->t_skipped++;
    return 0;
}

/* One pass over the stream (after its magic, when magic_read). */
static int im_pass(importer *im, bool magic_read)
{
    char e[256];
    if (im_header(im, magic_read))
        return -1;
    im->seq = 0;
    im->in_series = false;
    im->n = im->cur0 = 0;
    im->nulls = false;
    im->names_n = 0;
    if (im->names)
        memset(im->names, 0, im->names_cap * sizeof *im->names);
    im->t_series = im->t_rows = im->t_created = im->t_skipped = im->t_written_rows = 0;
    im->t_expired = im->t_expired_series = 0;
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
        if (h[1] & ~REC_F_OPTIONAL)
            return IFAIL(im, "record %u uses features this tsdb does not know (flags %#x): import it with a newer "
                             "tsdb", im->seq, h[1]);
        cur c = {im->rec.p, im->rec.p + len, false};
        int rc = 0;
        switch (h[0]) {
        case REC_SERIES:
            if (im->in_series)
                return IFAIL(im, "series %s has no end record: the stream is damaged", esc(im->name, e, sizeof e));
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
                return IFAIL(im, "series %s has no end record: the stream is damaged", esc(im->name, e, sizeof e));
            if (c.bad || series != im->t_series || rows != im->t_rows || records != im->seq)
                return IFAIL(im, "the trailer does not match the stream (%" PRIu64 " series, %" PRIu64
                                 " rows): it is damaged", series, rows);
            if (fgetc(im->in) != EOF)
                return IFAIL(im, "bytes follow the trailer at byte %" PRIu64 ": the stream is damaged", im->off);
            return im_insert(im);
        }
        default:
            if (!(h[1] & REC_F_OPTIONAL))
                return IFAIL(im, "record %u has kind %u, which this tsdb does not know: import it with a newer tsdb "
                                 "(the stream was written by tsdb %s)", im->seq, h[0],
                             esc(im->src_version, e, sizeof e));
        }
        if (rc)
            return -1;
        im->seq++;
    }
}

static void import_usage(void)
{
    fprintf(stderr, "usage: tsdb <dir> import [FILE|-] [--skip-existing|--merge|--replace] [--volume OLD=NEW]... "
                    "[--volume-root OLD=NEW]... [--create-volumes] [--no-retention] [--no-verify] "
                    "[--batch-rows N]\n"
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

/* The target does not exist yet or is an empty directory. */
static bool dir_is_new(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d)
        return errno == ENOENT;
    struct dirent *e;
    bool empty = true;
    while (empty && (e = readdir(d)))
        empty = strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0;
    closedir(d);
    return empty;
}

/* Makes a database written with fsync = 0 durable: every file system it lives on (syncfs reports the writeback
 * errors of each since Linux 5.8), its directories, and the entry of its root in the parent directory. Says what
 * failed; the import has failed then. */
static bool sync_all(const char *dir, char **vols, uint32_t nv)
{
    char e[256];
    for (uint32_t i = 0; i <= nv; i++) {
        const char *p = i < nv ? vols[i] : dir;
        int fd = open(p, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        const char *what = fd < 0 ? "open" : syncfs(fd) != 0 ? "syncfs" : fsync(fd) != 0 ? "fsync" : NULL;
        int err = errno;
        if (fd >= 0)
            close(fd);
        if (what) {
            fprintf(stderr, "tsdb: import: %s %s: %s\n", what, esc(p, e, sizeof e), strerror(err));
            return false;
        }
    }
    char *parent = strdup(dir), *slash = parent ? strrchr(parent, '/') : NULL;
    if (slash)
        slash[slash == parent] = 0;
    int fd = !parent ? -1 : open(slash ? parent : ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    bool ok = fd >= 0 && fsync(fd) == 0;
    int err = errno;
    if (!ok)
        fprintf(stderr, "tsdb: import: fsync %s: %s\n", parent ? esc(slash ? parent : ".", e, sizeof e) : "the parent "
                "directory", strerror(err));
    if (fd >= 0)
        close(fd);
    free(parent);
    return ok;
}

/* --volume and --volume-root: OLD=NEW, both normalised ("/a/b/" is "/a/b"); NEW may be empty for --volume (the
 * database directory). */
static bool add_map(vmap *m, int *n, const char *v, bool root)
{
    const char *eq = strchr(v, '=');
    if (!eq || eq == v)
        return false;
    char *from = strndup(v, (size_t)(eq - v)), *to = strdup(eq + 1);
    if (!from || !to || !vpath_valid(vpath_norm(from)) || (to[0] && !vpath_valid(vpath_norm(to))) ||
        (root && !to[0])) {
        fprintf(stderr, "tsdb: import: %s %s: OLD and NEW must be absolute paths without . or .. components%s\n",
                root ? "--volume-root" : "--volume", v, root ? "" : " (NEW may be empty: the database directory)");
        free(from);
        free(to);
        exit(2);
    }
    m[(*n)++] = (vmap){from, to};
    return true;
}

/* Everything the importer owns is freed by dump_import_main, so that no return path can leak it. */
static int import_run(const char *dir, int argc, char **argv, FILE **csv, importer *im)
{
    const char *in_name = NULL;
    bool check = false, verify = true, dump_opts = false;
    im->maps = calloc((size_t)argc + 1, sizeof *im->maps);
    im->roots = calloc((size_t)argc + 1, sizeof *im->roots);
    if (!im->maps || !im->roots) {
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
        else if (strcmp(a, "--no-retention") == 0)
            im->no_retention = true;
        else if (strcmp(a, "--no-verify") == 0)
            verify = false;
        else if (strcmp(a, "--volume") == 0 && v && add_map(im->maps, &im->nmaps, v, false))
            i++;
        else if (strcmp(a, "--volume-root") == 0 && v && add_map(im->roots, &im->nroots, v, true))
            i++;
        else if (strcmp(a, "--batch-rows") == 0 && v) {
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
    bool named = in_name && strcmp(in_name, "-") != 0;
    if (named) {
        if (!(in = fopen(in_name, "rb"))) {
            fprintf(stderr, "tsdb: import: %s: %s\n", in_name, strerror(errno));
            return 1;
        }
    } else {
        in_name = "standard input"; /* a terminal: CSV typed in ends with ^D (the first 16 bytes are read first) */
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
    pre->n = fread(pre->head, 1, 16, in);
    /* An empty input imports nothing, and a backup cut to a few bytes must not "restore" as such */
    if (pre->n == 0 || (named && pre->n < 16)) {
        if (ferror(in))
            fprintf(stderr, "tsdb: import: %s: %s\n", in_name, strerror(errno));
        else
            fprintf(stderr, "tsdb: import: %s %s: not an export stream or CSV\n", in_name,
                    pre->n ? "is shorter than a stream header" : "is empty");
        free(pre);
        if (in != stdin)
            fclose(in);
        return 1;
    }
    /* A stream has its version, with NUL bytes, in bytes 8..15; CSV text never has NUL bytes, so a CSV line that
     * happens to start with the magic is still CSV, and a stream whose magic is damaged is refused, not read as
     * CSV */
    bool stream = pre->n == 16 && memcmp(pre->head, DUMP_MAGIC, 8) == 0 && memchr(pre->head + 8, 0, 8);
    if (!stream) {
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
    if (im->existing == EXIST_REPLACE && !check && (!seekable || !verify)) {
        fprintf(stderr, "tsdb: import: --replace %s: it deletes what each series holds in the stream's time range, "
                        "so the whole stream must be checked before anything is deleted, and a pipe cannot be read "
                        "twice; save the stream to a file and import that, or use --merge\n",
                !verify ? "cannot be used with --no-verify" : "needs the stream in a file");
        free(pre);
        if (in != stdin)
            fclose(in);
        return 2;
    }
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
    struct timespec tsn; /* retention's "now": the wall clock, as the engine's */
    clock_gettime(CLOCK_REALTIME, &tsn);
    im->now_ms = (int64_t)tsn.tv_sec * 1000 + tsn.tv_nsec / 1000000;
    double t0 = now_s();
    int ret = 1;
    tsdb *db = NULL; /* none with --check: every series is taken as new and every volume as present */
    bool fresh = false;
    if (!check) {
        tsdb_options o;
        tsdb_options_init(&o);
        o.create_volumes = 0; /* volumes only through --create-volumes */
        /* Into a new database, nothing is synced file by file (two syncs per new series and one per part
         * otherwise: they bound small series to ~700 per second); every file system of it is synced once at the
         * end instead. A power loss before that leaves a database that fails its checks at open: it held
         * nothing else, so it is removed and the import rerun. An existing database keeps every sync. */
        fresh = dir_is_new(dir);
        if (fresh)
            o.fsync = 0;
        /* No WAL: rows of small series collect in the memtable and reach the disk as parts, synced, at the
         * flush at the end (large batches go straight to synced parts). A crash before that loses rows of this
         * import only: rerun it with --merge, which writes the same rows again. */
        o.wal_sync_ms = -1;
        int rc = tsdb_open(dir, &o, &db);
        if (rc) {
            fprintf(stderr, "tsdb: %s: %s\n", dir, tsdb_strerror(rc));
            if (in != stdin)
                fclose(in);
            return 1;
        }
        if (!realpath(dir, im->root))
            snprintf(im->root, sizeof im->root, "%s", tsdb_volume_path(db, 0));
    }
    im->db = db;
    size_t cap = im->batch_rows + DUMP_CHUNK_MAX;
    im->ts = malloc(cap * 8);
    im->val = malloc(cap * 8);
    im->sid = malloc(cap * 4);
    im->valid = malloc(cap / 8 + 1);
    if (!im->ts || !im->val || !im->sid || !im->valid || !im->zd) {
        fprintf(stderr, "tsdb: out of memory\n");
        if (db)
            tsdb_close(db);
        if (in != stdin)
            fclose(in);
        return 1;
    }
    char e[256], e2[256];
    int rc = 0;
    bool verified = false;
    if (check || (seekable && verify)) {
        im->apply = false;
        im->report = true;
        rc = im_pass(im, true);
        verified = !rc;
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
        im->report = !verified;
        rc = im_pass(im, !verified);
    }
    if (im->t_expired_series > 20)
        fprintf(stderr, "tsdb: import: warning: %" PRIu64 " series in all have %" PRIu64 " rows older than their "
                        "retention\n", im->t_expired_series, im->t_expired);
    if (rc) {
        fprintf(stderr, "tsdb: import: %s: %s\n", in_name, im->err);
        if (im->apply) {
            /* the series read whole are written; the rows of the one that failed are not */
            uint64_t whole = im->t_series;
            size_t keep = im->in_series ? im->cur0 : im->n;
            if (keep && im_write(im, keep)) {
                fprintf(stderr, "tsdb: import: %s\n", im->err);
                whole = 0;
            }
            if (im->in_series && im->created) {
                const char *name = esc(im->name, e, sizeof e);
                int drc = im->written ? tsdb_delete_range(db, im->id, im->min_written, im->max_written + 1) : 0;
                if (drc)
                    fprintf(stderr, "tsdb: import: series %s, which this import created, could not be emptied "
                                    "again: %s\n", name, tsdb_strerror(drc));
                else
                    fprintf(stderr, "tsdb: import: series %s, which this import created, is left empty: rerun "
                                    "the import with --merge\n", name);
            } else if (im->in_series && (im->written || im->deleted)) {
                fprintf(stderr, "tsdb: import: series %s is partly imported: rerun the import with --merge%s\n",
                        esc(im->name, e2, sizeof e2), im->replacing ? " or --replace" : "");
            }
            fprintf(stderr, "tsdb: import: %" PRIu64 " series before it were imported completely\n", whole);
        } else if (!check) {
            fprintf(stderr, "tsdb: import: nothing was written\n");
        }
    } else if (check) {
        fprintf(stderr, "%s: a whole tsdb export stream, version %u, from tsdb %s (format %u): %" PRIu64 " series, %"
                        PRIu64 " rows\n", in_name, im->version, esc(im->src_version, e, sizeof e), im->src_format,
                im->t_series, im->t_rows);
        ret = 0;
    } else if ((rc = tsdb_flush(db)) != 0) {
        fprintf(stderr, "tsdb: import: writing the last rows: %s\n", tsdb_strerror(rc));
    } else {
        ret = 0; /* reported below, once it is on disk */
    }
    if (db) {
        /* the volumes' paths outlive the handle: they are synced after the close */
        char **vols = NULL;
        uint32_t nv = fresh ? tsdb_volume_count(db) : 0;
        bool oom = nv && !(vols = calloc(nv, sizeof *vols));
        for (uint32_t i = 0; i < nv && !oom; i++)
            oom = !(vols[i] = strdup(tsdb_volume_path(db, i)));
        int crc = tsdb_close(db); /* flushes what the error path left in the memtable */
        if (crc) {
            fprintf(stderr, "tsdb: close: %s\n", tsdb_strerror(crc));
            ret = 1;
        }
        if (fresh && (oom || !sync_all(dir, vols, nv))) {
            if (oom)
                fprintf(stderr, "tsdb: import: out of memory\n");
            fprintf(stderr, "tsdb: import: %s: the new database is not on disk: delete it and import again\n",
                    esc(dir, e, sizeof e));
            ret = 1;
        }
        for (uint32_t i = 0; vols && i < nv; i++)
            free(vols[i]);
        free(vols);
        if (ret == 0 && !check) {
            double dt = now_s() - t0;
            fprintf(stderr, "imported %" PRIu64 " series (%" PRIu64 " created, %" PRIu64 " skipped), %" PRIu64
                            " rows in %.2f s (%.1f M rows/s)\n", im->t_series, im->t_created, im->t_skipped,
                    im->t_written_rows, dt, dt > 0 ? (double)im->t_written_rows / dt / 1e6 : 0.0);
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
    free(im.sid);
    free(im.valid);
    free(im.codes);
    free(im.names);
    free(im.rec.p);
    free(im.raw.p);
    free(im.name);
    free(im.path);
    free(im.src_version);
    for (int i = 0; i < im.nmaps; i++) {
        free(im.maps[i].from);
        free(im.maps[i].to);
    }
    for (int i = 0; i < im.nroots; i++) {
        free(im.roots[i].from);
        free(im.roots[i].to);
    }
    for (int i = 0; i < im.nvols_ok; i++)
        free(im.vols_ok[i]);
    free(im.vols_ok);
    free(im.maps);
    free(im.roots);
    return ret;
}
