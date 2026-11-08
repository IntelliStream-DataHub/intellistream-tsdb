#ifndef TSDB_INTERNAL_H
#define TSDB_INTERNAL_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "tsdb.h"

/* One storage directory (a volume). The public tsdb (volumes.c) routes to these. */
typedef struct eng eng;

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#define ZSTD_STATIC_LINKING_ONLY /* ZSTD_f_zstd1_magicless: stable since zstd 1.3, still in the experimental API */
#include <zstd.h>

#include "compat.h" /* macOS shims; nothing on Linux */
#include "marker.h" /* WAL marker payloads (format 9) */

#include "levels.h" /* before the declarations: renames them in a per-CPU-level compile */

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

/* 3: checksummed part header and footer, WAL records and dictionary records;
 * 4: time-weighted statistics in granule entries (136 bytes), range tombstones
 * between the index and the footer; 5: the spread (m2 or the sum of squares) in
 * granule entries (160 bytes), tombstones as in 4; 6: a 7-bit codec kind in block headers; 7: partition
 * classes (a partition key is 4 x its first base partition + its class, CLASSES holds each series' class
 * history) and zstd frames without magic number or content size, and the granule index bit-packed (gindex.c),
 * its length in the footer (64 bytes); 8: 64-bit series ids (volume + 1) << 32 | local, no CATALOG, each
 * directory's META says which volume it is ("volume=N"; the files inside a volume are as in 7); 9: the WAL record
 * has a 32-byte header with fno and commit_ms, and markers (docs/design-cdc.md 2.1), the part header 48 bytes (the
 * position range fno_lo/fno_hi and written_ms), each volume an FNO file (the durable bound of the numbers, fno.c),
 * META may say requires= and parts_v8=1 (a database `tsdb DIR upgrade` carried over from 8: its parts keep their
 * 24-byte headers until merges rewrite them). Databases of older versions are refused (upgrade, or export and
 * import). */
#define FORMAT_VERSION 9
#define PREV_FORMAT_VERSION 8
#define PART_HDR_V8 24 /* the header of a part written at format 8 */
/* Planted mutations: a test sets eng_test_mutant (tests link the engine) and must then fail, to show that it
 * catches the bug; nothing in the library or the server ever sets it. */
enum { MUT_NONE = 0, MUT_V8_HASH48, MUT_FNO_MANIFEST_ONLY, MUT_FNO_REUSE, MUT_MARKER_ROWS, MUT_FNO_NOSYNC };
extern int eng_test_mutant;
#define MAX_GRANULE_ROWS 65536
#define MAX_MERGE_PARTS 100
#define PARTS_TO_DELAY_INSERT 300 /* per partition, like parts_to_delay_insert */

/* A block's zstd frame has neither the magic number nor the content size (format 7): the block header holds the
 * inner length already. -7 bytes per block. Inline, so each CPU level of the codecs has its own copy. */
static inline size_t zstd_pack(ZSTD_CCtx *z, void *dst, size_t cap, const void *src, size_t len, int level)
{
    ZSTD_CCtx_setParameter(z, ZSTD_c_compressionLevel, level);
    return ZSTD_compress2(z, dst, cap, src, len);
}
static inline uint32_t load32(const void *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline void store32(void *p, uint32_t v) { memcpy(p, &v, 4); }

static inline int64_t floor_div(int64_t a, int64_t b) /* b > 0 */
{
    int64_t q = a / b;
    return (a % b < 0) ? q - 1 : q;
}

static inline int64_t sat_add(int64_t a, int64_t b)
{
    int64_t r;
    return __builtin_add_overflow(a, b, &r) ? (b > 0 ? INT64_MAX : INT64_MIN) : r;
}

static inline int64_t sat_mul(int64_t a, int64_t b)
{
    int64_t r;
    return __builtin_mul_overflow(a, b, &r) ? (((a < 0) != (b < 0)) ? INT64_MIN : INT64_MAX) : r;
}

/* ---- value types (values.c) ----
 * Values travel through the engine in slots of type_width(type) bytes:
 * float32 bits, uint32 0/1 for bool, uint32 codes for state (4 bytes);
 * float64 bits and int64 (8 bytes). A null is a reserved value per type. */
#define NTYPES 5
#define NULL_F32 TSDB_NULL_F32_BITS
#define NULL_F64 TSDB_NULL_F64_BITS
#define NULL_I64 INT64_MIN
#define NULL_BOOL 2u
#define NULL_STATE 0u

static inline size_t type_width(int t) { return t == TSDB_FLOAT64 || t == TSDB_INT64 ? 8 : 4; }
static inline bool type_valid(int t) { return t >= TSDB_FLOAT32 && t <= TSDB_STATE; }
static inline bool type_is_int(int t) { return t == TSDB_INT64 || t == TSDB_BOOL || t == TSDB_STATE; }
/* the null of type t as slot bits (zero-extended for 4-byte types) */
static inline uint64_t type_null(int t)
{
    switch (t) {
    case TSDB_FLOAT32: return NULL_F32;
    case TSDB_FLOAT64: return NULL_F64;
    case TSDB_INT64: return (uint64_t)NULL_I64;
    case TSDB_BOOL: return NULL_BOOL;
    default: return NULL_STATE;
    }
}
/* slot i of v (width w) as zero-extended bits */
static inline uint64_t slot_get(const void *v, size_t w, size_t i)
{
    if (w == 4) {
        uint32_t x;
        memcpy(&x, (const uint8_t *)v + 4 * i, 4);
        return x;
    }
    uint64_t x;
    memcpy(&x, (const uint8_t *)v + 8 * i, 8);
    return x;
}
static inline void slot_put(void *v, size_t w, size_t i, uint64_t x)
{
    if (w == 4) {
        uint32_t y = (uint32_t)x;
        memcpy((uint8_t *)v + 4 * i, &y, 4);
    } else {
        memcpy((uint8_t *)v + 8 * i, &x, 8);
    }
}

/* ---- query statistics (volumes.c) ----
 * Counters of the query a thread works for: the thread that called the query
 * and, while they help it, the pool's workers. NULL elsewhere (merges). */
typedef struct qstats {
    atomic_uint_fast64_t rows_decoded, rows_from_stats, memtable_rows, bytes_read;
    atomic_uint_fast64_t parts, granules, granules_from_stats;
} qstats;
extern __thread qstats *cur_qstats;
#define QS_ADD(field, v)                                                                 \
    do {                                                                                 \
        qstats *q_ = cur_qstats;                                                         \
        if (q_)                                                                          \
            atomic_fetch_add_explicit(&q_->field, (uint64_t)(v), memory_order_relaxed); \
    } while (0)

/* ---- hashing (xxhash64) ---- */
uint64_t xxh64(const void *data, size_t len, uint64_t seed);

/* ---- codecs ---- */
#define BLK_DD 1      /* DoubleDelta over int64 */
#define BLK_GORILLA 2 /* Gorilla over float32 */
#define BLK_DECIMAL 3 /* float32 that are exact decimals: scaled integer deltas */
#define BLK_CHIMP 4     /* Chimp over float32 */
#define BLK_DELTA32 5   /* delta of the float32 bit patterns */
#define BLK_BYTESPLIT 6 /* byte stream split of float32 */
#define BLK_TS_DELTA 7  /* int64 deltas, frame of reference */
#define BLK_TS_GRID 8   /* int64 residuals from a regular grid (clock jitter) */
#define BLK_ALPRD 9     /* float32 or float64 split into dictionary-coded high bits and packed low bits */
#define BLK_DECIMAL_BP 10 /* fixed-point integers, bit-packed zigzag deltas */
#define BLK_F64_DECIMAL 11 /* float64 that are exact decimals: integers as in BLK_INT */
#define BLK_XOR64 12       /* Gorilla over float64 */
#define BLK_BYTESPLIT64 13 /* byte stream split of float64 */
#define BLK_INT 14         /* int64: zigzag deltas bit-packed per 1024 values */
#define BLK_DELTA_BP 15    /* float32 or float64: zigzag deltas of the bit patterns, bit-packed per 1024 */
#define BLK_DICT 16        /* float32 or float64: sorted distinct values, ranks as bit-packed zigzag deltas */
#define BLK_F32_WIDE 17    /* float64 that are all float32 values: a float32 block (never ZSTD-wrapped itself) */
/* A kind names the transform; the series' type gives the width where both exist (BLK_ALPRD, BLK_DELTA_BP,
 * BLK_DICT). The header byte holds the kind in its low 7 bits (format 6; 4 bits before). */
#define BLK_KIND_MASK 0x7f
#define BLK_ZSTD 0x80 /* the transform output is additionally ZSTD compressed */
#define BLK_SLACK 32  /* decoders may read up to this many bytes past a block */

/* What ZSTD must save of `inner` transform bytes for a block to keep it, since decoding costs too: 1/8 for timestamp
 * blocks, 1/32 for value blocks. Every query that reads rows decodes the timestamps, and the timestamp blocks ZSTD
 * shrank by 1/32 to 1/8 (jittered clocks) were 0.65% of the bytes but 17-22% of a minute-bucket query on one thread
 * (the user's decision, 2026-11-02). Integer arithmetic on the kind: every CPU level keeps the same blocks. */
static inline size_t blk_zstd_min_saving(int kind, size_t inner)
{
    bool ts = kind == BLK_DD || kind == BLK_TS_DELTA || kind == BLK_TS_GRID;
    return inner / (ts ? 8 : 32);
}

typedef struct codec_ctx {
    struct codec_ctx *next; /* free list in eng */
    ZSTD_CCtx *cctx;
    ZSTD_DCtx *dctx;
    uint8_t *scratch;
    size_t scratch_cap;
    uint8_t *alt; /* candidate block while choosing a value encoding */
    size_t alt_cap;
    uint8_t *work; /* fixed point's integers and the dictionary's table of them (fixed_or_dict) */
    size_t work_cap;
    uint8_t *narrow; /* float64 values as float32 (BLK_F32_WIDE), both ways */
    size_t narrow_cap;
    int effort; /* CODEC_FAST or CODEC_MERGE, an input of AUTO like the values: the part writer sets it */
} codec_ctx;

/* How hard AUTO looks. Inserts and memtable flushes write CODEC_FAST: the candidates that pay for themselves
 * at insert speed. Merges re-encode every row they write (granules of parts no merge has written yet included,
 * see merge_parts) and add the candidates that cost several times the encode: the dictionary. Decoding does
 * not depend on it; the same values and effort give the same bytes on every CPU level. */
#define CODEC_FAST 0
#define CODEC_MERGE 1

size_t dd_bound(size_t n);
size_t gorilla_bound(size_t n);
size_t dd_encode(const int64_t *in, size_t n, uint8_t *out);
int dd_decode(const uint8_t *in, size_t len, size_t n, int64_t *out);
size_t gorilla_encode(const uint32_t *in, size_t n, uint8_t *out);
int gorilla_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t chimp_bound(size_t n);
size_t chimp_encode(const uint32_t *in, size_t n, uint8_t *out);
int chimp_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t chimp128_bound(size_t n);
size_t chimp128_encode(const uint32_t *in, size_t n, uint8_t *out);
int chimp128_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t patas_bound(size_t n);
size_t patas_encode(const uint32_t *in, size_t n, uint8_t *out);
int patas_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t delta32_encode(const uint32_t *in, size_t n, uint8_t *out);
int delta32_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t delta_bp32_bound(size_t n);
size_t delta_bp32_encode(const uint32_t *in, size_t n, uint8_t *out);
int delta_bp32_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t bytesplit_encode(const uint32_t *in, size_t n, uint8_t *out);
int bytesplit_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t tsfor_bound(size_t n);
size_t ts_grid_encode(const int64_t *ts, size_t n, uint8_t *out);
int ts_grid_decode(const uint8_t *in, size_t len, size_t n, int64_t *out);
size_t ts_delta_encode(const int64_t *ts, size_t n, uint8_t *out);
int ts_delta_decode(const uint8_t *in, size_t len, size_t n, int64_t *out);
size_t decimal_bp_bound(size_t n);
size_t decimal_bp_encode(const float *in, size_t n, uint8_t *out);
int decimal_bp_decode(const uint8_t *in, size_t len, size_t n, float *out);
size_t alp_bound(size_t n);
size_t alp_encode(const float *in, size_t n, uint8_t *out); /* 0 if the values are not decimal-like */
int alp_decode(const uint8_t *in, size_t len, size_t n, float *out);
size_t alprd_bound(size_t n);
size_t alprd_encode(const uint32_t *in, size_t n, uint8_t *out);
int alprd_decode(const uint8_t *in, size_t len, size_t n, uint32_t *out);
size_t alprd64_bound(size_t n);
size_t alprd64_encode(const uint64_t *in, size_t n, uint8_t *out);
int alprd64_decode(const uint8_t *in, size_t len, size_t n, uint64_t *out);
size_t decimal_bound(size_t n);
size_t decimal_encode(const float *in, size_t n, uint8_t *out); /* 0 if not applicable */
int decimal_decode(const uint8_t *in, size_t len, size_t n, float *out);

codec_ctx *codec_new(void);
void codec_free(codec_ctx *c);
size_t block_bound(size_t n);
size_t block_encode_ts(codec_ctx *c, int level, const int64_t *ts, size_t n, uint8_t *out);
/* Both return 0 when out of memory. */
/* value_codec: TSDB_VALUES_*, or VALUES_FIXED_ONLY for compact fixed point (16 bits a value at most) or nothing */
#define VALUES_FIXED_ONLY (-1)
size_t block_encode_f32(codec_ctx *c, int level, int value_codec, const float *v, size_t n, uint8_t *out);
int block_decode_ts(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int64_t *out);
int block_decode_f32(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, float *out);
/* any value type, in slots of type_width(type) (codec_typed.c) */
size_t block_encode_vals(codec_ctx *c, int level, int value_codec, int type, const void *v, size_t n, uint8_t *out);
int block_decode_vals(codec_ctx *c, int type, const uint8_t *blk, size_t len, size_t n, void *out);
/* shared by the codec files */
int codec_scratch_reserve(codec_ctx *c, size_t need);
size_t block_wrap(codec_ctx *c, int level, int kind, size_t inner, uint8_t *out);
size_t block_wrap_src(codec_ctx *c, int level, int kind, const uint8_t *src, size_t inner, uint8_t *out);
int codec_alt_reserve(codec_ctx *c, size_t need);
int codec_work_reserve(codec_ctx *c, size_t need);
/* a block's transform kind and its (decompressed) transform bytes */
int block_open(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int *kind, const uint8_t **p, size_t *plen);
/* AUTO's choice among transforms (codec_typed.c): enc encodes n values with one kind, 0 if not applicable */
typedef size_t (*codec_kind_fn)(int kind, const void *v, size_t n, uint8_t *out);
size_t auto_pick(codec_ctx *c, int level, const int *kinds, size_t nk, codec_kind_fn enc, const void *v, size_t n,
                 size_t width, int pre_kind, size_t pre_inner, size_t have, double all_bits, double surprise,
                 uint8_t *out);
size_t dict_block(codec_ctx *c, int level, const void *v, size_t n, int width, uint8_t *out);
size_t int_bp_bound(size_t n);
size_t dict_bound(size_t n);
size_t dict_encode(const void *v, size_t n, int width, uint8_t *out);
int dict_decode(const uint8_t *in, size_t len, size_t n, int width, void *out);
#define DICT_SPAN 65536 /* fixed point whose integers span more goes without a dictionary */
#define DICT_WORK(n) (16 * (size_t)(n) / 8 + 8 * (size_t)(n) + 2 * (size_t)DICT_SPAN + 16) /* c->work: fixed_or_dict */
/* q: the integers of pre_kind's fixed point, one per value (NULL if not kept), nexc of them exceptions */
size_t fixed_or_dict(codec_ctx *c, int level, int pre_kind, size_t pre_inner, const int64_t *q, size_t nexc,
                     const void *v, size_t n, int width, uint8_t *out);
/* as decimal_bp_encode / f64_decimal_encode, keeping the integers in keep[n] (its exceptions in *nexc) */
size_t decimal_bp_encode_keep(const float *in, size_t n, uint8_t *out, int64_t *keep, size_t *nexc);
size_t f64_decimal_encode_keep(const double *in, size_t n, uint8_t *out, int64_t *keep, size_t *nexc);
size_t int_bp_encode(const int64_t *in, size_t n, uint8_t *out);
int int_bp_decode(const uint8_t *in, size_t len, size_t n, int64_t *out);
size_t f64_decimal_bound(size_t n);
size_t f64_decimal_encode(const double *in, size_t n, uint8_t *out); /* 0 if not decimal */
int f64_decimal_decode(const uint8_t *in, size_t len, size_t n, double *out);
size_t xor64_bound(size_t n);
size_t xor64_encode(const uint64_t *in, size_t n, uint8_t *out);
int xor64_decode(const uint8_t *in, size_t len, size_t n, uint64_t *out);
size_t delta_bp64_bound(size_t n);
size_t delta_bp64_encode(const uint64_t *in, size_t n, uint8_t *out);
int delta_bp64_decode(const uint8_t *in, size_t len, size_t n, uint64_t *out);

/* ---- SIMD kernels (simd.c), dispatched once at startup ---- */
/* The statistics of one bucket. Only the half of the type is in use: buckets
 * of tiny intervals are many, so the state stays small.
 *
 * The spread (variance, standard deviation) of floats is kept in the stable
 * form: m2, the sum of squared deviations from the mean (sum / count). A run
 * of values gets its m2 in two passes (the mean, then the squared deviations
 * from it, corrected by their sum), and states combine with the parallel
 * formula of Chan, Golub and LeVeque (m2_merge), so an offset as large as
 * 101325 Pa costs no precision on a spread of +-5 the way a raw sum of squares
 * would (it cancels catastrophically). Integers keep the exact sum of squares
 * instead: 192 bits (isq + isq_hi * 2^128) hold 2^64 rows of int64 squares,
 * and the variance comes from exact integers once, at the end (agg_spread).
 * Those live beside the state, in an agg_dev, which only queries for the
 * spread keep: 48 bytes instead of 80 per bucket are worth 20-30% on buckets
 * of ten rows. Every function taking an agg_dev takes NULL for "no spread". */
typedef struct agg_state {
    uint64_t rows;  /* rows folded, nulls included */
    uint64_t count; /* known values */
    union {
        struct { double sum, min, max; };               /* float32, float64 */
        struct { __int128 isum; int64_t imin, imax; };  /* int64, bool, state */
    };
} agg_state;

typedef struct agg_dev {
    union {
        double m2;                                              /* float32, float64 */
        struct { unsigned __int128 isq; uint64_t isq_hi; };     /* int64, bool, state */
    };
} agg_dev;

/* One NaN for every NaN result: the quiet NaN 0x7ff8000000000000 (as float32,
 * 0x7fc00000). Which payload and sign an addition of NaNs keeps depends on the
 * order of the additions (ADDSD returns its first NaN operand), and that varies
 * with CPU levels, kernels, threads and merges; a result must not. Nor may a
 * result carry a stored value's payload: a signalling NaN with payload 0x1001
 * quiets into TSDB_NULL_F64_BITS. Stored values keep their bits; only results
 * (and granule sums, which are results kept on disk) are canonical. */
#define CANON_NAN_BITS 0x7ff8000000000000ull
static inline double nan_canonical(void)
{
    uint64_t b = CANON_NAN_BITS;
    double d;
    memcpy(&d, &b, 8);
    return d;
}
static inline double canon_nan(double x) { return x == x ? x : nan_canonical(); }

static inline void agg_init(agg_state *a, int type)
{
    a->rows = a->count = 0;
    if (type == TSDB_FLOAT32 || type == TSDB_FLOAT64) {
        a->sum = 0;
        a->min = __builtin_inf();
        a->max = -__builtin_inf();
    } else {
        a->isum = 0;
        a->imin = INT64_MAX;
        a->imax = INT64_MIN;
    }
}

static inline void agg_dev_init(agg_dev *x)
{
    x->isq = 0; /* covers m2 = 0 too */
    x->isq_hi = 0;
}

/* The m2 of two runs together (Chan, Golub and LeVeque 1979): na values with
 * sum sa and m2a, nb values with sum sb and m2b. One order of operations for
 * every caller, so that a combination rounds the same everywhere. A NaN m2 is
 * the canonical one: the payload of a NaN in the sums depends on the kernel. */
static inline double m2_merge(uint64_t na, double sa, double m2a, uint64_t nb, double sb, double m2b)
{
    if (!nb)
        return m2a;
    if (!na)
        return canon_nan(m2b);

    double d = sb / (double)nb - sa / (double)na;
    return canon_nan(m2a + m2b + d * d * ((double)na * (double)nb / ((double)na + (double)nb)));
}


/* Folds a run of c known float values into a: their sum s, and d1 and d2, the
 * sums of their deviations from the run's mean s / c and of the squares of
 * those (the second pass). d2 - d1^2 / c is the corrected two-pass m2 (Chan,
 * Golub and LeVeque 1983): d1 makes up for the rounding of the mean. */
static inline void agg_add_run(agg_state *a, agg_dev *x, uint64_t c, double s, double d1, double d2)
{
    if (!c)
        return;
    double m2 = d2 - d1 * d1 / (double)c;
    if (m2 < 0) /* rounding, with constant data; NaN stays */
        m2 = 0;
    x->m2 = m2_merge(a->count, a->sum, x->m2, c, s, m2);
    a->count += c;
    a->sum += s;
}

/* adds the 192-bit sum of squares (lo + hi * 2^128) to x's */
static inline void isq_add(agg_dev *x, unsigned __int128 lo, uint64_t hi)
{
    x->isq += lo;
    x->isq_hi += hi + (x->isq < lo);
}

/* b (with bx) into a (with ax); ax NULL: without the spread (only queries for it pay for it) */
static inline void agg_combine(agg_state *a, agg_dev *ax, const agg_state *b, const agg_dev *bx, int type)
{
    if (type == TSDB_FLOAT32 || type == TSDB_FLOAT64) {
        if (ax)
            ax->m2 = m2_merge(a->count, a->sum, ax->m2, b->count, b->sum, bx->m2);
        a->sum += b->sum;
        if (b->min < a->min) a->min = b->min;
        if (b->max > a->max) a->max = b->max;
    } else {
        a->isum += b->isum;
        if (b->imin < a->imin) a->imin = b->imin;
        if (b->imax > a->imax) a->imax = b->imax;
        if (ax)
            isq_add(ax, bx->isq, bx->isq_hi);
    }
    a->rows += b->rows;
    a->count += b->count;
}

/* The kernel dispatch (simd.c): hidden, so that -fPIC code reads these without a GOT load on every short fold */
#define SIMD_VAR extern __attribute__((visibility("hidden")))

/* Folds float32 values (no nulls) into a: count, sum (in double), min and max (NaN ignored); with x also
 * m2 into x (a second pass over the values). Queries that do not ask for the spread pass NULL: m2 and the
 * sums of squares are the cost of the spread, and a query of tiny buckets would pay it on every row. */
SIMD_VAR void (*simd_agg_f32)(const float *v, size_t n, agg_state *a, agg_dev *x);
/* The lanes of simd.c's float kernels (their order of additions). A run of fewer than simd_short values goes to
 * simd_small_*: the same bits as the kernels at a fraction of their fixed cost; values.c picks. simd_short is
 * SIMD_SHORT (the scalar short path's limit: the float32 lanes taken 16 at a time) or, where simd_small_* are the
 * SSE/AVX kernels of any length, the length from which the set's wider kernels win; simd_short_f64 the same for
 * float64 (AVX-512: its 512-bit float32 kernels lose to the 128-bit ones up to 127 rows, its float64 ones win from
 * 80). */
#define SIMD_LANES_F32 32
#define SIMD_LANES_F64 16
#define SIMD_SHORT 32
SIMD_VAR size_t simd_short, simd_short_f64;
SIMD_VAR void (*simd_small_f32)(const float *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x);
SIMD_VAR void (*simd_small_f64)(const double *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x);
/* the same with nulls: `nulls` of the n slots hold the type's null (0: none, the kernel above) */
SIMD_VAR void (*simd_fold_f32)(const float *v, size_t n, size_t nulls, agg_state *a, agg_dev *x);
SIMD_VAR void (*simd_fold_f64)(const double *v, size_t n, size_t nulls, agg_state *a, agg_dev *x);
/* int64 with nulls; NULL where only the scalar loop of values.c is left (exact results, so any order) */
SIMD_VAR void (*simd_fold_i64)(const int64_t *v, size_t n, agg_state *a, agg_dev *x);
/* bool and state codes (null: NULL_BOOL, NULL_STATE); NULL as for int64 */
SIMD_VAR void (*simd_fold_u32)(const uint32_t *v, size_t n, uint32_t null, agg_state *a, agg_dev *x);
/* Fewer values than these go to values.c's loops, which beat the integer kernels' setup and lane combine there:
 * int64 without and with the spread, bool and state. Per kernel set (simd_select, from fold-bench); the sums
 * are exact, so either side gives the same bits. */
SIMD_VAR size_t simd_i64_min, simd_i64_dev_min, simd_u32_min;
/* points below which lttb_argmax takes its scalar loop (the same pick), per kernel set */
SIMD_VAR size_t simd_lttb_small;
/* Folds n slots of `type` into a, skipping nulls, with the spread (values.c; granule statistics). */
void agg_fold(int type, const void *v, size_t n, agg_state *a, agg_dev *x);
/* the same, skipping the search for nulls when the caller knows there are none (a granule's count), and
 * the spread unless asked for */
void agg_fold_hint(int type, const void *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x);
struct granule;
/* The statistics of a granule's values; and a granule's statistics as an agg_state. */
void granule_stats(int type, const void *v, size_t n, struct granule *g);
void agg_from_granule(int type, const struct granule *g, agg_state *a, agg_dev *x);
/* The bucket's range, variances and standard deviations (query.c). */
void agg_spread(int type, const agg_state *a, const agg_dev *x, tsdb_spread_bucket *b);
static inline void agg_to_bucket(int type, const agg_state *a, tsdb_bucket *b)
{
    const double nan = nan_canonical();
    b->count = a->count;
    b->sum = 0;
    b->min = b->max = nan;
    b->isum = b->imin = b->imax = 0;
    if (type == TSDB_STATE || a->count == 0)
        return;
    if (type_is_int(type)) {
        b->isum = a->isum > INT64_MAX ? INT64_MAX : a->isum < INT64_MIN ? INT64_MIN : (int64_t)a->isum;
        b->imin = a->imin;
        b->imax = a->imax;
        b->sum = (double)a->isum;
        b->min = (double)a->imin;
        b->max = (double)a->imax;
        return;
    }
    b->sum = canon_nan(a->sum);
    if (a->min <= a->max) { /* else only NaN values; min and max skip NaNs, so neither is one here */
        b->min = a->min;
        b->max = a->max;
    }
}
/* LTTB (query.c): the index of the point with the largest triangle area with a = (ax, ay) and the mean c of
 * the next bucket, the first of equal areas, 0 when none is a number. Points are slot bits (zero-extended) of
 * values that are points (no nulls, NaN or infinities); x is milliseconds since t0. */
size_t lttb_argmax(int type, const int64_t *ts, const uint64_t *v, size_t n, int64_t t0, double ax, double ay,
                   double cx, double cy);
const char *simd_name(void);
void simd_init(void);
/* Re-picks the kernels for at most `level` (CPU_BASE: SSE4.2/AVX, CPU_V3: AVX2, CPU_V4: AVX-512); cpu.c calls it. */
void simd_select(int level);

/* ---- CPU levels (cpu.c, levels.h) ---- */
int cpu_level(void);                 /* the level in use */
int cpu_level_max(void);             /* the highest level both this CPU and this build have */
int cpu_set_level(int level);        /* for tests; -1 if this CPU or build lacks the level */
const void *cpu_level_probe(void);   /* for tests: the level's xxh64 in use (NULL without levels) */
const char *cpu_level_name(int level); /* "v2" (or "base" when the build targets more), "v3", "v4" */

/* Index of the first element >= key in a sorted array (branchless). */
static inline size_t lower_bound_i64(const int64_t *a, size_t n, int64_t key)
{
    const int64_t *base = a;
    while (n > 1) {
        size_t half = n / 2;
        base = (base[half - 1] < key) ? base + half : base;
        n -= half;
    }
    return (size_t)(base - a) + (n == 1 && *base < key);
}

/* ---- buckets (query.c) ----
 * The engine knows UTC only. Local calendars are the client's business: it
 * sends the bounds (BK_BOUNDS, from tsdb_calendar_bounds). */
typedef struct bucketer {
    int kind;         /* BK_ONE, BK_FIXED, BK_CALENDAR or BK_BOUNDS */
    int64_t start;    /* BK_ONE: the start reported for the one bucket */
    int64_t interval; /* BK_FIXED: length in ms */
    int unit;         /* BK_CALENDAR: TSDB_DAY .. TSDB_YEAR, in UTC */
    int64_t count;
    const int64_t *bounds; /* BK_BOUNDS: bucket i begins at bounds[i]; the last runs on without end */
    size_t nbounds;
} bucketer;
enum { BK_ONE, BK_FIXED, BK_CALENDAR, BK_BOUNDS };

/* ---- parts ---- */

/* One entry of a part's sparse index, decoded (gx_load) or about to be encoded (pw_push_index); on disk the
 * entries are bit-packed (gindex.c). The statistics cover the known values; their type is the series'. */
typedef struct granule {
    uint32_t series;
    uint32_t count;  /* rows, nulls included */
    int64_t ts_min;
    int64_t ts_max;
    uint64_t off;    /* file offset of the timestamp block; the value block follows it */
    uint32_t ts_len;
    uint32_t val_len;
    uint32_t nulls;
    uint32_t crc;    /* low 32 bits of xxh64 over both blocks */
    union { double fmin; int64_t imin; };
    union { double fmax; int64_t imax; };
    union { double fsum; uint64_t isum_lo; };
    int64_t isum_hi; /* integer sums are 128 bits */
    /* The spread: for floats m2, the sum of squared deviations from the mean
     * (fsum over the known values); for integers the exact sum of squares,
     * 192 bits (isq0 + isq1 * 2^64 + isq2 * 2^128). See agg_state. */
    union { double fm2; uint64_t isq0; };
    uint64_t isq1, isq2;
    /* Time-weighted statistics (twa.c): the granule's first and last values (as
     * doubles; GT_FIRST_NULL/GT_LAST_NULL when null), and over the segments
     * between its consecutive rows the STEP and LINEAR areas (value x ms) and
     * the time they cover; tw_gap is the longest of those segments (saturated),
     * so a query with a staleness limit knows whether the areas apply. */
    double tw_first, tw_last;
    double tw_step, tw_lin;
    uint64_t tw_step_ms, tw_lin_ms;
    uint32_t tw_gap;
    uint32_t tw_flags;
} granule;
_Static_assert(sizeof(granule) == 160, "granule must be 160 bytes");
enum { GT_FIRST_NULL = 1, GT_LAST_NULL = 2 };
/* The time-weighted statistics of a granule's rows (n >= 1, ts strictly increasing). */
void granule_twa(int type, const int64_t *ts, const void *v, size_t n, struct granule *g);
/* Granules of at most TWA_SEQ_MAX points add their segments' areas one after another (twa.c, compiled once: the
 * same bits on every CPU); longer ones take simd_twa, whose 16 lanes give the same bits on every kernel set. The
 * limit is where every set's kernel is at least as fast as that loop. */
#ifndef TWA_SEQ_MAX
#define TWA_SEQ_MAX 383
#endif
/* granule_twa's sums over the n - 1 segments of a longer granule (simd.c) */
typedef struct twa_sums {
    double step, lin;
    uint64_t sms, lms, gap;
} twa_sums;
SIMD_VAR void (*simd_twa)(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o);

typedef struct part_header {
    char magic[8];
    uint32_t version;
    uint32_t granule_rows;
    uint64_t wal_seq; /* newest WAL segment whose rows this part holds; 0 if none */
    /* format 9 (a format-8 part has the first 24 bytes only, PART_HDR_V8): the smallest first and the largest last
     * `fno` of the rows records the flush took, or the merged sources' range; 0, 0 for a part whose rows have no
     * number (a large insert, a replace, a delete). Rows only: markers are not in it (design-cdc.md 2.1) */
    uint64_t fno_lo, fno_hi;
    int64_t written_ms; /* wall clock when its newest rows were inserted; a merge output has the newest of its inputs */
} part_header;
_Static_assert(sizeof(part_header) == 48, "part header must be 48 bytes");

typedef struct part_footer {
    uint64_t index_off;
    uint64_t index_len; /* bytes of the packed index (gindex.c), a multiple of 8 */
    uint64_t rows;
    int64_t min_ts;
    int64_t max_ts;
    uint32_t ngranules;
    uint32_t version;
    uint64_t index_hash; /* xxh64 of the header, the index and the footer up to here */
    char magic[8];
} part_footer;
_Static_assert(sizeof(part_footer) == 64, "footer must be 64 bytes");
_Static_assert(sizeof(part_footer) >= BLK_SLACK, "footer must cover decoder overread");

/* A range tombstone: rows of `series` with start <= ts < end are deleted from
 * every part of the same partition with a lower block number (never from the
 * part that holds it: its own rows are newer). Stored like this in the
 * trailer of a part, sorted by (series, start), ranges of one series disjoint. */
typedef struct tomb {
    uint32_t series;
    uint32_t zero;
    int64_t start, end;
} tomb;
_Static_assert(sizeof(tomb) == 24, "tomb must be 24 bytes");

/* A sorted, coalesced set of tombstones, for applying several parts' at once. */
typedef struct tomb_set {
    tomb *t;
    size_t n, cap;
} tomb_set;
int tomb_set_add(tomb_set *s, const tomb *t, size_t n);
void tomb_set_finish(tomb_set *s); /* sorts and coalesces; call after adding */
void tomb_set_free(tomb_set *s);
/* index of the first range of `series` that ends after ts (s->n if none) */
size_t tomb_set_seek(const tomb_set *s, uint32_t series, int64_t ts);
/* whether a range of `series` intersects [lo, hi] */
bool tomb_set_hits(const tomb_set *s, uint32_t series, int64_t lo, int64_t hi);
/* Keeps the rows (ts ascending, slots of width w) at or after `cutoff` that no
 * range of `series` covers, in place; returns how many remain. */
size_t tomb_set_filter(const tomb_set *s, uint32_t series, int64_t cutoff, int64_t *ts, uint8_t *v, size_t w,
                       size_t n);

/* ---- the granule index (gindex.c) ----
 * A part's index stays in its mapping: every entry is GF_N fields at fixed bit offsets, each field with its own
 * frame of reference (base + i x slope + (bits << shift)) and width, chosen when the part is written; float
 * statistics that a float32 holds exactly take 32 bits or fewer (GK_F32). The series are a table of runs. Readers
 * decode the fields they need of entry i in place (gx_*), a few shifts and masks each; nothing is copied to the
 * heap, so the index pages stay shared page cache as before. */
enum { GF_COUNT, GF_TSMIN, GF_SPAN, GF_OFF, GF_TSLEN, GF_VALLEN, GF_NULLS, GF_CRC, GF_MIN, GF_MAX, GF_SUM,
       GF_ISUMHI, GF_M2, GF_ISQ1, GF_ISQ2, GF_FIRST, GF_LAST, GF_STEP, GF_LIN, GF_STEPMS, GF_LINMS, GF_GAP,
       GF_FLAGS, GF_N };
enum { GK_U64, GK_F32 };
/* GF_FLAGS: tw_flags in the low 32 bits; these say tw_step / tw_lin are not stored but derived (gix_dstep) */
#define GIX_STEP_DERIVED (1ull << 32)
#define GIX_LIN_DERIVED (1ull << 33)
typedef struct gix_field {
    uint64_t base, mask, slope;
    uint32_t byte;            /* of the 8 bytes loaded at entry + byte, bits [bit, bit + width) */
    uint8_t bit, shift, width, kind;
} gix_field;
typedef struct gix {
    const uint8_t *e;              /* entry i at e + i x stride */
    const uint32_t *rser, *rstart; /* run r: series rser[r] from entry rstart[r] */
    uint32_t n, nruns, stride;
    gix_field f[GF_N];
} gix;
/* Checks and lays out the index [b, b + len) of n entries; 8 readable bytes must follow it (the footer does).
 * TSDB_ERR_CORRUPT for anything but exactly what gix_encode writes the layout of. */
int gix_parse(const uint8_t *b, size_t len, uint32_t n, gix *x);
/* The packed index of g[0..n) (sorted by series), malloc'd, *len a multiple of 8; decoded again and compared
 * with g before it is returned: an encoder bug fails the write, it never stores a wrong statistic. */
int gix_encode(const struct granule *g, size_t n, uint8_t **out, size_t *len);
/* tw_step and tw_lin of a granule on a regular grid of gap ms without nulls, from its sum, first and last value:
 * stored as a flag when these give exactly the bits granule_twa computed (the writer checks each entry). */
static inline double gix_dstep(double sum, double last, uint32_t gap) { return (sum - last) * (double)gap; }
static inline double gix_dlin(double sum, double first, double last, uint32_t gap)
{
    return (sum - (first + last) * 0.5) * (double)gap;
}
static inline uint64_t gix_get(const gix *x, uint32_t i, int f)
{
    const gix_field *d = &x->f[f];
    uint64_t w;
    memcpy(&w, x->e + (size_t)i * x->stride + d->byte, 8);
    return d->base + (uint64_t)i * d->slope + (((w >> d->bit) & d->mask) << d->shift);
}
/* a 64-bit statistic's bits: a float32 field widened to the double it stands for */
static inline uint64_t gix_bits(const gix *x, uint32_t i, int f)
{
    uint64_t u = gix_get(x, i, f);
    if (x->f[f].kind == GK_F32) {
        uint32_t b = (uint32_t)u;
        float v;
        memcpy(&v, &b, 4);
        double d = v;
        memcpy(&u, &d, 8);
    }
    return u;
}
static inline double gix_f64(const gix *x, uint32_t i, int f)
{
    uint64_t u = gix_bits(x, i, f);
    double d;
    memcpy(&d, &u, 8);
    return d;
}
/* The first entry of run r. gix_parse checked the run table, but without pin_index the readers use it in
 * the part's mapping, where damage that reaches the file after the open shows (a misdirected write, a page misread
 * after eviction): every entry number taken from it is kept below n, so such damage gives wrong answers, as in a
 * granule payload, and never a read outside the index. */
static inline uint32_t gix_rstart(const gix *x, uint32_t r) { return x->rstart[r] < x->n ? x->rstart[r] : x->n; }
/* the run of series s: entries [*a, *b), empty at where it would be when the part has none of s */
static inline void gix_run(const gix *x, uint32_t s, uint32_t *a, uint32_t *b)
{
    uint32_t lo = 0, hi = x->nruns;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (x->rser[mid] < s)
            lo = mid + 1;
        else
            hi = mid;
    }
    *a = lo < x->nruns ? gix_rstart(x, lo) : x->n;
    *b = lo < x->nruns && x->rser[lo] == s ? (lo + 1 < x->nruns ? gix_rstart(x, lo + 1) : x->n) : *a;
    if (*b < *a)
        *b = *a;
}
/* the run holding entry i (i < n) */
static inline uint32_t gix_run_of(const gix *x, uint32_t i)
{
    uint32_t lo = 0, hi = x->nruns; /* the first run starting after i */
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (x->rstart[mid] <= i)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo ? lo - 1 : 0; /* 0 when rstart[0] is damaged (see gix_rstart) */
}

typedef struct part {
    char name[96];
    int dirfd;
    int64_t partition;
    uint64_t min_block, max_block;
    uint32_t level;
    uint32_t grows; /* granule_rows the part was written with */
    uint64_t wal_seq;
    uint64_t fno_lo, fno_hi; /* the header's position range of the rows (0, 0: none); 0 in a format-8 part */
    int64_t written_ms;      /* the header's write time; 0 in a format-8 part */
    uint32_t hdr_len;        /* sizeof(part_header), or PART_HDR_V8 for a part an upgrade carried over */
    const uint8_t *map;
    size_t map_len;
    gix ix; /* the granule index, in the map */
    uint32_t ng;
    const tomb *t; /* the part's tombstones (in the map, or in icopy); NULL when none */
    uint8_t *icopy; /* pin_index: the index and tombstones, copied when checked; NULL: read in the map */
    size_t pinned;  /* bytes of icopy */
    uint32_t nt;
    uint64_t index_off, index_len, index_hash; /* as the open checked them (part_index_intact) */
    uint64_t rows;
    int64_t min_ts, max_ts;
    bool verify; /* queries check each granule's checksum (tsdb_options.verify_checksums) */
    time_t created;
    time_t written; /* when its newest rows were inserted: a merge keeps its sources' (after a reopen, created) */
    atomic_int refs;
    /* protected by tsdb.lock */
    bool merging;
    bool dropped;
    atomic_bool damaged; /* its index no longer matches the open's checksum, or a merge could not decode one of its
                          * granules: out of merges and drops (part_checked, part_mark_payload) */
    atomic_bool wait_logged; /* db_wait_merges said once that inserts no longer wait for this damaged part's partition */
    uint64_t fail_seq;       /* db->merge_fail_seq when a merge that took it as a source last failed (db->lock) */
    uint64_t merge_len_gen;  /* db->ret_len_gen when a merge selected it (db->lock) */
    uint32_t ttl_gen;        /* retention generation ttl_any/ttl_all were computed for; 0: never */
    int64_t ttl_any, ttl_all; /* retention "now" after which some row / every row and tombstone has expired */
    atomic_bool obsolete; /* delete the file when the last reference goes */
    bool retired; /* set before obsolete: the file is retired_<name> (db_retire_locked), not <name> */
    char pending[64];     /* tmp file name while the part awaits parts_commit, else "" */
    /* the latest table's fold list (latest.c): per series of this pending part, its newest non-null row and the range
     * of its nulls, made by the part writer; the publisher folds and frees it (NULL when the table is off) */
    struct lfold *lf;
    size_t nlf, lfcap; /* lfcap: the allocation, for tsdb_free_big */
} part;


/* What gx_load decodes besides the count: the fields a reader uses, so a hot loop pays only for those (each field
 * is a few dependent loads, shifts and masks; the statistics path over many small parts is bound by them). */
enum {
    GW_TIME = 1,     /* ts_min, ts_max */
    GW_LOC = 2,      /* off, ts_len, val_len, crc, nulls */
    GW_STAT = 4,     /* nulls, min, max, sum (isum_hi only with GW_INT) */
    GW_SPREAD = 8,   /* m2 or isq0 (isq1, isq2 only with GW_INT) */
    GW_TWEND = 16,   /* ts_min, ts_max, tw_first, tw_last, tw_flags */
    GW_TWSTEP = 32,  /* nulls, tw_step, tw_step_ms (and sum, tw_gap when derived) */
    GW_TWLIN = 64,   /* nulls, tw_lin, tw_lin_ms (and sum, tw_gap when derived) */
    GW_SERIES = 128,
    GW_INT = 256,    /* the series is an integer type: the upper words of its sums matter */
    GW_TW = GW_TWEND | GW_TWSTEP | GW_TWLIN,
    GW_ALL = 511,
};
static inline uint32_t gx_count(const part *p, uint32_t i) { return (uint32_t)gix_get(&p->ix, i, GF_COUNT); }
static inline int64_t gx_ts_min(const part *p, uint32_t i) { return (int64_t)gix_get(&p->ix, i, GF_TSMIN); }
static inline int64_t gx_ts_max(const part *p, uint32_t i)
{
    return (int64_t)(gix_get(&p->ix, i, GF_TSMIN) + gix_get(&p->ix, i, GF_SPAN));
}
/* both ends of entry i: ts_min decoded once */
static inline void gx_ts(const part *p, uint32_t i, int64_t *lo, int64_t *hi)
{
    uint64_t t = gix_get(&p->ix, i, GF_TSMIN);
    *lo = (int64_t)t;
    *hi = (int64_t)(t + gix_get(&p->ix, i, GF_SPAN));
}
static inline uint32_t gx_series(const part *p, uint32_t i) { return p->ix.rser[gix_run_of(&p->ix, i)]; }
/* Entry i of p into *g: the count, and the groups in `want` (GW_*). Other fields are left as they were. */
static inline void gx_load(const part *p, uint32_t i, unsigned want, granule *g)
{
    const gix *x = &p->ix;
    g->count = (uint32_t)gix_get(x, i, GF_COUNT);
    uint64_t span = 0;
    if (want & (GW_TIME | GW_TWEND | GW_TWSTEP | GW_TWLIN)) {
        span = gix_get(x, i, GF_SPAN);
        g->ts_min = (int64_t)gix_get(x, i, GF_TSMIN);
        g->ts_max = (int64_t)((uint64_t)g->ts_min + span);
    }
    if (want & (GW_LOC | GW_STAT | GW_TWSTEP | GW_TWLIN))
        g->nulls = (uint32_t)gix_get(x, i, GF_NULLS);
    if (want & GW_LOC) {
        g->off = gix_get(x, i, GF_OFF);
        g->ts_len = (uint32_t)gix_get(x, i, GF_TSLEN);
        g->val_len = (uint32_t)gix_get(x, i, GF_VALLEN);
        g->crc = (uint32_t)gix_get(x, i, GF_CRC);
    }
    if (want & GW_STAT) {
        g->isum_lo = gix_bits(x, i, GF_SUM);
        g->imin = (int64_t)gix_bits(x, i, GF_MIN);
        g->imax = (int64_t)gix_bits(x, i, GF_MAX);
        g->isum_hi = want & GW_INT ? (int64_t)gix_get(x, i, GF_ISUMHI) : 0;
    }
    if (want & GW_SPREAD) {
        g->isq0 = gix_bits(x, i, GF_M2);
        g->isq1 = want & GW_INT ? gix_get(x, i, GF_ISQ1) : 0;
        g->isq2 = want & GW_INT ? gix_get(x, i, GF_ISQ2) : 0;
    }
    if (want & GW_TW) {
        uint64_t fl = gix_get(x, i, GF_FLAGS);
        g->tw_flags = (uint32_t)fl;
        if (want & GW_TWEND) {
            g->tw_first = gix_f64(x, i, GF_FIRST);
            g->tw_last = gix_f64(x, i, GF_LAST);
        }
        if (want & (GW_TWSTEP | GW_TWLIN)) {
            bool ds = want & GW_TWSTEP && fl & GIX_STEP_DERIVED, dl = want & GW_TWLIN && fl & GIX_LIN_DERIVED;
            if (ds || dl) {
                if (!(want & GW_STAT))
                    g->isum_lo = gix_bits(x, i, GF_SUM);
                g->tw_gap = (uint32_t)gix_get(x, i, GF_GAP);
                if (!(want & GW_TWEND)) {
                    g->tw_last = gix_f64(x, i, GF_LAST);
                    if (dl)
                        g->tw_first = gix_f64(x, i, GF_FIRST);
                }
            }
            if (want & GW_TWSTEP) {
                g->tw_step = ds ? gix_dstep(g->fsum, g->tw_last, g->tw_gap) : gix_f64(x, i, GF_STEP);
                g->tw_step_ms = span - gix_get(x, i, GF_STEPMS);
            }
            if (want & GW_TWLIN) {
                g->tw_lin = dl ? gix_dlin(g->fsum, g->tw_first, g->tw_last, g->tw_gap) : gix_f64(x, i, GF_LIN);
                g->tw_lin_ms = span - gix_get(x, i, GF_LINMS);
            }
        }
        if ((want & GW_TW) == GW_TW)
            g->tw_gap = (uint32_t)gix_get(x, i, GF_GAP);
    }
    if (want & GW_SERIES)
        g->series = gx_series(p, i);
}

/* pin: the index and tombstones are read from a copy made when they were checked (tsdb_options.pin_index), not
   from the mapping; the caller sets p->verify (granule checksums, tsdb_options.verify_checksums) */
int part_open(int dirfd, const char *name, bool pin, part **out);
int part_open_as(int dirfd, const char *file, const char *name, bool pin, bool v8_ok, part **out);
/* the position range a part writer begun on this thread takes (set around a flush; a merge sets its own) */
extern __thread uint64_t cur_fno_lo, cur_fno_hi;
bool part_index_intact(const part *p);
/* part_index_intact; a part that fails it is marked damaged, logged and counted once (st_broken_parts) */
bool part_checked(struct eng *db, part *p, const char *what);
/* A merge could not decode a granule of p: marks it damaged, logs and counts it once (in memory only) */
void part_mark_payload(struct eng *db, part *p, const char *what);
void part_ref(part *p);
void part_unref(part *p);
/* Decodes a granule: timestamps, and values (if v) as slots of `type`, into buffers of room for cap rows; checks
 * the granule's checksum first if verify. A count above cap or the part's granule size is TSDB_ERR_CORRUPT: without
 * pin_index the count is read in the mapping, where a write to the file can change it between the load that sized
 * the caller's buffers and the one in g, so cap is what the buffers hold, never a count loaded before. */
int granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                 bool verify);
/* the same for entry i: decodes its GW_LOC fields into *g first. Out of line on purpose: inlined, the entry's
 * decoding changes how the compiler lays out a caller's per-row loop (TWA lost 12% that way). */
int granule_read_at(codec_ctx *c, const part *p, uint32_t i, int type, int64_t *ts, void *v, size_t cap, bool verify,
                    granule *g);
extern atomic_uint_fast64_t granule_checks; /* granule checksums computed, for tests */
/* test hook: runs where a reader has used the counts of entries from i of p (without pin_index, in the mapping) and
 * is about to load them again (IX_*), so a test can change them in between, as a write to the file can */
enum { IX_SRC, IX_VISIBLE, IX_FITS, IX_COPY };
extern void (*_Atomic index_reload_hook)(const part *p, uint32_t i, int at);
#define IX_HOOK(p, i, at)                                                                                            \
    do {                                                                                                             \
        void (*h_)(const part *, uint32_t, int) = atomic_load_explicit(&index_reload_hook, memory_order_relaxed);   \
        if (h_)                                                                                                      \
            h_(p, i, at);                                                                                            \
    } while (0)

/* Hour cuts. A granule of a series also ends at a UTC hour edge e (a multiple of HOUR_CUT_MS) when at least
 * HOUR_CUT_MIN rows of it lie in the hour before e, [e - 1 h, e), so hourly buckets and local days (whole hours in
 * nearly every zone) are answered from granule statistics instead of decoding the granule that holds the edge. The
 * rule counts rows, not the rate of the last few (a burst would look dense) nor the class (a short retention keeps
 * slow series in class 0): a series of fewer rows an hour, steady or in bursts, keeps its 8192-row granules. A
 * writer counts the rows it was given, so a part that begins less than an hour's worth before an edge does not cut
 * there; merges do, once they see the hour. Part of the layout the writer makes, not of the format: readers take any
 * granule boundaries. A merge copies a granule only where the writer would cut its rows the same way
 * (pw_copy_fits, pw_hcut_before). 0: off. */
#ifndef HOUR_CUT_MIN
#define HOUR_CUT_MIN 3000
#endif
#define HOUR_CUT_MS 3600000
extern atomic_uint hour_cut_min; /* HOUR_CUT_MIN; tests set 0 to compare against the layout without hour cuts */
typedef struct part_writer {
    struct eng *db;
    int64_t partition;
    int fd;
    char tmp_name[64];
    uint8_t *buf;
    size_t len, cap;
    uint64_t off; /* file offset of buf[0] */
    granule *idx;
    size_t ng, idx_cap;
    uint64_t wal_seq;
    uint64_t fno_lo, fno_hi; /* the header's position range (pw_begin takes the flush's, a merge sets its own) */
    int64_t written_ms;
    uint32_t pend_series;
    int pend_type;
    size_t pend_n;
    int64_t *pend_ts;
    uint8_t *pend_v; /* slots of pend_type */
    uint32_t grows;
    int64_t cut_ms; /* > 0: granules end at multiples of it (GRANULE_CUT); 0 in a base partition */
    uint32_t hc_min;          /* > 0: hour cuts for series of at least this many rows an hour (HOUR_CUT_MIN) */
    uint32_t hk_series;       /* the series of the last rows given to the writer (emitted, pending or copied), */
    int64_t hk_hour, hk_end;  /* the hour of the last one (floor of ts / HOUR_CUT_MS) and where it ends, */
    uint64_t hk_cnt;          /* and how many of its rows given lie in that hour */
    int64_t *hk_buf;          /* stamps pw_copy_fits decoded: of entry hk_di of hk_dsrc, hk_dn of them */
    const struct part *hk_dsrc;
    uint32_t hk_di, hk_dn;
    int zstd_level;
    int value_codec;
    codec_ctx *cc;
    uint64_t rows;
    int64_t min_ts, max_ts;
    tomb *tombs; /* written after the index; sorted, disjoint per series */
    size_t nt;
    bool keep_empty;  /* write a part even with no rows and no tombstones (a merge's wal_seq anchor) */
    int *defer_fd;    /* pending parts: hand the fd over here, its writeback started, for the caller's fdatasync */
    int effort;       /* CODEC_FAST (pw_begin) or CODEC_MERGE */
    int64_t empty_ts; /* its min_ts = max_ts then */
    bool lf_on;       /* collect the latest table's fold list (pw_begin: when the engine has a table; merges clear it) */
    struct lfold *lf; /* per series of the rows given to pw_add, in series order */
    size_t nlf, lfcap;
} part_writer;

int pw_begin(part_writer *w, struct eng *db, int64_t partition, uint64_t wal_seq);
/* Rows of one series of `type` (v in its slots); (series, ts) order across calls. */
int pw_add(part_writer *w, uint32_t series, int type, const int64_t *ts, const void *v, size_t n);
/* entry i of src, copied as it is */
int pw_copy_granule(part_writer *w, const part *src, uint32_t i);
/* Whether a merge may copy entry i of src next (g: its series, count and window) and keep the layout one writer
 * given its rows would make: no hour cut inside it, and with want_end (a short granule) *end set to the hour edge
 * after it where the writer cuts (INT64_MAX: none; then only a day cut or the end of the series' rows ends it, which
 * the caller checks). 1 yes, 0 no, < 0 an error. Decides from the count, window and longest gap between rows where
 * they suffice, else from the timestamps, decoded (and kept for pw_copy_granule's count of the hour). */
int pw_copy_fits(part_writer *w, struct part *src, uint32_t i, const granule *g, bool want_end, int64_t *end);
/* whether w ends its pending rows at an hour edge at or before t (a cut before a granule of their series) */
bool pw_hcut_before(const part_writer *w, int64_t t);
/* Tombstones for the trailer, sorted by (series, start) and disjoint per series; copied. */
int pw_set_tombs(part_writer *w, const tomb *t, size_t n);
int pw_finish(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out);
/* As pw_finish, but the part stays under its tmp name until parts_commit. */
int pw_finish_pending(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out);
/* Numbers pending parts and gives them their names, all or none even across a
 * crash. Caller holds publish_lock and publishes them before releasing it.
 * A failure once the commit may have reached the disk (the intent's or the
 * names' directory fsync, a rename) fails the engine (eng_fail): the caller
 * then discards the parts without publishing them, and parts_discard leaves
 * their files for the next open. */
int parts_commit(struct eng *db, part **ps, size_t n);
void parts_discard(struct eng *db, part **ps, size_t n);
int intents_recover(struct eng *db);
/* Exactly the names the engine gives part writers' tmp files and parts: what a cleanup at open may touch. */
bool part_tmp_name(const char *name);
bool part_file_name(const char *name);
bool intent_file_name(const char *name); /* an insert's intent, txn_<pid>_<n>.intent */
bool wal_file_name(const char *name);    /* a WAL segment, wal_<16 lowercase hex>.log (memtable.c) */
void meta_tmp_make(char *buf, size_t cap); /* a volume add's own META temporary (db.c) */
bool meta_tmp_name(const char *name);      /* the engine's or an add's META temporary */
void pw_abort(part_writer *w);

/* Every range is half-open with an int64 end, so a row at INT64_MAX could never be read, counted or deleted:
   inserts refuse it (TSDB_ERR_INVALID) rather than keep a row no query reaches. A WAL written before this rule
   may still hold one; replay and flush keep it, as the build that acknowledged it did. */
#define TS_UNREACHABLE INT64_MAX

/* ---- series dictionary ---- */
struct cls_table;
typedef struct series_dict {
    pthread_mutex_t lock;        /* the table and the names; never held across I/O */
    pthread_mutex_t append_lock; /* one append (or follower's reload) at a time, across its I/O; before lock */
    const char *file;
    int fd;
    bool fsync;
    char **names;
    uint8_t *types; /* tsdb_type per id */
    size_t cap;
    atomic_uint count;
    uint32_t *table; /* open addressing; id + 1, 0 = empty */
    size_t tcap;
    uint64_t seed;   /* random per open: names chosen to collide cannot be planted (nothing persists the hash) */
    uint64_t file_off; /* bytes of SERIES already loaded */
    uint64_t last_off; /* where the last record loaded starts (a reload checks it is still there) */
    uint32_t last_check; /* and its check field */
    bool broken;       /* an append failed (under lock): the file may end in a torn record, append no more */
    bool tail_pending; /* a writer's open found a torn tail after file_off: cut by series_cut_tail, not before */
    /* test hook (tsdb_test_dict_fault), read atomically: called at the DICT_AT_* points; nonzero at DICT_AT_ADD
     * fails that add as if memory ran out */
    int (*test_fault)(void *arg, int point, const char *name);
    void *test_fault_arg;
    bool keep_names;   /* series_close leaves the name strings: tsdb_series_name pointers outlive a retired engine */
} series_dict;
enum { DICT_AT_ADD = 1, DICT_AT_FAILED = 2, DICT_AT_RELOAD = 3 };

/* The series dictionary (file SERIES) and the state names (file STATES) are both these. */
int series_open(series_dict *d, int dirfd, const char *file, bool read_only, bool fsync);
int series_reload(series_dict *d, int dirfd); /* readers: names appended by the writer since */
int series_cut_tail(series_dict *d); /* a writer's open, once its checks passed: cuts the torn tail it found */
/* before a torn tail is cut: the bytes [off, off + len) of the file read again from the disk equal `tail`, or
 * TSDB_ERR_IO (series.c) */
int tail_confirm(int dirfd, const char *file, const uint8_t *tail, uint64_t off, size_t len);
void series_close(series_dict *d);
/* create: make a missing name, of `type` (an existing name of another type is TSDB_ERR_INVALID) */
int series_get(series_dict *d, const char *name, bool create, int type, uint32_t *id);
const char *series_name(series_dict *d, uint32_t id);
int series_type(series_dict *d, uint32_t id); /* -1 for an unknown id */
/* Also the batch's class (cls_uniform's answer, -2 when a series is fresh), from the class bytes of ct. */
bool series_check(series_dict *d, const uint32_t *series, uint32_t cs, size_t n, int type, struct cls_table *ct,
                  int *ucls);

/* ---- working memory of multi-series tasks (multi.c) ----
 * A series that is not at the head of a multi-series request runs with a budget: where it would allocate more
 * (bucket accumulators, collected rows) it gives up with TSDB_ERR_DEFER and is run again at its turn, streaming,
 * as a single request. NULL on single-series paths. */
#define TSDB_ERR_DEFER (-100)
typedef struct qbudget {
    size_t bytes;
} qbudget;
extern __thread const qbudget *cur_budget;
extern size_t multi_defer_bytes, multi_defer_rows; /* 16 MiB, 65536; tests lower them to force deferral */
/* the most bounds one series of a TRIM request may keep: what one client request carries (tests lower it) */
extern size_t multi_max_bounds;
/* test hook: runs in tsdb_query_multi once the helpers were woken, before the caller conducts (a caller preempted
 * there, which tests make certain) */
extern void (*_Atomic multi_open_hook)(void);
/* test hook: runs in a merge with no lock held, at 0 just before its output is named (pw_finish) and at 1 just
 * after (named, the directory synced), before merge_commit_locked */
extern void (*_Atomic merge_hook)(int at);

/* Threads for a query step that decodes `rows` rows in `items` independent pieces (granules): one per
 * query_rows_per_thread rows, at most one per item and query_threads. Small reads are fastest on the calling
 * thread alone (no wake-up, no cache traffic), large ones on many. Every query kind that fans out uses it; what a
 * thread folds does not depend on the count (results are bit-identical at any count). query_rows_per_thread is
 * QUERY_ROWS_PER_THREAD, measured with tools/threads-bench (docs/design.md has the sweep); the bench changes it.
 * On a 7950X no window size was slower than one thread at 8192 or 16384 rows per thread; 16384 is as fast over
 * the sweep (geometric mean 0.32 of one thread at 32 threads for both) and wakes half the helpers for small
 * windows, which leaves them to other queries. */
#define QUERY_ROWS_PER_THREAD 16384
/* A TWA decodes and integrates every row (areas, gaps): about 12 times an aggregate's work per row, so its rows
 * count that many times over (threads-bench: 105 us against 8.1 us for one granule). */
#define TWA_ROW_WEIGHT 12
extern uint64_t query_rows_per_thread;
int query_threads_for(const eng *db, uint64_t rows, size_t items);
static inline bool qbudget_ok(size_t bytes)
{
    const qbudget *b = cur_budget;
    return !b || bytes <= b->bytes;
}

/* ---- query scheduler (sched.c) ---- */
typedef struct sched sched;
typedef struct sjob sjob;
/* One request's share of the scheduler: its jobs, the threads working for it and its cancel flag. Lives on the
 * stack of the call that began it (qs_begin, tsdb_query_multi). */
typedef struct sgroup {
    sched *s;
    struct sgroup *next, *prev, *outer;
    sjob *jobs; /* open or running, newest first */
    int cap;    /* threads working for it at most */
    int active; /* threads working for it now: the one that began it and its helpers */
    uint64_t seq;
    bool linked;
    bool counted; /* an outermost group: counted in the scheduler's live requests */
    atomic_int cancel;
} sgroup;
struct sjob {
    sched *s;
    sjob *next;
    sgroup *g;
    void (*fn)(void *arg, int i);
    void *arg;
    int want;     /* participants at most, the caller included */
    int helpers;  /* helpers inside fn now */
    int ids;      /* helpers that joined so far */
    uint64_t opens;
    bool open, closing, shared, implicit, saved_helper;
    sjob *saved_job;
    sgroup own; /* the group of a job opened outside any */
    pthread_cond_t done;
};
extern __thread sgroup *cur_group;
sched *sched_new(int query_threads, int request_threads);
void sched_free(sched *s);
int sched_helpers(const sched *s);
int sched_request_threads(const sched *s);
/* Begins a group on this thread (cap <= 0: request_threads); jobs opened under it belong to it. */
void sched_group_begin(sched *s, sgroup *g, int cap);
void sched_group_end(sgroup *g);
void sched_cancel(sgroup *g);
/* Runs fn(arg, i) on the caller (i == 0) and on up to n - 1 helpers, within the group's cap; fn claims items until
 * none are left. Returns the participants. */
int sched_run(sched *s, int n, void (*fn)(void *arg, int i), void *arg);
/* The parts of sched_run, for a caller that does other work while helpers run fn (tsdb_query_multi): open, then
 * reopen whenever fn may find items again after a helper found none, then close (waits for the helpers). */
void sched_open(sched *s, sjob *j, int want, void (*fn)(void *arg, int i), void *arg);
void sched_reopen(sjob *j);
void sched_close(sjob *j);
/* After each item of fn: 0 go on; 1 return now (a helper another group needs); TSDB_ERR_STOPPED the group was
 * cancelled. */
int sched_leave(void);
/* Decode buffers: cached per helper thread (helpers join and leave often), malloc'd elsewhere. One slot per
 * buffer that is live at once (0..3). */
void *qscratch_get(int slot, size_t bytes);
void qscratch_put(void *p);

/* ---- statistics (stats.c, docs/design-stats.md) ----
 * Query counters: one block per (thread, handle), written by its owner with plain relaxed stores, summed by readers.
 * Storage numbers: list changes push an event (db->lock held), a sweeper thread walks each part's index once. */
typedef struct qreg qreg;
enum { QC_QUERIES, QC_ROWS_DECODED, QC_ROWS_STATS, QC_ROWS_MEM, QC_BYTES_READ, QC_PARTS, QC_GRANULES,
       QC_GRANULES_STATS, QC_RESULT_ROWS, QC_NS, QC_N };
qreg *qreg_new(void);
void qreg_close(qreg *r); /* the handle's reference: blocks of threads that outlive the handle stay valid */
void qreg_add(qreg *r, const uint64_t v[QC_N]);
void qreg_totals(qreg *r, uint64_t out[QC_N]);

struct estats;
/* Test hooks (tests/test_stats.c): the ring holds this many events (at most its size, 4096) so a test can overflow
 * it; the sweeper does not run while stats_pause is set; tsdb_series_stats stops after the chunk in which its index
 * probes pass this number (default 100 million). */
extern atomic_uint stats_ring_cap;
extern atomic_uint stats_pause;
extern atomic_uint_fast64_t stats_open_reconciles;
extern atomic_uint_fast64_t sstats_budget;
void stats_start(eng *db, bool reconcile); /* idempotent; does nothing with stats = -1 */
void stats_open_done(eng *db); /* the open no longer changes the part list without db->lock: the sweeper may reconcile */
void stats_stop(eng *db);
void stats_push(eng *db, part *p, int kind); /* db->lock held (or the open, single-threaded) */
enum { STAT_EV_ADD = 1, STAT_EV_REMOVE = 2 };
void stats_list_swapped(eng *db); /* a follower replaced its whole list */
void stats_fill(eng *db, tsdb_volume_stats *vs);
int stats_settle(eng *db, int timeout_ms);
/* per-series numbers of one chunk of ids (sorted ascending, at most 256) from one cut of the engine */
typedef struct sscx sscx;
sscx *sscx_open(eng *db);
void sscx_close(sscx *x);
void sscx_run(sscx *x, const uint32_t *ids, size_t m, tsdb_series_stat *out, uint64_t *probes);

/* One outermost query's statistics and scheduler group (volumes.c). */
typedef struct qrun {
    qstats q;
    qreg *reg; /* the handle's counters, NULL: not counted */
    bool outer;
    struct timespec t0;
    sgroup g; /* the threads it may use (request_threads); a query inside a multi call uses the call's */
} qrun;
/* For tsdb_query_multi: begins the call's group (cap <= 0: request_threads) and totals; the end sets
 * tsdb_last_query_stats. */
void query_run_begin(tsdb *t, qrun *r, int cap);
void query_run_end(qrun *r, int type, uint64_t result_rows);
sched *query_sched(tsdb *t);

/* ---- memtable + WAL (memtable.c) ---- */
/* mt_series.fl (latest.c, design-latest.md 2.2): kept by mt_add when it is given the series' type */
enum { MTF_HAS_NN = 1, MTF_HAS_NULL = 2, MTF_NN_STALE = 4 };
typedef struct mt_series {
    int64_t *ts;
    uint64_t *v; /* slots, zero-extended */
    uint64_t *lsn; /* WAL sequence of each row; increasing, 0 when already durable */
    uint32_t n, cap;
    bool sorted; /* non-decreasing ts, in insertion order */
    uint8_t fl;   /* MTF_*: a non-null row was added; a null was added; the newest non-null row was overwritten by a null */
    uint8_t type; /* the series' type once a row was added with tracking (fl != 0), else 0 */
    uint32_t top_nn; /* index of the newest non-null row (the later one on equal timestamps), with MTF_HAS_NN */
} mt_series;
_Static_assert(sizeof(mt_series) == 40, "mt_series stays 40 bytes");

/* A flushed memtable's series and their newest timestamps, while its WAL segment may still be replayed (classes.c) */
struct cls_wtail {
    struct cls_wtail *next;
    uint64_t seq;
    size_t n;
    bool sorted; /* ids sorted (with max), done by the first switch that looks */
    uint32_t *ids;
    int64_t *max;
};

typedef struct memtable {
    atomic_int refs;
    mt_series *s; /* indexed by series id */
    uint32_t scap;
    bool s_map; /* s is an anonymous mapping (zero pages, grown by mremap: mt_reserve); else malloc'd */
    uint32_t *ids; /* series present, first-insert order */
    uint32_t nids, idcap;
    size_t rows;
    int64_t min_ts, max_ts;
    uint64_t wal_seq;
    int64_t created_ms;
    uint64_t fno_lo, fno_hi; /* the smallest first and largest last number of the rows records it took; 0, 0: none */
    uint64_t fno_end;        /* a sealed memtable: the next number at its seal, the end of its segment (FNO bound) */
} memtable;

/* Rows of one series copied out of a memtable: sorted, unique by ts; v in
 * slots of the series' type. */
typedef struct mem_run {
    int64_t *ts;
    uint8_t *v;
    size_t n;
} mem_run;

typedef struct part_list {
    part **p;
    size_t n, cap;
} part_list;

memtable *mt_new(uint64_t wal_seq);
void mt_ref(memtable *m);
void mt_unref(memtable *m);
/* v: slots of width w (4 or 8). lsn0: WAL sequence of the first row (the rest
 * follow), 0 for rows that are durable already.
 * type: the series' type (every row of one call has it), which makes mt_add keep the latest table's flags
 * (mt_series.fl, top_nn); -1: no tracking (replay and follower memtables, which no lookup reads) */
int mt_add(memtable *m, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n,
           uint64_t lsn0, int type);
/* only rows with a WAL sequence <= visible; values as slots of width w */
int mt_collect(const memtable *m, uint32_t id, size_t w, int64_t start, int64_t end, uint64_t visible, mem_run *out);
void mem_run_free(mem_run *r);
void mt_note_fno(memtable *m, uint64_t first, size_t n);
int mt_flush(struct eng *db);
int mem_insert(struct eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w,
               size_t n, int type);
/* WAL records, version 2 (format 9, docs/design-cdc.md 2.1):
 *   [u32 len | kind << 28][u32 header check][u64 hash][u64 fno][i64 commit_ms][payload]
 * kind 0, rows: len = rows (below 2^28), payload rows of 20 bytes (u32 series, i64 ts, u64 slot); takes len numbers.
 * kind 1, marker: len = payload bytes (a multiple of 4, at least 8), payload u16 type, u16 flags, u32 span, then the
 * type's body; takes span numbers (at least 1). A replay or a follower skips the markers it does not apply and
 * continues the numbering. The hash covers fno, commit_ms and the payload; the header check covers the first word and
 * the hash and binds the record to its segment and offset. wal_record returns the size of the record at segment offset
 * `off`, or 0 when it is torn or absent. */
#define WAL_HDR 32
#define WAL_ROW 20
#define WAL_KIND_ROWS 0
#define WAL_KIND_MARKER 1
#define WAL_MARKER_HDR 8 /* type, flags, span */
typedef struct wal_rec {
    uint32_t kind;
    uint32_t n;             /* rows, or payload bytes of a marker */
    uint64_t fno;           /* the first number it takes */
    uint64_t span;          /* how many: rows, or the marker's span */
    int64_t commit_ms;
    const uint8_t *payload; /* the rows, or the marker's payload (type first) */
    uint16_t mtype, mflags; /* a marker's */
} wal_rec;
size_t wal_record(const uint8_t *p, size_t avail, uint64_t seq, uint64_t off, wal_rec *r);
void wal_reseal(uint8_t *rec, uint64_t seq, uint64_t off); /* tests: a record changed or moved, sealed again */
/* Whether a valid record starts anywhere in p[from, len) (p at segment offset
 * base): then a bad record before it is damage, not a torn tail. */
bool wal_valid_after(const uint8_t *p, size_t len, size_t from, uint64_t seq, uint64_t base);
/* Appends a marker record (the engine's side of the namespace events; no backend in S0: tests, and the hooks that
 * come later). *fno gets its first number. Durable when it returns, where the WAL syncs per write. */
int wal_marker(struct eng *db, uint16_t type, uint16_t flags, uint32_t span, const void *body, size_t body_len,
               uint64_t *fno);
/* Encodes `m` (marker.h) and appends it as one record (wal_marker); *fno gets its first number. */
int wal_marker_mk(struct eng *db, const mk *m, uint64_t *fno);

/* The namespace-event hook (docs/design-ha-modes.md 2.1, M15): one call at each place a name or a file becomes
 * visible, in two phases. jr_intent runs before publish_lock is taken (a replicated backend appends the marker and
 * waits for its commit point there), jr_commit under publish_lock once block numbers are assigned (the ledger
 * backend of shared mode edits its ledger there). S0 has no backend: both do nothing and compile away, the call
 * sites are in place. `arg` is the event's payload (an `mk` or a part list), unused until a backend reads it. */
enum jr_kind {
    JR_COMMIT = 1, /* an insert, replace or delete committed its parts */
    JR_FLUSHED,    /* a memtable's parts were listed and its segment may go */
    JR_MERGE,      /* a merge replaced parts */
    JR_PURGE,      /* retired parts were deleted */
    JR_DROP,       /* tsdb_drop_before */
    JR_RETENTION,  /* a retention limit was appended */
    JR_DICT,       /* a SERIES, STATE or CLASSES record was appended */
};
static inline int jr_intent(struct eng *db, enum jr_kind kind, const void *arg)
{
    (void)db, (void)kind, (void)arg;
    return 0;
}
static inline void jr_commit(struct eng *db, enum jr_kind kind, const void *arg)
{
    (void)db, (void)kind, (void)arg;
}

#define FNO_FILE "FNO"
/* Log numbers stay below FNO_LIMIT (a record at or above it is not one: wal_record). A bound read at open (a slot, a
 * part's fno_hi, the end of a segment) at or above FNO_BOUND_MAX fails the open: the writer must never assign past
 * the limit, which would make its own records unreadable (a torn tail at the next open). The margin is room for the
 * raise by FNO_BELT that an open makes when one FNO slot is bad: that slot may have been the newest, and the open
 * cannot know how far the numbers ran past the other one, so it leaves a gap (numbers may have gaps, never repeat). */
#define FNO_LIMIT ((uint64_t)1 << 62)
#define FNO_BELT ((uint64_t)1 << 40)
#define FNO_BOUND_MAX (FNO_LIMIT - 2 * FNO_BELT)
int fno_create(int dirfd, uint64_t next, bool sync);
void v8_parts_account(struct eng *db, bool at_open); /* upgraded databases: parts with a format-8 header left (db.c) */
bool fno_file_intact(int dirfd);
void fno_slot_page(uint8_t *page, uint64_t next, uint64_t gen); /* 4096 bytes: a slot as FNO stores it */
int fno_load(struct eng *db, uint64_t *next, uint64_t *gen, int *slot, bool *torn);
int fno_require(struct eng *db, uint64_t v);
void wal_row(const uint8_t *r, uint32_t *id, int64_t *ts, uint64_t *slot);
int wal_commit(struct eng *db, uint64_t target);
int wal_open(struct eng *db, int (*before_replay)(struct eng *));
void wal_close(struct eng *db);
void *flusher_main(void *arg);

#define MAX_FLUSHING 4

/* ---- manifest and following (manifest.c) ---- */
#define MAX_TAILS 8

/* A follower's copy of rows from one WAL segment the writer has not flushed yet. */
typedef struct wal_tail {
    uint64_t seq;
    memtable *m; /* contents guarded by buf_lock */
    uint64_t off; /* bytes of the segment consumed */
    uint64_t bad_off;  /* a bad record seen here, with valid ones after it, */
    int64_t bad_since; /* since then (monotonic ms) */
    bool reported;
} wal_tail;

typedef struct graveyard_entry {
    char name[256];   /* retired_<part name> */
    int64_t deadline; /* monotonic ms (eng_mono_ms): a wall clock step must not purge early */
} graveyard_entry;

int manifest_publish(struct eng *db);
int follow_refresh(struct eng *db);
/* MANIFEST's counts of SERIES and STATES records, when it is valid and its writer synced them before it counted them
 * (no `fsync 0`); else TSDB_ERR_NOTFOUND */
int manifest_synced_counts(struct eng *db, uint32_t *series, uint32_t *states);
void *follower_main(void *arg);
void db_retire_locked(struct eng *db, part *p);
void db_retire_unlisted_locked(struct eng *db, part *p);
void graveyard_purge(struct eng *db, bool all);

/* ---- partition classes (classes.c) ----
 * A series' rows go to partitions of 1, 7, 28 or 364 base partitions (partition_ms) by its class; a class-c
 * partition holds series of class c only. Its key is kmul x (its first base partition) + c (kmul = 4; 1 with
 * classes off, where keys are today's base partition numbers), so keys of every class sort by time and are
 * compared for equality as before. A series' class may change at a time T (a switch, appended to CLASSES and
 * synced before anything uses it); rows never move: earlier rows stay where they are. */
#define PART_CLASSES 4
/* In a partition longer than a base partition (classes 1-3), a granule ends at every multiple of GRANULE_CUT base
 * partitions (aligned as partitions are) as well as at 8192 rows. A slow series' 8192 rows span days, and a bucket
 * that cuts through a granule must decode it instead of answering from its statistics. With a cut at every base
 * partition (a day by default), a bucket a class 0 partition answers from statistics (UTC days, and anything coarser
 * made of whole days) is answered from statistics in a class partition too. A week cut (7) made a 60 s series 43%
 * smaller (hdd_temp 0.152 against 0.268 bytes/row) but decoded nearly every granule for day and 2.5-day buckets.
 * Part of the layout the writer makes, not of the format: readers take any granule boundaries, and a merge
 * re-encodes a granule that crosses a cut instead of copying it. */
#ifndef GRANULE_CUT
#define GRANULE_CUT 1
#endif
#define CLS_MASK 3u     /* class byte: the class at the series' newest switch */
#define CLS_HIST 4u     /* it has a switch after INT64_MIN: the history is in the map (cls_mu) */
#define CLS_DECIDED 8u  /* not fresh: its class was decided (or it existed at open) */
typedef struct cls_table {
    uint64_t cap;
    struct cls_table *older;
    _Atomic uint8_t b[];
} cls_table;
typedef struct cls_hist {
    uint32_t series, n, cap;
    bool used;
    int64_t *from; /* increasing */
    uint8_t *cls;
} cls_hist;

/* ---- database ---- */
typedef struct ret_table {
    uint64_t cap;
    struct ret_table *older;
    _Atomic int64_t limit[];
} ret_table;

/* ---- the latest table (latest.c, docs/design-latest.md) ----
 * Per volume, indexed by local id, in pages of LT_PAGE entries of 16 bytes (t, x) and a bitmap of unknown entries.
 * t = ts ^ INT64_MAX, so all-zero memory is "empty" (ts = TSDB_TS_MAX is never a row's). Known: bit clear, the
 * newest non-null row in the parts is (ts, slot x). Unknown: bit set, t holds a bound u (every non-null row in the
 * parts is at or below it), x = 0. Read and written only under `rw` (latest_rw). */
#define LT_PAGE_BITS 16
#define LT_PAGE (1u << LT_PAGE_BITS)
#define LT_MAXPAGES (1u << (31 - LT_PAGE_BITS))
typedef struct lt_page {
    uint64_t e[2 * LT_PAGE];
    uint64_t unk[LT_PAGE / 64];
} lt_page;
#define LT_PAGE_BYTES (sizeof(lt_page))

/* What a publish brings for one series of one part: R, its newest non-null row, and N, the range of the timestamps
 * where its row is a null (conservative). Made by the part writer, folded by the publisher (design-latest.md 3). */
typedef struct lfold {
    uint32_t series;
    uint32_t flags; /* LF_R, LF_N */
    int64_t r_ts;
    uint64_t r_slot;
    int64_t n_lo, n_hi;
} lfold;
enum { LF_R = 1, LF_N = 2 };

typedef struct ltab {
    pthread_rwlock_t rw; /* latest_rw: entries, bitmaps, pages, drop_floor, pending and lgen */
    uint64_t lgen;       /* bumped by every write section (rw); the warmer's stores check it */
    lt_page **pg;        /* LT_MAXPAGES slots, made in id order, never moved */
    uint32_t npg;        /* pages made: ids below npg * LT_PAGE have entries; ids above have none yet (empty) */
    bool capped;         /* the budget refused page npg: ids from npg * LT_PAGE have no entry (the slow path) */
    int64_t drop_floor;  /* a known entry below it reads as unknown (ts) (tsdb_drop_before) */
    atomic_bool pending; /* a large flush's unlocked stores are under way (3.2) */
    pthread_mutex_t pmu; /* only for pcv */
    pthread_cond_t pcv;
    size_t bytes;        /* budget taken (rw) */
    /* the warmer: a thread of its own per volume, the only filler of entries */
    pthread_mutex_t hmu; /* leaf */
    pthread_cond_t hcv;
    uint32_t *hint;      /* local ids whose unknown entries want a fill, at most LT_HINTS */
    size_t nhint;
    bool rescan;         /* walk the unknown bitmap (after the open or a full hint set) */
    bool walk_floor;     /* walk the known entries below drop_floor */
    bool wstop, has_warmer;
    pthread_t warmer;
    atomic_uint_fast64_t st_table, st_search, st_floor, st_nn, st_fill_ok, st_fill_refused;
} ltab;
#define LT_HINTS 65536

/* a lookup that waits for a publisher's window or the warmer: test hooks (tests/test_latest.c) */
typedef void (*latest_hook_fn)(void *arg, int at);
enum { LH_FOLD_BEFORE_LIST = 1, LH_WARM_BEFORE_STORE = 2, LH_LOOKUP_AFTER_ENTRY = 3, LH_WARM_AFTER_CUT = 4,
       LH_WINDOW_MID = 5, LH_LF_FREE = 6 /* before lt_end frees a fold list (tests: the write lock is not held) */ };

struct eng {
    char *dir;
    int dirfd;
    int lockfd;
    tsdb_options opt;
    int64_t partition_ms;
    uint32_t granule_rows;
    /* partition classes (classes.c): on, key multiplier, length and key step per class */
    bool cls_on;
    int64_t cls_kmul, cls_len[PART_CLASSES], cls_km[PART_CLASSES];

    pthread_mutex_t lock; /* parts list and part flags */
    /* New parts get their block numbers, names and place in the list in one
     * step under this lock, so the list never has a gap that a merge could
     * span (a part numbered between two merged ones arriving later would
     * then lose to older data). */
    pthread_mutex_t publish_lock;
    pthread_cond_t merge_cv;  /* new parts or stop: wakes merge threads */
    pthread_cond_t merged_cv; /* a merge finished */
    part **parts;             /* sorted by (partition, min_block) */
    size_t nparts, parts_cap;
    atomic_uint_fast64_t next_block;
    atomic_uint tmp_counter;
    atomic_bool stop;

    pthread_t *mergers;
    int nmergers;
    uint64_t merge_fail_seq; /* counts failed merges (merge_commit_locked); parts remember it (part.fail_seq) (db->lock) */
    struct { int64_t partition; size_t n; bool used; } stalled[8]; /* partitions db_wait_merges declared stalled, with
                                                                    * the count then: later waiters do not wait again
                                                                    * until it falls or a merge of it commits (db->lock) */
    unsigned stalled_next;
    int64_t stall_log_ms; /* when db_wait_merges last said that merges of a partition made no progress (db->lock) */

    pthread_mutex_t cc_lock;
    codec_ctx *cc_free;

    series_dict series;
    series_dict states;

    /* retention (retention.c) */
    pthread_mutex_t ret_lock;
    /* ms per series id, 0 = keep: read without a lock (every query does); grown by a copy under ret_lock, the
     * replaced tables stay valid until close */
    _Atomic(struct ret_table *) ret_tab;
    atomic_bool has_retention; /* some series has a limit: queries and merges look it up */
    int ret_fd;
    uint64_t ret_off;
    uint64_t ret_size; /* a writer's open: RETENTION's size, cut back to ret_off once the open's checks passed */
    uint64_t ret_ids;  /* one past the highest series id a RETENTION record names (the open's check, N1) */
    bool ret_broken;

    /* partition classes (classes.c). Lock order: flush_lock > cls_rw > class_lock > the WAL writer role; cls_mu is a
     * leaf (the history map and the growth of cls_tab). */
    pthread_mutex_t class_lock; /* decisions, switches and appends to CLASSES */
    pthread_mutex_t cls_mu;
    _Atomic(struct cls_table *) cls_tab; /* class byte per series, read without a lock */
    cls_hist *hmap;                      /* open addressing by series (cls_mu) */
    size_t hcap, hn;
    int cls_fd;
    uint64_t cls_off; /* bytes of CLASSES applied (a writer appends here) */
    uint64_t cls_ids; /* one past the highest series id a CLASSES record names (the open's check) */
    bool cls_broken;
    atomic_uint_fast64_t st_class_switches;
    /* One frozen history per placement: a large insert, a replace or a flush holds cls_rw for reading from the
     * moment it looks up its rows' classes until their parts are listed; a switch takes it for writing (classes.c).
     * cls_gen counts the switches (changed under cls_rw held for writing). */
    pthread_rwlock_t cls_rw;
    _Atomic uint64_t cls_gen;
    /* the end of the newest partition tsdb_drop_before or retention dropped in this process (db->lock): until the
     * directory fsync its parts can come back after a power loss, so a switch never writes a T before it */
    int64_t cls_dropped_end;
    void (*test_precommit)(void *); /* test hook: runs after a large insert placed its rows, before it commits */
    void *test_precommit_arg;
    void (*test_midplace)(void *); /* test hook: runs inside a placement, before rows are grouped into parts */
    void *test_midplace_arg;
    /* flushed memtables whose WAL segment may still be replayed (seq > wal_removed_synced): the newest timestamp
     * of each series in them bounds the T of its switch (db->lock). wtail_all: the bound for every series when an
     * entry could not be allocated, up to segment wtail_all_seq. */
    struct cls_wtail *wtail;
    size_t nwtail;
    uint64_t wtail_all_seq;
    int64_t wtail_all_max;
    uint64_t wal_unlink_next; /* the first flushed segment whose unlink failed, to try again (flush_lock); 0: none */
    atomic_int_fast64_t clock_floor; /* now never goes below this */
    atomic_uint ret_gen;             /* bumped by every change of a limit (parts cache their expiry) */
    atomic_uint_fast64_t ret_len_gen; /* bumped (db->lock) when a change lowers a series' cutoff: a merge that spans one
                                       * lists across it in a latest_rw section, marking its entries (merge_commit_locked) */
    int64_t last_ttl_ms;             /* merge thread: last scan for expired rows, CLOCK_MONOTONIC (under lock) */
    bool drop_pending;               /* eng_drop_before is waiting for merges, then retiring (under lock) */
    int64_t drop_pending_end;        /* ...every partition ending at or before this: no merge of one is selected */
    sched *sched;
    bool own_sched; /* opened alone (eng_open), not by a tsdb that shares its scheduler */

    /* memtable and WAL (memtable.c) */
    pthread_mutex_t flush_lock;    /* one memtable flush at a time */
    pthread_mutex_t wal_sync_lock; /* guards wal_writing; never held across I/O */
    pthread_cond_t wal_cv;         /* wal_writing cleared, or wal_durable advanced */
    bool wal_writing;              /* one WAL writer (commit or segment switch) at a time */
    pthread_mutex_t buf_lock;      /* active memtable and pending WAL bytes */
    pthread_cond_t flush_cv;
    struct lw_fence *lw_head;      /* large writes between their memtable flush and their publish (buf_lock) */
    uint64_t lw_wakeups;           /* wake-ups of small writes waiting for one (buf_lock; tests) */
    /* tsdb_set_write_wait_hook: around a small write's wait for a fence, so a server with a fixed set of threads
     * starts another (buf_lock; a wait keeps the hook it counted with for its -1) */
    void (*wait_fn)(void *ctx, int delta);
    void *wait_ctx;
    memtable *active;
    memtable *flushing[MAX_FLUSHING]; /* oldest first; guarded by db->lock */
    size_t nflushing;
    bool wal_enabled;
    int wal_fd;
    uint64_t wal_seq;
    uint64_t wal_off; /* bytes in segment wal_seq (the WAL writer role) */
    /* the last record whose write and sync succeeded, in any segment (the WAL writer role; read once the engine is
     * halted): revalidation after an outage finds it again, byte for byte, before it trusts the log (ha.md 15.5) */
    uint64_t wal_last_seq, wal_last_off, wal_last_len, wal_last_hash;
    uint8_t *wal_buf, *wal_spare;
    size_t wal_len, wal_cap, wal_spare_cap;
    uint64_t wal_appended;            /* rows (and one per marker) appended (buf_lock) */
    uint64_t fno_next;                /* the next record number to assign (buf_lock) */
    bool wal_rec_open;                /* the last record in wal_buf is a rows record that takes more (buf_lock) */
    size_t wal_rec_at;                /* its offset in wal_buf */
    /* FNO (fno.c): the file, the slot the newest store is in, its generation and the value it holds */
    int fno_fd, fno_slot;
    uint64_t fno_gen;
    atomic_uint_fast64_t fno_durable;
    pthread_mutex_t fno_lock; /* after dir_sync_lock, before fail_lock; held across the store's I/O */
    atomic_uint_fast64_t st_fno_syncs;
    bool parts_v8;                    /* META said parts_v8=1 at open: format-8 part headers are valid here (upgrade) */
    bool v8_meta;                     /* META still says it (db->lock): v8_parts_account takes it out when none is left */
    atomic_uint_fast64_t wal_durable; /* rows durable */
    int64_t last_wal_sync_ms;
    int bg_error;
    bool flusher_stop;
    bool has_flusher;
    pthread_t flusher;

    atomic_uint_fast64_t st_merges, st_merged_rows, st_passthrough;
    atomic_uint_fast64_t st_wal_bytes, st_wal_syncs; /* tsdb_get_volume_stats; bumped by the WAL writer role */
    atomic_int_fast64_t follow_ok_ms; /* follower: mono_ms() of the last refresh that succeeded */
    atomic_uint_fast64_t st_broken_parts, st_follow_errors;

    /* writer: MANIFEST for followers, delayed deletion of replaced parts */
    pthread_mutex_t manifest_lock;
    uint64_t manifest_version;
    uint64_t wal_flushed; /* segments up to this are entirely in parts (db->lock) */
    /* segments up to wal_removed are unlinked, up to wal_removed_synced durably (db->lock): an empty part (a
     * merge's wal_seq anchor) is needed only while a segment up to its wal_seq may come back at open */
    _Atomic uint64_t wal_removed, wal_removed_synced; /* written under db->lock; the second also by eng_sync_dir */
    graveyard_entry *graveyard;
    size_t ngraveyard, graveyard_cap;

    /* follower */
    bool follower_stop;
    bool has_follower;
    pthread_t follower;
    pthread_mutex_t refresh_lock;
    uint64_t follow_version;
    uint64_t follow_check; /* the checksum of the MANIFEST taken: versions start again at every writer's open */
    int64_t manifest_bad_since; /* follower: MANIFEST failed its checks since (monotonic ms), else 0 */
    bool manifest_reported;     /* follower: and that was logged */
    char broken_reported[96];   /* follower: the last broken part logged */
    int64_t series_bad_since;   /* follower: SERIES has failed series_reload's checks since (monotonic ms), else 0 */
    bool series_bad_reported;
    int64_t series_gap_since;   /* follower: the oldest series SERIES lacks is missing since (monotonic ms), else 0 */
    /* follower: when each count of series first went missing ({need, since}, need increasing): the patience runs
     * per record, from when the first one SERIES lacks went missing, not from the start of a gap that a writer
     * creating series steadily would keep open for good */
    struct {
        uint32_t need;
        int64_t since;
    } sgap[32];
    uint32_t nsgap;
    bool writer_nofsync;        /* follower: the writer's MANIFEST says it runs without fsync */
    uint64_t follow_flushed, follow_active;
    wal_tail tails[MAX_TAILS]; /* oldest first; the array is changed under db->lock + buf_lock */
    size_t ntails;
    uint64_t tail_lsn; /* rows added to tails (buf_lock): their lsn, so a cut (qcut) can bound them */

    /* fail-stop (eng_fail): set once, never cleared; fail_msg is written under
     * fail_lock before `failed` is stored, so whoever sees `failed` can read it */
    atomic_bool failed;
    atomic_int refs;       /* callers inside this engine through the tsdb handle (volumes.c, ha.md 15.5 step 2) */
    atomic_int mrefs;      /* name, type and class lookups on it (route_meta): they never block a recovery's idle wait,
                            * only the free of a retired engine (a steady stream of lookups must not starve it) */
    int crefs;             /* request cuts (qcuts) holding parts of this engine (eng_cut_hold), under idle_mu: they keep
                            * the engine's memory and directory alive past a recovery, never count in `refs` (a recovery
                            * must not wait for a call that is stuck in another volume) */
    bool retire_deferred;  /* tsdb_volume_reopen swapped it out while a cut was still held: the last cut retires it */
    void *retire_owner;    /* the tsdb handle it does that for */
    uint32_t adopted_series, adopted_states; /* the first names the next engine took over (eng_adopt_names) */
    atomic_bool quiesced;  /* no new caller: the volume is being recovered */
    /* statistics (stats.c): `stats` exists once the sweeper has started and is freed by eng_close; `stats_on` is
     * set (under db->lock, after `stats` is complete) while list changes push events into its ring */
    _Atomic(struct estats *) stats;
    atomic_bool stats_on;
    atomic_flag stats_starting;
    bool retiring;         /* eng_close keeps the names of the dictionaries (eng_retire) */
    pthread_mutex_t fail_lock; /* innermost; held only to fill fail_msg */
    pthread_mutex_t dir_sync_lock; /* eng_sync_dir; inside every lock but fail_lock */
    char fail_msg[256];

    /* the latest table (latest.c): NULL without one (latest_cache 0, a follower or a read-only open) */
    ltab *lt;
    latest_hook_fn lt_hook; /* test hook (tests/test_latest.c), NULL otherwise */
    void *lt_hook_arg;
};

/* the list functions' hooks (db.c): one branch while statistics are off */
static inline void stats_ev_add(eng *db, part *p)
{
    if (UNLIKELY(atomic_load_explicit(&db->stats_on, memory_order_acquire)))
        stats_push(db, p, STAT_EV_ADD);
}
static inline void stats_ev_remove(eng *db, part *p)
{
    if (UNLIKELY(atomic_load_explicit(&db->stats_on, memory_order_acquire)))
        stats_push(db, p, STAT_EV_REMOVE);
}

/* Fail-stop. After a storage error that leaves durable state uncertain (a
 * failed WAL write or fdatasync, a failed directory fsync after a publish, a
 * failed MANIFEST rewrite, a failed series dictionary append) the engine stops
 * writing for good: Linux may drop the dirty pages of a failed fsync and clear
 * the error, so a retry can "succeed" without the data (PostgreSQL's
 * "fsyncgate", 2018). eng_fail records the first such error (`what` failed
 * with `err`, an errno), prints one line to stderr and sets db->failed; call
 * it before waking anyone who waits for the failed operation. Any lock may be
 * held: it takes only fail_lock, which nests inside every other lock. From then
 * on every write path returns TSDB_ERR_IO at once (eng_check), background
 * threads stop writing, reads keep working, and reopening recovers what was
 * durable. */
void eng_fail(struct eng *db, const char *what, int err);
/* fsync of the database directory after naming or removing files, failing the
 * engine (with `what`) if it fails. All callers share one directory fd, and a
 * writeback error is reported once per open file: a concurrent or later fsync
 * returns 0 although the entries of the failed one may never reach the disk.
 * So these fsyncs take turns, and none succeeds once the engine has failed:
 * a 0 means every entry named before the call is durable. */
int eng_sync_dir(struct eng *db, const char *what);
/* TSDB_ERR_IO once the engine has failed, else 0. */
static inline int eng_check(struct eng *db)
{
    return atomic_load_explicit(&db->failed, memory_order_acquire) ? TSDB_ERR_IO : 0;
}
/* The abandon flag of the request this thread runs (tsdb_set_abandon_flag; ha.md 15.3): a request parked on a stuck
 * volume is told to discard its work. Checked at the two places where a request's effects become visible or
 * durable: in mem_insert before the WAL append, and under publish_lock before block numbers are given out. A step
 * already past its check completes. The pointer is thread-local, which is right only because every such step runs
 * on the request thread (scheduler helpers never publish): a path that publishes from a helper thread has to pass the
 * flag on explicitly. Embedded use never sets it, so nothing is abandoned there. */
extern __thread const volatile int *cur_abandon;
static inline int eng_abandoned(void)
{
    const volatile int *f = cur_abandon;
    return f && __atomic_load_n(f, __ATOMIC_ACQUIRE) ? TSDB_ERR_ABANDONED : 0;
}
/* 0 while healthy; TSDB_ERR_IO with the message (directory, what failed, errno) once failed. */
int eng_failed_msg(struct eng *db, char *msg, size_t len);

codec_ctx *cc_get(struct eng *db);
void cc_put(struct eng *db, codec_ctx *c);
extern __thread size_t cc_gets; /* contexts this thread took (db.c); for tests: statistics alone take none */

/* classes.c: the partition key of a row, and a key's class and time range [lo, hi) (hi saturates at INT64_MAX
 * for the last partition, which then holds INT64_MAX itself) */
int64_t partition_of(struct eng *db, uint32_t series, int64_t ts);
void partition_range(const struct eng *db, int64_t key, int64_t *lo, int64_t *hi);
/* A drop (eng_drop_before) is under way that retires this partition: no new merge or rewrite may start in it, or
 * the drop could wait for as long as merges keep being selected. Caller holds db->lock. */
bool partition_dropping(const struct eng *db, int64_t key);
int partition_class(const struct eng *db, int64_t key);
/* Lists committed parts, taking over the caller's references, and rewrites
 * MANIFEST. Cannot fail: once listed, the parts are published and must not
 * be discarded; a MANIFEST failure fails the engine by itself (eng_fail). */
void db_add_parts(struct eng *db, part **ps, size_t n);
void db_wait_merges(struct eng *db, int64_t partition);
void db_stall_clear_locked(struct eng *db, int64_t partition); /* a merge of the partition committed */
/* How long (ms) db_wait_merges lets a partition's part count stand while no merge of it runs, before an insert goes on;
 * a variable so that tests can shorten it. */
extern atomic_int wait_stall_ms;
/* test hook: the merge thread's merges sleep this many ms (0: off) and then fail with TSDB_ERR_IO, like a merge
 * output that cannot be written (ENOSPC, EIO before it has a name): nothing is marked, the sources stay */
extern atomic_int merge_fault_ms;
size_t partition_part_count_locked(struct eng *db, int64_t partition);
void db_parts_insert_locked(struct eng *db, part *p);
void db_parts_remove_locked(struct eng *db, part *p);
part **db_snapshot(struct eng *db, int64_t start, int64_t end, size_t *n);
void db_release(part **parts, size_t n);

/* merge.c */
void *merge_thread_main(void *arg);
int merge_parts(struct eng *db, part **src, size_t k, part **out);
bool merge_commit_locked(struct eng *db, part **src, size_t k, part *out, int rc);

/* insert.c: sorts (stably), keeps the last row per (series, ts), writes one
 * part per partition. Parts are published, or handed back in `collect`. With `refuse` (rows from a caller, not
 * a flush of the WAL), a row at TS_UNREACHABLE fails the insert with TSDB_ERR_INVALID. */
/* ucls >= 0: every series of the batch has that class at every time (cls_uniform); -1: looked up per series. */
/* `fence` (without collect): the caller's lw_enter, left as soon as the parts are published or have failed. */
int insert_rows(struct eng *db, int type, const uint32_t *series, uint32_t const_series, const int64_t *ts,
                const void *v, size_t n, uint64_t wal_seq, part_list *collect, bool refuse, int ucls,
                const uint64_t *gen, struct lw_fence *fence);

/* Last write wins in real time (memtable.c). Memtable rows rank above every part, so a small write to a key of a
 * large insert, replace or delete must not be acknowledged while that write is between its memtable flush and its
 * publish: it would be older than the large write and still win. The large write describes its keys in a fence
 * (conservatively: a timestamp range per bucket of series), registers it with lw_enter in the same step that
 * decides whether the memtable needs a flush (a row added before is flushed under an older block number, one added
 * after sees the fence), and leaves with lw_leave once its parts are published or have failed. mem_insert waits,
 * holding no lock and before it appends anything, while one of its rows falls inside a registered fence, so
 * nothing the large write waits for (a flush, the WAL writer role, publish_lock) can wait for it. Rows that wait
 * reach the WAL after the publish, so a replay ranks them above the large write as well. A false overlap (another
 * series in the same bucket) only delays a write. A waiter sleeps on the cv of a fence it overlaps, so a large
 * write that leaves wakes only the writes it held. Large writes never wait for small ones: while overlapping large
 * writes follow each other without a gap, a small write into their keys waits for all of them (no ticket order:
 * passing a fence that came later, whose memtable flush has run, would bring the anomaly back). */
#define LW_BUCKETS 4096 /* a power of two: a row's bucket is a mask */
typedef struct lw_fence {
    struct lw_fence *next;
    int64_t lo, hi;   /* every key's timestamp is in [lo, hi]; lo > hi: no keys */
    uint32_t series;  /* nb == 0: the one series */
    uint32_t nb;      /* else LW_BUCKETS buckets of series (local id & (LW_BUCKETS - 1)), each with a range */
    int64_t *r;       /* nb buckets: lo, hi; valid until lw_leave */
    bool r_heap;      /* r was allocated (the thread's table was taken), else it is lw_table_take's */
    pthread_cond_t cv; /* from lw_enter to lw_leave: the writes waiting for this fence (buf_lock) */
    /* held: a waiter counted this large write's thread as held too, through hold_fn(hold_ctx, +1) (buf_lock); the
     * -1 goes to the same hook, whatever tsdb_set_write_wait_hook set since */
    bool held;
    void (*hold_fn)(void *ctx, int delta);
    void *hold_ctx;
} lw_fence;
/* The thread's fence table (insert.c): taken until lw_table_put; NULL while taken (or on failure). */
void *lw_table_take(size_t bytes);
void lw_table_put(void);
int lw_fence_rows(lw_fence *f, const uint32_t *series, uint32_t const_series, const int64_t *ts, size_t n);
void lw_fence_range(lw_fence *f, uint32_t series, int64_t start, int64_t end); /* [start, end) */
bool lw_enter(struct eng *db, lw_fence *f); /* returns whether the memtable holds rows to flush first */
void lw_leave(struct eng *db, lw_fence *f);
int part_list_push(part_list *l, part *p);
int parts_publish(struct eng *db, part **ps, size_t n);

/* the storage engine for one directory; the public API in tsdb.h wraps these */
int eng_open(const char *dir, const tsdb_options *opts, eng **out);
/* the same with the scheduler of the tsdb that owns the engine */
int eng_open_sched(const char *dir, const tsdb_options *opts, sched *s, eng **out);
int eng_close(eng *db);
/* As eng_close, for an engine replaced by a fresh open of its directory (ha.md 15.5): the name strings stay, for the
 * `tsdb_series_name` pointers callers hold until tsdb_close. */
int eng_retire(eng *db, pthread_mutex_t *mu, char ***kept, size_t *nkept, size_t *capkept);
/* The new engine of a reopen takes over the name strings of the old one that it also has (same name at the same
 * index; the dictionaries are append-only), frees its own copies, and eng_retire(old) then keeps only the rest: the
 * pointers tsdb_series_name handed out stay valid, and a recovery adds no names to kept_names. Before neu is listed. */
void eng_adopt_names(eng *neu, eng *old);
/* Recovery of a halted engine (revalidate.c, ha.md 15.5 step 3 and 4); see tsdb_volume_reopen. */
int wal_verify_tail(eng *db, const char *dir); /* 0 intact, 1 the storage lost what was durable, <0 error */
int wal_relog(eng *db, const char *dir, uint64_t *rows); /* the rows memory holds into a new segment */
typedef struct reval_part {
    char name[96];
    int64_t partition;
    uint64_t min_block, max_block, rows;
    uint32_t nt;
} reval_part;
int reval_dicts(eng *old, const char *dir, uint64_t *series_added, uint64_t *states_added);
int reval_capture(eng *old, reval_part **out, size_t *n);
void reval_parts(const eng *neu, const reval_part *x, size_t nx, uint64_t *lost, uint64_t *lost_rows);
/* reval_parts as the `check` of eng_reopen: it runs before the new engine's threads start (see there). */
typedef struct reval_check {
    const reval_part *x;
    size_t nx;
    uint64_t lost, lost_rows; /* out */
} reval_check;
void reval_check_open(const eng *neu, void *arg);
void reval_logs(const eng *old, const eng *neu, uint64_t *lost_bytes);
void eng_halt(eng *db);
/* A new engine for the directory of `old` (halted, closed afterwards by eng_retire) that takes over its lock.
 * `check(neu, arg)`, when given, runs on the opened engine before its merge and flush threads start: the only moment
 * its parts list is read without a lock and still the one the storage held. */
int eng_reopen(eng *old, const char *dir, const tsdb_options *opts, struct sched *qs, eng **out,
               void (*check)(const eng *neu, void *arg), void *arg);
int eng_series(eng *db, const char *name, int type, uint32_t *id);
int eng_series_lookup(eng *db, const char *name, uint32_t *id);
uint32_t eng_series_count(eng *db);
/* whether part p holds rows or tombstones of a series id >= n (the first such id in *s); db.c */
bool part_beyond_series(const part *p, uint32_t n, uint32_t *s);
const char *eng_series_name(eng *db, uint32_t id);
int eng_series_type(eng *db, uint32_t id);
int eng_state(eng *db, const char *name, bool create, uint32_t *code);
const char *eng_state_name(eng *db, uint32_t code);
/* v: slots of `type`; every series must be of that type */
int eng_insert(eng *db, int type, const uint32_t *series, const int64_t *ts, const void *v, size_t n);
/* The calling thread's grow-only buffer `which` (0, 1, 2) of at least `bytes`, for a caller that rearranges a batch
 * before eng_insert (the volume split, narrowed ids): a fresh malloc per batch of 8M rows or more is an mmap whose
 * pages fault in again every time, on every thread at once. Contents are lost when it grows; NULL on failure. */
void *eng_insert_scratch(int which, size_t bytes);
int eng_insert_series(eng *db, int type, uint32_t series, const int64_t *ts, const void *v, size_t n);
int eng_commit(eng *db);
int eng_flush(eng *db);
extern size_t desc_window_rows;
/* Raw rows in time order; v as slots of the series' type. */
/* ---- query internals shared with twa.c (query.c) ---- */
/* A part can have several entries: granules overlapped by a newer tombstone are decoded, filtered and moved to
 * `mem`, so parts[i] answers only for granules [gi[i], gj[i]) and those hold no deleted or expired row. Readers
 * use gi/gj, never part_find on parts[i]: only then do statistics stay exact. */
typedef struct snapshot {
    part **parts;
    uint32_t *gi, *gj; /* per entry: the granules of the series in the range */
    size_t np;
    mem_run *mem; /* rows of the series from memtables, oldest memtable first, and from parts with deletes */
    uint64_t *mprio; /* per run: its priority on equal timestamps */
    size_t nmem;
    int64_t start, end; /* the range gi/gj were found for */
} snapshot;
int snapshot_take(struct eng *db, uint32_t series, size_t w, int64_t start, int64_t end, snapshot *s);
void snapshot_release(snapshot *s);

/* One engine as of one instant, for every series of a multi-series request (tsdb_query_multi): the parts list,
 * the memtables with the last WAL sequence they may show, taken under db->lock like a snapshot. Snapshots of the
 * request's series are cut from it, so an insert is seen by all of them or by none. The parts are referenced once
 * per engine, not per series. */
typedef struct qcut {
    struct eng *db;
    part **parts;
    size_t np;
    memtable *ms[MAX_FLUSHING + 1]; /* the flushing memtables, oldest first, then the active one */
    size_t nms;
    bool has_active;               /* ms[nms - 1] was the active memtable: read under buf_lock */
    memtable *tails[MAX_TAILS];
    size_t ntails;
    uint64_t visible, tail_visible; /* rows with a higher lsn came after the cut */
} qcut;
typedef struct qcuts {
    pthread_mutex_t lock;
    qcut **c; /* each cut on its own: a pointer handed out stays valid while the array grows */
    size_t n, cap;
} qcuts;
/* One engine's cut on its own (latest.c): taken, then released, by the caller; the qcut is not heap-allocated. */
int cut_take(struct eng *db, qcut *c);
void cut_release(qcut *c);
/* A request's cuts (qcuts) keep each engine they cut allocated, with its directory open, until the request ends;
 * a recovery does not wait for them (volumes.c: the engine it replaced is retired by the last cut's drop) */
void eng_cut_hold(struct eng *db);
void eng_cut_drop(struct eng *db);
extern __thread qcuts *cur_cuts; /* the multi-series request this thread works for; NULL elsewhere */
void qcuts_init(qcuts *cs);
void qcuts_free(qcuts *cs);
uint64_t mem_prio(const snapshot *sn, size_t i);
/* whether mem run i came from a memtable, not from part granules decoded around tombstones */
bool mem_from_memtable(const snapshot *sn, size_t i);
/* Entry i's granules that may hold rows in [start, end): part_find limited to the entry's window. */
void snap_find(const snapshot *sn, size_t i, uint32_t series, int64_t start, int64_t end, uint32_t *gi,
               uint32_t *gj);
/* Granules [*gi, *gj) of series s that may hold rows in [start, end). */
void part_find(const part *p, uint32_t s, int64_t start, int64_t end, uint32_t *gi, uint32_t *gj);
typedef struct src {
    const part *p;   /* NULL for a memtable run */
    uint32_t gi, gj; /* granules still to load */
    uint64_t prio;   /* newer sources win on equal timestamps */
    bool loaded;
    size_t pos, n;   /* rows [pos, n) of ts/v are in range and unread */
    int64_t *ts;
    uint8_t *v;      /* slots of `type` */
    int type;
    size_t w;        /* slot width */
    bool own;        /* ts/v are decode buffers owned by the cursor, */
    uint32_t cap;    /* of room for cap rows */
} src;
void src_init_part(src *c, const part *p, uint32_t gi, uint32_t gj, int type);
void src_init_mem(src *c, const mem_run *r, uint64_t prio, int type);
void src_free(src *c);
typedef int (*slot_fn)(void *ctx, const int64_t *ts, const void *v, size_t n);
/* Emits the union of the sources in time order, newest row per timestamp. */
int merge_stream(codec_ctx *cc, src *cur, size_t nc, int64_t start, int64_t end, slot_fn fn, void *ctx);
int eng_query_raw_ex(eng *db, uint32_t series, int64_t start, int64_t end, bool descending, size_t limit,
                     slot_fn fn, void *ctx);
/* The newest row of each of n series of one engine, at one cut of it (latest.c; design-latest.md section 5). `ends`
 * (per series) or `end`: only rows before it. Default: the newest row whose last-write-wins value is not null;
 * with_nulls: the newest row. out[i].status is TSDB_ERR_NOTFOUND for a local id that names no series. */
typedef struct latest_row {
    int32_t status;
    uint8_t kind; /* tsdb_latest_kind */
    uint8_t type;
    int64_t ts;
    uint64_t slot; /* the value, zero-extended slot bits of the series' type */
} latest_row;
int eng_latest(eng *db, const uint32_t *local, size_t n, const int64_t *ends, int64_t end, bool with_nulls,
               latest_row *out);
int eng_latest_search(eng *db, const uint32_t *local, size_t n, const int64_t *ends, int64_t end, bool with_nulls,
                      latest_row *out); /* every id searched on one cut (the slow path) */
/* The warmer's fill: the newest non-null row in the parts of the cut (no memtable in it), no cutoff. */
int eng_latest_fill(eng *db, const qcut *c, const uint32_t *local, size_t n, latest_row *out);
/* The latest table (design-latest.md steps 2): created at open when the options and the role allow, rebuilt from
 * the parts after cls_check_parts and before the WAL replay, closed with the engine. */
int latest_open(eng *db);       /* the table, empty (NULL db->lt: off) */
int latest_build(eng *db);      /* every part's runs folded in; the volume's pages reserved first */
void latest_start(eng *db);     /* the warmer thread, once the engine is up */
void latest_stop(eng *db);      /* stops the warmer (eng_halt) */
bool latest_stats(eng *db, uint64_t out[6]); /* tests: see tsdb_test_latest_stats */
void latest_close(eng *db);     /* frees the table and gives its pages back to the budget */
/* A publisher's write section: lt_begin takes latest_rw for writing (no-op without a table); lt_fold, after
 * parts_commit succeeded and before the parts are listed, folds their fold lists and tombstones in block order;
 * the caller then lists them (under db->lock); lt_end bumps lgen, releases the lock and hints the warmer with the
 * ids the fold made unknown. Parts without a fold list (tombstone-only parts) fold only their tombstones. */
typedef struct lt_sec {
    uint32_t *id;
    size_t n, cap;
    bool oom;
    bool win;           /* a large flush's window (3.2) is open: lt_end closes it */
    const lfold **note; /* the window's folds left for the write section (a null over a known row) */
    size_t nnote, capnote;
    bool noteoom;       /* a note did not fit: the write section folds every list */
    struct lt_lf { void *p; size_t bytes; } *dfree; /* the fold lists, freed by lt_end after the lock is released: at */
    size_t ndfree;                                  /* 10^6 series one is 40 MB, and its munmap stalled every lookup */
} lt_sec;
void lt_begin(eng *db, lt_sec *sec);
/* lt_begin for a flush's parts: a long fold list is stored in a window without the lock (3.2) */
void lt_begin_flush(eng *db, lt_sec *sec, part **ps, size_t n);
void lt_fold(eng *db, lt_sec *sec, part **ps, size_t n);
void lt_end(eng *db, lt_sec *sec);
/* in a write section of lt_begin: */
void lt_retention_lowered(eng *db, lt_sec *sec, uint32_t series, int64_t oldc);
void lt_merge_marks(eng *db, lt_sec *sec, part **src, size_t k);
void lt_drop_floor(eng *db, int64_t ts);
/* the part writer's side: add the rows of one granule of `series` (strictly increasing ts, slots of type) */
int lf_granule(struct lfold **lf, size_t *n, size_t *cap, uint32_t series, int type, const int64_t *ts,
               const void *v, size_t cnt, uint32_t nulls);
/* LTTB downsampling to `threshold` points (>= 3) of the known, finite values; state series: TSDB_ERR_INVALID.
 * desc: the same points newest first; `keep` (0: all) is how many of them the caller takes at most, which bounds
 * what newest first holds in memory (fn still decides when to stop). */
int eng_query_lttb(eng *db, uint32_t series, int64_t start, int64_t end, size_t threshold, bool desc, size_t keep,
                   slot_fn fn, void *ctx);
extern size_t lttb_buffer_bytes; /* tests make it small to exercise the two-pass way */
extern size_t local_piece_bounds; /* local periods per window of tsdb_query_group_local (0: default); tests */
extern size_t agg_dense_buckets; /* the most buckets one dense array holds (0: the default); for tests */
extern size_t dense_chunk;       /* buckets per item of the passes over a dense array (0: the default); for tests */
extern size_t agg_dense_states;  /* bucket states of all dense arrays so far (src/query.c); for tests */
extern size_t local_twa_listed;  /* bounds local TWAs have listed so far (src/calendar.c); for tests */
/* Buckets newest first with desc; at most limit non-empty ones (0: all), computing only about as many. */
int eng_query_group(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, bool desc,
                    size_t limit, tsdb_bucket **out, size_t *nout);
/* the same with the spread: tsdb_spread_bucket results */
int eng_query_spread(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, bool desc,
                     size_t limit, tsdb_spread_bucket **out, size_t *nout);
/* Order and limit for a bucketed query (query.c has the design): run computes the non-empty buckets of
 * [lo, hi), whose ends are bucket edges or the query's, ascending, as rows of `row` bytes (malloc'ed; NULL
 * when none); bound finds the newest (newest true) or oldest time in [lo, hi) after or before which every
 * bucket is empty, false when all are (a sentinel such as lo - 1 would overflow at lo = INT64_MIN). */
typedef struct bk_windows {
    const bucketer *bk;
    bool desc;
    size_t limit;
    size_t row;
    void *ctx;
    int (*run)(void *ctx, int64_t lo, int64_t hi, void **res, size_t *n);
    bool (*bound)(void *ctx, int64_t lo, int64_t hi, bool newest, int64_t *at);
} bk_windows;
int bucket_windows(const bk_windows *w, int64_t start, int64_t end, void **out, size_t *nout);
int bucketer_init(bucketer *bk, int64_t start, const tsdb_group *g);
/* Bounds must be strictly increasing (bounds_check: 0 or TSDB_ERR_INVALID). checked: the caller has checked them,
 * once for many series (tsdb_query_multi), as a check per series costs a pass over every bound for each. */
int bounds_check(const int64_t *bounds, size_t n);
extern size_t bounds_checked; /* bounds that bounds_check has looked at so far (src/query.c); for tests */
int bucketer_bounds(bucketer *bk, const int64_t *bounds, size_t n, bool checked);
/* tsdb_query_bounds, tsdb_query_spread_bounds (kind TSDB_MULTI_SPREAD) and tsdb_query_twa_bounds (TSDB_MULTI_TWA,
 * opts unused, twa used) in one, with the bounds checked or not (volumes.c) */
int query_bounds_of(tsdb *t, int kind, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                    bool checked, const tsdb_query_opts *opts, const tsdb_twa_opts *twa, void **out, size_t *nout);
int64_t bucketer_key(const bucketer *bk, int64_t ts);    /* the bucket of ts */
int64_t bucketer_start(const bucketer *bk, int64_t key); /* the first instant of bucket key */
/* Time-weighted averages (twa.c). */
int eng_query_twa(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk,
                  const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
int eng_optimize(eng *db);
int eng_drop_before(eng *db, int64_t ts);
int eng_get_stats(eng *db, tsdb_stats *st);
int eng_get_volume_stats(eng *db, tsdb_volume_stats *vs);

static inline int64_t eng_wall_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static inline int64_t eng_mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}
int eng_foreach_part(eng *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx);
int eng_refresh(eng *db);
int eng_series_reload(eng *db); /* followers: SERIES appended since, when it grew */
/* retention.c */
static inline uint8_t cls_byte(eng *db, uint32_t s)
{
    cls_table *t = atomic_load_explicit(&db->cls_tab, memory_order_acquire);
    return t && s < t->cap ? atomic_load_explicit(&t->b[s], memory_order_relaxed) : 0;
}
/* The class of series s at ts and the time range [*lo, *hi) around ts where it holds. */
int cls_at(eng *db, uint32_t s, int64_t ts, int64_t *lo, int64_t *hi);
/* The class of every series of a batch when they share one with no switch (and none is fresh), else -1. */
int cls_uniform(eng *db, const uint32_t *ids, uint32_t cs, size_t n);
void cls_setup(eng *db, int classes);
int cls_open(eng *db, bool read_only);
int cls_cut_tail(eng *db); /* a writer's open, once its checks passed: cuts the torn tail cls_open found */
int cls_reload(eng *db);
void cls_close(eng *db);
/* Fresh series of a large insert get their class from its rows (once, from the beginning of time). */
int cls_classify_insert(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, size_t n);
/* The same for the fresh series of a memtable about to be flushed (caller holds flush_lock). */
int cls_classify_memtable(eng *db, const memtable *m);
/* After a flush (flush_lock held): a series of a long class that got much faster moves to a shorter one. */
int cls_flush_guard(eng *db, const memtable *m);
/* After an idle merge left one part in its partition: series whose rate left their class's band move. */
int cls_observe(eng *db, part *p);
/* A flushed memtable whose segment may still be replayed: its series' newest timestamps, built before the flush
 * takes its locks (NULL on ENOMEM), then added under db->lock (which also drops entries no longer replayable). */
struct cls_wtail *cls_wal_tail_new(const memtable *m);
void cls_wal_tail_locked(eng *db, struct cls_wtail *e, uint64_t seq, int64_t max_ts);
/* Placements (large inserts, replaces, flushes) hold the class history frozen from their first class lookup until
 * their parts are listed; a switch waits a few ms for them, then tries later. Never nested, and never held while waiting for a flush or a
 * merge (a switch holds flush_lock and the merge thread switches). */
void cls_freeze(eng *db);
void cls_thaw(eng *db);
/* At open: every part's rows lie in its partition's range, sorted, and in the class the history gives their
 * series there (TSDB_ERR_CORRUPT with a log line otherwise). */
int cls_check_parts(eng *db);
int eng_series_class(eng *db, uint32_t series, int *cls);
/* The switch generation, read (acquire) before series_check reads the class bytes: a large insert that finds it
 * changed once it holds cls_rw looks every class up again. */
uint64_t cls_gen_now(eng *db);
/* test hook: switch series to cls at the computed T, or at *at when that is later */
int eng_class_switch(eng *db, uint32_t series, int cls, const int64_t *at);
int tsdb_test_class_switch(struct tsdb *t, tsdb_sid series, int cls, const int64_t *at); /* through the volumes */
/* An id no volume can have by its bits: a zero volume field (an id cut to 32 bits) or bits 48-63 set. Queries answer
 * TSDB_ERR_INVALID for it, so a truncated id does not look like an empty range; a well formed id that is unknown
 * (a series a follower has not seen yet) answers no rows. */
static inline bool sid_malformed(tsdb_sid id) { return (id >> 48) != 0 || (id >> 32) == 0; }
/* test hook: fn(arg) runs in every large insert, replace and delete after its parts were written, before it commits */
void tsdb_test_precommit(struct tsdb *t, void (*fn)(void *), void *arg);
/* test: how often small writes waiting for a large write's publish woke up, over every open volume (lw_wakeups) */
uint64_t tsdb_test_fence_wakeups(struct tsdb *t);
/* test hook: fn(arg) runs inside every placement, before its sorted rows are grouped into parts */
void tsdb_test_midplace(struct tsdb *t, void (*fn)(void *), void *arg);
bool tsdb_test_latest_stats(struct tsdb *t, uint64_t out[6]);
void tsdb_test_latest_hook(struct tsdb *t, void (*fn)(void *, int), void *arg);
bool tsdb_test_latest_wr_held(struct tsdb *t); /* the table's lock is held for writing (tests) */
void tsdb_test_latest_window(int min); /* flushes with this many series or more use the 3.2 window (process-wide) */
void tsdb_test_latest_nowarm(int on); /* engines opened while on start no warmer (process-wide) */
/* tests: fn(arg, 0) runs in tsdb_volume_reopen before it halts the old engine, fn(arg, 1) once the new engine is
 * open and its threads run; a reference on a series' engine as a
 * name or type lookup holds it between its route and its use (tsdb_test_meta_use reads the type through it) */
void tsdb_test_reopen_hook(void (*fn)(void *, int phase), void *arg);
void *tsdb_test_meta_hold(struct tsdb *t, tsdb_sid id);
int tsdb_test_meta_use(void *hold);
void tsdb_test_meta_release(void *hold);
int tsdb_test_deferred_retires(struct tsdb *t); /* retired engines a multi call's cut still keeps */
size_t tsdb_test_kept_names(struct tsdb *t); /* name strings of retired engines still kept (not adopted by a new one) */
/* test hook: the WAL tail kept for switches, and how far segments are unlinked (durably) */
void tsdb_test_wal_state(struct tsdb *t, size_t *nwtail, uint64_t *removed, uint64_t *removed_synced);
/* test hook: every granule of the first volume, with its partition's class (tests/test_classes.c) */
typedef struct tsdb_test_granule {
    uint32_t series, count;
    int64_t ts_min, ts_max;
    uint32_t grows;
} tsdb_test_granule;
void tsdb_test_granules(struct tsdb *t, void (*fn)(void *ctx, int cls, uint32_t i, const tsdb_test_granule *g),
                        void *ctx);

int ret_open(eng *db, bool read_only);
int ret_reload(eng *db);
int ret_cut_tail(eng *db); /* a writer's open, once its checks passed: cuts the torn tail ret_open found */
void ret_close(eng *db);
void ret_clock_init(eng *db);
int64_t eng_now_ms(eng *db);
int64_t eng_retention(eng *db, uint32_t series);
int64_t eng_cutoff(eng *db, uint32_t series); /* rows before it have expired; INT64_MIN when none */
int64_t eng_cutoff_at(eng *db, uint32_t series, int64_t now);
int eng_set_retention(eng *db, uint32_t series, int64_t limit_ms);
/* delete.c */
int eng_delete_range(eng *db, uint32_t series, int64_t start, int64_t end);
int eng_replace_range(eng *db, int type, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                      const void *v, size_t n);
/* merge.c: drops partitions every series of which has expired (sets *dropped) and selects a part to rewrite
 * without its expired rows (returns 1, sel[0] marked merging and referenced); caller holds db->lock */
size_t ttl_select_locked(eng *db, part **sel, bool *dropped, bool *sync_wal);

#endif
