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
#include <zstd.h>

#include "levels.h" /* before the declarations: renames them in a per-CPU-level compile */

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

/* 3: checksummed part header and footer, WAL records and dictionary records;
 * 4: time-weighted statistics in granule entries (136 bytes), range tombstones
 * between the index and the footer; 5: the spread (m2 or the sum of squares) in
 * granule entries (160 bytes), tombstones as in 4. Databases of older versions
 * are refused (no release had them). */
#define FORMAT_VERSION 6
#define MAX_GRANULE_ROWS 65536
#define MAX_MERGE_PARTS 100
#define PARTS_TO_DELAY_INSERT 300 /* per partition, like parts_to_delay_insert */

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

/* One entry of a part's sparse index; stored on disk exactly like this (little
 * endian). The statistics cover the known values; their type is the series'. */
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
} part_header;

typedef struct part_footer {
    uint64_t index_off;
    uint64_t rows;
    int64_t min_ts;
    int64_t max_ts;
    uint32_t ngranules;
    uint32_t version;
    uint64_t index_hash; /* xxh64 of the header, the index and the footer up to here */
    char magic[8];
} part_footer;
_Static_assert(sizeof(part_footer) == 56, "footer must be 56 bytes");
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

typedef struct part {
    char name[96];
    int dirfd;
    int64_t partition;
    uint64_t min_block, max_block;
    uint32_t level;
    uint32_t grows; /* granule_rows the part was written with */
    uint64_t wal_seq;
    const uint8_t *map;
    size_t map_len;
    const granule *g;
    uint32_t ng;
    const tomb *t; /* the part's tombstones (in the map); NULL when none */
    uint32_t nt;
    uint64_t index_off;
    uint64_t rows;
    int64_t min_ts, max_ts;
    bool verify; /* queries check each granule's checksum (tsdb_options.verify_checksums) */
    time_t created;
    time_t written; /* when its newest rows were inserted: a merge keeps its sources' (after a reopen, created) */
    atomic_int refs;
    /* protected by tsdb.lock */
    bool merging;
    bool dropped;
    uint32_t ttl_gen;        /* retention generation ttl_any/ttl_all were computed for; 0: never */
    int64_t ttl_any, ttl_all; /* retention "now" after which some row / every row and tombstone has expired */
    atomic_bool obsolete; /* delete the file when the last reference goes */
    char pending[64];     /* tmp file name while the part awaits parts_commit, else "" */
} part;

int part_open(int dirfd, const char *name, part **out);
int part_open_as(int dirfd, const char *file, const char *name, part **out);
void part_ref(part *p);
void part_unref(part *p);
/* Decodes a granule: timestamps, and values (if v) as slots of `type`; checks
 * the granule's checksum first if verify. */
int granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, bool verify);
extern atomic_uint_fast64_t granule_checks; /* granule checksums computed, for tests */

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
    uint32_t pend_series;
    int pend_type;
    size_t pend_n;
    int64_t *pend_ts;
    uint8_t *pend_v; /* slots of pend_type */
    uint32_t grows;
    int zstd_level;
    int value_codec;
    codec_ctx *cc;
    uint64_t rows;
    int64_t min_ts, max_ts;
    tomb *tombs; /* written after the index; sorted, disjoint per series */
    size_t nt;
    bool keep_empty;  /* write a part even with no rows and no tombstones (a merge's wal_seq anchor) */
    int effort;       /* CODEC_FAST (pw_begin) or CODEC_MERGE */
    int64_t empty_ts; /* its min_ts = max_ts then */
} part_writer;

int pw_begin(part_writer *w, struct eng *db, int64_t partition, uint64_t wal_seq);
/* Rows of one series of `type` (v in its slots); (series, ts) order across calls. */
int pw_add(part_writer *w, uint32_t series, int type, const int64_t *ts, const void *v, size_t n);
int pw_copy_granule(part_writer *w, const part *src, const granule *g);
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
void pw_abort(part_writer *w);

/* Every range is half-open with an int64 end, so a row at INT64_MAX could never be read, counted or deleted:
   inserts refuse it (TSDB_ERR_INVALID) rather than keep a row no query reaches. A WAL written before this rule
   may still hold one; replay and flush keep it, as the build that acknowledged it did. */
#define TS_UNREACHABLE INT64_MAX

/* ---- series dictionary ---- */
typedef struct series_dict {
    pthread_mutex_t lock;
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
    bool broken;       /* an append failed (under lock): the file may end in a torn record, append no more */
} series_dict;

/* The series dictionary (file SERIES) and the state names (file STATES) are both these. */
int series_open(series_dict *d, int dirfd, const char *file, bool read_only, bool fsync);
int series_reload(series_dict *d, int dirfd); /* readers: names appended by the writer since */
void series_close(series_dict *d);
/* create: make a missing name, of `type` (an existing name of another type is TSDB_ERR_INVALID) */
int series_get(series_dict *d, const char *name, bool create, int type, uint32_t *id);
const char *series_name(series_dict *d, uint32_t id);
int series_type(series_dict *d, uint32_t id); /* -1 for an unknown id */
bool series_check(series_dict *d, const uint32_t *series, uint32_t cs, size_t n, int type);

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

/* One outermost query's statistics and scheduler group (volumes.c). */
typedef struct qrun {
    qstats q;
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
typedef struct mt_series {
    int64_t *ts;
    uint64_t *v; /* slots, zero-extended */
    uint64_t *lsn; /* WAL sequence of each row; increasing, 0 when already durable */
    uint32_t n, cap;
    bool sorted; /* non-decreasing ts, in insertion order */
} mt_series;

typedef struct memtable {
    atomic_int refs;
    mt_series *s; /* indexed by series id */
    uint32_t scap;
    uint32_t *ids; /* series present, first-insert order */
    uint32_t nids, idcap;
    size_t rows;
    int64_t min_ts, max_ts;
    uint64_t wal_seq;
    int64_t created_ms;
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
 * follow), 0 for rows that are durable already */
int mt_add(memtable *m, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w, size_t n,
           uint64_t lsn0);
/* only rows with a WAL sequence <= visible; values as slots of width w */
int mt_collect(const memtable *m, uint32_t id, size_t w, int64_t start, int64_t end, uint64_t visible, mem_run *out);
void mem_run_free(mem_run *r);
int mt_flush(struct eng *db);
int mem_insert(struct eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v, size_t w,
               size_t n);
/* WAL records: [u32 rows][u32 header check][u64 rows hash][rows of 20 bytes:
 * u32 series, i64 ts, u64 slot]. The header check covers the row count and the
 * hash, and binds the record to its segment and offset. wal_record returns the
 * size of the record at segment offset `off`, or 0 when it is torn or absent. */
#define WAL_HDR 16
#define WAL_ROW 20
size_t wal_record(const uint8_t *p, size_t avail, uint64_t seq, uint64_t off, size_t *nrows);
/* Whether a valid record starts anywhere in p[from, len) (p at segment offset
 * base): then a bad record before it is damage, not a torn tail. */
bool wal_valid_after(const uint8_t *p, size_t len, size_t from, uint64_t seq, uint64_t base);
void wal_row(const uint8_t *r, uint32_t *id, int64_t *ts, uint64_t *slot);
int wal_commit(struct eng *db, uint64_t target);
int wal_open(struct eng *db);
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
    int64_t deadline; /* realtime seconds */
} graveyard_entry;

int manifest_publish(struct eng *db);
int follow_refresh(struct eng *db);
void *follower_main(void *arg);
void db_retire_locked(struct eng *db, part *p);
void graveyard_purge(struct eng *db, bool all);

/* ---- database ---- */
typedef struct ret_table {
    uint64_t cap;
    struct ret_table *older;
    _Atomic int64_t limit[];
} ret_table;

struct eng {
    char *dir;
    int dirfd;
    int lockfd;
    tsdb_options opt;
    int64_t partition_ms;
    uint32_t granule_rows;

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
    bool ret_broken;
    atomic_int_fast64_t clock_floor; /* now never goes below this */
    atomic_uint ret_gen;             /* bumped by every change of a limit (parts cache their expiry) */
    int64_t last_ttl_ms;             /* merge thread: last scan for expired rows, CLOCK_MONOTONIC (under lock) */
    sched *sched;
    bool own_sched; /* opened alone (eng_open), not by a tsdb that shares its scheduler */

    /* memtable and WAL (memtable.c) */
    pthread_mutex_t flush_lock;    /* one memtable flush at a time */
    pthread_mutex_t wal_sync_lock; /* guards wal_writing; never held across I/O */
    pthread_cond_t wal_cv;         /* wal_writing cleared, or wal_durable advanced */
    bool wal_writing;              /* one WAL writer (commit or segment switch) at a time */
    pthread_mutex_t buf_lock;      /* active memtable and pending WAL bytes */
    pthread_cond_t flush_cv;
    memtable *active;
    memtable *flushing[MAX_FLUSHING]; /* oldest first; guarded by db->lock */
    size_t nflushing;
    bool wal_enabled;
    int wal_fd;
    uint64_t wal_seq;
    uint64_t wal_off; /* bytes in segment wal_seq (the WAL writer role) */
    uint8_t *wal_buf, *wal_spare;
    size_t wal_len, wal_cap, wal_spare_cap;
    uint64_t wal_appended;            /* rows appended (buf_lock) */
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
    uint64_t wal_removed, wal_removed_synced;
    graveyard_entry *graveyard;
    size_t ngraveyard, graveyard_cap;

    /* follower */
    bool follower_stop;
    bool has_follower;
    pthread_t follower;
    pthread_mutex_t refresh_lock;
    uint64_t follow_version;
    int64_t manifest_bad_since; /* follower: MANIFEST failed its checks since (monotonic ms), else 0 */
    bool manifest_reported;     /* follower: and that was logged */
    char broken_reported[96];   /* follower: the last broken part logged */
    uint64_t follow_flushed, follow_active;
    wal_tail tails[MAX_TAILS]; /* oldest first; the array is changed under db->lock + buf_lock */
    size_t ntails;
    uint64_t tail_lsn; /* rows added to tails (buf_lock): their lsn, so a cut (qcut) can bound them */

    /* fail-stop (eng_fail): set once, never cleared; fail_msg is written under
     * fail_lock before `failed` is stored, so whoever sees `failed` can read it */
    atomic_bool failed;
    pthread_mutex_t fail_lock; /* innermost; held only to fill fail_msg */
    pthread_mutex_t dir_sync_lock; /* eng_sync_dir; inside every lock but fail_lock */
    char fail_msg[256];
};

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
/* 0 while healthy; TSDB_ERR_IO with the message (directory, what failed, errno) once failed. */
int eng_failed_msg(struct eng *db, char *msg, size_t len);

codec_ctx *cc_get(struct eng *db);
void cc_put(struct eng *db, codec_ctx *c);

int64_t partition_of(const struct eng *db, int64_t ts);
/* Lists committed parts, taking over the caller's references, and rewrites
 * MANIFEST. Cannot fail: once listed, the parts are published and must not
 * be discarded; a MANIFEST failure fails the engine by itself (eng_fail). */
void db_add_parts(struct eng *db, part **ps, size_t n);
void db_wait_merges(struct eng *db, int64_t partition);
void db_parts_insert_locked(struct eng *db, part *p);
void db_parts_remove_locked(struct eng *db, part *p);
part **db_snapshot(struct eng *db, int64_t start, int64_t end, size_t *n);
void db_release(part **parts, size_t n);

/* merge.c */
void *merge_thread_main(void *arg);
int merge_parts(struct eng *db, part **src, size_t k, part **out);
void merge_commit_locked(struct eng *db, part **src, size_t k, part *out);

/* insert.c: sorts (stably), keeps the last row per (series, ts), writes one
 * part per partition. Parts are published, or handed back in `collect`. With `refuse` (rows from a caller, not
 * a flush of the WAL), a row at TS_UNREACHABLE fails the insert with TSDB_ERR_INVALID. */
int insert_rows(struct eng *db, int type, const uint32_t *series, uint32_t const_series, const int64_t *ts,
                const void *v, size_t n, uint64_t wal_seq, part_list *collect, bool refuse);
int part_list_push(part_list *l, part *p);
int parts_publish(struct eng *db, part **ps, size_t n);

/* the storage engine for one directory; the public API in tsdb.h wraps these */
int eng_open(const char *dir, const tsdb_options *opts, eng **out);
/* the same with the scheduler of the tsdb that owns the engine */
int eng_open_sched(const char *dir, const tsdb_options *opts, sched *s, eng **out);
int eng_close(eng *db);
int eng_series(eng *db, const char *name, int type, uint32_t *id);
int eng_series_lookup(eng *db, const char *name, uint32_t *id);
uint32_t eng_series_count(eng *db);
const char *eng_series_name(eng *db, uint32_t id);
int eng_series_type(eng *db, uint32_t id);
int eng_state(eng *db, const char *name, bool create, uint32_t *code);
const char *eng_state_name(eng *db, uint32_t code);
/* v: slots of `type`; every series must be of that type */
int eng_insert(eng *db, int type, const uint32_t *series, const int64_t *ts, const void *v, size_t n);
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
    qcut *c;
    size_t n, cap;
} qcuts;
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
    bool own;        /* ts/v are decode buffers owned by the cursor */
} src;
void src_init_part(src *c, const part *p, uint32_t gi, uint32_t gj, int type);
void src_init_mem(src *c, const mem_run *r, uint64_t prio, int type);
void src_free(src *c);
typedef int (*slot_fn)(void *ctx, const int64_t *ts, const void *v, size_t n);
/* Emits the union of the sources in time order, newest row per timestamp. */
int merge_stream(codec_ctx *cc, src *cur, size_t nc, int64_t start, int64_t end, slot_fn fn, void *ctx);
int eng_query_raw_ex(eng *db, uint32_t series, int64_t start, int64_t end, bool descending, size_t limit,
                     slot_fn fn, void *ctx);
/* LTTB downsampling to `threshold` points (>= 3) of the known, finite values; state series: TSDB_ERR_INVALID.
 * desc: the same points newest first; `keep` (0: all) is how many of them the caller takes at most, which bounds
 * what newest first holds in memory (fn still decides when to stop). */
int eng_query_lttb(eng *db, uint32_t series, int64_t start, int64_t end, size_t threshold, bool desc, size_t keep,
                   slot_fn fn, void *ctx);
extern size_t lttb_buffer_bytes; /* tests make it small to exercise the two-pass way */
extern size_t local_piece_bounds; /* local periods per window of tsdb_query_group_local (0: default); tests */
extern size_t agg_dense_buckets; /* the most buckets one dense array holds (0: the default); for tests */
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
int bucketer_bounds(bucketer *bk, const int64_t *bounds, size_t n);
int64_t bucketer_key(const bucketer *bk, int64_t ts);    /* the bucket of ts */
int64_t bucketer_start(const bucketer *bk, int64_t key); /* the first instant of bucket key */
/* Time-weighted averages (twa.c). */
int eng_query_twa(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk,
                  const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
int eng_optimize(eng *db);
int eng_drop_before(eng *db, int64_t ts);
int eng_get_stats(eng *db, tsdb_stats *st);
int eng_get_volume_stats(eng *db, tsdb_volume_stats *vs);

static inline int64_t eng_mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}
int eng_foreach_part(eng *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx);
int eng_refresh(eng *db);
/* retention.c */
int ret_open(eng *db, bool read_only);
int ret_reload(eng *db);
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
