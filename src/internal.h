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

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define FORMAT_VERSION 1
#define MAX_GRANULE_ROWS 65536
#define MAX_MERGE_PARTS 100
#define PARTS_TO_DELAY_INSERT 300 /* per partition, like parts_to_delay_insert */
#define IDLE_PARTITION_FORCE_MERGE_S 3600

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
#define BLK_ALPRD 9     /* float32 split into dictionary-coded high bits and packed low bits */
#define BLK_DECIMAL_BP 10 /* fixed-point integers, bit-packed zigzag deltas */
#define BLK_ZSTD 0x10 /* the transform output is additionally ZSTD compressed */
#define BLK_SLACK 32  /* decoders may read up to this many bytes past a block */

typedef struct codec_ctx {
    struct codec_ctx *next; /* free list in eng */
    ZSTD_CCtx *cctx;
    ZSTD_DCtx *dctx;
    uint8_t *scratch;
    size_t scratch_cap;
    uint8_t *alt; /* candidate block while choosing a value encoding */
    size_t alt_cap;
} codec_ctx;

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
size_t decimal_bound(size_t n);
size_t decimal_encode(const float *in, size_t n, uint8_t *out); /* 0 if not applicable */
int decimal_decode(const uint8_t *in, size_t len, size_t n, float *out);

codec_ctx *codec_new(void);
void codec_free(codec_ctx *c);
size_t block_bound(size_t n);
size_t block_encode_ts(codec_ctx *c, int level, const int64_t *ts, size_t n, uint8_t *out);
/* Both return 0 when out of memory. */
size_t block_encode_f32(codec_ctx *c, int level, int value_codec, const float *v, size_t n, uint8_t *out);
int block_decode_ts(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, int64_t *out);
int block_decode_f32(codec_ctx *c, const uint8_t *blk, size_t len, size_t n, float *out);

/* ---- SIMD kernels (simd.c), dispatched once at startup ---- */
typedef struct agg_state {
    uint64_t count;
    double sum;
    float min, max;
} agg_state;

static inline void agg_init(agg_state *a)
{
    a->count = 0;
    a->sum = 0;
    a->min = __builtin_inff();
    a->max = -__builtin_inff();
}

static inline void agg_merge(agg_state *a, uint64_t count, double sum, float mn, float mx)
{
    a->count += count;
    a->sum += sum;
    if (mn < a->min) a->min = mn;
    if (mx > a->max) a->max = mx;
}

/* Folds v[0..n) into a: count, sum (in double), min and max (NaN ignored). */
extern void (*simd_agg_f32)(const float *v, size_t n, agg_state *a);
const char *simd_name(void);
void simd_init(void);

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

/* ---- time zones (tz.c) and buckets (query.c) ---- */
typedef struct tz tz;
int tz_get(const char *name, const tz **out);
int32_t tz_offset(const tz *z, int64_t utc_seconds);  /* seconds east of UTC */
int64_t tz_utc(const tz *z, int64_t local_seconds);   /* first UTC second of a local time */
int32_t tz_fixed_offset(const tz *z, bool *fixed);
int64_t tz_next_transition(const tz *z, int64_t utc_seconds);
void tz_civil(int64_t days, int64_t *y, int *m, int *d);
int64_t tz_days_from_civil(int64_t y, int m, int d);

/* Maps timestamps to dense bucket keys and keys to bucket start instants. */
typedef struct bucketer {
    int kind;         /* BK_ONE, BK_FIXED or BK_CALENDAR */
    int64_t start;    /* BK_ONE: the start reported for the one bucket */
    int64_t interval; /* BK_FIXED: length in ms */
    int64_t shift;    /* BK_FIXED: ms added before dividing (a zone offset) */
    int unit;         /* BK_CALENDAR: TSDB_DAY .. TSDB_YEAR */
    int64_t count;
    const tz *zone;
} bucketer;
enum { BK_ONE, BK_FIXED, BK_CALENDAR };

/* ---- parts ---- */

/* One entry of a part's sparse index; stored on disk exactly like this (little endian). */
typedef struct granule {
    uint32_t series;
    uint32_t count;
    int64_t ts_min;
    int64_t ts_max;
    uint64_t off;    /* file offset of the timestamp block; the value block follows it */
    uint32_t ts_len;
    uint32_t val_len;
    double sum;
    float vmin;
    float vmax;
    uint32_t crc;    /* low 32 bits of xxh64 over both blocks */
    uint32_t reserved;
} granule;
_Static_assert(sizeof(granule) == 64, "granule must be 64 bytes");

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
    uint64_t index_hash;
    char magic[8];
} part_footer;
_Static_assert(sizeof(part_footer) == 56, "footer must be 56 bytes");
_Static_assert(sizeof(part_footer) >= BLK_SLACK, "footer must cover decoder overread");

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
    uint64_t index_off;
    uint64_t rows;
    int64_t min_ts, max_ts;
    time_t created;
    atomic_int refs;
    /* protected by tsdb.lock */
    bool merging;
    bool dropped;
    atomic_bool obsolete; /* delete the file when the last reference goes */
    char pending[64];     /* tmp file name while the part awaits parts_commit, else "" */
} part;

int part_open(int dirfd, const char *name, part **out);
int part_open_as(int dirfd, const char *file, const char *name, part **out);
void part_ref(part *p);
void part_unref(part *p);
int granule_read(codec_ctx *c, const part *p, const granule *g, int64_t *ts, float *v);

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
    size_t pend_n;
    int64_t *pend_ts;
    float *pend_v;
    uint32_t grows;
    int zstd_level;
    int value_codec;
    codec_ctx *cc;
    uint64_t rows;
    int64_t min_ts, max_ts;
} part_writer;

int pw_begin(part_writer *w, struct eng *db, int64_t partition, uint64_t wal_seq);
int pw_add(part_writer *w, uint32_t series, const int64_t *ts, const float *v, size_t n);
int pw_copy_granule(part_writer *w, const part *src, const granule *g);
int pw_finish(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out);
/* As pw_finish, but the part stays under its tmp name until parts_commit. */
int pw_finish_pending(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out);
/* Numbers pending parts and gives them their names, all or none even across a
 * crash. Caller holds publish_lock and publishes them before releasing it. */
int parts_commit(struct eng *db, part **ps, size_t n);
void parts_discard(struct eng *db, part **ps, size_t n);
int intents_recover(struct eng *db);
void pw_abort(part_writer *w);

/* ---- series dictionary ---- */
typedef struct series_dict {
    pthread_mutex_t lock;
    int fd;
    bool fsync;
    char **names;
    uint32_t cap;
    atomic_uint count;
    uint32_t *table; /* open addressing; id + 1, 0 = empty */
    uint32_t tcap;
    uint64_t file_off; /* bytes of SERIES already loaded */
} series_dict;

int series_open(series_dict *d, int dirfd, bool read_only, bool fsync);
int series_reload(series_dict *d, int dirfd); /* readers: names appended by the writer since */
void series_close(series_dict *d);
int series_get(series_dict *d, const char *name, bool create, uint32_t *id);
const char *series_name(series_dict *d, uint32_t id);

/* ---- thread pool ---- */
typedef struct pool pool;
pool *pool_new(int nthreads);
void pool_free(pool *p);
/* Runs fn(arg, i) for i in [0, n) where n <= the pool size + 1, the caller
 * acting as i == 0. Returns n actually used: it falls back to 1 when the pool
 * is busy with another query. */
int pool_run(pool *p, int n, void (*fn)(void *arg, int i), void *arg);

/* ---- memtable + WAL (memtable.c) ---- */
typedef struct mt_series {
    int64_t *ts;
    float *v;
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

/* Rows of one series copied out of a memtable: sorted, unique by ts. */
typedef struct mem_run {
    int64_t *ts;
    float *v;
    size_t n;
} mem_run;

typedef struct part_list {
    part **p;
    size_t n, cap;
} part_list;

memtable *mt_new(uint64_t wal_seq);
void mt_ref(memtable *m);
void mt_unref(memtable *m);
/* lsn0: WAL sequence of the first row (the rest follow), 0 for rows that are durable already */
int mt_add(memtable *m, const uint32_t *series, uint32_t cs, const int64_t *ts, const float *v, size_t n, uint64_t lsn0);
/* only rows with a WAL sequence <= visible */
int mt_collect(const memtable *m, uint32_t id, int64_t start, int64_t end, uint64_t visible, mem_run *out);
void mem_run_free(mem_run *r);
int mt_flush(struct eng *db);
int mem_insert(struct eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const float *v, size_t n);
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
    pool *qpool;

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

    /* writer: MANIFEST for followers, delayed deletion of replaced parts */
    pthread_mutex_t manifest_lock;
    uint64_t manifest_version;
    uint64_t wal_flushed; /* segments up to this are entirely in parts (db->lock) */
    graveyard_entry *graveyard;
    size_t ngraveyard, graveyard_cap;

    /* follower */
    bool follower_stop;
    bool has_follower;
    pthread_t follower;
    pthread_mutex_t refresh_lock;
    uint64_t follow_version;
    uint64_t follow_flushed, follow_active;
    wal_tail tails[MAX_TAILS]; /* oldest first; the array is changed under db->lock + buf_lock */
    size_t ntails;
};

codec_ctx *cc_get(struct eng *db);
void cc_put(struct eng *db, codec_ctx *c);

int64_t partition_of(const struct eng *db, int64_t ts);
int db_add_part(struct eng *db, part *p);
int db_add_parts(struct eng *db, part **ps, size_t n);
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
 * part per partition. Parts are published, or handed back in `collect`. */
int insert_rows(struct eng *db, const uint32_t *series, uint32_t const_series, const int64_t *ts,
                const float *v, size_t n, uint64_t wal_seq, part_list *collect);
int part_list_push(part_list *l, part *p);
int parts_publish(struct eng *db, part **ps, size_t n);

/* the storage engine for one directory; the public API in tsdb.h wraps these */
int eng_open(const char *dir, const tsdb_options *opts, eng **out);
int eng_close(eng *db);
int eng_series(eng *db, const char *name, uint32_t *id);
int eng_series_lookup(eng *db, const char *name, uint32_t *id);
uint32_t eng_series_count(eng *db);
const char *eng_series_name(eng *db, uint32_t id);
int eng_insert(eng *db, const uint32_t *series, const int64_t *ts, const float *values, size_t n);
int eng_insert_series(eng *db, uint32_t series, const int64_t *ts, const float *values, size_t n);
int eng_append(eng *db, uint32_t series, int64_t ts, float value);
int eng_commit(eng *db);
int eng_flush(eng *db);
int eng_query_raw(eng *db, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx);
int eng_query_points(eng *db, uint32_t series, int64_t start, int64_t end, tsdb_points *out);
extern size_t desc_window_rows;
int eng_query_raw_ex(eng *db, uint32_t series, int64_t start, int64_t end, bool descending, size_t limit,
                     tsdb_raw_fn fn, void *ctx);
int eng_query_agg(eng *db, uint32_t series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                  size_t *nout);
int eng_query_group(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, tsdb_bucket **out,
                    size_t *nout);
int bucketer_init(bucketer *bk, int64_t start, const tsdb_group *g);
int eng_optimize(eng *db);
int eng_drop_before(eng *db, int64_t ts);
int eng_get_stats(eng *db, tsdb_stats *st);
int eng_foreach_part(eng *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx);
int eng_refresh(eng *db);

/* the storage engine for one directory; the public API in tsdb.h wraps these */
int eng_open(const char *dir, const tsdb_options *opts, eng **out);
int eng_close(eng *db);
int eng_series(eng *db, const char *name, uint32_t *id);
int eng_series_lookup(eng *db, const char *name, uint32_t *id);
uint32_t eng_series_count(eng *db);
const char *eng_series_name(eng *db, uint32_t id);
int eng_insert(eng *db, const uint32_t *series, const int64_t *ts, const float *values, size_t n);
int eng_insert_series(eng *db, uint32_t series, const int64_t *ts, const float *values, size_t n);
int eng_append(eng *db, uint32_t series, int64_t ts, float value);
int eng_commit(eng *db);
int eng_flush(eng *db);
int eng_query_raw(eng *db, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx);
int eng_query_points(eng *db, uint32_t series, int64_t start, int64_t end, tsdb_points *out);
extern size_t desc_window_rows;
int eng_query_raw_ex(eng *db, uint32_t series, int64_t start, int64_t end, bool descending, size_t limit,
                     tsdb_raw_fn fn, void *ctx);
int eng_query_agg(eng *db, uint32_t series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                  size_t *nout);
int eng_query_group(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, tsdb_bucket **out,
                    size_t *nout);
int bucketer_init(bucketer *bk, int64_t start, const tsdb_group *g);
int eng_optimize(eng *db);
int eng_drop_before(eng *db, int64_t ts);
int eng_get_stats(eng *db, tsdb_stats *st);
int eng_foreach_part(eng *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx);
int eng_refresh(eng *db);

#endif
