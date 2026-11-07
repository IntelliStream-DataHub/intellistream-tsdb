/*
 * What the statistics files share (docs/design-stats.md): the sweeper's state (stats.c), the tenant machinery
 * (stats_tenant.c: the list, the map from series to tenant, the partition vectors, the HyperLogLogs) and the hot
 * lists and residency (stats_hot.c). Nothing outside src/stats*.c includes this.
 */
#ifndef TSDB_STATS_PRIV_H
#define TSDB_STATS_PRIV_H

#include "internal.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------ space-saving lists (stats_hot.c) */

/* A weighted space-saving summary of k = 256 items: every id whose true weight exceeds W / k is in it, and each
 * estimate is high by at most `err`. Entries sit in fixed slots; a min-heap of slots (by estimate) finds the one to
 * replace; a chained hash finds an id. ss_set keeps the k largest *values* instead, one per id (bytes per row). */
#define SS_K 256
typedef struct ss_ent {
    uint64_t id, est, err;
} ss_ent;
typedef struct ss {
    uint32_t n;
    ss_ent e[SS_K];
    uint8_t h[SS_K], hpos[SS_K];
    uint16_t bucket[512], nxt[SS_K];
} ss;
void ss_clear(ss *s);
void ss_add(ss *s, uint64_t id, uint64_t w);
void ss_set(ss *s, uint64_t id, uint64_t value);
const ss_ent *ss_find(const ss *s, uint64_t id);
uint64_t ss_min(const ss *s); /* the smallest estimate of a full list, else 0: what an id outside it may weigh */

/* Two windows of 60 s: the current and the one before. Updated by its owner, read by anyone, under its own mutex
 * (a leaf). `epoch` is the minute the current window belongs to. */
#define HOT_PERIOD_MS 60000
typedef struct hotl {
    pthread_mutex_t mu;
    int64_t epoch;
    int cur;
    ss w[2];
} hotl;
hotl *hotl_new(void);
void hotl_free(hotl *h);
void hotl_add(hotl *h, int64_t epoch, uint64_t id, uint64_t w);
int64_t hot_now_ms(void); /* wall clock in ms (tests may move it: stats_hot_clock_ms) */

/* A merge of several lists (the mergeable-summaries rule): an id absent from a full list weighs at most that
 * list's minimum, so it is added to the estimate and to its error. */
typedef struct hagg hagg;
hagg *hagg_new(void);
void hagg_free(hagg *a);
void hagg_add_ss(hagg *a, const ss *s, uint64_t sid_base, bool with_min); /* a list of local ids of one volume */
void hagg_add_hotl(hagg *a, hotl *h, uint64_t sid_base, int64_t now_epoch); /* its valid windows */
/* the top k by estimate, descending; *threshold: the sum of the lists' minima (an id not listed weighs at most that) */
int hagg_top(hagg *a, tsdb_hot *out, size_t k, size_t *n, uint64_t *threshold);
uint64_t hagg_estimate(hagg *a, uint64_t sid, uint64_t *err);

/* Per-thread read lists: one hotl per counter block, allocated at the thread's first sample. */
void qreg_hot_note(qreg *r, uint64_t sid);
void qreg_hot_collect(qreg *r, hagg *a);

/* ---------------------------------------------------------------------------- tenants (stats_tenant.c) */

typedef struct tlist {
    atomic_int refs;
    uint32_t n;         /* prefixes, sorted bytewise */
    char **s;
    uint32_t *len;
    uint32_t *no;       /* the tenant number of each (process-wide, never reused: tenant_intern) */
    int32_t *parent;    /* the nearest earlier entry that is a proper prefix of this one, or -1 */
    uint32_t maxno;     /* the largest number in the list */
    uint64_t hash;      /* of the sorted prefixes: two servers with the same list have the same hash */
} tlist;
/* Builds the list of prefixes (duplicates and empty strings are dropped; TSDB_ERR_INVALID: more than 65,535
 * different prefixes in this process's life). NULL out with n = 0 gives an empty list. */
int tlist_make(const char *const *prefixes, size_t n, tlist **out);
tlist *tlist_ref(tlist *t);
void tlist_unref(tlist *t);
uint32_t tlist_match(const tlist *t, const char *name); /* the tenant number of a name, 0 when none */
int tlist_find(const tlist *t, const char *prefix, uint32_t *no); /* TSDB_ERR_NOTFOUND */
#define TENANT_MAX 65535

typedef struct tent {
    uint64_t rows, nulls, logical, ts, val, index, granules;
} tent;
typedef struct tvec_e { /* one tenant's share of a partition: 64 bytes */
    uint32_t t, pad;
    tent v;
} tvec_e;
typedef struct tpage {
    uint16_t *v; /* NULL: every id of the page that the map has assigned belongs to `one` */
    uint16_t one;
} tpage;

typedef struct pvec { /* what one partition holds, per tenant, and whether it must be counted again */
    struct pvec *next, *qnext;
    int64_t key;
    tvec_e *v;
    uint32_t nv, cap;
    uint32_t nrec; /* part records in this partition */
    uint32_t tgen; /* the tenant list generation the vector is expressed in */
    bool stale, inq;
    int64_t stale_ns, last_ns;
} pvec;

typedef struct ttot { /* what a tenant has: always the sum of the partition vectors (and the counters since start) */
    uint64_t rows, nulls, logical, ts, val, index, granules;
    uint64_t series, created;
    uint64_t ing_rows, ing_bytes, merged_rows, removed_rows, retdrop_rows;
    uint64_t resident;
    bool partial; /* a move to or from it is not done */
} ttot;

#define HLL_M 1024
typedef struct thll {
    int64_t win[2]; /* the hour each register set counts */
    uint8_t *reg[2];
} thll;

/* ------------------------------------------------------------------------------------- the sweeper */

typedef struct stat_ev {
    part *p;
    uint8_t kind;
    bool fresh;   /* listed after the open finished: the writer's own flush or merge (ingest counts it) */
    bool dropped; /* REMOVE: retention or drop_before retired it */
} stat_ev;

/* What the sweeper counted of one part. `key` is the part's address; the rest is its identity, which makes a record
 * of a freed part whose address was reused another part's no match. */
typedef struct prec {
    struct prec *next;
    const part *key;
    int64_t partition;
    uint64_t min_block, max_block, rows;
    uint32_t level, gen;
    uint64_t logical, ts_bytes, val_bytes, index_bytes, other_bytes;
    bool mcounted;   /* its rows are in merged_rows already, or never belong there */
    uint16_t nres;   /* residency: what the last sample found, per tenant (stats_hot.c) */
    uint16_t *rt;
    uint64_t *rb;
    int64_t res_ns;
} prec;

typedef struct vtot {
    uint64_t logical, ts_bytes, val_bytes, index_bytes, other_bytes;
} vtot;

struct move { /* a series whose tenant a list change changed */
    uint32_t id;
    uint16_t from, to;
};

#define RING_N 4096
struct estats {
    eng *db;
    stat_ev ring[RING_N];
    atomic_uint_fast64_t head, tail;
    atomic_bool overflow, swapped, stop;
    atomic_bool opened; /* the open has finished: only then may the sweeper read db->parts (load_parts changes it unlocked) */
    atomic_uint_fast64_t overflows, cpu_ns;
    atomic_uint_fast64_t settle_req, settle_done;
    atomic_bool ingest_approx; /* the ring overflowed: ingest is approximate */
    pthread_t th;
    bool th_started;
    pthread_mutex_t mu; /* a leaf: records, totals, the pending list, tenant totals; also what cv waits on */
    pthread_cond_t cv, done_cv;
    prec **ht;
    size_t htn, nrec;
    uint32_t gen;
    vtot tot;
    part **pend; /* parts put off (their series are not in the dictionary yet), each with a reference */
    size_t npend, pend_cap;
    int64_t ready_ns; /* the throttle: the next walk may start then */
    int64_t mb_s;
    int64_t recompute_ns; /* a partition is counted again at most this often */

    /* tenants (stats_tenant.c). The sweeper alone changes tl, tp, pht and the scratch; the tenant totals (tt) and the
     * HyperLogLogs change under mu so that readers see whole parts. */
    tlist *tl;
    uint32_t tl_gen; /* bumped at each list the sweeper applies */
    tpage *tp;
    size_t ntp;
    uint32_t tfront; /* ids below it have a tenant in the map */
    uint32_t tbase;  /* the number of series when the sweeper started: later ones count as created */
    ttot *tt;
    size_t ttn;
    thll *hll;
    pvec **pht;
    size_t phtn, npv;
    pvec *qhead, *qtail;
    uint64_t moving; /* partitions whose vector is not yet in terms of tl_gen */
    int64_t oldest_stale_ns; /* mu */
    tent *sc, *sc2;  /* scratch, indexed by tenant number */
    uint32_t *touch, *touch2;
    size_t scn, ntouch, ntouch2;
    atomic_uint force; /* a settle is waiting: recompute everything stale now */
    hotl *hw;          /* the hottest series by rows written */
    ss bpr;            /* the worst series by bytes per row (mu) */
    size_t res_cursor;
    int64_t res_period_ns;
    uint64_t *runcnt;  /* the walk's rows per run, for the writes list and the HLL */
    size_t runcnt_cap;
    atomic_uint_fast64_t tmap_bytes;
    struct move *moves; /* series that changed tenant, sorted by (from, to, id), until every partition has moved */
    size_t nmoves;
    size_t mv_cursor;
};

int64_t stats_mono_ns(void);
bool stats_stopping(struct estats *es);

/* stats.c: the records and the walk, for the tenant and hot code */
enum { WALK_SPLIT = 1, WALK_BPR = 2, WALK_RUNCNT = 4 };
int part_walk(struct estats *es, const part *p, prec *r, unsigned flags);
bool rec_matches(const prec *r, const part *p);
prec *rec_find(struct estats *es, const part *p);
void rec_insert(struct estats *es, prec *r);
prec *rec_remove(struct estats *es, const part *p);
void tot_apply(struct estats *es, const prec *r, bool add);
void rec_discard(struct estats *es, prec *r, bool dropped);
void throttle_wait(struct estats *es);
void throttle_pay(struct estats *es, uint64_t bytes);
void process_events(struct estats *es, stat_ev *batch, size_t n);
/* The listed parts of one partition, with a reference each, taken in one db->lock hold together with the events
 * queued at that moment; the events are handled before it returns, so the records are exactly those of the list.
 * false: out of memory. */
bool partition_cut(struct estats *es, int64_t key, part ***ps, size_t *np);

/* stats_tenant.c */
void tenants_init(struct estats *es);
void tenants_free(struct estats *es);
void tenants_extend(struct estats *es, uint32_t nseries); /* assigns tenants to the ids below nseries */
void tenant_scratch_clear(struct estats *es);
void tenant_walk_run(struct estats *es, uint32_t id, uint64_t cnt, uint64_t nulls, uint64_t logical, uint64_t ts,
                     uint64_t val, uint64_t index, uint64_t granules);
void tenants_apply_list(struct estats *es);        /* the sweeper: a new list in db->tl */
bool tenants_step(struct estats *es, bool forced); /* moves and recounts, within the throttle; true: more to do now */
void pv_part_added(struct estats *es, const prec *r, bool fresh); /* the split is in the scratch */
void pv_part_removed(struct estats *es, const prec *r, bool dropped);
void tenants_ingest(struct estats *es, part *p, const prec *r); /* a flushed part: ingest, HLL, hot writes */
uint32_t tenant_of(struct estats *es, uint32_t id);
void tenants_fill(struct estats *es, uint32_t no, uint64_t v[17], uint32_t *flags, uint64_t *stale_s);
uint64_t tenants_map_bytes(struct estats *es);
void tenant_res_add(struct estats *es, uint32_t t, uint64_t bytes, bool add); /* es->mu held */
void tenants_grow_to(struct estats *es, uint32_t maxno);                      /* es->mu not held */

/* stats_hot.c */
void hot_bpr_run(struct estats *es, uint32_t id, uint64_t rows, uint64_t bytes, bool full);
size_t resident_bytes_range(const uint8_t *p, size_t len); /* bytes of [p, p+len) in the page cache */
void residency_step(struct estats *es);
void residency_forget(struct estats *es, prec *r); /* takes its sample out of the tenant totals */
void rec_free_res(prec *r);
#endif
