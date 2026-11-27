/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster reporting registries (R14, r14-report): heartbeat section registry and cluster metrics registry.
 *
 * Compiled twice:
 *   - default (creport.o, in CLUSTER_OBJ): pure registry with no engine/repld dependencies.
 *   - -DCREPORT_DRV_ONLY (creport_drv.o, in tsdbd binaries): tiny repld lag bridge (creport_repld_lag).
 */
#define _GNU_SOURCE
#include "creport.h"

#ifdef CREPORT_DRV_ONLY

#include "repld_int.h"
#include <time.h>

static uint64_t drv_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

void creport_repld_lag(uint32_t vol, uint64_t *lag_positions, uint64_t *lag_ms)
{
    if (lag_positions)
        *lag_positions = 0;
    if (lag_ms)
        *lag_ms = 0;
    struct rvol *rv = vol_find(vol);
    if (!rv)
        return;
    uint64_t pos = 0, ms = 0;
    pthread_mutex_lock(&rv->mu);
    if (rv->core) {
        uint64_t now = drv_now_ms();
        uint64_t written = repl_written(rv->core);
        uint64_t commit = repl_commit(rv->core);
        uint64_t applied = repl_applied(rv->core);
        if (repl_is_leader(rv->core)) {
            pos = written > commit ? written - commit : 0;
            if (pos > 0) {
                if (rv->cstall_ms > 0 && now > rv->cstall_ms)
                    ms = now - rv->cstall_ms;
                for (int k = 0; k < rv->nnodes && k < REPL_MAX_NODES; k++) {
                    if (k == rv->self)
                        continue;
                    if (rv->peer[k].ahead_ms > 0 && now > rv->peer[k].ahead_ms) {
                        uint64_t d = now - rv->peer[k].ahead_ms;
                        if (d > ms)
                            ms = d;
                    }
                }
            }
        } else {
            uint64_t top = commit > written ? commit : written;
            pos = top > applied ? top - applied : 0;
            if (pos > 0 && rv->fetch_ms > 0 && now > rv->fetch_ms)
                ms = now - rv->fetch_ms;
        }
    }
    pthread_mutex_unlock(&rv->mu);
    vol_put(rv);
    if (lag_positions)
        *lag_positions = pos;
    if (lag_ms)
        *lag_ms = ms;
}

#else /* !CREPORT_DRV_ONLY */

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ mutants & test hooks */

#ifdef TSDB_TEST_HOOKS
enum creport_mut {
    CREPORT_MUT_NONE = 0,
    CREPORT_MUT_METRICSTALE = 1,    /* snapshot frozen after initial capture with a held volume */
    CREPORT_MUT_SECTIONSDROP = 2,   /* registered heartbeat sections not encoded/sent */
    CREPORT_MUT_PROGRESSFROZEN = 3, /* leader keeps first active progress report per member */
    CREPORT_MUT_PROGRESSNOTOTAL = 4,/* progress bytes_total omitted (reported as 0) */
};

static atomic_int g_creport_mut_cached = -1;

static int creport_mutant(void)
{
    int v = atomic_load_explicit(&g_creport_mut_cached, memory_order_relaxed);
    if (v >= 0)
        return v;
    const char *e = getenv("TSDB_PROGRESS_MUTANT");
    if (!e || !*e)
        e = getenv("TSDB_REPORT_MUTANT");
    if (!e || !*e)
        e = getenv("TSDB_CLUSTER_MUTANT");
    v = CREPORT_MUT_NONE;
    if (e && strcmp(e, "metricstale") == 0)
        v = CREPORT_MUT_METRICSTALE;
    else if (e && strcmp(e, "sectionsdrop") == 0)
        v = CREPORT_MUT_SECTIONSDROP;
    else if (e && strcmp(e, "progressfrozen") == 0)
        v = CREPORT_MUT_PROGRESSFROZEN;
    else if (e && strcmp(e, "progressnototal") == 0)
        v = CREPORT_MUT_PROGRESSNOTOTAL;
    atomic_store_explicit(&g_creport_mut_cached, v, memory_order_relaxed);
    return v;
}
#endif

/* ------------------------------------------------------------------ heartbeat section registry */

struct sec_entry {
    uint8_t used;
    uint8_t type;
    creport_sec_encode_fn encode_fn;
    creport_sec_decode_fn decode_fn;
    creport_sec_free_fn free_fn;
    creport_sec_format_fn format_fn;
};

static pthread_mutex_t reg_mu = PTHREAD_MUTEX_INITIALIZER;
static struct sec_entry secs[CREPORT_SEC_MAX];
static uint32_t nsecs;
static atomic_uint_fast64_t g_sec_decode_errors;

uint64_t creport_sec_decode_errors(void) { return atomic_load_explicit(&g_sec_decode_errors, memory_order_relaxed); }

int creport_section(uint8_t type, creport_sec_encode_fn encode_fn, creport_sec_decode_fn decode_fn,
                    creport_sec_free_fn free_fn, creport_sec_format_fn format_fn)
{
    if (type <= CREPORT_SEC_BUILTIN_MAX || !encode_fn || !decode_fn || !free_fn)
        return -1;
    pthread_mutex_lock(&reg_mu);
    for (uint32_t i = 0; i < nsecs; i++) {
        if (secs[i].used && secs[i].type == type) {
            pthread_mutex_unlock(&reg_mu);
            return -1;
        }
    }
    int slot = -1;
    for (uint32_t i = 0; i < nsecs; i++) {
        if (!secs[i].used) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        if (nsecs >= CREPORT_SEC_MAX) {
            pthread_mutex_unlock(&reg_mu);
            return -1;
        }
        slot = (int)nsecs++;
    }
    secs[slot] = (struct sec_entry){
        .used = 1,
        .type = type,
        .encode_fn = encode_fn,
        .decode_fn = decode_fn,
        .free_fn = free_fn,
        .format_fn = format_fn,
    };
    pthread_mutex_unlock(&reg_mu);
    return 0;
}

int creport_section_unregister(uint8_t type)
{
    pthread_mutex_lock(&reg_mu);
    int found = -1;
    for (uint32_t i = 0; i < nsecs; i++) {
        if (secs[i].used && secs[i].type == type) {
            secs[i] = (struct sec_entry){0};
            found = 0;
            break;
        }
    }
    while (nsecs > 0 && !secs[nsecs - 1].used)
        nsecs--;
    pthread_mutex_unlock(&reg_mu);
    return found;
}

void creport_heard_init(struct creport_heard_secs *hs)
{
    if (hs)
        memset(hs, 0, sizeof *hs);
}

void creport_heard_clear(struct creport_heard_secs *hs)
{
    if (!hs)
        return;
    for (uint32_t i = 0; i < hs->n; i++) {
        if (hs->items[i].val && hs->items[i].free_fn)
            hs->items[i].free_fn(hs->items[i].val);
        hs->items[i] = (struct creport_sec_val){0};
    }
    hs->n = 0;
}

const void *creport_heard_get(const struct creport_heard_secs *hs, uint8_t type)
{
    if (!hs)
        return NULL;
    for (uint32_t i = 0; i < hs->n; i++)
        if (hs->items[i].type == type)
            return hs->items[i].val;
    return NULL;
}

size_t creport_heard_format(const struct creport_heard_secs *hs, uint32_t member_id, char *buf, size_t cap)
{
    if (!buf || cap == 0)
        return 0;
    buf[0] = '\0';
    if (!hs || hs->n == 0)
        return 0;

    /* Copy format function pointers under reg_mu, then format without holding reg_mu. */
    struct {
        uint8_t type;
        const void *val;
        creport_sec_format_fn fmt;
    } todo[CREPORT_SEC_MAX];
    uint32_t ntodo = 0;

    pthread_mutex_lock(&reg_mu);
    for (uint32_t i = 0; i < hs->n && ntodo < CREPORT_SEC_MAX; i++) {
        for (uint32_t j = 0; j < nsecs; j++) {
            if (secs[j].used && secs[j].type == hs->items[i].type && secs[j].format_fn) {
                todo[ntodo].type = hs->items[i].type;
                todo[ntodo].val = hs->items[i].val;
                todo[ntodo].fmt = secs[j].format_fn;
                ntodo++;
                break;
            }
        }
    }
    pthread_mutex_unlock(&reg_mu);

    size_t off = 0;
    for (uint32_t i = 0; i < ntodo && off + 1 < cap; i++) {
        size_t w = todo[i].fmt(member_id, todo[i].val, buf + off, cap - off);
        if (w >= cap - off)
            w = cap - off - 1;
        off += w;
        buf[off] = '\0';
    }
    return off;
}

void creport_sec_encode_all(creport_sec_emit_fn emit, void *ctx)
{
    if (!emit)
        return;
#ifdef TSDB_TEST_HOOKS
    if (creport_mutant() == CREPORT_MUT_SECTIONSDROP)
        return;
#endif
    struct sec_entry snap[CREPORT_SEC_MAX];
    uint32_t ns = 0;
    pthread_mutex_lock(&reg_mu);
    for (uint32_t i = 0; i < nsecs; i++)
        if (secs[i].used && secs[i].encode_fn)
            snap[ns++] = secs[i];
    pthread_mutex_unlock(&reg_mu);

    if (ns == 0)
        return;
    uint8_t *tmp = malloc(CREPORT_SEC_PAYLOAD_MAX);
    if (!tmp)
        return;
    for (uint32_t i = 0; i < ns; i++) {
        ssize_t n = snap[i].encode_fn(tmp, CREPORT_SEC_PAYLOAD_MAX);
        if (n > 0 && (size_t)n <= CREPORT_SEC_PAYLOAD_MAX)
            emit(ctx, snap[i].type, tmp, (size_t)n);
    }
    free(tmp);
}

int creport_sec_decode_into(struct creport_heard_secs *hs, uint16_t type, uint32_t member_id, const uint8_t *data,
                            size_t len)
{
    if (type <= CREPORT_SEC_BUILTIN_MAX || type > 255)
        return 1; /* unknown / out of registry range: skip cleanly */
    struct sec_entry ent = {0};
    pthread_mutex_lock(&reg_mu);
    for (uint32_t i = 0; i < nsecs; i++) {
        if (secs[i].used && secs[i].type == (uint8_t)type) {
            ent = secs[i];
            break;
        }
    }
    pthread_mutex_unlock(&reg_mu);
    if (!ent.used || !ent.decode_fn)
        return 1; /* unknown section type: skip cleanly */

    void *val = NULL;
    if (ent.decode_fn(member_id, data, len, &val) != 0) {
        atomic_fetch_add_explicit(&g_sec_decode_errors, 1, memory_order_relaxed);
        return -1;
    }

    if (!hs) {
        if (val && ent.free_fn)
            ent.free_fn(val);
        return 0;
    }
    for (uint32_t i = 0; i < hs->n; i++) {
        if (hs->items[i].type == (uint8_t)type) {
            if (hs->items[i].val && hs->items[i].free_fn)
                hs->items[i].free_fn(hs->items[i].val);
            hs->items[i].val = val;
            hs->items[i].free_fn = ent.free_fn;
            return 0;
        }
    }
    if (hs->n >= CREPORT_SEC_MAX) {
        if (val && ent.free_fn)
            ent.free_fn(val);
        atomic_fetch_add_explicit(&g_sec_decode_errors, 1, memory_order_relaxed);
        return -1;
    }
    hs->items[hs->n++] = (struct creport_sec_val){
        .type = (uint8_t)type,
        .val = val,
        .free_fn = ent.free_fn,
    };
    return 0;
}

/* ------------------------------------------------------------------ metrics registry */

struct creport_metric {
    char name[96];
    char help[320];
    enum creport_metric_type type;
    struct creport_snap_label labels[CREPORT_LABELS_MAX];
    uint8_t nlabels;
    _Atomic uint64_t raw; /* double bits for gauge, uint64_t for counter */
    struct creport_metric *next;
};

struct creport_collector {
    char name[96];
    char help[320];
    enum creport_metric_type type;
    creport_collect_fn fn;
    void *ctx;
    struct creport_collector *next;
};

static struct creport_metric *g_metrics;
static struct creport_metric *g_metrics_tail;
static struct creport_collector *g_collectors;
static struct creport_collector *g_collectors_tail;

static uint64_t dbl_to_u64(double d)
{
    uint64_t u;
    memcpy(&u, &d, sizeof u);
    return u;
}

static double u64_to_dbl(uint64_t u)
{
    double d;
    memcpy(&d, &u, sizeof d);
    return d;
}

static struct creport_metric *metric_add_impl(const char *name, enum creport_metric_type type, const char *help,
                                              const struct creport_label *labels, size_t nlabels)
{
    if (!name || !*name || nlabels > CREPORT_LABELS_MAX)
        return NULL;
    struct creport_metric *m = calloc(1, sizeof *m);
    if (!m)
        return NULL;
    snprintf(m->name, sizeof m->name, "%s", name);
    snprintf(m->help, sizeof m->help, "%s", help ? help : "");
    m->type = type;
    m->nlabels = (uint8_t)nlabels;
    for (size_t i = 0; i < nlabels; i++) {
        snprintf(m->labels[i].key, sizeof m->labels[i].key, "%s", labels[i].key ? labels[i].key : "");
        snprintf(m->labels[i].val, sizeof m->labels[i].val, "%s", labels[i].val ? labels[i].val : "");
    }
    atomic_init(&m->raw, type == CREPORT_GAUGE ? dbl_to_u64(0.0) : 0);
    pthread_mutex_lock(&reg_mu);
    if (g_metrics_tail)
        g_metrics_tail->next = m;
    else
        g_metrics = m;
    g_metrics_tail = m;
    pthread_mutex_unlock(&reg_mu);
    return m;
}

struct creport_metric *creport_metric_gauge(const char *name, const char *help, const struct creport_label *labels,
                                            size_t nlabels)
{
    return metric_add_impl(name, CREPORT_GAUGE, help, labels, nlabels);
}

struct creport_metric *creport_metric_counter(const char *name, const char *help, const struct creport_label *labels,
                                              size_t nlabels)
{
    return metric_add_impl(name, CREPORT_COUNTER, help, labels, nlabels);
}

void creport_metric_set(struct creport_metric *m, double val)
{
    if (!m)
        return;
    atomic_store_explicit(&m->raw, dbl_to_u64(val), memory_order_relaxed);
}

void creport_metric_add(struct creport_metric *m, uint64_t delta)
{
    if (!m)
        return;
    atomic_fetch_add_explicit(&m->raw, delta, memory_order_relaxed);
}

void creport_metric_inc(struct creport_metric *m) { creport_metric_add(m, 1); }

void creport_metric_del(struct creport_metric *m)
{
    if (!m)
        return;
    pthread_mutex_lock(&reg_mu);
    struct creport_metric *prev = NULL, *cur = g_metrics;
    while (cur) {
        if (cur == m) {
            if (prev)
                prev->next = cur->next;
            else
                g_metrics = cur->next;
            if (g_metrics_tail == cur)
                g_metrics_tail = prev;
            break;
        }
        prev = cur;
        cur = cur->next;
    }
    pthread_mutex_unlock(&reg_mu);
    free(m);
}

int creport_metric_collector(const char *name, enum creport_metric_type type, const char *help, creport_collect_fn fn,
                             void *ctx)
{
    if (!name || !*name || !fn)
        return -1;
    pthread_mutex_lock(&reg_mu);
    for (struct creport_collector *c = g_collectors; c; c = c->next) {
        if (strcmp(c->name, name) == 0) {
            pthread_mutex_unlock(&reg_mu);
            return -1;
        }
    }
    struct creport_collector *c = calloc(1, sizeof *c);
    if (!c) {
        pthread_mutex_unlock(&reg_mu);
        return -1;
    }
    snprintf(c->name, sizeof c->name, "%s", name);
    snprintf(c->help, sizeof c->help, "%s", help ? help : "");
    c->type = type;
    c->fn = fn;
    c->ctx = ctx;
    if (g_collectors_tail)
        g_collectors_tail->next = c;
    else
        g_collectors = c;
    g_collectors_tail = c;
    pthread_mutex_unlock(&reg_mu);
    return 0;
}

int creport_metric_collector_del(const char *name)
{
    if (!name)
        return -1;
    pthread_mutex_lock(&reg_mu);
    struct creport_collector *prev = NULL, *cur = g_collectors;
    while (cur) {
        if (strcmp(cur->name, name) == 0) {
            if (prev)
                prev->next = cur->next;
            else
                g_collectors = cur->next;
            if (g_collectors_tail == cur)
                g_collectors_tail = prev;
            pthread_mutex_unlock(&reg_mu);
            free(cur);
            return 0;
        }
        prev = cur;
        cur = cur->next;
    }
    pthread_mutex_unlock(&reg_mu);
    return -1;
}

/* ------------------------------------------------------------------ snapshot builder */

struct creport_sink {
    struct creport_snap_family *fam;
    int oom;
};

static struct creport_snap_family *snap_find_or_add(struct creport_snap *snap, const char *name,
                                                    enum creport_metric_type type, const char *help)
{
    for (size_t i = 0; i < snap->nfamilies; i++)
        if (strcmp(snap->families[i].name, name) == 0)
            return &snap->families[i];
    if (snap->nfamilies == snap->cap) {
        size_t nc = snap->cap ? snap->cap * 2 : 32;
        struct creport_snap_family *nf = realloc(snap->families, nc * sizeof *nf);
        if (!nf)
            return NULL;
        snap->families = nf;
        snap->cap = nc;
    }
    struct creport_snap_family *f = &snap->families[snap->nfamilies++];
    memset(f, 0, sizeof *f);
    snprintf(f->name, sizeof f->name, "%s", name);
    snprintf(f->help, sizeof f->help, "%s", help ? help : "");
    f->type = type;
    return f;
}

static struct creport_snap_sample *fam_push_sample(struct creport_snap_family *f, const struct creport_label *labels,
                                                   size_t nlabels)
{
    if (!f)
        return NULL;
    if (nlabels > CREPORT_LABELS_MAX)
        nlabels = CREPORT_LABELS_MAX;
    if (f->nsamples == f->cap) {
        size_t nc = f->cap ? f->cap * 2 : 8;
        struct creport_snap_sample *ns = realloc(f->samples, nc * sizeof *ns);
        if (!ns)
            return NULL;
        f->samples = ns;
        f->cap = nc;
    }
    struct creport_snap_sample *s = &f->samples[f->nsamples++];
    memset(s, 0, sizeof *s);
    s->nlabels = (uint8_t)nlabels;
    for (size_t i = 0; i < nlabels; i++) {
        snprintf(s->labels[i].key, sizeof s->labels[i].key, "%s", labels[i].key ? labels[i].key : "");
        snprintf(s->labels[i].val, sizeof s->labels[i].val, "%s", labels[i].val ? labels[i].val : "");
    }
    return s;
}

void creport_sink_put(struct creport_sink *sink, const struct creport_label *labels, size_t nlabels, double val)
{
    if (!sink || sink->oom)
        return;
    struct creport_snap_sample *s = fam_push_sample(sink->fam, labels, nlabels);
    if (!s) {
        sink->oom = 1;
        return;
    }
    s->val_kind = CREPORT_VAL_DOUBLE;
    s->dval = val;
}

void creport_sink_put_i64(struct creport_sink *sink, const struct creport_label *labels, size_t nlabels, int64_t val)
{
    if (!sink || sink->oom)
        return;
    struct creport_snap_sample *s = fam_push_sample(sink->fam, labels, nlabels);
    if (!s) {
        sink->oom = 1;
        return;
    }
    s->val_kind = CREPORT_VAL_I64;
    s->ival = val;
}

void creport_sink_put_u64(struct creport_sink *sink, const struct creport_label *labels, size_t nlabels, uint64_t val)
{
    if (!sink || sink->oom)
        return;
    struct creport_snap_sample *s = fam_push_sample(sink->fam, labels, nlabels);
    if (!s) {
        sink->oom = 1;
        return;
    }
    s->val_kind = CREPORT_VAL_U64;
    s->uval = val;
}

void creport_snap_free(struct creport_snap *snap)
{
    if (!snap)
        return;
    for (size_t i = 0; i < snap->nfamilies; i++)
        free(snap->families[i].samples);
    free(snap->families);
    free(snap);
}

#ifdef TSDB_TEST_HOOKS
static pthread_mutex_t stale_mu = PTHREAD_MUTEX_INITIALIZER;
static struct creport_snap *g_stale_snap;

static struct creport_snap *snap_clone(const struct creport_snap *src)
{
    if (!src)
        return NULL;
    struct creport_snap *dst = calloc(1, sizeof *dst);
    if (!dst)
        return NULL;
    if (src->nfamilies > 0) {
        dst->families = calloc(src->nfamilies, sizeof *dst->families);
        if (!dst->families) {
            free(dst);
            return NULL;
        }
        dst->cap = src->nfamilies;
        for (size_t i = 0; i < src->nfamilies; i++) {
            const struct creport_snap_family *sf = &src->families[i];
            struct creport_snap_family *df = &dst->families[i];
            *df = *sf;
            df->samples = NULL;
            df->cap = 0;
            if (sf->nsamples > 0) {
                df->samples = malloc(sf->nsamples * sizeof *df->samples);
                if (!df->samples) {
                    dst->nfamilies = i + 1;
                    creport_snap_free(dst);
                    return NULL;
                }
                memcpy(df->samples, sf->samples, sf->nsamples * sizeof *df->samples);
                df->cap = sf->nsamples;
            }
            dst->nfamilies = i + 1;
        }
    }
    return dst;
}

static int snap_has_held_volume(const struct creport_snap *snap)
{
    for (size_t i = 0; snap && i < snap->nfamilies; i++)
        if (strcmp(snap->families[i].name, "tsdb_cluster_volume_isr_size") == 0 && snap->families[i].nsamples > 0)
            return 1;
    return 0;
}
#endif

struct creport_snap *creport_snap_take(void)
{
#ifdef TSDB_TEST_HOOKS
    if (creport_mutant() == CREPORT_MUT_METRICSTALE) {
        pthread_mutex_lock(&stale_mu);
        if (g_stale_snap) {
            struct creport_snap *dup = snap_clone(g_stale_snap);
            pthread_mutex_unlock(&stale_mu);
            return dup;
        }
        pthread_mutex_unlock(&stale_mu);
    }
#endif
    struct creport_snap *snap = calloc(1, sizeof *snap);
    if (!snap)
        return NULL;

    /* Copy collector descriptors and direct metrics under reg_mu, then invoke collectors with zero locks held. */
    struct coll_copy {
        char name[96];
        char help[320];
        enum creport_metric_type type;
        creport_collect_fn fn;
        void *ctx;
    };
    struct coll_copy *colls = NULL;
    size_t ncolls = 0;

    pthread_mutex_lock(&reg_mu);
    for (struct creport_collector *c = g_collectors; c; c = c->next)
        ncolls++;
    if (ncolls > 0) {
        colls = malloc(ncolls * sizeof *colls);
        if (!colls) {
            pthread_mutex_unlock(&reg_mu);
            creport_snap_free(snap);
            return NULL;
        }
        size_t idx = 0;
        for (struct creport_collector *c = g_collectors; c; c = c->next) {
            memcpy(colls[idx].name, c->name, sizeof colls[idx].name);
            memcpy(colls[idx].help, c->help, sizeof colls[idx].help);
            colls[idx].type = c->type;
            colls[idx].fn = c->fn;
            colls[idx].ctx = c->ctx;
            idx++;
        }
    }
    for (struct creport_metric *m = g_metrics; m; m = m->next) {
        struct creport_snap_family *f = snap_find_or_add(snap, m->name, m->type, m->help);
        if (!f) {
            pthread_mutex_unlock(&reg_mu);
            free(colls);
            creport_snap_free(snap);
            return NULL;
        }
        struct creport_label lbl[CREPORT_LABELS_MAX];
        for (uint8_t k = 0; k < m->nlabels; k++) {
            lbl[k].key = m->labels[k].key;
            lbl[k].val = m->labels[k].val;
        }
        struct creport_snap_sample *s = fam_push_sample(f, lbl, m->nlabels);
        if (!s) {
            pthread_mutex_unlock(&reg_mu);
            free(colls);
            creport_snap_free(snap);
            return NULL;
        }
        uint64_t raw = atomic_load_explicit(&m->raw, memory_order_relaxed);
        if (m->type == CREPORT_COUNTER) {
            s->val_kind = CREPORT_VAL_U64;
            s->uval = raw;
        } else {
            s->val_kind = CREPORT_VAL_DOUBLE;
            s->dval = u64_to_dbl(raw);
        }
    }
    pthread_mutex_unlock(&reg_mu);

    for (size_t i = 0; i < ncolls; i++) {
        struct creport_snap_family *f = snap_find_or_add(snap, colls[i].name, colls[i].type, colls[i].help);
        if (!f) {
            free(colls);
            creport_snap_free(snap);
            return NULL;
        }
        struct creport_sink sink = {.fam = f, .oom = 0};
        colls[i].fn(colls[i].ctx, &sink);
        if (sink.oom) {
            free(colls);
            creport_snap_free(snap);
            return NULL;
        }
    }
    free(colls);

#ifdef TSDB_TEST_HOOKS
    if (creport_mutant() == CREPORT_MUT_METRICSTALE && snap_has_held_volume(snap)) {
        pthread_mutex_lock(&stale_mu);
        if (!g_stale_snap)
            g_stale_snap = snap_clone(snap);
        pthread_mutex_unlock(&stale_mu);
    }
#endif
    return snap;
}

static void prog_reset_state(void);

void creport_reset(void)
{
    pthread_mutex_lock(&reg_mu);
    memset(secs, 0, sizeof secs);
    nsecs = 0;
    struct creport_metric *m = g_metrics;
    g_metrics = g_metrics_tail = NULL;
    struct creport_collector *c = g_collectors;
    g_collectors = g_collectors_tail = NULL;
    pthread_mutex_unlock(&reg_mu);

    while (m) {
        struct creport_metric *nx = m->next;
        free(m);
        m = nx;
    }
    while (c) {
        struct creport_collector *nx = c->next;
        free(c);
        c = nx;
    }
    prog_reset_state();
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&stale_mu);
    creport_snap_free(g_stale_snap);
    g_stale_snap = NULL;
    pthread_mutex_unlock(&stale_mu);
    atomic_store_explicit(&g_creport_mut_cached, -1, memory_order_relaxed);
#endif
}

size_t creport_dump_registry(char *buf, size_t cap)
{
    if (!buf || cap == 0)
        return 0;
    buf[0] = '\0';
    size_t off = 0;
    pthread_mutex_lock(&reg_mu);
    for (uint32_t i = 0; i < nsecs && off + 1 < cap; i++) {
        if (!secs[i].used)
            continue;
        int n = snprintf(buf + off, cap - off, "section type=%u\n", (unsigned)secs[i].type);
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    for (struct creport_metric *m = g_metrics; m && off + 1 < cap; m = m->next) {
        int n = snprintf(buf + off, cap - off, "metric %s type=%s\n", m->name,
                         m->type == CREPORT_COUNTER ? "counter" : "gauge");
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    for (struct creport_collector *c = g_collectors; c && off + 1 < cap; c = c->next) {
        int n = snprintf(buf + off, cap - off, "collector %s type=%s\n", c->name,
                         c->type == CREPORT_COUNTER ? "counter" : "gauge");
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    pthread_mutex_unlock(&reg_mu);
    buf[off] = '\0';
    return off;
}

/* ------------------------------------------------------------------ HBS_PROGRESS (R14, r14-progress, D23) */

#define HBS_PROGRESS_VER 1u
#define HBS_PROGRESS_HDR_LEN 17u
#define HBS_PROGRESS_VOL_LEN 57u
#define HBS_PROGRESS_STORE_LEN 20u

static pthread_mutex_t prog_mu = PTHREAD_MUTEX_INITIALIZER;
static creport_progress_vols_fn g_prog_vols_fn;
static void *g_prog_vols_arg;
static creport_progress_stores_fn g_prog_stores_fn;
static void *g_prog_stores_arg;
static creport_young_scrub_age_fn g_prog_young_scrub_fn;
static void *g_prog_young_scrub_arg;
static creport_store_extra_fn g_prog_store_extra_fn;
static void *g_prog_store_extra_arg;
static int g_prog_registered;

#ifdef TSDB_TEST_HOOKS
static atomic_uint_fast64_t g_prog_stall_cached;
#define PROG_FROZEN_MAX 256
struct prog_frozen_ent {
    uint32_t member_id;
    struct creport_progress *saved;
};
static struct prog_frozen_ent g_prog_frozen[PROG_FROZEN_MAX];
static uint32_t g_prog_nfrozen;
#endif

static uint64_t prog_wall_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static uint64_t prog_mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

uint64_t creport_mono_ns(void) { return prog_mono_ns(); }

static void p_put32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static void p_put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static uint32_t p_get32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; i++)
        v |= (uint32_t)p[i] << (8 * i);
    return v;
}

static uint64_t p_get64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= (uint64_t)p[i] << (8 * i);
    return v;
}

const char *creport_rebuild_role_name(uint8_t role)
{
    switch (role) {
    case CREPORT_REBUILD_NONE: return "none";
    case CREPORT_REBUILD_SNAP_SRC: return "snap_src";
    case CREPORT_REBUILD_SNAP_DST: return "snap_dst";
    case CREPORT_REBUILD_FILL: return "fill";
    default: return "?";
    }
}

void creport_progress_free(struct creport_progress *p)
{
    if (!p)
        return;
    free(p->vols);
    free(p->stores);
    free(p);
}

#ifdef TSDB_TEST_HOOKS
static struct creport_progress *progress_clone(const struct creport_progress *src)
{
    if (!src)
        return NULL;
    struct creport_progress *dst = calloc(1, sizeof *dst);
    if (!dst)
        return NULL;
    dst->sent_wall_ms = src->sent_wall_ms;
    dst->recv_mono_ns = src->recv_mono_ns;
    dst->nvols = src->nvols;
    dst->nstores = src->nstores;
    if (src->nvols > 0) {
        dst->vols = malloc((size_t)src->nvols * sizeof *dst->vols);
        if (!dst->vols) {
            free(dst);
            return NULL;
        }
        memcpy(dst->vols, src->vols, (size_t)src->nvols * sizeof *dst->vols);
    }
    if (src->nstores > 0) {
        dst->stores = malloc((size_t)src->nstores * sizeof *dst->stores);
        if (!dst->stores) {
            free(dst->vols);
            free(dst);
            return NULL;
        }
        memcpy(dst->stores, src->stores, (size_t)src->nstores * sizeof *dst->stores);
    }
    return dst;
}

static int progress_has_activity(const struct creport_progress *p)
{
    if (!p)
        return 0;
    for (uint32_t i = 0; i < p->nvols; i++) {
        if (p->vols[i].role != CREPORT_REBUILD_NONE || p->vols[i].settle_keys_waiting > 0)
            return 1;
    }
    return 0;
}
#endif

static void prog_reset_state(void)
{
    pthread_mutex_lock(&prog_mu);
    g_prog_vols_fn = NULL;
    g_prog_vols_arg = NULL;
    g_prog_stores_fn = NULL;
    g_prog_stores_arg = NULL;
    g_prog_young_scrub_fn = NULL;
    g_prog_young_scrub_arg = NULL;
    g_prog_store_extra_fn = NULL;
    g_prog_store_extra_arg = NULL;
    g_prog_registered = 0;
#ifdef TSDB_TEST_HOOKS
    for (uint32_t i = 0; i < g_prog_nfrozen; i++)
        creport_progress_free(g_prog_frozen[i].saved);
    memset(g_prog_frozen, 0, sizeof g_prog_frozen);
    g_prog_nfrozen = 0;
    atomic_store_explicit(&g_prog_stall_cached, 0, memory_order_relaxed);
#endif
    pthread_mutex_unlock(&prog_mu);
}

void creport_progress_set_providers(creport_progress_vols_fn vols_fn, void *vols_arg,
                                    creport_progress_stores_fn stores_fn, void *stores_arg)
{
    pthread_mutex_lock(&prog_mu);
    g_prog_vols_fn = vols_fn;
    g_prog_vols_arg = vols_arg;
    g_prog_stores_fn = stores_fn;
    g_prog_stores_arg = stores_arg;
    pthread_mutex_unlock(&prog_mu);
}

void creport_progress_set_young_scrub(creport_young_scrub_age_fn fn, void *arg)
{
    pthread_mutex_lock(&prog_mu);
    g_prog_young_scrub_fn = fn;
    g_prog_young_scrub_arg = arg;
    pthread_mutex_unlock(&prog_mu);
}

void creport_progress_set_store_extra(creport_store_extra_fn fn, void *arg)
{
    pthread_mutex_lock(&prog_mu);
    g_prog_store_extra_fn = fn;
    g_prog_store_extra_arg = arg;
    pthread_mutex_unlock(&prog_mu);
}

uint64_t creport_progress_query_young_scrub(uint32_t volume)
{
    pthread_mutex_lock(&prog_mu);
    creport_young_scrub_age_fn fn = g_prog_young_scrub_fn;
    void *arg = g_prog_young_scrub_arg;
    pthread_mutex_unlock(&prog_mu);
    return fn ? fn(arg, volume) : 0;
}

void creport_progress_query_store_extra(uint32_t store, uint64_t *scrub_age_s, uint64_t *drain_bytes_left)
{
    if (scrub_age_s)
        *scrub_age_s = 0;
    if (drain_bytes_left)
        *drain_bytes_left = 0;
    pthread_mutex_lock(&prog_mu);
    creport_store_extra_fn fn = g_prog_store_extra_fn;
    void *arg = g_prog_store_extra_arg;
    pthread_mutex_unlock(&prog_mu);
    if (fn)
        fn(arg, store, scrub_age_s, drain_bytes_left);
}

uint64_t creport_progress_stall_ms(void)
{
#ifdef TSDB_TEST_HOOKS
    uint64_t v = atomic_load_explicit(&g_prog_stall_cached, memory_order_relaxed);
    if (v > 0)
        return v;
    uint64_t ms = CREPORT_REBUILD_STALL_MS;
    const char *e = getenv("TSDB_PROGRESS_STALL_MS");
    if (e && *e) {
        unsigned long long parsed = strtoull(e, NULL, 10);
        if (parsed > 0)
            ms = (uint64_t)parsed;
    } else {
        const char *ct = getenv("TSDB_CLUSTER_TEST");
        const char *p = ct ? strstr(ct, "progress_stall_ms=") : NULL;
        if (p) {
            unsigned long long parsed = strtoull(p + 18, NULL, 10);
            if (parsed > 0)
                ms = (uint64_t)parsed;
        }
    }
    atomic_store_explicit(&g_prog_stall_cached, ms, memory_order_relaxed);
    return ms;
#else
    return CREPORT_REBUILD_STALL_MS;
#endif
}

uint8_t creport_progress_is_stalled(uint8_t role, uint64_t sent_wall_ms, uint64_t last_progress_ms,
                                    uint64_t recv_mono_ns, uint64_t now_mono_ns)
{
    if (role == CREPORT_REBUILD_NONE)
        return 0;
    uint64_t thresh = creport_progress_stall_ms();
    uint64_t idle_ms = (last_progress_ms > 0 && sent_wall_ms > last_progress_ms) ? (sent_wall_ms - last_progress_ms) : 0;
    uint64_t beat_age_ms =
        (recv_mono_ns > 0 && now_mono_ns > recv_mono_ns) ? ((now_mono_ns - recv_mono_ns) / 1000000ull) : 0;
    uint64_t total_idle = beat_age_ms > UINT64_MAX - idle_ms ? UINT64_MAX : idle_ms + beat_age_ms;
    return total_idle >= thresh ? 1 : 0;
}

int creport_progress_zero_total(void)
{
#ifdef TSDB_TEST_HOOKS
    return creport_mutant() == CREPORT_MUT_PROGRESSNOTOTAL ? 1 : 0;
#else
    return 0;
#endif
}

static ssize_t progress_sec_encode(uint8_t *buf, size_t cap)
{
    pthread_mutex_lock(&prog_mu);
    creport_progress_vols_fn vfn = g_prog_vols_fn;
    void *varg = g_prog_vols_arg;
    creport_progress_stores_fn sfn = g_prog_stores_fn;
    void *sarg = g_prog_stores_arg;
    pthread_mutex_unlock(&prog_mu);

    if (!vfn && !sfn)
        return 0;

    struct creport_vol_progress *vols = NULL;
    struct creport_store_progress *stores = NULL;
    uint32_t nv = 0, ns = 0;

    if (vfn) {
        vols = calloc(CREPORT_PROGRESS_VOLS_MAX, sizeof *vols);
        if (!vols)
            return -1;
        nv = vfn(varg, vols, CREPORT_PROGRESS_VOLS_MAX);
        if (nv > CREPORT_PROGRESS_VOLS_MAX)
            nv = CREPORT_PROGRESS_VOLS_MAX;
        /* Emit only volumes with activity so idle clusters send zero HBS_PROGRESS bytes. */
        uint32_t out_nv = 0;
        for (uint32_t i = 0; i < nv; i++) {
            if (vols[i].role == CREPORT_REBUILD_NONE && vols[i].settle_keys_waiting == 0 &&
                vols[i].young_scrub_age_s == 0)
                continue;
            vols[out_nv++] = vols[i];
        }
        nv = out_nv;
    }
    if (sfn) {
        stores = calloc(CREPORT_PROGRESS_STORES_MAX, sizeof *stores);
        if (!stores) {
            free(vols);
            return -1;
        }
        ns = sfn(sarg, stores, CREPORT_PROGRESS_STORES_MAX);
        if (ns > CREPORT_PROGRESS_STORES_MAX)
            ns = CREPORT_PROGRESS_STORES_MAX;
        uint32_t out_ns = 0;
        for (uint32_t i = 0; i < ns; i++) {
            if (stores[i].scrub_age_s == 0 && stores[i].drain_bytes_left == 0)
                continue;
            stores[out_ns++] = stores[i];
        }
        ns = out_ns;
    }
    if (nv == 0 && ns == 0) {
        free(vols);
        free(stores);
        return 0;
    }
    size_t need = HBS_PROGRESS_HDR_LEN + (size_t)nv * HBS_PROGRESS_VOL_LEN + (size_t)ns * HBS_PROGRESS_STORE_LEN;
    if (need > cap) {
        free(vols);
        free(stores);
        return -1;
    }

    int zero_tot = creport_progress_zero_total();
    uint64_t now_wall = prog_wall_ms();
    size_t off = 0;
    buf[off++] = HBS_PROGRESS_VER;
    p_put64(buf + off, now_wall);
    off += 8;
    p_put32(buf + off, nv);
    off += 4;
    p_put32(buf + off, ns);
    off += 4;
    for (uint32_t i = 0; i < nv; i++) {
        uint64_t tot = zero_tot ? 0 : vols[i].bytes_total;
        p_put32(buf + off, vols[i].volume);
        off += 4;
        buf[off++] = vols[i].role;
        p_put64(buf + off, vols[i].bytes_done);
        off += 8;
        p_put64(buf + off, tot);
        off += 8;
        p_put64(buf + off, vols[i].started_ms);
        off += 8;
        p_put64(buf + off, vols[i].last_progress_ms);
        off += 8;
        p_put32(buf + off, vols[i].settle_keys_waiting);
        off += 4;
        p_put64(buf + off, vols[i].settle_oldest_wait_s);
        off += 8;
        p_put64(buf + off, vols[i].young_scrub_age_s);
        off += 8;
    }
    for (uint32_t i = 0; i < ns; i++) {
        p_put32(buf + off, stores[i].store);
        off += 4;
        p_put64(buf + off, stores[i].scrub_age_s);
        off += 8;
        p_put64(buf + off, stores[i].drain_bytes_left);
        off += 8;
    }
    free(vols);
    free(stores);
    return (ssize_t)off;
}

static int progress_sec_decode(uint32_t member_id, const uint8_t *data, size_t len, void **out_val)
{
    if (!data || !out_val || len < HBS_PROGRESS_HDR_LEN)
        return -1;
    if (data[0] != HBS_PROGRESS_VER)
        return -1;
    uint64_t sent_wall = p_get64(data + 1);
    uint32_t nv = p_get32(data + 9);
    uint32_t ns = p_get32(data + 13);
    if (nv > CREPORT_PROGRESS_VOLS_MAX || ns > CREPORT_PROGRESS_STORES_MAX)
        return -1;
    size_t want = HBS_PROGRESS_HDR_LEN + (size_t)nv * HBS_PROGRESS_VOL_LEN + (size_t)ns * HBS_PROGRESS_STORE_LEN;
    if (len != want)
        return -1;

    struct creport_progress *p = calloc(1, sizeof *p);
    if (!p)
        return -1;
    p->sent_wall_ms = sent_wall;
    p->recv_mono_ns = prog_mono_ns();
    p->nvols = nv;
    p->nstores = ns;
    if (nv > 0) {
        p->vols = calloc(nv, sizeof *p->vols);
        if (!p->vols) {
            free(p);
            return -1;
        }
    }
    if (ns > 0) {
        p->stores = calloc(ns, sizeof *p->stores);
        if (!p->stores) {
            free(p->vols);
            free(p);
            return -1;
        }
    }
    size_t off = HBS_PROGRESS_HDR_LEN;
    for (uint32_t i = 0; i < nv; i++) {
        uint8_t role = data[off + 4];
        if (role > CREPORT_REBUILD_FILL) {
            creport_progress_free(p);
            return -1;
        }
        p->vols[i].volume = p_get32(data + off);
        off += 4;
        p->vols[i].role = data[off++];
        p->vols[i].bytes_done = p_get64(data + off);
        off += 8;
        p->vols[i].bytes_total = p_get64(data + off);
        off += 8;
        p->vols[i].started_ms = p_get64(data + off);
        off += 8;
        p->vols[i].last_progress_ms = p_get64(data + off);
        off += 8;
        p->vols[i].settle_keys_waiting = p_get32(data + off);
        off += 4;
        p->vols[i].settle_oldest_wait_s = p_get64(data + off);
        off += 8;
        p->vols[i].young_scrub_age_s = p_get64(data + off);
        off += 8;
    }
    for (uint32_t i = 0; i < ns; i++) {
        p->stores[i].store = p_get32(data + off);
        off += 4;
        p->stores[i].scrub_age_s = p_get64(data + off);
        off += 8;
        p->stores[i].drain_bytes_left = p_get64(data + off);
        off += 8;
    }

#ifdef TSDB_TEST_HOOKS
    if (creport_mutant() == CREPORT_MUT_PROGRESSFROZEN) {
        /* Planted bug (progressfrozen): keep the first active progress report per member and ignore updates. */
        pthread_mutex_lock(&prog_mu);
        struct creport_progress *saved = NULL;
        for (uint32_t i = 0; i < g_prog_nfrozen; i++) {
            if (g_prog_frozen[i].member_id == member_id) {
                saved = g_prog_frozen[i].saved;
                break;
            }
        }
        if (saved) {
            struct creport_progress *dup = progress_clone(saved);
            pthread_mutex_unlock(&prog_mu);
            creport_progress_free(p);
            if (!dup)
                return -1;
            *out_val = dup;
            return 0;
        }
        if (progress_has_activity(p) && g_prog_nfrozen < PROG_FROZEN_MAX) {
            struct creport_progress *copy = progress_clone(p);
            if (copy) {
                g_prog_frozen[g_prog_nfrozen].member_id = member_id;
                g_prog_frozen[g_prog_nfrozen].saved = copy;
                g_prog_nfrozen++;
            }
        }
        pthread_mutex_unlock(&prog_mu);
    }
#else
    (void)member_id;
#endif

    *out_val = p;
    return 0;
}

static void progress_sec_free(void *val) { creport_progress_free((struct creport_progress *)val); }

int creport_progress_register(void)
{
    pthread_mutex_lock(&prog_mu);
    if (g_prog_registered) {
        pthread_mutex_unlock(&prog_mu);
        return 0;
    }
    pthread_mutex_unlock(&prog_mu);

    /* format_fn is NULL: STATUS carries structured HBS_PROGRESS in its appended PROG block without duplicating into
     * the 4 KiB text secbuf. */
    int rc = creport_section(HBS_PROGRESS, progress_sec_encode, progress_sec_decode, progress_sec_free, NULL);
    if (rc == 0) {
        pthread_mutex_lock(&prog_mu);
        g_prog_registered = 1;
        pthread_mutex_unlock(&prog_mu);
    }
    return rc;
}

/* ------------------------------------------------------------------ test hook (CREPORT_SEC_TEST) */

#ifdef TSDB_TEST_HOOKS
static atomic_uint_fast64_t g_test_seq;
static atomic_int g_test_registered;

static ssize_t test_sec_encode(uint8_t *buf, size_t cap)
{
    if (cap < 12)
        return -1;
    uint64_t seq = atomic_fetch_add_explicit(&g_test_seq, 1, memory_order_relaxed) + 1;
    memcpy(buf, "R14T", 4);
    for (int i = 0; i < 8; i++)
        buf[4 + i] = (uint8_t)(seq >> (8 * i));
    return 12;
}

static int test_sec_decode(uint32_t member_id, const uint8_t *data, size_t len, void **out_val)
{
    (void)member_id;
    if (len != 12 || memcmp(data, "R14T", 4) != 0)
        return -1;
    uint64_t seq = 0;
    for (int i = 0; i < 8; i++)
        seq |= (uint64_t)data[4 + i] << (8 * i);
    uint64_t *p = malloc(sizeof *p);
    if (!p)
        return -1;
    *p = seq;
    *out_val = p;
    return 0;
}

static void test_sec_free(void *val) { free(val); }

static size_t test_sec_format(uint32_t member_id, const void *val, char *buf, size_t cap)
{
    if (!val || !buf || cap == 0)
        return 0;
    uint64_t seq = *(const uint64_t *)val;
    int n = snprintf(buf, cap, "r14test node=%" PRIu32 " seq=%" PRIu64 "\n", member_id, seq);
    return n > 0 ? (size_t)n : 0;
}

void creport_test_dump_file(const char *state_dir)
{
    if (!state_dir || !*state_dir)
        return;
    const char *ct = getenv("TSDB_CLUSTER_TEST");
    const char *rt = getenv("TSDB_REPORT_TEST");
    if ((!ct || !strstr(ct, "report_test=1")) && (!rt || !*rt))
        return;
    char path[4200];
    snprintf(path, sizeof path, "%s/creport.dump", state_dir);
    char dump[16384];
    size_t n = creport_dump_registry(dump, sizeof dump);
    FILE *f = fopen(path, "wb");
    if (f) {
        if (n > 0)
            (void)fwrite(dump, 1, n, f);
        fclose(f);
    }
}

void creport_test_init(const char *state_dir)
{
    const char *ct = getenv("TSDB_CLUSTER_TEST");
    const char *rt = getenv("TSDB_REPORT_TEST");
    if ((!ct || !strstr(ct, "report_test=1")) && (!rt || !*rt))
        return;
    if (atomic_exchange(&g_test_registered, 1) == 0)
        (void)creport_section(CREPORT_SEC_TEST, test_sec_encode, test_sec_decode, test_sec_free, test_sec_format);
    creport_test_dump_file(state_dir);
}
#endif /* TSDB_TEST_HOOKS */

#endif /* !CREPORT_DRV_ONLY */
