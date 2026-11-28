/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster EC volume salvage: leader MK_DROP_OBJ commit + retire, replica apply, and lost-object scan.
 * See src/ecsalvage.h for design and invariants.
 */
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "chunk.h"
#include "ecapply.h"
#include "ecsalvage.h"
#include "ecswap.h"
#include "internal.h"
#include "repl.h"

void (*_Atomic ecsalvage_leader_crash_hook)(struct eng *db);

uint64_t ec_eng_parts_gen(const struct eng *db)
{
    return db ? atomic_load_explicit(&((struct eng *)db)->parts_gen, memory_order_acquire) : 0;
}

void ec_salvage_report_free(ec_salvage_report *rep)
{
    if (!rep)
        return;
    free(rep->objs);
    memset(rep, 0, sizeof *rep);
}

#ifdef TSDB_EC
static int salvage_mutant_notlost(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *m = getenv("TSDB_SALVAGE_MUTANT");
    return m && strcmp(m, "salvagenotlost") == 0;
#else
    return 0;
#endif
}

static bool oid_in_list(const uint8_t oid[16], const uint8_t *oids, uint32_t noids)
{
    for (uint32_t i = 0; i < noids; i++)
        if (memcmp(oid, oids + (size_t)i * MK_OID, MK_OID) == 0)
            return true;
    return false;
}

/* Evaluates one .ec object record: counts positions permanently lost (DESTROYED or unwritten at degraded settle)
 * and whether any position is held by an OUT member (which may still return). */
static void eval_obj(const ec_record *rec, ec_salvage_pos_fn pos_fn, void *pos_ctx, bool *is_lost_out,
                     bool *has_out_out)
{
    unsigned k = rec->k;
    unsigned m = rec->m;
    unsigned npos = k + m;
    unsigned gone = 0;
    bool any_out = false;
    for (unsigned i = 0; i < npos; i++) {
        if (rec->positions != 0 && (i >= 32u || !(rec->positions & (1u << i)))) {
            gone++;
            continue;
        }
        int st = pos_fn(pos_ctx, rec, i);
        if (st == EC_SALV_DESTROYED)
            gone++;
        else if (st == EC_SALV_OUT)
            any_out = true;
    }
    unsigned surviving = gone <= npos ? npos - gone : 0;
    bool lost = salvage_mutant_notlost() ? (surviving <= k) : (surviving < k);
    if (is_lost_out)
        *is_lost_out = lost;
    if (has_out_out)
        *has_out_out = any_out;
}

static int cmp_part_key_block(const void *a, const void *b)
{
    const part *pa = *(const part *const *)a;
    const part *pb = *(const part *const *)b;
    if (pa->partition < pb->partition)
        return -1;
    if (pa->partition > pb->partition)
        return 1;
    if (pa->min_block < pb->min_block)
        return -1;
    if (pa->min_block > pb->min_block)
        return 1;
    return strcmp(pa->name, pb->name);
}

/* Retires `parts[0..n)` grouped by partition key via ec_swap_salvage_at. On the leader (`salvage_pos > 0`), releases
 * `db->unsettle_hold_pos` on the final partition group (or immediately under publish_lock if every part was already
 * dropped concurrently). When `retired_out` is non-NULL, marks `retired_out[k] = true` for each `parts[k]` actually
 * retired by this call; `shadowed_out` accumulates parts that shadowed lower-block parts. */
static int retire_grouped(eng *db, part **parts, size_t n, uint64_t salvage_pos, bool clear_hold, bool *retired_out,
                          size_t *shadowed_out)
{
    if (n > 1)
        qsort(parts, n, sizeof *parts, cmp_part_key_block);
    if (retired_out)
        memset(retired_out, 0, n * sizeof *retired_out);

    size_t i = 0;
    int rc = 0;
    while (i < n && rc == 0) {
        int64_t key = parts[i]->partition;
        size_t j = i + 1;
        while (j < n && parts[j]->partition == key)
            j++;
        bool is_last = (j == n);

        /* Filter out any part that was concurrently dropped before this group's swap. */
        part **live = malloc((j - i) * sizeof *live);
        size_t *live_idx = retired_out ? malloc((j - i) * sizeof *live_idx) : NULL;
        if (!live || (retired_out && !live_idx)) {
            free(live);
            free(live_idx);
            return TSDB_ERR_NOMEM;
        }
        size_t nlive = 0;
        pthread_mutex_lock(&db->lock);
        for (size_t k = i; k < j; k++) {
            if (parts[k]->dropped)
                continue;
            for (size_t idx = 0; idx < db->nparts; idx++) {
                if (db->parts[idx] == parts[k]) {
                    if (live_idx)
                        live_idx[nlive] = k;
                    live[nlive++] = parts[k];
                    break;
                }
            }
        }
        pthread_mutex_unlock(&db->lock);

        if (nlive > 0) {
            rc = ec_swap_salvage_at(db, live, nlive, false, salvage_pos, true, clear_hold && is_last, shadowed_out);
            if (rc == 0 && retired_out) {
                for (size_t k = 0; k < nlive; k++)
                    retired_out[live_idx[k]] = true;
            }
        } else if (clear_hold && is_last) {
            pthread_mutex_lock(&db->publish_lock);
            atomic_store_explicit(&db->unsettle_hold_pos, 0, memory_order_release);
            pthread_mutex_unlock(&db->publish_lock);
        }
        free(live);
        free(live_idx);
        i = j;
    }
    return rc;
}
#endif

bool ec_eng_has_lost_obj(eng *db, ec_salvage_pos_fn pos_fn, void *pos_ctx)
{
    if (!db || !pos_fn)
        return false;
#ifndef TSDB_EC
    (void)pos_ctx;
    return false;
#else
    /* Never call pos_fn while holding db->lock (pos_fn may take cluster_copy or chunkpeer locks). Snapshot the
     * lightweight ec_record metadata needed (k, m, positions, stripe_version, oid) under db->lock first. */
    typedef struct {
        ec_record rec;
    } snap_rec;
    snap_rec *recs = NULL;
    size_t n = 0, cap = 0;
    bool oom = false;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (p->dropped || !ep || ep->test_mark)
            continue;
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 16;
            snap_rec *x = realloc(recs, nc * sizeof *x);
            if (!x) {
                oom = true;
                break;
            }
            recs = x;
            cap = nc;
        }
        recs[n].rec = ep->rec;
        recs[n].rec.sums = NULL;
        n++;
    }
    pthread_mutex_unlock(&db->lock);
    if (oom) {
        free(recs);
        return true; /* fail-safe: do not falsely claim nothing is lost on OOM */
    }

    bool found = false;
    for (size_t i = 0; i < n && !found; i++) {
        unsigned k = recs[i].rec.k, m = recs[i].rec.m, npos = k + m;
        unsigned gone = 0;
        for (unsigned pos = 0; pos < npos; pos++) {
            if (recs[i].rec.positions != 0 && (pos >= 32u || !(recs[i].rec.positions & (1u << pos)))) {
                gone++;
                continue;
            }
            if (pos_fn(pos_ctx, &recs[i].rec, pos) == EC_SALV_DESTROYED)
                gone++;
        }
        if (gone > m)
            found = true;
    }
    free(recs);
    return found;
#endif
}

int ec_salvage_leader(eng *db, ec_salvage_pos_fn pos_fn, void *pos_ctx, bool force, ec_salvage_report *rep, char *why,
                      size_t wn)
{
    if (rep)
        memset(rep, 0, sizeof *rep);
    if (why && wn)
        why[0] = '\0';
    if (!db || !pos_fn)
        return TSDB_ERR_INVALID;
#ifndef TSDB_EC
    (void)pos_ctx;
    (void)force;
    if (why && wn)
        snprintf(why, wn, "volume salvage requires the erasure codec (TSDB_EC)");
    return TSDB_ERR_INVALID;
#else
    if (db->opt.read_only || db->opt.follow || db->rp) {
        if (why && wn)
            snprintf(why, wn, "volume salvage must run on the volume leader");
        return TSDB_ERR_READONLY;
    }
    if (!db->ec_cluster) {
        if (why && wn)
            snprintf(why, wn, "volume is not a cluster erasure-coded volume");
        return TSDB_ERR_INVALID;
    }
    if (eng_check(db)) {
        if (why && wn)
            snprintf(why, wn, "volume engine has failed");
        return TSDB_ERR_IO;
    }

    /* Step 1: snapshot candidate .ec parts with references under db->lock, then evaluate unlocked. */
    part **cand = NULL;
    size_t ncand = 0, ccap = 0;
    int rc = 0;
    pthread_mutex_lock(&db->lock);
    if (eng_check(db)) {
        rc = TSDB_ERR_IO;
    } else {
        for (size_t i = 0; i < db->nparts; i++) {
            part *p = db->parts[i];
            struct ecpart *ep = atomic_load(&p->ec);
            if (p->dropped || !ep || ep->test_mark)
                continue;
            if (ncand == ccap) {
                size_t nc = ccap ? ccap * 2 : 16;
                part **x = realloc(cand, nc * sizeof *x);
                if (!x) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                cand = x;
                ccap = nc;
            }
            part_ref(p);
            cand[ncand++] = p;
        }
    }
    pthread_mutex_unlock(&db->lock);
    if (rc != 0) {
        for (size_t i = 0; i < ncand; i++)
            part_unref(cand[i]);
        free(cand);
        return rc;
    }
    if (ncand == 0) {
        free(cand);
        return 0;
    }

    part **lost = calloc(ncand, sizeof *lost);
    if (!lost) {
        for (size_t i = 0; i < ncand; i++)
            part_unref(cand[i]);
        free(cand);
        return TSDB_ERR_NOMEM;
    }
    size_t nlost = 0;
    bool any_out = false;
    for (size_t i = 0; i < ncand; i++) {
        struct ecpart *ep = atomic_load(&cand[i]->ec);
        bool is_lost = false, has_out = false;
        if (ep && !ep->test_mark)
            eval_obj(&ep->rec, pos_fn, pos_ctx, &is_lost, &has_out);
        if (has_out)
            any_out = true;
        if (is_lost)
            lost[nlost++] = cand[i];
        else
            part_unref(cand[i]);
    }
    free(cand);

    if (any_out && !force) {
        for (size_t i = 0; i < nlost; i++)
            part_unref(lost[i]);
        free(lost);
        if (why && wn)
            snprintf(why, wn,
                     "refusing salvage while an OUT stripe holder may still return (declare it destroyed or pass --force)");
        return TSDB_ERR_LOCKED;
    }
    if (nlost == 0) {
        free(lost);
        return 0;
    }

    /* Step 1b: unless --force, refuse to drop a lost .ec part that still has a local settled_<P>.part copy on the
     * leader (still recoverable within keep_s without losing data). */
    if (!force) {
        for (size_t i = 0; i < nlost; i++) {
            char settled[sizeof lost[0]->name + 16];
            size_t nl = strlen(lost[i]->name);
            if (nl > 3 && strcmp(lost[i]->name + nl - 3, ".ec") == 0) {
                snprintf(settled, sizeof settled, "settled_%.*s.part", (int)(nl - 3), lost[i]->name);
                if (faccessat(db->dirfd, settled, F_OK, 0) == 0) {
                    struct ecpart *ep = atomic_load(&lost[i]->ec);
                    char hex[33] = {0};
                    if (ep)
                        chunk_oid_hex(ep->rec.oid, hex);
                    for (size_t k = 0; k < nlost; k++)
                        part_unref(lost[k]);
                    free(lost);
                    if (why && wn)
                        snprintf(why, wn,
                                 "lost object %s still has local %s copy (recoverable; pass --force to drop anyway)",
                                 hex, settled);
                    return TSDB_ERR_LOCKED;
                }
            }
        }
    }

    /* Step 2: pin all lost .ec parts (`p->settling = true`) under db->lock. If an in-flight conversion unsettle or
     * re-encode (started before peers were marked DESTROYED) is still unwinding, wait briefly before giving up. */
    size_t pinned = 0;
    for (int attempt = 0; attempt < 40; attempt++) {
        bool busy = false;
        pthread_mutex_lock(&db->lock);
        if (eng_check(db)) {
            rc = TSDB_ERR_IO;
        } else {
            for (size_t i = 0; i < nlost; i++) {
                part *p = lost[i];
                if (p->dropped)
                    continue;
                bool listed = false;
                for (size_t j = 0; j < db->nparts; j++) {
                    if (db->parts[j] == p) {
                        listed = true;
                        break;
                    }
                }
                if (listed && (p->settling || p->reencoding || p->merging)) {
                    busy = true;
                    break;
                }
            }
            if (!busy) {
                for (size_t i = 0; i < nlost; i++) {
                    part *p = lost[i];
                    if (p->dropped) {
                        part_unref(p);
                        continue;
                    }
                    bool listed = false;
                    for (size_t j = 0; j < db->nparts; j++) {
                        if (db->parts[j] == p) {
                            listed = true;
                            break;
                        }
                    }
                    if (!listed) {
                        part_unref(p);
                        continue;
                    }
                    p->settling = true;
                    lost[pinned++] = p;
                }
            }
        }
        pthread_mutex_unlock(&db->lock);
        if (!busy || rc != 0)
            break;
        if (atomic_load(&db->stop)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        if (attempt + 1 == 40) {
            rc = TSDB_ERR_LOCKED;
            break;
        }
        struct timespec ts = {0, 50 * 1000000L};
        nanosleep(&ts, NULL);
    }
    if (rc != 0 || pinned == 0) {
        if (rc != 0) {
            for (size_t i = 0; i < nlost; i++)
                part_unref(lost[i]);
        }
        free(lost);
        if (rc == TSDB_ERR_LOCKED && why && wn)
            snprintf(why, wn, "a lost .ec part is busy (merging, settling or re-encoding); retry");
        return rc;
    }

    if (pinned > 1)
        qsort(lost, pinned, sizeof *lost, cmp_part_key_block);

    ec_salvage_obj *robjs = rep ? calloc(pinned, sizeof *robjs) : NULL;
    if (rep && !robjs) {
        rc = TSDB_ERR_NOMEM;
        goto unpin;
    }
    size_t nrep = 0;
    size_t shadowed = 0;
    uint64_t rows_lost = 0;

    /* Step 3: commit one MK_DROP_OBJ per batch of up to MK_FILES_MAX oids, then retire on the leader. */
    size_t off = 0;
    while (off < pinned && rc == 0) {
        size_t batch = pinned - off;
        if (batch > MK_FILES_MAX)
            batch = MK_FILES_MAX;

        uint8_t *oids = malloc(batch * MK_OID);
        bool *did_retire = calloc(batch, sizeof *did_retire);
        if (!oids || !did_retire) {
            free(oids);
            free(did_retire);
            rc = TSDB_ERR_NOMEM;
            break;
        }
        for (size_t i = 0; i < batch; i++) {
            struct ecpart *ep = atomic_load(&lost[off + i]->ec);
            memcpy(oids + i * MK_OID, ep->rec.oid, MK_OID);
        }

        uint64_t pos = 0;
        bool committed = false;
        jr_serial_lock(db);
        pthread_mutex_lock(&db->lock);
        bool still_ok = eng_check(db) == 0;
        bool hold_busy = atomic_load_explicit(&db->unsettle_hold_pos, memory_order_acquire) != 0;
        bool any_live = false;
        for (size_t i = 0; i < batch && still_ok && !hold_busy; i++) {
            if (lost[off + i]->dropped)
                continue;
            for (size_t j = 0; j < db->nparts; j++) {
                if (db->parts[j] == lost[off + i]) {
                    any_live = true;
                    break;
                }
            }
        }
        if (still_ok && !hold_busy && any_live) {
            pthread_mutex_lock(&db->buf_lock);
            uint64_t hold_pos = db->fno_next ? db->fno_next : 1;
            pthread_mutex_unlock(&db->buf_lock);
            atomic_store_explicit(&db->unsettle_hold_pos, hold_pos, memory_order_release);
        }
        pthread_mutex_unlock(&db->lock);

        if (!still_ok) {
            rc = TSDB_ERR_IO;
        } else if (hold_busy) {
            rc = TSDB_ERR_LOCKED;
            if (why && wn)
                snprintf(why, wn, "an unsettle or salvage is already in flight on this volume; retry");
        } else if (any_live) {
            mk m;
            memset(&m, 0, sizeof m);
            m.type = MK_DROP_OBJ;
            m.span = 1;
            m.u.drop_obj = (mk_drop_obj){.noids = (uint32_t)batch, .oids = oids};
            rc = jr_markers_append(db, &m, 1, "a DROP_OBJ marker was appended but its commit point did not come", &pos);
            if (rc == 0 && pos == 0)
                rc = TSDB_ERR_INVALID;
            if (rc == 0)
                rc = jr_markers_wait(db, pos, "a DROP_OBJ marker was appended but its commit point did not come");
            if (rc == 0) {
                committed = true;
                void (*lh)(struct eng *) = atomic_load(&ecsalvage_leader_crash_hook);
                if (lh)
                    lh(db);
                rc = retire_grouped(db, lost + off, batch, pos, true, did_retire, &shadowed);
            } else if (pos == 0) {
                atomic_store_explicit(&db->unsettle_hold_pos, 0, memory_order_release);
            }
            if (committed && rc != 0 && eng_check(db) == 0)
                eng_fail(db, "salvage retire failed after MK_DROP_OBJ committed", EIO);
        }
        jr_serial_unlock(db);
        free(oids);

        if (rc == 0 && committed) {
            for (size_t i = 0; i < batch; i++) {
                if (!did_retire[i])
                    continue;
                part *p = lost[off + i];
                struct ecpart *ep = atomic_load(&p->ec);
                rows_lost += p->rows;
                if (robjs) {
                    memcpy(robjs[nrep].oid, ep->rec.oid, 16);
                    robjs[nrep].partition = p->partition;
                    robjs[nrep].rows = p->rows;
                    nrep++;
                }
            }
        }
        free(did_retire);
        off += batch;
    }

    if (rc == 0 && rep) {
        rep->objs = robjs;
        rep->nobjs = nrep;
        rep->shadowed = shadowed;
        rep->rows_lost = rows_lost;
        robjs = NULL;
    }
    free(robjs);

unpin:
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < pinned; i++)
        lost[i]->settling = false;
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < pinned; i++)
        part_unref(lost[i]);
    free(lost);

    if (rc != 0 && why && wn && why[0] == '\0')
        snprintf(why, wn, "volume salvage failed: %s", tsdb_strerror(rc));
    return rc;
#endif
}

int ec_salvage_apply(eng *db, uint64_t pos, const mk *m)
{
    if (!db || !m || m->type != MK_DROP_OBJ)
        return TSDB_ERR_INVALID;
#ifndef TSDB_EC
    (void)pos;
    fprintf(stderr, "tsdb: %s: MK_DROP_OBJ at pos %" PRIu64 " needs the erasure codec (TSDB_EC)\n", db->dir, pos);
    return TSDB_ERR_INVALID;
#else
    if (eng_check(db))
        return TSDB_ERR_IO;

    const mk_drop_obj *d = &m->u.drop_obj;
    part **in = NULL;
    size_t n = 0, cap = 0;
    int rc = 0;
    int64_t t0 = eng_mono_ms();

    for (;;) {
        n = 0;
        rc = 0;
        bool busy = false;
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < db->nparts; i++) {
            part *p = db->parts[i];
            struct ecpart *ep = atomic_load(&p->ec);
            if (p->dropped || !ep || ep->test_mark)
                continue;
            if (!oid_in_list(ep->rec.oid, d->oids, d->noids))
                continue;
            if (p->settling || p->reencoding || p->merging) {
                busy = true;
                break;
            }
            if (n == cap) {
                size_t nc = cap ? cap * 2 : 8;
                part **x = realloc(in, nc * sizeof *x);
                if (!x) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                in = x;
                cap = nc;
            }
            part_ref(p);
            p->settling = true;
            in[n++] = p;
        }
        if (busy || rc != 0) {
            for (size_t i = 0; i < n; i++)
                in[i]->settling = false;
        }
        pthread_mutex_unlock(&db->lock);

        if (busy) {
            for (size_t i = 0; i < n; i++)
                part_unref(in[i]);
            n = 0;
            if (atomic_load_explicit(&db->stop, memory_order_acquire) ||
                atomic_load_explicit(&db->replica_stop, memory_order_acquire) || eng_check(db) != 0 ||
                eng_mono_ms() - t0 >= 10000) {
                rc = TSDB_ERR_LOCKED;
                break;
            }
            uint64_t gen = replica_mu_drop(db);
            struct timespec ts = {0, 50 * 1000000L};
            nanosleep(&ts, NULL);
            if (replica_mu_retake(db, gen)) {
                rc = RP_AGAIN;
                break;
            }
            continue;
        }
        break;
    }

    if (rc != 0) {
        for (size_t i = 0; i < n; i++)
            part_unref(in[i]);
        free(in);
        return rc;
    }
    if (n == 0) {
        free(in);
        return 0; /* idempotent replay: all named .ec parts already retired or dropped */
    }

    rc = retire_grouped(db, in, n, 0, false, NULL, NULL);

    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++)
        in[i]->settling = false;
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < n; i++)
        part_unref(in[i]);
    free(in);

    if (rc != 0 && rc != RP_AGAIN)
        fprintf(stderr, "tsdb: %s: MK_DROP_OBJ at pos %" PRIu64 " failed: %s\n", db->dir, pos, tsdb_strerror(rc));
    return rc;
#endif
}
