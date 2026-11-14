/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Healing, attaching and replacing the stores of a chunk backend (docs/design-cluster-data.md §5.5, §5.9, E15, E55).
 *
 * A chunk that is missing or damaged in its store is never edited: the object's data is read back through the
 * decoder (any k' good positions, each cell checked against its sum), the object is encoded again stripe by stripe,
 * and the cells of the position to rebuild go into a new chunk file (tmp, fsync, rename over the old name, directory
 * fsync: after a power loss the old file or the whole new one is there). What makes a rebuilt cell trustworthy is that
 * the same re-encode is compared, cell by cell, with the sums table of every chunk that survives: a set of chunks that
 * are not one encoding is refused and nothing is written. The rebuilt chunk is then read back and scrubbed.
 *
 * Which objects there are comes from the listings of the OTHER stores: a chunk found only on the store being healed is
 * an orphan (its object was deleted while the store was away, E14) and never a reason to rebuild anything. An object
 * with fewer than k' good positions is not rebuilt either, so the remains of a deleted object stay what they are. For
 * k' = 1 (a copy is enough to decode) the layer cannot tell an orphan from a survivor: the caller's `live` can.
 *
 * Epochs: a heal is fenced like a put (refused below the volume's EPOCH, raised above it), but the chunk it writes
 * carries the epoch the object was written at, so a delete at a lower epoch than the object's stays refused (E14).
 * A new store has every volume's fence raised, durably, before the first chunk is written to it.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------------------------------------------------------------- which objects */
typedef struct ent {
    uint8_t oid[16];
    int store;
    uint16_t pos;
} ent;

typedef struct entv {
    ent *v;
    size_t n, cap;
    int store;
} entv;

static int ent_add(void *vctx, const chunk_listed *c)
{
    entv *e = vctx;
    if (c->status != CHUNK_ST_OK) /* a chunk whose header does not parse names nothing */
        return 0;
    if (e->n == e->cap) {
        size_t nc = e->cap ? 2 * e->cap : 256;
        ent *nv = realloc(e->v, nc * sizeof *nv);
        if (!nv)
            return TSDB_ERR_NOMEM;
        e->v = nv;
        e->cap = nc;
    }
    memcpy(e->v[e->n].oid, c->oid, 16);
    e->v[e->n].store = e->store;
    e->v[e->n].pos = c->pos;
    e->n++;
    return 0;
}

static int ent_cmp(const void *a, const void *b) { return memcmp(a, b, 16); }

/* ---------------------------------------------------------------- one object */
typedef struct heal_ctx {
    chunk_backend *be;
    uint32_t vol;
    int store;
    chunk_store *tgt;
    const chunk_heal_opts *o;
    chunk_heal_report *rep;
} heal_ctx;

typedef struct enc_ctx {
    chunk_obj *ob;
    chunk_wr *w[CHUNK_POS_MAX];
    uint64_t *sums[CHUNK_POS_MAX]; /* what was written, for the read-back */
    bool need[CHUNK_POS_MAX];
    int mismatch;                  /* the position whose own table disagreed with the re-encode, else -1 */
} enc_ctx;

static int enc_cell(void *vctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    enc_ctx *c = vctx;
    if (c->need[pos]) {
        c->sums[pos][idx] = sum;
        return chunk_wr_cell_trusted(c->w[pos], data, len, sum);
    }
    const uint64_t *tab = chunk_obj_pos_sums(c->ob, pos);
    if (tab && tab[idx] != sum && chunk_mutant != CHUNK_MUT_HEAL_NOVERIFY) {
        c->mismatch = (int)pos;
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

static bool is_live(const heal_ctx *h, const uint8_t oid[16])
{
    if (chunk_mutant == CHUNK_MUT_HEAL_RESURRECT || !h->o->live)
        return true; /* planted: the caller's records are not asked */
    return h->o->live(h->o->live_ctx, oid) != 0;
}

/* Heals the positions of the object that are placed on the target store (only_pos >= 0: just that one, always with a
 * scrub). Counts into the report; returns non-zero only for what stops the whole heal (the store failed, no memory). */
static int heal_object(heal_ctx *h, const uint8_t vuuid[16], const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                       int only_pos, bool scrub)
{
    chunk_heal_report *rep = h->rep;
    unsigned np = chunk_npos(g), k = g->k;
    bool need[CHUNK_POS_MAX] = {false}, miss[CHUNK_POS_MAX] = {false}, mine[CHUNK_POS_MAX] = {false};
    bool any_mine = false;
    for (unsigned p = 0; p < np; p++)
        if (chunk_be_pos_store(h->be, oid, p) == h->store && (only_pos < 0 || (int)p == only_pos))
            mine[p] = any_mine = true;
    if (!any_mine)
        return 0;
    rep->objects++;
    if (!is_live(h, oid)) {
        rep->not_live++;
        return 0;
    }
    /* what is on the target, by itself: whole, missing, or damaged */
    unsigned nneed = 0;
    for (unsigned p = 0; p < np; p++) {
        if (!mine[p])
            continue;
        chunk_ident id = {.volume = h->vol, .pos = (uint16_t)p};
        memcpy(id.vuuid, vuuid, 16);
        memcpy(id.oid, oid, 16);
        chunk_rd *r = NULL;
        int rc = chunk_rd_open(h->tgt, &id, &r);
        if (rc == 0) {
            const chunk_hdr *hd = chunk_rd_hdr(r);
            uint64_t nbad = 0;
            if (memcmp(&hd->g, g, sizeof *g) != 0)
                rc = TSDB_ERR_CORRUPT; /* another encoding's chunk under this name */
            else if ((scrub || only_pos >= 0) && chunk_rd_scrub(r, &nbad) == 0 && nbad > 0)
                rc = TSDB_ERR_CORRUPT;
            chunk_rd_close(r);
            if (rc == 0) {
                rep->chunks_ok++;
                continue;
            }
        }
        if (rc == TSDB_ERR_NOTFOUND) {
            need[p] = miss[p] = true;
            nneed++;
        } else if (rc == TSDB_ERR_CORRUPT) {
            need[p] = true;
            nneed++;
        } else {
            char hex[33];
            chunk_oid_hex(oid, hex);
            rep->failed++; /* an I/O error: the old file may be fine; a second heal retries */
            fprintf(stderr, "tsdb: heal: object %s position %u: the chunk could not be read (%d)\n", hex, p, rc);
        }
    }
    if (nneed == 0)
        return 0;

    chunk_obj *ob = NULL;
    int rc = chunk_obj_open(h->be, h->vol, vuuid, oid, g, NULL, &ob);
    if (rc) {
        if (rc == TSDB_ERR_UNAVAILABLE)
            rep->unavailable++;
        else
            rep->unrecoverable++;
        return 0;
    }
    if (chunk_obj_epoch(ob) > epoch)
        epoch = chunk_obj_epoch(ob);
    /* positions that can still be read, counting those on stores that are away as not lost */
    unsigned navail = 0, ndown = 0;
    for (unsigned p = 0; p < np; p++) {
        int st = chunk_obj_pos_status(ob, p);
        navail += st == CHUNK_ST_OK;
        ndown += st == CHUNK_ST_DOWN;
    }
    unsigned want = chunk_mutant == CHUNK_MUT_HEAL_KMINUS1 ? k - 1 : k;
    if (navail < want) {
        if (navail + ndown >= k)
            rep->unavailable++;
        else
            rep->unrecoverable++;
        chunk_obj_close(ob);
        return 0;
    }

    enc_ctx ec;
    memset(&ec, 0, sizeof ec);
    ec.ob = ob;
    ec.mismatch = -1;
    int result = 0, fatal = 0; /* result: stop this object (counted); fatal: stop the heal (an error code) */
    uint8_t *buf = NULL;
    size_t cap = (size_t)k * (g->cell < g->len ? g->cell : (size_t)g->len);
    for (unsigned p = 0; p < np; p++)
        if (need[p]) {
            ec.need[p] = true;
            ec.sums[p] = malloc((size_t)chunk_pos_cells(g, p) * sizeof(uint64_t));
            if (!ec.sums[p])
                result = fatal = TSDB_ERR_NOMEM;
        }
    buf = result ? NULL : malloc(cap ? cap : 1);
    if (!buf && !result)
        result = fatal = TSDB_ERR_NOMEM;
    uint64_t hdr_epoch = chunk_mutant == CHUNK_MUT_HEAL_EPOCH0 ? 0 : epoch; /* planted: the object's epoch is lost */
    for (unsigned p = 0; p < np && result == 0; p++)
        if (need[p]) {
            chunk_ident id = {.volume = h->vol, .pos = (uint16_t)p};
            memcpy(id.vuuid, vuuid, 16);
            memcpy(id.oid, oid, 16);
            int r = chunk_wr_open_heal(h->tgt, &id, g, hdr_epoch, h->o->epoch, &ec.w[p]);
            if (r) {
                rep->failed++;
                fprintf(stderr, "tsdb: heal: cannot start the chunk of position %u (%d)\n", p, r);
                result = 1; /* this object only */
            }
        }
    bool done;
    for (uint64_t s = 0; result == 0 && s < chunk_nstripes(g); s++) {
        uint64_t off = s * (uint64_t)k * g->cell, n = g->len - off < (uint64_t)k * g->cell ? g->len - off
                                                                                           : (uint64_t)k * g->cell;
        int r = chunk_obj_read(ob, off, n, buf, 0, NULL);
        if (r && chunk_mutant == CHUNK_MUT_HEAL_KMINUS1) { /* planted: a stripe short of cells is made up (zeros) */
            memset(buf, 0, (size_t)n);
            r = 0;
        }
        if (r) {
            if (r == TSDB_ERR_UNAVAILABLE)
                rep->unavailable++;
            else if (r == TSDB_ERR_CORRUPT)
                rep->unrecoverable++;
            else
                rep->failed++;
            result = 1;
            break;
        }
        r = chunk_encode_stripe(g, s, buf, enc_cell, &ec);
        if (r) {
            rep->failed++;
            if (ec.mismatch >= 0)
                fprintf(stderr, "tsdb: heal: object: the chunk of position %d does not match the re-encode of the "
                                "others: the chunks are not one encoding, nothing is written\n", ec.mismatch);
            result = 1;
            break;
        }
    }
    done = result == 0;
    /* the caller's records once more, right before a name is made (a delete may have come in while this ran) */
    if (done && !is_live(h, oid)) {
        rep->not_live++;
        done = false;
    }
    for (unsigned p = 0; p < np; p++) {
        if (!ec.w[p])
            continue;
        if (!done) {
            chunk_wr_abort(ec.w[p]);
            ec.w[p] = NULL;
            continue;
        }
        int r = chunk_wr_commit(ec.w[p]); /* frees the writer on every path */
        ec.w[p] = NULL;
        if (r) {
            rep->failed++;
            fprintf(stderr, "tsdb: heal: the rebuilt chunk of position %u could not be made (%d)\n", p, r);
            done = false; /* the ones after it are aborted; those before it stand */
            if (chunk_store_failed(h->tgt))
                fatal = TSDB_ERR_IO;
            continue;
        }
        rep->bytes += chunk_file_size(g, p);
        bool ok = true;
        if (chunk_mutant != CHUNK_MUT_HEAL_NOVERIFY) { /* planted: no read-back */
            chunk_ident id = {.volume = h->vol, .pos = (uint16_t)p};
            memcpy(id.vuuid, vuuid, 16);
            memcpy(id.oid, oid, 16);
            chunk_rd *rd = NULL;
            uint64_t nbad = 0;
            ok = chunk_rd_open(h->tgt, &id, &rd) == 0;
            if (ok) {
                ok = chunk_rd_hdr(rd)->epoch == hdr_epoch && memcmp(chunk_rd_sums(rd), ec.sums[p],
                         (size_t)chunk_pos_cells(g, p) * sizeof(uint64_t)) == 0 &&
                     chunk_rd_scrub(rd, &nbad) == 0 && nbad == 0;
                chunk_rd_close(rd);
            }
        }
        if (!ok) {
            rep->failed++;
            fprintf(stderr, "tsdb: heal: the rebuilt chunk of position %u did not read back whole\n", p);
        } else if (miss[p])
            rep->rebuilt_missing++;
        else
            rep->rebuilt_damaged++;
    }
    for (unsigned p = 0; p < np; p++)
        free(ec.sums[p]);
    free(buf);
    chunk_obj_close(ob);
    return fatal;
}

/* ---------------------------------------------------------------- one store, one volume */
int chunk_be_heal(chunk_backend *be, uint32_t volume, int store, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    static const chunk_heal_opts none;
    chunk_heal_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (!o)
        o = &none;
    if (store < 0 || store >= be->n)
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, store);
    if (stt != CHUNK_STORE_UP && stt != CHUNK_STORE_REPLACING)
        return stt == CHUNK_STORE_DOWN_IOERR && chunk_be_open_store(be, store) ? TSDB_ERR_IO : TSDB_ERR_UNAVAILABLE;
    chunk_store *tgt = chunk_be_open_store(be, store);
    if (!tgt)
        return TSDB_ERR_UNAVAILABLE;
    int rc = chunk_be_prepare(be, volume, o->epoch);
    if (rc)
        return rc;
    entv ev = {.store = -1};
    for (int i = 0; i < be->n && rc == 0; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        if (i == store || !st)
            continue;
        ev.store = i;
        rc = chunk_store_list(st, volume, false, ent_add, &ev);
    }
    if (rc) {
        free(ev.v);
        return rc;
    }
    qsort(ev.v, ev.n, sizeof *ev.v, ent_cmp);
    heal_ctx h = {be, volume, store, tgt, o, rep};
    for (size_t i = 0; i < ev.n && rc == 0;) {
        size_t j = i;
        while (j < ev.n && memcmp(ev.v[j].oid, ev.v[i].oid, 16) == 0)
            j++;
        chunk_hdr hd;
        bool got = false;
        for (size_t x = i; x < j && !got; x++) { /* the first chunk of the object whose header reads */
            chunk_store *st = chunk_be_open_store(be, ev.v[x].store);
            got = st && chunk_store_peek(st, volume, ev.v[x].oid, ev.v[x].pos, &hd) == 0;
        }
        if (!got) {
            rep->failed++;
        } else {
            rc = heal_object(&h, hd.vuuid, ev.v[i].oid, &hd.g, hd.epoch, -1, o->scrub);
            if (chunk_store_failed(tgt) && rc == 0)
                rc = TSDB_ERR_IO;
        }
        i = j;
    }
    free(ev.v);
    return rc;
}

int chunk_be_repair(chunk_backend *be, const chunk_ident *id, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    static const chunk_heal_opts none;
    chunk_heal_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (!o)
        o = &none;
    int store = chunk_be_pos_store(be, id->oid, id->pos);
    int stt = chunk_be_store_state(be, store);
    if (stt != CHUNK_STORE_UP && stt != CHUNK_STORE_REPLACING)
        return TSDB_ERR_UNAVAILABLE;
    chunk_store *tgt = chunk_be_open_store(be, store);
    if (!tgt)
        return TSDB_ERR_UNAVAILABLE;
    int rc = chunk_be_prepare(be, id->volume, o->epoch);
    if (rc)
        return rc;
    chunk_obj *ob = NULL;
    rc = chunk_obj_open(be, id->volume, id->vuuid, id->oid, NULL, NULL, &ob);
    if (rc) { /* nothing of the object can be read: say which kind of nothing */
        if (rc == TSDB_ERR_UNAVAILABLE)
            rep->unavailable++;
        else
            rep->unrecoverable++;
        return 0;
    }
    chunk_geom g = *chunk_obj_geom(ob);
    uint64_t epoch = chunk_obj_epoch(ob);
    chunk_obj_close(ob);
    if (id->pos >= chunk_npos(&g))
        return TSDB_ERR_INVALID;
    heal_ctx h = {be, id->volume, store, tgt, o, rep};
    return heal_object(&h, id->vuuid, id->oid, &g, epoch, id->pos, true);
}

/* ---------------------------------------------------------------- settle, attach, replace */
/* Every volume any open store (or the fence) knows. */
static int volumes_of(chunk_backend *be, uint32_t **out, size_t *nout)
{
    uint32_t *v = NULL;
    size_t n = 0, cap = 0;
    pthread_mutex_lock(&be->mu);
    for (size_t i = 0; i < be->nf; i++) {
        if (n == cap) {
            cap = cap ? 2 * cap : 8;
            v = realloc(v, cap * sizeof *v);
            if (!v) {
                pthread_mutex_unlock(&be->mu);
                return TSDB_ERR_NOMEM;
            }
        }
        v[n++] = be->f[i].vol;
    }
    pthread_mutex_unlock(&be->mu);
    for (int i = 0; i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        uint32_t *vols;
        uint64_t *eps;
        size_t nv;
        if (!st || chunk_store_volumes(st, &vols, &eps, &nv) != 0)
            continue;
        for (size_t j = 0; j < nv; j++) {
            bool have = false;
            for (size_t x = 0; x < n; x++)
                have |= v[x] == vols[j];
            if (have)
                continue;
            if (n == cap) {
                cap = cap ? 2 * cap : 8;
                uint32_t *nvv = realloc(v, cap * sizeof *v);
                if (!nvv) {
                    free(vols);
                    free(eps);
                    free(v);
                    return TSDB_ERR_NOMEM;
                }
                v = nvv;
            }
            v[n++] = vols[j];
        }
        free(vols);
        free(eps);
    }
    *out = v;
    *nout = n;
    return 0;
}

static void report_add(chunk_heal_report *to, const chunk_heal_report *r)
{
    to->objects += r->objects;
    to->chunks_ok += r->chunks_ok;
    to->rebuilt_missing += r->rebuilt_missing;
    to->rebuilt_damaged += r->rebuilt_damaged;
    to->unrecoverable += r->unrecoverable;
    to->unavailable += r->unavailable;
    to->not_live += r->not_live;
    to->failed += r->failed;
    to->bytes += r->bytes;
}

int chunk_be_settle(chunk_backend *be, int store, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    chunk_heal_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (store < 0 || store >= be->n)
        return TSDB_ERR_INVALID;
    uint32_t *vols = NULL;
    size_t nv = 0;
    int rc = volumes_of(be, &vols, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++) {
        chunk_heal_opts ho;
        memset(&ho, 0, sizeof ho);
        if (o)
            ho = *o;
        if (ho.epoch == 0)
            ho.epoch = chunk_be_epoch(be, vols[i]); /* the backend's own account: the volume's current fence */
        chunk_heal_report r;
        rc = chunk_be_heal(be, vols[i], store, &ho, &r);
        report_add(rep, &r);
    }
    free(vols);
    if (rc)
        return rc;
    if (chunk_be_store_state(be, store) == CHUNK_STORE_REPLACING) {
        if (rep->failed == 0 && rep->unavailable == 0) {
            atomic_store(&be->state[store], CHUNK_STORE_UP);
            fprintf(stderr, "tsdb: chunk backend: store %d (%s) is UP: %llu chunks rebuilt, %llu objects past repair\n",
                    store, be->path[store] ? be->path[store] : "-",
                    (unsigned long long)(rep->rebuilt_missing + rep->rebuilt_damaged),
                    (unsigned long long)rep->unrecoverable);
            return 0;
        }
        return TSDB_ERR_UNAVAILABLE; /* filled as far as the other stores allow: settle again later */
    }
    return 0;
}

/* Puts the open store in the place of index i (which is DOWN) as REPLACING. */
static int install(chunk_backend *be, int i, chunk_store *ns, const char *path)
{
    char *p = strdup(path);
    if (!p)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&be->mu);
    chunk_store *old = atomic_load(&be->st[i]);
    if (old) { /* one that failed in service: kept open until close, a thread may still hold a writer on it */
        chunk_store **nr = realloc(be->retired, (size_t)(be->nretired + 1) * sizeof *nr);
        if (!nr) {
            pthread_mutex_unlock(&be->mu);
            free(p);
            return TSDB_ERR_NOMEM;
        }
        be->retired = nr;
        be->retired[be->nretired++] = old;
    }
    free(be->path[i]);
    be->path[i] = p;
    atomic_store(&be->state[i], CHUNK_STORE_REPLACING);
    atomic_store_explicit(&be->st[i], ns, memory_order_release);
    pthread_mutex_unlock(&be->mu);
    return 0;
}

/* Every volume's fence into the new store, durably. */
static int fence_new_store(chunk_backend *be, chunk_store *ns)
{
    uint32_t *vols = NULL;
    size_t nv = 0;
    int rc = volumes_of(be, &vols, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++)
        rc = chunk_store_raise(ns, vols[i], chunk_be_epoch(be, vols[i]));
    free(vols);
    return rc;
}

/* The caller's epoch below a volume's fence: refused before anything is made (a store half-filled for nothing). */
static int check_epoch(chunk_backend *be, const chunk_heal_opts *o)
{
    if (!o || o->epoch == 0)
        return 0;
    uint32_t *vols = NULL;
    size_t nv = 0;
    int rc = volumes_of(be, &vols, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++)
        if (o->epoch < chunk_be_epoch(be, vols[i]) && chunk_mutant != CHUNK_MUT_NOFENCE)
            rc = CHUNK_ERR_FENCED;
    free(vols);
    return rc;
}

static bool id_taken(chunk_backend *be, int except, const uint8_t id[16])
{
    for (int i = 0; i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        if (i != except && st && memcmp(chunk_store_id(st), id, 16) == 0)
            return true;
    }
    return false;
}

int chunk_be_attach(chunk_backend *be, int index, const char *path, const uint8_t *id, const chunk_heal_opts *o,
                    chunk_heal_report *rep)
{
    if (index < 0 || index >= be->n || !path)
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, index);
    if (stt == CHUNK_STORE_UP || stt == CHUNK_STORE_REPLACING || chunk_be_open_store(be, index))
        return TSDB_ERR_INVALID; /* open already (or failed in service: its descriptors cannot be trusted) */
    chunk_store *ns = NULL;
    int rc = check_epoch(be, o);
    if (rc)
        return rc;
    rc = chunk_store_open_quiet(path, id, &ns);
    if (rc)
        return rc;
    if (id_taken(be, index, chunk_store_id(ns))) {
        chunk_store_close(ns);
        return TSDB_ERR_INVALID; /* another index already is this store */
    }
    uint32_t *vols;
    uint64_t *eps;
    size_t nv;
    rc = chunk_store_volumes(ns, &vols, &eps, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++)
        rc = chunk_be_see_epoch(be, vols[i], eps[i]); /* it may have seen an epoch the others have not */
    if (rc == 0) {
        free(vols);
        free(eps);
    }
    if (rc == 0)
        rc = fence_new_store(be, ns); /* and the epochs it missed while it was away, before any chunk is made in it */
    if (rc == 0)
        rc = install(be, index, ns, path);
    if (rc) {
        chunk_store_close(ns);
        return rc;
    }
    return chunk_be_settle(be, index, o, rep);
}

int chunk_be_replace(chunk_backend *be, int index, const char *new_path, uint8_t new_id_out[16], const chunk_heal_opts *o,
                     chunk_heal_report *rep)
{
    if (index < 0 || index >= be->n || !new_path)
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, index);
    if (stt == CHUNK_STORE_UP || stt == CHUNK_STORE_REPLACING)
        return TSDB_ERR_INVALID; /* an UP store is not lost; a REPLACING one is settled, not replaced */
    uint8_t id[16];
    int rc = check_epoch(be, o);
    if (rc == 0)
        rc = chunk_store_init(new_path, NULL, id);
    if (rc)
        return rc;
    if (new_id_out)
        memcpy(new_id_out, id, 16);
    chunk_store *ns = NULL;
    rc = chunk_store_open_quiet(new_path, id, &ns);
    if (rc)
        return rc;
    if (chunk_mutant != CHUNK_MUT_REPLACE_LATE) /* planted: raised after the first chunk */
        rc = fence_new_store(be, ns);
    if (rc == 0)
        rc = install(be, index, ns, new_path);
    if (rc) {
        chunk_store_close(ns);
        return rc;
    }
    rc = chunk_be_settle(be, index, o, rep);
    if (chunk_mutant == CHUNK_MUT_REPLACE_LATE && fence_new_store(be, ns) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    return rc;
}
