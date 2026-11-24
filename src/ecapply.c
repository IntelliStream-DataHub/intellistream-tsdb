/* SPDX-License-Identifier: AGPL-3.0-only */
/* The apply of a committed MK_COLD on a log replica, the leader's swap hooks and the recovery's decision (ecapply.h;
 * design-cluster-data.md 2.3 step 6, the R8 plan's D2, D5, D6, D8).
 *
 * A replica's apply (ecapply_cold) in four steps, on the applier with rp->mu held as apply_commit:
 *   1. the marker checked (under rp->mu, no I/O): every object record decodes for a cluster ec volume and names its
 *      oid, the key and Z; one .ec file per object, the Y files parts of the key; no retired oid (R10's).
 *   2. rp->mu let go (replica_mu_drop): the sealed memtables flushed (replica_flush_through: never the active one, a
 *      memtable holds one leader segment and a part's wal_seq must keep meaning "every row of that segment"); under
 *      stage_mu every staged file read and checked against the marker (size, xxh64; an .ec file's record against the
 *      marker's bytes, a Y part's header checksum), the Y parts linked to tmp_0_ names and opened. A cut meanwhile:
 *      everything dropped, RP_AGAIN.
 *   3. publish_lock: fresh block numbers, the .ec parts first in the leader's order, then Y above them (rows written
 *      after the marker rank above both: higher numbers, or the memtable). Each .ec file is rewritten as a tmp with
 *      its record's block range and source set to its new name, re-hashed and synced (D5), and opened. Their PPOS
 *      records (Z), then RSTATE's COLD `doing` (rs_cold_begin), both before the intent's commit point.
 *   4. latest_rw and db->lock in one hold with the intent, so that a retention drop of the key (merge.c: it takes no
 *      publish_lock) is either before (nothing of the key listed: list nothing, retire nothing, the objects are the
 *      leader's to collect by MK_GC) or after (it retires the new parts). Every own part of the key with a position
 *      above Z stops the apply (the gate failed): nothing renamed, ec_diverge (durable, with no lock held), the
 *      applier stops on this volume. Else the intent v5 (src/ecswap.c has its lines and recovery), the directory
 *      synced, the renames (N, Y, R), the list changed (an own part retired is marked dropped: a merge of it in flight
 *      drops its output), released, the directory synced, the intent unlinked, MANIFEST published.
 * apply_cold writes RSTATE's `done` once this returns 0. A replay finds `doing` and asks ecapply_cold_decide.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/): without it a COLD is refused. */
#include "internal.h"
#include "ecapply.h"
#include "ecdiverge.h"
#include "ecdigest.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "ppos.h"
#include "eccold.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <stdatomic.h>
#include <stdlib.h>

void (*_Atomic ecapply_crash_hook)(struct eng *db, int which);
void (*_Atomic ecapply_test_hook)(struct eng *db, int step);
_Atomic uint64_t ecapply_test_ft;

static void crash_at(struct eng *db, int which)
{
    void (*h)(struct eng *, int) = atomic_load(&ecapply_crash_hook);
    if (h)
        h(db, which);
}

#ifdef TSDB_EC
/* one file of the marker */
typedef struct {
    char name[128]; /* the leader's name */
    uint64_t size, xxh, hc;
    uint8_t kind;
    uint64_t lblock; /* the leader's min_block: the order of the fresh numbers */
    uint8_t *buf;    /* .ec: the staged bytes, rewritten in place */
    size_t len;
    uint64_t roff, rlen;
    ec_record rec;
    bool rec_ok;
    part *p;
    char tmp[64];  /* the name the part was opened from (tmp_0_*.part, tmp_*.ec) */
    char fin[96];  /* its new name */
} cfile;

static uint64_t le64(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | b[i];
    return v;
}

static void st64(uint8_t *b, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}

static int by_lblock(const void *a, const void *b)
{
    const cfile *x = *(cfile *const *)a, *y = *(cfile *const *)b;
    return x->lblock < y->lblock ? -1 : x->lblock > y->lblock;
}

/* keep: an intent names the temporaries (written, its apply cut by a failed engine): the next open decides, and a tmp
 * unlinked here would read as "renamed" to a roll-forward */
static void cf_free(struct eng *db, cfile *f, size_t n, bool keep)
{
    for (size_t i = 0; i < n; i++) {
        if (f[i].p) {
            part_unref(f[i].p);
            if (f[i].tmp[0] && !keep)
                unlinkat(db->dirfd, f[i].tmp, 0);
        } else if (f[i].tmp[0] && f[i].kind == MK_FILE_Y && !keep) {
            unlinkat(db->dirfd, f[i].tmp, 0);
        }
        free(f[i].buf);
        if (f[i].rec_ok)
            ec_record_free(&f[i].rec);
    }
    free(f);
}

/* step 1: the marker's lists, checked (rp->mu held, no I/O) */
static int cold_check(struct eng *db, uint64_t pos, const mk_cold *c, cfile **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (c->nretired) {
        fprintf(stderr, "tsdb: %s: the COLD at position %" PRIu64 " retires objects (R10's re-encode): not applied by "
                        "this replica\n", db->dir, pos);
        return TSDB_ERR_INVALID;
    }
    size_t at = 0, nobj = 0;
    mk_cold_obj o;
    while (mk_cold_obj_next(c, &at, &o)) {
        ec_record r;
        if (ec_record_decode_as(o.rec, o.rec_len, &r, true) != 0) {
            fprintf(stderr, "tsdb: %s: an object record of the COLD at position %" PRIu64 " does not decode\n", db->dir,
                    pos);
            return TSDB_ERR_CORRUPT;
        }
        bool good = memcmp(r.oid, o.oid, MK_OID) == 0 && r.key == c->key &&
                    (r.ppos == c->z || ecapply_mutant == ECAPPLY_MUT_CHECKZ);
        ec_record_free(&r);
        if (!good) {
            fprintf(stderr, "tsdb: %s: an object record of the COLD at position %" PRIu64 " does not name its oid, key "
                            "%" PRId64 " and Z %" PRIu64 "\n", db->dir, pos, c->key, c->z);
            return TSDB_ERR_CORRUPT;
        }
        nobj++;
    }
    if (nobj != c->nobjs)
        return TSDB_ERR_CORRUPT;
    cfile *f = calloc(c->nfiles ? c->nfiles : 1, sizeof *f);
    if (!f)
        return TSDB_ERR_NOMEM;
    size_t n = 0, nec = 0;
    mk_file mf;
    at = 0;
    int rc = 0;
    while (rc == 0 && n < c->nfiles && mk_cold_file_next(c, &at, &mf)) {
        cfile *x = &f[n];
        int64_t k;
        if (mf.name_len >= sizeof x->name) {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        memcpy(x->name, mf.name, mf.name_len);
        x->name[mf.name_len] = '\0';
        x->size = mf.size, x->xxh = mf.xxh64, x->hc = mf.header_check, x->kind = mf.kind;
        bool good = sscanf(x->name, "p%" SCNd64 "_", &k) == 1 && k == c->key &&
                    (mf.kind == MK_FILE_EC ? ec_file_name(x->name) : mf.kind == MK_FILE_Y && part_file_name(x->name));
        nec += mf.kind == MK_FILE_EC;
        if (!good)
            rc = TSDB_ERR_CORRUPT;
        n++;
    }
    if (rc == 0 && (n != c->nfiles || nec != nobj))
        rc = TSDB_ERR_CORRUPT;
    if (rc) {
        fprintf(stderr, "tsdb: %s: the files of the COLD at position %" PRIu64 " (key %" PRId64 ") are not one .ec file "
                        "per object and parts of the key\n", db->dir, pos, c->key);
        free(f);
        return rc;
    }
    *out = f;
    *nout = n;
    return 0;
}

/* step 2 for one file (stage_mu held, rp->mu not): read and checked; an .ec file's record found, Y linked and opened */
static int cold_stage_one(struct eng *db, uint64_t pos, const mk_cold *c, cfile *x, uint8_t *seen)
{
    char sp[64];
    int rc = replica_stage_path(db, x->name, sp, sizeof sp);
    if (rc) {
        fprintf(stderr, "tsdb: %s: file %s of the COLD at position %" PRIu64 " is not staged\n", db->dir, x->name, pos);
        return rc;
    }
    if ((rc = read_file(db->dirfd, sp, &x->buf, &x->len)) != 0)
        return rc;
    if (x->len != x->size || xxh64(x->buf, x->len, 0) != x->xxh) {
        fprintf(stderr, "tsdb: %s: staged file %s does not match the COLD at position %" PRIu64 "\n", db->dir, sp, pos);
        return TSDB_ERR_CORRUPT;
    }
    if (x->kind == MK_FILE_Y) {
        free(x->buf);
        x->buf = NULL;
        uint64_t key;
        if (sscanf(sp, "stage_%16" SCNx64 ".part", &key) != 1)
            return TSDB_ERR_CORRUPT;
        snprintf(x->tmp, sizeof x->tmp, "tmp_0_%" PRIu64 ".part", key);
        unlinkat(db->dirfd, x->tmp, 0);
        if (linkat(db->dirfd, sp, db->dirfd, x->tmp, 0) != 0) {
            x->tmp[0] = '\0';
            return TSDB_ERR_IO;
        }
        if ((rc = part_open_as(db->dirfd, x->tmp, x->name, db->opt.pin_index, db->parts_v8, &x->p)) != 0)
            return rc;
        if ((x->p->index_hash != x->hc || x->p->grows != db->granule_rows || x->p->partition != c->key) &&
            ecapply_mutant != ECAPPLY_MUT_NOYHCHECK) {
            fprintf(stderr, "tsdb: %s: staged file %s does not have the header the COLD at position %" PRIu64 " names\n",
                    db->dir, sp, pos);
            return TSDB_ERR_CORRUPT;
        }
        x->p->verify = db->opt.verify_checksums;
        x->lblock = x->p->min_block;
        return 0;
    }
    /* .ec: the trailer places the record, which must be one of the marker's objects, byte for byte */
    const size_t T = EC_TRAILER_BYTES;
    if (x->len < T + EC_REC_FIXED)
        return TSDB_ERR_CORRUPT;
    const uint8_t *t = x->buf + x->len - T;
    x->roff = le64(t + EC_TR_REC_OFF), x->rlen = le64(t + EC_TR_REC_LEN);
    if (x->rlen > x->len - T || x->roff != x->len - T - x->rlen)
        return TSDB_ERR_CORRUPT;
    if (ec_record_decode_as(x->buf + x->roff, (size_t)x->rlen, &x->rec, true) != 0)
        return TSDB_ERR_CORRUPT;
    x->rec_ok = true;
    size_t at = 0, i = 0;
    mk_cold_obj o;
    while (mk_cold_obj_next(c, &at, &o)) {
        if (memcmp(o.oid, x->rec.oid, MK_OID) == 0 && o.rec_len == x->rlen &&
            memcmp(o.rec, x->buf + x->roff, o.rec_len) == 0 && (!seen[i] || ecapply_mutant == ECAPPLY_MUT_SEENSKIP)) {
            seen[i] = 1;
            x->lblock = x->rec.min_block;
            return 0;
        }
        i++;
    }
    fprintf(stderr, "tsdb: %s: staged file %s is not the .ec file of an object of the COLD at position %" PRIu64 "\n",
            db->dir, sp, pos);
    return TSDB_ERR_CORRUPT;
}

/* step 3 for one .ec file: its record under block number b (publish_lock held), a synced tmp, opened */
static int cold_ec_write(struct eng *db, cfile *x, uint64_t b)
{
    ec_record *r = &x->rec;
    r->min_block = r->max_block = b;
    memset(r->source, 0, sizeof r->source);
    snprintf(r->source, sizeof r->source, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_%u.part", r->key, b, b, r->level);
    snprintf(x->fin, sizeof x->fin, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_%u.ec", r->key, b, b, r->level);
    int rc = ec_record_encode_as(r, x->buf + x->roff, (size_t)x->rlen, true);
    if (rc)
        return rc;
    uint8_t *t = x->buf + x->len - EC_TRAILER_BYTES;
    st64(t + EC_TR_REC_HASH, xxh64(x->buf + x->roff, (size_t)x->rlen, 0));
    st64(t + EC_TR_HASH, xxh64(t, EC_TR_HASH, 0));
    snprintf(x->tmp, sizeof x->tmp, "tmp_%d_%u.ec", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));
    int fd = openat(db->dirfd, x->tmp, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) {
        x->tmp[0] = '\0';
        return TSDB_ERR_IO;
    }
    rc = write_full(fd, x->buf, x->len);
    if (rc == 0 && db->opt.fsync && tsdb_fdatasync(fd) != 0) /* nothing relies on it yet: this call fails */
        rc = TSDB_ERR_IO;
    if (close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    if (rc == 0)
        rc = part_open_ec(db, x->tmp, x->fin, &x->p);
    if (rc) {
        unlinkat(db->dirfd, x->tmp, 0);
        x->tmp[0] = '\0';
        return rc;
    }
    snprintf(x->p->pending, sizeof x->p->pending, "%s", x->tmp);
    return 0;
}

/* the intent v5 (src/ecswap.c): N and Y lines in listing order, then R; written and synced (db->lock held, as a drop's) */
static int cold_intent(struct eng *db, const char *intent, cfile **ord, size_t n, part **own, size_t nown)
{
    size_t cap = 64, len = 0;
    for (size_t i = 0; i < n; i++)
        cap += 8 + strlen(ord[i]->tmp) + strlen(ord[i]->fin);
    for (size_t i = 0; i < nown; i++)
        cap += 16 + 2 * strlen(own[i]->name);
    char *buf = malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    len += (size_t)snprintf(buf + len, cap - len, INTENT_V5_HEAD "%zu\n", n + nown);
    for (size_t i = 0; i < n; i++)
        len += (size_t)snprintf(buf + len, cap - len, "%c %s %s\n", ord[i]->kind == MK_FILE_EC ? 'N' : 'Y',
                                ord[i]->tmp, ord[i]->fin);
    for (size_t i = 0; i < nown; i++)
        len += (size_t)snprintf(buf + len, cap - len, "R %s settled_%s\n", own[i]->name, own[i]->name);
    len += (size_t)snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
    int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, buf, len) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0 && close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    free(buf);
    if (rc)
        unlinkat(db->dirfd, intent, 0);
    return rc;
}

enum { CS_APPLY, CS_DROPPED, CS_ABOVE };

static int cold_apply(struct eng *db, uint64_t pos, const mk_cold *c)
{
    cfile *f;
    size_t n;
    int rc = cold_check(db, pos, c, &f, &n);
    if (rc)
        return rc;
    crash_at(db, ECAPPLY_CRASH_BEFORE_APPLY);
    uint64_t gen = replica_mu_drop(db);
    const uint64_t tft = atomic_load(&ecapply_test_ft);
    rc = replica_flush_through(db, tft ? tft : c->flush_through); /* planted coldnoflushthrough there: the active one */
    uint8_t *seen = calloc(c->nobjs ? c->nobjs : 1, 1);
    if (!seen && rc == 0)
        rc = TSDB_ERR_NOMEM;
    replica_stage_hold(db);
    for (size_t i = 0; i < n && rc == 0; i++)
        rc = cold_stage_one(db, pos, c, &f[i], seen);
    replica_stage_release(db);
    free(seen);
    void (*th)(struct eng *, int) = atomic_load(&ecapply_test_hook);
    if (th && rc == 0)
        th(db, ECAPPLY_STEP_STAGED);
    if (replica_mu_retake(db, gen)) { /* a cut: this marker and its files may be gone */
        cf_free(db, f, n, false);
        return RP_AGAIN;
    }
    cfile **ord = malloc((n ? n : 1) * sizeof *ord);
    if (!ord && rc == 0)
        rc = TSDB_ERR_NOMEM;
    if (rc) {
        free(ord);
        cf_free(db, f, n, false);
        return rc;
    }
    size_t nec = 0;
    for (size_t i = 0; i < n; i++)
        if (f[i].kind == MK_FILE_EC)
            ord[nec++] = &f[i];
    for (size_t i = 0, k = nec; i < n; i++)
        if (f[i].kind == MK_FILE_Y)
            ord[k++] = &f[i];
    if (ecapply_mutant != ECAPPLY_MUT_NOORDER) { /* planted noorder: the marker's order, not the leader's blocks */
        qsort(ord, nec, sizeof *ord, by_lblock);
        qsort(ord + nec, n - nec, sizeof *ord, by_lblock);
    }

    pthread_mutex_lock(&db->publish_lock);
    rc = eng_check(db) ? TSDB_ERR_IO : 0;
    /* fresh numbers: the .ec parts in the leader's order, Y above them (planted ecabovey: the other way round) */
    for (size_t j = 0; j < n && rc == 0; j++) {
        size_t i = ecapply_mutant == ECAPPLY_MUT_ECABOVEY ? (j + (n - nec)) % n : j;
        cfile *x = ord[i];
        uint64_t b = atomic_fetch_add(&db->next_block, 1);
        if (x->kind == MK_FILE_EC) {
            rc = cold_ec_write(db, x, b);
        } else {
            x->p->min_block = x->p->max_block = b;
            snprintf(x->fin, sizeof x->fin, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_%u.part", x->p->partition, b, b,
                     x->p->level);
            snprintf(x->p->name, sizeof x->p->name, "%s", x->fin);
            snprintf(x->p->pending, sizeof x->p->pending, "%s", x->tmp);
        }
    }
    if (rc == 0 && n) { /* the new names' positions, durable before any of them is (src/ppos.h) */
        const char **nm = malloc(n * sizeof *nm);
        if (!nm)
            rc = TSDB_ERR_NOMEM;
        for (size_t i = 0; rc == 0 && i < n; i++)
            nm[i] = ord[i]->fin;
        if (rc == 0 && db->ppos)
            rc = ppos_record(db, nm, n, c->z ? c->z : PPOS_UNKNOWN);
        free(nm);
    }
    if (rc == 0)
        rc = rs_cold_begin(db, pos, c->key);
    if (rc) {
        pthread_mutex_unlock(&db->publish_lock);
        free(ord);
        cf_free(db, f, n, false);
        return rc;
    }

    char intent[64];
    snprintf(intent, sizeof intent, "txn_%d_%u.intent", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));
    part **own = NULL;
    size_t nown = 0, above = 0;
    const char *above_name = NULL;
    int state = CS_APPLY;
    bool renamed = false, written = false;
    lt_sec lsec;
    lt_begin(db, &lsec);
    pthread_mutex_lock(&db->lock);
    rc = eng_check(db) ? TSDB_ERR_IO : 0;
    size_t listed = 0;
    for (size_t i = 0; i < db->nparts; i++)
        listed += db->parts[i]->partition == c->key;
    if (rc == 0 && !listed)
        state = CS_DROPPED; /* this member's retention dropped the key before the hold */
    if (rc == 0 && state == CS_APPLY && !(own = malloc(listed * sizeof *own)))
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && state == CS_APPLY && i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *e = atomic_load(&p->ec);
        if (p->partition != c->key || (e && !e->test_mark))
            continue;
        /* no record (ppos_of then gives 0) is unknown, above every Z: retiring it as <= Z is what this check is for */
        uint64_t pp = PPOS_UNKNOWN;
        if (db->ppos && ppos_of(db, p->name, &pp) != 0 && ecapply_mutant != ECAPPLY_MUT_NOTFOUNDZERO)
            pp = PPOS_UNKNOWN;
        if (pp > c->z && ecapply_mutant != ECAPPLY_MUT_PPOSIGNORED) {
            above++;
            above_name = above_name ? above_name : p->name;
            continue;
        }
        own[nown++] = p;
    }
    if (rc == 0 && state == CS_APPLY && above) {
        fprintf(stderr, "tsdb: %s: the COLD at position %" PRIu64 " of key %" PRId64 " (Z %" PRIu64 ") finds %zu parts "
                        "of the key above Z (%s): nothing is retired, the volume is DIVERGED\n", db->dir, pos, c->key,
                c->z, above, above_name);
        state = CS_ABOVE;
    }
    if (rc == 0 && state == CS_APPLY) {
        rc = cold_intent(db, intent, ord, n, own, nown);
        written = rc == 0;
        if (rc == 0 && ecapply_mutant != ECAPPLY_MUT_V5NODIRSYNC &&
            eng_sync_dir(db, "fsync of the directory after writing a cold apply's intent") != 0)
            rc = TSDB_ERR_IO; /* the engine failed; the intent's name is the commit: the next open decides */
        renamed = rc == 0;
        for (size_t i = 0; renamed && i < n + nown; i++) {
            if (i == nec)
                crash_at(db, ECAPPLY_CRASH_MID_INTENT);
            const char *from, *to;
            char st[160];
            if (i < n) {
                from = ord[i]->tmp, to = ord[i]->fin;
                if (ord[i]->kind == MK_FILE_Y && ecapply_mutant == ECAPPLY_MUT_YCOLD) { /* planted: Y retired */
                    snprintf(st, sizeof st, "settled_%s", ord[i]->fin);
                    to = st;
                }
            } else {
                from = own[i - n]->name;
                snprintf(st, sizeof st, "settled_%s", from);
                to = st;
            }
            if (renameat(db->dirfd, from, db->dirfd, to) != 0) {
                eng_fail(db, "rename of a cold apply's part", errno);
                renamed = false;
                rc = TSDB_ERR_IO;
            }
        }
        if (renamed && nec == n + nown) /* nothing followed the .ec renames: the point is after them all the same */
            crash_at(db, ECAPPLY_CRASH_MID_INTENT);
        if (renamed) {
            for (size_t i = 0; i < nown; i++) {
                part_ref(own[i]);
                own[i]->dropped = ecapply_mutant != ECAPPLY_MUT_MERGEKEPT; /* a merge of it in flight drops its output */
                db_parts_remove_locked(db, own[i]);
            }
            for (size_t i = 0; i < n; i++) {
                part *p = ord[i]->p;
                ord[i]->p = NULL;
                p->pending[0] = '\0';
                if (ord[i]->kind == MK_FILE_Y && ecapply_mutant == ECAPPLY_MUT_YCOLD)
                    part_unref(p);
                else
                    db_parts_insert_locked(db, p); /* the open's reference goes to the list */
                ord[i]->tmp[0] = '\0';
            }
            pthread_cond_signal(&db->merge_cv);
        } else {
            nown = 0;
        }
    } else {
        nown = 0;
    }
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &lsec);
    if (renamed) {
        if ((rc = eng_sync_dir(db, "fsync of the directory after a cold apply's renames")) == 0 &&
            unlinkat(db->dirfd, intent, 0) != 0)
            fprintf(stderr, "tsdb: unlink of cold apply intent %s: %s (the next open rolls it forward)\n", intent,
                    strerror(errno));
    }
    pthread_mutex_unlock(&db->publish_lock);
    free(ord);
    /* refused before an intent: the temporaries go; renamed: the listed parts were taken out of f; an intent and no
     * renames (the engine failed): everything stays for the next open */
    cf_free(db, f, n, written && !renamed);
    if (renamed && nown) {
        eckeep_retired(db, own, nown, c->key, c->z); /* held until the MK_DIGEST of (key, Z) matched (r8j) */
        ecdigest_cold_bind(db, c->key, c->z, own, nown); /* durable before RSTATE's done: a replay rebinds */
    }
    for (size_t i = 0; i < nown; i++)
        part_unref(own[i]);
    free(own);
    if (rc)
        return rc;
    if (state == CS_ABOVE) {
        ec_diverge(db, c->key, NULL, EC_DIV_PPOS); /* durable before the flag; no engine lock held */
        return TSDB_ERR_CORRUPT;
    }
    if (renamed)
        (void)manifest_publish(db); /* as db_add_parts: a failure failed the engine, which the next call sees */
    return 0;
}
#endif

int ecapply_cold(struct eng *db, uint64_t pos, const mk *m)
{
#ifdef TSDB_EC
    if (!db->rp)
        return TSDB_ERR_INVALID;
    return cold_apply(db, pos, &m->u.cold);
#else
    (void)m;
    fprintf(stderr, "tsdb: %s: the COLD at position %" PRIu64 " needs the erasure codec, which this build has not\n",
            db->dir, pos);
    return TSDB_ERR_INVALID;
#endif
}

int ecapply_mutant;

int ecapply_mutant_parse(const char *name)
{
    static const char *const names[] = {"none", "ycold", "ecabovey", "pposignored", "v5nodirsync", "coldnoflushthrough",
                                        "leaderswapnors", "pposafterswap", "mergekept", "notfoundzero", "noorder",
                                        "checkz", "seenskip", "noyhcheck", "decidesome", "recrollmissing",
                                        "recnodiverge", "recnodirsync"};
    for (int i = 0; i < (int)(sizeof names / sizeof names[0]); i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}

/* Whether the COLD in `doing` was carried out: its swap takes no block numbers a replay could look for (a leader's keeps
 * its sources' names, D2; a replica's numbers are fresh and unknown to the log), so the answer is what is listed: an
 * .ec part of key `key` whose record names the oid. Every oid listed: done; none: not; some: the intent that lists them
 * all or none (v4 or v5) was not honoured, which is damage. A COLD with no objects is not (nothing to look for; its
 * apply lists nothing either). An oid listed twice is damage too: the apply that this decision guards would have
 * listed an object twice. */
int ecapply_cold_decide(struct eng *db, int64_t key, const uint8_t *oids, uint32_t n)
{
    if (n == 0)
        return 0;
    uint32_t *seen = calloc(n, sizeof *seen);
    if (!seen)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        const part *p = db->parts[i];
        const struct ecpart *e = atomic_load(&((part *)p)->ec);
        if (!e || e->test_mark || p->partition != key)
            continue;
        for (uint32_t k = 0; k < n; k++)
            if (memcmp(e->rec.oid, oids + (size_t)k * MK_OID, MK_OID) == 0)
                seen[k]++;
    }
    pthread_mutex_unlock(&db->lock);
    uint32_t listed = 0, twice = 0;
    for (uint32_t k = 0; k < n; k++)
        listed += seen[k] > 0, twice += seen[k] > 1;
    free(seen);
    if (twice || (listed && listed < n && ecapply_mutant != ECAPPLY_MUT_DECIDESOME)) {
        fprintf(stderr, "tsdb: %s: of the %u objects of a COLD of key %" PRId64 ", %u are listed (%u more than once): "
                        "its swap was all or nothing\n", db->dir, n, key, listed, twice);
        return TSDB_ERR_CORRUPT;
    }
    return listed == n ? 1 : 0;
}

/* The .ec parts' PPOS records (src/ppos.h, design-cluster-data.md 2.3 "Part position"), each at its object record's
 * ppos (the settle's cut Z; 0, as in no cluster record, is recorded as unknown), one append and fdatasync per run of
 * equal positions. Not stub behaviour: r8h keeps it. They go out before the intent, so every state a power cut can
 * leave holds at the next open: no intent (the .ec never named; its record is dead and the compaction drops it), an
 * intent rolled back (the sources keep their own records), rolled forward or standing (the .ec names have theirs).
 * A failed write or sync failed the engine (ppos.c); nothing of the swap is visible. Nothing on other volumes. */
static int swap_ppos(struct eng *db, struct part **ec, size_t n)
{
    if (!db->ppos)
        return 0;
    const char **nm = malloc(n * sizeof *nm);
    if (!nm)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (size_t i = 0, j; rc == 0 && i < n; i = j) {
        struct ecpart *e = atomic_load(&ec[i]->ec);
        uint64_t z = e ? e->rec.ppos : 0;
        for (j = i; j < n; j++) {
            struct ecpart *f = atomic_load(&ec[j]->ec);
            if ((f ? f->rec.ppos : 0) != z)
                break;
            nm[j - i] = ec[j]->name;
        }
        rc = ppos_record(db, nm, j - i, z ? z : PPOS_UNKNOWN);
    }
    free(nm);
    return rc;
}

/* A leader's swap of a settle (r8g's eccold_settle -> ec_swap_commit; publish_lock held): the COLD this engine is
 * carrying out, if any (eccold_swap_cur: between its marker and the end of its swap). Its objects are named by a
 * committed marker (`logged`): the replicas list them whatever this swap does. Before the intent's commit point: the
 * .ec parts' PPOS records, then RSTATE's `doing` (jr_cold_precommit), so a leader that loses power anywhere after it
 * and reopens as a replica asks ecapply_cold_decide what is listed instead of applying the COLD a second time (its swap
 * took no block number that rs_committed could find). A failed save failed the engine; the swap refuses (logged). */
int ecapply_swap_pre(struct eng *db, struct part **in, struct part **ec, size_t n, bool *logged)
{
    (void)in;
    *logged = false;
    uint64_t pos = 0;
    int64_t key = 0;
    if (eccold_swap_cur(db, &pos, &key, NULL) && pos)
        *logged = true;
    int rc = 0;
    if (ppos_mutant != PPOS_MUT_SWAPNONE && ppos_mutant != PPOS_MUT_SWAPAFTER &&
        ecapply_mutant != ECAPPLY_MUT_PPOSAFTERSWAP)
        rc = swap_ppos(db, ec, n);
    if (rc == 0 && pos && ecapply_mutant != ECAPPLY_MUT_LEADERSWAPNORS)
        rc = jr_cold_precommit(db, pos, key);
    return rc;
}

/* After the swap's renames and their directory sync (publish_lock held): RSTATE's `done` (a failed save fails the
 * engine: reopened as a replica it would find `doing` and decide by what is listed, which is still right). */
void ecapply_swap_post(struct eng *db, struct part **in, struct part **ec, size_t n)
{
    (void)in;
    if (ppos_mutant == PPOS_MUT_SWAPAFTER || ecapply_mutant == ECAPPLY_MUT_PPOSAFTERSWAP) /* planted: records late */
        swap_ppos(db, ec, n);
    uint64_t pos = 0;
    if (eccold_swap_cur(db, &pos, NULL, NULL) && pos && ecapply_mutant != ECAPPLY_MUT_LEADERSWAPNORS)
        (void)jr_cold_commit(db, pos);
}
