/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster EC garbage collection via committed MK_GC (src/ecgcmk.h; design-cluster-data.md E14, §5.7, D13).
 *
 * GCMK on-disk format: append-only file of 48-byte offset-bound checksummed records (like PPOS/CLASSES):
 *   [0..8)   u64 pos      log position of the MK_GC marker (> 0)
 *   [8..12)  u32 op       GCMK_OP_ADD (1) or GCMK_OP_DONE (2)
 *   [12..16) u32 0        reserved
 *   [16..32) u8  oid[16]  object id
 *   [32..40) u64 0        reserved
 *   [40..48) u64 check    xxh64(rec[0..40), off ^ 0x47434d4bULL)
 * A torn tail at EOF is truncated on open; a bad record before a valid one fails the open (TSDB_ERR_CORRUPT).
 * Compacts atomically via GCMK.tmp + fdatasync + rename + directory fsync when empty or past 64 KiB.
 *
 * Lock order: apply_mu > rp->mu / leader GC worker > g->fmu > g->mu (leaf); latest_rw > db->lock > ecgc_tier.mu.
 */
#define _GNU_SOURCE
#include "internal.h"
#include "chunk.h"
#include "coldops.h"
#include "eccache.h"
#include "eccold.h"
#include "ecgc.h"
#include "ecgcmk.h"
#include "ecpart.h"
#include "marker.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define GCMK_FILE "GCMK"
#define GCMK_TMP "GCMK.tmp"
#define GCMK_REC 48u
#define GCMK_COMPACT_BYTES (64u << 10)
#define GCMK_ORPHAN_GRACE_DEFAULT_MS 60000LL

enum { GCMK_OP_ADD = 1, GCMK_OP_DONE = 2 };

/* Test hooks configured by TSDB_ECGC_TEST (parsed via ecgc_env_init). */
static char test_crash_before_del[512];
static char test_pause_beforedel[512];
static atomic_bool test_crash_fired;
static atomic_bool test_pause_fired;

void ecgcmk_test_opt(const char *tok)
{
    if (strncmp(tok, "crash_before_del=", 17) == 0)
        snprintf(test_crash_before_del, sizeof test_crash_before_del, "%s", tok + 17);
    else if (strncmp(tok, "pause_beforedel=", 16) == 0)
        snprintf(test_pause_beforedel, sizeof test_pause_beforedel, "%s", tok + 16);
}

static int64_t orphan_grace_ms(void)
{
    ecgc_env_init();
    int64_t ov = atomic_load(&ecgc_test_orphan_grace_ms);
    return ov >= 0 ? ov : GCMK_ORPHAN_GRACE_DEFAULT_MS;
}

typedef struct gcmk_ent {
    uint8_t oid[16];
    uint64_t pos; /* MK_GC marker log position */
} gcmk_ent;

typedef struct gcmk_cand {
    uint8_t oid[16];
    int64_t first_seen_ms;
} gcmk_cand;

struct ecgcmk {
    pthread_mutex_t wmu; /* outermost: serializes ecgcmk_work on this engine */
    pthread_mutex_t fmu; /* serializes GCMK file appends and compaction */
    pthread_mutex_t mu;  /* leaf: protects ents and qorph */
    int fd;              /* -1 until first write or open */
    uint64_t off;        /* valid byte length of GCMK */
    bool broken;
    gcmk_ent *ents;
    size_t nents, capents;
    uint8_t (*qorph)[16]; /* explicitly aborted-settle oids (never directly deleted) */
    size_t nqorph, capqorph;
    /* Owned solely by ecgcmk_work (one GC worker runs on db at a time): */
    gcmk_cand *cands;
    size_t ncands, capcands;
    int64_t last_sweep_ms;
    atomic_bool fenced; /* a CHUNK_DEL returned CHUNK_ERR_FENCED (or INDETERMINATE): stop GC */
    atomic_bool stop;
};

/* ------------------------------------------------------------ GCMK record codec */

static void put32(uint8_t *b, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}

static void put64(uint8_t *b, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}

static uint32_t get32(const uint8_t *b)
{
    return (uint32_t)b[0] | (uint32_t)b[1] << 8 | (uint32_t)b[2] << 16 | (uint32_t)b[3] << 24;
}

static uint64_t get64(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | b[i];
    return v;
}

static uint64_t rec_check(const uint8_t rec[GCMK_REC], uint64_t off)
{
    return xxh64(rec, 40, off ^ 0x47434d4bULL);
}

static void rec_encode(uint8_t rec[GCMK_REC], uint64_t pos, uint32_t op, const uint8_t oid[16], uint64_t off)
{
    memset(rec, 0, GCMK_REC);
    put64(rec + 0, pos);
    put32(rec + 8, op);
    memcpy(rec + 16, oid, 16);
    put64(rec + 40, rec_check(rec, off));
}

static bool rec_decode(const uint8_t rec[GCMK_REC], uint64_t off, uint64_t *pos, uint32_t *op, uint8_t oid[16])
{
    if (get64(rec + 40) != rec_check(rec, off))
        return false;
    uint64_t p = get64(rec + 0);
    uint32_t o = get32(rec + 8);
    if (p == 0 || (o != GCMK_OP_ADD && o != GCMK_OP_DONE) || get32(rec + 12) != 0 || get64(rec + 32) != 0)
        return false;
    *pos = p;
    *op = o;
    memcpy(oid, rec + 16, 16);
    return true;
}

static int pwrite_all(int fd, const uint8_t *buf, size_t len, uint64_t off)
{
    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(fd, buf + done, len - done, (off_t)(off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return w < 0 ? errno : ENOSPC;
        done += (size_t)w;
    }
    return 0;
}

/* ------------------------------------------------------------ in-memory pending set (under g->mu) */

static ssize_t ent_find_locked(const struct ecgcmk *g, const uint8_t oid[16])
{
    for (size_t i = 0; i < g->nents; i++)
        if (memcmp(g->ents[i].oid, oid, 16) == 0)
            return (ssize_t)i;
    return -1;
}

static int ent_add_locked(struct ecgcmk *g, const uint8_t oid[16], uint64_t pos)
{
    ssize_t idx = ent_find_locked(g, oid);
    if (idx >= 0) {
        if (pos > g->ents[idx].pos)
            g->ents[idx].pos = pos;
        return 0;
    }
    if (g->nents == g->capents) {
        size_t cap = g->capents ? g->capents * 2 : 16;
        gcmk_ent *e = realloc(g->ents, cap * sizeof *e);
        if (!e)
            return TSDB_ERR_NOMEM;
        g->ents = e;
        g->capents = cap;
    }
    memcpy(g->ents[g->nents].oid, oid, 16);
    g->ents[g->nents].pos = pos;
    g->nents++;
    return 0;
}

static bool ent_del_locked(struct ecgcmk *g, const uint8_t oid[16])
{
    ssize_t idx = ent_find_locked(g, oid);
    if (idx < 0)
        return false;
    g->ents[idx] = g->ents[--g->nents];
    return true;
}

/* ------------------------------------------------------------ durable file ops (under g->fmu) */

static int gcmk_compact_locked(eng *db, struct ecgcmk *g)
{
    if (g->broken || eng_check(db))
        return TSDB_ERR_IO;
    pthread_mutex_lock(&g->mu);
    size_t n = g->nents;
    gcmk_ent *snap = n ? malloc(n * sizeof *snap) : NULL;
    if (n && !snap) {
        pthread_mutex_unlock(&g->mu);
        return TSDB_ERR_NOMEM;
    }
    if (n)
        memcpy(snap, g->ents, n * sizeof *snap);
    pthread_mutex_unlock(&g->mu);

    uint8_t *buf = n ? malloc(n * GCMK_REC) : NULL;
    if (n && !buf) {
        free(snap);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < n; i++)
        rec_encode(buf + i * GCMK_REC, snap[i].pos, GCMK_OP_ADD, snap[i].oid, (uint64_t)i * GCMK_REC);
    free(snap);

    int fd = openat(db->dirfd, GCMK_TMP, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int err = 0;
    if (fd < 0) {
        err = errno;
    } else {
        if (n)
            err = pwrite_all(fd, buf, n * GCMK_REC, 0);
        if (err == 0 && db->opt.fsync && tsdb_fdatasync(fd) != 0)
            err = errno;
        if (close(fd) != 0 && err == 0)
            err = errno;
    }
    free(buf);
    if (err == 0 && renameat(db->dirfd, GCMK_TMP, db->dirfd, GCMK_FILE) != 0)
        err = errno;
    if (err != 0) {
        unlinkat(db->dirfd, GCMK_TMP, 0);
        g->broken = true;
        eng_fail(db, "compaction of GCMK", err);
        return TSDB_ERR_IO;
    }
    if (db->opt.fsync) {
        int rc = eng_sync_dir(db, "fsync of the directory after compacting GCMK");
        if (rc) {
            g->broken = true;
            return rc;
        }
    }
    if (g->fd >= 0) {
        close(g->fd);
        g->fd = -1;
    }
    g->off = (uint64_t)n * GCMK_REC;
    return 0;
}

static int gcmk_append_locked(eng *db, struct ecgcmk *g, uint64_t pos, uint32_t op, const uint8_t (*oids)[16],
                              size_t noids)
{
    if (noids == 0)
        return 0;
    if (g->broken || eng_check(db))
        return TSDB_ERR_IO;
    if (g->fd < 0) {
        bool existed = faccessat(db->dirfd, GCMK_FILE, F_OK, 0) == 0;
        g->fd = openat(db->dirfd, GCMK_FILE, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (g->fd < 0) {
            int err = errno;
            g->broken = true;
            eng_fail(db, "open of GCMK", err);
            return TSDB_ERR_IO;
        }
        if (!existed && db->opt.fsync) {
            int rc = eng_sync_dir(db, "fsync of the directory after creating GCMK");
            if (rc) {
                g->broken = true;
                return rc;
            }
        }
    }
    uint8_t *buf = malloc(noids * GCMK_REC);
    if (!buf)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < noids; i++)
        rec_encode(buf + i * GCMK_REC, pos, op, oids[i], g->off + (uint64_t)i * GCMK_REC);
    int err = pwrite_all(g->fd, buf, noids * GCMK_REC, g->off);
    free(buf);
    if (err == 0 && db->opt.fsync && tsdb_fdatasync(g->fd) != 0)
        err = errno;
    if (err != 0) {
        g->broken = true;
        eng_fail(db, "write of GCMK", err);
        return TSDB_ERR_IO;
    }
    g->off += (uint64_t)noids * GCMK_REC;
    return 0;
}

/* ------------------------------------------------------------ open / close */

int ecgcmk_open(eng *db)
{
    ecgc_env_init();
    if (db->opt.read_only)
        return 0;
    struct ecgcmk *g = calloc(1, sizeof *g);
    if (!g)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&g->wmu, NULL);
    pthread_mutex_init(&g->fmu, NULL);
    pthread_mutex_init(&g->mu, NULL);
    g->fd = -1;
    db->gcmk = g;

    unlinkat(db->dirfd, GCMK_TMP, 0);
    int fd = openat(db->dirfd, GCMK_FILE, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT)
            return 0;
        fprintf(stderr, "tsdb: %s/GCMK: %s: the open fails\n", db->dir, strerror(errno));
        return TSDB_ERR_IO;
    }
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0) {
        int err = errno;
        close(fd);
        fprintf(stderr, "tsdb: %s/GCMK: fstat failed: %s\n", db->dir, strerror(err));
        return TSDB_ERR_IO;
    }
    size_t sz = (size_t)st.st_size;
    uint8_t *buf = sz ? malloc(sz) : NULL;
    if (sz && !buf) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    size_t got = 0;
    while (got < sz) {
        ssize_t r = pread(fd, buf + got, sz - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            free(buf);
            close(fd);
            return TSDB_ERR_IO;
        }
        got += (size_t)r;
    }

    size_t nrec = sz / GCMK_REC;
    size_t good_end = 0;
    bool saw_bad = false;
    int rc = 0;
    for (size_t i = 0; i < nrec; i++) {
        uint64_t off = (uint64_t)i * GCMK_REC;
        uint64_t pos = 0;
        uint32_t op = 0;
        uint8_t oid[16];
        if (!rec_decode(buf + off, off, &pos, &op, oid)) {
            saw_bad = true;
            continue;
        }
        if (saw_bad) {
            fprintf(stderr, "tsdb: %s/GCMK: damaged record before valid record at offset %" PRIu64 ": corrupt\n",
                    db->dir, off);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (op == GCMK_OP_ADD)
            rc = ent_add_locked(g, oid, pos);
        else
            (void)ent_del_locked(g, oid);
        if (rc)
            break;
        good_end = (size_t)(off + GCMK_REC);
    }
    free(buf);
    if (rc) {
        close(fd);
        return rc;
    }
    if (good_end < sz) {
        if (ftruncate(fd, (off_t)good_end) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0)) {
            close(fd);
            return TSDB_ERR_IO;
        }
    }
    g->fd = fd;
    g->off = (uint64_t)good_end;
    if (g->nents == 0 && g->off > 0)
        rc = gcmk_compact_locked(db, g);
    return rc;
}

void ecgcmk_stop(eng *db)
{
    if (db && db->gcmk)
        atomic_store(&db->gcmk->stop, true);
}

void ecgcmk_close(eng *db)
{
    struct ecgcmk *g = db ? db->gcmk : NULL;
    if (!g)
        return;
    if (g->fd >= 0)
        close(g->fd);
    free(g->ents);
    free(g->qorph);
    free(g->cands);
    pthread_mutex_destroy(&g->mu);
    pthread_mutex_destroy(&g->fmu);
    pthread_mutex_destroy(&g->wmu);
    free(g);
    db->gcmk = NULL;
}

size_t ecgcmk_pending_count(const eng *db)
{
    struct ecgcmk *g = db ? db->gcmk : NULL;
    if (!g)
        return 0;
    pthread_mutex_lock(&g->mu);
    size_t n = g->nents;
    pthread_mutex_unlock(&g->mu);
    return n;
}

bool ecgcmk_pending_has(const eng *db, const uint8_t oid[16])
{
    struct ecgcmk *g = db ? db->gcmk : NULL;
    if (!g)
        return false;
    pthread_mutex_lock(&g->mu);
    bool has = ent_find_locked(g, oid) >= 0;
    pthread_mutex_unlock(&g->mu);
    return has;
}

bool ecgcmk_is_fenced(const eng *db) { return db && db->gcmk && atomic_load(&db->gcmk->fenced); }

int ecgcmk_queue_orphan(eng *db, const uint8_t oid[16])
{
    struct ecgcmk *g = db ? db->gcmk : NULL;
    if (!g)
        return 0;
    pthread_mutex_lock(&g->mu);
    for (size_t i = 0; i < g->nqorph; i++) {
        if (memcmp(g->qorph[i], oid, 16) == 0) {
            pthread_mutex_unlock(&g->mu);
            return 0;
        }
    }
    if (g->nqorph == g->capqorph) {
        size_t cap = g->capqorph ? g->capqorph * 2 : 16;
        uint8_t (*q)[16] = realloc(g->qorph, cap * 16);
        if (!q) {
            pthread_mutex_unlock(&g->mu);
            return TSDB_ERR_NOMEM;
        }
        g->qorph = q;
        g->capqorph = cap;
    }
    memcpy(g->qorph[g->nqorph++], oid, 16);
    pthread_mutex_unlock(&g->mu);
    return 0;
}

/* ------------------------------------------------------------ apply MK_GC on every member */

int ecgcmk_apply(eng *db, uint64_t pos, const mk *m)
{
    ecgc_env_init();
    if (!m || m->type != MK_GC)
        return TSDB_ERR_INVALID;
    if (db->rp && ecgc_mutant == GC_MUT_REPLICANOOP)
        return 0; /* planted bug: replica ignores MK_GC */
    struct ecgcmk *g = db->gcmk;
    if (!g)
        return 0;
    const mk_gc *gc = &m->u.gc;
    if (gc->noids == 0)
        return 0;

    if (gc->reason == MK_GC_DONE) {
        pthread_mutex_lock(&g->fmu);
        uint8_t (*del)[16] = malloc((size_t)gc->noids * 16);
        if (!del) {
            pthread_mutex_unlock(&g->fmu);
            return TSDB_ERR_NOMEM;
        }
        size_t nd = 0;
        pthread_mutex_lock(&g->mu);
        for (uint32_t i = 0; i < gc->noids; i++) {
            const uint8_t *o = mk_oid_at(gc->oids, i);
            if (ent_find_locked(g, o) >= 0) {
                bool dup = false;
                for (size_t j = 0; j < nd && !dup; j++)
                    dup = memcmp(del[j], o, 16) == 0;
                if (!dup)
                    memcpy(del[nd++], o, 16);
            }
        }
        pthread_mutex_unlock(&g->mu);
        int rc = gcmk_append_locked(db, g, pos, GCMK_OP_DONE, del, nd);
        if (rc == 0) {
            pthread_mutex_lock(&g->mu);
            for (size_t i = 0; i < nd; i++)
                (void)ent_del_locked(g, del[i]);
            size_t left = g->nents;
            pthread_mutex_unlock(&g->mu);
            if (g->off > 0 && (left == 0 || g->off >= GCMK_COMPACT_BYTES))
                rc = gcmk_compact_locked(db, g);
        }
        free(del);
        pthread_mutex_unlock(&g->fmu);
        if (rc == 0) {
            /* Also clean up any lingering retired_*.ec (e.g. ex-leader crashed after MK_GC_RETIRED committed before
             * ecgcmk_apply ran and later reopened as a follower past RSTATE.kept). */
            char (*unlinks)[256] = NULL;
            size_t nunlinks = 0;
            bool retired_listed = false;
            lt_sec lsec;
            lt_begin(db, &lsec);
            pthread_mutex_lock(&db->lock);
            rc = ecgc_collect_oids_locked(db, gc->oids, gc->noids, &unlinks, &nunlinks, &retired_listed);
            pthread_mutex_unlock(&db->lock);
            lt_end(db, &lsec);
            uint64_t unlinked = 0;
            for (size_t i = 0; rc == 0 && i < nunlinks; i++) {
                if (unlinkat(db->dirfd, unlinks[i], 0) == 0) {
                    unlinked++;
                } else if (errno != ENOENT) {
                    int err = errno;
                    eng_fail(db, "unlink of retired .ec part by MK_GC{done}", err);
                    rc = TSDB_ERR_IO;
                }
            }
            free(unlinks);
            if (rc == 0 && (unlinked > 0 || retired_listed)) {
                if (db->opt.fsync)
                    rc = eng_sync_dir(db, "fsync of the directory after MK_GC{done} unlinked retired parts");
                if (unlinked > 0)
                    ecgc_note_unlinked(db->tier, unlinked);
            }
            if (rc == 0 && retired_listed)
                (void)manifest_publish(db);
        }
        return rc;
    }

    if (gc->reason != MK_GC_RETIRED && gc->reason != MK_GC_ORPHAN)
        return TSDB_ERR_INVALID;

    /* 1. Retire any still-listed .ec parts of these oids (a replica whose TTL is behind) and collect all retired_
     *    names to unlink after GCMK is durable. */
    char (*unlinks)[256] = NULL;
    size_t nunlinks = 0;
    bool retired_listed = false;
    lt_sec lsec;
    lt_begin(db, &lsec);
    pthread_mutex_lock(&db->lock);
    int rc = ecgc_collect_oids_locked(db, gc->oids, gc->noids, &unlinks, &nunlinks, &retired_listed);
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &lsec);
    if (rc)
        return rc;
    if (retired_listed)
        fprintf(stderr, "tsdb: %s: MK_GC at position %" PRIu64 " retired still-listed .ec part(s)\n", db->dir, pos);

    /* 2. Record oids in the durable pending-delete file GCMK before unlinking retired_ names. */
    pthread_mutex_lock(&g->fmu);
    uint8_t (*add)[16] = malloc((size_t)gc->noids * 16);
    if (!add) {
        pthread_mutex_unlock(&g->fmu);
        free(unlinks);
        return TSDB_ERR_NOMEM;
    }
    size_t na = 0;
    pthread_mutex_lock(&g->mu);
    for (uint32_t i = 0; i < gc->noids; i++) {
        const uint8_t *o = mk_oid_at(gc->oids, i);
        if (ent_find_locked(g, o) < 0) {
            bool dup = false;
            for (size_t j = 0; j < na && !dup; j++)
                dup = memcmp(add[j], o, 16) == 0;
            if (!dup)
                memcpy(add[na++], o, 16);
        }
    }
    pthread_mutex_unlock(&g->mu);
    rc = gcmk_append_locked(db, g, pos, GCMK_OP_ADD, add, na);
    if (rc == 0) {
        pthread_mutex_lock(&g->mu);
        for (size_t i = 0; i < na && rc == 0; i++)
            rc = ent_add_locked(g, add[i], pos);
        /* Also remove from qorph if queued there */
        for (size_t i = 0; i < g->nqorph;) {
            bool hit = false;
            for (uint32_t j = 0; j < gc->noids && !hit; j++)
                hit = memcmp(g->qorph[i], mk_oid_at(gc->oids, j), 16) == 0;
            if (hit)
                memcpy(g->qorph[i], g->qorph[--g->nqorph], 16);
            else
                i++;
        }
        pthread_mutex_unlock(&g->mu);
    }
    free(add);
    pthread_mutex_unlock(&g->fmu);

    /* 3. Unlink retired_ names and sync the directory. */
    uint64_t unlinked = 0;
    for (size_t i = 0; rc == 0 && i < nunlinks; i++) {
        if (unlinkat(db->dirfd, unlinks[i], 0) == 0) {
            unlinked++;
        } else if (errno != ENOENT) {
            int err = errno;
            eng_fail(db, "unlink of retired .ec part by MK_GC", err);
            rc = TSDB_ERR_IO;
        }
    }
    free(unlinks);
    if (rc == 0 && (unlinked > 0 || retired_listed)) {
        if (db->opt.fsync)
            rc = eng_sync_dir(db, "fsync of the directory after MK_GC unlinked retired parts");
        if (unlinked > 0)
            ecgc_note_unlinked(db->tier, unlinked);
    }
    if (rc == 0 && retired_listed)
        (void)manifest_publish(db);
    return rc;
}

/* ------------------------------------------------------------ WAL scan for E14 condition 2 */

typedef struct oid_vec {
    uint8_t (*v)[16];
    size_t n, cap;
    bool nomem;
} oid_vec;

static int ov_push(oid_vec *ov, const uint8_t oid[16])
{
    if (ov->n == ov->cap) {
        size_t cap = ov->cap ? ov->cap * 2 : 64;
        uint8_t (*nv)[16] = realloc(ov->v, cap * 16);
        if (!nv) {
            ov->nomem = true;
            return TSDB_ERR_NOMEM;
        }
        ov->v = nv;
        ov->cap = cap;
    }
    memcpy(ov->v[ov->n++], oid, 16);
    return 0;
}

static int oid_cmp(const void *a, const void *b) { return memcmp(a, b, 16); }

static int scan_wal_seg(eng *db, uint64_t seq, oid_vec *ov)
{
    char name[64];
    wal_name(name, sizeof name, seq);
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO; /* ENOENT: flushed & unlinked -> parts hold its effects */
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t flen = (size_t)st.st_size;
    size_t win = 256u << 10;
    uint8_t *buf = NULL;
    size_t pos0 = 0;
    int rc = 0;
    while (rc == 0 && pos0 < flen) {
        size_t want = flen - pos0 < win ? flen - pos0 : win;
        uint8_t *nb = realloc(buf, want);
        if (!nb) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        buf = nb;
        size_t got = 0;
        while (got < want) {
            ssize_t r = pread(fd, buf + got, want - got, (off_t)(pos0 + got));
            if (r < 0 && errno == EINTR)
                continue;
            if (r <= 0) {
                rc = TSDB_ERR_IO;
                break;
            }
            got += (size_t)r;
        }
        if (rc)
            break;
        bool more = pos0 + got < flen;
        size_t pos = 0;
        while (pos < got) {
            wal_rec wr;
            size_t sz = wal_record(buf + pos, got - pos, seq, pos0 + pos, &wr);
            if (sz == 0) {
                if (more)
                    break;
                pos = got; /* partial tail being written to active segment */
                break;
            }
            if (wr.kind == WAL_KIND_MARKER && (wr.mtype == MK_COLD || wr.mtype == MK_DIGEST)) {
                mk m;
                if (mk_decode(wr.payload, wr.n, &m) != 0) {
                    rc = TSDB_ERR_CORRUPT;
                    break;
                }
                if (m.type == MK_COLD) {
                    size_t at = 0;
                    mk_cold_obj o;
                    while (mk_cold_obj_next(&m.u.cold, &at, &o) && rc == 0)
                        rc = ov_push(ov, o.oid);
                } else if (m.type == MK_DIGEST) {
                    for (uint32_t i = 0; i < m.u.digest.noids && rc == 0; i++)
                        rc = ov_push(ov, mk_oid_at(m.u.digest.oids, i));
                }
            }
            pos += sz;
        }
        if (rc)
            break;
        if (pos == 0 && more)
            win *= 2; /* single record larger than window */
        pos0 += pos;
    }
    free(buf);
    close(fd);
    return rc;
}

static int u64_cmp(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/* Collects every oid named by any MK_COLD or MK_DIGEST record in any WAL segment on disk (committed or not). */
static int scan_wal_oids(eng *db, oid_vec *ov)
{
    int dfd = dup(db->dirfd);
    if (dfd < 0)
        return TSDB_ERR_IO;
    DIR *d = fdopendir(dfd);
    if (!d) {
        close(dfd);
        return TSDB_ERR_IO;
    }
    rewinddir(d);
    uint64_t *seqs = NULL;
    size_t n = 0, cap = 0;
    struct dirent *e;
    int rc = 0;
    for (errno = 0; (e = readdir(d)); errno = 0) {
        uint64_t sq = 0;
        int consumed = 0;
        if (!wal_file_name(e->d_name) || sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &sq, &consumed) != 1 ||
            e->d_name[consumed])
            continue;
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 16;
            uint64_t *ns = realloc(seqs, nc * sizeof *ns);
            if (!ns) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            seqs = ns;
            cap = nc;
        }
        seqs[n++] = sq;
    }
    if (rc == 0 && errno != 0)
        rc = TSDB_ERR_IO;
    closedir(d);
    if (rc == 0 && n > 1)
        qsort(seqs, n, sizeof *seqs, u64_cmp);
    for (size_t i = 0; rc == 0 && i < n; i++)
        rc = scan_wal_seg(db, seqs[i], ov);
    free(seqs);
    return rc;
}

/* ------------------------------------------------------------ leader GC execution */

static int append_and_apply_mkgc(eng *db, uint32_t reason, const uint8_t (*oids)[16], size_t noids)
{
    if (noids == 0 || noids > MK_FILES_MAX)
        return 0;
    mk m;
    memset(&m, 0, sizeof m);
    m.type = MK_GC;
    m.span = 1;
    m.u.gc.reason = reason;
    m.u.gc.noids = (uint32_t)noids;
    m.u.gc.oids = (const uint8_t *)oids;
    const char *what = reason == MK_GC_DONE ? "an MK_GC{done} marker was appended but its commit point did not come"
                                            : "an MK_GC marker was appended but its commit point did not come";
    uint64_t pos = 0;
    int rc = jr_markers_append(db, &m, 1, what, &pos);
    if (rc != 0)
        return rc;
    if (pos == 0)
        return TSDB_ERR_UNAVAILABLE;
    if (reason == MK_GC_DONE || ecgc_mutant != GC_MUT_UNCOMMITTED) {
        rc = jr_markers_wait(db, pos, what);
        if (rc != 0) {
            if (rc == TSDB_ERR_INDETERMINATE && db->gcmk)
                atomic_store(&db->gcmk->fenced, true);
            return rc;
        }
    }
    return ecgcmk_apply(db, pos, &m);
}

static void maybe_crash_before_del(eng *db)
{
    if (test_crash_before_del[0] && access(test_crash_before_del, F_OK) == 0 &&
        !atomic_exchange(&test_crash_fired, true)) {
        fprintf(stderr, "tsdb: %s: test: ecgc: exiting before CHUNK_DEL (crash_before_del)\n", db->dir);
        _exit(99);
    }
}

static bool maybe_pause_before_del(eng *db)
{
    if (test_pause_beforedel[0] && access(test_pause_beforedel, F_OK) == 0 &&
        !atomic_exchange(&test_pause_fired, true)) {
        fprintf(stderr, "tsdb: %s: test: ecgc: paused before CHUNK_DEL (pause_beforedel)\n", db->dir);
        while (access(test_pause_beforedel, F_OK) == 0 && !atomic_load(&db->gcmk->stop)) {
            struct timespec ts = {0, 50 * 1000000L};
            nanosleep(&ts, NULL);
        }
        fprintf(stderr, "tsdb: %s: test: ecgc: resumed before CHUNK_DEL\n", db->dir);
        return true;
    }
    return false;
}

static int delete_cluster_object(eng *db, coldvol *cv, const uint8_t oid[16], uint64_t mk_pos)
{
    struct ecgcmk *g = db->gcmk;
    if (!eccold_leader(db) || atomic_load(&g->fenced) || atomic_load(&g->stop) || eng_check(db))
        return TSDB_ERR_STOPPED;
    uint64_t epoch = coldvol_epoch(cv);
    if (epoch == 0 || !cv->ops->writable(cv->ctx))
        return TSDB_ERR_UNAVAILABLE;
    unsigned npos = cv->ops->nstripe ? cv->ops->nstripe(cv->ctx) : (unsigned)(cv->set.k + cv->set.m);
    if (npos == 0)
        return TSDB_ERR_UNAVAILABLE;
    for (int f = atomic_load(&ecgc_test_del_fail); f > 0; f = atomic_load(&ecgc_test_del_fail))
        if (atomic_compare_exchange_weak(&ecgc_test_del_fail, &f, f - 1))
            return TSDB_ERR_UNAVAILABLE;
    eccache_forget(db->tier, db->tier_vol, oid);
    for (unsigned pos = 0; pos < npos; pos++) {
        if (atomic_load(&g->stop) || eng_check(db))
            return TSDB_ERR_STOPPED;
        bool paused = pos == 0 && maybe_pause_before_del(db);
        int rc = cv->ops->del_gc ? cv->ops->del_gc(cv->ctx, oid, pos, epoch, mk_pos)
                                 : cv->ops->del(cv->ctx, oid, pos, epoch, COLD_DEL_MKGC);
        if (paused && rc == TSDB_ERR_UNAVAILABLE) {
            /* After SIGSTOP/SIGCONT past peerlink dead_ms, the first send can hit a half-closed socket before the
             * link reconnects; retry with the same old epoch so the remote store gets to evaluate and FENCE it. */
            for (int tries = 0; tries < 100 && rc == TSDB_ERR_UNAVAILABLE; tries++) {
                struct timespec ts = {0, 100 * 1000000L};
                nanosleep(&ts, NULL);
                rc = cv->ops->del_gc ? cv->ops->del_gc(cv->ctx, oid, pos, epoch, mk_pos)
                                     : cv->ops->del(cv->ctx, oid, pos, epoch, COLD_DEL_MKGC);
            }
        }
        if (rc == CHUNK_ERR_FENCED) {
            fprintf(stderr,
                    "tsdb: %s: chunk delete of position %u at epoch %" PRIu64
                    " was fenced: stopping GC on this deposed leader\n",
                    db->dir, pos, epoch);
            atomic_store(&g->fenced, true);
            return CHUNK_ERR_FENCED;
        }
        if (rc != 0 && rc != TSDB_ERR_NOTFOUND)
            return rc;
        if (ecgc_test_after_del)
            ecgc_test_after_del(db->tier, pos);
    }
    if (!cv->ops->writable(cv->ctx))
        return TSDB_ERR_UNAVAILABLE;
    return 0;
}

static void process_pending(eng *db, coldvol *cv)
{
    struct ecgcmk *g = db->gcmk;
    pthread_mutex_lock(&g->mu);
    size_t n = g->nents;
    gcmk_ent *snap = n ? malloc(n * sizeof *snap) : NULL;
    if (n && snap)
        memcpy(snap, g->ents, n * sizeof *snap);
    pthread_mutex_unlock(&g->mu);
    if (!n || !snap) {
        free(snap);
        return;
    }

    maybe_crash_before_del(db);

    uint8_t (*done)[16] = malloc(n * 16);
    if (!done) {
        free(snap);
        return;
    }
    size_t ndone = 0;
    for (size_t i = 0; i < n; i++) {
        if (!eccold_leader(db) || atomic_load(&g->fenced) || atomic_load(&g->stop) || eng_check(db))
            break;
        int rc = delete_cluster_object(db, cv, snap[i].oid, snap[i].pos);
        if (rc == 0) {
            memcpy(done[ndone++], snap[i].oid, 16);
        } else if (rc == CHUNK_ERR_FENCED || rc == TSDB_ERR_STOPPED) {
            break;
        }
    }
    free(snap);
    if (ndone > 0 && eccold_leader(db) && !atomic_load(&g->fenced) && !atomic_load(&g->stop) && !eng_check(db)) {
        for (size_t off = 0; off < ndone;) {
            size_t chunk = ndone - off < MK_FILES_MAX ? ndone - off : MK_FILES_MAX;
            if (append_and_apply_mkgc(db, MK_GC_DONE, done + off, chunk) != 0)
                break;
            off += chunk;
        }
    }
    free(done);
}

/* ------------------------------------------------------------ orphan discovery (E14) */

typedef struct oc_ent {
    uint8_t oid[16];
    uint8_t vuuid[16];
    uint64_t epoch;
    uint16_t pos;
    int32_t where;
    int status;
} oc_ent;

typedef struct oc_ctx {
    uint8_t (*named)[16];
    size_t nnamed;
    oc_ent *e;
    size_t n, cap;
    bool nomem;
} oc_ctx;

static int oc_cmp(const void *a, const void *b)
{
    const oc_ent *x = a, *y = b;
    int c = memcmp(x->oid, y->oid, 16);
    return c ? c : (x->pos > y->pos) - (x->pos < y->pos);
}

static int oc_list_cb(void *ctx, const chunk_listed *c)
{
    oc_ctx *s = ctx;
    if (s->nnamed && bsearch(c->oid, s->named, s->nnamed, 16, oid_cmp))
        return 0;
    if (s->n == s->cap) {
        size_t cap = s->cap ? s->cap * 2 : 64;
        oc_ent *e = realloc(s->e, cap * sizeof *e);
        if (!e) {
            s->nomem = true;
            return 1;
        }
        s->e = e;
        s->cap = cap;
    }
    oc_ent *e = &s->e[s->n++];
    memcpy(e->oid, c->oid, 16);
    memcpy(e->vuuid, c->vuuid, 16);
    e->epoch = c->epoch;
    e->pos = c->pos;
    e->where = c->store;
    e->status = c->status;
    return 0;
}

static bool vuuid_zero(const uint8_t u[16])
{
    for (int i = 0; i < 16; i++)
        if (u[i])
            return false;
    return true;
}

/* Resolve owner vuuid and max writer epoch across listed positions of one object.
 * Returns 1 if owned by vuuid (with *max_epoch > 0), 0 if foreign, -1 if unreadable. */
static int oc_obj_info(coldvol *cv, const uint8_t vuuid[16], const oc_ent *e, size_t n, uint64_t *max_epoch)
{
    *max_epoch = 0;
    bool any_ok = false, foreign = false;
    for (size_t i = 0; i < n; i++) {
        uint8_t u[16];
        uint64_t ep = 0;
        if (e[i].status == CHUNK_ST_OK && e[i].epoch > 0 && !vuuid_zero(e[i].vuuid)) {
            memcpy(u, e[i].vuuid, 16);
            ep = e[i].epoch;
        } else if (cv->ops->chunk_info) {
            chunk_listed cl = {.pos = e[i].pos, .store = e[i].where};
            memcpy(cl.oid, e[i].oid, 16);
            if (cv->ops->chunk_info(cv->ctx, &cl, u, &ep) != 0)
                continue;
        } else if (cv->ops->owner) {
            chunk_listed cl = {.pos = e[i].pos, .store = e[i].where};
            memcpy(cl.oid, e[i].oid, 16);
            if (cv->ops->owner(cv->ctx, &cl, u) != 0)
                continue;
            ep = e[i].epoch;
        } else {
            continue;
        }
        if (memcmp(u, vuuid, 16) != 0) {
            foreign = true;
            continue;
        }
        any_ok = true;
        if (ep > *max_epoch)
            *max_epoch = ep;
    }
    if (any_ok)
        return 1;
    return foreign ? 0 : -1;
}

static int collect_named(eng *db, oid_vec *ov)
{
    /* 1. WAL scan first (committed and uncommitted records): a segment unlinked concurrently has already applied
     *    its effects into listed/retired parts checked in step 2. Planted bug gcnamedorphan skips this scan. */
    int rc = 0;
    if (ecgc_mutant != GC_MUT_NAMEDORPHAN)
        rc = scan_wal_oids(db, ov);
    if (rc)
        return rc;

    /* 2. Listed .ec parts and retired .ec parts under db->lock. */
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && rc == 0; i++) {
        struct ecpart *ep = atomic_load(&db->parts[i]->ec);
        if (ep && !ep->test_mark)
            rc = ov_push(ov, ep->rec.oid);
    }
    uint8_t (*ret)[16] = NULL;
    size_t nret = 0;
    if (rc == 0)
        rc = ecgc_list_retired_locked(db, &ret, &nret);
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < nret && rc == 0; i++)
        rc = ov_push(ov, ret[i]);
    free(ret);
    if (rc)
        return rc;

    /* 3. Already pending in GCMK. */
    struct ecgcmk *g = db->gcmk;
    pthread_mutex_lock(&g->mu);
    for (size_t i = 0; i < g->nents && rc == 0; i++)
        rc = ov_push(ov, g->ents[i].oid);
    pthread_mutex_unlock(&g->mu);
    if (rc == 0 && ov->n > 1)
        qsort(ov->v, ov->n, 16, oid_cmp);
    return rc;
}

static void collect_orphans(eng *db, coldvol *cv)
{
    struct ecgcmk *g = db->gcmk;
    bool suspended = cv->ops->gc_suspended ? cv->ops->gc_suspended(cv->ctx) : false;
    if (suspended && ecgc_mutant != GC_MUT_SUSPENDIGNORED)
        return;

    uint64_t lead_epoch = coldvol_epoch(cv);
    if (lead_epoch == 0 || !cv->ops->writable(cv->ctx))
        return;

    int64_t now = eng_mono_ms();
    bool manual = atomic_load(&ecgc_test_manual);
    pthread_mutex_lock(&g->mu);
    size_t nq = g->nqorph;
    pthread_mutex_unlock(&g->mu);
    int64_t interval = atomic_load(&ecgc_test_poll_ms);
    if (interval <= 0)
        interval = 2000;
    if (!manual && nq == 0 && g->last_sweep_ms != 0 && now - g->last_sweep_ms < interval)
        return;
    g->last_sweep_ms = now;

    oid_vec named = {0};
    if (collect_named(db, &named) != 0) {
        free(named.v);
        return;
    }

    oid_vec due = {0};

    /* A. Explicitly queued aborted-settle orphans (put by this leader, never appended to MK_COLD). */
    pthread_mutex_lock(&g->mu);
    size_t nqa = g->nqorph;
    uint8_t (*qa)[16] = nqa ? malloc(nqa * 16) : NULL;
    if (qa)
        memcpy(qa, g->qorph, nqa * 16);
    pthread_mutex_unlock(&g->mu);
    for (size_t i = 0; qa && i < nqa && due.n < MK_FILES_MAX; i++) {
        if (named.n && bsearch(qa[i], named.v, named.n, 16, oid_cmp))
            continue;
        if (ecgc_mutant != GC_MUT_NOHOLD && ecgc_held(db->tier, db->tier_vol, qa[i]))
            continue;
        if (ov_push(&due, qa[i]) != 0)
            break;
    }
    free(qa);

    /* B. Listing sweep of chunk stores: E14's four conditions (not named, earlier epoch, older than grace, !suspended). */
    oc_ctx sc = {.named = named.v, .nnamed = named.n};
    int lrc = cv->ops->list_names(cv->ctx, oc_list_cb, &sc);
    if (lrc == 0 && !sc.nomem) {
        if (sc.n > 1)
            qsort(sc.e, sc.n, sizeof *sc.e, oc_cmp);
        int64_t grace = orphan_grace_ms();
        const uint8_t *vuuid = tier_vuuid(db);
        gcmk_cand *next = NULL;
        size_t nnext = 0, capnext = 0;

        for (size_t i = 0, j; i < sc.n; i = j) {
            for (j = i + 1; j < sc.n && memcmp(sc.e[j].oid, sc.e[i].oid, 16) == 0; j++)
                ;
            uint64_t obj_epoch = 0;
            int own = oc_obj_info(cv, vuuid, sc.e + i, j - i, &obj_epoch);
            if (own != 1 || obj_epoch == 0 || obj_epoch >= lead_epoch)
                continue; /* foreign, unreadable, or written in current/newer epoch */
            if (ecgc_mutant != GC_MUT_NOHOLD && ecgc_held(db->tier, db->tier_vol, sc.e[i].oid))
                continue;
            int64_t first = now;
            for (size_t c = 0; c < g->ncands; c++) {
                if (memcmp(g->cands[c].oid, sc.e[i].oid, 16) == 0) {
                    first = g->cands[c].first_seen_ms;
                    break;
                }
            }
            bool queued = false;
            if (now - first >= grace) {
                bool dup = false;
                for (size_t d = 0; d < due.n && !dup; d++)
                    dup = memcmp(due.v[d], sc.e[i].oid, 16) == 0;
                if (dup || (due.n < MK_FILES_MAX && ov_push(&due, sc.e[i].oid) == 0))
                    queued = true;
            }
            if (!queued) {
                if (nnext == capnext) {
                    size_t nc = capnext ? capnext * 2 : 16;
                    gcmk_cand *p = realloc(next, nc * sizeof *p);
                    if (!p)
                        break;
                    next = p;
                    capnext = nc;
                }
                memcpy(next[nnext].oid, sc.e[i].oid, 16);
                next[nnext].first_seen_ms = first;
                nnext++;
            }
        }
        free(g->cands);
        g->cands = next;
        g->ncands = nnext;
        g->capcands = capnext;
    }
    free(sc.e);
    free(named.v);

    if (due.n > 0 && eccold_leader(db) && !atomic_load(&g->fenced) && !atomic_load(&g->stop) && !eng_check(db)) {
        if (append_and_apply_mkgc(db, MK_GC_ORPHAN, due.v, due.n) == 0)
            process_pending(db, cv);
    }
    free(due.v);
}

void ecgcmk_work(eng *db)
{
    if (!db || !db->ec_cluster || !db->gcmk || !db->tier || eng_check(db) || atomic_load(&db->stop))
        return;
    struct ecgcmk *g = db->gcmk;
    pthread_mutex_lock(&g->wmu);
    if (!eccold_leader(db) || atomic_load(&g->fenced) || atomic_load(&g->stop) || eng_check(db) ||
        atomic_load(&db->stop))
        goto out;
    coldvol *cv = tier_cold(db);
    if (!cv)
        goto out;

    /* 1. Finish any already-committed MK_GC deletes (including those inherited across failover). */
    process_pending(db, cv);
    if (!eccold_leader(db) || atomic_load(&g->fenced) || atomic_load(&g->stop) || eng_check(db))
        goto out;

    /* 2. Retired .ec objects past obsolete_part_lifetime_s and unheld (runs even when gc_suspended). */
    uint8_t (*ret_oids)[16] = NULL;
    size_t nret = 0;
    if (ecgc_take_due_retired(db, eng_mono_ms(), &ret_oids, &nret) == 0 && nret > 0) {
        /* Exclude any oid already on the pending-delete list */
        size_t keep = 0;
        pthread_mutex_lock(&g->mu);
        for (size_t i = 0; i < nret; i++)
            if (ent_find_locked(g, ret_oids[i]) < 0)
                memcpy(ret_oids[keep++], ret_oids[i], 16);
        pthread_mutex_unlock(&g->mu);
        if (keep > 0 && append_and_apply_mkgc(db, MK_GC_RETIRED, ret_oids, keep) == 0)
            process_pending(db, cv);
    }
    free(ret_oids);
    if (!eccold_leader(db) || atomic_load(&g->fenced) || atomic_load(&g->stop) || eng_check(db))
        goto out;

    /* 3. Orphan collection by E14's four conditions (stopped while gc_suspended). */
    collect_orphans(db, cv);
out:
    pthread_mutex_unlock(&g->wmu);
}
