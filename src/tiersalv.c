/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local tier salvage past m LOST stores (src/tiersalv.h; R15 r15-salvage, E15, §15.7, D11).
 */
#define _GNU_SOURCE
#include "tiersalv.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "ecgc.h"
#include "ecpart.h"
#include "ecswap.h"
#include "ecunsettle.h"
#include "internal.h"
#include "tier.h"
#include "tiers.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int tiersalv_mutant;

int tiersalv_mutant_parse(const char *name)
{
    if (!name)
        return -1;
    if (strcmp(name, "none") == 0)
        return TIERSALV_MUT_NONE;
    if (strcmp(name, "salvagedown") == 0)
        return TIERSALV_MUT_DOWN;
    if (strcmp(name, "salvagelatest") == 0)
        return TIERSALV_MUT_LATEST;
    return -1;
}

static inline int tiersalv_mut(void)
{
#ifdef TSDB_TEST_HOOKS
    return tiersalv_mutant;
#else
    return TIERSALV_MUT_NONE;
#endif
}

void tiersalv_get_counters(const struct tier *t, tiersalv_counters *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof *out);
    if (!t)
        return;
    out->partitions = atomic_load_explicit(&((tier *)t)->salvaged_partitions, memory_order_relaxed);
    out->parts = atomic_load_explicit(&((tier *)t)->salvaged_parts, memory_order_relaxed);
    out->rows_lost = atomic_load_explicit(&((tier *)t)->salvaged_rows, memory_order_relaxed);
}

#ifdef TSDB_EC

static int cmp_i64(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a, y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

static bool store_is_down_not_lost(const chunk_backend *be, int i)
{
    if (chunk_be_store_is_lost(be, i))
        return false;
    int st = chunk_be_store_state(be, i);
    return st != CHUNK_STORE_UP && st != CHUNK_STORE_REPLACING;
}

int tiersalv_check_stores(const struct tier *t, bool force)
{
    if (!t || !t->be) {
        fprintf(stderr, "tsdb: tier salvage: no local tier backend is open\n");
        return TSDB_ERR_INVALID;
    }
    if (force || tiersalv_mut() == TIERSALV_MUT_DOWN)
        return 0;
    const chunk_backend *be = t->be;
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++) {
        if (store_is_down_not_lost(be, i)) {
            int st = chunk_be_store_state(be, i);
            char pbuf[PATH_MAX];
            bool has_p = chunk_be_store_path_copy(be, i, pbuf, sizeof pbuf);
            fprintf(stderr,
                    "tsdb: tier salvage: store %d (%s) is %s and not LOST (it may still come back): "
                    "refused without --force\n",
                    i, has_p ? pbuf : "?", chunk_store_state_name(st));
            return TSDB_ERR_UNAVAILABLE;
        }
    }
    return 0;
}

/* Determines whether an .ec part's object has strictly more than m positions on permanently LOST stores.
 * Runs unlocked (never holds db->lock across chunk_store_peek). Handles full stripes, small k' < k objects,
 * m = 0 single-copy objects, and objects written before a later `tier add` (via chunk_be_infer_place_n). */
static bool ec_part_is_lost(const eng *db, const chunk_backend *be, const part *p)
{
    struct ecpart *ep = atomic_load(&((part *)p)->ec);
    if (!ep || ep->test_mark)
        return false;
    const ec_record *r = &ep->rec;
    unsigned npos = (unsigned)r->k + (unsigned)r->m;
    if (npos == 0 || npos > CHUNK_POS_MAX)
        return false;
    int bn = chunk_be_nstores(be);
    if (bn <= 0)
        return false;

    int min_n = npos >= 2 ? (int)npos : 1;
    if (min_n > bn)
        min_n = bn;

    /* Fast path: only one possible store count for placement (e.g. 6 stores with 4+2 -> min_n == bn == 6). */
    if (min_n == bn) {
        unsigned lost_pos = 0;
        for (unsigned pos = 0; pos < npos; pos++) {
            int sidx = chunk_be_place_n(r->oid, pos, bn);
            if (chunk_be_store_is_lost(be, sidx))
                lost_pos++;
        }
        return lost_pos > (unsigned)r->m;
    }

    /* Multiple candidate store counts (e.g. k' < k or stores added after settle): observe any surviving chunk on
     * an open store to infer the exact store count `place_n` the object was placed with. */
    int obs_store[CHUNK_POS_MAX];
    unsigned obs_pos[CHUNK_POS_MAX];
    size_t nobs = 0;
    const uint8_t *vuuid = tier_vuuid(db);
    if (vuuid) {
        for (int s = 0; s < bn && nobs < CHUNK_POS_MAX; s++) {
            chunk_store *st = chunk_be_store_ref(be, s);
            if (!st)
                continue;
            for (unsigned pos = 0; pos < npos && nobs < CHUNK_POS_MAX; pos++) {
                chunk_hdr h;
                if (chunk_store_peek(st, db->tier_vol, r->oid, pos, &h) == 0 &&
                    memcmp(h.vuuid, vuuid, 16) == 0) {
                    obs_store[nobs] = s;
                    obs_pos[nobs] = pos;
                    nobs++;
                }
            }
            chunk_store_unref(st);
        }
    }

    if (nobs > 0) {
        int pn = chunk_be_infer_place_n(be, r->oid, npos, obs_store, obs_pos, nobs, -1);
        unsigned lost_pos = 0;
        for (unsigned pos = 0; pos < npos; pos++) {
            int sidx = chunk_be_place_n(r->oid, pos, pn);
            if (chunk_be_store_is_lost(be, sidx))
                lost_pos++;
        }
        return lost_pos > (unsigned)r->m;
    }

    /* No surviving chunks observed on any UP store: match chunkread.c's conservative multi-candidate rule so an
     * object written before a `tier add` is declared lost iff every candidate with 0 UP positions has > m LOST. */
    bool any_cand = false, all_cand_lost = true;
    for (int cand = bn; cand >= min_n; cand--) {
        unsigned lost_pos = 0, up_pos = 0;
        for (unsigned pos = 0; pos < npos; pos++) {
            int sidx = chunk_be_place_n(r->oid, pos, cand);
            if (chunk_be_store_is_lost(be, sidx))
                lost_pos++;
            else if (chunk_be_store_state(be, sidx) == CHUNK_STORE_UP)
                up_pos++;
        }
        if (up_pos > 0)
            continue;
        any_cand = true;
        if (lost_pos <= (unsigned)r->m)
            all_cand_lost = false;
    }
    if (any_cand)
        return all_cand_lost;

    unsigned lost_pos = 0;
    for (unsigned pos = 0; pos < npos; pos++) {
        int sidx = chunk_be_place_n(r->oid, pos, bn);
        if (chunk_be_store_is_lost(be, sidx))
            lost_pos++;
    }
    return lost_pos > (unsigned)r->m;
}

static int salvage_one_key(eng *db, int64_t key, bool force, tiersalv_report *rep)
{
    tier *t = db->tier;
    chunk_backend *be = tier_backend(t);
    int rc = tiersalv_check_stores(t, force);
    if (rc != 0)
        return rc;

    /* Step 1: snapshot all listed .ec parts of `key` under db->lock (no chunk I/O under lock). */
    part **cand = NULL;
    size_t ncand = 0, cap = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (p->partition != key || p->dropped || !ep || ep->test_mark)
            continue;
        if (ncand == cap) {
            size_t nc = cap ? cap * 2 : 8;
            part **x = realloc(cand, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            cand = x;
            cap = nc;
        }
        part_ref(p);
        cand[ncand++] = p;
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

    /* Step 2: filter to unrecoverable .ec parts (> m positions on LOST stores) unlocked. */
    part **lost = calloc(ncand, sizeof *lost);
    if (!lost) {
        for (size_t i = 0; i < ncand; i++)
            part_unref(cand[i]);
        free(cand);
        return TSDB_ERR_NOMEM;
    }
    size_t nlost = 0;
    for (size_t i = 0; i < ncand; i++) {
        if (ec_part_is_lost(db, be, cand[i]))
            lost[nlost++] = cand[i];
        else
            part_unref(cand[i]);
    }
    free(cand);
    if (nlost == 0) {
        free(lost);
        return 0;
    }

    /* Step 3: re-check store states and pin the lost .ec parts under db->lock (`p->settling = true`). */
    rc = tiersalv_check_stores(t, force);
    if (rc != 0) {
        for (size_t i = 0; i < nlost; i++)
            part_unref(lost[i]);
        free(lost);
        return rc;
    }

    size_t pinned = 0;
    pthread_mutex_lock(&db->lock);
    if (eng_check(db)) {
        rc = TSDB_ERR_IO;
    } else {
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
            if (p->settling || p->reencoding || p->merging) {
                rc = TSDB_ERR_LOCKED;
                /* unref remaining unpinned entries */
                for (size_t k = i; k < nlost; k++)
                    part_unref(lost[k]);
                break;
            }
            p->settling = true;
            lost[pinned++] = p;
        }
    }
    if (rc != 0) {
        for (size_t i = 0; i < pinned; i++)
            lost[i]->settling = false;
    }
    pthread_mutex_unlock(&db->lock);

    if (rc != 0 || pinned == 0) {
        for (size_t i = 0; i < pinned; i++)
            part_unref(lost[i]);
        if (rc == TSDB_ERR_IO) {
            for (size_t i = pinned; i < nlost; i++)
                part_unref(lost[i]);
        }
        free(lost);
        return rc;
    }

    /* Step 4: before dropping any lost .ec part as lost rows, check whether an intact local settled_<P>.part copy
     * still exists (kept during keep_s, or held while stores are LOST because digests cannot match). Restore any
     * verified copy in place via ec_swap_unsettle (0 rows lost); retire only genuinely unrecoverable parts. */
    part **keep_in = calloc(pinned, sizeof *keep_in);
    part **keep_out = calloc(pinned, sizeof *keep_out);
    part **drop_in = calloc(pinned, sizeof *drop_in);
    if (!keep_in || !keep_out || !drop_in) {
        free(keep_in);
        free(keep_out);
        free(drop_in);
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < pinned; i++)
            lost[i]->settling = false;
        pthread_mutex_unlock(&db->lock);
        for (size_t i = 0; i < pinned; i++)
            part_unref(lost[i]);
        free(lost);
        return TSDB_ERR_NOMEM;
    }

    size_t nkeep = 0, ndrop = 0;
    for (size_t i = 0; i < pinned && rc == 0; i++) {
        part *restored = NULL;
        int src_rc = ecunsettle_try_settled(db, lost[i], &restored);
        if (src_rc == 0) {
            keep_in[nkeep] = lost[i];
            keep_out[nkeep] = restored;
            nkeep++;
        } else if (src_rc == TSDB_ERR_NOTFOUND || src_rc == TSDB_ERR_CORRUPT) {
            drop_in[ndrop++] = lost[i];
        } else {
            rc = src_rc;
        }
    }

    size_t restored_cnt = 0;
    for (int attempt = 0; attempt < 2 && rc == 0 && nkeep > 0; attempt++) {
        int urc = ec_swap_unsettle(db, keep_in, keep_out, nkeep);
        for (size_t i = 0; i < nkeep; i++) {
            part_unref(keep_out[i]);
            keep_out[i] = NULL;
        }
        if (urc == 0) {
            restored_cnt = nkeep;
            nkeep = 0;
            break;
        }
        if (urc == TSDB_ERR_NOTFOUND && attempt == 0) {
            /* A settled_ copy was purged concurrently before the swap: re-probe and move missing ones to drop_in. */
            size_t next_keep = 0;
            for (size_t i = 0; i < nkeep && rc == 0; i++) {
                part *restored = NULL;
                int src_rc = ecunsettle_try_settled(db, keep_in[i], &restored);
                if (src_rc == 0) {
                    keep_in[next_keep] = keep_in[i];
                    keep_out[next_keep] = restored;
                    next_keep++;
                } else if (src_rc == TSDB_ERR_NOTFOUND || src_rc == TSDB_ERR_CORRUPT) {
                    drop_in[ndrop++] = keep_in[i];
                } else {
                    rc = src_rc;
                }
            }
            nkeep = next_keep;
            continue;
        }
        rc = urc;
    }
    for (size_t i = 0; i < nkeep; i++) {
        if (keep_out[i])
            part_unref(keep_out[i]);
    }
    if (restored_cnt > 0) {
        fprintf(stderr,
                "tsdb: %s: tier salvage: partition %" PRId64 ": restored %zu .ec part(s) from intact settled_ copies "
                "(0 rows lost)\n",
                db->dir, key, restored_cnt);
        if (rep)
            rep->restored += restored_cnt;
        ecgc_kick_tier(t);
    }

    /* Compute upper-bound raw rows lost from each unrecoverable .ec part's intact local index before retiring. */
    uint64_t rows_lost = 0;
    if (rc == 0 && ndrop > 0) {
        for (size_t i = 0; i < ndrop; i++)
            rows_lost += drop_in[i]->rows;
        bool no_lgen_mut = tiersalv_mut() == TIERSALV_MUT_LATEST;
        rc = ec_swap_salvage(db, drop_in, ndrop, no_lgen_mut);
    }

    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < pinned; i++)
        lost[i]->settling = false;
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < pinned; i++)
        part_unref(lost[i]);
    free(keep_in);
    free(keep_out);
    free(drop_in);
    free(lost);

    if (rc == 0 && ndrop > 0) {
        fprintf(stderr,
                "tsdb: %s: tier salvage: partition %" PRId64 ": retired %zu lost .ec part(s), %" PRIu64 " rows lost\n",
                db->dir, key, ndrop, rows_lost);
        atomic_fetch_add_explicit(&t->salvaged_partitions, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->salvaged_parts, ndrop, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->salvaged_rows, rows_lost, memory_order_relaxed);
        if (rep) {
            rep->partitions++;
            rep->parts += ndrop;
            rep->rows_lost += rows_lost;
        }
    }
    return rc;
}

int tiersalv_eng(eng *db, int64_t partition, bool all_partitions, bool force, tiersalv_report *rep)
{
    if (!db)
        return TSDB_ERR_INVALID;
    if (db->opt.read_only || db->opt.follow || db->rp)
        return TSDB_ERR_READONLY;
    if (db->ec_cluster || eng_jr(db) || db->replica_req) {
        fprintf(stderr, "tsdb: %s: tier salvage is for single-server local tiers only: refused\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (!db->tier || !tier_backend(db->tier)) {
        fprintf(stderr, "tsdb: %s: tier salvage: volume has no local tier attached\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (eng_check(db))
        return TSDB_ERR_IO;

    int rc = tiersalv_check_stores(db->tier, force);
    if (rc != 0)
        return rc;

    if (!all_partitions)
        return salvage_one_key(db, partition, force, rep);

    /* Collect all distinct partition keys with at least one listed .ec part, sorted ascending. */
    int64_t *keys = NULL;
    size_t nkeys = 0, cap = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (p->dropped || !ep || ep->test_mark)
            continue;
        bool dup = false;
        for (size_t j = 0; j < nkeys && !dup; j++)
            dup = keys[j] == p->partition;
        if (dup)
            continue;
        if (nkeys == cap) {
            size_t nc = cap ? cap * 2 : 16;
            int64_t *x = realloc(keys, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            keys = x;
            cap = nc;
        }
        keys[nkeys++] = p->partition;
    }
    pthread_mutex_unlock(&db->lock);
    if (rc != 0) {
        free(keys);
        return rc;
    }
    if (nkeys > 1)
        qsort(keys, nkeys, sizeof *keys, cmp_i64);
    for (size_t i = 0; i < nkeys && rc == 0; i++)
        rc = salvage_one_key(db, keys[i], force, rep);
    free(keys);
    return rc;
}

/* ------------------------------------------------------------ offline CLI runner */

static int cli_read_fd_all(int fd, char **out, size_t *len)
{
    *out = NULL;
    *len = 0;
    struct stat st;
    if (fstat(fd, &st) != 0)
        return TSDB_ERR_IO;
    if (st.st_size < 0 || (uint64_t)st.st_size > (64ull << 20))
        return TSDB_ERR_CORRUPT;
    *out = malloc((size_t)st.st_size + 1);
    if (!*out)
        return TSDB_ERR_NOMEM;
    ssize_t got = pread(fd, *out, (size_t)st.st_size, 0);
    if (got != st.st_size) {
        free(*out);
        *out = NULL;
        return TSDB_ERR_IO;
    }
    (*out)[got] = '\0';
    *len = (size_t)got;
    return 0;
}

static long cli_meta_volume(const char *txt)
{
    for (const char *l = txt; *l; l = strchr(l, '\n') ? strchr(l, '\n') + 1 : l + strlen(l)) {
        if (strncmp(l, "volume=", 7) != 0)
            continue;
        char *end = NULL;
        long v = strtol(l + 7, &end, 10);
        return end == l + 7 || (*end != '\n' && *end != '\0') || v < 0 || v >= 65535 ? -1 : v;
    }
    return -1;
}

static int cli_read_id_file(int dirfd, const char *name, uint64_t *id_out)
{
    int fd = openat(dirfd, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    char buf[64];
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0)
        return TSDB_ERR_CORRUPT;
    buf[n] = '\0';
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(buf, &end, 16);
    if (errno || end == buf || (*end != '\n' && *end != '\0'))
        return TSDB_ERR_CORRUPT;
    *id_out = (uint64_t)v;
    return 0;
}

/* Matches src/volumes.c vol_line_ok / volumes_parse: verifies every line's xxh64(path, len, line_idx) checksum and
 * rejects NUL bytes or torn/damaged lines with TSDB_ERR_CORRUPT. */
static int cli_read_volumes(int rootfd, char ***paths_out, size_t *n_out)
{
    *paths_out = NULL;
    *n_out = 0;
    int vfd = openat(rootfd, "VOLUMES", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (vfd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct flock fl = {.l_type = F_RDLCK, .l_whence = SEEK_SET};
    while (fcntl(vfd, F_OFD_SETLKW, &fl) != 0 && errno == EINTR)
        ;
    char *txt = NULL;
    size_t len = 0;
    int rc = cli_read_fd_all(vfd, &txt, &len);
    close(vfd);
    if (rc != 0)
        return rc;

    char **paths = NULL;
    size_t n = 0, cap = 0;
    for (char *line = txt, *next; rc == 0 && line < txt + len; line = next) {
        next = memchr(line, '\n', len - (size_t)(line - txt));
        if (!next) {
            fprintf(stderr, "tsdb: tier salvage: VOLUMES line %zu is incomplete\n", n + 1);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        size_t ll = (size_t)(next - line);
        *next++ = '\0';
        unsigned check = 0;
        int pos = 0;
        bool ok = ll >= 10 && !memchr(line, '\0', ll) && sscanf(line, "%8x %n", &check, &pos) == 1 && pos == 9 &&
                  check == (uint32_t)xxh64(line + 9, ll - 9, n + 1);
        if (!ok) {
            fprintf(stderr, "tsdb: tier salvage: VOLUMES line %zu is damaged\n", n + 1);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 8;
            char **x = realloc(paths, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            paths = x;
            cap = nc;
        }
        paths[n] = strdup(line + 9);
        if (!paths[n]) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        n++;
    }
    free(txt);
    if (rc != 0) {
        for (size_t i = 0; i < n; i++)
            free(paths[i]);
        free(paths);
        return rc;
    }
    *paths_out = paths;
    *n_out = n;
    return 0;
}

static int cli_check_extra_volume(const char *vdir, uint32_t vi, bool has_root_id, uint64_t root_id)
{
    int vfd = open(vdir, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (vfd < 0) {
        fprintf(stderr, "tsdb: tier salvage: volume %u (%s): %s\n", vi, vdir, strerror(errno));
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    if (has_root_id) {
        uint64_t vid = 0;
        int irc = cli_read_id_file(vfd, "ROOT_ID", &vid);
        if (irc != 0 || vid != root_id) {
            close(vfd);
            fprintf(stderr, "tsdb: tier salvage: volume %u (%s): ROOT_ID missing or does not match root ID\n", vi,
                    vdir);
            return TSDB_ERR_CORRUPT;
        }
    }
    char meta[4096];
    ssize_t mn = -1;
    int mfd = openat(vfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (mfd >= 0) {
        mn = read(mfd, meta, sizeof meta - 1);
        close(mfd);
    }
    close(vfd);
    if (mn < 0) {
        fprintf(stderr, "tsdb: tier salvage: volume %u (%s): missing or unreadable META\n", vi, vdir);
        return TSDB_ERR_CORRUPT;
    }
    meta[mn] = '\0';
    unsigned feats = 0;
    meta_requires_parse(meta, &feats, NULL, 0);
    if (feats & META_REQ_CLUSTER) {
        fprintf(stderr, "tsdb: tier salvage: volume %u (%s) is a cluster volume: refused\n", vi, vdir);
        return TSDB_ERR_INVALID;
    }
    long mv = cli_meta_volume(meta);
    if (mv != (long)vi) {
        fprintf(stderr, "tsdb: tier salvage: volume %u (%s): META records volume=%ld, expected %u\n", vi, vdir, mv, vi);
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

int tiersalv_cli(const char *dir, int64_t partition, bool all_partitions, bool force, tiersalv_report *rep)
{
    if (rep)
        memset(rep, 0, sizeof *rep);
    if (!dir || !*dir)
        return TSDB_ERR_INVALID;
    char root[PATH_MAX];
    if (!realpath(dir, root)) {
        fprintf(stderr, "tsdb: tier salvage: %s: %s\n", dir, strerror(errno));
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    int rootfd = open(root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (rootfd < 0) {
        fprintf(stderr, "tsdb: tier salvage: %s: %s\n", root, strerror(errno));
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }

    /* Check root META is readable, not a cluster volume, and volume=0 if recorded. */
    char meta[4096];
    ssize_t mn = -1;
    int mfd = openat(rootfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (mfd >= 0) {
        mn = read(mfd, meta, sizeof meta - 1);
        close(mfd);
    }
    if (mn < 0) {
        int err = errno;
        close(rootfd);
        fprintf(stderr, "tsdb: tier salvage: %s/META: %s\n", root, strerror(err));
        return err == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    meta[mn] = '\0';
    unsigned feats = 0;
    meta_requires_parse(meta, &feats, NULL, 0);
    if (feats & META_REQ_CLUSTER) {
        close(rootfd);
        fprintf(stderr, "tsdb: tier salvage: %s is a cluster database (use tsdb cluster volume salvage): refused\n",
                root);
        return TSDB_ERR_INVALID;
    }
    long root_mv = cli_meta_volume(meta);
    if (root_mv > 0) {
        close(rootfd);
        fprintf(stderr, "tsdb: tier salvage: %s/META records volume=%ld, expected 0 (pass the database root)\n", root,
                root_mv);
        return TSDB_ERR_CORRUPT;
    }

    uint64_t root_id = 0;
    int id_rc = cli_read_id_file(rootfd, "ID", &root_id);
    if (id_rc != 0 && id_rc != TSDB_ERR_NOTFOUND) {
        close(rootfd);
        fprintf(stderr, "tsdb: tier salvage: %s/ID is damaged\n", root);
        return id_rc;
    }
    bool has_root_id = id_rc == 0;

    /* Check TIERS exists and names stores. */
    tiers_view tv;
    tiers_view_init(&tv);
    int rc = tiers_load(rootfd, false, false, &tv);
    if (rc != 0) {
        tiers_view_free(&tv);
        close(rootfd);
        return rc;
    }
    if (tv.nstores == 0) {
        tiers_view_free(&tv);
        close(rootfd);
        fprintf(stderr, "tsdb: tier salvage: %s has no local tier stores configured\n", root);
        return TSDB_ERR_INVALID;
    }
    tiers_view_free(&tv);

    char **extra_vols = NULL;
    size_t nextra = 0;
    rc = cli_read_volumes(rootfd, &extra_vols, &nextra);
    close(rootfd);
    if (rc != 0)
        return rc;

    /* Validate every extra volume's ROOT_ID and META volume=<vi> before opening/modifying any volume. */
    for (size_t vi = 1; vi <= nextra && rc == 0; vi++)
        rc = cli_check_extra_volume(extra_vols[vi - 1], (uint32_t)vi, has_root_id, root_id);
    if (rc != 0) {
        for (size_t i = 0; i < nextra; i++)
            free(extra_vols[i]);
        free(extra_vols);
        return rc;
    }

    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 0;
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.stats = -1;
    o.latest_cache = 0;

    tier *t = NULL;
    rc = tier_open_salvage(root, &o, &t);
    if (rc != 0 || !t) {
        for (size_t i = 0; i < nextra; i++)
            free(extra_vols[i]);
        free(extra_vols);
        return rc ? rc : TSDB_ERR_INVALID;
    }
    tier_quiesce(t);

    /* Validate store states upfront before opening/modifying any volume. */
    rc = tiersalv_check_stores(t, force);
    if (rc != 0) {
        tier_put(t);
        for (size_t i = 0; i < nextra; i++)
            free(extra_vols[i]);
        free(extra_vols);
        return rc;
    }

    for (size_t vi = 0; vi <= nextra && rc == 0; vi++) {
        const char *vdir = vi == 0 ? root : extra_vols[vi - 1];
        eng *e = NULL;
        tier_want(t, (uint32_t)vi);
        rc = eng_open(vdir, &o, &e);
        tier_want(NULL, 0);
        if (rc != 0 || !e) {
            if (rc == TSDB_ERR_LOCKED)
                fprintf(stderr, "tsdb: tier salvage: %s is held by a running writer (stop tsdbd first)\n", vdir);
            rc = rc ? rc : TSDB_ERR_IO;
            break;
        }
        rc = tiersalv_eng(e, partition, all_partitions, force, rep);
        int crc = eng_close(e);
        if (rc == 0 && crc != 0)
            rc = crc;
    }

    tier_put(t);
    for (size_t i = 0; i < nextra; i++)
        free(extra_vols[i]);
    free(extra_vols);
    return rc;
}

#else /* !TSDB_EC */

int tiersalv_check_stores(const struct tier *t, bool force)
{
    (void)t;
    (void)force;
    fprintf(stderr, "tsdb: tier salvage: not built with the erasure codec (TSDB_EC)\n");
    return TSDB_ERR_INVALID;
}

int tiersalv_eng(eng *db, int64_t partition, bool all_partitions, bool force, tiersalv_report *rep)
{
    (void)db;
    (void)partition;
    (void)all_partitions;
    (void)force;
    (void)rep;
    fprintf(stderr, "tsdb: tier salvage: not built with the erasure codec (TSDB_EC)\n");
    return TSDB_ERR_INVALID;
}

int tiersalv_cli(const char *dir, int64_t partition, bool all_partitions, bool force, tiersalv_report *rep)
{
    (void)dir;
    (void)partition;
    (void)all_partitions;
    (void)force;
    if (rep)
        memset(rep, 0, sizeof *rep);
    fprintf(stderr, "tsdb: tier salvage: not built with the erasure codec (TSDB_EC)\n");
    return TSDB_ERR_INVALID;
}

#endif /* TSDB_EC */
