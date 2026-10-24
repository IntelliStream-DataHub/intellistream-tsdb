/*
 * Part files: immutable, sorted by (series, ts), written once and mmap'd.
 *
 *   header (24 bytes) | ts block, value block, ... per granule | pad to 8 |
 *   granule index (80 bytes per granule) | footer (56 bytes)
 *
 * A granule holds rows of one series; its value block is coded for the
 * series' type and its index entry carries the statistics of its known
 * values. Version 1 parts (float32, 64-byte index entries) are still read:
 * their index is converted to the current layout when they are opened.
 * The header also records the newest WAL segment whose rows the part holds.
 * A part is written to tmp_*.part, fsync'd, and renamed to its final name
 * p<partition>_<min_block>_<max_block>_<level>.part, which makes it visible.
 */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define PART_MAGIC "TSDBPART"
#define PART_END_MAGIC "TSDBEND1"
#define PW_FLUSH_BYTES (4u << 20)

static int write_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len) {
        ssize_t r = write(fd, p, len);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return TSDB_ERR_IO;
        }
        p += r;
        len -= (size_t)r;
    }
    return 0;
}

/* ---------------------------------------------------------------- reader */

int part_open(int dirfd, const char *name, part **out) { return part_open_as(dirfd, name, name, out); }

/* Opens the file `file` as the part called `name` (a follower may find a part
 * under its retired_ name). */
int part_open_as(int dirfd, const char *file, const char *name, part **out)
{
    *out = NULL;
    part *p = calloc(1, sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    int consumed = 0;
    if (strlen(name) >= sizeof p->name ||
        sscanf(name, "p%" SCNd64 "_%" SCNu64 "_%" SCNu64 "_%u.part%n", &p->partition, &p->min_block,
               &p->max_block, &p->level, &consumed) != 4 ||
        name[consumed] != '\0') {
        free(p);
        return TSDB_ERR_INVALID;
    }
    snprintf(p->name, sizeof p->name, "%s", name);
    p->dirfd = dirfd;

    int fd = openat(dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        int err = errno;
        free(p);
        return err == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        free(p);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size;
    if (size < sizeof(part_header) + sizeof(part_footer)) {
        close(fd);
        free(p);
        return TSDB_ERR_CORRUPT;
    }
    void *map = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        free(p);
        return TSDB_ERR_IO;
    }
    p->map = map;
    p->map_len = size;
    p->created = st.st_mtime;
    p->written = st.st_mtime;

    part_header h;
    part_footer f;
    memcpy(&h, p->map, sizeof h);
    memcpy(&f, p->map + size - sizeof f, sizeof f);
    int rc = TSDB_ERR_CORRUPT;
    if (memcmp(h.magic, PART_MAGIC, 8) != 0 || memcmp(f.magic, PART_END_MAGIC, 8) != 0)
        goto fail;
    if ((h.version != 1 && h.version != FORMAT_VERSION) || f.version != h.version || h.granule_rows == 0 ||
        h.granule_rows > MAX_GRANULE_ROWS)
        goto fail;
    p->grows = h.granule_rows;
    p->wal_seq = h.wal_seq;
    size_t entry = h.version == 1 ? sizeof(granule_v1) : sizeof(granule);
    if (f.index_off % 8 != 0 || f.index_off < sizeof h ||
        f.index_off + (uint64_t)f.ngranules * entry + sizeof f != size)
        goto fail;
    if (xxh64(p->map + f.index_off, (size_t)f.ngranules * entry, 0) != f.index_hash)
        goto fail;
    if (h.version == 1) {
        p->g_owned = malloc((f.ngranules ? f.ngranules : 1) * sizeof(granule));
        if (!p->g_owned) {
            rc = TSDB_ERR_NOMEM;
            goto fail;
        }
        for (uint32_t i = 0; i < f.ngranules; i++) {
            granule_v1 o;
            memcpy(&o, p->map + f.index_off + (size_t)i * sizeof o, sizeof o);
            granule *g = &p->g_owned[i];
            memset(g, 0, sizeof *g);
            g->series = o.series;
            g->count = o.count;
            g->ts_min = o.ts_min;
            g->ts_max = o.ts_max;
            g->off = o.off;
            g->ts_len = o.ts_len;
            g->val_len = o.val_len;
            g->crc = o.crc;
            g->fsum = o.sum;
            g->fmin = o.vmin;
            g->fmax = o.vmax;
        }
        p->g = p->g_owned;
    } else {
        p->g = (const granule *)(p->map + f.index_off);
    }
    p->ng = f.ngranules;
    p->index_off = f.index_off;
    p->rows = f.rows;
    p->min_ts = f.min_ts;
    p->max_ts = f.max_ts;
    atomic_init(&p->refs, 1);
    atomic_init(&p->obsolete, false);
    *out = p;
    return 0;
fail:
    munmap((void *)p->map, p->map_len);
    free(p->g_owned);
    free(p);
    return rc;
}

void part_ref(part *p) { atomic_fetch_add_explicit(&p->refs, 1, memory_order_relaxed); }

void part_unref(part *p)
{
    if (!p || atomic_fetch_sub_explicit(&p->refs, 1, memory_order_acq_rel) != 1)
        return;
    munmap((void *)p->map, p->map_len);
    if (atomic_load(&p->obsolete))
        unlinkat(p->dirfd, p->name, 0);
    free(p->g_owned);
    free(p);
}

int granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v)
{
    uint64_t len = (uint64_t)g->ts_len + g->val_len;
    if (g->off < sizeof(part_header) || g->off + len > p->index_off || g->count == 0 ||
        g->count > p->grows)
        return TSDB_ERR_CORRUPT;
    const uint8_t *blk = p->map + g->off;
    if ((uint32_t)xxh64(blk, len, 0) != g->crc)
        return TSDB_ERR_CORRUPT;
    QS_ADD(rows_decoded, g->count);
    QS_ADD(bytes_read, len);
    int rc = block_decode_ts(c, blk, g->ts_len, g->count, ts);
    if (rc == 0 && v)
        rc = block_decode_vals(c, type, blk + g->ts_len, g->val_len, g->count, v);
    return rc;
}

/* ---------------------------------------------------------------- writer */

int pw_begin(part_writer *w, struct eng *db, int64_t partition, uint64_t wal_seq)
{
    memset(w, 0, sizeof *w);
    w->db = db;
    w->fd = -1;
    w->partition = partition;
    w->wal_seq = wal_seq;
    w->grows = db->granule_rows;
    w->zstd_level = db->opt.zstd_level;
    w->value_codec = db->opt.value_codec;
    w->min_ts = INT64_MAX;
    w->max_ts = INT64_MIN;
    w->cap = PW_FLUSH_BYTES + 2 * block_bound(w->grows) + sizeof(part_header) + 8;
    w->buf = malloc(w->cap);
    w->pend_ts = malloc(w->grows * sizeof(int64_t));
    w->pend_v = malloc(w->grows * sizeof(uint64_t)); /* the widest slot */
    w->cc = cc_get(db);
    if (!w->buf || !w->pend_ts || !w->pend_v || !w->cc) {
        pw_abort(w);
        return TSDB_ERR_NOMEM;
    }
    snprintf(w->tmp_name, sizeof w->tmp_name, "tmp_%d_%u.part", (int)getpid(),
             atomic_fetch_add(&db->tmp_counter, 1));
    w->fd = openat(db->dirfd, w->tmp_name, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (w->fd < 0) {
        pw_abort(w);
        return TSDB_ERR_IO;
    }
    part_header h;
    memcpy(h.magic, PART_MAGIC, 8);
    h.version = FORMAT_VERSION;
    h.granule_rows = w->grows;
    h.wal_seq = w->wal_seq;
    memcpy(w->buf, &h, sizeof h);
    w->len = sizeof h;
    return 0;
}

static int pw_flush(part_writer *w)
{
    int rc = write_all(w->fd, w->buf, w->len);
    w->off += w->len;
    w->len = 0;
    return rc;
}

static int pw_push_index(part_writer *w, const granule *g)
{
    if (w->ng == w->idx_cap) {
        size_t cap = w->idx_cap ? w->idx_cap * 2 : 64;
        granule *idx = realloc(w->idx, cap * sizeof *idx);
        if (!idx)
            return TSDB_ERR_NOMEM;
        w->idx = idx;
        w->idx_cap = cap;
    }
    w->idx[w->ng++] = *g;
    w->rows += g->count;
    if (g->ts_min < w->min_ts)
        w->min_ts = g->ts_min;
    if (g->ts_max > w->max_ts)
        w->max_ts = g->ts_max;
    return w->len >= PW_FLUSH_BYTES ? pw_flush(w) : 0;
}

static int pw_emit(part_writer *w, uint32_t series, int type, const int64_t *ts, const void *v, size_t n)
{
    granule g;
    memset(&g, 0, sizeof g);
    uint8_t *start = w->buf + w->len;
    size_t tl = block_encode_ts(w->cc, w->zstd_level, ts, n, start);
    size_t vl = tl ? block_encode_vals(w->cc, w->zstd_level, w->value_codec, type, v, n, start + tl) : 0;
    if (!vl)
        return TSDB_ERR_NOMEM;
    granule_stats(type, v, n, &g);
    g.series = series;
    g.count = (uint32_t)n;
    g.ts_min = ts[0];
    g.ts_max = ts[n - 1];
    g.off = w->off + w->len;
    g.ts_len = (uint32_t)tl;
    g.val_len = (uint32_t)vl;
    g.crc = (uint32_t)xxh64(start, tl + vl, 0);
    w->len += tl + vl;
    return pw_push_index(w, &g);
}

static int pw_emit_pending(part_writer *w)
{
    if (!w->pend_n)
        return 0;
    int rc = pw_emit(w, w->pend_series, w->pend_type, w->pend_ts, w->pend_v, w->pend_n);
    w->pend_n = 0;
    return rc;
}

/* Rows must be in (series, ts) order across calls. Granules never span series. */
int pw_add(part_writer *w, uint32_t series, int type, const int64_t *ts, const void *v, size_t n)
{
    int rc;
    size_t wd = type_width(type);
    const uint8_t *vb = v;
    if (w->pend_n && w->pend_series != series && (rc = pw_emit_pending(w)) != 0)
        return rc;
    while (n) {
        if (!w->pend_n && n >= w->grows) {
            if ((rc = pw_emit(w, series, type, ts, vb, w->grows)) != 0)
                return rc;
            ts += w->grows;
            vb += w->grows * wd;
            n -= w->grows;
            continue;
        }
        size_t take = MIN(n, w->grows - w->pend_n);
        memcpy(w->pend_ts + w->pend_n, ts, take * sizeof *ts);
        memcpy(w->pend_v + w->pend_n * wd, vb, take * wd);
        w->pend_series = series;
        w->pend_type = type;
        w->pend_n += take;
        ts += take;
        vb += take * wd;
        n -= take;
        if (w->pend_n == w->grows && (rc = pw_emit_pending(w)) != 0)
            return rc;
    }
    return 0;
}

/* Copies an already encoded granule verbatim. Any pending rows are emitted
 * first, so callers only do this when the pending rows belong to an earlier series. */
int pw_copy_granule(part_writer *w, const part *src, const granule *g)
{
    int rc = pw_emit_pending(w);
    if (rc)
        return rc;
    size_t len = (size_t)g->ts_len + g->val_len;
    if (g->off + len > src->index_off)
        return TSDB_ERR_CORRUPT;
    if (w->len + len > w->cap && (rc = pw_flush(w)) != 0)
        return rc;
    granule ng = *g;
    ng.off = w->off + w->len;
    if (len > w->cap) {
        /* cannot happen with matching granule sizes, but stay safe */
        if ((rc = write_all(w->fd, src->map + g->off, len)) != 0)
            return rc;
        w->off += len;
    } else {
        memcpy(w->buf + w->len, src->map + g->off, len);
        w->len += len;
    }
    return pw_push_index(w, &ng);
}

void pw_abort(part_writer *w)
{
    if (w->fd >= 0) {
        close(w->fd);
        unlinkat(w->db->dirfd, w->tmp_name, 0);
    }
    if (w->cc)
        cc_put(w->db, w->cc);
    free(w->buf);
    free(w->idx);
    free(w->pend_ts);
    free(w->pend_v);
    memset(w, 0, sizeof *w);
    w->fd = -1;
}

static int pw_finish_mode(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, bool pending,
                          part **out)
{
    *out = NULL;
    eng *db = w->db;
    int rc = pw_emit_pending(w);
    if (rc)
        goto fail;
    if (w->ng == 0) {
        pw_abort(w);
        return 0;
    }
    while ((w->off + w->len) % 8)
        w->buf[w->len++] = 0;
    part_footer f;
    memset(&f, 0, sizeof f);
    f.index_off = w->off + w->len;
    f.rows = w->rows;
    f.min_ts = w->min_ts;
    f.max_ts = w->max_ts;
    f.ngranules = (uint32_t)w->ng;
    f.version = FORMAT_VERSION;
    f.index_hash = xxh64(w->idx, w->ng * sizeof(granule), 0);
    memcpy(f.magic, PART_END_MAGIC, 8);
    if ((rc = pw_flush(w)) != 0 || (rc = write_all(w->fd, w->idx, w->ng * sizeof(granule))) != 0 ||
        (rc = write_all(w->fd, &f, sizeof f)) != 0)
        goto fail;
    if (db->opt.fsync && fdatasync(w->fd) != 0) {
        rc = TSDB_ERR_IO;
        goto fail;
    }
    char name[96];
    snprintf(name, sizeof name, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_%u.part", w->partition, min_block,
             max_block, level);
    if (pending) {
        close(w->fd);
        w->fd = -1; /* the tmp file stays */
        rc = part_open_as(db->dirfd, w->tmp_name, name, out);
        if (rc == 0)
            snprintf((*out)->pending, sizeof (*out)->pending, "%s", w->tmp_name);
        else
            unlinkat(db->dirfd, w->tmp_name, 0);
        pw_abort(w);
        return rc;
    }
    if (renameat(db->dirfd, w->tmp_name, db->dirfd, name) != 0) {
        rc = TSDB_ERR_IO;
        goto fail;
    }
    close(w->fd);
    w->fd = -1;
    if (db->opt.fsync)
        fsync(db->dirfd);
    rc = part_open(db->dirfd, name, out);
    if (rc != 0)
        unlinkat(db->dirfd, name, 0);
    pw_abort(w);
    return rc;
fail:
    pw_abort(w);
    return rc;
}

int pw_finish(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out)
{
    return pw_finish_mode(w, min_block, max_block, level, false, out);
}

int pw_finish_pending(part_writer *w, uint64_t min_block, uint64_t max_block, uint32_t level, part **out)
{
    return pw_finish_mode(w, min_block, max_block, level, true, out);
}

/* ----------------------------------------------------- atomic commits */
/*
 * One insert can make several parts (one per partition). They are written
 * and fsync'd under tmp names; then an intent file listing the renames is
 * made durable, which is the commit point; then the renames happen and the
 * intent is removed. After a crash, an intent that is complete (it ends with
 * "end") is rolled forward; tmp files without one are deleted at open. So an
 * insert is all or nothing.
 */

#define INTENT_PREFIX "txn_"

void parts_discard(eng *db, part **ps, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (ps[i]->pending[0])
            unlinkat(db->dirfd, ps[i]->pending, 0);
        else
            atomic_store(&ps[i]->obsolete, true);
        part_unref(ps[i]);
    }
}

int parts_commit(eng *db, part **ps, size_t n)
{
    if (n == 0)
        return 0;
    /* block numbers in commit order: the caller publishes before anyone else commits */
    for (size_t i = 0; i < n; i++) {
        part *p = ps[i];
        p->min_block = p->max_block = atomic_fetch_add(&db->next_block, 1);
        p->level = 0;
        snprintf(p->name, sizeof p->name, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_0.part", p->partition, p->min_block,
                 p->max_block);
    }
    char intent[64] = "";
    if (n > 1) {
        size_t cap = 32 + n * 200, len = 0;
        char *buf = malloc(cap);
        if (!buf)
            return TSDB_ERR_NOMEM;
        len += (size_t)snprintf(buf + len, cap - len, "tsdb-intent 1\n");
        for (size_t i = 0; i < n; i++)
            len += (size_t)snprintf(buf + len, cap - len, "%s %s\n", ps[i]->pending, ps[i]->name);
        len += (size_t)snprintf(buf + len, cap - len, "end\n");
        snprintf(intent, sizeof intent, INTENT_PREFIX "%d_%u.intent", (int)getpid(),
                 atomic_fetch_add(&db->tmp_counter, 1));
        int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        int rc = fd < 0 || write_all(fd, buf, len) != 0 || (db->opt.fsync && fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
        if (fd >= 0)
            close(fd);
        free(buf);
        if (rc == 0 && db->opt.fsync && fsync(db->dirfd) != 0) /* the intent's name is the commit */
            rc = TSDB_ERR_IO;
        if (rc) {
            unlinkat(db->dirfd, intent, 0);
            return rc;
        }
    }
    for (size_t i = 0; i < n; i++) {
        if (renameat(db->dirfd, ps[i]->pending, db->dirfd, ps[i]->name) != 0 && !intent[0])
            return TSDB_ERR_IO; /* a single part: nothing happened */
        ps[i]->pending[0] = '\0';
    }
    if (db->opt.fsync)
        fsync(db->dirfd);
    if (intent[0])
        unlinkat(db->dirfd, intent, 0);
    return 0;
}

/* At open: finish committed inserts that a crash interrupted. */
int intents_recover(eng *db)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    struct dirent *e;
    bool any = false;
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, INTENT_PREFIX, strlen(INTENT_PREFIX)) != 0)
            continue;
        int fd = openat(db->dirfd, e->d_name, O_RDONLY | O_CLOEXEC);
        struct stat st;
        char *buf = NULL;
        if (fd >= 0 && fstat(fd, &st) == 0 && (buf = malloc((size_t)st.st_size + 1)) &&
            pread(fd, buf, (size_t)st.st_size, 0) == st.st_size) {
            buf[st.st_size] = '\0';
            size_t len = (size_t)st.st_size;
            bool complete = len >= 4 && strcmp(buf + len - 4, "end\n") == 0 && strncmp(buf, "tsdb-intent 1\n", 14) == 0;
            for (char *line = buf + 14, *next; complete && *line; line = next) {
                next = strchr(line, '\n');
                if (!next)
                    break;
                *next++ = '\0';
                char tmp[96], fin[128];
                if (sscanf(line, "%95s %127s", tmp, fin) == 2 && faccessat(db->dirfd, tmp, F_OK, 0) == 0)
                    renameat(db->dirfd, tmp, db->dirfd, fin); /* roll forward */
            }
        }
        free(buf);
        if (fd >= 0)
            close(fd);
        unlinkat(db->dirfd, e->d_name, 0);
        any = true;
    }
    closedir(d);
    if (any && db->opt.fsync)
        fsync(db->dirfd);
    return 0;
}
