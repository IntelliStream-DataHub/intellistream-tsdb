/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Engine young-part and .ec file scrubber (src/scrub.h; R12, r12-young).
 */
#include "scrub.h"
#include "ecpart.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int scrub_test_mutant;

#define EC_PART_MAGIC "TSDBPART"
#define EC_PART_END_MAGIC "TSDBEND1"
#define EC_TOMB_MAGIC 0x424d4f54u

static uint32_t le32(const uint8_t *b)
{
    return (uint32_t)b[0] | ((uint32_t)b[1] << 8) | ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}

static uint64_t le64(const uint8_t *b)
{
    return (uint64_t)le32(b) | ((uint64_t)le32(b + 4) << 32);
}

static int pread_exact(int fd, void *buf, size_t len, uint64_t off)
{
    uint8_t *p = buf;
    size_t done = 0;
    while (done < len) {
        ssize_t r = pread(fd, p + done, len - done, (off_t)(off + done));
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return TSDB_ERR_IO;
        }
        if (r == 0)
            return TSDB_ERR_CORRUPT; /* unexpected EOF / truncated file */
        done += (size_t)r;
    }
    return 0;
}

static uint64_t ec_src_hash(const uint8_t *h, size_t hlen, const uint8_t *ib, size_t ilen, size_t tlen,
                            const uint8_t *f)
{
    uint64_t x = xxh64(h, hlen, 0);
    x = xxh64(ib, ilen, x);
    if (tlen)
        x = xxh64(ib + ilen, tlen, x);
    return xxh64(f, offsetof(part_footer, index_hash), x);
}

static bool ec_tombs_ok(const uint8_t *buf, size_t tlen, const tomb **t_out, uint32_t *nt_out)
{
    *t_out = NULL;
    *nt_out = 0;
    if (tlen == 0)
        return true;
    if (tlen < 8 || (uintptr_t)buf % _Alignof(tomb) != 0)
        return false;
    uint32_t cnt, magic;
    memcpy(&cnt, buf + tlen - 8, 4);
    memcpy(&magic, buf + tlen - 4, 4);
    if (magic != EC_TOMB_MAGIC || cnt == 0 || (uint64_t)cnt * sizeof(tomb) + 8 != tlen)
        return false;
    const tomb *x = (const tomb *)buf;
    for (uint32_t i = 0; i < cnt; i++) {
        if (x[i].start >= x[i].end)
            return false;
        if (i && (x[i].series < x[i - 1].series || (x[i].series == x[i - 1].series && x[i].start < x[i - 1].end)))
            return false;
    }
    *t_out = x;
    *nt_out = cnt;
    return true;
}

static bool ec_source_name(const char *name, char out[96])
{
    size_t n = strlen(name);
    if (n < 3 || strcmp(name + n - 3, ".ec") != 0 || n - 3 + 6 > 96)
        return false;
    memset(out, 0, 96);
    memcpy(out, name, n - 3);
    memcpy(out + n - 3, ".part", 5);
    return true;
}

/* Verifies an .ec file on disk via pread (never touching p->map unless mutant SCRUB_MUT_MMAP is active),
 * checking trailer, object record, source header/footer/index/tombstones and granule layout just as part_open_ec_at
 * does. Returns 0, TSDB_ERR_NOTFOUND (ENOENT on open), TSDB_ERR_CORRUPT, TSDB_ERR_IO, or TSDB_ERR_NOMEM. */
static int ec_scrub_pread(const eng *db, const part *p, const struct ecpart *e, uint64_t *bytes_out)
{
    *bytes_out = 0;
    const size_t HL = sizeof(part_header), FL = sizeof(part_footer);
    size_t size = p->map_len;
    if (size < HL + FL + EC_REC_FIXED + EC_TRAILER_BYTES)
        return TSDB_ERR_CORRUPT;

    uint8_t *buf = NULL;
    const uint8_t *src = NULL;
    bool use_mmap = false;
#ifdef TSDB_TEST_HOOKS
    if (scrub_test_mutant == SCRUB_MUT_MMAP)
        use_mmap = true;
#endif
    if (use_mmap) {
        src = p->map;
    } else {
        int fd = openat(p->dirfd, p->name, O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            if (errno == ENOENT)
                return TSDB_ERR_NOTFOUND;
            if (errno == EMFILE || errno == ENFILE || errno == ENOMEM)
                return TSDB_ERR_NOMEM; /* transient resource pressure: retry, never mark part damaged */
            return TSDB_ERR_IO;
        }
        struct stat st;
        if (fstat(fd, &st) != 0) {
            close(fd);
            return TSDB_ERR_IO;
        }
        if (st.st_size < 0 || (size_t)st.st_size != size) {
            close(fd);
            return TSDB_ERR_CORRUPT;
        }
        buf = malloc(size + 8);
        if (!buf) {
            close(fd);
            return TSDB_ERR_NOMEM;
        }
        int rc = pread_exact(fd, buf, size, 0);
        close(fd);
        if (rc != 0) {
            free(buf);
            return rc;
        }
        memset(buf + size, 0, 8);
        src = buf;
    }

    *bytes_out = (uint64_t)size;
    int rc = TSDB_ERR_CORRUPT;
    ec_record rec;
    memset(&rec, 0, sizeof rec);
    uint8_t *ib_copy = NULL;

    /* Trailer */
    const uint8_t *tr = src + size - EC_TRAILER_BYTES;
    static const uint8_t zero16[16];
    if (memcmp(tr + EC_TR_MAGIC, EC_TRAILER_MAGIC, 8) != 0 || le32(tr + EC_TR_VERSION) != EC_TRAILER_VERSION ||
        le32(tr + EC_TR_REC_VERSION) != EC_RECORD_VERSION || memcmp(tr + EC_TR_RESERVED, zero16, 16) != 0 ||
        xxh64(tr, EC_TR_HASH, 0) != le64(tr + EC_TR_HASH))
        goto out;
    uint64_t roff = le64(tr + EC_TR_REC_OFF), rlen = le64(tr + EC_TR_REC_LEN);
    if (rlen > size - EC_TRAILER_BYTES || roff != size - EC_TRAILER_BYTES - rlen || roff < HL + FL || roff % 8 != 0)
        goto out;

    /* Record hash and decode */
    uint64_t rhash = xxh64(src + roff, (size_t)rlen, 0);
    if (rhash != le64(tr + EC_TR_REC_HASH) || rhash != e->rec_hash)
        goto out;
    if ((rc = ec_record_decode_as(src + roff, (size_t)rlen, &rec, db->ec_cluster)) != 0)
        goto out;
    rc = TSDB_ERR_CORRUPT;

    char srcname[96];
    if (!ec_source_name(p->name, srcname) || rec.key != p->partition || rec.min_block != p->min_block ||
        rec.max_block != p->max_block || rec.level != p->level || memcmp(rec.source, srcname, 96) != 0)
        goto out;
    if (rec.class_check == EC_CLS_FAILED || (rec.class_check != EC_CLS_PASSED && db->cls_on))
        goto out;

    /* Source header and footer */
    part_header h;
    part_footer f;
    memcpy(&h, src, HL);
    memcpy(&f, src + roff - FL, FL);
    if (memcmp(h.magic, EC_PART_MAGIC, 8) != 0 || memcmp(f.magic, EC_PART_END_MAGIC, 8) != 0 ||
        h.version != FORMAT_VERSION || f.version != h.version || h.granule_rows == 0 ||
        h.granule_rows > MAX_GRANULE_ROWS || h.granule_rows != p->grows)
        goto out;
    if (h.fno_lo > h.fno_hi || (h.fno_lo == 0) != (h.fno_hi == 0) || h.fno_hi >= FNO_BOUND_MAX)
        goto out;
    if (f.index_len % 8 != 0 || f.index_off % 8 != 0 || f.index_len > roff - FL - HL || f.index_off < HL)
        goto out;
    size_t ilen = (size_t)f.index_len, tlen = (size_t)(roff - FL - HL - f.index_len);
    if (rec.object_len < (uint64_t)ilen + tlen + FL || rec.object_len - ilen - tlen - FL != f.index_off)
        goto out;
    if (f.ngranules > f.index_off - HL || f.min_ts > f.max_ts || f.index_hash != p->index_hash ||
        f.ngranules != p->ng || f.rows != p->rows || f.min_ts != p->min_ts || f.max_ts != p->max_ts)
        goto out;
    if (ec_src_hash(src, HL, src + HL, ilen, tlen, src + roff - FL) != f.index_hash)
        goto out;

    /* Aligned copy of index + tombstones with 8 zero trailing bytes for gix_parse / tombs_parse */
    ib_copy = malloc(ilen + tlen + 8);
    if (!ib_copy) {
        rc = TSDB_ERR_NOMEM;
        goto out;
    }
    memcpy(ib_copy, src + HL, ilen + tlen);
    memset(ib_copy + ilen + tlen, 0, 8);
    const tomb *tombs = NULL;
    uint32_t nt = 0;
    gix ix;
    if (!ec_tombs_ok(ib_copy + ilen, tlen, &tombs, &nt) || gix_parse(ib_copy, ilen, f.ngranules, &ix) != 0)
        goto out;

    if (f.ngranules || nt) {
        uint64_t rows = 0, end = HL;
        int64_t lo = INT64_MAX, hi = INT64_MIN;
        for (uint32_t i = 0; i < f.ngranules; i++) {
            uint64_t off = gix_get(&ix, i, GF_OFF);
            uint64_t len = (uint64_t)(uint32_t)gix_get(&ix, i, GF_TSLEN) + (uint32_t)gix_get(&ix, i, GF_VALLEN);
            if (off < end || off > f.index_off || len == 0 || len > f.index_off - off)
                goto out;
            end = off + len;
            rows += (uint32_t)gix_get(&ix, i, GF_COUNT);
            int64_t tmin = (int64_t)gix_get(&ix, i, GF_TSMIN);
            int64_t tmax = (int64_t)((uint64_t)tmin + gix_get(&ix, i, GF_SPAN));
            if (tmin < lo)
                lo = tmin;
            if (tmax > hi)
                hi = tmax;
        }
        for (uint32_t i = 0; i < nt; i++) {
            if (tombs[i].start < lo)
                lo = tombs[i].start;
            if (tombs[i].end - 1 > hi)
                hi = tombs[i].end - 1;
        }
        if (rows != f.rows || lo != f.min_ts || hi != f.max_ts)
            goto out;
    }
    rc = 0;
out:
    free(ib_copy);
    ec_record_free(&rec);
    free(buf);
    return rc;
}

static int part_key_cmp(const part *p, int64_t partition, uint64_t min_block, uint64_t max_block, uint32_t level)
{
    if (p->partition != partition)
        return p->partition < partition ? -1 : 1;
    if (p->min_block != min_block)
        return p->min_block < min_block ? -1 : 1;
    if (p->max_block != max_block)
        return p->max_block > max_block ? -1 : 1; /* matches db->parts wider-first order (src/db.c part_cmp) */
    if (p->level != level)
        return p->level < level ? -1 : 1;
    return 0;
}

static bool part_still_listed(eng *db, const part *p)
{
    bool listed = false;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        if (db->parts[i] == p) {
            listed = !atomic_load_explicit(&p->obsolete, memory_order_acquire);
            break;
        }
    }
    pthread_mutex_unlock(&db->lock);
    return listed;
}

int eng_scrub_step(eng *db, uint64_t budget, scrub_cursor *cursor, scrub_report *report)
{
    if (!db || !cursor || !report)
        return TSDB_ERR_INVALID;
    memset(report, 0, sizeof *report);

    part *p = NULL;
    bool was_priority = false;
    uint32_t g_start = 0;

    pthread_mutex_lock(&db->lock);
    if (cursor->priority[0] != '\0') {
        for (size_t i = 0; i < db->nparts; i++) {
            part *c = db->parts[i];
            if (!atomic_load_explicit(&c->obsolete, memory_order_acquire) &&
                strcmp(c->name, cursor->priority) == 0) {
                p = c;
                part_ref(p);
                was_priority = true;
                g_start = cursor->priority_granule;
                break;
            }
        }
        if (!p) {
            /* Previously damaged priority part vanished before re-scrub (e.g. merged away after restart): report it
             * explicitly as skipped instead of silently advancing to a clean part so scrubd keeps the volume DAMAGED. */
            snprintf(report->part_name, sizeof report->part_name, "%s", cursor->priority);
            cursor->priority[0] = '\0';
            cursor->priority_granule = 0;
            report->skipped = true;
            pthread_mutex_unlock(&db->lock);
            return 0;
        }
    }
    if (!p) {
        if (cursor->started && cursor->next_granule > 0) {
            for (size_t i = 0; i < db->nparts; i++) {
                part *c = db->parts[i];
                if (atomic_load_explicit(&c->obsolete, memory_order_acquire))
                    continue;
                int cmp = part_key_cmp(c, cursor->partition, cursor->min_block, cursor->max_block, cursor->level);
                if (cmp == 0) {
                    p = c;
                    part_ref(p);
                    g_start = cursor->next_granule;
                    break;
                }
                if (cmp > 0)
                    break;
            }
            if (!p)
                cursor->next_granule = 0;
        }
        if (!p) {
            for (size_t i = 0; i < db->nparts; i++) {
                part *c = db->parts[i];
                if (atomic_load_explicit(&c->obsolete, memory_order_acquire))
                    continue;
                if (!cursor->started ||
                    part_key_cmp(c, cursor->partition, cursor->min_block, cursor->max_block, cursor->level) > 0) {
                    p = c;
                    part_ref(p);
                    g_start = 0;
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&db->lock);

    if (!p) {
        report->pass_done = true;
        cursor->started = false;
        cursor->next_granule = 0;
        return 0;
    }

    snprintf(report->part_name, sizeof report->part_name, "%s", p->name);

    /* Never hold any engine lock across I/O below. */
    if (atomic_load_explicit(&p->obsolete, memory_order_acquire)) {
        report->skipped = true;
        if (was_priority) {
            cursor->priority[0] = '\0';
            cursor->priority_granule = 0;
        } else {
            cursor->partition = p->partition;
            cursor->min_block = p->min_block;
            cursor->max_block = p->max_block;
            cursor->level = p->level;
            cursor->next_granule = 0;
            cursor->started = true;
        }
        part_unref(p);
        return 0;
    }

    const struct ecpart *e = atomic_load_explicit(&p->ec, memory_order_acquire);
    uint64_t bytes = 0;
    uint32_t g_next = p->ng;
    int rc;
    if (e && !e->test_mark)
        rc = ec_scrub_pread(db, p, e, &bytes);
    else
        rc = part_scrub_pread(p, g_start, budget, &g_next, &bytes);

    report->bytes = bytes;

    if (rc == TSDB_ERR_NOTFOUND) {
        if (!part_still_listed(db, p)) {
            /* Part was merged, dropped, or settled away concurrently: skip cleanly. */
            report->skipped = true;
            if (was_priority) {
                cursor->priority[0] = '\0';
                cursor->priority_granule = 0;
            } else {
                cursor->partition = p->partition;
                cursor->min_block = p->min_block;
                cursor->max_block = p->max_block;
                cursor->level = p->level;
                cursor->next_granule = 0;
                cursor->started = true;
            }
            part_unref(p);
            return 0;
        }
        /* Still listed and non-obsolete, yet missing on disk: damage! */
        rc = TSDB_ERR_CORRUPT;
    }

    if (rc == TSDB_ERR_NOMEM) {
        report->err = TSDB_ERR_NOMEM;
        part_unref(p);
        return TSDB_ERR_NOMEM;
    }

    if (rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_IO) {
        bool was_damaged = atomic_load_explicit(&p->damaged, memory_order_acquire);
        part_mark_payload(db, p, "scrub");
        report->damaged = 1;
        report->newly_damaged = !was_damaged;
        report->parts_done = 1;
        report->err = rc;
        if (was_priority) {
            cursor->priority[0] = '\0';
            cursor->priority_granule = 0;
        } else {
            cursor->partition = p->partition;
            cursor->min_block = p->min_block;
            cursor->max_block = p->max_block;
            cursor->level = p->level;
            cursor->next_granule = 0;
            cursor->started = true;
        }
        part_unref(p);
        return 0;
    }

    /* rc == 0 */
    if (g_next < p->ng) {
        if (was_priority) {
            cursor->priority_granule = g_next;
        } else {
            cursor->partition = p->partition;
            cursor->min_block = p->min_block;
            cursor->max_block = p->max_block;
            cursor->level = p->level;
            cursor->next_granule = g_next;
            cursor->started = true;
        }
    } else {
        report->parts_done = 1;
        if (was_priority) {
            cursor->priority[0] = '\0';
            cursor->priority_granule = 0;
        } else {
            cursor->partition = p->partition;
            cursor->min_block = p->min_block;
            cursor->max_block = p->max_block;
            cursor->level = p->level;
            cursor->next_granule = 0;
            cursor->started = true;
        }
    }

    part_unref(p);
    return 0;
}
