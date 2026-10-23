/*
 * One writer, any number of followers: on one host, or on several hosts that
 * share the directory over NFS.
 *
 * The writer describes the database in MANIFEST after every change of the
 * part set: a version, the active parts, the number of series, the newest WAL
 * segment that is entirely in parts, and the active segment. Followers poll
 * it and catch up: open new parts, drop removed ones, read new series names,
 * and tail the WAL segments that are not in parts yet into memtables of their
 * own. Dropping the memtable of a segment happens in the same step as adding
 * the parts that hold its rows, so a query never sees a row twice or misses one.
 *
 * NFS needs a few things that a local disk does not:
 *  - MANIFEST is rewritten in place and carries a checksum, never renamed: a
 *    reopened file is revalidated (close-to-open consistency), a name looked
 *    up again may come from the directory cache. A torn read fails the
 *    checksum and is retried at the next poll.
 *  - A file deleted on the server is gone for every other host, mmap'd or not
 *    (reading it raises SIGBUS). Parts replaced by a merge are therefore
 *    renamed to retired_<name> at once and deleted obsolete_part_lifetime_s
 *    later; renaming does not disturb hosts that have them open.
 *  - Followers find new files by name; mount them with lookupcache=positive so
 *    a recently created part or segment is not hidden by a cached "not found".
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#define MANIFEST_FILE "MANIFEST"
#define RETIRED_PREFIX "retired_"
#define WAL_ROW 16
#define WAL_HDR 8

static int64_t now_s(void) { return (int64_t)time(NULL); }

/* ------------------------------------------------------------- writer */

int manifest_publish(eng *db)
{
    if (db->opt.read_only)
        return 0;
    pthread_mutex_lock(&db->manifest_lock);
    pthread_mutex_lock(&db->lock);
    size_t cap = 256 + db->nparts * 112, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        pthread_mutex_unlock(&db->lock);
        pthread_mutex_unlock(&db->manifest_lock);
        return TSDB_ERR_NOMEM;
    }
    len += (size_t)snprintf(buf + len, cap - len,
                            "tsdb-manifest %d\nversion %" PRIu64 "\nseries %u\nwal_flushed %" PRIu64
                            "\nwal_active %" PRIu64 "\nparts %zu\n",
                            FORMAT_VERSION, ++db->manifest_version, atomic_load(&db->series.count), db->wal_flushed,
                            db->wal_enabled ? db->wal_seq : db->wal_flushed, db->nparts);
    for (size_t i = 0; i < db->nparts; i++)
        len += (size_t)snprintf(buf + len, cap - len, "%s\n", db->parts[i]->name);
    pthread_mutex_unlock(&db->lock);
    len += (size_t)snprintf(buf + len, cap - len, "checksum %016" PRIx64 "\n", xxh64(buf, len, 0));

    /* in place, never renamed: see the comment at the top */
    int rc = 0;
    int fd = openat(db->dirfd, MANIFEST_FILE, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0 || pwrite(fd, buf, len, 0) != (ssize_t)len || ftruncate(fd, (off_t)len) != 0)
        rc = TSDB_ERR_IO;
    if (fd >= 0 && close(fd) != 0) /* on NFS, close sends the data to the server */
        rc = TSDB_ERR_IO;
    free(buf);
    pthread_mutex_unlock(&db->manifest_lock);
    return rc;
}

/* Takes p out of service. It stays readable under its retired name until its
 * lifetime has passed, for followers that still use it. Caller holds db->lock. */
void db_retire_locked(eng *db, part *p)
{
    char retired[sizeof p->name + sizeof RETIRED_PREFIX];
    snprintf(retired, sizeof retired, RETIRED_PREFIX "%s", p->name);
    if (db->opt.obsolete_part_lifetime_s <= 0) {
        atomic_store(&p->obsolete, true); /* deleted when the last reference goes */
    } else if (renameat(db->dirfd, p->name, db->dirfd, retired) == 0) {
        if (db->ngraveyard == db->graveyard_cap) {
            size_t cap = db->graveyard_cap ? db->graveyard_cap * 2 : 64;
            graveyard_entry *g = realloc(db->graveyard, cap * sizeof *g);
            if (g) {
                db->graveyard = g;
                db->graveyard_cap = cap;
            }
        }
        if (db->ngraveyard < db->graveyard_cap) {
            graveyard_entry *e = &db->graveyard[db->ngraveyard++];
            snprintf(e->name, sizeof e->name, "%s", retired);
            e->deadline = now_s() + db->opt.obsolete_part_lifetime_s;
        }
        /* an unrecorded one is collected at the next open */
    }
    db_parts_remove_locked(db, p);
}

/* Deletes retired parts whose lifetime is over (or all of them). */
void graveyard_purge(eng *db, bool all)
{
    int64_t now = now_s();
    pthread_mutex_lock(&db->lock);
    size_t kept = 0;
    for (size_t i = 0; i < db->ngraveyard; i++) {
        graveyard_entry *e = &db->graveyard[i];
        if (all || e->deadline <= now)
            unlinkat(db->dirfd, e->name, 0);
        else
            db->graveyard[kept++] = *e;
    }
    db->ngraveyard = kept;
    pthread_mutex_unlock(&db->lock);
}

/* ------------------------------------------------------------ follower */

typedef struct manifest {
    uint64_t version, wal_flushed, wal_active;
    uint32_t series;
    char **parts;
    size_t nparts;
    char *text;
} manifest;

static void manifest_free(manifest *m)
{
    free(m->parts);
    free(m->text);
    memset(m, 0, sizeof *m);
}

/* TSDB_ERR_NOTFOUND: no MANIFEST; TSDB_ERR_CORRUPT: torn or damaged, retry later. */
static int manifest_read(eng *db, manifest *m)
{
    memset(m, 0, sizeof *m);
    int fd = openat(db->dirfd, MANIFEST_FILE, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size;
    m->text = malloc(size + 1);
    ssize_t got = m->text ? pread(fd, m->text, size, 0) : -1;
    close(fd);
    if (got != (ssize_t)size) {
        manifest_free(m);
        return m->text ? TSDB_ERR_CORRUPT : TSDB_ERR_NOMEM;
    }
    m->text[size] = '\0';
    /* the last line is "checksum <hex>" over everything before it */
    char *ck = size > 1 ? memrchr(m->text, '\n', size - 1) : NULL;
    char *body_end = ck ? ck + 1 : m->text;
    uint64_t want;
    if (!ck || sscanf(body_end, "checksum %" SCNx64, &want) != 1 ||
        xxh64(m->text, (size_t)(body_end - m->text), 0) != want) {
        manifest_free(m);
        return TSDB_ERR_CORRUPT;
    }
    *body_end = '\0';
    int format = 0;
    size_t nparts = 0;
    char *line = m->text, *next;
    int fields = 0;
    for (; line && *line; line = next) {
        next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        if (fields < 6) {
            fields += sscanf(line, "tsdb-manifest %d", &format) == 1;
            fields += sscanf(line, "version %" SCNu64, &m->version) == 1;
            fields += sscanf(line, "series %u", &m->series) == 1;
            fields += sscanf(line, "wal_flushed %" SCNu64, &m->wal_flushed) == 1;
            fields += sscanf(line, "wal_active %" SCNu64, &m->wal_active) == 1;
            if (sscanf(line, "parts %zu", &nparts) == 1) {
                fields++;
                m->parts = calloc(nparts + 1, sizeof *m->parts);
                if (!m->parts) {
                    manifest_free(m);
                    return TSDB_ERR_NOMEM;
                }
            }
            continue;
        }
        if (m->nparts < nparts)
            m->parts[m->nparts++] = line;
    }
    if (fields != 6 || format != FORMAT_VERSION || m->nparts != nparts) {
        manifest_free(m);
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

static int cmp_part_name(const void *a, const void *b)
{
    return strcmp((*(part *const *)a)->name, (*(part *const *)b)->name);
}

static int cmp_part_order(const void *x, const void *y)
{
    const part *a = *(part *const *)x, *b = *(part *const *)y;
    if (a->partition != b->partition)
        return a->partition < b->partition ? -1 : 1;
    if (a->min_block != b->min_block)
        return a->min_block < b->min_block ? -1 : 1;
    return (a->max_block < b->max_block) - (a->max_block > b->max_block);
}

/* Opens a part by its active name or, if the writer has just retired it, by its retired name. */
static int open_listed_part(eng *db, const char *name, part **out)
{
    int rc = part_open(db->dirfd, name, out);
    if (rc == TSDB_ERR_NOTFOUND) {
        char retired[256];
        snprintf(retired, sizeof retired, RETIRED_PREFIX "%s", name);
        rc = part_open_as(db->dirfd, retired, name, out);
    }
    return rc;
}

/* Reads new records of one WAL segment into its memtable. */
static int tail_segment(eng *db, wal_tail *t)
{
    char name[64];
    snprintf(name, sizeof name, "wal_%016" PRIx64 ".log", t->seq);
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC); /* reopened: revalidates the size on NFS */
    if (fd < 0)
        return 0; /* not created yet, or already flushed and deleted: the manifest catches up */
    struct stat st;
    if (fstat(fd, &st) != 0 || (uint64_t)st.st_size <= t->off) {
        close(fd);
        return 0;
    }
    size_t len = (size_t)((uint64_t)st.st_size - t->off);
    uint8_t *buf = malloc(len);
    ssize_t got = buf ? pread(fd, buf, len, (off_t)t->off) : -1;
    close(fd);
    if (got <= 0) {
        free(buf);
        return buf ? 0 : TSDB_ERR_NOMEM;
    }
    size_t pos = 0;
    int rc = 0;
    bool reloaded = false;
    while (rc == 0 && pos + WAL_HDR <= (size_t)got) {
        size_t n = load32(buf + pos);
        if (pos + WAL_HDR + n * WAL_ROW > (size_t)got ||
            (uint32_t)xxh64(buf + pos + WAL_HDR, n * WAL_ROW, 0) != load32(buf + pos + 4))
            break; /* the writer is still writing this record: next poll */
        const uint8_t *r = buf + pos + WAL_HDR;
        for (size_t i = 0; i < n && rc == 0; i++, r += WAL_ROW) {
            uint32_t id;
            int64_t ts;
            float v;
            memcpy(&id, r, 4);
            memcpy(&ts, r + 4, 8);
            memcpy(&v, r + 12, 4);
            if (id >= eng_series_count(db) && !reloaded) {
                series_reload(&db->series, db->dirfd); /* created after our last look */
                reloaded = true;
            }
            if (id >= eng_series_count(db))
                continue;
            pthread_mutex_lock(&db->buf_lock);
            rc = mt_add(t->m, NULL, id, &ts, &v, 1, 0);
            pthread_mutex_unlock(&db->buf_lock);
        }
        pos += WAL_HDR + n * WAL_ROW;
    }
    t->off += pos;
    free(buf);
    return rc;
}

int follow_refresh(eng *db)
{
    if (!db->opt.read_only)
        return 0;
    pthread_mutex_lock(&db->refresh_lock);
    manifest m;
    int rc = manifest_read(db, &m);
    if (rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_CORRUPT) {
        pthread_mutex_unlock(&db->refresh_lock);
        return 0; /* no writer yet, or caught mid-write: next time */
    }
    if (rc) {
        pthread_mutex_unlock(&db->refresh_lock);
        return rc;
    }
    if (m.version != db->follow_version) {
        series_reload(&db->series, db->dirfd);
        /* the new part set: parts we have stay, new ones are opened */
        size_t ncur;
        part **cur = db_snapshot(db, INT64_MIN, INT64_MAX, &ncur);
        part **next = calloc(m.nparts + 1, sizeof *next);
        size_t nnext = 0;
        if (!cur || !next)
            rc = TSDB_ERR_NOMEM;
        if (rc == 0 && ncur)
            qsort(cur, ncur, sizeof *cur, cmp_part_name);
        for (size_t i = 0; rc == 0 && i < m.nparts; i++) {
            part key, *kp = &key, **found = NULL;
            snprintf(key.name, sizeof key.name, "%s", m.parts[i]);
            if (ncur)
                found = bsearch(&kp, cur, ncur, sizeof *cur, cmp_part_name);
            if (found) {
                part_ref(*found);
                next[nnext++] = *found;
                continue;
            }
            part *p;
            int r = open_listed_part(db, m.parts[i], &p);
            if (r == 0 && p->grows == db->granule_rows) {
                next[nnext++] = p;
            } else {
                if (r == 0)
                    part_unref(p);
                rc = TSDB_ERR_STOPPED; /* gone before we opened it: newer manifest next time */
            }
        }
        if (rc == 0) {
            if (nnext)
                qsort(next, nnext, sizeof *next, cmp_part_order);
            memtable *dropped[MAX_TAILS];
            size_t ndropped = 0;
            pthread_mutex_lock(&db->lock);
            pthread_mutex_lock(&db->buf_lock);
            part **old = db->parts;
            size_t nold = db->nparts;
            db->parts = next;
            db->nparts = nnext;
            db->parts_cap = m.nparts + 1;
            /* segments now held by parts leave in the same step */
            size_t kept = 0;
            for (size_t i = 0; i < db->ntails; i++) {
                if (db->tails[i].seq <= m.wal_flushed)
                    dropped[ndropped++] = db->tails[i].m;
                else
                    db->tails[kept++] = db->tails[i];
            }
            db->ntails = kept;
            db->follow_version = m.version;
            db->follow_flushed = m.wal_flushed;
            db->follow_active = m.wal_active;
            pthread_mutex_unlock(&db->buf_lock);
            pthread_mutex_unlock(&db->lock);
            for (size_t i = 0; i < nold; i++)
                part_unref(old[i]);
            free(old);
            for (size_t i = 0; i < ndropped; i++)
                mt_unref(dropped[i]);
            next = NULL;
        } else {
            for (size_t i = 0; i < nnext; i++)
                part_unref(next[i]);
        }
        free(next);
        db_release(cur, ncur);
    }
    manifest_free(&m);
    if (rc == TSDB_ERR_STOPPED)
        rc = 0;

    /* rows of the segments the writer has not flushed yet */
    for (uint64_t seq = db->follow_flushed + 1; rc == 0 && seq <= db->follow_active && seq; seq++) {
        wal_tail *t = NULL;
        for (size_t i = 0; i < db->ntails; i++)
            if (db->tails[i].seq == seq)
                t = &db->tails[i];
        if (!t) {
            if (db->ntails == MAX_TAILS)
                break;
            memtable *mt = mt_new(seq);
            if (!mt) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            pthread_mutex_lock(&db->lock);
            pthread_mutex_lock(&db->buf_lock);
            t = &db->tails[db->ntails++];
            *t = (wal_tail){seq, mt, 0};
            pthread_mutex_unlock(&db->buf_lock);
            pthread_mutex_unlock(&db->lock);
        }
        rc = tail_segment(db, t);
    }
    pthread_mutex_unlock(&db->refresh_lock);
    return rc;
}

int eng_refresh(eng *db) { return follow_refresh(db); }

void *follower_main(void *arg)
{
    eng *db = arg;
    int ms = db->opt.refresh_ms > 0 ? db->opt.refresh_ms : 100;
    pthread_mutex_lock(&db->buf_lock);
    while (!db->follower_stop) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += ms / 1000;
        ts.tv_nsec += (ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&db->flush_cv, &db->buf_lock, &ts);
        if (db->follower_stop)
            break;
        pthread_mutex_unlock(&db->buf_lock);
        int rc = follow_refresh(db);
        if (rc)
            fprintf(stderr, "tsdb: refresh: %s\n", tsdb_strerror(rc));
        pthread_mutex_lock(&db->buf_lock);
    }
    pthread_mutex_unlock(&db->buf_lock);
    return NULL;
}
