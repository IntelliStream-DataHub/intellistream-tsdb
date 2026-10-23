/* Database lifecycle, the active parts list, buffered appends and maintenance. */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#define META_FILE "META"

const char *tsdb_strerror(int err)
{
    switch (err) {
    case TSDB_OK: return "ok";
    case TSDB_ERR_IO: return "I/O error";
    case TSDB_ERR_NOMEM: return "out of memory";
    case TSDB_ERR_CORRUPT: return "corrupt data";
    case TSDB_ERR_INVALID: return "invalid argument";
    case TSDB_ERR_LOCKED: return "database is locked by another process";
    case TSDB_ERR_NOTFOUND: return "not found";
    case TSDB_ERR_TOO_MANY_BUCKETS: return "too many buckets";
    case TSDB_ERR_STOPPED: return "stopped";
    case TSDB_ERR_READONLY: return "database is read only";
    default: return "unknown error";
    }
}

void tsdb_options_init(tsdb_options *o)
{
    memset(o, 0, sizeof *o);
    o->partition_ms = 86400000;
    o->granule_rows = 8192;
    o->zstd_level = 1;
    o->fsync = 1;
    o->merge_threads = 2;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    o->query_threads = (int)MIN(MAX(ncpu, 1), 256); /* every logical CPU, SMT included */
    o->buffer_rows = 1u << 20;
    o->flush_interval_ms = 5000;
    o->wal_sync_ms = 0;
    o->wal_max_batch_rows = 65536;
    o->refresh_ms = 100;
    o->obsolete_part_lifetime_s = 480; /* like ClickHouse's old_parts_lifetime */
    o->max_part_bytes = 4ull << 30;
}

/* ------------------------------------------------------------ codec pool */

codec_ctx *cc_get(eng *db)
{
    pthread_mutex_lock(&db->cc_lock);
    codec_ctx *c = db->cc_free;
    if (c)
        db->cc_free = c->next;
    pthread_mutex_unlock(&db->cc_lock);
    return c ? c : codec_new();
}

void cc_put(eng *db, codec_ctx *c)
{
    pthread_mutex_lock(&db->cc_lock);
    c->next = db->cc_free;
    db->cc_free = c;
    pthread_mutex_unlock(&db->cc_lock);
}

/* ------------------------------------------------------------ parts list */

int64_t partition_of(const eng *db, int64_t ts) { return floor_div(ts, db->partition_ms); }

static int part_cmp(const part *a, const part *b)
{
    if (a->partition != b->partition)
        return a->partition < b->partition ? -1 : 1;
    if (a->min_block != b->min_block)
        return a->min_block < b->min_block ? -1 : 1;
    return (a->max_block < b->max_block) - (a->max_block > b->max_block); /* wider first */
}

static int part_qsort_cmp(const void *x, const void *y) { return part_cmp(*(part *const *)x, *(part *const *)y); }

void db_parts_insert_locked(eng *db, part *p)
{
    if (db->nparts == db->parts_cap) {
        size_t cap = db->parts_cap ? db->parts_cap * 2 : 64;
        part **parts = realloc(db->parts, cap * sizeof *parts);
        if (!parts)
            abort(); /* the list must stay consistent; tiny allocation */
        db->parts = parts;
        db->parts_cap = cap;
    }
    size_t lo = 0, hi = db->nparts;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (part_cmp(db->parts[mid], p) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    memmove(db->parts + lo + 1, db->parts + lo, (db->nparts - lo) * sizeof *db->parts);
    db->parts[lo] = p;
    db->nparts++;
}

/* Removes p from the list and drops the list's reference. */
void db_parts_remove_locked(eng *db, part *p)
{
    for (size_t i = 0; i < db->nparts; i++) {
        if (db->parts[i] == p) {
            memmove(db->parts + i, db->parts + i + 1, (db->nparts - i - 1) * sizeof *db->parts);
            db->nparts--;
            part_unref(p);
            return;
        }
    }
}

static size_t partition_part_count_locked(eng *db, int64_t partition)
{
    size_t n = 0;
    for (size_t i = 0; i < db->nparts; i++)
        n += db->parts[i]->partition == partition;
    return n;
}

int db_add_part(eng *db, part *p) { return db_add_parts(db, &p, 1); }

/* Publishes parts together: a query sees all of them or none. */
int db_add_parts(eng *db, part **ps, size_t n)
{
    if (n == 0)
        return 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++)
        db_parts_insert_locked(db, ps[i]);
    pthread_cond_signal(&db->merge_cv);
    pthread_mutex_unlock(&db->lock);
    return manifest_publish(db);
}

/* Waits while a partition has too many parts (parts_to_delay_insert). */
void db_wait_merges(eng *db, int64_t partition)
{
    if (db->nmergers == 0)
        return;
    pthread_mutex_lock(&db->lock);
    while (!atomic_load(&db->stop) && partition_part_count_locked(db, partition) > PARTS_TO_DELAY_INSERT) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&db->merged_cv, &db->lock, &ts);
    }
    pthread_mutex_unlock(&db->lock);
}

part **db_snapshot(eng *db, int64_t start, int64_t end, size_t *n)
{
    *n = 0;
    pthread_mutex_lock(&db->lock);
    part **out = malloc((db->nparts ? db->nparts : 1) * sizeof *out);
    if (out) {
        for (size_t i = 0; i < db->nparts; i++) {
            part *p = db->parts[i];
            if (p->max_ts >= start && p->min_ts < end) {
                part_ref(p);
                out[(*n)++] = p;
            }
        }
    }
    pthread_mutex_unlock(&db->lock);
    return out;
}

void db_release(part **parts, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(parts[i]);
    free(parts);
}

/* ------------------------------------------------------------------ meta */

static int meta_load_or_create(eng *db, bool read_only)
{
    char buf[512];
    int fd = openat(db->dirfd, META_FILE, O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        ssize_t r = read(fd, buf, sizeof buf - 1);
        close(fd);
        if (r <= 0)
            return TSDB_ERR_CORRUPT;
        buf[r] = '\0';
        int version = 0;
        int64_t pms = 0;
        unsigned grows = 0;
        char *line = strstr(buf, "version=");
        if (line)
            version = atoi(line + 8);
        if ((line = strstr(buf, "partition_ms=")))
            pms = strtoll(line + 13, NULL, 10);
        if ((line = strstr(buf, "granule_rows=")))
            grows = (unsigned)strtoul(line + 13, NULL, 10);
        if (version != FORMAT_VERSION || pms <= 0 || grows == 0 || grows > MAX_GRANULE_ROWS)
            return TSDB_ERR_CORRUPT;
        db->partition_ms = pms;
        db->granule_rows = grows;
        return 0;
    }
    if (errno != ENOENT || read_only)
        return TSDB_ERR_IO;
    db->partition_ms = db->opt.partition_ms > 0 ? db->opt.partition_ms : 86400000;
    db->granule_rows = db->opt.granule_rows ? db->opt.granule_rows : 8192;
    if (db->granule_rows > MAX_GRANULE_ROWS)
        return TSDB_ERR_INVALID;
    int len = snprintf(buf, sizeof buf, "version=%d\npartition_ms=%" PRId64 "\ngranule_rows=%u\n", FORMAT_VERSION,
                       db->partition_ms, db->granule_rows);
    fd = openat(db->dirfd, META_FILE ".tmp", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;
    bool ok = write(fd, buf, (size_t)len) == len && fsync(fd) == 0;
    close(fd);
    if (!ok || renameat(db->dirfd, META_FILE ".tmp", db->dirfd, META_FILE) != 0)
        return TSDB_ERR_IO;
    fsync(db->dirfd);
    return 0;
}

/* Loads every part, removes leftovers of interrupted writes, and removes parts
 * that a merged part covers (a crash between publishing the merged part and
 * deleting its sources leaves both on disk). */
static int load_parts(eng *db, bool read_only)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d); /* the dup shares its offset with dirfd, which an earlier scan moved */
    struct dirent *e;
    int rc = 0;
    while ((e = readdir(d)) != NULL) {
        const char *name = e->d_name;
        size_t len = strlen(name);
        if (strncmp(name, "tmp_", 4) == 0 || strcmp(name, META_FILE ".tmp") == 0) {
            if (!read_only)
                unlinkat(db->dirfd, name, 0);
            continue;
        }
        if (strncmp(name, "retired_", 8) == 0) {
            /* replaced before the last shutdown: followers may still read it */
            if (!read_only && db->ngraveyard < 1u << 20) {
                if (db->ngraveyard == db->graveyard_cap) {
                    size_t cap = db->graveyard_cap ? db->graveyard_cap * 2 : 64;
                    graveyard_entry *g = realloc(db->graveyard, cap * sizeof *g);
                    if (!g) {
                        rc = TSDB_ERR_NOMEM;
                        break;
                    }
                    db->graveyard = g;
                    db->graveyard_cap = cap;
                }
                graveyard_entry *e = &db->graveyard[db->ngraveyard++];
                snprintf(e->name, sizeof e->name, "%s", name);
                e->deadline = (int64_t)time(NULL) + db->opt.obsolete_part_lifetime_s;
            }
            continue;
        }
        if (name[0] != 'p' || len < 6 || strcmp(name + len - 5, ".part") != 0)
            continue;
        part *p;
        int r = part_open(db->dirfd, name, &p);
        if (r == TSDB_ERR_NOTFOUND)
            continue; /* removed while we listed the directory */
        if (r == 0 && p->grows != db->granule_rows) {
            part_unref(p);
            r = TSDB_ERR_CORRUPT;
        }
        if (r == TSDB_ERR_CORRUPT || r == TSDB_ERR_INVALID) {
            fprintf(stderr, "tsdb: part %s is broken (%s), renamed to %s.broken\n", name, tsdb_strerror(r), name);
            if (!read_only) {
                char broken[300];
                snprintf(broken, sizeof broken, "%s.broken", name);
                renameat(db->dirfd, name, db->dirfd, broken);
            }
            continue;
        }
        if (r != 0) {
            rc = r;
            break;
        }
        if (db->nparts == db->parts_cap) {
            size_t cap = db->parts_cap ? db->parts_cap * 2 : 64;
            part **parts = realloc(db->parts, cap * sizeof *parts);
            if (!parts) {
                part_unref(p);
                rc = TSDB_ERR_NOMEM;
                break;
            }
            db->parts = parts;
            db->parts_cap = cap;
        }
        db->parts[db->nparts++] = p;
    }
    closedir(d);
    if (rc)
        return rc;

    if (db->nparts)
        qsort(db->parts, db->nparts, sizeof *db->parts, part_qsort_cmp);
    size_t kept = 0, total = db->nparts;
    uint64_t max_block = 0;
    for (size_t i = 0; i < total; i++) {
        part *p = db->parts[i];
        part *cover = kept ? db->parts[kept - 1] : NULL;
        if (cover && cover->partition == p->partition && p->max_block <= cover->max_block) {
            if (!read_only) {
                /* retire it like a merge would have: into the list, then out */
                db->parts[kept] = p;
                db->nparts = kept + 1;
                part_ref(p);
                db_retire_locked(db, p);
            }
            part_unref(p);
            continue;
        }
        db->parts[kept++] = p;
        max_block = MAX(max_block, p->max_block);
    }
    db->nparts = kept;
    atomic_store(&db->next_block, max_block + 1);
    return 0;
}

/* ------------------------------------------------------------- open/close */

int eng_open(const char *dir, const tsdb_options *opts, eng **out)
{
    *out = NULL;
    simd_init();
    eng *db = calloc(1, sizeof *db);
    if (!db)
        return TSDB_ERR_NOMEM;
    if (opts)
        db->opt = *opts;
    else
        tsdb_options_init(&db->opt);
    if (db->opt.follow)
        db->opt.read_only = 1;
    bool ro = db->opt.read_only;
    if (db->opt.max_part_bytes == 0)
        db->opt.max_part_bytes = 4ull << 30;
    if (db->opt.buffer_rows == 0)
        db->opt.buffer_rows = 1u << 20;
    if (db->opt.wal_max_batch_rows == 0)
        db->opt.wal_max_batch_rows = 65536;
    db->dirfd = db->lockfd = db->wal_fd = -1;
    db->series.fd = -1;
    db->wal_enabled = !ro && db->opt.wal_sync_ms >= 0;
    pthread_mutex_init(&db->lock, NULL);
    pthread_mutex_init(&db->cc_lock, NULL);
    pthread_mutex_init(&db->buf_lock, NULL);
    pthread_mutex_init(&db->flush_lock, NULL);
    pthread_mutex_init(&db->wal_sync_lock, NULL);
    pthread_mutex_init(&db->manifest_lock, NULL);
    pthread_mutex_init(&db->publish_lock, NULL);
    pthread_mutex_init(&db->refresh_lock, NULL);
    pthread_cond_init(&db->wal_cv, NULL);
    pthread_cond_init(&db->merge_cv, NULL);
    pthread_cond_init(&db->merged_cv, NULL);
    pthread_cond_init(&db->flush_cv, NULL);

    int rc;
    db->dir = strdup(dir);
    if (!ro)
        mkdir(dir, 0755);
    db->dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (!db->dir || db->dirfd < 0) {
        rc = db->dir ? TSDB_ERR_IO : TSDB_ERR_NOMEM;
        goto fail;
    }
    if (!ro) {
        db->lockfd = openat(db->dirfd, "LOCK", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (db->lockfd < 0) {
            rc = TSDB_ERR_IO;
            goto fail;
        }
        if (flock(db->lockfd, LOCK_EX | LOCK_NB) != 0) {
            rc = TSDB_ERR_LOCKED;
            goto fail;
        }
    }
    if ((rc = meta_load_or_create(db, ro)) != 0)
        goto fail;
    if ((rc = series_open(&db->series, db->dirfd, ro, db->opt.fsync)) != 0)
        goto fail;
    bool has_manifest = faccessat(db->dirfd, "MANIFEST", F_OK, 0) == 0;
    if (ro && has_manifest) {
        /* a reader follows what the writer published, not the directory */
        if ((rc = follow_refresh(db)) != 0)
            goto fail;
    } else {
        if (!ro && (rc = intents_recover(db)) != 0) /* before tmp files are cleaned up */
            goto fail;
        if ((rc = load_parts(db, ro)) != 0)
            goto fail;
        /* a follower takes WAL rows from the writer's manifest once there is one */
        if (!db->opt.follow && (rc = wal_open(db)) != 0)
            goto fail;
    }
    if (!ro) {
        db->wal_flushed = db->wal_seq - 1; /* the replay put every older segment into parts */
        if ((rc = manifest_publish(db)) != 0)
            goto fail;
    }

    db->qpool = pool_new(db->opt.query_threads);
    if (!ro && db->opt.merge_threads > 0) {
        db->mergers = calloc((size_t)db->opt.merge_threads, sizeof *db->mergers);
        for (int i = 0; db->mergers && i < db->opt.merge_threads; i++)
            if (pthread_create(&db->mergers[db->nmergers], NULL, merge_thread_main, db) == 0)
                db->nmergers++;
    }
    if (!ro && pthread_create(&db->flusher, NULL, flusher_main, db) == 0)
        db->has_flusher = true;
    if (db->opt.follow && pthread_create(&db->follower, NULL, follower_main, db) == 0)
        db->has_follower = true;
    *out = db;
    return 0;
fail:
    eng_close(db);
    return rc;
}

int eng_close(eng *db)
{
    if (!db)
        return 0;
    int rc = 0;
    if (db->has_flusher) {
        pthread_mutex_lock(&db->buf_lock);
        db->flusher_stop = true;
        pthread_cond_broadcast(&db->flush_cv);
        pthread_mutex_unlock(&db->buf_lock);
        pthread_join(db->flusher, NULL);
    }
    if (db->has_follower) {
        pthread_mutex_lock(&db->buf_lock);
        db->follower_stop = true;
        pthread_cond_broadcast(&db->flush_cv);
        pthread_mutex_unlock(&db->buf_lock);
        pthread_join(db->follower, NULL);
    }
    if (!db->opt.read_only && db->active)
        rc = eng_flush(db);

    pthread_mutex_lock(&db->lock);
    atomic_store(&db->stop, true);
    pthread_cond_broadcast(&db->merge_cv);
    pthread_cond_broadcast(&db->merged_cv);
    pthread_mutex_unlock(&db->lock);
    for (int i = 0; i < db->nmergers; i++)
        pthread_join(db->mergers[i], NULL);
    free(db->mergers);
    pool_free(db->qpool);
    wal_close(db);
    for (size_t i = 0; i < db->ntails; i++)
        mt_unref(db->tails[i].m);
    db->ntails = 0;
    if (db->dirfd >= 0 && !db->opt.read_only)
        graveyard_purge(db, false); /* the rest is collected at the next open */
    free(db->graveyard);

    for (size_t i = 0; i < db->nparts; i++)
        part_unref(db->parts[i]);
    free(db->parts);
    if (db->dirfd >= 0)
        series_close(&db->series);
    while (db->cc_free) {
        codec_ctx *c = db->cc_free;
        db->cc_free = c->next;
        codec_free(c);
    }
    if (db->lockfd >= 0)
        close(db->lockfd);
    if (db->dirfd >= 0)
        close(db->dirfd);
    pthread_mutex_destroy(&db->lock);
    pthread_mutex_destroy(&db->cc_lock);
    pthread_mutex_destroy(&db->buf_lock);
    pthread_mutex_destroy(&db->flush_lock);
    pthread_mutex_destroy(&db->wal_sync_lock);
    pthread_mutex_destroy(&db->manifest_lock);
    pthread_mutex_destroy(&db->publish_lock);
    pthread_mutex_destroy(&db->refresh_lock);
    pthread_cond_destroy(&db->wal_cv);
    pthread_cond_destroy(&db->merge_cv);
    pthread_cond_destroy(&db->merged_cv);
    pthread_cond_destroy(&db->flush_cv);
    free(db->dir);
    free(db);
    return rc;
}

/* ---------------------------------------------------------------- series */

int eng_series(eng *db, const char *name, uint32_t *id) { return series_get(&db->series, name, true, id); }
int eng_series_lookup(eng *db, const char *name, uint32_t *id) { return series_get(&db->series, name, false, id); }
uint32_t eng_series_count(eng *db) { return atomic_load(&db->series.count); }
const char *eng_series_name(eng *db, uint32_t id) { return series_name(&db->series, id); }

/* ---------------------------------------------------------------- writes */

/* Small batches go through the WAL into the memtable; large ones become
 * parts directly. A non-empty memtable is flushed first, so its rows get
 * older block numbers than the new part and "last write wins" holds. */
static int write_rows(eng *db, const uint32_t *series, uint32_t cs, const int64_t *ts, const float *v, size_t n)
{
    if (n == 0)
        return 0;
    if (n < db->opt.wal_max_batch_rows)
        return mem_insert(db, series, cs, ts, v, n);
    pthread_mutex_lock(&db->buf_lock);
    bool pending = db->active->rows > 0;
    pthread_mutex_unlock(&db->buf_lock);
    int rc = pending || db->nflushing ? mt_flush(db) : 0;
    return rc ? rc : insert_rows(db, series, cs, ts, v, n, 0, NULL);
}

int eng_insert(eng *db, const uint32_t *series, const int64_t *ts, const float *values, size_t n)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    if (n && (!series || !ts || !values))
        return TSDB_ERR_INVALID;
    uint32_t count = eng_series_count(db), max = 0;
    for (size_t i = 0; i < n; i++)
        max = series[i] > max ? series[i] : max;
    if (n && max >= count)
        return TSDB_ERR_INVALID;
    return write_rows(db, series, 0, ts, values, n);
}

int eng_insert_series(eng *db, uint32_t series, const int64_t *ts, const float *values, size_t n)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    if (series >= eng_series_count(db) || (n && (!ts || !values)))
        return TSDB_ERR_INVALID;
    return write_rows(db, NULL, series, ts, values, n);
}

int eng_append(eng *db, uint32_t series, int64_t ts, float value)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    if (series >= eng_series_count(db))
        return TSDB_ERR_INVALID;
    return mem_insert(db, NULL, series, &ts, &value, 1);
}

int eng_commit(eng *db)
{
    if (db->opt.read_only)
        return 0;
    pthread_mutex_lock(&db->buf_lock);
    uint64_t target = db->wal_appended;
    int rc = db->bg_error;
    db->bg_error = 0;
    pthread_mutex_unlock(&db->buf_lock);
    return rc ? rc : wal_commit(db, target);
}

int eng_flush(eng *db)
{
    if (db->opt.read_only)
        return 0;
    int rc = eng_commit(db);
    int rc2 = mt_flush(db);
    return rc ? rc : rc2;
}

/* ----------------------------------------------------------- maintenance */

int eng_drop_before(eng *db, int64_t ts)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    /* memtable rows become parts first, so they are dropped with them */
    int rc = eng_flush(db);
    if (rc)
        return rc;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts;) {
        part *p = db->parts[i];
        int64_t end = sat_add(sat_mul(p->partition, db->partition_ms), db->partition_ms);
        if (end <= ts) {
            p->dropped = true; /* an in-flight merge of it discards its result */
            db_retire_locked(db, p);
        } else {
            i++;
        }
    }
    pthread_mutex_unlock(&db->lock);
    if (db->opt.fsync)
        fsync(db->dirfd);
    return manifest_publish(db);
}

int eng_get_stats(eng *db, tsdb_stats *st)
{
    memset(st, 0, sizeof *st);
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        st->parts++;
        st->rows += p->rows;
        st->bytes += p->map_len;
        if (i == 0 || db->parts[i - 1]->partition != p->partition)
            st->partitions++;
    }
    for (size_t i = 0; i < db->nflushing; i++)
        st->memtable_rows += db->flushing[i]->rows;
    pthread_mutex_lock(&db->buf_lock);
    if (db->active)
        st->memtable_rows += db->active->rows;
    for (size_t i = 0; i < db->ntails; i++)
        st->memtable_rows += db->tails[i].m->rows;
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    st->merges = atomic_load(&db->st_merges);
    st->merged_rows = atomic_load(&db->st_merged_rows);
    st->passthrough_granules = atomic_load(&db->st_passthrough);
    return 0;
}

int eng_foreach_part(eng *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx)
{
    size_t n;
    part **ps = db_snapshot(db, INT64_MIN, INT64_MAX, &n);
    if (!ps)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++) {
        part *p = ps[i];
        tsdb_part_info info = {p->name, p->partition, p->min_ts, p->max_ts, p->rows, p->map_len, p->ng, p->level};
        fn(ctx, &info);
    }
    db_release(ps, n);
    return 0;
}
