/* Database lifecycle, the active parts list, buffered appends and maintenance. */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#define META_FILE "META"

const char *tsdb_version(void) { return TSDB_VERSION; }

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
    case TSDB_ERR_DENIED: return "permission denied";
    case TSDB_ERR_AUTH: return "login failed";
    default: return "unknown error";
    }
}

void tsdb_options_init(tsdb_options *o)
{
    memset(o, 0, sizeof *o);
    o->partition_ms = 86400000;
    o->partition_classes = 1;
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
    /* Parts written side by side (several loaders, or a memtable flush next to
     * a batch) overlap in time once merged in block order, and overlapping
     * granules are decoded instead of answered from their statistics: a
     * finished partition is worth merging completely soon after its last write. */
    o->merge_idle_s = 300;
    o->retention_merge_s = 3600;
    o->max_part_bytes = 4ull << 30;
    o->create_volumes = 1; /* the embedding program chooses the paths; tsdbd sets 0 */
}

/* ------------------------------------------------------------ codec pool */

__thread size_t cc_gets; /* per thread: a shared counter would be one more line every reader writes */

/* cc_lock is one mutex per volume that every reader meets: take a context only for work that decodes (the bucket
 * and TWA paths take theirs lazily), or queries answered from statistics alone queue here at many connections */
codec_ctx *cc_get(eng *db)
{
    cc_gets++;
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

size_t partition_part_count_locked(eng *db, int64_t partition)
{
    size_t n = 0;
    for (size_t i = 0; i < db->nparts; i++)
        n += db->parts[i]->partition == partition;
    return n;
}

/* Publishes parts together: a query sees all of them or none. The result of
 * manifest_publish is not the caller's: its parts are committed and listed. */
void db_add_parts(eng *db, part **ps, size_t n)
{
    if (n == 0)
        return;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++)
        db_parts_insert_locked(db, ps[i]);
    pthread_cond_signal(&db->merge_cv);
    pthread_mutex_unlock(&db->lock);
    manifest_publish(db);
}

atomic_int wait_stall_ms = 30000;
atomic_int merge_fault_ms = 0;

/* The remembered stalls (db->lock). A partition is declared stalled once, by the first waiter whose wait ran out:
 * every later waiter (the next insert, the next part of the same insert) goes on at once, as long as the count has
 * not fallen below the count then and no merge of the partition has committed. Otherwise every call would pay
 * wait_stall_ms again, and an insert that wrote k parts of the partition k times. */
static size_t stall_find_locked(eng *db, int64_t partition)
{
    size_t m = sizeof db->stalled / sizeof db->stalled[0];
    for (size_t i = 0; i < m; i++)
        if (db->stalled[i].used && db->stalled[i].partition == partition)
            return i;
    return m;
}

void db_stall_clear_locked(eng *db, int64_t partition)
{
    size_t i = stall_find_locked(db, partition);
    if (i < sizeof db->stalled / sizeof db->stalled[0])
        db->stalled[i].used = false;
}

static void stall_set_locked(eng *db, int64_t partition, size_t n)
{
    size_t m = sizeof db->stalled / sizeof db->stalled[0];
    size_t i = stall_find_locked(db, partition);
    if (i == m) { /* a free slot, else the oldest (round robin): forgetting one costs that partition one more wait */
        for (size_t j = 0; j < m && i == m; j++)
            if (!db->stalled[j].used)
                i = j;
        if (i == m)
            i = db->stalled_next++ % m;
    }
    db->stalled[i].partition = partition;
    db->stalled[i].n = n;
    db->stalled[i].used = true;
}

static int64_t wait_mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

/* Backpressure, not a safety rule: the rows are published and visible when an insert gets here, so the wait never
 * fails the insert; it only ends early. It ends at once when merges of the partition cannot make progress:
 *  - a part of it is damaged (marked by part_checked or part_mark_payload): select_merge_locked leaves the whole
 *    partition out, so the count would never fall. Said once per damaged part; the damage itself was logged and
 *    counted (st_broken_parts) when it was marked;
 *  - the engine failed (eng_fail): nothing is merged any more.
 * and after wait_stall_ms in which the count did not fall and no merge of the partition ran, for every other
 * cause (parts that can never pair under max_part_bytes): a rate-limited line. A merge in flight is not cut short:
 * its time does not count, unless a merge of the partition has failed since the wait began (merge_commit_locked
 * marks the sources: a full disk or EIO on the output fails the same merge every second and marks no part, and
 * counting only the time between merges would stretch the bound to wait_stall_ms times the length of a merge).
 * Then the partition is remembered as stalled (stalled[]): the next waiters go on at once until its count falls or
 * a merge of it commits, so the bound is paid once, not per call and per part. Without this a partition whose merges
 * kept failing held every writer of the process for ever (jepsen --nemesis misdirect, verify_checksums off). */
void db_wait_merges(eng *db, int64_t partition)
{
    if (db->nmergers == 0)
        return;
    pthread_mutex_lock(&db->lock);
    int64_t idle_ms = 0, last = wait_mono_ms();
    size_t prev = SIZE_MAX;
    uint64_t start_seq = db->merge_fail_seq;
    while (!atomic_load(&db->stop)) {
        size_t n = 0;
        bool merging = false, failed = false;
        part *bad = NULL;
        for (size_t i = 0; i < db->nparts; i++) {
            part *p = db->parts[i];
            if (p->partition != partition)
                continue;
            n++;
            merging |= p->merging;
            failed |= p->fail_seq > start_seq;
            if (!bad && atomic_load(&p->damaged))
                bad = p;
        }
        if (n <= PARTS_TO_DELAY_INSERT || eng_check(db)) {
            if (n <= PARTS_TO_DELAY_INSERT)
                db_stall_clear_locked(db, partition);
            break;
        }
        if (bad) {
            if (!atomic_exchange(&bad->wait_logged, true))
                fprintf(stderr, "tsdb: partition %" PRId64 " has %zu parts and part %s is damaged, so its merges "
                                "are left out: inserts into it no longer wait for merges (its part count grows "
                                "until the part is restored)\n", partition, n, bad->name);
            break;
        }
        size_t si = stall_find_locked(db, partition);
        if (si < sizeof db->stalled / sizeof db->stalled[0]) {
            if (n >= db->stalled[si].n)
                break; /* declared stalled, and nothing has moved since */
            db->stalled[si].used = false; /* the count fell: look again */
        }
        int64_t now = wait_mono_ms();
        if (n < prev) /* progress */
            idle_ms = 0;
        else if (!merging || failed)
            idle_ms += now - last;
        prev = n;
        last = now;
        if (idle_ms >= atomic_load(&wait_stall_ms)) {
            stall_set_locked(db, partition, n);
            if (!db->stall_log_ms || now - db->stall_log_ms >= 60000) {
                db->stall_log_ms = now ? now : 1;
                fprintf(stderr, "tsdb: partition %" PRId64 " has %zu parts and its merges made no progress for "
                                "%" PRId64 " ms (no merge of it can run: parts that cannot pair under "
                                "max_part_bytes, or merges that keep failing, see the log): the insert goes on\n",
                        partition, n, idle_ms);
            }
            break;
        }
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

/* Writes META for the current format, atomically. An engine writes its own META only in a new database directory,
 * which is volume 0; a volume's META, with its number, is written by the add that makes it (volumes.c). */
static int meta_write(eng *db)
{
    char buf[256];
    int len = snprintf(buf, sizeof buf,
                       "version=%d\npartition_ms=%" PRId64 "\ngranule_rows=%u\npartition_classes=%d\nvolume=0\n",
                       FORMAT_VERSION, db->partition_ms, db->granule_rows, db->cls_on ? 1 : 0);
    int fd = openat(db->dirfd, META_FILE ".tmp", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;
    bool ok = write(fd, buf, (size_t)len) == len && tsdb_fsync(fd) == 0;
    close(fd);
    if (!ok || renameat(db->dirfd, META_FILE ".tmp", db->dirfd, META_FILE) != 0)
        return TSDB_ERR_IO;
    return tsdb_fsync(db->dirfd) == 0 ? 0 : TSDB_ERR_IO; /* at creation: the open fails */
}

/* A volume add writes the volume's META outside any lock it shares with other adds of the same directory (two
 * CLIs, or two hosts that reach it by different paths), so each writes a temporary of its own and renames that:
 * META.tmp.HOST.PID.TID. A shared name let one add rename the file another was still writing. */
void meta_tmp_make(char *buf, size_t cap)
{
    char host[65] = "";
    if (gethostname(host, sizeof host - 1) != 0 || !host[0])
        snprintf(host, sizeof host, "localhost");
    for (char *c = host; *c; c++)
        if (!((*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') || (*c >= '0' && *c <= '9') || *c == '-' ||
              *c == '_'))
            *c = '_'; /* dots too: the last two separate the numbers */
    snprintf(buf, cap, "%s.tmp.%s.%ld.%ld", META_FILE, host, (long)getpid(), tsdb_gettid());
}

/* The engine's own META.tmp, or an add's (above): what the cleanup at open removes, and nothing else of a user's. */
bool meta_tmp_name(const char *name)
{
    if (strcmp(name, META_FILE ".tmp") == 0)
        return true;
    if (strncmp(name, META_FILE ".tmp.", sizeof META_FILE + 4) != 0)
        return false;
    const char *host = name + sizeof META_FILE + 4, *tid = strrchr(host, '.');
    if (!tid || tid == host)
        return false;
    const char *pid = tid - 1;
    while (pid > host && *pid != '.')
        pid--;
    if (pid == host || *pid != '.')
        return false;
    for (const char *c = host; c < pid; c++)
        if (*c == '.')
            return false;
    for (const char *c = pid + 1; c < tid; c++)
        if (*c < '0' || *c > '9')
            return false;
    for (const char *c = tid + 1; *c; c++)
        if (*c < '0' || *c > '9')
            return false;
    return tid > pid + 1 && tid[1];
}

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
        int classes = -1;
        if ((line = strstr(buf, "partition_classes=")))
            classes = atoi(line + 18);
        if (version != FORMAT_VERSION) {
            /* the format may change in a 0.9.x release (docs/compatibility.md): another version is refused, never
             * read as if it were ours */
            fprintf(stderr,
                    "tsdb: the database has disk format version %d; tsdb %s reads version %d only. Export the data "
                    "with the release that wrote it and import it here (docs/compatibility.md)\n",
                    version, TSDB_VERSION, FORMAT_VERSION);
            return TSDB_ERR_CORRUPT;
        }
        if (pms <= 0 || grows == 0 || grows > MAX_GRANULE_ROWS || classes < 0 || classes > 1)
            return TSDB_ERR_CORRUPT;
        db->partition_ms = pms;
        db->granule_rows = grows;
        cls_setup(db, classes);
        return 0;
    }
    if (errno != ENOENT || read_only)
        return TSDB_ERR_IO;
    db->partition_ms = db->opt.partition_ms > 0 ? db->opt.partition_ms : 86400000;
    db->granule_rows = db->opt.granule_rows ? db->opt.granule_rows : 8192;
    if (db->granule_rows > MAX_GRANULE_ROWS)
        return TSDB_ERR_INVALID;
    cls_setup(db, db->opt.partition_classes);
    return meta_write(db);
}

/* Every series a part holds rows or tombstones of is in SERIES: its record was synced before the series was used.
 * A part naming one beyond is damage to SERIES: records cut as a torn tail at its open, where damage to the last
 * records looks the same (a misdirected write once overwrote all four of a test's), or lost. The open fails rather
 * than serve those series as never created and their rows as gone. */
bool part_beyond_series(const part *p, uint32_t n, uint32_t *s)
{
    *s = p->ix.nruns ? p->ix.rser[p->ix.nruns - 1] : 0; /* runs are in series order: one per part */
    bool bad = p->ix.nruns && *s >= n;
    for (uint32_t t = 0; !bad && t < p->nt; t++)
        if ((bad = p->t[t].series >= n))
            *s = p->t[t].series;
    return bad;
}

static int parts_check_series(eng *db, bool read_only)
{
    uint32_t n = eng_series_count(db);
    bool reloaded = false;
    for (size_t i = 0; i < db->nparts; i++) {
        const part *p = db->parts[i];
        uint32_t s;
        bool bad = part_beyond_series(p, n, &s);
        if (bad && read_only && !reloaded) {
            /* A reader without MANIFEST read SERIES before it listed the parts: a writer that opened in between
             * may have created the series since (its record is synced before any part can hold it), so the file
             * is read again before it is judged. A writer holds the lock: its SERIES cannot grow meanwhile. */
            int rc = series_reload(&db->series, db->dirfd);
            if (rc != 0)
                return rc;
            n = eng_series_count(db);
            reloaded = true;
            i--;
            continue;
        }
        if (bad) {
            fprintf(stderr,
                    "tsdb: part %s holds series %u, but SERIES has only %u: SERIES is damaged. Nothing was changed: "
                    "restore SERIES from a backup or snapshot (a copy with at least %u records), then open again; "
                    "tsdb export cannot read the database either until then\n",
                    p->name, s, n, s + 1);
            return TSDB_ERR_CORRUPT;
        }
    }
    return 0;
}

static int dict_short(const char *file, const char *by, uint64_t named, uint32_t n)
{
    fprintf(stderr,
            "tsdb: %s, but %s has only %" PRIu32 " records: %s is damaged (its last records, cut as a torn tail, "
            "would be handed out again to new names). Nothing was changed: restore %s from a backup or snapshot (a "
            "copy with at least %" PRIu64 " records), then open again; tsdb export cannot read the database either "
            "until then\n",
            by, file, n, file, file, named);
    return TSDB_ERR_CORRUPT;
}

/* Besides parts and WAL rows (parts_check_series, wal_check_segment), these durable records name series and states,
 * and each was written after the record it names was synced: RETENTION (tsdb_set_retention takes ids SERIES has),
 * CLASSES (a class is decided for a series that exists), and the counts of a MANIFEST whose writer synced
 * (dict_append counts a record once it is synced). So one naming an id the dictionary lacks proves the dictionary
 * lost acknowledged records, typically its last ones, damaged at rest and taken for a torn tail; cut, their ids
 * would go to the next names, which would inherit a retention limit or a class, or (STATES) old rows' codes. State
 * values in parts and the WAL prove nothing: inserts take any code, not only those tsdb_state handed out. */
static int open_check_records(eng *db)
{
    uint32_t n = eng_series_count(db), ns = atomic_load(&db->states.count), ms, mst;
    char by[64];
    if (db->ret_ids > n) {
        snprintf(by, sizeof by, "RETENTION names series %" PRIu64, db->ret_ids - 1);
        return dict_short("SERIES", by, db->ret_ids, n);
    }
    if (db->cls_ids > n) {
        snprintf(by, sizeof by, "CLASSES names series %" PRIu64, db->cls_ids - 1);
        return dict_short("SERIES", by, db->cls_ids, n);
    }
    int rc = manifest_synced_counts(db, &ms, &mst);
    if (rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_CORRUPT)
        return 0; /* none, a writer's without fsync, or damaged: the writer rewrites it, and the parts were checked */
    if (rc)
        return rc;
    if (ms > n) {
        snprintf(by, sizeof by, "MANIFEST counts %" PRIu32 " series", ms);
        return dict_short("SERIES", by, ms, n);
    }
    if (mst > ns) {
        snprintf(by, sizeof by, "MANIFEST counts %" PRIu32 " states", mst);
        return dict_short("STATES", by, mst, ns);
    }
    return 0;
}

/* A writer's open cuts the torn tails it found (SERIES, STATES, CLASSES, RETENTION) only once every check that
 * could refuse the open passed: a part, a WAL row or another record can prove a "torn" record was acknowledged, and
 * a refused open must leave the files as they were, for the operator to repair. */
static int open_cut_tails(eng *db)
{
    int rc = open_check_records(db);
    if (rc == 0)
        rc = series_cut_tail(&db->series);
    if (rc == 0)
        rc = series_cut_tail(&db->states);
    if (rc == 0)
        rc = cls_cut_tail(db);
    if (rc == 0)
        rc = ret_cut_tail(db);
    return rc;
}

/* test hook (tsdb_test_before_parts): runs in an open that lists the parts itself, just before it does */
static void (*test_before_parts)(void *);
static void *test_before_parts_arg;
void tsdb_test_before_parts(void (*fn)(void *), void *arg)
{
    test_before_parts_arg = arg;
    test_before_parts = fn;
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
    /* errno is cleared before every readdir: NULL with errno set is a read error, never the end (a hung or failing
     * NFS server must not look like a directory with fewer parts) */
    for (errno = 0; (e = readdir(d)) != NULL; errno = 0) {
        const char *name = e->d_name;
        /* Only names the engine writes: a volume may be a directory with the user's own files in it
         * (tsdb_volume_add_dir), and their tmp_notes.txt or retired_old.csv is not ours to delete. */
        if (part_tmp_name(name) || meta_tmp_name(name)) {
            if (!read_only)
                unlinkat(db->dirfd, name, 0);
            continue;
        }
        if (strncmp(name, "retired_", 8) == 0 && part_file_name(name + 8)) {
            /* replaced before the last shutdown: followers may still read it */
            if (!read_only && db->opt.obsolete_part_lifetime_s <= 0) {
                unlinkat(db->dirfd, name, 0); /* no lifetime: nobody is promised the file */
            } else if (!read_only && db->ngraveyard < 1u << 20) {
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
                e->deadline = eng_mono_ms() + (int64_t)db->opt.obsolete_part_lifetime_s * 1000;
            }
            continue;
        }
        if (!part_file_name(name))
            continue;
        part *p;
        int r = part_open(db->dirfd, name, db->opt.pin_index, &p);
        if (r == TSDB_ERR_NOTFOUND)
            continue; /* removed while we listed the directory */
        if (r == 0 && p->grows != db->granule_rows) {
            part_unref(p);
            r = TSDB_ERR_CORRUPT;
        }
        if (r == 0)
            p->verify = db->opt.verify_checksums;
        if (r == TSDB_ERR_CORRUPT || r == TSDB_ERR_INVALID) {
            /* Never left out quietly: its rows would vanish and older values of
             * the same keys come back, and a merge would make that permanent.
             * Parts are synced before they are named, so this is damage or a bug
             * (or a power loss with fsync = 0, which leaves the same to an operator). */
            fprintf(stderr, "tsdb: part %s is broken (%s): the open fails; rename it to %s.broken to accept the "
                            "loss of its rows\n", name, tsdb_strerror(r), name);
            rc = TSDB_ERR_CORRUPT; /* not counted in st_broken_parts: nobody could read it */
            break;
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
    if (rc == 0 && errno != 0) {
        fprintf(stderr, "tsdb: reading the directory failed: %s: the open fails\n", strerror(errno));
        rc = TSDB_ERR_IO;
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

int eng_open(const char *dir, const tsdb_options *opts, eng **out) { return eng_open_sched(dir, opts, NULL, out); }

static __thread int adopt_lockfd = -1;
/* A check of the opened engine (eng_reopen), run by the open once nothing can fail any more and before the engine's
 * own threads start: they change the parts list (a merge, a flush), which the check reads without a lock. */
static __thread void (*open_check)(const eng *, void *);
static __thread void *open_check_arg;

int eng_reopen(eng *old, const char *dir, const tsdb_options *opts, sched *qs, eng **out,
               void (*check)(const eng *neu, void *arg), void *arg)
{
    open_check = check; /* taken by the open; cleared below when it failed before that */
    open_check_arg = arg;
    int rc;
    if (old->lockfd < 0) {
        rc = eng_open_sched(dir, opts, qs, out);
    } else {
        int fd = fcntl(old->lockfd, F_DUPFD_CLOEXEC, 3); /* the new engine owns this copy; the old one keeps its own */
        if (fd < 0) {
            open_check = NULL;
            return TSDB_ERR_IO;
        }
        adopt_lockfd = fd;
        rc = eng_open_sched(dir, opts, qs, out);
        if (adopt_lockfd >= 0) { /* the open failed before it took the fd */
            close(adopt_lockfd);
            adopt_lockfd = -1;
        }
    }
    open_check = NULL;
    return rc;
}

int eng_open_sched(const char *dir, const tsdb_options *opts, sched *qs, eng **out)
{
    *out = NULL;
    simd_init();
    eng *db = calloc(1, sizeof *db);
    if (!db)
        return TSDB_ERR_NOMEM;
    db->ret_fd = -2; /* retention not opened yet */
    db->cls_fd = -2;
    if (opts)
        db->opt = *opts;
    else
        tsdb_options_init(&db->opt);
    if (db->opt.value_codec < TSDB_VALUES_AUTO || db->opt.value_codec > TSDB_VALUES_CHIMP) {
        /* other numbers are the codec's own modes (VALUES_FIXED_ONLY), never a choice for every granule */
        free(db);
        return TSDB_ERR_INVALID;
    }
    if (db->opt.follow)
        db->opt.read_only = 1;
    bool ro = db->opt.read_only;
    if (db->opt.max_part_bytes == 0)
        db->opt.max_part_bytes = 4ull << 30;
    if (db->opt.buffer_rows == 0)
        db->opt.buffer_rows = 1u << 20;
    if (db->opt.wal_max_batch_rows == 0)
        db->opt.wal_max_batch_rows = 65536;
    if (db->opt.retention_merge_s == 0)
        db->opt.retention_merge_s = 3600;
    if (db->opt.merge_idle_s == 0) /* a zeroed struct: merging everything all the time is never meant */
        db->opt.merge_idle_s = 300;
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
    pthread_mutex_init(&db->fail_lock, NULL);
    pthread_mutex_init(&db->dir_sync_lock, NULL);
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
        if (adopt_lockfd >= 0) {
            /* a reopen of this directory by the process that holds its lock (eng_reopen_lockfd): the same open
             * file description, so the lock is never released between the old engine and the new one */
            db->lockfd = adopt_lockfd;
            adopt_lockfd = -1;
        } else {
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
    }
    if ((rc = meta_load_or_create(db, ro)) != 0)
        goto fail;
    if ((rc = series_open(&db->series, db->dirfd, "SERIES", ro, db->opt.fsync)) != 0 ||
        (rc = series_open(&db->states, db->dirfd, "STATES", ro, db->opt.fsync)) != 0 ||
        (rc = ret_open(db, ro)) != 0 || (rc = cls_open(db, ro)) != 0) /* classes before the WAL replay */
        goto fail;
    bool has_manifest = faccessat(db->dirfd, "MANIFEST", F_OK, 0) == 0;
    if (ro && has_manifest) {
        /* a reader follows what the writer published, not the directory */
        /* a MANIFEST caught mid-rewrite is read again, but a damaged one is not an empty database */
        /* and parts naming series SERIES lacks are not an empty view either: the open waits as a follower would,
           then fails (follow_refresh) */
        while ((rc = follow_refresh(db)) == 0 && !db->follow_version &&
               (db->manifest_bad_since || db->series_gap_since || db->series_bad_since))
            usleep(10000);
        if (rc == 0) /* CLASSES was read again after MANIFEST: its records come before the parts that use them */
            rc = cls_check_parts(db);
        /* CLASSES can also be newer than this MANIFEST: a merge after it may have deleted a series' newest rows,
         * and a switch then put its class change below rows this MANIFEST still lists. The writer published the
         * merge's MANIFEST before that switch, so a MANIFEST read again passes; on damage it stays the same. */
        for (int i = 0; rc == TSDB_ERR_CORRUPT && i < 100; i++) {
            uint64_t seen = db->follow_check;
            usleep(10000);
            if ((rc = follow_refresh(db)) == 0)
                rc = db->follow_check != seen ? cls_check_parts(db) : TSDB_ERR_CORRUPT;
        }
        if (rc)
            goto fail;
    } else {
        if (!ro && (rc = intents_recover(db)) != 0) /* before tmp files are cleaned up */
            goto fail;
        if (test_before_parts)
            test_before_parts(test_before_parts_arg);
        if ((rc = load_parts(db, ro)) != 0 || (rc = parts_check_series(db, ro)) != 0 ||
            (rc = cls_check_parts(db)) != 0)
            goto fail;
        /* a follower takes WAL rows from the writer's manifest once there is one; a writer cuts torn tails only
           after the WAL's checks too, just before the replay writes anything */
        if (!db->opt.follow)
            rc = wal_open(db, ro ? NULL : open_cut_tails);
        else if (!ro)
            rc = open_cut_tails(db);
        if (rc != 0)
            goto fail;
    }
    if (!ro) {
        db->wal_flushed = db->wal_seq - 1; /* the replay put every older segment into parts */
        db->wal_removed = db->wal_removed_synced = db->wal_seq - 1; /* and unlinked them, with a directory fsync */
        if ((rc = manifest_publish(db)) != 0)
            goto fail;
    }

    ret_clock_init(db);
    if (qs) {
        db->sched = qs;
    } else {
        db->sched = sched_new(db->opt.query_threads, db->opt.request_threads);
        db->own_sched = true;
        if (!db->sched) {
            rc = TSDB_ERR_NOMEM;
            goto fail;
        }
    }
    if (open_check) { /* a reopen's comparison with the engine it replaces: before any thread changes the parts */
        void (*check)(const eng *, void *) = open_check;
        open_check = NULL;
        check(db, open_check_arg);
    }
    if (!ro && db->opt.merge_threads > 0) {
        db->mergers = calloc((size_t)db->opt.merge_threads, sizeof *db->mergers);
        for (int i = 0; db->mergers && i < db->opt.merge_threads; i++)
            if (tsdb_thread_create(&db->mergers[db->nmergers], merge_thread_main, db) == 0)
                db->nmergers++;
    }
    if (!ro && tsdb_thread_create(&db->flusher, flusher_main, db) == 0)
        db->has_flusher = true;
    atomic_store(&db->follow_ok_ms, eng_mono_ms());
    if (db->opt.follow && tsdb_thread_create(&db->follower, follower_main, db) == 0)
        db->has_follower = true;
    *out = db;
    return 0;
fail:
    eng_close(db);
    return rc;
}

/* Stops and joins the threads of the engine, which then does nothing on its own: for a volume that is about to be
 * opened again (ha.md 15.5), so no thread of the old engine writes beside the new one. No flush: what was durable
 * is in the WAL, which the new open replays. A thread inside a hung call keeps this waiting: no deadline. */
void eng_halt(eng *db)
{
    if (db->has_flusher) {
        pthread_mutex_lock(&db->buf_lock);
        db->flusher_stop = true;
        pthread_cond_broadcast(&db->flush_cv);
        pthread_mutex_unlock(&db->buf_lock);
        pthread_join(db->flusher, NULL);
        db->has_flusher = false;
    }
    if (db->has_follower) {
        pthread_mutex_lock(&db->buf_lock);
        db->follower_stop = true;
        pthread_cond_broadcast(&db->flush_cv);
        pthread_mutex_unlock(&db->buf_lock);
        pthread_join(db->follower, NULL);
        db->has_follower = false;
    }
    pthread_mutex_lock(&db->lock);
    atomic_store(&db->stop, true);
    pthread_cond_broadcast(&db->merge_cv);
    pthread_cond_broadcast(&db->merged_cv);
    pthread_mutex_unlock(&db->lock);
    for (int i = 0; i < db->nmergers; i++)
        pthread_join(db->mergers[i], NULL);
    db->nmergers = 0;
}

/* Appends the name strings of d to *vec (freed by the owner of the vector, at tsdb_close). */
static void names_take(series_dict *d, char ***vec, size_t *n, size_t *cap)
{
    uint32_t cnt = atomic_load(&d->count);
    if (!d->file || !cnt)
        return;
    if (*n + cnt > *cap) {
        size_t nc = (*n + cnt) * 2;
        char **v = realloc(*vec, nc * sizeof *v);
        if (!v)
            return; /* the strings stay allocated and are never freed: a leak of a few bytes, not a dangling name */
        *vec = v;
        *cap = nc;
    }
    for (uint32_t i = 0; i < cnt; i++)
        (*vec)[(*n)++] = d->names[i];
}

int eng_retire(eng *db, pthread_mutex_t *mu, char ***kept, size_t *nkept, size_t *capkept)
{
    if (!db)
        return 0;
    db->retiring = true;
    pthread_mutex_lock(mu); /* retires of several volumes run at once; the close below may be slow and is outside */
    names_take(&db->series, kept, nkept, capkept);
    names_take(&db->states, kept, nkept, capkept);
    pthread_mutex_unlock(mu);
    return eng_close(db);
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
    /* a retired engine writes nothing: its replacement owns the directory */
    if (!db->opt.read_only && db->active && !db->retiring)
        rc = eng_flush(db);

    pthread_mutex_lock(&db->lock);
    atomic_store(&db->stop, true);
    pthread_cond_broadcast(&db->merge_cv);
    pthread_cond_broadcast(&db->merged_cv);
    pthread_mutex_unlock(&db->lock);
    for (int i = 0; i < db->nmergers; i++)
        pthread_join(db->mergers[i], NULL);
    free(db->mergers);
    if (db->own_sched)
        sched_free(db->sched);
    wal_close(db);
    for (size_t i = 0; i < db->ntails; i++)
        mt_unref(db->tails[i].m);
    db->ntails = 0;
    if (db->dirfd >= 0 && !db->opt.read_only && !db->retiring)
        graveyard_purge(db, false); /* the rest is collected at the next open */
    free(db->graveyard);

    for (size_t i = 0; i < db->nparts; i++)
        part_unref(db->parts[i]);
    free(db->parts);
    db->series.keep_names = db->states.keep_names = db->retiring;
    if (db->series.file)
        series_close(&db->series);
    if (db->states.file)
        series_close(&db->states);
    if (db->ret_fd != -2)
        ret_close(db);
    if (db->cls_fd != -2)
        cls_close(db);
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
    pthread_mutex_destroy(&db->fail_lock);
    pthread_mutex_destroy(&db->dir_sync_lock);
    pthread_cond_destroy(&db->wal_cv);
    pthread_cond_destroy(&db->merge_cv);
    pthread_cond_destroy(&db->merged_cv);
    pthread_cond_destroy(&db->flush_cv);
    free(db->dir);
    free(db);
    return rc;
}

/* ---------------------------------------------------------------- series */

/* ---------------------------------------------------------- fail-stop */

void eng_fail(eng *db, const char *what, int err)
{
    pthread_mutex_lock(&db->fail_lock);
    if (!atomic_load(&db->failed)) {
        snprintf(db->fail_msg, sizeof db->fail_msg, "%s: %s failed: %s", db->dir, what, strerror(err ? err : EIO));
        fprintf(stderr, "tsdb: %s; no more writes until the database is reopened\n", db->fail_msg);
        atomic_store_explicit(&db->failed, true, memory_order_release);
    }
    pthread_mutex_unlock(&db->fail_lock);
}

/* Every WAL segment unlinked before a directory fsync stays unlinked after a power loss: wal_removed_synced
 * follows each one (the WAL tail kept for class switches, and empty merge outputs, shrink with it). */
static void removed_synced_to(eng *db, uint64_t removed)
{
    uint64_t cur = atomic_load(&db->wal_removed_synced);
    while (cur < removed && !atomic_compare_exchange_weak(&db->wal_removed_synced, &cur, removed)) {
    }
}

int eng_sync_dir(eng *db, const char *what)
{
    uint64_t removed = atomic_load(&db->wal_removed); /* unlinked before the fsync below */
    if (!db->opt.fsync) {
        int rc = eng_check(db);
        if (rc == 0)
            removed_synced_to(db, removed);
        return rc;
    }
    pthread_mutex_lock(&db->dir_sync_lock);
    int rc = eng_check(db);
    if (rc == 0 && tsdb_fsync(db->dirfd) != 0) {
        eng_fail(db, what, errno);
        rc = TSDB_ERR_IO;
    }
    pthread_mutex_unlock(&db->dir_sync_lock);
    if (rc == 0)
        removed_synced_to(db, removed);
    return rc;
}

int eng_failed_msg(eng *db, char *msg, size_t len)
{
    if (!eng_check(db))
        return 0;
    if (msg && len) {
        pthread_mutex_lock(&db->fail_lock);
        snprintf(msg, len, "%s", db->fail_msg);
        pthread_mutex_unlock(&db->fail_lock);
    }
    return TSDB_ERR_IO;
}

/* A failed dictionary append may leave a torn record, which ends the file for
 * the next open: nothing may be appended after it. After a failure, existing
 * names are still found. */
static int dict_get(eng *db, series_dict *d, const char *what, const char *name, bool create, int type,
                    uint32_t *id)
{
    if (create && eng_check(db)) {
        int rc = series_get(d, name, false, type, id);
        return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_IO : rc;
    }
    errno = 0;
    int rc = series_get(d, name, create, type, id);
    /* errno 0: refused because an earlier append failed. That creator fails the engine with the real cause; this
     * one knows none, and the first eng_fail names the failure. */
    if (rc == TSDB_ERR_IO && create && !db->opt.read_only && errno != 0)
        eng_fail(db, what, errno);
    return rc;
}

int eng_series(eng *db, const char *name, int type, uint32_t *id)
{
    return dict_get(db, &db->series, "append to the series dictionary", name, true, type, id);
}
int eng_series_lookup(eng *db, const char *name, uint32_t *id) { return series_get(&db->series, name, false, 0, id); }
uint32_t eng_series_count(eng *db) { return atomic_load(&db->series.count); }
const char *eng_series_name(eng *db, uint32_t id) { return series_name(&db->series, id); }
int eng_series_type(eng *db, uint32_t id) { return series_type(&db->series, id); }

/* State codes are dictionary ids + 1: code 0 is null. */
int eng_state(eng *db, const char *name, bool create, uint32_t *code)
{
    uint32_t id;
    int rc = dict_get(db, &db->states, "append to the state dictionary", name, create, 0, &id);
    if (rc == TSDB_ERR_NOTFOUND && db->opt.read_only) {
        series_reload(&db->states, db->dirfd); /* the writer may have made it a moment ago */
        rc = series_get(&db->states, name, false, 0, &id);
    }
    if (rc == 0)
        *code = id + 1;
    return rc;
}

const char *eng_state_name(eng *db, uint32_t code)
{
    if (code == 0)
        return NULL;
    const char *name = series_name(&db->states, code - 1);
    if (!name && db->opt.read_only) {
        series_reload(&db->states, db->dirfd);
        name = series_name(&db->states, code - 1);
    }
    return name;
}

/* ---------------------------------------------------------------- writes */

/* Small batches go through the WAL into the memtable; large ones become
 * parts directly. A non-empty memtable is flushed first, so its rows get
 * older block numbers than the new part and "last write wins" holds. */
static int write_rows(eng *db, int type, const uint32_t *series, uint32_t cs, const int64_t *ts, const void *v,
                      size_t n, int ucls, uint64_t gen)
{
    int rc = eng_check(db);
    if (rc || n == 0)
        return rc;
    if (n < db->opt.wal_max_batch_rows) {
        /* a small batch: one pass that vectorizes costs nothing beside its WAL record (large ones check it in
           the passes the sort makes anyway, insert.c) */
        bool unreachable = false;
        for (size_t i = 0; i < n; i++)
            unreachable |= ts[i] == TS_UNREACHABLE;
        if (unreachable)
            return TSDB_ERR_INVALID;
        return mem_insert(db, series, cs, ts, v, type_width(type), n);
    }
    /* small writes to these keys wait from here until the parts are published (lw_fence) */
    lw_fence f;
    if ((rc = lw_fence_rows(&f, series, cs, ts, n)) != 0)
        return rc;
    rc = lw_enter(db, &f) ? mt_flush(db) : 0;
    if (rc == 0 && ucls == -2) { /* fresh series: their class comes from these rows */
        rc = cls_classify_insert(db, series, cs, ts, n);
        ucls = -1;
    }
    if (rc) {
        lw_leave(db, &f);
        return rc;
    }
    return insert_rows(db, type, series, cs, ts, v, n, 0, NULL, true, ucls, &gen, &f);
}

int eng_insert(eng *db, int type, const uint32_t *series, const int64_t *ts, const void *values, size_t n)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    if (n && (!series || !ts || !values))
        return TSDB_ERR_INVALID;
    int ucls = -1;
    uint64_t gen = cls_gen_now(db); /* before any class is looked up */
    if (n && !series_check(&db->series, series, 0, n, type, atomic_load_explicit(&db->cls_tab, memory_order_acquire),
                           &ucls))
        return TSDB_ERR_INVALID;
    return write_rows(db, type, series, 0, ts, values, n, ucls, gen);
}

int eng_insert_series(eng *db, int type, uint32_t series, const int64_t *ts, const void *values, size_t n)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    int ucls = -1;
    uint64_t gen = cls_gen_now(db);
    if (!series_check(&db->series, NULL, series, 0, type, atomic_load_explicit(&db->cls_tab, memory_order_acquire),
                      &ucls) ||
        (n && (!ts || !values)))
        return TSDB_ERR_INVALID;
    return write_rows(db, type, NULL, series, ts, values, n, ucls, gen);
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
    if (rc == 0)
        rc = eng_check(db); /* even with nothing pending: the caller learns of the failure */
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
    /* A merge of a partition being dropped names its output (durably) before it takes db->lock to list it. Dropped
     * meanwhile, its sources are gone and the output is the only part with their rows: a crash after the
     * acknowledgement would list it alone. So wait until the merge has listed it (the loop below retires it with
     * the rest, by rename, before the fsync) and look again: the wait released db->lock. The intent is published
     * first, so that no further merge of those partitions is selected meanwhile (select_merge_locked,
     * ttl_select_locked, eng_optimize): the wait ends with the merges already in flight. Drops are serialized by
     * drop_lock (tsdb_drop_before), so there is one intent at a time; it is cleared on every way out below. */
    db->drop_pending = true;
    db->drop_pending_end = ts;
    for (bool busy = true; busy && !eng_check(db);) {
        busy = false;
        for (size_t i = 0; i < db->nparts && !busy; i++) {
            int64_t lo, end;
            partition_range(db, db->parts[i]->partition, &lo, &end);
            busy = end <= ts && db->parts[i]->merging;
        }
        if (busy)
            pthread_cond_wait(&db->merged_cv, &db->lock);
    }
    for (size_t i = 0; i < db->nparts;) {
        part *p = db->parts[i];
        int64_t lo, end;
        partition_range(db, p->partition, &lo, &end);
        if (eng_check(db)) /* a rename below failed: retire nothing more */
            break;
        if (end <= ts) {
            p->dropped = true; /* an in-flight merge of it discards its result */
            db_retire_locked(db, p);
        } else {
            i++;
        }
    }
    db->drop_pending = false; /* also after a failed engine: the retire loop above is the only exit */
    pthread_mutex_unlock(&db->lock);
    if ((rc = eng_sync_dir(db, "fsync of the directory after retiring dropped parts")) != 0)
        return rc; /* failed since the flush, here or elsewhere */
    return manifest_publish(db);
}

/* The numbers of tsdb_stats in one pass over the parts; the volume statistics ask for the granule
 * and merging counts in the same pass (granules and rows then come from one snapshot). */
static void stats_walk(eng *db, tsdb_stats *st, uint64_t *granules, uint64_t *merging, uint64_t *pinned)
{
    memset(st, 0, sizeof *st);
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (pinned)
            *pinned += p->pinned;
        if (granules)
            *granules += p->ng;
        if (merging)
            *merging += p->merging;
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
    st->broken_parts = atomic_load(&db->st_broken_parts);
    st->follow_errors = atomic_load(&db->st_follow_errors);
}

int eng_get_stats(eng *db, tsdb_stats *st)
{
    stats_walk(db, st, NULL, NULL, NULL);
    return 0;
}

int eng_get_volume_stats(eng *db, tsdb_volume_stats *vs)
{
    memset(vs, 0, sizeof *vs);
    tsdb_stats st;
    stats_walk(db, &st, &vs->granules, &vs->merging_parts, &vs->pinned_bytes);
    vs->parts = st.parts;
    vs->partitions = st.partitions;
    vs->rows = st.rows;
    vs->bytes = st.bytes;
    vs->memtable_rows = st.memtable_rows;
    vs->merges = st.merges;
    vs->merged_rows = st.merged_rows;
    vs->broken_parts = st.broken_parts;
    vs->follow_errors = st.follow_errors;
    vs->wal_bytes = atomic_load_explicit(&db->st_wal_bytes, memory_order_relaxed);
    vs->wal_syncs = atomic_load_explicit(&db->st_wal_syncs, memory_order_relaxed);
    vs->failed = atomic_load(&db->failed) ? 1 : 0;
    if (db->opt.follow) {
        int64_t lag = eng_mono_ms() - atomic_load(&db->follow_ok_ms);
        vs->follow_refresh_age_ms = lag > 0 ? lag : 0;
    }
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
        tsdb_part_info info = {.name = p->name, .partition = p->partition, .min_ts = p->min_ts, .max_ts = p->max_ts,
                               .rows = p->rows, .bytes = p->map_len, .granules = p->ng, .level = p->level,
                               .created_s = p->created, .written_s = p->written,
                               .cls = partition_class(db, p->partition)};
        partition_range(db, p->partition, &info.start_ms, &info.end_ms);
        info.partition = floor_div(p->partition, db->cls_kmul); /* the base partition it starts in, as before */
        fn(ctx, &info);
    }
    db_release(ps, n);
    return 0;
}
