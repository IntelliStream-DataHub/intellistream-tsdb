/*
 * Retention per series: rows older than now - limit disappear.
 *
 * The limits live in RETENTION, an append-only file of 16-byte records
 * [u32 check][u32 series][i64 limit_ms]; the last record of a series wins and
 * a limit of 0 keeps everything. The check (low 32 bits of xxh64 of the rest,
 * seeded with the record's offset) tells a record from zeros or a torn append:
 * open keeps the records up to the first bad one, which is a torn tail and cut,
 * unless a valid record follows it (damage: the open fails, TSDB_ERR_CORRUPT).
 * A binary that predates retention ignores the file and keeps everything.
 *
 * Expired rows are hidden at once: queries start at the cutoff. Merges drop
 * them, a part that holds some is rewritten alone once it is old enough, and a
 * partition goes whole when every series in it has expired past its end.
 *
 * "now" is the wall clock in ms, but never earlier than a value the engine
 * already used: a clock stepped back does not bring expired rows back. The
 * floor starts at the newest part's mtime at open (a merge that dropped rows
 * wrote its part after it read the clock). A clock stepped forward expires rows
 * early, and for good once a merge has dropped them.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define RET_FILE "RETENTION"
#define RET_REC 16

static uint32_t rec_check(const uint8_t *rec, uint64_t off) { return (uint32_t)xxh64(rec + 4, RET_REC - 4, off); }

static bool rec_ok(const uint8_t *rec, uint64_t off)
{
    uint32_t c;
    memcpy(&c, rec, 4);
    return c == rec_check(rec, off);
}

static int ret_put_locked(eng *db, uint32_t series, int64_t limit)
{
    atomic_fetch_add(&db->ret_gen, 1);
    ret_table *t = atomic_load_explicit(&db->ret_tab, memory_order_relaxed);
    if (!t || series >= t->cap) {
        uint64_t cap = t ? t->cap : 1024;
        while (cap <= series)
            cap *= 2;
        ret_table *x = calloc(1, sizeof *x + cap * sizeof x->limit[0]);
        if (!x)
            return TSDB_ERR_NOMEM;
        x->cap = cap;
        x->older = t;
        for (uint64_t i = 0; t && i < t->cap; i++)
            atomic_init(&x->limit[i], atomic_load_explicit(&t->limit[i], memory_order_relaxed));
        atomic_store_explicit(&db->ret_tab, x, memory_order_release);
        t = x;
    }
    atomic_store_explicit(&t->limit[series], limit, memory_order_relaxed);
    if (limit > 0)
        atomic_store(&db->has_retention, true);
    return 0;
}

/* Reads the whole file again: it is tiny, and a reader sees the writer's appends this way. */
static int ret_load(eng *db, bool writer)
{
    int fd = openat(db->dirfd, RET_FILE, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size;
    uint8_t *buf = malloc(size ? size : 1);
    if (!buf) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    size_t got = 0;
    while (got < size) {
        ssize_t r = pread(fd, buf + got, size - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    close(fd);
    int rc = got == size ? 0 : TSDB_ERR_IO;
    size_t pos = 0;
    pthread_mutex_lock(&db->ret_lock);
    for (; rc == 0 && pos + RET_REC <= size && rec_ok(buf + pos, pos); pos += RET_REC) {
        uint32_t series;
        int64_t limit;
        memcpy(&series, buf + pos + 4, 4);
        memcpy(&limit, buf + pos + 8, 8);
        rc = ret_put_locked(db, series, limit);
    }
    for (size_t q = pos + RET_REC; rc == 0 && q + RET_REC <= size; q += RET_REC) {
        if (rec_ok(buf + q, q)) {
            fprintf(stderr, "tsdb: %s is damaged at offset %zu, before valid records\n", RET_FILE, pos);
            rc = TSDB_ERR_CORRUPT;
        }
    }
    if (writer)
        db->ret_off = pos; /* a torn tail is overwritten by the next append */
    pthread_mutex_unlock(&db->ret_lock);
    free(buf);
    return rc;
}

int ret_open(eng *db, bool read_only)
{
    pthread_mutex_init(&db->ret_lock, NULL);
    db->ret_fd = -1;
    return ret_load(db, !read_only);
}

int ret_reload(eng *db) { return ret_load(db, false); }

void ret_close(eng *db)
{
    if (db->ret_fd >= 0)
        close(db->ret_fd);
    db->ret_fd = -1;
    for (ret_table *t = atomic_load(&db->ret_tab), *o; t; t = o) {
        o = t->older;
        free(t);
    }
    atomic_store(&db->ret_tab, NULL);
    pthread_mutex_destroy(&db->ret_lock);
}

void ret_clock_init(eng *db)
{
    int64_t floor = 0;
    if (db->opt.clock_ms) { /* a test clock: part mtimes are on another time line */
        atomic_store(&db->clock_floor, floor);
        return;
    }
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        floor = MAX(floor, (int64_t)db->parts[i]->created * 1000);
    pthread_mutex_unlock(&db->lock);
    atomic_store(&db->clock_floor, floor);
}

int64_t eng_now_ms(eng *db)
{
    int64_t now;
    if (db->opt.clock_ms) {
        now = db->opt.clock_ms(db->opt.clock_arg);
    } else {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        now = (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    }
    int64_t floor = atomic_load(&db->clock_floor);
    while (now > floor && !atomic_compare_exchange_weak(&db->clock_floor, &floor, now)) {
    }
    return MAX(now, floor);
}

int64_t eng_retention(eng *db, uint32_t series)
{
    if (!atomic_load_explicit(&db->has_retention, memory_order_relaxed))
        return 0;
    ret_table *t = atomic_load_explicit(&db->ret_tab, memory_order_acquire);
    return t && series < t->cap ? atomic_load_explicit(&t->limit[series], memory_order_relaxed) : 0;
}

int64_t eng_cutoff_at(eng *db, uint32_t series, int64_t now)
{
    int64_t limit = eng_retention(db, series);
    if (limit <= 0)
        return INT64_MIN;
    return now < INT64_MIN + limit ? INT64_MIN : now - limit;
}

int64_t eng_cutoff(eng *db, uint32_t series)
{
    if (!atomic_load_explicit(&db->has_retention, memory_order_relaxed))
        return INT64_MIN;
    return eng_cutoff_at(db, series, eng_now_ms(db));
}

int eng_set_retention(eng *db, uint32_t series, int64_t limit_ms)
{
    if (db->opt.read_only)
        return TSDB_ERR_READONLY;
    if (limit_ms < 0 || series >= eng_series_count(db))
        return TSDB_ERR_INVALID;
    int rc = eng_check(db);
    if (rc)
        return rc;
    pthread_mutex_lock(&db->ret_lock);
    if (db->ret_broken) {
        pthread_mutex_unlock(&db->ret_lock);
        return TSDB_ERR_IO;
    }
    if (db->ret_fd < 0) {
        bool existed = faccessat(db->dirfd, RET_FILE, F_OK, 0) == 0;
        db->ret_fd = openat(db->dirfd, RET_FILE, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (db->ret_fd < 0 || (!existed && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)) {
            pthread_mutex_unlock(&db->ret_lock);
            return TSDB_ERR_IO;
        }
    }
    uint8_t rec[RET_REC];
    memcpy(rec + 4, &series, 4);
    memcpy(rec + 8, &limit_ms, 8);
    uint32_t c = rec_check(rec, db->ret_off);
    memcpy(rec, &c, 4);
    ssize_t w;
    do
        w = pwrite(db->ret_fd, rec, RET_REC, (off_t)db->ret_off);
    while (w < 0 && errno == EINTR);
    if (w != RET_REC || (db->opt.fsync && tsdb_fdatasync(db->ret_fd) != 0)) {
        db->ret_broken = true; /* the file may end in a torn record: append no more */
        pthread_mutex_unlock(&db->ret_lock);
        return TSDB_ERR_IO;
    }
    db->ret_off += RET_REC;
    rc = ret_put_locked(db, series, limit_ms);
    pthread_mutex_unlock(&db->ret_lock);
    if (rc == 0) {
        pthread_mutex_lock(&db->lock);
        pthread_cond_broadcast(&db->merge_cv); /* expired rows may be there to drop now */
        pthread_mutex_unlock(&db->lock);
        rc = manifest_publish(db); /* a new MANIFEST version: followers read RETENTION again */
    }
    return rc;
}
