/*
 * Series dictionary: name <-> dense uint32 id, and each series' value type.
 * Persisted as an append-only file of records; the id is the record index:
 *
 *   [u32 check][u16 length][u8 type][u8 0][name][zeros to a multiple of 8]
 *
 * The check (low 32 bits of xxh64 of the rest, seeded with the record's offset)
 * tells a record from zeros, garbage or a torn append. Open keeps the records
 * up to the first bad one. That one is a torn tail, cut, unless a valid record
 * follows it: then it is damage inside what was synced, and the open fails
 * (TSDB_ERR_CORRUPT) rather than shift every later id or drop names.
 *
 * The state dictionary (STATES: state names and their codes) is the same
 * structure in another file, without types.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MAX_NAME 32767
#define REC_HDR 8
/* Names per dictionary: ids stay below 2^31, and the table (twice as many slots) stays addressable by a size_t
   without wrapping. A create beyond is refused before its record is written; a file with more is damage. */
#define DICT_MAX_NAMES ((1u << 31) - 1)

static size_t name_hash(const series_dict *d, const char *s, size_t len) { return (size_t)xxh64(s, len, d->seed); }

static int table_grow(series_dict *d)
{
    size_t cap = d->tcap ? d->tcap * 2 : 1024;
    uint32_t *t = calloc(cap, sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    uint32_t n = atomic_load(&d->count);
    for (uint32_t id = 0; id < n; id++) {
        size_t h = name_hash(d, d->names[id], strlen(d->names[id])) & (cap - 1);
        while (t[h])
            h = (h + 1) & (cap - 1);
        t[h] = id + 1;
    }
    free(d->table);
    d->table = t;
    d->tcap = cap;
    return 0;
}

static int64_t table_find(const series_dict *d, const char *name, size_t len, size_t *slot)
{
    size_t h = name_hash(d, name, len) & (d->tcap - 1);
    while (d->table[h]) {
        uint32_t id = d->table[h] - 1;
        if (strcmp(d->names[id], name) == 0)
            return id;
        h = (h + 1) & (d->tcap - 1);
    }
    *slot = h;
    return -1;
}

/* Adds a name in memory; caller holds the lock (or is single threaded). */
static int dict_add(series_dict *d, const char *name, size_t len, int type, uint32_t *id)
{
    uint32_t n = atomic_load(&d->count);
    if (n >= DICT_MAX_NAMES)
        return TSDB_ERR_CORRUPT; /* series_get refuses such a create, so only a damaged or crafted file has more */
    if ((uint64_t)(n + 1) * 2 > d->tcap && table_grow(d) != 0)
        return TSDB_ERR_NOMEM;
    if (n == d->cap) {
        size_t cap = d->cap ? d->cap * 2 : 256;
        char **names = realloc(d->names, cap * sizeof *names);
        if (!names)
            return TSDB_ERR_NOMEM;
        d->names = names;
        uint8_t *types = realloc(d->types, cap);
        if (!types)
            return TSDB_ERR_NOMEM;
        d->types = types;
        d->cap = cap;
    }
    char *copy = malloc(len + 1);
    if (!copy)
        return TSDB_ERR_NOMEM;
    memcpy(copy, name, len);
    copy[len] = '\0';
    size_t slot = 0;
    table_find(d, copy, len, &slot);
    d->names[n] = copy;
    d->types[n] = (uint8_t)type;
    d->table[slot] = n + 1;
    atomic_store(&d->count, n + 1);
    *id = n;
    return 0;
}

static size_t rec_size(size_t len) { return (REC_HDR + len + 7) & ~(size_t)7; }

static uint32_t rec_check(const uint8_t *rec, size_t len, uint64_t off)
{
    return (uint32_t)xxh64(rec + 4, rec_size(len) - 4, off);
}

/* The record at buf[pos..size), which is at file offset base + pos: its name,
 * length and type; false when it is torn, damaged or not a record. */
static bool parse_record(const uint8_t *buf, size_t pos, size_t size, uint64_t base, const char **name, size_t *len,
                         int *type, size_t *rec)
{
    if (pos + REC_HDR > size)
        return false;
    const uint8_t *r = buf + pos;
    *len = (size_t)r[4] | ((size_t)r[5] << 8);
    if (*len == 0 || *len > MAX_NAME || r[7] != 0 || rec_size(*len) > size - pos ||
        rec_check(r, *len, base + pos) != load32(r))
        return false;
    *type = r[6];
    *name = (const char *)r + REC_HDR;
    *rec = rec_size(*len);
    return true;
}

/* Whether a valid record starts in buf[from, size): records are 8-byte aligned. */
static bool valid_after(const uint8_t *buf, size_t from, size_t size, uint64_t base)
{
    const char *name;
    size_t len, rec;
    int type;
    for (size_t q = ((base + from + 8) & ~(uint64_t)7) - base; q < size; q += 8)
        if (parse_record(buf, q, size, base, &name, &len, &type, &rec))
            return true;
    return false;
}

static int read_all(int fd, uint8_t *buf, size_t len, uint64_t off)
{
    for (size_t got = 0; got < len;) {
        ssize_t r = pread(fd, buf + got, len - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return TSDB_ERR_IO; /* a short read too: never taken for the end */
        got += (size_t)r;
    }
    return 0;
}

int series_open(series_dict *d, int dirfd, const char *file, bool read_only, bool fsync)
{
    memset(d, 0, sizeof *d);
    pthread_mutex_init(&d->lock, NULL);
    pthread_mutex_init(&d->append_lock, NULL);
    if (getrandom(&d->seed, sizeof d->seed, GRND_NONBLOCK) != sizeof d->seed)
        d->seed = xxh64(&d, sizeof d, (uint64_t)time(NULL) ^ (uint64_t)getpid()); /* no entropy yet: still not public */
    d->file = file;
    d->fsync = fsync;
    d->fd = openat(dirfd, file, (read_only ? O_RDONLY : O_RDWR | O_CREAT | O_APPEND) | O_CLOEXEC, 0644);
    if (d->fd < 0)
        return (read_only && errno == ENOENT) ? table_grow(d) : TSDB_ERR_IO;
    struct stat st;
    if (fstat(d->fd, &st) != 0)
        return TSDB_ERR_IO;
    size_t size = (size_t)st.st_size;
    uint8_t *buf = malloc(size ? size : 1);
    if (!buf)
        return TSDB_ERR_NOMEM;
    if (read_all(d->fd, buf, size, 0) != 0) {
        free(buf);
        return TSDB_ERR_IO;
    }
    int rc = table_grow(d);
    size_t pos = 0, len, rec;
    const char *name;
    int type;
    while (rc == 0 && parse_record(buf, pos, size, 0, &name, &len, &type, &rec)) {
        uint32_t id;
        rc = type_valid(type) ? dict_add(d, name, len, type, &id) : TSDB_ERR_CORRUPT;
        pos += rec;
    }
    if (rc == 0 && pos != size && valid_after(buf, pos, size, 0)) {
        fprintf(stderr, "tsdb: %s is damaged at offset %zu, before valid records\n", file, pos);
        rc = TSDB_ERR_CORRUPT;
    }
    free(buf);
    /* a torn append at the end is cut: it was never acknowledged */
    if (rc == 0 && pos != size && !read_only && ftruncate(d->fd, (off_t)pos) != 0)
        rc = TSDB_ERR_IO;
    d->file_off = pos;
    return rc;
}

/* Loads names a writer appended since. The file is reopened every time: on
 * NFS that is what revalidates its size (close-to-open consistency). */
int series_reload(series_dict *d, int dirfd)
{
    int fd = openat(dirfd, d->file, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct stat st;
    int rc = 0;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    pthread_mutex_lock(&d->lock);
    uint64_t off = d->file_off, size = (uint64_t)st.st_size;
    if (size > off) {
        size_t len = (size_t)(size - off);
        uint8_t *buf = malloc(len);
        rc = !buf ? TSDB_ERR_NOMEM : read_all(fd, buf, len, off);
        size_t pos = 0, n, rec;
        const char *name;
        int type;
        /* a record that does not check out is still being appended */
        while (rc == 0 && parse_record(buf, pos, len, off, &name, &n, &type, &rec)) {
            uint32_t id;
            rc = type_valid(type) ? dict_add(d, name, n, type, &id) : TSDB_ERR_CORRUPT;
            pos += rec;
        }
        if (rc == 0 && pos != len && valid_after(buf, pos, len, off))
            rc = TSDB_ERR_CORRUPT; /* reported by the refresh; read again from here next time */
        d->file_off = off + pos;
        free(buf);
    }
    pthread_mutex_unlock(&d->lock);
    close(fd);
    return rc;
}

void series_close(series_dict *d)
{
    uint32_t n = atomic_load(&d->count);
    for (uint32_t i = 0; i < n; i++)
        free(d->names[i]);
    free(d->names);
    free(d->types);
    free(d->table);
    if (d->fd >= 0)
        close(d->fd);
    pthread_mutex_destroy(&d->lock);
    pthread_mutex_destroy(&d->append_lock);
}

/* Appends a new name. The write and its fdatasync hold only append_lock, so lookups and the other names of this
 * dictionary (d->lock) never wait for a slow or hung disk; appends take turns, and each re-checks the name under
 * append_lock, so two creators of one name get one record. */
static int dict_append(series_dict *d, const char *name, size_t len, int type, uint32_t *id)
{
    pthread_mutex_lock(&d->append_lock);
    pthread_mutex_lock(&d->lock);
    size_t slot;
    int64_t found = table_find(d, name, len, &slot);
    int rc = 0;
    if (found >= 0) {
        *id = (uint32_t)found;
        if (d->types[found] != type)
            rc = TSDB_ERR_INVALID; /* exists with another type */
    } else if (d->fd < 0) {
        rc = TSDB_ERR_READONLY;
    } else if (d->broken) {
        rc = TSDB_ERR_IO; /* a record after a torn one would be lost at the next open, yet acknowledged */
    } else if (atomic_load(&d->count) >= DICT_MAX_NAMES) {
        rc = TSDB_ERR_INVALID; /* full: checked before the record is written, or every open would fail on it */
    }
    uint64_t off = d->file_off; /* only appends move it, and they hold append_lock */
    pthread_mutex_unlock(&d->lock);
    if (found >= 0 || rc != 0) {
        pthread_mutex_unlock(&d->append_lock);
        return rc;
    }
    size_t size = rec_size(len);
    uint8_t *rec = calloc(1, size);
    if (!rec) {
        pthread_mutex_unlock(&d->append_lock);
        return TSDB_ERR_NOMEM;
    }
    rec[4] = (uint8_t)(len & 0xff);
    rec[5] = (uint8_t)(len >> 8);
    rec[6] = (uint8_t)type;
    memcpy(rec + REC_HDR, name, len);
    store32(rec, rec_check(rec, len, off));
    int err = 0;
    ssize_t wr = write(d->fd, rec, size);
    if (wr != (ssize_t)size || (d->fsync && fdatasync(d->fd) != 0)) {
        rc = TSDB_ERR_IO; /* the caller fails the engine (dict_get): see eng_fail */
        err = wr >= 0 && wr != (ssize_t)size ? ENOSPC : errno;
    }
    free(rec);
    pthread_mutex_lock(&d->lock);
    if (rc == 0) {
        d->file_off = off + size;
        rc = dict_add(d, name, len, type, id);
    } else {
        /* Set before append_lock is released: the caller fails the engine only after it, and another creator
         * waiting for append_lock must not append behind the torn record meanwhile. */
        d->broken = true;
    }
    pthread_mutex_unlock(&d->lock);
    pthread_mutex_unlock(&d->append_lock);
    if (rc == TSDB_ERR_IO)
        errno = err;
    return rc;
}

int series_get(series_dict *d, const char *name, bool create, int type, uint32_t *id)
{
    size_t len = strlen(name);
    if (len == 0 || len > MAX_NAME || (create && !type_valid(type)))
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&d->lock);
    size_t slot;
    int64_t found = table_find(d, name, len, &slot);
    int rc = 0;
    if (found >= 0) {
        *id = (uint32_t)found;
        if (create && d->types[found] != type)
            rc = TSDB_ERR_INVALID; /* exists with another type */
    } else if (!create) {
        rc = TSDB_ERR_NOTFOUND;
    }
    pthread_mutex_unlock(&d->lock);
    if (found < 0 && create)
        rc = dict_append(d, name, len, type, id);
    return rc;
}

const char *series_name(series_dict *d, uint32_t id)
{
    pthread_mutex_lock(&d->lock);
    const char *name = id < atomic_load(&d->count) ? d->names[id] : NULL;
    pthread_mutex_unlock(&d->lock);
    return name;
}

int series_type(series_dict *d, uint32_t id)
{
    pthread_mutex_lock(&d->lock);
    int t = id < atomic_load(&d->count) ? d->types[id] : -1;
    pthread_mutex_unlock(&d->lock);
    return t;
}

/* Whether every series of a batch exists and has `type` (series NULL: cs alone). */
bool series_check(series_dict *d, const uint32_t *series, uint32_t cs, size_t n, int type, struct cls_table *ct,
                  int *ucls)
{
#define CLS_OF(id) (ct && (id) < ct->cap ? atomic_load_explicit(&ct->b[id], memory_order_relaxed) : 0)
    pthread_mutex_lock(&d->lock);
    uint32_t count = atomic_load(&d->count);
    bool ok = true, same = true;
    uint8_t b0 = 0;
    if (!series) {
        ok = cs < count && d->types[cs] == type;
        b0 = CLS_OF(cs);
    }
    /* rows come grouped by series: checking where the series changes is enough */
    for (size_t i = 0; series && ok && i < n; i++)
        if (i == 0 || series[i] != series[i - 1]) {
            ok = series[i] < count && d->types[series[i]] == type;
            uint8_t b = CLS_OF(series[i]);
            if (i == 0)
                b0 = b;
            if (!(b & CLS_DECIDED))
                b0 = 0; /* fresh: reported below whatever follows */
            same &= b == b0;
        }
    pthread_mutex_unlock(&d->lock);
#undef CLS_OF
    if (ucls)
        *ucls = !(b0 & CLS_DECIDED) ? -2 : same && !(b0 & CLS_HIST) ? (int)(b0 & CLS_MASK) : -1;
    return ok;
}
