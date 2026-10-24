/*
 * Series dictionary: name <-> dense uint32 id, and each series' value type.
 * Persisted as an append-only file of records; the id is the record index:
 *
 *   [u16 length][name]                  a float32 series (as written by version 1)
 *   [u16 length | 0x8000][u8 type][name] any other type
 *
 * The state dictionary (STATES: state names and their codes) is the same
 * structure in another file, without types.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAX_NAME 32767
#define TYPED 0x8000u

static uint32_t name_hash(const char *s, size_t len) { return (uint32_t)xxh64(s, len, 0x5e71e5); }

static int table_grow(series_dict *d)
{
    uint32_t cap = d->tcap ? d->tcap * 2 : 1024;
    uint32_t *t = calloc(cap, sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    uint32_t n = atomic_load(&d->count);
    for (uint32_t id = 0; id < n; id++) {
        uint32_t h = name_hash(d->names[id], strlen(d->names[id])) & (cap - 1);
        while (t[h])
            h = (h + 1) & (cap - 1);
        t[h] = id + 1;
    }
    free(d->table);
    d->table = t;
    d->tcap = cap;
    return 0;
}

static int64_t table_find(const series_dict *d, const char *name, size_t len, uint32_t *slot)
{
    uint32_t h = name_hash(name, len) & (d->tcap - 1);
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
    if ((uint64_t)(n + 1) * 2 > d->tcap && table_grow(d) != 0)
        return TSDB_ERR_NOMEM;
    if (n == d->cap) {
        uint32_t cap = d->cap ? d->cap * 2 : 256;
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
    uint32_t slot = 0;
    table_find(d, copy, len, &slot);
    d->names[n] = copy;
    d->types[n] = (uint8_t)type;
    d->table[slot] = n + 1;
    atomic_store(&d->count, n + 1);
    *id = n;
    return 0;
}

/* One record at buf[pos..size): its name, length and type; false when torn. */
static bool parse_record(const uint8_t *buf, size_t pos, size_t size, const char **name, size_t *len, int *type,
                         size_t *rec)
{
    if (pos + 2 > size)
        return false;
    size_t h = (size_t)buf[pos] | ((size_t)buf[pos + 1] << 8), hdr = 2;
    *type = TSDB_FLOAT32;
    if (h & TYPED) {
        if (pos + 3 > size)
            return false;
        *type = buf[pos + 2];
        hdr = 3;
    }
    *len = h & ~TYPED;
    if (pos + hdr + *len > size)
        return false;
    *name = (const char *)buf + pos + hdr;
    *rec = hdr + *len;
    return true;
}

int series_open(series_dict *d, int dirfd, const char *file, bool read_only, bool fsync)
{
    memset(d, 0, sizeof *d);
    pthread_mutex_init(&d->lock, NULL);
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
    size_t got = 0;
    while (got < size) {
        ssize_t r = pread(d->fd, buf + got, size - got, (off_t)got);
        if (r <= 0) {
            free(buf);
            return TSDB_ERR_IO;
        }
        got += (size_t)r;
    }
    int rc = table_grow(d);
    size_t pos = 0, len, rec;
    const char *name;
    int type;
    /* a torn append at the end is ignored: it was never acknowledged */
    while (rc == 0 && parse_record(buf, pos, size, &name, &len, &type, &rec)) {
        uint32_t id;
        rc = type_valid(type) ? dict_add(d, name, len, type, &id) : TSDB_ERR_CORRUPT;
        pos += rec;
    }
    free(buf);
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
        ssize_t got = buf ? pread(fd, buf, len, (off_t)off) : -1;
        size_t pos = 0, n, rec;
        const char *name;
        int type;
        /* a record that does not fit is still being appended */
        while (got > 0 && rc == 0 && parse_record(buf, pos, (size_t)got, &name, &n, &type, &rec)) {
            uint32_t id;
            rc = type_valid(type) ? dict_add(d, name, n, type, &id) : TSDB_ERR_CORRUPT;
            pos += rec;
        }
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
}

int series_get(series_dict *d, const char *name, bool create, int type, uint32_t *id)
{
    size_t len = strlen(name);
    if (len == 0 || len > MAX_NAME || (create && !type_valid(type)))
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&d->lock);
    uint32_t slot;
    int64_t found = table_find(d, name, len, &slot);
    int rc = 0;
    if (found >= 0) {
        *id = (uint32_t)found;
        if (create && d->types[found] != type)
            rc = TSDB_ERR_INVALID; /* exists with another type */
    } else if (!create) {
        rc = TSDB_ERR_NOTFOUND;
    } else if (d->fd < 0) {
        rc = TSDB_ERR_READONLY;
    } else if (d->broken) {
        rc = TSDB_ERR_IO; /* a record after a torn one would be lost at the next open, yet acknowledged */
    } else {
        size_t hdr = type == TSDB_FLOAT32 ? 2 : 3; /* float32: readable by version 1 */
        uint8_t *rec = malloc(len + hdr);
        if (!rec) {
            rc = TSDB_ERR_NOMEM;
        } else {
            size_t h = len | (hdr == 3 ? TYPED : 0);
            rec[0] = (uint8_t)(h & 0xff);
            rec[1] = (uint8_t)(h >> 8);
            if (hdr == 3)
                rec[2] = (uint8_t)type;
            memcpy(rec + hdr, name, len);
            int err = 0;
            ssize_t wr = write(d->fd, rec, len + hdr);
            if (wr != (ssize_t)(len + hdr) || (d->fsync && fdatasync(d->fd) != 0)) {
                rc = TSDB_ERR_IO; /* the caller fails the engine (dict_get): see eng_fail */
                err = wr >= 0 && wr != (ssize_t)(len + hdr) ? ENOSPC : errno;
            }
            free(rec);
            if (rc == TSDB_ERR_IO) {
                /* Set before the unlock: the caller fails the engine only after it, and another creator
                 * waiting for the lock must not append behind the torn record meanwhile. */
                d->broken = true;
                pthread_mutex_unlock(&d->lock);
                errno = err;
                return rc;
            }
            if (rc == 0)
                rc = dict_add(d, name, len, type, id);
        }
    }
    pthread_mutex_unlock(&d->lock);
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
bool series_check(series_dict *d, const uint32_t *series, uint32_t cs, size_t n, int type)
{
    pthread_mutex_lock(&d->lock);
    uint32_t count = atomic_load(&d->count);
    bool ok = true;
    if (!series)
        ok = cs < count && d->types[cs] == type;
    /* rows come grouped by series: checking where the series changes is enough */
    for (size_t i = 0; series && ok && i < n; i++)
        if (i == 0 || series[i] != series[i - 1])
            ok = series[i] < count && d->types[series[i]] == type;
    pthread_mutex_unlock(&d->lock);
    return ok;
}
