/*
 * Series dictionary: name <-> dense uint32 id. Persisted as an append-only
 * file of [u16 length][name bytes] records; the id is the record index.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#define SERIES_FILE "SERIES"
#define MAX_NAME 65535

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
static int dict_add(series_dict *d, const char *name, size_t len, uint32_t *id)
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
    d->table[slot] = n + 1;
    atomic_store(&d->count, n + 1);
    *id = n;
    return 0;
}

int series_open(series_dict *d, int dirfd, bool read_only, bool fsync)
{
    memset(d, 0, sizeof *d);
    pthread_mutex_init(&d->lock, NULL);
    d->fsync = fsync;
    d->fd = openat(dirfd, SERIES_FILE, (read_only ? O_RDONLY : O_RDWR | O_CREAT | O_APPEND) | O_CLOEXEC, 0644);
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
    size_t pos = 0;
    while (rc == 0 && pos + 2 <= size) {
        size_t len = (size_t)buf[pos] | ((size_t)buf[pos + 1] << 8);
        if (pos + 2 + len > size)
            break; /* torn append at the end: ignore it, it was never acknowledged */
        uint32_t id;
        rc = dict_add(d, (const char *)buf + pos + 2, len, &id);
        pos += 2 + len;
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
    int fd = openat(dirfd, SERIES_FILE, O_RDONLY | O_CLOEXEC);
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
        size_t pos = 0;
        while (got > 0 && rc == 0 && pos + 2 <= (size_t)got) {
            size_t n = (size_t)buf[pos] | ((size_t)buf[pos + 1] << 8);
            if (pos + 2 + n > (size_t)got)
                break; /* the writer is still appending this one */
            uint32_t id;
            rc = dict_add(d, (const char *)buf + pos + 2, n, &id);
            pos += 2 + n;
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
    free(d->table);
    if (d->fd >= 0)
        close(d->fd);
    pthread_mutex_destroy(&d->lock);
}

int series_get(series_dict *d, const char *name, bool create, uint32_t *id)
{
    size_t len = strlen(name);
    if (len == 0 || len > MAX_NAME)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&d->lock);
    uint32_t slot;
    int64_t found = table_find(d, name, len, &slot);
    int rc = 0;
    if (found >= 0) {
        *id = (uint32_t)found;
    } else if (!create) {
        rc = TSDB_ERR_NOTFOUND;
    } else if (d->fd < 0) {
        rc = TSDB_ERR_READONLY;
    } else {
        uint8_t *rec = malloc(len + 2);
        if (!rec) {
            rc = TSDB_ERR_NOMEM;
        } else {
            rec[0] = (uint8_t)(len & 0xff);
            rec[1] = (uint8_t)(len >> 8);
            memcpy(rec + 2, name, len);
            if (write(d->fd, rec, len + 2) != (ssize_t)(len + 2) || (d->fsync && fdatasync(d->fd) != 0))
                rc = TSDB_ERR_IO;
            free(rec);
            if (rc == 0)
                rc = dict_add(d, name, len, id);
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
