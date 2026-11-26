/* SPDX-License-Identifier: AGPL-3.0-only */
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

static uint64_t name_hash(const series_dict *d, const char *s, size_t len) { return xxh64(s, len, d->seed); }

static inline uint16_t hash_tag(uint64_t h) { return (uint16_t)(h >> 48); }

static series_tab *series_tab_alloc(uint32_t cap)
{
    size_t stride = sizeof(char *) + sizeof(uint16_t) + sizeof(uint8_t);
    if ((size_t)cap > (SIZE_MAX - sizeof(series_tab)) / stride)
        return NULL;
    series_tab *t = malloc(sizeof *t + (size_t)cap * stride);
    if (!t)
        return NULL;
    t->older = NULL;
    t->names = (char **)((uint8_t *)t + sizeof *t);
    t->lens = (uint16_t *)(t->names + cap);
    t->types = (uint8_t *)(t->lens + cap);
    t->cap = cap;
    t->pad = 0;
    return t;
}

static series_tab *series_tab_grow_prep(series_tab *old, uint32_t n)
{
    uint32_t cap = old ? (old->cap <= DICT_MAX_NAMES / 2 ? old->cap * 2 : DICT_MAX_NAMES) : 256;
    if (cap <= n)
        cap = n < DICT_MAX_NAMES ? n + 1 : DICT_MAX_NAMES;
    series_tab *t = series_tab_alloc(cap);
    if (!t)
        return NULL;
    t->older = old;
    if (old && n) {
        memcpy(t->names, old->names, (size_t)n * sizeof *t->names);
        memcpy(t->lens, old->lens, (size_t)n * sizeof *t->lens);
        memcpy(t->types, old->types, (size_t)n * sizeof *t->types);
    }
    return t;
}

static series_slot *table_grow_prep(const series_dict *d, const series_tab *tab, uint32_t n, size_t *out_cap)
{
    size_t cap = d->tcap ? d->tcap * 2 : 1024;
    while ((uint64_t)(n + 1) * 2 > cap)
        cap *= 2;
    series_slot *t = calloc(cap, sizeof *t);
    if (!t)
        return NULL;
    size_t mask = cap - 1;
    for (uint32_t id = 0; id < n; id++) {
        uint16_t len = tab->lens[id];
        uint64_t hash = name_hash(d, tab->names[id], len);
        size_t h = (size_t)hash & mask;
        while (t[h].id1)
            h = (h + 1) & mask;
        t[h] = (series_slot){.id1 = id + 1, .len = len, .tag = hash_tag(hash)};
    }
    *out_cap = cap;
    return t;
}

static int table_grow(series_dict *d, series_slot **old_out, size_t *old_cap_out)
{
    uint32_t n = atomic_load_explicit(&d->count, memory_order_relaxed);
    series_tab *tab = atomic_load_explicit(&d->tab, memory_order_relaxed);
    size_t cap = 0;
    series_slot *t = table_grow_prep(d, tab, n, &cap);
    if (!t)
        return TSDB_ERR_NOMEM;
    if (old_out) {
        *old_out = d->table;
        *old_cap_out = d->tcap;
    } else if (d->table) {
        tsdb_free_big(d->table, d->tcap * sizeof *d->table);
    }
    d->table = t;
    d->tcap = cap;
    return 0;
}

static int64_t table_find(const series_dict *d, const char *name, size_t len, uint64_t hash, size_t *slot)
{
    size_t mask = d->tcap - 1;
    size_t h = (size_t)hash & mask;
    uint16_t tag = hash_tag(hash);
    uint16_t ulen = (uint16_t)len;
    while (d->table[h].id1) {
        if (d->table[h].tag == tag && d->table[h].len == ulen) {
            uint32_t id = d->table[h].id1 - 1;
            if (memcmp(d->names[id], name, len) == 0)
                return id;
        }
        h = (h + 1) & mask;
    }
    *slot = h;
    return -1;
}

static int dict_hook(series_dict *d, int point, const char *name)
{
    int (*fn)(void *, int, const char *) = __atomic_load_n(&d->test_fault, __ATOMIC_ACQUIRE);
    return fn ? fn(__atomic_load_n(&d->test_fault_arg, __ATOMIC_ACQUIRE), point, name) : 0;
}

/* Adds a name in memory; caller holds d->lock (or is single threaded). Optional pre-built resources (prepared outside
 * d->lock under append_lock) are consumed on success or freed by the caller on failure. */
static int dict_add_ex(series_dict *d, const char *name, size_t len, uint64_t hash, int type, uint32_t *id,
                       series_slot *pre_table, size_t pre_tcap, series_tab *pre_tab, char *pre_copy,
                       series_slot **old_table_out, size_t *old_tcap_out)
{
    uint32_t n = atomic_load_explicit(&d->count, memory_order_relaxed);
    if (n >= DICT_MAX_NAMES)
        return TSDB_ERR_CORRUPT; /* series_get refuses such a create, so only a damaged or crafted file has more */
    series_tab *tab = atomic_load_explicit(&d->tab, memory_order_relaxed);
    series_tab *ntab = pre_tab;
    bool own_ntab = false;
    if (!ntab && (!tab || n == tab->cap)) {
        ntab = series_tab_grow_prep(tab, n);
        if (!ntab)
            return TSDB_ERR_NOMEM;
        own_ntab = true;
    }
    char *copy = pre_copy;
    bool own_copy = false;
    if (!copy) {
        copy = malloc(len + 1);
        if (!copy) {
            if (own_ntab)
                free(ntab);
            return TSDB_ERR_NOMEM;
        }
        memcpy(copy, name, len);
        copy[len] = '\0';
        own_copy = true;
    }
    if (dict_hook(d, DICT_AT_ADD, copy)) {
        if (own_copy)
            free(copy);
        if (own_ntab)
            free(ntab);
        return TSDB_ERR_NOMEM;
    }
    if (pre_table) {
        if (old_table_out) {
            *old_table_out = d->table;
            *old_tcap_out = d->tcap;
        } else if (d->table) {
            tsdb_free_big(d->table, d->tcap * sizeof *d->table);
        }
        d->table = pre_table;
        d->tcap = pre_tcap;
    } else if ((uint64_t)(n + 1) * 2 > d->tcap) {
        if (table_grow(d, old_table_out, old_tcap_out) != 0) {
            if (own_copy)
                free(copy);
            if (own_ntab)
                free(ntab);
            return TSDB_ERR_NOMEM;
        }
    }
    if (ntab) {
        tab = ntab;
        d->names = tab->names;
        d->types = tab->types;
        d->cap = tab->cap;
    }
    size_t slot = 0;
    table_find(d, copy, len, hash, &slot);
    tab->names[n] = copy;
    tab->lens[n] = (uint16_t)len;
    tab->types[n] = (uint8_t)type;
    if (ntab)
        atomic_store_explicit(&d->tab, tab, memory_order_release);
    d->table[slot] = (series_slot){.id1 = n + 1, .len = (uint16_t)len, .tag = hash_tag(hash)};
    atomic_store_explicit(&d->count, n + 1, memory_order_release);
    *id = n;
    return 0;
}

static int dict_add(series_dict *d, const char *name, size_t len, int type, uint32_t *id)
{
    return dict_add_ex(d, name, len, name_hash(d, name, len), type, id, NULL, 0, NULL, NULL, NULL, NULL);
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

/* A tail about to be cut as torn is read once more, from the disk: a read that returned bytes other than the file's
 * (a misdirected read, a page misread) makes valid records look torn, and cutting them would lose acknowledged
 * writes for a fault that a second read does not repeat. The cached pages are dropped first, since a misread page
 * stays in the page cache. Reads that differ fail the open (TSDB_ERR_IO) and cut nothing; a later open reads again. */
int tail_confirm(int dirfd, const char *file, const uint8_t *tail, uint64_t off, size_t len)
{
    int fd = openat(dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    (void)posix_fadvise(fd, (off_t)off, (off_t)len, POSIX_FADV_DONTNEED);
    uint8_t *b = malloc(len ? len : 1);
    int rc = !b ? TSDB_ERR_NOMEM : read_all(fd, b, len, off);
    if (rc == 0 && memcmp(b, tail, len) != 0) {
        fprintf(stderr, "tsdb: %s: two reads of its tail at offset %llu differ: the open fails and nothing is cut\n",
                file, (unsigned long long)off);
        rc = TSDB_ERR_IO;
    }
    free(b);
    close(fd);
    return rc;
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
        return (read_only && errno == ENOENT) ? table_grow(d, NULL, NULL) : TSDB_ERR_IO;
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
    int rc = table_grow(d, NULL, NULL);
    size_t pos = 0, len, rec;
    const char *name;
    int type;
    while (rc == 0 && parse_record(buf, pos, size, 0, &name, &len, &type, &rec)) {
        uint32_t id;
        rc = type_valid(type) ? dict_add(d, name, len, type, &id) : TSDB_ERR_CORRUPT;
        d->last_off = pos;
        d->last_check = load32(buf + pos);
        pos += rec;
    }
    /* Single-threaded open: no lock-free reader exists yet, so discard intermediate growth snapshots. */
    series_tab *cur = atomic_load_explicit(&d->tab, memory_order_relaxed);
    if (cur) {
        for (series_tab *t = cur->older, *o; t; t = o) {
            o = t->older;
            free(t);
        }
        cur->older = NULL;
    }
    if (rc == 0 && pos != size && valid_after(buf, pos, size, 0)) {
        fprintf(stderr, "tsdb: %s is damaged at offset %zu, before valid records\n", file, pos);
        rc = TSDB_ERR_CORRUPT;
    }
    /* A torn append at the end is cut: it was never acknowledged, or the last record was damaged at rest, which
       looks the same (hence the log line). Not here, though: series_cut_tail does it once the open's checks
       passed, since a part or a WAL row naming a series of the cut record proves it was acknowledged, and a
       refused open leaves the bytes for the operator to repair. */
    if (rc == 0 && pos != size && !read_only)
        rc = tail_confirm(dirfd, file, buf + pos, pos, size - pos);
    free(buf);
    d->tail_pending = rc == 0 && pos != size && !read_only;
    d->file_off = pos;
    return rc;
}

int series_cut_tail(series_dict *d)
{
    if (!d->tail_pending)
        return 0;
    struct stat st;
    if (fstat(d->fd, &st) != 0)
        return TSDB_ERR_IO;
    if ((uint64_t)st.st_size > d->file_off) {
        fprintf(stderr, "tsdb: %s: cutting a torn tail of %llu bytes at offset %llu\n", d->file,
                (unsigned long long)st.st_size - (unsigned long long)d->file_off, (unsigned long long)d->file_off);
        if (ftruncate(d->fd, (off_t)d->file_off) != 0)
            return TSDB_ERR_IO;
    }
    d->tail_pending = false;
    return 0;
}

/* Loads names a writer appended since. The file is reopened every time: on
 * NFS that is what revalidates its size (close-to-open consistency). The read holds append_lock (one reload at a
 * time; a reader appends nothing), not lock: lookups of the names already known never wait for a file system that
 * hangs. */
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
    pthread_mutex_lock(&d->append_lock);
    uint64_t off = d->file_off, size = (uint64_t)st.st_size; /* moved only under append_lock */
    /* The last record read is read again with the new ones: a writer that cut it (damaged at rest, taken for a torn
     * tail) hands its id to the next name, maybe in a record of the same size, and this reader would go on naming
     * that id after the record it read. A file shorter than what was read says the same. */
    uint64_t from = d->last_off < off ? d->last_off : off;
    if (size < off) {
        rc = TSDB_ERR_CORRUPT; /* reported by the refresh; read again from here next time */
    } else if (size > from) {
        size_t len = (size_t)(size - from);
        uint8_t *buf = malloc(len);
        dict_hook(d, DICT_AT_RELOAD, d->file);
        rc = !buf ? TSDB_ERR_NOMEM : read_all(fd, buf, len, from);
        pthread_mutex_lock(&d->lock);
        size_t pos = (size_t)(off - from), n, rec;
        const char *name;
        int type;
        struct {
            series_slot *t;
            size_t cap;
        } old_tabs[40];
        size_t nold = 0;
        /* the record's check covers its name and type, seeded with its offset */
        if (rc == 0 && pos && (!parse_record(buf, 0, len, from, &name, &n, &type, &rec) || rec != pos ||
                               load32(buf) != d->last_check))
            rc = TSDB_ERR_CORRUPT;
        /* a record that does not check out is still being appended */
        size_t start = pos;
        while (rc == 0 && parse_record(buf, pos, len, from, &name, &n, &type, &rec)) {
            uint32_t id;
            series_slot *ot = NULL;
            size_t ocap = 0;
            rc = type_valid(type)
                     ? dict_add_ex(d, name, n, name_hash(d, name, n), type, &id, NULL, 0, NULL, NULL,
                                   nold < 40 ? &ot : NULL, nold < 40 ? &ocap : NULL)
                     : TSDB_ERR_CORRUPT;
            if (ot) {
                old_tabs[nold].t = ot;
                old_tabs[nold++].cap = ocap;
            }
            if (rc == 0) {
                d->last_off = from + pos;
                d->last_check = load32(buf + pos);
                pos += rec; /* a record not added is read again next time: ids stay the record numbers */
            }
        }
        if (rc == 0 && pos != len && valid_after(buf, pos, len, from))
            rc = TSDB_ERR_CORRUPT; /* reported by the refresh; read again from here next time */
        d->file_off = from + (pos > start ? pos : start);
        pthread_mutex_unlock(&d->lock);
        for (size_t i = 0; i < nold; i++)
            tsdb_free_big(old_tabs[i].t, old_tabs[i].cap * sizeof *old_tabs[i].t);
        free(buf);
    }
    pthread_mutex_unlock(&d->append_lock);
    close(fd);
    return rc;
}

void series_close(series_dict *d)
{
    uint32_t n = atomic_load_explicit(&d->count, memory_order_acquire);
    series_tab *cur = atomic_load_explicit(&d->tab, memory_order_acquire);
    uint32_t start = d->keep_names ? n : d->keep_prefix;
    for (uint32_t i = start; cur && i < n; i++)
        free(cur->names[i]);
    for (series_tab *t = cur, *o; t; t = o) {
        o = t->older;
        free(t);
    }
    atomic_store_explicit(&d->tab, NULL, memory_order_relaxed);
    d->names = NULL;
    d->types = NULL;
    free(d->table);
    d->table = NULL;
    if (d->fd >= 0)
        close(d->fd);
    pthread_mutex_destroy(&d->lock);
    pthread_mutex_destroy(&d->append_lock);
}

/* Appends a new name. The write and its fdatasync hold only append_lock, so lookups and the other names of this
 * dictionary (d->lock) never wait for a slow or hung disk; appends take turns, and each re-checks the name under
 * append_lock, so two creators of one name get one record. */
static int dict_append(series_dict *d, const char *name, size_t len, uint64_t hash, int type, uint32_t *id)
{
    pthread_mutex_lock(&d->append_lock);
    pthread_mutex_lock(&d->lock);
    size_t slot;
    int64_t found = table_find(d, name, len, hash, &slot);
    int rc = 0;
    if (found >= 0) {
        *id = (uint32_t)found;
        if (d->types[found] != type)
            rc = TSDB_ERR_INVALID; /* exists with another type */
    } else if (d->fd < 0) {
        rc = TSDB_ERR_READONLY;
    } else if (d->broken || d->tail_pending) {
        rc = TSDB_ERR_IO; /* a record after a torn one would be lost at the next open, yet acknowledged */
    } else if (atomic_load_explicit(&d->count, memory_order_relaxed) >= DICT_MAX_NAMES) {
        rc = TSDB_ERR_INVALID; /* full: checked before the record is written, or every open would fail on it */
    }
    uint64_t off = d->file_off; /* only appends move it, and they hold append_lock */
    pthread_mutex_unlock(&d->lock);
    if (found >= 0 || rc != 0) {
        pthread_mutex_unlock(&d->append_lock);
        if (rc == TSDB_ERR_IO)
            errno = 0; /* broken: the creator whose append failed fails the engine, with its cause (dict_get) */
        return rc;
    }
    /* A leader's marker and its commit point come before the record (jr_markers): the id stays untaken meanwhile,
     * since only appends (append_lock) take one. A failure was handled where it happened (errno 0: dict_get does
     * not fail the engine again). */
    if (d->before_append && (rc = d->before_append(d->before_arg, atomic_load(&d->count), name, len, type)) != 0) {
        pthread_mutex_unlock(&d->append_lock);
        errno = 0;
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
    uint32_t check = rec_check(rec, len, off);
    store32(rec, check);
    int err = 0;
    ssize_t wr = write(d->fd, rec, size);
    if (wr != (ssize_t)size || (d->fsync && tsdb_fdatasync(d->fd) != 0)) {
        rc = TSDB_ERR_IO; /* the caller fails the engine (dict_get): see eng_fail */
        err = wr >= 0 && wr != (ssize_t)size ? ENOSPC : errno;
    }
    free(rec);
    /* Under append_lock, count, tab and tcap cannot change: prepare grown hash table, grown snapshot and name copy
     * outside d->lock so concurrent series_get lookups never stall on rehashing or page faults. */
    char *pre_copy = NULL;
    series_slot *pre_table = NULL, *old_table = NULL;
    size_t pre_tcap = 0, old_tcap = 0;
    series_tab *pre_tab = NULL;
    bool prep_ok = true;
    if (rc == 0) {
        uint32_t n = atomic_load_explicit(&d->count, memory_order_relaxed);
        series_tab *cur_tab = atomic_load_explicit(&d->tab, memory_order_relaxed);
        pre_copy = malloc(len + 1);
        if (!pre_copy) {
            prep_ok = false;
        } else {
            memcpy(pre_copy, name, len);
            pre_copy[len] = '\0';
        }
        if (prep_ok && (!cur_tab || n == cur_tab->cap)) {
            pre_tab = series_tab_grow_prep(cur_tab, n);
            if (!pre_tab)
                prep_ok = false;
        }
        if (prep_ok && (uint64_t)(n + 1) * 2 > d->tcap) {
            pre_table = table_grow_prep(d, cur_tab, n, &pre_tcap);
            if (!pre_table)
                prep_ok = false;
        }
        if (!prep_ok) {
            free(pre_copy);
            free(pre_tab);
            free(pre_table);
            pre_copy = NULL;
            pre_tab = NULL;
            pre_table = NULL;
        }
    }
    pthread_mutex_lock(&d->lock);
    if (rc == 0) {
        d->last_off = off;
        d->last_check = check;
        d->file_off = off + size;
        rc = prep_ok ? dict_add_ex(d, name, len, hash, type, id, pre_table, pre_tcap, pre_tab, pre_copy, &old_table,
                                   &old_tcap)
                     : TSDB_ERR_NOMEM;
        if (rc != 0) {
            /* The record is durable but not in the table: the next name would get its id here and the one after
             * it in the file, and after a reopen its rows would be another series'. Only a reopen, which reads the
             * file, puts the two in step again, so this is a failed append like a torn one. */
            d->broken = true;
            err = rc == TSDB_ERR_NOMEM ? ENOMEM : EIO;
            rc = TSDB_ERR_IO;
        } else {
            pre_copy = NULL;
            pre_tab = NULL;
            pre_table = NULL;
        }
    } else {
        /* Set before append_lock is released: the caller fails the engine only after it, and another creator
         * waiting for append_lock must not append behind the torn record meanwhile. */
        d->broken = true;
    }
    pthread_mutex_unlock(&d->lock);
    if (old_table)
        tsdb_free_big(old_table, old_tcap * sizeof *old_table);
    free(pre_copy);
    free(pre_tab);
    if (pre_table)
        tsdb_free_big(pre_table, pre_tcap * sizeof *pre_table);
    pthread_mutex_unlock(&d->append_lock);
    if (rc != 0)
        dict_hook(d, DICT_AT_FAILED, name);
    if (rc == TSDB_ERR_IO)
        errno = err;
    return rc;
}

int series_get(series_dict *d, const char *name, bool create, int type, uint32_t *id)
{
    size_t len = strlen(name);
    if (len == 0 || len > MAX_NAME || (create && !type_valid(type)))
        return TSDB_ERR_INVALID;
    uint64_t hash = name_hash(d, name, len);
    pthread_mutex_lock(&d->lock);
    size_t slot;
    int64_t found = table_find(d, name, len, hash, &slot);
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
        rc = dict_append(d, name, len, hash, type, id);
    return rc;
}

const char *series_name(series_dict *d, uint32_t id)
{
    uint32_t count = atomic_load_explicit(&d->count, memory_order_acquire);
    if (id >= count)
        return NULL;
    series_tab *t = atomic_load_explicit(&d->tab, memory_order_acquire);
    return t && id < t->cap ? t->names[id] : NULL;
}

int series_type(series_dict *d, uint32_t id)
{
    uint32_t count = atomic_load_explicit(&d->count, memory_order_acquire);
    if (id >= count)
        return -1;
    series_tab *t = atomic_load_explicit(&d->tab, memory_order_acquire);
    return t && id < t->cap ? t->types[id] : -1;
}

/* Whether every series of a batch exists and has `type` (series NULL: cs alone). */
bool series_check(series_dict *d, const uint32_t *series, uint32_t cs, size_t n, int type, struct cls_table *ct,
                  int *ucls)
{
#define CLS_OF(id) (ct && (id) < ct->cap ? atomic_load_explicit(&ct->b[id], memory_order_relaxed) : 0)
    uint32_t count = atomic_load_explicit(&d->count, memory_order_acquire);
    series_tab *t = count ? atomic_load_explicit(&d->tab, memory_order_acquire) : NULL;
    const uint8_t *types = t ? t->types : NULL;
    if (t && count > t->cap)
        count = t->cap;
    else if (!t)
        count = 0;
    bool ok = true, same = true;
    uint8_t b0 = 0;
    if (!series) {
        ok = cs < count && types[cs] == type;
        b0 = CLS_OF(cs);
    }
    /* rows come grouped by series: checking where the series changes is enough */
    for (size_t i = 0; series && ok && i < n; i++)
        if (i == 0 || series[i] != series[i - 1]) {
            ok = series[i] < count && types[series[i]] == type;
            uint8_t b = CLS_OF(series[i]);
            if (i == 0)
                b0 = b;
            if (!(b & CLS_DECIDED))
                b0 = 0; /* fresh: reported below whatever follows */
            same &= b == b0;
        }
#undef CLS_OF
    if (ucls)
        *ucls = !(b0 & CLS_DECIDED) ? -2 : same && !(b0 & CLS_HIST) ? (int)(b0 & CLS_MASK) : -1;
    return ok;
}
