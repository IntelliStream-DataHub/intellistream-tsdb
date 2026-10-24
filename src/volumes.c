/*
 * The public tsdb: one root directory plus any number of volumes, each a
 * storage directory of its own (its parts, WAL, MANIFEST and LOCK), for
 * example one per NFS mount. A series lives in one volume, chosen when it is
 * created (tsdb_series_at); everything about it is routed there. Writes to
 * different volumes share nothing but this routing table, so they scale with
 * the number of mounts.
 *
 * The root directory is volume 0, so a database without extra volumes is
 * exactly what a single engine stores. The root also holds:
 *
 *   VOLUMES   one line per extra volume (volume i is line i): "<check> <path>", check
 *             the low 32 bits of xxh64(path, seed i) in hex
 *   CATALOG   one 16-byte record per series, in global id order: u32 volume,
 *             u32 local id, u64 xxh64 of {global id, volume, local id}
 *   ID        a random identifier; every volume holds a copy in ROOT_ID, so a
 *             volume cannot be attached to another database by mistake
 *
 * A series is created in its volume first and then appended to CATALOG; a
 * crash in between leaves a series the catalog does not know, which the next
 * open appends.
 *
 * Both files only grow, by appends. At open the writer cuts a torn tail (a
 * partial or unchecked last record, with no valid one after it), so the next
 * append starts on a record boundary. A record that fails its check with a
 * valid one after it, or one naming a volume or series that does not exist,
 * is damage to durable data: the open fails with TSDB_ERR_CORRUPT rather than
 * renumber the series that follow. Followers stop at the first record they
 * cannot use (the writer may still be appending it) and read again later.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

#define VOLUMES_FILE "VOLUMES"
#define CATALOG_FILE "CATALOG"
#define ID_FILE "ID"
#define ROOT_ID_FILE "ROOT_ID"

struct tsdb {
    tsdb_options opt;
    char *dir;
    int dirfd;
    uint64_t id;
    pthread_rwlock_t lock; /* the arrays below; written when series or volumes are added */
    eng **vols;
    char **paths; /* canonical, [0] is the root */
    size_t nvols, vols_cap;
    uint32_t *cat_vol, *cat_local; /* by global series id */
    size_t ncat, cat_cap;
    uint32_t *by_name; /* open addressing over global ids + 1 */
    size_t name_cap;
    pthread_mutex_t create_lock; /* one series creation at a time */
};

/* ---------------------------------------------------------------- helpers */

static uint64_t name_hash(const char *s) { return xxh64(s, strlen(s), 0x76a1); }

static const char *gid_name(tsdb *t, uint32_t g) { return eng_series_name(t->vols[t->cat_vol[g]], t->cat_local[g]); }

static int names_grow(tsdb *t)
{
    size_t cap = t->name_cap ? t->name_cap * 2 : 1024;
    uint32_t *m = calloc(cap, sizeof *m);
    if (!m)
        return TSDB_ERR_NOMEM;
    for (size_t g = 0; g < t->ncat; g++) {
        size_t h = name_hash(gid_name(t, (uint32_t)g)) & (cap - 1);
        while (m[h])
            h = (h + 1) & (cap - 1);
        m[h] = (uint32_t)g + 1;
    }
    free(t->by_name);
    t->by_name = m;
    t->name_cap = cap;
    return 0;
}

static int64_t names_find(tsdb *t, const char *name)
{
    if (!t->name_cap)
        return -1;
    size_t h = name_hash(name) & (t->name_cap - 1);
    while (t->by_name[h]) {
        uint32_t g = t->by_name[h] - 1;
        if (strcmp(gid_name(t, g), name) == 0)
            return g;
        h = (h + 1) & (t->name_cap - 1);
    }
    return -1;
}

/* Adds a catalog entry in memory; caller holds the write lock. */
static int cat_add(tsdb *t, uint32_t vol, uint32_t local)
{
    if (t->ncat == t->cat_cap) {
        size_t cap = t->cat_cap ? t->cat_cap * 2 : 256;
        uint32_t *v = realloc(t->cat_vol, cap * sizeof *v);
        if (!v)
            return TSDB_ERR_NOMEM;
        t->cat_vol = v;
        uint32_t *l = realloc(t->cat_local, cap * sizeof *l);
        if (!l)
            return TSDB_ERR_NOMEM;
        t->cat_local = l;
        t->cat_cap = cap;
    }
    t->cat_vol[t->ncat] = vol;
    t->cat_local[t->ncat] = local;
    t->ncat++;
    if ((t->ncat + 1) * 2 > t->name_cap)
        return names_grow(t);
    size_t h = name_hash(gid_name(t, (uint32_t)(t->ncat - 1))) & (t->name_cap - 1);
    while (t->by_name[h])
        h = (h + 1) & (t->name_cap - 1);
    t->by_name[h] = (uint32_t)t->ncat;
    return 0;
}

/* errno tells why on failure */
static int append_file(int dirfd, const char *name, const void *data, size_t len, bool sync)
{
    int fd = openat(dirfd, name, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;
    ssize_t wr = write(fd, data, len);
    bool ok = wr == (ssize_t)len && (!sync || fdatasync(fd) == 0);
    int err = ok ? 0 : wr >= 0 && wr != (ssize_t)len ? ENOSPC : errno;
    close(fd);
    errno = err;
    return ok ? 0 : TSDB_ERR_IO;
}

/* A failed append to CATALOG or VOLUMES may leave a torn record, after which
 * the next open reads nothing: no record may follow it. The root volume's
 * engine holds the failure (eng_fail), so tsdb_failed reports it and writes
 * to the root volume stop too. */
static int append_root(tsdb *t, const char *name, const void *data, size_t len)
{
    int rc = eng_check(t->vols[0]);
    if (rc == 0 && (rc = append_file(t->dirfd, name, data, len, t->opt.fsync)) != 0) {
        char what[64];
        snprintf(what, sizeof what, "append to %s", name);
        eng_fail(t->vols[0], what, errno);
    }
    return rc;
}

#define CAT_REC 16

static uint64_t cat_check(uint64_t gid, uint32_t vol, uint32_t local)
{
    uint8_t b[16];
    memcpy(b, &gid, 8);
    memcpy(b + 8, &vol, 4);
    memcpy(b + 12, &local, 4);
    return xxh64(b, sizeof b, 0x434154414c4f47ull);
}

static void cat_record(uint8_t rec[CAT_REC], uint64_t gid, uint32_t vol, uint32_t local)
{
    uint64_t check = cat_check(gid, vol, local);
    memcpy(rec, &vol, 4);
    memcpy(rec + 4, &local, 4);
    memcpy(rec + 8, &check, 8);
}

static bool cat_valid(const char *rec, uint64_t gid)
{
    uint64_t check;
    memcpy(&check, rec + 8, 8);
    return check == cat_check(gid, load32(rec), load32(rec + 4));
}

/* The check of VOLUMES line i (counting from 1) for this path. */
static uint32_t vol_check(const char *path, size_t len, size_t i)
{
    return (uint32_t)xxh64(path, len, i);
}

/* Writer at open: cuts a torn tail off name at len. */
static int cut_tail(tsdb *t, const char *name, size_t len, size_t size)
{
    fprintf(stderr, "tsdb: %s: cutting a torn tail of %zu bytes\n", name, size - len);
    int fd = openat(t->dirfd, name, O_WRONLY | O_CLOEXEC);
    int rc = fd < 0 || ftruncate(fd, (off_t)len) != 0 || (t->opt.fsync && fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0)
        close(fd);
    return rc;
}

static int read_file_all(int dirfd, const char *name, char **out, size_t *len)
{
    *out = NULL;
    *len = 0;
    int fd = openat(dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    *out = malloc((size_t)st.st_size + 1);
    ssize_t got = *out ? pread(fd, *out, (size_t)st.st_size, 0) : -1;
    close(fd);
    if (got != st.st_size) { /* a short read is an error too, never the end of the file */
        free(*out);
        *out = NULL;
        return TSDB_ERR_IO;
    }
    (*out)[got] = '\0';
    *len = (size_t)got;
    return 0;
}

/* --------------------------------------------------------------- volumes */

static int check_or_write_root_id(tsdb *t, const char *path, bool create)
{
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    char *txt;
    size_t len;
    int rc = read_file_all(fd, ROOT_ID_FILE, &txt, &len);
    if (rc == TSDB_ERR_NOTFOUND && create) {
        char buf[32];
        int n = snprintf(buf, sizeof buf, "%016" PRIx64 "\n", t->id);
        rc = append_file(fd, ROOT_ID_FILE, buf, (size_t)n, t->opt.fsync);
    } else if (rc == 0) {
        uint64_t id = strtoull(txt, NULL, 16);
        rc = id == t->id ? 0 : TSDB_ERR_INVALID; /* a volume of another database */
        free(txt);
    }
    close(fd);
    return rc;
}

/* Opens volume `path` (canonical). Caller holds the write lock. */
static int volume_open(tsdb *t, const char *path, bool create, uint32_t *index)
{
    if (t->nvols == t->vols_cap) {
        size_t cap = t->vols_cap ? t->vols_cap * 2 : 8;
        eng **v = realloc(t->vols, cap * sizeof *v);
        if (!v)
            return TSDB_ERR_NOMEM;
        t->vols = v;
        char **p = realloc(t->paths, cap * sizeof *p);
        if (!p)
            return TSDB_ERR_NOMEM;
        t->paths = p;
        t->vols_cap = cap;
    }
    if (create)
        mkdir(path, 0755);
    int rc = check_or_write_root_id(t, path, create && !t->opt.read_only);
    if (rc)
        return rc;
    eng *e;
    if ((rc = eng_open(path, &t->opt, &e)) != 0)
        return rc;
    t->vols[t->nvols] = e;
    t->paths[t->nvols] = strdup(path);
    *index = (uint32_t)t->nvols++;
    return 0;
}

/* Reads VOLUMES and CATALOG entries beyond what we have (followers call this
 * again to catch up with the writer). Caller holds the write lock. */
static int load_layout(tsdb *t)
{
    bool writer = !t->opt.read_only;
    char *txt;
    size_t len;
    int rc = read_file_all(t->dirfd, VOLUMES_FILE, &txt, &len);
    if (rc == TSDB_ERR_NOTFOUND)
        rc = 0; /* no extra volumes */
    else if (rc)
        return rc;
    size_t line_no = 0, good_end = 0;
    bool bad = false;
    for (char *line = txt, *next; rc == 0 && line && *line; line = next) {
        next = strchr(line, '\n');
        if (!next)
            break; /* still being written, or torn */
        *next++ = '\0';
        unsigned check;
        int pos = 0;
        size_t plen = strlen(line);
        if (plen < 10 || sscanf(line, "%8x %n", &check, &pos) != 1 || pos != 9 ||
            check != vol_check(line + 9, plen - 9, line_no + 1)) {
            next[-1] = '\n'; /* the scan below looks for the line after this one */
            bad = true;
            break;
        }
        good_end = (size_t)(next - txt);
        if (++line_no < t->nvols)
            continue; /* already open */
        uint32_t idx;
        rc = volume_open(t, line + 9, false, &idx);
    }
    size_t vols_cut = SIZE_MAX, vols_len = len; /* cut after CATALOG checks out: it may need those lines */
    if (rc == 0 && writer && good_end < len) {
        /* damage when the line after the bad one passes its check (as the next volume's) */
        bool later = false;
        char *l = bad ? memchr(txt + good_end, '\n', len - good_end) : NULL;
        char *e = l ? memchr(l + 1, '\n', len - (size_t)(l + 1 - txt)) : NULL;
        if (e && e - l - 1 >= 10) {
            unsigned check;
            int pos = 0;
            *e = '\0';
            later = sscanf(l + 1, "%8x %n", &check, &pos) == 1 && pos == 9 &&
                    check == vol_check(l + 10, (size_t)(e - l - 10), line_no + 2);
        }
        if (later) {
            fprintf(stderr, "tsdb: %s: line %zu is damaged and later lines are not: the open fails\n", VOLUMES_FILE,
                    line_no + 1);
            rc = TSDB_ERR_CORRUPT;
        } else {
            vols_cut = good_end;
        }
    }
    free(txt);
    if (rc)
        return rc;
    rc = read_file_all(t->dirfd, CATALOG_FILE, &txt, &len);
    if (rc == TSDB_ERR_NOTFOUND)
        return vols_cut == SIZE_MAX ? 0 : cut_tail(t, VOLUMES_FILE, vols_cut, vols_len);
    if (rc)
        return rc;
    size_t off = t->ncat * CAT_REC;
    for (; rc == 0 && off + CAT_REC <= len; off += CAT_REC) {
        uint64_t gid = off / CAT_REC;
        if (!cat_valid(txt + off, gid))
            break;
        uint32_t vol = load32(txt + off), local = load32(txt + off + 4);
        if (vol >= t->nvols || local >= eng_series_count(t->vols[vol])) {
            if (!writer && vol < t->nvols)
                eng_refresh(t->vols[vol]); /* the writer created it a moment ago */
            if (!writer && (vol >= t->nvols || local >= eng_series_count(t->vols[vol])))
                break; /* not visible to us yet: next time */
            if (writer) {
                fprintf(stderr, "tsdb: %s: series %" PRIu64 " names volume %u series %u, which does not exist: the open "
                        "fails\n", CATALOG_FILE, gid, vol, local);
                rc = TSDB_ERR_CORRUPT;
                break;
            }
        }
        rc = cat_add(t, vol, local);
    }
    if (rc == 0 && writer && off < len) {
        /* the record at off is partial or fails its check: a torn tail unless a valid record follows */
        bool later = false;
        for (size_t o = off + CAT_REC, g = off / CAT_REC + 1; !later && o + CAT_REC <= len; o += CAT_REC, g++)
            later = cat_valid(txt + o, g);
        if (later) {
            fprintf(stderr, "tsdb: %s: record %zu is damaged and later records are not: the open fails\n",
                    CATALOG_FILE, off / CAT_REC);
            rc = TSDB_ERR_CORRUPT;
        } else {
            rc = cut_tail(t, CATALOG_FILE, off, len);
        }
    }
    free(txt);
    if (rc == 0 && vols_cut != SIZE_MAX)
        rc = cut_tail(t, VOLUMES_FILE, vols_cut, vols_len);
    return rc;
}

/* Writer: series that exist in a volume but not in CATALOG (a crash between
 * creating one and recording it, or a database from before volumes). */
static int adopt_orphans(tsdb *t)
{
    for (uint32_t v = 0; v < t->nvols; v++) {
        uint32_t n = eng_series_count(t->vols[v]);
        uint8_t *known = calloc(n ? n : 1, 1);
        if (!known)
            return TSDB_ERR_NOMEM;
        for (size_t g = 0; g < t->ncat; g++)
            if (t->cat_vol[g] == v && t->cat_local[g] < n)
                known[t->cat_local[g]] = 1;
        for (uint32_t l = 0; l < n; l++) {
            if (known[l])
                continue;
            uint8_t rec[CAT_REC];
            cat_record(rec, t->ncat, v, l);
            int rc = append_file(t->dirfd, CATALOG_FILE, rec, CAT_REC, t->opt.fsync);
            if (rc == 0)
                rc = cat_add(t, v, l);
            if (rc) {
                free(known);
                return rc;
            }
        }
        free(known);
    }
    return 0;
}

/* ------------------------------------------------------------- open/close */

int tsdb_open(const char *dir, const tsdb_options *opts, tsdb **out)
{
    *out = NULL;
    tsdb *t = calloc(1, sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    if (opts)
        t->opt = *opts;
    else
        tsdb_options_init(&t->opt);
    if (t->opt.follow)
        t->opt.read_only = 1;
    pthread_rwlock_init(&t->lock, NULL);
    pthread_mutex_init(&t->create_lock, NULL);
    t->dirfd = -1;
    eng *root;
    int rc = eng_open(dir, &t->opt, &root); /* creates the directory for a writer */
    if (rc) {
        tsdb_close(t);
        return rc;
    }
    char canon[PATH_MAX];
    t->dir = strdup(realpath(dir, canon) ? canon : dir);
    t->dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    t->vols = calloc(8, sizeof *t->vols);
    t->paths = calloc(8, sizeof *t->paths);
    t->vols_cap = 8;
    if (!t->dir || t->dirfd < 0 || !t->vols || !t->paths) {
        eng_close(root);
        tsdb_close(t);
        return TSDB_ERR_IO;
    }
    t->vols[0] = root;
    t->paths[0] = strdup(t->dir);
    t->nvols = 1;

    char *txt;
    size_t len;
    rc = read_file_all(t->dirfd, ID_FILE, &txt, &len);
    if (rc == 0) {
        t->id = strtoull(txt, NULL, 16);
        free(txt);
    } else if (rc == TSDB_ERR_NOTFOUND && !t->opt.read_only) {
        if (getrandom(&t->id, sizeof t->id, 0) != sizeof t->id)
            t->id = (uint64_t)time(NULL) ^ ((uint64_t)getpid() << 32);
        char buf[32];
        int n = snprintf(buf, sizeof buf, "%016" PRIx64 "\n", t->id);
        rc = append_file(t->dirfd, ID_FILE, buf, (size_t)n, t->opt.fsync);
    } else if (rc == TSDB_ERR_NOTFOUND) {
        rc = 0; /* read only, written by an older version: no volumes */
    }
    pthread_rwlock_wrlock(&t->lock);
    if (rc == 0)
        rc = names_grow(t);
    if (rc == 0)
        rc = load_layout(t);
    if (rc == 0 && !t->opt.read_only)
        rc = adopt_orphans(t);
    if (rc == 0 && t->opt.read_only && t->ncat == 0) {
        /* no CATALOG (an older database): the root's series are the global ones */
        for (uint32_t l = 0; rc == 0 && l < eng_series_count(root); l++)
            rc = cat_add(t, 0, l);
    }
    pthread_rwlock_unlock(&t->lock);
    if (rc) {
        tsdb_close(t);
        return rc;
    }
    *out = t;
    return 0;
}

int tsdb_close(tsdb *t)
{
    if (!t)
        return 0;
    int rc = 0;
    for (size_t i = 0; i < t->nvols; i++) {
        int r = eng_close(t->vols[i]);
        rc = rc ? rc : r;
        free(t->paths[i]);
    }
    free(t->vols);
    free(t->paths);
    free(t->cat_vol);
    free(t->cat_local);
    free(t->by_name);
    free(t->dir);
    if (t->dirfd >= 0)
        close(t->dirfd);
    pthread_rwlock_destroy(&t->lock);
    pthread_mutex_destroy(&t->create_lock);
    free(t);
    return rc;
}

/* ---------------------------------------------------------------- series */

/* A follower may be asked about series its writer created a moment ago. */
static void catch_up(tsdb *t)
{
    if (!t->opt.read_only)
        return;
    pthread_rwlock_wrlock(&t->lock);
    eng_refresh(t->vols[0]);
    load_layout(t);
    pthread_rwlock_unlock(&t->lock);
}

/* Routes a global id: the volume and its local id. */
static bool route(tsdb *t, uint32_t g, eng **e, uint32_t *local)
{
    pthread_rwlock_rdlock(&t->lock);
    bool ok = g < t->ncat;
    if (ok) {
        *e = t->vols[t->cat_vol[g]];
        *local = t->cat_local[g];
    }
    pthread_rwlock_unlock(&t->lock);
    if (!ok && t->opt.read_only) {
        catch_up(t);
        pthread_rwlock_rdlock(&t->lock);
        ok = g < t->ncat;
        if (ok) {
            *e = t->vols[t->cat_vol[g]];
            *local = t->cat_local[g];
        }
        pthread_rwlock_unlock(&t->lock);
    }
    return ok;
}

int tsdb_series_lookup(tsdb *t, const char *name, uint32_t *id)
{
    pthread_rwlock_rdlock(&t->lock);
    int64_t g = names_find(t, name);
    pthread_rwlock_unlock(&t->lock);
    if (g < 0 && t->opt.read_only) {
        catch_up(t);
        pthread_rwlock_rdlock(&t->lock);
        g = names_find(t, name);
        pthread_rwlock_unlock(&t->lock);
    }
    if (g < 0)
        return TSDB_ERR_NOTFOUND;
    *id = (uint32_t)g;
    return 0;
}

/* Gets or creates a series. type < 0: any existing type, else created as
 * float32; otherwise an existing series must have this type. */
static int series_make(tsdb *t, const char *name, int type, const char *path, uint32_t *id)
{
    if (!name || !*name || (type >= 0 && !type_valid(type)))
        return TSDB_ERR_INVALID;
    tsdb_type have;
    if (tsdb_series_lookup(t, name, id) == 0) /* exists: it stays in the volume it was created in */
        return type < 0 || (tsdb_series_type(t, *id, &have) == 0 && (int)have == type) ? 0 : TSDB_ERR_INVALID;
    if (t->opt.read_only)
        return TSDB_ERR_READONLY;
    if (eng_check(t->vols[0]))
        return TSDB_ERR_IO; /* CATALOG takes no more records */
    char canon[PATH_MAX];
    if (path) {
        mkdir(path, 0755);
        if (!realpath(path, canon))
            return TSDB_ERR_IO;
    }
    pthread_mutex_lock(&t->create_lock);
    int rc = 0;
    if (tsdb_series_lookup(t, name, id) == 0) {
        pthread_mutex_unlock(&t->create_lock);
        return type < 0 || (tsdb_series_type(t, *id, &have) == 0 && (int)have == type) ? 0 : TSDB_ERR_INVALID;
    }
    /* the volume: an existing one, or a new one */
    uint32_t vol = 0;
    pthread_rwlock_rdlock(&t->lock);
    bool found = !path;
    for (size_t v = 0; path && v < t->nvols && !found; v++)
        if (strcmp(t->paths[v], canon) == 0) {
            vol = (uint32_t)v;
            found = true;
        }
    pthread_rwlock_unlock(&t->lock);
    if (!found) {
        pthread_rwlock_wrlock(&t->lock);
        rc = volume_open(t, canon, true, &vol);
        pthread_rwlock_unlock(&t->lock);
        if (rc == 0) {
            size_t n = strlen(canon);
            char *line = malloc(n + 12);
            if (!line) {
                rc = TSDB_ERR_NOMEM;
            } else {
                snprintf(line, n + 12, "%08x %s\n", vol_check(canon, n, vol), canon);
                rc = append_root(t, VOLUMES_FILE, line, n + 10);
                free(line);
            }
        }
    }
    uint32_t local = 0;
    if (rc == 0)
        rc = eng_series(t->vols[vol], name, type < 0 ? TSDB_FLOAT32 : type, &local); /* the volume, then the catalog */
    if (rc == 0) {
        uint8_t rec[CAT_REC];
        cat_record(rec, t->ncat, vol, local);
        rc = append_root(t, CATALOG_FILE, rec, CAT_REC);
    }
    if (rc == 0) {
        pthread_rwlock_wrlock(&t->lock);
        *id = (uint32_t)t->ncat;
        rc = cat_add(t, vol, local);
        pthread_rwlock_unlock(&t->lock);
    }
    pthread_mutex_unlock(&t->create_lock);
    return rc;
}

int tsdb_series_at(tsdb *t, const char *name, const char *path, uint32_t *id)
{
    return series_make(t, name, -1, path, id);
}

int tsdb_series(tsdb *t, const char *name, uint32_t *id) { return series_make(t, name, -1, NULL, id); }

int tsdb_series_create(tsdb *t, const char *name, tsdb_type type, const char *path, uint32_t *id)
{
    return series_make(t, name, (int)type, path, id);
}

uint32_t tsdb_series_count(tsdb *t)
{
    pthread_rwlock_rdlock(&t->lock);
    uint32_t n = (uint32_t)t->ncat;
    pthread_rwlock_unlock(&t->lock);
    return n;
}

const char *tsdb_series_name(tsdb *t, uint32_t id)
{
    eng *e;
    uint32_t local;
    return route(t, id, &e, &local) ? eng_series_name(e, local) : NULL;
}

int tsdb_series_type(tsdb *t, uint32_t id, tsdb_type *type)
{
    eng *e;
    uint32_t local;
    int ty = route(t, id, &e, &local) ? eng_series_type(e, local) : -1;
    if (ty < 0)
        return TSDB_ERR_NOTFOUND;
    *type = (tsdb_type)ty;
    return 0;
}

/* ---------------------------------------------------------------- states */

static int state_get(tsdb *t, uint32_t series, const char *name, bool create, uint32_t *code)
{
    eng *e;
    uint32_t local;
    if (!name || !*name)
        return TSDB_ERR_INVALID;
    if (!route(t, series, &e, &local))
        return TSDB_ERR_NOTFOUND;
    if (create && t->opt.read_only)
        create = false;
    return eng_state(e, name, create, code);
}

int tsdb_state(tsdb *t, uint32_t series, const char *name, uint32_t *code)
{
    return state_get(t, series, name, true, code);
}

int tsdb_state_lookup(tsdb *t, uint32_t series, const char *name, uint32_t *code)
{
    return state_get(t, series, name, false, code);
}

const char *tsdb_state_name(tsdb *t, uint32_t series, uint32_t code)
{
    eng *e;
    uint32_t local;
    return route(t, series, &e, &local) ? eng_state_name(e, code) : NULL;
}

const char *tsdb_series_path(tsdb *t, uint32_t id)
{
    pthread_rwlock_rdlock(&t->lock);
    const char *p = id < t->ncat ? t->paths[t->cat_vol[id]] : NULL;
    pthread_rwlock_unlock(&t->lock);
    return p;
}

/* ---------------------------------------------------------------- writes */

/* A column as slots of its type: the caller's array when it already is one
 * (no nulls, not bool), else a converted copy in *owned. */
static int vals_import(const tsdb_values *in, size_t n, const void **slots, void **owned)
{
    *owned = NULL;
    if (!in || !type_valid(in->type) || (n && !in->values))
        return TSDB_ERR_INVALID;
    if (!in->valid && in->type != TSDB_BOOL) {
        *slots = in->values;
        return 0;
    }
    size_t w = type_width(in->type);
    uint8_t *o = malloc(n ? n * w : 1);
    if (!o)
        return TSDB_ERR_NOMEM;
    uint64_t null = type_null(in->type);
    for (size_t i = 0; i < n; i++) {
        uint64_t x = in->type == TSDB_BOOL ? ((const uint8_t *)in->values)[i] != 0 : slot_get(in->values, w, i);
        if (in->valid && !((in->valid[i >> 3] >> (i & 7)) & 1))
            x = null;
        slot_put(o, w, i, x);
    }
    *slots = o;
    *owned = o;
    return 0;
}

static int insert_series_slots(tsdb *t, int type, uint32_t series, const int64_t *ts, const void *v, size_t n)
{
    eng *e;
    uint32_t local;
    if (t->opt.read_only)
        return TSDB_ERR_READONLY;
    return route(t, series, &e, &local) ? eng_insert_series(e, type, local, ts, v, n) : TSDB_ERR_INVALID;
}

/* Rows of several series: split by volume, local ids, each volume written on
 * its own (a batch that spans volumes is durable per volume). */
static int insert_slots(tsdb *t, int type, const uint32_t *series, const int64_t *ts, const void *values, size_t n)
{
    if (t->opt.read_only)
        return TSDB_ERR_READONLY;
    if (n && (!series || !ts || !values))
        return TSDB_ERR_INVALID;
    size_t w = type_width(type);
    pthread_rwlock_rdlock(&t->lock);
    size_t nv = t->nvols, ncat = t->ncat;
    bool identity = nv == 1; /* the root alone: global ids are its local ids */
    for (size_t i = 0; i < n; i++)
        if (series[i] >= ncat) {
            pthread_rwlock_unlock(&t->lock);
            return TSDB_ERR_INVALID;
        }
    if (identity) {
        eng *root = t->vols[0];
        pthread_rwlock_unlock(&t->lock);
        return eng_insert(root, type, series, ts, values, n);
    }
    size_t *cnt = calloc(nv + 1, sizeof *cnt);
    uint32_t *ls = malloc(n * sizeof *ls);
    int64_t *lt = malloc(n * sizeof *lt);
    uint8_t *lv = malloc(n * w);
    int rc = (!cnt || !ls || !lt || !lv) ? TSDB_ERR_NOMEM : 0;
    if (rc == 0) {
        for (size_t i = 0; i < n; i++)
            cnt[t->cat_vol[series[i]] + 1]++;
        for (size_t v = 1; v <= nv; v++)
            cnt[v] += cnt[v - 1];
        size_t *pos = malloc(nv * sizeof *pos);
        if (!pos) {
            rc = TSDB_ERR_NOMEM;
        } else {
            memcpy(pos, cnt, nv * sizeof *pos);
            for (size_t i = 0; i < n; i++) { /* stable: later rows stay later */
                size_t o = pos[t->cat_vol[series[i]]]++;
                ls[o] = t->cat_local[series[i]];
                lt[o] = ts[i];
                memcpy(lv + o * w, (const uint8_t *)values + i * w, w);
            }
            free(pos);
        }
    }
    eng **vols = malloc(nv * sizeof *vols);
    if (vols)
        memcpy(vols, t->vols, nv * sizeof *vols);
    pthread_rwlock_unlock(&t->lock);
    for (size_t v = 0; rc == 0 && vols && v < nv; v++)
        if (cnt[v + 1] > cnt[v])
            rc = eng_insert(vols[v], type, ls + cnt[v], lt + cnt[v], lv + cnt[v] * w, cnt[v + 1] - cnt[v]);
    if (!vols && rc == 0)
        rc = TSDB_ERR_NOMEM;
    free(vols);
    free(cnt);
    free(ls);
    free(lt);
    free(lv);
    return rc;
}

int tsdb_insert_series(tsdb *t, uint32_t series, const int64_t *ts, const float *values, size_t n)
{
    return insert_series_slots(t, TSDB_FLOAT32, series, ts, values, n);
}

int tsdb_append(tsdb *t, uint32_t series, int64_t ts, float value)
{
    return insert_series_slots(t, TSDB_FLOAT32, series, &ts, &value, 1);
}

int tsdb_insert(tsdb *t, const uint32_t *series, const int64_t *ts, const float *values, size_t n)
{
    return insert_slots(t, TSDB_FLOAT32, series, ts, values, n);
}

int tsdb_insert_values(tsdb *t, const uint32_t *series, const int64_t *ts, const tsdb_values *values, size_t n)
{
    const void *slots;
    void *owned;
    int rc = vals_import(values, n, &slots, &owned);
    if (rc == 0)
        rc = insert_slots(t, values->type, series, ts, slots, n);
    free(owned);
    return rc;
}

int tsdb_insert_series_values(tsdb *t, uint32_t series, const int64_t *ts, const tsdb_values *values, size_t n)
{
    const void *slots;
    void *owned;
    int rc = vals_import(values, n, &slots, &owned);
    if (rc == 0)
        rc = insert_series_slots(t, values->type, series, ts, slots, n);
    free(owned);
    return rc;
}

/* ---------------------------------------------------- deletes, retention */

int tsdb_delete_range(tsdb *t, uint32_t series, int64_t start, int64_t end)
{
    eng *e;
    uint32_t local;
    if (t->opt.read_only)
        return TSDB_ERR_READONLY;
    return route(t, series, &e, &local) ? eng_delete_range(e, local, start, end) : TSDB_ERR_INVALID;
}

int tsdb_replace_range(tsdb *t, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                       const tsdb_values *values, size_t n)
{
    eng *e;
    uint32_t local;
    if (t->opt.read_only)
        return TSDB_ERR_READONLY;
    if (!route(t, series, &e, &local))
        return TSDB_ERR_INVALID;
    if (n == 0)
        return eng_delete_range(e, local, start, end);
    const void *slots;
    void *owned;
    int rc = vals_import(values, n, &slots, &owned);
    if (rc == 0)
        rc = eng_replace_range(e, values->type, local, start, end, ts, slots, n);
    free(owned);
    return rc;
}

int tsdb_set_retention(tsdb *t, uint32_t series, int64_t limit_ms)
{
    eng *e;
    uint32_t local;
    if (t->opt.read_only)
        return TSDB_ERR_READONLY;
    return route(t, series, &e, &local) ? eng_set_retention(e, local, limit_ms) : TSDB_ERR_NOTFOUND;
}

int tsdb_get_retention(tsdb *t, uint32_t series, int64_t *limit_ms)
{
    eng *e;
    uint32_t local;
    if (!route(t, series, &e, &local) || eng_series_type(e, local) < 0)
        return TSDB_ERR_NOTFOUND;
    *limit_ms = eng_retention(e, local);
    return 0;
}

/* Runs fn on every volume; the first error wins. */
static int each(tsdb *t, int (*fn)(eng *))
{
    pthread_rwlock_rdlock(&t->lock);
    size_t n = t->nvols;
    eng **v = malloc(n * sizeof *v);
    if (v)
        memcpy(v, t->vols, n * sizeof *v);
    pthread_rwlock_unlock(&t->lock);
    if (!v)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (size_t i = 0; i < n; i++) {
        int r = fn(v[i]);
        rc = rc ? rc : r;
    }
    free(v);
    return rc;
}

int tsdb_commit(tsdb *t) { return each(t, eng_commit); }

int tsdb_failed(tsdb *t, char *msg, size_t len)
{
    if (msg && len)
        msg[0] = '\0';
    pthread_rwlock_rdlock(&t->lock);
    int rc = 0;
    for (size_t i = 0; i < t->nvols && rc == 0; i++)
        rc = eng_failed_msg(t->vols[i], msg, len);
    pthread_rwlock_unlock(&t->lock);
    return rc;
}
int tsdb_flush(tsdb *t) { return each(t, eng_flush); }
int tsdb_optimize(tsdb *t) { return each(t, eng_optimize); }

int tsdb_refresh(tsdb *t)
{
    int rc = each(t, eng_refresh);
    catch_up(t);
    return rc;
}

static int64_t drop_ts;
static pthread_mutex_t drop_lock = PTHREAD_MUTEX_INITIALIZER;
static int drop_one(eng *e) { return eng_drop_before(e, drop_ts); }

int tsdb_drop_before(tsdb *t, int64_t ts)
{
    pthread_mutex_lock(&drop_lock);
    drop_ts = ts;
    int rc = each(t, drop_one);
    pthread_mutex_unlock(&drop_lock);
    return rc;
}

/* --------------------------------------------------------------- queries */

__thread qstats *cur_qstats;
static __thread tsdb_query_stats last_stats;

/* One query's statistics. Queries built on others (points on raw, _ex on
 * plain) are counted once, by the outermost call. */
typedef struct qrun {
    qstats q;
    bool outer;
    struct timespec t0;
} qrun;

static void qs_begin(qrun *r)
{
    r->outer = cur_qstats == NULL;
    if (!r->outer)
        return;
    memset(&r->q, 0, sizeof r->q);
    cur_qstats = &r->q;
    clock_gettime(CLOCK_MONOTONIC, &r->t0);
}

static void qs_end(qrun *r, int type, uint64_t result_rows)
{
    if (!r->outer)
        return;
    cur_qstats = NULL;
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    tsdb_query_stats *s = &last_stats;
    s->elapsed_s = (double)(t1.tv_sec - r->t0.tv_sec) + (double)(t1.tv_nsec - r->t0.tv_nsec) * 1e-9;
    s->rows_decoded = atomic_load(&r->q.rows_decoded);
    s->rows_from_stats = atomic_load(&r->q.rows_from_stats);
    s->memtable_rows = atomic_load(&r->q.memtable_rows);
    s->rows = s->rows_decoded + s->rows_from_stats + s->memtable_rows;
    s->bytes = s->rows * (8 + (type >= 0 ? type_width(type) : 4));
    s->bytes_read = atomic_load(&r->q.bytes_read);
    s->parts = atomic_load(&r->q.parts);
    s->granules = atomic_load(&r->q.granules);
    s->granules_from_stats = atomic_load(&r->q.granules_from_stats);
    s->result_rows = result_rows;
}

void tsdb_last_query_stats(tsdb_query_stats *out) { *out = last_stats; }

/* float32 rows to a tsdb_raw_fn: the slots are the floats */
typedef struct f32_out {
    tsdb_raw_fn fn;
    void *ctx;
    uint64_t rows;
} f32_out;

static int f32_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    f32_out *o = arg;
    o->rows += n;
    return o->fn(o->ctx, ts, v, n);
}

int tsdb_query_raw_ex(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                      tsdb_raw_fn fn, void *ctx)
{
    eng *e;
    uint32_t local;
    qrun qr;
    qs_begin(&qr);
    f32_out o = {fn, ctx, 0};
    int rc = 0;
    if (route(t, series, &e, &local)) {
        if (eng_series_type(e, local) != TSDB_FLOAT32)
            rc = TSDB_ERR_INVALID; /* tsdb_query_values reads every type */
        else
            rc = eng_query_raw_ex(e, local, start, end, opts && opts->order == TSDB_DESC, opts ? opts->limit : 0,
                                  f32_cb, &o);
    }
    qs_end(&qr, TSDB_FLOAT32, o.rows);
    return rc;
}

int tsdb_query_raw(tsdb *t, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx)
{
    return tsdb_query_raw_ex(t, series, start, end, NULL, fn, ctx);
}

static int collect_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    tsdb_points *p = ctx;
    if (p->n + n > p->cap) {
        size_t cap = MAX(MAX(p->cap * 2, p->n + n), (size_t)1024);
        int64_t *t2 = realloc(p->ts, cap * sizeof *t2);
        if (!t2)
            return 1;
        p->ts = t2;
        float *v2 = realloc(p->values, cap * sizeof *v2);
        if (!v2)
            return 1;
        p->values = v2;
        p->cap = cap;
    }
    memcpy(p->ts + p->n, ts, n * sizeof *ts);
    memcpy(p->values + p->n, v, n * sizeof *v);
    p->n += n;
    return 0;
}

int tsdb_query_points_ex(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                         tsdb_points *out)
{
    memset(out, 0, sizeof *out);
    int rc = tsdb_query_raw_ex(t, series, start, end, opts, collect_cb, out);
    if (rc == TSDB_ERR_STOPPED)
        rc = TSDB_ERR_NOMEM; /* collect_cb only stops when it cannot allocate */
    if (rc)
        tsdb_points_free(out);
    return rc;
}

int tsdb_query_points(tsdb *t, uint32_t series, int64_t start, int64_t end, tsdb_points *out)
{
    return tsdb_query_points_ex(t, series, start, end, NULL, out);
}

/* Slots to the caller's columns: a validity bitmap when a chunk holds nulls,
 * bools as bytes. */
typedef struct vals_out {
    tsdb_values_fn fn;
    void *ctx;
    int type;
    uint8_t *valid, *bytes;
    size_t cap;
    uint64_t rows;
} vals_out;

static int vals_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    vals_out *o = arg;
    o->rows += n;
    if (n > o->cap) {
        size_t cap = MAX(n, o->cap * 2);
        uint8_t *va = realloc(o->valid, (cap + 7) / 8);
        if (!va)
            return 1;
        o->valid = va;
        uint8_t *b = realloc(o->bytes, cap);
        if (!b)
            return 1;
        o->bytes = b;
        o->cap = cap;
    }
    size_t w = type_width(o->type);
    uint64_t null = type_null(o->type);
    bool nulls = false;
    memset(o->valid, 0, (n + 7) / 8);
    for (size_t i = 0; i < n; i++) {
        uint64_t x = slot_get(v, w, i);
        bool known = x != null;
        o->valid[i >> 3] |= (uint8_t)(known << (i & 7));
        nulls |= !known;
        if (o->type == TSDB_BOOL)
            o->bytes[i] = x == 1;
    }
    tsdb_values col = {(tsdb_type)o->type, o->type == TSDB_BOOL ? (const void *)o->bytes : v, nulls ? o->valid : NULL};
    return o->fn(o->ctx, ts, &col, n);
}

int tsdb_query_values(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                      tsdb_values_fn fn, void *ctx)
{
    eng *e;
    uint32_t local;
    qrun qr;
    qs_begin(&qr);
    vals_out o = {fn, ctx, -1, NULL, NULL, 0, 0};
    int rc = 0;
    if (route(t, series, &e, &local) && (o.type = eng_series_type(e, local)) >= 0)
        rc = eng_query_raw_ex(e, local, start, end, opts && opts->order == TSDB_DESC, opts ? opts->limit : 0, vals_cb,
                              &o);
    qs_end(&qr, o.type, o.rows);
    free(o.valid);
    free(o.bytes);
    return rc;
}

int tsdb_query_lttb(tsdb *t, uint32_t series, int64_t start, int64_t end, size_t threshold, tsdb_values_fn fn,
                    void *ctx)
{
    if (threshold < 3)
        return TSDB_ERR_INVALID;
    eng *e;
    uint32_t local;
    qrun qr;
    qs_begin(&qr);
    vals_out o = {fn, ctx, -1, NULL, NULL, 0, 0};
    int rc = 0;
    if (route(t, series, &e, &local) && (o.type = eng_series_type(e, local)) >= 0)
        rc = eng_query_lttb(e, local, start, end, threshold, vals_cb, &o);
    qs_end(&qr, o.type, o.rows);
    free(o.valid);
    free(o.bytes);
    return rc;
}

int tsdb_query_agg(tsdb *t, uint32_t series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                   size_t *nout)
{
    eng *e;
    uint32_t local;
    *out = NULL;
    *nout = 0;
    if (interval_ms < 0)
        return TSDB_ERR_INVALID;
    qrun qr;
    qs_begin(&qr);
    int rc = 0, type = -1;
    if (route(t, series, &e, &local)) {
        type = eng_series_type(e, local);
        rc = eng_query_agg(e, local, start, end, interval_ms, out, nout);
    }
    qs_end(&qr, type, *nout);
    return rc;
}

int tsdb_query_group_ex(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                        const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    int rc = tsdb_query_group(t, series, start, end, group, out, nout);
    if (opts && opts->limit && *nout > opts->limit)
        last_stats.result_rows = opts->limit;
    if (rc || !opts)
        return rc;
    if (opts->order == TSDB_DESC)
        for (size_t i = 0, j = *nout ? *nout - 1 : 0; i < j; i++, j--) {
            tsdb_bucket x = (*out)[i];
            (*out)[i] = (*out)[j];
            (*out)[j] = x;
        }
    if (opts->limit && *nout > opts->limit)
        *nout = opts->limit;
    return 0;
}

int tsdb_query_group(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                     tsdb_bucket **out, size_t *nout)
{
    eng *e;
    uint32_t local;
    *out = NULL;
    *nout = 0;
    bucketer bk;
    int rc = bucketer_init(&bk, start, group); /* NULL: one bucket */
    if (rc)
        return rc;
    qrun qr;
    qs_begin(&qr);
    int type = -1;
    if (route(t, series, &e, &local)) {
        type = eng_series_type(e, local);
        rc = eng_query_group(e, local, start, end, &bk, out, nout);
    }
    qs_end(&qr, type, *nout);
    return rc;
}

/* Time-weighted averages: one router for the interval, group and bounds forms. */
static int query_twa(tsdb *t, uint32_t series, int64_t start, int64_t end, const bucketer *bk,
                     const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    eng *e;
    uint32_t local;
    tsdb_twa_opts def = {0};
    if (!opts)
        opts = &def;
    qrun qr;
    qs_begin(&qr);
    int rc = 0, type = -1;
    if (route(t, series, &e, &local)) {
        type = eng_series_type(e, local);
        rc = eng_query_twa(e, local, start, end, bk, opts, out, nout);
    }
    if (rc == 0 && opts->order == TSDB_DESC)
        for (size_t i = 0, j = *nout ? *nout - 1 : 0; i < j; i++, j--) {
            tsdb_twa_bucket x = (*out)[i];
            (*out)[i] = (*out)[j];
            (*out)[j] = x;
        }
    if (rc == 0 && opts->limit && *nout > opts->limit)
        *nout = opts->limit;
    qs_end(&qr, type, *nout);
    return rc;
}

int tsdb_query_twa(tsdb *t, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                   const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (interval_ms < 0)
        return TSDB_ERR_INVALID;
    bucketer bk = {.kind = interval_ms ? BK_FIXED : BK_ONE, .start = start, .interval = interval_ms};
    return query_twa(t, series, start, end, &bk, opts, out, nout);
}

int tsdb_query_twa_group(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                         const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    bucketer bk;
    int rc = bucketer_init(&bk, start, group);
    return rc ? rc : query_twa(t, series, start, end, &bk, opts, out, nout);
}

int tsdb_query_twa_bounds(tsdb *t, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                          const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    bucketer bk;
    int rc = bounds ? bucketer_bounds(&bk, bounds, n) : TSDB_ERR_INVALID;
    if (rc)
        return rc;
    if (start < bounds[0]) /* segments before the first bound belong to no bucket */
        start = bounds[0];
    return query_twa(t, series, start, end, &bk, opts, out, nout);
}

int tsdb_query_bounds(tsdb *t, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    eng *e;
    uint32_t local;
    *out = NULL;
    *nout = 0;
    bucketer bk;
    int rc = bounds ? bucketer_bounds(&bk, bounds, n) : TSDB_ERR_INVALID;
    if (rc)
        return rc;
    if (start < bounds[0]) /* rows before the first bound belong to no bucket */
        start = bounds[0];
    qrun qr;
    qs_begin(&qr);
    int type = -1;
    if (route(t, series, &e, &local)) {
        type = eng_series_type(e, local);
        rc = eng_query_group(e, local, start, end, &bk, out, nout);
    }
    if (rc == 0 && opts) {
        if (opts->order == TSDB_DESC)
            for (size_t i = 0, j = *nout ? *nout - 1 : 0; i < j; i++, j--) {
                tsdb_bucket x = (*out)[i];
                (*out)[i] = (*out)[j];
                (*out)[j] = x;
            }
        if (opts->limit && *nout > opts->limit)
            *nout = opts->limit;
    }
    qs_end(&qr, type, *nout);
    return rc;
}

/* The spread: one router for the interval, group and bounds forms, like query_twa. */
static int query_spread(tsdb *t, uint32_t series, int64_t start, int64_t end, const bucketer *bk,
                        const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    eng *e;
    uint32_t local;
    qrun qr;
    qs_begin(&qr);
    int rc = 0, type = -1;
    if (route(t, series, &e, &local)) {
        type = eng_series_type(e, local);
        rc = eng_query_spread(e, local, start, end, bk, out, nout);
    }
    if (rc == 0 && opts && opts->order == TSDB_DESC)
        for (size_t i = 0, j = *nout ? *nout - 1 : 0; i < j; i++, j--) {
            tsdb_spread_bucket x = (*out)[i];
            (*out)[i] = (*out)[j];
            (*out)[j] = x;
        }
    if (rc == 0 && opts && opts->limit && *nout > opts->limit)
        *nout = opts->limit;
    qs_end(&qr, type, *nout);
    return rc;
}

int tsdb_query_spread(tsdb *t, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                      const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (interval_ms < 0)
        return TSDB_ERR_INVALID;
    bucketer bk = {.kind = interval_ms ? BK_FIXED : BK_ONE, .start = start, .interval = interval_ms};
    return query_spread(t, series, start, end, &bk, opts, out, nout);
}

int tsdb_query_spread_group(tsdb *t, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                            const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    bucketer bk;
    int rc = bucketer_init(&bk, start, group);
    return rc ? rc : query_spread(t, series, start, end, &bk, opts, out, nout);
}

int tsdb_query_spread_bounds(tsdb *t, uint32_t series, int64_t start, int64_t end, const int64_t *bounds,
                             size_t n, const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    bucketer bk;
    int rc = bounds ? bucketer_bounds(&bk, bounds, n) : TSDB_ERR_INVALID;
    if (rc)
        return rc;
    if (start < bounds[0]) /* rows before the first bound belong to no bucket */
        start = bounds[0];
    return query_spread(t, series, start, end, &bk, opts, out, nout);
}

/* ----------------------------------------------------------- inspection */

int tsdb_get_stats(tsdb *t, tsdb_stats *st)
{
    memset(st, 0, sizeof *st);
    pthread_rwlock_rdlock(&t->lock);
    for (size_t i = 0; i < t->nvols; i++) {
        tsdb_stats s;
        eng_get_stats(t->vols[i], &s);
        st->parts += s.parts;
        st->partitions += s.partitions;
        st->rows += s.rows;
        st->bytes += s.bytes;
        st->merges += s.merges;
        st->merged_rows += s.merged_rows;
        st->passthrough_granules += s.passthrough_granules;
        st->memtable_rows += s.memtable_rows;
        st->broken_parts += s.broken_parts;
        st->follow_errors += s.follow_errors;
    }
    pthread_rwlock_unlock(&t->lock);
    return 0;
}

int tsdb_get_volume_stats(tsdb *t, uint32_t volume, tsdb_volume_stats *st)
{
    pthread_rwlock_rdlock(&t->lock);
    int rc = volume < t->nvols ? eng_get_volume_stats(t->vols[volume], st) : TSDB_ERR_INVALID;
    pthread_rwlock_unlock(&t->lock);
    return rc;
}

int tsdb_foreach_part(tsdb *t, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx)
{
    pthread_rwlock_rdlock(&t->lock);
    int rc = 0;
    for (size_t i = 0; i < t->nvols && rc == 0; i++)
        rc = eng_foreach_part(t->vols[i], fn, ctx);
    pthread_rwlock_unlock(&t->lock);
    return rc;
}

uint32_t tsdb_volume_count(tsdb *t)
{
    pthread_rwlock_rdlock(&t->lock);
    uint32_t n = (uint32_t)t->nvols;
    pthread_rwlock_unlock(&t->lock);
    return n;
}

const char *tsdb_volume_path(tsdb *t, uint32_t index)
{
    pthread_rwlock_rdlock(&t->lock);
    const char *p = index < t->nvols ? t->paths[index] : NULL;
    pthread_rwlock_unlock(&t->lock);
    return p;
}
