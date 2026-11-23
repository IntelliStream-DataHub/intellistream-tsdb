/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshots of a leader's volume, the make side (docs/design-cluster.md 5.2-5.3, 8.1; R4 D3, D4). A member further
 * behind than the leader's keep caps, a fresh learner on an old volume and a reset member are rebuilt from a snapshot:
 * the leader's crash image at a cut P, its commit point. The replica opens it as a restart would: log start S's first
 * position, log end P, floor P, the epoch history up to P (r4-snapinst installs it; this file only makes it).
 *
 * The cut. Under flush_lock, then publish_lock (and inside them db->lock, buf_lock and lead_mu, each briefly), and with
 * no I/O but these reads of memory: P = the commit point; the names of the listed parts; the memtables' segments and
 * whether they hold committed rows; RSTATE as the leader holds it (leader_rs_image); the files of the COMMIT markers at
 * or below P (db->lf, leader_links_list). Both are then released, before any file is touched. Why that is a cut:
 * - every listed part holds rows at or below P: a leader's flush waits for the commit point before it lists (and holds
 *   flush_lock meanwhile), a large write lists only after its COMMIT marker is committed, a merge lists what was
 *   listed, and the commit point never goes back;
 * - no flush runs (flush_lock), so the memtables are exactly the rows not in parts; a memtable is one segment's (the
 *   WAL rotates with the memtable), so S, the oldest segment a replay needs, is the oldest memtable with rows;
 * - no DROP runs (it holds flush_lock across its marker, flush and retire) and no publish runs (publish_lock), so
 *   RSTATE's `done`, `doing` and drops agree with the list;
 * - a large write holds jr_serial, not flush_lock or publish_lock, between its marker's commit and its parts_commit, so
 *   P can pass a COMMIT whose parts are not listed: such a marker is above `done`, its stage_ links (made before its
 *   marker was appended) go into the snapshot under their stage_ names, and its segment too (S goes down to it). The
 *   files of markers at or below `done` go in as keep_<fno>_<key>.part, which is what a replica keeps them as.
 *
 * The links. A part is hard linked by its listed name; a merge that retired it after the cut renamed it retired_<name>
 * (kept for obsolete_part_lifetime_s), which is linked instead; with neither the make fails with SNAP_RETRY. db->lock is
 * not held across a link. A file linked (a part, a segment, a stage_ or keep_ file) stays alive however the leader
 * removes its own name later.
 *
 * The dictionaries. SERIES, STATES, RETENTION and CLASSES are appended to by writers that hold one lock from their
 * marker's append to their record's write (each dictionary's append_lock, ret_lock, class_lock). After the cut each of
 * these four locks is taken alone (never nested, never under another engine lock, so no lock-order edge is added), the
 * length in memory read, the lock released, and the file copied to that length: a writer whose marker is at or below P
 * appended it before P was read, so it held the lock then and its record is in before the barrier gets the lock.
 * Records past P may be in the copy too; a replay applies them again harmlessly: SERIES and STATES by id (db.c, the
 * entry is there with the same name), RETENTION and CLASSES by the record_off the marker names (a record already at its
 * offset is checked, not appended again). A record whose marker was never committed (a deposed leader's) is cut by the
 * replica's own rules, as after a restart.
 *
 * The log. Every segment from S to the one that holds P is linked, except that one, which is copied up to P's record
 * end (the leader is still writing it). A segment gone meanwhile (a flush and the keep bound) is SNAP_RETRY.
 *
 * Last SNAPMAN (below): every file is fsynced, the directory fsynced, then SNAPMAN written to a temporary, fsynced,
 * renamed and the directory fsynced again. A directory without SNAPMAN is half made; no make survives its engine:
 * snap_sweep removes every snap_* directory at the open. Nothing outside the directory changes.
 *
 * SNAPMAN (little endian): "TSNAPMAN" | u32 version | u32 nhist | u32 nfiles | u32 0 | i64 volume | 16-byte uuid |
 * u64 P | P's epoch | P's segment | S | S's first position | RSTATE's done | nhist x (u64 epoch, u64 first) |
 * nfiles x (u16 name length, name, u8 kind, u64 size, u64 xxh64) | u64 xxh64 of all before.
 *
 * Not built: .ec parts (R8: a volume with one is refused, TSDB_ERR_INVALID), reuse of files the receiver holds (D4, R9).
 */
#include "internal.h"
#include "snapshot.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

void (*snap_test_hook)(struct eng *db, int at, const char *arg);
int snap_mutant;

static void hook(eng *db, int at, const char *arg)
{
    void (*h)(eng *, int, const char *) = __atomic_load_n(&snap_test_hook, __ATOMIC_ACQUIRE);
    if (h)
        h(db, at, arg);
}

/* ---- xxh64 in pieces (the same function as codec.c's xxh64, seed 0) ---- */

#define XP1 11400714785074694791ULL
#define XP2 14029467366897019727ULL
#define XP3 1609587929392839161ULL
#define XP4 9650029242287828579ULL
#define XP5 2870177450012600261ULL

typedef struct {
    uint64_t v[4], total;
    uint8_t buf[32];
    size_t nbuf;
} xstate;

static inline uint64_t xrotl(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static inline uint64_t xround(uint64_t acc, uint64_t in) { return xrotl(acc + in * XP2, 31) * XP1; }
static inline uint64_t xmerge(uint64_t acc, uint64_t v) { return (acc ^ xround(0, v)) * XP1 + XP4; }
static inline uint64_t xrd64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}
static inline uint32_t xrd32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static void x_init(xstate *s)
{
    memset(s, 0, sizeof *s);
    s->v[0] = XP1 + XP2, s->v[1] = XP2, s->v[2] = 0, s->v[3] = (uint64_t)0 - XP1;
}

static void x_stripe(xstate *s, const uint8_t *p)
{
    for (int i = 0; i < 4; i++)
        s->v[i] = xround(s->v[i], xrd64(p + 8 * i));
}

static void x_update(xstate *s, const void *data, size_t len)
{
    const uint8_t *p = data;
    s->total += len;
    if (s->nbuf) {
        size_t k = 32 - s->nbuf < len ? 32 - s->nbuf : len;
        memcpy(s->buf + s->nbuf, p, k);
        s->nbuf += k, p += k, len -= k;
        if (s->nbuf < 32)
            return;
        x_stripe(s, s->buf);
        s->nbuf = 0;
    }
    for (; len >= 32; p += 32, len -= 32)
        x_stripe(s, p);
    memcpy(s->buf, p, len);
    s->nbuf = len;
}

static uint64_t x_final(const xstate *s)
{
    uint64_t h;
    if (s->total >= 32) {
        h = xrotl(s->v[0], 1) + xrotl(s->v[1], 7) + xrotl(s->v[2], 12) + xrotl(s->v[3], 18);
        for (int i = 0; i < 4; i++)
            h = xmerge(h, s->v[i]);
    } else {
        h = XP5;
    }
    h += s->total;
    const uint8_t *p = s->buf, *end = p + s->nbuf;
    for (; p + 8 <= end; p += 8)
        h = xrotl(h ^ xround(0, xrd64(p)), 27) * XP1 + XP4;
    if (p + 4 <= end) {
        h = xrotl(h ^ ((uint64_t)xrd32(p) * XP1), 23) * XP2 + XP3;
        p += 4;
    }
    for (; p < end; p++)
        h = xrotl(h ^ (*p * XP5), 11) * XP1;
    h ^= h >> 33;
    h *= XP2;
    h ^= h >> 29;
    h *= XP3;
    h ^= h >> 32;
    return h;
}

uint64_t snap_test_xxh64(const void *buf, size_t len, size_t step)
{
    xstate s;
    x_init(&s);
    for (size_t o = 0; o < len; o += step)
        x_update(&s, (const uint8_t *)buf + o, len - o < step ? len - o : step);
    return x_final(&s);
}

#define IOBUF (1u << 20)

int snap_xxh64_fd(int fd, uint64_t *size, uint64_t *xxh)
{
    uint8_t *b = malloc(IOBUF);
    if (!b)
        return TSDB_ERR_NOMEM;
    xstate s;
    x_init(&s);
    uint64_t off = 0;
    int rc = 0;
    for (;;) {
        ssize_t got = pread(fd, b, IOBUF, (off_t)off);
        if (got < 0 && errno == EINTR)
            continue;
        if (got < 0) {
            rc = TSDB_ERR_IO; /* a read error is never the end of the file */
            break;
        }
        if (got == 0)
            break;
        x_update(&s, b, (size_t)got);
        off += (uint64_t)got;
    }
    free(b);
    *size = off;
    *xxh = x_final(&s);
    return rc;
}

/* ---- the make ---- */

struct sfile {
    char name[128];
    uint8_t kind;
    uint64_t size, xxh;
};

typedef struct mk_ctx {
    eng *db;
    int sdfd;
    struct sfile *f;
    size_t nf, cap;
    struct snap_info *out;
} mk_ctx;

static int add_file(mk_ctx *c, const char *name, uint8_t kind, uint64_t size, uint64_t xxh)
{
    if (strlen(name) >= sizeof c->f[0].name)
        return TSDB_ERR_INVALID;
    if (c->nf == c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 64;
        void *p = realloc(c->f, nc * sizeof *c->f);
        if (!p)
            return TSDB_ERR_NOMEM;
        c->f = p, c->cap = nc;
    }
    struct sfile *f = &c->f[c->nf++];
    snprintf(f->name, sizeof f->name, "%s", name);
    f->kind = kind, f->size = size, f->xxh = xxh;
    c->out->bytes += size;
    return 0;
}

/* Hashes and syncs a file just made in the snapshot directory, and lists it */
static int seal_file(mk_ctx *c, const char *name, uint8_t kind)
{
    int fd = openat(c->sdfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    uint64_t size, xxh;
    int rc = snap_xxh64_fd(fd, &size, &xxh);
    if (rc == 0 && c->db->opt.fsync && tsdb_fdatasync(fd) != 0)
        rc = TSDB_ERR_IO;
    close(fd);
    return rc ? rc : add_file(c, name, kind, size, xxh);
}

/* Hard links the volume's `src` as `dst`: 0, 1 when `src` is not there, or an error */
static int link_one(mk_ctx *c, const char *src, const char *dst, uint8_t kind)
{
    if (linkat(c->db->dirfd, src, c->sdfd, dst, 0) != 0)
        return errno == ENOENT ? 1 : TSDB_ERR_IO;
    return seal_file(c, dst, kind);
}

/* Copies the first `len` bytes of the volume's `src` to `dst` (a file shorter than that is damage: the engine wrote
 * those bytes); `cut`: a WAL segment, copied record by record up to the one that ends at `cut` (P) */
static int copy_file(mk_ctx *c, const char *src, const char *dst, uint64_t len, uint8_t kind)
{
    int in = openat(c->db->dirfd, src, O_RDONLY | O_CLOEXEC);
    if (in < 0)
        return errno == ENOENT ? 1 : TSDB_ERR_IO;
    int out = openat(c->sdfd, dst, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    uint8_t *b = malloc(IOBUF);
    int rc = out < 0 ? TSDB_ERR_IO : !b ? TSDB_ERR_NOMEM : 0;
    for (uint64_t off = 0; rc == 0 && off < len;) {
        size_t want = len - off < IOBUF ? (size_t)(len - off) : IOBUF;
        ssize_t got = pread(in, b, want, (off_t)off);
        if (got < 0 && errno == EINTR)
            continue;
        if (got <= 0) {
            rc = got == 0 ? TSDB_ERR_CORRUPT : TSDB_ERR_IO;
            break;
        }
        if (write_full(out, b, (size_t)got) != 0)
            rc = TSDB_ERR_IO;
        off += (uint64_t)got;
    }
    free(b);
    close(in);
    if (out >= 0)
        close(out);
    return rc ? rc : seal_file(c, dst, kind);
}

/* The byte where the record that ends at position p ends in segment `seq` (records are read in windows, never the whole
 * segment at once): 0 and *end; TSDB_ERR_INVALID when p is inside a record or no record ends there */
static int seg_end_at(eng *db, uint64_t seq, uint64_t p, uint64_t *end)
{
    char name[64];
    wal_name(name, sizeof name, seq);
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? SNAP_RETRY : TSDB_ERR_IO;
    size_t cap = IOBUF;
    uint8_t *b = malloc(cap);
    uint64_t base = 0; /* file offset of b[0] */
    size_t have = 0;
    int rc = b ? 1 : TSDB_ERR_NOMEM;
    bool eof = false;
    while (rc == 1) {
        if (!eof && have < cap) {
            ssize_t got = pread(fd, b + have, cap - have, (off_t)(base + have));
            if (got < 0 && errno == EINTR)
                continue;
            if (got < 0) {
                rc = TSDB_ERR_IO;
                break;
            }
            eof = got == 0;
            have += (size_t)got;
        }
        size_t pos = 0;
        while (rc == 1 && pos < have) {
            wal_rec wr;
            size_t sz = wal_record(b + pos, have - pos, seq, base + pos, &wr);
            if (!sz)
                break;
            uint64_t last = wr.fno + wr.span - 1;
            if (wr.fno > p || last > p) {
                rc = TSDB_ERR_INVALID; /* past p without a record ending there */
                break;
            }
            pos += sz;
            if (last == p) {
                *end = base + pos;
                rc = 0;
            }
        }
        if (rc != 1)
            break;
        if (pos == 0 && eof) {
            rc = TSDB_ERR_INVALID; /* the bytes end (or stop parsing) before p's record */
            break;
        }
        if (pos == 0 && have == cap) { /* one record larger than the window */
            uint8_t *nb = realloc(b, cap * 2);
            if (!nb) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            b = nb, cap *= 2;
            continue;
        }
        memmove(b, b + pos, have - pos);
        base += pos, have -= pos;
        if (eof && pos == 0)
            rc = TSDB_ERR_INVALID;
    }
    free(b);
    close(fd);
    return rc;
}

/* The first position of segment seq (0: no record parses at its start) */
static int seg_first(eng *db, uint64_t seq, uint64_t *first)
{
    char name[64];
    wal_name(name, sizeof name, seq);
    *first = 0;
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? SNAP_RETRY : TSDB_ERR_IO;
    size_t cap = 65536;
    int rc = 1;
    while (rc == 1) {
        uint8_t *b = malloc(cap);
        if (!b) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        ssize_t got = pread(fd, b, cap, 0);
        if (got < 0) {
            free(b);
            rc = errno == EINTR ? 1 : TSDB_ERR_IO;
            continue;
        }
        wal_rec wr;
        if (wal_record(b, (size_t)got, seq, 0, &wr))
            *first = wr.fno, rc = 0;
        else if ((size_t)got < cap)
            rc = 0; /* empty, or its first record is still being written */
        else
            cap *= 2;
        free(b);
    }
    close(fd);
    return rc;
}

typedef struct seginfo {
    uint64_t seq, first;
} seginfo;

static int cmp_seginfo(const void *a, const void *b)
{
    const seginfo *x = a, *y = b;
    return x->seq < y->seq ? -1 : x->seq > y->seq;
}

/* The volume's wal_ segments with their first positions, ascending */
static int list_segs(eng *db, seginfo **out, size_t *n)
{
    *out = NULL, *n = 0;
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    seginfo *v = NULL;
    size_t nv = 0, cap = 0;
    int rc = 0;
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) {
        uint64_t sq;
        int consumed = 0;
        if (!wal_file_name(e->d_name) || sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &sq, &consumed) != 1 ||
            e->d_name[consumed])
            continue;
        if (nv == cap) {
            size_t nc = cap ? cap * 2 : 16;
            void *p = realloc(v, nc * sizeof *v);
            if (!p) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            v = p, cap = nc;
        }
        v[nv++] = (seginfo){sq, 0};
    }
    if (rc == 0 && errno)
        rc = TSDB_ERR_IO; /* a readdir error is an error, never the end of the directory */
    closedir(d);
    if (rc == 0)
        qsort(v, nv, sizeof *v, cmp_seginfo);
    for (size_t i = 0; rc == 0 && i < nv; i++) {
        rc = seg_first(db, v[i].seq, &v[i].first);
        if (rc == SNAP_RETRY) { /* gone since the listing (a flush removed it): not there */
            memmove(v + i, v + i + 1, (nv - i - 1) * sizeof *v);
            nv--, i--;
            rc = 0;
        }
    }
    if (rc) {
        free(v);
        return rc;
    }
    *out = v, *n = nv;
    return 0;
}

/* The volume number and uuid META records ("volume=N", "vuuid=<32 hex>") */
static void meta_ids(const uint8_t *b, size_t len, int64_t *volume, uint8_t uuid[16])
{
    *volume = -1;
    memset(uuid, 0, 16);
    for (size_t o = 0; o < len;) {
        const uint8_t *nl = memchr(b + o, '\n', len - o);
        size_t l = nl ? (size_t)(nl - (b + o)) : len - o;
        char line[96];
        if (l < sizeof line) {
            memcpy(line, b + o, l);
            line[l] = 0;
            unsigned long v;
            char hex[33];
            if (sscanf(line, "volume=%lu", &v) == 1)
                *volume = (int64_t)v;
            else if (sscanf(line, "vuuid=%32[0-9a-fA-F]", hex) == 1 && strlen(hex) == 32)
                for (int i = 0; i < 16; i++) {
                    unsigned x;
                    sscanf(hex + 2 * i, "%2x", &x);
                    uuid[i] = (uint8_t)x;
                }
        }
        o += l + 1;
    }
}

static void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void put64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }

static int write_man(mk_ctx *c, int64_t volume, const uint8_t uuid[16], const struct snap_info *si)
{
    size_t nh = 0;
    for (size_t i = 0; i < si->nhist; i++)
        if (si->hist[i].first <= si->p)
            nh++;
    size_t n = 8 + 16 + 8 + 16 + 6 * 8 + nh * 16 + 8;
    for (size_t i = 0; i < c->nf; i++)
        n += 2 + strlen(c->f[i].name) + 1 + 16;
    uint8_t *b = calloc(1, n);
    if (!b)
        return TSDB_ERR_NOMEM;
    size_t o = 0;
    memcpy(b, "TSNAPMAN", 8), o += 8;
    put32(b + o, SNAP_MAN_VERSION), o += 4;
    put32(b + o, (uint32_t)nh), o += 4;
    put32(b + o, (uint32_t)c->nf), o += 4;
    put32(b + o, 0), o += 4;
    put64(b + o, (uint64_t)volume), o += 8;
    memcpy(b + o, uuid, 16), o += 16;
    put64(b + o, si->p), o += 8;
    put64(b + o, si->p_epoch), o += 8;
    put64(b + o, si->p_seq), o += 8;
    put64(b + o, si->s_seq), o += 8;
    put64(b + o, si->s_first), o += 8;
    put64(b + o, si->done), o += 8;
    for (size_t i = 0; i < si->nhist; i++)
        if (si->hist[i].first <= si->p) {
            put64(b + o, si->hist[i].epoch), o += 8;
            put64(b + o, si->hist[i].first), o += 8;
        }
    for (size_t i = 0; i < c->nf; i++) {
        uint16_t l = (uint16_t)strlen(c->f[i].name);
        memcpy(b + o, &l, 2), o += 2;
        memcpy(b + o, c->f[i].name, l), o += l;
        b[o++] = c->f[i].kind;
        put64(b + o, c->f[i].size), o += 8;
        put64(b + o, c->f[i].xxh), o += 8;
    }
    put64(b + o, xxh64(b, o, 0)), o += 8;
    int fd = openat(c->sdfd, SNAP_MAN ".tmp", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, b, o) != 0 || (c->db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0)
        close(fd);
    free(b);
    if (rc == 0 && renameat(c->sdfd, SNAP_MAN ".tmp", c->sdfd, SNAP_MAN) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && c->db->opt.fsync && tsdb_fsync(c->sdfd) != 0)
        rc = TSDB_ERR_IO;
    return rc;
}

static bool snap_name_ok(const char *name)
{
    size_t l = strlen(name);
    if (l <= strlen(SNAP_PREFIX) || l > 64 || strncmp(name, SNAP_PREFIX, strlen(SNAP_PREFIX)) != 0)
        return false;
    for (size_t i = 0; i < l; i++)
        if (!((name[i] >= 'a' && name[i] <= 'z') || (name[i] >= 'A' && name[i] <= 'Z') ||
              (name[i] >= '0' && name[i] <= '9') || name[i] == '_' || name[i] == '-' || name[i] == '.'))
            return false;
    return strcmp(name + strlen(SNAP_PREFIX), ".") && strcmp(name + strlen(SNAP_PREFIX), "..");
}

struct dictlen {
    uint64_t series, states, ret, cls;
};

/* The lock barrier: each lock alone, the length its writers append at, released (snapshot.c's header) */
static void dict_lengths(eng *db, struct dictlen *dl)
{
    pthread_mutex_lock(&db->series.append_lock);
    dl->series = db->series.file_off;
    pthread_mutex_unlock(&db->series.append_lock);
    pthread_mutex_lock(&db->states.append_lock);
    dl->states = db->states.file_off;
    pthread_mutex_unlock(&db->states.append_lock);
    pthread_mutex_lock(&db->ret_lock);
    dl->ret = db->ret_off;
    pthread_mutex_unlock(&db->ret_lock);
    pthread_mutex_lock(&db->class_lock);
    dl->cls = db->cls_off;
    pthread_mutex_unlock(&db->class_lock);
}

static int copy_dicts(mk_ctx *c, const struct dictlen *dl)
{
    const struct {
        const char *name;
        uint64_t len;
    } d[] = {{"SERIES", dl->series}, {"STATES", dl->states}, {"RETENTION", dl->ret}, {"CLASSES", dl->cls}};
    for (size_t i = 0; i < sizeof d / sizeof d[0]; i++) {
        if (d[i].len == 0)
            continue; /* nothing written (the file may not exist) */
        int rc = copy_file(c, d[i].name, d[i].name, d[i].len, SNAP_DICT);
        if (rc)
            return rc == 1 ? TSDB_ERR_CORRUPT : rc; /* bytes the engine wrote are gone: damage */
    }
    return 0;
}

static int link_parts(mk_ctx *c, char (*names)[96], size_t n)
{
    for (size_t i = 0; i < n; i++) {
        hook(c->db, SNAP_AT_PART, names[i]);
        int rc = link_one(c, names[i], names[i], SNAP_PART);
        if (rc == 1 && snap_mutant != SNAP_MUT_NORETIRED) {
            char r[128];
            snprintf(r, sizeof r, "retired_%s", names[i]); /* a merge took it after the cut */
            rc = link_one(c, r, names[i], SNAP_PART);
        }
        if (rc == 1)
            return SNAP_RETRY; /* retired and deleted already: make it again */
        if (rc)
            return rc;
        c->out->nparts++;
    }
    return 0;
}

static int list_part_names(eng *db, char (**out)[96], size_t *n)
{
    pthread_mutex_lock(&db->lock);
    size_t np = db->nparts;
    char(*v)[96] = malloc((np ? np : 1) * sizeof *v);
    int rc = v ? 0 : TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < np; i++) {
        size_t l = strlen(db->parts[i]->name);
        if (l > 3 && strcmp(db->parts[i]->name + l - 3, ".ec") == 0)
            rc = TSDB_ERR_INVALID; /* settled parts in a snapshot are R8's */
        memcpy(v[i], db->parts[i]->name, sizeof v[i]);
    }
    pthread_mutex_unlock(&db->lock);
    if (rc) {
        free(v);
        return rc;
    }
    *out = v, *n = np;
    return 0;
}

int snap_make(eng *db, const char *name, struct snap_info *out)
{
    if (!eng_jr(db) || !db->lrs || db->rp || !snap_name_ok(name))
        return TSDB_ERR_INVALID;
    const struct snap_epoch *hist = out->hist;
    size_t nhist = out->nhist;
    memset(out, 0, sizeof *out);
    out->hist = hist, out->nhist = nhist;
    snprintf(out->path, sizeof out->path, "%s/%s", db->dir, name);
    if (mkdirat(db->dirfd, name, 0755) != 0)
        return errno == EEXIST ? TSDB_ERR_INVALID : TSDB_ERR_IO;
    mk_ctx c = {.db = db, .out = out};
    c.sdfd = openat(db->dirfd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    int rc = c.sdfd < 0 ? TSDB_ERR_IO : 0;
    char(*parts)[96] = NULL;
    size_t nparts = 0, nlf = 0, nsegs = 0;
    struct lkfile *lf = NULL;
    uint8_t *rsb = NULL, *meta = NULL;
    size_t rsl = 0, metal = 0;
    seginfo *segs = NULL;
    struct dictlen dl = {0};
    if (rc)
        goto out;
    if (snap_mutant == SNAP_MUT_DICTSHORT)
        dict_lengths(db, &dl); /* planted: before the cut, so a record whose marker P passes may be left out */

    /* ---- the cut (snapshot.c's header): memory only ---- */
    uint64_t p, done, mem_seq = UINT64_MAX;
    pthread_mutex_lock(&db->flush_lock);
    pthread_mutex_lock(&db->publish_lock);
    p = atomic_load(&db->lead_commit);
    if (snap_mutant != SNAP_MUT_AFTERP)
        rc = list_part_names(db, &parts, &nparts);
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    for (size_t i = 0; i <= db->nflushing; i++) {
        memtable *m = i < db->nflushing ? db->flushing[i] : db->active;
        if (m && m->rows && (m->fno_lo == 0 || m->fno_lo <= p))
            mem_seq = MIN(mem_seq, m->wal_seq); /* committed rows not in parts: their segment is replayed */
    }
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    if (rc == 0)
        rc = leader_rs_image(db, &rsb, &rsl, &done);
    if (rc == 0)
        rc = leader_links_list(db, p, &lf, &nlf);
    pthread_mutex_unlock(&db->publish_lock);
    pthread_mutex_unlock(&db->flush_lock);
    if (rc)
        goto out;
    out->p = p, out->done = done;
    hook(db, SNAP_AT_CUT, NULL);

    /* ---- the parts ---- */
    if (snap_mutant != SNAP_MUT_AFTERP && (rc = link_parts(&c, parts, nparts)) != 0)
        goto out;
    hook(db, SNAP_AT_LINKED, NULL);

    /* ---- the dictionaries, through the lock barrier ---- */
    if (snap_mutant != SNAP_MUT_DICTSHORT)
        dict_lengths(db, &dl);
    if ((rc = copy_dicts(&c, &dl)) != 0)
        goto out;
    if (snap_mutant == SNAP_MUT_AFTERP) { /* planted: the list as it is now, past the cut */
        if ((rc = list_part_names(db, &parts, &nparts)) != 0 || (rc = link_parts(&c, parts, nparts)) != 0)
            goto out;
    }
    hook(db, SNAP_AT_DICTS, NULL);

    /* ---- the log: S .. P's segment, and the COMMITs' files in it ---- */
    if ((rc = list_segs(db, &segs, &nsegs)) != 0)
        goto out;
    uint64_t need_pos = UINT64_MAX; /* the oldest COMMIT above done: its parts may not be listed */
    for (size_t i = 0; i < nlf; i++)
        if (!lf[i].keep && lf[i].fno > done)
            need_pos = MIN(need_pos, lf[i].fno);
    uint64_t pseq = 0, need_seq = 0;
    for (size_t i = 0; i < nsegs; i++)
        if (segs[i].first && segs[i].first <= p)
            pseq = segs[i].seq;
    for (size_t i = 0; need_pos != UINT64_MAX && i < nsegs; i++)
        if (segs[i].first && segs[i].first <= need_pos)
            need_seq = segs[i].seq;
    if (need_pos != UINT64_MAX && !need_seq) {
        rc = SNAP_RETRY; /* its segment went: the write has published since (parts are listed) */
        goto out;
    }
    uint64_t sseq = pseq;
    if (mem_seq != UINT64_MAX)
        sseq = MIN(sseq, mem_seq);
    if (need_seq)
        sseq = MIN(sseq, need_seq);
    if (p && !pseq && mem_seq != UINT64_MAX) {
        rc = TSDB_ERR_CORRUPT; /* rows not in parts, and no segment holds P */
        goto out;
    }
    out->p_seq = pseq, out->s_seq = pseq ? sseq : 0;
    for (size_t i = 0; pseq && i < nsegs; i++) {
        if (segs[i].seq < sseq || segs[i].seq > pseq)
            continue;
        if (!out->s_first)
            out->s_first = segs[i].first;
        char nm[64];
        wal_name(nm, sizeof nm, segs[i].seq);
        if (segs[i].seq == pseq) {
            uint64_t end;
            if ((rc = seg_end_at(db, pseq, p, &end)) != 0)
                goto out;
            rc = copy_file(&c, nm, nm, end, SNAP_SEG);
        } else {
            rc = link_one(&c, nm, nm, SNAP_SEG);
        }
        if (rc == 1)
            rc = SNAP_RETRY; /* a flush and the keep bound removed it */
        if (rc)
            goto out;
        out->nsegs++;
    }
    /* A gap in the numbers between S and P's segment is a promotion's jump, never a removed segment: one at or above S
     * goes only after a flush past the cut, and then the link above found it gone (SNAP_RETRY) or it was linked. */
    for (size_t i = 0; i < nlf; i++) {
        if (lf[i].fno < out->s_first || !out->s_first)
            continue; /* its marker is in parts already */
        bool above = !lf[i].keep && lf[i].fno > done;
        if ((above && snap_mutant == SNAP_MUT_NOSTAGE) || (!above && snap_mutant == SNAP_MUT_NOKEEP))
            continue;
        char src[64], dst[64];
        leader_link_name(src, sizeof src, &lf[i], lf[i].keep);
        leader_link_name(dst, sizeof dst, &lf[i], !above);
        rc = link_one(&c, src, dst, above ? SNAP_STAGE : SNAP_KEEP);
        if (rc == 1)
            rc = SNAP_RETRY; /* its segment went with it */
        if (rc)
            goto out;
        if (above)
            out->nstage++;
        else
            out->nkeep++;
    }
    hook(db, SNAP_AT_SEGS, NULL);

    /* ---- RSTATE, META ---- */
    int fd = openat(c.sdfd, "RSTATE", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    rc = fd < 0 || write_full(fd, rsb, rsl) != 0 ? TSDB_ERR_IO : 0;
    if (fd >= 0)
        close(fd);
    if (rc == 0)
        rc = seal_file(&c, "RSTATE", SNAP_RSTATE);
    int64_t volume = -1;
    uint8_t uuid[16] = {0};
    if (rc == 0 && (rc = read_file(db->dirfd, "META", &meta, &metal)) == 0) {
        meta_ids(meta, metal, &volume, uuid);
        rc = copy_file(&c, "META", "META", metal, SNAP_META);
        if (rc == 1)
            rc = TSDB_ERR_IO;
    }
    if (rc)
        goto out;

    /* ---- every file synced (seal_file), the directory, then SNAPMAN ---- */
    for (size_t i = 0; i < nhist; i++)
        if (hist[i].first <= p)
            out->p_epoch = hist[i].epoch;
    if (db->opt.fsync && snap_mutant != SNAP_MUT_NODIRSYNC && tsdb_fsync(c.sdfd) != 0) {
        rc = TSDB_ERR_IO;
        goto out;
    }
    hook(db, SNAP_AT_SYNCED, NULL);
    rc = write_man(&c, volume, uuid, out);
    out->nfiles = c.nf;
out:
    if (rc == 0)
        hook(db, SNAP_AT_DONE, NULL);
    if (c.sdfd >= 0)
        close(c.sdfd);
    if (rc)
        snap_release(out->path);
    free(parts);
    free(lf);
    free(rsb);
    free(meta);
    free(segs);
    free(c.f);
    return rc;
}

/* ---- release and sweep ---- */

static int rm_dir_at(int pfd, const char *name)
{
    int dfd = openat(pfd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (dfd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    DIR *d = fdopendir(dup(dfd));
    int rc = d ? 0 : TSDB_ERR_IO;
    if (d) {
        rewinddir(d);
        struct dirent *e;
        for (errno = 0; (e = readdir(d)); errno = 0) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
                continue;
            if (unlinkat(dfd, e->d_name, 0) != 0 && errno != ENOENT)
                rc = TSDB_ERR_IO;
        }
        if (errno)
            rc = TSDB_ERR_IO;
        closedir(d);
    }
    close(dfd);
    if (rc == 0 && unlinkat(pfd, name, AT_REMOVEDIR) != 0 && errno != ENOENT)
        rc = TSDB_ERR_IO;
    return rc;
}

int snap_release(const char *path)
{
    char parent[PATH_MAX];
    snprintf(parent, sizeof parent, "%s", path);
    char *slash = strrchr(parent, '/');
    if (!slash || !slash[1])
        return TSDB_ERR_INVALID;
    *slash = 0;
    int pfd = open(parent[0] ? parent : "/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (pfd < 0)
        return TSDB_ERR_IO;
    int rc = rm_dir_at(pfd, slash + 1);
    if (rc == 0 && tsdb_fsync(pfd) != 0)
        rc = TSDB_ERR_IO;
    close(pfd);
    return rc;
}

int snap_sweep(eng *db)
{
    DIR *d = fdopendir(dup(db->dirfd));
    if (!d)
        return TSDB_ERR_IO;
    rewinddir(d);
    char(*v)[256] = NULL;
    size_t n = 0;
    int rc = 0;
    struct dirent *e;
    for (errno = 0; (e = readdir(d)); errno = 0) {
        if (strncmp(e->d_name, SNAP_PREFIX, strlen(SNAP_PREFIX)) != 0)
            continue;
        struct stat st;
        if (fstatat(db->dirfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISDIR(st.st_mode))
            continue;
        void *p = realloc(v, (n + 1) * sizeof *v);
        if (!p) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        v = p;
        snprintf(v[n++], sizeof v[0], "%s", e->d_name);
    }
    if (rc == 0 && errno)
        rc = TSDB_ERR_IO;
    closedir(d);
    for (size_t i = 0; rc == 0 && i < n; i++)
        rc = rm_dir_at(db->dirfd, v[i]);
    if (rc == 0 && n && db->opt.fsync && tsdb_fsync(db->dirfd) != 0)
        rc = TSDB_ERR_IO; /* the open: no other user of the directory's fd yet */
    if (n)
        fprintf(stderr, "tsdb: %s: removed %zu snapshot director%s of an earlier run\n", db->dir, n, n == 1 ? "y" : "ies");
    free(v);
    return rc;
}

/* ---- SNAPMAN, read back ---- */

static uint64_t get64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

int snap_man_read(const char *dir, struct snap_man *m)
{
    memset(m, 0, sizeof *m);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    uint8_t *b = NULL;
    size_t len = 0;
    int rc = faccessat(dfd, SNAP_MAN, F_OK, 0) != 0 && errno == ENOENT ? TSDB_ERR_NOTFOUND
                                                                       : read_file(dfd, SNAP_MAN, &b, &len);
    close(dfd);
    if (rc)
        return rc;
    const size_t head = 8 + 16 + 8 + 16 + 6 * 8;
    bool ok = len >= head + 8 && memcmp(b, "TSNAPMAN", 8) == 0 && get64(b + len - 8) == xxh64(b, len - 8, 0);
    uint32_t nh = 0, nf = 0;
    if (ok) {
        memcpy(&m->version, b + 8, 4);
        memcpy(&nh, b + 12, 4);
        memcpy(&nf, b + 16, 4);
        ok = m->version == SNAP_MAN_VERSION && nh <= (len - head) / 16;
    }
    size_t o = 24;
    if (ok) {
        m->volume = (int64_t)get64(b + o), o += 8;
        memcpy(m->uuid, b + o, 16), o += 16;
        m->p = get64(b + o), o += 8;
        m->p_epoch = get64(b + o), o += 8;
        m->p_seq = get64(b + o), o += 8;
        m->s_seq = get64(b + o), o += 8;
        m->s_first = get64(b + o), o += 8;
        m->done = get64(b + o), o += 8;
        m->hist = calloc(nh ? nh : 1, sizeof *m->hist);
        m->files = calloc(nf ? nf : 1, sizeof *m->files);
        if (!m->hist || !m->files) {
            free(b);
            snap_man_free(m);
            return TSDB_ERR_NOMEM;
        }
        for (uint32_t i = 0; i < nh; i++, o += 16)
            m->hist[i] = (struct snap_epoch){get64(b + o), get64(b + o + 8)};
        m->nhist = nh;
        for (uint32_t i = 0; ok && i < nf; i++) {
            uint16_t l;
            if (o + 2 > len - 8) {
                ok = false;
                break;
            }
            memcpy(&l, b + o, 2), o += 2;
            if (l == 0 || l >= sizeof m->files[i].name || o + l + 17 > len - 8 || memchr(b + o, '/', l) ||
                memchr(b + o, 0, l)) {
                ok = false;
                break;
            }
            memcpy(m->files[i].name, b + o, l), o += l;
            m->files[i].kind = b[o++];
            m->files[i].size = get64(b + o), o += 8;
            m->files[i].xxh64 = get64(b + o), o += 8;
            m->nfiles++;
        }
        ok = ok && o == len - 8;
    }
    free(b);
    if (!ok) {
        snap_man_free(m);
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

void snap_man_free(struct snap_man *m)
{
    free(m->hist);
    free(m->files);
    m->hist = NULL, m->files = NULL;
    m->nhist = m->nfiles = 0;
}
