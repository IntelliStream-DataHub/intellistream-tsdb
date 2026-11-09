/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The change feed's files (feed.h): FEED, FEEDSLOTS, the word in META, and the order in which they are made and
 * removed. docs/design-cdc.md 2.7 and 2.13.
 *
 * FEED is replaced like META (tmp, fsync, rename): followers on other hosts read it by name. FEEDSLOTS is written in
 * place like MANIFEST and FNO, never renamed after it is made: two checksummed 4 KiB halves, the anchor (the newest
 * checkpoint known durable) and the scratch. A checkpoint goes to the scratch, is fdatasynced, and only then is the
 * scratch the anchor; so the anchor is never overwritten, and after a power loss it is intact. Its checkpoint now
 * holds the cleaner's floor; the halves are chosen by `gen` (a cleaner-only checkpoint does not move `ckpt_fno`,
 * so two halves can tie on it). The file is made at its full size. ENOSPC on a copy-on-write file system can still
 * happen on an in-place write (ZFS): it is fail-stop like any failed write of this file, and the free-space rule acts
 * long before that point.
 */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

#define FEED_MAGIC 0x3145454644425354ull /* "TSDBFEE1" little endian */
#define FSLOTS_MAGIC 0x314c534644425354ull /* "TSDBFSL1" */
#define FEED_SIZE 64
#define FS_PAGE 4096
#define FS_HDR 64

/* ------------------------------------------------------------------ names */

bool fwal_file_name(const char *name)
{
    return strlen(name) == 25 && strncmp(name, "fwal_", 5) == 0 && strspn(name + 5, "0123456789abcdef") == 16 &&
           strcmp(name + 21, ".log") == 0;
}

void fwal_name(char *buf, size_t len, uint64_t seq) { snprintf(buf, len, "fwal_%016" PRIx64 ".log", seq); }

/* ------------------------------------------------------------- small I/O */

static int dsync(const feed_dir *d)
{
    if (d->db)
        return eng_sync_dir(d->db, "fsync of the directory after a change of the feed's files");
    return !d->sync || tsdb_fsync(d->dirfd) == 0 ? 0 : TSDB_ERR_IO;
}

static int read_whole(int dirfd, const char *name, uint8_t **out, size_t *len)
{
    *out = NULL;
    *len = 0;
    int fd = openat(dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    size_t cap = 4096, n = 0;
    uint8_t *b = malloc(cap + 1);
    if (!b) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    for (;;) {
        if (n == cap) {
            uint8_t *nb = realloc(b, cap * 2 + 1);
            if (!nb) {
                free(b);
                close(fd);
                return TSDB_ERR_NOMEM;
            }
            b = nb;
            cap *= 2;
        }
        ssize_t r = read(fd, b + n, cap - n);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            free(b);
            close(fd);
            return TSDB_ERR_IO;
        }
        if (r == 0)
            break;
        n += (size_t)r;
    }
    close(fd);
    b[n] = 0;
    *out = b;
    *len = n;
    return 0;
}

/* tmp, write, fsync, rename. The directory fsync is the caller's. */
static int put_file(const feed_dir *d, const char *tmp, const char *name, const void *data, size_t n)
{
    int fd = openat(d->dirfd, tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;
    const uint8_t *p = data;
    size_t left = n;
    bool ok = true;
    while (left && ok) {
        ssize_t w = write(fd, p, left);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            ok = false;
            break;
        }
        p += w;
        left -= (size_t)w;
    }
    if (ok && d->sync && tsdb_fsync(fd) != 0)
        ok = false;
    close(fd);
    if (ok && renameat(d->dirfd, tmp, d->dirfd, name) != 0)
        ok = false;
    if (!ok) {
        int e = errno;
        unlinkat(d->dirfd, tmp, 0);
        errno = e;
        return TSDB_ERR_IO;
    }
    return 0;
}

/* ------------------------------------------------------------------ FEED */

void feed_cfg_defaults(feed_cfg *c, const feed_opts *o)
{
    memset(c, 0, sizeof *c);
    c->retention_s = o && o->retention_s ? o->retention_s : FEED_DEF_RETENTION_S;
    c->max_bytes = o && o->max_bytes ? o->max_bytes : FEED_DEF_MAX_BYTES;
    c->min_free_pct = o && o->min_free_pct ? o->min_free_pct : FEED_DEF_MIN_FREE_PCT;
    c->created_ms = eng_wall_ms();
    while (c->feed_id == 0) {
        if (getrandom(&c->feed_id, sizeof c->feed_id, 0) != (ssize_t)sizeof c->feed_id)
            c->feed_id = (uint64_t)eng_wall_ms() * 0x9e3779b97f4a7c15ull ^ (uint64_t)getpid();
    }
}

static void feed_fill(uint8_t *b, const feed_cfg *c)
{
    uint32_t version = 1;
    memset(b, 0, FEED_SIZE);
    uint64_t magic = FEED_MAGIC;
    memcpy(b, &magic, 8);
    memcpy(b + 8, &c->feed_id, 8);
    memcpy(b + 16, &c->max_bytes, 8);
    memcpy(b + 24, &c->created_ms, 8);
    memcpy(b + 32, &c->retention_s, 4);
    memcpy(b + 36, &c->min_free_pct, 4);
    memcpy(b + 40, &c->seeded, 4);
    memcpy(b + 44, &version, 4);
    uint64_t h = xxh64(b, 56, 0);
    memcpy(b + 56, &h, 8);
}

static bool feed_parse(const uint8_t *b, size_t n, feed_cfg *c)
{
    uint64_t magic, h;
    uint32_t version;
    if (n != FEED_SIZE)
        return false;
    memcpy(&magic, b, 8);
    memcpy(&h, b + 56, 8);
    memcpy(&version, b + 44, 4);
    if (magic != FEED_MAGIC || h != xxh64(b, 56, 0) || version != 1)
        return false;
    memcpy(&c->feed_id, b + 8, 8);
    memcpy(&c->max_bytes, b + 16, 8);
    memcpy(&c->created_ms, b + 24, 8);
    memcpy(&c->retention_s, b + 32, 4);
    memcpy(&c->min_free_pct, b + 36, 4);
    memcpy(&c->seeded, b + 40, 4);
    return c->feed_id != 0;
}

int feed_cfg_read(int dirfd, feed_cfg *c)
{
    uint8_t *b;
    size_t n;
    int rc = read_whole(dirfd, FEED_FILE, &b, &n);
    if (rc)
        return rc;
    bool ok = feed_parse(b, n, c);
    free(b);
    return ok ? 0 : TSDB_ERR_CORRUPT;
}

int feed_cfg_write(const feed_dir *d, const feed_cfg *c)
{
    uint8_t b[FEED_SIZE];
    feed_fill(b, c);
    return put_file(d, FEED_FILE ".tmp", FEED_FILE, b, sizeof b);
}

/* -------------------------------------------------------------- FEEDSLOTS */

static void fs_fill(uint8_t *page, const fslots *s)
{
    memset(page, 0, FS_PAGE);
    uint64_t magic = FSLOTS_MAGIC;
    memcpy(page, &magic, 8);
    memcpy(page + 8, &s->gen, 8);
    memcpy(page + 16, &s->floor_event, 8);
    memcpy(page + 24, &s->ret_fed, 8);
    memcpy(page + 32, &s->ckpt_fno, 8);
    memcpy(page + 40, &s->floor_check, 4);
    memcpy(page + 44, &s->body_len, 4);
    uint64_t h = xxh64(page + FS_HDR, s->body_len, xxh64(page, 56, 0));
    memcpy(page + 56, &h, 8);
}

static bool fs_parse(const uint8_t *page, fslots *s)
{
    uint64_t magic, h;
    memcpy(&magic, page, 8);
    memcpy(&h, page + 56, 8);
    fslots t;
    memcpy(&t.gen, page + 8, 8);
    memcpy(&t.floor_event, page + 16, 8);
    memcpy(&t.ret_fed, page + 24, 8);
    memcpy(&t.ckpt_fno, page + 32, 8);
    memcpy(&t.floor_check, page + 40, 4);
    memcpy(&t.body_len, page + 44, 4);
    if (magic != FSLOTS_MAGIC || t.body_len > FS_PAGE - FS_HDR || h != xxh64(page + FS_HDR, t.body_len, xxh64(page, 56, 0)))
        return false;
    *s = t;
    return true;
}

/* Both halves of a file image: *best is the valid one with the higher gen (index in *which). 0, or CORRUPT. */
static int fs_pick(const uint8_t *img, size_t n, fslots *best, int *which, bool *torn)
{
    fslots s[2];
    bool ok[2];
    for (int i = 0; i < 2; i++)
        ok[i] = n >= (size_t)(i + 1) * FS_PAGE && fs_parse(img + (size_t)i * FS_PAGE, &s[i]);
    if (!ok[0] && !ok[1])
        return TSDB_ERR_CORRUPT;
    int b = !ok[0] || (ok[1] && s[1].gen > s[0].gen) ? 1 : 0;
    *best = s[b];
    if (which)
        *which = b;
    if (torn)
        *torn = !(ok[0] && ok[1]);
    return 0;
}

int fslots_read(int dirfd, fslots *s)
{
    uint8_t *b;
    size_t n;
    int rc = read_whole(dirfd, FEEDSLOTS_FILE, &b, &n);
    if (rc)
        return rc;
    rc = fs_pick(b, n, s, NULL, NULL);
    free(b);
    return rc;
}

/* Makes FEEDSLOTS at its full size: both halves valid, the anchor (gen 1) and a copy (gen 0). */
static int fslots_make(const feed_dir *d, uint64_t floor_event, uint64_t ret_fed)
{
    uint8_t *img = calloc(2, FS_PAGE);
    if (!img)
        return TSDB_ERR_NOMEM;
    fslots s = {.gen = 1, .floor_event = floor_event, .ret_fed = ret_fed, .ckpt_fno = floor_event};
    fs_fill(img, &s);
    s.gen = 0;
    fs_fill(img + FS_PAGE, &s);
    int rc = put_file(d, FEEDSLOTS_FILE ".tmp", FEEDSLOTS_FILE, img, 2 * FS_PAGE);
    free(img);
    return rc;
}

static bool fslots_intact(int dirfd)
{
    uint8_t *b;
    size_t n;
    if (read_whole(dirfd, FEEDSLOTS_FILE, &b, &n) != 0)
        return false;
    fslots s;
    bool ok = n == 2 * FS_PAGE && fs_parse(b, &s) && fs_parse(b + FS_PAGE, &s);
    free(b);
    return ok;
}

/* The engine's side: FEEDSLOTS opened for writing, the anchor chosen. At open the file is fdatasynced, so what was
 * read is durable and the chosen half is the anchor from here on. Both halves bad is damage and fails the open. */
int fslots_open(struct eng *db)
{
    if (db->fs_fd >= 0)
        return 0;
    int fd = openat(db->dirfd, FEEDSLOTS_FILE, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    uint8_t img[2 * FS_PAGE];
    size_t got = 0;
    while (got < sizeof img) {
        ssize_t r = pread(fd, img + got, sizeof img - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    fslots s;
    int which = 0;
    if (fs_pick(img, got, &s, &which, NULL) != 0) {
        close(fd);
        fprintf(stderr, "tsdb: %s/FEEDSLOTS has no valid half: the open fails (restore it from a backup, or run "
                        "`tsdb DIR feed disable` after moving it away)\n", db->dir);
        return TSDB_ERR_CORRUPT;
    }
    if (db->opt.fsync && tsdb_fdatasync(fd) != 0) {
        int e = errno;
        close(fd);
        errno = e;
        return TSDB_ERR_IO;
    }
    db->fs_fd = fd;
    db->fs_anchor = which;
    db->fs_gen = s.gen;
    db->fs_ret_fed = s.ret_fed;
    db->fs_ckpt_fno = s.ckpt_fno;
    pthread_mutex_lock(&db->feed_lock);
    db->feed_floor_event = s.floor_event;
    db->feed_floor_check = s.floor_check;
    pthread_mutex_unlock(&db->feed_lock);
    return 0;
}

void fslots_close(struct eng *db)
{
    if (db->fs_fd >= 0)
        close(db->fs_fd);
    db->fs_fd = -1;
}

/* A checkpoint with a higher floor: scratch half, fdatasync, then it is the anchor. Never writes the anchor. A failed
 * write or fdatasync is fail-stop: a retried fdatasync proves nothing. The caller is the one cleaner thread. */
int fslots_set_floor(struct eng *db, uint64_t floor_event, uint32_t floor_check)
{
    if (db->fs_fd < 0)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&db->feed_lock);
    uint64_t cur = db->feed_floor_event;
    pthread_mutex_unlock(&db->feed_lock);
    if (floor_event <= cur)
        return 0; /* the floor never goes down */
    int rc = eng_check(db);
    if (rc)
        return rc;
    uint8_t page[FS_PAGE];
    fslots s = {.gen = db->fs_gen + 1, .floor_event = floor_event, .ret_fed = db->fs_ret_fed,
                .ckpt_fno = db->fs_ckpt_fno, .floor_check = floor_check};
    fs_fill(page, &s);
    int scratch = 1 - db->fs_anchor;
    errno = 0;
    ssize_t w = pwrite(db->fs_fd, page, FS_PAGE, (off_t)scratch * FS_PAGE);
    int err = w == FS_PAGE ? 0 : (w >= 0 ? ENOSPC : errno);
    if (err == 0 && db->opt.fsync && tsdb_fdatasync(db->fs_fd) != 0)
        err = errno ? errno : EIO;
    if (err) {
        eng_fail(db, "FEEDSLOTS write", err);
        return TSDB_ERR_IO;
    }
    db->fs_anchor = scratch;
    db->fs_gen = s.gen;
    pthread_mutex_lock(&db->feed_lock);
    db->feed_floor_event = floor_event;
    db->feed_floor_check = floor_check;
    pthread_mutex_unlock(&db->feed_lock);
    return 0;
}

/* ------------------------------------------------------------ META's word */

static const char *line_end(const char *l) { const char *e = strchr(l, '\n'); return e ? e : l + strlen(l); }

void meta_requires_parse(const char *txt, bool *feed, char *unknown, size_t cap)
{
    if (feed)
        *feed = false;
    if (unknown && cap)
        unknown[0] = '\0';
    size_t un = 0;
    for (const char *l = txt; *l; l = *line_end(l) ? line_end(l) + 1 : line_end(l)) {
        if (strncmp(l, "requires=", 9) != 0)
            continue;
        const char *e = line_end(l), *w = l + 9;
        while (w < e) {
            const char *c = memchr(w, ',', (size_t)(e - w));
            const char *we = c ? c : e;
            size_t wl = (size_t)(we - w);
            if (wl == 4 && strncmp(w, "feed", 4) == 0) {
                if (feed)
                    *feed = true;
            } else if (wl && unknown && cap && un + wl + 2 < cap) {
                if (un)
                    unknown[un++] = ',';
                memcpy(unknown + un, w, wl);
                un += wl;
                unknown[un] = '\0';
            }
            w = we + (c ? 1 : 0);
        }
    }
}

/* META with the word added or removed (a requires= line with other words keeps them). tmp, fsync, rename, then the
 * directory fsync. Nothing is written when META already says it. */
static int meta_word(const feed_dir *d, bool on)
{
    uint8_t *txt;
    size_t len;
    int rc = read_whole(d->dirfd, "META", &txt, &len);
    if (rc)
        return rc;
    bool has;
    meta_requires_parse((const char *)txt, &has, NULL, 0);
    if (has == on) {
        free(txt);
        return 0;
    }
    char unknown[96];
    meta_requires_parse((const char *)txt, NULL, unknown, sizeof unknown);
    char *out = malloc(len + 64 + sizeof unknown);
    if (!out) {
        free(txt);
        return TSDB_ERR_NOMEM;
    }
    size_t n = 0;
    for (const char *l = (const char *)txt; *l;) {
        const char *e = line_end(l);
        size_t ll = (size_t)(e - l) + (*e ? 1 : 0);
        if (strncmp(l, "requires=", 9) != 0) {
            memcpy(out + n, l, ll);
            n += ll;
            if (!*e)
                out[n++] = '\n';
        }
        l += ll;
    }
    free(txt);
    if (on || unknown[0])
        n += (size_t)snprintf(out + n, 64 + sizeof unknown, "requires=%s%s%s\n", unknown, unknown[0] && on ? "," : "",
                              on ? "feed" : "");
    char tmp[128];
    meta_tmp_make(tmp, sizeof tmp);
    rc = put_file(d, tmp, "META", out, n);
    free(out);
    return rc ? rc : dsync(d);
}

/* ---------------------------------------------------------- directory scan */

/* Removes the files of the feed that a state needs gone: fwal_ segments, FEEDSLOTS and its tmp, FEED's tmp, and with
 * all, FEED. *removed: something was unlinked. */
static int scrub(const feed_dir *d, bool all, bool *removed)
{
    int fd = dup(d->dirfd);
    DIR *dir = fd >= 0 ? fdopendir(fd) : NULL;
    if (!dir) {
        if (fd >= 0)
            close(fd);
        return TSDB_ERR_IO;
    }
    rewinddir(dir); /* the dup shares its offset with dirfd */
    int rc = 0;
    struct dirent *e;
    for (errno = 0; (e = readdir(dir)); errno = 0) {
        const char *nm = e->d_name;
        bool kill = fwal_file_name(nm) || strcmp(nm, FEEDSLOTS_FILE) == 0 || strcmp(nm, FEEDSLOTS_FILE ".tmp") == 0 ||
                    strcmp(nm, FEED_FILE ".tmp") == 0 || (all && strcmp(nm, FEED_FILE) == 0);
        if (!kill)
            continue;
        if (unlinkat(d->dirfd, nm, 0) != 0 && errno != ENOENT) {
            fprintf(stderr, "tsdb: removing %s failed: %s\n", nm, strerror(errno));
            rc = TSDB_ERR_IO;
            break;
        }
        *removed = true;
    }
    if (rc == 0 && errno != 0)
        rc = TSDB_ERR_IO; /* a read error is not the end of the directory */
    closedir(dir);
    return rc;
}

int feed_dir_state(int dirfd, feed_state *s)
{
    memset(s, 0, sizeof *s);
    uint8_t *txt;
    size_t len;
    int rc = read_whole(dirfd, "META", &txt, &len);
    if (rc == 0) {
        meta_requires_parse((const char *)txt, &s->word, NULL, 0);
        free(txt);
    } else if (rc != TSDB_ERR_NOTFOUND) {
        return rc;
    }
    s->feed = faccessat(dirfd, FEED_FILE, F_OK, AT_SYMLINK_NOFOLLOW) == 0;
    s->feedslots = faccessat(dirfd, FEEDSLOTS_FILE, F_OK, AT_SYMLINK_NOFOLLOW) == 0;
    int fd = dup(dirfd);
    DIR *dir = fd >= 0 ? fdopendir(fd) : NULL;
    if (!dir) {
        if (fd >= 0)
            close(fd);
        return TSDB_ERR_IO;
    }
    rewinddir(dir);
    struct dirent *e;
    for (errno = 0; (e = readdir(dir)); errno = 0) {
        if (fwal_file_name(e->d_name))
            s->any_fwal = true;
        else if (strcmp(e->d_name, FEEDSLOTS_FILE ".tmp") == 0 || strcmp(e->d_name, FEED_FILE ".tmp") == 0)
            s->junk = true;
    }
    rc = errno ? TSDB_ERR_IO : 0;
    closedir(dir);
    return rc;
}

int feed_scan_retained(int dirfd, uint64_t *segments, uint64_t *bytes)
{
    *segments = *bytes = 0;
    int fd = dup(dirfd);
    DIR *dir = fd >= 0 ? fdopendir(fd) : NULL;
    if (!dir) {
        if (fd >= 0)
            close(fd);
        return TSDB_ERR_IO;
    }
    rewinddir(dir);
    struct dirent *e;
    for (errno = 0; (e = readdir(dir)); errno = 0) {
        struct stat st;
        if (fwal_file_name(e->d_name) && fstatat(dirfd, e->d_name, &st, 0) == 0) {
            (*segments)++;
            *bytes += (uint64_t)st.st_size;
        }
    }
    int rc = errno ? TSDB_ERR_IO : 0;
    closedir(dir);
    return rc;
}

/* --------------------------------------------------------- enable, disable */

int feed_dir_enable(const feed_dir *d, const feed_cfg *cfg, uint64_t floor_event)
{
    feed_cfg have;
    feed_state st;
    int rc = feed_dir_state(d->dirfd, &st);
    if (rc)
        return rc;
    /* already on and whole: nothing to do (an add that retries, a repeated enable) */
    if (st.word && st.feed && feed_cfg_read(d->dirfd, &have) == 0 && fslots_intact(d->dirfd))
        return 0;
    /* leftovers of an interrupted disable (the CLI works on a closed directory: no open finished it) would join the
     * new feed's history across a gap: removed, durably, before anything is made */
    bool removed = false;
    if ((rc = scrub(d, true, &removed)) != 0)
        return rc;
    if (removed && (rc = dsync(d)) != 0)
        return rc;
    if ((rc = meta_word(d, true)) != 0) /* the word first: FEED implies it */
        return rc;
    struct stat sb;
    uint64_t ret_size = fstatat(d->dirfd, "RETENTION", &sb, 0) == 0 ? (uint64_t)sb.st_size : 0;
    if ((rc = fslots_make(d, floor_event, ret_size)) != 0 || (rc = dsync(d)) != 0)
        return rc;
    if ((rc = feed_cfg_write(d, cfg)) != 0) /* FEED last: it is "on" */
        return rc;
    return dsync(d);
}

int feed_dir_disable(const feed_dir *d)
{
    /* FEED first, and its removal durable before anything else goes: a crash otherwise leaves FEED with a gap in the
     * fwal_ files, or FEED without FEEDSLOTS */
    int rc = 0;
    bool removed = false;
    if (unlinkat(d->dirfd, FEED_FILE, 0) != 0 && errno != ENOENT)
        return TSDB_ERR_IO;
    if (unlinkat(d->dirfd, FEED_FILE ".tmp", 0) != 0 && errno != ENOENT)
        return TSDB_ERR_IO;
    if ((rc = dsync(d)) != 0)
        return rc;
    if ((rc = scrub(d, false, &removed)) != 0 || (rc = dsync(d)) != 0)
        return rc;
    return meta_word(d, false);
}

int feed_dir_new_id(const feed_dir *d, feed_cfg *cfg)
{
    feed_cfg fresh;
    feed_cfg_defaults(&fresh, NULL);
    cfg->feed_id = fresh.feed_id;
    cfg->created_ms = fresh.created_ms;
    int rc = feed_cfg_write(d, cfg);
    return rc ? rc : dsync(d);
}
