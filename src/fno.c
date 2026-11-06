/*
 * FNO: the durable bound of the WAL record numbers (`fno`, design-cdc.md 2.1), one file per volume.
 *
 * Numbers never go back across opens, whatever was removed in between: a segment unlinked after its flush, a
 * part retired by retention, an empty merge output dropped. The bound cannot live in MANIFEST: it is one copy
 * rewritten in place without a sync, so a power loss during any later rewrite tears it, and the writer would
 * then either fail the open or lose the bound. FNO is two slots, each on a page of its own (a torn write
 * reaches one page), each holding magic, `fno_next`, a generation and a checksum. A store overwrites the slot
 * with the older generation and syncs it; a slot torn by a power loss leaves the other, which holds the value
 * before it, and nothing relied on the newer value before its sync. The file is made at its full size, so a
 * store never allocates.
 *
 * Who needs what: before a WAL segment is unlinked (after its flush, after its replay at open, the empty
 * segment at close) FNO must hold at least the number that ends that segment (fno_require). At a clean close
 * it holds the exact next number. At open the next number is the largest of the valid slot, the end of every
 * valid record in every segment and 1 + the largest fno_hi of a part (never alone: a part range covers rows,
 * not markers). One bad slot is a torn store; both bad, or no file in a format-9 volume, fails the open.
 * Followers never read FNO. A failed store is fail-stop, ENOSPC too (a retried sync proves nothing).
 */
#include "internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>

#define FNO_PAGE 4096
#define FNO_MAGIC 0x314f4e4642445354ull /* "TSDBFNO1" little endian */

typedef struct fno_slot {
    uint64_t magic, next, gen, hash;
} fno_slot;

static void slot_fill(uint8_t *page, uint64_t next, uint64_t gen)
{
    fno_slot s = {FNO_MAGIC, next, gen, 0};
    s.hash = xxh64(&s, offsetof(fno_slot, hash), 0);
    memset(page, 0, FNO_PAGE);
    memcpy(page, &s, sizeof s);
}

static bool slot_parse(const uint8_t *page, uint64_t *next, uint64_t *gen)
{
    fno_slot s;
    memcpy(&s, page, sizeof s);
    if (s.magic != FNO_MAGIC || s.hash != xxh64(&s, offsetof(fno_slot, hash), 0) || s.next == 0)
        return false;
    *next = s.next;
    *gen = s.gen;
    return true;
}

/* Makes FNO in the directory (new database, new volume, upgrade) with both slots valid and `next` in them,
 * synced with its directory when sync is set. An existing file is left as it is. */
int fno_create(int dirfd, uint64_t next, bool sync)
{
    int fd = openat(dirfd, FNO_FILE ".tmp", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;
    uint8_t *pages = calloc(2, FNO_PAGE);
    if (!pages) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    slot_fill(pages, next, 1);
    slot_fill(pages + FNO_PAGE, next, 0);
    bool ok = pwrite(fd, pages, 2 * FNO_PAGE, 0) == 2 * FNO_PAGE && (!sync || tsdb_fsync(fd) == 0);
    free(pages);
    close(fd);
    if (!ok || renameat(dirfd, FNO_FILE ".tmp", dirfd, FNO_FILE) != 0)
        return TSDB_ERR_IO;
    return !sync || tsdb_fsync(dirfd) == 0 ? 0 : TSDB_ERR_IO;
}

/* Whether the directory has an FNO file whose two slots are both valid (an upgrade that died after making it, and
 * runs again, keeps it; any other file is made anew). */
bool fno_file_intact(int dirfd)
{
    uint8_t pages[2 * FNO_PAGE];
    int fd = openat(dirfd, FNO_FILE, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return false;
    ssize_t r = pread(fd, pages, sizeof pages, 0);
    close(fd);
    uint64_t n, g;
    return r == (ssize_t)sizeof pages && slot_parse(pages, &n, &g) && slot_parse(pages + FNO_PAGE, &n, &g);
}

/* Reads FNO: *next is the valid slot with the higher generation. TSDB_ERR_NOTFOUND: no file. TSDB_ERR_CORRUPT:
 * both slots bad (logged). *torn: one slot was bad, the other valid. */
int fno_load(eng *db, uint64_t *next, uint64_t *gen, int *slot, bool *torn)
{
    uint8_t pages[2 * FNO_PAGE];
    int fd = openat(db->dirfd, FNO_FILE, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    size_t got = 0;
    while (got < sizeof pages) {
        ssize_t r = pread(fd, pages + got, sizeof pages - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    if (got < sizeof pages)
        memset(pages + got, 0, sizeof pages - got); /* a short file: the slots it lacks are bad */
    uint64_t n[2], g[2];
    bool ok[2] = {slot_parse(pages, &n[0], &g[0]), slot_parse(pages + FNO_PAGE, &n[1], &g[1])};
    if (!ok[0] && !ok[1]) {
        close(fd);
        fprintf(stderr, "tsdb: FNO has no valid slot: the numbers of the log cannot be bounded, the open fails "
                        "(restore FNO from a backup, or export the data with a reader)\n");
        return TSDB_ERR_CORRUPT;
    }
    int best = !ok[0] || (ok[1] && g[1] > g[0]) ? 1 : 0;
    *next = n[best];
    *gen = g[best];
    *slot = best;
    *torn = !(ok[0] && ok[1]);
    if (db->fno_fd >= 0)
        close(db->fno_fd);
    db->fno_fd = fd;
    return 0;
}

/* FNO holds at least `v` once this returns 0. A failure fails the engine. */
int fno_require(eng *db, uint64_t v)
{
    if (db->fno_fd < 0 || v <= atomic_load_explicit(&db->fno_durable, memory_order_acquire))
        return 0;
    pthread_mutex_lock(&db->fno_lock);
    int rc = 0;
    if (v > db->fno_durable) {
        uint8_t page[FNO_PAGE];
        int target = 1 - db->fno_slot; /* the older generation */
        slot_fill(page, v, db->fno_gen + 1);
        errno = 0;
        ssize_t w = pwrite(db->fno_fd, page, FNO_PAGE, (off_t)target * FNO_PAGE);
        int err = w == FNO_PAGE ? 0 : (w >= 0 ? ENOSPC : errno);
        if (err == 0 && db->opt.fsync && tsdb_fdatasync(db->fno_fd) != 0)
            err = errno ? errno : EIO;
        if (err) {
            eng_fail(db, "FNO write", err);
            rc = TSDB_ERR_IO;
        } else {
            db->fno_slot = target;
            db->fno_gen++;
            atomic_store_explicit(&db->fno_durable, v, memory_order_release);
            atomic_fetch_add_explicit(&db->st_fno_syncs, 1, memory_order_relaxed);
        }
    }
    pthread_mutex_unlock(&db->fno_lock);
    return rc;
}
