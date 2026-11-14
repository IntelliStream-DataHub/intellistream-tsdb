/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The REPL file (docs/design-cluster.md 4.3; the scheme is FNO's, src/fno.c). See replfile.h for the contract.
 *
 * A slot is one 4 KiB page:
 *   head (128 bytes): magic, generation, len, nhist, the two uuids, node, incarnation, rid, open_gen, fence, led, floor,
 *                     log_start; then nhist entries of (epoch, start), then an xxh64 of everything before it.
 * `len` is the byte count up to and including the checksum; the rest of the page is zero and not read. A slot whose
 * checksum is right but whose content cannot be (rid 0, a history out of order) is as bad as a torn one: the decoder
 * the core feeds must never see it.
 */
#include "internal.h"
#include "replfile.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define RF_MAGIC 0x31504552425f5344ull /* "DS_BREP1" little endian: a tag, never read as text */
#define RF_HEAD 128u
#define RF_FILE (2 * REPLFILE_PAGE)

int replfile_test_mutant;
int replfile_test_eio_read;
int replfile_test_nomem;

struct replfile {
    int fd;
    int slot;                     /* the slot that holds the newest state */
    uint64_t gen;
    bool failed;
    struct replfile_state cur;    /* what the file holds (hist unused): the identity and the floors saves are held to */
};

static void put64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }
static uint64_t get64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }
static void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static uint32_t get32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }

static bool hist_ok(const struct repl_epoch *h, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++)
        if (h[i].start == 0 || (i && (h[i].epoch <= h[i - 1].epoch || h[i].start <= h[i - 1].start)))
            return false;
    return true;
}

/* The oldest entry worth keeping when the history has to fit: an entry goes when the next one starts at or below
 * log_start - 1, because the epoch of position log_start - 1 (the snapshot's last position, repl.h) stays in force
 * there and is the one the core needs; only epochs wholly below that position go. */
static uint32_t hist_first(const struct replfile_state *st)
{
    uint32_t first = 0;
    uint64_t ls = st->log_start ? st->log_start : 1;
    if (!st->hist)
        return 0;
    while (st->nhist - first > REPLFILE_MAX_HIST && first + 1 < st->nhist && st->hist[first + 1].start < ls)
        first++;
    return first;
}

int replfile_encode(uint8_t *page, const struct replfile_state *st, uint64_t gen)
{
    if (st->rid == 0 || (st->nhist && !st->hist) || !hist_ok(st->hist, st->nhist) || st->nhist > REPLFILE_MAX_HIST)
        return TSDB_ERR_INVALID;
    uint32_t len = RF_HEAD + st->nhist * 16 + 8;
    memset(page, 0, REPLFILE_PAGE);
    put64(page, RF_MAGIC);
    put64(page + 8, gen);
    put32(page + 16, len);
    put32(page + 20, st->nhist);
    memcpy(page + 24, st->volume_uuid, 16);
    memcpy(page + 40, st->cluster_uuid, 16);
    put32(page + 56, st->node);
    put64(page + 64, st->incarnation);
    put64(page + 72, st->rid);
    put64(page + 80, st->open_gen);
    put64(page + 88, st->fence);
    put64(page + 96, st->led);
    put64(page + 104, st->floor);
    put64(page + 112, st->log_start);
    for (uint32_t i = 0; i < st->nhist; i++) {
        put64(page + RF_HEAD + i * 16, st->hist[i].epoch);
        put64(page + RF_HEAD + i * 16 + 8, st->hist[i].start);
    }
    put64(page + len - 8, xxh64(page, len - 8, 0));
    return 0;
}

enum { PARSE_OK, PARSE_BAD, PARSE_NOMEM };

/* A failed allocation is not damage: it must never make a good slot look bad (the other, older one would win). */
static int parse_slot(const uint8_t *page, size_t n, struct replfile_state *st, uint64_t *gen)
{
    if (n < RF_HEAD + 8 || n > REPLFILE_PAGE || get64(page) != RF_MAGIC)
        return PARSE_BAD;
    uint32_t len = get32(page + 16), nh = get32(page + 20);
    if (nh > REPLFILE_MAX_HIST || len != RF_HEAD + nh * 16 + 8 || len > n || get32(page + 60) != 0 ||
        get64(page + 120) != 0)
        return PARSE_BAD;
    if (replfile_test_mutant != REPLFILE_MUT_NOCHECKSUM && get64(page + len - 8) != xxh64(page, len - 8, 0))
        return PARSE_BAD;
    struct replfile_state s;
    memset(&s, 0, sizeof s);
    memcpy(s.volume_uuid, page + 24, 16);
    memcpy(s.cluster_uuid, page + 40, 16);
    s.node = get32(page + 56);
    s.incarnation = get64(page + 64);
    s.rid = get64(page + 72);
    s.open_gen = get64(page + 80);
    s.fence = get64(page + 88);
    s.led = get64(page + 96);
    s.floor = get64(page + 104);
    s.log_start = get64(page + 112);
    if (s.rid == 0)
        return PARSE_BAD;
    if (nh) {
        s.hist = replfile_test_nomem ? NULL : malloc(nh * sizeof *s.hist);
        if (!s.hist)
            return PARSE_NOMEM;
        for (uint32_t i = 0; i < nh; i++) {
            s.hist[i].epoch = get64(page + RF_HEAD + i * 16);
            s.hist[i].start = get64(page + RF_HEAD + i * 16 + 8);
        }
        s.nhist = nh;
        if (!hist_ok(s.hist, nh)) {
            free(s.hist);
            return PARSE_BAD;
        }
    }
    *st = s;
    *gen = get64(page + 8);
    return PARSE_OK;
}

bool replfile_parse(const uint8_t *page, size_t n, struct replfile_state *st, uint64_t *gen)
{
    return parse_slot(page, n, st, gen) == PARSE_OK;
}

void replfile_state_free(struct replfile_state *st)
{
    free(st->hist);
    st->hist = NULL;
    st->nhist = 0;
}

uint64_t replfile_new_rid(void)
{
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return 0;
    uint64_t v = 0;
    for (int tries = 0; tries < 8 && v == 0; tries++) {
        size_t got = 0;
        while (got < sizeof v) {
            ssize_t r = read(fd, (char *)&v + got, sizeof v - got);
            if (r < 0 && errno == EINTR)
                continue;
            if (r <= 0) {
                close(fd);
                return 0;
            }
            got += (size_t)r;
        }
    }
    close(fd);
    return v;
}

static int rf_pwrite(int fd, const void *buf, size_t n, off_t off)
{
    const uint8_t *p = buf;
    while (n) {
        ssize_t w = pwrite(fd, p, n, off);
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0)
            return errno;
        if (w == 0)
            return ENOSPC;
        p += w, n -= (size_t)w, off += w;
    }
    return 0;
}

static struct replfile *handle(int fd, int slot, uint64_t gen, const struct replfile_state *st)
{
    struct replfile *rf = calloc(1, sizeof *rf);
    if (!rf)
        return NULL;
    rf->fd = fd, rf->slot = slot, rf->gen = gen;
    rf->cur = *st;
    rf->cur.hist = NULL, rf->cur.nhist = 0;
    return rf;
}

int replfile_create(int dirfd, struct replfile_state *st, bool sync, struct replfile **out)
{
    *out = NULL;
    if (faccessat(dirfd, REPLFILE_NAME, F_OK, 0) == 0)
        return TSDB_ERR_INVALID;
    if (st->rid == 0 && (st->rid = replfile_new_rid()) == 0)
        return TSDB_ERR_IO;
    uint8_t *pages = calloc(2, REPLFILE_PAGE);
    if (!pages)
        return TSDB_ERR_NOMEM;
    int rc = replfile_encode(pages, st, 1);
    if (rc == 0)
        rc = replfile_encode(pages + REPLFILE_PAGE, st, 0);
    if (rc != 0) {
        free(pages);
        return rc;
    }
    int fd = openat(dirfd, REPLFILE_NAME ".tmp", O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        free(pages);
        return TSDB_ERR_IO;
    }
    bool ok = rf_pwrite(fd, pages, RF_FILE, 0) == 0 &&
              (!sync || replfile_test_mutant == REPLFILE_MUT_NOSYNC || tsdb_fsync(fd) == 0);
    free(pages);
    if (ok && tsdb_rename_noreplace(dirfd, REPLFILE_NAME ".tmp", REPLFILE_NAME) != 0) {
        int e = errno;
        close(fd);
        return e == EEXIST ? TSDB_ERR_INVALID : TSDB_ERR_IO;
    }
    if (!ok ||
        (sync && replfile_test_mutant != REPLFILE_MUT_NOSYNC && tsdb_fsync(dirfd) != 0)) {
        close(fd);
        return TSDB_ERR_IO;
    }
    /* the handle's descriptor is one opened by the final name (as a load's is), not the one of the tmp file */
    close(fd);
    fd = openat(dirfd, REPLFILE_NAME, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    *out = handle(fd, 0, 1, st);
    if (!*out) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    return 0;
}

int replfile_pick(const uint8_t *pages, size_t got, struct replfile_state *st, uint64_t *gen, int *slot, bool *torn)
{
    struct replfile_state s[2];
    uint64_t g[2];
    int rc[2];
    for (int i = 0; i < 2; i++) {
        size_t off = (size_t)i * REPLFILE_PAGE;
        size_t n = got > off ? (got - off < REPLFILE_PAGE ? got - off : REPLFILE_PAGE) : 0; /* a short file: bad */
        rc[i] = parse_slot(pages + off, n, &s[i], &g[i]);
    }
    bool ok[2] = {rc[0] == PARSE_OK, rc[1] == PARSE_OK};
    if (rc[0] == PARSE_NOMEM || rc[1] == PARSE_NOMEM) {
        for (int i = 0; i < 2; i++)
            if (ok[i])
                replfile_state_free(&s[i]);
        return TSDB_ERR_NOMEM;
    }
    if (!ok[0] && !ok[1])
        return TSDB_ERR_CORRUPT;
    int best = !ok[0] || (ok[1] && g[1] > g[0]) ? 1 : 0;
    *st = s[best];
    *gen = g[best];
    *slot = best;
    *torn = !(ok[0] && ok[1]);
    for (int i = 0; i < 2; i++)
        if (ok[i] && i != best)
            replfile_state_free(&s[i]);
    return 0;
}

int replfile_load(int dirfd, struct replfile **out, struct replfile_state *st, bool *torn)
{
    *out = NULL;
    int fd = openat(dirfd, REPLFILE_NAME, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    uint8_t pages[RF_FILE];
    size_t got = 0;
    while (got < sizeof pages) {
        ssize_t r = replfile_test_eio_read ? (errno = EIO, -1) : pread(fd, pages + got, sizeof pages - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            close(fd);
            return TSDB_ERR_IO; /* a read error is never "the slot is bad" */
        }
        if (r == 0)
            break;
        got += (size_t)r;
    }
    uint64_t gen;
    int slot;
    bool t;
    int rc = replfile_pick(pages, got, st, &gen, &slot, &t);
    if (rc == TSDB_ERR_NOMEM) {
        close(fd);
        return rc;
    }
    if (rc != 0) {
        close(fd);
        fprintf(stderr, "tsdb: REPL has no valid slot: the replica's fence, rid and history are lost, the volume does "
                        "not open (rebuild the replica on a new directory)\n");
        return rc;
    }
    if (t) {
        fprintf(stderr, "tsdb: REPL slot %d is damaged, using slot %d (generation %llu): the state may be older than "
                        "the last acknowledged save\n", 1 - slot, slot, (unsigned long long)gen);
        /* rot of the newest slot looks like a torn write, and the lost save may have carried an open generation that
         * is registered already: skip a belt ahead, as FNO does, so the next registration is higher whatever was lost */
        st->open_gen += REPLFILE_OPEN_GEN_BELT;
    }
    struct replfile *rf = handle(fd, slot, gen, st);
    if (!rf) {
        close(fd);
        replfile_state_free(st);
        return TSDB_ERR_NOMEM;
    }
    *out = rf;
    if (torn)
        *torn = t;
    return 0;
}

int replfile_save(struct replfile *rf, const struct replfile_state *st)
{
    if (rf->failed)
        return TSDB_ERR_IO;
    const struct replfile_state *c = &rf->cur;
    if (memcmp(st->volume_uuid, c->volume_uuid, 16) != 0 || memcmp(st->cluster_uuid, c->cluster_uuid, 16) != 0 ||
        st->node != c->node || st->rid != c->rid || st->open_gen < c->open_gen || st->fence < c->fence ||
        st->led < c->led || st->floor < c->floor || st->log_start < c->log_start)
        return TSDB_ERR_INVALID;
    /* the state's own invariants: a caller's bug must fail here, not at the core's next open */
    if (st->led > st->fence || (st->log_start > 1 && st->log_start - 1 > st->floor) ||
        (st->nhist && st->hist && st->hist[st->nhist - 1].epoch > st->fence))
        return TSDB_ERR_INVALID;
    struct replfile_state w = *st;
    uint32_t first = hist_first(st);
    w.hist = st->hist ? st->hist + first : NULL;
    w.nhist = st->nhist - first;
    uint8_t page[REPLFILE_PAGE];
    int target = replfile_test_mutant == REPLFILE_MUT_SAMESLOT ? rf->slot : 1 - rf->slot; /* the older generation */
    if (replfile_encode(page, &w, rf->gen + 1) != 0)
        return TSDB_ERR_INVALID;
    int err = rf_pwrite(rf->fd, page, REPLFILE_PAGE, (off_t)target * REPLFILE_PAGE);
    if (err == 0 && replfile_test_mutant != REPLFILE_MUT_NOSYNC && tsdb_fdatasync(rf->fd) != 0)
        err = errno ? errno : EIO;
    if (err) {
        rf->failed = true; /* fail-stop: nothing about this file is known any more, and a retried sync proves nothing */
        fprintf(stderr, "tsdb: REPL write failed (%s): the volume fails\n", strerror(err));
        return TSDB_ERR_IO;
    }
    rf->slot = target;
    rf->gen++;
    rf->cur = *st;
    rf->cur.hist = NULL, rf->cur.nhist = 0;
    return 0;
}

uint64_t replfile_gen(const struct replfile *rf) { return rf->gen; }
bool replfile_failed(const struct replfile *rf) { return rf->failed; }

void replfile_close(struct replfile *rf)
{
    if (!rf)
        return;
    if (rf->fd >= 0)
        close(rf->fd);
    free(rf);
}

void replfile_to_durable(const struct replfile_state *st, uint64_t log_end, struct repl_durable *d)
{
    memset(d, 0, sizeof *d);
    d->rid = st->rid;
    d->fence = st->fence;
    d->led = st->led;
    d->floor = st->floor;
    d->log_start = st->log_start;
    d->log_end = log_end;
    d->hist = st->hist;
    /* the core fails an open whose history starts beyond the log's end: an epoch saved before its marker reached the
     * log (power loss in between) is dropped here, as the core's own cut drops it (repl.c) */
    uint32_t n = st->nhist;
    while (n && st->hist[n - 1].start > log_end)
        n--;
    d->nhist = n;
}

int replfile_from_durable(struct replfile_state *st, const struct repl_durable *d)
{
    struct repl_epoch *h = NULL;
    if (d->nhist) {
        h = malloc(d->nhist * sizeof *h);
        if (!h)
            return TSDB_ERR_NOMEM;
        memcpy(h, d->hist, d->nhist * sizeof *h);
    }
    free(st->hist);
    st->hist = h;
    st->nhist = d->nhist;
    st->rid = d->rid;
    st->fence = d->fence;
    st->led = d->led;
    st->floor = d->floor;
    st->log_start = d->log_start;
    return 0;
}
