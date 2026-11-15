/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The REPL file of a replica directory (src/replfile.c, docs/design-cluster.md 4.3). Tests:
 *  - create makes both slots and draws a rid that is never 0; saves alternate the two slots, and a load gives what
 *    the last save stored (identity, rid, open generation, fence, led, floor, log start, history);
 *  - a torn or damaged newest slot (a flipped bit, garbage, a short file) falls back to the older state, logged and
 *    reported through *torn, and the next save repairs it; both slots bad is CORRUPT; a missing file is NOTFOUND; a
 *    slot with a good checksum and an impossible content (rid 0, a history out of order) is bad too;
 *  - a save that changes the identity or sends a value back is refused with nothing written; a history over a slot
 *    drops its entries wholly below the log start, and is refused when that is not enough;
 *  - a write that fails (EFBIG, a short write) fails the handle for good: no retry, the old state is what the file holds;
 *  - the conversions to and from struct repl_durable;
 *  - power lost at every step of create (with the caller's rename of its tmp directory) and of every save (crashfs,
 *    unsynced data torn): the state found is the last one a save acknowledged or the one in flight, whole, never a mix.
 *
 *   ./build/test_replfile [scratch dir]
 *   ./build/test_replfile --mutant nosync|sameslot|nochecksum [scratch dir]    must fail
 *   ./build/test_replfile --child DIR ACKFILE      (internal: a process the shim kills)
 */
#define _GNU_SOURCE
#include "internal.h"
#include "replfile.h"

#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
static int saved_err = -1; /* the real stderr while quiet() has fd 2 pointed at /dev/null */
static void fail_at(const char *file, int line, const char *what)
{
    char buf[512];
    int n = snprintf(buf, sizeof buf, "%s:%d: CHECK failed: %s\n", file, line, what);
    if (write(saved_err >= 0 ? saved_err : 2, buf, (size_t)n) < 0)
        failures++;
    failures++;
}
#define CHECK(cond)                                     \
    do {                                                \
        if (!(cond))                                    \
            fail_at(__FILE__, __LINE__, #cond);         \
    } while (0)

static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

/* the log lines of damaged slots are expected: keep them out of the output */
static void quiet(bool on)
{
    fflush(stderr);
    if (on) {
        saved_err = dup(2);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 2);
            close(null);
        }
    } else if (saved_err >= 0) {
        dup2(saved_err, 2);
        close(saved_err);
        saved_err = -1;
    }
}

/* The state after save k (k = 0: as created). Every field moves, so a mixed slot shows. The power-loss runs scale the
 * history up (hist_scale) so that a slot spans many 512-byte sectors and a torn in-place rewrite mixes old and new. */
static int hist_scale = 1;
static void state_of(struct replfile_state *st, int k)
{
    int nh = k * hist_scale;
    memset(st, 0, sizeof *st);
    for (int i = 0; i < 16; i++)
        st->volume_uuid[i] = (uint8_t)(0x10 + i), st->cluster_uuid[i] = (uint8_t)(0xa0 + i);
    st->node = 7;
    st->incarnation = 100 + (uint64_t)k / 4;
    st->rid = 0x1234567890abcdefull;
    st->open_gen = 1 + (uint64_t)k / 3;
    st->fence = (uint64_t)k * (uint64_t)hist_scale; /* the history's epochs are 1..k*scale and never above the fence */
    st->led = (uint64_t)k / 2;
    st->floor = 3 * (uint64_t)k;
    st->log_start = 1 + (uint64_t)k / 4;
    st->nhist = (uint32_t)nh;
    st->hist = nh ? calloc((size_t)nh, sizeof *st->hist) : NULL;
    for (int i = 0; i < nh; i++)
        st->hist[i] = (struct repl_epoch){.epoch = (uint64_t)i + 1, .start = 5 * (uint64_t)i + 1};
}

static bool same(const struct replfile_state *a, const struct replfile_state *b)
{
    return memcmp(a->volume_uuid, b->volume_uuid, 16) == 0 && memcmp(a->cluster_uuid, b->cluster_uuid, 16) == 0 &&
           a->node == b->node && a->incarnation == b->incarnation && a->rid == b->rid &&
           /* a torn load raised it by a belt */
           a->open_gen % REPLFILE_OPEN_GEN_BELT == b->open_gen % REPLFILE_OPEN_GEN_BELT &&
           a->fence == b->fence && a->led == b->led && a->floor == b->floor && a->log_start == b->log_start &&
           a->nhist == b->nhist && (a->nhist == 0 || memcmp(a->hist, b->hist, a->nhist * sizeof *a->hist) == 0);
}

static bool is_state(const struct replfile_state *got, int k)
{
    struct replfile_state e;
    state_of(&e, k);
    bool r = same(got, &e);
    replfile_state_free(&e);
    return r;
}

static void fresh(const char *name, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, name);
    sh("rm -rf '%s'; mkdir -p '%s'", dir, dir);
}

static int open_dir(const char *dir)
{
    int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(fd >= 0);
    return fd;
}

static uint64_t raw_gen(const char *dir, int slot)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/REPL", dir);
    int fd = open(p, O_RDONLY);
    uint8_t b[24];
    uint64_t g = UINT64_MAX;
    if (fd >= 0 && pread(fd, b, sizeof b, (off_t)slot * REPLFILE_PAGE) == (ssize_t)sizeof b)
        memcpy(&g, b + 8, 8);
    if (fd >= 0)
        close(fd);
    return g;
}

/* overwrites bytes of the file at an offset (a damaged slot) */
static void poke(const char *dir, off_t off, const void *buf, size_t n)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/REPL", dir);
    int fd = open(p, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, buf, n, off) == (ssize_t)n);
    if (fd >= 0)
        close(fd);
}

static void flip(const char *dir, off_t off)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/REPL", dir);
    int fd = open(p, O_RDWR);
    uint8_t b = 0;
    CHECK(fd >= 0 && pread(fd, &b, 1, off) == 1);
    b ^= 0x10;
    CHECK(fd >= 0 && pwrite(fd, &b, 1, off) == 1);
    if (fd >= 0)
        close(fd);
}

/* a whole directory with the state of saves 0..n, the handle closed */
static void make(const char *dir, int n)
{
    struct replfile_state st;
    state_of(&st, 0);
    int dfd = open_dir(dir);
    struct replfile *rf;
    CHECK(replfile_create(dfd, &st, true, &rf) == 0);
    replfile_state_free(&st);
    for (int k = 1; k <= n && rf; k++) {
        state_of(&st, k);
        CHECK(replfile_save(rf, &st) == 0);
        replfile_state_free(&st);
    }
    replfile_close(rf);
    close(dfd);
}

static int load_k(const char *dir, struct replfile **rf, struct replfile_state *st, bool *torn)
{
    int dfd = open_dir(dir);
    int rc = replfile_load(dfd, rf, st, torn);
    close(dfd);
    return rc;
}

/* ------------------------------------------------------------ in-process tests */

static void test_basic(void)
{
    char dir[PATH_MAX + 32];
    fresh("basic", dir, sizeof dir);
    int dfd = open_dir(dir);
    struct replfile_state st;
    state_of(&st, 0);
    st.rid = 0; /* drawn */
    struct replfile *rf;
    CHECK(replfile_create(dfd, &st, true, &rf) == 0 && rf);
    CHECK(st.rid != 0);
    uint64_t rid = st.rid;
    CHECK(replfile_gen(rf) == 1 && raw_gen(dir, 0) == 1 && raw_gen(dir, 1) == 0);
    struct replfile *dup;
    CHECK(replfile_create(dfd, &st, true, &dup) == TSDB_ERR_INVALID); /* one rid for the life of a directory */
    struct stat sb;
    CHECK(fstatat(dfd, "REPL", &sb, 0) == 0 && sb.st_size == 2 * REPLFILE_PAGE);
    CHECK(fstatat(dfd, "REPL.tmp", &sb, 0) != 0);
    replfile_state_free(&st);
    /* the saves alternate the slots */
    for (int k = 1; k <= 6; k++) {
        state_of(&st, k);
        st.rid = rid;
        CHECK(replfile_save(rf, &st) == 0);
        replfile_state_free(&st);
        int newer = k % 2; /* create left generation 1 in slot 0: save 1 goes to slot 1, save 2 to slot 0 ... */
        CHECK(raw_gen(dir, newer) == (uint64_t)k + 1);
        CHECK(raw_gen(dir, 1 - newer) == (uint64_t)k);
    }
    replfile_close(rf);
    bool torn = true;
    struct replfile_state got;
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn);
    state_of(&st, 6);
    st.rid = rid;
    CHECK(same(&got, &st));
    CHECK(replfile_gen(rf) == 7);
    replfile_state_free(&st);
    replfile_state_free(&got);
    /* a load, then a save from the new handle goes to the other slot */
    state_of(&st, 7);
    st.rid = rid;
    CHECK(replfile_save(rf, &st) == 0 && raw_gen(dir, 1) == 8);
    replfile_state_free(&st);
    replfile_close(rf);
    close(dfd);
    fresh("empty", dir, sizeof dir);
    CHECK(load_k(dir, &rf, &got, NULL) == TSDB_ERR_NOTFOUND);
    printf("  create, alternating slots, load\n");
}

static void test_damage(void)
{
    char dir[PATH_MAX + 32];
    struct replfile *rf;
    struct replfile_state got;
    bool torn;
    quiet(true);
    /* saves 0..4: save k is in slot k % 2, so the newest (4) is in slot 0 and save 3 in slot 1 */
    fresh("dmg", dir, sizeof dir);
    make(dir, 4);
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn && is_state(&got, 4));
    replfile_state_free(&got);
    replfile_close(rf);
    /* a bit flipped in a payload field of the newest slot (fence): the checksum finds it */
    flip(dir, 88);
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && torn && is_state(&got, 3));
    replfile_state_free(&got);
    /* the next save repairs the bad slot (it takes the older generation, which is the bad one) */
    struct replfile_state st;
    state_of(&st, 5);
    CHECK(replfile_save(rf, &st) == TSDB_ERR_INVALID); /* the handle holds saves to the belted open generation */
    st.open_gen += REPLFILE_OPEN_GEN_BELT;
    CHECK(replfile_save(rf, &st) == 0);
    replfile_state_free(&st);
    replfile_close(rf);
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn && is_state(&got, 5));
    replfile_state_free(&got);
    replfile_close(rf);
    /* a bit flipped in the magic, the generation, the length, a field, the history, the checksum; a zeroed page */
    off_t offs[] = {0, 8, 16, 100, 128, 128 + 8, 128 + 3 * 16 + 3, 128 + 4 * 16 + 7 /* the checksum */};
    for (size_t i = 0; i < sizeof offs / sizeof *offs; i++) {
        fresh("dmg2", dir, sizeof dir);
        make(dir, 4);
        flip(dir, offs[i]);
        CHECK(load_k(dir, &rf, &got, &torn) == 0 && torn && is_state(&got, 3));
        if (rf) {
            replfile_state_free(&got);
            replfile_close(rf);
        }
    }
    {
        fresh("dmg2", dir, sizeof dir);
        make(dir, 4);
        uint8_t zero[REPLFILE_PAGE] = {0};
        poke(dir, 0, zero, sizeof zero);
        CHECK(load_k(dir, &rf, &got, &torn) == 0 && torn && is_state(&got, 3));
        replfile_state_free(&got);
        replfile_close(rf);
        /* both slots bad */
        poke(dir, REPLFILE_PAGE, zero, sizeof zero);
        CHECK(load_k(dir, &rf, &got, &torn) == TSDB_ERR_CORRUPT && !rf);
    }
    {
        /* garbage after a valid prefix (a sector written, the rest not) */
        fresh("dmg2", dir, sizeof dir);
        make(dir, 4);
        uint8_t junk[REPLFILE_PAGE - 100];
        memset(junk, 0xa5, sizeof junk);
        poke(dir, 100, junk, sizeof junk);
        CHECK(load_k(dir, &rf, &got, &torn) == 0 && torn && is_state(&got, 3));
        replfile_state_free(&got);
        replfile_close(rf);
        /* bytes after the slot's own length are not part of it */
        fresh("dmg2", dir, sizeof dir);
        make(dir, 4);
        poke(dir, 128 + 4 * 16 + 8, junk, 100);
        CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn && is_state(&got, 4));
        replfile_state_free(&got);
        replfile_close(rf);
    }
    {
        /* a short file: slot 1 is not there, slot 0 holds save 4 */
        fresh("dmg2", dir, sizeof dir);
        make(dir, 4);
        char p[PATH_MAX * 2];
        snprintf(p, sizeof p, "%s/REPL", dir);
        CHECK(truncate(p, REPLFILE_PAGE + 50) == 0);
        CHECK(load_k(dir, &rf, &got, &torn) == 0 && torn && is_state(&got, 4));
        replfile_state_free(&got);
        replfile_close(rf);
        CHECK(truncate(p, 100) == 0);
        CHECK(load_k(dir, &rf, &got, &torn) == TSDB_ERR_CORRUPT);
        CHECK(truncate(p, 0) == 0);
        CHECK(load_k(dir, &rf, &got, &torn) == TSDB_ERR_CORRUPT);
    }
    {
        /* a slot that is well checksummed but cannot be: rid 0, a history that goes back, a length that lies */
        uint8_t page[REPLFILE_PAGE];
        struct replfile_state st2;
        state_of(&st2, 3);
        CHECK(replfile_encode(page, &st2, 9) == 0);
        uint64_t zero = 0, h;
        uint32_t len = 128 + 3 * 16 + 8;
        uint8_t bad[REPLFILE_PAGE];
        uint64_t g;
        struct replfile_state p;
        CHECK(replfile_parse(page, sizeof page, &p, &g) && g == 9);
        replfile_state_free(&p);
        memcpy(bad, page, sizeof bad);
        memcpy(bad + 72, &zero, 8); /* rid 0 */
        h = xxh64(bad, len - 8, 0);
        memcpy(bad + len - 8, &h, 8);
        CHECK(!replfile_parse(bad, sizeof bad, &p, &g));
        memcpy(bad, page, sizeof bad);
        uint64_t e = 1; /* second entry's epoch back to 1 */
        memcpy(bad + 128 + 16, &e, 8);
        h = xxh64(bad, len - 8, 0);
        memcpy(bad + len - 8, &h, 8);
        CHECK(!replfile_parse(bad, sizeof bad, &p, &g));
        /* a reserved byte that is not zero, well checksummed: one encoding per state, so it is not a slot (the fuzzer
         * found a parse that took it and an encode that gave other bytes back) */
        for (int k = 0; k < 2; k++) {
            int off = k ? 120 : 60; /* the two reserved fields of the header */
            memcpy(bad, page, sizeof bad);
            bad[off + 2] = 5;
            h = xxh64(bad, len - 8, 0);
            memcpy(bad + len - 8, &h, 8);
            CHECK(!replfile_parse(bad, sizeof bad, &p, &g));
        }
        /* a history entry that starts at 0: no position, and the core fails an open on it */
        memcpy(bad, page, sizeof bad);
        memcpy(bad + 128 + 8, &zero, 8);
        h = xxh64(bad, len - 8, 0);
        memcpy(bad + len - 8, &h, 8);
        CHECK(!replfile_parse(bad, sizeof bad, &p, &g));
        CHECK(!replfile_parse(page, len - 1, &p, &g)); /* cut inside the checksum */
        CHECK(replfile_parse(page, len, &p, &g));
        replfile_state_free(&p);
        replfile_state_free(&st2);
    }
    quiet(false);
    printf("  a damaged newest slot falls back to the older one and the next save repairs it; both bad is CORRUPT\n");
}

static void test_unsure(void)
{
    char dir[PATH_MAX + 32];
    struct replfile *rf;
    struct replfile_state got, st;
    bool torn;
    quiet(true);
    /* rot of the newest slot after its save was acknowledged looks like a torn write: the state that comes back is
       older, so the open generation is raised by a belt, and the handle holds saves to it */
    fresh("unsure", dir, sizeof dir);
    make(dir, 4);
    flip(dir, 88);
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && torn);
    state_of(&st, 3);
    CHECK(got.open_gen == st.open_gen + REPLFILE_OPEN_GEN_BELT && got.fence == 3);
    replfile_state_free(&st);
    replfile_state_free(&got);
    state_of(&st, 4); /* what had been acknowledged: its open generation (2) is below the belted one */
    CHECK(replfile_save(rf, &st) == TSDB_ERR_INVALID);
    st.open_gen = 2 + REPLFILE_OPEN_GEN_BELT;
    CHECK(replfile_save(rf, &st) == 0);
    replfile_state_free(&st);
    replfile_close(rf);
    /* not torn: no belt */
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn && got.open_gen == 2 + REPLFILE_OPEN_GEN_BELT);
    replfile_state_free(&got);
    replfile_close(rf);
    /* a read error is an error, never the end of the file (then "both slots bad" or "torn") */
    fresh("unsure", dir, sizeof dir);
    make(dir, 4);
    replfile_test_eio_read = 1;
    CHECK(load_k(dir, &rf, &got, &torn) == TSDB_ERR_IO && !rf);
    replfile_test_eio_read = 0;
    /* a failed allocation is not damage: no older state, no CORRUPT */
    replfile_test_nomem = 1;
    CHECK(load_k(dir, &rf, &got, &torn) == TSDB_ERR_NOMEM && !rf);
    replfile_test_nomem = 0;
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn && is_state(&got, 4));
    replfile_state_free(&got);
    replfile_close(rf);
    quiet(false);
    printf("  a torn load raises the open generation by a belt; a read error and a failed allocation are not damage\n");
}

/* The history trim keeps the epoch of position log_start - 1 (repl.h: a replica opened at a snapshot P has log_start
   P + 1 and the epoch of P in hist). */
static void test_trim_boundary(void)
{
    char dir[PATH_MAX + 32];
    fresh("trim", dir, sizeof dir);
    make(dir, 0);
    struct replfile *rf;
    struct replfile_state got, s;
    bool torn;
    CHECK(load_k(dir, &rf, &got, &torn) == 0);
    replfile_state_free(&got);
    state_of(&s, 1);
    free(s.hist);
    s.nhist = REPLFILE_MAX_HIST + 1;
    s.hist = calloc(s.nhist, sizeof *s.hist);
    for (uint32_t i = 0; i < s.nhist; i++)
        s.hist[i] = (struct repl_epoch){.epoch = i + 1, .start = 10 * i + 1};
    s.fence = 400, s.floor = 5000;
    s.log_start = s.hist[1].start; /* position log_start - 1 is still in epoch 1: entry 0 is needed, nothing fits */
    CHECK(replfile_save(rf, &s) == TSDB_ERR_INVALID && !replfile_failed(rf));
    s.log_start = s.hist[1].start + 1; /* position log_start - 1 is the first of epoch 2: entry 0 may go */
    CHECK(replfile_save(rf, &s) == 0);
    replfile_close(rf);
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && got.nhist == REPLFILE_MAX_HIST && got.hist[0].epoch == 2);
    replfile_state_free(&got);
    replfile_state_free(&s);
    replfile_close(rf);
    printf("  the history trim keeps the epoch in force at log_start - 1\n");
}

static void test_rules(void)
{
    char dir[PATH_MAX + 32];
    fresh("rules", dir, sizeof dir);
    make(dir, 4);
    struct replfile *rf;
    struct replfile_state cur;
    bool torn;
    CHECK(load_k(dir, &rf, &cur, &torn) == 0);
    uint64_t g0 = replfile_gen(rf), a0 = raw_gen(dir, 0), a1 = raw_gen(dir, 1);
    struct replfile_state s;
    /* each of these is refused and writes nothing */
    for (int t = 0; t < 13; t++) {
        state_of(&s, 4);
        switch (t) {
        case 0: s.rid++; break;
        case 1: s.node++; break;
        case 2: s.volume_uuid[3]++; break;
        case 3: s.cluster_uuid[3]++; break;
        case 4: s.open_gen--; break;
        case 5: s.fence--, s.nhist--; break; /* (the history's last epoch is the fence: it goes with it) */
        case 6: s.led--; break;
        case 7: s.floor--; break;
        case 8: s.log_start--; break;
        case 9: s.hist[2].epoch = s.hist[1].epoch; break; /* not increasing */
        case 10: s.led = s.fence + 1; break;                /* led above fence */
        case 11: s.hist[3].epoch = s.fence + 1; break;      /* an epoch above the fence */
        case 12: s.log_start = s.floor + 2; break;          /* log_start - 1 above the floor */
        }
        CHECK(replfile_save(rf, &s) == TSDB_ERR_INVALID);
        replfile_state_free(&s);
    }
    CHECK(replfile_gen(rf) == g0 && raw_gen(dir, 0) == a0 && raw_gen(dir, 1) == a1 && !replfile_failed(rf));
    /* the same state again, and one that raises only the incarnation, are fine */
    state_of(&s, 4);
    s.incarnation += 5;
    CHECK(replfile_save(rf, &s) == 0);
    replfile_state_free(&s);
    /* a long history: 300 epochs. With the log start high the entries wholly below it go; with a low one the save is
       refused */
    struct replfile_state big;
    state_of(&big, 4);
    big.incarnation += 5;
    free(big.hist);
    big.nhist = 300;
    big.hist = calloc(300, sizeof *big.hist);
    for (int i = 0; i < 300; i++)
        big.hist[i] = (struct repl_epoch){.epoch = (uint64_t)i + 1, .start = 10 * (uint64_t)i + 1};
    big.fence = 400, big.floor = 5000, big.log_start = 1;
    CHECK(replfile_save(rf, &big) == TSDB_ERR_INVALID && !replfile_failed(rf));
    big.log_start = 10 * 100 + 1; /* epoch index 100 starts here: entries 0..99 end below it */
    CHECK(replfile_save(rf, &big) == 0);
    replfile_close(rf);
    struct replfile_state got;
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && !torn);
    CHECK(got.nhist == 247 || got.nhist == 300 - 53); /* trimmed to what fits */
    CHECK(got.nhist <= REPLFILE_MAX_HIST && got.hist[got.nhist - 1].epoch == 300);
    /* the entry in force at the log start is kept (an epoch that began below it and runs into the log) */
    CHECK(got.hist[0].start <= big.log_start);
    CHECK(got.fence == 400 && got.floor == 5000 && got.log_start == 1001);
    replfile_state_free(&got);
    replfile_state_free(&big);
    replfile_state_free(&cur);
    replfile_close(rf);
    /* the history of exactly the capacity fits whole */
    fresh("rules2", dir, sizeof dir);
    make(dir, 0);
    CHECK(load_k(dir, &rf, &got, &torn) == 0);
    replfile_state_free(&got);
    state_of(&s, 1);
    free(s.hist);
    s.nhist = REPLFILE_MAX_HIST;
    s.hist = calloc(REPLFILE_MAX_HIST, sizeof *s.hist);
    for (uint32_t i = 0; i < REPLFILE_MAX_HIST; i++)
        s.hist[i] = (struct repl_epoch){.epoch = i + 1, .start = i + 1};
    s.fence = REPLFILE_MAX_HIST, s.floor = 1000;
    CHECK(replfile_save(rf, &s) == 0);
    replfile_close(rf);
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && got.nhist == REPLFILE_MAX_HIST && same(&got, &s));
    replfile_state_free(&got);
    replfile_state_free(&s);
    replfile_close(rf);
    printf("  identity and the values that never go back are enforced, a long history is trimmed or refused\n");
}

static void test_failstop(void)
{
    char dir[PATH_MAX + 32];
    fresh("fail", dir, sizeof dir);
    make(dir, 2);
    struct replfile *rf;
    struct replfile_state got, s;
    bool torn;
    CHECK(load_k(dir, &rf, &got, &torn) == 0 && is_state(&got, 2));
    replfile_state_free(&got);
    struct rlimit old, lim;
    getrlimit(RLIMIT_FSIZE, &old);
    void (*oldsig)(int) = signal(SIGXFSZ, SIG_IGN);
    for (int variant = 0; variant < 2; variant++) {
        /* EFBIG with nothing written (the limit is the end of slot 0), then a short write (the limit cuts slot 1) */
        lim = old;
        lim.rlim_cur = variant == 0 ? REPLFILE_PAGE : 2 * REPLFILE_PAGE - 1;
        if (variant == 1) { /* a fresh handle: the earlier one has failed */
            replfile_close(rf);
            CHECK(load_k(dir, &rf, &got, &torn) == 0);
            replfile_state_free(&got);
        }
        CHECK(setrlimit(RLIMIT_FSIZE, &lim) == 0);
        state_of(&s, 3);
        quiet(true);
        int rc = replfile_save(rf, &s);
        quiet(false);
        CHECK(rc == TSDB_ERR_IO && replfile_failed(rf));
        setrlimit(RLIMIT_FSIZE, &old);
        /* no retry, even with room: the handle is done */
        CHECK(replfile_save(rf, &s) == TSDB_ERR_IO);
        replfile_state_free(&s);
        /* what the file holds is the old state (or, for a short write, whole new one: both are states a save may leave) */
        struct replfile *rf2;
        quiet(true);
        CHECK(load_k(dir, &rf2, &got, &torn) == 0 && (is_state(&got, 2) || is_state(&got, 3)));
        quiet(false);
        replfile_state_free(&got);
        replfile_close(rf2);
    }
    signal(SIGXFSZ, oldsig);
    replfile_close(rf);
    printf("  a failed write fails the handle for good\n");
}

static void test_convert(void)
{
    struct replfile_state st;
    state_of(&st, 5);
    struct repl_durable d;
    replfile_to_durable(&st, 77, &d);
    CHECK(d.rid == st.rid && d.fence == 5 && d.led == 2 && d.floor == 15 && d.log_start == 2 && d.log_end == 77 &&
          d.nhist == 5 && d.hist == st.hist);
    /* the history entries that start beyond the log's end are not handed to the core (it would fail the open) */
    replfile_to_durable(&st, 12, &d);
    CHECK(d.nhist == 3 && d.hist[2].start == 11);
    replfile_to_durable(&st, 10, &d);
    CHECK(d.nhist == 2);
    replfile_to_durable(&st, 0, &d);
    CHECK(d.nhist == 0);
    replfile_to_durable(&st, 77, &d);
    struct replfile_state other;
    state_of(&other, 0);
    CHECK(replfile_from_durable(&other, &d) == 0);
    other.rid = st.rid;
    CHECK(other.fence == 5 && other.nhist == 5 && other.hist != st.hist && other.hist[4].start == 21);
    other.open_gen = st.open_gen, other.incarnation = st.incarnation;
    CHECK(same(&other, &st));
    replfile_state_free(&st);
    replfile_state_free(&other);
    CHECK(replfile_new_rid() != 0);
    printf("  conversions to and from struct repl_durable\n");
}

/* ------------------------------------------------------------ power loss */

#define NSAVES 24

static void ack(const char *file, int n)
{
    int fd = open(file, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    char line[32];
    int len = snprintf(line, sizeof line, "%d\n", n);
    if (fd >= 0) {
        if (write(fd, line, (size_t)len) != len)
            _exit(2);
        close(fd);
    }
}

static int max_ack(const char *file)
{
    FILE *f = fopen(file, "r");
    int a, mx = -1;
    while (f && fscanf(f, "%d", &a) == 1)
        mx = a > mx ? a : mx;
    if (f)
        fclose(f);
    return mx;
}

/* The caller of a fresh replica: a tmp directory, REPL made in it, the directory renamed into place and its parent
 * synced, then the saves. Every step that returned is written to the ack file. */
static void child_run(const char *dir, const char *ackfile)
{
    hist_scale = 8;
    char tmp[PATH_MAX * 2], fin[PATH_MAX * 2];
    snprintf(tmp, sizeof tmp, "%s/new.tmp", dir);
    snprintf(fin, sizeof fin, "%s/new", dir);
    int pfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (pfd < 0 || mkdir(tmp, 0755) != 0)
        _exit(3);
    int dfd = open(tmp, O_RDONLY | O_DIRECTORY);
    struct replfile_state st;
    state_of(&st, 0);
    struct replfile *rf;
    if (dfd < 0 || replfile_create(dfd, &st, true, &rf) != 0)
        _exit(4);
    replfile_state_free(&st);
    if (fsync(pfd) != 0 || rename(tmp, fin) != 0 || fsync(pfd) != 0)
        _exit(5);
    ack(ackfile, 0);
    for (int k = 1; k <= NSAVES; k++) {
        state_of(&st, k);
        if (replfile_save(rf, &st) != 0)
            _exit(6);
        replfile_state_free(&st);
        ack(ackfile, k);
    }
    _exit(0);
}

static int run_child(const char *dir, const char *ackfile, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        char w[16], sd[16];
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_replfile", "--child", dir, ackfile, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* After a power loss: what the directory holds is a whole state, the last acknowledged one or the one in flight. */
static void check_after(const char *dir, const char *ackfile, int k)
{
    int mx = max_ack(ackfile);
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/new", dir);
    struct stat sb;
    if (stat(p, &sb) != 0) /* the rename was lost: the directory is still the caller's tmp */
        snprintf(p, sizeof p, "%s/new.tmp", dir);
    struct replfile *rf = NULL;
    struct replfile_state got;
    bool torn;
    int rc = stat(p, &sb) == 0 ? load_k(p, &rf, &got, &torn) : TSDB_ERR_NOTFOUND;
    if (rc != 0) {
        if (mx >= 0 || (rc != TSDB_ERR_NOTFOUND)) {
            char m[200];
            snprintf(m, sizeof m, "step %d: the load gave %d with the state %d acknowledged", k, rc, mx);
            fail_at(__FILE__, __LINE__, m);
        }
        return;
    }
    int found = (int)(got.fence / 8); /* hist_scale is 8 here: the history is 8 entries per save */
    bool was_torn = torn;
    if (!is_state(&got, found) || found < mx || found > mx + 1 || found > NSAVES) {
        char m[200];
        snprintf(m, sizeof m, "step %d: found state %d (whole: %d), the last acknowledged is %d", k, found,
                 is_state(&got, found), mx);
        fail_at(__FILE__, __LINE__, m);
    }
    replfile_state_free(&got);
    /* the next save works on what a torn slot left, and the result is read back */
    if (found < NSAVES) {
        struct replfile_state nx;
        state_of(&nx, found + 1);
        if (was_torn)
            nx.open_gen += REPLFILE_OPEN_GEN_BELT;
        CHECK(replfile_save(rf, &nx) == 0);
        replfile_state_free(&nx);
        replfile_close(rf);
        CHECK(load_k(p, &rf, &got, &torn) == 0 && !torn && is_state(&got, found + 1));
        replfile_state_free(&got);
    }
    replfile_close(rf);
}

static void test_power_loss(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32];
    hist_scale = 8;
    snprintf(ackf, sizeof ackf, "%s/pl.ack", S);
    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 800 && !finished; k++) {
        fresh("pl-dir", dir, sizeof dir);
        unlink(ackf);
        int code = run_child(dir, ackf, k, k + 1);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        quiet(true);
        check_after(dir, ackf, k);
        quiet(false);
    }
    CHECK(finished && crashes > 50);
    printf("  power lost at each of %d steps of create and %d saves: the state found is the last acknowledged or the "
           "one in flight, whole\n", crashes, NSAVES);
}

static void set_mutant(const char *m)
{
    replfile_test_mutant = strcmp(m, "nosync") == 0       ? REPLFILE_MUT_NOSYNC
                           : strcmp(m, "sameslot") == 0   ? REPLFILE_MUT_SAMESLOT
                           : strcmp(m, "nochecksum") == 0 ? REPLFILE_MUT_NOCHECKSUM
                                                          : REPLFILE_MUT_NONE;
    if (replfile_test_mutant == REPLFILE_MUT_NONE) {
        fprintf(stderr, "unknown mutant %s\n", m);
        exit(2);
    }
    setenv("TSDB_REPLFILE_MUTANT", m, 1); /* the processes the shim kills run the same bug */
}

int main(int argc, char **argv)
{
    if (argc >= 4 && strcmp(argv[1], "--child") == 0) {
        if (getenv("TSDB_REPLFILE_MUTANT"))
            set_mutant(getenv("TSDB_REPLFILE_MUTANT"));
        child_run(argv[2], argv[3]);
        return 0;
    }
    const char *scratch = "/tmp", *mutant = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            scratch = argv[i];
    }
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/test-replfile.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    if (mutant)
        set_mutant(mutant);
    if (replfile_test_mutant == REPLFILE_MUT_NOSYNC) { /* only the power-loss sweep can see this one */
        test_power_loss();
    } else {
        test_basic();
        test_damage();
        test_unsure();
        test_trim_boundary();
        test_rules();
        test_failstop();
        test_convert();
        if (!mutant || replfile_test_mutant == REPLFILE_MUT_SAMESLOT)
            test_power_loss();
    }
    sh("rm -rf '%s'", S);
    if (failures)
        fprintf(stderr, "%d replfile checks FAILED\n", failures);
    else
        printf("replfile tests passed\n");
    return failures ? 1 : 0;
}
