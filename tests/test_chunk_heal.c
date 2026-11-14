/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The directory backend with stores missing, damaged or replaced (src/chunkstore.c, src/chunkheal.c, src/chunkread.c;
 * docs/design-cluster-data.md §5.5, §5.9, E15, E55).
 *
 *  - open degraded: one or two of six stores missing, with another id, with a damaged META, failing with EIO: the
 *    backend opens when min_up are UP, each store's state and the one log line per down store are right, every object
 *    reads back byte-exact; fewer than min_up is UNAVAILABLE naming them; another owner (BUSY) and one store reached
 *    twice never become DOWN, they fail the open;
 *  - reads with every set of up to m stores down are exact for every object (k' < k and k' = 1 included), with more
 *    down they are UNAVAILABLE and never CORRUPT (E15), and with chunks really lost CORRUPT;
 *  - heal: a chunk deleted, truncated, emptied, or with a flipped byte in a cell (scrub only), the header, the sums
 *    table or the footer is rebuilt bit for bit, the report counts are exact, a second heal does nothing; below the
 *    fence it is refused and the rebuilt chunk keeps the epoch the object was written at;
 *  - what heal must not do: rebuild from fewer than k' positions (unrecoverable, unavailable), write when the
 *    survivors are not one encoding, bring back an object the caller deleted (k' = 1: a copy decodes by itself, only
 *    `live` knows), rebuild the remains of an object deleted while the store was away;
 *  - replace: a store removed from disk is replaced at a new path, its EPOCH holds the fence before any chunk, the
 *    objects are whole again with the new store alone holding its positions (two other stores down afterwards),
 *    reads in the middle of the fill go around what the new store lacks; attach of a store that was away fills what
 *    was written meanwhile;
 *  - power loss (crashfs) at every step of a heal and of a replace: a chunk is its old image or the whole new one, the
 *    new store holds no chunk before its fence, and a second run completes;
 *  - threads: a heal against readers and a writer (for tsan).
 *
 *   test_chunk_heal [DIR]             DIR is the scratch directory of the other tests
 *   test_chunk_heal --quick           coarser power-loss sweeps
 *   test_chunk_heal --mutant NAME     plants heal_noverify | heal_epoch0 | replace_late | open_busy_down | open_id_up |
 *                                     heal_kminus1 | heal_resurrect: must fail
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#ifndef TSDB_EC
#include <stdio.h>
int main(void)
{
    printf("test_chunk_heal: SKIPPED (this build has no erasure codec: it is x86-64 only)\n");
    return 0;
}
#else
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

/* paths are built into buffers that are far larger than any the test makes */
#pragma GCC diagnostic ignored "-Wformat-truncation"

#define VOL 1u
#define NST 6
#define MAXO 8
static const uint8_t VU[16] = {0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77};

static int failures;
static FILE *ERR;
static int devnull = -1;
#define CHECK(cond)                                                                           \
    do {                                                                                      \
        if (!(cond)) {                                                                        \
            fprintf(ERR ? ERR : stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            if (++failures > 60)                                                              \
                exit(1);                                                                      \
        }                                                                                     \
    } while (0)

static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];
static bool quick;

static uint64_t rng = 0x243f6a8885a308d3ull;
static uint64_t rnd(void)
{
    rng ^= rng >> 12, rng ^= rng << 25, rng ^= rng >> 27;
    return rng * 0x2545f4914f6cdd1dull;
}

static void sh(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void sh(const char *fmt, ...)
{
    char cmd[PATH_MAX * 3];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    if (system(cmd) != 0)
        fprintf(ERR, "(command failed: %s)\n", cmd);
}

static void fresh(const char *tag, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%s", S, tag);
    sh("rm -rf '%s'; mkdir -p '%s'", out, out);
}

/* The library's log lines go to /dev/null, except while a test captures them. */
static int saved_err = -1;
static void cap_start(const char *file)
{
    saved_err = dup(2);
    int fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0)
        dup2(fd, 2), close(fd);
}
static void cap_end(void)
{
    if (saved_err >= 0) {
        dup2(saved_err, 2);
        close(saved_err);
        saved_err = -1;
    }
}
static int cap_count(const char *file, const char *needle)
{
    FILE *f = fopen(file, "r");
    char line[1024];
    int n = 0;
    while (f && fgets(line, sizeof line, f))
        n += strstr(line, needle) != NULL;
    if (f)
        fclose(f);
    return n;
}

/* ---------------------------------------------------------------- files */
static uint8_t *slurp(const char *path, size_t *n)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *b = malloc(len > 0 ? (size_t)len : 1);
    if (!b || (len > 0 && fread(b, 1, (size_t)len, f) != (size_t)len)) {
        fclose(f);
        free(b);
        return NULL;
    }
    fclose(f);
    *n = (size_t)len;
    return b;
}

static void spit(const char *path, const uint8_t *b, size_t n)
{
    FILE *f = fopen(path, "wb");
    if (f) {
        if (n)
            fwrite(b, 1, n, f);
        fclose(f);
    }
}

typedef struct img {
    char name[64];
    uint8_t *b;
    size_t n;
} img;
typedef struct imgs {
    img *v;
    int n;
} imgs;

static void imgs_take(imgs *s, const char *store)
{
    char d[PATH_MAX + 64];
    snprintf(d, sizeof d, "%s/s%u", store, VOL);
    DIR *dir = opendir(d);
    struct dirent *e;
    memset(s, 0, sizeof *s);
    while (dir && (e = readdir(dir))) {
        size_t l = strlen(e->d_name);
        if (l < 7 || strcmp(e->d_name + l - 6, ".chunk") != 0)
            continue;
        s->v = realloc(s->v, (size_t)(s->n + 1) * sizeof *s->v);
        img *i = &s->v[s->n++];
        snprintf(i->name, sizeof i->name, "%s", e->d_name);
        char p[3 * PATH_MAX];
        snprintf(p, sizeof p, "%s/%s", d, e->d_name);
        i->b = slurp(p, &i->n);
    }
    if (dir)
        closedir(dir);
}

static void imgs_free(imgs *s)
{
    for (int i = 0; i < s->n; i++)
        free(s->v[i].b);
    free(s->v);
    memset(s, 0, sizeof *s);
}

/* How many of the images are not, byte for byte, the file of that name in the store's volume directory. */
static int imgs_diff(const imgs *s, const char *store)
{
    int bad = 0;
    for (int i = 0; i < s->n; i++) {
        char p[3 * PATH_MAX];
        snprintf(p, sizeof p, "%s/s%u/%s", store, VOL, s->v[i].name);
        size_t n = 0;
        uint8_t *b = slurp(p, &n);
        bad += !(b && n == s->v[i].n && memcmp(b, s->v[i].b, n) == 0);
        free(b);
    }
    return bad;
}

static int count_chunks(const char *store)
{
    imgs s;
    imgs_take(&s, store);
    int n = s.n;
    imgs_free(&s);
    return n;
}

enum { D_DELETE, D_TRUNC, D_EMPTY, D_CELL, D_HEADER, D_SUMS, D_FOOTER, D_KINDS };
static const char *dname[D_KINDS] = {"deleted", "truncated", "emptied", "cell flipped", "header flipped",
                                     "sums table flipped", "footer flipped"};

static void damage(const char *path, int kind)
{
    size_t n = 0;
    uint8_t *b = slurp(path, &n);
    if (!b)
        return;
    chunk_hdr h;
    memset(&h, 0, sizeof h);
    (void)chunk_hdr_decode(b, &h);
    switch (kind) {
    case D_DELETE: unlink(path); break;
    case D_TRUNC: spit(path, b, n - 7); break;
    case D_EMPTY: spit(path, b, 0); break;
    case D_CELL: b[CHUNK_HDR_BYTES + 1] ^= 0x5a; spit(path, b, n); break;
    case D_HEADER: b[20] ^= 0x5a; spit(path, b, n); break;
    case D_SUMS: b[CHUNK_HDR_BYTES + h.chunk_len + 1] ^= 0x5a; spit(path, b, n); break;
    case D_FOOTER: b[n - 3] ^= 0x5a; spit(path, b, n); break;
    }
    free(b);
}

/* ---------------------------------------------------------------- the world: six stores and some objects */
typedef struct obj {
    chunk_geom g;
    uint8_t oid[16];
    uint8_t *data;
} obj;

typedef struct world {
    char dir[PATH_MAX + 32];
    char path[NST][PATH_MAX + 64];
    uint8_t ids[NST * 16];
    chunk_backend *be;
    obj o[MAXO];
    int no;
} world;

static int place(const uint8_t oid[16], unsigned pos) { return (int)((xxh64(oid, 16, 0) + pos) % NST); }

static int pos_on(const obj *o, int store)
{
    for (unsigned p = 0; p < chunk_npos(&o->g); p++)
        if (place(o->oid, p) == store)
            return (int)p;
    return -1;
}

static void chunk_file(const world *w, const obj *o, int store, char *out, size_t cap)
{
    char hex[33];
    chunk_oid_hex(o->oid, hex);
    snprintf(out, cap, "%s/s%u/o%s.%d.chunk", w->path[store], VOL, hex, pos_on(o, store));
}

static int world_open(world *w, unsigned down_mask, int min_up, const uint8_t *ids)
{
    const char *pp[NST];
    for (int i = 0; i < NST; i++)
        pp[i] = down_mask >> i & 1 ? NULL : w->path[i];
    chunk_be_opts o = {.min_up = min_up};
    return chunk_be_open2(&w->be, pp, NST, ids ? ids : w->ids, &o);
}

static void world_close(world *w)
{
    chunk_be_close(w->be);
    w->be = NULL;
}

static void obj_make(obj *o, unsigned k, unsigned m, uint32_t cell, uint64_t len)
{
    memset(o, 0, sizeof *o);
    CHECK(chunk_geom_make(&o->g, k, m, cell, len) == 0);
    chunk_random(o->oid, 16);
    o->data = malloc((size_t)len);
    for (uint64_t i = 0; i < len; i++)
        o->data[i] = (uint8_t)(rnd() >> 29);
}

static int put_obj(chunk_backend *be, const obj *o, uint64_t epoch, unsigned mask)
{
    return chunk_be_put_object(be, VOL, VU, o->oid, &o->g, epoch, o->data, mask, NULL);
}

/* Six stores, the backend open, six objects (4+2 with three and nine and two stripes, k' = 2, k' = 1, k' = 3) at epoch 3,
 * the fence then at 5. */
static void world_build(world *w, const char *tag)
{
    memset(w, 0, sizeof *w);
    fresh(tag, w->dir, sizeof w->dir);
    for (int i = 0; i < NST; i++) {
        snprintf(w->path[i], sizeof w->path[i], "%s/st%d", w->dir, i);
        CHECK(chunk_store_init(w->path[i], NULL, w->ids + 16 * i) == 0);
    }
    CHECK(world_open(w, 0, NST, NULL) == 0);
    obj_make(&w->o[0], 4, 2, 64, 64 * 4 * 3 + 17);
    obj_make(&w->o[1], 4, 2, 100, 100 * 9);
    obj_make(&w->o[2], 2, 2, 64, 100);
    obj_make(&w->o[3], 1, 2, 64, 50);
    obj_make(&w->o[4], 3, 2, 32, 32 * 7 + 5);
    obj_make(&w->o[5], 4, 2, 50, 50 * 4 * 2);
    w->no = 6;
    CHECK(chunk_be_see_epoch(w->be, VOL, 3) == 0);
    for (int i = 0; i < w->no; i++)
        CHECK(put_obj(w->be, &w->o[i], 3, ~0u) == 0);
    CHECK(chunk_be_see_epoch(w->be, VOL, 5) == 0);
}

static void world_free(world *w)
{
    if (w->be)
        world_close(w);
    for (int i = 0; i < w->no; i++)
        free(w->o[i].data);
}

static int read_obj(chunk_backend *be, const obj *o, uint8_t *buf)
{
    chunk_obj *ob = NULL;
    int rc = chunk_obj_open(be, VOL, VU, o->oid, NULL, NULL, &ob);
    if (rc)
        return rc;
    rc = chunk_obj_read(ob, 0, o->g.len, buf, 0, NULL);
    chunk_obj_close(ob);
    return rc;
}

/* The read's answer must be exact bytes, or the one error E15 allows for the case (never wrong bytes, never lost). */
static bool read_is(chunk_backend *be, const obj *o, int want)
{
    uint8_t *buf = malloc((size_t)o->g.len);
    int rc = read_obj(be, o, buf);
    bool ok = rc == want && (rc != 0 || memcmp(buf, o->data, (size_t)o->g.len) == 0);
    free(buf);
    return ok;
}

static bool reads_exact(chunk_backend *be, const world *w)
{
    for (int i = 0; i < w->no; i++)
        if (!read_is(be, &w->o[i], 0))
            return false;
    return true;
}

static int expect_down(const obj *o, unsigned mask)
{
    int down = 0;
    for (unsigned p = 0; p < chunk_npos(&o->g); p++)
        down += mask >> place(o->oid, p) & 1;
    return down <= o->g.m ? 0 : TSDB_ERR_UNAVAILABLE;
}

/* ---------------------------------------------------------------- open with stores down */
static void test_open_degraded(void)
{
    enum { K_MISSING, K_OTHER_ID, K_DAMAGED, K_IOERR, KINDS };
    static const int want_state[KINDS] = {CHUNK_STORE_DOWN_MISSING, CHUNK_STORE_DOWN_OTHER_ID, CHUNK_STORE_DOWN_DAMAGED,
                                          CHUNK_STORE_DOWN_IOERR};
    int runs = 0;
    for (int kind = 0; kind < KINDS; kind++)
        for (int nbad = 0; nbad <= 3; nbad++) {
            if (kind == K_IOERR && nbad > 1)
                continue; /* the hook fails the first readdir of the first store that has a volume */
            world w;
            world_build(&w, "open");
            world_close(&w);
            int bad[3] = {kind == K_IOERR ? 0 : 1, 4, 3};
            uint8_t ids2[NST * 16];
            memcpy(ids2, w.ids, sizeof ids2);
            unsigned mask = 0;
            for (int b = 0; b < nbad; b++) {
                mask |= 1u << bad[b];
                char p[PATH_MAX + 96];
                if (kind == K_MISSING)
                    sh("rm -rf '%s'", w.path[bad[b]]);
                else if (kind == K_OTHER_ID)
                    ids2[16 * bad[b]] ^= 0x01;
                else if (kind == K_DAMAGED) {
                    snprintf(p, sizeof p, "%s/META", w.path[bad[b]]);
                    spit(p, (const uint8_t *)"damaged", 7);
                }
            }
            if (kind == K_IOERR && nbad)
                chunk_fault_readdir = 1;
            char logf[PATH_MAX + 64];
            snprintf(logf, sizeof logf, "%s/open.log", S);
            cap_start(logf);
            int rc = world_open(&w, 0, NST - 2, ids2);
            cap_end();
            chunk_fault_readdir = 0;
            if (nbad <= 2) {
                CHECK(rc == 0);
                if (rc == 0) {
                    for (int i = 0; i < NST; i++)
                        CHECK(chunk_be_store_state(w.be, i) == ((mask >> i & 1) ? want_state[kind] : CHUNK_STORE_UP));
                    CHECK(chunk_be_store_state(w.be, NST) == -1);
                    CHECK(cap_count(logf, chunk_store_state_name(want_state[kind])) == nbad); /* one line per down store */
                    CHECK(reads_exact(w.be, &w)); /* with up to m down, every object */
                }
            } else {
                CHECK(rc == TSDB_ERR_UNAVAILABLE); /* below min_up: refused, the down stores are named */
                CHECK(cap_count(logf, "3 of 6") + cap_count(logf, "3 are needed") + cap_count(logf, "needed") >= 1);
                world_close(&w);
                w.be = NULL;
                CHECK(world_open(&w, 0, 3, ids2) == 0); /* the caller that allows three down gets E15's answers */
                for (int i = 0; i < w.no; i++)
                    CHECK(read_is(w.be, &w.o[i], expect_down(&w.o[i], mask)));
            }
            if (rc == 0 || nbad > 2)
                world_close(&w);
            w.be = NULL;
            world_free(&w);
            runs++;
        }
    printf("  open degraded: %d runs (stores missing, with another id, a damaged META, EIO: states, log lines, min_up, reads)\n", runs);
}

static void test_open_refusals(void)
{
    world w;
    world_build(&w, "refuse");
    /* BUSY: another owner of a store (here this process, which holds the flock): two owners would let EPOCH go back,
     * so the open fails even when the caller would accept every store down */
    chunk_backend *b2 = NULL;
    const char *pp[NST];
    for (int i = 0; i < NST; i++)
        pp[i] = w.path[i];
    chunk_be_opts o0 = {.min_up = 0};
    CHECK(chunk_be_open2(&b2, pp, NST, w.ids, &o0) == CHUNK_ERR_BUSY);
    CHECK(reads_exact(w.be, &w)); /* and the first owner is unharmed */
    world_close(&w);
    /* one store reached twice: its copy has the same id */
    sh("cp -a '%s' '%s/copy'", w.path[0], w.dir);
    char cp[PATH_MAX + 64];
    snprintf(cp, sizeof cp, "%s/copy", w.dir);
    const char *two[2] = {w.path[0], cp};
    CHECK(chunk_be_open2(&b2, two, 2, NULL, &o0) == TSDB_ERR_INVALID);
    /* min_up 0 with every path missing: opens with nothing up, reads are UNAVAILABLE and nothing is NOTFOUND */
    const char *none[2] = {NULL, "/nonexistent-dir-for-test/x"};
    CHECK(chunk_be_open2(&b2, none, 2, NULL, &o0) == 0);
    CHECK(chunk_be_store_state(b2, 0) == CHUNK_STORE_DOWN_MISSING && chunk_be_store_state(b2, 1) == CHUNK_STORE_DOWN_MISSING);
    CHECK(chunk_be_store(b2, 0) == NULL);
    chunk_be_close(b2);
    /* the strict open still fails on a named store that is not there */
    chunk_backend *b3 = NULL;
    const char *miss[2] = {w.path[1], "/nonexistent-dir-for-test/x"};
    CHECK(chunk_be_open(&b3, miss, 2, NULL) != 0);
    world_free(&w);
    printf("  open refusals: another owner and one store twice fail the open, no store up is allowed only on request\n");
}

/* ---------------------------------------------------------------- reads around down stores */
static void test_reads_down(void)
{
    world w;
    world_build(&w, "readsdown");
    world_close(&w);
    int runs = 0;
    for (unsigned mask = 0; mask < (1u << NST); mask++) {
        if (__builtin_popcount(mask) > 3)
            continue;
        CHECK(world_open(&w, mask, 0, NULL) == 0);
        for (int i = 0; i < w.no; i++) {
            CHECK(read_is(w.be, &w.o[i], expect_down(&w.o[i], mask)));
            runs++;
        }
        world_close(&w);
    }
    /* chunks really gone (not a store that is down) are lost, and say so: three positions of a 4+2 object */
    CHECK(world_open(&w, 0, NST, NULL) == 0);
    char p[3 * PATH_MAX];
    for (int s = 0; s < 3; s++) {
        chunk_file(&w, &w.o[0], s, p, sizeof p);
        unlink(p);
    }
    CHECK(read_is(w.be, &w.o[0], TSDB_ERR_CORRUPT));
    world_free(&w);
    printf("  reads: every set of up to 3 stores down, %d object reads: exact up to m down, UNAVAILABLE beyond, CORRUPT only when lost\n", runs);
}

/* ---------------------------------------------------------------- heal */
static void test_heal_damage(void)
{
    world w;
    world_build(&w, "heal");
    int t = 2;
    imgs good;
    imgs_take(&good, w.path[t]);
    CHECK(good.n >= 4);
    CHECK(chunk_be_see_epoch(w.be, VOL, 7) == 0);
    int cases = 0;
    for (int kind = 0; kind < D_KINDS; kind++) {
        char p[3 * PATH_MAX];
        snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[kind % good.n].name);
        damage(p, kind);
        chunk_heal_report rep;
        chunk_heal_opts ho = {.epoch = 3};
        /* below the fence: refused, nothing written */
        CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == CHUNK_ERR_FENCED);
        CHECK(imgs_diff(&good, w.path[t]) == 1);
        ho.epoch = 7;
        if (kind == D_CELL) { /* a rotten cell is found by a scrub only */
            CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0);
            CHECK(rep.rebuilt_damaged == 0 && rep.rebuilt_missing == 0 && imgs_diff(&good, w.path[t]) == 1);
            ho.scrub = true;
        }
        CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0);
        if (rep.rebuilt_missing != (kind == D_DELETE) || rep.rebuilt_damaged != (kind != D_DELETE) ||
            rep.chunks_ok != (uint64_t)good.n - 1 || rep.objects != (uint64_t)good.n || rep.unrecoverable || rep.unavailable ||
            rep.failed || rep.not_live)
            fprintf(ERR, "  heal %s: objects %llu ok %llu missing %llu damaged %llu unrec %llu unavail %llu failed %llu\n",
                    dname[kind], (unsigned long long)rep.objects, (unsigned long long)rep.chunks_ok,
                    (unsigned long long)rep.rebuilt_missing, (unsigned long long)rep.rebuilt_damaged,
                    (unsigned long long)rep.unrecoverable, (unsigned long long)rep.unavailable,
                    (unsigned long long)rep.failed);
        CHECK(rep.rebuilt_missing == (kind == D_DELETE) && rep.rebuilt_damaged == (kind != D_DELETE));
        CHECK(rep.chunks_ok == (uint64_t)good.n - 1 && rep.objects == (uint64_t)good.n);
        CHECK(rep.unrecoverable == 0 && rep.unavailable == 0 && rep.failed == 0 && rep.not_live == 0);
        CHECK(rep.bytes == good.v[kind % good.n].n);
        CHECK(imgs_diff(&good, w.path[t]) == 0); /* bit for bit, the header's epoch (the object's, 3) included */
        /* a second heal does nothing */
        ho.scrub = true;
        CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0);
        CHECK(rep.rebuilt_missing == 0 && rep.rebuilt_damaged == 0 && rep.chunks_ok == (uint64_t)good.n && rep.bytes == 0);
        CHECK(reads_exact(w.be, &w));
        cases++;
    }
    /* a whole store's worth of damage at once, and the per-chunk repair a reader would call */
    for (int i = 0; i < good.n; i++) {
        char p[3 * PATH_MAX];
        snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[i].name);
        damage(p, i % D_KINDS);
    }
    chunk_heal_report rep;
    chunk_heal_opts ho = {.epoch = 7, .scrub = true};
    CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0);
    CHECK(rep.rebuilt_missing + rep.rebuilt_damaged == (uint64_t)good.n && imgs_diff(&good, w.path[t]) == 0);
    char p[3 * PATH_MAX];
    chunk_file(&w, &w.o[1], t, p, sizeof p);
    damage(p, D_CELL);
    chunk_ident id = {.volume = VOL, .pos = (uint16_t)pos_on(&w.o[1], t)};
    memcpy(id.vuuid, VU, 16);
    memcpy(id.oid, w.o[1].oid, 16);
    CHECK(chunk_be_repair(w.be, &id, &ho, &rep) == 0 && rep.rebuilt_damaged == 1 && imgs_diff(&good, w.path[t]) == 0);
    CHECK(chunk_be_repair(w.be, &id, &ho, &rep) == 0 && rep.chunks_ok == 1 && rep.rebuilt_damaged == 0);
    CHECK(chunk_be_heal(w.be, VOL, 0, &ho, &rep) == 0); /* another store: nothing to do */
    CHECK(chunk_be_heal(w.be, VOL, NST, &ho, &rep) == TSDB_ERR_INVALID);
    imgs_free(&good);
    world_free(&w);
    printf("  heal: %d damage kinds (deleted, truncated, emptied, cell, header, sums table, footer): bit-exact, counts, fence, repeat\n", cases);
}

static int never_live(void *ctx, const uint8_t oid[16])
{
    (void)ctx, (void)oid;
    return 0;
}

static void test_heal_refusals(void)
{
    world w;
    world_build(&w, "healno");
    char p[3 * PATH_MAX];
    chunk_heal_report rep;
    chunk_heal_opts ho = {.epoch = 5, .scrub = true};

    /* more than m positions lost: nothing fabricated, the answer is "lost", E15 */
    int a = place(w.o[0].oid, 0), b = place(w.o[0].oid, 1), c = place(w.o[0].oid, 2);
    imgs ia;
    imgs_take(&ia, w.path[a]);
    chunk_file(&w, &w.o[0], a, p, sizeof p);
    unlink(p);
    chunk_file(&w, &w.o[0], b, p, sizeof p);
    unlink(p);
    chunk_file(&w, &w.o[0], c, p, sizeof p);
    unlink(p);
    CHECK(chunk_be_heal(w.be, VOL, a, &ho, &rep) == 0);
    CHECK(rep.unrecoverable == 1 && rep.rebuilt_missing == 0 && rep.rebuilt_damaged == 0 && rep.failed == 0);
    chunk_file(&w, &w.o[0], a, p, sizeof p);
    CHECK(access(p, F_OK) != 0); /* nothing written */
    CHECK(read_is(w.be, &w.o[0], TSDB_ERR_CORRUPT));
    CHECK(imgs_diff(&ia, w.path[a]) == 1); /* the one file that is gone */
    imgs_free(&ia);
    /* the same with one of those stores merely away: not lost, UNAVAILABLE, and heal says so (the chunk of the store that
     * is away is put back first: it is there, only the store is not) */
    world_close(&w);
    CHECK(world_open(&w, 0, NST, NULL) == 0);
    CHECK(put_obj(w.be, &w.o[0], 5, 1u << 2) == 0); /* position 2 only, the same bytes */
    world_close(&w);
    CHECK(world_open(&w, 1u << c, 0, NULL) == 0);
    CHECK(chunk_be_heal(w.be, VOL, a, &ho, &rep) == 0);
    CHECK(rep.unavailable == 1 && rep.unrecoverable == 0 && rep.rebuilt_missing == 0);
    chunk_file(&w, &w.o[0], a, p, sizeof p);
    CHECK(access(p, F_OK) != 0);
    CHECK(read_is(w.be, &w.o[0], TSDB_ERR_UNAVAILABLE));
    world_free(&w);

    /* survivors that are not one encoding: a self-consistent chunk of other bytes at a data and at a parity position */
    for (int which = 0; which < 2; which++) {
        world v;
        world_build(&v, "healenc");
        int t = place(v.o[1].oid, 1), x = place(v.o[1].oid, which ? 5 : 0);
        chunk_file(&v, &v.o[1], t, p, sizeof p);
        unlink(p);
        unsigned pos = (unsigned)pos_on(&v.o[1], x);
        size_t len = (size_t)chunk_pos_len(&v.o[1].g, pos);
        uint8_t *junk = malloc(len);
        for (size_t i = 0; i < len; i++)
            junk[i] = (uint8_t)(rnd() >> 31);
        chunk_ident id = {.volume = VOL, .pos = (uint16_t)pos};
        memcpy(id.vuuid, VU, 16);
        memcpy(id.oid, v.o[1].oid, 16);
        CHECK(chunk_store_put(chunk_be_store(v.be, x), &id, &v.o[1].g, 5, junk, NULL) == 0);
        free(junk);
        CHECK(chunk_be_heal(v.be, VOL, t, &ho, &rep) == 0);
        CHECK(rep.failed == 1 && rep.rebuilt_missing == 0 && rep.rebuilt_damaged == 0);
        chunk_file(&v, &v.o[1], t, p, sizeof p);
        CHECK(access(p, F_OK) != 0); /* nothing written from chunks that disagree */
        world_free(&v);
    }

    /* no resurrection. (1) k' = 4: an object deleted while a store was away leaves one chunk there; the stores that
     * were up deleted theirs, and neither attach nor heal brings the object back */
    world r;
    world_build(&r, "healdel");
    int t = place(r.o[0].oid, 3);
    world_close(&r);
    CHECK(world_open(&r, 1u << t, NST - 1, NULL) == 0);
    for (unsigned pos = 0; pos < chunk_npos(&r.o[0].g); pos++) {
        chunk_ident id = {.volume = VOL, .pos = (uint16_t)pos};
        memcpy(id.vuuid, VU, 16);
        memcpy(id.oid, r.o[0].oid, 16);
        CHECK(chunk_be_del(r.be, &id, 5) == 0 || place(r.o[0].oid, pos) == t);
    }
    CHECK(chunk_be_attach(r.be, t, r.path[t], r.ids + 16 * t, &ho, &rep) == 0);
    CHECK(rep.rebuilt_missing == 0 && rep.rebuilt_damaged == 0 && chunk_be_store_state(r.be, t) == CHUNK_STORE_UP);
    int nfiles = 0;
    for (int s = 0; s < NST; s++) {
        chunk_file(&r, &r.o[0], s, p, sizeof p);
        nfiles += access(p, F_OK) == 0;
    }
    CHECK(nfiles == 1); /* the orphan on the store that was away, and nothing else */
    for (int s = 0; s < NST; s++) {
        if (s == t)
            continue;
        CHECK(chunk_be_heal(r.be, VOL, s, &ho, &rep) == 0);
        CHECK(rep.unrecoverable == 1); /* named by the orphan, one position of four: not rebuilt */
        chunk_file(&r, &r.o[0], s, p, sizeof p);
        CHECK(access(p, F_OK) != 0);
    }
    world_free(&r);

    /* (2) k' = 1: a copy decodes by itself, so only the caller's records can say the object is gone */
    world q;
    world_build(&q, "healdel1");
    int t1 = place(q.o[3].oid, 2), h = place(q.o[3].oid, 0);
    world_close(&q);
    CHECK(world_open(&q, 1u << t1, NST - 1, NULL) == 0);
    for (unsigned pos = 0; pos < 3; pos++) {
        chunk_ident id = {.volume = VOL, .pos = (uint16_t)pos};
        memcpy(id.vuuid, VU, 16);
        memcpy(id.oid, q.o[3].oid, 16);
        CHECK(chunk_be_del(q.be, &id, 5) == 0 || place(q.o[3].oid, pos) == t1);
    }
    CHECK(chunk_be_attach(q.be, t1, q.path[t1], NULL, &ho, &rep) == 0);
    ho.live = NULL;
    ho.live = never_live;
    CHECK(chunk_be_heal(q.be, VOL, h, &ho, &rep) == 0);
    CHECK(rep.not_live == rep.objects && rep.objects >= 1 && rep.rebuilt_missing == 0 && rep.rebuilt_damaged == 0);
    chunk_file(&q, &q.o[3], h, p, sizeof p);
    CHECK(access(p, F_OK) != 0);
    world_free(&q);
    printf("  heal refusals: lost (CORRUPT) vs away (UNAVAILABLE), chunks of two encodings, deleted objects stay deleted\n");
}

/* ---------------------------------------------------------------- replace and attach */
typedef struct watch {
    chunk_backend *be;
    world *w;
    int store;
    uint64_t fence;
    int calls, bad, replacing_reads;
} watch;

/* The caller's `live`, used here as a probe of the moment: the new store must hold the fence before anything is made in
 * it, be REPLACING, and every object must read exactly (around what the new store lacks). */
static int probe_live(void *vctx, const uint8_t oid[16])
{
    (void)oid;
    watch *x = vctx;
    x->calls++;
    chunk_store *st = chunk_be_store(x->be, x->store);
    if (!st || chunk_store_epoch(st, VOL) < x->fence)
        x->bad++;
    if (chunk_be_store_state(x->be, x->store) != CHUNK_STORE_REPLACING)
        x->bad++;
    if (!reads_exact(x->be, x->w))
        x->bad++;
    else
        x->replacing_reads++;
    return 1;
}

static void test_replace(void)
{
    world w;
    world_build(&w, "replace");
    int t = 4;
    imgs good;
    imgs_take(&good, w.path[t]);
    world_close(&w);
    sh("rm -rf '%s'", w.path[t]);
    CHECK(world_open(&w, 0, NST - 2, NULL) == 0);
    CHECK(chunk_be_store_state(w.be, t) == CHUNK_STORE_DOWN_MISSING);
    CHECK(reads_exact(w.be, &w));
    char np[PATH_MAX + 64];
    snprintf(np, sizeof np, "%s/new_t", w.dir);
    uint8_t newid[16];
    chunk_heal_report rep;
    /* refused: an UP store is not lost; an epoch below the fence (nothing is made at the new path) */
    CHECK(chunk_be_replace(w.be, 0, np, newid, NULL, &rep) == TSDB_ERR_INVALID);
    chunk_heal_opts low = {.epoch = 3};
    CHECK(chunk_be_replace(w.be, t, np, newid, &low, &rep) == CHUNK_ERR_FENCED && access(np, F_OK) != 0);
    watch x = {.be = w.be, .w = &w, .store = t, .fence = 5};
    chunk_heal_opts ho = {.epoch = 5, .live = probe_live, .live_ctx = &x};
    CHECK(chunk_be_replace(w.be, t, np, newid, &ho, &rep) == 0);
    CHECK(chunk_be_store_state(w.be, t) == CHUNK_STORE_UP);
    CHECK(x.calls >= good.n && x.bad == 0 && x.replacing_reads == x.calls);
    CHECK(rep.rebuilt_missing == (uint64_t)good.n && rep.failed == 0 && rep.unrecoverable == 0 && rep.unavailable == 0);
    CHECK(imgs_diff(&good, np) == 0 && count_chunks(np) == good.n);
    CHECK(memcmp(chunk_store_id(chunk_be_store(w.be, t)), newid, 16) == 0);
    CHECK(chunk_store_epoch(chunk_be_store(w.be, t), VOL) == 5);
    CHECK(chunk_be_replace(w.be, t, np, newid, &ho, &rep) == TSDB_ERR_INVALID); /* it is UP now */
    /* a directory that has a META is not a new store */
    CHECK(chunk_store_init(w.path[1], NULL, NULL) == TSDB_ERR_INVALID);
    world_close(&w);
    /* the new store alone holds its positions: take two others down (the new store is in the place of the old) and read */
    strcpy(w.path[t], np);
    memcpy(w.ids + 16 * t, newid, 16);
    for (int a = 0; a < NST; a++)
        for (int b = a + 1; b < NST; b++) {
            if (a == t || b == t)
                continue;
            CHECK(world_open(&w, 1u << a | 1u << b, NST - 2, NULL) == 0);
            CHECK(reads_exact(w.be, &w));
            world_close(&w);
        }
    imgs_free(&good);
    world_free(&w);
    printf("  replace: a lost store replaced at a new path: fence before the first chunk, reads go around the fill, bit-exact, alone sufficient\n");
}

static void test_attach(void)
{
    world w;
    world_build(&w, "attach");
    int t = 1;
    imgs old;
    imgs_take(&old, w.path[t]);
    world_close(&w);
    CHECK(world_open(&w, 1u << t, NST - 1, NULL) == 0);
    /* objects written while the store was away: only the positions placed elsewhere; and a fence raised meanwhile */
    obj extra[3];
    for (int i = 0; i < 3; i++) {
        obj_make(&extra[i], 4, 2, 64, 64 * 4 * 2 + (uint64_t)i * 9);
        CHECK(chunk_be_see_epoch(w.be, VOL, 6) == 0);
        unsigned mask = ~0u;
        int pt = pos_on(&extra[i], t);
        if (pt >= 0)
            mask &= ~(1u << pt);
        CHECK(put_obj(w.be, &extra[i], 6, mask) == 0);
    }
    CHECK(chunk_be_put(w.be, t, &(chunk_ident){.volume = VOL, .vuuid = {0}, .oid = {1}, .pos = 0}, &extra[0].g, 6,
                       extra[0].data, NULL) == TSDB_ERR_UNAVAILABLE); /* a put to a DOWN store is refused */
    world ew = w;
    memcpy(ew.o, extra, sizeof extra);
    ew.no = 3;
    watch x = {.be = w.be, .w = &ew, .store = t, .fence = 6};
    chunk_heal_report rep;
    chunk_heal_opts ho = {.epoch = 6, .live = probe_live, .live_ctx = &x};
    /* a store with another id is refused, never attached */
    uint8_t wrong[16];
    memcpy(wrong, w.ids + 16 * t, 16);
    wrong[0] ^= 1;
    CHECK(chunk_be_attach(w.be, t, w.path[t], wrong, &ho, &rep) == CHUNK_ERR_STORE_ID);
    CHECK(chunk_be_store_state(w.be, t) == CHUNK_STORE_DOWN_MISSING);
    CHECK(chunk_be_attach(w.be, 0, w.path[t], NULL, &ho, &rep) == TSDB_ERR_INVALID); /* index 0 is UP */
    CHECK(chunk_be_attach(w.be, t, w.path[t], w.ids + 16 * t, &ho, &rep) == 0);
    CHECK(chunk_be_store_state(w.be, t) == CHUNK_STORE_UP);
    CHECK(x.bad == 0 && x.calls > 0);
    int mine = 0;
    for (int i = 0; i < 3; i++)
        mine += pos_on(&extra[i], t) >= 0;
    CHECK(rep.rebuilt_missing == (uint64_t)mine);
    CHECK(imgs_diff(&old, w.path[t]) == 0); /* what it had is untouched */
    CHECK(chunk_store_epoch(chunk_be_store(w.be, t), VOL) == 6); /* the epochs it missed */
    world_close(&w);
    /* the chunks it was given are real: take two others down and read everything */
    for (int a = 0; a < NST; a++)
        for (int b = a + 1; b < NST; b++) {
            if (a == t || b == t)
                continue;
            CHECK(world_open(&w, 1u << a | 1u << b, NST - 2, NULL) == 0);
            CHECK(reads_exact(w.be, &w) && reads_exact(w.be, &ew));
            world_close(&w);
        }
    imgs_free(&old);
    for (int i = 0; i < 3; i++)
        free(extra[i].data);
    ew.no = 0;
    world_free(&w);
    printf("  attach: a store that was away (puts refused meanwhile) is filled before it is UP, wrong id refused\n");
}

/* ---------------------------------------------------------------- power loss */
static int run_child(const char *dir, const char *mode, int warmup, int seed, int t)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], wbuf[16], sd[16], tb[8];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(wbuf, sizeof wbuf, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        snprintf(tb, sizeof tb, "%d", t);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", wbuf, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0)
            dup2(null, 1), dup2(null, 2);
        execl(SELF, "test_chunk_heal", mode, dir, tb, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int child_open(const char *dir, chunk_backend **be)
{
    char p[NST][PATH_MAX + 96];
    const char *pp[NST];
    for (int i = 0; i < NST; i++) {
        snprintf(p[i], sizeof p[i], "%s/st%d", dir, i);
        pp[i] = p[i];
    }
    chunk_be_opts o = {.min_up = NST - 2};
    return chunk_be_open2(be, pp, NST, NULL, &o);
}

static int child_heal(const char *dir, int t)
{
    chunk_backend *be;
    if (child_open(dir, &be) != 0)
        return 2;
    chunk_heal_report rep;
    chunk_heal_opts ho = {.epoch = 5, .scrub = true};
    int rc = chunk_be_heal(be, VOL, t, &ho, &rep);
    chunk_be_close(be);
    return rc == 0 ? 0 : 3;
}

static int child_replace(const char *dir, int t)
{
    chunk_backend *be;
    if (child_open(dir, &be) != 0)
        return 2;
    char np[PATH_MAX + 96];
    snprintf(np, sizeof np, "%s/new_t", dir);
    uint8_t id[16];
    chunk_heal_report rep;
    chunk_heal_opts ho = {.epoch = 5};
    int rc = chunk_be_replace(be, t, np, id, &ho, &rep);
    chunk_be_close(be);
    return rc == 0 ? 0 : 3;
}

static void sweep_heal(int stride)
{
    int steps = 0, crashes = 0;
    bool finished = false;
    for (int k = 0; k < 8000 && !finished; k += stride) {
        world w;
        world_build(&w, "sweepheal");
        int t = 3;
        imgs good, pre;
        imgs_take(&good, w.path[t]);
        world_close(&w);
        char p[3 * PATH_MAX];
        snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[0].name);
        damage(p, D_DELETE);
        snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[1].name);
        damage(p, D_CELL);
        snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[2].name);
        damage(p, D_TRUNC);
        imgs_take(&pre, w.path[t]);
        int code = run_child(w.dir, "--child-heal", k, k + 1, t);
        if (code != 0 && code != 99) {
            fprintf(ERR, "  heal power step %d: the child exited with %d\n", k, code);
            CHECK(false);
            break;
        }
        crashes += code == 99;
        steps++;
        int before = failures;
        /* every file of the store is whole (its good image) or what it was before the heal: never a torn new one */
        for (int i = 0; i < good.n; i++) {
            snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[i].name);
            size_t n = 0;
            uint8_t *b = slurp(p, &n);
            bool is_good = b && n == good.v[i].n && memcmp(b, good.v[i].b, n) == 0, was = false;
            for (int j = 0; j < pre.n && !was; j++)
                was = strcmp(pre.v[j].name, good.v[i].name) == 0 && b && n == pre.v[j].n && memcmp(b, pre.v[j].b, n) == 0;
            if (!is_good && !was && !(i == 0 && !b)) {
                fprintf(ERR, "  heal power step %d: %s is neither its old image nor the whole new one\n", k, good.v[i].name);
                CHECK(false);
            }
            free(b);
        }
        /* a second heal completes */
        CHECK(world_open(&w, 0, NST - 2, NULL) == 0);
        chunk_heal_report rep;
        chunk_heal_opts ho = {.epoch = 5, .scrub = true};
        CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0 && rep.failed == 0 && rep.unrecoverable == 0);
        CHECK(imgs_diff(&good, w.path[t]) == 0 && reads_exact(w.be, &w));
        world_free(&w);
        imgs_free(&good);
        imgs_free(&pre);
        if (failures != before)
            break;
        finished = code == 0;
    }
    CHECK(finished && crashes > 10);
    printf("  heal power loss: %d fault points, a chunk is its old image or the whole new one, a second heal completes\n", steps);
}

static void sweep_replace(int stride)
{
    int steps = 0, crashes = 0, with_chunks = 0;
    bool finished = false;
    for (int k = 0; k < 8000 && !finished; k += stride) {
        world w;
        world_build(&w, "sweeprepl");
        int t = 3;
        imgs good;
        imgs_take(&good, w.path[t]);
        world_close(&w);
        sh("rm -rf '%s'", w.path[t]);
        int code = run_child(w.dir, "--child-replace", k, k + 1, t);
        if (code != 0 && code != 99) {
            fprintf(ERR, "  replace power step %d: the child exited with %d\n", k, code);
            CHECK(false);
            break;
        }
        crashes += code == 99;
        steps++;
        int before = failures;
        char np[PATH_MAX + 64];
        snprintf(np, sizeof np, "%s/new_t", w.dir);
        chunk_store *st = NULL;
        if (chunk_store_open_quiet(np, NULL, &st) == 0) {
            /* a half-made store: what chunks it has are whole, and it holds the fence before it holds any chunk */
            imgs have;
            imgs_take(&have, np);
            for (int i = 0; i < have.n; i++) {
                bool ok = false;
                for (int j = 0; j < good.n && !ok; j++)
                    ok = strcmp(good.v[j].name, have.v[i].name) == 0 && good.v[j].n == have.v[i].n &&
                         memcmp(good.v[j].b, have.v[i].b, have.v[i].n) == 0;
                if (!ok) {
                    fprintf(ERR, "  replace power step %d: %s in the new store is not a whole chunk\n", k, have.v[i].name);
                    CHECK(false);
                }
            }
            if (have.n > 0) {
                with_chunks++;
                if (chunk_store_epoch(st, VOL) < 5) {
                    fprintf(ERR, "  replace power step %d: the new store holds a chunk and EPOCH %llu below the fence\n", k,
                            (unsigned long long)chunk_store_epoch(st, VOL));
                    CHECK(false);
                }
            }
            imgs_free(&have);
            chunk_store_close(st);
        }
        /* the caller replaces again at another path */
        CHECK(world_open(&w, 0, NST - 2, NULL) == 0);
        char np2[PATH_MAX + 64];
        snprintf(np2, sizeof np2, "%s/new_t2", w.dir);
        uint8_t id[16];
        chunk_heal_report rep;
        chunk_heal_opts ho = {.epoch = 5};
        CHECK(chunk_be_replace(w.be, t, np2, id, &ho, &rep) == 0);
        CHECK(imgs_diff(&good, np2) == 0 && reads_exact(w.be, &w));
        world_free(&w);
        imgs_free(&good);
        if (failures != before)
            break;
        finished = code == 0;
    }
    CHECK(finished && crashes > 10 && with_chunks > 0);
    printf("  replace power loss: %d fault points, a half-made store has whole chunks and its fence first, a second replace completes\n", steps);
}

static void test_crashfs(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    sweep_heal(quick ? 6 : 1);
    sweep_replace(quick ? 6 : 1);
}

/* ---------------------------------------------------------------- threads */
typedef struct th {
    world *w;
    atomic_int stop;
    atomic_long errors, reads;
    obj *extra;
    int nextra;
    atomic_int written;
} th;

static void *th_reader(void *arg)
{
    th *t = arg;
    uint8_t *buf = malloc(4096);
    while (!atomic_load(&t->stop))
        for (int i = 0; i < t->w->no && !atomic_load(&t->stop); i++) {
            const obj *o = &t->w->o[i];
            int rc = read_obj(t->w->be, o, buf);
            atomic_fetch_add(&t->reads, 1);
            if (rc != 0 || memcmp(buf, o->data, (size_t)o->g.len) != 0)
                atomic_fetch_add(&t->errors, 1);
        }
    free(buf);
    return NULL;
}

static void *th_writer(void *arg)
{
    th *t = arg;
    for (int i = 0; i < t->nextra; i++) {
        if (put_obj(t->w->be, &t->extra[i], 5, ~0u) != 0)
            atomic_fetch_add(&t->errors, 1);
        atomic_fetch_add(&t->written, 1);
    }
    return NULL;
}

static void test_threads(void)
{
    world w;
    world_build(&w, "threads");
    int t = 0;
    imgs good;
    imgs_take(&good, w.path[t]);
    th s;
    memset(&s, 0, sizeof s);
    s.w = &w;
    s.nextra = quick ? 20 : 60;
    s.extra = calloc((size_t)s.nextra, sizeof *s.extra);
    for (int i = 0; i < s.nextra; i++)
        obj_make(&s.extra[i], 4, 2, 64, 64 * 4 * 2 + 5);
    pthread_t rt[3], wt;
    for (int i = 0; i < 3; i++)
        CHECK(pthread_create(&rt[i], NULL, th_reader, &s) == 0);
    CHECK(pthread_create(&wt, NULL, th_writer, &s) == 0);
    chunk_heal_opts ho = {.epoch = 5, .scrub = true};
    int rounds = 0;
    while (atomic_load(&s.written) < s.nextra && rounds < 200) {
        for (int i = 0; i < good.n; i++) { /* damage, then heal against the readers and the writer */
            char p[3 * PATH_MAX];
            snprintf(p, sizeof p, "%s/s%u/%s", w.path[t], VOL, good.v[i].name);
            if ((i + rounds) % 3 == 0)
                damage(p, (i + rounds) % D_KINDS);
        }
        chunk_heal_report rep;
        CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0); /* a put in flight may be rebuilt twice or look short: only the end counts */
        rounds++;
    }
    pthread_join(wt, NULL);
    atomic_store(&s.stop, 1);
    for (int i = 0; i < 3; i++)
        pthread_join(rt[i], NULL);
    chunk_heal_report rep;
    CHECK(chunk_be_heal(w.be, VOL, t, &ho, &rep) == 0 && rep.failed == 0 && rep.unrecoverable == 0 && rep.unavailable == 0);
    CHECK(imgs_diff(&good, w.path[t]) == 0 && reads_exact(w.be, &w));
    for (int i = 0; i < s.nextra; i++)
        CHECK(read_is(w.be, &s.extra[i], 0));
    CHECK(atomic_load(&s.errors) == 0 && atomic_load(&s.reads) > 0);
    for (int i = 0; i < s.nextra; i++)
        free(s.extra[i].data);
    free(s.extra);
    imgs_free(&good);
    world_free(&w);
    printf("  threads: %d heal rounds against 3 readers and a writer of %d objects: reads exact throughout, the end whole\n",
           rounds, s.nextra);
}

/* ---------------------------------------------------------------- main */
static void set_mutant(const char *m)
{
    if (!strcmp(m, "heal_noverify"))
        chunk_mutant = CHUNK_MUT_HEAL_NOVERIFY;
    else if (!strcmp(m, "heal_epoch0"))
        chunk_mutant = CHUNK_MUT_HEAL_EPOCH0;
    else if (!strcmp(m, "replace_late"))
        chunk_mutant = CHUNK_MUT_REPLACE_LATE;
    else if (!strcmp(m, "open_busy_down"))
        chunk_mutant = CHUNK_MUT_OPEN_BUSY_DOWN;
    else if (!strcmp(m, "open_id_up"))
        chunk_mutant = CHUNK_MUT_OPEN_ID_UP;
    else if (!strcmp(m, "heal_kminus1"))
        chunk_mutant = CHUNK_MUT_HEAL_KMINUS1;
    else if (!strcmp(m, "heal_resurrect"))
        chunk_mutant = CHUNK_MUT_HEAL_RESURRECT;
    else {
        fprintf(stderr, "test_chunk_heal: no mutant %s\n", m);
        exit(2);
    }
}

int main(int argc, char **argv)
{
    if (getenv("TSDB_CHUNK_MUTANT"))
        set_mutant(getenv("TSDB_CHUNK_MUTANT"));
    if (argc >= 4 && strcmp(argv[1], "--child-heal") == 0)
        return child_heal(argv[2], atoi(argv[3]));
    if (argc >= 4 && strcmp(argv[1], "--child-replace") == 0)
        return child_replace(argv[2], atoi(argv[3]));
    const char *mutant = NULL, *base = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = true;
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else
            base = argv[i];
    }
    if (getenv("TSDB_CHUNK_QUICK"))
        quick = true;
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/tsdb-chunk-heal-XXXXXX", base);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    if (mutant) {
        set_mutant(mutant);
        setenv("TSDB_CHUNK_MUTANT", mutant, 1);
    }
    printf("test_chunk_heal%s%s\n", quick ? " (quick)" : "", mutant ? " (mutant)" : "");
    fflush(stdout);
    ERR = fdopen(dup(2), "w");
    setvbuf(ERR, NULL, _IONBF, 0);
    if (!getenv("TSDB_HEAL_VERBOSE")) { /* the library's log lines, which every test here provokes */
        devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0)
            dup2(devnull, 2);
    }
    test_open_degraded();
    test_open_refusals();
    test_reads_down();
    test_heal_damage();
    test_heal_refusals();
    test_replace();
    test_attach();
    test_threads();
    test_crashfs();
    if (failures) {
        fprintf(ERR, "test_chunk_heal: %d check(s) failed (scratch %s)\n", failures, S);
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_chunk_heal: ok\n");
    return 0;
}
#endif
