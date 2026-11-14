/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The erasure codec of src/ec (intellistream-objects' src/codec, copied by tools/sync-ec.sh): GF(2^8) arithmetic and
 * systematic Cauchy Reed-Solomon.
 *
 * What is checked, at every CPU level this CPU has (cpu_set_level) and with every GF(256) kernel that level allows
 * (iso_gf256_kernels: the scalar one, SSSE3, AVX2, AVX-512, the two GFNI ones), so a part written on one machine is
 * decoded by another to the same bytes:
 *
 *  - the field: every product against a carry-less multiply written here (not the library's tables), inverses, powers;
 *  - `dst ^= c * src` for all 256 coefficients, every length 0..130 and 4099 bytes, aligned and not, and the bytes
 *    around the destination untouched;
 *  - encode against an oracle written here from the documented generator (parity row i, column j = 1 / ((k + i) ^ j),
 *    the bytes that live on the platter) for 4+2, 8+2, 8+4, 16+4 and the small k that a tier of 3 to 6 stores gets
 *    (2+1, 3+1, 4+1, 5+1, 3+2, and 1+1, 16+1); longer shards against the scalar kernel, which the short ones tie to
 *    the oracle;
 *  - decode of every erasure pattern of 0..m shards, bit exact, for decode, decode_data and both kinds of plan: all
 *    patterns of every geometry at short lengths (16+4: 6,196 of them), sampled ones at longer lengths, and m + 1
 *    erasures refused with ISO_ERR_RANGE;
 *  - lengths from 1 byte to 4 MiB + 5, shards at unaligned addresses, canaries around every shard;
 *  - the parity of fixed data against pinned checksums (the format must not move at a sync);
 *  - the dispatch: a level never lets a kernel run that needs instructions above it (TSDB_CPU=v2 must not execute an
 *    AVX instruction: the qemu SandyBridge run of `make dist-check-sandybridge` executes this test).
 *
 * Not the codec's job, and so not checked as a property of it: noticing wrong input. A shard that is present but not
 * what was encoded decodes to wrong output with ISO_OK; the caller's hashes find that (wrong_input_is_not_detected
 * shows it, so that nobody builds on the opposite).
 *
 *   test_rs [DIR]                          DIR is the scratch directory of the other tests and is not used
 *   test_rs --mutant coef|kernel|guard     plants a bug the test must fail on (the Makefile requires it)
 *   test_rs --print                        prints the pinned checksums
 *   test_rs --quick, or TSDB_RS_QUICK=1    fewer lengths and no shard above 64 KiB, for qemu
 */
#include "internal.h"

#ifndef TSDB_EC /* Apple silicon and other machines that are not x86-64: the Makefile builds no src/ec there */
#include <stdio.h>
int main(void)
{
    printf("test_rs: SKIPPED (this build has no erasure codec: it is x86-64 only)\n");
    return 0;
}
#else
#include "ec/gf256.h"
#include "ec/rs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            if (++failures > 40)                                                     \
                exit(1);                                                             \
        }                                                                            \
    } while (0)

static const char *mutant = "";
static int print_sums, quick; /* quick: for an emulated CPU (qemu-user is 10 to 50 times slower) */

/* ---- the oracle: GF(2^8) by carry-less multiplication and reduction by 0x11d, nothing shared with the library ---- */

static uint8_t smul(uint8_t a, uint8_t b)
{
    unsigned r = 0, x = a;
    for (unsigned i = 0; i < 8; i++)
        if (b >> i & 1)
            r ^= x << i;
    for (int bit = 14; bit >= 8; bit--)
        if (r >> bit & 1)
            r ^= 0x11dU << (bit - 8);
    return (uint8_t)r;
}

static uint8_t sinv(uint8_t a)
{
    for (unsigned b = 1; b < 256; b++)
        if (smul(a, (uint8_t)b) == 1)
            return (uint8_t)b;
    return 0;
}

static uint8_t mt[256][256]; /* products, from smul */

static uint8_t oracle_coef(unsigned k, unsigned i, unsigned j) { return sinv((uint8_t)((k + i) ^ j)); }

/* ---- data ---- */

static uint64_t rs_state;
static uint64_t rnd(void)
{
    rs_state ^= rs_state << 13;
    rs_state ^= rs_state >> 7;
    rs_state ^= rs_state << 17;
    return rs_state;
}
static void fill(uint8_t *p, size_t n, uint64_t seed)
{
    rs_state = seed * 0x9e3779b97f4a7c15ULL + 1;
    for (size_t i = 0; i < n; i++)
        p[i] = (uint8_t)(rnd() >> 24);
}

#define PAD_BEFORE 64
#define PAD_AFTER 192
#define CANARY 0xA5

/* A shard at an address that is not 64-byte aligned, and not the same offset in any two shards, with canaries
 * around it that no kernel may touch. */
typedef struct {
    uint8_t *raw, *p, *end; /* end: the end of the allocation */
    size_t len;
} shard;

static void shard_new(shard *s, size_t len, unsigned idx)
{
    size_t off = PAD_BEFORE + (idx * 13 + 1) % 47;
    s->len = len;
    s->raw = malloc(off + len + PAD_AFTER);
    CHECK(s->raw);
    memset(s->raw, CANARY, off + len + PAD_AFTER);
    s->p = s->raw + off;
    s->end = s->raw + off + len + PAD_AFTER;
    memset(s->p, 0, len);
}
static void shard_free(shard *s) { free(s->raw); }
static bool canary_ok(const shard *s)
{
    for (const uint8_t *q = s->raw; q < s->p; q++)
        if (*q != CANARY)
            return false;
    for (const uint8_t *q = s->p + s->len; q < s->end; q++)
        if (*q != CANARY)
            return false;
    return true;
}

/* ---- kernels: the mutants ---- */

static void planted_kernel(uint8_t *dst, const uint8_t *src, size_t len, uint8_t c)
{
    iso_gf256_mul_add_scalar(dst, src, len, c);
    if (!strcmp(mutant, "kernel") && len > 70)
        dst[len - 3] ^= 1; /* a wrong byte in the tail of every long run */
    if (!strcmp(mutant, "guard") && len > 5)
        dst[len] ^= 0x80; /* one byte past the end */
}

/* mul_add against the table oracle, with the destination's surroundings checked */
static void check_mul_add(const iso_gf256_kernel *k, size_t len, unsigned c, unsigned doff, unsigned soff)
{
    enum { MAXL = 4099 + 64 };
    static uint8_t srcb[MAXL + 16], dstb[MAXL + 16 + 192], want[MAXL + 16 + 192];
    for (size_t i = 0; i < sizeof srcb; i++)
        srcb[i] = (uint8_t)(i * 37 + c + 11);
    memset(dstb, CANARY, sizeof dstb);
    for (size_t i = 0; i < len; i++)
        dstb[doff + i] = (uint8_t)(i * 101 + 7);
    memcpy(want, dstb, sizeof want);
    for (size_t i = 0; i < len; i++)
        want[doff + i] ^= mt[c][srcb[soff + i]];
    k->fn(dstb + doff, srcb + soff, len, (uint8_t)c);
    if (memcmp(dstb, want, doff + len + 100) != 0) {
        fprintf(stderr, "kernel %s: mul_add by %02x, length %zu, dst+%u src+%u differs\n", k->name, c, len, doff, soff);
        CHECK(0);
    }
}

static void check_kernel_mul_add(const iso_gf256_kernel *k)
{
    for (unsigned c = 0; c < 256; c++) {
        for (size_t len = 0; len <= 130; len++) {
            check_mul_add(k, len, c, 0, 0);
            if (c & 1)
                check_mul_add(k, len, c, 5, 3);
        }
        check_mul_add(k, 4099, c, 3, 7);
    }
}

/* ---- one geometry ---- */

typedef struct {
    unsigned k, m, n;
    shard sh[ISO_RS_MAX_SHARDS]; /* the encoded stripe */
    shard w[ISO_RS_MAX_SHARDS];  /* the one that decode works on */
    size_t len;
} stripe;

static void stripe_new(stripe *s, unsigned k, unsigned m, size_t len, uint64_t seed)
{
    s->k = k, s->m = m, s->n = k + m, s->len = len;
    for (unsigned i = 0; i < s->n; i++) {
        shard_new(&s->sh[i], len, i);
        shard_new(&s->w[i], len, i + 5);
    }
    for (unsigned i = 0; i < k; i++)
        fill(s->sh[i].p, len, seed * 64 + i);
}
static void stripe_free(stripe *s)
{
    for (unsigned i = 0; i < s->n; i++) {
        shard_free(&s->sh[i]);
        shard_free(&s->w[i]);
    }
}

static void encode(const iso_rs *rs, stripe *s)
{
    uint8_t *p[ISO_RS_MAX_SHARDS];
    for (unsigned i = 0; i < s->n; i++)
        p[i] = s->sh[i].p;
    iso_rs_encode(rs, p, s->len);
}

/* parity of the data by the documented formula */
static bool oracle_parity_ok(const stripe *s)
{
    for (unsigned i = 0; i < s->m; i++) {
        for (size_t b = 0; b < s->len; b++) {
            uint8_t v = 0;
            for (unsigned j = 0; j < s->k; j++)
                v ^= mt[oracle_coef(s->k, i, j)][s->sh[j].p[b]];
            if (s->sh[s->k + i].p[b] != v) {
                fprintf(stderr, "parity %u of %u+%u, byte %zu of %zu: have %02x, formula says %02x\n", i, s->k, s->m, b,
                        s->len, s->sh[s->k + i].p[b], v);
                return false;
            }
        }
    }
    return true;
}

/* decode one erasure pattern (bit i set: shard i absent) four ways, each compared with the encoded stripe */
static void check_pattern(const iso_rs *rs, stripe *s, unsigned mask, bool want_ok)
{
    bool present[ISO_RS_MAX_SHARDS];
    uint8_t *w[ISO_RS_MAX_SHARDS];
    for (int way = 0; way < 4; way++) {
        if (s->len >= 65536 && (way == 1 || way == 2))
            continue; /* long shards: two ways are enough (decode, and a data-only plan) */
        for (unsigned i = 0; i < s->n; i++) {
            present[i] = !(mask >> i & 1);
            w[i] = s->w[i].p;
            if (present[i])
                memcpy(w[i], s->sh[i].p, s->len);
            else
                memset(w[i], 0xEE, s->len);
        }
        bool data_only = way == 1 || way == 3;
        iso_err rc;
        if (way < 2) {
            rc = data_only ? iso_rs_decode_data(rs, w, present, s->len) : iso_rs_decode(rs, w, present, s->len);
        } else {
            iso_rs_plan *pl = iso_rs_plan_new(rs, present, data_only);
            CHECK((pl != NULL) == want_ok);
            rc = ISO_ERR_RANGE;
            if (pl) {
                iso_rs_plan_apply(pl, w, s->len);
                iso_rs_plan_free(pl);
                rc = ISO_OK;
            }
        }
        if (!want_ok) {
            CHECK(rc == ISO_ERR_RANGE);
            continue;
        }
        CHECK(rc == ISO_OK);
        for (unsigned i = 0; i < s->n; i++) {
            bool untouched = i >= s->k && !present[i] && data_only; /* decode_data leaves absent parity alone */
            if (untouched) {
                bool left = true;
                for (size_t b = 0; b < s->len; b++)
                    left &= w[i][b] == 0xEE;
                CHECK(left);
            } else if (memcmp(w[i], s->sh[i].p, s->len) != 0) {
                fprintf(stderr, "%u+%u len %zu, absent mask %05x, way %d: shard %u differs\n", s->k, s->m, s->len, mask,
                        way, i);
                CHECK(0);
                return;
            }
            CHECK(canary_ok(&s->w[i]));
        }
    }
}

static unsigned popcount(unsigned x) { return (unsigned)__builtin_popcount(x); }

static unsigned total_patterns;

/* every pattern of at most m absent shards, or the worst ones and `sample` random ones (0: all) */
static void check_patterns(const iso_rs *rs, stripe *s, unsigned sample, uint64_t seed)
{
    unsigned n = s->n;
    if (!sample) {
        for (unsigned mask = 0; mask < 1u << n; mask++)
            if (popcount(mask) <= s->m) {
                check_pattern(rs, s, mask, true);
                total_patterns++;
            }
    } else {
        rs_state = seed | 1;
        /* the worst ones first: the m first shards (data lost), the m last (parity lost), every other one, none */
        unsigned first = (1u << s->m) - 1, last = first << (n - s->m), mid = 0;
        for (unsigned i = 0; i < s->m; i++)
            mid |= 1u << ((i * 2 + 1) % n);
        unsigned fixed[] = {0, first, last, mid};
        for (unsigned i = 0; i < sizeof fixed / sizeof *fixed; i++) {
            check_pattern(rs, s, fixed[i], true);
            total_patterns++;
        }
        for (unsigned t = 0; t < sample; t++) {
            unsigned mask = 0, want = (unsigned)(rnd() >> 33) % (s->m + 1);
            while (popcount(mask) < want)
                mask |= 1u << ((unsigned)(rnd() >> 33) % n);
            check_pattern(rs, s, mask, true);
            total_patterns++;
        }
    }
    /* one more absent than the code can lose: refused */
    if (s->m + 1 <= n) {
        unsigned mask = (1u << (s->m + 1)) - 1;
        check_pattern(rs, s, mask, false);
        check_pattern(rs, s, mask << (n - s->m - 1), false);
    }
}

typedef struct {
    unsigned k, m;
} geom;
static const geom geoms[] = {{4, 2}, {8, 2}, {8, 4}, {16, 4}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {3, 2}, {1, 1}, {16, 1}};

static void plant_coef(iso_rs *rs, unsigned k)
{
    if (!strcmp(mutant, "coef"))
        ((uint8_t *)iso_rs_matrix_row(rs, k))[k > 1 ? 1 : 0] ^= 1; /* a flipped generator coefficient */
}

static void check_geometry(const geom *g, size_t len, unsigned sample, bool oracle)
{
    iso_rs *rs = iso_rs_new(g->k, g->m);
    CHECK(rs != NULL);
    if (!rs)
        return;
    plant_coef(rs, g->k);
    stripe s;
    stripe_new(&s, g->k, g->m, len, g->k * 1000 + g->m * 10 + len);
    encode(rs, &s);
    for (unsigned i = 0; i < s.n; i++)
        CHECK(canary_ok(&s.sh[i]));
    if (oracle)
        CHECK(oracle_parity_ok(&s));
    check_patterns(rs, &s, sample, len * 7 + g->k);
    stripe_free(&s);
    iso_rs_free(rs);
}

/* long shards: the stripe under the kernel under test against the scalar kernel's, then a few patterns */
static void check_long(const iso_gf256_kernel *kn, const geom *g, size_t len)
{
    iso_rs *rs = iso_rs_new(g->k, g->m);
    stripe a, b;
    plant_coef(rs, g->k);
    stripe_new(&a, g->k, g->m, len, 77 + g->k);
    stripe_new(&b, g->k, g->m, len, 77 + g->k);
    encode(rs, &a); /* under the kernel under test */
    iso_gf256_use(NULL);
    encode(rs, &b); /* the scalar kernel */
    iso_gf256_use(kn);
    for (unsigned i = 0; i < a.n; i++) {
        CHECK(memcmp(a.sh[i].p, b.sh[i].p, len) == 0);
        CHECK(canary_ok(&a.sh[i]));
    }
    check_patterns(rs, &a, 1, len);
    stripe_free(&a);
    stripe_free(&b);
    iso_rs_free(rs);
}

/* ---- the pinned parity of fixed data: the format on the platter ---- */

static uint64_t fnv(const uint8_t *p, size_t n, uint64_t h)
{
    for (size_t i = 0; i < n; i++)
        h = (h ^ p[i]) * 0x100000001b3ULL;
    return h;
}

static const struct {
    unsigned k, m;
    uint64_t sum;
} golden[] = {
    {4, 2, 0xa80a5cbdcd044c7dULL},
    {8, 2, 0xf30f2040506e8a50ULL},
    {8, 4, 0xd66694200dc0a874ULL},
    {16, 4, 0x4619556a0dd334eaULL},
    {2, 1, 0xf59b17b145425ea4ULL},
    {3, 1, 0xa10fb498f75873e3ULL},
    {4, 1, 0x5eba5a5d276e7e36ULL},
    {5, 1, 0x1d1cf75cc7b6de56ULL},
    {3, 2, 0xf070834546fb72fbULL},
    {1, 1, 0xe7583cee9e275a19ULL},
    {16, 1, 0xbb68ceb24c1a8e67ULL}
};

static uint64_t golden_sum(unsigned k, unsigned m)
{
    enum { LEN = 1009 };
    iso_rs *rs = iso_rs_new(k, m);
    plant_coef(rs, k);
    stripe s;
    stripe_new(&s, k, m, LEN, 0x5eed + k * 31 + m);
    encode(rs, &s);
    uint64_t h = 0xcbf29ce484222325ULL;
    for (unsigned i = k; i < k + m; i++)
        h = fnv(s.sh[i].p, LEN, h);
    stripe_free(&s);
    iso_rs_free(rs);
    return h;
}

static void check_golden(void)
{
    static int printed;
    for (size_t i = 0; i < sizeof golden / sizeof *golden; i++) {
        uint64_t h = golden_sum(golden[i].k, golden[i].m);
        if (print_sums) {
            if (i == 0 && printed++)
                return;
            printf("    {%u, %u, 0x%016llxULL},\n", golden[i].k, golden[i].m, (unsigned long long)h);
        } else if (h != golden[i].sum) {
            fprintf(stderr, "%u+%u: the parity of the fixed data is %016llx, pinned %016llx: the code on the platter moved\n",
                    golden[i].k, golden[i].m, (unsigned long long)h, (unsigned long long)golden[i].sum);
            CHECK(0);
        }
    }
}

/* ---- wrong input is not the codec's job ---- */

static void wrong_input_is_not_detected(void)
{
    iso_rs *rs = iso_rs_new(4, 2);
    stripe s;
    stripe_new(&s, 4, 2, 100, 9);
    encode(rs, &s);
    bool present[6] = {true, true, true, false, true, true};
    uint8_t *w[6];
    for (unsigned i = 0; i < 6; i++) {
        w[i] = s.w[i].p;
        memcpy(w[i], s.sh[i].p, 100);
    }
    w[1][17] ^= 0x40; /* a present shard that is not what was encoded */
    iso_err rc = iso_rs_decode(rs, w, present, 100);
    CHECK(rc == ISO_OK);                      /* no error: the codec cannot know */
    CHECK(memcmp(w[3], s.sh[3].p, 100) != 0); /* the rebuilt shard is wrong; the caller's hash would say so */
    stripe_free(&s);
    iso_rs_free(rs);
}

static void check_construction(void)
{
    CHECK(iso_rs_new(0, 1) == NULL);
    CHECK(iso_rs_new(1, 0) == NULL);
    CHECK(iso_rs_new(ISO_RS_MAX_K + 1, 1) == NULL);
    CHECK(iso_rs_new(1, ISO_RS_MAX_M + 1) == NULL);
    iso_rs *rs = iso_rs_new(8, 4);
    CHECK(iso_rs_k(rs) == 8 && iso_rs_m(rs) == 4);
    for (unsigned i = 0; i < 8; i++)
        for (unsigned j = 0; j < 8; j++)
            CHECK(iso_rs_matrix_row(rs, i)[j] == (i == j));
    for (unsigned i = 0; i < 4; i++)
        for (unsigned j = 0; j < 8; j++)
            CHECK(iso_rs_matrix_row(rs, 8 + i)[j] == oracle_coef(8, i, j));
    iso_rs_free(rs);
}

/* ---- the field ---- */

static void check_field(void)
{
    for (unsigned a = 0; a < 256; a++)
        for (unsigned b = 0; b < 256; b++) {
            mt[a][b] = smul((uint8_t)a, (uint8_t)b);
            if (iso_gf256_mul((uint8_t)a, (uint8_t)b) != mt[a][b] || iso_gf256_row((uint8_t)a)[b] != mt[a][b]) {
                fprintf(stderr, "gf256 %02x * %02x: %02x, expected %02x\n", a, b, iso_gf256_mul((uint8_t)a, (uint8_t)b),
                        mt[a][b]);
                CHECK(0);
            }
        }
    CHECK(smul(0x80, 2) == 0x1d && smul(0xff, 2) == 0xe3 && sinv(2) == 0x8e); /* what the polynomial says */
    for (unsigned a = 1; a < 256; a++) {
        CHECK(iso_gf256_inv((uint8_t)a) == sinv((uint8_t)a));
        CHECK(iso_gf256_mul((uint8_t)a, iso_gf256_inv((uint8_t)a)) == 1);
        CHECK(iso_gf256_div(iso_gf256_mul((uint8_t)a, 0x53), (uint8_t)a) == 0x53);
        uint8_t p = 1;
        for (int n = 0; n < 6; n++) {
            CHECK(iso_gf256_pow((uint8_t)a, n) == p);
            p = smul(p, (uint8_t)a);
        }
        CHECK(iso_gf256_pow((uint8_t)a, -1) == sinv((uint8_t)a));
    }
}

static void check_mul_set(void)
{
    uint8_t src[300], dst[300], want[300];
    fill(src, sizeof src, 3);
    for (unsigned c = 0; c < 256; c += 7)
        for (size_t len = 0; len <= 300; len += (len < 70 ? 1 : 37)) {
            memset(dst, 0x77, sizeof dst);
            for (size_t i = 0; i < len; i++)
                want[i] = mt[c][src[i]];
            iso_gf256_mul_set(dst, src, len, (uint8_t)c);
            CHECK(memcmp(dst, want, len) == 0);
            for (size_t i = len; i < sizeof dst; i++)
                CHECK(dst[i] == 0x77);
        }
}

/* ---- levels and kernels ---- */

static bool is_name(const char *s, const char *p) { return !strcmp(s, p); }
static bool wide_name(const char *s) { return !strncmp(s, "avx512", 6) || is_name(s, "gfni512"); }

/* what a level may run: nothing that needs instructions above it */
static void check_level_caps(int level, const iso_gf256_kernel *ks, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (!ks[i].available)
            continue;
        if (level < CPU_V3) /* SSE only: a SandyBridge, a Westmere, a TSDB_CPU=v2 process */
            CHECK(is_name(ks[i].name, "scalar") || is_name(ks[i].name, "ssse3"));
        if (level < CPU_V4)
            CHECK(!wide_name(ks[i].name));
    }
    const char *now = iso_gf256_impl();
    if (level < CPU_V3)
        CHECK(is_name(now, "ssse3") || is_name(now, "scalar"));
    if (level < CPU_V4)
        CHECK(!wide_name(now));
}

/* The kernel the ladder must pick at `level`, worked out here from the compiler's own CPU probe (not compat.c's, so a
 * slip in the probe or the cap shows): the widest the hardware and the level allow. A kernel below that passes every
 * correctness check and costs 8 to 35 times the encode speed (docs/tasks/ec-bench.md), so the choice is checked too. */
static void check_best_kernel(int level)
{
    __builtin_cpu_init();
    bool ssse3 = __builtin_cpu_supports("ssse3");
    bool avx2 = level >= CPU_V3 && __builtin_cpu_supports("avx2");
    bool gfni = level >= CPU_V3 && __builtin_cpu_supports("gfni");
    bool bw = level >= CPU_V4 && __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw");
    bool wide = bw && tsdb_ec_prefer_512();
    const char *want = gfni && wide ? "gfni512" : gfni && avx2 ? "gfni256" : wide ? "avx512" : avx2 ? "avx2"
                       : ssse3 ? "ssse3" : "scalar";
    const char *now = iso_gf256_impl();
    if (strcmp(now, want))
        fprintf(stderr, "level %s: the ladder runs %s, the hardware allows %s\n", cpu_level_name(level), now, want);
    CHECK(is_name(now, want));
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else if (!strcmp(argv[i], "--print"))
            print_sums = 1;
        else if (!strcmp(argv[i], "--quick"))
            quick = 1;
    }
    if (getenv("TSDB_RS_QUICK") && *getenv("TSDB_RS_QUICK") == '1')
        quick = 1;
    check_field();
    check_construction();
    check_golden();
    wrong_input_is_not_detected();

    int start = cpu_level();
    {   /* the state the process starts in (cpu_pick: TSDB_CPU=v2 on a SandyBridge, say) already obeys its level */
        iso_gf256_kernel ks0[ISO_GF256_MAX_KERNELS + 1];
        check_level_caps(start, ks0, iso_gf256_kernels(ks0));
        check_best_kernel(start);
    }
    unsigned configs = 0;
    for (int level = CPU_BASE; level <= cpu_level_max(); level++) {
        if (cpu_set_level(level) != 0)
            continue;
        iso_gf256_kernel ks[ISO_GF256_MAX_KERNELS + 1];
        size_t nk = iso_gf256_kernels(ks);
        check_level_caps(level, ks, nk);
        check_best_kernel(level);
        if (!strcmp(mutant, "kernel") || !strcmp(mutant, "guard"))
            ks[nk++] = (iso_gf256_kernel){"planted", planted_kernel, true, 1, NULL};
        for (size_t ki = 0; ki < nk; ki++) {
            if (!ks[ki].available)
                continue;
            iso_gf256_use(&ks[ki]);
            configs++;
            printf("level %s, kernel %s\n", cpu_level_name(level), ks[ki].name);
            check_kernel_mul_add(&ks[ki]);
            check_mul_set();
            check_golden(); /* whichever kernel: the same bytes */
            for (size_t gi = 0; gi < sizeof geoms / sizeof *geoms; gi++) {
                const geom *g = &geoms[gi];
                bool big = g->k + g->m >= 20;
                /* every pattern at lengths on and around the vector widths, against the formula */
                static const size_t shorts[] = {1, 2, 15, 33, 67, 130};
                for (size_t li = 0; li < sizeof shorts / sizeof *shorts; li++) {
                    if (quick && li != 0 && li != 3 && li != 5)
                        continue;
                    check_geometry(g, shorts[li], big && li > 2 ? 100 : 0, true);
                }
                check_geometry(g, 300, big ? 200 : 0, true);
                check_geometry(g, 4096 + 1, 60, false);
                check_geometry(g, 65536 + 3, 6, false);
                if (!quick)
                    check_long(&ks[ki], g, 1048576 + 13);
            }
            /* the largest, once per kernel, for the two geometries that matter */
            if (!quick) {
                check_long(&ks[ki], &geoms[2], 4 * 1048576 + 5);
                check_long(&ks[ki], &geoms[3], 4 * 1048576 + 5);
            }
        }
    }
    cpu_set_level(start);
    printf("%u kernel configurations, %u erasure patterns decoded\n", configs, total_patterns);
    if (failures) {
        fprintf(stderr, "test_rs: %d failures\n", failures);
        return 1;
    }
    printf("test_rs: ok\n");
    return 0;
}
#endif /* TSDB_EC */
