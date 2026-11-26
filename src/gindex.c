/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The granule index of a part, bit-packed at fixed width and read in place from the mapping.
 *
 *   varint nruns
 *   GF_N field descriptors: u8 width, u8 (shift | kind << 6 | has_slope << 7), varint base, [varint zigzag slope]
 *   zeros to a multiple of 4
 *   u32 series[nruns], u32 start[nruns]        the runs: series[r] from entry start[r] (start[0] = 0)
 *   n entries of `stride` bytes
 *   zeros to a multiple of 8
 *
 * Field f of entry i is base + i x slope + (bits << shift), u64 arithmetic wrapping, where bits are the field's
 * `width` bits of the entry: fields wider than 57 bits take 8 bytes of their own at the start of the entry (in
 * field order), the others follow packed in field order from the next bit, so each is one unaligned 8-byte load,
 * a shift and a mask. The writer picks per part and field the frame of reference with the fewest bits: the
 * smallest value as base (unsigned or signed order), or a straight line through the first and last value (block
 * offsets and granule starts grow with i), and the trailing zero bits all differences share (sums of float32
 * values have low mantissa bits zero). A float statistic every entry of the part holds as an exact float32 is
 * stored as the float32's bits (GK_F32): widening it back gives the same double, bit for bit. The span is
 * ts_max - ts_min; the step and linear times are stored as span minus the time (usually 0: no bits).
 * tw_step and tw_lin of a granule on a regular grid are flags in GF_FLAGS when gix_dstep / gix_dlin give their
 * bits exactly (checked per entry when written), and take no bits when every entry has them.
 *
 * Nothing is rounded: every field decodes to the bits the writer was given (granule statistics must match the
 * rows exactly), which gix_encode checks by decoding its output before a part is named. The arithmetic is
 * integer, apart from the float32 widening (exact) and gix_dstep/gix_dlin (two or three IEEE operations under
 * -ffp-contract=off): the same bits on every CPU. The file is compiled once (not per CPU level).
 *
 * gix_parse trusts nothing: widths, shifts, kinds, run order, lengths adding up exactly and zero padding are
 * checked, so a damaged index the hash missed (it is no authentication) fails the open with TSDB_ERR_CORRUPT and
 * never makes a reader load outside the index plus the 8 bytes that follow it (the footer is there).
 */
#include "internal.h"

#include <stdlib.h>

#define GIX_WIDE 57 /* wider fields take a whole 8 bytes: 57 bits + a bit offset of 7 still fit one load */

static bool field_float(int f)
{
    return f == GF_MIN || f == GF_MAX || f == GF_SUM || f == GF_M2 || f == GF_FIRST || f == GF_LAST ||
           f == GF_STEP || f == GF_LIN;
}

static size_t put_varint(uint8_t *b, uint64_t v)
{
    size_t n = 0;
    while (v >= 0x80) {
        b[n++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    b[n++] = (uint8_t)v;
    return n;
}

static bool get_varint(const uint8_t *b, size_t len, size_t *pos, uint64_t *v)
{
    uint64_t x = 0;
    for (int s = 0; s < 64 && *pos < len; s += 7) {
        uint8_t c = b[(*pos)++];
        if (s == 63 && c > 1)
            return false; /* more than 64 bits */
        x |= (uint64_t)(c & 0x7f) << s;
        if (!(c & 0x80)) {
            *v = x;
            return true;
        }
    }
    return false;
}

/* Bit offsets of the fields from their widths: the rule both sides share. Returns the entry's bit length. */
static uint64_t layout(gix *x)
{
    uint64_t pos = 0;
    for (int f = 0; f < GF_N; f++)
        if (x->f[f].width > GIX_WIDE) {
            x->f[f].byte = (uint32_t)(pos / 8);
            x->f[f].bit = 0;
            pos += 64;
        }
    for (int f = 0; f < GF_N; f++) {
        gix_field *d = &x->f[f];
        if (d->width > GIX_WIDE)
            continue;
        d->byte = d->width ? (uint32_t)(pos / 8) : 0;
        d->bit = d->width ? (uint8_t)(pos % 8) : 0;
        pos += d->width;
    }
    for (int f = 0; f < GF_N; f++) {
        gix_field *d = &x->f[f];
        d->mask = d->width == 64 ? UINT64_MAX : (1ull << d->width) - 1;
    }
    return pos;
}

int gix_parse(const uint8_t *b, size_t len, uint32_t n, gix *x)
{
    memset(x, 0, sizeof *x);
    size_t pos = 0;
    uint64_t nruns, v;
    if (len % 8 || !get_varint(b, len, &pos, &nruns) || nruns > n || (n > 0) != (nruns > 0))
        return TSDB_ERR_CORRUPT;
    for (int f = 0; f < GF_N; f++) {
        gix_field *d = &x->f[f];
        if (len - pos < 2)
            return TSDB_ERR_CORRUPT;
        uint8_t w = b[pos], sk = b[pos + 1];
        pos += 2;
        d->width = w;
        d->shift = sk & 63;
        d->kind = (sk >> 6) & 1;
        if (w > 64 || d->shift + w > 64 || (d->kind == GK_F32 && (!field_float(f) || d->shift + w > 32)) ||
            (d->shift && !w) || !get_varint(b, len, &pos, &d->base))
            return TSDB_ERR_CORRUPT;
        if (d->kind == GK_F32 && d->base > UINT32_MAX)
            return TSDB_ERR_CORRUPT;
        if (sk & 128) {
            if (d->kind != GK_U64 || !get_varint(b, len, &pos, &v) || v == 0)
                return TSDB_ERR_CORRUPT;
            d->slope = (v >> 1) ^ -(v & 1);
        }
    }
    uint64_t bits = layout(x);
    x->stride = (uint32_t)((bits + 7) / 8);
    while (pos % 4) {
        if (pos >= len || b[pos++])
            return TSDB_ERR_CORRUPT;
    }
    if ((len - pos) / 8 < nruns)
        return TSDB_ERR_CORRUPT;
    x->rser = (const uint32_t *)(b + pos);
    x->rstart = (const uint32_t *)(b + pos + 4 * nruns);
    pos += 8 * nruns;
    for (uint64_t r = 0; r < nruns; r++)
        if ((r == 0 ? x->rstart[0] != 0 : x->rser[r] <= x->rser[r - 1] || x->rstart[r] <= x->rstart[r - 1]) ||
            x->rstart[r] >= n)
            return TSDB_ERR_CORRUPT;
    if ((uint64_t)n * x->stride > len - pos)
        return TSDB_ERR_CORRUPT;
    x->e = b + pos;
    pos += (size_t)n * x->stride;
    if (len - pos >= 8)
        return TSDB_ERR_CORRUPT;
    for (; pos < len; pos++)
        if (b[pos])
            return TSDB_ERR_CORRUPT;
    x->n = n;
    x->nruns = (uint32_t)nruns;
    return 0;
}

/* ------------------------------------------------------------------ writer */

static bool same_bits(double a, double b) { return memcmp(&a, &b, 8) == 0; }

/* the float32 that the double with bits u is exactly, as its bits */
static bool as_f32(uint64_t u, uint32_t *out)
{
    double d;
    memcpy(&d, &u, 8);
    float f = (float)d;
    double back = f;
    if (!same_bits(back, d))
        return false;
    memcpy(out, &f, 4);
    return true;
}

static int bits_of(uint64_t v) { return v ? 64 - __builtin_clzll(v) : 0; }

/* Derivable step and linear areas (bit 0: step, bit 1: lin). Never from a NaN: which payload an operation on two
 * NaNs returns (first + last with both NaN) is not defined by IEEE 754, differs between compilers, optimization
 * levels and CPUs, and the decoder (gix_dlin inlined in gx_load) need not evaluate it as this loop does, so the
 * stored flag would decode to other bits and gix_encode's check would refuse the part. A NaN entry keeps its areas
 * as explicit fields. */
static inline uint8_t granule_skip(const granule *e)
{
    bool nan = e->fsum != e->fsum || e->tw_first != e->tw_first || e->tw_last != e->tw_last;
    uint8_t s0 = !nan && same_bits(gix_dstep(e->fsum, e->tw_last, e->tw_gap), e->tw_step);
    uint8_t s1 = !nan && same_bits(gix_dlin(e->fsum, e->tw_first, e->tw_last, e->tw_gap), e->tw_lin);
    return (uint8_t)(s0 | (s1 << 1));
}

static inline void extract_fields(const granule *g, uint8_t sk, uint64_t fv[GF_N])
{
    uint64_t span = (uint64_t)g->ts_max - (uint64_t)g->ts_min;
    fv[GF_COUNT] = g->count;
    fv[GF_TSMIN] = (uint64_t)g->ts_min;
    fv[GF_SPAN] = span;
    fv[GF_OFF] = g->off;
    fv[GF_TSLEN] = g->ts_len;
    fv[GF_VALLEN] = g->val_len;
    fv[GF_NULLS] = g->nulls;
    fv[GF_CRC] = g->crc;
    fv[GF_MIN] = (uint64_t)g->imin;
    fv[GF_MAX] = (uint64_t)g->imax;
    fv[GF_SUM] = g->isum_lo;
    fv[GF_ISUMHI] = (uint64_t)g->isum_hi;
    fv[GF_M2] = g->isq0;
    fv[GF_ISQ1] = g->isq1;
    fv[GF_ISQ2] = g->isq2;
    memcpy(&fv[GF_FIRST], &g->tw_first, 8);
    memcpy(&fv[GF_LAST], &g->tw_last, 8);
    memcpy(&fv[GF_STEP], &g->tw_step, 8);
    memcpy(&fv[GF_LIN], &g->tw_lin, 8);
    fv[GF_STEPMS] = span - g->tw_step_ms;
    fv[GF_LINMS] = span - g->tw_lin_ms;
    fv[GF_GAP] = g->tw_gap;
    fv[GF_FLAGS] = (uint64_t)g->tw_flags | ((uint64_t)(sk & 1) * GIX_STEP_DERIVED) |
                   ((uint64_t)((sk >> 1) & 1) * GIX_LIN_DERIVED);
}

/* Single-pass frame-of-reference tracker: unsigned and signed bounds plus the OR of (v ^ first).
 * For any base taken from the values, ctz(OR_i (v_i - base)) == ctz(OR_i (v_i ^ first)) because 2^s divides
 * (v_i - base) for all i iff all v_i share the same low s bits, so one pass suffices for both orders and shift. */
typedef struct ftrack {
    uint64_t umin, umax;
    int64_t smin, smax;
    uint64_t first, any_xor;
    bool used;
} ftrack;

static inline void ftrack_init(ftrack *t)
{
    t->umin = UINT64_MAX;
    t->umax = 0;
    t->smin = INT64_MAX;
    t->smax = INT64_MIN;
    t->first = 0;
    t->any_xor = 0;
    t->used = false;
}

static inline void ftrack_init_one(ftrack *t, uint64_t val)
{
    t->used = true;
    t->first = val;
    t->umin = t->umax = val;
    t->smin = t->smax = (int64_t)val;
    t->any_xor = 0;
}

static inline void ftrack_add_used(ftrack *t, uint64_t val)
{
    if (val < t->umin)
        t->umin = val;
    if (val > t->umax)
        t->umax = val;
    if ((int64_t)val < t->smin)
        t->smin = (int64_t)val;
    if ((int64_t)val > t->smax)
        t->smax = (int64_t)val;
    t->any_xor |= val ^ t->first;
}

static inline void ftrack_add(ftrack *t, uint64_t val)
{
    if (UNLIKELY(!t->used)) {
        ftrack_init_one(t, val);
        return;
    }
    ftrack_add_used(t, val);
}

/* Float32 tracker: values are uint32_t bit patterns (always non-negative as int64_t, so unsigned order suffices). */
typedef struct f32track {
    uint32_t umin, umax, first, any_xor;
    bool used;
} f32track;

static inline void f32track_init(f32track *t)
{
    t->umin = UINT32_MAX;
    t->umax = 0;
    t->first = 0;
    t->any_xor = 0;
    t->used = false;
}

static inline void f32track_add(f32track *t, uint32_t val)
{
    if (UNLIKELY(!t->used)) {
        t->used = true;
        t->first = val;
        t->umin = t->umax = val;
        return;
    }
    if (val < t->umin)
        t->umin = val;
    if (val > t->umax)
        t->umax = val;
    t->any_xor |= val ^ t->first;
}

static void f32track_finish(const f32track *t, gix_field *d)
{
    d->width = 0;
    d->shift = 0;
    d->base = 0;
    if (!t->used)
        return;
    int sh = t->any_xor ? __builtin_ctz(t->any_xor) : 0;
    uint32_t range = t->umax - t->umin;
    int w = bits_of((uint64_t)(range >> sh));
    d->base = t->umin;
    d->shift = (uint8_t)(w ? sh : 0);
    d->width = (uint8_t)w;
}

static void ftrack_finish(const ftrack *t, gix_field *d)
{
    d->width = 0;
    d->shift = 0;
    d->base = 0;
    if (!t->used)
        return;
    int sh = t->any_xor ? __builtin_ctzll(t->any_xor) : 0;
    uint64_t urange = t->umax - t->umin;
    uint64_t srange = (uint64_t)t->smax - (uint64_t)t->smin;
    int uw = bits_of(urange >> sh);
    d->base = t->umin;
    d->shift = (uint8_t)(uw ? sh : 0);
    d->width = (uint8_t)uw;
    if (srange < urange) {
        int sw = bits_of(srange >> sh);
        if (sw < uw) {
            d->base = (uint64_t)t->smin;
            d->shift = (uint8_t)(sw ? sh : 0);
            d->width = (uint8_t)sw;
        }
    }
}

int gix_encode_parsed(const granule *g, size_t n, uint8_t **out, size_t *len, gix *parsed)
{
    *out = NULL;
    *len = 0;
    if (n > UINT32_MAX)
        return TSDB_ERR_INVALID;
    int rc = TSDB_ERR_NOMEM;
    uint8_t *skip = NULL;
    uint8_t *buf = NULL;
    size_t cap = 0;
    gix x = {0}, y;
    uint32_t nruns = 0;
    if (n > 0) {
        if (!(skip = malloc(n)))
            goto out;
        _Static_assert(GF_N <= 32, "field bitmasks require GF_N <= 32");
        uint8_t sk0 = granule_skip(&g[0]);
        skip[0] = sk0;
        uint64_t fv0[GF_N];
        extract_fields(&g[0], sk0, fv0);
        uint64_t cand_slope[GF_N] = {0};
        uint32_t slp_mask = 0;
        if (n > 2) {
            uint64_t fvn[GF_N];
            extract_fields(&g[n - 1], granule_skip(&g[n - 1]), fvn);
            for (int f = 0; f < GF_N; f++) {
                if (f == GF_STEP || f == GF_LIN)
                    continue;
                __int128 diff = (__int128)(int64_t)fvn[f] - (int64_t)fv0[f];
                cand_slope[f] = (uint64_t)(int64_t)(diff / (__int128)(n - 1));
                if (cand_slope[f])
                    slp_mask |= 1u << f;
            }
        }
        ftrack pln[GF_N], slp[GF_N];
        f32track f32t[GF_N];
        uint32_t f32_mask = 0;
        for (int f = 0; f < GF_N; f++) {
            ftrack_init(&pln[f]);
            ftrack_init(&slp[f]);
            f32track_init(&f32t[f]);
            if (field_float(f))
                f32_mask |= 1u << f;
        }
        /* Entry 0 initializes all non-skippable plain/slope trackers so i = 1..n-1 needs no !used check. */
        uint32_t prev_series = g[0].series;
        nruns = 1;
        for (int f = 0; f < GF_N; f++) {
            if ((f == GF_STEP && (sk0 & 1)) || (f == GF_LIN && (sk0 & 2)))
                continue;
            ftrack_init_one(&pln[f], fv0[f]);
        }
        for (uint32_t m = slp_mask; m; m &= m - 1) {
            int f = __builtin_ctz(m);
            ftrack_init_one(&slp[f], fv0[f]);
        }
        for (uint32_t m = f32_mask; m; m &= m - 1) {
            int f = __builtin_ctz(m);
            if ((f == GF_STEP && (sk0 & 1)) || (f == GF_LIN && (sk0 & 2)))
                continue;
            uint32_t b32;
            if (as_f32(fv0[f], &b32))
                f32track_add(&f32t[f], b32);
            else
                f32_mask &= ~(1u << f);
        }
        for (size_t i = 1; i < n; i++) {
            const granule *e = &g[i];
            if (UNLIKELY(e->series < prev_series)) {
                rc = TSDB_ERR_INVALID;
                goto out;
            }
            if (e->series != prev_series) {
                nruns++;
                prev_series = e->series;
            }
            uint8_t sk = granule_skip(e);
            skip[i] = sk;
            uint64_t fv[GF_N];
            extract_fields(e, sk, fv);
            _Static_assert(GF_LIN == GF_STEP + 1, "GF_STEP and GF_LIN must be adjacent");
            for (int f = 0; f < GF_STEP; f++)
                ftrack_add_used(&pln[f], fv[f]);
            if (!(sk & 1))
                ftrack_add(&pln[GF_STEP], fv[GF_STEP]);
            if (!(sk & 2))
                ftrack_add(&pln[GF_LIN], fv[GF_LIN]);
            for (int f = GF_LIN + 1; f < GF_N; f++)
                ftrack_add_used(&pln[f], fv[f]);
            for (uint32_t m = slp_mask; m; m &= m - 1) {
                int f = __builtin_ctz(m);
                ftrack_add_used(&slp[f], fv[f] - (uint64_t)i * cand_slope[f]);
            }
            for (uint32_t m = f32_mask; m; m &= m - 1) {
                int f = __builtin_ctz(m);
                if ((f == GF_STEP && (sk & 1)) || (f == GF_LIN && (sk & 2)))
                    continue;
                uint32_t b32;
                if (as_f32(fv[f], &b32))
                    f32track_add(&f32t[f], b32);
                else
                    f32_mask &= ~(1u << f);
            }
        }
        for (int f = 0; f < GF_N; f++) {
            gix_field best = {0};
            ftrack_finish(&pln[f], &best);
            best.kind = GK_U64;
            if ((slp_mask & (1u << f)) && best.width) {
                gix_field c = {0};
                ftrack_finish(&slp[f], &c);
                if (c.width < best.width) {
                    best = c;
                    best.slope = cand_slope[f];
                }
            }
            if ((f32_mask & (1u << f)) && best.width > 0) {
                gix_field c = {0};
                f32track_finish(&f32t[f], &c);
                if (c.width < best.width) {
                    best = c;
                    best.kind = GK_F32;
                }
            }
            best.mask = best.width == 64 ? UINT64_MAX : (1ull << best.width) - 1;
            x.f[f] = best;
        }
    }
    uint64_t bits = layout(&x);
    x.stride = (uint32_t)((bits + 7) / 8);
    cap = 16 + GF_N * 22 + 4 + 8 * (size_t)nruns + n * (size_t)x.stride + 16;
    rc = TSDB_ERR_NOMEM;
    if (!(buf = calloc(1, cap)))
        goto out;
    size_t pos = put_varint(buf, nruns);
    for (int f = 0; f < GF_N; f++) {
        const gix_field *d = &x.f[f];
        buf[pos++] = d->width;
        buf[pos++] = (uint8_t)(d->shift | d->kind << 6 | (d->slope ? 128 : 0));
        pos += put_varint(buf + pos, d->base);
        if (d->slope)
            pos += put_varint(buf + pos, (d->slope << 1) ^ (uint64_t)((int64_t)d->slope >> 63));
    }
    pos = (pos + 3) & ~(size_t)3;
    size_t rpos = pos;
    pos += 8 * (size_t)nruns;
    uint8_t *e = buf + pos;
    int active[GF_N], na = 0;
    for (int f = 0; f < GF_N; f++)
        if (x.f[f].width)
            active[na++] = f;
    uint32_t k = 0;
    for (size_t i = 0; i < n; i++) {
        if (i == 0 || g[i].series != g[i - 1].series) {
            memcpy(buf + rpos + 4 * (size_t)k, &g[i].series, 4);
            uint32_t st = (uint32_t)i;
            memcpy(buf + rpos + 4 * ((size_t)nruns + k), &st, 4);
            k++;
        }
        uint8_t sk = skip[i];
        uint64_t fv[GF_N];
        extract_fields(&g[i], sk, fv);
        uint8_t *q = e + i * (size_t)x.stride;
        for (int a = 0; a < na; a++) {
            int f = active[a];
            if ((f == GF_STEP && (sk & 1)) || (f == GF_LIN && (sk & 2)))
                continue;
            const gix_field *d = &x.f[f];
            uint64_t val = fv[f];
            if (d->kind == GK_F32) {
                double dv;
                memcpy(&dv, &val, 8);
                float fv32 = (float)dv;
                uint32_t b32;
                memcpy(&b32, &fv32, 4);
                val = b32;
            }
            uint64_t delta = (val - d->base - (uint64_t)i * d->slope) >> d->shift;
            uint64_t w;
            memcpy(&w, q + d->byte, 8);
            w |= delta << d->bit;
            memcpy(q + d->byte, &w, 8);
        }
    }
    pos += n * (size_t)x.stride;
    pos = (pos + 7) & ~(size_t)7;
    /* the stores above wrote at most 7 bytes past the entries, all zero bits: the padding stays zero */
    rc = TSDB_ERR_CORRUPT;
    if (gix_parse(buf, pos, (uint32_t)n, &y) != 0) {
        fprintf(stderr, "tsdb: the granule index encoder wrote an index it cannot parse: part refused\n");
        goto out;
    }
    part pp;
    memset(&pp, 0, sizeof pp);
    pp.ix = y;
    pp.ng = (uint32_t)n;
    uint32_t run = 0;
    for (size_t i = 0; i < n; i++) {
        if (run + 1 < y.nruns && y.rstart[run + 1] == (uint32_t)i)
            run++;
        granule d;
        memset(&d, 0, sizeof d);
        gx_load(&pp, (uint32_t)i, GW_ALL & ~GW_SERIES, &d);
        d.series = y.rser[run];
        if (memcmp(&d, &g[i], sizeof d) != 0) {
            fprintf(stderr, "tsdb: the granule index encoder lost bits of entry %zu (series %u): part refused\n", i,
                    g[i].series);
            goto out;
        }
    }
    if (n > 0 && run + 1 != y.nruns) {
        fprintf(stderr, "tsdb: the granule index encoder run table mismatch: part refused\n");
        goto out;
    }
    *out = buf;
    *len = pos;
    if (parsed)
        *parsed = y;
    buf = NULL;
    rc = 0;
out:
    tsdb_free_big(skip, n);
    tsdb_free_big(buf, cap);
    return rc;
}

int gix_encode(const granule *g, size_t n, uint8_t **out, size_t *len)
{
    return gix_encode_parsed(g, n, out, len, NULL);
}
