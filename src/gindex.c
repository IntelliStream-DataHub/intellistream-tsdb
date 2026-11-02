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

static uint64_t field_value(const granule *g, int f)
{
    uint64_t span = (uint64_t)g->ts_max - (uint64_t)g->ts_min;
    switch (f) {
    case GF_COUNT: return g->count;
    case GF_TSMIN: return (uint64_t)g->ts_min;
    case GF_SPAN: return span;
    case GF_OFF: return g->off;
    case GF_TSLEN: return g->ts_len;
    case GF_VALLEN: return g->val_len;
    case GF_NULLS: return g->nulls;
    case GF_CRC: return g->crc;
    case GF_MIN: return (uint64_t)g->imin;
    case GF_MAX: return (uint64_t)g->imax;
    case GF_SUM: return g->isum_lo;
    case GF_ISUMHI: return (uint64_t)g->isum_hi;
    case GF_M2: return g->isq0;
    case GF_ISQ1: return g->isq1;
    case GF_ISQ2: return g->isq2;
    case GF_FIRST: { uint64_t u; memcpy(&u, &g->tw_first, 8); return u; }
    case GF_LAST: { uint64_t u; memcpy(&u, &g->tw_last, 8); return u; }
    case GF_STEP: { uint64_t u; memcpy(&u, &g->tw_step, 8); return u; }
    case GF_LIN: { uint64_t u; memcpy(&u, &g->tw_lin, 8); return u; }
    case GF_STEPMS: return span - g->tw_step_ms;
    case GF_LINMS: return span - g->tw_lin_ms;
    case GF_GAP: return g->tw_gap;
    default: return g->tw_flags; /* GF_FLAGS; the derived bits are added by the caller */
    }
}

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

/* A frame of reference for r[0..n) (entries with skip[i] ignored): base, shift and width with the fewest bits,
 * in unsigned or signed order. */
static void frame(const uint64_t *r, const uint8_t *skip, size_t n, gix_field *d)
{
    uint64_t umin = UINT64_MAX, umax = 0;
    int64_t smin = INT64_MAX, smax = INT64_MIN;
    size_t used = 0;
    for (size_t i = 0; i < n; i++) {
        if (skip && skip[i])
            continue;
        used++;
        umin = MIN(umin, r[i]);
        umax = MAX(umax, r[i]);
        smin = MIN(smin, (int64_t)r[i]);
        smax = MAX(smax, (int64_t)r[i]);
    }
    d->width = 0;
    d->shift = 0;
    d->base = 0;
    if (!used)
        return;
    uint64_t bases[2] = {umin, (uint64_t)smin};
    uint64_t ranges[2] = {umax - umin, (uint64_t)smax - (uint64_t)smin};
    int best = -1;
    for (int k = 0; k < 2; k++) {
        if (k == 1 && ranges[1] >= ranges[0])
            continue;
        uint64_t any = 0;
        for (size_t i = 0; i < n; i++)
            if (!skip || !skip[i])
                any |= r[i] - bases[k];
        int sh = any ? __builtin_ctzll(any) : 0;
        int w = bits_of(ranges[k] >> sh);
        if (best < 0 || w < d->width) {
            best = k;
            d->base = bases[k];
            d->shift = (uint8_t)(w ? sh : 0);
            d->width = (uint8_t)w;
        }
    }
}

static int pack_field(const granule *g, size_t n, const uint8_t *skip, int f, uint64_t *v, uint64_t *r,
                      gix_field *d, uint64_t *out_delta)
{
    for (size_t i = 0; i < n; i++)
        v[i] = field_value(&g[i], f);
    if (f == GF_FLAGS)
        for (size_t i = 0; i < n; i++)
            v[i] |= (uint64_t)skip[2 * i] * GIX_STEP_DERIVED | (uint64_t)skip[2 * i + 1] * GIX_LIN_DERIVED;
    const uint8_t *sk = NULL;
    uint8_t *own = NULL;
    if (f == GF_STEP || f == GF_LIN) {
        if (!(own = malloc(n + 1)))
            return TSDB_ERR_NOMEM;
        for (size_t i = 0; i < n; i++)
            own[i] = skip[2 * i + (f == GF_LIN)];
        sk = own;
    }
    /* plain */
    gix_field best = {0};
    frame(v, sk, n, &best);
    best.kind = GK_U64;
    /* a line through the first and last value */
    if (!sk && n > 2 && best.width) {
        __int128 diff = (__int128)(int64_t)v[n - 1] - (int64_t)v[0];
        int64_t slope = (int64_t)(diff / (__int128)(n - 1));
        if (slope) {
            for (size_t i = 0; i < n; i++)
                r[i] = v[i] - (uint64_t)i * (uint64_t)slope;
            gix_field c = {0};
            frame(r, NULL, n, &c);
            if (c.width < best.width) {
                best = c;
                best.slope = (uint64_t)slope;
            }
        }
    }
    /* float32 */
    if (field_float(f) && best.width > 0) {
        size_t i;
        for (i = 0; i < n; i++) {
            uint32_t b32 = 0;
            if (sk && sk[i])
                r[i] = 0;
            else if (!as_f32(v[i], &b32))
                break;
            r[i] = b32;
        }
        if (i == n) {
            gix_field c = {0};
            uint32_t umin = UINT32_MAX;
            for (i = 0; i < n; i++)
                if (!sk || !sk[i])
                    umin = MIN(umin, (uint32_t)r[i]);
            for (i = 0; i < n; i++)
                if (sk && sk[i])
                    r[i] = umin;
            frame(r, sk, n, &c);
            if (c.width < best.width) {
                best = c;
                best.kind = GK_F32;
            }
        }
    }
    best.mask = best.width == 64 ? UINT64_MAX : (1ull << best.width) - 1;
    /* the deltas to store, in r */
    for (size_t i = 0; i < n; i++) {
        uint64_t val = v[i];
        if (best.kind == GK_F32) {
            uint32_t b32 = 0;
            if (!sk || !sk[i])
                as_f32(v[i], &b32);
            else
                b32 = (uint32_t)best.base;
            val = b32;
        } else if (sk && sk[i]) {
            val = best.base + (uint64_t)i * best.slope;
        }
        out_delta[i] = (val - best.base - (uint64_t)i * best.slope) >> best.shift;
    }
    free(own);
    *d = best;
    return 0;
}

int gix_encode(const granule *g, size_t n, uint8_t **out, size_t *len)
{
    *out = NULL;
    *len = 0;
    if (n > UINT32_MAX)
        return TSDB_ERR_INVALID;
    int rc = TSDB_ERR_NOMEM;
    uint64_t *v = malloc((n + 1) * sizeof *v), *r = malloc((n + 1) * sizeof *r);
    uint64_t *delta = malloc((n + 1) * GF_N * sizeof *delta);
    uint8_t *skip = calloc(2 * n + 2, 1);
    uint8_t *buf = NULL;
    gix x = {0}, y;
    if (!v || !r || !delta || !skip)
        goto out;
    /* derivable step and linear areas. Never from a NaN: which payload an operation on two NaNs returns (first + last
     * with both NaN) is not defined by IEEE 754, differs between compilers, optimization levels and CPUs, and the
     * decoder (gix_dlin inlined in gx_load) need not evaluate it as this loop does, so the stored flag would decode
     * to other bits and gix_encode's check would refuse the part. A NaN entry keeps its areas as explicit fields. */
    for (size_t i = 0; i < n; i++) {
        const granule *e = &g[i];
        bool nan = e->fsum != e->fsum || e->tw_first != e->tw_first || e->tw_last != e->tw_last;
        skip[2 * i] = !nan && same_bits(gix_dstep(e->fsum, e->tw_last, e->tw_gap), e->tw_step);
        skip[2 * i + 1] = !nan && same_bits(gix_dlin(e->fsum, e->tw_first, e->tw_last, e->tw_gap), e->tw_lin);
    }
    for (int f = 0; f < GF_N; f++)
        if ((rc = pack_field(g, n, skip, f, v, r, &x.f[f], delta + (size_t)f * n)) != 0)
            goto out;
    uint64_t bits = layout(&x);
    x.stride = (uint32_t)((bits + 7) / 8);
    uint32_t nruns = 0;
    for (size_t i = 0; i < n; i++)
        nruns += i == 0 || g[i].series != g[i - 1].series;
    for (size_t i = 1; i < n; i++)
        if (g[i].series < g[i - 1].series) {
            rc = TSDB_ERR_INVALID;
            goto out;
        }
    size_t cap = 16 + GF_N * 22 + 4 + 8 * (size_t)nruns + n * (size_t)x.stride + 16;
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
    uint32_t k = 0;
    for (size_t i = 0; i < n; i++)
        if (i == 0 || g[i].series != g[i - 1].series) {
            memcpy(buf + pos + 4 * k, &g[i].series, 4);
            uint32_t st = (uint32_t)i;
            memcpy(buf + pos + 4 * ((size_t)nruns + k), &st, 4);
            k++;
        }
    pos += 8 * (size_t)nruns;
    uint8_t *e = buf + pos;
    for (size_t i = 0; i < n; i++) {
        uint8_t *q = e + i * x.stride;
        for (int f = 0; f < GF_N; f++) {
            const gix_field *d = &x.f[f];
            if (!d->width)
                continue;
            uint64_t w;
            memcpy(&w, q + d->byte, 8);
            w |= delta[(size_t)f * n + i] << d->bit;
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
    for (size_t i = 0; i < n; i++) {
        granule d;
        memset(&d, 0, sizeof d);
        gx_load(&pp, (uint32_t)i, GW_ALL, &d);
        if (memcmp(&d, &g[i], sizeof d) != 0) {
            fprintf(stderr, "tsdb: the granule index encoder lost bits of entry %zu (series %u): part refused\n", i,
                    g[i].series);
            goto out;
        }
    }
    *out = buf;
    *len = pos;
    buf = NULL;
    rc = 0;
out:
    free(v);
    free(r);
    free(delta);
    free(skip);
    free(buf);
    return rc;
}
