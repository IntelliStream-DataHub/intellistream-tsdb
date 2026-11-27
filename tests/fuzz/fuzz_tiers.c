/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Fuzzer for the TIERS file parser (src/tiers.c): tiers_parse_buf must never read past
 * its input length, must reject invalid records cleanly, and any accepted view must
 * re-encode and re-parse consistently. A second pass repairs record framing/checksums
 * so payload validation paths behind xxh64 are exercised directly.
 */
#include "fuzz.h"
#include "internal.h"
#include "tiers.h"

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}

static size_t fuzz_rec_size(size_t plen) { return ((5u + plen + 7u) & ~(size_t)7u) + 8u; }

static bool id_in_view(const tiers_view *v, const uint8_t id[16])
{
    for (size_t i = 0; i < v->nseen_ids; i++)
        if (memcmp(v->seen_ids[i], id, 16) == 0)
            return true;
    return false;
}

static void make_fresh_id(const tiers_view *v, uint64_t seed, uint8_t out[16])
{
    for (uint64_t attempt = 0;; attempt++) {
        uint64_t a = xxh64(&seed, sizeof seed, 0x9e3779b97f4a7c15ull ^ attempt);
        uint64_t b = xxh64(&attempt, sizeof attempt, 0xbf58476d1ce4e5b9ull ^ seed);
        memcpy(out, &a, 8);
        memcpy(out + 8, &b, 8);
        out[0] |= 1; /* never all-zero */
        if (!id_in_view(v, out))
            return;
    }
}

static void verify_parsed(const uint8_t *p, size_t size)
{
    tiers_view v;
    size_t good_end = 0;
    if (tiers_parse_buf(p, size, "fuzz", &v, &good_end) != 0)
        return;

    if (good_end > size || (good_end & 7u) != 0 || v.file_end != good_end)
        abort();
    if (v.nstores > TIERS_STORES_MAX)
        abort();

    /* Exercise effective settings lookup on global and per-volume entries. */
    tiers_settings eff;
    tiers_view_settings_for(&v, TIERS_VOL_ALL, &eff);
    tiers_view_settings_for(&v, 0, &eff);
    for (size_t i = 0; i < v.nvols; i++)
        tiers_view_settings_for(&v, v.vols[i].volume, &eff);

    /* Re-encode the current logical view into a fresh buffer and verify it parses identically.
     * Non-ACTIVE slots may have had chained replacements or reused mount points of other LOST slots,
     * so seed non-ACTIVE slots with unique synthetic (tmp_id, /fuzz-orig/<i>) before transitioning
     * them to their final state and path. */
    size_t cap = 1024u + v.nstores * (2u * PATH_MAX + 256u) + (v.nvols + 1u) * 64u;
    uint8_t *buf = malloc(cap);
    if (!buf) {
        tiers_view_free(&v);
        return;
    }
    size_t off = 0;
    uint8_t tmp_ids[TIERS_STORES_MAX][16];
    for (size_t i = 0; i < v.nstores; i++) {
        const tiers_store *s = &v.stores[i];
        if (tiers_path_valid(s->path) != 0 || s->cls > TSDB_TIER_CLASS_HDD || s->state > TIERS_STATE_REPLACED_BY)
            abort();
        if (s->state == TIERS_STATE_ACTIVE) {
            int n = tiers_encode_store(off, s->id, s->cls, s->single_copy, s->path, buf + off, cap - off);
            if (n <= 0)
                abort();
            off += (size_t)n;
        } else {
            make_fresh_id(&v, (uint64_t)i + 1u, tmp_ids[i]);
            char orig[64];
            for (size_t k = 0;; k++) {
                snprintf(orig, sizeof orig, "/fuzz-orig/%zu-%zu", i, k);
                bool clash = false;
                for (size_t j = 0; j < v.nstores && !clash; j++)
                    if (strcmp(v.stores[j].path, orig) == 0)
                        clash = true;
                if (!clash)
                    break;
            }
            int n = tiers_encode_store(off, tmp_ids[i], s->cls, s->single_copy, orig, buf + off, cap - off);
            if (n <= 0)
                abort();
            off += (size_t)n;
            n = tiers_encode_state(off, tmp_ids[i], TIERS_STATE_LOST, 0, false, NULL, NULL, buf + off, cap - off);
            if (n <= 0)
                abort();
            off += (size_t)n;
        }
    }
    for (size_t i = 0; i < v.nstores; i++) {
        const tiers_store *s = &v.stores[i];
        if (s->state == TIERS_STATE_LOST) {
            int n = tiers_encode_state(off, tmp_ids[i], TIERS_STATE_REPLACED_BY, s->cls, s->single_copy, s->id,
                                       s->path, buf + off, cap - off);
            if (n <= 0)
                abort();
            off += (size_t)n;
            n = tiers_encode_state(off, s->id, TIERS_STATE_LOST, 0, false, NULL, NULL, buf + off, cap - off);
            if (n <= 0)
                abort();
            off += (size_t)n;
        } else if (s->state == TIERS_STATE_REPLACED_BY) {
            int n = tiers_encode_state(off, tmp_ids[i], TIERS_STATE_REPLACED_BY, s->cls, s->single_copy,
                                       s->replaced_by, s->path, buf + off, cap - off);
            if (n <= 0)
                abort();
            off += (size_t)n;
        }
    }
    if (v.global.set_mask != 0) {
        int n = tiers_encode_settings(off, TIERS_VOL_ALL, &v.global, buf + off, cap - off);
        if (n <= 0)
            abort();
        off += (size_t)n;
    }
    for (size_t i = 0; i < v.nvols; i++) {
        int n = tiers_encode_settings(off, v.vols[i].volume, &v.vols[i].s, buf + off, cap - off);
        if (n <= 0)
            abort();
        off += (size_t)n;
    }

    tiers_view v2;
    size_t ge2 = 0;
    if (tiers_parse_buf(buf, off, "fuzz-rt", &v2, &ge2) != 0)
        abort();
    if (ge2 != off || v2.nstores != v.nstores || v2.nvols != v.nvols)
        abort();
    for (size_t i = 0; i < v.nstores; i++) {
        bool id_ok = v.stores[i].state == TIERS_STATE_REPLACED_BY
                         ? memcmp(v2.stores[i].replaced_by, v.stores[i].replaced_by, 16) == 0
                         : memcmp(v2.stores[i].id, v.stores[i].id, 16) == 0;
        if (!id_ok || v2.stores[i].cls != v.stores[i].cls || v2.stores[i].single_copy != v.stores[i].single_copy ||
            v2.stores[i].state != v.stores[i].state || strcmp(v2.stores[i].path, v.stores[i].path) != 0)
            abort();
    }
    if (v2.global.set_mask != v.global.set_mask || v2.global.k != v.global.k || v2.global.m != v.global.m ||
        v2.global.tier_after_s != v.global.tier_after_s || v2.global.tier_high_pct != v.global.tier_high_pct ||
        v2.global.young_high_pct != v.global.young_high_pct || v2.global.rebuild_mb_s != v.global.rebuild_mb_s ||
        v2.global.scrub_mb_s != v.global.scrub_mb_s || v2.global.scrub_days != v.global.scrub_days)
        abort();

    tiers_view_free(&v2);
    free(buf);
    tiers_view_free(&v);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    uint8_t *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    if (size)
        memcpy(p, data, size);
    else
        p[0] = 0;

    /* Pass 1: raw bytes. */
    verify_parsed(p, size);

    /* Pass 2: fix framing padding and xxh64 checksums along a sequence of records so payload checks run. */
    if (size >= 16u && size <= 32768u) {
        uint8_t *q = malloc(size);
        if (q) {
            memcpy(q, p, size);
            size_t pos = 0;
            while (pos + 16u <= size) {
                uint32_t raw_len = (uint32_t)q[pos] | ((uint32_t)q[pos + 1] << 8);
                size_t max_plen = size - pos - 13u; /* 5 header + 8 checksum */
                if (max_plen > TIERS_PAYLOAD_MAX)
                    max_plen = TIERS_PAYLOAD_MAX;
                size_t plen = raw_len ? (raw_len % (max_plen + 1u)) : 1u;
                if (plen == 0)
                    plen = 1u;
                size_t rs = fuzz_rec_size(plen);
                while (rs > size - pos && plen > 1u) {
                    plen--;
                    rs = fuzz_rec_size(plen);
                }
                if (rs > size - pos)
                    break;
                q[pos + 0] = (uint8_t)plen;
                q[pos + 1] = (uint8_t)(plen >> 8);
                q[pos + 2] = (uint8_t)(plen >> 16);
                q[pos + 3] = (uint8_t)(plen >> 24);
                q[pos + 4] = (uint8_t)((q[pos + 4] % 3u) + 1u);
                if (rs - 8u > 5u + plen)
                    memset(q + pos + 5u + plen, 0, (rs - 8u) - (5u + plen));
                uint64_t h = xxh64(q + pos, rs - 8u, (uint64_t)pos ^ TIERS_XXH_SEED);
                for (int i = 0; i < 8; i++)
                    q[pos + rs - 8u + (size_t)i] = (uint8_t)(h >> (8 * i));
                pos += rs;
            }
            verify_parsed(q, size);
            free(q);
        }
    }

    free(p);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    uint8_t buf[4096];
    uint8_t id1[16], id2[16], id3[16];
    for (int i = 0; i < 16; i++) {
        id1[i] = (uint8_t)(0x10 + i);
        id2[i] = (uint8_t)(0x30 + i);
        id3[i] = (uint8_t)(0x50 + i);
    }

    fuzz_put(dir, "seed-empty", "", 0);

    size_t off = 0;
    int n = tiers_encode_store(off, id1, TSDB_TIER_CLASS_HDD, false, "/mnt/hdd0/tier", buf + off, sizeof buf - off);
    if (n <= 0)
        exit(1);
    off += (size_t)n;
    fuzz_put(dir, "seed-store-hdd", buf, off);

    n = tiers_encode_store(off, id2, TSDB_TIER_CLASS_NVME, true, "/mnt/nvme0/tier", buf + off, sizeof buf - off);
    if (n <= 0)
        exit(1);
    off += (size_t)n;

    tiers_settings gs;
    memset(&gs, 0, sizeof gs);
    gs.set_mask = TSDB_TIER_SET_REDUNDANCY | TSDB_TIER_SET_AFTER | TSDB_TIER_SET_HIGH_PCT |
                  TSDB_TIER_SET_YOUNG_HIGH_PCT | TSDB_TIER_SET_REBUILD_MB_S | TSDB_TIER_SET_SCRUB_MB_S |
                  TSDB_TIER_SET_SCRUB_DAYS;
    gs.k = 4;
    gs.m = 2;
    gs.tier_after_s = 86400;
    gs.tier_high_pct = 75;
    gs.young_high_pct = 88;
    gs.rebuild_mb_s = 150;
    gs.scrub_mb_s = 40;
    gs.scrub_days = 7;
    n = tiers_encode_settings(off, TIERS_VOL_ALL, &gs, buf + off, sizeof buf - off);
    if (n <= 0)
        exit(1);
    off += (size_t)n;

    tiers_settings vs;
    memset(&vs, 0, sizeof vs);
    vs.set_mask = TSDB_TIER_SET_AFTER;
    vs.tier_after_s = 3600;
    n = tiers_encode_settings(off, 1, &vs, buf + off, sizeof buf - off);
    if (n <= 0)
        exit(1);
    off += (size_t)n;

    n = tiers_encode_state(off, id1, TIERS_STATE_LOST, 0, false, NULL, NULL, buf + off, sizeof buf - off);
    if (n <= 0)
        exit(1);
    off += (size_t)n;

    n = tiers_encode_state(off, id1, TIERS_STATE_REPLACED_BY, TSDB_TIER_CLASS_SSD, false, id3, "/mnt/ssd0/tier",
                           buf + off, sizeof buf - off);
    if (n <= 0)
        exit(1);
    off += (size_t)n;
    fuzz_put(dir, "seed-all-kinds", buf, off);

    /* Torn tail seed */
    fuzz_put(dir, "seed-torn-tail", buf, off - 11);

    /* Corrupt record before good records */
    uint8_t bad[4096];
    memcpy(bad, buf, off);
    bad[10] ^= 0x01;
    fuzz_put(dir, "seed-corrupt-mid", bad, off);
}
