/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * TIERS: append-only checksummed tier catalog beside VOLUMES, and root META db_id
 * (src/tiers.h, docs/design-cluster-data.md §15.7, R15 D2, D3, D18).
 */
#include "internal.h"
#include "tiers.h"
#include "chunk.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef TSDB_TEST_HOOKS
int tiers_test_mutant;
static inline int tiers_mut(void) { return tiers_test_mutant; }
#else
static inline int tiers_mut(void) { return 0; }
#endif

#define TIERS_REC_SETTINGS_PLEN 40u
#define TIERS_MAX_VOL_INDEX 65534u
#define TIERS_SECONDS_MAX 315360000ull
#define TIERS_RATE_MB_S_MAX 100000u
#define TIERS_SCRUB_DAYS_MAX 3650u
#define TIERS_SET_ALL_BITS                                                                                             \
    (TSDB_TIER_SET_REDUNDANCY | TSDB_TIER_SET_AFTER | TSDB_TIER_SET_HIGH_PCT | TSDB_TIER_SET_YOUNG_HIGH_PCT |          \
     TSDB_TIER_SET_REBUILD_MB_S | TSDB_TIER_SET_SCRUB_MB_S | TSDB_TIER_SET_SCRUB_DAYS)

/* ---------------------------------------------------------------- helpers */

static size_t rec_size(size_t plen) { return ((5u + plen + 7u) & ~(size_t)7u) + 8u; }

static uint16_t ld16(const uint8_t *p) { return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8)); }

static uint32_t ld32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t ld64(const uint8_t *p) { return (uint64_t)ld32(p) | ((uint64_t)ld32(p + 4) << 32); }

static void st16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
}

static void st32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static void st64(uint8_t *p, uint64_t v)
{
    st32(p, (uint32_t)v);
    st32(p + 4, (uint32_t)(v >> 32));
}

static bool is_all_zero(const uint8_t id[16])
{
    uint8_t acc = 0;
    for (int i = 0; i < 16; i++)
        acc |= id[i];
    return acc == 0;
}

static int hex_val(int c)
{
    return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
}

int tiers_class_parse(const char *s, uint8_t *out)
{
    if (!out)
        return TSDB_ERR_INVALID;
    if (!s || !*s || strcmp(s, "unknown") == 0) {
        *out = TSDB_TIER_CLASS_UNKNOWN;
        return 0;
    }
    if (strcmp(s, "nvme") == 0) {
        *out = TSDB_TIER_CLASS_NVME;
        return 0;
    }
    if (strcmp(s, "ssd") == 0) {
        *out = TSDB_TIER_CLASS_SSD;
        return 0;
    }
    if (strcmp(s, "hdd") == 0) {
        *out = TSDB_TIER_CLASS_HDD;
        return 0;
    }
    return TSDB_ERR_INVALID;
}

const char *tiers_class_name(uint8_t cls)
{
    switch (cls) {
    case TSDB_TIER_CLASS_NVME:
        return "nvme";
    case TSDB_TIER_CLASS_SSD:
        return "ssd";
    case TSDB_TIER_CLASS_HDD:
        return "hdd";
    default:
        return "unknown";
    }
}

int tiers_path_valid(const char *p)
{
    if (!p || p[0] != '/' || strlen(p) >= PATH_MAX)
        return TSDB_ERR_INVALID;
    for (const unsigned char *s = (const unsigned char *)p; *s; s++)
        if (*s < 0x20 || *s == 0x7f)
            return TSDB_ERR_INVALID;
    for (const char *c = p; *c;) {
        while (*c == '/')
            c++;
        size_t n = strcspn(c, "/");
        if ((n == 1 && c[0] == '.') || (n == 2 && c[0] == '.' && c[1] == '.'))
            return TSDB_ERR_INVALID;
        c += n;
    }
    return 0;
}

/* ---------------------------------------------------------------- view */

void tiers_settings_defaults(tiers_settings *s)
{
    memset(s, 0, sizeof *s);
    s->tier_after_s = TIERS_AFTER_S_DEFAULT;
    s->tier_high_pct = TIERS_HIGH_PCT_DEFAULT;
    s->young_high_pct = TIERS_YOUNG_HIGH_PCT_DEFAULT;
    s->rebuild_mb_s = TIERS_REBUILD_MB_S_DEFAULT;
    s->scrub_mb_s = TIERS_SCRUB_MB_S_DEFAULT;
    s->scrub_days = TIERS_SCRUB_DAYS_DEFAULT;
}

void tiers_view_init(tiers_view *v)
{
    memset(v, 0, sizeof *v);
    tiers_settings_defaults(&v->global);
}

void tiers_view_free(tiers_view *v)
{
    if (!v)
        return;
    for (size_t i = 0; i < v->nstores; i++) {
        free(v->stores[i].path);
        free(v->stores[i].orig_path);
    }
    free(v->stores);
    free(v->vols);
    free(v->seen_ids);
    memset(v, 0, sizeof *v);
}

static void settings_merge(tiers_settings *dst, const tiers_settings *src)
{
    uint32_t m = src->set_mask;
    if (m & TSDB_TIER_SET_REDUNDANCY) {
        dst->k = src->k;
        dst->m = src->m;
    }
    if (m & TSDB_TIER_SET_AFTER)
        dst->tier_after_s = src->tier_after_s;
    if (m & TSDB_TIER_SET_HIGH_PCT)
        dst->tier_high_pct = src->tier_high_pct;
    if (m & TSDB_TIER_SET_YOUNG_HIGH_PCT)
        dst->young_high_pct = src->young_high_pct;
    if (m & TSDB_TIER_SET_REBUILD_MB_S)
        dst->rebuild_mb_s = src->rebuild_mb_s;
    if (m & TSDB_TIER_SET_SCRUB_MB_S)
        dst->scrub_mb_s = src->scrub_mb_s;
    if (m & TSDB_TIER_SET_SCRUB_DAYS)
        dst->scrub_days = src->scrub_days;
    dst->set_mask |= m;
}

void tiers_view_settings_for(const tiers_view *v, uint32_t volume, tiers_settings *out)
{
    tiers_settings_defaults(out);
    if (!v)
        return;
    settings_merge(out, &v->global);
    if (volume == TIERS_VOL_ALL)
        return;
    for (size_t i = 0; i < v->nvols; i++) {
        if (v->vols[i].volume == volume) {
            settings_merge(out, &v->vols[i].s);
            break;
        }
    }
}

static bool settings_valid(uint32_t volume, const tiers_settings *s)
{
    if (volume != TIERS_VOL_ALL && volume > TIERS_MAX_VOL_INDEX)
        return false;
    if (!s || s->set_mask == 0 || (s->set_mask & ~TIERS_SET_ALL_BITS) != 0)
        return false;
    if (s->set_mask & TSDB_TIER_SET_REDUNDANCY) {
        if (s->k < 1 || s->k > CHUNK_K_MAX || s->m > CHUNK_M_MAX)
            return false;
        if (s->m == 0 && s->k != 1)
            return false; /* D5: m = 0 only with k = 1 */
    } else if (s->k != 0 || s->m != 0) {
        return false;
    }
    if (s->set_mask & TSDB_TIER_SET_AFTER) {
        if (s->tier_after_s > TIERS_SECONDS_MAX)
            return false;
    } else if (s->tier_after_s != 0) {
        return false;
    }
    if (s->set_mask & TSDB_TIER_SET_HIGH_PCT) {
        if (s->tier_high_pct < 1 || s->tier_high_pct > 100)
            return false;
    } else if (s->tier_high_pct != 0) {
        return false;
    }
    if (s->set_mask & TSDB_TIER_SET_YOUNG_HIGH_PCT) {
        if (s->young_high_pct < 1 || s->young_high_pct > 100)
            return false;
    } else if (s->young_high_pct != 0) {
        return false;
    }
    if (s->set_mask & TSDB_TIER_SET_REBUILD_MB_S) {
        if (s->rebuild_mb_s < 1 || s->rebuild_mb_s > TIERS_RATE_MB_S_MAX)
            return false;
    } else if (s->rebuild_mb_s != 0) {
        return false;
    }
    if (s->set_mask & TSDB_TIER_SET_SCRUB_MB_S) {
        if (s->scrub_mb_s < 1 || s->scrub_mb_s > TIERS_RATE_MB_S_MAX)
            return false;
    } else if (s->scrub_mb_s != 0) {
        return false;
    }
    if (s->set_mask & TSDB_TIER_SET_SCRUB_DAYS) {
        if (s->scrub_days > TIERS_SCRUB_DAYS_MAX)
            return false;
    } else if (s->scrub_days != 0) {
        return false;
    }
    return true;
}

/* ---------------------------------------------------------------- record encoding */

static int frame_finish(uint64_t off, uint8_t type, size_t plen, uint8_t *out, size_t cap)
{
    if (plen == 0 || plen > TIERS_PAYLOAD_MAX)
        return TSDB_ERR_INVALID;
    size_t rs = rec_size(plen);
    if (rs > cap)
        return TSDB_ERR_INVALID;
    st32(out, (uint32_t)plen);
    out[4] = type;
    if (rs - 8 > 5 + plen)
        memset(out + 5 + plen, 0, (rs - 8) - (5 + plen));
    uint64_t sum = xxh64(out, rs - 8, off ^ TIERS_XXH_SEED);
    st64(out + rs - 8, sum);
    return (int)rs;
}

int tiers_encode_store(uint64_t off, const uint8_t id[16], uint8_t cls, bool single_copy, const char *path,
                       uint8_t *out, size_t cap)
{
    if (!id || is_all_zero(id) || cls > TSDB_TIER_CLASS_HDD || tiers_path_valid(path) != 0 || !out)
        return TSDB_ERR_INVALID;
    size_t plen_str = strlen(path);
    if (plen_str == 0 || plen_str >= PATH_MAX)
        return TSDB_ERR_INVALID;
    size_t plen = 20u + plen_str;
    if (rec_size(plen) > cap)
        return TSDB_ERR_INVALID;
    uint8_t *pl = out + 5;
    memcpy(pl, id, 16);
    pl[16] = cls;
    pl[17] = single_copy ? TIERS_STORE_SINGLE_COPY : 0;
    st16(pl + 18, (uint16_t)plen_str);
    memcpy(pl + 20, path, plen_str);
    return frame_finish(off, TIERS_REC_STORE, plen, out, cap);
}

int tiers_encode_state(uint64_t off, const uint8_t id[16], uint8_t state, uint8_t new_cls, bool new_single_copy,
                       const uint8_t new_id[16], const char *new_path, uint8_t *out, size_t cap)
{
    if (!id || is_all_zero(id) || !out)
        return TSDB_ERR_INVALID;
    size_t np_len = 0;
    if (state == TIERS_STATE_LOST) {
        if (new_cls != 0 || new_single_copy || (new_id && !is_all_zero(new_id)) || (new_path && *new_path))
            return TSDB_ERR_INVALID;
    } else if (state == TIERS_STATE_REPLACED_BY) {
        if (!new_id || is_all_zero(new_id) || new_cls > TSDB_TIER_CLASS_HDD || tiers_path_valid(new_path) != 0)
            return TSDB_ERR_INVALID;
        np_len = strlen(new_path);
        if (np_len == 0 || np_len >= PATH_MAX)
            return TSDB_ERR_INVALID;
    } else {
        return TSDB_ERR_INVALID;
    }
    size_t plen = 37u + np_len;
    if (rec_size(plen) > cap)
        return TSDB_ERR_INVALID;
    uint8_t *pl = out + 5;
    memcpy(pl, id, 16);
    pl[16] = state;
    pl[17] = state == TIERS_STATE_REPLACED_BY ? new_cls : 0;
    pl[18] = (state == TIERS_STATE_REPLACED_BY && new_single_copy) ? TIERS_STORE_SINGLE_COPY : 0;
    if (state == TIERS_STATE_REPLACED_BY)
        memcpy(pl + 19, new_id, 16);
    else
        memset(pl + 19, 0, 16);
    st16(pl + 35, (uint16_t)np_len);
    if (np_len)
        memcpy(pl + 37, new_path, np_len);
    return frame_finish(off, TIERS_REC_STATE, plen, out, cap);
}

int tiers_encode_settings(uint64_t off, uint32_t volume, const tiers_settings *s, uint8_t *out, size_t cap)
{
    if (!out || !s)
        return TSDB_ERR_INVALID;
    /* Canonicalize unset fields to 0 so every encoded SETTINGS record has one exact byte form. */
    tiers_settings c;
    memset(&c, 0, sizeof c);
    c.set_mask = s->set_mask;
    if (c.set_mask & TSDB_TIER_SET_REDUNDANCY) {
        c.k = s->k;
        c.m = s->m;
    }
    if (c.set_mask & TSDB_TIER_SET_AFTER)
        c.tier_after_s = s->tier_after_s;
    if (c.set_mask & TSDB_TIER_SET_HIGH_PCT)
        c.tier_high_pct = s->tier_high_pct;
    if (c.set_mask & TSDB_TIER_SET_YOUNG_HIGH_PCT)
        c.young_high_pct = s->young_high_pct;
    if (c.set_mask & TSDB_TIER_SET_REBUILD_MB_S)
        c.rebuild_mb_s = s->rebuild_mb_s;
    if (c.set_mask & TSDB_TIER_SET_SCRUB_MB_S)
        c.scrub_mb_s = s->scrub_mb_s;
    if (c.set_mask & TSDB_TIER_SET_SCRUB_DAYS)
        c.scrub_days = s->scrub_days;
    if (!settings_valid(volume, &c))
        return TSDB_ERR_INVALID;
    if (rec_size(TIERS_REC_SETTINGS_PLEN) > cap)
        return TSDB_ERR_INVALID;
    uint8_t *pl = out + 5;
    st32(pl + 0, volume);
    st32(pl + 4, c.set_mask);
    st16(pl + 8, c.k);
    st16(pl + 10, c.m);
    st32(pl + 12, c.tier_high_pct);
    st32(pl + 16, c.young_high_pct);
    st32(pl + 20, c.rebuild_mb_s);
    st32(pl + 24, c.scrub_mb_s);
    st32(pl + 28, c.scrub_days);
    st64(pl + 32, c.tier_after_s);
    return frame_finish(off, TIERS_REC_SETTINGS, TIERS_REC_SETTINGS_PLEN, out, cap);
}

/* ---------------------------------------------------------------- record parsing */

static bool parse_rec(const uint8_t *buf, size_t pos, size_t size, uint8_t *type, const uint8_t **pl, size_t *plen,
                      size_t *rs)
{
    if (pos + 16u > size)
        return false;
    const uint8_t *r = buf + pos;
    uint32_t len = ld32(r);
    if (len == 0 || len > TIERS_PAYLOAD_MAX)
        return false;
    size_t sz = rec_size(len);
    if (sz > size - pos)
        return false;
    for (size_t i = 5u + len; i < sz - 8u; i++)
        if (r[i] != 0)
            return false;
    if (xxh64(r, sz - 8u, (uint64_t)pos ^ TIERS_XXH_SEED) != ld64(r + sz - 8u))
        return false;
    *type = r[4];
    *pl = r + 5;
    *plen = len;
    *rs = sz;
    return true;
}

static bool valid_after(const uint8_t *buf, size_t from, size_t size)
{
    uint8_t type;
    const uint8_t *pl;
    size_t plen, rs;
    for (size_t q = from + 8u; q + 16u <= size; q += 8u) {
        if (parse_rec(buf, q, size, &type, &pl, &plen, &rs))
            return true;
    }
    return false;
}

static bool seen_id_has(const tiers_view *v, const uint8_t id[16])
{
    for (size_t i = 0; i < v->nseen_ids; i++)
        if (memcmp(v->seen_ids[i], id, 16) == 0)
            return true;
    return false;
}

static int seen_id_add(tiers_view *v, const uint8_t id[16])
{
    uint8_t (*p)[16] = realloc(v->seen_ids, (v->nseen_ids + 1) * sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    v->seen_ids = p;
    memcpy(v->seen_ids[v->nseen_ids++], id, 16);
    return 0;
}

static int apply_store(tiers_view *v, const uint8_t *pl, size_t plen)
{
    if (plen < 21u)
        return TSDB_ERR_CORRUPT;
    uint8_t id[16];
    memcpy(id, pl, 16);
    uint8_t cls = pl[16];
    uint8_t flags = pl[17];
    uint16_t path_len = ld16(pl + 18);
    if (is_all_zero(id) || cls > TSDB_TIER_CLASS_HDD || (flags & ~TIERS_STORE_SINGLE_COPY) != 0 || path_len == 0 ||
        path_len >= PATH_MAX || plen != 20u + (size_t)path_len)
        return TSDB_ERR_CORRUPT;
    if (memchr(pl + 20, '\0', path_len) != NULL)
        return TSDB_ERR_CORRUPT;
    char path[PATH_MAX];
    memcpy(path, pl + 20, path_len);
    path[path_len] = '\0';
    if (tiers_path_valid(path) != 0)
        return TSDB_ERR_CORRUPT;
    if (v->nstores >= TIERS_STORES_MAX || seen_id_has(v, id))
        return TSDB_ERR_CORRUPT;
    for (size_t i = 0; i < v->nstores; i++) {
        if (strcmp(v->stores[i].path, path) == 0)
            return TSDB_ERR_CORRUPT;
    }
    char *pcopy = strdup(path);
    if (!pcopy)
        return TSDB_ERR_NOMEM;
    tiers_store *ns = realloc(v->stores, (v->nstores + 1) * sizeof *ns);
    if (!ns) {
        free(pcopy);
        return TSDB_ERR_NOMEM;
    }
    v->stores = ns;
    int rc = seen_id_add(v, id);
    if (rc) {
        free(pcopy);
        return rc;
    }
    tiers_store *st = &v->stores[v->nstores++];
    memset(st, 0, sizeof *st);
    memcpy(st->id, id, 16);
    st->cls = cls;
    st->single_copy = (flags & TIERS_STORE_SINGLE_COPY) != 0;
    st->state = TIERS_STATE_ACTIVE;
    st->path = pcopy;
    return 0;
}

static int apply_state(tiers_view *v, const uint8_t *pl, size_t plen)
{
    if (plen < 37u)
        return TSDB_ERR_CORRUPT;
    uint8_t id[16], new_id[16];
    memcpy(id, pl, 16);
    uint8_t state = pl[16];
    uint8_t new_cls = pl[17];
    uint8_t new_flags = pl[18];
    memcpy(new_id, pl + 19, 16);
    uint16_t np_len = ld16(pl + 35);
    if (is_all_zero(id) || plen != 37u + (size_t)np_len)
        return TSDB_ERR_CORRUPT;
    if (state == TIERS_STATE_LOST) {
        if (new_cls != 0 || new_flags != 0 || !is_all_zero(new_id) || np_len != 0)
            return TSDB_ERR_CORRUPT;
        tiers_store *target = NULL;
        for (size_t i = 0; i < v->nstores; i++) {
            tiers_store *s = &v->stores[i];
            if (s->state == TIERS_STATE_ACTIVE && memcmp(s->id, id, 16) == 0) {
                target = s;
                break;
            }
            if (s->state == TIERS_STATE_REPLACED_BY && memcmp(s->replaced_by, id, 16) == 0) {
                target = s;
                break;
            }
        }
        if (!target)
            return TSDB_ERR_CORRUPT;
        if (target->state == TIERS_STATE_REPLACED_BY) {
            memcpy(target->id, target->replaced_by, 16);
            memset(target->replaced_by, 0, 16);
        }
        target->state = TIERS_STATE_LOST;
        return 0;
    }
    if (state == TIERS_STATE_REPLACED_BY) {
        if (is_all_zero(new_id) || new_cls > TSDB_TIER_CLASS_HDD || (new_flags & ~TIERS_STORE_SINGLE_COPY) != 0 ||
            np_len == 0 || np_len >= PATH_MAX || memchr(pl + 37, '\0', np_len) != NULL)
            return TSDB_ERR_CORRUPT;
        char npath[PATH_MAX];
        memcpy(npath, pl + 37, np_len);
        npath[np_len] = '\0';
        if (tiers_path_valid(npath) != 0 || seen_id_has(v, new_id))
            return TSDB_ERR_CORRUPT;
        tiers_store *target = NULL;
        size_t tidx = 0;
        for (size_t i = 0; i < v->nstores; i++) {
            if (v->stores[i].state == TIERS_STATE_LOST && memcmp(v->stores[i].id, id, 16) == 0) {
                target = &v->stores[i];
                tidx = i;
                break;
            }
        }
        if (!target)
            return TSDB_ERR_CORRUPT;
        /* Replacement path may reuse the lost slot's own mount point, but not another live store's path. */
        for (size_t i = 0; i < v->nstores; i++) {
            if (i != tidx && v->stores[i].state != TIERS_STATE_LOST && strcmp(v->stores[i].path, npath) == 0)
                return TSDB_ERR_CORRUPT;
        }
        char *pcopy = strdup(npath);
        if (!pcopy)
            return TSDB_ERR_NOMEM;
        int rc = seen_id_add(v, new_id);
        if (rc) {
            free(pcopy);
            return rc;
        }
        if (!target->orig_path)
            target->orig_path = target->path;
        else
            free(target->path);
        target->path = pcopy;
        memcpy(target->replaced_by, new_id, 16);
        target->cls = new_cls;
        target->single_copy = (new_flags & TIERS_STORE_SINGLE_COPY) != 0;
        target->state = TIERS_STATE_REPLACED_BY;
        return 0;
    }
    return TSDB_ERR_CORRUPT;
}

static int apply_settings(tiers_view *v, const uint8_t *pl, size_t plen)
{
    if (plen != TIERS_REC_SETTINGS_PLEN)
        return TSDB_ERR_CORRUPT;
    uint32_t volume = ld32(pl + 0);
    tiers_settings s;
    memset(&s, 0, sizeof s);
    s.set_mask = ld32(pl + 4);
    s.k = ld16(pl + 8);
    s.m = ld16(pl + 10);
    s.tier_high_pct = ld32(pl + 12);
    s.young_high_pct = ld32(pl + 16);
    s.rebuild_mb_s = ld32(pl + 20);
    s.scrub_mb_s = ld32(pl + 24);
    s.scrub_days = ld32(pl + 28);
    s.tier_after_s = ld64(pl + 32);
    if (!settings_valid(volume, &s))
        return TSDB_ERR_CORRUPT;
    if (volume == TIERS_VOL_ALL) {
        settings_merge(&v->global, &s);
        return 0;
    }
    for (size_t i = 0; i < v->nvols; i++) {
        if (v->vols[i].volume == volume) {
            settings_merge(&v->vols[i].s, &s);
            return 0;
        }
    }
    tiers_vol_settings *nv = realloc(v->vols, (v->nvols + 1) * sizeof *nv);
    if (!nv)
        return TSDB_ERR_NOMEM;
    v->vols = nv;
    tiers_vol_settings *vs = &v->vols[v->nvols++];
    vs->volume = volume;
    memset(&vs->s, 0, sizeof vs->s);
    settings_merge(&vs->s, &s);
    return 0;
}

int tiers_parse_buf(const uint8_t *buf, size_t size, const char *label, tiers_view *out, size_t *good_end)
{
    const char *lbl = label ? label : TIERS_FILE;
    tiers_view_init(out);
    if (good_end)
        *good_end = 0;

    uint8_t type;
    const uint8_t *pl;
    size_t plen, rs, p = 0;
    while (parse_rec(buf, p, size, &type, &pl, &plen, &rs))
        p += rs;

    if (p != size && valid_after(buf, p, size)) {
        fprintf(stderr, "tsdb: %s is damaged at offset %zu, before valid records\n", lbl, p);
        tiers_view_free(out);
        return TSDB_ERR_CORRUPT;
    }

    int rc = 0;
    size_t pos = 0;
    while (rc == 0 && pos < p && parse_rec(buf, pos, size, &type, &pl, &plen, &rs)) {
        if (type == TIERS_REC_STORE)
            rc = apply_store(out, pl, plen);
        else if (type == TIERS_REC_STATE)
            rc = apply_state(out, pl, plen);
        else if (type == TIERS_REC_SETTINGS)
            rc = apply_settings(out, pl, plen);
        else
            rc = TSDB_ERR_CORRUPT;
        if (rc == TSDB_ERR_CORRUPT)
            fprintf(stderr, "tsdb: %s: record at offset %zu (type %u) is invalid\n", lbl, pos, (unsigned)type);
        if (rc == 0)
            pos += rs;
    }
    if (rc != 0) {
        tiers_view_free(out);
        return rc;
    }
    out->file_end = pos;
    if (good_end)
        *good_end = pos;
    return 0;
}

/* ---------------------------------------------------------------- file I/O */

static int read_at_exact(int fd, uint8_t *buf, size_t len, uint64_t off)
{
    size_t got = 0;
    while (got < len) {
        ssize_t r = pread(fd, buf + got, len - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            if (r == 0)
                errno = EIO;
            return TSDB_ERR_IO;
        }
        got += (size_t)r;
    }
    return 0;
}

static int write_at_exact(int fd, const uint8_t *buf, size_t len, uint64_t off)
{
    size_t put = 0;
    while (put < len) {
        ssize_t w = pwrite(fd, buf + put, len - put, (off_t)(off + put));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            if (w == 0)
                errno = ENOSPC;
            return TSDB_ERR_IO;
        }
        put += (size_t)w;
    }
    return 0;
}

int tiers_load_fd(int fd, bool cut, bool sync, const char *label, tiers_view *out)
{
    const char *lbl = label ? label : TIERS_FILE;
    tiers_view_init(out);
    struct stat st;
    if (fstat(fd, &st) != 0) {
        fprintf(stderr, "tsdb: %s: fstat failed: %s\n", lbl, strerror(errno));
        return TSDB_ERR_IO;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "tsdb: %s is not a regular file\n", lbl);
        return TSDB_ERR_IO;
    }
    if (st.st_size < 0 || (uint64_t)st.st_size > (4ull << 20)) {
        fprintf(stderr, "tsdb: %s has invalid size %lld\n", lbl, (long long)st.st_size);
        return TSDB_ERR_CORRUPT;
    }
    size_t size = (size_t)st.st_size;
    uint8_t *buf = malloc(size ? size : 1);
    if (!buf)
        return TSDB_ERR_NOMEM;
    if (size > 0 && read_at_exact(fd, buf, size, 0) != 0) {
        fprintf(stderr, "tsdb: %s: read failed: %s\n", lbl, strerror(errno));
        free(buf);
        return TSDB_ERR_IO;
    }
    size_t good_end = 0;
    int rc = tiers_parse_buf(buf, size, lbl, out, &good_end);
    if (rc == 0)
        out->exists = true;
    if (rc == 0 && cut && good_end < size) {
        size_t tail = size - good_end;
        uint8_t *again = malloc(tail);
        if (!again) {
            rc = TSDB_ERR_NOMEM;
        } else {
            (void)posix_fadvise(fd, (off_t)good_end, (off_t)tail, POSIX_FADV_DONTNEED);
            if (read_at_exact(fd, again, tail, good_end) != 0) {
                fprintf(stderr, "tsdb: %s: second read of tail at offset %zu failed: nothing cut\n", lbl, good_end);
                rc = TSDB_ERR_IO;
            } else if (memcmp(again, buf + good_end, tail) != 0) {
                fprintf(stderr, "tsdb: %s: two reads of tail at offset %zu differ: nothing cut\n", lbl, good_end);
                rc = TSDB_ERR_IO;
            }
            free(again);
        }
        if (rc == 0) {
            fprintf(stderr, "tsdb: %s: cutting a torn tail of %zu bytes at offset %zu\n", lbl, tail, good_end);
            if (ftruncate(fd, (off_t)good_end) != 0 ||
                (sync && tiers_mut() != TIERS_MUT_NOSYNC && tsdb_fdatasync(fd) != 0)) {
                fprintf(stderr, "tsdb: %s: truncating torn tail failed: %s\n", lbl, strerror(errno));
                rc = TSDB_ERR_IO;
            }
        }
        if (rc != 0)
            tiers_view_free(out);
    }
    free(buf);
    return rc;
}

int tiers_load(int rootfd, bool cut, bool sync, tiers_view *out)
{
    tiers_view_init(out);
    int fd = openat(rootfd, TIERS_FILE, (cut ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT)
            return 0; /* exists = false, empty view */
        fprintf(stderr, "tsdb: %s: open failed: %s\n", TIERS_FILE, strerror(errno));
        return TSDB_ERR_IO;
    }
    int rc = tiers_load_fd(fd, cut, sync, TIERS_FILE, out);
    close(fd);
    return rc;
}

/* Appends an already-encoded record at `off` into open `fd`, syncing data and (if created or off==0) rootfd.
 * On failure, cuts back to `off` (best effort, as USERS) and returns TSDB_ERR_IO. */
static int append_raw_locked(int rootfd, int fd, bool created, uint64_t off, const uint8_t *rec, size_t len, bool sync)
{
    int rc = write_at_exact(fd, rec, len, off);
    if (rc == 0 && sync && tiers_mut() != TIERS_MUT_NOSYNC && tsdb_fdatasync(fd) != 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && (created || off == 0) && sync && tiers_mut() != TIERS_MUT_NOSYNC && tsdb_fsync(rootfd) != 0)
        rc = TSDB_ERR_IO;
    if (rc != 0) {
        int e = errno;
        fprintf(stderr, "tsdb: %s: append or sync failed (%s): rolling back\n", TIERS_FILE,
                strerror(e ? e : EIO));
        if (ftruncate(fd, (off_t)off) != 0)
            fprintf(stderr, "tsdb: %s: rollback truncate failed: %s\n", TIERS_FILE, strerror(errno));
        errno = e;
    }
    return rc;
}

static int open_for_append(int rootfd, bool sync, int *fd_out, bool *created_out, tiers_view *view_out)
{
    *fd_out = -1;
    *created_out = false;
    bool existed = faccessat(rootfd, TIERS_FILE, F_OK, AT_SYMLINK_NOFOLLOW) == 0;
    int fd = openat(rootfd, TIERS_FILE, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "tsdb: %s: open failed: %s\n", TIERS_FILE, strerror(errno));
        return TSDB_ERR_IO;
    }
    int rc = tiers_load_fd(fd, true, sync, TIERS_FILE, view_out);
    if (rc != 0) {
        close(fd);
        return rc;
    }
    *fd_out = fd;
    *created_out = !existed;
    return 0;
}

int tiers_append_store(int rootfd, bool sync, const uint8_t id[16], uint8_t cls, bool single_copy, const char *path)
{
    int fd = -1;
    bool created = false;
    tiers_view v;
    int rc = open_for_append(rootfd, sync, &fd, &created, &v);
    if (rc != 0)
        return rc;

    uint8_t rec[PATH_MAX + 64];
    int rlen = tiers_encode_store(v.file_end, id, cls, single_copy, path, rec, sizeof rec);
    if (rlen < 0) {
        rc = rlen;
    } else {
        /* Dry-run apply on in-memory view to reject duplicate ids/paths or exceeding max stores before writing. */
        rc = apply_store(&v, rec + 5, (size_t)ld32(rec));
        if (rc == TSDB_ERR_CORRUPT)
            rc = TSDB_ERR_INVALID;
    }
    if (rc == 0)
        rc = append_raw_locked(rootfd, fd, created, v.file_end, rec, (size_t)rlen, sync);
    close(fd);
    tiers_view_free(&v);
    return rc;
}

int tiers_append_state(int rootfd, bool sync, const uint8_t id[16], uint8_t state, uint8_t new_cls,
                       bool new_single_copy, const uint8_t new_id[16], const char *new_path)
{
    int fd = -1;
    bool created = false;
    tiers_view v;
    int rc = open_for_append(rootfd, sync, &fd, &created, &v);
    if (rc != 0)
        return rc;

    uint8_t rec[PATH_MAX + 80];
    int rlen = tiers_encode_state(v.file_end, id, state, new_cls, new_single_copy, new_id, new_path, rec, sizeof rec);
    if (rlen < 0) {
        rc = rlen;
    } else {
        rc = apply_state(&v, rec + 5, (size_t)ld32(rec));
        if (rc == TSDB_ERR_CORRUPT)
            rc = TSDB_ERR_INVALID;
    }
    if (rc == 0)
        rc = append_raw_locked(rootfd, fd, created, v.file_end, rec, (size_t)rlen, sync);
    close(fd);
    tiers_view_free(&v);
    return rc;
}

int tiers_append_settings(int rootfd, bool sync, uint32_t volume, const tiers_settings *s)
{
    int fd = -1;
    bool created = false;
    tiers_view v;
    int rc = open_for_append(rootfd, sync, &fd, &created, &v);
    if (rc != 0)
        return rc;

    uint8_t rec[64];
    int rlen = tiers_encode_settings(v.file_end, volume, s, rec, sizeof rec);
    if (rlen < 0) {
        rc = rlen;
    } else {
        rc = apply_settings(&v, rec + 5, (size_t)ld32(rec));
        if (rc == TSDB_ERR_CORRUPT)
            rc = TSDB_ERR_INVALID;
    }
    if (rc == 0)
        rc = append_raw_locked(rootfd, fd, created, v.file_end, rec, (size_t)rlen, sync);
    close(fd);
    tiers_view_free(&v);
    return rc;
}

/* ---------------------------------------------------------------- root META db_id */

static int read_meta_text(int rootfd, char **out, size_t *len_out)
{
    *out = NULL;
    *len_out = 0;
    int fd = openat(rootfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 || st.st_size > (1 << 20)) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t sz = (size_t)st.st_size;
    char *buf = malloc(sz + 1);
    if (!buf) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    if (sz > 0 && read_at_exact(fd, (uint8_t *)buf, sz, 0) != 0) {
        free(buf);
        close(fd);
        return TSDB_ERR_IO;
    }
    close(fd);
    buf[sz] = '\0';
    *out = buf;
    *len_out = sz;
    return 0;
}

static int parse_db_id_from_meta(const char *txt, bool *has_out, uint8_t id_out[16])
{
    *has_out = false;
    bool found = false;
    for (const char *l = txt; *l;) {
        const char *nl = strchr(l, '\n');
        size_t ll = nl ? (size_t)(nl - l) : strlen(l);
        if (ll >= 6 && memcmp(l, "db_id=", 6) == 0) {
            if (found) {
                fprintf(stderr, "tsdb: root META: duplicate db_id= line\n");
                return TSDB_ERR_CORRUPT;
            }
            if (ll != 6 + 32) {
                fprintf(stderr, "tsdb: root META: db_id= line has invalid length\n");
                return TSDB_ERR_CORRUPT;
            }
            const char *h = l + 6;
            uint8_t id[16];
            for (int i = 0; i < 16; i++) {
                int a = hex_val((unsigned char)h[2 * i]);
                int b = hex_val((unsigned char)h[2 * i + 1]);
                if (a < 0 || b < 0) {
                    fprintf(stderr, "tsdb: root META: db_id= line has invalid hex\n");
                    return TSDB_ERR_CORRUPT;
                }
                id[i] = (uint8_t)((a << 4) | b);
            }
            if (is_all_zero(id)) {
                fprintf(stderr, "tsdb: root META: db_id= is all zero\n");
                return TSDB_ERR_CORRUPT;
            }
            memcpy(id_out, id, 16);
            found = true;
        }
        l = nl ? nl + 1 : l + ll;
    }
    *has_out = found;
    return 0;
}

int tiers_db_id_read(int rootfd, bool *has_out, uint8_t id_out[16])
{
    *has_out = false;
    char *txt = NULL;
    size_t len = 0;
    int rc = read_meta_text(rootfd, &txt, &len);
    if (rc != 0)
        return rc;
    rc = parse_db_id_from_meta(txt, has_out, id_out);
    free(txt);
    return rc;
}

int tiers_db_id_ensure(int rootfd, bool sync, uint8_t id_out[16])
{
    char *txt = NULL;
    size_t len = 0;
    int rc = read_meta_text(rootfd, &txt, &len);
    if (rc != 0)
        return rc;
    bool has = false;
    rc = parse_db_id_from_meta(txt, &has, id_out);
    if (rc != 0 || has) {
        free(txt);
        return rc;
    }
    uint8_t id[16];
    do {
        if (chunk_random(id, 16) != 0) {
            free(txt);
            return TSDB_ERR_IO;
        }
    } while (is_all_zero(id));

    char hex[33];
    chunk_oid_hex(id, hex);
    bool need_nl = len > 0 && txt[len - 1] != '\n';
    size_t nlen = len + (need_nl ? 1u : 0u) + 6u + 32u + 1u;
    char *buf = malloc(nlen + 1);
    if (!buf) {
        free(txt);
        return TSDB_ERR_NOMEM;
    }
    size_t n = 0;
    if (len > 0) {
        memcpy(buf, txt, len);
        n = len;
    }
    if (need_nl)
        buf[n++] = '\n';
    memcpy(buf + n, "db_id=", 6);
    n += 6;
    memcpy(buf + n, hex, 32);
    n += 32;
    buf[n++] = '\n';
    free(txt);

    char tmp[128];
    meta_tmp_make(tmp, sizeof tmp);
    int fd = openat(rootfd, tmp, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0) {
        free(buf);
        return TSDB_ERR_IO;
    }
    rc = write_at_exact(fd, (const uint8_t *)buf, n, 0);
    if (rc == 0 && sync && tiers_mut() != TIERS_MUT_NOSYNC && tsdb_fsync(fd) != 0)
        rc = TSDB_ERR_IO;
    close(fd);
    free(buf);
    if (rc == 0 && renameat(rootfd, tmp, rootfd, "META") != 0)
        rc = TSDB_ERR_IO;
    if (rc != 0) {
        unlinkat(rootfd, tmp, 0);
        return rc;
    }
    if (sync && tiers_mut() != TIERS_MUT_NOSYNC && tsdb_fsync(rootfd) != 0)
        return TSDB_ERR_IO;
    memcpy(id_out, id, 16);
    return 0;
}
