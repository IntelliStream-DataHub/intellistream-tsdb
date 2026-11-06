/*
 * The cluster's metadata state machine; see meta.h for what it holds and the rules it keeps.
 *
 * Bytes, all little-endian:
 *   entry     u32 magic "TME1", u32 nops, then nops times { u8 kind, u32 len, body[len] }
 *   snapshot  u32 magic "TMS1", u32 version 1, u64 applied, u64 applied_term, the body below, u32 crc32c of all before
 * Strings are u16 length + bytes (no terminator), bounded by their field. A body is read by a reader that fails on
 * any overrun, so a damaged entry is META_ERR_FORMAT, never a read past the end.
 *
 * One rule beyond meta.h's: an entry touches a volume at most once (a second operation on it is META_ERR_INVALID).
 * Then a volume's change in one entry is one step, which meta_check_step can judge from the states on either side.
 */
#include "meta.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ENTRY_MAGIC 0x31454d54u /* "TME1" */
#define SNAP_MAGIC 0x31534d54u  /* "TMS1" */

uint32_t iso_crc32c(uint32_t crc, const void *buf, size_t len); /* src/raft/compat.c */

const char *meta_strerror(int rc)
{
    switch (rc) {
    case META_OK: return "ok";
    case META_ERR_FORMAT: return "an entry or image that does not decode";
    case META_ERR_EXISTS: return "already exists";
    case META_ERR_NOTFOUND: return "not found";
    case META_ERR_STALE: return "a stale epoch or ISR version";
    case META_ERR_INVALID: return "not allowed";
    case META_ERR_STATE: return "not in this state of the cluster";
    }
    return "unknown";
}

const char *meta_member_state_name(int s)
{
    static const char *n[] = {"up", "down", "draining", "out"};
    return s >= 0 && s < 4 ? n[s] : "?";
}

void meta_settings_default(struct meta_settings *s)
{
    s->replication = 3;
    s->min_isr = 1;
    s->node_timeout_ms = 3000;
    s->heartbeat_ms = 500;
    s->isr_lag_ms = 1000;
    s->out_after_s = 1800;
    s->voter_replace_s = 600;
    s->voters = 5;
    s->volumes_per_server = 16;
}

uint32_t meta_voter_target(const struct meta_settings *s, uint32_t servers)
{
    uint32_t t = servers >= 5 ? 5 : servers >= 3 ? 3 : 1;
    uint32_t max = s->voters ? s->voters : 5;
    if (max % 2 == 0)
        max--; /* an even group tolerates no more losses than the odd one below it */
    return t < max ? t : max;
}

void meta_init(struct meta_state *m)
{
    memset(m, 0, sizeof *m);
    meta_settings_default(&m->set);
    m->next_member = 1;
    m->slot_home = malloc(META_SLOTS * sizeof *m->slot_home);
    if (!m->slot_home)
        abort();
    for (uint32_t i = 0; i < META_SLOTS; i++)
        m->slot_home[i] = META_NONE;
}

void meta_free(struct meta_state *m)
{
    for (uint32_t i = 0; i < m->nvolumes; i++)
        free(m->volumes[i].hist);
    for (uint32_t i = 0; i < m->nusers; i++)
        free(m->users[i].blob);
    free(m->members);
    free(m->volumes);
    free(m->slot_home);
    free(m->moves);
    free(m->users);
    memset(m, 0, sizeof *m);
}

static void *grow(void *p, uint32_t *cap, uint32_t need, size_t size)
{
    if (need <= *cap)
        return p;
    uint32_t c = *cap ? *cap : 8;
    while (c < need)
        c *= 2;
    void *q = realloc(p, (size_t)c * size);
    if (!q)
        abort();
    *cap = c;
    return q;
}

void meta_copy(struct meta_state *dst, const struct meta_state *src)
{
    *dst = *src;
    dst->members = malloc((src->nmembers + 1) * sizeof *dst->members);
    dst->volumes = malloc((src->nvolumes + 1) * sizeof *dst->volumes);
    dst->moves = malloc((src->nmoves + 1) * sizeof *dst->moves);
    dst->users = malloc((src->nusers + 1) * sizeof *dst->users);
    dst->slot_home = malloc(META_SLOTS * sizeof *dst->slot_home);
    if (!dst->members || !dst->volumes || !dst->moves || !dst->users || !dst->slot_home)
        abort();
    dst->members_cap = src->nmembers + 1;
    dst->volumes_cap = src->nvolumes + 1;
    dst->moves_cap = src->nmoves + 1;
    dst->users_cap = src->nusers + 1;
    memcpy(dst->members, src->members, src->nmembers * sizeof *dst->members);
    memcpy(dst->volumes, src->volumes, src->nvolumes * sizeof *dst->volumes);
    memcpy(dst->moves, src->moves, src->nmoves * sizeof *dst->moves);
    memcpy(dst->users, src->users, src->nusers * sizeof *dst->users);
    memcpy(dst->slot_home, src->slot_home, META_SLOTS * sizeof *dst->slot_home);
    for (uint32_t i = 0; i < src->nvolumes; i++) {
        struct meta_volume *v = &dst->volumes[i];
        v->hist = malloc((v->nhist + 1) * sizeof *v->hist);
        if (!v->hist)
            abort();
        memcpy(v->hist, src->volumes[i].hist, v->nhist * sizeof *v->hist);
    }
    for (uint32_t i = 0; i < src->nusers; i++) {
        struct meta_user *u = &dst->users[i];
        u->blob = malloc(u->len + 1);
        if (!u->blob)
            abort();
        memcpy(u->blob, src->users[i].blob, u->len);
    }
}

/* ---------------------------------------------------------------- lookups */

static int32_t member_at(const struct meta_state *m, uint32_t id)
{
    uint32_t lo = 0, hi = m->nmembers;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (m->members[mid].id < id)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < m->nmembers && m->members[lo].id == id ? (int32_t)lo : -1 - (int32_t)lo;
}

static int32_t volume_at(const struct meta_state *m, uint32_t number)
{
    uint32_t lo = 0, hi = m->nvolumes;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (m->volumes[mid].number < number)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < m->nvolumes && m->volumes[lo].number == number ? (int32_t)lo : -1 - (int32_t)lo;
}

static int32_t user_at(const struct meta_state *m, const char *name)
{
    uint32_t lo = 0, hi = m->nusers;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (strcmp(m->users[mid].name, name) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < m->nusers && !strcmp(m->users[lo].name, name) ? (int32_t)lo : -1 - (int32_t)lo;
}

const struct meta_member *meta_member(const struct meta_state *m, uint32_t id)
{
    int32_t i = member_at(m, id);
    return i >= 0 ? &m->members[i] : NULL;
}

const struct meta_volume *meta_volume(const struct meta_state *m, uint32_t number)
{
    int32_t i = volume_at(m, number);
    return i >= 0 ? &m->volumes[i] : NULL;
}

int meta_volume_in_isr(const struct meta_volume *v, uint32_t node)
{
    for (uint32_t i = 0; i < v->nisr; i++)
        if (v->isr[i] == node)
            return 1;
    return 0;
}

static int replica_at(const struct meta_volume *v, uint32_t node)
{
    for (uint32_t i = 0; i < v->nrep; i++)
        if (v->rep[i].node == node)
            return (int)i;
    return -1;
}

/* ---------------------------------------------------------------- bytes */

struct wr {
    uint8_t **buf;
    size_t *len, *cap;
};

static void put(struct wr *w, const void *p, size_t n)
{
    if (*w->len + n > *w->cap) {
        size_t c = *w->cap ? *w->cap : 256;
        while (c < *w->len + n)
            c *= 2;
        uint8_t *b = realloc(*w->buf, c);
        if (!b)
            abort();
        *w->buf = b;
        *w->cap = c;
    }
    memcpy(*w->buf + *w->len, p, n);
    *w->len += n;
}

static void put8(struct wr *w, uint8_t v) { put(w, &v, 1); }
static void put16(struct wr *w, uint16_t v) { put(w, &v, 2); }
static void put32(struct wr *w, uint32_t v) { put(w, &v, 4); }
static void put64(struct wr *w, uint64_t v) { put(w, &v, 8); }

static void putstr(struct wr *w, const char *s, size_t max)
{
    size_t n = strnlen(s, max - 1);
    put16(w, (uint16_t)n);
    put(w, s, n);
}

struct rd {
    const uint8_t *p;
    size_t len, off;
    int bad;
};

static int get(struct rd *r, void *out, size_t n)
{
    if (r->bad || n > r->len - r->off) {
        r->bad = 1;
        memset(out, 0, n);
        return -1;
    }
    memcpy(out, r->p + r->off, n);
    r->off += n;
    return 0;
}

static uint8_t get8(struct rd *r) { uint8_t v; get(r, &v, 1); return v; }
static uint16_t get16(struct rd *r) { uint16_t v; get(r, &v, 2); return v; }
static uint32_t get32(struct rd *r) { uint32_t v; get(r, &v, 4); return v; }
static uint64_t get64(struct rd *r) { uint64_t v; get(r, &v, 8); return v; }

/* A string of at most max - 1 bytes, no NUL inside; a longer one is damage. */
static void getstr(struct rd *r, char *out, size_t max)
{
    uint16_t n = get16(r);
    if (n >= max) {
        r->bad = 1;
        out[0] = 0;
        return;
    }
    get(r, out, n);
    out[n] = 0;
    if (strlen(out) != n)
        r->bad = 1;
}

/* ---------------------------------------------------------------- entries */

void meta_entry_init(struct meta_entry *e)
{
    memset(e, 0, sizeof *e);
    struct wr w = {&e->buf, &e->len, &e->cap};
    put32(&w, ENTRY_MAGIC);
    put32(&w, 0);
}

void meta_entry_free(struct meta_entry *e)
{
    free(e->buf);
    memset(e, 0, sizeof *e);
}

void meta_entry_finish(struct meta_entry *e)
{
    memcpy(e->buf + 4, &e->nops, 4);
}

/* An operation: kind, then its body's length, filled in by op_end. */
static size_t op_begin(struct meta_entry *e, struct wr *w, uint8_t kind)
{
    w->buf = &e->buf;
    w->len = &e->len;
    w->cap = &e->cap;
    put8(w, kind);
    put32(w, 0);
    return e->len;
}

static void op_end(struct meta_entry *e, size_t start)
{
    uint32_t n = (uint32_t)(e->len - start);
    memcpy(e->buf + start - 4, &n, 4);
    e->nops++;
}

static void put_settings(struct wr *w, const struct meta_settings *s)
{
    put32(w, 9);
    put32(w, s->replication);
    put32(w, s->min_isr);
    put32(w, s->node_timeout_ms);
    put32(w, s->heartbeat_ms);
    put32(w, s->isr_lag_ms);
    put32(w, s->out_after_s);
    put32(w, s->voter_replace_s);
    put32(w, s->voters);
    put32(w, s->volumes_per_server);
}

static void get_settings(struct rd *r, struct meta_settings *s)
{
    uint32_t n = get32(r);
    if (n != 9) { /* a later build adds fields at the end and raises the count; this one knows 9 */
        r->bad = 1;
        return;
    }
    s->replication = get32(r);
    s->min_isr = get32(r);
    s->node_timeout_ms = get32(r);
    s->heartbeat_ms = get32(r);
    s->isr_lag_ms = get32(r);
    s->out_after_s = get32(r);
    s->voter_replace_s = get32(r);
    s->voters = get32(r);
    s->volumes_per_server = get32(r);
}

static void put_member(struct wr *w, const struct meta_member *mb)
{
    put32(w, mb->id);
    put8(w, mb->state);
    put64(w, mb->incarnation);
    putstr(w, mb->addr, META_ADDR_MAX);
    putstr(w, mb->peer, META_ADDR_MAX);
    putstr(w, mb->rack, META_RACK_MAX);
}

static void get_member(struct rd *r, struct meta_member *mb)
{
    memset(mb, 0, sizeof *mb);
    mb->id = get32(r);
    mb->state = get8(r);
    mb->incarnation = get64(r);
    getstr(r, mb->addr, META_ADDR_MAX);
    getstr(r, mb->peer, META_ADDR_MAX);
    getstr(r, mb->rack, META_RACK_MAX);
    if (mb->state > META_OUT)
        r->bad = 1;
}

static void put_ids(struct wr *w, const uint32_t *ids, uint32_t n)
{
    put32(w, n);
    for (uint32_t i = 0; i < n; i++)
        put32(w, ids[i]);
}

static uint32_t get_ids(struct rd *r, uint32_t *ids)
{
    uint32_t n = get32(r);
    if (n > META_REPLICAS_MAX) {
        r->bad = 1;
        return 0;
    }
    for (uint32_t i = 0; i < n; i++)
        ids[i] = get32(r);
    return n;
}

void meta_op_init(struct meta_entry *e, const uint8_t uuid[16], const struct meta_settings *s,
                  const struct meta_member *first)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_INIT);
    put(&w, uuid, 16);
    put_settings(&w, s);
    put_member(&w, first);
    op_end(e, st);
}

void meta_op_member_add(struct meta_entry *e, const struct meta_member *mb)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_MEMBER_ADD);
    put_member(&w, mb);
    op_end(e, st);
}

void meta_op_member_state(struct meta_entry *e, uint32_t id, int state, uint64_t incarnation)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_MEMBER_STATE);
    put32(&w, id);
    put8(&w, (uint8_t)state);
    put64(&w, incarnation);
    op_end(e, st);
}

void meta_op_member_addr(struct meta_entry *e, uint32_t id, uint64_t incarnation, const char *addr,
                         const char *peer)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_MEMBER_ADDR);
    put32(&w, id);
    put64(&w, incarnation);
    putstr(&w, addr, META_ADDR_MAX);
    putstr(&w, peer, META_ADDR_MAX);
    op_end(e, st);
}

void meta_op_settings(struct meta_entry *e, const struct meta_settings *s)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SETTINGS);
    put_settings(&w, s);
    op_end(e, st);
}

void meta_op_volume_add(struct meta_entry *e, uint32_t number, const char *name, const uint8_t uuid[16],
                        uint32_t min_isr, const uint32_t *nodes, uint32_t n)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_ADD);
    put32(&w, number);
    putstr(&w, name, META_NAME_MAX);
    put(&w, uuid, 16);
    put32(&w, min_isr);
    put_ids(&w, nodes, n);
    op_end(e, st);
}

void meta_op_volume_leader(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t leader,
                           const uint32_t *isr, uint32_t nisr, uint64_t start)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_LEADER);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, leader);
    put_ids(&w, isr, nisr);
    put64(&w, start);
    op_end(e, st);
}

void meta_op_isr_change(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t isr_version,
                        const uint32_t *isr, uint32_t nisr)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_ISR_CHANGE);
    put32(&w, number);
    put64(&w, epoch);
    put64(&w, isr_version);
    put_ids(&w, isr, nisr);
    op_end(e, st);
}

void meta_op_replica_add(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_ADD);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, node);
    op_end(e, st);
}

void meta_op_replica_remove(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_REMOVE);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, node);
    op_end(e, st);
}

void meta_op_slot_home(struct meta_entry *e, uint32_t first, uint32_t count, uint32_t volume)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SLOT_HOME);
    put32(&w, first);
    put32(&w, count);
    put32(&w, volume);
    op_end(e, st);
}

void meta_op_user_put(struct meta_entry *e, const char *name, const void *blob, uint32_t len)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_USER_PUT);
    putstr(&w, name, META_NAME_MAX);
    put32(&w, len);
    put(&w, blob, len);
    op_end(e, st);
}

void meta_op_user_del(struct meta_entry *e, const char *name)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_USER_DEL);
    putstr(&w, name, META_NAME_MAX);
    op_end(e, st);
}

/* ---------------------------------------------------------------- apply */

static void sort_ids(uint32_t *ids, uint32_t n)
{
    for (uint32_t i = 1; i < n; i++)
        for (uint32_t j = i; j > 0 && ids[j - 1] > ids[j]; j--) {
            uint32_t t = ids[j];
            ids[j] = ids[j - 1];
            ids[j - 1] = t;
        }
}

/* distinct ids, every one a replica of `v` */
static int ids_valid(const struct meta_volume *v, const uint32_t *ids, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        if (replica_at(v, ids[i]) < 0)
            return 0;
        for (uint32_t j = 0; j < i; j++)
            if (ids[j] == ids[i])
                return 0;
    }
    return 1;
}

static int has_id(const uint32_t *ids, uint32_t n, uint32_t id)
{
    for (uint32_t i = 0; i < n; i++)
        if (ids[i] == id)
            return 1;
    return 0;
}

static int addr_taken(const struct meta_state *m, const char *peer)
{
    for (uint32_t i = 0; i < m->nmembers; i++)
        if (m->members[i].state != META_OUT && !strcmp(m->members[i].peer, peer))
            return 1;
    return 0;
}

static int insert_member(struct meta_state *m, const struct meta_member *mb)
{
    if (mb->id == 0 || mb->id == META_NONE || !mb->peer[0])
        return META_ERR_INVALID;
    int32_t at = member_at(m, mb->id);
    if (at >= 0 || mb->id < m->next_member || addr_taken(m, mb->peer))
        return META_ERR_EXISTS; /* ids are never reused, nor a live peer address shared */
    uint32_t pos = (uint32_t)(-1 - at);
    m->members = grow(m->members, &m->members_cap, m->nmembers + 1, sizeof *m->members);
    memmove(&m->members[pos + 1], &m->members[pos], (m->nmembers - pos) * sizeof *m->members);
    m->members[pos] = *mb;
    m->members[pos].state = META_UP;
    m->nmembers++;
    m->next_member = mb->id + 1;
    return META_OK;
}

static int apply_op(struct meta_state *m, uint8_t kind, struct rd *r, uint32_t *touched, uint32_t *ntouched)
{
    if (kind != META_OP_INIT && !m->initialised) {
        /* decode anyway, so a damaged body is still a format error */
        r->off = r->len;
        return META_ERR_STATE;
    }
    switch (kind) {
    case META_OP_INIT: {
        uint8_t uuid[16];
        struct meta_settings s;
        struct meta_member mb;
        get(r, uuid, 16);
        get_settings(r, &s);
        get_member(r, &mb);
        if (r->bad)
            return META_ERR_FORMAT;
        if (m->initialised)
            return META_ERR_STATE;
        if (s.replication < 1 || s.replication > META_REPLICAS_MAX || s.min_isr < 1 || s.min_isr > s.replication)
            return META_ERR_INVALID;
        int rc = insert_member(m, &mb);
        if (rc)
            return rc;
        memcpy(m->uuid, uuid, 16);
        m->set = s;
        m->initialised = 1;
        return META_OK;
    }
    case META_OP_MEMBER_ADD: {
        struct meta_member mb;
        get_member(r, &mb);
        if (r->bad)
            return META_ERR_FORMAT;
        return insert_member(m, &mb);
    }
    case META_OP_MEMBER_STATE: {
        uint32_t id = get32(r);
        uint8_t st = get8(r);
        uint64_t inc = get64(r);
        if (r->bad || st > META_OUT)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, id);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state == META_OUT)
            return META_ERR_STATE; /* out is final: the server comes back under a new id */
        if (inc < mb->incarnation)
            return META_ERR_STALE; /* a decision about an earlier run of that server */
        mb->state = st;
        mb->incarnation = inc;
        return META_OK;
    }
    case META_OP_SETTINGS: {
        struct meta_settings s;
        get_settings(r, &s);
        if (r->bad)
            return META_ERR_FORMAT;
        if (s.replication < 1 || s.replication > META_REPLICAS_MAX || s.min_isr < 1 || s.min_isr > s.replication)
            return META_ERR_INVALID;
        m->set = s;
        return META_OK;
    }
    case META_OP_VOLUME_ADD: {
        uint32_t number = get32(r);
        char name[META_NAME_MAX];
        uint8_t uuid[16];
        uint32_t nodes[META_REPLICAS_MAX];
        getstr(r, name, sizeof name);
        get(r, uuid, 16);
        uint32_t min_isr = get32(r);
        uint32_t n = get_ids(r, nodes);
        if (r->bad)
            return META_ERR_FORMAT;
        if (number > META_VOLUME_MAX || !name[0] || n < 1 || min_isr < 1 || min_isr > n)
            return META_ERR_INVALID;
        if (volume_at(m, number) >= 0 || number < m->next_volume)
            return META_ERR_EXISTS;
        for (uint32_t i = 0; i < m->nvolumes; i++)
            if (!strcmp(m->volumes[i].name, name))
                return META_ERR_EXISTS;
        for (uint32_t i = 0; i < n; i++) {
            const struct meta_member *mb = meta_member(m, nodes[i]);
            if (!mb || mb->state == META_OUT || has_id(nodes, i, nodes[i]))
                return META_ERR_INVALID;
        }
        int32_t at = volume_at(m, number);
        uint32_t pos = (uint32_t)(-1 - at);
        m->volumes = grow(m->volumes, &m->volumes_cap, m->nvolumes + 1, sizeof *m->volumes);
        memmove(&m->volumes[pos + 1], &m->volumes[pos], (m->nvolumes - pos) * sizeof *m->volumes);
        struct meta_volume *v = &m->volumes[pos];
        memset(v, 0, sizeof *v);
        v->number = number;
        memcpy(v->name, name, sizeof name);
        memcpy(v->uuid, uuid, 16);
        v->min_isr = min_isr;
        v->nrep = n;
        for (uint32_t i = 0; i < n; i++)
            v->rep[i].node = nodes[i];
        v->leader = nodes[0];
        v->epoch = 1;
        v->nisr = n;
        memcpy(v->isr, nodes, n * sizeof *nodes);
        sort_ids(v->isr, n);
        v->isr_version = 1;
        v->hist = malloc(sizeof *v->hist);
        if (!v->hist)
            abort();
        v->hist[0] = (struct meta_epoch){1, 0};
        v->nhist = 1;
        m->nvolumes++;
        m->next_volume = number + 1;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_LEADER:
    case META_OP_ISR_CHANGE:
    case META_OP_REPLICA_ADD:
    case META_OP_REPLICA_REMOVE: {
        uint32_t number = get32(r);
        uint64_t epoch = get64(r), isr_version = 0, start = 0;
        uint32_t node = 0, ids[META_REPLICAS_MAX], n = 0;
        if (kind == META_OP_VOLUME_LEADER) {
            node = get32(r);
            n = get_ids(r, ids);
            start = get64(r);
        } else if (kind == META_OP_ISR_CHANGE) {
            isr_version = get64(r);
            n = get_ids(r, ids);
        } else {
            node = get32(r);
        }
        if (r->bad)
            return META_ERR_FORMAT;
        int32_t at = volume_at(m, number);
        if (at < 0)
            return META_ERR_NOTFOUND;
        for (uint32_t i = 0; i < *ntouched; i++)
            if (touched[i] == number)
                return META_ERR_INVALID; /* once per entry (head comment) */
        struct meta_volume *v = &m->volumes[at];
        if (epoch != v->epoch || (kind == META_OP_ISR_CHANGE && isr_version != v->isr_version))
            return META_ERR_STALE;
        if (kind == META_OP_VOLUME_LEADER) {
            /* C12: the new leader and the new ISR come from the ISR of the epoch that ends */
            if (n < 1 || !has_id(ids, n, node) || !meta_volume_in_isr(v, node))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < n; i++)
                if (!meta_volume_in_isr(v, ids[i]) || has_id(ids, i, ids[i]))
                    return META_ERR_INVALID;
            v->hist = realloc(v->hist, (v->nhist + 1) * sizeof *v->hist);
            if (!v->hist)
                abort();
            v->epoch++;
            v->hist[v->nhist++] = (struct meta_epoch){v->epoch, start};
            v->leader = node;
            v->nisr = n;
            memcpy(v->isr, ids, n * sizeof *ids);
            sort_ids(v->isr, n);
            v->isr_version++;
        } else if (kind == META_OP_ISR_CHANGE) {
            if (n < 1 || !ids_valid(v, ids, n) || !has_id(ids, n, v->leader))
                return META_ERR_INVALID;
            v->nisr = n;
            memcpy(v->isr, ids, n * sizeof *ids);
            sort_ids(v->isr, n);
            v->isr_version++;
            for (uint32_t i = 0; i < n; i++) /* a learner that joined the ISR is a full replica */
                v->rep[replica_at(v, ids[i])].learner = 0;
        } else if (kind == META_OP_REPLICA_ADD) {
            const struct meta_member *mb = meta_member(m, node);
            if (!mb || mb->state == META_OUT)
                return META_ERR_INVALID;
            if (replica_at(v, node) >= 0)
                return META_ERR_EXISTS;
            if (v->nrep >= META_REPLICAS_MAX)
                return META_ERR_INVALID;
            v->rep[v->nrep++] = (struct meta_replica){node, 1};
        } else {
            int i = replica_at(v, node);
            if (i < 0)
                return META_ERR_NOTFOUND;
            if (node == v->leader || meta_volume_in_isr(v, node))
                return META_ERR_INVALID; /* the ISR shrinks first: a move never lowers the in-sync copies */
            memmove(&v->rep[i], &v->rep[i + 1], (v->nrep - (uint32_t)i - 1) * sizeof v->rep[0]);
            v->nrep--;
        }
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_SLOT_HOME: {
        uint32_t first = get32(r), count = get32(r), vol = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (first >= META_SLOTS || count < 1 || count > META_SLOTS - first)
            return META_ERR_INVALID;
        if (volume_at(m, vol) < 0)
            return META_ERR_NOTFOUND;
        for (uint32_t s = first; s < first + count; s++) {
            uint32_t old = m->slot_home[s];
            if (old == vol)
                continue;
            if (old != META_NONE) { /* the new home asks the old one for names it does not know (§3.3) */
                m->moves = grow(m->moves, &m->moves_cap, m->nmoves + 1, sizeof *m->moves);
                m->moves[m->nmoves++] = (struct meta_slot_move){s, old};
            }
            m->slot_home[s] = vol;
        }
        return META_OK;
    }
    case META_OP_USER_PUT: {
        char name[META_NAME_MAX];
        getstr(r, name, sizeof name);
        uint32_t len = get32(r);
        if (r->bad || len > META_USER_BLOB_MAX || len > r->len - r->off)
            return META_ERR_FORMAT;
        if (!name[0])
            return META_ERR_INVALID;
        uint8_t *blob = malloc(len + 1);
        if (!blob)
            abort();
        get(r, blob, len);
        int32_t at = user_at(m, name);
        if (at >= 0) {
            free(m->users[at].blob);
        } else {
            uint32_t pos = (uint32_t)(-1 - at);
            m->users = grow(m->users, &m->users_cap, m->nusers + 1, sizeof *m->users);
            memmove(&m->users[pos + 1], &m->users[pos], (m->nusers - pos) * sizeof *m->users);
            m->nusers++;
            at = (int32_t)pos;
            memcpy(m->users[at].name, name, sizeof name);
        }
        m->users[at].blob = blob;
        m->users[at].len = len;
        return META_OK;
    }
    case META_OP_USER_DEL: {
        char name[META_NAME_MAX];
        getstr(r, name, sizeof name);
        if (r->bad)
            return META_ERR_FORMAT;
        int32_t at = user_at(m, name);
        if (at < 0)
            return META_ERR_NOTFOUND;
        free(m->users[at].blob);
        memmove(&m->users[at], &m->users[at + 1], (m->nusers - (uint32_t)at - 1) * sizeof *m->users);
        m->nusers--;
        return META_OK;
    }
    case META_OP_MEMBER_ADDR: {
        uint32_t id = get32(r);
        uint64_t inc = get64(r);
        char addr[META_ADDR_MAX], peer[META_ADDR_MAX];
        getstr(r, addr, sizeof addr);
        getstr(r, peer, sizeof peer);
        if (r->bad)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, id);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (!peer[0] || !addr[0])
            return META_ERR_INVALID;
        if (mb->state == META_OUT)
            return META_ERR_STATE;
        if (inc < mb->incarnation)
            return META_ERR_STALE; /* addresses of an earlier run */
        if (strcmp(mb->peer, peer) != 0 && addr_taken(m, peer))
            return META_ERR_EXISTS;
        snprintf(mb->addr, sizeof mb->addr, "%s", addr);
        snprintf(mb->peer, sizeof mb->peer, "%s", peer);
        return META_OK;
    }
    }
    return META_ERR_FORMAT; /* an operation this build does not know (head comment of meta.h) */
}

/* Every operation's body must decode before anything is applied: an entry that is damaged anywhere changes
 * nothing, on every server alike. */
static int entry_valid(const uint8_t *p, size_t len, uint32_t *nops)
{
    struct rd r = {p, len, 0, 0};
    if (get32(&r) != ENTRY_MAGIC)
        return 0;
    *nops = get32(&r);
    if (r.bad || *nops > META_OPS_MAX)
        return 0;
    for (uint32_t i = 0; i < *nops; i++) {
        uint8_t kind = get8(&r);
        uint32_t n = get32(&r);
        if (r.bad || kind < META_OP_INIT || kind > META_OP_LAST || n > len - r.off)
            return 0;
        r.off += n;
    }
    return r.off == len;
}

int meta_apply(struct meta_state *m, uint64_t index, uint64_t term, const void *data, size_t len, int *res,
               uint32_t nres)
{
    uint32_t nops;
    if (!entry_valid(data, len, &nops))
        return META_ERR_FORMAT;
    /* bodies are checked as they apply; a body that fails to decode midway would leave earlier ops applied, so
     * decode every body on a scratch copy first (cheap: entries are small) */
    struct meta_state scratch;
    meta_copy(&scratch, m);
    struct rd r = {data, len, 8, 0};
    uint32_t touched[META_OPS_MAX], ntouched = 0;
    int rcs[META_OPS_MAX];
    for (uint32_t i = 0; i < nops; i++) {
        uint8_t kind = get8(&r);
        uint32_t n = get32(&r);
        struct rd body = {r.p + r.off, n, 0, 0};
        r.off += n;
        rcs[i] = apply_op(&scratch, kind, &body, touched, &ntouched);
        if (rcs[i] == META_ERR_FORMAT || (rcs[i] == META_OK && body.off != body.len)) {
            meta_free(&scratch);
            return META_ERR_FORMAT;
        }
    }
    meta_free(m);
    *m = scratch;
    m->applied = index;
    m->applied_term = term;
    for (uint32_t i = 0; i < nres && i < nops; i++)
        res[i] = rcs[i];
    return META_OK;
}

void meta_apply_other(struct meta_state *m, uint64_t index, uint64_t term, const void *config, size_t config_len)
{
    if (config && config_len && config_len <= META_CONFIG_MAX) {
        memcpy(m->config, config, config_len);
        m->config_len = (uint32_t)config_len;
    }
    m->applied = index;
    m->applied_term = term;
}

/* ---------------------------------------------------------------- snapshots */

static void encode_body(struct wr *w, const struct meta_state *m)
{
    put32(w, m->config_len);
    put(w, m->config, m->config_len);
    put8(w, (uint8_t)m->initialised);
    put(w, m->uuid, 16);
    put_settings(w, &m->set);
    put32(w, m->next_member);
    put32(w, m->nmembers);
    for (uint32_t i = 0; i < m->nmembers; i++)
        put_member(w, &m->members[i]);
    put32(w, m->next_volume);
    put32(w, m->nvolumes);
    for (uint32_t i = 0; i < m->nvolumes; i++) {
        const struct meta_volume *v = &m->volumes[i];
        put32(w, v->number);
        putstr(w, v->name, META_NAME_MAX);
        put(w, v->uuid, 16);
        put32(w, v->min_isr);
        put32(w, v->nrep);
        for (uint32_t j = 0; j < v->nrep; j++) {
            put32(w, v->rep[j].node);
            put8(w, v->rep[j].learner);
        }
        put32(w, v->leader);
        put64(w, v->epoch);
        put_ids(w, v->isr, v->nisr);
        put64(w, v->isr_version);
        put32(w, v->nhist);
        for (uint32_t j = 0; j < v->nhist; j++) {
            put64(w, v->hist[j].epoch);
            put64(w, v->hist[j].start);
        }
    }
    /* slots as runs of one home: a handful of runs, not 65,536 entries */
    uint32_t nruns = 0;
    for (uint32_t s = 0; s < META_SLOTS; s++)
        if (s == 0 || m->slot_home[s] != m->slot_home[s - 1])
            nruns++;
    put32(w, nruns);
    for (uint32_t s = 0; s < META_SLOTS;) {
        uint32_t e = s + 1;
        while (e < META_SLOTS && m->slot_home[e] == m->slot_home[s])
            e++;
        put32(w, e - s);
        put32(w, m->slot_home[s]);
        s = e;
    }
    put32(w, m->nmoves);
    for (uint32_t i = 0; i < m->nmoves; i++) {
        put32(w, m->moves[i].slot);
        put32(w, m->moves[i].home);
    }
    put32(w, m->nusers);
    for (uint32_t i = 0; i < m->nusers; i++) {
        putstr(w, m->users[i].name, META_NAME_MAX);
        put32(w, m->users[i].len);
        put(w, m->users[i].blob, m->users[i].len);
    }
}

int meta_snapshot_encode(const struct meta_state *m, uint8_t **out, size_t *len)
{
    uint8_t *buf = NULL;
    size_t n = 0, cap = 0;
    struct wr w = {&buf, &n, &cap};
    put32(&w, SNAP_MAGIC);
    put32(&w, 1);
    put64(&w, m->applied);
    put64(&w, m->applied_term);
    encode_body(&w, m);
    put32(&w, iso_crc32c(0, buf, n));
    *out = buf;
    *len = n;
    return META_OK;
}

static int decode_body(struct rd *r, struct meta_state *m)
{
    m->config_len = get32(r);
    if (m->config_len > META_CONFIG_MAX)
        return -1;
    get(r, m->config, m->config_len);
    m->initialised = get8(r);
    get(r, m->uuid, 16);
    get_settings(r, &m->set);
    m->next_member = get32(r);
    uint32_t n = get32(r);
    if (r->bad || n > (r->len - r->off) / 8)
        return -1;
    m->members = grow(m->members, &m->members_cap, n + 1, sizeof *m->members);
    for (uint32_t i = 0; i < n && !r->bad; i++) {
        get_member(r, &m->members[i]);
        if (i && m->members[i].id <= m->members[i - 1].id)
            r->bad = 1;
        m->nmembers = i + 1;
    }
    m->next_volume = get32(r);
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 16)
        return -1;
    m->volumes = grow(m->volumes, &m->volumes_cap, n + 1, sizeof *m->volumes);
    for (uint32_t i = 0; i < n && !r->bad; i++) {
        struct meta_volume *v = &m->volumes[i];
        memset(v, 0, sizeof *v);
        m->nvolumes = i + 1;
        v->number = get32(r);
        getstr(r, v->name, META_NAME_MAX);
        get(r, v->uuid, 16);
        v->min_isr = get32(r);
        v->nrep = get32(r);
        if (v->nrep > META_REPLICAS_MAX)
            return -1;
        for (uint32_t j = 0; j < v->nrep; j++) {
            v->rep[j].node = get32(r);
            v->rep[j].learner = get8(r);
        }
        v->leader = get32(r);
        v->epoch = get64(r);
        v->nisr = get_ids(r, v->isr);
        v->isr_version = get64(r);
        uint32_t nh = get32(r);
        if (r->bad || nh > (r->len - r->off) / 16)
            return -1;
        v->hist = malloc((nh + 1) * sizeof *v->hist);
        if (!v->hist)
            abort();
        v->nhist = nh;
        for (uint32_t j = 0; j < nh; j++) {
            v->hist[j].epoch = get64(r);
            v->hist[j].start = get64(r);
        }
        if (i && v->number <= m->volumes[i - 1].number)
            r->bad = 1;
    }
    uint32_t nruns = get32(r), s = 0;
    for (uint32_t i = 0; i < nruns && !r->bad; i++) {
        uint32_t cnt = get32(r), home = get32(r);
        if (cnt > META_SLOTS - s)
            return -1;
        for (uint32_t j = 0; j < cnt; j++)
            m->slot_home[s++] = home;
    }
    if (s != META_SLOTS)
        return -1;
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 8)
        return -1;
    m->moves = grow(m->moves, &m->moves_cap, n + 1, sizeof *m->moves);
    for (uint32_t i = 0; i < n; i++) {
        m->moves[i].slot = get32(r);
        m->moves[i].home = get32(r);
    }
    m->nmoves = n;
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 6)
        return -1;
    m->users = grow(m->users, &m->users_cap, n + 1, sizeof *m->users);
    for (uint32_t i = 0; i < n && !r->bad; i++) {
        struct meta_user *u = &m->users[i];
        getstr(r, u->name, META_NAME_MAX);
        u->len = get32(r);
        if (u->len > META_USER_BLOB_MAX)
            return -1;
        u->blob = malloc(u->len + 1);
        if (!u->blob)
            abort();
        m->nusers = i + 1;
        get(r, u->blob, u->len);
        if (i && strcmp(u->name, m->users[i - 1].name) <= 0)
            r->bad = 1;
    }
    return r->bad || r->off != r->len ? -1 : 0;
}

int meta_snapshot_decode(struct meta_state *m, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    uint32_t crc;
    if (len < 28)
        return META_ERR_FORMAT;
    memcpy(&crc, p + len - 4, 4);
    if (iso_crc32c(0, p, len - 4) != crc)
        return META_ERR_FORMAT;
    struct rd r = {p, len - 4, 0, 0};
    if (get32(&r) != SNAP_MAGIC || get32(&r) != 1)
        return META_ERR_FORMAT;
    struct meta_state t;
    meta_init(&t);
    t.applied = get64(&r);
    t.applied_term = get64(&r);
    if (decode_body(&r, &t) != 0) {
        meta_free(&t);
        return META_ERR_FORMAT;
    }
    meta_free(m);
    *m = t;
    return META_OK;
}

uint64_t meta_hash(const struct meta_state *m)
{
    uint8_t *buf = NULL;
    size_t n = 0, cap = 0;
    struct wr w = {&buf, &n, &cap};
    encode_body(&w, m);
    uint64_t h = 0xcbf29ce484222325ull; /* FNV-1a */
    for (size_t i = 0; i < n; i++)
        h = (h ^ buf[i]) * 0x100000001b3ull;
    free(buf);
    return h;
}

/* ---------------------------------------------------------------- the checker */

int meta_check_step(const struct meta_state *prev, const struct meta_state *next, char *why, size_t n)
{
    for (uint32_t i = 0; i < prev->nvolumes; i++) {
        const struct meta_volume *a = &prev->volumes[i], *b = meta_volume(next, a->number);
        if (!b) {
            snprintf(why, n, "volume %u disappeared", a->number);
            return -1;
        }
        if (b->epoch < a->epoch) {
            snprintf(why, n, "volume %u: epoch went back from %llu to %llu", a->number,
                     (unsigned long long)a->epoch, (unsigned long long)b->epoch);
            return -1;
        }
        if (b->epoch > a->epoch + 1) {
            snprintf(why, n, "volume %u: epoch jumped from %llu to %llu in one entry", a->number,
                     (unsigned long long)a->epoch, (unsigned long long)b->epoch);
            return -1;
        }
        if (b->epoch == a->epoch + 1 && !meta_volume_in_isr(a, b->leader)) {
            snprintf(why, n, "volume %u: leader %u of epoch %llu was not in the ISR of epoch %llu", a->number,
                     b->leader, (unsigned long long)b->epoch, (unsigned long long)a->epoch);
            return -1;
        }
        if (b->epoch == a->epoch + 1)
            for (uint32_t j = 0; j < b->nisr; j++)
                if (!meta_volume_in_isr(a, b->isr[j])) {
                    snprintf(why, n, "volume %u: epoch %llu's ISR holds %u, which epoch %llu's did not", a->number,
                             (unsigned long long)b->epoch, b->isr[j], (unsigned long long)a->epoch);
                    return -1;
                }
        if (b->epoch == a->epoch && b->leader != a->leader) {
            snprintf(why, n, "volume %u: leader changed from %u to %u within epoch %llu", a->number, a->leader,
                     b->leader, (unsigned long long)a->epoch);
            return -1;
        }
        int isr_same = a->nisr == b->nisr && !memcmp(a->isr, b->isr, a->nisr * sizeof a->isr[0]);
        if (!isr_same && b->isr_version <= a->isr_version) {
            snprintf(why, n, "volume %u: the ISR changed without a new ISR version", a->number);
            return -1;
        }
        if (b->isr_version < a->isr_version) {
            snprintf(why, n, "volume %u: the ISR version went back", a->number);
            return -1;
        }
    }
    for (uint32_t i = 0; i < next->nvolumes; i++) {
        const struct meta_volume *b = &next->volumes[i];
        if (!meta_volume_in_isr(b, b->leader)) {
            snprintf(why, n, "volume %u: leader %u is not in its ISR", b->number, b->leader);
            return -1;
        }
        for (uint32_t j = 0; j < b->nisr; j++)
            if (replica_at(b, b->isr[j]) < 0) {
                snprintf(why, n, "volume %u: ISR member %u is not a replica", b->number, b->isr[j]);
                return -1;
            }
        if (b->nhist < 1 || b->hist[b->nhist - 1].epoch != b->epoch) {
            snprintf(why, n, "volume %u: the epoch history does not end at epoch %llu", b->number,
                     (unsigned long long)b->epoch);
            return -1;
        }
    }
    for (uint32_t i = 0; i < prev->nmembers; i++)
        if (!meta_member(next, prev->members[i].id)) {
            snprintf(why, n, "member %u disappeared", prev->members[i].id);
            return -1;
        }
    if (next->next_member < prev->next_member || next->next_volume < prev->next_volume) {
        snprintf(why, n, "an id counter went back");
        return -1;
    }
    return 0;
}
