/*
 * The cluster's metadata state machine; see meta.h for what it holds and the rules it keeps.
 *
 * Bytes, all little-endian:
 *   entry     u32 magic "TME1", u32 nops, then nops times { u8 kind, u32 len, body[len] }
 *   snapshot  u32 magic "TMS1", u32 version 2, u64 applied, u64 applied_term, the body below, u32 crc32c of all before
 * (version 2: shared volumes, replica instances, domains, slot moves in flight; version 1 is refused: no cluster
 * ran on it, and a cluster built before is re-initialised.) The snapshot is also the persisted copy a member keeps in
 * its --state-dir: one format. Strings are u16 length + bytes (no terminator), bounded by their field. A body is read by a reader that fails on
 * any overrun, so a damaged entry is META_ERR_FORMAT, never a read past the end.
 *
 * One rule beyond meta.h's: an entry touches a volume at most once (a second operation on it is META_ERR_INVALID).
 * Then a volume's change in one entry is one step, which meta_check_step can judge from the states on either side.
 */
#include "meta.h"

#include "repl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SNAP_VERSION 2

/* tests/test_meta.c --mutant N plants bug N (MUT(n)) in a build with -DMETA_MUTANTS; the simulator and the unit tests
 * must catch every one (a checker that catches nothing proves nothing). Without the define, MUT is 0. */
#ifdef META_MUTANTS
uint64_t meta_mutants;
#define MUT(n) ((meta_mutants >> (n)) & 1)
#else
#define MUT(n) 0
#endif

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
    case META_ERR_RANGE: return "the volume's epoch space is used up";
    case META_ERR_ATTEST: return "the failover mode needs an attestation";
    case META_ERR_BLOCKED: return "the node is barred from claims by a failed claim";
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
    s->failover = META_FAILOVER_LEASE;
    s->read_barrier = 1;
    s->lease_s = 10;
}

uint32_t meta_voter_target(const struct meta_settings *s, uint32_t servers)
{
    uint32_t t = servers >= 5 ? 5 : servers >= 3 ? 3 : 1;
    uint32_t max = s->voters ? s->voters : 5;
    if (max % 2 == 0)
        max--; /* an even group tolerates no more losses than the odd one below it */
    return t < max ? t : max;
}

/* memcpy with a null pointer is undefined even for 0 bytes, and an empty state's arrays are null (UBSan) */
static void copy_n(void *dst, const void *src, size_t n)
{
    if (n)
        memcpy(dst, src, n);
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
    free(m->domains);
    free(m->since);
    free(m->pend);
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
    dst->domains = malloc((src->ndomains + 1) * sizeof *dst->domains);
    dst->since = malloc((src->nsince + 1) * sizeof *dst->since);
    dst->pend = malloc((src->npend + 1) * sizeof *dst->pend);
    dst->slot_home = malloc(META_SLOTS * sizeof *dst->slot_home);
    if (!dst->members || !dst->volumes || !dst->moves || !dst->users || !dst->slot_home || !dst->domains ||
        !dst->since || !dst->pend)
        abort();
    dst->domains_cap = src->ndomains + 1;
    dst->since_cap = src->nsince + 1;
    dst->pend_cap = src->npend + 1;
    copy_n(dst->domains, src->domains, src->ndomains * sizeof *dst->domains);
    copy_n(dst->since, src->since, src->nsince * sizeof *dst->since);
    copy_n(dst->pend, src->pend, src->npend * sizeof *dst->pend);
    dst->members_cap = src->nmembers + 1;
    dst->volumes_cap = src->nvolumes + 1;
    dst->moves_cap = src->nmoves + 1;
    dst->users_cap = src->nusers + 1;
    copy_n(dst->members, src->members, src->nmembers * sizeof *dst->members);
    copy_n(dst->volumes, src->volumes, src->nvolumes * sizeof *dst->volumes);
    copy_n(dst->moves, src->moves, src->nmoves * sizeof *dst->moves);
    copy_n(dst->users, src->users, src->nusers * sizeof *dst->users);
    memcpy(dst->slot_home, src->slot_home, META_SLOTS * sizeof *dst->slot_home);
    for (uint32_t i = 0; i < src->nvolumes; i++) {
        struct meta_volume *v = &dst->volumes[i];
        v->hist = malloc((v->nhist + 1) * sizeof *v->hist);
        if (!v->hist)
            abort();
        copy_n(v->hist, src->volumes[i].hist, v->nhist * sizeof *v->hist);
    }
    for (uint32_t i = 0; i < src->nusers; i++) {
        struct meta_user *u = &dst->users[i];
        u->blob = malloc(u->len + 1);
        if (!u->blob)
            abort();
        copy_n(u->blob, src->users[i].blob, u->len);
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

static int32_t domain_at(const struct meta_state *m, const char *name)
{
    uint32_t lo = 0, hi = m->ndomains;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (strcmp(m->domains[mid].name, name) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < m->ndomains && !strcmp(m->domains[lo].name, name) ? (int32_t)lo : -1 - (int32_t)lo;
}

static int32_t pend_at(const struct meta_state *m, uint32_t slot)
{
    uint32_t lo = 0, hi = m->npend;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (m->pend[mid].slot < slot)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < m->npend && m->pend[lo].slot == slot ? (int32_t)lo : -1 - (int32_t)lo;
}

const struct meta_domain *meta_domain(const struct meta_state *m, const char *name)
{
    int32_t i = domain_at(m, name);
    return i >= 0 ? &m->domains[i] : NULL;
}

const struct meta_slot_pending *meta_slot_pending(const struct meta_state *m, uint32_t slot)
{
    int32_t i = pend_at(m, slot);
    return i >= 0 ? &m->pend[i] : NULL;
}

/* since runs: ascending, disjoint; a slot in none has no home */
uint64_t meta_slot_since(const struct meta_state *m, uint32_t s)
{
    uint32_t lo = 0, hi = m->nsince;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (m->since[mid].first + m->since[mid].count <= s)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < m->nsince && m->since[lo].first <= s ? m->since[lo].index : 0;
}

/* Make [first, first + count) one run of `index`, cutting the runs it overlaps. */
static void since_set(struct meta_state *m, uint32_t first, uint32_t count, uint64_t index)
{
    uint32_t end = first + count;
    struct meta_since_run *out = malloc((m->nsince + 3) * sizeof *out);
    if (!out)
        abort();
    uint32_t n = 0;
    int placed = 0;
    for (uint32_t i = 0; i < m->nsince; i++) {
        struct meta_since_run r = m->since[i];
        uint32_t rend = r.first + r.count;
        if (rend <= first) {
            out[n++] = r;
            continue;
        }
        if (r.first >= end) {
            if (!placed) {
                out[n++] = (struct meta_since_run){first, count, index};
                placed = 1;
            }
            out[n++] = r;
            continue;
        }
        if (r.first < first)
            out[n++] = (struct meta_since_run){r.first, first - r.first, r.index};
        if (!placed) {
            out[n++] = (struct meta_since_run){first, count, index};
            placed = 1;
        }
        if (rend > end)
            out[n++] = (struct meta_since_run){end, rend - end, r.index};
    }
    if (!placed)
        out[n++] = (struct meta_since_run){first, count, index};
    m->since = grow(m->since, &m->since_cap, n + 1, sizeof *m->since);
    copy_n(m->since, out, n * sizeof *out);
    m->nsince = n;
    free(out);
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

const struct meta_replica *meta_replica(const struct meta_volume *v, uint32_t node)
{
    int i = replica_at(v, node);
    return i >= 0 ? &v->rep[i] : NULL;
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
    put32(w, 12);
    put32(w, s->replication);
    put32(w, s->min_isr);
    put32(w, s->node_timeout_ms);
    put32(w, s->heartbeat_ms);
    put32(w, s->isr_lag_ms);
    put32(w, s->out_after_s);
    put32(w, s->voter_replace_s);
    put32(w, s->voters);
    put32(w, s->volumes_per_server);
    put8(w, s->failover);
    put8(w, s->read_barrier);
    put32(w, s->lease_s);
}

static void get_settings(struct rd *r, struct meta_settings *s)
{
    uint32_t n = get32(r);
    if (n != 12) { /* a later build adds fields at the end and raises the count; this one knows 12 */
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
    s->failover = get8(r);
    s->read_barrier = get8(r);
    s->lease_s = get32(r);
}

static int settings_valid(const struct meta_settings *s)
{
    return s->replication >= 1 && s->replication <= META_REPLICAS_MAX && s->min_isr >= 1 &&
           s->min_isr <= s->replication && s->failover <= META_FAILOVER_BMC && s->read_barrier <= 1 &&
           s->lease_s >= 1;
}

static void put_member(struct wr *w, const struct meta_member *mb)
{
    put32(w, mb->id);
    put8(w, mb->state);
    put64(w, mb->incarnation);
    putstr(w, mb->addr, META_ADDR_MAX);
    putstr(w, mb->peer, META_ADDR_MAX);
    putstr(w, mb->rack, META_RACK_MAX);
    putstr(w, mb->power_feed, META_FEED_MAX);
    putstr(w, mb->bmc, META_BMC_MAX);
    put8(w, mb->flags);
    put(w, mb->ident, 16);
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
    getstr(r, mb->power_feed, META_FEED_MAX);
    getstr(r, mb->bmc, META_BMC_MAX);
    mb->flags = get8(r);
    get(r, mb->ident, 16);
    if (mb->state > META_OUT || (mb->flags & ~META_MF_ALL))
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

static void put_pos(struct wr *w, const struct meta_positions *p)
{
    put64(w, p->wal_seg);
    put64(w, p->wal_off);
    put64(w, p->ledger_gen);
    put64(w, p->ledger_off);
    put64(w, p->series_len);
    put64(w, p->states_len);
    put64(w, p->classes_len);
    put64(w, p->retention_len);
}

static void get_pos(struct rd *r, struct meta_positions *p)
{
    p->wal_seg = get64(r);
    p->wal_off = get64(r);
    p->ledger_gen = get64(r);
    p->ledger_off = get64(r);
    p->series_len = get64(r);
    p->states_len = get64(r);
    p->classes_len = get64(r);
    p->retention_len = get64(r);
}

static void put_storage(struct wr *w, const struct meta_storage *s)
{
    putstr(w, s->path, META_PATH_MAX);
    putstr(w, s->export, META_PATH_MAX);
    putstr(w, s->fstype, META_FSTYPE_MAX);
    putstr(w, s->group, META_GROUP_MAX);
}

static void get_storage(struct rd *r, struct meta_storage *s)
{
    memset(s, 0, sizeof *s);
    getstr(r, s->path, META_PATH_MAX);
    getstr(r, s->export, META_PATH_MAX);
    getstr(r, s->fstype, META_FSTYPE_MAX);
    getstr(r, s->group, META_GROUP_MAX);
}

static int storage_empty(const struct meta_storage *s)
{
    return !s->path[0] && !s->export[0] && !s->fstype[0] && !s->group[0];
}

/* a printable path or export: no control bytes */
static int text_ok(const char *s)
{
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || *s == 0x7f)
            return 0;
    return 1;
}

void meta_op_init(struct meta_entry *e, const uint8_t uuid[16], const struct meta_settings *s,
                  const struct meta_member *first, int mode, uint32_t level)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_INIT);
    put(&w, uuid, 16);
    put_settings(&w, s);
    put_member(&w, first);
    put8(&w, (uint8_t)mode);
    put32(&w, level);
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

void meta_op_member_flags(struct meta_entry *e, uint32_t id, uint64_t incarnation, uint8_t flags)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_MEMBER_FLAGS);
    put32(&w, id);
    put64(&w, incarnation);
    put8(&w, flags);
    op_end(e, st);
}

void meta_op_settings(struct meta_entry *e, const struct meta_settings *s)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SETTINGS);
    put_settings(&w, s);
    op_end(e, st);
}

void meta_op_volume_add(struct meta_entry *e, const struct meta_volume_add *a)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_ADD);
    put32(&w, a->number);
    putstr(&w, a->name, META_NAME_MAX);
    put(&w, a->uuid, 16);
    put8(&w, a->kind);
    put_storage(&w, &a->st);
    put32(&w, a->min_isr);
    put_ids(&w, a->nodes, a->n);
    for (uint32_t i = 0; i < a->n; i++)
        put64(&w, a->rid[i]);
    op_end(e, st);
}

void meta_op_volume_leader(struct meta_entry *e, const struct meta_grant *g)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_LEADER);
    put32(&w, g->number);
    put64(&w, g->epoch);
    put64(&w, g->new_epoch);
    put64(&w, g->isr_version);
    put8(&w, g->reason);
    put32(&w, g->leader);
    put64(&w, g->start);
    put_pos(&w, &g->pos);
    put8(&w, g->att.kind);
    putstr(&w, g->att.who, META_NAME_MAX);
    put64(&w, g->att.ms);
    put64(&w, g->att.incarnation);
    put_ids(&w, g->fenced, g->nfenced);
    put32(&w, g->nreplies);
    for (uint32_t i = 0; i < g->nreplies; i++) {
        put32(&w, g->reply[i].node);
        put64(&w, g->reply[i].rid);
        put64(&w, g->reply[i].durable);
        put64(&w, g->reply[i].last_epoch);
    }
    op_end(e, st);
}

void meta_op_isr_change(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t isr_version,
                        const uint32_t *isr, const uint64_t *rid, uint32_t nisr, uint64_t commit)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_ISR_CHANGE);
    put32(&w, number);
    put64(&w, epoch);
    put64(&w, isr_version);
    put_ids(&w, isr, nisr);
    for (uint32_t i = 0; i < nisr; i++)
        put64(&w, rid ? rid[i] : 0);
    put64(&w, commit);
    op_end(e, st);
}

void meta_op_replica_add(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node, uint64_t rid)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_ADD);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, node);
    put64(&w, rid);
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

void meta_op_volume_self(struct meta_entry *e, uint32_t number, uint32_t node, uint64_t new_epoch)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_SELF);
    put32(&w, number);
    put32(&w, node);
    put64(&w, new_epoch);
    op_end(e, st);
}

void meta_op_volume_reserve(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t series_reserved,
                            uint32_t states_reserved, const struct meta_positions *pos)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_RESERVE);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, series_reserved);
    put32(&w, states_reserved);
    put_pos(&w, pos);
    op_end(e, st);
}

void meta_op_volume_drop(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t wm_gen,
                         int64_t dropped_before)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_DROP);
    put32(&w, number);
    put64(&w, epoch);
    put64(&w, wm_gen);
    put64(&w, (uint64_t)dropped_before);
    op_end(e, st);
}

void meta_op_claim_refused(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t highest_seen)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_CLAIM_REFUSED);
    put32(&w, number);
    put64(&w, epoch);
    put64(&w, highest_seen);
    op_end(e, st);
}

void meta_op_claim_failed(struct meta_entry *e, uint32_t number, uint64_t epoch, uint8_t reason)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_CLAIM_FAILED);
    put32(&w, number);
    put64(&w, epoch);
    put8(&w, reason);
    op_end(e, st);
}

void meta_op_volume_relocate(struct meta_entry *e, uint32_t number, uint64_t epoch, const char *export_path)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_RELOCATE);
    put32(&w, number);
    put64(&w, epoch);
    putstr(&w, export_path, META_PATH_MAX);
    op_end(e, st);
}

void meta_op_domain_state(struct meta_entry *e, const char *name, int state)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_DOMAIN_STATE);
    putstr(&w, name, META_DOMAIN_MAX);
    put8(&w, (uint8_t)state);
    op_end(e, st);
}

void meta_op_slot_move_pending(struct meta_entry *e, uint32_t slot, uint32_t from, uint32_t to)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SLOT_MOVE_PENDING);
    put32(&w, slot);
    put32(&w, from);
    put32(&w, to);
    op_end(e, st);
}

void meta_op_slot_freeze(struct meta_entry *e, uint32_t slot, uint32_t from, uint64_t from_epoch)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SLOT_FREEZE);
    put32(&w, slot);
    put32(&w, from);
    put64(&w, from_epoch);
    op_end(e, st);
}

void meta_op_slot_move(struct meta_entry *e, uint32_t slot, uint32_t to)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SLOT_MOVE);
    put32(&w, slot);
    put32(&w, to);
    op_end(e, st);
}

void meta_op_slot_move_cancel(struct meta_entry *e, uint32_t slot)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_SLOT_MOVE_CANCEL);
    put32(&w, slot);
    op_end(e, st);
}

void meta_op_replica_open(struct meta_entry *e, uint32_t number, uint32_t node, uint64_t rid, uint64_t gen)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_OPEN);
    put32(&w, number);
    put32(&w, node);
    put64(&w, rid);
    put64(&w, gen);
    op_end(e, st);
}

void meta_op_restore(struct meta_entry *e, uint64_t gen)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_RESTORE);
    put64(&w, gen);
    op_end(e, st);
}

void meta_op_name_group(struct meta_entry *e)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_NAME_GROUP);
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

static int ident_taken(const struct meta_state *m, const uint8_t ident[16])
{
    static const uint8_t zero[16];
    if (!memcmp(ident, zero, 16))
        return 0;
    for (uint32_t i = 0; i < m->nmembers; i++)
        if (m->members[i].state != META_OUT && !memcmp(m->members[i].ident, ident, 16))
            return 1;
    return 0;
}

static int insert_member(struct meta_state *m, const struct meta_member *mb)
{
    if (mb->id == 0 || mb->id == META_NONE || !mb->peer[0])
        return META_ERR_INVALID;
    int32_t at = member_at(m, mb->id);
    if (at >= 0 || mb->id < m->next_member || addr_taken(m, mb->peer) || ident_taken(m, mb->ident))
        return META_ERR_EXISTS; /* ids are never reused, nor a live server or peer address under two ids */
    uint32_t pos = (uint32_t)(-1 - at);
    m->members = grow(m->members, &m->members_cap, m->nmembers + 1, sizeof *m->members);
    memmove(&m->members[pos + 1], &m->members[pos], (m->nmembers - pos) * sizeof *m->members);
    m->members[pos] = *mb;
    m->members[pos].state = META_UP;
    m->nmembers++;
    m->next_member = mb->id + 1;
    return META_OK;
}

/* What an entry has touched: volumes by number, name slots by SLOT_TAG | slot. One operation per volume and one per
 * slot in an entry, so each step the checker sees is one operation's. */
#define SLOT_TAG 0x80000000u

static int touched_has(const uint32_t *t, uint32_t n, uint32_t key)
{
    for (uint32_t i = 0; i < n; i++)
        if (t[i] == key)
            return 1;
    return 0;
}

static int vol_get(struct meta_state *m, uint32_t number, const uint32_t *t, uint32_t nt, struct meta_volume **out)
{
    int32_t at = volume_at(m, number);
    if (at < 0)
        return META_ERR_NOTFOUND;
    if (touched_has(t, nt, number))
        return META_ERR_INVALID; /* once per entry (head comment) */
    *out = &m->volumes[at];
    return META_OK;
}

static void isr_set(struct meta_volume *v, const uint32_t *ids, uint32_t n)
{
    v->nisr = n;
    memcpy(v->isr, ids, n * sizeof *ids);
    sort_ids(v->isr, n);
}

/* The grant of a replicated volume: the promotion of design-cluster.md §6.2 over the fence replies, by the function
 * the replication simulator checks. Replica indexes (rep[]) are its node numbers. */
static int promote(struct meta_volume *v, const struct meta_grant *g)
{
    struct repl_vmeta vm;
    memset(&vm, 0, sizeof vm);
    vm.epoch = v->epoch;
    vm.isr_version = v->isr_version;
    vm.leader = replica_at(v, v->leader);
    for (uint32_t i = 0; i < v->nrep; i++) {
        if (meta_volume_in_isr(v, v->rep[i].node)) {
            vm.isr |= 1u << i;
            vm.rid[i] = v->rep[i].rid;
        }
        if (v->rep[i].unanswered)
            vm.unanswered |= 1u << i;
    }
    uint32_t fenced = 0;
    for (uint32_t i = 0; i < g->nfenced; i++) {
        int x = replica_at(v, g->fenced[i]);
        if (x < 0)
            return META_ERR_INVALID;
        fenced |= 1u << x;
    }
    struct repl_fence_reply rr[META_REPLICAS_MAX];
    for (uint32_t i = 0; i < g->nreplies; i++) {
        int x = replica_at(v, g->reply[i].node);
        if (x < 0)
            return META_ERR_INVALID;
        rr[i] = (struct repl_fence_reply){x, g->reply[i].durable, g->reply[i].last_epoch, g->reply[i].rid};
    }
    int rc = repl_meta_promote(&vm, g->epoch, g->isr_version, fenced, rr, (int)g->nreplies);
    if (rc == REPL_E_STALE)
        return META_ERR_STALE;
    if (rc != REPL_OK || vm.leader < 0 || v->rep[vm.leader].node != g->leader)
        return META_ERR_INVALID; /* the proposer's pick is not what the rule gives: a bug or a stale view */
    uint32_t ids[META_REPLICAS_MAX], n = 0;
    for (uint32_t i = 0; i < v->nrep; i++) {
        v->rep[i].unanswered = (vm.unanswered >> i) & 1;
        if ((vm.isr >> i) & 1) {
            ids[n++] = v->rep[i].node;
            v->rep[i].learner = 0;
        }
    }
    isr_set(v, ids, n);
    v->hist = realloc(v->hist, (v->nhist + 1) * sizeof *v->hist);
    if (!v->hist)
        abort();
    v->epoch = MUT(1) ? g->new_epoch : vm.epoch;
    v->hist[v->nhist++] = (struct meta_epoch){v->epoch, g->start};
    v->leader = g->leader;
    v->isr_version = vm.isr_version;
    return META_OK;
}

/* A shared volume's grant to g->leader: the rules of the head comment. */
static int grant_shared(struct meta_state *m, struct meta_volume *v, const struct meta_grant *g)
{
    int w = replica_at(v, g->leader);
    const struct meta_member *mb = meta_member(m, g->leader);
    if (w < 0 || !mb || mb->state == META_OUT)
        return META_ERR_INVALID;
    int same = g->leader == v->leader;
    if (g->new_epoch <= v->epoch || (!same && g->new_epoch < v->epoch + (MUT(2) ? 1 : 2)))
        return META_ERR_INVALID; /* e + 1 is the owner's */
    if (g->new_epoch >= META_EPOCH_LIMIT - 1 && !MUT(9))
        return META_ERR_RANGE; /* the reservation after it must stay legal too */
    if (g->new_epoch <= v->highest_seen)
        return META_ERR_STALE;
    if (v->rep[w].claim_blocked)
        for (uint32_t i = 0; i < v->nrep; i++)
            if (!v->rep[i].claim_blocked)
                return META_ERR_BLOCKED;
    if (!same && !MUT(8) && m->set.failover != META_FAILOVER_LEASE &&
        (g->reason == META_GR_UNHEARD || g->reason == META_GR_FENCED)) {
        const struct meta_attest *a = &g->att;
        if (a->kind == META_ATT_NONE || !a->who[0] || !a->ms)
            return META_ERR_ATTEST;
        if (m->set.failover == META_FAILOVER_BMC) {
            const struct meta_member *old = meta_member(m, v->leader);
            if (a->kind != META_ATT_BMC || (old && a->incarnation < old->incarnation))
                return META_ERR_ATTEST;
        }
    }
    v->epoch = g->new_epoch;
    v->leader = g->leader;
    v->pos = g->pos;
    return META_OK;
}

static void slot_pend_remove(struct meta_state *m, int32_t at)
{
    memmove(&m->pend[at], &m->pend[at + 1], (m->npend - (uint32_t)at - 1) * sizeof m->pend[0]);
    m->npend--;
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
        uint8_t mode = get8(r);
        uint32_t level = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (m->initialised)
            return META_ERR_STATE;
        if (!settings_valid(&s) || (mode != META_MODE_REPLICATED && mode != META_MODE_SHARED) ||
            level < 1 || level > META_LEVEL)
            return META_ERR_INVALID;
        int rc = insert_member(m, &mb);
        if (rc)
            return rc;
        memcpy(m->uuid, uuid, 16);
        m->set = s;
        m->mode = mode;
        m->level = level;
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
    case META_OP_MEMBER_FLAGS: {
        uint32_t id = get32(r);
        uint64_t inc = get64(r);
        uint8_t flags = get8(r);
        if (r->bad || (flags & ~META_MF_ALL))
            return META_ERR_FORMAT;
        int32_t at = member_at(m, id);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state == META_OUT)
            return META_ERR_STATE;
        if (inc < mb->incarnation)
            return META_ERR_STALE; /* the flags of an earlier run */
        mb->flags = flags;
        if (flags & META_MF_VOLUMES_OK) /* it says it is well again: its CLAIM_FAILED bars end (F8) */
            for (uint32_t i = 0; i < m->nvolumes; i++)
                for (uint32_t j = 0; j < m->volumes[i].nrep; j++)
                    if (m->volumes[i].rep[j].node == id)
                        m->volumes[i].rep[j].claim_blocked = 0;
        return META_OK;
    }
    case META_OP_SETTINGS: {
        struct meta_settings s;
        get_settings(r, &s);
        if (r->bad)
            return META_ERR_FORMAT;
        if (!settings_valid(&s))
            return META_ERR_INVALID;
        m->set = s;
        return META_OK;
    }
    case META_OP_VOLUME_ADD: {
        uint32_t number = get32(r);
        char name[META_NAME_MAX];
        uint8_t uuid[16];
        uint32_t nodes[META_REPLICAS_MAX];
        uint64_t rid[META_REPLICAS_MAX];
        struct meta_storage st;
        getstr(r, name, sizeof name);
        get(r, uuid, 16);
        uint8_t vk = get8(r);
        get_storage(r, &st);
        uint32_t min_isr = get32(r);
        uint32_t n = get_ids(r, nodes);
        for (uint32_t i = 0; i < n; i++)
            rid[i] = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (number > META_VOLUME_MAX || !name[0] || n < 1 || min_isr < 1 || min_isr > n || vk != m->mode)
            return META_ERR_INVALID; /* a volume of the other kind is refused (M7) */
        if (vk == META_VOL_SHARED) {
            if (st.path[0] != '/' || !st.export[0] || !text_ok(st.path) || !text_ok(st.export) ||
                !text_ok(st.fstype) || !text_ok(st.group))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < n; i++)
                if (rid[i])
                    return META_ERR_INVALID;
        } else {
            if (!storage_empty(&st))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < n; i++) {
                if (!rid[i])
                    return META_ERR_INVALID;
                for (uint32_t j = 0; j < i; j++)
                    if (rid[j] == rid[i])
                        return META_ERR_INVALID;
            }
        }
        if (volume_at(m, number) >= 0 || number < m->next_volume)
            return META_ERR_EXISTS;
        for (uint32_t i = 0; i < m->nvolumes; i++)
            if (!strcmp(m->volumes[i].name, name))
                return META_ERR_EXISTS;
        for (uint32_t i = 0; i < n; i++) {
            const struct meta_member *mb = meta_member(m, nodes[i]);
            if (!mb || mb->state == META_OUT || !(mb->flags & META_MF_VOLUMES_OK) || has_id(nodes, i, nodes[i]))
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
        v->kind = vk;
        v->st = st;
        v->min_isr = min_isr;
        v->nrep = n;
        for (uint32_t i = 0; i < n; i++) {
            v->rep[i].node = nodes[i];
            v->rep[i].rid = rid[i];
        }
        v->leader = nodes[0];
        v->epoch = 1;
        isr_set(v, nodes, n);
        v->isr_version = 1;
        if (vk == META_VOL_REPLICATED) {
            v->hist = malloc(sizeof *v->hist);
            if (!v->hist)
                abort();
            v->hist[0] = (struct meta_epoch){1, 0};
            v->nhist = 1;
        }
        m->nvolumes++;
        m->next_volume = number + 1;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_LEADER: {
        struct meta_grant g;
        memset(&g, 0, sizeof g);
        g.number = get32(r);
        g.epoch = get64(r);
        g.new_epoch = get64(r);
        g.isr_version = get64(r);
        g.reason = get8(r);
        g.leader = get32(r);
        g.start = get64(r);
        get_pos(r, &g.pos);
        g.att.kind = get8(r);
        getstr(r, g.att.who, sizeof g.att.who);
        g.att.ms = get64(r);
        g.att.incarnation = get64(r);
        g.nfenced = get_ids(r, g.fenced);
        g.nreplies = get32(r);
        if (g.nreplies > META_REPLICAS_MAX)
            r->bad = 1;
        for (uint32_t i = 0; i < g.nreplies && !r->bad; i++) {
            g.reply[i].node = get32(r);
            g.reply[i].rid = get64(r);
            g.reply[i].durable = get64(r);
            g.reply[i].last_epoch = get64(r);
        }
        if (r->bad || g.att.kind > META_ATT_BMC)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, g.number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (g.epoch != v->epoch || g.isr_version != v->isr_version)
            return META_ERR_STALE;
        if (g.reason < 1 || g.reason > META_GR_LAST)
            return META_ERR_INVALID;
        if (v->kind == META_VOL_REPLICATED) {
            if ((g.new_epoch != v->epoch + 1 && !(MUT(1) && g.new_epoch > v->epoch)) || v->nhist < 1 ||
                (g.start <= v->hist[v->nhist - 1].start && !MUT(13)))
                return META_ERR_INVALID;
            rc = promote(v, &g);
            if (rc == META_OK)
                v->pos = g.pos;
        } else {
            rc = grant_shared(m, v, &g);
        }
        if (rc == META_OK)
            touched[(*ntouched)++] = g.number;
        return rc;
    }
    case META_OP_ISR_CHANGE:
    case META_OP_REPLICA_ADD:
    case META_OP_REPLICA_REMOVE: {
        uint32_t number = get32(r);
        uint64_t epoch = get64(r), isr_version = 0, rid = 0, commit = 0, rids[META_REPLICAS_MAX];
        uint32_t node = 0, ids[META_REPLICAS_MAX], n = 0;
        if (kind == META_OP_ISR_CHANGE) {
            isr_version = get64(r);
            n = get_ids(r, ids);
            for (uint32_t i = 0; i < n; i++)
                rids[i] = get64(r);
            commit = get64(r);
        } else {
            node = get32(r);
            if (kind == META_OP_REPLICA_ADD)
                rid = get64(r);
        }
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (epoch != v->epoch || (kind == META_OP_ISR_CHANGE && isr_version != v->isr_version))
            return META_ERR_STALE;
        int shared = v->kind == META_VOL_SHARED;
        if (kind == META_OP_ISR_CHANGE) {
            if (shared) /* every candidate is in sync: they all see the same storage */
                return META_ERR_INVALID;
            if (n < 1 || !ids_valid(v, ids, n) || !has_id(ids, n, v->leader))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < n; i++) /* a member whose instance differs is not the one in the record */
                if (rids[i] != v->rep[replica_at(v, ids[i])].rid && !MUT(10))
                    return META_ERR_INVALID;
            isr_set(v, ids, n);
            v->isr_version++;
            if (commit)
                v->isr_commit = commit;
            for (uint32_t i = 0; i < v->nrep; i++)
                v->rep[i].unanswered = 0; /* the new leader has heard or dropped them (item 16) */
            for (uint32_t i = 0; i < n; i++) /* a learner that joined the ISR is a full replica */
                v->rep[replica_at(v, ids[i])].learner = 0;
        } else if (kind == META_OP_REPLICA_ADD) {
            const struct meta_member *mb = meta_member(m, node);
            if (!mb || mb->state == META_OUT || !(mb->flags & META_MF_VOLUMES_OK))
                return META_ERR_INVALID;
            if (replica_at(v, node) >= 0)
                return META_ERR_EXISTS;
            if (v->nrep >= META_REPLICAS_MAX || (shared ? rid != 0 : rid == 0))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < v->nrep; i++)
                if (rid && v->rep[i].rid == rid)
                    return META_ERR_INVALID; /* one instance, one replica */
            v->rep[v->nrep] = (struct meta_replica){.node = node, .learner = shared ? 0 : 1, .rid = rid};
            v->nrep++;
            if (shared) { /* a candidate is in the ISR from the start */
                v->isr[v->nisr++] = node;
                sort_ids(v->isr, v->nisr);
                v->isr_version++;
            }
        } else {
            int i = replica_at(v, node);
            if (i < 0)
                return META_ERR_NOTFOUND;
            if ((node == v->leader && !MUT(11)) || (!shared && meta_volume_in_isr(v, node)))
                return META_ERR_INVALID; /* the ISR shrinks first: a move never lowers the in-sync copies */
            memmove(&v->rep[i], &v->rep[i + 1], (v->nrep - (uint32_t)i - 1) * sizeof v->rep[0]);
            v->nrep--;
            if (shared) {
                uint32_t keep[META_REPLICAS_MAX], k = 0;
                for (uint32_t j = 0; j < v->nisr; j++)
                    if (v->isr[j] != node)
                        keep[k++] = v->isr[j];
                isr_set(v, keep, k);
                v->isr_version++;
            }
        }
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_SELF: {
        uint32_t number = get32(r), node = get32(r);
        uint64_t new_epoch = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->kind != META_VOL_SHARED)
            return META_ERR_INVALID;
        /* conditional on the epoch before it and on the owner (§5.4): no liveness condition, the storage lock is
         * the fence and the proposer judges the rest */
        if ((v->leader != node && !MUT(6)) || new_epoch != v->epoch + 1)
            return META_ERR_STALE;
        if (new_epoch >= META_EPOCH_LIMIT)
            return META_ERR_RANGE;
        v->epoch = new_epoch;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_RESERVE:
    case META_OP_VOLUME_DROP:
    case META_OP_CLAIM_REFUSED:
    case META_OP_CLAIM_FAILED:
    case META_OP_VOLUME_RELOCATE: {
        uint32_t number = get32(r), sr = 0, st = 0;
        uint64_t epoch = get64(r), a = 0, b = 0;
        struct meta_positions pos;
        char path[META_PATH_MAX];
        memset(&pos, 0, sizeof pos);
        path[0] = 0;
        if (kind == META_OP_VOLUME_RESERVE) {
            sr = get32(r);
            st = get32(r);
            get_pos(r, &pos);
        } else if (kind == META_OP_VOLUME_DROP) {
            a = get64(r);
            b = get64(r);
        } else if (kind == META_OP_CLAIM_REFUSED) {
            a = get64(r);
        } else if (kind == META_OP_CLAIM_FAILED) {
            (void)get8(r); /* the reason: the leader's log line, not state */
        } else {
            getstr(r, path, sizeof path);
        }
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->kind != META_VOL_SHARED)
            return META_ERR_INVALID;
        if (epoch != v->epoch && !(MUT(4) && kind == META_OP_VOLUME_RESERVE)) /* exactly: an owner that moved itself to E + 1 proposes again at E + 1 */
            return META_ERR_STALE;
        switch (kind) {
        case META_OP_VOLUME_RESERVE:
            if ((sr < v->series_reserved || st < v->states_reserved) && !MUT(5))
                return META_ERR_STALE; /* absolute values, never lower: a late proposal of an earlier block */
            v->series_reserved = sr;
            v->states_reserved = st;
            v->pos = pos;
            break;
        case META_OP_VOLUME_DROP:
            if (a < v->wm_gen || (int64_t)b < v->dropped_before)
                return META_ERR_STALE;
            v->wm_gen = a;
            v->dropped_before = (int64_t)b;
            break;
        case META_OP_CLAIM_REFUSED:
            if (a > v->highest_seen)
                v->highest_seen = a;
            break;
        case META_OP_CLAIM_FAILED:
            if (!MUT(7))
                v->rep[replica_at(v, v->leader)].claim_blocked = 1; /* the epoch names the claimant: the leader */
            break;
        default:
            if (!path[0] || !text_ok(path))
                return META_ERR_INVALID;
            snprintf(v->st.export, sizeof v->st.export, "%s", path);
            break;
        }
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_REPLICA_OPEN: {
        uint32_t number = get32(r), node = get32(r);
        uint64_t rid = get64(r), gen = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->kind != META_VOL_REPLICATED)
            return META_ERR_INVALID;
        int i = replica_at(v, node);
        if (i < 0)
            return META_ERR_NOTFOUND;
        if (v->rep[i].rid != rid || gen <= v->rep[i].open_gen)
            return META_ERR_STALE; /* another instance of the directory, or a restored copy's older generation */
        v->rep[i].open_gen = gen;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_DOMAIN_STATE: {
        char name[META_DOMAIN_MAX];
        getstr(r, name, sizeof name);
        uint8_t st = get8(r);
        if (r->bad || st > META_DOM_ROLLED_BACK)
            return META_ERR_FORMAT;
        if (!name[0])
            return META_ERR_INVALID;
        int32_t at = domain_at(m, name);
        if (at < 0) {
            uint32_t pos = (uint32_t)(-1 - at);
            m->domains = grow(m->domains, &m->domains_cap, m->ndomains + 1, sizeof *m->domains);
            memmove(&m->domains[pos + 1], &m->domains[pos], (m->ndomains - pos) * sizeof *m->domains);
            m->ndomains++;
            memcpy(m->domains[pos].name, name, sizeof name);
            at = (int32_t)pos;
        }
        m->domains[at].state = st;
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
        /* only slots without a home get one here; a slot that has one moves by the freeze protocol (§5.7) */
        for (uint32_t s = first; s < first + count; s++)
            if (m->slot_home[s] != META_NONE && m->slot_home[s] != vol)
                return META_ERR_INVALID;
        for (uint32_t s = first; s < first + count;) {
            if (m->slot_home[s] != META_NONE) {
                s++;
                continue;
            }
            uint32_t e = s;
            while (e < first + count && m->slot_home[e] == META_NONE)
                m->slot_home[e++] = vol;
            since_set(m, s, e - s, m->applied);
            s = e;
        }
        return META_OK;
    }
    case META_OP_SLOT_MOVE_PENDING: {
        uint32_t slot = get32(r), from = get32(r), to = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (slot >= META_SLOTS || from == to || from == META_NONE || touched_has(touched, *ntouched, SLOT_TAG | slot))
            return META_ERR_INVALID;
        if (volume_at(m, to) < 0)
            return META_ERR_NOTFOUND;
        if (m->slot_home[slot] != from)
            return META_ERR_STALE;
        int32_t at = pend_at(m, slot);
        if (at >= 0)
            return META_ERR_EXISTS;
        uint32_t pos = (uint32_t)(-1 - at);
        m->pend = grow(m->pend, &m->pend_cap, m->npend + 1, sizeof *m->pend);
        memmove(&m->pend[pos + 1], &m->pend[pos], (m->npend - pos) * sizeof *m->pend);
        m->pend[pos] = (struct meta_slot_pending){slot, from, to, 0};
        m->npend++;
        touched[(*ntouched)++] = SLOT_TAG | slot;
        return META_OK;
    }
    case META_OP_SLOT_FREEZE: {
        uint32_t slot = get32(r), from = get32(r);
        uint64_t epoch = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (slot >= META_SLOTS || !epoch || touched_has(touched, *ntouched, SLOT_TAG | slot))
            return META_ERR_INVALID;
        int32_t at = pend_at(m, slot);
        if (at < 0)
            return META_ERR_NOTFOUND;
        if (m->pend[at].from != from)
            return META_ERR_INVALID;
        int32_t vi = volume_at(m, from);
        if (vi < 0)
            return META_ERR_NOTFOUND;
        if (m->volumes[vi].epoch != epoch && !MUT(12))
            return META_ERR_STALE; /* the freeze is conditional on A's epoch: a stale owner's changes nothing */
        if (m->pend[at].freeze_epoch)
            return META_ERR_EXISTS;
        m->pend[at].freeze_epoch = epoch;
        touched[(*ntouched)++] = SLOT_TAG | slot;
        return META_OK;
    }
    case META_OP_SLOT_MOVE: {
        uint32_t slot = get32(r), to = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (slot >= META_SLOTS || touched_has(touched, *ntouched, SLOT_TAG | slot))
            return META_ERR_INVALID;
        int32_t at = pend_at(m, slot);
        if (at < 0)
            return META_ERR_NOTFOUND;
        /* a freeze committed before this entry, under the epoch A had then (a later owner of A holds it in its copy) */
        if (m->pend[at].to != to || (!m->pend[at].freeze_epoch && !MUT(3)))
            return META_ERR_INVALID;
        m->moves = grow(m->moves, &m->moves_cap, m->nmoves + 1, sizeof *m->moves);
        m->moves[m->nmoves++] = (struct meta_slot_move){slot, m->pend[at].from, meta_slot_since(m, slot)};
        m->slot_home[slot] = to;
        since_set(m, slot, 1, m->applied);
        slot_pend_remove(m, at);
        touched[(*ntouched)++] = SLOT_TAG | slot;
        return META_OK;
    }
    case META_OP_SLOT_MOVE_CANCEL: {
        uint32_t slot = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (slot >= META_SLOTS || touched_has(touched, *ntouched, SLOT_TAG | slot))
            return META_ERR_INVALID;
        int32_t at = pend_at(m, slot);
        if (at < 0)
            return META_ERR_NOTFOUND;
        slot_pend_remove(m, at);
        touched[(*ntouched)++] = SLOT_TAG | slot;
        return META_OK;
    }
    case META_OP_RESTORE: {
        uint64_t gen = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (gen <= m->restore_gen)
            return META_ERR_STALE;
        m->restore_gen = gen;
        return META_OK;
    }
    case META_OP_NAME_GROUP:
        r->off = r->len; /* a number kept for S4's prefix groups: nothing applies it yet */
        return META_ERR_INVALID;
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
    scratch.applied = index; /* what the slot operations record as "from index" */
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

static void put_volume(struct wr *w, const struct meta_volume *v)
{
    put32(w, v->number);
    putstr(w, v->name, META_NAME_MAX);
    put(w, v->uuid, 16);
    put8(w, v->kind);
    put_storage(w, &v->st);
    put32(w, v->min_isr);
    put32(w, v->nrep);
    for (uint32_t j = 0; j < v->nrep; j++) {
        put32(w, v->rep[j].node);
        put8(w, v->rep[j].learner);
        put8(w, v->rep[j].unanswered);
        put8(w, v->rep[j].claim_blocked);
        put64(w, v->rep[j].rid);
        put64(w, v->rep[j].open_gen);
    }
    put32(w, v->leader);
    put64(w, v->epoch);
    put_ids(w, v->isr, v->nisr);
    put64(w, v->isr_version);
    put64(w, v->isr_commit);
    put64(w, v->highest_seen);
    put32(w, v->series_reserved);
    put32(w, v->states_reserved);
    put64(w, v->wm_gen);
    put64(w, (uint64_t)v->dropped_before);
    put_pos(w, &v->pos);
    put32(w, v->nhist);
    for (uint32_t j = 0; j < v->nhist; j++) {
        put64(w, v->hist[j].epoch);
        put64(w, v->hist[j].start);
    }
}

static void encode_body(struct wr *w, const struct meta_state *m)
{
    put32(w, m->config_len);
    put(w, m->config, m->config_len);
    put8(w, (uint8_t)m->initialised);
    put(w, m->uuid, 16);
    put8(w, m->mode);
    put32(w, m->level);
    put64(w, m->restore_gen);
    put_settings(w, &m->set);
    put32(w, m->next_member);
    put32(w, m->nmembers);
    for (uint32_t i = 0; i < m->nmembers; i++)
        put_member(w, &m->members[i]);
    put32(w, m->next_volume);
    put32(w, m->nvolumes);
    for (uint32_t i = 0; i < m->nvolumes; i++)
        put_volume(w, &m->volumes[i]);
    put32(w, m->ndomains);
    for (uint32_t i = 0; i < m->ndomains; i++) {
        putstr(w, m->domains[i].name, META_DOMAIN_MAX);
        put8(w, m->domains[i].state);
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
    /* the runs since one index, adjacent equal ones merged: the same state has the same bytes (and the same hash)
     * whichever operations built it */
    uint32_t nsince = 0;
    for (uint32_t i = 0; i < m->nsince; i++)
        if (!i || m->since[i].index != m->since[i - 1].index ||
            m->since[i].first != m->since[i - 1].first + m->since[i - 1].count)
            nsince++;
    put32(w, nsince);
    for (uint32_t i = 0; i < m->nsince;) {
        uint32_t cnt = m->since[i].count, j = i + 1;
        while (j < m->nsince && m->since[j].index == m->since[i].index &&
               m->since[j].first == m->since[j - 1].first + m->since[j - 1].count)
            cnt += m->since[j++].count;
        put32(w, m->since[i].first);
        put32(w, cnt);
        put64(w, m->since[i].index);
        i = j;
    }
    put32(w, m->nmoves);
    for (uint32_t i = 0; i < m->nmoves; i++) {
        put32(w, m->moves[i].slot);
        put32(w, m->moves[i].home);
        put64(w, m->moves[i].from_index);
    }
    put32(w, m->npend);
    for (uint32_t i = 0; i < m->npend; i++) {
        put32(w, m->pend[i].slot);
        put32(w, m->pend[i].from);
        put32(w, m->pend[i].to);
        put64(w, m->pend[i].freeze_epoch);
    }
    put32(w, m->nusers);
    for (uint32_t i = 0; i < m->nusers; i++) {
        putstr(w, m->users[i].name, META_NAME_MAX);
        put32(w, m->users[i].len);
        put(w, m->users[i].blob, m->users[i].len);
    }
    put32(w, 0); /* the prefix-group rules (design-ha-modes.md M23): S4 fills the list; none exist yet */
}

int meta_snapshot_encode(const struct meta_state *m, uint8_t **out, size_t *len)
{
    uint8_t *buf = NULL;
    size_t n = 0, cap = 0;
    struct wr w = {&buf, &n, &cap};
    put32(&w, SNAP_MAGIC);
    put32(&w, SNAP_VERSION);
    put64(&w, m->applied);
    put64(&w, m->applied_term);
    encode_body(&w, m);
    put32(&w, iso_crc32c(0, buf, n));
    *out = buf;
    *len = n;
    return META_OK;
}

static int get_volume(struct rd *r, struct meta_volume *v)
{
    memset(v, 0, sizeof *v);
    v->number = get32(r);
    getstr(r, v->name, META_NAME_MAX);
    get(r, v->uuid, 16);
    v->kind = get8(r);
    get_storage(r, &v->st);
    v->min_isr = get32(r);
    v->nrep = get32(r);
    if (v->nrep > META_REPLICAS_MAX || (v->kind != META_VOL_REPLICATED && v->kind != META_VOL_SHARED))
        return -1;
    for (uint32_t j = 0; j < v->nrep; j++) {
        v->rep[j].node = get32(r);
        v->rep[j].learner = get8(r);
        v->rep[j].unanswered = get8(r);
        v->rep[j].claim_blocked = get8(r);
        v->rep[j].rid = get64(r);
        v->rep[j].open_gen = get64(r);
        if (v->rep[j].learner > 1 || v->rep[j].unanswered > 1 || v->rep[j].claim_blocked > 1)
            return -1;
    }
    v->leader = get32(r);
    v->epoch = get64(r);
    v->nisr = get_ids(r, v->isr);
    v->isr_version = get64(r);
    v->isr_commit = get64(r);
    v->highest_seen = get64(r);
    v->series_reserved = get32(r);
    v->states_reserved = get32(r);
    v->wm_gen = get64(r);
    v->dropped_before = (int64_t)get64(r);
    get_pos(r, &v->pos);
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
    return 0;
}

static int decode_body(struct rd *r, struct meta_state *m)
{
    m->config_len = get32(r);
    if (m->config_len > META_CONFIG_MAX)
        return -1;
    get(r, m->config, m->config_len);
    m->initialised = get8(r);
    get(r, m->uuid, 16);
    m->mode = get8(r);
    m->level = get32(r);
    m->restore_gen = get64(r);
    if (m->mode > META_MODE_SHARED || m->level > META_LEVEL) /* a level this build does not apply: never guess */
        return -1;
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
        m->nvolumes = i + 1;
        if (get_volume(r, v) != 0)
            return -1;
        if (i && v->number <= m->volumes[i - 1].number)
            r->bad = 1;
    }
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 3)
        return -1;
    m->domains = grow(m->domains, &m->domains_cap, n + 1, sizeof *m->domains);
    for (uint32_t i = 0; i < n && !r->bad; i++) {
        getstr(r, m->domains[i].name, META_DOMAIN_MAX);
        m->domains[i].state = get8(r);
        if (m->domains[i].state > META_DOM_ROLLED_BACK || (i && strcmp(m->domains[i].name, m->domains[i - 1].name) <= 0))
            r->bad = 1;
        m->ndomains = i + 1;
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
    if (r->bad || n > (r->len - r->off) / 16)
        return -1;
    m->since = grow(m->since, &m->since_cap, n + 1, sizeof *m->since);
    for (uint32_t i = 0; i < n; i++) {
        m->since[i].first = get32(r);
        m->since[i].count = get32(r);
        m->since[i].index = get64(r);
        if (r->bad || !m->since[i].count || m->since[i].first >= META_SLOTS ||
            m->since[i].count > META_SLOTS - m->since[i].first ||
            (i && m->since[i].first < m->since[i - 1].first + m->since[i - 1].count))
            return -1;
        m->nsince = i + 1;
    }
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 16)
        return -1;
    m->moves = grow(m->moves, &m->moves_cap, n + 1, sizeof *m->moves);
    for (uint32_t i = 0; i < n; i++) {
        m->moves[i].slot = get32(r);
        m->moves[i].home = get32(r);
        m->moves[i].from_index = get64(r);
    }
    m->nmoves = n;
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 20)
        return -1;
    m->pend = grow(m->pend, &m->pend_cap, n + 1, sizeof *m->pend);
    for (uint32_t i = 0; i < n; i++) {
        m->pend[i].slot = get32(r);
        m->pend[i].from = get32(r);
        m->pend[i].to = get32(r);
        m->pend[i].freeze_epoch = get64(r);
        if (r->bad || m->pend[i].slot >= META_SLOTS || (i && m->pend[i].slot <= m->pend[i - 1].slot))
            return -1;
        m->npend = i + 1;
    }
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
    if (get32(r) != 0) /* rules this build has no list for */
        return -1;
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
    if (get32(&r) != SNAP_MAGIC || get32(&r) != SNAP_VERSION)
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

#define FAIL(...)                       \
    do {                                \
        snprintf(why, n, __VA_ARGS__);  \
        return -1;                      \
    } while (0)

/* memcmp with a null pointer is undefined even for 0 bytes (UBSan) */
static int same_bytes(const void *a, const void *b, size_t n) { return !n || !memcmp(a, b, n); }

static int check_volume_step(const struct meta_volume *a, const struct meta_volume *b, char *why, size_t n)
{
    unsigned va = a->number;
    if (a->kind != b->kind)
        FAIL("volume %u: its kind changed", va);
    if (b->epoch < a->epoch)
        FAIL("volume %u: epoch went back from %llu to %llu", va, (unsigned long long)a->epoch,
             (unsigned long long)b->epoch);
    int moved = b->leader != a->leader;
    if (b->kind == META_VOL_REPLICATED) {
        if (b->epoch > a->epoch + 1)
            FAIL("volume %u: epoch jumped from %llu to %llu in one entry", va, (unsigned long long)a->epoch,
                 (unsigned long long)b->epoch);
        if (b->epoch == a->epoch && moved)
            FAIL("volume %u: leader changed from %u to %u within epoch %llu", va, a->leader, b->leader,
                 (unsigned long long)a->epoch);
    } else {
        /* the owner holds epoch + 1: another owner starts at epoch + 2 or later, and no epoch is ever re-used by a
         * new owner. Every relaxation here is for shared volumes only: a replicated volume that skips an epoch is
         * caught above. */
        if (moved && b->epoch < a->epoch + 2)
            FAIL("volume %u: the owner changed from %u to %u at epoch %llu, which was reserved for %u (from %llu)",
                 va, a->leader, b->leader, (unsigned long long)b->epoch, a->leader, (unsigned long long)a->epoch);
        if (b->epoch >= META_EPOCH_LIMIT)
            FAIL("volume %u: epoch %llu is beyond the u24 bound", va, (unsigned long long)b->epoch);
    }
    if (b->epoch > a->epoch && moved && !meta_volume_in_isr(a, b->leader))
        FAIL("volume %u: leader %u of epoch %llu was not in the ISR of epoch %llu", va, b->leader,
             (unsigned long long)b->epoch, (unsigned long long)a->epoch);
    if (b->kind == META_VOL_REPLICATED && b->epoch > a->epoch)
        for (uint32_t j = 0; j < b->nisr; j++)
            if (!meta_volume_in_isr(a, b->isr[j]))
                FAIL("volume %u: epoch %llu's ISR holds %u, which epoch %llu's did not", va,
                     (unsigned long long)b->epoch, b->isr[j], (unsigned long long)a->epoch);
    int isr_same = a->nisr == b->nisr && !memcmp(a->isr, b->isr, a->nisr * sizeof a->isr[0]);
    if (!isr_same && b->isr_version <= a->isr_version)
        FAIL("volume %u: the ISR changed without a new ISR version", va);
    if (b->isr_version < a->isr_version)
        FAIL("volume %u: the ISR version went back", va);
    /* a member that joined the ISR within an epoch was a replica (REPLICA_ADD) with the instance it has now; a member
     * that stays keeps its instance (a different rid is a different directory, not in the ISR) */
    for (uint32_t j = 0; j < b->nisr; j++) {
        const struct meta_replica *rb = meta_replica(b, b->isr[j]), *ra = meta_replica(a, b->isr[j]);
        if (!rb)
            continue; /* the next-state checks name it */
        if (meta_volume_in_isr(a, b->isr[j])) {
            if (ra && ra->rid != rb->rid)
                FAIL("volume %u: member %u is in the ISR with a changed replica instance", va, b->isr[j]);
        } else if (b->kind == META_VOL_REPLICATED && b->epoch == a->epoch && (!ra || ra->rid != rb->rid)) {
            FAIL("volume %u: ISR member %u joined without having been added as a replica with that instance", va,
                 b->isr[j]);
        }
    }
    if (b->highest_seen < a->highest_seen || b->series_reserved < a->series_reserved ||
        b->states_reserved < a->states_reserved || b->wm_gen < a->wm_gen || b->dropped_before < a->dropped_before)
        FAIL("volume %u: a reservation, watermark or highest epoch seen went down", va);
    for (uint32_t j = 0; j < a->nrep; j++) {
        const struct meta_replica *rb = meta_replica(b, a->rep[j].node);
        if (rb && rb->rid == a->rep[j].rid && rb->open_gen < a->rep[j].open_gen)
            FAIL("volume %u: replica %u's open generation went down", va, a->rep[j].node);
    }
    /* the epoch history only grows at its end, each start above the previous (replicated; a shared volume's start
     * means nothing and is not kept) */
    if (b->kind == META_VOL_REPLICATED) {
        if (b->nhist < a->nhist || !same_bytes(a->hist, b->hist, a->nhist * sizeof *a->hist))
            FAIL("volume %u: the epoch history changed before its end", va);
        for (uint32_t j = 1; j < b->nhist; j++)
            if (b->hist[j].start <= b->hist[j - 1].start || b->hist[j].epoch <= b->hist[j - 1].epoch)
                FAIL("volume %u: epoch %llu's start %llu is not above the one before", va,
                     (unsigned long long)b->hist[j].epoch, (unsigned long long)b->hist[j].start);
    }
    return 0;
}

int meta_check_step(const struct meta_state *prev, const struct meta_state *next, char *why, size_t n)
{
    if (prev->mode && next->mode != prev->mode)
        FAIL("the deployment's mode changed");
    if (next->level < prev->level || next->restore_gen < prev->restore_gen)
        FAIL("the level or the restore generation went down");
    for (uint32_t i = 0; i < prev->nvolumes; i++) {
        const struct meta_volume *a = &prev->volumes[i], *b = meta_volume(next, a->number);
        if (!b)
            FAIL("volume %u disappeared", a->number);
        if (check_volume_step(a, b, why, n))
            return -1;
    }
    for (uint32_t i = 0; i < next->nvolumes; i++) {
        const struct meta_volume *b = &next->volumes[i];
        if (!meta_volume_in_isr(b, b->leader))
            FAIL("volume %u: leader %u is not in its ISR", b->number, b->leader);
        for (uint32_t j = 0; j < b->nisr; j++)
            if (replica_at(b, b->isr[j]) < 0)
                FAIL("volume %u: ISR member %u is not a replica", b->number, b->isr[j]);
        if (next->mode && b->kind != next->mode)
            FAIL("volume %u: kind %u in a deployment of mode %u", b->number, b->kind, next->mode);
        for (uint32_t j = 0; j < b->nrep; j++) {
            if (b->rep[j].unanswered && !meta_volume_in_isr(b, b->rep[j].node))
                FAIL("volume %u: replica %u is marked unanswered but not in the ISR", b->number, b->rep[j].node);
            if (b->rep[j].learner && meta_volume_in_isr(b, b->rep[j].node))
                FAIL("volume %u: learner %u is in the ISR", b->number, b->rep[j].node);
        }
        if (b->kind == META_VOL_SHARED) {
            /* the ISR is every candidate: they all see the same storage */
            if (b->nisr != b->nrep || b->nhist != 0)
                FAIL("volume %u: a shared volume's ISR is not every candidate (or it keeps a history)", b->number);
        } else if (b->nhist < 1 || b->hist[b->nhist - 1].epoch != b->epoch) {
            FAIL("volume %u: the epoch history does not end at epoch %llu", b->number, (unsigned long long)b->epoch);
        }
    }
    for (uint32_t i = 0; i < prev->nmembers; i++)
        if (!meta_member(next, prev->members[i].id))
            FAIL("member %u disappeared", prev->members[i].id);
    if (next->next_member < prev->next_member || next->next_volume < prev->next_volume)
        FAIL("an id counter went back");
    /* a slot changes home only from a freeze committed in an earlier entry, under the epoch its old home had then */
    if (memcmp(prev->slot_home, next->slot_home, META_SLOTS * sizeof *prev->slot_home) != 0)
        for (uint32_t s = 0; s < META_SLOTS; s++) {
            if (prev->slot_home[s] == next->slot_home[s] || prev->slot_home[s] == META_NONE)
                continue;
            const struct meta_slot_pending *p = meta_slot_pending(prev, s);
            const struct meta_volume *from = meta_volume(prev, prev->slot_home[s]);
            if (!p || p->from != prev->slot_home[s] || p->to != next->slot_home[s] || !p->freeze_epoch)
                FAIL("slot %u moved from volume %u to %u without a committed freeze", s, prev->slot_home[s],
                     next->slot_home[s]);
            if (!from || p->freeze_epoch > from->epoch)
                FAIL("slot %u's freeze names an epoch its old home never had", s);
        }
    if (next->nmoves < prev->nmoves || !same_bytes(prev->moves, next->moves, prev->nmoves * sizeof *prev->moves))
        FAIL("the slot move chain changed before its end");
    for (uint32_t i = 0; i < next->npend; i++) {
        const struct meta_slot_pending *p = &next->pend[i];
        if (next->slot_home[p->slot] != p->from)
            FAIL("slot %u: a pending move names a home the slot does not have", p->slot);
        if (p->freeze_epoch) {
            const struct meta_volume *from = meta_volume(next, p->from);
            if (!from || p->freeze_epoch > from->epoch)
                FAIL("slot %u's freeze names an epoch its old home never had", p->slot);
        }
    }
    return 0;
}
