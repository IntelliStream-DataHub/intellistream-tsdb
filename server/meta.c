/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cluster's metadata state machine; see meta.h for what it holds and the rules it keeps.
 *
 * Bytes, all little-endian:
 *   entry     u32 magic "TME3" ("TME1" up to snapshot version 4: R4 grew the bodies of INIT, REPLICA_ADD and
 *             VOLUME_LEADER; "TME2" in version 5: R8 grew VOLUME_ADD's), u32 nops, then nops times { u8 kind, u32 len,
 *             body[len] }
 *   snapshot  u32 magic "TMS1", u32 version (SNAP_VERSION), u64 applied, u64 applied_term, the body below, u32 crc32c
 *             of all before
 * (version 2: shared volumes, replica instances, domains, slot moves in flight; version 1 is refused: no cluster
 * ran on it, and a cluster built before is re-initialised.) The snapshot is also the persisted copy a member keeps in
 * its --state-dir: one format. Strings are u16 length + bytes (no terminator), bounded by their field. A body is read by a reader that fails on
 * any overrun, so a damaged entry is META_ERR_FORMAT, never a read past the end.
 *
 * One rule beyond meta.h's: an entry touches a volume at most once (a second operation on it is META_ERR_INVALID).
 * Then a volume's change in one entry is one step, which meta_check_step can judge from the states on either side.
 * GC_SUSPEND is the one exception: it only sets a flag that nothing but GC_RESUME clears, so it may share an entry
 * with the ISR_CHANGE of force-leader (it goes first: the suspension is committed with the unclean election or before).
 */
#include "meta.h"

#include "repl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h> /* MUT(59) downsinceapply only; apply never reads a clock */

/* 3: the replicated volumes' settings (R3); 4: users with uid and version, max_uid (S2-1); 5: replica set changes
 * (R4: set_version, a replica's `replaces`, the last grant's target and end, rebuild_mb_s, rebuild_max,
 * transfer_wait_ms, leader_balance_s); 6: erasure-coded volumes (R8: redundancy, k, m, the stripe group and its version,
 * gc_suspended, gc_gen); 7: failure handling, holds, DESTROYED, stripe fill states and R9 settings (D2-D4, D9-D10,
 * D23); 8: member `replaced_by` for `node replace OLD NEW` failover resume (r9-ops, D22); 9: R10 redundancy conversion
 * record (`conv`, `conv_cancel_gen`), `retired_ver` on `stripe_pos`, and leader live-object counts per `stripe_version`
 * (E2, §6.4, D26, D28); 10: R13 member disks, declared role, computed capability, store ops, and R12/R13 scrub/disk
 * settings (E20, E25, E49, E52, E53, E57). A preview cluster is initialised again, not upgraded (the plan's D9): an
 * image of another version is refused, and its reader names both (meta_snapshot_version). */
#define SNAP_VERSION META_SNAP_VERSION

/* tests/test_meta.c --mutant N plants bug N (MUT(n)) in a build with -DMETA_MUTANTS; the simulator and the unit tests
 * must catch every one (a checker that catches nothing proves nothing). Without the define, MUT is 0. */
#ifdef META_MUTANTS
int meta_mutant;
#define MUT(n) (meta_mutant == (n))
#else
#define MUT(n) 0
#endif

#define ENTRY_MAGIC 0x37454d54u    /* "TME7": entries of snapshot version 10 (R13 disks, roles, store states, settings) */
#define ENTRY_MAGIC_V1 0x31454d54u /* "TME1": entries of the builds of versions 1-4, refused by name */
#define ENTRY_MAGIC_V2 0x32454d54u /* "TME2": entries of the builds of version 5, refused by name */
#define ENTRY_MAGIC_V3 0x33454d54u /* "TME3": entries of the builds of version 6, refused by name */
#define ENTRY_MAGIC_V4 0x34454d54u /* "TME4": entries of the builds of version 7, refused by name */
#define ENTRY_MAGIC_V5 0x35454d54u /* "TME5": entries of the builds of version 8, refused by name */
#define ENTRY_MAGIC_V6 0x36454d54u /* "TME6": entries of the builds of version 9, refused by name */
#define SNAP_MAGIC 0x31534d54u     /* "TMS1" */

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
    static const char *n[] = {"up", "down", "draining", "out", "destroyed"};
    return s >= 0 && s <= META_DESTROYED ? n[s] : "?";
}

const char *meta_down_reason_name(int r)
{
    static const char *n[] = {"none", "timeout", "failstop", "fault", "drain"};
    return r >= 0 && r <= META_DOWN_LAST ? n[r] : "?";
}

const char *meta_stripe_state_name(int s)
{
    static const char *n[] = {"whole", "filling"};
    return s >= 0 && s <= META_STRIPE_FILLING ? n[s] : "?";
}

const char *meta_conv_phase_name(int phase)
{
    static const char *n[] = {"none", "grow", "record", "reencode", "drop", "shrink"};
    return phase >= 0 && phase <= META_CONV_LAST ? n[phase] : "?";
}

const char *meta_disk_class_name(int c)
{
    static const char *n[] = {"none", "nvme", "ssd", "hdd"};
    return c >= 0 && c <= META_DISK_CLASS_LAST ? n[c] : "?";
}

const char *meta_store_state_name(int s)
{
    static const char *n[] = {"up", "down", "out", "destroyed"};
    return s >= 0 && s <= META_STORE_STATE_LAST ? n[s] : "?";
}

const char *meta_role_name(int r)
{
    static const char *n[] = {"both", "compute", "storage"};
    return r >= 0 && r <= META_ROLE_LAST ? n[r] : "?";
}

const char *meta_cap_name(uint8_t cap)
{
    static const char *n[] = {"none", "compute", "storage", "both"};
    return cap <= META_CAP_BOTH ? n[cap] : "?";
}

void meta_settings_default(struct meta_settings *s)
{
    s->replication = 3;
    s->min_isr = 1;
    s->node_timeout_ms = 3000;
    s->heartbeat_ms = 500;
    s->isr_lag_ms = 1000;
    s->rebuild_after_s = 1800;
    s->voter_replace_s = 600;
    s->voters = 5;
    s->volumes_per_server = 16;
    s->failover = META_FAILOVER_LEASE;
    s->read_barrier = 1;
    s->lease_s = 10;
    s->lease_ms = 2000;
    s->drift_ppm = 500;
    s->unanswered_ms = 1000;
    s->commit_interval_ms = 5;
    s->repl_retain_mb = 4096;
    s->repl_retain_s = 1800;
    s->idle_seal_ms = 1000;
    s->rebuild_mb_s = 200;
    s->rebuild_max = 2;
    s->transfer_wait_ms = 2000;
    s->leader_balance_s = 0; /* off until the owner decides from r4-transfer's measured write pause (D15) */
    s->opening_hold_s = 3600;
    s->failstop_out_s = 60;
    s->out_guard_pct = 25;
    s->out_guard_hold_s = 7200;
    s->repl_retain_down_gb = 64;
    s->young_high_pct = 90;
    s->balance_mb_s = 0;
    s->rebuild_concurrency = 4;
    s->scrub_days = 14;
    s->scrub_mb_s = 50;
    s->scrub_out_count = 8;
    s->log_on_hdd = 0;
    s->young_warn_pct = 75;
    s->young_move_pct = 80;
    s->disk_stall_s = 10;
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

/* The 65,536-entry slot_home table (256 KiB) is shared copy-on-write across copies of the state (meta_copy runs on
 * every applied entry, and almost no entry changes slot homes). A refcount header precedes the entries, like ublob;
 * any writer detaches via meta_slot_home_mut() when refs > 1. */
struct slot_hdr {
    uint32_t refs, pad;
};

static uint32_t *slots_new(void)
{
    struct slot_hdr *h = malloc(sizeof *h + META_SLOTS * sizeof(uint32_t));
    if (!h)
        abort();
    h->refs = 1;
    h->pad = 0;
    return (uint32_t *)(h + 1);
}

static void slots_ref(const uint32_t *p)
{
    if (p)
        __atomic_add_fetch(&((struct slot_hdr *)(void *)p - 1)->refs, 1, __ATOMIC_RELAXED);
}

static void slots_unref(const uint32_t *p)
{
    if (p && __atomic_sub_fetch(&((struct slot_hdr *)(void *)p - 1)->refs, 1, __ATOMIC_ACQ_REL) == 0)
        free((struct slot_hdr *)(void *)p - 1);
}

uint32_t *meta_slot_home_mut(struct meta_state *m)
{
    if (!m->slot_home) {
        uint32_t *sh = slots_new();
        for (uint32_t i = 0; i < META_SLOTS; i++)
            sh[i] = META_NONE;
        m->slot_home = sh;
        return sh;
    }
    struct slot_hdr *h = (struct slot_hdr *)(void *)m->slot_home - 1;
    if (__atomic_load_n(&h->refs, __ATOMIC_ACQUIRE) == 1)
        return (uint32_t *)(void *)m->slot_home;
    uint32_t *copy = slots_new();
    memcpy(copy, m->slot_home, META_SLOTS * sizeof *copy);
    slots_unref(m->slot_home);
    m->slot_home = copy;
    return copy;
}

void meta_init(struct meta_state *m)
{
    memset(m, 0, sizeof *m);
    meta_settings_default(&m->set);
    m->next_member = 1;
    uint32_t *sh = slots_new();
    for (uint32_t i = 0; i < META_SLOTS; i++)
        sh[i] = META_NONE;
    m->slot_home = sh;
}

/* A user's blob is shared by every copy of the state (meta_copy runs once per entry: copying megabytes of users each
 * time would cost more than the entry) and never changed after its put; a count before the bytes says how many
 * copies hold it. Copies may be handed to other threads (the cluster thread's watch), so the count is atomic. */
struct ublob {
    uint32_t refs, pad;
};

static uint8_t *ublob_new(uint32_t len)
{
    struct ublob *h = malloc(sizeof *h + (size_t)len + 1);
    if (!h)
        abort(); /* as every other op: a failed apply on one member would let the copies diverge */
    h->refs = 1;
    return (uint8_t *)(h + 1);
}

static void ublob_ref(uint8_t *b) { __atomic_add_fetch(&((struct ublob *)b - 1)->refs, 1, __ATOMIC_RELAXED); }

static void ublob_unref(uint8_t *b)
{
    if (b && __atomic_sub_fetch(&((struct ublob *)b - 1)->refs, 1, __ATOMIC_ACQ_REL) == 0)
        free((struct ublob *)b - 1);
}

static void put_le32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v, p[1] = (uint8_t)(v >> 8), p[2] = (uint8_t)(v >> 16), p[3] = (uint8_t)(v >> 24);
}

static uint32_t get_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

void meta_free(struct meta_state *m)
{
    for (uint32_t i = 0; i < m->nvolumes; i++)
        free(m->volumes[i].hist);
    for (uint32_t i = 0; i < m->nusers; i++)
        ublob_unref(m->users[i].blob);
    free(m->domains);
    free(m->since);
    free(m->pend);
    free(m->members);
    free(m->volumes);
    slots_unref(m->slot_home);
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
    if (!dst->members || !dst->volumes || !dst->moves || !dst->users || !dst->domains || !dst->since || !dst->pend)
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
    dst->slot_home = src->slot_home;
    slots_ref(dst->slot_home);
    for (uint32_t i = 0; i < src->nvolumes; i++) {
        struct meta_volume *v = &dst->volumes[i];
        v->hist = malloc((v->nhist + 1) * sizeof *v->hist);
        if (!v->hist)
            abort();
        copy_n(v->hist, src->volumes[i].hist, v->nhist * sizeof *v->hist);
    }
    for (uint32_t i = 0; i < src->nusers; i++)
        ublob_ref(dst->users[i].blob);
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

uint64_t meta_users_bytes(const struct meta_state *m)
{
    uint64_t n = 0;
    for (uint32_t i = 0; i < m->nusers; i++)
        n += m->users[i].len + strlen(m->users[i].name) + 1;
    return n;
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

const struct meta_user *meta_user(const struct meta_state *m, const char *name)
{
    int32_t at = user_at(m, name);
    return at >= 0 ? &m->users[at] : NULL;
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return x < y ? -1 : x > y;
}

/* 0 when no two users share a uid (a decoded snapshot; apply keeps it by construction) */
static int users_uids_unique(const struct meta_state *m)
{
    if (m->nusers < 2)
        return 0;
    uint32_t *u = malloc(m->nusers * sizeof *u);
    if (!u)
        abort();
    for (uint32_t i = 0; i < m->nusers; i++)
        u[i] = m->users[i].uid;
    qsort(u, m->nusers, sizeof *u, cmp_u32);
    int rc = 0;
    for (uint32_t i = 1; i < m->nusers && !rc; i++)
        rc = u[i] == u[i - 1] ? -1 : 0;
    free(u);
    return rc;
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

static int store_id_zero(const uint8_t id[16])
{
    static const uint8_t zero[16];
    return memcmp(id, zero, 16) == 0;
}

static int member_disk_at(const struct meta_member *mb, const uint8_t store_id[16])
{
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++)
        if (memcmp(mb->disks[i].store_id, store_id, 16) == 0)
            return (int)i;
    return -1;
}

const struct meta_disk *meta_member_disk(const struct meta_member *mb, const uint8_t store_id[16])
{
    if (!mb || !store_id)
        return NULL;
    int i = member_disk_at(mb, store_id);
    return i >= 0 ? &mb->disks[i] : NULL;
}

int meta_member_has_fast_disk(const struct meta_member *mb)
{
    if (!mb)
        return 0;
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++)
        if (mb->disks[i].state == META_STORE_UP && meta_disk_class_fast(mb->disks[i].disk_class))
            return 1;
    return 0;
}

uint8_t meta_member_effective_cap(const struct meta_member *mb)
{
    if (!mb)
        return META_CAP_NONE;
    uint8_t mask = mb->role == META_ROLE_COMPUTE ? META_CAP_COMPUTE
                 : mb->role == META_ROLE_STORAGE ? META_CAP_STORAGE
                                                 : META_CAP_BOTH;
    return (uint8_t)(mb->capability & mask);
}

/* Compute member capability from UP disks and cluster log_on_hdd (E49):
 *   compute-capable = UP fast disk (nvme/ssd), or UP hdd when log_on_hdd == 1 and no fast disk;
 *   storage-capable = UP disk with META_DISK_USE_CHUNK. */
static uint8_t member_compute_cap(const struct meta_member *mb, uint8_t log_on_hdd)
{
    int has_fast = 0, has_hdd = 0, has_chunk = 0;
    for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++) {
        const struct meta_disk *d = &mb->disks[i];
        if (d->state != META_STORE_UP)
            continue;
        if (meta_disk_class_fast(d->disk_class))
            has_fast = 1;
        else if (d->disk_class == META_DISK_HDD)
            has_hdd = 1;
        if (d->uses & META_DISK_USE_CHUNK)
            has_chunk = 1;
    }
    uint8_t cap = META_CAP_NONE;
    if (has_fast || (log_on_hdd && has_hdd))
        cap |= META_CAP_COMPUTE;
    if (has_chunk)
        cap |= META_CAP_STORAGE;
    return cap;
}

static int store_id_known(const struct meta_state *m, const uint8_t store_id[16])
{
    for (uint32_t i = 0; i < m->nmembers; i++)
        if (member_disk_at(&m->members[i], store_id) >= 0)
            return 1;
    return 0;
}

/* 1 if member `mb` is eligible for a log replica (E49, E50):
 *   - declared role must allow compute (BOTH or COMPUTE);
 *   - when disks are registered (ndisks > 0), effective capability must include META_CAP_COMPUTE.
 * Members with 0 registered disks (pre-r13-start) pass the disk check for backwards compatibility. */
static int member_log_capable(const struct meta_member *mb)
{
    if (!mb || mb->role == META_ROLE_STORAGE)
        return 0;
    if (mb->ndisks == 0)
        return 1;
    return (meta_member_effective_cap(mb) & META_CAP_COMPUTE) != 0;
}

static int member_log_ok(const struct meta_member *mb)
{
    if (!mb || mb->role == META_ROLE_STORAGE)
        return 0;
    if (mb->ndisks == 0)
        return 1;
    if (MUT(68)) { /* logonhdd: ignores log_on_hdd (allows HDD-only member even when log_on_hdd == 0) */
        for (uint32_t i = 0; i < mb->ndisks && i < META_DISKS_MAX; i++)
            if (mb->disks[i].state == META_STORE_UP)
                return 1;
        return 0;
    }
    return (meta_member_effective_cap(mb) & META_CAP_COMPUTE) != 0;
}

/* 1 if member `mb` is eligible for an EC stripe position (E49, E51):
 *   - declared role must allow storage (BOTH or STORAGE);
 *   - when disks are registered (ndisks > 0), effective capability must include META_CAP_STORAGE. */
static int member_storage_capable(const struct meta_member *mb)
{
    if (!mb || mb->role == META_ROLE_COMPUTE)
        return 0;
    if (mb->ndisks == 0)
        return 1;
    return (meta_member_effective_cap(mb) & META_CAP_STORAGE) != 0;
}

static int disk_fields_valid(const struct meta_disk *d)
{
    return !store_id_zero(d->store_id) && d->disk_class >= META_DISK_NVME && d->disk_class <= META_DISK_CLASS_LAST &&
           d->uses != 0 && !(d->uses & ~META_DISK_USE_ALL) && d->state <= META_STORE_STATE_LAST && d->size_bytes > 0;
}

static int stores_unique(const struct meta_state *m)
{
    for (uint32_t i = 0; i < m->nmembers; i++) {
        const struct meta_member *mi = &m->members[i];
        for (uint32_t di = 0; di < mi->ndisks && di < META_DISKS_MAX; di++) {
            for (uint32_t dj = di + 1; dj < mi->ndisks && dj < META_DISKS_MAX; dj++)
                if (memcmp(mi->disks[di].store_id, mi->disks[dj].store_id, 16) == 0)
                    return 0;
            for (uint32_t j = i + 1; j < m->nmembers; j++)
                if (member_disk_at(&m->members[j], mi->disks[di].store_id) >= 0)
                    return 0;
        }
    }
    return 1;
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

static int member_live(const struct meta_state *m, const struct meta_liveness *liveness, uint32_t node)
{
    const struct meta_member *mb = meta_member(m, node);
    if (!mb || (mb->state != META_UP && mb->state != META_DRAINING))
        return 0;
    if (liveness && liveness->live && !liveness->live(liveness->ctx, node))
        return 0;
    return 1;
}

static int stripe_margin_for(const struct meta_state *m, const struct meta_volume *v,
                             const struct meta_liveness *liveness, uint32_t k, uint32_t npos)
{
    uint32_t lim = npos < v->nstripe ? npos : v->nstripe;
    if (lim > META_STRIPE_MAX)
        lim = META_STRIPE_MAX;
    int present = 0;
    for (uint32_t p = 0; p < lim; p++) {
        if (v->stripe_pos[p].state == META_STRIPE_FILLING)
            continue; /* missing for objects below this position's set_ver */
        if (member_live(m, liveness, v->stripe[p]))
            present++;
    }
    return present - (int)k;
}

void meta_vol_margins(const struct meta_state *m, const struct meta_volume *v, const struct meta_liveness *liveness,
                      int *young, int *settled)
{
    if (young)
        *young = v->kind == META_VOL_SHARED ? 0 : (int)v->nisr - (int)v->min_isr;
    if (!settled)
        return;
    int have = 0, best = 0;
    if (v->redundancy == META_RED_EC && v->nstripe > 0) {
        best = stripe_margin_for(m, v, liveness, v->ec_k, (uint32_t)v->ec_k + v->ec_m);
        have = 1;
    }
    /* During REENCODE/DROP, old objects may still use the original EC geometry: take the minimum (D29). */
    if ((v->conv.phase == META_CONV_REENCODE || v->conv.phase == META_CONV_DROP) &&
        v->conv.orig_redundancy == META_RED_EC && v->nstripe >= (uint32_t)v->conv.orig_k + v->conv.orig_m) {
        int om = stripe_margin_for(m, v, liveness, v->conv.orig_k, (uint32_t)v->conv.orig_k + v->conv.orig_m);
        if (!have || om < best)
            best = om;
        have = 1;
    }
    /* After a cancelled REENCODE, leftover target-geometry EC objects keep capping settled margin until cleared (D29). */
    if (v->mixed_redundancy == META_RED_EC && v->nstripe >= (uint32_t)v->mixed_k + v->mixed_m) {
        int mm = stripe_margin_for(m, v, liveness, v->mixed_k, (uint32_t)v->mixed_k + v->mixed_m);
        if (!have || mm < best)
            best = mm;
        have = 1;
    }
    *settled = have ? best : 0;
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
    put32(w, META_SETTINGS_WORDS);
    put32(w, s->replication);
    put32(w, s->min_isr);
    put32(w, s->node_timeout_ms);
    put32(w, s->heartbeat_ms);
    put32(w, s->isr_lag_ms);
    put32(w, s->rebuild_after_s);
    put32(w, s->voter_replace_s);
    put32(w, s->voters);
    put32(w, s->volumes_per_server);
    put8(w, s->failover);
    put8(w, s->read_barrier);
    put32(w, s->lease_s);
    put32(w, s->lease_ms);
    put32(w, s->drift_ppm);
    put32(w, s->unanswered_ms);
    put32(w, s->commit_interval_ms);
    put32(w, s->repl_retain_mb);
    put32(w, s->repl_retain_s);
    put32(w, s->idle_seal_ms);
    put32(w, s->rebuild_mb_s);
    put32(w, s->rebuild_max);
    put32(w, s->transfer_wait_ms);
    put32(w, s->leader_balance_s);
    put32(w, s->opening_hold_s);
    put32(w, s->failstop_out_s);
    put32(w, s->out_guard_pct);
    put32(w, s->out_guard_hold_s);
    put32(w, s->repl_retain_down_gb);
    put32(w, s->young_high_pct);
    put32(w, s->balance_mb_s);
    put32(w, s->rebuild_concurrency);
    put32(w, s->scrub_days);
    put32(w, s->scrub_mb_s);
    put32(w, s->scrub_out_count);
    put8(w, s->log_on_hdd);
    put32(w, s->young_warn_pct);
    put32(w, s->young_move_pct);
    put32(w, s->disk_stall_s);
}

static void get_settings(struct rd *r, struct meta_settings *s)
{
    uint32_t n = get32(r);
    if (n != META_SETTINGS_WORDS) { /* the count of this build's fields: until 1.0 a change of them bumps the level (and re-inits), after it
                    * a new field is a new op kind (the freeze rule at the head of this file) */
        r->bad = 1;
        return;
    }
    s->replication = get32(r);
    s->min_isr = get32(r);
    s->node_timeout_ms = get32(r);
    s->heartbeat_ms = get32(r);
    s->isr_lag_ms = get32(r);
    s->rebuild_after_s = get32(r);
    s->voter_replace_s = get32(r);
    s->voters = get32(r);
    s->volumes_per_server = get32(r);
    s->failover = get8(r);
    s->read_barrier = get8(r);
    s->lease_s = get32(r);
    s->lease_ms = get32(r);
    s->drift_ppm = get32(r);
    s->unanswered_ms = get32(r);
    s->commit_interval_ms = get32(r);
    s->repl_retain_mb = get32(r);
    s->repl_retain_s = get32(r);
    s->idle_seal_ms = get32(r);
    s->rebuild_mb_s = get32(r);
    s->rebuild_max = get32(r);
    s->transfer_wait_ms = get32(r);
    s->leader_balance_s = get32(r);
    s->opening_hold_s = get32(r);
    s->failstop_out_s = get32(r);
    s->out_guard_pct = get32(r);
    s->out_guard_hold_s = get32(r);
    s->repl_retain_down_gb = get32(r);
    s->young_high_pct = get32(r);
    s->balance_mb_s = get32(r);
    s->rebuild_concurrency = get32(r);
    s->scrub_days = get32(r);
    s->scrub_mb_s = get32(r);
    s->scrub_out_count = get32(r);
    s->log_on_hdd = get8(r);
    s->young_warn_pct = get32(r);
    s->young_move_pct = get32(r);
    s->disk_stall_s = get32(r);
}

static int settings_valid(const struct meta_settings *s)
{
    return s->replication >= 1 && s->replication <= META_REPLICAS_MAX && s->min_isr >= 1 &&
           s->min_isr <= s->replication && s->failover <= META_FAILOVER_BMC && s->read_barrier <= 1 &&
           s->lease_s >= 1 && s->lease_ms >= 1 && s->drift_ppm < 1000000 && s->commit_interval_ms >= 1 &&
           s->isr_lag_ms >= META_ISR_LAG_MIN_MS && s->idle_seal_ms >= META_IDLE_SEAL_MIN_MS &&
           s->rebuild_after_s >= 1 && s->rebuild_mb_s >= 1 && s->rebuild_max >= 1 && s->rebuild_max <= 16 &&
           s->transfer_wait_ms >= 100 && s->transfer_wait_ms <= 60000 && s->leader_balance_s <= 86400 &&
           s->opening_hold_s <= 86400 && s->failstop_out_s >= 1 && s->out_guard_pct >= 1 && s->out_guard_pct <= 100 &&
           s->out_guard_hold_s >= 1 && s->out_guard_hold_s <= 86400 && s->young_high_pct >= 10 &&
           s->young_high_pct <= 99 && s->rebuild_concurrency >= 1 && s->rebuild_concurrency <= 64 &&
           s->scrub_days >= 1 && s->scrub_days <= 365 && s->scrub_mb_s >= 1 && s->scrub_out_count >= 1 &&
           s->log_on_hdd <= 1 && s->young_warn_pct >= 1 && s->young_move_pct >= 1 &&
           s->young_warn_pct <= s->young_move_pct && s->young_move_pct < s->young_high_pct &&
           s->disk_stall_s >= 1 && s->disk_stall_s <= 3600;
}

static void put_disk(struct wr *w, const struct meta_disk *d)
{
    put(w, d->store_id, 16);
    put8(w, d->disk_class);
    put8(w, d->uses);
    put8(w, d->state);
    put64(w, d->size_bytes);
    put64(w, d->sync_us);
}

static void get_disk(struct rd *r, struct meta_disk *d)
{
    memset(d, 0, sizeof *d);
    get(r, d->store_id, 16);
    d->disk_class = get8(r);
    d->uses = get8(r);
    d->state = get8(r);
    d->size_bytes = get64(r);
    d->sync_us = get64(r);
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
    put64(w, mb->down_since_ms);
    put8(w, mb->down_reason);
    put64(w, mb->down_clock_ms);
    put64(w, mb->hold_until_ms);
    put64(w, mb->hold_clock_ms);
    put32(w, MUT(60) ? 0u : mb->replaced_by); /* replacenosnap */
    put8(w, mb->role);
    put8(w, mb->capability);
    uint32_t nd = mb->ndisks <= META_DISKS_MAX ? mb->ndisks : META_DISKS_MAX;
    put32(w, nd);
    for (uint32_t i = 0; i < nd; i++)
        put_disk(w, &mb->disks[i]);
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
    mb->down_since_ms = get64(r);
    mb->down_reason = get8(r);
    mb->down_clock_ms = get64(r);
    mb->hold_until_ms = get64(r);
    mb->hold_clock_ms = get64(r);
    mb->replaced_by = get32(r);
    mb->role = get8(r);
    mb->capability = get8(r);
    uint32_t nd = get32(r);
    if (mb->state > META_DESTROYED || (mb->flags & ~META_MF_ALL) || mb->down_reason > META_DOWN_LAST ||
        mb->role > META_ROLE_LAST || (mb->capability & ~META_CAP_BOTH) || nd > META_DISKS_MAX) {
        r->bad = 1;
        return;
    }
    mb->ndisks = nd;
    for (uint32_t i = 0; i < nd && !r->bad; i++) {
        get_disk(r, &mb->disks[i]);
        if (!disk_fields_valid(&mb->disks[i]))
            r->bad = 1;
    }
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

void meta_op_member_state(struct meta_entry *e, uint32_t id, int state, uint64_t incarnation, uint8_t reason,
                          uint64_t since_ms, uint64_t op_clock_ms)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_MEMBER_STATE);
    put32(&w, id);
    put8(&w, (uint8_t)state);
    put64(&w, incarnation);
    put8(&w, reason);
    put64(&w, since_ms);
    put64(&w, op_clock_ms);
    op_end(e, st);
}

void meta_op_node_hold(struct meta_entry *e, uint32_t id, uint64_t until_ms, uint64_t op_clock_ms)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_NODE_HOLD);
    put32(&w, id);
    put64(&w, until_ms);
    put64(&w, op_clock_ms);
    op_end(e, st);
}

void meta_op_node_replace(struct meta_entry *e, uint32_t old_node, uint32_t new_node)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_NODE_REPLACE);
    put32(&w, old_node);
    put32(&w, new_node);
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
    put8(&w, a->redundancy);
    put8(&w, a->ec_k);
    put8(&w, a->ec_m);
    uint32_t ns = a->nstripe <= META_STRIPE_MAX ? a->nstripe : 0;
    put32(&w, ns);
    for (uint32_t i = 0; i < ns; i++)
        put32(&w, a->stripe[i]);
    op_end(e, st);
}

uint32_t meta_volume_min_isr(uint8_t redundancy, uint8_t ec_m, uint32_t n, uint32_t cluster_min_isr)
{
    uint32_t want = redundancy == META_RED_EC ? (MUT(51) ? 1 : ec_m) : cluster_min_isr; /* minisrone */
    if (want < 1)
        want = 1;
    return want < n ? want : n;
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
    put32(&w, g->target);
    put64(&w, g->min_end);
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

void meta_op_replica_add(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node, uint64_t rid,
                         uint32_t replaces)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_ADD);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, node);
    put64(&w, rid);
    put32(&w, replaces);
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

void meta_op_user_put_as(struct meta_entry *e, const char *name, uint64_t version, const void *blob, uint32_t len,
                         const char *actor, uint64_t actor_version)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_USER_PUT);
    putstr(&w, name, META_USER_NAME_MAX);
    put64(&w, version);
    put32(&w, len);
    put(&w, blob, len);
    if (actor && actor[0]) { /* the guard: a trailer, absent for the operator's changes */
        putstr(&w, actor, META_USER_NAME_MAX);
        put64(&w, actor_version);
    }
    op_end(e, st);
}

void meta_op_user_put(struct meta_entry *e, const char *name, uint64_t version, const void *blob, uint32_t len)
{
    meta_op_user_put_as(e, name, version, blob, len, NULL, 0);
}

void meta_op_user_del_as(struct meta_entry *e, const char *name, const char *actor, uint64_t actor_version)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_USER_DEL);
    putstr(&w, name, META_USER_NAME_MAX);
    if (actor && actor[0]) {
        putstr(&w, actor, META_USER_NAME_MAX);
        put64(&w, actor_version);
    }
    op_end(e, st);
}

void meta_op_user_del(struct meta_entry *e, const char *name) { meta_op_user_del_as(e, name, NULL, 0); }

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

void meta_op_replica_open(struct meta_entry *e, uint32_t number, uint32_t node, uint64_t rid, uint64_t gen,
                          uint64_t nonce)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_OPEN);
    put32(&w, number);
    put32(&w, node);
    put64(&w, rid);
    put64(&w, gen);
    put64(&w, nonce);
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

void meta_op_replica_reset(struct meta_entry *e, uint32_t number, uint64_t epoch, uint32_t node, uint64_t rid)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_REPLICA_RESET);
    put32(&w, number);
    put64(&w, epoch);
    put32(&w, node);
    put64(&w, rid);
    op_end(e, st);
}

void meta_op_stripe_set(struct meta_entry *e, uint32_t number, uint32_t pos, uint32_t node, uint8_t state,
                        uint32_t prev, uint64_t stripe_version)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_STRIPE_SET);
    put32(&w, number);
    put32(&w, pos);
    put32(&w, node);
    put8(&w, state);
    put32(&w, prev);
    put64(&w, stripe_version);
    op_end(e, st);
}

void meta_op_stripe_whole(struct meta_entry *e, uint32_t number, uint32_t pos, uint64_t stripe_version)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_STRIPE_WHOLE);
    put32(&w, number);
    put32(&w, pos);
    put64(&w, stripe_version);
    op_end(e, st);
}

static void op_vol_only(struct meta_entry *e, uint8_t kind, uint32_t number)
{
    struct wr w;
    size_t st = op_begin(e, &w, kind);
    put32(&w, number);
    op_end(e, st);
}

void meta_op_gc_suspend(struct meta_entry *e, uint32_t number) { op_vol_only(e, META_OP_GC_SUSPEND, number); }
void meta_op_gc_resume(struct meta_entry *e, uint32_t number) { op_vol_only(e, META_OP_GC_RESUME, number); }

void meta_op_volume_redundancy(struct meta_entry *e, uint32_t number, const struct meta_redundancy_target *target,
                               uint64_t epoch)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_REDUNDANCY);
    put32(&w, number);
    put64(&w, epoch);
    put8(&w, target->redundancy);
    put8(&w, target->ec_k);
    put8(&w, target->ec_m);
    put32(&w, target->replicas);
    op_end(e, st);
}

void meta_op_volume_conv_step(struct meta_entry *e, uint32_t number, uint8_t from_phase, uint8_t to_phase,
                              const uint32_t *stripe, uint32_t nstripe)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_CONV_STEP);
    put32(&w, number);
    put8(&w, from_phase);
    put8(&w, to_phase);
    uint32_t ns = (stripe && nstripe <= META_STRIPE_MAX) ? nstripe : 0;
    put32(&w, ns);
    for (uint32_t i = 0; i < ns; i++)
        put32(&w, stripe[i]);
    op_end(e, st);
}

void meta_op_volume_conv_cancel(struct meta_entry *e, uint32_t number)
{
    op_vol_only(e, META_OP_VOLUME_CONV_CANCEL, number);
}

void meta_op_volume_live_objs(struct meta_entry *e, uint32_t number, uint64_t epoch, uint64_t stripe_version,
                              uint8_t overflow, const struct meta_live_sv *sv, uint32_t nsv)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_VOLUME_LIVE_OBJS);
    put32(&w, number);
    put64(&w, epoch);
    put64(&w, stripe_version);
    put8(&w, overflow);
    uint32_t n = (sv && nsv <= META_LIVE_SV_MAX) ? nsv : 0;
    put32(&w, n);
    for (uint32_t i = 0; i < n; i++) {
        put64(&w, sv[i].stripe_version);
        put64(&w, sv[i].count);
    }
    op_end(e, st);
}

void meta_op_store_add(struct meta_entry *e, uint32_t node, const struct meta_disk *disk)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_STORE_ADD);
    put32(&w, node);
    put_disk(&w, disk);
    op_end(e, st);
}

void meta_op_store_class(struct meta_entry *e, uint32_t node, const uint8_t store_id[16], uint8_t disk_class)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_STORE_CLASS);
    put32(&w, node);
    put(&w, store_id, 16);
    put8(&w, disk_class);
    op_end(e, st);
}

void meta_op_store_state(struct meta_entry *e, uint32_t node, const uint8_t store_id[16], uint8_t state)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_STORE_STATE);
    put32(&w, node);
    put(&w, store_id, 16);
    put8(&w, state);
    op_end(e, st);
}

void meta_op_member_role(struct meta_entry *e, uint32_t node, uint8_t role)
{
    struct wr w;
    size_t st = op_begin(e, &w, META_OP_MEMBER_ROLE);
    put32(&w, node);
    put8(&w, role);
    op_end(e, st);
}

void meta_op_volume_force(struct meta_entry *e, const struct meta_volume *v, uint32_t node)
{
    /* the leader's log may lack markers the old ISR committed: no orphan may be judged by it until the operator says
     * so (E14), including while retired EC positions still exist during ec -> replicas conversion or after cancel */
    if ((v->redundancy == META_RED_EC || v->nstripe > 0) && !MUT(54)) /* forcenosuspend */
        meta_op_gc_suspend(e, v->number);
    uint32_t ids[META_REPLICAS_MAX];
    uint64_t rids[META_REPLICAS_MAX];
    uint32_t n = 0;
    for (uint32_t i = 0; i < v->nrep && n < META_REPLICAS_MAX; i++)
        if (meta_volume_in_isr(v, v->rep[i].node) || v->rep[i].node == node) {
            ids[n] = v->rep[i].node;
            rids[n++] = v->rep[i].rid;
        }
    meta_op_isr_change(e, v->number, v->epoch, v->isr_version, ids, rids, n, 0);
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

/* OUT members keep their peer address and identity reserved (D2: a returning server is the same server); only
 * DESTROYED members free them. */
static int addr_taken(const struct meta_state *m, const char *peer)
{
    for (uint32_t i = 0; i < m->nmembers; i++)
        if (m->members[i].state != META_DESTROYED && !strcmp(m->members[i].peer, peer))
            return 1;
    return 0;
}

static int ident_taken(const struct meta_state *m, const uint8_t ident[16])
{
    static const uint8_t zero[16];
    if (!memcmp(ident, zero, 16))
        return 0;
    for (uint32_t i = 0; i < m->nmembers; i++)
        if (m->members[i].state != META_DESTROYED && !memcmp(m->members[i].ident, ident, 16))
            return 1;
    return 0;
}

static int insert_member(struct meta_state *m, const struct meta_member *mb)
{
    if (mb->id == 0 || mb->id == META_NONE || !mb->peer[0] || mb->role > META_ROLE_LAST || mb->ndisks > META_DISKS_MAX)
        return META_ERR_INVALID;
    int32_t at = member_at(m, mb->id);
    if (at >= 0 || mb->id < m->next_member || addr_taken(m, mb->peer) || ident_taken(m, mb->ident))
        return META_ERR_EXISTS; /* ids are never reused, nor a live or out server's peer address/ident under two ids */
    for (uint32_t i = 0; i < mb->ndisks; i++) {
        if (!disk_fields_valid(&mb->disks[i]) || mb->disks[i].state != META_STORE_UP)
            return META_ERR_INVALID;
        if (store_id_known(m, mb->disks[i].store_id))
            return META_ERR_EXISTS;
        for (uint32_t j = 0; j < i; j++)
            if (memcmp(mb->disks[j].store_id, mb->disks[i].store_id, 16) == 0)
                return META_ERR_EXISTS;
    }
    uint8_t cap = member_compute_cap(mb, m->set.log_on_hdd);
    if (mb->ndisks > 0 && ((mb->role == META_ROLE_COMPUTE && !(cap & META_CAP_COMPUTE)) ||
                           (mb->role == META_ROLE_STORAGE && !(cap & META_CAP_STORAGE))))
        return META_ERR_INVALID;
    uint32_t pos = (uint32_t)(-1 - at);
    m->members = grow(m->members, &m->members_cap, m->nmembers + 1, sizeof *m->members);
    memmove(&m->members[pos + 1], &m->members[pos], (m->nmembers - pos) * sizeof *m->members);
    m->members[pos] = *mb;
    m->members[pos].state = META_UP;
    m->members[pos].down_since_ms = 0;
    m->members[pos].down_reason = META_DOWN_NONE;
    m->members[pos].down_clock_ms = 0;
    m->members[pos].hold_until_ms = 0;
    m->members[pos].hold_clock_ms = 0;
    m->members[pos].replaced_by = 0;
    m->members[pos].capability = cap;
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
/* repl_meta_promote over the record and the grant's fences and replies: the rule both the proposer (the pick, through
 * meta_promote_pick) and the apply run */
static int promote_rule(const struct meta_volume *v, const struct meta_grant *g, struct repl_vmeta *out)
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
    /* a planned transfer's grant (D14) names its target: the rule picks it or nothing (repl_meta_promote_to); MUT(49)
     * (targetignored) picks as for a failover */
    int target = g->target && !MUT(49) ? replica_at(v, g->target) : -1;
    if (g->target && !MUT(49) && target < 0)
        return META_ERR_INVALID;
    /* MUT(14) (versionless) drops the ISR version check here too: repl_meta_promote makes it again */
    int rc = repl_meta_promote_to(&vm, g->epoch, MUT(14) ? v->isr_version : g->isr_version, fenced, rr,
                                  (int)g->nreplies, target, g->target ? g->min_end : 0);
    *out = vm;
    if (rc == REPL_E_STALE)
        return META_ERR_STALE;
    if (rc != REPL_OK || vm.leader < 0 || (uint32_t)vm.leader >= v->nrep)
        return META_ERR_INVALID;
    return META_OK;
}

int meta_promote_pick(const struct meta_volume *v, const struct meta_grant *g, uint32_t *leader)
{
    struct repl_vmeta vm;
    int rc = promote_rule(v, g, &vm);
    if (rc == META_OK)
        *leader = v->rep[vm.leader].node;
    return rc;
}

static int promote(struct meta_volume *v, const struct meta_grant *g)
{
    struct repl_vmeta vm;
    int rc = promote_rule(v, g, &vm);
    if (rc != META_OK)
        return rc;
    if (v->rep[vm.leader].node != g->leader)
        return META_ERR_INVALID; /* the proposer's pick is not what the rule gives: a bug or a stale view */
    if (g->target && v->rep[vm.leader].node != g->target && !MUT(49))
        return META_ERR_INVALID; /* a planned grant leads to its target or not at all (D14) */
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
    if (MUT(52)) /* gcresumeany: a new leader turns orphan collection back on */
        v->gc_suspended = 0;
    return META_OK;
}

/* A shared volume's grant to g->leader: the rules of the head comment. */
static int grant_shared(struct meta_state *m, struct meta_volume *v, const struct meta_grant *g)
{
    int w = replica_at(v, g->leader);
    const struct meta_member *mb = meta_member(m, g->leader);
    if (w < 0 || !mb || (mb->state >= META_OUT && !MUT(25)))
        return META_ERR_INVALID;
    int same = g->leader == v->leader;
    if ((g->new_epoch <= v->epoch && !(MUT(31) && same && g->new_epoch == v->epoch)) || (!same && g->new_epoch < v->epoch + (MUT(2) ? 1 : 2)))
        return META_ERR_INVALID; /* e + 1 is the owner's */
    if (g->new_epoch >= META_EPOCH_LIMIT - 1 && !MUT(9))
        return META_ERR_RANGE; /* the reservation after it must stay legal too */
    if (g->new_epoch <= v->highest_seen)
        return META_ERR_STALE;
    if (v->rep[w].claim_blocked)
        for (uint32_t i = 0; i < v->nrep; i++)
            if (!v->rep[i].claim_blocked)
                return META_ERR_BLOCKED;
    if (g->reason == META_GR_RESIGNED)
        return META_ERR_INVALID; /* the replicated mode's reason: a shared owner does not resign */
    /* Under attested and bmc every move of a shared volume to another node needs the attestation, whatever reason the
     * proposer names: a label it chooses must not decide whether the only server-side fence is applied. Two moves
     * stand on the old owner's own word: PLANNED (its release is the basis) and REFUSED (the claimant that refused
     * never held the lock). */
    if (!same && !MUT(8) && m->set.failover != META_FAILOVER_LEASE && g->reason != META_GR_PLANNED &&
        g->reason != META_GR_REFUSED) {
        const struct meta_attest *a = &g->att;
        if ((a->kind == META_ATT_NONE && !MUT(35)) || !a->who[0] || !a->ms)
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

static uint32_t vol_active_nstripe(const struct meta_volume *v)
{
    return v->redundancy == META_RED_EC ? (uint32_t)v->ec_k + v->ec_m : 0;
}

/* 1 iff all pre-conversion EC objects (stripe_version < conv.new_stripe_version) are proven gone under the volume's
 * current epoch by a non-overflow VOLUME_LIVE_OBJS report (§6.4 step 3, D29). Required before leaving REENCODE when
 * orig_redundancy == META_RED_EC, including grows/reshapes that have no retired positions to drop. */
static int reencode_done(const struct meta_volume *v)
{
    if (v->conv.orig_redundancy != META_RED_EC)
        return 1;
    if (!v->conv.new_stripe_version || v->live_epoch != v->epoch || v->live_overflow ||
        v->live_sv_at < v->conv.new_stripe_version)
        return 0;
    for (uint32_t i = 0; i < v->nlive_sv && i < META_LIVE_SV_MAX; i++)
        if (v->live_sv[i].stripe_version < v->conv.new_stripe_version && v->live_sv[i].count > 0)
            return 0;
    return 1;
}

static void vol_live_clear(struct meta_volume *v)
{
    v->live_epoch = 0;
    v->live_sv_at = 0;
    v->live_overflow = 0;
    v->nlive_sv = 0;
    memset(v->live_sv, 0, sizeof v->live_sv);
}

/* 1 if v's current-epoch live-object report proves zero live objects name any position in [from .. v->nstripe) (D28). */
static int stripe_drop_safe(const struct meta_volume *v, uint32_t from)
{
    if (v->live_epoch != v->epoch || v->live_overflow)
        return 0;
    for (uint32_t p = from; p < v->nstripe; p++) {
        uint64_t rv = v->stripe_pos[p].retired_ver;
        if (!rv || v->live_sv_at < rv)
            return 0;
        for (uint32_t i = 0; i < v->nlive_sv; i++)
            if (v->live_sv[i].count > 0 && v->live_sv[i].stripe_version < rv)
                return 0;
    }
    return 1;
}

/* The actor guard of USER_PUT and USER_DEL (the bytes of r from `at` on; none: 0): a change a user asked for through
 * a server whose copy may lag is checked there against the actor's rights as that copy holds them, so it applies only
 * while the actor still has the version it was checked at (STALE otherwise, the actor dropped included): a demotion
 * or drop of the actor committed before it always wins. FORMAT for a guard that does not decode. */
static int user_guard(const struct meta_state *m, const struct rd *r, size_t at)
{
    if (at >= r->len)
        return 0;
    struct rd g = {r->p + at, r->len - at, 0, 0};
    char actor[META_USER_NAME_MAX];
    getstr(&g, actor, sizeof actor);
    uint64_t av = get64(&g);
    if (g.bad || g.off != g.len || !actor[0])
        return META_ERR_FORMAT;
    if (MUT(39)) /* noactorguard: the guard is ignored (a demoted actor's change through a lagging server applies) */
        return 0;
    int32_t a = user_at(m, actor);
    return a >= 0 && m->users[a].version == av ? 0 : META_ERR_STALE;
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
        for (uint32_t i = 0; i < m->nmembers; i++)
            m->members[i].capability = member_compute_cap(&m->members[i], m->set.log_on_hdd);
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
        uint8_t reason = get8(r);
        uint64_t since_ms = get64(r);
        uint64_t op_clock_ms = get64(r);
        if (r->bad || st > META_DESTROYED || reason > META_DOWN_LAST)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, id);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state == META_DESTROYED && !MUT(58)) /* destroyedback */
            return META_ERR_STATE; /* destroyed is final (D2) */
        if (st == META_DESTROYED && mb->state != META_OUT)
            return META_ERR_STATE; /* destroyed only from OUT (D2) */
        if (mb->state == META_OUT) {
            if (st == META_UP) {
                if (MUT(55)) /* outfinal: OUT -> UP refused */
                    return META_ERR_STATE;
                if (inc <= mb->incarnation)
                    return META_ERR_STALE; /* OUT -> UP requires a new incarnation (D2) */
            } else if (st == META_DESTROYED) {
                if (inc < mb->incarnation)
                    return META_ERR_STALE;
            } else {
                return META_ERR_STATE;
            }
        } else if (inc < mb->incarnation) {
            return META_ERR_STALE; /* a decision about an earlier run of that server */
        }
        if (st == META_DOWN) {
            if (reason == META_DOWN_NONE || !since_ms || since_ms > op_clock_ms)
                return META_ERR_INVALID;
        } else {
            if (reason != META_DOWN_NONE || since_ms != 0)
                return META_ERR_INVALID;
        }
        int same_down = mb->state == META_DOWN && st == META_DOWN && inc == mb->incarnation;
        mb->state = st;
        mb->incarnation = inc;
        if (st == META_DOWN) {
            mb->down_reason = reason;
            if (MUT(59)) { /* downsinceapply: apply reads its own clock (members diverge) */
                struct timespec ts;
                clock_gettime(CLOCK_MONOTONIC, &ts);
                mb->down_since_ms = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
                mb->down_clock_ms = mb->down_since_ms;
            } else if (!same_down || since_ms < mb->down_since_ms) {
                /* repeated DOWN at the same incarnation (failover or reason escalation) never pushes down_since_ms forward */
                mb->down_since_ms = since_ms;
                mb->down_clock_ms = op_clock_ms;
            }
        } else {
            mb->down_since_ms = 0;
            mb->down_reason = META_DOWN_NONE;
            mb->down_clock_ms = 0;
            if (st == META_OUT || st == META_DESTROYED) {
                mb->hold_until_ms = 0;
                mb->hold_clock_ms = 0;
            }
            if (st == META_DESTROYED && !MUT(61)) /* destroyedkeepreplace */
                mb->replaced_by = 0;
        }
        return META_OK;
    }
    case META_OP_NODE_HOLD: {
        uint32_t id = get32(r);
        uint64_t until_ms = get64(r);
        uint64_t op_clock_ms = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, id);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state >= META_OUT)
            return META_ERR_STATE; /* no hold on OUT or DESTROYED members */
        if (until_ms == 0) {
            mb->hold_until_ms = 0;
            mb->hold_clock_ms = 0;
            return META_OK;
        }
        if (!op_clock_ms || until_ms < op_clock_ms || (!MUT(56) && until_ms - op_clock_ms > META_HOLD_MAX_MS))
            return META_ERR_INVALID; /* holdunbounded: skips the 24 h bound (D3, E12) */
        mb->hold_until_ms = until_ms;
        mb->hold_clock_ms = op_clock_ms;
        return META_OK;
    }
    case META_OP_NODE_REPLACE: {
        uint32_t id = get32(r);
        uint32_t target = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, id);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state == META_DESTROYED)
            return META_ERR_STATE;
        if (target == 0) {
            mb->replaced_by = 0;
            return META_OK;
        }
        if (target == id)
            return META_ERR_INVALID;
        const struct meta_member *nt = meta_member(m, target);
        if (!nt)
            return META_ERR_NOTFOUND;
        if (nt->state >= META_OUT)
            return META_ERR_STATE;
        /* Re-check at apply time so a concurrent volume placement cannot stall the forced replace target. */
        for (uint32_t i = 0; i < m->nvolumes; i++) {
            const struct meta_volume *v = &m->volumes[i];
            int old_holds = meta_replica(v, id) != NULL;
            if (!old_holds && v->redundancy == META_RED_EC) {
                for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
                    if (v->stripe[p] == id)
                        old_holds = 1;
            }
            if (!old_holds)
                continue;
            if (meta_replica(v, target) != NULL)
                return META_ERR_INVALID;
            if (v->redundancy == META_RED_EC) {
                for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
                    if (v->stripe[p] == target)
                        return META_ERR_INVALID;
            }
        }
        mb->replaced_by = target;
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
        if (mb->state >= META_OUT)
            return META_ERR_STATE;
        if (inc < mb->incarnation)
            return META_ERR_STALE; /* the flags of an earlier run */
        int healed = !(mb->flags & META_MF_CLAIM_OK) && (flags & META_MF_CLAIM_OK);
        mb->flags = flags;
        if (healed || MUT(28)) /* it says its claims work again (a 0 -> 1 of claim_ok): its CLAIM_FAILED bars end (F8) */
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
        for (uint32_t i = 0; i < m->nmembers; i++)
            m->members[i].capability = member_compute_cap(&m->members[i], m->set.log_on_hdd);
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
        uint8_t red = get8(r), ek = get8(r), em = get8(r);
        uint32_t ns = get32(r), stripe[META_STRIPE_MAX];
        if (ns > META_STRIPE_MAX)
            r->bad = 1;
        for (uint32_t i = 0; i < ns && !r->bad; i++)
            stripe[i] = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (number > META_VOLUME_MAX || !name[0] || n < 1 || min_isr < 1 || min_isr > n || vk != m->mode)
            return META_ERR_INVALID; /* a volume of the other kind is refused (M7) */
        if (red == META_RED_REPLICAS) {
            if (ek || em || ns)
                return META_ERR_INVALID;
        } else if (red == META_RED_EC) {
            /* the group is the proposer's pick (heartbeats are no state); apply only checks it: k + m distinct
             * members, none out. min_isr is at least m (E24, E35), or every replica where there are fewer */
            if (vk != META_VOL_REPLICATED || ek < 1 || ek > META_EC_K_MAX || em < 1 || em > META_EC_M_MAX ||
                ns != (uint32_t)ek + em || min_isr < (em < n ? em : n))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < ns; i++) {
                const struct meta_member *sm = meta_member(m, stripe[i]);
                if (!sm || sm->state >= META_OUT || !member_storage_capable(sm) || has_id(stripe, i, stripe[i]))
                    return META_ERR_INVALID;
            }
            if (MUT(53)) { /* stripeatapply: the group from this copy's own members, not the proposer's pick */
                uint32_t k2 = 0;
                for (uint32_t i = 0; i < m->nmembers && k2 < ns; i++)
                    if (m->members[i].state < META_OUT)
                        stripe[k2++] = m->members[i].id;
            }
        } else {
            return META_ERR_INVALID;
        }
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
            if (!mb || mb->state >= META_OUT || !(mb->flags & META_MF_VOLUMES_OK) ||
                (vk == META_VOL_REPLICATED && !member_log_ok(mb)) || has_id(nodes, i, nodes[i]))
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
        v->set_version = 1;
        v->redundancy = red;
        if (red == META_RED_EC) {
            v->ec_k = ek;
            v->ec_m = em;
            v->nstripe = ns;
            memcpy(v->stripe, stripe, ns * sizeof *stripe);
            for (uint32_t i = 0; i < ns; i++)
                v->stripe_pos[i] = (struct meta_stripe_pos){.state = META_STRIPE_WHOLE, .prev = 0, .set_ver = 1};
            v->stripe_version = 1;
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
        g.target = get32(r);
        g.min_end = get64(r);
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
        if (g.epoch != v->epoch || (g.isr_version != v->isr_version && !MUT(14)))
            return META_ERR_STALE;
        if (g.reason < 1 || g.reason > META_GR_LAST)
            return META_ERR_INVALID;
        /* a planned transfer's target (D14): replicated, PLANNED, an ISR member other than the leader it moves away
         * from; its end only with it. Which member leads is still promote_rule's (r4-transfer has it pick the target
         * or nothing) */
        if (g.target && (v->kind != META_VOL_REPLICATED || g.reason != META_GR_PLANNED ||
                         (!meta_volume_in_isr(v, g.target) && !MUT(47)) || (g.target == v->leader && !MUT(48))))
            return META_ERR_INVALID;
        if (!g.target && g.min_end)
            return META_ERR_INVALID;
        if (v->kind == META_VOL_REPLICATED) {
            if ((g.new_epoch != v->epoch + 1 && !(MUT(1) && g.new_epoch > v->epoch)) || v->nhist < 1 ||
                (g.start <= v->hist[v->nhist - 1].start && !MUT(13)))
                return META_ERR_INVALID;
            if (g.new_epoch >= META_EPOCH_LIMIT && !MUT(9))
                return META_ERR_RANGE; /* D10: the engine refuses it (replica_epoch_begin) */
            rc = promote(v, &g);
            if (rc == META_OK) {
                v->pos = g.pos;
                v->grant_target = g.target;
                v->grant_min_end = g.min_end;
            }
        } else {
            rc = grant_shared(m, v, &g);
        }
        if (rc == META_OK)
            touched[(*ntouched)++] = g.number;
        return rc;
    }
    case META_OP_ISR_CHANGE:
    case META_OP_REPLICA_ADD:
    case META_OP_REPLICA_REMOVE:
    case META_OP_REPLICA_RESET: {
        uint32_t number = get32(r);
        uint64_t epoch = get64(r), isr_version = 0, rid = 0, commit = 0, rids[META_REPLICAS_MAX];
        uint32_t node = 0, replaces = 0, ids[META_REPLICAS_MAX], n = 0;
        if (kind == META_OP_ISR_CHANGE) {
            isr_version = get64(r);
            n = get_ids(r, ids);
            for (uint32_t i = 0; i < n; i++)
                rids[i] = get64(r);
            commit = get64(r);
        } else {
            node = get32(r);
            if (kind != META_OP_REPLICA_REMOVE)
                rid = get64(r);
            if (kind == META_OP_REPLICA_ADD)
                replaces = get32(r);
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
            v->set_version += MUT(41); /* setverraiseisr: the ISR is not the replica set */
            if (commit)
                v->isr_commit = commit;
            for (uint32_t i = 0; i < v->nrep; i++)
                v->rep[i].unanswered = 0; /* the new leader has heard or dropped them (item 16) */
            for (uint32_t i = 0; i < n; i++) /* a learner that joined the ISR is a full replica */
                v->rep[replica_at(v, ids[i])].learner = 0;
        } else if (kind == META_OP_REPLICA_ADD) {
            const struct meta_member *mb = meta_member(m, node);
            if (!mb || mb->state >= META_OUT || !(mb->flags & META_MF_VOLUMES_OK) || (!shared && !member_log_ok(mb)))
                return META_ERR_INVALID;
            if (replica_at(v, node) >= 0)
                return META_ERR_EXISTS;
            if (v->nrep >= META_REPLICAS_MAX || (shared ? rid != 0 : rid == 0))
                return META_ERR_INVALID;
            for (uint32_t i = 0; i < v->nrep; i++)
                if (rid && v->rep[i].rid == rid)
                    return META_ERR_INVALID; /* one instance, one replica */
            if (replaces) { /* a move (D10): one per volume, of a member that is not itself moving in */
                if (shared)
                    return META_ERR_INVALID;
                if (v->conv.phase != META_CONV_NONE)
                    return META_ERR_STATE; /* no concurrent move while a redundancy conversion runs (D26) */
                int a = replica_at(v, replaces);
                if (a < 0)
                    return META_ERR_NOTFOUND;
                for (uint32_t i = 0; i < v->nrep; i++)
                    if (v->rep[i].replaces && !MUT(44)) /* twomoves */
                        return META_ERR_EXISTS;
                if (v->rep[a].replaces) /* implied by the one-move rule above; kept should that rule ever go */
                    return META_ERR_INVALID;
            }
            v->rep[v->nrep] =
                (struct meta_replica){.node = node, .learner = shared ? 0 : 1, .rid = rid, .replaces = replaces};
            v->nrep++;
            if (shared) { /* a candidate is in the ISR from the start */
                v->isr[v->nisr++] = node;
                sort_ids(v->isr, v->nisr);
                v->isr_version++;
            }
        } else if (kind == META_OP_REPLICA_REMOVE) {
            int i = replica_at(v, node);
            if (i < 0)
                return META_ERR_NOTFOUND;
            if ((node == v->leader && !MUT(11)) || (!shared && meta_volume_in_isr(v, node)))
                return META_ERR_INVALID; /* the ISR shrinks first: a move never lowers the in-sync copies */
            if (v->nrep <= v->min_isr)
                return META_ERR_INVALID; /* min_isr <= replicas: a volume never asks for copies it cannot have */
            if (v->conv.phase != META_CONV_NONE) {
                /* §6.4 step 5: never shrink log replicas before SHRINK, and never below target_replicas during SHRINK */
                uint32_t floor = v->conv.target_replicas;
                if (v->conv.phase < META_CONV_SHRINK && v->conv.orig_replicas > floor)
                    floor = v->conv.orig_replicas;
                if (v->nrep <= floor)
                    return META_ERR_STATE;
            }
            int b = -1; /* the learner moving in for `node`: this REMOVE ends its move */
            for (uint32_t j = 0; j < v->nrep; j++)
                if (v->rep[j].replaces == node)
                    b = (int)j;
            if (b >= 0 && !MUT(45)) /* replacesstays */
                v->rep[b].replaces = 0;
            if (b >= 0 && !MUT(46)) { /* moveslotlast: the learner would stay at its own slot */
                v->rep[i] = v->rep[b]; /* B takes A's slot: a move of rep[0] keeps the preferred leader's (D10) */
                i = b;
            }
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
        } else { /* REPLICA_RESET (D13): the member's directory is made again, as a new instance rebuilt by snapshot */
            int i = replica_at(v, node);
            if (shared)
                return META_ERR_INVALID;
            if (i < 0)
                return META_ERR_NOTFOUND;
            /* never a copy a promotion may need: neither the leader nor an ISR member (resetleader, resetinisr) */
            if (node == v->leader ? !MUT(43) : (meta_volume_in_isr(v, node) && !MUT(42)))
                return META_ERR_INVALID;
            if (!rid)
                return META_ERR_INVALID;
            for (uint32_t j = 0; j < v->nrep; j++)
                if (v->rep[j].rid == rid)
                    return META_ERR_INVALID; /* one instance, one replica; and a new one */
            v->rep[i].rid = rid;
            v->rep[i].learner = 1;
            v->rep[i].open_gen = 0;
            v->rep[i].open_nonce = 0;
            v->rep[i].unanswered = 0;
        }
        if (kind != META_OP_ISR_CHANGE)
            v->set_version += !MUT(40); /* setvernoraise */
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
        uint64_t rid = get64(r), gen = get64(r), nonce = get64(r);
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
        v->rep[i].open_nonce = nonce;
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
        int any = 0;
        for (uint32_t s = first; s < first + count; s++) {
            if (m->slot_home[s] != META_NONE && m->slot_home[s] != vol)
                return META_ERR_INVALID;
            if (m->slot_home[s] == META_NONE)
                any = 1;
        }
        if (!any)
            return META_OK;
        uint32_t *sh = meta_slot_home_mut(m);
        for (uint32_t s = first; s < first + count;) {
            if (sh[s] != META_NONE) {
                s++;
                continue;
            }
            uint32_t e = s;
            while (e < first + count && sh[e] == META_NONE)
                sh[e++] = vol;
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
        meta_slot_home_mut(m)[slot] = to;
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
        /* after a restore the leaders' logs may lack markers that were committed before it (E14, §5.7) */
        for (uint32_t i = 0; i < m->nvolumes; i++)
            if (m->volumes[i].redundancy == META_RED_EC || m->volumes[i].nstripe > 0)
                m->volumes[i].gc_suspended = 1;
        return META_OK;
    }
    case META_OP_STRIPE_SET: {
        uint32_t number = get32(r), pos = get32(r), node = get32(r);
        uint8_t st = get8(r);
        uint32_t prev = get32(r);
        uint64_t sv = get64(r);
        if (r->bad || st > META_STRIPE_FILLING)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->nstripe == 0 || pos >= v->nstripe)
            return META_ERR_INVALID;
        if (sv != v->stripe_version && !MUT(50)) /* stripeuncond */
            return META_ERR_STALE;
        if (node == 0) {
            /* Drop retired positions [pos .. v->nstripe - 1] (shrink nstripe to pos; D28). */
            if (st != META_STRIPE_WHOLE || prev != 0)
                return META_ERR_INVALID;
            if (v->conv.phase != META_CONV_NONE && v->conv.phase != META_CONV_DROP)
                return META_ERR_STATE;
            uint32_t active = vol_active_nstripe(v);
            if (pos < active)
                return META_ERR_INVALID; /* never drop an active geometry position */
            if (v->mixed_redundancy == META_RED_EC && pos < (uint32_t)v->mixed_k + v->mixed_m)
                return META_ERR_STATE; /* leftover cancelled-REENCODE objects still use these positions */
            if (!MUT(62) && !stripe_drop_safe(v, pos)) /* stripelive: skip live-object refusal */
                return META_ERR_STATE;
            for (uint32_t i = pos; i < v->nstripe; i++) {
                v->stripe[i] = 0;
                memset(&v->stripe_pos[i], 0, sizeof v->stripe_pos[i]);
            }
            v->nstripe = pos;
            v->stripe_version++;
            touched[(*ntouched)++] = number;
            return META_OK;
        }
        const struct meta_member *mb = meta_member(m, node);
        if (!mb || mb->state >= META_OUT || ((pos >= v->nstripe || node != v->stripe[pos]) && !member_storage_capable(mb)))
            return META_ERR_INVALID;
        for (uint32_t i = 0; i < v->nstripe; i++)
            if (i != pos && v->stripe[i] == node)
                return META_ERR_INVALID; /* one position per node (E9) */
        if (st == META_STRIPE_WHOLE && (prev != 0 || v->stripe_pos[pos].state == META_STRIPE_FILLING))
            return META_ERR_INVALID; /* a filling position goes whole only by STRIPE_WHOLE (D10) */
        if (prev && (!meta_member(m, prev) || prev == node))
            return META_ERR_INVALID;
        uint64_t rver = MUT(64) ? 0 : v->stripe_pos[pos].retired_ver; /* retireclear: drop retired_ver on replace */
        v->stripe[pos] = node;
        v->stripe_version++;
        v->stripe_pos[pos] =
            (struct meta_stripe_pos){.state = st, .prev = prev, .set_ver = v->stripe_version, .retired_ver = rver};
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_STRIPE_WHOLE: {
        uint32_t number = get32(r), pos = get32(r);
        uint64_t sv = get64(r);
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->nstripe == 0 || pos >= v->nstripe || v->stripe_pos[pos].state != META_STRIPE_FILLING)
            return META_ERR_INVALID;
        if (sv != v->stripe_version && !MUT(57)) /* wholeanyversion */
            return META_ERR_STALE;
        v->stripe_pos[pos].state = META_STRIPE_WHOLE;
        v->stripe_whole_gen++;
        v->stripe_whole_ver = sv;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_REDUNDANCY: {
        uint32_t number = get32(r);
        uint64_t epoch = get64(r);
        struct meta_redundancy_target t;
        t.redundancy = get8(r);
        t.ec_k = get8(r);
        t.ec_m = get8(r);
        t.replicas = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (epoch != v->epoch)
            return META_ERR_STALE;
        if (v->kind != META_VOL_REPLICATED)
            return META_ERR_INVALID;
        if (v->conv.phase != META_CONV_NONE)
            return META_ERR_EXISTS; /* a conversion already runs on this volume (D26) */
        if (v->mixed_redundancy != META_RED_REPLICAS)
            return META_ERR_STATE; /* leftover objects from a cancelled conversion have not finished clearing */
        for (uint32_t i = 0; i < v->nrep; i++)
            if (v->rep[i].replaces || v->rep[i].learner)
                return META_ERR_STATE; /* a move or replica set change runs on v */
        for (uint32_t p = 0; p < v->nstripe; p++)
            if (v->stripe_pos[p].state == META_STRIPE_FILLING)
                return META_ERR_STATE; /* a stripe position change runs on v */
        uint32_t trep = 0;
        if (t.redundancy == META_RED_EC) {
            if (t.ec_k < 1 || t.ec_k > META_EC_K_MAX || t.ec_m < 1 || t.ec_m > META_EC_M_MAX)
                return META_ERR_INVALID;
            trep = t.replicas ? t.replicas : (uint32_t)t.ec_m + 1;
            if (trep != (uint32_t)t.ec_m + 1)
                return META_ERR_INVALID;
            if (v->redundancy == META_RED_EC && v->ec_k == t.ec_k && v->ec_m == t.ec_m)
                return META_ERR_INVALID; /* already at this geometry (retired positions drop directly via STRIPE_SET) */
        } else if (t.redundancy == META_RED_REPLICAS) {
            if (t.ec_k || t.ec_m)
                return META_ERR_INVALID;
            trep = t.replicas ? t.replicas : m->set.replication;
            if (trep < 1 || trep > META_REPLICAS_MAX)
                return META_ERR_INVALID;
            if (v->redundancy == META_RED_REPLICAS && v->nrep == trep && v->nstripe == 0)
                return META_ERR_INVALID; /* already at this replication */
        } else {
            return META_ERR_INVALID;
        }
        v->conv = (struct meta_conv){
            .phase = META_CONV_GROW,
            .target_redundancy = t.redundancy,
            .target_k = t.ec_k,
            .target_m = t.ec_m,
            .target_replicas = trep,
            .new_stripe_version = 0,
            .orig_redundancy = v->redundancy,
            .orig_k = v->ec_k,
            .orig_m = v->ec_m,
            .orig_replicas = v->nrep,
            .orig_min_isr = v->min_isr,
        };
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_CONV_STEP: {
        uint32_t number = get32(r);
        uint8_t from_phase = get8(r), to_phase = get8(r);
        uint32_t ns = get32(r), stripe[META_STRIPE_MAX];
        if (ns > META_STRIPE_MAX)
            r->bad = 1;
        for (uint32_t i = 0; i < ns && !r->bad; i++)
            stripe[i] = get32(r);
        if (r->bad || from_phase < META_CONV_GROW || from_phase > META_CONV_LAST || to_phase > META_CONV_LAST)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->conv.phase == META_CONV_NONE)
            return META_ERR_STATE;
        if (v->conv.phase != from_phase)
            return META_ERR_STALE; /* conditional on current phase */
        uint8_t next = from_phase == META_CONV_SHRINK ? META_CONV_NONE : (uint8_t)(from_phase + 1);
        if (to_phase != next) {
            /* convskip: MUT(63) allows skipping forward across phases */
            if (!MUT(63) || (to_phase != META_CONV_NONE && to_phase <= from_phase))
                return META_ERR_INVALID;
        }
        if (from_phase != META_CONV_RECORD && ns != 0)
            return META_ERR_INVALID;
        if (from_phase == META_CONV_GROW) {
            if (v->nrep < v->conv.target_replicas || v->nisr < v->conv.target_replicas)
                return META_ERR_STATE; /* log group must reach target_replicas in ISR first (D26, §6.4) */
        } else if (from_phase == META_CONV_RECORD) {
            if (v->nrep < v->conv.target_replicas)
                return META_ERR_STATE;
            for (uint32_t i = 0; i < v->nstripe; i++)
                if (v->stripe_pos[i].state == META_STRIPE_FILLING)
                    return META_ERR_STATE; /* no stripe position fill may be in flight at RECORD */
            if (v->conv.target_redundancy == META_RED_EC) {
                uint32_t new_min = meta_volume_min_isr(META_RED_EC, v->conv.target_m, v->nrep, m->set.min_isr);
                if (v->nisr < new_min)
                    return META_ERR_STATE; /* ISR must meet the new min_isr before RECORD (§6.4 step 1) */
                uint32_t need = (uint32_t)v->conv.target_k + v->conv.target_m;
                for (uint32_t i = 0; i < need && i < v->nstripe; i++) {
                    const struct meta_member *sm = meta_member(m, v->stripe[i]);
                    if (!sm || sm->state >= META_OUT)
                        return META_ERR_INVALID; /* reused stripe position's member is OUT or DESTROYED */
                }
                if (need > v->nstripe) {
                    if (ns != need)
                        return META_ERR_INVALID;
                    for (uint32_t i = 0; i < v->nstripe; i++)
                        if (stripe[i] != v->stripe[i])
                            return META_ERR_INVALID; /* existing positions never renumbered (E9) */
                    for (uint32_t i = v->nstripe; i < need; i++) {
                        const struct meta_member *sm = meta_member(m, stripe[i]);
                        if (!sm || sm->state >= META_OUT || !member_storage_capable(sm) || has_id(stripe, i, stripe[i]))
                            return META_ERR_INVALID;
                    }
                } else if (ns != 0) {
                    if (ns != need)
                        return META_ERR_INVALID;
                    for (uint32_t i = 0; i < need; i++)
                        if (stripe[i] != v->stripe[i])
                            return META_ERR_INVALID;
                }
                v->redundancy = META_RED_EC;
                v->ec_k = v->conv.target_k;
                v->ec_m = v->conv.target_m;
                v->min_isr = new_min;
                v->stripe_version++;
                v->conv.new_stripe_version = v->stripe_version;
                if (need > v->nstripe) {
                    for (uint32_t i = v->nstripe; i < need; i++) {
                        v->stripe[i] = stripe[i];
                        v->stripe_pos[i] = (struct meta_stripe_pos){
                            .state = META_STRIPE_WHOLE, .prev = 0, .set_ver = v->stripe_version, .retired_ver = 0};
                    }
                    v->nstripe = need;
                }
                for (uint32_t i = 0; i < need; i++)
                    v->stripe_pos[i].retired_ver = 0;
                for (uint32_t i = need; i < v->nstripe; i++)
                    if (v->stripe_pos[i].retired_ver == 0)
                        v->stripe_pos[i].retired_ver = v->stripe_version;
                vol_live_clear(v);
            } else {
                if (ns != 0)
                    return META_ERR_INVALID;
                uint32_t new_min;
                if (v->conv.orig_redundancy == META_RED_REPLICAS) {
                    /* Replicas -> replicas: preserve operator's min_isr, capped at target_replicas and nrep. */
                    uint32_t cap = v->conv.target_replicas < v->nrep ? v->conv.target_replicas : v->nrep;
                    new_min = v->conv.orig_min_isr < cap ? v->conv.orig_min_isr : cap;
                    if (new_min < 1)
                        new_min = 1;
                } else {
                    new_min = meta_volume_min_isr(META_RED_REPLICAS, 0, v->nrep, m->set.min_isr);
                }
                if (v->nisr < new_min)
                    return META_ERR_STATE; /* ISR must meet the new min_isr before RECORD */
                v->redundancy = META_RED_REPLICAS;
                v->ec_k = 0;
                v->ec_m = 0;
                v->min_isr = new_min;
                if (v->nstripe > 0 || v->stripe_version > 0) {
                    v->stripe_version++;
                    v->conv.new_stripe_version = v->stripe_version;
                    for (uint32_t i = 0; i < v->nstripe; i++)
                        if (v->stripe_pos[i].retired_ver == 0)
                            v->stripe_pos[i].retired_ver = v->stripe_version;
                }
                vol_live_clear(v);
            }
        } else if (from_phase == META_CONV_REENCODE) {
            if (!reencode_done(v) && !MUT(66)) /* reencskip: leave REENCODE with old EC objects still live/unknown */
                return META_ERR_STATE;
        } else if (from_phase == META_CONV_DROP) {
            if (v->nstripe != vol_active_nstripe(v) && !MUT(63))
                return META_ERR_STATE; /* retired stripe positions must be dropped first */
        } else if (from_phase == META_CONV_SHRINK) {
            if (v->nrep > v->conv.target_replicas)
                return META_ERR_STATE; /* excess log replicas must be removed first */
        }
        if (to_phase == META_CONV_NONE)
            memset(&v->conv, 0, sizeof v->conv);
        else
            v->conv.phase = to_phase;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_CONV_CANCEL: {
        uint32_t number = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->conv.phase == META_CONV_NONE || v->conv.phase >= META_CONV_DROP)
            return META_ERR_STATE; /* --cancel only before DROP (D26) */
        if (v->conv.phase == META_CONV_REENCODE) {
            /* If target was EC, objects may have been written under that geometry in [new_stripe_version .. sv]:
             * keep a persisted mixed-geometry record so meta_vol_margins never overstates settled margin until a
             * current-epoch VOLUME_LIVE_OBJS proves 0 live objects remain in that range (D29). */
            if (v->redundancy == META_RED_EC) {
                v->mixed_redundancy = META_RED_EC;
                v->mixed_k = v->ec_k;
                v->mixed_m = v->ec_m;
                v->mixed_sv_lo = v->conv.new_stripe_version;
                v->mixed_sv_hi = v->stripe_version;
            }
            /* Revert geometry & operator's min_isr back to orig; never drop stripe positions without live-object check. */
            v->redundancy = v->conv.orig_redundancy;
            v->ec_k = v->conv.orig_k;
            v->ec_m = v->conv.orig_m;
            uint32_t restored = v->conv.orig_min_isr <= v->nrep ? v->conv.orig_min_isr : v->nrep;
            if (restored < 1)
                restored = 1;
            if (v->redundancy == META_RED_EC) {
                uint32_t ec_floor = v->ec_m < v->nrep ? v->ec_m : v->nrep;
                if (restored < ec_floor)
                    restored = ec_floor;
            }
            v->min_isr = restored;
            uint32_t active = vol_active_nstripe(v);
            if (v->nstripe > 0 || v->stripe_version > 0) {
                v->stripe_version++;
                for (uint32_t i = 0; i < active && i < v->nstripe; i++)
                    v->stripe_pos[i].retired_ver = 0;
                for (uint32_t i = active; i < v->nstripe; i++)
                    if (v->stripe_pos[i].retired_ver == 0)
                        v->stripe_pos[i].retired_ver = v->stripe_version;
            }
            vol_live_clear(v);
        }
        memset(&v->conv, 0, sizeof v->conv);
        v->conv_cancel_gen++;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_VOLUME_LIVE_OBJS: {
        uint32_t number = get32(r);
        uint64_t epoch = get64(r), sv = get64(r);
        uint8_t overflow = get8(r);
        uint32_t n = get32(r);
        struct meta_live_sv items[META_LIVE_SV_MAX];
        if (overflow > 1 || n > META_LIVE_SV_MAX)
            r->bad = 1;
        for (uint32_t i = 0; i < n && !r->bad; i++) {
            items[i].stripe_version = get64(r);
            items[i].count = get64(r);
        }
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        int rc = vol_get(m, number, touched, *ntouched, &v);
        if (rc)
            return rc;
        if (v->kind != META_VOL_REPLICATED)
            return META_ERR_INVALID;
        if ((epoch != v->epoch && !MUT(65)) || sv != v->stripe_version) /* livestale: accept stale epoch */
            return META_ERR_STALE;
        for (uint32_t i = 0; i < n; i++)
            if (items[i].stripe_version < 1 || items[i].stripe_version > v->stripe_version ||
                (i > 0 && items[i].stripe_version <= items[i - 1].stripe_version))
                return META_ERR_INVALID;
        v->live_epoch = epoch;
        v->live_sv_at = sv;
        v->live_overflow = overflow;
        v->nlive_sv = n;
        copy_n(v->live_sv, items, n * sizeof items[0]);
        if (n < META_LIVE_SV_MAX)
            memset(v->live_sv + n, 0, (META_LIVE_SV_MAX - n) * sizeof v->live_sv[0]);
        /* Clear leftover cancelled-REENCODE mixed record once a non-overflow current-epoch report proves 0 live
         * objects remain in [mixed_sv_lo .. mixed_sv_hi]. */
        if (v->mixed_redundancy == META_RED_EC && !overflow && v->live_epoch == v->epoch &&
            v->live_sv_at >= v->mixed_sv_hi) {
            int still = 0;
            for (uint32_t i = 0; i < n; i++)
                if (items[i].count > 0 && items[i].stripe_version >= v->mixed_sv_lo &&
                    items[i].stripe_version <= v->mixed_sv_hi)
                    still = 1;
            if (!still) {
                v->mixed_redundancy = META_RED_REPLICAS;
                v->mixed_k = 0;
                v->mixed_m = 0;
                v->mixed_sv_lo = 0;
                v->mixed_sv_hi = 0;
            }
        }
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_GC_SUSPEND:
    case META_OP_GC_RESUME: {
        uint32_t number = get32(r);
        if (r->bad)
            return META_ERR_FORMAT;
        struct meta_volume *v;
        if (kind == META_OP_GC_SUSPEND) { /* not counted as a touch (the head comment) */
            int32_t at = volume_at(m, number);
            if (at < 0)
                return META_ERR_NOTFOUND;
            v = &m->volumes[at];
        } else {
            int rc = vol_get(m, number, touched, *ntouched, &v);
            if (rc)
                return rc;
        }
        if (v->redundancy != META_RED_EC && v->nstripe == 0)
            return META_ERR_INVALID;
        if (kind == META_OP_GC_SUSPEND) {
            v->gc_suspended = 1;
            return META_OK;
        }
        v->gc_suspended = 0;
        v->gc_gen++;
        touched[(*ntouched)++] = number;
        return META_OK;
    }
    case META_OP_NAME_GROUP:
        r->off = r->len; /* a number kept for S4's prefix groups: nothing applies it yet */
        return META_ERR_INVALID;
    case META_OP_USER_PUT: {
        char name[META_USER_NAME_MAX];
        getstr(r, name, sizeof name);
        uint64_t version = get64(r);
        uint32_t len = get32(r);
        if (r->bad || len > META_USER_BLOB_MAX || len > r->len - r->off)
            return META_ERR_FORMAT;
        int guard = user_guard(m, r, r->off + len);
        if (guard == META_ERR_FORMAT)
            return guard;
        if (!name[0] || len < META_USER_UID_LEN) {
            r->off = r->len;
            return META_ERR_INVALID;
        }
        int32_t at = user_at(m, name);
        if (!MUT(36)) { /* userstaleok: an unconditional put (a passwd racing a revoke undoes the revoke) */
            int rc = at >= 0 ? (version == 0 ? META_ERR_EXISTS : version != m->users[at].version ? META_ERR_STALE : 0)
                             : (version != 0 ? META_ERR_NOTFOUND : 0);
            if (rc || guard) {
                r->off = r->len;
                return rc ? rc : guard;
            }
        }
        /* the budget (nousersbudget: none, and a snapshot passes a peer frame: node add and catch-up stop for good) */
        uint64_t total = meta_users_bytes(m) + len + strlen(name) + 1 -
                         (at >= 0 ? m->users[at].len + strlen(name) + 1 : 0);
        if ((at < 0 && m->max_uid == UINT32_MAX) || (total > META_USERS_BYTES_MAX && !MUT(38))) {
            r->off += len;
            return META_ERR_RANGE;
        }
        uint8_t *blob = ublob_new(len);
        get(r, blob, len);
        uint32_t uid;
        if (at >= 0) {
            uid = m->users[at].uid;
            ublob_unref(m->users[at].blob);
        } else {
            /* uidfromproposer: the proposer's uid (two proposers reading one copy pick the same) */
            uid = MUT(37) && get_le32(blob) ? get_le32(blob) : m->max_uid + 1;
            if (uid > m->max_uid)
                m->max_uid = uid;
            uint32_t pos = (uint32_t)(-1 - at);
            m->users = grow(m->users, &m->users_cap, m->nusers + 1, sizeof *m->users);
            memmove(&m->users[pos + 1], &m->users[pos], (m->nusers - pos) * sizeof *m->users);
            m->nusers++;
            at = (int32_t)pos;
            memcpy(m->users[at].name, name, sizeof name);
        }
        put_le32(blob, uid);
        m->users[at].uid = uid;
        m->users[at].version = m->applied; /* meta_apply sets the scratch copy's applied to this entry's index */
        m->users[at].blob = blob;
        m->users[at].len = len;
        r->off = r->len; /* past the guard */
        return META_OK;
    }
    case META_OP_USER_DEL: {
        char name[META_USER_NAME_MAX];
        getstr(r, name, sizeof name);
        if (r->bad)
            return META_ERR_FORMAT;
        int guard = user_guard(m, r, r->off);
        if (guard == META_ERR_FORMAT)
            return guard;
        r->off = r->len;
        int32_t at = user_at(m, name);
        if (at < 0)
            return META_ERR_NOTFOUND;
        if (guard)
            return guard;
        ublob_unref(m->users[at].blob);
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
        if (mb->state == META_DESTROYED)
            return META_ERR_STATE;
        if (mb->state == META_OUT ? inc <= mb->incarnation : inc < mb->incarnation)
            return META_ERR_STALE; /* addresses of an earlier run (OUT needs a new incarnation to return) */
        if (strcmp(mb->peer, peer) != 0 && addr_taken(m, peer))
            return META_ERR_EXISTS;
        snprintf(mb->addr, sizeof mb->addr, "%s", addr);
        snprintf(mb->peer, sizeof mb->peer, "%s", peer);
        return META_OK;
    }
    case META_OP_STORE_ADD: {
        uint32_t node = get32(r);
        struct meta_disk d;
        get_disk(r, &d);
        if (r->bad || d.state > META_STORE_STATE_LAST || (d.uses & ~META_DISK_USE_ALL))
            return META_ERR_FORMAT;
        if (!disk_fields_valid(&d) || d.state != META_STORE_UP)
            return META_ERR_INVALID;
        int32_t at = member_at(m, node);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state >= META_OUT)
            return META_ERR_STATE;
        if (!MUT(67) && store_id_known(m, d.store_id)) /* storeaddtwice: a disk moved between members is not new */
            return META_ERR_EXISTS;
        if (mb->ndisks >= META_DISKS_MAX)
            return META_ERR_RANGE;
        mb->disks[mb->ndisks++] = d;
        mb->capability = member_compute_cap(mb, m->set.log_on_hdd);
        return META_OK;
    }
    case META_OP_STORE_CLASS: {
        uint32_t node = get32(r);
        uint8_t store_id[16];
        get(r, store_id, 16);
        uint8_t cls = get8(r);
        if (r->bad)
            return META_ERR_FORMAT;
        if (cls < META_DISK_NVME || cls > META_DISK_CLASS_LAST)
            return META_ERR_INVALID;
        int32_t at = member_at(m, node);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state == META_DESTROYED)
            return META_ERR_STATE;
        int di = member_disk_at(mb, store_id);
        if (di < 0)
            return META_ERR_NOTFOUND;
        if (mb->disks[di].state == META_STORE_DESTROYED)
            return META_ERR_STATE;
        mb->disks[di].disk_class = cls;
        mb->capability = member_compute_cap(mb, m->set.log_on_hdd);
        return META_OK;
    }
    case META_OP_STORE_STATE: {
        uint32_t node = get32(r);
        uint8_t store_id[16];
        get(r, store_id, 16);
        uint8_t st = get8(r);
        if (r->bad || st > META_STORE_STATE_LAST)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, node);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state == META_DESTROYED)
            return META_ERR_STATE;
        int di = member_disk_at(mb, store_id);
        if (di < 0)
            return META_ERR_NOTFOUND;
        struct meta_disk *d = &mb->disks[di];
        if (d->state == META_STORE_DESTROYED && !(MUT(70) && st == META_STORE_UP)) /* storedestroyedback */
            return META_ERR_STATE; /* DESTROYED is final */
        if (st == META_STORE_OUT && d->state != META_STORE_DOWN)
            return META_ERR_STATE; /* OUT only from DOWN */
        if (st == META_STORE_DESTROYED && d->state != META_STORE_OUT)
            return META_ERR_STATE; /* DESTROYED only from OUT (matches MEMBER_STATE) */
        if (d->state == META_STORE_OUT && st != META_STORE_UP && st != META_STORE_DESTROYED)
            return META_ERR_STATE; /* OUT may only return to UP or become DESTROYED (no OUT -> DOWN) */
        d->state = st;
        mb->capability = member_compute_cap(mb, m->set.log_on_hdd);
        return META_OK;
    }
    case META_OP_MEMBER_ROLE: {
        uint32_t node = get32(r);
        uint8_t role = get8(r);
        if (r->bad || role > META_ROLE_LAST)
            return META_ERR_FORMAT;
        int32_t at = member_at(m, node);
        if (at < 0)
            return META_ERR_NOTFOUND;
        struct meta_member *mb = &m->members[at];
        if (mb->state >= META_OUT)
            return META_ERR_STATE;
        if (mb->ndisks > 0 && !MUT(69)) { /* rolewiden: role wider than UP disks' capability */
            if (role == META_ROLE_COMPUTE && !(mb->capability & META_CAP_COMPUTE))
                return META_ERR_INVALID;
            if (role == META_ROLE_STORAGE && !(mb->capability & META_CAP_STORAGE))
                return META_ERR_INVALID;
        }
        mb->role = role;
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

int meta_entry_older(const void *data, size_t len)
{
    struct rd r = {data, len, 0, 0};
    uint32_t magic = get32(&r);
    return (magic == ENTRY_MAGIC_V1 || magic == ENTRY_MAGIC_V2 || magic == ENTRY_MAGIC_V3 ||
            magic == ENTRY_MAGIC_V4 || magic == ENTRY_MAGIC_V5 || magic == ENTRY_MAGIC_V6) &&
           !r.bad;
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
        put64(w, v->rep[j].open_nonce);
        put32(w, v->rep[j].replaces);
    }
    put64(w, v->set_version);
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
    put32(w, v->grant_target);
    put64(w, v->grant_min_end);
    put8(w, v->redundancy);
    put8(w, v->ec_k);
    put8(w, v->ec_m);
    put32(w, v->nstripe);
    for (uint32_t j = 0; j < v->nstripe; j++) {
        put32(w, v->stripe[j]);
        put8(w, v->stripe_pos[j].state);
        put32(w, v->stripe_pos[j].prev);
        put64(w, v->stripe_pos[j].set_ver);
        put64(w, v->stripe_pos[j].retired_ver);
    }
    put64(w, v->stripe_version);
    put64(w, v->stripe_whole_gen);
    put64(w, v->stripe_whole_ver);
    put8(w, v->gc_suspended);
    put64(w, v->gc_gen);
    put8(w, v->conv.phase);
    put8(w, v->conv.target_redundancy);
    put8(w, v->conv.target_k);
    put8(w, v->conv.target_m);
    put32(w, v->conv.target_replicas);
    put64(w, v->conv.new_stripe_version);
    put8(w, v->conv.orig_redundancy);
    put8(w, v->conv.orig_k);
    put8(w, v->conv.orig_m);
    put32(w, v->conv.orig_replicas);
    put32(w, v->conv.orig_min_isr);
    put64(w, v->conv_cancel_gen);
    put8(w, v->mixed_redundancy);
    put8(w, v->mixed_k);
    put8(w, v->mixed_m);
    put64(w, v->mixed_sv_lo);
    put64(w, v->mixed_sv_hi);
    put64(w, v->live_epoch);
    put64(w, v->live_sv_at);
    put8(w, v->live_overflow);
    put32(w, v->nlive_sv);
    for (uint32_t j = 0; j < v->nlive_sv; j++) {
        put64(w, v->live_sv[j].stripe_version);
        put64(w, v->live_sv[j].count);
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
    put32(w, m->max_uid);
    put32(w, m->nusers);
    for (uint32_t i = 0; i < m->nusers; i++) {
        putstr(w, m->users[i].name, META_USER_NAME_MAX);
        put32(w, m->users[i].uid);
        put64(w, m->users[i].version);
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
        v->rep[j].open_nonce = get64(r);
        v->rep[j].replaces = get32(r);
        if (v->rep[j].learner > 1 || v->rep[j].unanswered > 1 || v->rep[j].claim_blocked > 1)
            return -1;
    }
    v->set_version = get64(r);
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
    v->grant_target = get32(r);
    v->grant_min_end = get64(r);
    v->redundancy = get8(r);
    v->ec_k = get8(r);
    v->ec_m = get8(r);
    v->nstripe = get32(r);
    if (v->nstripe > META_STRIPE_MAX || v->redundancy > META_RED_EC)
        return -1;
    for (uint32_t j = 0; j < v->nstripe; j++) {
        v->stripe[j] = get32(r);
        v->stripe_pos[j].state = get8(r);
        v->stripe_pos[j].prev = get32(r);
        v->stripe_pos[j].set_ver = get64(r);
        v->stripe_pos[j].retired_ver = get64(r);
        if (v->stripe_pos[j].state > META_STRIPE_FILLING)
            return -1;
    }
    v->stripe_version = get64(r);
    v->stripe_whole_gen = get64(r);
    v->stripe_whole_ver = get64(r);
    v->gc_suspended = get8(r);
    v->gc_gen = get64(r);
    v->conv.phase = get8(r);
    v->conv.target_redundancy = get8(r);
    v->conv.target_k = get8(r);
    v->conv.target_m = get8(r);
    v->conv.target_replicas = get32(r);
    v->conv.new_stripe_version = get64(r);
    v->conv.orig_redundancy = get8(r);
    v->conv.orig_k = get8(r);
    v->conv.orig_m = get8(r);
    v->conv.orig_replicas = get32(r);
    v->conv.orig_min_isr = get32(r);
    v->conv_cancel_gen = get64(r);
    v->mixed_redundancy = get8(r);
    v->mixed_k = get8(r);
    v->mixed_m = get8(r);
    v->mixed_sv_lo = get64(r);
    v->mixed_sv_hi = get64(r);
    v->live_epoch = get64(r);
    v->live_sv_at = get64(r);
    v->live_overflow = get8(r);
    v->nlive_sv = get32(r);
    if (r->bad || v->conv.phase > META_CONV_LAST || v->live_overflow > 1 || v->nlive_sv > META_LIVE_SV_MAX)
        return -1;
    for (uint32_t j = 0; j < v->nlive_sv; j++) {
        v->live_sv[j].stripe_version = get64(r);
        v->live_sv[j].count = get64(r);
        if (v->live_sv[j].stripe_version < 1 || v->live_sv[j].stripe_version > v->stripe_version ||
            (j > 0 && v->live_sv[j].stripe_version <= v->live_sv[j - 1].stripe_version))
            return -1;
    }
    if (v->live_epoch == 0 ? (v->live_sv_at || v->live_overflow || v->nlive_sv)
                           : (v->live_epoch > v->epoch || v->live_sv_at > v->stripe_version))
        return -1;
    if (v->mixed_redundancy == META_RED_REPLICAS) {
        if (v->mixed_k || v->mixed_m || v->mixed_sv_lo || v->mixed_sv_hi)
            return -1;
    } else if (v->mixed_redundancy == META_RED_EC) {
        if (v->kind != META_VOL_REPLICATED || v->conv.phase != META_CONV_NONE || v->mixed_k < 1 ||
            v->mixed_k > META_EC_K_MAX || v->mixed_m < 1 || v->mixed_m > META_EC_M_MAX ||
            (uint32_t)v->mixed_k + v->mixed_m > v->nstripe || v->mixed_sv_lo < 1 ||
            v->mixed_sv_lo > v->mixed_sv_hi || v->mixed_sv_hi >= v->stripe_version)
            return -1;
    } else {
        return -1;
    }
    if (v->conv.phase == META_CONV_NONE) {
        if (v->conv.target_redundancy || v->conv.target_k || v->conv.target_m || v->conv.target_replicas ||
            v->conv.new_stripe_version || v->conv.orig_redundancy || v->conv.orig_k || v->conv.orig_m ||
            v->conv.orig_replicas || v->conv.orig_min_isr)
            return -1;
    } else {
        if (v->kind != META_VOL_REPLICATED || v->conv.target_redundancy > META_RED_EC ||
            v->conv.orig_redundancy > META_RED_EC || v->conv.target_replicas < 1 ||
            v->conv.target_replicas > META_REPLICAS_MAX || v->conv.orig_replicas < 1 ||
            v->conv.orig_replicas > META_REPLICAS_MAX || v->conv.orig_min_isr < 1 ||
            v->conv.orig_min_isr > v->conv.orig_replicas)
            return -1;
        if (v->conv.phase < META_CONV_REENCODE && v->conv.new_stripe_version != 0)
            return -1;
        if (v->conv.new_stripe_version > v->stripe_version)
            return -1;
    }
    if (v->redundancy == META_RED_EC || v->nstripe > 0) {
        if (!v->stripe_version)
            return -1;
        for (uint32_t j = 0; j < v->nstripe; j++)
            if (v->stripe_pos[j].set_ver < 1 || v->stripe_pos[j].set_ver > v->stripe_version ||
                v->stripe_pos[j].retired_ver > v->stripe_version)
                return -1;
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
        struct meta_member *mb = &m->members[i];
        get_member(r, mb);
        if (i && mb->id <= m->members[i - 1].id)
            r->bad = 1;
        if (mb->state == META_DOWN ? (!mb->down_since_ms || !mb->down_reason || mb->down_since_ms > mb->down_clock_ms)
                                   : (mb->down_since_ms || mb->down_reason != META_DOWN_NONE || mb->down_clock_ms))
            r->bad = 1;
        if (mb->hold_until_ms ? (mb->state >= META_OUT || !mb->hold_clock_ms || mb->hold_until_ms < mb->hold_clock_ms ||
                                 mb->hold_until_ms - mb->hold_clock_ms > META_HOLD_MAX_MS)
                              : (mb->hold_clock_ms != 0))
            r->bad = 1;
        m->nmembers = i + 1;
    }
    for (uint32_t i = 0; i < m->nmembers && !r->bad; i++) {
        const struct meta_member *mb = &m->members[i];
        if (mb->replaced_by &&
            (mb->replaced_by == mb->id || mb->state == META_DESTROYED || !meta_member(m, mb->replaced_by)))
            r->bad = 1;
        if (mb->capability != member_compute_cap(mb, m->set.log_on_hdd))
            r->bad = 1;
    }
    if (!r->bad && !stores_unique(m))
        r->bad = 1;
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
        if (v->redundancy == META_RED_EC || v->nstripe > 0)
            for (uint32_t j = 0; j < v->nstripe; j++)
                if (v->stripe_pos[j].prev &&
                    (!meta_member(m, v->stripe_pos[j].prev) || v->stripe_pos[j].prev == v->stripe[j]))
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
    uint32_t *sh = meta_slot_home_mut(m);
    for (uint32_t i = 0; i < nruns && !r->bad; i++) {
        uint32_t cnt = get32(r), home = get32(r);
        if (cnt > META_SLOTS - s)
            return -1;
        for (uint32_t j = 0; j < cnt; j++)
            sh[s++] = home;
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
    m->max_uid = get32(r);
    n = get32(r);
    if (r->bad || n > (r->len - r->off) / 22)
        return -1;
    m->users = grow(m->users, &m->users_cap, n + 1, sizeof *m->users);
    for (uint32_t i = 0; i < n && !r->bad; i++) {
        struct meta_user *u = &m->users[i];
        getstr(r, u->name, META_USER_NAME_MAX);
        u->uid = get32(r);
        u->version = get64(r);
        u->len = get32(r);
        if (r->bad || u->len > META_USER_BLOB_MAX || u->len < META_USER_UID_LEN || u->len > r->len - r->off)
            return -1;
        u->blob = ublob_new(u->len);
        m->nusers = i + 1;
        get(r, u->blob, u->len);
        if (!u->name[0] || (i && strcmp(u->name, m->users[i - 1].name) <= 0) || !u->uid || u->uid > m->max_uid ||
            get_le32(u->blob) != u->uid || u->version > m->applied)
            r->bad = 1;
    }
    if (!r->bad && users_uids_unique(m) != 0)
        return -1;
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

uint32_t meta_snapshot_version(const void *buf, size_t len)
{
    const uint8_t *p = buf;
    uint32_t crc, magic, version;
    if (!p || len < 28)
        return 0;
    memcpy(&crc, p + len - 4, 4);
    if (iso_crc32c(0, p, len - 4) != crc)
        return 0;
    struct rd r = {p, len - 4, 0, 0};
    magic = get32(&r);
    version = get32(&r);
    return magic == SNAP_MAGIC ? version : 0;
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

/* rep[] of a and b hold the same nodes with the same instances in the same order */
static int same_set(const struct meta_volume *a, const struct meta_volume *b)
{
    if (a->nrep != b->nrep)
        return 0;
    for (uint32_t j = 0; j < a->nrep; j++)
        if (a->rep[j].node != b->rep[j].node || a->rep[j].rid != b->rep[j].rid)
            return 0;
    return 1;
}

/* b's replica set is a's after exactly one REPLICA_ADD, REPLICA_REMOVE or REPLICA_RESET: an ADD appends; a REMOVE
 * keeps the order of the rest, and one that ends a move puts the learner into the removed member's slot; a RESET gives
 * one member a new instance, a learner with open generation 0 (its server opens it in a later entry). 0, or -1 with
 * a sentence. */
static int one_set_op(const struct meta_volume *a, const struct meta_volume *b, char *why, size_t n)
{
    unsigned va = a->number;
    if (b->nrep == a->nrep + 1) {
        for (uint32_t j = 0; j < a->nrep; j++)
            if (a->rep[j].node != b->rep[j].node || a->rep[j].rid != b->rep[j].rid)
                FAIL("volume %u: a REPLICA_ADD changed replica %u's slot or instance", va, a->rep[j].node);
        return 0;
    }
    if (b->nrep + 1 == a->nrep) {
        int gone = -1, mover = -1;
        for (uint32_t j = 0; j < a->nrep; j++)
            if (!meta_replica(b, a->rep[j].node))
                gone = (int)j;
        if (gone < 0)
            FAIL("volume %u: one replica fewer, but every node is still there", va);
        for (uint32_t j = 0; j < a->nrep; j++)
            if (a->rep[j].replaces == a->rep[gone].node)
                mover = (int)j;
        struct meta_replica e[META_REPLICAS_MAX];
        uint32_t ne = 0;
        for (uint32_t j = 0; j < a->nrep; j++) {
            if ((int)j == mover)
                continue;
            if ((int)j == gone) {
                if (mover >= 0)
                    e[ne++] = a->rep[mover];
                continue;
            }
            e[ne++] = a->rep[j];
        }
        for (uint32_t j = 0; j < ne; j++)
            if (e[j].node != b->rep[j].node || e[j].rid != b->rep[j].rid)
                FAIL("volume %u: after the REMOVE of %u, slot %u holds %u, not %u%s", va, a->rep[gone].node, j,
                     b->rep[j].node, e[j].node, mover >= 0 ? " (the learner of its move takes its slot)" : "");
        return 0;
    }
    int changed = -1;
    for (uint32_t j = 0; j < a->nrep; j++) {
        if (a->rep[j].node != b->rep[j].node)
            FAIL("volume %u: the replica set version rose by one, but slot %u went from %u to %u", va, j,
                 a->rep[j].node, b->rep[j].node);
        if (a->rep[j].rid != b->rep[j].rid) {
            if (changed >= 0)
                FAIL("volume %u: one REPLICA_RESET gave two members new instances", va);
            changed = (int)j;
        }
    }
    if (changed < 0)
        FAIL("volume %u: the replica set version rose by one, but the replica set is the same", va);
    const struct meta_replica *rr = &b->rep[changed];
    if (!rr->learner || rr->open_gen || rr->open_nonce)
        FAIL("volume %u: member %u was reset but is not a learner of open generation 0", va, rr->node);
    return 0;
}

static int check_volume_step(const struct meta_volume *a, const struct meta_volume *b, char *why, size_t n)
{
    unsigned va = a->number;
    if (a->kind != b->kind)
        FAIL("volume %u: its kind changed", va);
    if (b->conv_cancel_gen < a->conv_cancel_gen)
        FAIL("volume %u: conv_cancel_gen went back", va);
    if (b->conv.phase > META_CONV_LAST)
        FAIL("volume %u: invalid conversion phase %u", va, b->conv.phase);
    int conv_started = a->conv.phase == META_CONV_NONE && b->conv.phase != META_CONV_NONE;
    int conv_ended = a->conv.phase != META_CONV_NONE && b->conv.phase == META_CONV_NONE;
    int conv_cancelled = conv_ended && b->conv_cancel_gen == a->conv_cancel_gen + 1;
    int conv_recorded = a->conv.phase == META_CONV_RECORD && b->conv.phase == META_CONV_REENCODE;
    int conv_reenc_cancel = a->conv.phase == META_CONV_REENCODE && conv_cancelled;
    if (b->conv_cancel_gen > a->conv_cancel_gen && !conv_cancelled)
        FAIL("volume %u: conv_cancel_gen rose without cancelling a conversion", va);
    if (conv_started) {
        if (b->conv.phase != META_CONV_GROW || b->conv.new_stripe_version != 0 ||
            b->conv.orig_redundancy != a->redundancy || b->conv.orig_k != a->ec_k || b->conv.orig_m != a->ec_m ||
            b->conv.orig_replicas != a->nrep || b->conv.orig_min_isr != a->min_isr ||
            b->conv.target_replicas < 1 || b->conv.target_replicas > META_REPLICAS_MAX ||
            a->mixed_redundancy != META_RED_REPLICAS)
            FAIL("volume %u: conversion started in phase %s (new_stripe_version %llu)", va,
                 meta_conv_phase_name(b->conv.phase), (unsigned long long)b->conv.new_stripe_version);
    } else if (conv_ended) {
        if (conv_cancelled) {
            if (a->conv.phase >= META_CONV_DROP)
                FAIL("volume %u: conversion cancelled in phase %s (only allowed before DROP)", va,
                     meta_conv_phase_name(a->conv.phase));
        } else if (a->conv.phase != META_CONV_SHRINK) {
            FAIL("volume %u: conversion ended directly from phase %s without completing SHRINK or cancelling", va,
                 meta_conv_phase_name(a->conv.phase));
        }
    } else if (a->conv.phase != META_CONV_NONE && b->conv.phase != a->conv.phase) {
        if (b->conv.phase != a->conv.phase + 1)
            FAIL("volume %u: conversion skipped phase (%s -> %s)", va, meta_conv_phase_name(a->conv.phase),
                 meta_conv_phase_name(b->conv.phase));
        if (b->conv.target_redundancy != a->conv.target_redundancy || b->conv.target_k != a->conv.target_k ||
            b->conv.target_m != a->conv.target_m || b->conv.target_replicas != a->conv.target_replicas ||
            b->conv.orig_redundancy != a->conv.orig_redundancy || b->conv.orig_k != a->conv.orig_k ||
            b->conv.orig_m != a->conv.orig_m || b->conv.orig_replicas != a->conv.orig_replicas ||
            b->conv.orig_min_isr != a->conv.orig_min_isr)
            FAIL("volume %u: conversion target or orig changed mid-conversion", va);
        if (a->conv.phase == META_CONV_GROW &&
            (a->nrep < a->conv.target_replicas || a->nisr < a->conv.target_replicas))
            FAIL("volume %u: left GROW with nrep %u, nisr %u (need %u)", va, a->nrep, a->nisr,
                 a->conv.target_replicas);
        if (a->conv.phase == META_CONV_REENCODE && !reencode_done(a))
            FAIL("volume %u: left REENCODE with pre-conversion EC objects live or unknown (sv < %llu)", va,
                 (unsigned long long)a->conv.new_stripe_version);
        if (a->conv.phase == META_CONV_DROP && a->nstripe != vol_active_nstripe(a))
            FAIL("volume %u: left DROP with %u stripe positions (active %u)", va, a->nstripe, vol_active_nstripe(a));
    }
    int red_chg = a->redundancy != b->redundancy || a->ec_k != b->ec_k || a->ec_m != b->ec_m;
    if (red_chg) {
        if (conv_recorded) {
            if (b->redundancy != a->conv.target_redundancy || b->ec_k != a->conv.target_k ||
                b->ec_m != a->conv.target_m)
                FAIL("volume %u: RECORD did not apply conversion target redundancy", va);
        } else if (conv_reenc_cancel) {
            if (b->redundancy != a->conv.orig_redundancy || b->ec_k != a->conv.orig_k || b->ec_m != a->conv.orig_m)
                FAIL("volume %u: REENCODE cancel did not restore original redundancy", va);
        } else {
            FAIL("volume %u: its redundancy changed outside RECORD or REENCODE cancel", va);
        }
    }
    if (conv_recorded) {
        if (b->redundancy == META_RED_EC) {
            uint32_t want = b->ec_m < b->nrep ? b->ec_m : b->nrep;
            if (b->min_isr != want)
                FAIL("volume %u: min_isr %u after RECORD, expected %u (E24)", va, b->min_isr, want);
        } else if (a->conv.orig_redundancy == META_RED_REPLICAS) {
            uint32_t cap = a->conv.target_replicas < b->nrep ? a->conv.target_replicas : b->nrep;
            uint32_t want = a->conv.orig_min_isr < cap ? a->conv.orig_min_isr : cap;
            if (want < 1)
                want = 1;
            if (b->min_isr != want)
                FAIL("volume %u: min_isr %u after replicas RECORD, expected %u", va, b->min_isr, want);
        }
    } else if (conv_reenc_cancel) {
        uint32_t want = a->conv.orig_min_isr <= b->nrep ? a->conv.orig_min_isr : b->nrep;
        if (want < 1)
            want = 1;
        if (b->redundancy == META_RED_EC) {
            uint32_t ec_floor = b->ec_m < b->nrep ? b->ec_m : b->nrep;
            if (want < ec_floor)
                want = ec_floor;
        }
        if (b->min_isr != want)
            FAIL("volume %u: min_isr %u after REENCODE cancel, expected orig %u", va, b->min_isr, want);
    }
    if (b->stripe_version < a->stripe_version)
        FAIL("volume %u: stripe version went back from %llu to %llu", va, (unsigned long long)a->stripe_version,
             (unsigned long long)b->stripe_version);
    if (b->stripe_whole_gen < a->stripe_whole_gen)
        FAIL("volume %u: stripe whole generation went back", va);
    if (b->nstripe > a->nstripe) {
        if (!conv_recorded || b->redundancy != META_RED_EC || b->nstripe != vol_active_nstripe(b) ||
            b->stripe_version != a->stripe_version + 1 || b->conv.new_stripe_version != b->stripe_version)
            FAIL("volume %u: nstripe grew %u -> %u outside RECORD", va, a->nstripe, b->nstripe);
        for (uint32_t j = 0; j < a->nstripe; j++)
            if (a->stripe[j] != b->stripe[j] || a->stripe_pos[j].state != b->stripe_pos[j].state ||
                a->stripe_pos[j].prev != b->stripe_pos[j].prev || a->stripe_pos[j].set_ver != b->stripe_pos[j].set_ver)
                FAIL("volume %u: RECORD modified existing stripe position %u", va, j);
        for (uint32_t j = a->nstripe; j < b->nstripe; j++)
            if (b->stripe_pos[j].state != META_STRIPE_WHOLE || b->stripe_pos[j].prev != 0 ||
                b->stripe_pos[j].set_ver != b->stripe_version || b->stripe_pos[j].retired_ver != 0)
                FAIL("volume %u: RECORD added position %u not whole at new stripe_version", va, j);
    } else if (b->nstripe < a->nstripe) {
        if (b->stripe_version != a->stripe_version + 1 || b->stripe_whole_gen != a->stripe_whole_gen)
            FAIL("volume %u: nstripe shrank %u -> %u without STRIPE_SET version bump", va, a->nstripe, b->nstripe);
        if (a->conv.phase != META_CONV_NONE && a->conv.phase != META_CONV_DROP)
            FAIL("volume %u: nstripe shrank in phase %s", va, meta_conv_phase_name(a->conv.phase));
        if (b->nstripe < vol_active_nstripe(b))
            FAIL("volume %u: nstripe shrank to %u below active geometry %u", va, b->nstripe, vol_active_nstripe(b));
        if (a->mixed_redundancy == META_RED_EC && b->nstripe < (uint32_t)a->mixed_k + a->mixed_m)
            FAIL("volume %u: nstripe shrank to %u below mixed geometry %u+%u", va, b->nstripe, a->mixed_k, a->mixed_m);
        if (!stripe_drop_safe(a, b->nstripe))
            FAIL("volume %u: STRIPE_SET dropped positions [%u..%u) with live or unknown objects (epoch %llu vs live %llu)",
                 va, b->nstripe, a->nstripe, (unsigned long long)a->epoch, (unsigned long long)a->live_epoch);
        for (uint32_t j = 0; j < b->nstripe; j++)
            if (a->stripe[j] != b->stripe[j] || a->stripe_pos[j].state != b->stripe_pos[j].state ||
                a->stripe_pos[j].prev != b->stripe_pos[j].prev ||
                a->stripe_pos[j].set_ver != b->stripe_pos[j].set_ver ||
                a->stripe_pos[j].retired_ver != b->stripe_pos[j].retired_ver)
                FAIL("volume %u: STRIPE_SET drop modified kept position %u", va, j);
    } else {
        uint32_t sdiff = 0, wdiff = 0;
        for (uint32_t j = 0; j < a->nstripe && j < META_STRIPE_MAX; j++) {
            int pos_chg = a->stripe[j] != b->stripe[j] || a->stripe_pos[j].prev != b->stripe_pos[j].prev ||
                          a->stripe_pos[j].set_ver != b->stripe_pos[j].set_ver ||
                          (a->stripe_pos[j].state == META_STRIPE_WHOLE &&
                           b->stripe_pos[j].state == META_STRIPE_FILLING);
            int to_whole =
                a->stripe_pos[j].state == META_STRIPE_FILLING && b->stripe_pos[j].state == META_STRIPE_WHOLE;
            sdiff += (uint32_t)pos_chg;
            wdiff += (uint32_t)to_whole;
            if (pos_chg && b->stripe_version > a->stripe_version && b->stripe_pos[j].set_ver != b->stripe_version)
                FAIL("volume %u: stripe position %u changed with set_ver %llu, not %llu", va, j,
                     (unsigned long long)b->stripe_pos[j].set_ver, (unsigned long long)b->stripe_version);
            if (b->stripe_pos[j].retired_ver != a->stripe_pos[j].retired_ver) {
                if (!conv_recorded && !conv_reenc_cancel)
                    FAIL("volume %u: stripe position %u retired_ver changed (%llu -> %llu) outside RECORD or REENCODE cancel",
                         va, j, (unsigned long long)a->stripe_pos[j].retired_ver,
                         (unsigned long long)b->stripe_pos[j].retired_ver);
                uint32_t act_b = vol_active_nstripe(b);
                if (j < act_b) {
                    if (b->stripe_pos[j].retired_ver != 0)
                        FAIL("volume %u: active position %u has retired_ver %llu", va, j,
                             (unsigned long long)b->stripe_pos[j].retired_ver);
                } else {
                    if (a->stripe_pos[j].retired_ver != 0 || b->stripe_pos[j].retired_ver != b->stripe_version)
                        FAIL("volume %u: retired position %u retired_ver moved %llu -> %llu (sv %llu)", va, j,
                             (unsigned long long)a->stripe_pos[j].retired_ver,
                             (unsigned long long)b->stripe_pos[j].retired_ver,
                             (unsigned long long)b->stripe_version);
                }
            }
        }
        if (b->stripe_version == a->stripe_version && sdiff)
            FAIL("volume %u: a stripe position changed without a version bump", va);
        if (b->stripe_version > a->stripe_version &&
            (b->stripe_version != a->stripe_version + 1 || sdiff > 1 || wdiff > 0 ||
             b->stripe_whole_gen != a->stripe_whole_gen || (sdiff > 0 && (conv_recorded || conv_reenc_cancel))))
            FAIL("volume %u: stripe version %llu -> %llu with %u positions moved, %u whole (one STRIPE_SET, one position)",
                 va, (unsigned long long)a->stripe_version, (unsigned long long)b->stripe_version, sdiff, wdiff);
        if (wdiff > 0 || b->stripe_whole_gen > a->stripe_whole_gen) {
            if (wdiff != 1 || b->stripe_whole_gen != a->stripe_whole_gen + 1 ||
                b->stripe_version != a->stripe_version || b->stripe_whole_ver != b->stripe_version)
                FAIL("volume %u: position went whole wrongly (wdiff %u, gen %llu -> %llu, ver %llu, current %llu)", va,
                     wdiff, (unsigned long long)a->stripe_whole_gen, (unsigned long long)b->stripe_whole_gen,
                     (unsigned long long)b->stripe_whole_ver, (unsigned long long)b->stripe_version);
        }
    }
    /* live_* fields may change only by VOLUME_LIVE_OBJS (at current epoch & stripe_version) or be cleared by RECORD /
     * REENCODE cancel. */
    {
        int live_same = a->live_epoch == b->live_epoch && a->live_sv_at == b->live_sv_at &&
                        a->live_overflow == b->live_overflow && a->nlive_sv == b->nlive_sv;
        for (uint32_t j = 0; live_same && j < a->nlive_sv && j < META_LIVE_SV_MAX; j++)
            if (a->live_sv[j].stripe_version != b->live_sv[j].stripe_version ||
                a->live_sv[j].count != b->live_sv[j].count)
                live_same = 0;
        if (!live_same) {
            int cleared = !b->live_epoch && !b->live_sv_at && !b->live_overflow && !b->nlive_sv;
            if (cleared) {
                if (!conv_recorded && !conv_reenc_cancel)
                    FAIL("volume %u: live_sv cleared outside RECORD or REENCODE cancel", va);
            } else {
                if (b->live_epoch != b->epoch || b->epoch != a->epoch || b->stripe_version != a->stripe_version ||
                    b->nstripe != a->nstripe || b->conv.phase != a->conv.phase ||
                    b->live_sv_at != b->stripe_version || b->live_overflow > 1 || b->nlive_sv > META_LIVE_SV_MAX)
                    FAIL("volume %u: live_sv changed illegally (epoch %llu/%llu, sv %llu/%llu)", va,
                         (unsigned long long)b->live_epoch, (unsigned long long)b->epoch,
                         (unsigned long long)b->live_sv_at, (unsigned long long)b->stripe_version);
                for (uint32_t j = 0; j < b->nlive_sv; j++)
                    if (b->live_sv[j].stripe_version < 1 || b->live_sv[j].stripe_version > b->stripe_version ||
                        (j > 0 && b->live_sv[j].stripe_version <= b->live_sv[j - 1].stripe_version))
                        FAIL("volume %u: live_sv[%u] out of range or unsorted", va, j);
            }
        }
    }
    /* mixed_* (leftover EC geometry from a cancelled REENCODE) may be set only on REENCODE cancel of an EC target,
     * and cleared only by a current-epoch non-overflow VOLUME_LIVE_OBJS proving 0 live objects in [lo .. hi]. */
    {
        int mixed_same = a->mixed_redundancy == b->mixed_redundancy && a->mixed_k == b->mixed_k &&
                         a->mixed_m == b->mixed_m && a->mixed_sv_lo == b->mixed_sv_lo &&
                         a->mixed_sv_hi == b->mixed_sv_hi;
        if (!mixed_same) {
            if (a->mixed_redundancy == META_RED_REPLICAS && b->mixed_redundancy == META_RED_EC) {
                if (!conv_reenc_cancel || a->redundancy != META_RED_EC || b->mixed_k != a->ec_k ||
                    b->mixed_m != a->ec_m || b->mixed_sv_lo != a->conv.new_stripe_version ||
                    b->mixed_sv_hi != a->stripe_version)
                    FAIL("volume %u: mixed geometry set outside EC REENCODE cancel", va);
            } else if (a->mixed_redundancy == META_RED_EC && b->mixed_redundancy == META_RED_REPLICAS) {
                if (b->mixed_k || b->mixed_m || b->mixed_sv_lo || b->mixed_sv_hi || b->live_epoch != b->epoch ||
                    b->live_overflow || b->live_sv_at < a->mixed_sv_hi)
                    FAIL("volume %u: mixed geometry cleared without current-epoch live report", va);
                for (uint32_t j = 0; j < b->nlive_sv; j++)
                    if (b->live_sv[j].count > 0 && b->live_sv[j].stripe_version >= a->mixed_sv_lo &&
                        b->live_sv[j].stripe_version <= a->mixed_sv_hi)
                        FAIL("volume %u: mixed geometry cleared while live objects remain in [%llu..%llu]", va,
                             (unsigned long long)a->mixed_sv_lo, (unsigned long long)a->mixed_sv_hi);
            } else {
                FAIL("volume %u: mixed geometry changed illegally", va);
            }
        }
    }
    if (b->gc_gen < a->gc_gen)
        FAIL("volume %u: gc_gen went back", va);
    if (a->gc_suspended && !b->gc_suspended && b->gc_gen == a->gc_gen)
        FAIL("volume %u: orphan collection resumed without GC_RESUME", va);
    if (b->epoch < a->epoch)
        FAIL("volume %u: epoch went back from %llu to %llu", va, (unsigned long long)a->epoch,
             (unsigned long long)b->epoch);
    int moved = b->leader != a->leader;
    if (b->epoch >= META_EPOCH_LIMIT && b->epoch != a->epoch)
        FAIL("volume %u: epoch %llu is beyond META_EPOCH_LIMIT", va, (unsigned long long)b->epoch);
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
        /* a grant lands above every epoch a claimant reported (highest_seen); only the owner's own step to e + 1
         * (VOLUME_SELF) is exempt */
        if (b->epoch > a->epoch && (moved || b->epoch != a->epoch + 1) && b->epoch <= a->highest_seen)
            FAIL("volume %u: epoch %llu was granted although epoch %llu was seen", va, (unsigned long long)b->epoch,
                 (unsigned long long)a->highest_seen);
    }
    if (b->epoch > a->epoch && moved && !meta_volume_in_isr(a, b->leader))
        FAIL("volume %u: leader %u of epoch %llu was not in the ISR of epoch %llu", va, b->leader,
             (unsigned long long)b->epoch, (unsigned long long)a->epoch);
    if (b->kind == META_VOL_REPLICATED && b->epoch > a->epoch)
        for (uint32_t j = 0; j < b->nisr; j++)
            if (!meta_volume_in_isr(a, b->isr[j]))
                FAIL("volume %u: epoch %llu's ISR holds %u, which epoch %llu's did not", va,
                     (unsigned long long)b->epoch, b->isr[j], (unsigned long long)a->epoch);
    /* the replica set moves exactly with its version (D8): a node's index in rep[] names it only with the version */
    if (b->set_version < a->set_version)
        FAIL("volume %u: the replica set version went back", va);
    if (b->set_version == a->set_version && !same_set(a, b))
        FAIL("volume %u: the replica set changed without a new set version", va);
    if (b->set_version == a->set_version + 1 && one_set_op(a, b, why, n))
        return -1;
    /* a planned transfer's target was in the ISR the grant met (D14); it changes only with the epoch */
    if (b->epoch > a->epoch && b->grant_target && !meta_volume_in_isr(a, b->grant_target))
        FAIL("volume %u: epoch %llu's grant names target %u, which was not in the ISR of epoch %llu", va,
             (unsigned long long)b->epoch, b->grant_target, (unsigned long long)a->epoch);
    if (b->epoch > a->epoch && b->grant_target && b->grant_target == a->leader)
        FAIL("volume %u: epoch %llu's grant names target %u, the leader of epoch %llu it moves away from", va,
             (unsigned long long)b->epoch, b->grant_target, (unsigned long long)a->epoch);
    if (b->epoch > a->epoch && b->grant_target && b->leader != b->grant_target)
        FAIL("volume %u: epoch %llu's grant names target %u, but %u leads it", va, (unsigned long long)b->epoch,
             b->grant_target, b->leader);
    if (b->epoch == a->epoch && (b->grant_target != a->grant_target || b->grant_min_end != a->grant_min_end))
        FAIL("volume %u: the grant's target changed within epoch %llu", va, (unsigned long long)a->epoch);
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
        if (a->kind == META_VOL_SHARED && b->leader != a->leader) {
            /* a shared volume goes to a member that is in the group, and not to one a claim fault barred while
             * another candidate is not barred */
            const struct meta_member *mp = meta_member(prev, b->leader);
            const struct meta_member *mn = meta_member(next, b->leader);
            /* OUT -> UP is reversible (D2), so one entry can bring a member back and grant to it, or grant then OUT */
            if (!mp || !mn || (mp->state >= META_OUT && mn->state >= META_OUT))
                FAIL("volume %u: granted to node %u, which is not a member", a->number, b->leader);
            const struct meta_replica *rn = meta_replica(a, b->leader);
            if (rn && rn->claim_blocked)
                for (uint32_t j = 0; j < a->nrep; j++)
                    if (!a->rep[j].claim_blocked)
                        FAIL("volume %u: granted to node %u, whose claims failed, while node %u was not barred",
                             a->number, b->leader, a->rep[j].node);
        }
    }
    for (uint32_t i = 0; i < next->nvolumes; i++) {
        const struct meta_volume *b = &next->volumes[i];
        if (b->min_isr < 1 || b->min_isr > b->nrep)
            FAIL("volume %u: min_isr %u with %u replicas", b->number, b->min_isr, b->nrep);
        const struct meta_volume *a = meta_volume(prev, b->number);
        if (b->redundancy == META_RED_EC) {
            uint32_t active = (uint32_t)b->ec_k + b->ec_m;
            if (b->kind != META_VOL_REPLICATED || b->ec_k < 1 || b->ec_k > META_EC_K_MAX || b->ec_m < 1 ||
                b->ec_m > META_EC_M_MAX || b->nstripe < active || b->nstripe > META_STRIPE_MAX || !b->stripe_version)
                FAIL("volume %u: an EC volume of k %u, m %u with %u positions", b->number, b->ec_k, b->ec_m,
                     b->nstripe);
            if (!a) {
                if (b->nstripe != active || b->stripe_version != 1 || b->stripe_whole_gen || b->stripe_whole_ver ||
                    b->gc_gen || b->min_isr < (b->ec_m < b->nrep ? b->ec_m : b->nrep))
                    FAIL("volume %u: a new EC volume with stripe version %llu, whole_gen %llu, gc_gen %llu, min_isr %u (m %u)",
                         b->number, (unsigned long long)b->stripe_version, (unsigned long long)b->stripe_whole_gen,
                         (unsigned long long)b->gc_gen, b->min_isr, b->ec_m);
                for (uint32_t j = 0; j < b->nstripe; j++)
                    if (b->stripe_pos[j].state != META_STRIPE_WHOLE || b->stripe_pos[j].prev != 0 ||
                        b->stripe_pos[j].set_ver != 1 || b->stripe_pos[j].retired_ver != 0)
                        FAIL("volume %u: new EC position %u is not whole at version 1", b->number, j);
            }
        } else if (b->redundancy == META_RED_REPLICAS) {
            if (b->ec_k || b->ec_m || b->nstripe > META_STRIPE_MAX)
                FAIL("volume %u: a replicas volume with EC geometry", b->number);
            if (!a && (b->nstripe || b->stripe_version || b->stripe_whole_gen || b->stripe_whole_ver ||
                       b->gc_suspended || b->gc_gen))
                FAIL("volume %u: a new replicas volume with EC fields", b->number);
            if (!b->stripe_version && (b->nstripe || b->stripe_whole_gen || b->stripe_whole_ver || b->gc_suspended ||
                                       b->gc_gen))
                FAIL("volume %u: a replicas volume with EC fields", b->number);
        } else {
            FAIL("volume %u: unknown redundancy %u", b->number, b->redundancy);
        }
        if (a && (a->redundancy == META_RED_EC || a->nstripe > 0) &&
            (b->redundancy == META_RED_EC || b->nstripe > 0) && next->restore_gen > prev->restore_gen &&
            !b->gc_suspended && b->gc_gen == a->gc_gen)
            FAIL("volume %u: a restore left orphan collection on", b->number);
        if (b->mixed_redundancy == META_RED_REPLICAS) {
            if (b->mixed_k || b->mixed_m || b->mixed_sv_lo || b->mixed_sv_hi)
                FAIL("volume %u: non-zero mixed fields while mixed_redundancy is 0", b->number);
        } else if (b->mixed_redundancy == META_RED_EC) {
            if (b->conv.phase != META_CONV_NONE || b->mixed_k < 1 || b->mixed_k > META_EC_K_MAX || b->mixed_m < 1 ||
                b->mixed_m > META_EC_M_MAX || (uint32_t)b->mixed_k + b->mixed_m > b->nstripe ||
                b->mixed_sv_lo < 1 || b->mixed_sv_lo > b->mixed_sv_hi || b->mixed_sv_hi >= b->stripe_version)
                FAIL("volume %u: invalid mixed EC geometry (%u+%u, nstripe %u, sv %llu..%llu/%llu)", b->number,
                     b->mixed_k, b->mixed_m, b->nstripe, (unsigned long long)b->mixed_sv_lo,
                     (unsigned long long)b->mixed_sv_hi, (unsigned long long)b->stripe_version);
        } else {
            FAIL("volume %u: unknown mixed_redundancy %u", b->number, b->mixed_redundancy);
        }
        /* Validate all held stripe positions (both active EC and retired positions awaiting DROP). */
        uint32_t active_ns = vol_active_nstripe(b);
        for (uint32_t j = 0; j < b->nstripe; j++) {
            if (!meta_member(next, b->stripe[j]))
                FAIL("volume %u: stripe position %u names node %u, which is not a member", b->number, j, b->stripe[j]);
            if (b->stripe_pos[j].state > META_STRIPE_FILLING || b->stripe_pos[j].set_ver < 1 ||
                b->stripe_pos[j].set_ver > b->stripe_version || b->stripe_pos[j].retired_ver > b->stripe_version)
                FAIL("volume %u: stripe position %u has invalid state %u, set_ver %llu or retired_ver %llu (sv %llu)",
                     b->number, j, b->stripe_pos[j].state, (unsigned long long)b->stripe_pos[j].set_ver,
                     (unsigned long long)b->stripe_pos[j].retired_ver, (unsigned long long)b->stripe_version);
            if (j < active_ns && b->stripe_pos[j].retired_ver != 0)
                FAIL("volume %u: active stripe position %u marked retired", b->number, j);
            if (j >= active_ns && b->stripe_pos[j].retired_ver == 0)
                FAIL("volume %u: trailing stripe position %u lacks retired_ver", b->number, j);
            if (b->stripe_pos[j].prev &&
                (!meta_member(next, b->stripe_pos[j].prev) || b->stripe_pos[j].prev == b->stripe[j]))
                FAIL("volume %u: stripe position %u names invalid prev node %u", b->number, j, b->stripe_pos[j].prev);
            for (uint32_t k = 0; k < j; k++)
                if (b->stripe[k] == b->stripe[j])
                    FAIL("volume %u: node %u holds stripe positions %u and %u", b->number, b->stripe[j], k, j);
        }
        /* Conversion state and lower-geometry position floor (D26, E24). */
        if (b->conv.phase != META_CONV_NONE) {
            if (b->kind != META_VOL_REPLICATED || b->conv.phase > META_CONV_LAST)
                FAIL("volume %u: invalid conversion phase %u", b->number, b->conv.phase);
            uint32_t orig_ns = b->conv.orig_redundancy == META_RED_EC ? (uint32_t)b->conv.orig_k + b->conv.orig_m : 0;
            uint32_t targ_ns =
                b->conv.target_redundancy == META_RED_EC ? (uint32_t)b->conv.target_k + b->conv.target_m : 0;
            uint32_t lower_ns = orig_ns < targ_ns ? orig_ns : targ_ns;
            if (b->nstripe < lower_ns)
                FAIL("volume %u: %u stripe positions below lower geometry %u during conversion", b->number, b->nstripe,
                     lower_ns);
            if (b->conv.phase < META_CONV_REENCODE && b->conv.new_stripe_version != 0)
                FAIL("volume %u: new_stripe_version set before REENCODE", b->number);
            if (b->conv.new_stripe_version > b->stripe_version)
                FAIL("volume %u: new_stripe_version past stripe_version", b->number);
        }
        if (b->live_epoch > b->epoch || b->live_sv_at > b->stripe_version || b->nlive_sv > META_LIVE_SV_MAX ||
            b->live_overflow > 1)
            FAIL("volume %u: invalid live-object report state", b->number);
        for (uint32_t j = 0; j < b->nlive_sv; j++)
            if (b->live_sv[j].stripe_version < 1 || b->live_sv[j].stripe_version > b->stripe_version ||
                (j > 0 && b->live_sv[j].stripe_version <= b->live_sv[j - 1].stripe_version))
                FAIL("volume %u: invalid live_sv entry %u", b->number, j);
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
            for (uint32_t k = 0; k < j; k++) {
                if (b->rep[k].node == b->rep[j].node)
                    FAIL("volume %u: node %u holds two replicas (%u and %u)", b->number, b->rep[j].node, k, j);
                if (b->kind == META_VOL_REPLICATED && b->rep[k].rid == b->rep[j].rid)
                    FAIL("volume %u: replicas %u and %u share instance %llu", b->number, b->rep[k].node,
                         b->rep[j].node, (unsigned long long)b->rep[j].rid);
            }
            if (b->kind == META_VOL_REPLICATED && !b->rep[j].rid)
                FAIL("volume %u: replica %u has no instance", b->number, b->rep[j].node);
            /* R13 (E49, E50, D15): a newly placed log replica must land on a compute-capable member (or one with 0
             * registered disks) whose declared role allows compute; checked across prev/next for multi-op entries. */
            if (b->kind == META_VOL_REPLICATED && (!a || !meta_replica(a, b->rep[j].node))) {
                const struct meta_member *mp = meta_member(prev, b->rep[j].node);
                const struct meta_member *mn = meta_member(next, b->rep[j].node);
                if (!mn || (!member_log_capable(mp) && !member_log_capable(mn)))
                    FAIL("volume %u: log replica placed on node %u without compute capability (role %s, cap %s, log_on_hdd %u)",
                         b->number, b->rep[j].node, mn ? meta_role_name(mn->role) : "?",
                         mn ? meta_cap_name(mn->capability) : "?", next->set.log_on_hdd);
            }
        }
        /* R13 (E49, E51): newly placed EC stripe positions must land on storage-capable members (when disks exist). */
        if (b->redundancy == META_RED_EC) {
            for (uint32_t j = 0; j < b->nstripe && j < META_STRIPE_MAX; j++) {
                if (!a || j >= a->nstripe || a->stripe[j] != b->stripe[j]) {
                    const struct meta_member *mp = meta_member(prev, b->stripe[j]);
                    const struct meta_member *mn = meta_member(next, b->stripe[j]);
                    if (!mn || (!member_storage_capable(mp) && !member_storage_capable(mn)))
                        FAIL("volume %u: stripe position %u placed on node %u without storage capability (role %s, cap %s)",
                             b->number, j, b->stripe[j], mn ? meta_role_name(mn->role) : "?",
                             mn ? meta_cap_name(mn->capability) : "?");
                }
            }
        }
        /* at most one move, of a current member that is not itself moving in (D10) */
        uint32_t moves = 0;
        for (uint32_t j = 0; j < b->nrep; j++) {
            uint32_t x = b->rep[j].replaces;
            if (!x)
                continue;
            moves++;
            const struct meta_replica *ra = meta_replica(b, x);
            /* ra->replaces is implied by moves > 1 below; kept should the one-move rule ever go */
            if (b->kind != META_VOL_REPLICATED || !ra || x == b->rep[j].node || ra->replaces)
                FAIL("volume %u: replica %u moves in for %u, which is not a member it may replace", b->number,
                     b->rep[j].node, x);
        }
        if (moves > 1)
            FAIL("volume %u: %u moves at once", b->number, moves);
        if (b->kind == META_VOL_SHARED) {
            /* the ISR is every candidate: they all see the same storage */
            if (b->nisr != b->nrep || b->nhist != 0)
                FAIL("volume %u: a shared volume's ISR is not every candidate (or it keeps a history)", b->number);
        } else if (b->nhist < 1 || b->hist[b->nhist - 1].epoch != b->epoch) {
            FAIL("volume %u: the epoch history does not end at epoch %llu", b->number, (unsigned long long)b->epoch);
        }
    }
    for (uint32_t i = 0; i < prev->nmembers; i++) {
        const struct meta_member *a = &prev->members[i], *b = meta_member(next, a->id);
        if (!b)
            FAIL("member %u disappeared", a->id);
        if (b->incarnation < a->incarnation)
            FAIL("member %u: incarnation went back (%llu -> %llu)", a->id, (unsigned long long)a->incarnation,
                 (unsigned long long)b->incarnation);
        if (a->state == META_DESTROYED && b->state != META_DESTROYED)
            FAIL("member %u: left DESTROYED to %s", a->id, meta_member_state_name(b->state));
        if (a->state == META_OUT && b->state != META_OUT && b->state != META_DESTROYED &&
            b->incarnation <= a->incarnation)
            FAIL("member %u: left OUT to %s without a new incarnation (%llu -> %llu)", a->id,
                 meta_member_state_name(b->state), (unsigned long long)a->incarnation,
                 (unsigned long long)b->incarnation);
        if (b->ndisks < a->ndisks)
            FAIL("member %u: disk count went down (%u -> %u)", a->id, a->ndisks, b->ndisks);
        for (uint32_t j = 0; j < a->ndisks && j < META_DISKS_MAX; j++) {
            if (memcmp(a->disks[j].store_id, b->disks[j].store_id, 16) != 0)
                FAIL("member %u: disk %u changed store_id", a->id, j);
            if (a->disks[j].state == META_STORE_DESTROYED && b->disks[j].state != META_STORE_DESTROYED)
                FAIL("member %u: disk %u left DESTROYED to %s", a->id, j, meta_store_state_name(b->disks[j].state));
        }
        if (a->role != b->role && b->ndisks > 0) {
            if (b->role == META_ROLE_COMPUTE && !(a->ndisks == 0 || (a->capability & META_CAP_COMPUTE)) &&
                !(b->capability & META_CAP_COMPUTE))
                FAIL("member %u: role set to compute without compute capability", a->id);
            if (b->role == META_ROLE_STORAGE && !(a->ndisks == 0 || (a->capability & META_CAP_STORAGE)) &&
                !(b->capability & META_CAP_STORAGE))
                FAIL("member %u: role set to storage without storage capability", a->id);
        }
    }
    for (uint32_t i = 0; i < next->nmembers; i++) {
        const struct meta_member *b = &next->members[i];
        if (b->state > META_DESTROYED)
            FAIL("member %u: unknown state %u", b->id, b->state);
        if (b->role > META_ROLE_LAST)
            FAIL("member %u: unknown role %u", b->id, b->role);
        if (b->ndisks > META_DISKS_MAX)
            FAIL("member %u: ndisks %u exceeds META_DISKS_MAX", b->id, b->ndisks);
        for (uint32_t j = 0; j < b->ndisks; j++)
            if (!disk_fields_valid(&b->disks[j]))
                FAIL("member %u: disk %u has invalid fields", b->id, j);
        uint8_t want_cap = member_compute_cap(b, next->set.log_on_hdd);
        if (b->capability != want_cap)
            FAIL("member %u: capability %s does not match UP disks (%s)", b->id, meta_cap_name(b->capability),
                 meta_cap_name(want_cap));
        if (!meta_member(prev, b->id) && b->ndisks > 0) {
            if ((b->role == META_ROLE_COMPUTE && !(b->capability & META_CAP_COMPUTE)) ||
                (b->role == META_ROLE_STORAGE && !(b->capability & META_CAP_STORAGE)))
                FAIL("member %u: added with role %s unsupported by disks (%s)", b->id, meta_role_name(b->role),
                     meta_cap_name(b->capability));
        }
        if (b->state == META_DOWN) {
            if (!b->down_since_ms || b->down_reason < META_DOWN_TIMEOUT || b->down_reason > META_DOWN_LAST)
                FAIL("member %u: DOWN without down_since or reason (%llu, %u)", b->id,
                     (unsigned long long)b->down_since_ms, b->down_reason);
            if (b->down_since_ms > b->down_clock_ms)
                FAIL("member %u: down_since %llu is in the future of op clock %llu", b->id,
                     (unsigned long long)b->down_since_ms, (unsigned long long)b->down_clock_ms);
        } else if (b->down_since_ms || b->down_reason != META_DOWN_NONE || b->down_clock_ms) {
            FAIL("member %u: %s with down_since %llu or reason %u", b->id, meta_member_state_name(b->state),
                 (unsigned long long)b->down_since_ms, b->down_reason);
        }
        if (b->hold_until_ms) {
            if (b->state >= META_OUT)
                FAIL("member %u: %s with an active hold", b->id, meta_member_state_name(b->state));
            if (!b->hold_clock_ms || b->hold_until_ms < b->hold_clock_ms ||
                b->hold_until_ms - b->hold_clock_ms > META_HOLD_MAX_MS)
                FAIL("member %u: hold %llu exceeds META_HOLD_MAX_MS past clock %llu", b->id,
                     (unsigned long long)b->hold_until_ms, (unsigned long long)b->hold_clock_ms);
        } else if (b->hold_clock_ms) {
            FAIL("member %u: hold_clock set without hold_until", b->id);
        }
        if (b->replaced_by) {
            if (b->replaced_by == b->id)
                FAIL("member %u: replaced_by names itself", b->id);
            if (b->state == META_DESTROYED)
                FAIL("member %u: DESTROYED with replaced_by %u", b->id, b->replaced_by);
            if (!meta_member(next, b->replaced_by))
                FAIL("member %u: replaced_by unknown member %u", b->id, b->replaced_by);
        }
    }
    if (!stores_unique(next))
        FAIL("two disks share a store_id");
    if (next->next_member < prev->next_member || next->next_volume < prev->next_volume)
        FAIL("an id counter went back");
    /* a slot changes home only from a freeze committed in an earlier entry, under the epoch its old home had then */
    if (prev->slot_home != next->slot_home &&
        memcmp(prev->slot_home, next->slot_home, META_SLOTS * sizeof *prev->slot_home) != 0)
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
    /* users: uids never go back and name one user; a user's uid stays while it lives; a version moves only with a
     * put, which is the entry's own index */
    if (next->max_uid < prev->max_uid)
        FAIL("max_uid went down (%u -> %u)", prev->max_uid, next->max_uid);
    for (uint32_t i = 0; i < next->nusers; i++) {
        const struct meta_user *u = &next->users[i], *o = meta_user(prev, u->name);
        if (!u->uid || u->uid > next->max_uid || u->len < META_USER_UID_LEN || get_le32(u->blob) != u->uid)
            FAIL("user %s: uid %u outside 1..%u or not in its blob", u->name, u->uid, next->max_uid);
        if (i && strcmp(u->name, next->users[i - 1].name) <= 0)
            FAIL("users out of order at %s", u->name);
        if (o && o->version == u->version &&
            (o->uid != u->uid || o->len != u->len || (o->blob != u->blob && memcmp(o->blob, u->blob, u->len))))
            FAIL("user %s changed without a put", u->name);
        if (o && o->version != u->version && u->version != next->applied)
            FAIL("user %s: version %llu is not this entry's index %llu", u->name, (unsigned long long)u->version,
                 (unsigned long long)next->applied);
        if (o && o->uid != u->uid && u->uid <= prev->max_uid)
            FAIL("user %s: uid %u -> %u, a uid already given out", u->name, o->uid, u->uid);
        if (!o && u->uid <= prev->max_uid)
            FAIL("user %s: new with uid %u, at or below max_uid %u", u->name, u->uid, prev->max_uid);
    }
    if (users_uids_unique(next) != 0)
        FAIL("two users share a uid");
    if (meta_users_bytes(next) > META_USERS_BYTES_MAX)
        FAIL("the users take %llu bytes, above %u", (unsigned long long)meta_users_bytes(next), META_USERS_BYTES_MAX);
    return 0;
}
