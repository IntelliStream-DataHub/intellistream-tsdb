/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R10 online redundancy conversion Jepsen harness support (--convert A:B; docs/design-cluster-data.md §6.4, §11,
 * r10-11-15-scope/PLAN.md D26-D29, row r10-jc).
 *
 * Included by jepsen_cluster.c right after cluster_r13.h. Uses its node table, views, admin calls, EC/R9 helpers,
 * and histories.
 *
 * Usage:
 *   jepsen_cluster --convert A:B [--convert-at 30] [--convert-timeout-s 90] --workload meta,kv --nemesis kill|partition
 *
 * Supported endpoints A and B (A != B):
 *   - replicas=N (or rN): N in 1..META_REPLICAS_MAX
 *   - K+M (or ec=K+M):    K in 1..META_EC_K_MAX, M in 1..META_EC_M_MAX (m + 1 log replicas)
 *
 * Canonical R10 transitions:
 *   - replicas=3:4+2   (GROW/RECORD + EC tier attach; verifies post-conversion settles produce 4+2 .ec parts)
 *   - 4+2:4+3          (GROW 3->4, RECORD 4+3, REENCODE old .ec parts to 4+3, DROP/SHRINK; verifies all .ec parts 4+3)
 *   - 4+2:replicas=3   (RECORD replicas=3, REENCODE/MK_UNSETTLE all .ec parts back to .part, DROP stripe; verifies 0 .ec)
 *
 * Started --convert-at seconds (default 30 s) into the run while kill or partition faults and active kv/replace/fill
 * writers continue. Enforces D15 (--io epoll on nodes >= 2). Checker verifies:
 *   - all normal Jepsen invariants (election safety, checks 1-5: durability/no lost writes, validity/no resurrected or
 *     refused writes, atomic visibility, no stale reads, monotonic reads, 0 DIVERGED/LOCAL_FAULT, full live per-member
 *     replica equality across all final ISR members via ec_members_check),
 *   - strict phase ordering during conversion (no stripe drop during REENCODE; and if a conversion shrinks log
 *     replicas, no shrink before SHRINK),
 *   - conversion completes (or prints a detailed diagnostic explaining the phase and counts where it stalled),
 *   - final on-disk .ec geometry matches B (all (1..k)+m for EC targets; 0 live .ec parts for replicas targets).
 */
#ifndef JEPSEN_CLUSTER_R10_H
#define JEPSEN_CLUSTER_R10_H

/* ---- R10 conversion configuration & runtime state (r10_convert_on / r10_need_ec declared in jepsen_cluster.c) */
static int r10_convert_at_s = 30;       /* --convert-at S (default 30 s into the run) */
static int r10_convert_timeout_s = 90;  /* --convert-timeout-s S (quiet-tail wait after last heal) */
static int r10_min_nodes = 3;
static char r10_spec[64];
static char r10_src_desc[32], r10_dst_desc[32];
static struct meta_redundancy_target r10_src, r10_dst;
static uint32_t r10_orig_nstripe, r10_orig_nrep;

static pthread_mutex_t r10_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int r10_conv_started;    /* 0 = pending, 1 = started, -1 = failed to start */
static _Atomic int r10_conv_done;       /* 1 = reached target with conv.phase == NONE */
static int64_t r10_conv_start_ms, r10_conv_done_ms;
static unsigned r10_phases_seen;        /* bitmask of (1u << mv.conv.phase) observed */
static int r10_pre_ec_parts;            /* .ec parts settled before starting conversion (for EC sources) */
static int r10_post_ec_parts;           /* live .ec parts across replicas at end of run */
static int r10_end_bad;                 /* R10-specific violations */
static int r10_dropearly_bad, r10_shrinkfirst_bad;
static char r10_why_not[512];

static uint16_t r10_le16(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1] << 8); }
static uint32_t r10_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint64_t r10_le64(const uint8_t *p)
{
    return (uint64_t)r10_le32(p) | ((uint64_t)r10_le32(p + 4) << 32);
}

static int r10_parse_side(const char *s, struct meta_redundancy_target *out, char *desc, size_t dcap)
{
    memset(out, 0, sizeof *out);
    if (!s || !*s)
        return -1;
    unsigned n = 0;
    char extra = 0;
    if (sscanf(s, "replicas=%u%c", &n, &extra) == 1 || sscanf(s, "r%u%c", &n, &extra) == 1) {
        if (n < 1 || n > META_REPLICAS_MAX)
            return -1;
        out->redundancy = META_RED_REPLICAS;
        out->replicas = n;
        snprintf(desc, dcap, "replicas=%u", n);
        return 0;
    }
    const char *p = !strncmp(s, "ec=", 3) ? s + 3 : s;
    unsigned k = 0, m = 0;
    if (sscanf(p, "%u+%u%c", &k, &m, &extra) == 2) {
        if (k < 1 || k > META_EC_K_MAX || m < 1 || m > META_EC_M_MAX)
            return -1;
        out->redundancy = META_RED_EC;
        out->ec_k = (uint8_t)k;
        out->ec_m = (uint8_t)m;
        out->replicas = m + 1;
        snprintf(desc, dcap, "%u+%u", k, m);
        return 0;
    }
    return -1;
}

static int r10_parse_convert(const char *spec)
{
    if (!spec || !*spec || strlen(spec) >= sizeof r10_spec)
        return -1;
    const char *colon = strchr(spec, ':');
    if (!colon || colon == spec || !colon[1] || strchr(colon + 1, ':'))
        return -1;
    char left[32], right[32];
    size_t ll = (size_t)(colon - spec);
    if (ll >= sizeof left || strlen(colon + 1) >= sizeof right)
        return -1;
    memcpy(left, spec, ll);
    left[ll] = '\0';
    snprintf(right, sizeof right, "%s", colon + 1);
    if (r10_parse_side(left, &r10_src, r10_src_desc, sizeof r10_src_desc) != 0 ||
        r10_parse_side(right, &r10_dst, r10_dst_desc, sizeof r10_dst_desc) != 0)
        return -1;
    if (r10_src.redundancy == r10_dst.redundancy && r10_src.replicas == r10_dst.replicas &&
        r10_src.ec_k == r10_dst.ec_k && r10_src.ec_m == r10_dst.ec_m)
        return -1;
    snprintf(r10_spec, sizeof r10_spec, "%s:%s", r10_src_desc, r10_dst_desc);
    r10_convert_on = 1;
    r10_need_ec = (r10_src.redundancy == META_RED_EC || r10_dst.redundancy == META_RED_EC);
    r10_orig_nstripe = r10_src.redundancy == META_RED_EC ? (uint32_t)r10_src.ec_k + (uint32_t)r10_src.ec_m : 0;
    r10_orig_nrep = r10_src.replicas;
    uint32_t dst_nstripe = r10_dst.redundancy == META_RED_EC ? (uint32_t)r10_dst.ec_k + (uint32_t)r10_dst.ec_m : 0;
    uint32_t need = 3;
    if (r10_orig_nstripe > need)
        need = r10_orig_nstripe;
    if (dst_nstripe > need)
        need = dst_nstripe;
    if (r10_src.replicas > need)
        need = r10_src.replicas;
    if (r10_dst.replicas > need)
        need = r10_dst.replicas;
    r10_min_nodes = (int)need;
    return 0;
}

static int r10_count_kvr_ec_parts(void)
{
    struct meta_volume mv;
    if (r9_vol_rec(&mv) != 0)
        return 0;
    int total = 0;
    for (uint32_t r = 0; r < mv.nrep && r < META_REPLICAS_MAX; r++) {
        int ni = node_by_id(mv.rep[r].node);
        if (ni < 0)
            continue;
        char vdir[PATH_MAX + 64];
        node_vol_dir(ni, rep_number, vdir, sizeof vdir);
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] == 'p' && l > 3 && strcmp(e->d_name + l - 3, ".ec") == 0)
                total++;
        }
        closedir(dp);
    }
    return total;
}

static int r10_conv_matches_target(const struct meta_volume *mv)
{
    if (mv->conv.phase != META_CONV_NONE)
        return 0;
    if (mv->redundancy != r10_dst.redundancy)
        return 0;
    if (mv->nrep != r10_dst.replicas || mv->nisr != r10_dst.replicas)
        return 0;
    for (uint32_t k = 0; k < mv->nrep && k < META_REPLICAS_MAX; k++)
        if (mv->rep[k].learner)
            return 0;
    if (r10_dst.redundancy == META_RED_EC) {
        uint32_t want_ns = (uint32_t)r10_dst.ec_k + (uint32_t)r10_dst.ec_m;
        if (mv->ec_k != r10_dst.ec_k || mv->ec_m != r10_dst.ec_m || mv->nstripe != want_ns)
            return 0;
        for (uint32_t p = 0; p < mv->nstripe && p < META_STRIPE_MAX; p++)
            if (mv->stripe_pos[p].state != META_STRIPE_WHOLE)
                return 0;
    } else {
        if (mv->nstripe != 0)
            return 0;
    }
    return 1;
}

/* Poll conversion state and enforce phase-ordering invariants (called from ec_poller every ~100 ms and from the
 * control loop). Thread-safe via r10_mu. */
static void r10_poll_conv(void)
{
    if (!r10_convert_on || atomic_load(&r10_conv_started) != 1 || atomic_load(&r10_conv_done))
        return;
    struct meta_volume mv;
    if (r9_vol_rec(&mv) != 0)
        return;
    pthread_mutex_lock(&r10_mu);
    if (atomic_load(&r10_conv_done)) {
        pthread_mutex_unlock(&r10_mu);
        return;
    }
    if (mv.conv.phase < 16)
        r10_phases_seen |= (1u << mv.conv.phase);
    if (mv.conv.phase == META_CONV_REENCODE && r10_orig_nstripe > 0 && mv.nstripe < r10_orig_nstripe &&
        !r10_dropearly_bad) {
        r10_dropearly_bad = 1;
        r10_end_bad++;
        snprintf(r10_why_not, sizeof r10_why_not,
                 "stripe positions dropped during REENCODE (nstripe=%" PRIu32 " < %" PRIu32 "; dropearly?)",
                 mv.nstripe, r10_orig_nstripe);
        fprintf(stderr, "jepsen-cluster: FAIL: r10: %s\n", r10_why_not);
    }
    if (mv.conv.phase != META_CONV_NONE && mv.conv.phase < META_CONV_SHRINK && mv.nrep < r10_orig_nrep &&
        !r10_shrinkfirst_bad) {
        r10_shrinkfirst_bad = 1;
        r10_end_bad++;
        snprintf(r10_why_not, sizeof r10_why_not,
                 "log replicas shrunk before SHRINK (phase=%s nrep=%" PRIu32 " < %" PRIu32 ")",
                 meta_conv_phase_name(mv.conv.phase), mv.nrep, r10_orig_nrep);
        fprintf(stderr, "jepsen-cluster: FAIL: r10: %s\n", r10_why_not);
    }
    if (r10_conv_matches_target(&mv)) {
        r10_conv_done_ms = now_ms();
        int64_t elapsed = r10_conv_done_ms - r10_conv_start_ms;
        atomic_store(&r10_conv_done, 1);
        pthread_mutex_unlock(&r10_mu);
        ev("r10: conversion %s finished in %lld ms", r10_spec, (long long)elapsed);
        printf("jepsen-cluster: r10: conversion %s on kvr finished in %lld ms (nrep=%" PRIu32 ", nisr=%" PRIu32
               ", nstripe=%" PRIu32 ")\n",
               r10_spec, (long long)elapsed, mv.nrep, mv.nisr, mv.nstripe);
        fflush(stdout);
        return;
    }
    pthread_mutex_unlock(&r10_mu);
}

/* Trigger CLUSTER_OP_VOLUME_REDUNDANCY once r10_convert_at_s seconds have elapsed into the run. */
static void r10_maybe_start_conv(int64_t t_start)
{
    if (!r10_convert_on || atomic_load(&r10_conv_started) != 0)
        return;
    if (t_start > 0 && now_ms() - t_start < (int64_t)r10_convert_at_s * 1000LL)
        return;

    /* For EC sources (4+2 -> 4+3, 4+2 -> replicas=3), ensure at least one .ec part settled before starting conversion
     * so REENCODE / MK_UNSETTLE always operates on live settled objects (and dropearly is always exercised). */
    if (r10_src.redundancy == META_RED_EC) {
        int nec = r10_count_kvr_ec_parts();
        if (nec == 0) {
            for (int64_t w0 = now_ms(); now_ms() - w0 < 25000; hold_ms(250)) {
                nec = r10_count_kvr_ec_parts();
                if (nec > 0)
                    break;
            }
        }
        r10_pre_ec_parts = nec;
        if (nec == 0) {
            pthread_mutex_lock(&r10_mu);
            atomic_store(&r10_conv_started, -1);
            r10_end_bad++;
            snprintf(r10_why_not, sizeof r10_why_not,
                     "0 .ec parts settled on kvr before starting %s conversion (nothing to re-encode/unsettle)",
                     r10_spec);
            fprintf(stderr, "jepsen-cluster: FAIL: r10: %s\n", r10_why_not);
            pthread_mutex_unlock(&r10_mu);
            return;
        }
    }

    uint8_t body[16];
    size_t bl = cluster_volume_redundancy_encode(rep_number, &r10_dst, 0, body);
    int rc = -1;
    for (int64_t a0 = now_ms(); now_ms() - a0 < 30000; hold_ms(250)) {
        rc = admin_any(CLUSTER_OP_VOLUME_REDUNDANCY, body, bl, 15000);
        if (rc == 0)
            break;
        if (rc != CLUSTER_ERR_NOTLEADER && rc != CLUSTER_ERR_RETRY && rc != CLUSTER_ERR_FEW &&
            rc != META_ERR_STATE)
            break;
    }
    if (rc != 0) {
        pthread_mutex_lock(&r10_mu);
        atomic_store(&r10_conv_started, -1);
        r10_end_bad++;
        snprintf(r10_why_not, sizeof r10_why_not, "CLUSTER_OP_VOLUME_REDUNDANCY (%s) failed: %s", r10_spec,
                 cluster_strerror(rc));
        fprintf(stderr, "jepsen-cluster: FAIL: r10: %s\n", r10_why_not);
        pthread_mutex_unlock(&r10_mu);
        return;
    }
    pthread_mutex_lock(&r10_mu);
    r10_conv_start_ms = now_ms();
    int64_t start_ms = r10_conv_start_ms;
    atomic_store(&r10_conv_started, 1);
    pthread_mutex_unlock(&r10_mu);
    ev("r10: started conversion %s on kvr (v%u, pre_ec_parts=%d)", r10_spec, rep_number, r10_pre_ec_parts);
    printf("jepsen-cluster: r10: started conversion %s on kvr (v%" PRIu32 ") at %lld ms (pre_ec_parts=%d)\n", r10_spec,
           rep_number, t_start > 0 ? (long long)(start_ms - t_start) : 0LL, r10_pre_ec_parts);
    fflush(stdout);
    r10_poll_conv();
}

/* Wait for conversion to finish after the last heal, while writers are still running, then sync rep_n/rep_nodes. */
static void r10_await_conv_end(void)
{
    if (!r10_convert_on)
        return;
    if (atomic_load(&r10_conv_started) == 0)
        r10_maybe_start_conv(0);
    if (atomic_load(&r10_conv_started) == 1 && !atomic_load(&r10_conv_done)) {
        for (int64_t t0 = now_ms(); now_ms() - t0 < (int64_t)r10_convert_timeout_s * 1000LL; hold_ms(250)) {
            r10_poll_conv();
            if (atomic_load(&r10_conv_done))
                break;
        }
    }
    struct meta_volume mv;
    int have_mv = (r9_vol_rec(&mv) == 0);
    if (!atomic_load(&r10_conv_done)) {
        pthread_mutex_lock(&r10_mu);
        if (!r10_why_not[0]) {
            if (have_mv) {
                uint64_t old_objs = 0;
                for (uint32_t i = 0; i < mv.nlive_sv && i < META_LIVE_SV_MAX; i++)
                    if (mv.live_sv[i].stripe_version < mv.conv.new_stripe_version)
                        old_objs += mv.live_sv[i].count;
                snprintf(r10_why_not, sizeof r10_why_not,
                         "conversion %s did not finish within %d s after last heal (stuck in phase=%s, red=%u %u+%u, "
                         "nstripe=%" PRIu32 ", nrep=%" PRIu32 ", nisr=%" PRIu32 ", mixed=%u, old_objs=%" PRIu64
                         ", sv=%" PRIu64 "/%" PRIu64 "; dropearly?)",
                         r10_spec, r10_convert_timeout_s, meta_conv_phase_name(mv.conv.phase), mv.redundancy, mv.ec_k,
                         mv.ec_m, mv.nstripe, mv.nrep, mv.nisr, mv.mixed_redundancy, old_objs, mv.live_sv_at,
                         mv.conv.new_stripe_version);
            } else {
                snprintf(r10_why_not, sizeof r10_why_not,
                         "conversion %s did not finish within %d s after last heal (no kvr metadata view)", r10_spec,
                         r10_convert_timeout_s);
            }
            fprintf(stderr, "jepsen-cluster: FAIL: r10: %s\n", r10_why_not);
            r10_end_bad++;
        }
        pthread_mutex_unlock(&r10_mu);
        /* Keep rep_n aligned with actual non-learner replicas so final checks do not crash on stale rep_n. */
        if (have_mv && mv.nrep >= 1 && mv.nrep <= META_REPLICAS_MAX) {
            rep_n = (int)mv.nrep;
            (void)rep_sync_members();
        }
        return;
    }

    /* Conversion finished: align rep_n and rep_nodes[] with the target replica count (e.g. 3 -> 4 for 4+2 -> 4+3). */
    rep_n = (int)r10_dst.replicas;
    if (!rep_sync_members()) {
        fprintf(stderr, "jepsen-cluster: FAIL: r10: could not sync kvr's %d final replicas after %s\n", rep_n,
                r10_spec);
        pthread_mutex_lock(&r10_mu);
        r10_end_bad++;
        pthread_mutex_unlock(&r10_mu);
        return;
    }

    /* For replicas -> ec, wait briefly after conversion finishes so at least 1 .ec part settles under the new geometry. */
    if (r10_src.redundancy == META_RED_REPLICAS && r10_dst.redundancy == META_RED_EC) {
        int nec = r10_count_kvr_ec_parts();
        for (int64_t t0 = now_ms(); nec == 0 && now_ms() - t0 < 25000; hold_ms(250))
            nec = r10_count_kvr_ec_parts();
        if (nec == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r10: %s completed, but 0 .ec parts settled within 25 s\n", r10_spec);
            pthread_mutex_lock(&r10_mu);
            r10_end_bad++;
            pthread_mutex_unlock(&r10_mu);
        }
    }
}

/* Verify on-disk .ec files after clean stop match target redundancy (geometry (1..k)+m for EC, 0 live .ec for replicas). */
static int r10_post_stop_check(const char *const *dirs, int nd)
{
    if (!r10_convert_on || !atomic_load(&r10_conv_done) || nd <= 0)
        return 0;
    int bad = 0;
    int nec = 0;
    uint32_t valid_mask = r10_dst.redundancy == META_RED_EC
                              ? ((uint32_t)1u << ((uint32_t)r10_dst.ec_k + (uint32_t)r10_dst.ec_m)) - 1u
                              : 0u;
    for (int d = 0; d < nd; d++) {
        DIR *dp = opendir(dirs[d]);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            nec++;
            if (r10_dst.redundancy == META_RED_REPLICAS) {
                fprintf(stderr, "jepsen-cluster: FAIL: r10: live .ec part %s/%s remained after %s\n", dirs[d],
                        e->d_name, r10_spec);
                bad++;
                continue;
            }
            char fp[PATH_MAX + 300];
            snprintf(fp, sizeof fp, "%s/%s", dirs[d], e->d_name);
            int fd = open(fp, O_RDONLY | O_CLOEXEC);
            if (fd < 0) {
                fprintf(stderr, "jepsen-cluster: FAIL: r10: cannot open %s: %s\n", fp, strerror(errno));
                bad++;
                continue;
            }
            struct stat st;
            uint8_t tr[64], rhdr[64];
            if (fstat(fd, &st) != 0 || st.st_size < 64 + 216 || pread(fd, tr, 64, st.st_size - 64) != 64 ||
                memcmp(tr, "TSDB-EC\n", 8) != 0) {
                fprintf(stderr, "jepsen-cluster: FAIL: r10: %s has invalid .ec trailer\n", fp);
                close(fd);
                bad++;
                continue;
            }
            uint64_t roff = r10_le64(tr + 16), rlen = r10_le64(tr + 24);
            if (rlen < 216 || roff > (uint64_t)st.st_size || rlen > (uint64_t)st.st_size - roff ||
                roff + rlen + 64 != (uint64_t)st.st_size || pread(fd, rhdr, 64, (off_t)roff) != 64) {
                fprintf(stderr, "jepsen-cluster: FAIL: r10: %s has invalid .ec record header\n", fp);
                close(fd);
                bad++;
                continue;
            }
            close(fd);
            uint16_t k = r10_le16(rhdr + 52), m = r10_le16(rhdr + 54);
            uint32_t pos = r10_le32(rhdr + 60);
            unsigned bits = (unsigned)__builtin_popcount(pos);
            unsigned npos = (unsigned)k + (unsigned)m;
            if (k < 1 || k > r10_dst.ec_k || m != r10_dst.ec_m) {
                fprintf(stderr, "jepsen-cluster: FAIL: r10: %s has geometry %u+%u after %s, want (1..%u)+%u\n", fp, k, m,
                        r10_spec, r10_dst.ec_k, r10_dst.ec_m);
                bad++;
            } else if (pos != 0 && ((pos & ~valid_mask) != 0 || (bits != npos && !(m >= 2 && bits == npos - 1)))) {
                fprintf(stderr, "jepsen-cluster: FAIL: r10: %s has positions 0x%x outside mask 0x%x after %s\n", fp, pos,
                        valid_mask, r10_spec);
                bad++;
            }
        }
        closedir(dp);
    }
    r10_post_ec_parts = nec;
    if (r10_dst.redundancy == META_RED_EC && nec == 0 && ec_settles) {
        fprintf(stderr, "jepsen-cluster: FAIL: r10: 0 .ec parts on kvr's replicas after %s\n", r10_spec);
        bad++;
    }
    return bad;
}

static int r10_end_check(void)
{
    if (!r10_convert_on)
        return 0;
    pthread_mutex_lock(&r10_mu);
    int bad = r10_end_bad;
    pthread_mutex_unlock(&r10_mu);
    return bad;
}

static void r10_summary(void)
{
    if (!r10_convert_on)
        return;
    pthread_mutex_lock(&r10_mu);
    int started = atomic_load(&r10_conv_started);
    int done = atomic_load(&r10_conv_done);
    int64_t elapsed = done ? (r10_conv_done_ms - r10_conv_start_ms) : -1LL;
    unsigned phases = r10_phases_seen;
    int bad = r10_end_bad;
    char why[sizeof r10_why_not];
    memcpy(why, r10_why_not, sizeof why);
    pthread_mutex_unlock(&r10_mu);
    printf("r10: convert %s (at %d s, started=%d, done=%d, elapsed=%lld ms, phases=0x%x, pre_ec=%d, post_ec=%d, bad=%d)%s%s\n",
           r10_spec, r10_convert_at_s, started, done, (long long)elapsed, phases, r10_pre_ec_parts,
           r10_post_ec_parts, bad, why[0] ? " why: " : "", why);
}

#endif
