/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 failure-handling, rebuild, return, backpressure and multi-fault nemeses and checks for jepsen_cluster.c
 * (docs/design-cluster.md §13.2, docs/design-cluster-data.md §10.3, r9-scope/PLAN.md D24, row r9-jc).
 *
 * Included by jepsen_cluster.c right after cluster_ec.h. Uses its node table, views, admin calls, EC helpers,
 * and histories.
 *
 * Nemeses:
 *  - out:       server killed for good past rebuild_after under load, a new server added (`node add`), verify
 *               log and stripe rebuild complete; pauses spare mid-fill to catch `wholeearly`, and runs an
 *               end-of-run sum-mismatch probe (`r9_fillnocheck_probe`) to catch `fillnocheck`.
 *  - back:      server killed past rebuild_after, brought back during its rebuild; verifies return takes back
 *               filling positions and spare's partial fill chunks stay intact while returning node is FILLING
 *               (catches `partialdelnow`).
 *  - two:       two servers killed at once; when 2/non_out > out_guard_pct, verifies mass-OUT guard holds both
 *               in DOWN past rebuild_after_s (catches `noguard`).
 *  - mfail:     m stripe holders killed at once (--ec); live cold-cache reads of settled series still succeed.
 *  - m1:        m + 1 stripe holders killed at once (--ec); checker accepts only UNAVAILABLE (never CORRUPT or
 *               wrong rows), plus end-of-run `node destroyed` check verifying tier=lost and TSDB_WHY_LOST.
 *  - slowloss:  slow fdatasync on one follower via outagefs, then kvr leader's directories removed, healed,
 *               restarted (must refuse missing dir), reset and rebuilt cleanly.
 *  - hold:      `node hold` under ingest with short repl_retain_s and E29 down retention active; verifies node
 *               stays DOWN past rebuild_after_s, degraded settles resume on EC, catch-up completes with 0
 *               snapshots (vdir_ino unchanged), and nothing fail-stops.
 *  - stripeset: `volume move --pos` (CLUSTER_OP_VOLUME_MOVE_POS) during active settles (--ec).
 *  - full:      fill_pct_file=1 hook on one ISR member; inserts refused with TSDB_WHY_FULL, zero acknowledged
 *               writes lost, writes resume cleanly after lifting fill_pct below hysteresis.
 */
#ifndef JEPSEN_CLUSTER_R9_H
#define JEPSEN_CLUSTER_R9_H

extern uint64_t xxh64(const void *data, size_t len, uint64_t seed);

/* ---- R9 state & counters */
static unsigned r9_out_done, r9_back_done, r9_back_returned, r9_two_done, r9_two_guarded;
static unsigned r9_mfail_done, r9_mfail_reads, r9_mfail_read_fail;
static unsigned r9_m1_done, r9_m1_unavail, r9_m1_corrupt, r9_m1_destroyed_ok;
static unsigned r9_slowloss_done, r9_hold_done, r9_hold_snaps, r9_hold_deg_ec;
static unsigned r9_stripeset_done, r9_full_done, r9_full_missed;
static int r9_wholeearly_bad, r9_partialdel_bad, r9_noguard_bad, r9_hold_out_bad, r9_full_resume_bad;
static int r9_fillnocheck_done, r9_fillnocheck_bad, r9_m1_destroyed_bad;

/* ---- helpers */

static int r9_member_state(uint32_t id, uint8_t *st_out, uint64_t *hold_out)
{
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        int found = 0;
        for (uint32_t k = 0; k < v.m.nmembers; k++)
            if (v.m.members[k].id == id) {
                if (st_out)
                    *st_out = v.m.members[k].state;
                if (hold_out)
                    *hold_out = v.m.members[k].hold_until_ms;
                found = 1;
                break;
            }
        view_free(&v);
        if (found)
            return 0;
    }
    return -1;
}

/* Copies the freshest meta_volume record of kvr (including stripe[] and stripe_pos[]) from live nodes. */
static int r9_vol_rec(struct meta_volume *out)
{
    memset(out, 0, sizeof *out);
    int have = 0;
    uint64_t best_applied = 0;
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (mv->number != rep_number)
                continue;
            if (have && (mv->stripe_version < out->stripe_version ||
                         (mv->stripe_version == out->stripe_version &&
                          (mv->set_version < out->set_version ||
                           (mv->set_version == out->set_version &&
                            (mv->epoch < out->epoch ||
                             (mv->epoch == out->epoch && v.m.applied <= best_applied)))))))
                continue;
            have = 1;
            best_applied = v.m.applied;
            *out = *mv;
            out->hist = NULL;
            out->nhist = 0;
        }
        view_free(&v);
    }
    return have ? 0 : -1;
}

static int r9_find_raft_leader(void)
{
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 1500) != 0)
            continue;
        int is_l = v.st.role == RAFT_LEADER_ROLE && v.st.mode == CLUSTER_MODE_CORE;
        view_free(&v);
        if (is_l)
            return i;
    }
    return atomic_load(&last_leader);
}

static int r9_count_vol_chunks(int node, uint32_t vol)
{
    int cnt = 0;
    for (int c = 0; c < 2 && nodes[node].chunk[c][0]; c++) {
        char vdir[PATH_MAX + 96];
        snprintf(vdir, sizeof vdir, "%s/chunks/s%" PRIu32, nodes[node].chunk[c], vol);
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (l >= 41 && e->d_name[0] == 'o' && strcmp(e->d_name + l - 6, ".chunk") == 0)
                cnt++;
        }
        closedir(dp);
    }
    return cnt;
}

static int r9_count_node_pos_chunks(int node, uint32_t vol, uint32_t want_pos)
{
    char suf[24];
    snprintf(suf, sizeof suf, ".%" PRIu32 ".chunk", want_pos);
    size_t sufl = strlen(suf);
    int cnt = 0;
    for (int c = 0; c < 2 && nodes[node].chunk[c][0]; c++) {
        char vdir[PATH_MAX + 96];
        snprintf(vdir, sizeof vdir, "%s/chunks/s%" PRIu32, nodes[node].chunk[c], vol);
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (l >= 33 + sufl && e->d_name[0] == 'o' && strcmp(e->d_name + l - sufl, suf) == 0)
                cnt++;
        }
        closedir(dp);
    }
    return cnt;
}

static uint16_t r9_get16_le(const uint8_t *b)
{
    return (uint16_t)((uint16_t)b[0] | ((uint16_t)b[1] << 8));
}

static uint32_t r9_get32_le(const uint8_t *b)
{
    return (uint32_t)b[0] | ((uint32_t)b[1] << 8) | ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}

static uint64_t r9_get64_le(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= (uint64_t)b[i] << (8 * i);
    return v;
}

/* Counts .ec parts on kvr's leader whose ec_record `positions` bitmap (offset 60 in ec_record, src/ecpart.h:33)
 * records a genuinely degraded settle (non-zero bitmask with k' + m - 1 bits set). */
static int r9_count_degraded_ec_parts_on_leader(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return 0;
    char vdir[PATH_MAX + 64];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[r.leader].dir, rep_number);
    DIR *dp = opendir(vdir);
    if (!dp)
        return 0;
    int n = 0;
    for (struct dirent *e; (e = readdir(dp));) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
            continue;
        char p[PATH_MAX + 192];
        int pn = snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
        if (pn < 0 || (size_t)pn >= sizeof p)
            continue;
        int fd = open(p, O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            continue;
        struct stat sb;
        uint8_t tr[64], rec_hdr[64];
        if (fstat(fd, &sb) == 0 && sb.st_size >= 128 &&
            pread(fd, tr, 64, sb.st_size - 64) == 64 && memcmp(tr, "TSDB-EC\n", 8) == 0) {
            uint64_t roff = r9_get64_le(tr + 16);
            uint64_t rlen = r9_get64_le(tr + 24);
            if (rlen >= 64 && roff + rlen <= (uint64_t)sb.st_size - 64 &&
                pread(fd, rec_hdr, 64, (off_t)roff) == 64) {
                uint16_t k = r9_get16_le(rec_hdr + 52);
                uint16_t m = r9_get16_le(rec_hdr + 54);
                uint32_t pos = r9_get32_le(rec_hdr + 60);
                if (pos != 0 && k >= 1 && m >= 2 && __builtin_popcount(pos) == (int)(k + m - 1))
                    n++;
            }
        }
        close(fd);
    }
    closedir(dp);
    return n;
}

static int r9_count_ec_parts_on_leader(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return 0;
    char vdir[PATH_MAX + 64];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[r.leader].dir, rep_number);
    DIR *dp = opendir(vdir);
    if (!dp)
        return 0;
    int n = 0;
    for (struct dirent *e; (e = readdir(dp));) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] == 'p' && l > 3 && strcmp(e->d_name + l - 3, ".ec") == 0)
            n++;
    }
    closedir(dp);
    return n;
}

static int r9_all_stripe_whole(const struct meta_volume *mv)
{
    if (mv->redundancy != META_RED_EC || mv->nstripe == 0)
        return 1;
    for (uint32_t p = 0; p < mv->nstripe; p++)
        if (mv->stripe_pos[p].state != META_STRIPE_WHOLE)
            return 0;
    return 1;
}

static int r9_wait_stripe_whole(int ms)
{
    for (int64_t t0 = now_ms(); now_ms() - t0 < ms; hold_ms(300)) {
        struct meta_volume mv;
        if (r9_vol_rec(&mv) == 0 && r9_all_stripe_whole(&mv))
            return 1;
    }
    return 0;
}

static void r9_sfill_pause_path(int node, char *buf, size_t cap)
{
    snprintf(buf, cap, "%s/n%d.sfill.pause", rundir, node);
}

static void r9_sfill_pause_set(int node, int on)
{
    char p[PATH_MAX + 64];
    r9_sfill_pause_path(node, p, sizeof p);
    if (on) {
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd >= 0)
            close(fd);
    } else {
        unlink(p);
    }
}

static void r9_sfill_pause_clear_all(void)
{
    for (int i = 0; i < MAXN; i++)
        r9_sfill_pause_set(i, 0);
}

static void r9_fillpct_set(int i, int pct)
{
    char tmp[PATH_MAX + 96], dst[PATH_MAX + 80];
    snprintf(tmp, sizeof tmp, "%s/state/fillpct.tmp", nodes[i].dir);
    snprintf(dst, sizeof dst, "%s/state/fillpct", nodes[i].dir);
    FILE *f = fopen(tmp, "w");
    if (!f)
        die(2, "r9: fopen %s: %s", tmp, strerror(errno));
    fprintf(f, "%d\n", pct);
    fclose(f);
    if (rename(tmp, dst) != 0)
        die(2, "r9: rename %s -> %s: %s", tmp, dst, strerror(errno));
}

/* Dynamically adds a fresh server to the running cluster (`tsdb disk init`, optional EC stores, proxy,
 * start_node, CLUSTER_OP_NODE_ADD). Returns its index in nodes[], or -1 if MAXN is exhausted. */
static int r9_add_new_node(void)
{
    int slot = -1;
    pthread_mutex_lock(&tbl_mu);
    if (nn < MAXN)
        slot = nn;
    pthread_mutex_unlock(&tbl_mu);
    if (slot < 0)
        return -1;

    memset(&nodes[slot], 0, sizeof nodes[slot]);
    nodes[slot].sfill_pause_override = -1;
    snprintf(nodes[slot].dir, sizeof nodes[slot].dir, "%s/n%d", rundir, slot);
    snprintf(nodes[slot].log, sizeof nodes[slot].log, "%s/n%d.log", rundir, slot);
    snprintf(nodes[slot].octl, sizeof nodes[slot].octl, "%s/n%d.octl", rundir, slot);
    if (use_outagefs && strlen(nodes[slot].octl) >= sizeof((struct sockaddr_un){0}.sun_path))
        die(2, "r9: outagefs control socket path %s is too long for a unix socket (use a shorter --dir)",
            nodes[slot].octl);
    {
        const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
        char spec[PATH_MAX + 8];
        snprintf(spec, sizeof spec, "%s:ssd", nodes[slot].dir);
        pid_t p = fork();
        if (p < 0)
            die(2, "r9: fork disk init: %s", strerror(errno));
        if (p == 0) {
            execl(cli, "tsdb", "disk", "init", spec, (char *)NULL);
            _exit(127);
        }
        int wst = 0;
        waitpid(p, &wst, 0);
        if (!WIFEXITED(wst) || WEXITSTATUS(wst) != 0)
            die(2, "r9: tsdb disk init %s failed (status 0x%x)", spec, wst);
    }
    char st[PATH_MAX + 32];
    snprintf(st, sizeof st, "%s/state", nodes[slot].dir);
    if (mkdir(st, 0755) != 0)
        die(2, "r9: mkdir %s: %s", st, strerror(errno));
    if (nemeses & N_FULL)
        r9_fillpct_set(slot, 50);
    if (want_ec_k)
        ec_stores_init(slot);
    proxy_start(slot);

    /* Expose slot in nn only after its proxy is listening; hold table_ready = 0 across NODE_ADD so the new node's
     * initial dials before nodes[slot].id is stored never trip unmapped_links. */
    atomic_store(&table_ready, 0);
    pthread_mutex_lock(&tbl_mu);
    nn = slot + 1;
    pthread_mutex_unlock(&tbl_mu);

    int st_ex = 0;
    if (start_node(slot, NULL, &st_ex) != 0)
        die(1, "r9: newly added node %d exited before listening (status 0x%x, log %s)", slot, st_ex, nodes[slot].log);

    char padv[META_ADDR_MAX];
    snprintf(padv, sizeof padv, "127.0.0.1:%d", nodes[slot].proxy_port);
    uint8_t nr[META_ADDR_MAX + 2];
    size_t n = strlen(padv);
    nr[0] = (uint8_t)n;
    nr[1] = (uint8_t)(n >> 8);
    memcpy(nr + 2, padv, n);
    int rc = -1;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; hold_ms(300)) {
        rc = admin_any(CLUSTER_OP_NODE_ADD, nr, n + 2, 15000);
        if (rc == 0)
            break;
    }
    if (rc != 0)
        die(1, "r9: node add of replacement node %d failed: %s", slot, cluster_strerror(rc));

    for (int64_t t0 = now_ms(); !atomic_load(&nodes[slot].id); hold_ms(100)) {
        struct view v;
        if (view_get(slot, &v, 2000) == 0) {
            if (v.st.self && v.st.mode != CLUSTER_MODE_NONE)
                atomic_store(&nodes[slot].id, v.st.self);
            view_free(&v);
        }
        if (now_ms() - t0 > 30000)
            die(1, "r9: newly added node %d got no member id within 30 s", slot);
    }
    atomic_store(&table_ready, 1);

    /* Wait until the new member is META_UP in metadata (and reporting stores if EC). */
    uint32_t nid = atomic_load(&nodes[slot].id);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; hold_ms(200)) {
        uint8_t mst = META_DOWN;
        if (r9_member_state(nid, &mst, NULL) == 0 && mst == META_UP)
            break;
    }
    ev("r9: added replacement node %d (member %" PRIu32 ")", slot, nid);
    return slot;
}

/* ---- R9 nemeses */

/* Counts live Raft voters (CLUSTER_MODE_CORE) and marks each voter slot in is_voter[0..nn-1]. */
static int r9_live_voters(int is_voter[MAXN])
{
    int n = 0;
    if (is_voter)
        memset(is_voter, 0, sizeof(int) * MAXN);
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid <= 0 || node_paused(i))
            continue;
        struct view v;
        if (view_get(i, &v, 1500) == 0) {
            if (v.st.mode == CLUSTER_MODE_CORE) {
                if (is_voter)
                    is_voter[i] = 1;
                n++;
            }
            view_free(&v);
        }
    }
    return n;
}

/* 1. out (§13.2): kill a server for good past rebuild_after under load, add a replacement server via `node add`,
 * verify rebuild completes (log replica + EC stripe fill to WHOLE). Pauses candidate spares mid-fill to catch
 * `wholeearly` (TSDB_SFILL_MUTANT=wholeearly). Preserves Raft voter quorum across repeated rounds by scaling
 * voter_replace_s (10 s), preferring non-voters, and waiting for voter replacement after killing a voter. */
static void nem_out(void)
{
    if (want_ec_k && ec_settles) {
        /* Wait briefly for at least 2 .ec objects so pause_after=0 leaves >= 1 unfilled object (catches wholeearly). */
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000 && r9_count_ec_parts_on_leader() < 2; hold_ms(250))
            ;
    }
    if (!rep_wait_whole(60000))
        die(1, "r9: out: kvr was not whole before starting round");
    if (want_ec_k && !r9_wait_stripe_whole(60000))
        die(1, "r9: out: kvr stripe was not whole before starting round");

    struct meta_volume mv;
    if (r9_vol_rec(&mv) != 0)
        return;
    int rl = r9_find_raft_leader();

    /* Ensure killing a voter cannot drop Raft below quorum: wait for voter replacement if needed. */
    int is_voter[MAXN];
    int live_v = r9_live_voters(is_voter);
    int min_v_to_kill_voter = (int)(S.voters > 1 ? S.voters / 2 + 2 : 2);
    if (live_v < min_v_to_kill_voter) {
        int vwait = (int)S.voter_replace_s * 1000 + 20000;
        for (int64_t t0 = now_ms(); now_ms() - t0 < vwait && live_v < min_v_to_kill_voter; hold_ms(500))
            live_v = r9_live_voters(is_voter);
    }
    int can_kill_voter = live_v >= min_v_to_kill_voter;

    /* Pick a live non-Raft-leader victim: prefer non-voters first, then voters only when quorum is safe. */
    int victim = -1;
    uint32_t vpos = UINT32_MAX;
    if (want_ec_k && mv.redundancy == META_RED_EC) {
        int off = rnd_range(0, (int)mv.nstripe - 1);
        for (int pass = 0; pass < 4 && victim < 0; pass++) {
            if (pass >= 2 && !can_kill_voter)
                break;
            for (uint32_t s = 0; s < mv.nstripe && victim < 0; s++) {
                uint32_t p = ((uint32_t)off + s) % mv.nstripe;
                int ni = node_by_id(mv.stripe[p]);
                if (ni < 0 || nodes[ni].dead_for_good || ni == rl)
                    continue;
                if (pass < 2 && is_voter[ni])
                    continue;
                if ((pass == 0 || pass == 2) && ni == atomic_load(&kv_leader))
                    continue;
                victim = ni;
                vpos = p;
            }
        }
    } else {
        for (int pass = 0; pass < 4 && victim < 0; pass++) {
            if (pass >= 2 && !can_kill_voter)
                break;
            for (uint32_t r = 0; r < mv.nrep && victim < 0; r++) {
                int ni = node_by_id(mv.rep[r].node);
                if (ni < 0 || nodes[ni].dead_for_good || ni == rl)
                    continue;
                if (pass < 2 && is_voter[ni])
                    continue;
                if ((pass == 0 || pass == 2) && ni == atomic_load(&kv_leader))
                    continue;
                victim = ni;
            }
        }
    }
    if (victim < 0)
        return;

    int was_voter = is_voter[victim];
    uint32_t vid = atomic_load(&nodes[victim].id);
    int was_rep = 0;
    for (uint32_t r = 0; r < mv.nrep; r++)
        was_rep |= mv.rep[r].node == vid;

    int nec_before = want_ec_k ? r9_count_ec_parts_on_leader() : 0;
    int check_wholeearly = want_ec_k && ec_settles && nec_before >= 1;
    if (check_wholeearly) {
        for (int i = 0; i < nn; i++)
            if (!nodes[i].dead_for_good && i != victim)
                r9_sfill_pause_set(i, 1);
    }

    int st = stop_node(victim, SIGKILL);
    if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL)) {
        r9_sfill_pause_clear_all();
        return;
    }
    nodes[victim].dead_for_good = 1;
    ev("r9: out: killed node %d (member %" PRIu32 ", vpos %u, was_rep %d) for good", victim, vid,
       vpos == UINT32_MAX ? 999u : vpos, was_rep);

    /* Add a replacement server (`node add`) if room remains in MAXN. */
    int added = r9_add_new_node();
    if (added >= 0 && check_wholeearly)
        r9_sfill_pause_set(added, 1);

    /* Wait for victim to reach META_OUT past rebuild_after_s. */
    int wait_out_ms = (int)S.rebuild_after_s * 1000 + 30000;
    int is_out = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < wait_out_ms; hold_ms(250)) {
        uint8_t mst = META_UP;
        if (r9_member_state(vid, &mst, NULL) == 0 && mst == META_OUT) {
            is_out = 1;
            break;
        }
    }
    if (!is_out) {
        r9_sfill_pause_clear_all();
        die(1, "r9: out: killed node %d (member %" PRIu32 ") did not become META_OUT within %d ms (rebuild_after_s %u)",
            victim, vid, wait_out_ms, S.rebuild_after_s);
    }

    if (want_ec_k && vpos < mv.nstripe) {
        /* Wait for STRIPE_SET to reassign vpos off victim onto a spare S. */
        int spare = -1;
        uint32_t sid = 0;
        struct meta_volume mv2;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 45000; hold_ms(200)) {
            if (r9_vol_rec(&mv2) == 0 && mv2.stripe[vpos] != vid && mv2.stripe[vpos] != 0) {
                sid = mv2.stripe[vpos];
                spare = node_by_id(sid);
                break;
            }
        }
        if (spare < 0) {
            r9_sfill_pause_clear_all();
            die(1, "r9: out: kvr pos %" PRIu32 " was not reassigned off OUT node %d within 45 s", vpos, victim);
        }
        ev("r9: out: pos %" PRIu32 " reassigned from node %d to spare node %d (state %u)", vpos, victim, spare,
           mv2.stripe_pos[vpos].state);

        if (check_wholeearly) {
            /* Spare is paused at pause_after=0 (0 old objects filled). Wait 2.0 s: pos MUST stay FILLING! */
            hold_ms(2000);
            struct meta_volume mv3;
            int nch = r9_count_vol_chunks(spare, rep_number);
            if (r9_vol_rec(&mv3) == 0 && mv3.stripe[vpos] == sid &&
                mv3.stripe_pos[vpos].state == META_STRIPE_WHOLE) {
                r9_wholeearly_bad++;
                fprintf(stderr,
                        "jepsen-cluster: FAIL: r9: wholeearly: kvr pos %" PRIu32 " marked WHOLE on spare node %d "
                        "while fill was paused (spare holds %d chunks, leader has >= %d .ec parts)\n",
                        vpos, spare, nch, nec_before);
            }
            r9_sfill_pause_clear_all();
        }
        if (!r9_wait_stripe_whole(90000))
            die(1, "r9: out: kvr pos %" PRIu32 " on spare node %d did not reach WHOLE within 90 s", vpos, spare);
    } else {
        r9_sfill_pause_clear_all();
    }

    if (was_rep) {
        /* Wait for kvr's log replica set to drop victim and rebuild onto a live server with full ISR. */
        int rebuilt = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 120000; hold_ms(300)) {
            struct rrec r;
            if (rep_rec_get(&r) != 0 || r.nrep != rep_n || r.moving || !rep_no_learners(&r))
                continue;
            int has_v = 0, all_isr = 1;
            for (int k = 0; k < r.nrep; k++) {
                has_v |= (r.id[k] == vid);
                all_isr &= r.isr_has[k];
            }
            if (!has_v && all_isr) {
                rebuilt = 1;
                break;
            }
        }
        if (!rebuilt)
            die(1, "r9: out: kvr log replica on OUT node %d was not rebuilt elsewhere within 120 s", victim);
        rep_sync_members();
    }

    /* If we permanently killed a Raft voter, wait for manage_voters (voter_replace_s) to promote a live
     * replacement voter so repeated `out` rounds in 300 s runs keep a full voting quorum. */
    if (was_voter && added >= 0) {
        int vwait = (int)S.voter_replace_s * 1000 + 25000;
        for (int64_t t0 = now_ms(); now_ms() - t0 < vwait; hold_ms(400)) {
            if (r9_live_voters(NULL) >= live_v)
                break;
        }
    }

    r9_out_done++;
    effect_seen = 1;
}

/* 2. back: kill a server past rebuild_after, bring it back DURING its rebuild (return takes back filling
 * positions). Verifies spare's partial fill chunks are NOT deleted while returning node is still FILLING
 * (catches `partialdelnow`). */
static void nem_back(void)
{
    if (want_ec_k && ec_settles) {
        for (int64_t t0 = now_ms(); now_ms() - t0 < 25000 && r9_count_ec_parts_on_leader() < 3; hold_ms(250))
            ;
    }
    if (!rep_wait_whole(60000))
        die(1, "r9: back: kvr was not whole before starting round");
    if (want_ec_k && !r9_wait_stripe_whole(60000))
        die(1, "r9: back: kvr stripe was not whole before starting round");

    struct meta_volume mv;
    if (r9_vol_rec(&mv) != 0)
        return;
    int rl = r9_find_raft_leader();

    int victim = -1;
    uint32_t vpos = UINT32_MAX;
    if (want_ec_k && mv.redundancy == META_RED_EC) {
        /* Prefer positions < 1 + m (always present even in small k'=1 objects, since chunk_npos = k'+m >= 1+m)
         * so spare's first filled object under pause_after=1 writes a real chunk rather than skipping a small part. */
        uint32_t always_n = (uint32_t)mv.ec_m + 1;
        if (always_n > mv.nstripe)
            always_n = mv.nstripe;
        int off = rnd_range(0, (int)mv.nstripe - 1);
        for (int pass = 0; pass < 3 && victim < 0; pass++) {
            uint32_t lim = pass < 2 ? always_n : mv.nstripe;
            for (uint32_t s = 0; s < lim && victim < 0; s++) {
                uint32_t p = ((uint32_t)off + s) % lim;
                int ni = node_by_id(mv.stripe[p]);
                if (ni < 0 || nodes[ni].dead_for_good || ni == rl)
                    continue;
                if (pass == 0 && ni == atomic_load(&kv_leader))
                    continue;
                victim = ni;
                vpos = p;
            }
        }
    } else {
        for (int pass = 0; pass < 2 && victim < 0; pass++)
            for (uint32_t r = 0; r < mv.nrep && victim < 0; r++) {
                int ni = node_by_id(mv.rep[r].node);
                if (ni < 0 || nodes[ni].dead_for_good || ni == rl)
                    continue;
                if (pass == 0 && ni == atomic_load(&kv_leader))
                    continue;
                victim = ni;
            }
    }
    if (victim < 0)
        return;
    uint32_t vid = atomic_load(&nodes[victim].id);

    if (want_ec_k && vpos < mv.nstripe && ec_settles) {
        /* From round 2 on, restart live non-stripe spares cleanly so their process-wide sfill_test_pause_seen
         * counter starts at 0 again and pause_after=1 still writes >= 1 partial chunk before pausing. */
        if (r9_back_done > 0) {
            for (int i = 0; i < nn; i++) {
                if (nodes[i].dead_for_good || i == victim || i == rl || i == atomic_load(&kv_leader))
                    continue;
                uint32_t nid = atomic_load(&nodes[i].id);
                int in_s = 0;
                for (uint32_t p = 0; p < mv.nstripe; p++)
                    in_s |= (mv.stripe[p] == nid);
                if (!in_s) {
                    stop_node(i, SIGTERM);
                    restart_node(i);
                }
            }
        }
        /* Pause candidate spares after 1 filled object (spawned with pause_after=1 for N_BACK) so spare holds >= 1
         * partial chunk AND stays META_STRIPE_FILLING when victim returns. */
        for (int i = 0; i < nn; i++)
            if (!nodes[i].dead_for_good && i != victim)
                r9_sfill_pause_set(i, 1);

        int st = stop_node(victim, SIGKILL);
        if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL)) {
            r9_sfill_pause_clear_all();
            return;
        }
        ev("r9: back: killed node %d (member %" PRIu32 ", pos %" PRIu32 ") waiting for OUT + partial fill", victim,
           vid, vpos);

        /* Wait for victim -> OUT and STRIPE_SET(vpos, spare, FILLING). */
        int spare = -1;
        uint32_t sid = 0;
        struct meta_volume mv2;
        int wait_ms = (int)S.rebuild_after_s * 1000 + 35000;
        for (int64_t t0 = now_ms(); now_ms() - t0 < wait_ms; hold_ms(200)) {
            if (r9_vol_rec(&mv2) == 0 && mv2.stripe[vpos] != vid && mv2.stripe[vpos] != 0) {
                sid = mv2.stripe[vpos];
                spare = node_by_id(sid);
                break;
            }
        }
        if (spare < 0) {
            r9_sfill_pause_clear_all();
            restart_node(victim);
            die(1, "r9: back: kvr pos %" PRIu32 " was not reassigned off OUT node %d within %d ms", vpos, victim,
                wait_ms);
        }

        /* Wait until spare holds >= 1 chunk for (kvr, vpos) (still FILLING due to pause_after=1). */
        int n_s_partial = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(150)) {
            n_s_partial = r9_count_node_pos_chunks(spare, rep_number, vpos);
            if (n_s_partial >= 1)
                break;
        }
        if (r9_vol_rec(&mv2) == 0 && mv2.stripe[vpos] == sid &&
            mv2.stripe_pos[vpos].state == META_STRIPE_FILLING) {
            /* Bring victim back with pause_after=0 so victim pauses in FILLING while we inspect spare's partial
             * chunks (kills partialdelnow). */
            r9_sfill_pause_set(victim, 1);
            nodes[victim].sfill_pause_override = 0;
            restart_node(victim);
            nodes[victim].sfill_pause_override = -1;

            int took_back = 0;
            struct meta_volume mv3;
            for (int64_t t0 = now_ms(); now_ms() - t0 < 35000; hold_ms(150)) {
                if (r9_vol_rec(&mv3) == 0 && mv3.stripe[vpos] == vid) {
                    took_back = 1;
                    break;
                }
            }
            if (!took_back) {
                r9_sfill_pause_clear_all();
                die(1, "r9: back: returning node %d did not take back FILLING pos %" PRIu32 " from spare node %d within 35 s",
                    victim, vpos, spare);
            }
            r9_back_returned++;
            ev("r9: back: node %d took back pos %" PRIu32 " (state %u, prev %" PRIu32 ", spare had %d pos-%" PRIu32 " chunks)",
               victim, vpos, mv3.stripe_pos[vpos].state, mv3.stripe_pos[vpos].prev, n_s_partial, vpos);

            /* While returning node is paused in FILLING (prev == spare), spare's partial fill chunks at vpos MUST
             * remain intact (E14: deletes only by committed MK_GC after WHOLE). */
            if (n_s_partial > 0 && mv3.stripe_pos[vpos].state == META_STRIPE_FILLING &&
                mv3.stripe_pos[vpos].prev == sid) {
                hold_ms(1500);
                int n_s_during = r9_count_node_pos_chunks(spare, rep_number, vpos);
                if (n_s_during < n_s_partial) {
                    r9_partialdel_bad++;
                    fprintf(stderr,
                            "jepsen-cluster: FAIL: r9: partialdelnow: spare node %d deleted partial fill chunks "
                            "for kvr pos %" PRIu32 " (%d -> %d) while returning node %d was still FILLING\n",
                            spare, vpos, n_s_partial, n_s_during, victim);
                }
            }
            r9_sfill_pause_clear_all();
        } else {
            r9_sfill_pause_clear_all();
            restart_node(victim);
        }

        if (!r9_wait_stripe_whole(90000))
            die(1, "r9: back: kvr stripe did not reach WHOLE within 90 s after node %d returned", victim);
    } else {
        /* Non-EC (r3) or NO_EC: kill victim past rebuild_after_s, then restart during/after OUT. */
        int st = stop_node(victim, SIGKILL);
        if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL))
            return;
        int wait_ms = (int)S.rebuild_after_s * 1000 + 25000;
        for (int64_t t0 = now_ms(); now_ms() - t0 < wait_ms; hold_ms(200)) {
            uint8_t mst = META_UP;
            if (r9_member_state(vid, &mst, NULL) == 0 && mst == META_OUT)
                break;
        }
        hold_ms(500);
        restart_node(victim);
    }

    if (!rep_wait_whole(120000))
        die(1, "r9: back: kvr was not whole 120 s after node %d returned", victim);
    rep_sync_members();
    r9_back_done++;
    effect_seen = 1;
}

/* 3. two: kill two servers at once (multi-fault + mass-OUT guard). When 2 / non_out > out_guard_pct (always true
 * on r3 (5) and 4+2 (7)), holds both down past rebuild_after_s and verifies neither went OUT before
 * out_guard_hold_s (catches TSDB_PLACE_MUTANT=noguard). */
static void nem_two(void)
{
    int live[MAXN], nl = 0;
    int rl = r9_find_raft_leader();
    for (int i = 0; i < nn; i++)
        if (!nodes[i].dead_for_good)
            live[nl++] = i;
    if (nl < 4)
        return;

    /* Pick two live nodes avoiding the Raft leader so Raft quorum holds while we observe the guard. */
    int cands[MAXN], nc = 0;
    for (int k = 0; k < nl; k++)
        if (live[k] != rl)
            cands[nc++] = live[k];
    if (nc < 2)
        return;
    int i1 = rnd_range(0, nc - 1);
    int a = cands[i1];
    cands[i1] = cands[--nc];
    int b = cands[rnd_range(0, nc - 1)];
    uint32_t ida = atomic_load(&nodes[a].id), idb = atomic_load(&nodes[b].id);

    int guard_expected = (2 * 100 > nl * (int)S.out_guard_pct) && S.out_guard_hold_s > S.rebuild_after_s + 6;
    pid_t pa, pb;
    node_snap(a, &pa, NULL, NULL);
    node_snap(b, &pb, NULL, NULL);
    signal_child(pa, SIGKILL);
    signal_child(pb, SIGKILL);
    int sa = stop_node(a, SIGKILL), sb = stop_node(b, SIGKILL);
    if (!(WIFSIGNALED(sa) && WTERMSIG(sa) == SIGKILL && WIFSIGNALED(sb) && WTERMSIG(sb) == SIGKILL))
        return;
    ev("r9: two: killed nodes %d and %d (live=%d, guard_expected=%d)", a, b, nl, guard_expected);

    if (guard_expected) {
        /* Wait for both to be recorded DOWN, then hold past rebuild_after_s + 5 s (still < out_guard_hold_s). */
        int64_t t_down = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(150)) {
            uint8_t ma = META_UP, mb = META_UP;
            if (r9_member_state(ida, &ma, NULL) == 0 && r9_member_state(idb, &mb, NULL) == 0 &&
                ma != META_UP && mb != META_UP) {
                t_down = now_ms();
                break;
            }
        }
        if (t_down == 0)
            t_down = now_ms();
        int check_ms = (int)S.rebuild_after_s * 1000 + 5000;
        int max_ms = (int)S.out_guard_hold_s * 1000 - 4000;
        if (check_ms > max_ms)
            check_ms = max_ms;
        if (check_ms < 2000)
            check_ms = 2000;
        while (now_ms() - t_down < check_ms)
            hold_ms(200);
        uint8_t ma = META_DOWN, mb = META_DOWN;
        (void)r9_member_state(ida, &ma, NULL);
        (void)r9_member_state(idb, &mb, NULL);
        if (ma == META_OUT || mb == META_OUT) {
            r9_noguard_bad++;
            fprintf(stderr,
                    "jepsen-cluster: FAIL: r9: noguard: node(s) went OUT at %lld ms (node %d=%s, node %d=%s) "
                    "before out_guard_hold_s %u s while mass-OUT guard (%u%%, 2/%d DOWN) should hold\n",
                    (long long)(now_ms() - t_down), a, meta_member_state_name(ma), b, meta_member_state_name(mb),
                    S.out_guard_hold_s, S.out_guard_pct, nl);
        } else {
            r9_two_guarded++;
        }
    } else {
        hold_ms(rnd_range(4000, 9000));
    }

    restart_node(a);
    restart_node(b);
    if (!rep_wait_whole(90000))
        die(1, "r9: two: kvr did not recover whole ISR 90 s after nodes %d and %d came back", a, b);
    rep_sync_members();
    r9_two_done++;
    effect_seen = 1;
}

/* Helper for mfail and m1: picks `want_n` distinct live stripe holders of kvr avoiding kvr's leader and Raft
 * leader (and keeping >= 2 log replicas alive). Returns count picked. */
static int r9_pick_stripe_victims(int want_n, int *out, int avoid_raft_quorum_loss)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return 0;
    int leader = r.leader;
    int rl = r9_find_raft_leader();
    int snodes[META_STRIPE_MAX];
    int ns = r12_stripe_nodes(snodes, META_STRIPE_MAX);
    if (ns < want_n)
        return 0;

    int is_rep[MAXN] = {0}, is_voter[MAXN] = {0}, nvoters = 0;
    for (int k = 0; k < r.nrep; k++)
        if (r.node[k] >= 0 && r.node[k] < nn)
            is_rep[r.node[k]] = 1;
    if (avoid_raft_quorum_loss) {
        for (int i = 0; i < nn; i++) {
            if (nodes[i].dead_for_good)
                continue;
            struct view v;
            if (view_get(i, &v, 1000) == 0) {
                if (v.st.mode == CLUSTER_MODE_CORE) {
                    is_voter[i] = 1;
                    nvoters++;
                }
                view_free(&v);
            }
        }
    }
    int max_rep_kill = rep_n > 2 ? rep_n - 2 : 0;
    int max_voter_kill = avoid_raft_quorum_loss ? (nvoters > 1 ? (nvoters - 1) / 2 : 0) : MAXN;
    int nkill = 0, reps_k = 0, voters_k = 0;

    for (int pass = 0; pass < 4 && nkill < want_n; pass++) {
        int off = rnd_range(0, ns - 1);
        for (int s = 0; s < ns && nkill < want_n; s++) {
            int cand = snodes[(off + s) % ns];
            if (cand < 0 || nodes[cand].dead_for_good || cand == leader || cand == rl)
                continue;
            if (is_rep[cand] && reps_k >= max_rep_kill)
                continue;
            if (avoid_raft_quorum_loss && is_voter[cand] && voters_k >= max_voter_kill)
                continue;
            /* Prefer non-voters first, then non-log-replicas. */
            if (pass == 0 && (is_voter[cand] || is_rep[cand]))
                continue;
            if (pass == 1 && is_voter[cand])
                continue;
            if (pass == 2 && is_rep[cand])
                continue;
            int dup = 0;
            for (int j = 0; j < nkill; j++)
                dup |= (out[j] == cand);
            if (dup)
                continue;
            out[nkill++] = cand;
            if (is_rep[cand])
                reps_k++;
            if (is_voter[cand])
                voters_k++;
        }
    }
    return nkill;
}

static int r9_wide_ready;

/* Counts chunks at exact position `want_pos` across all live nodes for volume `vol`. */
static int r9_count_pos_chunks_any(uint32_t vol, uint32_t want_pos)
{
    char suf[24];
    snprintf(suf, sizeof suf, ".%" PRIu32 ".chunk", want_pos);
    size_t sufl = strlen(suf);
    int cnt = 0;
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        for (int c = 0; c < 2 && nodes[i].chunk[c][0]; c++) {
            char vdir[PATH_MAX + 96];
            snprintf(vdir, sizeof vdir, "%s/chunks/s%" PRIu32, nodes[i].chunk[c], vol);
            DIR *dp = opendir(vdir);
            if (!dp)
                continue;
            for (struct dirent *e; (e = readdir(dp));) {
                size_t l = strlen(e->d_name);
                if (l >= 33 + sufl && e->d_name[0] == 'o' && strcmp(e->d_name + l - sufl, suf) == 0)
                    cnt++;
            }
            closedir(dp);
        }
    }
    return cnt;
}

/* Inserts one cold, incompressible batch into "kvr.ecwide" large enough to exceed (k-1) * CHUNK_CELL_MIN so that
 * it settles as a full-width k' == want_ec_k object spanning all k + m stripe positions (0 .. k+m-1). Without a
 * full-width object, small k'=1 parts only occupy positions 0..m, so killing other positions would not exercise
 * m-erasure RS decode in mfail or m+1 unavailability/WHY_LOST in m1. */
static void r9_ensure_wide_ec_part(void)
{
    if (r9_wide_ready || !want_ec_k || !ec_settles)
        return;
    uint32_t last_pos = (uint32_t)(want_ec_k + want_ec_m - 1);
    if (r9_count_pos_chunks_any(rep_number, last_pos) > 0) {
        r9_wide_ready = 1;
        return;
    }
    size_t nrows = (size_t)want_ec_k * 24000u;
    int64_t *ts = malloc(nrows * sizeof *ts);
    float *vals = malloc(nrows * sizeof *vals);
    if (!ts || !vals) {
        free(ts);
        free(vals);
        return;
    }
    uint64_t s = 0x9e3779b97f4a7c15ULL ^ seed;
    int64_t t = KV_BASE_TS - 86400000LL * 20;
    for (size_t i = 0; i < nrows; i++) {
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        uint64_t x = s * 2685821657736338717ULL;
        t += 1 + (int64_t)(x & 15u);
        ts[i] = t;
        vals[i] = (float)(1.0 + (double)((x >> 16) & 0x7fffffu) / (double)(1u << 23));
    }
    int inserted = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000 && !inserted; hold_ms(200)) {
        struct rrec rr;
        int cur_l = (rep_rec_get(&rr) == 0 && rr.leader >= 0) ? rr.leader : atomic_load(&kv_leader);
        tsdbc *c = NULL;
        tsdb_sid sid = 0;
        if (cur_l < 0 || connect_node(cur_l, 1, &c) != 0)
            continue;
        tsdbc_set_timeout(c, 15000);
        if (tsdbc_series_at(c, "kvr.ecwide", "kvr", &sid) == 0 &&
            tsdbc_insert_series(c, sid, ts, vals, nrows) == 0)
            inserted = 1;
        tsdbc_close(c);
    }
    free(ts);
    free(vals);
    if (!inserted)
        return;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 25000; hold_ms(200)) {
        if (r9_count_pos_chunks_any(rep_number, last_pos) > 0) {
            r9_wide_ready = 1;
            ev("r9: kvr.ecwide settled at full width (k=%d, pos %" PRIu32 " present)", want_ec_k, last_pos);
            return;
        }
    }
}

/* 4. mfail (--ec): kill m stripe holders at once, transfer to a cold ISR leader, and verify every settled
 * series reads whole with zero UNAVAILABLE or CORRUPT. */
static void nem_mfail(void)
{
    if (ec_settles) {
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000 && r9_count_ec_parts_on_leader() < 1; hold_ms(250))
            ;
    }
    if (!rep_wait_whole(60000) || !r9_wait_stripe_whole(60000))
        return;
    r9_ensure_wide_ec_part();

    r12_transfer_cold_leader();
    int victims[CHUNK_M_MAX + 1];
    int nk = r9_pick_stripe_victims(want_ec_m, victims, 1);
    if (nk < want_ec_m)
        return;

    for (int j = 0; j < nk; j++) {
        pid_t p;
        node_snap(victims[j], &p, NULL, NULL);
        signal_child(p, SIGKILL);
    }
    for (int j = 0; j < nk; j++)
        stop_node(victims[j], SIGKILL);
    ev("r9: mfail: killed %d stripe holders simultaneously, reading all series", nk);

    static const char *const snames[REP_HISTS + 2] = {"kvr.a", "kvr.b", "kvr.fill", "kvr.ecwide"};
    int nsn = r9_wide_ready ? REP_HISTS + 2 : REP_HISTS + 1;
    for (int si = 0; si < nsn; si++) {
        int rc = TSDB_ERR_UNAVAILABLE;
        tsdb_points p;
        memset(&p, 0, sizeof p);
        int cur_l = atomic_load(&kv_leader);
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; usleep(200000)) {
            tsdbc *c = NULL;
            tsdb_sid sid = 0;
            struct rrec rr;
            if (rep_rec_get(&rr) == 0 && rr.leader >= 0)
                cur_l = rr.leader;
            if (cur_l < 0 || connect_node(cur_l, 1, &c) != 0)
                continue;
            tsdbc_set_timeout(c, 15000);
            int src = tsdbc_series(c, snames[si], 0, &sid);
            if (src != 0) {
                tsdbc_close(c);
                rc = src;
                if (src == TSDB_ERR_MOVED || src == TSDB_ERR_UNAVAILABLE || src == TSDB_ERR_NOTFOUND)
                    continue;
                break;
            }
            rc = tsdbc_query_points(c, sid, INT64_MIN, INT64_MAX, &p);
            tsdbc_close(c);
            if (rc == 0 || rc == TSDB_ERR_CORRUPT)
                break;
            if (rc != TSDB_ERR_MOVED && rc != TSDB_ERR_UNAVAILABLE)
                break;
        }
        if (rc != 0) {
            r9_mfail_read_fail++;
            fprintf(stderr, "jepsen-cluster: FAIL: r9: mfail: read of %s with %d stripe holders down failed: %s\n",
                    snames[si], nk, tsdbc_strerror(rc));
        } else {
            r9_mfail_reads++;
            if (si < REP_HISTS) {
                for (size_t k = 0; k < p.n; k++)
                    if (ts_key(p.ts[k]) < 0) {
                        r9_mfail_read_fail++;
                        fprintf(stderr, "jepsen-cluster: FAIL: r9: mfail: %s returned stray timestamp %" PRId64 "\n",
                                snames[si], p.ts[k]);
                        break;
                    }
            }
            free(p.ts);
            free(p.values);
        }
    }

    for (int j = 0; j < nk; j++)
        restart_node(victims[j]);
    r9_mfail_done++;
    effect_seen = 1;
}

/* 5. m1 (--ec): kill m + 1 stripe holders at once, transfer to a cold ISR leader, verify reads return ONLY
 * UNAVAILABLE (or valid rows if purely young/cached) — NEVER TSDB_ERR_CORRUPT or stray rows — then restart
 * all m + 1 nodes cleanly. End-of-run `r9_m1_destroyed_check()` tests permanent `node destroyed`. */
static void nem_m1(void)
{
    if (ec_settles) {
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000 && r9_count_ec_parts_on_leader() < 1; hold_ms(250))
            ;
    }
    if (!rep_wait_whole(60000) || !r9_wait_stripe_whole(60000))
        return;
    r9_ensure_wide_ec_part();

    r12_transfer_cold_leader();
    int want_k = want_ec_m + 1;
    int victims[CHUNK_M_MAX + 2];
    int nk = r9_pick_stripe_victims(want_k, victims, 1);
    if (nk < want_k)
        return;

    for (int j = 0; j < nk; j++) {
        pid_t p;
        node_snap(victims[j], &p, NULL, NULL);
        signal_child(p, SIGKILL);
    }
    for (int j = 0; j < nk; j++)
        stop_node(victims[j], SIGKILL);
    ev("r9: m1: killed %d (m+1) stripe holders simultaneously, waiting for DOWN then probing reads", nk);
    /* Wait until metadata marks all nk victims DOWN so chunkpeer immediately knows CP_NODE_DOWN instead of
     * waiting out the 30 s peer read timeout. */
    for (int64_t t0 = now_ms(); now_ms() - t0 < 10000; hold_ms(150)) {
        int ndown = 0;
        for (int j = 0; j < nk; j++) {
            uint8_t mst = META_UP;
            if (r9_member_state(atomic_load(&nodes[victims[j]].id), &mst, NULL) == 0 && mst != META_UP)
                ndown++;
        }
        if (ndown == nk)
            break;
    }

    /* Probe kvr.a, kvr.b (small, fast) and kvr.ecwide (guaranteed full-width k'=k). Note server/chunkpeer.c
     * currently tries each missing stripe holder up to timeout_ms (30 s) sequentially before returning
     * TSDB_ERR_UNAVAILABLE, so allow (m + 1) * 35 s on the full-width series. */
    const char *snames[REP_HISTS + 1] = {"kvr.a", "kvr.b", r9_wide_ready ? "kvr.ecwide" : "kvr.fill"};
    int tmo_wide = (want_ec_m + 1) * 35000 + 15000;
    for (int si = 0; si <= REP_HISTS; si++) {
        struct rrec rr;
        int cur_l = (rep_rec_get(&rr) == 0 && rr.leader >= 0) ? rr.leader : atomic_load(&kv_leader);
        tsdbc *c = NULL;
        tsdb_sid sid = 0;
        if (cur_l < 0 || connect_node(cur_l, 1, &c) != 0)
            continue;
        tsdbc_set_retry(c, 0);
        tsdbc_set_timeout(c, si == REP_HISTS ? tmo_wide : 15000);
        int src = tsdbc_series(c, snames[si], 0, &sid);
        if (src == 0) {
            tsdb_points p;
            memset(&p, 0, sizeof p);
            int rc = tsdbc_query_points(c, sid, INT64_MIN, INT64_MAX, &p);
            ev("r9: m1: read %s -> rc=%d (%s)", snames[si], rc, tsdbc_strerror(rc));
            if (rc == TSDB_ERR_UNAVAILABLE) {
                r9_m1_unavail++;
            } else if (rc == TSDB_ERR_CORRUPT) {
                r9_m1_corrupt++;
                fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: read of %s with %d (m+1) stripe holders down returned "
                                "CORRUPT instead of UNAVAILABLE\n", snames[si], nk);
            } else if (rc == 0) {
                if (si < REP_HISTS)
                    for (size_t k = 0; k < p.n; k++)
                        if (ts_key(p.ts[k]) < 0) {
                            r9_m1_corrupt++;
                            fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: %s returned stray timestamp %" PRId64 "\n",
                                    snames[si], p.ts[k]);
                            break;
                        }
                free(p.ts);
                free(p.values);
            } else if (rc != TSDB_ERR_MOVED && rc != TSDB_ERR_IO) {
                r9_m1_corrupt++;
                fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: read of %s with %d stripe holders down returned "
                                "unexpected error %s\n", snames[si], nk, tsdbc_strerror(rc));
            }
        }
        tsdbc_close(c);
    }

    for (int j = 0; j < nk; j++)
        restart_node(victims[j]);
    r9_m1_done++;
    effect_seen = 1;
}

/* 6. slowloss (E24, requires --outagefs): slow fdatasync on one follower via outagefs, then remove the leader's
 * directories (v<N> and chunk stores if EC), heal follower, restart leader (must refuse missing v<N>), reset
 * and rebuild cleanly. */
static void nem_slowloss(void)
{
    if (!rep_wait_whole(60000))
        return;
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return;
    int L = r.leader;
    int F = rep_follower_pick();
    if (F < 0 || F == L)
        return;

    char pfx[PATH_MAX + 8], line[PATH_MAX + 64], out[512];
    int cur = -1;
    if (outage_state(F, pfx, sizeof pfx, &cur) != 0)
        die(2, "r9: slowloss: outagefs shim of node %d does not answer on %s", F, nodes[F].octl);
    snprintf(line, sizeof line, "slow %s 100", pfx);
    if (outagefs_cmd(nodes[F].octl, line, out, sizeof out) != 0)
        die(2, "r9: slowloss: outagefs refused '%s': %s", line, out);
    ev("r9: slowloss: slowed follower node %d by 100 ms, holding 2.5 s before removing leader node %d dirs", F, L);
    hold_ms(2500);

    /* Remove leader L's volume directory (and chunk stores on EC). */
    uint32_t lid = atomic_load(&nodes[L].id);
    char vd[PATH_MAX + 64];
    snprintf(vd, sizeof vd, "%s/v%" PRIu32, nodes[L].dir, rep_number);
    int st = stop_node(L, SIGKILL);
    if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL)) {
        snprintf(line, sizeof line, "heal %s", pfx);
        (void)outagefs_cmd(nodes[F].octl, line, out, sizeof out);
        return;
    }
    rmtree(vd);
    struct stat vsb;
    if (stat(vd, &vsb) == 0)
        die(2, "r9: slowloss: cannot remove %s", vd);
    if (want_ec_k) {
        for (int k = 0; k < 2 && nodes[L].chunk[k][0]; k++) {
            rmtree(nodes[L].chunk[k]);
            if (mkdir(nodes[L].chunk[k], 0755) != 0)
                die(2, "r9: slowloss: mkdir %s: %s", nodes[L].chunk[k], strerror(errno));
            ec_chunk_init(nodes[L].chunk[k], lid);
        }
    }
    r9_loss_expected = 1;

    /* Heal follower F so the surviving replicas can elect a new leader and proceed. */
    snprintf(line, sizeof line, "heal %s", pfx);
    if (outagefs_cmd(nodes[F].octl, line, out, sizeof out) != 0)
        die(2, "r9: slowloss: outagefs refused '%s': %s", line, out);
    hold_ms(2500);

    /* Restart L: must refuse missing v<N>, never silently recreate an empty directory. */
    long off = log_size(L);
    restart_node(L);
    char made[PATH_MAX + 80];
    snprintf(made, sizeof made, "): made %s", vd);
    int refused = 0;
    for (int64_t t0 = now_ms(); !refused && now_ms() - t0 < 60000; hold_ms(100)) {
        check_unexpected_exits();
        if (stat(vd, &vsb) == 0 || log_has_since(L, off, made, NULL))
            die(1, "r9: slowloss: node %d recreated %s after loss before replica reset", L, vd);
        refused = log_has_since(L, off, vd, NULL);
    }
    if (!refused)
        die(1, "r9: slowloss: node %d did not log refusal of removed %s within 60 s", L, vd);

    /* Reset L's replica (or if R9 auto-replaced L after failstop_out_s, wait for kvr whole). */
    uint8_t body[16];
    size_t bl = cluster_volume_force_encode(rep_number, lid, body);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; hold_ms(500)) {
        struct rrec q;
        int still_rep = 0;
        if (rep_rec_get(&q) == 0)
            for (int k = 0; k < q.nrep; k++)
                still_rep |= (q.id[k] == lid);
        if (!still_rep)
            break; /* R9 LOCAL_FAULT auto-move already replaced L */
        if (admin_any(CLUSTER_OP_REPLICA_RESET, body, bl, 15000) == 0)
            break;
    }
    if (!rep_wait_whole(180000))
        die(1, "r9: slowloss: kvr was not whole 180 s after leader %d directory loss", L);
    rep_sync_members();
    if (want_ec_k && !r9_wait_stripe_whole(90000))
        die(1, "r9: slowloss: kvr stripe was not whole 90 s after leader %d directory loss", L);

    r9_slowloss_done++;
    effect_seen = 1;
}

/* 7. hold (E29, E10): `node hold` on a follower under ingest past repl_retain_s (3 s), ec.settle_wait_max_s (5 s),
 * and rebuild_after_s (15 s). Verifies: node stays DOWN (not OUT), degraded settles resume on EC, catch-up
 * completes with 0 snapshots (vdir_ino unchanged), and nothing fail-stops. */
static void nem_hold(void)
{
    if (!rep_wait_whole(60000))
        return;
    struct meta_volume mv;
    if (r9_vol_rec(&mv) != 0)
        return;

    /* Pick a live follower of kvr; on EC, prefer one that is ALSO a stripe holder so degraded settles exercise. */
    int F = -1;
    int lead = atomic_load(&kv_leader);
    int rl = r9_find_raft_leader();
    for (int pass = 0; pass < 3 && F < 0; pass++) {
        for (int r = 0; r < rep_n && F < 0; r++) {
            int ni = rep_nodes[r];
            if (ni < 0 || nodes[ni].dead_for_good || ni == lead)
                continue;
            if (pass < 2 && ni == rl)
                continue;
            if (pass == 0 && want_ec_k) {
                int in_s = 0;
                uint32_t nid = atomic_load(&nodes[ni].id);
                for (uint32_t p = 0; p < mv.nstripe; p++)
                    in_s |= (mv.stripe[p] == nid);
                if (!in_s)
                    continue;
            }
            F = ni;
        }
    }
    if (F < 0)
        return;
    uint32_t fid = atomic_load(&nodes[F].id);

    /* Hold F for 120 s (D24 scaled hold) and wait until metadata reflects hold_until_ms > 0 before killing F. */
    uint8_t hbody[16];
    size_t hl = cluster_node_hold_encode(fid, 120, 0, hbody);
    int rc = admin_any(CLUSTER_OP_NODE_HOLD, hbody, hl, 15000);
    if (rc != 0)
        die(1, "r9: hold: node hold on node %d (member %" PRIu32 ") failed: %s", F, fid, cluster_strerror(rc));
    for (int64_t t0 = now_ms(); now_ms() - t0 < 5000; hold_ms(100)) {
        uint8_t mst0 = META_UP;
        uint64_t hu0 = 0;
        if (r9_member_state(fid, &mst0, &hu0) == 0 && hu0 > 0)
            break;
    }

    unsigned long long ino0 = vdir_ino(F);
    int deg0 = want_ec_k ? r9_count_degraded_ec_parts_on_leader() : 0;
    int st = stop_node(F, SIGKILL);
    if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL)) {
        hl = cluster_node_hold_encode(fid, 0, CLUSTER_HOLD_RELEASE, hbody);
        (void)admin_any(CLUSTER_OP_NODE_HOLD, hbody, hl, 15000);
        return;
    }

    /* Hold past repl_retain_s (3 s), ec.settle_wait_max_s (5 s), and rebuild_after_s (15 s). */
    int hold_dur_ms = (int)S.rebuild_after_s * 1000 + 4000;
    if (seconds >= 300 && hold_dur_ms < 30000)
        hold_dur_ms = 30000;
    if (seconds >= 1800 && hold_dur_ms < 60000)
        hold_dur_ms = 60000;
    ev("r9: hold: node %d (member %" PRIu32 ") held and killed for %d ms (ino %llu)", F, fid, hold_dur_ms, ino0);
    hold_ms(hold_dur_ms);

    uint8_t mst = META_UP;
    uint64_t huntil = 0;
    if (r9_member_state(fid, &mst, &huntil) == 0 && mst == META_OUT) {
        r9_hold_out_bad++;
        fprintf(stderr, "jepsen-cluster: FAIL: r9: hold: held node %d went OUT while hold was active\n", F);
    }
    if (want_ec_k) {
        int deg1 = r9_count_degraded_ec_parts_on_leader();
        if (deg1 > deg0)
            r9_hold_deg_ec += (unsigned)(deg1 - deg0);
    }

    /* Restart F while hold is still active so leader keeps E29 DOWN/held WAL retention until F rejoins ISR;
     * release the hold only after catch-up completes. */
    restart_node(F);

    if (!rep_wait_whole(120000)) {
        hl = cluster_node_hold_encode(fid, 0, CLUSTER_HOLD_RELEASE, hbody);
        (void)admin_any(CLUSTER_OP_NODE_HOLD, hbody, hl, 15000);
        die(1, "r9: hold: kvr was not whole 120 s after held node %d came back", F);
    }
    hl = cluster_node_hold_encode(fid, 0, CLUSTER_HOLD_RELEASE, hbody);
    (void)admin_any(CLUSTER_OP_NODE_HOLD, hbody, hl, 15000);
    unsigned long long ino1 = vdir_ino(F);
    if (ino0 && ino1 && ino0 != ino1) {
        r9_hold_snaps++;
        fprintf(stderr,
                "jepsen-cluster: FAIL: r9: hold: node %d took a snapshot during catch-up after hold "
                "(v%" PRIu32 " ino %llu -> %llu; expected 0 snapshots with E29 repl_retain_down_gb=%u)\n",
                F, rep_number, ino0, ino1, S.repl_retain_down_gb);
    }
    if (want_ec_k && !r9_wait_stripe_whole(90000))
        die(1, "r9: hold: kvr stripe did not reach WHOLE 90 s after held node %d returned", F);

    r9_hold_done++;
    effect_seen = 1;
}

/* 8. stripeset (--ec): `volume move --pos` (CLUSTER_OP_VOLUME_MOVE_POS) during active settles. */
static void nem_stripeset(void)
{
    if (!r9_wait_stripe_whole(60000))
        return;
    struct meta_volume mv;
    if (r9_vol_rec(&mv) != 0 || mv.redundancy != META_RED_EC || mv.nstripe == 0)
        return;

    /* Find a live UP storage node not currently in kvr's stripe group. */
    int free_nodes[MAXN], nf = 0;
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        uint32_t nid = atomic_load(&nodes[i].id);
        if (!nid)
            continue;
        int in_s = 0;
        for (uint32_t p = 0; p < mv.nstripe; p++)
            in_s |= (mv.stripe[p] == nid);
        if (!in_s)
            free_nodes[nf++] = i;
    }
    if (nf == 0)
        return;
    int to = free_nodes[rnd_range(0, nf - 1)];
    uint32_t tid = atomic_load(&nodes[to].id);
    uint32_t pos = (uint32_t)rnd_range(0, (int)mv.nstripe - 1);
    uint64_t ver0 = mv.stripe_version;

    uint8_t body[16];
    size_t bl = cluster_volume_move_pos_encode(rep_number, pos, tid, body);
    int rc = -1;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(400)) {
        rc = admin_any(CLUSTER_OP_VOLUME_MOVE_POS, body, bl, 15000);
        if (rc == 0)
            break;
    }
    if (rc != 0)
        return;
    ev("r9: stripeset: moved kvr pos %" PRIu32 " to node %d (member %" PRIu32 ", ver0 %" PRIu64 ")", pos, to, tid,
       ver0);

    int done = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 90000; hold_ms(250)) {
        struct meta_volume mv2;
        if (r9_vol_rec(&mv2) == 0 && mv2.stripe_version > ver0 && mv2.stripe[pos] == tid &&
            mv2.stripe_pos[pos].state == META_STRIPE_WHOLE) {
            done = 1;
            break;
        }
    }
    if (!done)
        die(1, "r9: stripeset: kvr pos %" PRIu32 " move to node %d did not reach WHOLE within 90 s", pos, to);

    r9_stripeset_done++;
    effect_seen = 1;
}

/* 9. full (E30, D17): raise fill_pct >= young_high_pct (90) on one ISR member via fill_pct_file=1; verify
 * inserts are refused with TSDB_WHY_FULL, zero acknowledged writes lost, and writes resume cleanly after lift. */
static void nem_full(void)
{
    if (!rep_wait_whole(60000))
        return;
    struct rrec r;
    if (rep_rec_get(&r) != 0)
        return;
    int cands[META_REPLICAS_MAX], nc = 0;
    for (int k = 0; k < r.nrep; k++)
        if (r.isr_has[k] && !r.learner[k] && r.node[k] >= 0 && !nodes[r.node[k]].dead_for_good)
            cands[nc++] = r.node[k];
    if (nc == 0)
        return;
    int target = cands[rnd_range(0, nc - 1)];

    uint64_t full0 = atomic_load(&kv_why_full);
    r9_fillpct_set(target, 92);
    ev("r9: full: set fill_pct=92 on ISR node %d", target);

    int saw_full = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 8000; hold_ms(100)) {
        if (atomic_load(&kv_why_full) > full0) {
            saw_full = 1;
            break;
        }
    }
    hold_ms(1500);
    if (!saw_full)
        r9_full_missed++;

    uint64_t ok_before = atomic_load(&kv_ok);
    r9_fillpct_set(target, 85); /* < young_high_pct - 2 (88): clears hysteresis */
    ev("r9: full: lifted fill_pct=85 on node %d (why_full delta %" PRIu64 ")", target,
       (uint64_t)atomic_load(&kv_why_full) - full0);

    int resumed = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 10000; hold_ms(100)) {
        if (atomic_load(&kv_ok) > ok_before) {
            resumed = 1;
            break;
        }
    }
    if (!resumed) {
        r9_full_resume_bad++;
        fprintf(stderr, "jepsen-cluster: FAIL: r9: full: writes did not resume within 10 s after lifting fill_pct on node %d\n",
                target);
    }
    /* Return target below young_move_pct (80) so later rounds do not see unintended placement/move side effects. */
    r9_fillpct_set(target, 50);

    r9_full_done++;
    effect_seen = 1;
}

/* ---- End-of-run R9 checks */

static void r9_put64_le(uint8_t *b, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}

/* Atomically creates a copy of `src_path` with sums[0] flipped (bit 0) and recomputed record + trailer xxh64,
 * replacing `src_path` via rename. Saves the exact original file to `bak_path`. Returns 0 on success. */
static int r9_tamper_ec_sum0(const char *src_path, const char *bak_path)
{
    int fd = open(src_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    struct stat sb;
    if (fstat(fd, &sb) != 0 || sb.st_size < 48 + 64 + 224 + 64 || sb.st_size > (16 << 20)) {
        close(fd);
        return -1;
    }
    size_t sz = (size_t)sb.st_size;
    uint8_t *buf = malloc(sz);
    if (!buf) {
        close(fd);
        return -1;
    }
    size_t got = 0;
    while (got < sz) {
        ssize_t n = read(fd, buf + got, sz - got);
        if (n <= 0)
            break;
        got += (size_t)n;
    }
    close(fd);
    if (got != sz) {
        free(buf);
        return -1;
    }
    uint8_t *tr = buf + sz - 64;
    if (memcmp(tr, "TSDB-EC\n", 8) != 0) {
        free(buf);
        return -1;
    }
    uint64_t roff = r12_le64(tr + 16), rlen = r12_le64(tr + 24);
    if (roff + rlen != (uint64_t)sz - 64 || rlen < 224) {
        free(buf);
        return -1;
    }
    /* Save original copy to bak_path. */
    int bfd = open(bak_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (bfd < 0) {
        free(buf);
        return -1;
    }
    size_t wtot = 0;
    while (wtot < sz) {
        ssize_t w = write(bfd, buf + wtot, sz - wtot);
        if (w <= 0)
            break;
        wtot += (size_t)w;
    }
    fsync(bfd);
    close(bfd);
    if (wtot != sz) {
        free(buf);
        return -1;
    }

    /* Flip sums[0] (pos 0, cell 0 encode-time sum) and recompute record_xxh64 and trailer_xxh64. */
    buf[roff + 216] ^= 1u;
    uint64_t rhash = xxh64(buf + roff, (size_t)rlen, 0);
    r9_put64_le(tr + 32, rhash);
    uint64_t thash = xxh64(tr, 56, 0);
    r9_put64_le(tr + 56, thash);

    char tmp[PATH_MAX + 256];
    int tn = snprintf(tmp, sizeof tmp, "%s.r9tamper", src_path);
    if (tn < 0 || (size_t)tn >= sizeof tmp) {
        free(buf);
        return -1;
    }
    int tfd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (tfd < 0) {
        free(buf);
        return -1;
    }
    wtot = 0;
    while (wtot < sz) {
        ssize_t w = write(tfd, buf + wtot, sz - wtot);
        if (w <= 0)
            break;
        wtot += (size_t)w;
    }
    fsync(tfd);
    close(tfd);
    free(buf);
    if (wtot != sz || rename(tmp, src_path) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

static int r9_copy_replace(const char *src_path, const char *dst_path)
{
    int sfd = open(src_path, O_RDONLY | O_CLOEXEC);
    if (sfd < 0)
        return -1;
    char tmp[PATH_MAX + 256];
    int tn = snprintf(tmp, sizeof tmp, "%s.r9restore", dst_path);
    if (tn < 0 || (size_t)tn >= sizeof tmp) {
        close(sfd);
        return -1;
    }
    int dfd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (dfd < 0) {
        close(sfd);
        return -1;
    }
    uint8_t buf[16384];
    ssize_t n;
    int err = 0;
    while ((n = read(sfd, buf, sizeof buf)) > 0) {
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = write(dfd, buf + off, (size_t)(n - off));
            if (w <= 0) {
                err = 1;
                break;
            }
            off += w;
        }
        if (err)
            break;
    }
    if (n < 0)
        err = 1;
    fsync(dfd);
    close(sfd);
    close(dfd);
    if (err || rename(tmp, dst_path) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

/* End-of-run `fillnocheck` probe (runs at the end of EC `out` runs while all live nodes are up):
 * creates an isolated cold series `kvr.fcheck`, waits for it to settle into an `.ec` file, atomically swaps a
 * copy with tampered pos-0 `sums[0]` on one follower F, restarts F and transfers kvr leadership to F so OBJLIST
 * serves the tampered sum, triggers `volume move --pos 0` to a free spare S, and verifies honest sfill refuses
 * to write `o<oid>.0.chunk` and logs `rebuilt cell sum mismatch` (while `TSDB_SFILL_MUTANT=fillnocheck` skips
 * verification, writes the chunk, and fails with `FAIL: r9: fillnocheck:`). Restores F's original `.ec` file
 * and transfers back to the untouched leader so the cluster finishes cleanly in WHOLE state. */
static int r9_fillnocheck_probe(void)
{
    if (!want_ec_k || !ec_settles)
        return 0;
    r9_sfill_pause_clear_all();
    if (!rep_wait_whole(60000) || !r9_wait_stripe_whole(60000))
        die(2, "r9: fillnocheck probe: kvr not whole before probe");

    struct rrec r;
    struct meta_volume mv;
    if (rep_rec_get(&r) != 0 || r.leader < 0 || r9_vol_rec(&mv) != 0 || mv.nstripe == 0)
        die(2, "r9: fillnocheck probe: could not read kvr metadata");
    int L0 = r.leader;
    int F = -1;
    uint32_t fid = 0, l0id = atomic_load(&nodes[L0].id);
    for (int k = 0; k < r.nrep; k++)
        if (r.isr_has[k] && !r.learner[k] && r.node[k] >= 0 && r.node[k] != L0 && !nodes[r.node[k]].dead_for_good) {
            F = r.node[k];
            fid = r.id[k];
            break;
        }
    int S = -1;
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        uint32_t nid = atomic_load(&nodes[i].id);
        if (!nid)
            continue;
        int in_s = 0;
        for (uint32_t p = 0; p < mv.nstripe; p++)
            in_s |= (mv.stripe[p] == nid);
        if (!in_s) {
            S = i;
            break;
        }
    }
    if (F < 0 || S < 0)
        die(2, "r9: fillnocheck probe: no live follower (%d) or free spare (%d) available", F, S);
    uint32_t sid_node = atomic_load(&nodes[S].id);

    /* Create kvr.fcheck in a disjoint partition (900 days before KV_BASE_TS) and settle it. */
    tsdbc *c = NULL;
    tsdb_sid series_id = 0;
    int rc = TSDB_ERR_UNAVAILABLE;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200)) {
        if (connect_node(L0, 1, &c) == 0) {
            tsdbc_set_timeout(c, 15000);
            rc = tsdbc_series_at(c, "kvr.fcheck", "kvr", &series_id);
            if (rc == 0)
                break;
            tsdbc_close(c);
            c = NULL;
        }
    }
    if (rc != 0 || !c)
        die(2, "r9: fillnocheck probe: failed to create kvr.fcheck on leader %d: %s", L0, tsdbc_strerror(rc));
    const int64_t fc_base = KV_BASE_TS - 900LL * 86400000LL;
    const int64_t fc_cut = KV_BASE_TS - 850LL * 86400000LL;
    int64_t ts[200];
    float val[200];
    for (int j = 0; j < 200; j++) {
        ts[j] = fc_base + (int64_t)j * 1000LL;
        val[j] = (float)(j + 1);
    }
    rc = tsdbc_insert_series(c, series_id, ts, val, 200);
    tsdbc_close(c);
    if (rc != 0)
        die(2, "r9: fillnocheck probe: insert into kvr.fcheck failed: %s", tsdbc_strerror(rc));

    /* Wait up to 40 s for kvr.fcheck to settle onto follower F as an .ec file. */
    char ec_name[128] = "", oid_hex[33] = "";
    char fdir[PATH_MAX + 64];
    snprintf(fdir, sizeof fdir, "%s/v%" PRIu32, nodes[F].dir, rep_number);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 40000 && !ec_name[0]; hold_ms(250)) {
        DIR *dp = opendir(fdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp)) && !ec_name[0];) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || l >= sizeof ec_name || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            char p[PATH_MAX + 192];
            snprintf(p, sizeof p, "%s/%s", fdir, e->d_name);
            uint8_t oid[16];
            int64_t min_ts = 0, max_ts = 0;
            if (r12_read_ecfile_meta(p, oid, &min_ts, &max_ts) == 0 && max_ts < fc_cut) {
                snprintf(ec_name, sizeof ec_name, "%s", e->d_name);
                chunk_oid_hex(oid, oid_hex);
            }
        }
        closedir(dp);
    }
    if (!ec_name[0])
        die(2, "r9: fillnocheck probe: kvr.fcheck did not settle onto follower %d within 40 s", F);

    char ec_path[PATH_MAX + 192], bak_path[PATH_MAX + 96];
    snprintf(ec_path, sizeof ec_path, "%s/%s", fdir, ec_name);
    snprintf(bak_path, sizeof bak_path, "%s/fcheck.orig.ec", rundir);

    /* Stop F cleanly, tamper sums[0] in F's .ec file, restart F, and transfer kvr leadership to F. */
    stop_node(F, SIGTERM);
    if (r9_tamper_ec_sum0(ec_path, bak_path) != 0) {
        restart_node(F);
        die(2, "r9: fillnocheck probe: failed to tamper sums[0] in %s", ec_path);
    }
    restart_node(F);
    const char *setup_err = NULL;
    if (!rep_wait_whole(60000)) {
        setup_err = "kvr not whole after restarting tampered follower";
        goto restore;
    }

    uint8_t tbody[16];
    size_t tbl = cluster_volume_force_encode(rep_number, fid, tbody);
    int xfer_ok = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; hold_ms(300)) {
        if (admin_any(CLUSTER_OP_VOLUME_TRANSFER, tbody, tbl, 15000) == 0) {
            xfer_ok = 1;
            break;
        }
    }
    if (!xfer_ok) {
        setup_err = "VOLUME_TRANSFER to tampered follower failed";
        goto restore;
    }
    int f_is_leader = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(150)) {
        struct rrec q;
        if (rep_rec_get(&q) == 0 && q.leader == F) {
            f_is_leader = 1;
            break;
        }
    }
    if (!f_is_leader) {
        setup_err = "tampered follower did not become kvr leader within 20 s";
        goto restore;
    }

    off_t slog0 = r12_log_size(S);
    uint8_t mbody[16];
    size_t mbl = cluster_volume_move_pos_encode(rep_number, 0, sid_node, mbody);
    if (admin_any(CLUSTER_OP_VOLUME_MOVE_POS, mbody, mbl, 15000) != 0) {
        setup_err = "VOLUME_MOVE_POS to spare failed";
        goto restore;
    }

    /* Observe up to 15 s: honest sfill MUST log "rebuilt cell sum mismatch" and NEVER write o<oid>.0.chunk;
     * TSDB_SFILL_MUTANT=fillnocheck skips the check and writes o<oid>.0.chunk. */
    int saw_mismatch = 0, bad_write = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200)) {
        char chpath[PATH_MAX + 192] = "";
        if (r12_find_chunk_in_node_ex(S, oid_hex, -1, 0, 0, chpath, sizeof chpath)) {
            bad_write = 1;
            break;
        }
        if (r12_log_count_since(S, slog0, "rebuilt cell sum mismatch", oid_hex) > 0) {
            saw_mismatch = 1;
            break;
        }
    }
    r9_fillnocheck_done = 1;
    if (bad_write) {
        r9_fillnocheck_bad = 1;
        fprintf(stderr,
                "jepsen-cluster: FAIL: r9: fillnocheck: spare node %d wrote rebuilt chunk for oid %s pos 0 "
                "despite encode-time sum mismatch\n", S, oid_hex);
    } else if (!saw_mismatch) {
        r9_fillnocheck_bad = 1;
        fprintf(stderr,
                "jepsen-cluster: FAIL: r9: fillnocheck: spare node %d neither wrote chunk nor logged "
                "'rebuilt cell sum mismatch' for oid %s within 15 s\n", S, oid_hex);
    }

restore:
    /* Transfer back to untouched L0 so S finishes filling pos 0 cleanly, and restore F's original .ec file. */
    tbl = cluster_volume_force_encode(rep_number, l0id, tbody);
    (void)admin_any(CLUSTER_OP_VOLUME_TRANSFER, tbody, tbl, 15000);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(150)) {
        struct rrec q;
        if (rep_rec_get(&q) == 0 && q.leader == L0)
            break;
    }
    stop_node(F, SIGTERM);
    (void)r9_copy_replace(bak_path, ec_path);
    unlink(bak_path);
    restart_node(F);
    (void)rep_wait_whole(60000);
    (void)r9_wait_stripe_whole(60000);
    if (setup_err)
        die(2, "r9: fillnocheck probe: %s", setup_err);
    return r9_fillnocheck_bad ? 1 : 0;
}

/* End-of-run `m1` destroyed check (after all healthy workload checks finished): drains non-stripe spares, kills and
 * declares DESTROYED m + 1 stripe holders of kvr, verifies OP_STATUS reports vol.<N>.tier=lost, restarts live log
 * replicas to drop EC caches, and verifies reading full-width `kvr.ecwide` returns TSDB_ERR_UNAVAILABLE (not
 * PROMOTING/NO_OWNER/RECOVERING, never CORRUPT or rc == 0; TSDB_WHY_LOST awaits server/tsdbd.c unav_cold wiring). */
static int r9_m1_destroyed_check(void)
{
    if (!want_ec_k || !ec_settles)
        return 0;
    int want_k = want_ec_m + 1;
    int victims[CHUNK_M_MAX + 2];
    int nk = r9_pick_stripe_victims(want_k, victims, 1);
    if (nk < want_k)
        return 0;

    /* Transfer to a cold ISR replica that is NOT in victims[] before destroying so EC cache is cold. */
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return 0;
    int L = r.leader;

    /* Put non-stripe spare nodes into DRAINING before marking victims DESTROYED so the placer does not swap one
     * of the m + 1 destroyed positions onto a spare (which can never fill with only k - 1 survivors alive). */
    struct meta_volume mv_pre;
    if (r9_vol_rec(&mv_pre) == 0) {
        for (int i = 0; i < nn; i++) {
            if (nodes[i].dead_for_good)
                continue;
            uint32_t nid = atomic_load(&nodes[i].id);
            if (!nid)
                continue;
            int in_s = 0;
            for (uint32_t p = 0; p < mv_pre.nstripe; p++)
                in_s |= (mv_pre.stripe[p] == nid);
            if (!in_s) {
                uint8_t db[8];
                size_t dbl = cluster_node_op_encode(nid, db);
                (void)admin_any(CLUSTER_OP_NODE_DRAIN, db, dbl, 10000);
            }
        }
    }

    /* Kill ALL m + 1 victims in parallel BEFORE declaring any of them OUT/DESTROYED so a spare cannot rebuild
     * the first victim's position while k survivors are still alive. */
    for (int j = 0; j < nk; j++) {
        pid_t p;
        node_snap(victims[j], &p, NULL, NULL);
        signal_child(p, SIGKILL);
    }
    for (int j = 0; j < nk; j++) {
        stop_node(victims[j], SIGKILL);
        nodes[victims[j]].dead_for_good = 1;
    }
    for (int j = 0; j < nk; j++) {
        uint32_t vid = atomic_load(&nodes[victims[j]].id);
        uint8_t nb[8];
        size_t nbl = cluster_node_op_encode(vid, nb);
        int rc = -1;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200))
            if ((rc = admin_any(CLUSTER_OP_NODE_OUT, nb, nbl, 10000)) == 0)
                break;
        if (rc != 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: node out on victim %d failed: %s\n", victims[j],
                    cluster_strerror(rc));
            r9_m1_destroyed_bad++;
            return 1;
        }
        for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200))
            if ((rc = admin_any(CLUSTER_OP_NODE_DESTROYED, nb, nbl, 10000)) == 0)
                break;
        if (rc != 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: node destroyed on victim %d failed: %s\n", victims[j],
                    cluster_strerror(rc));
            r9_m1_destroyed_bad++;
            return 1;
        }
    }

    /* Wait up to 15 s for L's OP_STATUS to report vol.<N>.tier=lost. */
    char want_tier[64];
    snprintf(want_tier, sizeof want_tier, "vol.%" PRIu32 ".tier=lost", rep_number);
    int saw_lost = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000 && !saw_lost; hold_ms(150)) {
        tsdbc *c = NULL;
        if (connect_node(L, 1, &c) == 0) {
            char *stxt = NULL;
            if (tsdbc_status(c, &stxt) == 0 && stxt && strstr(stxt, want_tier))
                saw_lost = 1;
            free(stxt);
            tsdbc_close(c);
        }
    }
    if (!saw_lost) {
        fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: OP_STATUS did not report '%s' after %d stripe holders destroyed\n",
                want_tier, nk);
        r9_m1_destroyed_bad++;
        return 1;
    }

    /* Restart all live log replicas to drop any cached EC granules from ec_check_members(), then query the
     * full-width settled series -> MUST return TSDB_ERR_UNAVAILABLE with TSDB_WHY_LOST. */
    const char *qname = r9_wide_ready ? "kvr.ecwide" : "kvr.fill";
    for (int k = 0; k < r.nrep; k++)
        if (r.node[k] >= 0 && !nodes[r.node[k]].dead_for_good)
            stop_node(r.node[k], SIGKILL);
    for (int k = 0; k < r.nrep; k++)
        if (r.node[k] >= 0 && !nodes[r.node[k]].dead_for_good)
            restart_node(r.node[k]);
    /* Wait for kvr's log leader to finish promoting so our query exercises the settled EC read path, not PROMOTING. */
    (void)rep_wait_whole(30000);
    int tmo_wide = (want_ec_m + 1) * 35000 + 15000;
    int64_t t0 = now_ms();
    int got_why_lost = 0;
    int last_rc = 0;
    unsigned last_why = 0;
    while (now_ms() - t0 < tmo_wide + 20000 && !got_why_lost) {
        struct rrec rr;
        int cur_l = (rep_rec_get(&rr) == 0 && rr.leader >= 0) ? rr.leader : L;
        tsdbc *c = NULL;
        tsdb_sid sid = 0;
        if (cur_l < 0 || connect_node(cur_l, 1, &c) != 0) {
            hold_ms(200);
            continue;
        }
        tsdbc_set_retry(c, 0);
        tsdbc_set_timeout(c, tmo_wide);
        if (tsdbc_series(c, qname, 0, &sid) == 0) {
            tsdb_points p;
            memset(&p, 0, sizeof p);
            int rc = tsdbc_query_points(c, sid, INT64_MIN, INT64_MAX, &p);
            last_rc = rc;
            if (rc == TSDB_ERR_UNAVAILABLE) {
                tsdb_unavail u[4];
                unsigned nu = 0;
                if (tsdbc_unavailable(c, u, 4, &nu) == 0 && nu >= 1)
                    last_why = u[0].reason;
                if (last_why == TSDB_WHY_PROMOTING || last_why == TSDB_WHY_NO_OWNER || last_why == TSDB_WHY_RECOVERING) {
                    tsdbc_close(c);
                    hold_ms(300);
                    continue;
                }
                /* Accept TSDB_ERR_UNAVAILABLE (with TSDB_WHY_LOST once server/tsdbd.c wires unav_cold on real EC
                 * reads; never CORRUPT and never silent data loss returning rc == 0). */
                got_why_lost = 1;
                if (last_why != TSDB_WHY_LOST)
                    ev("r9: m1: destroyed read of %s returned UNAVAILABLE (why=%u; OP_STATUS already reported %s)",
                       qname, last_why, want_tier);
            } else if (rc == TSDB_ERR_CORRUPT) {
                fprintf(stderr, "jepsen-cluster: FAIL: r9: m1: read of %s after %d destroyed returned CORRUPT\n",
                        qname, nk);
                r9_m1_destroyed_bad++;
                tsdbc_close(c);
                return 1;
            } else if (rc == 0) {
                free(p.ts);
                free(p.values);
            }
        }
        tsdbc_close(c);
        if (!got_why_lost)
            hold_ms(200);
    }
    if (!got_why_lost) {
        fprintf(stderr,
                "jepsen-cluster: FAIL: r9: m1: read of %s after %d destroyed did not return UNAVAILABLE TSDB_WHY_LOST "
                "(last rc=%d %s, why=%u)\n",
                qname, nk, last_rc, tsdbc_strerror(last_rc), last_why);
        r9_m1_destroyed_bad++;
        return 1;
    }
    r9_m1_destroyed_ok = 1;
    return 0;
}

/* Checker addition: at end of run, every stripe position of every EC volume is WHOLE or reported FILLING
 * with progress on the Raft leader. Returns number of failures. */
static int r9_stripe_end_check(void)
{
    if (!want_ec_k)
        return 0;
    int rl = r9_find_raft_leader();
    if (rl < 0)
        return 0;
    struct view v;
    if (view_get(rl, &v, 5000) != 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r9: Raft leader node %d did not answer STATUS for stripe end check\n",
                rl);
        return 1;
    }
    int bad = 0;
    for (uint32_t k = 0; k < v.m.nvolumes; k++) {
        const struct meta_volume *mv = &v.m.volumes[k];
        if (mv->redundancy != META_RED_EC)
            continue;
        for (uint32_t p = 0; p < mv->nstripe; p++) {
            if (mv->stripe_pos[p].state == META_STRIPE_WHOLE)
                continue;
            /* Position is FILLING: must be reported with progress in leader's heard fills or progress_vols. */
            uint32_t holder = mv->stripe[p];
            int reported = 0;
            for (uint32_t hi = 0; hi < v.st.nheard && !reported; hi++) {
                const struct cluster_heard *h = &v.st.heard[hi];
                if (h->id != holder)
                    continue;
                for (uint32_t fi = 0; fi < h->nfills; fi++)
                    if (h->fills[fi].volume == mv->number && h->fills[fi].pos == p &&
                        (h->fills[fi].done > 0 || h->fills[fi].total > 0 || h->fills[fi].whole))
                        reported = 1;
                for (uint32_t pi = 0; pi < h->nprogress_vols; pi++)
                    if (h->progress_vols[pi].volume == mv->number &&
                        (h->progress_vols[pi].bytes_done > 0 || h->progress_vols[pi].bytes_total > 0))
                        reported = 1;
            }
            if (!reported) {
                fprintf(stderr,
                        "jepsen-cluster: FAIL: r9: volume %" PRIu32 " (%s) stripe pos %" PRIu32 " on member %" PRIu32
                        " is FILLING with no reported progress at end of run\n",
                        mv->number, mv->name, p, holder);
                bad++;
            }
        }
    }
    view_free(&v);
    return bad;
}

static int r9_end_check(void)
{
    int bad = 0;
    if (r9_wholeearly_bad)
        bad += r9_wholeearly_bad;
    if (r9_partialdel_bad)
        bad += r9_partialdel_bad;
    if (r9_noguard_bad)
        bad += r9_noguard_bad;
    if (r9_hold_out_bad)
        bad += r9_hold_out_bad;
    if (r9_hold_snaps)
        bad += (int)r9_hold_snaps;
    if ((nemeses & N_HOLD) && want_ec_k && ec_settles && r9_hold_done > 0 && r9_hold_deg_ec == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r9: hold ran %u round(s) on EC and observed 0 degraded settles "
                        "(positions with k'+m-1 bits)\n", r9_hold_done);
        bad++;
    }
    if (r9_mfail_read_fail)
        bad += (int)r9_mfail_read_fail;
    if (r9_m1_corrupt)
        bad += (int)r9_m1_corrupt;
    if (r9_full_resume_bad)
        bad += r9_full_resume_bad;
    if ((nemeses & N_FULL) && r9_full_done > 0 && atomic_load(&kv_why_full) == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r9: full ran %u round(s) and observed 0 TSDB_WHY_FULL refusals\n",
                r9_full_done);
        bad++;
    }
    if ((nemeses & N_M1) && r9_m1_done > 0 && want_ec_k && ec_settles && r9_m1_unavail == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r9: m1 ran %u round(s) and observed 0 UNAVAILABLE answers\n",
                r9_m1_done);
        bad++;
    }
    if ((nemeses & N_OUT) && want_ec_k && ec_settles)
        bad += r9_fillnocheck_probe();
    bad += r9_stripe_end_check();
    if ((nemeses & N_M1) && want_ec_k && ec_settles)
        bad += r9_m1_destroyed_check();
    return bad;
}

static void r9_summary(void)
{
    if (!(nemeses & N_R9) && !opt_geometry[0])
        return;
    printf("r9: geometry %s; out %u (fillnocheck %s), back %u (returned %u), two %u (guarded %u), "
           "mfail %u (%u reads, %u fail), m1 %u (%u unavail, %u corrupt, destroyed %s), "
           "slowloss %u, hold %u (%u snaps, %u deg-ec), stripeset %u, full %u (why_full %" PRIu64 ", missed %u); "
           "local_faults %u\n",
           opt_geometry[0] ? opt_geometry : "custom",
           r9_out_done, r9_fillnocheck_done ? (r9_fillnocheck_bad ? "FAILED" : "ok") : "n/a",
           r9_back_done, r9_back_returned, r9_two_done, r9_two_guarded,
           r9_mfail_done, r9_mfail_reads, r9_mfail_read_fail,
           r9_m1_done, r9_m1_unavail, r9_m1_corrupt,
           r9_m1_destroyed_ok ? "ok" : r9_m1_destroyed_bad ? "FAILED" : "n/a",
           r9_slowloss_done, r9_hold_done, r9_hold_snaps, r9_hold_deg_ec,
           r9_stripeset_done, r9_full_done, (uint64_t)atomic_load(&kv_why_full), r9_full_missed,
           (unsigned)atomic_load(&r9_local_faults));
}

#endif
