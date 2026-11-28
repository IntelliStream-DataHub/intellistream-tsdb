/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R13 two-class disks on one machine and jepsen-cluster disk nemeses (docs/design-cluster-data.md §15.9,
 * r12-14-scope/PLAN.md D21, row r13-jc).
 *
 * Included by jepsen_cluster.c right after cluster_r9.h. Uses its node table, views, admin calls, EC/R9
 * helpers, and histories.
 *
 * Setup (--disks nvme,nvme,hdd,hdd [--hdd-dir DIR] [--hdd-slow-ms MS] [--hdd-rate-mb MB]):
 *  - Each node gets its fast (:nvme/:ssd) disks under `<rundir>/n<i>/<cls><k>` and its capacity (:hdd) disks
 *    under a second prefix `<hdd_rundir>/n<i>/hdd<k>`, each initialised with `tsdb disk init PATH:CLASS`,
 *    started with `--disk PATH:CLASS ... --allow-shared-device` (omitting `--dir`) and `--io epoll` on
 *    nodes >= 2 (at most two io_uring nodes).
 *  - Every node runs under outagefs (`--outagefs build/liboutagefs.so`) with all its disks as independent
 *    prefixes; each `:hdd` disk gets baseline `slow MS` (seek latency) and `rate MB/s` (throughput cap).
 *
 * Nemeses:
 *  - hddeio:    EIO on one hard drive under load; verifies disk goes DOWN/OUT, server process stays alive,
 *               chunks rebuild locally onto a sibling HDD (0 on NVMe), and replacement HDD adopts UP.
 *  - nvmestuck: hang on one NVMe holding kvr under load; verifies leader failover within disk_stall_s +
 *               failover, server process stays alive (catches TSDB_DISKS_MUTANT=stallexit), and heal reopens
 *               the replica and rejoins full ISR.
 *  - diskloss:  one disk of each class (one :hdd and one :nvme) lost, rebuilt onto its sibling of the same
 *               class, wiped empty, re-initialised with `tsdb disk init`, and adopted UP.
 *  - diskback:  a disk hung away and healed within rebuild_after_s (never goes OUT); verifies clean attach
 *               with 0 rebuild (HDD REPLACING -> UP attach, NVMe reopen with unchanged vdir_ino).
 *  - nvmefill:  a fast disk holding kvr fills to FULL (>= young_high_pct) under ingest; verifies kvr writes
 *               are refused with TSDB_WHY_FULL while a sibling volume on the SAME node's other NVMe disk
 *               keeps accepting writes with 0 refusals (catches TSDB_DISKS_MUTANT=fullall), then resumes
 *               cleanly after fill clears.
 */
#ifndef JEPSEN_CLUSTER_R13_H
#define JEPSEN_CLUSTER_R13_H

/* ---- R13 state & counters */
static unsigned r13_hddeio_done, r13_hddeio_rebuilds, r13_hddeio_replaced;
static unsigned r13_nvmestuck_done, r13_nvmestuck_failovers, r13_nvmestuck_reopened;
static unsigned r13_diskloss_done, r13_diskloss_hdd, r13_diskloss_nvme, r13_diskloss_rebuilds, r13_diskloss_replaced;
static unsigned r13_diskback_done, r13_diskback_attaches, r13_diskback_rebuilds;
static unsigned r13_nvmefill_done, r13_nvmefill_sib_ok, r13_nvmefill_sib_refusals;
static uint64_t r13_nvmefill_kvr_refusals;
static int r13_proc_exits_bad, r13_fullall_bad, r13_nvme_chunks_bad, r13_resume_bad, r13_diskback_out_bad;
static unsigned r13_stage_seq;

/* Sibling volume on a second NVMe of a kvr replica node (for nvmefill / fullall check). */
static uint32_t r13_sib_num;
static char r13_sib_name[32];
static tsdb_sid r13_sib_sid;
static int r13_sib_target_node = -1;
static int r13_sib_kvr_disk = -1;
static int r13_sib_other_disk = -1;
static int64_t r13_sib_ts_next;

/* ---- --disks parsing & initialisation */

static int r13_parse_disks(const char *spec)
{
    char buf[128];
    if (!spec || !*spec || strlen(spec) >= sizeof buf)
        return -1;
    snprintf(buf, sizeof buf, "%s", spec);
    r13_ndisks_cfg = 0;
    for (char *t = strtok(buf, ","); t; t = strtok(NULL, ",")) {
        if (r13_ndisks_cfg >= R13_MAX_DISKS)
            return -1;
        if (strcmp(t, "nvme") != 0 && strcmp(t, "ssd") != 0 && strcmp(t, "hdd") != 0)
            return -1;
        snprintf(r13_disk_cls[r13_ndisks_cfg], sizeof r13_disk_cls[0], "%s", t);
        r13_ndisks_cfg++;
    }
    if (r13_ndisks_cfg == 0)
        return -1;
    snprintf(r13_disk_spec, sizeof r13_disk_spec, "%s", spec);
    r13_disks_on = 1;
    return 0;
}

static void r13_disk_init_cli(const char *path, const char *cls)
{
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char spec[PATH_MAX + 16];
    snprintf(spec, sizeof spec, "%s:%s", path, cls);
    pid_t p = fork();
    if (p < 0)
        die(2, "r13: fork disk init: %s", strerror(errno));
    if (p == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, 1);
            close(devnull);
        }
        execl(cli, "tsdb", "disk", "init", spec, (char *)NULL);
        _exit(127);
    }
    int wst = 0;
    while (waitpid(p, &wst, 0) < 0 && errno == EINTR)
        ;
    if (!WIFEXITED(wst) || WEXITSTATUS(wst) != 0)
        die(2, "r13: tsdb disk init %s failed (status 0x%x; TSDB_CLI=%s)", spec, wst, cli);
}

static void r13_init_node_disks(int i)
{
    struct node *n = &nodes[i];
    if (mkdir(n->dir, 0755) != 0 && errno != EEXIST)
        die(2, "r13: mkdir %s: %s", n->dir, strerror(errno));
    char hnode_dir[PATH_MAX - 16];
    snprintf(hnode_dir, sizeof hnode_dir, "%s/n%d", hdd_rundir, i);
    int have_hdd = 0;
    for (int d = 0; d < r13_ndisks_cfg; d++)
        if (strcmp(r13_disk_cls[d], "hdd") == 0)
            have_hdd = 1;
    if (have_hdd && mkdir(hnode_dir, 0755) != 0 && errno != EEXIST)
        die(2, "r13: mkdir %s: %s", hnode_dir, strerror(errno));

    char ndir[PATH_MAX + 16];
    snprintf(ndir, sizeof ndir, "%s", n->dir);
    n->ndisks = r13_ndisks_cfg;
    n->chunk[0][0] = 0;
    n->chunk[1][0] = 0;
    int fast_k = 0, hdd_k = 0, chunk_k = 0;
    for (int d = 0; d < r13_ndisks_cfg; d++) {
        struct node_disk *dk = &n->disks[d];
        memset(dk, 0, sizeof *dk);
        snprintf(dk->cls, sizeof dk->cls, "%s", r13_disk_cls[d]);
        dk->is_fast = (strcmp(dk->cls, "hdd") != 0);
        if (dk->is_fast)
            snprintf(dk->path, sizeof dk->path, "%s/%s%d", ndir, dk->cls, fast_k++);
        else
            snprintf(dk->path, sizeof dk->path, "%s/hdd%d", hnode_dir, hdd_k++);
        if (mkdir(dk->path, 0755) != 0 && errno != EEXIST)
            die(2, "r13: mkdir %s: %s", dk->path, strerror(errno));
        r13_disk_init_cli(dk->path, dk->cls);
        disk_meta dm;
        if (disk_load(dk->path, &dm) != 0)
            die(2, "r13: disk_load %s failed", dk->path);
        memcpy(dk->store_id, dm.store_id, 16);
        chunk_oid_hex(dm.store_id, dk->hex);
        if (!dk->is_fast && chunk_k < 2)
            snprintf(n->chunk[chunk_k++], sizeof n->chunk[0], "%s", dk->path);
    }
    /* All-flash fallback: map first two fast disks to chunk[] when no HDD is configured. */
    if (chunk_k == 0) {
        for (int d = 0; d < n->ndisks && chunk_k < 2; d++)
            snprintf(n->chunk[chunk_k++], sizeof n->chunk[0], "%s", n->disks[d].path);
    }
    snprintf(n->usage_file, sizeof n->usage_file, "%s/disk_usage.txt", ndir);
    unlink(n->usage_file);
}

/* Re-apply baseline HDD outagefs slow + rate profile to one HDD disk of node i. */
static void r13_reapply_hdd_disk(int i, int d)
{
    if (!r13_disks_on || !use_outagefs || d < 0 || d >= nodes[i].ndisks || nodes[i].disks[d].is_fast)
        return;
    char line[PATH_MAX + 64], out[512];
    if (opt_hdd_slow_ms > 0) {
        snprintf(line, sizeof line, "slow %s %d", nodes[i].disks[d].path, opt_hdd_slow_ms);
        if (outagefs_cmd(nodes[i].octl, line, out, sizeof out) != 0)
            die(2, "r13: outagefs '%s' on node %d failed: %s", line, i, out);
    }
    if (opt_hdd_rate_mb > 0.0) {
        snprintf(line, sizeof line, "rate %s %.2f", nodes[i].disks[d].path, opt_hdd_rate_mb);
        if (outagefs_cmd(nodes[i].octl, line, out, sizeof out) != 0)
            die(2, "r13: outagefs '%s' on node %d failed: %s", line, i, out);
    }
}

/* Wait for node i's outagefs control socket and apply baseline HDD slow + rate limits to every :hdd disk. */
static void r13_apply_hdd_profile(int i)
{
    if (!r13_disks_on || !use_outagefs || nodes[i].ndisks == 0)
        return;
    char out[2048];
    int ok = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 10000; usleep(50000)) {
        if (outagefs_cmd(nodes[i].octl, "state", out, sizeof out) == 0) {
            ok = 1;
            break;
        }
    }
    if (!ok)
        die(2, "r13: outagefs control socket %s for node %d not ready within 10 s", nodes[i].octl, i);
    for (int d = 0; d < nodes[i].ndisks; d++)
        if (!nodes[i].disks[d].is_fast)
            r13_reapply_hdd_disk(i, d);
}

static void r13_outage_disk(int i, int d, const char *verb)
{
    char line[PATH_MAX + 64], out[512];
    snprintf(line, sizeof line, "%s %s", verb, nodes[i].disks[d].path);
    if (outagefs_cmd(nodes[i].octl, line, out, sizeof out) != 0)
        die(2, "r13: outagefs '%s' on node %d failed: %s", line, i, out);
}

/* ---- helpers */

static int r13_node_disk_of_vol(int node, uint32_t vol)
{
    if (node < 0 || node >= nn || !vol)
        return -1;
    struct stat sb;
    for (int pass = 0; pass < 2; pass++) {
        for (int d = 0; d < nodes[node].ndisks; d++) {
            if ((pass == 0) != (nodes[node].disks[d].is_fast != 0))
                continue;
            char vp[PATH_MAX + 64];
            snprintf(vp, sizeof vp, "%s/v%" PRIu32, nodes[node].disks[d].path, vol);
            if (stat(vp, &sb) == 0 && S_ISDIR(sb.st_mode))
                return d;
        }
    }
    return -1;
}

static void node_vol_dir(int node, uint32_t vol, char *out, size_t cap)
{
    if (node >= 0 && node < nn && nodes[node].ndisks > 0) {
        int d = r13_node_disk_of_vol(node, vol);
        if (d >= 0) {
            snprintf(out, cap, "%s/v%" PRIu32, nodes[node].disks[d].path, vol);
            return;
        }
        snprintf(out, cap, "%s/v%" PRIu32, nodes[node].disks[0].path, vol);
        return;
    }
    snprintf(out, cap, "%s/v%" PRIu32, nodes[node].dir, vol);
}

static int r13_disk_meta_state(uint32_t member_id, const uint8_t store_id[16])
{
    int rl = r9_find_raft_leader();
    int order[MAXN], no = 0;
    if (rl >= 0 && rl < nn)
        order[no++] = rl;
    for (int i = 0; i < nn; i++)
        if (i != rl)
            order[no++] = i;
    for (int k = 0; k < no; k++) {
        int i = order[k];
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        int st = -1;
        const struct meta_member *mb = meta_member(&v.m, member_id);
        if (mb) {
            const struct meta_disk *dk = meta_member_disk(mb, store_id);
            if (dk)
                st = (int)dk->state;
        }
        view_free(&v);
        if (st >= 0)
            return st;
    }
    return -1;
}

static void r13_wait_all_disks_registered(int timeout_ms)
{
    if (!r13_disks_on)
        return;
    for (int64_t t0 = now_ms(); now_ms() - t0 < timeout_ms; usleep(200000)) {
        check_unexpected_exits();
        int rl = r9_find_raft_leader();
        if (rl < 0)
            rl = 0;
        struct view v;
        if (view_get(rl, &v, 2000) != 0)
            continue;
        int all_ok = 1;
        for (int i = 0; i < nn && all_ok; i++) {
            uint32_t nid = atomic_load(&nodes[i].id);
            const struct meta_member *mb = meta_member(&v.m, nid);
            if (!mb || mb->ndisks < (uint32_t)nodes[i].ndisks) {
                all_ok = 0;
                break;
            }
            for (int d = 0; d < nodes[i].ndisks; d++) {
                const struct meta_disk *dk = meta_member_disk(mb, nodes[i].disks[d].store_id);
                if (!dk || dk->state != META_STORE_UP) {
                    all_ok = 0;
                    break;
                }
            }
        }
        view_free(&v);
        if (all_ok)
            return;
    }
    die(1, "r13: not all configured disks registered META_STORE_UP within %d ms", timeout_ms);
}

static int r13_count_chunks_in_disk(const char *disk_path, uint32_t vol)
{
    char vdir[PATH_MAX + 96];
    snprintf(vdir, sizeof vdir, "%s/chunks/s%" PRIu32, disk_path, vol);
    DIR *dp = opendir(vdir);
    if (!dp)
        return 0;
    int cnt = 0;
    for (struct dirent *e; (e = readdir(dp));) {
        size_t l = strlen(e->d_name);
        if (l >= 41 && e->d_name[0] == 'o' && strcmp(e->d_name + l - 6, ".chunk") == 0)
            cnt++;
    }
    closedir(dp);
    return cnt;
}

static int r13_nvme_chunk_count(int node, uint32_t vol)
{
    int have_hdd = 0, cnt = 0;
    for (int d = 0; d < nodes[node].ndisks; d++)
        if (!nodes[node].disks[d].is_fast)
            have_hdd = 1;
    if (!have_hdd)
        return 0;
    for (int d = 0; d < nodes[node].ndisks; d++)
        if (nodes[node].disks[d].is_fast)
            cnt += r13_count_chunks_in_disk(nodes[node].disks[d].path, vol);
    return cnt;
}

static void r13_assert_pid_alive(int node, pid_t pid_before, const char *ctx)
{
    check_unexpected_exits();
    pid_t cur = 0;
    node_snap(node, &cur, NULL, NULL);
    if (cur <= 0 || cur != pid_before) {
        r13_proc_exits_bad++;
        die(1, "r13: %s: node %d process exited or changed pid (%d -> %d; expected to stay alive, log %s)",
            ctx, node, (int)pid_before, (int)cur, nodes[node].log);
    }
}

/* Scrape GET /metrics of node i and find integer value after `needle` (must start with '\n' or metric name). */
static int r13_scrape_metric_i64(int node, const char *needle, int64_t *out)
{
    pid_t pid;
    int mport, paused;
    pthread_mutex_lock(&tbl_mu);
    pid = nodes[node].pid;
    mport = nodes[node].mport;
    paused = nodes[node].paused;
    pthread_mutex_unlock(&tbl_mu);
    if (pid <= 0 || paused || mport <= 0)
        return 0;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return 0;
    struct timeval tv = {2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)mport)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    static const char rq[] = "GET /metrics HTTP/1.0\r\n\r\n";
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0 || !th_owns_port(pid, mport) ||
        write(fd, rq, sizeof rq - 1) != (ssize_t)(sizeof rq - 1)) {
        close(fd);
        return 0;
    }
    char chunk[4096], line[512];
    size_t ln = 0, nlen = strlen(needle);
    int skipping = 0, found = 0;
    for (ssize_t r; !found && (r = read(fd, chunk, sizeof chunk)) > 0;) {
        for (ssize_t k = 0; k < r && !found; k++) {
            if (chunk[k] != '\n') {
                if (ln + 1 < sizeof line)
                    line[ln++] = chunk[k];
                else
                    skipping = 1;
                continue;
            }
            if (ln > 0 && line[ln - 1] == '\r')
                ln--;
            line[ln] = 0;
            if (!skipping && ln >= nlen && strncmp(line, needle, nlen) == 0) {
                char *ep = NULL;
                long long v = strtoll(line + nlen, &ep, 10);
                if (ep && ep > line + nlen && (*ep == '\0' || *ep == ' ')) {
                    *out = (int64_t)v;
                    found = 1;
                }
            }
            ln = 0;
            skipping = 0;
        }
    }
    close(fd);
    return found;
}

static int r13_scrape_disk_young_bytes(int node, const char *hex, const char *cls, uint64_t *size_out, uint64_t *young_out)
{
    char key[192];
    int64_t sz = 0;
    snprintf(key, sizeof key, "tsdb_disk_size_bytes{store=\"%s\",class=\"%s\"} ", hex, cls);
    if (!r13_scrape_metric_i64(node, key, &sz) || sz <= 0)
        return 0;
    static const char *const uses[5] = {"young", "retained", "retired", "local", "index"};
    uint64_t tot = 0;
    for (int u = 0; u < 5; u++) {
        int64_t v = 0;
        snprintf(key, sizeof key, "tsdb_disk_bytes{store=\"%s\",class=\"%s\",use=\"%s\"} ", hex, cls, uses[u]);
        if (r13_scrape_metric_i64(node, key, &v) && v > 0)
            tot += (uint64_t)v;
    }
    *size_out = (uint64_t)sz;
    *young_out = tot;
    return 1;
}

static void r13_write_usage_file(int node, const char *content)
{
    if (!nodes[node].usage_file[0])
        return;
    if (!content) {
        unlink(nodes[node].usage_file);
        return;
    }
    char tmp[PATH_MAX + 48];
    snprintf(tmp, sizeof tmp, "%s.tmp", nodes[node].usage_file);
    FILE *f = fopen(tmp, "w");
    if (!f)
        die(2, "r13: fopen %s: %s", tmp, strerror(errno));
    fputs(content, f);
    fclose(f);
    if (rename(tmp, nodes[node].usage_file) != 0)
        die(2, "r13: rename %s -> %s: %s", tmp, nodes[node].usage_file, strerror(errno));
}

/* Mark OUT disk `d` on `node` DESTROYED via CLUSTER_OP_DISK_REPLACE, wipe its data + DISK, stage a fresh
 * `tsdb disk init` DISK into place, wait for live adoption into META_STORE_UP, and update nodes[node].disks[d]. */
static void r13_disk_replace_swap(int node, int d)
{
    struct node_disk *dk = &nodes[node].disks[d];
    uint32_t nid = atomic_load(&nodes[node].id);
    uint8_t body[24];
    size_t bl = cluster_disk_op_encode(nid, dk->store_id, body);
    int rc = -1;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(200)) {
        rc = admin_any(CLUSTER_OP_DISK_REPLACE, body, bl, 15000);
        if (rc == 0)
            break;
    }
    if (rc != 0)
        die(1, "r13: CLUSTER_OP_DISK_REPLACE node %d disk %s failed: %s", node, dk->hex, cluster_strerror(rc));

    for (int64_t t0 = now_ms(); r13_disk_meta_state(nid, dk->store_id) != META_STORE_DESTROYED; hold_ms(150))
        if (now_ms() - t0 > 20000)
            die(1, "r13: node %d disk %s did not reach META_STORE_DESTROYED within 20 s", node, dk->hex);

    if (use_outagefs)
        r13_outage_disk(node, d, "heal");

    if (!dk->is_fast) {
        char cdir[PATH_MAX + 64];
        snprintf(cdir, sizeof cdir, "%s/chunks", dk->path);
        rmtree(cdir);
    } else {
        /* Wipe kvr (and any other v<N>) replica dir left on the replaced fast disk. */
        DIR *dp = opendir(dk->path);
        if (dp) {
            for (struct dirent *e; (e = readdir(dp));) {
                if (e->d_name[0] == 'v' && e->d_name[1] >= '1' && e->d_name[1] <= '9') {
                    char sub[PATH_MAX + 300];
                    snprintf(sub, sizeof sub, "%s/%s", dk->path, e->d_name);
                    rmtree(sub);
                }
            }
            closedir(dp);
        }
    }

    char old_meta[PATH_MAX + 64], stage[PATH_MAX + 64], stage_meta[PATH_MAX + 96];
    snprintf(old_meta, sizeof old_meta, "%s/%s", dk->path, DISK_META_FILE);
    snprintf(stage, sizeof stage, "%s/r13_stage_%d_%d_%u", rundir, node, d, ++r13_stage_seq);
    snprintf(stage_meta, sizeof stage_meta, "%s/%s", stage, DISK_META_FILE);
    rmtree(stage);
    if (mkdir(stage, 0755) != 0)
        die(2, "r13: mkdir %s: %s", stage, strerror(errno));
    r13_disk_init_cli(stage, dk->cls);

    disk_meta new_dm;
    if (disk_load(stage, &new_dm) != 0)
        die(2, "r13: disk_load staged %s failed", stage);
    if (rename(stage_meta, old_meta) != 0)
        die(2, "r13: rename %s -> %s: %s", stage_meta, old_meta, strerror(errno));
    rmtree(stage);

    for (int64_t t0 = now_ms(); r13_disk_meta_state(nid, new_dm.store_id) != META_STORE_UP; hold_ms(200))
        if (now_ms() - t0 > 30000)
            die(1, "r13: replacement %s disk on node %d (%s) was not adopted META_STORE_UP within 30 s",
                dk->cls, node, dk->path);

    memcpy(dk->store_id, new_dm.store_id, 16);
    chunk_oid_hex(new_dm.store_id, dk->hex);
    if (!dk->is_fast)
        r13_reapply_hdd_disk(node, d);
}

/* ---- nvmefill setup: ensure a sibling volume shares a node with kvr on that node's OTHER fast disk */

static int r13_vol_leader_node(uint32_t vnum)
{
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        int lead = -1;
        for (uint32_t k = 0; k < v.m.nvolumes; k++)
            if (v.m.volumes[k].number == vnum && v.m.volumes[k].leader) {
                lead = node_by_id(v.m.volumes[k].leader);
                break;
            }
        view_free(&v);
        if (lead >= 0)
            return lead;
    }
    return -1;
}

static void r13_nvmefill_setup(void)
{
    if (!(nemeses & N_NVMEFILL))
        return;
    r13_sib_num = 0;
    r13_sib_target_node = -1;
    r13_sib_kvr_disk = -1;
    r13_sib_other_disk = -1;
    r13_sib_ts_next = KV_BASE_TS + 5000000LL;

    /* Create replicated sibling volumes until one shares a node with kvr on a DISTINCT fast disk of that node
     * (guarantees TSDB_DISKS_MUTANT=fullall triggers on r13_sib_name when kvr's fast disk fills!). */
    for (int attempt = 0; attempt < nn && r13_sib_target_node < 0; attempt++) {
        char vn[32];
        snprintf(vn, sizeof vn, "r13sib%d", attempt + 1);
        uint8_t req[META_NAME_MAX + 8];
        size_t rl = cluster_volume_add_encode(vn, 3, req);
        int rc = admin_any(CLUSTER_OP_VOLUME_ADD, req, rl, 15000);
        if (rc != 0)
            die(1, "r13: nvmefill: volume add %s failed: %s", vn, cluster_strerror(rc));

        uint32_t vnum = 0;
        int lead = -1;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; usleep(150000)) {
            struct view v;
            if (view_get(0, &v, 2000) != 0)
                continue;
            for (uint32_t k = 0; k < v.m.nvolumes; k++) {
                const struct meta_volume *mv = &v.m.volumes[k];
                if (strcmp(mv->name, vn) != 0 || !mv->leader || mv->nisr < mv->nrep)
                    continue;
                int all_open = 1;
                for (uint32_t r = 0; r < mv->nrep; r++)
                    all_open &= (mv->rep[r].open_gen >= 1);
                if (all_open) {
                    vnum = mv->number;
                    lead = node_by_id(mv->leader);
                }
            }
            view_free(&v);
            if (vnum && lead >= 0)
                break;
        }
        if (!vnum || lead < 0)
            die(1, "r13: nvmefill: sibling volume %s did not open with full ISR", vn);

        /* Check if any replica node of kvr holds BOTH kvr and vn on TWO DIFFERENT fast disks. */
        for (int r = 0; r < rep_n; r++) {
            int ni = rep_nodes[r];
            int kd = r13_node_disk_of_vol(ni, rep_number);
            int od = r13_node_disk_of_vol(ni, vnum);
            if (kd >= 0 && od >= 0 && kd != od && nodes[ni].disks[kd].is_fast && nodes[ni].disks[od].is_fast) {
                r13_sib_num = vnum;
                snprintf(r13_sib_name, sizeof r13_sib_name, "%s", vn);
                r13_sib_target_node = ni;
                r13_sib_kvr_disk = kd;
                r13_sib_other_disk = od;
                break;
            }
        }
    }
    if (r13_sib_target_node < 0)
        die(2, "r13: nvmefill: could not place a sibling volume on a second fast disk of any kvr replica (need >= 2 fast disks in --disks)");

    /* Create series r13.sib inside r13_sib_name. */
    int lead = r13_vol_leader_node(r13_sib_num);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; usleep(150000)) {
        lead = r13_vol_leader_node(r13_sib_num);
        if (lead < 0)
            continue;
        tsdbc *c = NULL;
        tsdb_sid sid = 0;
        int rc = connect_node(lead, 1, &c);
        if (rc == 0)
            rc = tsdbc_series_at(c, "r13.sib", r13_sib_name, &sid);
        if (c)
            tsdbc_close(c);
        if (rc == 0) {
            r13_sib_sid = sid;
            break;
        }
    }
    if (!r13_sib_sid)
        die(2, "r13: nvmefill: could not create series r13.sib in %s", r13_sib_name);

    printf("jepsen-cluster: r13 nvmefill: node %d holds kvr (v%" PRIu32 ") on %s (%s) and sibling %s (v%" PRIu32
           ") on %s (%s)\n",
           r13_sib_target_node, rep_number, nodes[r13_sib_target_node].disks[r13_sib_kvr_disk].path,
           nodes[r13_sib_target_node].disks[r13_sib_kvr_disk].hex, r13_sib_name, r13_sib_num,
           nodes[r13_sib_target_node].disks[r13_sib_other_disk].path,
           nodes[r13_sib_target_node].disks[r13_sib_other_disk].hex);
}

/* ---- R13 stripe-holder helpers */

static int r13_node_stripe_pos(uint32_t nid, uint32_t vnum)
{
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        int found = -1;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (mv->number != vnum || mv->redundancy != META_RED_EC)
                continue;
            for (uint32_t p = 0; p < mv->nstripe && p < META_STRIPE_MAX; p++) {
                if (mv->stripe[p] == nid && mv->stripe_pos[p].state == META_STRIPE_WHOLE) {
                    found = (int)p;
                    break;
                }
            }
            break;
        }
        view_free(&v);
        return found;
    }
    return -1;
}

static int r13_stripe_pos_whole(uint32_t vnum, int pos)
{
    if (pos < 0 || pos >= META_STRIPE_MAX)
        return 0;
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        int whole = 0;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (mv->number != vnum || mv->redundancy != META_RED_EC)
                continue;
            if ((uint32_t)pos < mv->nstripe && mv->stripe_pos[pos].state == META_STRIPE_WHOLE)
                whole = 1;
            break;
        }
        view_free(&v);
        return whole;
    }
    return 0;
}

/* ---- R13 nemeses */

/* 1. hddeio (§15.9): EIO on one hard drive under load; verifies disk goes DOWN/OUT, server process stays alive,
 * chunks rebuild locally onto sibling HDD (never onto NVMe), and replacement HDD adopts UP. */
static void nem_hddeio(void)
{
    if (want_ec_k && ec_settles && r13_hddeio_done == 0) {
        /* Wait briefly on the first round so at least 1 settled chunk lands on a live stripe-holder HDD. */
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(300)) {
            int any = 0;
            for (int i = 0; i < nn && !any; i++) {
                if (r13_node_stripe_pos(atomic_load(&nodes[i].id), rep_number) < 0)
                    continue;
                for (int d = 0; d < nodes[i].ndisks && !any; d++)
                    if (!nodes[i].disks[d].is_fast && r13_count_chunks_in_disk(nodes[i].disks[d].path, rep_number) > 0)
                        any = 1;
            }
            if (any)
                break;
        }
    }

    int target = -1, hd = -1, sib_d = -1, held_pos = -1;
    int off = rnd_range(0, nn - 1);
    for (int pass = 0; pass < 3 && target < 0; pass++) {
        for (int k = 0; k < nn && target < 0; k++) {
            int i = (off + k) % nn;
            if (nodes[i].dead_for_good)
                continue;
            int spos = (want_ec_k && ec_settles) ? r13_node_stripe_pos(atomic_load(&nodes[i].id), rep_number) : -1;
            if (want_ec_k && ec_settles && pass < 2 && spos < 0)
                continue;
            int hlist[R13_MAX_DISKS], nh = 0;
            for (int d = 0; d < nodes[i].ndisks; d++)
                if (!nodes[i].disks[d].is_fast)
                    hlist[nh++] = d;
            if (nh == 0 || (pass < 2 && nh < 2))
                continue;
            for (int j = 0; j < nh; j++) {
                int d = hlist[(j + (int)(rnd() % (uint64_t)nh)) % nh];
                int nc = r13_count_chunks_in_disk(nodes[i].disks[d].path, rep_number);
                if (pass == 0 && nc == 0)
                    continue;
                target = i;
                hd = d;
                sib_d = nh >= 2 ? hlist[(hlist[0] == d) ? 1 : 0] : -1;
                held_pos = spos;
                break;
            }
        }
    }
    if (target < 0 || hd < 0)
        return;

    pid_t pid0 = 0;
    node_snap(target, &pid0, NULL, NULL);
    uint32_t nid = atomic_load(&nodes[target].id);
    int ch_before = r13_count_chunks_in_disk(nodes[target].disks[hd].path, rep_number);
    int sib_before = sib_d >= 0 ? r13_count_chunks_in_disk(nodes[target].disks[sib_d].path, rep_number) : 0;

    ev("r13: hddeio: injecting EIO on node %d disk %s (pos=%d, chunks=%d, sib_chunks=%d)", target,
       nodes[target].disks[hd].path, held_pos, ch_before, sib_before);
    r13_outage_disk(target, hd, "eio");

    /* Wait for HDD to transition DOWN -> OUT (failstop_out_s=3 s) while verifying process stays alive! */
    int reached_out = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 25000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "hddeio");
        if (r13_disk_meta_state(nid, nodes[target].disks[hd].store_id) == META_STORE_OUT) {
            reached_out = 1;
            break;
        }
    }
    if (!reached_out)
        die(1, "r13: hddeio: node %d HDD %s did not reach META_STORE_OUT within 25 s under EIO", target,
            nodes[target].disks[hd].hex);

    if (want_ec_k && ec_settles && sib_d >= 0 && held_pos >= 0 && ch_before > 0) {
        int rebuilt = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 45000; hold_ms(250)) {
            r13_assert_pid_alive(target, pid0, "hddeio rebuild");
            int nvc = r13_nvme_chunk_count(target, rep_number);
            if (nvc > 0) {
                r13_nvme_chunks_bad++;
                die(1, "r13: hddeio: %d rebuilt chunk(s) landed on NVMe fast disk of node %d!", nvc, target);
            }
            if (r13_count_chunks_in_disk(nodes[target].disks[sib_d].path, rep_number) >= sib_before + ch_before) {
                rebuilt = 1;
                r13_hddeio_rebuilds++;
                break;
            }
            /* If auto-balance moved held_pos off target onto a 2-HDD peer, wait until that position is WHOLE. */
            if (r13_node_stripe_pos(nid, rep_number) != held_pos && r13_stripe_pos_whole(rep_number, held_pos)) {
                rebuilt = 1;
                r13_hddeio_rebuilds++;
                break;
            }
        }
        if (!rebuilt)
            die(1, "r13: hddeio: %d lost chunk(s) on node %d HDD did not rebuild onto sibling HDD within 45 s",
                ch_before, target);
    } else {
        hold_ms(1500);
        r13_assert_pid_alive(target, pid0, "hddeio");
    }

    r13_disk_replace_swap(target, hd);
    r13_assert_pid_alive(target, pid0, "hddeio post-replace");
    r13_hddeio_replaced++;
    r13_hddeio_done++;
    effect_seen = 1;
}

/* 2. nvmestuck (§15.9): one NVMe stuck (outagefs hang) under load; verifies leader failover within disk_stall_s +
 * failover, server process stays alive (catches TSDB_DISKS_MUTANT=stallexit), and heal reopens replica & rejoins ISR. */
static void nem_nvmestuck(void)
{
    if (!rep_wait_whole(60000))
        die(1, "r13: nvmestuck: kvr not whole before round");
    struct rrec pre;
    if (rep_rec_get(&pre) != 0 || pre.leader < 0)
        return;

    /* Prefer kvr's current leader so every round proves fast-disk stall triggers leader failover without process exit. */
    int target = pre.leader;
    int nv_d = r13_node_disk_of_vol(target, rep_number);
    if (nv_d < 0 || !nodes[target].disks[nv_d].is_fast) {
        for (int k = 0; k < pre.nrep; k++) {
            int ni = pre.node[k];
            int d = r13_node_disk_of_vol(ni, rep_number);
            if (d >= 0 && nodes[ni].disks[d].is_fast) {
                target = ni;
                nv_d = d;
                break;
            }
        }
    }
    if (nv_d < 0 || !nodes[target].disks[nv_d].is_fast)
        return;

    int was_leader = (target == pre.leader);
    uint64_t e0 = pre.epoch;
    uint64_t og0 = rrec_open_gen(&pre, target);
    pid_t pid0 = 0;
    node_snap(target, &pid0, NULL, NULL);
    uint32_t nid = atomic_load(&nodes[target].id);

    /* Fast-disk DOWN intentionally reports LOCAL_FAULT on its volumes (never DIVERGED). */
    r9_loss_expected = 1;
    ev("r13: nvmestuck: hanging NVMe %s on node %d (was_leader=%d, epoch=%" PRIu64 ")",
       nodes[target].disks[nv_d].path, target, was_leader, e0);
    r13_outage_disk(target, nv_d, "hang");

    int down_seen = 0, failed_over = !was_leader;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(150)) {
        r13_assert_pid_alive(target, pid0, "nvmestuck");
        if (!down_seen && r13_disk_meta_state(nid, nodes[target].disks[nv_d].store_id) == META_STORE_DOWN)
            down_seen = 1;
        if (!failed_over) {
            struct rrec cur;
            if (rep_rec_get(&cur) == 0 && cur.epoch > e0 && cur.leader >= 0 && cur.leader != target)
                failed_over = 1;
        }
        if (down_seen && failed_over)
            break;
    }
    if (!down_seen || !failed_over) {
        r13_outage_disk(target, nv_d, "heal");
        die(1, "r13: nvmestuck: node %d NVMe hang did not mark DOWN (%d) and fail over kvr (%d) within 20 s",
            target, down_seen, failed_over);
    }
    if (was_leader)
        r13_nvmestuck_failovers++;

    /* Keep NVMe hung briefly (strictly < rebuild_after_s) while verifying writes proceed on surviving leader
     * and target process stays alive! */
    uint64_t ok0 = atomic_load(&kv_ok);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 3000; hold_ms(150))
        r13_assert_pid_alive(target, pid0, "nvmestuck hold");
    if (atomic_load(&kv_ok) == ok0) {
        /* Give surviving leader a few more seconds to acknowledge a write while NVMe stays hung. */
        for (int64_t t0 = now_ms(); now_ms() - t0 < 6000 && atomic_load(&kv_ok) == ok0; hold_ms(150))
            r13_assert_pid_alive(target, pid0, "nvmestuck write progress");
    }

    r13_outage_disk(target, nv_d, "heal");

    /* Wait for NVMe to return UP, kvr to reopen (open_gen > og0), and full ISR recovery — same pid! */
    int recovered = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 60000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "nvmestuck heal");
        rep_sync_members();
        struct rrec post;
        if (r13_disk_meta_state(nid, nodes[target].disks[nv_d].store_id) != META_STORE_UP || rep_rec_get(&post) != 0)
            continue;
        int has_t = 0, nisr = 0, any_lrn = 0;
        for (int r = 0; r < post.nrep; r++) {
            if (post.node[r] == target)
                has_t = 1;
            if (post.isr_has[r])
                nisr++;
            if (post.learner[r])
                any_lrn = 1;
        }
        int isr_full = (nisr == post.nrep && post.nrep >= 3 && !any_lrn && !post.moving);
        if (isr_full && ((!has_t) || rrec_open_gen(&post, target) > og0)) {
            recovered = 1;
            break;
        }
    }
    if (!recovered)
        die(1, "r13: nvmestuck: node %d NVMe did not heal, reopen (open_gen > %" PRIu64 "), and rejoin ISR within 60 s",
            target, og0);

    r13_nvmestuck_reopened++;
    r13_nvmestuck_done++;
    effect_seen = 1;
}

/* 3. diskloss (§15.9, with --disks): one disk of EACH class (one :hdd and one :nvme) removed/lost, rebuilt onto its
 * sibling of the same class, replaced empty and re-initialised with `tsdb disk init`, and adopted UP. */
static void nem_r13_diskloss(void)
{
    if (!rep_wait_whole(60000))
        die(1, "r13: diskloss: kvr not whole before round");
    r9_loss_expected = 1;

    int did_any = 0;

    /* ---- Part A: capacity (:hdd) disk loss -> OUT -> sibling HDD rebuild -> wipe & re-init -> UP */
    if (want_ec_k && ec_settles && r13_diskloss_done == 0) {
        for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(300)) {
            int any = 0;
            for (int i = 0; i < nn && !any; i++) {
                if (r13_node_stripe_pos(atomic_load(&nodes[i].id), rep_number) < 0)
                    continue;
                for (int d = 0; d < nodes[i].ndisks && !any; d++)
                    if (!nodes[i].disks[d].is_fast && r13_count_chunks_in_disk(nodes[i].disks[d].path, rep_number) > 0)
                        any = 1;
            }
            if (any)
                break;
        }
    }
    int hnode = -1, hd = -1, hsib = -1, hpos = -1;
    int off = rnd_range(0, nn - 1);
    for (int pass = 0; pass < 3 && hnode < 0; pass++) {
        for (int k = 0; k < nn && hnode < 0; k++) {
            int i = (off + k) % nn;
            if (nodes[i].dead_for_good)
                continue;
            int spos = (want_ec_k && ec_settles) ? r13_node_stripe_pos(atomic_load(&nodes[i].id), rep_number) : -1;
            if (want_ec_k && ec_settles && pass < 2 && spos < 0)
                continue;
            int hlist[R13_MAX_DISKS], nh = 0;
            for (int d = 0; d < nodes[i].ndisks; d++)
                if (!nodes[i].disks[d].is_fast)
                    hlist[nh++] = d;
            if (nh == 0 || (pass < 2 && nh < 2))
                continue;
            for (int j = 0; j < nh; j++) {
                int d = hlist[(j + (int)(rnd() % (uint64_t)nh)) % nh];
                if (pass == 0 && r13_count_chunks_in_disk(nodes[i].disks[d].path, rep_number) == 0)
                    continue;
                hnode = i;
                hd = d;
                hsib = nh >= 2 ? hlist[(hlist[0] == d) ? 1 : 0] : -1;
                hpos = spos;
                break;
            }
        }
    }
    if (hnode >= 0 && hd >= 0) {
        uint32_t hnid = atomic_load(&nodes[hnode].id);
        int hc_before = r13_count_chunks_in_disk(nodes[hnode].disks[hd].path, rep_number);
        int hsib_before = hsib >= 0 ? r13_count_chunks_in_disk(nodes[hnode].disks[hsib].path, rep_number) : 0;
        ev("r13: diskloss(hdd): node %d disk %s (pos=%d, chunks=%d)", hnode, nodes[hnode].disks[hd].path, hpos,
           hc_before);
        if (use_outagefs) {
            r13_outage_disk(hnode, hd, "eio");
        } else {
            uint8_t ob[24];
            size_t obl = cluster_disk_op_encode(hnid, nodes[hnode].disks[hd].store_id, ob);
            if (admin_any(CLUSTER_OP_DISK_OUT, ob, obl, 15000) != 0)
                die(1, "r13: diskloss(hdd): CLUSTER_OP_DISK_OUT failed on node %d", hnode);
        }
        int is_out = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 25000; hold_ms(200))
            if (r13_disk_meta_state(hnid, nodes[hnode].disks[hd].store_id) == META_STORE_OUT) {
                is_out = 1;
                break;
            }
        if (!is_out)
            die(1, "r13: diskloss(hdd): node %d HDD did not reach META_STORE_OUT within 25 s", hnode);
        if (want_ec_k && ec_settles && hsib >= 0 && hpos >= 0 && hc_before > 0) {
            int rb = 0;
            for (int64_t t0 = now_ms(); now_ms() - t0 < 45000; hold_ms(250)) {
                int nvc = r13_nvme_chunk_count(hnode, rep_number);
                if (nvc > 0) {
                    r13_nvme_chunks_bad++;
                    die(1, "r13: diskloss(hdd): %d rebuilt chunk(s) landed on NVMe of node %d!", nvc, hnode);
                }
                if (r13_count_chunks_in_disk(nodes[hnode].disks[hsib].path, rep_number) >= hsib_before + hc_before) {
                    rb = 1;
                    break;
                }
                if (r13_node_stripe_pos(hnid, rep_number) != hpos && r13_stripe_pos_whole(rep_number, hpos)) {
                    rb = 1;
                    break;
                }
            }
            if (!rb)
                die(1, "r13: diskloss(hdd): lost chunks did not rebuild onto sibling HDD of node %d within 45 s", hnode);
            r13_diskloss_rebuilds++;
        }
        r13_disk_replace_swap(hnode, hd);
        r13_diskloss_hdd++;
        r13_diskloss_replaced++;
        did_any = 1;
    }

    /* ---- Part B: fast (:nvme) disk loss -> OUT -> sibling NVMe or peer log replica rebuild -> wipe & re-init -> UP */
    struct rrec rr;
    int fnode = -1, fd_old = -1, fd_sib = -1;
    if (rep_rec_get(&rr) == 0) {
        int roff = rnd_range(0, rr.nrep - 1);
        for (int k = 0; k < rr.nrep && fnode < 0; k++) {
            int ni = rr.node[(roff + k) % rr.nrep];
            if (ni < 0 || nodes[ni].dead_for_good)
                continue;
            int d_cur = r13_node_disk_of_vol(ni, rep_number);
            if (d_cur < 0 || !nodes[ni].disks[d_cur].is_fast)
                continue;
            for (int d = 0; d < nodes[ni].ndisks; d++) {
                if (d != d_cur && nodes[ni].disks[d].is_fast) {
                    fnode = ni;
                    fd_old = d_cur;
                    fd_sib = d;
                    break;
                }
            }
        }
    }
    if (fnode >= 0 && fd_old >= 0 && fd_sib >= 0) {
        uint32_t fnid = atomic_load(&nodes[fnode].id);
        ev("r13: diskloss(nvme): node %d disk %s -> sibling %s", fnode, nodes[fnode].disks[fd_old].path,
           nodes[fnode].disks[fd_sib].path);
        if (use_outagefs) {
            r13_outage_disk(fnode, fd_old, "eio");
            /* Wait for automatic DOWN -> OUT via rebuild_after_s (10 s). */
        } else {
            uint8_t ob[24];
            size_t obl = cluster_disk_op_encode(fnid, nodes[fnode].disks[fd_old].store_id, ob);
            if (admin_any(CLUSTER_OP_DISK_OUT, ob, obl, 15000) != 0)
                die(1, "r13: diskloss(nvme): CLUSTER_OP_DISK_OUT failed on node %d", fnode);
        }
        int out_wait_ms = (int)S.rebuild_after_s * 1000 + 20000;
        int is_out = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < out_wait_ms; hold_ms(200))
            if (r13_disk_meta_state(fnid, nodes[fnode].disks[fd_old].store_id) == META_STORE_OUT) {
                is_out = 1;
                break;
            }
        if (!is_out)
            die(1, "r13: diskloss(nvme): node %d NVMe did not reach META_STORE_OUT within %d ms", fnode, out_wait_ms);

        /* Wait for kvr log replica to rebuild onto sibling NVMe fd_sib or onto a healthy peer and reach full ISR. */
        char vnew[PATH_MAX + 64];
        snprintf(vnew, sizeof vnew, "%s/v%" PRIu32, nodes[fnode].disks[fd_sib].path, rep_number);
        int rebuilt = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 60000; hold_ms(250)) {
            rep_sync_members();
            struct rrec cur;
            if (rep_rec_get(&cur) != 0)
                continue;
            int has_fnode = 0, nisr = 0, any_lrn = 0;
            for (int r = 0; r < cur.nrep; r++) {
                if (cur.node[r] == fnode)
                    has_fnode = 1;
                if (cur.isr_has[r])
                    nisr++;
                if (cur.learner[r])
                    any_lrn = 1;
            }
            int isr_full = (nisr == cur.nrep && cur.nrep >= 3 && !any_lrn && !cur.moving);
            struct stat sb;
            if (isr_full && ((!has_fnode) || (stat(vnew, &sb) == 0 && S_ISDIR(sb.st_mode)))) {
                rebuilt = 1;
                break;
            }
        }
        if (!rebuilt)
            die(1, "r13: diskloss(nvme): kvr replica on node %d did not rebuild with full ISR within 60 s", fnode);

        r13_disk_replace_swap(fnode, fd_old);
        rep_sync_members();
        r13_diskloss_nvme++;
        r13_diskloss_rebuilds++;
        r13_diskloss_replaced++;
        did_any = 1;
    }

    if (did_any) {
        r13_diskloss_done++;
        effect_seen = 1;
    }
}

/* 4. diskback (§15.9): a disk away (hung) and back within rebuild_after_s; verifies it stays META_STORE_DOWN
 * (never META_STORE_OUT) and attaches cleanly back to META_STORE_UP with 0 rebuild. */
static void nem_diskback(void)
{
    if (!rep_wait_whole(60000))
        die(1, "r13: diskback: kvr not whole before round");

    /* Alternate HDD (even rounds) and NVMe (odd rounds) when both classes are configured. */
    int want_hdd = (r13_diskback_done % 2 == 0);
    int have_hdd = 0;
    for (int d = 0; d < r13_ndisks_cfg; d++)
        if (strcmp(r13_disk_cls[d], "hdd") == 0)
            have_hdd = 1;
    if (!have_hdd)
        want_hdd = 0;

    int hold_s = opt_diskback_hold_s > 0 ? opt_diskback_hold_s : rnd_range(5, 8);
    if ((uint32_t)hold_s >= S.rebuild_after_s)
        hold_s = (int)(S.rebuild_after_s > 6 ? S.rebuild_after_s - 5 : 2);

    if (want_hdd) {
        if (want_ec_k && ec_settles && r13_diskback_done == 0) {
            for (int64_t t0 = now_ms(); now_ms() - t0 < 20000; hold_ms(300)) {
                int any = 0;
                for (int i = 0; i < nn && !any; i++)
                    for (int d = 0; d < nodes[i].ndisks && !any; d++)
                        if (!nodes[i].disks[d].is_fast &&
                            r13_count_chunks_in_disk(nodes[i].disks[d].path, rep_number) > 0)
                            any = 1;
                if (any)
                    break;
            }
        }
        int target = -1, hd = -1;
        int off = rnd_range(0, nn - 1);
        for (int pass = 0; pass < 2 && target < 0; pass++) {
            for (int k = 0; k < nn && target < 0; k++) {
                int i = (off + k) % nn;
                if (nodes[i].dead_for_good)
                    continue;
                if (pass == 0 && want_ec_k && ec_settles &&
                    r13_node_stripe_pos(atomic_load(&nodes[i].id), rep_number) < 0)
                    continue;
                for (int d = 0; d < nodes[i].ndisks; d++) {
                    if (nodes[i].disks[d].is_fast)
                        continue;
                    if (pass == 0 && want_ec_k && ec_settles &&
                        r13_count_chunks_in_disk(nodes[i].disks[d].path, rep_number) == 0)
                        continue;
                    target = i;
                    hd = d;
                    break;
                }
            }
        }
        if (target < 0 || hd < 0)
            return;

        pid_t pid0 = 0;
        node_snap(target, &pid0, NULL, NULL);
        uint32_t nid = atomic_load(&nodes[target].id);
        long loff = log_size(target);
        int chunks_pre = r13_count_chunks_in_disk(nodes[target].disks[hd].path, rep_number);

        ev("r13: diskback(hdd): hanging %s on node %d for %d s (rebuild_after_s=%u, chunks=%d)",
           nodes[target].disks[hd].path, target, hold_s, S.rebuild_after_s, chunks_pre);
        r13_outage_disk(target, hd, "hang");

        int down_seen = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200)) {
            r13_assert_pid_alive(target, pid0, "diskback(hdd)");
            int st = r13_disk_meta_state(nid, nodes[target].disks[hd].store_id);
            if (st == META_STORE_OUT) {
                r13_diskback_out_bad++;
                r13_outage_disk(target, hd, "heal");
                die(1, "r13: diskback(hdd): node %d HDD %s went META_STORE_OUT before rebuild_after_s (%u s)",
                    target, nodes[target].disks[hd].hex, S.rebuild_after_s);
            }
            if (st == META_STORE_DOWN) {
                down_seen = 1;
                break;
            }
        }
        if (!down_seen) {
            r13_outage_disk(target, hd, "heal");
            die(1, "r13: diskback(hdd): node %d HDD %s did not enter META_STORE_DOWN within 15 s", target,
                nodes[target].disks[hd].hex);
        }

        /* Hold away for hold_s (< rebuild_after_s), verifying it stays DOWN (never OUT). */
        for (int64_t t0 = now_ms(); now_ms() - t0 < (int64_t)hold_s * 1000LL; hold_ms(400)) {
            r13_assert_pid_alive(target, pid0, "diskback(hdd) hold");
            if (r13_disk_meta_state(nid, nodes[target].disks[hd].store_id) == META_STORE_OUT) {
                r13_diskback_out_bad++;
                r13_outage_disk(target, hd, "heal");
                die(1, "r13: diskback(hdd): node %d HDD %s went META_STORE_OUT during %d s hold (rebuild_after_s=%u s)",
                    target, nodes[target].disks[hd].hex, hold_s, S.rebuild_after_s);
            }
        }

        r13_outage_disk(target, hd, "heal");
        r13_reapply_hdd_disk(target, hd);

        int back_up = 0;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; hold_ms(200)) {
            r13_assert_pid_alive(target, pid0, "diskback(hdd) attach");
            int st = r13_disk_meta_state(nid, nodes[target].disks[hd].store_id);
            if (st == META_STORE_OUT) {
                r13_diskback_out_bad++;
                die(1, "r13: diskback(hdd): node %d HDD %s went META_STORE_OUT after heal", target,
                    nodes[target].disks[hd].hex);
            }
            if (st == META_STORE_UP) {
                back_up = 1;
                break;
            }
        }
        if (!back_up)
            die(1, "r13: diskback(hdd): node %d HDD %s did not attach back to META_STORE_UP within 30 s", target,
                nodes[target].disks[hd].hex);

        int chunks_post = r13_count_chunks_in_disk(nodes[target].disks[hd].path, rep_number);
        if (chunks_post < chunks_pre) {
            r13_diskback_rebuilds++;
            die(1, "r13: diskback(hdd): node %d HDD chunks shrank across return (%d -> %d)", target, chunks_pre,
                chunks_post);
        }
        if (!log_has_since(target, loff, "attached as REPLACING", NULL) &&
            !log_has_since(target, loff, "reconciled and promoted REPLACING -> UP", NULL))
            ev("r13: diskback(hdd): note: node %d HDD returned UP without REPLACING log (no chunks?)", target);

        r13_diskback_attaches++;
        r13_diskback_done++;
        effect_seen = 1;
        return;
    }

    /* Case NVMe away and back within rebuild_after_s: reopen from disk with unchanged vdir_ino (0 snapshot rebuild). */
    int target = rep_follower_pick();
    if (target < 0)
        return;
    int nv_d = r13_node_disk_of_vol(target, rep_number);
    if (nv_d < 0 || !nodes[target].disks[nv_d].is_fast)
        return;
    struct rrec pre;
    if (rep_rec_get(&pre) != 0)
        return;
    pid_t pid0 = 0;
    node_snap(target, &pid0, NULL, NULL);
    uint32_t nid = atomic_load(&nodes[target].id);
    uint64_t og0 = rrec_open_gen(&pre, target);
    unsigned long long ino0 = vdir_ino(target);

    r9_loss_expected = 1;
    ev("r13: diskback(nvme): hanging %s on node %d for %d s (rebuild_after_s=%u, ino=%llu)",
       nodes[target].disks[nv_d].path, target, hold_s, S.rebuild_after_s, ino0);
    r13_outage_disk(target, nv_d, "hang");

    int down_seen = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "diskback(nvme)");
        int st = r13_disk_meta_state(nid, nodes[target].disks[nv_d].store_id);
        if (st == META_STORE_OUT) {
            r13_diskback_out_bad++;
            r13_outage_disk(target, nv_d, "heal");
            die(1, "r13: diskback(nvme): node %d NVMe went META_STORE_OUT before rebuild_after_s (%u s)", target,
                S.rebuild_after_s);
        }
        if (st == META_STORE_DOWN) {
            down_seen = 1;
            break;
        }
    }
    if (!down_seen) {
        r13_outage_disk(target, nv_d, "heal");
        die(1, "r13: diskback(nvme): node %d NVMe did not enter META_STORE_DOWN within 15 s", target);
    }

    for (int64_t t0 = now_ms(); now_ms() - t0 < (int64_t)hold_s * 1000LL; hold_ms(400)) {
        r13_assert_pid_alive(target, pid0, "diskback(nvme) hold");
        if (r13_disk_meta_state(nid, nodes[target].disks[nv_d].store_id) == META_STORE_OUT) {
            r13_diskback_out_bad++;
            r13_outage_disk(target, nv_d, "heal");
            die(1, "r13: diskback(nvme): node %d NVMe went META_STORE_OUT during %d s hold", target, hold_s);
        }
    }

    r13_outage_disk(target, nv_d, "heal");

    int back_up = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 45000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "diskback(nvme) heal");
        struct rrec post;
        uint64_t ver;
        if (r13_disk_meta_state(nid, nodes[target].disks[nv_d].store_id) == META_STORE_UP &&
            rep_rec_get(&post) == 0 && rrec_open_gen(&post, target) > og0 &&
            rep_isr_mask(&ver) == rep_full_mask()) {
            back_up = 1;
            break;
        }
    }
    if (!back_up)
        die(1, "r13: diskback(nvme): node %d NVMe did not reopen and rejoin ISR within 45 s", target);

    unsigned long long ino1 = vdir_ino(target);
    if (ino0 && ino1 && ino0 != ino1) {
        r13_diskback_rebuilds++;
        die(1, "r13: diskback(nvme): node %d kvr replica directory inode changed (%llu -> %llu): snapshot rebuild happened within rebuild_after_s!",
            target, ino0, ino1);
    }
    r13_diskback_attaches++;
    r13_diskback_done++;
    effect_seen = 1;
}

/* 5. nvmefill (§15.9): a fast disk holding kvr fills to FULL (>= young_high_pct) under ongoing ingest; verifies
 * kvr writes are refused with TSDB_WHY_FULL while sibling volume r13_sib_name on the SAME node's OTHER NVMe disk
 * keeps accepting writes cleanly with 0 refusals (catches TSDB_DISKS_MUTANT=fullall). */
static void nem_nvmefill(void)
{
    if (r13_sib_target_node < 0 || !r13_sib_num || !r13_sib_sid)
        return;
    int target = r13_sib_target_node;
    int kd = r13_node_disk_of_vol(target, rep_number);
    int od = r13_node_disk_of_vol(target, r13_sib_num);
    if (kd < 0 || od < 0 || kd == od)
        return;

    pid_t pid0 = 0;
    node_snap(target, &pid0, NULL, NULL);
    const char *khex = nodes[target].disks[kd].hex;
    const char *kcls = nodes[target].disks[kd].cls;

    uint64_t sz = 0, yb = 0;
    if (!r13_scrape_disk_young_bytes(target, khex, kcls, &sz, &yb) || sz == 0) {
        sz = 100ULL * 1024 * 1024;
        yb = 1024 * 1024;
    }

    /* Start by setting a tight virtual cap slightly above current young bytes (~88% full) so ongoing ingest
     * drives the disk past 90% naturally; if settles shrink young bytes before crossing 90%, fall back to phantom
     * bytes at 92% on ONLY this fast disk so FULL triggers reliably within a few seconds. */
    uint64_t wf0 = atomic_load(&kv_why_full);
    char ubuf[256];
    if (yb >= 65536) {
        uint64_t vcap = yb + (yb / 10u) + 16384u;
        snprintf(ubuf, sizeof ubuf, "%s size %" PRIu64 "\n", khex, vcap);
    } else {
        uint64_t ph92 = (sz / 100u) * 92u + (sz % 100u);
        snprintf(ubuf, sizeof ubuf, "%s phantom %" PRIu64 " 0\n", khex, ph92);
    }
    r13_write_usage_file(target, ubuf);
    ev("r13: nvmefill: capped node %d fast disk %s (young=%" PRIu64 ", size=%" PRIu64 ")", target, khex, yb, sz);

    char kvr_full_key[64], sib_full_key[64];
    snprintf(kvr_full_key, sizeof kvr_full_key, "tsdb_volume_full{volume=\"%" PRIu32 "\"} ", rep_number);
    snprintf(sib_full_key, sizeof sib_full_key, "tsdb_volume_full{volume=\"%" PRIu32 "\"} ", r13_sib_num);

    int saw_kvr_full = 0, nudged = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "nvmefill ramp");
        int64_t vf = 0;
        if (r13_scrape_metric_i64(target, kvr_full_key, &vf) && vf == 1 && atomic_load(&kv_why_full) > wf0) {
            saw_kvr_full = 1;
            break;
        }
        if (!nudged && now_ms() - t0 >= 3000) {
            uint64_t ph92 = (sz / 100u) * 92u + (sz % 100u);
            snprintf(ubuf, sizeof ubuf, "%s phantom %" PRIu64 " 0\n", khex, ph92);
            r13_write_usage_file(target, ubuf);
            nudged = 1;
        }
    }
    if (!saw_kvr_full) {
        r13_write_usage_file(target, NULL);
        die(1, "r13: nvmefill: kvr did not enter FULL with TSDB_WHY_FULL within 15 s on node %d disk %s", target, khex);
    }

    /* Let heartbeats propagate the FULL flag across the cluster (~400 ms), then verify:
     *  (a) tsdb_volume_full{volume="<r13_sib_num>"} == 0 on `target` (NOT 1!)
     *  (b) writes to r13_sib_name (on target's OTHER NVMe disk!) succeed with 0 TSDB_WHY_FULL refusals! */
    hold_ms(400);
    int64_t sib_vf = -1;
    if (r13_scrape_metric_i64(target, sib_full_key, &sib_vf) && sib_vf != 0) {
        r13_fullall_bad++;
        fprintf(stderr,
                "jepsen-cluster: FAIL: r13: nvmefill: sibling volume %s (v%" PRIu32 ", not on full disk %s) "
                "reported tsdb_volume_full=%" PRId64 " on node %d (fullall!)\n",
                r13_sib_name, r13_sib_num, khex, sib_vf, target);
    }

    int sib_lead = r13_vol_leader_node(r13_sib_num);
    for (int k = 0; k < 10; k++) {
        r13_assert_pid_alive(target, pid0, "nvmefill sibling probe");
        if (sib_lead < 0)
            sib_lead = r13_vol_leader_node(r13_sib_num);
        tsdbc *cs = NULL;
        if (sib_lead >= 0 && connect_node(sib_lead, 1, &cs) == 0) {
            int64_t ts = r13_sib_ts_next;
            r13_sib_ts_next += 1000;
            float fv = (float)(k + 1);
            int rc = tsdbc_insert_series(cs, r13_sib_sid, &ts, &fv, 1);
            if (rc == 0) {
                r13_nvmefill_sib_ok++;
            } else if (rc == TSDB_ERR_UNAVAILABLE) {
                tsdb_unavail u[4];
                unsigned nu = 0;
                int is_full = 0;
                if (tsdbc_unavailable(cs, u, 4, &nu) == 0)
                    for (unsigned ui = 0; ui < nu; ui++)
                        if (u[ui].reason == TSDB_WHY_FULL)
                            is_full = 1;
                if (is_full) {
                    r13_nvmefill_sib_refusals++;
                    r13_fullall_bad++;
                    fprintf(stderr,
                            "jepsen-cluster: FAIL: r13: nvmefill: write to volume %s (v%" PRIu32
                            ", no replica on full disk %s) was refused with TSDB_WHY_FULL (fullall!)\n",
                            r13_sib_name, r13_sib_num, khex);
                }
            } else if (rc == TSDB_ERR_MOVED) {
                sib_lead = r13_vol_leader_node(r13_sib_num);
            }
            tsdbc_close(cs);
        }
        hold_ms(150);
    }

    /* Lift fill override and verify kvr clears FULL and resumes acknowledging writes cleanly. */
    r13_write_usage_file(target, NULL);
    int cleared = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "nvmefill clear");
        int64_t vf = 1;
        if (r13_scrape_metric_i64(target, kvr_full_key, &vf) && vf == 0) {
            cleared = 1;
            break;
        }
    }
    uint64_t ok0 = atomic_load(&kv_ok);
    int resumed = 0;
    for (int64_t t0 = now_ms(); cleared && now_ms() - t0 < 15000; hold_ms(200)) {
        r13_assert_pid_alive(target, pid0, "nvmefill resume");
        if (atomic_load(&kv_ok) > ok0) {
            resumed = 1;
            break;
        }
    }
    if (!cleared || !resumed) {
        r13_resume_bad++;
        fprintf(stderr,
                "jepsen-cluster: FAIL: r13: nvmefill: kvr did not clear FULL (%d) and resume writes (%d) after lifting fill on node %d\n",
                cleared, resumed, target);
    }

    r13_nvmefill_kvr_refusals += atomic_load(&kv_why_full) - wf0;
    r13_nvmefill_done++;
    effect_seen = 1;
}

/* ---- R13 end check & summary */

static int r13_end_check(void)
{
    if (!r13_disks_on && !(nemeses & N_R13))
        return 0;
    int bad = 0;
    if (r13_proc_exits_bad)
        bad += r13_proc_exits_bad;
    if (r13_fullall_bad)
        bad += r13_fullall_bad;
    if (r13_nvme_chunks_bad)
        bad += r13_nvme_chunks_bad;
    if (r13_resume_bad)
        bad += r13_resume_bad;
    if (r13_diskback_out_bad)
        bad += r13_diskback_out_bad;
    if (r13_diskback_rebuilds)
        bad += (int)r13_diskback_rebuilds;

    if ((nemeses & N_HDDEIO) && r13_hddeio_done == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r13: hddeio ran 0 rounds\n");
        bad++;
    }
    if ((nemeses & N_HDDEIO) && want_ec_k && ec_settles && r13_hddeio_rebuilds == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r13: hddeio verified 0 EC HDD rebuilds\n");
        bad++;
    }
    if ((nemeses & N_NVMESTUCK) && r13_nvmestuck_done == 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r13: nvmestuck ran 0 rounds\n");
        bad++;
    }
    if (r13_disks_on && (nemeses & N_DISKLOSS)) {
        if (r13_diskloss_done == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r13: diskloss ran 0 rounds\n");
            bad++;
        }
        int have_hdd = 0, have_fast = 0;
        for (int d = 0; d < r13_ndisks_cfg; d++) {
            if (strcmp(r13_disk_cls[d], "hdd") == 0)
                have_hdd++;
            else
                have_fast++;
        }
        if (have_hdd && r13_diskloss_hdd == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r13: diskloss removed 0 HDD disks\n");
            bad++;
        }
        if (have_fast >= 2 && r13_diskloss_nvme == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r13: diskloss removed 0 NVMe disks\n");
            bad++;
        }
    }
    if ((nemeses & N_DISKBACK) && (r13_diskback_done == 0 || r13_diskback_attaches == 0)) {
        fprintf(stderr, "jepsen-cluster: FAIL: r13: diskback completed %u round(s) with %u attaches\n",
                r13_diskback_done, r13_diskback_attaches);
        bad++;
    }
    if (nemeses & N_NVMEFILL) {
        if (r13_nvmefill_done == 0 || r13_nvmefill_kvr_refusals == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r13: nvmefill ran %u round(s) with %" PRIu64 " TSDB_WHY_FULL refusals on kvr\n",
                    r13_nvmefill_done, r13_nvmefill_kvr_refusals);
            bad++;
        }
        if (r13_nvmefill_sib_ok == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r13: nvmefill completed 0 successful writes to sibling volume %s\n",
                    r13_sib_name);
            bad++;
        }
        if (r13_nvmefill_sib_refusals > 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r13: nvmefill refused %u write(s) on sibling volume %s with TSDB_WHY_FULL\n",
                    r13_nvmefill_sib_refusals, r13_sib_name);
            bad++;
        }
    }
    /* Verify zero chunks landed on any NVMe/SSD fast disk on mixed (fast + hdd) servers. */
    if (r13_disks_on && want_ec_k && ec_settles) {
        for (int i = 0; i < nn; i++) {
            int nvc = r13_nvme_chunk_count(i, rep_number);
            if (nvc > 0) {
                fprintf(stderr, "jepsen-cluster: FAIL: r13: node %d holds %d EC chunk(s) on fast disk despite HDD stores\n",
                        i, nvc);
                bad++;
            }
        }
    }
    return bad;
}

static void r13_summary(void)
{
    if (!r13_disks_on && !(nemeses & N_R13))
        return;
    printf("r13: disks %s (hdd slow=%dms rate=%.0fMB/s); hddeio %u (rebuilds %u, replaced %u), "
           "nvmestuck %u (failovers %u, reopened %u), diskloss %u (hdd %u, nvme %u, rebuilds %u, replaced %u), "
           "diskback %u (attaches %u, rebuilds %u), nvmefill %u (kvr_refusals %" PRIu64 ", sib_ok %u, sib_refusals %u)\n",
           r13_disks_on ? r13_disk_spec : "none", opt_hdd_slow_ms, opt_hdd_rate_mb,
           r13_hddeio_done, r13_hddeio_rebuilds, r13_hddeio_replaced,
           r13_nvmestuck_done, r13_nvmestuck_failovers, r13_nvmestuck_reopened,
           r13_diskloss_done, r13_diskloss_hdd, r13_diskloss_nvme, r13_diskloss_rebuilds, r13_diskloss_replaced,
           r13_diskback_done, r13_diskback_attaches, r13_diskback_rebuilds,
           r13_nvmefill_done, r13_nvmefill_kvr_refusals, r13_nvmefill_sib_ok, r13_nvmefill_sib_refusals);
}

#endif /* JEPSEN_CLUSTER_R13_H */
