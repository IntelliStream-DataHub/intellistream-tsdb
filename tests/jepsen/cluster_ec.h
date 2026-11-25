/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * jepsen-cluster with erasure-coded volumes (--ec K+M; docs/design-cluster-data.md 10.3, row r8l-jc). Included by
 * jepsen_cluster.c before its nemesis dispatch; it uses that file's node table, views, admin calls and histories.
 *
 * What is different in an --ec run.
 *  - Setup: every node gets two chunk stores (`tsdb chunk-store init PATH --cluster UUID --node ID`) once the cluster has
 *    member ids, and is restarted with --chunk-dir for both, --metrics-port auto and --ec-settle-grace-s 3 (and
 *    --ec-experimental when this tsdbd has it: `ec_experimental`, found from its --help). kvr is then added with
 *    VOLUME_ADD_EC (m = 2, so 3 replicas as in every kv run), and the workloads of kv, kv-via and classes run on it
 *    unchanged: their keys are back-dated (KV_BASE_TS is in 2023), so their partition ended long before the grace and
 *    settles while the writers go on with late rows into it; the filler's large inserts land a day later and become
 *    the young parts above the settled object (E6). Settles are whole keys (D21).
 *  - Check E1, divergence: in a run without rot or disk loss (`ec_damaged`) no volume may be DIVERGED (`tsdb_volume_diverged`
 *    on every node's /metrics, polled about once a second and at the end) or stopped on its node (a "volume N: stopped:"
 *    line of a node's log, which is what LOCAL_FAULT in the heartbeat is made of). A leader whose digest or cut is wrong
 *    tends to end as a DIVERGED replica while its own answers stay right: a checker that only counts lost writes passes it.
 *  - Check E2, settles: after the run the replicas hold `.ec` parts (counted in the replica directories, with the parts
 *    per partition). A run with none fails when this tsdbd settles at all (`ec_experimental`).
 *  - Check E3, every member: after the final read leadership moves to each ISR member in turn (VOLUME_TRANSFER, as churn
 *    does) and every key is read again through it: the answers must equal the final read. A member that settled
 *    differently, or one whose cut or Y is wrong, answers otherwise.
 *  - Nemeses: partial (a replica of kvr cut from the metadata group's Raft leader alone: the data links of kvr's leader
 *    to it stay, the metadata path is gone), diskloss (a node's chunk stores removed and made again empty with its
 *    cluster and node: the node comes back with no chunk), rot (bytes flipped in a chunk file at rest; needs a tsdbd that
 *    settles). The last two excuse E1.
 *  - Mutants of the harness (--mutant, each must fail, `make jepsen-cluster-selftest`): ec-diverged (a poll that finds a
 *    volume DIVERGED), ec-member-differs (one member's answer has a key changed), ec-nosettle (the part count is 0).
 *    The server's own (ycold, ecabovey: r8h; cutnogate: r8g) are TSDB_REPLD_MUTANT in the tsdbd that runs: the Makefile
 *    runs them when the binary has the name.
 */
#ifndef JEPSEN_CLUSTER_EC_H
#define JEPSEN_CLUSTER_EC_H

static uint8_t ec_uuid[16];
static int ec_damaged;              /* a rot or disk loss happened: DIVERGED is then allowed (reported, not failed) */
static unsigned ec_polls, ec_div_polls, ec_stopped_lines;
static unsigned ec_rots, ec_diskloss, ec_partials, ec_member_reads, ec_member_bad;
static int ec_ec_parts = -1, ec_partitions_max;
static char ec_div_msg[256];

/* does the tsdbd under test settle ec volumes (it takes --ec-experimental)? */
static void ec_detect(void)
{
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    char cmd[PATH_MAX + 32], line[512];
    snprintf(cmd, sizeof cmd, "'%s' --help 2>&1", bin);
    FILE *f = popen(cmd, "r");
    if (!f)
        return;
    while (fgets(line, sizeof line, f))
        if (strstr(line, "--ec-experimental"))
            ec_experimental = 1;
    pclose(f);
}

/* tsdb chunk-store init PATH --cluster UUID --node ID (the CLI, TSDB_CLI): a store of this cluster and node */
static void ec_chunk_init(const char *path, uint32_t id)
{
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char hex[40], ids[16];
    for (int k = 0; k < 16; k++)
        snprintf(hex + 2 * k, 3, "%02x", ec_uuid[k]);
    snprintf(ids, sizeof ids, "%" PRIu32, id);
    pid_t p = fork();
    if (p < 0)
        die(2, "fork: %s", strerror(errno));
    if (p == 0) {
        execl(cli, "tsdb", "chunk-store", "init", path, "--cluster", hex, "--node", ids, (char *)NULL);
        _exit(127);
    }
    int st = 0;
    waitpid(p, &st, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
        die(2, "tsdb chunk-store init %s failed (status 0x%x; TSDB_CLI=%s)", path, st, cli);
}

static void ec_stores_init(int i)
{
    for (int k = 0; k < 2; k++) {
        snprintf(nodes[i].chunk[k], sizeof nodes[i].chunk[k], "%s/chunk%d", nodes[i].dir, k);
        if (mkdir(nodes[i].chunk[k], 0755) != 0 && errno != EEXIST)
            die(2, "mkdir %s: %s", nodes[i].chunk[k], strerror(errno));
        ec_chunk_init(nodes[i].chunk[k], atomic_load(&nodes[i].id));
    }
}

/* after cluster_form: the cluster's uuid, every node's stores, every node restarted with them */
static void ec_form_stores(void)
{
    struct view v;
    if (view_get(0, &v, 5000) != 0)
        die(1, "ec: no STATUS of node 0 for the cluster's uuid");
    memcpy(ec_uuid, v.m.uuid, sizeof ec_uuid);
    view_free(&v);
    for (int i = 0; i < nn; i++) {
        int st = stop_node(i, SIGTERM);
        if (!(WIFEXITED(st) && WEXITSTATUS(st) == 0))
            die(1, "ec: node %d did not stop cleanly to get its chunk stores (status 0x%x)", i, st);
        ec_stores_init(i);
        restart_node(i);
        settle("a chunk store");
    }
}

/* kvr as VOLUME_ADD_EC: replicas 0 is m + 1; the add is refused (FEW) until the members report their stores */
static void ec_add_kvr(void)
{
    uint8_t req[2 + META_NAME_MAX + 6];
    size_t rl = cluster_volume_add_ec_encode("kvr", 0, (uint8_t)want_ec_k, (uint8_t)want_ec_m, req);
    int rc = -1;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 60000; usleep(300000)) {
        rc = admin_real(0, CLUSTER_OP_VOLUME_ADD_EC, req, rl, 1, 15000);
        if (rc == 0)
            return;
        if (rc != CLUSTER_ERR_FEW && rc != CLUSTER_ERR_NOTLEADER)
            break;
    }
    die(1, "volume add kvr ec=%d+%d: %s", want_ec_k, want_ec_m, cluster_strerror(rc));
}

/* GET /metrics of a node into b (0 on success) */
static int ec_http_get(int port, char *b, size_t bn)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct timeval tv = {1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    static const char rq[] = "GET /metrics HTTP/1.0\r\n\r\n";
    size_t n = 0;
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0 || write(fd, rq, sizeof rq - 1) != (ssize_t)(sizeof rq - 1)) {
        close(fd);
        return -1;
    }
    for (ssize_t r; n + 1 < bn && (r = read(fd, b + n, bn - 1 - n)) > 0;)
        n += (size_t)r;
    close(fd);
    b[n] = 0;
    return n > 0 ? 0 : -1;
}

/* a "tsdb_volume_diverged{...} 1" line in a node's metrics text */
static int ec_text_diverged(const char *b)
{
    for (const char *p = b; (p = strstr(p, "tsdb_volume_diverged{")); p++) {
        const char *e = strchr(p, '\n');
        if (e && e - p >= 2 && e[-1] == '1' && e[-2] == ' ')
            return 1;
    }
    return 0;
}

static void ec_poll(void)
{
    static char b[1 << 18];
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        if (pid <= 0 || node_paused(i) || nodes[i].mport <= 0)
            continue;
        ec_polls++;
        int bad = ec_http_get(nodes[i].mport, b, sizeof b) == 0 && ec_text_diverged(b);
        if (!strcmp(mutant, "ec-diverged") && ec_polls == 20) /* planted: a poll that finds a DIVERGED volume */
            bad = 1;
        if (bad && !ec_div_polls++)
            snprintf(ec_div_msg, sizeof ec_div_msg, "node %d reports a volume DIVERGED (tsdb_volume_diverged 1)", i);
    }
}

/* once a second at most, from hold_ms (the control thread) */
static void ec_tick(void)
{
    static int64_t last;
    if (!want_ec_k || now_ms() - last < 1000)
        return;
    last = now_ms();
    ec_poll();
}

/* "volume N: stopped:" in a node's log: the volume failed there (what the heartbeat calls LOCAL_FAULT) */
static unsigned ec_log_stops(void)
{
    unsigned n = 0;
    char key[48], line[1024];
    snprintf(key, sizeof key, "volume %" PRIu32 ": stopped:", (uint32_t)rep_number);
    for (int i = 0; i < nn; i++) {
        FILE *f = fopen(nodes[i].log, "r");
        while (f && fgets(line, sizeof line, f))
            n += strstr(line, key) != NULL;
        if (f)
            fclose(f);
    }
    return n;
}

/* E1 (and the final poll), after the last heal. Returns the number of failures. */
static int ec_div_check(void)
{
    ec_poll();
    ec_stopped_lines = ec_log_stops();
    int bad = 0;
    if (ec_div_polls || ec_stopped_lines)
        printf("jepsen-cluster: ec: %u of %u polls found a volume DIVERGED (%s), %u \"volume stopped\" line(s) in the node logs%s\n",
               ec_div_polls, ec_polls, ec_div_polls ? ec_div_msg : "none", ec_stopped_lines,
               ec_damaged ? " (rot or disk loss ran: allowed)" : "");
    if (!ec_damaged && (ec_div_polls || ec_stopped_lines)) {
        fprintf(stderr, "jepsen-cluster: FAIL: a volume was DIVERGED or stopped in a run without rot or disk loss: %s, %u "
                "stop line(s)\n", ec_div_polls ? ec_div_msg : "no DIVERGED poll", ec_stopped_lines);
        bad++;
    }
    return bad;
}

/* E3: leadership to each ISR member in turn, every key of every history read through it and compared with the final
 * read (kv_final's). The writers are stopped; the ISR is whole. */
static int ec_members_check(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: ec: no record of kvr for the per-member reads\n");
        return 1;
    }
    int bad = 0, order[META_REPLICAS_MAX], no = 0;
    for (int k = 0; k < r.nrep; k++)
        if (r.isr_has[k] && !r.learner[k] && r.node[k] >= 0)
            order[no++] = k;
    for (int q = 0; q < no; q++) {
        int k = order[q], to = r.node[k];
        rep_rec_get(&r); /* the leader may have moved by the last transfer */
        if (r.leader != to) {
            uint8_t body[16];
            size_t bl = cluster_volume_force_encode(rep_number, r.id[k], body);
            int rc = admin_any(CLUSTER_OP_VOLUME_TRANSFER, body, bl, 20000);
            if (rc != 0) {
                fprintf(stderr, "jepsen-cluster: FAIL: ec: kvr was not moved to node %d for its read: %s\n", to,
                        cluster_strerror(rc));
                bad++;
                continue;
            }
            int64_t t0 = now_ms();
            while (now_ms() - t0 < 30000 && !churn_landed_check(to))
                usleep(200000);
        }
        for (int hi = 0; hi < nhists; hi++) {
            struct rd rd;
            memset(&rd, 0, sizeof rd);
            int got = -1;
            for (int64_t t0 = now_ms(); got != 0 && now_ms() - t0 < 30000; usleep(200000)) {
                tsdbc *c = NULL;
                tsdb_sid sid;
                if (connect_node(to, 1, &c) == 0) {
                    if (kv_resolve(c, hi, &sid) == 0)
                        got = kv_read_all(c, sid, &rd);
                    tsdbc_close(c);
                }
            }
            if (got != 0) {
                fprintf(stderr, "jepsen-cluster: FAIL: ec: no read of %s through member node %d\n", hists[hi].name, to);
                bad++;
                continue;
            }
            if (!strcmp(mutant, "ec-member-differs") && q == 1 && hi == 0) /* planted: one member answers differently */
                rd.seen[0] = rd.seen[0] ? 0 : 1;
            ec_member_reads++;
            for (int key = 0; key < KEYS; key++)
                if (rd.seen[key] != hists[hi].final[key]) {
                    fprintf(stderr, "jepsen-cluster: FAIL: ec: %s key %d reads write %u through member node %d, the final "
                            "read had %u\n", hists[hi].name, key, (unsigned)rd.seen[key], to, (unsigned)hists[hi].final[key]);
                    bad++;
                    ec_member_bad++;
                    break;
                }
        }
    }
    return bad;
}

/* E2, after the stop: .ec parts on the replicas, and parts per partition (the K of p<K>_<A>_<B>_<L>) */
static int ec_parts_check(const char *const *dirs, int nd)
{
    int nec = 0, npart = 0, per_max = 0;
    for (int d = 0; d < nd; d++) {
        DIR *dp = opendir(dirs[d]);
        if (!dp)
            continue;
        long keys[256];
        int cnt[256], nk = 0;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            int is_ec = l > 3 && !strcmp(e->d_name + l - 3, ".ec"), is_part = l > 5 && !strcmp(e->d_name + l - 5, ".part");
            if (e->d_name[0] != 'p' || (!is_ec && !is_part))
                continue;
            nec += is_ec;
            npart += is_part;
            long key = strtol(e->d_name + 1, NULL, 10);
            int j = 0;
            while (j < nk && keys[j] != key)
                j++;
            if (j == nk && nk < 256) {
                keys[nk] = key;
                cnt[nk++] = 0;
            }
            if (j < nk && ++cnt[j] > per_max)
                per_max = cnt[j];
        }
        closedir(dp);
    }
    if (!strcmp(mutant, "ec-nosettle")) /* planted: nothing settled */
        nec = 0;
    ec_ec_parts = nec;
    ec_partitions_max = per_max;
    int bad = 0;
    if (nec == 0 && ec_experimental) {
        fprintf(stderr, "jepsen-cluster: FAIL: ec: no .ec part on any replica of kvr after the run: nothing settled\n");
        bad++;
    }
    return bad;
}

/* ---- nemeses */

/* partial: a replica of kvr (not its leader) cut from the Raft leader alone, for a few seconds; kvr's own links stay */
static void nem_partial(void)
{
    int l = atomic_load(&last_leader), kl = atomic_load(&kv_leader), x = -1;
    if (l < 0 || l >= nn)
        l = rnd_range(0, nn - 1);
    ju_fault(-1);
    for (int tries = 0; tries < 20 && (x < 0 || x == l || x == kl); tries++)
        x = rnd_range(0, nn - 1);
    if (x == l || x == kl)
        x = (l + 1) % nn == kl ? (l + 2) % nn : (l + 1) % nn;
    net_cut(l, x, 1);
    ec_partials++;
    effect_seen = 1;
    hold_ms(rnd_range(3000, 8000));
    net_heal();
}

static int ec_chunk_find(const char *dir, int depth, char *out, size_t on, int *seen)
{
    DIR *dp = opendir(dir);
    if (!dp)
        return 0;
    int found = 0;
    for (struct dirent *e; !found && (e = readdir(dp));) {
        if (e->d_name[0] == '.')
            continue;
        char p[PATH_MAX + 64];
        snprintf(p, sizeof p, "%s/%s", dir, e->d_name);
        struct stat sb;
        if (lstat(p, &sb) != 0)
            continue;
        if (S_ISDIR(sb.st_mode)) {
            if (depth < 4)
                found = ec_chunk_find(p, depth + 1, out, on, seen);
        } else if (S_ISREG(sb.st_mode) && sb.st_size >= 8192 && strncmp(e->d_name, "tmp_", 4) && strcmp(e->d_name, "META") &&
                   strncmp(e->d_name, "EPOCH", 5) && strcmp(e->d_name, "LOCK")) {
            if ((*seen)++ == 0 || rnd() % 4 == 0) {
                snprintf(out, on, "%s", p);
                found = rnd() % 2;
            }
        }
    }
    closedir(dp);
    return found;
}

/* rot: bytes flipped in one chunk file of a random node, at rest (the node goes on running) */
static void nem_rot(void)
{
    char path[PATH_MAX + 64] = "";
    int seen = 0;
    for (int tries = 0; tries < 30 && !path[0]; tries++) {
        int i = rnd_range(0, nn - 1);
        seen = 0;
        ec_chunk_find(nodes[i].chunk[rnd() % 2], 0, path, sizeof path, &seen);
        if (!path[0])
            ec_chunk_find(nodes[i].chunk[0], 0, path, sizeof path, &seen);
        if (!path[0])
            hold_ms(500);
    }
    if (!path[0])
        return; /* no chunk yet: the round has no effect and the run says so */
    struct stat sb;
    int fd = open(path, O_RDWR);
    if (fd < 0 || fstat(fd, &sb) != 0)
        die(1, "ec: rot: cannot open %s: %s", path, strerror(errno));
    int flips = rnd_range(1, 8);
    for (int k = 0; k < flips; k++) {
        off_t off = (off_t)(rnd() % (uint64_t)sb.st_size);
        uint8_t b;
        if (pread(fd, &b, 1, off) != 1)
            continue;
        b ^= (uint8_t)(1u << (rnd() % 8));
        if (pwrite(fd, &b, 1, off) != 1)
            die(1, "ec: rot: write to %s: %s", path, strerror(errno));
    }
    close(fd);
    ec_rots++;
    ec_damaged = 1;
    effect_seen = 1;
    ev("rot: %d bit(s) flipped in %s", flips, path);
    hold_ms(rnd_range(1000, 3000));
}

/* diskloss: a node's chunk stores gone, made again empty with its cluster and node, the node started with them */
static void nem_diskloss(void)
{
    int i = rnd_range(0, nn - 1);
    stop_node(i, SIGKILL);
    for (int k = 0; k < 2; k++) {
        rmtree(nodes[i].chunk[k]);
        if (mkdir(nodes[i].chunk[k], 0755) != 0)
            die(1, "ec: diskloss: mkdir %s: %s", nodes[i].chunk[k], strerror(errno));
        ec_chunk_init(nodes[i].chunk[k], atomic_load(&nodes[i].id));
    }
    ec_diskloss++;
    ec_damaged = 1;
    effect_seen = 1;
    hold_ms(rnd_range(200, 1500));
    restart_node(i);
}

static void ec_summary(void)
{
    if (!want_ec_k)
        return;
    printf("ec %d+%d: %s; %u polls of /metrics (%u DIVERGED), %u node-log stop line(s); .ec parts on the replicas: %d, most "
           "parts of one partition on a replica: %d; per-member reads %u (%u differing); partial %u, diskloss %u, rot %u\n",
           want_ec_k, want_ec_m, ec_experimental ? "tsdbd settles (--ec-experimental)" : "tsdbd does not settle yet",
           ec_polls, ec_div_polls, ec_stopped_lines, ec_ec_parts, ec_partitions_max, ec_member_reads, ec_member_bad,
           ec_partials, ec_diskloss, ec_rots);
}

#endif
