/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * jepsen-cluster with erasure-coded volumes (--ec K+M; docs/design-cluster-data.md 10.3, row r8l-jc). Included by
 * jepsen_cluster.c before its nemesis dispatch; it uses that file's node table, views, admin calls and histories.
 *
 * What is different in an --ec run.
 *  - Setup: every node gets two chunk stores (`tsdb chunk-store init PATH --cluster UUID --node ID`) once the cluster has
 *    member ids, and is restarted with --chunk-dir for both, --metrics-port auto and --ec-settle-grace-s 3 (when this
 *    tsdbd settles ec volumes: `ec_settles`, found from `TSDB_EC` and its --help). kvr is then added with
 *    VOLUME_ADD_EC (m + 1 log replicas, 3 for m = 2 and 5 for m = 4), and the workloads of kv, kv-via and classes run on it:
 *    their keys are back-dated (KV_BASE_TS is in 2023), so their partition ended long before the grace and settles while
 *    the writers go on with late rows into it, and (--ec only) a third of the single-key writes are replaces of that key
 *    (a tombstone and a row in one commit: the young part with a tombstone above a settled object, E6); the filler's
 *    large inserts land a day later and become further young parts. Settles are whole keys (D21). Not built: a delete
 *    aimed at a key (the model of check_hist has no "key is gone again") and large inserts that cover keys.
 *    D15: io_uring only on the first two nodes of a run, epoll on the rest (start_node).
 *  - Check E1, divergence: in a run without rot or disk loss (`ec_damaged`) no volume may be DIVERGED (`tsdb_volume_diverged`
 *    on every node's /metrics, polled about once a second by a thread of its own and at the end; the text is read line by
 *    line) or stopped on its node (a "volume N: stopped:"
 *    line of a node's log for kvr's number, counted once at the end: a proxy for LOCAL_FAULT in the heartbeat, which is
 *    not polled (STATUS); the stop of another volume is not seen, and a log a power loss cut loses its line). A leader whose digest or cut is wrong
 *    tends to end as a DIVERGED replica while its own answers stay right: a checker that only counts lost writes passes it.
 *  - Check E2, settles: after the run the replicas hold `.ec` parts (counted in the replica directories, with the parts
 *    per partition). A run with none fails when this tsdbd settles at all (`ec_settles`).
 *  - Check E3, every member: after the final read leadership moves to each ISR member in turn (VOLUME_TRANSFER, as churn
 *    does) and every key is read again through it: the answers must equal the final read. A member that settled
 *    differently, or one whose cut or Y is wrong, answers otherwise. A transfer that did not land within 30 s fails (reads
 *    through a member that does not lead are forwarded to the leader and would equal the final read by construction).
 *  - Nemeses: partial (the metadata group's Raft leader cut from the stripe members of kvr but kvr's leader, whose links
 *    to the stripe stay; the proxy cuts every kind of link between two nodes; net_cut_done: the heal checks the cut held), diskloss (a node's chunk stores removed and made again empty with its
 *    cluster and node: the node comes back with no chunk), rot (bytes flipped in a chunk file at rest; needs a tsdbd that
 *    settles). The last two excuse E1.
 *  - Mutants of the harness (--mutant, each must fail, `make jepsen-cluster-selftest`): ec-diverged (a poll that finds a
 *    volume DIVERGED: tsdbd's own plant, TSDB_REPLD_TEST=plantdiverge and a diverge.N file in a replica's directory, so
 *    the fetch, the parse and the log count are tried against the real thing), ec-member-differs (one member's answer has a key changed), ec-notransfer (no
 *    transfer is made: a read through a member that never led must not count), ec-nosettle (the part count is 0).
 *    The server's own (ycold, ecabovey: r8h; cutnogate: r8g) are TSDB_REPLD_MUTANT in the tsdbd that runs: the Makefile
 *    runs them when the binary has the name.
 */
#ifndef JEPSEN_CLUSTER_EC_H
#define JEPSEN_CLUSTER_EC_H

static uint8_t ec_uuid[16];
static int ec_damaged;              /* a rot or disk loss happened: DIVERGED is then allowed (reported, not failed) */
static _Atomic unsigned ec_polls, ec_div_polls, ec_poll_errs; /* written by the poller thread, read after its join */
static unsigned ec_stopped_lines;
static unsigned ec_transfers, ec_rots, ec_diskloss, ec_partials, ec_member_reads, ec_member_bad;
static _Atomic int ec_poll_run;
static pthread_t ec_poll_th;
static int ec_poll_started;
static int ec_ec_parts = -1, ec_partitions_max;
static char ec_div_msg[256];
static void ec_poller_start(void);

/* does the build settle ec volumes (compiled with TSDB_EC and tsdbd accepts --ec-settle-grace-s)? */
static void ec_detect(void)
{
#ifdef TSDB_EC
    const char *bin = getenv("TSDBD") ? getenv("TSDBD") : "./build/tsdbd";
    char cmd[PATH_MAX + 32], line[512];
    snprintf(cmd, sizeof cmd, "'%s' --help 2>&1", bin);
    FILE *f = popen(cmd, "r");
    if (!f)
        return;
    while (fgets(line, sizeof line, f))
        if (strstr(line, "--ec-settle-grace-s"))
            ec_settles = 1;
    pclose(f);
#endif
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

/* after cluster_form: the cluster's uuid, every node's stores, every node restarted in parallel with them */
static void ec_form_stores(void)
{
    struct view v;
    if (view_get(0, &v, 5000) != 0)
        die(2, "ec: no STATUS of node 0 for the cluster's uuid");
    memcpy(ec_uuid, v.m.uuid, sizeof ec_uuid);
    view_free(&v);
    if (stop_nodes_par() != 0)
        die(1, "ec: a node did not stop cleanly to get its chunk stores");
    for (int i = 0; i < nn; i++)
        ec_stores_init(i);
    start_nodes_par(term_cp >= 0 ? term_spec : NULL);
    restarts += (unsigned)nn;
    settle("the chunk stores");
    ec_poller_start();
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

/* GET /metrics of a node, read line by line (the text of a node with many volumes is long, and the diverged family is
 * printed late: nothing is cut off by a buffer). 1: a "tsdb_volume_diverged{...} 1" line, 0: none, -1: no answer. A line
 * longer than the buffer is skipped to its end (no metric line is that long). */
static int ec_poll_node(int port)
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
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0 || write(fd, rq, sizeof rq - 1) != (ssize_t)(sizeof rq - 1)) {
        close(fd);
        return -1;
    }
    char chunk[4096], line[512];
    size_t ln = 0;
    int skipping = 0, got = 0, div = 0;
    for (ssize_t r; (r = read(fd, chunk, sizeof chunk)) > 0;) {
        got = 1;
        for (ssize_t k = 0; k < r; k++) {
            if (chunk[k] != '\n') {
                if (ln + 1 < sizeof line)
                    line[ln++] = chunk[k];
                else
                    skipping = 1;
                continue;
            }
            line[ln] = 0;
            if (!skipping && !strncmp(line, "tsdb_volume_diverged{", 21) && ln >= 2 && line[ln - 1] == '1' && line[ln - 2] == ' ')
                div = 1;
            ln = 0;
            skipping = 0;
        }
    }
    close(fd);
    return got ? div : -1;
}

static void ec_poll(void)
{
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        int port;
        node_snap(i, &pid, NULL, NULL);
        pthread_mutex_lock(&tbl_mu);
        port = nodes[i].mport;
        pthread_mutex_unlock(&tbl_mu);
        if (pid <= 0 || node_paused(i) || port <= 0)
            continue;
        atomic_fetch_add(&ec_polls, 1);
        int d = ec_poll_node(port);
        if (d < 0)
            atomic_fetch_add(&ec_poll_errs, 1); /* a node going down or starting: no answer is no verdict */
        if (d > 0 && atomic_fetch_add(&ec_div_polls, 1) == 0)
            snprintf(ec_div_msg, sizeof ec_div_msg, "node %d reports a volume DIVERGED (tsdb_volume_diverged 1)", i);
    }
}

/* --mutant ec-diverged: tsdbd's own plant (TSDB_REPLD_TEST=plantdiverge, set for every node in main) on a replica of kvr,
 * once the run has been going for ten polls: the volume writes the DIVERGED file a failed digest writes, stops and is rebuilt
 * by a snapshot (D8). It proves the metrics fetch and parse and the node-log count against the real thing. */
static int ec_plant(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return 0;
    for (int k = 0; k < r.nrep; k++)
        if (r.node[k] >= 0 && r.node[k] != r.leader) {
            char f[PATH_MAX + 64];
            snprintf(f, sizeof f, "%s/diverge.%" PRIu32, nodes[r.node[k]].dir, (uint32_t)rep_number);
            int fd = open(f, O_WRONLY | O_CREAT, 0644);
            if (fd >= 0)
                close(fd);
            return fd >= 0;
        }
    return 0;
}

/* about once a second, a thread of its own: a node whose metrics thread is slow (a stalled disk, a restart) costs its own
 * 1 s timeouts here and never stretches a nemesis' hold or the check for a server that exited */
static void *ec_poller(void *arg)
{
    (void)arg;
    int planted = 0;
    while (atomic_load(&ec_poll_run)) {
        int64_t t0 = now_ms();
        ec_poll();
        if (!planted && !strcmp(mutant, "ec-diverged") && atomic_load(&ec_polls) >= 10 * (unsigned)nn)
            planted = ec_plant();
        while (atomic_load(&ec_poll_run) && now_ms() - t0 < 1000)
            usleep(50000);
    }
    return NULL;
}

static void ec_poller_start(void)
{
    if (ec_poll_started)
        return;
    ec_poll_started = 1;
    atomic_store(&ec_poll_run, 1);
    if (pthread_create(&ec_poll_th, NULL, ec_poller, NULL) != 0)
        die(2, "ec: no thread for the metrics poll");
}

static void ec_poller_stop(void)
{
    if (!ec_poll_started)
        return;
    atomic_store(&ec_poll_run, 0);
    pthread_join(ec_poll_th, NULL);
    ec_poll_started = 0;
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
    ec_poller_stop();
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

/* Live snapshot of the known series of kvr (kvr.a, kvr.b, kvr.fill; unlike offline cvol_rows which enumerates all
 * series in DICT) through member `to` (which currently leads kvr and reads its own DICT, young parts and settled .ec
 * parts through the live chunk tier). Resolves every series name on `to` via tsdbc_series() so each replica's DICT is
 * verified rather than bypassed through cached hist_sid[]. Retries up to 60 s while a freshly transferred leader
 * finishes reopening kvr. Returns 0 on success, -1 on failure. */
static int ec_live_snap(int to, struct snaps *out, int *last_rc)
{
    static const char *const snames[REP_HISTS + 1] = {"kvr.a", "kvr.b", "kvr.fill"};
    memset(out, 0, sizeof *out);
    if (last_rc)
        *last_rc = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 60000; usleep(200000)) {
        tsdbc *c = NULL;
        if (connect_node(to, 1, &c) != 0)
            continue;
        tsdbc_set_timeout(c, 15000); /* cold-cache full scan of kvr.fill across .ec objects can exceed 3 s */
        int ok = 1;
        snaps_free(out);
        for (int i = 0; i <= REP_HISTS && ok; i++) {
            tsdb_sid sid = 0;
            int rc = tsdbc_series(c, snames[i], 0, &sid);
            if (rc != 0) {
                if (last_rc)
                    *last_rc = rc;
                ok = 0;
                break;
            }
            if (i < REP_HISTS && hist_sid[i] && sid != hist_sid[i]) {
                fprintf(stderr, "jepsen-cluster: FAIL: ec: member node %d resolved %s to sid %" PRIu64 ", expected %" PRIu64 "\n",
                        to, snames[i], (uint64_t)sid, (uint64_t)hist_sid[i]);
                if (last_rc)
                    *last_rc = TSDB_ERR_CORRUPT;
                snaps_free(out);
                tsdbc_close(c);
                return -1;
            }
            tsdb_points p;
            memset(&p, 0, sizeof p);
            rc = tsdbc_query_points(c, sid, INT64_MIN, INT64_MAX, &p);
            if (rc != 0) {
                if (last_rc)
                    *last_rc = rc;
                ok = 0;
                break;
            }
            if (out->n == out->cap) {
                out->cap = out->cap ? out->cap * 2 : 8;
                out->s = realloc(out->s, out->cap * sizeof *out->s);
                if (!out->s)
                    die(2, "out of memory");
            }
            struct snapshot *s = &out->s[out->n++];
            snprintf(s->name, sizeof s->name, "%s", snames[i]);
            s->n = p.n;
            s->ts = p.ts;
            s->v = p.values;
        }
        tsdbc_close(c);
        if (ok) {
            if (out->n)
                qsort(out->s, out->n, sizeof *out->s, cmp_snap);
            return 0;
        }
        snaps_free(out);
    }
    return -1;
}

/* E3 + full replica verification (replaces offline read-only cvol_rows, which cannot open META requires=tier volumes):
 * transfers leadership of kvr to each ISR member in turn, verifies that the target leads kvr immediately before AND at
 * the same epoch immediately after ec_live_snap() (so a replica that stepped down and forwarded reads cannot pass
 * undetected), reads every known series (kvr.a, kvr.b, kvr.fill) directly from that member as leader, and checks:
 *  - every key of every history equals kv_final (no stray timestamps; supports ec-member-differs and rep-lost-write),
 *  - all series and rows are bit-for-bit identical across all replicas (snaps_diff),
 *  - every filler batch in kvr.fill is whole or absent, whole when acknowledged, absent when refused (fill_check). */
static int ec_members_check(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: ec: no record of kvr for the per-member reads\n");
        return 1;
    }
    int bad = 0, diff = 0, target_node[META_REPLICAS_MAX], no = 0;
    uint32_t target_id[META_REPLICAS_MAX];
    /* Copy the target list out of `r` so subsequent rep_rec_get() calls never overwrite loop state; read the current
     * leader first so we never transfer away from a member and immediately back while it reopens. */
    for (int pass = 0; pass < 2; pass++)
        for (int k = 0; k < r.nrep; k++)
            if (r.isr_has[k] && !r.learner[k] && r.node[k] >= 0 && ((r.node[k] == r.leader) == (pass == 0))) {
                target_node[no] = r.node[k];
                target_id[no] = r.id[k];
                no++;
            }
    if (no != rep_n) {
        fprintf(stderr, "jepsen-cluster: FAIL: ec: only %d of %d replicas in kvr's ISR for the per-member reads\n", no,
                rep_n);
        bad++;
    }
    struct snaps first;
    memset(&first, 0, sizeof first);
    int have_first = 0;
    char first_lbl[64] = "";
    for (int q = 0; q < no; q++) {
        int to = target_node[q];
        uint32_t tid = target_id[q];
        int rslot = q;
        for (int s = 0; s < rep_n; s++)
            if (rep_nodes[s] == to) {
                rslot = s;
                break;
            }
        struct rrec cur;
        if (rep_rec_get(&cur) != 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: ec: no record of kvr before per-member read of node %d\n", to);
            bad++;
            continue;
        }
        if (cur.leader != to) {
            uint8_t body[16];
            size_t bl = cluster_volume_force_encode(rep_number, tid, body);
            int rc = 0;
            if (strcmp(mutant, "ec-notransfer") != 0) { /* planted: none is made */
                /* Retry up to 30 s: a target that was recently deposed or a new leader still warming may briefly
                 * refuse VOLUME_TRANSFER until it rejoins the ISR. */
                for (int64_t tr0 = now_ms(); (rc = admin_any(CLUSTER_OP_VOLUME_TRANSFER, body, bl, 20000)) != 0 &&
                     now_ms() - tr0 < 30000; usleep(300000))
                    ;
            }
            if (rc != 0) {
                fprintf(stderr, "jepsen-cluster: FAIL: ec: kvr was not moved to node %d for its read: %s\n", to,
                        cluster_strerror(rc));
                bad++;
                continue;
            }
            /* tsdbd forwards by default: a read through a member that does not lead is answered by the leader, so a
             * transfer that did not land would make this check read the old leader and pass by construction */
            int64_t t0 = now_ms();
            while (now_ms() - t0 < 30000) {
                if (rep_rec_get(&cur) == 0 && cur.leader == to)
                    break;
                usleep(200000);
            }
            if (cur.leader != to) {
                fprintf(stderr, "jepsen-cluster: FAIL: ec: kvr did not move to node %d within 30 s of its transfer: its "
                        "reads would be the old leader's\n", to);
                bad++;
                continue;
            }
            ec_transfers++;
        }
        /* Re-check (leader, epoch) immediately before ec_live_snap() for every q (including q == 0) */
        if (rep_rec_get(&cur) != 0 || cur.leader != to) {
            fprintf(stderr, "jepsen-cluster: FAIL: ec: kvr leader is node %d (want %d) immediately before reading node %d\n",
                    cur.leader, to, to);
            bad++;
            continue;
        }
        struct snaps sn;
        int lrc = 0;
        if (ec_live_snap(to, &sn, &lrc) != 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: ec: no live read of kvr through member node %d (%s)\n", to,
                    lrc ? tsdbc_strerror(lrc) : "unreachable");
            bad++;
            continue;
        }
        /* Verify leadership did not leave `to` or advance epoch during ec_live_snap() (forwarding blind spot) */
        struct rrec after;
        if (rep_rec_get(&after) != 0 || after.leader != to || after.epoch != cur.epoch) {
            fprintf(stderr, "jepsen-cluster: FAIL: ec: kvr leadership changed during live read of node %d "
                    "(before: leader=%d epoch=%" PRIu64 ", after: leader=%d epoch=%" PRIu64 "): read may have been forwarded\n",
                    to, cur.leader, cur.epoch, after.leader, after.epoch);
            snaps_free(&sn);
            bad++;
            continue;
        }
        for (int hi = 0; hi < nhists; hi++) {
            uint32_t seen[KEYS];
            int stray = 0;
            snap_seen(&sn, hists[hi].name, seen, &stray);
            if (!strcmp(mutant, "ec-member-differs") && q == 1 && hi == 0) /* planted: one member answers differently */
                seen[0] = seen[0] ? 0 : 1;
            if (!strcmp(mutant, "rep-lost-write") && q == no - 1 && hi == 0)
                for (int key = 0; key < KEYS; key++)
                    if (seen[key] && seen[key] <= hists[hi].nw && hists[hi].w[seen[key] - 1].st == WS_OK) {
                        seen[key] = 0;
                        break;
                    }
            ec_member_reads++;
            if (stray) {
                fprintf(stderr, "jepsen-cluster: FAIL: ec: %s through member node %d has a timestamp that is no key\n",
                        hists[hi].name, to);
                bad++;
                diff++;
            }
            for (int key = 0; key < KEYS; key++)
                if (seen[key] != hists[hi].final[key]) {
                    fprintf(stderr, "jepsen-cluster: FAIL: ec: %s key %d reads write %u through member node %d, the final "
                            "read had %u\n", hists[hi].name, key, (unsigned)seen[key], to, (unsigned)hists[hi].final[key]);
                    bad++;
                    diff++;
                    ec_member_bad++;
                    break;
                }
        }
        uint64_t fapplied = 0;
        int fbad = fill_check(&sn, rslot, 1, &fapplied);
        if (fbad) {
            fprintf(stderr, "jepsen-cluster: replica %s: %d violation(s) of the filler's large writes\n",
                    rep_dir[rslot], fbad);
            bad += fbad;
            diff += fbad;
        } else
            printf("jepsen-cluster: replica %s: kvr.fill holds every one of its %zu batches whole or not at all "
                   "(%" PRIu64 " indeterminate applied)\n", rep_dir[rslot], nfb, fapplied);
        if (!have_first) {
            first = sn;
            have_first = 1;
            snprintf(first_lbl, sizeof first_lbl, "node %d", to);
        } else {
            char cur_lbl[64];
            snprintf(cur_lbl, sizeof cur_lbl, "node %d", to);
            int d = snaps_diff(&first, &sn, first_lbl, cur_lbl);
            if (d) {
                bad += d;
                diff += d;
            }
            snaps_free(&sn);
        }
    }
    snaps_free(&first);
    if (diff)
        fprintf(stderr, "jepsen-cluster: FAIL: %d difference(s) between the replicas of kvr, with its leader's "
                        "final read, or in the filler's large writes\n", diff);
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
        long *keys = NULL;
        int *cnt = NULL, nk = 0, kcap = 0;
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
            if (j == nk) {
                if (nk == kcap) {
                    kcap = kcap ? 2 * kcap : 64;
                    long *nkeys = realloc(keys, (size_t)kcap * sizeof *keys);
                    int *ncnt = realloc(cnt, (size_t)kcap * sizeof *cnt);
                    if (!nkeys || !ncnt)
                        die(2, "ec: out of memory counting parts");
                    keys = nkeys;
                    cnt = ncnt;
                }
                keys[nk] = key;
                cnt[nk++] = 0;
            }
            if (++cnt[j] > per_max)
                per_max = cnt[j];
        }
        closedir(dp);
        free(keys);
        free(cnt);
    }
    if (!strcmp(mutant, "ec-nosettle")) /* planted: nothing settled */
        nec = 0;
    ec_ec_parts = nec;
    ec_partitions_max = per_max;
    int bad = 0;
    if (nec == 0 && ec_settles) {
        fprintf(stderr, "jepsen-cluster: FAIL: ec: no .ec part on any replica of kvr after the run: nothing settled\n");
        bad++;
    }
    return bad;
}

/* ---- nemeses */

/* partial: the Raft leader of the metadata group cut from every stripe member of kvr but kvr's own leader: the leader
 * keeps its links to the stripe (writes, chunk puts and gets go on) and the members lose the metadata path, the one a
 * member needs for an epoch, a digest's answer or a heartbeat. The harness' proxy cuts a whole peer connection pair, so
 * every kind of link between the cut nodes goes, not the metadata one alone (design 10.3's "stripe members to metadata
 * down" as far as a proxy can make it); when the Raft leader is kvr's leader itself, one other replica is cut from it
 * instead. net_cut_done takes the byte counts the heal checks: a round whose rules do not apply fails like a partition's. */
static void nem_partial(void)
{
    int l = atomic_load(&last_leader), kl = atomic_load(&kv_leader), cut = 0;
    struct rrec r;
    if (l < 0 || l >= nn)
        l = rnd_range(0, nn - 1);
    ju_fault(-1);
    if (rep_rec_get(&r) == 0) {
        if (r.leader >= 0)
            kl = r.leader;
        int first = -1;
        for (int k = 0; k < r.nrep; k++) {
            int x = r.node[k];
            if (x < 0 || x >= nn || x == l || x == kl)
                continue;
            if (l == kl) { /* the Raft leader leads kvr: one other replica only */
                if (first < 0 || rnd() % 2)
                    first = x;
                continue;
            }
            net_cut(l, x, 1);
            cut++;
        }
        if (l == kl && first >= 0) {
            net_cut(l, first, 1);
            cut++;
        }
    }
    if (!cut) { /* no replica to aim at (kvr not whole): any other node */
        int x = rnd_range(0, nn - 1);
        if (x == l)
            x = (l + 1) % nn;
        net_cut(l, x, 1);
    }
    net_cut_done();
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
        size_t l = strlen(e->d_name);
        if (S_ISDIR(sb.st_mode)) {
            if (depth < 4)
                found = ec_chunk_find(p, depth + 1, out, on, seen);
        } else if (S_ISREG(sb.st_mode) && sb.st_size >= 256 && e->d_name[0] == 'o' && l > 6 &&
                   !strcmp(e->d_name + l - 6, ".chunk")) {
            if ((*seen)++ == 0 || rnd() % 4 == 0) {
                snprintf(out, on, "%s", p);
                found = rnd() % 2;
            }
        }
    }
    closedir(dp);
    return found;
}

/* Track rotted chunk files so at most want_ec_m distinct positions of any object are rotted simultaneously (R12 scrub
 * repair is not built yet, so rotting > m positions of one object makes it unrecoverable). Up to m positions stay
 * rotted across rounds and through the final per-member reads. */
struct ec_rot_rec {
    char path[PATH_MAX + 64];
    char oid[64];
    int node;
    int nflips;
    off_t off[8];
    uint8_t orig[8];
};
#define EC_ROT_MAX 256
static struct ec_rot_rec ec_rot_tab[EC_ROT_MAX];
static int ec_rot_n;

static void ec_rot_oid(const char *path, char *oid, size_t on)
{
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    if (*base == 'o')
        base++;
    size_t n = strcspn(base, ".");
    if (n >= on)
        n = on - 1;
    memcpy(oid, base, n);
    oid[n] = 0;
}

static void ec_rot_heal_idx(int idx)
{
    struct ec_rot_rec *r = &ec_rot_tab[idx];
    int fd = open(r->path, O_RDWR);
    if (fd < 0 && errno != ENOENT)
        die(2, "ec: rot heal: cannot open %s: %s", r->path, strerror(errno));
    if (fd >= 0) {
        for (int k = r->nflips - 1; k >= 0; k--)
            if (pwrite(fd, &r->orig[k], 1, r->off[k]) != 1)
                die(2, "ec: rot heal: write to %s: %s", r->path, strerror(errno));
        fsync(fd);
        close(fd);
    }
    memmove(&ec_rot_tab[idx], &ec_rot_tab[idx + 1], (size_t)(ec_rot_n - idx - 1) * sizeof *ec_rot_tab);
    ec_rot_n--;
}

static void ec_rot_heal_node(const char *oid, int node)
{
    /* Undo all stacked flips for (oid, node) in exact reverse chronological order so the chunk is restored byte-for-byte. */
    for (int k = ec_rot_n - 1; k >= 0; k--)
        if (ec_rot_tab[k].node == node && strcmp(ec_rot_tab[k].oid, oid) == 0)
            ec_rot_heal_idx(k);
}

/* rot: bytes flipped in one chunk file of a random node, at rest (the node goes on running) */
static void nem_rot(void)
{
    char path[PATH_MAX + 64] = "";
    int seen = 0, node = -1;
    for (int tries = 0; tries < 30 && !path[0]; tries++) {
        int i = rnd_range(0, nn - 1);
        int c0 = (int)(rnd() % 2);
        seen = 0;
        ec_chunk_find(nodes[i].chunk[c0], 0, path, sizeof path, &seen);
        if (!path[0])
            ec_chunk_find(nodes[i].chunk[1 - c0], 0, path, sizeof path, &seen);
        if (path[0])
            node = i;
        else
            hold_ms(500);
    }
    if (!path[0])
        return; /* no chunk yet: the round has no effect and the run says so */
    char oid[64];
    ec_rot_oid(path, oid, sizeof oid);
    /* Count distinct nodes currently rotted for this object; heal the oldest node completely if adding `node` would exceed m. */
    int already = 0, first_oid = -1, distinct = 0;
    int seen_nodes[MAXN];
    for (int k = 0; k < ec_rot_n; k++) {
        if (strcmp(ec_rot_tab[k].oid, oid) != 0)
            continue;
        if (first_oid < 0)
            first_oid = k;
        if (ec_rot_tab[k].node == node)
            already = 1;
        int dup = 0;
        for (int j = 0; j < distinct; j++)
            dup |= seen_nodes[j] == ec_rot_tab[k].node;
        if (!dup && distinct < MAXN)
            seen_nodes[distinct++] = ec_rot_tab[k].node;
    }
    if (!already && distinct >= want_ec_m && first_oid >= 0)
        ec_rot_heal_node(oid, ec_rot_tab[first_oid].node);
    if (ec_rot_n >= EC_ROT_MAX) {
        char old_oid[64];
        snprintf(old_oid, sizeof old_oid, "%s", ec_rot_tab[0].oid);
        ec_rot_heal_node(old_oid, ec_rot_tab[0].node);
    }
    struct stat sb;
    int fd = open(path, O_RDWR);
    if (fd < 0 && errno == ENOENT) /* a chunk file is always a new inode: a heal or a gc took it between the find and here */
        return;
    if (fd < 0 || fstat(fd, &sb) != 0)
        die(2, "ec: rot: cannot open %s: %s", path, strerror(errno)); /* the harness' own error, never a VIOLATION (1) */
    struct ec_rot_rec rec;
    memset(&rec, 0, sizeof rec);
    snprintf(rec.path, sizeof rec.path, "%s", path);
    snprintf(rec.oid, sizeof rec.oid, "%s", oid);
    rec.node = node;
    int flips = rnd_range(1, 8);
    for (int k = 0; k < flips; k++) {
        off_t off = (off_t)(rnd() % (uint64_t)sb.st_size);
        uint8_t b;
        if (pread(fd, &b, 1, off) != 1)
            continue;
        rec.off[rec.nflips] = off;
        rec.orig[rec.nflips] = b;
        rec.nflips++;
        b ^= (uint8_t)(1u << (rnd() % 8));
        if (pwrite(fd, &b, 1, off) != 1)
            die(2, "ec: rot: write to %s: %s", path, strerror(errno));
    }
    fsync(fd);
    close(fd);
    if (rec.nflips)
        ec_rot_tab[ec_rot_n++] = rec;
    ec_rots++;
    ec_damaged = 1;
    effect_seen = 1;
    ev("rot: %d bit(s) flipped in %s (node %d, oid %s)", rec.nflips, path, node, oid);
    hold_ms(rnd_range(1000, 3000));
}

/* diskloss: a node's chunk stores gone, made again empty with its cluster and node, the node started with them.
 * At most want_ec_m distinct nodes are wiped per run (R9 chunk rebuild is not built yet, so wiping > m distinct nodes
 * would lose > m positions of settled objects). */
static int ec_wiped[MAXN], ec_nwiped;
static void nem_diskloss(void)
{
    int i;
    if (ec_nwiped >= want_ec_m) {
        int cand[MAXN], nc = 0;
        for (int k = 0; k < nn; k++)
            if (ec_wiped[k])
                cand[nc++] = k;
        i = cand[rnd_range(0, nc - 1)];
    } else {
        i = rnd_range(0, nn - 1);
        if (!ec_wiped[i]) {
            ec_wiped[i] = 1;
            ec_nwiped++;
        }
    }
    stop_node(i, SIGKILL);
    for (int k = 0; k < 2; k++) {
        rmtree(nodes[i].chunk[k]);
        if (mkdir(nodes[i].chunk[k], 0755) != 0)
            die(2, "ec: diskloss: mkdir %s: %s", nodes[i].chunk[k], strerror(errno));
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
    printf("ec %d+%d: %s; %u polls of /metrics (%u DIVERGED, %u without an answer), %u node-log stop line(s); .ec parts on the replicas: %d, most "
           "parts of one partition on a replica: %d; per-member reads %u through %u transfers (%u differing); kv replaces %" PRIu64 "; partial %u, diskloss %u, rot %u\n",
           want_ec_k, want_ec_m, ec_settles ? "tsdbd settles (TSDB_EC)" : "tsdbd does not settle (NO_EC)",
           ec_polls, ec_div_polls, ec_poll_errs, ec_stopped_lines, ec_ec_parts, ec_partitions_max, ec_member_reads, ec_transfers, ec_member_bad,
           (uint64_t)atomic_load(&ec_replaces), ec_partials, ec_diskloss, ec_rots);
}

#endif
