/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * jepsen-cluster with erasure-coded volumes (--ec K+M; docs/design-cluster-data.md 10.3, row r8l-jc). Included by
 * jepsen_cluster.c before its nemesis dispatch; it uses that file's node table, views, admin calls and histories.
 *
 * What is different in an --ec run.
 *  - Setup: every node gets two capacity disks (`tsdb disk init PATH:hdd`) once the cluster has
 *    member ids, and is restarted with `--disk PATH:hdd` for both, --metrics-port auto and --ec-settle-grace-s 3 (when this
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

/* ---- R12 scrub / repair / heal counters and /metrics delta accumulator (survives node restarts) ---- */
struct r12_node_m {
    int ok, div;
    uint64_t chunk_scrub_chunks;
    uint64_t chunk_scrub_damaged;
    uint64_t chunk_repairs_repaired;
    uint64_t chunk_repairs_unrecoverable;
    uint64_t chunk_repairs_not_live;
    uint64_t chunk_repair_bytes;
    uint64_t chunk_repair_qdepth;
    uint64_t young_scrub_parts;
    uint64_t young_scrub_damaged;
    uint64_t auto_resets_damaged;
    uint64_t auto_resets_failed;
    uint64_t auto_resets_xfer;
};

static pthread_mutex_t r12_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t r12_node_mu_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t r12_node_mu[MAXN];
static int64_t r12_prev_proc[MAXN];
static struct r12_node_m r12_prev[MAXN];
static uint64_t r12_chunk_scrub_chunks_node[MAXN]; /* monotonic per-node scrubbed chunks across restarts */
static uint64_t r12_chunk_damaged, r12_chunk_repaired, r12_chunk_unrecoverable, r12_chunk_not_live, r12_chunk_repair_bytes;
static uint64_t r12_young_parts, r12_young_damaged, r12_auto_resets_damaged, r12_auto_resets_failed, r12_auto_resets_xfer;
static uint64_t r12_repair_qdepth;
static unsigned r12_rotparity_planted, r12_rotcold_planted, r12_rotyoung_planted, r12_rotyoung_rebuilt;
static unsigned r12_rotthenm_planted, r12_rotthenm_rounds, r12_rotthenm_reads, r12_rotthenm_read_fail;
static unsigned r12_rotthenm_repair_timeout;
static int r12_gc_race_done, r12_gc_race_bad, r12_harness_err;

static void r12_node_mu_init(void)
{
    for (int i = 0; i < MAXN; i++)
        pthread_mutex_init(&r12_node_mu[i], NULL);
}

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

/* tsdb disk init PATH:hdd (the CLI, TSDB_CLI): a capacity disk with its chunks/ store */
static void ec_chunk_init(const char *path, uint32_t id)
{
    (void)id;
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char spec[PATH_MAX + 8];
    snprintf(spec, sizeof spec, "%s:hdd", path);
    pid_t p = fork();
    if (p < 0)
        die(2, "fork: %s", strerror(errno));
    if (p == 0) {
        execl(cli, "tsdb", "disk", "init", spec, (char *)NULL);
        _exit(127);
    }
    int st = 0;
    waitpid(p, &st, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
        die(2, "tsdb disk init %s failed (status 0x%x; TSDB_CLI=%s)", spec, st, cli);
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

static int r12_tail_u64(const char *line, uint64_t *out)
{
    const char *sp = strrchr(line, ' ');
    if (!sp || !sp[1] || sp[1] < '0' || sp[1] > '9')
        return 0;
    char *end = NULL;
    unsigned long long v = strtoull(sp + 1, &end, 10);
    if (!end || (*end != '\0' && *end != '\r'))
        return 0;
    *out = (uint64_t)v;
    return 1;
}

static void r12_parse_metric_line(const char *line, struct r12_node_m *out)
{
    uint64_t v = 0;
    if (!strncmp(line, "tsdb_volume_diverged{", 21)) {
        if (r12_tail_u64(line, &v) && v == 1)
            out->div = 1;
    } else if (!strncmp(line, "tsdb_chunk_scrub_chunks_total{", 30)) {
        if (r12_tail_u64(line, &v))
            out->chunk_scrub_chunks += v;
    } else if (!strncmp(line, "tsdb_chunk_scrub_damaged_total{", 31)) {
        if (r12_tail_u64(line, &v))
            out->chunk_scrub_damaged += v;
    } else if (!strncmp(line, "tsdb_chunk_repairs_total{result=\"repaired\"} ", 44)) {
        if (r12_tail_u64(line, &v))
            out->chunk_repairs_repaired += v;
    } else if (!strncmp(line, "tsdb_chunk_repairs_total{result=\"unrecoverable\"} ", 49)) {
        if (r12_tail_u64(line, &v))
            out->chunk_repairs_unrecoverable += v;
    } else if (!strncmp(line, "tsdb_chunk_repairs_total{result=\"not_live\"} ", 44)) {
        if (r12_tail_u64(line, &v))
            out->chunk_repairs_not_live += v;
    } else if (!strncmp(line, "tsdb_chunk_repair_bytes_total ", 30)) {
        if (r12_tail_u64(line, &v))
            out->chunk_repair_bytes += v;
    } else if (!strncmp(line, "tsdb_chunk_repair_queue_depth ", 30)) {
        if (r12_tail_u64(line, &v))
            out->chunk_repair_qdepth += v;
    } else if (!strncmp(line, "tsdb_young_scrub_parts_total{", 29)) {
        if (r12_tail_u64(line, &v))
            out->young_scrub_parts += v;
    } else if (!strncmp(line, "tsdb_young_scrub_damaged_total{", 31)) {
        if (r12_tail_u64(line, &v))
            out->young_scrub_damaged += v;
    } else if (!strncmp(line, "tsdb_cluster_auto_resets_total{why=\"damaged\"} ", 46)) {
        if (r12_tail_u64(line, &v))
            out->auto_resets_damaged += v;
    } else if (!strncmp(line, "tsdb_cluster_auto_resets_total{why=\"failed\"} ", 45)) {
        if (r12_tail_u64(line, &v))
            out->auto_resets_failed += v;
    } else if (!strncmp(line, "tsdb_cluster_auto_resets_total{why=\"leader_transfer\"} ", 54)) {
        if (r12_tail_u64(line, &v))
            out->auto_resets_xfer += v;
    }
}

/* GET /metrics of a node, read line by line (the text of a node with many volumes is long, and the diverged and R12
 * families are printed late: nothing is cut off by a buffer). Verifies `pid` owns `port` via th_owns_port after
 * connect so a foreign tsdbd reusing a released port can never skew verdicts. Populates *out with ok=1 on success. */
static void ec_scrape_node(pid_t pid, int port, struct r12_node_m *out)
{
    memset(out, 0, sizeof *out);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return;
    struct timeval tv = {1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    static const char rq[] = "GET /metrics HTTP/1.0\r\n\r\n";
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0 || !th_owns_port(pid, port) ||
        write(fd, rq, sizeof rq - 1) != (ssize_t)(sizeof rq - 1)) {
        close(fd);
        return;
    }
    char chunk[4096], line[512];
    size_t ln = 0;
    int skipping = 0, got = 0;
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
            if (ln > 0 && line[ln - 1] == '\r')
                ln--;
            line[ln] = 0;
            if (!skipping && ln > 0)
                r12_parse_metric_line(line, out);
            ln = 0;
            skipping = 0;
        }
    }
    close(fd);
    out->ok = got;
}

static void r12_recalc_qdepth_locked(void)
{
    uint64_t qd = 0;
    for (int k = 0; k < nn; k++)
        qd += r12_prev[k].chunk_repair_qdepth;
    r12_repair_qdepth = qd;
}

static void r12_metrics_accum(int i, int64_t proc_ns, const struct r12_node_m *m)
{
    pthread_mutex_lock(&r12_mu);
    if (proc_ns < r12_prev_proc[i]) {
        /* Stale scrape from a previous process incarnation: ignore. */
        pthread_mutex_unlock(&r12_mu);
        return;
    }
    int reset = (proc_ns > r12_prev_proc[i]);
    if (reset) {
        memset(&r12_prev[i], 0, sizeof r12_prev[i]);
        r12_prev_proc[i] = proc_ns;
    }
    /* Within a single process incarnation, counters are strictly monotonic: a smaller value is a stale sample and is
     * never treated as a reset. */
#define R12_ACC(field, dst)                                                                                            \
    do {                                                                                                               \
        uint64_t cur = m->field;                                                                                       \
        uint64_t prev = r12_prev[i].field;                                                                             \
        if (cur > prev) {                                                                                              \
            (dst) += cur - prev;                                                                                       \
            r12_prev[i].field = cur;                                                                                   \
        }                                                                                                              \
    } while (0)
    R12_ACC(chunk_scrub_chunks, r12_chunk_scrub_chunks_node[i]);
    R12_ACC(chunk_scrub_damaged, r12_chunk_damaged);
    R12_ACC(chunk_repairs_repaired, r12_chunk_repaired);
    R12_ACC(chunk_repairs_unrecoverable, r12_chunk_unrecoverable);
    R12_ACC(chunk_repairs_not_live, r12_chunk_not_live);
    R12_ACC(chunk_repair_bytes, r12_chunk_repair_bytes);
    R12_ACC(young_scrub_parts, r12_young_parts);
    R12_ACC(young_scrub_damaged, r12_young_damaged);
    R12_ACC(auto_resets_damaged, r12_auto_resets_damaged);
    R12_ACC(auto_resets_failed, r12_auto_resets_failed);
    R12_ACC(auto_resets_xfer, r12_auto_resets_xfer);
#undef R12_ACC
    r12_prev[i].chunk_repair_qdepth = m->chunk_repair_qdepth;
    r12_prev[i].div = m->div;
    r12_prev[i].ok = m->ok;
    r12_recalc_qdepth_locked();
    pthread_mutex_unlock(&r12_mu);
}

/* Scrapes node i's /metrics, folds counter deltas into the R12 totals, and returns 1 if diverged, 0 if ok, -1 on err. */
static int r12_metrics_sample_node(int i)
{
    pthread_once(&r12_node_mu_once, r12_node_mu_init);
    pthread_mutex_lock(&r12_node_mu[i]);
    pid_t pid;
    int port, paused;
    int64_t proc_ns;
    pthread_mutex_lock(&tbl_mu);
    pid = nodes[i].pid;
    port = nodes[i].mport;
    paused = nodes[i].paused;
    proc_ns = nodes[i].proc_ns;
    pthread_mutex_unlock(&tbl_mu);
    if (pid <= 0 || paused || port <= 0) {
        if (pid <= 0) {
            pthread_mutex_lock(&r12_mu);
            r12_prev[i].chunk_repair_qdepth = 0;
            r12_recalc_qdepth_locked();
            pthread_mutex_unlock(&r12_mu);
        }
        pthread_mutex_unlock(&r12_node_mu[i]);
        return -1;
    }
    struct r12_node_m m;
    ec_scrape_node(pid, port, &m);
    if (!m.ok) {
        pthread_mutex_unlock(&r12_node_mu[i]);
        return -1;
    }
    r12_metrics_accum(i, proc_ns, &m);
    pthread_mutex_unlock(&r12_node_mu[i]);
    return m.div;
}

static void r12_metrics_sample_all(void)
{
    for (int i = 0; i < nn; i++)
        if (!nodes[i].dead_for_good)
            (void)r12_metrics_sample_node(i);
}

static void ec_poll(void)
{
    for (int i = 0; i < nn; i++) {
        if (nodes[i].dead_for_good)
            continue;
        pid_t pid;
        int port, paused;
        pthread_mutex_lock(&tbl_mu);
        pid = nodes[i].pid;
        port = nodes[i].mport;
        paused = nodes[i].paused;
        pthread_mutex_unlock(&tbl_mu);
        if (pid <= 0 || paused || port <= 0)
            continue;
        atomic_fetch_add(&ec_polls, 1);
        int d = r12_metrics_sample_node(i);
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
    unsigned lf = atomic_load(&r9_local_faults);
    int fault_excused = ec_damaged || r9_loss_expected;
    int bad = 0;
    if (ec_div_polls || ec_stopped_lines || lf)
        printf("jepsen-cluster: ec: %u of %u polls found a volume DIVERGED (%s), %u \"volume stopped\" line(s), "
               "%u LOCAL_FAULT sample(s) (%s)%s\n",
               ec_div_polls, ec_polls, ec_div_polls ? ec_div_msg : "none", ec_stopped_lines, lf,
               lf ? r9_local_fault_msg : "none",
               ec_damaged ? " (rot or disk loss ran: allowed)" : fault_excused ? " (expected loss)" : "");
    if (!ec_damaged && ec_div_polls) {
        fprintf(stderr, "jepsen-cluster: FAIL: a volume was DIVERGED or stopped in a run without rot or disk loss: %s, %u "
                "stop line(s)\n", ec_div_msg, ec_stopped_lines);
        bad++;
    } else if (!fault_excused && (ec_stopped_lines || lf)) {
        fprintf(stderr, "jepsen-cluster: FAIL: a volume was DIVERGED or stopped (LOCAL_FAULT) in a run without rot or disk loss: "
                "%u stop line(s), %u LOCAL_FAULT (%s)\n", ec_stopped_lines, lf, lf ? r9_local_fault_msg : "none");
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
    /* Each wipe creates 2 fresh store ids via `tsdb disk init` (old ids stay in metadata until r13-ops marks them
     * OUT/DESTROYED; META_DISKS_MAX = 64 caps registered stores per member, well above a 300 s diskloss run). */
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

/* ---- R12 helpers, nemeses (rotparity, rotcold, rotyoung, rotthenm), GC-vs-repair check, and metrics checks ---- */

static uint64_t r12_le64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = (v << 8) | p[i];
    return v;
}

static ino_t r12_dir_ino(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0 ? sb.st_ino : 0;
}

static off_t r12_log_size(int node)
{
    struct stat sb;
    return stat(nodes[node].log, &sb) == 0 ? sb.st_size : 0;
}

static int r12_log_count_since(int node, off_t from_off, const char *n1, const char *n2)
{
    FILE *f = fopen(nodes[node].log, "r");
    if (!f)
        return 0;
    if (from_off > 0 && fseeko(f, from_off, SEEK_SET) != 0) {
        fclose(f);
        return 0;
    }
    char line[1024];
    int n = 0;
    while (fgets(line, sizeof line, f))
        if (strstr(line, n1) && (!n2 || strstr(line, n2)))
            n++;
    fclose(f);
    return n;
}

static int r12_read_ecfile_meta(const char *path, uint8_t oid_out[16], int64_t *min_ts_out, int64_t *max_ts_out)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    struct stat sb;
    if (fstat(fd, &sb) != 0 || sb.st_size < 48 + 64 + 216 + 64) {
        close(fd);
        return -1;
    }
    uint8_t tr[64], ftr[64], rec[32];
    if (pread(fd, tr, sizeof tr, sb.st_size - 64) != (ssize_t)sizeof tr || memcmp(tr, "TSDB-EC\n", 8) != 0) {
        close(fd);
        return -1;
    }
    uint64_t roff = r12_le64(tr + 16), rlen = r12_le64(tr + 24);
    if (roff < 48 + 64 || rlen < 216 || roff + rlen != (uint64_t)sb.st_size - 64 ||
        pread(fd, ftr, sizeof ftr, (off_t)(roff - 64)) != (ssize_t)sizeof ftr || memcmp(ftr + 56, "TSDBEND1", 8) != 0 ||
        pread(fd, rec, sizeof rec, (off_t)roff) != (ssize_t)sizeof rec) {
        close(fd);
        return -1;
    }
    close(fd);
    memcpy(oid_out, rec + 8, 16);
    if (min_ts_out)
        *min_ts_out = (int64_t)r12_le64(ftr + 24);
    if (max_ts_out)
        *max_ts_out = (int64_t)r12_le64(ftr + 32);
    return 0;
}

/* Tracks recently rotted (oid, pos) so rounds avoid colliding with chunkrepair's 30 s per-(oid, pos) cooldown when
 * other targets exist. Owned exclusively by the nemesis thread (no lock needed). */
struct r12_cool_ent {
    char oid[33];
    uint16_t pos;
    int64_t at_ms;
};
#define R12_COOL_MAX 128
static struct r12_cool_ent r12_cool[R12_COOL_MAX];
static int r12_cool_n;

static int r12_is_cooling(const char *oid_hex, unsigned pos)
{
    int64_t now = now_ms();
    for (int k = 0; k < r12_cool_n; k++)
        if (r12_cool[k].pos == pos && strcmp(r12_cool[k].oid, oid_hex) == 0 && now - r12_cool[k].at_ms < 32000)
            return 1;
    return 0;
}

static void r12_note_rot(const char *oid_hex, unsigned pos)
{
    int64_t now = now_ms();
    for (int k = 0; k < r12_cool_n; k++)
        if (r12_cool[k].pos == pos && strcmp(r12_cool[k].oid, oid_hex) == 0) {
            r12_cool[k].at_ms = now;
            return;
        }
    int slot;
    if (r12_cool_n < R12_COOL_MAX) {
        slot = r12_cool_n++;
    } else {
        slot = 0;
        for (int k = 1; k < R12_COOL_MAX; k++)
            if (r12_cool[k].at_ms < r12_cool[slot].at_ms)
                slot = k;
    }
    snprintf(r12_cool[slot].oid, sizeof r12_cool[slot].oid, "%s", oid_hex);
    r12_cool[slot].pos = (uint16_t)pos;
    r12_cool[slot].at_ms = now;
}

/* Flips 1 byte strictly inside the LAST cell payload of a chunk file (catches scrubskipcell, which skips the last cell).
 * want_parity: 1 = parity only (pos >= g.k), 0 = data only (pos < g.k), -1 = either. If ino_out is non-NULL, stores the
 * inode of the flipped file so callers can confirm atomic replacement on disk. Returns 0 on flip, -1 otherwise. */
static int r12_flip_last_cell_ex(const char *path, int want_parity, chunk_hdr *hdr_out, ino_t *ino_out)
{
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return -1;
    struct stat sb;
    uint8_t hb[CHUNK_HDR_BYTES];
    chunk_hdr h;
    if (fstat(fd, &sb) != 0 || sb.st_size < CHUNK_HDR_BYTES + CHUNK_FTR_BYTES ||
        pread(fd, hb, sizeof hb, 0) != (ssize_t)sizeof hb || chunk_hdr_decode(hb, &h) != 0) {
        close(fd);
        return -1;
    }
    int is_parity = h.pos >= h.g.k;
    if ((want_parity == 1 && !is_parity) || (want_parity == 0 && is_parity)) {
        close(fd);
        return -1;
    }
    uint64_t nc = chunk_pos_cells(&h.g, h.pos);
    if (nc == 0 || h.chunk_len == 0 || (uint64_t)sb.st_size < CHUNK_HDR_BYTES + h.chunk_len + CHUNK_FTR_BYTES) {
        close(fd);
        return -1;
    }
    uint64_t lcl = is_parity ? chunk_stripe_len(&h.g, nc - 1)
                             : chunk_cell_len(&h.g, (nc - 1) * (uint64_t)h.g.k + h.pos);
    if (lcl == 0 || lcl > h.chunk_len) {
        close(fd);
        return -1;
    }
    uint64_t cell_off = CHUNK_HDR_BYTES + h.chunk_len - lcl;
    off_t off = (off_t)(cell_off + (rnd() % lcl));
    uint8_t b = 0;
    if (pread(fd, &b, 1, off) != 1) {
        close(fd);
        return -1;
    }
    b ^= 0x5au;
    if (pwrite(fd, &b, 1, off) != 1)
        die(2, "r12: flip pwrite %s: %s", path, strerror(errno));
    (void)fsync(fd); /* best-effort flush of planted rot */
    close(fd);
    if (hdr_out)
        *hdr_out = h;
    if (ino_out)
        *ino_out = sb.st_ino;
    return 0;
}

static int r12_flip_last_cell(const char *path, int want_parity, chunk_hdr *hdr_out)
{
    return r12_flip_last_cell_ex(path, want_parity, hdr_out, NULL);
}

static int r12_peek_chunk_parity(const char *path, chunk_hdr *hdr_out)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    uint8_t hb[CHUNK_HDR_BYTES];
    chunk_hdr h;
    ssize_t r = pread(fd, hb, sizeof hb, 0);
    close(fd);
    if (r != (ssize_t)sizeof hb || chunk_hdr_decode(hb, &h) != 0)
        return -1;
    if (hdr_out)
        *hdr_out = h;
    return h.pos >= h.g.k ? 1 : 0;
}

/* Searches both chunk stores of `node` under s<rep_number> for a chunk matching optional `want_oid_hex`, `want_parity`
 * (1 parity, 0 data, -1 any), and optional `want_pos` (>= 0 exact pos, -1 any). When `skip_cool` is set, skips
 * (oid, pos) entries within chunkrepair's 32 s cooldown window. Uniformly picks among matches. */
static int r12_find_chunk_in_node_ex(int node, const char *want_oid_hex, int want_parity, int want_pos, int skip_cool,
                                     char *out, size_t on)
{
    int matches = 0;
    out[0] = 0;
    for (int c = 0; c < 2 && nodes[node].chunk[c][0]; c++) {
        char vdir[PATH_MAX + 96];
        snprintf(vdir, sizeof vdir, "%s/chunks/s%" PRIu32, nodes[node].chunk[c], rep_number);
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (l < 41 || e->d_name[0] != 'o' || e->d_name[33] != '.' || strcmp(e->d_name + l - 6, ".chunk") != 0)
                continue;
            if (want_oid_hex && strncmp(e->d_name + 1, want_oid_hex, 32) != 0)
                continue;
            char *end = NULL;
            unsigned long pos = strtoul(e->d_name + 34, &end, 10);
            if (!end || strcmp(end, ".chunk") != 0)
                continue;
            if (want_pos >= 0 && (int)pos != want_pos)
                continue;
            char oid_s[33];
            memcpy(oid_s, e->d_name + 1, 32);
            oid_s[32] = 0;
            if (skip_cool && r12_is_cooling(oid_s, (unsigned)pos))
                continue;
            char p[PATH_MAX + 192];
            snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
            if (want_parity >= 0) {
                int ip = r12_peek_chunk_parity(p, NULL);
                if (ip < 0 || ip != want_parity)
                    continue;
            }
            if (rnd() % (uint64_t)(++matches) == 0)
                snprintf(out, on, "%s", p);
        }
        closedir(dp);
    }
    return matches > 0;
}

static int r12_find_chunk_in_node(int node, const char *want_oid_hex, int want_parity, int want_pos, char *out, size_t on)
{
    if (r12_find_chunk_in_node_ex(node, want_oid_hex, want_parity, want_pos, 1, out, on))
        return 1;
    return r12_find_chunk_in_node_ex(node, want_oid_hex, want_parity, want_pos, 0, out, on);
}

static int r12_count_oid_chunks(const char *oid_hex)
{
    int cnt = 0;
    for (int i = 0; i < nn; i++)
        for (int c = 0; c < 2 && nodes[i].chunk[c][0]; c++) {
            char vdir[PATH_MAX + 96];
            snprintf(vdir, sizeof vdir, "%s/chunks/s%" PRIu32, nodes[i].chunk[c], rep_number);
            DIR *dp = opendir(vdir);
            if (!dp)
                continue;
            for (struct dirent *e; (e = readdir(dp));) {
                size_t l = strlen(e->d_name);
                if (l >= 41 && e->d_name[0] == 'o' && strncmp(e->d_name + 1, oid_hex, 32) == 0 &&
                    strcmp(e->d_name + l - 6, ".chunk") == 0)
                    cnt++;
            }
            closedir(dp);
        }
    return cnt;
}

/* Lists live .ec object oids in kvr's leader (or any replica). want_cold_only: 1 = only cold kvr.fill parts
 * (footer min_ts >= FILL_BASE, never read by workload threads), 0 = all live .ec objects. */
static int r12_list_ec_oids(char oids[][33], int max_oids, int want_cold_only)
{
    int n = 0;
    int ldr = atomic_load(&kv_leader);
    int order[META_REPLICAS_MAX + 1], no = 0;
    if (ldr >= 0 && ldr < nn)
        order[no++] = ldr;
    for (int r = 0; r < rep_n; r++)
        if (rep_nodes[r] >= 0 && rep_nodes[r] != ldr)
            order[no++] = rep_nodes[r];
    for (int oi = 0; oi < no && n == 0; oi++) {
        char vdir[PATH_MAX + 64];
        snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[order[oi]].dir, rep_number);
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp)) && n < max_oids;) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            char p[PATH_MAX + 192];
            snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
            uint8_t oid[16];
            int64_t min_ts = 0, max_ts = 0;
            if (r12_read_ecfile_meta(p, oid, &min_ts, &max_ts) != 0)
                continue;
            if (want_cold_only && min_ts < FILL_BASE)
                continue;
            char hex[33];
            chunk_oid_hex(oid, hex);
            int dup = 0;
            for (int j = 0; j < n; j++)
                dup |= strcmp(oids[j], hex) == 0;
            if (!dup)
                snprintf(oids[n++], 33, "%s", hex);
        }
        closedir(dp);
    }
    return n;
}

/* Gets kvr's current stripe members (node indices in nodes[]). Returns count. */
static int r12_stripe_nodes(int *out_nodes, int max_n)
{
    int best_n = 0;
    uint64_t best_applied = 0;
    for (int i = 0; i < nn; i++) {
        pid_t pid;
        node_snap(i, &pid, NULL, NULL);
        struct view v;
        if (pid <= 0 || node_paused(i) || view_get(i, &v, 2000) != 0)
            continue;
        for (uint32_t k = 0; k < v.m.nvolumes; k++) {
            const struct meta_volume *mv = &v.m.volumes[k];
            if (mv->number != rep_number || mv->redundancy != META_RED_EC)
                continue;
            if (best_n && v.m.applied <= best_applied)
                continue;
            best_applied = v.m.applied;
            best_n = 0;
            for (uint32_t p = 0; p < mv->nstripe && best_n < max_n; p++) {
                int ni = node_by_id(mv->stripe[p]);
                if (ni >= 0)
                    out_nodes[best_n++] = ni;
            }
        }
        view_free(&v);
    }
    return best_n;
}

/* rotparity (R12): flips 1 byte in the last cell of a parity chunk (pos >= k') of a settled object at rest. */
static void nem_rotparity(void)
{
    char path[PATH_MAX + 192] = "";
    int node = -1;
    chunk_hdr h;
    for (int tries = 0; tries < 50 && node < 0; tries++) {
        int off = rnd_range(0, nn - 1);
        for (int s = 0; s < nn; s++) {
            int i = (off + s) % nn;
            if (r12_find_chunk_in_node(i, NULL, 1, -1, path, sizeof path) && r12_flip_last_cell(path, 1, &h) == 0) {
                node = i;
                break;
            }
        }
        if (node < 0)
            hold_ms(500);
    }
    if (node < 0)
        return;
    r12_rotparity_planted++;
    effect_seen = 1;
    char hex[33];
    chunk_oid_hex(h.oid, hex);
    r12_note_rot(hex, h.pos);
    ev("rotparity: flipped last cell of parity chunk %s (node %d, oid %s, pos %u >= k' %u)", path, node, hex,
       (unsigned)h.pos, (unsigned)h.g.k);
    hold_ms(rnd_range(2000, 4000));
}

/* rotcold (R12): flips 1 byte in the last cell of a data chunk (pos == 0 < k') of a settled cold kvr.fill object that
 * no workload reader ever queries during the run. Only background chunk scrub can find and repair it. */
static void nem_rotcold(void)
{
    char path[PATH_MAX + 192] = "";
    int node = -1;
    chunk_hdr h;
    for (int tries = 0; tries < 50 && node < 0; tries++) {
        char oids[64][33];
        int no = r12_list_ec_oids(oids, 64, 1);
        if (no > 0) {
            int start = rnd_range(0, no - 1);
            for (int pass = 0; pass < 2 && node < 0; pass++) {
                for (int oi = 0; oi < no && node < 0; oi++) {
                    const char *oid = oids[(start + oi) % no];
                    if (pass == 0 && r12_is_cooling(oid, 0))
                        continue;
                    /* Target pos 0 (data) so even if a mutant disables repair we never corrupt > 1 position of one object. */
                    for (int i = 0; i < nn; i++) {
                        if (r12_find_chunk_in_node_ex(i, oid, 0, 0, pass == 0, path, sizeof path) &&
                            r12_flip_last_cell(path, 0, &h) == 0) {
                            node = i;
                            break;
                        }
                    }
                }
            }
        }
        if (node < 0)
            hold_ms(500);
    }
    if (node < 0)
        return;
    r12_rotcold_planted++;
    effect_seen = 1;
    char hex[33];
    chunk_oid_hex(h.oid, hex);
    r12_note_rot(hex, h.pos);
    ev("rotcold: flipped last cell of cold data chunk %s (node %d, oid %s, pos %u < k' %u)", path, node, hex,
       (unsigned)h.pos, (unsigned)h.g.k);
    hold_ms(rnd_range(2000, 4000));
}

/* Finds a young .part file in `vdir` (preferring larger / merged parts so a merge does not retire it before scrubd
 * runs) and flips a byte strictly inside the LAST granule payload (at footer.index_off - 8, before the 0..7 byte
 * 8-byte alignment pad, catching youngskipgranule which skips the last granule). Returns 0 on flip, -1 otherwise. */
static int r12_flip_young_granule(const char *vdir, char *picked, size_t pcap)
{
    DIR *dp = opendir(vdir);
    if (!dp)
        return -1;
    char best_name[128] = "";
    off_t best_sz = 0;
    for (struct dirent *e; (e = readdir(dp));) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] != 'p' || l <= 5 || strcmp(e->d_name + l - 5, ".part") != 0 || l >= sizeof best_name)
            continue;
        char p[PATH_MAX + 192];
        snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
        struct stat sb;
        if (stat(p, &sb) != 0 || sb.st_size < 48 + 64 + 32 || sb.st_size <= best_sz)
            continue;
        /* Verify the candidate has granule payload (not tombstone-only) before picking it over smaller valid parts. */
        int pfd = open(p, O_RDONLY | O_CLOEXEC);
        if (pfd < 0)
            continue;
        uint8_t cftr[64];
        int ok = pread(pfd, cftr, sizeof cftr, sb.st_size - 64) == (ssize_t)sizeof cftr &&
                 memcmp(cftr + 56, "TSDBEND1", 8) == 0;
        close(pfd);
        if (!ok)
            continue;
        uint64_t cio = r12_le64(cftr);
        if (cio <= 48 + 8 || cio > (uint64_t)sb.st_size - 64)
            continue;
        best_sz = sb.st_size;
        snprintf(best_name, sizeof best_name, "%s", e->d_name);
    }
    closedir(dp);
    if (!best_name[0])
        return -1;
    char p[PATH_MAX + 192];
    snprintf(p, sizeof p, "%s/%s", vdir, best_name);
    int fd = open(p, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return -1;
    struct stat sb;
    uint8_t ftr[64];
    if (fstat(fd, &sb) != 0 || sb.st_size < 48 + 64 + 32 ||
        pread(fd, ftr, sizeof ftr, sb.st_size - 64) != (ssize_t)sizeof ftr || memcmp(ftr + 56, "TSDBEND1", 8) != 0) {
        close(fd);
        return -1;
    }
    uint64_t index_off = r12_le64(ftr);
    if (index_off <= 48 + 8 || index_off > (uint64_t)sb.st_size - 64) {
        close(fd);
        return -1;
    }
    /* index_off is 8-byte aligned (0..7 bytes of trailing zero pad after the last granule), so index_off - 8 lies
     * strictly inside the last granule's checksummed payload. */
    off_t off = (off_t)(index_off - 8);
    uint8_t b = 0;
    if (pread(fd, &b, 1, off) != 1) {
        close(fd);
        return -1;
    }
    b ^= 0x5au;
    if (pwrite(fd, &b, 1, off) != 1)
        die(2, "r12: rotyoung pwrite %s: %s", p, strerror(errno));
    (void)fsync(fd); /* best-effort flush of planted rot */
    close(fd);
    snprintf(picked, pcap, "%s", best_name);
    return 0;
}

/* rotyoung (R12, D7/D8): flips a byte in the last granule of a young .part file on one ISR follower of kvr, then waits
 * for scrubd to detect damage, leave ISR, and cheal on the Raft leader to auto-reset and rebuild it via R4 snapshot. */
static void nem_rotyoung(void)
{
    struct rrec r;
    int target = -1, tslot = -1;
    uint64_t old_rid = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 30000; usleep(200000)) {
        check_unexpected_exits();
        if (rep_rec_get(&r) != 0 || r.leader < 0 || !rep_no_learners(&r))
            continue;
        int cands[META_REPLICAS_MAX], nc = 0;
        for (int k = 0; k < r.nrep; k++)
            if (r.node[k] >= 0 && r.node[k] != r.leader && r.isr_has[k] && !r.learner[k])
                cands[nc++] = k;
        if (nc > 0) {
            tslot = cands[rnd_range(0, nc - 1)];
            target = r.node[tslot];
            old_rid = r.rid[tslot];
            break;
        }
    }
    if (target < 0)
        return;
    char vdir[PATH_MAX + 64], picked[128] = "";
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[target].dir, rep_number);
    ino_t old_ino = r12_dir_ino(vdir);
    int flipped = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(300)) {
        if (r12_flip_young_granule(vdir, picked, sizeof picked) == 0) {
            flipped = 1;
            break;
        }
    }
    if (!flipped)
        return;
    r12_rotyoung_planted++;
    effect_seen = 1;
    ev("rotyoung: flipped last granule of %s/%s (node %d, rid %" PRIu64 ")", vdir, picked, target, old_rid);

    /* Wait up to 25 s for scrubd -> CL_HEALTH_DAMAGED -> cheal REPLICA_RESET (new rid) -> R4 snapshot -> rejoin ISR. */
    int rebuilt = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 25000; hold_ms(300)) {
        r12_metrics_sample_all();
        struct rrec cur;
        if (rep_rec_get(&cur) != 0)
            continue;
        for (int k = 0; k < cur.nrep; k++) {
            if (cur.node[k] == target && cur.rid[k] != old_rid && !cur.learner[k] && cur.isr_has[k]) {
                rebuilt = 1;
                ev("rotyoung: node %d auto-reset and rebuilt (rid %" PRIu64 " -> %" PRIu64 ", ino %llu -> %llu)",
                   target, old_rid, cur.rid[k], (unsigned long long)old_ino, (unsigned long long)r12_dir_ino(vdir));
                break;
            }
        }
        if (rebuilt)
            break;
    }
    if (rebuilt)
        r12_rotyoung_rebuilt++;
    hold_ms(rnd_range(1000, 2000));
}

/* Transfers kvr leadership to another healthy ISR member so its EC granule cache is cold before a degraded read, and
 * waits until the new leader is actively serving kvr (answers a 1-row query without MOVED/NOTFOUND) before returning. */
static void r12_transfer_cold_leader(void)
{
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return;
    int cand_n[META_REPLICAS_MAX], nc = 0;
    uint32_t cand_id[META_REPLICAS_MAX];
    for (int k = 0; k < r.nrep; k++)
        if (r.node[k] >= 0 && r.node[k] != r.leader && r.isr_has[k] && !r.learner[k]) {
            cand_n[nc] = r.node[k];
            cand_id[nc] = r.id[k];
            nc++;
        }
    if (nc == 0) {
        ev("rotthenm: cold leader transfer skipped (no alternate healthy ISR member)");
        return;
    }
    int pick = rnd_range(0, nc - 1);
    int to = cand_n[pick];
    uint8_t body[16];
    size_t bl = cluster_volume_force_encode(rep_number, cand_id[pick], body);
    if (admin_any(CLUSTER_OP_VOLUME_TRANSFER, body, bl, 10000) != 0) {
        ev("rotthenm: cold leader transfer to node %d refused; continuing on current leader", to);
        return;
    }
    int landed = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 10000; usleep(150000)) {
        struct rrec cur;
        if (rep_rec_get(&cur) == 0 && cur.leader == to) {
            landed = 1;
            break;
        }
    }
    if (!landed) {
        ev("rotthenm: cold leader transfer to node %d did not land within 10 s", to);
        return;
    }
    /* Wait until `to` finishes reopening kvr as writer (a query succeeds, proving it neither returns NOTFOUND nor MOVED). */
    for (int64_t t0 = now_ms(); now_ms() - t0 < 10000; usleep(100000)) {
        tsdbc *c = NULL;
        tsdb_sid sid = 0;
        if (connect_node(to, 1, &c) == 0) {
            int rc = tsdbc_series(c, "kvr.a", 0, &sid);
            if (rc == 0) {
                tsdb_points p;
                memset(&p, 0, sizeof p);
                rc = tsdbc_query_points(c, sid, KV_BASE_TS, KV_BASE_TS + 1, &p);
                free(p.ts);
                free(p.values);
            }
            tsdbc_close(c);
            if (rc == 0)
                return;
        }
    }
    ev("rotthenm: new leader node %d did not serve a probe query within 10 s", to);
}

/* Full-stripe (k' == want_ec_k) cold series seeded once for rotthenm so every stripe position 0..k+m-1 holds a chunk
 * and killing m DATA positions mathematically forces RS decode through the repaired parity chunk(s). */
static int r12_big_ready, r12_big_tries;
static char r12_big_oid[33];
static size_t r12_big_rows;

static int r12_seed_big_stripe(void)
{
    if (r12_big_ready)
        return 1;
    if (r12_big_tries >= 2)
        return 0;
    r12_big_tries++;
    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0)
        return 0;
    int L = r.leader;
    tsdbc *c = NULL;
    tsdb_sid sid = 0;
    int rc = TSDB_ERR_UNAVAILABLE;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; usleep(150000)) {
        if (connect_node(L, 1, &c) == 0) {
            tsdbc_set_timeout(c, 15000);
            rc = tsdbc_series_at(c, "kvr.big", "kvr", &sid);
            if (rc == 0)
                break;
            tsdbc_close(c);
            c = NULL;
        }
    }
    if (rc != 0 || !c)
        return 0;

    /* ~24000 high-entropy float32 rows per k (> 64 KiB compressed per k -> k' == want_ec_k full stripe!), starting on
     * a UTC day boundary so all rows (9600 s at 100 ms spacing) stay inside one 1-day partition without splitting. */
    const int batches = want_ec_k * 24;
    const int per_batch = 1000;
    const int64_t big_base = ((KV_BASE_TS - 200LL * 86400000LL) / 86400000LL) * 86400000LL;
    const int64_t big_lo = big_base - 86400000LL;
    const int64_t big_hi = big_base + 2LL * 86400000LL;
    int64_t *ts = malloc((size_t)per_batch * sizeof *ts);
    float *val = malloc((size_t)per_batch * sizeof *val);
    if (!ts || !val)
        die(2, "r12: OOM seeding kvr.big");
    uint64_t rng = mix(seed * 9973 + 17);
    for (int b = 0; b < batches; b++) {
        for (int j = 0; j < per_batch; j++) {
            ts[j] = big_base + (int64_t)(b * per_batch + j) * 100LL;
            rng = mix(rng);
            /* High-entropy IEEE-754 float bits (exponents 127..134, random 23-bit mantissa) resist ALP/zstd so the
             * settled part is > k * 64 KiB and uses all k + m stripe positions. */
            uint32_t bits = ((127u + (uint32_t)((rng >> 24) & 7u)) << 23) | (uint32_t)(rng & 0x7fffffu);
            memcpy(&val[j], &bits, sizeof val[j]);
        }
        for (int64_t t0 = now_ms(); (rc = tsdbc_insert_series(c, sid, ts, val, (size_t)per_batch)) != 0; usleep(100000)) {
            if (rc != TSDB_ERR_MOVED && rc != TSDB_ERR_UNAVAILABLE)
                break;
            if (now_ms() - t0 > 15000)
                break;
            tsdbc_close(c);
            c = NULL;
            if (rep_rec_get(&r) == 0 && r.leader >= 0)
                L = r.leader;
            if (connect_node(L, 1, &c) != 0)
                break;
            tsdbc_set_timeout(c, 15000);
        }
        if (rc != 0)
            break;
    }
    free(ts);
    free(val);
    if (c)
        tsdbc_close(c);
    if (rc != 0)
        return 0;

    /* Wait up to 40 s for kvr.big to settle into a full-stripe .ec object. */
    char vdir[PATH_MAX + 64];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[L].dir, rep_number);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 40000; hold_ms(250)) {
        if (rep_rec_get(&r) == 0 && r.leader >= 0) {
            L = r.leader;
            snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[L].dir, rep_number);
        }
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            char p[PATH_MAX + 192];
            snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
            uint8_t oid[16];
            int64_t min_ts = 0, max_ts = 0;
            if (r12_read_ecfile_meta(p, oid, &min_ts, &max_ts) == 0 && min_ts >= big_lo && max_ts <= big_hi) {
                char hex[33];
                chunk_oid_hex(oid, hex);
                int nch = r12_count_oid_chunks(hex);
                if (nch >= want_ec_k + want_ec_m) {
                    snprintf(r12_big_oid, sizeof r12_big_oid, "%s", hex);
                    /* Total rows inserted across the whole kvr.big series (nem_rotthenm queries [INT64_MIN, INT64_MAX],
                     * so this matches even if kvr.big settled across more than one .ec object). */
                    r12_big_rows = (size_t)batches * (size_t)per_batch;
                    r12_big_ready = 1;
                    ev("rotthenm: seeded full-stripe kvr.big (%zu rows, oid %s, %d chunks)", r12_big_rows, r12_big_oid, nch);
                    closedir(dp);
                    return 1;
                }
            }
        }
        closedir(dp);
    }
    return 0;
}

/* Returns 1 if any live p*.ec file on kvr's current leader (or fallback replica) still references `oid_hex`, 0 if it
 * has been retired (re-encoded or GC'd) so chunkrepair can legitimately decline repair with NOT_LIVE. Scans all .ec
 * files without a cap so long runs with >128 objects never misclassify a live oid as retired. */
static int r12_oid_still_live(const char *oid_hex)
{
    int ldr = atomic_load(&kv_leader);
    int order[META_REPLICAS_MAX + 1], no = 0;
    if (ldr >= 0 && ldr < nn)
        order[no++] = ldr;
    for (int r = 0; r < rep_n; r++)
        if (rep_nodes[r] >= 0 && rep_nodes[r] != ldr)
            order[no++] = rep_nodes[r];
    for (int oi = 0; oi < no; oi++) {
        char vdir[PATH_MAX + 64];
        snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[order[oi]].dir, rep_number);
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        int saw_any = 0;
        for (struct dirent *e; (e = readdir(dp));) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            saw_any = 1;
            char p[PATH_MAX + 192];
            snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
            uint8_t oid[16];
            if (r12_read_ecfile_meta(p, oid, NULL, NULL) != 0)
                continue;
            char hex[33];
            chunk_oid_hex(oid, hex);
            if (strcmp(hex, oid_hex) == 0) {
                closedir(dp);
                return 1;
            }
        }
        closedir(dp);
        if (saw_any)
            return 0;
    }
    return 0;
}

/* rotthenm (R12 gate): rots 1 position per settled object on up to m - 1 stripe members (including parity of the
 * full-stripe kvr.big object and cold kvr.fill objects), confirms every still-live rotted chunk is replaced on disk,
 * then kills m OTHER stripe members (preferring DATA positions of kvr.big and avoiding the Raft leader so RS decode
 * MUST read the repaired parity chunk) and reads every settled series (kvr.big, kvr.a, kvr.b, kvr.fill): must answer
 * whole with zero UNAVAILABLE or CORRUPT. */
static void nem_rotthenm(void)
{
    (void)r12_seed_big_stripe();
    /* Prefer cold kvr.fill oids (+ kvr.big) which are never overwritten/re-encoded; fall back to all oids only if none
     * have settled yet. */
    char oids[64][33];
    int no = 0;
    for (int tries = 0; tries < 50 && no == 0; tries++) {
        no = r12_list_ec_oids(oids, 64, 1);
        if (no == 0 && r12_big_ready)
            break;
        if (no == 0 && tries >= 10)
            no = r12_list_ec_oids(oids, 64, 0);
        if (no == 0)
            hold_ms(500);
    }
    if (no == 0 && !r12_big_ready)
        return;

    int snodes[META_STRIPE_MAX];
    int ns = r12_stripe_nodes(snodes, META_STRIPE_MAX);
    if (ns < want_ec_k + want_ec_m)
        return;

    /* Pick nrot = max(1, m - 1) distinct stripe members that actually hold a target chunk (preferring parity holders
     * of kvr.big when ready, else parity holders of one of the listed oids[], else any holder of one of those oids). */
    int nrot_want = want_ec_m > 1 ? want_ec_m - 1 : 1;
    int rot_nodes[CHUNK_M_MAX];
    int nrot = 0;
    int parity_cands[META_STRIPE_MAX], npc = 0;
    int any_cands[META_STRIPE_MAX], nac = 0;
    int lim_oids = no > 8 ? 8 : no;
    int oid_start = no > 0 ? rnd_range(0, no - 1) : 0;
    for (int s = 0; s < ns; s++) {
        char p[PATH_MAX + 192];
        int has_par = 0, has_any = 0;
        if (r12_big_ready) {
            if (r12_find_chunk_in_node_ex(snodes[s], r12_big_oid, 1, -1, 0, p, sizeof p))
                has_par = has_any = 1;
            else if (r12_find_chunk_in_node_ex(snodes[s], r12_big_oid, -1, -1, 0, p, sizeof p))
                has_any = 1;
        }
        for (int oi = 0; oi < lim_oids && !has_par; oi++) {
            const char *oid = oids[(oid_start + oi) % no];
            if (r12_find_chunk_in_node_ex(snodes[s], oid, 1, -1, 0, p, sizeof p))
                has_par = has_any = 1;
            else if (!has_any && r12_find_chunk_in_node_ex(snodes[s], oid, -1, -1, 0, p, sizeof p))
                has_any = 1;
        }
        if (has_par)
            parity_cands[npc++] = snodes[s];
        if (has_any)
            any_cands[nac++] = snodes[s];
    }
    while (nrot < nrot_want && npc > 0) {
        int idx = rnd_range(0, npc - 1);
        rot_nodes[nrot++] = parity_cands[idx];
        parity_cands[idx] = parity_cands[--npc];
    }
    for (int s = 0; s < nac && nrot < nrot_want; s++) {
        int dup = 0;
        for (int j = 0; j < nrot; j++)
            dup |= rot_nodes[j] == any_cands[s];
        if (!dup)
            rot_nodes[nrot++] = any_cands[s];
    }

    struct {
        char path[PATH_MAX + 192];
        char oid[33];
        ino_t ino;
        int node;
    } rots[64];
    unsigned rotted = 0;
    int rot_has[MAXN] = {0};
    /* Always include kvr.big if not cooling (or on fallback), plus up to 7 other cold settled objects. */
    for (int pass = 0; pass < 2 && rotted == 0; pass++) {
        if (r12_big_ready) {
            for (int j = 0; j < nrot && rotted < 64; j++) {
                int rn = rot_nodes[j];
                char path[PATH_MAX + 192];
                chunk_hdr h;
                ino_t ino = 0;
                if (r12_find_chunk_in_node_ex(rn, r12_big_oid, -1, -1, pass == 0, path, sizeof path) &&
                    r12_flip_last_cell_ex(path, -1, &h, &ino) == 0) {
                    snprintf(rots[rotted].path, sizeof rots[rotted].path, "%s", path);
                    snprintf(rots[rotted].oid, sizeof rots[rotted].oid, "%s", r12_big_oid);
                    rots[rotted].ino = ino;
                    rots[rotted].node = rn;
                    rotted++;
                    rot_has[rn] = 1;
                    r12_note_rot(r12_big_oid, h.pos);
                    ev("rotthenm: flipped last cell of %s (node %d, big oid %s, pos %u, k'=%u)", path, rn, r12_big_oid,
                       (unsigned)h.pos, (unsigned)h.g.k);
                }
            }
        }
        for (int oi = 0; oi < lim_oids && rotted < 64; oi++) {
            const char *oid = oids[(oid_start + oi) % no];
            if (r12_big_ready && strcmp(oid, r12_big_oid) == 0)
                continue;
            for (int j = 0; j < nrot && rotted < 64; j++) {
                int rn = rot_nodes[j];
                char path[PATH_MAX + 192];
                chunk_hdr h;
                ino_t ino = 0;
                if (r12_find_chunk_in_node_ex(rn, oid, -1, -1, pass == 0, path, sizeof path) &&
                    r12_flip_last_cell_ex(path, -1, &h, &ino) == 0) {
                    snprintf(rots[rotted].path, sizeof rots[rotted].path, "%s", path);
                    snprintf(rots[rotted].oid, sizeof rots[rotted].oid, "%s", oid);
                    rots[rotted].ino = ino;
                    rots[rotted].node = rn;
                    rotted++;
                    rot_has[rn] = 1;
                    r12_note_rot(oid, h.pos);
                    ev("rotthenm: flipped last cell of %s (node %d, oid %s, pos %u, k'=%u)", path, rn, oid,
                       (unsigned)h.pos, (unsigned)h.g.k);
                }
            }
        }
    }
    if (rotted == 0)
        return;

    r12_rotthenm_planted += rotted;
    effect_seen = 1;

    /* Confirm every still-live rotted chunk file was atomically replaced on disk (new inode via chunk_wr_commit
     * renameat) and the repair queue drained before transferring leadership and killing m stripe members (capped at
     * 35 s). An object retired by re-encode/GC is legitimately declined by repair_one (NOT_LIVE) and counts as resolved. */
    unsigned unrepaired = rotted;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 35000; hold_ms(250)) {
        r12_metrics_sample_all();
        unrepaired = 0;
        for (unsigned j = 0; j < rotted; j++) {
            struct stat sb;
            if (stat(rots[j].path, &sb) == 0 && sb.st_ino == rots[j].ino && r12_oid_still_live(rots[j].oid))
                unrepaired++;
        }
        pthread_mutex_lock(&r12_mu);
        uint64_t cur_qd = r12_repair_qdepth;
        pthread_mutex_unlock(&r12_mu);
        if (unrepaired == 0 && cur_qd == 0)
            break;
    }
    if (unrepaired > 0) {
        r12_rotthenm_repair_timeout++;
        fprintf(stderr, "jepsen-cluster: FAIL: r12: rotthenm: round %u: %u of %u rotted chunk(s) were not repaired within 35 s\n",
                r12_rotthenm_rounds + 1, unrepaired, rotted);
        return;
    }

    /* Transfer leadership to another ISR replica so its EC granule cache is cold before killing m stripe members. */
    r12_transfer_cold_leader();

    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0) {
        ev("rotthenm: skipped m-kill phase (no kvr leader after cold transfer)");
        return;
    }
    int leader = r.leader;
    int raft_leader = -1;
    for (int i = 0; i < nn; i++) {
        struct view v;
        if (view_get(i, &v, 1000) == 0) {
            if (v.st.role == RAFT_LEADER_ROLE && v.st.mode == CLUSTER_MODE_CORE)
                raft_leader = i;
            view_free(&v);
            if (raft_leader >= 0)
                break;
        }
    }
    int is_rep[MAXN] = {0};
    for (int k = 0; k < r.nrep; k++)
        if (r.node[k] >= 0 && r.node[k] < nn)
            is_rep[r.node[k]] = 1;

    /* Pick want_ec_m stripe members DISJOINT from rot_nodes[] and leader, preferring DATA holders of kvr.big,
     * non-log-replica members, and non-Raft-leader members so kvr keeps >= min_isr log replicas without a Raft
     * election stall and RS decode MUST use the repaired parity. */
    int kill_nodes[CHUNK_M_MAX], nkill = 0;
    int reps_killed = 0, max_rep_kill = rep_n > 2 ? rep_n - 2 : 0;
    int holds_big_data[MAXN] = {0};
    if (r12_big_ready) {
        for (int s = 0; s < ns; s++) {
            char p[PATH_MAX + 192];
            if (r12_find_chunk_in_node_ex(snodes[s], r12_big_oid, 0, -1, 0, p, sizeof p))
                holds_big_data[snodes[s]] = 1;
        }
    }
    for (int pass = 0; pass < 5 && nkill < want_ec_m; pass++) {
        int off = rnd_range(0, ns - 1);
        for (int s = 0; s < ns && nkill < want_ec_m; s++) {
            int cand = snodes[(off + s) % ns];
            if (cand == leader || rot_has[cand])
                continue;
            if (pass < 4 && cand == raft_leader)
                continue;
            if (pass == 0 && (is_rep[cand] || !holds_big_data[cand]))
                continue;
            if (pass == 1 && (!holds_big_data[cand] || (is_rep[cand] && reps_killed >= max_rep_kill)))
                continue;
            if (pass == 2 && is_rep[cand])
                continue;
            if ((pass == 3 || pass == 4) && is_rep[cand] && reps_killed >= max_rep_kill)
                continue;
            int dup = 0;
            for (int j = 0; j < nkill; j++)
                dup |= kill_nodes[j] == cand;
            if (dup)
                continue;
            kill_nodes[nkill++] = cand;
            if (is_rep[cand])
                reps_killed++;
        }
    }
    if (nkill < want_ec_m) {
        ev("rotthenm: skipped m-kill phase (found only %d of %d disjoint stripe members to kill)", nkill, want_ec_m);
        return;
    }

    for (int j = 0; j < nkill; j++) {
        (void)r12_metrics_sample_node(kill_nodes[j]);
        stop_node(kill_nodes[j], SIGKILL);
    }
    ev("rotthenm: killed %d stripe members (leader node %d), reading every settled series", nkill, leader);

    /* Read kvr.big (if seeded), kvr.a, kvr.b, and kvr.fill directly from the live leader while m stripe members are
     * dead: must answer whole with ZERO TSDB_ERR_UNAVAILABLE or TSDB_ERR_CORRUPT (25 s bound allows any transient
     * Raft election + lease renewal to finish cleanly). */
    const char *snames[REP_HISTS + 2] = {"kvr.a", "kvr.b", "kvr.fill", "kvr.big"};
    int nsnames = r12_big_ready ? (REP_HISTS + 2) : (REP_HISTS + 1);
    for (int si = 0; si < nsnames; si++) {
        int rc = TSDB_ERR_UNAVAILABLE;
        tsdb_points p;
        memset(&p, 0, sizeof p);
        int cur_l = leader;
        for (int64_t t0 = now_ms(); now_ms() - t0 < 25000; usleep(200000)) {
            tsdbc *c = NULL;
            tsdb_sid sid = 0;
            if (connect_node(cur_l, 1, &c) != 0) {
                struct rrec rr;
                if (rep_rec_get(&rr) == 0 && rr.leader >= 0)
                    cur_l = rr.leader;
                continue;
            }
            tsdbc_set_timeout(c, 15000);
            int src = tsdbc_series(c, snames[si], 0, &sid);
            if (src != 0) {
                tsdbc_close(c);
                rc = src;
                if (src == TSDB_ERR_MOVED || src == TSDB_ERR_NOTFOUND || src == TSDB_ERR_UNAVAILABLE) {
                    struct rrec rr;
                    if (rep_rec_get(&rr) == 0 && rr.leader >= 0)
                        cur_l = rr.leader;
                    continue;
                }
                break;
            }
            rc = tsdbc_query_points(c, sid, INT64_MIN, INT64_MAX, &p);
            if (rc == TSDB_ERR_MOVED || rc == TSDB_ERR_UNAVAILABLE) {
                struct rrec rr;
                if (rep_rec_get(&rr) == 0 && rr.leader >= 0)
                    cur_l = rr.leader;
            }
            tsdbc_close(c);
            if (rc == 0 || rc == TSDB_ERR_CORRUPT)
                break;
            if (rc != TSDB_ERR_MOVED && rc != TSDB_ERR_UNAVAILABLE)
                break;
        }
        if (rc != 0) {
            r12_rotthenm_read_fail++;
            fprintf(stderr, "jepsen-cluster: FAIL: rotthenm: round %u: read of %s with %d stripe members down failed: %s\n",
                    r12_rotthenm_rounds + 1, snames[si], nkill, tsdbc_strerror(rc));
        } else {
            r12_rotthenm_reads++;
            if (si < REP_HISTS) {
                for (size_t k = 0; k < p.n; k++)
                    if (ts_key(p.ts[k]) < 0) {
                        r12_rotthenm_read_fail++;
                        fprintf(stderr, "jepsen-cluster: FAIL: rotthenm: round %u: %s returned stray timestamp %" PRId64 "\n",
                                r12_rotthenm_rounds + 1, snames[si], p.ts[k]);
                        break;
                    }
            } else if (si == REP_HISTS + 1 && p.n != r12_big_rows) {
                r12_rotthenm_read_fail++;
                fprintf(stderr, "jepsen-cluster: FAIL: rotthenm: round %u: kvr.big returned %zu rows, want %zu\n",
                        r12_rotthenm_rounds + 1, p.n, r12_big_rows);
            }
            free(p.ts);
            free(p.values);
        }
    }

    for (int j = 0; j < nkill; j++)
        restart_node(kill_nodes[j]);
    r12_rotthenm_rounds++;
    hold_ms(rnd_range(1000, 2000));
}

/* D9 GC-vs-repair check (run at the end of rotcold while all nodes are up): creates an isolated cold series `kvr.gc`
 * in its own partition, waits for settle, pauses chunk repair before put via TSDB_REPAIR_TEST=pause_before_put, flips
 * pos 0, expires `kvr.gc` via retention so MK_GC deletes all chunks of that oid, resumes repair, and asserts 0 chunks
 * resurrected, 0 "chunk replace refused" log lines for this oid after check start, and not_live >= 1 (catches
 * repairnolive). Setup errors set r12_harness_err (exit 2 only if no real violation was found) rather than die(2). */
static int r12_gc_repair_check(void)
{
    int bad = 0;
    char pausef[PATH_MAX + 64];
    snprintf(pausef, sizeof pausef, "%s/repair.pause", rundir);
    unlink(pausef);

    off_t log_off[MAXN];
    for (int i = 0; i < nn; i++)
        log_off[i] = r12_log_size(i);

    struct rrec r;
    if (rep_rec_get(&r) != 0 || r.leader < 0) {
        fprintf(stderr, "jepsen-cluster: harness error: r12: no kvr leader for GC-vs-repair check\n");
        r12_harness_err = 1;
        return 0;
    }
    int L = r.leader;
    tsdbc *c = NULL;
    tsdb_sid sid = 0;
    int rc = TSDB_ERR_UNAVAILABLE;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; usleep(200000)) {
        if (connect_node(L, 1, &c) == 0) {
            tsdbc_set_timeout(c, 15000);
            rc = tsdbc_series_at(c, "kvr.gc", "kvr", &sid);
            if (rc == 0)
                break;
            tsdbc_close(c);
            c = NULL;
        }
    }
    if (rc != 0 || !c) {
        fprintf(stderr, "jepsen-cluster: harness error: r12: GC-vs-repair check could not create kvr.gc: %s\n",
                tsdbc_strerror(rc));
        r12_harness_err = 1;
        return 0;
    }
    /* Place kvr.gc 800 days before KV_BASE_TS (well past the 364-day max class and any kvr.big range) so its partition
     * is strictly disjoint from all other series. */
    const int64_t gc_base = KV_BASE_TS - 800LL * 86400000LL;
    const int64_t gc_cutoff = KV_BASE_TS - 730LL * 86400000LL;
    int64_t ts[200];
    float val[200];
    for (int j = 0; j < 200; j++) {
        ts[j] = gc_base + (int64_t)j * 1000LL;
        val[j] = (float)(j + 1);
    }
    rc = tsdbc_insert_series(c, sid, ts, val, 200);
    if (rc != 0) {
        tsdbc_close(c);
        fprintf(stderr, "jepsen-cluster: harness error: r12: GC-vs-repair insert kvr.gc failed: %s\n",
                tsdbc_strerror(rc));
        r12_harness_err = 1;
        return 0;
    }

    char oid_hex[33] = "";
    char pos0_path[PATH_MAX + 192] = "";
    char vdir[PATH_MAX + 64];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, nodes[L].dir, rep_number);
    for (int64_t t0 = now_ms(); now_ms() - t0 < 40000 && !pos0_path[0]; hold_ms(250)) {
        DIR *dp = opendir(vdir);
        if (!dp)
            continue;
        for (struct dirent *e; (e = readdir(dp)) && !pos0_path[0];) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l <= 3 || strcmp(e->d_name + l - 3, ".ec") != 0)
                continue;
            char p[PATH_MAX + 192];
            snprintf(p, sizeof p, "%s/%s", vdir, e->d_name);
            uint8_t oid[16];
            int64_t min_ts = 0, max_ts = 0;
            if (r12_read_ecfile_meta(p, oid, &min_ts, &max_ts) == 0 && max_ts < gc_cutoff) {
                chunk_oid_hex(oid, oid_hex);
                for (int i = 0; i < nn; i++)
                    if (r12_find_chunk_in_node_ex(i, oid_hex, 0, 0, 0, pos0_path, sizeof pos0_path))
                        break;
            }
        }
        closedir(dp);
    }
    if (!pos0_path[0]) {
        tsdbc_close(c);
        fprintf(stderr, "jepsen-cluster: harness error: r12: kvr.gc did not settle within 40 s for GC-vs-repair check\n");
        r12_harness_err = 1;
        return 0;
    }

    char oid_needle[48];
    snprintf(oid_needle, sizeof oid_needle, "oid=%s", oid_hex);

    r12_metrics_sample_all();
    pthread_mutex_lock(&r12_mu);
    uint64_t nl0 = r12_chunk_not_live;
    pthread_mutex_unlock(&r12_mu);

    int pfd = open(pausef, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
    if (pfd < 0) {
        tsdbc_close(c);
        fprintf(stderr, "jepsen-cluster: harness error: r12: create %s: %s\n", pausef, strerror(errno));
        r12_harness_err = 1;
        return 0;
    }
    close(pfd);

    chunk_hdr h;
    if (r12_flip_last_cell(pos0_path, 0, &h) != 0) {
        unlink(pausef);
        tsdbc_close(c);
        fprintf(stderr, "jepsen-cluster: harness error: r12: could not flip %s for GC-vs-repair check\n", pos0_path);
        r12_harness_err = 1;
        return 0;
    }
    int paused = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 20000 && !paused; hold_ms(150))
        for (int i = 0; i < nn; i++)
            if (r12_log_count_since(i, log_off[i], "chunkrepair: paused before put", oid_needle) > 0) {
                paused = 1;
                break;
            }
    if (!paused) {
        unlink(pausef);
        tsdbc_close(c);
        fprintf(stderr, "jepsen-cluster: harness error: r12: no node paused repair before put for oid %s within 20 s\n", oid_hex);
        r12_harness_err = 1;
        return 0;
    }

    rc = tsdbc_set_retention(c, sid, 86400000LL);
    tsdbc_close(c);
    if (rc != 0) {
        unlink(pausef);
        fprintf(stderr, "jepsen-cluster: harness error: r12: set_retention on kvr.gc failed: %s\n", tsdbc_strerror(rc));
        r12_harness_err = 1;
        return 0;
    }

    int deleted = 0;
    for (int64_t t0 = now_ms(); now_ms() - t0 < 45000; hold_ms(200))
        if (r12_count_oid_chunks(oid_hex) == 0) {
            deleted = 1;
            break;
        }
    unlink(pausef);
    if (!deleted) {
        fprintf(stderr, "jepsen-cluster: harness error: r12: MK_GC did not delete kvr.gc oid %s within 45 s (%d chunks left)\n",
                oid_hex, r12_count_oid_chunks(oid_hex));
        r12_harness_err = 1;
        return 0;
    }

    for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(150)) {
        r12_metrics_sample_all();
        pthread_mutex_lock(&r12_mu);
        uint64_t qd = r12_repair_qdepth;
        uint64_t nl = r12_chunk_not_live;
        pthread_mutex_unlock(&r12_mu);
        int resumed = 0;
        for (int i = 0; i < nn; i++)
            if (r12_log_count_since(i, log_off[i], "chunkrepair: resumed before put", oid_needle) > 0) {
                resumed = 1;
                break;
            }
        if (resumed && qd == 0 && nl > nl0)
            break;
    }
    r12_metrics_sample_all();

    int left = r12_count_oid_chunks(oid_hex);
    if (left != 0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r12: chunk repair resurrected %d chunk(s) of GC'd oid %s\n", left, oid_hex);
        bad++;
    }
    for (int i = 0; i < nn; i++)
        if (r12_log_count_since(i, log_off[i], "chunk replace refused", oid_needle) > 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: leader sent replace-put for GC'd oid %s (\"chunk replace refused\" in node %d log)\n",
                    oid_hex, i);
            bad++;
        }
    pthread_mutex_lock(&r12_mu);
    uint64_t nl1 = r12_chunk_not_live;
    pthread_mutex_unlock(&r12_mu);
    if (nl1 <= nl0) {
        fprintf(stderr, "jepsen-cluster: FAIL: r12: GC-vs-repair check expected tsdb_chunk_repairs_total{result=\"not_live\"} > %" PRIu64 " (got %" PRIu64 ")\n",
                nl0, nl1);
        bad++;
    }
    r12_gc_race_done = 1;
    r12_gc_race_bad = bad;
    return bad;
}

/* End-of-run R12 verification (called while all nodes are up and serving /metrics, before stop_nodes_par). */
static int r12_end_check(void)
{
    int bad = 0;
    unsigned chunk_planted = r12_rotparity_planted + r12_rotcold_planted + r12_rotthenm_planted;
    if (nemeses & N_R12_EC) {
        /* Give any rot planted near t_end time to be scrubbed and repaired. */
        for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(250)) {
            r12_metrics_sample_all();
            pthread_mutex_lock(&r12_mu);
            uint64_t dmg = r12_chunk_damaged, rep = r12_chunk_repaired, qd = r12_repair_qdepth;
            pthread_mutex_unlock(&r12_mu);
            if (rep > 0 && qd == 0)
                break;
            if (dmg == 0 && now_ms() - t0 >= 5000)
                break;
        }
        r12_metrics_sample_all();
        pthread_mutex_lock(&r12_mu);
        uint64_t dmg = r12_chunk_damaged, rep = r12_chunk_repaired, unrec = r12_chunk_unrecoverable;
        pthread_mutex_unlock(&r12_mu);
        if (chunk_planted == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: chunk rot nemesis planted 0 rots\n");
            bad++;
        } else if (dmg == 0 || rep == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: %u chunk rot(s) planted, but /metrics reports %" PRIu64
                            " damaged (tsdb_chunk_scrub_damaged_total) and %" PRIu64 " repaired (tsdb_chunk_repairs_total{result=\"repaired\"})\n",
                    chunk_planted, dmg, rep);
            bad++;
        }
        if (unrec > 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: /metrics reports %" PRIu64 " unrecoverable chunk repair(s)\n", unrec);
            bad++;
        }
        if (r12_rotthenm_repair_timeout > 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: rotthenm had %u round(s) whose rotted chunk(s) were not repaired in time\n",
                    r12_rotthenm_repair_timeout);
            bad++;
        }
        if ((nemeses & N_ROTTHENM) && r12_rotthenm_rounds == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: rotthenm completed 0 rounds\n");
            bad++;
        }
        if (r12_rotthenm_read_fail > 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: rotthenm had %u read failure(s) (UNAVAILABLE/CORRUPT) with %d stripe members down\n",
                    r12_rotthenm_read_fail, want_ec_m);
            bad++;
        }
        if ((nemeses & N_ROTCOLD) && dmg > 0)
            bad += r12_gc_repair_check();
    }
    if (nemeses & N_ROTYOUNG) {
        for (int64_t t0 = now_ms(); now_ms() - t0 < 15000; hold_ms(250)) {
            r12_metrics_sample_all();
            pthread_mutex_lock(&r12_mu);
            uint64_t yd = r12_young_damaged, ar = r12_auto_resets_damaged;
            pthread_mutex_unlock(&r12_mu);
            if (yd > 0 && ar > 0)
                break;
            if (yd == 0 && now_ms() - t0 >= 5000)
                break;
        }
        r12_metrics_sample_all();
        pthread_mutex_lock(&r12_mu);
        uint64_t yd = r12_young_damaged, ar = r12_auto_resets_damaged;
        pthread_mutex_unlock(&r12_mu);
        if (r12_rotyoung_planted == 0) {
            fprintf(stderr, "jepsen-cluster: FAIL: r12: rotyoung planted 0 granule rots\n");
            bad++;
        } else if (yd == 0 || ar == 0 || r12_rotyoung_rebuilt == 0) {
            /* Makefile jepsen-cluster-selftest (youngskipgranule) matches this exact wording. */
            fprintf(stderr, "jepsen-cluster: FAIL: r12: rotyoung planted %u rot(s), rebuilt %u, /metrics reports "
                            "young_damaged=%" PRIu64 " auto_resets_damaged=%" PRIu64 "\n",
                    r12_rotyoung_planted, r12_rotyoung_rebuilt, yd, ar);
            bad++;
        }
    }
    return bad;
}

static void r12_summary(void)
{
    if (!(nemeses & N_R12))
        return;
    pthread_mutex_lock(&r12_mu);
    uint64_t cs = 0;
    for (int i = 0; i < nn; i++)
        cs += r12_chunk_scrub_chunks_node[i];
    uint64_t cd = r12_chunk_damaged, cr = r12_chunk_repaired, cu = r12_chunk_unrecoverable, cnl = r12_chunk_not_live,
             cb = r12_chunk_repair_bytes, yp = r12_young_parts, yd = r12_young_damaged, ard = r12_auto_resets_damaged,
             arf = r12_auto_resets_failed, arx = r12_auto_resets_xfer;
    pthread_mutex_unlock(&r12_mu);
    printf("r12: rotparity %u, rotcold %u (gc-race %s), rotthenm %u rounds (%u chunks rotted, %u repair-timeouts, %u reads, %u failed), "
           "rotyoung %u (%u rebuilt); /metrics chunk_scrubbed %" PRIu64 ", chunk_damaged %" PRIu64 ", chunk_repaired %" PRIu64 " (%" PRIu64
           " bytes, unrecoverable %" PRIu64 ", not_live %" PRIu64 "), young_parts %" PRIu64 ", young_damaged %" PRIu64
           ", auto_resets(damaged=%" PRIu64 ", failed=%" PRIu64 ", xfer=%" PRIu64 ")\n",
           r12_rotparity_planted, r12_rotcold_planted,
           r12_gc_race_done ? (r12_gc_race_bad ? "FAILED" : "ok") : "n/a", r12_rotthenm_rounds, r12_rotthenm_planted,
           r12_rotthenm_repair_timeout, r12_rotthenm_reads, r12_rotthenm_read_fail, r12_rotyoung_planted,
           r12_rotyoung_rebuilt, cs, cd, cr, cb, cu, cnl, yp, yd, ard, arf, arx);
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
