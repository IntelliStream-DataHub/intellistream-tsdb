/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * `tsdb cluster ...`: the admin ops of tsdbd's cluster thread (server/cluster.c), sent to a server's peer address.
 * No directory: the CLI holds no cluster state; it authenticates with the cluster secret (--secret-file) and prints
 * what the server answers.
 *
 *   tsdb cluster init PEER --secret-file F [--replication 3] [--min-isr 1] [--node-timeout-ms 3000]
 *        [--heartbeat-ms 500] [--isr-lag-ms 1000] [--rebuild-after-s 1800] [--voter-replace-s 600] [--voters 5]
 *        [--volumes-per-server 16]
 *   tsdb cluster node add PEER NEW_PEER --secret-file F      (PEER: any member; the CLI follows to the leader once)
 *   tsdb cluster status PEER --secret-file F                 (one line per fact, for people and scripts)
 *   tsdb cluster secret FILE                                 (a new secret: 32 random bytes as hex, mode 0600)
 */
#include "cluster.h"
#include "meta.h"
#include "place.h"
#include "raft/raft.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int cluster_cli(int argc, char **argv); /* tools/tsdb-cli.c */
int cluster_user_cli(int argc, char **argv); /* tools/cluster-user-cli.c */
int cluster_user_first_check(const char *name, const char *pwfile); /* cluster-user-cli.c */
int cluster_user_add_first(const char *addr, const uint8_t *secret, size_t slen, const char *name,
                           const char *pwfile);

/* The one text of the cluster commands: `tsdb cluster` (or help, --help) prints it on stdout and exits 0; a mistake in
 * a command prints the same on stderr and exits 2. docs/cluster-preview.md has the longer story. */
static const char cl_help[] =
    "tsdb cluster: the commands of tsdbd's cluster (a 0.x preview: replicated and erasure-coded volumes,\n"
    "failover and moves; docs/cluster-preview.md is the tutorial).\n"
    "\n"
    "usage: tsdb cluster secret FILE\n"
    "       tsdb cluster init PEER --secret-file F [options]\n"
    "       tsdb cluster node add PEER NEW_PEER --secret-file F\n"
    "       tsdb cluster node hold PEER NODE (--for DUR | --release) --secret-file F\n"
    "       tsdb cluster node out|destroyed PEER NODE --secret-file F\n"
    "       tsdb cluster node drain PEER NODE [--cancel] --secret-file F\n"
    "       tsdb cluster node replace PEER OLD (NEW | --cancel) --secret-file F\n"
    "       tsdb cluster volume add PEER NAME [--replicas N | --redundancy ec=K+M|replicas=N] --secret-file F\n"
    "       tsdb cluster volume gc resume PEER VOLUME --secret-file F\n"
    "       tsdb cluster volume force-leader PEER VOLUME NODE --secret-file F\n"
    "       tsdb cluster volume transfer PEER VOLUME NODE --secret-file F\n"
    "       tsdb cluster volume move PEER VOLUME --log --from NODE --to NODE [--cancel] --secret-file F\n"
    "       tsdb cluster volume move PEER VOLUME --pos P --to NODE --secret-file F\n"
    "       tsdb cluster replica reset PEER VOLUME NODE --secret-file F\n"
    "       tsdb cluster status PEER --secret-file F\n"
       "       tsdb cluster user add|passwd|drop|grant|revoke|list|import PEER ... --secret-file F\n"
    "       tsdb cluster help\n"
    "\n"
    "  PEER and NEW_PEER are a server's peer address, host:port (tsdbd --cluster, its --peer-port and --advertise).\n"
    "  No directory: these commands talk to a running server and keep no state of their own. The secret file is the\n"
    "  one every server was started with (tsdbd --cluster-secret-file); it is never given on the command line.\n"
    "\n"
    "  secret FILE         make a new cluster secret (32 random bytes as hex, mode 0600). Never overwrites a file.\n"
    "                      Copy it to every server, by a safe way.\n"
    "  init PEER           create the cluster on one server that is in no cluster yet. It becomes node 1 and the\n"
    "                      leader. Settings are fixed here, for the life of the cluster.\n"
    "  node add PEER NEW   add the server at NEW (it must be running with --cluster and in no cluster yet) to the\n"
    "                      cluster that PEER belongs to; PEER may be any member. The members vote\n"
    "                      in groups of 1, 3 or 5 (at most --voters); the rest watch and take a voter's place when\n"
    "                      one is lost. A witness (tsdbd --witness) is added the same way.\n"
    "  node hold PEER NODE (--for DUR | --release)  hold NODE against automatic OUT for DUR (e.g. 30m, 2h; at most\n"
    "                      1 day), or release an active hold.\n"
    "  node out PEER NODE  mark NODE out immediately so its log replicas and stripe positions rebuild elsewhere.\n"
    "  node drain PEER NODE [--cancel]  mark NODE draining to move every log replica and stripe position off it\n"
    "                      before marking it out automatically (--cancel returns a live draining node to up).\n"
    "  node replace PEER OLD (NEW | --cancel)  move every log replica and stripe position of OLD directly onto NEW\n"
    "                      (which must be up, accept volumes, have chunk stores if OLD holds stripe positions, and\n"
    "                      hold none of OLD's volumes); OLD is held while the replace runs and marked out once empty.\n"
    "                      --cancel clears an active replace and releases OLD's hold.\n"
    "  node destroyed PEER NODE  declare an OUT node permanently destroyed (final; frees its peer address and\n"
    "                      identity; more than M destroyed stripe holders mark an EC volume LOST).\n"
    "  volume add PEER NAME  a replicated volume called NAME on N servers (the cluster's --replication by default).\n"
    "                      The leader of the group picks its number and its servers; each of them makes the\n"
    "                      volume's directory (v<number> in its --dir) and opens it. Clients create and find\n"
    "                      series inside the volume they name. Its leader sends every write to the other copies.\n"
    "                      --redundancy ec=K+M (K 1 to 16, M 1 to 4): settled parts are erasure coded across a\n"
    "                      stripe group of K+M members with a chunk store up (picked once, never renumbered); its\n"
    "                      log is kept on M+1 servers and min_isr is M. replicas=N is --replicas N.\n"
    "  volume gc resume PEER VOLUME  turns the orphan collection of an erasure-coded volume (a name, else a\n"
    "                      number) back on. It stops after a force-leader (and a restore, once built; status:\n"
    "                      gc_suspended 1) until an operator has checked that no chunk the volume still needs\n"
    "                      would be taken for an orphan.\n"
    "  volume force-leader PEER VOLUME NODE  when no ISR member of volume number VOLUME is up (clients get\n"
    "                      UNAVAILABLE NO_OWNER), makes the replica on node NODE its leader. Unclean: writes\n"
    "                      acknowledged past that replica's log end are lost; the leader of the group logs its log\n"
    "                      end beside the commit point of the last ISR shrink.\n"
    "  volume transfer PEER VOLUME NODE  hands the leadership of volume number VOLUME to NODE, an in-sync copy:\n"
    "                      writes pause while the leader drains to it (at most --transfer-wait-ms).\n"
    "  volume move PEER VOLUME --log --from A --to B  moves the copy on node A to node B: B is added and filled,\n"
    "                      then A is removed (--log: B catches up from the log and a snapshot; --cancel stops a\n"
    "                      move that has not removed A yet).\n"
    "  volume move PEER VOLUME --pos P --to B  moves stripe position P of erasure-coded volume VOLUME to node B\n"
    "                      (B is marked filling with the previous holder kept until whole).\n"
    "  replica reset PEER VOLUME NODE  gives the copy on NODE (neither the leader nor in sync) a new identity: its\n"
    "                      directory is set aside as v<N>.lost-<rid> and it is rebuilt from a snapshot.\n"
    "  status PEER         what that server knows: its role, the leader, the settings, every member (up or down,\n"
    "                      incarnation, when it was last heard), the volumes and users. One fact a line.\n"
    "\n"
    "  user ...            users and grants of the whole cluster, kept by the group (`tsdb cluster user` says how).\n"
    "                      init --superuser NAME --password-file F makes the first superuser.\n"
    "\n"
    "  init options (each takes a whole number above 0; defaults in brackets):\n"
    "    --replication N         copies of every volume the cluster will keep [3]\n"
    "    --min-isr N             copies that must be in sync for a write to count, at most --replication [1]\n"
    "    --node-timeout-ms MS    a server is marked down after this long without a heartbeat [3000]\n"
    "    --heartbeat-ms MS       how often servers report, below --node-timeout-ms [500]\n"
    "    --isr-lag-ms MS         a copy silent this long leaves the in-sync set [1000] (at least 500)\n"
    "    --rebuild-after-s S     a server down this long is marked out [1800] (stored, not acted on yet)\n"
    "    --voter-replace-s S     a voter down this long is replaced by another member [600]\n"
    "    --voters N              the most voters the group has (an even number counts as one less) [5]\n"
    "    --volumes-per-server N  volumes led per server at init [16] (stored, not acted on yet)\n"
    "    --mode replicated|shared    shared-nothing copies, or shared (NFS) volumes [replicated] (shared: not built)\n"
    "    --failover lease|attested|bmc   how a dead owner is replaced in shared mode [lease] (shared: not built)\n"
    "    --read-barrier on|off   owners answer reads only behind a fenced write [on] (stored, not acted on yet)\n"
    "    --lease-s S             the lease of a shared volume [10] (stored, not acted on yet)\n"
    "    --lease-ms MS           a replicated volume's leader read lease [2000]\n"
    "    --drift-ppm N           the clock drift every lease allows for [500]\n"
    "    --unanswered-ms MS      a copy kept in sync without answering a promotion is dropped after this [1000]\n"
    "    --commit-interval-ms MS the commit point reaches the copies within this [5]\n"
    "    --repl-retain-mb MB     log kept for a copy that fell behind, at most [4096]\n"
    "    --repl-retain-s S       and for at most this long [1800]\n"
    "    --idle-seal-ms MS       an idle leader closes its log segment after this [1000] (at least 100)\n"
    "    --rebuild-mb-s MB       a snapshot's send rate per link [200]\n"
    "    --rebuild-max N         snapshots sent and received per server at once [2] (at most 16)\n"
    "    --transfer-wait-ms MS   a leader transfer's drain, at most [2000] (100 to 60000)\n"
    "    --leader-balance-s S    leadership goes back to a volume's first server once it has been in sync this\n"
    "                            long [0: off] (at most 86400)\n"
    "    --opening-hold-s S      extra hold from OUT while a server reports opening [3600] (0 to 86400)\n"
    "    --failstop-out-s S      fast path to OUT after a fail-stop beat or replica fault [60]\n"
    "    --out-guard-pct N       mass-OUT guard threshold, % of non-OUT members down [25] (1 to 100)\n"
    "    --out-guard-hold-s S    upper bound of the mass-OUT guard before OUT proceeds [7200] (1 to 86400)\n"
    "    --repl-retain-down-gb GB  per-server WAL retention budget for down or held members [64]\n"
    "    --young-high-pct N      data dir fill % at which a member reports FULL [90] (10 to 99, > --young-move-pct)\n"
    "    --balance-mb-s MB       balancer move rate, MB/s [0: = rebuild-mb-s]\n"
    "    --rebuild-concurrency N EC objects filled in flight per server [4] (1 to 64)\n"
    "    --scrub-days N          scrub every chunk and young part every N days [14] (1 to 365) (stored, not acted on yet)\n"
    "    --scrub-mb-s MB         scrub pace per disk, MB/s [50] (stored, not acted on yet)\n"
    "    --scrub-out-count N     damaged chunks per store per UTC day before OUT [8] (stored, not acted on yet)\n"
    "    --log-on-hdd on|off     allow log replicas on HDD-only members [off] (stored, not acted on yet)\n"
    "    --young-warn-pct N      fast disk fill % warning alert [75] (1 to 98, <= move < high) (stored, not acted on yet)\n"
    "    --young-move-pct N      fast disk fill % move-off mark [80] (1 to 98, >= warn, < high) (stored, not acted on yet)\n"
    "    --disk-stall-s S        disk probe stall before DOWN, s [10] (1 to 3600) (stored, not acted on yet)\n"
    "  The settings are fixed here and shown by status; they cannot be changed on a running cluster.\n"
    "  A setting marked (stored, not acted on yet) is kept and shown, and nothing uses it.\n";

static int cl_usage(void)
{
    fputs(cl_help, stderr);
    return 2;
}

static int num(const char *flag, const char *s, uint32_t *out)
{
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    if (!*s || *end || v == 0 || v > 0x7fffffff) {
        fprintf(stderr, "tsdb cluster: %s %s: expected a whole number above 0\n", flag, s);
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

/* Duration in seconds (plain integer, or suffix s/m/h/d), bounded to 1 .. max_s (D3, D22). */
static int dur_s(const char *flag, const char *s, uint32_t max_s, uint32_t *out)
{
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    unsigned long mult = 1;
    if (!*s || end == s) {
        fprintf(stderr, "tsdb cluster: %s %s: expected a duration (e.g. 600s, 10m, 1h)\n", flag, s);
        return -1;
    }
    if (*end) {
        if (end[1] != '\0') {
            fprintf(stderr, "tsdb cluster: %s %s: unknown unit (use s, m, h or d)\n", flag, s);
            return -1;
        }
        if (*end == 's')
            mult = 1;
        else if (*end == 'm')
            mult = 60;
        else if (*end == 'h')
            mult = 3600;
        else if (*end == 'd')
            mult = 86400;
        else {
            fprintf(stderr, "tsdb cluster: %s %s: unknown unit (use s, m, h or d)\n", flag, s);
            return -1;
        }
    }
    if (v == 0 || v > (unsigned long)max_s / mult) {
        fprintf(stderr, "tsdb cluster: %s %s: expected 1 s to %" PRIu32 " s\n", flag, s, max_s);
        return -1;
    }
    *out = (uint32_t)(v * mult);
    return 0;
}

/* `volume gc resume`'s VOLUME: a name looked up in the status of PEER (any member answers it), else a number (a
 * volume may be named "7": its name wins over volume number 7) */
static int gc_volume(const char *s, const char *addr, const uint8_t *secret, size_t slen, uint32_t *out)
{
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    int numeric = *s && !*end && v > 0 && v <= UINT32_MAX;
    char err[1024];
    int rc;
    uint8_t *reply = NULL;
    size_t len = 0;
    if (cluster_admin(addr, secret, slen, CLUSTER_OP_STATUS, NULL, 0, 30000, &rc, &reply, &len, err, sizeof err) != 0) {
        fprintf(stderr, "tsdb cluster: %s\n", err);
        return -1;
    }
    struct cluster_status st;
    struct meta_state m;
    meta_init(&m);
    int found = 0;
    if (rc == 0 && cluster_status_parse(reply, len, &st) == 0) {
        if (st.image_len && meta_snapshot_decode(&m, st.image, st.image_len) == META_OK)
            for (uint32_t i = 0; i < m.nvolumes; i++)
                if (!strcmp(m.volumes[i].name, s)) {
                    *out = m.volumes[i].number;
                    found = 1;
                }
        cluster_status_free(&st);
    }
    meta_free(&m);
    free(reply);
    if (!found && numeric) {
        *out = (uint32_t)v;
        found = 1;
    }
    if (!found)
        fprintf(stderr, "tsdb cluster volume gc resume: no volume named %s in the status of %s\n", s, addr);
    return found ? 0 : -1;
}

static const char *role_name(int r)
{
    static const char *n[] = {"follower", "pre-candidate", "candidate", "leader"};
    return r >= 0 && r < 4 ? n[r] : "?";
}

static uint64_t status_add_sat(uint64_t a, uint64_t b)
{
    return b > UINT64_MAX - a ? UINT64_MAX : a + b;
}

static void status_print_guard(const struct meta_state *m, uint64_t now_ms)
{
    struct pout_guard g;
    pout_guard_eval(m, m->set.out_guard_pct, m->set.out_guard_hold_s, now_ms, &g);
    if (!g.active)
        return;
    uint64_t since_s = now_ms > g.start_ms ? (now_ms - g.start_ms) / UINT64_C(1000) : 0;
    uint64_t left_s = g.due_ms > now_ms ? (g.due_ms - now_ms + UINT64_C(999)) / UINT64_C(1000) : 0;
    const char *why = g.pct_triggered && g.rack_triggered ? "pct+rack" : g.pct_triggered ? "pct" : "rack";
    printf("guard active yes reason %s down %" PRIu32 "/%" PRIu32 " since %" PRIu64 "s bound %" PRIu32 "s left %" PRIu64
           "s\n",
           why, g.ndown, g.non_out, since_s, m->set.out_guard_hold_s, left_s);
}

static void print_status(const struct cluster_status *st)
{
    static const char *mode[] = {"none", "core", "watch"};
    printf("node %" PRIu32 " mode %s role %s term %" PRIu64 " leader %" PRIu32 " commit %" PRIu64
           " applied %" PRIu64 "\n",
           st->self, st->mode < 3 ? mode[st->mode] : "?", st->mode == CLUSTER_MODE_CORE ? role_name(st->role) : "-",
           st->term, st->leader, st->commit, st->applied);
    if (st->stale)
        printf("stale this is the copy of an earlier run; no watch stream has confirmed it yet\n");
    struct meta_state m;
    meta_init(&m);
    if (!st->image_len || meta_snapshot_decode(&m, st->image, st->image_len) != META_OK) {
        printf("cluster %s\n", st->image_len ? "(the state does not decode)" : "(no state yet)");
        meta_free(&m);
        return;
    }
    uint64_t now_ms = st->wall_ms;
    if (!now_ms) {
        struct timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts) == 0)
            now_ms = (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
    }
    char u[33];
    for (int i = 0; i < 16; i++)
        sprintf(u + 2 * i, "%02x", m.uuid[i]);
    if (m.initialised)
        printf("cluster %s replication %" PRIu32 " min_isr %" PRIu32 " node_timeout_ms %" PRIu32
               " heartbeat_ms %" PRIu32 " voter_replace_s %" PRIu32 " voters_max %" PRIu32 " mode %s failover %s"
               " read_barrier %s lease_s %" PRIu32 " restore_gen %" PRIu64 "\n",
               u, m.set.replication, m.set.min_isr, m.set.node_timeout_ms, m.set.heartbeat_ms,
               m.set.voter_replace_s, m.set.voters, m.mode == META_MODE_SHARED ? "shared" : "replicated",
               m.set.failover == META_FAILOVER_BMC ? "bmc" : m.set.failover == META_FAILOVER_ATTESTED ? "attested"
                                                                                                     : "lease",
               m.set.read_barrier ? "on" : "off", m.set.lease_s, m.restore_gen);
    if (m.initialised) {
        printf("replicated lease_ms %" PRIu32 " drift_ppm %" PRIu32 " unanswered_ms %" PRIu32
               " commit_interval_ms %" PRIu32 " repl_retain_mb %" PRIu32 " repl_retain_s %" PRIu32
               " idle_seal_ms %" PRIu32 " rebuild_mb_s %" PRIu32 " rebuild_max %" PRIu32 " transfer_wait_ms %" PRIu32
               " leader_balance_s %" PRIu32 "\n",
               m.set.lease_ms, m.set.drift_ppm, m.set.unanswered_ms, m.set.commit_interval_ms,
               m.set.repl_retain_mb, m.set.repl_retain_s, m.set.idle_seal_ms, m.set.rebuild_mb_s, m.set.rebuild_max,
               m.set.transfer_wait_ms, m.set.leader_balance_s);
        printf("failure rebuild_after_s %" PRIu32 " opening_hold_s %" PRIu32 " failstop_out_s %" PRIu32
               " out_guard_pct %" PRIu32 " out_guard_hold_s %" PRIu32 " repl_retain_down_gb %" PRIu32
               " young_high_pct %" PRIu32 " balance_mb_s %" PRIu32 " rebuild_concurrency %" PRIu32 "\n",
               m.set.rebuild_after_s, m.set.opening_hold_s, m.set.failstop_out_s, m.set.out_guard_pct,
               m.set.out_guard_hold_s, m.set.repl_retain_down_gb, m.set.young_high_pct, m.set.balance_mb_s,
               m.set.rebuild_concurrency);
        printf("scrub scrub_days %" PRIu32 " scrub_mb_s %" PRIu32 " scrub_out_count %" PRIu32
               " log_on_hdd %s young_warn_pct %" PRIu32 " young_move_pct %" PRIu32 " disk_stall_s %" PRIu32 "\n",
               m.set.scrub_days, m.set.scrub_mb_s, m.set.scrub_out_count, m.set.log_on_hdd ? "on" : "off",
               m.set.young_warn_pct, m.set.young_move_pct, m.set.disk_stall_s);
    } else {
        printf("cluster %s (not initialised)\n", u);
    }
    raft_config cfg = {0};
    if (m.config_len)
        raft_config_decode(m.config, m.config_len, &cfg);
    printf("group voters");
    int any = 0;
    for (int pass = 0; pass < 2; pass++) {
        if (pass) {
            printf("%s learners", any ? "" : " -");
            any = 0;
        }
        for (uint32_t i = 0; i < cfg.n; i++)
            if (cfg.m[i].learner == (pass == 1)) {
                printf("%s%" PRIu32, any ? "," : " ", cfg.m[i].id);
                any = 1;
            }
    }
    printf("%s\n", any ? "" : " -");
    if (m.initialised)
        status_print_guard(&m, now_ms);
    for (uint32_t i = 0; i < m.nmembers; i++) {
        const struct meta_member *mb = &m.members[i];
        char heard[48] = "never";
        for (uint32_t j = 0; j < st->nheard; j++)
            if (st->heard[j].id == mb->id && st->heard[j].ago_ms != UINT64_MAX)
                snprintf(heard, sizeof heard, "%" PRIu64 "ms", st->heard[j].ago_ms);
        const struct cluster_heard *h = NULL;
        for (uint32_t j = 0; j < st->nheard; j++)
            if (st->heard[j].id == mb->id)
                h = &st->heard[j];
        printf("member %" PRIu32 " %s incarnation %" PRIu64 " addr %s peer %s rack %s heard %s feed %s voter_ok %d"
               " volumes_ok %d core_dropped %d read_barrier %s",
               mb->id, meta_member_state_name(mb->state), mb->incarnation, mb->addr, mb->peer,
               mb->rack[0] ? mb->rack : "-", heard, mb->power_feed[0] ? mb->power_feed : "-",
               !!(mb->flags & META_MF_VOTER_OK), !!(mb->flags & META_MF_VOLUMES_OK), h ? h->core_dropped : 0,
               !h || (!h->flags && !h->state_free) ? "?" : h->read_barrier ? "on" : "off");
        if (mb->state == META_DOWN && mb->down_since_ms) {
            uint64_t age_s = now_ms > mb->down_since_ms ? (now_ms - mb->down_since_ms) / 1000ull : 0;
            uint32_t base_s = mb->down_reason == META_DOWN_FAILSTOP ? m.set.failstop_out_s : m.set.rebuild_after_s;
            uint64_t due_ms = status_add_sat(mb->down_since_ms, (uint64_t)base_s * 1000ull);
            if (h && h->opening && h->ago_ms != UINT64_MAX &&
                (m.set.node_timeout_ms == 0 || h->ago_ms < m.set.node_timeout_ms))
                due_ms = status_add_sat(due_ms, (uint64_t)m.set.opening_hold_s * 1000ull);
            if (mb->hold_until_ms > due_ms)
                due_ms = mb->hold_until_ms;
            uint64_t out_in_s = due_ms > now_ms ? (due_ms - now_ms + 999ull) / 1000ull : 0;
            printf(" down_reason %s down_age %" PRIu64 "s out_in %" PRIu64 "s",
                   meta_down_reason_name(mb->down_reason), age_s, out_in_s);
        }
        if (mb->hold_until_ms > now_ms)
            printf(" hold_left %" PRIu64 "s", (mb->hold_until_ms - now_ms + UINT64_C(999)) / UINT64_C(1000));
        if (mb->replaced_by)
            printf(" replaced_by %" PRIu32, mb->replaced_by);
        if (mb->role != META_ROLE_BOTH || mb->ndisks > 0)
            printf(" role %s cap %s disks %" PRIu32, meta_role_name(mb->role), meta_cap_name(mb->capability),
                   mb->ndisks);
        printf("\n");
        for (uint32_t j = 0; j < mb->ndisks && j < META_DISKS_MAX; j++) {
            const struct meta_disk *d = &mb->disks[j];
            char sid[33];
            for (int k = 0; k < 16; k++)
                sprintf(sid + 2 * k, "%02x", d->store_id[k]);
            const char *uses = (d->uses == META_DISK_USE_ALL)     ? "log,chunk"
                               : (d->uses == META_DISK_USE_LOG)   ? "log"
                               : (d->uses == META_DISK_USE_CHUNK) ? "chunk"
                                                                  : "-";
            printf("disk node %" PRIu32 " store %s class %s uses %s size %" PRIu64 " sync_us %" PRIu64 " state %s\n",
                   mb->id, sid, meta_disk_class_name(d->disk_class), uses, d->size_bytes, d->sync_us,
                   meta_store_state_name(d->state));
        }
        for (uint32_t j = 0; h && j < h->nvols; j++) {
            const struct cluster_vol_status *v = &h->vols[j];
            printf("volume-report node %" PRIu32 " volume %" PRIu32 " role %s health %s epoch %" PRIu64
                   " domain %s local_fault_s %" PRIu64 " fenced_age_ms %" PRIu64 "\n",
                   mb->id, v->v.volume, v->v.role == CL_ROLE_OWNER ? "owner" : v->v.role == CL_ROLE_RELEASING ? "releasing" : v->v.role == CL_ROLE_PROMOTING ? "promoting" : "none",
                   v->v.health == CL_HEALTH_OK             ? "ok"
                   : v->v.health == CL_HEALTH_LOCAL_FAULT  ? "local-fault"
                   : v->v.health == CL_HEALTH_DOMAIN_FAULT ? "domain-fault"
                   : v->v.health == CL_HEALTH_DAMAGED      ? "damaged"
                                                           : "unknown",
                   v->v.epoch, v->v.domain[0] ? v->v.domain : "-", v->local_fault_s, v->v.fenced_age_ms);
        }
        for (uint32_t j = 0; h && j < h->ndoms; j++)
            printf("domain-report node %" PRIu32 " %s %s\n", mb->id, h->doms[j].name,
                   h->doms[j].state == CL_DOM_OK ? "ok" : h->doms[j].state == CL_DOM_SLOW ? "slow" : "fault");
        for (uint32_t j = 0; h && j < h->nprogress_vols; j++) {
            const struct creport_vol_progress *vp = &h->progress_vols[j];
            if (vp->role == CREPORT_REBUILD_NONE && vp->settle_keys_waiting == 0 && vp->young_scrub_age_s == 0)
                continue;
            printf("progress node %" PRIu32 " volume %" PRIu32 " role %s done %" PRIu64 "/%" PRIu64
                   " started_ms %" PRIu64 " last_progress_ms %" PRIu64 " stalled %u settle_keys %" PRIu32
                   " settle_oldest_s %" PRIu64 " young_scrub_age_s %" PRIu64 "\n",
                   mb->id, vp->volume, creport_rebuild_role_name(vp->role), vp->bytes_done, vp->bytes_total,
                   vp->started_ms, vp->last_progress_ms, (unsigned)vp->stalled, vp->settle_keys_waiting,
                   vp->settle_oldest_wait_s, vp->young_scrub_age_s);
        }
        for (uint32_t j = 0; h && j < h->nprogress_stores; j++) {
            const struct creport_store_progress *sp = &h->progress_stores[j];
            if (sp->scrub_age_s == 0 && sp->drain_bytes_left == 0)
                continue;
            printf("store-progress node %" PRIu32 " store %" PRIu32 " scrub_age_s %" PRIu64
                   " drain_bytes_left %" PRIu64 "\n",
                   mb->id, sp->store, sp->scrub_age_s, sp->drain_bytes_left);
        }
    }
    for (uint32_t i = 0; i < m.nvolumes; i++) {
        const struct meta_volume *v = &m.volumes[i];
        char vu[33];
        for (int k = 0; k < 16; k++)
            sprintf(vu + 2 * k, "%02x", v->uuid[k]);
        printf("volume %" PRIu32 " name %s uuid %s leader %" PRIu32 " epoch %" PRIu64 " min_isr %" PRIu32
               " replicas", v->number, v->name, vu, v->leader, v->epoch, v->min_isr);
        for (uint32_t k = 0; k < v->nrep; k++)
            printf("%s%" PRIu32 "%s", k ? "," : " ", v->rep[k].node, v->rep[k].learner ? "(learner)" : "");
        printf(" isr");
        for (uint32_t k = 0; k < v->nisr; k++)
            printf("%s%" PRIu32, k ? "," : " ", v->isr[k]);
        printf("%s isr_version %" PRIu64 " set_version %" PRIu64, v->nisr ? "" : " -", v->isr_version,
               v->set_version);
        if (v->nrep) /* leader balance (D15) moves the leadership back to rep[0] */
            printf(" preferred %" PRIu32, v->rep[0].node);
        if (v->redundancy == META_RED_EC) { /* R8: the stripe group by position (E9) and the orphan collection (E14) */
            printf(" redundancy ec=%u+%u stripe", v->ec_k, v->ec_m);
            for (uint32_t k = 0; k < v->nstripe && k < META_STRIPE_MAX; k++)
                printf("%s%" PRIu32, k ? "," : " ", v->stripe[k]);
            printf(" stripe_version %" PRIu64 " gc_suspended %u", v->stripe_version, v->gc_suspended);
            if (place_stripe_mixed_class(&m, v))
                printf(" mixed_class 1");
        } else {
            printf(" redundancy replicas=%" PRIu32, v->nrep);
        }
        printf(" rack loss survivable: %s\n", place_rack_survivable(&m, v) ? "yes" : "no");
        if (v->redundancy == META_RED_EC && place_stripe_mixed_class(&m, v))
            printf("health warn=mixed_stripe_class volume=%" PRIu32 " name=%s\n", v->number, v->name);
        for (uint32_t k = 0; k < v->nrep; k++) {
            printf("replica volume %" PRIu32 " node %" PRIu32 " rid %016" PRIx64 " open_gen %" PRIu64, v->number,
                   v->rep[k].node, v->rep[k].rid, v->rep[k].open_gen);
            if (v->rep[k].replaces) /* a move in progress: this learner takes that member's slot (D10) */
                printf(" replaces %" PRIu32, v->rep[k].replaces);
            printf("\n");
        }
        if (v->redundancy == META_RED_EC) {
            for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                if (v->stripe_pos[p].state != META_STRIPE_FILLING)
                    continue;
                const struct cluster_fill_report *fr = NULL;
                for (uint32_t j = 0; j < st->nheard && !fr; j++) {
                    if (st->heard[j].id != v->stripe[p])
                        continue;
                    for (uint32_t fi = 0; fi < st->heard[j].nfills; fi++) {
                        const struct cluster_fill_report *c = &st->heard[j].fills[fi];
                        if (c->volume == v->number && c->pos == p && c->version == v->stripe_version) {
                            fr = c;
                            break;
                        }
                    }
                }
                printf("fill volume %" PRIu32 " pos %" PRIu32 " node %" PRIu32 " prev %" PRIu32 " version %" PRIu64
                       " done %" PRIu64 "/%" PRIu64 " whole %u\n",
                       v->number, p, v->stripe[p], v->stripe_pos[p].prev, v->stripe_version, fr ? fr->done : 0,
                       fr ? fr->total : 0, fr ? fr->whole : 0u);
            }
        }
    }
    printf("volumes %" PRIu32 " users %" PRIu32 "\n", m.nvolumes, m.nusers);
    if (st->ntr >= CL_TR_N) /* the Raft leader's side counts on the member that ran them, the rest on every member */
        printf("transfers started %" PRIu64 " done %" PRIu64 " timed_out %" PRIu64 " cancelled %" PRIu64
               " fell_back %" PRIu64 " balance %" PRIu64 " drains_timed_out %" PRIu64 " resumed_blind %" PRIu64
               " ready %" PRIu64 " cuts %" PRIu64 "\n",
               st->tr[CL_TR_STARTED], st->tr[CL_TR_DONE], st->tr[CL_TR_TIMEDOUT], st->tr[CL_TR_CANCELLED],
               st->tr[CL_TR_FALLBACK], st->tr[CL_TR_BALANCE], st->tr[CL_TR_DRAIN_TIMEDOUT], st->tr[CL_TR_BLIND],
               st->tr[CL_TR_READY], st->tr[CL_TR_CUTS]);
    if (st->ntr >= CL_TR_N) /* replica moves (cmove.c): counted on the Raft leader that ran them */
        printf("moves started %" PRIu64 " done %" PRIu64 " cancelled %" PRIu64 "\n", st->tr[CL_TR_MV_STARTED],
               st->tr[CL_TR_MV_DONE], st->tr[CL_TR_MV_CANCELLED]);
    fputs(st->moves, stdout); /* "move volume V node A -> node B: STEP, AGE s", one per move running */
    fputs(st->links, stdout); /* "peer links KIND: none N header N full N tcp N rdma N", the answering server's */
    meta_free(&m);
}

int cluster_cli(int argc, char **argv)
{
    /* argv[0] is "cluster". No verb, or help: the commands, on stdout, success (asking is not a mistake). */
    if (argc == 1 || (argc == 2 && (!strcmp(argv[1], "help") || !strcmp(argv[1], "--help") ||
                                    !strcmp(argv[1], "-h")))) {
        fputs(cl_help, stdout);
        return 0;
    }
    if (argc < 3)
        return cl_usage();
    if (!strcmp(argv[1], "secret")) {
        char err[1024];
        if (argc != 3)
            return cl_usage();
        if (cluster_make_secret(argv[2], err, sizeof err) != 0) {
            fprintf(stderr, "tsdb cluster: %s\n", err);
            return 1;
        }
        printf("wrote a new cluster secret to %s (copy it to every server; tsdbd --cluster-secret-file)\n", argv[2]);
        return 0;
    }
    const char *verb = argv[1], *addr = NULL, *newpeer = NULL, *secret_file = NULL;
    const char *superuser = NULL, *pwfile = NULL;
    uint32_t force_vol = 0, force_node = 0; /* volume force-leader */
    int r4op = 0, r9op = 0;                               /* R4 and R9 operator ops */
    const char *gc_vol = NULL;                            /* volume gc resume: a number or a name */
    uint32_t r4_vol = 0, r4_node = 0, mv_from = 0, mv_to = 0, mv_flags = 0;
    uint32_t r9_node = 0, r9_node2 = 0, hold_for_s = 0, hold_flags = 0, drain_flags = 0, repl_flags = 0, mv_pos = 0;
    int have_pos = 0, have_hold_for = 0;
    int i = 2;
    if (!strcmp(verb, "user"))
        return cluster_user_cli(argc, argv);
    if (!strcmp(verb, "node")) {
        if (argc >= 5 && !strcmp(argv[2], "add")) {
            addr = argv[3];
            newpeer = argv[4];
            i = 5;
        } else if (argc >= 5 && (!strcmp(argv[2], "hold") || !strcmp(argv[2], "out") || !strcmp(argv[2], "drain") ||
                                 !strcmp(argv[2], "destroyed"))) {
            addr = argv[3];
            if (num("NODE", argv[4], &r9_node) != 0)
                return 2;
            r9op = !strcmp(argv[2], "hold")    ? CLUSTER_OP_NODE_HOLD
                   : !strcmp(argv[2], "out")   ? CLUSTER_OP_NODE_OUT
                   : !strcmp(argv[2], "drain") ? CLUSTER_OP_NODE_DRAIN
                                               : CLUSTER_OP_NODE_DESTROYED;
            i = 5;
        } else if (argc >= 6 && !strcmp(argv[2], "replace")) {
            addr = argv[3];
            if (num("OLD", argv[4], &r9_node) != 0)
                return 2;
            if (!strcmp(argv[5], "--cancel")) {
                repl_flags |= CLUSTER_REPLACE_CANCEL;
                r9_node2 = 0;
            } else {
                if (num("NEW", argv[5], &r9_node2) != 0)
                    return 2;
                if (r9_node == r9_node2) {
                    fprintf(stderr, "tsdb cluster node replace: OLD and NEW must be different nodes\n");
                    return 2;
                }
            }
            r9op = CLUSTER_OP_NODE_REPLACE;
            i = 6;
        } else {
            return cl_usage();
        }
    } else if ((!strcmp(verb, "volume") && argc >= 6 && !strcmp(argv[2], "transfer")) ||
               (!strcmp(verb, "replica") && argc >= 6 && !strcmp(argv[2], "reset"))) {
        addr = argv[3];
        if (num("VOLUME", argv[4], &r4_vol) != 0 || num("NODE", argv[5], &r4_node) != 0)
            return 2;
        r4op = !strcmp(verb, "replica") ? CLUSTER_OP_REPLICA_RESET : CLUSTER_OP_VOLUME_TRANSFER;
        i = 6;
    } else if (!strcmp(verb, "volume") && argc >= 5 && !strcmp(argv[2], "move")) {
        addr = argv[3];
        if (num("VOLUME", argv[4], &r4_vol) != 0)
            return 2;
        r4op = CLUSTER_OP_VOLUME_MOVE;
        i = 5;
    } else if (!strcmp(verb, "volume") && argc >= 6 && !strcmp(argv[2], "gc")) {
        if (strcmp(argv[3], "resume") != 0)
            return cl_usage();
        addr = argv[4];
        gc_vol = argv[5];
        i = 6;
    } else if (!strcmp(verb, "volume") && argc >= 6 && !strcmp(argv[2], "force-leader")) {
        addr = argv[3];
        char *end1, *end2;
        unsigned long fv = strtoul(argv[4], &end1, 10), fn = strtoul(argv[5], &end2, 10);
        if (*end1 || *end2 || !fv || !fn || fv > UINT32_MAX || fn > UINT32_MAX)
            return cl_usage();
        force_vol = (uint32_t)fv;
        force_node = (uint32_t)fn;
        i = 6;
    } else if (!strcmp(verb, "volume")) {
        if (argc < 5 || strcmp(argv[2], "add") != 0)
            return cl_usage();
        addr = argv[3];
        newpeer = argv[4]; /* the volume's name */
        i = 5;
    } else if (!strcmp(verb, "init") || !strcmp(verb, "status")) {
        addr = argv[2];
        i = 3;
    } else {
        return cl_usage();
    }
    struct meta_settings s;
    meta_settings_default(&s);
    int mode = META_MODE_REPLICATED;
    uint32_t replicas = 0; /* volume add: 0 takes the cluster's replication (an ec volume's m + 1) */
    uint32_t ec_k = 0, ec_m = 0; /* volume add --redundancy ec=K+M */
    struct {
        const char *flag;
        uint32_t *v;
    } nums[] = {{"--replication", &s.replication},
                {"--min-isr", &s.min_isr},
                {"--node-timeout-ms", &s.node_timeout_ms},
                {"--heartbeat-ms", &s.heartbeat_ms},
                {"--isr-lag-ms", &s.isr_lag_ms},
                {"--rebuild-after-s", &s.rebuild_after_s},
                {"--failstop-out-s", &s.failstop_out_s},
                {"--out-guard-pct", &s.out_guard_pct},
                {"--out-guard-hold-s", &s.out_guard_hold_s},
                {"--young-high-pct", &s.young_high_pct},
                {"--rebuild-concurrency", &s.rebuild_concurrency},
                {"--scrub-days", &s.scrub_days},
                {"--scrub-mb-s", &s.scrub_mb_s},
                {"--scrub-out-count", &s.scrub_out_count},
                {"--young-warn-pct", &s.young_warn_pct},
                {"--young-move-pct", &s.young_move_pct},
                {"--disk-stall-s", &s.disk_stall_s},
                {"--voter-replace-s", &s.voter_replace_s},
                {"--voters", &s.voters},
                {"--volumes-per-server", &s.volumes_per_server},
                {"--lease-s", &s.lease_s},
                {"--lease-ms", &s.lease_ms},
                {"--drift-ppm", &s.drift_ppm},
                {"--unanswered-ms", &s.unanswered_ms},
                {"--commit-interval-ms", &s.commit_interval_ms},
                {"--repl-retain-mb", &s.repl_retain_mb},
                {"--repl-retain-s", &s.repl_retain_s},
                {"--idle-seal-ms", &s.idle_seal_ms},
                {"--rebuild-mb-s", &s.rebuild_mb_s},
                {"--rebuild-max", &s.rebuild_max},
                {"--transfer-wait-ms", &s.transfer_wait_ms}};
    for (; i < argc; i++) {
        if (!strcmp(argv[i], "--secret-file") && i + 1 < argc) {
            secret_file = argv[++i];
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--superuser")) {
            superuser = argv[++i];
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--password-file")) {
            pwfile = argv[++i];
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--mode")) {
            mode = !strcmp(argv[i + 1], "shared") ? META_MODE_SHARED
                   : !strcmp(argv[i + 1], "replicated") ? META_MODE_REPLICATED : 0;
            if (!mode)
                return cl_usage();
            i++;
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--failover")) {
            s.failover = !strcmp(argv[i + 1], "lease") ? META_FAILOVER_LEASE
                         : !strcmp(argv[i + 1], "attested") ? META_FAILOVER_ATTESTED
                         : !strcmp(argv[i + 1], "bmc") ? META_FAILOVER_BMC : 99;
            if (s.failover == 99)
                return cl_usage();
            i++;
            continue;
        }
        if (r9op == CLUSTER_OP_NODE_HOLD && !strcmp(argv[i], "--release")) {
            hold_flags |= CLUSTER_HOLD_RELEASE;
            continue;
        }
        if (r9op == CLUSTER_OP_NODE_DRAIN && !strcmp(argv[i], "--cancel")) {
            drain_flags |= CLUSTER_DRAIN_CANCEL;
            continue;
        }
        if (r9op == CLUSTER_OP_NODE_HOLD && i + 1 < argc && !strcmp(argv[i], "--for")) {
            if (dur_s("--for", argv[i + 1], 86400, &hold_for_s) != 0)
                return 2;
            have_hold_for = 1;
            i++;
            continue;
        }
        if (r4op == CLUSTER_OP_VOLUME_MOVE && (!strcmp(argv[i], "--log") || !strcmp(argv[i], "--cancel"))) {
            mv_flags |= !strcmp(argv[i], "--log") ? CLUSTER_MOVE_LOG : CLUSTER_MOVE_CANCEL;
            continue;
        }
        if (r4op == CLUSTER_OP_VOLUME_MOVE && i + 1 < argc && !strcmp(argv[i], "--pos")) {
            char *end;
            unsigned long p = strtoul(argv[i + 1], &end, 10);
            if (!argv[i + 1][0] || *end || p >= META_STRIPE_MAX) {
                fprintf(stderr, "tsdb cluster volume move: --pos %s: expected 0 to %d\n", argv[i + 1],
                        META_STRIPE_MAX - 1);
                return 2;
            }
            mv_pos = (uint32_t)p;
            have_pos = 1;
            i++;
            continue;
        }
        if (r4op == CLUSTER_OP_VOLUME_MOVE && i + 1 < argc && (!strcmp(argv[i], "--from") || !strcmp(argv[i], "--to"))) {
            if (num(argv[i], argv[i + 1], !strcmp(argv[i], "--from") ? &mv_from : &mv_to) != 0)
                return 2;
            i++;
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc &&
            (!strcmp(argv[i], "--opening-hold-s") || !strcmp(argv[i], "--repl-retain-down-gb") ||
             !strcmp(argv[i], "--balance-mb-s"))) {
            char *end;
            unsigned long v = strtoul(argv[i + 1], &end, 10);
            unsigned long maxv = !strcmp(argv[i], "--opening-hold-s") ? 86400ul : 0x7ffffffful;
            if (!argv[i + 1][0] || *end || v > maxv) {
                fprintf(stderr, "tsdb cluster: %s %s: expected 0 to %lu\n", argv[i], argv[i + 1], maxv);
                return 2;
            }
            if (!strcmp(argv[i], "--opening-hold-s"))
                s.opening_hold_s = (uint32_t)v;
            else if (!strcmp(argv[i], "--repl-retain-down-gb"))
                s.repl_retain_down_gb = (uint32_t)v;
            else
                s.balance_mb_s = (uint32_t)v;
            i++;
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--leader-balance-s")) { /* 0 is off */
            char *end;
            unsigned long v = strtoul(argv[i + 1], &end, 10);
            if (!argv[i + 1][0] || *end || v > 86400) {
                fprintf(stderr, "tsdb cluster: --leader-balance-s %s: expected 0 (off) to 86400\n", argv[i + 1]);
                return 2;
            }
            s.leader_balance_s = (uint32_t)v;
            i++;
            continue;
        }
        if (!strcmp(verb, "volume") && newpeer && i + 1 < argc && !strcmp(argv[i], "--redundancy")) {
            const char *rv = argv[i + 1];
            char *end;
            if (!strncmp(rv, "replicas=", 9)) {
                if (num("--redundancy replicas=", rv + 9, &replicas) != 0)
                    return 2;
                ec_k = ec_m = 0;
            } else if (!strncmp(rv, "ec=", 3)) {
                unsigned long k = strtoul(rv + 3, &end, 10), m = 0;
                int ok = end != rv + 3 && *end == '+';
                if (ok) {
                    const char *ms = end + 1;
                    m = strtoul(ms, &end, 10);
                    ok = end != ms && !*end;
                }
                if (!ok || k < 1 || k > META_EC_K_MAX || m < 1 || m > META_EC_M_MAX) {
                    fprintf(stderr, "tsdb cluster: --redundancy %s: expected ec=K+M with K 1 to %d and M 1 to %d\n", rv,
                            META_EC_K_MAX, META_EC_M_MAX);
                    return 2;
                }
                ec_k = (uint32_t)k;
                ec_m = (uint32_t)m;
            } else {
                fprintf(stderr, "tsdb cluster: --redundancy %s: expected ec=K+M or replicas=N\n", rv);
                return 2;
            }
            i++;
            continue;
        }
        if (!strcmp(verb, "volume") && i + 1 < argc && !strcmp(argv[i], "--replicas")) {
            if (num(argv[i], argv[i + 1], &replicas) != 0)
                return 2;
            i++;
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--read-barrier")) {
            if (strcmp(argv[i + 1], "on") && strcmp(argv[i + 1], "off"))
                return cl_usage();
            s.read_barrier = !strcmp(argv[i + 1], "on");
            i++;
            continue;
        }
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--log-on-hdd")) {
            if (!strcmp(argv[i + 1], "on") || !strcmp(argv[i + 1], "1"))
                s.log_on_hdd = 1;
            else if (!strcmp(argv[i + 1], "off") || !strcmp(argv[i + 1], "0"))
                s.log_on_hdd = 0;
            else
                return cl_usage();
            i++;
            continue;
        }
        size_t k = 0;
        for (; k < sizeof nums / sizeof nums[0]; k++)
            if (!strcmp(verb, "init") && !strcmp(argv[i], nums[k].flag) && i + 1 < argc)
                break;
        if (k == sizeof nums / sizeof nums[0])
            return cl_usage();
        if (num(argv[i], argv[i + 1], nums[k].v) != 0)
            return 2;
        i++;
    }
    if (!strcmp(verb, "init") && (s.young_warn_pct > s.young_move_pct || s.young_move_pct >= s.young_high_pct)) {
        fprintf(stderr,
                "tsdb cluster: --young-warn-pct (%u) <= --young-move-pct (%u) < --young-high-pct (%u) required\n",
                s.young_warn_pct, s.young_move_pct, s.young_high_pct);
        return 2;
    }
    if (!secret_file) {
        fprintf(stderr, "tsdb cluster: --secret-file is required (the cluster's secret, as tsdbd's "
                        "--cluster-secret-file)\n");
        return 2;
    }
    if (r9op == CLUSTER_OP_NODE_HOLD) {
        int rel = (hold_flags & CLUSTER_HOLD_RELEASE) != 0;
        if (rel == have_hold_for) {
            fprintf(stderr, "tsdb cluster node hold: give exactly one of --for DUR or --release\n");
            return 2;
        }
    }
    if (r4op == CLUSTER_OP_VOLUME_MOVE && have_pos) {
        if (mv_flags || mv_from || !mv_to) {
            fprintf(stderr, "tsdb cluster volume move --pos: --pos P and --to NODE are required (no --log/--from)\n");
            return 2;
        }
        r4op = 0;
        r9op = CLUSTER_OP_VOLUME_MOVE_POS;
    } else if (r4op == CLUSTER_OP_VOLUME_MOVE &&
               (!(mv_flags & CLUSTER_MOVE_LOG) || !mv_from || !mv_to || mv_from == mv_to)) {
        fprintf(stderr, "tsdb cluster volume move: --log, --from NODE and --to NODE (another node) are required\n");
        return 2;
    }
    if (s.rebuild_max > 16 || s.transfer_wait_ms < 100 || s.transfer_wait_ms > 60000) {
        fprintf(stderr, "tsdb cluster: --rebuild-max is at most 16, --transfer-wait-ms 100 to 60000\n");
        return 2;
    }
    if (s.heartbeat_ms >= s.node_timeout_ms || s.min_isr > s.replication) {
        fprintf(stderr, "tsdb cluster: --heartbeat-ms must be below --node-timeout-ms, --min-isr at most "
                        "--replication\n");
        return 2;
    }
    /* the superuser's name and password are checked before INIT: an init cannot be run again, so a bad file found after
     * it would leave a group without a user */
    if (superuser && cluster_user_first_check(superuser, pwfile) != 0)
        return 2;
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t slen;
    char err[1024];
    if (cluster_read_secret(secret_file, secret, sizeof secret, &slen, err, sizeof err) != 0) {
        fprintf(stderr, "tsdb cluster: %s\n", err);
        return 1;
    }
    uint8_t req[512];
    size_t rl = 0;
    int op;
    if (!strcmp(verb, "init")) {
        op = CLUSTER_OP_INIT;
        rl = cluster_settings_encode(&s, mode, req);
    } else if (r9op) {
        op = r9op;
        if (op == CLUSTER_OP_NODE_HOLD)
            rl = cluster_node_hold_encode(r9_node, hold_for_s, hold_flags, req);
        else if (op == CLUSTER_OP_NODE_DRAIN)
            rl = cluster_node_drain_encode(r9_node, drain_flags, req);
        else if (op == CLUSTER_OP_NODE_REPLACE)
            rl = (repl_flags & CLUSTER_REPLACE_CANCEL) ? cluster_node_replace_cancel_encode(r9_node, req)
                                                       : cluster_node_replace_encode(r9_node, r9_node2, req);
        else if (op == CLUSTER_OP_VOLUME_MOVE_POS)
            rl = cluster_volume_move_pos_encode(r4_vol, mv_pos, mv_to, req);
        else
            rl = cluster_node_op_encode(r9_node, req);
    } else if (!strcmp(verb, "node")) {
        op = CLUSTER_OP_NODE_ADD;
        size_t n = strlen(newpeer);
        if (n >= META_ADDR_MAX || !strchr(newpeer, ':')) {
            fprintf(stderr, "tsdb cluster: %s: expected host:port\n", newpeer);
            return 2;
        }
        req[0] = (uint8_t)n;
        req[1] = (uint8_t)(n >> 8);
        memcpy(req + 2, newpeer, n);
        rl = 2 + n;
    } else if (r4op) {
        op = r4op;
        rl = op == CLUSTER_OP_VOLUME_MOVE ? cluster_volume_move_encode(r4_vol, mv_from, mv_to, mv_flags, req)
                                          : cluster_volume_force_encode(r4_vol, r4_node, req);
    } else if (gc_vol) {
        op = CLUSTER_OP_VOLUME_GC_RESUME;
        uint32_t gv = 0;
        if (gc_volume(gc_vol, addr, secret, slen, &gv) != 0)
            return 1;
        for (int j = 0; j < 4; j++)
            req[j] = (uint8_t)(gv >> (8 * j));
        rl = 4;
    } else if (force_vol) {
        op = CLUSTER_OP_VOLUME_FORCE;
        rl = cluster_volume_force_encode(force_vol, force_node, req);
    } else if (!strcmp(verb, "volume")) {
        op = ec_k ? CLUSTER_OP_VOLUME_ADD_EC : CLUSTER_OP_VOLUME_ADD;
        size_t n = strlen(newpeer);
        if (n == 0 || n >= META_NAME_MAX) {
            fprintf(stderr, "tsdb cluster: %s: a volume name is 1 to %d bytes\n", newpeer, META_NAME_MAX - 1);
            return 2;
        }
        rl = ec_k ? cluster_volume_add_ec_encode(newpeer, replicas, (uint8_t)ec_k, (uint8_t)ec_m, req)
                  : cluster_volume_add_encode(newpeer, replicas, req);
    } else {
        op = CLUSTER_OP_STATUS;
    }
    char target[META_ADDR_MAX + 1];
    snprintf(target, sizeof target, "%s", addr);
    /* a transfer is answered once its new leader is committed: the drain (transfer_wait_ms, at most 60 s), the READY's
     * slack (1 s) and the targeted promotion (2 s) before it, so 30 s is short of the longest setting */
    int wait_ms = op == CLUSTER_OP_VOLUME_TRANSFER ? 90000 : 30000;
    for (int hop = 0;; hop++) {
        int rc;
        uint8_t *reply = NULL;
        size_t len = 0;
        if (cluster_admin(target, secret, slen, op, req, rl, wait_ms, &rc, &reply, &len, err, sizeof err) != 0) {
            fprintf(stderr, "tsdb cluster: %s\n", err);
            return 1;
        }
        if (rc == CLUSTER_ERR_NOTLEADER && hop == 0 && len > 0 && len < sizeof target) {
            memcpy(target, reply, len); /* follow to the leader once */
            target[len] = 0;
            free(reply);
            continue;
        }
        if (rc != 0) {
            if ((op == CLUSTER_OP_VOLUME_FORCE || op == CLUSTER_OP_VOLUME_ADD || op == CLUSTER_OP_VOLUME_ADD_EC ||
                 op == CLUSTER_OP_VOLUME_GC_RESUME || r4op ||
                 r9op) && len) /* the leader's reason, e.g. how many members qualify for a stripe group */
                fprintf(stderr, "tsdb cluster %s %s: %.*s\n", verb, argv[2], (int)len, (const char *)reply);
            fprintf(stderr, "tsdb cluster %s: %s\n", verb, cluster_strerror(rc));
            free(reply);
            return 1;
        }
        if (op == CLUSTER_OP_STATUS) {
            struct cluster_status st;
            if (cluster_status_parse(reply, len, &st) != 0) {
                fprintf(stderr, "tsdb cluster status: the reply does not decode\n");
                free(reply);
                return 1;
            }
            print_status(&st);
            cluster_status_free(&st);
        } else if (op == CLUSTER_OP_VOLUME_ADD || op == CLUSTER_OP_VOLUME_ADD_EC) {
            uint32_t number = 0;
            for (size_t k = 0; k < 4 && k < len; k++)
                number |= (uint32_t)reply[k] << (8 * k);
            printf("added volume %s: number %" PRIu32 "\n", newpeer, number);
        } else if (op == CLUSTER_OP_VOLUME_FORCE || r4op || r9op) {
            printf("%.*s\n", (int)len, (const char *)reply);
        } else if (op == CLUSTER_OP_VOLUME_GC_RESUME) {
            printf("volume %s: orphan collection resumed\n", gc_vol);
        } else if (op == CLUSTER_OP_INIT) {
            printf("initialised: node 1 at %s\n", target);
            if (superuser) { /* the group's first user entries: the superuser, before anything else is asked */
                free(reply);
                if (cluster_user_add_first(target, secret, slen, superuser, pwfile) == 0)
                    return 0;
                fprintf(stderr, "tsdb cluster init: the cluster is initialised without its superuser; make it with "
                                "tsdb cluster user add %s %s --superuser --password-file F --secret-file F\n",
                        target, superuser);
                return 1;
            }
        } else {
            printf("added %s\n", newpeer);
        }
        free(reply);
        return 0;
    }
}
