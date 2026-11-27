/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * `tsdb cluster ...`: the admin ops of tsdbd's cluster thread (server/cluster.c), sent to a server's peer address.
 * No directory: the CLI holds no cluster state; it authenticates with the cluster secret (--secret-file) and prints
 * what the server answers.
 *
 * Structured as a verb table (`cl_verbs[]`: {words, min/max args, flags, encoder, printer, help}) with one shared
 * argument parser and one leader-following RPC loop (one hop on CLUSTER_ERR_NOTLEADER). Exit codes: 0 ok, 1 refused
 * or failed, 2 usage error.
 *
 * Status views (E44, §15.4, D24):
 *   tsdb cluster status [--json] PEER --secret-file F
 *   tsdb cluster status volumes [--json] PEER --secret-file F
 *   tsdb cluster status volume V [--json] PEER --secret-file F
 *   tsdb cluster status disks [--json] PEER --secret-file F
 */
#include "cluster.h"
#include "meta.h"
#include "place.h"
#include "raft/raft.h"

#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int cluster_cli(int argc, char **argv);                                      /* tools/tsdb-cli.c */
int cluster_user_cli(int argc, char **argv);                                 /* tools/cluster-user-cli.c */
int cluster_user_first_check(const char *name, const char *pwfile);          /* cluster-user-cli.c */
int cluster_user_add_first(const char *addr, const uint8_t *secret, size_t slen, const char *name, const char *pwfile);

/* ------------------------------------------------------------------ top-level help text */

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
    "       tsdb cluster status PEER [--json] --secret-file F\n"
    "       tsdb cluster status volumes [--json] PEER --secret-file F\n"
    "       tsdb cluster status volume V [--json] PEER --secret-file F\n"
    "       tsdb cluster status disks [--json] PEER --secret-file F\n"
    "       tsdb cluster user add|passwd|drop|grant|revoke|list|import PEER ... --secret-file F\n"
    "       tsdb cluster help [VERB]\n"
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
    "  status PEER         what that server knows: its role, the leader, quorum, settings, every member (up or\n"
    "                      down, incarnation, when it was last heard), health summary, volumes and users. Views:\n"
    "                      `status volumes`, `status volume V`, `status disks`. `--json` on each (`help status`).\n"
    "\n"
    "  user ...            users and grants of the whole cluster, kept by the group (`tsdb cluster user` says how).\n"
    "                      init --superuser NAME --password-file F makes the first superuser.\n"
    "  help [VERB]         show this overview, or detailed help (and JSON keys) for VERB.\n"
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

static const char help_secret[] =
    "usage: tsdb cluster secret FILE\n"
    "\n"
    "  Generate a new cluster secret (32 random bytes as hex, mode 0600) in FILE.\n"
    "  Never overwrites an existing file. Copy it securely to every server for tsdbd --cluster-secret-file.\n"
    "  Exit codes: 0 ok, 1 failed, 2 usage.\n";

static const char help_init[] =
    "usage: tsdb cluster init PEER --secret-file F [options]\n"
    "\n"
    "  Initialise a new cluster on PEER (a server not yet in any cluster). PEER becomes node 1 and Raft leader.\n"
    "  Optionally creates the first superuser with --superuser NAME --password-file F (password never in argv).\n"
    "  Run `tsdb cluster help` to list all init setting flags. Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_node_add[] =
    "usage: tsdb cluster node add PEER NEW_PEER --secret-file F\n"
    "\n"
    "  Add NEW_PEER (host:port) to the cluster that PEER belongs to; follows once to the Raft leader.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_node_hold[] =
    "usage: tsdb cluster node hold PEER NODE (--for DUR | --release) --secret-file F\n"
    "\n"
    "  Hold NODE against automatic OUT for DUR (e.g. 600s, 30m, 2h, 1d; max 1 day), or --release an active hold.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_node_out[] =
    "usage: tsdb cluster node out PEER NODE --secret-file F\n"
    "\n"
    "  Mark NODE out immediately so its log replicas and stripe positions rebuild elsewhere.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_node_drain[] =
    "usage: tsdb cluster node drain PEER NODE [--cancel] --secret-file F\n"
    "\n"
    "  Mark NODE draining to move every log replica and stripe position off it before automatic OUT.\n"
    "  --cancel returns a live draining node to UP. Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_node_replace[] =
    "usage: tsdb cluster node replace PEER OLD (NEW | --cancel) --secret-file F\n"
    "\n"
    "  Move every log replica and stripe position of OLD directly onto NEW, holding OLD until empty.\n"
    "  --cancel clears an active replace and releases OLD's hold. Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_node_destroyed[] =
    "usage: tsdb cluster node destroyed PEER NODE --secret-file F\n"
    "\n"
    "  Declare an OUT node permanently destroyed (irreversible; > M destroyed stripe holders mark an EC volume LOST).\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_volume_add[] =
    "usage: tsdb cluster volume add PEER NAME [--replicas N | --redundancy ec=K+M|replicas=N] --secret-file F\n"
    "\n"
    "  Create a replicated or erasure-coded volume named NAME. K is 1..16, M is 1..4.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_volume_gc_resume[] =
    "usage: tsdb cluster volume gc resume PEER VOLUME --secret-file F\n"
    "\n"
    "  Resume orphan chunk collection on erasure-coded VOLUME (name or number) after force-leader or restore.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_volume_force_leader[] =
    "usage: tsdb cluster volume force-leader PEER VOLUME NODE --secret-file F\n"
    "\n"
    "  Unclean failover: when no ISR member of VOLUME is UP, promote replica NODE to leader.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_volume_transfer[] =
    "usage: tsdb cluster volume transfer PEER VOLUME NODE --secret-file F\n"
    "\n"
    "  Planned leadership transfer of VOLUME to in-sync replica NODE.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_volume_move[] =
    "usage: tsdb cluster volume move PEER VOLUME --log --from A --to B [--cancel] --secret-file F\n"
    "       tsdb cluster volume move PEER VOLUME --pos P --to B --secret-file F\n"
    "\n"
    "  Move a log replica (--log --from A --to B [--cancel]) or an EC stripe position (--pos P --to B).\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_replica_reset[] =
    "usage: tsdb cluster replica reset PEER VOLUME NODE --secret-file F\n"
    "\n"
    "  Give non-leader, non-ISR replica NODE of VOLUME a fresh identity and rebuild it from a snapshot.\n"
    "  Exit codes: 0 ok, 1 refused/failed, 2 usage.\n";

static const char help_status[] =
    "usage: tsdb cluster status PEER [--json] --secret-file F\n"
    "       tsdb cluster status volumes [--json] PEER --secret-file F\n"
    "       tsdb cluster status volume V [--json] PEER --secret-file F\n"
    "       tsdb cluster status disks [--json] PEER --secret-file F\n"
    "\n"
    "  Views (`--json` may appear before or after the view/peer):\n"
    "    status            summary: node, cluster, quorum, settings, guard, health, members, volumes/users counts\n"
    "    status volumes    per-volume table: leader, epoch, ISR, young/settled margins, health, rebuild, settle\n"
    "    status volume V   detailed view of one volume (V is a volume name or number)\n"
    "    status disks      per-class disk capacity & marks table, or \"no disks reported\"\n"
    "\n"
    "  Notes:\n"
    "    young_margin      ISR size minus 1 (null for shared volumes).\n"
    "    settled_margin    EC only: whole live stripe positions minus k (excluding filling positions, taking the\n"
    "                      minimum across any active or cancelled re-encode geometry; null for non-EC volumes).\n"
    "    disk usage fields retired_bytes, index_bytes, and days_to_mark are reserved (currently null / \"-\").\n"
    "\n"
    "  Stable --json keys (numbers are JSON numbers, booleans are JSON booleans):\n"
    "    status --json:\n"
    "      top-level: view, node, cluster, quorum, settings, guard, health, members,\n"
    "                 volumes_count, users_count, transfers, moves\n"
    "      node:      id, mode, role, stale, term, leader, commit, applied, users_caught_up\n"
    "      quorum:    has_quorum, raft_leader, term, voters, learners, voters_total, voters_up, quorum_needed\n"
    "      health:    status, members_total, members_up, members_down, members_draining, members_out,\n"
    "                 members_destroyed, volumes_total, volumes_ok, volumes_degraded,\n"
    "                 volumes_unavailable, volumes_lost, warnings\n"
    "      members[]: id, state, incarnation, addr, peer, rack, power_feed, heard_ms, voter_ok, volumes_ok,\n"
    "                 core_dropped, read_barrier, role, capability, disks, down_reason, down_age_s,\n"
    "                 out_in_s, hold_left_s, replaced_by\n"
    "    status volumes --json:\n"
    "      top-level: view, cluster_uuid, leader, term, applied, volumes\n"
    "    status volume V --json:\n"
    "      top-level: view, cluster_uuid, leader, term, applied, volume\n"
    "      volume:    number, name, uuid, kind, leader, preferred, epoch, min_isr, isr, isr_version,\n"
    "                 set_version, redundancy, ec_k, ec_m, gc_suspended, mixed_class, rack_survivable,\n"
    "                 young_margin, settled_margin, settled_state, health, warnings, replicas, stripe,\n"
    "                 reports, rebuild, settle_backlog, young_scrub_age_s\n"
    "    status disks --json:\n"
    "      top-level: view, reported, chunk_fill_mark_pct, classes, disks, warnings\n"
    "      classes[]: class, disks, disks_up, disks_down, disks_out, disks_destroyed, size_bytes,\n"
    "                 young_bytes, retired_bytes, index_bytes, chunk_bytes, free_bytes, marks,\n"
    "                 young_warn_pct, young_move_pct, young_high_pct, chunk_mark_pct, days_to_mark\n"
    "      disks[]:   node, store_id, class, uses, state, size_bytes, sync_us, young_bytes, chunk_bytes\n"
    "\n"
    "  Exit codes: 0 ok, 1 refused/failed (e.g. unknown volume or connection error), 2 usage.\n";

static const char help_status_volumes[] =
    "usage: tsdb cluster status volumes [--json] PEER --secret-file F\n"
    "\n"
    "  Show every volume's leader, epoch, ISR, young and settled margins, health, rebuild progress and settle backlog.\n"
    "  --json top-level keys: view, cluster_uuid, leader, term, applied, volumes.\n"
    "  Each volumes[] object has: number, name, uuid, kind, leader, preferred, epoch, min_isr, isr, isr_version,\n"
    "  set_version, redundancy, ec_k, ec_m, gc_suspended, mixed_class, rack_survivable, young_margin,\n"
    "  settled_margin, settled_state, health, warnings, replicas, stripe, reports, rebuild, settle_backlog,\n"
    "  young_scrub_age_s. Exit codes: 0 ok, 1 failed, 2 usage.\n";

static const char help_status_volume[] =
    "usage: tsdb cluster status volume V [--json] PEER --secret-file F\n"
    "\n"
    "  Show detailed status for volume V (exact volume name, else numeric volume number).\n"
    "  --json top-level keys: view, cluster_uuid, leader, term, applied, volume.\n"
    "  The volume object has: number, name, uuid, kind, leader, preferred, epoch, min_isr, isr, isr_version,\n"
    "  set_version, redundancy, ec_k, ec_m, gc_suspended, mixed_class, rack_survivable, young_margin,\n"
    "  settled_margin, settled_state, health, warnings, replicas, stripe, reports, rebuild, settle_backlog,\n"
    "  young_scrub_age_s. Exit codes: 0 ok, 1 unknown volume or failed, 2 usage.\n";

static const char help_status_disks[] =
    "usage: tsdb cluster status disks [--json] PEER --secret-file F\n"
    "\n"
    "  Show per-class disk capacity, usage by tier and marks, or \"no disks reported\" if none.\n"
    "  --json top-level keys: view, reported, chunk_fill_mark_pct, classes, disks, warnings.\n"
    "  classes[] keys: class, disks, disks_up, disks_down, disks_out, disks_destroyed, size_bytes, young_bytes,\n"
    "  retired_bytes, index_bytes, chunk_bytes, free_bytes, marks, young_warn_pct, young_move_pct,\n"
    "  young_high_pct, chunk_mark_pct, days_to_mark (retired_bytes, index_bytes, days_to_mark are reserved/null).\n"
    "  disks[] keys: node, store_id, class, uses, state, size_bytes, sync_us, young_bytes, chunk_bytes.\n"
    "  Exit codes: 0 ok, 1 failed, 2 usage.\n";

static const char help_user[] =
    "usage: tsdb cluster user add|passwd|drop|grant|revoke|list|import PEER ... --secret-file F\n"
    "\n"
    "  Manage cluster-wide users and grants kept in the metadata group. Run `tsdb cluster user` for subcommand usage.\n";

static int cl_usage(void)
{
    fputs(cl_help, stderr);
    return 2;
}

/* ------------------------------------------------------------------ parsing & formatting helpers */

static int num(const char *flag, const char *s, uint32_t *out)
{
    char *end;
    if (!s || !*s || !isdigit((unsigned char)*s)) {
        fprintf(stderr, "tsdb cluster: %s %s: expected a whole number above 0\n", flag, s ? s : "");
        return -1;
    }
    unsigned long v = strtoul(s, &end, 10);
    if (*end || v == 0 || v > 0x7fffffff) {
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
    if (!s || !*s || !isdigit((unsigned char)*s)) {
        fprintf(stderr, "tsdb cluster: %s %s: expected a duration (e.g. 600s, 10m, 1h)\n", flag, s ? s : "");
        return -1;
    }
    unsigned long v = strtoul(s, &end, 10);
    unsigned long mult = 1;
    if (end == s) {
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

static void hex16(const uint8_t in[16], char out[33])
{
    static const char hx[] = "0123456789abcdef";
    for (int i = 0; i < 16; i++) {
        out[2 * i] = hx[in[i] >> 4];
        out[2 * i + 1] = hx[in[i] & 0xf];
    }
    out[32] = '\0';
}

static int parse_hex16(const char *s, uint8_t out[16])
{
    if (!s || strlen(s) != 32)
        return -1;
    for (int i = 0; i < 16; i++) {
        unsigned hi, lo;
        char c0 = s[2 * i], c1 = s[2 * i + 1];
        if (c0 >= '0' && c0 <= '9')
            hi = (unsigned)(c0 - '0');
        else if (c0 >= 'a' && c0 <= 'f')
            hi = (unsigned)(c0 - 'a' + 10);
        else if (c0 >= 'A' && c0 <= 'F')
            hi = (unsigned)(c0 - 'A' + 10);
        else
            return -1;
        if (c1 >= '0' && c1 <= '9')
            lo = (unsigned)(c1 - '0');
        else if (c1 >= 'a' && c1 <= 'f')
            lo = (unsigned)(c1 - 'a' + 10);
        else if (c1 >= 'A' && c1 <= 'F')
            lo = (unsigned)(c1 - 'A' + 10);
        else
            return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
}

static void json_str(FILE *f, const char *s)
{
    fputc('"', f);
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        switch (*p) {
        case '"': fputs("\\\"", f); break;
        case '\\': fputs("\\\\", f); break;
        case '\b': fputs("\\b", f); break;
        case '\f': fputs("\\f", f); break;
        case '\n': fputs("\\n", f); break;
        case '\r': fputs("\\r", f); break;
        case '\t': fputs("\\t", f); break;
        default:
            if (*p < 0x20)
                fprintf(f, "\\u%04x", (unsigned)*p);
            else
                fputc(*p, f);
            break;
        }
    }
    fputc('"', f);
}

static void json_str_or_null(FILE *f, const char *s)
{
    if (!s || !*s)
        fputs("null", f);
    else
        json_str(f, s);
}

static void fmt_bytes(uint64_t b, char *buf, size_t cap)
{
    if (b == 0) {
        snprintf(buf, cap, "0");
        return;
    }
    if (b < 1000) {
        snprintf(buf, cap, "%u B", (unsigned)b);
        return;
    }
    static const char *const units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    double v = (double)b;
    int u = 0;
    while (v >= 1000.0 && u < 5) {
        v /= 1000.0;
        u++;
    }
    if (v >= 100.0)
        snprintf(buf, cap, "%.0f %s", v, units[u]);
    else if (v >= 10.0)
        snprintf(buf, cap, "%.1f %s", v, units[u]);
    else
        snprintf(buf, cap, "%.2f %s", v, units[u]);
}

static const char *role_name(int r)
{
    static const char *const n[] = {"follower", "pre-candidate", "candidate", "leader"};
    return r >= 0 && r < 4 ? n[r] : "?";
}

static const char *vol_report_role_name(uint8_t r)
{
    return r == CL_ROLE_OWNER       ? "owner"
           : r == CL_ROLE_RELEASING ? "releasing"
           : r == CL_ROLE_PROMOTING ? "promoting"
                                    : "none";
}

static const char *vol_report_health_name(uint8_t h)
{
    return h == CL_HEALTH_OK             ? "ok"
           : h == CL_HEALTH_LOCAL_FAULT  ? "local-fault"
           : h == CL_HEALTH_DOMAIN_FAULT ? "domain-fault"
           : h == CL_HEALTH_DAMAGED      ? "damaged"
                                         : "unknown";
}

static uint64_t status_add_sat(uint64_t a, uint64_t b)
{
    return b > UINT64_MAX - a ? UINT64_MAX : a + b;
}

static uint64_t status_now_ms(const struct cluster_status *st)
{
    if (st->wall_ms)
        return st->wall_ms;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0)
        return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
    return 0;
}

static const struct cluster_heard *find_heard(const struct cluster_status *st, uint32_t id)
{
    for (uint32_t j = 0; j < st->nheard; j++)
        if (st->heard[j].id == id)
            return &st->heard[j];
    return NULL;
}

/* ------------------------------------------------------------------ shared status analysis */

struct cl_quorum_info {
    uint32_t raft_leader;
    uint64_t term;
    int has_quorum;
    uint32_t nvoters, nlearners;
    uint32_t voters[64], learners[64];
    uint32_t voters_up;
    uint32_t quorum_needed;
};

static void compute_quorum(const struct cluster_status *st, const struct meta_state *m, int have_meta,
                           struct cl_quorum_info *out)
{
    memset(out, 0, sizeof *out);
    out->raft_leader = st->leader;
    out->term = st->term;
    if (!have_meta || !m->config_len)
        return;
    raft_config cfg = {0};
    if (!raft_config_decode(m->config, m->config_len, &cfg))
        return;
    for (uint32_t i = 0; i < cfg.n; i++) {
        if (cfg.m[i].learner) {
            if (out->nlearners < 64)
                out->learners[out->nlearners++] = cfg.m[i].id;
        } else {
            if (out->nvoters < 64)
                out->voters[out->nvoters++] = cfg.m[i].id;
            const struct meta_member *mb = meta_member(m, cfg.m[i].id);
            if (mb && (mb->state == META_UP || mb->state == META_DRAINING))
                out->voters_up++;
        }
    }
    out->quorum_needed = out->nvoters ? (out->nvoters / 2u) + 1u : 0u;
    out->has_quorum = (!st->stale && st->leader != 0 && out->nvoters > 0 && out->voters_up >= out->quorum_needed);
}

struct cl_vol_view {
    int young_margin;
    int young_na;
    int settled_margin;
    int settled_na;
    const char *settled_state; /* "none", "ok", "unavailable", "lost" */
    const char *health;        /* "ok", "degraded", "damaged", "unavailable", "lost" */
    int mixed_class;
    int rack_survivable;
    int rebuild_active;
    int rebuild_stalled;
    uint64_t rebuild_bytes_done;
    uint64_t rebuild_bytes_total;
    uint32_t nrebuild_entries;
    uint32_t nfills_active;
    uint32_t settle_keys_waiting;
    uint64_t settle_oldest_wait_s;
    uint64_t young_scrub_age_s;
    uint32_t nwarnings;
    const char *warnings[8];
};

static void add_warn(struct cl_vol_view *vv, const char *w)
{
    for (uint32_t i = 0; i < vv->nwarnings; i++)
        if (strcmp(vv->warnings[i], w) == 0)
            return;
    if (vv->nwarnings < 8)
        vv->warnings[vv->nwarnings++] = w;
}

static void compute_vol_view(const struct cluster_status *st, const struct meta_state *m, const struct meta_volume *v,
                             struct cl_vol_view *out)
{
    memset(out, 0, sizeof *out);
    if (v->kind == META_VOL_SHARED) {
        out->young_na = 1;
        out->young_margin = 0;
    } else {
        out->young_margin = (int)v->nisr - 1;
    }
    out->rack_survivable = place_rack_survivable(m, v);
    int any_rack = 0;
    for (uint32_t i = 0; i < m->nmembers && !any_rack; i++)
        if (m->members[i].rack[0])
            any_rack = 1;
    if (any_rack && v->kind != META_VOL_SHARED && !out->rack_survivable)
        add_warn(out, "rack_not_survivable");

    uint32_t destroyed = 0;
    int stripe_degraded = 0;
    if (v->redundancy == META_RED_EC && v->nstripe > 0) {
        int sm = 0;
        meta_vol_margins(m, v, NULL, NULL, &sm);
        out->settled_margin = sm;
        out->settled_na = 0;
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
            const struct meta_member *mb = meta_member(m, v->stripe[p]);
            if (mb && mb->state == META_DESTROYED)
                destroyed++;
            if (v->stripe_pos[p].state == META_STRIPE_FILLING) {
                out->nfills_active++;
                stripe_degraded = 1;
            }
        }
        if (destroyed > v->ec_m)
            out->settled_state = "lost";
        else if (out->settled_margin < 0)
            out->settled_state = "unavailable";
        else
            out->settled_state = "ok";
        if (out->settled_margin < (int)v->ec_m)
            stripe_degraded = 1;
        out->mixed_class = place_stripe_mixed_class(m, v);
        if (out->mixed_class)
            add_warn(out, "mixed_stripe_class");
        if (v->gc_suspended)
            add_warn(out, "gc_suspended");
    } else {
        out->settled_na = 1;
        out->settled_state = "none";
    }

    int any_damaged = 0, any_fault = 0;
    int have_leader_settle = 0;
    for (uint32_t i = 0; i < st->nheard; i++) {
        const struct cluster_heard *h = &st->heard[i];
        for (uint32_t j = 0; j < h->nvols; j++) {
            const struct cluster_vol_report *vr = &h->vols[j].v;
            if (vr->volume != v->number)
                continue;
            if (vr->health == CL_HEALTH_DAMAGED)
                any_damaged = 1;
            else if (vr->health == CL_HEALTH_LOCAL_FAULT || vr->health == CL_HEALTH_DOMAIN_FAULT)
                any_fault = 1;
        }
        for (uint32_t j = 0; j < h->nprogress_vols; j++) {
            const struct creport_vol_progress *vp = &h->progress_vols[j];
            if (vp->volume != v->number)
                continue;
            if (vp->role != CREPORT_REBUILD_NONE) {
                out->rebuild_active = 1;
                if (vp->stalled)
                    out->rebuild_stalled = 1;
                out->rebuild_bytes_done = status_add_sat(out->rebuild_bytes_done, vp->bytes_done);
                out->rebuild_bytes_total = status_add_sat(out->rebuild_bytes_total, vp->bytes_total);
                out->nrebuild_entries++;
            }
            if (h->id == v->leader) {
                out->settle_keys_waiting = vp->settle_keys_waiting;
                out->settle_oldest_wait_s = vp->settle_oldest_wait_s;
                have_leader_settle = 1;
            } else if (!have_leader_settle) {
                if (vp->settle_keys_waiting > out->settle_keys_waiting)
                    out->settle_keys_waiting = vp->settle_keys_waiting;
                if (vp->settle_oldest_wait_s > out->settle_oldest_wait_s)
                    out->settle_oldest_wait_s = vp->settle_oldest_wait_s;
            }
            if (vp->young_scrub_age_s > out->young_scrub_age_s)
                out->young_scrub_age_s = vp->young_scrub_age_s;
        }
    }
    if (out->nfills_active > 0)
        out->rebuild_active = 1;
    if (out->rebuild_stalled)
        add_warn(out, "rebuild_stalled");
    if (any_damaged)
        add_warn(out, "replica_damaged");
    if (any_fault)
        add_warn(out, "replica_fault");

    const struct meta_member *lmb = v->leader ? meta_member(m, v->leader) : NULL;
    int leader_live = lmb && (lmb->state == META_UP || lmb->state == META_DRAINING);
    int young_unav = !leader_live || (v->kind != META_VOL_SHARED && (v->nisr == 0 || v->nisr < v->min_isr));
    uint32_t nfull_rep = 0;
    for (uint32_t k = 0; k < v->nrep; k++)
        if (!v->rep[k].learner)
            nfull_rep++;
    int isr_degraded = (v->kind != META_VOL_SHARED && v->nisr < nfull_rep);
    if (isr_degraded)
        add_warn(out, "isr_degraded");
    if (stripe_degraded)
        add_warn(out, "stripe_degraded");

    if (strcmp(out->settled_state, "lost") == 0)
        out->health = "lost";
    else if (young_unav || strcmp(out->settled_state, "unavailable") == 0)
        out->health = "unavailable";
    else if (any_damaged)
        out->health = "damaged";
    else if (any_fault || isr_degraded || stripe_degraded || v->gc_suspended || out->mixed_class)
        out->health = "degraded";
    else
        out->health = "ok";
}

/* ---- Disks collection & §15.4 per-class aggregation ---- */

struct cl_disk_item {
    uint32_t node;
    uint8_t store_id[16];
    uint8_t disk_class; /* enum meta_disk_class */
    uint8_t uses;       /* enum meta_disk_uses */
    uint8_t state;      /* enum meta_store_state */
    uint8_t from_meta;  /* 1 when registered in metadata (authoritative for class/uses/state) */
    uint8_t have_usage;
    uint64_t size_bytes;
    uint64_t sync_us;
    uint64_t young_bytes;
    uint64_t chunk_bytes;
};

struct cl_disk_class_agg {
    uint8_t disk_class;
    uint32_t disks;
    uint32_t disks_up, disks_down, disks_out, disks_destroyed;
    uint64_t size_bytes;
    int have_usage;
    int has_log_use;
    int has_chunk_use;
    uint64_t young_bytes;
    uint64_t chunk_bytes;
    uint64_t free_bytes;
};

struct cl_disk_summary {
    struct cl_disk_item *items;
    size_t nitems, cap_items;
    char **warnings;
    size_t nwarnings, cap_warnings;
    struct cl_disk_class_agg cls[4]; /* indexed by META_DISK_NVME(1)..META_DISK_HDD(3) */
    uint32_t chunk_fill_mark_pct;
};

static void disk_summary_free(struct cl_disk_summary *ds)
{
    free(ds->items);
    for (size_t i = 0; i < ds->nwarnings; i++)
        free(ds->warnings[i]);
    free(ds->warnings);
    memset(ds, 0, sizeof *ds);
}

static void disk_summary_add_warn(struct cl_disk_summary *ds, const char *msg)
{
    for (size_t i = 0; i < ds->nwarnings; i++)
        if (strcmp(ds->warnings[i], msg) == 0)
            return;
    if (ds->nwarnings == ds->cap_warnings) {
        size_t nc = ds->cap_warnings ? ds->cap_warnings * 2 : 8;
        char **p = realloc(ds->warnings, nc * sizeof *p);
        if (!p)
            return;
        ds->warnings = p;
        ds->cap_warnings = nc;
    }
    char *copy = strdup(msg);
    if (copy)
        ds->warnings[ds->nwarnings++] = copy;
}

static struct cl_disk_item *disk_summary_upsert(struct cl_disk_summary *ds, uint32_t node, const uint8_t sid[16])
{
    for (size_t i = 0; i < ds->nitems; i++)
        if (ds->items[i].node == node && memcmp(ds->items[i].store_id, sid, 16) == 0)
            return &ds->items[i];
    if (ds->nitems == ds->cap_items) {
        size_t nc = ds->cap_items ? ds->cap_items * 2 : 16;
        struct cl_disk_item *p = realloc(ds->items, nc * sizeof *p);
        if (!p)
            return NULL;
        ds->items = p;
        ds->cap_items = nc;
    }
    struct cl_disk_item *it = &ds->items[ds->nitems++];
    memset(it, 0, sizeof *it);
    it->node = node;
    memcpy(it->store_id, sid, 16);
    return it;
}

static uint8_t parse_disk_class_name(const char *s)
{
    if (!strcmp(s, "nvme"))
        return META_DISK_NVME;
    if (!strcmp(s, "ssd"))
        return META_DISK_SSD;
    if (!strcmp(s, "hdd"))
        return META_DISK_HDD;
    return META_DISK_NONE;
}

static uint8_t parse_store_state_name(const char *s)
{
    if (!strcmp(s, "up"))
        return META_STORE_UP;
    if (!strcmp(s, "down"))
        return META_STORE_DOWN;
    if (!strcmp(s, "out"))
        return META_STORE_OUT;
    if (!strcmp(s, "destroyed"))
        return META_STORE_DESTROYED;
    return 0xffu; /* unknown state: never treated as UP */
}

static uint8_t parse_disk_uses_name(const char *s)
{
    if (!strcmp(s, "log,chunk"))
        return META_DISK_USE_ALL;
    if (!strcmp(s, "log"))
        return META_DISK_USE_LOG;
    if (!strcmp(s, "chunk"))
        return META_DISK_USE_CHUNK;
    return 0;
}

/* Parse HBS_DISKS formatted section lines emitted by server/disks.c:disks_sec_format(). */
static void parse_disk_sections(struct cl_disk_summary *ds, const char *sections)
{
    if (!sections || !*sections)
        return;
    const char *p = sections;
    while (*p) {
        const char *nl = strchr(p, '\n');
        size_t l = nl ? (size_t)(nl - p) : strlen(p);
        char line[512];
        if (l >= sizeof line)
            l = sizeof line - 1;
        memcpy(line, p, l);
        line[l] = '\0';
        p = nl ? nl + 1 : p + strlen(p);

        if (strncmp(line, "disks node=", 11) == 0) {
            uint32_t nid = 0, cnt = 0;
            if (sscanf(line, "disks node=%" SCNu32 " warn=allow_shared_device", &nid) == 1) {
                char w[128];
                snprintf(w, sizeof w, "node %" PRIu32 ": allow_shared_device", nid);
                disk_summary_add_warn(ds, w);
            } else if (sscanf(line, "disks node=%" SCNu32 " warn=class_mismatch count=%" SCNu32, &nid, &cnt) == 2) {
                char w[128];
                snprintf(w, sizeof w, "node %" PRIu32 ": class_mismatch (%" PRIu32 ")", nid, cnt);
                disk_summary_add_warn(ds, w);
            }
        } else if (strncmp(line, "disk node=", 10) == 0) {
            uint32_t nid = 0;
            char hex[40] = "", cls[16] = "", uses[24] = "", state[24] = "";
            uint64_t sz = 0, sus = 0, yb = 0, cb = 0;
            int n = sscanf(line,
                           "disk node=%" SCNu32 " id=%39s class=%15s uses=%23s state=%23s size=%" SCNu64
                           " sync_us=%" SCNu64 " young_bytes=%" SCNu64 " chunk_bytes=%" SCNu64,
                           &nid, hex, cls, uses, state, &sz, &sus, &yb, &cb);
            if (n < 7)
                continue;
            uint8_t sid[16];
            if (parse_hex16(hex, sid) != 0)
                continue;
            uint8_t dc = parse_disk_class_name(cls);
            if (dc < META_DISK_NVME || dc > META_DISK_CLASS_LAST)
                continue;
            uint8_t stv = parse_store_state_name(state);
            struct cl_disk_item *it = disk_summary_upsert(ds, nid, sid);
            if (!it)
                continue;
            /* Metadata is authoritative for registered disks (especially OUT/DESTROYED); heartbeats only supply
             * usage bytes for registered disks and initial fields for not-yet-committed disks. */
            if (!it->from_meta) {
                if (stv == 0xffu) {
                    /* Discard unrecognized unregistered disk row rather than treating it as UP. */
                    if (ds->nitems > 0 && &ds->items[ds->nitems - 1] == it)
                        ds->nitems--;
                    continue;
                }
                it->disk_class = dc;
                uint8_t u = parse_disk_uses_name(uses);
                if (u)
                    it->uses = u;
                it->state = stv;
                if (sz > 0)
                    it->size_bytes = sz;
                it->sync_us = sus;
            } else {
                if (it->size_bytes == 0 && sz > 0)
                    it->size_bytes = sz;
                if (it->sync_us == 0)
                    it->sync_us = sus;
            }
            if (n == 9) {
                it->have_usage = 1;
                it->young_bytes = yb;
                it->chunk_bytes = cb;
            }
        }
    }
}

/* Cluster-wide chunk store fill mark in % per §5.6 / E53 (matching server/place.c:chunk_fill_mark_ppm). */
static uint32_t compute_chunk_fill_mark_pct(const struct meta_state *m, int have_meta)
{
    if (!have_meta || m->nmembers == 0)
        return 90;
    int all_racked = 1;
    uint64_t total_cap = 0;
    uint32_t nstorage = 0;
    for (uint32_t i = 0; i < m->nmembers; i++) {
        const struct meta_member *mb = &m->members[i];
        if (mb->state >= META_OUT || mb->role == META_ROLE_COMPUTE || mb->ndisks == 0)
            continue;
        uint64_t mcap = 0;
        for (uint32_t d = 0; d < mb->ndisks && d < META_DISKS_MAX; d++) {
            const struct meta_disk *dk = &mb->disks[d];
            if (dk->state == META_STORE_UP && (dk->uses & META_DISK_USE_CHUNK))
                mcap = status_add_sat(mcap, dk->size_bytes);
        }
        if (mcap == 0)
            continue;
        nstorage++;
        total_cap = status_add_sat(total_cap, mcap);
        if (!mb->rack[0])
            all_racked = 0;
    }
    if (total_cap == 0 || nstorage <= 1)
        return 90;
    uint64_t max_dom = 0;
    for (uint32_t i = 0; i < m->nmembers; i++) {
        const struct meta_member *mb = &m->members[i];
        if (mb->state >= META_OUT || mb->role == META_ROLE_COMPUTE || mb->ndisks == 0)
            continue;
        uint64_t dcap = 0;
        if (all_racked) {
            int seen = 0;
            for (uint32_t j = 0; j < i; j++) {
                if (m->members[j].state < META_OUT && m->members[j].role != META_ROLE_COMPUTE &&
                    m->members[j].ndisks > 0 && !strcmp(m->members[j].rack, mb->rack)) {
                    seen = 1;
                    break;
                }
            }
            if (seen)
                continue;
            for (uint32_t j = i; j < m->nmembers; j++) {
                const struct meta_member *mj = &m->members[j];
                if (mj->state >= META_OUT || mj->role == META_ROLE_COMPUTE || mj->ndisks == 0 ||
                    strcmp(mj->rack, mb->rack) != 0)
                    continue;
                for (uint32_t d = 0; d < mj->ndisks && d < META_DISKS_MAX; d++) {
                    const struct meta_disk *dk = &mj->disks[d];
                    if (dk->state == META_STORE_UP && (dk->uses & META_DISK_USE_CHUNK))
                        dcap = status_add_sat(dcap, dk->size_bytes);
                }
            }
        } else {
            for (uint32_t d = 0; d < mb->ndisks && d < META_DISKS_MAX; d++) {
                const struct meta_disk *dk = &mb->disks[d];
                if (dk->state == META_STORE_UP && (dk->uses & META_DISK_USE_CHUNK))
                    dcap = status_add_sat(dcap, dk->size_bytes);
            }
        }
        if (dcap > max_dom)
            max_dom = dcap;
    }
    if (max_dom >= total_cap)
        return 90;
    uint64_t ppm = (uint64_t)(((__uint128_t)950000u * (total_cap - max_dom)) / total_cap);
    uint32_t pct = (uint32_t)(ppm / 10000u);
    return pct > 95 ? 95 : pct;
}

static void collect_disks(const struct cluster_status *st, const struct meta_state *m, int have_meta,
                          struct cl_disk_summary *ds)
{
    memset(ds, 0, sizeof *ds);
    if (have_meta) {
        for (uint32_t i = 0; i < m->nmembers; i++) {
            const struct meta_member *mb = &m->members[i];
            for (uint32_t j = 0; j < mb->ndisks && j < META_DISKS_MAX; j++) {
                const struct meta_disk *d = &mb->disks[j];
                if (d->disk_class < META_DISK_NVME || d->disk_class > META_DISK_CLASS_LAST)
                    continue;
                struct cl_disk_item *it = disk_summary_upsert(ds, mb->id, d->store_id);
                if (!it)
                    continue;
                it->from_meta = 1;
                it->disk_class = d->disk_class;
                it->uses = d->uses;
                it->state = d->state;
                it->size_bytes = d->size_bytes;
                it->sync_us = d->sync_us;
            }
        }
    }
    for (uint32_t i = 0; i < st->nheard; i++)
        parse_disk_sections(ds, st->heard[i].sections);

    for (int c = META_DISK_NVME; c <= META_DISK_HDD; c++)
        ds->cls[c].disk_class = (uint8_t)c;

    for (size_t i = 0; i < ds->nitems; i++) {
        const struct cl_disk_item *it = &ds->items[i];
        if (it->disk_class < META_DISK_NVME || it->disk_class > META_DISK_HDD)
            continue;
        struct cl_disk_class_agg *a = &ds->cls[it->disk_class];
        a->disks++;
        if (it->state == META_STORE_UP)
            a->disks_up++;
        else if (it->state == META_STORE_DOWN)
            a->disks_down++;
        else if (it->state == META_STORE_OUT)
            a->disks_out++;
        else if (it->state == META_STORE_DESTROYED)
            a->disks_destroyed++;
        if (it->uses & META_DISK_USE_LOG)
            a->has_log_use = 1;
        if (it->uses & META_DISK_USE_CHUNK)
            a->has_chunk_use = 1;
        if (it->state != META_STORE_DESTROYED)
            a->size_bytes = status_add_sat(a->size_bytes, it->size_bytes);
        if (it->have_usage) {
            a->have_usage = 1;
            a->young_bytes = status_add_sat(a->young_bytes, it->young_bytes);
            a->chunk_bytes = status_add_sat(a->chunk_bytes, it->chunk_bytes);
        }
    }
    for (int c = META_DISK_NVME; c <= META_DISK_HDD; c++) {
        struct cl_disk_class_agg *a = &ds->cls[c];
        if (a->have_usage) {
            uint64_t used = status_add_sat(a->young_bytes, a->chunk_bytes);
            a->free_bytes = a->size_bytes > used ? a->size_bytes - used : 0;
        }
    }
    ds->chunk_fill_mark_pct = compute_chunk_fill_mark_pct(m, have_meta);
}

/* Shared cluster health summary used by both text and --json summary printers so they never diverge. */
struct cl_health_summary {
    const char *status;
    uint32_t m_up, m_down, m_drain, m_out, m_dest;
    uint32_t v_ok, v_deg, v_unav, v_lost;
    struct cl_disk_summary ds; /* carries deduplicated warnings[] */
};

static void compute_health_summary(const struct cluster_status *st, const struct meta_state *m, int have_meta,
                                   const struct cl_quorum_info *qi, const struct pout_guard *g,
                                   struct cl_health_summary *out)
{
    memset(out, 0, sizeof *out);
    collect_disks(st, m, have_meta, &out->ds);
    if (have_meta) {
        for (uint32_t i = 0; i < m->nmembers; i++) {
            switch (m->members[i].state) {
            case META_UP: out->m_up++; break;
            case META_DOWN: out->m_down++; break;
            case META_DRAINING: out->m_drain++; break;
            case META_OUT: out->m_out++; break;
            case META_DESTROYED: out->m_dest++; break;
            default: break;
            }
        }
        for (uint32_t i = 0; i < m->nvolumes; i++) {
            const struct meta_volume *v = &m->volumes[i];
            struct cl_vol_view vv;
            compute_vol_view(st, m, v, &vv);
            if (!strcmp(vv.health, "lost"))
                out->v_lost++;
            else if (!strcmp(vv.health, "unavailable"))
                out->v_unav++;
            else if (!strcmp(vv.health, "degraded") || !strcmp(vv.health, "damaged"))
                out->v_deg++;
            else
                out->v_ok++;
            for (uint32_t wi = 0; wi < vv.nwarnings; wi++) {
                char w[192];
                snprintf(w, sizeof w, "%s volume=%" PRIu32 " name=%s", vv.warnings[wi], v->number, v->name);
                disk_summary_add_warn(&out->ds, w);
            }
        }
    }
    if (g && g->active)
        disk_summary_add_warn(&out->ds, "mass_out_guard_active");
    if (have_meta && m->initialised && !qi->has_quorum)
        disk_summary_add_warn(&out->ds, "no_quorum");

    out->status = (!have_meta || !m->initialised)                              ? "uninitialised"
                  : (!qi->has_quorum || out->v_lost > 0 || out->v_unav > 0)    ? "critical"
                  : (out->v_deg > 0 || out->m_down > 0)                        ? "degraded"
                  : (out->ds.nwarnings > 0)                                    ? "warn"
                                                                               : "ok";
}

static void health_summary_free(struct cl_health_summary *hs)
{
    disk_summary_free(&hs->ds);
}

/* ------------------------------------------------------------------ status view printers */

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

/* Print a single volume's JSON object (shared between `status volumes --json` and `status volume V --json`). */
static void print_volume_json(const struct cluster_status *st, const struct meta_state *m, const struct meta_volume *v,
                              const char *indent)
{
    struct cl_vol_view vv;
    compute_vol_view(st, m, v, &vv);
    char vu[33];
    hex16(v->uuid, vu);
    char red[32];
    if (v->redundancy == META_RED_EC)
        snprintf(red, sizeof red, "ec=%u+%u", v->ec_k, v->ec_m);
    else
        snprintf(red, sizeof red, "replicas=%" PRIu32, v->nrep);

    printf("%s{\n", indent);
    printf("%s  \"number\": %" PRIu32 ",\n", indent, v->number);
    printf("%s  \"name\": ", indent);
    json_str(stdout, v->name);
    printf(",\n%s  \"uuid\": \"%s\",\n", indent, vu);
    printf("%s  \"kind\": \"%s\",\n", indent, v->kind == META_VOL_SHARED ? "shared" : "replicated");
    printf("%s  \"leader\": %" PRIu32 ",\n", indent, v->leader);
    printf("%s  \"preferred\": %" PRIu32 ",\n", indent, v->nrep ? v->rep[0].node : 0u);
    printf("%s  \"epoch\": %" PRIu64 ",\n", indent, v->epoch);
    printf("%s  \"min_isr\": %" PRIu32 ",\n", indent, v->min_isr);
    printf("%s  \"isr\": [", indent);
    for (uint32_t k = 0; k < v->nisr; k++)
        printf("%s%" PRIu32, k ? ", " : "", v->isr[k]);
    printf("],\n");
    printf("%s  \"isr_version\": %" PRIu64 ",\n", indent, v->isr_version);
    printf("%s  \"set_version\": %" PRIu64 ",\n", indent, v->set_version);
    printf("%s  \"redundancy\": \"%s\",\n", indent, red);
    printf("%s  \"ec_k\": %u,\n", indent, (unsigned)(v->redundancy == META_RED_EC ? v->ec_k : 0));
    printf("%s  \"ec_m\": %u,\n", indent, (unsigned)(v->redundancy == META_RED_EC ? v->ec_m : 0));
    printf("%s  \"gc_suspended\": %s,\n", indent, v->gc_suspended ? "true" : "false");
    printf("%s  \"mixed_class\": %s,\n", indent, vv.mixed_class ? "true" : "false");
    printf("%s  \"rack_survivable\": %s,\n", indent, vv.rack_survivable ? "true" : "false");
    if (vv.young_na)
        printf("%s  \"young_margin\": null,\n", indent);
    else
        printf("%s  \"young_margin\": %d,\n", indent, vv.young_margin);
    if (vv.settled_na)
        printf("%s  \"settled_margin\": null,\n", indent);
    else
        printf("%s  \"settled_margin\": %d,\n", indent, vv.settled_margin);
    printf("%s  \"settled_state\": \"%s\",\n", indent, vv.settled_state);
    printf("%s  \"health\": \"%s\",\n", indent, vv.health);

    printf("%s  \"warnings\": [", indent);
    for (uint32_t k = 0; k < vv.nwarnings; k++) {
        if (k)
            fputs(", ", stdout);
        json_str(stdout, vv.warnings[k]);
    }
    printf("],\n");

    printf("%s  \"replicas\": [", indent);
    for (uint32_t k = 0; k < v->nrep; k++) {
        printf("%s{\"node\": %" PRIu32 ", \"learner\": %s, \"rid\": \"%016" PRIx64 "\", \"open_gen\": %" PRIu64
               ", \"replaces\": %" PRIu32 "}",
               k ? ", " : "", v->rep[k].node, v->rep[k].learner ? "true" : "false", v->rep[k].rid, v->rep[k].open_gen,
               v->rep[k].replaces);
    }
    printf("],\n");

    printf("%s  \"stripe\": [", indent);
    if (v->redundancy == META_RED_EC) {
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
            printf("%s{\"pos\": %" PRIu32 ", \"node\": %" PRIu32 ", \"state\": \"%s\", \"prev\": %" PRIu32
                   ", \"set_ver\": %" PRIu64 "}",
                   p ? ", " : "", p, v->stripe[p], meta_stripe_state_name(v->stripe_pos[p].state),
                   v->stripe_pos[p].prev, v->stripe_pos[p].set_ver);
        }
    }
    printf("],\n");

    printf("%s  \"reports\": [", indent);
    int nrep_out = 0;
    for (uint32_t i = 0; i < st->nheard; i++) {
        const struct cluster_heard *h = &st->heard[i];
        for (uint32_t j = 0; j < h->nvols; j++) {
            const struct cluster_vol_status *vs = &h->vols[j];
            if (vs->v.volume != v->number)
                continue;
            printf("%s{\"node\": %" PRIu32 ", \"role\": \"%s\", \"health\": \"%s\", \"fault_reason\": \"%s\", "
                   "\"epoch\": %" PRIu64 ", \"domain\": ",
                   nrep_out++ ? ", " : "", h->id, vol_report_role_name(vs->v.role),
                   vol_report_health_name(vs->v.health), cluster_fault_reason_name(vs->v.fault_reason), vs->v.epoch);
            json_str_or_null(stdout, vs->v.domain);
            printf(", \"local_fault_s\": %" PRIu64 ", \"fenced_age_ms\": %" PRIu64 "}", vs->local_fault_s,
                   vs->v.fenced_age_ms);
        }
    }
    printf("],\n");

    printf("%s  \"rebuild\": {\"active\": %s, \"stalled\": %s, \"bytes_done\": %" PRIu64 ", \"bytes_total\": %" PRIu64
           ", \"entries\": [",
           indent, vv.rebuild_active ? "true" : "false", vv.rebuild_stalled ? "true" : "false", vv.rebuild_bytes_done,
           vv.rebuild_bytes_total);
    int ne = 0;
    for (uint32_t i = 0; i < st->nheard; i++) {
        const struct cluster_heard *h = &st->heard[i];
        for (uint32_t j = 0; j < h->nprogress_vols; j++) {
            const struct creport_vol_progress *vp = &h->progress_vols[j];
            if (vp->volume != v->number || vp->role == CREPORT_REBUILD_NONE)
                continue;
            printf("%s{\"node\": %" PRIu32 ", \"role\": \"%s\", \"bytes_done\": %" PRIu64 ", \"bytes_total\": %" PRIu64
                   ", \"started_ms\": %" PRIu64 ", \"last_progress_ms\": %" PRIu64 ", \"stalled\": %s}",
                   ne++ ? ", " : "", h->id, creport_rebuild_role_name(vp->role), vp->bytes_done, vp->bytes_total,
                   vp->started_ms, vp->last_progress_ms, vp->stalled ? "true" : "false");
        }
    }
    printf("], \"fills\": [");
    int nf = 0;
    if (v->redundancy == META_RED_EC) {
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
            if (v->stripe_pos[p].state != META_STRIPE_FILLING)
                continue;
            const struct cluster_fill_report *fr = NULL;
            for (uint32_t i = 0; i < st->nheard && !fr; i++) {
                if (st->heard[i].id != v->stripe[p])
                    continue;
                for (uint32_t fi = 0; fi < st->heard[i].nfills; fi++) {
                    const struct cluster_fill_report *c = &st->heard[i].fills[fi];
                    if (c->volume == v->number && c->pos == p && c->version == v->stripe_version) {
                        fr = c;
                        break;
                    }
                }
            }
            printf("%s{\"pos\": %" PRIu32 ", \"node\": %" PRIu32 ", \"prev\": %" PRIu32 ", \"version\": %" PRIu64
                   ", \"done\": %" PRIu64 ", \"total\": %" PRIu64 ", \"whole\": %s}",
                   nf++ ? ", " : "", p, v->stripe[p], v->stripe_pos[p].prev, v->stripe_version, fr ? fr->done : 0,
                   fr ? fr->total : 0, (fr && fr->whole) ? "true" : "false");
        }
    }
    printf("]},\n");

    printf("%s  \"settle_backlog\": {\"keys_waiting\": %" PRIu32 ", \"oldest_wait_s\": %" PRIu64 "},\n", indent,
           vv.settle_keys_waiting, vv.settle_oldest_wait_s);
    printf("%s  \"young_scrub_age_s\": %" PRIu64 "\n", indent, vv.young_scrub_age_s);
    printf("%s}", indent);
}

/* View 1: `tsdb cluster status [--json] PEER` */
static void print_status_summary_text(const struct cluster_status *st)
{
    static const char *const mode[] = {"none", "core", "watch"};
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
    uint64_t now_ms = status_now_ms(st);
    char u[33];
    hex16(m.uuid, u);
    if (m.initialised)
        printf("cluster %s replication %" PRIu32 " min_isr %" PRIu32 " node_timeout_ms %" PRIu32
               " heartbeat_ms %" PRIu32 " voter_replace_s %" PRIu32 " voters_max %" PRIu32 " mode %s failover %s"
               " read_barrier %s lease_s %" PRIu32 " restore_gen %" PRIu64 "\n",
               u, m.set.replication, m.set.min_isr, m.set.node_timeout_ms, m.set.heartbeat_ms, m.set.voter_replace_s,
               m.set.voters, m.mode == META_MODE_SHARED ? "shared" : "replicated",
               m.set.failover == META_FAILOVER_BMC        ? "bmc"
               : m.set.failover == META_FAILOVER_ATTESTED ? "attested"
                                                          : "lease",
               m.set.read_barrier ? "on" : "off", m.set.lease_s, m.restore_gen);
    if (m.initialised) {
        printf("replicated lease_ms %" PRIu32 " drift_ppm %" PRIu32 " unanswered_ms %" PRIu32
               " commit_interval_ms %" PRIu32 " repl_retain_mb %" PRIu32 " repl_retain_s %" PRIu32
               " idle_seal_ms %" PRIu32 " rebuild_mb_s %" PRIu32 " rebuild_max %" PRIu32 " transfer_wait_ms %" PRIu32
               " leader_balance_s %" PRIu32 "\n",
               m.set.lease_ms, m.set.drift_ppm, m.set.unanswered_ms, m.set.commit_interval_ms, m.set.repl_retain_mb,
               m.set.repl_retain_s, m.set.idle_seal_ms, m.set.rebuild_mb_s, m.set.rebuild_max, m.set.transfer_wait_ms,
               m.set.leader_balance_s);
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

    struct cl_quorum_info qi;
    compute_quorum(st, &m, 1, &qi);
    printf("group voters");
    for (uint32_t i = 0; i < qi.nvoters; i++)
        printf("%s%" PRIu32, i ? "," : " ", qi.voters[i]);
    printf("%s learners", qi.nvoters ? "" : " -");
    for (uint32_t i = 0; i < qi.nlearners; i++)
        printf("%s%" PRIu32, i ? "," : " ", qi.learners[i]);
    printf("%s\n", qi.nlearners ? "" : " -");
    struct pout_guard g = {0};
    if (m.initialised) {
        printf("quorum %s voters_up %" PRIu32 "/%" PRIu32 " needed %" PRIu32 "\n", qi.has_quorum ? "yes" : "no",
               qi.voters_up, qi.nvoters, qi.quorum_needed);
        pout_guard_eval(&m, m.set.out_guard_pct, m.set.out_guard_hold_s, now_ms, &g);
        status_print_guard(&m, now_ms);
    }

    for (uint32_t i = 0; i < m.nmembers; i++) {
        const struct meta_member *mb = &m.members[i];
        char heard[48] = "never";
        const struct cluster_heard *h = find_heard(st, mb->id);
        if (h && h->ago_ms != UINT64_MAX)
            snprintf(heard, sizeof heard, "%" PRIu64 "ms", h->ago_ms);
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
            hex16(d->store_id, sid);
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
                   mb->id, v->v.volume, vol_report_role_name(v->v.role), vol_report_health_name(v->v.health),
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
        struct cl_vol_view vv;
        compute_vol_view(st, &m, v, &vv);

        char vu[33];
        hex16(v->uuid, vu);
        printf("volume %" PRIu32 " name %s uuid %s leader %" PRIu32 " epoch %" PRIu64 " min_isr %" PRIu32 " replicas",
               v->number, v->name, vu, v->leader, v->epoch, v->min_isr);
        for (uint32_t k = 0; k < v->nrep; k++)
            printf("%s%" PRIu32 "%s", k ? "," : " ", v->rep[k].node, v->rep[k].learner ? "(learner)" : "");
        printf(" isr");
        for (uint32_t k = 0; k < v->nisr; k++)
            printf("%s%" PRIu32, k ? "," : " ", v->isr[k]);
        printf("%s isr_version %" PRIu64 " set_version %" PRIu64, v->nisr ? "" : " -", v->isr_version, v->set_version);
        if (v->nrep) /* leader balance (D15) moves the leadership back to rep[0] */
            printf(" preferred %" PRIu32, v->rep[0].node);
        if (v->redundancy == META_RED_EC) { /* R8: the stripe group by position (E9) and the orphan collection (E14) */
            printf(" redundancy ec=%u+%u stripe", v->ec_k, v->ec_m);
            for (uint32_t k = 0; k < v->nstripe && k < META_STRIPE_MAX; k++)
                printf("%s%" PRIu32, k ? "," : " ", v->stripe[k]);
            printf(" stripe_version %" PRIu64 " gc_suspended %u", v->stripe_version, v->gc_suspended);
            if (vv.mixed_class)
                printf(" mixed_class 1");
        } else {
            printf(" redundancy replicas=%" PRIu32, v->nrep);
        }
        printf(" rack loss survivable: %s\n", vv.rack_survivable ? "yes" : "no");
        if (v->redundancy == META_RED_EC && vv.mixed_class)
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
    if (m.initialised) {
        struct cl_health_summary hs;
        compute_health_summary(st, &m, 1, &qi, &g, &hs);
        printf("health summary %s volumes_ok %" PRIu32 "/%" PRIu32 " degraded %" PRIu32 " unavailable %" PRIu32
               " lost %" PRIu32 " warnings %zu\n",
               hs.status, hs.v_ok, m.nvolumes, hs.v_deg, hs.v_unav, hs.v_lost, hs.ds.nwarnings);
        health_summary_free(&hs);
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

static void print_status_summary_json(const struct cluster_status *st)
{
    static const char *const mode[] = {"none", "core", "watch"};
    struct meta_state m;
    meta_init(&m);
    int have_meta = st->image_len && meta_snapshot_decode(&m, st->image, st->image_len) == META_OK;
    uint64_t now_ms = status_now_ms(st);

    struct cl_quorum_info qi;
    compute_quorum(st, &m, have_meta, &qi);

    struct pout_guard g = {0};
    if (have_meta && m.initialised)
        pout_guard_eval(&m, m.set.out_guard_pct, m.set.out_guard_hold_s, now_ms, &g);

    struct cl_health_summary hs;
    compute_health_summary(st, &m, have_meta, &qi, &g, &hs);

    fputs("{\n", stdout);
    fputs("  \"view\": \"summary\",\n", stdout);
    printf("  \"node\": {\"id\": %" PRIu32 ", \"mode\": \"%s\", \"role\": ", st->self,
           st->mode < 3 ? mode[st->mode] : "?");
    if (st->mode == CLUSTER_MODE_CORE)
        json_str(stdout, role_name(st->role));
    else
        fputs("null", stdout);
    printf(", \"stale\": %s, \"term\": %" PRIu64 ", \"leader\": %" PRIu32 ", \"commit\": %" PRIu64
           ", \"applied\": %" PRIu64 ", \"users_caught_up\": %s},\n",
           st->stale ? "true" : "false", st->term, st->leader, st->commit, st->applied,
           st->users_caught_up ? "true" : "false");

    if (have_meta) {
        char u[33];
        hex16(m.uuid, u);
        printf("  \"cluster\": {\"uuid\": \"%s\", \"initialised\": %s, \"mode\": \"%s\", \"restore_gen\": %" PRIu64
               "},\n",
               u, m.initialised ? "true" : "false", m.mode == META_MODE_SHARED ? "shared" : "replicated",
               m.restore_gen);
    } else {
        fputs("  \"cluster\": null,\n", stdout);
    }

    printf("  \"quorum\": {\"has_quorum\": %s, \"raft_leader\": %" PRIu32 ", \"term\": %" PRIu64 ", \"voters\": [",
           qi.has_quorum ? "true" : "false", qi.raft_leader, qi.term);
    for (uint32_t i = 0; i < qi.nvoters; i++)
        printf("%s%" PRIu32, i ? ", " : "", qi.voters[i]);
    fputs("], \"learners\": [", stdout);
    for (uint32_t i = 0; i < qi.nlearners; i++)
        printf("%s%" PRIu32, i ? ", " : "", qi.learners[i]);
    printf("], \"voters_total\": %" PRIu32 ", \"voters_up\": %" PRIu32 ", \"quorum_needed\": %" PRIu32 "},\n",
           qi.nvoters, qi.voters_up, qi.quorum_needed);

    if (have_meta && m.initialised) {
        const char *fo = m.set.failover == META_FAILOVER_BMC        ? "bmc"
                         : m.set.failover == META_FAILOVER_ATTESTED ? "attested"
                                                                    : "lease";
        printf("  \"settings\": {\n"
               "    \"replication\": %" PRIu32 ", \"min_isr\": %" PRIu32 ", \"node_timeout_ms\": %" PRIu32 ",\n"
               "    \"heartbeat_ms\": %" PRIu32 ", \"isr_lag_ms\": %" PRIu32 ", \"rebuild_after_s\": %" PRIu32 ",\n"
               "    \"voter_replace_s\": %" PRIu32 ", \"voters_max\": %" PRIu32 ", \"volumes_per_server\": %" PRIu32
               ",\n"
               "    \"failover\": \"%s\", \"read_barrier\": %s, \"lease_s\": %" PRIu32 ",\n"
               "    \"lease_ms\": %" PRIu32 ", \"drift_ppm\": %" PRIu32 ", \"unanswered_ms\": %" PRIu32 ",\n"
               "    \"commit_interval_ms\": %" PRIu32 ", \"repl_retain_mb\": %" PRIu32 ", \"repl_retain_s\": %" PRIu32
               ",\n"
               "    \"idle_seal_ms\": %" PRIu32 ", \"rebuild_mb_s\": %" PRIu32 ", \"rebuild_max\": %" PRIu32 ",\n"
               "    \"transfer_wait_ms\": %" PRIu32 ", \"leader_balance_s\": %" PRIu32 ", \"opening_hold_s\": %" PRIu32
               ",\n"
               "    \"failstop_out_s\": %" PRIu32 ", \"out_guard_pct\": %" PRIu32 ", \"out_guard_hold_s\": %" PRIu32
               ",\n"
               "    \"repl_retain_down_gb\": %" PRIu32 ", \"young_high_pct\": %" PRIu32 ", \"balance_mb_s\": %" PRIu32
               ",\n"
               "    \"rebuild_concurrency\": %" PRIu32 ", \"scrub_days\": %" PRIu32 ", \"scrub_mb_s\": %" PRIu32 ",\n"
               "    \"scrub_out_count\": %" PRIu32 ", \"log_on_hdd\": %s, \"young_warn_pct\": %" PRIu32 ",\n"
               "    \"young_move_pct\": %" PRIu32 ", \"disk_stall_s\": %" PRIu32 "\n"
               "  },\n",
               m.set.replication, m.set.min_isr, m.set.node_timeout_ms, m.set.heartbeat_ms, m.set.isr_lag_ms,
               m.set.rebuild_after_s, m.set.voter_replace_s, m.set.voters, m.set.volumes_per_server, fo,
               m.set.read_barrier ? "true" : "false", m.set.lease_s, m.set.lease_ms, m.set.drift_ppm,
               m.set.unanswered_ms, m.set.commit_interval_ms, m.set.repl_retain_mb, m.set.repl_retain_s,
               m.set.idle_seal_ms, m.set.rebuild_mb_s, m.set.rebuild_max, m.set.transfer_wait_ms,
               m.set.leader_balance_s, m.set.opening_hold_s, m.set.failstop_out_s, m.set.out_guard_pct,
               m.set.out_guard_hold_s, m.set.repl_retain_down_gb, m.set.young_high_pct, m.set.balance_mb_s,
               m.set.rebuild_concurrency, m.set.scrub_days, m.set.scrub_mb_s, m.set.scrub_out_count,
               m.set.log_on_hdd ? "true" : "false", m.set.young_warn_pct, m.set.young_move_pct, m.set.disk_stall_s);
    } else {
        fputs("  \"settings\": null,\n", stdout);
    }

    uint64_t g_since = (g.active && now_ms > g.start_ms) ? (now_ms - g.start_ms) / 1000ull : 0;
    uint64_t g_left = (g.active && g.due_ms > now_ms) ? (g.due_ms - now_ms + 999ull) / 1000ull : 0;
    const char *g_why = !g.active                              ? "none"
                        : (g.pct_triggered && g.rack_triggered) ? "pct+rack"
                        : g.pct_triggered                      ? "pct"
                                                               : "rack";
    printf("  \"guard\": {\"active\": %s, \"reason\": \"%s\", \"down\": %" PRIu32 ", \"non_out\": %" PRIu32
           ", \"since_s\": %" PRIu64 ", \"bound_s\": %" PRIu32 ", \"left_s\": %" PRIu64 "},\n",
           g.active ? "true" : "false", g_why, g.ndown, g.non_out, g_since,
           (have_meta && m.initialised) ? m.set.out_guard_hold_s : 0u, g_left);

    printf("  \"health\": {\"status\": \"%s\", \"members_total\": %" PRIu32 ", \"members_up\": %" PRIu32
           ", \"members_down\": %" PRIu32 ", \"members_draining\": %" PRIu32 ", \"members_out\": %" PRIu32
           ", \"members_destroyed\": %" PRIu32 ", \"volumes_total\": %" PRIu32 ", \"volumes_ok\": %" PRIu32
           ", \"volumes_degraded\": %" PRIu32 ", \"volumes_unavailable\": %" PRIu32 ", \"volumes_lost\": %" PRIu32
           ", \"warnings\": [",
           hs.status, have_meta ? m.nmembers : 0u, hs.m_up, hs.m_down, hs.m_drain, hs.m_out, hs.m_dest,
           have_meta ? m.nvolumes : 0u, hs.v_ok, hs.v_deg, hs.v_unav, hs.v_lost);
    for (size_t i = 0; i < hs.ds.nwarnings; i++) {
        if (i)
            fputs(", ", stdout);
        json_str(stdout, hs.ds.warnings[i]);
    }
    fputs("]},\n", stdout);

    fputs("  \"members\": [", stdout);
    if (have_meta && m.nmembers > 0) {
        fputc('\n', stdout);
        for (uint32_t i = 0; i < m.nmembers; i++) {
            const struct meta_member *mb = &m.members[i];
            const struct cluster_heard *h = find_heard(st, mb->id);
            printf("    {\"id\": %" PRIu32 ", \"state\": \"%s\", \"incarnation\": %" PRIu64 ", \"addr\": ", mb->id,
                   meta_member_state_name(mb->state), mb->incarnation);
            json_str(stdout, mb->addr);
            fputs(", \"peer\": ", stdout);
            json_str(stdout, mb->peer);
            fputs(", \"rack\": ", stdout);
            json_str_or_null(stdout, mb->rack);
            fputs(", \"power_feed\": ", stdout);
            json_str_or_null(stdout, mb->power_feed);
            fputs(", \"heard_ms\": ", stdout);
            if (h && h->ago_ms != UINT64_MAX)
                printf("%" PRIu64, h->ago_ms);
            else
                fputs("null", stdout);
            printf(", \"voter_ok\": %s, \"volumes_ok\": %s, \"core_dropped\": %s, \"read_barrier\": ",
                   (mb->flags & META_MF_VOTER_OK) ? "true" : "false",
                   (mb->flags & META_MF_VOLUMES_OK) ? "true" : "false", (h && h->core_dropped) ? "true" : "false");
            if (!h || (!h->flags && !h->state_free))
                fputs("null", stdout);
            else
                fputs(h->read_barrier ? "true" : "false", stdout);
            printf(", \"role\": \"%s\", \"capability\": \"%s\", \"disks\": %" PRIu32, meta_role_name(mb->role),
                   meta_cap_name(mb->capability), mb->ndisks);
            if (mb->state == META_DOWN && mb->down_since_ms) {
                uint64_t age_s = now_ms > mb->down_since_ms ? (now_ms - mb->down_since_ms) / 1000ull : 0;
                uint32_t base_s =
                    mb->down_reason == META_DOWN_FAILSTOP ? m.set.failstop_out_s : m.set.rebuild_after_s;
                uint64_t due_ms = status_add_sat(mb->down_since_ms, (uint64_t)base_s * 1000ull);
                if (h && h->opening && h->ago_ms != UINT64_MAX &&
                    (m.set.node_timeout_ms == 0 || h->ago_ms < m.set.node_timeout_ms))
                    due_ms = status_add_sat(due_ms, (uint64_t)m.set.opening_hold_s * 1000ull);
                if (mb->hold_until_ms > due_ms)
                    due_ms = mb->hold_until_ms;
                uint64_t out_in_s = due_ms > now_ms ? (due_ms - now_ms + 999ull) / 1000ull : 0;
                printf(", \"down_reason\": \"%s\", \"down_age_s\": %" PRIu64 ", \"out_in_s\": %" PRIu64,
                       meta_down_reason_name(mb->down_reason), age_s, out_in_s);
            } else {
                fputs(", \"down_reason\": null, \"down_age_s\": null, \"out_in_s\": null", stdout);
            }
            if (mb->hold_until_ms > now_ms)
                printf(", \"hold_left_s\": %" PRIu64,
                       (uint64_t)((mb->hold_until_ms - now_ms + UINT64_C(999)) / UINT64_C(1000)));
            else
                fputs(", \"hold_left_s\": null", stdout);
            if (mb->replaced_by)
                printf(", \"replaced_by\": %" PRIu32, mb->replaced_by);
            else
                fputs(", \"replaced_by\": null", stdout);
            printf("}%s\n", i + 1 < m.nmembers ? "," : "");
        }
        fputs("  ],\n", stdout);
    } else {
        fputs("],\n", stdout);
    }

    printf("  \"volumes_count\": %" PRIu32 ",\n", have_meta ? m.nvolumes : 0u);
    printf("  \"users_count\": %" PRIu32 ",\n", have_meta ? m.nusers : 0u);
    if (st->ntr >= CL_TR_N) {
        printf("  \"transfers\": {\"started\": %" PRIu64 ", \"done\": %" PRIu64 ", \"timed_out\": %" PRIu64
               ", \"cancelled\": %" PRIu64 ", \"fell_back\": %" PRIu64 ", \"balance\": %" PRIu64
               ", \"drains_timed_out\": %" PRIu64 ", \"resumed_blind\": %" PRIu64 ", \"ready\": %" PRIu64
               ", \"cuts\": %" PRIu64 "},\n",
               st->tr[CL_TR_STARTED], st->tr[CL_TR_DONE], st->tr[CL_TR_TIMEDOUT], st->tr[CL_TR_CANCELLED],
               st->tr[CL_TR_FALLBACK], st->tr[CL_TR_BALANCE], st->tr[CL_TR_DRAIN_TIMEDOUT], st->tr[CL_TR_BLIND],
               st->tr[CL_TR_READY], st->tr[CL_TR_CUTS]);
        printf("  \"moves\": {\"started\": %" PRIu64 ", \"done\": %" PRIu64 ", \"cancelled\": %" PRIu64 "}\n",
               st->tr[CL_TR_MV_STARTED], st->tr[CL_TR_MV_DONE], st->tr[CL_TR_MV_CANCELLED]);
    } else {
        fputs("  \"transfers\": null,\n", stdout);
        fputs("  \"moves\": null\n", stdout);
    }
    fputs("}\n", stdout);

    health_summary_free(&hs);
    meta_free(&m);
}

/* View 2: `tsdb cluster status volumes [--json] PEER` */
static int print_status_volumes(const struct cluster_status *st, int json)
{
    struct meta_state m;
    meta_init(&m);
    int have_meta = st->image_len && meta_snapshot_decode(&m, st->image, st->image_len) == META_OK;

    if (json) {
        char u[33] = "";
        if (have_meta)
            hex16(m.uuid, u);
        fputs("{\n", stdout);
        fputs("  \"view\": \"volumes\",\n", stdout);
        fputs("  \"cluster_uuid\": ", stdout);
        json_str_or_null(stdout, have_meta ? u : NULL);
        printf(",\n  \"leader\": %" PRIu32 ",\n", st->leader);
        printf("  \"term\": %" PRIu64 ",\n", st->term);
        printf("  \"applied\": %" PRIu64 ",\n", st->applied);
        fputs("  \"volumes\": [", stdout);
        if (have_meta && m.nvolumes > 0) {
            fputc('\n', stdout);
            for (uint32_t i = 0; i < m.nvolumes; i++) {
                print_volume_json(st, &m, &m.volumes[i], "    ");
                printf("%s\n", i + 1 < m.nvolumes ? "," : "");
            }
            fputs("  ]\n", stdout);
        } else {
            fputs("]\n", stdout);
        }
        fputs("}\n", stdout);
        meta_free(&m);
        return 0;
    }

    if (!have_meta || m.nvolumes == 0) {
        puts("no volumes");
        meta_free(&m);
        return 0;
    }

    printf("%-4s %-14s %-11s %-6s %-5s %-11s %-8s %-11s %-18s %s\n", "vol", "name", "red", "leader", "epoch", "isr",
           "margins", "health", "rebuild", "settle");
    for (uint32_t i = 0; i < m.nvolumes; i++) {
        const struct meta_volume *v = &m.volumes[i];
        struct cl_vol_view vv;
        compute_vol_view(st, &m, v, &vv);

        char red[24], lbuf[16], isr[64], marg[24], rbuf[64], sbuf[32];
        if (v->redundancy == META_RED_EC)
            snprintf(red, sizeof red, "ec=%u+%u", v->ec_k, v->ec_m);
        else
            snprintf(red, sizeof red, "replicas=%" PRIu32, v->nrep);
        if (v->leader)
            snprintf(lbuf, sizeof lbuf, "%" PRIu32, v->leader);
        else
            snprintf(lbuf, sizeof lbuf, "-");

        size_t off = 0;
        if (v->nisr == 0) {
            snprintf(isr, sizeof isr, "-");
        } else {
            isr[0] = '\0';
            for (uint32_t k = 0; k < v->nisr && off + 1 < sizeof isr; k++) {
                int n = snprintf(isr + off, sizeof isr - off, "%s%" PRIu32, k ? "," : "", v->isr[k]);
                if (n > 0)
                    off += (size_t)n < sizeof isr - off ? (size_t)n : sizeof isr - off - 1;
            }
        }
        if (vv.young_na && vv.settled_na)
            snprintf(marg, sizeof marg, "-/-");
        else if (vv.young_na)
            snprintf(marg, sizeof marg, "-/%d", vv.settled_margin);
        else if (vv.settled_na)
            snprintf(marg, sizeof marg, "%d/-", vv.young_margin);
        else
            snprintf(marg, sizeof marg, "%d/%d", vv.young_margin, vv.settled_margin);

        if (!vv.rebuild_active) {
            snprintf(rbuf, sizeof rbuf, "-");
        } else if (vv.nrebuild_entries > 0) {
            char b1[20], b2[20];
            fmt_bytes(vv.rebuild_bytes_done, b1, sizeof b1);
            fmt_bytes(vv.rebuild_bytes_total, b2, sizeof b2);
            snprintf(rbuf, sizeof rbuf, "%s/%s%s", b1, b2, vv.rebuild_stalled ? "(stalled)" : "");
        } else {
            snprintf(rbuf, sizeof rbuf, "fill:%" PRIu32, vv.nfills_active);
        }

        if (vv.settle_keys_waiting == 0)
            snprintf(sbuf, sizeof sbuf, "0");
        else
            snprintf(sbuf, sizeof sbuf, "%" PRIu32 "(%" PRIu64 "s)", vv.settle_keys_waiting, vv.settle_oldest_wait_s);

        printf("%-4" PRIu32 " %-14s %-11s %-6s %-5" PRIu64 " %-11s %-8s %-11s %-18s %s\n", v->number, v->name, red,
               lbuf, v->epoch, isr, marg, vv.health, rbuf, sbuf);
    }
    meta_free(&m);
    return 0;
}

/* Resolve volume V by exact name first, then numeric volume number. */
static const struct meta_volume *find_vol_arg(const struct meta_state *m, const char *arg)
{
    for (uint32_t i = 0; i < m->nvolumes; i++)
        if (strcmp(m->volumes[i].name, arg) == 0)
            return &m->volumes[i];
    if (isdigit((unsigned char)arg[0])) {
        char *end;
        unsigned long v = strtoul(arg, &end, 10);
        if (!*end && v > 0 && v <= UINT32_MAX)
            return meta_volume(m, (uint32_t)v);
    }
    return NULL;
}

/* View 3: `tsdb cluster status volume V [--json] PEER` */
static int print_status_volume(const struct cluster_status *st, const char *vol_arg, int json)
{
    struct meta_state m;
    meta_init(&m);
    int have_meta = st->image_len && meta_snapshot_decode(&m, st->image, st->image_len) == META_OK;
    const struct meta_volume *v = have_meta ? find_vol_arg(&m, vol_arg) : NULL;
    if (!v) {
        fprintf(stderr, "tsdb cluster status volume: no such volume: %s\n", vol_arg);
        meta_free(&m);
        return 1;
    }

    if (json) {
        char u[33];
        hex16(m.uuid, u);
        fputs("{\n", stdout);
        fputs("  \"view\": \"volume\",\n", stdout);
        printf("  \"cluster_uuid\": \"%s\",\n", u);
        printf("  \"leader\": %" PRIu32 ",\n", st->leader);
        printf("  \"term\": %" PRIu64 ",\n", st->term);
        printf("  \"applied\": %" PRIu64 ",\n", st->applied);
        fputs("  \"volume\":\n", stdout);
        print_volume_json(st, &m, v, "  ");
        fputs("\n}\n", stdout);
        meta_free(&m);
        return 0;
    }

    struct cl_vol_view vv;
    compute_vol_view(st, &m, v, &vv);
    char vu[33];
    hex16(v->uuid, vu);
    if (v->redundancy == META_RED_EC)
        printf("volume %" PRIu32 " name %s uuid %s health %s redundancy ec=%u+%u\n", v->number, v->name, vu, vv.health,
               v->ec_k, v->ec_m);
    else
        printf("volume %" PRIu32 " name %s uuid %s health %s redundancy replicas=%" PRIu32 "\n", v->number, v->name, vu,
               vv.health, v->nrep);

    printf("leader %" PRIu32 " preferred %" PRIu32 " epoch %" PRIu64 " min_isr %" PRIu32 " isr", v->leader,
           v->nrep ? v->rep[0].node : 0u, v->epoch, v->min_isr);
    for (uint32_t k = 0; k < v->nisr; k++)
        printf("%s%" PRIu32, k ? "," : " ", v->isr[k]);
    printf("%s isr_version %" PRIu64 " set_version %" PRIu64 "\n", v->nisr ? "" : " -", v->isr_version, v->set_version);

    printf("margins young ");
    if (vv.young_na)
        fputs("-", stdout);
    else
        printf("%d", vv.young_margin);
    printf(" settled ");
    if (vv.settled_na)
        fputs("-", stdout);
    else
        printf("%d", vv.settled_margin);
    printf(" settled_state %s rack_survivable %s\n", vv.settled_state, vv.rack_survivable ? "yes" : "no");

    if (v->redundancy == META_RED_EC) {
        printf("stripe");
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++)
            printf("%s%" PRIu32 "%s", p ? "," : " ", v->stripe[p],
                   v->stripe_pos[p].state == META_STRIPE_FILLING ? "(filling)" : "");
        printf("%s stripe_version %" PRIu64 " gc_suspended %u mixed_class %d\n", v->nstripe ? "" : " -",
               v->stripe_version, v->gc_suspended, vv.mixed_class);
    }

    for (uint32_t k = 0; k < v->nrep; k++) {
        printf("replica node %" PRIu32 " rid %016" PRIx64 " open_gen %" PRIu64 " learner %u", v->rep[k].node,
               v->rep[k].rid, v->rep[k].open_gen, (unsigned)v->rep[k].learner);
        if (v->rep[k].replaces)
            printf(" replaces %" PRIu32, v->rep[k].replaces);
        const struct cluster_heard *h = find_heard(st, v->rep[k].node);
        for (uint32_t j = 0; h && j < h->nvols; j++) {
            if (h->vols[j].v.volume != v->number)
                continue;
            const struct cluster_vol_status *vs = &h->vols[j];
            printf(" role %s report_health %s epoch %" PRIu64 " domain %s local_fault_s %" PRIu64
                   " fenced_age_ms %" PRIu64,
                   vol_report_role_name(vs->v.role), vol_report_health_name(vs->v.health), vs->v.epoch,
                   vs->v.domain[0] ? vs->v.domain : "-", vs->local_fault_s, vs->v.fenced_age_ms);
            break;
        }
        printf("\n");
    }

    if (v->redundancy == META_RED_EC) {
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
            if (v->stripe_pos[p].state != META_STRIPE_FILLING)
                continue;
            const struct cluster_fill_report *fr = NULL;
            for (uint32_t i = 0; i < st->nheard && !fr; i++) {
                if (st->heard[i].id != v->stripe[p])
                    continue;
                for (uint32_t fi = 0; fi < st->heard[i].nfills; fi++) {
                    const struct cluster_fill_report *c = &st->heard[i].fills[fi];
                    if (c->volume == v->number && c->pos == p && c->version == v->stripe_version) {
                        fr = c;
                        break;
                    }
                }
            }
            printf("fill pos %" PRIu32 " node %" PRIu32 " prev %" PRIu32 " version %" PRIu64 " done %" PRIu64 "/%" PRIu64
                   " whole %u\n",
                   p, v->stripe[p], v->stripe_pos[p].prev, v->stripe_version, fr ? fr->done : 0, fr ? fr->total : 0,
                   fr ? fr->whole : 0u);
        }
    }

    int any_rb = 0;
    for (uint32_t i = 0; i < st->nheard; i++) {
        const struct cluster_heard *h = &st->heard[i];
        for (uint32_t j = 0; j < h->nprogress_vols; j++) {
            const struct creport_vol_progress *vp = &h->progress_vols[j];
            if (vp->volume != v->number || vp->role == CREPORT_REBUILD_NONE)
                continue;
            any_rb = 1;
            printf("rebuild node %" PRIu32 " role %s done %" PRIu64 "/%" PRIu64 " stalled %u started_ms %" PRIu64
                   " last_progress_ms %" PRIu64 "\n",
                   h->id, creport_rebuild_role_name(vp->role), vp->bytes_done, vp->bytes_total, (unsigned)vp->stalled,
                   vp->started_ms, vp->last_progress_ms);
        }
    }
    if (!any_rb && vv.nfills_active == 0)
        puts("rebuild none");

    printf("settle keys_waiting %" PRIu32 " oldest_wait_s %" PRIu64 " young_scrub_age_s %" PRIu64 "\n",
           vv.settle_keys_waiting, vv.settle_oldest_wait_s, vv.young_scrub_age_s);

    if (vv.nwarnings > 0) {
        fputs("warnings", stdout);
        for (uint32_t k = 0; k < vv.nwarnings; k++)
            printf("%s%s", k ? "," : " ", vv.warnings[k]);
        fputc('\n', stdout);
    }

    meta_free(&m);
    return 0;
}

/* View 4: `tsdb cluster status disks [--json] PEER` (§15.4 table) */
static int print_status_disks(const struct cluster_status *st, int json)
{
    struct meta_state m;
    meta_init(&m);
    int have_meta = st->image_len && meta_snapshot_decode(&m, st->image, st->image_len) == META_OK;
    uint32_t warn_pct = (have_meta && m.initialised) ? m.set.young_warn_pct : 75u;
    uint32_t move_pct = (have_meta && m.initialised) ? m.set.young_move_pct : 80u;
    uint32_t high_pct = (have_meta && m.initialised) ? m.set.young_high_pct : 90u;

    struct cl_disk_summary ds;
    collect_disks(st, &m, have_meta, &ds);

    if (json) {
        fputs("{\n", stdout);
        fputs("  \"view\": \"disks\",\n", stdout);
        printf("  \"reported\": %s,\n", ds.nitems > 0 ? "true" : "false");
        printf("  \"chunk_fill_mark_pct\": %" PRIu32 ",\n", ds.chunk_fill_mark_pct);
        fputs("  \"classes\": [", stdout);
        int nc = 0;
        for (int c = META_DISK_NVME; c <= META_DISK_HDD; c++) {
            const struct cl_disk_class_agg *a = &ds.cls[c];
            if (a->disks == 0)
                continue;
            int fast = meta_disk_class_fast(c);
            char marks[32];
            if (fast)
                snprintf(marks, sizeof marks, "%" PRIu32 "/%" PRIu32 "/%" PRIu32 "%%", warn_pct, move_pct, high_pct);
            else
                snprintf(marks, sizeof marks, "%" PRIu32 "%%", ds.chunk_fill_mark_pct);
            printf("%s\n    {\"class\": \"%s\", \"disks\": %" PRIu32 ", \"disks_up\": %" PRIu32
                   ", \"disks_down\": %" PRIu32 ", \"disks_out\": %" PRIu32 ", \"disks_destroyed\": %" PRIu32
                   ", \"size_bytes\": %" PRIu64 ", \"young_bytes\": ",
                   nc++ ? "," : "", meta_disk_class_name(c), a->disks, a->disks_up, a->disks_down, a->disks_out,
                   a->disks_destroyed, a->size_bytes);
            if (a->have_usage && (fast || a->has_log_use))
                printf("%" PRIu64, a->young_bytes);
            else
                fputs("null", stdout);
            fputs(", \"retired_bytes\": null, \"index_bytes\": null, \"chunk_bytes\": ", stdout);
            if (a->have_usage)
                printf("%" PRIu64, a->chunk_bytes);
            else
                fputs("null", stdout);
            fputs(", \"free_bytes\": ", stdout);
            if (a->have_usage)
                printf("%" PRIu64, a->free_bytes);
            else
                fputs("null", stdout);
            printf(", \"marks\": \"%s\"", marks);
            if (fast)
                printf(", \"young_warn_pct\": %" PRIu32 ", \"young_move_pct\": %" PRIu32 ", \"young_high_pct\": %" PRIu32,
                       warn_pct, move_pct, high_pct);
            else
                fputs(", \"young_warn_pct\": null, \"young_move_pct\": null, \"young_high_pct\": null", stdout);
            if (!fast || a->has_chunk_use)
                printf(", \"chunk_mark_pct\": %" PRIu32, ds.chunk_fill_mark_pct);
            else
                fputs(", \"chunk_mark_pct\": null", stdout);
            fputs(", \"days_to_mark\": null}", stdout);
        }
        fputs(nc ? "\n  ],\n" : "],\n", stdout);

        fputs("  \"disks\": [", stdout);
        for (size_t i = 0; i < ds.nitems; i++) {
            const struct cl_disk_item *it = &ds.items[i];
            char sid[33];
            hex16(it->store_id, sid);
            printf("%s\n    {\"node\": %" PRIu32 ", \"store_id\": \"%s\", \"class\": \"%s\", \"uses\": [",
                   i ? "," : "", it->node, sid, meta_disk_class_name(it->disk_class));
            int nu = 0;
            if (it->uses & META_DISK_USE_LOG)
                printf("%s\"log\"", nu++ ? ", " : "");
            if (it->uses & META_DISK_USE_CHUNK)
                printf("%s\"chunk\"", nu++ ? ", " : "");
            printf("], \"state\": \"%s\", \"size_bytes\": %" PRIu64 ", \"sync_us\": %" PRIu64 ", \"young_bytes\": ",
                   meta_store_state_name(it->state), it->size_bytes, it->sync_us);
            if (it->have_usage)
                printf("%" PRIu64 ", \"chunk_bytes\": %" PRIu64 "}", it->young_bytes, it->chunk_bytes);
            else
                fputs("null, \"chunk_bytes\": null}", stdout);
        }
        fputs(ds.nitems ? "\n  ],\n" : "],\n", stdout);

        fputs("  \"warnings\": [", stdout);
        for (size_t i = 0; i < ds.nwarnings; i++) {
            if (i)
                fputs(", ", stdout);
            json_str(stdout, ds.warnings[i]);
        }
        fputs("]\n}\n", stdout);

        disk_summary_free(&ds);
        meta_free(&m);
        return 0;
    }

    if (ds.nitems == 0) {
        puts("no disks reported");
        disk_summary_free(&ds);
        meta_free(&m);
        return 0;
    }

    printf("%-6s %-6s %-8s %-7s %-8s %-7s %-8s %-8s %-11s %s\n", "class", "disks", "size", "young", "retired", "index",
           "chunks", "free", "marks", "days to mark");
    for (int c = META_DISK_NVME; c <= META_DISK_HDD; c++) {
        const struct cl_disk_class_agg *a = &ds.cls[c];
        if (a->disks == 0)
            continue;
        int fast = meta_disk_class_fast(c);
        char s_sz[20], s_y[20], s_c[20], s_f[20], marks[24];
        fmt_bytes(a->size_bytes, s_sz, sizeof s_sz);
        if (a->have_usage && (fast || a->has_log_use))
            fmt_bytes(a->young_bytes, s_y, sizeof s_y);
        else
            snprintf(s_y, sizeof s_y, "-");
        if (a->have_usage) {
            fmt_bytes(a->chunk_bytes, s_c, sizeof s_c);
            fmt_bytes(a->free_bytes, s_f, sizeof s_f);
        } else {
            snprintf(s_c, sizeof s_c, "-");
            snprintf(s_f, sizeof s_f, "-");
        }
        if (fast)
            snprintf(marks, sizeof marks, "%" PRIu32 "/%" PRIu32 "/%" PRIu32 "%%", warn_pct, move_pct, high_pct);
        else
            snprintf(marks, sizeof marks, "%" PRIu32 "%%", ds.chunk_fill_mark_pct);
        printf("%-6s %-6" PRIu32 " %-8s %-7s %-8s %-7s %-8s %-8s %-11s %s\n", meta_disk_class_name(c), a->disks, s_sz,
               s_y, "-", "-", s_c, s_f, marks, "-");
    }
    for (size_t i = 0; i < ds.nwarnings; i++)
        printf("warn %s\n", ds.warnings[i]);

    disk_summary_free(&ds);
    meta_free(&m);
    return 0;
}

/* ------------------------------------------------------------------ verb table & execution context */

enum {
    CL_VF_LOCAL = 1u << 0,              /* handled locally without --secret-file or RPC (secret, user) */
    CL_VF_PRINT_ERR_BODY = 1u << 1,     /* print leader's error reply body on non-zero rc */
    CL_VF_SECRET_IN_ENCODE = 1u << 2,   /* encoder needs ctx->secret loaded first (volume gc resume) */
    CL_VF_RAW_ARGS = 1u << 3,           /* subcommand parses raw_argc/raw_argv directly (user) */
};

enum cl_status_view {
    CL_VIEW_SUMMARY = 0,
    CL_VIEW_VOLUMES = 1,
    CL_VIEW_VOLUME = 2,
    CL_VIEW_DISKS = 3,
};

struct cl_verb;

struct cl_ctx {
    const struct cl_verb *verb;
    int raw_argc;
    char **raw_argv;

    const char *pos[8]; /* positional arguments after verb words */
    int npos;

    const char *secret_file;
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t slen;

    const char *peer;
    char target[META_ADDR_MAX + 1]; /* updated on NOTLEADER follow */
    int op;

    /* init */
    struct meta_settings init_s;
    int init_mode;
    const char *superuser;
    const char *pwfile;

    /* volume add */
    uint32_t replicas;
    uint32_t ec_k, ec_m;

    /* volume move / node hold / node drain / node replace */
    uint32_t mv_from, mv_to, mv_flags, mv_pos;
    int have_pos;
    uint32_t hold_for_s, hold_flags, drain_flags, repl_flags;
    int have_hold_for;

    /* status */
    int status_view;
    int json;
    const char *vol_arg;
};

typedef int (*cl_parse_flag_fn)(struct cl_ctx *ctx, int argc, char **argv, int *idx);
typedef int (*cl_encode_fn)(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len);
typedef int (*cl_print_fn)(struct cl_ctx *ctx, const uint8_t *reply, size_t len);

struct cl_verb {
    const char *words[3];        /* 1 to 3 words; unused slots NULL */
    int nwords;                  /* 1, 2, or 3 */
    int min_args;                /* minimum positional args after words */
    int max_args;                /* maximum positional args after words (-1 = unlimited) */
    unsigned flags;              /* CL_VF_* */
    int op;                      /* default CLUSTER_OP_* */
    int timeout_ms;              /* RPC timeout (0 = 30000 ms) */
    cl_parse_flag_fn parse_flag; /* returns 1 if consumed, 0 if unknown, -1 on usage error (exit 2) */
    cl_encode_fn encode;         /* returns 0 ok, 1 runtime error (said), 2 usage error (said) */
    cl_print_fn print;           /* returns 0 ok, 1 error */
    const char *help;            /* detailed help for `tsdb cluster help VERB` */
};

/* ---- flag parsers ---- */

static int parse_flag_init(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    int i = *idx;
    const char *a = argv[i];
    if (i + 1 >= argc)
        return 0;
    const char *v = argv[i + 1];
    struct meta_settings *s = &ctx->init_s;

    if (!strcmp(a, "--superuser")) {
        ctx->superuser = v;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--password-file")) {
        ctx->pwfile = v;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--mode")) {
        ctx->init_mode = !strcmp(v, "shared")       ? META_MODE_SHARED
                         : !strcmp(v, "replicated") ? META_MODE_REPLICATED
                                                    : 0;
        if (!ctx->init_mode)
            return cl_usage(), -1;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--failover")) {
        s->failover = !strcmp(v, "lease")      ? META_FAILOVER_LEASE
                      : !strcmp(v, "attested") ? META_FAILOVER_ATTESTED
                      : !strcmp(v, "bmc")      ? META_FAILOVER_BMC
                                               : 99;
        if (s->failover == 99)
            return cl_usage(), -1;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--read-barrier")) {
        if (strcmp(v, "on") && strcmp(v, "off"))
            return cl_usage(), -1;
        s->read_barrier = !strcmp(v, "on");
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--log-on-hdd")) {
        if (!strcmp(v, "on") || !strcmp(v, "1"))
            s->log_on_hdd = 1;
        else if (!strcmp(v, "off") || !strcmp(v, "0"))
            s->log_on_hdd = 0;
        else
            return cl_usage(), -1;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--leader-balance-s")) {
        char *end;
        unsigned long x = strtoul(v, &end, 10);
        if (!v[0] || !isdigit((unsigned char)v[0]) || *end || x > 86400) {
            fprintf(stderr, "tsdb cluster: --leader-balance-s %s: expected 0 (off) to 86400\n", v);
            return -1;
        }
        s->leader_balance_s = (uint32_t)x;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(a, "--opening-hold-s") || !strcmp(a, "--repl-retain-down-gb") || !strcmp(a, "--balance-mb-s")) {
        char *end;
        unsigned long x = strtoul(v, &end, 10);
        unsigned long maxv = !strcmp(a, "--opening-hold-s") ? 86400ul : 0x7ffffffful;
        if (!v[0] || !isdigit((unsigned char)v[0]) || *end || x > maxv) {
            fprintf(stderr, "tsdb cluster: %s %s: expected 0 to %lu\n", a, v, maxv);
            return -1;
        }
        if (!strcmp(a, "--opening-hold-s"))
            s->opening_hold_s = (uint32_t)x;
        else if (!strcmp(a, "--repl-retain-down-gb"))
            s->repl_retain_down_gb = (uint32_t)x;
        else
            s->balance_mb_s = (uint32_t)x;
        *idx = i + 1;
        return 1;
    }

    struct {
        const char *flag;
        uint32_t *dst;
    } nums[] = {
        {"--replication", &s->replication},
        {"--min-isr", &s->min_isr},
        {"--node-timeout-ms", &s->node_timeout_ms},
        {"--heartbeat-ms", &s->heartbeat_ms},
        {"--isr-lag-ms", &s->isr_lag_ms},
        {"--rebuild-after-s", &s->rebuild_after_s},
        {"--failstop-out-s", &s->failstop_out_s},
        {"--out-guard-pct", &s->out_guard_pct},
        {"--out-guard-hold-s", &s->out_guard_hold_s},
        {"--young-high-pct", &s->young_high_pct},
        {"--rebuild-concurrency", &s->rebuild_concurrency},
        {"--scrub-days", &s->scrub_days},
        {"--scrub-mb-s", &s->scrub_mb_s},
        {"--scrub-out-count", &s->scrub_out_count},
        {"--young-warn-pct", &s->young_warn_pct},
        {"--young-move-pct", &s->young_move_pct},
        {"--disk-stall-s", &s->disk_stall_s},
        {"--voter-replace-s", &s->voter_replace_s},
        {"--voters", &s->voters},
        {"--volumes-per-server", &s->volumes_per_server},
        {"--lease-s", &s->lease_s},
        {"--lease-ms", &s->lease_ms},
        {"--drift-ppm", &s->drift_ppm},
        {"--unanswered-ms", &s->unanswered_ms},
        {"--commit-interval-ms", &s->commit_interval_ms},
        {"--repl-retain-mb", &s->repl_retain_mb},
        {"--repl-retain-s", &s->repl_retain_s},
        {"--idle-seal-ms", &s->idle_seal_ms},
        {"--rebuild-mb-s", &s->rebuild_mb_s},
        {"--rebuild-max", &s->rebuild_max},
        {"--transfer-wait-ms", &s->transfer_wait_ms},
    };
    for (size_t k = 0; k < sizeof nums / sizeof nums[0]; k++) {
        if (!strcmp(a, nums[k].flag)) {
            if (num(a, v, nums[k].dst) != 0)
                return -1;
            *idx = i + 1;
            return 1;
        }
    }
    return 0;
}

static int parse_flag_node_hold(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    int i = *idx;
    if (!strcmp(argv[i], "--release")) {
        ctx->hold_flags |= CLUSTER_HOLD_RELEASE;
        return 1;
    }
    if (!strcmp(argv[i], "--for") && i + 1 < argc) {
        if (dur_s("--for", argv[i + 1], 86400, &ctx->hold_for_s) != 0)
            return -1;
        ctx->have_hold_for = 1;
        *idx = i + 1;
        return 1;
    }
    return 0;
}

static int parse_flag_node_drain(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    (void)argc;
    if (!strcmp(argv[*idx], "--cancel")) {
        ctx->drain_flags |= CLUSTER_DRAIN_CANCEL;
        return 1;
    }
    return 0;
}

static int parse_flag_node_replace(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    (void)argc;
    if (!strcmp(argv[*idx], "--cancel")) {
        ctx->repl_flags |= CLUSTER_REPLACE_CANCEL;
        return 1;
    }
    return 0;
}

static int parse_flag_volume_add(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    int i = *idx;
    if (i + 1 >= argc)
        return 0;
    if (!strcmp(argv[i], "--replicas")) {
        if (num(argv[i], argv[i + 1], &ctx->replicas) != 0)
            return -1;
        *idx = i + 1;
        return 1;
    }
    if (!strcmp(argv[i], "--redundancy")) {
        const char *rv = argv[i + 1];
        char *end;
        if (!strncmp(rv, "replicas=", 9)) {
            if (num("--redundancy replicas=", rv + 9, &ctx->replicas) != 0)
                return -1;
            ctx->ec_k = ctx->ec_m = 0;
        } else if (!strncmp(rv, "ec=", 3)) {
            unsigned long k = strtoul(rv + 3, &end, 10), m = 0;
            int ok = end != rv + 3 && *end == '+' && isdigit((unsigned char)rv[3]);
            if (ok) {
                const char *ms = end + 1;
                m = strtoul(ms, &end, 10);
                ok = end != ms && !*end && isdigit((unsigned char)ms[0]);
            }
            if (!ok || k < 1 || k > META_EC_K_MAX || m < 1 || m > META_EC_M_MAX) {
                fprintf(stderr, "tsdb cluster: --redundancy %s: expected ec=K+M with K 1 to %d and M 1 to %d\n", rv,
                        META_EC_K_MAX, META_EC_M_MAX);
                return -1;
            }
            ctx->ec_k = (uint32_t)k;
            ctx->ec_m = (uint32_t)m;
        } else {
            fprintf(stderr, "tsdb cluster: --redundancy %s: expected ec=K+M or replicas=N\n", rv);
            return -1;
        }
        *idx = i + 1;
        return 1;
    }
    return 0;
}

static int parse_flag_volume_move(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    int i = *idx;
    if (!strcmp(argv[i], "--log")) {
        ctx->mv_flags |= CLUSTER_MOVE_LOG;
        return 1;
    }
    if (!strcmp(argv[i], "--cancel")) {
        ctx->mv_flags |= CLUSTER_MOVE_CANCEL;
        return 1;
    }
    if (!strcmp(argv[i], "--pos") && i + 1 < argc) {
        char *end;
        const char *pv = argv[i + 1];
        unsigned long p = strtoul(pv, &end, 10);
        if (!pv[0] || !isdigit((unsigned char)pv[0]) || *end || p >= META_STRIPE_MAX) {
            fprintf(stderr, "tsdb cluster volume move: --pos %s: expected 0 to %d\n", pv, META_STRIPE_MAX - 1);
            return -1;
        }
        ctx->mv_pos = (uint32_t)p;
        ctx->have_pos = 1;
        *idx = i + 1;
        return 1;
    }
    if ((!strcmp(argv[i], "--from") || !strcmp(argv[i], "--to")) && i + 1 < argc) {
        if (num(argv[i], argv[i + 1], !strcmp(argv[i], "--from") ? &ctx->mv_from : &ctx->mv_to) != 0)
            return -1;
        *idx = i + 1;
        return 1;
    }
    return 0;
}

static int parse_flag_status(struct cl_ctx *ctx, int argc, char **argv, int *idx)
{
    (void)argc;
    if (!strcmp(argv[*idx], "--json")) {
        ctx->json = 1;
        return 1;
    }
    return 0;
}

/* ---- local handlers & request encoders ---- */

static int enc_secret(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)req;
    (void)cap;
    (void)req_len;
    char err[1024];
    if (cluster_make_secret(ctx->pos[0], err, sizeof err) != 0) {
        fprintf(stderr, "tsdb cluster: %s\n", err);
        return 1;
    }
    printf("wrote a new cluster secret to %s (copy it to every server; tsdbd --cluster-secret-file)\n", ctx->pos[0]);
    return 0;
}

static int enc_user(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)req;
    (void)cap;
    (void)req_len;
    return cluster_user_cli(ctx->raw_argc, ctx->raw_argv);
}

static int enc_init(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    const struct meta_settings *s = &ctx->init_s;
    if (s->young_warn_pct > s->young_move_pct || s->young_move_pct >= s->young_high_pct) {
        fprintf(stderr,
                "tsdb cluster: --young-warn-pct (%u) <= --young-move-pct (%u) < --young-high-pct (%u) required\n",
                s->young_warn_pct, s->young_move_pct, s->young_high_pct);
        return 2;
    }
    if (s->rebuild_max > 16 || s->transfer_wait_ms < 100 || s->transfer_wait_ms > 60000) {
        fprintf(stderr, "tsdb cluster: --rebuild-max is at most 16, --transfer-wait-ms 100 to 60000\n");
        return 2;
    }
    if (s->heartbeat_ms >= s->node_timeout_ms || s->min_isr > s->replication) {
        fprintf(stderr, "tsdb cluster: --heartbeat-ms must be below --node-timeout-ms, --min-isr at most "
                        "--replication\n");
        return 2;
    }
    /* Check superuser name and password file before INIT so a bad file never leaves an initialised cluster userless. */
    if (ctx->superuser && cluster_user_first_check(ctx->superuser, ctx->pwfile) != 0)
        return 2;
    ctx->peer = ctx->pos[0];
    *req_len = cluster_settings_encode(s, ctx->init_mode, req);
    return 0;
}

static int enc_node_add(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    const char *newpeer = ctx->pos[1];
    size_t n = strlen(newpeer);
    if (n >= META_ADDR_MAX || !strchr(newpeer, ':')) {
        fprintf(stderr, "tsdb cluster: %s: expected host:port\n", newpeer);
        return 2;
    }
    req[0] = (uint8_t)n;
    req[1] = (uint8_t)(n >> 8);
    memcpy(req + 2, newpeer, n);
    *req_len = 2 + n;
    return 0;
}

static int enc_node_hold(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    uint32_t node = 0;
    if (num("NODE", ctx->pos[1], &node) != 0)
        return 2;
    int rel = (ctx->hold_flags & CLUSTER_HOLD_RELEASE) != 0;
    if (rel == ctx->have_hold_for) {
        fprintf(stderr, "tsdb cluster node hold: give exactly one of --for DUR or --release\n");
        return 2;
    }
    *req_len = cluster_node_hold_encode(node, ctx->hold_for_s, ctx->hold_flags, req);
    return 0;
}

static int enc_node_single(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    uint32_t node = 0;
    if (num("NODE", ctx->pos[1], &node) != 0)
        return 2;
    if (ctx->op == CLUSTER_OP_NODE_DRAIN)
        *req_len = cluster_node_drain_encode(node, ctx->drain_flags, req);
    else
        *req_len = cluster_node_op_encode(node, req);
    return 0;
}

static int enc_node_replace(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    uint32_t old_node = 0, new_node = 0;
    if (num("OLD", ctx->pos[1], &old_node) != 0)
        return 2;
    if (ctx->repl_flags & CLUSTER_REPLACE_CANCEL) {
        if (ctx->npos != 2)
            return cl_usage();
        *req_len = cluster_node_replace_cancel_encode(old_node, req);
        return 0;
    }
    if (ctx->npos != 3)
        return cl_usage();
    if (num("NEW", ctx->pos[2], &new_node) != 0)
        return 2;
    if (old_node == new_node) {
        fprintf(stderr, "tsdb cluster node replace: OLD and NEW must be different nodes\n");
        return 2;
    }
    *req_len = cluster_node_replace_encode(old_node, new_node, req);
    return 0;
}

static int enc_volume_add(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    const char *vname = ctx->pos[1];
    size_t n = strlen(vname);
    if (n == 0 || n >= META_NAME_MAX) {
        fprintf(stderr, "tsdb cluster: %s: a volume name is 1 to %d bytes\n", vname, META_NAME_MAX - 1);
        return 2;
    }
    ctx->op = ctx->ec_k ? CLUSTER_OP_VOLUME_ADD_EC : CLUSTER_OP_VOLUME_ADD;
    *req_len = ctx->ec_k ? cluster_volume_add_ec_encode(vname, ctx->replicas, (uint8_t)ctx->ec_k, (uint8_t)ctx->ec_m, req)
                         : cluster_volume_add_encode(vname, ctx->replicas, req);
    return 0;
}

/* `volume gc resume`'s VOLUME: exact name in PEER's status, else numeric volume number. */
static int resolve_gc_volume(const char *s, const char *addr, const uint8_t *secret, size_t slen, uint32_t *out)
{
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    int numeric = *s && isdigit((unsigned char)*s) && !*end && v > 0 && v <= UINT32_MAX;
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

static int enc_volume_gc_resume(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    uint32_t gv = 0;
    if (resolve_gc_volume(ctx->pos[1], ctx->peer, ctx->secret, ctx->slen, &gv) != 0)
        return 1;
    for (int j = 0; j < 4; j++)
        req[j] = (uint8_t)(gv >> (8 * j));
    *req_len = 4;
    return 0;
}

static int enc_vol_node(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    uint32_t vol = 0, node = 0;
    if (ctx->op == CLUSTER_OP_VOLUME_FORCE) {
        char *end1, *end2;
        unsigned long fv = strtoul(ctx->pos[1], &end1, 10), fn = strtoul(ctx->pos[2], &end2, 10);
        if (!isdigit((unsigned char)ctx->pos[1][0]) || !isdigit((unsigned char)ctx->pos[2][0]) || *end1 || *end2 ||
            !fv || !fn || fv > UINT32_MAX || fn > UINT32_MAX)
            return cl_usage();
        vol = (uint32_t)fv;
        node = (uint32_t)fn;
    } else {
        if (num("VOLUME", ctx->pos[1], &vol) != 0 || num("NODE", ctx->pos[2], &node) != 0)
            return 2;
    }
    *req_len = cluster_volume_force_encode(vol, node, req);
    return 0;
}

static int enc_volume_move(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)cap;
    ctx->peer = ctx->pos[0];
    uint32_t vol = 0;
    if (num("VOLUME", ctx->pos[1], &vol) != 0)
        return 2;
    if (ctx->have_pos) {
        if (ctx->mv_flags || ctx->mv_from || !ctx->mv_to) {
            fprintf(stderr, "tsdb cluster volume move --pos: --pos P and --to NODE are required (no --log/--from)\n");
            return 2;
        }
        ctx->op = CLUSTER_OP_VOLUME_MOVE_POS;
        *req_len = cluster_volume_move_pos_encode(vol, ctx->mv_pos, ctx->mv_to, req);
        return 0;
    }
    if (!(ctx->mv_flags & CLUSTER_MOVE_LOG) || !ctx->mv_from || !ctx->mv_to || ctx->mv_from == ctx->mv_to) {
        fprintf(stderr, "tsdb cluster volume move: --log, --from NODE and --to NODE (another node) are required\n");
        return 2;
    }
    *req_len = cluster_volume_move_encode(vol, ctx->mv_from, ctx->mv_to, ctx->mv_flags, req);
    return 0;
}

static int enc_status_summary(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)req;
    (void)cap;
    ctx->status_view = CL_VIEW_SUMMARY;
    ctx->peer = ctx->pos[0];
    *req_len = 0;
    return 0;
}

static int enc_status_volumes(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)req;
    (void)cap;
    ctx->status_view = CL_VIEW_VOLUMES;
    ctx->peer = ctx->pos[0];
    *req_len = 0;
    return 0;
}

static int enc_status_volume(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)req;
    (void)cap;
    ctx->status_view = CL_VIEW_VOLUME;
    ctx->vol_arg = ctx->pos[0];
    ctx->peer = ctx->pos[1];
    *req_len = 0;
    return 0;
}

static int enc_status_disks(struct cl_ctx *ctx, uint8_t *req, size_t cap, size_t *req_len)
{
    (void)req;
    (void)cap;
    ctx->status_view = CL_VIEW_DISKS;
    ctx->peer = ctx->pos[0];
    *req_len = 0;
    return 0;
}

/* ---- reply printers ---- */

static int prt_init(struct cl_ctx *ctx, const uint8_t *reply, size_t len)
{
    (void)reply;
    (void)len;
    printf("initialised: node 1 at %s\n", ctx->target);
    if (ctx->superuser) {
        if (cluster_user_add_first(ctx->target, ctx->secret, ctx->slen, ctx->superuser, ctx->pwfile) == 0)
            return 0;
        fprintf(stderr, "tsdb cluster init: the cluster is initialised without its superuser; make it with "
                        "tsdb cluster user add %s %s --superuser --password-file F --secret-file F\n",
                ctx->target, ctx->superuser);
        return 1;
    }
    return 0;
}

static int prt_node_add(struct cl_ctx *ctx, const uint8_t *reply, size_t len)
{
    (void)reply;
    (void)len;
    printf("added %s\n", ctx->pos[1]);
    return 0;
}

static int prt_text_reply(struct cl_ctx *ctx, const uint8_t *reply, size_t len)
{
    (void)ctx;
    printf("%.*s\n", (int)len, (const char *)reply);
    return 0;
}

static int prt_volume_add(struct cl_ctx *ctx, const uint8_t *reply, size_t len)
{
    uint32_t number = 0;
    for (size_t k = 0; k < 4 && k < len; k++)
        number |= (uint32_t)reply[k] << (8 * k);
    printf("added volume %s: number %" PRIu32 "\n", ctx->pos[1], number);
    return 0;
}

static int prt_volume_gc_resume(struct cl_ctx *ctx, const uint8_t *reply, size_t len)
{
    (void)reply;
    (void)len;
    printf("volume %s: orphan collection resumed\n", ctx->pos[1]);
    return 0;
}

static int prt_status(struct cl_ctx *ctx, const uint8_t *reply, size_t len)
{
    struct cluster_status st;
    if (cluster_status_parse(reply, len, &st) != 0) {
        fprintf(stderr, "tsdb cluster status: the reply does not decode\n");
        return 1;
    }
    int rc = 0;
    switch (ctx->status_view) {
    case CL_VIEW_SUMMARY:
        if (ctx->json)
            print_status_summary_json(&st);
        else
            print_status_summary_text(&st);
        break;
    case CL_VIEW_VOLUMES:
        rc = print_status_volumes(&st, ctx->json);
        break;
    case CL_VIEW_VOLUME:
        rc = print_status_volume(&st, ctx->vol_arg, ctx->json);
        break;
    case CL_VIEW_DISKS:
        rc = print_status_disks(&st, ctx->json);
        break;
    default:
        rc = 1;
        break;
    }
    cluster_status_free(&st);
    return rc;
}

/* ---- the verb table: longest word prefixes first (or matched by best nwords) ---- */

static const struct cl_verb cl_verbs[] = {
    {{"secret", NULL, NULL}, 1, 1, 1, CL_VF_LOCAL, 0, 0, NULL, enc_secret, NULL, help_secret},
    {{"user", NULL, NULL}, 1, 0, -1, CL_VF_LOCAL | CL_VF_RAW_ARGS, 0, 0, NULL, enc_user, NULL, help_user},
    {{"init", NULL, NULL}, 1, 1, 1, 0, CLUSTER_OP_INIT, 30000, parse_flag_init, enc_init, prt_init, help_init},
    {{"node", "add", NULL}, 2, 2, 2, 0, CLUSTER_OP_NODE_ADD, 30000, NULL, enc_node_add, prt_node_add, help_node_add},
    {{"node", "hold", NULL}, 2, 2, 2, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_NODE_HOLD, 30000, parse_flag_node_hold,
     enc_node_hold, prt_text_reply, help_node_hold},
    {{"node", "out", NULL}, 2, 2, 2, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_NODE_OUT, 30000, NULL, enc_node_single,
     prt_text_reply, help_node_out},
    {{"node", "drain", NULL}, 2, 2, 2, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_NODE_DRAIN, 30000, parse_flag_node_drain,
     enc_node_single, prt_text_reply, help_node_drain},
    {{"node", "replace", NULL}, 2, 2, 3, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_NODE_REPLACE, 30000, parse_flag_node_replace,
     enc_node_replace, prt_text_reply, help_node_replace},
    {{"node", "destroyed", NULL}, 2, 2, 2, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_NODE_DESTROYED, 30000, NULL,
     enc_node_single, prt_text_reply, help_node_destroyed},
    {{"volume", "gc", "resume"}, 3, 2, 2, CL_VF_PRINT_ERR_BODY | CL_VF_SECRET_IN_ENCODE, CLUSTER_OP_VOLUME_GC_RESUME,
     30000, NULL, enc_volume_gc_resume, prt_volume_gc_resume, help_volume_gc_resume},
    {{"volume", "add", NULL}, 2, 2, 2, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_VOLUME_ADD, 30000, parse_flag_volume_add,
     enc_volume_add, prt_volume_add, help_volume_add},
    {{"volume", "force-leader", NULL}, 2, 3, 3, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_VOLUME_FORCE, 30000, NULL,
     enc_vol_node, prt_text_reply, help_volume_force_leader},
    /* A transfer waits for the drain (transfer_wait_ms, up to 60 s), READY slack (1 s), and targeted promotion (2 s),
     * so 90 s covers the longest setting (30 s would be too short). */
    {{"volume", "transfer", NULL}, 2, 3, 3, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_VOLUME_TRANSFER, 90000, NULL, enc_vol_node,
     prt_text_reply, help_volume_transfer},
    {{"volume", "move", NULL}, 2, 2, 2, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_VOLUME_MOVE, 30000, parse_flag_volume_move,
     enc_volume_move, prt_text_reply, help_volume_move},
    {{"replica", "reset", NULL}, 2, 3, 3, CL_VF_PRINT_ERR_BODY, CLUSTER_OP_REPLICA_RESET, 30000, NULL, enc_vol_node,
     prt_text_reply, help_replica_reset},
    {{"status", "volumes", NULL}, 2, 1, 1, 0, CLUSTER_OP_STATUS, 30000, parse_flag_status, enc_status_volumes,
     prt_status, help_status_volumes},
    {{"status", "volume", NULL}, 2, 2, 2, 0, CLUSTER_OP_STATUS, 30000, parse_flag_status, enc_status_volume, prt_status,
     help_status_volume},
    {{"status", "disks", NULL}, 2, 1, 1, 0, CLUSTER_OP_STATUS, 30000, parse_flag_status, enc_status_disks, prt_status,
     help_status_disks},
    {{"status", NULL, NULL}, 1, 1, 1, 0, CLUSTER_OP_STATUS, 30000, parse_flag_status, enc_status_summary, prt_status,
     help_status},
};

static const size_t cl_nverbs = sizeof cl_verbs / sizeof cl_verbs[0];

static const struct cl_verb *find_verb_exact(int argc, char **argv, int start)
{
    const struct cl_verb *best = NULL;
    for (size_t i = 0; i < cl_nverbs; i++) {
        const struct cl_verb *v = &cl_verbs[i];
        if (start + v->nwords > argc)
            continue;
        int ok = 1;
        for (int w = 0; w < v->nwords; w++) {
            if (strcmp(argv[start + w], v->words[w]) != 0) {
                ok = 0;
                break;
            }
        }
        if (ok && (!best || v->nwords > best->nwords))
            best = v;
    }
    return best;
}

static int run_help(int argc, char **argv)
{
    if (argc <= 2) {
        fputs(cl_help, stdout);
        return 0;
    }
    /* Exact verb match (e.g., `help status`, `help status disks`, `help node hold`, `help volume gc resume`). */
    int want_words = argc - 2;
    for (size_t i = 0; i < cl_nverbs; i++) {
        const struct cl_verb *v = &cl_verbs[i];
        if (v->nwords != want_words)
            continue;
        int ok = 1;
        for (int w = 0; w < v->nwords; w++)
            if (strcmp(argv[2 + w], v->words[w]) != 0)
                ok = 0;
        if (ok) {
            fputs(v->help, stdout);
            return 0;
        }
    }
    /* Group prefix match (e.g., `help node`, `help volume`, `help replica`): print every verb in that group. */
    int printed = 0;
    for (size_t i = 0; i < cl_nverbs; i++) {
        const struct cl_verb *v = &cl_verbs[i];
        if (want_words < v->nwords) {
            int ok = 1;
            for (int w = 0; w < want_words; w++)
                if (strcmp(argv[2 + w], v->words[w]) != 0)
                    ok = 0;
            if (ok) {
                if (printed)
                    fputc('\n', stdout);
                fputs(v->help, stdout);
                printed = 1;
            }
        }
    }
    if (printed)
        return 0;
    fputs("tsdb cluster help: unknown verb '", stderr);
    for (int i = 2; i < argc; i++)
        fprintf(stderr, "%s%s", i > 2 ? " " : "", argv[i]);
    fputs("'\n\n", stderr);
    return cl_usage();
}

int cluster_cli(int argc, char **argv)
{
    /* argv[0] is "cluster". No verb, or help: the commands, on stdout, success (asking is not a mistake). */
    if (argc == 1) {
        fputs(cl_help, stdout);
        return 0;
    }
    if (!strcmp(argv[1], "help") || !strcmp(argv[1], "--help") || !strcmp(argv[1], "-h")) {
        if (!strcmp(argv[1], "help"))
            return run_help(argc, argv);
        if (argc == 2) {
            fputs(cl_help, stdout);
            return 0;
        }
        return cl_usage();
    }

    struct cl_ctx ctx;
    memset(&ctx, 0, sizeof ctx);
    ctx.raw_argc = argc;
    ctx.raw_argv = argv;
    meta_settings_default(&ctx.init_s);
    ctx.init_mode = META_MODE_REPLICATED;

    /* Match the verb. For `status`, allow `--json` (or `--secret-file F`) to appear before the view name
     * (`status --json volumes PEER`, `status --json volume V PEER`, `status --json disks PEER`). */
    const struct cl_verb *v = NULL;
    int arg_start = 1;
    int skip_idx = -1;
    if (!strcmp(argv[1], "status")) {
        /* Scan past any leading flags before the first non-flag token to detect sub-views. */
        int scan = 2;
        while (scan < argc) {
            if (!strcmp(argv[scan], "--json")) {
                scan++;
            } else if (!strcmp(argv[scan], "--secret-file") && scan + 1 < argc) {
                scan += 2;
            } else {
                break;
            }
        }
        if (scan < argc && (!strcmp(argv[scan], "volumes") || !strcmp(argv[scan], "volume") ||
                            !strcmp(argv[scan], "disks"))) {
            for (size_t k = 0; k < cl_nverbs; k++) {
                if (cl_verbs[k].nwords == 2 && !strcmp(cl_verbs[k].words[0], "status") &&
                    !strcmp(cl_verbs[k].words[1], argv[scan])) {
                    v = &cl_verbs[k];
                    arg_start = 2;
                    skip_idx = scan;
                    break;
                }
            }
        }
        if (!v) {
            v = find_verb_exact(argc, argv, 1);
            arg_start = 2;
        }
    } else {
        v = find_verb_exact(argc, argv, 1);
        if (v)
            arg_start = 1 + v->nwords;
    }

    if (!v)
        return cl_usage();

    ctx.verb = v;
    ctx.op = v->op;

    if (v->flags & CL_VF_RAW_ARGS)
        return v->encode(&ctx, NULL, 0, NULL);

    /* Parse flags and positional arguments in one pass after the matched verb words. */
    for (int i = arg_start; i < argc; i++) {
        if (i == skip_idx)
            continue;
        if (!(v->flags & CL_VF_LOCAL) && !strcmp(argv[i], "--secret-file")) {
            if (i + 1 >= argc)
                return cl_usage();
            ctx.secret_file = argv[++i];
            continue;
        }
        if (v->parse_flag) {
            int pr = v->parse_flag(&ctx, argc, argv, &i);
            if (pr < 0)
                return 2;
            if (pr > 0)
                continue;
        }
        if (strncmp(argv[i], "--", 2) == 0)
            return cl_usage();
        if (ctx.npos >= (int)(sizeof ctx.pos / sizeof ctx.pos[0]))
            return cl_usage();
        ctx.pos[ctx.npos++] = argv[i];
    }

    if (ctx.npos < v->min_args || (v->max_args >= 0 && ctx.npos > v->max_args))
        return cl_usage();

    if (v->flags & CL_VF_LOCAL)
        return v->encode(&ctx, NULL, 0, NULL);

    if (!ctx.secret_file) {
        fprintf(stderr, "tsdb cluster: --secret-file is required (the cluster's secret, as tsdbd's "
                        "--cluster-secret-file)\n");
        return 2;
    }

    char err[1024];
    uint8_t req[512];
    size_t rl = 0;
    if (v->flags & CL_VF_SECRET_IN_ENCODE) {
        if (cluster_read_secret(ctx.secret_file, ctx.secret, sizeof ctx.secret, &ctx.slen, err, sizeof err) != 0) {
            fprintf(stderr, "tsdb cluster: %s\n", err);
            return 1;
        }
    }
    int erc = v->encode(&ctx, req, sizeof req, &rl);
    if (erc != 0)
        return erc;
    if (!(v->flags & CL_VF_SECRET_IN_ENCODE)) {
        if (cluster_read_secret(ctx.secret_file, ctx.secret, sizeof ctx.secret, &ctx.slen, err, sizeof err) != 0) {
            fprintf(stderr, "tsdb cluster: %s\n", err);
            return 1;
        }
    }

    snprintf(ctx.target, sizeof ctx.target, "%s", ctx.peer);
    int wait_ms = v->timeout_ms ? v->timeout_ms : 30000;
    for (int hop = 0;; hop++) {
        int rc = 0;
        uint8_t *reply = NULL;
        size_t len = 0;
        if (cluster_admin(ctx.target, ctx.secret, ctx.slen, ctx.op, req, rl, wait_ms, &rc, &reply, &len, err,
                          sizeof err) != 0) {
            fprintf(stderr, "tsdb cluster: %s\n", err);
            return 1;
        }
        if (rc == CLUSTER_ERR_NOTLEADER && hop == 0 && len > 0 && len < sizeof ctx.target) {
            memcpy(ctx.target, reply, len); /* follow to the leader once */
            ctx.target[len] = '\0';
            free(reply);
            continue;
        }
        if (rc != 0) {
            if ((v->flags & CL_VF_PRINT_ERR_BODY) && len) {
                if (v->nwords >= 2)
                    fprintf(stderr, "tsdb cluster %s %s: %.*s\n", v->words[0], v->words[1], (int)len,
                            (const char *)reply);
                else
                    fprintf(stderr, "tsdb cluster %s: %.*s\n", v->words[0], (int)len, (const char *)reply);
            }
            fprintf(stderr, "tsdb cluster %s: %s\n", v->words[0], cluster_strerror(rc));
            free(reply);
            return 1;
        }
        int prc = v->print(&ctx, reply, len);
        free(reply);
        return prc;
    }
}
