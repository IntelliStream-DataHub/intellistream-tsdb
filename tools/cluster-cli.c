/*
 * `tsdb cluster ...`: the admin ops of tsdbd's cluster thread (server/cluster.c), sent to a server's peer address.
 * No directory: the CLI holds no cluster state; it authenticates with the cluster secret (--secret-file) and prints
 * what the server answers.
 *
 *   tsdb cluster init PEER --secret-file F [--replication 3] [--min-isr 1] [--node-timeout-ms 3000]
 *        [--heartbeat-ms 500] [--isr-lag-ms 1000] [--out-after-s 1800] [--voter-replace-s 600] [--voters 5]
 *        [--volumes-per-server 16]
 *   tsdb cluster node add PEER NEW_PEER --secret-file F      (PEER: any member; the CLI follows to the leader once)
 *   tsdb cluster status PEER --secret-file F                 (one line per fact, for people and scripts)
 *   tsdb cluster secret FILE                                 (a new secret: 32 random bytes as hex, mode 0600)
 */
#include "cluster.h"
#include "meta.h"
#include "raft/raft.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int cluster_cli(int argc, char **argv); /* tools/tsdb-cli.c */

/* The one text of the cluster commands: `tsdb cluster` (or help, --help) prints it on stdout and exits 0; a mistake in
 * a command prints the same on stderr and exits 2. docs/cluster-preview.md has the longer story. */
static const char cl_help[] =
    "tsdb cluster: the commands of tsdbd's cluster (a preview: the metadata group only, no data replication and\n"
    "no failover yet; docs/cluster-preview.md is the tutorial).\n"
    "\n"
    "usage: tsdb cluster secret FILE\n"
    "       tsdb cluster init PEER --secret-file F [options]\n"
    "       tsdb cluster node add PEER NEW_PEER --secret-file F\n"
    "       tsdb cluster status PEER --secret-file F\n"
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
    "  status PEER         what that server knows: its role, the leader, the settings, every member (up or down,\n"
    "                      incarnation, when it was last heard), the volumes and users. One fact a line.\n"
    "\n"
    "  init options (each takes a whole number above 0; defaults in brackets):\n"
    "    --replication N         copies of every volume the cluster will keep [3]\n"
    "    --min-isr N             copies that must be in sync for a write to count, at most --replication [1]\n"
    "    --node-timeout-ms MS    a server is marked down after this long without a heartbeat [3000]\n"
    "    --heartbeat-ms MS       how often servers report, below --node-timeout-ms [500]\n"
    "    --isr-lag-ms MS         a copy this far behind leaves the in-sync set [1000]\n"
    "    --out-after-s S         a server down this long is marked out [1800]\n"
    "    --voter-replace-s S     a voter down this long is replaced by another member [600]\n"
    "    --voters N              the most voters the group has (an even number counts as one less) [5]\n"
    "    --volumes-per-server N  volumes led per server at init [16]\n"
    "    --mode replicated|shared    shared-nothing copies, or shared (NFS) volumes [replicated]\n"
    "    --failover lease|attested|bmc   how a dead owner is replaced in shared mode [lease]\n"
    "    --read-barrier on|off   owners answer reads only behind a fenced write [on]\n"
    "    --lease-s S             the lease of a shared volume [10]\n"
    "  In this preview the settings are stored and shown by status; nothing acts on the replication, failover and\n"
    "  volume ones yet.\n";

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

static const char *role_name(int r)
{
    static const char *n[] = {"follower", "pre-candidate", "candidate", "leader"};
    return r >= 0 && r < 4 ? n[r] : "?";
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
    else
        printf("cluster %s (not initialised)\n", u);
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
               " volumes_ok %d core_dropped %d read_barrier %s\n",
               mb->id, meta_member_state_name(mb->state), mb->incarnation, mb->addr, mb->peer,
               mb->rack[0] ? mb->rack : "-", heard, mb->power_feed[0] ? mb->power_feed : "-",
               !!(mb->flags & META_MF_VOTER_OK), !!(mb->flags & META_MF_VOLUMES_OK), h ? h->core_dropped : 0,
               !h || (!h->flags && !h->state_free) ? "?" : h->read_barrier ? "on" : "off");
        for (uint32_t j = 0; h && j < h->nvols; j++) {
            const struct cluster_vol_status *v = &h->vols[j];
            printf("volume-report node %" PRIu32 " volume %" PRIu32 " role %s health %s epoch %" PRIu64
                   " domain %s local_fault_s %" PRIu64 " fenced_age_ms %" PRIu64 "\n",
                   mb->id, v->v.volume, v->v.role == CL_ROLE_OWNER ? "owner" : v->v.role == CL_ROLE_RELEASING ? "releasing" : "none",
                   v->v.health == CL_HEALTH_OK ? "ok" : v->v.health == CL_HEALTH_LOCAL_FAULT ? "local-fault" : "domain-fault",
                   v->v.epoch, v->v.domain[0] ? v->v.domain : "-", v->local_fault_s, v->v.fenced_age_ms);
        }
        for (uint32_t j = 0; h && j < h->ndoms; j++)
            printf("domain-report node %" PRIu32 " %s %s\n", mb->id, h->doms[j].name,
                   h->doms[j].state == CL_DOM_OK ? "ok" : h->doms[j].state == CL_DOM_SLOW ? "slow" : "fault");
    }
    printf("volumes %" PRIu32 " users %" PRIu32 "\n", m.nvolumes, m.nusers);
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
    int i = 2;
    if (!strcmp(verb, "node")) {
        if (argc < 5 || strcmp(argv[2], "add") != 0)
            return cl_usage();
        addr = argv[3];
        newpeer = argv[4];
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
    struct {
        const char *flag;
        uint32_t *v;
    } nums[] = {{"--replication", &s.replication},
                {"--min-isr", &s.min_isr},
                {"--node-timeout-ms", &s.node_timeout_ms},
                {"--heartbeat-ms", &s.heartbeat_ms},
                {"--isr-lag-ms", &s.isr_lag_ms},
                {"--out-after-s", &s.out_after_s},
                {"--voter-replace-s", &s.voter_replace_s},
                {"--voters", &s.voters},
                {"--volumes-per-server", &s.volumes_per_server},
                {"--lease-s", &s.lease_s}};
    for (; i < argc; i++) {
        if (!strcmp(argv[i], "--secret-file") && i + 1 < argc) {
            secret_file = argv[++i];
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
        if (!strcmp(verb, "init") && i + 1 < argc && !strcmp(argv[i], "--read-barrier")) {
            if (strcmp(argv[i + 1], "on") && strcmp(argv[i + 1], "off"))
                return cl_usage();
            s.read_barrier = !strcmp(argv[i + 1], "on");
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
    if (!secret_file) {
        fprintf(stderr, "tsdb cluster: --secret-file is required (the cluster's secret, as tsdbd's "
                        "--cluster-secret-file)\n");
        return 2;
    }
    if (s.heartbeat_ms >= s.node_timeout_ms || s.min_isr > s.replication) {
        fprintf(stderr, "tsdb cluster: --heartbeat-ms must be below --node-timeout-ms, --min-isr at most "
                        "--replication\n");
        return 2;
    }
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
    } else {
        op = CLUSTER_OP_STATUS;
    }
    char target[META_ADDR_MAX + 1];
    snprintf(target, sizeof target, "%s", addr);
    for (int hop = 0;; hop++) {
        int rc;
        uint8_t *reply = NULL;
        size_t len = 0;
        if (cluster_admin(target, secret, slen, op, req, rl, 30000, &rc, &reply, &len, err, sizeof err) != 0) {
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
        } else if (op == CLUSTER_OP_INIT) {
            printf("initialised: node 1 at %s\n", target);
        } else {
            printf("added %s\n", newpeer);
        }
        free(reply);
        return 0;
    }
}
