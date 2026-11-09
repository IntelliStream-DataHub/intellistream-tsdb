/* SPDX-License-Identifier: AGPL-3.0-only */
/* Storage domain health (src/health.c, ha.md 15.2): probes, the states and what moves between them, against the
 * outagefs shim. Three domains, A and A2 (two filesystems of one server) and B (another server). The thresholds are
 * scaled down so the whole test takes about 30 s. Usage: test_health [scratch directory]. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "outagefs_ctl.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>

static int failures;
#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                        \
        }                                                                      \
    } while (0)

static char ctl[256];
static tsdb_health *h;
static int dom_a, dom_a2, dom_b;

static atomic_int changes;
static pthread_mutex_t lg_mu = PTHREAD_MUTEX_INITIALIZER;
static char lg[8192];

static void on_change(void *ctx, int dom, int old, int nw)
{
    (void)ctx;
    atomic_fetch_add(&changes, 1);
    pthread_mutex_lock(&lg_mu);
    size_t l = strlen(lg);
    snprintf(lg + l, sizeof lg - l, "%d:%d>%d ", dom, old, nw);
    pthread_mutex_unlock(&lg_mu);
}

static int cmd(const char *verb, const char *dir, const char *arg)
{
    char line[600];
    snprintf(line, sizeof line, "%s %s%s%s", verb, dir, arg ? " " : "", arg ? arg : "");
    return outagefs_cmd(ctl, line, NULL, 0);
}

static void nap(int ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

/* waits up to ms for a domain to be in a state */
static bool wait_state(int dom, int st, int ms)
{
    for (int t = 0; t < ms; t += 20) {
        if (tsdb_health_state(h, dom) == st)
            return true;
        nap(20);
    }
    return tsdb_health_state(h, dom) == st;
}

static bool stays(int dom, int st, int ms)
{
    for (int t = 0; t < ms; t += 20) {
        if (tsdb_health_state(h, dom) != st)
            return false;
        nap(20);
    }
    return true;
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    const char *const names[] = {"a", "a2", "b"};
    if (outagefs_reexec(argv, base, names, 3))
        return 1;
    snprintf(ctl, sizeof ctl, "%s", getenv("OUTAGEFS_CTL"));
    const char *da = outagefs_dir("a"), *da2 = outagefs_dir("a2"), *db = outagefs_dir("b");

    tsdb_health_options o = {.down_ms = 1500,
                             .read_every_ms = 100,
                             .write_every_ms = 200,
                             .recover_ms = 1000,
                             .fast_ms = 500,
                             .spawn_ms = 2500,
                             .degraded_min_ms = 300,
                             .degraded_window_ms = 1000,
                             .degraded_exit_ms = 1500,
                             .auto_up = 1,
                             .node = "t1",
                             .on_change = on_change};
    h = tsdb_health_open(&o);
    CHECK(h != NULL);
    dom_a = tsdb_health_add_domain(h, "a", da);
    dom_a2 = tsdb_health_add_domain(h, "a2", da2);
    dom_b = tsdb_health_add_domain(h, "b", db);
    CHECK(dom_a == 0 && dom_a2 == 1 && dom_b == 2 && tsdb_health_domains(h) == 3);

    /* everything UP, probe files made, probes counted */
    nap(1200);
    for (int d = 0; d < 3; d++) {
        tsdb_domain_info in;
        CHECK(tsdb_health_info(h, d, &in) == 0);
        CHECK(in.state == TSDB_DOM_UP);
        CHECK(in.probes_ok >= 5 && in.probes_err == 0);
        CHECK(in.threads == 1);
    }
    char p[512];
    snprintf(p, sizeof p, "%s/PROBE", da);
    CHECK(access(p, F_OK) == 0);
    snprintf(p, sizeof p, "%s/PROBE.t1", da);
    CHECK(access(p, F_OK) == 0);
    CHECK(atomic_load(&changes) == 0);

    /* hang: only A goes DOWN, after about down_ms; its sibling and the other server stay UP */
    CHECK(cmd("hang", da, NULL) == 0);
    CHECK(stays(dom_a, TSDB_DOM_UP, 1000)); /* not yet: a probe has been out for 1 s of the 1.5 */
    CHECK(wait_state(dom_a, TSDB_DOM_DOWN, 2000));
    CHECK(tsdb_health_state(h, dom_a2) == TSDB_DOM_UP && tsdb_health_state(h, dom_b) == TSDB_DOM_UP);
    tsdb_domain_info in;
    CHECK(tsdb_health_info(h, dom_a, &in) == 0 && in.outage_id == 1 && in.stuck_ms > 1000);
    /* stuck past spawn_ms: a second probe thread */
    nap(1500);
    CHECK(tsdb_health_info(h, dom_a, &in) == 0 && in.threads == 2);
    CHECK(tsdb_health_state(h, dom_a) == TSDB_DOM_DOWN);
    /* heal: the stuck threads return, write probes work for recover_ms, then UP (auto_up) */
    CHECK(cmd("heal", da, NULL) == 0);
    CHECK(wait_state(dom_a, TSDB_DOM_RECOVERING, 2500) || tsdb_health_state(h, dom_a) == TSDB_DOM_UP);
    CHECK(wait_state(dom_a, TSDB_DOM_UP, 2000));
    nap(300);
    CHECK(tsdb_health_info(h, dom_a, &in) == 0 && in.threads == 1);
    CHECK(in.age_ms < 5000 && in.outage_id == 1);

    /* a read-only heal is not enough: with writes failing, the domain does not leave DOWN (reads would pass) */
    CHECK(cmd("eio", da, NULL) == 0);
    CHECK(wait_state(dom_a, TSDB_DOM_DOWN, 2500)); /* three failed probes */
    CHECK(tsdb_health_info(h, dom_a, &in) == 0 && in.outage_id == 2 && in.probes_err >= 3);
    CHECK(cmd("heal", da, NULL) == 0);
    CHECK(wait_state(dom_a, TSDB_DOM_UP, 4000));

    /* full: ENOSPC on the write probe sets `full` and nothing else */
    CHECK(cmd("enospc", db, NULL) == 0);
    nap(600);
    CHECK(tsdb_health_info(h, dom_b, &in) == 0 && in.full == 1 && in.state == TSDB_DOM_UP);
    CHECK(cmd("heal", db, NULL) == 0);
    nap(600);
    CHECK(tsdb_health_info(h, dom_b, &in) == 0 && in.full == 0 && in.state == TSDB_DOM_UP);

    /* slow: DEGRADED, nothing is refused (not DOWN); leaves after degraded_exit_ms under the threshold */
    CHECK(cmd("slow", da2, "400") == 0);
    CHECK(wait_state(dom_a2, TSDB_DOM_DEGRADED, 3000));
    CHECK(stays(dom_a2, TSDB_DOM_DEGRADED, 800));
    CHECK(tsdb_health_state(h, dom_a) == TSDB_DOM_UP);
    CHECK(cmd("heal", da2, NULL) == 0);
    CHECK(stays(dom_a2, TSDB_DOM_DEGRADED, 500)); /* hysteresis: not at once */
    CHECK(wait_state(dom_a2, TSDB_DOM_UP, 4500));

    /* the callback saw the transitions of the domains, and B's none */
    pthread_mutex_lock(&lg_mu);
    CHECK(strstr(lg, "0:0>2") && strstr(lg, "0:2>3") && strstr(lg, "0:3>0"));
    CHECK(strstr(lg, "1:0>1") && strstr(lg, "1:1>0"));
    CHECK(!strstr(lg, "2:"));
    pthread_mutex_unlock(&lg_mu);

    /* a manual owner: with auto_up off, RECOVERING stays until tsdb_health_set_up */
    tsdb_health_close(h);
    o.auto_up = 0;
    o.on_change = NULL;
    h = tsdb_health_open(&o);
    int d0 = tsdb_health_add_domain(h, "b", db);
    CHECK(cmd("eio", db, NULL) == 0);
    CHECK(wait_state(d0, TSDB_DOM_DOWN, 2500));
    CHECK(cmd("heal", db, NULL) == 0);
    CHECK(wait_state(d0, TSDB_DOM_RECOVERING, 4000));
    CHECK(stays(d0, TSDB_DOM_RECOVERING, 1500));
    tsdb_health_set_up(h, d0);
    CHECK(tsdb_health_state(h, d0) == TSDB_DOM_UP);
    tsdb_health_close(h);

    /* read-only: no write probe, no files made */
    char ro[300];
    snprintf(ro, sizeof ro, "%s/ro", db);
    mkdir(ro, 0755);
    o.read_only = 1;
    h = tsdb_health_open(&o);
    int dr = tsdb_health_add_domain(h, "ro", ro);
    nap(600);
    CHECK(tsdb_health_state(h, dr) == TSDB_DOM_DOWN || tsdb_health_state(h, dr) == TSDB_DOM_UP);
    snprintf(p, sizeof p, "%s/PROBE.t1", ro);
    CHECK(access(p, F_OK) != 0);
    tsdb_health_close(h);

    if (failures)
        fprintf(stderr, "test_health: %d failures (log: %s)\n", failures, lg);
    else
        printf("test_health: ok\n");
    return failures ? 1 : 0;
}
