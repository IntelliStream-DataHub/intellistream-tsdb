/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Storage domains and their health (docs/ha.md §15.2; N1, step O2).
 *
 * A domain is what fails together: one NFS superblock, in the expected topology one export of one ZFS pool, with
 * several volumes as subdirectories. Each domain has a probe thread that does nothing but I/O on a file of the
 * domain, and a monitor thread (one for all domains) that does none: it reads the clock and what the probe
 * threads recorded, and decides. A probe thread is allowed to hang, and on a hard NFS mount it does, for as long as
 * the server is away: a thread in such a call cannot be interrupted, not even by SIGKILL (ha.md §15.1, fact 1).
 *
 *   every read_every_ms   4 KiB read with O_DIRECT from <dir>/PROBE (so the client's cache and the server's ARC
 *                         are the only caches in the way)
 *   every write_every_ms  4 KiB pwrite and fdatasync of <dir>/PROBE.<node>
 *
 * States: UP; DEGRADED (a minute of write probes slower than max(degraded_min_ms, 10 x the baseline); merges are
 * throttled, nothing is refused); DOWN (a probe has been outstanding for down_ms, or three failed in a row with
 * errors other than ENOSPC and EDQUOT: every volume is refused); RECOVERING (write and fdatasync probes have worked
 * for recover_ms, each within fast_ms, after every thread stuck on the domain has returned; read probes alone never
 * leave DOWN, because the server's ARC answers reads while writes to a suspended pool hang). The owner of the
 * volumes (tsdbd) recovers them one at a time while the domain is RECOVERING and calls tsdb_health_set_up when it
 * is done; without that (auto_up) the monitor does it. A domain that went DOWN three times in ten minutes needs 60 s
 * of good probes: it is flapping, and recovering volumes into the next outage costs more than waiting.
 *
 * ENOSPC or EDQUOT on the write probe does not take the domain down: it sets `full`, which the owner turns into
 * FULL for the domain's volumes (writes refused, reads served), until a write probe works again.
 *
 * A second probe thread starts when the first has been stuck for spawn_ms, at most two per domain, so a recovered
 * server is noticed while the first thread is still waiting for the kernel to give up its call.
 * tsdb_health_close joins the threads that are not stuck; with a stuck one it leaves the structure allocated (the
 * thread still uses it) and returns, as the process is about to exit anyway.
 *
 * Lock order: h->mu is a leaf (nothing is called with it held except clock reads; the callback runs after it).
 */
#define _GNU_SOURCE
#include "health_rules.h"
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define PROBE_BYTES 4096
#define MAX_WORKERS 2
#define LAT_N 64

typedef struct {
    int64_t at_ms;
    int ms;
} lat_sample;

struct hdomain {
    char *name;
    char *dir;
    int state;
    int64_t since_ms;  /* monotonic */
    int64_t since_wall_ms;
    uint64_t outage_id; /* incremented each time the domain leaves UP */
    bool full;
    /* probe threads: when the call in flight began (0: none) */
    int64_t inflight_ms[MAX_WORKERS];
    int64_t started_ms[MAX_WORKERS];
    bool alive[MAX_WORKERS];
    int running; /* workers that have not returned from their loop */
    int fails;   /* consecutive failed probes */
    int64_t good_since_ms; /* start of the current run of good write probes (0: none) */
    int64_t last_read_ms, last_write_ms; /* latencies of the last probes */
    uint64_t probes_ok, probes_err;
    lat_sample lat[LAT_N];
    int nlat;
    double base_ms; /* slow average of the write probe latency */
    int64_t up_since_ms;    /* the last time the state became UP: probes begun before it are no evidence of slowness */
    int64_t below_since_ms; /* DEGRADED: since when the latency is under the threshold */
    int64_t down_at[HEALTH_FLAP_DOWNS]; /* when the last DOWNs began */
    int ndown;
    int64_t slow_logged_ms; /* the last "a slow probe restarts the count" line (0: none) and the DOWN it was for */
    int slow_logged_ndown;
    bool probe_now;
    int64_t next_read_ms, next_write_ms;
    tsdb_health *h;
};

struct tsdb_health {
    tsdb_health_options o;
    char node[64];
    pthread_mutex_t mu;
    pthread_cond_t cv; /* wakes probe threads (probe now, close) and the closer */
    struct hdomain **doms;
    int ndoms;
    bool closing;
    pthread_t monitor;
    bool monitor_started;
    bool read_only;
};

static int64_t mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static int64_t wall_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

/* ------------------------------------------------------------ probe I/O */

/* Returns 0 or an errno. */
static int probe_read(const char *dir)
{
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s/PROBE", dir);
    void *buf;
    if (posix_memalign(&buf, PROBE_BYTES, PROBE_BYTES))
        return ENOMEM;
    int fd = open(path, O_RDONLY | O_DIRECT | O_CLOEXEC);
    if (fd < 0 && errno == EINVAL) /* a file system without O_DIRECT (tmpfs): the cache is all there is */
        fd = open(path, O_RDONLY | O_CLOEXEC);
    int err = 0;
    if (fd < 0)
        err = errno;
    else if (pread(fd, buf, PROBE_BYTES, 0) < 0)
        err = errno;
    if (fd >= 0)
        close(fd);
    free(buf);
    return err;
}

/* Makes PROBE if the directory has none: an existing directory of a volume, so this creates nothing implicitly. */
static int probe_make(const char *dir)
{
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s/PROBE", dir);
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0)
        return errno == EEXIST ? 0 : errno;
    char z[PROBE_BYTES] = {0};
    int err = pwrite(fd, z, sizeof z, 0) == (ssize_t)sizeof z && fdatasync(fd) == 0 ? 0 : errno;
    close(fd);
    return err;
}

static int probe_write(const char *dir, const char *node, uint64_t n)
{
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s/PROBE.%s", dir, node);
    int fd = open(path, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0)
        return errno;
    char blk[PROBE_BYTES] = {0};
    snprintf(blk, sizeof blk, "tsdb probe %llu %lld\n", (unsigned long long)n, (long long)wall_ms());
    int err = 0;
    if (pwrite(fd, blk, sizeof blk, 0) != (ssize_t)sizeof blk)
        err = errno ? errno : EIO;
    else if (fdatasync(fd) != 0)
        err = errno;
    close(fd);
    return err;
}

/* ---------------------------------------------------------- state change */

/* How long good write probes must last for the domain to leave DOWN: recover_ms, and the flapping minimum when it
 * went DOWN HEALTH_FLAP_DOWNS times within HEALTH_FLAP_WINDOW_MS. Caller holds h->mu. */
static int64_t down_need_ms(const struct hdomain *d, int64_t now)
{
    int64_t need = d->h->o.recover_ms;
    if (d->ndown >= HEALTH_FLAP_DOWNS && now - d->down_at[(d->ndown - HEALTH_FLAP_DOWNS) % HEALTH_FLAP_DOWNS] <
                                             HEALTH_FLAP_WINDOW_MS)
        need = need > HEALTH_FLAP_NEED_MS ? need : HEALTH_FLAP_NEED_MS;
    return need;
}

/* Caller holds h->mu. Returns true when the state changed (the callback is for the caller to run). */
static bool set_state(struct hdomain *d, int st, int64_t now)
{
    if (d->state == st)
        return false;
    if (st == TSDB_DOM_DOWN && d->state != TSDB_DOM_RECOVERING)
        d->outage_id++; /* an outage; a relapse from RECOVERING is the same one */
    if (st == TSDB_DOM_DOWN) {
        d->nlat = 0; /* what was measured before the outage says nothing of the storage after it */
        d->down_at[d->ndown % HEALTH_FLAP_DOWNS] = now;
        d->ndown++;
        d->good_since_ms = 0;
    }
    d->state = st;
    d->since_ms = now;
    d->since_wall_ms = wall_ms();
    d->below_since_ms = 0;
    if (st == TSDB_DOM_UP)
        d->up_since_ms = now;
    return true;
}

static void fire(tsdb_health *h, int dom, int old, int nw)
{
    if (h->o.on_change)
        h->o.on_change(h->o.ctx, dom, old, nw);
}

static int dom_index(tsdb_health *h, struct hdomain *d)
{
    for (int i = 0; i < h->ndoms; i++)
        if (h->doms[i] == d)
            return i;
    return -1;
}

/* A probe finished (any worker): records it and answers for the failure rules of DOWN. */
static void probe_done(struct hdomain *d, int slot, bool write, int err, int64_t start, int64_t now)
{
    tsdb_health *h = d->h;
    int ms = (int)(now - start);
    int old = -1, nw = -1;
    bool slow_note = false;
    int64_t slow_need_ms = 0, slow_run_ms = 0;
    pthread_mutex_lock(&h->mu);
    d->inflight_ms[slot] = 0;
    if (write)
        d->last_write_ms = ms;
    else
        d->last_read_ms = ms;
    bool space = err == ENOSPC || err == EDQUOT;
    if (err == 0 || space) {
        d->probes_ok++;
        d->fails = 0;
        if (write) {
            d->full = space;
            if (!space && ms <= h->o.fast_ms) {
                if (!d->good_since_ms)
                    d->good_since_ms = now;
            } else if (!space) {
                /* worked, but too slowly to count: the run of good probes starts over. In DOWN that is the one
                 * thing an operator cannot see from outside (a flapping domain on a slow disk stays DOWN with every
                 * probe succeeding), so the first one of a DOWN, and then one a minute at most, is logged. */
                if (d->state == TSDB_DOM_DOWN &&
                    (d->slow_logged_ndown != d->ndown || now - d->slow_logged_ms >= 60000)) {
                    d->slow_logged_ndown = d->ndown;
                    d->slow_logged_ms = now;
                    slow_note = true;
                    slow_need_ms = down_need_ms(d, now);
                    slow_run_ms = d->good_since_ms ? now - d->good_since_ms : 0;
                }
                d->good_since_ms = 0;
            }
            if (start >= d->up_since_ms && (d->state == TSDB_DOM_UP || d->state == TSDB_DOM_DEGRADED)) {
                lat_sample *s = &d->lat[d->nlat % LAT_N];
                s->at_ms = now;
                s->ms = ms;
                d->nlat++;
            }
        }
    } else {
        d->probes_err++;
        d->fails++;
        d->good_since_ms = 0;
        if (write && err == EROFS)
            d->full = false;
        if (d->fails >= 3 && d->state != TSDB_DOM_DOWN) {
            old = d->state;
            if (set_state(d, TSDB_DOM_DOWN, now))
                nw = TSDB_DOM_DOWN;
        }
    }
    pthread_mutex_unlock(&h->mu);
    if (slow_note) /* the monotonic clock in seconds, as tsdbd's own lines */
        fprintf(stderr,
                "tsdb: [%.3f] storage domain %s: a write probe took %d ms (more than %d ms): the %lld ms of good probes "
                "needed to leave DOWN start over (%lld ms had passed)\n",
                (double)now / 1000.0, d->name, ms, h->o.fast_ms, (long long)slow_need_ms, (long long)slow_run_ms);
    if (nw >= 0)
        fire(h, dom_index(h, d), old, nw);
}

/* ------------------------------------------------------------- the threads */

struct worker_arg {
    struct hdomain *d;
    int slot;
};

static void *probe_main(void *p)
{
    struct worker_arg *wa = p;
    struct hdomain *d = wa->d;
    int slot = wa->slot;
    free(wa);
    tsdb_health *h = d->h;
    uint64_t wn = 0;
    bool made = false;
    for (;;) {
        pthread_mutex_lock(&h->mu);
        int64_t now = mono_ms();
        while (!h->closing && !d->probe_now && now < d->next_read_ms && now < d->next_write_ms) {
            int64_t wake = d->next_read_ms < d->next_write_ms ? d->next_read_ms : d->next_write_ms;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            int64_t add = wake - now;
            ts.tv_sec += add / 1000;
            ts.tv_nsec += (add % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_cond_timedwait(&h->cv, &h->mu, &ts);
            now = mono_ms();
        }
        bool closing = h->closing;
        /* the older of two threads leaves once its stuck call has returned: the younger carries on */
        bool surplus = d->running > 1 && d->started_ms[slot] <= d->started_ms[1 - slot] && d->alive[1 - slot];
        bool write = !h->read_only && now >= d->next_write_ms;
        d->probe_now = false;
        if (closing || surplus) {
            d->running--;
            d->alive[slot] = false;
            pthread_cond_broadcast(&h->cv);
            pthread_mutex_unlock(&h->mu);
            return NULL;
        }
        if (write) {
            d->next_write_ms = now + h->o.write_every_ms;
            d->next_read_ms = now + h->o.read_every_ms;
        } else {
            d->next_read_ms = now + h->o.read_every_ms;
        }
        d->inflight_ms[slot] = now;
        pthread_mutex_unlock(&h->mu);

        int err;
        if (write) {
            err = made ? 0 : probe_make(d->dir);
            made = made || err == 0;
            if (!err)
                err = probe_write(d->dir, h->node, ++wn);
        } else {
            err = probe_read(d->dir);
            if (err == ENOENT && !h->read_only) {
                made = false;
                err = probe_make(d->dir);
                if (!err)
                    err = probe_read(d->dir);
            }
        }
        probe_done(d, slot, write, err, now, mono_ms());
    }
}

static int start_worker(tsdb_health *h, struct hdomain *d)
{
    int slot = d->alive[0] ? 1 : 0;
    if (d->alive[slot])
        return TSDB_ERR_INVALID;
    struct worker_arg *wa = malloc(sizeof *wa);
    if (!wa)
        return TSDB_ERR_NOMEM;
    wa->d = d;
    wa->slot = slot;
    pthread_t t;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&at, 256 * 1024);
    d->alive[slot] = true;
    d->started_ms[slot] = mono_ms();
    d->running++;
    int rc = pthread_create(&t, &at, probe_main, wa);
    pthread_attr_destroy(&at);
    if (rc) {
        d->alive[slot] = false;
        d->running--;
        free(wa);
        return TSDB_ERR_NOMEM;
    }
    return 0;
}

/* p99 of the write probes of the last minute (the slowest one: a minute holds twelve samples). */
static int lat_p99(struct hdomain *d, int64_t now, int window_ms)
{
    int worst = 0;
    int n = d->nlat < LAT_N ? d->nlat : LAT_N;
    for (int i = 0; i < n; i++)
        if (now - d->lat[i].at_ms <= window_ms && d->lat[i].ms > worst)
            worst = d->lat[i].ms;
    return worst;
}

typedef struct {
    int dom, old, nw;
} hev;

static void tick_domain(tsdb_health *h, int di, int64_t now, hev *events, int *nev)
{
    struct hdomain *d = h->doms[di];
    int old = d->state;
    /* the oldest probe in flight, and whether a thread is stuck at all */
    int64_t oldest = 0;
    bool any = false;
    for (int s = 0; s < MAX_WORKERS; s++)
        if (d->inflight_ms[s] && (!any || d->inflight_ms[s] < oldest)) {
            oldest = d->inflight_ms[s];
            any = true;
        }
    int64_t age = any ? now - oldest : 0;
    bool stuck = any && age >= h->o.fast_ms; /* a probe outstanding longer than a good one takes */

    /* a second probe thread once the first has been stuck for spawn_ms */
    if (any && age >= h->o.spawn_ms && d->running < MAX_WORKERS)
        start_worker(h, d);

    switch (d->state) {
    case TSDB_DOM_UP:
    case TSDB_DOM_DEGRADED:
        if (any && age >= h->o.down_ms) {
            set_state(d, TSDB_DOM_DOWN, now);
            break;
        }
        {
            int p99 = lat_p99(d, now, h->o.degraded_window_ms);
            double thr = d->base_ms * 10 > h->o.degraded_min_ms ? d->base_ms * 10 : h->o.degraded_min_ms;
            if (d->state == TSDB_DOM_UP && p99 > thr) {
                set_state(d, TSDB_DOM_DEGRADED, now);
            } else if (d->state == TSDB_DOM_DEGRADED) {
                if (p99 > thr)
                    d->below_since_ms = 0;
                else if (!d->below_since_ms)
                    d->below_since_ms = now;
                else if (now - d->below_since_ms >= h->o.degraded_exit_ms)
                    set_state(d, TSDB_DOM_UP, now);
            }
            if (d->state == TSDB_DOM_UP && d->nlat) {
                /* the baseline follows the median-ish latency slowly; a DEGRADED period does not move it */
                double last = d->lat[(d->nlat - 1) % LAT_N].ms;
                d->base_ms = d->base_ms == 0 ? last : d->base_ms * 0.999 + last * 0.001;
            }
        }
        break;
    case TSDB_DOM_DOWN: {
        /* flapping: HEALTH_FLAP_DOWNS DOWNs in ten minutes need a minute of good probes instead of recover_ms */
        int64_t need = down_need_ms(d, now);
        if (!stuck && d->good_since_ms && now - d->good_since_ms >= need) {
            set_state(d, TSDB_DOM_RECOVERING, now);
            d->good_since_ms = 0;
        }
        break;
    }
    case TSDB_DOM_RECOVERING:
        if (any && age >= h->o.down_ms)
            set_state(d, TSDB_DOM_DOWN, now);
        else if (h->o.auto_up)
            set_state(d, TSDB_DOM_UP, now);
        break;
    }
    if (d->state != old && *nev < 64) {
        events[(*nev)++] = (hev){di, old, d->state};
    }
}

static void *monitor_main(void *p)
{
    tsdb_health *h = p;
    for (;;) {
        struct timespec nap = {0, 50 * 1000000L};
        nanosleep(&nap, NULL);
        hev ev[64];
        int nev = 0;
        pthread_mutex_lock(&h->mu);
        if (h->closing) {
            pthread_mutex_unlock(&h->mu);
            return NULL;
        }
        int64_t now = mono_ms();
        for (int i = 0; i < h->ndoms; i++)
            tick_domain(h, i, now, ev, &nev);
        pthread_mutex_unlock(&h->mu);
        for (int i = 0; i < nev; i++)
            fire(h, ev[i].dom, ev[i].old, ev[i].nw);
    }
}

/* -------------------------------------------------------------- the API */

static void defaults(tsdb_health_options *o)
{
    if (o->down_ms <= 0)
        o->down_ms = 10000;
    if (o->read_every_ms <= 0)
        o->read_every_ms = 1000;
    if (o->write_every_ms <= 0)
        o->write_every_ms = 5000;
    if (o->recover_ms <= 0)
        o->recover_ms = 10000;
    if (o->fast_ms <= 0)
        o->fast_ms = 1000;
    if (o->spawn_ms <= 0)
        o->spawn_ms = 60000;
    if (o->degraded_min_ms <= 0)
        o->degraded_min_ms = 500;
    if (o->degraded_window_ms <= 0)
        o->degraded_window_ms = 60000;
    if (o->degraded_exit_ms <= 0)
        o->degraded_exit_ms = 120000;
}

tsdb_health *tsdb_health_open(const tsdb_health_options *opts)
{
    tsdb_health *h = calloc(1, sizeof *h);
    if (!h)
        return NULL;
    if (opts)
        h->o = *opts;
    defaults(&h->o);
    h->read_only = h->o.read_only != 0;
    if (h->o.node && *h->o.node)
        snprintf(h->node, sizeof h->node, "%s", h->o.node);
    else {
        char host[40] = "node";
        gethostname(host, sizeof host - 1);
        snprintf(h->node, sizeof h->node, "%.40s-%d", host, (int)getpid());
    }
    pthread_mutex_init(&h->mu, NULL);
    pthread_cond_init(&h->cv, NULL);
    if (pthread_create(&h->monitor, NULL, monitor_main, h) != 0) {
        free(h);
        return NULL;
    }
    h->monitor_started = true;
    return h;
}

int tsdb_health_add_domain(tsdb_health *h, const char *name, const char *dir)
{
    struct hdomain *d = calloc(1, sizeof *d);
    if (!d)
        return TSDB_ERR_NOMEM;
    d->name = strdup(name);
    d->dir = strdup(dir);
    d->h = h;
    d->state = TSDB_DOM_UP;
    int64_t now = mono_ms();
    d->since_ms = now;
    d->since_wall_ms = wall_ms();
    d->next_read_ms = now;
    d->next_write_ms = now;
    pthread_mutex_lock(&h->mu);
    struct hdomain **nd = realloc(h->doms, (size_t)(h->ndoms + 1) * sizeof *nd);
    if (!d->name || !d->dir || !nd) {
        pthread_mutex_unlock(&h->mu);
        free(d->name);
        free(d->dir);
        free(d);
        return TSDB_ERR_NOMEM;
    }
    h->doms = nd;
    h->doms[h->ndoms] = d;
    int idx = h->ndoms++;
    int rc = start_worker(h, d);
    pthread_mutex_unlock(&h->mu);
    return rc ? rc : idx;
}

int tsdb_health_domains(tsdb_health *h)
{
    pthread_mutex_lock(&h->mu);
    int n = h->ndoms;
    pthread_mutex_unlock(&h->mu);
    return n;
}

int tsdb_health_info(tsdb_health *h, int dom, tsdb_domain_info *out)
{
    pthread_mutex_lock(&h->mu);
    if (dom < 0 || dom >= h->ndoms) {
        pthread_mutex_unlock(&h->mu);
        return TSDB_ERR_NOTFOUND;
    }
    struct hdomain *d = h->doms[dom];
    int64_t now = mono_ms();
    memset(out, 0, sizeof *out);
    out->name = d->name;
    out->dir = d->dir;
    out->state = d->state;
    out->since_ms = d->since_wall_ms;
    out->age_ms = now - d->since_ms;
    out->outage_id = d->outage_id;
    out->full = d->full;
    out->read_ms = d->last_read_ms;
    out->write_ms = d->last_write_ms;
    out->probes_ok = d->probes_ok;
    out->probes_err = d->probes_err;
    out->threads = d->running;
    int64_t oldest = 0;
    for (int s = 0; s < MAX_WORKERS; s++)
        if (d->inflight_ms[s] && (!oldest || d->inflight_ms[s] < oldest))
            oldest = d->inflight_ms[s];
    out->stuck_ms = oldest ? now - oldest : 0;
    pthread_mutex_unlock(&h->mu);
    return 0;
}

int tsdb_health_state(tsdb_health *h, int dom)
{
    tsdb_domain_info in;
    return tsdb_health_info(h, dom, &in) ? -1 : in.state;
}

void tsdb_health_probe_now(tsdb_health *h, int dom)
{
    pthread_mutex_lock(&h->mu);
    if (dom >= 0 && dom < h->ndoms) {
        h->doms[dom]->probe_now = true;
        pthread_cond_broadcast(&h->cv);
    }
    pthread_mutex_unlock(&h->mu);
}

/* The owner of the volumes has recovered them all: RECOVERING -> UP. */
void tsdb_health_set_up(tsdb_health *h, int dom)
{
    int old = -1;
    pthread_mutex_lock(&h->mu);
    if (dom >= 0 && dom < h->ndoms && h->doms[dom]->state == TSDB_DOM_RECOVERING) {
        old = h->doms[dom]->state;
        set_state(h->doms[dom], TSDB_DOM_UP, mono_ms());
    }
    pthread_mutex_unlock(&h->mu);
    if (old >= 0)
        fire(h, dom, old, TSDB_DOM_UP);
}

void tsdb_health_close(tsdb_health *h)
{
    if (!h)
        return;
    pthread_mutex_lock(&h->mu);
    h->closing = true;
    pthread_cond_broadcast(&h->cv);
    pthread_mutex_unlock(&h->mu);
    if (h->monitor_started)
        pthread_join(h->monitor, NULL);
    /* wait for the probe threads that are not stuck */
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 2;
    bool stuck = false;
    pthread_mutex_lock(&h->mu);
    for (int i = 0; i < h->ndoms; i++)
        while (h->doms[i]->running > 0)
            if (pthread_cond_timedwait(&h->cv, &h->mu, &dl) != 0) {
                stuck = true;
                break;
            }
    pthread_mutex_unlock(&h->mu);
    if (stuck)
        return; /* a thread is still inside the kernel: its domain and h stay allocated */
    for (int i = 0; i < h->ndoms; i++) {
        free(h->doms[i]->name);
        free(h->doms[i]->dir);
        free(h->doms[i]);
    }
    free(h->doms);
    pthread_mutex_destroy(&h->mu);
    pthread_cond_destroy(&h->cv);
    free(h);
}
