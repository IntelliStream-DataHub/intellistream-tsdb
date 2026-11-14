/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's cluster volume manager (docs/design-cluster.md §3.1, §8.5; DECISION D2 of r3-scope/PLAN.md).
 *
 * The cluster thread pushes the replicated volume records that name this server (cluster_set_volume_watch, a copy
 * under `mu`, never blocking); this thread does the I/O. For each record:
 *
 *   1. The directory <dir>/v<N>. A new one is made whole or not at all: <dir>/v<N>.tmp (removed first if a crash
 *      left one), META (the root's format lines, volume=N, volume_uuid=, cluster=, requires=cluster,replica) and REPL
 *      (the replica instance `rid` from the record, generation 0), each fsynced, the directory fsynced, renamed to
 *      v<N>, the root fsynced. An existing one must hold the record's rid (else it is another instance of the
 *      replica: refused, naming both) and a generation at or above the record's open_gen (else it is an older copy
 *      of the directory: refused, naming both).
 *   2. The open generation: REPL's generation + 1, saved (tmp, fsync, rename, directory fsync) before it is
 *      reported. The heartbeat carries (rid, gen); the metadata leader commits REPLICA_OPEN (cluster.c,
 *      propose_opens). Until this server's copy shows open_gen >= gen the volume is declared but not open: requests
 *      get UNAVAILABLE (RECOVERING), so nothing answers, acknowledges or leads under a generation the group has not
 *      recorded.
 *   3. The engine (tsdb_cvol_open): the record's leader as a writer, every other replica as a replica
 *      (eng_open_replica). Until r3d-stream wires the replication stream a leader takes no writes: it is opened as a
 *      replica too, unless the test switch TSDB_CLUSTER_WRITES=1 (environment, read once at start) says otherwise.
 *      A replica's engine answers READONLY; tsdbd turns that into UNAVAILABLE with TSDB_WHY_FENCED (cvol_why).
 *
 * REPL is a stub (key=value lines: rid, gen) until r3c2-replfile's server/replfile.* is merged.
 * Mutants (TSDB_CVOL_MUTANT, tests only): `noopengen` opens the engine before REPLICA_OPEN commits; `samerid` draws
 * a new rid at every open instead of keeping the directory's.
 */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "cvol.h"
#include "../src/cvol.h"
#include "tsdb_protocol.h"

enum { MV_NONE = 0, MV_WAIT, MV_OPEN, MV_REFUSED };

struct mvol {
    uint32_t number;
    int state;
    int writer;   /* opened as the writer */
    uint64_t rid; /* the directory's */
    uint64_t gen; /* the generation this open saved */
    uint64_t epoch;
};

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t th;
    int started, stop, dirty, active;
    int synced; /* the manager has gone through the volume records of a metadata copy once (every one declared) */
    tsdb *t;
    char dir[4096];
    uint32_t self;
    struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS]; /* the latest push */
    uint32_t nrecs;
    struct mvol vols[CLUSTER_REPORT_VOLS]; /* what this manager made of them; under mu */
    uint32_t nvols;
    int writes;  /* TSDB_CLUSTER_WRITES=1 */
    int mutant;  /* 1 noopengen, 2 samerid */
} M = {.mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER};

static void say(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void say(const char *fmt, ...)
{
    char line[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof line, fmt, ap);
    va_end(ap);
    fprintf(stderr, "tsdbd: volumes: %s\n", line);
}

static void hex16(const uint8_t *b, char *out)
{
    for (int i = 0; i < 16; i++)
        snprintf(out + 2 * i, 3, "%02x", b[i]);
}

static int sync_dir(const char *d)
{
    int fd = open(d, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int rc = fsync(fd);
    close(fd);
    return rc;
}

/* a new file in `dir`, whole: written, fsynced; the caller syncs the directory */
static int write_new(const char *dir, const char *name, const char *txt)
{
    char p[4300];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;
    size_t len = strlen(txt);
    int rc = write(fd, txt, len) == (ssize_t)len && fsync(fd) == 0 ? 0 : -1;
    if (close(fd) != 0)
        rc = -1;
    return rc;
}

static int repl_save(const char *vdir, uint64_t rid, uint64_t gen)
{
    char txt[128];
    snprintf(txt, sizeof txt, "rid=%016" PRIx64 "\ngen=%" PRIu64 "\n", rid, gen);
    char a[4300], b[4300];
    snprintf(a, sizeof a, "%s/REPL.tmp", vdir);
    snprintf(b, sizeof b, "%s/REPL", vdir);
    if (write_new(vdir, "REPL.tmp", txt) != 0 || rename(a, b) != 0 || sync_dir(vdir) != 0)
        return -1;
    return 0;
}

/* 0, 1 when there is no REPL, -1 when it cannot be read or does not parse */
static int repl_load(const char *vdir, uint64_t *rid, uint64_t *gen)
{
    char p[4300], buf[256];
    snprintf(p, sizeof p, "%s/REPL", vdir);
    int fd = open(p, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 1 : -1;
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0)
        return -1;
    buf[n] = 0;
    char *r = strstr(buf, "rid="), *g = strstr(buf, "gen=");
    if (!r || !g)
        return -1;
    *rid = strtoull(r + 4, NULL, 16);
    *gen = strtoull(g + 4, NULL, 10);
    return 0;
}

/* the root's format lines (version, partition_ms, granule_rows, partition_classes), so every volume has its layout */
static int root_format(char *out, size_t cap)
{
    char p[4200], buf[4096];
    snprintf(p, sizeof p, "%s/META", M.dir);
    int fd = open(p, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0)
        return -1;
    buf[n] = 0;
    size_t o = 0;
    static const char *keys[] = {"version=", "partition_ms=", "granule_rows=", "partition_classes="};
    for (char *l = buf; *l;) {
        char *e = strchr(l, '\n');
        size_t ll = e ? (size_t)(e - l) : strlen(l);
        for (size_t k = 0; k < sizeof keys / sizeof keys[0]; k++)
            if (!strncmp(l, keys[k], strlen(keys[k])) && o + ll + 2 < cap) {
                memcpy(out + o, l, ll);
                o += ll;
                out[o++] = '\n';
            }
        l = e ? e + 1 : l + ll;
    }
    out[o] = 0;
    return strstr(out, "version=") ? 0 : -1;
}

/* makes <dir>/v<N> whole (module comment, step 1); 0 or -1 with a line said */
static int make_dir(const struct cluster_vol_rec *r, const char *vdir)
{
    char tmp[4300], meta[4096], u[33], c[33];
    snprintf(tmp, sizeof tmp, "%s.tmp", vdir);
    /* a crash may have left a tmp directory: its files, then itself (it holds only META, REPL and their tmps) */
    static const char *left[] = {"META", "REPL", "REPL.tmp"};
    for (size_t i = 0; i < 3; i++) {
        char f[4400];
        snprintf(f, sizeof f, "%s/%s", tmp, left[i]);
        unlink(f);
    }
    if (rmdir(tmp) != 0 && errno != ENOENT) {
        say("volume %" PRIu32 ": cannot remove the leftover %s: %s", r->number, tmp, strerror(errno));
        return -1;
    }
    if (root_format(meta, sizeof meta - 256) != 0) {
        say("volume %" PRIu32 ": cannot read the format lines of %s/META", r->number, M.dir);
        return -1;
    }
    hex16(r->uuid, u);
    hex16(r->cluster, c);
    size_t o = strlen(meta);
    snprintf(meta + o, sizeof meta - o, "volume=%" PRIu32 "\nvolume_uuid=%s\ncluster=%s\nrequires=cluster,replica\n",
             r->number, u, c);
    char rp[128];
    snprintf(rp, sizeof rp, "rid=%016" PRIx64 "\ngen=0\n", r->rid);
    if (mkdir(tmp, 0755) != 0 || write_new(tmp, "META", meta) != 0 || write_new(tmp, "REPL", rp) != 0 ||
        tsdb_cvol_fno_create(tmp) != 0 || sync_dir(tmp) != 0 || rename(tmp, vdir) != 0 || sync_dir(M.dir) != 0) {
        say("volume %" PRIu32 ": cannot make %s: %s", r->number, vdir, strerror(errno));
        return -1;
    }
    say("volume %" PRIu32 " (%s): made %s, replica %016" PRIx64, r->number, r->name, vdir, r->rid);
    return 0;
}

static struct mvol *mv_get(uint32_t number)
{
    for (uint32_t i = 0; i < M.nvols; i++)
        if (M.vols[i].number == number)
            return &M.vols[i];
    return NULL;
}

/* step 1 and 2 for a record not seen yet; the new state under mu */
static void prepare(const struct cluster_vol_rec *r)
{
    char vdir[4200];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, M.dir, r->number);
    struct mvol m = {.number = r->number, .state = MV_REFUSED, .epoch = r->epoch};
    struct stat st;
    uint64_t rid = 0, gen = 0;
    int rc = tsdb_cvol_declare(M.t, r->number, r->name);
    if (rc != 0) {
        say("volume %" PRIu32 " (%s): cannot be declared: %s", r->number, r->name, tsdb_strerror(rc));
        goto out;
    }
    if (stat(vdir, &st) != 0) {
        if (errno != ENOENT || make_dir(r, vdir) != 0)
            goto out;
    }
    int lr = repl_load(vdir, &rid, &gen);
    if (lr != 0) {
        say("volume %" PRIu32 ": %s/REPL %s: the volume is not opened", r->number, vdir,
            lr > 0 ? "is missing" : "cannot be read");
        goto out;
    }
    if (M.mutant == 2) /* samerid: a new instance at every open */
        rid = r->rid ^ (uint64_t)time(NULL) ^ ((uint64_t)getpid() << 32);
    if (rid != r->rid) {
        say("volume %" PRIu32 ": %s holds replica %016" PRIx64 ", the metadata group names %016" PRIx64
            " for this server: another instance of the directory, not opened",
            r->number, vdir, rid, r->rid);
        if (M.mutant != 2)
            goto out;
    }
    if (gen < r->open_gen) {
        say("volume %" PRIu32 ": %s is at open generation %" PRIu64 ", below the %" PRIu64
            " the metadata group recorded: an older copy of the directory, not opened",
            r->number, vdir, gen, r->open_gen);
        goto out;
    }
    if (repl_save(vdir, rid, gen + 1) != 0) {
        say("volume %" PRIu32 ": cannot save open generation %" PRIu64 " in %s/REPL: %s", r->number, gen + 1, vdir,
            strerror(errno));
        goto out;
    }
    m.rid = rid;
    m.gen = gen + 1;
    m.state = MV_WAIT;
    say("volume %" PRIu32 " (%s): open generation %" PRIu64 ", waiting for the metadata group to record it",
        r->number, r->name, m.gen);
out:
    pthread_mutex_lock(&M.mu);
    if (M.nvols < CLUSTER_REPORT_VOLS)
        M.vols[M.nvols++] = m;
    pthread_mutex_unlock(&M.mu);
}

/* step 3 once the generation is recorded */
static void open_engine(const struct cluster_vol_rec *r, struct mvol *m)
{
    char vdir[4200];
    snprintf(vdir, sizeof vdir, "%s/v%" PRIu32, M.dir, r->number);
    int writer = r->leader == M.self && M.writes;
    int rc = tsdb_cvol_open(M.t, r->number, vdir, !writer, 0);
    pthread_mutex_lock(&M.mu);
    if (rc == 0) {
        m->state = MV_OPEN;
        m->writer = writer;
    } else {
        m->state = MV_REFUSED;
    }
    pthread_mutex_unlock(&M.mu);
    if (rc == 0)
        say("volume %" PRIu32 " (%s): open as the %s, generation %" PRIu64, r->number, r->name,
            writer ? "writer" : r->leader == M.self ? "leader (writes off until replication: a replica)" : "replica",
            m->gen);
    else
        say("volume %" PRIu32 " (%s): the open of %s failed: %s", r->number, r->name, vdir, tsdb_strerror(rc));
}

static void *mgr_main(void *arg)
{
    (void)arg;
    static struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS];
    pthread_mutex_lock(&M.mu);
    for (;;) {
        while (!M.dirty && !M.stop)
            pthread_cond_wait(&M.cv, &M.mu);
        if (M.stop)
            break;
        M.dirty = 0;
        uint32_t n = M.nrecs;
        memcpy(recs, M.recs, n * sizeof recs[0]);
        pthread_mutex_unlock(&M.mu);
        for (uint32_t i = 0; i < n && !M.stop; i++) {
            const struct cluster_vol_rec *r = &recs[i];
            pthread_mutex_lock(&M.mu);
            struct mvol *m = mv_get(r->number);
            int st = m ? m->state : MV_NONE;
            if (m)
                m->epoch = r->epoch;
            pthread_mutex_unlock(&M.mu);
            if (st == MV_NONE)
                prepare(r);
            pthread_mutex_lock(&M.mu);
            m = mv_get(r->number);
            int ready = m && m->state == MV_WAIT && (r->open_gen >= m->gen || M.mutant == 1);
            pthread_mutex_unlock(&M.mu);
            if (ready)
                open_engine(r, m);
        }
        pthread_mutex_lock(&M.mu);
        M.synced = 1;
    }
    pthread_mutex_unlock(&M.mu);
    return NULL;
}

static void on_watch(void *arg, uint32_t self, const struct cluster_vol_rec *v, uint32_t n)
{
    (void)arg;
    pthread_mutex_lock(&M.mu);
    M.self = self;
    if (n > CLUSTER_REPORT_VOLS)
        n = CLUSTER_REPORT_VOLS;
    memcpy(M.recs, v, n * sizeof *v);
    M.nrecs = n;
    M.dirty = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
}

static void on_report(void *arg, struct cluster_report *out)
{
    (void)arg;
    pthread_mutex_lock(&M.mu);
    for (uint32_t i = 0; i < M.nvols && out->nvols < CLUSTER_REPORT_VOLS; i++) {
        const struct mvol *m = &M.vols[i];
        if (m->state != MV_WAIT && m->state != MV_OPEN)
            continue;
        struct cluster_vol_report *v = &out->vols[out->nvols++];
        v->volume = m->number;
        v->role = m->state == MV_OPEN && m->writer ? CL_ROLE_OWNER : CL_ROLE_NONE;
        v->health = CL_HEALTH_OK;
        v->epoch = m->epoch;
        v->rid = m->rid;
        v->open_gen = m->gen;
    }
    pthread_mutex_unlock(&M.mu);
}

int cvol_why(uint32_t v)
{
    if (!M.active)
        return -1;
    int why = -1;
    pthread_mutex_lock(&M.mu);
    const struct mvol *m = mv_get(v);
    if (m)
        why = m->state == MV_OPEN ? (m->writer ? -1 : TSDB_WHY_FENCED) : TSDB_WHY_RECOVERING;
    pthread_mutex_unlock(&M.mu);
    return why;
}

int cvol_active(void) { return M.active; }

int cvol_synced(void)
{
    pthread_mutex_lock(&M.mu);
    int s = M.synced;
    pthread_mutex_unlock(&M.mu);
    return s;
}

int cvol_start(tsdb *t, const char *dir)
{
    M.t = t;
    snprintf(M.dir, sizeof M.dir, "%s", dir);
    const char *w = getenv("TSDB_CLUSTER_WRITES");
    M.writes = w && !strcmp(w, "1");
    const char *mu = getenv("TSDB_CVOL_MUTANT");
    M.mutant = !mu ? 0 : !strcmp(mu, "noopengen") ? 1 : !strcmp(mu, "samerid") ? 2 : 0;
    if (M.writes)
        say("TSDB_CLUSTER_WRITES=1: a volume's leader takes writes before replication is wired (tests only)");
    if (M.mutant)
        say("MUTANT %s (tests only)", mu);
    if (pthread_create(&M.th, NULL, mgr_main, NULL) != 0) {
        say("cannot start the volume manager thread");
        return -1;
    }
    M.started = 1;
    M.active = 1;
    cluster_set_volume_watch(on_watch, NULL);
    cluster_set_reporter(on_report, NULL);
    return 0;
}

void cvol_stop(void)
{
    if (!M.started)
        return;
    pthread_mutex_lock(&M.mu);
    M.stop = 1;
    pthread_cond_signal(&M.cv);
    pthread_mutex_unlock(&M.mu);
    pthread_join(M.th, NULL);
    M.started = 0;
}
