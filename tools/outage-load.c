/* outage-load: a steady writer per series and a read-back check, for the manual NFS outage runs
 * (docs/tasks/nfs-outage.md). It is the small cousin of tests/test_outage.c, for a real tsdbd on real storage.
 *
 *   outage-load run --port P --series NAME[:VOLUME-PATH]... --ledger FILE [--seconds S] [--interval-ms MS]
 *                   [--user U --password-file F]
 *       One thread per series writes one row every MS (default 10): ts = the wall clock in ms, raised to keep it
 *       strictly increasing, value = the ts. Every 10 s it prints, per series, the rows acknowledged, refused with
 *       UNAVAILABLE (definitely not applied), indeterminate (any other error) and the slowest answer in ms. The
 *       ledger gets "NAME S TS" before each call and "NAME A|R|M TS" after it (acknowledged, refused, maybe), so
 *       that the check can judge a run whose writer was killed: a call without an answer is a maybe.
 *   outage-load check --port P --ledger FILE [--user U --password-file F]
 *       Reads every series of the ledger back: an acknowledged row must be there, a refused row must not be.
 *       A maybe may be either. Exit status 0 when nothing is wrong, 1 otherwise.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"

#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAXS 64

static const char *host = "127.0.0.1";
static int port;
static const char *user, *pwfile;
static atomic_int stop;
static FILE *ledger;
static pthread_mutex_t ledger_mu = PTHREAD_MUTEX_INITIALIZER;

static double now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (double)t.tv_sec * 1000.0 + (double)t.tv_nsec / 1e6;
}

static void msleep(int ms)
{
    struct timespec t = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&t, NULL);
}

/* What this tool records is what the server answers, so the client's own retry of UNAVAILABLE is off. */
static int connect_c(tsdbc **c)
{
    int rc;
    if (user) {
        tsdbc_options o = {.user = user, .password_file = pwfile};
        rc = tsdbc_connect_opts(host, port, &o, c);
    } else {
        rc = tsdbc_connect_timeout(host, port, 3000, c);
    }
    if (rc == 0)
        tsdbc_set_retry(*c, 0);
    return rc;
}

static void note(const char *name, char what, int64_t ts)
{
    pthread_mutex_lock(&ledger_mu);
    fprintf(ledger, "%s %c %lld\n", name, what, (long long)ts);
    pthread_mutex_unlock(&ledger_mu);
}

typedef struct {
    char name[256], path[PATH_MAX];
    int interval_ms;
    _Atomic long acked, refused, maybe, worst_ms;
} writer_t;

static void *writer_main(void *arg)
{
    writer_t *w = arg;
    tsdbc *c = NULL;
    tsdb_sid id = 0;
    int64_t last = 0;
    while (!atomic_load(&stop)) {
        if (!c) {
            if (connect_c(&c) != 0) {
                c = NULL;
                msleep(500);
                continue;
            }
        }
        if (!id) {
            int rc = tsdbc_series_at(c, w->name, w->path[0] ? w->path : NULL, &id);
            if (rc != 0) {
                id = 0;
                if (rc == TSDB_ERR_IO) {
                    tsdbc_close(c);
                    c = NULL;
                }
                msleep(500);
                continue;
            }
        }
        int64_t ts = (int64_t)now_ms();
        if (ts <= last)
            ts = last + 1;
        last = ts;
        float v = (float)(ts % 1000000);
        note(w->name, 'S', ts);
        double t0 = now_ms();
        int rc = tsdbc_insert_series(c, id, &ts, &v, 1);
        long dt = (long)(now_ms() - t0);
        if (dt > atomic_load(&w->worst_ms))
            atomic_store(&w->worst_ms, dt);
        if (rc == 0) {
            note(w->name, 'A', ts);
            atomic_fetch_add(&w->acked, 1);
        } else if (rc == TSDB_ERR_UNAVAILABLE) {
            note(w->name, 'R', ts);
            atomic_fetch_add(&w->refused, 1);
            msleep(100);
        } else {
            note(w->name, 'M', ts);
            atomic_fetch_add(&w->maybe, 1);
            tsdbc_close(c); /* the connection may be gone or out of step: start a new one */
            c = NULL;
            msleep(100);
        }
        msleep(w->interval_ms);
    }
    if (c)
        tsdbc_close(c);
    return NULL;
}

static int run(int nser, char **specs, double seconds, int interval_ms)
{
    writer_t *w = calloc((size_t)nser, sizeof *w);
    pthread_t th[MAXS];
    for (int i = 0; i < nser; i++) {
        char *colon = strchr(specs[i], ':');
        snprintf(w[i].name, sizeof w[i].name, "%.*s", colon ? (int)(colon - specs[i]) : (int)strlen(specs[i]), specs[i]);
        if (colon)
            snprintf(w[i].path, sizeof w[i].path, "%s", colon + 1);
        w[i].interval_ms = interval_ms;
        pthread_create(&th[i], NULL, writer_main, &w[i]);
    }
    double t0 = now_ms(), next = t0 + 10000;
    while (now_ms() - t0 < seconds * 1000) {
        msleep(200);
        if (now_ms() >= next) {
            next += 10000;
            pthread_mutex_lock(&ledger_mu);
            fflush(ledger);
            pthread_mutex_unlock(&ledger_mu);
            for (int i = 0; i < nser; i++)
                printf("%6.0fs %-24s acked %ld refused %ld maybe %ld worst %ld ms\n", (now_ms() - t0) / 1000, w[i].name,
                       atomic_load(&w[i].acked), atomic_load(&w[i].refused), atomic_load(&w[i].maybe),
                       atomic_exchange(&w[i].worst_ms, 0));
            fflush(stdout);
        }
    }
    atomic_store(&stop, 1);
    for (int i = 0; i < nser; i++)
        pthread_join(th[i], NULL);
    fflush(ledger);
    return 0;
}

/* ------------------------------------------------------------------ check */

typedef struct {
    int64_t ts;
    char st; /* S, A, R or M */
} ent;

typedef struct {
    char name[256];
    ent *e;
    size_t n, cap;
} ser;

static ser sers[MAXS];
static size_t nsers;

static ser *find_ser(const char *name)
{
    for (size_t i = 0; i < nsers; i++)
        if (strcmp(sers[i].name, name) == 0)
            return &sers[i];
    if (nsers == MAXS)
        return NULL;
    snprintf(sers[nsers].name, sizeof sers[nsers].name, "%s", name);
    return &sers[nsers++];
}

typedef struct {
    ser *s;
    size_t i; /* next ledger entry */
    long acked_missing, refused_present, rows;
} judge;

static int judge_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    judge *j = ctx;
    (void)v;
    for (size_t k = 0; k < n; k++) {
        j->rows++;
        while (j->i < j->s->n && j->s->e[j->i].ts < ts[k]) {
            if (j->s->e[j->i].st == 'A')
                j->acked_missing++;
            j->i++;
        }
        if (j->i < j->s->n && j->s->e[j->i].ts == ts[k]) {
            if (j->s->e[j->i].st == 'R')
                j->refused_present++;
            j->i++;
        }
    }
    return 0;
}

static int check(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        perror(path);
        return 1;
    }
    char name[256], st;
    long long ts;
    while (fscanf(f, "%255s %c %lld", name, &st, &ts) == 3) {
        ser *s = find_ser(name);
        if (!s)
            continue;
        if (st != 'S' && s->n && s->e[s->n - 1].ts == ts && s->e[s->n - 1].st == 'S') {
            s->e[s->n - 1].st = st;
            continue;
        }
        if (s->n == s->cap) {
            s->cap = s->cap ? s->cap * 2 : 1024;
            s->e = realloc(s->e, s->cap * sizeof *s->e);
        }
        s->e[s->n++] = (ent){ts, st}; /* an S left without an answer counts as a maybe */
    }
    fclose(f);
    tsdbc *c;
    if (connect_c(&c) != 0) {
        fprintf(stderr, "cannot connect to %s:%d\n", host, port);
        return 1;
    }
    int bad = 0;
    for (size_t i = 0; i < nsers; i++) {
        tsdb_sid id;
        int rc = tsdbc_series(c, sers[i].name, 0, &id);
        if (rc != 0) {
            printf("%s: lookup failed: %s\n", sers[i].name, tsdbc_strerror(rc));
            bad = 1;
            continue;
        }
        judge j = {.s = &sers[i]};
        rc = tsdbc_query_raw(c, id, 0, INT64_MAX / 2, judge_cb, &j);
        while (rc == 0 && j.i < j.s->n) { /* the ledger's rows after the last row read back */
            if (j.s->e[j.i].st == 'A')
                j.acked_missing++;
            j.i++;
        }
        size_t na = 0, nr = 0, nm = 0;
        for (size_t k = 0; k < sers[i].n; k++)
            switch (sers[i].e[k].st) {
            case 'A': na++; break;
            case 'R': nr++; break;
            default: nm++;
            }
        printf("%s: %zu acknowledged, %zu refused, %zu maybe in the ledger; %ld rows read back: %ld acknowledged "
               "rows missing, %ld refused rows present%s\n",
               sers[i].name, na, nr, nm, j.rows, j.acked_missing, j.refused_present, rc ? " (QUERY FAILED)" : "");
        if (rc || j.acked_missing || j.refused_present)
            bad = 1;
    }
    tsdbc_close(c);
    puts(bad ? "FAILED" : "ok");
    return bad;
}

int main(int argc, char **argv)
{
    if (argc < 2 || (strcmp(argv[1], "run") && strcmp(argv[1], "check"))) {
        fprintf(stderr, "usage: outage-load run|check --port P [--series NAME[:VOLUME]]... --ledger FILE "
                        "[--seconds S] [--interval-ms MS] [--user U --password-file F]\n");
        return 2;
    }
    int is_run = strcmp(argv[1], "run") == 0;
    static struct option opts[] = {{"port", 1, 0, 'p'},     {"series", 1, 0, 's'},   {"ledger", 1, 0, 'l'},
                                   {"seconds", 1, 0, 't'},  {"interval-ms", 1, 0, 'i'}, {"user", 1, 0, 'u'},
                                   {"password-file", 1, 0, 'f'}, {"host", 1, 0, 'h'}, {0, 0, 0, 0}};
    char *specs[MAXS];
    int nspec = 0, interval = 10;
    double seconds = 3600;
    const char *lpath = NULL;
    argv++, argc--;
    int o;
    while ((o = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (o) {
        case 'p': port = atoi(optarg); break;
        case 's':
            if (nspec == MAXS)
                return 2;
            specs[nspec++] = optarg;
            break;
        case 'l': lpath = optarg; break;
        case 't': seconds = atof(optarg); break;
        case 'i': interval = atoi(optarg); break;
        case 'u': user = optarg; break;
        case 'f': pwfile = optarg; break;
        case 'h': host = optarg; break;
        default: return 2;
        }
    }
    if (!port || !lpath || (is_run && !nspec)) {
        fprintf(stderr, "outage-load: --port, --ledger and (for run) --series are required\n");
        return 2;
    }
    if (!is_run)
        return check(lpath);
    ledger = fopen(lpath, "a");
    if (!ledger) {
        perror(lpath);
        return 1;
    }
    return run(nspec, specs, seconds, interval);
}
