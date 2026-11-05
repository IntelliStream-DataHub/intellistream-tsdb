/*
 * The workloads of tools/auth-bench.sh that tsdbc-bench does not have: inserts of a chosen shape, logins per second,
 * and (with a login) grants in bulk.
 *
 *   auth-bench-client insert series|rows [--conns 1] [--seconds 3] [--rows 1000] [--pool 1000]
 *   auth-bench-client logins [--conns 1] [--seconds 3] [--cold]
 *   auth-bench-client grant --grantee U --kind prefix|name|glob --rights N FILE      (needs -DAB_ADMIN)
 *   [--host H] [--port P] [--user U --password-file F]                               (the login, needs -DAB_LOGIN)
 *
 * It is compiled by auth-bench.sh against the client library of each tree it measures (main before the login
 * exists, main after, the auth build), so the same source drives all of them: -DAB_LOGIN where the client has
 * tsdbc_connect_opts, -DAB_ADMIN where it has tsdbc_grant. Without them it connects without a login, which is all
 * a tsdbd without auth wants.
 *
 * insert: every request carries --rows rows (float32) in one tsdbc_insert call, waited for before the next. "series":
 * all rows of a request belong to one series, picked round robin from the pool of --pool series "ins.N" (created
 * first, outside the timing); "rows": row j of a request belongs to series j of the pool (so --rows is at most
 * --pool), the shape of collectors that send one reading from each of many sensors. Timestamps never repeat in a
 * series, so no write replaces another. The pool is not a series any query benchmark reads.
 *
 * logins: connect, one small request (a series lookup), close, in a loop on --conns threads. A login's PBKDF2 runs
 * once per process (the client caches the keys), so "cached" is the steady state of a long-lived client and --cold
 * forks a child for every login, which starts with an empty cache: the cost of a first login, a short-lived process.
 */
#define _GNU_SOURCE
#include "../client/tsdb_client.h"
#include "cbench.h"
#ifdef AB_ADMIN
#include "../include/tsdb_protocol.h" /* TSDB_SEL_* */
#endif

#include <getopt.h>
#include <pthread.h>
#include <sys/wait.h>

static const char *host = "127.0.0.1";
static int port = 7070, conns = 1, cold;
static double seconds = CB_DEFAULT_SECONDS;
static size_t rows_per_req = 1000, pool = 1000;
static struct {
    const char *user, *password_file; /* user NULL: no login */
} login;

static void die(const char *what, int rc)
{
    fprintf(stderr, "auth-bench-client: %s: %s\n", what, tsdbc_strerror(rc));
    exit(1);
}

static int connect_one(tsdbc **c)
{
#ifdef AB_LOGIN
    if (login.user) {
        tsdbc_options o = {.user = login.user, .password_file = login.password_file};
        return tsdbc_connect_opts(host, port, &o, c);
    }
#endif
    return tsdbc_connect(host, port, c);
}

/* ---------------------------------------------------------------- insert */

static tsdb_sid *pool_ids;
static int by_series;

typedef struct ijob {
    int idx;
    uint64_t requests, rows;
    double secs;
} ijob;

static void *inserter(void *arg)
{
    ijob *j = arg;
    tsdbc *c;
    int rc = connect_one(&c);
    if (rc)
        die("connect", rc);
    tsdb_sid *s = malloc(rows_per_req * sizeof *s);
    int64_t *t = malloc(rows_per_req * sizeof *t);
    float *v = malloc(rows_per_req * sizeof *v);
    uint64_t k = 0;
    double t0 = now_s();
    while (now_s() - t0 < seconds) {
        for (int rep = 0; rep < 8; rep++, k++) {
            /* the stamp is unique per (series, connection): 256 connections at most, so idx is the low digit */
            for (size_t r = 0; r < rows_per_req; r++) {
                t[r] = CB_T0 + (int64_t)((k * rows_per_req + r) * 256 + (uint64_t)j->idx);
                v[r] = (float)((k + r) % 1000) * 0.1f;
                if (!by_series)
                    s[r] = pool_ids[r % pool];
            }
            if (by_series)
                rc = tsdbc_insert_series(c, pool_ids[(k * 31 + (uint64_t)j->idx) % pool], t, v, rows_per_req);
            else
                rc = tsdbc_insert(c, s, t, v, rows_per_req);
            if (rc)
                die("insert", rc);
            j->requests++;
            j->rows += rows_per_req;
        }
    }
    j->secs = now_s() - t0;
    tsdbc_close(c);
    free(s);
    free(t);
    free(v);
    return NULL;
}

static int insert_main(void)
{
    if (rows_per_req < 1 || pool < 1 || (!by_series && rows_per_req > pool)) {
        fprintf(stderr, "auth-bench-client: --rows must be at least 1 (at most --pool for \"rows\")\n");
        return 2;
    }
    tsdbc *c;
    int rc = connect_one(&c);
    if (rc)
        die("connect", rc);
    pool_ids = calloc(pool, sizeof *pool_ids);
    for (size_t i = 0; i < pool; i++) {
        char name[64];
        snprintf(name, sizeof name, "ins.%zu", i);
        if ((rc = tsdbc_series(c, name, 1, &pool_ids[i])) != 0)
            die(name, rc);
    }
    tsdbc_close(c);
    pthread_t th[256];
    ijob jobs[256];
    for (int i = 0; i < conns; i++) {
        jobs[i] = (ijob){.idx = i};
        pthread_create(&th[i], NULL, inserter, &jobs[i]);
    }
    uint64_t req = 0, rows = 0;
    double secs = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(th[i], NULL);
        req += jobs[i].requests;
        rows += jobs[i].rows;
        secs = jobs[i].secs > secs ? jobs[i].secs : secs;
    }
    printf("insert %s, %d connection(s), %zu rows a request: %.0f requests/s = %.3f M rows/s\n",
           by_series ? "one series per request" : "a series per row", conns, rows_per_req, (double)req / secs,
           (double)rows / secs / 1e6);
    printf("RESULT kind=insert mode=%s conns=%d requests=%" PRIu64 " rows=%" PRIu64 " secs=%.4f\n",
           by_series ? "series" : "rows", conns, req, rows, secs);
    return 0;
}

/* ---------------------------------------------------------------- logins */

/* the series the tiny request looks up: the first of the load, which every configuration holds */
static int one_login(void)
{
    tsdbc *c;
    int rc = connect_one(&c);
    if (rc)
        return rc;
    char name[64];
    tsdb_sid id;
    snprintf(name, sizeof name, "%s.0", ds_info[0].name);
    rc = tsdbc_series(c, name, 0, &id);
    tsdbc_close(c);
    return rc;
}

typedef struct ljob {
    uint64_t logins;
    double secs;
} ljob;

static void *login_loop(void *arg)
{
    ljob *j = arg;
    double t0 = now_s();
    while (now_s() - t0 < seconds) {
        int rc;
        if (cold) {
            fflush(NULL);
            pid_t p = fork();
            if (p < 0)
                die("fork", -1);
            if (p == 0) {
                int r = one_login();
                if (r)
                    fprintf(stderr, "auth-bench-client: login: %s\n", tsdbc_strerror(r));
                _exit(r ? 1 : 0);
            }
            int st = 0;
            if (waitpid(p, &st, 0) != p || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
                exit(1);
            rc = 0;
        } else {
            rc = one_login();
        }
        if (rc)
            die("login", rc);
        j->logins++;
    }
    j->secs = now_s() - t0;
    return NULL;
}

static int logins_main(void)
{
    pthread_t th[256];
    ljob jobs[256] = {0};
    for (int i = 0; i < conns; i++)
        pthread_create(&th[i], NULL, login_loop, &jobs[i]);
    uint64_t n = 0;
    double secs = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(th[i], NULL);
        n += jobs[i].logins;
        secs = jobs[i].secs > secs ? jobs[i].secs : secs;
    }
    printf("logins (%s), %d thread(s): %.0f logins/s\n", cold ? "cold, a process each" : "key cache", conns,
           (double)n / secs);
    printf("RESULT kind=login cold=%d conns=%d logins=%" PRIu64 " secs=%.4f\n", cold, conns, n, secs);
    return 0;
}

/* ----------------------------------------------------------------- grant */

#ifdef AB_ADMIN
static int grant_main(const char *grantee, const char *kind, int rights, const char *path)
{
    int k = !strcmp(kind, "prefix") ? TSDB_SEL_PREFIX : !strcmp(kind, "name") ? TSDB_SEL_NAME
                                                         : !strcmp(kind, "glob") ? TSDB_SEL_GLOB : -1;
    FILE *f = fopen(path, "r");
    if (!grantee || k < 0 || rights < 1 || !f) {
        fprintf(stderr, "auth-bench-client: grant wants --grantee, --kind prefix|name|glob, --rights and a file\n");
        return 2;
    }
    tsdbc *c;
    int rc = connect_one(&c);
    if (rc)
        die("connect", rc);
    char line[1024];
    int n = 0;
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\n")] = 0;
        if ((rc = tsdbc_grant(c, grantee, (uint8_t)k, line, (uint8_t)rights)) != 0)
            die(line, rc);
        n++;
    }
    fclose(f);
    tsdbc_close(c);
    printf("%d %s grants for %s\n", n, kind, grantee);
    return 0;
}
#endif

int main(int argc, char **argv)
{
    static struct option opts[] = {{"host", required_argument, 0, 'h'},    {"port", required_argument, 0, 'p'},
                                   {"conns", required_argument, 0, 'c'},   {"seconds", required_argument, 0, 't'},
                                   {"rows", required_argument, 0, 'r'},    {"pool", required_argument, 0, 'P'},
                                   {"cold", no_argument, 0, 'C'},          {"user", required_argument, 0, 'u'},
                                   {"password-file", required_argument, 0, 'w'},
                                   {"grantee", required_argument, 0, 'g'}, {"kind", required_argument, 0, 'k'},
                                   {"rights", required_argument, 0, 'R'},  {0, 0, 0, 0}};
    const char *grantee = NULL, *kind = "";
    int rights = 0, ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'h': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 'c': conns = atoi(optarg); break;
        case 't': seconds = atof(optarg); break;
        case 'r': rows_per_req = strtoull(optarg, NULL, 10); break;
        case 'P': pool = strtoull(optarg, NULL, 10); break;
        case 'C': cold = 1; break;
        case 'u': login.user = optarg; break;
        case 'w': login.password_file = optarg; break;
        case 'g': grantee = optarg; break;
        case 'k': kind = optarg; break;
        case 'R': rights = atoi(optarg); break;
        default: return 2;
        }
    }
#ifndef AB_LOGIN
    if (login.user) {
        fprintf(stderr, "auth-bench-client: built without -DAB_LOGIN: this client has no login\n");
        return 2;
    }
#endif
    if (conns < 1 || conns > 256 || optind >= argc) {
        fprintf(stderr, "usage: auth-bench-client insert series|rows | logins [--cold] | grant ... FILE\n"
                        "       [--conns N] [--seconds S] [--rows R] [--pool P] [--host H] [--port P]\n"
                        "       [--user U --password-file F]\n");
        return 2;
    }
    const char *mode = argv[optind];
    if (!strcmp(mode, "insert") && optind + 1 < argc && (!strcmp(argv[optind + 1], "series") || !strcmp(argv[optind + 1], "rows"))) {
        by_series = !strcmp(argv[optind + 1], "series");
        return insert_main();
    }
    if (!strcmp(mode, "logins"))
        return logins_main();
#ifdef AB_ADMIN
    if (!strcmp(mode, "grant") && optind + 1 < argc)
        return grant_main(grantee, kind, rights, argv[optind + 1]);
#else
    (void)grantee;
    (void)kind;
    (void)rights;
#endif
    fprintf(stderr, "auth-bench-client: unknown mode %s\n", mode);
    return 2;
}
