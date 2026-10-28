/*
 * Query load driver for the systems spoken to over HTTP or the PostgreSQL wire protocol.
 *   qdrive http HOST PORT HEADERS QFILE THREADS SECONDS [MAXQ]   lines: "GET path" or "POST path<TAB>ctype<TAB>body",
 *                                                                 or several of them joined by " && "
 *   qdrive pg   CONNINFO QFILE THREADS SECONDS [MAXQ]            lines: SQL
 *
 * Each thread has its own connection; thread i runs queries i, i+T, i+2T, ... of the file
 * (wrapping) until SECONDS have passed or it has run MAXQ queries, so all threads together walk
 * the file in order. MAXQ 0 runs every query of the file exactly once instead, each thread taking the
 * next one when it is free (100 per-series queries on 16 connections, say). Every response is read completely (libpq fetches whole results), and any
 * failed query stops the run rather than being counted. Prints queries, q/s, and the median and
 * p99 latency over all queries.
 *
 * A query that runs longer than QDRIVE_TIMEOUT seconds (default 600; 0: no limit) fails the run with exit status
 * 124 and names the query: a server that never answers (TDengine with one vnode-query thread deadlocked on a batch
 * query for 90 minutes) must not hold the run and the shared benchmark lock forever. A watchdog in the main thread
 * checks, so the limit covers HTTP and libpq alike without touching their I/O.
 *
 * SQL goes through PQexec, the simple query protocol with literals, as an application would send
 * it; prepared statements could plan differently (TimescaleDB chunk exclusion, for one). The libpq
 * functions are declared here so only the shared library is needed, not its headers.
 */
#define _GNU_SOURCE
#include "httpc.h"
#include <pthread.h>
#include <time.h>

typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;
PGconn *PQconnectdb(const char *conninfo);
int PQstatus(const PGconn *conn);
char *PQerrorMessage(const PGconn *conn);
PGresult *PQexec(PGconn *conn, const char *query);
int PQresultStatus(const PGresult *res);
char *PQresultErrorMessage(const PGresult *res);
int PQntuples(const PGresult *res);
void PQclear(PGresult *res);
void PQfinish(PGconn *conn);

static int mode_pg;
static const char *host, *headers, *conninfo;
static const char *err_prefix; /* QDRIVE_ERR: a reply body starting with it is a failed query */
static size_t err_len;
/* QDRIVE_ERR_SUB: substrings, separated by '|', whose presence anywhere in a reply marks a failed query (InfluxDB 2
 * answers a failed InfluxQL statement with status 200 and "error": in its results, and a Flux error raised mid-stream
 * as a CSV table headed ,error,reference). The search is not timed. */
static char *err_sub[8];
static int n_err_sub;
static int port, threads;
static double seconds;
static long maxq = -1;  /* queries per thread; 0: every query of the file once, taken from a shared queue */
static size_t next_q;
static double qtimeout = 600;
static char **qs;
static size_t nq;

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

typedef struct job {
    int id;
    double busy_since; /* start of the query in flight, 0 between queries; read by the watchdog */
    const char *cur;
    int done;
    double *lat;
    size_t n, cap;
    double secs;
    unsigned long long rows, bytes;
} job;

static void *run(void *arg)
{
    job *j = arg;
    hconn c;
    PGconn *pg = NULL;
    hbuf resp = {0};
    if (mode_pg) {
        pg = PQconnectdb(conninfo);
        if (PQstatus(pg) != 0) {
            fprintf(stderr, "pg connect: %s\n", PQerrorMessage(pg));
            exit(1);
        }
    } else if (h_init(&c, host, port) != 0)
        exit(1);
    double t0 = now();
    for (size_t i = (size_t)j->id;; i += (size_t)threads) {
        if (maxq == 0) { /* the file once, as a shared queue: the next query goes to the first free thread */
            i = __atomic_fetch_add(&next_q, 1, __ATOMIC_RELAXED);
            if (i >= nq) break;
        } else if ((maxq > 0 && (long)j->n >= maxq) || now() - t0 >= seconds)
            break;
        const char *q = qs[i % nq];
        double a = now(), untimed = 0;
        __atomic_store_n(&j->cur, q, __ATOMIC_RELAXED);
        __atomic_store(&j->busy_since, &a, __ATOMIC_RELEASE);
        if (mode_pg) {
            PGresult *r = PQexec(pg, q);
            int st = PQresultStatus(r);
            if (st != 1 && st != 2) {
                fprintf(stderr, "query failed: %s\n%.300s\n", PQresultErrorMessage(r), q);
                exit(1);
            }
            j->rows += (unsigned long long)PQntuples(r);
            PQclear(r);
        } else
            /* a line may hold several requests joined by " && " (Prometheus: one query_range for the whole
               buckets of a window, one instant query for the partial ones at its ends); they run one after
               the other and count as one query */
            for (const char *seg = q; seg;) {
                const char *nx = strstr(seg, " && ");
                size_t sl = nx ? (size_t)(nx - seg) : strlen(seg);
                char one[16384];
                snprintf(one, sizeof one, "%.*s", (int)sl, seg);
                seg = nx ? nx + 4 : NULL;
                char method[8], path[16384], ctype[128] = "";
                const char *body = "";
                const char *sp = strchr(one, ' ');
                snprintf(method, sizeof method, "%.*s", (int)(sp - one), one);
                const char *t1 = strchr(sp + 1, '\t');
                if (t1) {
                    snprintf(path, sizeof path, "%.*s", (int)(t1 - sp - 1), sp + 1);
                    const char *t2 = strchr(t1 + 1, '\t');
                    snprintf(ctype, sizeof ctype, "Content-Type: %.*s\r\n", (int)(t2 - t1 - 1), t1 + 1);
                    body = t2 + 1;
                } else
                    snprintf(path, sizeof path, "%s", sp + 1);
                char hdrs[4096];
                snprintf(hdrs, sizeof hdrs, "%s%s", headers, ctype);
                size_t rb = 0;
                resp.n = 0;
                int st = h_request(&c, method, path, hdrs, body, strlen(body), &resp, &rb);
                /* IoTDB's REST service answers a failed query (out of query memory, say) with status 200 and a body
                 * {"code":...}: QDRIVE_ERR names such a prefix, so a failure is never timed as a fast answer */
                int bad = st != 200 || (err_prefix && resp.n >= err_len && !memcmp(resp.p, err_prefix, err_len));
                if (!bad && n_err_sub) {
                    double s0 = now();
                    for (int e = 0; e < n_err_sub && !bad; e++)
                        bad = resp.n && memmem(resp.p, resp.n, err_sub[e], strlen(err_sub[e])) != NULL;
                    untimed += now() - s0;
                }
                if (bad) {
                    fprintf(stderr, "query failed: status %d: %.*s\n%.300s\n", st, (int)(resp.n < 500 ? resp.n : 500),
                            resp.p ? resp.p : "", one);
                    exit(1);
                }
                j->bytes += rb;
            }
        double idle = 0;
        __atomic_store(&j->busy_since, &idle, __ATOMIC_RELEASE);
        double dt = now() - a - untimed;
        if (j->n == j->cap) j->lat = realloc(j->lat, (j->cap = j->cap ? j->cap * 2 : 4096) * sizeof(double));
        j->lat[j->n++] = dt;
    }
    j->secs = now() - t0;
    if (pg) PQfinish(pg);
    __atomic_store_n(&j->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

static int cmpd(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

int main(int argc, char **argv)
{
    int k;
    err_prefix = getenv("QDRIVE_ERR");
    if (err_prefix && !*err_prefix) err_prefix = NULL;
    err_len = err_prefix ? strlen(err_prefix) : 0;
    const char *es = getenv("QDRIVE_ERR_SUB");
    if (es && *es)
        for (char *d = strdup(es), *t; n_err_sub < 8 && (t = strsep(&d, "|"));)
            if (*t) err_sub[n_err_sub++] = t;
    if (argc >= 8 && !strcmp(argv[1], "http")) {
        host = argv[2];
        port = atoi(argv[3]);
        headers = argv[4];
        k = 5;
    } else if (argc >= 6 && !strcmp(argv[1], "pg")) {
        mode_pg = 1;
        conninfo = argv[2];
        k = 3;
    } else {
        fprintf(stderr, "usage: see source\n");
        return 2;
    }
    const char *to = getenv("QDRIVE_TIMEOUT");
    if (to && *to) qtimeout = atof(to);
    FILE *f = fopen(argv[k], "r");
    if (!f) { /* a missing query file is a failed run, never a crash in getline */
        fprintf(stderr, "qdrive: cannot open %s: %s\n", argv[k], strerror(errno));
        return 2;
    }
    threads = atoi(argv[k + 1]);
    seconds = atof(argv[k + 2]);
    if (argc > k + 3) maxq = atol(argv[k + 3]);
    char *line = NULL;
    size_t cap = 0, qcap = 0;
    ssize_t l;
    while ((l = getline(&line, &cap, f)) > 0) {
        if (line[l - 1] == '\n') line[--l] = 0;
        if (!l) continue;
        if (nq == qcap) qs = realloc(qs, (qcap = qcap ? qcap * 2 : 1024) * sizeof *qs);
        qs[nq++] = strdup(line);
    }
    job *jobs = calloc((size_t)threads, sizeof *jobs);
    pthread_t th[512];
    for (int i = 0; i < threads; i++) {
        jobs[i].id = i;
        pthread_create(&th[i], NULL, run, &jobs[i]);
    }
    for (;;) { /* watchdog: fail the run on a query that never ends */
        int alive = 0;
        double t = now();
        for (int i = 0; i < threads; i++) {
            if (__atomic_load_n(&jobs[i].done, __ATOMIC_ACQUIRE)) continue;
            alive++;
            double b;
            __atomic_load(&jobs[i].busy_since, &b, __ATOMIC_ACQUIRE);
            if (qtimeout > 0 && b > 0 && t - b > qtimeout) {
                const char *q = __atomic_load_n(&jobs[i].cur, __ATOMIC_RELAXED);
                fprintf(stderr, "query timed out after %.0f s (QDRIVE_TIMEOUT): %.300s\n", t - b, q ? q : "");
                fflush(NULL);
                _exit(124);
            }
        }
        if (!alive) break;
        struct timespec ts = {0, 200 * 1000 * 1000};
        nanosleep(&ts, NULL);
    }
    size_t total = 0;
    double secs = 0;
    unsigned long long rows = 0, bytes = 0;
    for (int i = 0; i < threads; i++) {
        pthread_join(th[i], NULL);
        total += jobs[i].n;
        secs = jobs[i].secs > secs ? jobs[i].secs : secs;
        rows += jobs[i].rows;
        bytes += jobs[i].bytes;
    }
    double *all = malloc(total * sizeof *all);
    size_t m = 0;
    for (int i = 0; i < threads; i++)
        for (size_t x = 0; x < jobs[i].n; x++) all[m++] = jobs[i].lat[x];
    qsort(all, m, sizeof *all, cmpd);
    printf("%s c=%d: %zu queries in %.2f s = %.1f q/s, median %.3f ms, p99 %.3f ms (rows %llu, bytes %llu)\n",
           argv[k], threads, total, secs, total / secs, all[m / 2] * 1e3, all[(size_t)(m * 0.99)] * 1e3, rows, bytes);
    return 0;
}
