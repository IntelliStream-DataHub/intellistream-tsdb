/*
 * Query load driver for the systems spoken to over HTTP or the PostgreSQL wire protocol.
 *   qdrive http HOST PORT HEADERS QFILE THREADS SECONDS [MAXQ]   lines: "GET path" or "POST path<TAB>ctype<TAB>body"
 *   qdrive pg   CONNINFO QFILE THREADS SECONDS [MAXQ]            lines: SQL
 *
 * Each thread has its own connection; thread i runs queries i, i+T, i+2T, ... of the file
 * (wrapping) until SECONDS have passed or it has run MAXQ queries, so all threads together walk
 * the file in order. Every response is read completely (libpq fetches whole results), and any
 * failed query stops the run rather than being counted. Prints queries, q/s, and the median and
 * p99 latency over all queries.
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
static int port, threads;
static double seconds;
static long maxq = -1;
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
        if ((maxq >= 0 && (long)j->n >= maxq) || now() - t0 >= seconds) break;
        const char *q = qs[i % nq];
        double a = now();
        if (mode_pg) {
            PGresult *r = PQexec(pg, q);
            int st = PQresultStatus(r);
            if (st != 1 && st != 2) {
                fprintf(stderr, "query failed: %s\n%.300s\n", PQresultErrorMessage(r), q);
                exit(1);
            }
            j->rows += (unsigned long long)PQntuples(r);
            PQclear(r);
        } else {
            char method[8], path[16384], ctype[128] = "";
            const char *body = "";
            const char *sp = strchr(q, ' ');
            snprintf(method, sizeof method, "%.*s", (int)(sp - q), q);
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
            if (st != 200) {
                fprintf(stderr, "query failed: status %d: %.500s\n%.300s\n", st, resp.p ? resp.p : "", q);
                exit(1);
            }
            j->bytes += rb;
        }
        double dt = now() - a;
        if (j->n == j->cap) j->lat = realloc(j->lat, (j->cap = j->cap ? j->cap * 2 : 4096) * sizeof(double));
        j->lat[j->n++] = dt;
    }
    j->secs = now() - t0;
    if (pg) PQfinish(pg);
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
    FILE *f = fopen(argv[k], "r");
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
