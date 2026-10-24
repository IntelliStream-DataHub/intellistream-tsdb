/*
 * Parallel line protocol loader (InfluxDB 2 and 3, QuestDB):
 *   loadlp FILE HOST PORT PATH HEADERS LINES_PER_BATCH THREADS
 * HEADERS are extra header lines, each ending in \r\n ("" for none).
 *
 * The file is mmapped and cut into batches at line ends. Workers take the next batch from a shared
 * counter, so batches go out roughly in file order whatever the thread count. The time runs from
 * the first request to the last acknowledgement; 429, 503 and dropped connections are retried
 * (and counted), any other error stops the load.
 */
#define _GNU_SOURCE
#include "httpc.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>

static const char *host, *path, *headers;
static int port;
static const char *data;
static size_t *bounds, nbatches;
static atomic_size_t next;
static atomic_ulong posts, retries;

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static void *worker(void *arg)
{
    (void)arg;
    hconn c;
    if (h_init(&c, host, port) != 0) exit(1);
    hbuf resp = {0};
    for (;;) {
        size_t b = atomic_fetch_add(&next, 1);
        if (b >= nbatches) break;
        for (int tries = 0;; tries++) {
            resp.n = 0;
            int st = h_request(&c, "POST", path, headers, data + bounds[b], bounds[b + 1] - bounds[b], &resp, NULL);
            if (st >= 200 && st < 300) break;
            if ((st == 429 || st == 503 || st < 0) && tries < 50) { /* back off and retry */
                atomic_fetch_add(&retries, 1);
                fprintf(stderr, "retry batch %zu: status %d %.200s\n", b, st, resp.p ? resp.p : "");
                usleep(200000);
                continue;
            }
            fprintf(stderr, "batch %zu: status %d: %.500s\n", b, st, resp.p ? resp.p : "");
            exit(1);
        }
        atomic_fetch_add(&posts, 1);
    }
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc != 8) {
        fprintf(stderr, "usage: loadlp FILE HOST PORT PATH HEADERS LINES THREADS\n");
        return 2;
    }
    host = argv[2];
    port = atoi(argv[3]);
    path = argv[4];
    headers = argv[5];
    size_t lines = strtoull(argv[6], NULL, 10);
    int threads = atoi(argv[7]);
    int fd = open(argv[1], O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    size_t cap = 1024, nl = 0, rows = 0;
    bounds = malloc(cap * sizeof *bounds);
    bounds[0] = 0;
    for (size_t i = 0; i < (size_t)st.st_size; i++)
        if (data[i] == '\n' && ++nl == lines) {
            if (++nbatches + 1 >= cap) bounds = realloc(bounds, (cap *= 2) * sizeof *bounds);
            bounds[nbatches] = i + 1;
            rows += nl;
            nl = 0;
        }
    if (nl) {
        bounds[++nbatches] = st.st_size;
        rows += nl;
    }
    fprintf(stderr, "%zu rows in %zu batches of %zu lines, %d threads\n", rows, nbatches, lines, threads);
    pthread_t th[256];
    double t0 = now();
    for (int i = 0; i < threads; i++) pthread_create(&th[i], NULL, worker, NULL);
    for (int i = 0; i < threads; i++) pthread_join(th[i], NULL);
    double dt = now() - t0;
    printf("loaded %zu rows in %.2f s = %.3f M rows/s (%lu posts, %lu retries, %d threads, %zu lines/batch)\n", rows,
           dt, rows / dt / 1e6, (unsigned long)posts, (unsigned long)retries, threads, lines);
    return 0;
}
