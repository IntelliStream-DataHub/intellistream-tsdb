/* duckq: one DuckDB statement timed, DuckDB's counterpart of `chq sqlt` (embq, which the multi-series branch owns,
 * has only an untimed `sql`).
 *
 *   duckq sqlt DB REPS OUT SQL   the statement REPS times after a warm-up, timed around duckdb_query and the
 *                                materialized result; the last answer, untimed, as TSV to OUT (NULL for nulls,
 *                                the same text as `embq duckdb sql`, which rel.py tsv reads)
 *
 * The database is opened as embq opens it (threads = DUCK_THREADS or 32, memory_limit = DUCK_MEMORY or 24GB), so
 * these timings and embq's random-window timings run under the same settings. Opening is not timed.
 */
#include <duckdb.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void die(const char *what, const char *why)
{
    fprintf(stderr, "duckq: %s: %s\n", what, why);
    exit(1);
}

int main(int argc, char **argv)
{
    if (argc != 6 || strcmp(argv[1], "sqlt")) {
        fprintf(stderr, "usage: duckq sqlt DB REPS OUT SQL\n");
        return 2;
    }
    int reps = atoi(argv[3]);
    if (reps < 1 || reps > 100) die("REPS", "1 to 100");
    duckdb_database db;
    duckdb_connection c;
    duckdb_config cfg;
    char *err = NULL;
    duckdb_create_config(&cfg);
    duckdb_set_config(cfg, "threads", getenv("DUCK_THREADS") ? getenv("DUCK_THREADS") : "32");
    duckdb_set_config(cfg, "memory_limit", getenv("DUCK_MEMORY") ? getenv("DUCK_MEMORY") : "24GB");
    if (duckdb_open_ext(argv[2], &db, cfg, &err) == DuckDBError) die(argv[2], err ? err : "cannot open");
    duckdb_destroy_config(&cfg);
    if (duckdb_connect(db, &c) == DuckDBError) die("duckdb", "cannot connect");
    double t[101], mean = 0, sd = 0;
    duckdb_result r;
    for (int i = 0; i <= reps; i++) { /* run 0 is the warm-up */
        double t0 = now();
        if (duckdb_query(c, argv[5], &r) == DuckDBError) die(argv[5], duckdb_result_error(&r));
        idx_t rows = duckdb_row_count(&r);
        t[i] = now() - t0;
        if (i) printf("sqlt run %d: %.4f s (%llu rows)\n", i, t[i], (unsigned long long)rows);
        if (i < reps) duckdb_destroy_result(&r);
    }
    for (int i = 1; i <= reps; i++) mean += t[i] / reps;
    for (int i = 1; i <= reps; i++) sd += (t[i] - mean) * (t[i] - mean);
    printf("sqlt: mean %.4f s sd %.4f, %d runs after a warm-up\n", mean, reps > 1 ? sqrt(sd / (reps - 1)) : 0.0, reps);
    FILE *o = fopen(argv[4], "w");
    if (!o) die(argv[4], "cannot write");
    idx_t cols = duckdb_column_count(&r), rows = duckdb_row_count(&r);
    for (idx_t i = 0; i < rows; i++) {
        for (idx_t k = 0; k < cols; k++) {
            char *v = duckdb_value_varchar(&r, k, i);
            fprintf(o, "%s%s", k ? "\t" : "", v ? v : "NULL");
            duckdb_free(v);
        }
        fputc('\n', o);
    }
    if (fclose(o)) die(argv[4], "write failed");
    duckdb_destroy_result(&r);
    duckdb_disconnect(&c);
    duckdb_close(&db);
    return 0;
}
