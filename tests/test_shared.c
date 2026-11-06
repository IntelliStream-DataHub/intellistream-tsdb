/*
 * The engine through libtsdb.so, as the embedded bindings (Rust, Python, Java
 * FFM) use it: only the public API, memory returned by the library freed with
 * tsdb_free, and no internal symbol visible.
 */
#include "tsdb.h"

#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)
#define OK(x) CHECK((x) == 0)

#ifdef __APPLE__
#define LIBTSDB "libtsdb.dylib"
#else
#define LIBTSDB "libtsdb.so"
#endif

int main(int argc, char **argv)
{
    char dir[512], cmd[600];
    snprintf(dir, sizeof dir, "%s/tsdb-shared-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;

    /* engine internals stay hidden: a binding can only reach the public API */
    void *self = dlopen(LIBTSDB, RTLD_NOW | RTLD_NOLOAD);
    CHECK(self != NULL);
    if (self) {
        CHECK(dlsym(self, "tsdb_open") != NULL);
        CHECK(dlsym(self, "tsdb_free") != NULL);
        CHECK(dlsym(self, "eng_open") == NULL);
        CHECK(dlsym(self, "parts_publish") == NULL);
        CHECK(dlsym(self, "tsdb_struct_size") != NULL);
    }

    /* what a binding compares with its own layouts when it loads the library */
    CHECK(tsdb_struct_size(TSDB_STRUCT_OPTIONS) == sizeof(tsdb_options));
    CHECK(tsdb_struct_size(TSDB_STRUCT_VALUES) == sizeof(tsdb_values));
    CHECK(tsdb_struct_size(TSDB_STRUCT_BUCKET) == sizeof(tsdb_bucket));
    CHECK(tsdb_struct_size(TSDB_STRUCT_POINTS) == sizeof(tsdb_points));
    CHECK(tsdb_struct_size(TSDB_STRUCT_GROUP) == sizeof(tsdb_group));
    CHECK(tsdb_struct_size(TSDB_STRUCT_QUERY_OPTS) == sizeof(tsdb_query_opts));
    CHECK(tsdb_struct_size(TSDB_STRUCT_QUERY_STATS) == sizeof(tsdb_query_stats));
    CHECK(tsdb_struct_size(TSDB_STRUCT_STATS) == sizeof(tsdb_stats));
    CHECK(tsdb_struct_size(TSDB_STRUCT_PART_INFO) == sizeof(tsdb_part_info));
    CHECK(tsdb_struct_size(TSDB_STRUCT_SPREAD_BUCKET) == sizeof(tsdb_spread_bucket));
    CHECK(tsdb_struct_size(TSDB_STRUCT_TWA_OPTS) == sizeof(tsdb_twa_opts));
    CHECK(tsdb_struct_size(TSDB_STRUCT_TWA_BUCKET) == sizeof(tsdb_twa_bucket));
    CHECK(tsdb_struct_size(TSDB_STRUCT_MULTI_QUERY) == sizeof(tsdb_multi_query));
    CHECK(tsdb_struct_size(TSDB_STRUCT_SERIES_RESULT) == sizeof(tsdb_series_result));
    CHECK(tsdb_struct_size(TSDB_STRUCT_MULTI_OPTS) == sizeof(tsdb_multi_opts));
    CHECK(tsdb_struct_size(TSDB_STRUCT_LATEST) == sizeof(tsdb_latest_result));
    CHECK(tsdb_struct_size(TSDB_STRUCT_LATEST_OPTS) == sizeof(tsdb_latest_opts));
    CHECK(tsdb_struct_size(-1) == 0 && tsdb_struct_size(TSDB_STRUCT_LATEST_OPTS + 1) == 0);

    tsdb *db;
    OK(tsdb_open(dir, NULL, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "pump7.flow", TSDB_FLOAT64, NULL, &id));
    enum { N = 1000 };
    int64_t ts[N];
    double v[N];
    for (int i = 0; i < N; i++) {
        ts[i] = 1700000000000 + (int64_t)i * 1000;
        v[i] = i * 0.5;
    }
    tsdb_values col = {TSDB_FLOAT64, v, NULL};
    OK(tsdb_insert_series_values(db, id, ts, &col, N));
    tsdb_bucket *b;
    size_t nb;
    OK(tsdb_query_agg(db, id, INT64_MIN, INT64_MAX, 0, &b, &nb));
    CHECK(nb == 1 && b[0].count == N && b[0].sum == 0.5 * N * (N - 1) / 2);
    tsdb_free(b);
    OK(tsdb_close(db));
    if (system(cmd) != 0)
        return 1;
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("shared library tests passed\n");
    return 0;
}
