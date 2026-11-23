/* SPDX-License-Identifier: AGPL-3.0-only */
/* The random seeds of the tests. By default a test keeps its constants, so `make test` is the same run every time and
 * a failure repeats. TSDB_TEST_SEED=N changes every seed that goes through ts_seed (N mixed into the constant) and the
 * test says which: "seed <name>: <value> (TSDB_TEST_SEED=N)". `make test-seeds` runs the random-heavy tests under a
 * fresh N and prints it, so a failure found that way is rerun with the same N. */
#ifndef TSDB_TEST_SEED_H
#define TSDB_TEST_SEED_H
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* The seed `base` of the generator `name`, changed by TSDB_TEST_SEED when that is set (and not 0). */
static inline uint64_t ts_seed(const char *name, uint64_t base)
{
    const char *e = getenv("TSDB_TEST_SEED");
    if (!e || !*e)
        return base;
    uint64_t v = strtoull(e, NULL, 0);
    if (v == 0)
        return base;
    uint64_t s = base ^ (v * 0x9e3779b97f4a7c15ull);
    s ^= s >> 31;
    s *= 0xbf58476d1ce4e5b9ull;
    s ^= s >> 29;
    fprintf(stderr, "seed %s: %" PRIu64 " (TSDB_TEST_SEED=%s)\n", name, s, e);
    return s;
}
#endif
