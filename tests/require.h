/* SPDX-License-Identifier: AGPL-3.0-only */
/* What a test needs from its surroundings (libcrashfs.so beside the binary, the CLI, a fixture) and what it does
 * when that is missing. A silent skip would turn the strongest durability evidence (the power-loss sweeps) into a
 * no-op that prints "ok", so a skip is either an explicit failure or an explicit, loud "SKIPPED" line:
 *   TSDB_REQUIRE_CRASHFS=1  (the Makefile sets it on Linux, where the shim is always built) makes a missing shim a
 *   failure; without it (macOS has no LD_PRELOAD shim) the test prints "SKIPPED" and says why. */
#ifndef TSDB_TEST_REQUIRE_H
#define TSDB_TEST_REQUIRE_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Every skip is counted, and the test's last line says how many ("all tests passed (2 skipped)"): a run that
 * skipped its power-loss checks must not read like one that ran them. */
static int tr_skipped;
static inline const char *tr_skips(void)
{
    static char buf[32];
    if (tr_skipped == 0)
        return "";
    snprintf(buf, sizeof buf, " (%d skipped)", tr_skipped);
    return buf;
}
/* A part `what` that did not run, reported loudly and counted. */
static inline void tr_skip(const char *what, const char *why)
{
    tr_skipped++;
    printf("  %s: SKIPPED (%s)\n", what, why);
}

/* The power-loss part `what` cannot run because `why` is missing. Returns 1 when that is a failure (the caller adds it
 * to its failure count), 0 when it is only reported as skipped. */
static inline int tr_crashfs_missing(const char *what, const char *why)
{
    const char *e = getenv("TSDB_REQUIRE_CRASHFS");
    if (e && *e && strcmp(e, "0") != 0) {
        fprintf(stderr, "FAIL: %s: needs %s, which is missing (TSDB_REQUIRE_CRASHFS is set: build it with make)\n", what,
                why);
        return 1;
    }
    tr_skipped++;
    printf("  %s: SKIPPED (no %s)\n", what, why);
    return 0;
}
#endif
