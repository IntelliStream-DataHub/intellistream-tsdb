/* SPDX-License-Identifier: AGPL-3.0-only */
/* A scratch directory of its own for a test run: BASE/NAME.XXXXXX, made with mkdtemp, so two runs (or two worktrees)
 * at once, and a leftover of a killed run, never meet in a fixed name. The test removes it when it passed
 * (scratch_done(0)) and keeps it when it failed, for the evidence. Callers still use fixed names inside it. */
#ifndef TSDB_TEST_SCRATCH_H
#define TSDB_TEST_SCRATCH_H
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char scratch_root_dir[1024];

/* BASE is the scratch directory the Makefile passes (or /tmp). Exits 1 when it cannot be made. */
static inline const char *scratch_make(const char *base, const char *name)
{
    snprintf(scratch_root_dir, sizeof scratch_root_dir, "%s/%s.XXXXXX", base ? base : "/tmp", name);
    if (!mkdtemp(scratch_root_dir)) {
        fprintf(stderr, "%s: mkdtemp %s: %s\n", name, scratch_root_dir, strerror(errno));
        exit(1);
    }
    return scratch_root_dir;
}

/* Removes the directory scratch_make made when `failures` is 0; says where it is left otherwise. */
static inline void scratch_done(int failures)
{
    if (!scratch_root_dir[0])
        return;
    if (failures) {
        fprintf(stderr, "scratch directory kept: %s\n", scratch_root_dir);
        return;
    }
    char cmd[1100];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", scratch_root_dir);
    if (system(cmd) != 0)
        fprintf(stderr, "could not remove %s\n", scratch_root_dir);
}
#endif
