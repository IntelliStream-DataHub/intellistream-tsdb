/*
 * Helpers shared by the harnesses: scratch directories and files. Each
 * harness defines LLVMFuzzerTestOneInput (and optionally LLVMFuzzerInitialize
 * and fuzz_seeds, see driver.c). Harnesses that write files work under
 * $FUZZ_TMP (default /tmp), in a directory of their own per process.
 */
#ifndef TSDB_FUZZ_H
#define TSDB_FUZZ_H
#define _GNU_SOURCE
#include <fcntl.h>
#include <inttypes.h>
#include <ftw.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerInitialize(int *argc, char ***argv);
void fuzz_seeds(const char *dir);

static inline int fuzz_put(const char *dir, const char *name, const void *d, size_t n)
{
    char path[8192];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;
    ssize_t w = n ? write(fd, d, n) : 0;
    close(fd);
    return w == (ssize_t)n ? 0 : -1;
}

/* the whole file, or NULL; *n its length */
static inline uint8_t *fuzz_get(const char *dir, const char *name, size_t *n)
{
    char path[8192];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *b = malloc(len > 0 ? (size_t)len : 1);
    *n = b && len > 0 ? fread(b, 1, (size_t)len, f) : 0;
    fclose(f);
    return b;
}

static int fuzz_rm1(const char *p, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st, (void)flag, (void)ftw;
    return remove(p);
}

static inline void fuzz_rm(const char *dir) { nftw(dir, fuzz_rm1, 16, FTW_DEPTH | FTW_PHYS); }

static const char *fuzz_made[4];

/* atexit: removes the directories fuzz_dir made */
static inline void fuzz_rm_at_exit(void)
{
    for (int i = 0; i < 4; i++)
        if (fuzz_made[i])
            fuzz_rm(fuzz_made[i]);
}

/* a fresh directory <$FUZZ_TMP>/tsdb-fuzz-<tag>-<pid>, emptied if it exists */
static inline const char *fuzz_dir(const char *tag)
{
    static char path[4][1024];
    static int k;
    char *p = path[k & 3];
    fuzz_made[k++ & 3] = p;
    const char *base = getenv("FUZZ_TMP");
    snprintf(p, sizeof path[0], "%s/tsdb-fuzz-%s-%d", base && *base ? base : "/tmp", tag, (int)getpid());
    fuzz_rm(p);
    if (mkdir(p, 0755) != 0) {
        perror(p);
        exit(1);
    }
    return p;
}

#endif
