/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Drop OS page cache for files and directory trees without root (D31).
 *
 * Calls posix_fadvise(POSIX_FADV_DONTNEED) on every regular file under each
 * given path and checks remaining page residency with mincore().
 *
 *   dropcache [--quiet] [--strict] [--max-resident-pct PCT] PATH [PATH ...]
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __APPLE__
typedef char mincore_vec_t;
#else
typedef unsigned char mincore_vec_t;
#endif

typedef struct dc_state {
    size_t page_size;
    size_t chunk_bytes;
    mincore_vec_t *vec;
    uint64_t files;
    uint64_t bytes;
    uint64_t total_pages;
    uint64_t resident_pages;
    uint64_t fadvise_errors;
    uint64_t io_errors;
} dc_state;

static void drop_file(dc_state *st, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        if (errno != ENOENT)
            st->io_errors++;
        return;
    }
    struct stat sb;
    if (fstat(fd, &sb) != 0) {
        st->io_errors++;
        close(fd);
        return;
    }
    if (!S_ISREG(sb.st_mode) || sb.st_size <= 0) {
        close(fd);
        return;
    }

#ifdef POSIX_FADV_DONTNEED
    if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0)
        st->fadvise_errors++;
#endif

    st->files++;
    st->bytes += (uint64_t)sb.st_size;

    uint64_t size = (uint64_t)sb.st_size;
    for (uint64_t off = 0; off < size; off += st->chunk_bytes) {
        uint64_t rem = size - off;
        size_t len = rem < st->chunk_bytes ? (size_t)rem : st->chunk_bytes;
        void *m = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, (off_t)off);
        if (m == MAP_FAILED) {
            st->io_errors++;
            break;
        }
        size_t npages = (len + st->page_size - 1) / st->page_size;
        if (mincore(m, len, st->vec) == 0) {
            st->total_pages += npages;
            for (size_t i = 0; i < npages; i++)
                if (st->vec[i] & 1)
                    st->resident_pages++;
        } else {
            st->io_errors++;
        }
        munmap(m, len);
    }
    close(fd);
}

static void walk_path(dc_state *st, const char *path, int depth)
{
    if (depth > 64)
        return;
    struct stat sb;
    if (lstat(path, &sb) != 0) {
        if (errno != ENOENT)
            st->io_errors++;
        return;
    }
    if (S_ISREG(sb.st_mode)) {
        drop_file(st, path);
        return;
    }
    if (!S_ISDIR(sb.st_mode))
        return;

    DIR *d = opendir(path);
    if (!d) {
        if (errno != ENOENT)
            st->io_errors++;
        return;
    }
    struct dirent *de;
    char child[PATH_MAX];
    for (;;) {
        errno = 0;
        de = readdir(d);
        if (!de) {
            if (errno != 0)
                st->io_errors++;
            break;
        }
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        int n = snprintf(child, sizeof child, "%s/%s", path, de->d_name);
        if (n < 0 || (size_t)n >= sizeof child) {
            st->io_errors++;
            continue;
        }
        walk_path(st, child, depth + 1);
    }
    closedir(d);
}

static void usage(void)
{
    fprintf(stderr, "usage: dropcache [--quiet] [--strict] [--max-resident-pct PCT] PATH [PATH ...]\n");
}

int main(int argc, char **argv)
{
    bool quiet = false;
    bool strict = false;
    double max_res_pct = -1.0;

    int i = 1;
    while (i < argc && argv[i][0] == '-') {
        if (strcmp(argv[i], "--quiet") == 0 || strcmp(argv[i], "-q") == 0) {
            quiet = true;
        } else if (strcmp(argv[i], "--strict") == 0) {
            strict = true;
        } else if (strcmp(argv[i], "--max-resident-pct") == 0 && i + 1 < argc) {
            char *end = NULL;
            max_res_pct = strtod(argv[++i], &end);
            if (!end || *end != '\0' || max_res_pct < 0.0 || max_res_pct > 100.0) {
                fprintf(stderr, "dropcache: invalid --max-resident-pct value\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage();
            return 0;
        } else if (strcmp(argv[i], "--") == 0) {
            i++;
            break;
        } else {
            fprintf(stderr, "dropcache: unknown option '%s'\n", argv[i]);
            usage();
            return 2;
        }
        i++;
    }
    if (i >= argc) {
        usage();
        return 2;
    }

    long ps = sysconf(_SC_PAGESIZE);
    if (ps <= 0)
        ps = 4096;
    dc_state st;
    memset(&st, 0, sizeof st);
    st.page_size = (size_t)ps;
    st.chunk_bytes = ((size_t)(16u << 20) / st.page_size) * st.page_size;
    if (st.chunk_bytes < st.page_size)
        st.chunk_bytes = st.page_size;
    st.vec = malloc(st.chunk_bytes / st.page_size);
    if (!st.vec) {
        fprintf(stderr, "dropcache: out of memory\n");
        return 1;
    }

    for (; i < argc; i++)
        walk_path(&st, argv[i], 0);

    free(st.vec);

    double res_pct = st.total_pages ? (double)st.resident_pages * 100.0 / (double)st.total_pages : 0.0;
    if (!quiet) {
        printf("dropcache: %" PRIu64 " files, %" PRIu64 " bytes, %" PRIu64 " / %" PRIu64
               " pages resident (%.2f%%)",
               st.files, st.bytes, st.resident_pages, st.total_pages, res_pct);
        if (st.fadvise_errors || st.io_errors)
            printf(", %" PRIu64 " fadvise errors, %" PRIu64 " io errors", st.fadvise_errors, st.io_errors);
        printf("\n");
    }

#ifndef POSIX_FADV_DONTNEED
    if (!quiet)
        fprintf(stderr, "dropcache: warning: posix_fadvise(POSIX_FADV_DONTNEED) unavailable on this platform\n");
    if (strict)
        return 1;
#endif
    if (st.fadvise_errors > 0)
        return 1;
    if (strict && st.resident_pages > 0)
        return 1;
    if (max_res_pct >= 0.0 && res_pct > max_res_pct)
        return 1;
    return st.io_errors > 0 ? 1 : 0;
}
