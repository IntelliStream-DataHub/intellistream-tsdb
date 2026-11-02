/*
 * A small database for the file harnesses: four series of four types over two
 * daily partitions, written as parts of 64-row granules (one with a range
 * tombstone), optionally an extra volume, and a few rows left in the WAL. A
 * snapshot (every file and directory under the root) lets a harness restore
 * the same tree before each input, in the same place: volume paths are
 * absolute.
 */
#ifndef TSDB_FUZZ_TEMPLATE_H
#define TSDB_FUZZ_TEMPLATE_H
#include "fuzz.h"
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>

typedef struct snap_file {
    char rel[512];
    uint8_t *d;
    size_t n;
    bool dir;
} snap_file;

typedef struct snap {
    snap_file *f;
    size_t n;
} snap;

static snap *snap_cur;
static size_t snap_root_len;

static int snap_walk(const char *p, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st;
    if (ftw->level == 0 || (flag != FTW_D && flag != FTW_F))
        return 0;
    snap *s = snap_cur;
    s->f = realloc(s->f, (s->n + 1) * sizeof *s->f);
    snap_file *f = &s->f[s->n++];
    memset(f, 0, sizeof *f);
    snprintf(f->rel, sizeof f->rel, "%s", p + snap_root_len + 1);
    f->dir = flag == FTW_D;
    if (!f->dir) {
        char dir[4096];
        snprintf(dir, sizeof dir, "%.*s", (int)snap_root_len, p);
        f->d = fuzz_get(dir, f->rel, &f->n);
    }
    return 0;
}

static inline void snap_take(snap *s, const char *root)
{
    memset(s, 0, sizeof *s);
    snap_cur = s;
    snap_root_len = strlen(root);
    nftw(root, snap_walk, 16, FTW_PHYS); /* pre-order: a directory before its files */
}

/* empties root and writes the snapshot there, leaving out the file named skip (NULL: none) */
static inline void snap_put(const snap *s, const char *root, const char *skip)
{
    fuzz_rm(root);
    mkdir(root, 0755);
    for (size_t i = 0; i < s->n; i++) {
        char path[8192];
        snprintf(path, sizeof path, "%s/%s", root, s->f[i].rel);
        if (s->f[i].dir)
            mkdir(path, 0755);
        else if (!skip || strcmp(s->f[i].rel, skip))
            fuzz_put(root, s->f[i].rel, s->f[i].d, s->f[i].n);
    }
}

/* the first file whose name starts with prefix and ends with suffix, or NULL */
static inline const snap_file *tpl_find(const snap *s, const char *prefix, const char *suffix)
{
    for (size_t i = 0; i < s->n; i++) {
        const char *r = s->f[i].rel;
        size_t lr = strlen(r), ls = strlen(suffix);
        if (!s->f[i].dir && !strncmp(r, prefix, strlen(prefix)) && lr >= ls && !strcmp(r + lr - ls, suffix))
            return &s->f[i];
    }
    return NULL;
}

static inline void template_opts(tsdb_options *o)
{
    tsdb_options_init(o);
    o->fsync = 0;
    o->merge_threads = 0;
    o->granule_rows = 64;
    o->wal_max_batch_rows = 100;
}

/* Builds the template in root. parts: rows written as parts; volume: series "b" in root/vol2; open_snap: a
 * snapshot taken while the database is still open, so the WAL still holds the last rows. */
static inline void template_build(const char *root, bool parts, bool volume, snap *open_snap)
{
    tsdb_options o;
    template_opts(&o);
    tsdb *db;
    char vol[4096];
    snprintf(vol, sizeof vol, "%s/vol2", root);
    if (volume)
        mkdir(vol, 0755);
    if (tsdb_open(root, &o, &db) != 0) {
        fprintf(stderr, "fuzz: cannot create the template in %s\n", root);
        exit(1);
    }
    tsdb_sid id[4];
    tsdb_series_create(db, "a", TSDB_FLOAT32, NULL, &id[0]);
    tsdb_series_create(db, "b", TSDB_FLOAT64, volume ? vol : NULL, &id[1]);
    tsdb_series_create(db, "c", TSDB_INT64, NULL, &id[2]);
    tsdb_series_create(db, "d", TSDB_BOOL, NULL, &id[3]);
    enum { N = 300 };
    int64_t ts[N];
    float f[N];
    double d[N];
    int64_t iv[N];
    uint32_t bv[N];
    uint8_t valid[N];
    for (int i = 0; i < N; i++) {
        ts[i] = 1704067200000 + (int64_t)i * 600000; /* every 10 min: two days */
        f[i] = 20.0f + (float)(i % 37) * 0.1f;
        d[i] = 1000.0 + i * 0.25;
        iv[i] = 5000 + (i * 7919) % 101;
        bv[i] = (uint32_t)(i / 40 & 1);
        valid[i] = i % 17 != 3;
    }
    if (parts) {
        tsdb_values v[4] = {{TSDB_FLOAT32, f, NULL}, {TSDB_FLOAT64, d, valid}, {TSDB_INT64, iv, NULL},
                            {TSDB_BOOL, bv, valid}};
        for (int s = 0; s < 4; s++)
            tsdb_insert_series_values(db, id[s], ts, &v[s], N);
        tsdb_delete_range(db, id[0], ts[50], ts[70]);
    }
    for (int i = 0; i < 20; i++)
        tsdb_append(db, id[0], ts[N - 1] + 1000 * (i + 1), (float)i);
    if (open_snap)
        snap_take(open_snap, root);
    tsdb_close(db);
}
#endif
