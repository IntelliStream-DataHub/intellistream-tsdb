/* SPDX-License-Identifier: AGPL-3.0-only */
// shape: the shape of a RowBinary input (u32 series, i64 ts ms, f32 value per 16-byte row), per series:
//   series rows duplicates unordered nans max_gap_ms first_ms last_ms
// The release run's answers are checked on this data only, and several TWA recipes (QuestDB's twap() with
// SAMPLE BY, the split_once segments of rel.py) and VictoriaMetrics' deduplication are right only when no
// series has a duplicate or out-of-order timestamp, a null (NaN) or a gap longer than a bucket. prep.sh
// checks these numbers against the bucket widths and stops when the data has another shape.
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The u32 is not a tsdb series id (those are 64-bit and carry the volume): it is the series number, an index into the
 * bench's id table, and the table below is indexed by it. */
typedef struct { int64_t rows, dups, unordered, nans, max_gap, first, last; } shape;

int main(int argc, char **argv) {
    if (argc != 2) { fprintf(stderr, "usage: shape FILE.rowbinary\n"); return 2; }
    FILE *f = fopen(argv[1], "rb");
    if (!f) { perror(argv[1]); return 1; }
    size_t cap = 0;
    shape *s = NULL;
    static unsigned char buf[16 << 16];
    size_t n, tail = 0;
    while ((n = fread(buf + tail, 1, sizeof buf - tail, f)) > 0) {
        n += tail;
        size_t whole = n / 16 * 16;
        for (size_t i = 0; i < whole; i += 16) {
            uint32_t id; int64_t ts; float v;
            memcpy(&id, buf + i, 4); memcpy(&ts, buf + i + 4, 8); memcpy(&v, buf + i + 12, 4);
            if (id >= cap) {
                size_t nc = cap ? cap : 1024;
                while (nc <= id) nc *= 2;
                shape *ns = realloc(s, nc * sizeof *s);
                if (!ns) { fprintf(stderr, "shape: out of memory\n"); return 1; }
                memset(ns + cap, 0, (nc - cap) * sizeof *s);
                s = ns; cap = nc;
            }
            shape *p = &s[id];
            if (p->rows == 0) p->first = ts;
            else if (ts == p->last) p->dups++;
            else if (ts < p->last) p->unordered++;
            else if (ts - p->last > p->max_gap) p->max_gap = ts - p->last;
            if (isnan(v)) p->nans++;
            if (p->rows == 0 || ts > p->last) p->last = ts;
            p->rows++;
        }
        tail = n - whole;
        memmove(buf, buf + whole, tail);
    }
    if (ferror(f)) { perror(argv[1]); return 1; }
    if (tail) { fprintf(stderr, "shape: %zu bytes after the last whole row\n", tail); return 1; }
    fclose(f);
    printf("# series rows duplicates unordered nans max_gap_ms first_ms last_ms\n");
    for (size_t i = 0; i < cap; i++)
        if (s[i].rows)
            printf("%zu %lld %lld %lld %lld %lld %lld %lld\n", i, (long long)s[i].rows, (long long)s[i].dups,
                   (long long)s[i].unordered, (long long)s[i].nans, (long long)s[i].max_gap, (long long)s[i].first,
                   (long long)s[i].last);
    free(s);
    return 0;
}
