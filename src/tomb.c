/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Range tombstones (deletes) and the sets that apply them.
 *
 * A delete is a part that holds tombstones: {series, [start, end)}. It hides
 * the rows of that series in the parts of its partition with a lower block
 * number, so "last write wins" orders deletes like any write: an insert
 * published after the delete has a higher block number and stays visible, one
 * published before it is hidden. A part's tombstones never hide its own rows
 * (a replace writes the tombstones and the new rows as one commit), nor
 * memtable rows (memtables are newer than every part, and a delete flushes
 * the memtable before it commits).
 *
 * Readers and merges gather the tombstones of every newer part into a
 * tomb_set: sorted by (series, start), overlapping or touching ranges of one
 * series coalesced, so a row is hidden iff a binary search finds a range
 * around it.
 */
#include "internal.h"

#include <stdlib.h>

int tomb_set_add(tomb_set *s, const tomb *t, size_t n)
{
    if (n == 0)
        return 0;
    if (s->n + n > s->cap) {
        size_t cap = MAX(s->cap * 2, s->n + n);
        tomb *x = realloc(s->t, cap * sizeof *x);
        if (!x)
            return TSDB_ERR_NOMEM;
        s->t = x;
        s->cap = cap;
    }
    memcpy(s->t + s->n, t, n * sizeof *t);
    s->n += n;
    return 0;
}

static int cmp_tomb(const void *a, const void *b)
{
    const tomb *x = a, *y = b;
    if (x->series != y->series)
        return x->series < y->series ? -1 : 1;
    return (x->start > y->start) - (x->start < y->start);
}

void tomb_set_finish(tomb_set *s)
{
    if (s->n < 2)
        return;
    qsort(s->t, s->n, sizeof *s->t, cmp_tomb);
    size_t o = 0;
    for (size_t i = 1; i < s->n; i++) {
        tomb *last = &s->t[o];
        if (s->t[i].series == last->series && s->t[i].start <= last->end)
            last->end = MAX(last->end, s->t[i].end);
        else
            s->t[++o] = s->t[i];
    }
    s->n = o + 1;
}

void tomb_set_free(tomb_set *s)
{
    free(s->t);
    memset(s, 0, sizeof *s);
}

size_t tomb_set_seek(const tomb_set *s, uint32_t series, int64_t ts)
{
    size_t lo = 0, hi = s->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const tomb *t = &s->t[mid];
        if (t->series < series || (t->series == series && t->end <= ts))
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

bool tomb_set_hits(const tomb_set *s, uint32_t series, int64_t lo, int64_t hi)
{
    size_t i = tomb_set_seek(s, series, lo);
    return i < s->n && s->t[i].series == series && s->t[i].start <= hi;
}

size_t tomb_set_filter(const tomb_set *s, uint32_t series, int64_t cutoff, int64_t *ts, uint8_t *v, size_t w,
                       size_t n)
{
    size_t i = 0;
    while (i < n && ts[i] < cutoff)
        i++;
    size_t k = s->n ? tomb_set_seek(s, series, i < n ? ts[i] : INT64_MAX) : s->n;
    size_t o = 0;
    for (; i < n; i++) {
        while (k < s->n && s->t[k].series == series && s->t[k].end <= ts[i])
            k++;
        if (k < s->n && s->t[k].series == series && s->t[k].start <= ts[i])
            continue; /* deleted */
        if (o != i) {
            ts[o] = ts[i];
            memcpy(v + o * w, v + i * w, w);
        }
        o++;
    }
    return o;
}
