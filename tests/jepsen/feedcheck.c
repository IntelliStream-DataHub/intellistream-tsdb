/*
 * The offline check of a database directory whose change feed is on (docs/design-cdc.md 2.6; step F1), run by jepsen
 * on the directory a crash or a power loss left (before the next server recovers it) and after a clean stop. What F2's
 * reader relies on:
 *   - every fwal_ segment is whole: valid to its last byte (a segment is sealed only after its data is synced, and a
 *     torn tail is cut first), so a reader never meets a record that was acknowledged to nobody;
 *   - the seqs above the floor are contiguous: the cleaner removes from the oldest, a rename keeps the order, so a gap is a lost
 *     segment. After a crash the check counts wal_ names too (the open renames them);
 *   - log numbers only increase across segments;
 *   - the floor (FEEDSLOTS) lies below the first number of the oldest segment above it: an event at or below it is
 *     gone and must not be claimed to be retained. Segments wholly below it are leftovers of a cleaner that died
 *     between making the floor durable and unlinking; they are ignored (the next pass removes them).
 * It reads files only, never opens the database.
 */
#define _GNU_SOURCE
#include "../../src/internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FC_MAX 4096

struct fc_seg {
    uint64_t seq;
    bool retained; /* fwal_ (sealed) rather than wal_ (the open does it) */
};

static int fc_cmp(const void *a, const void *b)
{
    const struct fc_seg *x = a, *y = b;
    return x->seq < y->seq ? -1 : x->seq > y->seq;
}

/* The records of one segment: the count, the first and last numbers; -1 when it is torn or unreadable. */
static int fc_walk(int dirfd, const char *name, uint64_t seq, uint64_t *first, uint64_t *last)
{
    int fd = openat(dirfd, name, O_RDONLY | O_CLOEXEC);
    struct stat st;
    if (fd < 0)
        return -2; /* removed by a cleaner while we looked: the caller treats it as gone */
    if (fstat(fd, &st) != 0) {
        close(fd);
        return -1;
    }
    size_t len = (size_t)st.st_size, got = 0;
    uint8_t *b = malloc(len ? len : 1);
    while (b && got < len) {
        ssize_t r = pread(fd, b + got, len - got, (off_t)got);
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    close(fd);
    if (!b || got != len) {
        free(b);
        return -1;
    }
    size_t pos = 0, rec;
    int n = 0;
    wal_rec r;
    *first = *last = 0;
    while (pos < len && (rec = wal_record(b + pos, len - pos, seq, pos, &r)) != 0) {
        if (!n)
            *first = r.fno;
        *last = r.fno + r.span - 1;
        pos += rec;
        n++;
    }
    free(b);
    return pos == len ? n : -1;
}

/* 0 when the directory is consistent (or has no feed; why then says what was seen), else a message in why. crashed: wal_ segments are expected. */
int feedcheck_dir(const char *dir, bool crashed, char *why, size_t cap)
{
    why[0] = 0;
    int dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return 0;
    if (faccessat(dirfd, FEED_FILE, F_OK, 0) != 0) {
        close(dirfd);
        return 0;
    }
    struct fc_seg *s = malloc(FC_MAX * sizeof *s);
    int n = 0, rc = 0;
    DIR *d = fdopendir(dup(dirfd));
    struct dirent *e;
    while (d && (e = readdir(d)) && n < FC_MAX) {
        bool f = fwal_file_name(e->d_name);
        if (f || (crashed && wal_file_name(e->d_name))) {
            s[n].seq = strtoull(e->d_name + (f ? 5 : 4), NULL, 16);
            s[n].retained = f;
            n++;
        }
    }
    if (d)
        closedir(d);
    qsort(s, (size_t)n, sizeof *s, fc_cmp);
    fslots fs;
    bool have_floor = fslots_read(dirfd, &fs) == 0;
    if (!have_floor) {
        snprintf(why, cap, "FEED exists but FEEDSLOTS has no valid half");
        rc = 1;
    }
    /* Segments entirely at or below the floor are leftovers of a cleaner that died between making the floor durable
     * and unlinking (the next pass removes them first): the floor already excludes them, so only the segments above it
     * must be contiguous and whole in order. A segment that straddles the floor is a violation: the floor is the last
     * number of a whole segment. */
    uint64_t prev_last = 0;
    int prev_i = -1;
    bool seen_records = false;
    for (int i = 0; i < n && !rc; i++) {
        char name[64];
        uint64_t f, l;
        if (s[i].retained)
            fwal_name(name, sizeof name, s[i].seq);
        else
            snprintf(name, sizeof name, "wal_%016" PRIx64 ".log", s[i].seq);
        int recs = fc_walk(dirfd, name, s[i].seq, &f, &l);
        if (recs == -2) { /* nothing runs a cleaner on a directory we check */
            snprintf(why, cap, "%s vanished during the check", name);
            rc = 1;
        } else if (recs < 0) {
            /* a wal_ may be torn only as the newest segment of a crashed directory */
            if (s[i].retained || !crashed || i + 1 < n) {
                snprintf(why, cap, "%s is torn or damaged", name);
                rc = 1;
            }
            prev_i = i;
        } else if (recs == 0 && s[i].retained) {
            snprintf(why, cap, "%s is retained but holds no record", name);
            rc = 1;
        } else if (recs > 0 && have_floor && l <= fs.floor_event) {
            continue; /* a leftover below the floor: not part of the history */
        } else {
            if (recs > 0 && have_floor && f <= fs.floor_event) {
                snprintf(why, cap, "%s (%" PRIu64 "..%" PRIu64 ") straddles the floor %" PRIu64, name, f, l,
                         fs.floor_event);
                rc = 1;
                break;
            }
            if (prev_i >= 0 && s[i].seq != s[prev_i].seq + 1) {
                snprintf(why, cap, "segment seqs not contiguous above the floor: %s follows seq %" PRIu64, name,
                         s[prev_i].seq);
                rc = 1;
                break;
            }
            if (recs > 0) {
                if (seen_records && f <= prev_last) {
                    snprintf(why, cap, "%s starts at %" PRIu64 ", not above %" PRIu64, name, f, prev_last);
                    rc = 1;
                }
                seen_records = true;
                prev_last = l;
            }
            prev_i = i;
        }
    }
    if (!rc && have_floor) /* what it saw, for the run's report */
        snprintf(why, cap, "%d segments, %" PRIu64 " numbers kept, floor %" PRIu64 ", %" PRIu64 " floor generation", n,
                 seen_records ? prev_last - fs.floor_event : 0, fs.floor_event, fs.gen);
    free(s);
    close(dirfd);
    return rc;
}
