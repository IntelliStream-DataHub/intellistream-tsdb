/* SPDX-License-Identifier: AGPL-3.0-only */
/* The DIVERGED file (ecdiverge.h has the design and the record layout). */
#include "internal.h"
#include "ecdiverge.h"
#include "feed.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DIV_FILE "DIVERGED"
#define DIV_REC 48

int ecdiverge_mutant;
int ecdiverge_test_misread;

struct divrec {
    int64_t key, ms;
    uint32_t why, flags;
    uint8_t oid[16];
};

struct ecdiverge {
    pthread_mutex_t mu;
    int fd;          /* opened at the first append */
    uint64_t off;    /* the end of the valid records: the next append goes here */
    uint64_t size;   /* the file's size at the open: a torn tail past off is cut by the first append */
    bool broken;     /* a write or sync failed: the engine failed, nothing more is appended */
    size_t n, cap;
    struct divrec *r;
};

static uint32_t rec_check(const uint8_t *rec, uint64_t off)
{
    return (uint32_t)xxh64(rec + 4, DIV_REC - 4, ecdiverge_mutant == ECDIV_MUT_SEED0 ? 0 : off);
}

static bool rec_ok(const uint8_t *rec, uint64_t off)
{
    uint32_t c, zero;
    memcpy(&c, rec, 4);
    memcpy(&zero, rec + 44, 4);
    return c == rec_check(rec, off) && zero == 0;
}

static void rec_decode(const uint8_t *rec, struct divrec *o)
{
    memcpy(&o->why, rec + 4, 4);
    memcpy(&o->key, rec + 8, 8);
    memcpy(o->oid, rec + 16, 16);
    memcpy(&o->ms, rec + 32, 8);
    memcpy(&o->flags, rec + 40, 4);
}

static void rec_encode(uint8_t *rec, const struct divrec *r, uint64_t off)
{
    memset(rec, 0, DIV_REC);
    memcpy(rec + 4, &r->why, 4);
    memcpy(rec + 8, &r->key, 8);
    memcpy(rec + 16, r->oid, 16);
    memcpy(rec + 32, &r->ms, 8);
    memcpy(rec + 40, &r->flags, 4);
    uint32_t c = rec_check(rec, off);
    memcpy(rec, &c, 4);
}

static const char *why_name(uint32_t why)
{
    return why == EC_DIV_DIGEST ? "a settle's digest did not match"
           : why == EC_DIV_PPOS ? "a part above the cold point at a cold apply"
           : why == EC_DIV_TEST ? "planted by a test"
                                : "an unknown reason";
}

static void rec_line(const struct divrec *r, char *out, size_t cap)
{
    char oid[33] = "none";
    if (r->flags & 1)
        for (int i = 0; i < 16; i++)
            snprintf(oid + 2 * i, 3, "%02x", r->oid[i]);
    snprintf(out, cap, "key %" PRId64 " object %s at %" PRId64 " ms: %s", r->key, oid, r->ms, why_name(r->why));
}

static int push(struct ecdiverge *d, const struct divrec *r)
{
    if (d->n == d->cap) {
        size_t cap = d->cap ? d->cap * 2 : 4;
        struct divrec *x = realloc(d->r, cap * sizeof *x);
        if (!x)
            return TSDB_ERR_NOMEM;
        d->r = x;
        d->cap = cap;
    }
    d->r[d->n++] = *r;
    return 0;
}

/* The records of buf[0..size) into d; *end: the end of the valid ones (a torn tail lies past it). TSDB_ERR_CORRUPT: a
 * bad record with a valid one after it. */
static int parse(const uint8_t *buf, size_t size, struct ecdiverge *d, const char *dir, size_t *end)
{
    int rc = 0;
    size_t pos = 0;
    for (; rc == 0 && pos + DIV_REC <= size && rec_ok(buf + pos, pos); pos += DIV_REC) {
        struct divrec r;
        rec_decode(buf + pos, &r);
        rc = push(d, &r);
    }
    for (size_t q = pos + DIV_REC; rc == 0 && q + DIV_REC <= size; q += DIV_REC)
        if (rec_ok(buf + q, q)) {
            fprintf(stderr, "tsdb: %s/%s is damaged at offset %zu, before valid records\n", dir, DIV_FILE, pos);
            if (ecdiverge_mutant != ECDIV_MUT_NOCORRUPT)
                rc = TSDB_ERR_CORRUPT;
            break;
        }
    *end = pos;
    return rc;
}

int ecdiverge_parse(const uint8_t *buf, size_t len, size_t *n)
{
    struct ecdiverge d = {.fd = -1};
    size_t end;
    int rc = parse(buf, len, &d, "(fuzz)", &end);
    *n = d.n;
    free(d.r);
    return rc;
}

/* Reads DIVERGED in dirfd into d:
 0 (no file: no records), TSDB_ERR_IO, TSDB_ERR_CORRUPT (a bad record before a valid
 * one). `confirm`: a writer's open confirms a torn tail by a second read before it may be cut. */
static int load(int dirfd, const char *dir, struct ecdiverge *d, bool confirm)
{
    int fd = openat(dirfd, DIV_FILE, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size, got = 0;
    uint8_t *buf = malloc(size ? size : 1);
    if (!buf) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    while (got < size) {
        ssize_t r = pread(fd, buf + got, size - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break; /* an error or a file that shrank: never the end of the records */
        got += (size_t)r;
    }
    close(fd);
    int rc = 0;
    if (got != size) {
        if (ecdiverge_mutant == ECDIV_MUT_READERREND)
            size = got;
        else
            rc = TSDB_ERR_IO;
    }
    if (rc == 0 && size && ecdiverge_test_misread) /* a misread page (tests): the second read sees the disk's bytes */
        buf[size - 1] ^= 0x5a;
    size_t pos = 0;
    if (rc == 0)
        rc = parse(buf, size, d, dir, &pos);
    if (rc == 0 && confirm && pos < size && ecdiverge_mutant != ECDIV_MUT_NOTAILCONFIRM)
        rc = tail_confirm(dirfd, DIV_FILE, buf + pos, pos, size - pos);
    d->off = pos;
    d->size = size;
    free(buf);
    return rc;
}

int ecdiverge_open(struct eng *db)
{
    struct ecdiverge *d = calloc(1, sizeof *d);
    if (!d)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&d->mu, NULL);
    d->fd = -1;
    db->diverge = d;
    int rc = load(db->dirfd, db->dir, d, !db->opt.read_only);
    if (rc)
        return rc;
    /* The name of a DIVERGED whose writer died between its fdatasync and its directory fsync is made durable by this
     * open before anything acts on the flag: the WAL open fsyncs the directory (wal_open in memtable.c, before
     * any thread starts). test_diverge's preexist case fails without that fsync. */
    if (d->n) {
        char line[160];
        rec_line(&d->r[0], line, sizeof line);
        fprintf(stderr,
                "tsdb: %s: the volume is DIVERGED (%zu record%s in %s, the first: %s): refused until an operator runs "
                "`tsdb DIR volume diverged --clear`\n",
                db->dir, d->n, d->n == 1 ? "" : "s", DIV_FILE, line);
        atomic_store_explicit(&db->diverged, true, memory_order_release);
    }
    return 0;
}

void ecdiverge_close(struct eng *db)
{
    struct ecdiverge *d = db->diverge;
    if (!d)
        return;
    if (d->fd >= 0)
        close(d->fd);
    pthread_mutex_destroy(&d->mu);
    free(d->r);
    free(d);
    db->diverge = NULL;
}

int ec_diverge(struct eng *db, int64_t key, const uint8_t *oid, int why)
{
    struct ecdiverge *d = db->diverge;
    struct divrec r = {.key = key, .why = (uint32_t)why, .flags = oid ? 1u : 0u};
    if (oid)
        memcpy(r.oid, oid, 16);
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    r.ms = (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    int rc = 0;
    if (!d || db->opt.read_only) { /* no file to write (a reader): refused in memory, as before R8 */
        atomic_store_explicit(&db->diverged, true, memory_order_release);
        return 0;
    }
    pthread_mutex_lock(&d->mu);
    for (size_t i = 0; i < d->n; i++)
        if (d->r[i].key == r.key && d->r[i].why == r.why && d->r[i].flags == r.flags &&
            !memcmp(d->r[i].oid, r.oid, 16))
            goto done; /* already durable (a restart's digest that failed again) */
    if (d->broken || eng_check(db)) {
        rc = TSDB_ERR_IO;
        goto done;
    }
    if (ecdiverge_mutant != ECDIV_MUT_MEM) {
        const char *step = "open";
        /* The first record of the file syncs the directory, whoever made the file (an empty one a process left when
         * it died is synced by the open's replay too, but nothing here relies on it). */

        bool first = d->n == 0;
        int err = 0;
        if (d->fd < 0)
            d->fd = openat(db->dirfd, DIV_FILE, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (d->fd < 0) {
            err = errno;
        } else if (d->size > d->off && ecdiverge_mutant != ECDIV_MUT_NOCUT &&
                   (step = "cut of a torn tail", ftruncate(d->fd, (off_t)d->off) != 0)) {
            err = errno;
        } else {
            d->size = d->off;
            uint8_t rec[DIV_REC];
            rec_encode(rec, &r, d->off);
            ssize_t w;
            step = "write";
            do
                w = pwrite(d->fd, rec, DIV_REC, (off_t)d->off);
            while (w < 0 && errno == EINTR);
            if (w != DIV_REC)
                err = w < 0 ? errno : EIO;
            else if ((step = "fdatasync", ecdiverge_mutant != ECDIV_MUT_NOFDATASYNC && tsdb_fdatasync(d->fd) != 0))
                err = errno;
        }
        if (err) {
            d->broken = true;
            char what[64];
            snprintf(what, sizeof what, "the %s of %s", step, DIV_FILE);
            if (ecdiverge_mutant != ECDIV_MUT_NOFAIL)
                eng_fail(db, what, err);
            rc = TSDB_ERR_IO;
            goto done;
        }
        d->off += DIV_REC;
        d->size = d->off;
        if (first && ecdiverge_mutant != ECDIV_MUT_NODIRSYNC &&
            (rc = eng_sync_dir(db, DIV_FILE)) != 0) { /* fails the engine itself */

            d->broken = true;
            goto done;
        }
    }
    rc = push(d, &r); /* NOMEM: the next call writes the record again, which only repeats it */
    if (rc == TSDB_ERR_NOMEM)
        rc = 0;
done:
    pthread_mutex_unlock(&d->mu);
    atomic_store_explicit(&db->diverged, true, memory_order_release);
    return rc;
}

int ecdiverge_cli(const char *dir, bool clear, FILE *out)
{
    /* Strip trailing slashes (except "/") so O_NOFOLLOW refuses symlinked target directories too. */
    size_t dlen = strlen(dir);
    while (dlen > 1 && dir[dlen - 1] == '/')
        dlen--;
    char *dcopy = NULL;
    const char *p = dir;
    if (dir[dlen] != '\0') {
        dcopy = malloc(dlen + 1);
        if (!dcopy)
            return TSDB_ERR_NOMEM;
        memcpy(dcopy, dir, dlen);
        dcopy[dlen] = '\0';
        p = dcopy;
    }
    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    int oerr = errno;
    free(dcopy);
    if (dirfd < 0) {
        fprintf(stderr, "tsdb: %s: %s\n", dir, strerror(oerr));
        return oerr == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    /* --clear decides from META whether this is a cluster volume (D23): a META that cannot be read refuses, never
     * counts as "not a cluster volume"; it also says that DIR is a database directory at all. The writer's LOCK is
     * taken before the records are read, so that every record removed is one this command logged. */
    unsigned feats = 0;
    int rc = 0, lockfd = -1;
    if (clear) {
        char meta[4096];
        ssize_t n = -1;
        int mfd = openat(dirfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (mfd >= 0) {
            n = read(mfd, meta, sizeof meta - 1);
            close(mfd);
        }
        if (n >= 0) {
            meta[n] = 0;
            meta_requires_parse(meta, &feats, NULL, 0);
        } else if (ecdiverge_mutant != ECDIV_MUT_METAOPEN) {
            fprintf(stderr, "tsdb: %s/META cannot be read (%s): not cleared, whether it is a cluster volume is "
                            "unknown\n", dir, strerror(errno));
            rc = errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
        }
    }
    if (rc == 0 && clear && (feats & META_REQ_CLUSTER) && ecdiverge_mutant != ECDIV_MUT_CLEARCLUSTER) {
        fprintf(stderr,
                "tsdb: %s is a cluster volume's directory: its DIVERGED is cleared only by rebuilding the replica, "
                "which comes with R4 (design-cluster-data.md D23: a diverged replica back in the ISR could be promoted "
                "with its diverged rows): refused\n",
                dir);
        rc = TSDB_ERR_INVALID;
    }
    if (rc == 0 && clear) {
        lockfd = openat(dirfd, "LOCK", O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
        if (lockfd < 0 || flock(lockfd, LOCK_EX | LOCK_NB) != 0) {
            fprintf(stderr, "tsdb: %s: a writer (tsdbd, or another tsdb command) holds the directory: stop it first; "
                            "nothing is cleared\n", dir);
            rc = lockfd < 0 ? TSDB_ERR_IO : TSDB_ERR_LOCKED;
        }
    }
    struct ecdiverge d = {.fd = -1};
    if (rc == 0)
        rc = load(dirfd, dir, &d, false);
    if (rc == 0) {
        for (size_t i = 0; i < d.n; i++) {
            char line[160];
            rec_line(&d.r[i], line, sizeof line);
            fprintf(out, "%s\tDIVERGED\t%s\n", dir, line);
        }
        if (!d.n)
            fprintf(out, "%s\tnot diverged\n", dir);
    }
    if (rc == 0 && clear && d.n) {

        for (size_t i = 0; i < d.n; i++) {
            char line[160];
            rec_line(&d.r[i], line, sizeof line);
            fprintf(stderr, "tsdb: %s: removing the DIVERGED record %s\n", dir, line);
        }
        if (unlinkat(dirfd, DIV_FILE, 0) != 0 || tsdb_fsync(dirfd) != 0) {
            fprintf(stderr, "tsdb: %s: removing %s failed: %s\n", dir, DIV_FILE, strerror(errno));
            rc = TSDB_ERR_IO;
        } else {
            fprintf(stderr, "tsdb: %s: DIVERGED cleared; the next open serves the volume again\n", dir);
        }
    }
    if (lockfd >= 0)
        close(lockfd);
    close(dirfd);
    free(d.r);
    return rc ? rc : (int)d.n;
}
