/* SPDX-License-Identifier: AGPL-3.0-only */
/* tsdbd --read-only, op by op (generated from TSDB_OPS like tests/test_auth_ops.c): a database written and flushed
 * with the library, then served read-only without login. Every op that would write answers TSDB_ERR_READONLY (a
 * minimal well-formed frame for each, so a refusal for a malformed frame cannot pass for the right one); every op
 * that reads answers as the library does; TOPOLOGY says follower; and not one byte of the directory changes, neither
 * while the ops run nor in the server's shutdown (a content hash of every file, a new or missing file counts).
 * An op of TSDB_OPS without a row here fails the test, so a new write op cannot slip in unchecked.
 *
 *   TSDBD=build/tsdbd TSDBD_IO=epoll ./build/test_readonly [scratch]
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_crypto.h"
#include "tsdb_protocol.h"
#include "users.h"
#include "harness.h"
#include "auth_harness.h"

#include <dirent.h>

/* W: writes, must be READONLY. R: reads, must answer 0 (COMMIT and FLUSH too: a read-only engine has nothing to flush,
 * and the disk check below is what says they wrote nothing). X: not a data op here (login, user admin): refused, not
 * with READONLY, and the server answers. Q: CANCEL, which has no reply of its own. */
enum { W, R, X, Q, F };
/* F: a write that a read-only engine answers NOTFOUND, as it does for a name a follower has not seen yet (eng_state,
 * dict_get: the state dictionary is reloaded, and a name that is still missing is NOTFOUND, never created). */

typedef struct row {
    uint8_t op, kind, variant;
    const char *what;
} row;

static const row rows[] = {
    {TSDB_OP_HELLO, R, 0, "hello"},
    {TSDB_OP_SERIES, R, 0, "series lookup"},
    {TSDB_OP_SERIES, W, 1, "series create by name"},
    {TSDB_OP_SERIES_CREATE, W, 0, "series_create"},
    {TSDB_OP_SERIES_NAME, R, 0, "series_name"},
    {TSDB_OP_SERIES_TYPE, R, 0, "series_type"},
    {TSDB_OP_INSERT, W, 0, "insert"},
    {TSDB_OP_INSERT_VALUES, W, 0, "insert_values"},
    {TSDB_OP_QUERY_RAW, R, 0, "query_raw"},
    {TSDB_OP_QUERY_VALUES, R, 0, "query_values"},
    {TSDB_OP_QUERY_BUCKETS, R, 0, "query_buckets"},
    {TSDB_OP_QUERY_BOUNDS, R, 0, "query_bounds"},
    {TSDB_OP_QUERY_LTTB, R, 0, "query_lttb"},
    {TSDB_OP_QUERY_TWA, R, 0, "query_twa"},
    {TSDB_OP_QUERY_MULTI, R, 0, "query_multi"},
    {TSDB_OP_QUERY_LATEST, R, 0, "query_latest"},
    {TSDB_OP_STATE, F, 1, "state create"},
    {TSDB_OP_STATE, R, 0, "state lookup"},
    {TSDB_OP_STATE_NAME, R, 0, "state_name"},
    {TSDB_OP_COMMIT, R, 0, "commit (nothing to flush)"},
    {TSDB_OP_FLUSH, R, 0, "flush (nothing to flush)"},
    {TSDB_OP_OPTIMIZE, W, 0, "optimize"},
    {TSDB_OP_DELETE_RANGE, W, 0, "delete_range"},
    {TSDB_OP_REPLACE_RANGE, W, 0, "replace_range"},
    {TSDB_OP_SET_RETENTION, W, 0, "set_retention"},
    {TSDB_OP_GET_RETENTION, R, 0, "get_retention"},
    {TSDB_OP_STATS, R, 0, "stats"},
    {TSDB_OP_LAST_STATS, R, 0, "last_stats"},
    {TSDB_OP_CANCEL, Q, 0, "cancel"},
    {TSDB_OP_TOPOLOGY, R, 0, "topology"},
    {TSDB_OP_STATUS, R, 0, "status"},
    {TSDB_OP_WHOAMI, R, 0, "whoami"},
    {TSDB_OP_AUTH_START, X, 0, "auth_start"},
    {TSDB_OP_AUTH_CONTINUE, X, 0, "auth_continue"},
    {TSDB_OP_USER_CREATE, X, 0, "user_create"},
    {TSDB_OP_USER_DROP, X, 0, "user_drop"},
    {TSDB_OP_USER_ALTER, X, 0, "user_alter"},
    {TSDB_OP_USER_PASSWORD, X, 0, "user_password"},
    {TSDB_OP_GRANT, X, 0, "grant"},
    {TSDB_OP_REVOKE, X, 0, "revoke"},
    {TSDB_OP_USER_LIST, X, 0, "user_list"},
};
#define NROWS (sizeof rows / sizeof *rows)

static tsdb_sid fid, iid, sid_; /* ro/f (float32), ro/i (int64), ro/s (state, code "on") */

static void put16(uint8_t *b, uint16_t v) { memcpy(b, &v, 2); }
static void put32(uint8_t *b, uint32_t v) { memcpy(b, &v, 4); }
static void put64(uint8_t *b, uint64_t v) { memcpy(b, &v, 8); }

/* A minimal well-formed payload; the frames are those of test_auth_ops.c's build(). */
static size_t build(const row *s, uint8_t *b)
{
    const char *name = s->variant == 1 && s->op == TSDB_OP_SERIES ? "ro/new" : "ro/f";
    size_t nl = strlen(name);
    uint64_t t0 = (uint64_t)T0, t1 = (uint64_t)T0 + 1000;
    float one = 1;
    tsdb_sid id = fid;
    memset(b, 0, 512);
    switch (s->op) {
    case TSDB_OP_HELLO: put32(b, TSDB_PROTOCOL_VERSION); return 4;
    case TSDB_OP_CANCEL: put32(b, 12345); return 4;
    case TSDB_OP_SERIES: b[0] = s->variant; put16(b + 1, (uint16_t)nl); memcpy(b + 3, name, nl); return 3 + nl;
    case TSDB_OP_SERIES_CREATE: b[0] = TSDB_FLOAT32; put16(b + 1, 7); memcpy(b + 3, "ro/new2", 7); return 5 + 7;
    case TSDB_OP_SERIES_NAME: case TSDB_OP_SERIES_TYPE: case TSDB_OP_GET_RETENTION: put64(b, id); return 8;
    case TSDB_OP_STATE: /* the create variant names a code the database does not have */
        id = sid_; put64(b, id); b[8] = s->variant; put16(b + 9, s->variant ? 3 : 2);
        memcpy(b + 11, s->variant ? "off" : "on", s->variant ? 3 : 2); return s->variant ? 14 : 13;
    case TSDB_OP_STATE_NAME: id = sid_; put64(b, id); put32(b + 8, 1); return 12;
    case TSDB_OP_INSERT: case TSDB_OP_INSERT_VALUES:
        put32(b, 1); put64(b + 8, id); put64(b + 16, t0); memcpy(b + 24, &one, 4); return 28;
    case TSDB_OP_QUERY_RAW: case TSDB_OP_QUERY_VALUES: case TSDB_OP_DELETE_RANGE:
        put64(b, id); put64(b + 8, t0); put64(b + 16, t1); return 24;
    case TSDB_OP_QUERY_LTTB: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); put64(b + 24, 3); return 32;
    case TSDB_OP_QUERY_BUCKETS: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); return 48;
    case TSDB_OP_QUERY_BOUNDS: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); put32(b + 33, 1);
        put64(b + 37, t0); return 45;
    case TSDB_OP_QUERY_TWA: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); return TSDB_TWA_HEADER;
    case TSDB_OP_SET_RETENTION: put64(b, id); return 16;
    case TSDB_OP_REPLACE_RANGE: put32(b, 1); put64(b + 8, id); put64(b + 16, t0); put64(b + 24, t1);
        put64(b + 32, t0); memcpy(b + 40, &one, 4); return 44;
    case TSDB_OP_QUERY_MULTI:
        b[0] = TSDB_OP_QUERY_VALUES; put32(b + 4, 1); put32(b + 16, 24); put64(b + 24, id);
        put64(b + 32, TSDB_MANY_SERIES); put64(b + 40, t0); put64(b + 48, t1); return 56;
    case TSDB_OP_QUERY_LATEST: put32(b + 4, 1); put64(b + 8, (uint64_t)INT64_MAX); put64(b + 16, id); return 24;
    default: return 0;
    }
}

#define NOREPLY (INT32_MIN + 1)
#define CLOSED INT32_MIN

/* The request's first reply frame's status, with a WHOAMI behind it to know where its answer ends. */
static int32_t ask(int fd, uint8_t op, const uint8_t *p, size_t len)
{
    raw_send(fd, 100, op, p, (uint32_t)len);
    raw_send(fd, 101, TSDB_OP_WHOAMI, NULL, 0);
    int32_t first = NOREPLY;
    for (;;) {
        uint32_t id = 0;
        uint8_t rest[64];
        size_t n;
        int32_t st = raw_frame(fd, &id, rest, sizeof rest, &n, NULL, NULL);
        if (st == INT32_MIN)
            return CLOSED;
        if (id == 101)
            return first;
        if (id == 100 && first == NOREPLY) {
            first = st;
            if (op == TSDB_OP_QUERY_LATEST && st == 0 && n >= TSDB_LATEST_PREFIX + 4)
                memcpy(&first, rest + TSDB_LATEST_PREFIX, 4);
        }
    }
}

/* ------------------------------------------------------------ the directory's content */

static uint64_t fnv(uint64_t h, const void *p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        h = (h ^ ((const uint8_t *)p)[i]) * 1099511628211ull;
    return h;
}

/* A hash of every path, size and content under dir; *files counts the files. Names are visited in sorted order. */
static int cmp_names(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

static uint64_t tree_hash(const char *dir, size_t *files, uint64_t h)
{
    DIR *d = opendir(dir);
    if (!d) {
        CHECK(!"opendir");
        return h;
    }
    char *names[4096];
    size_t n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL && n < 4096)
        if (strcmp(e->d_name, ".") && strcmp(e->d_name, ".."))
            names[n++] = strdup(e->d_name);
    closedir(d);
    qsort(names, n, sizeof *names, cmp_names);
    for (size_t i = 0; i < n; i++) {
        char path[PATH_MAX + 300];
        snprintf(path, sizeof path, "%s/%s", dir, names[i]);
        h = fnv(h, names[i], strlen(names[i]));
        struct stat st;
        if (stat(path, &st) != 0) {
            CHECK(!"stat");
        } else if (S_ISDIR(st.st_mode)) {
            h = tree_hash(path, files, h);
        } else {
            (*files)++;
            h = fnv(h, &st.st_size, sizeof st.st_size);
            FILE *f = fopen(path, "rb");
            uint8_t buf[65536];
            size_t k;
            while (f && (k = fread(buf, 1, sizeof buf, f)) > 0)
                h = fnv(h, buf, k);
            if (f)
                fclose(f);
        }
        free(names[i]);
    }
    return h;
}

static void make_db(const char *dir, size_t n)
{
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_create(db, "ro/f", TSDB_FLOAT32, NULL, &fid));
    OK(tsdb_series_create(db, "ro/i", TSDB_INT64, NULL, &iid));
    OK(tsdb_series_create(db, "ro/s", TSDB_STATE, NULL, &sid_));
    uint32_t code;
    OK(tsdb_state(db, sid_, "on", &code)); /* STATE_NAME and the lookup find it; the create op names another */
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    for (size_t i = 0; i < n; i++) {
        ts[i] = T0 + (int64_t)i * 10;
        v[i] = (float)i * 0.5f;
    }
    OK(tsdb_insert_series(db, fid, ts, v, n));
    OK(tsdb_set_retention(db, fid, 365LL * 86400 * 1000 * 100));
    OK(tsdb_flush(db));
    free(ts);
    free(v);
    tsdb_close(db);
}

static void reads_match(tsdbc *c)
{
    size_t n = 0;
    OK(tsdbc_query_raw(c, fid, INT64_MIN, INT64_MAX, count_fn, &n));
    CHECK(n == 1000);
    tsdb_bucket *b;
    size_t nb;
    OK(tsdbc_query_agg(c, fid, INT64_MIN, INT64_MAX, 0, &b, &nb));
    CHECK(nb == 1 && b[0].count == 1000);
    free(b);
    int64_t lim = 0;
    OK(tsdbc_get_retention(c, fid, &lim));
    CHECK(lim == 365LL * 86400 * 1000 * 100);
    tsdb_type ty;
    OK(tsdbc_series_type(c, iid, &ty));
    CHECK(ty == TSDB_INT64);
    tsdb_sid again = 0;
    OK(tsdbc_series(c, "ro/f", 0, &again));
    CHECK(again == fid);
    tsdb_sid nope = 0;
    CHECK(tsdbc_series(c, "ro/new", 0, &nope) == TSDB_ERR_NOTFOUND); /* the create op above made nothing */
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    th_watchdog("test_readonly", 600);
    printf("test_readonly (%s)\n", getenv("TSDBD_IO") ? getenv("TSDBD_IO") : "default io");
    char dir[PATH_MAX + 16];
    snprintf(dir, sizeof dir, "%s/tsdbd-readonly-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    ah_setup(dir);
    make_db(dir, 1000);

    /* every op of the protocol has a row */
    static const uint8_t listed[] = {
#define OPNUM_(name, num, metric) num,
        TSDB_OPS(OPNUM_)
#undef OPNUM_
    };
    for (size_t i = 0; i < sizeof listed; i++) {
        bool have = false;
        for (size_t j = 0; j < NROWS; j++)
            have |= rows[j].op == listed[i];
        if (!have) {
            fprintf(stderr, "test_readonly: op %u of TSDB_OPS has no row in rows[]\n", listed[i]);
            failures++;
        }
    }

    size_t files0 = 0, files1 = 0, files2 = 0;
    uint64_t h0 = tree_hash(dir, &files0, 14695981039346656037ull);
    CHECK(files0 > 0);
    const char *extra[] = {"--no-auth", "--read-only", NULL};
    pid_t srv = start(dir, extra);
    int fd = raw_connect();
    int nw = 0, nr = 0;
    for (size_t i = 0; i < NROWS; i++) {
        const row *s = &rows[i];
        uint8_t b[512];
        size_t len = build(s, b);
        int32_t st = ask(fd, s->op, b, len);
        if (s->kind == W) {
            nw++;
            if (st != TSDB_ERR_READONLY) {
                fprintf(stderr, "test_readonly: %s answered %d, want READONLY (%d)\n", s->what, (int)st,
                        TSDB_ERR_READONLY);
                failures++;
            }
        } else if (s->kind == F) {
            nw++;
            if (st != TSDB_ERR_NOTFOUND) {
                fprintf(stderr, "test_readonly: %s answered %d, want NOTFOUND (%d)\n", s->what, (int)st,
                        TSDB_ERR_NOTFOUND);
                failures++;
            }
        } else if (s->kind == Q) {
            if (st != NOREPLY) {
                fprintf(stderr, "test_readonly: %s answered %d, want no reply\n", s->what, (int)st);
                failures++;
            }
        } else if (s->kind == R) {
            nr++;
            if (st != 0) {
                fprintf(stderr, "test_readonly: %s answered %d, want 0\n", s->what, (int)st);
                failures++;
            }
        } else if (st == TSDB_ERR_READONLY || st == CLOSED || st == NOREPLY) {
            /* login and user admin have no meaning here; what they must not be is a crash or silence */
            fprintf(stderr, "test_readonly: %s answered %d (a refusal other than READONLY expected)\n", s->what,
                    (int)st);
            failures++;
        }
        if (st == CLOSED) { /* the server went away: reconnect so one failure does not hide the rest */
            close(fd);
            fd = raw_connect();
        }
    }
    CHECK(nw >= 9 && nr >= 20); /* the table holds what it claims */
    close(fd);

    tsdbc *c;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    reads_match(c);
    /* the typed client calls too: a refusal is READONLY and nothing more is lost */
    int64_t one_ts = T0 + 5000;
    float one_v = 1;
    CHECK(tsdbc_insert_series(c, fid, &one_ts, &one_v, 1) == TSDB_ERR_READONLY);
    CHECK(tsdbc_delete_range(c, fid, T0, T0 + 100) == TSDB_ERR_READONLY);
    CHECK(tsdbc_set_retention(c, fid, 1000) == TSDB_ERR_READONLY);
    CHECK(tsdbc_flush(c) == 0); /* nothing to flush */
    CHECK(tsdbc_optimize(c) == TSDB_ERR_READONLY);
    tsdbc_topo *tp = NULL;
    OK(tsdbc_topology(c, &tp));
    CHECK(tp && tp->nvols >= 1);
    for (size_t v = 0; tp && v < tp->nvols; v++)
        CHECK(tp->vols[v].role == TSDB_ROLE_FOLLOWER);
    tsdbc_topology_free(tp);
    reads_match(c); /* and the answers are the same after every refusal */
    tsdbc_close(c);

    uint64_t h1 = tree_hash(dir, &files1, 14695981039346656037ull);
    if (h1 != h0 || files1 != files0)
        fprintf(stderr, "test_readonly: the directory changed while the server ran (%zu files before, %zu after)\n",
                files0, files1);
    CHECK(h1 == h0 && files1 == files0);
    stop(srv);
    uint64_t h2 = tree_hash(dir, &files2, 14695981039346656037ull);
    if (h2 != h0 || files2 != files0)
        fprintf(stderr, "test_readonly: the directory changed in the server's shutdown (%zu files before, %zu after)\n",
                files0, files2);
    CHECK(h2 == h0 && files2 == files0);

    if (failures) {
        fprintf(stderr, "test_readonly: %d failures (server log %s)\n", failures, srvlog);
        return 1;
    }
    char cmd[3 * PATH_MAX + 64];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'", dir, srvlog);
    if (system(cmd) != 0) {
        fprintf(stderr, "test_readonly: could not remove %s\n", dir);
        return 1;
    }
    printf("test_readonly: ok (%d refused writes, %d reads, %zu files unchanged)\n", nw, nr, files0);
    return 0;
}
