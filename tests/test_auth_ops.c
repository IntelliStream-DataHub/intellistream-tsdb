/* Every op across tenants (docs/design-auth.md §5 and §9), generated from TSDB_OPS: tsdbd with login on, users of
 * acme/ holding all rights there, each single right on one series, read on all, no grants, and rights given by a
 * glob. For every op of the list a minimal well-formed frame goes out over a logged-in raw connection, naming a
 * series of the user's own (the answer must be the superuser's) and one of globex/ (by id or by name: DENIED), and
 * the answer is checked against this file's copy of §5's table: the test's own, so a wrong row in server/auth.c's
 * table is a difference, not a second copy of the mistake. An op in TSDB_OPS without a row here fails the test, as
 * does one without a row in auth.c's table; every op number outside the list is DENIED. Then QUERY_MULTI with mixed
 * series, inserts (packed and plain, a series per row) with one foreign id among thousands, the escalation cases of
 * §2 over the wire, and the rights map with globs.
 *
 *   TSDBD=build/tsdbd TSDBD_IO=epoll ./build/test_auth_ops [scratch]
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_crypto.h"
#include "tsdb_protocol.h"
#include "auth.h"
#include "users.h"
#include "harness.h"
#include "auth_harness.h"

/* §5's table, as this test reads it */
enum { K_ANY, K_PRE, K_LOGIN, K_SERIES, K_SUPER, K_STATS, K_COMMIT, K_ADMIN };
enum { BY_NONE, BY_ID, BY_NAME };
enum { N_ANY = 0x80, N_R = 1, N_I = 2, N_D = 4 };

typedef struct spec {
    uint8_t op, kind, need, by, variant; /* variant: SERIES' and STATE's create byte */
} spec;

static const spec specs[] = {
    {TSDB_OP_HELLO, K_ANY, 0, BY_NONE, 0},
    {TSDB_OP_AUTH_START, K_PRE, 0, BY_NONE, 0},
    {TSDB_OP_AUTH_CONTINUE, K_PRE, 0, BY_NONE, 0},
    {TSDB_OP_CANCEL, K_LOGIN, 0, BY_NONE, 0},
    {TSDB_OP_LAST_STATS, K_LOGIN, 0, BY_NONE, 0},
    {TSDB_OP_WHOAMI, K_LOGIN, 0, BY_NONE, 0},
    {TSDB_OP_TOPOLOGY, K_LOGIN, 0, BY_NONE, 0},
    {TSDB_OP_STATUS, K_SUPER, 0, BY_NONE, 0},
    {TSDB_OP_SERIES, K_SERIES, N_ANY, BY_NAME, 0},
    {TSDB_OP_SERIES, K_SERIES, N_I, BY_NAME, 1},
    {TSDB_OP_SERIES_CREATE, K_SERIES, N_I, BY_NAME, 0},
    {TSDB_OP_SERIES_NAME, K_SERIES, N_ANY, BY_ID, 0},
    {TSDB_OP_SERIES_TYPE, K_SERIES, N_ANY, BY_ID, 0},
    {TSDB_OP_GET_RETENTION, K_SERIES, N_ANY, BY_ID, 0},
    {TSDB_OP_STATE, K_SERIES, N_I, BY_ID, 1},
    {TSDB_OP_STATE, K_SERIES, N_ANY, BY_ID, 0},
    {TSDB_OP_STATE_NAME, K_SERIES, N_ANY, BY_ID, 0},
    {TSDB_OP_INSERT, K_SERIES, N_I, BY_ID, 0},
    {TSDB_OP_INSERT_VALUES, K_SERIES, N_I, BY_ID, 0},
    {TSDB_OP_QUERY_RAW, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_VALUES, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_LTTB, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_BUCKETS, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_BOUNDS, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_TWA, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_MULTI, K_SERIES, N_R, BY_ID, 0},
    {TSDB_OP_QUERY_LATEST, K_SERIES, N_R, BY_ID, 0}, /* the record's status: the frame's own is 0 */
    {TSDB_OP_DELETE_RANGE, K_SERIES, N_D, BY_ID, 0},
    {TSDB_OP_SET_RETENTION, K_SERIES, N_D, BY_ID, 0},
    {TSDB_OP_REPLACE_RANGE, K_SERIES, N_I | N_D, BY_ID, 0},
    {TSDB_OP_COMMIT, K_COMMIT, 0, BY_NONE, 0},
    {TSDB_OP_FLUSH, K_SUPER, 0, BY_NONE, 0},
    {TSDB_OP_OPTIMIZE, K_SUPER, 0, BY_NONE, 0},
    {TSDB_OP_STATS, K_STATS, 0, BY_NONE, 0},
    {TSDB_OP_USER_CREATE, K_ADMIN, 0, BY_NONE, 0},
    {TSDB_OP_USER_DROP, K_ADMIN, 0, BY_NONE, 0},
    {TSDB_OP_USER_ALTER, K_ADMIN, 0, BY_NONE, 0},
    {TSDB_OP_USER_PASSWORD, K_ADMIN, 0, BY_NONE, 0},
    {TSDB_OP_GRANT, K_ADMIN, 0, BY_NONE, 0},
    {TSDB_OP_REVOKE, K_ADMIN, 0, BY_NONE, 0},
    {TSDB_OP_USER_LIST, K_ADMIN, 0, BY_NONE, 0},
};
#define NSPECS (sizeof specs / sizeof *specs)

/* The users and what they hold on their own series and on globex's. */
typedef struct tuser {
    const char *name, *pw;
    uint8_t own, foreign; /* N_R | N_I | N_D on the own series (acme/x, or acme/s1/x1 for the glob user) */
    bool super, stats, commit, glob;
    int fd;
} tuser;

static tuser users_t[] = {
    {"root", "rootpw", 7, 7, true, true, true, false, -1},
    {"acme/admin", "a", 7, 0, false, false, true, false, -1},
    {"acme/r", "r", N_R, 0, false, false, false, false, -1},
    {"acme/i", "i", N_I, 0, false, false, true, false, -1},
    {"acme/d", "d", N_D, 0, false, false, true, false, -1},
    {"acme/none", "n", 0, 0, false, false, false, false, -1},
    {"reader", "e", N_R, N_R, false, true, false, false, -1},
    {"acme/glob", "g", 7, 0, false, false, true, true, -1},
};
#define NUSERS (sizeof users_t / sizeof *users_t)

static tsdb_sid ax, ay, gx, as1, gs1; /* acme/x, acme/y, globex/x, acme/s1/x1, globex/s1/x1 */

static void put16(uint8_t *b, uint16_t v) { memcpy(b, &v, 2); }
static void put32(uint8_t *b, uint32_t v) { memcpy(b, &v, 4); }
static void put64(uint8_t *b, uint64_t v) { memcpy(b, &v, 8); }

/* A minimal well-formed payload of op naming the series (id, name). */
static size_t build(const spec *s, tsdb_sid id, const char *name, uint8_t *b)
{
    size_t nl = strlen(name);
    uint64_t t0 = (uint64_t)T0, t1 = (uint64_t)T0 + 1000;
    float one = 1;
    memset(b, 0, 512);
    switch (s->op) {
    case TSDB_OP_HELLO: put32(b, TSDB_PROTOCOL_VERSION); return 4;
    case TSDB_OP_AUTH_START: b[0] = 13; memcpy(b + 1, TSDB_SCRAM_SHA256, 13); put16(b + 14, 15);
        memcpy(b + 16, "n,,n=root,r=abc", 15); return 31;
    case TSDB_OP_AUTH_CONTINUE: put16(b, 6); memcpy(b + 2, "c=biws", 6); return 8;
    case TSDB_OP_CANCEL: put32(b, 12345); return 4;
    case TSDB_OP_SERIES: b[0] = s->variant; put16(b + 1, (uint16_t)nl); memcpy(b + 3, name, nl); return 3 + nl;
    case TSDB_OP_SERIES_CREATE: b[0] = TSDB_FLOAT32; put16(b + 1, (uint16_t)nl); memcpy(b + 3, name, nl);
        return 5 + nl;
    case TSDB_OP_SERIES_NAME: case TSDB_OP_SERIES_TYPE: case TSDB_OP_GET_RETENTION: put64(b, id); return 8;
    case TSDB_OP_STATE: put64(b, id); b[8] = s->variant; put16(b + 9, 2); memcpy(b + 11, "on", 2); return 13;
    case TSDB_OP_STATE_NAME: put64(b, id); put32(b + 8, 1); return 12; /* "on", the first code STATE made */
    case TSDB_OP_INSERT: case TSDB_OP_INSERT_VALUES: /* both: u32 n, (u32 0 | u8 type, u8 flags, u16 0), u64 id */
        put32(b, 1); put64(b + 8, id); put64(b + 16, t0); memcpy(b + 24, &one, 4); return 28;
    case TSDB_OP_QUERY_RAW: case TSDB_OP_QUERY_VALUES: case TSDB_OP_DELETE_RANGE:
        put64(b, id); put64(b + 8, t0); put64(b + 16, t1); return 24;
    case TSDB_OP_QUERY_LTTB: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); put64(b + 24, 3); return 32;
    case TSDB_OP_QUERY_BUCKETS: /* interval 0: one bucket; unit 0, count 0, order 0, limit 0, no zone */
        put64(b, id); put64(b + 8, t0); put64(b + 16, t1); return 48;
    case TSDB_OP_QUERY_BOUNDS: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); put32(b + 33, 1);
        put64(b + 37, t0); return 45;
    case TSDB_OP_QUERY_TWA: put64(b, id); put64(b + 8, t0); put64(b + 16, t1); return TSDB_TWA_HEADER;
    case TSDB_OP_SET_RETENTION: put64(b, id); return 16; /* limit 0: keep everything */
    case TSDB_OP_REPLACE_RANGE: put32(b, 1); put64(b + 8, id); put64(b + 16, t0); put64(b + 24, t1);
        put64(b + 32, t0); memcpy(b + 40, &one, 4); return 44;
    case TSDB_OP_QUERY_MULTI: /* QUERY_VALUES over one series: header, the id, the template */
        b[0] = TSDB_OP_QUERY_VALUES; put32(b + 4, 1); put32(b + 16, 24); put64(b + 24, id);
        put64(b + 32, TSDB_MANY_SERIES); put64(b + 40, t0); put64(b + 48, t1); return 56;
    case TSDB_OP_QUERY_LATEST: /* one id, no bound: u8 flags, u8 0, u16 threads, u32 n, i64 end, the id */
        put32(b + 4, 1); put64(b + 8, (uint64_t)INT64_MAX); put64(b + 16, id); return 24;
    default: return 0; /* no payload: LAST_STATS, WHOAMI, TOPOLOGY, STATUS, COMMIT, FLUSH, OPTIMIZE, STATS, USER_LIST */
    }
}

/* The admin ops aimed at root (or, for USER_CREATE, a new superuser): only a superuser may. */
static size_t build_admin(uint8_t op, uint8_t *b)
{
    const char *n = op == TSDB_OP_USER_CREATE ? "acme/su" : "root";
    size_t nl = strlen(n), o = 0;
    memset(b, 0, 512);
    put16(b, (uint16_t)nl);
    memcpy(b + 2, n, nl);
    o = 2 + nl;
    if (op == TSDB_OP_USER_CREATE || op == TSDB_OP_USER_ALTER)
        b[o++] = op == TSDB_OP_USER_CREATE ? TSDB_USER_SUPERUSER : 0;
    if (op == TSDB_OP_USER_CREATE || op == TSDB_OP_USER_PASSWORD) { /* a valid verifier */
        put32(b + o, ITER_MIN);
        b[o + 4] = 16;
        tsdb_random(b + o + 5, 16);
        tsdb_scram_keys("pw", 2, b + o + 5, 16, ITER_MIN, NULL, b + o + 21, b + o + 53);
        o += 85;
    }
    if (op == TSDB_OP_GRANT || op == TSDB_OP_REVOKE) {
        b[o++] = TSDB_SEL_PREFIX;
        b[o++] = TSDB_RIGHT_READ;
        put16(b + o, 6);
        memcpy(b + o + 2, "acme/x", 6);
        o += 8;
    }
    return op == TSDB_OP_USER_LIST ? 0 : o;
}

#define NOREPLY (INT32_MIN + 1)
#define CLOSED INT32_MIN

/* Sends a request and a WHOAMI behind it: the request's first reply frame's status, NOREPLY when the WHOAMI's
 * answer came first (CANCEL), CLOSED at the end of the stream. Requests of a connection run in order. */
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
                memcpy(&first, rest + TSDB_LATEST_PREFIX, 4); /* the record's status: the frame's is the request's */
        }
    }
}

static const char *opname(uint8_t op)
{
#define NAME_(n, num, m) case num: return #n;
    switch (op) { TSDB_OPS(NAME_) default: return "?"; }
#undef NAME_
}

static bool enough(uint8_t have, uint8_t need) { return need == N_ANY ? have != 0 : (have & need) == need; }

/* ------------------------------------------------------------ the tests */

/* Each op of TSDB_OPS has a row here and one in auth.c; no other op number has one there. */
static void tables(void)
{
    bool listed[256] = {false};
#define LIST_(n, num, m) listed[num] = true;
    TSDB_OPS(LIST_)
#undef LIST_
    for (int op = 0; op < 256; op++) {
        bool here = false;
        for (size_t i = 0; i < NSPECS; i++)
            here |= specs[i].op == op;
        if (listed[op] && !here) {
            fprintf(stderr, "  op %s (%d) has no row in this test's table: add it from design-auth.md §5\n",
                    opname((uint8_t)op), op);
            failures++;
        }
        if (listed[op] && auth_ops[op].who == P_NONE) {
            fprintf(stderr, "  op %s (%d) has no row in server/auth.c's table: tsdbd refuses it\n",
                    opname((uint8_t)op), op);
            failures++;
        }
        CHECK(listed[op] || auth_ops[op].who == P_NONE);
        CHECK(listed[op] || !here);
    }
    printf("  tables: every op of TSDB_OPS has a row here and in server/auth.c; no other op has one\n");
}

static void unknown_ops(int fd)
{
    bool listed[256] = {false};
#define LIST_(n, num, m) listed[num] = true;
    TSDB_OPS(LIST_)
#undef LIST_
    int n = 0;
    for (int op = 0; op < 256; op++) {
        if (listed[op])
            continue;
        int32_t st = ask(fd, (uint8_t)op, NULL, 0);
        if (st != TSDB_ERR_DENIED)
            fprintf(stderr, "  unknown op %d from a superuser: %d, want DENIED\n", op, st);
        CHECK(st == TSDB_ERR_DENIED);
        n++;
    }
    printf("  %d unknown op numbers: DENIED to a superuser\n", n);
}

/* The superuser's answers on each own series: what every user with enough rights must get. */
static int32_t ref[2][NSPECS];

static void reference(void)
{
    tuser *root = &users_t[0];
    const tsdb_sid own[2] = {ax, as1};
    const char *names[2] = {"acme/x", "acme/s1/x1"};
    uint8_t b[512];
    for (int pass = 0; pass < 2; pass++) /* the second pass: after the first made what it creates (a state name) */
        for (int k = 0; k < 2; k++)
            for (size_t i = 0; i < NSPECS; i++)
                if (specs[i].kind == K_SERIES)
                    ref[k][i] = ask(root->fd, specs[i].op, b, build(&specs[i], own[k], names[k], b));
    for (int k = 0; k < 2; k++)
        for (size_t i = 0; i < NSPECS; i++)
            if (specs[i].kind == K_SERIES && ref[k][i] != 0) {
                fprintf(stderr, "  %s (create %u) on %s from root: %d, want 0 (a malformed test frame?)\n",
                        opname(specs[i].op), specs[i].variant, names[k], ref[k][i]);
                failures++;
            }
}

static void every_op(void)
{
    uint8_t b[512];
    size_t checked = 0;
    for (size_t u = 0; u < NUSERS; u++) {
        tuser *t = &users_t[u];
        int k = t->glob;
        tsdb_sid own = k ? as1 : ax, foreign = k ? gs1 : gx;
        const char *own_name = k ? "acme/s1/x1" : "acme/x", *foreign_name = k ? "globex/s1/x1" : "globex/x";
        for (size_t i = 0; i < NSPECS; i++) {
            const spec *s = &specs[i];
            int32_t want, got;
            switch (s->kind) {
            case K_ANY: want = 0; break;
            case K_PRE: want = TSDB_ERR_INVALID; break; /* logged in already */
            case K_LOGIN: want = s->op == TSDB_OP_CANCEL ? NOREPLY : 0; break;
            case K_SUPER: want = t->super ? 0 : TSDB_ERR_DENIED; break;
            case K_STATS: want = t->stats ? 0 : TSDB_ERR_DENIED; break;
            case K_COMMIT: want = t->commit ? 0 : TSDB_ERR_DENIED; break;
            case K_ADMIN: /* aimed at root: only a superuser could; listing answers everyone */
                if (t->super && s->op != TSDB_OP_USER_LIST)
                    continue;
                want = s->op == TSDB_OP_USER_LIST ? 0 : TSDB_ERR_DENIED;
                got = ask(t->fd, s->op, b, build_admin(s->op, b));
                goto compare;
            default: /* K_SERIES */
                want = enough(t->own, s->need) ? ref[k][i] : TSDB_ERR_DENIED;
                got = ask(t->fd, s->op, b, build(s, own, own_name, b));
                if (got != want) {
                    fprintf(stderr, "  %s %s (create %u) on its own %s: %d, want %d\n", t->name, opname(s->op),
                            s->variant, own_name, got, want);
                    failures++;
                }
                checked++;
                want = enough(t->foreign, s->need) ? 0 : TSDB_ERR_DENIED;
                got = ask(t->fd, s->op, b, build(s, foreign, foreign_name, b));
                if (want == 0 ? got == TSDB_ERR_DENIED : got != want) {
                    fprintf(stderr, "  %s %s (create %u) on %s: %d, want %s\n", t->name, opname(s->op), s->variant,
                            foreign_name, got, want == 0 ? "not DENIED" : "DENIED");
                    failures++;
                }
                checked++;
                continue;
            }
            got = ask(t->fd, s->op, b, build(s, own, own_name, b));
        compare:
            if (got != want) {
                fprintf(stderr, "  %s %s: %d, want %d\n", t->name, opname(s->op), got, want);
                failures++;
            }
            checked++;
        }
    }
    printf("  every op from %zu users, own and foreign series: %zu answers as §5 says\n", NUSERS, checked);
}

/* Denied is missing (A16): a series that does not exist answers as one the user may not see, unless the user has
 * the right where it would be. */
static void missing(void)
{
    uint8_t b[512];
    const spec q = {TSDB_OP_QUERY_RAW, K_SERIES, N_R, BY_ID, 0}, look = {TSDB_OP_SERIES, K_SERIES, N_ANY, BY_NAME, 0};
    int fd = users_t[1].fd; /* acme/admin */
    tsdb_sid unknown = TSDB_SID(0, 999999);
    CHECK(ask(fd, q.op, b, build(&q, unknown, "", b)) == TSDB_ERR_DENIED);
    const spec lat = {TSDB_OP_QUERY_LATEST, K_SERIES, N_R, BY_ID, 0};
    CHECK(ask(fd, lat.op, b, build(&lat, unknown, "", b)) == TSDB_ERR_DENIED); /* in its record */
    CHECK(ask(users_t[0].fd, lat.op, b, build(&lat, unknown, "", b)) == TSDB_ERR_NOTFOUND);
    CHECK(ask(fd, look.op, b, build(&look, 0, "globex/nothere", b)) == TSDB_ERR_DENIED);
    CHECK(ask(fd, look.op, b, build(&look, 0, "acme/nothere", b)) == TSDB_ERR_NOTFOUND);
    CHECK(ask(users_t[0].fd, look.op, b, build(&look, 0, "globex/nothere", b)) == TSDB_ERR_NOTFOUND);
    printf("  a missing series: DENIED outside the user's grants, NOTFOUND inside\n");
}

/* QUERY_MULTI answers each series with its own status (A17). */
static void multi_mixed(void)
{
    const tsdb_sid ids[4] = {ax, gx, ay, TSDB_SID(0, 999999)};
    uint8_t b[24 + 32 + 24] = {0};
    b[0] = TSDB_OP_QUERY_VALUES;
    put32(b + 4, 4);
    put32(b + 16, 24);
    memcpy(b + 24, ids, 32);
    put64(b + 56, TSDB_MANY_SERIES);
    put64(b + 64, (uint64_t)T0);
    put64(b + 72, (uint64_t)T0 + 1000);
    const int32_t want_admin[4] = {0, TSDB_ERR_DENIED, 0, TSDB_ERR_DENIED}, want_root[4] = {0, 0, 0, 0};
    for (int who = 0; who < 2; who++) {
        int fd = users_t[who].fd;
        const int32_t *want = who ? want_admin : want_root;
        raw_send(fd, 7, TSDB_OP_QUERY_MULTI, b, sizeof b);
        int32_t st[4] = {1, 1, 1, 1}, end = 1;
        for (;;) {
            uint32_t id, idx;
            uint8_t rest[64];
            size_t n;
            int32_t s = raw_frame(fd, &id, rest, sizeof rest, &n, NULL, NULL);
            if (s == INT32_MIN || n < 4) {
                CHECK(!"a QUERY_MULTI reply");
                break;
            }
            memcpy(&idx, rest, 4);
            if (idx == TSDB_MULTI_END) {
                end = s;
                break;
            }
            if (idx < 4 && st[idx] == 1)
                st[idx] = s;
        }
        CHECK(end == 0);
        for (int i = 0; i < 4; i++) {
            if (st[i] != want[i])
                fprintf(stderr, "  QUERY_MULTI from %s, series %d: %d, want %d\n", users_t[who].name, i, st[i],
                        want[i]);
            CHECK(st[i] == want[i]);
        }
    }
    printf("  QUERY_MULTI over own, foreign, own and unknown ids: each its own status\n");
}

/* One foreign id among thousands refuses the whole insert (inserts are all or nothing), packed and plain. */
static void insert_one_foreign(void)
{
    tsdbc *c = login_as("acme/admin", "a");
    enum { N = 5000 };
    static tsdb_sid ids[N];
    static int64_t ts[N];
    static float v[N];
    for (int level = 1; level >= 0; level--) { /* the default (packed, zstd), then plain arrays */
        OK(tsdbc_set_compression(c, level));
        int64_t base = T0 + 100000 + (int64_t)level * 10 * N;
        for (int i = 0; i < N; i++) {
            ids[i] = i % 2 ? ax : ay;
            ts[i] = base + i;
            v[i] = (float)i;
        }
        ids[N / 2 + 1] = gx;
        CHECK(tsdbc_insert(c, ids, ts, v, N) == TSDB_ERR_DENIED);
        size_t rows = 0;
        OK(tsdbc_query_raw(c, ax, base, base + N, count_fn, &rows));
        OK(tsdbc_query_raw(c, ay, base, base + N, count_fn, &rows));
        CHECK(rows == 0);
        ids[N / 2 + 1] = ax; /* the same frame without it goes in */
        OK(tsdbc_insert(c, ids, ts, v, N));
        OK(tsdbc_query_raw(c, ax, base, base + N, count_fn, &rows));
        OK(tsdbc_query_raw(c, ay, base, base + N, count_fn, &rows));
        CHECK(rows == N);
    }
    tsdbc_close(c);
    printf("  inserts of %d rows with one foreign id (packed and plain): refused whole\n", N);
}

/* §2's escalation cases, over the wire. */
static void escalation(void)
{
    tsdbc *root = login_as("root", "rootpw"), *adm = login_as("acme/admin", "a");
    OK(tsdbc_user_create(adm, "acme/new", "pw", 0, 0));
    CHECK(tsdbc_user_create(adm, "globex/new", "pw", 0, 0) == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_create(adm, "acme/su", "pw", 0, TSDB_USER_SUPERUSER) == TSDB_ERR_DENIED);
    OK(tsdbc_grant(adm, "acme/new", TSDB_SEL_PREFIX, "acme/x", TSDB_RIGHT_READ));
    CHECK(tsdbc_grant(adm, "acme/new", TSDB_SEL_PREFIX, "globex/", TSDB_RIGHT_READ) == TSDB_ERR_DENIED);
    CHECK(tsdbc_grant(adm, "acme/new", TSDB_SEL_ALL, "", TSDB_RIGHT_READ) == TSDB_ERR_DENIED);
    CHECK(tsdbc_grant(adm, "acme/new", TSDB_SEL_GLOB, "*", TSDB_RIGHT_READ) == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_alter(adm, "acme/new", TSDB_USER_SUPERUSER) == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_drop(adm, "root") == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_password(adm, "root", "mine", 0) == TSDB_ERR_DENIED);
    /* a cross-tenant share moves the user out of acme's admins' reach */
    OK(tsdbc_grant(root, "acme/new", TSDB_SEL_NAME, "globex/x", TSDB_RIGHT_READ));
    CHECK(tsdbc_user_password(adm, "acme/new", "mine", 0) == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_drop(adm, "acme/new") == TSDB_ERR_DENIED);
    CHECK(tsdbc_revoke(adm, "acme/new", TSDB_SEL_NAME, "globex/x", TSDB_RIGHT_READ) == TSDB_ERR_DENIED);
    OK(tsdbc_revoke(adm, "acme/new", TSDB_SEL_PREFIX, "acme/x", TSDB_RIGHT_READ)); /* its own share it may cut */
    tsdbc *nw = login_as("acme/new", "pw");
    CHECK(query(nw, gx) == 0 && query(nw, ax) == TSDB_ERR_DENIED);
    OK(tsdbc_user_password(nw, "acme/new", "pw2", 0)); /* anyone may set their own */
    tsdbc_close(nw);
    nw = login_as("acme/new", "pw2");
    tsdbc_close(nw);
    /* the last superuser cannot be dropped or demoted online */
    CHECK(tsdbc_user_drop(root, "root") == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_alter(root, "root", 0) == TSDB_ERR_DENIED);
    /* a user with no grants: WHOAMI, its own password, nothing else */
    tsdbc *none = login_as("acme/none", "n");
    OK(tsdbc_user_password(none, "acme/none", "n2", 0));
    CHECK(tsdbc_user_create(none, "acme/none2", "pw", 0, 0) == TSDB_ERR_DENIED);
    tsdbc_user *list = NULL;
    size_t n = 0;
    OK(tsdbc_user_list(none, &list, &n));
    CHECK(n == 1 && strcmp(list[0].name, "acme/none") == 0);
    tsdbc_users_free(list, n);
    tsdbc_close(none);
    OK(tsdbc_user_list(adm, &list, &n)); /* itself and whom it manages: never root, reader or the shared user */
    bool saw_root = false, saw_new = false, saw_r = false;
    for (size_t i = 0; i < n; i++) {
        saw_root |= strcmp(list[i].name, "root") == 0 || strcmp(list[i].name, "reader") == 0;
        saw_new |= strcmp(list[i].name, "acme/new") == 0;
        saw_r |= strcmp(list[i].name, "acme/r") == 0;
    }
    CHECK(!saw_root && !saw_new && saw_r);
    tsdbc_users_free(list, n);
    tsdbc_close(adm);
    tsdbc_close(root);
    printf("  escalation: outside grants, superusers, shared users, the last superuser, own password\n");
}

/* The rights map with globs: a miss only a glob answers, the hit after it, and the glob's removal. */
static void glob_map(void)
{
    tsdbc *root = login_as("root", "rootpw"), *g = login_as("acme/glob", "g");
    CHECK(query(g, as1) == 0);
    CHECK(query(g, as1) == 0);
    CHECK(query(g, ax) == TSDB_ERR_DENIED); /* acme/x is not acme/?*\/x* */
    OK(tsdbc_revoke(root, "acme/glob", TSDB_SEL_GLOB, "acme/?*/x*", TSDB_RIGHT_READ));
    CHECK(query(g, as1) == TSDB_ERR_DENIED);
    OK(tsdbc_grant(root, "acme/glob", TSDB_SEL_GLOB, "acme/?*/x*", TSDB_RIGHT_READ));
    CHECK(query(g, as1) == 0);
    tsdbc_close(g);
    tsdbc_close(root);
    printf("  globs in the rights map: a miss, the hit, a revoke and a grant seen at once\n");
}

static void make_users(const char *dir)
{
    users us;
    OK(users_open(&us, dir, true));
    for (size_t i = 0; i < NUSERS; i++) {
        user_verifier v = {.iterations = ITER_MIN, .salt_len = 16};
        tsdb_random(v.salt, 16);
        tsdb_scram_keys(users_t[i].pw, strlen(users_t[i].pw), v.salt, 16, ITER_MIN, NULL, v.stored_key, v.server_key);
        uint32_t uid;
        OK(users_create(&us, 0, users_t[i].name, users_t[i].super ? USER_SUPERUSER : 0, &v, &uid));
    }
    OK(users_grant(&us, 0, "acme/admin", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_grant(&us, 0, "acme/r", SEL_NAME, "acme/x", 6, R_READ));
    OK(users_grant(&us, 0, "acme/i", SEL_NAME, "acme/x", 6, R_INSERT));
    OK(users_grant(&us, 0, "acme/d", SEL_NAME, "acme/x", 6, R_DELETE));
    OK(users_grant(&us, 0, "reader", SEL_ALL, "", 0, R_READ));
    OK(users_grant(&us, 0, "acme/glob", SEL_GLOB, "acme/?*/x*", 10, R_READ | R_INSERT | R_DELETE));
    users_close(&us);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    printf("test_auth_ops (%s)\n", getenv("TSDBD_IO") ? getenv("TSDBD_IO") : "default io");
    char dir[PATH_MAX + 16];
    snprintf(dir, sizeof dir, "%s/tsdbd-authops-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    ah_setup(dir);
    tables();
    mkdir(dir, 0755);
    make_users(dir);
    pid_t srv = start(dir, NULL);
    tsdbc *root = login_as("root", "rootpw");
    OK(tsdbc_series(root, "acme/x", 1, &ax));
    OK(tsdbc_series(root, "acme/y", 1, &ay));
    OK(tsdbc_series(root, "globex/x", 1, &gx));
    OK(tsdbc_series(root, "acme/s1/x1", 1, &as1));
    OK(tsdbc_series(root, "globex/s1/x1", 1, &gs1));
    tsdbc_close(root);
    for (size_t i = 0; i < NUSERS; i++) {
        users_t[i].fd = raw_connect();
        if (raw_login(users_t[i].fd, users_t[i].name, users_t[i].pw) != 0) {
            fprintf(stderr, "test_auth_ops: %s cannot log in (server log %s)\n", users_t[i].name, srvlog);
            return 1;
        }
    }
    unknown_ops(users_t[0].fd);
    reference();
    every_op();
    missing();
    multi_mixed();
    insert_one_foreign();
    escalation();
    glob_map();
    for (size_t i = 0; i < NUSERS; i++)
        close(users_t[i].fd);
    stop(srv);
    if (failures) {
        fprintf(stderr, "test_auth_ops: %d failures (server log %s)\n", failures, srvlog);
        return 1;
    }
    char cmd[6 * PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s' '%s'", dir, srvlog, clilog);
    if (system(cmd) != 0)
        fprintf(stderr, "test_auth_ops: could not remove %s\n", dir);
    printf("test_auth_ops: ok\n");
    return 0;
}
