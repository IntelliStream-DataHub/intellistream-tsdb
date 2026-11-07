/*
 * fuzz_frames: feeds arbitrary bytes to tsdbd's request path (parse() splits them into frames, handle() decodes
 * and runs each request against a real database) without sockets or threads, and checks the replies.
 *
 * It includes server/tsdbd.c itself, so the code under test is the server's own, static functions included;
 * the only differences are OUT_WAIT (where a worker would wait for a send to drain its output, the replies so far
 * are checked and dropped here, and after every request) and a guard on volume paths (the fuzzer must not create
 * directories outside its scratch directory). OUT_CAP is smaller than the server's, so replies go out in pieces
 * more often; what a connection buffers is then bounded as in the server (an allocation over the 300 MB that
 * `make fuzz-frames` allows is a finding).
 *
 *   fuzz_frames seeds DIR                       write a valid seed frame file per request form
 *   fuzz_frames replay [--dir D] FILE|DIR...    run each input once (make test: the committed corpus)
 *   fuzz_frames fuzz [--dir D] [--seconds N] [--out DIR] [--seed S] CORPUS...
 *       mutate the corpus for N seconds; inputs that reach new code go to --out, a failing one to
 *       --out/crash-* (the run then stops with status 1)
 *
 * Built with ASan and UBSan (make fuzz-frames). There is no clang here, so instead of libFuzzer it has its own
 * driver: GCC's -fsanitize-coverage=trace-pc calls __sanitizer_cov_trace_pc on every basic block of this file
 * (that is, of tsdbd.c), whose edges, counted in AFL's buckets, decide which inputs are kept. Mutations know the
 * frame layout: most keep each frame's length right, so they reach the handlers instead of the framing check.
 *
 * What counts as a failure: a sanitizer report, a crash, an input that runs longer than 20 s, and a reply
 * stream that is not well formed (each request answered by REPLY frames carrying its id, at least one).
 *
 * Login is on (server/auth.c with a USERS of its own): an input logs in as root, as a user with grants, or not at
 * all, by a 4-byte mode in front of its frames (run_one); the pre-login limits and the SCRAM parser get the frames
 * of a client that has not logged in, the checks get those of one that has.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>

/* Volume paths come from the client: keep them inside the scratch directory (the server's own checks of
 * them belong to the volume code and its tests). */
static int fz_path_ok(const char *path) { return !path || (path[0] != '/' && !strstr(path, "..")); }
static int fz_series_at(tsdb *d, const char *name, const char *path, tsdb_sid *id)
{
    return fz_path_ok(path) ? tsdb_series_at(d, name, path, id) : TSDB_ERR_INVALID;
}
static int fz_series_create(tsdb *d, const char *name, tsdb_type t, const char *path, tsdb_sid *id)
{
    return fz_path_ok(path) ? tsdb_series_create(d, name, t, path, id) : TSDB_ERR_INVALID;
}
#define tsdb_series_at fz_series_at
#define tsdb_series_create fz_series_create
struct conn;
static void fz_out_wait(struct conn *c);
#define OUT_WAIT fz_out_wait
#define OUT_CAP (256u << 10)
#define main tsdbd_main
#include "../../server/tsdbd.c"
#undef main
#undef tsdb_series_at
#undef tsdb_series_create

#include "../../client/tsdb_pack.h"
#include "../../client/tsdb_crypto.h"

/* ------------------------------------------------------------ coverage */

#define MAP_SIZE (1u << 16)
/* GCC and newer clang have the attribute; Apple's clang does not (and there the coverage build is not used) */
#if defined(__has_attribute)
#if __has_attribute(no_sanitize_coverage)
#define NO_COV __attribute__((no_sanitize_coverage))
#endif
#endif
#ifndef NO_COV
#define NO_COV
#endif

static uint8_t cov_map[MAP_SIZE], virgin[MAP_SIZE];
static __thread uintptr_t cov_prev;
static int cov_on;

void __sanitizer_cov_trace_pc(void);
NO_COV void __sanitizer_cov_trace_pc(void)
{
    if (!cov_on)
        return;
    uintptr_t pc = (uintptr_t)__builtin_return_address(0);
    pc = (pc >> 4) ^ (pc << 8);
    cov_map[(pc ^ cov_prev) & (MAP_SIZE - 1)]++;
    cov_prev = pc >> 1;
}

static uint8_t bucket(uint8_t n)
{
    return n == 0 ? 0 : n == 1 ? 1 : n == 2 ? 2 : n == 3 ? 4 : n < 8 ? 8 : n < 16 ? 16 : n < 32 ? 32 : n < 128 ? 64 : 128;
}

/* Whether the last run reached an edge (or a hit-count bucket of one) no earlier run did. */
NO_COV static int cov_new(void)
{
    int fresh = 0;
    for (size_t i = 0; i < MAP_SIZE; i++) {
        if (!cov_map[i])
            continue;
        uint8_t b = bucket(cov_map[i]);
        if (b & ~virgin[i]) {
            virgin[i] |= b;
            fresh = 1;
        }
    }
    return fresh;
}

NO_COV static size_t cov_edges(void)
{
    size_t n = 0;
    for (size_t i = 0; i < MAP_SIZE; i++)
        n += virgin[i] != 0;
    return n;
}

/* ----------------------------------------------------------- the server */

static char db_dir[4096];
static const char *crash_dir;
static int verbose; /* FUZZ_VERBOSE: each request's status (to check the seeds do what they should) */
static const uint8_t *cur_in;
static size_t cur_len;

/* Login is required, as in tsdbd without --no-auth. Before its frames run, an input logs in through the same
 * parse() and handle() path: as "root" (a superuser, so that every handler is reached; the committed corpus from
 * before login runs this way), as "fz" (grants on some series and an admin prefix, so that the rights map, the
 * per-id checks and the admin rules decide) when it starts with mode_bytes[M_USER], and not at all with
 * mode_bytes[M_PRE] (its frames are what a client sends before login). A mode is 4 bytes no other input can start
 * with (a frame length over TSDB_MAX_FRAME, which closes the connection at once). Both nonces and the salts are
 * fixed, so each login is the same three frames, made once. */
#define MODE_LEN 4
enum { M_ROOT, M_USER, M_PRE };
static const uint8_t mode_bytes[3][MODE_LEN] = {{0}, {0xff, 0xff, 0xff, 'U'}, {0xff, 0xff, 0xff, 'P'}};
static uint8_t *login_frames[2]; /* M_ROOT, M_USER */
static size_t login_len[2];

static int input_mode(const uint8_t *p, size_t len)
{
    for (int m = M_USER; m <= M_PRE; m++)
        if (len >= MODE_LEN && memcmp(p, mode_bytes[m], MODE_LEN) == 0)
            return m;
    return M_ROOT;
}

/* USERS lives in its own directory (auth_init's), and an input that may have changed it (an admin op) gets the
 * file as it was made back, so that every input meets the same users, in a replay as in a run. */
static char users_dir[4200];
static uint8_t *users_file;
static size_t users_len;
static bool users_dirty;

static void users_reset(void)
{
    char p[4300], t[4300];
    snprintf(p, sizeof p, "%s/USERS", users_dir);
    snprintf(t, sizeof t, "%s/USERS.fz", users_dir);
    auth_shutdown();
    int fd = open(t, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0 || write(fd, users_file, users_len) != (ssize_t)users_len || close(fd) != 0 || rename(t, p) != 0 ||
        auth_init(users_dir, true, true, db) != 0) {
        fprintf(stderr, "fuzz_frames: cannot put USERS back in %s\n", users_dir);
        exit(2);
    }
    users_dirty = false;
}

static void fail_dump(void)
{
    if (!cur_in || !crash_dir)
        return;
    char p[4200];
    snprintf(p, sizeof p, "%s/crash-%ld-%d", crash_dir, (long)time(NULL), getpid());
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd >= 0) {
        if (write(fd, cur_in, cur_len) != (ssize_t)cur_len)
            perror("fuzz_frames: write crash input");
        close(fd);
        fprintf(stderr, "fuzz_frames: failing input saved as %s\n", p);
    }
}

static void on_fatal(int sig)
{
    fail_dump();
    signal(sig, SIG_DFL);
    if (sig == SIGALRM)
        fprintf(stderr, "fuzz_frames: an input ran longer than 20 s\n");
    raise(sig == SIGALRM ? SIGABRT : sig);
}

/* the sanitizers' runtime has it; a plain build (make test replays the corpus) does not */
extern void __sanitizer_set_death_callback(void (*cb)(void)) __attribute__((weak));

static void rm_rf(const char *path)
{
    char cmd[8400];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", path);
    if (system(cmd) != 0)
        fprintf(stderr, "fuzz_frames: could not remove %s\n", path);
}

/* A fresh database with a series of every type (TSDB_SID(0, 0 .. 4)) and a state name, so inputs find something. */
static void db_start(void)
{
    char d[4200];
    snprintf(d, sizeof d, "%s/db", db_dir);
    rm_rf(d);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.query_threads = 1;
    o.wal_sync_ms = -1;
    o.partition_ms = 3600000;
    o.granule_rows = 64;
    if (tsdb_open(d, &o, &db) != 0) {
        fprintf(stderr, "fuzz_frames: cannot open %s\n", d);
        exit(2);
    }
    static const char *names[] = {"f32", "f64", "i64", "bool", "state"};
    tsdb_sid id[5] = {0};
    for (unsigned t = 0; t < 5; t++)
        tsdb_series_create(db, names[t], (tsdb_type)t, NULL, &id[t]);
    uint32_t code;
    tsdb_state(db, id[4], "on", &code);
    int64_t ts[200];
    float v[200];
    for (int i = 0; i < 200; i++)
        ts[i] = 1000 * i, v[i] = (float)i;
    tsdb_insert_series(db, id[0], ts, v, 200);
    tsdb_flush(db);
    tsdb_insert_series(db, id[0], ts, v, 50); /* some rows in a part, some in the memtable */
    users_reset(); /* auth.c's handle on the database is this one */
}

static void db_stop(void)
{
    tsdb_close(db);
    db = NULL;
}

/* Checks the replies to request `seq` in out[0, len): REPLY frames with its id, at least one. */
/* The reply stream of the request running (id fz_seq), checked as it is drained: REPLY frames carrying its id,
 * each of a valid length; fz_skip is what remains of the frame being drained. */
static uint32_t fz_seq;
static uint8_t fz_op;
static size_t fz_skip, fz_frames, fz_bytes;
static int32_t fz_status; /* of the first frame */

static void bad_replies(void)
{
    fprintf(stderr, "fuzz_frames: bad reply stream for op %u (%zu bytes, %zu frames)\n", fz_op, fz_bytes, fz_frames);
    fail_dump();
    abort();
}

/* Checks and drops the complete frames and frame pieces of c's output; what is left is the start of a frame
 * header. The server's out_put calls it where a worker would wait for the client to read. */
static void fz_out_wait(struct conn *c)
{
    const uint8_t *out = c->out.p;
    size_t off = 0, len = c->out.len;
    while (off < len) {
        if (fz_skip) {
            size_t take = len - off < fz_skip ? len - off : fz_skip;
            off += take;
            fz_skip -= take;
            continue;
        }
        if (len - off < 4 + 9)
            break;
        uint32_t flen, id;
        memcpy(&flen, out + off, 4);
        memcpy(&id, out + off + 4, 4);
        if (flen < 9 || flen > TSDB_MAX_FRAME + TSDB_MULTI_PREFIX || id != fz_seq || out[off + 8] != TSDB_OP_REPLY)
            bad_replies();
        if (!fz_frames)
            memcpy(&fz_status, out + off + 9, 4);
        fz_frames++;
        fz_skip = 4 + (size_t)flen;
    }
    fz_bytes += off;
    memmove(c->out.p, out + off, len - off);
    c->out.len = len - off;
}

/* After a request: its replies complete, at least one frame. */
static void check_replies(conn *c, uint32_t seq, uint8_t op)
{
    fz_out_wait(c);
    if (c->out.len || fz_skip || !fz_frames)
        bad_replies();
    if (verbose)
        fprintf(stderr, "  op %u: status %d, %zu reply bytes\n", op, fz_status, fz_bytes);
    fz_seq = seq + 1;
    fz_op = 0;
    fz_frames = fz_bytes = 0;
}

/* Delivers n bytes to c, as recv would, and runs what they complete. After each request it does what the event
 * loop does when a worker is done: a closing login or a dropped user ends reading (eof), an answered AUTH_CONTINUE
 * lets parse() go on, and what waited behind the two requests a connection may queue before login is read.
 * false: the connection closed (or parked for memory, which only a large frame's partial header can be here). */
static bool feed(conn *c, const uint8_t *p, size_t n, uint32_t *seq)
{
    if (buf_reserve(&c->in, n) != 0)
        abort();
    memcpy(c->in.p + c->in.len, p, n);
    c->in.len += n;
    bool ok = parse(c) == 0;
    while (c->q_head) {
        req *r = c->q_head;
        c->q_head = r->next;
        if (!c->q_head)
            c->q_tail = NULL;
        c->q_bytes -= r->len;
        atomic_store(&c->cancel_running, r->cancelled); /* as dispatch() does: a CANCEL that came first */
        uint8_t op = r->op;
        r->id = *seq;
        fz_seq = *seq;
        fz_op = op;
        handle(r);
        check_replies(c, *seq, op);
        (*seq)++;
        req_free(r);
        if (op >= TSDB_OP_USER_CREATE && op <= TSDB_OP_REVOKE)
            users_dirty = true; /* refused or not: the next input meets USERS as it was made */
        if (atomic_load(&c->auth_end))
            c->eof = true;
        if (c->auth_hold && op == TSDB_OP_AUTH_CONTINUE)
            c->auth_hold = false;
        if (ok && !c->eof && c->in.len)
            ok = parse(c) == 0;
    }
    return ok;
}

/* One input: what a client sends on one connection, after the login of its mode. Requests run in order, as a
 * worker would run them. */
static void run_one(const uint8_t *in, size_t len)
{
    cur_in = in;
    cur_len = len;
    int mode = input_mode(in, len);
    if (mode != M_ROOT)
        in += MODE_LEN, len -= MODE_LEN;
    conn *c = calloc(1, sizeof *c);
    if (!c)
        abort();
    c->fd = -1;
    atomic_store(&c->proto, TSDB_PROTOCOL_VERSION); /* HA bodies on, as for a client that said 13 */
    c->n_out = (note){NULL, c, false}; /* as on_accept: replies push n_out onto note_top, which nobody takes here */
    c->n_done = (note){NULL, c, true};
    pthread_mutex_init(&c->lock, NULL);
    conn_cv_init(&c->cv);
    auth_conn_init(&c->auth); /* as on_accept: login required */
    atomic_store(&c->authed, c->auth.state == AS_IN);
    uint32_t seq = 0;
    bool ok = true;
    if (mode != M_PRE && (!feed(c, login_frames[mode], login_len[mode], &seq) || !atomic_load(&c->authed) ||
                          c->eof)) {
        fprintf(stderr, "fuzz_frames: the fixed login as %s failed\n", mode == M_ROOT ? "root" : "fz");
        fail_dump();
        abort();
    }
    /* in pieces, as recv would deliver them: the first piece is short, to go through partial frames */
    size_t first = len > 7 ? 7 : len, at = 0;
    while (at < len && ok && !c->eof) { /* eof: a refused HELLO or login, after which tsdbd reads nothing more */
        size_t take = at == 0 ? first : len - at;
        ok = feed(c, in + at, take, &seq);
        at += take;
    }
    auth_conn_free(&c->auth);
    if (users_dirty)
        users_reset();
    free(c->in.p);
    c->in = (buf){0};
    in_account(c);
    free(c->out.p);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    atomic_store(&note_top, NULL); /* it may point into c */
    free(c);
    cur_in = NULL;
}

static void timed_run(const uint8_t *in, size_t len)
{
    alarm(20);
    run_one(in, len);
    alarm(0);
}

/* ---------------------------------------------------------------- seeds */

typedef struct fb {
    uint8_t *p;
    size_t len, cap;
} fb;

static void put(fb *b, const void *d, size_t n)
{
    if (b->len + n > b->cap) {
        b->cap = (b->len + n) * 2 + 64;
        b->p = realloc(b->p, b->cap);
        if (!b->p)
            abort();
    }
    memcpy(b->p + b->len, d, n);
    b->len += n;
}
static void p8(fb *b, uint8_t v) { put(b, &v, 1); }
static void p16(fb *b, uint16_t v) { put(b, &v, 2); }
static void p32(fb *b, uint32_t v) { put(b, &v, 4); }
static void p64(fb *b, int64_t v) { put(b, &v, 8); }
static void psid(fb *b, uint64_t v) { put(b, &v, 8); } /* a series id (u64 since protocol 11) */
static void pstr16(fb *b, const char *s)
{
    p16(b, (uint16_t)strlen(s));
    put(b, s, strlen(s));
}

/* Appends frame (op, payload) to out. */
static void frame(fb *out, uint8_t op, const fb *pl)
{
    p32(out, (uint32_t)(5 + pl->len));
    p32(out, 1);
    p8(out, op);
    if (pl->len)
        put(out, pl->p, pl->len);
}

static void seed_write(const char *dir, const char *name, const fb *b)
{
    char p[4200];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "wb");
    if (!f || fwrite(b->p, 1, b->len, f) != b->len || fclose(f) != 0) {
        fprintf(stderr, "fuzz_frames: cannot write %s\n", p);
        exit(2);
    }
}

/* INSERT_VALUES or REPLACE_RANGE arrays for n rows of `type` (bitmap when valid), plus series when many */
static void values_body(fb *b, unsigned type, uint32_t n, bool valid, bool many)
{
    unsigned w = tsdb_wire_width(type);
    for (uint32_t i = 0; i < n; i++)
        p64(b, 5000 + 1000 * (int64_t)i);
    for (uint32_t i = 0; i < n; i++) {
        uint8_t v[8] = {0};
        if (type == 0) {
            float f = 1.5f * (float)i;
            memcpy(v, &f, 4);
        } else if (type == 1) {
            double d = 2.5 * i;
            memcpy(v, &d, 8);
        } else if (type == 2) {
            int64_t x = (int64_t)i * 7 - 3;
            memcpy(v, &x, 8);
        } else if (type == 3) {
            v[0] = i & 1;
        } else {
            v[0] = 1; /* the state code of "on" */
        }
        put(b, v, w);
    }
    if (valid)
        for (uint32_t i = 0; i < (n + 7) / 8; i++)
            p8(b, 0xb5);
    if (many) { /* b holds the payload from its start: pad to 8, then a series of this type per row */
        while (b->len % 8)
            p8(b, 0);
        for (uint32_t i = 0; i < n; i++)
            psid(b, type);
    }
}

static void values_header(fb *b, uint32_t n, uint64_t s, unsigned type, uint8_t flags)
{
    p32(b, n);
    p8(b, (uint8_t)type);
    p8(b, flags);
    p16(b, 0);
    psid(b, s);
}

/* A QUERY_MULTI payload (version 11): header, the arrays its flags ask for, the series, the template t
 * (an inner op's payload with series TSDB_MANY_SERIES). */
static void multi_payload(fb *p, uint8_t op, uint8_t flags, uint16_t threads, uint32_t n, uint64_t max_rows,
                          const fb *t)
{
    p8(p, op);
    p8(p, flags);
    p16(p, threads);
    p32(p, n);
    p64(p, (int64_t)max_rows);
    p32(p, (uint32_t)t->len);
    p32(p, 0);
    for (int a = 0; a < 3; a++)
        if (flags & (1 << a))
            for (uint32_t i = 0; i < n; i++)
                p64(p, a == 0 ? (int64_t)i * 1000 : a == 1 ? 900000 - (int64_t)i * 1000 : 10000 * (int64_t)(i + 1));
    for (uint32_t i = 0; i < n; i++) /* 77 and 2^32 + 1: unknown (not series 1) */
        psid(p, i == 2 ? 77 : i == 4 ? 0x100000001ull : i % 5);
    put(p, t->p, t->len);
}

/* ---------------------------------------------------------------- login */

static const struct fz_user {
    const char *name, *pw;
    uint8_t salt_byte;
} fz_users[2] = {{"root", "rootpw", 0x11}, {"fz", "fzpw", 0x22}}; /* by mode: M_ROOT, M_USER */
#define FZ_CNONCE "fzclient"
#define FZ_SNONCE "fuzznonce" /* auth_test_nonce */

/* The client-final-message of user u's fixed exchange (bad: one bit of the proof flipped). The server-first-message
 * it answers is what auth.c sends for these nonces and the salt; the login in run_one checks that it is. */
static void fz_client_final(int u, bool bad, char *out, size_t cap)
{
    uint8_t salt[16], ck[32], sk[32], svk[32], sig[32], proof[32];
    memset(salt, fz_users[u].salt_byte, sizeof salt);
    tsdb_scram_keys(fz_users[u].pw, strlen(fz_users[u].pw), salt, 16, ITER_MIN, ck, sk, svk);
    char sb[tsdb_base64_len(16) + 1], pb[tsdb_base64_len(32) + 1], am[600];
    tsdb_base64_encode(salt, 16, sb);
    snprintf(am, sizeof am, "n=%s,r=" FZ_CNONCE ",r=" FZ_CNONCE FZ_SNONCE ",s=%s,i=%u,c=biws,r=" FZ_CNONCE FZ_SNONCE,
             fz_users[u].name, sb, (unsigned)ITER_MIN);
    tsdb_hmac_sha256(sk, 32, am, strlen(am), sig);
    for (int k = 0; k < 32; k++)
        proof[k] = ck[k] ^ sig[k];
    if (bad)
        proof[7] ^= 0x10;
    tsdb_base64_encode(proof, 32, pb);
    snprintf(out, cap, "c=biws,r=" FZ_CNONCE FZ_SNONCE ",p=%s", pb);
}

static void fz_hello(fb *f)
{
    fb p = {0};
    p32(&p, TSDB_PROTOCOL_VERSION);
    frame(f, TSDB_OP_HELLO, &p);
    free(p.p);
}

static void fz_start(fb *f, const char *mech, const char *client_first)
{
    fb p = {0};
    p8(&p, (uint8_t)strlen(mech));
    put(&p, mech, strlen(mech));
    pstr16(&p, client_first);
    frame(f, TSDB_OP_AUTH_START, &p);
    free(p.p);
}

static void fz_continue(fb *f, const char *client_final)
{
    fb p = {0};
    pstr16(&p, client_final);
    frame(f, TSDB_OP_AUTH_CONTINUE, &p);
    free(p.p);
}

/* HELLO, AUTH_START and AUTH_CONTINUE: user u's login. */
static void fz_login(fb *f, int u, bool bad)
{
    char cf[64], fin[200];
    fz_hello(f);
    snprintf(cf, sizeof cf, "n,,n=%s,r=" FZ_CNONCE, fz_users[u].name);
    fz_start(f, TSDB_SCRAM_SHA256, cf);
    fz_client_final(u, bad, fin, sizeof fin);
    fz_continue(f, fin);
}

/* A user admin op: the user's name, then u8 flags (flags >= 0), a verifier (ver), a grant (sel: u8 kind, u8 rights,
 * u16 len, selector). */
static void admin_frame(fb *f, uint8_t op, const char *name, int flags, bool ver, uint8_t kind, uint8_t rights,
                        const char *sel)
{
    fb p = {0};
    pstr16(&p, name);
    if (flags >= 0)
        p8(&p, (uint8_t)flags);
    if (ver) {
        p32(&p, ITER_MIN);
        p8(&p, 16);
        for (int i = 0; i < 16 + 64; i++)
            p8(&p, (uint8_t)(0x33 + i));
    }
    if (sel) {
        p8(&p, kind);
        p8(&p, rights);
        pstr16(&p, sel);
    }
    frame(f, op, &p);
    free(p.p);
}

static void id_frame(fb *f, uint8_t op, tsdb_sid id, bool range)
{
    fb p = {0};
    psid(&p, id);
    if (range)
        p64(&p, 0), p64(&p, 1000000);
    frame(f, op, &p);
    free(p.p);
}

static void name_frame(fb *f, uint8_t op, uint8_t create, const char *name)
{
    fb p = {0};
    p8(&p, create);
    pstr16(&p, name);
    frame(f, op, &p);
    free(p.p);
}

/* The seeds of login and of the checks after it, in the modes of run_one. */
static void login_seeds(const char *dir)
{
    fb f = {0}, p = {0};
    char cf[64], fin[200];
    snprintf(cf, sizeof cf, "n,,n=fz,r=" FZ_CNONCE);
    fz_client_final(M_USER, false, fin, sizeof fin);
#define MSEED(name, mode, BODY)                                                                                        \
    do {                                                                                                               \
        f.len = 0;                                                                                                     \
        put(&f, mode_bytes[mode], mode == M_ROOT ? 0 : MODE_LEN);                                                      \
        BODY;                                                                                                          \
        seed_write(dir, name, &f);                                                                                     \
    } while (0)
    /* before login */
    MSEED("pre-hello", M_PRE, fz_hello(&f));
    MSEED("pre-start", M_PRE, (fz_hello(&f), fz_start(&f, TSDB_SCRAM_SHA256, cf)));
    MSEED("pre-login-fz", M_PRE, (fz_login(&f, M_USER, false), id_frame(&f, TSDB_OP_QUERY_VALUES, TSDB_SID(0, 0), true),
                                  frame(&f, TSDB_OP_LAST_STATS, &p)));
    MSEED("pre-wrong-proof", M_PRE, (fz_login(&f, M_USER, true), fz_hello(&f)));
    MSEED("pre-unknown-user", M_PRE, (fz_hello(&f), fz_start(&f, TSDB_SCRAM_SHA256, "n,,n=nobody,r=" FZ_CNONCE),
                                      fz_continue(&f, fin)));
    MSEED("pre-bad-first", M_PRE, fz_start(&f, TSDB_SCRAM_SHA256, "n,,n=fz,r=" FZ_CNONCE ",x=1"));
    MSEED("pre-gs2-y", M_PRE, fz_start(&f, TSDB_SCRAM_SHA256, "y,,n=fz,r=" FZ_CNONCE));
    MSEED("pre-sha1", M_PRE, fz_start(&f, "SCRAM-SHA-1", cf));
    MSEED("pre-continue-first", M_PRE, fz_continue(&f, fin));
    MSEED("pre-stats", M_PRE, frame(&f, TSDB_OP_STATS, &p));
    MSEED("pre-cancel", M_PRE, (p32(&p, 0), frame(&f, TSDB_OP_CANCEL, &p), p.len = 0));
    MSEED("pre-big", M_PRE, ({
              for (int i = 0; i < 1100; i++)
                  p32(&p, TSDB_PROTOCOL_VERSION);
              frame(&f, TSDB_OP_HELLO, &p);
              p.len = 0;
          }));
    MSEED("pre-three", M_PRE, (fz_hello(&f), fz_hello(&f), fz_hello(&f), fz_start(&f, TSDB_SCRAM_SHA256, cf)));
    /* as fz: its grants decide */
    MSEED("fz-whoami", M_USER, (frame(&f, TSDB_OP_WHOAMI, &p), frame(&f, TSDB_OP_USER_LIST, &p)));
    MSEED("fz-data", M_USER, ({
              for (uint32_t i = 0; i < 5; i++) {
                  id_frame(&f, TSDB_OP_QUERY_VALUES, TSDB_SID(0, i), true);
                  id_frame(&f, TSDB_OP_GET_RETENTION, TSDB_SID(0, i), false);
              }
              id_frame(&f, TSDB_OP_DELETE_RANGE, TSDB_SID(0, 0), true);
              id_frame(&f, TSDB_OP_DELETE_RANGE, TSDB_SID(0, 1), true);
              id_frame(&f, TSDB_OP_QUERY_VALUES, TSDB_SID(0, 77), true); /* missing: DENIED, not NOTFOUND */
              name_frame(&f, TSDB_OP_SERIES, 0, "f64");
              name_frame(&f, TSDB_OP_SERIES, 0, "bool");
              name_frame(&f, TSDB_OP_SERIES, 1, "new1");
              name_frame(&f, TSDB_OP_SERIES, 1, "zzz");
              frame(&f, TSDB_OP_STATS, &p);
              fz_hello(&f); /* AUTH_* after login: INVALID, and the connection goes on */
              fz_start(&f, TSDB_SCRAM_SHA256, cf);
          }));
    MSEED("fz-admin-own", M_USER, ({
              admin_frame(&f, TSDB_OP_USER_CREATE, "fz/a", 0, true, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_GRANT, "fz/a", -1, false, SEL_PREFIX, R_READ, "fz/x");
              admin_frame(&f, TSDB_OP_GRANT, "fz/a", -1, false, SEL_GLOB, R_READ | R_INSERT, "fz/*y");
              frame(&f, TSDB_OP_USER_LIST, &p);
              admin_frame(&f, TSDB_OP_USER_ALTER, "fz/a", 0, false, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_USER_PASSWORD, "fz/a", -1, true, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_REVOKE, "fz/a", -1, false, SEL_PREFIX, R_READ, "fz/x");
              admin_frame(&f, TSDB_OP_USER_DROP, "fz/a", -1, false, 0, 0, NULL);
          }));
    MSEED("fz-admin-foreign", M_USER, ({
              admin_frame(&f, TSDB_OP_USER_DROP, "root", -1, false, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_USER_ALTER, "fz", TSDB_USER_SUPERUSER, false, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_GRANT, "fz", -1, false, SEL_ALL, R_EVERY, "");
              admin_frame(&f, TSDB_OP_USER_PASSWORD, "root", -1, true, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_USER_CREATE, "other", 1, true, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_USER_PASSWORD, "fz", -1, true, 0, 0, NULL);
              id_frame(&f, TSDB_OP_QUERY_VALUES, TSDB_SID(0, 0), true);
          }));
    /* as root */
    MSEED("root-admin", M_ROOT, ({
              admin_frame(&f, TSDB_OP_USER_CREATE, "u1", 0, true, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_GRANT, "u1", -1, false, SEL_GLOB, R_READ, "f*");
              admin_frame(&f, TSDB_OP_GRANT, "u1", -1, false, SEL_NAME, R_ADMIN, "state");
              frame(&f, TSDB_OP_WHOAMI, &p);
              frame(&f, TSDB_OP_USER_LIST, &p);
              admin_frame(&f, TSDB_OP_USER_ALTER, "u1", TSDB_USER_SUPERUSER, false, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_USER_PASSWORD, "u1", -1, true, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_REVOKE, "u1", -1, false, SEL_GLOB, R_READ, "f*");
              admin_frame(&f, TSDB_OP_USER_DROP, "u1", -1, false, 0, 0, NULL);
              admin_frame(&f, TSDB_OP_USER_DROP, "root", -1, false, 0, 0, NULL); /* the last superuser */
              admin_frame(&f, TSDB_OP_USER_ALTER, "root", 0, false, 0, 0, NULL);
          }));
#undef MSEED
    free(f.p);
    free(p.p);
}

/* USERS with root and fz, whose grants reach each kind of check: a name (f32: read, insert, delete), a prefix
 * (i: read), a glob (?oo*: read, so bool), insert on prefix new (the seeds create "new"), admin on prefix fz/
 * (users fz/... of its own to create, grant to and drop). Kept in memory for users_reset; the login frames too. */
static void users_make(void)
{
    snprintf(users_dir, sizeof users_dir, "%s/auth-users", db_dir);
    rm_rf(users_dir);
    mkdir(users_dir, 0755);
    users us;
    int rc = users_open(&us, users_dir, true);
    for (int u = 0; u < 2 && rc == 0; u++) {
        user_verifier v = {.iterations = ITER_MIN, .salt_len = 16};
        memset(v.salt, fz_users[u].salt_byte, 16);
        tsdb_scram_keys(fz_users[u].pw, strlen(fz_users[u].pw), v.salt, 16, ITER_MIN, NULL, v.stored_key,
                        v.server_key);
        uint32_t uid;
        rc = users_create(&us, 0, fz_users[u].name, u == 0 ? USER_SUPERUSER : 0, &v, &uid);
    }
    static const struct {
        uint8_t kind;
        const char *sel;
        uint8_t rights;
    } g[] = {{SEL_NAME, "f32", R_READ | R_INSERT | R_DELETE},
             {SEL_PREFIX, "i", R_READ},
             {SEL_GLOB, "?oo*", R_READ},
             {SEL_PREFIX, "new", R_INSERT},
             {SEL_PREFIX, "fz/", R_ADMIN}};
    for (size_t i = 0; i < sizeof g / sizeof g[0] && rc == 0; i++)
        rc = users_grant(&us, 0, "fz", g[i].kind, g[i].sel, strlen(g[i].sel), g[i].rights);
    if (rc == 0)
        users_close(&us);
    char p[4300];
    snprintf(p, sizeof p, "%s/USERS", users_dir);
    FILE *f = rc == 0 ? fopen(p, "rb") : NULL;
    uint8_t b[16384];
    users_len = f ? fread(b, 1, sizeof b, f) : 0;
    if (!f || !users_len || users_len == sizeof b || fclose(f) != 0 || !(users_file = malloc(users_len))) {
        fprintf(stderr, "fuzz_frames: cannot make USERS in %s: %s\n", users_dir, tsdb_strerror(rc));
        exit(2);
    }
    memcpy(users_file, b, users_len);
    auth_test_nonce = FZ_SNONCE;
    for (int u = 0; u < 2; u++) {
        fb l = {0};
        fz_login(&l, u, false);
        login_frames[u] = l.p;
        login_len[u] = l.len;
    }
}

static int cmd_seeds(const char *dir)
{
    mkdir(dir, 0755);
    fb f = {0}, p = {0};
#define SEED(name, op, BODY)                                                                                           \
    do {                                                                                                               \
        f.len = p.len = 0;                                                                                             \
        BODY;                                                                                                          \
        frame(&f, op, &p);                                                                                             \
        seed_write(dir, name, &f);                                                                                     \
    } while (0)
    SEED("hello", TSDB_OP_HELLO, p32(&p, TSDB_PROTOCOL_VERSION));
    SEED("hello-1", TSDB_OP_HELLO, p32(&p, 1));
    SEED("series-lookup", TSDB_OP_SERIES, (p8(&p, 0), pstr16(&p, "f32")));
    SEED("series-create", TSDB_OP_SERIES, (p8(&p, 1), pstr16(&p, "new")));
    SEED("series-path", TSDB_OP_SERIES, (p8(&p, 1), pstr16(&p, "vol"), pstr16(&p, "vol1")));
    SEED("hello-10", TSDB_OP_HELLO, p32(&p, 10)); /* refused: the connection reads nothing more */
    SEED("series-name", TSDB_OP_SERIES_NAME, psid(&p, 2));
    SEED("topology", TSDB_OP_TOPOLOGY, (void)0);
    SEED("topology-since", TSDB_OP_TOPOLOGY, p64(&p, 1)); /* since_gen */
    SEED("topology-bad-len", TSDB_OP_TOPOLOGY, p32(&p, 1)); /* neither empty nor a u64: INVALID */
    SEED("status", TSDB_OP_STATUS, (void)0);
    SEED("status-junk", TSDB_OP_STATUS, p32(&p, 7));
    SEED("series-name-id64", TSDB_OP_SERIES_NAME, psid(&p, 0x100000002ull)); /* unknown, not series 2 */
    SEED("insert", TSDB_OP_INSERT, ({
             p32(&p, 3);
             p32(&p, 0);
             psid(&p, 0);
             for (int i = 0; i < 3; i++)
                 p64(&p, 300000 + i);
             for (int i = 0; i < 3; i++) {
                 float v = (float)i;
                 put(&p, &v, 4);
             }
         }));
    SEED("insert-many", TSDB_OP_INSERT, ({
             p32(&p, 3);
             p32(&p, 0);
             psid(&p, TSDB_MANY_SERIES);
             p64(&p, 7);
             p64(&p, 8);
             p64(&p, 9);
             float v[3] = {1, 2, 3};
             put(&p, v, 12);
             p32(&p, 0); /* to a multiple of 8 */
             psid(&p, 0);
             psid(&p, 0);
             psid(&p, 0);
         }));
    SEED("query-raw", TSDB_OP_QUERY_RAW, (psid(&p, 0), p64(&p, 0), p64(&p, 1000000)));
    SEED("query-raw-ex", TSDB_OP_QUERY_RAW, (psid(&p, 0), p64(&p, 0), p64(&p, 1000000), p8(&p, 1), p64(&p, 10)));
    SEED("query-raw-id64", TSDB_OP_QUERY_RAW, (psid(&p, 0x100000000ull), p64(&p, 0), p64(&p, 1000000)));
    SEED("commit", TSDB_OP_COMMIT, (void)0);
    SEED("flush", TSDB_OP_FLUSH, (void)0);
    SEED("optimize", TSDB_OP_OPTIMIZE, (void)0);
    SEED("stats", TSDB_OP_STATS, (void)0);
    SEED("series-create-typed", TSDB_OP_SERIES_CREATE, (p8(&p, 2), pstr16(&p, "i"), p16(&p, 0)));
    SEED("series-create-path", TSDB_OP_SERIES_CREATE, (p8(&p, 1), pstr16(&p, "d"), pstr16(&p, "vol2")));
    SEED("series-type", TSDB_OP_SERIES_TYPE, psid(&p, 1));
    for (unsigned t = 0; t <= TSDB_STATE; t++) {
        char name[64];
        snprintf(name, sizeof name, "insert-values-%u", t);
        SEED(name, TSDB_OP_INSERT_VALUES, (values_header(&p, 5, t, t, 0), values_body(&p, t, 5, false, false)));
        snprintf(name, sizeof name, "insert-values-valid-many-%u", t);
        SEED(name, TSDB_OP_INSERT_VALUES,
             (values_header(&p, 9, TSDB_MANY_SERIES, t, TSDB_VALID), values_body(&p, t, 9, true, true)));
        snprintf(name, sizeof name, "replace-%u", t);
        SEED(name, TSDB_OP_REPLACE_RANGE, ({
                 values_header(&p, 3, t, t, TSDB_VALID);
                 p64(&p, 0);
                 p64(&p, 100000);
                 values_body(&p, t, 3, true, false);
             }));
        /* packed: the arrays reshaped and compressed, as the clients send them */
        for (int many = 0; many < 2; many++) {
            uint32_t n = 20;
            int64_t ts[20];
            uint8_t vals[20 * 8] = {0}, valid[3] = {0xff, 0x7f, 0x0f};
            uint64_t series[20];
            for (uint32_t i = 0; i < n; i++) /* float32: ids across 2^32 and back, and to UINT64_MAX - 1 (unknown) */
                ts[i] = 50000 + 10 * i, series[i] = t || i < 7 || i > 9 ? t : i == 7 ? 0x100000005ull : UINT64_MAX - 1,
                vals[i * tsdb_wire_width(t)] = (uint8_t)(t == 4 ? 0 : i);
            size_t bound = tsdb_pack_raw_bound(n, t, 1, many);
            uint8_t *raw = malloc(bound), *z = malloc(ZSTD_compressBound(bound));
            size_t rl = tsdb_pack_reshape(n, t, ts, vals, valid, many ? series : NULL, raw);
            size_t zl = ZSTD_compress(z, ZSTD_compressBound(bound), raw, rl, 1);
            snprintf(name, sizeof name, "insert-packed-%u%s", t, many ? "-many" : "");
            SEED(name, TSDB_OP_INSERT_VALUES, ({
                     values_header(&p, n, many ? TSDB_MANY_SERIES : t, t, TSDB_PACKED | TSDB_VALID);
                     p32(&p, (uint32_t)rl);
                     put(&p, z, zl);
                 }));
            free(raw);
            free(z);
        }
    }
    SEED("query-values", TSDB_OP_QUERY_VALUES, (psid(&p, 1), p64(&p, 0), p64(&p, 1000000)));
    SEED("query-values-ex", TSDB_OP_QUERY_VALUES, (psid(&p, 2), p64(&p, 0), p64(&p, 1000000), p8(&p, 1), p64(&p, 3)));
    SEED("state", TSDB_OP_STATE, (psid(&p, 4), p8(&p, 1), pstr16(&p, "off")));
    SEED("state-lookup", TSDB_OP_STATE, (psid(&p, 4), p8(&p, 0), pstr16(&p, "on")));
    SEED("state-name", TSDB_OP_STATE_NAME, (psid(&p, 4), p32(&p, 1)));
    SEED("query-buckets", TSDB_OP_QUERY_BUCKETS, ({
             psid(&p, 0);
             p64(&p, 0);
             p64(&p, 1000000);
             p64(&p, 20000);
             p8(&p, 0);
             p32(&p, 0);
             p8(&p, 0);
             p64(&p, 0);
             p16(&p, 0);
         }));
    SEED("query-buckets-spread", TSDB_OP_QUERY_BUCKETS, ({
             psid(&p, 0);
             p64(&p, 0);
             p64(&p, 1000000);
             p64(&p, 0);
             p8(&p, 1);
             p32(&p, 1);
             p8(&p, 1);
             p64(&p, 2);
             pstr16(&p, "UTC");
             p8(&p, TSDB_SPREAD);
         }));
    SEED("last-stats", TSDB_OP_LAST_STATS, (void)0);
    SEED("query-bounds", TSDB_OP_QUERY_BOUNDS, ({
             psid(&p, 0);
             p64(&p, 0);
             p64(&p, 1000000);
             p8(&p, 0);
             p64(&p, 0);
             p32(&p, 3);
             p64(&p, 0);
             p64(&p, 50000);
             p64(&p, 100000);
         }));
    SEED("query-bounds-spread", TSDB_OP_QUERY_BOUNDS, ({
             psid(&p, 1);
             p64(&p, 0);
             p64(&p, 1000000);
             p8(&p, 1);
             p64(&p, 1);
             p32(&p, 2);
             p64(&p, 0);
             p64(&p, 50000);
             p8(&p, TSDB_SPREAD);
         }));
    SEED("query-lttb", TSDB_OP_QUERY_LTTB, (psid(&p, 0), p64(&p, 0), p64(&p, 1000000), p64(&p, 10)));
    SEED("query-lttb-ex", TSDB_OP_QUERY_LTTB,
         (psid(&p, 0), p64(&p, 0), p64(&p, 1000000), p64(&p, 10), p8(&p, 1), p64(&p, 4)));
    for (int form = 0; form < 3; form++) {
        char name[64];
        snprintf(name, sizeof name, "query-twa-%d", form);
        SEED(name, TSDB_OP_QUERY_TWA, ({
                 psid(&p, 0);
                 p64(&p, 0);
                 p64(&p, 1000000);
                 p8(&p, (uint8_t)(form & 1)); /* method */
                 p8(&p, 0);
                 p8(&p, form == 1 ? 1 : 0); /* unit */
                 p8(&p, 0);
                 p64(&p, 5000);
                 p64(&p, 0);
                 p64(&p, form == 0 ? 10000 : 0);
                 p32(&p, form == 1 ? 1 : 0);
                 p32(&p, form == 2 ? 2 : 0);
                 if (form == 2) {
                     p64(&p, 0);
                     p64(&p, 30000);
                 }
             }));
    }
    /* TWA every 5 ms over series 0's 200 s: some 40,000 buckets, a frame of 1.6 MB that reaches the output in
     * pieces (OUT_PIECE) with the output drained between them (OUT_CAP); alone and as one series of a QUERY_MULTI */
    for (int multi = 0; multi < 2; multi++) {
        fb t = {0};
        psid(&t, multi ? TSDB_MANY_SERIES : 0);
        p64(&t, 0);
        p64(&t, 200000);
        p8(&t, 0), p8(&t, 0), p8(&t, 0), p8(&t, 0);
        p64(&t, 0);
        p64(&t, 0);
        p64(&t, 5);
        p32(&t, 0);
        p32(&t, 0);
        f.len = p.len = 0;
        if (multi)
            multi_payload(&p, TSDB_OP_QUERY_TWA, 0, 0, 2, 0, &t);
        frame(&f, multi ? TSDB_OP_QUERY_MULTI : TSDB_OP_QUERY_TWA, multi ? &p : &t);
        seed_write(dir, multi ? "multi-twa-large" : "query-twa-large", &f);
        free(t.p);
    }
    /* QUERY_MULTI: every inner op, every flag; then CANCEL of a queued one, and of nobody */
    {
        fb t = {0};
        struct {
            const char *name;
            uint8_t op, flags, form;
            uint16_t threads;
            uint64_t max_rows;
        } mq[] = {
            {"multi-values", TSDB_OP_QUERY_VALUES, 0, 0, 0, 0},
            {"multi-values-per", TSDB_OP_QUERY_VALUES, TSDB_MULTI_PER_START | TSDB_MULTI_PER_END, 1, 2, 0},
            {"multi-values-max-rows", TSDB_OP_QUERY_VALUES, 0, 0, 1, 5},
            {"multi-buckets", TSDB_OP_QUERY_BUCKETS, 0, 0, 0, 0},
            {"multi-buckets-per", TSDB_OP_QUERY_BUCKETS,
             TSDB_MULTI_PER_START | TSDB_MULTI_PER_END | TSDB_MULTI_PER_INTERVAL, 1, 3, 0},
            {"multi-buckets-group", TSDB_OP_QUERY_BUCKETS, TSDB_MULTI_PER_END, 2, 0, 0},
            {"multi-bounds", TSDB_OP_QUERY_BOUNDS, 0, 0, 0, 0},
            {"multi-bounds-trim", TSDB_OP_QUERY_BOUNDS, TSDB_MULTI_WIRE_TRIM_LO | TSDB_MULTI_WIRE_TRIM_HI, 0, 2, 0},
            {"multi-lttb", TSDB_OP_QUERY_LTTB, TSDB_MULTI_PER_START, 1, 0, 0},
            {"multi-twa-interval", TSDB_OP_QUERY_TWA, TSDB_MULTI_PER_INTERVAL, 0, 0, 0},
            {"multi-twa-unit", TSDB_OP_QUERY_TWA, TSDB_MULTI_PER_START, 1, 4, 0},
            {"multi-twa-bounds-trim", TSDB_OP_QUERY_TWA, TSDB_MULTI_WIRE_TRIM_LO, 2, 0, 3},
        };
        for (size_t k = 0; k < sizeof mq / sizeof mq[0]; k++) {
            bool ps = mq[k].flags & TSDB_MULTI_PER_START, pe = mq[k].flags & TSDB_MULTI_PER_END,
                 pi = mq[k].flags & TSDB_MULTI_PER_INTERVAL;
            int form = mq[k].form;
            t.len = 0;
            psid(&t, TSDB_MANY_SERIES);
            p64(&t, ps ? 0 : 0);
            p64(&t, pe ? 0 : 1000000);
            switch (mq[k].op) {
            case TSDB_OP_QUERY_VALUES: p8(&t, (uint8_t)form), p64(&t, form ? 7 : 0); break;
            case TSDB_OP_QUERY_LTTB: p64(&t, 10), p8(&t, 1), p64(&t, 4); break;
            case TSDB_OP_QUERY_BUCKETS:
                p64(&t, pi || form == 2 ? 0 : 20000);
                p8(&t, form == 2 ? 1 : 0); /* unit: hours */
                p32(&t, form == 2 ? 1 : 0);
                p8(&t, (uint8_t)(form & 1));
                p64(&t, form ? 2 : 0);
                if (form == 2)
                    pstr16(&t, "UTC");
                else
                    p16(&t, 0);
                p8(&t, form == 1 ? TSDB_SPREAD : 0);
                break;
            case TSDB_OP_QUERY_BOUNDS:
                p8(&t, 0), p64(&t, 0), p32(&t, 3), p64(&t, 0), p64(&t, 50000), p64(&t, 100000);
                break;
            case TSDB_OP_QUERY_TWA:
                p8(&t, (uint8_t)(form & 1)); /* method */
                p8(&t, 0);
                p8(&t, form == 1 ? 1 : 0); /* unit */
                p8(&t, 0);
                p64(&t, 5000);
                p64(&t, 0);
                p64(&t, form == 0 && !pi ? 10000 : 0);
                p32(&t, form == 1 ? 1 : 0);
                p32(&t, form == 2 ? 2 : 0);
                if (form == 2)
                    p64(&t, 0), p64(&t, 30000);
                break;
            }
            SEED(mq[k].name, TSDB_OP_QUERY_MULTI,
                 multi_payload(&p, mq[k].op, mq[k].flags, mq[k].threads, 6, mq[k].max_rows, &t));
        }
        /* a QUERY_MULTI, a second one cancelled while queued, a CANCEL of nobody, then a query */
        f.len = p.len = 0;
        t.len = 0;
        psid(&t, TSDB_MANY_SERIES), p64(&t, 0), p64(&t, 1000000), p8(&t, 0), p64(&t, 0);
        multi_payload(&p, TSDB_OP_QUERY_VALUES, 0, 0, 3, 0, &t);
        frame(&f, TSDB_OP_QUERY_MULTI, &p);
        frame(&f, TSDB_OP_QUERY_MULTI, &p);
        p.len = 0;
        p32(&p, 1);
        frame(&f, TSDB_OP_CANCEL, &p);
        p.len = 0;
        p32(&p, 12345);
        frame(&f, TSDB_OP_CANCEL, &p);
        frame(&f, TSDB_OP_LAST_STATS, &(fb){0});
        seed_write(dir, "multi-cancel", &f);
        free(t.p);
    }
    SEED("delete-range", TSDB_OP_DELETE_RANGE, (psid(&p, 0), p64(&p, 10000), p64(&p, 20000)));
    SEED("delete-range-id64", TSDB_OP_DELETE_RANGE, (psid(&p, 0x100000000ull), p64(&p, 10000), p64(&p, 20000)));
    SEED("set-retention", TSDB_OP_SET_RETENTION, (psid(&p, 1), p64(&p, 86400000)));
    SEED("get-retention", TSDB_OP_GET_RETENTION, psid(&p, 1));
    /* a pipeline: several requests in one input */
    f.len = 0;
    p.len = 0;
    p32(&p, TSDB_PROTOCOL_VERSION);
    frame(&f, TSDB_OP_HELLO, &p);
    p.len = 0;
    values_header(&p, 4, 1, 1, 0);
    values_body(&p, 1, 4, false, false);
    frame(&f, TSDB_OP_INSERT_VALUES, &p);
    p.len = 0;
    psid(&p, 1), p64(&p, 0), p64(&p, 1000000);
    frame(&f, TSDB_OP_QUERY_VALUES, &p);
    frame(&f, TSDB_OP_LAST_STATS, &(fb){0});
    seed_write(dir, "pipeline", &f);
    free(f.p);
    free(p.p);
    login_seeds(dir);
    return 0;
}

/* ------------------------------------------------------------ the corpus */

typedef struct input {
    uint8_t *p;
    size_t len;
} input;
static input *corpus;
static size_t ncorpus, corpus_cap;

static void corpus_add(const uint8_t *p, size_t len)
{
    if (ncorpus == corpus_cap) {
        corpus_cap = corpus_cap ? corpus_cap * 2 : 256;
        corpus = realloc(corpus, corpus_cap * sizeof *corpus);
        if (!corpus)
            abort();
    }
    uint8_t *c = malloc(len ? len : 1);
    if (!c)
        abort();
    memcpy(c, p, len);
    corpus[ncorpus++] = (input){c, len};
}

static int load_file(const char *path)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return -1;
    uint8_t *b = malloc(1 << 20);
    size_t n = b ? fread(b, 1, 1 << 20, f) : 0;
    fclose(f);
    if (b)
        corpus_add(b, n);
    free(b);
    return 0;
}

/* A fuzz run seeds from its earlier output as well; a crash-* input there is replayed by `replay`, not mutated. */
static bool skip_crashes;

static void load_path(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "fuzz_frames: %s: %s\n", path, strerror(errno));
        exit(2);
    }
    if (!S_ISDIR(st.st_mode)) {
        load_file(path);
        return;
    }
    struct dirent **list;
    int n = scandir(path, &list, NULL, alphasort);
    for (int i = 0; i < n; i++) {
        if (list[i]->d_name[0] != '.' && !(skip_crashes && !strncmp(list[i]->d_name, "crash-", 6))) {
            char p[4200];
            snprintf(p, sizeof p, "%s/%s", path, list[i]->d_name);
            if (stat(p, &st) == 0 && S_ISREG(st.st_mode))
                load_file(p);
        }
        free(list[i]);
    }
    free(list);
}

/* ------------------------------------------------------------ mutations */

static uint64_t rng_state = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}
static size_t below(size_t n) { return n ? (size_t)(rnd() % n) : 0; }

#define FZ_MAX_INPUT (64u << 10)

/* The frames of an input: offsets of their payloads (after the 9-byte header). Bytes after the last whole
 * frame are a tail. */
typedef struct fr {
    size_t at, len; /* payload */
} fr;

static size_t split(const uint8_t *p, size_t len, fr *out, size_t max)
{
    size_t n = 0, off = 0;
    while (n < max && len - off >= 9) {
        uint32_t flen;
        memcpy(&flen, p + off, 4);
        if (flen < 5 || len - off - 4 < flen)
            break;
        out[n++] = (fr){off + 9, flen - 5};
        off += 4 + flen;
    }
    return n;
}

static const int64_t interesting[] = {0,          1,          -1,         2,          3,          7,         8,
                                      16,         64,         255,        256,        4096,       65535,     65536,
                                      0x7fff,     0x8000,     0x7fffffff, 0x80000000, 0xffffffff, 0x100000000ll,
                                      INT64_MAX,  INT64_MIN,  INT64_MAX - 1, INT64_MIN + 1, 86400000, 3600000,
                                      (int64_t)TSDB_MAX_FRAME, (int64_t)TSDB_MAX_FRAME / 8, (int64_t)1 << 40};

/* Writes the length of the frame whose payload starts at `at` (n bytes). */
static void set_len(uint8_t *p, size_t at, size_t n)
{
    uint32_t l = (uint32_t)(n + 5);
    memcpy(p + at - 9, &l, 4);
}

/* Returns the new length of p (capacity FZ_MAX_INPUT). */
static size_t mutate(uint8_t *p, size_t len)
{
    fr frs[256];
    size_t nf = split(p, len, frs, 256);
    int rounds = 1 + (int)below(4);
    for (int k = 0; k < rounds; k++) {
        unsigned what = (unsigned)below(nf ? 14 : 3);
        if (what < 3 || !nf) { /* havoc on raw bytes, framing included */
            if (!len)
                return len;
            size_t i = below(len);
            if (what == 0)
                p[i] ^= (uint8_t)(1u << below(8));
            else if (what == 1)
                p[i] = (uint8_t)rnd();
            else if (len > 1)
                len = 1 + below(len); /* truncate */
            nf = split(p, len, frs, 256);
            continue;
        }
        fr *f = &frs[below(nf)];
        switch (what) {
        case 3:
        case 4: /* a byte of a payload */
            if (f->len)
                p[f->at + below(f->len)] ^= (uint8_t)(1u << below(8));
            break;
        case 5:
        case 6:
        case 7: { /* an interesting value at a field position (4- or 8-byte, aligned to the layouts' fields) */
            int64_t v = interesting[below(sizeof interesting / sizeof *interesting)];
            if (rnd() & 1)
                v = (int64_t)((uint64_t)v + below(3) - 1);
            size_t w = rnd() & 1 ? 8 : rnd() & 1 ? 4 : rnd() & 1 ? 2 : 1;
            if (f->len >= w) {
                size_t at = f->at + below(f->len - w + 1);
                memcpy(p + at, &v, w);
            }
            break;
        }
        case 8: /* the op */
            p[f->at - 1] = (uint8_t)below(32);
            break;
        case 9: { /* grow or shrink a payload, keeping its length right */
            size_t end = f->at + f->len;
            if (rnd() & 1 && f->len) {
                size_t cut = 1 + below(f->len < 16 ? f->len : 16);
                memmove(p + end - cut, p + end, len - end);
                len -= cut;
                set_len(p, f->at, f->len - cut);
            } else {
                size_t add = 1 + below(rnd() & 1 ? 8 : 64);
                if (len + add > FZ_MAX_INPUT)
                    break;
                memmove(p + end + add, p + end, len - end);
                for (size_t i = 0; i < add; i++)
                    p[end + i] = rnd() & 1 ? 0 : (uint8_t)rnd();
                len += add;
                set_len(p, f->at, f->len + add);
            }
            nf = split(p, len, frs, 256);
            break;
        }
        case 10: { /* duplicate a frame */
            size_t fl = 9 + f->len;
            if (len + fl > FZ_MAX_INPUT)
                break;
            memcpy(p + len, p + f->at - 9, fl);
            len += fl;
            nf = split(p, len, frs, 256);
            break;
        }
        case 11: { /* append a frame of another input */
            input *o = &corpus[below(ncorpus)];
            const uint8_t *op = o->p + (input_mode(o->p, o->len) == M_ROOT ? 0 : MODE_LEN);
            fr of[64];
            size_t on = split(op, o->len - (size_t)(op - o->p), of, 64);
            if (!on)
                break;
            fr *g = &of[below(on)];
            if (len + 9 + g->len > FZ_MAX_INPUT)
                break;
            memcpy(p + len, op + g->at - 9, 9 + g->len);
            len += 9 + g->len;
            nf = split(p, len, frs, 256);
            break;
        }
        case 12: /* a payload's bytes from another frame of the same op, spliced */
        case 13: { /* the count fields: n consistent or not with the payload */
            if (f->len >= 4) {
                uint32_t n = (uint32_t)below(f->len / 4 + 2);
                if (what == 13 && rnd() & 1)
                    n = (uint32_t)rnd();
                size_t at = f->at + (below(4) == 0 ? below(f->len - 3) : 0);
                memcpy(p + at, &n, 4);
            }
            break;
        }
        }
    }
    return len;
}

/* mutate on the frames after the mode; now and then the mode changes (who sends them: root, fz, nobody yet).
 * p holds FZ_MAX_INPUT + MODE_LEN bytes. */
static size_t mutate_input(uint8_t *p, size_t len)
{
    int mode = input_mode(p, len);
    size_t at = mode == M_ROOT ? 0 : MODE_LEN;
    len = at + mutate(p + at, len - at);
    if (below(16) == 0) {
        int to = (int)below(3);
        size_t to_at = to == M_ROOT ? 0 : MODE_LEN;
        memmove(p + to_at, p + at, len - at);
        memcpy(p, mode_bytes[to], to_at);
        len = len - at + to_at;
    }
    return len;
}

/* ----------------------------------------------------------------- main */

static void setup(const char *dir)
{
    if (dir)
        snprintf(db_dir, sizeof db_dir, "%s", dir);
    else
        snprintf(db_dir, sizeof db_dir, "/tmp/fuzz_frames.%d", getpid());
    mkdir(db_dir, 0755);
    if (chdir(db_dir) != 0) { /* relative volume paths land in it */
        perror("fuzz_frames: chdir");
        exit(2);
    }
    signal(SIGALRM, on_fatal);
    signal(SIGSEGV, on_fatal);
    signal(SIGABRT, on_fatal);
    signal(SIGPIPE, SIG_IGN);
    if (__sanitizer_set_death_callback)
        __sanitizer_set_death_callback(fail_dump);
    send_timeout_s = 1;
    users_make();
    db_start();
}

int main(int argc, char **argv)
{
    if (argc >= 3 && !strcmp(argv[1], "seeds"))
        return cmd_seeds(argv[2]);
    if (argc < 3 || (strcmp(argv[1], "replay") && strcmp(argv[1], "fuzz"))) {
        fprintf(stderr, "usage: fuzz_frames seeds DIR | replay [--dir D] PATH... | fuzz [--dir D] [--seconds N] "
                        "[--out DIR] [--seed S] CORPUS...\n");
        return 2;
    }
    bool fuzz = !strcmp(argv[1], "fuzz");
    const char *dir = NULL, *out = NULL;
    long seconds = 60;
    int i = 2;
    for (; i < argc && argv[i][0] == '-'; i++) {
        if (i + 1 >= argc)
            break;
        if (!strcmp(argv[i], "--dir"))
            dir = argv[++i];
        else if (!strcmp(argv[i], "--out"))
            out = argv[++i];
        else if (!strcmp(argv[i], "--seconds"))
            seconds = strtol(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--seed"))
            rng_state = strtoull(argv[++i], NULL, 0) | 1;
        else
            break;
    }
    skip_crashes = fuzz;
    for (; i < argc; i++)
        load_path(argv[i]);
    if (!ncorpus) {
        fprintf(stderr, "fuzz_frames: no inputs\n");
        return 2;
    }
    if (out) {
        mkdir(out, 0755);
        static char abs_out[4096];
        if (!realpath(out, abs_out)) {
            perror("fuzz_frames: --out");
            return 2;
        }
        crash_dir = abs_out;
    }
    char cwd[4096];
    if (!getcwd(cwd, sizeof cwd))
        return 2;
    setup(dir);
    if (!fuzz) {
        verbose = getenv("FUZZ_VERBOSE") != NULL;
        for (size_t k = 0; k < ncorpus; k++)
            timed_run(corpus[k].p, corpus[k].len);
        printf("fuzz_frames: %zu inputs replayed\n", ncorpus);
        db_stop();
        if (!dir)
            rm_rf(db_dir);
        return 0;
    }
    cov_on = 1;
    for (size_t k = 0; k < ncorpus; k++) {
        memset(cov_map, 0, sizeof cov_map);
        timed_run(corpus[k].p, corpus[k].len);
        cov_new();
    }
    printf("fuzz_frames: %zu seeds, %zu edges\n", ncorpus, cov_edges());
    uint8_t *buf = malloc(FZ_MAX_INPUT + MODE_LEN);
    time_t t0 = time(NULL), last = t0;
    uint64_t execs = 0, kept = 0;
    while (time(NULL) - t0 < seconds) {
        input *src = &corpus[below(ncorpus)];
        size_t len = src->len < FZ_MAX_INPUT ? src->len : FZ_MAX_INPUT;
        memcpy(buf, src->p, len);
        len = mutate_input(buf, len);
        memset(cov_map, 0, sizeof cov_map);
        timed_run(buf, len);
        execs++;
        if (cov_new()) {
            corpus_add(buf, len);
            kept++;
            if (out) {
                char p[4200];
                snprintf(p, sizeof p, "%s/in-%ld-%d-%06llu", crash_dir, (long)t0, getpid(), (unsigned long long)kept);
                int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
                if (fd >= 0) {
                    if (write(fd, buf, len) != (ssize_t)len)
                        perror("fuzz_frames: write");
                    close(fd);
                }
            }
        }
        if (execs % 5000 == 0) { /* the database grows with what inputs insert: start again now and then */
            db_stop();
            db_start();
        }
        if (time(NULL) - last >= 30) {
            last = time(NULL);
            printf("fuzz_frames: %llu execs, %zu inputs, %zu edges, %lld s\n", (unsigned long long)execs, ncorpus,
                   cov_edges(), (long long)(last - t0));
            fflush(stdout);
        }
    }
    printf("fuzz_frames: done: %llu execs, %zu inputs (%llu new), %zu edges\n", (unsigned long long)execs, ncorpus,
           (unsigned long long)kept, cov_edges());
    free(buf);
    db_stop();
    if (!dir)
        rm_rf(db_dir);
    (void)cwd;
    return 0;
}
