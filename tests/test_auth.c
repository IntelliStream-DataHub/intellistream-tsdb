/* SPDX-License-Identifier: AGPL-3.0-only */
/* Login (docs/design-auth.md §3, §9): tsdbd started with USERS and without --no-auth, through the C client and raw
 * frames, on the backend TSDBD_IO names (make test runs uring and epoll). The RFC 7677 §3 exchange runs twice byte
 * for byte: the C client against a fake server here, and tsdbd's server side (server/auth.c, linked in) in this
 * process. Users are made with the CLI (`tsdb DIR user ...`, beside TSDBD), which is also how an operator's offline
 * edit reaches a running server.
 *
 *   TSDBD=build/tsdbd TSDBD_IO=epoll ./build/test_auth [scratch]
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
#include "rfc7677.h"
#include "client_hooks.h"
#include "portable.h" /* pthread_barrier_t on macOS */

#include <pthread.h>

/* s= and i= of a server-first-message, for comparing them */
static void salt_iter(const char *sf, char *out, size_t cap)
{
    const char *s = strstr(sf, ",s=");
    snprintf(out, cap, "%s", s ? s + 1 : "");
}

/* ------------------------------------------------------------ the tests */

static tsdb_sid sx, sy, sz; /* acme/x, acme/y, globex/z */

static void test_login(void)
{
    tsdbc *root = login_as("root", "rootpw");
    tsdbc_user *me = NULL;
    OK(tsdbc_whoami(root, &me));
    CHECK(strcmp(me->name, "root") == 0 && (me->flags & TSDB_USER_SUPERUSER));
    tsdbc_users_free(me, 1);
    OK(tsdbc_series(root, "acme/x", 1, &sx));
    OK(tsdbc_series(root, "acme/y", 1, &sy));
    OK(tsdbc_series(root, "globex/z", 1, &sz));
    int64_t ts = T0;
    float v = 1;
    OK(tsdbc_insert_series(root, sx, &ts, &v, 1));
    OK(tsdbc_insert_series(root, sy, &ts, &v, 1));
    OK(tsdbc_insert_series(root, sz, &ts, &v, 1));
    OK(tsdbc_commit(root));
    tsdbc_close(root);

    tsdbc *c = NULL;
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "root", "wrongpw", &c) == TSDB_ERR_AUTH && !c);
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "nobody", "rootpw", &c) == TSDB_ERR_AUTH && !c);
    CHECK(tsdbc_connect("127.0.0.1", port, &c) == TSDB_ERR_AUTH && !c); /* the server asks; no user given */

    /* the password from a file, its first line */
    char pwf[PATH_MAX + 64];
    snprintf(pwf, sizeof pwf, "%s.pw", srvlog);
    FILE *f = fopen(pwf, "w");
    fputs("ingestpw\r\nsecond line\n", f);
    fclose(f);
    tsdbc_options o = {.user = "acme/ingest", .password_file = pwf, .require_auth = 1};
    OK(tsdbc_connect_opts("127.0.0.1", port, &o, &c));
    OK(tsdbc_whoami(c, &me));
    CHECK(strcmp(me->name, "acme/ingest") == 0 && me->flags == 0 && me->ngrants == 0);
    tsdbc_users_free(me, 1);
    CHECK(query(c, sx) == TSDB_ERR_DENIED); /* no grants yet */
    tsdbc_close(c);
    unlink(pwf);
    printf("  login: right and wrong password, unknown user, no user, password file\n");
}

/* REVIEW3 #1: a pool's connections start together. PBKDF2 runs once for all of them (the others wait for it), and
 * the cache gets one entry, so another user's entry survives. Iterations high enough that the old code (every miss
 * computed and inserted its copy) ran it once per connection and filled the 16-entry ring with copies. */
#define POOL 24
struct pool_arg {
    const char *user, *pw;
    pthread_barrier_t *go;
    int rc;
};

static void *pool_conn(void *arg)
{
    struct pool_arg *a = arg;
    tsdbc *c = NULL;
    pthread_barrier_wait(a->go);
    a->rc = tsdbc_connect_auth("127.0.0.1", port, a->user, a->pw, &c);
    if (c)
        tsdbc_close(c);
    return NULL;
}

static unsigned cold_pool(const char *user, const char *pw, int n)
{
    pthread_t th[POOL];
    struct pool_arg a[POOL];
    pthread_barrier_t go;
    pthread_barrier_init(&go, NULL, (unsigned)n);
    unsigned before = __atomic_load_n(&tsdbc_test_pbkdf2_runs, __ATOMIC_RELAXED);
    for (int i = 0; i < n; i++) {
        a[i] = (struct pool_arg){.user = user, .pw = pw, .go = &go, .rc = 1};
        pthread_create(&th[i], NULL, pool_conn, &a[i]);
    }
    for (int i = 0; i < n; i++) {
        pthread_join(th[i], NULL);
        CHECK(a[i].rc == 0);
    }
    pthread_barrier_destroy(&go);
    return __atomic_load_n(&tsdbc_test_pbkdf2_runs, __ATOMIC_RELAXED) - before;
}

/* The PBKDF2 runs of one login, which must end in rc */
static unsigned runs_of_login(const char *user, const char *pw, int rc)
{
    unsigned before = __atomic_load_n(&tsdbc_test_pbkdf2_runs, __ATOMIC_RELAXED);
    tsdbc *c = NULL;
    CHECK(tsdbc_connect_auth("127.0.0.1", port, user, pw, &c) == rc);
    if (c)
        tsdbc_close(c);
    return __atomic_load_n(&tsdbc_test_pbkdf2_runs, __ATOMIC_RELAXED) - before;
}

static void test_key_cache(void)
{
    tsdbc *root = login_as("root", "rootpw");
    OK(tsdbc_user_create(root, "acme/pool", "poolpw", 100000, 0));
    OK(tsdbc_user_create(root, "acme/pool2", "pool2pw", 100000, 0));
    tsdbc_close(root);
    CHECK(runs_of_login("root", "rootpw", 0) == 0); /* cached since test_login */
    unsigned r = cold_pool("acme/pool", "poolpw", POOL);
    CHECK(r == 1);
    CHECK(runs_of_login("acme/pool", "poolpw", 0) == 0);
    r += cold_pool("acme/pool2", "pool2pw", 16);
    CHECK(r == 2);
    CHECK(runs_of_login("root", "rootpw", 0) == 0); /* the cold pool of 16 left root's entry alone */
    CHECK(runs_of_login("acme/pool", "poolpw", 0) == 0);
    CHECK(runs_of_login("acme/pool", "wrong", TSDB_ERR_AUTH) == 1);
    CHECK(runs_of_login("acme/pool", "poolpw", 0) == 0);
    printf("  key cache: a cold pool of %d runs PBKDF2 once (%u runs for two pools); other users' keys stay\n", POOL,
           r);
}

/* REVIEW3 #2, #3: a password is 1 to 1023 bytes everywhere; a password file's first line without "\n" or "\r\n";
 * an empty or longer one, or a file that is not there, is TSDB_ERR_INVALID before anything is sent. */
static int file_login(const char *user, const char *content, size_t n)
{
    char pwf[PATH_MAX + 64];
    snprintf(pwf, sizeof pwf, "%s.pwrule", srvlog);
    FILE *f = fopen(pwf, "w");
    fwrite(content, 1, n, f);
    fclose(f);
    tsdbc_options o = {.user = user, .password_file = pwf};
    tsdbc *c = NULL;
    int rc = tsdbc_connect_opts("127.0.0.1", port, &o, &c);
    if (c)
        tsdbc_close(c);
    unlink(pwf);
    return rc;
}

static void test_password_rule(void)
{
    char p1023[1023 + 3], p1024[1024 + 1];
    for (int i = 0; i < 1023; i++)
        p1023[i] = (char)(' ' + i % 95); /* spaces, commas and '=' too */
    p1023[1023] = '\0';
    memcpy(p1024, p1023, 1023);
    p1024[1023] = 'x', p1024[1024] = '\0';
    tsdbc *root = login_as("root", "rootpw"), *c = NULL;
    CHECK(tsdbc_user_create(root, "acme/p0", "", 0, 0) == TSDB_ERR_INVALID);
    CHECK(tsdbc_user_create(root, "acme/p0", p1024, 0, 0) == TSDB_ERR_INVALID);
    OK(tsdbc_user_create(root, "acme/p1", "1", 0, 0));
    OK(tsdbc_user_create(root, "acme/p1023", p1023, 0, 0));
    CHECK(tsdbc_user_password(root, "acme/p1", "", 0) == TSDB_ERR_INVALID);
    CHECK(tsdbc_user_password(root, "acme/p1", p1024, 0) == TSDB_ERR_INVALID);
    tsdbc_close(root);

    tsdbc_close(login_as("acme/p1", "1"));
    tsdbc_close(login_as("acme/p1023", p1023));
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "acme/p1", "", &c) == TSDB_ERR_INVALID && !c);
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "acme/p1023", p1024, &c) == TSDB_ERR_INVALID && !c);
    /* refused up front: no server needed */
    CHECK(tsdbc_connect_auth("127.0.0.1", 1, "acme/p1", "", &c) == TSDB_ERR_INVALID && !c);

    CHECK(file_login("acme/p1", "1", 1) == 0);
    CHECK(file_login("acme/p1", "1\n", 2) == 0);
    CHECK(file_login("acme/p1", "1\r\nx\n", 5) == 0);
    CHECK(file_login("acme/p1023", p1023, 1023) == 0);
    memcpy(p1023 + 1023, "\r\n", 3);
    CHECK(file_login("acme/p1023", p1023, 1025) == 0);
    CHECK(file_login("acme/p1023", p1023, 1024) == TSDB_ERR_INVALID); /* a lone '\r' is the 1024th byte */
    p1023[1023] = '\0';
    CHECK(file_login("acme/p1023", p1024, 1024) == TSDB_ERR_INVALID);
    CHECK(file_login("acme/p1", "", 0) == TSDB_ERR_INVALID);
    CHECK(file_login("acme/p1", "\n1\n", 3) == TSDB_ERR_INVALID);
    CHECK(file_login("acme/p1", "\r\n", 2) == TSDB_ERR_INVALID);
    CHECK(file_login("acme/p1", "1\0002\n", 4) == TSDB_ERR_INVALID);
    tsdbc_options o = {.user = "acme/p1", .password_file = "/nonexistent/tsdb-password"};
    CHECK(tsdbc_connect_opts("127.0.0.1", port, &o, &c) == TSDB_ERR_INVALID && !c);
    printf("  password rule: 1 to 1023 bytes by string, file and verifier; files with and without CR LF\n");
}

/* A4: an unknown name gets a server-first-message of the same shape as a real one, with the same salt and
 * iterations on every try and after a restart; its failure is the same bytes as a wrong password's. Returns the
 * mock's "s=...,i=..." for the restart check. */
static void test_mock_salt(char *mock, size_t cap)
{
    char sf1[600], sf2[600], sfr[600], a[200], b[200], r[200];
    int fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=nobody,r=abc", sf1, sizeof sf1) == 0);
    close(fd);
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=nobody,r=abc", sf2, sizeof sf2) == 0);
    close(fd);
    salt_iter(sf1, a, sizeof a);
    salt_iter(sf2, b, sizeof b);
    CHECK(a[0] && strcmp(a, b) == 0);
    CHECK(strcmp(sf1, sf2) != 0); /* the server's nonce is new every time */
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=acme/ingest,r=abc", sfr, sizeof sfr) == 0);
    close(fd);
    salt_iter(sfr, r, sizeof r);
    CHECK(strlen(r) == strlen(a) && strcmp(strstr(r, ",i="), strstr(a, ",i=")) == 0); /* the same shape */
    CHECK(strcmp(r, a) != 0);
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=nobodyelse,r=abc", sf2, sizeof sf2) == 0);
    close(fd);
    salt_iter(sf2, b, sizeof b);
    CHECK(strcmp(a, b) != 0); /* a salt per name, not one for every unknown name */
    snprintf(mock, cap, "%s", a);

    /* the failure: the same frame for an unknown user and for a wrong password, then the end of the stream */
    uint8_t f1[64], f2[64], rest[64];
    size_t n1 = 0, n2 = 0, nr;
    char fin[700], v[64];
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=nobody,r=abc", sf1, sizeof sf1) == 0);
    client_final("nobody", "rootpw", "abc", sf1, fin, sizeof fin, v, sizeof v);
    raw_continue_send(fd, fin);
    CHECK(raw_frame(fd, NULL, rest, sizeof rest, &nr, f1, &n1) == TSDB_ERR_AUTH);
    CHECK(closed(fd));
    close(fd);
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=acme/ingest,r=abc", sf2, sizeof sf2) == 0);
    client_final("acme/ingest", "wrongpw", "abc", sf2, fin, sizeof fin, v, sizeof v);
    raw_continue_send(fd, fin);
    CHECK(raw_frame(fd, NULL, rest, sizeof rest, &nr, f2, &n2) == TSDB_ERR_AUTH);
    CHECK(closed(fd));
    close(fd);
    CHECK(n1 == 13 && n1 == n2 && memcmp(f1, f2, n1) == 0);
    printf("  unknown user: the mock salt (%s), failure bytes as a wrong password's\n", mock);
}

/* Anything but "n,,n=NAME,r=NONCE" and SCRAM-SHA-256 fails the login and closes the connection. */
static void test_malformed(void)
{
    static const char *bad[] = {
        "n,a=root,n=root,r=abc",   /* an authzid */
        "y,,n=root,r=abc",         /* "the client supports binding, the server does not" */
        "p=tls-unique,,n=root,r=abc", /* channel binding */
        "n,,n=root,r=abc,x=ext",   /* an extension */
        "n,,m=ext,n=root,r=abc",   /* a mandatory extension */
        "n,,n=root",               /* no nonce */
        "n,,n=root,r=",            /* an empty nonce */
        "n,,r=abc,n=root",         /* out of order */
        "n,,n=,r=abc",             /* an empty name */
        "n,,n=ro=2Cot,r=abc",      /* a name with an escape (no user name holds ',' or '=') */
        "n,,n=root,r=a b",         /* a nonce with a space */
        "",
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        int fd = raw_connect();
        CHECK(raw_hello(fd) == 0);
        int32_t st = raw_start(fd, bad[i], NULL, 0);
        if (st != TSDB_ERR_AUTH)
            fprintf(stderr, "  client-first \"%s\": %d\n", bad[i], st);
        CHECK(st == TSDB_ERR_AUTH);
        CHECK(closed(fd));
        close(fd);
    }
    int fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start_mech(fd, "SCRAM-SHA-1", "n,,n=root,r=abc", NULL, 0) == TSDB_ERR_AUTH);
    CHECK(closed(fd));
    close(fd);

    /* client-final: the wrong channel binding, a nonce of its own, no proof, a short proof */
    char sf[600], fin[700], v[64];
    const char *finals[] = {"c=eSws,r=%s,p=AAAA", "c=biws,r=%sX,p=%s", "c=biws,r=%s", "c=biws,r=%s,p=AAAA"};
    for (size_t i = 0; i < 4; i++) {
        fd = raw_connect();
        CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=root,r=abc", sf, sizeof sf) == 0);
        char nonce[300];
        snprintf(nonce, sizeof nonce, "%.*s", (int)(strstr(sf, ",s=") - sf - 2), sf + 2);
        client_final("root", "rootpw", "abc", sf, fin, sizeof fin, v, sizeof v);
        char f[800];
        snprintf(f, sizeof f, finals[i], nonce, strstr(fin, ",p=") + 3);
        raw_continue_send(fd, f);
        uint8_t r[64];
        size_t n;
        CHECK(raw_reply(fd, r, sizeof r, &n) == TSDB_ERR_AUTH);
        CHECK(closed(fd));
        close(fd);
    }
    /* AUTH_CONTINUE first */
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0);
    raw_continue_send(fd, "c=biws,r=abc,p=AAAA");
    uint8_t r[64];
    size_t n;
    CHECK(raw_reply(fd, r, sizeof r, &n) == TSDB_ERR_AUTH && closed(fd));
    close(fd);
    /* the right proof over raw frames works (the helpers are not what fails above) */
    fd = raw_connect();
    CHECK(raw_login(fd, "root", "rootpw") == 0);
    close(fd);
    printf("  malformed client-first and client-final messages, an unknown mechanism: AUTH, closed\n");
}

static void test_prelogin(void)
{
    uint8_t r[256];
    size_t n;
    /* any other op before login: AUTH, closed */
    int fd = raw_connect();
    raw_send(fd, 9, TSDB_OP_STATS, NULL, 0);
    CHECK(raw_reply(fd, r, sizeof r, &n) == TSDB_ERR_AUTH && n == 0);
    CHECK(closed(fd));
    close(fd);
    fd = raw_connect();
    CHECK(raw_hello(fd) == 0);
    tsdb_sid id = sx;
    raw_send(fd, 9, TSDB_OP_SERIES_NAME, &id, 8);
    CHECK(raw_reply(fd, r, sizeof r, &n) == TSDB_ERR_AUTH && closed(fd));
    close(fd);
    /* a frame over TSDB_PRELOGIN_MAX: closed at once, unanswered */
    fd = raw_connect();
    static uint8_t big[TSDB_PRELOGIN_MAX + 64];
    raw_send(fd, 9, TSDB_OP_HELLO, big, sizeof big);
    CHECK(closed(fd));
    close(fd);
    /* CANCEL before login closes */
    fd = raw_connect();
    uint32_t cancel_id = 1;
    raw_send(fd, 9, TSDB_OP_CANCEL, &cancel_id, 4);
    CHECK(closed(fd));
    close(fd);
    /* three frames in one write before login (review 2 #6): two are queued (A5), the third waits in the buffer and
     * is read once one of them is answered, though no byte follows. Three HELLOs: three replies. HELLO, AUTH_START,
     * QUERY_RAW: the query hears AUTH and the connection closes, at once, not at the login timeout (10 s) */
    uint32_t ver = TSDB_PROTOCOL_VERSION;
    uint8_t w[512];
    const char *cf = "n,,n=root,r=abcdefghijkl";
    uint8_t st[128];
    size_t ml = strlen(TSDB_SCRAM_SHA256), cl = strlen(cf);
    uint16_t l16 = (uint16_t)cl;
    st[0] = (uint8_t)ml;
    memcpy(st + 1, TSDB_SCRAM_SHA256, ml);
    memcpy(st + 1 + ml, &l16, 2);
    memcpy(st + 3 + ml, cf, cl);
    uint8_t q[24] = {0};
    memcpy(q, &id, 8);
    struct {
        uint8_t op;
        const void *p;
        uint32_t len;
    } three[2][3] = {{{TSDB_OP_HELLO, &ver, 4}, {TSDB_OP_HELLO, &ver, 4}, {TSDB_OP_HELLO, &ver, 4}},
                     {{TSDB_OP_HELLO, &ver, 4}, {TSDB_OP_AUTH_START, st, (uint32_t)(3 + ml + cl)},
                      {TSDB_OP_QUERY_RAW, q, 24}}};
    for (int k = 0; k < 2; k++) {
        size_t at = 0;
        for (uint32_t i = 0; i < 3; i++) {
            uint32_t fl = three[k][i].len + 5, fid = i + 1;
            memcpy(w + at, &fl, 4);
            memcpy(w + at + 4, &fid, 4);
            w[at + 8] = three[k][i].op;
            memcpy(w + at + 9, three[k][i].p, three[k][i].len);
            at += 9 + three[k][i].len;
        }
        fd = raw_connect();
        struct timeval tv = {3, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        double t0 = now_s();
        CHECK(write(fd, w, at) == (ssize_t)at);
        for (uint32_t i = 1; i <= 3; i++) {
            uint32_t rid = 0;
            int32_t s = raw_frame(fd, &rid, r, sizeof r, &n, NULL, NULL);
            CHECK(rid == i && s == (k == 1 && i == 3 ? TSDB_ERR_AUTH : 0));
        }
        if (k == 1)
            CHECK(closed(fd));
        CHECK(now_s() - t0 < 2.5);
        close(fd);
    }
    /* after login: AUTH_START and AUTH_CONTINUE are INVALID, and the connection goes on */
    fd = raw_connect();
    CHECK(raw_login(fd, "root", "rootpw") == 0);
    CHECK(raw_start(fd, "n,,n=root,r=abc", NULL, 0) == TSDB_ERR_INVALID);
    raw_continue_send(fd, "c=biws,r=abc,p=AAAA");
    CHECK(raw_reply(fd, r, sizeof r, &n) == TSDB_ERR_INVALID);
    raw_send(fd, 9, TSDB_OP_SERIES_NAME, &id, 8);
    CHECK(raw_reply(fd, r, sizeof r, &n) == 0);
    close(fd);
    printf("  before login: other ops, a long frame and CANCEL close; three pipelined frames all answered (the third "
           "past the window: AUTH, closed); AUTH_* after login is INVALID\n");
}

/* --login-timeout: a connection that has not logged in by then is closed; a logged-in one is not. */
static void test_login_timeout(const char *dir)
{
    const char *extra[] = {"--login-timeout", "1", NULL};
    pid_t srv = start(dir, extra);
    int fd = raw_connect();
    CHECK(raw_hello(fd) == 0);
    tsdbc *c = login_as("root", "rootpw");
    double t = now_s();
    CHECK(closed(fd));
    double took = now_s() - t;
    close(fd);
    CHECK(took >= 0.5 && took < 10);
    nap_ms(1500);
    tsdbc_user *me = NULL;
    OK(tsdbc_whoami(c, &me));
    tsdbc_users_free(me, 1);
    tsdbc_close(c);
    stop(srv);
    printf("  --login-timeout 1: closed after %.2f s; a logged-in connection stays\n", took);
}

/* Review 2 #1: a tenant admin gives a user it made 64 globs of A24's longest, which pass the literal prefix and
 * suffix filters, and that user sends SERIES requests with the longest names, over 16 connections (tsdbd runs 2
 * workers here). Names over the engine's limit are INVALID before any matching; the others cost one linear pass
 * per glob, outside any lock: root's WHOAMI on its own connection is answered within a small bound (the
 * backtracking matcher under the user's mutex held it for seconds: 8.8 s in the review). */
static void test_glob_stall(void)
{
    tsdbc *adm = login_as("acme/admin", "adminpw");
    OK(tsdbc_user_create(adm, "acme/g", "gpw", 0, 0));
    for (int i = 0; i < USER_GLOBS; i++) {
        char g[300];
        int n = snprintf(g, sizeof g, "acme/*");
        memset(g + n, 'a', 240);
        snprintf(g + n + 240, sizeof g - (size_t)n - 240, "c%02d*b", i);
        OK(tsdbc_grant(adm, "acme/g", TSDB_SEL_GLOB, g, TSDB_RIGHT_READ));
    }
    tsdbc_close(adm);
    int r = raw_connect();
    CHECK(raw_login(r, "root", "rootpw") == 0);
    enum { CONNS = 16, LONG = 65535, MAX = 32767 };
    static uint8_t p[3 + LONG];
    uint16_t l = LONG;
    memcpy(p + 1, &l, 2);
    memcpy(p + 3, "acme/", 5);
    memset(p + 8, 'a', LONG - 6);
    p[3 + LONG - 1] = 'b';
    int g[CONNS];
    for (int k = 0; k < CONNS; k++) {
        g[k] = raw_connect();
        CHECK(raw_login(g[k], "acme/g", "gpw") == 0);
    }
    double t0 = now_s();
    for (int k = 0; k < CONNS; k++) { /* half over the engine's limit, half at it (ending in 'b' as well) */
        if (k == CONNS / 2) {
            l = MAX;
            memcpy(p + 1, &l, 2);
            p[3 + MAX - 1] = 'b';
        }
        raw_send(g[k], 100, TSDB_OP_SERIES, p, 3u + l);
    }
    nap_ms(20);
    uint8_t b[512];
    size_t n;
    double w0 = now_s();
    raw_send(r, 9, TSDB_OP_WHOAMI, NULL, 0);
    CHECK(raw_reply(r, b, sizeof b, &n) == 0);
    double whoami_ms = (now_s() - w0) * 1e3;
    for (int k = 0; k < CONNS; k++) {
        CHECK(raw_reply(g[k], b, sizeof b, &n) == (k < CONNS / 2 ? TSDB_ERR_INVALID : TSDB_ERR_DENIED));
        close(g[k]);
    }
    double all_ms = (now_s() - t0) * 1e3;
    CHECK(whoami_ms < 1000);
    close(r);
    printf("  64 long globs, 16 SERIES of the longest names: root's WHOAMI meanwhile %.1f ms, all answered in %.0f ms\n",
           whoami_ms, all_ms);
}

/* ------------------------------------------------------------ server/auth.c in this process (review 2) */

static size_t start_body(uint8_t *p, const char *cf)
{
    size_t ml = strlen(TSDB_SCRAM_SHA256), cl = strlen(cf);
    uint16_t l = (uint16_t)cl;
    p[0] = (uint8_t)ml;
    memcpy(p + 1, TSDB_SCRAM_SHA256, ml);
    memcpy(p + 1 + ml, &l, 2);
    memcpy(p + 3 + ml, cf, cl);
    return 3 + ml + cl;
}

static size_t continue_body(uint8_t *q, const char *fin)
{
    size_t fl = strlen(fin);
    uint16_t l = (uint16_t)fl;
    memcpy(q, &l, 2);
    memcpy(q + 2, fin, fl);
    return 2 + fl;
}

/* A login through auth_scram_start and _continue, with password pw: 0 or the status; *hs and *hc (when not NULL)
 * the HMACs each step computed. */
static int inproc_login(auth_conn *a, const char *user, const char *pw, unsigned *hs, unsigned *hc)
{
    uint8_t p[512], q[800], out[800];
    size_t ol;
    char cf[300], sf[700], fin[800], v[64];
    snprintf(cf, sizeof cf, "n,,n=%s,r=testnonce", user);
    auth_conn_init(a);
    unsigned h0 = atomic_load(&auth_test_hmacs);
    int rc = auth_scram_start(a, p, start_body(p, cf), out, sizeof out, &ol, "test");
    unsigned h1 = atomic_load(&auth_test_hmacs);
    if (hs)
        *hs = h1 - h0;
    if (rc != 0)
        return rc;
    uint16_t l;
    memcpy(&l, out, 2);
    memcpy(sf, out + 2, l);
    sf[l] = 0;
    client_final(user, pw, "testnonce", sf, fin, sizeof fin, v, sizeof v);
    rc = auth_scram_continue(a, q, continue_body(q, fin), out, sizeof out, &ol, "test");
    if (hc)
        *hc = atomic_load(&auth_test_hmacs) - h1;
    return rc;
}

static pthread_mutex_t hang_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t hang_cv = PTHREAD_COND_INITIALIZER;
static bool hang_in, hang_go;

/* auth_reload's hook: blocks as an open of USERS on a hard NFS mount whose server is down does */
static void hang_hook(void)
{
    pthread_mutex_lock(&hang_mu);
    hang_in = true;
    pthread_cond_broadcast(&hang_cv);
    while (!hang_go)
        pthread_cond_wait(&hang_cv, &hang_mu);
    pthread_mutex_unlock(&hang_mu);
}

static void *reload_thread(void *arg)
{
    (void)arg;
    auth_reload();
    return NULL;
}

typedef struct during {
    auth_conn *in; /* logged in before the hang */
    tsdb_sid own, foreign;
    int login, fresh, name, own_id, foreign_id;
} during;

static void *during_hang(void *arg)
{
    during *d = arg;
    auth_conn a;
    d->login = inproc_login(&a, "acme/r", "pw", NULL, NULL);
    auth_conn_free(&a);
    uint8_t s[16] = {0};
    uint16_t l = 7;
    memcpy(s + 1, &l, 2);
    memcpy(s + 3, "acme/s0", 7);
    d->fresh = auth_fresh(d->in);
    d->name = auth_check(d->in, TSDB_OP_SERIES, s, 10);
    d->own_id = auth_id(d->in, d->own, R_READ);
    d->foreign_id = auth_id(d->in, d->foreign, R_READ);
    return NULL;
}

/* Series i of test_inproc is acme's: even i in even 4096-id blocks, odd i in odd ones, so one offset in two
 * neighbouring leaves of the rights map belongs to two tenants (a leaf taken for another catches the eye). */
static bool acme_id(uint32_t i) { return (i + i / 4096) % 2 == 0; }

/* server/auth.c linked in, on a database of 100,000 series (acme/sN or globex/sN by acme_id):
 *  - #4: a known and an unknown name compute the same HMACs at AUTH_START and at a failed AUTH_CONTINUE;
 *  - #5: a user with no grant probing every id allocates no rights map; with the budget of all maps cut to 32 KiB,
 *    a user's answers on every id stay right while its map stops growing;
 *  - #2: while a reload is blocked inside its read of USERS, a new login and a logged-in user's checks answer. */
static void test_inproc(const char *base)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s-inproc", base);
    mkdir(dir, 0755);
    users u;
    OK(users_open(&u, dir, true));
    user_verifier v = {.iterations = ITER_MIN, .salt_len = 16};
    memcpy(v.salt, "0123456789abcdef", 16);
    tsdb_scram_keys("pw", 2, v.salt, 16, ITER_MIN, NULL, v.stored_key, v.server_key);
    OK(users_create(&u, 0, "root", USER_SUPERUSER, &v, NULL));
    OK(users_create(&u, 0, "acme/r", 0, &v, NULL));
    OK(users_grant(&u, 0, "acme/r", SEL_PREFIX, "acme/", 5, R_READ));
    OK(users_create(&u, 0, "nogrant", 0, &v, NULL));
    users_close(&u);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *db = NULL;
    OK(tsdb_open(dir, &o, &db));
    enum { N = 100000 };
    static tsdb_sid ids[N];
    for (uint32_t i = 0; i < N; i++) {
        char nm[32];
        snprintf(nm, sizeof nm, "%s/s%u", acme_id(i) ? "acme" : "globex", i);
        OK(tsdb_series_create(db, nm, TSDB_FLOAT32, NULL, &ids[i]));
    }
    OK(auth_init(dir, true, true, db));

    /* #4 */
    unsigned hs[2], hc[2];
    const char *who[2] = {"acme/r", "nosuchuser"};
    for (int k = 0; k < 2; k++) {
        auth_conn a;
        CHECK(inproc_login(&a, who[k], "wrongpw", &hs[k], &hc[k]) == TSDB_ERR_AUTH);
        auth_conn_free(&a);
    }
    CHECK(hs[0] == 3 && hs[1] == 3 && hc[0] == hc[1] && hc[0] == 2);

    /* #5 */
    auth_conn ng, r;
    CHECK(inproc_login(&ng, "nogrant", "pw", NULL, NULL) == 0);
    int wrong = 0;
    for (uint32_t i = 0; i < N; i++)
        wrong += auth_id(&ng, ids[i], R_READ) != TSDB_ERR_DENIED;
    CHECK(wrong == 0 && auth_ids(&ng, ids, N, R_READ) == TSDB_ERR_DENIED);
    size_t probed = auth_map_total();
    CHECK(probed == 0);
    size_t budget = auth_map_budget;
    auth_map_budget = 32 << 10;
    CHECK(inproc_login(&r, "acme/r", "pw", NULL, NULL) == 0);
    for (int pass = 0; pass < 2; pass++)
        for (uint32_t i = 0; i < N; i++)
            wrong += auth_id(&r, ids[i], R_READ) != (acme_id(i) ? 0 : TSDB_ERR_DENIED);
    static tsdb_sid own[N];
    uint32_t nown = 0;
    for (uint32_t i = 0; i < N; i++)
        if (acme_id(i))
            own[nown++] = ids[i];
    /* a frame of acme's ids across the blocks (the first four cached, the rest past the budget), then each foreign
     * id after an own one of the block before it */
    CHECK(wrong == 0 && auth_ids(&r, own, nown, R_READ) == 0 && auth_ids(&r, ids, N, R_READ) == TSDB_ERR_DENIED);
    for (uint32_t b = 1; b < N / 4096; b++) {
        uint32_t i = b * 4096 + (b * 37) % 4000 + 1, j = i - 4096; /* one offset, neither end of a block */
        tsdb_sid pair[2] = {ids[j + !acme_id(j)], ids[i + acme_id(i)]};
        wrong += auth_ids(&r, pair, 2, R_READ) != TSDB_ERR_DENIED || auth_ids(&r, pair, 1, R_READ) != 0;
    }
    CHECK(wrong == 0);
    size_t capped = auth_map_total();
    CHECK(capped > 0 && capped <= (32 << 10) + 4096);
    auth_map_budget = budget;

    /* #2 */
    during d = {.in = &r, .own = own[0], .foreign = ids[1], .login = -1};
    auth_test_reload_hook = hang_hook;
    pthread_t rt, dt;
    CHECK(pthread_create(&rt, NULL, reload_thread, NULL) == 0);
    pthread_mutex_lock(&hang_mu);
    while (!hang_in)
        pthread_cond_wait(&hang_cv, &hang_mu);
    pthread_mutex_unlock(&hang_mu);
    CHECK(pthread_create(&dt, NULL, during_hang, &d) == 0);
    struct timespec until;
    clock_gettime(CLOCK_REALTIME, &until);
    until.tv_sec += 5;
    bool answered = pthread_timedjoin_np(dt, NULL, &until) == 0;
    CHECK(answered);
    pthread_mutex_lock(&hang_mu);
    hang_go = true;
    pthread_cond_broadcast(&hang_cv);
    pthread_mutex_unlock(&hang_mu);
    if (!answered)
        pthread_join(dt, NULL);
    pthread_join(rt, NULL);
    auth_test_reload_hook = NULL;
    CHECK(d.login == 0 && d.fresh == 0 && d.name == 0 && d.own_id == 0 && d.foreign_id == TSDB_ERR_DENIED);

    auth_conn_free(&ng);
    auth_conn_free(&r);
    auth_shutdown();
    tsdb_close(db);
    printf("  in process: the same HMACs for a known and an unknown name (%u, %u); %d ids probed without a grant: "
           "map %zu bytes; past a 32 KiB budget (%zu bytes) every answer right; a login and checks answer while a "
           "reload hangs in USERS\n", hs[0], hc[0], N, probed, capped);
}

/* Grants and revokes over the wire, an offline CLI edit and a drop reach a connected user's next request. */
static void test_changes(const char *dir)
{
    tsdbc *root = login_as("root", "rootpw"), *in = login_as("acme/ingest", "ingestpw");
    CHECK(query(in, sx) == TSDB_ERR_DENIED);
    OK(tsdbc_grant(root, "acme/ingest", TSDB_SEL_PREFIX, "acme/", TSDB_RIGHT_READ));
    CHECK(query(in, sx) == 0); /* the next request */
    CHECK(query(in, sy) == 0);
    CHECK(query(in, sz) == TSDB_ERR_DENIED);
    OK(tsdbc_revoke(root, "acme/ingest", TSDB_SEL_PREFIX, "acme/", TSDB_RIGHT_READ));
    CHECK(query(in, sx) == TSDB_ERR_DENIED);
    CHECK(query(in, sy) == TSDB_ERR_DENIED);

    /* the CLI appends to USERS; the reload tick (1 s) brings it to the connection */
    CHECK(cli_user(dir, NULL, "grant acme/ingest read name:acme/y") == 0);
    double t = now_s(), took = -1;
    while (now_s() - t < 10) {
        if (query(in, sy) == 0) {
            took = now_s() - t;
            break;
        }
        nap_ms(20);
    }
    CHECK(took >= 0 && took < 3);
    CHECK(query(in, sx) == TSDB_ERR_DENIED);

    /* dropped while connected: AUTH on the next request, then closed */
    OK(tsdbc_user_drop(root, "acme/ingest"));
    CHECK(query(in, sy) == TSDB_ERR_AUTH);
    CHECK(query(in, sy) == TSDB_ERR_IO);
    tsdbc_close(in);
    tsdbc *c = NULL;
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "acme/ingest", "ingestpw", &c) == TSDB_ERR_AUTH);
    /* made again over the wire (a new uid: the old grants are gone) */
    OK(tsdbc_user_create(root, "acme/ingest", "newpw", 0, 0));
    in = login_as("acme/ingest", "newpw");
    CHECK(query(in, sy) == TSDB_ERR_DENIED);
    tsdbc_close(in);
    tsdbc_close(root);
    printf("  grant, revoke and drop seen by the next request; a CLI grant after %.0f ms\n", took * 1000);
}

/* A follower reads USERS too: logins work, changes are refused there, the writer's directory edits reach it. */
static void test_follower(const char *dir)
{
    int wport = port;
    const char *extra[] = {"--follow", NULL};
    pid_t f = start(dir, extra);
    tsdbc *root = login_as("root", "rootpw"), *in = login_as("acme/ingest", "newpw");
    int rc = tsdbc_user_create(root, "acme/other", "pw", 0, 0);
    CHECK(rc == TSDB_ERR_READONLY);
    CHECK(query(in, sx) == TSDB_ERR_DENIED);
    CHECK(cli_user(dir, NULL, "grant acme/ingest read name:acme/x") == 0);
    double t = now_s(), took = -1;
    while (now_s() - t < 10) {
        if (query(in, sx) == 0) {
            took = now_s() - t;
            break;
        }
        nap_ms(20);
    }
    CHECK(took >= 0 && took < 3);
    CHECK(query(in, sz) == TSDB_ERR_DENIED);
    CHECK(cli_user(dir, NULL, "drop acme/ingest") == 0);
    t = now_s();
    while (now_s() - t < 10 && query(in, sx) == 0)
        nap_ms(20);
    CHECK(query(in, sx) == TSDB_ERR_IO); /* answered AUTH once, then closed */
    tsdbc_close(in);
    tsdbc_close(root);
    stop(f);
    port = wport;
    CHECK(cli_user(dir, "ingestpw", "add acme/ingest") == 0);
    printf("  follower: logins, admin ops READONLY, the CLI's grant after %.0f ms, its drop\n", took * 1000);
}

/* A29: USERS losing its last superuser while tsdbd runs: it keeps serving with one log line; a restart refuses. */
static void test_no_superuser_left(const char *dir, pid_t *srv)
{
    tsdbc *in = login_as("acme/ingest", "ingestpw");
    CHECK(cli_user(dir, NULL, "drop root") == 0);
    const char *line = "USERS has no superuser any more";
    double t = now_s();
    while (now_s() - t < 10 && log_count(line) == 0)
        nap_ms(50);
    CHECK(log_count(line) == 1);
    tsdbc_user *me = NULL;
    OK(tsdbc_whoami(in, &me)); /* still served */
    tsdbc_users_free(me, 1);
    tsdbc *a = login_as("acme/admin", "adminpw"); /* and new logins too */
    tsdbc_close(a);
    nap_ms(2500); /* more ticks: the line is not repeated */
    CHECK(log_count(line) == 1);
    tsdbc_close(in);
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "root", "rootpw", &a) == TSDB_ERR_AUTH);
    stop(*srv);
    *srv = 0;
    int before = log_count("no superuser: run");
    CHECK(start_fails(dir, NULL) == 1);
    CHECK(log_count("no superuser: run") == before + 1);
    printf("  the last superuser dropped while running: served on, one log line; the restart exits 1\n");
}

/* A13 and A28: no superuser refuses the start; --no-auth does not read USERS and refuses the user admin ops. */
static void test_no_auth(const char *base)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s-noauth", base);
    mkdir(dir, 0755);
    CHECK(start_fails(dir, NULL) == 1); /* no USERS, no superuser */
    char path[PATH_MAX + 48];
    snprintf(path, sizeof path, "%s/USERS", dir);
    FILE *f = fopen(path, "w"); /* damage it does not read */
    fputs("not a USERS file", f);
    fclose(f);
    const char *extra[] = {"--no-auth", NULL};
    pid_t srv = start(dir, extra);
    tsdbc *c = NULL;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    tsdbc_user *me = NULL;
    OK(tsdbc_whoami(c, &me));
    CHECK(me->name[0] == 0 && (me->flags & TSDB_USER_SUPERUSER));
    tsdbc_users_free(me, 1);
    CHECK(tsdbc_user_create(c, "x", "pw", 0, 0) == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_drop(c, "x") == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_alter(c, "x", TSDB_USER_SUPERUSER) == TSDB_ERR_DENIED);
    CHECK(tsdbc_user_password(c, "x", "pw", 0) == TSDB_ERR_DENIED);
    CHECK(tsdbc_grant(c, "x", TSDB_SEL_ALL, "", TSDB_RIGHT_READ) == TSDB_ERR_DENIED);
    CHECK(tsdbc_revoke(c, "x", TSDB_SEL_ALL, "", TSDB_RIGHT_READ) == TSDB_ERR_DENIED);
    tsdbc_user *list = NULL;
    size_t n = 0;
    CHECK(tsdbc_user_list(c, &list, &n) == TSDB_ERR_DENIED);
    tsdb_sid id;
    OK(tsdbc_series(c, "a", 1, &id));
    int64_t ts = T0;
    float v = 2;
    OK(tsdbc_insert_series(c, id, &ts, &v, 1));
    size_t rows = 0;
    OK(tsdbc_query_raw(c, id, T0, T0 + 1, count_fn, &rows));
    CHECK(rows == 1);
    tsdbc_close(c);
    /* credentials to a server that does not ask: used without them, unless the client requires a login */
    OK(tsdbc_connect_auth("127.0.0.1", port, "x", "pw", &c));
    tsdbc_close(c);
    tsdbc_options o = {.user = "x", .password = "pw", .require_auth = 1};
    c = NULL;
    CHECK(tsdbc_connect_opts("127.0.0.1", port, &o, &c) == TSDB_ERR_AUTH && !c);
    stop(srv);
    printf("  no superuser refuses the start; --no-auth: USERS unread, admin ops DENIED, data ops work\n");
}

/* ------------------------------------------------------------ RFC 7677 §3, both sides */

struct fake {
    int lfd, port;
    pthread_t th;
    char first[256], final[256]; /* what the client sent */
};

static bool fake_read(int fd, uint32_t *id, uint8_t *op, uint8_t *body, size_t cap, size_t *n)
{
    uint32_t len;
    if (!read_full(fd, &len, 4) || len < 5 || len - 5 > cap || !read_full(fd, id, 4) || !read_full(fd, op, 1))
        return false;
    *n = len - 5;
    return read_full(fd, body, *n);
}

static void fake_reply(int fd, uint32_t id, const void *body, size_t n)
{
    uint8_t f[512];
    uint32_t len = (uint32_t)(9 + n);
    int32_t st = 0;
    memcpy(f, &len, 4);
    memcpy(f + 4, &id, 4);
    f[8] = TSDB_OP_REPLY;
    memcpy(f + 9, &st, 4);
    memcpy(f + 13, body, n);
    CHECK(write(fd, f, 13 + n) == (ssize_t)(13 + n));
}

static void fake_msg(int fd, uint32_t id, const char *m)
{
    uint8_t b[300];
    uint16_t l = (uint16_t)strlen(m);
    memcpy(b, &l, 2);
    memcpy(b + 2, m, l);
    fake_reply(fd, id, b, 2u + l);
}

/* The server's side of the RFC's exchange, from the RFC's bytes: it answers what the RFC answers, whatever the
 * client sends, and keeps what the client sent for the test to compare. */
static void *fake_main(void *arg)
{
    struct fake *f = arg;
    int fd = accept(f->lfd, NULL, NULL);
    uint8_t body[512], op;
    uint32_t id;
    size_t n;
    if (fd < 0)
        return NULL;
    if (fake_read(fd, &id, &op, body, sizeof body, &n) && op == TSDB_OP_HELLO) {
        uint8_t h[32];
        uint32_t v = TSDB_PROTOCOL_VERSION;
        uint16_t flags = TSDB_HELLO_AUTH, nm = 1;
        size_t ml = strlen(TSDB_SCRAM_SHA256);
        memcpy(h, &v, 4);
        memcpy(h + 4, &flags, 2);
        memcpy(h + 6, &nm, 2);
        h[8] = (uint8_t)ml;
        memcpy(h + 9, TSDB_SCRAM_SHA256, ml);
        fake_reply(fd, id, h, 9 + ml);
    }
    if (fake_read(fd, &id, &op, body, sizeof body, &n) && op == TSDB_OP_AUTH_START && n > 3 && n > 3u + body[0]) {
        size_t ml = body[0];
        snprintf(f->first, sizeof f->first, "%.*s", (int)(n - 3 - ml), (char *)body + 3 + ml);
        fake_msg(fd, id, RFC7677_SERVER_FIRST);
    }
    if (fake_read(fd, &id, &op, body, sizeof body, &n) && op == TSDB_OP_AUTH_CONTINUE && n > 2) {
        snprintf(f->final, sizeof f->final, "%.*s", (int)(n - 2), (char *)body + 2);
        fake_msg(fd, id, RFC7677_SERVER_FINAL);
    }
    while (fake_read(fd, &id, &op, body, sizeof body, &n)) /* until the client closes */
        ;
    close(fd);
    return NULL;
}

static void test_rfc7677_client(void)
{
    struct fake f = {0};
    struct sockaddr_in a = {.sin_family = AF_INET};
    socklen_t al = sizeof a;
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    f.lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (f.lfd < 0 || bind(f.lfd, (struct sockaddr *)&a, sizeof a) != 0 || listen(f.lfd, 1) != 0 ||
        getsockname(f.lfd, (struct sockaddr *)&a, &al) != 0) {
        perror("fake server");
        exit(1);
    }
    pthread_create(&f.th, NULL, fake_main, &f);
    tsdbc_test_nonce = RFC7677_CNONCE;
    tsdbc *c = NULL;
    int rc = tsdbc_connect_auth("127.0.0.1", ntohs(a.sin_port), RFC7677_USER, RFC7677_PASSWORD, &c);
    tsdbc_test_nonce = NULL;
    CHECK(rc == 0); /* the client checked the RFC's ServerSignature */
    if (c)
        tsdbc_close(c);
    pthread_join(f.th, NULL);
    close(f.lfd);
    CHECK(strcmp(f.first, RFC7677_CLIENT_FIRST) == 0);
    CHECK(strcmp(f.final, RFC7677_CLIENT_FINAL) == 0);
    printf("  RFC 7677 §3, the C client: client-first and client-final byte for byte\n");
}

/* tsdbd's server side in this process, with the RFC's verifier in USERS and the RFC's server nonce. */
static void test_rfc7677_server(const char *base)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s-rfc", base);
    mkdir(dir, 0755);
    users u;
    OK(users_open(&u, dir, true));
    user_verifier v = {.iterations = RFC7677_ITER};
    size_t sl = 0;
    CHECK(tsdb_base64_decode(RFC7677_SALT_B64, strlen(RFC7677_SALT_B64), v.salt, sizeof v.salt, &sl) == 0);
    v.salt_len = (uint8_t)sl;
    tsdb_scram_keys(RFC7677_PASSWORD, strlen(RFC7677_PASSWORD), v.salt, sl, v.iterations, NULL, v.stored_key,
                    v.server_key);
    uint32_t uid;
    OK(users_create(&u, 0, RFC7677_USER, USER_SUPERUSER, &v, &uid));
    users_close(&u);
    OK(auth_init(dir, true, false, NULL));
    auth_test_nonce = RFC7677_SNONCE;

    uint8_t p[256], out[512];
    size_t ml = strlen(TSDB_SCRAM_SHA256), cl = strlen(RFC7677_CLIENT_FIRST), ol = 0;
    uint16_t l = (uint16_t)cl;
    p[0] = (uint8_t)ml;
    memcpy(p + 1, TSDB_SCRAM_SHA256, ml);
    memcpy(p + 1 + ml, &l, 2);
    memcpy(p + 3 + ml, RFC7677_CLIENT_FIRST, cl);
    uint8_t q[256];
    size_t fl = strlen(RFC7677_CLIENT_FINAL);
    l = (uint16_t)fl;
    memcpy(q, &l, 2);
    memcpy(q + 2, RFC7677_CLIENT_FINAL, fl);

    auth_conn a;
    auth_conn_init(&a);
    CHECK(auth_scram_start(&a, p, 3 + ml + cl, out, sizeof out, &ol, "test") == 0);
    memcpy(&l, out, 2);
    CHECK(ol == 2u + l && l == strlen(RFC7677_SERVER_FIRST) && memcmp(out + 2, RFC7677_SERVER_FIRST, l) == 0);
    CHECK(auth_scram_continue(&a, q, 2 + fl, out, sizeof out, &ol, "test") == 0);
    memcpy(&l, out, 2);
    CHECK(ol == 2u + l && l == strlen(RFC7677_SERVER_FINAL) && memcmp(out + 2, RFC7677_SERVER_FINAL, l) == 0);
    CHECK(a.state == AS_IN && a.au);
    auth_conn_free(&a);

    /* one bit of the proof wrong: refused */
    auth_conn_init(&a);
    CHECK(auth_scram_start(&a, p, 3 + ml + cl, out, sizeof out, &ol, "test") == 0);
    q[2 + fl - 3] ^= 1;
    CHECK(auth_scram_continue(&a, q, 2 + fl, out, sizeof out, &ol, "test") == TSDB_ERR_AUTH);
    CHECK(a.state == AS_FAILED && !a.au);
    auth_conn_free(&a);
    auth_test_nonce = NULL;
    auth_shutdown();
    printf("  RFC 7677 §3, tsdbd's server side: server-first and server-final byte for byte\n");
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    printf("test_auth (%s)\n", getenv("TSDBD_IO") ? getenv("TSDBD_IO") : "default io");
    char dir[PATH_MAX + 16];
    snprintf(dir, sizeof dir, "%s/tsdbd-auth-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    ah_setup(dir);

    test_rfc7677_client();
    test_rfc7677_server(dir);
    test_inproc(dir);
    test_no_auth(dir);

    int before = failures; /* a failure above does not stop the server tests: only the CLI's own */
    CHECK(cli_user(dir, "rootpw", "add root --superuser") == 0);
    CHECK(cli_user(dir, "adminpw", "add acme/admin") == 0);
    CHECK(cli_user(dir, NULL, "grant acme/admin admin prefix:acme/") == 0);
    CHECK(cli_user(dir, "ingestpw", "add acme/ingest") == 0);
    if (failures > before) {
        fprintf(stderr, "test_auth: the CLI could not make the users (%s, log %s)\n", cli, clilog);
        return 1;
    }
    pid_t srv = start(dir, NULL);
    test_login();
    test_key_cache();
    test_password_rule();
    char mock[200], again[200], sf[600];
    test_mock_salt(mock, sizeof mock);
    test_malformed();
    test_prelogin();
    test_glob_stall();
    stop(srv);
    srv = start(dir, NULL); /* A4: the mock salt survives a restart */
    int fd = raw_connect();
    CHECK(raw_hello(fd) == 0 && raw_start(fd, "n,,n=nobody,r=abc", sf, sizeof sf) == 0);
    close(fd);
    salt_iter(sf, again, sizeof again);
    CHECK(strcmp(mock, again) == 0);
    test_changes(dir);
    test_follower(dir);
    /* no secret in the server's log, though it logged the failed logins */
    CHECK(log_count("login failed") > 0);
    CHECK(log_count("rootpw") == 0 && log_count("ingestpw") == 0 && log_count("wrongpw") == 0 &&
          log_count("newpw") == 0);
    stop(srv);
    test_login_timeout(dir);
    srv = start(dir, NULL);
    test_no_superuser_left(dir, &srv);
    stop(srv);
    if (failures) {
        fprintf(stderr, "test_auth: %d failures (server log %s)\n", failures, srvlog);
        return 1;
    }
    char cmd[8 * PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s-noauth' '%s-rfc' '%s-inproc' '%s' '%s'", dir, dir, dir, dir, srvlog,
             clilog);
    if (system(cmd) != 0)
        fprintf(stderr, "test_auth: could not remove %s\n", dir);
    printf("test_auth: ok\n");
    return 0;
}
