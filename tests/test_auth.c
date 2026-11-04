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

#include <pthread.h>

extern const char *tsdbc_test_nonce; /* client/tsdb_client.c's test hook (not in the header) */

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
    /* after login: AUTH_START and AUTH_CONTINUE are INVALID, and the connection goes on */
    fd = raw_connect();
    CHECK(raw_login(fd, "root", "rootpw") == 0);
    CHECK(raw_start(fd, "n,,n=root,r=abc", NULL, 0) == TSDB_ERR_INVALID);
    raw_continue_send(fd, "c=biws,r=abc,p=AAAA");
    CHECK(raw_reply(fd, r, sizeof r, &n) == TSDB_ERR_INVALID);
    raw_send(fd, 9, TSDB_OP_SERIES_NAME, &id, 8);
    CHECK(raw_reply(fd, r, sizeof r, &n) == 0);
    close(fd);
    printf("  before login: other ops, a long frame and CANCEL close; AUTH_* after login is INVALID\n");
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
    test_no_auth(dir);

    CHECK(cli_user(dir, "rootpw", "add root --superuser") == 0);
    CHECK(cli_user(dir, "adminpw", "add acme/admin") == 0);
    CHECK(cli_user(dir, NULL, "grant acme/admin admin prefix:acme/") == 0);
    CHECK(cli_user(dir, "ingestpw", "add acme/ingest") == 0);
    if (failures) {
        fprintf(stderr, "test_auth: the CLI could not make the users (%s, log %s)\n", cli, clilog);
        return 1;
    }
    pid_t srv = start(dir, NULL);
    test_login();
    char mock[200], again[200], sf[600];
    test_mock_salt(mock, sizeof mock);
    test_malformed();
    test_prelogin();
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
    char cmd[6 * PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s-noauth' '%s-rfc' '%s' '%s'", dir, dir, dir, srvlog, clilog);
    if (system(cmd) != 0)
        fprintf(stderr, "test_auth: could not remove %s\n", dir);
    printf("test_auth: ok\n");
    return 0;
}
