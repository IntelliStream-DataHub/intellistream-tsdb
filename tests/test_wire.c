/* Protocol 13 (docs/ha.md 9, 15.4): the HA bodies and what the C client does with them.
 *
 *   codec     every body of include/tsdb_wire.h: encode, decode, every truncation and every one-byte mutation (no read
 *             outside the range, nothing accepted that the layout does not allow); the bytes equal the vectors in
 *             tests/data/wire13-vectors.txt, which the Rust, Python and Java tests check against their own coding
 *             (`test_wire --emit` prints them).
 *   unavail   a fake server answers UNAVAILABLE (retry_after 300 ms) twice, then succeeds: the client waited the
 *             retry_after each time and sent the same request again; past the time budget the status and the body
 *             are returned; with the budget 0 there is no retry; a server without the body (protocol 12) is
 *             retried after a second.
 *   moved     a fake server answers MOVED naming another fake server: the client goes there and sends it again;
 *             without an address and without other nodes the status and the body are returned.
 *   partial   PARTIAL and DROPPED come back with their bodies and are not sent again.
 *   pipeline  five pipelined inserts, the third refused once: the server sees 0 1 2 3 4 and then 2 3 4 again, in order.
 *   nodes     a connect string with a node that refuses connections and a good one.
 *   topology  TOPOLOGY decoded; a body cut short is TSDB_ERR_CORRUPT.
 *
 * Nothing here starts tsdbd (tests/test_unavailable.c does: the outage shim gives the real UNAVAILABLE). The fake
 * servers listen on port 0 and hand out the port they got. */
#define _GNU_SOURCE
#include "tsdb_client.h"
#include "tsdb_protocol.h"
#include "tsdb_wire.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static double now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec * 1e3 + (double)t.tv_nsec / 1e6;
}

/* ------------------------------------------------------------ the vectors */

/* The bodies the vectors name; the same values are built by the other languages' tests from the same file. */
static const tsdb_unavail V_UN[2] = {
    {.vol = 3, .reason = TSDB_WHY_NFS_DOWN, .flags = 0, .epoch = 7, .since_ms = 1700000000123LL,
     .retry_after_ms = 300, .outage_id = 0x1122334455667788ULL},
    {.vol = TSDB_VOL_ANY, .reason = TSDB_WHY_RECOVERING, .flags = 1, .epoch = 0, .since_ms = -5,
     .retry_after_ms = 60000, .outage_id = 0},
};
static const tsdb_moved V_MV[2] = {
    {.vol = 1, .epoch = 9, .flags = TSDB_MOVED_HANDOFF, .node_id = 4, .addr = "db2.example:7070", .addr_len = 16},
    {.vol = 2, .epoch = 10, .flags = 0, .node_id = 0, .addr = "", .addr_len = 0},
};
static const tsdb_partial_vol V_PV[2] = {{.vol = 1, .status = 0}, {.vol = 3, .status = TSDB_ERR_UNAVAILABLE}};
static const uint8_t V_BITS[2] = {0x24, 0x01}; /* 10 rows: rows 2, 5 and 8 not applied */

struct vec {
    const char *name;
    uint8_t buf[512];
    size_t len;
};
static struct vec vecs[16];
static int nvecs;

static struct vec *vec_new(const char *name)
{
    struct vec *v = &vecs[nvecs++];
    v->name = name;
    return v;
}

static void build_vectors(void)
{
    struct vec *v;
    nvecs = 0;
    v = vec_new("unavailable");
    v->len = tsdb_wire_put_unavail(v->buf, sizeof v->buf, V_UN, 2);
    v = vec_new("unavailable-resend");
    v->len = tsdb_wire_put_unavail(v->buf, sizeof v->buf, V_UN, 1);
    tsdb_wire_wr64(v->buf + v->len, 0xA1B2C3D4E5F60718ULL);
    v->len += 8;
    v = vec_new("moved");
    v->len = tsdb_wire_put_moved(v->buf, sizeof v->buf, V_MV, 2);
    v = vec_new("partial");
    v->len = tsdb_wire_put_partial(v->buf, sizeof v->buf, V_PV, 2, 10, V_BITS, V_UN, 2);
    v = vec_new("dropped");
    v->len = tsdb_wire_put_dropped(v->buf, sizeof v->buf, 5, 1700000000999LL);
    v = vec_new("topology");
    size_t at = 0;
    tsdb_topo_node nd[2] = {{.node_id = 1, .alive = 1, .addr = "", .addr_len = 0},
                            {.node_id = 2, .alive = 0, .addr = "10.0.0.2:7070", .addr_len = 13}};
    at += tsdb_wire_put_topo_head(v->buf + at, sizeof v->buf - at, 0xDEADBEEFULL, 2);
    for (int i = 0; i < 2; i++)
        at += tsdb_wire_put_topo_node(v->buf + at, sizeof v->buf - at, &nd[i]);
    at += tsdb_wire_put_u32(v->buf + at, sizeof v->buf - at, 2);
    tsdb_topo_vol tv[2] = {
        {.vol = 0, .role = TSDB_ROLE_OWNER, .health = TSDB_HEALTH_AVAILABLE, .epoch = 1, .reason = 0, .domain = 0xffff,
         .since_ms = 0, .owner = 1, .server = 0, .addr = "", .addr_len = 0, .path = "/srv/a", .path_len = 6},
        {.vol = 1, .role = TSDB_ROLE_FOLLOWER, .health = TSDB_HEALTH_UNAVAILABLE, .epoch = 2, .reason = TSDB_WHY_NFS_DOWN,
         .domain = 1, .since_ms = 123456789, .owner = 2, .server = 3, .addr = "10.0.0.2:7070", .addr_len = 13,
         .path = "", .path_len = 0}};
    for (int i = 0; i < 2; i++)
        at += tsdb_wire_put_topo_vol(v->buf + at, sizeof v->buf - at, &tv[i]);
    v->len = at;
}

static void emit(void)
{
    build_vectors();
    for (int i = 0; i < nvecs; i++) {
        printf("%s ", vecs[i].name);
        for (size_t k = 0; k < vecs[i].len; k++)
            printf("%02x", vecs[i].buf[k]);
        printf("\n");
    }
}

static int hexv(int c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; }

static void check_vectors_file(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "cannot read %s: run `test_wire --emit > %s`\n", path, path);
        failures++;
        return;
    }
    char line[2048];
    int seen = 0;
    while (fgets(line, sizeof line, f)) {
        char *sp = strchr(line, ' ');
        if (!sp)
            continue;
        *sp = 0;
        char *h = sp + 1;
        size_t hl = strcspn(h, "\r\n");
        int found = 0;
        for (int i = 0; i < nvecs; i++) {
            if (strcmp(vecs[i].name, line))
                continue;
            found = 1;
            seen++;
            CHECK(hl == vecs[i].len * 2);
            for (size_t k = 0; k < vecs[i].len && k * 2 + 1 < hl; k++)
                if (hexv(h[k * 2]) * 16 + hexv(h[k * 2 + 1]) != vecs[i].buf[k]) {
                    fprintf(stderr, "vector %s differs at byte %zu\n", line, k);
                    failures++;
                    break;
                }
        }
        CHECK(found);
    }
    CHECK(seen == nvecs);
    fclose(f);
}

/* ------------------------------------------------------------ codec */

static void test_codec(void)
{
    build_vectors();
    tsdb_unavail e[4];
    tsdb_moved m[4];
    unsigned n;
    size_t used;

    struct vec *v = &vecs[0];
    CHECK(tsdb_wire_get_unavail(v->buf, v->len, e, 4, &n, &used) == 0 && n == 2 && used == v->len);
    CHECK(memcmp(&e[0], &V_UN[0], sizeof e[0]) == 0 || (e[0].vol == 3 && e[0].epoch == 7 && e[0].retry_after_ms == 300 &&
                                                         e[0].outage_id == 0x1122334455667788ULL));
    CHECK(e[1].vol == TSDB_VOL_ANY && e[1].reason == TSDB_WHY_RECOVERING && e[1].flags == 1 &&
          e[1].retry_after_ms == 60000 && e[1].since_ms == -5);
    CHECK(tsdb_wire_retry_after(e, 2) == TSDB_RETRY_MAX_MS || tsdb_wire_retry_after(e, 2) == 60000);
    CHECK(tsdb_wire_retry_after(e, 0) == TSDB_RETRY_MIN_MS);

    v = &vecs[1];
    CHECK(tsdb_wire_get_unavail(v->buf, v->len, e, 4, &n, &used) == 0 && n == 1 && v->len - used == 8);
    uint64_t rf = 0;
    CHECK(tsdb_wire_get_resend(v->buf + used, v->len - used, &rf) == 1 && rf == 0xA1B2C3D4E5F60718ULL);
    CHECK(tsdb_wire_get_resend(v->buf + used, 3, &rf) == TSDB_ERR_INVALID);

    v = &vecs[2];
    CHECK(tsdb_wire_get_moved(v->buf, v->len, m, 4, &n, &used) == 0 && n == 2 && used == v->len);
    CHECK(m[0].vol == 1 && m[0].epoch == 9 && m[0].flags == TSDB_MOVED_HANDOFF && m[0].node_id == 4 &&
          m[0].addr_len == 16 && memcmp(m[0].addr, "db2.example:7070", 16) == 0);
    CHECK(m[1].vol == 2 && m[1].addr_len == 0);

    v = &vecs[3];
    tsdb_partial_vol pv[4];
    unsigned nv, ne;
    uint32_t nrows;
    const uint8_t *bits;
    CHECK(tsdb_wire_get_partial(v->buf, v->len, pv, 4, &nv, &nrows, &bits, e, 4, &ne) == 0);
    CHECK(nv == 2 && ne == 2 && nrows == 10 && bits[0] == 0x24 && bits[1] == 0x01);
    CHECK(pv[1].vol == 3 && pv[1].status == TSDB_ERR_UNAVAILABLE);

    v = &vecs[4];
    uint16_t dv;
    int64_t db;
    CHECK(tsdb_wire_get_dropped(v->buf, v->len, &dv, &db) == 0 && dv == 5 && db == 1700000000999LL);
    CHECK(tsdb_wire_get_dropped(v->buf, v->len - 1, &dv, &db) == TSDB_ERR_INVALID);

    v = &vecs[5];
    tsdb_topo_it it;
    CHECK(tsdb_wire_topo_open(&it, v->buf, v->len) == 0 && it.gen == 0xDEADBEEFULL && it.nn == 2);
    tsdb_topo_node nd;
    int k = 0, r;
    while ((r = tsdb_wire_topo_node(&it, &nd)) == 1)
        k++;
    CHECK(r == 0 && k == 2 && it.nv == 2);
    tsdb_topo_vol tv;
    k = 0;
    while ((r = tsdb_wire_topo_vol(&it, &tv)) == 1) {
        if (k == 1)
            CHECK(tv.role == TSDB_ROLE_FOLLOWER && tv.health == TSDB_HEALTH_UNAVAILABLE && tv.addr_len == 13 &&
                  tv.path_len == 0 && tv.since_ms == 123456789);
        else
            CHECK(tv.path_len == 6 && memcmp(tv.path, "/srv/a", 6) == 0 && tv.domain == 0xffff);
        k++;
    }
    CHECK(r == 0 && k == 2);

    /* sizing call: the encoder writes nothing when the buffer is too small and returns the size */
    uint8_t one[1] = {0xAA};
    CHECK(tsdb_wire_put_unavail(one, 1, V_UN, 2) == 2 + 2 * TSDB_UNAVAIL_ENTRY && one[0] == 0xAA);
    /* an address no decoder accepts is not encoded */
    char big[TSDB_ADDR_MAX + 2];
    memset(big, 'a', sizeof big);
    tsdb_moved bm = {.vol = 1, .addr = big, .addr_len = TSDB_ADDR_MAX + 1};
    uint8_t buf[600];
    CHECK(tsdb_wire_put_moved(buf, sizeof buf, &bm, 1) == 0);

    /* every truncation of a body is refused or, where a prefix is itself a body, accepted only whole */
    for (int i = 0; i < 4; i++) {
        v = &vecs[i];
        for (size_t cut = 0; cut < v->len; cut++) {
            int rc = -1;
            if (i < 2)
                rc = tsdb_wire_get_unavail(v->buf, cut, e, 4, &n, &used);
            else if (i == 2)
                rc = tsdb_wire_get_moved(v->buf, cut, m, 4, &n, &used);
            else
                rc = tsdb_wire_get_partial(v->buf, cut, pv, 4, &nv, &nrows, &bits, e, 4, &ne);
            if (i == 1 && cut >= 2 + TSDB_UNAVAIL_ENTRY)
                continue; /* the entries alone, with the optional resend_from cut */
            CHECK(rc == TSDB_ERR_INVALID);
        }
    }
    for (size_t cut = 0; cut < vecs[5].len; cut++) {
        tsdb_topo_it t2;
        int ok = tsdb_wire_topo_open(&t2, vecs[5].buf, cut) == 0;
        while (ok && (r = tsdb_wire_topo_node(&t2, &nd)) == 1)
            ;
        if (ok && r < 0)
            ok = 0;
        while (ok && (r = tsdb_wire_topo_vol(&t2, &tv)) == 1)
            ;
        CHECK(!(ok && r == 0)); /* a cut body is never taken for a whole one */
    }

    /* one-byte mutations: whatever the decoders say, they stay inside the range (run under ASan: make asan) */
    unsigned long seed = 12345;
    for (int i = 0; i < 6; i++) {
        for (size_t at = 0; at < vecs[i].len; at++) {
            for (int rep = 0; rep < 8; rep++) {
                uint8_t *copy = malloc(vecs[i].len);
                memcpy(copy, vecs[i].buf, vecs[i].len);
                seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
                copy[at] ^= (uint8_t)(seed >> 33) | 1;
                if (i < 2)
                    (void)tsdb_wire_get_unavail(copy, vecs[i].len, e, 4, &n, &used);
                else if (i == 2)
                    (void)tsdb_wire_get_moved(copy, vecs[i].len, m, 4, &n, &used);
                else if (i == 3)
                    (void)tsdb_wire_get_partial(copy, vecs[i].len, pv, 4, &nv, &nrows, &bits, e, 4, &ne);
                else if (i == 4)
                    (void)tsdb_wire_get_dropped(copy, vecs[i].len, &dv, &db);
                else {
                    tsdb_topo_it t3;
                    if (tsdb_wire_topo_open(&t3, copy, vecs[i].len) == 0) {
                        while (tsdb_wire_topo_node(&t3, &nd) == 1)
                            ;
                        while (tsdb_wire_topo_vol(&t3, &tv) == 1)
                            ;
                    }
                }
                free(copy);
            }
        }
    }
    CHECK(tsdb_wire_error_shape(TSDB_OP_QUERY_RAW) == 5);
    CHECK(tsdb_wire_error_shape(TSDB_OP_SERIES) == 0);
}

/* ------------------------------------------------------------ fake servers */

struct fake {
    int lfd, port;
    void (*script)(struct fake *f, int fd);
    pthread_t th;
    int nconn;              /* connections to accept before the thread ends */
    _Atomic int nreq;       /* requests (not HELLO) seen */
    int ts_seen[32], nts;   /* INSERT_VALUES / INSERT first timestamps in the order they arrived */
    double req_at[32];      /* when each request arrived */
    uint32_t req_id[32];
    void *arg;
};

static bool read_all(int fd, void *p, size_t n)
{
    uint8_t *o = p;
    while (n) {
        ssize_t r = read(fd, o, n);
        if (r <= 0)
            return false;
        o += r;
        n -= (size_t)r;
    }
    return true;
}

static bool read_req(int fd, uint32_t *id, uint8_t *op, uint8_t *body, size_t cap, size_t *blen)
{
    uint8_t h[TSDB_FRAME_HEADER];
    if (!read_all(fd, h, sizeof h))
        return false;
    uint32_t len;
    memcpy(&len, h, 4);
    memcpy(id, h + 4, 4);
    *op = h[8];
    size_t n = len - 5;
    if (n > cap)
        return false;
    *blen = n;
    return read_all(fd, body, n);
}

static void reply(int fd, uint32_t id, int32_t status, const void *body, size_t n)
{
    uint8_t h[13];
    uint32_t len = (uint32_t)(9 + n);
    memcpy(h, &len, 4);
    memcpy(h + 4, &id, 4);
    h[8] = TSDB_OP_REPLY;
    memcpy(h + 9, &status, 4);
    if (send(fd, h, sizeof h, MSG_NOSIGNAL) < 0)
        return;
    if (n)
        (void)!send(fd, body, n, MSG_NOSIGNAL);
}

static void *fake_main(void *arg)
{
    struct fake *f = arg;
    for (int i = 0; i < f->nconn; i++) {
        int fd = accept(f->lfd, NULL, NULL);
        if (fd < 0)
            break;
        uint8_t body[64], op;
        uint32_t id, v = TSDB_PROTOCOL_VERSION;
        uint8_t h[8] = {0};
        memcpy(h, &v, 4);
        size_t n;
        if (read_req(fd, &id, &op, body, sizeof body, &n) && op == TSDB_OP_HELLO) {
            reply(fd, id, 0, h, sizeof h);
            f->script(f, fd);
        }
        close(fd);
    }
    return NULL;
}

static void fake_start(struct fake *f, int nconn, void (*script)(struct fake *, int))
{
    memset(f, 0, sizeof *f);
    f->lfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t al = sizeof a;
    if (f->lfd < 0 || bind(f->lfd, (struct sockaddr *)&a, sizeof a) != 0 || listen(f->lfd, 4) != 0 ||
        getsockname(f->lfd, (struct sockaddr *)&a, &al) != 0) {
        perror("fake server");
        exit(2);
    }
    f->port = ntohs(a.sin_port);
    f->script = script;
    f->nconn = nconn;
    pthread_create(&f->th, NULL, fake_main, f);
}

static void fake_stop(struct fake *f)
{
    shutdown(f->lfd, SHUT_RDWR); /* wakes an accept that no client will ever make */
    pthread_join(f->th, NULL);
    close(f->lfd);
}

/* Records a request; returns its sequence number (0 based). */
static int note_req(struct fake *f, uint32_t id)
{
    int i = atomic_fetch_add(&f->nreq, 1);
    if (i < 32) {
        f->req_at[i] = now_ms();
        f->req_id[i] = id;
    }
    return i;
}

static void body_unavail(uint8_t *b, size_t *n, uint32_t retry_ms)
{
    tsdb_unavail e = {.vol = 3, .reason = TSDB_WHY_NFS_DOWN, .epoch = 7, .since_ms = 1000, .retry_after_ms = retry_ms,
                      .outage_id = 42};
    *n = tsdb_wire_put_unavail(b, 64, &e, 1);
}

/* SERIES requests: the first `refuse` of them UNAVAILABLE with retry_after 300 ms, then an id. */
static void script_refuse_n(struct fake *f, int fd)
{
    uint8_t body[512], op, ub[64];
    size_t n, un;
    uint32_t id;
    long refuse = (long)(intptr_t)f->arg;
    body_unavail(ub, &un, 300);
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        int i = note_req(f, id);
        if (i < refuse)
            reply(fd, id, TSDB_ERR_UNAVAILABLE, ub, un);
        else {
            uint64_t sid = TSDB_SID(0, 5);
            reply(fd, id, 0, &sid, 8);
        }
    }
}

/* SERIES requests: always UNAVAILABLE with retry_after 200 ms. */
static void script_refuse_all(struct fake *f, int fd)
{
    uint8_t body[512], op, ub[64];
    size_t n, un;
    uint32_t id;
    body_unavail(ub, &un, 200);
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        note_req(f, id);
        reply(fd, id, TSDB_ERR_UNAVAILABLE, ub, un);
    }
}

/* The status only, as a protocol 12 server answers it. */
static void script_refuse_nobody(struct fake *f, int fd)
{
    uint8_t body[512], op;
    size_t n;
    uint32_t id;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        note_req(f, id);
        reply(fd, id, TSDB_ERR_UNAVAILABLE, NULL, 0);
    }
}

static void test_unavailable(void)
{
    struct fake f;
    tsdbc *c;
    tsdb_sid sid = 0;
    tsdbc_err_info ei;

    /* two refusals, then success: waits 2 x 300 ms and sends the same request (same id) three times */
    fake_start(&f, 1, script_refuse_n);
    f.arg = (void *)(intptr_t)2;
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    double t0 = now_ms();
    int rc = tsdbc_series(c, "pump1.flow", 1, &sid);
    double dt = now_ms() - t0;
    CHECK(rc == 0);
    CHECK(sid == TSDB_SID(0, 5));
    CHECK(dt >= 550 && dt < 3000); /* mutation "ignore retry_after": 0 ms; "no retry": rc is UNAVAILABLE */
    CHECK(atomic_load(&f.nreq) == 3);
    CHECK(f.req_id[0] == f.req_id[1] && f.req_id[1] == f.req_id[2]);
    CHECK(f.req_at[1] - f.req_at[0] >= 280 && f.req_at[2] - f.req_at[1] >= 280);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_UNAVAILABLE && ei.has_body && ei.nunavail == 1);
    CHECK(ei.unavail[0].vol == 3 && ei.unavail[0].reason == TSDB_WHY_NFS_DOWN && ei.unavail[0].epoch == 7 &&
          ei.unavail[0].retry_after_ms == 300 && ei.unavail[0].outage_id == 42);
    /* the connection is still in step: the next call works */
    CHECK(tsdbc_series(c, "pump2.flow", 1, &sid) == 0);
    tsdbc_close(c);
    fake_stop(&f);

    /* a refusal for longer than the budget: the status and the body come back, after about the budget */
    fake_start(&f, 1, script_refuse_all);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    CHECK(tsdbc_set_retry(c, 700) == 0);
    t0 = now_ms();
    rc = tsdbc_series(c, "pump1.flow", 1, &sid);
    dt = now_ms() - t0;
    CHECK(rc == TSDB_ERR_UNAVAILABLE);
    CHECK(dt >= 350 && dt < 2000);
    CHECK(atomic_load(&f.nreq) >= 3 && atomic_load(&f.nreq) <= 5);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_UNAVAILABLE && ei.has_body && ei.unavail[0].vol == 3);
    tsdb_unavail ue[2];
    unsigned un = 0;
    CHECK(tsdbc_unavailable(c, ue, 2, &un) == 0 && un == 1 && ue[0].retry_after_ms == 200);
    tsdbc_close(c);
    fake_stop(&f);

    /* budget 0: no retry */
    fake_start(&f, 1, script_refuse_all);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    CHECK(tsdbc_set_retry(c, 0) == 0);
    rc = tsdbc_series(c, "pump1.flow", 1, &sid);
    CHECK(rc == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&f.nreq) == 1);
    tsdbc_close(c);
    fake_stop(&f);

    /* a server without the body: the status, has_body 0, a second between tries */
    fake_start(&f, 1, script_refuse_nobody);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    CHECK(tsdbc_set_retry(c, 1500) == 0);
    t0 = now_ms();
    rc = tsdbc_series(c, "pump1.flow", 1, &sid);
    dt = now_ms() - t0;
    CHECK(rc == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&f.nreq) == 2 && dt >= 950 && dt < 2500);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_UNAVAILABLE && !ei.has_body);
    tsdbc_close(c);
    fake_stop(&f);
}

/* ------------------------------------------------------------ MOVED */

static void script_ok(struct fake *f, int fd)
{
    uint8_t body[512], op;
    size_t n;
    uint32_t id;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        note_req(f, id);
        uint64_t sid = TSDB_SID(1, 9);
        reply(fd, id, 0, &sid, 8);
    }
}

static int moved_port; /* what script_moved names */
static int moved_mode; /* 0: names the port, 1: names no address */

static void script_moved(struct fake *f, int fd)
{
    uint8_t body[512], op, mb[128];
    size_t n;
    uint32_t id;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        note_req(f, id);
        char addr[64];
        snprintf(addr, sizeof addr, "127.0.0.1:%d", moved_port);
        tsdb_moved m = {.vol = 1, .epoch = 12, .flags = 0, .node_id = 2, .addr = moved_mode ? "" : addr,
                        .addr_len = moved_mode ? 0 : (uint16_t)strlen(addr)};
        size_t k = tsdb_wire_put_moved(mb, sizeof mb, &m, 1);
        reply(fd, id, TSDB_ERR_MOVED, mb, k);
    }
}

static void test_moved(void)
{
    struct fake a, b;
    tsdbc *c;
    tsdb_sid sid = 0;
    tsdbc_err_info ei;

    fake_start(&b, 1, script_ok);
    fake_start(&a, 1, script_moved);
    moved_port = b.port;
    moved_mode = 0;
    CHECK(tsdbc_connect("127.0.0.1", a.port, &c) == 0);
    int rc = tsdbc_series(c, "pump1.flow", 1, &sid);
    CHECK(rc == 0 && sid == TSDB_SID(1, 9)); /* mutation "do not follow MOVED": rc is MOVED */
    CHECK(atomic_load(&a.nreq) == 1 && atomic_load(&b.nreq) == 1);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_MOVED && ei.nmoved == 1 && ei.moved[0].epoch == 12 &&
          ei.moved[0].node_id == 2 && ei.moved[0].vol == 1);
    /* the connection now is the second server's */
    CHECK(tsdbc_series(c, "pump2.flow", 1, &sid) == 0 && atomic_load(&b.nreq) == 2 && atomic_load(&a.nreq) == 1);
    tsdbc_close(c);
    fake_stop(&a);
    fake_stop(&b);

    /* no address, no other node: the status and the body */
    fake_start(&a, 1, script_moved);
    moved_mode = 1;
    CHECK(tsdbc_connect("127.0.0.1", a.port, &c) == 0);
    rc = tsdbc_series(c, "pump1.flow", 1, &sid);
    CHECK(rc == TSDB_ERR_MOVED);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_MOVED && ei.has_body && ei.nmoved == 1 && ei.moved[0].addr[0] == 0);
    tsdbc_close(c);
    fake_stop(&a);

    /* no address, but another node in the connect string: it goes there. The list is shuffled, so the client
     * starts at a (MOVED, then b) or at b (answers at once): over a few rounds both happen. */
    int via_moved = 0;
    for (int rep = 0; rep < 12; rep++) {
        fake_start(&b, 1, script_ok);
        fake_start(&a, 1, script_moved);
        moved_mode = 1;
        char nodes[128];
        snprintf(nodes, sizeof nodes, "127.0.0.1:%d,127.0.0.1:%d", a.port, b.port);
        CHECK(tsdbc_connect_nodes(nodes, NULL, &c) == 0);
        rc = tsdbc_series(c, "pump1.flow", 1, &sid);
        CHECK(rc == 0 && sid == TSDB_SID(1, 9));
        if (atomic_load(&a.nreq) == 1)
            via_moved++;
        tsdbc_close(c);
        fake_stop(&a);
        fake_stop(&b);
    }
    CHECK(via_moved > 0 && via_moved < 12);
}

/* ------------------------------------------------------------ PARTIAL and DROPPED */

static void script_partial_dropped(struct fake *f, int fd)
{
    uint8_t body[512], op, pb[256];
    size_t n;
    uint32_t id;
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        int i = note_req(f, id);
        if (i == 0) {
            size_t k = tsdb_wire_put_partial(pb, sizeof pb, V_PV, 2, 10, V_BITS, V_UN, 1);
            reply(fd, id, TSDB_ERR_PARTIAL, pb, k);
        } else if (i == 1) {
            size_t k = tsdb_wire_put_dropped(pb, sizeof pb, 5, 1700000000999LL);
            reply(fd, id, TSDB_ERR_DROPPED, pb, k);
        } else {
            reply(fd, id, TSDB_ERR_DROPPED, "xx", 2); /* a malformed body: the status stands, no body */
        }
    }
}

static void test_partial_dropped(void)
{
    struct fake f;
    tsdbc *c;
    tsdb_sid sid = 0;
    tsdbc_err_info ei;
    fake_start(&f, 1, script_partial_dropped);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    CHECK(tsdbc_series(c, "a.b", 1, &sid) == TSDB_ERR_PARTIAL);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_PARTIAL && ei.has_body && ei.nvols == 2 && ei.nrows == 10 &&
          ei.not_applied && ei.not_applied[0] == 0x24 && ei.vols[1].status == TSDB_ERR_UNAVAILABLE &&
          ei.nunavail == 1);
    CHECK(tsdbc_series(c, "a.b", 1, &sid) == TSDB_ERR_DROPPED);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_DROPPED && ei.has_body && ei.dropped_vol == 5 &&
          ei.dropped_before == 1700000000999LL);
    CHECK(tsdbc_series(c, "a.b", 1, &sid) == TSDB_ERR_DROPPED);
    CHECK(tsdbc_last_error(c, &ei) == TSDB_ERR_DROPPED && !ei.has_body);
    CHECK(atomic_load(&f.nreq) == 3); /* never sent again */
    tsdbc_close(c);
    fake_stop(&f);
}

/* ------------------------------------------------------------ pipelined inserts */

/* Frames are told apart by a hash of their payload (the client packs inserts, so the rows are not at a fixed
 * place); the third frame to arrive is refused once. */
static uint64_t fnv(const uint8_t *p, size_t n)
{
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++)
        h = (h ^ p[i]) * 1099511628211ULL;
    return h;
}
static uint64_t pipe_hash[32];

static void script_pipeline(struct fake *f, int fd)
{
    uint8_t body[4096], op, ub[64];
    size_t n, un;
    uint32_t id;
    body_unavail(ub, &un, 100);
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        int i = note_req(f, id);
        if (i < 32)
            pipe_hash[i] = fnv(body, n);
        if (i == 2)
            reply(fd, id, TSDB_ERR_UNAVAILABLE, ub, un);
        else
            reply(fd, id, 0, NULL, 0);
    }
}

static void test_pipeline(void)
{
    struct fake f;
    tsdbc *c;
    fake_start(&f, 1, script_pipeline);
    CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
    for (int i = 0; i < 5; i++) {
        int64_t ts = 1000 + i * 77777;
        float v = (float)i;
        CHECK(tsdbc_insert_series_async(c, TSDB_SID(0, 5), &ts, &v, 1) == 0);
    }
    CHECK(tsdbc_wait(c) == 0); /* mutation "no resend": the refusal reaches the caller */
    /* arrival order: 0 1 2 3 4, then 2 3 4 again (the ones after the refused one go again, in order) */
    CHECK(atomic_load(&f.nreq) == 8);
    static const int want[8] = {0, 1, 2, 3, 4, 2, 3, 4};
    uint64_t first[5];
    for (int i = 0; i < 5; i++)
        first[i] = pipe_hash[i];
    for (int i = 0; i < 8; i++)
        CHECK(pipe_hash[i] == first[want[i]]);
    for (int i = 0; i < 5; i++)
        for (int k = i + 1; k < 5; k++)
            CHECK(first[i] != first[k]);
    CHECK(f.req_id[5] == f.req_id[2] && f.req_id[6] == f.req_id[3] && f.req_id[7] == f.req_id[4]);
    int64_t ts = 9;
    float v = 9;
    CHECK(tsdbc_insert_series(c, TSDB_SID(0, 5), &ts, &v, 1) == 0);
    tsdbc_close(c);
    fake_stop(&f);
}

/* ------------------------------------------------------------ several nodes */

static void test_nodes(void)
{
    struct fake f;
    tsdbc *c;
    tsdb_sid sid = 0;
    /* a port that refuses connections: bound, never listening */
    int dead = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t al = sizeof a;
    CHECK(bind(dead, (struct sockaddr *)&a, sizeof a) == 0 && getsockname(dead, (struct sockaddr *)&a, &al) == 0);
    int dead_port = ntohs(a.sin_port);
    fake_start(&f, 1, script_ok);
    for (int rep = 0; rep < 6; rep++) { /* the order is shuffled: both orders come up */
        if (rep)
            fake_start(&f, 1, script_ok);
        char nodes[160];
        snprintf(nodes, sizeof nodes, "127.0.0.1:%d, 127.0.0.1:%d", dead_port, f.port);
        CHECK(tsdbc_connect_nodes(nodes, NULL, &c) == 0);
        CHECK(tsdbc_series(c, "x.y", 1, &sid) == 0 && sid == TSDB_SID(1, 9));
        tsdbc_close(c);
        fake_stop(&f);
    }
    char nodes[160];
    snprintf(nodes, sizeof nodes, "127.0.0.1:%d,127.0.0.1:%d", dead_port, dead_port);
    CHECK(tsdbc_connect_nodes(nodes, NULL, &c) == TSDB_ERR_IO);
    close(dead);
}

/* ------------------------------------------------------------ TOPOLOGY */

static int topo_cut;

static void script_topology(struct fake *f, int fd)
{
    uint8_t body[512], op;
    size_t n;
    uint32_t id;
    struct vec *v = &vecs[5];
    while (read_req(fd, &id, &op, body, sizeof body, &n)) {
        note_req(f, id);
        reply(fd, id, 0, v->buf, topo_cut ? v->len - 3 : v->len);
    }
}

static void test_topology(void)
{
    struct fake f;
    tsdbc *c;
    tsdbc_topo *t = NULL;
    for (int cut = 0; cut < 2; cut++) {
        topo_cut = cut;
        fake_start(&f, 1, script_topology);
        CHECK(tsdbc_connect("127.0.0.1", f.port, &c) == 0);
        int rc = tsdbc_topology(c, &t);
        if (!cut) {
            CHECK(rc == 0 && t && t->gen == 0xDEADBEEFULL && t->nnodes == 2 && t->nvols == 2);
            if (t && t->nnodes == 2 && t->nvols == 2) {
                CHECK(t->nodes[0].alive == 1 && t->nodes[0].addr[0] == 0 && t->nodes[1].alive == 0 &&
                      strcmp(t->nodes[1].addr, "10.0.0.2:7070") == 0);
                CHECK(t->vols[0].role == TSDB_ROLE_OWNER && strcmp(t->vols[0].path, "/srv/a") == 0);
                CHECK(t->vols[1].health == TSDB_HEALTH_UNAVAILABLE && t->vols[1].reason == TSDB_WHY_NFS_DOWN &&
                      t->vols[1].owner == 2 && t->vols[1].server == 3);
            }
            tsdbc_topology_free(t);
        } else {
            CHECK(rc == TSDB_ERR_CORRUPT && t == NULL);
        }
        tsdbc_close(c);
        fake_stop(&f);
    }
}

int main(int argc, char **argv)
{
    if (argc > 1 && strcmp(argv[1], "--emit") == 0) {
        emit();
        return 0;
    }
    signal(SIGPIPE, SIG_IGN);
    test_codec();
    check_vectors_file(argc > 1 ? argv[1] : "tests/data/wire13-vectors.txt");
    test_unavailable();
    test_moved();
    test_partial_dropped();
    test_pipeline();
    test_nodes();
    test_topology();
    if (failures) {
        fprintf(stderr, "test_wire: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_wire: ok\n");
    return 0;
}
