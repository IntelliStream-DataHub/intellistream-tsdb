/* The HA reply bodies of protocol 13 (include/tsdb_wire.h, which the C client, tsdbd and the tests share): every decoder
 * on arbitrary bytes, and whatever one accepts must encode again to the bytes it read. Byte 0 picks the body (UNAVAILABLE,
 * MOVED, PARTIAL, DROPPED, TOPOLOGY, the TOPOLOGY request, the resend_from tail), the rest is the body. The decoders must
 * never read past len (ASan sees it), never accept a count the bytes cannot hold, and the strings they hand out stay
 * inside the body. */
#include "fuzz.h"
#include "tsdb.h"
#include "tsdb_protocol.h"
#include "tsdb_wire.h"

#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "fuzz_wire: %s:%d: %s\n", __FILE__, __LINE__, #c);                                         \
            abort();                                                                                                   \
        }                                                                                                              \
    } while (0)

static void unavail(const uint8_t *p, size_t len)
{
    static tsdb_unavail e[16];
    unsigned n = 0;
    size_t used = 0;
    if (tsdb_wire_get_unavail(p, len, e, 16, &n, &used) != 0)
        return;
    CHECK(used <= len && used == 2 + (size_t)n * TSDB_UNAVAIL_ENTRY && n <= TSDB_UNAVAIL_MAX);
    uint64_t r;
    int rs = tsdb_wire_get_resend(p + used, len - used, &r);
    CHECK(rs == 0 || rs == 1 || rs == TSDB_ERR_INVALID);
    if (n <= 16) {
        uint8_t out[2 + 16 * TSDB_UNAVAIL_ENTRY];
        CHECK(tsdb_wire_put_unavail(out, sizeof out, e, n) == used);
        CHECK(memcmp(out, p, used) == 0);
    }
    (void)tsdb_wire_retry_after(e, n < 16 ? n : 16);
}

static void moved(const uint8_t *p, size_t len)
{
    static tsdb_moved m[16];
    unsigned n = 0;
    size_t used = 0;
    if (tsdb_wire_get_moved(p, len, m, 16, &n, &used) != 0)
        return;
    CHECK(used <= len && n <= TSDB_UNAVAIL_MAX);
    for (unsigned i = 0; i < n && i < 16; i++) {
        CHECK(m[i].addr_len <= TSDB_ADDR_MAX);
        CHECK(m[i].addr >= (const char *)p && m[i].addr + m[i].addr_len <= (const char *)p + len);
    }
    if (n <= 16) {
        uint8_t out[2 + 16 * (13 + TSDB_ADDR_MAX)];
        CHECK(tsdb_wire_put_moved(out, sizeof out, m, n) == used);
        CHECK(memcmp(out, p, used) == 0);
    }
    uint64_t r;
    (void)tsdb_wire_get_resend(p + used, len - used, &r);
}

static void partial(const uint8_t *p, size_t len)
{
    static tsdb_partial_vol v[16];
    static tsdb_unavail e[16];
    unsigned nv = 0, ne = 0;
    uint32_t nrows = 0;
    const uint8_t *bits = NULL;
    if (tsdb_wire_get_partial(p, len, v, 16, &nv, &nrows, &bits, e, 16, &ne) != 0)
        return;
    CHECK(nv <= TSDB_UNAVAIL_MAX && ne <= TSDB_UNAVAIL_MAX && nrows <= TSDB_MAX_FRAME);
    CHECK(bits >= p && bits + (nrows + 7) / 8 <= p + len);
    /* the body is exactly what the counts say: a re-encode with the same counts has the same length */
    static uint8_t out[1 << 16];
    if (nv <= 16 && ne <= 16 && (size_t)nrows / 8 + 1 + 2 + 4 * 16 + 4 + 2 + 16 * TSDB_UNAVAIL_ENTRY <= sizeof out) {
        size_t w = tsdb_wire_put_partial(out, sizeof out, v, nv, nrows, bits, e, ne);
        CHECK(w == len && memcmp(out, p, len) == 0);
    }
}

static void dropped(const uint8_t *p, size_t len)
{
    uint16_t vol;
    int64_t before;
    if (tsdb_wire_get_dropped(p, len, &vol, &before) != 0)
        return;
    uint8_t out[16];
    size_t w = tsdb_wire_put_dropped(out, sizeof out, vol, before);
    CHECK(w <= len && memcmp(out, p, w) == 0);
}

static void topology(const uint8_t *p, size_t len)
{
    tsdb_topo_it it;
    if (tsdb_wire_topo_open(&it, p, len) != 0)
        return;
    tsdb_topo_node n;
    tsdb_topo_vol v;
    uint32_t nodes = 0, vols = 0;
    int r;
    while ((r = tsdb_wire_topo_node(&it, &n)) == 1) {
        CHECK(n.addr_len <= TSDB_ADDR_MAX && n.addr >= (const char *)p && n.addr + n.addr_len <= (const char *)p + len);
        nodes++;
    }
    if (r != 0)
        return;
    CHECK(nodes == it.nn);
    while ((r = tsdb_wire_topo_vol(&it, &v)) == 1) {
        CHECK(v.addr >= (const char *)p && v.addr + v.addr_len <= (const char *)p + len);
        CHECK(v.path >= (const char *)p && v.path + v.path_len <= (const char *)p + len);
        vols++;
    }
    if (r == 0)
        CHECK(vols == it.nv && it.at == len);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 1)
        return 0;
    const uint8_t *p = data + 1;
    size_t len = size - 1;
    /* a copy at the exact length, so that ASan sees a read past the end */
    uint8_t *c = malloc(len ? len : 1);
    if (!c)
        abort();
    memcpy(c, p, len);
    switch (data[0] % 6) {
    case 0: unavail(c, len); break;
    case 1: moved(c, len); break;
    case 2: partial(c, len); break;
    case 3: dropped(c, len); break;
    case 4: topology(c, len); break;
    default: {
        uint64_t g;
        int rc = tsdb_wire_get_topology_request(c, len, &g);
        CHECK((rc == 0) == (len == 0 || len == 8));
        break;
    }
    }
    free(c);
    return 0;
}

static void seed(const char *dir, const char *name, int kind, const uint8_t *b, size_t n)
{
    uint8_t *f = malloc(n + 1);
    f[0] = (uint8_t)kind;
    memcpy(f + 1, b, n);
    fuzz_put(dir, name, f, n + 1);
    free(f);
}

void fuzz_seeds(const char *dir)
{
    uint8_t b[4096];
    tsdb_unavail u[2] = {{3, TSDB_WHY_NFS_DOWN, 0, 7, 123456, 300, 99}, {TSDB_VOL_ANY, TSDB_WHY_NFS_DOWN, 1, 0, 0, 5000, 0}};
    size_t n = tsdb_wire_put_unavail(b, sizeof b, u, 2);
    seed(dir, "seed-unavail", 0, b, n);
    memset(b + n, 0, 8);
    seed(dir, "seed-unavail-resend", 0, b, n + 8);
    tsdb_moved m[2] = {{2, 5, 0, 7, "10.0.0.2:7070", 13}, {3, 6, TSDB_MOVED_HANDOFF, 8, "", 0}};
    n = tsdb_wire_put_moved(b, sizeof b, m, 2);
    seed(dir, "seed-moved", 1, b, n);
    tsdb_partial_vol pv[2] = {{1, 0}, {2, TSDB_ERR_UNAVAILABLE}};
    uint8_t bits[2] = {0x05, 0x01};
    n = tsdb_wire_put_partial(b, sizeof b, pv, 2, 10, bits, u, 1);
    seed(dir, "seed-partial", 2, b, n);
    n = tsdb_wire_put_dropped(b, sizeof b, 4, 1700000000000LL);
    seed(dir, "seed-dropped", 3, b, n);
    uint8_t t[4096];
    size_t w = tsdb_wire_put_topo_head(t, sizeof t, 42, 1);
    tsdb_topo_node tn = {1, 1, "127.0.0.1:7070", 14};
    w += tsdb_wire_put_topo_node(t + w, sizeof t - w, &tn);
    w += tsdb_wire_put_u32(t + w, sizeof t - w, 1);
    tsdb_topo_vol tv = {0, TSDB_ROLE_OWNER, TSDB_HEALTH_AVAILABLE, 1, 0, 0, 0, 1, 0, "", 0, "/data/vol0", 10};
    w += tsdb_wire_put_topo_vol(t + w, sizeof t - w, &tv);
    seed(dir, "seed-topology", 4, t, w);
    uint8_t g[8] = {42};
    seed(dir, "seed-topology-request", 5, g, 8);
    seed(dir, "seed-topology-request-empty", 5, g, 0);
}
