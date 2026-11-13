/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Statistics over the network (docs/design-stats.md section 5.1, protocol 15): the ops STATS_SERIES and STATS_TENANTS
 * of tsdbd, through the C client and as raw frames.
 *
 *  - login on: root, a tenant admin of acme/, a reader of acme/ and a reader of everything. The records are checked
 *    against what was written, then (after the server stopped) byte for byte against tsdb_series_stats and
 *    tsdb_tenant_stats_get of the engine on the same directory. Denied and missing look alike for a user without the
 *    right on `all`, a tenant admin sees its own tenant and no other, the error bounds and thresholds of the hot lists
 *    go only to who reads everything, per-tenant counters of the caller, the metrics port names no tenant.
 *  - --no-auth with --tenants FILE: the tenants of the file, SIGHUP, malformed frames (INVALID, the connection goes on),
 *    the 64 MB cap of a reply with its cursor, CANCEL, --no-stats.
 *  - a fake server of protocol 15 that answers one id at a time (the client asks again for the rest) and one that
 *    changes its tenant list between two pages; a server of protocol 14: INVALID, nothing sent.
 *
 *   TSDBD=build/tsdbd TSDBD_IO=epoll ./build/test_stats_net [scratch]
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_client.h"
#include "tsdb_crypto.h"
#include "tsdb_protocol.h"
#include "users.h"
#include "harness.h"
#include "auth_harness.h"

#include <poll.h>
#include <pthread.h>

enum { NA = 1000, NB = 500, NZ = 10, NP = 20 };
static tsdb_sid ia, ib, iz, ip;
static const tsdb_sid unknown_id = TSDB_SID(0, 0xffffff00u);

static void insert_f32(tsdbc *c, tsdb_sid id, int n)
{
    int64_t *ts = malloc((size_t)n * sizeof *ts);
    float *v = malloc((size_t)n * sizeof *v);
    for (int i = 0; i < n; i++) {
        ts[i] = T0 + (int64_t)i * 1000;
        v[i] = (float)i * 0.5f;
    }
    OK(tsdbc_insert_series(c, id, ts, v, (size_t)n));
    free(ts);
    free(v);
}

static void make_data(tsdbc *root)
{
    OK(tsdbc_series_create(root, "acme/a", TSDB_FLOAT32, NULL, &ia));
    OK(tsdbc_series_create(root, "acme/b", TSDB_INT64, NULL, &ib));
    OK(tsdbc_series_create(root, "globex/z", TSDB_FLOAT32, NULL, &iz));
    OK(tsdbc_series_create(root, "plain", TSDB_FLOAT32, NULL, &ip));
    insert_f32(root, ia, NA);
    insert_f32(root, iz, NZ);
    insert_f32(root, ip, NP);
    int64_t ts[NB], iv[NB];
    uint8_t mask[(NB + 7) / 8];
    memset(mask, 0xff, sizeof mask);
    for (int i = 0; i < NB; i++) {
        ts[i] = T0 + (int64_t)i * 1000;
        iv[i] = i * 3;
        if (i % 10 == 0)
            mask[i >> 3] &= (uint8_t)~(1u << (i & 7)); /* 50 nulls */
    }
    tsdb_values col = {TSDB_INT64, iv, mask};
    OK(tsdbc_insert_series_values(root, ib, ts, &col, NB));
    OK(tsdbc_flush(root));
}

/* Waits until the sweeper has counted every part (the totals are then exact). */
static void settle(tsdbc *c)
{
    for (int i = 0; i < 300; i++) {
        tsdbc_tenant_result r;
        OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r));
        bool ok = !(r.flags & TSDB_TSTATS_PARTIAL) && r.parts_counted == r.parts_listed && r.parts_listed > 0;
        for (size_t j = 0; j < r.nrows; j++)
            ok = ok && !(r.rows[j].st.flags & TSDB_TENANT_PARTIAL);
        tsdbc_tenant_result_free(&r);
        if (ok)
            return;
        nap_ms(100);
    }
    CHECK(!"the sweeper settled");
}

static const tsdbc_tenant_row *row_of(const tsdbc_tenant_result *r, const char *prefix, uint32_t volume)
{
    for (size_t i = 0; i < r->nrows; i++)
        if (strcmp(r->rows[i].prefix, prefix) == 0 && r->rows[i].volume == volume)
            return &r->rows[i];
    return NULL;
}

static tsdb_series_stat saved[4]; /* what the server said, for the byte-for-byte check after it stopped */
static tsdbc_tenant_row saved_acme, saved_none;

static void test_series_root(tsdbc *root)
{
    printf("  STATS_SERIES as the superuser: the numbers of what was written\n");
    tsdb_sid ids[6] = {ia, ib, iz, ip, unknown_id, ia | 1ull << 48};
    tsdb_series_stat r[6];
    OK(tsdbc_series_stats(root, ids, 6, 0, r));
    CHECK(r[0].status == 0 && r[0].type == TSDB_FLOAT32 && r[0].rows == NA && r[0].nulls == 0 &&
          r[0].memtable_rows == 0 && r[0].first_ts == T0 && r[0].last_ts == T0 + (NA - 1) * 1000 &&
          r[0].logical_bytes == (uint64_t)NA * 12 && r[0].parts >= 1 && r[0].granules >= 1 && r[0].ts_bytes > 0 &&
          r[0].value_bytes > 0 && !(r[0].flags & TSDB_STATS_NO_BOUND) && r[0].live_rows == 0);
    CHECK(r[1].status == 0 && r[1].type == TSDB_INT64 && r[1].rows == NB && r[1].nulls == NB / 10 &&
          r[1].logical_bytes == (uint64_t)NB * 16 && r[1].last_ts == T0 + (NB - 1) * 1000);
    CHECK(r[2].status == 0 && r[2].rows == NZ && r[3].status == 0 && r[3].rows == NP);
    for (int i = 4; i < 6; i++) { /* an id that names no series: NOTFOUND, every field after the status 0 */
        tsdb_series_stat z = {0};
        z.status = TSDB_ERR_NOTFOUND;
        CHECK(memcmp(&r[i], &z, sizeof z) == 0);
    }
    for (int i = 0; i < 4; i++)
        saved[i] = r[i];

    /* the resident flag, and a request larger than one frame of ids: the client asks in pieces */
    tsdb_series_stat two[2];
    OK(tsdbc_series_stats(root, ids, 2, TSDB_SSTAT_REQ_RESIDENT, two));
    CHECK(two[0].rows == NA && two[1].rows == NB);
    size_t big = 70000;
    tsdb_sid *bi = malloc(big * sizeof *bi);
    tsdb_series_stat *bo = malloc(big * sizeof *bo);
    for (size_t i = 0; i < big; i++)
        bi[i] = (i % 3 == 0) ? ia : (i % 3 == 1) ? ib : unknown_id;
    OK(tsdbc_series_stats(root, bi, big, 0, bo));
    for (size_t i = 0; i < big; i++)
        if (!(bo[i].status == (i % 3 == 2 ? TSDB_ERR_NOTFOUND : 0) && bo[i].rows == (i % 3 == 0 ? NA : i % 3 == 1 ? NB : 0))) {
            CHECK(!"a record of the 70,000 in its place");
            break;
        }
    OK(tsdbc_series_stats(root, bi, 2100, TSDB_SSTAT_REQ_RESIDENT, bo));
    CHECK(bo[2099].status == (2099 % 3 == 2 ? TSDB_ERR_NOTFOUND : 0));
    free(bi);
    free(bo);
    CHECK(tsdbc_series_stats(root, ids, 2, 2, two) == TSDB_ERR_INVALID); /* EXACT_ROWS is reserved */
    CHECK(tsdbc_series_stats(root, ids, 0, 0, NULL) == 0);
}

static void test_tenants_root(tsdbc *root)
{
    printf("  STATS_TENANTS as the superuser: rows, volumes, the caller's counters, the hot lists\n");
    tsdbc_tenant_result r;
    OK(tsdbc_tenant_stats(root, NULL, 0, 0, 0, &r));
    CHECK(r.nrows == 2 && r.rows[0].prefix_len == 0 && strcmp(r.rows[1].prefix, "acme/") == 0);
    CHECK(r.parts_listed > 0 && r.parts_counted == r.parts_listed && r.generation > 0 && r.list_hash != 0);
    const tsdbc_tenant_row *none = row_of(&r, "", TSDB_TSTATS_VOLUME_SUM), *acme = row_of(&r, "acme/", TSDB_TSTATS_VOLUME_SUM);
    CHECK(none && acme && none->status == 0 && acme->status == 0);
    if (none && acme) {
        CHECK(acme->st.series == 2 && acme->st.rows == NA + NB && acme->st.nulls == NB / 10 &&
              acme->st.logical_bytes == (uint64_t)NA * 12 + (uint64_t)NB * 16 && acme->st.granules >= 2);
        CHECK(none->st.series == 2 && none->st.rows == NZ + NP && none->st.nulls == 0);
        CHECK(acme->st.ts_bytes == saved[0].ts_bytes + saved[1].ts_bytes &&
              acme->st.value_bytes == saved[0].value_bytes + saved[1].value_bytes);
        CHECK(!acme->has_caller && acme->nhot == 0);
        saved_acme = *acme; /* only .st is used: the pointers belong to r */
        saved_acme.prefix = NULL;
        saved_acme.hot = NULL;
        saved_none = *none;
        saved_none.prefix = NULL;
        saved_none.hot = NULL;
    }
    uint32_t gen = r.generation;
    uint64_t hash = r.list_hash;
    tsdbc_tenant_result_free(&r);

    /* one volume here: its row has the numbers of the sum; named tenants; (none) is NULL or "" */
    const char *want[2] = {"acme/", NULL};
    OK(tsdbc_tenant_stats(root, want, 2, TSDB_TSTATS_VOLUMES | TSDB_TSTATS_CALLER, 0, &r));
    CHECK(r.nrows == 4 && r.generation == gen && r.list_hash == hash);
    const tsdbc_tenant_row *v0 = row_of(&r, "acme/", 0);
    acme = row_of(&r, "acme/", TSDB_TSTATS_VOLUME_SUM);
    CHECK(v0 && acme && v0->st.rows == acme->st.rows && v0->st.series == acme->st.series && !v0->has_caller);
    CHECK(acme && acme->has_caller && !row_of(&r, "", 0)->has_caller);
    none = row_of(&r, "", TSDB_TSTATS_VOLUME_SUM);
    /* root's own requests count under (none): it wrote and flushed, and asked statistics */
    CHECK(none && none->has_caller && none->caller.requests_insert > 0 && none->caller.requests_admin > 0 &&
          none->caller.inserted_rows >= NA + NB + NZ + NP && none->caller.inserted_bytes > 0 &&
          none->caller.errors_notfound == 0);
    tsdbc_tenant_result_free(&r);

    const char *missing[1] = {"nobody/"};
    OK(tsdbc_tenant_stats(root, missing, 1, 0, 0, &r));
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_NOTFOUND && r.rows[0].st.rows == 0);
    tsdbc_tenant_result_free(&r);

    /* the hot lists: the rows just written are the hottest of acme/ (writes), with the threshold of each kind */
    const char *ac[1] = {"acme/"};
    OK(tsdbc_tenant_stats(root, ac, 1, TSDB_TSTATS_HOT, 4, &r));
    CHECK(r.nrows == 1 && r.rows[0].status == 0);
    int thresholds = 0;
    bool found_a = false;
    for (size_t i = 0; r.nrows && i < r.rows[0].nhot; i++) {
        const tsdbc_hot *h = &r.rows[0].hot[i];
        thresholds += h->id == 0;
        if (h->kind == TSDB_HOT_WRITES && h->id == ia)
            found_a = h->estimate >= NA - h->error && h->estimate <= NA + h->error;
        CHECK(h->id == 0 || h->id == ia || h->id == ib);
    }
    CHECK(thresholds == 3 && found_a);
    tsdbc_tenant_result_free(&r);
}

static void test_rights(void)
{
    printf("  rights: a tenant admin sees its own tenant, denied and missing look alike, bounds only for readers of all\n");
    tsdbc *adm = login_as("acme/admin", "adminpw"), *rd = login_as("acme/reader", "readerpw"),
          *all = login_as("viewer", "viewpw"), *root = login_as("root", "rootpw");
    tsdbc_tenant_result r;

    OK(tsdbc_tenant_stats(adm, NULL, 0, 0, 0, &r));
    CHECK(r.nrows == 1 && strcmp(r.rows[0].prefix, "acme/") == 0 && r.rows[0].status == 0 &&
          r.rows[0].st.series == 2 && r.parts_counted == 0 && r.parts_listed == 0);
    tsdbc_tenant_result_free(&r);
    const char *q1[1] = {"globex/"}, *q2[1] = {NULL}, *q3[1] = {"acme/sub"}, *q4[1] = {"globex/zz"}, *q5[1] = {"acme/"};
    OK(tsdbc_tenant_stats(adm, q1, 1, TSDB_TSTATS_HOT | TSDB_TSTATS_CALLER, 3, &r));
    tsdbc_tenant_caller nocaller = {0};
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_DENIED && r.rows[0].st.rows == 0 && r.rows[0].nhot == 0 &&
          memcmp(&r.rows[0].caller, &nocaller, sizeof nocaller) == 0); /* the block is there, all zero */
    tsdbc_tenant_result_free(&r);
    OK(tsdbc_tenant_stats(adm, q2, 1, 0, 0, &r)); /* (none) is the whole server's rest */
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_DENIED && r.rows[0].st.series == 0);
    tsdbc_tenant_result_free(&r);
    OK(tsdbc_tenant_stats(adm, q3, 1, 0, 0, &r)); /* a name the admin may see, not in the list: NOTFOUND */
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_NOTFOUND);
    tsdbc_tenant_result_free(&r);
    OK(tsdbc_tenant_stats(adm, q4, 1, 0, 0, &r)); /* one it may not, not in the list: DENIED like the existing one */
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_DENIED);
    tsdbc_tenant_result_free(&r);

    /* the hot lists for an admin: its own series, no thresholds, no error bounds */
    OK(tsdbc_tenant_stats(adm, q5, 1, TSDB_TSTATS_HOT | TSDB_TSTATS_CALLER, 4, &r));
    CHECK(r.nrows == 1 && r.rows[0].status == 0 && r.rows[0].has_caller && r.rows[0].caller.requests_other > 0);
    for (size_t i = 0; r.nrows && i < r.rows[0].nhot; i++)
        CHECK(r.rows[0].hot[i].id != 0 && r.rows[0].hot[i].error == 0 &&
              (r.rows[0].hot[i].id == ia || r.rows[0].hot[i].id == ib));
    tsdbc_tenant_result_free(&r);

    /* a reader without admin: nothing to list, and a named tenant is DENIED */
    OK(tsdbc_tenant_stats(rd, NULL, 0, 0, 0, &r));
    CHECK(r.nrows == 0);
    tsdbc_tenant_result_free(&r);
    OK(tsdbc_tenant_stats(rd, q5, 1, 0, 0, &r));
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_DENIED && r.rows[0].st.rows == 0);
    tsdbc_tenant_result_free(&r);

    /* a reader of everything (not a superuser) sees every tenant, with the totals of parts */
    OK(tsdbc_tenant_stats(all, NULL, 0, TSDB_TSTATS_HOT, 2, &r));
    CHECK(r.nrows == 2 && r.parts_listed > 0 && r.parts_counted == r.parts_listed);
    tsdbc_tenant_result_free(&r);

    /* STATS_SERIES: the own series, a foreign one and an id that names nothing: the last two alike */
    tsdb_sid ids[4] = {ia, iz, unknown_id, ib};
    tsdb_series_stat s[4];
    OK(tsdbc_series_stats(rd, ids, 4, 0, s));
    CHECK(s[0].status == 0 && s[0].rows == NA && s[1].status == TSDB_ERR_DENIED && s[2].status == TSDB_ERR_DENIED &&
          s[3].status == 0 && s[3].rows == NB);
    tsdb_series_stat zero = {0};
    zero.status = TSDB_ERR_DENIED;
    CHECK(memcmp(&s[1], &zero, sizeof zero) == 0 && memcmp(&s[2], &zero, sizeof zero) == 0);
    CHECK((s[0].flags & TSDB_STATS_NO_BOUND) && s[0].reads_err == 0 && s[0].writes_err == 0);
    OK(tsdbc_series_stats(all, ids, 4, 0, s)); /* read on all: a missing id is NOTFOUND, the bounds are given */
    CHECK(s[0].status == 0 && s[1].status == 0 && s[1].rows == NZ && s[2].status == TSDB_ERR_NOTFOUND &&
          !(s[0].flags & TSDB_STATS_NO_BOUND));
    OK(tsdbc_series_stats(root, ids, 4, 0, s));
    CHECK(s[1].status == 0 && s[2].status == TSDB_ERR_NOTFOUND);

    /* a denied request counts for the tenant of the user that asked (denied series, failed logins) */
    OK(tsdbc_tenant_stats(adm, q5, 1, TSDB_TSTATS_CALLER, 0, &r));
    uint64_t denied_before = r.rows[0].caller.denied_series, fails_before = r.rows[0].caller.failed_logins;
    tsdbc_tenant_result_free(&r);
    tsdb_sid foreign[3] = {iz, iz, ip};
    OK(tsdbc_series_stats(adm, foreign, 3, 0, s));
    tsdbc *bad = NULL;
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "acme/admin", "wrongpw", &bad) == TSDB_ERR_AUTH && !bad);
    OK(tsdbc_tenant_stats(adm, q5, 1, TSDB_TSTATS_CALLER, 0, &r));
    CHECK(r.rows[0].caller.denied_series >= denied_before + 3 && r.rows[0].caller.failed_logins >= fails_before + 1);
    tsdbc_tenant_result_free(&r);

    tsdbc_close(adm);
    tsdbc_close(rd);
    tsdbc_close(all);
    tsdbc_close(root);
}

static void test_metrics_no_tenants(void)
{
    printf("  the metrics port names no tenant\n");
    CHECK(th_mport > 0);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)th_mport)};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    CHECK(connect(fd, (struct sockaddr *)&sa, sizeof sa) == 0);
    struct timeval tv = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    static const char req[] = "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";
    CHECK(write(fd, req, sizeof req - 1) == (ssize_t)sizeof req - 1);
    static char buf[1 << 20];
    size_t got = 0;
    for (ssize_t n; got < sizeof buf - 1 && (n = read(fd, buf + got, sizeof buf - 1 - got)) > 0;)
        got += (size_t)n;
    buf[got] = 0;
    close(fd);
    CHECK(got > 1000 && strstr(buf, "tsdb_") != NULL);
    const char *bad = strstr(buf, "acme") ? strstr(buf, "acme") : strstr(buf, "globex") ? strstr(buf, "globex") : strstr(buf, "tenant=") ? strstr(buf, "tenant=") : strstr(buf, "tsdb_tenant");
    if (bad)
        fprintf(stderr, "test_stats_net: the metrics port says: %.80s\n", bad);
    CHECK(!bad);
}

/* After the server stopped: the engine on the same directory gives the same records, byte for byte (but the
 * sampled hot lists), and the same tenant numbers. */
static void test_oracle(const char *dir)
{
    printf("  the records of the server equal tsdb_series_stats and tsdb_tenant_stats_get\n");
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    const char *acme = "acme/";
    OK(tsdb_set_tenants(db, &acme, 1));
    OK(tsdb_stats_settle(db, 30000));
    tsdb_sid ids[4] = {ia, ib, iz, ip};
    tsdb_series_stat e[4];
    size_t done = 0;
    OK(tsdb_series_stats(db, ids, 4, 0, e, &done));
    CHECK(done == 4);
    for (int i = 0; i < 4; i++) {
        tsdb_series_stat a = saved[i], b = e[i];
        a.reads_est = a.reads_err = a.writes_est = a.writes_err = 0;
        b.reads_est = b.reads_err = b.writes_est = b.writes_err = 0;
        CHECK(memcmp(&a, &b, sizeof a) == 0);
    }
    tsdb_tenant_stats ta, tn;
    OK(tsdb_tenant_stats_get(db, "acme/", TSDB_TENANT_VOLUME_SUM, &ta));
    OK(tsdb_tenant_stats_get(db, NULL, TSDB_TENANT_VOLUME_SUM, &tn));
    const tsdb_tenant_stats *sa = &saved_acme.st, *sn = &saved_none.st;
    CHECK(sa->series == ta.series && sa->rows == ta.rows && sa->nulls == ta.nulls &&
          sa->logical_bytes == ta.logical_bytes && sa->ts_bytes == ta.ts_bytes && sa->value_bytes == ta.value_bytes &&
          sa->index_bytes == ta.index_bytes && sa->granules == ta.granules);
    CHECK(sn->series == tn.series && sn->rows == tn.rows && sn->ts_bytes == tn.ts_bytes &&
          sn->value_bytes == tn.value_bytes && sn->granules == tn.granules);
    tsdb_close(db);
}


/* ------------------------------------------------------------ login on, second run: what the first could not show */

static tsdbc_tenant_caller caller_of(tsdbc *root, const char *prefix)
{
    tsdbc_tenant_result r;
    const char *q[1] = {prefix};
    tsdbc_tenant_caller c = {0};
    OK(tsdbc_tenant_stats(root, q, 1, TSDB_TSTATS_CALLER, 0, &r));
    CHECK(r.nrows == 1 && r.rows[0].status == 0 && r.rows[0].has_caller);
    if (r.nrows)
        c = r.rows[0].caller;
    tsdbc_tenant_result_free(&r);
    return c;
}

static int ignore_series(void *ctx, const tsdb_series_result *r)
{
    (void)ctx;
    (void)r;
    return 0;
}

/* The hot lists carry error bounds only for whoever reads everything: more series than the summary holds (256) make
 * entries with an error above 0, which the superuser sees and a tenant admin does not. */
static void test_hot_bounds(tsdbc *root)
{
    printf("  hot lists: error bounds for who reads everything, none for a tenant admin\n");
    enum { MANY = 400 };
    for (int i = 0; i < MANY; i++) {
        char name[32];
        tsdb_sid id;
        snprintf(name, sizeof name, "acme/h%04d", i);
        OK(tsdbc_series_create(root, name, TSDB_FLOAT32, NULL, &id));
        insert_f32(root, id, 1 + i % 3);
    }
    OK(tsdbc_flush(root));
    settle(root);
    const char *ac[1] = {"acme/"};
    bool bounded = false;
    for (int t = 0; t < 100 && !bounded; t++) {
        tsdbc_tenant_result r;
        OK(tsdbc_tenant_stats(root, ac, 1, TSDB_TSTATS_HOT, TSDB_TSTATS_MAX_K, &r));
        for (size_t i = 0; r.nrows && i < r.rows[0].nhot; i++)
            bounded = bounded || (r.rows[0].hot[i].id != 0 && r.rows[0].hot[i].error > 0);
        tsdbc_tenant_result_free(&r);
        if (!bounded)
            nap_ms(100);
    }
    CHECK(bounded); /* the superuser sees a bound */
    tsdbc *adm = login_as("acme/admin", "adminpw");
    tsdbc_tenant_result r;
    OK(tsdbc_tenant_stats(adm, ac, 1, TSDB_TSTATS_HOT, TSDB_TSTATS_MAX_K, &r));
    CHECK(r.nrows == 1 && r.rows[0].status == 0 && r.rows[0].nhot > 0);
    for (size_t i = 0; r.nrows && i < r.rows[0].nhot; i++)
        CHECK(r.rows[0].hot[i].id != 0 && r.rows[0].hot[i].error == 0);
    tsdbc_tenant_result_free(&r);
    tsdbc_close(adm);
}

/* Counters of another tenant never reach a tenant admin (a DENIED row has a zero block), the request classes, the
 * two kinds of denial, and the tenant a user's requests count under. */
static void test_counters(tsdbc *root)
{
    printf("  caller counters: a foreign tenant's block is zero, the classes, denied requests and series, the user's tenant\n");
    tsdbc *boss = login_as("globex/boss", "bosspw"), *adm = login_as("acme/admin", "adminpw"),
          *rd = login_as("acme/reader", "readerpw");
    /* globex/ has traffic: inserts, queries, a failed login */
    for (int i = 0; i < 3; i++) {
        insert_f32(boss, iz, NZ);
        CHECK(query(boss, iz) == 0);
    }
    tsdbc *bad = NULL;
    CHECK(tsdbc_connect_auth("127.0.0.1", port, "globex/boss", "wrongpw", &bad) == TSDB_ERR_AUTH && !bad);
    tsdbc_tenant_caller g = caller_of(root, "globex/");
    CHECK(g.requests_insert >= 3 && g.requests_query >= 3 && g.failed_logins >= 1 && g.inserted_rows >= 3 * NZ);
    tsdbc_tenant_result r;
    const char *gx[1] = {"globex/"};
    OK(tsdbc_tenant_stats(adm, gx, 1, TSDB_TSTATS_CALLER, 0, &r));
    tsdbc_tenant_caller zero = {0};
    CHECK(r.nrows == 1 && r.rows[0].status == TSDB_ERR_DENIED && memcmp(&r.rows[0].caller, &zero, sizeof zero) == 0);
    tsdbc_tenant_result_free(&r);

    /* the classes: three queries of the reader count as queries and nothing else */
    tsdbc_tenant_caller a0 = caller_of(root, "acme/");
    for (int i = 0; i < 3; i++)
        CHECK(query(rd, ia) == 0);
    tsdbc_tenant_caller a1 = caller_of(root, "acme/");
    CHECK(a1.requests_query >= a0.requests_query + 3 && a1.requests_insert == a0.requests_insert &&
          a1.requests_delete == a0.requests_delete && a1.denied_requests == a0.denied_requests);
    insert_f32(adm, ia, 5);
    tsdbc_tenant_caller a2 = caller_of(root, "acme/");
    CHECK(a2.requests_insert >= a1.requests_insert + 1 && a2.requests_query == a1.requests_query);

    /* a whole request refused is a denied request, a denied series inside a QUERY_MULTI a denied series */
    CHECK(query(rd, iz) == TSDB_ERR_DENIED);
    tsdbc_tenant_caller a3 = caller_of(root, "acme/");
    CHECK(a3.denied_requests == a2.denied_requests + 1 && a3.denied_series == a2.denied_series);
    tsdb_sid two[2] = {ia, iz};
    tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = T0, .end = T0 + 10000};
    (void)tsdbc_query_multi(rd, &q, two, 2, NULL, ignore_series, NULL);
    tsdbc_tenant_caller a4 = caller_of(root, "acme/");
    CHECK(a4.denied_series == a3.denied_series + 1 && a4.denied_requests == a3.denied_requests);

    /* acme/x is named under acme/ but holds a grant outside it: its requests are nobody's tenant's */
    tsdbc *x = login_as("acme/x", "xpw");
    tsdbc_tenant_caller n0 = caller_of(root, NULL), b0 = caller_of(root, "acme/"), g0 = caller_of(root, "globex/");
    for (int i = 0; i < 3; i++)
        CHECK(query(x, iz) == 0);
    tsdbc_tenant_caller n1 = caller_of(root, NULL), b1 = caller_of(root, "acme/"), g1 = caller_of(root, "globex/");
    CHECK(n1.requests_query >= n0.requests_query + 3 && b1.requests_query == b0.requests_query &&
          g1.requests_query == g0.requests_query);
    tsdbc_close(x);
    tsdbc_close(rd);
    tsdbc_close(adm);
    tsdbc_close(boss);
}

static void test_phase2(const char *dir)
{
    CHECK(cli_user(dir, "bosspw", "add globex/boss") == 0);
    CHECK(cli_user(dir, NULL, "grant globex/boss admin prefix:globex/") == 0);
    CHECK(cli_user(dir, "xpw", "add acme/x") == 0);
    CHECK(cli_user(dir, NULL, "grant acme/x read prefix:globex/") == 0);
    const char *extra[] = {"--tenants-settle", "1", NULL};
    pid_t srv = start(dir, extra);
    tsdbc *root = login_as("root", "rootpw");
    test_hot_bounds(root);
    test_counters(root);
    tsdbc_close(root);
    stop(srv);
}

/* ------------------------------------------------------------ no login: raw frames, a tenants file, the cap */

static uint8_t *tenants_req(uint16_t flags, uint16_t k, uint32_t n, uint32_t cursor, uint32_t zero, const char *name,
                            size_t namelen, size_t *len)
{
    size_t total = 16 + (n ? n * (2 + namelen) : 0);
    uint8_t *p = calloc(1, total + 1);
    memcpy(p, &flags, 2);
    memcpy(p + 2, &k, 2);
    memcpy(p + 4, &n, 4);
    memcpy(p + 8, &cursor, 4);
    memcpy(p + 12, &zero, 4);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t l = (uint16_t)namelen;
        memcpy(p + 16 + (size_t)i * (2 + namelen), &l, 2);
        memset(p + 16 + (size_t)i * (2 + namelen) + 2, 'a' + (int)(i % 26), namelen);
    }
    *len = total;
    return p;
}

/* A request that must be answered with `want` and no body, the connection staying usable. */
static void expect_status(uint8_t op, const void *p, size_t len, int32_t want, const char *what)
{
    int fd = raw_connect();
    raw_send(fd, 7, op, p, (uint32_t)len);
    uint8_t rest[64];
    size_t n;
    int32_t st = raw_reply(fd, rest, sizeof rest, &n);
    if (st != want || n != 0) {
        fprintf(stderr, "test_stats_net: %s: status %d, %zu body bytes, want %d\n", what, st, n, want);
        CHECK(!"a bad request answered as expected");
    }
    raw_send(fd, 8, TSDB_OP_STATS, NULL, 0);
    uint32_t id = 0;
    size_t m;
    CHECK(raw_frame(fd, &id, rest, sizeof rest, &m, NULL, NULL) == 0 && id == 8);
    close(fd);
}

static void test_malformed(void)
{
    printf("  malformed frames: INVALID with no body, the connection goes on\n");
    uint8_t *p;
    size_t len;
    uint64_t ids[3] = {ia, ib, iz};
    uint8_t sp[8 + sizeof ids];
    uint32_t n3 = 3, zero = 0, two = 2, one = 1;
    memcpy(sp, &n3, 4);
    memcpy(sp + 4, &zero, 4);
    memcpy(sp + 8, ids, sizeof ids);
    expect_status(TSDB_OP_STATS_SERIES, sp, sizeof sp - 1, TSDB_ERR_INVALID, "an id cut short");
    expect_status(TSDB_OP_STATS_SERIES, sp, 7, TSDB_ERR_INVALID, "shorter than the header");
    expect_status(TSDB_OP_STATS_SERIES, NULL, 0, TSDB_ERR_INVALID, "empty");
    uint8_t sp2[sizeof sp + 8];
    memcpy(sp2, sp, sizeof sp);
    memset(sp2 + sizeof sp, 0, 8);
    expect_status(TSDB_OP_STATS_SERIES, sp2, sizeof sp2, TSDB_ERR_INVALID, "an id too many");
    memcpy(sp + 4, &two, 4);
    expect_status(TSDB_OP_STATS_SERIES, sp, sizeof sp, TSDB_ERR_INVALID, "EXACT_ROWS is reserved");
    memcpy(sp + 4, &(uint32_t){4}, 4);
    expect_status(TSDB_OP_STATS_SERIES, sp, sizeof sp, TSDB_ERR_INVALID, "an unknown flag");
    memcpy(sp + 4, &zero, 4);
    memcpy(sp, &zero, 4);
    expect_status(TSDB_OP_STATS_SERIES, sp, 8, TSDB_ERR_INVALID, "n = 0");
    uint32_t over = TSDB_STATS_SERIES_MAX_IDS + 1;
    memcpy(sp, &over, 4);
    expect_status(TSDB_OP_STATS_SERIES, sp, sizeof sp, TSDB_ERR_INVALID, "n over the limit");
    memcpy(sp, &n3, 4);
    memcpy(sp + 4, &one, 4);
    uint32_t big = TSDB_STATS_SERIES_RESIDENT_MAX + 1;
    uint8_t *rp = calloc(1, 8 + (size_t)big * 8);
    memcpy(rp, &big, 4);
    memcpy(rp + 4, &one, 4);
    expect_status(TSDB_OP_STATS_SERIES, rp, 8 + (size_t)big * 8, TSDB_ERR_INVALID, "RESIDENT with 1,025 ids");
    free(rp);

    p = tenants_req(0, 0, 0, 0, 1, "", 0, &len);
    expect_status(TSDB_OP_STATS_TENANTS, p, len, TSDB_ERR_INVALID, "the reserved word");
    free(p);
    p = tenants_req(8, 0, 0, 0, 0, "", 0, &len);
    expect_status(TSDB_OP_STATS_TENANTS, p, len, TSDB_ERR_INVALID, "an unknown flag");
    free(p);
    p = tenants_req(0, TSDB_TSTATS_MAX_K + 1, 0, 0, 0, "", 0, &len);
    expect_status(TSDB_OP_STATS_TENANTS, p, len, TSDB_ERR_INVALID, "k over 256");
    free(p);
    p = tenants_req(0, 0, 2, 0, 0, "x", 1, &len);
    expect_status(TSDB_OP_STATS_TENANTS, p, len - 1, TSDB_ERR_INVALID, "a name cut short");
    expect_status(TSDB_OP_STATS_TENANTS, p, 15, TSDB_ERR_INVALID, "shorter than the header");
    uint8_t *q = malloc(len + 1);
    memcpy(q, p, len);
    q[len] = 0;
    expect_status(TSDB_OP_STATS_TENANTS, q, len + 1, TSDB_ERR_INVALID, "a byte after the last name");
    free(q);
    free(p);
    p = tenants_req(0, 0, 0, 0, 0, "", 0, &len);
    expect_status(TSDB_OP_STATS_TENANTS, p, len + 1, TSDB_ERR_INVALID, "n = 0 with a body");
    free(p);
}

/* A reply stops at 64 MB: 1,100 names of 64,000 bytes make 70 MB of rows (each echoes its name), the first reply
 * ends below the cap with a cursor, the client's loop collects the rest. */
/* A prefix with a NUL byte names no tenant (and no series): the row is NOTFOUND. Cut at the NUL it would have been the
 * numbers of "acme/". */
static void test_nul_prefix(void)
{
    printf("  a tenant name with a NUL byte is NOTFOUND, not the tenant before the NUL\n");
    static const char name[] = {'a', 'c', 'm', 'e', '/', 0, 'x'};
    uint8_t req[16 + 2 + sizeof name] = {0};
    uint16_t flags = TSDB_TSTATS_CALLER, k = 0, l = sizeof name;
    uint32_t n = 1;
    memcpy(req, &flags, 2);
    memcpy(req + 2, &k, 2);
    memcpy(req + 4, &n, 4);
    memcpy(req + 16, &l, 2);
    memcpy(req + 18, name, sizeof name);
    int fd = raw_connect();
    raw_send(fd, 7, TSDB_OP_STATS_TENANTS, req, sizeof req);
    uint8_t rest[256];
    size_t nr;
    CHECK(raw_reply(fd, rest, sizeof rest, &nr) == 0 && nr > TSDB_TSTATS_HEADER + 16);
    int32_t st;
    memcpy(&st, rest + TSDB_TSTATS_HEADER, 4);
    CHECK(st == TSDB_ERR_NOTFOUND);
    close(fd);
}

/* The 64 MB cap counts what a group of rows can take: a tenant with a long prefix on a server of many volumes writes
 * a row for each, and the reply must not pass the cap by the rows of its last group. */
static void test_cap_volumes(const char *dir, const char *tfile)
{
    printf("  the cap with a row per volume and long tenant names\n");
    enum { NV = 16, NT = 150, PL = 32000 }; /* a prefix is at most 32767 bytes */
    const char *plain[] = {"--no-auth", NULL};
    stop(start(dir, plain)); /* makes the database that the volumes are added to */
    char cmd[4 * PATH_MAX];
    for (int i = 1; i <= NV; i++) {
        snprintf(cmd, sizeof cmd, "%s-v%d", dir, i); /* beside the root: a volume inside it is refused */
        CHECK(mkdir(cmd, 0755) == 0);
        snprintf(cmd, sizeof cmd, "%s %s volume add %s-v%d </dev/null >>%s 2>&1", cli, dir, dir, i, clilog);
        CHECK(system(cmd) == 0);
    }
    FILE *f = fopen(tfile, "w");
    CHECK(f != NULL);
    for (int i = 0; i < NT; i++) {
        char *pre = malloc(PL + 1);
        memset(pre, 'a' + i % 26, PL);
        snprintf(pre, 8, "t%05d/", i); /* distinct, none the prefix of another */
        pre[strlen(pre)] = 'a' + i % 26;
        pre[PL] = 0;
        fprintf(f, "%s\n", pre);
        free(pre);
    }
    fclose(f);
    const char *extra[] = {"--no-auth", "--tenants", tfile, NULL};
    pid_t srv = start(dir, extra);
    int fd = raw_connect();
    size_t len;
    uint8_t *p = tenants_req(TSDB_TSTATS_VOLUMES, 0, 0, 0, 0, "", 0, &len);
    raw_send(fd, 9, TSDB_OP_STATS_TENANTS, p, (uint32_t)len);
    uint8_t head[64];
    size_t total = 0;
    CHECK(raw_reply(fd, head, sizeof head, &total) == 0);
    uint32_t count, next;
    memcpy(&count, head, 4);
    memcpy(&next, head + 8, 4);
    CHECK(total <= TSDB_TSTATS_MAX_REPLY && total > TSDB_TSTATS_MAX_REPLY / 2 && next != TSDB_TSTATS_DONE && count > 0);
    free(p);
    close(fd);
    stop(srv);
}

static void test_cap(tsdbc *c)
{
    printf("  the 64 MB cap of a reply, its cursor, and the client following it\n");
    const uint32_t n = 1100;
    const size_t nl = 64000;
    size_t len;
    uint8_t *p = tenants_req(0, 0, n, 0, 0, "", nl, &len);
    const size_t plen = len;
    int fd = raw_connect();
    raw_send(fd, 9, TSDB_OP_STATS_TENANTS, p, (uint32_t)plen);
    uint8_t head[64];
    size_t total;
    CHECK(raw_reply(fd, head, sizeof head, &total) == 0);
    uint32_t count, next;
    memcpy(&count, head, 4);
    memcpy(&next, head + 8, 4);
    CHECK(total <= TSDB_TSTATS_MAX_REPLY && total > TSDB_TSTATS_MAX_REPLY - 4096 - 262144);
    CHECK(count > 0 && count < n && next == count);
    /* the next page from the cursor */
    uint8_t *p2 = tenants_req(0, 0, n, next, 0, "", nl, &len);
    raw_send(fd, 10, TSDB_OP_STATS_TENANTS, p2, (uint32_t)len);
    uint32_t c2, n2;
    CHECK(raw_reply(fd, head, sizeof head, &total) == 0);
    memcpy(&c2, head, 4);
    memcpy(&n2, head + 8, 4);
    CHECK(c2 == n - count && n2 == TSDB_TSTATS_DONE);
    close(fd);
    free(p2);

    /* the client: every row, in order, though several replies */
    const char **names = malloc(n * sizeof *names);
    char *buf = malloc((size_t)n * (nl + 1));
    for (uint32_t i = 0; i < n; i++) {
        names[i] = buf + (size_t)i * (nl + 1);
        memset(buf + (size_t)i * (nl + 1), 'a' + (int)(i % 26), nl);
        buf[(size_t)i * (nl + 1) + nl] = 0;
    }
    tsdbc_tenant_result r;
    OK(tsdbc_tenant_stats(c, names, n, 0, 0, &r));
    CHECK(r.nrows == n);
    for (uint32_t i = 0; i < r.nrows && i < n; i++)
        if (r.rows[i].prefix_len != nl || strcmp(r.rows[i].prefix, names[i]) != 0 ||
            r.rows[i].status != TSDB_ERR_NOTFOUND) {
            CHECK(!"row i is name i, NOTFOUND");
            break;
        }
    tsdbc_tenant_result_free(&r);
    free(names);
    free(buf);

    /* CANCEL: the reply is the rows or STOPPED, and the connection stays usable */
    fd = raw_connect();
    raw_send(fd, 20, TSDB_OP_STATS_TENANTS, p, (uint32_t)plen);
    uint32_t cid = 20;
    raw_send(fd, 20, TSDB_OP_CANCEL, &cid, 4);
    int32_t st = raw_reply(fd, head, sizeof head, &total);
    CHECK(st == 0 || st == TSDB_ERR_STOPPED);
    if (st == TSDB_ERR_STOPPED)
        CHECK(total == 0);
    raw_send(fd, 21, TSDB_OP_STATS, NULL, 0);
    uint32_t id;
    size_t m;
    uint8_t rest[512];
    CHECK(raw_frame(fd, &id, rest, sizeof rest, &m, NULL, NULL) == 0 || id == 21);
    close(fd);

    /* CANCEL of a STATS_SERIES that waits behind a long one on the same connection: it is answered STOPPED, the
     * long one is answered whole, and the connection goes on */
    fd = raw_connect();
    enum { LONG = 65536 };
    size_t sl = 8 + (size_t)LONG * 8;
    uint8_t *sp = calloc(1, sl);
    uint32_t nn = LONG, nz = 0;
    memcpy(sp, &nn, 4);
    memcpy(sp + 4, &nz, 4);
    for (size_t i = 0; i < LONG; i++)
        memcpy(sp + 8 + i * 8, &ia, 8);
    uint8_t one[16];
    nn = 1;
    memcpy(one, &nn, 4);
    memcpy(one + 4, &nz, 4);
    memcpy(one + 8, &ia, 8);
    int stopped = 0;
    for (int attempt = 0; attempt < 5 && !stopped; attempt++) {
        raw_send(fd, 30, TSDB_OP_STATS_SERIES, sp, (uint32_t)sl);
        raw_send(fd, 31, TSDB_OP_STATS_SERIES, one, sizeof one);
        uint32_t c31 = 31;
        raw_send(fd, 32, TSDB_OP_CANCEL, &c31, 4);
        uint32_t rid = 0;
        size_t nr = 0;
        uint8_t big[64];
        int32_t s1 = raw_frame(fd, &rid, big, sizeof big, &nr, NULL, NULL);
        CHECK(rid == 30 && s1 == 0);
        int32_t s2 = raw_frame(fd, &rid, big, sizeof big, &nr, NULL, NULL);
        CHECK(rid == 31 && (s2 == 0 || s2 == TSDB_ERR_STOPPED));
        stopped = s2 == TSDB_ERR_STOPPED;
    }
    CHECK(stopped);
    free(sp);
    close(fd);
    free(p);
}

static void test_noauth_tenants(const char *dir, const char *tfile)
{
    printf("  --no-auth with --tenants FILE: the file's tenants, a change at SIGHUP\n");
    FILE *f = fopen(tfile, "w");
    CHECK(f != NULL);
    fputs("# the tenants of this server\nglobex/\nacme/\n", f);
    fclose(f);
    const char *extra[] = {"--no-auth", "--tenants", tfile, "--tenants-settle", "1", "--metrics-port", "auto", NULL};
    pid_t srv = start(dir, extra);
    tsdbc *c = NULL;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    tsdb_sid a, g, o;
    OK(tsdbc_series_create(c, "acme/a", TSDB_FLOAT32, NULL, &a));
    OK(tsdbc_series_create(c, "globex/g", TSDB_FLOAT32, NULL, &g));
    OK(tsdbc_series_create(c, "other/o", TSDB_FLOAT32, NULL, &o));
    insert_f32(c, a, 100);
    insert_f32(c, g, 50);
    insert_f32(c, o, 5);
    OK(tsdbc_flush(c));
    settle(c);
    tsdbc_tenant_result r;
    OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r));
    CHECK(r.nrows == 3 && r.rows[0].prefix_len == 0 && strcmp(r.rows[1].prefix, "acme/") == 0 &&
          strcmp(r.rows[2].prefix, "globex/") == 0);
    CHECK(row_of(&r, "acme/", TSDB_TSTATS_VOLUME_SUM)->st.rows == 100 &&
          row_of(&r, "globex/", TSDB_TSTATS_VOLUME_SUM)->st.rows == 50 &&
          row_of(&r, "", TSDB_TSTATS_VOLUME_SUM)->st.rows == 5);
    uint32_t gen = r.generation;
    uint64_t hash = r.list_hash;
    tsdbc_tenant_result_free(&r);

    /* every op is unchecked here, a nonexistent id is NOTFOUND */
    tsdb_sid ids[2] = {g, unknown_id};
    tsdb_series_stat s[2];
    OK(tsdbc_series_stats(c, ids, 2, 0, s));
    CHECK(s[0].status == 0 && s[0].rows == 50 && s[1].status == TSDB_ERR_NOTFOUND);

    /* SIGHUP with a new file: the list changes after the settle time, the generation and the hash move */
    f = fopen(tfile, "w");
    fputs("acme/\n", f);
    fclose(f);
    kill(srv, SIGHUP);
    bool changed = false;
    for (int i = 0; i < 150 && !changed; i++) {
        nap_ms(100);
        OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r));
        changed = r.nrows == 2 && r.generation != gen && r.list_hash != hash;
        tsdbc_tenant_result_free(&r);
    }
    CHECK(changed);
    settle(c);
    OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r)); /* globex/'s series are in (none) now */
    CHECK(r.nrows == 2 && row_of(&r, "", TSDB_TSTATS_VOLUME_SUM)->st.rows == 55 &&
          row_of(&r, "acme/", TSDB_TSTATS_VOLUME_SUM)->st.rows == 100);
    tsdbc_tenant_result_free(&r);
    test_metrics_no_tenants();

    tsdb_sid fa[2] = {a, g};
    OK(tsdbc_series_stats(c, fa, 2, 0, s));
    CHECK(s[0].rows == 100 && s[1].rows == 50);
    tsdbc_close(c);
    c = NULL;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    ia = a;
    ib = g;
    iz = o;
    test_malformed();
    test_nul_prefix();
    test_cap(c);
    tsdbc_close(c);
    stop(srv);
}

static void test_no_stats(const char *dir)
{
    printf("  --no-stats: STATS_TENANTS is INVALID\n");
    const char *extra[] = {"--no-auth", "--no-stats", NULL};
    pid_t srv = start(dir, extra);
    tsdbc *c = NULL;
    OK(tsdbc_connect("127.0.0.1", port, &c));
    tsdbc_tenant_result r;
    CHECK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r) == TSDB_ERR_INVALID);
    tsdbc_close(c);
    stop(srv);
}

/* ------------------------------------------------------------ fake servers */

static uint32_t fake_version = 15;
static int fake_mode, fake_others, fake_series_max_n, fake_requests;

static void fake_reply_body(int fd, uint32_t id, const void *b, size_t n)
{
    uint8_t h[13] = {0};
    uint32_t flen = (uint32_t)(9 + n);
    memcpy(h, &flen, 4);
    memcpy(h + 4, &id, 4);
    h[8] = TSDB_OP_REPLY;
    CHECK(write(fd, h, 13) == 13 && (n == 0 || write(fd, b, n) == (ssize_t)n));
}

static size_t fake_row(uint8_t *o, int k)
{
    memset(o, 0, 16 + 8 + 144);
    uint16_t pl = 2;
    memcpy(o + 12, &pl, 2);
    o[16] = 't';
    o[17] = (uint8_t)('0' + k);
    uint64_t series = 100 + (uint64_t)k;
    memcpy(o + 24, &series, 8);
    return 16 + 8 + 144;
}

static void *fake_server(void *arg)
{
    int fd = accept(*(int *)arg, NULL, NULL);
    uint8_t h[9];
    static uint8_t body[1 << 20];
    int tenant_calls = 0;
    while (fd >= 0 && read_full(fd, h, 9)) {
        uint32_t flen, id;
        memcpy(&flen, h, 4);
        memcpy(&id, h + 4, 4);
        if (flen < 5 || flen - 5 > sizeof body || !read_full(fd, body, flen - 5))
            break;
        if (h[8] == TSDB_OP_HELLO) {
            uint8_t hb[8] = {0};
            memcpy(hb, &fake_version, 4);
            fake_reply_body(fd, id, hb, sizeof hb);
            continue;
        }
        fake_others++;
        fake_requests++;
        if (h[8] == TSDB_OP_STATS_SERIES) {
            uint32_t n;
            memcpy(&n, body, 4);
            if ((int)n > fake_series_max_n)
                fake_series_max_n = (int)n;
            uint32_t flags;
            memcpy(&flags, body + 4, 4);
            /* two ids at most per reply: the work budget of a server that is busy */
            uint32_t done = n < 2 ? n : 2;
            uint8_t out[8 + 2 * TSDB_STATS_SERIES_REC] = {0};
            memcpy(out, &done, 4);
            for (uint32_t i = 0; i < done; i++) {
                uint64_t sid;
                memcpy(&sid, body + 8 + (size_t)i * 8, 8);
                memcpy(out + 8 + (size_t)i * TSDB_STATS_SERIES_REC + 8, &sid, 8); /* rows := the id asked */
            }
            fake_reply_body(fd, id, out, 8 + (size_t)done * TSDB_STATS_SERIES_REC);
        } else if (h[8] == TSDB_OP_STATS_TENANTS) {
            uint32_t cursor;
            memcpy(&cursor, body + 8, 4);
            tenant_calls++;
            uint32_t gen = 5, next = cursor == 0 ? 1 : TSDB_TSTATS_DONE;
            if (fake_mode == 1 && tenant_calls >= 2 && tenant_calls < 4)
                gen = 6; /* the list changed after the first page: the second page is of the new list */
            if (fake_mode == 1 && tenant_calls >= 4)
                gen = 6;
            if (fake_mode == 2 && tenant_calls >= 2)
                gen = 5 + (uint32_t)tenant_calls; /* it never settles */
            uint8_t out[TSDB_TSTATS_HEADER + 200] = {0};
            uint32_t count = 1;
            memcpy(out, &count, 4);
            memcpy(out + 8, &next, 4);
            memcpy(out + 12, &gen, 4);
            uint64_t hash = 77 + gen;
            memcpy(out + 16, &hash, 8);
            size_t rl = fake_row(out + TSDB_TSTATS_HEADER, (int)cursor);
            fake_reply_body(fd, id, out, TSDB_TSTATS_HEADER + rl);
        } else {
            uint8_t none[1] = {0};
            int32_t st = TSDB_ERR_INVALID;
            (void)none;
            uint8_t hh[13] = {0};
            uint32_t fl = 9;
            memcpy(hh, &fl, 4);
            memcpy(hh + 4, &id, 4);
            hh[8] = TSDB_OP_REPLY;
            memcpy(hh + 9, &st, 4);
            CHECK(write(fd, hh, 13) == 13);
        }
    }
    if (fd >= 0)
        close(fd);
    return NULL;
}

static tsdbc *fake_connect(pthread_t *th, int *lfd, uint32_t version)
{
    *lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET};
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    socklen_t sl = sizeof sa;
    CHECK(bind(*lfd, (struct sockaddr *)&sa, sizeof sa) == 0 && listen(*lfd, 1) == 0 &&
          getsockname(*lfd, (struct sockaddr *)&sa, &sl) == 0);
    fake_version = version;
    fake_others = fake_series_max_n = fake_requests = 0;
    CHECK(pthread_create(th, NULL, fake_server, lfd) == 0);
    tsdbc *c = NULL;
    CHECK(tsdbc_connect("127.0.0.1", ntohs(sa.sin_port), &c) == 0 && c);
    return c;
}

/* The 'rows' field of a fake record is the id asked: a client that asks again for ids[done..] puts every record in
 * its place. */
static void test_fake(void)
{
    printf("  a busy server (two ids a reply), a list that changes between pages, a server of protocol 14\n");
    pthread_t th;
    int lfd;
    tsdbc *c = fake_connect(&th, &lfd, 15);
    if (c) {
        enum { N = 2100 };
        static tsdb_sid ids[N];
        static tsdb_series_stat out[N];
        for (int i = 0; i < N; i++)
            ids[i] = TSDB_SID(0, (uint32_t)i + 1);
        OK(tsdbc_series_stats(c, ids, N, TSDB_SSTAT_REQ_RESIDENT, out));
        bool ok = true;
        for (int i = 0; i < N; i++)
            ok = ok && out[i].rows == ids[i];
        CHECK(ok);
        CHECK(fake_series_max_n <= (int)TSDB_STATS_SERIES_RESIDENT_MAX && fake_requests == N / 2);

        tsdbc_tenant_result r;
        fake_mode = 0;
        OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r));
        CHECK(r.nrows == 2 && strcmp(r.rows[0].prefix, "t0") == 0 && strcmp(r.rows[1].prefix, "t1") == 0 &&
              r.rows[0].st.series == 100 && r.rows[1].st.series == 101 && r.generation == 5);
        tsdbc_tenant_result_free(&r);
        fake_mode = 1; /* the second reply of this call is of generation 6: the call starts over */
        OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r));
        CHECK(r.nrows == 2 && r.generation == 6 && r.list_hash == 77 + 6);
        tsdbc_tenant_result_free(&r);
        fake_mode = 2; /* a list that never rests: the call gives up (IO), freeing what it had */
        CHECK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r) == TSDB_ERR_IO && r.nrows == 0 && r.rows == NULL);
        fake_mode = 0;
        OK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r)); /* the connection is fine */
        tsdbc_tenant_result_free(&r);
        tsdbc_close(c);
    }
    pthread_join(th, NULL);
    close(lfd);

    c = fake_connect(&th, &lfd, 14);
    if (c) {
        tsdb_sid id = TSDB_SID(0, 1);
        tsdb_series_stat s;
        tsdbc_tenant_result r;
        CHECK(tsdbc_series_stats(c, &id, 1, 0, &s) == TSDB_ERR_INVALID);
        CHECK(tsdbc_tenant_stats(c, NULL, 0, 0, 0, &r) == TSDB_ERR_INVALID);
        tsdbc_close(c);
    }
    pthread_join(th, NULL);
    close(lfd);
    CHECK(fake_others == 0); /* nothing but the HELLO went out */
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    printf("test_stats_net (%s)\n", getenv("TSDBD_IO") ? getenv("TSDBD_IO") : "default io");
    char dir[PATH_MAX + 16];
    snprintf(dir, sizeof dir, "%s/tsdbd-statsnet-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    ah_setup(dir);
    test_fake();

    CHECK(cli_user(dir, "rootpw", "add root --superuser") == 0);
    CHECK(cli_user(dir, "adminpw", "add acme/admin") == 0);
    CHECK(cli_user(dir, NULL, "grant acme/admin admin prefix:acme/") == 0);
    CHECK(cli_user(dir, "readerpw", "add acme/reader") == 0);
    CHECK(cli_user(dir, NULL, "grant acme/reader read prefix:acme/") == 0);
    CHECK(cli_user(dir, "viewpw", "add viewer") == 0);
    CHECK(cli_user(dir, NULL, "grant viewer read all") == 0);
    if (failures) {
        fprintf(stderr, "test_stats_net: the CLI could not make the users (%s, log %s)\n", cli, clilog);
        return 1;
    }
    const char *extra[] = {"--metrics-port", "auto", NULL};
    pid_t srv = start(dir, extra);
    printf(" login on\n");
    tsdbc *root = login_as("root", "rootpw");
    make_data(root);
    settle(root);
    test_series_root(root);
    test_tenants_root(root);
    test_rights();
    test_metrics_no_tenants();
    tsdbc_close(root);
    stop(srv);
    test_oracle(dir);
    printf(" login on, the server started again\n");
    test_phase2(dir);

    printf(" no login\n");
    char d2[PATH_MAX + 32], tf[PATH_MAX + 32];
    snprintf(d2, sizeof d2, "%s-noauth", dir);
    snprintf(tf, sizeof tf, "%s-tenants", dir);
    test_noauth_tenants(d2, tf);
    snprintf(d2, sizeof d2, "%s-nostats", dir);
    test_no_stats(d2);
    snprintf(d2, sizeof d2, "%s-capvol", dir);
    snprintf(tf, sizeof tf, "%s-capvol-tenants", dir);
    test_cap_volumes(d2, tf);

    if (failures) {
        fprintf(stderr, "test_stats_net: %d failures (server log %s)\n", failures, srvlog);
        return 1;
    }
    char cmd[8 * PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s-noauth' '%s-nostats' '%s-tenants' '%s-capvol' '%s-capvol-tenants' '%s-capvol-v'* '%s' '%s'", dir, dir, dir, dir, dir, dir, dir, srvlog,
             clilog);
    if (system(cmd) != 0)
        fprintf(stderr, "test_stats_net: could not remove %s\n", dir);
    printf("test_stats_net: ok\n");
    return 0;
}
