/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit, fault-injection (outagefs EIO), and concurrency tests for young-part and .ec scrub
 * (src/scrub.[ch], src/part.c part_scrub_pread; R12, r12-young).
 *
 * Usage: test_scrub [--mutant youngskipgranule|youngmmap] [SCRATCH]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "ecpart.h"
#include "scrub.h"
#include "settle.h"
#include "tier.h"
#include "watchdog.h"
#ifdef __linux__
#include "outagefs_ctl.h"
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
static char scratch_base[PATH_MAX / 2];

#define CHECK(cond, ...)                                                                                               \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "FAIL %s:%d: %s: ", __FILE__, __LINE__, #cond);                                            \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fputc('\n', stderr);                                                                                       \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static void rm_rf(const char *path)
{
    struct stat st;
    if (lstat(path, &st) != 0)
        return;
    if (!S_ISDIR(st.st_mode)) {
        (void)unlink(path);
        return;
    }
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
                continue;
            char sub[PATH_MAX];
            int n = snprintf(sub, sizeof sub, "%s/%s", path, e->d_name);
            if (n > 0 && (size_t)n < sizeof sub)
                rm_rf(sub);
        }
        closedir(d);
    }
    (void)rmdir(path);
}

static void mk_fresh(const char *tag, char *out, size_t cap)
{
    static int seq;
    snprintf(out, cap, "%s/scrub-%d-%s-%d", scratch_base, (int)getpid(), tag, seq++);
    rm_rf(out);
    if (mkdir(out, 0755) != 0) {
        perror("mkdir");
        exit(1);
    }
}

static int flip_byte(const char *dir, const char *name, uint64_t off)
{
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return -1;
    uint8_t b = 0;
    if (pread(fd, &b, 1, (off_t)off) != 1) {
        close(fd);
        return -1;
    }
    b ^= 0x5au;
    int rc = pwrite(fd, &b, 1, (off_t)off) == 1 ? 0 : -1;
    close(fd);
    return rc;
}

static int write_batch(eng *db, uint32_t sid, int64_t t0, int n, float base)
{
    int64_t *ts = malloc((size_t)n * sizeof *ts);
    float *vs = malloc((size_t)n * sizeof *vs);
    if (!ts || !vs) {
        free(ts);
        free(vs);
        return TSDB_ERR_NOMEM;
    }
    for (int i = 0; i < n; i++) {
        ts[i] = t0 + (int64_t)i * 1000;
        vs[i] = base + (float)i * 0.25f;
    }
    int rc = eng_insert_series(db, TSDB_FLOAT32, sid, ts, vs, (size_t)n);
    free(ts);
    free(vs);
    return rc;
}

static void run_full_pass(eng *db, uint64_t budget, uint32_t *parts_done, uint32_t *damaged, uint64_t *bytes,
                          int *last_err)
{
    scrub_cursor cur = {0};
    scrub_report rep;
    uint32_t pd = 0, dmg = 0;
    uint64_t by = 0;
    int lerr = 0;
    for (int guard = 0; guard < 100000; guard++) {
        int rc = eng_scrub_step(db, budget, &cur, &rep);
        CHECK(rc == 0, "eng_scrub_step returned %d", rc);
        if (rc != 0)
            break;
        by += rep.bytes;
        pd += rep.parts_done;
        if (rep.damaged) {
            dmg++;
            lerr = rep.err;
        }
        if (rep.pass_done)
            break;
    }
    if (parts_done)
        *parts_done = pd;
    if (damaged)
        *damaged = dmg;
    if (bytes)
        *bytes = by;
    if (last_err)
        *last_err = lerr;
}

/* Case 1 & 2: flushed + merged parts, multi-granule last-granule payload flip, middle granule flip, index flip,
 * footer flip, header flip, and priority re-scrub order. */
static void test_parts_and_flips(void)
{
    char dir[PATH_MAX];
    mk_fresh("parts", dir, sizeof dir);

    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64;
    o.merge_threads = 0;
    o.verify_checksums = 0; /* prove scrub catches damage even with verify_checksums off */
    o.fsync = 0;

    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0, "eng_open");
    uint32_t s0 = 0, s1 = 0;
    CHECK(eng_series(db, "m.a", TSDB_FLOAT32, &s0) == 0, "series s0");
    CHECK(eng_series(db, "m.b", TSDB_FLOAT32, &s1) == 0, "series s1");

    /* Two flushed parts in day 0 -> merge into one merged part with >= 6 granules. */
    CHECK(write_batch(db, s0, 1000, 300, 1.0f) == 0, "batch 1");
    CHECK(mt_flush(db) == 0, "flush 1");
    CHECK(write_batch(db, s1, 1000, 300, 2.0f) == 0, "batch 2");
    CHECK(mt_flush(db) == 0, "flush 2");
    CHECK(eng_optimize(db) == 0, "optimize merge");

    /* One unmerged flushed part in day 1 with >= 5 granules (320 rows / 64 = 5 granules). */
    const int64_t day1 = 86400000LL;
    CHECK(write_batch(db, s0, day1 + 1000, 320, 3.0f) == 0, "batch 3");
    CHECK(mt_flush(db) == 0, "flush 3");

    pthread_mutex_lock(&db->lock);
    size_t nparts = db->nparts;
    CHECK(nparts == 2, "expected 2 parts (1 merged + 1 flushed), got %zu", nparts);
    part *p_merged = nparts > 0 ? db->parts[0] : NULL;
    part *p_flushed = nparts > 1 ? db->parts[1] : NULL;
    CHECK(p_merged && p_merged->level > 0 && p_merged->ng >= 4, "merged part ng=%u level=%u",
          p_merged ? p_merged->ng : 0, p_merged ? p_merged->level : 0);
    CHECK(p_flushed && p_flushed->level == 0 && p_flushed->ng >= 4, "flushed part ng=%u level=%u",
          p_flushed ? p_flushed->ng : 0, p_flushed ? p_flushed->level : 0);
    char name_merged[96] = "", name_flushed[96] = "";
    uint64_t last_g_off = 0, last_g_len = 0;
    if (p_merged)
        snprintf(name_merged, sizeof name_merged, "%s", p_merged->name);
    if (p_flushed) {
        snprintf(name_flushed, sizeof name_flushed, "%s", p_flushed->name);
        granule g = {0};
        gx_load(p_flushed, p_flushed->ng - 1, GW_LOC, &g);
        last_g_off = g.off;
        last_g_len = (uint64_t)g.ts_len + g.val_len;
    }
    pthread_mutex_unlock(&db->lock);

    /* Clean pass with tiny budget (128 bytes) to exercise multi-step granule resumption. */
    uint32_t pd = 0, dmg = 0;
    uint64_t bytes = 0;
    int lerr = 0;
    run_full_pass(db, 128, &pd, &dmg, &bytes, &lerr);
    CHECK(pd == 2 && dmg == 0 && bytes > 0, "clean pass pd=%u dmg=%u bytes=%" PRIu64, pd, dmg, bytes);
    CHECK(atomic_load(&db->st_broken_parts) == 0, "clean st_broken_parts");

    /* Flip a byte in the LAST granule payload of p_flushed -> MUST be caught (catches youngskipgranule!). */
    CHECK(last_g_len > 0 && flip_byte(dir, name_flushed, last_g_off + last_g_len / 2) == 0, "flip last granule");
    run_full_pass(db, 128, &pd, &dmg, &bytes, &lerr);
    CHECK(dmg == 1 && lerr == TSDB_ERR_CORRUPT, "last granule flip: dmg=%u err=%d", dmg, lerr);
    CHECK(atomic_load(&p_flushed->damaged), "p_flushed marked damaged");
    CHECK(atomic_load(&db->st_broken_parts) == 1, "st_broken_parts == 1, got %" PRIu64,
          (uint64_t)atomic_load(&db->st_broken_parts));

    /* Priority re-scrub must inspect name_flushed BEFORE name_merged even though name_merged sorts first. */
    scrub_cursor pcur = {0};
    snprintf(pcur.priority, sizeof pcur.priority, "%s", name_flushed);
    scrub_report prep;
    CHECK(eng_scrub_step(db, 0, &pcur, &prep) == 0, "priority step");
    CHECK(strcmp(prep.part_name, name_flushed) == 0 && prep.damaged == 1 && !prep.newly_damaged,
          "priority part=%s dmg=%u newly=%d", prep.part_name, prep.damaged, prep.newly_damaged);

    /* Vanished priority part (e.g. merged away after restart): must report skipped with the vanished name and NOT
     * silently fall through to scrub a clean part. */
    scrub_cursor vcur = {0};
    snprintf(vcur.priority, sizeof vcur.priority, "p0_999_999_0.part");
    scrub_report vrep;
    CHECK(eng_scrub_step(db, 0, &vcur, &vrep) == 0, "vanished priority step");
    CHECK(vrep.skipped && vrep.parts_done == 0 && vrep.damaged == 0 && strcmp(vrep.part_name, "p0_999_999_0.part") == 0 &&
              vcur.priority[0] == '\0',
          "vanished priority: skipped=%d pd=%u dmg=%u name=%s", vrep.skipped, vrep.parts_done, vrep.damaged,
          vrep.part_name);

    /* Flip a byte in a middle granule of the merged part -> caught & marked. */
    granule gm = {0};
    gx_load(p_merged, p_merged->ng / 2, GW_LOC, &gm);
    CHECK(flip_byte(dir, name_merged, gm.off + ((uint64_t)gm.ts_len + gm.val_len) / 2) == 0, "flip merged granule");
    run_full_pass(db, 0, &pd, &dmg, &bytes, &lerr);
    CHECK(dmg == 2 && lerr == TSDB_ERR_CORRUPT, "both parts damaged: dmg=%u err=%d", dmg, lerr);
    CHECK(atomic_load(&p_merged->damaged), "p_merged marked damaged");
    CHECK(atomic_load(&db->st_broken_parts) == 2, "st_broken_parts == 2");
    CHECK(eng_close(db) == 0, "eng_close parts");

    /* Separate checks for index flip, footer flip, and header flip (each on a fresh clean DB). */
    for (int kind = 0; kind < 3; kind++) {
        char d2[PATH_MAX];
        mk_fresh(kind == 0 ? "idx" : kind == 1 ? "ftr" : "hdr", d2, sizeof d2);
        eng *db2 = NULL;
        CHECK(eng_open(d2, &o, &db2) == 0, "eng_open kind=%d", kind);
        uint32_t sx = 0;
        CHECK(eng_series(db2, "k.s", TSDB_FLOAT32, &sx) == 0, "series kind=%d", kind);
        CHECK(write_batch(db2, sx, 1000, 200, 5.0f) == 0 && mt_flush(db2) == 0, "flush kind=%d", kind);
        pthread_mutex_lock(&db2->lock);
        part *p = db2->nparts ? db2->parts[0] : NULL;
        char pname[96] = "";
        uint64_t flip_off = 0;
        if (p) {
            snprintf(pname, sizeof pname, "%s", p->name);
            if (kind == 0)
                flip_off = p->index_off + p->index_len / 2;
            else if (kind == 1)
                flip_off = p->map_len - sizeof(part_footer) + 4;
            else
                flip_off = 16; /* header wal_seq/version bytes */
        }
        pthread_mutex_unlock(&db2->lock);
        CHECK(p && flip_byte(d2, pname, flip_off) == 0, "flip kind=%d off=%" PRIu64, kind, flip_off);
        run_full_pass(db2, 0, &pd, &dmg, &bytes, &lerr);
        CHECK(dmg == 1 && lerr == TSDB_ERR_CORRUPT && p && atomic_load(&p->damaged),
              "kind=%d: dmg=%u err=%d marked=%d", kind, dmg, lerr, p ? (int)atomic_load(&p->damaged) : 0);
        CHECK(eng_close(db2) == 0, "close kind=%d", kind);
    }
    printf("  parts_and_flips: ok\n");
}

/* Case 3: .ec part scrub (record, trailer, source footer flips) under TSDB_TEST_TIER. */
static void test_ec_scrub(void)
{
#ifndef TSDB_EC
    printf("  ec_scrub: skipped (built without TSDB_EC)\n");
#else
    for (int kind = 0; kind < 3; kind++) {
        char root[PATH_MAX - 64], dbdir[PATH_MAX], st[3][PATH_MAX];
        mk_fresh(kind == 0 ? "ecrec" : kind == 1 ? "ectr" : "ecftr", root, sizeof root);
        snprintf(dbdir, sizeof dbdir, "%s/db", root);
        CHECK(mkdir(dbdir, 0755) == 0, "mkdir db");
        for (int i = 0; i < 3; i++) {
            snprintf(st[i], sizeof st[i], "%s/st%d", root, i);
            CHECK(mkdir(st[i], 0755) == 0, "mkdir st%d", i);
        }
        char spec[4 * PATH_MAX + 128];
        snprintf(spec, sizeof spec, "k=2,m=1,dirs=%s:%s:%s,grace_s=0,coarse_s=0,eager=1", st[0], st[1], st[2]);
        setenv("TSDB_TEST_TIER", spec, 1);
        setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);

        tsdb_options o;
        tsdb_options_init(&o);
        o.granule_rows = 64;
        o.merge_threads = 0;
        o.create_volumes = 0;
        o.fsync = 0;

        tier *tr = NULL;
        eng *db = NULL;
        CHECK(tier_open(dbdir, &o, &tr) == 0, "tier_open");
        tier_want(tr, 0);
        CHECK(eng_open(dbdir, &o, &db) == 0, "eng_open ec");
        tier_want(NULL, 0);
        unsetenv("TSDB_TEST_TIER");
        unsetenv("TSDB_TEST_TIER_I_KNOW");

        uint32_t sid = 0;
        CHECK(eng_series(db, "ec.s0", TSDB_FLOAT32, &sid) == 0, "ec series");
        CHECK(write_batch(db, sid, 1000, 256, 10.0f) == 0 && mt_flush(db) == 0, "ec flush");
        pthread_mutex_lock(&db->lock);
        part *sp = db->nparts ? db->parts[0] : NULL;
        int64_t skey = sp ? sp->partition : 0;
        if (sp)
            part_ref(sp);
        pthread_mutex_unlock(&db->lock);
        CHECK(sp != NULL, "young part before settle");
        size_t settled = 0;
        if (sp) {
            CHECK(eng_settle(db, skey, &sp, 1, &settled) == 0 && settled == 1, "eng_settle settled part");
            part_unref(sp);
        }

        pthread_mutex_lock(&db->lock);
        part *ecp = NULL;
        for (size_t i = 0; i < db->nparts; i++)
            if (atomic_load(&db->parts[i]->ec) != NULL)
                ecp = db->parts[i];
        char ecname[96] = "";
        uint64_t flip_off = 0;
        if (ecp) {
            struct ecpart *e = atomic_load(&ecp->ec);
            snprintf(ecname, sizeof ecname, "%s", ecp->name);
            if (kind == 0)
                flip_off = e->rec_off + EC_REC_OID + 2; /* inside ec_record */
            else if (kind == 1)
                flip_off = ecp->map_len - EC_TRAILER_BYTES + EC_TR_REC_HASH; /* inside ec trailer */
            else
                flip_off = e->rec_off - sizeof(part_footer) + 8; /* inside source footer */
        }
        pthread_mutex_unlock(&db->lock);
        CHECK(ecp != NULL, "found settled .ec part");

        /* Clean scrub first */
        uint32_t pd = 0, dmg = 0;
        uint64_t bytes = 0;
        int lerr = 0;
        run_full_pass(db, 0, &pd, &dmg, &bytes, &lerr);
        CHECK(pd == 1 && dmg == 0 && bytes > 0, "clean ec scrub pd=%u dmg=%u", pd, dmg);

        /* Flip byte -> must be caught & marked */
        CHECK(ecp && flip_byte(dbdir, ecname, flip_off) == 0, "flip ec kind=%d", kind);
        run_full_pass(db, 0, &pd, &dmg, &bytes, &lerr);
        CHECK(dmg == 1 && lerr == TSDB_ERR_CORRUPT && ecp && atomic_load(&ecp->damaged),
              "ec flip kind=%d: dmg=%u err=%d marked=%d", kind, dmg, lerr, ecp ? (int)atomic_load(&ecp->damaged) : 0);

        CHECK(eng_close(db) == 0, "close ec db");
        tier_put(tr);
    }
    printf("  ec_scrub: ok\n");
#endif
}

/* Case 4: outagefs EIO (no SIGBUS, damage reported) + truncated file (pread -> CORRUPT, mmap -> SIGBUS). */
static void test_eio_no_sigbus(void)
{
#ifdef __linux__
    const char *ctl = getenv("OUTAGEFS_CTL");
    const char *edir = outagefs_dir("eio");
    if (ctl && edir) {
        char dbdir[PATH_MAX];
        snprintf(dbdir, sizeof dbdir, "%s/db", edir);
        rm_rf(dbdir);
        CHECK(mkdir(dbdir, 0755) == 0, "mkdir eio db");

        tsdb_options o;
        tsdb_options_init(&o);
        o.granule_rows = 64;
        o.merge_threads = 0;
        o.fsync = 0;
        eng *db = NULL;
        CHECK(eng_open(dbdir, &o, &db) == 0, "eng_open eio");
        uint32_t s0 = 0;
        CHECK(eng_series(db, "eio.s", TSDB_FLOAT32, &s0) == 0, "eio series");
        CHECK(write_batch(db, s0, 1000, 320, 4.0f) == 0 && mt_flush(db) == 0, "eio flush");

        char cmd[PATH_MAX + 32], out[256];
        snprintf(cmd, sizeof cmd, "eio %s", edir);
        CHECK(outagefs_cmd(ctl, cmd, out, sizeof out) == 0, "outagefs eio: %s", out);

        uint32_t pd = 0, dmg = 0;
        uint64_t bytes = 0;
        int lerr = 0;
        run_full_pass(db, 0, &pd, &dmg, &bytes, &lerr);
        CHECK(dmg == 1 && lerr == TSDB_ERR_IO, "outagefs EIO: dmg=%u err=%d (want 1, TSDB_ERR_IO)", dmg, lerr);
        CHECK(atomic_load(&db->st_broken_parts) == 1, "eio st_broken_parts == 1");

        snprintf(cmd, sizeof cmd, "heal %s", edir);
        CHECK(outagefs_cmd(ctl, cmd, out, sizeof out) == 0, "outagefs heal: %s", out);
        CHECK(eng_close(db) == 0, "close eio db");
    } else {
        printf("  outagefs_eio: skipped (OUTAGEFS_CTL not active)\n");
    }
#endif

    /* Truncated part behind the mapping: pread sees CORRUPT (no SIGBUS); youngmmap dies with SIGBUS! */
    char tdir[PATH_MAX - 128];
    mk_fresh("trunc", tdir, sizeof tdir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64;
    o.merge_threads = 0;
    o.fsync = 0;
    eng *db = NULL;
    CHECK(eng_open(tdir, &o, &db) == 0, "eng_open trunc");
    uint32_t s0 = 0;
    CHECK(eng_series(db, "tr.s", TSDB_FLOAT32, &s0) == 0, "trunc series");
    CHECK(write_batch(db, s0, 1000, 4096, 1.0f) == 0 && mt_flush(db) == 0, "trunc flush");

    pthread_mutex_lock(&db->lock);
    part *p = db->nparts ? db->parts[0] : NULL;
    char pname[96] = "";
    if (p)
        snprintf(pname, sizeof pname, "%s", p->name);
    pthread_mutex_unlock(&db->lock);
    CHECK(p != NULL, "trunc part present");

    char fpath[PATH_MAX];
    snprintf(fpath, sizeof fpath, "%s/%s", tdir, pname);
    CHECK(truncate(fpath, 0) == 0, "truncate part to 0");

    uint32_t pd = 0, dmg = 0;
    uint64_t bytes = 0;
    int lerr = 0;
    run_full_pass(db, 0, &pd, &dmg, &bytes, &lerr);
    CHECK(dmg == 1 && lerr == TSDB_ERR_CORRUPT && p && atomic_load(&p->damaged),
          "truncated part: dmg=%u err=%d", dmg, lerr);
    CHECK(eng_close(db) == 0, "close trunc db");
    printf("  eio_no_sigbus: ok\n");
}

/* Case 5: concurrent inserts, merges, and drops while scrubbing (verified under ASan+UBSan and TSan). */
struct conc_ctx {
    eng *db;
    uint32_t sid;
    atomic_bool stop;
    atomic_uint_fast64_t scrub_parts;
    atomic_uint_fast64_t scrub_skipped;
    atomic_uint_fast64_t scrub_damaged;
    atomic_int scrub_err;
};

static void *writer_fn(void *arg)
{
    struct conc_ctx *c = arg;
    const int64_t day = 86400000LL;
    for (int round = 0; round < 60 && !atomic_load(&c->stop); round++) {
        int64_t t0 = (int64_t)(round / 3) * day + (int64_t)(round % 3) * 100000 + 1000;
        if (write_batch(c->db, c->sid, t0, 160, (float)round) != 0)
            break;
        if (mt_flush(c->db) != 0)
            break;
        usleep(5000);
    }
    return NULL;
}

static void *dropper_fn(void *arg)
{
    struct conc_ctx *c = arg;
    const int64_t day = 86400000LL;
    for (int d = 1; d <= 15 && !atomic_load(&c->stop); d++) {
        usleep(20000);
        (void)eng_drop_before(c->db, (int64_t)d * day);
    }
    return NULL;
}

static void *scrubber_fn(void *arg)
{
    struct conc_ctx *c = arg;
    scrub_cursor cur = {0};
    scrub_report rep;
    while (!atomic_load(&c->stop)) {
        int rc = eng_scrub_step(c->db, 2048, &cur, &rep);
        if (rc != 0) {
            atomic_store(&c->scrub_err, rc);
            break;
        }
        if (rep.parts_done)
            atomic_fetch_add(&c->scrub_parts, rep.parts_done);
        if (rep.skipped)
            atomic_fetch_add(&c->scrub_skipped, 1);
        if (rep.damaged)
            atomic_fetch_add(&c->scrub_damaged, rep.damaged);
        if (rep.pass_done)
            usleep(1000);
    }
    return NULL;
}

static void test_concurrent_scrub(void)
{
    char dir[PATH_MAX];
    mk_fresh("conc", dir, sizeof dir);

    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64;
    o.merge_threads = 2;
    o.merge_idle_s = 1;
    o.fsync = 0;

    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0, "eng_open conc");
    uint32_t sid = 0;
    CHECK(eng_series(db, "conc.s", TSDB_FLOAT32, &sid) == 0, "conc series");
    /* Seed an initial part so scrubber has work immediately */
    CHECK(write_batch(db, sid, 1000, 200, 0.5f) == 0 && mt_flush(db) == 0, "seed conc");

    struct conc_ctx ctx = {.db = db, .sid = sid};
    atomic_init(&ctx.stop, false);
    pthread_t tw, td, ts1, ts2;
    CHECK(pthread_create(&tw, NULL, writer_fn, &ctx) == 0, "create writer");
    CHECK(pthread_create(&td, NULL, dropper_fn, &ctx) == 0, "create dropper");
    CHECK(pthread_create(&ts1, NULL, scrubber_fn, &ctx) == 0, "create scrubber 1");
    CHECK(pthread_create(&ts2, NULL, scrubber_fn, &ctx) == 0, "create scrubber 2");

    pthread_join(tw, NULL);
    pthread_join(td, NULL);
    atomic_store(&ctx.stop, true);
    pthread_join(ts1, NULL);
    pthread_join(ts2, NULL);

    CHECK(atomic_load(&ctx.scrub_err) == 0, "conc scrub err=%d", atomic_load(&ctx.scrub_err));
    CHECK(atomic_load(&ctx.scrub_damaged) == 0, "conc false damage=%" PRIu64,
          (uint64_t)atomic_load(&ctx.scrub_damaged));
    CHECK(atomic_load(&ctx.scrub_parts) > 0, "conc scrubbed %" PRIu64 " parts",
          (uint64_t)atomic_load(&ctx.scrub_parts));
    CHECK(atomic_load(&db->st_broken_parts) == 0, "conc st_broken_parts == 0");
    CHECK(eng_close(db) == 0, "close conc db");
    printf("  concurrent_scrub: ok (parts=%" PRIu64 ", skipped=%" PRIu64 ")\n",
           (uint64_t)atomic_load(&ctx.scrub_parts), (uint64_t)atomic_load(&ctx.scrub_skipped));
}

int main(int argc, char **argv)
{
    const char *mut = NULL;
    const char *dir_arg = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mut = argv[++i];
        else if (argv[i][0] != '-')
            dir_arg = argv[i];
        else {
            fprintf(stderr, "usage: test_scrub [--mutant youngskipgranule|youngmmap] [SCRATCH]\n");
            return 2;
        }
    }

    const char *base = dir_arg && *dir_arg ? dir_arg : getenv("TMPDIR");
    if (!base || !*base)
        base = "/tmp";
    if (mkdir(base, 0755) != 0 && errno != EEXIST) {
        perror("mkdir base");
        return 1;
    }

#ifdef __linux__
    /* Re-exec under liboutagefs.so so Case 4 tests real EIO interception on openat/pread. */
    if (!getenv("OUTAGEFS_CTL")) {
        if (!getenv("OUTAGEFS_LIB")) {
            char sib[PATH_MAX];
            const char *sl = strrchr(argv[0], '/');
            if (sl) {
                snprintf(sib, sizeof sib, "%.*s/liboutagefs.so", (int)(sl - argv[0]), argv[0]);
                if (access(sib, R_OK) == 0)
                    setenv("OUTAGEFS_LIB", sib, 1);
            }
        }
        const char *lib = getenv("OUTAGEFS_LIB") ? getenv("OUTAGEFS_LIB") : "build/liboutagefs.so";
        if (access(lib, R_OK) == 0) {
            const char *names[] = {"eio"};
            if (outagefs_reexec(argv, base, names, 1) != 0)
                return 1;
        }
    }
#endif

    th_watchdog("test_scrub", 300);

    if (mut) {
        if (strcmp(mut, "youngskipgranule") == 0)
            scrub_test_mutant = SCRUB_MUT_SKIPGRANULE;
        else if (strcmp(mut, "youngmmap") == 0)
            scrub_test_mutant = SCRUB_MUT_MMAP;
        else {
            fprintf(stderr, "test_scrub: unknown mutant %s\n", mut);
            return 2;
        }
    }

    snprintf(scratch_base, sizeof scratch_base, "%s/tsdb_scrub.%d", base, (int)getpid());
    rm_rf(scratch_base);
    if (mkdir(scratch_base, 0755) != 0) {
        perror("mkdir scratch");
        return 1;
    }

    test_parts_and_flips();
    test_ec_scrub();
    test_eio_no_sigbus();
    test_concurrent_scrub();

    rm_rf(scratch_base);
#ifdef __linux__
    const char *edir = outagefs_dir("eio");
    if (edir) {
        char parent[PATH_MAX];
        snprintf(parent, sizeof parent, "%s", edir);
        char *sl = strrchr(parent, '/');
        if (sl) {
            *sl = '\0';
            rm_rf(parent);
        }
    }
#endif

    if (failures) {
        fprintf(stderr, "test_scrub: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_scrub: ok\n");
    return 0;
}
