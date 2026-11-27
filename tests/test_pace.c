/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit tests for R9 rebuild/balance pacing (server/pace.[ch]) and the 1-min/1-hr p99 halving window
 * (server/metrics.[ch]; docs/design-cluster-data.md E13, design-cluster.md §8.4; R9 plan D18; track r9-pace).
 *
 * Supports `--mutant pacefifo|nohalve` (and TSDB_PACE_MUTANT) for `make pace-mutants`.
 */
#include "../src/cvol.h"
#include "metrics.h"
#include "pace.h"
#include "tsdb.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int active_mutant = PACE_MUT_NONE;
static int failures = 0;
static const char *scratch_dir = "/tmp";

#define CHECK(cond, ...)                                                                                               \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "FAIL %s:%d: " #cond " ", __FILE__, __LINE__);                                             \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fprintf(stderr, "\n");                                                                                     \
            failures++;                                                                                                \
            return;                                                                                                    \
        }                                                                                                              \
    } while (0)

static void reset_env(void)
{
    pace_reset();
    if (active_mutant != PACE_MUT_NONE)
        pace_set_mutant(active_mutant);
}

static uint64_t mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* 1. Effective rates per margin (0, 1, 2+) and balance default/override, plus actual pacing throughput. */
static void test_rates_by_margin(void)
{
    reset_env();
    struct pace_stats st;
    pace_stats(&st);
    CHECK(st.rate_bytes_s[PACE_SEND] == PACE_DEFAULT_REBUILD_BYTES_S, "default send rate %" PRIu64,
          st.rate_bytes_s[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_RECV] == PACE_DEFAULT_REBUILD_BYTES_S, "default recv rate %" PRIu64,
          st.rate_bytes_s[PACE_RECV]);
    CHECK(st.active_margin[PACE_SEND] == PACE_MARGIN_NONE, "idle margin %d", st.active_margin[PACE_SEND]);
    CHECK(st.halved == 0, "halved %d", st.halved);

    const uint64_t rb = 4ull << 20; /* 4 MiB/s */
    const uint64_t bb = 2ull << 20; /* 2 MiB/s */
    struct pace_cfg cfg = {.rebuild_bytes_s = rb, .balance_bytes_s = bb};
    pace_configure(&cfg);

    /* Margin 2 -> rebuild_bytes_s (4 MiB/s). */
    uint64_t j2 = pace_job_begin_ex(PACE_SEND, 2, 0);
    CHECK(j2 > 0, "j2");
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 2, "margin %d", st.active_margin[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_SEND] == rb, "rate %" PRIu64 " want %" PRIu64, st.rate_bytes_s[PACE_SEND], rb);
    /* Transfer 1.5 MiB in 256 KiB chunks: 50 ms burst = 200 KiB, remaining ~1.3 MiB at 4 MiB/s >= ~250 ms. */
    uint64_t t0 = mono_ns();
    for (int i = 0; i < 6; i++) {
        uint64_t w = pace_take(PACE_SEND, 256ull << 10, 2);
        CHECK(w != PACE_REFUSED, "rebuild refused");
    }
    uint64_t dt_ms = (mono_ns() - t0) / 1000000ull;
    CHECK(dt_ms >= 200 && dt_ms <= 5000, "margin-2 1.5 MiB at 4 MiB/s took %" PRIu64 " ms", dt_ms);
    pace_job_end_ex(PACE_SEND, j2);

    /* Margin 1 -> 4 * rebuild_bytes_s (16 MiB/s): check computed virtual-clock wait (not tight wall-clock upper bound
     * that could flake on a loaded/TSan CI host). 2 MiB at 16 MiB/s costs 125 ms - 50 ms burst = ~75 ms wait. */
    reset_env();
    pace_configure(&cfg);
    uint64_t j1 = pace_job_begin_ex(PACE_SEND, 1, 0);
    CHECK(j1 > 0, "j1");
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 1, "margin %d", st.active_margin[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_SEND] == rb * 4, "rate %" PRIu64 " want %" PRIu64, st.rate_bytes_s[PACE_SEND], rb * 4);
    CHECK(pace_try_take(PACE_SEND, 2ull << 20, 1) == 0, "margin-1 first burst");
    uint64_t wm1 = pace_try_take(PACE_SEND, 256ull << 10, 1);
    CHECK(wm1 > 20000000ull && wm1 < 150000000ull, "margin-1 computed wait=%" PRIu64 " ns (want ~75 ms at 16 MiB/s)",
          wm1);
    pace_job_end_ex(PACE_SEND, j1);

    /* Margin 0 -> unlimited, zero wait even for 1 GiB. */
    reset_env();
    pace_configure(&cfg);
    uint64_t j0 = pace_job_begin_ex(PACE_SEND, 0, 0);
    CHECK(j0 > 0, "j0");
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 0, "margin %d", st.active_margin[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_SEND] == PACE_RATE_UNLIMITED, "rate %" PRIu64, st.rate_bytes_s[PACE_SEND]);
    t0 = mono_ns();
    uint64_t w0 = pace_take(PACE_SEND, 1ull << 30, 0);
    dt_ms = (mono_ns() - t0) / 1000000ull;
    CHECK(w0 == 0 && dt_ms < 50, "margin-0 waited %" PRIu64 " ns (%" PRIu64 " ms)", w0, dt_ms);
    pace_stats(&st);
    CHECK(st.class_bytes[PACE_SEND][PACE_CLS_MARGIN0] == (1ull << 30), "margin0 class bytes %" PRIu64,
          st.class_bytes[PACE_SEND][PACE_CLS_MARGIN0]);
    pace_job_end_ex(PACE_SEND, j0);

    printf("  rates_by_margin: ok\n");
}

/* 2. Lowest-margin precedence across concurrent jobs (catches mutant `pacefifo`). */
static void test_lowest_margin_precedence(void)
{
    reset_env();
    const uint64_t rb = 5ull << 20; /* 5 MiB/s */
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = rb, .balance_bytes_s = 0});

    /* Register margin 2 FIRST, then margin 1 SECOND. Bucket MUST jump to 4 * rb (20 MiB/s). */
    uint64_t ja = pace_job_begin_ex(PACE_SEND, 2, 0);
    uint64_t jb = pace_job_begin_ex(PACE_SEND, 1, 0);
    struct pace_stats st;
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 1, "active_margin=%d want 1 (first job was margin 2)",
          st.active_margin[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_SEND] == rb * 4, "rate=%" PRIu64 " want %" PRIu64 " (pacefifo?)",
          st.rate_bytes_s[PACE_SEND], rb * 4);

    /* Verify actual pacing rate via peerlink adapter matches 20 MiB/s, not 5 MiB/s. */
    uint64_t w1 = pace_peerlink_send(NULL, 5, 2ull << 20); /* 2 MiB admitted on burst */
    CHECK(w1 == 0, "first chunk should pass burst: w=%" PRIu64, w1);
    uint64_t w2 = pace_peerlink_send(NULL, 5, 256ull << 10);
    /* At 20 MiB/s, 2 MiB costs 100 ms minus 50 ms burst = ~50 ms wait.
     * At 5 MiB/s (pacefifo), 2 MiB costs 400 ms minus 50 ms burst = ~350 ms wait! */
    CHECK(w2 > 0 && w2 < 120000000ull, "wait after 2 MiB = %" PRIu64 " ns (want ~50 ms at 20 MiB/s, not ~350 ms)", w2);

    /* Register margin 0 THIRD: must immediately go unlimited and clear virtual clock debt. */
    uint64_t jc = pace_job_begin_ex(PACE_SEND, 0, 0);
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 0, "active_margin=%d want 0", st.active_margin[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_SEND] == PACE_RATE_UNLIMITED, "rate=%" PRIu64 " want unlimited",
          st.rate_bytes_s[PACE_SEND]);
    CHECK(pace_peerlink_send(NULL, 5, 1ull << 20) == 0, "margin-0 peerlink send should not wait");

    /* Unwind: end margin 0 -> back to margin 1; end margin 1 -> back to margin 2; end margin 2 -> idle. */
    pace_job_end_ex(PACE_SEND, jc);
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 1 && st.rate_bytes_s[PACE_SEND] == rb * 4, "back to margin 1");

    pace_job_end_ex(PACE_SEND, jb);
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 2 && st.rate_bytes_s[PACE_SEND] == rb, "back to margin 2");

    /* Dynamic margin update on running job: 2 -> 1 -> 0 -> 2. */
    pace_job_update_ex(PACE_SEND, ja, 1);
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 1 && st.rate_bytes_s[PACE_SEND] == rb * 4, "updated to margin 1");

    pace_job_update_ex(PACE_SEND, ja, 0);
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 0 && st.rate_bytes_s[PACE_SEND] == PACE_RATE_UNLIMITED, "updated to margin 0");

    pace_job_end_ex(PACE_SEND, ja);
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == PACE_MARGIN_NONE, "back to idle");

    printf("  lowest_margin_precedence: ok\n");
}

/* 3. Synthetic histogram 1-min p99 > 2x 1-hr median halving + recovery + margin-0 exemption (catches `nohalve`). */
static void test_p99_halving_and_recovery(void)
{
    reset_env();
    const uint64_t rb = 10ull << 20; /* 10 MiB/s */
    const uint64_t bb = 6ull << 20;  /* 6 MiB/s */
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = rb, .balance_bytes_s = bb});

    metrics_p99_win win;
    metrics_p99_win_init(&win);

    uint64_t qcum[METRICS_HIST_BUCKETS] = {0};
    uint64_t wcum[METRICS_HIST_BUCKETS] = {0};
    int b_fast = metrics_bucket_for(0.001); /* 1 ms */
    int b_slow = metrics_bucket_for(0.050); /* 50 ms */
    int b_wal_slow = metrics_bucket_for(0.100); /* 100 ms */

    /* Phase A: 180 seconds (3 minutes) of steady 1 ms queries and 1 ms WAL syncs -> establish 1-hr median baseline. */
    int h = 0;
    for (int sec = 0; sec < 180; sec++) {
        qcum[b_fast] += 100;
        wcum[b_fast] += 100;
        h = metrics_p99_win_step(&win, qcum, wcum);
        CHECK(h == 0, "baseline sec %d triggered halving (p99=%.6f med=%.6f)", sec, win.query.last_p99_1m,
              win.query.last_median_1h);
    }
    CHECK(win.query.min_count >= 2 && win.query.last_median_1h > 0.0 && win.query.last_median_1h <= 0.0011,
          "query 1h median=%.6f min_count=%d", win.query.last_median_1h, win.query.min_count);

    /* Phase B: query latency spikes to 50 ms -> 1-min p99 >> 2x 1-hr median -> halving triggers! */
    for (int sec = 0; sec < 10; sec++) {
        qcum[b_slow] += 100;
        wcum[b_fast] += 100;
        h = metrics_p99_win_step(&win, qcum, wcum);
    }
    CHECK(h == 1, "query p99 spike did not trigger halving (p99=%.6f med=%.6f)", win.query.last_p99_1m,
          win.query.last_median_1h);

    pace_set_p99_halved(h);
    struct pace_stats st;

    /* Margin 2 halved: 10 MiB/s -> 5 MiB/s. */
    uint64_t j2 = pace_job_begin_ex(PACE_SEND, 2, 0);
    pace_stats(&st);
    CHECK(st.halved == 1, "st.halved=%d want 1 (nohalve?)", st.halved);
    CHECK(st.rate_bytes_s[PACE_SEND] == rb / 2, "halved margin-2 rate=%" PRIu64 " want %" PRIu64,
          st.rate_bytes_s[PACE_SEND], rb / 2);
    /* Verify actual bucket wait reflects halved rate: 2 MiB at 5 MiB/s = 400 ms - 50 ms burst = ~350 ms,
     * whereas at unhalved 10 MiB/s it would be 200 ms - 50 ms = ~150 ms. */
    CHECK(pace_try_take(PACE_SEND, 2ull << 20, 2) == 0, "first burst chunk");
    uint64_t wh = pace_try_take(PACE_SEND, 256ull << 10, 2);
    CHECK(wh > 250000000ull, "halved wait=%" PRIu64 " ns (want > 250 ms at 5 MiB/s, not ~150 ms at 10 MiB/s)", wh);
    pace_job_end_ex(PACE_SEND, j2);

    /* Margin 1 halved: 40 MiB/s -> 20 MiB/s. */
    uint64_t j1 = pace_job_begin_ex(PACE_RECV, 1, 0);
    pace_stats(&st);
    CHECK(st.rate_bytes_s[PACE_RECV] == (rb * 4) / 2, "halved margin-1 rate=%" PRIu64 " want %" PRIu64,
          st.rate_bytes_s[PACE_RECV], (rb * 4) / 2);
    pace_job_end_ex(PACE_RECV, j1);

    /* Balance halved: 6 MiB/s -> 3 MiB/s. */
    uint64_t jbal = pace_job_begin_ex(PACE_RECV, PACE_MARGIN_BALANCE, 1);
    pace_stats(&st);
    CHECK(st.rate_bytes_s[PACE_RECV] == bb / 2, "halved balance rate=%" PRIu64 " want %" PRIu64,
          st.rate_bytes_s[PACE_RECV], bb / 2);
    pace_job_end_ex(PACE_RECV, jbal);

    /* Margin 0 EXEMPT from halving: stays unlimited and 0 wait! */
    uint64_t j0 = pace_job_begin_ex(PACE_SEND, 0, 0);
    pace_stats(&st);
    CHECK(st.rate_bytes_s[PACE_SEND] == PACE_RATE_UNLIMITED, "margin-0 halved! rate=%" PRIu64,
          st.rate_bytes_s[PACE_SEND]);
    CHECK(pace_take(PACE_SEND, 512ull << 20, 0) == 0, "margin-0 waited while halved");
    pace_job_end_ex(PACE_SEND, j0);

    /* Phase C: query latency returns to 1 ms for 65 seconds -> spike leaves 1-min window -> recovers! */
    for (int sec = 0; sec < 65; sec++) {
        qcum[b_fast] += 100;
        wcum[b_fast] += 100;
        h = metrics_p99_win_step(&win, qcum, wcum);
    }
    CHECK(h == 0, "did not recover after query spike ended (p99=%.6f med=%.6f)", win.query.last_p99_1m,
          win.query.last_median_1h);
    pace_set_p99_halved(h);
    pace_stats(&st);
    CHECK(st.halved == 0 && st.rate_bytes_s[PACE_SEND] == rb, "recovered rate=%" PRIu64 " halved=%d",
          st.rate_bytes_s[PACE_SEND], st.halved);

    /* Phase D: WAL sync p99 spikes to 100 ms while queries stay fast -> halving triggers, then recovers! */
    for (int sec = 0; sec < 10; sec++) {
        qcum[b_fast] += 100;
        wcum[b_wal_slow] += 100;
        h = metrics_p99_win_step(&win, qcum, wcum);
    }
    CHECK(h == 1, "WAL sync p99 spike did not trigger halving (p99=%.6f med=%.6f)", win.wal_sync.last_p99_1m,
          win.wal_sync.last_median_1h);
    for (int sec = 0; sec < 65; sec++) {
        qcum[b_fast] += 100;
        wcum[b_fast] += 100;
        h = metrics_p99_win_step(&win, qcum, wcum);
    }
    CHECK(h == 0, "did not recover after WAL sync spike ended");

    printf("  p99_halving_and_recovery: ok\n");
}

/* 4. Balance jobs take balance_mb_s (0 = rebuild_mb_s) and yield/are refused while any rebuild is active. */
static void test_balance_yields_to_rebuild(void)
{
    reset_env();
    const uint64_t rb = 8ull << 20;
    /* balance_bytes_s = 0 -> equals rebuild_bytes_s. */
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = rb, .balance_bytes_s = 0});
    uint64_t jb = pace_job_begin_ex(PACE_SEND, PACE_MARGIN_BALANCE, 1);
    struct pace_stats st;
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == PACE_MARGIN_BALANCE, "active_margin=%d", st.active_margin[PACE_SEND]);
    CHECK(st.rate_bytes_s[PACE_SEND] == rb, "balance default rate=%" PRIu64 " want %" PRIu64,
          st.rate_bytes_s[PACE_SEND], rb);

    /* Explicit balance_bytes_s = 3 MiB/s. */
    const uint64_t bb = 3ull << 20;
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = rb, .balance_bytes_s = bb});
    pace_stats(&st);
    CHECK(st.rate_bytes_s[PACE_SEND] == bb, "balance explicit rate=%" PRIu64 " want %" PRIu64,
          st.rate_bytes_s[PACE_SEND], bb);
    uint64_t w = pace_take(PACE_SEND, 64ull << 10, PACE_MARGIN_BALANCE);
    CHECK(w != PACE_REFUSED, "balance refused with no rebuild active");
    pace_stats(&st);
    CHECK(st.class_bytes[PACE_SEND][PACE_CLS_BALANCE] == (64ull << 10), "balance class bytes %" PRIu64,
          st.class_bytes[PACE_SEND][PACE_CLS_BALANCE]);

    /* Register a rebuild job on PACE_SEND -> balance MUST be refused on PACE_SEND, but NOT on PACE_RECV! */
    uint64_t jr = pace_job_begin_ex(PACE_SEND, 2, 0);
    CHECK(pace_try_take(PACE_SEND, 64ull << 10, PACE_MARGIN_BALANCE) == PACE_REFUSED, "try_take not refused");
    CHECK(pace_take(PACE_SEND, 64ull << 10, PACE_MARGIN_BALANCE) == PACE_REFUSED, "take not refused");
    pace_stats(&st);
    CHECK(st.balance_refused[PACE_SEND] == 2, "balance_refused=%" PRIu64, st.balance_refused[PACE_SEND]);
    CHECK(st.class_bytes[PACE_SEND][PACE_CLS_BALANCE] == (64ull << 10), "refused bytes counted");
    CHECK(st.active_margin[PACE_SEND] == 2 && st.rate_bytes_s[PACE_SEND] == rb, "rebuild took precedence");

    /* PACE_RECV has no rebuild job -> balance on PACE_RECV succeeds! */
    CHECK(pace_take(PACE_RECV, 32ull << 10, PACE_MARGIN_BALANCE) != PACE_REFUSED, "recv balance wrongly refused");
    pace_stats(&st);
    CHECK(st.class_bytes[PACE_RECV][PACE_CLS_BALANCE] == (32ull << 10), "recv balance bytes %" PRIu64,
          st.class_bytes[PACE_RECV][PACE_CLS_BALANCE]);

    /* End rebuild job on PACE_SEND -> balance on PACE_SEND immediately resumes! */
    pace_job_end_ex(PACE_SEND, jr);
    CHECK(pace_take(PACE_SEND, 64ull << 10, PACE_MARGIN_BALANCE) != PACE_REFUSED, "balance still refused after rebuild");
    pace_stats(&st);
    CHECK(st.class_bytes[PACE_SEND][PACE_CLS_BALANCE] == (128ull << 10), "balance resumed bytes %" PRIu64,
          st.class_bytes[PACE_SEND][PACE_CLS_BALANCE]);
    pace_job_end_ex(PACE_SEND, jb);

    printf("  balance_yields_to_rebuild: ok\n");
}

/* 5. Directional send/recv independence and per-class counters. */
static void test_directional_independence(void)
{
    reset_env();
    const uint64_t rb = 1ull << 20; /* 1 MiB/s */
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = rb, .balance_bytes_s = 0});

    /* Saturate PACE_SEND with a large margin-2 take (512 KiB at 1 MiB/s -> ~450 ms debt). */
    pace_job_begin(PACE_SEND, 2);
    CHECK(pace_try_take(PACE_SEND, 512ull << 10, 2) == 0, "send burst");
    uint64_t ws = pace_try_take(PACE_SEND, 64ull << 10, 2);
    CHECK(ws > 200000000ull, "send should be throttled: ws=%" PRIu64, ws);

    /* PACE_RECV is completely independent: zero wait and its own margin/rate. */
    pace_job_begin(PACE_RECV, 1);
    uint64_t wr = pace_try_take(PACE_RECV, 256ull << 10, 1);
    CHECK(wr == 0, "recv blocked by send! wr=%" PRIu64, wr);

    struct pace_stats st;
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == 2 && st.rate_bytes_s[PACE_SEND] == rb, "send state");
    CHECK(st.active_margin[PACE_RECV] == 1 && st.rate_bytes_s[PACE_RECV] == rb * 4, "recv state");
    CHECK(st.class_bytes[PACE_SEND][PACE_CLS_MARGIN2PLUS] == (512ull << 10), "send class2+");
    CHECK(st.class_bytes[PACE_RECV][PACE_CLS_MARGIN1] == (256ull << 10), "recv class1");
    CHECK(st.taken_bytes[PACE_SEND] == (512ull << 10) && st.taken_bytes[PACE_RECV] == (256ull << 10), "taken totals");

    pace_job_end(PACE_SEND, 2);
    pace_job_end(PACE_RECV, 1);

    printf("  directional_independence: ok\n");
}

/* 6. Concurrency under pthreads (ASan/UBSan/TSan safety). */
struct conc_arg {
    int tid;
    uint64_t bytes;
};

static void *conc_worker(void *arg)
{
    struct conc_arg *a = arg;
    enum pace_dir dir = (a->tid & 1) ? PACE_RECV : PACE_SEND;
    int margin = (a->tid % 3); /* 0, 1, 2 */
    for (int iter = 0; iter < 50; iter++) {
        uint64_t id = pace_job_begin_ex(dir, margin, 0);
        if (iter % 10 == 0)
            pace_job_update_ex(dir, id, (margin + 1) % 3);
        uint64_t w = pace_take(dir, 4096, margin);
        if (w != PACE_REFUSED)
            a->bytes += 4096;
        if (a->tid == 0 && (iter % 15 == 0))
            pace_set_p99_halved((iter / 15) & 1);
        if (a->tid == 1 && (iter % 20 == 0)) {
            struct pace_stats st;
            pace_stats(&st);
            (void)st;
        }
        pace_job_end_ex(dir, id);
    }
    return NULL;
}

static void test_concurrency(void)
{
    reset_env();
    /* High rate so 8 threads finish quickly while heavily exercising locks, condvars, job list and stats. */
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = 100ull << 20, .balance_bytes_s = 50ull << 20});

    enum { NTH = 8 };
    pthread_t th[NTH];
    struct conc_arg ca[NTH];
    memset(ca, 0, sizeof ca);
    for (int i = 0; i < NTH; i++) {
        ca[i].tid = i;
        CHECK(pthread_create(&th[i], NULL, conc_worker, &ca[i]) == 0, "pthread_create %d", i);
    }
    uint64_t want_send = 0, want_recv = 0;
    for (int i = 0; i < NTH; i++) {
        pthread_join(th[i], NULL);
        if (i & 1)
            want_recv += ca[i].bytes;
        else
            want_send += ca[i].bytes;
    }
    struct pace_stats st;
    pace_stats(&st);
    CHECK(st.active_margin[PACE_SEND] == PACE_MARGIN_NONE, "send jobs leaked: %d", st.active_margin[PACE_SEND]);
    CHECK(st.active_margin[PACE_RECV] == PACE_MARGIN_NONE, "recv jobs leaked: %d", st.active_margin[PACE_RECV]);
    CHECK(st.taken_bytes[PACE_SEND] == want_send, "send bytes %" PRIu64 " != %" PRIu64, st.taken_bytes[PACE_SEND],
          want_send);
    CHECK(st.taken_bytes[PACE_RECV] == want_recv, "recv bytes %" PRIu64 " != %" PRIu64, st.taken_bytes[PACE_RECV],
          want_recv);

    printf("  concurrency: ok (8 threads, %" PRIu64 " send + %" PRIu64 " recv bytes)\n", want_send, want_recv);
}

/* 7. Receive-side wall-clock throttling and max_wait_ns cap (pace_take_capped). */
static void test_recv_throttle_and_cap(void)
{
    reset_env();
    const uint64_t rb = 2ull << 20; /* 2 MiB/s */
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = rb, .balance_bytes_s = 0});

    /* Receive side genuinely throttles on its own (1 MiB in 256 KiB chunks at 2 MiB/s: >= ~300 ms). */
    uint64_t jr = pace_job_begin_ex(PACE_RECV, 2, 0);
    uint64_t t0 = mono_ns();
    for (int i = 0; i < 4; i++)
        CHECK(pace_take(PACE_RECV, 256ull << 10, 2) != PACE_REFUSED, "recv take refused");
    uint64_t dt_ms = (mono_ns() - t0) / 1000000ull;
    CHECK(dt_ms >= 250 && dt_ms <= 5000, "recv 1 MiB at 2 MiB/s took %" PRIu64 " ms", dt_ms);
    pace_job_end_ex(PACE_RECV, jr);

    /* Capped wait: saturate PACE_RECV with 4 MiB debt at 1 MiB/s (~4 s debt), then call pace_take_capped with 100 ms
     * cap: must return near 100 ms (never stalling for 4 s), clamp virtual-clock debt to <= cap, and count the bytes. */
    reset_env();
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = 1ull << 20, .balance_bytes_s = 0});
    jr = pace_job_begin_ex(PACE_RECV, 2, 0);
    CHECK(pace_try_take(PACE_RECV, 4ull << 20, 2) == 0, "seed 4 s debt");
    t0 = mono_ns();
    uint64_t wc = pace_take_capped(PACE_RECV, 256ull << 10, 2, 100ull * 1000000ull);
    dt_ms = (mono_ns() - t0) / 1000000ull;
    CHECK(wc != PACE_REFUSED && dt_ms >= 80 && dt_ms < 2000, "capped wait took %" PRIu64 " ms (wc=%" PRIu64 ")", dt_ms,
          wc);
    /* Verify forced admission clamped virtual clock debt to <= 100 ms (did not keep 4 s debt). */
    uint64_t rem = pace_try_take(PACE_RECV, 64ull << 10, 2);
    CHECK(rem <= 150000000ull, "debt after capped force = %" PRIu64 " ns (want <= 100 ms cap)", rem);
    struct pace_stats st;
    pace_stats(&st);
    CHECK(st.taken_bytes[PACE_RECV] == (4ull << 20) + (256ull << 10), "capped bytes %" PRIu64,
          st.taken_bytes[PACE_RECV]);
    pace_job_end_ex(PACE_RECV, jr);

    printf("  recv_throttle_and_cap: ok\n");
}

/* 8. Real engine WAL fdatasync observer (eng_set_wal_sync_observer -> metrics_wal_sync) and headless
 * metrics_start(listen_fd = -1) stepping the 1-s p99 halving callback without --metrics-port. */
static atomic_int headless_ticks;
static void headless_halve_cb(int h)
{
    (void)h;
    atomic_fetch_add_explicit(&headless_ticks, 1, memory_order_relaxed);
}

static void test_wal_sync_observer_and_headless_metrics(void)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-pace-wal-XXXXXX", scratch_dir);
    CHECK(mkdtemp(dir) != NULL, "mkdtemp %s", dir);

    metrics_enable();
    eng_set_wal_sync_observer(metrics_wal_sync);

    uint64_t q0[METRICS_HIST_BUCKETS], w0[METRICS_HIST_BUCKETS];
    metrics_p99_snapshot(q0, w0);
    uint64_t sum0 = 0;
    for (int b = 0; b < METRICS_HIST_BUCKETS; b++)
        sum0 += w0[b];

    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 1;
    o.wal_sync_ms = 0;
    o.stats = -1;
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0, "tsdb_open");
    tsdb_sid s = 0;
    CHECK(tsdb_series(db, "pace.wal", &s) == 0, "tsdb_series");
    for (int i = 0; i < 5; i++) {
        int64_t ts = 1000 + i;
        float v = (float)i;
        CHECK(tsdb_insert_series(db, s, &ts, &v, 1) == 0, "insert %d", i);
    }

    uint64_t q1[METRICS_HIST_BUCKETS], w1[METRICS_HIST_BUCKETS];
    metrics_p99_snapshot(q1, w1);
    uint64_t sum1 = 0;
    for (int b = 0; b < METRICS_HIST_BUCKETS; b++)
        sum1 += w1[b];
    CHECK(sum1 >= sum0 + 5, "WAL sync observer recorded %" PRIu64 " syncs (want >= 5)", sum1 - sum0);

    /* Headless metrics thread (listen_fd = -1, no HTTP port) steps on_p99_halved every second. */
    atomic_store_explicit(&headless_ticks, 0, memory_order_relaxed);
    metrics_cfg mc = {.db = db, .listen_fd = -1, .io = "epoll", .role = "writer", .on_p99_halved = headless_halve_cb};
    CHECK(metrics_start(&mc) == 0, "headless metrics_start");
    struct timespec sl = {1, 300000000};
    nanosleep(&sl, NULL);
    metrics_stop();
    int ticks = atomic_load_explicit(&headless_ticks, memory_order_relaxed);
    CHECK(ticks >= 1, "headless p99 halving callback fired %d times (want >= 1)", ticks);

    eng_set_wal_sync_observer(NULL);
    tsdb_close(db);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0, "cleanup scratch dir");

    printf("  wal_sync_observer_and_headless_metrics: ok (%" PRIu64 " WAL syncs recorded, %d headless ticks)\n",
           sum1 - sum0, ticks);
}

int main(int argc, char **argv)
{
    const char *td = getenv("TMPDIR");
    if (td && *td)
        scratch_dir = td;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            active_mutant = pace_mutant_parse(argv[++i]);
            if (active_mutant < 0) {
                fprintf(stderr, "test_pace: unknown mutant '%s'\n", argv[i]);
                return 2;
            }
        } else if (argv[i][0] != '-') {
            scratch_dir = argv[i];
        }
    }
    const char *env = getenv("TSDB_PACE_MUTANT");
    if (active_mutant == PACE_MUT_NONE && env && *env) {
        active_mutant = pace_mutant_parse(env);
        if (active_mutant < 0) {
            fprintf(stderr, "test_pace: unknown TSDB_PACE_MUTANT '%s'\n", env);
            return 2;
        }
    }

    printf("test_pace%s%s:\n", active_mutant ? " --mutant " : "",
           active_mutant == PACE_MUT_FIFO ? "pacefifo" : active_mutant == PACE_MUT_NOHALVE ? "nohalve" : "");
    test_rates_by_margin();
    test_lowest_margin_precedence();
    test_p99_halving_and_recovery();
    test_balance_yields_to_rebuild();
    test_directional_independence();
    test_concurrency();
    test_recv_throttle_and_cap();
    test_wal_sync_observer_and_headless_metrics();
    pace_reset();

    if (failures) {
        fprintf(stderr, "test_pace: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_pace: OK\n");
    return 0;
}
