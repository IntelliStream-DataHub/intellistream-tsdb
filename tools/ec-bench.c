/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Speed of the erasure codec (src/ec, Reed-Solomon over GF(2^8)) per geometry, CPU level and kernel, on one core.
 *
 *   ec-bench [--len 1048576] [--rounds 5] [--ms 200] [--geoms 4+2,8+2,8+4,16+4] [--level v2|v3|v4]
 *
 * For each CPU level this machine has (cpu_set_level: what TSDB_CPU=v2|v3|v4 gives a process) and each GF(256)
 * kernel that level allows, and each geometry k+m (k data shards, m parity shards of --len bytes):
 *   encode   the m parity shards from the k data shards, in MB/s of data (k x len bytes, 1 MB = 10^6 bytes)
 *   decode   the m data shards lost (the worst case that can be rebuilt) rebuilt from the k that are left, by
 *            iso_rs_decode_data, in MB/s of the stripe's data (k x len): the matrix inversion is part of it
 *   rebuild  the same with the plan built once (iso_rs_plan_apply): pure data movement, what a recovery job
 *            that rebuilds thousands of segments of one lost device sees
 * Mean and sd of the rounds, the kernels interleaved in every round (so a clock change hits all of them), each
 * round at least --ms long. The buffers are allocated once, 64-byte aligned, and written before the first round, so
 * no page fault is timed. The shard length defaults to 1 MiB: larger than L2, in L3 for the small geometries; a
 * node's segments are larger still (docs/design-cluster-data.md), so --len 4194304 shows the memory-bound figure.
 * Numbers go in BENCHMARKS.md only from a quiet machine (flock -o -x on BENCH.lock, AGENTS.md "Benchmarks").
 */
#include "../src/internal.h"
#include "common.h"

#include "../src/ec/gf256.h"
#include "../src/ec/rs.h"

#include <math.h>

#define MAXR 64
#define MAXG 16
#define MAXV 8

typedef struct {
    unsigned k, m;
} geom;

static void usage(void)
{
    fprintf(stderr, "usage: ec-bench [--len BYTES] [--rounds N] [--ms N] [--geoms 4+2,8+2,...] [--level v2|v3|v4]\n");
    exit(2);
}

static uint8_t *aligned(size_t n)
{
    void *p;
    if (posix_memalign(&p, 64, n))
        abort();
    return p;
}

static void stats(const double *x, int n, double *mean, double *sd)
{
    double s = 0, q = 0;
    for (int i = 0; i < n; i++)
        s += x[i];
    *mean = s / n;
    for (int i = 0; i < n; i++)
        q += (x[i] - *mean) * (x[i] - *mean);
    *sd = n > 1 ? sqrt(q / (n - 1)) : 0;
}

typedef struct {
    const iso_rs *rs;
    iso_rs_plan *plan;
    uint8_t *sh[32];
    bool present[32];
    unsigned k, m;
    size_t len;
} job;

static double timed(job *j, int what, double ms)
{
    double t0 = now_s(), t = t0;
    long reps = 0;
    do {
        for (int i = 0; i < 4; i++) {
            if (what == 0)
                iso_rs_encode(j->rs, j->sh, j->len);
            else if (what == 1)
                (void)iso_rs_decode_data(j->rs, j->sh, j->present, j->len);
            else
                iso_rs_plan_apply(j->plan, j->sh, j->len);
        }
        reps += 4;
        t = now_s();
    } while ((t - t0) * 1e3 < ms);
    return (double)reps * (double)j->k * (double)j->len / (t - t0) / 1e6;
}

int main(int argc, char **argv)
{
    size_t len = 1 << 20;
    int rounds = 5;
    double ms = 200;
    geom gs[MAXG] = {{4, 2}, {8, 2}, {8, 4}, {16, 4}};
    int ng = 4, only_level = -1;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--len") && i + 1 < argc)
            len = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--rounds") && i + 1 < argc)
            rounds = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--ms") && i + 1 < argc)
            ms = atof(argv[++i]);
        else if (!strcmp(argv[i], "--level") && i + 1 < argc) {
            const char *v = argv[++i];
            only_level = !strcmp(v, "v2") ? CPU_BASE : !strcmp(v, "v3") ? CPU_V3 : !strcmp(v, "v4") ? CPU_V4 : -2;
            if (only_level == -2)
                usage();
        } else if (!strcmp(argv[i], "--geoms") && i + 1 < argc) {
            ng = 0;
            for (char *s = argv[++i]; *s && ng < MAXG;) {
                unsigned k, m;
                int used = 0;
                if (sscanf(s, "%u+%u%n", &k, &m, &used) != 2 || k < 1 || m < 1 || k + m > 32 || k + m > 255)
                    usage();
                gs[ng++] = (geom){k, m};
                s += used;
                if (*s == ',')
                    s++;
            }
        } else
            usage();
    }
    if (len < 64 || rounds < 1 || rounds > MAXR || ng == 0)
        usage();
    printf("ec-bench: shard %zu B, %d rounds of at least %.0f ms, one core; MB/s of the stripe's data (k x shard)\n", len,
           rounds, ms);
    printf("%-9s %-8s %-6s %17s %17s %17s\n", "level", "kernel", "k+m", "encode", "decode", "rebuild (plan)");

    int start = cpu_level();
    for (int level = CPU_BASE; level <= cpu_level_max(); level++) {
        if (only_level >= 0 && level != only_level)
            continue;
        if (cpu_set_level(level) != 0)
            continue;
        iso_gf256_kernel ks[ISO_GF256_MAX_KERNELS + 1];
        size_t nk = iso_gf256_kernels(ks);
        size_t use[ISO_GF256_MAX_KERNELS + 1], nu = 0;
        for (size_t i = 0; i < nk; i++)
            if (ks[i].available)
                use[nu++] = i;
        for (int gi = 0; gi < ng; gi++) {
            job j = {.k = gs[gi].k, .m = gs[gi].m, .len = len};
            j.rs = iso_rs_new(j.k, j.m);
            uint64_t x = 0x9e3779b97f4a7c15ull;
            for (unsigned i = 0; i < j.k + j.m; i++) {
                j.sh[i] = aligned(len);
                for (size_t b = 0; b < len; b += 8) {
                    x = x * 6364136223846793005ull + 1442695040888963407ull;
                    memcpy(j.sh[i] + b, &x, len - b < 8 ? len - b : 8);
                }
                j.present[i] = true;
            }
            iso_rs_encode(j.rs, j.sh, len);
            unsigned lost = j.m < j.k ? j.m : j.k; /* the first shards: data, the ones a read must rebuild */
            for (unsigned i = 0; i < lost; i++)
                j.present[i] = false;
            j.plan = iso_rs_plan_new(j.rs, j.present, true);
            double v[3][ISO_GF256_MAX_KERNELS + 1][MAXR];
            for (int r = 0; r < rounds; r++)
                for (size_t u = 0; u < nu; u++) {
                    iso_gf256_use(&ks[use[u]]);
                    for (int w = 0; w < 3; w++)
                        v[w][u][r] = timed(&j, w, ms);
                }
            for (size_t u = 0; u < nu; u++) {
                char c[3][32];
                for (int w = 0; w < 3; w++) {
                    double mean, sd;
                    stats(v[w][u], rounds, &mean, &sd);
                    snprintf(c[w], sizeof c[w], "%8.0f +-%-6.0f", mean, sd);
                }
                char g[16];
                snprintf(g, sizeof g, "%u+%u", j.k, j.m);
                printf("%-9s %-8s %-6s %17s %17s %17s\n", cpu_level_name(level), ks[use[u]].name, g, c[0], c[1], c[2]);
            }
            fflush(stdout);
            iso_rs_plan_free(j.plan);
            iso_rs_free((iso_rs *)j.rs);
            for (unsigned i = 0; i < j.k + j.m; i++)
                free(j.sh[i]);
        }
    }
    cpu_set_level(start);
    return 0;
}
