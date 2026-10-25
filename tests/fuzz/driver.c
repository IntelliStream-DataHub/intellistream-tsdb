/*
 * A small coverage-guided mutational fuzzer, for machines without clang's
 * libFuzzer. The harnesses have libFuzzer's interface (LLVMFuzzerTestOneInput,
 * optionally LLVMFuzzerInitialize), so with clang they build with
 * -fsanitize=fuzzer instead of this file.
 *
 * Coverage: the code under test is compiled with GCC's
 * -fsanitize-coverage=trace-pc, which calls __sanitizer_cov_trace_pc at every
 * basic block; that hook (this file is compiled without the flag) marks the
 * edge from the previous block in a bitmap, AFL-style, with hit counts in
 * buckets. An input that reaches a new edge or bucket joins the corpus and is
 * written to the output directory. ASan and UBSan (halt_on_error) find the
 * bugs; on a crash, a death callback writes the input to crash-<hash>, and an
 * input that runs longer than the timeout is written to timeout-<hash>.
 *
 *   fuzz-X [-t seconds] [-o dir] [-m max_len] [-s seed] DIR|FILE...  fuzz from the corpus in DIR...
 *   fuzz-X -replay DIR|FILE...                                       run every input once (make test)
 *   fuzz-X -seeds DIR                                                write the harness's seed inputs to DIR
 *
 * Without coverage instrumentation (the replay build in make test) the hook is
 * never called and only replay makes sense.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerInitialize(int *argc, char ***argv) __attribute__((weak));
/* the harness's seeds, written as files into dir (tests/fuzz/fuzz.h has helpers) */
void fuzz_seeds(const char *dir) __attribute__((weak));

#define MAP_SIZE (1u << 16)
static uint8_t cov[MAP_SIZE], seen[MAP_SIZE];
static __thread uintptr_t prev_loc;

void __sanitizer_cov_trace_pc(void)
{
    uintptr_t loc = (uintptr_t)__builtin_return_address(0);
    loc = (uintptr_t)(((uint64_t)loc * 0x9e3779b97f4a7c15ull) >> 48);
    cov[(loc ^ prev_loc) & (MAP_SIZE - 1)]++;
    prev_loc = loc >> 1;
}

typedef struct input {
    uint8_t *d;
    size_t n;
} input;

static input *corp;
static size_t ncorp, capcorp;
static const uint8_t *cur_data;
static size_t cur_size;
static const char *outdir = "."; /* crash-* and timeout-* go here */
static bool keep_corpus;         /* -o given: new inputs go there as cov-* */
static uint64_t rng = 0x9e3779b97f4a7c15ull;

static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static uint64_t fnv(const uint8_t *d, size_t n)
{
    uint64_t h = 0xcbf29ce484222325ull;
    for (size_t i = 0; i < n; i++)
        h = (h ^ d[i]) * 0x100000001b3ull;
    return h;
}

/* async-signal-safe: used from the death callback and the alarm handler */
static void save_as(const char *prefix, const uint8_t *d, size_t n)
{
    char path[4096];
    snprintf(path, sizeof path, "%s/%s-%016llx", outdir, prefix, (unsigned long long)fnv(d, n));
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        for (size_t off = 0; off < n;) {
            ssize_t w = write(fd, d + off, n - off);
            if (w <= 0)
                break;
            off += (size_t)w;
        }
        close(fd);
        if (strcmp(prefix, "cov"))
            fprintf(stderr, "fuzz: input written to %s\n", path);
    }
}

static void on_death(void)
{
    if (cur_data)
        save_as("crash", cur_data, cur_size);
}

static void on_alarm(int sig)
{
    (void)sig;
    if (cur_data)
        save_as("timeout", cur_data, cur_size);
    fprintf(stderr, "fuzz: timeout\n");
    abort();
}

void __sanitizer_set_death_callback(void (*cb)(void)) __attribute__((weak));

static void add_input(const uint8_t *d, size_t n)
{
    if (ncorp == capcorp) {
        capcorp = capcorp ? 2 * capcorp : 256;
        corp = realloc(corp, capcorp * sizeof *corp);
        if (!corp)
            abort();
    }
    corp[ncorp].d = malloc(n ? n : 1);
    memcpy(corp[ncorp].d, d, n);
    corp[ncorp++].n = n;
}

static int read_file(const char *path, uint8_t **d, size_t *n)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return -1;
    size_t cap = 4096, len = 0;
    uint8_t *b = malloc(cap);
    for (size_t got; (got = fread(b + len, 1, cap - len, f)) > 0;) {
        len += got;
        if (len == cap)
            b = realloc(b, cap *= 2);
    }
    fclose(f);
    *d = b;
    *n = len;
    return 0;
}

/* calls fn for every regular file under path (a file or a directory), in name order */
static void each_file(const char *path, void (*fn)(const char *))
{
    struct stat st;
    if (stat(path, &st) != 0)
        return;
    if (!S_ISDIR(st.st_mode)) {
        fn(path);
        return;
    }
    struct dirent **ents;
    int k = scandir(path, &ents, NULL, alphasort);
    for (int i = 0; i < k; i++) {
        if (ents[i]->d_name[0] != '.') {
            char sub[4096];
            snprintf(sub, sizeof sub, "%s/%s", path, ents[i]->d_name);
            each_file(sub, fn);
        }
        free(ents[i]);
    }
    if (k >= 0)
        free(ents);
}

static unsigned timeout_s = 10;

static void run_one(const uint8_t *d, size_t n)
{
    /* a copy of exactly n bytes, so ASan sees any read past the input */
    uint8_t *copy = malloc(n ? n : 1);
    memcpy(copy, d, n);
    cur_data = copy;
    cur_size = n;
    alarm(timeout_s);
    LLVMFuzzerTestOneInput(copy, n);
    alarm(0);
    cur_data = NULL;
    free(copy);
}

static size_t nreplayed;

static void replay_file(const char *path)
{
    uint8_t *d;
    size_t n;
    if (read_file(path, &d, &n) != 0) {
        fprintf(stderr, "fuzz: cannot read %s\n", path);
        exit(1);
    }
    run_one(d, n);
    free(d);
    nreplayed++;
}

static void load_file(const char *path)
{
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    if (!strncmp(base, "crash-", 6) || !strncmp(base, "timeout-", 8))
        return; /* findings, not corpus */
    uint8_t *d;
    size_t n;
    if (read_file(path, &d, &n) == 0) {
        add_input(d, n);
        free(d);
    }
}

static uint8_t bucket(uint8_t c)
{
    return c == 0 ? 0 : c == 1 ? 1 : c == 2 ? 2 : c == 3 ? 4 : c < 8 ? 8 : c < 16 ? 16 : c < 32 ? 32 : c < 128 ? 64 : 128;
}

/* whether the last run reached an edge or hit-count bucket no earlier run did */
static int new_coverage(void)
{
    int fresh = 0;
    for (size_t i = 0; i < MAP_SIZE; i++) {
        if (cov[i]) {
            uint8_t b = bucket(cov[i]);
            if (b & ~seen[i]) {
                seen[i] |= b;
                fresh = 1;
            }
            cov[i] = 0;
        }
    }
    return fresh;
}

static size_t edges(void)
{
    size_t k = 0;
    for (size_t i = 0; i < MAP_SIZE; i++)
        k += seen[i] != 0;
    return k;
}

static const int64_t interesting[] = {0, 1, -1, 2, 16, 32, 64, 127, 128, 255, 256, 4096, 8192, 65535, 65536,
                                      INT32_MAX, INT32_MIN, (int64_t)UINT32_MAX, INT64_MAX, INT64_MIN};

/* mutates b[0..n) in place within cap bytes; returns the new length */
static size_t mutate(uint8_t *b, size_t n, size_t cap)
{
    int k = 1 + (int)(rnd() % 6);
    for (int m = 0; m < k; m++) {
        size_t pos = n ? rnd() % n : 0;
        switch (rnd() % 10) {
        case 0: /* flip a bit */
            if (n)
                b[pos] ^= (uint8_t)(1u << (rnd() % 8));
            break;
        case 1: /* a random byte */
            if (n)
                b[pos] = (uint8_t)rnd();
            break;
        case 2: /* add or subtract a little */
            if (n)
                b[pos] += (uint8_t)(rnd() % 35) - 17;
            break;
        case 3:
        case 4: { /* an interesting value of 1, 2, 4 or 8 bytes, either byte order */
            size_t w = (size_t)1 << (rnd() % 4);
            if (n < w)
                break;
            pos = rnd() % (n - w + 1);
            uint64_t v = (uint64_t)interesting[rnd() % (sizeof interesting / sizeof *interesting)];
            if (rnd() & 1)
                v = __builtin_bswap64(v) >> (64 - 8 * w);
            memcpy(b + pos, &v, w);
            break;
        }
        case 5: { /* delete a range */
            if (!n)
                break;
            size_t len = 1 + rnd() % (n - pos < 64 ? n - pos : 64);
            memmove(b + pos, b + pos + len, n - pos - len);
            n -= len;
            break;
        }
        case 6: { /* insert random bytes */
            size_t len = 1 + rnd() % 16;
            if (n + len > cap)
                break;
            memmove(b + pos + len, b + pos, n - pos);
            for (size_t i = 0; i < len; i++)
                b[pos + i] = (uint8_t)rnd();
            n += len;
            break;
        }
        case 7: { /* copy a range within the input */
            if (n < 2)
                break;
            size_t src = rnd() % n, len = 1 + rnd() % (n - (src > pos ? src : pos));
            memmove(b + pos, b + src, len);
            break;
        }
        case 8: { /* overwrite with a piece of another input */
            const input *o = &corp[rnd() % ncorp];
            if (!o->n || !n)
                break;
            size_t src = rnd() % o->n, len = 1 + rnd() % (o->n - src);
            if (len > n - pos)
                len = n - pos;
            memcpy(b + pos, o->d + src, len);
            break;
        }
        case 9: { /* splice: our head, another input's tail */
            const input *o = &corp[rnd() % ncorp];
            if (!o->n)
                break;
            size_t src = rnd() % o->n, len = o->n - src;
            if (pos + len > cap)
                len = cap - pos;
            memcpy(b + pos, o->d + src, len);
            n = pos + len;
            break;
        }
        }
    }
    return n;
}

int main(int argc, char **argv)
{
    double seconds = 60;
    size_t max_len = 1 << 16;
    int replay = 0, first = 1;
    const char *seeds = NULL;
    for (; first < argc && argv[first][0] == '-'; first++) {
        if (!strcmp(argv[first], "-replay"))
            replay = 1;
        else if (!strcmp(argv[first], "-seeds") && first + 1 < argc)
            seeds = argv[++first];
        else if (!strcmp(argv[first], "-t") && first + 1 < argc)
            seconds = atof(argv[++first]);
        else if (!strcmp(argv[first], "-o") && first + 1 < argc)
            outdir = argv[++first], keep_corpus = true;
        else if (!strcmp(argv[first], "-m") && first + 1 < argc)
            max_len = strtoull(argv[++first], NULL, 0);
        else if (!strcmp(argv[first], "-s") && first + 1 < argc)
            rng = strtoull(argv[++first], NULL, 0) | 1;
        else if (!strcmp(argv[first], "-timeout") && first + 1 < argc)
            timeout_s = (unsigned)atoi(argv[++first]);
        else {
            fprintf(stderr, "usage: %s [-t s] [-o dir] [-m len] [-s seed] [-timeout s] [-replay] [-seeds dir] DIR|FILE...\n",
                    argv[0]);
            return 2;
        }
    }
    if (LLVMFuzzerInitialize)
        LLVMFuzzerInitialize(&argc, &argv);
    if (seeds) {
        if (!fuzz_seeds) {
            fprintf(stderr, "fuzz: this harness has no seeds\n");
            return 2;
        }
        mkdir(seeds, 0755);
        fuzz_seeds(seeds);
        return 0;
    }
    if (__sanitizer_set_death_callback)
        __sanitizer_set_death_callback(on_death);
    signal(SIGALRM, on_alarm);
    if (replay) {
        for (int i = first; i < argc; i++)
            each_file(argv[i], replay_file);
        printf("%s: %zu inputs replayed\n", argv[0], nreplayed);
        return 0;
    }
    for (int i = first; i < argc; i++)
        each_file(argv[i], load_file);
    if (!ncorp)
        add_input((const uint8_t *)"", 0);
    mkdir(outdir, 0755);
    for (size_t i = 0; i < ncorp; i++) {
        run_one(corp[i].d, corp[i].n);
        new_coverage();
    }
    fprintf(stderr, "fuzz: %zu inputs, %zu edges\n", ncorp, edges());
    size_t cap = max_len;
    uint8_t *b = malloc(cap);
    struct timespec t0, t;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    double last_report = 0, el = 0;
    uint64_t execs = 0;
    while (el < seconds) {
        const input *src = &corp[rnd() % ncorp];
        size_t n = src->n < cap ? src->n : cap;
        memcpy(b, src->d, n);
        n = mutate(b, n, cap);
        run_one(b, n);
        execs++;
        if (new_coverage()) {
            add_input(b, n);
            if (keep_corpus)
                save_as("cov", b, n);
        }
        clock_gettime(CLOCK_MONOTONIC, &t);
        el = (double)(t.tv_sec - t0.tv_sec) + (double)(t.tv_nsec - t0.tv_nsec) / 1e9;
        if (el - last_report >= 30 || el >= seconds) {
            last_report = el;
            fprintf(stderr, "fuzz: %.0f s, %llu execs (%.0f/s), corpus %zu, edges %zu\n", el,
                    (unsigned long long)execs, execs / (el > 0 ? el : 1), ncorp, edges());
        }
    }
    free(b);
    return 0;
}
