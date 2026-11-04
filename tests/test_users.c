/*
 * USERS and the grant model (server/users.c, docs/design-auth.md §2, §4, §9):
 *  - selectors: user names, normalization (A24), globs against a reference matcher, rights words;
 *  - the inside-rules (A25) cell by cell for and against, and the manage/grant/revoke checks of A9 with the
 *    escalations they must refuse;
 *  - rights_of against a matcher that tries every grant, on random grants and names;
 *  - the file: round trip, a torn tail cut at every length, every byte flipped (damage before a valid record fails
 *    the open), records that check out but make no sense, incremental reloads that meet damage and keep the last
 *    good state, compaction (also under a reader in another process), appends from two processes at once, a file
 *    that went away, fail-stop on EIO and ENOSPC (this file's write/fdatasync/fsync/rename fail on demand, as in
 *    test_failstop), and power lost at every step of the CLI's add, revoke and compact (crashfs);
 *  - the findings of the first review (REVIEW1 #1-#6, #8): another file at the same inode and size, the compaction's
 *    rename-to-fsync window, failed appends cut off and no-op answers synced, revoke of implied rights and of
 *    grants not held, owner and mode of USERS, the indexed manage checks against trying every grant.
 *
 *   ./build/test_users [scratch dir]
 */
#include "users.c" /* -Iserver: the record builders and the batch internals */

#include <ftw.h>
#include <limits.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>

static int failures;

#define CHECK(cond)                                                                                                \
    do {                                                                                                           \
        if (!(cond)) {                                                                                             \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                              \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)

#define OK(expr)                                                                                                   \
    do {                                                                                                           \
        int rc_ = (expr);                                                                                          \
        if (rc_ != 0) {                                                                                            \
            fprintf(stderr, "%s:%d: %s = %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_));                   \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)

/* ------------------------------------------------------- fault injection */

enum { F_WRITE = 1, F_FDATASYNC = 2, F_FSYNC_DIR = 4, F_RENAME = 8 };
static int f_op, f_errno, f_fired;
static bool f_partial, f_drop;
static off_t f_synced = -1; /* USERS' size at its last successful fdatasync */
static int f_syncs;         /* successful fdatasyncs of USERS */
static int f_dirsyncs;      /* successful fsyncs of a directory */
static void (*after_rename)(const char *to); /* runs right after a real rename */

static bool fd_is(int fd, const char *suffix)
{
    char link[64], path[4200];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof path - 1);
    if (n <= 0)
        return false;
    path[n] = '\0';
    size_t s = strlen(suffix);
    return (size_t)n >= s && strcmp(path + n - s, suffix) == 0;
}

ssize_t write(int fd, const void *buf, size_t n)
{
    if (f_op == F_WRITE && fd_is(fd, "/USERS")) {
        f_op = 0, f_fired++;
        if (f_partial && n > 1)
            (void)syscall(SYS_write, fd, buf, n / 2);
        errno = f_errno;
        return -1;
    }
    return syscall(SYS_write, fd, buf, n);
}

int fdatasync(int fd)
{
    bool users_file = fd_is(fd, "/USERS");
    if (f_op == F_FDATASYNC && users_file) {
        f_op = 0, f_fired++;
        if (f_drop && f_synced >= 0)
            CHECK(ftruncate(fd, f_synced) == 0); /* the kernel dropped the dirty pages */
        errno = EIO;
        return -1;
    }
    int r = (int)syscall(SYS_fdatasync, fd);
    struct stat st;
    if (r == 0 && users_file && fstat(fd, &st) == 0)
        f_synced = st.st_size, f_syncs++;
    return r;
}

int fsync(int fd)
{
    struct stat st;
    if (f_op == F_FSYNC_DIR && fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
        f_op = 0, f_fired++;
        errno = EIO;
        return -1;
    }
    int r = (int)syscall(SYS_fsync, fd);
    if (r == 0 && fstat(fd, &st) == 0 && S_ISDIR(st.st_mode))
        f_dirsyncs++;
    return r;
}

/* another user than the directory's owner, without being one */
static uid_t fake_euid = (uid_t)-1;
uid_t geteuid(void) { return fake_euid != (uid_t)-1 ? fake_euid : (uid_t)syscall(SYS_geteuid); }

int rename(const char *a, const char *b)
{
    if (f_op == F_RENAME) {
        f_op = 0, f_fired++;
        errno = EIO;
        return -1;
    }
    int r = (int)syscall(SYS_renameat, AT_FDCWD, a, AT_FDCWD, b);
    if (r == 0 && after_rename)
        after_rename(b);
    return r;
}

static void arm(int op, int err, bool partial, bool drop)
{
    f_op = op, f_errno = err, f_partial = partial, f_drop = drop, f_fired = 0;
}

/* ------------------------------------------------------------- helpers */

static char base_dir[512];

static int rm_one(const char *p, const struct stat *st, int flag, struct FTW *f)
{
    (void)st, (void)flag, (void)f;
    return remove(p);
}

static void fresh(const char *name, char *out, size_t cap)
{
    snprintf(out, cap, "%s/test_users_%d_%s", base_dir, (int)getpid(), name);
    nftw(out, rm_one, 16, FTW_DEPTH | FTW_PHYS);
    mkdir(out, 0755);
}

static void cleanup(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

/* The whole state as text, for comparing two states. */
static const char *dump(const users *us)
{
    static char s[1 << 16];
    size_t n = (size_t)snprintf(s, sizeof s, "max=%u\n", us->max_uid);
    for (uint32_t i = 0; i < us->n && n < sizeof s; i++) {
        const user *u = us->u[i];
        n += (size_t)snprintf(s + n, sizeof s - n, "%u %s f%u v%d/%u/%u/%02x%02x:", u->uid, u->name, u->flags,
                              u->has_verifier, u->v.iterations, u->v.salt_len, u->v.stored_key[0], u->v.server_key[31]);
        for (uint32_t j = 0; j < u->ng && n < sizeof s; j++)
            n += (size_t)snprintf(s + n, sizeof s - n, " %u/%u/%s", u->g[j].kind, u->g[j].rights, u->g[j].sel);
        if (n < sizeof s)
            s[n++] = '\n';
    }
    s[n < sizeof s ? n : sizeof s - 1] = '\0';
    return s;
}

static char *dump_dup(const users *us) { return strdup(dump(us)); }

static bool same(const users *a, const users *b)
{
    char *s = dump_dup(a);
    bool r = strcmp(s, dump(b)) == 0;
    free(s);
    return r;
}

static user_verifier verifier(const char *pw)
{
    user_verifier v = {.iterations = ITER_MIN, .salt_len = SALT_MIN};
    for (int i = 0; i < SALT_MIN; i++)
        v.salt[i] = (uint8_t)(i * 7 + pw[0]);
    tsdb_scram_keys(pw, strlen(pw), v.salt, v.salt_len, v.iterations, NULL, v.stored_key, v.server_key);
    return v;
}

static off_t file_size(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : -1;
}

static int copy_file(const char *from, const char *to, off_t len)
{
    int a = open(from, O_RDONLY), b = open(to, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    char buf[65536];
    int rc = a < 0 || b < 0 ? -1 : 0;
    while (rc == 0 && len > 0) {
        ssize_t r = read(a, buf, (size_t)(len < (off_t)sizeof buf ? len : (off_t)sizeof buf));
        if (r <= 0 || syscall(SYS_write, b, buf, (size_t)r) != r)
            rc = -1;
        else
            len -= r;
    }
    if (a >= 0)
        close(a);
    if (b >= 0)
        close(b);
    return rc;
}

/* in-memory users for the pure checks */
static user *mk(uint32_t uid, const char *name, uint8_t flags)
{
    user *u = calloc(1, sizeof *u);
    u->uid = uid, u->flags = flags;
    snprintf(u->name, sizeof u->name, "%s", name);
    return u;
}

static user_grant sel(const char *text)
{
    uint8_t kind;
    const char *s;
    size_t len;
    user_grant g = {0};
    if (sel_parse(text, &kind, &s, &len) != 0) {
        fprintf(stderr, "bad selector in the test: %s\n", text);
        failures++;
        return g;
    }
    char *out = malloc(len + 1);
    size_t nl;
    if (sel_normalize(kind, s, len, &g.kind, out, &nl) != 0) {
        fprintf(stderr, "selector not valid in the test: %s\n", text);
        failures++;
    }
    g.sel = out, g.len = (uint16_t)nl;
    if (g.kind == SEL_GLOB)
        glob_parts(&g);
    return g;
}

static void give(user *u, const char *text, uint8_t rights)
{
    if (u->ng == u->gcap) {
        u->gcap = u->gcap ? 2 * u->gcap : 4;
        u->g = realloc(u->g, u->gcap * sizeof *u->g);
    }
    user_grant g = sel(text);
    g.rights = rights;
    u->g[u->ng++] = g;
    umatch_free(u->m);
    u->m = NULL;
}

/* ------------------------------------------------------- the grant model */

static void names(void)
{
    static const char *good[] = {"a", "acme/ingest", "Z9._-@/:+", "x/"};
    static const char *bad[] = {"", "/acme", "a b", "a,b", "a=b", "æ", "a\tb", "a\\b", "a*"};
    for (size_t i = 0; i < sizeof good / sizeof *good; i++)
        CHECK(user_name_valid(good[i], strlen(good[i])));
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++)
        CHECK(!user_name_valid(bad[i], strlen(bad[i])));
    char n[200];
    memset(n, 'a', sizeof n);
    CHECK(user_name_valid(n, 128) && !user_name_valid(n, 129));
    CHECK(!user_name_valid("a\0b", 3));
    printf("  user names: the allowed bytes, 1 to 128 of them, not starting with /\n");
}

static void normalize(void)
{
    static const struct {
        uint8_t kind;
        const char *in;
        uint8_t okind;
        const char *out;
    } t[] = {
        {SEL_ALL, "", SEL_ALL, ""},
        {SEL_PREFIX, "", SEL_ALL, ""},
        {SEL_PREFIX, "acme/", SEL_PREFIX, "acme/"},
        {SEL_PREFIX, "a*", SEL_PREFIX, "a*"}, /* prefixes and names are bytes: no wildcards */
        {SEL_NAME, "x?", SEL_NAME, "x?"},
        {SEL_GLOB, "acme/x", SEL_NAME, "acme/x"},
        {SEL_GLOB, "acme/\\*x", SEL_NAME, "acme/*x"},
        {SEL_GLOB, "acme/*", SEL_PREFIX, "acme/"},
        {SEL_GLOB, "acme/***", SEL_PREFIX, "acme/"},
        {SEL_GLOB, "*", SEL_ALL, ""},
        {SEL_GLOB, "**", SEL_ALL, ""},
        {SEL_GLOB, "\\a\\c\\m\\e/*", SEL_PREFIX, "acme/"},
        {SEL_GLOB, "acme/\\**", SEL_PREFIX, "acme/*"},
        {SEL_GLOB, "acme/*/t?", SEL_GLOB, "acme/*/t?"},
        {SEL_GLOB, "acme/**/t?", SEL_GLOB, "acme/*/t?"},
        {SEL_GLOB, "a\\b*\\?", SEL_GLOB, "ab*\\?"},
        {SEL_GLOB, "a\\\\*", SEL_PREFIX, "a\\"},
        {SEL_GLOB, "?", SEL_GLOB, "?"},
        {SEL_GLOB, "*x", SEL_GLOB, "*x"},
        {SEL_GLOB, "x*y*", SEL_GLOB, "x*y*"},
    };
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        char out[300];
        uint8_t k;
        size_t n;
        int rc = sel_normalize(t[i].kind, t[i].in, strlen(t[i].in), &k, out, &n);
        if (rc || k != t[i].okind || n != strlen(t[i].out) || strcmp(out, t[i].out) != 0) {
            fprintf(stderr, "  normalize %u \"%s\": rc %d kind %u \"%s\", want %u \"%s\"\n", t[i].kind, t[i].in, rc, k,
                    rc ? "" : out, t[i].okind, t[i].out);
            failures++;
        }
    }
    char out[70000], big[70000];
    uint8_t k;
    size_t n;
    CHECK(sel_normalize(SEL_GLOB, "abc\\", 4, &k, out, &n) == TSDB_ERR_INVALID); /* a lone '\' at the end */
    CHECK(sel_normalize(SEL_GLOB, "", 0, &k, out, &n) == TSDB_ERR_INVALID);
    CHECK(sel_normalize(SEL_NAME, "", 0, &k, out, &n) == TSDB_ERR_INVALID);
    CHECK(sel_normalize(SEL_ALL, "x", 1, &k, out, &n) == TSDB_ERR_INVALID);
    CHECK(sel_normalize(SEL_NAME, "a\0b", 3, &k, out, &n) == TSDB_ERR_INVALID);
    CHECK(sel_normalize(4, "a", 1, &k, out, &n) == TSDB_ERR_INVALID);
    memset(big, 'a', sizeof big);
    big[0] = '*';
    CHECK(sel_normalize(SEL_GLOB, big, GLOB_MAX, &k, out, &n) == 0 && k == SEL_GLOB);
    CHECK(sel_normalize(SEL_GLOB, big, GLOB_MAX + 1, &k, out, &n) == TSDB_ERR_INVALID); /* 257 bytes */
    CHECK(sel_normalize(SEL_NAME, big, SEL_MAX, &k, out, &n) == 0);
    CHECK(sel_normalize(SEL_NAME, big, SEL_MAX + 1, &k, out, &n) == TSDB_ERR_INVALID);

    uint8_t kind;
    const char *s;
    size_t len;
    CHECK(sel_parse("all", &kind, &s, &len) == 0 && kind == SEL_ALL && len == 0);
    CHECK(sel_parse("prefix:acme/", &kind, &s, &len) == 0 && kind == SEL_PREFIX && strcmp(s, "acme/") == 0);
    CHECK(sel_parse("name:a:b", &kind, &s, &len) == 0 && kind == SEL_NAME && strcmp(s, "a:b") == 0);
    CHECK(sel_parse("glob:a*", &kind, &s, &len) == 0 && kind == SEL_GLOB && len == 2);
    CHECK(sel_parse("acme/", &kind, &s, &len) != 0 && sel_parse("All", &kind, &s, &len) != 0);

    CHECK(rights_parse("read") == R_READ && rights_parse("write") == R_WRITE);
    CHECK(rights_parse("admin,read") == (R_ADMIN | R_READ));
    CHECK(rights_parse("read,insert,delete,admin") == R_EVERY);
    CHECK(rights_parse("") == 0 && rights_parse("read,") == 0 && rights_parse("reads") == 0 &&
          rights_parse("read,,insert") == 0 && rights_parse("READ") == 0);
    printf("  selectors: normal forms (A24), caps, parsing; rights words\n");
}

/* The obvious recursive matcher, to check the greedy one against. */
static bool ref_match(const char *g, size_t gl, const char *s, size_t sl)
{
    if (gl == 0)
        return sl == 0;
    if (g[0] == '*')
        return ref_match(g + 1, gl - 1, s, sl) || (sl && ref_match(g, gl, s + 1, sl - 1));
    if (!sl)
        return false;
    if (g[0] == '?')
        return ref_match(g + 1, gl - 1, s + 1, sl - 1);
    size_t w = g[0] == '\\' && gl > 1 ? 2 : 1;
    return g[w - 1] == s[0] && ref_match(g + w, gl - w, s + 1, sl - 1);
}

static void globs(void)
{
    static const struct {
        const char *g, *s;
        bool m;
    } t[] = {
        {"a*", "a", true},          /* '*' empty */
        {"*", "", true},
        {"a*b", "ab", true},
        {"a*b", "axxb", true},      /* in the middle */
        {"a*b", "axxbc", false},
        {"*a*b*", "xxaxxbxx", true}, /* several, at both ends */
        {"*a*b*", "xxbxxaxx", false},
        {"a?c", "abc", true},       /* '?' against one byte */
        {"a?c", "ac", false},       /* and none */
        {"a?c", "abbc", false},
        {"a\\*", "a*", true},       /* escapes */
        {"a\\*", "ab", false},
        {"a\\?", "a?", true},
        {"a\\?", "ab", false},
        {"a\\\\", "a\\", true},
        {"a/*", "a/b/c", true},     /* '/' is an ordinary byte */
        {"a.*:x", "a.b:c:x", true},
        {"\xc3\xa6*", "\xc3\xa6x", true}, /* bytes above 127 */
        {"?", "\xc3\xa6", false},           /* one byte, not one character */
        {"??", "\xc3\xa6", true},
        {"*a*a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
        {"acme/site?\?/*/temp?","acme/site07/line3/temp1", true},
        {"acme/site?\?/*/temp?","acme/site7/line3/temp1", false},
    };
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        bool m = glob_match(t[i].g, strlen(t[i].g), t[i].s, strlen(t[i].s));
        if (m != t[i].m) {
            fprintf(stderr, "  glob \"%s\" on \"%s\": %d\n", t[i].g, t[i].s, m);
            failures++;
        }
    }
    /* a name of 65,535 bytes, and the adversarial shape against 1000 bytes (bounded: glen x len steps) */
    size_t big = 65535;
    char *name = malloc(big + 1);
    memset(name, 'a', big);
    name[big - 1] = 'z';
    CHECK(glob_match("a*z", 3, name, big) && !glob_match("a*y", 3, name, big) && glob_match("*?z", 3, name, big));
    const char *adv = "*a*a*a*a*a*a*a*a*b";
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < 100; i++)
        CHECK(!glob_match(adv, strlen(adv), name, 1000));
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double us_each = ((double)(t1.tv_sec - t0.tv_sec) * 1e9 + (double)(t1.tv_nsec - t0.tv_nsec)) / 100 / 1e3;
    CHECK(us_each < 500); /* about 1 us measured; quadratic would be far beyond, even under sanitizers */
    free(name);
    /* random patterns over a small alphabet against the recursive matcher */
    unsigned seed = 12345;
    const char alpha[] = "ab/*?\\";
    for (int i = 0; i < 200000; i++) {
        char g[12], s[12];
        size_t gl = (size_t)rand_r(&seed) % 9, sl = (size_t)rand_r(&seed) % 9;
        for (size_t j = 0; j < gl; j++)
            g[j] = alpha[rand_r(&seed) % 6];
        for (size_t j = 0; j < sl; j++)
            s[j] = alpha[rand_r(&seed) % 3];
        if (gl && g[gl - 1] == '\\' && (gl < 2 || g[gl - 2] != '\\'))
            continue; /* not a valid glob (a lone '\' at the end) */
        bool lone = false; /* only globs as sel_normalize accepts them */
        for (size_t j = 0; j < gl; j++)
            if (g[j] == '\\' && ++j == gl)
                lone = true;
        if (lone)
            continue;
        if (glob_match(g, gl, s, sl) != ref_match(g, gl, s, sl)) {
            fprintf(stderr, "  glob \"%.*s\" on \"%.*s\" differs from the reference\n", (int)gl, g, (int)sl, s);
            failures++;
            break;
        }
        /* and its normal form matches the same names */
        char out[16];
        uint8_t k;
        size_t n;
        if (gl && sel_normalize(SEL_GLOB, g, gl, &k, out, &n) == 0) {
            user_grant ng = {.kind = k, .sel = out, .len = (uint16_t)n};
            if (grant_matches(&ng, s, sl) != ref_match(g, gl, s, sl)) {
                fprintf(stderr, "  the normal form of \"%.*s\" (%u \"%s\") differs on \"%.*s\"\n", (int)gl, g, k, out,
                        (int)sl, s);
                failures++;
                break;
            }
        }
    }
    printf("  globs: the table of §9, 65,535-byte names, an adversarial pattern, 200,000 random ones\n");
}

static double ms_since(const struct timespec *t0)
{
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    return (double)(t1.tv_sec - t0->tv_sec) * 1e3 + (double)(t1.tv_nsec - t0->tv_nsec) / 1e6;
}

/* The bit-parallel matcher on globs of one to five words (up to 300 tokens) against the recursive matcher, names
 * made from the glob so that about half match; then review 2's worst case: 64 globs of A24's longest that pass
 * the literal prefix and suffix filters, against the longest names (rights_of took 578 ms on 65,535 bytes with the
 * backtracking matcher). */
static void globs_linear(void)
{
    unsigned seed = 4242;
    int matched = 0;
    for (int i = 0; i < 20000; i++) {
        char g[700], s[1400];
        size_t gl = 0, sl = 0, ntok = 40 + (size_t)rand_r(&seed) % 261, stars = 0;
        for (size_t t = 0; t < ntok; t++) {
            unsigned r = (unsigned)rand_r(&seed) % 100;
            if (r < 2 && stars < 2 && (!gl || g[gl - 1] != '*')) { /* the reference is polynomial in 2 stars */
                g[gl++] = '*', stars++;
                for (int k = rand_r(&seed) % 4; k > 0; k--)
                    s[sl++] = "ab*"[rand_r(&seed) % 3];
            } else if (r < 20) {
                g[gl++] = '?';
                s[sl++] = "ab?"[rand_r(&seed) % 3];
            } else if (r < 23) {
                g[gl++] = '\\', g[gl++] = '*';
                s[sl++] = '*';
            } else {
                char c = "ab"[rand_r(&seed) % 2];
                g[gl++] = c;
                s[sl++] = c;
            }
        }
        if (rand_r(&seed) % 2 && sl) /* one byte changed, dropped or added */
            switch (rand_r(&seed) % 3) {
            case 0: s[rand_r(&seed) % sl] ^= 3; break;
            case 1: sl--; break;
            default: s[sl++] = 'a'; break;
            }
        bool want = ref_match(g, gl, s, sl);
        matched += want;
        user_grant ng;
        if (glob_match(g, gl, s, sl) != want || grant_init(&ng, SEL_GLOB, R_READ, g, gl) != 0 ||
            grant_matches(&ng, s, sl) != want) {
            fprintf(stderr, "  glob of %zu bytes on %zu bytes differs from the reference (want %d)\n", gl, sl, want);
            failures++;
            break;
        }
        grant_clear(&ng);
    }
    CHECK(matched > 5000 && matched < 15000);

    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/globcost-%d", base_dir, (int)getpid());
    CHECK(mkdir(dir, 0755) == 0);
    users us;
    OK(users_open(&us, dir, true));
    user_verifier v = {.iterations = ITER_MIN, .salt_len = 16};
    OK(users_create(&us, 0, "acme/g", 0, &v, NULL));
    for (int i = 0; i < USER_GLOBS; i++) {
        char g[300];
        int n = snprintf(g, sizeof g, "acme/*");
        memset(g + n, 'a', 240);
        n += 240;
        n += snprintf(g + n, sizeof g - (size_t)n, "c%02d*b", i); /* 251 bytes */
        OK(users_grant(&us, 0, "acme/g", SEL_GLOB, g, (size_t)n, R_READ));
    }
    user *u = (user *)users_find(&us, "acme/g");
    size_t len = 65535;
    char *name = malloc(len);
    memcpy(name, "acme/", 5);
    memset(name + 5, 'a', len - 6);
    name[len - 1] = 'b';
    CHECK(user_index_build(u));
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    CHECK(rights_of(u, name, len) == 0);
    double ms = ms_since(&t0);
    name[32766] = 'b';
    clock_gettime(CLOCK_MONOTONIC, &t0);
    CHECK(rights_of(u, name, 32767) == 0);
    double ms2 = ms_since(&t0);
    memcpy(name + 1000, "c07", 3); /* and one that matches: glob 7 */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    CHECK(rights_of(u, name, 32767) == R_READ);
    double ms3 = ms_since(&t0);
    /* about 13 ms here, 70 under TSan, 106 under ASan; the backtracking matcher took 578 ms on the first */
    CHECK(ms < 300 && ms2 < 300 && ms3 < 300);
    free(name);
    users_close(&us);
    cleanup(dir);
    printf("  globs: 20,000 random globs of 40-300 tokens against the reference; 64 globs x a 65,535-byte name: %.1f "
           "ms (32,767 bytes: %.1f ms, a match %.1f ms)\n", ms, ms2, ms3);
}

static void free_grant(user_grant *g) { grant_clear(g); }

/* Every cell of §2's inside table, for and against. */
static void inside(void)
{
    static const struct {
        const char *s, *t;
        bool in;
    } t[] = {
        {"all", "all", true},
        {"all", "prefix:acme/", false},
        {"all", "glob:acme/s*/t?", false},
        {"all", "name:acme/s1/t1", false},
        {"prefix:acme/s/", "all", true},
        {"prefix:acme/s/", "prefix:acme/", true},
        {"prefix:acme/", "prefix:acme/", true},
        {"prefix:acme/", "prefix:acme/s/", false},
        {"prefix:globex/", "prefix:acme/", false},
        {"prefix:acme/s1/t", "glob:acme/s*/t?", false}, /* a prefix is never inside a glob */
        {"prefix:acme/s1/t", "glob:acme/s*/t*", false}, /* not even one the glob covers */
        {"prefix:acme/s1/t1", "glob:acme/s*", true},    /* glob:acme/s* is prefix:acme/s */
        {"prefix:acme/s1/t1", "name:acme/s1/t1", false},
        {"glob:acme/s*/t?", "all", true},
        {"glob:acme/s*/t?", "prefix:acme/", true},      /* its literal prefix acme/s starts with acme/ */
        {"glob:acme/s*/t?", "prefix:acme/s", true},
        {"glob:acme/s*/t?", "prefix:acme/s1", false},   /* acme/s does not start with acme/s1 */
        {"glob:acme/s*/t?", "glob:acme/s*/t?", true},
        {"glob:acme/s**/t?", "glob:acme/s*/t?", true},  /* equal once normalized */
        {"glob:acme/s*/t?", "glob:acme/s*/x?", false},  /* the same literal prefix */
        {"glob:acme/s1/t?", "glob:acme/s*/t?", false},  /* a subset, but inclusion is not decided */
        {"glob:acme/s*/t?", "name:acme/s1/t1", false},
        {"name:acme/s1/t1", "all", true},
        {"name:acme/s1/t1", "prefix:acme/", true},
        {"name:acme/s1/t1", "prefix:globex/", false},
        {"name:acme/s1/t1", "glob:acme/s*/t?", true},
        {"name:acme/s1/x1", "glob:acme/s*/t?", false},
        {"name:acme/s1/t1", "name:acme/s1/t1", true},
        {"name:acme/s1/t2", "name:acme/s1/t1", false},
        {"glob:acme/*", "prefix:acme/", true},          /* compared as prefix:acme/ */
        {"prefix:acme/x", "glob:acme/*", true},         /* that glob is prefix:acme/ */
        {"glob:acme/x", "name:acme/x", true},           /* a glob without wildcards is a name */
    };
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        user_grant s = sel(t[i].s), g = sel(t[i].t);
        if (sel_inside(&s, &g) != t[i].in) {
            fprintf(stderr, "  %s inside %s: %d, want %d\n", t[i].s, t[i].t, !t[i].in, t[i].in);
            failures++;
        }
        free_grant(&s), free_grant(&g);
    }
    printf("  inside-rules: every cell of the table for and against (%zu cases)\n", sizeof t / sizeof *t);
}

static void free_user(user *u) { user_free(u); }

static bool grant_ok(const user *x, const user *u, const char *s)
{
    user_grant g = sel(s);
    bool r = may_grant(x, u, &g);
    free_grant(&g);
    return r;
}

static bool revoke_ok(const user *x, const char *s)
{
    user_grant g = sel(s);
    bool r = may_revoke(x, &g);
    free_grant(&g);
    return r;
}

/* A9: who manages whom, and what each may grant; every escalation the design names must be refused. */
static void escalation(void)
{
    user *root = mk(1, "root", USER_SUPERUSER);
    user *admin = mk(2, "acme/admin", 0); /* the tenant admin */
    give(admin, "prefix:acme/", R_ADMIN);
    user *bob = mk(3, "acme/bob", 0);
    give(bob, "prefix:acme/s/", R_READ);
    user *shared = mk(4, "acme/shared", 0); /* holds a share a superuser made */
    give(shared, "prefix:acme/", R_READ);
    give(shared, "name:weather/oslo", R_READ);
    user *carol = mk(5, "globex/carol", 0);
    user *asu = mk(6, "acme/su", USER_SUPERUSER);
    user *gadm = mk(7, "acme/users/gadmin", 0); /* admin through a glob, plus a user namespace */
    give(gadm, "glob:acme/s*/t?", R_ADMIN);
    give(gadm, "prefix:acme/users/", R_ADMIN);
    user *u1 = mk(8, "acme/users/u1", 0);
    user *globonly = mk(9, "acme/g", 0);
    give(globonly, "glob:acme/*/x?", R_ADMIN);
    user *nobody = mk(10, "acme/nobody", 0);
    user *wide = mk(11, "acme/wide", 0);
    give(wide, "all", R_ADMIN);

    /* who manages whom */
    CHECK(user_manages(NULL, asu) && user_manages(root, asu) && user_manages(root, carol));
    CHECK(user_manages(admin, bob));
    CHECK(!user_manages(admin, shared)); /* a right admin could not have given: no takeover of bob's password */
    CHECK(!user_manages(admin, carol));  /* not under its prefix */
    CHECK(!user_manages(admin, asu));    /* a superuser, though named under acme/ */
    CHECK(!user_manages(globonly, u1) && !user_manages(globonly, nobody)); /* a glob admin manages nobody */
    CHECK(user_manages(gadm, u1) && !user_manages(gadm, bob));
    CHECK(user_manages(wide, carol) && !user_manages(wide, asu));
    CHECK(!user_manages(nobody, u1) && !user_manages(bob, nobody));

    /* create */
    CHECK(may_create(NULL, "x", USER_SUPERUSER) && may_create(root, "x", USER_SUPERUSER));
    CHECK(may_create(admin, "acme/new", 0) && !may_create(admin, "globex/new", 0));
    CHECK(!may_create(admin, "acme/new", USER_SUPERUSER)); /* only superusers create superusers */
    CHECK(!may_create(admin, "acmecorp/x", 0)); /* the prefix acme/ ends in '/': acmecorp/ is outside */
    CHECK(!may_create(globonly, "acme/x/x1", 0)); /* user names under prefixes only */
    CHECK(may_create(wide, "anything", 0) && !may_create(bob, "acme/s/new", 0));

    /* grant */
    CHECK(grant_ok(admin, bob, "prefix:acme/x/") && grant_ok(admin, bob, "glob:acme/*/t?") &&
          grant_ok(admin, bob, "name:acme/q"));
    CHECK(!grant_ok(admin, bob, "prefix:globex/") && !grant_ok(admin, bob, "all") &&
          !grant_ok(admin, bob, "name:weather/oslo"));
    CHECK(!grant_ok(admin, carol, "prefix:acme/x/")); /* inside, but carol is not admin's to manage */
    CHECK(!grant_ok(admin, shared, "prefix:acme/x/"));
    CHECK(!grant_ok(gadm, u1, "prefix:acme/s/"));     /* a glob admin granting a prefix */
    CHECK(!grant_ok(gadm, u1, "glob:acme/s*/x?"));    /* another glob of the same literal prefix */
    CHECK(grant_ok(gadm, u1, "name:acme/s1/t1"));     /* a name it matches */
    CHECK(!grant_ok(gadm, u1, "name:acme/s1/x1"));    /* and one it does not */
    CHECK(grant_ok(gadm, u1, "glob:acme/s*/t?"));     /* the glob itself */
    CHECK(grant_ok(root, carol, "all") && grant_ok(NULL, asu, "all"));
    CHECK(!grant_ok(bob, bob, "prefix:acme/s/"));     /* read is not admin */

    /* revoke: any user, selectors inside the actor's admin selectors */
    CHECK(revoke_ok(admin, "prefix:acme/s/") && revoke_ok(admin, "name:acme/x"));
    CHECK(!revoke_ok(admin, "name:weather/oslo") && !revoke_ok(admin, "all") && !revoke_ok(bob, "prefix:acme/s/"));
    CHECK(revoke_ok(gadm, "name:acme/s9/t9") && !revoke_ok(gadm, "prefix:acme/s"));
    CHECK(revoke_ok(root, "all") && revoke_ok(NULL, "prefix:x"));

    /* password, list */
    CHECK(may_passwd(bob, bob) && may_passwd(admin, bob) && !may_passwd(admin, shared) && !may_passwd(bob, admin));
    CHECK(!may_passwd(admin, carol) && may_passwd(root, carol) && may_passwd(NULL, asu) && !may_passwd(admin, asu));
    CHECK(may_list(bob, bob) && may_list(admin, bob) && !may_list(admin, carol) && !may_list(bob, admin));

    /* and the rights a tenant admin holds on series: admin implies read, insert and delete */
    CHECK(rights_of(admin, "acme/x", 6) == R_EVERY && rights_of(admin, "globex/x", 8) == 0);

    user *all[] = {root, admin, bob, shared, carol, asu, gadm, u1, globonly, nobody, wide};
    for (size_t i = 0; i < sizeof all / sizeof *all; i++)
        free_user(all[i]);
    printf("  manage, create, grant, revoke, password and list checks of A9, escalations refused\n");
}

/* The name table (REVIEW1 #8): users added and dropped at random, in a table small enough that its clusters wrap
 * around; after every step each live name is found, each dropped one is not, as a scan of the array says. */
static void name_table(void)
{
    users us;
    memset(&us, 0, sizeof us);
    us.fd = -1;
    unsigned seed = 11;
    uint32_t next = 1;
    char name[32];
    int bad = 0;
    for (int step = 0; step < 20000 && !bad; step++) {
        if (us.n < 40 && (us.n < 10 || rand_r(&seed) % 2)) {
            if (us.n == us.cap) {
                us.cap = us.cap ? 2 * us.cap : 16;
                us.u = realloc(us.u, us.cap * sizeof *us.u);
            }
            snprintf(name, sizeof name, "u%u", rand_r(&seed) % 100000);
            if (users_find(&us, name))
                continue;
            us.u[us.n++] = mk(next++, name, 0); /* uids grow: the array stays sorted */
            CHECK(names_add(&us, us.u[us.n - 1]) == 0);
        } else {
            uint32_t i = (uint32_t)rand_r(&seed) % us.n;
            snprintf(name, sizeof name, "%s", us.u[i]->name);
            names_del(&us, us.u[i]);
            free_user(us.u[i]);
            memmove(us.u + i, us.u + i + 1, (us.n - i - 1) * sizeof *us.u);
            us.n--;
            bad |= users_find(&us, name) != NULL;
        }
        for (uint32_t i = 0; i < us.n; i++)
            bad |= users_find(&us, us.u[i]->name) != us.u[i];
        bad |= us.names_n != us.n;
    }
    CHECK(!bad);
    users_close(&us);
    printf("  the name table: 20000 random adds and drops, every name found where a scan finds it\n");
}

static void matching(void)
{
    user *u = mk(1, "u", 0);
    give(u, "name:acme/a", R_READ);
    give(u, "prefix:acme/b/", R_INSERT);
    give(u, "prefix:acme/", R_DELETE);
    give(u, "glob:acme/*/t?", R_READ | R_INSERT);
    give(u, "glob:x?y", R_ADMIN);
    CHECK(rights_of(u, "acme/a", 6) == (R_READ | R_DELETE));
    CHECK(rights_of(u, "acme/b/q", 8) == (R_INSERT | R_DELETE));
    CHECK(rights_of(u, "acme/s/t1", 9) == (R_READ | R_INSERT | R_DELETE)); /* a miss only a glob answers */
    CHECK(rights_of(u, "acme/s/t12", 10) == R_DELETE);
    CHECK(rights_of(u, "xzy", 3) == R_EVERY && rights_of(u, "xy", 2) == 0);
    CHECK(rights_of(u, "acm", 3) == 0 && rights_of(u, "", 0) == 0);
    /* a grant change that removes the glob: the index is rebuilt */
    grant_clear(&u->g[3]);
    memmove(u->g + 3, u->g + 4, sizeof *u->g);
    u->ng--;
    umatch_free(u->m);
    u->m = NULL;
    CHECK(rights_of(u, "acme/s/t1", 9) == R_DELETE);
    u->flags = USER_SUPERUSER;
    CHECK(rights_of(u, "anything", 8) == R_EVERY);
    free_user(u);

    /* random users and names: the index answers what trying every grant answers */
    unsigned seed = 7;
    const char alpha[] = "ab/";
    for (int round = 0; round < 300; round++) {
        user *r = mk(1, "r", 0);
        int ng = rand_r(&seed) % 40;
        for (int i = 0; i < ng; i++) {
            char s[16], text[24];
            size_t n = (size_t)rand_r(&seed) % 6;
            for (size_t j = 0; j < n; j++)
                s[j] = "ab/*?"[rand_r(&seed) % ((i % 3 == 2) ? 5 : 3)];
            s[n] = '\0';
            static const char *kinds[] = {"prefix:", "name:", "glob:"};
            if (rand_r(&seed) % 50 == 0)
                snprintf(text, sizeof text, "all");
            else
                snprintf(text, sizeof text, "%s%s", kinds[i % 3], s);
            uint8_t kind;
            const char *sp;
            size_t len;
            char out[24];
            uint8_t k;
            size_t on;
            sel_parse(text, &kind, &sp, &len);
            if (sel_normalize(kind, sp, len, &k, out, &on) != 0)
                continue;
            /* one grant per (kind, selector), as USERS keeps them */
            bool dup = false;
            for (uint32_t j = 0; j < r->ng; j++)
                dup |= r->g[j].kind == k && r->g[j].len == on && memcmp(r->g[j].sel, out, on) == 0;
            if (!dup)
                give(r, text, (uint8_t)(1 + rand_r(&seed) % 15));
        }
        for (int i = 0; i < 300; i++) {
            char name[12];
            size_t n = (size_t)rand_r(&seed) % 9;
            for (size_t j = 0; j < n; j++)
                name[j] = alpha[rand_r(&seed) % 3];
            if (rights_of(r, name, n) != rights_of_slow(r, name, n)) {
                fprintf(stderr, "  rights_of differs from trying every grant on \"%.*s\"\n", (int)n, name);
                failures++;
                round = 1000;
                break;
            }
            /* user names under r's admin prefixes: the index against every grant */
            char uname[12];
            memcpy(uname, name, n);
            uname[n] = '\0';
            if (n && names_under(r, uname) != names_under_slow(r, uname)) {
                fprintf(stderr, "  names_under differs from trying every grant on \"%s\"\n", uname);
                failures++;
                round = 1000;
                break;
            }
        }
        /* selectors inside r's admin selectors (who may grant, revoke and manage, REVIEW1 #8): the index against
         * every grant tried */
        for (int i = 0; i < 300; i++) {
            char s[16], out[24];
            size_t n = (size_t)rand_r(&seed) % 6;
            for (size_t j = 0; j < n; j++)
                s[j] = "ab/*?"[rand_r(&seed) % 5];
            user_grant g = {0};
            size_t on;
            uint8_t kind = (uint8_t)(rand_r(&seed) % 4);
            if (sel_normalize(kind, s, kind == SEL_ALL ? 0 : n, &g.kind, out, &on) != 0)
                continue;
            g.sel = out, g.len = (uint16_t)on;
            if (g.kind == SEL_GLOB && glob_parts(&g) != 0)
                continue;
            if (inside_admin(r, &g) != inside_admin_slow(r, &g)) {
                fprintf(stderr, "  inside_admin differs from trying every grant on kind %u \"%s\"\n", g.kind, out);
                failures++;
                round = 1000;
            }
            free(g.lit), free(g.suf), free(g.prog);
        }
        free_user(r);
    }
    printf("  rights_of, inside_admin, names_under: grants of every kind, a glob removed, superuser; the index "
           "against every grant tried\n");
}

/* ------------------------------------------------------------- the file */

/* A file with a history: sizes[i] and states[i] after change i (0: the empty file). Returns the count. */
static int build_history(const char *dir, off_t *sizes, char **states, int cap)
{
    users us;
    OK(users_open(&us, dir, true));
    char path[4200];
    snprintf(path, sizeof path, "%s/USERS", dir);
    int k = 0;
    sizes[k] = 0, states[k++] = dump_dup(&us);
    user_verifier v = verifier("pw");
#define STEP(expr)                                                                                                 \
    do {                                                                                                           \
        OK(expr);                                                                                                  \
        if (k < cap)                                                                                               \
            sizes[k] = file_size(path), states[k++] = dump_dup(&us);                                               \
    } while (0)
    STEP(users_create(&us, 0, "root", USER_SUPERUSER, &v, NULL));
    STEP(users_create(&us, 0, "acme/admin", 0, &v, NULL));
    STEP(users_grant(&us, 0, "acme/admin", SEL_PREFIX, "acme/", 5, R_ADMIN));
    STEP(users_create(&us, 0, "acme/bob", 0, NULL, NULL));
    STEP(users_grant(&us, 0, "acme/bob", SEL_GLOB, "acme/*/t?", 9, R_READ));
    STEP(users_grant(&us, 0, "acme/bob", SEL_NAME, "acme/x", 6, R_INSERT));
    STEP(users_set_verifier(&us, 0, "acme/bob", &v));
    STEP(users_set_flags(&us, 0, "acme/bob", USER_SUPERUSER));
    STEP(users_set_flags(&us, 0, "acme/bob", 0));
    STEP(users_revoke(&us, 0, "acme/bob", SEL_NAME, "acme/x", 6, R_INSERT));
    STEP(users_drop(&us, 0, "acme/admin"));
    STEP(users_grant(&us, 0, "acme/bob", SEL_ALL, "", 0, R_READ)); /* the last change: one record */
#undef STEP
    users_close(&us);
    return k;
}

static void round_trip(void)
{
    char dir[600], path[700];
    fresh("trip", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    off_t sizes[32];
    char *states[32];
    int k = build_history(dir, sizes, states, 32);
    struct stat st;
    CHECK(stat(path, &st) == 0 && (st.st_mode & 0777) == 0600);
    users us;
    OK(users_open(&us, dir, false));
    CHECK(strcmp(dump(&us), states[k - 1]) == 0);
    const user *bob = users_find(&us, "acme/bob");
    CHECK(bob && bob->uid == 3 && bob->has_verifier && bob->ng == 2 && !users_find(&us, "acme/admin"));
    CHECK(us.max_uid == 3 && users_superusers(&us) == 1);
    /* a read-only one refuses changes */
    CHECK(users_create(&us, 0, "x", 0, NULL, NULL) == TSDB_ERR_READONLY);
    users_close(&us);
    /* argument checks, before anything is written */
    OK(users_open(&us, dir, true));
    user_verifier v = verifier("pw"), weak = v;
    weak.iterations = 4095;
    CHECK(users_create(&us, 0, "bad name", 0, NULL, NULL) == TSDB_ERR_INVALID);
    CHECK(users_create(&us, 0, "acme/bob", 0, NULL, NULL) == TSDB_ERR_INVALID); /* exists */
    CHECK(users_create(&us, 0, "x", 0, &weak, NULL) == TSDB_ERR_INVALID);
    weak = v, weak.salt_len = 15;
    CHECK(users_create(&us, 0, "x", 0, &weak, NULL) == TSDB_ERR_INVALID);
    CHECK(users_create(&us, 0, "x", 2, NULL, NULL) == TSDB_ERR_INVALID);
    CHECK(users_grant(&us, 0, "acme/bob", SEL_PREFIX, "a", 1, 0) == TSDB_ERR_INVALID);
    CHECK(users_grant(&us, 0, "acme/bob", SEL_PREFIX, "a", 1, 16) == TSDB_ERR_INVALID);
    CHECK(users_grant(&us, 0, "acme/bob", SEL_GLOB, "a\\", 2, R_READ) == TSDB_ERR_INVALID);
    CHECK(users_grant(&us, 0, "nobody", SEL_PREFIX, "a", 1, R_READ) == TSDB_ERR_NOTFOUND);
    CHECK(users_drop(&us, 0, "nobody") == TSDB_ERR_NOTFOUND);
    CHECK(users_set_verifier(&us, 0, "acme/bob", &weak) == TSDB_ERR_INVALID);
    CHECK(file_size(path) == sizes[k - 1]);
    /* changes that change nothing write nothing; a revoke of a grant not held says so */
    CHECK(users_revoke(&us, 0, "acme/bob", SEL_NAME, "nothing", 7, R_READ) == TSDB_ERR_NOTFOUND);
    OK(users_grant(&us, 0, "acme/bob", SEL_ALL, "", 0, R_READ));
    OK(users_set_flags(&us, 0, "acme/bob", 0));
    CHECK(file_size(path) == sizes[k - 1]);
    /* the glob cap: 64 glob grants, the 65th refused (a glob normalized to a prefix is not a glob) */
    CHECK(glob_count(users_find(&us, "acme/bob")) == 1);
    for (int i = 1; i < USER_GLOBS; i++) {
        char g[32];
        snprintf(g, sizeof g, "g%d/*?", i);
        OK(users_grant(&us, 0, "acme/bob", SEL_GLOB, g, strlen(g), R_READ));
    }
    CHECK(users_grant(&us, 0, "acme/bob", SEL_GLOB, "z*?", 3, R_READ) == TSDB_ERR_INVALID);
    OK(users_grant(&us, 0, "acme/bob", SEL_GLOB, "g2/*?", 5, R_INSERT)); /* an existing one may change */
    OK(users_grant(&us, 0, "acme/bob", SEL_GLOB, "z/*", 3, R_READ));
    OK(users_revoke(&us, 0, "acme/bob", SEL_GLOB, "g1/*?", 5, R_READ));
    OK(users_grant(&us, 0, "acme/bob", SEL_GLOB, "z*?", 3, R_READ));
    char *now = dump_dup(&us);
    users_close(&us);
    OK(users_open(&us, dir, false));
    CHECK(strcmp(dump(&us), now) == 0);
    users_close(&us);
    free(now);
    for (int i = 0; i < k; i++)
        free(states[i]);
    cleanup(dir);
    printf("  round trip of every record type, mode 0600, argument checks, no-op changes, the glob cap\n");
}

/* The file cut at every length: it opens with the changes that are whole, and the next writer cuts the rest. */
static void torn_tail(void)
{
    char dir[600], path[700], cut[600], cpath[700];
    fresh("torn", dir, sizeof dir);
    fresh("torn_cut", cut, sizeof cut);
    snprintf(path, sizeof path, "%s/USERS", dir);
    snprintf(cpath, sizeof cpath, "%s/USERS", cut);
    off_t sizes[32];
    char *states[32];
    int k = build_history(dir, sizes, states, 32);
    off_t total = sizes[k - 1];
    int opened = 0;
    for (off_t len = 0; len <= total; len++) {
        copy_file(path, cpath, len);
        int whole = 0;
        while (whole + 1 < k && sizes[whole + 1] <= len)
            whole++;
        users us;
        int rc = users_open(&us, cut, true);
        CHECK(rc == 0);
        if (rc == 0 && strcmp(dump(&us), states[whole]) != 0) {
            fprintf(stderr, "  cut at %lld: not the state after change %d\n", (long long)len, whole);
            failures++;
        }
        opened += rc == 0;
        if (len % 7 == 0 || len == total - 1) {
            /* a writer cuts the torn tail and appends after the last whole change */
            user_verifier v = verifier("x");
            if (whole == 0 || !users_find(&us, "newcomer")) {
                rc = users_create(&us, 0, "newcomer", 0, &v, NULL);
                CHECK(rc == 0);
            }
            users_close(&us);
            CHECK(users_open(&us, cut, false) == 0 && users_find(&us, "newcomer"));
            if (whole && len < total)
                CHECK(file_size(cpath) > sizes[whole]);
        }
        users_close(&us);
    }
    for (int i = 0; i < k; i++)
        free(states[i]);
    cleanup(dir), cleanup(cut);
    printf("  a torn tail at each of %d lengths opens as the last whole change; a writer cuts it\n", opened);
}

/* Each byte flipped: inside the last record the file opens as before it (a torn append looks the same); anywhere
 * before it, a valid record follows the damage and the open fails. */
static void damage(void)
{
    char dir[600], path[700], bad[600], bpath[700];
    fresh("dmg", dir, sizeof dir);
    fresh("dmg_bad", bad, sizeof bad);
    snprintf(path, sizeof path, "%s/USERS", dir);
    snprintf(bpath, sizeof bpath, "%s/USERS", bad);
    off_t sizes[32];
    char *states[32];
    int k = build_history(dir, sizes, states, 32);
    off_t total = sizes[k - 1], last = sizes[k - 2];
    int corrupt = 0, cut = 0;
    for (off_t p = 0; p < total; p++) {
        copy_file(path, bpath, total);
        int fd = open(bpath, O_RDWR);
        uint8_t b;
        CHECK(pread(fd, &b, 1, p) == 1);
        b ^= (uint8_t)(1u << (p % 8));
        CHECK(pwrite(fd, &b, 1, p) == 1);
        close(fd);
        users us;
        int rc = users_open(&us, bad, false);
        if (p >= last) {
            CHECK(rc == 0 && strcmp(dump(&us), states[k - 2]) == 0);
            cut += rc == 0;
        } else {
            if (rc != TSDB_ERR_CORRUPT)
                fprintf(stderr, "  byte %lld flipped: %s\n", (long long)p, tsdb_strerror(rc));
            CHECK(rc == TSDB_ERR_CORRUPT);
            corrupt += rc == TSDB_ERR_CORRUPT;
        }
        users_close(&us);
    }
    for (int i = 0; i < k; i++)
        free(states[i]);
    cleanup(dir), cleanup(bad);
    printf("  every byte flipped: %d before the last record fail the open, %d in it open without it\n", corrupt, cut);
}

/* Writes records built by hand (each change a list of records) to dir/USERS. */
static void write_records(const char *dir, rbuf *b)
{
    char path[700];
    snprintf(path, sizeof path, "%s/USERS", dir);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    CHECK(fd >= 0 && syscall(SYS_write, fd, b->p, b->n) == (long)b->n);
    close(fd);
}

/* Records that check out but do not fit the state: the open fails rather than skip them. */
static void nonsense(void)
{
    char dir[600];
    fresh("nonsense", dir, sizeof dir);
    uint8_t key[32] = {1};
    user_verifier v = verifier("pw");
    for (int c = 0; c < 15; c++) {
        rbuf b;
        memset(&b, 0, sizeof b);
        if (c != 0)
            rec_header(&b, key);
        switch (c) {
        case 0: rec_user(&b, 1, 0, "a"); break;                              /* no header first */
        case 1: rec_header(&b, key); break;                                  /* a second header */
        case 2: rec_grant(&b, 5, SEL_PREFIX, R_READ, "a", 1); break;        /* a user that does not exist */
        case 3: rec_user(&b, 2, 0, "a"), rec_user(&b, 1, 0, "b"); break;    /* a uid that goes back */
        case 4: rec_user(&b, 1, 0, "a"), rec_user(&b, 2, 0, "a"); break;    /* a name that is taken */
        case 5: rec_user(&b, 1, 0, "/a"); break;                             /* not a user name */
        case 6: rec_user(&b, 1, 0, "a"), rec_grant(&b, 1, SEL_GLOB, R_READ, "a/*", 3); break; /* a glob not normal */
        case 7: rec_user(&b, 1, 0, "a"), rec_grant(&b, 1, SEL_PREFIX, R_READ, "", 0); break;  /* "" is all */
        case 8: rec_user(&b, 1, 0, "a"), rec_grant(&b, 1, SEL_NAME, 16, "x", 1); break;       /* rights */
        case 9: rec_user(&b, 1, 0, "a"), rec_grant(&b, 1, 4, R_READ, "x", 1); break;          /* kind */
        case 10: rec_user(&b, 1, 4, "a"); break;                                               /* flags */
        case 11: rec_user(&b, 1, 0, "a"), rec_u8(&b, REC_DROP, 1, -1), rec_u8(&b, REC_FLAGS, 1, 0); break;
        case 12: {
            user_verifier w = v;
            w.iterations = 1;
            rec_user(&b, 1, 0, "a"), rec_verifier(&b, 1, &w);
            break;
        }
        case 13: rec_user(&b, 1, 0, "a"), rec_u8(&b, 9, 1, -1); break; /* an unknown type */
        case 14: rec_user(&b, 1, 0, "a"), rec_u8(&b, 0, 1, -1); break; /* type 0 */
        }
        write_records(dir, &b);
        users us;
        int rc = users_open(&us, dir, false);
        if (rc != TSDB_ERR_CORRUPT)
            fprintf(stderr, "  nonsense case %d: %s\n", c, tsdb_strerror(rc));
        CHECK(rc == TSDB_ERR_CORRUPT && us.n == 0);
        users_close(&us);
        free(b.p);
    }
    cleanup(dir);
    printf("  records that check out but make no sense fail the open (15 kinds)\n");
}

static void append_raw(const char *dir, rbuf *b, size_t from, size_t len)
{
    char path[700];
    snprintf(path, sizeof path, "%s/USERS", dir);
    int fd = open(path, O_WRONLY | O_APPEND);
    CHECK(fd >= 0 && syscall(SYS_write, fd, b->p + from, len) == (long)len);
    close(fd);
}

static void reload(void)
{
    char dir[600], path[700];
    fresh("reload", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    users a, w;
    OK(users_open(&a, dir, true));
    OK(users_open(&w, dir, true));
    user_verifier v = verifier("pw");
    OK(users_create(&w, 0, "root", USER_SUPERUSER, &v, NULL));
    CHECK(a.n == 0);
    OK(users_reload(&a));
    CHECK(a.n == 1 && same(&a, &w));
    OK(users_create(&w, 0, "bob", 0, &v, NULL));
    OK(users_grant(&w, 0, "bob", SEL_PREFIX, "b/", 2, R_READ));
    /* the other one writes after reading what w wrote: under the lock it catches up first */
    OK(users_grant(&a, 0, "bob", SEL_PREFIX, "c/", 2, R_READ));
    CHECK(users_find(&a, "bob") && users_find(&a, "bob")->ng == 2);
    OK(users_reload(&w));
    char *sa = dump_dup(&a);
    CHECK(strcmp(sa, dump(&w)) == 0);
    free(sa);

    /* half a record is not yet one: read when it is whole */
    rbuf b;
    memset(&b, 0, sizeof b);
    b.base = (uint64_t)file_size(path);
    rec_grant(&b, 2, SEL_NAME, R_INSERT, "n", 1);
    size_t half = b.n / 2;
    append_raw(dir, &b, 0, half);
    sa = dump_dup(&a);
    OK(users_reload(&a));
    CHECK(strcmp(sa, dump(&a)) == 0);
    append_raw(dir, &b, half, b.n - half);
    OK(users_reload(&a));
    CHECK(users_find(&a, "bob")->ng == 3);
    free(sa);
    free(b.p);

    /* a batch that meets a record that does not fit after one that does: neither is applied (A11) */
    sa = dump_dup(&a);
    memset(&b, 0, sizeof b);
    b.base = (uint64_t)file_size(path);
    rec_grant(&b, 2, SEL_NAME, R_READ, "m", 1);
    rec_u8(&b, REC_FLAGS, 2, 1);
    rec_u8(&b, REC_DROP, 2, -1);
    rec_grant(&b, 99, SEL_NAME, R_READ, "m", 1);
    append_raw(dir, &b, 0, b.n);
    CHECK(users_reload(&a) == TSDB_ERR_CORRUPT && a.damaged);
    CHECK(strcmp(sa, dump(&a)) == 0);
    /* and changes are refused until a restart */
    CHECK(users_create(&a, 0, "late", 0, &v, NULL) == TSDB_ERR_CORRUPT);
    CHECK(users_reload(&a) == TSDB_ERR_CORRUPT && strcmp(sa, dump(&a)) == 0);
    free(sa);
    free(b.p);
    users_close(&a);
    users_close(&w);

    /* damage among new records, with valid ones after it: the reader keeps what it had */
    fresh("reload2", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    OK(users_open(&a, dir, true));
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "root", USER_SUPERUSER, &v, NULL));
    OK(users_reload(&a));
    off_t at = file_size(path);
    OK(users_create(&w, 0, "bob", 0, &v, NULL));
    OK(users_create(&w, 0, "carol", 0, &v, NULL));
    int fd = open(path, O_RDWR);
    uint8_t x;
    CHECK(pread(fd, &x, 1, at + 9) == 1);
    x ^= 0x20;
    CHECK(pwrite(fd, &x, 1, at + 9) == 1);
    close(fd);
    sa = dump_dup(&a);
    CHECK(users_reload(&a) == TSDB_ERR_CORRUPT && strcmp(sa, dump(&a)) == 0 && a.damaged);
    free(sa);
    users_close(&a);
    users_close(&w);
    CHECK(users_open(&a, dir, false) == TSDB_ERR_CORRUPT);
    users_close(&a);

    /* the file went away: no empty one takes its place, and the users stay */
    fresh("gone", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    OK(users_open(&a, dir, true));
    OK(users_create(&a, 0, "root", USER_SUPERUSER, &v, NULL));
    users_close(&a);
    OK(users_open(&a, dir, true));
    unlink(path);
    CHECK(users_create(&a, 0, "bob", 0, &v, NULL) == TSDB_ERR_CORRUPT && access(path, F_OK) != 0);
    CHECK(a.n == 1);
    users_close(&a);
    OK(users_open(&a, dir, true));
    OK(users_create(&a, 0, "root", USER_SUPERUSER, &v, NULL));
    unlink(path);
    CHECK(users_reload(&a) == TSDB_ERR_CORRUPT && a.n == 1);
    users_close(&a);
    cleanup(dir);
    printf("  reloads: incremental, half a record, a batch that does not fit, damage, a file that went away\n");
}

static void compaction(void)
{
    char dir[600], path[700];
    fresh("compact", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    off_t sizes[32];
    char *states[32];
    int k = build_history(dir, sizes, states, 32);
    users a, r;
    OK(users_open(&a, dir, true));
    OK(users_open(&r, dir, false));
    for (int i = 0; i < 50; i++)
        OK(users_grant(&a, 0, "acme/bob", SEL_NAME, "flip", 4, (uint8_t)(1 + i % 15)));
    OK(users_reload(&r));
    char *before = dump_dup(&a);
    off_t big = file_size(path);
    struct stat s0, s1;
    stat(path, &s0);
    OK(users_compact(&a));
    stat(path, &s1);
    CHECK(s1.st_ino != s0.st_ino && file_size(path) < big && (s1.st_mode & 0777) == 0600);
    CHECK(strcmp(before, dump(&a)) == 0);
    /* a reader of the old file sees another inode and reads the new one from the start */
    OK(users_reload(&r));
    CHECK(strcmp(before, dump(&r)) == 0);
    /* a writer that held the old file appends to the new one */
    users w;
    OK(users_open(&w, dir, true));
    OK(users_compact(&a));
    OK(users_grant(&w, 0, "acme/bob", SEL_NAME, "after", 5, R_READ));
    OK(users_reload(&a));
    OK(users_reload(&r));
    CHECK(same(&a, &w) && same(&r, &w));
    users_close(&w);
    /* uids are never reused: the highest was dropped before the compaction */
    user_verifier v = verifier("pw");
    uint32_t uid;
    OK(users_create(&a, 0, "temp", 0, &v, &uid));
    CHECK(uid == 4);
    OK(users_drop(&a, 0, "temp"));
    OK(users_compact(&a));
    users_close(&a);
    OK(users_open(&a, dir, true));
    CHECK(a.max_uid == 4 && !users_find(&a, "temp"));
    OK(users_create(&a, 0, "temp", 0, &v, &uid));
    CHECK(uid == 5);
    /* a failed rename leaves the file as it was and fails just the call */
    off_t size = file_size(path);
    arm(F_RENAME, EIO, false, false);
    CHECK(users_compact(&a) == TSDB_ERR_IO && f_fired == 1 && !a.failed && file_size(path) == size);
    OK(users_create(&a, 0, "after-rename", 0, &v, NULL));
    users_close(&a);
    users_close(&r);
    for (int i = 0; i < k; i++)
        free(states[i]);
    free(before);
    cleanup(dir);
    printf("  compaction: the same state, smaller, a new inode readers follow, uids not reused, rename failure\n");
}

/* A child that adds users and compacts while the parent reloads: every reload succeeds and is a state the writer
 * had (its users are u0..uN without gaps, each with its grant). */
static void compaction_under_reader(void)
{
    char dir[600];
    fresh("cread", dir, sizeof dir);
    users w;
    user_verifier v = verifier("pw");
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "root", USER_SUPERUSER, &v, NULL));
    users_close(&w);
    int n = 150;
    pid_t pid = fork();
    if (pid == 0) {
        users c;
        int bad = users_open(&c, dir, true);
        for (int i = 0; i < n && !bad; i++) {
            char name[32], g[32];
            snprintf(name, sizeof name, "u%d", i);
            snprintf(g, sizeof g, "t%d/", i);
            bad |= users_create(&c, 0, name, 0, &v, NULL);
            bad |= users_grant(&c, 0, name, SEL_PREFIX, g, strlen(g), R_READ);
            if (i % 5 == 4)
                bad |= users_compact(&c);
        }
        users_close(&c);
        _exit(bad ? 1 : 0);
    }
    users r;
    OK(users_open(&r, dir, false));
    int reloads = 0, st = 0;
    for (;;) {
        bool done = waitpid(pid, &st, WNOHANG) == pid;
        int rc = users_reload(&r);
        CHECK(rc == 0);
        reloads++;
        /* u0..u(m-1) present in order, each but maybe the newest with its grant */
        for (uint32_t i = 1; i < r.n; i++) {
            char name[32];
            snprintf(name, sizeof name, "u%u", i - 1);
            if (strcmp(r.u[i]->name, name) != 0 || (i + 1 < r.n && r.u[i]->ng != 1)) {
                fprintf(stderr, "  a reload saw a state the writer never had (user %u: %s)\n", i, r.u[i]->name);
                failures++;
                done = true;
                break;
            }
        }
        if (done || rc)
            break;
    }
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    OK(users_reload(&r));
    CHECK(r.n == (uint32_t)n + 1 && r.u[n]->ng == 1);
    users_close(&r);
    cleanup(dir);
    printf("  compaction under a reader in another process: %d reloads, each a state the writer had\n", reloads);
}

/* Two processes append at once under the OFD lock: every change of both is there, in a file that checks out. */
static void two_writers(void)
{
    char dir[600];
    fresh("two", dir, sizeof dir);
    users w;
    user_verifier v = verifier("pw");
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "shared", 0, &v, NULL));
    users_close(&w);
    int per = 200;
    pid_t kids[2];
    for (int k = 0; k < 2; k++) {
        if ((kids[k] = fork()) == 0) {
            users c;
            int bad = users_open(&c, dir, true);
            for (int i = 0; i < per && !bad; i++) {
                char g[32];
                snprintf(g, sizeof g, "p%d/%d", k, i);
                bad |= users_grant(&c, 0, "shared", SEL_NAME, g, strlen(g), (uint8_t)(1 + i % 15));
                if (i % 20 == 0) {
                    char name[32];
                    snprintf(name, sizeof name, "k%d-%d", k, i);
                    bad |= users_create(&c, 0, name, 0, NULL, NULL);
                }
            }
            users_close(&c);
            _exit(bad ? 1 : 0);
        }
    }
    for (int k = 0; k < 2; k++) {
        int st;
        CHECK(waitpid(kids[k], &st, 0) == kids[k] && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    }
    OK(users_open(&w, dir, false));
    const user *s = users_find(&w, "shared");
    CHECK(s && s->ng == 2u * (uint32_t)per && w.n == 1 + 2 * (uint32_t)(per / 20) && w.max_uid == w.n);
    users_close(&w);
    cleanup(dir);
    printf("  two processes appending at once: all %d changes, uids unique\n", 2 * (per + per / 20));
}

static int fail_calls;
static void on_fail(void *arg, const char *msg)
{
    (void)arg, (void)msg;
    fail_calls++;
}

/* A failed write or sync is fail-stop: the change is not acknowledged, nothing after it is, and a reopen shows the
 * acknowledged changes and not the failed one. */
static void failstop(void)
{
    static const struct {
        int op, err;
        bool partial, drop;
        const char *what;
    } t[] = {
        {F_WRITE, EIO, true, false, "a torn write (EIO)"},
        {F_WRITE, ENOSPC, false, false, "a full disk (ENOSPC)"},
        {F_WRITE, ENOSPC, true, false, "a full disk midway"},
        {F_FDATASYNC, EIO, false, true, "fdatasync (EIO, pages dropped)"},
        {F_FDATASYNC, EIO, false, false, "fdatasync (EIO, pages kept)"},
    };
    user_verifier v = verifier("pw");
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        char dir[600];
        fresh("fail", dir, sizeof dir);
        users us;
        OK(users_open(&us, dir, true));
        us.on_fail = on_fail;
        fail_calls = 0;
        OK(users_create(&us, 0, "root", USER_SUPERUSER, &v, NULL));
        OK(users_create(&us, 0, "bob", 0, &v, NULL));
        OK(users_grant(&us, 0, "bob", SEL_PREFIX, "acme/", 5, R_READ));
        char *acked = dump_dup(&us);
        char path[700];
        snprintf(path, sizeof path, "%s/USERS", dir);
        off_t before = file_size(path);
        arm(t[i].op, t[i].err, t[i].partial, t[i].drop);
        CHECK(users_revoke(&us, 0, "bob", SEL_PREFIX, "acme/", 5, R_READ) == TSDB_ERR_IO);
        CHECK(f_fired == 1 && us.failed && fail_calls == 1);
        /* not applied in memory, and nothing more acknowledged */
        CHECK(strcmp(acked, dump(&us)) == 0);
        CHECK(users_create(&us, 0, "carol", 0, &v, NULL) == TSDB_ERR_IO && fail_calls == 1);
        /* the failed records are cut off again (REVIEW1 #3): after a failed writeback their pages are clean, maybe
         * not on the disk, and a retry must not read them as its change already made */
        CHECK(file_size(path) == before);
        users_close(&us);
        /* a reopen: the acknowledged state, never anything torn or failed */
        users r;
        OK(users_open(&r, dir, true));
        const user *bob = users_find(&r, "bob");
        CHECK(bob && !users_find(&r, "carol"));
        CHECK(strcmp(acked, dump(&r)) == 0);
        /* the retry writes its record and syncs it */
        int syncs = f_syncs;
        OK(users_revoke(&r, 0, "bob", SEL_PREFIX, "acme/", 5, R_READ));
        CHECK(f_syncs > syncs && file_size(path) > before && users_find(&r, "bob")->ng == 0);
        OK(users_grant(&r, 0, "bob", SEL_PREFIX, "acme/", 5, R_READ));
        OK(users_create(&r, 0, "carol", 0, &v, NULL)); /* the next writer cuts what was torn */
        users_close(&r);
        OK(users_open(&r, dir, false));
        CHECK(users_find(&r, "carol") != NULL);
        users_close(&r);
        free(acked);
        cleanup(dir);
        printf("  fail-stop: %s\n", t[i].what);
    }
    /* the directory fsync of a new file: its name may be lost, so nothing is acknowledged */
    char dir[600];
    fresh("faildir", dir, sizeof dir);
    users us;
    OK(users_open(&us, dir, true));
    arm(F_FSYNC_DIR, EIO, false, false);
    CHECK(users_create(&us, 0, "root", USER_SUPERUSER, &v, NULL) == TSDB_ERR_IO && f_fired == 1 && us.failed);
    CHECK(us.n == 0);
    users_close(&us);
    /* and after a compaction's rename (the name is visible): fail-stop too */
    fresh("failcompact", dir, sizeof dir);
    OK(users_open(&us, dir, true));
    OK(users_create(&us, 0, "root", USER_SUPERUSER, &v, NULL));
    arm(F_FSYNC_DIR, EIO, false, false);
    CHECK(users_compact(&us) == TSDB_ERR_IO && f_fired == 1 && us.failed);
    CHECK(users_create(&us, 0, "bob", 0, &v, NULL) == TSDB_ERR_IO);
    users_close(&us);
    OK(users_open(&us, dir, false));
    CHECK(us.n == 1);
    users_close(&us);
    cleanup(dir);
    printf("  fail-stop: the directory fsync of a new USERS, and after a compaction's rename\n");
}

/* Online, actors are users: the checks of A9 hold against the file's state, and the last superuser stays. */
static void online(void)
{
    char dir[600];
    fresh("online", dir, sizeof dir);
    users us;
    user_verifier v = verifier("pw");
    OK(users_open(&us, dir, true));
    uint32_t root, admin, bob, carol, nobody;
    OK(users_create(&us, 0, "root", USER_SUPERUSER, &v, &root));
    OK(users_create(&us, root, "acme/admin", 0, &v, &admin));
    OK(users_grant(&us, root, "acme/admin", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_create(&us, admin, "acme/bob", 0, &v, &bob));
    CHECK(users_create(&us, admin, "globex/x", 0, &v, NULL) == TSDB_ERR_DENIED);
    CHECK(users_create(&us, admin, "acme/su", USER_SUPERUSER, &v, NULL) == TSDB_ERR_DENIED);
    OK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "acme/s/", 7, R_READ));
    CHECK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "globex/", 7, R_READ) == TSDB_ERR_DENIED);
    OK(users_create(&us, root, "globex/carol", 0, &v, &carol));
    OK(users_create(&us, root, "nobody", 0, &v, &nobody));
    /* a user admin cannot touch looks like one that does not exist */
    CHECK(users_drop(&us, admin, "globex/carol") == TSDB_ERR_DENIED);
    CHECK(users_drop(&us, admin, "globex/none") == TSDB_ERR_DENIED);
    CHECK(users_drop(&us, admin, "acme/none") == TSDB_ERR_NOTFOUND);
    /* a share made by the superuser moves bob out of admin's reach */
    OK(users_grant(&us, root, "acme/bob", SEL_NAME, "weather/oslo", 12, R_READ));
    CHECK(users_set_verifier(&us, admin, "acme/bob", &v) == TSDB_ERR_DENIED);
    CHECK(users_drop(&us, admin, "acme/bob") == TSDB_ERR_DENIED);
    OK(users_set_verifier(&us, bob, "acme/bob", &v)); /* bob's own */
    /* a tenant may cut a share of its own data from anyone */
    OK(users_grant(&us, root, "globex/carol", SEL_NAME, "acme/s/t", 8, R_READ));
    OK(users_revoke(&us, admin, "globex/carol", SEL_NAME, "acme/s/t", 8, R_READ));
    CHECK(users_revoke(&us, admin, "acme/bob", SEL_NAME, "weather/oslo", 12, R_READ) == TSDB_ERR_DENIED);
    OK(users_revoke(&us, root, "acme/bob", SEL_NAME, "weather/oslo", 12, R_READ));
    OK(users_set_verifier(&us, admin, "acme/bob", &v)); /* back in admin's reach */
    /* flags: superusers only; the last superuser is neither dropped nor demoted online */
    CHECK(users_set_flags(&us, admin, "acme/bob", USER_SUPERUSER) == TSDB_ERR_DENIED);
    CHECK(users_drop(&us, root, "root") == TSDB_ERR_DENIED);
    CHECK(users_set_flags(&us, root, "root", 0) == TSDB_ERR_DENIED);
    OK(users_set_flags(&us, root, "nobody", USER_SUPERUSER));
    OK(users_set_flags(&us, nobody, "root", 0)); /* now there is another */
    CHECK(users_drop(&us, nobody, "nobody") == TSDB_ERR_DENIED);
    /* an actor dropped meanwhile acts no more */
    OK(users_drop(&us, nobody, "acme/admin"));
    CHECK(users_create(&us, admin, "acme/z", 0, &v, NULL) == TSDB_ERR_DENIED);
    /* offline the operator may drop the last superuser (tsdbd then refuses to start until one is added) */
    OK(users_drop(&us, 0, "nobody"));
    CHECK(users_superusers(&us) == 0);
    users_close(&us);
    cleanup(dir);
    printf("  online checks against the file's state: tenants, shares, the last superuser, a dropped actor\n");
}

/* ------------------------------------------------------- review 1 */

/* REVIEW1 #1: a reader that closes USERS between reloads (tsdbd's tick, a follower) can meet a later compaction's
 * file under the inode number it read, at the same size (a demotion compacts to as many bytes as before it). ext4
 * does reuse the numbers; here the new bytes are written into the very inode the reader read, so the test does not
 * depend on the file system: same inode, same size, another file. */
static void replaced_same_size(void)
{
    char dir[600], other[600], path[700], opath[700];
    fresh("same", dir, sizeof dir);
    fresh("same2", other, sizeof other);
    snprintf(path, sizeof path, "%s/USERS", dir);
    snprintf(opath, sizeof opath, "%s/USERS", other);
    user_verifier v = verifier("pw");
    users w, o, r;
    OK(users_open(&w, dir, true));
    OK(users_open(&o, other, true));
    users *both[] = {&w, &o};
    for (int i = 0; i < 2; i++) {
        OK(users_create(both[i], 0, "root", USER_SUPERUSER, &v, NULL));
        OK(users_create(both[i], 0, "bob", USER_SUPERUSER, &v, NULL));
    }
    OK(users_open(&r, dir, false));
    CHECK(users_find(&r, "bob")->flags == USER_SUPERUSER);
    /* the other file: bob demoted, compacted to the size the reader read */
    OK(users_set_flags(&o, 0, "bob", 0));
    OK(users_compact(&o));
    CHECK(file_size(opath) == (off_t)r.off);
    struct stat a, b;
    CHECK(stat(path, &a) == 0);
    CHECK(copy_file(opath, path, file_size(opath)) == 0); /* in place: O_TRUNC, the same inode */
    CHECK(stat(path, &b) == 0 && a.st_ino == b.st_ino && b.st_size == (off_t)r.off);
    OK(users_reload(&r));
    CHECK(users_find(&r, "bob") && users_find(&r, "bob")->flags == 0);
    CHECK(same(&r, &o));
    /* and a writer that kept its descriptor sees it too */
    OK(users_grant(&w, 0, "bob", SEL_ALL, "", 0, R_READ));
    CHECK(users_find(&w, "bob")->flags == 0);
    users_close(&w), users_close(&o), users_close(&r);
    cleanup(dir), cleanup(other);
    printf("  a reload tells another file of the same inode and size by its file_id (REVIEW1 #1)\n");
}

/* REVIEW1 #2: between a compaction's rename and its directory fsync the new file is locked, so no writer appends
 * (and acknowledges) to a file whose name a power loss can still take back. */
static const char *probe_path;
static int probe_locked = -1;

static void probe_lock(const char *to)
{
    if (!probe_path || strcmp(to, probe_path) != 0)
        return;
    int fd = open(to, O_RDWR | O_CLOEXEC);
    struct flock fl = {.l_type = F_WRLCK, .l_whence = SEEK_SET};
    probe_locked = fd >= 0 && fcntl(fd, F_OFD_SETLK, &fl) != 0 && (errno == EAGAIN || errno == EACCES);
    if (fd >= 0)
        close(fd);
}

static void compaction_window(void)
{
    char dir[600], path[700];
    fresh("window", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    user_verifier v = verifier("pw");
    users w;
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "root", USER_SUPERUSER, &v, NULL));
    OK(users_create(&w, 0, "bob", 0, &v, NULL));
    probe_path = path, probe_locked = -1, after_rename = probe_lock;
    int dirsyncs = f_dirsyncs;
    OK(users_compact(&w));
    after_rename = NULL, probe_path = NULL;
    CHECK(probe_locked == 1);
    CHECK(f_dirsyncs > dirsyncs);
    /* and released after it: the next writer gets the lock */
    users x;
    OK(users_open(&x, dir, true));
    OK(users_grant(&x, 0, "bob", SEL_ALL, "", 0, R_READ));
    users_close(&x), users_close(&w);
    cleanup(dir);
    printf("  a compaction's new file is locked from before its rename to its directory fsync (REVIEW1 #2)\n");
}

/* REVIEW1 #3 (the part failstop() does not cover): an answer that changes nothing rests on the file as read, which
 * may hold records of a writer that died before its fdatasync; and a file whose writer died before the directory
 * fsync has a name a power loss can take back. Both are made durable before anything is acknowledged. */
static void unsynced_state(void)
{
    char dir[600], path[700];
    fresh("unsynced", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    user_verifier v = verifier("pw");
    users w;
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "root", USER_SUPERUSER, &v, NULL));
    OK(users_create(&w, 0, "bob", 0, &v, NULL));
    users_close(&w);
    /* a writer that died between its write and its fdatasync: bob's grant is in the page cache only */
    rbuf b;
    memset(&b, 0, sizeof b);
    b.base = (uint64_t)file_size(path);
    rec_grant(&b, 2, SEL_ALL, R_READ, "", 0);
    int fd = open(path, O_WRONLY | O_APPEND);
    CHECK(fd >= 0 && syscall(SYS_write, fd, b.p, b.n) == (long)b.n);
    close(fd);
    free(b.p);
    /* a fresh writer: its directory fsync comes with its first change, even though the file is not new */
    users x;
    OK(users_open(&x, dir, true));
    int syncs = f_syncs, dirsyncs = f_dirsyncs;
    off_t size = file_size(path);
    OK(users_grant(&x, 0, "bob", SEL_ALL, "", 0, R_READ)); /* held already: nothing written, but synced */
    CHECK(file_size(path) == size && f_syncs == syncs + 1 && f_dirsyncs == dirsyncs + 1);
    OK(users_set_flags(&x, 0, "bob", 0));
    CHECK(file_size(path) == size && f_syncs == syncs + 2 && f_dirsyncs == dirsyncs + 1);
    CHECK(users_revoke(&x, 0, "bob", SEL_NAME, "x", 1, R_READ) == TSDB_ERR_NOTFOUND);
    CHECK(users_drop(&x, 0, "nobody") == TSDB_ERR_NOTFOUND);
    CHECK(file_size(path) == size && f_syncs == syncs + 4 && f_dirsyncs == dirsyncs + 1);
    /* a failed fdatasync there is fail-stop like any other */
    arm(F_FDATASYNC, EIO, false, false);
    CHECK(users_set_flags(&x, 0, "bob", 0) == TSDB_ERR_IO && x.failed);
    users_close(&x);
    cleanup(dir);
    printf("  answers that change nothing are synced first; a new writer fsyncs the directory once (REVIEW1 #3)\n");
}

/* REVIEW1 #4 and #5: revoking a right admin implies takes admin with it; revoking what is not held says so, but only
 * to an actor that could know the user exists. */
static void revoke_rules(void)
{
    char dir[600], path[700];
    fresh("revoke", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    user_verifier v = verifier("pw");
    users us;
    OK(users_open(&us, dir, true));
    uint32_t root, admin;
    OK(users_create(&us, 0, "root", USER_SUPERUSER, &v, &root));
    OK(users_create(&us, root, "acme/admin", 0, &v, &admin));
    OK(users_grant(&us, root, "acme/admin", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_create(&us, root, "acme/bob", 0, &v, NULL));
#define BOB(name) rights_of((user *)users_find(&us, "acme/bob"), name, strlen(name))
    /* read out of admin: admin goes, insert and delete stay */
    OK(users_grant(&us, 0, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_revoke(&us, 0, "acme/bob", SEL_PREFIX, "acme/", 5, R_READ));
    CHECK(BOB("acme/x") == (R_INSERT | R_DELETE));
    /* write (insert and delete) out of admin, by the tenant admin: read stays */
    OK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN));
    CHECK(BOB("acme/x") == R_EVERY);
    OK(users_revoke(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_WRITE));
    CHECK(BOB("acme/x") == R_READ);
    /* delete alone (A23: an ingest credential that must not erase history) */
    OK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_revoke(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_DELETE));
    CHECK(BOB("acme/x") == (R_READ | R_INSERT));
    /* admin itself: the rights it implied go with it, those granted as such stay */
    OK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_revoke(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN));
    CHECK(BOB("acme/x") == (R_READ | R_INSERT));
    OK(users_revoke(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_READ | R_INSERT));
    OK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN));
    OK(users_revoke(&us, admin, "acme/bob", SEL_PREFIX, "acme/", 5, R_ADMIN | R_READ));
    CHECK(BOB("acme/x") == 0 && users_find(&us, "acme/bob")->ng == 0);
    /* a grant not held, or none of the rights named: NOTFOUND, nothing written */
    off_t size = file_size(path);
    CHECK(users_revoke(&us, 0, "acme/bob", SEL_PREFIX, "acme/", 5, R_READ) == TSDB_ERR_NOTFOUND);
    OK(users_grant(&us, admin, "acme/bob", SEL_PREFIX, "acme/s/", 7, R_READ));
    size = file_size(path);
    CHECK(users_revoke(&us, admin, "acme/bob", SEL_PREFIX, "acme/s/", 7, R_INSERT) == TSDB_ERR_NOTFOUND);
    CHECK(users_revoke(&us, admin, "acme/bob", SEL_NAME, "acme/s/t", 8, R_READ) == TSDB_ERR_NOTFOUND);
    CHECK(file_size(path) == size && BOB("acme/s/t") == R_READ);
    /* #5: the acme admin learns nothing of globex's users by revoking its own selector from them */
    OK(users_create(&us, root, "globex/ceo", 0, &v, NULL));
    CHECK(users_revoke(&us, admin, "globex/ceo", SEL_PREFIX, "acme/", 5, R_READ) == TSDB_ERR_DENIED);
    CHECK(users_revoke(&us, admin, "globex/nobody", SEL_PREFIX, "acme/", 5, R_READ) == TSDB_ERR_DENIED);
    /* a share of acme's data is still cut from them */
    OK(users_grant(&us, root, "globex/ceo", SEL_NAME, "acme/s/t", 8, R_READ));
    OK(users_revoke(&us, admin, "globex/ceo", SEL_NAME, "acme/s/t", 8, R_READ));
    CHECK(users_find(&us, "globex/ceo")->ng == 0);
    CHECK(users_revoke(&us, admin, "globex/ceo", SEL_NAME, "acme/s/t", 8, R_READ) == TSDB_ERR_DENIED);
    /* the superuser and the operator see NOTFOUND */
    CHECK(users_revoke(&us, root, "globex/ceo", SEL_NAME, "acme/s/t", 8, R_READ) == TSDB_ERR_NOTFOUND);
#undef BOB
    users_close(&us);
    cleanup(dir);
    printf("  revoke: implied rights take admin with them; not held is NOTFOUND only to who may know (REVIEW1 #4, #5)\n");
}

/* REVIEW1 #6: USERS (and a compaction's file) gets the directory's owner and group and mode 0600; an existing file
 * readable by others is set to 0600; a writer that is neither the directory's owner nor root is refused. */
static mode_t mode_of(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 ? st.st_mode & 07777 : (mode_t)-1;
}

static void permissions(void)
{
    char dir[600], path[700], tmp[720];
    fresh("perms", dir, sizeof dir);
    snprintf(path, sizeof path, "%s/USERS", dir);
    snprintf(tmp, sizeof tmp, "%s.compact", path);
    user_verifier v = verifier("pw");
    /* a group of this user's other than its own: the directory gets it, the files must follow (without root, the
     * group is the part of "the directory's owner" a test can change) */
    gid_t groups[64], g = (gid_t)-1;
    int ng = getgroups(64, groups);
    for (int i = 0; i < ng; i++)
        if (groups[i] != getegid())
            g = groups[i];
    if (g != (gid_t)-1)
        CHECK(chown(dir, (uid_t)-1, g) == 0);
    users w;
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "root", USER_SUPERUSER, &v, NULL));
    struct stat st;
    CHECK(mode_of(path) == 0600);
    if (g != (gid_t)-1)
        CHECK(stat(path, &st) == 0 && st.st_gid == g);
    /* a stale compaction file of mode 0644 (O_TRUNC keeps a mode): the compacted USERS is 0600 */
    int fd = open(tmp, O_WRONLY | O_CREAT, 0644);
    CHECK(fd >= 0 && fchmod(fd, 0644) == 0);
    close(fd);
    OK(users_compact(&w));
    CHECK(mode_of(path) == 0600);
    if (g != (gid_t)-1)
        CHECK(stat(path, &st) == 0 && st.st_gid == g);
    users_close(&w);
    /* an existing USERS readable by others: a reader sets it to 0600, and so does a writer */
    CHECK(chmod(path, 0644) == 0);
    users r;
    OK(users_open(&r, dir, false));
    CHECK(mode_of(path) == 0600);
    users_close(&r);
    CHECK(chmod(path, 0640) == 0);
    OK(users_open(&w, dir, true));
    OK(users_create(&w, 0, "bob", 0, &v, NULL));
    CHECK(mode_of(path) == 0600);
    /* another uid than the directory's owner (and not root): refused, nothing written */
    off_t size = file_size(path);
    fake_euid = (uid_t)syscall(SYS_geteuid) + 1;
    if (fake_euid == 0)
        fake_euid = 2;
    CHECK(users_create(&w, 0, "carol", 0, &v, NULL) == TSDB_ERR_IO);
    users x;
    char empty[600];
    fresh("perms-new", empty, sizeof empty);
    OK(users_open(&x, empty, true));
    CHECK(users_create(&x, 0, "root", USER_SUPERUSER, &v, NULL) == TSDB_ERR_IO);
    char epath[700];
    snprintf(epath, sizeof epath, "%s/USERS", empty);
    CHECK(access(epath, F_OK) != 0); /* no file made */
    fake_euid = (uid_t)-1;
    OK(users_create(&x, 0, "root", USER_SUPERUSER, &v, NULL));
    CHECK(file_size(path) == size);
    OK(users_create(&w, 0, "carol", 0, &v, NULL));
    users_close(&w), users_close(&x);
    cleanup(dir), cleanup(empty);
    printf("  USERS: the directory's owner and group (group %s), mode 0600, other writers refused (REVIEW1 #6)\n",
           g != (gid_t)-1 ? "tested" : "not tested: no second group");
}

/* ------------------------------------------------------- power loss */

static char cli[PATH_MAX], shim[PATH_MAX];

/* Runs the CLI under crashfs, power lost at its step `warm` (or at its exit): 99 for a loss, else its status. */
static int run_crashed(const char *top, int warm, int tear, char *const argv[])
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], seed[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warm);
        snprintf(seed, sizeof seed, "%d", tear);
        setenv("LD_PRELOAD", shim, 1);
        setenv("CRASHFS_ROOT", top, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        if (tear) {
            setenv("CRASHFS_TEAR", "1", 1);
            setenv("CRASHFS_SEED", seed, 1);
        }
        setenv("TSDB_PASSWORD", "secret", 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0)
            dup2(null, 1), dup2(null, 2);
        execv(cli, argv);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int run_cli(char *const argv[])
{
    pid_t pid = fork();
    if (pid == 0) {
        setenv("TSDB_PASSWORD", "secret", 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0)
            dup2(null, 1);
        execv(cli, argv);
        _exit(127);
    }
    int st = -1;
    return pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

/* Power lost at every step of the CLI's add (a new file and directory entry), revoke and compact, and as each
 * exits: USERS opens every time, and once the CLI said it was done, its change is there. */
static void power_loss(void)
{
    if (access(cli, X_OK) != 0 || access(shim, R_OK) != 0) {
        printf("  power loss: skipped (no %s or %s)\n", cli, shim);
        return;
    }
    static const char *verbs[] = {"add", "revoke", "compact"};
    for (int vi = 0; vi < 3; vi++) {
        for (int tear = 0; tear <= 2; tear++) {
            int crashes = 0, finished = 0;
            for (int k = 0; k < 400 && !finished; k++) {
                char top[600], db[700];
                fresh("power", top, sizeof top);
                snprintf(db, sizeof db, "%s/db", top);
                /* the starting state, made without the shim */
                char *mk_root[] = {cli, db, "user", "add", "root", "--superuser", NULL};
                char *mk_bob[] = {cli, db, "user", "add", "acme/bob", NULL};
                char *gr[] = {cli, db, "user", "grant", "acme/bob", "read,insert", "prefix:acme/", NULL};
                if (vi > 0) {
                    CHECK(run_cli(mk_root) == 0 && run_cli(mk_bob) == 0 && run_cli(gr) == 0);
                    char *rv[] = {cli, db, "user", "revoke", "acme/bob", "insert", "prefix:acme/", NULL};
                    for (int i = 0; i < 3; i++) /* something to compact */
                        CHECK(run_cli(rv) == 0 && run_cli(gr) == 0);
                } else {
                    mkdir(db, 0755);
                    int fd = open(top, O_RDONLY | O_DIRECTORY); /* the shim's starting point is durable */
                    if (fd >= 0)
                        syscall(SYS_fsync, fd), close(fd);
                }
                char *add[] = {cli, db, "user", "add", "root", "--superuser", NULL};
                char *rev[] = {cli, db, "user", "revoke", "acme/bob", "read", "prefix:acme/", NULL};
                char *cmp[] = {cli, db, "user", "compact", NULL};
                char *const *argv = vi == 0 ? add : vi == 1 ? rev : cmp;
                int code = run_crashed(top, k, tear ? k * 31 + tear : 0, argv);
                CHECK(code == 99 || code == 0);
                if (code != 99 && code != 0)
                    break;
                crashes += code == 99;
                finished = code == 0;
                users us;
                int rc = users_open(&us, db, true);
                if (rc)
                    fprintf(stderr, "  %s, power lost at step %d (tear %d): USERS does not open: %s\n", verbs[vi],
                            k + 1, tear, tsdb_strerror(rc));
                CHECK(rc == 0);
                if (rc == 0 && finished) {
                    const user *root = users_find(&us, "root"), *bob = users_find(&us, "acme/bob");
                    if (vi == 0)
                        CHECK(root && root->has_verifier && (root->flags & USER_SUPERUSER)); /* it said so */
                    if (vi == 1)
                        CHECK(bob && bob->ng == 1 && bob->g[0].rights == R_INSERT); /* the revoke survives */
                    if (vi == 2)
                        CHECK(bob && bob->ng == 1 && bob->g[0].rights == (R_READ | R_INSERT) && root);
                }
                if (rc == 0 && !finished && vi == 1) {
                    const user *bob = users_find(&us, "acme/bob");
                    CHECK(bob && bob->ng == 1); /* before or after the revoke, nothing else */
                }
                if (rc == 0 && !finished && vi == 2)
                    CHECK(users_find(&us, "acme/bob") && us.n == 2); /* the old file or the new one */
                /* the next writer works on what the loss left */
                if (rc == 0) {
                    user_verifier v = verifier("x");
                    OK(users_create(&us, 0, "after", 0, &v, NULL));
                }
                users_close(&us);
                cleanup(top);
            }
            CHECK(finished && crashes > 2);
            printf("  power lost at each of %d steps of user %s%s, and at its exit: USERS opens, the change holds\n",
                   crashes, verbs[vi], tear ? " (torn writes)" : "");
        }
    }
}

int main(int argc, char **argv)
{
    snprintf(base_dir, sizeof base_dir, "%s", argc > 1 ? argv[1] : "/tmp");
    char self[PATH_MAX];
    ssize_t n = readlink("/proc/self/exe", self, sizeof self - 1);
    self[n > 0 ? n : 0] = '\0';
    char *slash = strrchr(self, '/');
    if (slash)
        *slash = '\0';
    snprintf(cli, sizeof cli, "%s/tsdb", self);
    snprintf(shim, sizeof shim, "%s/libcrashfs.so", self);
    printf("test_users\n");
    names();
    normalize();
    globs();
    globs_linear();
    inside();
    escalation();
    matching();
    name_table();
    round_trip();
    torn_tail();
    damage();
    nonsense();
    reload();
    compaction();
    compaction_under_reader();
    two_writers();
    failstop();
    online();
    replaced_same_size();
    compaction_window();
    unsynced_state();
    revoke_rules();
    permissions();
    power_loss();
    if (failures) {
        fprintf(stderr, "test_users: %d failures\n", failures);
        return 1;
    }
    printf("test_users: ok\n");
    return 0;
}
