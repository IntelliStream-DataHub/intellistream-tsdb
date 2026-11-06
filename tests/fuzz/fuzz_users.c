/* USERS (server/users.c): users_open on the first `split` bytes, then the rest appended and users_reload (the
 * incremental catch_up), then lookups, rights_of on a few names, and a writable open that appends a grant (the
 * writer's catch_up with its cut). xxh64 is no authentication: every record is re-sealed unless bit 6 of its type
 * byte is set (cleared, record left as it is). Linked with users.o and tsdb_crypto.o (the Makefile). */
#include "fuzz.h"
#include "users.h"
#include "tsdb.h"

uint64_t xxh64(const void *data, size_t len, uint64_t seed);
static const char *D;

static size_t rsize(size_t len) { return ((5 + len + 7) & ~(size_t)7) + 8; }

static void reseal(uint8_t *b, size_t size)
{
    for (size_t q = 0; q + 16 <= size;) {
        uint32_t len = (uint32_t)b[q] | (uint32_t)b[q + 1] << 8 | (uint32_t)b[q + 2] << 16 | (uint32_t)b[q + 3] << 24;
        if (len > 70000 || rsize(len) > size - q)
            return;
        bool keep = b[q + 4] & 0x40;
        b[q + 4] &= ~0x40;
        size_t rs = rsize(len);
        if (!keep) {
            uint64_t c = xxh64(b + q, rs - 8, q);
            memcpy(b + q + rs - 8, &c, 8);
        }
        q += rs;
    }
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("users");
    atexit(fuzz_rm_at_exit);
    return 0;
}

static void probe(users *us)
{
    static const char *names[] = {"a", "b", "admin", "x/y", "sensor.1", "z"};
    for (size_t i = 0; i < 6; i++) {
        const user *u = users_find(us, names[i]);
        if (!u)
            continue;
        user *c = user_copy(u);
        if (c) {
            for (size_t k = 0; k < 6; k++)
                (void)rights_of(c, names[k], strlen(names[k]));
            (void)rights_of_slow(c, "sensor.1", 8);
            user_release(c);
        }
    }
    for (uint32_t uid = 0; uid < 8; uid++)
        (void)users_by_uid(us, uid);
    (void)users_superusers(us);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    if (size < 3)
        return 0;
    size_t split = (size_t)(data[0] | data[1] << 8);
    data += 2, size -= 2;
    uint8_t *b = malloc(size);
    memcpy(b, data, size);
    reseal(b, size);
    split = split % (size + 1);
    split &= ~(size_t)7;
    char p[8300];
    snprintf(p, sizeof p, "%s/USERS", D);
    unlink(p);
    fuzz_put(D, "USERS", b, split);
    chmod(p, 0600);
    users us;
    if (users_open(&us, D, false) == 0) {
        probe(&us);
        int fd = open(p, O_WRONLY | O_APPEND);
        if (fd >= 0) {
            if (write(fd, b + split, size - split) < 0) {}
            close(fd);
        }
        if (users_reload(&us) == 0)
            probe(&us);
        users_close(&us);
    } else {
        fuzz_put(D, "USERS", b, size);
        chmod(p, 0600);
    }
    if (users_open(&us, D, true) == 0) {
        (void)users_grant(&us, 0, "a", SEL_PREFIX, "s", 1, R_READ);
        probe(&us);
        users_close(&us);
    }
    free(b);
    return 0;
}

void fuzz_seeds(const char *dir)
{
    const char *t = fuzz_dir("users-seed");
    users us;
    if (users_open(&us, t, true) != 0)
        return;
    user_verifier v = {.iterations = 4096, .salt_len = 16};
    uint32_t uid;
    users_create(&us, 0, "a", 0, &v, &uid);
    users_create(&us, 0, "admin", USER_SUPERUSER, &v, &uid);
    users_create(&us, 0, "b", 0, &v, &uid);
    users_grant(&us, 0, "a", SEL_NAME, "sensor.1", 8, R_READ);
    users_grant(&us, 0, "a", SEL_GLOB, "x/*y?", 5, R_EVERY);
    users_grant(&us, 0, "b", SEL_ALL, "", 0, R_ADMIN);
    users_set_flags(&us, 0, "b", USER_SUPERUSER);
    users_revoke(&us, 0, "a", SEL_NAME, "sensor.1", 8, R_READ);
    users_drop(&us, 0, "b");
    users_close(&us);
    size_t n;
    uint8_t *f = fuzz_get(t, "USERS", &n);
    if (f) {
        uint8_t *s = malloc(n + 2);
        s[0] = (uint8_t)(n / 2), s[1] = (uint8_t)(n / 2 >> 8);
        memcpy(s + 2, f, n);
        fuzz_put(dir, "seed-users", s, n + 2);
        s[0] = 0, s[1] = 0;
        fuzz_put(dir, "seed-users-0", s, n + 2);
        free(s);
        free(f);
    }
}
