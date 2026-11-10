/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The helpers of compat.h: tsdb's versions of what the copied Raft core takes from isod's src/common. See compat.h.
 */
#include "compat.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/random.h>
#include <time.h>
#include <unistd.h>

bool iso_log_quiet;

const char *iso_strerr(int err)
{
    switch (err) {
    case ISO_OK: return "ISO_OK";
    case ISO_ERR_NOMEM: return "ISO_ERR_NOMEM";
    case ISO_ERR_INVAL: return "ISO_ERR_INVAL";
    case ISO_ERR_IO: return "ISO_ERR_IO";
    case ISO_ERR_NOTFOUND: return "ISO_ERR_NOTFOUND";
    case ISO_ERR_EXISTS: return "ISO_ERR_EXISTS";
    case ISO_ERR_CORRUPT: return "ISO_ERR_CORRUPT";
    case ISO_ERR_FULL: return "ISO_ERR_FULL";
    case ISO_ERR_AGAIN: return "ISO_ERR_AGAIN";
    case ISO_ERR_TIMEOUT: return "ISO_ERR_TIMEOUT";
    case ISO_ERR_CLOSED: return "ISO_ERR_CLOSED";
    case ISO_ERR_PERM: return "ISO_ERR_PERM";
    case ISO_ERR_RANGE: return "ISO_ERR_RANGE";
    case ISO_ERR_UNSUPPORTED: return "ISO_ERR_UNSUPPORTED";
    case ISO_ERR_TOOBIG: return "ISO_ERR_TOOBIG";
    case ISO_ERR_BUSY: return "ISO_ERR_BUSY";
    case ISO_ERR_INTERNAL: return "ISO_ERR_INTERNAL";
    case ISO_ERR_PROTO: return "ISO_ERR_PROTO";
    case ISO_ERR_NOTLEADER: return "ISO_ERR_NOTLEADER";
    }
    return "ISO_ERR_?";
}

iso_err iso_errf(char *err, size_t errsize, iso_err rc, const char *fmt, ...)
{
    if (err && errsize) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(err, errsize, fmt, ap);
        va_end(ap);
    }
    return rc;
}

iso_err iso_err_from_errno(int e)
{
    switch (e) {
    case 0: return ISO_OK;
    case ENOMEM: return ISO_ERR_NOMEM;
    case EINVAL: return ISO_ERR_INVAL;
    case ENOENT: return ISO_ERR_NOTFOUND;
    case EEXIST: return ISO_ERR_EXISTS;
    case ENOSPC: case EDQUOT: return ISO_ERR_FULL;
    case EAGAIN: return ISO_ERR_AGAIN;
    case ETIMEDOUT: return ISO_ERR_TIMEOUT;
    case EPERM: case EACCES: return ISO_ERR_PERM;
    case ERANGE: return ISO_ERR_RANGE;
    case EFBIG: case E2BIG: return ISO_ERR_TOOBIG;
    case EBUSY: return ISO_ERR_BUSY;
    default: return ISO_ERR_IO;
    }
}

static void *oom(size_t n)
{
    fprintf(stderr, "raft: out of memory (%zu bytes)\n", n);
    abort();
}

void *iso_xmalloc(size_t n)
{
    void *p = malloc(n ? n : 1);
    return p ? p : oom(n);
}

void *iso_xcalloc(size_t n, size_t size)
{
    void *p = calloc(n ? n : 1, size ? size : 1);
    return p ? p : oom(n * size);
}

void *iso_xrealloc(void *p, size_t n)
{
    void *q = realloc(p, n ? n : 1);
    return q ? q : oom(n);
}

char *iso_xstrdup(const char *s)
{
    return iso_xmemdup(s, strlen(s) + 1);
}

void *iso_xmemdup(const void *p, size_t n)
{
    void *q = iso_xmalloc(n);
    if (n)
        memcpy(q, p, n);
    return q;
}

size_t iso_strlcpy(char *dst, const char *src, size_t dstsize)
{
    size_t n = strlen(src);
    if (dstsize) {
        size_t c = n < dstsize - 1 ? n : dstsize - 1;
        memcpy(dst, src, c);
        dst[c] = 0;
    }
    return n;
}

void iso_random_bytes(void *buf, size_t n)
{
    uint8_t *p = buf;
    while (n) {
        ssize_t r = getrandom(p, n, 0);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "raft: getrandom: %s\n", strerror(errno));
            abort();
        }
        p += r;
        n -= (size_t)r;
    }
}

int iso_fd_reopen(int fd, int flags)
{
    char path[64];
    snprintf(path, sizeof path, "/proc/self/fd/%d", fd);
    return open(path, flags | O_CLOEXEC);
}

const char *iso_fd_reopen_remedy(int e)
{
    if (e == EMFILE || e == ENFILE)
        return " Raise the descriptor limit (ulimit -n) and restart.";
    if (e == ENOENT)
        return " /proc must be mounted.";
    return "";
}

int iso_fsync_parent(const char *path)
{
    char dir[4096];
    const char *slash = strrchr(path, '/');
    if (!slash)
        snprintf(dir, sizeof dir, ".");
    else if (slash == path)
        snprintf(dir, sizeof dir, "/");
    else
        snprintf(dir, sizeof dir, "%.*s", (int)(slash - path), path);
    int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return errno;
    int e = fsync(fd) ? errno : 0;
    close(fd);
    return e == EINVAL ? 0 : e;
}

#ifdef ISO_DEBUG
ssize_t (*iso_pwrite_hook)(int fd, const void *buf, size_t len, uint64_t off);
#endif

uint64_t iso_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t splitmix64(uint64_t *x)
{
    uint64_t z = (*x += 0x9e3779b97f4a7c15ull);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
}

void iso_rng_seed(iso_rng *r, uint64_t seed)
{
    for (int i = 0; i < 4; i++)
        r->s[i] = splitmix64(&seed);
}

static inline uint64_t rotl(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

uint64_t iso_rng_u64(iso_rng *r)
{
    uint64_t *s = r->s;
    uint64_t out = rotl(s[1] * 5, 7) * 9, t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotl(s[3], 45);
    return out;
}

uint64_t iso_rng_below(iso_rng *r, uint64_t n)
{
    /* Lemire's multiply-shift with rejection: no modulo bias */
    unsigned __int128 m = (unsigned __int128)iso_rng_u64(r) * n;
    uint64_t lo = (uint64_t)m;
    if (lo < n) {
        uint64_t t = -n % n;
        while (lo < t) {
            m = (unsigned __int128)iso_rng_u64(r) * n;
            lo = (uint64_t)m;
        }
    }
    return (uint64_t)(m >> 64);
}

double iso_rng_double(iso_rng *r)
{
    return (double)(iso_rng_u64(r) >> 11) * 0x1.0p-53;
}

void iso_rng_shuffle(iso_rng *r, void *base, size_t n, size_t size)
{
    uint8_t *b = base, tmp[256];
    if (size > sizeof tmp)
        abort();
    for (size_t i = n; i > 1; i--) {
        size_t j = (size_t)iso_rng_below(r, i);
        if (j != i - 1) {
            memcpy(tmp, b + j * size, size);
            memcpy(b + j * size, b + (i - 1) * size, size);
            memcpy(b + (i - 1) * size, tmp, size);
        }
    }
}

/* CRC-32C, reflected polynomial 0x82F63B78, a byte-wise table built once. The metadata log is kilobytes a minute. */
static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = c & 1 ? (c >> 1) ^ 0x82F63B78u : c >> 1;
        crc_table[i] = c;
    }
}

uint32_t iso_crc32c(uint32_t crc, const void *buf, size_t len)
{
    pthread_once(&crc_once, crc_init);
    const uint8_t *p = buf;
    crc = ~crc;
    while (len--)
        crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    return ~crc;
}

void iso_log_write(iso_log_level level, const char *fmt, ...)
{
    if (iso_log_quiet && level != ISO_LOG_ERR)
        return;
    char line[2048];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof line, fmt, ap);
    va_end(ap);
    fprintf(stderr, "raft: %s%s\n", level == ISO_LOG_ERR ? "error: " : level == ISO_LOG_WARN ? "warning: " : "",
            line);
}
