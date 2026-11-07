/*
 * What the Raft core copied from intellistream-objects (src/raft/UPSTREAM) includes from isod's own src/common and
 * src/proto, written for tsdb. The copied files include "common/util.h", "common/rng.h", "common/clock.h",
 * "common/crc32c.h", "common/log.h" and "proto/frame.h"; each of those is a one-line header under compat/ that
 * points here, so the copies stay byte for byte as upstream has them (tools/sync-raft.sh) and are built with
 * -Isrc -Isrc/raft/compat.
 *
 * Only what the four files use is here, with isod's meaning: the error codes and their numbers, little-endian
 * access, the x-allocators that abort, xoshiro256** for the simulator and the election timers, CRC-32C as a
 * running checksum (pass 0 first; the value is the conventional pre- and post-inverted one), and log lines. The log
 * lines go to stderr with a "raft:" prefix; `iso_log_quiet` silences the INFO and WARN ones (the simulator and the
 * unit tests open logs by the hundred). Nothing here is part of libtsdb: the cluster is a tsdbd mode
 * (docs/design-cluster.md C24).
 */
#ifndef TSDB_RAFT_COMPAT_H
#define TSDB_RAFT_COMPAT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#define ISO_LIKELY(x)   __builtin_expect(!!(x), 1)
#define ISO_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define ISO_UNUSED      __attribute__((unused))
#define ISO_NORETURN    __attribute__((noreturn))
#define ISO_NOINLINE    __attribute__((noinline))
#define ISO_MALLOC_LIKE __attribute__((malloc))
#define ISO_PRINTF(f, a) __attribute__((format(printf, f, a)))

#define ISO_ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))
#define ISO_MIN(a, b) ((a) < (b) ? (a) : (b))
#define ISO_MAX(a, b) ((a) > (b) ? (a) : (b))
#define ISO_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)

typedef enum {
    ISO_OK = 0,
    ISO_ERR_NOMEM = -1,
    ISO_ERR_INVAL = -2,
    ISO_ERR_IO = -3,
    ISO_ERR_NOTFOUND = -4,
    ISO_ERR_EXISTS = -5,
    ISO_ERR_CORRUPT = -6,
    ISO_ERR_FULL = -7,
    ISO_ERR_AGAIN = -8,
    ISO_ERR_TIMEOUT = -9,
    ISO_ERR_CLOSED = -10,
    ISO_ERR_PERM = -11,
    ISO_ERR_RANGE = -12,
    ISO_ERR_UNSUPPORTED = -13,
    ISO_ERR_TOOBIG = -14,
    ISO_ERR_BUSY = -15,
    ISO_ERR_INTERNAL = -16,
    ISO_ERR_PROTO = -17,
    ISO_ERR_NOTLEADER = -18,
} iso_err;

const char *iso_strerr(int err);
iso_err iso_errf(char *err, size_t errsize, iso_err rc, const char *fmt, ...) ISO_PRINTF(4, 5);
iso_err iso_err_from_errno(int e);

void *iso_xmalloc(size_t n) ISO_MALLOC_LIKE;
void *iso_xcalloc(size_t n, size_t size) ISO_MALLOC_LIKE;
void *iso_xrealloc(void *p, size_t n);
char *iso_xstrdup(const char *s) ISO_MALLOC_LIKE;
void *iso_xmemdup(const void *p, size_t n) ISO_MALLOC_LIKE;
size_t iso_strlcpy(char *dst, const char *src, size_t dstsize);

static inline uint16_t iso_get_u16le(const void *p) { uint16_t v; memcpy(&v, p, sizeof v); return v; }
static inline uint32_t iso_get_u32le(const void *p) { uint32_t v; memcpy(&v, p, sizeof v); return v; }
static inline uint64_t iso_get_u64le(const void *p) { uint64_t v; memcpy(&v, p, sizeof v); return v; }
static inline void iso_put_u16le(void *p, uint16_t v) { memcpy(p, &v, sizeof v); }
static inline void iso_put_u32le(void *p, uint32_t v) { memcpy(p, &v, sizeof v); }
static inline void iso_put_u64le(void *p, uint64_t v) { memcpy(p, &v, sizeof v); }

void iso_random_bytes(void *buf, size_t n);

/* A new open file description of the file `fd` names (through /proc/self/fd), not a dup: the raft log's hard state
 * and segments each hear their own writeback errors. -1 with errno. */
int iso_fd_reopen(int fd, int flags);
const char *iso_fd_reopen_remedy(int e);
/* fsync of the directory that holds `path`: 0 or the errno (EINVAL, no directory sync, is 0). */
int iso_fsync_parent(const char *path);
#define ISO_FSYNC_REMEDY                                                                                    \
    "The filesystem would not make the name durable, so a power cut could take it back: check the disk (the " \
    "kernel log, `smartctl`) and the filesystem, then try again"

#ifdef ISO_DEBUG
extern ssize_t (*iso_pwrite_hook)(int fd, const void *buf, size_t len, uint64_t off);
#endif

/* CLOCK_MONOTONIC in nanoseconds. */
uint64_t iso_now_ns(void);

/* xoshiro256**, seeded through SplitMix64 */
typedef struct {
    uint64_t s[4];
} iso_rng;
void iso_rng_seed(iso_rng *r, uint64_t seed);
uint64_t iso_rng_u64(iso_rng *r);
uint64_t iso_rng_below(iso_rng *r, uint64_t n);
double iso_rng_double(iso_rng *r);
static inline bool iso_rng_chance(iso_rng *r, double p) { return iso_rng_double(r) < p; }
void iso_rng_shuffle(iso_rng *r, void *base, size_t n, size_t size);

uint32_t iso_crc32c(uint32_t crc, const void *buf, size_t len);
static inline uint32_t iso_crc32c_buf(const void *buf, size_t len) { return iso_crc32c(0, buf, len); }

typedef enum { ISO_LOG_ERR, ISO_LOG_WARN, ISO_LOG_INFO } iso_log_level;
extern bool iso_log_quiet;
void iso_log_write(iso_log_level level, const char *fmt, ...) ISO_PRINTF(2, 3);
#define LOG_ERR(...)  iso_log_write(ISO_LOG_ERR, __VA_ARGS__)
#define LOG_WARN(...) iso_log_write(ISO_LOG_WARN, __VA_ARGS__)
#define LOG_INFO(...) iso_log_write(ISO_LOG_INFO, __VA_ARGS__)

/* The largest frame payload of a transport (isod's proto/frame.h): what bounds an entry, and with it the log's
 * segment size. tsdb's metadata entries are kilobytes; 4 MiB as upstream. */
#define ISO_FRAME_MAX_PAYLOAD (4u * 1024 * 1024)

#endif
