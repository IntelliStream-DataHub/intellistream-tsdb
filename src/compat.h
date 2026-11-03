/* compat.h: what Linux has and macOS (the developer instance, arm64 or x86-64) lacks.
 *
 * Everything here is under __APPLE__; on Linux this header adds nothing, so the Linux build is the code
 * as it was. Each shim keeps the behaviour the engine relies on, or says what is weaker and why that is
 * acceptable for a developer instance:
 *
 *  - getrandom           arc4random_buf (never blocks, never short)
 *  - sync_file_range     nothing: it only started writeback early; the caller's fdatasync still makes it durable
 *  - memrchr             a loop
 *  - st_ctim, st_mtim    the BSD names
 *  - gettid              pthread_threadid_np
 *  - rwlock timed wait   trywrlock and a 1 ms sleep until the deadline (no writer preference: a class switch
 *                        that waits gives up after CLS_SWITCH_WAIT_MS as before and tries later)
 *  - MSG_NOSIGNAL        0, with SO_NOSIGPIPE on the socket (tsdb_nosigpipe)
 *  - thread stacks       8 MB: macOS gives a secondary thread 512 KB, Linux 8 MB
 *  - fsync, fdatasync    tsdb_fsync and tsdb_fdatasync: macOS's fsync only hands the data to the drive, whose cache a
 *                        power loss empties, so they follow the real call with fcntl(F_FULLFSYNC) (the drive's own
 *                        cache flush; slower, tens of ms for a call on some SSDs). TSDB_FAST_FSYNC=1 in the
 *                        environment gives that up (for tests, and for data that can be thrown away). On Linux they
 *                        are fsync and fdatasync.
 */
#ifndef TSDB_COMPAT_H
#define TSDB_COMPAT_H

#ifdef __APPLE__

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Durable sync: the real call first (the one tests interpose, and what Linux does), then the drive's cache flush.
 * A file system without F_FULLFSYNC (it answers ENOTSUP, EINVAL or ENOTTY) has only the first. Any other error,
 * EIO above all, is the caller's to treat as a failed sync. */
static inline int tsdb_fast_fsync(void)
{
    static int mode = -1;
    int m = __atomic_load_n(&mode, __ATOMIC_RELAXED);
    if (m < 0) {
        const char *e = getenv("TSDB_FAST_FSYNC");
        m = e && *e && *e != '0';
        __atomic_store_n(&mode, m, __ATOMIC_RELAXED);
    }
    return m;
}

static inline int tsdb_full_flush(int fd)
{
    if (tsdb_fast_fsync() || fcntl(fd, F_FULLFSYNC) == 0)
        return 0;
    return errno == ENOTSUP || errno == EINVAL || errno == ENOTTY ? 0 : -1;
}

/* in libSystem but in no SDK header (clang lets the call through, the tests take its address) */
int fdatasync(int fd);

static inline int tsdb_fsync(int fd) { return fsync(fd) != 0 ? -1 : tsdb_full_flush(fd); }
static inline int tsdb_fdatasync(int fd) { return fdatasync(fd) != 0 ? -1 : tsdb_full_flush(fd); }

#define GRND_NONBLOCK 1
static inline ssize_t getrandom(void *buf, size_t n, unsigned flags)
{
    (void)flags;
    arc4random_buf(buf, n);
    return (ssize_t)n;
}

#define SYNC_FILE_RANGE_WRITE 0
static inline int sync_file_range(int fd, off_t off, off_t n, unsigned flags)
{
    (void)fd, (void)off, (void)n, (void)flags;
    return 0;
}

static inline void *memrchr(const void *s, int c, size_t n)
{
    const unsigned char *p = (const unsigned char *)s + n;
    while (n--)
        if (*--p == (unsigned char)c)
            return (void *)p;
    return NULL;
}

#define st_ctim st_ctimespec
#define st_mtim st_mtimespec

static inline long tsdb_gettid(void)
{
    uint64_t t = 0;
    pthread_threadid_np(NULL, &t);
    return (long)t;
}

static inline int pthread_rwlock_timedwrlock(pthread_rwlock_t *l, const struct timespec *deadline)
{
    for (;;) {
        int rc = pthread_rwlock_trywrlock(l);
        if (rc != EBUSY)
            return rc;
        struct timespec now, nap = {0, 1000000};
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > deadline->tv_sec || (now.tv_sec == deadline->tv_sec && now.tv_nsec >= deadline->tv_nsec))
            return ETIMEDOUT;
        nanosleep(&nap, NULL);
    }
}

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
/* a write to a closed peer must be an EPIPE, not a SIGPIPE: set it once on every socket we send on */
static inline void tsdb_nosigpipe(int fd)
{
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
}

/* accept4 and socket(..., SOCK_CLOEXEC) with fcntl, on a socket that does not raise SIGPIPE; the flag values are
 * ours (macOS has no SOCK_*): never pass them to the real socket() */
#define SOCK_CLOEXEC 0x01000000
#define SOCK_NONBLOCK 0x02000000
static inline int tsdb_sock_flags(int fd, int flags)
{
    if (fd < 0)
        return fd;
    if (flags & SOCK_CLOEXEC)
        fcntl(fd, F_SETFD, FD_CLOEXEC);
    int fl = fcntl(fd, F_GETFL); /* an accepted socket inherits the listener's O_NONBLOCK here */
    fcntl(fd, F_SETFL, (flags & SOCK_NONBLOCK) ? fl | O_NONBLOCK : fl & ~O_NONBLOCK);
    tsdb_nosigpipe(fd);
    return fd;
}
static inline int accept4(int fd, struct sockaddr *a, socklen_t *l, int flags)
{
    return tsdb_sock_flags(accept(fd, a, l), flags);
}
static inline int tsdb_socket_cloexec(int domain, int type, int proto)
{
    return tsdb_sock_flags(socket(domain, type, proto), SOCK_CLOEXEC);
}

#define TSDB_THREAD_STACK ((size_t)8 << 20)
/* pthread_create with an 8 MB stack */
static inline int tsdb_thread_create(pthread_t *t, void *(*fn)(void *), void *arg)
{
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setstacksize(&at, TSDB_THREAD_STACK);
    int rc = pthread_create(t, &at, fn, arg);
    pthread_attr_destroy(&at);
    return rc;
}

#else /* Linux and the rest: the defaults are right */

#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <sys/socket.h>

static inline long tsdb_gettid(void) { return (long)syscall(SYS_gettid); }
static inline int tsdb_fsync(int fd) { return fsync(fd); }
static inline int tsdb_fdatasync(int fd) { return fdatasync(fd); }
static inline void tsdb_nosigpipe(int fd) { (void)fd; }
static inline int tsdb_socket_cloexec(int domain, int type, int proto)
{
    return socket(domain, type | SOCK_CLOEXEC, proto);
}
static inline int tsdb_thread_create(pthread_t *t, void *(*fn)(void *), void *arg)
{
    return pthread_create(t, NULL, fn, arg);
}

#endif /* __APPLE__ */

#endif
