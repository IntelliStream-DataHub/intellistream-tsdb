/* epoll_compat.h: the part of epoll that tsdbd uses, over kqueue, for macOS (a developer instance; the loop is
 * the same as on Linux, only how it asks the kernel differs).
 *
 *  - EPOLL_CTL_ADD only (tsdbd never modifies or deletes: closing the descriptor removes its filters).
 *  - EPOLLET is EV_CLEAR: an edge per arrival of data or of room, like epoll's. Without it the filter is level
 *    triggered (the wake pipe and the tick).
 *  - kqueue has a read and a write filter per descriptor: one epoll_event comes out per filter that fired, so a
 *    descriptor may appear twice in a batch, which the loop copes with (it queues a connection once).
 *  - EV_EOF (the peer shut down or reset) is reported as EPOLLRDHUP | EPOLLHUP, a socket error as EPOLLERR.
 */
#ifndef TSDBD_EPOLL_COMPAT_H
#define TSDBD_EPOLL_COMPAT_H

#ifdef __APPLE__

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <sys/event.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define EPOLLIN 0x001u
#define EPOLLOUT 0x004u
#define EPOLLERR 0x008u
#define EPOLLHUP 0x010u
#define EPOLLRDHUP 0x2000u
#define EPOLLET (1u << 31)
#define EPOLL_CLOEXEC 1
#define EPOLL_CTL_ADD 1

typedef union epoll_data {
    void *ptr;
    int fd;
    uint32_t u32;
    uint64_t u64;
} epoll_data_t;

struct epoll_event {
    uint32_t events;
    epoll_data_t data;
};

static inline int epoll_create1(int flags)
{
    int kq = kqueue();
    if (kq >= 0 && (flags & EPOLL_CLOEXEC))
        fcntl(kq, F_SETFD, FD_CLOEXEC);
    return kq;
}

static inline int epoll_ctl(int kq, int op, int fd, struct epoll_event *e)
{
    if (op != EPOLL_CTL_ADD) {
        errno = EINVAL;
        return -1;
    }
    unsigned short fl = EV_ADD | EV_ENABLE | ((e->events & EPOLLET) ? EV_CLEAR : 0);
    struct kevent ch[2];
    int n = 0;
    if (e->events & EPOLLIN)
        EV_SET(&ch[n++], (uintptr_t)fd, EVFILT_READ, fl, 0, 0, NULL);
    if (e->events & EPOLLOUT)
        EV_SET(&ch[n++], (uintptr_t)fd, EVFILT_WRITE, fl, 0, 0, NULL);
    return kevent(kq, ch, n, NULL, 0, NULL) < 0 ? -1 : 0; /* no room for results: an error is the return value */
}

static inline int epoll_wait(int kq, struct epoll_event *out, int max, int timeout_ms)
{
    struct kevent ev[256];
    if (max > 256)
        max = 256;
    struct timespec ts = {timeout_ms / 1000, (long)(timeout_ms % 1000) * 1000000L};
    int n = kevent(kq, NULL, 0, ev, max, timeout_ms < 0 ? NULL : &ts);
    for (int i = 0; i < n; i++) {
        uint32_t m = 0;
        if (ev[i].filter == EVFILT_READ)
            m |= EPOLLIN;
        if (ev[i].filter == EVFILT_WRITE)
            m |= EPOLLOUT;
        if (ev[i].flags & EV_EOF)
            m |= EPOLLRDHUP | EPOLLHUP;
        if (ev[i].flags & EV_ERROR)
            m |= EPOLLERR;
        out[i].events = m;
        out[i].data.u64 = 0;
        out[i].data.fd = (int)ev[i].ident;
    }
    return n;
}

#endif /* __APPLE__ */

#endif
