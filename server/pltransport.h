/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The transport under one link connection of peerlink (docs/design-rdma.md §5.1, track rd-seam): the bytes of the
 * link's frames go through a `struct pl_transport` and nothing else, so another transport (rd-verbs: SEND/RECV on an
 * RC queue pair) can carry them without a change to the link's state machine, its frames, timeouts, sequence numbers,
 * frame check, token buckets, keepalive or the WRITTEN/NEVER_WRITTEN accounting of plink_sendv.
 *
 * What a transport is. A reliable, ordered byte stream in both directions, non-blocking, run by the one link thread
 * that owns the connection (no op is called from another thread: other threads wake the link thread through its event
 * fd, never touch its transport). The link thread writes the encoded
 * frames from its write buffer and feeds what it reads to its frame reader, exactly as on a socket: a transport that
 * splits a frame over several of its own messages (slots) must deliver the bytes in order and whole, and an end or an
 * error of the transport ends the connection (the driver resumes from its own state, as after a TCP failure).
 *
 * Connect and accept are deliberately outside the interface, and so is the handshake (HELLO, HELLO_ACK, AUTH): they
 * always run on the TCP connection (peerlink's dialer and acceptor, with plain sockets), which
 * is the anchor of every link (design-rdma.md §5.1 step 1, §5.3). A link thread starts its connection on the TCP
 * transport made from that socket; an upgrade (rd-upgrade) replaces it with a transport that takes the TCP one over
 * as its anchor (its fd stays in the poll set, so that the end of the TCP connection is the end of the connection,
 * §5.1 step 8) and closes it with itself.
 *
 * Writes. `writev` takes what it can at once and says how much: a short count or EAGAIN means "no room now" (a TCP
 * send buffer that is full, a verbs transport with no send credit or slot) and the link thread polls for PL_TR_OUT.
 * Bytes a writev took count as WRITTEN for plink_sendv's on_sent (D5: taken, never "acknowledged by the peer"): a
 * transport never takes bytes it may later drop silently while the connection lives. The link thread treats a write
 * that is held back (its buffer not drained) as a slow peer, not a silent one: a transport whose peer stops returning
 * credits looks the same as a TCP window that stays shut, and is found the same way.
 *
 * Polling. Each turn the link thread asks the transport for the descriptors to poll (`poll_fds`: what it wants, read
 * or write, and the transport's own needs, such as a completion channel that must be polled while the link is
 * paused, so that send completions free slots) and for what it can already do without waiting (`*ready`: bytes
 * already received, room already granted); with something ready it does not sleep. After the poll, `poll_ready`
 * turns the revents of those descriptors into PL_TR_IN / PL_TR_OUT (consuming whatever events they announce: a
 * completion channel is drained and re-armed there). A hangup or an error is reported as PL_TR_IN, also when the
 * link did not ask to read (a paused link): the next `read` says what it was (0 for the end, -1 with errno).
 * `*ready` is the only news of bytes a transport took in before the poll (a completion queue drained in poll_fds):
 * no descriptor announces them again, so a transport that has them must say so there (tests/pl_fake.h's `edge` mode
 * is such a transport; test_peerlink `fakeready`). The link thread writes whenever its buffer holds bytes, so
 * PL_TR_OUT from poll_ready only ends the poll; poll_fds returns at most PL_TR_POLL_MAX descriptors (more ends the
 * connection with a line).
 *
 * No `shutdown` op yet: nothing ends a connection from another thread. rd-upgrade adds what it needs with its caller.
 */
#ifndef TSDB_PLTRANSPORT_H
#define TSDB_PLTRANSPORT_H

#include <poll.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/uio.h>

#define PL_TR_POLL_MAX 4 /* descriptors one transport may ask the link thread to poll */
enum { PL_TR_IN = 1, PL_TR_OUT = 2 };

struct pl_transport;

struct pl_tr_ops {
    const char *name; /* "tcp", ... (logs) */
    /* up to n bytes in: > 0 their count, 0 the peer ended the stream, -1 with errno (EAGAIN, EINTR: nothing now;
     * anything else ends the connection) */
    ssize_t (*read)(struct pl_transport *t, void *p, size_t n);
    /* the bytes of iov[0..n) out, in order: the count taken (may be short), -1 with errno (EAGAIN, EINTR: no room
     * now; anything else ends the connection) */
    ssize_t (*writev)(struct pl_transport *t, const struct iovec *iov, int n);
    /* fill pf[0..k) with the descriptors to poll for `want` (PL_TR_IN, PL_TR_OUT; 0 still reports a hangup) and
     * return k (<= PL_TR_POLL_MAX); *ready gets what of `want` is possible now without waiting, and PL_TR_IN for an
     * end or error already known */
    int (*poll_fds)(struct pl_transport *t, int want, struct pollfd *pf, int *ready);
    /* after poll(pf[0..k)): what of the last poll_fds' `want` the revents make possible, and PL_TR_IN for a hangup or
     * an error (never PL_TR_IN for bytes a paused link did not ask for) */
    int (*poll_ready)(struct pl_transport *t, const struct pollfd *pf, int k);
    /* release it: descriptors closed; a verbs transport drains its queue pair before its slots go back. The link
     * thread calls it last, after on_link(down) */
    void (*close)(struct pl_transport *t);
};

struct pl_transport {
    const struct pl_tr_ops *ops;
};

#endif
