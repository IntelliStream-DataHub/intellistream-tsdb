/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * link-bench: what a stream link between two servers costs for frames of the peer links' sizes (docs/design-rdma.md).
 * It speaks peerlink's frame shape ([u32 len][u8 type][payload][HMAC-SHA256 tag 32], the tag over a per-direction
 * sequence number, the type and the payload, as server/peerlink.c's frame_tag) over a stream socket, so one binary
 * measures TCP and rsocket:
 *
 *   link-bench serve [--rsocket] [--bind ADDR] [--port-file FILE] [--buf BYTES] [--parent PID]
 *                                                    one connection at a time, until killed (or PID is gone)
 *   link-bench run [--rsocket] --host ADDR --port P --mode bw|lat --size N [--secs S] [--tag | --mac NAME]
 *                  [--buf BYTES] [--batch N] [--label L]
 *   link-bench mac [--mac NAME] [--sizes N,N...] [--secs S]
 *                                                    one core tagging frames in memory: MB/s and ns per frame of each
 *                                                    MAC candidate (tools/mac_cands.h; all that run here without --mac)
 *
 * --tag is --mac hmac (peerlink's HMAC-SHA256 up to cluster version 7). The server takes the MAC from the client.
 *
 * --rsocket calls librdmacm's rsocket API (rsocket, rconnect, rread...), loaded with dlopen so that the binary builds
 * without librdmacm's headers and runs without the library when the flag is absent. It is not librspreload on
 * purpose: the preload falls back to a kernel socket when rconnect fails, which would send TCP over the interface the
 * rdma_cm address is on (IPoIB on InfiniBand), and a fallen-back run would measure TCP under the rsocket label. With
 * --rsocket a failure is a failure.
 *
 * bw: the client streams frames for --secs, the server reads (and checks every tag with --tag); lat: one frame each
 * way, the round trip timed per frame. Both sides count their own CPU (getrusage, user + system, the whole process:
 * librspreload's threads included) and the client prints one line of key=value: Gbit/s of payload, frames/s, the CPU
 * seconds per GB of payload on each side, and in lat mode the round trip's mean, p50 and p99 in microseconds.
 * --buf sizes the socket buffers before listen or connect (an accepted socket, TCP's or rsocket's, takes them from the
 * listening one: set after accept they come too late), so it must be given to both sides. --batch N (bw) writes N
 * frames per write() as peerlink does (frames appended to its write buffer); without it every frame is one write with
 * TCP_NODELAY, a syscall per frame, which is what the small-frame rows of docs/design-rdma.md 3.3 measured.
 * The server counts a frame whose tag does not verify (srv_errors) and goes on with the next one: the framing is intact.
 * --bad-tag-at K (a test hook, tests/test_link_bench.sh) makes the client send frame K with a wrong tag.
 * The server binds port 0 and writes "PORT PID" to --port-file (tmp, rename) once it listens: no port is picked here.
 * No address is built in: hosts are arguments (tools/rdma-bench.sh drives it on a pair of servers).
 */
#define _GNU_SOURCE
#include "mac_cands.h"

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <signal.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* the stream calls, from libc or from librdmacm's rsocket API (--rsocket) */
static struct {
    int (*socket)(int, int, int);
    int (*bind)(int, const struct sockaddr *, socklen_t);
    int (*listen)(int, int);
    int (*accept)(int, struct sockaddr *, socklen_t *);
    int (*connect)(int, const struct sockaddr *, socklen_t);
    int (*setsockopt)(int, int, int, const void *, socklen_t);
    int (*getsockname)(int, struct sockaddr *, socklen_t *);
    ssize_t (*read)(int, void *, size_t);
    ssize_t (*write)(int, const void *, size_t);
    int (*close)(int);
} io = {socket, bind, listen, accept, connect, setsockopt, getsockname, read, write, close};

static void use_rsocket(void)
{
    void *h = dlopen("librdmacm.so.1", RTLD_NOW);
    if (!h) {
        fprintf(stderr, "link-bench: --rsocket: %s\n", dlerror());
        exit(2);
    }
#define LB_SYM(f, name)                                                                                              \
    do {                                                                                                             \
        *(void **)&io.f = dlsym(h, name);                                                                            \
        if (!io.f) {                                                                                                 \
            fprintf(stderr, "link-bench: --rsocket: librdmacm has no %s\n", name);                                   \
            exit(2);                                                                                                 \
        }                                                                                                            \
    } while (0)
    LB_SYM(socket, "rsocket");
    LB_SYM(bind, "rbind");
    LB_SYM(listen, "rlisten");
    LB_SYM(accept, "raccept");
    LB_SYM(connect, "rconnect");
    LB_SYM(setsockopt, "rsetsockopt");
    LB_SYM(getsockname, "rgetsockname");
    LB_SYM(read, "rread");
    LB_SYM(write, "rwrite");
    LB_SYM(close, "rclose");
#undef LB_SYM
}

#define MAGIC 0x4c4b4231u /* "LKB1" */
#define TAG 32
#define SIZE_MAX_ (8u << 20) /* PL_FRAME_MAX */
#define LAT_MAX 2000000

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static uint64_t cpu_us(void)
{
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    return (uint64_t)(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000u +
           (uint64_t)(ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
}

static int full_write(int fd, const void *p, size_t n)
{
    const uint8_t *b = p;
    while (n) {
        ssize_t w = io.write(fd, b, n);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return -1;
        b += w;
        n -= (size_t)w;
    }
    return 0;
}

static int full_read(int fd, void *p, size_t n)
{
    uint8_t *b = p;
    while (n) {
        ssize_t r = io.read(fd, b, n);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return -1;
        b += r;
        n -= (size_t)r;
    }
    return 0;
}

static void put32(uint8_t *p, uint32_t v)
{
    memcpy(p, &v, 4);
}
static uint32_t get32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

/* the frame's tag: one of mac_cands.c's candidates under a fixed bench key (the cost, not the secret, is measured) */
static int mac_setup(struct mc *m, int kind)
{
    uint8_t key[32];
    for (int i = 0; i < 32; i++)
        key[i] = (uint8_t)(i * 7 + 1);
    if (kind < 0 || kind >= MC_N || mc_init(m, kind, key) != 0) {
        fprintf(stderr, "link-bench: the MAC %s does not run on this CPU\n", mc_name(kind));
        return -1;
    }
    return 0;
}

static uint64_t bad_tag_at = UINT64_MAX; /* --bad-tag-at: the sequence number whose tag is sent wrong */

/* seals one frame in place: [u32 len = 1 + n + tag][u8 type][payload n][tag]; f holds 5 + n + TAG; returns its length */
static size_t frame_seal(uint8_t *f, size_t n, const struct mc *k, uint64_t *seq)
{
    size_t tl = k->taglen;
    put32(f, (uint32_t)(1 + n + tl));
    f[4] = 1;
    if (tl) {
        mc_tag(k, *seq, 1, f + 5, n, "", 0, f + 5 + n);
        if (*seq == bad_tag_at)
            f[5 + n] ^= 1;
        (*seq)++;
    }
    return 5 + n + tl;
}

static int send_frame(int fd, uint8_t *buf, size_t n, const struct mc *k, uint64_t *seq)
{
    return full_write(fd, buf, frame_seal(buf, n, k, seq));
}

/* reads one frame into buf; returns the payload length, -2 for the end frame (len 0), -3 for a whole frame whose tag
 * is wrong (the stream stays in step), -1 on an error */
static long recv_frame(int fd, uint8_t *buf, size_t cap, const struct mc *k, uint64_t *seq)
{
    if (full_read(fd, buf, 4))
        return -1;
    uint32_t len = get32(buf);
    if (len == 0)
        return -2;
    size_t tl = k->taglen;
    if (len < 1u + tl || len > cap - 4 || full_read(fd, buf + 4, len))
        return -1;
    size_t n = len - 1 - tl;
    if (tl) {
        uint8_t t[32];
        mc_tag(k, (*seq)++, buf[4], buf + 5, n, "", 0, t);
        if (memcmp(t, buf + 5 + n, tl))
            return -3;
    }
    return (long)n;
}

struct hello {
    uint32_t magic, mode, size, tag; /* mode 0: bw, 1: lat */
};
struct result {
    uint64_t frames, bytes, cpu_us, errors;
};

static void set_opts(int fd, int buf)
{
    int one = 1;
    io.setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (buf > 0) {
        io.setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof buf);
        io.setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof buf);
    }
}

static void serve_one(int fd)
{
    struct hello h;
    struct result r = {0};
    if (full_read(fd, &h, sizeof h) || h.magic != MAGIC || h.size > SIZE_MAX_)
        return;
    size_t cap = 5 + (size_t)h.size + TAG;
    uint8_t *buf = malloc(cap);
    if (!buf)
        return;
    struct mc k;
    if (mac_setup(&k, (int)h.tag) != 0) {
        free(buf);
        return;
    }
    uint64_t rseq = 0, wseq = 0, c0 = cpu_us();
    for (;;) {
        long n = recv_frame(fd, buf, cap, &k, &rseq);
        if (n == -2)
            break;
        if (n == -1) {
            r.errors++;
            break;
        }
        if (n == -3) {
            r.errors++;
            n = 0; /* echoed empty in lat: the client's check of the echo's length fails */
        }
        r.frames++;
        r.bytes += (uint64_t)n;
        if (h.mode == 1 && send_frame(fd, buf, (size_t)n, &k, &wseq)) {
            r.errors++;
            break;
        }
    }
    r.cpu_us = cpu_us() - c0;
    full_write(fd, &r, sizeof r);
    free(buf);
}

static int serve(const char *bind_addr, const char *port_file, int buf, long parent)
{
#ifdef __linux__
    /* a test that dies must not leave its server behind: stop with the parent, and refuse to start under another */
    if (parent > 0 && (prctl(PR_SET_PDEATHSIG, SIGTERM) || getppid() != (pid_t)parent)) {
        fprintf(stderr, "link-bench: parent %ld is gone\n", parent);
        return 1;
    }
#else
    (void)parent;
#endif
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM, .ai_flags = AI_PASSIVE}, *ai;
    if (getaddrinfo(bind_addr, "0", &hints, &ai)) {
        fprintf(stderr, "link-bench: cannot resolve %s\n", bind_addr ? bind_addr : "(any)");
        return 1;
    }
    int ls = io.socket(ai->ai_family, SOCK_STREAM, 0), one = 1;
    io.setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    set_opts(ls, buf); /* before listen: the accepted socket takes its buffers from here */
    if (ls < 0 || io.bind(ls, ai->ai_addr, ai->ai_addrlen) || io.listen(ls, 4)) {
        perror("link-bench: listen");
        return 1;
    }
    freeaddrinfo(ai);
    struct sockaddr_storage sa;
    socklen_t sl = sizeof sa;
    io.getsockname(ls, (struct sockaddr *)&sa, &sl);
    int port = ntohs(sa.ss_family == AF_INET6 ? ((struct sockaddr_in6 *)&sa)->sin6_port
                                              : ((struct sockaddr_in *)&sa)->sin_port);
    if (port_file) {
        char tmp[4096];
        snprintf(tmp, sizeof tmp, "%s.tmp", port_file);
        FILE *f = fopen(tmp, "w");
        if (!f || fprintf(f, "%d %d\n", port, (int)getpid()) < 0 || fclose(f) || rename(tmp, port_file)) {
            perror("link-bench: port file");
            return 1;
        }
    } else
        printf("%d %d\n", port, (int)getpid());
    fflush(stdout);
    for (;;) {
        int fd = io.accept(ls, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            perror("link-bench: accept");
            return 1;
        }
        set_opts(fd, 0);
        serve_one(fd);
        io.close(fd);
    }
}

static int cmp_d(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static int run(const char *host, const char *port, int mode, size_t size, double secs, int tag, int buf, int batch,
               const char *label)
{
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM}, *ai;
    if (getaddrinfo(host, port, &hints, &ai)) {
        fprintf(stderr, "link-bench: cannot resolve %s\n", host);
        return 1;
    }
    int fd = io.socket(ai->ai_family, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("link-bench: socket");
        return 1;
    }
    set_opts(fd, buf);
    if (io.connect(fd, ai->ai_addr, ai->ai_addrlen)) {
        fprintf(stderr, "link-bench: connect %s port %s: %s\n", host, port, strerror(errno));
        return 2;
    }
    freeaddrinfo(ai);
    struct hello h = {MAGIC, (uint32_t)mode, (uint32_t)size, (uint32_t)tag};
    size_t cap = 5 + size + TAG;
    uint8_t *b = malloc(cap);
    double *lat = mode ? malloc(sizeof(double) * LAT_MAX) : NULL;
    if (!b || (mode && !lat) || full_write(fd, &h, sizeof h))
        return 1;
    for (size_t i = 0; i < cap; i++)
        b[i] = (uint8_t)(i * 131 + (i >> 9)); /* not all zero: a transport that compresses would cheat */
    struct mc k;
    if (mac_setup(&k, tag) != 0)
        return 2;
    size_t stride = 5 + size + k.taglen;
    uint8_t *bb = NULL; /* --batch: batch frames back to back, one write */
    if (!mode && batch > 1) {
        if (!(bb = malloc(stride * (size_t)batch)))
            return 1;
        for (int j = 0; j < batch; j++)
            memcpy(bb + stride * (size_t)j, b, stride);
    }
    uint64_t wseq = 0, rseq = 0, frames = 0, nlat = 0, c0 = cpu_us();
    double t0 = now_s(), t = t0, sum = 0;
    while (t - t0 < secs) {
        if (bb) {
            for (int j = 0; j < batch; j++)
                frame_seal(bb + stride * (size_t)j, size, &k, &wseq);
            if (full_write(fd, bb, stride * (size_t)batch)) {
                fprintf(stderr, "link-bench: write failed\n");
                return 1;
            }
            frames += (uint64_t)batch;
            t = now_s();
            continue;
        }
        for (int i = 0; i < (mode ? 1 : 16); i++) {
            double s = mode ? now_s() : 0;
            if (send_frame(fd, b, size, &k, &wseq)) {
                fprintf(stderr, "link-bench: write failed\n");
                return 1;
            }
            if (mode) {
                if (recv_frame(fd, b, cap, &k, &rseq) != (long)size) {
                    fprintf(stderr, "link-bench: bad echo\n");
                    return 1;
                }
                double d = (now_s() - s) * 1e6;
                sum += d;
                if (nlat < LAT_MAX)
                    lat[nlat++] = d;
            }
            frames++;
        }
        t = now_s();
    }
    uint8_t end[4] = {0};
    struct result r;
    if (full_write(fd, end, 4) || full_read(fd, &r, sizeof r)) {
        fprintf(stderr, "link-bench: no result from the server\n");
        return 1;
    }
    double el = now_s() - t0; /* bw: until the server read the last frame */
    uint64_t c1 = cpu_us() - c0;
    double gb = (double)frames * (double)size / 1e9;
    printf("link-bench label=%s mode=%s size=%zu tag=%d batch=%d secs=%.4f frames=%" PRIu64 " gbit=%.3f fps=%.0f"
           " cli_cpu_s_per_gb=%.3f srv_cpu_s_per_gb=%.3f cli_cpu_pct=%.1f srv_cpu_pct=%.1f srv_errors=%" PRIu64,
           label, mode ? "lat" : "bw", size, tag, bb ? batch : 1, el, frames, gb * 8 / el, (double)frames / el,
           gb > 0 ? (double)c1 / 1e6 / gb : 0, gb > 0 ? (double)r.cpu_us / 1e6 / gb : 0, (double)c1 / 1e4 / el,
           (double)r.cpu_us / 1e4 / el, r.errors);
    if (mode && nlat) {
        qsort(lat, nlat, sizeof *lat, cmp_d);
        printf(" rtt_sum_s=%.4f rtt_mean_us=%.2f rtt_p50_us=%.2f rtt_p99_us=%.2f", sum / 1e6, sum / (double)frames,
               lat[nlat / 2],
               lat[(size_t)((double)nlat * 0.99)]);
    }
    printf("\n");
    io.close(fd);
    free(b);
    free(bb);
    free(lat);
    return r.errors || r.frames != frames ? 1 : 0;
}

static void usage(void)
{
    fputs("usage: link-bench serve [--rsocket] [--bind ADDR] [--port-file FILE] [--buf BYTES] [--parent PID]\n"
          "       link-bench run [--rsocket] --host ADDR --port P --mode bw|lat --size N [--secs S] [--tag] [--buf BYTES]"
          " [--batch N] [--label L]\n",
          stderr);
    exit(2);
}

/* one core tags frames of each size for `secs` of its CPU time: payload MB/s, ns per frame, CPU s per GB */
static int mac_bench(int only, const char *sizes, double secs)
{
    if (mc_selftest() != 0) {
        fprintf(stderr, "link-bench: a MAC candidate gives wrong tags here\n");
        return 1;
    }
    size_t cap = 1 << 20;
    for (const char *p = sizes; *p; p += *p == ',') {
        size_t n = strtoull(p, (char **)&p, 0);
        if (n > cap)
            cap = n;
        if (*p && *p != ',')
            usage();
    }
    uint8_t *b = malloc(cap);
    if (!b)
        return 1;
    for (size_t i = 0; i < cap; i++)
        b[i] = (uint8_t)(i * 131 + (i >> 9));
    for (int kind = MC_HMAC; kind < MC_N; kind++) {
        if (only > 0 && kind != only)
            continue;
        struct mc m;
        if (mac_setup(&m, kind) != 0) {
            if (only > 0)
                return 2;
            printf("mac=%s unavailable=1\n", mc_name(kind));
            continue;
        }
        for (const char *p = sizes; *p; p += *p == ',') {
            size_t n = strtoull(p, (char **)&p, 0);
            uint64_t frames = 0, sink = 0, seq = 0;
            struct timespec c0, c1;
            clock_gettime(CLOCK_THREAD_CPUTIME_ID, &c0);
            double el = 0;
            do {
                for (int j = 0; j < 64; j++) {
                    uint8_t t[32];
                    mc_tag(&m, seq++, 1, b, n, "", 0, t);
                    sink += t[0];
                }
                frames += 64;
                clock_gettime(CLOCK_THREAD_CPUTIME_ID, &c1);
                el = (double)(c1.tv_sec - c0.tv_sec) + (double)(c1.tv_nsec - c0.tv_nsec) / 1e9;
            } while (el < secs);
            double bytes = (double)frames * (double)n;
            printf("mac=%s size=%zu MB_s=%.1f ns_frame=%.1f cpu_s_GB=%.3f sink=%" PRIu64 "\n", mc_name(kind), n,
                   bytes / el / 1e6, el * 1e9 / (double)frames, el / (bytes / 1e9), sink & 1);
            fflush(stdout);
        }
    }
    free(b);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2)
        usage();
    const char *bind_addr = NULL, *port_file = NULL, *host = NULL, *port = NULL, *label = "-";
    int mode = 0, tag = 0, buf = 0, batch = 1;
    long parent = 0;
    size_t size = 1024;
    const char *sizes = "64,1024,16384,1048576";
    double secs = 3;
    for (int i = 2; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!strcmp(a, "--tag")) {
            tag = MC_HMAC;
            continue;
        }
        if (!strcmp(a, "--rsocket")) {
            use_rsocket();
            continue;
        }
        if (!v)
            usage();
        i++;
        if (!strcmp(a, "--bind"))
            bind_addr = v;
        else if (!strcmp(a, "--port-file"))
            port_file = v;
        else if (!strcmp(a, "--host"))
            host = v;
        else if (!strcmp(a, "--port"))
            port = v;
        else if (!strcmp(a, "--mode"))
            mode = !strcmp(v, "lat") ? 1 : !strcmp(v, "bw") ? 0 : (usage(), 0);
        else if (!strcmp(a, "--size"))
            size = strtoull(v, NULL, 0);
        else if (!strcmp(a, "--secs"))
            secs = atof(v);
        else if (!strcmp(a, "--buf"))
            buf = atoi(v);
        else if (!strcmp(a, "--mac")) {
            if ((tag = mc_kind(v)) < 0)
                usage();
        } else if (!strcmp(a, "--sizes"))
            sizes = v;
        else if (!strcmp(a, "--label"))
            label = v;
        else if (!strcmp(a, "--batch"))
            batch = atoi(v);
        else if (!strcmp(a, "--parent"))
            parent = atol(v);
        else if (!strcmp(a, "--bad-tag-at"))
            bad_tag_at = strtoull(v, NULL, 0);
        else
            usage();
    }
    if (size == 0 || size > SIZE_MAX_) {
        fprintf(stderr, "link-bench: --size must be 1 .. %u\n", SIZE_MAX_);
        return 2;
    }
    if (batch < 1 || batch > 4096) {
        fprintf(stderr, "link-bench: --batch must be 1 .. 4096\n");
        return 2;
    }
    if (!strcmp(argv[1], "mac"))
        return mac_bench(tag ? tag : -1, sizes, secs);
    signal(SIGPIPE, SIG_IGN); /* a peer that went away is a failed write, not a dead process */
    if (!strcmp(argv[1], "serve"))
        return serve(bind_addr, port_file, buf, parent);
    if (!strcmp(argv[1], "run") && host && port)
        return run(host, port, mode, size, secs, tag, buf, batch, label);
    usage();
}
