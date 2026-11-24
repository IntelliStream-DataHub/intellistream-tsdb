/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * link-bench: what a stream link between two servers costs for frames of the peer links' sizes (docs/design-rdma.md).
 * It speaks peerlink's frame shape ([u32 len][u8 type][payload][HMAC-SHA256 tag 32], the tag over a per-direction
 * sequence number, the type and the payload, as server/peerlink.c's frame_tag) over a plain stream socket, so one
 * binary measures TCP on any interface and rsocket under librspreload (LD_PRELOAD), without linking librdmacm:
 *
 *   link-bench serve [--bind ADDR] [--port-file FILE]        one connection at a time, until killed
 *   link-bench run --host ADDR --port P --mode bw|lat --size N [--secs S] [--tag] [--buf BYTES] [--label L]
 *
 * bw: the client streams frames for --secs, the server reads (and checks every tag with --tag); lat: one frame each
 * way, the round trip timed per frame. Both sides count their own CPU (getrusage, user + system, the whole process:
 * librspreload's threads included) and the client prints one line of key=value: Gbit/s of payload, frames/s, the CPU
 * seconds per GB of payload on each side, and in lat mode the round trip's mean, p50 and p99 in microseconds.
 * The server binds port 0 and writes "PORT PID" to --port-file (tmp, rename) once it listens: no port is picked here.
 * No address is built in: hosts are arguments (tools/rdma-bench.sh drives it on a pair of servers).
 */
#define _GNU_SOURCE
#include "../client/tsdb_crypto.h"

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
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

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
        ssize_t w = write(fd, b, n);
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
        ssize_t r = read(fd, b, n);
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

/* peerlink's HMAC with the pads absorbed once (server/peerlink.c hk_init, hk_mac, frame_tag) */
struct hk {
    tsdb_sha256 in, out;
};

static void hk_init(struct hk *k)
{
    uint8_t pad[64];
    for (int i = 0; i < 64; i++)
        pad[i] = (uint8_t)(i * 7 + 1) ^ 0x36; /* a fixed bench key: the cost, not the secret, is measured */
    tsdb_sha256_init(&k->in);
    tsdb_sha256_update(&k->in, pad, 64);
    for (int i = 0; i < 64; i++)
        pad[i] = (uint8_t)(i * 7 + 1) ^ 0x5c;
    tsdb_sha256_init(&k->out);
    tsdb_sha256_update(&k->out, pad, 64);
}

static void frame_tag(const struct hk *k, uint64_t seq, uint8_t type, const void *p, size_t n, uint8_t out[32])
{
    uint8_t s[9], ih[32];
    memcpy(s, &seq, 8);
    s[8] = type;
    tsdb_sha256 x = k->in;
    tsdb_sha256_update(&x, s, 9);
    tsdb_sha256_update(&x, p, n);
    tsdb_sha256_final(&x, ih);
    x = k->out;
    tsdb_sha256_update(&x, ih, 32);
    tsdb_sha256_final(&x, out);
}

/* one frame: [u32 len = 1 + n + tag][u8 type][payload n][tag]; buf holds 5 + n + TAG */
static int send_frame(int fd, uint8_t *buf, size_t n, int tag, const struct hk *k, uint64_t *seq)
{
    put32(buf, (uint32_t)(1 + n + (tag ? TAG : 0)));
    buf[4] = 1;
    if (tag)
        frame_tag(k, (*seq)++, 1, buf + 5, n, buf + 5 + n);
    return full_write(fd, buf, 5 + n + (tag ? TAG : 0));
}

/* reads one frame into buf; returns the payload length, -2 for the end frame (len 0), -1 on error or a bad tag */
static long recv_frame(int fd, uint8_t *buf, size_t cap, int tag, const struct hk *k, uint64_t *seq)
{
    if (full_read(fd, buf, 4))
        return -1;
    uint32_t len = get32(buf);
    if (len == 0)
        return -2;
    if (len < 1u + (tag ? TAG : 0) || len > cap - 4 || full_read(fd, buf + 4, len))
        return -1;
    size_t n = len - 1 - (tag ? TAG : 0);
    if (tag) {
        uint8_t t[32];
        frame_tag(k, (*seq)++, buf[4], buf + 5, n, t);
        if (memcmp(t, buf + 5 + n, TAG))
            return -1;
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
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (buf > 0) {
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof buf);
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof buf);
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
    struct hk k;
    hk_init(&k);
    uint64_t rseq = 0, wseq = 0, c0 = cpu_us();
    for (;;) {
        long n = recv_frame(fd, buf, cap, (int)h.tag, &k, &rseq);
        if (n == -2)
            break;
        if (n < 0) {
            r.errors++;
            break;
        }
        r.frames++;
        r.bytes += (uint64_t)n;
        if (h.mode == 1 && send_frame(fd, buf, (size_t)n, (int)h.tag, &k, &wseq)) {
            r.errors++;
            break;
        }
    }
    r.cpu_us = cpu_us() - c0;
    full_write(fd, &r, sizeof r);
    free(buf);
}

static int serve(const char *bind_addr, const char *port_file, int buf)
{
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM, .ai_flags = AI_PASSIVE}, *ai;
    if (getaddrinfo(bind_addr, "0", &hints, &ai)) {
        fprintf(stderr, "link-bench: cannot resolve %s\n", bind_addr ? bind_addr : "(any)");
        return 1;
    }
    int ls = socket(ai->ai_family, SOCK_STREAM, 0), one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (ls < 0 || bind(ls, ai->ai_addr, ai->ai_addrlen) || listen(ls, 4)) {
        perror("link-bench: listen");
        return 1;
    }
    freeaddrinfo(ai);
    struct sockaddr_storage sa;
    socklen_t sl = sizeof sa;
    getsockname(ls, (struct sockaddr *)&sa, &sl);
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
        int fd = accept(ls, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            perror("link-bench: accept");
            return 1;
        }
        set_opts(fd, buf);
        serve_one(fd);
        close(fd);
    }
}

static int cmp_d(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static int run(const char *host, const char *port, int mode, size_t size, double secs, int tag, int buf,
               const char *label)
{
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM}, *ai;
    if (getaddrinfo(host, port, &hints, &ai)) {
        fprintf(stderr, "link-bench: cannot resolve %s\n", host);
        return 1;
    }
    int fd = socket(ai->ai_family, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("link-bench: socket");
        return 1;
    }
    set_opts(fd, buf);
    if (connect(fd, ai->ai_addr, ai->ai_addrlen)) {
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
    struct hk k;
    hk_init(&k);
    uint64_t wseq = 0, rseq = 0, frames = 0, nlat = 0, c0 = cpu_us();
    double t0 = now_s(), t = t0, sum = 0;
    while (t - t0 < secs) {
        for (int i = 0; i < (mode ? 1 : 16); i++) {
            double s = mode ? now_s() : 0;
            if (send_frame(fd, b, size, tag, &k, &wseq)) {
                fprintf(stderr, "link-bench: write failed\n");
                return 1;
            }
            if (mode) {
                if (recv_frame(fd, b, cap, tag, &k, &rseq) != (long)size) {
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
    printf("link-bench label=%s mode=%s size=%zu tag=%d secs=%.2f frames=%" PRIu64 " gbit=%.3f fps=%.0f"
           " cli_cpu_s_per_gb=%.3f srv_cpu_s_per_gb=%.3f cli_cpu_pct=%.1f srv_cpu_pct=%.1f srv_errors=%" PRIu64,
           label, mode ? "lat" : "bw", size, tag, el, frames, gb * 8 / el, (double)frames / el,
           gb > 0 ? (double)c1 / 1e6 / gb : 0, gb > 0 ? (double)r.cpu_us / 1e6 / gb : 0, (double)c1 / 1e4 / el,
           (double)r.cpu_us / 1e4 / el, r.errors);
    if (mode && nlat) {
        qsort(lat, nlat, sizeof *lat, cmp_d);
        printf(" rtt_mean_us=%.2f rtt_p50_us=%.2f rtt_p99_us=%.2f", sum / (double)frames, lat[nlat / 2],
               lat[(size_t)((double)nlat * 0.99)]);
    }
    printf("\n");
    close(fd);
    free(b);
    free(lat);
    return r.errors || r.frames != frames ? 1 : 0;
}

static void usage(void)
{
    fputs("usage: link-bench serve [--bind ADDR] [--port-file FILE] [--buf BYTES]\n"
          "       link-bench run --host ADDR --port P --mode bw|lat --size N [--secs S] [--tag] [--buf BYTES]"
          " [--label L]\n",
          stderr);
    exit(2);
}

int main(int argc, char **argv)
{
    if (argc < 2)
        usage();
    const char *bind_addr = NULL, *port_file = NULL, *host = NULL, *port = NULL, *label = "-";
    int mode = 0, tag = 0, buf = 0;
    size_t size = 1024;
    double secs = 3;
    for (int i = 2; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!strcmp(a, "--tag")) {
            tag = 1;
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
        else if (!strcmp(a, "--label"))
            label = v;
        else
            usage();
    }
    if (size == 0 || size > SIZE_MAX_) {
        fprintf(stderr, "link-bench: --size must be 1 .. %u\n", SIZE_MAX_);
        return 2;
    }
    if (!strcmp(argv[1], "serve"))
        return serve(bind_addr, port_file, buf);
    if (!strcmp(argv[1], "run") && host && port)
        return run(host, port, mode, size, secs, tag, buf, label);
    usage();
}
