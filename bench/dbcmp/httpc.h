/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Minimal HTTP/1.1 client for loadlp and qdrive: one keep-alive connection per thread, a request
 * at a time, Content-Length or chunked responses. Bodies are always read to the end, so a query
 * is timed until the client has its whole result, as with the other protocols. It exists so the
 * harness needs no libcurl headers and adds as little client cost as possible; it does no TLS,
 * redirects or proxies, and retries a request once when the server closed an idle connection.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct hconn {
    int fd, port;
    const char *host;
    char *buf; /* response bytes not yet consumed */
    size_t cap, len, off;
} hconn;

static int h_open(hconn *c)
{
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(c->port)};
    inet_pton(AF_INET, c->host, &a.sin_addr);
    c->fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (connect(c->fd, (struct sockaddr *)&a, sizeof a) != 0) {
        perror("connect");
        close(c->fd);
        c->fd = -1;
        return -1;
    }
    c->len = c->off = 0;
    return 0;
}

static int h_init(hconn *c, const char *host, int port)
{
    memset(c, 0, sizeof *c);
    c->host = host;
    c->port = port;
    c->cap = 1 << 20;
    c->buf = malloc(c->cap);
    return h_open(c);
}

static int h_writeall(int fd, const char *p, size_t n)
{
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

/* make at least `need` unconsumed bytes available */
static int h_fill(hconn *c, size_t need)
{
    while (c->len - c->off < need) {
        if (c->off && c->off == c->len) c->off = c->len = 0;
        if (c->len == c->cap) {
            if (c->off) {
                memmove(c->buf, c->buf + c->off, c->len - c->off);
                c->len -= c->off;
                c->off = 0;
            } else {
                c->cap *= 2;
                c->buf = realloc(c->buf, c->cap);
            }
        }
        ssize_t r = read(c->fd, c->buf + c->len, c->cap - c->len);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        c->len += (size_t)r;
    }
    return 0;
}

/* returns a pointer to the line (without CRLF) and consumes it */
static char *h_line(hconn *c)
{
    for (;;) {
        char *s = c->buf + c->off, *e = memchr(s, '\n', c->len - c->off);
        if (e) {
            *e = 0;
            if (e > s && e[-1] == '\r') e[-1] = 0;
            c->off = (size_t)(e + 1 - c->buf);
            return s;
        }
        if (h_fill(c, c->len - c->off + 1) != 0) return NULL;
    }
}

/* Body bytes are appended to *body (if body != NULL), else discarded. Returns the status or -1. */
typedef struct hbuf { char *p; size_t n, cap; } hbuf;
static void hb_add(hbuf *b, const char *p, size_t n)
{
    if (!b) return;
    if (b->n + n + 1 > b->cap) {
        b->cap = (b->n + n + 1) * 2;
        b->p = realloc(b->p, b->cap);
    }
    memcpy(b->p + b->n, p, n);
    b->n += n;
    b->p[b->n] = 0;
}

static int h_body(hconn *c, size_t n, hbuf *out, size_t *total)
{
    while (n) {
        if (h_fill(c, 1) != 0) return -1;
        size_t k = c->len - c->off < n ? c->len - c->off : n;
        hb_add(out, c->buf + c->off, k);
        c->off += k;
        n -= k;
        *total += k;
    }
    return 0;
}

static int h_request(hconn *c, const char *method, const char *path, const char *headers, const char *body,
                     size_t blen, hbuf *out, size_t *rbytes)
{
    for (int attempt = 0; attempt < 2; attempt++) {
        if (c->fd < 0 && h_open(c) != 0) return -1;
        /* a GET of a batch of 100 series carries its query in the path (InfluxQL: 30 KB and more): the head is
           sized to fit, never cut (a cut request could be a valid, smaller query and be timed as one) */
        char small[8192], *hdr = small;
        const char *fmt = "%s %s HTTP/1.1\r\nHost: %s:%d\r\n%sContent-Length: %zu\r\n\r\n";
        int hl = snprintf(hdr, sizeof small, fmt, method, path, c->host, c->port, headers ? headers : "", blen);
        if (hl < 0) return -1;
        if ((size_t)hl >= sizeof small) {
            if (!(hdr = malloc((size_t)hl + 1))) return -1;
            snprintf(hdr, (size_t)hl + 1, fmt, method, path, c->host, c->port, headers ? headers : "", blen);
        }
        int werr = h_writeall(c->fd, hdr, (size_t)hl) != 0 || (blen && h_writeall(c->fd, body, blen) != 0);
        if (hdr != small) free(hdr);
        if (werr) {
            close(c->fd);
            c->fd = -1;
            continue; /* the server closed an idle keep-alive connection: retry once */
        }
        char *l = h_line(c);
        if (!l) {
            close(c->fd);
            c->fd = -1;
            if (attempt == 0) continue;
            return -1;
        }
        int status = atoi(l + 9);
        long long clen = -1;
        int chunked = 0, closeit = 0;
        while ((l = h_line(c)) && *l) {
            if (!strncasecmp(l, "content-length:", 15)) clen = atoll(l + 15);
            else if (!strncasecmp(l, "transfer-encoding:", 18) && strcasestr(l, "chunked")) chunked = 1;
            else if (!strncasecmp(l, "connection:", 11) && strcasestr(l, "close")) closeit = 1;
        }
        if (!l) return -1;
        size_t total = 0;
        if (chunked) {
            for (;;) {
                char *cl = h_line(c);
                if (!cl) return -1;
                size_t n = strtoull(cl, NULL, 16);
                if (n == 0) {
                    while ((cl = h_line(c)) && *cl) {}
                    break;
                }
                if (h_body(c, n, out, &total) != 0 || !h_line(c)) return -1;
            }
        } else if (clen >= 0) {
            if (h_body(c, (size_t)clen, out, &total) != 0) return -1;
        } else if (status != 204 && status != 304) { /* read to close */
            while (h_fill(c, 1) == 0) {
                hb_add(out, c->buf + c->off, c->len - c->off);
                total += c->len - c->off;
                c->off = c->len;
            }
            closeit = 1;
        }
        if (rbytes) *rbytes = total;
        if (closeit) {
            close(c->fd);
            c->fd = -1;
        }
        return status;
    }
    return -1;
}
