/* SPDX-License-Identifier: AGPL-3.0-only */
/* Bounded and growable printf-style string buffers for tsdbd. */
#ifndef TSDBD_STBUF_H
#define TSDBD_STBUF_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* Safe bounded append into a fixed-size string buffer; never underflows cap - *pos. */
static inline void str_appendf(char *buf, size_t cap, size_t *pos, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));
static inline void str_appendf(char *buf, size_t cap, size_t *pos, const char *fmt, ...)
{
    if (cap == 0 || *pos >= cap - 1)
        return;
    size_t rem = cap - *pos;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + *pos, rem, fmt, ap);
    va_end(ap);
    if (n < 0)
        return;
    if ((size_t)n >= rem)
        *pos = cap - 1;
    else
        *pos += (size_t)n;
}

/* Growable formatted buffer for STATUS responses; never truncates or underflows cap - len. */
typedef struct {
    char *p;
    size_t len, cap;
    bool oom;
} st_buf;

static inline void st_putf(st_buf *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static inline void st_putf(st_buf *b, const char *fmt, ...)
{
    if (b->oom)
        return;
    va_list ap, ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    size_t avail = (b->p && b->cap > b->len) ? b->cap - b->len : 0;
    int n = vsnprintf(b->p ? b->p + b->len : NULL, avail, fmt, ap);
    va_end(ap);
    if (n < 0) {
        va_end(ap2);
        return;
    }
    size_t need = (size_t)n;
    if (need >= avail) {
        if (need >= SIZE_MAX - b->len) {
            va_end(ap2);
            b->oom = true;
            return;
        }
        size_t want = b->len + need + 1;
        size_t ncap = b->cap ? b->cap : 512;
        while (ncap < want) {
            if (ncap > SIZE_MAX / 2) {
                ncap = want;
                break;
            }
            ncap *= 2;
        }
        char *np = realloc(b->p, ncap);
        if (!np) {
            va_end(ap2);
            b->oom = true;
            return;
        }
        b->p = np;
        b->cap = ncap;
        n = vsnprintf(b->p + b->len, b->cap - b->len, fmt, ap2);
        va_end(ap2);
        if (n < 0)
            return;
        if ((size_t)n >= b->cap - b->len)
            need = b->cap - b->len - 1;
        else
            need = (size_t)n;
    } else {
        va_end(ap2);
    }
    b->len += need;
}

#endif
