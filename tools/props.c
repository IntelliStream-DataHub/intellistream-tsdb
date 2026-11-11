/* SPDX-License-Identifier: AGPL-3.0-only */
/* A reader of .properties files (props.h). */
#include "props.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void seterr(char *err, size_t sz, const char *fmt, ...)
{
    if (!err || !sz)
        return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, sz, fmt, ap);
    va_end(ap);
}

static int is_ws(char c)
{
    return c == ' ' || c == '\t' || c == '\f';
}

/* Is the character at s[i] preceded by an odd number of backslashes, not looking before s[lo]? (it is escaped) */
static int escaped_at(const char *s, size_t lo, size_t i)
{
    size_t n = 0;
    while (i - n > lo && s[i - n - 1] == '\\')
        n++;
    return n & 1;
}

/* The line (1-based) of the first byte that is not well-formed UTF-8 or is NUL, or 0 when all is well. Strict: no
 * overlong forms, no surrogates, nothing above U+10FFFF. */
static int utf8_bad_line(const unsigned char *s, size_t n)
{
    int line = 1;
    for (size_t i = 0; i < n;) {
        unsigned char c = s[i];
        if (c == '\n' || (c == '\r' && !(i + 1 < n && s[i + 1] == '\n')))
            line++;
        if (c == 0)
            return line;
        if (c < 0x80) {
            i++;
            continue;
        }
        size_t k;
        unsigned cp;
        if (c >= 0xC2 && c <= 0xDF) {
            k = 1;
            cp = c & 0x1F;
        } else if (c >= 0xE0 && c <= 0xEF) {
            k = 2;
            cp = c & 0x0F;
        } else if (c >= 0xF0 && c <= 0xF4) {
            k = 3;
            cp = c & 0x07;
        } else {
            return line;
        }
        if (i + k >= n)
            return line;
        for (size_t j = 1; j <= k; j++) {
            if ((s[i + j] & 0xC0) != 0x80)
                return line;
            cp = cp << 6 | (s[i + j] & 0x3F);
        }
        if ((k == 2 && (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF))) || (k == 3 && (cp < 0x10000 || cp > 0x10FFFF)))
            return line;
        i += k + 1;
    }
    return 0;
}

static size_t put_utf8(char *o, unsigned cp)
{
    if (cp < 0x80) {
        o[0] = (char)cp;
        return 1;
    }
    if (cp < 0x800) {
        o[0] = (char)(0xC0 | cp >> 6);
        o[1] = (char)(0x80 | (cp & 0x3F));
        return 2;
    }
    if (cp < 0x10000) {
        o[0] = (char)(0xE0 | cp >> 12);
        o[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        o[2] = (char)(0x80 | (cp & 0x3F));
        return 3;
    }
    o[0] = (char)(0xF0 | cp >> 18);
    o[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    o[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
    o[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

static int hex4(const char *s, size_t avail, unsigned *v)
{
    if (avail < 4)
        return -1;
    unsigned r = 0;
    for (int i = 0; i < 4; i++) {
        char c = s[i];
        unsigned d = c >= '0' && c <= '9' ? (unsigned)(c - '0')
                     : c >= 'a' && c <= 'f' ? (unsigned)(c - 'a' + 10)
                     : c >= 'A' && c <= 'F' ? (unsigned)(c - 'A' + 10) : 99u;
        if (d == 99u)
            return -1;
        r = r << 4 | d;
    }
    *v = r;
    return 0;
}

/* s[0..n) with its escapes processed, as a new string. */
static int unesc(const char *s, size_t n, char **out, char *err, size_t errsz)
{
    char *o = malloc(n + 1); /* an escape never grows: \uXXXX (6 bytes) makes at most 3, a pair (12) 4 */
    if (!o) {
        seterr(err, errsz, "out of memory");
        return -1;
    }
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] != '\\') {
            o[w++] = s[i];
            continue;
        }
        if (++i >= n) { /* cannot happen for what props_parse hands over */
            seterr(err, errsz, "a backslash at the end of the value");
            goto bad;
        }
        char c = s[i];
        switch (c) {
        case 't': o[w++] = '\t'; break;
        case 'n': o[w++] = '\n'; break;
        case 'r': o[w++] = '\r'; break;
        case 'f': o[w++] = '\f'; break;
        case '\\': case '=': case ':': case '#': case '!': case ' ': case ',': o[w++] = c; break;
        case 'u': {
            unsigned cp, lo;
            if (hex4(s + i + 1, n - i - 1, &cp) != 0) {
                seterr(err, errsz, "\\u must be followed by four hex digits");
                goto bad;
            }
            i += 4;
            if (cp >= 0xD800 && cp <= 0xDBFF) { /* a high surrogate needs its low half, written as \uXXXX too */
                if (i + 6 < n && s[i + 1] == '\\' && s[i + 2] == 'u' && hex4(s + i + 3, n - i - 3, &lo) == 0 &&
                    lo >= 0xDC00 && lo <= 0xDFFF) {
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                    i += 6;
                } else {
                    seterr(err, errsz, "\\u%04X is half of a surrogate pair without the other half", cp);
                    goto bad;
                }
            } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                seterr(err, errsz, "\\u%04X is half of a surrogate pair without the other half", cp);
                goto bad;
            }
            if (cp == 0) {
                seterr(err, errsz, "\\u0000: a value cannot hold a NUL");
                goto bad;
            }
            w += put_utf8(o + w, cp);
            break;
        }
        default:
            if ((unsigned char)c >= 0x20 && (unsigned char)c < 0x7F)
                seterr(err, errsz, "unknown escape \\%c (a backslash is written \\\\)", c);
            else
                seterr(err, errsz, "unknown escape after a backslash (a backslash is written \\\\)");
            goto bad;
        }
    }
    o[w] = 0;
    *out = o;
    return 0;
bad:
    free(o);
    return -1;
}

typedef struct {
    char *p;
    size_t n, cap;
} buf;

static int buf_add(buf *b, const char *s, size_t n)
{
    if (b->n + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap : 128;
        while (cap < b->n + n + 1)
            cap *= 2;
        char *q = realloc(b->p, cap);
        if (!q)
            return -1;
        b->p = q;
        b->cap = cap;
    }
    memcpy(b->p + b->n, s, n);
    b->n += n;
    b->p[b->n] = 0;
    return 0;
}

void props_free(props *p)
{
    for (size_t i = 0; p && i < p->n; i++) {
        free(p->v[i].key);
        free(p->v[i].raw);
    }
    if (p) {
        free(p->v);
        p->v = NULL;
        p->n = 0;
    }
}

int props_parse(const char *buf_in, size_t len, props *out, char *err, size_t errsz)
{
    out->v = NULL;
    out->n = 0;
    const char *s = buf_in;
    if (len >= 3 && (unsigned char)s[0] == 0xEF && (unsigned char)s[1] == 0xBB && (unsigned char)s[2] == 0xBF) {
        s += 3; /* a byte order mark that an editor wrote */
        len -= 3;
    }
    int bad = utf8_bad_line((const unsigned char *)s, len);
    if (bad) {
        seterr(err, errsz, "line %d: not valid UTF-8 (the file is read as UTF-8; write other characters as \\uXXXX) "
                           "or holds a NUL byte", bad);
        return -1;
    }
    size_t cap = 0;
    buf b = {0};
    size_t i = 0;
    int line = 1;
    while (i < len) {
        while (i < len && is_ws(s[i]))
            i++;
        if (i >= len)
            break;
        if (s[i] == '\n' || s[i] == '\r') {
            i += (s[i] == '\r' && i + 1 < len && s[i + 1] == '\n') ? 2 : 1;
            line++;
            continue;
        }
        if (s[i] == '#' || s[i] == '!') {
            while (i < len && s[i] != '\n' && s[i] != '\r')
                i++;
            continue;
        }
        int start = line;
        b.n = 0;
        if (b.p)
            b.p[0] = 0;
        for (;;) {
            size_t ps = i;
            while (i < len && s[i] != '\n' && s[i] != '\r')
                i++;
            size_t bs = 0;
            while (bs < i - ps && s[i - 1 - bs] == '\\')
                bs++;
            size_t pe = i;
            if (i < len) {
                i += (s[i] == '\r' && i + 1 < len && s[i + 1] == '\n') ? 2 : 1;
                line++;
            }
            if (buf_add(&b, s + ps, pe - ps) != 0)
                goto oom;
            if (!(bs & 1))
                break;
            b.n--; /* the continuing backslash */
            b.p[b.n] = 0;
            if (i >= len) {
                seterr(err, errsz, "line %d: the file ends in a backslash (a continuation with nothing after it)", start);
                goto fail;
            }
            while (i < len && is_ws(s[i]))
                i++;
        }
        /* key: up to the first unescaped white space, = or : */
        size_t j = 0;
        while (j < b.n && !is_ws(b.p[j]) && b.p[j] != '=' && b.p[j] != ':') {
            if (b.p[j] == '\\' && j + 1 < b.n)
                j++;
            j++;
        }
        size_t klen = j;
        while (j < b.n && is_ws(b.p[j]))
            j++;
        if (j < b.n && (b.p[j] == '=' || b.p[j] == ':'))
            j++;
        while (j < b.n && is_ws(b.p[j]))
            j++;
        size_t vend = b.n;
        while (vend > j && is_ws(b.p[vend - 1]) && !escaped_at(b.p, j, vend - 1))
            vend--;
        if (klen == 0) {
            seterr(err, errsz, "line %d: no key before the separator", start);
            goto fail;
        }
        char *key, e2[256];
        if (unesc(b.p, klen, &key, e2, sizeof e2) != 0) {
            seterr(err, errsz, "line %d: key: %s", start, e2);
            goto fail;
        }
        char *raw = malloc(vend - j + 1);
        if (!raw) {
            free(key);
            goto oom;
        }
        memcpy(raw, b.p + j, vend - j);
        raw[vend - j] = 0;
        if (out->n == cap) {
            cap = cap ? cap * 2 : 16;
            prop *nv = realloc(out->v, cap * sizeof *nv);
            if (!nv) {
                free(key);
                free(raw);
                goto oom;
            }
            out->v = nv;
        }
        out->v[out->n++] = (prop){key, raw, start};
    }
    free(b.p);
    return 0;
oom:
    seterr(err, errsz, "out of memory");
fail:
    free(b.p);
    props_free(out);
    return -1;
}

/* Index of the first comma of raw[from..n) that is not escaped, or n. */
static size_t next_comma(const char *raw, size_t from, size_t n)
{
    for (size_t i = from; i < n; i++) {
        if (raw[i] == '\\')
            i++;
        else if (raw[i] == ',')
            return i;
    }
    return n;
}

int props_value(const char *raw, char **out, char *err, size_t errsz)
{
    size_t n = strlen(raw);
    if (next_comma(raw, 0, n) < n) {
        seterr(err, errsz, "a list where one value is expected (write \\, for a comma that belongs to the value)");
        return -1;
    }
    return unesc(raw, n, out, err, errsz);
}

void props_items_free(char **items, size_t n)
{
    for (size_t i = 0; items && i < n; i++)
        free(items[i]);
    free(items);
}

int props_split(const char *raw, char ***items, size_t *n, char *err, size_t errsz)
{
    size_t len = strlen(raw), cnt = 0, cap = 0, i = 0;
    char **v = NULL;
    *items = NULL;
    *n = 0;
    while (i < len && is_ws(raw[i]))
        i++;
    if (i >= len)
        return 0; /* no elements */
    for (i = 0;;) {
        size_t e = next_comma(raw, i, len), a = i, z = e;
        while (a < z && is_ws(raw[a]))
            a++;
        while (z > a && is_ws(raw[z - 1]) && !escaped_at(raw, a, z - 1))
            z--;
        if (a == z) {
            seterr(err, errsz, "an empty element in the list (a doubled or trailing comma)");
            goto bad;
        }
        if (cnt == cap) {
            cap = cap ? cap * 2 : 8;
            char **nv = realloc(v, cap * sizeof *nv);
            if (!nv) {
                seterr(err, errsz, "out of memory");
                goto bad;
            }
            v = nv;
        }
        if (unesc(raw + a, z - a, &v[cnt], err, errsz) != 0)
            goto bad;
        cnt++;
        if (e >= len)
            break;
        i = e + 1;
    }
    *items = v;
    *n = cnt;
    return 0;
bad:
    props_items_free(v, cnt);
    return -1;
}
