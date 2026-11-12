/* SPDX-License-Identifier: Apache-2.0 */
/*
 * SHA-256 (FIPS 180-4), HMAC (RFC 2104), PBKDF2 (RFC 8018), base64 (RFC 4648)
 * for SCRAM-SHA-256; tests/test_crypto.c checks them against the published
 * vectors and the RFC 7677 exchange. Plain C, compiled once: every CPU level
 * of the tarball runs the same code, and none of it is on a hot path.
 */
#define _GNU_SOURCE
#include "tsdb_crypto.h"

#include <errno.h>
#include <stdbool.h>
#include <string.h>
#ifdef __APPLE__
#include <stdlib.h> /* arc4random_buf */
#endif
#include <sys/random.h>

static const uint32_t K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
};

static inline uint32_t ror(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

static void block(uint32_t h[8], const uint8_t *p)
{
    uint32_t w[64];
    for (int i = 0; i < 16; i++)
        w[i] = (uint32_t)p[4 * i] << 24 | (uint32_t)p[4 * i + 1] << 16 | (uint32_t)p[4 * i + 2] << 8 | p[4 * i + 3];
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = ror(w[i - 15], 7) ^ ror(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = ror(w[i - 2], 17) ^ ror(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], k = h[7];
    for (int i = 0; i < 64; i++) {
        uint32_t t1 = k + (ror(e, 6) ^ ror(e, 11) ^ ror(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + w[i];
        uint32_t t2 = (ror(a, 2) ^ ror(a, 13) ^ ror(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
        k = g;
        g = f;
        f = e;
        e = d + t1;
        d = c;
        c = b;
        b = a;
        a = t1 + t2;
    }
    h[0] += a, h[1] += b, h[2] += c, h[3] += d, h[4] += e, h[5] += f, h[6] += g, h[7] += k;
}

void tsdb_sha256_init(tsdb_sha256 *c)
{
    static const uint32_t iv[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                   0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    memcpy(c->h, iv, sizeof iv);
    c->bytes = 0;
    c->n = 0;
}

void tsdb_sha256_update(tsdb_sha256 *c, const void *data, size_t len)
{
    const uint8_t *p = data;
    if (!len)
        return; /* data may be NULL: no memcpy or arithmetic on it (undefined even for 0 bytes) */
    c->bytes += len;
    if (c->n) {
        size_t k = 64 - c->n < len ? 64 - c->n : len;
        memcpy(c->buf + c->n, p, k);
        c->n += k, p += k, len -= k;
        if (c->n < 64)
            return;
        block(c->h, c->buf);
        c->n = 0;
    }
    for (; len >= 64; p += 64, len -= 64)
        block(c->h, p);
    memcpy(c->buf, p, len);
    c->n = len;
}

void tsdb_sha256_final(tsdb_sha256 *c, uint8_t out[TSDB_SHA256_LEN])
{
    uint64_t bits = c->bytes * 8;
    c->buf[c->n++] = 0x80;
    if (c->n > 56) {
        memset(c->buf + c->n, 0, 64 - c->n);
        block(c->h, c->buf);
        c->n = 0;
    }
    memset(c->buf + c->n, 0, 56 - c->n);
    for (int i = 0; i < 8; i++)
        c->buf[56 + i] = (uint8_t)(bits >> (56 - 8 * i));
    block(c->h, c->buf);
    for (int i = 0; i < 8; i++)
        for (int j = 0; j < 4; j++)
            out[4 * i + j] = (uint8_t)(c->h[i] >> (24 - 8 * j));
    tsdb_wipe(c, sizeof *c);
}

void tsdb_sha256_buf(const void *data, size_t len, uint8_t out[TSDB_SHA256_LEN])
{
    tsdb_sha256 c;
    tsdb_sha256_init(&c);
    tsdb_sha256_update(&c, data, len);
    tsdb_sha256_final(&c, out);
}

/* HMAC with the key's pads hashed once: PBKDF2 calls it thousands of times with one key. */
typedef struct hmac_key {
    tsdb_sha256 inner, outer; /* states after the ipad and opad blocks */
} hmac_key;

static void hmac_init(hmac_key *k, const void *key, size_t key_len)
{
    uint8_t kb[64] = {0}, pad[64];
    if (key_len > 64)
        tsdb_sha256_buf(key, key_len, kb);
    else if (key_len)
        memcpy(kb, key, key_len);
    for (int i = 0; i < 64; i++)
        pad[i] = kb[i] ^ 0x36;
    tsdb_sha256_init(&k->inner);
    tsdb_sha256_update(&k->inner, pad, 64);
    for (int i = 0; i < 64; i++)
        pad[i] = kb[i] ^ 0x5c;
    tsdb_sha256_init(&k->outer);
    tsdb_sha256_update(&k->outer, pad, 64);
    tsdb_wipe(kb, sizeof kb);
    tsdb_wipe(pad, sizeof pad);
}

static void hmac_run(const hmac_key *k, const void *msg, size_t len, uint8_t out[TSDB_SHA256_LEN])
{
    tsdb_sha256 c = k->inner;
    tsdb_sha256_update(&c, msg, len);
    tsdb_sha256_final(&c, out);
    c = k->outer;
    tsdb_sha256_update(&c, out, TSDB_SHA256_LEN);
    tsdb_sha256_final(&c, out);
}

void tsdb_hmac_sha256(const void *key, size_t key_len, const void *msg, size_t msg_len,
                      uint8_t out[TSDB_SHA256_LEN])
{
    hmac_key k;
    hmac_init(&k, key, key_len);
    hmac_run(&k, msg, msg_len, out);
    tsdb_wipe(&k, sizeof k);
}

void tsdb_pbkdf2_sha256(const void *password, size_t password_len, const void *salt, size_t salt_len,
                        uint32_t iterations, uint8_t *out, size_t out_len)
{
    hmac_key k;
    hmac_init(&k, password, password_len);
    uint8_t u[TSDB_SHA256_LEN], t[TSDB_SHA256_LEN];
    for (uint32_t blk = 1; out_len; blk++) {
        uint8_t be[4] = {(uint8_t)(blk >> 24), (uint8_t)(blk >> 16), (uint8_t)(blk >> 8), (uint8_t)blk};
        tsdb_sha256 c = k.inner; /* U1 = HMAC(P, S || INT(i)) */
        tsdb_sha256_update(&c, salt, salt_len);
        tsdb_sha256_update(&c, be, 4);
        tsdb_sha256_final(&c, u);
        c = k.outer;
        tsdb_sha256_update(&c, u, sizeof u);
        tsdb_sha256_final(&c, u);
        memcpy(t, u, sizeof t);
        for (uint32_t i = 1; i < iterations; i++) {
            hmac_run(&k, u, sizeof u, u);
            for (size_t j = 0; j < sizeof t; j++)
                t[j] ^= u[j];
        }
        size_t n = out_len < sizeof t ? out_len : sizeof t;
        memcpy(out, t, n);
        out += n, out_len -= n;
    }
    tsdb_wipe(&k, sizeof k);
    tsdb_wipe(u, sizeof u);
    tsdb_wipe(t, sizeof t);
}

static const char B64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

size_t tsdb_base64_encode(const void *in, size_t len, char *out)
{
    const uint8_t *p = in;
    size_t o = 0;
    for (; len >= 3; p += 3, len -= 3) {
        uint32_t v = (uint32_t)p[0] << 16 | (uint32_t)p[1] << 8 | p[2];
        out[o++] = B64[v >> 18], out[o++] = B64[(v >> 12) & 63], out[o++] = B64[(v >> 6) & 63], out[o++] = B64[v & 63];
    }
    if (len) {
        uint32_t v = (uint32_t)p[0] << 16 | (len == 2 ? (uint32_t)p[1] << 8 : 0);
        out[o++] = B64[v >> 18], out[o++] = B64[(v >> 12) & 63];
        out[o++] = len == 2 ? B64[(v >> 6) & 63] : '=';
        out[o++] = '=';
    }
    out[o] = '\0';
    return o;
}

static int b64_val(unsigned char c)
{
    if (c >= 'A' && c <= 'Z')
        return c - 'A';
    if (c >= 'a' && c <= 'z')
        return c - 'a' + 26;
    if (c >= '0' && c <= '9')
        return c - '0' + 52;
    return c == '+' ? 62 : c == '/' ? 63 : -1;
}

int tsdb_base64_decode(const char *in, size_t len, uint8_t *out, size_t cap, size_t *out_len)
{
    if (len % 4)
        return -1;
    size_t o = 0;
    for (size_t i = 0; i < len; i += 4) {
        bool last = i + 4 == len;
        int pad = last ? (in[i + 3] == '=') + (in[i + 3] == '=' && in[i + 2] == '=') : 0;
        int v[4];
        for (int j = 0; j < 4 - pad; j++)
            if ((v[j] = b64_val((unsigned char)in[i + j])) < 0)
                return -1;
        size_t n = 3 - (size_t)pad;
        if (o + n > cap)
            return -1;
        uint32_t x = (uint32_t)v[0] << 18 | (uint32_t)v[1] << 12;
        if (pad < 2)
            x |= (uint32_t)v[2] << 6;
        if (pad < 1)
            x |= (uint32_t)v[3];
        if ((pad == 1 && (x & 0xff)) || (pad == 2 && (x & 0xffff)))
            return -1; /* bits after the data must be zero: one text per byte string */
        out[o++] = (uint8_t)(x >> 16);
        if (n > 1)
            out[o++] = (uint8_t)(x >> 8);
        if (n > 2)
            out[o++] = (uint8_t)x;
    }
    *out_len = o;
    return 0;
}

int tsdb_ct_equal(const void *a, const void *b, size_t n)
{
    const volatile uint8_t *x = a, *y = b;
    uint8_t d = 0;
    for (size_t i = 0; i < n; i++)
        d |= x[i] ^ y[i];
    return d == 0;
}

int tsdb_random(void *buf, size_t n)
{
#ifdef __APPLE__ /* no getrandom: arc4random_buf, from the kernel's generator, never blocks and never fails */
    arc4random_buf(buf, n);
#else
    uint8_t *p = buf;
    while (n) {
        ssize_t r = getrandom(p, n, 0);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return -1;
        p += r, n -= (size_t)r;
    }
#endif
    return 0;
}

/* through a volatile pointer: a memset of memory that is about to die may otherwise be dropped */
static void *(*volatile wipe_fn)(void *, int, size_t) = memset;

void tsdb_wipe(void *p, size_t n) { wipe_fn(p, 0, n); }

void tsdb_scram_keys(const void *password, size_t password_len, const void *salt, size_t salt_len,
                     uint32_t iterations, uint8_t client_key[TSDB_SHA256_LEN], uint8_t stored_key[TSDB_SHA256_LEN],
                     uint8_t server_key[TSDB_SHA256_LEN])
{
    uint8_t sp[TSDB_SHA256_LEN], ck[TSDB_SHA256_LEN];
    tsdb_pbkdf2_sha256(password, password_len, salt, salt_len, iterations, sp, sizeof sp);
    tsdb_hmac_sha256(sp, sizeof sp, "Client Key", 10, ck);
    if (client_key)
        memcpy(client_key, ck, sizeof ck);
    if (stored_key)
        tsdb_sha256_buf(ck, sizeof ck, stored_key);
    if (server_key)
        tsdb_hmac_sha256(sp, sizeof sp, "Server Key", 10, server_key);
    tsdb_wipe(sp, sizeof sp);
    tsdb_wipe(ck, sizeof ck);
}

int tsdb_read_password(FILE *f, char pw[TSDB_PASSWORD_MAX + 1])
{
    char line[TSDB_PASSWORD_MAX + 2]; /* the password, a '\r' before the '\n', and one byte to see a longer line */
    size_t n = 0;
    int ch;
    while ((ch = getc(f)) != EOF && ch != '\n')
        if (n < sizeof line)
            line[n++] = (char)ch; /* the rest of a long line is read and dropped */
        else
            n = sizeof line + 1;
    if (ferror(f))
        return tsdb_wipe(line, sizeof line), -1;
    if (ch == '\n' && n && n <= sizeof line && line[n - 1] == '\r')
        n--; /* "\r\n": only before a '\n' is a '\r' a line end */
    int rc = 0;
    if (n == 0 || n > TSDB_PASSWORD_MAX || memchr(line, '\0', n))
        rc = -2;
    else {
        memcpy(pw, line, n);
        pw[n] = '\0';
    }
    tsdb_wipe(line, sizeof line);
    return rc;
}
