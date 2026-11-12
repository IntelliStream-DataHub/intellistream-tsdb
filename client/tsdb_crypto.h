/* SPDX-License-Identifier: Apache-2.0 */
/*
 * The cryptography of login (docs/design-auth.md A19): SHA-256, HMAC-SHA-256,
 * PBKDF2-HMAC-SHA-256, base64 and a constant-time compare, written out so that
 * tsdbd, the C client and the CLI need no crypto library (libcrypto's soname
 * differs between distributions, which the portable tarball cannot follow).
 * Nothing here is fast on purpose: a login runs PBKDF2 once per process on the
 * client and a few HMACs on the server.
 */
#ifndef TSDB_CRYPTO_H
#define TSDB_CRYPTO_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#define TSDB_SHA256_LEN 32

/* The password rule of every client and the CLI (design-auth.md §6): 1 to TSDB_PASSWORD_MAX bytes, no NUL; a
 * password file holds it in its first line, ended by "\n" or "\r\n" (or the end of the file). */
#define TSDB_PASSWORD_MAX 1023

/* The password in the first line of f: 0; -1 (errno set) when f cannot be read; -2 when that line is empty, longer
 * than TSDB_PASSWORD_MAX or holds a NUL. */
int tsdb_read_password(FILE *f, char pw[TSDB_PASSWORD_MAX + 1]);

typedef struct tsdb_sha256 {
    uint32_t h[8];
    uint64_t bytes; /* hashed so far */
    uint8_t buf[64];
    size_t n; /* bytes in buf */
} tsdb_sha256;

void tsdb_sha256_init(tsdb_sha256 *c);
void tsdb_sha256_update(tsdb_sha256 *c, const void *data, size_t len);
void tsdb_sha256_final(tsdb_sha256 *c, uint8_t out[TSDB_SHA256_LEN]);
void tsdb_sha256_buf(const void *data, size_t len, uint8_t out[TSDB_SHA256_LEN]);

void tsdb_hmac_sha256(const void *key, size_t key_len, const void *msg, size_t msg_len,
                      uint8_t out[TSDB_SHA256_LEN]);

/* RFC 8018 PBKDF2 with HMAC-SHA-256; out_len bytes (any length); iterations >= 1. */
void tsdb_pbkdf2_sha256(const void *password, size_t password_len, const void *salt, size_t salt_len,
                        uint32_t iterations, uint8_t *out, size_t out_len);

/* RFC 4648 base64 with padding. encode writes tsdb_base64_len(n) chars and a NUL. decode is strict (padding
 * required, no whitespace, unused bits zero): 0 and *out_len, or -1 for anything else or more than cap bytes. */
#define tsdb_base64_len(n) (((n) + 2) / 3 * 4)
size_t tsdb_base64_encode(const void *in, size_t len, char *out);
int tsdb_base64_decode(const char *in, size_t len, uint8_t *out, size_t cap, size_t *out_len);

/* 1 when the n bytes are equal; the time depends on n only. */
int tsdb_ct_equal(const void *a, const void *b, size_t n);

/* n bytes from getrandom(2), waiting for the pool at boot: 0, or -1 (errno set). */
int tsdb_random(void *buf, size_t n);

/* Clears a secret in a way the compiler may not drop. */
void tsdb_wipe(void *p, size_t n);

/* The SCRAM-SHA-256 keys of a password (RFC 5802 §3): SaltedPassword = PBKDF2(password, salt, i), ClientKey =
 * HMAC(SaltedPassword, "Client Key"), StoredKey = SHA-256(ClientKey), ServerKey = HMAC(SaltedPassword, "Server
 * Key"). Any output may be NULL. A server keeps salt, i, StoredKey and ServerKey; a client keeps ClientKey and
 * ServerKey to log in again without PBKDF2. */
void tsdb_scram_keys(const void *password, size_t password_len, const void *salt, size_t salt_len,
                     uint32_t iterations, uint8_t client_key[TSDB_SHA256_LEN], uint8_t stored_key[TSDB_SHA256_LEN],
                     uint8_t server_key[TSDB_SHA256_LEN]);

#endif
