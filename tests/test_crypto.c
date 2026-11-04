/*
 * The login cryptography (client/tsdb_crypto.c) against published vectors:
 * SHA-256 (FIPS 180-4 examples), HMAC-SHA-256 (RFC 4231 cases 1 to 7),
 * PBKDF2-HMAC-SHA-256 (RFC 7914 §11), base64 (RFC 4648 §10), and the SCRAM
 * exchange of RFC 7677 §3 computed from the primitives, byte for byte.
 *
 *   ./build/test_crypto
 */
#include "tsdb_crypto.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CHECK(cond)                                                                                                \
    do {                                                                                                           \
        if (!(cond)) {                                                                                             \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                              \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)

static bool hex_is(const uint8_t *b, size_t n, const char *hex)
{
    char s[2 * 256 + 1];
    for (size_t i = 0; i < n; i++)
        snprintf(s + 2 * i, 3, "%02x", b[i]);
    if (strcmp(s, hex) != 0) {
        fprintf(stderr, "  got  %s\n  want %s\n", s, hex);
        return false;
    }
    return true;
}

static void sha256(void)
{
    uint8_t h[32];
    tsdb_sha256_buf("", 0, h);
    CHECK(hex_is(h, 32, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    tsdb_sha256_buf("abc", 3, h);
    CHECK(hex_is(h, 32, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    const char *m448 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    tsdb_sha256_buf(m448, strlen(m448), h);
    CHECK(hex_is(h, 32, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"));
    /* a million 'a', fed in uneven pieces so that every path of update (partial buffer, whole blocks) runs */
    char a[1000];
    memset(a, 'a', sizeof a);
    tsdb_sha256 c;
    tsdb_sha256_init(&c);
    size_t left = 1000000, step = 1;
    while (left) {
        size_t n = step < left ? step : left;
        tsdb_sha256_update(&c, a, n);
        left -= n;
        step = step % 997 + 1;
    }
    tsdb_sha256_final(&c, h);
    CHECK(hex_is(h, 32, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"));
    /* the padding edges: 55, 56 and 64 bytes (one or two final blocks) agree with feeding byte by byte */
    for (size_t n = 50; n <= 130; n++) {
        uint8_t one[32], each[32];
        tsdb_sha256_buf(a, n, one);
        tsdb_sha256_init(&c);
        for (size_t i = 0; i < n; i++)
            tsdb_sha256_update(&c, a, 1);
        tsdb_sha256_final(&c, each);
        CHECK(memcmp(one, each, 32) == 0);
    }
    printf("  SHA-256: FIPS 180-4 examples, a million 'a' in pieces\n");
}

static void hmac(void)
{
    static const struct {
        size_t klen;
        uint8_t kbyte; /* 0: key below */
        const char *key, *data;
        size_t dlen;
        uint8_t dbyte;
        const char *hex;
    } v[] = {
        {20, 0x0b, NULL, "Hi There", 0, 0, "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"},
        {4, 0, "Jefe", "what do ya want for nothing?", 0, 0,
         "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"},
        {20, 0xaa, NULL, NULL, 50, 0xdd, "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"},
        {25, 0, NULL, NULL, 50, 0xcd, "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"},
        {20, 0x0c, NULL, "Test With Truncation", 0, 0, "a3b6167473100ee06e0c796c2955552b"},
        {131, 0xaa, NULL, "Test Using Larger Than Block-Size Key - Hash Key First", 0, 0,
         "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"},
        {131, 0xaa, NULL,
         "This is a test using a larger than block-size key and a larger than block-size data. The key needs to be "
         "hashed before being used by the HMAC algorithm.",
         0, 0, "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2"},
    };
    for (size_t i = 0; i < sizeof v / sizeof v[0]; i++) {
        uint8_t key[131], data[64], out[32];
        if (v[i].key)
            memcpy(key, v[i].key, v[i].klen);
        else if (v[i].kbyte)
            memset(key, v[i].kbyte, v[i].klen);
        else
            for (size_t j = 0; j < v[i].klen; j++)
                key[j] = (uint8_t)(j + 1); /* case 4: 0x01 .. 0x19 */
        const void *d = v[i].data;
        size_t dlen = d ? strlen(v[i].data) : v[i].dlen;
        if (!d) {
            memset(data, v[i].dbyte, dlen);
            d = data;
        }
        tsdb_hmac_sha256(key, v[i].klen, d, dlen, out);
        size_t hlen = strlen(v[i].hex) / 2; /* case 5 is truncated to 128 bits */
        if (!hex_is(out, hlen, v[i].hex)) {
            fprintf(stderr, "  RFC 4231 case %zu\n", i + 1);
            failures++;
        }
    }
    printf("  HMAC-SHA-256: RFC 4231 cases 1 to 7\n");
}

static void pbkdf2(void)
{
    uint8_t out[64];
    tsdb_pbkdf2_sha256("passwd", 6, "salt", 4, 1, out, 64);
    CHECK(hex_is(out, 64, "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc49ca9cccf179b645991664b39d77ef317"
                          "c71b845b1e30bd509112041d3a19783"));
    tsdb_pbkdf2_sha256("Password", 8, "NaCl", 4, 80000, out, 64);
    CHECK(hex_is(out, 64, "4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56a1d425a1225833549adb841b51c9b31"
                          "76a272bdebba1d078478f62b397f33c8d"));
    /* a shorter output is the prefix of the longer one */
    uint8_t short_out[20];
    tsdb_pbkdf2_sha256("Password", 8, "NaCl", 4, 3, out, 64);
    tsdb_pbkdf2_sha256("Password", 8, "NaCl", 4, 3, short_out, 20);
    CHECK(memcmp(out, short_out, 20) == 0);
    printf("  PBKDF2-HMAC-SHA-256: RFC 7914 §11\n");
}

static void base64(void)
{
    static const char *plain[] = {"", "f", "fo", "foo", "foob", "fooba", "foobar"};
    static const char *enc[] = {"", "Zg==", "Zm8=", "Zm9v", "Zm9vYg==", "Zm9vYmE=", "Zm9vYmFy"};
    for (int i = 0; i < 7; i++) {
        char s[16];
        uint8_t b[16];
        size_t n = 99;
        CHECK(tsdb_base64_encode(plain[i], strlen(plain[i]), s) == strlen(enc[i]) && strcmp(s, enc[i]) == 0);
        CHECK(tsdb_base64_decode(enc[i], strlen(enc[i]), b, sizeof b, &n) == 0 && n == strlen(plain[i]) &&
              memcmp(b, plain[i], n) == 0);
        /* too small an output is refused, not overrun */
        if (n)
            CHECK(tsdb_base64_decode(enc[i], strlen(enc[i]), b, n - 1, &n) == -1);
    }
    /* strict: no missing padding, no whitespace, no bits left over, no '=' inside, no other alphabet */
    static const char *bad[] = {"Zg", "Zg=", "Zm9", "Zg=a", "Z===", "====", "Zh==", "Zm9=", "Zm 9v", "Zm9v\n",
                                "Zg==Zg==", "Zm-v", "Zm_v", "Zm9v====", "=m9v"};
    for (size_t i = 0; i < sizeof bad / sizeof bad[0]; i++) {
        uint8_t b[16];
        size_t n;
        if (tsdb_base64_decode(bad[i], strlen(bad[i]), b, sizeof b, &n) != -1) {
            fprintf(stderr, "  base64 accepted \"%s\"\n", bad[i]);
            failures++;
        }
    }
    /* every byte value round-trips at every length mod 3 */
    uint8_t all[256], back[256];
    char s[tsdb_base64_len(256) + 1];
    for (int i = 0; i < 256; i++)
        all[i] = (uint8_t)i;
    for (size_t len = 253; len <= 256; len++) {
        size_t n;
        tsdb_base64_encode(all, len, s);
        CHECK(tsdb_base64_decode(s, strlen(s), back, sizeof back, &n) == 0 && n == len && memcmp(all, back, n) == 0);
    }
    printf("  base64: RFC 4648 §10, strict decoding\n");
}

static void misc(void)
{
    uint8_t a[32] = {0}, b[32] = {0};
    CHECK(tsdb_ct_equal(a, b, 32));
    for (int i = 0; i < 32; i++) {
        b[i] = 0x80;
        CHECK(!tsdb_ct_equal(a, b, 32));
        b[i] = 0;
    }
    uint8_t r1[18], r2[18];
    CHECK(tsdb_random(r1, sizeof r1) == 0 && tsdb_random(r2, sizeof r2) == 0 && memcmp(r1, r2, sizeof r1) != 0);
    tsdb_wipe(r1, sizeof r1);
    CHECK(tsdb_ct_equal(r1, a, sizeof r1));
    printf("  constant-time compare, getrandom, wipe\n");
}

/* RFC 7677 §3: user "user", password "pencil", the nonces fixed. Every message is built here from the primitives
 * as tsdbd and the C client will, and must equal the RFC's bytes. */
static void scram_rfc7677(void)
{
    const char *cnonce = "rOprNGfwEbeRWgbNEkqO", *nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const char *salt_b64 = "W22ZaJ0SNY7soEsUEjb6gQ==";
    uint8_t salt[64];
    size_t salt_len;
    CHECK(tsdb_base64_decode(salt_b64, strlen(salt_b64), salt, sizeof salt, &salt_len) == 0 && salt_len == 16);

    /* the server's verifier, as `tsdb user add` computes it */
    uint8_t client_key[32], stored_key[32], server_key[32];
    tsdb_scram_keys("pencil", 6, salt, salt_len, 4096, client_key, stored_key, server_key);

    char client_first_bare[128], server_first[256], client_final_wo_proof[256], auth[640];
    snprintf(client_first_bare, sizeof client_first_bare, "n=user,r=%s", cnonce);
    char client_first[160];
    snprintf(client_first, sizeof client_first, "n,,%s", client_first_bare);
    CHECK(strcmp(client_first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO") == 0);
    snprintf(server_first, sizeof server_first, "r=%s,s=%s,i=%d", nonce, salt_b64, 4096);
    CHECK(strcmp(server_first,
                 "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096") == 0);
    char gs2[8];
    tsdb_base64_encode("n,,", 3, gs2);
    CHECK(strcmp(gs2, "biws") == 0);
    snprintf(client_final_wo_proof, sizeof client_final_wo_proof, "c=%s,r=%s", gs2, nonce);
    snprintf(auth, sizeof auth, "%s,%s,%s", client_first_bare, server_first, client_final_wo_proof);

    uint8_t client_sig[32], proof[32], server_sig[32];
    tsdb_hmac_sha256(stored_key, 32, auth, strlen(auth), client_sig);
    for (int i = 0; i < 32; i++)
        proof[i] = client_key[i] ^ client_sig[i];
    char proof_b64[tsdb_base64_len(32) + 1], client_final[320];
    tsdb_base64_encode(proof, 32, proof_b64);
    snprintf(client_final, sizeof client_final, "%s,p=%s", client_final_wo_proof, proof_b64);
    CHECK(strcmp(client_final, "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
                               "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=") == 0);

    /* the server's check: StoredKey = H(ClientProof XOR ClientSignature), compared in constant time */
    uint8_t recovered[32], h[32];
    for (int i = 0; i < 32; i++)
        recovered[i] = proof[i] ^ client_sig[i];
    tsdb_sha256_buf(recovered, 32, h);
    CHECK(tsdb_ct_equal(h, stored_key, 32));
    proof[7] ^= 1; /* a wrong proof fails */
    for (int i = 0; i < 32; i++)
        recovered[i] = proof[i] ^ client_sig[i];
    tsdb_sha256_buf(recovered, 32, h);
    CHECK(!tsdb_ct_equal(h, stored_key, 32));

    tsdb_hmac_sha256(server_key, 32, auth, strlen(auth), server_sig);
    char server_final[64];
    char sig_b64[tsdb_base64_len(32) + 1];
    tsdb_base64_encode(server_sig, 32, sig_b64);
    snprintf(server_final, sizeof server_final, "v=%s", sig_b64);
    CHECK(strcmp(server_final, "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=") == 0);
    /* and the keys the server stores (the same computed by Python's hashlib, for the other clients' tests) */
    CHECK(hex_is(stored_key, 32, "586e5df283e6dceb5c3e791d8b8528ec191e664045ce971792e2e6b5bb13e2a6"));
    CHECK(hex_is(server_key, 32, "c1f3cbc1c13a9d35a14c0990eed97629ea225863e566a4314ab99f3f00e5d9d5"));
    printf("  SCRAM-SHA-256: the RFC 7677 §3 exchange byte for byte\n");
}

int main(void)
{
    printf("test_crypto\n");
    sha256();
    hmac();
    pbkdf2();
    base64();
    misc();
    scram_rfc7677();
    if (failures) {
        fprintf(stderr, "test_crypto: %d failures\n", failures);
        return 1;
    }
    printf("test_crypto: ok\n");
    return 0;
}
