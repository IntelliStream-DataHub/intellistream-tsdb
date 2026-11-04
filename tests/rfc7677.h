/* The SCRAM-SHA-256 exchange of RFC 7677 §3, copied from the RFC text: user "user", password "pencil", both nonces
 * fixed. tests/test_crypto.c builds it from the primitives; tests/test_auth.c runs it through the C client against a
 * fake server and through tsdbd's server side (server/auth.c), byte for byte. */
#ifndef TSDB_TEST_RFC7677_H
#define TSDB_TEST_RFC7677_H

#define RFC7677_USER "user"
#define RFC7677_PASSWORD "pencil"
#define RFC7677_CNONCE "rOprNGfwEbeRWgbNEkqO"
#define RFC7677_SNONCE "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0" /* the server's part of the nonce */
#define RFC7677_NONCE RFC7677_CNONCE RFC7677_SNONCE
#define RFC7677_SALT_B64 "W22ZaJ0SNY7soEsUEjb6gQ=="
#define RFC7677_ITER 4096
#define RFC7677_CLIENT_FIRST "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
#define RFC7677_SERVER_FIRST "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
#define RFC7677_CLIENT_FINAL                                                                                       \
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
#define RFC7677_SERVER_FINAL "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="

#endif
