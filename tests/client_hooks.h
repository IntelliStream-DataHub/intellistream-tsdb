/* The C client's test hooks: only in build/tsdb_client_hooks.o (client/tsdb_client.c compiled with
 * -DTSDBC_TEST_HOOKS), never in libtsdbclient.a or tsdb_client.h. */
#ifndef TSDB_CLIENT_HOOKS_H
#define TSDB_CLIENT_HOOKS_H

extern const char *tsdbc_test_nonce;    /* the client nonce of every later login when set */
extern unsigned tsdbc_test_pbkdf2_runs; /* PBKDF2 runs of the key cache so far */

#endif
