/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tenants in tsdbd (docs/design-stats.md section 4, ST5, ST6, ST16): the list of tenant prefixes (a file, or the
 * prefixes that tenant admins hold `admin` on), the tenant of a user, and the per-tenant request counters that
 * STATS_TENANTS reports as its by-caller block.
 *
 * The list reaches the engine through tsdb_options at open and tsdb_set_tenants later, only when the set of prefixes
 * changed and stayed the same for the settle time (30 s). Per-tenant numbers are never put on the metrics port.
 */
#ifndef TSDBD_TENANTS_H
#define TSDBD_TENANTS_H

#include "tsdb.h"
#include "users.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* The by-caller counters, in the order of the wire block (design-stats.md 5.1). */
enum {
    TN_REQ_QUERY, TN_REQ_INSERT, TN_REQ_DELETE, TN_REQ_ADMIN, TN_REQ_OTHER,
    TN_ROWS_DECODED, TN_ROWS_STATS, TN_ROWS_MEMTABLE, TN_BYTES_READ, TN_RESULT_ROWS, TN_QUERY_US,
    TN_INSERTED_ROWS, TN_INSERTED_BYTES,
    TN_ERR_INVALID, TN_ERR_NOTFOUND, TN_ERR_IO, TN_ERR_OTHER,
    TN_DENIED_REQUESTS, TN_DENIED_SERIES, TN_FAILED_LOGINS,
    TN_NC
};

/* Counts n on the calling thread's block under tenant `slot` (0: "(none)"): a relaxed store, no read-modify-write,
 * no lock; an allocation that fails means "not counted", never an error. */
void tn_count(uint16_t slot, int counter, uint64_t n);
/* The sums over every thread (those that have ended too) for the tenant with this slot. */
void tn_read(uint16_t slot, uint64_t out[TN_NC]);

/* The slot of a prefix of the list (0: "(none)", the empty prefix); slots are never reused within a process.
 * tn_slot_find returns -1 for a prefix that never had one. */
int tn_slot_find(const char *prefix, size_t len);
/* The tenant of a series name: the longest prefix of the current list that it starts with, else 0. */
uint16_t tn_slot_name(const char *name, size_t len);
/* The tenant of a user (ST6), with the list generation it was computed under in *gen: the longest prefix of the list that the user's name starts with and that holds every
 * one of its grants (prefix: and name: under it, glob: with a literal head under it); a superuser and anyone else
 * is 0. */
uint16_t tn_slot_user(const user *u, uint32_t *gen);
/* The list's generation: one more at every applied change; auth.c rebuilds user states made under another. */
uint32_t tn_gen(void);

/* ---- the list's sources ---- */

/* Reads a tenants file (one prefix per line, # comments, blank lines skipped) into a malloc'd array. 0, or a
 * negative TSDB_ERR_* after a message on stderr. */
int tn_read_file(const char *path, char ***out, size_t *n);
/* The `prefix:` selectors of every `admin` grant in users (ST5, decision 1). */
int tn_from_users(const users *us, char ***out, size_t *n);
void tn_free_list(char **v, size_t n);

/* Before tsdb_open: the list the engine opens with (the file, else USERS read in dir when login is on), and the
 * server's own copy. file may be NULL. *out is for tsdb_options.tenants; free it with tn_free_list after the open. */
int tn_init(const char *dir, const char *file, bool auth_on, int settle_s, char ***out, size_t *n);
/* After tsdb_open: the engine the later lists go to, and the thread that applies them (a tick of 100 ms: SIGHUP's
 * reload of the file, a set that stayed the same for the settle time). */
int tn_start(tsdb *db);
void tn_stop(void);
/* A new set of prefixes from the default source (USERS): ignored when a file names the tenants. Copies. */
void tn_offer(char *const *prefixes, size_t n);
/* SIGHUP: the file is read again at the next tick (async-signal-safe). */
void tn_hup(void);
/* Test hook (tests/test_tn_race.c): called by the tick thread after the engine took a list, before cur changes. */
extern void (*tn_test_after_set)(void);

#endif
