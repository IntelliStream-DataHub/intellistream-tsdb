/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The settle thread of a tier and the settle policy (R7, r7-settle; D11: nice 19, one settle at a time, beside the
 * merge threads, which merge_threads = 0 does not stop). r7-iface left the hooks of src/tier.h as stubs that do
 * nothing; r7-settle fills them (and src/ecpolicy.c).
 */
#ifndef TSDB_SETTLE_H
#define TSDB_SETTLE_H
#include "internal.h"
struct tier;
/* t->settle: made by ecsettle_tier_open (tier_open), stopped by ecsettle_tier_quiesce (tsdb_close, before the volumes
 * close: no settle runs from there on) and freed by ecsettle_tier_close (the last tier_put; also after a failed or
 * missing open). */
int ecsettle_tier_open(struct tier *t);
void ecsettle_tier_quiesce(struct tier *t);
void ecsettle_tier_close(struct tier *t);
/* db->ecstate->settle (tier_engine_attach, tier_engine_detach) */
int ecsettle_eng_open(eng *db);
void ecsettle_eng_close(eng *db);
#endif
