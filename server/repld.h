/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's replication driver (docs/design-cluster.md 5.1-5.4, C5, C25; r3d-stream): one `repl` core (src/repl.h) per
 * cluster volume this server holds, carried out against the volume's engine (src/replica.h on a replica, the leader's
 * jr_ops of src/internal.h on the writer) and the data links (server/peerlink.h, payloads in server/replwire.h).
 * See server/repld.c.
 */
#ifndef TSDBD_REPLD_H
#define TSDBD_REPLD_H

#include <stdint.h>

#include "tsdb.h"
#include "cluster.h"

/* The driver for the handle `t` whose root is `dir` (cluster mode). Call once, before the first repld_open. */
void repld_init(tsdb *t, const char *dir);
/* Opens the volume of record `r` from its directory (<dir>/v<N>, made and its generation recorded by server/cvol.c) as a
 * replica with the REPL file's commit floor, and starts its driver; the record's leader then leads it (an EPOCH marker,
 * the commit point past it, the promotion to the writer). 0, or a TSDB_ERR_* (nothing started). */
int repld_open(const struct cluster_vol_rec *r);
/* A newer metadata record of a volume repld_open started (the ISR, the epoch, the members' addresses). */
void repld_update(const struct cluster_vol_rec *r);
/* 1 while volume v is held here and failed (FAIL, a failed save, the engine's fail-stop): it is quiesced and reported
 * failed, the process goes on (DECISION D13). */
int repld_failed(uint32_t v);
/* Volume v's engine failed (eng_fail): 1 when the driver holds v (it stops and quiesces it, the process goes on, D13),
 * 0 when v is no volume of the driver (tsdbd's own rules apply). */
int repld_engine_failed(uint32_t v, const char *msg);
/* Stops every driver thread and the links (before the handle is closed). */
void repld_stop(void);
/* After repld_stop: closes the engines of the volumes the driver stopped on a failure (D13), whose close fails by
 * design; tsdb_close then reports the other volumes' errors only. */
void repld_close_failed(void);

#endif
