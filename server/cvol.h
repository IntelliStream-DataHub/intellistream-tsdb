/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's cluster volume manager (docs/design-cluster.md §3.1, §8.5; r3c-volumes): makes and opens the v<N>
 * directories of the replicated volumes the metadata group names this server for. See server/cvol.c.
 */
#ifndef TSDBD_CVOL_H
#define TSDBD_CVOL_H

#include <stdint.h>

#include "tsdb.h"
#include "cluster.h"

/* Starts the manager thread for the handle `t` (already in cluster mode, tsdb_cvol_mode) whose root is `dir`, and
 * hooks it into the cluster thread (cluster_set_volume_watch, cluster_set_reporter): call before cluster_start.
 * 0, or -1 with a line on stderr. */
int cvol_start(tsdb *t, const char *dir);
void cvol_stop(void);
/* The reason (TSDB_WHY_*) a request for cluster volume `v` is refused, for UNAVAILABLE's body: FENCED on a replica
 * (and on a leader whose writes are not switched on), RECOVERING while the volume is not open yet, UNREACHABLE on the
 * leader while it holds no read lease (cluster_lease_valid: no Raft leader's answer extended it). -1: v is no
 * cluster volume of this server or nothing is wrong with it. */
int cvol_why(uint32_t v);
/* 1 when volume v is a cluster volume open and led here (whatever the lease says) */
int cvol_led(uint32_t v);
/* 1 once cvol_start ran: an engine's READONLY on a cluster volume becomes UNAVAILABLE (FENCED). */
int cvol_active(void);
/* 1 once the manager went through the volume records of a metadata copy: before that a volume name it does not know
 * may be one the copy has not shown it yet (UNAVAILABLE, RECOVERING, rather than NOTFOUND). */
int cvol_synced(void);

#endif
