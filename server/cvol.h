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
/* 1 under TSDB_REPLICA_READS=1 (a test hook standing in for design-cluster.md 7.2's REPLICA_OK, not built): a replica
 * that holds a read lease answers reads from what it applied. Otherwise every read of a replicated volume goes to its
 * leader (D5): a replica, a deposed leader above all, may not have applied a write the new leader acknowledged. */
int cvol_replica_reads(void);
/* 1 when volume v is a cluster volume open and led here (whatever the lease says) */
int cvol_led(uint32_t v);
/* cvol_led, and the epoch it leads (the replication driver's; 0 for a writer the manager opened, which no driver
 * deposes): a leader deposed and promoted again leads a later epoch, so (1, epoch) names one leadership */
int cvol_lead(uint32_t v, uint64_t *epoch);
/* 1: volume v's record names another server as leader and no ISR member is up in the group's view (design-cluster.md
 * 6.2): UNAVAILABLE(NO_OWNER) until a member returns or an operator forces a leader, not MOVED to a dead server */
int cvol_no_owner(uint32_t v);
/* 1 once cvol_start ran: an engine's READONLY on a cluster volume becomes UNAVAILABLE (FENCED). */
int cvol_active(void);
/* 1 once the manager went through the volume records of a metadata copy: before that a volume name it does not know
 * may be one the copy has not shown it yet (UNAVAILABLE, RECOVERING, rather than NOTFOUND). */
int cvol_synced(void);
struct snap_ident;
/* The install of a received snapshot of replicated volume v (server/snapinst.c; the snapshot worker, no lock held; R4
 * D5, D6): takes the volume from the manager (requests UNAVAILABLE RECOVERING), stops its driver and closes its engine
 * when one is open, then calls fn(ctx, id, had_engine, log_end) with the identity REPL carries (the record's) and the
 * stopped driver's log end (repld_close_end; REPLD_END_UNKNOWN with no engine or no core), and hands the
 * volume back to be prepared and opened again as a new record would be (a new open generation, REPLICA_OPEN waited
 * for) whatever fn returned. CVOL_NOT_TAKEN without calling fn: not this manager's volume, being installed, or
 * stopping. */
#define CVOL_NOT_TAKEN (-1000)
int cvol_install(uint32_t v, int (*fn)(void *ctx, const struct snap_ident *id, int had_engine, uint64_t log_end),
                 void *ctx);

#endif
