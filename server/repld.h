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
/* Opens the volume of record `r` from its directory (<disk_root>/v<N>, or <dir>/v<N> when disk_root is NULL/empty;
 * made and its generation recorded by server/cvol.c) as a replica with the REPL file's commit floor, and starts its
 * driver; the record's leader then leads it (an EPOCH marker, the commit point past it, the promotion to the writer).
 * 0, or a TSDB_ERR_* (nothing started). */
int repld_open(const struct cluster_vol_rec *r, const char *disk_root);
/* A newer metadata record of a volume repld_open started (the ISR, the epoch, the members' addresses). */
void repld_update(const struct cluster_vol_rec *r);
/* 1 while volume v is held here and failed (FAIL, a failed save, the engine's fail-stop): it is quiesced and reported
 * failed, the process goes on (DECISION D13). */
int repld_failed(uint32_t v);
/* Whether this node writes volume v: 1 while its driver is the writer and its core still leads (epoch: the core's),
 * 0 when it drives v otherwise (a replica, promoting, deposed, failed), -1 when no driver drives v here. Reads and
 * writes are served, and OWNER reported in the heartbeat, only on 1: a deposed writer stops at once, before its engine
 * is reopened as a replica (design-cluster.md 6.3, 7.1). */
int repld_leading(uint32_t v, uint64_t *epoch);
/* (repld_leading: 1 the writer, 2 promoting to *epoch, 0 neither, -1 no such volume) */
/* Volume v's engine failed (eng_fail): 1 when the driver holds v (it stops and quiesces it, the process goes on, D13),
 * 0 when v is no volume of the driver (tsdbd's own rules apply). */
int repld_engine_failed(uint32_t v, const char *msg);
/* R13 (r13-fault, E57, D16): volume v's fast disk went DOWN (probe stall or errors): stop the driver, depose any
 * leader immediately without waiting for stuck calls, quiesce the engine, and schedule a heartbeat (LOCAL_FAULT).
 * Returns 1 when a driver held v, 0 otherwise. */
int repld_disk_down(uint32_t v, const char *why);
/* 1 while volume v is led here with fewer in-sync replicas than its min_isr: writes are refused (REPL_E_ISR). */
int repld_isr_short(uint32_t v);
/* 1 while volume v is led here and this server or any current ISR member is FULL (R9 E30, D17): writes are refused
 * at admission with UNAVAILABLE TSDB_WHY_FULL (retry_after 1000 ms). */
int repld_full(uint32_t v);
/* Records a FULL refusal for volume v (per-volume and process-total atomic counters, for STATUS and metrics). */
void repld_note_full_refusal(uint32_t v);
/* Refusal count for volume v (UINT32_MAX or TSDB_VOL_ANY: process-wide sum across volumes). Exported for STATUS
 * and r14-report's Prometheus metrics without touching server/metrics.c here. */
uint64_t repld_full_refusals(uint32_t v);
/* 1 only when the planted mutant fullafteradmit is active (tests only; always 0 when TSDB_TEST_HOOKS is off). */
int repld_full_after_admit(void);
/* 1 while volume v is led here and a planned transfer refuses its writes (rtransfer.c): UNAVAILABLE PROMOTING, a
 * retry after about 50 ms, definite (never admitted). */
int repld_draining(uint32_t v);
/* the volume leaders' side of STATUS's transfer counters (cluster_set_drv_counts): out[CL_TR_DRAIN_TIMEDOUT ..
 * CL_TR_CUTS]; the rest left alone */
void repld_transfer_counts(uint64_t out[CL_TR_N]);
/* A FENCE(v, fence) from the Raft leader `from` (the cluster thread's handler, cluster_set_fence_handler): handed to the
 * volume's core, whose reply goes back through cluster_vol_msg once the fence is saved. No such volume: ignored. */
void repld_fence(void *arg, uint32_t v, uint64_t fence, uint32_t from);
/* A VOLCMD from the Raft leader `from` (cluster_set_volcmd_handler): queued for the volume's thread (IN_CMD), which
 * hands TRANSFER and CANCEL to rtransfer.c, DROP_MEMBER to rset.c. Any thread. */
void repld_volcmd(void *arg, const struct cluster_volcmd *c, uint32_t from);
/* The ISR changes the volumes led here ask the metadata group for (cluster_report.isr); at most max. */
unsigned repld_isr_requests(struct cluster_isr_req *out, unsigned max);
/* The servers this one shares a running volume with and has heard nothing from on any of them for after_ms or more
 * (the heartbeat's "unreachable" section, design-cluster.md §2.2): at most max into out; any thread. */
unsigned repld_unreachable(struct cluster_unreach *out, unsigned max, uint32_t after_ms);
/* Stops every driver thread and the links (before the handle is closed). */
void repld_stop(void);
/* Wakes volume v's replication driver thread (e.g. when scrubd marks young damage). Any thread; no-op if absent. */
void repld_wake_vol(uint32_t v);
/* Closes one volume's driver and engine (a replica the record no longer names, or one reset: server/cvolset_mgr.c): its
 * thread stopped and joined, a writer deposed, the engine closed, the driver unlisted and freed once the last holder of
 * a vol_find reference (a link thread, the cluster thread, a worker, the snapshot worker) gives it back. 0 (also when
 * none was open), the manager's thread. */
int repld_close(uint32_t v);
/* repld_close, and *log_end the replica's durable log end once its driver was joined (nothing appends after it): 0 for a
 * DIVERGED replica that asked for its rebuild, REPLD_END_UNKNOWN when no driver or no core was there (cvol_install) */
#define REPLD_END_UNKNOWN UINT64_MAX
int repld_close_end(uint32_t v, uint64_t *log_end);
/* After repld_stop: closes the engines of the volumes the driver stopped on a failure (D13), whose close fails by
 * design; tsdb_close then reports the other volumes' errors only. */
void repld_close_failed(void);

/* snapshot transfers of this server since its start (server/rsnap.c; STATUS snap.*, metrics tsdb_snapshot_*) */
struct rsnap_stats {
    uint64_t asked;          /* requests this server sent as a receiver */
    uint64_t sent, received; /* whole snapshots */
    uint64_t refused_space;  /* refused for the free space of the receiver (D5) */
    uint64_t failed;         /* abandoned or released before the end, either side */
    uint64_t busy;           /* BUSY answers, either side */
    uint64_t bytes_sent, bytes_received;
    uint64_t last_ms;        /* the last snapshot received: ms from its request to its last file */
    uint64_t installed;      /* snapshots installed (server/snapinst.c) */
    uint64_t refused_stale;  /* received and not installed: P at or below the log end, or fenced above its epoch */
    uint64_t install_ms;     /* the last install: ms from the end of the transfer to the volume handed back */
    uint64_t kind5_bytes, kind2_bytes; /* bytes the snapshots' links (kind 5) and the COMMIT files' (kind 2) took
                                        * from their buckets (peerlink_kind_bytes) */
};
void rsnap_get_stats(struct rsnap_stats *out);

/* Active snapshot transfer progress per volume (server/rsnap.c; HBS_PROGRESS, R14). */
struct rsnap_progress {
    uint32_t volume;
    uint8_t role; /* 1 = source (sending), 2 = receiver (receiving) */
    uint64_t bytes_done;
    uint64_t bytes_total;
    uint64_t started_ms;       /* Unix epoch ms */
    uint64_t last_progress_ms; /* Unix epoch ms */
};
/* Lock-safe getter (takes S.mu briefly, no I/O). Returns entries written (<= cap). */
uint32_t rsnap_get_progress(struct rsnap_progress *out, uint32_t cap);

#endif
