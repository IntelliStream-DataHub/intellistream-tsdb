/*
 * Copyright 2026 IntelliStream AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 *
 * Copied from intellistream-objects src/raft (src/raft/UPSTREAM names the commit) by tools/sync-raft.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
/* The Raft log on disk (plan section 6): append-only segments plus a two-slot hard-state file.
 *
 * `raft/log-<first_index>` files of about 64 MiB, each a 64-byte header followed by records
 * `{crc32c, len, index, term, type, payload}`. Opening scans every record to rebuild the index
 * in memory; a bad record at the very end of the last segment is a torn write and is cut off,
 * a bad record anywhere else is corruption and a refusal to start that names the file, the
 * offset and the remedy. So is a bad record at or below the durable floor the hard state names
 * (raft_log_durable below; plan 28h, P1m-15), even in the last segment: what the replica synced and
 * acknowledged is never cut as a tear. Truncating a conflicting suffix is an ftruncate at the record's offset
 * (later segments are deleted); compaction deletes whole segments below the snapshot; a reset
 * after a snapshot install starts a fresh segment on the image's index and term.
 *
 * The hard state `{term, vote}`, a reseed's vote fence and the durable floor live in `raft/hard`:
 * two 4 KiB slots written alternately with a sequence number and a checksum, fdatasync'ed before
 * the call returns (a floor's slot: before its sync round reports), so a crash in the middle of a
 * write leaves the previous slot intact. What the log sits on (its
 * base index and term) is the log's own business and is not repeated here.
 *
 * Durability is split so a slow disk stays out of the event loop: raft_log_sync_prepare() hands
 * out duplicated descriptors of every segment touched since the last call, and
 * raft_log_sync_run() does the fdatasync wherever the caller likes (the daemon's compute pool),
 * after which raft_persisted() is what the core hears. A segment the round cannot hand out -- more
 * than it holds, or no duplicate to be had at the descriptor limit -- is synced inside the
 * prepare, and one of those that fails fails the round (#264, plan 26z).
 */
#ifndef ISO_RAFT_RAFT_LOG_H
#define ISO_RAFT_RAFT_LOG_H

#include "raft/raft.h"
#include "raft/raft_wire.h"

typedef struct raft_log raft_log;

/* The layout's fixed parts that a size is computed from: a segment's header, a record's header --
 * which is also the size of the terminator a region writes after the last record -- and what a
 * region holds ahead of its slots (its own header and the hard-state ring). */
#define RAFT_LOG_SEGMENT_HEAD 64u
#define RAFT_LOG_RECORD_HEAD 32u
#define RAFT_LOG_REGION_HEAD ((uint64_t)128 << 10)

/* **The smallest segment a region is carved into** (plan section 23, #161): its header, one record
 * of the largest entry replication can carry, and the terminator after it. A record never
 * straddles two segments and a slot does not grow the way a file does, so a slot smaller than this
 * is a slot some entry a leader can send does not fit -- and before #161 the appender wrote such an
 * entry anyway, over the slots after its own. */
#define RAFT_LOG_SEGMENT_MIN \
	((uint64_t)RAFT_LOG_SEGMENT_HEAD + 2u * RAFT_LOG_RECORD_HEAD + RAFT_WIRE_ENTRY_MAX)

/* **And the fewest slots a region is carved into**: the eighth the carve has always taken, now a
 * floor as well. The log's only lever is compaction, which frees whole segments and never the
 * last, and the metadata group snapshots early when two slots or fewer are free -- so a region of
 * three or four slots would sit in that state for good. Eight leaves the early snapshot for the
 * case it is for. */
#define RAFT_LOG_REGION_SLOTS 8u
#define RAFT_LOG_REGION_MIN (RAFT_LOG_REGION_HEAD + RAFT_LOG_REGION_SLOTS * RAFT_LOG_SEGMENT_MIN)

typedef struct {
	/* 0 selects 64 MiB; a record never straddles two segments. In a directory a segment is a
	 * file, and one whose single record is larger than this grows to hold it; a region's slot
	 * cannot, so there the record is refused (#161) and the carve keeps it from happening. */
	uint64_t segment_bytes;

	/* The log in a region of a raw device instead of a directory of files (plan section 23,
	 * 94-c). **Zero `region_len` means a directory**, which is everything today and is the
	 * convention `kv_env_opts` set in 94-b-i -- it is the one that survives a caller writing
	 * `{0}`, where a `region_fd` discriminator would name standard input.
	 *
	 * The descriptor is the vdev's and is never closed here, nor written through: the log opens
	 * the same device again, a description of its own for every segment and the hard state
	 * alike, and closes that with the log -- so that a write error the disk reports reaches the
	 * log's own syncs and is not taken by the metadata store's sync of a shared description
	 * (#114, plan 28c). `region_off` must be 64 KiB aligned. The
	 * region's own header decides `segment_bytes`; the field above is the upper bound a
	 * configuration expresses, and a difference is one line at open rather than a refusal.
	 * `region_name` is what a message calls the device, since there is no path to quote. */
	int region_fd;
	uint64_t region_off;
	uint64_t region_len;
	const char *region_name;

	/* **What every refusal of this log's contents ends with** (plan section 23, #185): the
	 * owner's remedy for a log that cannot be read, in place of the log's own words. The log
	 * knows its directory or its device and nothing else; the metadata group knows the node's
	 * number and the group's voters, and with them leads with `isopool reseed`, which rebuilds
	 * the replica while its node serves (`iso_mds_log_remedy`). NULL gives the log's own words:
	 * the manual remedy, with the only voter's case beside it. Read only during raft_log_open,
	 * and never as a format. */
	const char *remedy;

	/* **Opened to be emptied** (plan 5a-i-g, L; section 23): the reseed's open, after which the
	 * caller may call raft_log_hard_fence, then raft_log_discard, and raft_log_close -- nothing
	 * else until the discard has run. The segments are found and not read: a directory's by
	 * their names, a region's by the slot headers that verify. So a log whose contents a plain
	 * open refuses -- a damaged record, two segments at one index, a gap, a bad segment header --
	 * opens to be thrown away, which is the one thing a reseed wants of it, and the hard state is
	 * read as ever. A region whose own header does not check out, or does not fit, is laid out
	 * again by the discard with `raft_log_carve`'s geometry, the hard state kept. Nothing is
	 * written by the open itself: a reseed fences the vote before anything goes. */
	bool discard_only;

	/* **How often the durable floor is written, at most** (#483, P1m-15, plan 28h): see
	 * raft_log_durable. Zero is once a second. A unit test sets one nanosecond to write it on
	 * every round it can. */
	uint64_t floor_every_ns;
} raft_log_opts;

/* Open the log in `dir`, creating the directory when needed. An empty log is started on
 * `base_index`/`base_term` -- the state machine's applied index and term, so a node whose log
 * directory was deleted comes back knowing exactly what it lacks and is re-fed by the leader.
 * When segments exist the base comes from them and the arguments are ignored. */
iso_err raft_log_open(const char *dir, const raft_log_opts *opts, raft_index base_index,
		      raft_term base_term, raft_log **out, char *err, size_t errsize);
void raft_log_close(raft_log *l);

/* What raft_new() takes; pass the raft_log as the store pointer. compact() keeps whole
 * segments, so first() may stay below the index asked for -- the core allows that. get() copies
 * each record into a read arena that only release() rewinds (#201), so a caller that reads the
 * log itself, outside a core, calls release() when it is done with what it read. */
extern const raft_log_ops raft_log_file_ops;

/* **Empty the log, and keep the hard state** (plan 5a-i-g, L: the reseed's `discard_log`). The
 * log's own reset to index 0: every segment is deleted, the last first, and one is started at
 * index 1 on term 0. The hard state -- `raft/hard`, or the region's ring -- is not touched, so a
 * replica keeps its term and its vote at every point a crash could stop this, and nothing has to
 * save them again. One stopped part way leaves a prefix of the old log, which a second call
 * empties. After a `discard_only` open it is the call that makes the log a log again, and a
 * region's header that did not check out is laid out again once every segment has gone and
 * before the new one is made. */
iso_err raft_log_discard(raft_log *l, char *err, size_t errsize);

#ifdef ISO_DEBUG
/* A test seam, compiled only into debug builds, as `iso_pwrite_hook` is: when non-zero, the next
 * reset or discard fails once it has deleted this many segments, as a crash or a failed unlink or
 * slot release there would leave the log (plan 5a-i-g, test 24). Cleared when it fires. */
extern unsigned raft_log_test_fail_reset_after;

/* A path whose existence refuses the segment the next reset starts the log on, as a create at the
 * descriptor limit is refused (EMFILE) -- after every segment has gone -- and removes the file (#308,
 * plan 28c). The log faults; the core installs nothing. */
extern const char *raft_log_test_fail_reset_file;

/* A path whose existence refuses the next no-op entry appended -- the one a new leader opens its
 * term with -- as a failed write is refused: the record taken back, cleanly, so no fault of the
 * log's -- and removes the file (#439, minor 68, plan 28c). */
extern const char *raft_log_test_fail_noop_file;

/* Two more for a sync round (plan 26z, #264), debug builds only. `raft_log_test_sync_fds`, when 1
 * to 7, is how many descriptors a round hands out before the rest of the segments it touched are
 * synced on the spot -- as a round of more than eight is -- so that a test reaches that path with
 * two entries rather than eight segments' worth. `raft_log_test_fail_inline_sync`, when non-zero,
 * fails that many of the syncs a round makes on the spot with EIO -- standing in for the
 * `fdatasync`'s result, so the failure takes the branch a disk's error takes and is said as one is
 * ("... the sync of entries 41 to 41 failed: Input/output error"), after a WARN that the seam
 * fired; counted down as each fires. And
 * `raft_log_test_fail_inline_file`, when set, is a path whose existence arms the same failure for
 * the next such sync, which removes it: how a test fails one sync of a running node at the moment
 * it chooses, once its cluster has formed. */
extern unsigned raft_log_test_sync_fds;
extern unsigned raft_log_test_fail_inline_sync;
extern const char *raft_log_test_fail_inline_file;
/* And how far apart the WARN about rounds with no duplicate descriptor is said, at most, in
 * nanoseconds; 0 is the minute it is. For the test of its second line. */
extern uint64_t raft_log_test_say_every_ns;

/* And the syncs the log makes outside a round (#114, plan 28c): `raft_log_test_fail_own_sync`,
 * when non-zero, fails that many of them with EIO -- a slot given back, a segment made, a
 * directory synced, standing in for the call's result -- counted down as each fires; and
 * `raft_log_test_fail_own_file` is a path whose existence arms the same failure for the next one,
 * which removes it. Each fires a fault (`raft_log_fault`). */
extern unsigned raft_log_test_fail_own_sync;
extern const char *raft_log_test_fail_own_file;

/* **A power cut the moment after a roll** (#296, plan 28c), in a directory: at the next roll that
 * leaves a tail no sync has been asked of -- written after the round or the sync that last covered
 * its segment -- once the new segment's first entry is written, every segment but the last is cut
 * back to what a sync was asked of, and the last keeps all it was given, as a disk whose writeback
 * ran ahead could leave them. `raft_log_test_cut_at_roll`, when non-zero, arms it that many times
 * and the append returns, for a test that closes the log and opens it again
 * (`raft_log_test_cut_done` says it fired); `raft_log_test_cut_at_roll_file` is a path whose
 * existence arms it once, removing it, and the process then ends (`_exit(86)`), as a power cut
 * ends it. */
extern unsigned raft_log_test_cut_at_roll;
extern const char *raft_log_test_cut_at_roll_file;
extern bool raft_log_test_cut_done;
#endif

/* One of these is filled per sync round: descriptors to fdatasync (already dup'ed; run closes
 * them) and the index and term the log reached when it was prepared. `err` is the errno of the
 * first sync the round made on the spot that failed, or 0.
 *
 * **The round is durable only if every descriptor's sync succeeds and `err` is 0** (#264, plan
 * 26z). A segment the round cannot hand out -- more segments touched than `fds` holds, or no
 * duplicate to be had because the process is at its descriptor limit -- is synced inside
 * raft_log_sync_prepare, through its own descriptor, and the prepare returns ISO_ERR_IO when one
 * of those failed: the caller reports nothing of the round persisted (and still closes what it
 * was handed). Before #264 the overflow's failure was only logged and the round's index went on to
 * cover it, and a -1 from a failed `dup` was handed out to fail the round as "Bad file
 * descriptor" -- a disk blamed for a descriptor limit. `fds` never holds -1. */
typedef struct {
	int fds[8];
	unsigned n;
	int err;
	raft_index index;
	raft_term term;
} raft_log_sync;

/* ISO_ERR_IO: a sync made on the spot failed (`out->err` says how); the round is not durable,
 * whatever the descriptors it handed out then say. And every round of a log that has faulted
 * (`raft_log_fault`) is refused so, with nothing handed out. */
iso_err raft_log_sync_prepare(raft_log *l, raft_log_sync *out);
/* Any thread. ISO_ERR_IO -- a descriptor's sync, or `s->err` -- is fail-stop. Closes them. */
iso_err raft_log_sync_run(raft_log_sync *s);

/* **The durable floor** (#483, P1m-15, plan 28h): the newest index the hard-state ring names as
 * synced and committed, below which a record that does not verify at open is rot, never a torn
 * tail. Each contract is at the definition in raft_log.c; all three on the log's owner. */
/* Every sync round is done: the log is durable through `index`/`term`. */
void raft_log_durable(raft_log *l, raft_index index, raft_term term);
/* Note the commit; true when a floor's slot is due and the caller, idle, is to start a round. */
bool raft_log_floor_due(raft_log *l, raft_index commit, uint64_t now_ns);
/* The floor the ring holds on the platter now, and its term; 0 when none. */
raft_index raft_log_floor(const raft_log *l, raft_term *term);

/* What a raft directory holds, **without opening it** -- because opening it creates the first
 * segment, and a caller that has to tell "this node has a history" from "this node has never run"
 * would destroy the answer by asking for it (plan section 23, 94-b-ii-c).
 *
 * A missing directory is not an error: it is the answer `segments == 0`. `first` is the lowest
 * index any segment name carries, which is what a refusal has to quote, and is left at zero when
 * there are none. Only names are read; nothing is opened and no header is parsed, so a directory
 * whose segments are damaged still reports them as present -- which is correct here, since the
 * question is whether this node has a log at all.
 */
typedef struct {
	unsigned segments;
	raft_index first;
	/* Region only, and zero for a directory: how many segment slots the region was carved
	 * into. A directory grows until the filesystem is full; a region does not, so the number
	 * is something `isod check` has to be able to say (plan section 23, 94-c). */
	unsigned slots_total;
} raft_log_dir_info;

iso_err raft_log_probe_dir(const char *dir, raft_log_dir_info *out, char *err, size_t errsize);

/* The same question of a region, with one more answer: `ISO_ERR_NOTFOUND` means the region was
 * never formatted (all zero), which is "this node has no log" exactly as a missing directory is.
 * Anything else that does not check out is `ISO_ERR_CORRUPT` -- **unformatted is not damaged**, the
 * rule 94-b-i landed. */
iso_err raft_log_probe_region(int fd, uint64_t off, uint64_t len, raft_log_dir_info *out, char *err,
			      size_t errsize);

/* **Whether a reseed can empty the raft log in this region** (plan section 23, the review of #185):
 * ISO_OK when a `discard_only` open of it would open -- its header checks out and fits, or does not
 * and the region can be laid out again with `raft_log_carve`'s geometry -- and otherwise the code
 * and the words that open would refuse it with: ISO_ERR_NOTFOUND for a region never formatted, and
 * the carve's refusal for one smaller than RAFT_LOG_REGION_MIN, which a label made before #161 can
 * have. Reads the region's header and nothing else, writes nothing and logs nothing, so a caller
 * can ask before it changes anything -- as the reseed does before its demotion. `name` is what the
 * words call the region. */
iso_err raft_log_region_discardable(int fd, uint64_t off, uint64_t len, const char *name, char *err,
				    size_t errsize);

/* **What a refusal of a region's own header ends with where the region cannot be laid out again**
 * (plan section 23, the review of #185): smaller than RAFT_LOG_REGION_MIN, so that no reseed can
 * empty the log, and a reformat of `device` -- which gives the log at least that -- is the one
 * remedy. Why, then #164's two sentences: a group's only voter copies its device aside, and where
 * the group has other voters, once `isopool mds` shows them at the leader's commit, the node is
 * stopped, the device reformatted and the node started to be re-fed by the leader. The log uses it
 * for its own refusals of such a header in place of its owner's remedy, and the node for the probe
 * of a blank store's log region. One or more sentences, ending in a full stop. */
void raft_log_small_region_remedy(uint64_t region_len, const char *device, char *out, size_t n);

/* Lay out a raft log in a region: the header, the hard-state ring and an array of empty segment
 * slots. **Making one is a verb** -- an open never formats, for the reason a store that formats
 * what it is pointed at is a store that eats the wrong disk. `segment_bytes` is fixed here because
 * it decides how many slots there are; 0 selects 64 MiB. Refuses a region that already holds a log
 * unless `force`, and refuses one too small for two segments.
 *
 * **This is the mechanism, not the policy**: it lays out whatever geometry it is given, which is
 * what lets a test roll a log through slots of a few KiB. What a node formats with comes from
 * `raft_log_carve`, and only from there. */
iso_err raft_log_format_region(int fd, uint64_t off, uint64_t len, uint64_t segment_bytes,
			       bool force, char *err, size_t errsize);

/* What segment size to format a region of `region_len` with, given the configured upper bound --
 * or a refusal (plan section 23, 94-c and #161).
 *
 * **`[mds] log_segment_bytes` is a ceiling, not an instruction** -- it says how large a segment may
 * grow before the log rolls, which is a statement about a disk and not about this region. A region
 * also has to hold enough segments to be able to compact at all, so the size is the smaller of the
 * two: the configured bound, or an eighth of the room the region has. A test image and a 14 TB disk
 * then both get a sensible carve without anyone computing one.
 *
 * **And never smaller than RAFT_LOG_SEGMENT_MIN**, which is what #161 added. A ceiling below it is
 * raised to it, with one INFO line; it is a ceiling, and refusing to start over one would make a
 * setting that says "small segments" into a node that does not run. A region too small for
 * RAFT_LOG_REGION_SLOTS such segments is refused -- ISO_ERR_INVAL, naming both sizes and the
 * remedy, which is room: no ceiling can make a region hold what it has no room for.
 *
 * Every caller that formats a region for a node uses this, so `isod mds init` and the node's own
 * never-run branch cannot choose differently. */
iso_err raft_log_carve(uint64_t region_len, uint64_t configured, uint64_t *segment_bytes,
		       char *err, size_t errsize);

/* An entry the log refused because no segment of this region can hold it: `index`, its payload's
 * `bytes`, and `most`, the largest payload one of these segments holds (plan section 23, #161).
 * Only a region refuses this way -- a file grows to hold its one record -- and the refusal is the
 * append's ISO_ERR_TOOBIG; this is what it was about, for the owner that has to say so.
 *
 * `raft_log_take_refusal` hands the last one over and forgets it, so an owner that asks after
 * every call it makes into the core hears each refusal once, from the call that met it. */
typedef struct {
	raft_index index;
	uint64_t bytes;
	uint64_t most;
} raft_log_refusal;

bool raft_log_take_refusal(raft_log *l, raft_log_refusal *out);

/* **Whether the log has faulted, and in what words** (#114, plan 28c). A write or a sync the log
 * makes outside a sync round failed: a slot given back (compaction, truncation, reset), a segment
 * made at a roll, a truncation, a reset's header, a directory's fsync, a failed append that could
 * not be taken back. Then what the log holds on its disk is no longer known to be what it holds in
 * memory, and the failed sync may have taken a write error that the next round's would not report
 * -- one sync of a description reports an error once. So the fault is kept from the first and
 * never cleared, every later round is refused (`raft_log_sync_prepare`), and the owner halts: the
 * metadata group asks after every call it makes into the core, before it acts on anything the core
 * queued. `what` gets one sentence naming where the log lives -- the device, or the directory --
 * what failed and how, without a full stop. Until #114 these failures were logged, returned to a
 * core that did not look, and the replica went on acknowledging. */
bool raft_log_fault(const raft_log *l, char *what, size_t n);

/* Hard state. Load reports ISO_ERR_NOTFOUND for a node that has never voted. */
iso_err raft_log_hard_load(const char *dir, raft_hard_state *out, char *err, size_t errsize);
iso_err raft_log_hard_load_region(int fd, uint64_t off, uint64_t len, raft_hard_state *out,
				  char *err, size_t errsize);
iso_err raft_log_hard_save(raft_log *l, const raft_hard_state *hs, char *err, size_t errsize);

/* **The reseed's fence, written before its discard** (plan 5a-i-g, L's demotion's repair). The hard
 * state on disk, with its term raised to `term` when that is higher -- the vote is then cleared,
 * since this node never reached that term -- and otherwise kept, and the vote fence set to
 * `(fence_index, fence_term)`: the leader's commit when it answered that this node is a learner,
 * and that entry's term. One durable write, the one a vote is saved with, so a crash leaves either
 * the old hard state or this one. A slot that exists and does not check out is not fenced over:
 * ISO_ERR_CORRUPT, as raft_log_hard_load refuses it. `out`, when not NULL, is what was written. */
iso_err raft_log_hard_fence(raft_log *l, raft_term term, raft_index fence_index,
			    raft_term fence_term, raft_hard_state *out, char *err, size_t errsize);

typedef struct {
	unsigned segments;
	uint64_t bytes;   /* on disk, headers included */
	uint64_t entries; /* held in the log */
	raft_index first;
	raft_index last;
	uint64_t torn_tail_bytes; /* discarded at the last open */

	/* Region mode only, and zero for a directory. **A directory grows until the filesystem is
	 * full; a region has a fixed number of slots**, so how many are left is a thing an operator
	 * has to be able to see before it matters -- the log's only lever is compaction, and
	 * compaction is driven by the snapshot interval and not by space. */
	uint64_t slots_total;
	uint64_t slots_free;
	uint64_t segment_bytes;

	/* **What get() has handed out and nobody has released** (#201): the read arena's blocks now,
	 * and the most they have come to since the open. A core releases at the end of every drain,
	 * so the peak is the most one drain read -- a batch a follower applies, the appends a leader
	 * ships a follower it is catching up -- and `arena_bytes` between drains is the one block the
	 * release keeps. */
	uint64_t arena_bytes;
	uint64_t arena_peak;

	/* Syncs a round made on the spot rather than hand out (#264, plan 26z): every one -- more
	 * segments touched than a round holds, or no duplicate descriptor to be had -- and of those,
	 * the ones for want of a duplicate: a process at its descriptor limit. */
	uint64_t syncs_inline;
	uint64_t syncs_no_dup;

	/* The durable floor (#483, P1m-15, plan 28h): the index the ring names on the platter
	 * (raft_log_floor), and the one a round in flight carries a slot for, 0 when none does. */
	raft_index floor;
	raft_index floor_handed;
} raft_log_stats;

void raft_log_stat(const raft_log *l, raft_log_stats *out);

#endif /* ISO_RAFT_RAFT_LOG_H */
