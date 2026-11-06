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
/*-------------------------------------------------------------------------
 *
 * raft_log.c
 *	  The Raft log on disk: segments in a directory or a device region, and the
 *	  hard-state ring.
 *
 * INTERFACE ROUTINES
 *		raft_log_open			- scan the log, cut a torn tail, refuse anything else
 *		raft_log_close			- close it; nothing is synced
 *		raft_log_hard_save		- write and sync the next hard-state slot
 *		raft_log_hard_fence		- a reseed's vote fence, before its discard
 *		raft_log_discard		- empty the log, keeping the hard state
 *		raft_log_sync_prepare	- hand out a round's descriptors (and a floor's slot)
 *		raft_log_sync_run		- fdatasync a round's descriptors, on any thread
 *		raft_log_durable		- every round is done through an index; a floor slot counts
 *		raft_log_floor_due		- note the commit; whether a floor's slot is due
 *		raft_log_floor			- the durable floor on the platter
 *		raft_log_fault			- whether the log has failed, and why
 *		raft_log_carve, raft_log_format_region, raft_log_probe_region,
 *		raft_log_probe_dir, raft_log_hard_load, raft_log_hard_load_region,
 *		raft_log_region_discardable, raft_log_small_region_remedy
 *								- a region's geometry and the tools' reads
 *		raft_log_stat, raft_log_take_refusal
 *								- what the operator and the group are told
 *
 * NOTES
 *	  src/raft/README is the design: the formats, who calls what on which
 *	  thread, what is on the platter when, and what an open makes of each
 *	  thing a crash can leave. raft_log.h holds the contract of each call; the
 *	  byte layouts are below. In short: the core's log store
 *	  (raft_log_file_ops) appends to the last segment and never syncs; the
 *	  owner's sync rounds do, and only a completed round lets the core
 *	  acknowledge. The open tells a tear from rot by place -- the end of the
 *	  last segment is where a crash leaves one -- and, since plan 28h, by the
 *	  durable floor the hard-state ring names: at or below it nothing is a
 *	  tear. A truncation or a reset lowers the floor on the platter before it
 *	  cuts anything, so a crash between the two never leaves a floor past the
 *	  log; compaction removes only what the store holds, which the open takes
 *	  as the log's base.
 *
 *-------------------------------------------------------------------------
 */
#include "raft/raft_log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include "common/clock.h"
#include "common/crc32c.h"
#include "common/log.h"

#define SEG_MAGIC "ISORAFT1"
#define REG_MAGIC "ISORLOG1"
#define HARD_MAGIC "ISOHARD1"
#define SEG_HEADER RAFT_LOG_SEGMENT_HEAD
#define SEG_VERSION 2 /* 1 had no nonce (plan section 23, 94-c) */
#define REC_HEADER RAFT_LOG_RECORD_HEAD
#define HARD_SLOT 4096
#define DEFAULT_SEGMENT (64u << 20)
#define ARENA_BLOCK (1u << 20)
#define REC_MAX_LEN (64u << 20) /* a record longer than this is corruption, not data */

/* Segment header, 64 bytes:
 *   0  magic[8]
 *   8  u32 version (2)
 *  12  u32 crc32c of the header with this field zero
 *  16  u64 first index held by this segment
 *  24  u64 term of first-1 (the log's base term for the oldest segment)
 *  32  u64 nonce, random per segment; every record's checksum is seeded with it
 *  40  zeros
 * Record header, 32 bytes:
 *   0  u32 crc32c of the nonce followed by bytes [4, 32+len)
 *   4  u32 payload length
 *   8  u64 index
 *  16  u64 term
 *  24  u8 type, 7 zero bytes
 *
 * **The nonce is what makes a slot reusable.** In a region a segment lives in a fixed slot, and a
 * slot that compaction freed still holds the previous occupant's records past wherever the new one
 * stops writing. Their indexes and terms can line up with what the scan expects -- rarely, but
 * "rarely" is a coincidence, not a rule -- so seeding every checksum with the segment's own nonce
 * makes an earlier occupant's record structurally unreadable by a later one. Cost: eight bytes of a
 * header that had thirty-two spare, and one more crc32c call over eight bytes.
 *
 * A log written by an older build has version 1 and is refused as a bad header, with the remedy
 * every refusal of a log's contents names (`log_remedy`): from the metadata group, `isopool
 * reseed` first, which rebuilds the replica while its node serves (#185), then the manual one --
 * delete the raft directory and start the node again, it comes back on its applied index.
 *
 * Region layout (plan section 23, 94-c), all offsets from the region's start:
 *   0            the region header below, 512 bytes in the first aligned block
 *   REG_ALIGN    two HARD_SLOT hard-state slots -- the ring, unchanged
 *   2*REG_ALIGN  `n_slots` slots of `segment_bytes`, each one segment
 *
 * Region header, 512 bytes:
 *   0  magic[8]
 *   8  u32 version (1)
 *  12  u32 zero
 *  16  u64 segment_bytes
 *  24  u64 n_slots
 *  32  zeros
 * 508  u32 crc32c of bytes [0, 508) */
#define REG_HEADER 512
#define REG_VERSION 1
#define REG_ALIGN 65536u
#define REG_HARD_OFF ((uint64_t)REG_ALIGN)
#define REG_SLOTS_OFF ((uint64_t)REG_ALIGN * 2)
ISO_STATIC_ASSERT(REG_SLOTS_OFF == RAFT_LOG_REGION_HEAD, "the header's figure is the layout's");
ISO_STATIC_ASSERT(RAFT_LOG_SEGMENT_MIN <= DEFAULT_SEGMENT, "the default holds the largest entry");
ISO_STATIC_ASSERT(RAFT_WIRE_ENTRY_MAX <= REC_MAX_LEN, "a replicable entry is not corruption");

typedef struct {
	raft_index first;
	raft_term prev_term;
	uint64_t nonce;
	int fd;
	uint64_t base;  /* byte offset of the segment within `fd`; 0 for a file */
	uint64_t slot;  /* region mode: which slot holds it */
	uint64_t size;  /* bytes written, header included, relative to `base` */
	bool dirty;     /* written since the last sync prepare */
#ifdef ISO_DEBUG
	uint64_t handed; /* `size` when a sync was last asked of it: a round's or the log's own */
#endif
	uint64_t *off;  /* record offsets, per entry, relative to `base` */
	raft_term *term;
	uint32_t *len;
	size_t n, cap;
} segment;

typedef struct arena_block {
	struct arena_block *next;
	size_t used, cap;
	uint8_t data[];
} arena_block;

struct raft_log {
	char *dir; /* NULL in region mode */
	int dirfd;
	uint64_t segment_bytes;
	segment *seg;
	unsigned n_seg, cap_seg;
	arena_block *arena;
	/* What the arena holds now, blocks and all, and the most it has held since the open: the
	 * bytes one drain's reads cost, which since #201 nothing gives back before the drain ends. */
	uint64_t arena_bytes, arena_peak;
	/* The hard state's: `raft/hard`, or in a region a description of the device of its own
	 * (#305, plan 28c), never the log's -- a round on the ring syncs a duplicate of that, and
	 * either could take the other's write error and leave the other clean. */
	int hard_fd;
	bool hard_seq_known; /* the ring's sequence has been read back from the disk */
	uint64_t hard_seq;
	uint64_t torn_tail_bytes;

	/* Region mode. `region_len` is the discriminator -- zero means a directory of files, the
	 * convention `kv_env_opts` set in 94-b-i and the one that survives a caller writing `{0}`.
	 * The descriptor is the log's own, opened on the vdev's device at the open and closed with
	 * the log: never the vdev's, which the metadata store wrote through until #114 (plan 28c),
	 * so that either one's sync could take the other's write error. */
	int region_fd;
	uint64_t region_off, region_len;
	uint64_t n_slots;
	bool *slot_used;
	char *region_name; /* what a message calls it, since there is no path */

	/* The last entry the append refused for want of a segment that holds it (#161), until
	 * `raft_log_take_refusal` hands it over. */
	raft_log_refusal refused;
	bool has_refused;

	/* The open's own state (#185): the owner's remedy, borrowed for the length of
	 * raft_log_open; opened to be emptied (`raft_log_opts.discard_only`), which only the open
	 * reads; and a region's header to lay out again, which the discard writes. */
	const char *remedy;
	bool discard_only;
	bool relay_header;

	/* Syncs a round made on the spot rather than hand out (#264, plan 26z): every one, and those
	 * for want of a duplicate descriptor -- the process at its descriptor limit -- which are said
	 * in a WARN at most once a minute: when it was said, and how many since. */
	uint64_t syncs_inline, syncs_no_dup;
	uint64_t no_dup_unsaid;
	uint64_t no_dup_said_ns;
	bool no_dup_said;

	/* **A fault** (#114, plan 28c): a write or a sync the log made outside a round that failed --
	 * a slot given back, a segment made or rolled from, a truncation, a reset's header, a
	 * directory synced, a failed append that could not be taken back. The first is kept, said
	 * once and never cleared: what the log holds on disk is no longer known to be what it holds
	 * in memory, and a failed sync may have taken a write error that the next one would not
	 * report. Every round after it is refused (`raft_log_sync_prepare`), and the owner, which
	 * asks `raft_log_fault` after each call into the core, halts its replica. */
	bool faulted;
	int fault_errno;
	char fault[512];

	/* **A log with no segment** (#308, plan 28c): what a reset that deleted every one and
	 * could not make the next leaves -- faulted, always -- and a `discard_only` open of a log that
	 * had none, until its discard. `first`, `last` and `term_at` answer from here then: the index
	 * before the first entry the log would hold, and its term. A reset sets it before it deletes
	 * anything. It used to be read off `seg[n_seg - 1]` with no segment there, and the halted
	 * replica's first status read took the node down with it. */
	raft_index bare_index;
	raft_term bare_term;

	/* **The durable floor** (#483, P1m-15, plan 28h; raft_log_durable). `floor` is what the
	 * ring names on the platter: a record at or below it was synced and committed, so the open
	 * refuses one that does not verify instead of cutting it. `floor_handed` is a floor written
	 * into a slot that a round in flight syncs, 0 when none: durable once every round is
	 * (raft_log_durable), or once a save syncs the ring first (`hard_settle`). `durable` is what
	 * completed rounds cover; `floor_want` the floor due next, the lower of `durable` and the
	 * commit (raft_log_floor_due). `floor_fd` is the ring's descriptor for the floor's slots,
	 * apart from `hard_fd` so a term or a vote saved while a round syncs a floor hears its own
	 * write error (#305); opened at the first floor. `hard_last` is the term, vote and fence of
	 * the ring's newest slot, which a floor's slot carries unchanged; `hard_any` that there is
	 * one. All on the owner's thread. */
	raft_index floor, floor_handed, durable, floor_want;
	raft_term floor_term, floor_handed_term, floor_want_term;
	uint64_t floor_every_ns, floor_at_ns;
	int floor_fd;
	raft_hard_state hard_last;
	bool hard_any;
};

static bool floor_covers(const raft_log *l, raft_index index);
static iso_err floor_refuse(raft_log *l, const char *what, char *err, size_t errsize);
static iso_err floor_load(raft_log *l, char *err, size_t errsize);
static iso_err floor_check_end(raft_log *l, raft_index base_index, char *err, size_t errsize);
static iso_err floor_lower(raft_log *l, raft_index keep, raft_term keep_term, char *err,
			   size_t errsize);
static void floor_hand(raft_log *l, raft_log_sync *out);

static bool in_region(const raft_log *l) { return l->region_len != 0; }

/* What an error message names. A file log has a directory; a region has a device and an offset,
 * and the caller gave us the words for it. */
static const char *log_name(const raft_log *l) { return l->dir ? l->dir : l->region_name; }

/* **What an operator does about a log in a region that cannot be read** (plan section 23, #164).
 * There is no directory to delete: the log is a region of the metadata device, and the one verb
 * that lays it out again, `isod mds init --force`, lays out the store beside it too -- so the
 * replica comes back empty and is re-fed by the leader, which is a remedy only where a leader
 * holds what this replica held. A directory's log is deleted alone and its store keeps its applied
 * index, which is why the directory's refusals say something else. These are the log's own words,
 * which a refusal ends in when its owner gave none (`log_remedy`): the command last -- a health
 * reason that has to cut a long line keeps the first sentence and the last ones (`halt_detail`,
 * health.c) -- and its one argument is the device, so the command is the one to type. The
 * metadata group gives its own, which lead with `isopool reseed` (#185). */
#define REGION_REMEDY                                                                            \
	"A group's only voter holds the only copy of its metadata: copy its device aside. Where "  \
	"the group has other voters, once `isopool mds` shows them at the leader's commit, "    \
	"stop the node, reformat the device with `isod mds init --force %s` and start it to be "   \
	"re-fed by the leader."

/* **And a directory's**, the same two cases (#185). Its log is deleted alone, and the store keeps
 * its applied index, so that is the manual remedy where other voters hold what it held; for a
 * group's only voter it would delete the only copy of what the store has not applied yet. */
#define DIR_REMEDY                                                                               \
	"A group's only voter holds the only copy of its metadata: copy its raft directory and "   \
	"its store aside. Where the group has other voters, once `isopool mds` shows them at "  \
	"the leader's commit, stop the node, delete the raft directory %s and start it again: it " \
	"comes back on its state machine's applied index and is re-fed by the leader."

/* **What every refusal of the log's contents ends with** (#185): the owner's remedy when it gave
 * one -- the metadata group's (`iso_mds_log_remedy`), which knows the node's number and the
 * group's voters and so leads with `isopool reseed`, the command that rebuilds the replica
 * while its node serves -- and otherwise the log's own manual remedy, both cases said. One place,
 * so no refusal can name something else; a region's own header has one exception
 * (`header_remedy`). */
static const char *log_remedy(const raft_log *l, char *buf, size_t n)
{
	if (l->remedy)
		return l->remedy;
	if (in_region(l))
		snprintf(buf, n, REGION_REMEDY, log_name(l));
	else
		snprintf(buf, n, DIR_REMEDY, log_name(l));
	return buf;
}

void raft_log_small_region_remedy(uint64_t region_len, const char *device, char *out, size_t n)
{
	/* The reason first and #164's two sentences last: a replica's health keeps a remedy's last
	 * sentences whole, and the command is in the last one. */
	snprintf(out, n,
		 "This log region is %" PRIu64 " bytes, less than the %" PRIu64 " a raft log now "
		 "needs, so `isopool reseed` cannot lay it out again: only a reformat can. "
		 REGION_REMEDY,
		 region_len, (uint64_t)RAFT_LOG_REGION_MIN, device);
}

/* **What a refusal of a region's own header ends with** (plan section 23, the review of #185).
 * The reseed empties such a log by laying the header out again with the carve's geometry
 * (`relay_geometry`), which a region smaller than RAFT_LOG_REGION_MIN cannot be given -- a label
 * made before #161 carved some -- so there the owner's remedy, which leads with the reseed, would
 * send the operator to a command that cannot finish, and that stopped part way over a store it
 * had already emptied. The reformat is named instead: it lays the device out again, and gives the
 * log a region of at least that. Every other refusal is one the reseed can empty, the slots of a
 * small region included, since it keeps a header that checks out. */
static const char *header_remedy(const raft_log *l, char *buf, size_t n)
{
	if (l->region_len >= RAFT_LOG_REGION_MIN)
		return log_remedy(l, buf, n);
	raft_log_small_region_remedy(l->region_len, log_name(l), buf, n);
	return buf;
}

/* ---- small helpers */

static iso_err io_err(char *err, size_t errsize, const char *what, const char *path)
{
	int e = errno;
	return iso_errf(err, errsize, iso_err_from_errno(e), "%s %s: %s", what, path, strerror(e));
}

/* **The log faulted** (#114, plan 28c): `what` failed, with `e`. The first is kept and said; see
 * `faulted` in the struct. The words name where the log lives -- the device, or the directory --
 * since that is what an operator checks, and say that nothing past it is acknowledged. */
static void log_fault(raft_log *l, int e, const char *what)
{
	if (l->faulted)
		return;
	l->faulted = true;
	l->fault_errno = e ? e : EIO;
	snprintf(l->fault, sizeof l->fault,
		 "the raft log on %s could not be kept on its disk: %s failed (%s), so nothing it "
		 "holds is acknowledged from here on",
		 log_name(l), what, strerror(l->fault_errno));
	LOG_ERR("%s", l->fault);
}

bool raft_log_fault(const raft_log *l, char *what, size_t n)
{
	if (!l->faulted)
		return false;
	if (what && n)
		iso_strlcpy(what, l->fault, n);
	return true;
}

#ifdef ISO_DEBUG
unsigned raft_log_test_fail_own_sync;
const char *raft_log_test_fail_own_file;
#endif

/* **A sync the log makes outside a round** -- of a slot given back, a segment made or rolled from,
 * the directory -- of `fd`, a directory's with `fsync` (`dir`): 0, or the errno it failed with.
 * One place, so that every one of them takes the fault's branch alike and the test seam reaches
 * each (#114, plan 28c). */
static int own_sync(raft_log *l, int fd, bool dir)
{
	int rc = dir ? fsync(fd) : fdatasync(fd);
	int e = rc != 0 ? (errno ? errno : EIO) : 0;
#ifdef ISO_DEBUG
	/* Stands in for the call's result, so a test fails the branch a disk's error takes. Armed by
	 * a count, or by a file the failure removes (an unlink that finds it is the test-and-clear). */
	bool armed = raft_log_test_fail_own_sync ||
		     (raft_log_test_fail_own_file && unlink(raft_log_test_fail_own_file) == 0);
	if (armed) {
		if (raft_log_test_fail_own_sync)
			raft_log_test_fail_own_sync--;
		LOG_WARN("raft log %s: a fault injected by the test seam fails a sync it makes "
			 "outside a round",
			 log_name(l));
		e = EIO;
	}
	/* What the power-cut seam keeps (#296): what this sync covered, of the segments whose
	 * descriptor it was -- the one it was, never the one a caller meant. */
	if (!e && !dir)
		for (unsigned k = 0; k < l->n_seg; k++)
			if (l->seg[k].fd == fd)
				l->seg[k].handed = l->seg[k].size;
#else
	(void)l;
#endif
	return e;
}

/* There is at least one segment between calls, but in a log that faulted at a reset or one opened
 * to be emptied (`bare_index`): every caller that can meet one of those asks `n_seg` first. */
static segment *last_seg(raft_log *l) { return &l->seg[l->n_seg - 1]; }

static raft_index seg_last(const segment *s) { return s->first + s->n - 1; }

static segment *seg_for(raft_log *l, raft_index i)
{
	segment *segs = l->seg;
	for (unsigned k = l->n_seg; k-- > 0;) {
		segment *s = segs + k;
		if (i >= s->first)
			return i <= seg_last(s) ? s : NULL;
	}
	return NULL;
}

static void seg_reserve(segment *s, size_t n)
{
	if (s->n + n <= s->cap)
		return;
	size_t cap = s->cap ? s->cap : 1024;
	while (cap < s->n + n)
		cap *= 2;
	s->off = iso_xrealloc(s->off, cap * sizeof *s->off);
	s->term = iso_xrealloc(s->term, cap * sizeof *s->term);
	s->len = iso_xrealloc(s->len, cap * sizeof *s->len);
	s->cap = cap;
}

static void seg_free(segment *s)
{
	if (s->fd >= 0)
		close(s->fd);
	free(s->off);
	free(s->term);
	free(s->len);
	memset(s, 0, sizeof *s);
	s->fd = -1;
}

static void seg_name(char *buf, size_t n, raft_index first)
{
	snprintf(buf, n, "log-%020" PRIu64, first);
}

static iso_err sync_dir(raft_log *l, char *err, size_t errsize)
{
	int e = own_sync(l, l->dirfd, true);
	if (e) {
		log_fault(l, e, "the fsync of its directory");
		errno = e;
		return io_err(err, errsize, "fsync", log_name(l));
	}
	return ISO_OK;
}

/* ---- the read arena: what get() hands out, copied off the segments. A pointer into it stays
 * valid, and its bytes unchanged, until release() -- across appends, truncations, resets and
 * compactions, none of which touches a copy (plan 5a-i-g, #201). It used to be rewound at every
 * one of them, and the core queues actions that name those bytes across exactly those calls: a
 * proposal from inside an APPLY rewound it under the APPENDs queued for the followers, and a
 * snapshot inside an apply freed the block the next batch had been read into. */

static void *arena_alloc(raft_log *l, size_t n)
{
	arena_block *b = l->arena;
	if (!b || b->cap - b->used < n) {
		size_t cap = n > ARENA_BLOCK ? n : ARENA_BLOCK;
		arena_block *nb = iso_xmalloc(sizeof *nb + cap);
		nb->next = b;
		nb->used = 0;
		nb->cap = cap;
		l->arena = b = nb;
		l->arena_bytes += sizeof *nb + cap;
		if (l->arena_bytes > l->arena_peak)
			l->arena_peak = l->arena_bytes;
	}
	void *p = b->data + b->used;
	b->used += n;
	return p;
}

static void arena_reset(raft_log *l)
{
	arena_block *b = l->arena;
	while (b) {
		arena_block *next = b->next;
		if (next) {
			l->arena_bytes -= sizeof *b + b->cap;
			free(b);
		} else {
			b->used = 0;
			l->arena = b;
		}
		b = next;
	}
}

/* ---- segment files */

static void put_seg_header(uint8_t *h, raft_index first, raft_term prev_term, uint64_t nonce)
{
	memset(h, 0, SEG_HEADER);
	memcpy(h, SEG_MAGIC, 8);
	iso_put_u32le(h + 8, SEG_VERSION);
	iso_put_u64le(h + 16, first);
	iso_put_u64le(h + 24, prev_term);
	iso_put_u64le(h + 32, nonce);
	iso_put_u32le(h + 12, iso_crc32c_buf(h, SEG_HEADER));
}

static bool check_seg_header(const uint8_t *h, raft_index *first, raft_term *prev_term,
			     uint64_t *nonce)
{
	uint8_t copy[SEG_HEADER];
	memcpy(copy, h, SEG_HEADER);
	uint32_t crc = iso_get_u32le(copy + 12);
	iso_put_u32le(copy + 12, 0);
	if (memcmp(copy, SEG_MAGIC, 8) != 0 || iso_get_u32le(copy + 8) != SEG_VERSION ||
	    iso_crc32c_buf(copy, SEG_HEADER) != crc)
		return false;
	*first = iso_get_u64le(copy + 16);
	*prev_term = iso_get_u64le(copy + 24);
	*nonce = iso_get_u64le(copy + 32);
	return true;
}

/* A record's checksum covers its own header and payload, seeded with the segment's nonce -- so a
 * record cannot be read by a segment that did not write it. See the layout note at the top. */
static uint32_t rec_crc(uint64_t nonce, const uint8_t *rh, const void *data, uint32_t len)
{
	uint8_t n8[8];
	iso_put_u64le(n8, nonce);
	uint32_t c = iso_crc32c(0, n8, 8);
	c = iso_crc32c(c, rh + 4, REC_HEADER - 4);
	return iso_crc32c(c, data, len);
}

/* ---- the region
 *
 * The slot array replaces three things the directory was doing: the namespace (which segments
 * exist, and in what order), the allocator (room for a new one) and the length of each. The first
 * two are these helpers; the third is the terminator in `seg_append_at` and `seg_scan`. */

static uint64_t slot_base(const raft_log *l, uint64_t slot)
{
	return l->region_off + REG_SLOTS_OFF + slot * l->segment_bytes;
}

/* How many slots a region of this size holds, and whether it holds any at all. */
static uint64_t region_slots(uint64_t region_len, uint64_t segment_bytes)
{
	if (region_len <= REG_SLOTS_OFF || !segment_bytes)
		return 0;
	return (region_len - REG_SLOTS_OFF) / segment_bytes;
}

static void put_reg_header(uint8_t *h, uint64_t segment_bytes, uint64_t n_slots)
{
	memset(h, 0, REG_HEADER);
	memcpy(h, REG_MAGIC, 8);
	iso_put_u32le(h + 8, REG_VERSION);
	iso_put_u64le(h + 16, segment_bytes);
	iso_put_u64le(h + 24, n_slots);
	iso_put_u32le(h + REG_HEADER - 4, iso_crc32c_buf(h, REG_HEADER - 4));
}

/* Three answers, not two: a header that verifies, an all-zero header (the region was never
 * formatted, which is not damage -- the rule 94-b-i landed), or anything else, which is. */
typedef enum { REG_GOOD, REG_BLANK, REG_BAD } reg_state;

static reg_state check_reg_header(const uint8_t *h, uint64_t *segment_bytes, uint64_t *n_slots)
{
	if (memcmp(h, REG_MAGIC, 8) == 0 && iso_get_u32le(h + 8) == REG_VERSION &&
	    iso_crc32c_buf(h, REG_HEADER - 4) == iso_get_u32le(h + REG_HEADER - 4)) {
		*segment_bytes = iso_get_u64le(h + 16);
		*n_slots = iso_get_u64le(h + 24);
		return REG_GOOD;
	}
	for (size_t i = 0; i < REG_HEADER; i++)
		if (h[i])
			return REG_BAD;
	return REG_BLANK;
}

/* Whether a header that checks out gives a geometry this region can be opened with. The format
 * refuses a segment too small for a header, one record header and the terminator, so one here was
 * not written by it -- and `f_append`'s arithmetic on what a slot holds must not be handed one.
 * The open and `raft_log_region_discardable` ask it alike. */
static bool geometry_fits(uint64_t region_len, uint64_t segment_bytes, uint64_t n_slots)
{
	return segment_bytes >= SEG_HEADER + 2 * REC_HEADER &&
	       n_slots == region_slots(region_len, segment_bytes);
}

static iso_err seg_create(raft_log *l, raft_index first, raft_term prev_term, char *err,
			  size_t errsize)
{
	char name[64];
	seg_name(name, sizeof name, first);
	uint64_t nonce = 0;
	iso_random_bytes(&nonce, sizeof nonce);
	uint8_t h[SEG_HEADER + REC_HEADER] = { 0 };
	put_seg_header(h, first, prev_term, nonce);

	int fd;
	uint64_t base = 0, slot = 0;
	if (in_region(l)) {
		/* The allocator. The lowest free slot, so which one a given sequence of operations
		 * picks is a fact a test can assert rather than a detail of the heap. */
		while (slot < l->n_slots && l->slot_used[slot])
			slot++;
		if (slot == l->n_slots) {
			iso_err frc = iso_errf(
				err, errsize, ISO_ERR_FULL,
				"the raft log region on %s is full: all %" PRIu64
				" slots of %" PRIu64 " bytes hold a segment. The log is "
				"compacted by snapshots, so this means the region is too small "
				"for this node's write rate; reformat the device with a larger "
				"`--raft-bytes`.",
				log_name(l), l->n_slots, l->segment_bytes);
			errno = ENOSPC; /* the cause, for a reset that faults on it (#308) */
			return frc;
		}
		fd = l->region_fd;
		base = slot_base(l, slot);
		/* Header *and* the terminator that says the segment is empty, in one write: a slot
		 * that a previous occupant filled would otherwise read as holding its records. */
		bool wrote = pwrite(fd, h, sizeof h, (off_t)base) == (ssize_t)sizeof h;
		int e = wrote ? own_sync(l, fd, false) : (errno ? errno : EIO);
		if (e) {
			/* The region's one descriptor: a failed sync of it may have taken an error a
			 * page of the log met, which no later sync reports (#114, plan 28c). A write
			 * that failed leaves the slot free, as it was. */
			if (wrote)
				log_fault(l, e, "making a segment");
			errno = e;
			return io_err(err, errsize, "writing", log_name(l));
		}
	} else {
		fd = openat(l->dirfd, name, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
		if (fd < 0)
			return io_err(err, errsize, "creating", name);
		bool wrote = pwrite(fd, h, SEG_HEADER, 0) == SEG_HEADER;
		int e = wrote ? own_sync(l, fd, false) : (errno ? errno : EIO);
		if (e) {
			/* Nothing else is in this file, and it goes. A write that failed leaves the log
			 * as it was; a sync that failed faults it, as every failed sync does (#114). */
			if (wrote)
				log_fault(l, e, "making a segment");
			errno = e;
			iso_err rc = io_err(err, errsize, "writing", name);
			close(fd);
			unlinkat(l->dirfd, name, 0);
			errno = e; /* the cause, not the cleanup's: a reset faults on it (#308) */
			return rc;
		}
		iso_err rc = sync_dir(l, err, errsize);
		if (rc != ISO_OK) {
			int se = errno;
			close(fd);
			errno = se;
			return rc;
		}
	}

	if (l->n_seg == l->cap_seg) {
		l->cap_seg = l->cap_seg ? l->cap_seg * 2 : 8;
		l->seg = iso_xrealloc(l->seg, l->cap_seg * sizeof *l->seg);
	}
	segment *s = &l->seg[l->n_seg++];
	memset(s, 0, sizeof *s);
	s->first = first;
	s->prev_term = prev_term;
	s->nonce = nonce;
	s->fd = fd;
	s->base = base;
	s->slot = slot;
	s->size = SEG_HEADER;
#ifdef ISO_DEBUG
	s->handed = SEG_HEADER;
#endif
	if (in_region(l))
		l->slot_used[slot] = true;
	return ISO_OK;
}

/* Give a slot back: zero its header, and **fdatasync before returning**.
 *
 * That inline sync is the shape `sync_dir` has at every site that unlinks, and the reason survives
 * the move. `f_truncate` drops every segment starting at or after `from` and the very next append
 * creates one at `first == from`. If the zeroed header is not durable when the create picks a
 * different slot, a crash leaves two valid headers claiming one `first` and nothing says which is
 * newer (plan section 23, 94-c). */
static iso_err slot_release(raft_log *l, uint64_t slot, char *err, size_t errsize)
{
	uint8_t zero[SEG_HEADER] = { 0 };
	int e = pwrite(l->region_fd, zero, sizeof zero, (off_t)slot_base(l, slot)) ==
				(ssize_t)sizeof zero
			? own_sync(l, l->region_fd, false)
			: (errno ? errno : EIO);
	if (e) {
		/* **A fault, not a line** (#114, plan 28c). This sync is of the region's one
		 * descriptor, so a failure may be a write error of any page the log wrote -- an entry a
		 * round has yet to sync, which that round's sync would then not report -- and a header
		 * left valid is the second segment at one index the comment above is about. It used to
		 * be logged by the compaction and nothing else: the replica went on acknowledging. */
		log_fault(l, e, "giving a segment's slot back");
		errno = e;
		return io_err(err, errsize, "clearing a slot of", log_name(l));
	}
	l->slot_used[slot] = false;
	return ISO_OK;
}

static iso_err seg_delete_last(raft_log *l, char *err, size_t errsize)
{
	segment *s = last_seg(l);
	if (in_region(l)) {
		uint64_t slot = s->slot;
		s->fd = -1; /* borrowed from the region: seg_free must not close it */
		seg_free(s);
		l->n_seg--;
		return slot_release(l, slot, err, errsize);
	}
	char name[64];
	seg_name(name, sizeof name, s->first);
	seg_free(s);
	l->n_seg--;
	if (unlinkat(l->dirfd, name, 0) != 0) {
		/* A segment the log no longer holds is still on the disk, entries Raft said to drop
		 * among them, where the next open finds it. */
		int e = errno;
		log_fault(l, e, "deleting a segment");
		errno = e;
		return io_err(err, errsize, "deleting", name);
	}
	return ISO_OK;
}

/* Scan one segment's records into memory. A bad record in the last segment is a torn tail
 * and is cut off; anywhere else it is corruption. */
static iso_err seg_scan(raft_log *l, segment *s, bool is_last, const char *name, char *err,
			size_t errsize)
{
	uint64_t size, off = SEG_HEADER;
	if (in_region(l)) {
		/* **A slot has no length, so the bound is the slot and the end is a terminator.**
		 * Every append writes 32 zero bytes after its record and `seg_create` writes one at
		 * the start, so an all-zero record header is the ordinary end of the log. Without it
		 * the scan could not tell a healthy segment from a torn one -- both stop at a record
		 * that does not verify -- and every clean start would report a torn tail and have to
		 * zero the rest of the slot to be safe (plan section 23, 94-c). */
		size = l->segment_bytes;
	} else {
		struct stat st;
		if (fstat(s->fd, &st) != 0)
			return io_err(err, errsize, "stat", name);
		size = (uint64_t)st.st_size;
	}
	size_t bufcap = 1u << 20;
	uint8_t *buf = iso_xmalloc(bufcap);
	raft_index expect = s->first;
	raft_term prev_term = s->prev_term;
	iso_err rc = ISO_OK;
	const char *why = NULL;

	while (off < size) {
		if (size - off < REC_HEADER) {
			why = "a record header runs past the end of the file";
			break;
		}
		uint8_t rh[REC_HEADER];
		if (pread(s->fd, rh, REC_HEADER, (off_t)(s->base + off)) != REC_HEADER) {
			rc = io_err(err, errsize, "reading", name);
			break;
		}
		if (in_region(l)) {
			bool blank = true;
			for (unsigned b = 0; b < REC_HEADER && blank; b++)
				blank = rh[b] == 0;
			if (blank)
				break; /* the terminator: the log ends here, and cleanly */
		}
		uint32_t crc = iso_get_u32le(rh), len = iso_get_u32le(rh + 4);
		raft_index index = iso_get_u64le(rh + 8);
		raft_term term = iso_get_u64le(rh + 16);
		if (len > REC_MAX_LEN || size - off - REC_HEADER < len) {
			why = "a record runs past the end of the file";
			break;
		}
		if (len > bufcap) {
			bufcap = len;
			buf = iso_xrealloc(buf, bufcap);
		}
		if (len && pread(s->fd, buf, len, (off_t)(s->base + off + REC_HEADER)) != (ssize_t)len) {
			rc = io_err(err, errsize, "reading", name);
			break;
		}
		if (rec_crc(s->nonce, rh, buf, len) != crc) {
			why = "a record's checksum does not match";
			break;
		}
		if (index != expect || term < prev_term || rh[24] > RAFT_ENTRY_CONFIG) {
			why = "a record's index or term is out of sequence";
			break;
		}
		seg_reserve(s, 1);
		s->off[s->n] = off;
		s->term[s->n] = term;
		s->len[s->n] = len;
		s->n++;
		expect++;
		prev_term = term;
		off += REC_HEADER + len;
	}
	free(buf);
	if (rc != ISO_OK)
		return rc;
	if (why) {
		char remedy[1024];
		if (!is_last)
			return iso_errf(err, errsize, ISO_ERR_CORRUPT,
					"raft log %s/%s is corrupt at offset %" PRIu64 " (%s). %s",
					log_name(l), name, off, why, log_remedy(l, remedy, sizeof remedy));
		if (floor_covers(l, expect)) {
			/* Rot in what this replica acknowledged, not a tear: refused before
			 * anything is cut, so a restart refuses it again (plan 28h, P1m-15). */
			char what[256];
			snprintf(what, sizeof what,
				 "%s: the record at index %" PRIu64 " (offset %" PRIu64 ") does not "
				 "verify (%s)",
				 name, expect, off, why);
			return floor_refuse(l, what, err, errsize);
		}
		if (in_region(l)) {
			/* **A torn record does not mean the records after it were never written.**
			 * `f_append` issues one `pwritev` per entry without waiting, so the disk may
			 * have completed a later one and lost this one -- which would leave an intact
			 * record of *this* segment, with this nonce and the index the scan expects,
			 * sitting exactly where the next append will look. So the rest of the slot
			 * goes, not just the record that tore. A file gets this from `ftruncate`; a
			 * slot has to be told. It costs one segment-sized write, on a crash recovery
			 * path, once (plan section 23, 94-c). */
			LOG_WARN("raft log %s/%s: %s at offset %" PRIu64
				 "; the log ends there and the rest of the slot is cleared",
				 log_name(l), name, why, off);
			uint64_t rest = size - off;
			uint8_t *zeros = iso_xcalloc(1, rest);
			bool ok = pwrite(s->fd, zeros, rest, (off_t)(s->base + off)) == (ssize_t)rest &&
				  fdatasync(s->fd) == 0;
			free(zeros);
			if (!ok)
				return io_err(err, errsize, "clearing a torn tail of", log_name(l));
			l->torn_tail_bytes += rest;
		} else {
			LOG_WARN("raft log %s/%s: %s at offset %" PRIu64 "; cutting off %" PRIu64
				 " bytes written during the last crash",
				 log_name(l), name, why, off, size - off);
			l->torn_tail_bytes += size - off;
			if (ftruncate(s->fd, (off_t)off) != 0 || fdatasync(s->fd) != 0)
				return io_err(err, errsize, "truncating", name);
		}
	}
	/* `off` is where the records stop, which in a file is the file's size and in a slot is what
	 * the terminator marked. */
	s->size = off;
#ifdef ISO_DEBUG
	s->handed = off;
#endif
	return ISO_OK;
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

/* A segment continues the one before it when the indexes meet and the terms agree. */
static bool segs_join(const segment *p, const segment *s)
{
	return p->n > 0 && s->first == seg_last(p) + 1 && s->prev_term == p->term[p->n - 1];
}

static segment *seg_push(raft_log *l)
{
	if (l->n_seg == l->cap_seg) {
		l->cap_seg = l->cap_seg ? l->cap_seg * 2 : 8;
		l->seg = iso_xrealloc(l->seg, l->cap_seg * sizeof *l->seg);
	}
	segment *s = &l->seg[l->n_seg++];
	memset(s, 0, sizeof *s);
	return s;
}

/* One occupied slot, before it is scanned: the slot array is the namespace, and `first` is the
 * order the file names used to give. */
typedef struct {
	raft_index first;
	raft_term prev_term;
	uint64_t nonce;
	uint64_t slot;
} found_slot;

static int cmp_found(const void *a, const void *b)
{
	const found_slot *x = a, *y = b;
	return x->first < y->first ? -1 : x->first > y->first;
}

static iso_err open_region(raft_log *l, char *err, size_t errsize)
{
	found_slot *f = iso_xcalloc(l->n_slots ? l->n_slots : 1, sizeof *f);
	unsigned n = 0;
	iso_err rc = ISO_OK;

	for (uint64_t k = 0; k < l->n_slots; k++) {
		uint8_t h[SEG_HEADER];
		if (pread(l->region_fd, h, SEG_HEADER, (off_t)slot_base(l, k)) != SEG_HEADER) {
			rc = io_err(err, errsize, "reading a slot of", log_name(l));
			goto done;
		}
		raft_index first;
		raft_term prev_term;
		uint64_t nonce;
		if (!check_seg_header(h, &first, &prev_term, &nonce))
			continue; /* free: never written, cleared by compaction, or a torn create */
		l->slot_used[k] = true;
		f[n++] = (found_slot){ first, prev_term, nonce, k };
	}
	qsort(f, n, sizeof f[0], cmp_found);

	/* **Opened to be emptied**: the slots are the segments, in the order their headers
	 * give, and nothing in them is read -- the discard releases each one, the last first. */
	for (unsigned k = 0; l->discard_only && k < n; k++) {
		segment *s = seg_push(l);
		s->first = f[k].first;
		s->prev_term = f[k].prev_term;
		s->nonce = f[k].nonce;
		s->fd = l->region_fd;
		s->base = slot_base(l, f[k].slot);
		s->slot = f[k].slot;
		s->size = SEG_HEADER;
	}
	char remedy[1024];
	for (unsigned k = 0; !l->discard_only && k < n; k++) {
		if (k > 0 && f[k].first == f[k - 1].first) {
			/* Two live headers claiming one index. `slot_release` fdatasyncs precisely so
			 * this cannot be left behind, so seeing it means something else went wrong and
			 * guessing which is newer is not available -- there is no generation here. */
			rc = iso_errf(err, errsize, ISO_ERR_CORRUPT,
				      "the raft log on %s has two segments that both start at index %" PRIu64
				      " (slots %" PRIu64 " and %" PRIu64 "). %s",
				      log_name(l), f[k].first, f[k - 1].slot, f[k].slot,
				      log_remedy(l, remedy, sizeof remedy));
			goto done;
		}
		char name[64];
		seg_name(name, sizeof name, f[k].first);
		segment *s = seg_push(l);
		s->first = f[k].first;
		s->prev_term = f[k].prev_term;
		s->nonce = f[k].nonce;
		s->fd = l->region_fd;
		s->base = slot_base(l, f[k].slot);
		s->slot = f[k].slot;
		rc = seg_scan(l, s, k == n - 1, name, err, errsize);
		if (rc != ISO_OK)
			goto done;
		if (k > 0 && !segs_join(&l->seg[l->n_seg - 2], s)) {
			if (k == n - 1 && s->n == 0) {
				/* An empty tail segment left by a truncation: harmless, and the slot
				 * goes back. */
				uint64_t slot = s->slot;
				s->fd = -1;
				seg_free(s);
				l->n_seg--;
				rc = slot_release(l, slot, err, errsize);
				goto done;
			}
			rc = iso_errf(err, errsize, ISO_ERR_CORRUPT,
				      "the raft log on %s has a segment at index %" PRIu64
				      " that does not continue the one before it. %s",
				      log_name(l), s->first, log_remedy(l, remedy, sizeof remedy));
			goto done;
		}
	}
done:
	free(f);
	return rc;
}

static iso_err open_listed(raft_log *l, const uint64_t *firsts, size_t n, char *err,
			   size_t errsize);

/* Every segment in the directory, however many: the open read the first 1,024 names `readdir`
 * gave and dropped the rest without a word (#411). A
 * filesystem that lists oldest first (tmpfs on the fleet's 6.12) then lost the log's newest
 * entries -- durable, committed, applied -- and the next append found their first segment's name
 * taken ("File exists"); one that lists newest first lost its oldest ones; one in hash order
 * refused to open, the segments it kept not continuing one another. A log holds that many
 * segments when they are small: `mds.log_segment_bytes = 1` and `snapshot_every`'s 10,000 entries
 * before a compaction. */
static iso_err open_existing(raft_log *l, char *err, size_t errsize)
{
	int lfd = dup(l->dirfd);
	DIR *d = lfd >= 0 ? fdopendir(lfd) : NULL;
	if (!d) {
		if (lfd >= 0)
			close(lfd);
		return io_err(err, errsize, "listing", log_name(l));
	}
	uint64_t *firsts = NULL;
	size_t n = 0, cap = 0;
	struct dirent *e;
	while ((e = readdir(d)) != NULL) {
		uint64_t first;
		char extra;
		if (sscanf(e->d_name, "log-%" SCNu64 "%c", &first, &extra) != 1)
			continue;
		if (n == cap) {
			cap = cap ? cap * 2 : 64;
			firsts = iso_xrealloc(firsts, cap * sizeof *firsts);
		}
		firsts[n++] = first;
	}
	closedir(d);
	if (n)
		qsort(firsts, n, sizeof firsts[0], cmp_u64);
	iso_err rc = open_listed(l, firsts, n, err, errsize);
	free(firsts);
	return rc;
}

/* Open the segments `firsts` names, in order (sorted): each scanned, joined to the one before. */
static iso_err open_listed(raft_log *l, const uint64_t *firsts, size_t n, char *err,
			   size_t errsize)
{
	char remedy[1024];
	for (size_t k = 0; k < n; k++) {
		if (l->discard_only) {
			/* Opened to be emptied: a segment is its name, and the discard
			 * deletes it by that name, the last first. Nothing is opened or read. */
			segment *s = seg_push(l);
			s->first = firsts[k];
			s->fd = -1;
			continue;
		}
		char name[64];
		seg_name(name, sizeof name, firsts[k]);
		int fd = openat(l->dirfd, name, O_RDWR | O_CLOEXEC);
		if (fd < 0)
			return io_err(err, errsize, "opening", name);
		uint8_t h[SEG_HEADER];
		raft_index first;
		raft_term prev_term;
		uint64_t nonce;
		ssize_t got = pread(fd, h, SEG_HEADER, 0);
		bool ok = got == SEG_HEADER && check_seg_header(h, &first, &prev_term, &nonce) &&
			  first == firsts[k];
		if (!ok) {
			close(fd);
			if (k == n - 1 && k > 0 && floor_covers(l, firsts[k])) {
				/* Not created during the last crash: it holds what this
				 * replica acknowledged, so it is not deleted (plan 28h, P1m-15). */
				char what[256];
				snprintf(what, sizeof what,
					 "%s: the segment that starts at index %" PRIu64
					 " has no valid header",
					 name, firsts[k]);
				return floor_refuse(l, what, err, errsize);
			}
			if (k == n - 1 && k > 0) {
				/* Created during the last crash, before its header landed. */
				LOG_WARN("raft log %s/%s has no valid header; removing it", log_name(l), name);
				unlinkat(l->dirfd, name, 0);
				break;
			}
			return iso_errf(err, errsize, ISO_ERR_CORRUPT,
					"raft log %s/%s has a bad header. %s", log_name(l), name,
					log_remedy(l, remedy, sizeof remedy));
		}
		segment *s = seg_push(l);
		s->first = first;
		s->prev_term = prev_term;
		s->nonce = nonce;
		s->fd = fd;
		iso_err rc = seg_scan(l, s, k == n - 1, name, err, errsize);
		if (rc != ISO_OK)
			return rc;
		if (k > 0) {
			if (!segs_join(&l->seg[l->n_seg - 2], s)) {
				if (k == n - 1 && s->n == 0) {
					/* An empty tail segment left by a truncation: harmless. */
					char gone[64];
					seg_name(gone, sizeof gone, s->first);
					seg_free(s);
					l->n_seg--;
					unlinkat(l->dirfd, gone, 0);
					break;
				}
				return iso_errf(err, errsize, ISO_ERR_CORRUPT,
						"raft log %s/%s does not continue the previous segment. %s",
						log_name(l), name, log_remedy(l, remedy, sizeof remedy));
			}
		}
	}
	return ISO_OK;
}

/* The geometry a region whose header a reseed cannot use is laid out again with -- the carve's --
 * or the words it is refused in: a region smaller than RAFT_LOG_REGION_MIN, which a label made
 * before #161 can have, cannot be laid out again (`raft_log_region_discardable`, and
 * `header_remedy` for why the refusal names no reseed). */
static iso_err relay_carve(const char *name, uint64_t region_len, uint64_t want_segment_bytes,
			   const char *why, uint64_t *seg_bytes, char *err, size_t errsize)
{
	char cerr[512] = "";
	iso_err rc = raft_log_carve(region_len, want_segment_bytes, seg_bytes, cerr, sizeof cerr);
	if (rc != ISO_OK)
		return iso_errf(err, errsize, rc,
				"the raft log header on %s %s, and the region cannot be laid out "
				"again: %s",
				name, why, cerr);
	return ISO_OK;
}

/* **A region's header that a reseed cannot use is laid out again** (plan section 23). The
 * reseed's open (`discard_only`) keeps nothing of the log but the hard state, which sits at a fixed
 * offset of its own, so a header that does not check out -- or checks out and does not fit the
 * region the label gives -- costs it nothing but the geometry. That is the carve's, from the
 * region's length and the configured ceiling, as `isod mds init` lays a region out; the slots of
 * that geometry whose headers verify are the segments to release, and the header itself is written
 * by the discard once they have gone, after the vote is fenced. Nothing is written here. */
static iso_err relay_geometry(raft_log *l, uint64_t want_segment_bytes, const char *why, char *err,
			      size_t errsize)
{
	uint64_t seg_bytes = 0;
	iso_err rc = relay_carve(log_name(l), l->region_len, want_segment_bytes, why, &seg_bytes, err,
				 errsize);
	if (rc != ISO_OK)
		return rc;
	l->segment_bytes = seg_bytes;
	l->n_slots = region_slots(l->region_len, seg_bytes);
	l->slot_used = iso_xcalloc(l->n_slots ? l->n_slots : 1, sizeof *l->slot_used);
	l->relay_header = true;
	LOG_WARN("%s: the raft log header %s; the log is being emptied, and the region is laid out "
		 "again in %" PRIu64 " slots of %" PRIu64 " bytes, as `isod mds init` lays one out",
		 log_name(l), why, l->n_slots, seg_bytes);
	return ISO_OK;
}

/* Read the region header and set the geometry from it. **The region's header wins over the
 * caller's `segment_bytes`**, which is the upper bound a config expresses and not a fact about this
 * device: the slots were laid out at format and moving the boundary now would move every segment.
 * A difference is worth one line, not a refusal. */
static iso_err region_geometry(raft_log *l, uint64_t want_segment_bytes, char *err, size_t errsize)
{
	uint8_t h[REG_HEADER];
	if (pread(l->region_fd, h, REG_HEADER, (off_t)l->region_off) != REG_HEADER)
		return io_err(err, errsize, "reading the header of", log_name(l));

	uint64_t seg_bytes = 0, n_slots = 0;
	char remedy[1024];
	switch (check_reg_header(h, &seg_bytes, &n_slots)) {
	case REG_BLANK:
		return iso_errf(err, errsize, ISO_ERR_NOTFOUND,
				"%s has no raft log in it yet. `isod mds init` makes one; an open never "
				"formats, because formatting is a verb.",
				log_name(l));
	case REG_BAD:
		if (l->discard_only)
			return relay_geometry(l, want_segment_bytes, "does not check out", err,
					      errsize);
		return iso_errf(err, errsize, ISO_ERR_CORRUPT,
				"the raft log header on %s does not check out. %s", log_name(l),
				header_remedy(l, remedy, sizeof remedy));
	case REG_GOOD: break;
	}

	if (!geometry_fits(l->region_len, seg_bytes, n_slots)) {
		if (l->discard_only)
			return relay_geometry(l, want_segment_bytes, "does not fit the region", err,
					      errsize);
		return iso_errf(err, errsize, ISO_ERR_CORRUPT,
				"the raft log header on %s says %" PRIu64 " slots of %" PRIu64
				" bytes, which does not fit the %" PRIu64 " bytes it has. %s",
				log_name(l), n_slots, seg_bytes, l->region_len,
				header_remedy(l, remedy, sizeof remedy));
	}
	if (want_segment_bytes && want_segment_bytes != seg_bytes)
		LOG_INFO("%s: the raft log was formatted with %" PRIu64
			 " byte segments and the configuration asks for %" PRIu64
			 "; the region's own geometry is what is used",
			 log_name(l), seg_bytes, want_segment_bytes);
	/* **A region carved before #161 can hold slots smaller than an entry**, and nothing about it
	 * is wrong until such an entry arrives -- when this node refuses it: a leader turns the
	 * proposal away and a follower's replica halts. Said at every open, while it is only a
	 * risk, because the remedy is a reformat and that is best done before it is needed. */
	if (seg_bytes < RAFT_LOG_SEGMENT_MIN)
		LOG_WARN("%s: the raft log's segments are %" PRIu64 " bytes and hold an entry of at "
			 "most %" PRIu64 ", where replication can carry one of %" PRIu64
			 ": a larger one is refused here, and a follower sent one halts its replica. "
			 "Where the group has other voters, stop the node, reformat the device with "
			 "`isod mds init --force` (which carves segments of at least %" PRIu64
			 " bytes) and start it to be re-fed by the leader; a group's only voter holds "
			 "the only copy of its metadata, so add a second voter first.",
			 log_name(l), seg_bytes, seg_bytes - SEG_HEADER - 2 * REC_HEADER,
			 (uint64_t)RAFT_WIRE_ENTRY_MAX, (uint64_t)RAFT_LOG_SEGMENT_MIN);

	l->segment_bytes = seg_bytes;
	l->n_slots = n_slots;
	l->slot_used = iso_xcalloc(n_slots ? n_slots : 1, sizeof *l->slot_used);
	return ISO_OK;
}

iso_err raft_log_open(const char *dir, const raft_log_opts *opts, raft_index base_index,
		      raft_term base_term, raft_log **out, char *err, size_t errsize)
{
	*out = NULL;
	bool region = opts && opts->region_len != 0;

	raft_log *l = iso_xcalloc(1, sizeof *l);
	l->dirfd = -1;
	l->hard_fd = -1;
	l->floor_fd = -1;
	l->floor_every_ns = opts && opts->floor_every_ns ? opts->floor_every_ns : 1000000000ull;
	l->segment_bytes = opts && opts->segment_bytes ? opts->segment_bytes : DEFAULT_SEGMENT;
	if (l->segment_bytes < SEG_HEADER + REC_HEADER)
		l->segment_bytes = SEG_HEADER + REC_HEADER;
	l->remedy = opts ? opts->remedy : NULL;
	l->discard_only = opts && opts->discard_only;

	iso_err rc;
	if (region) {
		l->region_off = opts->region_off;
		l->region_len = opts->region_len;
		l->region_name = iso_xstrdup(opts->region_name ? opts->region_name
							       : "the metadata device");
		/* **A description of the log's own** on the vdev's device (#114, plan 28c): the
		 * metadata store in the same device writes through another, and each must hear every
		 * write error the disk reports -- a shared description reports one to whichever syncs
		 * first. Closed with the log. */
		l->region_fd = iso_fd_reopen(opts->region_fd, O_RDWR);
		if (l->region_fd < 0) {
			int e = errno;
			rc = iso_errf(err, errsize, ISO_ERR_IO,
				      "cannot open a descriptor of the raft log's own on %s "
				      "(/proc/self/fd/%d): %s. The log needs one apart from the "
				      "metadata store's so that each hears every write error the disk "
				      "reports%s",
				      l->region_name, opts->region_fd, strerror(e),
				      iso_fd_reopen_remedy(e));
			raft_log_close(l);
			return rc;
		}
		/* **And the hard state another** (#305, plan 28c). Its ring is inside the region, and
		 * it used to be written through the log's description -- which every sync round on the
		 * ring holds a duplicate of. Two syncs of one description in flight at once take one
		 * write error between them, so a round could take the error of a term or a vote just
		 * written, the save return 0, and the vote go out before the round's failure halted
		 * the replica: a vote a power cut then forgets. Opened now, before any write, so it
		 * hears every error the device reports from here on. */
		l->hard_fd = iso_fd_reopen(opts->region_fd, O_RDWR);
		if (l->hard_fd < 0) {
			int e = errno;
			rc = iso_errf(err, errsize, ISO_ERR_IO,
				      "cannot open a descriptor of the raft hard state's own on %s "
				      "(/proc/self/fd/%d): %s. The term and the vote need one apart "
				      "from the log's sync rounds so that each hears every write error "
				      "the disk reports%s",
				      l->region_name, opts->region_fd, strerror(e),
				      iso_fd_reopen_remedy(e));
			raft_log_close(l);
			return rc;
		}
		/* The floor before any segment is read: the scan asks it (plan 28h, P1m-15). */
		rc = floor_load(l, err, errsize);
		if (rc == ISO_OK)
			rc = region_geometry(l, opts->segment_bytes, err, errsize);
		if (rc == ISO_OK)
			rc = open_region(l, err, errsize);
	} else {
		if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
			rc = io_err(err, errsize, "creating", dir);
			raft_log_close(l);
			return rc;
		}
		/* **Its own entry durable** (#129, plan 28c), made now or by a life a power cut
		 * ended: the segments below sync the directory they are in, and that makes nothing
		 * of the directory itself durable. Once an open. */
		int de = iso_fsync_parent(dir);
		if (de) {
			rc = iso_errf(err, errsize, ISO_ERR_IO,
				      "%s: syncing the directory that holds it: %s. " ISO_FSYNC_REMEDY, dir,
				      strerror(de));
			raft_log_close(l);
			return rc;
		}
		l->dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
		if (l->dirfd < 0) {
			rc = io_err(err, errsize, "opening", dir);
			raft_log_close(l);
			return rc;
		}
		l->dir = iso_xstrdup(dir);
		rc = floor_load(l, err, errsize);
		if (rc == ISO_OK)
			rc = open_existing(l, err, errsize);
	}
	if (rc == ISO_OK && !l->discard_only)
		rc = floor_check_end(l, base_index, err, errsize);

	/* A log opened to be emptied is left without a segment when it has none: the discard makes
	 * the one it starts again on, and nothing is written before the caller's fence. */
	if (rc == ISO_OK && l->n_seg == 0 && !l->discard_only)
		rc = seg_create(l, base_index + 1, base_term, err, errsize);
	l->remedy = NULL; /* the caller's, and only for the open */
	if (rc != ISO_OK) {
		raft_log_close(l);
		return rc;
	}
	*out = l;
	return ISO_OK;
}

void raft_log_close(raft_log *l)
{
	if (!l)
		return;
	for (unsigned k = 0; k < l->n_seg; k++) {
		if (in_region(l))
			l->seg[k].fd = -1; /* the region's descriptor, closed below */
		seg_free(&l->seg[k]);
	}
	free(l->seg);
	arena_block *b = l->arena;
	while (b) {
		arena_block *next = b->next;
		free(b);
		b = next;
	}
	if (l->hard_fd >= 0)
		close(l->hard_fd);
	if (l->floor_fd >= 0)
		close(l->floor_fd);
	if (in_region(l) && l->region_fd >= 0)
		close(l->region_fd);
	if (l->dirfd >= 0)
		close(l->dirfd);
	free(l->slot_used);
	free(l->region_name);
	free(l->dir);
	free(l);
}

/* ---- the vtable */

static raft_index f_first(void *log)
{
	raft_log *l = log;
	return l->n_seg ? l->seg[0].first : l->bare_index + 1;
}

static raft_index f_last(void *log)
{
	raft_log *l = log;
	if (!l->n_seg)
		return l->bare_index;
	segment *s = last_seg(l);
	return s->n ? seg_last(s) : s->first - 1;
}

static raft_term f_term_at(void *log, raft_index i)
{
	raft_log *l = log;
	if (!l->n_seg)
		return i == l->bare_index ? l->bare_term : 0;
	if (i == l->seg[0].first - 1)
		return l->seg[0].prev_term;
	segment *s = seg_for(l, i);
	return s ? s->term[i - s->first] : 0;
}

static unsigned f_get(void *log, raft_index lo, raft_index hi, size_t max_bytes, raft_entry *out,
		      unsigned max_n)
{
	raft_log *l = log;
	unsigned n = 0;
	size_t bytes = 0;
	for (raft_index i = lo; i <= hi && n < max_n; i++) {
		segment *s = seg_for(l, i);
		if (!s)
			break;
		size_t k = i - s->first;
		uint32_t len = s->len[k];
		if (n && bytes + len > max_bytes)
			break;
		uint8_t *buf = arena_alloc(l, len + REC_HEADER);
		ssize_t got = pread(s->fd, buf, len + REC_HEADER, (off_t)(s->base + s->off[k]));
		if (got != (ssize_t)(len + REC_HEADER)) {
			LOG_ERR("raft log %s: reading index %" PRIu64 " failed: %s", log_name(l), i,
				got < 0 ? strerror(errno) : "short read");
			break;
		}
		out[n++] = (raft_entry){ .index = i, .term = s->term[k], .type = buf[24],
					 .len = len, .data = buf + REC_HEADER };
		bytes += len;
	}
	return n;
}

#ifdef ISO_DEBUG
unsigned raft_log_test_cut_at_roll;
const char *raft_log_test_cut_at_roll_file;
bool raft_log_test_cut_done;

/* Whether the cut seam is armed for this roll (#296, plan 28c): by a count, or by a file the cut
 * removes. Asked only at a roll that leaves a tail no sync has been asked of, so an armed seam
 * waits for one. */
static bool cut_armed(void)
{
	if (raft_log_test_cut_at_roll) {
		raft_log_test_cut_at_roll--;
		return true;
	}
	return raft_log_test_cut_at_roll_file && unlink(raft_log_test_cut_at_roll_file) == 0;
}

/* **A power cut, as the disk could leave it, the moment after a roll** (#296, plan 28c): every
 * segment but the last loses what no sync was asked of -- what was written after the round or the
 * sync that last covered it, which nothing has made durable -- and the last keeps all of it, as a
 * writeback that ran ahead would. Legal for any disk: what is kept was written, what is lost was
 * never synced. Armed by the file, the process then ends where a power cut would end it; armed by
 * the count, it returns, and the test closes the log and opens it again. */
static void power_cut(raft_log *l, bool by_file)
{
	uint64_t lost = 0;
	for (unsigned k = 0; k + 1 < l->n_seg; k++) {
		segment *s = &l->seg[k];
		if (s->size > s->handed && ftruncate(s->fd, (off_t)s->handed) == 0)
			lost += s->size - s->handed;
	}
	raft_log_test_cut_done = true;
	LOG_WARN("raft log %s: a power cut simulated by the test seam right after a roll: %llu "
		 "bytes no sync was asked of are lost from the segments before the last%s",
		 log_name(l), (unsigned long long)lost, by_file ? ", and the process ends" : "");
	if (by_file)
		_exit(86);
}
#endif

static iso_err f_append(void *log, const raft_entry *es, unsigned n)
{
	raft_log *l = log;
#ifdef ISO_DEBUG
	bool cut = false, by_file = false;
#endif
	/* A log with no segment is one whose reset faulted: nothing goes in it. */
	if (n && !l->n_seg)
		return ISO_ERR_IO;
	for (unsigned i = 0; i < n; i++) {
		const raft_entry *e = &es[i];
		segment *s = last_seg(l);
		raft_index last = f_last(l);
		if (e->index != last + 1 || e->len > REC_MAX_LEN)
			return ISO_ERR_INVAL;
		/* The terminator has to fit too, or the next scan of a brim-full slot would run off
		 * the end of it rather than meeting an end marker. */
		uint64_t need = REC_HEADER + (uint64_t)e->len + (in_region(l) ? REC_HEADER : 0);
		/* **A record no slot can hold is refused, never written** (plan section 23, #161).
		 * The roll below starts a fresh segment for a record that does not fit the current
		 * one -- but only when the current one holds records, so a record larger than a whole
		 * slot went into an empty one from its start and ran on over the slots after it. A
		 * file grows to hold it; a slot does not, and every replica refused to start at its
		 * next open. `raft_log_carve` sizes every slot a node formats to hold any entry a
		 * leader can replicate, so this is met only on a region carved before #161 or by a
		 * test's own geometry; the owner hears why from `raft_log_take_refusal`. */
		if (in_region(l) && SEG_HEADER + need > l->segment_bytes) {
			l->refused = (raft_log_refusal){ .index = e->index,
							 .bytes = e->len,
							 .most = l->segment_bytes - SEG_HEADER -
								 2 * REC_HEADER };
			l->has_refused = true;
			return ISO_ERR_TOOBIG;
		}
		if (s->n && s->size + need > l->segment_bytes) {
#ifdef ISO_DEBUG
			/* The cut seam: at a roll that leaves a tail no sync was asked of (#296). */
			by_file = raft_log_test_cut_at_roll == 0;
			cut = !in_region(l) && s->size > s->handed && cut_armed();
#endif
			/* **The segment rolled from is on the disk before the next one is made** (#296,
			 * plan 28c). In a directory each segment is a file, and the round that would
			 * sync this one's tail syncs the next one in a separate `fdatasync` -- which
			 * writeback can run ahead of -- so a power cut could keep the next segment's
			 * first entry and lose this one's tail: a log with a gap, which the next open
			 * refuses, and a replica that needs a reseed. Synced always, not only when
			 * written since the last round: a round may still be on its way. One sync a
			 * roll, of a segment of 64 MiB by default. It orders this tail before the next
			 * segment's first entry and stands in for no round's: the segment stays `dirty`,
			 * so the next round still hands it out (#264's rounds are as they were) and pays
			 * one sync of a clean file for it, off the loop. A region needs none: the next
			 * segment's header is synced through the device, which writes this one's tail
			 * back with it. */
			if (!in_region(l)) {
				int e2 = own_sync(l, s->fd, false);
				if (e2) {
					log_fault(l, e2, "syncing the segment a roll leaves");
					return ISO_ERR_IO;
				}
			}
			char err[256];
			iso_err rc = seg_create(l, e->index, s->term[s->n - 1], err, sizeof err);
			if (rc != ISO_OK) {
				LOG_ERR("%s", err);
				return rc;
			}
			s = last_seg(l);
		}
		uint8_t rh[REC_HEADER] = { 0 };
		iso_put_u32le(rh + 4, e->len);
		iso_put_u64le(rh + 8, e->index);
		iso_put_u64le(rh + 16, e->term);
		rh[24] = e->type;
		iso_put_u32le(rh, rec_crc(s->nonce, rh, e->data, e->len));

		/* **The terminator**: 32 zero bytes after the record, so a scan of this slot can tell
		 * the end of the log from a record that does not verify. It rides the same `pwritev`
		 * -- no extra syscall, no extra fsync, and exactly as durable as the record it
		 * follows. A file does not need it: `st_size` says where the records stop, and a
		 * trailing 32 bytes would make every clean segment look torn (section 23, 94-c). */
		static const uint8_t terminator[REC_HEADER] = { 0 };
		struct iovec iov[3] = { { rh, REC_HEADER },
					{ (void *)e->data, e->len },
					{ (void *)terminator, REC_HEADER } };
		int n_iov = e->len ? 2 : 1;
		if (in_region(l))
			iov[n_iov++] = (struct iovec){ (void *)terminator, REC_HEADER };
		ssize_t want = REC_HEADER + (ssize_t)e->len + (in_region(l) ? REC_HEADER : 0);
		bool refused = false;
#ifdef ISO_DEBUG
		/* The seam: a no-op's write refused as a disk refuses one, standing in for its result,
		 * so the failure is taken back below as a disk's is. */
		if (e->type == RAFT_ENTRY_NOOP && raft_log_test_fail_noop_file &&
		    unlink(raft_log_test_fail_noop_file) == 0) {
			LOG_WARN("raft log %s: a fault injected by the test seam refuses the no-op at "
				 "index %" PRIu64,
				 log_name(l), e->index);
			errno = EIO;
			refused = true;
		}
#endif
		if (refused || pwritev(s->fd, iov, n_iov, (off_t)(s->base + s->size)) != want) {
			LOG_ERR("raft log %s: appending index %" PRIu64 " failed: %s", log_name(l),
				e->index, strerror(errno));
			/* Leave no half record behind: the next append would sit after it. */
			/* And if that cannot be done either, the half record stays where the next
			 * append would sit after it: a fault, not a line (#114, plan 28c). */
			if (in_region(l)) {
				uint8_t zero[REC_HEADER] = { 0 };
				if (pwrite(s->fd, zero, sizeof zero, (off_t)(s->base + s->size)) !=
				    (ssize_t)sizeof zero)
					log_fault(l, errno, "clearing a failed append");
			} else if (ftruncate(s->fd, (off_t)s->size) != 0) {
				log_fault(l, errno, "truncating a failed append");
			}
			return ISO_ERR_IO;
		}
		seg_reserve(s, 1);
		s->off[s->n] = s->size;
		s->term[s->n] = e->term;
		s->len[s->n] = e->len;
		s->n++;
		/* The records, not the terminator: `size` is where the next record goes, and the
		 * terminator is what the next record overwrites. */
		s->size += REC_HEADER + (uint64_t)e->len;
		s->dirty = true;
#ifdef ISO_DEBUG
		if (cut) {
			power_cut(l, by_file);
			cut = false;
		}
#endif
	}
	return ISO_OK;
}

static iso_err f_truncate(void *log, raft_index from)
{
	raft_log *l = log;
	if (!l->n_seg)
		return ISO_ERR_IO; /* a reset faulted: there is nothing to cut */
	if (from < l->seg[0].first)
		return ISO_ERR_INVAL;
	if (from > f_last(l))
		return ISO_OK;
	char err[256];
	if (floor_lower(l, from - 1, f_term_at(l, from - 1), err, sizeof err) != ISO_OK) {
		LOG_ERR("%s", err);
		return ISO_ERR_IO; /* the log faulted */
	}
	/* Drop whole segments that start at or after `from`, then cut the one that holds it. */
	while (l->n_seg > 1 && last_seg(l)->first >= from) {
		iso_err rc = seg_delete_last(l, err, sizeof err); /* a failure faulted the log */
		if (rc != ISO_OK)
			return rc;
	}
	segment *s = last_seg(l);
	if (from <= seg_last(s)) {
		size_t keep = from - s->first;
		uint64_t off = s->off[keep];
		if (in_region(l)) {
			/* **Zero the whole suffix, not just a marker at the cut.**
			 *
			 * On a filesystem `ftruncate` makes the dropped entries cease to exist. A slot
			 * keeps them, and the next append writes over the cut -- so if the new record
			 * happens to be the same length as the old one, the scan's next stop is exactly
			 * where the old *following* record sits: this segment's nonce, a valid
			 * checksum, and precisely the index the scan expects. `seg_scan` also refuses
			 * `term < prev_term` and a conflicting suffix is always overwritten by a higher
			 * term, so that check does reject it -- but that argument reaches through
			 * one-leader-per-term and append-only-within-a-term to conclude that a byte
			 * pattern cannot be misread, and a log format should not rest on it. These
			 * bytes are exactly the entries Raft said to drop, so they go. Bounded by one
			 * segment, and truncation happens at a leadership change, not on the write path
			 * (plan section 23, 94-c).
			 *
			 * The zeroing also leaves a terminator at the cut, because a dropped record is
			 * at least REC_HEADER long. */
			uint64_t rest = s->size - off;
			uint8_t *zeros = iso_xcalloc(1, rest);
			bool ok = pwrite(s->fd, zeros, rest, (off_t)(s->base + off)) == (ssize_t)rest;
			int e = ok ? 0 : errno;
			free(zeros);
			if (!ok) {
				/* The entries Raft said to drop are still readable where the next
				 * append writes: the log is not what the core is told (#114). */
				log_fault(l, e, "clearing the entries a truncation dropped");
				return ISO_ERR_IO;
			}
		} else if (ftruncate(s->fd, (off_t)off) != 0) {
			log_fault(l, errno, "truncating a segment");
			return ISO_ERR_IO;
		}
		s->n = keep;
		s->size = off;
		s->dirty = true;
	}
	/* A file syncs its directory, because segments were unlinked. A region has already synced
	 * each slot it released, inline, for the reason `slot_release` gives; the zeroed suffix
	 * above rides the dirty flag to the next sync round, exactly as `ftruncate` does. */
	if (in_region(l))
		return ISO_OK;
	return sync_dir(l, err, sizeof err); /* a failure faulted the log */
}

#ifdef ISO_DEBUG
unsigned raft_log_test_fail_reset_after;
const char *raft_log_test_fail_reset_file;
const char *raft_log_test_fail_noop_file;
#endif

/* Every segment goes, the last first, and one is started on `index + 1` with `term` before it.
 * Deleting from the end keeps what is left a prefix of the log at every step, so a reset that stops
 * part way -- a crash, a failed unlink or slot release -- leaves a log that still reads, and that
 * the next reset empties.
 *
 * The durable floor goes to 0 on the platter first, whatever `index` is (#483, P1m-15, plan 28h):
 * a reset keeps none of the old log, so the old floor protects nothing in the new one, while a
 * reset stopped part way leaves a prefix that may end below it -- an install's reset is above the
 * floor, and a crash between its deletes used to leave a log the next open refused as rot (found
 * by review). One write of the hard state per reset that finds a floor; none when there is none. */
static iso_err log_reset(raft_log *l, raft_index index, raft_term term, char *err, size_t errsize)
{
	unsigned deleted = 0;
	(void)deleted; /* read only by the debug build's seam */
	iso_err frc = floor_lower(l, 0, 0, err, errsize);
	if (frc != ISO_OK)
		return frc;
	l->bare_index = index;
	l->bare_term = term;
	while (l->n_seg) {
#ifdef ISO_DEBUG
		if (raft_log_test_fail_reset_after && deleted == raft_log_test_fail_reset_after) {
			raft_log_test_fail_reset_after = 0;
			return iso_errf(err, errsize, ISO_ERR_IO,
					"%s: a test seam stopped the reset after %u segment(s)",
					log_name(l), deleted);
		}
#endif
		iso_err rc = seg_delete_last(l, err, errsize);
		if (rc != ISO_OK)
			return rc;
		deleted++;
	}
	if (l->relay_header) {
		/* The region's header, laid out again (`relay_geometry`): after every slot of
		 * its geometry that held a segment has been released, and before the one the log
		 * starts again on is made, so that a crash between the two leaves a region that opens
		 * empty rather than one whose header names slots still holding the old log. */
		uint8_t h[REG_HEADER];
		put_reg_header(h, l->segment_bytes, l->n_slots);
		int e = pwrite(l->region_fd, h, REG_HEADER, (off_t)l->region_off) == REG_HEADER
				? own_sync(l, l->region_fd, false)
				: (errno ? errno : EIO);
		if (e) {
			log_fault(l, e, "laying the region's header out again");
			errno = e;
			return io_err(err, errsize, "writing the raft log header on", log_name(l));
		}
		l->relay_header = false;
	}
	/* **A reset that cannot make its segment faults the log** (#308, plan 28c), however
	 * the making failed -- a failed sync faulted it already; a create refused at the descriptor
	 * limit, a full disk, a header that would not write did not. A roll's new segment that fails
	 * so changes nothing (the segment before it is still the last), but here every segment has
	 * gone: the log holds nothing, and the core had acknowledged the install anyway, and read the
	 * last index of a log that had none. */
	iso_err rc;
#ifdef ISO_DEBUG
	/* The seam: the create refused as at the descriptor limit, standing in for its result. */
	if (raft_log_test_fail_reset_file && unlink(raft_log_test_fail_reset_file) == 0) {
		LOG_WARN("raft log %s: a fault injected by the test seam refuses the segment a reset "
			 "starts the log on",
			 log_name(l));
		errno = EMFILE;
		rc = io_err(err, errsize, "creating the segment a reset starts it on, in", log_name(l));
	} else
#endif
		rc = seg_create(l, index + 1, term, err, errsize);
	if (rc != ISO_OK) {
		/* `seg_create` leaves the cause in errno on every failure, its cleanup's calls aside. */
		int e = errno;
		log_fault(l, e ? e : EIO, "making the segment a reset starts the log on");
	}
	return rc;
}

static iso_err f_reset(void *log, raft_index index, raft_term term)
{
	char err[256];
	iso_err rc = log_reset(log, index, term, err, sizeof err);
	if (rc != ISO_OK)
		LOG_ERR("%s", err);
	return rc;
}

iso_err raft_log_discard(raft_log *l, char *err, size_t errsize)
{
	return log_reset(l, 0, 0, err, errsize);
}

static iso_err f_compact(void *log, raft_index upto)
{
	raft_log *l = log;
	unsigned drop = 0;
	while (drop + 1 < l->n_seg && l->seg[drop].n && seg_last(&l->seg[drop]) <= upto)
		drop++;
	if (!drop)
		return ISO_OK;
	iso_err rc = ISO_OK;
	char err[256];
	/* **Every failure here faults the log** (#114, plan 28c): a slot given back is a sync of the
	 * region's one descriptor, which a write error of any page the log wrote may have failed,
	 * and its next round would not hear of it. It used to be logged, returned to a core that
	 * did not look, and the replica went on acknowledging. */
	for (unsigned k = 0; k < drop; k++) {
		if (in_region(l)) {
			uint64_t slot = l->seg[k].slot;
			l->seg[k].fd = -1; /* borrowed */
			seg_free(&l->seg[k]);
			if (slot_release(l, slot, err, sizeof err) != ISO_OK)
				rc = ISO_ERR_IO;
			continue;
		}
		char name[64];
		seg_name(name, sizeof name, l->seg[k].first);
		seg_free(&l->seg[k]);
		if (unlinkat(l->dirfd, name, 0) != 0) {
			log_fault(l, errno, "deleting a compacted segment");
			rc = ISO_ERR_IO;
		}
	}
	memmove(l->seg, l->seg + drop, (l->n_seg - drop) * sizeof *l->seg);
	l->n_seg -= drop;
	/* A region synced each slot as it released it; a directory syncs once at the end. */
	if (!in_region(l) && sync_dir(l, err, sizeof err) != ISO_OK)
		rc = ISO_ERR_IO;
	return rc;
}

/* Nothing the core handed out is named any more: the arena is rewound, and the next get() writes
 * where the first did. */
static void f_release(void *log) { arena_reset(log); }

const raft_log_ops raft_log_file_ops = {
	.first = f_first, .last = f_last, .term_at = f_term_at, .get = f_get,
	.append = f_append, .truncate = f_truncate, .reset = f_reset, .compact = f_compact,
	.release = f_release,
};

/* ---- durability */

#ifdef ISO_DEBUG
unsigned raft_log_test_sync_fds;
unsigned raft_log_test_fail_inline_sync;
const char *raft_log_test_fail_inline_file;
uint64_t raft_log_test_say_every_ns;
#endif

/* One fdatasync a round makes on the spot rather than hand out, of the descriptor `fd` holding
 * entries `first` to `last` (none when `last < first`, a segment's header alone): 0, or the
 * errno it failed with. */
static int sync_on_the_spot(const raft_log *l, int fd, raft_index first, raft_index last)
{
	int rc = fdatasync(fd);
	int e = rc != 0 ? (errno ? errno : EIO) : 0;
#ifdef ISO_DEBUG
	/* The seam stands in for the call's result, so that what a test fails is the branch a disk's
	 * error takes. Armed by a count, or by a file the failure removes (an unlink that finds it is
	 * the test-and-clear, and needs no descriptor). */
	bool armed = raft_log_test_fail_inline_sync ||
		     (raft_log_test_fail_inline_file && unlink(raft_log_test_fail_inline_file) == 0);
	if (armed) {
		if (raft_log_test_fail_inline_sync)
			raft_log_test_fail_inline_sync--;
		LOG_WARN("raft log %s: a fault injected by the test seam fails the sync made on the "
			 "spot of entries %llu to %llu",
			 log_name(l), (unsigned long long)first, (unsigned long long)last);
		rc = -1;
		e = EIO;
	}
#endif
	if (rc != 0) {
		LOG_ERR("raft log %s: the sync of entries %llu to %llu failed: %s", log_name(l),
			(unsigned long long)first, (unsigned long long)last, strerror(e));
		return e;
	}
	return 0;
}

/* How many descriptors a round hands out before it syncs the rest on the spot. */
static unsigned round_fds(const raft_log_sync *out)
{
	unsigned cap = ISO_ARRAY_LEN(out->fds);
#ifdef ISO_DEBUG
	if (raft_log_test_sync_fds && raft_log_test_sync_fds < cap)
		cap = raft_log_test_sync_fds;
#endif
	return cap;
}

/* How often a round that could not duplicate a descriptor is said, at most. */
#define NO_DUP_SAY_NS (60ull * 1000000000ull)

/* A round could not duplicate a descriptor (#264, plan 26z): the process is at its descriptor
 * limit, and every round until it is not will say the same. So the first is said, and then at most
 * one line a minute with how many since the line before -- never one a sync. The interval is a
 * comparison, never `now - said`, which a clock read before the line was said would underflow. */
static void no_dup_say(raft_log *l, int e)
{
	l->syncs_no_dup++;
	l->no_dup_unsaid++;
	uint64_t every = NO_DUP_SAY_NS;
#ifdef ISO_DEBUG
	if (raft_log_test_say_every_ns)
		every = raft_log_test_say_every_ns;
#endif
	uint64_t now = iso_now_ns();
	if (l->no_dup_said && now < l->no_dup_said_ns + every)
		return;
	/* Both limits: a node raised its soft limit to the hard one as it started (#265), so the
	 * hard one is what an operator raises; `ulimit -n` would name what the node already did. */
	struct rlimit rl;
	unsigned long long lim = 0, hard = 0;
	if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
		lim = (unsigned long long)rl.rlim_cur;
		hard = (unsigned long long)rl.rlim_max;
	}
	unsigned long long n = l->no_dup_unsaid;
	LOG_WARN("raft log %s: %llu sync%s had no descriptor to spare (%s), so %s made on the spot, "
		 "holding up the loop that asked: this process is at its descriptor limit, %llu "
		 "(RLIMIT_NOFILE; the hard limit is %llu). Raise the hard limit -- LimitNOFILE= in "
		 "the service's unit, `ulimit -Hn` where it is started; a node raises its soft limit "
		 "to it as it starts -- or find what holds them in /proc/%d/fd",
		 log_name(l), n, n == 1 ? "" : "s", strerror(e), n == 1 ? "it was" : "they were", lim,
		 hard, (int)getpid());
	l->no_dup_said = true;
	l->no_dup_said_ns = now;
	l->no_dup_unsaid = 0;
}

/* One descriptor of a round, `fd`, holding entries `first` to `last`. While the round has room it
 * hands out a duplicate, so that the sync can wait on the ring while the log goes on: a compaction
 * or a truncation closes the segment's own descriptor, and a number closed and reused would sync
 * another file. Otherwise -- the round full, or no duplicate to be had -- `fd` is synced here,
 * where it is certainly open (#264). A sync made here that fails fails the round: the first
 * failure's errno goes in `err`, and nothing the round covers may be reported persisted. */
static void round_take(raft_log *l, raft_log_sync *out, int fd, raft_index first, raft_index last)
{
	if (out->n < round_fds(out)) {
		int d = fcntl(fd, F_DUPFD_CLOEXEC, 0);
		if (d >= 0) {
			out->fds[out->n++] = d;
			return;
		}
		no_dup_say(l, errno ? errno : EMFILE);
	}
	l->syncs_inline++;
	int e = sync_on_the_spot(l, fd, first, last);
	if (e && !out->err)
		out->err = e;
}

iso_err raft_log_sync_prepare(raft_log *l, raft_log_sync *out)
{
	out->n = 0;
	out->err = 0;
	if (l->faulted) {
		/* A log that faulted reports nothing durable again (#114, plan 28c), whatever its
		 * descriptors would say now: the failed sync may have taken the error. Asked first: a
		 * reset whose new segment failed leaves no segment to ask the last index of. */
		out->index = 0;
		out->term = 0;
		out->err = l->fault_errno;
		return ISO_ERR_IO;
	}
	out->index = f_last(l);
	out->term = f_term_at(l, out->index);
	if (in_region(l)) {
		/* Every segment is in the same device, so eight duplicates of one descriptor would
		 * be eight fdatasyncs of the same thing. One, if anything was touched at all. */
		bool any = false;
		raft_index first = 0, last = 0;
		for (unsigned k = 0; k < l->n_seg; k++) {
			segment *s = &l->seg[k];
			if (s->dirty && !any)
				first = s->first;
			if (s->dirty)
				last = s->first + s->n - 1;
			any |= s->dirty;
			s->dirty = false;
		}
		if (any)
			round_take(l, out, l->region_fd, first, last);
		floor_hand(l, out);
		return out->err ? ISO_ERR_IO : ISO_OK;
	}
	for (unsigned k = 0; k < l->n_seg; k++) {
		segment *s = &l->seg[k];
		if (!s->dirty)
			continue;
		s->dirty = false;
#ifdef ISO_DEBUG
		s->handed = s->size;
#endif
		round_take(l, out, s->fd, s->first, s->first + s->n - 1);
	}
	floor_hand(l, out);
	return out->err ? ISO_ERR_IO : ISO_OK;
}

iso_err raft_log_sync_run(raft_log_sync *s)
{
	/* A sync the round made on the spot that failed is the round's failure, whatever the
	 * descriptors it handed out say. */
	iso_err rc = s->err ? ISO_ERR_IO : ISO_OK;
	for (unsigned i = 0; i < s->n; i++) {
		if (s->fds[i] < 0 || fdatasync(s->fds[i]) != 0)
			rc = ISO_ERR_IO;
		if (s->fds[i] >= 0)
			close(s->fds[i]);
	}
	s->n = 0;
	return rc;
}

/* ---- hard state
 *
 * Slot, 4 KiB: magic[8], u32 crc32c of bytes [12, 40), u64 seq, u64 term, u32 vote, zeros to 40;
 * then the vote fence (raft_hard_state; plan 5a-i-g, L's demotion's repair): u64 fence_index,
 * u64 fence_term, and a u32 crc32c of bytes [40, 56); then the durable floor (#483, P1m-15, plan
 * 28h): u64 floor_index, u64 floor_term, and a u32 crc32c of bytes [60, 76); zeros after.
 *
 * The floor is kept as the fence is, for the same reason: twenty zero bytes are no floor -- what
 * every older build writes, and a downgrade's save loses the floor, which only gives back the
 * truncation of a bad record that the floor would have refused.
 *
 * The fence has a checksum of its own so that the first stays what it was: an older build reads
 * the slot as it always did, and ignores the fence. Twenty zero bytes are a slot with no fence --
 * what every older build wrote. A slot whose second checksum fails is not valid, as a torn one is
 * not, and the other slot is read. */

/* Where the two slots live: the head of a file of their own, or a fixed offset in the region. */
static uint64_t hard_base(const raft_log *l)
{
	return in_region(l) ? l->region_off + REG_HARD_OFF : 0;
}

static bool check_slot(const uint8_t *p, uint64_t *seq, raft_hard_state *hs, raft_index *floor,
		       raft_term *floor_term)
{
	if (memcmp(p, HARD_MAGIC, 8) != 0 || iso_get_u32le(p + 8) != iso_crc32c_buf(p + 12, 28))
		return false;
	static const uint8_t none[20];
	bool fenced = memcmp(p + 40, none, sizeof none) != 0;
	if (fenced && iso_get_u32le(p + 56) != iso_crc32c_buf(p + 40, 16))
		return false;
	bool floored = memcmp(p + 60, none, sizeof none) != 0;
	if (floored && iso_get_u32le(p + 76) != iso_crc32c_buf(p + 60, 16))
		return false;
	*floor = floored ? iso_get_u64le(p + 60) : 0;
	*floor_term = floored ? iso_get_u64le(p + 68) : 0;
	*seq = iso_get_u64le(p + 12);
	hs->term = iso_get_u64le(p + 20);
	hs->vote = iso_get_u32le(p + 28);
	hs->fence_index = fenced ? iso_get_u64le(p + 40) : 0;
	hs->fence_term = fenced ? iso_get_u64le(p + 48) : 0;
	return true;
}

/* The ring's newest valid slot: its hard state, sequence and floor (`floor` and `floor_term` may
 * be NULL). `*any` false when neither slot is valid. ISO_ERR_IO: the read failed. */
static iso_err hard_read_floor(int fd, uint64_t base, raft_hard_state *out, uint64_t *seq_out,
			       bool *any, raft_index *floor, raft_term *floor_term)
{
	uint8_t buf[2 * HARD_SLOT];
	ssize_t got = pread(fd, buf, sizeof buf, (off_t)base);
	if (got < 0)
		return ISO_ERR_IO;
	*any = false;
	for (unsigned i = 0; i < 2; i++) {
		if ((size_t)got < (i + 1) * HARD_SLOT)
			break;
		uint64_t seq;
		raft_hard_state hs;
		raft_index f;
		raft_term ft;
		if (check_slot(buf + i * HARD_SLOT, &seq, &hs, &f, &ft) && (!*any || seq > *seq_out)) {
			*any = true;
			*seq_out = seq;
			*out = hs;
			if (floor)
				*floor = f;
			if (floor_term)
				*floor_term = ft;
		}
	}
	return ISO_OK;
}

static iso_err hard_read(int fd, uint64_t base, raft_hard_state *out, uint64_t *seq_out, bool *any)
{
	return hard_read_floor(fd, base, out, seq_out, any, NULL, NULL);
}

iso_err raft_log_probe_dir(const char *dir, raft_log_dir_info *out, char *err, size_t errsize)
{
	memset(out, 0, sizeof *out);

	DIR *d = opendir(dir);
	if (!d) {
		/* A node that has never run has no raft directory, and that is an answer rather
		 * than a failure -- the caller's whole question is whether this node has a log. */
		if (errno == ENOENT)
			return ISO_OK;
		return io_err(err, errsize, "listing", dir);
	}

	struct dirent *e;
	while ((e = readdir(d)) != NULL) {
		uint64_t first;
		char extra;
		/* The same match `load_segments` uses: `%c` catches a trailing anything, so
		 * `log-7.tmp` is not a segment. */
		if (sscanf(e->d_name, "log-%" SCNu64 "%c", &first, &extra) != 1)
			continue;
		if (out->segments == 0 || first < out->first)
			out->first = first;
		out->segments++;
	}
	closedir(d);
	return ISO_OK;
}

iso_err raft_log_hard_load(const char *dir, raft_hard_state *out, char *err, size_t errsize)
{
	char path[4096];
	if (snprintf(path, sizeof path, "%s/hard", dir) >= (int)sizeof path)
		return iso_errf(err, errsize, ISO_ERR_INVAL, "raft directory path too long");
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		if (errno == ENOENT)
			return ISO_ERR_NOTFOUND;
		return io_err(err, errsize, "opening", path);
	}
	struct stat st;
	bool any = false;
	uint64_t seq = 0;
	iso_err rc = fstat(fd, &st) != 0 ? ISO_ERR_IO : hard_read(fd, 0, out, &seq, &any);
	close(fd);
	if (rc != ISO_OK)
		return iso_errf(err, errsize, rc, "reading %s failed", path);
	if (!any)
		return iso_errf(err, errsize, ISO_ERR_CORRUPT,
				"%s holds no valid hard state (%lld bytes). This node's term and vote "
				"are lost; do not guess them. Restore the file from a backup, or remove "
				"this node from the metadata group and add it back as a new member.",
				path, (long long)st.st_size);
	return ISO_OK;
}

/* The hard state as the disk has it now, and the ring's sequence with it: opened on first use. */
static iso_err hard_current(raft_log *l, raft_hard_state *cur, bool *any, char *err, size_t errsize)
{
	memset(cur, 0, sizeof *cur);
	*any = false;
	if (l->hard_fd < 0) {
		l->hard_fd = openat(l->dirfd, "hard", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
		if (l->hard_fd < 0) {
			iso_err rc = io_err(err, errsize, "opening", "hard");
			return rc != ISO_OK ? rc : ISO_ERR_IO; /* whatever errno said */
		}
	}
	uint64_t seq = 0;
	if (hard_read(l->hard_fd, hard_base(l), cur, &seq, any) != ISO_OK)
		return io_err(err, errsize, "reading", in_region(l) ? log_name(l) : "hard");
	l->hard_seq = *any ? seq : 0;
	l->hard_seq_known = true;
	l->hard_any = *any;
	if (*any)
		l->hard_last = *cur;
	return ISO_OK;
}

/* One slot of the ring, in `slot` (HARD_SLOT bytes): `hs` and the floor, at sequence `seq`. */
static void hard_slot_fill(uint8_t *slot, uint64_t seq, const raft_hard_state *hs, raft_index floor,
			   raft_term floor_term)
{
	memset(slot, 0, HARD_SLOT);
	memcpy(slot, HARD_MAGIC, 8);
	iso_put_u64le(slot + 12, seq);
	iso_put_u64le(slot + 20, hs->term);
	iso_put_u32le(slot + 28, hs->vote);
	iso_put_u32le(slot + 8, iso_crc32c_buf(slot + 12, 28));
	if (hs->fence_index) {
		iso_put_u64le(slot + 40, hs->fence_index);
		iso_put_u64le(slot + 48, hs->fence_term);
		iso_put_u32le(slot + 56, iso_crc32c_buf(slot + 40, 16));
	}
	if (floor) {
		iso_put_u64le(slot + 60, floor);
		iso_put_u64le(slot + 68, floor_term);
		iso_put_u32le(slot + 76, iso_crc32c_buf(slot + 60, 16));
	}
}

/*
 * hard_settle
 *		Make a floor's slot that a round in flight carries durable now, before the ring is
 *		written again.
 *
 * The ring survives a torn write because the slot a write overwrites is the one before the
 * newest, and the newest is durable. A floor's slot is not until its round ends, so the next
 * write would overwrite the only durable slot with the newest still in the cache, and a power cut
 * between the two could leave neither. Through `hard_fd`, which syncs the slot whichever
 * descriptor wrote it, and hears its own errors (#305). ISO_ERR_IO with `err` set on a failed
 * sync; the caller's write is then not made.
 */
static iso_err hard_settle(raft_log *l, char *err, size_t errsize)
{
	if (!l->floor_handed)
		return ISO_OK;
	if (fdatasync(l->hard_fd) != 0)
		return io_err(err, errsize, "syncing", in_region(l) ? log_name(l) : "hard");
	l->floor = l->floor_handed;
	l->floor_term = l->floor_handed_term;
	l->floor_handed = 0;
	l->floor_handed_term = 0;
	return ISO_OK;
}

/* Write and sync the ring's next slot: `hs` with the floor given, through `hard_fd`. */
static iso_err hard_write(raft_log *l, const raft_hard_state *hs, raft_index floor,
			  raft_term floor_term, char *err, size_t errsize)
{
	uint64_t seq = l->hard_seq + 1;
	uint8_t slot[HARD_SLOT];
	hard_slot_fill(slot, seq, hs, floor, floor_term);
	off_t off = (off_t)(hard_base(l) + (seq % 2) * HARD_SLOT);
	if (pwrite(l->hard_fd, slot, HARD_SLOT, off) != HARD_SLOT || fdatasync(l->hard_fd) != 0)
		return io_err(err, errsize, "writing", in_region(l) ? log_name(l) : "hard");
	l->hard_seq = seq;
	l->hard_last = *hs;
	l->hard_any = true;
	l->floor = floor;
	l->floor_term = floor_term;
	return ISO_OK;
}

iso_err raft_log_hard_save(raft_log *l, const raft_hard_state *hs, char *err, size_t errsize)
{
	if (!l->hard_seq_known) {
		raft_hard_state cur;
		bool any = false;
		iso_err rc = hard_current(l, &cur, &any, err, errsize);
		if (rc != ISO_OK)
			return rc;
	}
	iso_err hrc = hard_settle(l, err, errsize);
	if (hrc != ISO_OK)
		return hrc;
	uint64_t seq = l->hard_seq + 1;
	hrc = hard_write(l, hs, l->floor, l->floor_term, err, errsize);
	if (hrc != ISO_OK)
		return hrc;
	if (seq == 1 && !in_region(l)) {
		/* The first write creates the file, so its name has to be durable too. A region's
		 * slots were placed at format and there is no name to sync. */
		iso_err rc = sync_dir(l, err, errsize);
		if (rc != ISO_OK)
			return rc;
	}
	return ISO_OK;
}

iso_err raft_log_hard_fence(raft_log *l, raft_term term, raft_index fence_index,
			    raft_term fence_term, raft_hard_state *out, char *err, size_t errsize)
{
	if (fence_index == 0 || fence_term == 0 || fence_term > term)
		return iso_errf(err, errsize, ISO_ERR_INVAL,
				"a vote fence names an entry and its term, from a leader's term at "
				"or past it (asked for %llu/%llu in term %llu)",
				(unsigned long long)fence_index, (unsigned long long)fence_term,
				(unsigned long long)term);
	raft_hard_state cur;
	bool any = false;
	iso_err rc = hard_current(l, &cur, &any, err, errsize);
	if (rc != ISO_OK)
		return rc;
	if (!any) {
		/* Never written, or written and then lost: only the first may be fenced from
		 * nothing. A term and a vote that did exist are not guessed at
		 * (raft_log_hard_load). */
		uint8_t buf[2 * HARD_SLOT];
		ssize_t got = pread(l->hard_fd, buf, sizeof buf, (off_t)hard_base(l));
		if (got < 0)
			return io_err(err, errsize, "reading", in_region(l) ? log_name(l) : "hard");
		const char *what = in_region(l) ? log_name(l) : "raft/hard";
		for (ssize_t i = 0; i < got; i++)
			if (buf[i])
				return iso_errf(err, errsize, ISO_ERR_CORRUPT,
						"%s holds no valid hard state, so this node's "
						"term and vote are lost; they are not guessed at, "
						"and nothing is fenced over them",
						what);
	}
	if (term > cur.term) {
		cur.term = term;
		cur.vote = 0; /* a term it had not reached, so one it had not voted in */
	}
	cur.fence_index = fence_index;
	cur.fence_term = fence_term;
	rc = raft_log_hard_save(l, &cur, err, errsize);
	if (rc == ISO_OK && out)
		*out = cur;
	return rc;
}

/* ---- the durable floor (#483, P1m-15, plan 28h)
 *
 * A record in the last segment that does not verify used to be taken for a torn tail and cut --
 * a file truncated at it, a region's slot zeroed from it on -- even when it had been synced and
 * acknowledged long before the crash. A replica that forgets what it acknowledged can, with one
 * more lost voter, form a quorum that never held a committed entry. So the ring names a floor:
 * the newest index known both durable (every completed round covers it, raft_log_durable) and
 * committed (raft_log_floor_due). Committed, because a follower's conflict truncation may cut an
 * acknowledged entry that is not, and the floor must never name what a power cut may then tear;
 * durable, because a follower's commit can run ahead of its own sync. A floor too low gives back
 * today's truncation; one too high would halt a healthy replica after a power cut, so every bound
 * errs low.
 *
 * Written lazily, at most once `floor_every_ns`, by a sync round: the prepare writes the slot
 * through `floor_fd` and hands a duplicate out with the segments (`floor_hand`), so the loop never
 * waits on it. The slot names a floor that was durable before it was written, so the order the
 * round's syncs complete in does not matter. The ring stays whole because no slot is written
 * while a floor's is in flight without syncing it first (`hard_settle`, and `floor_slot_due`
 * waits for the round). Anything that would leave the log below the floor lowers it durably
 * first (`floor_lower`): a truncation (never, with a correct core, below the commit) to the
 * truncation's point, and every reset -- an install, a reseed's discard -- to 0, because a reset's
 * deletes cut short by a crash leave a prefix that may end below any floor.
 *
 * At open, before anything is cut: a bad record at or below the floor in the last segment, a last
 * segment at or below it with no valid header, and a log that ends below it are refused as
 * ISO_ERR_CORRUPT with the owner's remedy (`floor_refuse`), which a metadata group turns into a
 * replica that halts alone at start (#117). A reseed's open (`discard_only`) loads the floor, so
 * that its discard lowers it, and checks nothing: it reads no record and no segment header past
 * the slot or name that places it, and raft_log_open does not call `floor_check_end` for it, so
 * none of the checks below is reached from it. */

/* Whether the floor claims `index`: the ring names a floor at or past it. */
static bool floor_covers(const raft_log *l, raft_index index)
{
	return l->floor && index <= l->floor;
}

/* The open's refusal of what the floor covers: `what` says where, and the owner's remedy ends it
 * (the health reason that cuts a long line keeps the first sentence and the last). */
static iso_err floor_refuse(raft_log *l, const char *what, char *err, size_t errsize)
{
	char remedy[1024];
	return iso_errf(err, errsize, ISO_ERR_CORRUPT,
			"raft log %s is damaged where this replica made it durable and acknowledged "
			"it: %s, at or below index %" PRIu64 " (term %" PRIu64 "), which the hard "
			"state names as synced and committed. That is rot or a lying disk, not a "
			"write torn by a crash, so it is not cut: cutting it would forget an "
			"acknowledged write. %s",
			log_name(l), what, l->floor, l->floor_term, log_remedy(l, remedy, sizeof remedy));
}

/*
 * floor_load
 *		Read the ring's floor, before any segment is scanned.
 *
 * In a region `hard_fd` is open already; a directory's `hard` is opened here without creating it
 * -- a node that never saved a term has none, and an empty file would read as a lost hard state
 * (raft_log_hard_load). No file, or no valid slot, is no floor. Caches the newest slot for the
 * floor's own slots to carry. ISO_ERR_IO with `err` set when the ring cannot be read.
 */
static iso_err floor_load(raft_log *l, char *err, size_t errsize)
{
	if (l->hard_fd < 0) {
		int fd = openat(l->dirfd, "hard", O_RDWR | O_CLOEXEC);
		if (fd < 0)
			return errno == ENOENT ? ISO_OK : io_err(err, errsize, "opening", "hard");
		l->hard_fd = fd;
	}
	raft_hard_state hs;
	uint64_t seq = 0;
	bool any = false;
	if (hard_read_floor(l->hard_fd, hard_base(l), &hs, &seq, &any, &l->floor, &l->floor_term) !=
	    ISO_OK)
		return io_err(err, errsize, "reading", in_region(l) ? log_name(l) : "hard");
	l->hard_seq = any ? seq : 0;
	l->hard_seq_known = true;
	l->hard_any = any;
	if (any)
		l->hard_last = hs;
	return ISO_OK;
}

/* After the scan: a log that ends below the floor lost what this replica acknowledged -- a region's
 * record header rotted to zeros reads as the terminator, a rotted segment header as a free slot,
 * and a vanished last file as a shorter log, and each scans clean. A log with no segment ends at
 * `base_index`, the store's applied index, which holds everything at or below it. */
static iso_err floor_check_end(raft_log *l, raft_index base_index, char *err, size_t errsize)
{
	raft_index last = l->n_seg ? f_last(l) : base_index;
	if (!floor_covers(l, last + 1))
		return ISO_OK;
	char what[160];
	snprintf(what, sizeof what, "the log ends at index %" PRIu64, last);
	return floor_refuse(l, what, err, errsize);
}

/*
 * floor_lower
 *		Lower the floor to `keep` (whose term is `keep_term`) on the platter before the log is
 *		cut to it.
 *
 * Called by a truncation from `keep + 1`, and by every reset with `keep` 0, before either removes
 * anything: after them the log may hold new, unsynced entries past `keep`, which a power cut may
 * tear and a floor above `keep` would then refuse; and a reset's deletes, cut short, may leave a
 * prefix that ends anywhere below the floor (log_reset). Nothing is written when the floor, and a
 * floor handed to a round in flight, are already at or below it, which is every truncation a
 * correct core makes (the floor is at most the commit). `durable` and `floor_want` are lowered
 * too, so a report or a want older than the cut cannot raise the floor over entries written
 * after it. On a failed write the log faults and ISO_ERR_IO is returned with `err` set: the caller
 * cuts nothing.
 */
static iso_err floor_lower(raft_log *l, raft_index keep, raft_term keep_term, char *err,
			   size_t errsize)
{
	if (l->durable > keep)
		l->durable = keep;
	if (l->floor_want > keep) {
		l->floor_want = 0;
		l->floor_want_term = 0;
	}
	if (l->floor <= keep && l->floor_handed <= keep)
		return ISO_OK;
	iso_err rc = hard_settle(l, err, errsize);
	if (rc == ISO_OK)
		rc = hard_write(l, &l->hard_last, keep, keep ? keep_term : 0, err, errsize);
	if (rc != ISO_OK)
		log_fault(l, errno ? errno : EIO, "lowering the durable floor in the hard state");
	return rc;
}

/* Whether a round is to carry a floor's slot now: a valid slot to carry the term and vote of, a
 * floor higher than the ring's, none in flight, and `floor_every_ns` since the last. The interval
 * is a comparison, never `now - at`, which an earlier clock read would underflow. */
static bool floor_slot_due(const raft_log *l, uint64_t now_ns)
{
	return l->hard_any && l->hard_seq_known && !l->faulted && !l->floor_handed &&
	       l->floor_want > l->floor && now_ns >= l->floor_at_ns + l->floor_every_ns;
}

/*
 * floor_hand
 *		Write a floor's slot into the ring and hand it to the round `out` is.
 *
 * Only into a round with room for one more descriptor -- the floor waits for the next round
 * rather than be synced on the spot. A descriptor that cannot be opened leaves the floor for the
 * next round too. A write that fails faults the log and fails the round (`out->err`): the device
 * is failing, as any write the log makes outside a round says.
 */
static void floor_hand(raft_log *l, raft_log_sync *out)
{
	if (out->n >= round_fds(out) || !l->hard_any || l->floor_want <= l->floor || l->floor_handed)
		return;
	uint64_t now = iso_now_ns();
	if (!floor_slot_due(l, now))
		return;
	if (l->floor_fd < 0) {
		l->floor_fd = in_region(l) ? iso_fd_reopen(l->hard_fd, O_RDWR)
					   : openat(l->dirfd, "hard", O_RDWR | O_CLOEXEC);
		if (l->floor_fd < 0)
			return;
	}
	uint64_t seq = l->hard_seq + 1;
	uint8_t slot[HARD_SLOT];
	hard_slot_fill(slot, seq, &l->hard_last, l->floor_want, l->floor_want_term);
	off_t off = (off_t)(hard_base(l) + (seq % 2) * HARD_SLOT);
	if (pwrite(l->floor_fd, slot, HARD_SLOT, off) != HARD_SLOT) {
		int e = errno ? errno : EIO;
		log_fault(l, e, "writing the durable floor into the hard state");
		if (!out->err)
			out->err = e;
		return;
	}
	l->hard_seq = seq;
	l->floor_handed = l->floor_want;
	l->floor_handed_term = l->floor_want_term;
	l->floor_at_ns = now;
	/* XXX In a region `floor_fd` is a second description of the device the round's one
	 * segment descriptor already flushes, so a round that carries both makes two device cache
	 * flushes where one would do: when the round has a dirty segment, the slot could ride that
	 * descriptor (fdatasync writes back the whole mapping, and errseq reports a writeback error
	 * to every description). Left until a real disk measures what the second costs (plan 28h,
	 * P1m-15, *Cost*). */
	round_take(l, out, l->floor_fd, l->floor_want, l->floor_want);
}

/*
 * raft_log_durable
 *		Every sync round is done: the log is durable through `index`/`term`.
 *
 * On the owner, from the place that reports raft_persisted once no round is in flight. A floor's
 * slot handed to any of those rounds is on the platter now. The index is taken only when the log
 * still holds it with that term -- a truncation since the round was made cut it -- and is one
 * bound of the floor; raft_log_floor_due gives the other.
 */
void raft_log_durable(raft_log *l, raft_index index, raft_term term)
{
	if (l->floor_handed) {
		l->floor = l->floor_handed;
		l->floor_term = l->floor_handed_term;
		l->floor_handed = 0;
		l->floor_handed_term = 0;
	}
	if (index > l->durable && l->n_seg && index <= f_last(l) && f_term_at(l, index) == term)
		l->durable = index;
}

/*
 * raft_log_floor_due
 *		Note the replica's commit index; true when a floor's slot is due and no round would
 *		otherwise carry it.
 *
 * The floor due is the lower of `commit` and what raft_log_durable last took. True means the
 * caller, with no round in flight, is to start one (raft_log_sync_prepare writes the slot), so an
 * idle replica's floor catches up within `floor_every_ns`; under load the next round carries it
 * anyway.
 */
bool raft_log_floor_due(raft_log *l, raft_index commit, uint64_t now_ns)
{
	raft_index want = l->durable < commit ? l->durable : commit;
	if (want > l->floor_want && l->n_seg) {
		raft_term t = f_term_at(l, want);
		if (t) {
			l->floor_want = want;
			l->floor_want_term = t;
		}
	}
	return floor_slot_due(l, now_ns);
}

/*
 * raft_log_floor
 *		The floor the ring holds on the platter now, and its term in `*term` when `term` is not
 *		NULL; 0 (and term 0) when there is none.
 *
 * A slot handed to a round in flight is not counted until raft_log_durable says the round is
 * done. On the owner.
 */
raft_index raft_log_floor(const raft_log *l, raft_term *term)
{
	if (term)
		*term = l->floor_term;
	return l->floor;
}

void raft_log_stat(const raft_log *l, raft_log_stats *out)
{
	memset(out, 0, sizeof *out);
	out->segments = l->n_seg;
	for (unsigned k = 0; k < l->n_seg; k++) {
		out->bytes += l->seg[k].size;
		out->entries += l->seg[k].n;
	}
	out->first = f_first((void *)l);
	out->last = f_last((void *)l);
	out->torn_tail_bytes = l->torn_tail_bytes;
	out->slots_total = l->n_slots;
	for (uint64_t k = 0; k < l->n_slots; k++)
		if (!l->slot_used[k])
			out->slots_free++;
	out->segment_bytes = l->segment_bytes;
	out->arena_bytes = l->arena_bytes;
	out->arena_peak = l->arena_peak;
	out->syncs_inline = l->syncs_inline;
	out->syncs_no_dup = l->syncs_no_dup;
	out->floor = l->floor;
	out->floor_handed = l->floor_handed;
}

/* ---- the region as a format
 *
 * Making one is a verb, not something an open does. That rule came out of 94-b-i: a store that
 * formats what it is pointed at is a store that eats the wrong disk, and the difference between
 * "never formatted" and "damaged" is the one an operator needs most. */

iso_err raft_log_carve(uint64_t region_len, uint64_t configured, uint64_t *segment_bytes,
		       char *err, size_t errsize)
{
	*segment_bytes = 0;
	/* **Room is the remedy, and the only one.** A smaller `log_segment_bytes` used to be named
	 * here too, and it is what made a slot smaller than an entry (#161). */
	if (region_len < RAFT_LOG_REGION_MIN)
		return iso_errf(err, errsize, ISO_ERR_INVAL,
				"a raft log region of %" PRIu64 " bytes is too small: a raft log "
				"needs at least %" PRIu64 " -- %u segments of %" PRIu64
				" bytes, each able to hold the largest entry replication can carry "
				"(%" PRIu64 " bytes), behind %" PRIu64
				" bytes of headers. Give the log at least that with `isod mds init "
				"--raft-bytes`, on a metadata device with room for the store beside it.",
				region_len, (uint64_t)RAFT_LOG_REGION_MIN, RAFT_LOG_REGION_SLOTS,
				(uint64_t)RAFT_LOG_SEGMENT_MIN, (uint64_t)RAFT_WIRE_ENTRY_MAX,
				(uint64_t)RAFT_LOG_REGION_HEAD);

	uint64_t ceiling = configured ? configured : DEFAULT_SEGMENT;
	if (ceiling < RAFT_LOG_SEGMENT_MIN) {
		LOG_INFO("`[mds] log_segment_bytes` is %" PRIu64 ", and a raft log segment in a region "
			 "holds at least %" PRIu64 " so that the largest entry replication can carry "
			 "(%" PRIu64 " bytes) fits one; segments are carved at that",
			 ceiling, (uint64_t)RAFT_LOG_SEGMENT_MIN, (uint64_t)RAFT_WIRE_ENTRY_MAX);
		ceiling = RAFT_LOG_SEGMENT_MIN;
	}
	/* At least RAFT_LOG_SEGMENT_MIN, because the region is at least RAFT_LOG_REGION_MIN. */
	uint64_t eighth = (region_len - RAFT_LOG_REGION_HEAD) / RAFT_LOG_REGION_SLOTS;
	*segment_bytes = ceiling < eighth ? ceiling : eighth;
	return ISO_OK;
}

bool raft_log_take_refusal(raft_log *l, raft_log_refusal *out)
{
	if (!l->has_refused)
		return false;
	*out = l->refused;
	l->has_refused = false;
	return true;
}

iso_err raft_log_format_region(int fd, uint64_t off, uint64_t len, uint64_t segment_bytes,
			       bool force, char *err, size_t errsize)
{
#define BAIL(code, ...)                                      \
	do {                                                 \
		if (err)                                     \
			snprintf(err, errsize, __VA_ARGS__); \
		return (code);                               \
	} while (0)

	if (off % REG_ALIGN)
		BAIL(ISO_ERR_INVAL, "a raft log region starts on a %u byte boundary, not %llu",
		     REG_ALIGN, (unsigned long long)off);
	if (!segment_bytes)
		segment_bytes = DEFAULT_SEGMENT;
	if (segment_bytes < SEG_HEADER + 2 * REC_HEADER)
		BAIL(ISO_ERR_INVAL, "a raft log segment holds at least %u bytes, not %llu",
		     SEG_HEADER + 2 * REC_HEADER, (unsigned long long)segment_bytes);

	uint64_t n_slots = region_slots(len, segment_bytes);
	if (n_slots < 2)
		BAIL(ISO_ERR_INVAL,
		     "%llu bytes is not enough for a raft log: it needs %llu bytes of headers plus at "
		     "least two segments of %llu. Give the log more of the device with "
		     "`--raft-bytes`, or a larger metadata device.",
		     (unsigned long long)len, (unsigned long long)REG_SLOTS_OFF,
		     (unsigned long long)segment_bytes);

	uint8_t h[REG_HEADER];
	if (pread(fd, h, REG_HEADER, (off_t)off) != REG_HEADER)
		return io_err(err, errsize, "reading", "the raft log region");
	uint64_t had_bytes = 0, had_slots = 0;
	if (!force && check_reg_header(h, &had_bytes, &had_slots) == REG_GOOD)
		BAIL(ISO_ERR_EXISTS,
		     "there is already a raft log here (%llu slots of %llu bytes). Formatting it "
		     "discards this node's log; pass --force if that is what you mean.",
		     (unsigned long long)had_slots, (unsigned long long)had_bytes);

	/* **The header goes last**, so a format interrupted anywhere leaves a region that reads as
	 * never formatted rather than as one with a slot array that is half somebody else's. Same
	 * ordering as the snapshot install in 94-b-ii-a, for the same reason. */
	uint8_t blank[REG_HEADER] = { 0 };
	if (pwrite(fd, blank, REG_HEADER, (off_t)off) != REG_HEADER)
		return io_err(err, errsize, "clearing", "the raft log header");

	uint8_t zeros[2 * HARD_SLOT] = { 0 };
	if (pwrite(fd, zeros, sizeof zeros, (off_t)(off + REG_HARD_OFF)) != (ssize_t)sizeof zeros)
		return io_err(err, errsize, "clearing", "the raft hard state");

	/* Only the headers: a slot whose header does not verify is free, and zeroing whole slots
	 * would mean writing the entire region. The nonce is what keeps a later occupant from
	 * reading what is left behind. */
	uint8_t slot_hdr[SEG_HEADER] = { 0 };
	for (uint64_t k = 0; k < n_slots; k++) {
		off_t at = (off_t)(off + REG_SLOTS_OFF + k * segment_bytes);
		if (pwrite(fd, slot_hdr, SEG_HEADER, at) != SEG_HEADER)
			return io_err(err, errsize, "clearing", "a raft log slot");
	}
	if (fdatasync(fd) != 0)
		return io_err(err, errsize, "flushing", "the raft log region");

	put_reg_header(h, segment_bytes, n_slots);
	if (pwrite(fd, h, REG_HEADER, (off_t)off) != REG_HEADER || fdatasync(fd) != 0)
		return io_err(err, errsize, "writing", "the raft log header");
	return ISO_OK;
#undef BAIL
}

iso_err raft_log_probe_region(int fd, uint64_t off, uint64_t len, raft_log_dir_info *out, char *err,
			      size_t errsize)
{
	memset(out, 0, sizeof *out);

	uint8_t h[REG_HEADER];
	if (pread(fd, h, REG_HEADER, (off_t)off) != REG_HEADER)
		return io_err(err, errsize, "reading", "the raft log region");
	uint64_t segment_bytes = 0, n_slots = 0;
	switch (check_reg_header(h, &segment_bytes, &n_slots)) {
	case REG_BLANK:
		/* Never formatted, which is the answer "this node has no log" -- the same shape
		 * `raft_log_probe_dir` gives for a missing directory. */
		return ISO_ERR_NOTFOUND;
	case REG_BAD:
		return iso_errf(err, errsize, ISO_ERR_CORRUPT,
				"the raft log header on the metadata device does not check out");
	case REG_GOOD: break;
	}
	if (!segment_bytes || n_slots != region_slots(len, segment_bytes))
		return iso_errf(err, errsize, ISO_ERR_CORRUPT,
				"the raft log header says %" PRIu64 " slots of %" PRIu64
				" bytes, which does not fit the region",
				n_slots, segment_bytes);

	out->slots_total = (unsigned)n_slots;
	for (uint64_t k = 0; k < n_slots; k++) {
		uint8_t sh[SEG_HEADER];
		if (pread(fd, sh, SEG_HEADER, (off_t)(off + REG_SLOTS_OFF + k * segment_bytes)) !=
		    SEG_HEADER)
			return io_err(err, errsize, "reading", "a raft log slot");
		raft_index first;
		raft_term prev_term;
		uint64_t nonce;
		if (!check_seg_header(sh, &first, &prev_term, &nonce))
			continue;
		if (out->segments == 0 || first < out->first)
			out->first = first;
		out->segments++;
	}
	return ISO_OK;
}

iso_err raft_log_region_discardable(int fd, uint64_t off, uint64_t len, const char *name, char *err,
				    size_t errsize)
{
	uint8_t h[REG_HEADER];
	if (pread(fd, h, REG_HEADER, (off_t)off) != REG_HEADER)
		return io_err(err, errsize, "reading the header of", name);
	uint64_t segment_bytes = 0, n_slots = 0;
	const char *why = "does not check out";
	switch (check_reg_header(h, &segment_bytes, &n_slots)) {
	case REG_BLANK:
		return iso_errf(err, errsize, ISO_ERR_NOTFOUND,
				"%s has no raft log in it yet. `isod mds init` makes one; an open never "
				"formats, because formatting is a verb.",
				name);
	case REG_GOOD:
		if (geometry_fits(len, segment_bytes, n_slots))
			return ISO_OK;
		why = "does not fit the region";
		break;
	case REG_BAD: break;
	}
	/* Laid out again by the discard, as `relay_geometry` would: the configured ceiling decides
	 * only the slots' size, never whether the carve refuses, so none is asked here. */
	uint64_t seg = 0;
	return relay_carve(name, len, 0, why, &seg, err, errsize);
}

iso_err raft_log_hard_load_region(int fd, uint64_t off, uint64_t len, raft_hard_state *out,
				  char *err, size_t errsize)
{
	(void)len;
	bool any = false;
	uint64_t seq = 0;
	iso_err rc = hard_read(fd, off + REG_HARD_OFF, out, &seq, &any);
	if (rc != ISO_OK)
		return iso_errf(err, errsize, rc, "reading the raft hard state failed");
	if (any)
		return ISO_OK;

	/* **Blank is not damaged, here too.** Both slots zero is a node that has never voted, which
	 * a fresh format leaves; anything else with no valid slot is a term and a vote that were
	 * lost, and those must never be guessed. */
	uint8_t buf[2 * HARD_SLOT];
	if (pread(fd, buf, sizeof buf, (off_t)(off + REG_HARD_OFF)) != (ssize_t)sizeof buf)
		return io_err(err, errsize, "reading", "the raft hard state");
	for (size_t i = 0; i < sizeof buf; i++)
		if (buf[i])
			return iso_errf(err, errsize, ISO_ERR_CORRUPT,
					"the raft hard state on the metadata device holds no valid slot. "
					"This node's term and vote are lost; do not guess them. Remove this "
					"node from the metadata group and add it back as a new member.");
	return ISO_ERR_NOTFOUND;
}
