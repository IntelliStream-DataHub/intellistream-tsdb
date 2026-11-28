# R10: Re-encode and online redundancy conversion — closed (as built, 2026-11-28)

**Closed (built on `main` for 0.11.0).** Both halves of R10 are built, verified by unit/cluster tests, planted mutants,
and Jepsen fault-injection runs (`make jepsen --tier 6 --reencode`, `make jepsen-cluster-r10`):

1. **Single-server and cluster E6 re-encode** (`r10-reencode`, `r10-crepl`, `r10-jc`): combines fragmented `.ec`
   partitions (`> ecpolicy_objects_max`, default 4) and tombstone-heavy `.ec` partitions (`> 10%` of combinable `.ec`
   rows) into one `.ec` object (or an empty `.part` when all rows are deleted/expired), preserving logical equivalence,
   latest-table invariants, insert backpressure, and crash safety.
2. **Cluster online redundancy conversion and `MK_UNSETTLE`** (`r10-meta`, `r10-unsettle`, `r10-driver`, `r10-jc`):
   converts running replicated cluster volumes online between `replicas=N` and `ec=K+M` (and between EC geometries)
   through the five committed metadata phases `GROW -> RECORD -> REENCODE -> DROP -> SHRINK`, with `--cancel` before
   `DROP`, `STRIPE_SET` live-object refusal, retired-position orphan cleanup before `STRIPE_DROP`, and minimum-margin
   reporting across active/converting/cancelled geometries.

## What was built (`file:line`)

### Re-encode policy, merge, backpressure, and swap

- **E6 policy & bounded input (`src/ecpolicy.c:99-122, 138-150, 594-1043`, `src/settle.h:131-137`):**
  - Trigger 1 (`src/ecpolicy.c:849-947`): more than `ecpolicy_objects_max` (default `ECPOLICY_OBJECTS_MAX_DEFAULT = 4`,
    overridable in single-server tests via `TSDB_TEST_TIER` `reencode_objects`) combinable `.ec` objects in a key,
    counting only `.ec` objects that can pair with a neighbour `.ec` within `max_part_bytes` (`4 GiB`, `src/db.c:70`)
    and `MAX_MERGE_PARTS` (`100`, `src/internal.h`) so oversized objects never trigger forever.
  - Trigger 2 (`src/ecpolicy.c:950-1042`): tombstones in newer combinable parts covering more than `ECPOLICY_TOMB_PCT`
    (`10%`, overridable via `TSDB_TEST_TIER` `reencode_tomb_pct`) of combinable `.ec` rows, evaluated outside `db->lock`
    on a ref-counted snapshot of candidate runs.
  - Skips keys with a pending `eckeep` digest (`eckeep_key_pending`, `src/ecpolicy.c:829, 964`), a dropping partition,
    damaged/settling parts, `test_mark` parts, or a failure within `ECPOLICY_BACKOFF_MS` (1 hour; `rfailed_recently`,
    `src/ecpolicy.c:66-72, 99-122`). Eager mode is capped at `ECPOLICY_EAGER_REENCODE_PER_MIN` (15/min,
    `src/ecpolicy.c:801-803`, `src/settle.h:132`).
- **Merge execution, `.ec` decoding, and no-mark error handling (`src/merge.c:262-272, 497-520, 663-666`,
  `src/ecpart.c:681`, `src/settle.c:1004-1244`):**
  - `merge_parts_reencode` (`src/merge.c:663-666`) decodes rows from `.ec` and young `.part` sources under frozen
    retention cutoffs (`ecd_freeze`) and retains tombstones when lower parts exist (`tombs_below`,
    `src/settle.c:1126-1141`). `.ec` parts are never passed to `pw_copy_granule` (`src/merge.c:497-520`,
    `src/ecpart.c:681`).
  - A `TSDB_ERR_UNAVAILABLE` (DOWN store) or `TSDB_ERR_CORRUPT` read from an `.ec` source through the seam aborts the
    re-encode without marking the part damaged or bumping `merge_fail_seq` (`src/merge.c:264-271`,
    `src/settle.c:1022-1029`).
  - Before swapping, single-server `eng_reencode_ex` verifies `ec_logical_digest_frozen` of the combined output (read
    back through the chunk seam when `.ec`, or directly when an empty `.part`) against the inputs
    (`src/settle.c:1161-1176`, `src/ecdigest.c:276`).
- **Latest table and backpressure (`src/ecswap.c:806, 842`, `src/db.c:416-442, 1854-1878, 2105`, `src/settle.c:1219`):**
  - `ec_swap_reencode` calls `lt_merge_marks(db, &lsec, in, nin)` inside its `latest_rw` > `db->lock` write section
    (`src/ecswap.c:770-806`) and broadcasts `db->merged_cv` (`src/ecswap.c:842`, `src/settle.c:1219`).
  - `db_wait_merges` (`src/db.c:416-442`) counts `p->reencoding` in `merging` and keeps inserts waiting while an active
    re-encode is reducing part count; `drop_settle_locked` (`src/db.c:1864-1878`) waits for in-flight `p->reencoding`
    parts before `drop_intent_prepare` runs unlocked. `part_merge_barrier` (`src/ecskip.h:52`) keeps ordinary merges off
    `.ec` and `settling` parts.
- **Intent v4/v5 swaps and recovery (`src/ecswap.c:638-908, 951-1251, 1735-1895`, `src/ecapply.c:305-326, 505-542, 663`):**
  - Single-server and cluster leader re-encode (`ec_swap_reencode`, `src/ecswap.c:638-669, 719-908`) writes **intent v4**
    (`tsdb-intent 4 1 <nin>`): one `L <tmp> <out>` line (`.ec` or empty `.part`) and `<nin>` `R` lines
    (`R <p.ec> retired_<p.ec>` for `.ec` sources, `R <p.part> settled_<p.part>` for young `.part` sources), syncs `L`
    before retiring `.ec` sources, and recovers via `ecswap_recover` (`src/ecswap.c:1735`).
  - Cluster replica `MK_COLD` re-encode apply (`src/ecapply.c:305-326, 505-542`) writes **intent v5** (`N`, `Y`,
    `R <own.part> settled_<own.part>`, and `R <own.ec> retired_<own.ec>`), with `ecapply_cold_decide_ex`
    (`src/ecapply.c:663`) handling empty-output recovery.
  - Reverse unsettle (`ec_swap_unsettle_at`, `src/ecswap.c:951-973, 1016-1251`) writes **intent v5** (`L` restoring
    `P.part`, `U <P.ec> retired_<P.ec>`).

### Cluster re-encode, `MK_UNSETTLE`, metadata conversion record, and driver

- **Cluster re-encode (`src/eccold.c:293-310, 885-1240`, `src/ecdigest.c:403`, `src/ecgc.c:962`, `src/settle.c:1081-1090`):**
  - `eng_reencode_ex` enforces the cluster suffix invariant (no young `.part` below `I`, no `.ec` above `I`;
    `src/settle.c:1081-1090`), merges and verifies pre-put, puts chunks, fast-aborts `stripe_wait` on `epoch` or
    `stripe_version` change (`src/eccold.c:293-309`), cuts at `Z`, appends `MK_COLD` naming both the new object (or
    empty `.part` as `Y`) and `retired[]` oids, swaps via `ec_swap_reencode` with `ecgc_retire_locked_digest`
    (`src/ecgc.c:962`), verifies post-swap `ecdigest_leader_reencode` (`src/ecdigest.c:403`), commits `MK_DIGEST`, and
    releases retired objects to `MK_GC` only after `MK_DIGEST` (`need_applied = digest_pos`).
- **Cluster `MK_UNSETTLE` (`src/marker.h:52`, `src/ecunsettle.c:550-995`, `src/ecswap.c:1016-1248`):**
  - `MK_UNSETTLE = 14` (`src/marker.h:52`). `eng_unsettle_key` (`src/ecunsettle.c:550`) decodes unlocked, takes
    `jr_serial`, arms `db->unsettle_hold_pos` before appending `MK_UNSETTLE{key, oids[]}`, swaps via
    `ec_swap_unsettle_at` (clamping `REPL.floor`, `keep_floor`, and `snap_make` until durable), and gates `MK_GC` on
    `repl_applied_floor` across all log replicas. Followers apply `MK_UNSETTLE` in `ec_unsettle_apply`
    (`src/ecunsettle.c:846`) before acknowledging.
- **Metadata conversion record and `STRIPE_SET` refusal (`server/meta.h:349-442, 579-582, 748-766, 792`,
  `server/meta.c:2957-3310`):**
  - Persisted `struct meta_conv` (`phase`, `target_*`, `new_stripe_version`, `orig_*`, `orig_min_isr`),
    `conv_cancel_gen`, `mixed_redundancy`/`mixed_k`/`mixed_m`/`mixed_sv_lo`/`mixed_sv_hi`, `stripe_pos[].retired_ver`,
    and `live_epoch`/`live_sv_at`/`live_overflow`/`nlive_sv`/`live_sv[]` (introduced in `META_SNAP_VERSION = 9` /
    `"TME6"`, now `META_SNAP_VERSION = 12` / `"TME9"`).
  - Ops `META_OP_VOLUME_REDUNDANCY`, `META_OP_VOLUME_CONV_STEP`, `META_OP_VOLUME_CONV_CANCEL`, and
    `META_OP_VOLUME_LIVE_OBJS` (`server/meta.c:3031-3310`).
  - `META_OP_STRIPE_SET` with `node == 0` (`meta_op_stripe_drop`, `server/meta.c:2972-2993`) is allowed only in
    `META_CONV_NONE` or `META_CONV_DROP`, refuses to drop active or `mixed_redundancy` positions, and refuses unless
    `stripe_drop_safe()` confirms current-epoch, non-overflow `live_sv[]` reports 0 live objects at every
    `stripe_version < retired_ver`.
- **Conversion driver, volume-leader worker, orphan cleanup, margins, and CLI (`server/credund.c:1-852`,
  `server/chunkpeer_cluster.c:1856-2438`, `src/ecgc.c:1545`, `server/margins.c:112-154`, `tools/cluster-cli.c:59, 125-127,
  300-308, 1468-1485, 2955-3005`):**
  - `credund_admin` / `credund_duty` on the Raft leader drive `GROW -> RECORD -> REENCODE -> DROP -> SHRINK`, handle
    `--cancel` before `DROP` plus idle cleanup of cancelled-`REENCODE` `mixed_redundancy` leftovers, and pause/refuse
    when `cluster_versions_mixed()` (`server/cluster.c:5704-5718`, P1 hook) returns 1.
  - `conv_main` / `chunkpeer_conv_cmd` run `eng_reencode` or `eng_unsettle_key` per key paced by `pace_take_capped`
    (`rebuild_mb_s`) and report `HBS_LIVE_OBJS` via `publish_live_objs` / `ecgc_live_by_sv` (`src/ecgc.c:1545`),
    holding `META_OP_STRIPE_DROP` until retired-position orphans are cleaned via `MK_GC_RELEASE`.
  - `margins_of` (`server/margins.c:129-151`) takes the minimum settled margin across active, `orig` (during
    `REENCODE`/`DROP`), and `mixed_redundancy` geometries (D29).
  - Operator verb: `tsdb cluster volume redundancy PEER VOLUME (ec=K+M | replicas=N | --cancel) --secret-file F`
    (progress in `tsdb cluster status volume V [--json]`).

### Tests, mutants, and Jepsen coverage

- Unit & cluster suites: `tests/test_ecswap.c`, `tests/test_settle.c`, `tests/test_settle_oracle.c`, `tests/test_meta.c`,
  `tests/test_ecapply.c` (`reencode`, `unsettle`), `tests/test_ecgc.c` (`live_by_sv_apply_gap`), `tests/test_eccold.c`,
  `tests/test_chunkpeer.c` (`5f`), `tests/test_margins.c`, `tests/test_place.c`, `tests/test_cluster.c` (`reencode`,
  `unsettle`, `redund`), `tests/test_cluster_cli.sh`.
- Planted mutants: `reencodedroptomb`/`droptomb`, `reencoderankdown`, `reencodemarkunavail` (`test_settle`),
  `stripelive`, `convskip`, `retireclear`, `livestale`, `reencskip` (`tests/meta_mutants.sh`),
  `retiredunnamed`, `gcbeforedigest` (`make reencode-mutants`), `unsettleleaderonly`, `unsettlenogcgate`,
  `unsettlenofloorclamp`, `unsettlesnapnohold` (`make unsettle-mutants`), `dropearly`, `shrinkfirst`
  (`make redund-mutants`).
- Jepsen: `jepsen --tier 6 --reencode` (`kill`, `power`, `settle-steps` with 5 `reencode-*` crash points, `eio`;
  selftest `droptomb`), `jepsen_cluster --convert A:B` (`replicas=3:4+2`, `4+2:4+3`, `4+2:replicas=3` under `kill`,
  `partition`; selftest `dropearly`; `make jepsen-cluster-r10`).

## Remaining / deferred items (tracked outside R10)

- **Single-server geometry-mismatch background sweep (`r10-geom`):** `tsdb DIR tier set --redundancy K+M [--volume N]`
  updates `TIERS` live via `tierctl` (`src/tierctl.c:496-525`) so new settles and subsequent E6/`--shrink` re-encodes
  use the new geometry, while older settled objects remain readable under their recorded per-object geometry; a
  standalone single-server policy trigger that re-encodes every older object solely for geometry mismatch is not built
  yet.
- **Rolling-upgrade mixed-version pause (E38):** wired in `server/credund.c:461, 742` and
  `server/chunkpeer_cluster.c:2232` via `cluster_versions_mixed()` (`server/cluster.c:5704`), which returns `0` (except
  under `TSDB_CLUSTER_TEST=versions_mixed_file=1`) until P1 implements the cluster version gate.
- **Carry-over R7 items:** testing settle/re-encode against a real format-8 (v8) part fixture
  (`docs/tasks/pre-1.0-v8-parts.md`), the 4 GiB `TSDB_TEST_BIG=1` big-part stress test, and settle/re-encode cost
  numbers per GiB (tracked under R11).
