# 64-bit series ids

Status: design, not implemented. Branch `series64-design`, written on 2026-11-02 from main at 58456ee.

The user decided on 2026-11-02 to widen series ids from 32 to 64 bits before the first release (0.9.0, not tagged
yet), for the production target of 1 billion series over about 1000 volumes on dual-socket servers. The roadmap
row "Series addressed by volume path and name with 64-bit ids" (docs/roadmap.md:51) and the failover design
(docs/ha.md §5, decision D9) already sketch the id; this document turns that into a change to today's code, site by
site, and keeps the failover parts (LOST entries, `STALE_ID`, `volumes.cache`, forwarding) out of it.

## Summary

- **The id is `(volume + 1) << 32 | local`**, a `uint64_t` (`tsdb_sid`). `volume` is the volume's number (0 for the
  database directory, line i of `VOLUMES` for volume i, as today), `local` the series' index in that volume's
  `SERIES` dictionary (below 2^31, as today). Bits 48-63 are zero. Every valid id therefore lies in
  `[2^32, 2^48)`: never 0, never a value a 32-bit field can hold, positive in a Java `long`, exact in a double.
- **The engine does not change.** Each volume's engine already numbers its series densely with `uint32_t` (the
  memtable, the retention and class tables are arrays indexed by that number), and every file in a volume (WAL,
  parts, granule index, tombstones, `CLASSES`, `RETENTION`, `SERIES`, `MANIFEST`) holds only local ids. The volume
  half of the id is the directory the file is in. So no file inside a volume changes, and nothing per row or per
  granule costs a bit more.
- **CATALOG goes away.** Today's dense global ids exist only to route a 32-bit id to (volume, local) through the
  root's `CATALOG` (16 bytes a series on disk, 16-32 bytes a series in memory, read at every open, one more fsync
  per new series). With the volume inside the id, routing is a shift and a mask.
- **Disk format 8** (CATALOG neither read nor written; each volume's `META` names its number), **protocol 11**
  (every id on the wire a `u64`; a server refuses a HELLO below 11 and a client a server below 11), **export
  stream unchanged** (it never carried ids: series travel by name and volume path, docs/export-format.md).
- **Cost:** zero bytes on disk per row, per granule and per series (minus CATALOG's 16); 4 more bytes per id on
  the wire (0 where a reserved word takes it, about 0 in packed inserts); 4 more bytes per row in the caller's id
  arrays of many-series calls. No published benchmark number should move; `insert-bench` and `tsdbc-bench load`
  are rerun to check.

## 1. Where a series id lives today

Line numbers are at 58456ee. "Global" ids are the dense CATALOG numbers the API hands out; "local" ids are a
volume's own dense numbers, which only the engine (`src/` except `volumes.c` and `multi.c`) sees.

### 1.1 Public C API (`include/tsdb.h`)

| lines | what | today |
|---|---|---|
| 163-165, 169 | `tsdb_series`, `tsdb_series_at`, `tsdb_series_lookup`, `tsdb_series_create`: `uint32_t *id` out | global |
| 170, 172-173 | `tsdb_series_type`, `tsdb_series_name`, `tsdb_series_path(uint32_t id)` | global |
| 171 | `uint32_t tsdb_series_count(tsdb *)`: callers loop `0 .. count-1` (dense ids) | count |
| 174-175, 184-185, 666 | `tsdb_volume_count`, `tsdb_volume_path(uint32_t index)`, `tsdb_volume_add(_dir)(.., uint32_t *index)`, `tsdb_get_volume_stats(uint32_t volume)` | volume numbers, not ids |
| 199-206 | `tsdb_insert(const uint32_t *series, ..)`, `tsdb_insert_values(const uint32_t *series)`, `tsdb_insert_series(_values)(uint32_t series)` | global |
| 225-226, 238-239 | `tsdb_delete_range`, `tsdb_replace_range`, `tsdb_set_retention`, `tsdb_get_retention` | global |
| 244-246 | `tsdb_state`, `tsdb_state_lookup`, `tsdb_state_name(uint32_t series, .., uint32_t code)`: the series picks the volume; codes stay u32 | global |
| 249 | `tsdb_append(uint32_t series)` | global |
| 261-520 | every query: `tsdb_query_raw(_ex)`, `_points(_ex)`, `_values`, `_lttb(_ex)`, `_agg(_ex)`, `_group(_ex)`, `_bounds`, `_spread(_group, _bounds)`, `_twa(_group, _bounds)` | global |
| 579-588 | `tsdb_series_result { size_t index; uint32_t series; int status; int last; size_t n; .. }` | global; offsets 0/8/12/16, pad 20, n 24 |
| 604, 608 | `tsdb_query_multi(.., const uint32_t *series, size_t n)`, `tsdb_query_multi_collect` | global |
| 697 | `tsdb_series_class(uint32_t series)` | global |
| 711-728 | `tsdb_struct_size(kind)`: the only ABI check bindings have (sizes only) | ABI |

Also `client/tsdb_calendar.h`/`src/calendar.c`: `tsdb_query_group_local`, `tsdb_query_spread_group_local`,
`tsdb_query_twa_group_local` take `uint32_t series` (bound by client/rust-sys/src/lib.rs:509-536), and the
test hooks in `src/volumes.c:1089-1146` (`tsdb_test_class_switch(uint32_t id)`, `tsdb_test_granules`).

### 1.2 The routing layer (`src/volumes.c`, `src/multi.c`)

| lines | what |
|---|---|
| volumes.c:13-20 | file header: `CATALOG`, one 16-byte record per series in global id order: `u32 volume, u32 local, u64 xxh64(global id, volume, local)` |
| volumes.c:56-58 | `struct tsdb`: `uint32_t *cat_vol, *cat_local` by global id; `uint32_t *by_name`, open addressing over global ids + 1 |
| volumes.c:70-129 | `gid_name`, `names_grow`, `names_find`, `cat_add`: the global name index |
| volumes.c:162-185 | `cat_check`, `cat_record`, `cat_valid` |
| volumes.c:249, 446 | `MAX_VOLUMES 65535` (the root and 65534 more), enforced at `volume add` |
| volumes.c:579-675 | `load_layout`: reads VOLUMES, then all of CATALOG (writer: torn tail cut, damage fails the open) |
| volumes.c:677-704 | `adopt_orphans`: series in a volume that CATALOG lacks (a crash between the two appends) |
| volumes.c:781-783 | read-only open of a database without CATALOG: the root's series by local id |
| volumes.c:823-854 | `catch_up`, `route(uint32_t g)`: `g < ncat`, then `cat_vol[g]`, `cat_local[g]` |
| volumes.c:856-873 | `tsdb_series_lookup`: `names_find` over every volume's names |
| volumes.c:1006-1049 | `series_make`: lookup, `create_lock`, `eng_series` in the volume, then a CATALOG append to the root (two durable appends), `*id = ncat` |
| volumes.c:1063-1068 | `tsdb_series_count`: `ncat` |
| volumes.c:1148-1186 | state ops and `tsdb_series_path`: route by global id |
| volumes.c:1215-1285 | `insert_series_slots`, `insert_slots`: checks `series[i] < ncat`; with the root alone (`identity`) hands the caller's array to `eng_insert` unchanged, else counting-sorts rows per volume into a `uint32_t` local array |
| volumes.c:1287-1370, 1499-1850 | inserts, deletes, replace, retention, every query: `route()` then the engine with the local id |
| multi.c:58, 185, 250, 314, 339, 513, 626, 642 | `mcall.series` (`const uint32_t *`), the `tsdb_series_result` initialisers, `run_one` calling `tsdb_query_values(db, id, ..)` |

### 1.3 The engine and its files (local ids: unchanged, see §2.2)

| where | what | width |
|---|---|---|
| src/series.c:1-15, 30 | `SERIES`: `[u32 check][u16 len][u8 type][u8 0][name][pad to 8]`, the id is the record index; `DICT_MAX_NAMES = 2^31 - 1` | implicit |
| src/internal.h:903-932 | `series_dict`: `uint32_t *table` (id + 1), `names`, `types` by id; `series_get(.., uint32_t *id)` | u32 |
| src/memtable.c:9, internal.h:1093-1096 | WAL rows of 20 bytes: `u32 series, i64 ts, u64 slot` | u32 |
| internal.h:1036-1062 | memtable: `mt_series *s` **indexed by series id**, `uint32_t *ids`; `cls_wtail.ids` | u32 index |
| internal.h:1205-1223 | `eng`: retention limits **an array by series id**, `cls_tab` a class byte **per series id**, `cls_hist` by series | u32 index |
| src/gindex.c:7, internal.h:634, 679, 765 | granule index: `u32 series[nruns], u32 start[nruns]`, a run table per part (not bit-packed); `gx_series`, `gix_run` | u32 per run |
| internal.h:517-519 | `struct granule.series` (the decoded form, never on disk) | u32 |
| internal.h:586-595, src/part.c:5-11, tomb.c | tombstone trailer: `{u32 series, u32 zero, i64 start, i64 end}`, 24 bytes | u32 + reserved u32 |
| src/classes.c:42 | `CLASSES`: 24-byte records `[u32 check][u32 series][i64 from][u32 class][u32 0]` | u32 |
| src/retention.c:5 | `RETENTION`: `[u32 check][u32 series][i64 limit_ms]` | u32 |
| src/manifest.c:62-67, 152 | `MANIFEST` line `series %u`: the volume's series count, for followers | count |
| src/part.c:285-355, internal.h:854-879 | part writer `pw_add(uint32_t series)`, `pend_series`; `(series, ts)` order check | u32 |
| src/db.c:664, 747; internal.h:1349-1410 | `eng_series`, `eng_insert(const uint32_t *series)`, `eng_insert_series`, `snapshot_take`, `partition_of`, `insert_rows` | u32 |
| src/db.c:196-245 | `META` (text: `version=`, `partition_ms=`, `granule_rows=`, `partition_classes=`); a volume's META is a copy of the root's (volumes.c:405-413); the refusal message of another format | none |

### 1.4 The wire protocol (`include/tsdb_protocol.h`, version 10)

| op (lines) | id fields today |
|---|---|
| HELLO (18-20) | none; `TSDB_PROTOCOL_VERSION 10` (152) |
| SERIES, SERIES_CREATE (21-23, 44-45) | reply `u32 id` |
| SERIES_NAME, SERIES_TYPE (24, 46) | `u32 id` |
| INSERT (25-27), version 1 float32 | `u32 n, u32 series`; many-series: `u32 0xffffffff` (`TSDB_MANY_SERIES`, 158) then `u32 series[n]` after the values |
| INSERT_VALUES (47-62) | 16-byte header `u32 n, u32 series, u8 type, u8 flags, u16 0, u32 0` (`TSDB_VALUES_HEADER`); many-series: pad to 4, `u32 series[n]`; packed (`TSDB_PACKED`): the id column inside the zstd frame as zigzag varint deltas (client/tsdb_pack.h) |
| QUERY_RAW, QUERY_VALUES, QUERY_AGG, QUERY_GROUP (29-35, 53) | leading `u32 series` |
| STATE, STATE_NAME (63-65) | `u32 series` (next to `u32 code` in STATE_NAME) |
| QUERY_BUCKETS, QUERY_BOUNDS, QUERY_LTTB, QUERY_TWA (66-93) | leading `u32 series`; fixed sizes depend on it: LTTB 28/37 bytes, `TSDB_TWA_HEADER 56` |
| DELETE_RANGE, SET_RETENTION, GET_RETENTION (97, 101-102) | `u32 series, u32 0` (the reserved word aligns the i64); GET: `u32 series` |
| REPLACE_RANGE (98-100) | the INSERT_VALUES header (`TSDB_REPLACE_HEADER 32`) |
| QUERY_MULTI (126-142) | `u32 series[n]`, zero bytes to a multiple of 8; the template's series `0xffffffff`; replies carry `u32 index` (`TSDB_MULTI_PREFIX 4`), an index into the request, not an id |
| CANCEL (143-145) | `u32 id` is a frame id, not a series |

### 1.5 The server (`server/tsdbd.c`; `server/metrics.c` has no ids)

tsdbd keeps no cache or array indexed by series id and checks no id against a count: an unknown id goes to the
library, which answers it. Every site is a parse or a reply:

| lines | op | today |
|---|---|---|
| 1081-1110, 1247-1268 | SERIES, SERIES_CREATE | `uint32_t id`, reply of 4 bytes (1106, 1264) |
| 1112-1126, 1270-1284, 1490-1503 | SERIES_NAME, SERIES_TYPE, GET_RETENTION | `len != 4`, `memcpy(&id, p, 4)` |
| 1127-1146 | INSERT | n at 0, series at 4; `many ? 16 : 12` bytes a row (1137); the id array cast in place, `(const uint32_t *)(p + 8 + n * 12)` (1143) |
| 1286-1313 | INSERT_VALUES | series at 4 (1295); id array at `(end + 3) & ~3`, `len == sofs + n * 4` (1301), cast in place (1310) |
| 601-640 | `insert_packed` | unpacked size `n * (8 + w + (many ? 4 : 0))` (615), `malloc(n * 4 + 4)` (624), `tsdb_pack_unshape(.., uint32_t *series)` |
| 690-704, 725-830 | `qreq.series` and the parsers `parse_values` (lengths 20/29, LTTB 28/37), `parse_buckets` (`len >= 44`), `parse_bounds` (n at 29, bounds at 33), `parse_twa` (`TSDB_TWA_HEADER` 56) | series at offset 0, every later field after it |
| 1147-1245 | QUERY_RAW, QUERY_AGG (len 28), QUERY_GROUP (count at 21, zone at 34): parsed inline | `uint32_t s` |
| 1315-1440 | QUERY_VALUES, QUERY_LTTB, QUERY_BUCKETS, QUERY_BOUNDS through `qr.series` | |
| 1347-1378 | STATE (`len >= 7`, create at 4), STATE_NAME (`len != 8`, code at 4) | |
| 1442-1488 | DELETE_RANGE (len 24, `u32 0` at 4), REPLACE_RANGE (series at 4, start at 16), SET_RETENTION (len 16, `u32 0` at 4) | |
| 836-883 | `multi_fn`: `ix = (uint32_t)r->index` (an index), `tsdb_series_type(db, r->series)` (859) | |
| 927-1059 | `multi_parse`: ids at `(at + n * 4 + 7) & ~7` (953), padding check (956-958), cast in place (960), template check `qr->series != TSDB_MANY_SERIES` (1004); `handle_multi` passes the array to `tsdb_query_multi` (1050) | |
| 1861-1912 | CANCEL: a request id | not an id |

### 1.6 The C client (`client/tsdb_client.h`, `client/tsdb_client.c`, `client/tsdb_pack.[ch]`)

| where | what |
|---|---|
| tsdb_client.h:7-9, 48-169 | about 45 prototypes with `uint32_t series`, `uint32_t *id`, `const uint32_t *series` |
| tsdb_client.c:358-384, 874-897 | `series_req`, `series_create`: reply `n != 4` is CORRUPT, `memcpy(id, b, 4)` |
| tsdb_client.c:393-397, 901-905, 1316-1323 | SERIES_NAME, SERIES_TYPE, GET_RETENTION: 4-byte request |
| tsdb_client.c:417-437 | INSERT: `uint32_t h[2] = {m, series ? MANY : s}`, id iov `m * 4` (429) |
| tsdb_client.c:962-1040 | `send_packed`, `insert_values_frames`: series at header + 4 (989-991, 1019-1021); pad to 4 (1028); id iov `m * 4` (1029) |
| tsdb_client.c:464-1110 | the queries build `q[29]`, `q[44]`, `q[33]`, `q[TSDB_TWA_HEADER]`, `q[37]` with the series at 0 |
| tsdb_client.c:1172-1208, 1261-1313 | STATE (`h[7]`), STATE_NAME (`{series, code}`), DELETE_RANGE (`b[24]`), REPLACE_RANGE, SET_RETENTION (`b[16]`) |
| tsdb_client.c:1398-1427, 1500-1521, 1578-1636, 1655-1863 | QUERY_MULTI: reads `ix` (index), template with `uint32_t any = TSDB_MANY_SERIES`, id iov `k * 4` and pad `(8 - k * 4 % 8) % 8` (1622-1623), results carry `series[i]` |
| tsdb_client.c:23 | `INSERT_FRAME_ROWS (8u << 20)`: rows with ids grow from 16 to 20 bytes; 8 Mi rows stay under `TSDB_MAX_FRAME` |
| tsdb_pack.h:11, 26-32 | packed id column only for `TSDB_MANY_SERIES`: zigzag deltas from the previous id (the first from 0) as LEB128 varints; `const uint32_t *series` |
| tsdb_pack.c:38, 59-64, 94-103 | `raw_bound` `many ? n * 5`; encode `zigzag((int64_t)series[i] - (int64_t)ps)`; decode refuses `ps < 0 \|\| ps > UINT32_MAX` |

The C client sends INSERT and QUERY_RAW (version-1 ops) but never QUERY_AGG or QUERY_GROUP (tsdb_client.c:678 uses
QUERY_BUCKETS with unit 0 instead); neither do the Rust and Java clients (Java defines the op codes, Protocol.java:
39, 44, and does not use them).

### 1.7 Rust (`client/rust`, `client/rust-sys`)

| where | what | today |
|---|---|---|
| src/protocol.rs:20, 27, 141 | `PROTOCOL_VERSION = 10`, `MANY_SERIES = u32::MAX`, `FrameBuf::u32s` for id arrays | u32 |
| src/client.rs:107, 315-341, 1775-1781 | HELLO, `server_version`, `need_version(v)`; gates at 334 (3), 1230 (4), 979/981 (5/9), 1257/1342/1396 (8), 1560/1614 (6), 1770 (7), multi.rs:169/247/282 (10) | gates |
| src/client.rs:370-477 | `series*` return `u32` (`r.u32()` at 431, 462); `series_name/type(id: u32)` | u32 |
| src/client.rs:496-535 | `state*(series: u32, ..)`: `f.u32(series).u32(code)` | u32 |
| src/client.rs:563-799 | `insert*`: `series: u32` / `&[u32]`; INSERT header 704-724; INSERT_VALUES header 732-799, id column `align4().u32s()` at 791, packed at 774 | u32 |
| src/client.rs:852-1640 | every query: `.u32(series)` at 1096, 1141, 1349, 1403, 1619 | u32 |
| src/client.rs:1663-1756 | DELETE_RANGE, SET_RETENTION `f.u32(series).u32(0)`; REPLACE_RANGE 1715; GET_RETENTION 1756 | u32 + pad |
| src/client.rs:2064-2075 | `check_lengths`: refuses `MANY_SERIES` as an id | sentinel |
| src/client/multi.rs:60-434, 466-482, 611 | multi API `&[u32]`; QUERY_MULTI body `k*4`, pad `(8 - k*4 % 8) % 8`; template starts with `MANY_SERIES` (4 bytes) | u32 |
| src/pack.rs:45, 68-73, 106-133, 164 | packed id column: zigzag of `x as i64 - prev` as varint; test decoder `u32::try_from` | u32 deltas |
| src/batch.rs:47-109, 205-211, 247-506 | `RangeQuery.series`, `AggQuery.series`, `Target::Series(u32)`, `Each(&[u32])`, `HashMap<u32, usize>` | u32 |
| src/database.rs:29-335 | the `Database` trait over client and embedded: every `series: u32` | u32 |
| src/embedded.rs:396, 674-1222; src/embedded/multi.rs:52-313 | embedded `Db`: `series*() -> u32`, `series_count() -> u32`, every op `u32`/`&[u32]`; multi passes `series.as_ptr()` | u32 |
| rust-sys/src/lib.rs:268-307, 311-536, 596-606, 645-660 | FFI declarations: `*mut u32` out ids, `*const u32` arrays, `tsdb_series_count() -> u32`, `tsdb_series_result.series: u32` | u32 |
| rust-sys/layout.c:47-49, tests/layout.rs:7-108 | struct offsets and sizes checked against C (names and offsets, not function types) | layout test |
| examples/common/mod.rs:40-47; insert_bench.rs, embedded_bench.rs, embedded_suite.rs, query_bench.rs | `series as usize % 7`; 16-byte bench rows `u32 series, i64 ts, f32`; `ids[si as usize]`; random ids `0..n` | dense ids |
| tests/server.rs:243; tests/batch.rs:17-50, 253; tests/multi.rs:299; deletes.rs, packed.rs, embedded.rs, timeout.rs | `u32::MAX` refused; ids `0..24` and `k as u32 % 3` used without creating series; `5_000_000 + i` as unknown ids | dense ids |

### 1.8 Python (`client/python`, PyO3 over the Rust crate)

| where | what |
|---|---|
| src/lib.rs:81-86, src/arrays.rs:60 | `series_column() -> Col<u32>`: a buffer must be **uint32**, else a list of ints |
| src/lib.rs:700-1557 | `Client` methods: `series: u32`, `id: u32`, returns `u32` |
| src/lib.rs:943, 1994 | `protocol_version()`, `PROTOCOL_VERSION` (gates inherited from Rust) |
| src/embedded.rs:133, 264-825 | `Db`: the same; `series_count() -> u32` (300) |
| src/arrays.rs:107-306, src/pool.rs:53-77, src/multi.rs:72-471 | `Dest::Each(Col<u32>)`, `HashMap<u32, ..>` caches, `Vec<u32>` results |
| tsdb_client.pyi:53, 621, 1094, 1672, 1700 | `_SeriesIds = Buffer \| Sequence[int]`; docs say "uint32 array" (`int` elsewhere needs no change) |
| tests: test_client.py:99, test_batch.py:57-341, test_inputs.py:58-107, test_types.py:218 | `np.array(ids, dtype=np.uint32)` as ids |
| tests/test_zones.py:121, tests/test_packed.py:115-117 | parse frames at fixed offsets (`"<IqqqBIBQH"`, `p[:10]`, `p[16:20]`) |
| tests/test_embedded.py:92; bench_insert.py:33-94; bench_embedded_suite.py:76-201 | `series_count()` as a bound; `"<Iqf"` rows; `range(series_count())` |

### 1.9 Java (`client/java`, `client/java-embedded`)

| where | what |
|---|---|
| Protocol.java:6, 11, 14-18, 24-26 | `VERSION = 10`, `MANY_SERIES = 0xffffffff` (int -1), `VALUES_HEADER 16`, `REPLACE_HEADER 32`, `TWA_HEADER 56`, multi constants |
| TsdbClient.java:111, 363-371, 1656-1668, 2683-2689 | HELLO and `needVersion`; gates at 910 (4), 1643 (3), 1115/1137/1165 (8), 1214/1242 (6), 1319/1321 (5/9), 2170 (10), 2679 (7) |
| TsdbClient.java:421-482, 1339-1446 | `int series(..)`, `seriesAt`, `seriesCreate` (`in.getInt()` at 466); `seriesName/Type(int)`; `state*(int series, ..)` |
| TsdbClient.java:501-660, 1479-1690 | `insert*(int / int[])`; INSERT `rowBytes = 8 + 4 + Integer.BYTES`; INSERT_VALUES pad to 4 (1613), `putInts(many)`; `sendPacked` header (1685); reserved-id checks 639, 1592 |
| TsdbClient.java:683-1840, 2583-2662 | every query `putInt(series)`; DELETE_RANGE/SET_RETENTION `putInt(series).putInt(0)`; REPLACE_RANGE 2619 |
| TsdbClient.java:1943-2460, 2887-2897, 3040-3046 | multi API `int[] series`; template `putInt(MANY_SERIES)` (2382); body `k*Integer.BYTES`, pad `(8 - k*4 % 8) % 8`; `putInts` shared with state codes |
| Pack.java:23, 33, 46-52 | packed id column: `rawBound` `n*5` (u32 varint), `toUnsignedLong(s) - ps` zigzag |
| bench/BenchCommon.java:47-150, bench/InsertBench.java:78-139 | 16-byte rows with `u32 id`; `int[] ids = new int[max + 1]` |
| test: CompressionTest.java:295-312, TimeZoneGroupTest.java:215-297, PackTest, MultiTest, TypedValuesTest | frame offsets (`getInt(4)` series, count at offset 29); `int[]` ids |
| java-embedded Native.java:203-212 | `SERIES_RESULT = {SIZE_T index, JAVA_INT series, JAVA_INT status, JAVA_INT last, padding(4), SIZE_T n, ..}` |
| Native.java:242-275 | load-time `checkAbi`: compares `tsdb_struct_size` **sizes only** |
| Native.java:369-456 | downcall descriptors: `JAVA_INT` for every id parameter, `SERIES_COUNT` returns int |
| EmbeddedDb.java:273-442 | `allocate(JAVA_INT)` out id; `seriesType/Class/Name/Path(int)`; `int seriesCount()`, documented as "ids are 0 .. seriesCount() - 1" |
| EmbeddedDb.java:525-891, 1221-2471 | `state*`, `insert*` (`allocateFrom(JAVA_INT, ..)`, offsets `*4L` at 672, 833), queries, multi (`allocateFrom(JAVA_INT, series)` 2035, result stride 2006) |
| test: LayoutTest.java:51-278 (against test/c/layouts.c:63-89), EmbeddedDbTest.java:96, 759-770 | offsets and sizes; `handlesMatchThePrototypes` maps `uint64_t` to `long`; `seriesCount()` |

### 1.10 Pulsar connector (`connectors/pulsar`)

| where | what |
|---|---|
| SeriesCache.java:22-38 | `Entry { final int id; .. }`, `LinkedHashMap<String, Entry>` by name (state codes `Integer`, unchanged) |
| TsdbConnection.java:30-33, 97-120 | `int state(int series, ..)`, `insertAsync(int[] series, ..)`; `int id = c.series(..)`, `c.seriesCreate(..)` |
| Shard.java:404-408 | `int[] ids`, `ids[i] = entry.id` per batch |
| test: FakeTsdb.java:39-127, PartsTest.java:64, TsdbdSinkTest.java:80-134, RequestTimeoutTest.java:164, test/IntegrationDriver.java:131 | `Map<Integer, String>`, `assertEquals(1, id)`, `int id = c.series(..)` |

### 1.11 Tools (`tools/`)

| where | what | dense ids assumed |
|---|---|---|
| tsdb-cli.c:101-130, 175, 222-275, 316, 335, 414, 478-557, 771-782 | import batches `uint32_t *s`, query contexts, ids from `tsdb_series_lookup` | no |
| tsdb-cli.c:722-725, 731-739, 750-753 | `create` prints `%u`; `series` lists `for (uint32_t i = 0; i < tsdb_series_count(db); i++)`; `stats` prints the count | yes (the listing) |
| dump.c:275, 354-361, 604-625 | export: `for (uint32_t id = 0; id < count; id++)`, `%u` in messages | yes (the export loop) |
| dump.c:777, 795, 1254, 1328-1370, 1472, 2232 | import: the target's id per series, `uint32_t *sid` with a hard-coded `malloc(cap * 4)` (2232) | no |
| dump.c, dump.h | the stream itself: names and paths, no ids | n/a |
| tsdb-bench.c:76-93, 176, 235, 329-337, 396, 582-590, 626-679 | creates `0 .. nseries` and **checks `id == i`** (334), then inserts the index as the id | yes |
| scale-bench.c:45-79, 115-119, 175-181, 288, 329-335, 363 | "series s gets id s" and the check `id != s`; `series = %u` in the SQL for ClickHouse | yes |
| tsdbc-bench.c:30, 56, 98-106, 205-209, 257-262, 377 | creates `0 .. maxs` and ignores the ids, inserts the row file's index as the id; `malloc(batch * 4)` | yes |
| insert-bench.c:17, 41-43, 61, 112-121, 176, 235 | `id = pos % nseries`, per-series state `st[id]`; ids of series created in volumes ignored | yes |
| cbench.h:47-55, 96-101, 123, 155-262 | `rbrow {u32 s; i64 t; f32 v}`, the 16-byte RowBinary row file shared with ClickHouse; `count[s]` arrays by id | yes |
| threads-bench.c:29-44, twa-bench.c:31-130, datasets.h:74-77 | one `uint32_t` id; `ds_init(.., uint32_t series)` seeds a generator with an index | type only |

The benchmark row files (`rbrow`, the Rust/Java/Python bench `"<Iqf"` rows) are a file format of the benchmarks,
also loaded into ClickHouse; their `u32` stays and becomes what it always meant, an index into the bench's own
table of ids.

### 1.12 Tests

Counts of 32-bit id declarations per file: test_server 36, test_tsdb 32, test_classes 30, test_failstop 25,
test_client 20, test_delete 20, test_spread 17, test_server_multi 14, test_twa 14, test_crafted 13, test_export 12,
test_limits 12, test_damage 11, test_follow 10, test_types 10, test_stability 8, test_volumes 7, test_multi 7,
samekeys.h 7, test_lttb 6, fuzz_frames 5, test_gindex 4, jepsen 3, test_ports 3, others 1-2. What breaks beyond the
types:

- **Literal ids** passed without creating a series (test_delete 22, test_server 13, test_tsdb 12, test_client 11,
  fuzz_frames 3, test_follow 2, test_lttb 1; e.g. test_tsdb.c:665 `tsdb_insert_series(db, 5, ..)`, test_tsdb.c:1000
  `tsdb_append(db, 0, ..)`, test_delete.c:704 `tsdb_set_retention(db, 99, 5) != 0` as an unknown id).
- **Dense-id checks**: test_tsdb.c:623 `ids[i] == i`, test_delete.c:179 `id == s`, test_crafted.c:808, 815; the
  fake server of test_client.c:303-320, 430-455 answers a 4-byte id 42.
- **`tsdb_series_count` loops**: test_export 10 (e.g. 736), test_volumes 6, test_delete 1, test_tsdb 1, the volume
  fuzzer 1.
- **Hand-built frames**: test_server.c `raw_insert` (188-195), `raw_query_agg` (853-858) and 2814-3151 (14 copies,
  25 uses); test_server_multi.c `multi_frame` (306-322, `n * 4` and the pad), `values_tmpl` (325-330), `big_id`
  (378, 697-699).
- **Fuzz corpus**: tests/fuzz/fuzz_frames.c writes the 79 seeds in tests/fuzz/frames/ with `p32(&p, id)`
  (414-505; fixture ids 0-4 at 149-180, packed seeds with `uint32_t series[20]`); regenerated with
  `fuzz_frames seeds DIR`.
- **Unaffected but retyped**: tests/integration (c/itc.c:38-391 keeps ids from `tsdbc_series`; run.py, gen.py and
  mutate.py work by name), tests/jepsen/jepsen.c:139 (`uint32_t ids[MAX_SERIES]` filled by `tsdbc_series`),
  test_classes.c:49 (`tsdb_test_class_switch` prototype).

Client and binding tests are in §1.7-1.10.

## 2. The change

### 2.1 The id

```c
typedef uint64_t tsdb_sid;
#define TSDB_SID(volume, local) ((((tsdb_sid)(volume) + 1) << 32) | (uint32_t)(local))
#define TSDB_SID_VOLUME(id) ((uint32_t)((id) >> 32) - 1)   /* valid ids only */
#define TSDB_SID_LOCAL(id) ((uint32_t)(id))
#define TSDB_SID_NONE 0                                     /* never a series */
```

| bits | field | range |
|---|---|---|
| 0-31 | `local`: the record index in the volume's `SERIES` | 0 .. 2^31 - 2 (`DICT_MAX_NAMES`, series.c:30) |
| 32-47 | `volume + 1`: the volume's number plus one | 1 .. 65535 (`MAX_VOLUMES`, volumes.c:249) |
| 48-63 | zero | an id with any of them set is not a series |

**Why the volume and a local id, not a dense global counter.**

- Routing needs no table. Today a global id is looked up in `cat_vol`/`cat_local` under `t->lock`; at 1 billion
  series those two arrays and the name index `by_name` take 16-32 GB of RAM, and `CATALOG` is a 16 GB file read
  and hashed at every open. With the volume in the id, `route()` is `id >> 32` and a bounds check.
- Creation touches only its volume. Today a new series is two durable appends, the volume's `SERIES` and then the
  root's `CATALOG` (volumes.c:1033-1040), both under the global `create_lock`, with a crash window between them that
  `adopt_orphans` repairs. Without CATALOG it is one append in one directory: one fsync fewer per series, and a
  root on a slow or absent NFS server no longer stalls creation in other volumes (the point of D9 in docs/ha.md).
- The engine keeps its dense local ids, which several structures need: the memtable's `mt_series` array, the
  retention array and the class byte table are indexed by local id (internal.h:1056, 1210, 1222). A global 64-bit
  id inside the engine would turn each into a hash map on the insert and query paths.
- It is the id docs/ha.md §5 designed for failover, so failover (after 1.0) needs no second id change.

**Why `volume + 1`.** ha.md §5 writes `vol << 32 | local`, which gives the database directory's series ids below
2^32, the same values as today. Then an embedded database without volumes (most tests, most bindings tests) never
has an id above 2^32, and a truncation to 32 bits anywhere (a C cast, a Java `int`, a Rust `as u32`, a Python
`"I"` format, a `%u`) goes unnoticed until someone adds a volume. With the `+ 1`:

- every id of every database is at least 2^32, so every test exercises the high half;
- a truncated id has a zero volume field and is refused (INVALID or NOTFOUND), never taken for another series;
- 0 is never an id, which gives callers a "no series" value (`TSDB_SID_NONE`).

**Why bits 48-63 stay zero.** Ids then fit a Java `long` without a sign and a double (JSON, JavaScript) exactly, and
the bits stay free for something a later format needs (docs/ha.md D36 decided against an incarnation number in the
id; this keeps that door open without a format change). The library and tsdbd refuse an id with any of them set
like any unknown id.

**Ids are never reused.** `SERIES` is append-only and volumes are never removed, so `(volume, local)` names one
series for the life of the database, as today's global id does. (A storage rollback that shortens `SERIES` is the
failover design's concern: LOST entries, docs/ha.md D36.)

**Limits.** 2^31 - 1 series per volume, 65,535 volumes (the database directory and 65,534 more): about 1.4 x
10^14 series. The production target, 10^9 series over 1000 volumes, is 10^6 per volume. At exhaustion:

- a volume whose `SERIES` holds 2^31 - 1 names refuses a new series with `TSDB_ERR_INVALID` before anything is
  written (series.c:28-30, unchanged); existing series and other volumes go on; the operator adds a volume;
- `volume add` refuses a 65,535th extra volume with `TSDB_ERR_INVALID` (volumes.c:446, unchanged).

Neither limit is new. What goes away is today's implicit limit of 2^32 series in the whole database (the width of a
global id), which 1000 volumes of 10^6 series do not reach but 3000 volumes of 2 x 10^6 would.

### 2.2 What does not change, and why

| what | why it stays |
|---|---|
| every file inside a volume: `SERIES`, `STATES`, `RETENTION`, `CLASSES`, WAL segments, parts (granule index run table, tombstones, footer), `MANIFEST` | they hold local ids, and the volume is the directory they are in; widening them would add 4 bytes per WAL row (20 to 24, +20%) and per run, and gain nothing |
| the engine's types (`uint32_t series` in `src/` outside volumes.c and multi.c) | the same: the engine sees one volume; `TSDB_SID_LOCAL` is applied once, in `route()` and `insert_slots` |
| state codes (`uint32_t code`, STATE replies, `Column::State(&[u32])`, Java `putInts` for codes) | codes are per volume and small; not series ids |
| volume numbers in the API (`tsdb_volume_path(uint32_t index)`, `tsdb_get_volume_stats(uint32_t volume)`) | at most 65,535 |
| `tsdb_series_result.index`, QUERY_MULTI's `u32 index` per reply frame, `TSDB_MULTI_END` | positions in the caller's array (at most 65,536 per request), not ids |
| CANCEL's `u32 id`, the frame id | a request id |
| the export stream (docs/export-format.md, stream version 1) | it carries no ids at all: a SERIES record is name, volume path, type and retention, and rows follow their SERIES record; ids are assigned by the importing database (§3.3) |

### 2.3 The routing layer

`src/volumes.c`:

- `route(tsdb *t, tsdb_sid id, eng **e, uint32_t *local)`: refuse `id >> 48 != 0` and `id >> 32 == 0`; `v = (id >>
  32) - 1`; `v < nvols` and `local < eng_series_count(vols[v])`, else unknown. A reader (`read_only`) that finds `v`
  beyond its volumes re-reads VOLUMES; one that finds `local` beyond the count calls `eng_refresh(vols[v])` (which
  reloads `SERIES`) once and checks again. This replaces `catch_up`'s CATALOG reading.
- `struct tsdb` loses `cat_vol`, `cat_local`, `ncat`, `cat_cap`, `by_name`, `name_cap`; the functions `gid_name`,
  `names_grow`, `names_find`, `cat_add`, `cat_check`, `cat_record`, `cat_valid`, `adopt_orphans`, the CATALOG half
  of `load_layout`, and the CATALOG append in `series_make` go.
- **Name lookups.** Names stay unique in the whole database (today's rule; see open question 2):
  - `tsdb_series_at(name, path)` / `tsdb_series_create(.., path)`: look the name up in the named volume first (one
    probe of that volume's own hash table, `series_get(.., create=false)`); found: done, as fast as today. Not
    found: under `create_lock`, probe every other volume (a series found there is returned, as today: "it stays in
    the volume it was created in"), else create it in the named volume with `eng_series`. A probe is one hash
    lookup in memory, so 1000 volumes cost on the order of 100 µs, next to the fsync a creation does anyway.
  - `tsdb_series(name)`, `tsdb_series_lookup(name)` without a path: the database directory first, then every volume
    in order. A caller with many volumes should name the path; tsdbd's SERIES with a path takes the fast path.
    The Pulsar sink names no path today (TsdbConnection.java:97-110: `c.series(name, false)`, `c.seriesCreate(name,
    type)`), so it creates every series in the database directory and its lookups hit on the first probe; see open
    question 9.
  - No global name index is kept: it would cost what `by_name` costs today (8-16 bytes a series), and the per-volume
    tables already exist.
- `insert_slots`: validate every id, then counting-sort rows by `TSDB_SID_VOLUME` into a `uint32_t` local array
  per volume, as today's multi-volume path does. The `identity` shortcut (the root alone: the caller's array goes to
  the engine as is, volumes.c:1235-1242) cannot survive, since the caller's array is now 64-bit; the root-only case
  becomes one pass that narrows the ids into a scratch array. `insert_series_slots` and the single-series paths
  route once.
- **Listing.** Ids are no longer `0 .. count - 1`. `tsdb_series_count` returns `uint64_t` (the sum over volumes);
  new: `uint32_t tsdb_volume_series_count(tsdb *, uint32_t volume)` (a volume's ids are `TSDB_SID(volume, 0 ..
  count - 1)`), and `int tsdb_series_foreach(tsdb *, int (*fn)(void *ctx, tsdb_sid id), void *ctx)` for callers
  who want every series (volume order, then creation order). The listing order changes from global creation order
  to volume order; nothing promised the old one.
- `tsdb_series_path`, the state functions, the test hooks: `tsdb_sid`.

`src/multi.c`: `mcall.series` and the API arrays become `const tsdb_sid *`; `run_one` passes the id on. The
per-volume cuts (`cur_cuts`) already work per volume and do not care how the id is laid out.

`src/calendar.c`: the three `*_local` functions take `tsdb_sid`.

### 2.4 Disk format 8

`FORMAT_VERSION` goes from 7 to 8 (src/internal.h:38), written into `META`, `MANIFEST` and every part as now:

- **CATALOG is neither read nor written.** A format-8 database never has one.
- **Each volume's META records its number**: `volume=N` (0 in the database directory). It is written when `volume
  add` knows the number, under the VOLUMES write lock, before the line is appended (the directory's META is
  rewritten atomically, tmp + rename + directory fsync, as `meta_write` does; today a new volume's META is a
  copy of the root's, volumes.c:405-413, so the copy gets its own `volume=` line). Every open checks that the META of the
  directory on line N says `volume=N`, else fails with `TSDB_ERR_CORRUPT` and a line naming both. A directory whose
  META has a number no line gives it is a crashed add (the case `volume_dir_prepare` already recognises by
  ROOT_ID and META alone) and is rewritten by the retried add.
  Why: the volume number is now inside ids that clients hold for a long time (the Pulsar sink caches them). The
  VOLUMES checksum binds a path to its line (volumes.c:187, seeded with the line number), so a reordered file is
  caught today; META's number also catches one directory reached by two paths (a bind mount, a second mount of one
  export), or a VOLUMES rebuilt from a list, which would otherwise send a cached id's writes to another series
  without a word. This is the `(number, uuid)` check of docs/ha.md §5 without the uuid.
- The open of a format-7 database is refused by the existing check (db.c:234-241) with the existing message, which
  names both versions:

  ```
  tsdb: the database has disk format version 7; tsdb 0.9.0 reads version 8 only. Export the data with the
  release that wrote it and import it here (docs/compatibility.md)
  ```

Nothing else in a volume changes. The bump is needed although the data is laid out as before: the meaning of the
database's ids changes, a format-7 CATALOG would be silently ignored, and META gains a field the open relies on;
the rule (AGENTS.md "Versions", docs/compatibility.md) is that a layout change bumps the version and the old one is
refused. Since 0.9.0 is not tagged, no released database exists in format 7, and nothing needs migrating; anyone
with a format-7 database from main exports it with that build and imports it with the new one (open question 3
asks whether an in-place upgrade is wanted instead).

### 2.5 Protocol 11

`TSDB_PROTOCOL_VERSION` goes from 10 to 11. Every id on the wire becomes a `u64`; nothing else changes.

**HELLO.** The server answers the lower of both versions, as now, but refuses a client below 11: status
`TSDB_ERR_INVALID` and the connection is closed. A client whose server answers below 11 fails its connect with
`TSDB_ERR_INVALID` and a message ("tsdbd speaks protocol N; this client needs 11: upgrade the server"). A version-10
peer would put a u32 where a u64 is read, so there is no operation both could do safely, and AGENTS.md forbids
legacy paths for earlier protocol versions before 1.0. The existing gates (3 to 10) then always pass; they are
removed in a later commit (§5).

| op | version 11 | size change |
|---|---|---|
| SERIES, SERIES_CREATE | reply `u64 id` | +4 |
| SERIES_NAME, SERIES_TYPE | `u64 id` | +4 |
| INSERT (version 1 layout, float32) | `u32 n, u32 0, u64 series`, then `i64 ts[n], f32 v[n]`; many-series: `series = TSDB_MANY_SERIES` (now `UINT64_MAX`), then after the values zero bytes to a multiple of 8 and `u64 series[n]` | +8 header; +4 a row with many series |
| INSERT_VALUES | header `u32 n, u8 type, u8 flags, u16 0, u64 series` (still 16 bytes, `TSDB_VALUES_HEADER` unchanged); many-series: zero bytes to a multiple of 8, `u64 series[n]` | 0 header; +4 a row unpacked |
| INSERT_VALUES with `TSDB_PACKED` | header as above; the id column in the zstd frame stays zigzag varint deltas, now of 64-bit values with wrapping subtraction (client/tsdb_pack.h, Rust pack.rs, Java Pack.java); the raw bound is 10 bytes a row instead of 5 | about 0 (rows of one series: delta 0, one byte, as now) |
| REPLACE_RANGE | the INSERT_VALUES header, `i64 start, i64 end` (32 bytes, unchanged) | 0 |
| QUERY_RAW, QUERY_VALUES, QUERY_AGG, QUERY_GROUP, QUERY_BUCKETS, QUERY_BOUNDS, QUERY_LTTB, QUERY_TWA | leading `u64 series`; LTTB 32 / 41 bytes; `TSDB_TWA_HEADER` 60 | +4 |
| STATE | `u64 series, u8 create, u16 len, name` | +4 |
| STATE_NAME | `u64 series, u32 code` | +4 |
| DELETE_RANGE | `u64 series, i64 start, i64 end` (the reserved u32 is taken) | 0 |
| SET_RETENTION | `u64 series, i64 limit_ms` | 0 |
| GET_RETENTION | `u64 series` | +4 |
| QUERY_MULTI | `u64 series[n]` (8-aligned by itself: the zero padding after it disappears); the template's series field is a `u64` set to `UINT64_MAX` | +4 a series |
| everything else (replies of queries, QUERY_MULTI reply frames and END, CANCEL, STATS, LAST_STATS, COMMIT, FLUSH, OPTIMIZE) | unchanged | 0 |

Every id array starts at an 8-aligned offset of the payload, which tsdbd mallocs (16-byte aligned, tsdbd.c:175),
so the server keeps reading the arrays in place as it does now (tsdbd.c:960, 1143, 1310), as `const uint64_t *`.

An unknown id (a zero volume field, high bits set, a volume or local id beyond what exists) gets what an unknown id
gets today: `TSDB_ERR_INVALID` for writes, `TSDB_ERR_NOTFOUND` for SERIES_NAME, SERIES_TYPE and the state and
retention lookups, status 0 and no rows for a series of QUERY_MULTI.

**Clients.** All four speak 11 in the same commit (AGENTS.md, "Protocol"), with the API types: C `tsdb_sid`
(`uint64_t`); Rust `u64` (a `pub type SeriesId = u64` alias for readability); Python `int` (arrays: `uint64`
buffers, and `uint32` buffers widened on input so existing numpy code keeps working); Java `long` and `long[]`
(id methods taking `int` are removed, not overloaded: `insert(int, ..)` beside `insert(long, ..)` would silently
widen a truncated id, which is the bug this change must surface).

### 2.6 Embedded bindings and the C ABI

- rust-sys: every `u32` id becomes `u64`, `tsdb_series_count() -> u64`, the new functions of §2.3,
  `tsdb_series_result.series: u64`; `layout.c` and `tests/layout.rs` follow.
- Java `Native.java`: `SERIES_RESULT = {SIZE_T index, JAVA_LONG series, JAVA_INT status, JAVA_INT last, SIZE_T n,
  ..}` (the padding after `last` goes); every id descriptor `JAVA_LONG`; `SERIES_COUNT` returns `JAVA_LONG`.
- **The size check does not catch this.** `tsdb_series_result` keeps its size (72 bytes on x86-64) while `status`
  and `last` move from offsets 12 and 16 to 16 and 20, and `Native.checkAbi` compares sizes only, so an old jar
  on a new `libtsdb.so` would load and then read statuses from the wrong place and pass `int` ids where the
  library reads `uint64_t`. Add `int tsdb_abi_version(void)` to tsdb.h (a number raised with every incompatible
  change of a public struct or signature; it starts at 1 with this change) and refuse a library whose number
  differs in `Native` at load and in the Rust and Python `Db::open`. `tsdb_struct_size` stays.
- Python `Db`, Rust `Db`, Java `EmbeddedDb`: as the clients in §2.5; `seriesCount()` returns `long` and the
  "ids are 0 .. seriesCount() - 1" doc goes; listing uses the new functions.

## 3. Compatibility

### 3.1 Disk format

Format 8 reads format 8 only. A format-7 directory (from main before this change; no release wrote one) is refused
at open with the message of §2.4, touching nothing. A format-8 database is refused by a format-7 build with the
same message the other way round. docs/compatibility.md gets a row for 8: "CATALOG removed: series ids name their
volume; each volume's META holds its number".

### 3.2 Protocol

Protocol 11 speaks to protocol 11 only, refused at HELLO either way (§2.5). docs/compatibility.md's paragraph on
HELLO is updated: within 0.9.x a release that changes the protocol may raise the floor, which this one does. From
1.0 on, the floor is the 1.0 version and stays.

### 3.3 Export and import

The stream is unchanged and stays version 1: it names each series by `str32 name` and `str32 volume path`
(docs/export-format.md, SERIES record), never by id, and the STATES record's `u32 code` is a state code. The
exporter (`tools/dump.c`) walks the series with `tsdb_series_foreach` instead of `0 .. count - 1`; the importer
creates each series by name and path and uses whatever id the target gives it. So:

- every stream written so far, `tests/data/export-v1.tsdbdump` included, imports into format 8 unchanged;
- a format-7 database is carried over by `tsdb export` with the old build and `tsdb import` with the new one;
- no stream version 2 is needed. If a later format ever puts ids into the stream (it should not: they are
  meaningless in another database), that is a new optional record kind under the rules of export-format.md.

The order in which series appear in a stream changes from global creation order to volume order; the format does
not promise an order.

### 3.4 Ids held outside the database

Ids are not stable across an export and import (they never were), and a client holding ids from a format-7
database must resolve them again by name; the Pulsar sink does so at start (`SeriesCache` is filled by `series()`
calls). Nothing persists ids outside a database today (checked: the connector's cache is in memory).

## 4. Cost

| what | today | format 8 / protocol 11 | change |
|---|---:|---:|---|
| WAL, per row | 20 B | 20 B | 0 (local id) |
| memtable, per row | 24 B (`ts`, slot, `lsn`; the id is the array index) | 24 B | 0 |
| granule index | ~32 B an entry; run table 8 B per series and part | the same | 0 bits |
| tombstone | 24 B | 24 B | 0 |
| `SERIES` / `CLASSES` / `RETENTION` per series | 8 B + name / 24 B a switch / 16 B a limit | the same | 0 |
| `CATALOG` per series (root, on disk) | 16 B | none | -16 B (-16 GB at 10^9 series) |
| routing RAM per series (`cat_vol` + `cat_local` 8 B, `by_name` u32 slots at load <= 1/2: 8 B; up to twice each after doubling) | 16-32 B | 0 | -16 to -32 B (-16 to -32 GB at 10^9) |
| open | reads and hashes all of CATALOG | reads VOLUMES only | faster with many series |
| creating a series in a volume | 2 durable appends (volume, root) | 1 | -1 fsync |
| a request about one series (queries, SERIES_NAME, STATE, GET_RETENTION) | | | +4 B |
| DELETE_RANGE, SET_RETENTION, INSERT_VALUES / REPLACE_RANGE header | | | 0 |
| unpacked many-series insert, per row | f32 16 B, f64 20 B | 20 B, 24 B | +4 B (+25% / +20%) |
| packed many-series insert (what the clients send to a server of 3 or more) | zigzag varint deltas, zstd | the same, of u64 | ~0 (a volume change between consecutive rows: a 5-byte varint instead of ~1-3) |
| QUERY_MULTI request | 4 B a series (65,536: 256 KiB) | 8 B (512 KiB) | +4 B a series |
| embedded many-series calls (`tsdb_insert`, `tsdb_query_multi`) | caller's id array 4 B a row | 8 B a row | +4 B a row; the database-directory-only insert path gains one narrowing pass (well under 1 ns a row against tens of ns a row for the insert) |

**Benchmarks.** No size, compression or query number in AGENTS.md or the README should move: nothing per row or
per granule changes on disk, and query requests grow by 4 bytes. To confirm, on a quiet machine, mean of several
runs, before and after:

- `build/insert-bench` (the rows through `tsdb_insert` with 1000 series per call: the narrowing pass);
- `build/tsdbc-bench load` and `query` (wire sizes; the load uses packed frames);
- `build/tsdb-bench` load (embedded inserts) and its size line, which must be identical;
- a series creation rate with volumes (no benchmark has one; `scale-bench load` creates 10,000 series), where the
  missing CATALOG fsync should show as faster creation.

## 5. Implementation plan

Each commit builds and passes its gates on its own. The protocol commit carries all four clients (AGENTS.md).

1. **`tsdb_sid` in the C API, values unchanged.** `typedef uint64_t tsdb_sid` in tsdb.h; every id parameter, out
   pointer, array and the `tsdb_series_result.series` field becomes `tsdb_sid`; `tsdb_series_count` returns
   `uint64_t`; add `tsdb_series_foreach` (correct under either numbering) and `tsdb_abi_version`. The `TSDB_SID`
   macros and `tsdb_volume_series_count` wait for commit 3, where they become true. `volumes.c` still hands out dense CATALOG ids (now in a 64-bit type; an id at or above
   2^32 is simply unknown). tsdbd narrows ids into its version-10 frames (safe: every id is still below 2^32), and
   the C client keeps its API for now. The embedded bindings change with the ABI: rust-sys (FFI, `layout.c`,
   `tests/layout.rs`), Rust `Db`, Python `Db`, Java `Native`/`EmbeddedDb` (descriptors, `SERIES_RESULT`, the ABI
   check), and their tests. Tools and tests use `tsdb_sid` and `PRIu64`; listing loops use the new functions.
   Gates: `make test`, `make asan`, `make test-clients`.
2. **Protocol 11.** `include/tsdb_protocol.h` (§2.5), tsdbd's parsing and replies, the HELLO floor; the C client
   (`tsdb_client.h` ids become `tsdb_sid`; `tsdb_pack.c` 64-bit deltas); the Rust, Python and Java clients (§2.5),
   the Pulsar connector (`int` ids to `long`), the fuzz frame corpus (tests/fuzz/frames) re-encoded, the wire-offset
   tests (Python test_zones.py, test_packed.py; Java CompressionTest, TimeZoneGroupTest). Ids are still below 2^32
   here. Gates: `make test` (both IO backends), `make test IO=epoll`, `make test-clients`, `make integration-tests`,
   a short `make fuzz-frames`.
3. **The id layout and format 8.** volumes.c as in §2.3 (route by bits, CATALOG removed, name lookups by probe,
   listing, the `TSDB_SID` macros and `tsdb_volume_series_count`), `META volume=N` (§2.4), `FORMAT_VERSION 8`, `tsdb_abi_version` raised if anything moved since commit 1.
   From here every id is at least 2^32, so any truncation left in a client, a binding or a test fails. Tools: the
   CLI's `series` listing and output (`PRIu64`), `dump.c` export loop. Gates: `make test`, `make asan`,
   `make tsan`, `make jepsen` (series creation and the open change), `make test-clients`,
   `make integration-tests`, `make dist` and `make dist-test`, `make check-version`.
   The benchmarks that assume dense ids (§1.11: tsdb-bench's `id == i` check, scale-bench's `id != s`,
   tsdbc-bench and insert-bench inserting an index as the id, cbench's arrays by id) fail here and are fixed in the
   same commit: each keeps the ids it gets from `tsdb_series*` in a table and indexes that.
4. **Bench files and numbers.** The bench row files (`cbench.h` `rbrow`, Rust insert_bench / embedded_suite, Java
   BenchCommon, Python bench_insert `"<Iqf"`, the RowBinary for ClickHouse) keep their `u32`, documented as an index
   into the bench's id table, so files already generated stay usable and ClickHouse's tables do not change. Then
   the measurements of §4.
5. **Docs.** docs/compatibility.md (format 8, protocol 11, the HELLO floor), CHANGELOG.md ("Changed": series ids
   are 64-bit; CATALOG removed; protocol 11; format 8 with the export/import note), AGENTS.md (format history,
   protocol history, the Volumes paragraph, the series id rule), docs/design.md (CATALOG passages at 424, 761-797),
   docs/export-format.md (one sentence: the stream carries no ids, ids are the target's), docs/ha.md §5 (the
   `+ 1`, CATALOG already gone), docs/roadmap.md (the row done), client READMEs, `tsdb_client.pyi` docs.
6. **Cleanup.** Remove the client gates for versions 3 to 10 (they always pass) and their tests of older servers.

### Tests to add

All with ids above 2^32, which commit 3 makes the only kind there is; where a test needs a second volume it adds one.

- **Ids:** `TSDB_SID` round trips; ids of the database directory are `2^32 + local`, of volume v are `(v+1) << 32
  | local`; an id with a zero volume field, a high bit set, a volume beyond `tsdb_volume_count`, or a local id beyond
  the volume's count is refused by every entry point (insert, append, delete, replace, retention, state, every
  query, multi, series name/type/path/class) with the documented code, and a 32-bit truncation of a real id is
  refused, not routed (tests/test_volumes.c).
- **Insert and query** through every path with ids in two volumes: `tsdb_insert` with rows of both volumes
  interleaved (stable order, last write wins), the database-directory-only narrowing path, `tsdb_append`, large
  inserts straight to parts.
- **QUERY_MULTI and `tsdb_query_multi`** over series of three volumes, duplicates, unknown ids (status 0, no rows),
  index order (tests/test_multi.c, tests/test_server_multi.c).
- **Delete and replace** on series in a volume (tests/test_delete.c).
- **WAL replay**: kill after acknowledged appends to series in two volumes, reopen, rows and ids intact (a case in
  tests/test_volumes.c and the jepsen `kv` workload with a volume).
- **Creation**: one append per series (no CATALOG file after creating series in volumes); a crash between the
  volume's SERIES append and the reply leaves a series that the next lookup by name finds (crashfs); name
  uniqueness across volumes kept (creating an existing name with another path returns the existing id).
- **Followers**: a reader resolves an id created after it opened (SERIES reload), and one in a volume added after
  it opened (VOLUMES reload) (tests/test_follow.c).
- **META**: a volume whose META number differs from its VOLUMES line fails the open with `TSDB_ERR_CORRUPT` and a
  line naming both; a crashed `volume add` (META written, no line) is completed by the retried add.
- **Format 7 refused**: a directory with `version=7` in META fails `tsdb_open` with `TSDB_ERR_CORRUPT` and the exact
  message of §2.4 (tests/test_damage.c), and the CLI and tsdbd exit 1 having touched nothing.
- **Export and import**: a database with series in two volumes round-trips (names, paths, types, retention, rows),
  with different ids in the target; `tests/data/export-v1.tsdbdump` still imports (tests/test_export.c).
- **Protocol**: HELLO 10 refused by tsdbd, a client refusing a server that answers 10 (each client); each op with an
  id above 2^32 (tests/test_server.c, tests/test_client.c, each client's tests); packed inserts whose consecutive
  ids jump between volumes (wrapping deltas, both directions); `UINT64_MAX` refused as an id; the fuzz corpus
  replayed.
- **Bindings**: rust-sys and Java layout tests with the new `tsdb_series_result` offsets; `tsdb_abi_version`
  mismatch refused by `Native` (LayoutTest) and the Rust/Python `Db`; Python accepts `uint64` and `uint32` id
  arrays and refuses a negative or > 2^64 id.
- **Pulsar connector**: SeriesCache with `long` ids above 2^32 against tsdbd (TsdbdSinkTest).

### Gates

`make test` (and `IO=epoll`), `make asan`, `make tsan`, `make jepsen` (kv and classes), `make test-clients`,
`make integration-tests`, `make fuzz-frames` (a short run), `make dist` and `make dist-test`, `make check-version`,
and for the connector `mvn test` in connectors/pulsar. The benchmarks of §4 last, on a quiet machine.

## 6. Open questions for the user

1. **`(volume + 1) << 32 | local` or ha.md's `volume << 32 | local`?** The `+ 1` makes every id at least 2^32
   (truncation bugs cannot hide in databases without volumes) and keeps 0 free; ha.md §5 would be updated to
   match. Recommended: `+ 1`.
2. **Names unique in the whole database, or per volume?** Today a name exists once in the database, and
   `tsdb_series_at` returns the existing series wherever it is. ha.md D9 makes names unique per volume (the path
   is part of the address). Keeping today's rule costs a probe of every volume when a name is created and of every
   volume when a lookup names no path (§2.3), and no memory. Recommended: keep today's rule now, decide again with
   failover.
3. **Format 8 with a refusal, or open format 7 in place?** The data is the same; an upgrade would only write
   `volume=N` into each META and delete CATALOG. With no release in format 7, the refusal (and export/import for
   anyone running main) is simpler and follows the rules. Recommended: refuse.
4. **The volume number in META now, or with failover (with the uuid of ha.md §5)?** Recommended: now; it guards
   the ids clients cache, and is small.
5. **The version-1 ops QUERY_AGG and QUERY_GROUP**, which no client sends (C, Rust and Java use QUERY_BUCKETS;
   §1.6): widen them like the rest, or remove them in protocol 11 (docs/ha.md §9 suggests it) and save widening
   and testing two parsers in tsdbd? Recommended: remove. INSERT and QUERY_RAW are still sent by the C and Java
   clients and are widened.
6. **The HELLO floor of 11** (no version-10 client or server works with an 11 one): acceptable for 0.9.0?
   Recommended: yes, nothing has been released.
7. **Ids documented or opaque?** This design documents the layout and gives `TSDB_SID_VOLUME`, so a client can
   split a batch per volume (what failover's direct mode needs). The alternative is to call ids opaque and keep
   the freedom to change the layout before 1.0.
8. **`tsdb_abi_version()`**: add it (recommended: the size check alone misses this very change), or rely on
   using the same release for the library and its bindings?
9. **Volumes for the Pulsar sink.** The sink creates every series in the database directory (no path in its
   SERIES requests), so the production layout of 1000 volumes needs a way for it to pick a volume per series (a
   mapping from topic or name prefix to a volume path in its config). Not part of this change, but it decides
   whether lookups without a path (the slow case of §2.3) matter in production. In scope here, or a follow-up?
