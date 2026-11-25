# A fuzz harness for bucketed queries over a damaged part

**Done.** Follow-up to the fix of `fold_into` (a damaged granule wrote outside a bucketed query's arrays). Implemented in `tests/fuzz/fuzz_bucketed.c` with seeds in `tests/fuzz/corpus/bucketed/`.

`granule_decode` now refuses a granule whose timestamps do not strictly increase inside its entry's
[ts_min, ts_max], and `fuzz_part` and `fuzz_codec` reach that check. Nothing fuzzes what the readers do with the
rows that pass it: every bucketer (dense and sparse group, spread, TWA, LTTB, minute and hour buckets, QUERY_MULTI,
the latest table) trusts the rows' order and window, and the next reader that trusts something more of the decoded
bytes (a value used as an index, a TWA step, a new bucketer) would be found by a crash in production.

The harness: take a small part written by the engine (one series, a few granules, two overlapping parts so that
`merge_stream` interleaves them), let the fuzzer mutate the granule payloads (not the index or the header, which the
open checks), open with `verify_checksums = 0` and run one query of each kind over the whole range and over random
windows, under ASan and UBSan. Any answer is allowed; a report is not. `tests/fuzz/fuzz_part.c` shows how a part is
built and mapped from fuzz input; `tests/test_damage.c` `test_ts_damage` runs the queries.
