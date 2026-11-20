# A damaged granule can crash a bucketed query (src/query.c fold_into)

**Open. Found by r7-seam (2026-11-18) under its mutant `seamnoverify`; not fixed there (outside the track).**

`fold_into` (src/query.c, about line 980) indexes `acc[l[k].b - t->b0]` with bucket numbers computed from decoded
timestamps, without checking that they fall inside the query's buckets. A granule of a `.part` read without
`verify_checksums` (the default) whose bytes are damaged decodes to arbitrary timestamps, and `eng_query_group` then
writes outside `acc` (seen: SIGSEGV, backtrace `fold_into <- fold_emit <- merge_stream <- bq_dense`). AGENTS.md:
"decoders must be safe (no UB, no overread) on any bytes": wrong values are allowed there, a crash is not.

`.ec` parts are not affected: the seam checks every granule's crc before it is decoded (src/eccache.c).

Fix: drop (or clamp and count) a row whose bucket lies outside [b0, b0 + nbuckets) before it reaches `fold_into`, in
the dense and the sparse paths, and check the other bucketers (spread, TWA, LTTB) for the same indexing. Regression
test: a part with one granule's timestamp bytes flipped (as tests/test_seam.c case_bad builds one), read by
`eng_query_group` with `verify_checksums = 0`, under ASan: no report, any answer.
