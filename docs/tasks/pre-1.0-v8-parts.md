# Before 1.0: remove the reader of format-8 part headers

Step S0 (format 9) made `tsdb DIR upgrade` carry a format-8 database over in place without rewriting its parts: they keep
their 24-byte headers, META says `parts_v8=1` and `part_open_as` reads a part with a format-8 header (`hdr_len` =
`PART_HDR_V8`) only in such a volume, and only when header and footer both say 8. Merges of idle partitions and
`tsdb_optimize` rewrite those parts one at a time (`rewrite_alone` in `src/merge.c`), the open logs how many are left,
and `v8_parts_account` (`src/db.c`) takes the line out of META when none is.

The format freezes at 1.0, so this reader must not become part of it. Before the 1.0 tag:

1. Delete the `v8_ok` branch of `part_open_as`, `PART_HDR_V8`, `part->hdr_len` (always 48), `rewrite_alone`'s v8 test,
   `v8_parts_account`, `db->parts_v8` / `db->v8_meta`, the `parts_v8=` lines in `volumes.c` and `tsdb_upgrade`
   (`up_*` in `src/volumes.c`, the CLI command, `tests/test_upgrade.c`, the fixture `tests/data/v8-fixture.*`; keep the
   export `v8-fixture.tsdbdump`, which `test_export` can import: the stream is stable).
2. An open that finds a part with a format-8 header (a database that was upgraded and never had its parts rewritten)
   fails like any damaged part, and its message names the fix: run 0.9.x (the last 0.9 release) until the open
   reports no format-8 part, or `tsdb DIR optimize` there, or export and import.
3. Say so in docs/compatibility.md and the changelog of the release that removes it, and that the last 0.9.x release
   is the one to upgrade with.
4. Until then the allowance does no harm: nothing in a database created at 9 can use it (test_upgrade checks that a v8
   header where META does not allow it is damage).
