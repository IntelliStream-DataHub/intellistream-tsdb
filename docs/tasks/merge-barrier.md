# Follow-up to the merge-wait fix (0.9.1)

0.9.1 ends an insert's wait for merges when a partition has a damaged part (`db_wait_merges`), and leaves the whole
partition out of merges (`select_merge_locked`, `eng_optimize`, the retention rules) while a part of it is marked.
Two things were left out on purpose:

- **A damaged part as a barrier.** Merges combine adjacent block ranges only, so the ranges below and above a damaged
  part could each still be merged: the damaged part's tombstones keep hiding lower blocks, and the tombstones of merged
  upper parts hide its rows as before. That bounds the partition's part count, which today grows without limit until
  the part is restored (mmaps and descriptors per part, slower queries). It touches `select_merge_locked`, the first-k
  selection of `eng_optimize`, the solo rewrite and the retention drops, and changes what "optimize leaves one part per
  partition" means: too much for a patch release.
- **A cap on the part count.** Refusing an insert would have to happen before anything is written (the wait comes
  after the publish), and memtable flushes add parts without waiting, so a cap would stop large inserts while small
  ones flow. It needs a new error path (`TSDB_ERR_UNAVAILABLE`-like) in the engine and the four clients.

No new counter: `broken_parts` counts the part once (`part_mark_payload`), a field in `tsdb_stats` would change the C
structs the bindings use (`tsdb_abi_version`).
