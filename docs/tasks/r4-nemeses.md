# R4's `longdown` and `rebuild` nemeses of jepsen-cluster: written, not yet run whole

**Open (from the r4-docs review, 2026-11-25). Owner: R4's gate (the next track that runs `make jepsen-cluster`).**

`tests/jepsen/jepsen_cluster.c` has `longdown` (a replica down past `repl_retain_s` under writes, then back: it must
take a snapshot) and `rebuild` (a stopped replica's `v<N>` removed, the server started, `replica reset`, rebuilt by a
snapshot). Both have selftest mutants, but their `built` flag in the `r4nem` table is 0: each is refused with a line
until the flag is flipped, because neither has been run whole against the merged R4. Until then R4's rebuild by
snapshot and `replica reset` have their evidence in `test_cluster` (`snap*`, `addsnap`, `keepcap`, the reset
scenarios) and in `test_snapshot`, and none in jepsen-cluster.

What is needed:
1. `JEPSEN_R4_TRY=1 make jepsen-cluster JEPSEN_NEMESES="longdown rebuild"` on both IO backends, a few seeds each, with
   the final-state checks (members of the final record compared, none a learner, no `v<N>.snap`, `v<N>.old`,
   `v<N>.removed-*` or `snap_*` left).
2. A failure is either a harness bug (fix it) or a wrong answer of tsdbd (a violation; never repeated).
3. Flip both `built` flags to 1 in `r4nem`, make `jepsen_cluster --built-nemeses` list them, and say so in
   design-cluster.md 13.2 and 14 (step 7), design-ha-modes.md (R4 and its jepsen row) and the CHANGELOG.
4. An erasure-coded volume that has settled a key cannot rebuild by snapshot yet (docs/tasks/r8-from-r7.md): the
   `rebuild` nemesis runs on the replicated kv volume only.
