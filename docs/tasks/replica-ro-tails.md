# A read-only open of a promoted replica's directory: the tail loop after the segment jump

**Open (from the replica-race review, F3, 2026-11-18; not reproduced).**

`follow_refresh` (src/manifest.c) reads the segments `follow_flushed + 1 .. follow_active` that MANIFEST names, one
tail per sequence number, and stops at `MAX_TAILS` (8). A missing segment reads as empty (ENOENT: not yet made, or
flushed away). After a promotion, segment numbers jump (`epoch << 40`): a MANIFEST that still says the old
`wal_flushed` with the new `wal_active` would fill the 8 tails with numbers that were never segments and never reach the
active one, so a read-only open would miss its rows.

The review's probe (a read-only open of the promoted writer's directory and of a member's, right after the first small
write after the promotion in `case_promote(0)` of tests/test_replica.c) passed: there `wal_flushed` had caught up before
the MANIFEST a reader saw. Not shown either way: a promoted replica that has not flushed since the jump.

To do, if read-only opens of replica directories are to be supported (jepsen-cluster's end check opens a stopped
replica's directory): a test case for exactly that state, and if it fails, walk only the segments that exist (a
directory listing between the two bounds) instead of every number between them.

## Status (r4-jepsen, after review)

`case_promote` of tests/test_replica.c now produces the state on purpose and asserts it from the MANIFEST file
(`man_bounds`): after the epoch's marker is applied and before `replica_promote`, the candidate's directory and the other
member's both have `wal_flushed` below 2^40 (1 to 4 in the runs) and `wal_active` at 2^40, and both are opened read-only
and compared with the old leader. The first version of the case only opened after the promotion, where the promotion's own
flush (and a rotation's) had already moved `wal_flushed` to 2^40, so it never reached the state; the opens after the
promotion stay as the second and third state.

What this does not prove: rows in the epoch's own segment (2^40). In these runs it holds the EPOCH marker only (the
promoted writer's first segment is 2^40 + 1, the replicas of the new leader take its records in that one), so a tail loop
that never reaches 2^40 loses no row here. If a segment above the old numbers ever holds rows while `wal_flushed` is still
below the jump, this state needs a case with those rows. Nor does it show that the new opens have teeth against a walk that stops early: a mutant of the tail loop that takes one
segment only once `follow_active` is past 2^40 passed them, because these directories hold unflushed rows in at most one
segment below the jump (the earlier `case_ro_open` catches a walk that stops early everywhere). A case with rows in two
unflushed segments before the epoch's would. A walk of the segments that exist (a listing between the bounds)
would not depend on any of this.
