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
