# Users in the cluster: answers from a stale copy, an empty list after a promotion, and what the users workload does not cover

Found by the users workload of `make jepsen-cluster` (tests/jepsen/cluster_users.h, track s2j-jusers). Two server
bugs and a list of what the workload cannot yet reach.

## 1. A no-op or a refusal is answered from the local copy (server/auth.c `admin_group`) -- DONE (track s2k-users)

Fixed: a read barrier (`group_sync` -> `cluster_user_sync`, the leader's commit index under its lease, then this copy's
users through it) and a check again before a no-op or refusal is answered; a no-op waits for the members. Tests:
`test_cluster userstale` (fails on 2c89230; also a no-op's OK waiting for a paused member) and `userlease` (a leader
cut off from its group refuses the barrier), mutants `nooplocal`, `syncnowait`, `nomembers`, `syncnolease` in
`make auth-mutants`; not tested: the barrier's term check (a SYNC before a new leader's first commit). The harness's
`noop` exemption is gone and its check 6 makes a refusal contradicting an acknowledged change a violation.

`admin_group` proposes only when the member's own copy says something changes (`pr.op != 0`). When the copy says "nothing
to change" (a grant already held) or refuses (NOTFOUND, INVALID, DENIED from `admin_change`) the answer goes out with no
proposal, no version condition and no wait for the members. Design-auth.md A32 ("two copies must never disagree on what was
acknowledged") covers only the proposal path. On a member whose copy lags (a pause just ended, a restart past its barrier
but behind, a member in the minority of a partition):

- `grant` of a grant the copy still holds, after the group committed its revoke: OK, nothing proposed. An acknowledged grant
  that holds nowhere.
- `revoke` of a grant the copy has not seen yet: NOTFOUND while the acknowledged grant stands in the group. Likewise
  `drop` / `passwd` NOTFOUND after an acknowledged add, `add` INVALID after an acknowledged drop.

Observed: the no-op OK acknowledged in under 1 ms, a member one entry behind denying 3 ms later (jepsen-cluster users,
kill, uring, seed 1). The stale-copy direction (a lagging member answering contradicting an acknowledged change) is by
the code only; 8 harness runs under partition, pause and kill did not hit it.

Fix: confirm a no-op or a refusal against the group before answering it (a read barrier: the leader's commit index, then
wait until this copy applied it, as the restart barrier does; or propose a conditional no-op on the version checked), then
wait for the members as for a real change. Regression tests: a `test_cluster` scenario (pause one member, revoke through
another, grant through the paused one on resume: must not answer OK without the revoke being undone, and a revoke refused
as NOTFOUND must not be answered while the grant stands), and in the harness remove the exemption in `ju_admin`
(`noop`, counted in the report) and add a check that a refusal contradicting an acknowledged earlier change is a violation.

## 2. A promotion empties the users watch for about 110 ms -- DONE (track s2k-users)

Fixed: `users_push` gives the watch nothing while the copy is older than what the watch last took (a lower restore
generation, or the same one and a lower applied index). Tests: `test_cluster userpromote` (an install held 2 s on the
promoted member; fails on 2c89230) and `userreplay` (a copy replayed from entry 1 passes through older, non-empty lists;
auth.c logs a list that goes back), mutants `pushrebuild` and `holdempty` (TSDB_CLUSTER_MUTANT) in
`make auth-mutants`; the harness no longer waits for the voters
(`ju_await_voters` removed).

A learner promoted to voter gets its Raft core and rebuilds its metadata copy from nothing (`core_boot`: `meta_init`);
`users_push` then gives auth an EMPTY list until the copy has applied past it. Every login on that node is refused (the
superuser's too) and open sessions end: a node add drops every session on that node. The harness waits for the voters
(`ju_await_voters`) to avoid it; without that wait the pause nemesis fails about 1 run in 3, so removing the wait is the
regression scenario of the fix. Fix idea: `users_push` keeps the last list while the copy is rebuilt, until the copy has
applied past it.

## 3. What the users workload does not cover

Still open. The laggard list stays off the wire reply of the user admin ops: the clients' calls (`tsdbc_grant` and the
rest) return a status only, so carrying it means a protocol and API change in all four clients (s2k-users left it).

- The CLI path (`tsdb cluster user ... --wait-ms`, the only one that reports members still behind) runs once, before any
  fault. The wire reply carries no laggard list (`admin_group` drops `lag[]`), so the checker binds a member that was
  running before a change by a timing rule (an acknowledgement under 3.5 s, the member open a second before it). A server
  that lets a member stay behind after a slow acknowledgement passes. Drive a share of the admin ops through the CLI and
  bind by its list, or put the list on the wire reply.
- The actor guard (A34: a demoted admin's change is refused): no workload step demotes an admin; the mutant `noguard`
  survives. A second admin whose superuser right is revoked while it acts would cover it.
- A failed push of the users watch (`TSDB_AUTH_TEST failpush_file`): nothing sets it; the mutant `pushnoretry` survives.
  `test_cluster` covers both; the workload does not.
- `barrierifempty` is caught in 1 of 3 seeds: not deterministic, so it is not in `jepsen-cluster-selftest`.
- The cluster is initialised with `tsdb cluster init`, then the superuser is added; `init --superuser` is not driven.
