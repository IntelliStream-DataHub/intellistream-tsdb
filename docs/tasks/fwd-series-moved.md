# A series made by path through a member that does not lead: MOVED instead of forwarded

**Open (found while building r4-move, 2026-11-24; not investigated).**

With `--forward on` (the default), `tsdbc_series_at(c, name, "plant")` sent to a cluster member that does not lead
volume `plant` answered `TSDB_ERR_MOVED` (-10) for the whole of the client's retry budget (30 s), while INSERTs of
series that already existed, sent to the same member over the same kind of connection, were forwarded to the leader
and acknowledged. `route_decide` (server/tsdbd.c) places SERIES and SERIES_CREATE by their path, so the entry should
forward such a frame like any other; something on the forwarding path (the entry's `fwd_entry`, or the owner's
`fwd_serve` and its gates) answers MOVED for it instead.

Seen in `test_cluster moveleader` and `moverep0` before they were changed to make their series on the volume's leader
(`move_writers` in tests/test_cluster.c: the series are made through `lead`, the writers then write through any
member). Expected by design (docs/design-cluster.md 6.4, S2-2): a client that names a path reaches the leader in one
hop through any member, creates included.

To do: a `test_cluster forward` case that creates a series by path through a non-leader and requires the id back; find
which side answers MOVED (TSDB_REPLD_TRACE, the entry's and the owner's logs); fix; a `make fwd-mutants` row if the
fix is a rule of fwdreq.c.
