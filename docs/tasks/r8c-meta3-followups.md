# Erasure-coded volumes in the metadata (server/meta.c, server/cluster.c): what r8c-meta3 left open

**Open (from the r8c-meta3 review, 2026-11-24). Owner: R8 (r8f or a later R8 track).**

## An ec=K+1 volume of two replicas flapped its leadership

On test_cluster ecadd's six servers (cutlinks in TSDB_REPLD_TEST, chunk stores, servers restarted with new peer
ports), an ec=1+1 volume (two replicas, `min_isr` 1 from `meta_volume_min_isr`) changed its leader from epoch 1 on with
no cut file: each leader asked for an ISR of one, the cut logic (cluster.c, `promo_start` with META_GR_PLANNED) moved
the volume to the other replica, which did the same; the epochs ran past 23. The data link between the two nodes
apparently never came up. Not investigated: unknown whether it predates r8c for any 2-replica volume (`volcmd`, a
2-replica replicas volume, passes), and nothing tests a 2-replica EC volume now. What is needed before r8f relies on
small EC volumes: a scenario that adds an ec=K+1 volume, writes and checks that its epoch stays put, then the cause.

## The stripe group's rack preference

`place_stripe` spreads the positions over racks first (a preference only: it never refuses), where PLAN.md D12 says
"no rack rule". Kept, because it costs nothing when no member names a rack and spreads the group when they do; no test
sets racks for an EC add. Either PLAN.md records it, or it goes.

## Who proposes the rest

STRIPE_SET by a healer, GC_SUSPEND by anything but force-leader and a restore, and a `tsdb cluster restore` (nothing
proposes META_OP_RESTORE yet: its suspension of gc is reached only in test_meta's simulator), and the use of
`cluster_copy_caught_up_at` by the chunk server (r8e) and the healer (r8f).
