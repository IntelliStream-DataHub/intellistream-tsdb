/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes (rset.h): NOT BUILT. r4-setchg builds them (repl_reconfig through the old record, the set version
 * on every message), r4-move the DROP_MEMBER of a move. Until then a driver keeps the replica set it opened with until
 * a restart, says so once, and sends with set version 0.
 */
#include <inttypes.h>

#include "rset.h"

void rset_change(struct rvol *rv, const struct cluster_vol_rec *r)
{
    if (rv->set_changed) /* a member's cursor and acks must never pass to another node: ignored, said once */
        return;
    rv->set_changed = 1;
    say("volume %" PRIu32 ": its replica set changed; this driver keeps the old one until a restart", r->number);
}

int rset_msg_ok(struct rvol *rv, const struct repl_msg *m)
{
    (void)rv, (void)m;
    return 1;
}

void rset_stamp(struct rvol *rv, struct repl_msg *m) { (void)rv, (void)m; }

void rset_cmd(struct rvol *rv, const struct cluster_volcmd *c)
{
    if (!rv->said_cmd_drop)
        say("volume %" PRIu32 ": DROP_MEMBER of node %" PRIu32 " at epoch %" PRIu64 ": replica set changes are not"
            " built yet; ignored", rv->number, c->node, c->epoch);
    rv->said_cmd_drop = 1;
}
