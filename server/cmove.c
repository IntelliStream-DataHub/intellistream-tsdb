/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica moves, the Raft leader's side (R4 plan D10, D11; cluster_int.h): NOT BUILT (r4-move). The admin op answers
 * "not built" (with the body it decoded: test_cluster r4stubs checks the CLI's encoding).
 */
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "cluster_int.h"

void cmove_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t v, from, to, flags;
    char t[160];
    int tl = cluster_volume_move_decode(body, n, &v, &from, &to, &flags) != 0
                 ? snprintf(t, sizeof t, "not built yet (a body of %zu bytes)", n)
                 : snprintf(t, sizeof t, "not built yet (move of volume %" PRIu32 " from node %" PRIu32 " to node %"
                            PRIu32 "%s%s)", v, from, to, flags & CLUSTER_MOVE_LOG ? ", log" : "",
                            flags & CLUSTER_MOVE_CANCEL ? ", cancel" : "");
    cl_admin_reply(conn, reqid, META_ERR_INVALID, t, (size_t)tl);
}

void cmove_duty(uint64_t now) { (void)now; }
