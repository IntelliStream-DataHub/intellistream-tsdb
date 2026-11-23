/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica reset, the Raft leader's side (R4 plan D13; cluster_int.h): NOT BUILT (r4-setchg). The admin op answers
 * "not built".
 */
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "cluster_int.h"

void cset_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t v, node;
    char t[128];
    int tl = cluster_volume_force_decode(body, n, &v, &node) != 0
                 ? snprintf(t, sizeof t, "not built yet (a body of %zu bytes)", n)
                 : snprintf(t, sizeof t, "not built yet (reset of volume %" PRIu32 " on node %" PRIu32 ")", v, node);
    cl_admin_reply(conn, reqid, META_ERR_INVALID, t, (size_t)tl);
}
