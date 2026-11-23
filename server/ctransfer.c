/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Planned leader transfer, the Raft leader's side (R4 plan D14, D15; cluster_int.h): NOT BUILT (r4-transfer). The
 * admin op answers "not built"; nothing is sent and READY is ignored.
 */
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "cluster_int.h"

void ctr_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t v, node;
    char t[128];
    int tl = cluster_volume_force_decode(body, n, &v, &node) != 0
                 ? snprintf(t, sizeof t, "not built yet (a body of %zu bytes)", n)
                 : snprintf(t, sizeof t, "not built yet (transfer of volume %" PRIu32 " to node %" PRIu32 ")", v, node);
    cl_admin_reply(conn, reqid, META_ERR_INVALID, t, (size_t)tl);
}

void ctr_duty(uint64_t now) { (void)now; }

void ctr_ready(uint32_t from, const struct cluster_vmsg *m) { (void)from, (void)m; }
