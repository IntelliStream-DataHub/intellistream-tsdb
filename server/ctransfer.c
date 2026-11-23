/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Planned leader transfer, the Raft leader's side (R4 plan D14, D15; cluster_int.h): NOT BUILT (r4-transfer). The
 * admin op answers "not built"; nothing is sent and READY is ignored.
 */
#include <string.h>

#include "cluster_int.h"

void ctr_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    (void)body, (void)n;
    static const char t[] = "not built yet";
    cl_admin_reply(conn, reqid, META_ERR_INVALID, t, strlen(t));
}

void ctr_duty(uint64_t now) { (void)now; }

void ctr_ready(uint32_t from, const struct cluster_vmsg *m) { (void)from, (void)m; }
