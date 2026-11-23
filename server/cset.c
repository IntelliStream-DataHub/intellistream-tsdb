/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica reset, the Raft leader's side (R4 plan D13; cluster_int.h): NOT BUILT (r4-setchg). The admin op answers
 * "not built".
 */
#include <string.h>

#include "cluster_int.h"

void cset_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    (void)body, (void)n;
    static const char t[] = "not built yet";
    cl_admin_reply(conn, reqid, META_ERR_INVALID, t, strlen(t));
}
