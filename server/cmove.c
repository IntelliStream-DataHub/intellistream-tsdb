/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica moves, the Raft leader's side (R4 plan D10, D11; cluster_int.h): NOT BUILT (r4-move). The admin op answers
 * "not built".
 */
#include <string.h>

#include "cluster_int.h"

void cmove_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    (void)body, (void)n;
    static const char t[] = "not built yet";
    cl_admin_reply(conn, reqid, META_ERR_INVALID, t, strlen(t));
}

void cmove_duty(uint64_t now) { (void)now; }
