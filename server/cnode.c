/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 operator admin handlers on the Raft leader (R9 plan D22; cluster_int.h; track r9-ops).
 *
 * Handles CLUSTER_OP_NODE_HOLD (15), CLUSTER_OP_NODE_OUT (16), CLUSTER_OP_NODE_DRAIN (17),
 * CLUSTER_OP_NODE_REPLACE (18), CLUSTER_OP_NODE_DESTROYED (19), and CLUSTER_OP_VOLUME_MOVE_POS (20).
 * Runs on the cluster thread only, after cluster.c verifies leadership (returning CLUSTER_ERR_NOTLEADER on a
 * follower so the CLI follows once).
 *
 * Stub until r9-ops fills each handler in: decodes and validates the request body length and replies "not built".
 */
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>

#include "cluster_int.h"

static void reply(uint64_t conn, uint32_t reqid, int32_t rc, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));
static void reply(uint64_t conn, uint32_t reqid, int32_t rc, const char *fmt, ...)
{
    char b[320];
    va_list ap;
    va_start(ap, fmt);
    int l = vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    cl_admin_reply(conn, reqid, rc, b, l < 0 ? 0 : (size_t)l < sizeof b ? (size_t)l : sizeof b - 1);
}

void cnode_hold_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0, for_s = 0, flags = 0;
    if (cluster_node_hold_decode(body, n, &node, &for_s, &flags) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node hold: a body of 12 bytes is expected (got %zu)", n);
        return;
    }
    if (flags & CLUSTER_HOLD_RELEASE)
        reply(conn, reqid, META_ERR_INVALID, "node hold release of node %" PRIu32 ": not built", node);
    else
        reply(conn, reqid, META_ERR_INVALID, "node hold of node %" PRIu32 " for %" PRIu32 " s: not built", node,
              for_s);
}

void cnode_out_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    if (cluster_node_op_decode(body, n, &node) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node out: a body of 4 bytes is expected (got %zu)", n);
        return;
    }
    reply(conn, reqid, META_ERR_INVALID, "node out of node %" PRIu32 ": not built", node);
}

void cnode_drain_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    if (cluster_node_op_decode(body, n, &node) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node drain: a body of 4 bytes is expected (got %zu)", n);
        return;
    }
    reply(conn, reqid, META_ERR_INVALID, "node drain of node %" PRIu32 ": not built", node);
}

void cnode_replace_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t old_node = 0, new_node = 0;
    if (cluster_node_replace_decode(body, n, &old_node, &new_node) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node replace: a body of 8 bytes is expected (got %zu)", n);
        return;
    }
    reply(conn, reqid, META_ERR_INVALID, "node replace of node %" PRIu32 " with node %" PRIu32 ": not built",
          old_node, new_node);
}

void cnode_destroyed_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t node = 0;
    if (cluster_node_op_decode(body, n, &node) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "node destroyed: a body of 4 bytes is expected (got %zu)", n);
        return;
    }
    reply(conn, reqid, META_ERR_INVALID, "node destroyed of node %" PRIu32 ": not built", node);
}

void cnode_move_pos_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t vol = 0, pos = 0, to = 0;
    if (cluster_volume_move_pos_decode(body, n, &vol, &pos, &to) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "volume move --pos: a body of 12 bytes is expected (got %zu)", n);
        return;
    }
    reply(conn, reqid, META_ERR_INVALID, "volume %" PRIu32 " move pos %" PRIu32 " to node %" PRIu32 ": not built",
          vol, pos, to);
}
