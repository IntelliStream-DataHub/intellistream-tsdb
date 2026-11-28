/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Redundancy conversion driver on the Raft leader (R10 r10-driver; docs/design-cluster-data.md §6.4, E2, E38;
 * PLAN.md D26-D29). Runs on the cluster thread only, like cmove.c and cnode.c.
 */
#ifndef TSDB_CREDUND_H
#define TSDB_CREDUND_H

#include <stddef.h>
#include <stdint.h>

void credund_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n);
void credund_duty(uint64_t now);

#endif
