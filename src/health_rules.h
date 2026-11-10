/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The flapping rule of the storage health monitor (docs/ha.md 15.2 "Hysteresis"): a domain that went DOWN
 * HEALTH_FLAP_DOWNS times within HEALTH_FLAP_WINDOW_MS needs HEALTH_FLAP_NEED_MS of good write probes to leave DOWN,
 * not recover_ms. One place for the numbers: src/health.c applies them, and tests/jepsen/jepsen.c derives the bound
 * it holds a healed volume to from the same three, so a change here moves the check with it
 * (tests/test_health.c, "flapping", checks the rule itself).
 */
#ifndef TSDB_HEALTH_RULES_H
#define TSDB_HEALTH_RULES_H

#define HEALTH_FLAP_DOWNS 3
#define HEALTH_FLAP_WINDOW_MS 600000
#define HEALTH_FLAP_NEED_MS 60000

#endif
