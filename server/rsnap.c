/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshot transfer (rsnap.h): NOT BUILT. r4-xferlink builds the request, the leader's source and the transfer on the
 * kind-5 link; r4-xferinst the receiver's install and reopen. Until then a replica that needs a snapshot says so once
 * per open and stays a learner, and anything that arrives on a kind-5 link is refused.
 */
#include <inttypes.h>
#include <stdatomic.h>

#include "rsnap.h"

void rsnap_need(struct rvol *rv, int src)
{
    (void)src;
    if (!rv->snap_said) /* the core repeats the notice until the replica catches up: said once per open */
        say("volume %" PRIu32 ": the leader no longer has the records this replica needs: it needs a snapshot (not"
            " built yet); it stays a learner", rv->number);
    rv->snap_said = 1;
}

void rsnap_in(struct rvol *rv, uint32_t node, const uint8_t *p, size_t n) { (void)rv, (void)node, (void)p, (void)n; }

void rsnap_tick(struct rvol *rv) { (void)rv; }

void rsnap_stop(struct rvol *rv) { (void)rv; }

static atomic_int said;

static void not_built(void)
{
    if (!atomic_exchange(&said, 1))
        say("a snapshot link (kind 5) sent something: snapshot transfer is not built yet; refused");
}

void rsnap_frame(struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    (void)l, (void)type, (void)p, (void)n;
    not_built();
}

int rsnap_bin_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                   uint64_t *resume, int *prefix_fd)
{
    (void)arg, (void)l, (void)file, (void)meta, (void)meta_len, (void)total, (void)resume, (void)prefix_fd;
    not_built();
    return -1;
}

int rsnap_bin_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    (void)arg, (void)l, (void)file, (void)off, (void)p, (void)n;
    return -1;
}

void rsnap_bin_end(void *arg, struct plink *l, uint64_t file, int status) { (void)arg, (void)l, (void)file, (void)status; }
