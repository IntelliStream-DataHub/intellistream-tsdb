/* SPDX-License-Identifier: AGPL-3.0-only */
/* A snapshot's install on disk (snapinst.h): NOT BUILT (r4-xferinst). No install ever starts, so none is left over. */
#include "snapinst.h"

int snapinst_leftovers(const char *root, uint32_t number)
{
    (void)root, (void)number;
    return 0;
}
