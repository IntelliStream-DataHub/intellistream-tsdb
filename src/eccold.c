/* SPDX-License-Identifier: AGPL-3.0-only */
/* STUB (r8a1-mk) of eccold.h: r8g-cut fills it. Every call leaves main's behaviour as it is. */
#include "internal.h"
#include "eccold.h"

bool eccold_may_attach(struct eng *db)
{
    (void)db;
    return false; /* R7's refusal stands until r8g */
}

int eccold_open(struct eng *db)
{
    (void)db;
    return 0;
}

void eccold_close(struct eng *db)
{
    (void)db;
}
