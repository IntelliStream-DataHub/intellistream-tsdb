// SPDX-License-Identifier: AGPL-3.0-only
// For the jepsen self-test: stands in for a tsdbd whose settle hook (TSDB_SETTLE_CRASH_AT) has been reached. Preloaded
// after crashfs into a real tsdbd; a second or two after start it ends the process with exit(99) (FAKE_EXIT=exit: the
// power cut of crashfs's destructor follows) or with _exit(99) (FAKE_EXIT=_exit: no destructor, no power cut). The
// settle-steps nemesis must accept the first and fail the second.
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void *ender(void *a)
{
    (void)a;
    usleep(1500000 + (unsigned)(getpid() % 3) * 500000);
    if (getenv("FAKE_EXIT") && !strcmp(getenv("FAKE_EXIT"), "_exit"))
        _exit(99);
    exit(99);
}

__attribute__((constructor)) static void init(void)
{
    if (getenv("TSDB_SETTLE_CRASH_AT") && getenv("FAKE_EXIT")) {
        pthread_t th;
        pthread_create(&th, NULL, ender, NULL);
    }
}
