/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef TSDB_TEST_WATCHDOG_H
#define TSDB_TEST_WATCHDOG_H
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* A watchdog for a whole test process: when `seconds` have passed (times TH_WATCHDOG_SCALE from the environment,
 * and 10 under a sanitizer, which runs the same test several times slower) the process prints which test hung and
 * exits with status 124. A test that waits for a server forever then fails by name instead of holding a job (or a
 * CI runner) until something else gives up; the servers it started follow it out (--exit-with-parent). It is a
 * thread, not alarm(): a test may use SIGALRM for itself (test_stability does). The deadline is for the whole run,
 * far above the longest honest one, so it never fires on a slow box that is merely busy. */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define TH_SAN_SCALE 10
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define TH_SAN_SCALE 10
#endif
#endif
#ifndef TH_SAN_SCALE
#define TH_SAN_SCALE 1
#endif
static char th_watch_name[64];
static long th_watch_seconds;

static void *th_watch_main(void *arg)
{
    (void)arg;
    struct timespec ts = {.tv_sec = th_watch_seconds};
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
        ;
    char msg[160];
    int n = snprintf(msg, sizeof msg, "\nFAIL %s: watchdog: still running after %ld s, it is hung (exit 124)\n",
                     th_watch_name, th_watch_seconds);
    if (n > 0 && write(2, msg, (size_t)n) < 0)
        _exit(124);
    _exit(124);
}

static inline void th_watchdog(const char *name, long seconds)
{
    const char *sc = getenv("TH_WATCHDOG_SCALE");
    long scale = sc && atol(sc) > 0 ? atol(sc) : 1;
    snprintf(th_watch_name, sizeof th_watch_name, "%s", name);
    th_watch_seconds = seconds * scale * TH_SAN_SCALE;
    pthread_t t;
    if (pthread_create(&t, NULL, th_watch_main, NULL) != 0) {
        fprintf(stderr, "%s: cannot start the watchdog thread\n", name);
        exit(1);
    }
    pthread_detach(t);
}

#endif
