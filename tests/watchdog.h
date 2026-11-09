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
 * POSIX timer on Linux (a thread elsewhere), not alarm(): a test may use SIGALRM for itself (test_stability does). The deadline is for the whole run,
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
static char th_watch_msg[160];
static size_t th_watch_msg_len;

static void th_watch_fire(void)
{
    if (write(2, th_watch_msg, th_watch_msg_len) < 0)
        _exit(124);
    _exit(124);
}

#ifdef __linux__
/* A POSIX timer that sends a signal, not a thread: a process with a second thread cannot fork and then start threads
 * in the child under ThreadSanitizer ("starting new threads after multi-threaded fork is not supported. Dying"), and
 * many tests fork a child that opens a database. The timer is not inherited by a child, as the thread was not. The
 * signal is SIGRTMIN (no test uses it; SIGALRM is left to test_stability), the handler only writes and exits. */
#include <signal.h>
static void th_watch_sig(int sig)
{
    (void)sig;
    th_watch_fire();
}

static inline void th_watch_start(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = th_watch_sig;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    timer_t t;
    struct sigevent ev;
    memset(&ev, 0, sizeof ev);
    ev.sigev_notify = SIGEV_SIGNAL;
    ev.sigev_signo = SIGRTMIN;
    struct itimerspec its;
    memset(&its, 0, sizeof its);
    its.it_value.tv_sec = th_watch_seconds;
    if (sigaction(SIGRTMIN, &sa, NULL) != 0 || timer_create(CLOCK_MONOTONIC, &ev, &t) != 0 ||
        timer_settime(t, 0, &its, NULL) != 0) {
        fprintf(stderr, "%s: cannot start the watchdog timer\n", th_watch_name);
        exit(1);
    }
}
#else
/* No timer_create (macOS): a thread. */
static void *th_watch_main(void *arg)
{
    (void)arg;
    struct timespec ts = {.tv_sec = th_watch_seconds};
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
        ;
    th_watch_fire();
    return NULL;
}

static inline void th_watch_start(void)
{
    pthread_t t;
    if (pthread_create(&t, NULL, th_watch_main, NULL) != 0) {
        fprintf(stderr, "%s: cannot start the watchdog thread\n", th_watch_name);
        exit(1);
    }
    pthread_detach(t);
}
#endif

static inline void th_watchdog(const char *name, long seconds)
{
    const char *sc = getenv("TH_WATCHDOG_SCALE");
    long scale = sc && atol(sc) > 0 ? atol(sc) : 1;
    snprintf(th_watch_name, sizeof th_watch_name, "%s", name);
    th_watch_seconds = seconds * scale * TH_SAN_SCALE;
    int n = snprintf(th_watch_msg, sizeof th_watch_msg,
                     "\nFAIL %s: watchdog: still running after %ld s, it is hung (exit 124)\n", th_watch_name,
                     th_watch_seconds);
    th_watch_msg_len = n > 0 && (size_t)n < sizeof th_watch_msg ? (size_t)n : sizeof th_watch_msg - 1;
    th_watch_start();
}

#endif
