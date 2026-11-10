/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef TSDB_TEST_WATCHDOG_H
#define TSDB_TEST_WATCHDOG_H
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* A watchdog for a whole test process: when `seconds` have passed (times TH_WATCHDOG_SCALE from the environment,
 * and 10 under a sanitizer, which runs the same test several times slower) the process prints which test hung and
 * exits with status 124. A test that waits for a server forever then fails by name instead of holding a job (or a
 * CI runner) until something else gives up; the servers it started follow it out (--exit-with-parent). On a machine
 * whose CPUs have been saturated for minutes (a gate niced beside other gates: test_group under TSan, 3 minutes alone,
 * met its 12000 s) the deadline is renewed up to twice, with a line that says so. It is a
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
static char th_watch_name[96];
static long th_watch_seconds;
static char th_watch_msg[224];
static size_t th_watch_msg_len;

/* A test that wants to say more about where it hangs (jepsen: which phase, the threads of the processes it started)
 * sets this; it runs in the signal handler, after the message and before the exit, so it may use only what a signal
 * handler may (fork, exec, waitpid, write). */
static void (*th_watch_hook)(void);

static void th_watch_fire(void)
{
    if (write(2, th_watch_msg, th_watch_msg_len) < 0)
        _exit(124);
    if (th_watch_hook)
        th_watch_hook();
    _exit(124);
}

#ifdef __linux__
/* A POSIX timer that sends a signal, not a thread: a process with a second thread cannot fork and then start threads
 * in the child under ThreadSanitizer ("starting new threads after multi-threaded fork is not supported. Dying"), and
 * many tests fork a child that opens a database. The timer is not inherited by a child, as the thread was not. The
 * signal is SIGRTMIN (no test uses it; SIGALRM is left to test_stability), the handler only writes and exits. */
#include <signal.h>
static timer_t th_watch_timer;
static int th_watch_ext;
static char th_watch_psi[256];

/* The CPU pressure of the whole machine ("some" avg300 of /proc/pressure/cpu, whole percent; 0 where there is none):
 * a test niced behind a saturated machine (a gate beside other gates) runs hundreds of times slower than alone, which
 * is not a hang. Only open, read and close in here: it runs in a signal handler. */
static int th_cpu_pressure(void)
{
    char b[256];
    int fd = open(th_watch_psi, O_RDONLY);
    if (fd < 0)
        return 0;
    ssize_t n = read(fd, b, sizeof b - 1);
    close(fd);
    if (n <= 0)
        return 0;
    b[n] = 0;
    const char *p = strstr(b, "avg300=");
    int pct = 0;
    for (p = p ? p + 7 : ""; *p >= '0' && *p <= '9'; p++)
        pct = pct * 10 + (*p - '0');
    return pct;
}

/* A whole number as text into out (no stdio: the signal handler uses it); returns the length. */
static size_t th_watch_itoa(int v, char *out)
{
    char t[16];
    size_t n = 0, k = 0;
    if (v < 0)
        v = 0;
    do {
        t[n++] = (char)('0' + v % 10);
        v /= 10;
    } while (v && n < sizeof t);
    while (n)
        out[k++] = t[--n];
    return k;
}

#define TH_WATCH_BUSY_PCT 30 /* the machine has been this busy for five minutes: a slow test is not yet a hung one */
#define TH_WATCH_EXTENSIONS 2

static void th_watch_sig(int sig)
{
    (void)sig;
    int pct = th_watch_ext < TH_WATCH_EXTENSIONS ? th_cpu_pressure() : 0;
    if (th_watch_ext < TH_WATCH_EXTENSIONS && pct >= TH_WATCH_BUSY_PCT) {
        struct itimerspec its;
        memset(&its, 0, sizeof its);
        its.it_value.tv_sec = th_watch_seconds;
        if (timer_settime(th_watch_timer, 0, &its, NULL) == 0) {
            th_watch_ext++;
            /* "\nwatchdog: NAME: the machine is busy (CPU pressure avg300 P%), the test gets more time (renewal K of N)\n" */
            char note[320];
            size_t k = 0, l;
            static const char a[] = "\nwatchdog: ", b[] = ": the machine is busy (CPU pressure avg300 ",
                              c[] = "%), the test gets more time (renewal ", d[] = " of ", e[] = ")\n";
            memcpy(note, a, sizeof a - 1), k += sizeof a - 1;
            l = strnlen(th_watch_name, 96), memcpy(note + k, th_watch_name, l), k += l;
            memcpy(note + k, b, sizeof b - 1), k += sizeof b - 1;
            k += th_watch_itoa(pct, note + k);
            memcpy(note + k, c, sizeof c - 1), k += sizeof c - 1;
            k += th_watch_itoa(th_watch_ext, note + k);
            memcpy(note + k, d, sizeof d - 1), k += sizeof d - 1;
            k += th_watch_itoa(TH_WATCH_EXTENSIONS, note + k);
            memcpy(note + k, e, sizeof e - 1), k += sizeof e - 1;
            if (write(2, note, k) < 0)
                th_watch_ext = TH_WATCH_EXTENSIONS;
            return;
        }
    }
    th_watch_fire();
}

static inline void th_watch_start(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = th_watch_sig;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    struct sigevent ev;
    memset(&ev, 0, sizeof ev);
    ev.sigev_notify = SIGEV_SIGNAL;
    ev.sigev_signo = SIGRTMIN;
    struct itimerspec its;
    memset(&its, 0, sizeof its);
    its.it_value.tv_sec = th_watch_seconds;
    const char *psi = getenv("TH_WATCHDOG_PSI"); /* test_watchdog points it at a file of its own */
    snprintf(th_watch_psi, sizeof th_watch_psi, "%s", psi ? psi : "/proc/pressure/cpu");
    th_watch_ext = 0;
    if (sigaction(SIGRTMIN, &sa, NULL) != 0 || timer_create(CLOCK_MONOTONIC, &ev, &th_watch_timer) != 0 ||
        timer_settime(th_watch_timer, 0, &its, NULL) != 0) {
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
