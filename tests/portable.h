/* portable.h: what the tests use of Linux, for macOS (see src/compat.h for the library's own).
 *
 *  - REAL(name): the libc function a test has replaced with its own (a fault injector linked statically with the
 *    engine, whose definition wins over libc's). On Linux the tests call syscall(); macOS has no stable syscall
 *    numbers, so the next definition is looked up with dlsym(RTLD_NEXT).
 *  - pthread_barrier_t, which macOS lacks.
 *  - fd_path: the path an open descriptor names (/proc/self/fd/N on Linux, F_GETPATH on macOS).
 */
#ifndef TSDB_TESTS_PORTABLE_H
#define TSDB_TESTS_PORTABLE_H

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stddef.h>
#include <sys/types.h>
#include <unistd.h>

#include "../src/compat.h"

/* A shell command prefix that limits a command to s seconds (GNU timeout is not on macOS; perl's alarm survives
   exec) */
#ifdef __APPLE__
#define TIMEOUT_KILL_CMD(s) "perl -e 'alarm " #s "; exec @ARGV' --"
#define TIMEOUT_TERM_CMD(s) "perl -e 'alarm " #s "; exec @ARGV' --"
#else
#define TIMEOUT_KILL_CMD(s) "timeout -s KILL " #s
#define TIMEOUT_TERM_CMD(s) "timeout " #s
#endif

/* macOS starts a process with a soft limit of 256 open files, and a volume takes several: raise it to what the
 * kernel allows (a no-op on Linux, where the usual limit is 1024 or more) */
#include <sys/resource.h>
static inline void tp_raise_nofile(void)
{
#ifdef __APPLE__
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max < 10240 ? rl.rlim_max : 10240;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
#endif
}

/* ru_maxrss: kilobytes on Linux, bytes on macOS */
#ifdef __APPLE__
#define RU_MAXRSS_KB(u) ((u).ru_maxrss / 1024)
#else
#define RU_MAXRSS_KB(u) ((u).ru_maxrss)
#endif

#ifdef __APPLE__

#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>

#define REAL(name)                                                                                                 \
    ({                                                                                                             \
        static __typeof__(&name) real_;                                                                            \
        if (!real_)                                                                                                \
            real_ = (__typeof__(&name))dlsym(RTLD_NEXT, #name);                                                    \
        real_;                                                                                                     \
    })

#define PTHREAD_BARRIER_SERIAL_THREAD (-1)
typedef struct {
    pthread_mutex_t m;
    pthread_cond_t c;
    unsigned need, have, gen;
} pthread_barrier_t;

static inline int pthread_barrier_init(pthread_barrier_t *b, const void *attr, unsigned n)
{
    (void)attr;
    pthread_mutex_init(&b->m, NULL);
    pthread_cond_init(&b->c, NULL);
    b->need = n, b->have = 0, b->gen = 0;
    return 0;
}

static inline int pthread_barrier_wait(pthread_barrier_t *b)
{
    pthread_mutex_lock(&b->m);
    unsigned gen = b->gen;
    int rc = 0;
    if (++b->have == b->need) {
        b->have = 0;
        b->gen++;
        pthread_cond_broadcast(&b->c);
        rc = PTHREAD_BARRIER_SERIAL_THREAD;
    } else {
        while (gen == b->gen)
            pthread_cond_wait(&b->c, &b->m);
    }
    pthread_mutex_unlock(&b->m);
    return rc;
}

static inline int pthread_barrier_destroy(pthread_barrier_t *b)
{
    pthread_cond_destroy(&b->c);
    pthread_mutex_destroy(&b->m);
    return 0;
}

/* the path of fd into out (NUL-terminated): its length, or -1 */
static inline ssize_t fd_path(int fd, char *out, size_t cap)
{
    char p[PATH_MAX];
    if (fcntl(fd, F_GETPATH, p) != 0)
        return -1;
    size_t n = strlen(p);
    if (n >= cap)
        return -1;
    memcpy(out, p, n + 1);
    return (ssize_t)n;
}

/* the running executable's path into out: its length, or -1 (Linux tests read /proc/self/exe) */
#include <libproc.h>
#include <mach/mach_time.h>
#include <mach-o/dyld.h>
#include <stdlib.h>
#include <sys/resource.h>

static inline ssize_t tp_self_exe(char *out, size_t cap)
{
    char p[PATH_MAX], real[PATH_MAX];
    uint32_t n = sizeof p;
    if (_NSGetExecutablePath(p, &n) != 0 || !realpath(p, real) || strlen(real) >= cap)
        return -1;
    strcpy(out, real);
    return (ssize_t)strlen(real);
}

/* descriptors a process of ours holds, -1 if it cannot be listed */
static inline int tp_fd_count(pid_t pid)
{
    int cap = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, NULL, 0); /* only an estimate, with slack: ask for the list */
    if (cap <= 0)
        return -1;
    cap += 64 * (int)sizeof(struct proc_fdinfo); /* descriptors opened since */
    struct proc_fdinfo *fds = malloc((size_t)cap);
    if (!fds)
        return -1;
    int bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds, cap);
    free(fds);
    return bytes < 0 ? -1 : bytes / (int)sizeof(struct proc_fdinfo);
}

/* CPU seconds (user + system) a process of ours has used, -1 if unknown */
static inline double tp_cpu_s(pid_t pid)
{
    struct rusage_info_v4 ri;
    if (proc_pid_rusage(pid, RUSAGE_INFO_V4, (rusage_info_t *)&ri) != 0)
        return -1;
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    return (double)(ri.ri_user_time + ri.ri_system_time) * tb.numer / tb.denom * 1e-9;
}

/* memory a process of ours holds that is not a mapped file: its physical footprint, and the highest it has been
 * (the counterparts of RssAnon and VmHWM), in KiB; -1 if unknown */
static inline long tp_footprint_kb(pid_t pid, bool peak)
{
    struct rusage_info_v4 ri;
    if (proc_pid_rusage(pid, RUSAGE_INFO_V4, (rusage_info_t *)&ri) != 0)
        return -1;
    return (long)((peak ? ri.ri_lifetime_max_phys_footprint : ri.ri_phys_footprint) / 1024);
}

#else /* Linux */

static inline ssize_t tp_self_exe(char *out, size_t cap)
{
    ssize_t n = readlink("/proc/self/exe", out, cap - 1);
    if (n >= 0)
        out[n] = '\0';
    return n;
}

static inline ssize_t fd_path(int fd, char *out, size_t cap)
{
    char link[64];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t l = readlink(link, out, cap - 1);
    if (l >= 0)
        out[l] = '\0';
    return l;
}

#endif

#endif
