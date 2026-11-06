/* The client side of the outagefs shim's control socket (tests/jepsen/outagefs.c): one command per connection. */
#ifndef TSDB_TEST_OUTAGEFS_CTL_H
#define TSDB_TEST_OUTAGEFS_CTL_H
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/* Sends "CMD PREFIX [ARG]" to the shim listening on `sock` and copies its answer (every line) into out. Returns 0
 * when the last line is "ok", -1 otherwise (out says why). */
static inline int outagefs_cmd(const char *sock, const char *line, char *out, size_t n)
{
    struct sockaddr_un sa = {.sun_family = AF_UNIX};
    if (strlen(sock) >= sizeof sa.sun_path)
        return -1;
    strcpy(sa.sun_path, sock);
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    size_t got = 0;
    if (out && n)
        out[0] = 0;
    if (fd < 0 || connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        if (out && n)
            snprintf(out, n, "err connect");
        if (fd >= 0)
            close(fd);
        return -1;
    }
    char cmd[600];
    if (strlen(line) > sizeof cmd - 2)
        return -1;
    int len = snprintf(cmd, sizeof cmd, "%s\n", line);
    if (write(fd, cmd, (size_t)len) != len) {
        close(fd);
        return -1;
    }
    char buf[4096];
    ssize_t r;
    while ((r = read(fd, buf + got, sizeof buf - 1 - got)) > 0)
        got += (size_t)r;
    buf[got] = 0;
    close(fd);
    if (out && n) {
        size_t c = got < n - 1 ? got : n - 1;
        memcpy(out, buf, c);
        out[c] = 0;
    }
    const char *last = got ? buf + got - 1 : buf;
    while (last > buf && last[-1] != '\n')
        last--;
    return got && strncmp(last, "ok", 2) == 0 ? 0 : -1;
}

/* A test starts itself again under the shim: makes BASE/outagefs.PID/<name> for each name, sets OUTAGEFS_ROOTS,
 * OUTAGEFS_CTL, LD_PRELOAD (from OUTAGEFS_LIB) and OUTAGEFS_DIR_<name>, and execs argv. Returns only on failure.
 * Once under the shim (OUTAGEFS_CTL set) it returns 0 at once; outagefs_dir then gives each directory. */
static inline int outagefs_reexec(char **argv, const char *base, const char *const *names, int n)
{
    if (getenv("OUTAGEFS_CTL"))
        return 0;
    char top[200], roots[2048] = "", ctl[220], var[128], dir[260];
    snprintf(top, sizeof top, "%s/outagefs.%d", base, (int)getpid());
    mkdir(top, 0755);
    for (int i = 0; i < n; i++) {
        snprintf(dir, sizeof dir, "%s/%s", top, names[i]);
        mkdir(dir, 0755);
        snprintf(var, sizeof var, "OUTAGEFS_DIR_%s", names[i]);
        setenv(var, dir, 1);
        size_t l = strlen(roots);
        snprintf(roots + l, sizeof roots - l, "%s%s", l ? ":" : "", dir);
    }
    snprintf(ctl, sizeof ctl, "%s/ctl", top);
    setenv("OUTAGEFS_ROOTS", roots, 1);
    setenv("OUTAGEFS_CTL", ctl, 1);
    setenv("LD_PRELOAD", getenv("OUTAGEFS_LIB") ? getenv("OUTAGEFS_LIB") : "build/liboutagefs.so", 1);
    /* make asan: the sanitizer runtime is no longer first in the library list once the shim is preloaded */
    char asan[512];
    snprintf(asan, sizeof asan, "verify_asan_link_order=0%s%.400s", getenv("ASAN_OPTIONS") ? ":" : "",
             getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
    setenv("ASAN_OPTIONS", asan, 1);
    execv("/proc/self/exe", argv);
    perror("exec");
    return -1;
}
static inline const char *outagefs_dir(const char *name)
{
    char var[128];
    snprintf(var, sizeof var, "OUTAGEFS_DIR_%s", name);
    return getenv(var);
}
#endif
