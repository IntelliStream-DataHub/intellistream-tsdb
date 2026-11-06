/* The client side of the outagefs shim's control socket (tests/jepsen/outagefs.c): one command per connection. */
#ifndef TSDB_TEST_OUTAGEFS_CTL_H
#define TSDB_TEST_OUTAGEFS_CTL_H
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/* Sends "CMD PREFIX [ARG]" to the shim listening on `sock` and copies its answer (every line) into out. Returns 0
 * when the last line is "ok", -1 otherwise (out says why). */
static inline int outagefs_cmd(const char *sock, const char *line, char *out, size_t n)
{
    struct sockaddr_un sa = {.sun_family = AF_UNIX};
    snprintf(sa.sun_path, sizeof sa.sun_path, "%s", sock);
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
    if (out && n)
        snprintf(out, n, "%s", buf);
    const char *last = got ? buf + got - 1 : buf;
    while (last > buf && last[-1] != '\n')
        last--;
    return got && strncmp(last, "ok", 2) == 0 ? 0 : -1;
}
#endif
