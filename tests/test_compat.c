/* The portability shims of src/compat.h that need a test of their own.
 *
 * tsdb_fsync and tsdb_fdatasync: on macOS a plain fsync does not flush the drive's cache, so a power loss can lose
 * what was acknowledged. They must issue fcntl(F_FULLFSYNC) after the real call (this test's own fcntl, which sees
 * every call from the code compiled into it), give it up only with TSDB_FAST_FSYNC, accept a file system that has no
 * F_FULLFSYNC, and report every other failure. On Linux they are the plain calls and the test checks just that. */
#include "../src/compat.h"
#include "portable.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

#ifdef __APPLE__
static int fullsyncs, full_errno; /* calls seen, and the errno the next one fails with (0: it succeeds) */

int fcntl(int fd, int cmd, ...)
{
    va_list ap;
    va_start(ap, cmd);
    long arg = va_arg(ap, long);
    va_end(ap);
    if (cmd == F_FULLFSYNC) {
        fullsyncs++;
        if (full_errno) {
            errno = full_errno;
            return -1;
        }
    }
    return REAL(fcntl)(fd, cmd, arg);
}
#endif

int main(int argc, char **argv)
{
    char path[512];
    snprintf(path, sizeof path, "%s/tsdb-compat-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0 && write(fd, "x", 1) == 1);
    int dfd = open(argc > 1 ? argv[1] : "/tmp", O_RDONLY);
    CHECK(dfd >= 0);

    CHECK(tsdb_fsync(fd) == 0 && tsdb_fdatasync(fd) == 0);
    CHECK(tsdb_fsync(dfd) == 0); /* a directory, as after a rename */
    errno = 0;
    CHECK(tsdb_fsync(-1) == -1 && errno == EBADF);
    CHECK(tsdb_fdatasync(-1) == -1 && errno == EBADF);
#ifdef __APPLE__
    const char *fast = getenv("TSDB_FAST_FSYNC"); /* the Makefile runs this test both ways */
    if (fast && *fast && *fast != '0') {
        CHECK(tsdb_fast_fsync() == 1 && fullsyncs == 0); /* given up on purpose: the real calls only */
    } else {
        CHECK(tsdb_fast_fsync() == 0);
        CHECK(fullsyncs == 3); /* the two on the file and the one on the directory; failed ones never get that far */
        int before = fullsyncs;
        full_errno = ENOTSUP; /* a file system without it: the fsync before was all there was */
        CHECK(tsdb_fsync(fd) == 0 && fullsyncs == before + 1);
        full_errno = EIO; /* the drive could not be flushed: a failed sync, and the caller fails stop */
        errno = 0;
        CHECK(tsdb_fsync(fd) == -1 && errno == EIO);
        errno = 0;
        CHECK(tsdb_fdatasync(fd) == -1 && errno == EIO);
        full_errno = 0;
    }
#endif
    close(fd);
    close(dfd);
    unlink(path);
    if (failures) {
        fprintf(stderr, "test_compat: %d failures\n", failures);
        return 1;
    }
    printf("test_compat: ok\n");
    return 0;
}
