/* The outagefs shim (tests/jepsen/outagefs.c): hang, error, slow, full and rollback faults per prefix, applied to
 * this very process, which the test starts again under LD_PRELOAD. Usage: test_outagefs [scratch directory]. */
#define _GNU_SOURCE
#include "outagefs_ctl.h"

#include <dirent.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

static int failures;
#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                        \
        }                                                                      \
    } while (0)

static char ctl[256], dir_a[256], dir_b[256];

static int cmd(const char *verb, const char *dir, const char *arg)
{
    char line[600], out[256];
    snprintf(line, sizeof line, "%s %s%s%s", verb, dir, arg ? " " : "", arg ? arg : "");
    int rc = outagefs_cmd(ctl, line, out, sizeof out);
    if (rc)
        fprintf(stderr, "ctl '%s': %s\n", line, out);
    return rc;
}

static void sleep_ms(int ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

static double now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e3 + t.tv_nsec / 1e6;
}

static void put(const char *path, const char *text)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        CHECK(write(fd, text, strlen(text)) == (ssize_t)strlen(text));
        close(fd);
    }
}

extern ssize_t __pread_chk(int, void *, size_t, off_t, size_t);
extern ssize_t __read_chk(int, void *, size_t, size_t);

static void get(const char *path, char *buf, size_t n)
{
    int fd = open(path, O_RDONLY);
    ssize_t r = fd >= 0 ? read(fd, buf, n - 1) : -1;
    buf[r > 0 ? r : 0] = 0;
    if (fd >= 0)
        close(fd);
}

static atomic_int hung_done;
static int hung_fd;

static void *hung_writer(void *arg)
{
    (void)arg;
    ssize_t r = pwrite(hung_fd, "x", 1, 0);
    atomic_store(&hung_done, r == 1 ? 1 : -1);
    return NULL;
}

static void run(void)
{
    char p[512], buf[256];
    snprintf(p, sizeof p, "%s/f", dir_a);
    put(p, "hello");
    get(p, buf, sizeof buf);
    CHECK(!strcmp(buf, "hello"));

    /* eio: every call on A fails; B is not affected */
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0);
    CHECK(cmd("eio", dir_a, NULL) == 0);
    CHECK(pread(fd, buf, 5, 0) == -1 && errno == EIO);
    /* the fortified entry points glibc's headers pick when the buffer size is known (make asan builds with them) */
    CHECK(__pread_chk(fd, buf, 5, 0, sizeof buf) == -1 && errno == EIO);
    CHECK(__read_chk(fd, buf, 5, sizeof buf) == -1 && errno == EIO);
    CHECK(fsync(fd) == -1 && errno == EIO);
    CHECK(open(p, O_RDONLY) == -1 && errno == EIO);
    char pb[512];
    snprintf(pb, sizeof pb, "%s/f", dir_b);
    put(pb, "b");
    get(pb, buf, sizeof buf);
    CHECK(!strcmp(buf, "b"));
    CHECK(cmd("heal", dir_a, NULL) == 0);
    CHECK(pread(fd, buf, 5, 0) == 5);

    /* estale and etimedout */
    CHECK(cmd("estale", dir_a, NULL) == 0);
    CHECK(pread(fd, buf, 5, 0) == -1 && errno == ESTALE);
    CHECK(cmd("etimedout", dir_a, NULL) == 0);
    CHECK(pread(fd, buf, 5, 0) == -1 && errno == ETIMEDOUT);
    CHECK(cmd("heal", dir_a, NULL) == 0);

    /* enospc: growing calls fail, reads and syncs of what exists work */
    CHECK(cmd("enospc", dir_a, NULL) == 0);
    CHECK(pwrite(fd, "z", 1, 0) == -1 && errno == ENOSPC);
    CHECK(pread(fd, buf, 5, 0) == 5);
    CHECK(fdatasync(fd) == 0);
    char q[512];
    snprintf(q, sizeof q, "%s/new", dir_a);
    CHECK(open(q, O_WRONLY | O_CREAT, 0644) == -1 && errno == ENOSPC);
    CHECK(cmd("heal", dir_a, NULL) == 0);

    /* slow */
    CHECK(cmd("slow", dir_a, "200") == 0);
    double t0 = now_ms();
    CHECK(pread(fd, buf, 5, 0) == 5);
    CHECK(now_ms() - t0 >= 190);
    CHECK(cmd("heal", dir_a, NULL) == 0);
    t0 = now_ms();
    CHECK(pread(fd, buf, 5, 0) == 5);
    CHECK(now_ms() - t0 < 100);

    /* hang: the writer blocks, a sibling prefix goes on, heal lets it finish */
    hung_fd = fd;
    CHECK(cmd("hang", dir_a, NULL) == 0);
    pthread_t th;
    pthread_create(&th, NULL, hung_writer, NULL);
    sleep_ms(300);
    CHECK(atomic_load(&hung_done) == 0);
    t0 = now_ms();
    put(pb, "still going");
    CHECK(now_ms() - t0 < 100);
    char out[512];
    CHECK(outagefs_cmd(ctl, "state", out, sizeof out) == 0);
    CHECK(strstr(out, "hang") && strstr(out, "hung=1"));
    CHECK(cmd("heal", dir_a, NULL) == 0);
    pthread_join(th, NULL);
    CHECK(atomic_load(&hung_done) == 1);

    /* readdir fails visibly: end of directory with errno set, never a clean short listing */
    DIR *d = opendir(dir_a);
    CHECK(d != NULL);
    int dfd = open(dir_a, O_RDONLY | O_DIRECTORY);
    DIR *d2 = fdopendir(dup(dfd));
    CHECK(cmd("eio", dir_a, NULL) == 0);
    errno = 0;
    CHECK(readdir(d2) == NULL && errno == EIO);
    CHECK(cmd("heal", dir_a, NULL) == 0);
    rewinddir(d2);
    CHECK(readdir(d2) != NULL);
    closedir(d2);
    closedir(d);
    close(dfd);

    /* snap and rollback: same inode, older bytes; files created since are gone; files removed come back */
    snprintf(q, sizeof q, "%s/sub", dir_a);
    mkdir(q, 0755);
    snprintf(q, sizeof q, "%s/sub/g", dir_a);
    put(q, "g-old");
    put(p, "f-old-long-contents");
    CHECK(cmd("snap", dir_a, NULL) == 0);
    struct stat st0, st1;
    CHECK(stat(p, &st0) == 0);
    put(p, "f-new");
    char late[512];
    snprintf(late, sizeof late, "%s/late", dir_a);
    put(late, "late");
    CHECK(unlink(q) == 0);
    CHECK(cmd("rollback", dir_a, NULL) == 0);
    get(p, buf, sizeof buf);
    CHECK(!strcmp(buf, "f-old-long-contents"));
    CHECK(stat(p, &st1) == 0 && st0.st_ino == st1.st_ino);
    CHECK(access(late, F_OK) == -1);
    get(q, buf, sizeof buf);
    CHECK(!strcmp(buf, "g-old"));
    CHECK(cmd("rollback", "/nonexistent", NULL) != 0);
    close(fd);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    if (outagefs_reexec(argv, base, (const char *const[]){"a", "b"}, 2))
        return 1;
    snprintf(ctl, sizeof ctl, "%s", getenv("OUTAGEFS_CTL"));
    snprintf(dir_a, sizeof dir_a, "%s", outagefs_dir("a"));
    snprintf(dir_b, sizeof dir_b, "%s", outagefs_dir("b"));
    run();
    if (failures)
        fprintf(stderr, "test_outagefs: %d failures\n", failures);
    else
        printf("test_outagefs: ok\n");
    return failures ? 1 : 0;
}
