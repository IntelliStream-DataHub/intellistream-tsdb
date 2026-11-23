/* SPDX-License-Identifier: AGPL-3.0-only */
/* crashfs (tests/jepsen/crashfs.c) rolls files back at a power loss. A thread that reads a mapping of one of them while it is cut
 * dies of SIGBUS before the thread that rolls back reaches its _exit; that is the power going out under it, and the shim must end
 * the process with its own status 99, as a power loss does, not with the signal (a worker of tsdbd in part_open_as did that and an
 * honest jepsen run failed "ended with status 0x87, not a power loss"). The program runs under LD_PRELOAD=libcrashfs.so with
 * CRASHFS_ROOT set to its directory and prints nothing; the Makefile (jepsen-selftest) wants exit status 99.
 * Usage: test_crashfs_bus DIR */
#define _GNU_SOURCE
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define MAP_BYTES (1u << 20)

static atomic_int go;

static void *reader(void *p)
{
    volatile const unsigned char *m = p;
    unsigned sum = 0;
    atomic_store(&go, 1);
    for (;;)
        for (unsigned i = 0; i < MAP_BYTES; i += 4096)
            sum += m[i]; /* faults once the file is cut under the mapping */
    return (void *)(size_t)sum;
}

static void make_file(const char *dir, int i, size_t bytes)
{
    char path[4096];
    snprintf(path, sizeof path, "%s/f%04d", dir, i);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        _exit(2);
    char buf[512];
    memset(buf, 'x', sizeof buf);
    for (size_t off = 0; off < bytes; off += sizeof buf)
        if (write(fd, buf, sizeof buf) < 0)
            _exit(2);
    close(fd); /* never synced: the rollback cuts it to nothing */
}

int main(int argc, char **argv)
{
    if (argc < 2)
        return 2;
    const char *dir = argv[1];
    for (int i = 0; i < 300; i++) /* the rollback has work to do before and after the mapped file */
        make_file(dir, i, 512);
    char path[4096];
    snprintf(path, sizeof path, "%s/mapped", dir);
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0 || ftruncate(fd, MAP_BYTES) != 0)
        return 2;
    unsigned char *m = mmap(NULL, MAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (m == MAP_FAILED)
        return 2;
    memset(m, 1, MAP_BYTES);
    pthread_t th;
    if (pthread_create(&th, NULL, reader, m) != 0)
        return 2;
    while (!atomic_load(&go))
        usleep(1000);
    usleep(20000); /* the reader is spinning */
    for (int i = 300; i < 2300; i++)
        make_file(dir, i, 512);
    raise(SIGUSR2); /* the plug: the power is lost at the next tracked operation */
    for (int i = 2300; i < 2400; i++)
        make_file(dir, i, 512);
    return 3; /* the loss did not happen */
}
