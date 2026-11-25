/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * crashfs: an LD_PRELOAD shim that simulates losing power, in the spirit of
 * Jepsen's LazyFS and of ALICE.
 *
 * For files under $CRASHFS_ROOT it keeps what a disk would really hold:
 *  - file data is durable up to the size it had at its last fsync/fdatasync;
 *    regions rewritten in place since then are dirty;
 *  - creating (a file, a directory or a hard link: link and linkat), renaming and removing a directory entry is
 *    durable only after an fsync of that directory. A call that fails changes nothing the shim keeps (round 16).
 * On a "power loss" it truncates every file to its durable size, gives the
 * dirty regions their old bytes back (a real disk keeps old or new pages of a
 * sector it was rewriting, never blank ones; CRASHFS_REWRITE=zero zeroes them,
 * harsher than any disk), undoes every directory change that was not made
 * durable, newest first, and exits at once.
 *
 * A power loss happens with probability $CRASHFS_PROB at each tracked
 * operation (after $CRASHFS_WARMUP of them), or at the next one after
 * SIGUSR2. Every tracked operation runs under one lock, so no thread can
 * write while the state is being rolled back. writev and pwritev are tracked
 * too (gathered into one write or pwrite): the Raft log appends with
 * pwritev, and one that ran past the lock while the power loss cut its file
 * left a hole of zeros under a later record that the process then read back.
 *
 * With CRASHFS_MODE=eio the fault is an I/O error instead, the way Linux
 * reports a failed writeback ("fsyncgate", PostgreSQL 2018): at the point
 * where a power loss would happen, the next write, pwrite, fsync or fdatasync
 * of a tracked file fails with EIO (a write after storing half of its bytes).
 * From then on that file or directory lies, in the worst way Linux allows:
 * its syncs return 0 but make nothing durable, because the kernel dropped the
 * pages and cleared the error. When the process exits, everything not made
 * durable is rolled back as in a power loss, so a write acknowledged after
 * the error, on the strength of a sync that lied, is lost and a checker sees
 * it. One fault per process; SIGUSR2 after it is a power loss at the next
 * tracked operation, for a process that carries on. With $CRASHFS_EIO_ONLY, only files whose name
 * starts with it fail (e.g. "wal_"); with $CRASHFS_EIO_ON, only that call (write, pwrite, fsync, fdatasync
 * or syncfs). If $CRASHFS_MARK is set, the path of the
 * file that failed is written there, so a harness knows the fault happened.
 *
 * $CRASHFS_PULL_FILE: a SIGUSR2 that reaches the shim appends "pid N power loss requested by SIGUSR2" there as it is
 * delivered (the process loses power at its next file operation, or by its own odds just before: exit 99), so a harness counts the
 * plugs it pulled from what the shim did, not from what it sent, and a loss that the shim's own odds drew in the same moment
 * is not lost to the count.
 * $CRASHFS_IGNORE_USR2=1 makes the shim ignore SIGUSR2 (a self-test of that count); =2 makes it honour the signal and not log the
 * plug (the loss is as sudden as ever, the harness has no proof of it: whatever the speed of the file system, the plug is there and not counted).
 *
 * With CRASHFS_TEAR=1 a power loss keeps what a real disk may keep rather
 * than the least it must (each choice drawn from $CRASHFS_SEED):
 *  - of a file's unsynced appends, a random prefix: none, all, or up to a
 *    512-byte sector boundary (the page cache writes back in any order, but
 *    a sector is written whole);
 *  - or, at random, nothing after the durable end but garbage up to the end
 *    of the sector that holds it (that sector was being written when the
 *    power went; the bytes before the durable end were synced and stay);
 *  - of the sectors rewritten in place since the last sync, each one old or
 *    new at random (MANIFEST is rewritten in place);
 *  - of the directory operations not made durable, a random subset, in the
 *    order they were made for each name: once one on a name is undone,
 *    every later one on that name (or inside it) is undone too.
 * $CRASHFS_UNDO_ONE=k replaces that random subset by a chosen one: every directory operation not made durable is kept but the
 * k-th of them (0-based, oldest first), which is undone with whatever it forces (later ones on its names); k at or past their
 * number keeps them all. $CRASHFS_UNDO_COUNT=FILE then gets their number, written at the power loss, so a harness steps k from
 * 0 up to it. A sweep of power losses at every step with fixed seeds finds a window like "the intent's unlink kept, one of
 * its renames lost" only by the luck of the draws (a reorder of the draws once hid test_drops' ttlunlinkfirst); this finds it
 * at every step where it exists.
 *
 * A file's record (its durable size, its lie after an EIO, its kept old sectors) is found by its inode, and an inode is
 * freed and given to a new file while the process runs. Three belts keep a new file from inheriting a dead one's record,
 * each with a case of tests/test_crashfs.c `reuse` where it alone stands between them: a file created through the shim's
 * open starts a fresh record (file_renew in file_get; `reuse-open`: the old file freed where the shim did not see it); a
 * durable unlink of a file's last name drops its record (do_sync; `reuse-unseen`: the new file made where the shim does not
 * see it, then renamed through it); a close of a tracked descriptor of a file with no name left drops the record a write
 * through it re-made after that unlink (close; `reuse-close`). Known limit: a file whose death the shim did not see (a raw
 * unlink, glibc's remove) and whose inode then goes to a file whose birth it did not see either (mkstemp and mkostemp create
 * through glibc's internal open, which LD_PRELOAD does not reach: server/chunksrv.c chunksrv_stage, server/tsdbd.c) inherits
 * the record at its first rename through the shim. No tsdb path removes a tracked file unseen.
 *
 * A duplicate of a tracked descriptor (dup, dup2, dup3, fcntl F_DUPFD) is the same file: syncing it makes the file
 * durable (the Raft log of tsdbd --cluster hands duplicates of its segments to a worker to sync).
 *
 * syncfs of a tracked file or directory makes everything on its file system
 * durable (files that lie after an injected EIO excepted), and is a point
 * where the EIO can be injected.
 *
 * The dirty regions that rewrote durable bytes get those bytes back at a power loss, not zeros (what a disk keeps
 * of an in-place overwrite: old or new, never blank: round 13, the default; CRASHFS_REWRITE=old says it too); data
 * past the durable size is cut, and what a file system may return there (zeros, a hole) is all that is made up.
 * Without CRASHFS_TEAR (no kept prefixes, no mixed sectors), so a log append never synced is lost for certain, and
 * a rewritten hard-state slot is not blank. CRASHFS_REWRITE=zero gives the old harsher default: the rewritten
 * region (never a byte nobody rewrote) comes back as zeros.
 *
 * A file is found by the identity of the descriptor's inode, not by the name the descriptor was opened under: a write,
 * truncate or sync through a descriptor that is still open on a file that was renamed since (or whose old name now
 * names another file) is that file's, and an fsync then makes the renamed file durable.
 *
 * With CRASHFS_AT_EXIT=1 the power also fails as the process exits normally:
 * what a command reported done must already be durable. With $CRASHFS_EXIT_MARK set, the process that rolled
 * back at its exit creates `<that path>.<pid>` (per pid: a helper the process started inherits the shim and
 * rolls back on its own), so a harness can tell an exit that was a power cut from a `_exit`, which skips the
 * destructor and cuts nothing.
 * With $CRASHFS_EIO_DIR, only files below that directory fail (CRASHFS_EIO_ONLY and it both apply).
 *
 * With CRASHFS_MODE=misdirect there is no power loss; instead, with
 * probability $CRASHFS_MD_RATE per write, pwrite, read or pread of a regular
 * file inside $CRASHFS_ROOT, the I/O goes to the wrong place, as a disk or a
 * controller with a firmware bug may send it (misdirected I/O):
 *  - a write: one chunk of it (512 or 4096 bytes, or all of a smaller write,
 *    at an aligned or unaligned offset) lands at another offset of the same
 *    file, or in another file of the same directory (at a random offset or at
 *    the one it was meant for), clamped to that file's size. The chunk's own
 *    place keeps its old bytes (zeros where the write extended the file); the
 *    call reports success;
 *  - a read: one chunk of the buffer comes from another offset of the same
 *    file (a transient fault: the next read is right);
 *  - a part file (*.part), which tsdb reads through a read-only shared
 *    mapping whose loads no shim can see: with probability
 *    $CRASHFS_MD_MAP_RATE (default 0.05) its mmap returns a private copy of
 *    the file with one chunk taken from another offset, as a page misread
 *    into the page cache would be until it is evicted. The copy does not see
 *    later writes to the file; parts are never written after they are named.
 * $CRASHFS_MD_FILES=parts limits the faults to part files (as source and as
 * destination), =noparts to every other file. Every fault is appended to
 * $CRASHFS_MD_LOG (else stderr) as one line: pid, kind, file, offsets and
 * length, so a checker can tie what it sees to a fault.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#define MAX_FDS 65536
#define MAX_FILES 65536
#define MAX_OPS 1000000

typedef struct file {
    dev_t dev;
    ino_t ino;
    char path[PATH_MAX];
    off_t durable;
    struct dirty_ext {
        off_t off, len;
    } *dirty; /* CRASHFS_REWRITE=zero only: the regions rewritten in place (or cut) since the last sync */
    int ndirty, capdirty;
    bool used;
    bool lying; /* after an injected EIO: syncs succeed without making anything durable */
    struct old_sector *old; /* CRASHFS_TEAR or the old-bytes default: durable sectors rewritten since the last sync, as they were */
    size_t nold, capold;
    unsigned tsave;  /* open(O_TRUNC) of a durable file: the id of the copy of what a power loss brings back (0: none), until the file is synced */
    off_t tsave_len; /* its length */
    size_t tsave_ndops; /* how many directory operations there were when the truncation was made (CRASHFS_TEAR: a rename after it that survives takes it along) */
} file;

#define SECTOR 512
struct old_sector {
    off_t off;
    unsigned char data[SECTOR];
};

typedef enum { OP_CREATE, OP_RENAME, OP_UNLINK, OP_MKDIR } op_kind;

typedef struct dirop {
    op_kind kind;
    char dir[PATH_MAX];   /* the directory whose fsync makes it durable */
    char a[PATH_MAX];     /* created / renamed from / removed */
    char b[PATH_MAX];     /* renamed to */
    char saved[PATH_MAX]; /* a file this op replaced or removed, kept aside */
    bool done;            /* made durable */
    bool undone;          /* rolled back by the power loss */
} dirop;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static char root[PATH_MAX];
static size_t rootlen;
static bool active;
static double prob;
static long warmup, ops;
static uint64_t rng;
static volatile sig_atomic_t crash_requested;
static bool tear; /* CRASHFS_TEAR=1 */
static bool rewrite_zero; /* CRASHFS_REWRITE=zero: the regions rewritten in place come back as zeros (harsher than a disk); else as they were */
static unsigned tear_prefix, tear_garbage, tear_old, tear_kept; /* what the power loss kept, for the message */
static long undo_one = -1;          /* CRASHFS_UNDO_ONE: the one unsynced directory operation undone (-1: drawn at random) */
static char undo_count[PATH_MAX];   /* CRASHFS_UNDO_COUNT: where their number goes at the power loss */
static bool eio_mode, eio_done; /* CRASHFS_MODE=eio, and the error was injected */
static bool loss_at_exit;       /* CRASHFS_AT_EXIT=1 */
static bool md_mode;            /* CRASHFS_MODE=misdirect */
static double md_rate, md_map_rate;
static int md_files; /* 0: every file, 1: parts only, 2: all but parts */
static char md_log[PATH_MAX];
static char lying_dir[PATH_MAX]; /* a directory whose syncs lie after the error */
static char mark[PATH_MAX], eio_only[64], eio_on[16], eio_dir[PATH_MAX], exit_mark[PATH_MAX];
static unsigned mark_slow_ms; /* CRASHFS_MARK_SLOW_MS: the mark file is created this long before its line is written (a thread that waits for a CPU does the same) */
static char *fdpath[MAX_FDS];
/* the file a tracked descriptor was opened on (round 14). A descriptor closed by a call the shim does not see (closedir
 * of a dup'ed directory descriptor; fclose) leaves its entry behind, and the next socket, pipe or eventfd of that number
 * would be taken for the file: a worker's wake-up write failed "on the directory" with EIO, the server went on, and the
 * harness reported a server that did not fail-stop. fd_tracked checks the descriptor still is that file. */
static dev_t fddev[MAX_FDS];
static ino_t fdino[MAX_FDS];
static file *files;
static size_t nfiles;
static dirop *dops;
static size_t ndops;
static unsigned saved_counter;

#define REAL(name) static __typeof__(name) *real_##name
REAL(open);
REAL(read);
REAL(pread);
REAL(mmap);
REAL(openat);
REAL(write);
REAL(pwrite);
REAL(writev);
REAL(pwritev);
REAL(fsync);
REAL(fdatasync);
REAL(syncfs);
REAL(rename);
REAL(renameat);
REAL(unlink);
REAL(unlinkat);
REAL(close);
REAL(dup);
REAL(dup2);
REAL(dup3);
REAL(fcntl);
REAL(mkdir);
REAL(mkdirat);
REAL(ftruncate);
REAL(closedir);

static volatile sig_atomic_t power_losing; /* the power loss is rolling files back: another thread that reads a mapping of one gets SIGBUS */
static volatile sig_atomic_t usr2_seen; /* only a SIGUSR2 sets it: the power loss line says so (jepsen counts the nemesis's own) */
static char pull_file[PATH_MAX];        /* $CRASHFS_PULL_FILE: a line per power loss that a SIGUSR2 asked for, written by the shim */
static int ignore_usr2;                 /* $CRASHFS_IGNORE_USR2: 1: a shim that does not honour the pull, 2: one that honours it and does not log it (jepsen's self-test of its count) */

/* The harness's proof of a plug it pulled is written where the signal is delivered, not where the process dies of it: a loss
 * drawn by the shim's own odds on another thread can win the race to the exit (the signal arriving while that loss is rolling
 * back, 2 of 17 pulls of a classes run), and the harness then saw a server that lost power with the pull's line missing.
 * The server loses power either way, and a signal the shim ignored (=1) or a server that was dead (never delivered) leaves no
 * line. Async-signal-safe: raw system calls, the line built by hand. */
static void log_pull_delivered(void)
{
    if (!pull_file[0] || ignore_usr2 == 2)
        return;
    char line[64], digits[16];
    int nd = 0, n = 0;
    long pid = (long)getpid();
    do
        digits[nd++] = (char)('0' + pid % 10);
    while ((pid /= 10) > 0);
    static const char head[] = "pid ", tail[] = " power loss requested by SIGUSR2\n";
    for (size_t i = 0; i < sizeof head - 1; i++)
        line[n++] = head[i];
    while (nd > 0)
        line[n++] = digits[--nd];
    for (size_t i = 0; i < sizeof tail - 1; i++)
        line[n++] = tail[i];
    int fd = (int)syscall(SYS_openat, AT_FDCWD, pull_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        if (syscall(SYS_write, fd, line, (size_t)n) < 0) {
        }
        syscall(SYS_close, fd);
    }
}

static void on_bus(int sig);

static void on_usr2(int sig)
{
    (void)sig;
    if (ignore_usr2 == 1)
        return;
    int saved = errno;
    usr2_seen = 1;
    log_pull_delivered();
    crash_requested = 1;
    errno = saved;
}

/* 16 GB of address space, touched only as far as used. Not calloc: a sanitizer's allocator
 * clears it all, which takes seconds when the shim is preloaded into a sanitizer build. */
static void *zeroed(size_t size)
{
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}

/* A list that grows (the dirty extents, the kept old sectors) lives in its own mapping, never in the heap: a sanitizer's leak
 * check does not scan the shim's mapped tables (`files`), so a heap block only they point to reads as a leak at the exit of a
 * process that loaded the shim (test_export under ASan: the default old-bytes mode of round 13 keeps sectors in every run). */
static void *regrow(void *p, size_t oldsz, size_t newsz)
{
    void *n = zeroed(newsz);
    if (!n)
        return NULL;
    if (p) {
        memcpy(n, p, oldsz);
        munmap(p, oldsz);
    }
    return n;
}

__attribute__((constructor)) static void init(void)
{
#define LOAD(name) real_##name = dlsym(RTLD_NEXT, #name)
    LOAD(open);
    LOAD(read);
    LOAD(pread);
    LOAD(mmap);
    LOAD(openat);
    LOAD(write);
    LOAD(pwrite);
    LOAD(writev);
    LOAD(pwritev);
    LOAD(fsync);
    LOAD(fdatasync);
    LOAD(syncfs);
    LOAD(rename);
    LOAD(renameat);
    LOAD(unlink);
    LOAD(unlinkat);
    LOAD(close);
    LOAD(dup);
    LOAD(dup2);
    LOAD(dup3);
    LOAD(fcntl);
    LOAD(mkdir);
    LOAD(mkdirat);
    LOAD(ftruncate);
    LOAD(closedir);
    const char *r = getenv("CRASHFS_ROOT");
    if (!r || !realpath(r, root))
        return;
    rootlen = strlen(root);
    prob = getenv("CRASHFS_PROB") ? atof(getenv("CRASHFS_PROB")) : 0;
    warmup = getenv("CRASHFS_WARMUP") ? atol(getenv("CRASHFS_WARMUP")) : 0;
    rng = getenv("CRASHFS_SEED") ? strtoull(getenv("CRASHFS_SEED"), NULL, 10) : (uint64_t)getpid();
    rng = rng * 6364136223846793005ull + 1;
    files = zeroed(MAX_FILES * sizeof *files);
    dops = zeroed(MAX_OPS * sizeof *dops);
    eio_mode = getenv("CRASHFS_MODE") && !strcmp(getenv("CRASHFS_MODE"), "eio");
    if (getenv("CRASHFS_EIO_ONLY"))
        snprintf(eio_only, sizeof eio_only, "%s", getenv("CRASHFS_EIO_ONLY"));
    if (getenv("CRASHFS_EIO_ON"))
        snprintf(eio_on, sizeof eio_on, "%s", getenv("CRASHFS_EIO_ON"));
    if (getenv("CRASHFS_EIO_DIR"))
        snprintf(eio_dir, sizeof eio_dir, "%s", getenv("CRASHFS_EIO_DIR"));
    if (getenv("CRASHFS_EXIT_MARK"))
        snprintf(exit_mark, sizeof exit_mark, "%s", getenv("CRASHFS_EXIT_MARK"));
    if (getenv("CRASHFS_MARK"))
        snprintf(mark, sizeof mark, "%s", getenv("CRASHFS_MARK"));
    if (getenv("CRASHFS_MARK_SLOW_MS"))
        mark_slow_ms = (unsigned)atoi(getenv("CRASHFS_MARK_SLOW_MS"));
    tear = getenv("CRASHFS_TEAR") && atoi(getenv("CRASHFS_TEAR")) != 0;
    if (getenv("CRASHFS_UNDO_ONE"))
        undo_one = atol(getenv("CRASHFS_UNDO_ONE"));
    if (getenv("CRASHFS_UNDO_COUNT"))
        snprintf(undo_count, sizeof undo_count, "%s", getenv("CRASHFS_UNDO_COUNT"));
    rewrite_zero = getenv("CRASHFS_REWRITE") && !strcmp(getenv("CRASHFS_REWRITE"), "zero");
    loss_at_exit = getenv("CRASHFS_AT_EXIT") && atoi(getenv("CRASHFS_AT_EXIT")) != 0;
    md_mode = getenv("CRASHFS_MODE") && !strcmp(getenv("CRASHFS_MODE"), "misdirect");
    md_rate = getenv("CRASHFS_MD_RATE") ? atof(getenv("CRASHFS_MD_RATE")) : 0.001;
    md_map_rate = getenv("CRASHFS_MD_MAP_RATE") ? atof(getenv("CRASHFS_MD_MAP_RATE")) : 0.05;
    const char *mf = getenv("CRASHFS_MD_FILES");
    md_files = !mf ? 0 : !strcmp(mf, "parts") ? 1 : !strcmp(mf, "noparts") ? 2 : 0;
    if (getenv("CRASHFS_MD_LOG"))
        snprintf(md_log, sizeof md_log, "%s", getenv("CRASHFS_MD_LOG"));
    if (md_mode)
        prob = 0;
    if (getenv("CRASHFS_PULL_FILE"))
        snprintf(pull_file, sizeof pull_file, "%s", getenv("CRASHFS_PULL_FILE"));
    ignore_usr2 = getenv("CRASHFS_IGNORE_USR2") ? atoi(getenv("CRASHFS_IGNORE_USR2")) : 0;
    signal(SIGUSR2, on_usr2);
    signal(SIGBUS, on_bus);
    active = files && dops;
}

static double urand(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return (double)(rng >> 11) / 9007199254740992.0;
}

/* ------------------------------------------------------------- tracking */

/* misdirect mode keeps none of the power-loss state: only which descriptors are its files (md_open) */
static bool tracked(const char *p) { return active && !md_mode && strncmp(p, root, rootlen) == 0; }

static void path_of_at(int dirfd, const char *name, char *out)
{
    if (name[0] == '/' || dirfd == AT_FDCWD) {
        if (name[0] == '/')
            snprintf(out, PATH_MAX, "%s", name);
        else {
            char cwd[PATH_MAX];
            snprintf(out, PATH_MAX, "%s/%s", getcwd(cwd, sizeof cwd) ? cwd : ".", name);
        }
        return;
    }
    char link[64], dir[PATH_MAX];
    snprintf(link, sizeof link, "/proc/self/fd/%d", dirfd);
    ssize_t n = readlink(link, dir, sizeof dir - 1);
    dir[n > 0 ? n : 0] = '\0';
    snprintf(out, PATH_MAX, "%s/%s", dir, name);
}

static void dir_of(const char *p, char *out)
{
    snprintf(out, PATH_MAX, "%s", p);
    char *s = strrchr(out, '/');
    if (s && s != out)
        *s = '\0';
}

static void tsave_drop(file *f);

/* A record whose inode was freed and is now another file's: nothing of the dead file (its durable size, its lie after an EIO,
 * its kept old sectors) is the new one's. Inodes are freed while the process runs since a durable unlink removes its kept-aside
 * copy at the directory's fsync (do_sync): before that fix a chunk put whose tmp file got the inode of a tmp that had met an
 * injected EIO inherited its lie, its fsync never counted, and the power loss at exit cut an acknowledged chunk to 0 bytes
 * (test_chunk's eio sweep). */
static void file_renew(file *f, off_t durable)
{
    tsave_drop(f);
    f->durable = durable;
    f->ndirty = 0;
    f->nold = 0;
    f->lying = false;
}

/* The inode (dev, ino) is free now, or will be at its last close: its records go (see the head of the file). */
static void record_drop(dev_t dev, ino_t ino)
{
    for (size_t j = 0; j < nfiles; j++)
        if (files[j].used && files[j].dev == dev && files[j].ino == ino) {
            file_renew(&files[j], 0);
            files[j].used = false;
        }
}

/* The record of the file at `path`, found by its inode, made if there is none. create_new: the caller just created the
 * file, so a record with its inode is a dead file's (the inode was reused) and starts afresh. */
static file *file_get(const char *path, bool create_new)
{
    struct stat st;
    if (stat(path, &st) != 0)
        return NULL;
    for (size_t i = 0; i < nfiles; i++)
        if (files[i].used && files[i].dev == st.st_dev && files[i].ino == st.st_ino) {
            snprintf(files[i].path, PATH_MAX, "%s", path);
            if (create_new)
                file_renew(&files[i], 0);
            return &files[i];
        }
    if (nfiles == MAX_FILES || !S_ISREG(st.st_mode))
        return NULL;
    file *f = &files[nfiles++];
    memset(f, 0, sizeof *f);
    f->used = true;
    f->dev = st.st_dev;
    f->ino = st.st_ino;
    snprintf(f->path, PATH_MAX, "%s", path);
    f->durable = create_new ? 0 : st.st_size; /* files from before we started are durable */
    return f;
}

/* The file behind a tracked descriptor, found by the identity of its inode: the name the descriptor was opened under may have been
 * renamed since (the descriptor's file is then the renamed one), or may name another file now (a new file made at the old name).
 * Round 13 (VERIFY12): a sync or write through such a descriptor went to the record of whatever the old name said, so an fsync never
 * raised the renamed file's durable size and a power loss cut fsynced bytes. A file that has no record yet (made before the shim was
 * loaded, or opened by a path the shim did not see) gets one, durable as it is. */
static file *file_of_fd(int fd)
{
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
        return NULL;
    for (size_t i = 0; i < nfiles; i++)
        if (files[i].used && files[i].dev == st.st_dev && files[i].ino == st.st_ino)
            return &files[i];
    if (nfiles == MAX_FILES)
        return NULL;
    file *f = &files[nfiles++];
    memset(f, 0, sizeof *f);
    f->used = true;
    f->dev = st.st_dev;
    f->ino = st.st_ino;
    snprintf(f->path, PATH_MAX, "%s", fdpath[fd] ? fdpath[fd] : "");
    f->durable = st.st_size;
    return f;
}

/* Zero mode: a region of a durable file that was rewritten or cut, joined to the last one when they touch (never a region that nobody wrote). */
static void dirty_add(file *f, off_t off, off_t len)
{
    if (len <= 0)
        return;
    if (f->ndirty > 0) {
        struct dirty_ext *l = &f->dirty[f->ndirty - 1];
        if (off <= l->off + l->len && off + len >= l->off) {
            off_t lo = off < l->off ? off : l->off, hi = off + len > l->off + l->len ? off + len : l->off + l->len;
            l->off = lo;
            l->len = hi - lo;
            return;
        }
    }
    if (f->ndirty == f->capdirty) {
        int cap = f->capdirty ? 2 * f->capdirty : 16;
        struct dirty_ext *d = regrow(f->dirty, (size_t)f->capdirty * sizeof *d, (size_t)cap * sizeof *d);
        if (!d) { /* no room: the region is not remembered, so it keeps its new bytes (the disk may have kept them too) */
            return;
        }
        f->dirty = d;
        f->capdirty = cap;
    }
    f->dirty[f->ndirty].off = off;
    f->dirty[f->ndirty].len = len;
    f->ndirty++;
}

/* The bytes [off, off + n) of the file behind `fd`, read through a descriptor crashfs opens itself: the caller's may be
 * write-only (tsdb opens CLASSES O_WRONLY), and a failed pread there once left zeros that tear_file wrote back over
 * synced records. False when they cannot be read in full: the caller keeps nothing, never bytes it made up. */
static bool read_through(int fd, const char *path, void *buf, size_t n, off_t off)
{
    char self[64];
    snprintf(self, sizeof self, "/proc/self/fd/%d", fd);
    int rfd = real_open(self, O_RDONLY | O_CLOEXEC, 0);
    if (rfd < 0 && path[0]) { /* no /proc: the file's path, if it is still the same inode */
        struct stat a, b;
        rfd = real_open(path, O_RDONLY | O_CLOEXEC, 0);
        if (rfd >= 0 && (fstat(rfd, &a) != 0 || fstat(fd, &b) != 0 || a.st_ino != b.st_ino || a.st_dev != b.st_dev)) {
            real_close(rfd);
            rfd = -1;
        }
    }
    if (rfd < 0)
        return false;
    size_t got = 0;
    while (got < n) {
        ssize_t r = real_pread(rfd, (char *)buf + got, n - got, off + (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    real_close(rfd);
    return got == n;
}

/* CRASHFS_TEAR: keeps the durable content of the sectors in [off, end) that
 * are about to change, once per sync, so a power loss can bring them back. */
static void keep_old(file *f, int fd, off_t off, off_t end)
{
    if (!tear && rewrite_zero)
        return;
    if (end > f->durable)
        end = f->durable;
    for (off_t s = off / SECTOR * SECTOR; s < end; s += SECTOR) {
        bool have = false;
        for (size_t i = 0; i < f->nold && !have; i++)
            have = f->old[i].off == s;
        if (have)
            continue;
        unsigned char data[SECTOR];
        off_t len = f->durable - s < SECTOR ? f->durable - s : SECTOR; /* the durable bytes of the sector */
        memset(data, 0, sizeof data);
        if (!read_through(fd, f->path, data, (size_t)len, s))
            continue; /* not kept: a sector of made-up bytes is a loss no disk has */
        if (f->nold == f->capold) {
            size_t cap = f->capold ? 2 * f->capold : 16;
            struct old_sector *o = regrow(f->old, f->capold * sizeof *o, cap * sizeof *o);
            if (!o)
                return;
            f->old = o;
            f->capold = cap;
        }
        struct old_sector *o = &f->old[f->nold++];
        o->off = s;
        memcpy(o->data, data, SECTOR);
    }
}

/* A path inside directory `from` (not `from` itself) moved to the same place under `to`. */
static void rekey(char *p, const char *from, const char *to)
{
    size_t n = strlen(from);
    char t[PATH_MAX];
    if (n && !strncmp(p, from, n) && p[n] == '/' && snprintf(t, sizeof t, "%s%s", to, p + n) < (int)sizeof t)
        memcpy(p, t, strlen(t) + 1);
}

/* Path p, recorded by operation i, under the name it has now: moved by every directory rename after operation i
 * that is in effect (while running, all of them; during a power loss, those not undone). Operations record the
 * names they saw; a directory renamed later (a database built beside its name, then renamed to it) moves them. */
static void op_now(size_t i, const char *p, char *out)
{
    char t[PATH_MAX];
    snprintf(t, sizeof t, "%s", p);
    for (size_t j = i + 1; j < ndops; j++)
        if (dops[j].kind == OP_RENAME && !dops[j].undone)
            rekey(t, dops[j].a, dops[j].b);
    memcpy(out, t, strlen(t) + 1);
}

static void add_dirop(op_kind k, const char *a, const char *b, const char *saved)
{
    if (ndops == MAX_OPS)
        return;
    dirop *d = &dops[ndops++];
    memset(d, 0, sizeof *d);
    d->kind = k;
    dir_of(k == OP_RENAME ? b : a, d->dir);
    snprintf(d->a, PATH_MAX, "%s", a);
    if (b)
        snprintf(d->b, PATH_MAX, "%s", b);
    if (saved)
        snprintf(d->saved, PATH_MAX, "%s", saved);
}

/* Moves an existing file aside so that undoing can bring it back. */
static bool keep_aside(const char *path, char *saved)
{
    struct stat st;
    if (lstat(path, &st) != 0 || S_ISDIR(st.st_mode)) /* unlink of a directory fails (EISDIR): nothing is kept aside, the real call fails */
        return false;
    char dir[PATH_MAX];
    dir_of(path, dir);
    snprintf(saved, PATH_MAX, "%s/.crashfs_saved_%d_%u", dir, (int)getpid(), saved_counter++);
    return real_rename(path, saved) == 0;
}

/* open(O_TRUNC) of a durable file (round 14; VERIFY13 finding 2). The file stays the same file (its name and its inode: a
 * descriptor, a hard link or a mapping that another party holds sees what a truncation does, never a new file). What it holds
 * from then on is new content, none of it durable until the file is synced: an fsync of the descriptor makes the truncation and
 * what was written after it durable (on ext4 and every journalling file system the truncation commits with the data's size at
 * that sync), and so does a sync of its directory or of the file system (the transaction that carries the truncation commits).
 * Until then a power loss may leave the truncation undone: the old content is back as it was durable (a copy, kept in the root
 * directory, made at the truncation: the durable prefix with the rewritten sectors put back, or zeroed in the harsher mode). With
 * CRASHFS_TEAR the new content may instead be what the loss leaves (a prefix of it, an empty file), as for any unsynced data.
 * Never an older state than the last sync, never a mix of old and new bytes that no disk leaves in the plain modes. An open(O_TRUNC) that
 * fails truncated nothing (the state before it is put back), and with CRASHFS_TEAR a rename of the file that survives after the truncation takes
 * the truncation with it (the metadata operations of a journalled file system commit in order). Known gap: only the file's last rename is
 * looked at; a chain of renames (tmp to a, a to b) with the first kept and the last not may bring the old content back. No tsdb path does it
 * (every tmp is fsynced before its rename). */
static unsigned tsave_counter;
static void tsave_name(unsigned id, char *out)
{
    snprintf(out, PATH_MAX, "%s/.crashfs_tsave_%d_%u", root, (int)getpid(), id);
}

static void tsave_drop(file *f)
{
    if (f->tsave) {
        char sv[PATH_MAX];
        tsave_name(f->tsave, sv);
        real_unlink(sv);
        f->tsave = 0;
    }
}

/* A copy of what a power loss now leaves of `f` (read through `path`), at the saved name; its length, or -1. */
static off_t tsave_make(file *f, const char *path, const char *sv)
{
    int rfd = real_open(path, O_RDONLY | O_CLOEXEC, 0);
    if (rfd < 0)
        return -1;
    int wfd = real_open(sv, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (wfd < 0) {
        real_close(rfd);
        return -1;
    }
    static __thread char buf[1 << 16];
    off_t at = 0;
    bool ok = true;
    while (ok && at < f->durable) {
        size_t want = (size_t)(f->durable - at < (off_t)sizeof buf ? f->durable - at : (off_t)sizeof buf);
        ssize_t r = real_pread(rfd, buf, want, at);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break; /* the file is shorter than its durable size (an unsynced cut): the old sectors below put back what they can */
        ssize_t w = real_pwrite(wfd, buf, (size_t)r, at);
        ok = w == r;
        at += r;
    }
    off_t len = at;
    if (ok && !rewrite_zero) {
        for (size_t i = 0; i < f->nold; i++) {
            struct old_sector *o = &f->old[i];
            off_t l = f->durable - o->off < SECTOR ? f->durable - o->off : SECTOR;
            if (l > 0 && real_pwrite(wfd, o->data, (size_t)l, o->off) == l && o->off + l > len)
                len = o->off + l;
        }
    }
    for (int k = 0; ok && rewrite_zero && k < f->ndirty; k++) {
        off_t end = f->dirty[k].off + f->dirty[k].len;
        if (end > len)
            end = len;
        static const char zeros[4096];
        for (off_t o = f->dirty[k].off; o < end; o += (off_t)sizeof zeros)
            if (real_pwrite(wfd, zeros, (size_t)(end - o < (off_t)sizeof zeros ? end - o : (off_t)sizeof zeros), o) < 0)
                break;
    }
    if (len > f->durable)
        len = f->durable;
    if (ok && real_ftruncate(wfd, len) != 0)
        ok = false;
    real_close(rfd);
    real_close(wfd);
    if (!ok)
        real_unlink(sv);
    return ok ? len : -1;
}

/* open(O_TRUNC) of `f`: keeps the durable image aside, and the file's content is new from here on. */
static void tsave_trunc(file *f, const char *path)
{
    if (f->durable > 0 && !f->tsave) {
        char sv[PATH_MAX];
        unsigned id = ++tsave_counter;
        tsave_name(id, sv);
        off_t len = tsave_make(f, path, sv);
        if (len >= 0) {
            f->tsave = id;
            f->tsave_len = len;
            f->tsave_ndops = ndops;
        }
    }
    f->durable = 0;
    f->ndirty = 0;
    f->nold = 0;
}

/* The power loss brings the old content back into the file behind `fd`. */
static void tsave_restore(file *f, int fd)
{
    char sv[PATH_MAX];
    tsave_name(f->tsave, sv);
    int rfd = real_open(sv, O_RDONLY | O_CLOEXEC, 0);
    if (rfd < 0)
        return;
    static __thread char buf[1 << 16];
    off_t at = 0;
    while (at < f->tsave_len) {
        ssize_t r = real_pread(rfd, buf, (size_t)(f->tsave_len - at < (off_t)sizeof buf ? f->tsave_len - at : (off_t)sizeof buf), at);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0 || real_pwrite(fd, buf, (size_t)r, at) != r)
            break;
        at += r;
    }
    if (at == f->tsave_len && real_ftruncate(fd, f->tsave_len) != 0) {
    }
    real_close(rfd);
}

/* ---------------------------------------------------------- power loss */

static void rm_tree(const char *p)
{
    struct stat st;
    if (lstat(p, &st) != 0)
        return;
    if (S_ISDIR(st.st_mode)) {
        DIR *d = opendir(p);
        struct dirent *e;
        while (d && (e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
                continue;
            char c[PATH_MAX * 2];
            snprintf(c, sizeof c, "%s/%s", p, e->d_name);
            rm_tree(c);
        }
        if (d)
            closedir(d);
        rmdir(p);
    } else {
        real_unlink(p);
    }
}

/* CRASHFS_TEAR: what a disk may keep of a file (see the header). */
static void tear_file(file *f, int fd, off_t size)
{
    /* sectors rewritten in place: old or new, each; past the current end
     * (an unsynced truncate) the old ones come back */
    for (size_t i = 0; i < f->nold; i++) {
        struct old_sector *o = &f->old[i];
        off_t len = f->durable - o->off < SECTOR ? f->durable - o->off : SECTOR;
        if (o->off + len > size || urand() < 0.5) {
            tear_old++;
            if (real_pwrite(fd, o->data, (size_t)len, o->off) < 0) {
            }
        }
    }
    if (size <= f->durable)
        return;
    off_t keep;
    off_t sector_end = (f->durable + SECTOR - 1) / SECTOR * SECTOR;
    if (f->durable % SECTOR && urand() < 0.25) {
        /* the sector holding the durable end was being written: garbage after that end */
        keep = sector_end < size ? sector_end : size;
        tear_garbage++;
        unsigned char junk[SECTOR];
        for (off_t k = 0; k < keep - f->durable; k++)
            junk[k] = (unsigned char)(urand() * 256);
        if (real_pwrite(fd, junk, (size_t)(keep - f->durable), f->durable) < 0) {
        }
    } else {
        /* cut at the durable end, at a sector boundary after it, or nowhere: one of them at random */
        off_t first = f->durable / SECTOR * SECTOR + SECTOR;
        off_t nb = first < size ? (size - 1 - first) / SECTOR + 1 : 0; /* boundaries strictly inside */
        off_t k = (off_t)(urand() * (double)(nb + 2));
        keep = k == 0 ? f->durable : k > nb ? size : first + (k - 1) * SECTOR;
        if (keep > f->durable)
            tear_prefix++;
    }
    if (keep < size && real_ftruncate(fd, keep) != 0) {
    }
}

/* CRASHFS_TEAR: names whose undone operations force later ones on them (or inside them) to be undone too. */
static bool name_cut(char **cut, size_t ncut, const char *name)
{
    if (!name[0])
        return false;
    for (size_t i = 0; i < ncut; i++) {
        size_t n = strlen(cut[i]);
        if (!strncmp(cut[i], name, n) && (name[n] == '\0' || name[n] == '/'))
            return true;
    }
    return false;
}

static void tear_dirops(void)
{
    char **cut = malloc((2 * ndops + 1) * sizeof *cut);
    size_t ncut = 0;
    if (!cut)
        return;
    /* oldest first: each not durable op is kept at random (or all but the chosen one), unless an earlier one on its names
     * was undone */
    long nth = 0;
    for (size_t i = 0; i < ndops; i++) {
        dirop *d = &dops[i];
        if (d->done)
            continue;
        char srcdir[PATH_MAX];
        dir_of(d->a, srcdir);
        bool forced = name_cut(cut, ncut, d->a) || name_cut(cut, ncut, d->b) || name_cut(cut, ncut, srcdir) ||
                      name_cut(cut, ncut, d->dir);
        bool keep = undo_one >= 0 ? nth != undo_one : urand() < 0.5;
        nth++;
        if (!forced && keep) {
            d->done = true; /* reached the disk after all */
            tear_kept++;
            continue;
        }
        cut[ncut++] = d->a;
        if (d->b[0])
            cut[ncut++] = d->b;
    }
    free(cut);
    if (undo_count[0]) { /* raw calls: the lock is held and the shim's own open would track the file */
        char line[32];
        int n = snprintf(line, sizeof line, "%ld\n", nth);
        int fd = real_open(undo_count, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            if (real_write(fd, line, (size_t)n) < 0) {
            }
            real_close(fd);
        }
    }
}

/* Rolls the tracked tree back to what was made durable. Called with the lock held. */
/* CRASHFS_TEAR: a rename of the file (after its O_TRUNC) that reached the disk took the truncation, which came before it, along: a journalled file system
 * commits its metadata operations in order, so the old content is not back in a file that was renamed over its target (round 15, VERIFY14: the
 * old tmp's content in the target). Only the file's last rename is looked at (its current name is the destination). */
static bool tsave_carried(const file *f)
{
    for (size_t i = f->tsave_ndops; i < ndops; i++)
        if (dops[i].kind == OP_RENAME && dops[i].done && !strcmp(dops[i].b, f->path))
            return true;
    return false;
}

static void rollback(void)
{
    if (tear)
        tear_dirops(); /* the survivors among the directory operations are known before the files are cut back */
    /* file data first, by each file's current name */
    for (size_t i = 0; i < nfiles; i++) {
        file *f = &files[i];
        struct stat st;
        if (!f->used || stat(f->path, &st) != 0 || st.st_ino != f->ino)
            continue;
        int fd = real_open(f->path, O_WRONLY);
        if (fd < 0)
            continue;
        if (f->tsave && (!tear || (!tsave_carried(f) && urand() < 0.5))) { /* the truncation was never synced: undone (with CRASHFS_TEAR, or the new content torn) */
            tsave_restore(f, fd);
            real_close(fd);
            continue;
        }
        if (tear) {
            tear_file(f, fd, st.st_size);
            real_close(fd);
            continue;
        }
        if (!rewrite_zero) { /* rewritten in place, never synced: the old bytes come back, not zeros */
            for (size_t i = 0; i < f->nold; i++) {
                struct old_sector *o = &f->old[i];
                off_t len = f->durable - o->off < SECTOR ? f->durable - o->off : SECTOR;
                if (len > 0 && real_pwrite(fd, o->data, (size_t)len, o->off) < 0)
                    break;
            }
        }
        for (int k = 0; rewrite_zero && k < f->ndirty; k++) { /* CRASHFS_REWRITE=zero: rewritten in place, never synced */
            off_t end = f->dirty[k].off + f->dirty[k].len;
            if (end > f->durable)
                end = f->durable;
            static const char zeros[4096];
            for (off_t o = f->dirty[k].off; o < end; o += (off_t)sizeof zeros) {
                size_t n = (size_t)(end - o < (off_t)sizeof zeros ? end - o : (off_t)sizeof zeros);
                if (real_pwrite(fd, zeros, n, o) < 0)
                    break;
            }
        }
        if (st.st_size > f->durable)
            real_ftruncate(fd, f->durable);
        real_close(fd);
    }
    /* then directory entries, newest first */
    for (size_t i = ndops; i-- > 0;) {
        dirop *d = &dops[i];
        if (d->done)
            continue;
        op_now(i, d->a, d->a);
        op_now(i, d->b, d->b);
        op_now(i, d->saved, d->saved);
        d->undone = true;
        switch (d->kind) {
        case OP_CREATE: real_unlink(d->a); break;
        case OP_MKDIR: rm_tree(d->a); break;
        case OP_RENAME:
            real_rename(d->b, d->a);
            if (d->saved[0])
                real_rename(d->saved, d->b);
            break;
        case OP_UNLINK:
            if (d->saved[0])
                real_rename(d->saved, d->a);
            break;
        }
    }
    /* entries made durable that were kept aside: really gone now */
    for (size_t i = 0; i < ndops; i++)
        if (dops[i].done && dops[i].saved[0])
            rm_tree(dops[i].saved);
    for (size_t i = 0; i < nfiles; i++)
        tsave_drop(&files[i]);
}

/* A power cut stops the process at once without running sanitizer finalizers: _exit() is intercepted by ThreadSanitizer,
 * whose leak report on exit turns status 99 into 66 with halt_on_error=1 (as in server/crashpt.c). */
__attribute__((noreturn)) static void power_cut_exit(void)
{
#ifdef __linux__
    syscall(SYS_exit_group, 99);
#endif
    _exit(99);
}

/* A thread that reads a part it mapped, while the rollback cuts the file back, dies of SIGBUS before the thread that rolls back reaches
 * its _exit: the power went out under it, which is what a power loss is (the process is gone, status 99), not a crash of the server
 * (an honest run was failed "ended with status 0x87, not a power loss": a worker in part_open_as read a tmp part that the rollback had cut).
 * Any other SIGBUS keeps its default action. */
static void on_bus(int sig)
{
    if (power_losing)
        power_cut_exit();
    signal(sig, SIG_DFL); /* the instruction faults again: the default action */
}

__attribute__((noreturn)) static void power_loss(void)
{
    power_losing = 1;
    rollback();
    char msg[200];
    /* "requested by SIGUSR2": the harness (tests/jepsen/jepsen.c) pulled the plug; without it the loss was drawn by the
     * shim's own odds. The harness counts the first kind from this line, not from its own bookkeeping. */
    const char *by = usr2_seen ? " requested by SIGUSR2" : "";
    int n = tear ? snprintf(msg, sizeof msg,
                            "crashfs: power loss%s (torn: %u unsynced prefixes kept, %u garbage tails, %u old sectors "
                            "back, %u unsynced directory operations kept)\n",
                            by, tear_prefix, tear_garbage, tear_old, tear_kept)
                 : snprintf(msg, sizeof msg, "crashfs: power loss%s\n", by);
    if (real_write(2, msg, (size_t)n) < 0) {
    }
    power_cut_exit();
}

/* Called with the lock held, before each tracked operation. In eio mode a
 * fault only becomes due here; eio_now() injects it at the next write or sync. */
static void maybe_crash(void)
{
    ops++;
    if (eio_mode) {
        if (eio_done && crash_requested) /* a process that carried on after the error loses power */
            power_loss();
        if (!eio_done && !crash_requested && ops > warmup && prob > 0 && urand() < prob)
            crash_requested = 1;
        return;
    }
    if (crash_requested || (ops > warmup && prob > 0 && urand() < prob))
        power_loss();
}

/* Called with the lock held by a write or sync of fd: true if it must fail
 * with EIO now. Marks the file (or directory) as lying from here on. */
static bool eio_now(int fd, const char *what)
{
    if (!eio_mode || eio_done || !crash_requested)
        return false;
    const char *base = strrchr(fdpath[fd], '/') ? strrchr(fdpath[fd], '/') + 1 : fdpath[fd];
    if (eio_only[0] && strncmp(base, eio_only, strlen(eio_only)) != 0)
        return false;
    if (eio_on[0] && strcmp(what, eio_on) != 0)
        return false;
    if (eio_dir[0] && (strncmp(fdpath[fd], eio_dir, strlen(eio_dir)) != 0 || fdpath[fd][strlen(eio_dir)] != '/'))
        return false;
    eio_done = true;
    crash_requested = 0;
    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
        snprintf(lying_dir, sizeof lying_dir, "%s", fdpath[fd]);
    } else {
        file *f = file_of_fd(fd);
        if (f)
            f->lying = true;
    }
    char msg[PATH_MAX + 64];
    int n = snprintf(msg, sizeof msg, "crashfs: EIO on %s of %s\n", what, fdpath[fd]);
    if (real_write(2, msg, (size_t)n) < 0) {
    }
    if (mark[0]) {
        int m = real_open(mark, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (m >= 0) {
            if (mark_slow_ms)
                usleep(mark_slow_ms * 1000);
            n = snprintf(msg, sizeof msg, "%s\n", fdpath[fd]);
            if (real_write(m, msg, (size_t)n) < 0) {
            }
            real_close(m);
        }
    }
    return true;
}

/* After an injected EIO the process may exit on its own (fail-stop) or be
 * stopped: either way, what its lying syncs did not make durable is gone.
 * With CRASHFS_AT_EXIT the power fails just after a process that finished
 * (a command line tool: what it reported done must be on disk). */
__attribute__((destructor)) static void fini(void)
{
    if (!eio_done && !(loss_at_exit && active)) {
        for (size_t i = 0; i < nfiles; i++) /* no power loss: the copies kept for one are not left in the directory */
            tsave_drop(&files[i]);
        return;
    }
    pthread_mutex_lock(&lock); /* never released: no thread writes after this */
    rollback();
    if (exit_mark[0]) {
        char mp[PATH_MAX + 16];
        snprintf(mp, sizeof mp, "%s.%d", exit_mark, (int)getpid());
        int m = real_open(mp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (m >= 0)
            real_close(m);
    }
    static const char msg[] = "crashfs: at exit, unsynced data lost\n";
    if (real_write(2, msg, sizeof msg - 1) < 0) {
    }
}

/* ----------------------------------------------------------- misdirect */

static unsigned long md_seq; /* faults injected by this process */

static bool md_part(const char *p)
{
    size_t n = strlen(p);
    return n > 5 && !strcmp(p + n - 5, ".part");
}

/* A file of the database: inside the root, not a lock or the shim's own leftovers, and of the kind
 * CRASHFS_MD_FILES asks for. */
static bool md_eligible(const char *p)
{
    if (!active || strncmp(p, root, rootlen) != 0 || p[rootlen] != '/')
        return false;
    const char *base = strrchr(p, '/') + 1;
    if (base[0] == '.' || !strcmp(base, "LOCK"))
        return false;
    return md_files == 0 || (md_files == 1) == md_part(base);
}

static void md_note(const char *fmt, ...)
{
    char msg[3 * PATH_MAX];
    int n = snprintf(msg, sizeof msg, "crashfs[%d] misdirect %lu: ", (int)getpid(), ++md_seq);
    va_list ap;
    va_start(ap, fmt);
    n += vsnprintf(msg + n, sizeof msg - (size_t)n, fmt, ap);
    va_end(ap);
    if (n >= (int)sizeof msg - 1)
        n = (int)sizeof msg - 2;
    msg[n++] = '\n';
    int fd = md_log[0] ? real_open(md_log, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644) : 2;
    if (fd >= 0 && real_write(fd, msg, (size_t)n) < 0) {
    }
    if (fd > 2)
        real_close(fd);
}

static uint64_t md_below(uint64_t n) { return n ? (uint64_t)(urand() * (double)n) % n : 0; }

/* A chunk [*a, *a + *c) of [off, off + n): one 512 or 4096-byte block, aligned in the file or not, or all of a
 * smaller request. */
static void md_chunk(off_t off, size_t n, off_t *a, size_t *c)
{
    size_t want = urand() < 0.5 ? 512 : 4096;
    *c = n < want ? n : want;
    off_t at = off + (off_t)md_below(n - *c + 1);
    if (urand() < 0.5) { /* aligned to its size in the file, where that stays inside the request */
        off_t al = at / (off_t)want * (off_t)want;
        if (al >= off && al + (off_t)*c <= off + (off_t)n)
            at = al;
    }
    *a = at;
}

/* Where a misdirected chunk of `c` bytes meant for `a` in `path` lands: another offset of it (when it has data),
 * or another file of its directory, at a random offset or at `a`. Writes the destination's path and returns its
 * offset; -1 if there is nowhere (an empty directory and file). */
static off_t md_dest(const char *path, ino_t self, off_t size, off_t a, size_t c, char *dest)
{
    double kind = urand();
    if (kind < 0.5 || size == 0) { /* another file of the directory */
        char dir[PATH_MAX];
        dir_of(path, dir);
        DIR *d = opendir(dir);
        char pick[PATH_MAX] = "";
        off_t psize = 0;
        unsigned seen = 0;
        struct dirent *e;
        while (d && (e = readdir(d)) != NULL) {
            char p[PATH_MAX + 256];
            struct stat st;
            snprintf(p, sizeof p, "%s/%s", dir, e->d_name);
            if (!md_eligible(p) || stat(p, &st) != 0 || !S_ISREG(st.st_mode) || st.st_ino == self || st.st_size == 0)
                continue;
            if (md_below(++seen) == 0) { /* reservoir: each file equally likely */
                snprintf(pick, sizeof pick, "%.*s", PATH_MAX - 1, p);
                psize = st.st_size;
            }
        }
        if (d)
            closedir(d);
        if (pick[0]) {
            snprintf(dest, PATH_MAX, "%s", pick);
            if (kind < 0.2 && a < psize) /* the offset it was meant for, in the wrong file */
                return a;
            off_t o = (off_t)md_below((uint64_t)psize);
            return urand() < 0.5 ? o / (off_t)c * (off_t)c : o;
        }
        if (size == 0)
            return -1;
    }
    snprintf(dest, PATH_MAX, "%s", path);
    for (int i = 0; i < 8; i++) { /* another offset of the same file, not where it was meant to go */
        off_t o = (off_t)md_below((uint64_t)size);
        if (urand() < 0.5)
            o = o / 512 * 512;
        if (o != a)
            return o;
    }
    return -1;
}

static void md_pwrite_all(int fd, const char *buf, size_t n, off_t off)
{
    while (n > 0) {
        ssize_t r = real_pwrite(fd, buf, n, off);
        if (r <= 0)
            return;
        buf += r, n -= (size_t)r, off += r;
    }
}

/* A write of n bytes at `off` (for write(): where the file position or O_APPEND puts it) with one chunk sent
 * elsewhere. Called with the lock held; returns n, or -1 if it could not set up the fault (then nothing was
 * written and the caller writes normally). */
static ssize_t md_write(int fd, const void *buf, size_t n, off_t off, bool positional)
{
    struct stat st;
    if (n == 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
        return -1;
    int fl = fcntl(fd, F_GETFL);
    if (!positional)
        off = (fl & O_APPEND) ? st.st_size : lseek(fd, 0, SEEK_CUR);
    if (off < 0)
        return -1;
    off_t a;
    size_t c;
    md_chunk(off, n, &a, &c);
    char dest[PATH_MAX];
    off_t to = md_dest(fdpath[fd], st.st_ino, st.st_size, a, c, dest);
    if (to < 0)
        return -1;
    /* the file's own descriptor may be O_APPEND, where Linux appends a pwrite wherever it was aimed */
    char self[64];
    snprintf(self, sizeof self, "/proc/self/fd/%d", fd);
    int wfd = real_open(self, O_WRONLY | O_CLOEXEC, 0);
    if (wfd < 0)
        return -1;
    const char *b = buf;
    md_pwrite_all(wfd, b, (size_t)(a - off), off);
    md_pwrite_all(wfd, b + (a - off) + c, n - (size_t)(a - off) - c, a + (off_t)c);
    if (a + (off_t)c > st.st_size && a + (off_t)c == off + (off_t)n && real_ftruncate(wfd, off + (off_t)n) != 0) {
    } /* the file still grows: the chunk's place reads as zeros */
    int dfd = strcmp(dest, fdpath[fd]) ? real_open(dest, O_WRONLY | O_CLOEXEC, 0) : wfd;
    struct stat ds;
    size_t m = 0;
    if (dfd >= 0 && fstat(dfd, &ds) == 0 && to < ds.st_size) {
        m = (size_t)(ds.st_size - to) < c ? (size_t)(ds.st_size - to) : c;
        md_pwrite_all(dfd, b + (a - off), m, to);
    }
    if (dfd >= 0 && dfd != wfd)
        real_close(dfd);
    real_close(wfd);
    if (!positional && !(fl & O_APPEND))
        lseek(fd, off + (off_t)n, SEEK_SET);
    md_note("write %s [%lld, +%zu) of a %zu-byte write at %lld -> %s at %lld (%zu bytes)", fdpath[fd],
            (long long)a, c, n, (long long)off, dest, (long long)to, m);
    return (ssize_t)n;
}

/* After a read of r bytes at `off` into buf: one chunk of it replaced by bytes from another offset of the file. */
static void md_read(int fd, void *buf, size_t r, off_t off)
{
    struct stat st;
    if (r == 0 || off < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 2)
        return;
    off_t a;
    size_t c;
    md_chunk(off, r, &a, &c);
    if ((off_t)c > st.st_size)
        return;
    off_t from = (off_t)md_below((uint64_t)(st.st_size - (off_t)c + 1));
    if (urand() < 0.5 && from / 512 * 512 != a)
        from = from / 512 * 512;
    if (from == a)
        return;
    ssize_t got = real_pread(fd, (char *)buf + (a - off), c, from);
    md_note("read %s [%lld, +%zu) of a %zu-byte read at %lld <- from %lld (%zd bytes)", fdpath[fd], (long long)a, c,
            r, (long long)off, (long long)from, got);
}

/* --------------------------------------------------------------- hooks */

/* A descriptor number open() hands out is not the file a record of it names any more: that one was closed where this
 * shim does not see it (glibc's closedir of an fdopendir(dup(fd))). A stale record made an fsync of a file outside
 * the root fail as an EIO on a directory inside it. */
static void fd_forget(int fd)
{
    if (fd >= 0 && fd < MAX_FDS && fdpath[fd]) {
        pthread_mutex_lock(&lock);
        free(fdpath[fd]);
        fdpath[fd] = NULL;
        pthread_mutex_unlock(&lock);
    }
}

int closedir(DIR *d)
{
    fd_forget(dirfd(d));
    return real_closedir(d);
}

/* misdirect mode: remember the descriptor of a database file, nothing else */
static int md_open(int dirfd, const char *name, int flags, mode_t mode, bool at, const char *path)
{
    int fd = at ? real_openat(dirfd, name, flags, mode) : real_open(name, flags, mode);
    fd_forget(fd);
    struct stat st;
    if (fd >= 0 && fd < MAX_FDS && md_eligible(path) && fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) {
        pthread_mutex_lock(&lock);
        free(fdpath[fd]);
        fdpath[fd] = strdup(path);
        fddev[fd] = st.st_dev;
        fdino[fd] = st.st_ino;
        pthread_mutex_unlock(&lock);
    }
    return fd;
}

static int do_open(int dirfd, const char *name, int flags, mode_t mode, bool at)
{
    char path[PATH_MAX];
    path_of_at(at ? dirfd : AT_FDCWD, name, path);
    if (md_mode)
        return md_open(dirfd, name, flags, mode, at, path);
    if (!tracked(path)) {
        int fd = at ? real_openat(dirfd, name, flags, mode) : real_open(name, flags, mode);
        fd_forget(fd);
        return fd;
    }
    pthread_mutex_lock(&lock);
    maybe_crash();
    struct stat st;
    bool existed = stat(path, &st) == 0;
    file *tf = NULL;
    struct { /* what the file was before tsave_trunc: an open that FAILS truncated nothing (round 15, VERIFY14: EMFILE, EEXIST) */
        unsigned tsave;
        off_t tsave_len, durable;
        size_t tsave_ndops;
        int ndirty;
        size_t nold;
    } keep = {0, 0, 0, 0, 0, 0};
    if ((flags & O_TRUNC) && existed && S_ISREG(st.st_mode)) {
        tf = file_get(path, false);
        if (tf) { /* the same file, truncated: not a removal and a new file (round 14), whatever O_CREAT says */
            keep.tsave = tf->tsave, keep.tsave_len = tf->tsave_len, keep.tsave_ndops = tf->tsave_ndops;
            keep.durable = tf->durable, keep.ndirty = tf->ndirty, keep.nold = tf->nold;
            tsave_trunc(tf, path);
        }
    }
    int fd = at ? real_openat(dirfd, name, flags, mode) : real_open(name, flags, mode);
    if (fd < 0 && tf) {
        if (tf->tsave != keep.tsave)
            tsave_drop(tf); /* the copy this call made */
        tf->tsave = keep.tsave;
        tf->tsave_len = keep.tsave_len;
        tf->tsave_ndops = keep.tsave_ndops;
        tf->durable = keep.durable; /* dirty and old are kept as they were: tsave_trunc only set their counts to 0 */
        tf->ndirty = keep.ndirty;
        tf->nold = keep.nold;
    }
    if (fd >= 0) {
        if ((flags & O_CREAT) && !existed) {
            add_dirop(OP_CREATE, path, NULL, NULL);
            file_get(path, true);
        } else {
            file_get(path, false);
        }
        if (fd < MAX_FDS) {
            free(fdpath[fd]);
            fdpath[fd] = strdup(path);
            if (fstat(fd, &st) == 0) {
                fddev[fd] = st.st_dev;
                fdino[fd] = st.st_ino;
            }
        }
    }
    pthread_mutex_unlock(&lock);
    return fd;
}

int open(const char *name, int flags, ...)
{
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    return do_open(AT_FDCWD, name, flags, mode, false);
}

int openat(int dirfd, const char *name, int flags, ...)
{
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    return do_open(dirfd, name, flags, mode, true);
}

int open64(const char *name, int flags, ...) __attribute__((alias("open")));
int openat64(int dirfd, const char *name, int flags, ...) __attribute__((alias("openat")));

static bool fd_is(int fd) /* the entry is there and the descriptor still is the file it was made for */
{
    struct stat st;
    return fd >= 0 && fd < MAX_FDS && fdpath[fd] && fstat(fd, &st) == 0 && st.st_dev == fddev[fd] &&
           st.st_ino == fdino[fd];
}

static bool fd_tracked(int fd) /* the lock is not held */
{
    if (fd < 0 || fd >= MAX_FDS || !fdpath[fd])
        return false;
    if (fd_is(fd))
        return true;
    pthread_mutex_lock(&lock); /* closed behind the shim's back: whatever has this number now is not the file */
    if (fdpath[fd] && !fd_is(fd)) {
        free(fdpath[fd]);
        fdpath[fd] = NULL;
    }
    bool t = fdpath[fd] != NULL;
    pthread_mutex_unlock(&lock);
    return t;
}

/* misdirect mode: a write that may go astray; false if the caller must do it as usual */
static bool md_maybe_write(int fd, const void *buf, size_t n, off_t off, bool positional, ssize_t *r)
{
    pthread_mutex_lock(&lock);
    *r = fd_is(fd) && urand() < md_rate ? md_write(fd, buf, n, off, positional) : -1;
    pthread_mutex_unlock(&lock);
    return *r >= 0;
}

ssize_t write(int fd, const void *buf, size_t n)
{
    if (!fd_tracked(fd))
        return real_write(fd, buf, n);
    if (md_mode) {
        ssize_t r;
        return md_maybe_write(fd, buf, n, -1, false, &r) ? r : real_write(fd, buf, n);
    }
    pthread_mutex_lock(&lock);
    maybe_crash();
    if (eio_now(fd, "write")) {
        if (n > 1 && real_write(fd, buf, n / 2) < 0) { /* a torn record */
        }
        pthread_mutex_unlock(&lock);
        errno = EIO;
        return -1;
    }
    ssize_t r = real_write(fd, buf, n); /* tsdb only appends with write() */
    pthread_mutex_unlock(&lock);
    return r;
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    if (!fd_tracked(fd))
        return real_pwrite(fd, buf, n, off);
    if (md_mode) {
        ssize_t r;
        return md_maybe_write(fd, buf, n, off, true, &r) ? r : real_pwrite(fd, buf, n, off);
    }
    pthread_mutex_lock(&lock);
    maybe_crash();
    file *f = file_of_fd(fd);
    bool fail = eio_now(fd, "pwrite");
    if (fail)
        n = n > 1 ? n / 2 : 0;
    const off_t durable = f ? f->durable : 0;
    const size_t nold = f ? f->nold : 0;
    const bool rewrite = f && off < durable; /* a rewrite of durable data */
    if (rewrite)
        keep_old(f, fd, off, off + (off_t)n);
    ssize_t r = real_pwrite(fd, buf, n, off);
    if (rewrite && r <= 0) { /* nothing was written (EBADF on an O_RDONLY descriptor, ENOSPC, EIO): nothing was rewritten */
        f->nold = nold;
    } else if (rewrite && rewrite_zero) { /* the bytes that were written, no more (a short write rewrote less) */
        off_t end = off + (off_t)r;
        dirty_add(f, off, (end < durable ? end : durable) - off);
    }
    pthread_mutex_unlock(&lock);
    if (fail) {
        errno = EIO;
        return -1;
    }
    return r;
}

ssize_t pwrite64(int fd, const void *buf, size_t n, off_t off) __attribute__((alias("pwrite")));

/* writev and pwritev of a tracked file: gathered and passed to write and pwrite above, so they count as operations,
 * wait for the lock while a power loss rolls back, and keep the old sectors of a rewrite like any other write */
static void *gather(const struct iovec *iov, int cnt, size_t *n)
{
    size_t t = 0;
    for (int i = 0; i < cnt; i++)
        t += iov[i].iov_len;
    char *b = malloc(t ? t : 1);
    if (!b)
        return NULL;
    size_t o = 0;
    for (int i = 0; i < cnt; i++) {
        memcpy(b + o, iov[i].iov_base, iov[i].iov_len);
        o += iov[i].iov_len;
    }
    *n = t;
    return b;
}

ssize_t writev(int fd, const struct iovec *iov, int cnt)
{
    if (!fd_tracked(fd) || cnt < 0)
        return real_writev(fd, iov, cnt);
    size_t n;
    void *b = gather(iov, cnt, &n);
    if (!b) {
        errno = ENOMEM;
        return -1;
    }
    ssize_t r = write(fd, b, n);
    int e = errno;
    free(b);
    errno = e;
    return r;
}

ssize_t pwritev(int fd, const struct iovec *iov, int cnt, off_t off)
{
    if (!fd_tracked(fd) || cnt < 0)
        return real_pwritev(fd, iov, cnt, off);
    size_t n;
    void *b = gather(iov, cnt, &n);
    if (!b) {
        errno = ENOMEM;
        return -1;
    }
    ssize_t r = pwrite(fd, b, n, off);
    int e = errno;
    free(b);
    errno = e;
    return r;
}

ssize_t pwritev64(int fd, const struct iovec *iov, int cnt, off_t off) __attribute__((alias("pwritev")));

ssize_t read(int fd, void *buf, size_t n)
{
    if (!md_mode || !fd_tracked(fd))
        return real_read(fd, buf, n);
    off_t off = lseek(fd, 0, SEEK_CUR);
    ssize_t r = real_read(fd, buf, n);
    pthread_mutex_lock(&lock);
    if (r > 0 && fd_is(fd) && urand() < md_rate)
        md_read(fd, buf, (size_t)r, off);
    pthread_mutex_unlock(&lock);
    return r;
}

ssize_t pread(int fd, void *buf, size_t n, off_t off)
{
    if (!md_mode || !fd_tracked(fd))
        return real_pread(fd, buf, n, off);
    ssize_t r = real_pread(fd, buf, n, off);
    pthread_mutex_lock(&lock);
    if (r > 0 && fd_is(fd) && urand() < md_rate)
        md_read(fd, buf, (size_t)r, off);
    pthread_mutex_unlock(&lock);
    return r;
}

ssize_t pread64(int fd, void *buf, size_t n, off_t off) __attribute__((alias("pread")));

/* misdirect mode, a part mapped for reading: sometimes a private copy with one chunk from elsewhere in the file.
 * Mappings of anything else, and every mapping before init loaded the real mmap, go straight through. */
void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t off)
{
    if (!real_mmap)
        return (void *)syscall(SYS_mmap, addr, len, prot, flags, fd, off);
    if (!md_mode || !fd_tracked(fd) || (prot & PROT_WRITE) || !(flags & MAP_SHARED) || off != 0 || addr ||
        len > ((size_t)1 << 28))
        return real_mmap(addr, len, prot, flags, fd, off);
    pthread_mutex_lock(&lock);
    bool fault = fd_is(fd) && md_part(fdpath[fd]) && urand() < md_map_rate;
    struct stat st;
    void *p = MAP_FAILED;
    if (fault && fstat(fd, &st) == 0 && (size_t)st.st_size == len && len >= 2)
        p = real_mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p != MAP_FAILED) {
        size_t got = 0;
        while (got < len) {
            ssize_t r = real_pread(fd, (char *)p + got, len - got, (off_t)got);
            if (r <= 0)
                break;
            got += (size_t)r;
        }
        off_t a, from;
        size_t c;
        md_chunk(0, len, &a, &c);
        from = (off_t)md_below(len - c + 1);
        if (urand() < 0.5)
            from = from / 512 * 512;
        if (got != len || from == a || real_pread(fd, (char *)p + a, c, from) != (ssize_t)c ||
            mprotect(p, len, prot) != 0) {
            munmap(p, len);
            p = MAP_FAILED;
        } else {
            md_note("map %s [%lld, +%zu) of %zu bytes <- from %lld", fdpath[fd], (long long)a, c, len,
                    (long long)from);
        }
    }
    pthread_mutex_unlock(&lock);
    return p != MAP_FAILED ? p : real_mmap(addr, len, prot, flags, fd, off);
}

void *mmap64(void *addr, size_t len, int prot, int flags, int fd, off_t off) __attribute__((alias("mmap")));

static int do_sync(int fd, bool data_only)
{
    if (!fd_tracked(fd) || md_mode)
        return data_only ? real_fdatasync(fd) : real_fsync(fd);
    pthread_mutex_lock(&lock);
    maybe_crash();
    if (eio_now(fd, data_only ? "fdatasync" : "fsync")) { /* the pages are dropped: nothing becomes durable */
        pthread_mutex_unlock(&lock);
        errno = EIO;
        return -1;
    }
    int r = data_only ? real_fdatasync(fd) : real_fsync(fd);
    if (r == 0) {
        struct stat st;
        if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
            if (lying_dir[0] && !strcmp(lying_dir, fdpath[fd])) {
                pthread_mutex_unlock(&lock);
                return 0;
            }
            /* the directory's entries are durable now */
            char now[PATH_MAX];
            for (size_t i = 0; i < ndops; i++)
                if (!dops[i].done && (op_now(i, dops[i].dir, now), !strcmp(now, fdpath[fd]))) {
                    dops[i].done = true;
                    if (dops[i].kind == OP_UNLINK && dops[i].saved[0]) {
                        /* the unlink is durable: the copy kept aside goes now, not at the power loss, so the directory
                         * can be emptied and removed in the process (test_snapshot: an install removes v<N>.old). Its
                         * inode may be freed and given to a new file: the record goes with its last name (a file
                         * still open, or linked elsewhere, gets a new record, durable as it is, at its next use) */
                        struct stat sst;
                        if (lstat(dops[i].saved, &sst) == 0 && S_ISREG(sst.st_mode) && sst.st_nlink == 1)
                            record_drop(sst.st_dev, sst.st_ino);
                        rm_tree(dops[i].saved);
                        dops[i].saved[0] = 0;
                    }
                    if (dops[i].kind == OP_RENAME && dops[i].a[0]) {
                        /* the old name is gone for good too, if it was in another dir */
                    }
                }
            for (size_t i = 0; i < nfiles; i++) { /* the transaction that carries a truncation commits with the directory's sync */
                char fdir[PATH_MAX];
                dir_of(files[i].path, fdir);
                if (files[i].used && files[i].tsave && !strcmp(fdir, fdpath[fd]))
                    tsave_drop(&files[i]);
            }
        } else {
            file *f = file_of_fd(fd);
            if (f && !f->lying) {
                f->durable = st.st_size;
                f->ndirty = 0;
                f->nold = 0;
                tsave_drop(f); /* the truncation and the new content are durable */
            }
        }
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int fsync(int fd) { return do_sync(fd, false); }
int fdatasync(int fd) { return do_sync(fd, true); }

int syncfs(int fd)
{
    if (!fd_tracked(fd) || md_mode)
        return real_syncfs(fd);
    pthread_mutex_lock(&lock);
    maybe_crash();
    if (eio_now(fd, "syncfs")) {
        pthread_mutex_unlock(&lock);
        errno = EIO;
        return -1;
    }
    int r = real_syncfs(fd);
    struct stat fs;
    if (r == 0 && fstat(fd, &fs) == 0) {
        for (size_t i = 0; i < nfiles; i++) {
            file *f = &files[i];
            struct stat st;
            if (f->used && !f->lying && stat(f->path, &st) == 0 && st.st_dev == fs.st_dev && st.st_ino == f->ino) {
                f->durable = st.st_size;
                f->ndirty = 0;
                f->nold = 0;
                tsave_drop(f);
            }
        }
        for (size_t i = 0; i < ndops; i++) {
            struct stat st;
            if (!dops[i].done && !(lying_dir[0] && !strcmp(lying_dir, dops[i].dir)) &&
                (stat(dops[i].dir, &st) != 0 || st.st_dev == fs.st_dev))
                dops[i].done = true;
        }
    }
    pthread_mutex_unlock(&lock);
    return r;
}

static int do_rename(int ofd, const char *oldp, int nfd, const char *newp, bool at)
{
    char a[PATH_MAX], b[PATH_MAX];
    path_of_at(at ? ofd : AT_FDCWD, oldp, a);
    path_of_at(at ? nfd : AT_FDCWD, newp, b);
    if (!tracked(a) && !tracked(b))
        return at ? real_renameat(ofd, oldp, nfd, newp) : real_rename(oldp, newp);
    pthread_mutex_lock(&lock);
    maybe_crash();
    char saved[PATH_MAX] = "";
    struct stat st;
    if (stat(b, &st) == 0 && S_ISREG(st.st_mode)) {
        /* replacing an existing file: keep the old one for an undo */
        char hard[PATH_MAX];
        char dir[PATH_MAX];
        dir_of(b, dir);
        snprintf(hard, PATH_MAX, "%s/.crashfs_saved_%d_%u", dir, (int)getpid(), saved_counter++);
        if (syscall(SYS_linkat, AT_FDCWD, b, AT_FDCWD, hard, 0) == 0) /* (not link(): that one is the shim's own, tracked) */
            snprintf(saved, PATH_MAX, "%s", hard);
    }
    int r = at ? real_renameat(ofd, oldp, nfd, newp) : real_rename(oldp, newp);
    if (r == 0) {
        add_dirop(OP_RENAME, a, b, saved[0] ? saved : NULL);
        file_get(b, false);
        if (stat(b, &st) == 0 && S_ISREG(st.st_mode)) { /* a descriptor still open on the file is the file under its new name */
            for (int fd = 0; fd < MAX_FDS; fd++)
                if (fdpath[fd] && !strcmp(fdpath[fd], a)) {
                    char *n = strdup(b);
                    if (n) {
                        free(fdpath[fd]);
                        fdpath[fd] = n;
                    }
                }
        }
        if (stat(b, &st) == 0 && S_ISDIR(st.st_mode)) { /* what is inside moved with it */
            for (size_t i = 0; i < nfiles; i++)
                rekey(files[i].path, a, b);
            for (int fd = 0; fd < MAX_FDS; fd++) {
                char p[PATH_MAX];
                if (!fdpath[fd])
                    continue;
                snprintf(p, sizeof p, "%s", fdpath[fd]);
                rekey(p, a, b);
                char *n = strcmp(p, fdpath[fd]) ? strdup(p) : NULL;
                if (n) {
                    free(fdpath[fd]);
                    fdpath[fd] = n;
                }
            }
        }
    } else if (saved[0]) {
        real_unlink(saved);
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int rename(const char *a, const char *b) { return do_rename(AT_FDCWD, a, AT_FDCWD, b, false); }
int renameat(int ofd, const char *a, int nfd, const char *b) { return do_rename(ofd, a, nfd, b, true); }

/* renameat2: the engine renames a retained WAL segment with RENAME_NOREPLACE (the change feed). Flags 0 and NOREPLACE
 * (1) are tracked as a rename, so that a power loss undoes one whose directory was not synced; the other flags
 * (exchange, whiteout) the engine never uses: they pass through untracked. */
int renameat2(int ofd, const char *a, int nfd, const char *b, unsigned flags)
{
    if (flags & ~1u) {
        errno = EINVAL;
        return -1;
    }
    if (flags & 1u) {
        char pb[PATH_MAX];
        path_of_at(nfd, b, pb);
        struct stat st;
        if (lstat(pb, &st) == 0) {
            errno = EEXIST;
            return -1;
        }
    }
    return do_rename(ofd, a, nfd, b, true);
}

static int do_unlink(int dirfd, const char *name, int flags, bool at)
{
    char p[PATH_MAX];
    path_of_at(at ? dirfd : AT_FDCWD, name, p);
    if (!tracked(p) || (flags & AT_REMOVEDIR))
        return at ? real_unlinkat(dirfd, name, flags) : real_unlink(name);
    pthread_mutex_lock(&lock);
    maybe_crash();
    char saved[PATH_MAX];
    int r;
    if (keep_aside(p, saved)) {
        add_dirop(OP_UNLINK, p, NULL, saved);
        r = 0;
    } else {
        r = at ? real_unlinkat(dirfd, name, flags) : real_unlink(name);
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int unlink(const char *name) { return do_unlink(AT_FDCWD, name, 0, false); }

/* link and linkat (round 16, VERIFY15): a new hard link is a new directory entry, durable only after an fsync of its directory,
 * like a created file (a leader links its COMMIT's pending parts to stage_ names and syncs the directory before the marker is
 * written: src/replica.c). A power loss undoes it (OP_CREATE: the new name is unlinked; the file stays under its other names and
 * keeps its data). Not tracked before: a link never synced always survived. The file's record keeps the name it had (the rollback
 * of its data finds it by that name or by the last one a rename gave it). */
static int do_link(int ofd, const char *a, int nfd, const char *b, int flags)
{
    char pb[PATH_MAX];
    path_of_at(nfd, b, pb);
    if (!tracked(pb))
        return (int)syscall(SYS_linkat, ofd, a, nfd, b, flags);
    pthread_mutex_lock(&lock);
    maybe_crash();
    int r = (int)syscall(SYS_linkat, ofd, a, nfd, b, flags);
    if (r == 0)
        add_dirop(OP_CREATE, pb, NULL, NULL);
    pthread_mutex_unlock(&lock);
    return r;
}

int link(const char *a, const char *b) { return do_link(AT_FDCWD, a, AT_FDCWD, b, 0); }
int linkat(int ofd, const char *a, int nfd, const char *b, int flags) { return do_link(ofd, a, nfd, b, flags); }
int unlinkat(int dirfd, const char *name, int flags) { return do_unlink(dirfd, name, flags, true); }

int mkdir(const char *p, mode_t mode)
{
    char a[PATH_MAX];
    path_of_at(AT_FDCWD, p, a);
    if (!tracked(a))
        return real_mkdir(p, mode);
    pthread_mutex_lock(&lock);
    maybe_crash();
    int r = real_mkdir(p, mode);
    if (r == 0)
        add_dirop(OP_MKDIR, a, NULL, NULL);
    pthread_mutex_unlock(&lock);
    return r;
}

int mkdirat(int dirfd, const char *p, mode_t mode)
{
    char a[PATH_MAX];
    path_of_at(dirfd, p, a);
    if (!tracked(a))
        return real_mkdirat(dirfd, p, mode);
    pthread_mutex_lock(&lock);
    maybe_crash();
    int r = real_mkdirat(dirfd, p, mode);
    if (r == 0)
        add_dirop(OP_MKDIR, a, NULL, NULL);
    pthread_mutex_unlock(&lock);
    return r;
}

/* A call that fails changed nothing on a real disk, so what the shim keeps for a power loss is committed only when the
 * call returned (round 16, VERIFY15: a failed ftruncate on an O_RDONLY descriptor zeroed durable bytes in the zero mode;
 * round 14 had the same for open(O_TRUNC)). The old sectors keep_old reads must be read before the call; they are
 * dropped again when it failed (they only ever make a region come back as it was, but a failed call has no region). */
int ftruncate(int fd, off_t len)
{
    if (!fd_tracked(fd) || md_mode)
        return real_ftruncate(fd, len);
    pthread_mutex_lock(&lock);
    maybe_crash();
    file *f = file_of_fd(fd);
    const bool cut = f && len >= 0 && len < f->durable; /* cutting durable data, unsynced */
    const off_t durable = f ? f->durable : 0;
    const size_t nold = f ? f->nold : 0;
    if (cut)
        keep_old(f, fd, len, durable);
    int r = real_ftruncate(fd, len);
    if (cut && r != 0) {
        f->nold = nold;
    } else if (cut && rewrite_zero) {
        dirty_add(f, len, durable - len);
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int ftruncate64(int fd, off_t len) __attribute__((alias("ftruncate")));

/* a duplicate of a tracked descriptor is the same open file: its fsync makes the file durable (the Raft log hands
 * duplicates of its segments to a worker thread to sync) */
static void dup_note(int from, int to)
{
    if (to < 0 || to >= MAX_FDS || from < 0 || from >= MAX_FDS || to == from)
        return;
    pthread_mutex_lock(&lock);
    free(fdpath[to]);
    fdpath[to] = fdpath[from] ? strdup(fdpath[from]) : NULL;
    fddev[to] = fddev[from];
    fdino[to] = fdino[from];
    pthread_mutex_unlock(&lock);
}

int dup(int fd)
{
    int r = real_dup(fd);
    dup_note(fd, r);
    return r;
}

int dup2(int fd, int to)
{
    int r = real_dup2(fd, to);
    dup_note(fd, r);
    return r;
}

int dup3(int fd, int to, int flags)
{
    int r = real_dup3(fd, to, flags);
    dup_note(fd, r);
    return r;
}

int fcntl(int fd, int cmd, ...)
{
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *); /* every command takes at most one int, long or pointer */
    va_end(ap);
    int r = real_fcntl(fd, cmd, arg);
    if (r >= 0 && (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC))
        dup_note(fd, r);
    return r;
}

/* The close of a tracked descriptor of a regular file with no name left frees its inode, unless another tracked descriptor
 * holds it: the record a write through it re-made after a durable unlink (file_of_fd) goes with it. */
int close(int fd)
{
    if (fd >= 0 && fd < MAX_FDS && fdpath[fd]) {
        struct stat st;
        bool gone = fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_nlink == 0;
        pthread_mutex_lock(&lock);
        free(fdpath[fd]);
        fdpath[fd] = NULL;
        for (int k = 0; gone && k < MAX_FDS; k++)
            if (fdpath[k] && fddev[k] == st.st_dev && fdino[k] == st.st_ino)
                gone = false;
        if (gone)
            record_drop(st.st_dev, st.st_ino);
        pthread_mutex_unlock(&lock);
    }
    return real_close(fd);
}
