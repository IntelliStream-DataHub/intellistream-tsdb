# tsdb on macOS

macOS (Apple silicon and Intel) is a **developer platform**. tsdb builds there, `make test` passes, and `tsdbd`, the
`tsdb` CLI, the C client, the other clients and the embedded library all work, so you can run a local instance and
write code against it. It is not for production data and not for measuring speed: production runs on Linux, and
every benchmark in this repository is a Linux number.

## Build and test

```sh
xcode-select --install      # cc, make and git, if you do not have them yet
brew install zstd           # the only library tsdb needs
make                        # into build/
make test IO=epoll          # unit, integration and server tests (about 10 minutes)
make test-clients           # Rust, Python and Java clients (needs cargo, python3 and mvn)
```

The Makefile sees Darwin by itself. It then builds with `-mcpu=native` on Apple silicon, takes zstd from
`brew --prefix zstd`, makes `libtsdb.dylib` instead of `libtsdb.so`, and builds `tsdbd` without io_uring (as if you
had typed `NO_URING=1`). zstd somewhere else: `make ZSTD_LIB="-L/path/lib -lzstd" CFLAGS="-O3 -g -I/path/include"`.

The shell's default limit of open files is 256. Raise it before you run a server with many connections, or a program
with dozens of volumes: `ulimit -n 10240` (`tsdbd` raises its own limit as far as the kernel lets it).

## Run a server

`tsdbd` asks for a login by default, on macOS as on Linux, and does not start until the database has a superuser:

```sh
build/tsdb /tmp/db user add admin --superuser --password-file pw.txt
build/tsdbd --dir /tmp/db --port 7070
```

For a quick local test without users: `build/tsdbd --dir /tmp/db --no-auth` (anyone who can reach the port can
read and write everything, so keep it on 127.0.0.1).

## What differs from Linux

| | Linux | macOS |
|---|---|---|
| `tsdbd` event loop | io_uring or epoll | epoll emulated over kqueue (`server/epoll_compat.h`); only `--io epoll` |
| `tsdbd` wake-ups, timer | eventfd, timerfd | a pipe, a kqueue timer |
| aggregation kernels | AVX-512 / AVX2 / SSE, chosen at startup | Apple silicon: scalar; Intel Macs: as Linux (untried) |
| `make dist` CPU levels | yes | no (one build for the machine) |
| bytes on disk | | the same: a database moves between the two |
| durability | `fsync`, `fdatasync` | the same, then `fcntl(F_FULLFSYNC)` (see below) |
| thread stacks | 8 MB | set to 8 MB (`tsdb_thread_create`); macOS gives 512 KB |
| `make jepsen`, `make dist*`, crashfs | yes | no: they need `LD_PRELOAD`, `/proc`, glibc or Linux containers |
| `make asan`, `make tsan` | yes | no: see "Sanitizers" |

**Durability.** On macOS a plain `fsync` hands the data to the drive but does not empty the drive's own cache, so a
power cut could still lose an acknowledged write. tsdb therefore follows every `fsync` and `fdatasync` with
`fcntl(F_FULLFSYNC)`, which waits for the drive. That is much slower (about 3.5 ms a call on the laptop this was
ported on, against 0.03 ms for the plain call). For throwaway data, `TSDB_FAST_FSYNC=1` in the environment skips it.

Smaller differences, all in `src/compat.h` with the reason for each:

- `posix_fadvise` does nothing. tsdb uses it to drop a file's pages from the cache before it reads a damaged-looking
  tail a second time; on macOS that second read may come from the cache.
- `sync_file_range` does nothing. It only starts the writeback early; the `fdatasync` after it still makes the data
  durable.
- `tsdb import` with `fsync = 0` has no `syncfs` on macOS. It calls `sync()` (which starts writing every file system's
  dirty data without waiting for it) and then syncs the directories, so the import is durable only as far as that got.
- The warning that ext4 and XFS keep no checksums of the data is never printed: macOS file systems have other names.

All of these are compiled only on macOS (`#ifdef __APPLE__`, or a `tsdb_*` wrapper that is the plain call on Linux).
The Linux build is the same machine code as without the port.

## Tests skipped on macOS

Each prints a line saying so when it runs.

- The power-loss and fail-stop runs under the `crashfs` shim (`test_server`, `test_export`, `test_users`,
  `test_volumes`): they need `LD_PRELOAD`, which works differently on macOS. Run `make jepsen` on Linux.
- The five volume tests that simulate a hung file system with a FIFO. On macOS they did not fail, they **hung**: the
  add did not block where Linux's does, and a later step then waited on a FIFO forever. Whether tsdb copes with a
  hung volume on macOS is therefore not known; do not rely on it there.
- `test_ports` orphan and launcher_race. `tsdbd --exit-with-parent` works on macOS (it checks once a second whether
  its parent is gone), but these two tests need a subreaper (`PR_SET_CHILD_SUBREAPER`), which macOS lacks.
- `test_spread` on Apple silicon: its reference arithmetic needs the 64-bit mantissa of x86's `long double`.
- `test_connect_timeout` (a full accept queue answers differently) and the io_uring fallback of `test_stability`.
- The metrics test that a client that does not read loses part of its reply: macOS loopback buffers hold all of it.

## Sanitizers

UBSan works (`-fsanitize=undefined`). The ASan and TSan runtimes did not start on the macOS 26 machine this was
ported on, so the memory and thread checks (`make asan`, `make tsan`) run on Linux only.

## Known limits

- `/tmp` is a link to `/private/tmp`. tsdb stores volume paths in their canonical form, so a volume added as
  `/tmp/x` is listed (and exported) as `/private/tmp/x`.
- `make test-clients` passes on macOS (Rust, Python, the Java TCP and embedded clients, the Python sdist). The
  embedded Rust and Python builds hide the engine's internal symbols with `ld -r -exported_symbols_list`
  (`client/rust-sys/build.rs`); the Java embedded jar carries `libtsdb.dylib` (Maven profiles `macos-aarch64` and
  `macos-x86_64`). The Pulsar connector and `make integration-tests` were not run on macOS.
- `test_multi` failed about 1 run in 12 on macOS (the budget check for series 0): a helper thread can claim series 0
  ahead of the head. The race exists on Linux too but is rarer there.
- VOLUMES and USERS are locked with open-file-description locks (`F_OFD_SETLKW`), which recent macOS SDKs define.
  If yours does not, the build stops in `src/volumes.c`; report it ([docs/tasks/macos.md](tasks/macos.md)).
