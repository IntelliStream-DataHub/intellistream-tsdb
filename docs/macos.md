# tsdb on macOS

macOS (Apple silicon, and Intel) is a **developer platform**: it builds, `make test` passes, and `tsdbd`, `tsdb`, the C
client and the embedded library work, so you can run a local instance and develop against it. It is not meant for
production data or for measuring speed. Production runs on Linux.

## Build

```sh
brew install zstd           # the only dependency; Xcode's command line tools give cc and make
make                        # into build/
make test IO=epoll          # unit, integration and server tests (about 10 minutes)
```

The Makefile detects Darwin by itself: `-mcpu=native`, zstd from `brew --prefix zstd`, `libtsdb.dylib`, and no
io_uring or liburing (`NO_URING=1` is implied). Any other prefix: `make ZSTD_LIB="-L/path -lzstd" CFLAGS="-O3 -g -I/path/include"`.

Raise the open-file limit before you run a server with many connections or an embedded process with dozens of volumes:
the shell's default is 256 (`ulimit -n 10240`; `tsdbd` raises its own limit as far as the kernel lets it).

## What differs from Linux

| | Linux | macOS |
|---|---|---|
| `tsdbd` event loop | io_uring or epoll | epoll emulated over kqueue (`server/epoll_compat.h`), one backend, `--io epoll` |
| speed | SIMD aggregation kernels, CPU levels | the scalar kernels; codecs unchanged (same bytes on disk) |
| durability | `fdatasync` | `fsync` followed by `fcntl(F_FULLFSYNC)`, the drive's own cache flush, so an acknowledged write survives a power loss as on Linux; each sync costs about 3.5 ms instead of 0.03 ms on this laptop (measured). `TSDB_FAST_FSYNC=1` in the environment gives it up for throwaway data |
| threads | 8 MB stacks | made 8 MB too (`tsdb_thread_create`), macOS gives 512 KB by default |
| `make jepsen`, `make dist*`, crashfs | yes | no: they need `LD_PRELOAD`, `/proc`, glibc, io_uring or Linux containers |
| sanitizers | `make asan`, `make tsan` | UBSan works (`-fsanitize=undefined`); the ASan and TSan runtimes did not start on the macOS 26 machine this was ported on, so memory and thread checks are for Linux |

The shims live in `src/compat.h` (library), `server/epoll_compat.h` (server) and `tests/portable.h` (tests); each is
empty on Linux, so the Linux build is the code as it was.

## Tests that are skipped on macOS (they say so when they run)

- fail-stop and power-loss runs under the `crashfs` shim (`test_server`, `test_export`, `test_volumes`):
  `LD_PRELOAD` interposition does not work on macOS binaries the same way. Run `make jepsen` on Linux.
- the volume tests that simulate a hung file system with a FIFO. They did not fail on macOS, they **hung**: the add
  did not block where Linux's does, and a later step then waited for a FIFO that had become `META`. Whether the engine's
  handling of a hung volume holds on macOS is therefore unverified; do not rely on it there.
- `test_spread`: its reference arithmetic needs the 64-bit mantissa of x86's `long double` (arm64 has a plain double).
- `test_connect_timeout` and the io_uring fallback of `test_stability` (no full accept queue behaviour / no io_uring).
- the metrics test that a client that does not read loses part of the reply (macOS loopback buffers hold it all).

## Known limits

- macOS has no parent-death signal: if a test is killed, the `tsdbd` it started may be left running (`pgrep tsdbd`).
- `/tmp` is a link to `/private/tmp`; volume paths are stored canonically, so a dump made from `/tmp/x` names
  `/private/tmp/x`.
- `make test-clients` passes on macOS (Rust, Python, the Java TCP and embedded clients, the Python sdist). The embedded
  Rust and Python builds hide the engine's internal symbols with `ld -r -exported_symbols_list`
  (`client/rust-sys/build.rs`), and the Java embedded jar bundles `libtsdb.dylib` (profiles `macos-aarch64`,
  `macos-x86_64`). The Pulsar connector and `make integration-tests` were not run.
- `test_multi` fails about 1 run in 12 (`test_multi.c:808`, measured on a quiet machine). A helper thread can claim
  series 0 ahead of the head, which charges the budget the test says stays at zero; the race exists on Linux but is
  rarer there.
