# intellistream-tsdb-sys

Raw FFI declarations of tsdb's public C API (`include/tsdb.h` and
`include/tsdb_calendar.h`), and the engine itself: by default `build.rs`
compiles the C sources of this repository with the `cc` crate, so a Rust
program that uses it needs no `libtsdb.so`, no tsdbd and no C toolchain
beyond the one `cc` finds.

Use the safe API instead: `tsdb_client::Db` (crate `intellistream-tsdb-client`, feature
`embedded`). This crate is for code that wants the C functions directly.
The package is called `intellistream-tsdb-sys`; the library inside it is
still `tsdb_sys`, so you write `use tsdb_sys::...`.

## Build

| feature        | effect |
|----------------|--------|
| `bundled-zstd` (default) | libzstd is compiled from source by `zstd-sys` |
| `system`       | link a prebuilt `libtsdb.so` (`make` in the repository root); `TSDB_LIB_DIR` names its directory, default `../../build` |
| `native`       | compile the engine with `-march=native` |

The default build is portable: the engine is compiled for the target's
baseline, like `make PORTABLE=1`, and on x86-64 the codec files are compiled
twice more for x86-64-v3 and v4 (as in `make dist`, see `src/levels.h`); the
engine picks the copy the CPU supports at startup, and the aggregation
kernels pick AVX-512, AVX2 or scalar code at runtime either way. The flags are
the Makefile's: `-std=gnu11 -O3 -fno-math-errno -ffp-contract=off`, so parts
written by a Rust program are byte for byte those `make` would write.

`TSDB_SRC_DIR` points the build at another checkout of the repository. Outside
the repository (a package of this crate, such as the Python sdist) the build
uses `csrc/`, the copy of the C sources that `vendor.sh` makes; `make
python-sdist` runs it.

On ELF targets the engine's objects are linked into one with `cc -r` and
`objcopy` keeps only the `tsdb_*` symbols global, as `tools/libtsdb.map` does
for `libtsdb.so`: the engine's internal names (`pool_new`, `wal_open`, ...)
cannot clash with another library's in the program that links it
(`tests/symbols.rs`). `OBJCOPY` names the tool if it is not `objcopy` or
`llvm-objcopy`.

## Why hand-written

The API is about fifty functions and ten structs, and it changes rarely.
Hand-written declarations need no libclang at build time and read like the
header; `tests/layout.rs` compares the size and every field offset of each
struct with what the C compiler says (`layout.c`), so a header change that
the declarations miss fails the tests instead of corrupting memory.
