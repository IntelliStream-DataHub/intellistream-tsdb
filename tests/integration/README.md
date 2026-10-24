# tests/integration

`make integration-tests` runs tsdb end to end the way a user would: real `tsdbd` processes built from this tree,
the `tsdb` CLI, and the C, Rust, Python and Java clients, on both server backends (`--io uring` and `--io epoll`;
epoll alone where io_uring is unavailable). About two minutes. It needs what `make test-clients` needs
(cargo, python3, mvn) and builds its programs into `build/integ/`.

    make integration-tests
    make integration-tests INTEG_ARGS="--only matrix,follower --io epoll --seed 7"
    make integration-tests-dist          # the same against the binaries of `make dist`
    python3 tests/integration/mutate.py  # plants known bugs in a copy of the tree: every one must be caught

| file | what |
|---|---|
| `run.py` | the driver: starts and stops processes, runs the scenarios, compares, writes `build/integ/logs/<time>/` |
| `gen.py` | test data (write scripts, queries) and an independent model of every answer (zoneinfo for calendars) |
| `c/itc.c`, `rust/`, `python/itp.py`, `java/Itj.java` | the same small program in each client language: `write`, `read`, `load`, `admin` |
| `mutate.py` | the harness's own test: planted bugs (see the list with `--list`) |

Scenarios (see the docstring of `run.py` for the details): `matrix` (each client writes a set, every client reads
every set; answers identical to each other, to the model and to `tsdb query` on the stopped directory), `cli`
(`tsdb create/import`, read back by the clients), `follower` (`tsdbd --follow` catches up with two waves),
`durability` (SIGKILL in the middle of a pipelined load from each client).

Logs: one directory per scenario and backend with the write scripts, every client's output, the server logs and
`notes.txt`; `summary.txt` at the top. The data directories are removed unless `--keep` or the scenario failed.

What is compared and what is not: bytes, with these exceptions that the server cannot promise. The payload of a NaN
sum depends on which NaN was added first, so it is compared as "NaN". Sums of float series are not compared between
the memtable, flushed and merged states of the same rows (a different order of additions rounds differently); the
model checks them to 1e-9 of the sum of magnitudes, and exactly where NaN or infinity is involved.
