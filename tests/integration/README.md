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
| `selftest.py` | tests of the harness itself (`make integration-selftest`, seconds): a broken io_uring backend fails the run, signal handling, the verdicts of `mutate.py` |
| `mutate.py` | the harness's own test: planted bugs (see the list with `--list`) |

Scenarios (see the docstring of `run.py` for the details): `matrix` (each client writes a set, every client reads
every set; answers identical to each other, to the model and to `tsdb query` on the stopped directory), `cli`
(`tsdb create/import`, read back by the clients), `follower` (`tsdbd --follow` catches up with two waves),
`stale_wal` (a WAL segment left behind by a flush is put back after newer parts exist and must not be replayed),
`durability` (SIGKILL in the middle of a pipelined load from each client; 8 batches are sent, then all 8 are waited
for, so acknowledgements come in groups of 8 and 0 to 7 batches are unacknowledged at the kill).

Logs: one directory per scenario and backend with the write scripts, every client's output, the server logs and
`notes.txt`; `summary.txt` at the top. The data directories are removed unless `--keep` or the scenario failed.

What is compared and what is not: bytes, with these exceptions that the server cannot promise. The payload of a NaN
sum depends on which NaN was added first, so it is compared as "NaN". Sums of float series are not compared between
the memtable, flushed and merged states of the same rows (a different order of additions rounds differently); the
model checks them to 1e-9 of the sum of magnitudes, and exactly where NaN or infinity is involved.

Limits, so that a green run is not read as more than it is:

- Float32 NaNs are quiet NaNs only. The Python client hands float32 values to Python as `f64::from(f32)` and narrows
  Python floats with `as f32`; on x86-64 both quiet a signalling NaN (`0x7f800001` reads back as `0x7fc00001`), so
  Python can neither carry nor write one and the data set leaves them out (float64 does include a signalling NaN).
- Durability kills with SIGKILL at moments chosen by client acknowledgements. A WAL segment that survives its flush
  exists only between the part publish and the unlink of the segment (`memtable.c`), and no such kill lands there, so
  the `stale_wal` scenario makes that state by hand (segments copied away while live and put back after the flush).
  Power loss and exact crash points are `make jepsen`.
- Error replies are compared only as "an error", not by code, between the clients.
- The suite is a driver and is not wired into the sanitizers. A one-off run of the full suite against ASan+UBSan
  builds of `tsdbd`, `tsdb` and `itc` (epoll) reported nothing; Rust, Python and Java clients were not instrumented.
- Zones with a midnight clock change (America/Havana, Asia/Beirut) are in the matrix, but sub-day units in a zone and
  counts above 1 are compared between the clients and with `tsdb query` only, not with the model.
