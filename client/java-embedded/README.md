# tsdb-embedded: tsdb inside the JVM (Java 22+)

`EmbeddedDb` runs the tsdb engine in your Java process. There is no tsdbd server and no network: calls go
straight into `libtsdb.so` through the Java FFM API (`java.lang.foreign`, final since Java 22).

The methods and types are the same as the TCP client's (`TsdbClient`, in `client/java`). To move code from a
server to an embedded database, change how you open it. Everything else stays: `Bucket`, `Column`, `Group`,
`Points`, `ValuePoints`, `TsdbException` and its status codes.

## Quick start

```sh
make build/libtsdb.so                                   # the engine as a shared library
cd client/java && mvn -q install -DskipTests            # the shared types (Java 17)
cd ../java-embedded && mvn -q package                   # target/tsdb-embedded-*.jar, libtsdb.so inside
```

```java
import ai.intellistream.tsdb.*;
import ai.intellistream.tsdb.embedded.*;

try (EmbeddedDb db = EmbeddedDb.open("/var/lib/tsdb")) {
    int id = db.series("boiler.temp");
    db.insert(id, new long[] {t0, t0 + 60_000}, new float[] {61.5f, 61.8f});

    Points raw = db.queryPoints(id, t0, t1);
    List<Bucket> hourly = db.queryAgg(id, t0, t1, 3_600_000);
    List<Bucket> daily = db.queryGroup(id, t0, t1, Group.of(Unit.DAY).in("Europe/Oslo"));
}
```

Run it with native access allowed:

```sh
java --enable-native-access=ALL-UNNAMED -cp tsdb-embedded.jar:lib/tsdb-client.jar MyApp
```

- On the class path: `--enable-native-access=ALL-UNNAMED`.
- On the module path: `--enable-native-access=ai.intellistream.tsdb.embedded`.
- In your own executable jar (`java -jar app.jar`): the line `Enable-Native-Access: ALL-UNNAMED` in that jar's
  manifest. The JVM reads it only from the jar it starts, never from a library jar such as tsdb-embedded.jar.

Without the flag, the JVM prints a warning on the first call, and future JDKs will refuse the call. With the flag
it prints nothing.

## Where libtsdb.so comes from

The library is looked up in this order. The first match wins:

1. The system property `-Dtsdb.library=/path/libtsdb.so`, or the environment variable `TSDB_LIBRARY`. You asked
   for it explicitly, so this comes first.
2. The copy bundled in the jar under `ai/intellistream/tsdb/embedded/native/linux-x86_64/` (or `linux-aarch64`, the
   machine the jar was built on). It is extracted to a temporary file in `java.io.tmpdir`, loaded, and deleted at
   once, so nothing is left behind even if the JVM crashes. If that directory is mounted `noexec`, the load fails:
   point `-Djava.io.tmpdir` elsewhere, or use `-Dtsdb.library`.
3. `libtsdb.so` in a directory of `java.library.path`. It comes after the bundled copy because on Linux that path
   includes /usr/lib and /lib, where an older installed libtsdb.so would otherwise win unnoticed.
4. The dynamic linker's own search (`LD_LIBRARY_PATH`, the ld.so cache).

Whichever it is, the binding asks the library for the size of every public struct (`tsdb_struct_size`) when it
loads it and refuses one built from other headers with an `UnsatisfiedLinkError`, instead of corrupting memory.
(The spread and TWA structs have no such size yet; the tests check them against the headers, and a library
without their functions fails to load.)
The library needs `libzstd.so.1` at run time (`apt install libzstd1`, `dnf install libzstd`).

`EmbeddedDb.librarySource()` tells you which one was used. The bundled copy is whatever `build/libtsdb.so` was
when the jar was built (`-Dtsdb.native.dir=...` picks another directory). A plain `make` builds for the build
machine's CPU (`-march=native`). For a jar that runs on other machines, build the library with
`make PORTABLE=1` or `make dist` first.

## How it behaves

- **Threads.** Every method is thread safe, and one `EmbeddedDb` is meant to be shared by all threads, like the C
  handle. `close()` waits for the calls already running, and later calls fail with `ERR_IO`. Calling `close()`
  from inside a query's consumer throws `IllegalStateException` instead of deadlocking.
- **One writer per directory.** A second open of a directory that is open for writing fails with `ERR_LOCKED`,
  whether it comes from this process or another one. `new EmbeddedOptions().readOnly(true)` opens the directory
  beside a writer, for example beside a running tsdbd.
- **Inserts.** An insert returns once its rows are durable and visible, as the options say (`fsync`,
  `walSyncMs`). `insertAsync` does the same thing, and `await()` returns at once, so code written for the TCP
  client's pipelining keeps working.
- **Streaming.** `queryRaw` and `queryValues` with a consumer get chunks of up to 16384 rows (`EmbeddedDb.CHUNK`),
  as over TCP. The arrays are reused from one chunk to the next. The consumer runs on the calling thread. If it
  returns `false`, the query stops at once. If it throws, the query stops and the exception is rethrown by the
  query method; it never unwinds into C.
- **Time zones.** UTC grouping runs in the engine. Grouping in any other zone is computed in Java with
  `Group.bounds`, exactly as the TCP client does it, and the result is the same as the engine's own
  `tsdb_query_group_local` (the tests compare the two).
- **Query statistics.** `lastQueryStats()` describes the calling thread's last query, and that includes a
  virtual thread's own.
- **Memory.** Arguments are copied into native memory for the length of one call. Inserts copy into blocks the
  database keeps for reuse, one per thread inserting at once and each up to 32 MiB, because allocating and freeing
  the copy of a million rows on every call cost more than the copying. The blocks of one `EmbeddedDb` stay below
  `EmbeddedOptions.scratchBytes` (256 MiB by default; 0 keeps none) and are freed by `close()`. A call that needs
  more than 32 MiB, or arrives when the limit is used up, gets native memory of its own that is freed when it
  returns, as every call did before. Arrays the engine returns are copied into Java objects and freed with
  `tsdb_free` at once.
- **The same queries as over TCP.** `querySpread*`, `queryTwa*`, `queryLttb`, `queryAgg` with an order and a
  limit, `deleteRange`, `replaceRange`, `setRetention` and `retention` take the TCP client's arguments and return
  its types (`SpreadBucket`, `TwaBucket`, `TwaOptions`, `ValuePoints`); `TcpCompareTest` checks that both give
  the same answers. Spread and TWA groups in a time zone get their bounds from `Group.bounds` too, as the TCP
  client does: the engine's calendar reads the system's tzdata and does not know every zone id java.time takes
  (`UTC+2`, `GMT-05:30`). `queryLttb` checks the threshold before the series, and an unknown series gives no
  points, as tsdbd does. A range with more local periods than the calendar computes fails with
  `ERR_TOO_MANY_BUCKETS`, as in every client.
- **Embedded only.** `append`, `dropBefore`, `refresh`, `failed()`, `parts()`, `seriesPath`, `volumeCount`,
  `volumePath`, `simdLevel()` and every field of `EmbeddedOptions` (`tsdb_options`).

## Tests and benchmarks

```sh
make test-java-embedded          # or: cd client/java-embedded && mvn test
mvn test -Dtsdb.library=/path/to/other/libtsdb.so    # e.g. a sanitizer build
```

The tests do four things:

- check every struct layout against the C compiler (`src/test/c/layouts.c`);
- cover all value types, nulls, special floats, corrections, zones, bounds, limits and orders;
- check many threads on one database, exceptions thrown in consumers, closing under load, and that every array
  the engine returned was freed;
- write a database embedded, serve it with `build/tsdbd`, and check that the TCP client gets the same answers.

`EmbeddedBench` is tsdb-bench in Java, on the rows that `tsdb-bench --export` writes. With `--load FILE` and
`--qps N` it does exactly what `tsdb-bench --load` and `--qps` do (tools/cbench.h: the same series names, slices
of the file per thread, window classes, generator and seeds), and `bench/clients/run.sh` runs it as the
"embedded Java" rows next to embedded C and the TCP clients. Without them it runs tsdb-bench's default query
suite:

```sh
build/tsdb-bench --export /data/rows.rowbinary --series 100 --points 1000000
java --enable-native-access=ALL-UNNAMED \
  -cp target/tsdb-embedded-0.9.0.jar:target/lib/tsdb-client-0.9.0.jar \
  ai.intellistream.tsdb.embedded.bench.EmbeddedBench --dir /data/emb --file /data/rows.rowbinary \
  --series 100 --points 1000000
```

In that default run, loading is not tsdb-bench's insert: tsdb-bench gives each writer thread its own series and generates the rows,
while EmbeddedBench reads the exported file in batches of `--batch` rows over all series, batch `b` to thread
`b % threads`. Compare its insert rate with a C program that loads the same file in the same batches, not with
tsdb-bench's `insert+generate` line. The query lines are comparable one for one.
