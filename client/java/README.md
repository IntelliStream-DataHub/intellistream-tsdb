# tsdb Java client

A Java client for `tsdbd`, the TCP server of the tsdb time series database. It speaks the protocol of
[`include/tsdb_protocol.h`](../../include/tsdb_protocol.h), mirrors the C client in [`client/`](..), and has no
runtime dependencies. Built for Java 17 and later.

## Quick start

Start a server (from the repository root, after `make`):

```sh
./build/tsdbd --dir /tmp/tsdb-demo --port 7070
```

Then connect, create a series, insert a batch, and query it raw, aggregated and grouped by local day:

```java
package ai.intellistream.tsdb.example;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.Unit;

import java.time.Instant;

/** Quick start: {@code java -cp tsdb-client.jar ai.intellistream.tsdb.example.Example [host] [port]}. */
public final class Example {
    private Example() {
    }

    /**
     * Runs the example.
     *
     * @param args the host and port of a running tsdbd; default 127.0.0.1 7070
     * @throws Exception if the server cannot be reached or a request fails
     */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : TsdbClient.DEFAULT_PORT;

        try (TsdbClient client = TsdbClient.connect(host, port)) {
            int pump = client.series("pump7.flow_m3h"); // get or create

            // One reading per minute for a day, from 2024-03-01T00:00Z, as a single batch.
            long t0 = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            int n = 24 * 60;
            long[] ts = new long[n];
            float[] values = new float[n];
            for (int i = 0; i < n; i++) {
                ts[i] = t0 + i * 60_000L;
                values[i] = 40 + (i % 60) / 10f;
            }
            client.insert(pump, ts, values);

            // Raw points: the three newest.
            Points newest = client.queryPoints(pump, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 3);
            for (int i = 0; i < newest.size(); i++)
                System.out.printf("%s %.1f%n", Instant.ofEpochMilli(newest.timestamp(i)), newest.value(i));

            // Aggregates over fixed 6-hour intervals.
            for (Bucket b : client.queryAgg(pump, t0, t0 + 86_400_000L, 6 * 3_600_000L))
                System.out.printf("%s count %d avg %.2f min %.1f max %.1f%n", Instant.ofEpochMilli(b.start()),
                        b.count(), b.avg(), b.min(), b.max());

            // Calendar days in Oslo (UTC+1 in March): the UTC day spans two local days.
            Group osloDays = Group.of(Unit.DAY).in("Europe/Oslo");
            for (Bucket b : client.queryGroup(pump, Long.MIN_VALUE, Long.MAX_VALUE, osloDays))
                System.out.printf("Oslo day from %s: %d points%n", Instant.ofEpochMilli(b.start()), b.count());
        }
    }
}
```

This is `src/main/java/ai/intellistream/tsdb/example/Example.java`, verbatim; the tests check that the two match
and run it against a live server. Run it with:

```sh
mvn -q package -DskipTests
java -cp target/tsdb-client-0.1.0-SNAPSHOT.jar ai.intellistream.tsdb.example.Example 127.0.0.1 7070
```

## Build and test

Requirements: JDK 17 or later, Maven 3.6 or later, and the server built in the repository root.

```sh
make -C ../..            # builds build/tsdbd, which the tests start
mvn package              # compile, run the tests, build target/tsdb-client-0.1.0-SNAPSHOT.jar
mvn test                 # the tests only
mvn package -DskipTests  # the jar only
mvn install              # into ~/.m2, for use as ai.intellistream:tsdb-client:0.1.0-SNAPSHOT
```

The tests are integration tests. Each test class starts `../../build/tsdbd` on a free port of 127.0.0.1 with a
fresh database directory, stops it with SIGTERM, checks that it exits cleanly and deletes the directory. Two
properties change where things are:

- `-Dtsdbd=/path/to/tsdbd`: another server binary, such as a sanitizer build;
- `-Dtsdb.test.dir=/path`: where the database directories and server logs go (default: `java.io.tmpdir`).

## Usage

```xml
<dependency>
  <groupId>ai.intellistream</groupId>
  <artifactId>tsdb-client</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

| Method | Does |
| --- | --- |
| `TsdbClient.connect(host, port[, timeout])` | opens a connection (`close()` closes it) |
| `series(name[, create])`, `seriesAt(name, path)` | id of a series, created if missing (`seriesAt`: in a volume on the server) |
| `seriesName(id)` | name of a series |
| `insert(series, ts, values[, off, len])` | inserts into one series, waits until durable and visible |
| `insert(int[] series, ts, values[, off, len])` | inserts rows of several series, one id per row |
| `insertAsync(...)`, `await()`, `pending()` | pipelined inserts: send now, collect acknowledgements later |
| `queryPoints(series, start, end[, order, limit])` | raw points as `long[]` and `float[]` |
| `queryRaw(series, start, end[, order, limit], consumer)` | raw points streamed in chunks, for large results |
| `queryAgg(series, start, end, intervalMs)` | count/sum/min/max per fixed interval, as `Bucket` records |
| `queryGroup(series, start, end, group[, order, limit])` | the same per calendar unit, in a time zone |
| `commit()`, `flush()`, `optimize()`, `stats()` | maintenance and storage statistics |
| `hello()` | the server's protocol version |

Points are timestamps in milliseconds since the Unix epoch and float32 values. Writing an existing (series,
timestamp) replaces its value: the last write wins. Time ranges are half open, `[start, end)`; pass
`Long.MIN_VALUE` and `Long.MAX_VALUE` for everything.

**Streaming.** `queryRaw` hands the consumer chunks of up to 16384 points in arrays it reuses, so memory stays
bounded. Return `false` to stop; the protocol cannot cancel a query, so the client still reads (and discards) the
rest of the stream. Use a limit to bound the work instead.

```java
client.queryRaw(id, start, end, Order.ASC, 0, (ts, values, n) -> {
    for (int i = 0; i < n; i++)
        sink.write(ts[i], values[i]);
    return true;
});
```

**Pipelining.** `insertAsync` sends without waiting; `await()` collects the acknowledgements and throws the first
error. The server runs one connection's requests in order, so pipelined writes keep last-write-wins. Every other
call collects outstanding acknowledgements first, and `close()` does too, because the server drops the queued
requests of a connection that closes. At most `TsdbClient.MAX_IN_FLIGHT` (4096) insert frames are outstanding;
past that, `insertAsync` reads the oldest acknowledgement before sending.

**Errors.** Everything that fails throws `TsdbException`, whose `status()` is one of the `Status.ERR_*` codes of
`include/tsdb.h` (`Status.message(code)` describes it). An error status from the server leaves the connection
usable; a connection failure or a malformed reply closes it (`isOpen()` turns false). Arguments the protocol cannot
carry, such as arrays of different lengths, throw `IllegalArgumentException` without contacting the server.

**Threads.** A `TsdbClient` is one connection and is not thread safe: use one client per thread. The server runs
the requests of different connections in parallel.

**Inserts.** A batch is applied all or nothing, except that batches over 8M rows go out as several frames (as
in the C client), each applied on its own. Columns are copied into a 1 MiB direct buffer in bulk, so even large
batches cost no per-row work in Java and no extra allocation.
