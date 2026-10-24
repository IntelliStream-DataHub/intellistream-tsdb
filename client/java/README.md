# tsdb Java client

This is a Java library for talking to `tsdbd`, the server of the tsdb time series database. A time series is a
list of measurements over time, such as the flow of a pump once a minute. You can store measurements, read them
back, and ask for totals per hour, per day or per month.

You need Java 17 or newer and Maven. The library uses one other library,
[zstd-jni](https://github.com/luben/zstd-jni), to compress data on the network.

## Quick start

1. Build and start the server. From the root of this repository:

   ```sh
   make
   ./build/tsdbd --dir /tmp/tsdb-demo --port 7070
   ```

2. In another terminal, build the client and run the example below:

   ```sh
   cd client/java
   mvn -q package -DskipTests
   java -cp target/tsdb-client-0.1.0-SNAPSHOT.jar ai.intellistream.tsdb.example.Example 127.0.0.1 7070
   ```

3. To use the client in your own Maven project, install it into your local Maven repository with `mvn install`
   and add this dependency:

   ```xml
   <dependency>
     <groupId>ai.intellistream</groupId>
     <artifactId>tsdb-client</artifactId>
     <version>0.1.0-SNAPSHOT</version>
   </dependency>
   ```

The example (`src/main/java/ai/intellistream/tsdb/example/Example.java`) stores one value per minute for a day,
then reads them back in three ways:

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

## Connect

```java
try (TsdbClient client = TsdbClient.connect("127.0.0.1", 7070)) {
    // use the client here; it is closed at the end of the block
}
```

One client is one network connection. Use it from one thread at a time. If several threads need the database,
give each its own client.

By default a request waits as long as the server takes, so a server that hangs (a stalled disk, a dead peer)
hangs the calling thread. `TsdbClient.connect(host, port, Duration.ofSeconds(10), Duration.ofSeconds(30))` bounds
the connect and every later wait for the server (the longest silence it puts up with, not a deadline for the whole
call), and `setRequestTimeout` changes it on a live client. A call that
times out throws `TsdbException` with `Status.ERR_IO` (`isTimeout()` is true) and closes the client, so make a new
one. TCP keepalive is on: after 30 seconds of silence the client probes every 10 seconds and drops the connection
after 3 unanswered probes (`setKeepAlive` changes or turns it off).

## Create a series

A series has a name and a value type. `series` gets a series of 32-bit floats, and creates it if it is missing.
`seriesCreate` lets you choose another type (see [Value types and nulls](#value-types-and-nulls)).

```java
int flow = client.series("pump7.flow");
int energy = client.seriesCreate("pump7.energy_wh", ValueType.INT64);
```

Both return a number, the series id. The other calls take this id.

## Insert

Times are milliseconds since 1970-01-01 00:00 UTC, as `Instant.toEpochMilli()` gives them.

```java
long t0 = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
long[] times = {t0, t0 + 60_000, t0 + 120_000};
float[] values = {41.5f, 42.0f, 41.8f};
client.insert(flow, times, values);
```

`insert` returns when the server has saved the rows. If you write the same series and time again, the new value
replaces the old one. Send many rows per call: one call with 10,000 rows is far faster than 10,000 calls with one
row. To keep sending without waiting for each answer, use `insertAsync`, then call `await()` once at the end.

## Read raw points

A time range includes its start and excludes its end. `Long.MIN_VALUE` and `Long.MAX_VALUE` mean "from the
beginning" and "to the end".

```java
Points hour = client.queryPoints(flow, t0, t0 + 3_600_000);
for (int i = 0; i < hour.size(); i++)
    System.out.println(Instant.ofEpochMilli(hour.timestamp(i)) + " " + hour.value(i));

Points newest = client.queryPoints(flow, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 10); // the 10 newest
```

For results too large for memory, `queryRaw` hands you the points in pieces of up to 16,384.

## Aggregate

`queryAgg` cuts a time range into equal intervals and returns the count, sum, minimum, maximum and average of each
one. Intervals without data are left out.

```java
for (Bucket b : client.queryAgg(flow, t0, t0 + 86_400_000, 3_600_000)) // one bucket per hour
    System.out.println(Instant.ofEpochMilli(b.start()) + " avg " + b.avg() + " max " + b.max());
```

An interval of 0 gives one bucket for the whole range.

## Group by day in a time zone

`queryGroup` groups by calendar units: `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK` (Monday to Sunday), `MONTH`,
`QUARTER` and `YEAR`, in UTC or in a time zone.

```java
Group osloDays = Group.of(Unit.DAY).in("Europe/Oslo"); // or .in(ZoneId.of("Europe/Oslo"))
for (Bucket b : client.queryGroup(flow, Long.MIN_VALUE, Long.MAX_VALUE, osloDays))
    System.out.println(Instant.ofEpochMilli(b.start()) + " " + b.count());

Group quarterHours = Group.of(Unit.MINUTE, 15);        // 15 minutes per bucket, in UTC
```

A day in Oslo starts at local midnight. That is 23:00 UTC in winter and 22:00 UTC in summer, and the days when
the clocks change have 23 or 25 hours. Each bucket's `start()` is the UTC time its day began.

The server only knows UTC. The client looks up the time zone in Java's own time zone data (`java.time`), works
out when each local day begins, and sends those times to the server. `osloDays.bounds(start, end)` returns these
times if you want them, and `queryBounds` takes your own list of start times.

## Value types and nulls

A series holds one type of value, chosen when it is created:

| `ValueType` | Java array | Note |
| --- | --- | --- |
| `FLOAT32` | `float[]` | what `series(name)` creates |
| `FLOAT64` | `double[]` | |
| `INT64` | `long[]` | sums are exact (`isum()`, `imin()`, `imax()` in a `Bucket`) |
| `BOOL` | `boolean[]` | sums count `true` as 1 |
| `STATE` | `int[]` | names such as "running", stored as numbers: `client.state(series, "running")` |

A `Column` holds the values. A null means "unknown value"; mark nulls with a `BitSet`:

```java
long[] times4 = {t0, t0 + 60_000, t0 + 120_000, t0 + 180_000};
BitSet unknown = new BitSet();
unknown.set(2); // the third value is unknown
client.insertSeriesValues(energy, times4, Column.ofLongs(1_000, 1_250, 0, 1_900).withNulls(unknown));

ValuePoints read = client.queryValues(energy, Long.MIN_VALUE, Long.MAX_VALUE);
long[] wh = read.values().longs();
boolean missing = read.values().isNull(2); // true
```

Aggregates skip nulls. `TypedExample.java` in the example folder shows all of this, with state names too.

## Compression

Inserts are compressed with zstd, level 1, by default. A row then takes a few bytes on the network instead of
16 or more. The client only compresses when the server supports it (version 3 or newer) and zstd-jni is on the
class path; otherwise it sends rows uncompressed.

```java
client.setCompression(0);  // off: less CPU, more network traffic
client.setCompression(3);  // smaller, slower; any zstd level works
long sent = client.bytesSent();
```

zstd-jni loads native code. On Java 22 and newer, run with `--enable-native-access=ALL-UNNAMED` to avoid a
warning.

## Errors

A failed request throws `TsdbException`. Its `status()` is one of the `Status.ERR_*` codes, and `getMessage()`
explains it. After an error from the server the client still works. After a network error `isOpen()` is false:
make a new client. Bad arguments, such as arrays of different lengths, throw `IllegalArgumentException` before
anything is sent.

## Insert benchmark

`InsertBench` measures how fast the client can insert. It reads a file of rows made by `tsdb-bench`, sends it over
one or more connections, and prints rows per second and bytes per row.

```sh
../../build/tsdb-bench --export /tmp/x.rb --series 10 --points 100000
../../build/tsdbd --dir /tmp/bench-db --port 7070 &
mvn -q package -DskipTests
java --enable-native-access=ALL-UNNAMED -cp target/tsdb-client-0.1.0-SNAPSHOT.jar \
    ai.intellistream.tsdb.bench.InsertBench /tmp/x.rb --port 7070 --conns 4 --compression 1
```

`--compression off` sends rows uncompressed. Use a large file (hundreds of millions of rows): small runs mostly
measure Java warming up.

`QueryBench` runs the random query windows of `tsdbc-bench query` (`--conns N`, `--seconds S`).
`../../bench/clients/run.sh` runs both next to the other clients and the embedded library.

## Build, test and docs

```sh
make -C ../..           # the server, which the tests start
mvn package             # build and run all tests
mvn javadoc:javadoc     # API docs in target/reports/apidocs/index.html
```

The tests start `../../build/tsdbd` themselves, each on a free port with a fresh directory. Use
`-Dtsdbd=/path/to/tsdbd` to test another server binary and `-Dtsdb.test.dir=/path` to choose where the test data
goes.
