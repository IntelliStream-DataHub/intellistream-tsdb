# tsdb Java client

This is a Java library for talking to `tsdbd`, the server of the tsdb time series database. A time series is a
list of measurements over time, such as the flow of a pump once a minute. You can store measurements, read them
back, and ask for totals per hour, per day or per month.

You need Java 17 or newer and Maven. The library uses one other library,
[zstd-jni](https://github.com/luben/zstd-jni), to compress data on the network.

To run the database inside your Java program instead of talking to a server, use `tsdb-embedded`
([client/java-embedded](../java-embedded/README.md)). It needs Java 22 or newer and calls the engine directly,
with no server and no network. Its `EmbeddedDb` has the same methods as `TsdbClient` and uses the same types,
so the same code works with either one: only the line that opens the database changes.

```java
try (EmbeddedDb db = EmbeddedDb.open("/var/lib/tsdb")) {   // instead of TsdbClient.connect("host", 7070)
    long id = db.series("pump7.flow");
    db.insert(id, times, values);
}
```

Start Java with `--enable-native-access=ALL-UNNAMED` so that it allows the calls into the engine.

## Quick start

1. Build and start the server. From the root of this repository:

   ```sh
   make
   mkdir -p /tmp/tsdb-demo
   (umask 077 && echo 'try-out-password-17' > /tmp/tsdb-demo/password)
   ./build/tsdb /tmp/tsdb-demo/db user add admin --superuser --password-file /tmp/tsdb-demo/password
   ./build/tsdbd --dir /tmp/tsdb-demo/db --port 7070
   ```

   The server lets in only clients that log in, so first add a user, `admin`, who may do everything (a
   "superuser"). Its password goes in a file only you can read.

2. In another terminal, build the client and run the example below. It logs in as `admin` with the password from
   `/tmp/tsdb-demo/password`:

   ```sh
   cd client/java
   mvn -q package -DskipTests
   java -cp target/tsdb-client-0.9.1.jar ai.intellistream.tsdb.example.Example 127.0.0.1 7070 admin /tmp/tsdb-demo/password
   ```

3. To use the client in your own Maven project, install it into your local Maven repository with `mvn install`
   and add this dependency:

   ```xml
   <dependency>
     <groupId>ai.intellistream</groupId>
     <artifactId>tsdb-client</artifactId>
     <version>0.9.1</version>
   </dependency>
   ```

The example (`src/main/java/ai/intellistream/tsdb/example/Example.java`) stores one value per minute for a day,
then reads them back in three ways:

```java
package ai.intellistream.tsdb.example;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.Unit;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Quick start: {@code java -cp tsdb-client.jar ai.intellistream.tsdb.example.Example [host] [port] [user]
 * [password-file]}.
 */
public final class Example {
    private Example() {
    }

    /**
     * Runs the example.
     *
     * @param args the host and port of a running tsdbd, the user to log in as and the file with its password;
     *     default 127.0.0.1 7070 admin /tmp/tsdb-demo/password
     * @throws Exception if the server cannot be reached, the login fails or a request fails
     */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : TsdbClient.DEFAULT_PORT;
        String user = args.length > 2 ? args[2] : "admin";
        Path passwordFile = Path.of(args.length > 3 ? args[3] : "/tmp/tsdb-demo/password");

        // Log in as user, with the password from the first line of passwordFile.
        ConnectOptions login = ConnectOptions.defaults().user(user).passwordFile(passwordFile);
        try (TsdbClient client = TsdbClient.connect(host, port, login)) {
            long pump = client.series("pump7.flow_m3h"); // get or create

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
ConnectOptions login = ConnectOptions.defaults().user("admin").passwordFile(Path.of("/tmp/tsdb-demo/password"));
try (TsdbClient client = TsdbClient.connect("127.0.0.1", 7070, login)) {
    // use the client here; it is closed at the end of the block
}
```

One client is one network connection. Use it from one thread at a time. If several threads need the database,
give each its own client.

`TsdbClient.connect(host, port)` gives up on the connect after 10 seconds.
By default a request waits as long as the server takes, so a server that hangs (a stalled disk, a dead peer)
hangs the calling thread. `TsdbClient.connect(host, port, Duration.ofSeconds(10), Duration.ofSeconds(30))` bounds
the connect and every later wait for the server (the longest silence it puts up with, not a deadline for the whole
call), and `setRequestTimeout` changes it on a live client. A call that
times out throws `TsdbException` with `Status.ERR_IO` (`isTimeout()` is true) and closes the client, so make a new
one. TCP keepalive is on: after 30 seconds of silence the client probes every 10 seconds and drops the connection
after 3 unanswered probes (`setKeepAlive` changes or turns it off).

## Log in

tsdbd lets in only clients that log in with a user name and a password. The password never crosses the network:
client and server prove to each other that they know it (SCRAM-SHA-256). `ConnectOptions` says how to log in:

- `.user("acme/ingest")`: the user name.
- `.passwordFile(Path.of("/etc/tsdb/acme-ingest.password"))`: read the password from the first line of this file.
  Keep the file readable only by you. Or `.password(chars)` with the password itself, from wherever your program
  keeps secrets.
- `.connectTimeout(d)` and `.requestTimeout(d)`: as the two `Duration`s of `connect` above.
- `.requireAuth(true)`: refuse a server that asks for no login (one started with `--no-auth`). Without it, such a
  server is used without a login.

A wrong user name and a wrong password give the same error, `TsdbException` with `Status.ERR_AUTH`.
`TsdbClient.connect(host, port)` without options logs in as nobody, which only a server started with `--no-auth`
accepts. A client keeps what it worked out from the password, so connecting again as the same user is quick.

## Users and rights

A superuser may do everything. Every other user has rights (`Rights.READ`, `INSERT`, `DELETE`, `ADMIN`; `WRITE` is
insert and delete) on the series a selector matches: `Selector.all()`, `Selector.prefix("acme/")` (the names that
start with it), `Selector.name("pump7.flow")` or `Selector.glob("acme/*/temp")` (`*` is any text, `?` one byte).
`Selector.parse("prefix:acme/")` and `Rights.parse("read,insert")` read the text the CLI uses. A series a user may
not touch answers `Status.ERR_DENIED`, whether it exists or not, so nobody learns the names of other users' series.

A superuser, or a user with `admin` on a prefix, adds and changes users over the connection. This is the heart of
`ai.intellistream.tsdb.example.UsersExample`, which you can run after the quick start as the `Example` above:

```java
ConnectOptions login = ConnectOptions.defaults().user(user).passwordFile(passwordFile);
try (TsdbClient admin = TsdbClient.connect(host, port, login)) {
    // A user for a collector of the tenant "acme": it may add rows to the series whose names start
    // with "acme/", and do nothing else.
    admin.userCreate("acme/ingest", "collector-password-5".toCharArray(), false);
    admin.grant("acme/ingest", Selector.prefix("acme/"), Rights.INSERT);
    for (User u : admin.userList()) {
        StringJoiner grants = new StringJoiner(", ");
        for (Grant g : u.grants())
            grants.add(g.rights() + " on " + g.selector());
        System.out.println(u.name() + ": " + (u.superuser() ? "superuser" : grants));
    }

    // The collector logs in with its own password and writes a row.
    ConnectOptions collectorLogin = ConnectOptions.defaults().user("acme/ingest")
            .password("collector-password-5");
    try (TsdbClient collector = TsdbClient.connect(host, port, collectorLogin)) {
        long pump = collector.series("acme/pump1");
        collector.insert(pump, new long[] {1_700_000_000_000L}, new float[] {1.5f});

        // It may not read the row back, nor see a series of another tenant.
        try {
            collector.queryPoints(pump, Long.MIN_VALUE, Long.MAX_VALUE, Order.ASC, 10);
        } catch (TsdbException e) {
            System.out.println("read acme/pump1: " + e.getMessage());
        }
        try {
            collector.series("globex/pump1");
        } catch (TsdbException e) {
            System.out.println("globex/pump1: " + e.getMessage());
        }
    }

    // Drop the user again, so that the example can run once more.
    admin.userDrop("acme/ingest");
}
```

It prints:

```text
admin: superuser
acme/ingest: insert on prefix:acme/
read acme/pump1: permission denied (-16)
globex/pump1: permission denied (-16)
```

The other calls: `whoami()` (who this connection is, with its grants), `userPassword(name, chars)`,
`userAlter(name, superuser)` (superusers only) and `revoke(name, selector, rights)`. The password does not travel
here either: the client sends only a value made from it. On the server's own machine an operator does the same with
`tsdb DIR user ...` (see the main README, "Users and logins"); a server started with `--no-auth` has no users and
refuses these calls.

## Create a series

Series ids are 64-bit numbers in a `long`, handed out by the database: the volume in the high half, so every id is
at least 2^32 and positive. Look them up by name and keep them in a `long`, never an `int`; do not count on them
being 0, 1, 2 and so on. The client speaks protocol 14 (a server of 12 works too) and
`TsdbClient.connect` fails with `Status.ERR_INVALID` ("upgrade the server") when the server answers a version below 12.

A series has a name and a value type. `series` gets a series of 32-bit floats, and creates it if it is missing.
`seriesCreate` lets you choose another type (see [Value types and nulls](#value-types-and-nulls)).

```java
long flow = client.series("pump7.flow");
long energy = client.seriesCreate("pump7.energy_wh", ValueType.INT64);
```

Both return a number, the series id (a `long`). The other calls take this id.

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

## Spread, time-weighted averages and charts

```java
// range, variance and standard deviation per hour, newest hour first, the last 24 only
List<SpreadBucket> spread = client.querySpread(energy, t0, t0 + 86_400_000, 3_600_000, Order.DESC, 24);
double sd = spread.get(0).stddevSamp();

// time-weighted average per hour: each reading counts for as long as it held
List<TwaBucket> twa = client.queryTwa(energy, t0, t0 + 86_400_000, 3_600_000,
        TwaOptions.linear().maxGap(600_000));   // straight lines between readings; gaps over 10 minutes count as unknown
double avg = twa.get(0).twa();

// at most 500 points that keep the shape of the line, for a chart
ValuePoints chart = client.queryLttb(energy, t0, t0 + 86_400_000, 500);
```

`querySpreadGroup`, `querySpreadBounds`, `queryTwaGroup` and `queryTwaBounds` take calendar groups and bucket
starts as `queryGroup` and `queryBounds` do.

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

## Delete, replace and retention

```java
client.deleteRange(energy, t0, t0 + 3_600_000);   // one hour, its end excluded; Long.MIN_VALUE/MAX_VALUE: open
client.replaceRange(energy, t0, t0 + 2000, new long[] {t0}, Column.ofLongs(7));
client.setRetention(energy, 30 * 86_400_000L);    // keep 30 days; 0 keeps everything
long limit = client.retention(energy);
```

A delete returns once it is durable; queries stop seeing the rows at once.
A replace is atomic: readers see the old rows or the new ones, never a mix.
Rows older than a series' retention limit disappear from queries at once and
from disk as merges get to them.

## Compression

Inserts are compressed with zstd, level 1, by default. A row then takes a few bytes on the network instead of
16 or more. The client only compresses when zstd-jni is on the
class path; otherwise it sends rows uncompressed.

```java
client.setCompression(0);  // off: less CPU, more network traffic
client.setCompression(3);  // smaller, slower; any zstd level works
long sent = client.bytesSent();
```

zstd-jni loads native code. On Java 22 and newer, run with `--enable-native-access=ALL-UNNAMED` to avoid a
warning.

## Many series in one request

The `...Multi` methods ask one question of a whole list of series in a single request:
`queryValuesMulti`, `queryLttbMulti`, `queryBucketsMulti`, `querySpreadMulti` and `queryTwaMulti`. The server
works on the series in parallel, sends the answers back in the order of the list, and reads every series at the
same instant. Each series has its own result, so an unknown or bad series does not spoil the others:

```java
long[] pumps = new long[200];
for (int i = 0; i < pumps.length; i++) pumps[i] = client.series("pump" + i + ".flow");
long day = 86_400_000L;
// every pump by hour over one day
List<SeriesResult<List<Bucket>>> hourly =
        client.queryBucketsMulti(pumps, 0, day, Buckets.every(3_600_000), Order.ASC, 0, MultiOptions.defaults());
for (int i = 0; i < pumps.length; i++) {
    SeriesResult<List<Bucket>> r = hourly.get(i);
    if (r.ok()) System.out.println("pump " + pumps[i] + ": " + r.get().size() + " hours");
    else System.out.println("pump " + pumps[i] + ": status " + r.status());
}
```

`Buckets` says how to cut each range: `one()`, `every(ms)`, per-series `intervals(...)`, calendar periods of a
`group(...)` in any time zone, or explicit `bounds(...)`. `MultiOptions` can give each series its own start and
end, cap the threads the request uses (`threads(n)`) and the rows or buckets of all series together
(`maxRows(n)`). `get()` throws the series' error. A list longer than 65,536 series is split into several
requests for you. `queryValuesMulti` with a `MultiValuesConsumer` streams raw rows as they arrive; a consumer that
returns `false` stops the request on the server too. `EmbeddedDb` (client/java-embedded) has the same methods.

## The newest value of many series

`latest(long...)` and `latest(long[] ids, LatestOptions o)` (protocol 14) return the newest row of every series in one
request, in the order of the ids; `latestByName(String[] names, LatestOptions o)` takes names (ids are cached per
connection). Each element is a `SeriesResult<Latest>`; `get()` throws `NOTFOUND` or `DENIED` for that series (a
denied series and a missing one look alike to a user without `read` on it). `Latest` has `kind()` (`VALUE`, `NULL`
with `LatestOptions.defaults().withNulls()`, or `NONE`), `ts()` and the value. `end(t)` and `ends(long[])` ask as of a
time. `EmbeddedDb` has the same methods.

```java
List<SeriesResult<Latest>> r = c.latestByName(new String[] {"pump0.flow", "pump1.flow"}, LatestOptions.defaults());
Latest first = r.get(0).get();
System.out.println(first.kind() + " " + first.ts());
```

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
java --enable-native-access=ALL-UNNAMED -cp target/tsdb-client-0.9.1.jar \
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
