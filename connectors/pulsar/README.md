# tsdb sink for Apache Pulsar 5

A Pulsar IO sink that writes points from Pulsar topics into `tsdbd`.

Pulsar is the buffer in front of tsdb. Producers send sensor points to a Pulsar
topic; this sink takes them off the topic and inserts them into tsdb. When tsdb
cannot take writes (its disk, or the NFS server under it, is gone for an hour),
the sink waits and retries, and Pulsar keeps the points until tsdb is back.

What it promises:

- **Nothing is lost.** A message is acknowledged to Pulsar only after tsdbd has
  confirmed the insert that holds it, and tsdbd confirms only what is on disk,
  as long as it runs in its default sync mode (`--wal-sync-ms 0`, fsync on).
  With `--wal-sync-ms N` or `--no-fsync` tsdbd confirms inserts before they are
  on disk, and a crash of its host can lose points the sink has already
  acknowledged: run tsdbd behind this sink without those options. If the sink, tsdbd or Pulsar crashes, unacknowledged messages are delivered
  again. Writing a point twice is harmless: tsdb keeps one value per series and
  time, the last one written (but see the request timeout below for rewrites
  of the same key).
- **Order per series.** Points of one series are written in the order Pulsar
  delivered them, so a correction (same series and time, new value) wins over
  the value it corrects. A batch that fails is retried as it is, never
  reordered.
- **Bad data does not block the stream.** A message tsdb can never accept (not
  JSON, a value of the wrong type, a series name tsdb refuses) goes to a
  dead-letter topic and the sink carries on.
- **Backpressure.** The sink holds a bounded number of unacknowledged messages
  (and bytes). When tsdb is down or slow it stops taking new ones, and Pulsar
  stops delivering.

Built and tested against Apache Pulsar **5.0.0** (the `pulsar-io-core` 5.0.0
`Sink` interface, the Pulsar 5 `v5` client API for the dead-letter topic). It
needs Java 17 or newer and a Pulsar 5 function runtime.

## Messages

One message is one point, as JSON (no schema, or a `BYTES`/`STRING` topic) or
as a JSON or AVRO record with these fields:

```json
{"series": "pump7.flow", "ts": 1709251200000, "value": 41.5, "type": "float64"}
```

| field | meaning |
|---|---|
| `series` | series name |
| `ts` | time, milliseconds since 1970-01-01 UTC |
| `value` | a number, `true`/`false`, a text (for a `state` series) or `null` |
| `type` | optional: `float64`, `float32`, `int64`, `bool` or `state`; used when the series is created, and checked when it exists |

A series that does not exist yet is created with `type`, or with `defaultType`
if the message has none (or the point is dead-lettered, with
`autoCreate=false`). A series keeps its type: a point of another type is
dead-lettered.

Several points in one message are fine too: `{"points": [point, ...]}`, a JSON
array of points, or one series with arrays: `{"series": "s", "ts": [1, 2],
"value": [0.5, 0.7]}`. Field names are configurable.

## Rules for producers

1. **Use the series name as the message key.** Pulsar keeps order per key;
   the sink keeps order per series. If two keys carry points of one series,
   their order is lost.
   A message may hold points of several series (the `points` array). Then the
   sink acknowledges messages in the order they were delivered only on an
   ordered input (`--retain-key-ordering` or `--retain-ordering`, where Pulsar
   acknowledges a prefix of the stream) or with `connections = 1`. Elsewhere
   (`Key_Shared`, `Failover` on a classic topic) with several connections, a
   later message can be acknowledged before an earlier one whose other series
   waits on another connection; if the sink then crashes, only the earlier one
   comes again, and its old value of a key overwrites the newer one.
2. **Use a scalable topic (`topic://...`), and run the sink with
   `--retain-key-ordering`.** Scalable topics are new in Pulsar 5: they split
   and merge segments as the load changes while still delivering each key in
   order. With `--retain-key-ordering` (or `--retain-ordering`) the Pulsar 5
   runtime reads them with a stream consumer, which keeps that order.
   On a classic `persistent://` topic, use a `Failover`/`Exclusive`
   subscription or `Key_Shared`. Do **not** use a `Shared` subscription: it
   hands the messages of one key to several consumers at once and redelivers
   them out of order, so an old value can overwrite a correction.
3. **Configure `deadLetterTopic`.** On ordered subscriptions Pulsar 5 has no
   negative acknowledgement and rejects the runtime's own dead-letter options,
   so the sink publishes refused messages itself (original bytes, key and
   properties, plus `tsdb-error`, `tsdb-status`, `tsdb-source-topic` and `tsdb-source-message-id`
   properties) and then acknowledges them. Without it the sink calls
   `record.fail()`, which on an ordered subscription stops the sink instance,
   so the sink refuses to start with `--retain-key-ordering` or
   `--retain-ordering` and no `deadLetterTopic`. Elsewhere it falls back to
   `record.fail()` (with the runtime's `--dead-letter-topic`, if set).
4. **Never set an ack timeout (`--timeout-ms`).** While tsdb is down, messages
   wait in the sink for hours. An ack timeout would deliver them again meanwhile,
   and the copy, queued behind newer messages of its key, would overwrite their
   values. The sink refuses to start with one.

Note: the Pulsar 5.0 announcement says the v4 subscription types (Shared,
Key_Shared, Failover, Exclusive) remain supported next to scalable topics;
PIP-379 was the Key_Shared rework of Pulsar 4.0, not a removal.

## Deploy

Build the client and the NAR (needs Maven):

```sh
(cd client/java && mvn -q install -DskipTests)
(cd connectors/pulsar && mvn -q package)          # target/tsdb-pulsar-sink-0.9.0.nar
```

Create the topics and start the sink:

```sh
bin/pulsar-admin scalable-topics create -s 4 public/default/points
bin/pulsar-admin scalable-topics create public/default/points-dlq
bin/pulsar-admin sinks create --archive tsdb-pulsar-sink-0.9.0.nar \
    --tenant public --namespace default --name tsdb \
    --inputs topic://public/default/points --subs-name tsdb --retain-key-ordering \
    --sink-config '{"hosts": "tsdb1:7070", "connections": 4,
                    "deadLetterTopic": "topic://public/default/points-dlq"}'
```

`sinks localrun` with the same arguments runs it outside a function worker.
More than one instance (`--parallelism`) keeps per-series order only with
key-ordered delivery (`--retain-key-ordering`), where Pulsar gives each key to
one instance at a time; with `--retain-ordering` keep `--parallelism 1`.

### Login

tsdbd asks every client to log in, unless it was started with `--no-auth`. Give
the sink a user of its own that may insert into the series it writes, and
nothing else. On the tsdb host (the password is read from the file's first line,
never from the command line):

```sh
tsdb /var/lib/tsdb user add pulsar-sink --password-file sink.pw
tsdb /var/lib/tsdb user grant pulsar-sink insert prefix:plant1/
```

Then give the sink the user in its config, and the password in one of the two
ways below. The sink refuses a `password` in the config itself: Pulsar keeps the
config as plain text and shows it to anyone who may read the sink's settings
(`pulsar-admin sinks get`).

**A password file** (use this when Pulsar runs functions as processes or
threads, which is its default): `passwordFile` names a file on the worker host
whose first line is the password. Put it on every worker host that may run the
sink, owned by the user the function worker runs as and readable by nobody else
(`chmod 600`). It is read again at every reconnect, so a changed password is
picked up without a restart.

```sh
bin/pulsar-admin sinks create --archive tsdb-pulsar-sink-0.9.0.nar \
    --tenant public --namespace default --name tsdb \
    --inputs topic://public/default/points --subs-name tsdb --retain-key-ordering \
    --sink-config '{"hosts": "tsdb1:7070", "connections": 4, "user": "pulsar-sink",
                    "passwordFile": "/etc/pulsar/tsdb-sink.password",
                    "deadLetterTopic": "topic://public/default/points-dlq"}'
```

**A Pulsar secret** named `password` (`--secrets '{"password": ...}'` instead of
`passwordFile`), but only with a secrets provider that really keeps it secret,
such as Kubernetes secrets (there the value in `--secrets` names a Kubernetes
secret, not the password). Pulsar's default secrets provider does not: it keeps
the secret as clear text, exactly like the config. With it, the password shows
in `pulsar-admin sinks get` to anyone who may read the sink's settings, and, when
the worker runs the sink as a process, on that process's command line, which
every user of the worker host can list (`ps`). With the default provider, use
`passwordFile`.

If tsdbd refuses the login (wrong password, unknown or dropped user), the sink
does not drop anything: it waits and tries again with its usual backoff, logs one
warning per try that says the login was refused, and Pulsar keeps the messages
meanwhile. A message for a series the user has no right on is refused by tsdbd
(`TSDB_ERR_DENIED`) and goes to the dead-letter topic; the rest is written.

## Configuration

| key | default | meaning |
|---|---|---|
| `hosts` | `127.0.0.1` | tsdbd endpoints, `host[:port]` separated by commas, tried in order on every (re)connect, starting after the last one that refused writes (read only, locked, stopping) |
| `port` | `7070` | port of endpoints that name none |
| `user` | empty | tsdbd user to log in as (see Login); empty: no login, for a tsdbd started with `--no-auth` |
| `password` | empty | the user's password: only as the Pulsar secret `password` (`--secrets`), never in the config; with Pulsar's default (clear text) secrets provider it is as visible as the config: use `passwordFile` then (see Login) |
| `passwordFile` | empty | instead of `password`: a file on the worker host (mode 0600, owned by the worker's user) whose first line is the password, read at every (re)connect |
| `connections` | `1` | connections to tsdbd; each series always uses the same one (by a hash of its name), which keeps its order |
| `batchRows` | `10000` | a batch is sent when it holds this many points |
| `batchBytes` | `4194304` | ... or about this many bytes |
| `lingerMs` | `20` | ... or this long after its first point arrived |
| `maxInFlightRecords` | `50000` | messages received but not yet acknowledged; the sink stops taking more beyond this |
| `maxInFlightBytes` | `67108864` | the same, in bytes of points |
| `retryInitialMs` | `100` | first delay before retrying a failed batch |
| `retryMaxMs` | `30000` | longest delay; it doubles up to this |
| `retryJitter` | `0.2` | random part of each delay (fraction) |
| `connectTimeoutMs` | `10000` | timeout of a connection attempt |
| `requestTimeoutMs` | `30000` | longest wait for tsdb on an open connection; `0` waits forever (see below) |
| `defaultType` | `float64` | type of new series whose points name none |
| `autoCreate` | `true` | create missing series; `false` dead-letters their points |
| `deadLetterTopic` | empty | where refused messages go (see above); empty: `record.fail()` |
| `compressionLevel` | `1` | zstd level of inserts; `0` sends them uncompressed |
| `seriesCacheSize` | `100000` | series names remembered per connection |
| `seriesCacheBytes` | `67108864` | heap the remembered series and state names may take per connection (an estimate); the least recently used go first |
| `stateCacheSize` | `4096` | state names remembered per state series; a forgotten one is asked from tsdbd again (same code) |
| `closeTimeoutMs` | `30000` | how long stopping waits for batches in flight; what is not written by then stays unacknowledged |
| `seriesField`, `timestampField`, `valueField`, `typeField`, `pointsField` | `series`, `ts`, `value`, `type`, `points` | field names |
| `permanentCodes`, `retryableCodes` | empty | tsdb status codes to treat as permanent or retryable, overriding the defaults below |

## Errors

| failure | what the sink does |
|---|---|
| connection refused, reset or timed out; no answer within `requestTimeoutMs` (the connection is closed); `TSDB_ERR_IO` (disk or NFS gone), `NOMEM`, `CORRUPT`; any code it does not know | waits (exponential backoff with jitter, capped) and sends the **same** batch again; acknowledges nothing meanwhile |
| `LOCKED`, `STOPPED`, `READONLY` (a follower after a failover, a server shutting down) | as above, but first closes the connection, so the retry goes to the next endpoint in `hosts` |
| `AUTH`: the login was refused (wrong password, unknown or dropped user), or tsdbd wants a login and `user` is empty | waits and tries again with backoff, as above; logs one warning per try that names the login |
| `TSDB_ERR_INVALID` (bad name, wrong type), `NOTFOUND` (unknown series, `autoCreate=false`), `DENIED` (the user has no right on the series), `TOO_MANY_BUCKETS`; a message that does not parse; a value that is tsdb's encoding of null (int64 `-9223372036854775808`, the reserved NaN of float32/float64), which would silently read back as null | dead-letters that message; the rest of the batch is written |
| the dead-letter topic refuses the message | keeps it unacknowledged and tries again (backoff), so the stream behind it waits; counted in `tsdb_dead_letter_failures`, logged as an error when only an operator can fix it (permissions, missing topic, size limit) |

A tsdb that stops answering (an NFS outage parks its requests, a dead peer sends
no reset) would block a writer for good, so every wait for tsdb on an open
connection is bounded by `requestTimeoutMs` (30 s; `0` waits forever; the
longest silence tolerated, not a deadline for a whole batch). On a
timeout the sink closes the connection, backs off and sends the same batch again
on a new one, as for any lost connection. Set it above the longest write you
expect to take. A batch that is only slow is sent twice, and the first copy may
still be parked in tsdb when the second one runs: it can then be applied after
points the sink wrote later on the new connection, so for a key written more
than once within that window the older value can win. Keys written once (the
usual case: a point per series and time) are unaffected. TCP keepalive is on
as well.

Unknown codes are retried on purpose: a wrong "retry" stalls the sink where
everyone can see it, a wrong "dead-letter" silently moves data aside. Later
protocol versions may add codes such as "unavailable" or "moved"; they are
retried until `ErrorClassifier` names them, and `permanentCodes` /
`retryableCodes` change the decision without a rebuild.

If one message holds points of several series and one of them is refused, the
whole message is dead-lettered; its other points may already be in tsdb. Do not
replay such a dead letter as it is once newer values of those series have been
written: its old points would overwrite them. Replay only the refused points,
or the message before newer data of its series. The `tsdb-error` reason is cut
to 1000 characters.

## Metrics

Reported through the sink context: `tsdb_records_written`, `tsdb_rows_written`,
`tsdb_retries`, `tsdb_dead_lettered`, `tsdb_dead_letter_failures` (alert on it:
the stream waits for the dead-letter topic), `tsdb_backoff_ms` (current delay, 0
when healthy) and `tsdb_batch_latency_ms`.

## Tests

```sh
make                                     # build/tsdbd, in the repository root
cd connectors/pulsar && mvn test         # unit tests, and tests against build/tsdbd (kill -9 included)
connectors/pulsar/test/integration.sh    # Pulsar 5.0.0 in Podman, faults; a few minutes
```

The integration test runs Pulsar standalone in Podman (the `apachepulsar/pulsar:5.0.0`
image, pinned by digest, bound to 127.0.0.1) and the sink with `sinks localrun`
on a scalable topic. It produces points over 500 series with corrections, and
checks that each reaches tsdb exactly once with its last value while it kills
tsdbd with `kill -9`, stops it for minutes (`STOP_S`, default 180 s), kills the
sink with `kill -9`, and sends messages tsdb must refuse (they must arrive on
the dead-letter topic).

Login is on: the test makes the users with the `tsdb` command line before tsdbd
starts (a superuser, and a `sink` user with `insert prefix:pump` alone), gives
the sink its password with `--secrets`, and sends one message for a series
outside that grant, which must reach the dead-letter topic too. While tsdbd is
stopped it changes the sink's password, so tsdbd refuses the sink's login when
it comes back: nothing may be acknowledged until the password is put back with
the command line, beside the running tsdbd. `AUTH=0` runs tsdbd with
`--no-auth` instead.
