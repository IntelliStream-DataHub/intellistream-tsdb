/**
 * Client for {@code tsdbd}, the TCP server of the tsdb time series database.
 *
 * <p>A series is a named sequence of points: a timestamp (milliseconds since the Unix epoch) and a value. Writing
 * the same series and timestamp again replaces the value: the last write wins. Time ranges are half open,
 * {@code [start, end)}; {@link java.lang.Long#MIN_VALUE} and {@link java.lang.Long#MAX_VALUE} mean "everything".
 *
 * <pre>{@code
 * try (TsdbClient client = TsdbClient.connect("127.0.0.1", TsdbClient.DEFAULT_PORT)) {
 *     int pump = client.series("pump7.flow");                      // get or create a float32 series
 *     client.insert(pump, new long[] {t0, t0 + 60_000}, new float[] {41.5f, 42.0f});
 *
 *     Points raw = client.queryPoints(pump, t0, t0 + 3_600_000);   // the points of one hour
 *     List<Bucket> perMinute = client.queryAgg(pump, t0, t0 + 3_600_000, 60_000);
 *     List<Bucket> osloDays = client.queryGroup(pump, Long.MIN_VALUE, Long.MAX_VALUE,
 *             Group.of(Unit.DAY).in("Europe/Oslo"));               // local days: 23, 24 or 25 hours
 * }
 * }</pre>
 *
 * <p>Start with {@link ai.intellistream.tsdb.TsdbClient}. Other value types (float64, int64, bool, state) and nulls
 * travel in a {@link ai.intellistream.tsdb.Column}; aggregates come back as {@link ai.intellistream.tsdb.Bucket}s;
 * failures throw {@link ai.intellistream.tsdb.TsdbException} with a {@link ai.intellistream.tsdb.Status} code.
 *
 * <p>The client speaks the protocol of {@code include/tsdb_protocol.h} over one blocking socket per client. Its one
 * runtime dependency, zstd-jni, compresses inserts and is optional. Time zones come from the JDK
 * ({@code java.time}): the server knows UTC only, so the client turns local days, weeks and months into UTC bounds
 * itself ({@link ai.intellistream.tsdb.Group#bounds}).
 */
package ai.intellistream.tsdb;
