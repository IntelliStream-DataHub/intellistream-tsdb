/**
 * The benchmarks: {@link ai.intellistream.tsdb.bench.InsertBench} loads a file of rows into tsdbd over one or more
 * connections and prints rows per second and bytes sent per row; {@link ai.intellistream.tsdb.bench.QueryBench}
 * runs the random query windows of {@code tsdbc-bench query}.
 *
 * <pre>{@code
 * java -cp target/tsdb-client-0.9.0.jar ai.intellistream.tsdb.bench.InsertBench /tmp/x.rb --conns 4
 * java -cp target/tsdb-client-0.9.0.jar ai.intellistream.tsdb.bench.QueryBench --conns 32
 * }</pre>
 */
package ai.intellistream.tsdb.bench;
