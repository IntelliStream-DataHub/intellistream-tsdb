package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.ValueType;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration of {@link TsdbSink}, read from the sink's {@code --sink-config} (JSON or YAML map).
 *
 * <p>Unknown keys are an error, so a misspelt option fails the sink at start instead of being ignored. Every option
 * has a default; {@link #validate()} checks the ranges.
 */
public class TsdbSinkConfig {
    /**
     * tsdbd endpoints, "host[:port]" separated by commas, tried in order whenever a connection is (re)opened,
     * starting after the last one that refused writes (read only, locked, stopping).
     */
    @FieldDoc(required = false, defaultValue = "127.0.0.1",
            help = "tsdbd endpoints, host[:port] separated by commas; tried in order on every (re)connect, "
                    + "starting after the last one that refused writes")
    public String hosts = "127.0.0.1";

    /** Port of endpoints that do not name one. */
    @FieldDoc(required = false, defaultValue = "7070", help = "port of endpoints in hosts that do not name one")
    public int port = 7070;

    /** Connections to tsdbd; each series always uses the same one, which keeps its order. */
    @FieldDoc(required = false, defaultValue = "1",
            help = "connections to tsdbd; series are spread over them by a hash of the name, which keeps per-series order")
    public int connections = 1;

    /** A batch is sent when it holds this many points. */
    @FieldDoc(required = false, defaultValue = "10000", help = "send a batch when it holds this many points")
    public int batchRows = 10_000;

    /** A batch is sent when its points take about this many bytes. */
    @FieldDoc(required = false, defaultValue = "4194304", help = "send a batch when its points take about this many bytes")
    public long batchBytes = 4L << 20;

    /** A batch is sent at the latest this long after its first record arrived. */
    @FieldDoc(required = false, defaultValue = "20", help = "send a batch at the latest this many ms after its first record")
    public long lingerMs = 20;

    /** At most this many records are accepted but not yet acknowledged; write() blocks beyond. */
    @FieldDoc(required = false, defaultValue = "50000",
            help = "records received but not yet acknowledged; write() blocks when reached, so Pulsar stops delivering")
    public int maxInFlightRecords = 50_000;

    /** At most about this many bytes of points are accepted but not yet acknowledged. */
    @FieldDoc(required = false, defaultValue = "67108864",
            help = "bytes of points received but not yet acknowledged; write() blocks when reached")
    public long maxInFlightBytes = 64L << 20;

    /** First delay before retrying a batch that failed for a retryable reason. */
    @FieldDoc(required = false, defaultValue = "100", help = "first retry delay in ms; doubles on every failure")
    public long retryInitialMs = 100;

    /** Longest delay between retries. */
    @FieldDoc(required = false, defaultValue = "30000", help = "longest retry delay in ms")
    public long retryMaxMs = 30_000;

    /** Random part of each delay, as a fraction of it (0 to 1). */
    @FieldDoc(required = false, defaultValue = "0.2", help = "random part of each retry delay, a fraction from 0 to 1")
    public double retryJitter = 0.2;

    /** Timeout of a connection attempt. */
    @FieldDoc(required = false, defaultValue = "10000", help = "connect timeout in ms")
    public long connectTimeoutMs = 10_000;

    /**
     * Longest wait for tsdb on an open connection, sending or receiving. A stalled server (an NFS outage parks its
     * requests, a dead peer sends no reset) would otherwise block a writer for good; on a timeout the connection is
     * closed and the batch retried on a new one, like any other lost connection.
     */
    @FieldDoc(required = false, defaultValue = "30000",
            help = "longest wait for tsdb on an open connection in ms; 0 waits forever. A timeout closes the "
                    + "connection and the batch is retried")
    public long requestTimeoutMs = 30_000;

    /** Type of series the sink creates when a point names none. */
    @FieldDoc(required = false, defaultValue = "float64",
            help = "type of series created for points without a type field: float64, float32, int64, bool or state")
    public String defaultType = "float64";

    /** Whether missing series are created; when false, their points go to the dead-letter path. */
    @FieldDoc(required = false, defaultValue = "true",
            help = "create missing series; when false, points of unknown series are dead-lettered")
    public boolean autoCreate = true;

    /**
     * Topic for records tsdb refuses for good. Empty: such records are failed with {@code record.fail()}, which
     * only a Shared subscription with the runtime's dead letter policy can turn into a dead letter.
     */
    @FieldDoc(required = false, defaultValue = "",
            help = "topic that receives records tsdb refuses for good (published with the Pulsar 5 client); "
                    + "empty: record.fail(), which needs a Shared subscription with a dead letter policy")
    public String deadLetterTopic = "";

    /** zstd level of packed inserts; 0 sends plain arrays. */
    @FieldDoc(required = false, defaultValue = "1", help = "zstd level of packed inserts; 0 sends plain arrays")
    public int compressionLevel = 1;

    /** Bound of the series name cache, per connection. */
    @FieldDoc(required = false, defaultValue = "100000", help = "series names cached per connection (LRU)")
    public int seriesCacheSize = 100_000;

    /** How long close() waits for in-flight batches; what is not written by then stays unacknowledged. */
    @FieldDoc(required = false, defaultValue = "30000",
            help = "ms close() waits for in-flight batches; records not written by then are left unacknowledged")
    public long closeTimeoutMs = 30_000;

    /** Field holding the series name. */
    @FieldDoc(required = false, defaultValue = "series", help = "record field with the series name")
    public String seriesField = "series";

    /** Field holding the timestamp, ms since the Unix epoch (UTC). */
    @FieldDoc(required = false, defaultValue = "ts", help = "record field with the timestamp, ms since the epoch")
    public String timestampField = "ts";

    /** Field holding the value. */
    @FieldDoc(required = false, defaultValue = "value", help = "record field with the value (number, bool, string or null)")
    public String valueField = "value";

    /** Optional field naming the value type of the series. */
    @FieldDoc(required = false, defaultValue = "type", help = "optional record field with the series type")
    public String typeField = "type";

    /** Field holding the array of points of a batch message. */
    @FieldDoc(required = false, defaultValue = "points", help = "record field with an array of points (batch message)")
    public String pointsField = "points";

    /** Status codes treated as permanent in addition to (or instead of) the defaults, e.g. "-4,-6". */
    @FieldDoc(required = false, defaultValue = "",
            help = "tsdb status codes to treat as permanent (dead-letter), comma separated; overrides the default")
    public String permanentCodes = "";

    /** Status codes treated as retryable, overriding the defaults. */
    @FieldDoc(required = false, defaultValue = "",
            help = "tsdb status codes to treat as retryable, comma separated; overrides the default")
    public String retryableCodes = "";

    /**
     * Reads a configuration map; unknown keys and wrong types are errors.
     *
     * @param map the sink configuration as Pulsar passes it to {@code open}
     * @return the validated configuration
     * @throws IllegalArgumentException for unknown keys, wrong types or values out of range
     */
    public static TsdbSinkConfig load(Map<String, Object> map) {
        ObjectMapper m = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        TsdbSinkConfig c = m.convertValue(map == null ? Map.of() : map, TsdbSinkConfig.class);
        c.validate();
        return c;
    }

    /**
     * Checks ranges and parses the derived values.
     *
     * @throws IllegalArgumentException for a value out of range
     */
    public void validate() {
        require(!endpoints().isEmpty(), "hosts is empty");
        require(port > 0 && port < 65536, "port out of range");
        require(connections >= 1 && connections <= 256, "connections must be 1 to 256");
        require(batchRows >= 1, "batchRows must be positive");
        require(batchBytes >= 1, "batchBytes must be positive");
        require(lingerMs >= 0, "lingerMs must not be negative");
        require(maxInFlightRecords >= 1, "maxInFlightRecords must be positive");
        require(maxInFlightBytes >= 1, "maxInFlightBytes must be positive");
        require(retryInitialMs >= 1 && retryMaxMs >= retryInitialMs, "need 1 <= retryInitialMs <= retryMaxMs");
        require(retryJitter >= 0 && retryJitter <= 1, "retryJitter must be 0 to 1");
        require(connectTimeoutMs >= 1, "connectTimeoutMs must be positive");
        require(requestTimeoutMs >= 0, "requestTimeoutMs must not be negative");
        require(seriesCacheSize >= 1, "seriesCacheSize must be positive");
        require(closeTimeoutMs >= 0, "closeTimeoutMs must not be negative");
        defaultValueType();
        codes(permanentCodes);
        codes(retryableCodes);
        for (String f : new String[] {seriesField, timestampField, valueField, typeField, pointsField})
            require(f != null && !f.isEmpty(), "field names must not be empty");
    }

    /**
     * The endpoints of {@link #hosts}.
     *
     * @return host and port pairs, in the configured order
     */
    public List<Map.Entry<String, Integer>> endpoints() {
        List<Map.Entry<String, Integer>> r = new ArrayList<>();
        if (hosts == null)
            return r;
        for (String h : hosts.split(",")) {
            h = h.trim();
            if (h.isEmpty())
                continue;
            int colon = h.lastIndexOf(':');
            if (colon > 0 && h.indexOf(':') == colon)
                r.add(Map.entry(h.substring(0, colon), Integer.parseInt(h.substring(colon + 1))));
            else
                r.add(Map.entry(h, port));
        }
        return r;
    }

    /**
     * The type of series created for points that name none.
     *
     * @return {@link #defaultType} as a value type
     * @throws IllegalArgumentException for an unknown name
     */
    public ValueType defaultValueType() {
        ValueType t = parseType(defaultType);
        require(t != null, "unknown defaultType " + defaultType);
        return t;
    }

    /**
     * Parses a type name as records and the configuration write it.
     *
     * @param name float64, float32, int64, bool or state (any case)
     * @return the type, or {@code null} for an unknown name
     */
    public static ValueType parseType(String name) {
        if (name == null)
            return null;
        switch (name.toLowerCase(Locale.ROOT)) {
        case "float64": case "double": return ValueType.FLOAT64;
        case "float32": case "float": return ValueType.FLOAT32;
        case "int64": case "long": return ValueType.INT64;
        case "bool": case "boolean": return ValueType.BOOL;
        case "state": return ValueType.STATE;
        default: return null;
        }
    }

    static Set<Integer> codes(String csv) {
        Set<Integer> r = new HashSet<>();
        if (csv == null)
            return r;
        for (String s : csv.split(",")) {
            s = s.trim();
            if (s.isEmpty())
                continue;
            int c = Integer.parseInt(s);
            require(c < 0, "status codes are negative: " + s);
            r.add(c);
        }
        return r;
    }

    private static void require(boolean ok, String message) {
        if (!ok)
            throw new IllegalArgumentException("tsdb sink config: " + message);
    }
}
