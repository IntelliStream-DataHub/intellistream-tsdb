// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.ValueType;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration of {@link TsdbSink}, read from the sink's {@code --sink-config} (JSON or YAML map) and, for the
 * {@code password}, from Pulsar's secrets.
 *
 * <p>Unknown keys are an error, so a misspelt option fails the sink at start instead of being ignored. Every option
 * has a default; {@link #validate()} checks the ranges.
 */
public class TsdbSinkConfig {
    /**
     * The tsdbd user the sink logs in as; empty logs in as nobody, which only a tsdbd started with {@code --no-auth}
     * accepts. The user needs {@code insert} on the series the sink writes (which also lets it look them up and
     * create them); a record of any other series is refused ({@code TSDB_ERR_DENIED}) and dead-lettered.
     */
    @FieldDoc(required = false, defaultValue = "",
            help = "tsdbd user to log in as (needs insert on the series written); empty: no login, "
                    + "for a tsdbd started with --no-auth")
    public String user = "";

    /**
     * The user's password. Sensitive: it is read from Pulsar's secrets (a secret named {@code password}, given with
     * {@code --secrets}), never from the plain configuration, which Pulsar stores and shows as it is. That keeps
     * it hidden only with a secrets provider that is not clear text (Kubernetes secrets): Pulsar's default provider
     * stores and shows the secret like the configuration ({@code sinks get}), and a process runtime puts it on the
     * instance's command line, which every user of the worker host can list. Use {@link #passwordFile} there.
     */
    @FieldDoc(required = false, defaultValue = "", sensitive = true,
            help = "the user's password; a secret named password (--secrets), never the plain config. Pulsar's "
                    + "default secrets provider is clear text and shows it like the config: use passwordFile then")
    public String password = "";

    /**
     * A file on the worker host whose first line is the password, read at every (re)connect; instead of
     * {@link #password}. Owned by the user the function worker runs as, mode 0600.
     */
    @FieldDoc(required = false, defaultValue = "",
            help = "file whose first line is the user's password (mode 0600, owned by the worker's user), read at "
                    + "every (re)connect; instead of password")
    public String passwordFile = "";

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

    /** Bound of the estimated heap of the cached series and state names, per connection. */
    @FieldDoc(required = false, defaultValue = "67108864",
            help = "bytes of series and state names cached per connection (estimated heap, LRU)")
    public long seriesCacheBytes = 64L << 20;

    /** Bound of the state names cached per state series. */
    @FieldDoc(required = false, defaultValue = "4096", help = "state names cached per state series (LRU)")
    public int stateCacheSize = 4096;

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

    /**
     * Exact names of the series this sink instance writes. Together with {@link #seriesPrefixes} and
     * {@link #seriesPattern}; when none of the three is set, every series is written.
     */
    @FieldDoc(required = false, defaultValue = "",
            help = "series this instance writes: exact names. With seriesPrefixes and seriesPattern they add up; "
                    + "none of the three set: all series. Messages for other series are acknowledged and dropped")
    public List<String> series = new ArrayList<>();

    /** Prefixes of the names of the series this sink instance writes; how tenants are named. */
    @FieldDoc(required = false, defaultValue = "",
            help = "series this instance writes: name prefixes, e.g. a tenant's. Adds up with series and "
                    + "seriesPattern. Messages for other series are acknowledged and dropped")
    public List<String> seriesPrefixes = new ArrayList<>();

    /** A Java regular expression the whole name of a series must match to be written. */
    @FieldDoc(required = false, defaultValue = "",
            help = "series this instance writes: a Java regular expression the whole name must match. Adds up "
                    + "with series and seriesPrefixes. Messages for other series are acknowledged and dropped")
    public String seriesPattern = "";

    /**
     * Volume paths for new series: a name prefix (or a whole name) to the path of a volume on the tsdbd host. The
     * longest matching key wins; unmatched names are created in the database directory.
     */
    @FieldDoc(required = false, defaultValue = "",
            help = "volume for new series: name prefix or name to an absolute volume path, longest prefix wins; "
                    + "other series go to the database directory. The operator must add the volume first "
                    + "(tsdb DIR volume add PATH), else the message is dead-lettered")
    public Map<String, String> volumes = new HashMap<>();

    private SeriesSelection selection;

    /** Status codes treated as permanent in addition to (or instead of) the defaults, e.g. "-4,-6". */
    @FieldDoc(required = false, defaultValue = "",
            help = "tsdb status codes to treat as permanent (dead-letter), comma separated; overrides the default")
    public String permanentCodes = "";

    /** Status codes treated as retryable, overriding the defaults. */
    @FieldDoc(required = false, defaultValue = "",
            help = "tsdb status codes to treat as retryable, comma separated; overrides the default")
    public String retryableCodes = "";

    /**
     * Reads a configuration map without secrets; unknown keys and wrong types are errors.
     *
     * @param map the sink configuration as Pulsar passes it to {@code open}
     * @return the validated configuration
     * @throws IllegalArgumentException for unknown keys, wrong types, values out of range or a password (which is
     *     a secret)
     */
    public static TsdbSinkConfig load(Map<String, Object> map) {
        return load(map, null);
    }

    /**
     * Reads a configuration map with the sensitive options ({@code password}) taken from the sink's secrets, as
     * Pulsar's own connectors do ({@code IOConfigUtils.loadWithSecrets}); unknown keys and wrong types are errors.
     *
     * @param map the sink configuration as Pulsar passes it to {@code open}
     * @param ctx the sink context, whose {@code getSecret} gives the secrets; {@code null} reads the map alone
     * @return the validated configuration
     * @throws IllegalArgumentException for a password in the plain configuration, unknown keys, wrong types or values
     *     out of range
     */
    public static TsdbSinkConfig load(Map<String, Object> map, SinkContext ctx) {
        Map<String, Object> m = map == null ? Map.of() : map;
        /* Pulsar keeps the sink config in its metadata store and shows it to anyone who may list sinks */
        require(!m.containsKey("password"), "password belongs in the sink's secrets (--secrets '{\"password\": ...}'), "
                + "not in its configuration; or use passwordFile");
        /* a single name is a list of one; done here because IOConfigUtils converts with a mapper of its own */
        for (String k : List.of("series", "seriesPrefixes")) {
            if (m.get(k) instanceof String one) {
                /* "acme.,globex." (the way permanentCodes is written) would be one prefix that selects nothing, and
                 * the sink would acknowledge and drop every message; a list still allows a name with a comma */
                require(one.indexOf(',') < 0, k + " is one string with a comma: give a JSON list ([\"acme.\", "
                        + "\"globex.\"]), not a comma separated string");
                m = new HashMap<>(m);
                m.put(k, List.of(one));
            }
        }
        TsdbSinkConfig c;
        try {
            c = ctx == null
                    ? new ObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                            .convertValue(m, TsdbSinkConfig.class)
                    : IOConfigUtils.loadWithSecrets(m, TsdbSinkConfig.class, ctx);
        } catch (IllegalArgumentException e) {
            /* Jackson's message names the unknown or mistyped key, never a value */
            throw new IllegalArgumentException("tsdb sink config: " + e.getMessage(), e);
        }
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
        require(seriesCacheBytes >= 1, "seriesCacheBytes must be positive");
        require(stateCacheSize >= 1, "stateCacheSize must be positive");
        require(closeTimeoutMs >= 0, "closeTimeoutMs must not be negative");
        selection = new SeriesSelection(series, seriesPrefixes, seriesPattern, volumes);
        defaultValueType();
        codes(permanentCodes);
        codes(retryableCodes);
        for (String f : new String[] {seriesField, timestampField, valueField, typeField, pointsField})
            require(f != null && !f.isEmpty(), "field names must not be empty");
        validateLogin();
    }

    /* Mistakes in the login fail the sink at start: at connect they would only be retried, as a refused login is. */
    private void validateLogin() {
        boolean hasUser = user != null && !user.isEmpty();
        boolean hasPassword = password != null && !password.isEmpty();
        boolean hasFile = passwordFile != null && !passwordFile.isEmpty();
        if (!hasUser) {
            require(!hasPassword && !hasFile, "a password or passwordFile without a user");
            return;
        }
        byte[] u = user.getBytes(StandardCharsets.UTF_8);
        /* SCRAM's rule for a user name, which the client checks only at connect */
        require(u.length <= 255 && user.indexOf(',') < 0 && user.indexOf('=') < 0 && user.indexOf('\0') < 0,
                "user must be 1 to 255 bytes without ',', '=' or NUL");
        require(hasPassword != hasFile, hasPassword ? "both password and passwordFile are set; give one"
                : "user " + user + " needs a password (a secret) or a passwordFile");
        if (hasPassword) {
            try {
                ConnectOptions.defaults().password(password); /* the one password rule of every client */
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("tsdb sink config: password: " + e.getMessage());
            }
        } else {
            require(Files.isReadable(Path.of(passwordFile)), "passwordFile " + passwordFile + " cannot be read");
        }
    }

    /**
     * The options of a connection: the login and the timeouts.
     *
     * @return options for {@code TsdbClient.connect}
     */
    public ConnectOptions connectOptions() {
        ConnectOptions o = ConnectOptions.defaults()
                .connectTimeout(java.time.Duration.ofMillis(connectTimeoutMs))
                .requestTimeout(java.time.Duration.ofMillis(requestTimeoutMs));
        if (user == null || user.isEmpty())
            return o;
        o = o.user(user);
        return password != null && !password.isEmpty() ? o.password(password) : o.passwordFile(Path.of(passwordFile));
    }

    /**
     * Which series this instance takes and where new ones are created.
     *
     * @return the selection built by {@link #validate()}
     */
    SeriesSelection selection() {
        if (selection == null)
            selection = new SeriesSelection(series, seriesPrefixes, seriesPattern, volumes);
        return selection;
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
