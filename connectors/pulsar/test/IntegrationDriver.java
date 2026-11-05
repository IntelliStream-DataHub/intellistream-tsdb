import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValuePoints;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Produces the integration test's points with the Pulsar 5 (v5 API) client and checks what reached tsdbd; driven by
 * test/integration.sh.
 *
 * <p>Record i of n writes key k(i): records come in pairs on the same key (a correction right behind the value, so the
 * final value shows whether per-series order held), and the last eighth corrects keys of the first one, long after.
 * The value of record i is i, so the final value of a key is the largest i that wrote it.
 *
 * <pre>
 * produce URL TOPIC N FROM TO [unbatched]
 *                                  records [FROM, TO) of N, keyed by series; unbatched sends one message per entry,
 *                                  so that the subscription backlog (which counts entries) counts records
 * poison URL TOPIC [denied]        records tsdb must refuse; prints how many; denied adds one on a series outside
 *                                  the sink user's grant (insert prefix:pump), which tsdbd answers DENIED
 * verify HOST PORT N [USER PWFILE] every key present once with its last value; exit 1 otherwise; logs in as USER
 *                                  with the password in PWFILE (the superuser: the sink's user cannot read)
 * dlq URL TOPIC EXPECTED SECONDS   counts messages on the dead-letter topic; exit 1 unless EXPECTED arrive
 * tenants URL TOPIC N              N points on each of the series acme.s0-s9 and globex.s0-s9 (the selection case)
 * verify-tenants HOST PORT N VOLUME [USER PWFILE]
 *                                  every acme.* point present, each acme.* series in volume number VOLUME, no
 *                                  globex.* series at all (the selecting sink acknowledged and dropped them)
 * </pre>
 */
public final class IntegrationDriver {
    static final int SERIES = 500;
    static final long T0 = 1_700_000_000_000L;

    static int key(int i, int n) {
        int m = n - n / 8;
        return i < m ? i / 2 : (i - m) / 2;
    }

    static String series(int k) {
        return "pump" + (k % SERIES);
    }

    static long ts(int k) {
        return T0 + (long) (k / SERIES) * 1000;
    }

    public static void main(String[] a) throws Exception {
        switch (a[0]) {
        case "produce":
            produce(a[1], a[2], Integer.parseInt(a[3]), Integer.parseInt(a[4]), Integer.parseInt(a[5]),
                    a.length > 6 && a[6].equals("unbatched"));
            break;
        case "poison":
            poison(a[1], a[2], a.length > 3 && a[3].equals("denied"));
            break;
        case "verify":
            ConnectOptions o = ConnectOptions.defaults().connectTimeout(Duration.ofSeconds(5));
            if (a.length > 5)
                o = o.user(a[4]).passwordFile(Path.of(a[5])).requireAuth(true);
            System.exit(verify(a[1], Integer.parseInt(a[2]), Integer.parseInt(a[3]), o) ? 0 : 1);
            break;
        case "tenants":
            tenants(a[1], a[2], Integer.parseInt(a[3]));
            break;
        case "verify-tenants":
            ConnectOptions to = ConnectOptions.defaults().connectTimeout(Duration.ofSeconds(5));
            if (a.length > 6)
                to = to.user(a[5]).passwordFile(Path.of(a[6])).requireAuth(true);
            System.exit(verifyTenants(a[1], Integer.parseInt(a[2]), Integer.parseInt(a[3]), Integer.parseInt(a[4]),
                    to) ? 0 : 1);
            break;
        case "dlq":
            System.exit(dlq(a[1], a[2], Integer.parseInt(a[3]), Integer.parseInt(a[4])) ? 0 : 1);
            break;
        default:
            throw new IllegalArgumentException(a[0]);
        }
    }

    static PulsarClient client(String url) throws Exception {
        return PulsarClient.builder().serviceUrl(url).build();
    }

    static void send(Producer<byte[]> p, ArrayDeque<CompletableFuture<?>> window, String key, String json) {
        while (window.size() >= 1000)
            window.poll().join();
        window.add(p.async().newMessage().key(key).value(json.getBytes(StandardCharsets.UTF_8)).send());
    }

    static Producer<byte[]> producer(PulsarClient c, String topic, boolean unbatched) throws Exception {
        var b = c.newProducer(Schema.bytes()).topic(topic);
        return unbatched ? b.batchingPolicy(BatchingPolicy.ofDisabled()).create() : b.create();
    }

    static void produce(String url, String topic, int n, int from, int to, boolean unbatched) throws Exception {
        long t = System.nanoTime();
        try (PulsarClient c = client(url); Producer<byte[]> p = producer(c, topic, unbatched)) {
            ArrayDeque<CompletableFuture<?>> window = new ArrayDeque<>();
            for (int i = from; i < to; i++) {
                int k = key(i, n);
                send(p, window, series(k), "{\"series\":\"" + series(k) + "\",\"ts\":" + ts(k) + ",\"value\":" + i + "}");
            }
            while (!window.isEmpty())
                window.poll().join();
        }
        System.out.printf("produced [%d, %d) in %.1f s%n", from, to, (System.nanoTime() - t) / 1e9);
    }

    static void poison(String url, String topic, boolean denied) throws Exception {
        String[] bad = {
            "{\"series\":\"pump3\",\"ts\":" + T0 + ",\"value\":\"high\"}", /* a string into a float64 series */
            "{\"series\":\"pump4\",\"ts\":" + T0 + ",\"value\":1,\"type\":\"int64\"}", /* pump4 is float64 */
            "{\"series\":\"" + "n".repeat(40_000) + "\",\"ts\":" + T0 + ",\"value\":1}", /* name too long */
            "{not json",
            "{\"series\":\"boiler1\",\"ts\":" + T0 + ",\"value\":1}", /* outside the grant: DENIED, never retried */
        };
        if (!denied)
            bad = Arrays.copyOf(bad, bad.length - 1);
        try (PulsarClient c = client(url); Producer<byte[]> p = c.newProducer(Schema.bytes()).topic(topic).create()) {
            ArrayDeque<CompletableFuture<?>> window = new ArrayDeque<>();
            for (int i = 0; i < bad.length; i++)
                send(p, window, i < 2 ? "pump" + (3 + i) : "poison" + i, bad[i]);
            while (!window.isEmpty())
                window.poll().join();
        }
        System.out.println(bad.length);
    }

    static boolean verify(String host, int port, int n, ConnectOptions options) throws TsdbException {
        Map<Long, Integer> want = new HashMap<>(); /* (series index, ts) -> last value */
        for (int i = 0; i < n; i++) {
            int k = key(i, n);
            want.merge((long) (k % SERIES) << 40 | (k / SERIES), i, Math::max);
        }
        int missing = 0, wrong = 0, extra = 0;
        long rows = 0;
        String example = null;
        try (TsdbClient c = TsdbClient.connect(host, port, options)) {
            for (int s = 0; s < SERIES; s++) {
                ValuePoints v;
                try {
                    v = c.queryValues(c.series("pump" + s, false), Long.MIN_VALUE, Long.MAX_VALUE);
                } catch (TsdbException e) {
                    continue; /* not created yet: all its keys are missing */
                }
                rows += v.size();
                for (int i = 0; i < v.size(); i++) {
                    long key = (long) s << 40 | ((v.timestamps()[i] - T0) / 1000);
                    Integer w = want.get(key);
                    double got = v.values().isNull(i) ? Double.NaN : v.values().doubles()[i];
                    if (w == null)
                        extra++;
                    else if (got != w) {
                        wrong++;
                        if (example == null)
                            example = "pump" + s + "@" + v.timestamps()[i] + " = " + got + ", want " + w;
                    }
                }
            }
        }
        missing = (int) Math.max(0, want.size() - (rows - extra));
        boolean ok = missing == 0 && wrong == 0 && extra == 0 && rows == want.size();
        System.out.printf("verify: %s keys=%d rows=%d missing=%d wrong=%d extra=%d%s%n", ok ? "OK" : "NOT YET",
                want.size(), rows, missing, wrong, extra, example == null ? "" : " e.g. " + example);
        return ok;
    }

    static final String[] TENANTS = {"acme.", "globex."};

    static void tenants(String url, String topic, int n) throws Exception {
        try (PulsarClient c = client(url); Producer<byte[]> p = producer(c, topic, false)) {
            ArrayDeque<CompletableFuture<?>> window = new ArrayDeque<>();
            for (int i = 0; i < n; i++)
                for (String t : TENANTS)
                    for (int s = 0; s < 10; s++)
                        send(p, window, t + "s" + s, "{\"series\":\"" + t + "s" + s + "\",\"ts\":" + (T0 + i * 1000L)
                                + ",\"value\":" + i + "}");
            while (!window.isEmpty())
                window.poll().join();
        }
        System.out.println("produced " + n + " points on each of 20 series");
    }

    static boolean verifyTenants(String host, int port, int n, int volume, ConnectOptions options)
            throws TsdbException {
        int rows = 0, wrong = 0, misplaced = 0, missing = 0, present = 0;
        try (TsdbClient c = TsdbClient.connect(host, port, options)) {
            for (int s = 0; s < 10; s++) {
                long id;
                try {
                    id = c.series("acme.s" + s, false);
                } catch (TsdbException e) {
                    missing++;
                    continue;
                }
                if ((id >>> 32) - 1 != volume)
                    misplaced++;
                ValuePoints v = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE);
                rows += v.size();
                for (int i = 0; i < v.size(); i++)
                    if (v.timestamps()[i] != T0 + i * 1000L || v.values().doubles()[i] != i)
                        wrong++;
                try {
                    c.series("globex.s" + s, false);
                    present++;
                } catch (TsdbException e) {
                    if (e.status() != Status.ERR_NOTFOUND)
                        throw e;
                }
            }
        }
        boolean ok = missing == 0 && rows == 10 * n && wrong == 0 && misplaced == 0 && present == 0;
        System.out.printf("verify-tenants: %s acme rows=%d of %d, series missing=%d, wrong=%d, not in volume %d=%d; "
                + "globex series present=%d%n", ok ? "OK" : "NOT YET", rows, 10 * n, missing, wrong, volume, misplaced,
                present);
        return ok;
    }

    static boolean dlq(String url, String topic, int expected, int seconds) throws Exception {
        int got = 0;
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        try (PulsarClient c = client(url);
             StreamConsumer<byte[]> s = c.newStreamConsumer(Schema.bytes()).topic(topic).subscriptionName("it-check")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST).subscribe()) {
            while (got < expected && System.nanoTime() < end) {
                Message<byte[]> m = s.receive(Duration.ofSeconds(1));
                if (m == null)
                    continue;
                got++;
                String body = new String(m.data(), StandardCharsets.UTF_8);
                System.out.println("dead letter: " + (body.length() > 80 ? body.substring(0, 80) + "..." : body)
                        + " " + m.properties());
                s.acknowledgeCumulative(m.id());
            }
        }
        System.out.println("dead letters: " + got + " of " + expected);
        return got == expected;
    }
}
