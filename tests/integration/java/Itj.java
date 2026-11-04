import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The Java client of the integration tests: same command line and output as c/itc.c, rust/ and python/itp.py
 * (see run.py). */
public final class Itj {
    record Series(long id, ValueType type) {}

    static TsdbClient c;
    static final Map<String, Series> series = new HashMap<>();
    static final Map<String, String> names = new HashMap<>();
    static final PrintStream out = new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), false);

    static ValueType parseType(String s) {
        return switch (s) {
            case "float32" -> ValueType.FLOAT32;
            case "float64" -> ValueType.FLOAT64;
            case "int64" -> ValueType.INT64;
            case "bool" -> ValueType.BOOL;
            case "state" -> ValueType.STATE;
            default -> throw new IllegalArgumentException(s);
        };
    }

    static Series find(String name, ValueType create) throws TsdbException {
        Series s = series.get(name);
        if (s != null)
            return s;
        try {
            long id = create != null ? c.seriesCreate(name, create) : c.series(name, false);
            s = new Series(id, create != null ? create : c.seriesType(id));
        } catch (TsdbException e) {
            if (create != null)
                throw e;
            return null;
        }
        series.put(name, s);
        return s;
    }

    static void write(String path) throws Exception {
        List<String> lines = Files.readAllLines(Path.of(path));
        for (int i = 0; i < lines.size(); ) {
            String[] p = lines.get(i++).trim().split("\\s+");
            if (p[0].isEmpty())
                continue;
            if (p[0].equals("S")) {
                find(p[1], parseType(p[2]));
                continue;
            }
            boolean multi = p[0].equals("M");
            int level = Integer.parseInt(p[2]);
            String flags = p[3];
            int n = Integer.parseInt(p[4]);
            ValueType ty;
            long one = 0;
            if (multi)
                ty = parseType(p[1]);
            else {
                Series s = find(p[1], null);
                ty = s.type();
                one = s.id();
            }
            long[] ts = new long[n];
            long[] ids = new long[n];
            float[] f = new float[n];
            double[] d = new double[n];
            long[] l = new long[n];
            boolean[] b = new boolean[n];
            int[] st = new int[n];
            BitSet nulls = new BitSet(n);
            for (int k = 0; k < n; k++) {
                String[] r = lines.get(i++).trim().split("\\s+");
                int o = 0;
                long sid = one;
                if (multi)
                    sid = find(r[o++], null).id();
                ids[k] = sid;
                ts[k] = Long.parseLong(r[o]);
                String tok = r[o + 1];
                if (tok.equals("N")) {
                    nulls.set(k);
                    continue;
                }
                switch (ty) {
                    case FLOAT32 -> f[k] = Float.intBitsToFloat((int) Long.parseLong(tok, 16));
                    case FLOAT64 -> d[k] = Double.longBitsToDouble(Long.parseUnsignedLong(tok, 16));
                    case INT64 -> l[k] = Long.parseLong(tok);
                    case BOOL -> b[k] = tok.equals("1");
                    default -> st[k] = c.state(sid, tok);
                }
            }
            c.setCompression(level);
            boolean async = flags.contains("async");
            if (flags.contains("legacy")) {
                if (multi) {
                    if (async) c.insertAsync(ids, ts, f); else c.insert(ids, ts, f);
                } else {
                    if (async) c.insertAsync(one, ts, f); else c.insert(one, ts, f);
                }
                continue;
            }
            Column col = switch (ty) {
                case FLOAT32 -> Column.ofFloats(f);
                case FLOAT64 -> Column.ofDoubles(d);
                case INT64 -> Column.ofLongs(l);
                case BOOL -> Column.ofBools(b);
                default -> Column.ofStates(st);
            };
            if (!nulls.isEmpty())
                col = col.withNulls(nulls);
            if (multi) {
                if (async) c.insertValuesAsync(ids, ts, col); else c.insertValues(ids, ts, col);
            } else {
                if (async) c.insertSeriesValuesAsync(one, ts, col); else c.insertSeriesValues(one, ts, col);
            }
        }
        c.await();
    }

    static String hex(long v, int digits) {
        String s = Long.toHexString(v);
        return "0".repeat(Math.max(0, digits - s.length())) + s;
    }

    static void buckets(List<String> rows, List<Bucket> bs) {
        for (Bucket b : bs)
            rows.add(b.start() + "\t" + b.count() + "\t" + hex(Double.doubleToRawLongBits(b.sum()), 16) + "\t"
                    + hex(Double.doubleToRawLongBits(b.min()), 16) + "\t" + hex(Double.doubleToRawLongBits(b.max()), 16)
                    + "\t" + b.isum() + "\t" + b.imin() + "\t" + b.imax());
    }

    static String stateName(long series, int code) throws TsdbException {
        String k = series + ":" + code;
        String v = names.get(k);
        if (v == null) {
            v = c.stateName(series, code);
            names.put(k, v);
        }
        return v;
    }

    static Unit unit(String s) {
        return Unit.valueOf(s.toUpperCase());
    }

    static void read(String path) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (String line : Files.readAllLines(Path.of(path))) {
            if (line.isBlank())
                continue;
            sb.append("## ").append(line).append('\n');
            String[] q = line.split(" ");
            Series s = find(q[1], null);
            if (s == null) {
                sb.append("!!\n");
                continue;
            }
            long start = Long.parseLong(q[2]), end = Long.parseLong(q[3]);
            List<String> rows = new ArrayList<>();
            try {
                switch (q[0]) {
                    case "raw" -> {
                        ValuePoints vp = c.queryValues(s.id(), start, end, order(q[4]), Long.parseLong(q[5]));
                        Column col = vp.values();
                        long[] ts = vp.timestamps();
                        for (int i = 0; i < vp.size(); i++) {
                            String v;
                            if (col.isNull(i))
                                v = "N";
                            else
                                v = switch (col.type()) {
                                    case FLOAT32 -> hex(Float.floatToRawIntBits(col.floats()[i]) & 0xffffffffL, 8);
                                    case FLOAT64 -> hex(Double.doubleToRawLongBits(col.doubles()[i]), 16);
                                    case INT64 -> Long.toString(col.longs()[i]);
                                    case BOOL -> col.bools()[i] ? "1" : "0";
                                    default -> stateName(s.id(), col.states()[i]);
                                };
                            rows.add(ts[i] + "\t" + v);
                        }
                    }
                    case "f32raw" -> {
                        Points p = c.queryPoints(s.id(), start, end, order(q[4]), Long.parseLong(q[5]));
                        for (int i = 0; i < p.size(); i++)
                            rows.add(p.timestamp(i) + "\t" + hex(Float.floatToRawIntBits(p.value(i)) & 0xffffffffL, 8));
                    }
                    case "agg" -> buckets(rows, c.queryAgg(s.id(), start, end, Long.parseLong(q[4])));
                    case "grp" -> {
                        Group g = null;
                        if (!q[4].equals("none")) {
                            g = Group.of(unit(q[4]), Integer.parseInt(q[5]));
                            if (!q[6].equals("-")) {
                                try {
                                    g = g.in(q[6]); // an unknown zone is refused here, not by the query
                                } catch (IllegalArgumentException e) {
                                    throw new TsdbException(-4, "unknown time zone", e);
                                }
                            }
                        }
                        buckets(rows, c.queryGroup(s.id(), start, end, g, order(q[7]), Long.parseLong(q[8])));
                    }
                    case "bnd" -> {
                        String[] bs = q[6].split(",");
                        long[] bounds = new long[bs.length];
                        for (int i = 0; i < bs.length; i++)
                            bounds[i] = Long.parseLong(bs[i]);
                        buckets(rows, c.queryBounds(s.id(), start, end, bounds, order(q[4]), Long.parseLong(q[5])));
                    }
                    default -> throw new IllegalArgumentException(q[0]);
                }
            } catch (TsdbException | java.time.DateTimeException e) {
                sb.append("!!\n");
                continue;
            }
            for (String r : rows)
                sb.append(r).append('\n');
        }
        out.print(sb);
        out.flush();
    }

    static Order order(String s) {
        return s.equals("desc") ? Order.DESC : Order.ASC;
    }

    static final int LOAD_ROWS = 1000000, LOAD_BATCH = 200, LOAD_FLIGHT = 8;

    static void load(String name, long seed, int level) throws Exception {
        long id = c.seriesCreate(name, ValueType.INT64);
        c.setCompression(level);
        out.println("READY");
        out.flush();
        int nb = LOAD_ROWS / LOAD_BATCH;
        for (int pass = 1; pass <= 2; pass++) {
            int first = 0;
            for (int b = 0; b < nb; b++) {
                long[] ts = new long[LOAD_BATCH], v = new long[LOAD_BATCH];
                for (int i = 0; i < LOAD_BATCH; i++) {
                    long r = (long) b * LOAD_BATCH + i;
                    ts[i] = 1700000000000L + r * 1000;
                    v[i] = seed * 100000000L + r + (pass == 2 ? 1000000 : 0);
                }
                c.insertSeriesValuesAsync(id, ts, Column.ofLongs(v));
                if ((b + 1) % LOAD_FLIGHT == 0 || b == nb - 1) {
                    c.await();
                    out.println("ACK " + pass + " " + first + " " + b);
                    out.flush();
                    first = b + 1;
                }
            }
            if (pass == 1) { // wait for the driver: the clients start pass 2 together
                out.println("PASS1");
                out.flush();
                new BufferedReader(new java.io.InputStreamReader(System.in)).readLine();
            }
        }
        out.println("DONE");
        out.flush();
    }

    public static void main(String[] a) throws Exception {
        if (a.length < 3) {
            System.err.println("usage: Itj write|read|load|admin <port> <arg>...");
            System.exit(2);
        }
        // run.py --auth on names the user to log in as in the environment, as it does for every client program
        String user = System.getenv("ITG_USER");
        c = user == null ? TsdbClient.connect("127.0.0.1", Integer.parseInt(a[1]))
                : TsdbClient.connect("127.0.0.1", Integer.parseInt(a[1]), ConnectOptions.defaults().user(user)
                        .passwordFile(Path.of(System.getenv("ITG_PASSWORD_FILE"))).requireAuth(true));
        switch (a[0]) {
            case "write" -> write(a[2]);
            case "read" -> read(a[2]);
            case "load" -> load(a[2], Long.parseLong(a[3]), a.length > 4 ? Integer.parseInt(a[4]) : 1);
            case "admin" -> {
                if (a[2].equals("flush")) c.flush(); else c.optimize();
            }
            default -> System.exit(2);
        }
        c.await();
        c.close();
    }
}
