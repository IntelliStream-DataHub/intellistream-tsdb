package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Runs build/tsdbd for the tests on 127.0.0.1, in its default durable mode (fsync, sync WAL): what the sink
 * acknowledges must survive {@link #kill}. Restarts can reuse the port, so the sink reconnects to the same address.
 *
 * <p>System properties: {@code tsdbd} is the binary (default ../../build/tsdbd), {@code tsdb.test.dir} the parent of
 * the scratch directories (default java.io.tmpdir).
 */
final class Tsdbd implements AutoCloseable {
    final Path dir;
    final int port;
    private final Process process;

    private Tsdbd(Path dir, int port, Process process) {
        this.dir = dir;
        this.port = port;
        this.process = process;
    }

    static Path binary() {
        return Paths.get(System.getProperty("tsdbd", "../../build/tsdbd")).toAbsolutePath().normalize();
    }

    static Path scratch(String prefix) throws IOException {
        Path base = Paths.get(System.getProperty("tsdb.test.dir", System.getProperty("java.io.tmpdir")));
        Files.createDirectories(base);
        return Files.createTempDirectory(base, prefix);
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        }
    }

    /** Starts on a free port, retrying with another one if it was taken meanwhile. */
    static Tsdbd start(Path dir) throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 5; attempt++) {
            Tsdbd t = tryStart(dir, freePort());
            if (t != null)
                return t;
        }
        throw new IllegalStateException("tsdbd did not start; see " + log(dir));
    }

    /** Starts on {@code port}; a restart after {@link #kill} may find it briefly still taken, so it retries. */
    static Tsdbd start(Path dir, int port) throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 50; attempt++) {
            Tsdbd t = tryStart(dir, port);
            if (t != null)
                return t;
            Thread.sleep(100);
        }
        throw new IllegalStateException("tsdbd did not start on port " + port + "; see " + log(dir));
    }

    /** Starts on a free port with extra options, such as {@code --read-only}. */
    static Tsdbd start(Path dir, String... options) throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 5; attempt++) {
            Tsdbd t = tryStart(dir, freePort(), options);
            if (t != null)
                return t;
        }
        throw new IllegalStateException("tsdbd did not start; see " + log(dir));
    }

    private static Path log(Path dir) {
        return dir.resolveSibling(dir.getFileName() + ".log");
    }

    private static Tsdbd tryStart(Path dir, int port, String... options) throws IOException, InterruptedException {
        Path bin = binary();
        if (!Files.isExecutable(bin))
            throw new IllegalStateException(bin + " is missing: run make in the repository root first");
        Path log = log(dir);
        int seen = Files.exists(log) ? (int) Files.size(log) : 0; /* earlier runs on the same directory */
        List<String> cmd = new ArrayList<>(List.of(bin.toString(), "--dir", dir.toString(), "--listen", "127.0.0.1",
                "--port", Integer.toString(port), "--workers", "2"));
        cmd.addAll(List.of(options));
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                .start();
        /* its own "serving" line, not an accepting port: another process may hold the port, and then this one exits */
        String ready = "on 127.0.0.1:" + port + ",";
        for (int i = 0; i < 1000 && p.isAlive(); i++) {
            if (Files.exists(log) && Files.readString(log, StandardCharsets.UTF_8).indexOf(ready, seen) >= 0)
                return new Tsdbd(dir, port, p);
            Thread.sleep(20);
        }
        p.destroyForcibly().waitFor();
        return null;
    }

    TsdbClient client() throws TsdbException {
        return TsdbClient.connect("127.0.0.1", port);
    }

    /** SIGKILL: no flush, no clean shutdown. Only this process, through its handle (never a pid we computed). */
    void kill() throws InterruptedException {
        process.destroyForcibly().waitFor();
    }

    /** SIGTERM and wait. */
    void stop() throws InterruptedException {
        process.destroy();
        if (!process.waitFor(60, TimeUnit.SECONDS))
            process.destroyForcibly().waitFor();
    }

    @Override
    public void close() throws InterruptedException {
        if (process.isAlive())
            stop();
    }

    static void delete(Path p) throws IOException {
        if (!Files.exists(p))
            return;
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
        }
        Files.deleteIfExists(log(p));
    }
}
