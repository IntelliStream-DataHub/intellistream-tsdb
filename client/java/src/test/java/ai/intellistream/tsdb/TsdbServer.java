package ai.intellistream.tsdb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Runs build/tsdbd for the tests: on a free port of 127.0.0.1, on a given directory, stopped with SIGTERM.
 *
 * <p>System properties: {@code tsdbd} is the binary (default ../../build/tsdbd), {@code tsdb.test.dir} the parent of
 * the scratch directories (default java.io.tmpdir).
 */
final class TsdbServer implements AutoCloseable {
    final Path dir;
    final int port;
    private final Process process;
    private final Path log;

    private TsdbServer(Path dir, int port, Process process, Path log) {
        this.dir = dir;
        this.port = port;
        this.process = process;
        this.log = log;
    }

    static Path binary() {
        return Paths.get(System.getProperty("tsdbd", "../../build/tsdbd")).toAbsolutePath().normalize();
    }

    /** A fresh scratch directory for one test class. */
    static Path scratch(String prefix) throws IOException {
        Path base = Paths.get(System.getProperty("tsdb.test.dir", System.getProperty("java.io.tmpdir")));
        Files.createDirectories(base);
        return Files.createTempDirectory(base, prefix);
    }

    /** Starts the server on {@code dir}, retrying with another port if the chosen one was taken meanwhile. */
    static TsdbServer start(Path dir) throws IOException, InterruptedException {
        Path bin = binary();
        if (!Files.isExecutable(bin))
            throw new IllegalStateException(bin + " is missing: run make in the repository root first");
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                port = s.getLocalPort();
            }
            Path log = dir.resolveSibling(dir.getFileName() + ".log");
            int seen = Files.exists(log) ? (int) Files.size(log) : 0; /* earlier runs on the same directory */
            Process p = new ProcessBuilder(bin.toString(), "--dir", dir.toString(), "--port", Integer.toString(port),
                    "--no-fsync", "--workers", "4")
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                    .start();
            /* Wait for its own "serving" line rather than for the port to accept: another process (say, a
             * server of another test run) may have taken the port, and then this one exits instead. */
            String ready = "on 127.0.0.1:" + port + ",";
            for (int i = 0; i < 500 && p.isAlive(); i++) {
                if (Files.exists(log) && Files.readString(log, StandardCharsets.UTF_8).indexOf(ready, seen) >= 0)
                    return new TsdbServer(dir, port, p, log);
                Thread.sleep(20);
            }
            p.destroyForcibly().waitFor();
        }
        throw new IllegalStateException("tsdbd did not start; see " + dir.resolveSibling(dir.getFileName() + ".log"));
    }

    TsdbClient client() throws TsdbException {
        return TsdbClient.connect("127.0.0.1", port);
    }

    /** Stops the server with SIGTERM and returns its exit code. */
    int stop() throws InterruptedException {
        process.destroy(); /* SIGTERM */
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
            process.destroyForcibly().waitFor();
            return -1;
        }
        return process.exitValue();
    }

    String log() throws IOException {
        return Files.exists(log) ? Files.readString(log, StandardCharsets.UTF_8) : "";
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            try {
                stop();
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    static void delete(Path p) throws IOException {
        if (!Files.exists(p))
            return;
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
        }
    }
}
