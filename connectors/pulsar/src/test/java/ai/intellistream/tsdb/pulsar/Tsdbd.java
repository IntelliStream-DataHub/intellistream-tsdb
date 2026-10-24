package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import java.io.IOException;
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

    /** Starts on a port tsdbd chooses ({@code --port 0}), which it reports in a port file. */
    static Tsdbd start(Path dir) throws IOException, InterruptedException {
        Tsdbd t = tryStart(dir, 0);
        if (t == null)
            throw new IllegalStateException("tsdbd did not start; see " + log(dir));
        return t;
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

    /** Starts on a port tsdbd chooses, with extra options, such as {@code --read-only}. */
    static Tsdbd start(Path dir, String... options) throws IOException, InterruptedException {
        Tsdbd t = tryStart(dir, 0, options);
        if (t == null)
            throw new IllegalStateException("tsdbd did not start; see " + log(dir));
        return t;
    }

    private static Path log(Path dir) {
        return dir.resolveSibling(dir.getFileName() + ".log");
    }

    /**
     * Starts tsdbd on {@code port} (0: any free one) with a port file, "PORT PID" once it serves. The pid must be
     * the process started here: a test never talks to a server it did not start. Null when it exited (a taken port).
     */
    private static Tsdbd tryStart(Path dir, int port, String... options) throws IOException, InterruptedException {
        Path bin = binary();
        if (!Files.isExecutable(bin))
            throw new IllegalStateException(bin + " is missing: run make in the repository root first");
        Path log = log(dir);
        Path portFile = dir.resolveSibling(dir.getFileName() + ".port");
        Files.deleteIfExists(portFile);
        List<String> cmd = new ArrayList<>(List.of(bin.toString(), "--dir", dir.toString(), "--listen", "127.0.0.1",
                "--port", Integer.toString(port), "--port-file", portFile.toString(), "--workers", "2"));
        cmd.addAll(List.of(options));
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                .start();
        for (int i = 0; i < 1500 && p.isAlive(); i++) {
            if (Files.exists(portFile)) {
                String[] f = Files.readString(portFile, StandardCharsets.UTF_8).trim().split("\\s+");
                if (f.length == 2) {
                    Files.deleteIfExists(portFile);
                    if (Long.parseLong(f[1]) != p.pid()) {
                        p.destroyForcibly().waitFor();
                        throw new IllegalStateException(portFile + " names pid " + f[1]
                                + ", but the server started here is pid " + p.pid());
                    }
                    return new Tsdbd(dir, Integer.parseInt(f[0]), p);
                }
            }
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
