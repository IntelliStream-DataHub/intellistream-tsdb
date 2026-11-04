package ai.intellistream.tsdb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Runs build/tsdbd for the tests: on a port tsdbd chose on 127.0.0.1, on a given directory, stopped with SIGTERM.
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

    /**
     * Starts the server on {@code dir} on a port tsdbd chose ({@code --port 0 --port-file}). The port file holds
     * "PORT PID" once it serves; the pid must be the process started here, so a test never talks to a server it
     * did not start.
     */
    static TsdbServer start(Path dir) throws IOException, InterruptedException {
        return start(dir, false);
    }

    /**
     * Starts the server on a fresh directory whose USERS file holds superuser {@code root} with {@code rootPassword},
     * made by the CLI as an operator does; login is required.
     */
    static TsdbServer startAuth(Path dir, String rootPassword) throws IOException, InterruptedException {
        userCli(dir, rootPassword, "add", "root", "--superuser");
        return start(dir, true);
    }

    /** Runs {@code tsdb DIR user ARGS...} with the password in TSDB_PASSWORD (none if null); fails if it fails. */
    static void userCli(Path dir, String password, String... args) throws IOException, InterruptedException {
        java.util.List<String> cmd = new java.util.ArrayList<>(java.util.List.of(
                binary().resolveSibling("tsdb").toString(), dir.toString(), "user"));
        cmd.addAll(java.util.List.of(args));
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")));
        pb.environment().remove("TSDB_PASSWORD");
        pb.environment().remove("LD_PRELOAD");
        if (password != null)
            pb.environment().put("TSDB_PASSWORD", password);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0)
            throw new IllegalStateException("tsdb user " + String.join(" ", args) + ": " + out);
    }

    /** Starts the server on {@code dir}, with login required ({@code auth}) or {@code --no-auth}. */
    static TsdbServer start(Path dir, boolean auth) throws IOException, InterruptedException {
        Path bin = binary();
        if (!Files.isExecutable(bin))
            throw new IllegalStateException(bin + " is missing: run make in the repository root first");
        Path log = dir.resolveSibling(dir.getFileName() + ".log");
        Path portFile = dir.resolveSibling(dir.getFileName() + ".port");
        Files.deleteIfExists(portFile);
        java.util.List<String> cmd = new java.util.ArrayList<>(java.util.List.of(bin.toString(), "--dir",
                dir.toString(), "--port", "0", "--port-file", portFile.toString(), "--no-fsync", "--workers", "4"));
        if (!auth)
            cmd.add("--no-auth");
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
                    return new TsdbServer(dir, Integer.parseInt(f[0]), p, log);
                }
            }
            Thread.sleep(20);
        }
        boolean alive = p.isAlive();
        p.destroyForcibly().waitFor();
        throw new IllegalStateException("tsdbd " + (alive ? "wrote no port file in 30 s" : "exited before it listened")
                + "; see " + log);
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

    /**
     * Adds volume {@code path} as an operator does, with the {@code tsdb} CLI beside tsdbd ({@code volume add}):
     * tsdbd never creates a volume for a client.
     */
    void addVolume(Path path) throws IOException, InterruptedException {
        Path cli = binary().resolveSibling("tsdb");
        Process p = new ProcessBuilder(cli.toString(), dir.toString(), "volume", "add", path.toString())
                .redirectErrorStream(true)
                .start();
        String out = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (p.waitFor() != 0)
            throw new IllegalStateException("volume add " + path + ": " + out);
    }

    static void delete(Path p) throws IOException {
        if (!Files.exists(p))
            return;
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
        }
    }
}
