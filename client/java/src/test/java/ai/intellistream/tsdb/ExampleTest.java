package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import ai.intellistream.tsdb.example.Example;
import ai.intellistream.tsdb.example.TypedExample;
import org.junit.jupiter.api.Test;

/** The README's quick start is the example class, and the examples run against a live server. */
class ExampleTest {
    static final Path BASE = Paths.get(System.getProperty("basedir", "."));

    @Test
    void readmeQuickStartIsTheExampleClasses() throws Exception {
        String readme = Files.readString(BASE.resolve("README.md"), StandardCharsets.UTF_8);
        assertTrue(readme.startsWith("# "), "README starts with its title");
        int section = readme.indexOf("\n## ");
        assertTrue(readme.startsWith("## Quick start", section + 1), "the first section is the quick start");
        int next = readme.indexOf("\n## ", section + 1);
        String quick = readme.substring(section, next < 0 ? readme.length() : next);
        List<String> blocks = new ArrayList<>();
        for (int open = quick.indexOf("```java\n"); open >= 0; open = quick.indexOf("```java\n", open + 1)) {
            int close = quick.indexOf("\n```\n", open);
            blocks.add(quick.substring(open + "```java\n".length(), close + 1));
        }
        String dir = "src/main/java/ai/intellistream/tsdb/example/";
        assertEquals(List.of(Files.readString(BASE.resolve(dir + "Example.java"), StandardCharsets.UTF_8)), blocks,
                "README.md's quick start must hold Example.java, verbatim");
    }

    /**
     * The README's other Java snippets compile: each becomes a method of one class, whose fields stand for the
     * variables the snippets share ({@code client}, {@code flow}, ...).
     */
    @Test
    void readmeSnippetsCompile() throws Exception {
        String readme = Files.readString(BASE.resolve("README.md"), StandardCharsets.UTF_8);
        int after = readme.indexOf("\n## ", readme.indexOf("\n## Quick start") + 1);
        StringBuilder src = new StringBuilder("import ai.intellistream.tsdb.*;\nimport java.time.*;\n"
                + "import java.util.*;\nclass ReadmeSnippets {\n    static TsdbClient client;\n"
                + "    static int flow, energy;\n    static long t0;\n");
        int n = 0;
        for (int open = readme.indexOf("```java\n", after); open >= 0; open = readme.indexOf("```java\n", open + 1)) {
            int close = readme.indexOf("\n```\n", open);
            src.append("    static void snippet").append(n++).append("() throws Exception {\n")
                    .append(readme, open + "```java\n".length(), close + 1).append("    }\n");
        }
        src.append("}\n");
        assertTrue(n >= 8, n + " snippets");
        Path dir = TsdbServer.scratch("tsdb-readme-");
        try {
            Files.writeString(dir.resolve("ReadmeSnippets.java"), src);
            javax.tools.JavaCompiler javac = javax.tools.ToolProvider.getSystemJavaCompiler();
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            int rc = javac.run(null, null, err, "-d", dir.toString(), "-cp", System.getProperty("java.class.path"),
                    dir.resolve("ReadmeSnippets.java").toString());
            assertEquals(0, rc, () -> err.toString(StandardCharsets.UTF_8) + "\n" + src);
        } finally {
            TsdbServer.delete(dir);
        }
    }

    /** Runs an example's main against the server and returns what it printed. */
    static String run(TsdbServer s, ThrowingMain main) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream old = System.out;
        Locale locale = Locale.getDefault();
        System.setOut(new PrintStream(buf, true, StandardCharsets.UTF_8));
        Locale.setDefault(Locale.ROOT); /* decimal points */
        try {
            main.run(new String[] {"127.0.0.1", Integer.toString(s.port)});
        } finally {
            System.setOut(old);
            Locale.setDefault(locale);
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    interface ThrowingMain {
        void run(String[] args) throws Exception;
    }

    @Test
    void exampleRuns() throws Exception {
        Path scratch = TsdbServer.scratch("tsdbd-java-example-");
        try (TsdbServer s = TsdbServer.start(scratch.resolve("db"))) {
            String expected = String.join(System.lineSeparator(),
                    "2024-03-01T23:59:00Z 45.9",
                    "2024-03-01T23:58:00Z 45.8",
                    "2024-03-01T23:57:00Z 45.7",
                    "2024-03-01T00:00:00Z count 360 avg 42.95 min 40.0 max 45.9",
                    "2024-03-01T06:00:00Z count 360 avg 42.95 min 40.0 max 45.9",
                    "2024-03-01T12:00:00Z count 360 avg 42.95 min 40.0 max 45.9",
                    "2024-03-01T18:00:00Z count 360 avg 42.95 min 40.0 max 45.9",
                    "Oslo day from 2024-02-29T23:00:00Z: 1380 points",
                    "Oslo day from 2024-03-01T23:00:00Z: 60 points",
                    "");
            assertEquals(expected, run(s, Example::main));
            assertEquals(String.join(System.lineSeparator(),
                    "2024-03-01T00:00:00Z 1000",
                    "2024-03-01T00:01:00Z 1250",
                    "2024-03-01T00:02:00Z null",
                    "2024-03-01T00:03:00Z 1900",
                    "known 3, sum 4150, max 1900",
                    "running running stopped running",
                    ""), run(s, TypedExample::main));
            assertEquals(0, s.stop(), s.log());
        } finally {
            TsdbServer.delete(scratch);
        }
    }
}
