package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import ai.intellistream.tsdb.example.Example;
import org.junit.jupiter.api.Test;

/** The README's quick start is the example class, and the example runs against a live server. */
class ExampleTest {
    static final Path BASE = Paths.get(System.getProperty("basedir", "."));

    @Test
    void readmeQuickStartIsTheExampleClass() throws Exception {
        String readme = Files.readString(BASE.resolve("README.md"), StandardCharsets.UTF_8);
        assertTrue(readme.startsWith("# "), "README starts with its title");
        int section = readme.indexOf("\n## ");
        assertTrue(readme.startsWith("## Quick start", section + 1), "the first section is the quick start");
        int open = readme.indexOf("```java\n", section);
        int close = readme.indexOf("\n```\n", open);
        assertTrue(open > 0 && close > open, "the quick start has a java code block");
        String block = readme.substring(open + "```java\n".length(), close + 1);
        String source = Files.readString(BASE.resolve("src/main/java/ai/intellistream/tsdb/example/Example.java"),
                StandardCharsets.UTF_8);
        assertEquals(source, block, "README.md's quick start must be Example.java, verbatim");
    }

    @Test
    void exampleRuns() throws Exception {
        Path scratch = TsdbServer.scratch("tsdbd-java-example-");
        try (TsdbServer s = TsdbServer.start(scratch.resolve("db"))) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            PrintStream old = System.out;
            Locale locale = Locale.getDefault();
            System.setOut(new PrintStream(buf, true, StandardCharsets.UTF_8));
            Locale.setDefault(Locale.ROOT); /* decimal points */
            try {
                Example.main(new String[] {"127.0.0.1", Integer.toString(s.port)});
            } finally {
                System.setOut(old);
                Locale.setDefault(locale);
            }
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
            assertEquals(expected, buf.toString(StandardCharsets.UTF_8));
            assertEquals(0, s.stop(), s.log());
        } finally {
            TsdbServer.delete(scratch);
        }
    }
}
