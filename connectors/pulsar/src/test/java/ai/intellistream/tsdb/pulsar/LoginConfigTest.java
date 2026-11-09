// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The login options: the password from Pulsar's secrets only, and setup mistakes refused at open. */
class LoginConfigTest {
    @TempDir
    Path tmp;

    /** A sink context whose secrets are {@code secrets}. */
    private static SinkContext secrets(Map<String, String> secrets) {
        return (SinkContext) Proxy.newProxyInstance(SinkContext.class.getClassLoader(), new Class<?>[] {SinkContext.class},
                (proxy, m, args) -> m.getName().equals("getSecret") ? secrets.get((String) args[0]) : null);
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    @Test
    void thePasswordComesFromTheSecrets() {
        TsdbSinkConfig c = TsdbSinkConfig.load(map("user", "acme/ingest", "connections", 2),
                secrets(Map.of("password", "s3cret-pw")));
        assertEquals("acme/ingest", c.user);
        assertEquals("s3cret-pw", c.password);
        assertEquals(2, c.connections);
        assertFalse(c.connectOptions().toString().contains("s3cret-pw"), "the password in toString()");
    }

    /**
     * The selection read through Pulsar's IOConfigUtils (its own mapper), as the runtime does: a single prefix is a
     * list of one there too, and the volumes and the pattern come through beside the secret.
     */
    @Test
    void theSelectionComesThroughWithTheSecrets() {
        TsdbSinkConfig c = TsdbSinkConfig.load(map("user", "acme-sink", "seriesPrefixes", "acme.", "series",
                List.of("pump7.flow"), "seriesPattern", "x[0-9]", "volumes", Map.of("acme.", "/vol/acme")),
                secrets(Map.of("password", "pw-1")));
        assertEquals("pw-1", c.password);
        SeriesSelection s = c.selection();
        assertTrue(s.matches("acme.a") && s.matches("pump7.flow") && s.matches("x3"));
        assertFalse(s.matches("globex.a"));
        assertEquals("/vol/acme", s.volumeFor("acme.a"));
        assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("seriesPrefix", "acme."), secrets(Map.of())));
    }

    /**
     * "acme.,globex." written like permanentCodes was one prefix that matched nothing: the sink started and dropped
     * every message. Refused through both loaders; a JSON list (which may hold a name with a comma) still works.
     */
    @Test
    void aCommaSeparatedStringIsRefusedForTheSelection() {
        for (String k : List.of("seriesPrefixes", "series")) {
            for (SinkContext ctx : java.util.Arrays.asList(null, secrets(Map.of()))) {
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> TsdbSinkConfig.load(map(k, "acme.,globex."), ctx), k);
                assertTrue(e.getMessage().contains(k) && e.getMessage().contains("JSON list"), e.getMessage());
                SeriesSelection s = TsdbSinkConfig.load(map(k, List.of("acme.", "globex.")), ctx).selection();
                assertTrue(s.matches(k.equals("series") ? "globex." : "globex.x"), k);
                assertTrue(TsdbSinkConfig.load(map(k, List.of("a,b")), ctx).selection().matches("a,b"), k);
            }
        }
    }

    @Test
    void aPasswordInThePlainConfigIsRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("user", "u", "password", "pw"), secrets(Map.of())));
        assertTrue(e.getMessage().contains("secrets"), e.getMessage());
        assertFalse(e.getMessage().contains("pw\""), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(map("user", "u", "password", "pw")));
    }

    @Test
    void unknownKeysStillFailThroughTheSecretsLoader() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("hostz", "x"), secrets(Map.of())));
        assertTrue(e.getMessage().contains("hostz"), e.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("connections", 0), secrets(Map.of())));
    }

    /** Pulsar's loader fills absent options from @FieldDoc defaultValue: they must be the Java defaults. */
    @Test
    void theSecretsLoaderKeepsEveryDefault() throws Exception {
        TsdbSinkConfig viaSecrets = TsdbSinkConfig.load(Map.of(), secrets(Map.of()));
        TsdbSinkConfig plain = new TsdbSinkConfig();
        for (Field f : TsdbSinkConfig.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers()))
                continue;
            assertEquals(f.get(plain), f.get(viaSecrets), f.getName());
        }
    }

    @Test
    void loginMistakesFailAtOpen() throws Exception {
        Path pw = Files.writeString(tmp.resolve("pw"), "from-a-file\n");
        SinkContext none = secrets(Map.of());
        SinkContext secret = secrets(Map.of("password", "pw-1"));
        /* a user needs one password, a password needs a user */
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(map("user", "u"), none));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(map(), secret));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(map("passwordFile", pw.toString())));
        assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("user", "u", "passwordFile", pw.toString()), secret));
        /* names SCRAM cannot carry, passwords outside the one rule */
        for (String bad : new String[] {"a,b", "a=b", "a\0b", "x".repeat(256)})
            assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(map("user", bad), secret), bad);
        assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("user", "u"), secrets(Map.of("password", "a\0b"))));
        assertThrows(IllegalArgumentException.class,
                () -> TsdbSinkConfig.load(map("user", "u"), secrets(Map.of("password", "p".repeat(1024)))));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(
                map("user", "u", "passwordFile", tmp.resolve("missing").toString())));
        /* and the good ones */
        assertEquals(pw.toString(), TsdbSinkConfig.load(map("user", "u", "passwordFile", pw.toString())).passwordFile);
        assertEquals("p".repeat(1023),
                TsdbSinkConfig.load(map("user", "u"), secrets(Map.of("password", "p".repeat(1023)))).password);
        assertEquals("x".repeat(255), TsdbSinkConfig.load(map("user", "x".repeat(255)), secret).user);
    }
}
