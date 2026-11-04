package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Which series a sink instance takes, and the volume a new one is created in. */
class SeriesSelectionTest {
    private static SeriesSelection sel(List<String> series, List<String> prefixes, String pattern) {
        return new SeriesSelection(series, prefixes, pattern, Map.of());
    }

    @Test
    void nothingConfiguredTakesAll() {
        SeriesSelection s = sel(null, null, null);
        assertTrue(s.isAll());
        assertTrue(s.matches("anything"));
        assertTrue(sel(List.of(), List.of(), "").matches("x"));
        assertTrue(TsdbSinkConfig.load(Map.of()).selection().isAll());
    }

    @Test
    void exactNamesOnly() {
        SeriesSelection s = sel(List.of("pump7.flow", "pump7.temp"), null, null);
        assertFalse(s.isAll());
        assertTrue(s.matches("pump7.flow"));
        assertTrue(s.matches("pump7.temp"));
        assertFalse(s.matches("pump7.flow2"));
        assertFalse(s.matches("pump7"));
        assertFalse(s.matches("PUMP7.FLOW"));
    }

    @Test
    void prefixes() {
        SeriesSelection s = sel(null, List.of("acme.", "globex/"), null);
        assertTrue(s.matches("acme.pump7.flow"));
        assertTrue(s.matches("acme."));
        assertTrue(s.matches("globex/a"));
        assertFalse(s.matches("acme"));
        assertFalse(s.matches("xacme.pump"));
        assertFalse(s.matches("initech.pump"));
    }

    @Test
    void patternMustMatchTheWholeName() {
        SeriesSelection s = sel(null, null, "pump[0-9]+\\.flow");
        assertTrue(s.matches("pump7.flow"));
        assertTrue(s.matches("pump123.flow"));
        assertFalse(s.matches("pump7.flow.raw")); /* a prefix of the name matching is not enough */
        assertFalse(s.matches("big-pump7.flow"));
        assertFalse(s.matches("pump.flow"));
    }

    @Test
    void theThreeAddUp() {
        SeriesSelection s = sel(List.of("solo"), List.of("acme."), ".*\\.temp");
        assertTrue(s.matches("solo"));
        assertTrue(s.matches("acme.x"));
        assertTrue(s.matches("globex.temp"));
        assertFalse(s.matches("globex.flow"));
        assertFalse(s.matches("solo2"));
    }

    @Test
    void longestPrefixChoosesTheVolume() {
        SeriesSelection s = new SeriesSelection(null, null, null,
                Map.of("acme.", "/vol/acme", "acme.big.", "/vol/big", "acme.big.exact", "/vol/exact", "globex.", "/vol/g"));
        assertEquals("/vol/acme", s.volumeFor("acme.small.x"));
        assertEquals("/vol/big", s.volumeFor("acme.big.x"));
        assertEquals("/vol/exact", s.volumeFor("acme.big.exact"));
        assertEquals("/vol/exact", s.volumeFor("acme.big.exact-and-more"));
        assertEquals("/vol/big", s.volumeFor("acme.big.exac"));
        assertEquals("/vol/g", s.volumeFor("globex.a"));
        assertNull(s.volumeFor("initech.a"), "unmapped names go to the database directory");
        assertNull(s.volumeFor("acme"));
    }

    @Test
    void volumeDoesNotDependOnTheSelection() {
        SeriesSelection s = new SeriesSelection(null, List.of("acme."), null, Map.of("acme.", "/vol/acme"));
        assertEquals("/vol/acme", s.volumeFor("acme.x"));
        assertFalse(s.matches("other.x"));
        assertNull(s.volumeFor("other.x"));
    }

    @Test
    void badConfigurationIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> sel(List.of(""), null, null));
        assertThrows(IllegalArgumentException.class, () -> sel(null, List.of(""), null));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> sel(null, null, "(unclosed"));
        assertTrue(e.getMessage().contains("seriesPattern"), e.getMessage());
        for (String path : new String[] {"relative/vol", "", "/a/../b", "/a/./b", "/a\nb"})
            assertThrows(IllegalArgumentException.class,
                    () -> new SeriesSelection(null, null, null, Map.of("acme.", path)), path);
        assertThrows(IllegalArgumentException.class, () -> new SeriesSelection(null, null, null, Map.of("", "/v")));
    }

    @Test
    void readFromTheSinkConfiguration() {
        TsdbSinkConfig c = TsdbSinkConfig.load(Map.of(
                "series", List.of("a", "b"),
                "seriesPrefixes", "acme.", /* a single value is a list of one */
                "seriesPattern", "x[0-9]",
                "volumes", Map.of("acme.", "/vol/acme", "acme.hot.", "/vol/hot")));
        SeriesSelection s = c.selection();
        assertTrue(s.matches("a") && s.matches("acme.q") && s.matches("x5"));
        assertFalse(s.matches("c"));
        assertEquals("/vol/hot", s.volumeFor("acme.hot.t"));
        assertEquals("/vol/acme", s.volumeFor("acme.cold"));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("seriesPattern", "(")));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("volumes", Map.of("a", "rel"))));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("seriesPrefix", List.of("a"))));
    }
}
