package ai.intellistream.tsdb.pulsar;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Which series a sink instance takes, and in which volume it creates the new ones.
 *
 * <p><b>Selection.</b> With no {@code series}, {@code seriesPrefixes} or {@code seriesPattern} configured every series
 * is taken. Otherwise a series is taken when it is in {@code series} (exact name), starts with one of
 * {@code seriesPrefixes}, or matches {@code seriesPattern} as a whole: the three are added together, not intersected.
 *
 * <p><b>Volumes.</b> {@code volumes} maps a prefix (or a whole name, which is its own longest prefix) to the path of a
 * volume; the longest matching key wins. Names no key matches have no volume: they are created in the database
 * directory.
 *
 * <p>Immutable and thread safe.
 */
final class SeriesSelection {
    private final boolean all;
    private final Set<String> names;
    private final List<String> prefixes;
    private final Pattern pattern;
    /** The keys of the volume map, longest first, so that the first match is the longest. */
    private final List<Map.Entry<String, String>> volumes;

    /**
     * Builds a selection; the arguments are the configuration values and may be {@code null} or empty.
     *
     * @param series exact names
     * @param prefixes name prefixes
     * @param pattern a Java regular expression the whole name must match, or {@code null} or empty
     * @param volumes prefix or name to volume path
     * @throws IllegalArgumentException for an empty name or prefix, an invalid pattern or a path that is not absolute
     */
    SeriesSelection(List<String> series, List<String> prefixes, String pattern, Map<String, String> volumes) {
        this.names = new HashSet<>();
        for (String s : orEmpty(series)) {
            if (s == null || s.isEmpty())
                throw new IllegalArgumentException("tsdb sink config: series holds an empty name");
            names.add(s);
        }
        this.prefixes = new ArrayList<>();
        for (String p : orEmpty(prefixes)) {
            /* an empty prefix would select everything: say "all" by leaving the selection out */
            if (p == null || p.isEmpty())
                throw new IllegalArgumentException("tsdb sink config: seriesPrefixes holds an empty prefix; to take "
                        + "all series leave series, seriesPrefixes and seriesPattern out");
            this.prefixes.add(p);
        }
        if (pattern != null && !pattern.isEmpty()) {
            try {
                this.pattern = Pattern.compile(pattern);
            } catch (java.util.regex.PatternSyntaxException e) {
                throw new IllegalArgumentException("tsdb sink config: seriesPattern is not a valid regular expression: "
                        + e.getMessage());
            }
        } else
            this.pattern = null;
        this.all = names.isEmpty() && this.prefixes.isEmpty() && this.pattern == null;
        List<Map.Entry<String, String>> v = new ArrayList<>();
        if (volumes != null)
            for (Map.Entry<String, String> e : volumes.entrySet()) {
                String k = e.getKey(), path = e.getValue();
                if (k == null || k.isEmpty())
                    throw new IllegalArgumentException("tsdb sink config: volumes holds an empty prefix");
                if (path == null || !validPath(path))
                    throw new IllegalArgumentException("tsdb sink config: volumes[" + k + "] = " + path
                            + " is not an absolute path without control characters, '.' or '..'");
                v.add(Map.entry(k, path));
            }
        /* longest key first; equal lengths cannot both match one name, so their order does not matter */
        v.sort(Comparator.comparingInt((Map.Entry<String, String> e) -> e.getKey().length()).reversed());
        this.volumes = v;
    }

    private static <T> List<T> orEmpty(List<T> l) {
        return l == null ? List.of() : l;
    }

    /** The server's rule for a volume path (path_valid), as far as it can be checked here. */
    private static boolean validPath(String p) {
        if (!p.startsWith("/"))
            return false;
        for (int i = 0; i < p.length(); i++)
            if (p.charAt(i) < 0x20 || p.charAt(i) == 0x7f)
                return false;
        for (String seg : p.split("/"))
            if (seg.equals(".") || seg.equals(".."))
                return false;
        return true;
    }

    /** Whether the selection takes every series. */
    boolean isAll() {
        return all;
    }

    /**
     * Whether this sink instance writes a series.
     *
     * @param name the series name
     * @return true if it is selected
     */
    boolean matches(String name) {
        if (all || names.contains(name))
            return true;
        for (String p : prefixes)
            if (name.startsWith(p))
                return true;
        return pattern != null && pattern.matcher(name).matches();
    }

    /**
     * The volume a new series is created in.
     *
     * @param name the series name
     * @return the path of the volume of the longest key that is a prefix of the name, or {@code null} for the
     *     database directory
     */
    String volumeFor(String name) {
        for (Map.Entry<String, String> e : volumes)
            if (name.startsWith(e.getKey()))
                return e.getValue();
        return null;
    }

    /** A line for the log at start. */
    String describe() {
        if (all)
            return "all series";
        StringBuilder b = new StringBuilder();
        if (!names.isEmpty())
            b.append(names.size()).append(" name(s) ");
        if (!prefixes.isEmpty())
            b.append("prefixes ").append(prefixes).append(' ');
        if (pattern != null)
            b.append("pattern ").append(pattern.pattern());
        return b.toString().trim();
    }
}
