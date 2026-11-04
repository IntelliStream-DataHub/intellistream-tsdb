package ai.intellistream.tsdb;

import java.util.Objects;

/**
 * Which series a grant covers ({@code TSDB_SEL_*}). Parsed from and shown as the CLI writes them: {@code all},
 * {@code prefix:P}, {@code name:N}, {@code glob:G}.
 *
 * <p>A prefix is a byte prefix of the name ({@code prefix:acme} also matches {@code acmecorp/x}; tenants end theirs
 * in {@code /}). In a glob {@code *} matches any run of bytes, {@code ?} one byte and {@code \} makes the next byte
 * literal; the server stores a glob without a wildcard as a name and one ending in a single final {@code *} as a
 * prefix.
 *
 * @param kind the kind
 * @param text the prefix, name or glob; "" for {@link Kind#ALL}
 */
public record Selector(Kind kind, String text) {
    /** The kinds, in the order of their numbers on the wire. */
    public enum Kind {
        /** Every series. */
        ALL,
        /** The names that start with the text. */
        PREFIX,
        /** One series name. */
        NAME,
        /** The names the glob matches. */
        GLOB
    }

    private static final Selector ALL = new Selector(Kind.ALL, "");

    /**
     * Checks the parts.
     *
     * @param kind the kind
     * @param text the text, "" for {@link Kind#ALL}
     */
    public Selector {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(text, "text");
        if (kind == Kind.ALL && !text.isEmpty())
            throw new IllegalArgumentException("selector all has no text");
    }

    /**
     * Every series.
     *
     * @return the selector
     */
    public static Selector all() {
        return ALL;
    }

    /**
     * The names that start with {@code p}.
     *
     * @param p the prefix
     * @return the selector
     */
    public static Selector prefix(String p) {
        return new Selector(Kind.PREFIX, p);
    }

    /**
     * One series.
     *
     * @param n its name
     * @return the selector
     */
    public static Selector name(String n) {
        return new Selector(Kind.NAME, n);
    }

    /**
     * The names glob {@code g} matches.
     *
     * @param g the glob
     * @return the selector
     */
    public static Selector glob(String g) {
        return new Selector(Kind.GLOB, g);
    }

    /**
     * Parses {@code all}, {@code prefix:P}, {@code name:N} or {@code glob:G}.
     *
     * @param s the selector as the CLI writes it
     * @return the selector
     * @throws IllegalArgumentException for anything else
     */
    public static Selector parse(String s) {
        if (s.equals("all"))
            return ALL;
        int c = s.indexOf(':');
        String kind = c < 0 ? "" : s.substring(0, c), text = c < 0 ? "" : s.substring(c + 1);
        switch (kind) {
        case "prefix": return prefix(text);
        case "name": return name(text);
        case "glob": return glob(text);
        default: throw new IllegalArgumentException("selector \"" + s + "\" is none of all, prefix:P, name:N, glob:G");
        }
    }

    /** As the CLI writes it: "all", "prefix:acme/" and so on. */
    @Override
    public String toString() {
        return kind == Kind.ALL ? "all" : kind.name().toLowerCase(java.util.Locale.ROOT) + ":" + text;
    }
}
