package ai.intellistream.tsdb;

/**
 * The rights a grant gives on the series its {@link Selector} matches ({@code TSDB_RIGHT_*}); combine them with
 * {@link #or}.
 *
 * <p>{@code admin} on a selector also lets the holder manage users and grants inside it and implies the other three
 * there. Parsed from and shown as the CLI writes them: {@code read,insert}, {@code write} (insert and delete).
 *
 * @param bits the bits as the wire carries them, unknown ones included (a newer server may know more)
 */
public record Rights(int bits) {
    /** Reading the series' rows, aggregates and metadata. */
    public static final Rights READ = new Rights(1);
    /** Inserting rows (and creating series the selector matches). */
    public static final Rights INSERT = new Rights(2);
    /** Deleting rows, replacing ranges and setting retention. */
    public static final Rights DELETE = new Rights(4);
    /** Managing the users and grants inside the selector. */
    public static final Rights ADMIN = new Rights(8);
    /** Insert and delete. */
    public static final Rights WRITE = new Rights(2 | 4);
    /** No right. */
    public static final Rights NONE = new Rights(0);

    private static final String[] NAMES = {"read", "insert", "delete", "admin"};

    /**
     * Checks the bits.
     *
     * @param bits 0 to 255
     */
    public Rights {
        if (bits < 0 || bits > 0xff)
            throw new IllegalArgumentException("rights 0x" + Integer.toHexString(bits) + ": 8 bits");
    }

    /**
     * These rights and {@code o}.
     *
     * @param o more rights
     * @return the union
     */
    public Rights or(Rights o) {
        return new Rights(bits | o.bits);
    }

    /**
     * Whether these hold every right of {@code o}.
     *
     * @param o rights
     * @return {@code true} if all of them are here
     */
    public boolean contains(Rights o) {
        return (bits & o.bits) == o.bits;
    }

    /**
     * Parses comma-separated names: {@code read}, {@code insert}, {@code delete}, {@code admin}, {@code write}.
     *
     * @param s such as "read,write"
     * @return the rights
     * @throws IllegalArgumentException for another word
     */
    public static Rights parse(String s) {
        int b = 0;
        for (String w : s.split(",", -1)) {
            String t = w.trim();
            int i = java.util.Arrays.asList(NAMES).indexOf(t);
            if (t.equals("write"))
                b |= WRITE.bits;
            else if (i >= 0)
                b |= 1 << i;
            else
                throw new IllegalArgumentException("rights \"" + s + "\": \"" + t
                        + "\" is none of read, insert, delete, admin, write");
        }
        return new Rights(b);
    }

    /** As the CLI writes them: "read,insert", "none" for no right; unknown bits in hex. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < NAMES.length; i++)
            if ((bits & 1 << i) != 0)
                sb.append(sb.length() > 0 ? "," : "").append(NAMES[i]);
        int rest = bits & ~0xf;
        if (rest != 0)
            sb.append(sb.length() > 0 ? "," : "").append(String.format("0x%02x", rest));
        return sb.length() > 0 ? sb.toString() : "none";
    }
}
