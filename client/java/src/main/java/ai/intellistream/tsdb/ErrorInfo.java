package ai.intellistream.tsdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The body of an UNAVAILABLE, MOVED, PARTIAL or DROPPED reply (protocol 13, {@code include/tsdb_wire.h}): why a
 * request was refused, where a volume went, what a frame that spans volumes did.
 *
 * <p>The decoder never trusts a count: an entry count the bytes cannot hold, or bytes left over, make the body
 * malformed, and {@link #hasBody()} is {@code false} (the status alone still says what to do).
 */
public final class ErrorInfo {
    private static final int UNAVAIL_MAX = 1024;
    private static final int UNAVAIL_ENTRY = 28;
    private static final int ADDR_MAX = 255;
    /** The {@code vol} of an UNAVAILABLE entry that names no one volume. */
    public static final int VOL_ANY = 0xffff;
    /** MOVED flag: a clean release is under way. */
    public static final int MOVED_HANDOFF = 1;

    /**
     * One volume a request was refused for.
     *
     * @param vol the volume, or {@link #VOL_ANY}
     * @param reason why ({@code TSDB_WHY_*}: 1 storage, 2 stopped, ...)
     * @param flags entry flags
     * @param epoch the volume's epoch
     * @param sinceMs when the outage began, ms since the epoch of the server's clock
     * @param retryAfterMs how long to wait before asking again
     * @param outageId the outage's id
     */
    public record Unavailable(int vol, int reason, int flags, long epoch, long sinceMs, long retryAfterMs,
            long outageId) {
    }

    /**
     * One volume another node owns.
     *
     * @param vol the volume
     * @param epoch its epoch
     * @param flags {@link #MOVED_HANDOFF} or 0
     * @param nodeId the owner's node id
     * @param addr the owner's address, empty when the body names none
     */
    public record Moved(int vol, long epoch, int flags, long nodeId, String addr) {
    }

    /**
     * A volume's own status in a PARTIAL reply.
     *
     * @param vol the volume
     * @param status 0, or UNAVAILABLE, MOVED or DROPPED
     */
    public record PartialVol(int vol, int status) {
    }

    private final int status;
    private final boolean hasBody;
    private List<Unavailable> unavailable = List.of();
    private List<Moved> moved = List.of();
    private List<PartialVol> volumes = List.of();
    private Long resendFrom;
    private long rows;
    private byte[] notApplied = new byte[0];
    private int droppedVol;
    private long droppedBefore;

    private ErrorInfo(int status, boolean hasBody) {
        this.status = status;
        this.hasBody = hasBody;
    }

    /** No body was seen (the status of a reply that has none, or a malformed one). */
    static ErrorInfo bare(int status) {
        return new ErrorInfo(status, false);
    }

    /**
     * Returns the status of the reply, 0 for the info of no reply yet.
     *
     * @return the status of the reply, 0 for the info of no reply yet
     */
    public int status() {
        return status;
    }

    /**
     * Returns {@code false}: no body, or a malformed one; only {@link #status()} is known.
     *
     * @return {@code false}: no body, or a malformed one; only {@link #status()} is known
     */
    public boolean hasBody() {
        return hasBody;
    }

    /**
     * Returns the UNAVAILABLE entries (a PARTIAL's refused volumes too).
     *
     * @return the UNAVAILABLE entries (a PARTIAL's refused volumes too)
     */
    public List<Unavailable> unavailable() {
        return unavailable;
    }

    /**
     * Returns the MOVED entries.
     *
     * @return the MOVED entries
     */
    public List<Moved> moved() {
        return moved;
    }

    /**
     * Returns the {@code resend_from} of a refusal of a frame that carried a sequence number, or {@code null}.
     *
     * @return the {@code resend_from} of a refusal of a frame that carried a sequence number, or {@code null}
     */
    public Long resendFrom() {
        return resendFrom;
    }

    /**
     * Returns PARTIAL: each volume's status.
     *
     * @return PARTIAL: each volume's status
     */
    public List<PartialVol> volumes() {
        return volumes;
    }

    /**
     * Returns PARTIAL: the rows of the frame.
     *
     * @return PARTIAL: the rows of the frame
     */
    public long rows() {
        return rows;
    }

    /**
     * PARTIAL: whether a row was left out.
     *
     * @param row the row's index in the frame
     * @return {@code true} if the server did not apply it
     */
    public boolean rowNotApplied(int row) {
        int i = row >>> 3;
        return row >= 0 && i < notApplied.length && (notApplied[i] >> (row & 7) & 1) != 0;
    }

    /**
     * Returns DROPPED: the volume.
     *
     * @return DROPPED: the volume
     */
    public int droppedVol() {
        return droppedVol;
    }

    /**
     * Returns DROPPED: rows before this timestamp (ms) are never accepted.
     *
     * @return DROPPED: rows before this timestamp (ms) are never accepted
     */
    public long droppedBefore() {
        return droppedBefore;
    }

    /**
     * Returns how long the server asks to wait: the largest {@code retryAfterMs} of the entries, 0 without any.
     *
     * @return how long the server asks to wait: the largest {@code retryAfterMs} of the entries, 0 without any
     */
    public long retryAfterMs() {
        long m = 0;
        for (Unavailable u : unavailable)
            m = Math.max(m, u.retryAfterMs());
        return m;
    }

    /** Where the body of an error reply starts, after the op's own empty reply ({@code tsdb_wire_error_shape}). */
    static int shape(byte op) {
        switch (op) {
        case Protocol.OP_QUERY_RAW: return 5;
        case Protocol.OP_QUERY_VALUES:
        case Protocol.OP_QUERY_LTTB:
        case Protocol.OP_QUERY_MULTI: return 8;
        default: return 0;
        }
    }

    /** The info of an error reply whose payload (after the status) is {@code payload}, for a request of op {@code op}. */
    static ErrorInfo of(int status, ByteBuffer payload, byte op) {
        int off = shape(op);
        if (payload.remaining() < off)
            return bare(status);
        ByteBuffer b = payload.slice(payload.position() + off, payload.remaining() - off).order(ByteOrder.LITTLE_ENDIAN);
        return decode(status, b);
    }

    /** Decodes a body; {@link #bare(int)} when malformed. */
    static ErrorInfo decode(int status, ByteBuffer b) {
        try {
            ErrorInfo e = new ErrorInfo(status, true);
            switch (status) {
            case Status.ERR_UNAVAILABLE:
                e.unavailable = entries(b);
                e.resend(b);
                break;
            case Status.ERR_MOVED: {
                int n = u16(b);
                if (n > UNAVAIL_MAX || b.remaining() < n * 13)
                    return bare(status);
                List<Moved> l = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    int vol = u16(b);
                    long epoch = u32(b);
                    int flags = b.get() & 0xff;
                    long node = u32(b);
                    l.add(new Moved(vol, epoch, flags, node, addr(b, u16(b))));
                }
                e.moved = l;
                e.resend(b);
                break;
            }
            case Status.ERR_PARTIAL: {
                int nv = u16(b);
                if (nv > UNAVAIL_MAX || b.remaining() < nv * 4 + 4)
                    return bare(status);
                List<PartialVol> l = new ArrayList<>(nv);
                for (int i = 0; i < nv; i++)
                    l.add(new PartialVol(u16(b), (short) u16(b)));
                e.volumes = l;
                e.rows = u32(b);
                if (e.rows > Protocol.MAX_FRAME)
                    return bare(status);
                e.notApplied = new byte[(int) ((e.rows + 7) / 8)];
                if (b.remaining() < e.notApplied.length)
                    return bare(status);
                b.get(e.notApplied);
                e.unavailable = entries(b);
                if (b.hasRemaining())
                    return bare(status);
                break;
            }
            case Status.ERR_DROPPED:
                e.droppedVol = u16(b);
                e.droppedBefore = b.getLong();
                if (b.hasRemaining())
                    return bare(status); /* exactly 10 bytes, as the C codec reads it */
                break;
            default:
                return bare(status);
            }
            return e;
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException x) {
            return bare(status);
        }
    }

    private void resend(ByteBuffer b) {
        if (b.remaining() == 8)
            resendFrom = b.getLong();
        else if (b.hasRemaining())
            throw new IllegalArgumentException("trailing bytes");
    }

    private static List<Unavailable> entries(ByteBuffer b) {
        int n = u16(b);
        if (n > UNAVAIL_MAX || b.remaining() < n * UNAVAIL_ENTRY)
            throw new IllegalArgumentException("entries");
        List<Unavailable> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int vol = u16(b);
            int reason = b.get() & 0xff, flags = b.get() & 0xff;
            long epoch = u32(b);
            long since = b.getLong();
            long retry = u32(b);
            l.add(new Unavailable(vol, reason, flags, epoch, since, retry, b.getLong()));
        }
        return l;
    }

    static int u16(ByteBuffer b) {
        return b.getShort() & 0xffff;
    }

    static long u32(ByteBuffer b) {
        return b.getInt() & 0xffffffffL;
    }

    static String addr(ByteBuffer b, int n) {
        if (n > ADDR_MAX || b.remaining() < n)
            throw new IllegalArgumentException("address");
        byte[] a = new byte[n];
        b.get(a);
        return new String(a, StandardCharsets.UTF_8);
    }

    /**
     * "host:port", "[v6]:port" or "host" (port 7070) as "host:port" for connecting; {@code null} if it makes no sense.
     */
    static String parseNode(String s) {
        s = s.trim();
        if (s.isEmpty())
            return null;
        if (s.charAt(0) == '[') {
            int e = s.indexOf(']');
            if (e < 2)
                return null;
            String host = s.substring(1, e), tail = s.substring(e + 1);
            if (tail.isEmpty())
                return "[" + host + "]:" + Protocol.DEFAULT_PORT;
            return tail.charAt(0) == ':' && portOk(tail.substring(1)) ? "[" + host + "]" + tail : null;
        }
        int colons = (int) s.chars().filter(c -> c == ':').count();
        if (colons == 0)
            return s + ":" + Protocol.DEFAULT_PORT;
        if (colons == 1) {
            int i = s.indexOf(':');
            return i > 0 && portOk(s.substring(i + 1)) ? s : null;
        }
        return "[" + s + "]:" + Protocol.DEFAULT_PORT; /* a bare IPv6 address has no port to read */
    }

    private static boolean portOk(String p) {
        try {
            int v = Integer.parseInt(p);
            return v >= 1 && v <= 65535 && !p.startsWith("+");
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
