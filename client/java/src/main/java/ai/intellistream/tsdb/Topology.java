// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The reply of {@link TsdbClient#topology()}: the nodes and volumes a server lists (protocol 13). A single server
 * lists itself.
 *
 * @param generation a number that changes when the answer does
 * @param nodes the nodes
 * @param volumes the volumes
 */
public record Topology(long generation, List<Node> nodes, List<Vol> volumes) {
    /** The volume has a storage outage ({@code TSDB_HEALTH_*}: 0 healthy, 1 degraded, 2 unavailable). */
    public static final int HEALTH_UNAVAILABLE = 2;

    /**
     * A node of the cluster.
     *
     * @param nodeId the node's id
     * @param alive whether the cluster sees it alive
     * @param addr its address; empty: the node this answer came from
     */
    public record Node(long nodeId, boolean alive, String addr) {
    }

    /**
     * A volume.
     *
     * @param vol the volume's number
     * @param role {@code TSDB_ROLE_*}
     * @param health {@code TSDB_HEALTH_*}
     * @param epoch the volume's epoch
     * @param reason {@code TSDB_WHY_*} when not available
     * @param domain the storage domain, 0xffff when unknown
     * @param sinceMs since when the health has held, ms
     * @param owner the owner's node id
     * @param server the server (a node's index)
     * @param addr the owner's address, empty: this node
     * @param path the volume's directory; empty unless the user is a superuser
     */
    public record Vol(int vol, int role, int health, long epoch, int reason, int domain, long sinceMs, long owner,
            int server, String addr, String path) {
    }

    /** Decodes a TOPOLOGY reply; {@code null} when malformed. */
    static Topology decode(ByteBuffer body) {
        ByteBuffer b = body.slice().order(ByteOrder.LITTLE_ENDIAN);
        try {
            long generation = b.getLong();
            long nn = ErrorInfo.u32(b);
            if (nn > 4096 || b.remaining() < nn * 7)
                return null;
            List<Node> nodes = new ArrayList<>((int) nn);
            for (long i = 0; i < nn; i++) {
                long id = ErrorInfo.u32(b);
                boolean alive = b.get() != 0;
                nodes.add(new Node(id, alive, ErrorInfo.addr(b, ErrorInfo.u16(b))));
            }
            long nv = ErrorInfo.u32(b);
            if (nv > 65535 || b.remaining() < nv * 30)
                return null;
            List<Vol> vols = new ArrayList<>((int) nv);
            for (long i = 0; i < nv; i++) {
                int vol = ErrorInfo.u16(b);
                int role = b.get() & 0xff, health = b.get() & 0xff;
                long epoch = ErrorInfo.u32(b);
                int reason = b.get() & 0xff;
                b.get();
                int domain = ErrorInfo.u16(b);
                long since = b.getLong();
                long owner = ErrorInfo.u32(b);
                int server = ErrorInfo.u16(b);
                String addr = ErrorInfo.addr(b, ErrorInfo.u16(b));
                int pl = ErrorInfo.u16(b);
                if (pl > 4096 || b.remaining() < pl)
                    return null;
                byte[] p = new byte[pl];
                b.get(p);
                vols.add(new Vol(vol, role, health, epoch, reason, domain, since, owner, server, addr,
                        new String(p, StandardCharsets.UTF_8)));
            }
            return b.hasRemaining() ? null : new Topology(generation, List.copyOf(nodes), List.copyOf(vols));
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
            return null;
        }
    }
}
