// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import ai.intellistream.tsdb.CompressionTest.FakeServer;

/**
 * Protocol 12 brought the login (13, the HA bodies, keeps it as the floor), so a server that answers HELLO with a lower version (and its 4-byte reply) cannot
 * be spoken to: the connect fails before any request is sent, and says why.
 */
class ProtocolVersionTest {
    @Test
    void serversBelow12AreRefusedAtConnect() throws Exception {
        for (int version = 0; version < 12; version++) {
            try (FakeServer f = new FakeServer(version)) {
                TsdbException e = assertThrows(TsdbException.class,
                        () -> TsdbClient.connect("127.0.0.1", f.port()), "version " + version);
                assertEquals(Status.ERR_INVALID, e.status(), "version " + version);
                assertTrue(e.getMessage().contains("tsdbd speaks protocol " + version
                        + "; this client needs 12: upgrade the server"), e.getMessage());
                assertEquals(List.of((int) Protocol.OP_HELLO), f.ops(), "HELLO is the only frame sent, version "
                        + version);
            }
        }
    }

    @Test
    void version12IsAccepted() throws Exception {
        try (FakeServer f = new FakeServer(12); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(12, c.hello());
            assertTrue(c.isOpen());
        }
    }

    @Test
    void version13IsAccepted() throws Exception {
        try (FakeServer f = new FakeServer(13); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(13, c.hello());
        }
    }

    @Test
    void aNewerServerIsAccepted() throws Exception {
        /* tsdbd answers the lower of both versions, so this is only a server that lies; the client does not mind */
        try (FakeServer f = new FakeServer(14); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(14, c.hello());
        }
    }
}
