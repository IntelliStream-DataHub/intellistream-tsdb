package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * The client's side of SCRAM-SHA-256 against fake servers: the RFC 7677 §3 exchange byte for byte (its values read
 * from tests/rfc7677.h, which the C and Rust tests use too), the server-first and server-final messages it must
 * refuse, and the primitives against their RFC vectors.
 */
class ScramTest {
    /**
     * The string macros of tests/rfc7677.h: {@code #define NAME "text"}, adjacent literals and earlier macros
     * concatenated, {@code \} continuations joined.
     */
    static Map<String, String> rfc7677() throws IOException {
        String text = Files.readString(Paths.get("../../tests/rfc7677.h")).replace("\\\n", " ");
        Map<String, String> m = new HashMap<>();
        Pattern token = Pattern.compile("\\s*(?:\"([^\"]*)\"|([A-Za-z0-9_]+))");
        for (String line : text.split("\n")) {
            line = line.trim();
            if (!line.startsWith("#define "))
                continue;
            String rest = line.substring(8);
            int sp = rest.indexOf(' ');
            if (sp < 0)
                continue;
            String name = rest.substring(0, sp);
            StringBuilder value = new StringBuilder();
            Matcher t = token.matcher(rest);
            t.region(sp, rest.length());
            while (t.lookingAt()) {
                if (t.group(1) != null)
                    value.append(t.group(1));
                else
                    value.append(m.getOrDefault(t.group(2), t.group(2))); /* an earlier macro, or a number */
                t.region(t.end(), rest.length());
            }
            m.put(name, value.toString());
        }
        return m;
    }

    /**
     * A fake tsdbd of protocol 12 that asks for a login: answers HELLO, then AUTH_START with {@code first} and
     * AUTH_CONTINUE with {@code last} (u16 length, message). The future gives the client-first and client-final
     * messages it got ("" for one it did not).
     */
    static final class Fake implements AutoCloseable {
        final ServerSocket listener;
        final CompletableFuture<String[]> got = new CompletableFuture<>();

        Fake(String first, String last) throws IOException {
            listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            Thread t = new Thread(() -> serve(first, last), "fake-scram");
            t.setDaemon(true);
            t.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        private void serve(String first, String last) {
            String[] msgs = {"", ""};
            try (Socket s = listener.accept()) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                for (;;) {
                    byte[] h = new byte[9];
                    try {
                        in.readFully(h);
                    } catch (IOException e) {
                        break;
                    }
                    ByteBuffer hb = ByteBuffer.wrap(h).order(ByteOrder.LITTLE_ENDIAN);
                    byte[] p = new byte[hb.getInt(0) - 5];
                    in.readFully(p);
                    ByteBuffer pb = ByteBuffer.wrap(p).order(ByteOrder.LITTLE_ENDIAN);
                    byte[] body;
                    switch (h[8]) {
                    case Protocol.OP_HELLO:
                        body = ByteBuffer.allocate(8 + 12 + 14).order(ByteOrder.LITTLE_ENDIAN).putInt(12)
                                .putShort((short) Protocol.HELLO_AUTH).putShort((short) 2)
                                .put("\u000bSCRAM-SHA-1\rSCRAM-SHA-256".getBytes(StandardCharsets.US_ASCII)).array();
                        break;
                    case Protocol.OP_AUTH_START:
                        assertEquals("\rSCRAM-SHA-256", new String(p, 0, 14, StandardCharsets.US_ASCII));
                        assertEquals(p.length, 16 + (pb.getShort(14) & 0xffff));
                        msgs[0] = new String(p, 16, p.length - 16, StandardCharsets.UTF_8);
                        body = message(first);
                        break;
                    case Protocol.OP_AUTH_CONTINUE:
                        assertEquals(p.length, 2 + (pb.getShort(0) & 0xffff));
                        msgs[1] = new String(p, 2, p.length - 2, StandardCharsets.UTF_8);
                        body = message(last);
                        break;
                    default:
                        throw new AssertionError("op " + h[8] + " before the login finished");
                    }
                    out.write(ByteBuffer.allocate(13 + body.length).order(ByteOrder.LITTLE_ENDIAN)
                            .putInt(9 + body.length).put(h, 4, 4).put((byte) 0).putInt(0).put(body).array());
                    if (h[8] == Protocol.OP_AUTH_CONTINUE)
                        break;
                }
                got.complete(msgs);
            } catch (IOException | AssertionError e) {
                got.completeExceptionally(e);
            }
        }

        private static byte[] message(String m) {
            byte[] b = m.getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(2 + b.length).order(ByteOrder.LITTLE_ENDIAN).putShort((short) b.length).put(b)
                    .array();
        }

        String[] messages() throws Exception {
            return got.get(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }
    }

    static TsdbClient login(int port, String user, String password) throws TsdbException {
        return TsdbClient.connect("127.0.0.1", port, ConnectOptions.defaults().user(user).password(password));
    }

    @Test
    void rfc7677ExchangeByteForByte() throws Exception {
        Map<String, String> v = rfc7677();
        Scram.setClientNonce(v.get("RFC7677_CNONCE"));
        try (Fake f = new Fake(v.get("RFC7677_SERVER_FIRST"), v.get("RFC7677_SERVER_FINAL"))) {
            TsdbClient c = login(f.port(), v.get("RFC7677_USER"), v.get("RFC7677_PASSWORD"));
            String[] m = f.messages();
            assertEquals(v.get("RFC7677_CLIENT_FIRST"), m[0]);
            assertEquals(v.get("RFC7677_CLIENT_FINAL"), m[1]);
            assertTrue(c.isOpen());
            c.close();
        } finally {
            Scram.setClientNonce(null);
        }
        assertEquals(v.get("RFC7677_NONCE"), v.get("RFC7677_CNONCE") + v.get("RFC7677_SNONCE"));
    }

    /**
     * A server that does not hold the verifier cannot pass: a wrong ServerSignature, an {@code e=}, a malformed
     * server-first all fail the connect with ERR_AUTH.
     */
    @Test
    void badServerMessagesFailTheLogin() throws Exception {
        Map<String, String> v = rfc7677();
        String sf = v.get("RFC7677_SERVER_FIRST"), fin = v.get("RFC7677_SERVER_FINAL");
        char[] wrong = fin.toCharArray();
        wrong[5] ^= 1; /* another base64 letter */
        List<String[]> cases = new ArrayList<>(List.of(
                new String[] {sf, new String(wrong)},
                new String[] {sf, "e=invalid-proof"},
                new String[] {sf, ""},
                new String[] {sf, fin + ",x=1"},
                new String[] {sf.replace(",i=4096", ",i=4095"), fin}, /* a downgrade */
                new String[] {sf.replace(",i=4096", ",i=04096"), fin},
                new String[] {sf.replace(",i=4096", ",i=4096,x=1"), fin}, /* an extension */
                new String[] {sf.replace("r=rOprNGfwEbeRWgbNEkqO", "r=xOprNGfwEbeRWgbNEkqO"), fin}, /* not our nonce */
                new String[] {"r=" + v.get("RFC7677_CNONCE") + ",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", fin},
                new String[] {sf.replace(",s=W22ZaJ0SNY7soEsUEjb6gQ==", ",s=W22Z!J0SNY7soEsUEjb6gQ=="), fin},
                new String[] {sf.replace(",s=W22ZaJ0SNY7soEsUEjb6gQ==", ",s="), fin},
                new String[] {sf.replace(",s=W22ZaJ0SNY7soEsUEjb6gQ==", ",s=W22ZaJ0SNY7soEsUEjb6gQ"), fin},
                new String[] {sf.replace(",i=4096", ",i=99999999999"), fin},
                new String[] {"m=x," + sf, fin}));
        Scram.setClientNonce(v.get("RFC7677_CNONCE"));
        try {
            for (String[] c : cases) {
                try (Fake f = new Fake(c[0], c[1])) {
                    TsdbException e = assertThrows(TsdbException.class, () -> login(f.port(), "user", "pencil"),
                            c[0] + " / " + c[1] + " must fail");
                    assertEquals(Status.ERR_AUTH, e.status(), c[0] + " / " + c[1] + ": " + e);
                    f.messages();
                }
            }
        } finally {
            Scram.setClientNonce(null);
        }
    }

    @Test
    void aServerAskingForALoginGetsNoneWithoutAUser() throws Exception {
        try (Fake f = new Fake("", "")) {
            TsdbException e = assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1", f.port()));
            assertEquals(Status.ERR_AUTH, e.status());
            assertTrue(e.getMessage().contains("requires a login"), e.getMessage());
            assertEquals("", f.messages()[0], "nothing was sent after HELLO");
        }
        /* a user without a password, or one SCRAM cannot carry, is refused before anything is sent */
        assertThrows(IllegalArgumentException.class,
                () -> TsdbClient.connect("127.0.0.1", 1, ConnectOptions.defaults().user("u")));
        assertThrows(IllegalArgumentException.class, () -> ConnectOptions.defaults().password(""));
    }

    @Test
    void primitivesMatchTheirRfcs() {
        HexFormat hex = HexFormat.of();
        /* RFC 7914 §11, the first 32 bytes of each */
        assertEquals("55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc",
                hex.formatHex(Scram.pbkdf2("passwd".getBytes(StandardCharsets.US_ASCII),
                        "salt".getBytes(StandardCharsets.US_ASCII), 1)));
        assertEquals("4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56",
                hex.formatHex(Scram.pbkdf2("Password".getBytes(StandardCharsets.US_ASCII),
                        "NaCl".getBytes(StandardCharsets.US_ASCII), 80000)));
        /* RFC 4231 test case 2; an empty key is the HMAC of 64 zero bytes */
        assertEquals("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
                hex.formatHex(Scram.hmac("Jefe".getBytes(StandardCharsets.US_ASCII),
                        "what do ya want for nothing?".getBytes(StandardCharsets.US_ASCII))));
        assertArrayEquals(Scram.hmac(new byte[64], new byte[] {1}), Scram.hmac(new byte[0], new byte[] {1}));
        /* strict base64 (RFC 4648 §10) */
        assertArrayEquals("fooba".getBytes(StandardCharsets.US_ASCII), Scram.unbase64("Zm9vYmE="));
        assertArrayEquals(new byte[0], Scram.unbase64(""));
        for (String bad : new String[] {"Zg=", "Z===", "Zg==Zg==", "Zm9v!A==", "Zm=v", "Zg"})
            assertNull(Scram.unbase64(bad), bad);
    }

    @Test
    void rightsAndSelectorsParseAsTheCliWritesThem() {
        assertEquals(Rights.READ.or(Rights.INSERT).or(Rights.DELETE), Rights.parse("read,write"));
        assertEquals("read,admin", Rights.READ.or(Rights.ADMIN).toString());
        assertEquals("none", Rights.NONE.toString());
        assertThrows(IllegalArgumentException.class, () -> Rights.parse("read,fly"));
        for (String s : new String[] {"all", "prefix:acme/", "name:acme/x", "glob:acme/*/t?", "prefix:"})
            assertEquals(s, Selector.parse(s).toString());
        assertThrows(IllegalArgumentException.class, () -> Selector.parse("acme/"));
    }
}
