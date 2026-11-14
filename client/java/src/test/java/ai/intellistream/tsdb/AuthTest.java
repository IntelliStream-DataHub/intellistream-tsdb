// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Login and access control against a real tsdbd (protocol 12): USERS made by the CLI, a login with the right and a
 * wrong password, an unknown user, requireAuth against {@code --no-auth}, DENIED on another tenant's series, a round
 * trip through every admin call, and the key cache (PBKDF2 once per process for a user, salt and password).
 */
class AuthTest {
    static final long T0 = 1_700_000_000_000L;
    static final String ROOT_PW = "root-secret";
    static final long ALL = Long.MIN_VALUE, NOW = Long.MAX_VALUE;

    Path dir;
    TsdbServer srv;

    @BeforeEach
    void start() throws Exception {
        dir = TsdbServer.scratch("tsdb-java-auth");
        srv = TsdbServer.startAuth(dir, ROOT_PW);
    }

    @AfterEach
    void stop() throws Exception {
        if (srv != null)
            srv.close();
        TsdbServer.delete(dir);
    }

    TsdbClient login(String user, String pw) throws TsdbException {
        return TsdbClient.connect("127.0.0.1", srv.port, ConnectOptions.defaults().user(user).password(pw));
    }

    static void denied(Executable call) {
        TsdbException e = assertThrows(TsdbException.class, call);
        assertEquals(Status.ERR_DENIED, e.status(), e.getMessage());
    }

    static TsdbException authFails(Executable call) {
        TsdbException e = assertThrows(TsdbException.class, call);
        assertEquals(Status.ERR_AUTH, e.status(), e.getMessage());
        return e;
    }

    @Test
    void loginWithTheRightPasswordWrongPasswordAndUnknownUser() throws Exception {
        try (TsdbClient c = login("root", ROOT_PW)) {
            User me = c.whoami();
            assertEquals(new User("root", true, List.of()), me);
            assertEquals(15, c.hello());
            long id = c.series("plant/a");
            c.insert(id, new long[] {T0}, new float[] {1.5f});

            TsdbException wrong = authFails(() -> login("root", "not-it"));
            /* an unknown user fails the same way, with the same message */
            assertEquals(wrong.getMessage(), authFails(() -> login("nobody", "not-it")).getMessage());
            /* no login at all: the client says why, before sending anything but HELLO */
            assertTrue(authFails(() -> TsdbClient.connect("127.0.0.1", srv.port)).getMessage()
                    .contains("requires a login"));
            /* a name SCRAM cannot carry is refused before it is sent */
            assertThrows(IllegalArgumentException.class, () -> login("a,b", "x"));

            /* the password from a file: its first line, a CR LF cut */
            Path pf = dir.resolveSibling(dir.getFileName() + ".pw");
            Files.writeString(pf, ROOT_PW + "\r\nignored\n", StandardCharsets.UTF_8);
            ConnectOptions o = ConnectOptions.defaults().user("root").passwordFile(pf).requireAuth(true);
            try (TsdbClient c2 = TsdbClient.connect("127.0.0.1", srv.port, o)) {
                assertArrayEquals(new float[] {1.5f}, c2.queryPoints(id, ALL, NOW).values());
            }
            Files.writeString(pf, "");
            assertEquals(Status.ERR_INVALID,
                    assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1", srv.port, o)).status());
            Files.delete(pf);
            /* a missing file is a mistake to fix, not a network error (ERR_IO) to retry; it is named (REVIEW3 #3) */
            TsdbException missing =
                    assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1", srv.port, o));
            assertEquals(Status.ERR_INVALID, missing.status());
            assertTrue(missing.getMessage().contains(pf.toString()), missing.getMessage());
            assertThrows(IllegalArgumentException.class,
                    () -> TsdbClient.connect("127.0.0.1", srv.port, ConnectOptions.defaults().user("root")));
            assertFalse((o + " " + ConnectOptions.defaults().password(ROOT_PW)).contains(ROOT_PW));
        }
    }

    @Test
    void requireAuthRefusesAServerWithoutLogin() throws Exception {
        srv.close();
        srv = TsdbServer.start(TsdbServer.scratch("tsdb-java-noauth")); /* --no-auth */
        ConnectOptions o = ConnectOptions.defaults().user("root").password("x");
        /* credentials are not needed there, and are not sent */
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", srv.port, o)) {
            assertEquals(new User("", true, List.of()), c.whoami());
            /* without login every admin call is refused: users are managed with the offline CLI */
            denied(c::userList);
            denied(() -> c.userCreate("x", "pw".toCharArray(), false));
            denied(() -> c.grant("x", Selector.all(), Rights.READ));
        }
        assertTrue(authFails(() -> TsdbClient.connect("127.0.0.1", srv.port, o.requireAuth(true))).getMessage()
                .contains("--no-auth"));
        TsdbServer.delete(srv.dir);
    }

    @Test
    void aTenantSeesOnlyItsSeries() throws Exception {
        try (TsdbClient r = login("root", ROOT_PW)) {
            long own = r.series("acme/x");
            long foreign = r.series("globex/y");
            r.insert(foreign, new long[] {T0}, new float[] {9});
            r.userCreate("acme/u", "u-pw".toCharArray(), false);
            r.grant("acme/u", Selector.parse("prefix:acme/"), Rights.parse("read,insert"));

            try (TsdbClient u = login("acme/u", "u-pw")) {
                assertEquals(own, u.series("acme/x", false));
                u.insert(own, new long[] {T0}, new float[] {1});
                assertArrayEquals(new float[] {1}, u.queryPoints(own, ALL, NOW).values());
                denied(() -> u.series("globex/y", false));
                /* a series that does not exist answers the same */
                denied(() -> u.series("globex/missing", false));
                denied(() -> u.queryPoints(foreign, ALL, NOW));
                denied(() -> u.seriesName(foreign));
                denied(() -> u.insert(foreign, new long[] {T0 + 1}, new float[] {2}));
                denied(() -> u.series("globex/new"));
                /* no delete right on its own series either */
                denied(() -> u.deleteRange(own, ALL, NOW));
                assertTrue(u.isOpen(), "DENIED leaves the connection usable");
                /* QUERY_MULTI answers what it may */
                List<SeriesResult<ValuePoints>> m = u.queryValuesMulti(new long[] {own, foreign}, ALL, NOW);
                assertArrayEquals(new long[] {T0}, m.get(0).get().timestamps());
                assertEquals(Status.ERR_DENIED, m.get(1).status());
                /* and so does QUERY_LATEST: denied and missing look alike */
                List<SeriesResult<Latest>> l = u.latest(own, foreign, (1L << 32) | 999_999);
                assertEquals(T0, l.get(0).get().ts());
                assertEquals(Status.ERR_DENIED, l.get(1).status());
                assertEquals(Status.ERR_DENIED, l.get(2).status());
                List<SeriesResult<Latest>> ln = u.latestByName(new String[] {"acme/x", "globex/y", "globex/missing"},
                        LatestOptions.defaults());
                assertEquals(1f, ln.get(0).get().asDouble());
                assertEquals(Status.ERR_DENIED, ln.get(1).status());
                assertEquals(Status.ERR_DENIED, ln.get(2).status());
            }
            assertArrayEquals(new float[] {9}, r.queryPoints(foreign, ALL, NOW).values());
        }
    }

    @Test
    void adminCallsRoundTrip() throws Exception {
        try (TsdbClient r = login("root", ROOT_PW)) {
            r.userCreate("acme/admin", "a-pw".toCharArray(), false, 5000);
            r.grant("acme/admin", Selector.prefix("acme/"), Rights.ADMIN);
            assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                    () -> r.userCreate("acme/admin", "again".toCharArray(), false)).status());
            assertThrows(IllegalArgumentException.class, () -> r.userCreate("x", new char[0], false));
            assertThrows(IllegalArgumentException.class, () -> r.userCreate("x", "pw".toCharArray(), false, 100));

            /* the tenant admin creates and manages users under its prefix, and nothing outside it */
            try (TsdbClient a = login("acme/admin", "a-pw")) {
                a.userCreate("acme/ingest", "i-pw".toCharArray(), false);
                a.grant("acme/ingest", Selector.parse("glob:acme/*/temp"), Rights.INSERT.or(Rights.READ));
                a.grant("acme/ingest", Selector.name("acme/line1"), Rights.WRITE);
                denied(() -> a.userCreate("globex/x", "pw".toCharArray(), false));
                denied(() -> a.userCreate("acme/root2", "pw".toCharArray(), true));
                denied(() -> a.grant("acme/ingest", Selector.prefix("globex/"), Rights.READ));
                denied(() -> a.userAlter("acme/ingest", true));

                User me = a.whoami();
                assertEquals("acme/admin", me.name());
                assertEquals(List.of(new Grant(Selector.prefix("acme/"), Rights.ADMIN)), me.grants());
                assertEquals(List.of("acme/admin", "acme/ingest"), names(a));
                assertEquals(List.of("acme/admin", "acme/ingest", "root"), names(r));
                User ingest = r.userList().stream().filter(u -> u.name().equals("acme/ingest")).findFirst()
                        .orElseThrow();
                assertFalse(ingest.superuser());
                List<String> grants = new ArrayList<>();
                for (Grant g : ingest.grants())
                    grants.add(g.selector() + " " + g.rights());
                grants.sort(null);
                assertEquals(List.of("glob:acme/*/temp read,insert", "name:acme/line1 insert,delete"), grants);

                /* the user's grants work, and a revoke is seen by its next request */
                try (TsdbClient i = login("acme/ingest", "i-pw")) {
                    long t = r.series("acme/site1/temp");
                    i.insert(t, new long[] {T0}, new float[] {20});
                    a.revoke("acme/ingest", Selector.parse("glob:acme/*/temp"), Rights.INSERT);
                    denied(() -> i.insert(t, new long[] {T0 + 1}, new float[] {21}));
                    assertArrayEquals(new float[] {20}, i.queryPoints(t, ALL, NOW).values());

                    /* a new password: the old one stops working */
                    a.userPassword("acme/ingest", "i-pw2".toCharArray());
                    authFails(() -> login("acme/ingest", "i-pw"));
                    login("acme/ingest", "i-pw2").close();

                    /* superuser flag, then drop: the user's open connection is closed, and it cannot log in */
                    r.userAlter("acme/ingest", true);
                    try (TsdbClient s = login("acme/ingest", "i-pw2")) {
                        assertTrue(s.whoami().superuser());
                    }
                    r.userAlter("acme/ingest", false);
                    a.userDrop("acme/ingest");
                    assertThrows(TsdbException.class, i::whoami);
                    authFails(() -> login("acme/ingest", "i-pw2"));
                }
                denied(() -> r.userDrop("root")); /* the last superuser stays */
                assertEquals(List.of("acme/admin", "root"), names(r));
            }
        }
    }

    static List<String> names(TsdbClient c) throws TsdbException {
        List<String> n = new ArrayList<>();
        for (User u : c.userList())
            n.add(u.name());
        n.sort(null);
        return n;
    }

    /** A grant added by the operator's CLI while tsdbd runs reaches the user within the reload tick. */
    @Test
    void aCliGrantIsSeenWhileRunning() throws Exception {
        try (TsdbClient r = login("root", ROOT_PW)) {
            long id = r.series("plant/b");
            TsdbServer.userCli(dir, "op-pw", "add", "operator");
            long deadline = System.nanoTime() + 10_000_000_000L;
            TsdbClient o = null;
            while (o == null) {
                try {
                    o = login("operator", "op-pw");
                } catch (TsdbException e) {
                    if (e.status() != Status.ERR_AUTH || System.nanoTime() > deadline)
                        throw e;
                    Thread.sleep(50);
                }
            }
            try (TsdbClient op = o) {
                denied(() -> op.series("plant/b", false));
                TsdbServer.userCli(dir, null, "grant", "operator", "read", "name:plant/b");
                for (;;) {
                    try {
                        assertEquals(id, op.series("plant/b", false));
                        break;
                    } catch (TsdbException e) {
                        if (e.status() != Status.ERR_DENIED || System.nanoTime() > deadline)
                            throw e;
                        Thread.sleep(50);
                    }
                }
            }
        }
    }

    /**
     * The key cache: PBKDF2 runs once per process for a user, salt and password, not once per connection, also
     * for connections opened in parallel. The server's fresh salt makes this its own entry.
     */
    @Test
    void pbkdf2RunsOncePerProcess() throws Exception {
        ConnectOptions o = ConnectOptions.defaults().user("root").password(ROOT_PW);
        long before = Scram.pbkdf2Runs();
        List<TsdbClient> clients = new ArrayList<>();
        try {
            for (int i = 0; i < 5; i++)
                clients.add(TsdbClient.connect("127.0.0.1", srv.port, o));
            assertEquals(1, Scram.pbkdf2Runs() - before, "PBKDF2 once for 5 logins");
            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                List<Future<String>> f = new ArrayList<>();
                for (int i = 0; i < 4; i++)
                    f.add(pool.submit(() -> {
                        try (TsdbClient c = TsdbClient.connect("127.0.0.1", srv.port, o)) {
                            return c.whoami().name();
                        }
                    }));
                for (Future<String> x : f)
                    assertEquals("root", x.get());
            } finally {
                pool.shutdown();
            }
            assertEquals(1, Scram.pbkdf2Runs() - before, "PBKDF2 once for 9 logins");

            /* another password is another entry (and fails); the right one is still cached */
            authFails(() -> TsdbClient.connect("127.0.0.1", srv.port, o.password("other")));
            assertEquals(2, Scram.pbkdf2Runs() - before);
            TsdbClient.connect("127.0.0.1", srv.port, o).close();
            assertEquals(2, Scram.pbkdf2Runs() - before);
        } finally {
            for (TsdbClient c : clients)
                c.close();
        }
    }

    /** {@code n} threads that connect as {@code user} at once; the PBKDF2 runs they cost. */
    long coldPool(String user, String pw, int n) throws Exception {
        long before = Scram.pbkdf2Runs();
        java.util.concurrent.CyclicBarrier go = new java.util.concurrent.CyclicBarrier(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<String>> f = new ArrayList<>();
            for (int i = 0; i < n; i++)
                f.add(pool.submit(() -> {
                    go.await();
                    try (TsdbClient c = login(user, pw)) {
                        return c.whoami().name();
                    }
                }));
            for (Future<String> x : f)
                assertEquals(user, x.get());
        } finally {
            pool.shutdown();
        }
        return Scram.pbkdf2Runs() - before;
    }

    long runsOfLogin(String user, String pw) throws Exception {
        long before = Scram.pbkdf2Runs();
        login(user, pw).close();
        return Scram.pbkdf2Runs() - before;
    }

    /**
     * REVIEW3 #1: connections that start together (a sink's threads) run PBKDF2 once and add one cache entry, so
     * other users' keys stay. Iterations high enough that, without single flight, all of them missed.
     */
    @Test
    void aColdPoolRunsPbkdf2OnceAndKeepsOtherUsersKeys() throws Exception {
        try (TsdbClient r = login("root", ROOT_PW)) {
            r.userCreate("cold/a", "a-pw".toCharArray(), false, 100_000);
            r.userCreate("cold/b", "b-pw".toCharArray(), false, 100_000);
        }
        login("root", ROOT_PW).close();
        assertEquals(1, coldPool("cold/a", "a-pw", 24), "a cold pool of 24, one PBKDF2");
        assertEquals(1, coldPool("cold/b", "b-pw", 16), "a cold pool of 16, one PBKDF2");
        assertEquals(0, runsOfLogin("root", ROOT_PW), "root's key survived the cold pools");
        assertEquals(0, runsOfLogin("cold/a", "a-pw"), "cold/a's key survived the cold pool of cold/b");
    }

    /** A login with the password in a file holding {@code content}. */
    void fileLogin(String user, byte[] content) throws Exception {
        Path pf = dir.resolveSibling(dir.getFileName() + ".pwrule");
        Files.write(pf, content);
        try {
            TsdbClient.connect("127.0.0.1", srv.port, ConnectOptions.defaults().user(user).passwordFile(pf)).close();
        } finally {
            Files.delete(pf);
        }
    }

    /**
     * REVIEW3 #2, #3, #4: 1 to 1023 bytes in every client; a file's first line without LF or CR LF; anything else,
     * and a file that is not there, is ERR_INVALID naming it. A password with an unpaired surrogate is refused, not
     * stored as "pw?".
     */
    @Test
    void thePasswordRuleAndPasswordFiles() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1023; i++)
            sb.append((char) (' ' + i % 95));
        String p1023 = sb.toString(), p1024 = p1023 + "x";
        try (TsdbClient r = login("root", ROOT_PW)) {
            for (String bad : new String[] {"", p1024, "a\0b", "pw\uD800"}) {
                assertThrows(IllegalArgumentException.class, () -> r.userCreate("pw/0", bad.toCharArray(), false));
                assertThrows(IllegalArgumentException.class, () -> r.userPassword("root", bad.toCharArray()));
            }
            r.userCreate("pw/1", "1".toCharArray(), false);
            r.userCreate("pw/1023", p1023.toCharArray(), false);
            r.userCreate("pw/q", "pw?".toCharArray(), false);
        }
        login("pw/1", "1").close();
        login("pw/1023", p1023).close();
        login("pw/q", "pw?").close();
        assertThrows(IllegalArgumentException.class, () -> login("pw/q", "pw\uD800"));
        assertThrows(IllegalArgumentException.class, () -> login("pw/1023", p1024));

        fileLogin("pw/1", "1".getBytes(StandardCharsets.UTF_8));
        fileLogin("pw/1", "1\n".getBytes(StandardCharsets.UTF_8));
        fileLogin("pw/1", "1\r\nx\n".getBytes(StandardCharsets.UTF_8));
        fileLogin("pw/1023", p1023.getBytes(StandardCharsets.UTF_8));
        fileLogin("pw/1023", (p1023 + "\r\n").getBytes(StandardCharsets.UTF_8));
        String[][] bad = {{"pw/1023", p1023 + "\r"}, {"pw/1023", p1024 + "\n"}, {"pw/1", ""}, {"pw/1", "\n1\n"},
            {"pw/1", "\r\n"}, {"pw/1", "1\u00002\n"}};
        for (String[] b : bad) {
            TsdbException e = assertThrows(TsdbException.class,
                    () -> fileLogin(b[0], b[1].getBytes(StandardCharsets.UTF_8)), b[1].length() + " bytes");
            assertEquals(Status.ERR_INVALID, e.status(), e.getMessage());
            assertTrue(e.getMessage().contains(".pwrule"), e.getMessage());
        }
    }

    /**
     * REVIEW3 #7: the rights of every grant read back print in the grammar Rights.parse and tsdbd read: the 15
     * non-empty combinations of the four rights, granted again from their printed form, give the same grants.
     */
    @Test
    void rightsPrintAsTheyParse() throws Exception {
        try (TsdbClient r = login("root", ROOT_PW)) {
            r.userCreate("rt/a", "a-pw".toCharArray(), false);
            r.userCreate("rt/b", "b-pw".toCharArray(), false);
            for (int bits = 1; bits < 16; bits++)
                r.grant("rt/a", Selector.name("rt/" + bits), new Rights(bits));
            List<String> a = new ArrayList<>(), b = new ArrayList<>();
            for (User u : r.userList())
                if (u.name().equals("rt/a"))
                    for (Grant g : u.grants())
                        a.add(g.selector() + " " + g.rights());
            assertEquals(15, a.size());
            for (String g : a) {
                String[] sr = g.split(" ");
                r.grant("rt/b", Selector.parse(sr[0]), Rights.parse(sr[1]));
            }
            for (User u : r.userList())
                if (u.name().equals("rt/b"))
                    for (Grant g : u.grants())
                        b.add(g.selector() + " " + g.rights());
            java.util.Collections.sort(a);
            java.util.Collections.sort(b);
            assertEquals(a, b);
            r.grant("rt/b", Selector.name("rt/sp"), Rights.parse("read , insert"));
            assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                    () -> r.grant("rt/b", Selector.name("rt/x"), Rights.parse("0x10"))).status());
        }
    }

    /**
     * REVIEW3 #6: a grant of a kind a later server adds (9 here) reaches whoami and userList as Kind.UNKNOWN with its
     * number, and the connection stays usable. A fake server of protocol 12 without login answers with one.
     */
    @Test
    void anUnknownSelectorKindIsKept() throws Exception {
        byte[] user = {1, 0, 'u', 0, 2, 0, 0, 0, 9, 0x45, 3, 0, 'x', '/', '*', 1, 1, 2, 0, 'p', '/'};
        try (java.net.ServerSocket l = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            Thread t = new Thread(() -> {
                try (java.net.Socket s = l.accept()) {
                    java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
                    for (;;) {
                        byte[] h = new byte[9];
                        in.readFully(h);
                        int len = java.nio.ByteBuffer.wrap(h).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(0);
                        in.readFully(new byte[len - 5]);
                        byte[] body = switch (h[8]) {
                        case Protocol.OP_HELLO -> new byte[] {12, 0, 0, 0, 0, 0, 0, 0};
                        case Protocol.OP_WHOAMI -> user;
                        case Protocol.OP_USER_LIST -> java.nio.ByteBuffer.allocate(5 + user.length).put((byte) 1)
                                .put(new byte[] {1, 0, 0, 0}).put(user).array();
                        default -> new byte[0];
                        };
                        s.getOutputStream().write(java.nio.ByteBuffer.allocate(13 + body.length)
                                .order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(9 + body.length).put(h, 4, 4)
                                .put((byte) 0).putInt(0).put(body).array());
                    }
                } catch (java.io.IOException e) {
                    /* the client closed */
                }
            });
            t.start();
            try (TsdbClient c = TsdbClient.connect("127.0.0.1", l.getLocalPort())) {
                for (User u : new User[] {c.whoami(), c.userList().get(0), c.whoami()}) {
                    assertEquals("u", u.name());
                    Selector s = u.grants().get(0).selector();
                    assertEquals(Selector.Kind.UNKNOWN, s.kind());
                    assertEquals(9, s.code());
                    assertEquals("kind#9:x/*", s.toString());
                    assertEquals("read,delete,0x40", u.grants().get(0).rights().toString());
                    assertEquals(Selector.prefix("p/"), u.grants().get(1).selector());
                }
                Selector other = c.whoami().grants().get(0).selector();
                assertThrows(IllegalArgumentException.class, () -> c.grant("u", other, Rights.READ));
            }
            t.join(10_000);
        }
    }
}
