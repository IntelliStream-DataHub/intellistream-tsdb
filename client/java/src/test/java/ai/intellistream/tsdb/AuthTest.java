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
            assertEquals(12, c.hello());
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
            assertThrows(IllegalArgumentException.class, () -> TsdbClient.connect("127.0.0.1", srv.port, o));
            Files.delete(pf);
            assertEquals(Status.ERR_IO,
                    assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1", srv.port, o)).status());
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
}
