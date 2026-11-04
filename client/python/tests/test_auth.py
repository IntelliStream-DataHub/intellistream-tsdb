"""Login (SCRAM-SHA-256, protocol 12) and access control through the Rust client: the RFC 7677 §3 exchange byte for
byte against a fake server (its values read from tests/rfc7677.h, which the C and Rust tests use too), server
messages that must fail the login, then a real tsdbd with USERS made by the CLI: the right and a wrong password, an
unknown user, require_auth against --no-auth, ERR_DENIED on another tenant's series, every admin call, and PBKDF2
once per process for a user and password."""

import re
import socket
import struct
import threading
import time

import pytest
import tsdb_client
from conftest import ROOT, ROOT_PASSWORD, user_cli
from tsdb_client import Client, Pool, TsdbError

T0 = 1_700_000_000_000


def rfc7677():
    """The string macros of tests/rfc7677.h: adjacent literals and earlier macros concatenated."""
    text = (ROOT / "tests" / "rfc7677.h").read_text().replace("\\\n", " ")
    m = {}
    for line in text.splitlines():
        d = re.match(r"\s*#define\s+(\w+)\s+(.*)", line)
        if not d:
            continue
        name, rest, value = d.group(1), d.group(2), ""
        while rest := rest.lstrip():
            if lit := re.match(r'"([^"]*)"', rest):
                value += lit.group(1)
            elif ident := re.match(r"\w+", rest):
                value += m.get(ident.group(0), ident.group(0))  # an earlier macro, or a number
                lit = ident
            else:
                break  # a comment
            rest = rest[lit.end() :]
        m[name] = value
    return m


V = rfc7677()


def fake(first, last):
    """A fake tsdbd of protocol 12 that asks for a login: answers HELLO, AUTH_START with first and AUTH_CONTINUE
    with last. Returns the port and a function that waits for it and gives the client-first and client-final
    messages it got."""
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    got = {"first": "", "final": ""}

    def msg(b):
        return struct.pack("<H", len(b)) + b

    def serve():
        s, _ = lst.accept()
        f = s.makefile("rb")
        while hdr := f.read(9):
            length, rid, op = struct.unpack("<IIB", hdr)
            p = f.read(length - 5)
            if op == 1:
                # TSDB_HELLO_AUTH, two mechanisms: the client picks SCRAM-SHA-256
                body = struct.pack("<IHH", 12, 1, 2) + b"\x0bSCRAM-SHA-1\x0dSCRAM-SHA-256"
            elif op == 29:
                assert p[:14] == b"\x0dSCRAM-SHA-256"
                (n,) = struct.unpack_from("<H", p, 14)
                assert len(p) == 16 + n
                got["first"] = p[16:].decode()
                body = msg(first.encode())
            elif op == 30:
                (n,) = struct.unpack_from("<H", p)
                assert len(p) == 2 + n
                got["final"] = p[2:].decode()
                body = msg(last.encode())
            else:
                raise AssertionError(f"op {op} before the login finished")
            try:
                s.sendall(struct.pack("<IIBi", 9 + len(body), rid, 0, 0) + body)
            except OSError:
                break
            if op == 30:
                break
        s.close()
        lst.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()

    def done():
        th.join(10)
        assert not th.is_alive()
        return got["first"], got["final"]

    return lst.getsockname()[1], done


@pytest.fixture
def fixed_nonce():
    tsdb_client._set_client_nonce(V["RFC7677_CNONCE"])
    try:
        yield
    finally:
        tsdb_client._set_client_nonce(None)


def test_rfc7677_exchange_byte_for_byte(fixed_nonce):
    port, done = fake(V["RFC7677_SERVER_FIRST"], V["RFC7677_SERVER_FINAL"])
    c = Client(port=port, user=V["RFC7677_USER"], password=V["RFC7677_PASSWORD"])
    first, final = done()
    assert first == V["RFC7677_CLIENT_FIRST"]
    assert final == V["RFC7677_CLIENT_FINAL"]
    assert V["RFC7677_NONCE"] == V["RFC7677_CNONCE"] + V["RFC7677_SNONCE"]
    c.close()


def bad_server_messages():
    sf, fin = V["RFC7677_SERVER_FIRST"], V["RFC7677_SERVER_FINAL"]
    wrong_sig = fin[:5] + chr(ord(fin[5]) ^ 1) + fin[6:]  # another base64 letter
    salt = ",s=W22ZaJ0SNY7soEsUEjb6gQ=="
    return [
        (sf, wrong_sig),
        (sf, "e=invalid-proof"),
        (sf, ""),
        (sf, fin + ",x=1"),
        (sf.replace(",i=4096", ",i=4095"), fin),  # a downgrade
        (sf.replace(",i=4096", ",i=4096,x=1"), fin),  # an extension
        (sf.replace("r=rOprNGfwEbeRWgbNEkqO", "r=xOprNGfwEbeRWgbNEkqO"), fin),  # not our nonce
        (f"r={V['RFC7677_CNONCE']}{salt},i=4096", fin),  # no server part of the nonce
        (sf.replace(salt, ",s=W22Z!J0SNY7soEsUEjb6gQ=="), fin),
        ("m=x," + sf, fin),
    ]


@pytest.mark.parametrize("first,last", bad_server_messages())
def test_a_server_that_does_not_prove_the_verifier_fails_the_login(fixed_nonce, first, last):
    port, done = fake(first, last)
    with pytest.raises(TsdbError) as e:
        Client(port=port, user="user", password="pencil")
    assert e.value.code == tsdb_client.ERR_AUTH == -17
    done()


def test_a_server_asking_for_a_login_gets_none_without_a_user():
    port, done = fake("", "")
    with pytest.raises(TsdbError) as e:
        Client(port=port)
    assert e.value.code == tsdb_client.ERR_AUTH
    assert "requires a login" in str(e.value)
    assert done() == ("", ""), "nothing was sent after HELLO"
    with pytest.raises(ValueError, match="password"):
        Client(port=port, user="u")  # refused before it connects


def login(srv, user, password, **kw):
    return Client(port=srv.port, user=user, password=password, **kw)


def test_login_with_the_right_password_a_wrong_one_and_an_unknown_user(auth_server, tmp_path):
    srv = auth_server
    with login(srv, "root", ROOT_PASSWORD) as c:
        me = c.whoami()
        assert (me.name, me.superuser, me.grants) == ("root", True, [])
        assert "root" in repr(me)
        assert c.protocol_version() == 12 == tsdb_client.PROTOCOL_VERSION
        s = c.series("plant/a")
        c.insert_series(s, [T0], [1.5])
    with pytest.raises(TsdbError) as wrong:
        login(srv, "root", "not-it")
    assert wrong.value.code == tsdb_client.ERR_AUTH
    assert tsdb_client.strerror(tsdb_client.ERR_AUTH) == "login failed or required"
    # an unknown user fails the same way, with the same message
    with pytest.raises(TsdbError) as unknown:
        login(srv, "nobody", "not-it")
    assert (unknown.value.code, str(unknown.value)) == (wrong.value.code, str(wrong.value))
    with pytest.raises(TsdbError, match="requires a login") as e:
        Client(port=srv.port)
    assert e.value.code == tsdb_client.ERR_AUTH

    # the password from a file (str or a path): its first line, a CR LF cut
    pf = tmp_path / "pw"
    pf.write_text(ROOT_PASSWORD + "\r\nignored\n")
    for f in (pf, str(pf)):
        with Client(port=srv.port, user="root", password_file=f, require_auth=True) as c:
            assert c.query_raw(s, numpy=False) == ([T0], [1.5])
    pf.write_text("")
    with pytest.raises(TsdbError) as e:
        Client(port=srv.port, user="root", password_file=pf)
    assert e.value.code == tsdb_client.ERR_INVALID
    pf.unlink()
    with pytest.raises(TsdbError) as e:
        Client(port=srv.port, user="root", password_file=pf)
    # a missing file is a mistake to fix, not a network error (ERR_IO) to retry; the error names it (REVIEW3 #3)
    assert e.value.code == tsdb_client.ERR_INVALID and str(pf) in str(e.value)

    # a pool logs in on every connection
    with Pool(port=srv.port, connections=3, user="root", password=ROOT_PASSWORD) as p:
        got = p.query_raw_many([s, s, s])
        assert [(list(ts), list(v)) for ts, v in got] == [([T0], [1.5])] * 3
    with pytest.raises(TsdbError) as e:
        Pool(port=srv.port, connections=2, user="root", password="not-it")
    assert e.value.code == tsdb_client.ERR_AUTH


def test_require_auth_refuses_a_server_without_login(server):
    # credentials are not needed there, and are not sent
    with Client(port=server.port, user="root", password="x") as c:
        me = c.whoami()
        assert (me.name, me.superuser) == ("", True)
        # without login every admin call is refused: users are managed with the offline CLI
        for call in (c.user_list, lambda: c.user_create("x", "pw"), lambda: c.grant("x", "all", "read")):
            with pytest.raises(TsdbError) as e:
                call()
            assert e.value.code == tsdb_client.ERR_DENIED == -16
    with pytest.raises(TsdbError, match="--no-auth") as e:
        Client(port=server.port, user="root", password="x", require_auth=True)
    assert e.value.code == tsdb_client.ERR_AUTH


def denied(call):
    with pytest.raises(TsdbError) as e:
        call()
    assert e.value.code == tsdb_client.ERR_DENIED, str(e.value)


def test_a_tenant_sees_only_its_series(auth_server):
    srv = auth_server
    r = login(srv, "root", ROOT_PASSWORD)
    own = r.series("acme/x")
    foreign = r.series("globex/y")
    r.insert_series(foreign, [T0], [9.0])
    r.user_create("acme/u", "u-pw")
    r.grant("acme/u", "prefix:acme/", "read,insert")

    u = login(srv, "acme/u", "u-pw")
    assert u.series("acme/x", create=False) == own
    u.insert_series(own, [T0], [1.0])
    assert u.query_raw(own, numpy=False) == ([T0], [1.0])
    denied(lambda: u.series("globex/y", create=False))
    # a series that does not exist answers the same
    denied(lambda: u.series("globex/missing", create=False))
    denied(lambda: u.query_raw(foreign))
    denied(lambda: u.series_name(foreign))
    denied(lambda: u.insert_series(foreign, [T0 + 1], [2.0]))
    denied(lambda: u.series("globex/new"))
    denied(lambda: u.delete_range(own, None, None))  # no delete right on its own series either
    # a multi-series query answers what it may
    m = u.query_values_multi([own, foreign], errors="status", numpy=False)
    assert list(m.status) == [0, tsdb_client.ERR_DENIED]
    assert m[0][0] == [T0]
    assert m.error_of(1).code == tsdb_client.ERR_DENIED
    assert r.query_raw(foreign, numpy=False) == ([T0], [9.0])
    u.close()
    r.close()


def test_admin_calls_round_trip(auth_server):
    srv = auth_server
    r = login(srv, "root", ROOT_PASSWORD)
    r.user_create("acme/admin", "a-pw", iterations=5000)
    r.grant("acme/admin", "prefix:acme/", tsdb_client.RIGHT_ADMIN)
    with pytest.raises(TsdbError) as e:
        r.user_create("acme/admin", "again")
    assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(TsdbError) as e:
        r.user_create("x", "pw", iterations=100)
    assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(ValueError):  # the password rule (test_the_password_rule_and_password_files)
        r.user_create("x", "")
    for sel, rights in (("acme/", "read"), ("all", "read,fly"), ("all", 0), ("all", 16), ("all", 1.5)):
        with pytest.raises(ValueError):
            r.grant("acme/admin", sel, rights)

    # the tenant admin creates and manages users under its prefix, and nothing outside it
    a = login(srv, "acme/admin", "a-pw")
    a.user_create("acme/ingest", "i-pw")
    a.grant("acme/ingest", "glob:acme/*/temp", tsdb_client.RIGHT_INSERT | tsdb_client.RIGHT_READ)
    a.grant("acme/ingest", "name:acme/line1", "write")
    denied(lambda: a.user_create("globex/x", "pw"))
    denied(lambda: a.user_create("acme/root2", "pw", superuser=True))
    denied(lambda: a.grant("acme/ingest", "prefix:globex/", "read"))
    denied(lambda: a.user_alter("acme/ingest", superuser=True))

    me = a.whoami()
    assert (me.name, me.superuser, me.grants) == ("acme/admin", False, [("prefix:acme/", "admin")])
    assert sorted(u.name for u in a.user_list()) == ["acme/admin", "acme/ingest"]
    users = {u.name: u for u in r.user_list()}
    assert sorted(users) == ["acme/admin", "acme/ingest", "root"]
    assert not users["acme/ingest"].superuser
    assert sorted(users["acme/ingest"].grants) == [("glob:acme/*/temp", "read,insert"),
                                                  ("name:acme/line1", "insert,delete")]

    # the grants work, and a revoke is seen by the user's next request
    i = login(srv, "acme/ingest", "i-pw")
    t = r.series("acme/site1/temp")
    i.insert_series(t, [T0], [20.0])
    a.revoke("acme/ingest", "glob:acme/*/temp", "insert")
    denied(lambda: i.insert_series(t, [T0 + 1], [21.0]))
    assert i.query_raw(t, numpy=False) == ([T0], [20.0])

    # a new password: the old one stops working
    a.user_password("acme/ingest", "i-pw2")
    with pytest.raises(TsdbError) as e:
        login(srv, "acme/ingest", "i-pw")
    assert e.value.code == tsdb_client.ERR_AUTH
    login(srv, "acme/ingest", "i-pw2").close()

    # superuser flag, then drop: the user's open connection is closed, and it cannot log in
    r.user_alter("acme/ingest", superuser=True)
    with login(srv, "acme/ingest", "i-pw2") as c:
        assert c.whoami().superuser
    r.user_alter("acme/ingest", superuser=False)
    a.user_drop("acme/ingest")
    with pytest.raises(TsdbError):
        i.whoami()
    with pytest.raises(TsdbError) as e:
        login(srv, "acme/ingest", "i-pw2")
    assert e.value.code == tsdb_client.ERR_AUTH
    denied(lambda: r.user_drop("root"))  # the last superuser stays
    assert sorted(u.name for u in r.user_list()) == ["acme/admin", "root"]
    a.close()
    r.close()


def test_pbkdf2_runs_once_per_user_and_password(auth_server):
    srv = auth_server
    with login(srv, "root", ROOT_PASSWORD) as r:
        r.user_create("cache/u", "c-pw")  # the verifier is PBKDF2 too
    before = tsdb_client._pbkdf2_runs()
    clients = [login(srv, "cache/u", "c-pw") for _ in range(5)]
    assert tsdb_client._pbkdf2_runs() == before + 1
    with Pool(port=srv.port, connections=4, user="cache/u", password="c-pw") as p:
        assert p.connections == 4
    assert tsdb_client._pbkdf2_runs() == before + 1, "9 logins, one PBKDF2"
    with pytest.raises(TsdbError):
        login(srv, "cache/u", "wrong")
    assert tsdb_client._pbkdf2_runs() == before + 2
    login(srv, "cache/u", "c-pw").close()
    assert tsdb_client._pbkdf2_runs() == before + 2
    for c in clients:
        assert c.whoami().name == "cache/u"
        c.close()


def test_a_cli_grant_is_seen_while_running(auth_server):
    """A user and a grant added by the operator's CLI while tsdbd runs reach it within the reload tick."""
    srv = auth_server
    r = login(srv, "root", ROOT_PASSWORD)
    s = r.series("plant/b")
    user_cli(srv.dir, "op-pw", "add", "operator")
    deadline = time.time() + 10
    while True:
        try:
            o = login(srv, "operator", "op-pw")
            break
        except TsdbError as e:
            if e.code != tsdb_client.ERR_AUTH or time.time() > deadline:
                raise
            time.sleep(0.05)
    denied(lambda: o.series("plant/b", create=False))
    user_cli(srv.dir, None, "grant", "operator", "read", "name:plant/b")
    while True:
        try:
            assert o.series("plant/b", create=False) == s
            break
        except TsdbError as e:
            if e.code != tsdb_client.ERR_DENIED or time.time() > deadline:
                raise
            time.sleep(0.05)
    o.close()
    r.close()


def test_the_module_names_the_auth_constants():
    assert (tsdb_client.RIGHT_READ, tsdb_client.RIGHT_INSERT, tsdb_client.RIGHT_DELETE) == (1, 2, 4)
    assert (tsdb_client.RIGHT_ADMIN, tsdb_client.RIGHT_WRITE) == (8, 6)
    assert tsdb_client.SCRAM_ITERATIONS == 4096
    assert tsdb_client.PASSWORD_MAX == 1023
    assert tsdb_client.strerror(tsdb_client.ERR_DENIED) == "permission denied"


def test_a_cold_pool_runs_pbkdf2_once_and_keeps_other_users_keys(auth_server):
    """REVIEW3 #1: the connections of a Pool start together; one PBKDF2 for all of them, one cache entry."""
    srv = auth_server
    with login(srv, "root", ROOT_PASSWORD) as r:
        # iterations high enough that all connections of the pool miss before the first is done
        r.user_create("cold/a", "a-pw", iterations=100_000)
        r.user_create("cold/b", "b-pw", iterations=100_000)
    login(srv, "root", ROOT_PASSWORD).close()
    before = tsdb_client._pbkdf2_runs()
    with Pool(port=srv.port, connections=24, user="cold/a", password="a-pw") as p:
        assert p.connections == 24
    assert tsdb_client._pbkdf2_runs() == before + 1, "a cold pool of 24, one PBKDF2"
    with Pool(port=srv.port, connections=16, user="cold/b", password="b-pw"):
        pass
    assert tsdb_client._pbkdf2_runs() == before + 2, "a cold pool of 16, one PBKDF2"
    login(srv, "root", ROOT_PASSWORD).close()
    login(srv, "cold/a", "a-pw").close()
    assert tsdb_client._pbkdf2_runs() == before + 2, "the other users' keys survived the cold pools"


def test_the_password_rule_and_password_files(auth_server, tmp_path):
    """REVIEW3 #2, #3: 1 to 1023 bytes in every client; a file's first line without LF or CR LF."""
    srv = auth_server
    p1023 = "".join(chr(32 + i % 95) for i in range(1023))
    p1024 = p1023 + "x"
    with login(srv, "root", ROOT_PASSWORD) as r:
        for bad in ("", p1024, "a\0b"):
            with pytest.raises(ValueError):
                r.user_create("pw/0", bad)
            with pytest.raises(ValueError):
                r.user_password("root", bad)
        r.user_create("pw/1", "1")
        r.user_create("pw/1023", p1023)
    login(srv, "pw/1", "1").close()
    login(srv, "pw/1023", p1023).close()
    for user, bad in (("pw/1", ""), ("pw/1023", p1024)):
        with pytest.raises(ValueError):
            login(srv, user, bad)
    with pytest.raises(ValueError):
        Client(port=1, user="pw/1", password="")  # before connecting: nothing listens there
    with pytest.raises(UnicodeEncodeError):
        login(srv, "pw/1", "pw\ud800")  # a str that is no UTF-8 is never sent

    pf = tmp_path / "pw"

    def file_login(user, content):
        pf.write_bytes(content)
        Client(port=srv.port, user=user, password_file=pf).close()

    for user, good in (("pw/1", b"1"), ("pw/1", b"1\n"), ("pw/1", b"1\r\nx\n")):
        file_login(user, good)
    file_login("pw/1023", p1023.encode())
    file_login("pw/1023", p1023.encode() + b"\r\n")
    bad = [("pw/1023", p1023.encode() + b"\r"), ("pw/1023", p1024.encode() + b"\n")]
    bad += [("pw/1", b""), ("pw/1", b"\n1\n"), ("pw/1", b"\r\n"), ("pw/1", b"1\x002\n")]
    for user, content in bad:
        with pytest.raises(TsdbError) as e:
            file_login(user, content)
        assert e.value.code == tsdb_client.ERR_INVALID and str(pf) in str(e.value), content


def test_rights_print_as_they_parse(auth_server):
    """REVIEW3 #7: the rights of every grant read back print in the grammar grant() and tsdbd read: the 15
    non-empty combinations of the four rights, granted again from their printed form, give the same grants."""
    srv = auth_server
    names = ["read", "insert", "delete", "admin"]
    with login(srv, "root", ROOT_PASSWORD) as r:
        r.user_create("rt/a", "a-pw")
        r.user_create("rt/b", "b-pw")
        for bits in range(1, 16):
            r.grant("rt/a", f"name:rt/{bits}", ",".join(n for i, n in enumerate(names) if bits >> i & 1))
        printed = {u.name: sorted(u.grants) for u in r.user_list() if u.name.startswith("rt/")}
        assert len(printed["rt/a"]) == 15
        for sel, rights in printed["rt/a"]:
            r.grant("rt/b", sel, rights)
        assert sorted(next(u for u in r.user_list() if u.name == "rt/b").grants) == printed["rt/a"]
        r.grant("rt/b", "name:rt/sp", "read , insert")  # spaces around a name, as tsdbd reads them
        assert ("name:rt/sp", "read,insert") in next(u for u in r.user_list() if u.name == "rt/b").grants
        for nothing in ("none", "0x10"):  # they parse, and the server refuses to grant them
            with pytest.raises(TsdbError) as e:
                r.grant("rt/b", "name:rt/x", nothing)
            assert e.value.code == tsdb_client.ERR_INVALID
        for bad in ("read\t", "reads", "read,,insert", "", "0x1"):
            with pytest.raises(ValueError):
                r.grant("rt/b", "name:rt/x", bad)


def test_an_unknown_selector_kind_is_kept():
    """REVIEW3 #6: a grant of a kind a later server adds (9 here) reaches whoami and user_list as "kind#9:TEXT",
    with bits no client knows as 0xHH, and the connection stays usable."""
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    user = b"\x01\x00u\x00\x02\x00\x00\x00" + b"\x09\x45\x03\x00x/*" + b"\x01\x01\x02\x00p/"

    def serve():
        s, _ = lst.accept()
        f = s.makefile("rb")
        while hdr := f.read(9):
            length, rid, op = struct.unpack("<IIB", hdr)
            f.read(length - 5)
            body = {1: struct.pack("<IHH", 12, 0, 0), 31: user, 38: b"\x01\x01\x00\x00\x00" + user}.get(op, b"")
            s.sendall(struct.pack("<IIBi", 9 + len(body), rid, 0, 0) + body)
        s.close()
        lst.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    with Client(port=lst.getsockname()[1]) as c:
        for u in (c.whoami(), c.user_list()[0], c.whoami()):
            assert u.name == "u"
            assert u.grants == [("kind#9:x/*", "read,delete,0x40"), ("prefix:p/", "read")]
        with pytest.raises(ValueError):
            c.grant("u", "kind#9:x/*", "read")
    th.join(10)
