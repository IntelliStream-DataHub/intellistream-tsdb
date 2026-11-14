// SPDX-License-Identifier: Apache-2.0
//! Login and access control against a real tsdbd (protocol 12): USERS made by the CLI, a login with the right
//! and a wrong password, an unknown user, require_auth against `--no-auth`, DENIED on another tenant's series,
//! and a round trip through every admin call.

mod common;

use common::{Server, T0, test_dir, user_cli};
use tsdb_client::{Client, ConnectOptions, Error, Pool, QueryOpts, Rights, Selector, Status};

const ROOT_PW: &str = "root-secret";

fn login(srv: &Server, user: &str, pw: &str) -> tsdb_client::Result<Client> {
    Client::connect_opts(srv.addr(), &ConnectOptions::new().user(user).password(pw))
}

fn root(srv: &Server) -> Client {
    login(srv, "root", ROOT_PW).expect("root logs in")
}

#[test]
fn login_with_the_right_password_wrong_password_and_unknown_user() {
    let srv = Server::start_auth(ROOT_PW);
    let mut c = root(&srv);
    let me = c.whoami().unwrap();
    assert_eq!((me.name.as_str(), me.superuser, me.grants.len()), ("root", true, 0));
    assert_eq!(c.protocol_version().unwrap(), 15);
    let id = c.series("plant/a").unwrap();
    c.insert_series(id, &[T0], &[1.5]).unwrap();

    let wrong = login(&srv, "root", "not-it").unwrap_err();
    assert!(matches!(wrong, Error::Auth(_)), "{wrong:?}");
    assert_eq!((wrong.status(), wrong.code()), (Some(Status::Auth), -17));
    // an unknown user fails the same way, with the same message
    let unknown = login(&srv, "nobody", "not-it").unwrap_err();
    assert_eq!(unknown.to_string(), wrong.to_string());
    // no login at all: the client says why, before sending anything but HELLO
    let e = Client::connect(srv.addr()).unwrap_err();
    assert!(matches!(&e, Error::Auth(m) if m.contains("requires a login")), "{e:?}");
    // a name SCRAM cannot carry is refused before it is sent
    assert!(matches!(login(&srv, "a,b", "x").unwrap_err(), Error::InvalidArgument(_)));

    // the password from a file: its first line, a CR LF cut
    let pf = test_dir("tsdb-rust-pw");
    std::fs::write(&pf, format!("{ROOT_PW}\r\nignored\n")).unwrap();
    let opts = ConnectOptions::new().user("root").password_file(&pf).require_auth(true);
    let mut c2 = Client::connect_opts(srv.addr(), &opts).unwrap();
    assert_eq!(c2.query_points(id, .., QueryOpts::asc()).unwrap().values, vec![1.5]);
    std::fs::write(&pf, "").unwrap();
    assert!(matches!(Client::connect_opts(srv.addr(), &opts).unwrap_err(), Error::InvalidArgument(_)));
    std::fs::remove_file(&pf).unwrap();
    // a missing file is the caller's mistake, not a network error to retry (REVIEW3 #3)
    assert!(matches!(Client::connect_opts(srv.addr(), &opts).unwrap_err(), Error::InvalidArgument(_)));
    assert!(matches!(
        Client::connect_opts(srv.addr(), &ConnectOptions::new().user("root")).unwrap_err(),
        Error::InvalidArgument(_)
    ));
    assert!(!format!("{opts:?} {:?}", ConnectOptions::new().password(ROOT_PW)).contains(ROOT_PW));

    // a pool logs in on every connection
    let pool = Pool::connect_opts(srv.addr(), 3, ConnectOptions::new().user("root").password(ROOT_PW)).unwrap();
    let got = pool.map(6, |c, _| Ok(c.whoami()?.name)).unwrap();
    assert!(got.iter().all(|n| n == "root"));
}

#[test]
fn require_auth_refuses_a_server_without_login() {
    let srv = Server::start(); // --no-auth
    let opts = ConnectOptions::new().user("root").password("x");
    // credentials are not needed there, and are not sent
    let mut c = Client::connect_opts(srv.addr(), &opts).unwrap();
    let me = c.whoami().unwrap();
    assert_eq!((me.name.as_str(), me.superuser), ("", true));
    // without login every admin call is refused (A28): users are managed with the offline CLI
    assert_eq!(c.user_list().unwrap_err().status(), Some(Status::Denied));
    assert_eq!(c.user_create("x", "pw", false, None).unwrap_err().status(), Some(Status::Denied));
    assert_eq!(c.grant("x", &Selector::All, Rights::READ).unwrap_err().status(), Some(Status::Denied));
    let e = Client::connect_opts(srv.addr(), &opts.require_auth(true)).unwrap_err();
    assert!(matches!(&e, Error::Auth(m) if m.contains("--no-auth")), "{e:?}");
}

#[test]
fn a_tenant_sees_only_its_series() {
    let srv = Server::start_auth(ROOT_PW);
    let mut r = root(&srv);
    let own = r.series("acme/x").unwrap();
    let foreign = r.series("globex/y").unwrap();
    r.insert_series(foreign, &[T0], &[9.0]).unwrap();
    r.user_create("acme/u", "u-pw", false, None).unwrap();
    r.grant("acme/u", &"prefix:acme/".parse().unwrap(), "read,insert".parse().unwrap()).unwrap();

    let mut u = login(&srv, "acme/u", "u-pw").unwrap();
    assert_eq!(u.series_lookup("acme/x").unwrap(), own);
    u.insert_series(own, &[T0], &[1.0]).unwrap();
    assert_eq!(u.query_points(own, .., QueryOpts::asc()).unwrap().values, vec![1.0]);
    let denied = |e: Error| assert_eq!((e.status(), e.code()), (Some(Status::Denied), -16), "{e}");
    denied(u.series_lookup("globex/y").unwrap_err());
    // a series that does not exist answers the same (A16)
    denied(u.series_lookup("globex/missing").unwrap_err());
    denied(u.query_points(foreign, .., QueryOpts::asc()).unwrap_err());
    denied(u.series_name(foreign).unwrap_err());
    denied(u.insert_series(foreign, &[T0 + 1], &[2.0]).unwrap_err());
    denied(u.series("globex/new").unwrap_err());
    // no delete right on its own series either
    denied(u.delete_range(own, ..).unwrap_err());
    // QUERY_MULTI answers what it may (A17)
    let m = u.query_rows_multi(&[own, foreign], .., QueryOpts::asc(), Default::default()).unwrap();
    assert_eq!(m[0].as_ref().unwrap().ts, vec![T0]);
    assert_eq!(m[1].as_ref().unwrap_err().status(), Some(Status::Denied));
    assert_eq!(r.query_points(foreign, .., QueryOpts::asc()).unwrap().values, vec![9.0]);
}

#[test]
fn admin_calls_round_trip() {
    let srv = Server::start_auth(ROOT_PW);
    let mut r = root(&srv);
    r.user_create("acme/admin", "a-pw", false, Some(5000)).unwrap();
    r.grant("acme/admin", &Selector::Prefix("acme/".into()), Rights::ADMIN).unwrap();
    assert_eq!(r.user_create("acme/admin", "again", false, None).unwrap_err().status(), Some(Status::Invalid));
    assert!(matches!(r.user_create("x", "", false, None).unwrap_err(), Error::InvalidArgument(_)));
    assert!(matches!(r.user_create("x", "pw", false, Some(100)).unwrap_err(), Error::InvalidArgument(_)));

    // the tenant admin creates and manages users under its prefix, and nothing outside it
    let mut a = login(&srv, "acme/admin", "a-pw").unwrap();
    a.user_create("acme/ingest", "i-pw", false, None).unwrap();
    a.grant("acme/ingest", &"glob:acme/*/temp".parse().unwrap(), Rights::INSERT | Rights::READ).unwrap();
    a.grant("acme/ingest", &Selector::Name("acme/line1".into()), Rights::WRITE).unwrap();
    let denied = |e: Error| assert_eq!(e.status(), Some(Status::Denied), "{e}");
    denied(a.user_create("globex/x", "pw", false, None).unwrap_err());
    denied(a.user_create("acme/root2", "pw", true, None).unwrap_err());
    denied(a.grant("acme/ingest", &Selector::Prefix("globex/".into()), Rights::READ).unwrap_err());
    denied(a.user_alter("acme/ingest", true).unwrap_err());

    let me = a.whoami().unwrap();
    assert_eq!(me.name, "acme/admin");
    assert_eq!(me.grants.len(), 1);
    assert_eq!((&me.grants[0].selector, me.grants[0].rights), (&Selector::Prefix("acme/".into()), Rights::ADMIN));
    let names = |c: &mut Client| {
        let mut v: Vec<String> = c.user_list().unwrap().into_iter().map(|u| u.name).collect();
        v.sort();
        v
    };
    assert_eq!(names(&mut a), ["acme/admin", "acme/ingest"]);
    assert_eq!(names(&mut r), ["acme/admin", "acme/ingest", "root"]);
    let ingest = r.user_list().unwrap().into_iter().find(|u| u.name == "acme/ingest").unwrap();
    assert!(!ingest.superuser);
    let mut grants: Vec<String> = ingest.grants.iter().map(|g| format!("{} {}", g.selector, g.rights)).collect();
    grants.sort();
    assert_eq!(grants, ["glob:acme/*/temp read,insert", "name:acme/line1 insert,delete"]);

    // the user's grants work, and a revoke is seen by its next request
    let mut i = login(&srv, "acme/ingest", "i-pw").unwrap();
    let t = r.series("acme/site1/temp").unwrap();
    i.insert_series(t, &[T0], &[20.0]).unwrap();
    a.revoke("acme/ingest", &"glob:acme/*/temp".parse().unwrap(), Rights::INSERT).unwrap();
    denied(i.insert_series(t, &[T0 + 1], &[21.0]).unwrap_err());
    assert_eq!(i.query_points(t, .., QueryOpts::asc()).unwrap().values, vec![20.0]);

    // a new password: the old one stops working
    a.user_password("acme/ingest", "i-pw2", None).unwrap();
    assert!(matches!(login(&srv, "acme/ingest", "i-pw").unwrap_err(), Error::Auth(_)));
    login(&srv, "acme/ingest", "i-pw2").unwrap();

    // superuser flag, then drop: the user's open connection is closed, and it cannot log in
    r.user_alter("acme/ingest", true).unwrap();
    assert!(login(&srv, "acme/ingest", "i-pw2").unwrap().whoami().unwrap().superuser);
    r.user_alter("acme/ingest", false).unwrap();
    a.user_drop("acme/ingest").unwrap();
    assert!(i.whoami().is_err());
    assert!(matches!(login(&srv, "acme/ingest", "i-pw2").unwrap_err(), Error::Auth(_)));
    denied(r.user_drop("root").unwrap_err()); // the last superuser stays
    assert_eq!(names(&mut r), ["acme/admin", "root"]);
}

/// A grant added by the operator's CLI while tsdbd runs reaches the user within the reload tick.
#[test]
fn a_cli_grant_is_seen_while_running() {
    let srv = Server::start_auth(ROOT_PW);
    let mut r = root(&srv);
    let id = r.series("plant/b").unwrap();
    user_cli(&srv.dir, Some("op-pw"), &["add", "operator"]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut o = loop {
        match login(&srv, "operator", "op-pw") {
            Ok(c) => break c,
            Err(Error::Auth(_)) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(50))
            }
            Err(e) => panic!("{e}"),
        }
    };
    assert_eq!(o.series_lookup("plant/b").unwrap_err().status(), Some(Status::Denied));
    user_cli(&srv.dir, None, &["grant", "operator", "read", "name:plant/b"]);
    loop {
        match o.series_lookup("plant/b") {
            Ok(got) => break assert_eq!(got, id),
            Err(e) if e.status() == Some(Status::Denied) && std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(50))
            }
            Err(e) => panic!("{e}"),
        }
    }
}

/// A login with the password in a file holding `content`.
fn file_login(srv: &Server, user: &str, content: &[u8]) -> tsdb_client::Result<Client> {
    let f = srv.dir.with_extension(format!("pw-{}", content.len()));
    std::fs::write(&f, content).unwrap();
    let r = Client::connect_opts(srv.addr(), &ConnectOptions::new().user(user).password_file(&f));
    std::fs::remove_file(&f).unwrap();
    r
}

fn invalid(r: tsdb_client::Result<impl std::fmt::Debug>) -> String {
    match r {
        Err(e @ Error::InvalidArgument(_)) => {
            assert_eq!(e.code(), -4);
            e.to_string()
        }
        other => panic!("not InvalidArgument: {other:?}"),
    }
}

/// REVIEW3 #2, #3: one password rule in every client: 1 to 1023 bytes; a file's first line without "\n" or
/// "\r\n"; anything else, and a file that is not there, is InvalidArgument (-4) naming the file, before a login.
#[test]
fn the_password_rule_and_password_files() {
    let srv = Server::start_auth(ROOT_PW);
    let p1023: String = (0..1023).map(|i| char::from(b' ' + (i % 95) as u8)).collect();
    let p1024 = format!("{p1023}x");
    let mut r = root(&srv);
    invalid(r.user_create("p0", "", false, None));
    invalid(r.user_create("p0", &p1024, false, None));
    invalid(r.user_create("p0", "a\0b", false, None));
    r.user_create("p1", "1", false, None).unwrap();
    r.user_create("p1023", &p1023, false, None).unwrap();
    invalid(r.user_password("p1", "", None));
    invalid(r.user_password("p1", &p1024, None));

    login(&srv, "p1", "1").unwrap();
    login(&srv, "p1023", &p1023).unwrap();
    invalid(login(&srv, "p1", ""));
    invalid(login(&srv, "p1023", &p1024));
    // refused before connecting: no server listens there
    invalid(Client::connect_opts("127.0.0.1:1", &ConnectOptions::new().user("p1").password("")));

    file_login(&srv, "p1", b"1").unwrap();
    file_login(&srv, "p1", b"1\n").unwrap();
    file_login(&srv, "p1", b"1\r\nx\n").unwrap();
    file_login(&srv, "p1023", p1023.as_bytes()).unwrap();
    file_login(&srv, "p1023", format!("{p1023}\r\n").as_bytes()).unwrap();
    let e = invalid(file_login(&srv, "p1023", format!("{p1023}\r").as_bytes())); // the '\r' is the 1024th byte
    assert!(e.contains("password file") && e.contains("1 to 1023 bytes"), "{e}");
    invalid(file_login(&srv, "p1023", format!("{p1024}\n").as_bytes()));
    for bad in [&b""[..], b"\n1\n", b"\r\n", b"1\x002\n"] {
        invalid(file_login(&srv, "p1", bad));
    }
    let missing = srv.dir.join("no-such-password-file");
    let e = invalid(Client::connect_opts(srv.addr(), &ConnectOptions::new().user("p1").password_file(&missing)));
    assert!(e.contains(&missing.display().to_string()), "the error names the file: {e}");
}

/// REVIEW3 #6: a grant of a kind a later server adds reaches whoami and user_list as Selector::Other, and the
/// connection stays usable. A fake server of protocol 12 without login answers with one.
#[test]
fn an_unknown_selector_kind_is_kept() {
    use std::io::{Read, Write};
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        // a user "u": no flags, two grants: kind 9 "x/*" with read,delete and a bit no client knows, prefix "p/"
        let mut user = vec![1, 0, b'u', 0, 2, 0, 0, 0];
        user.extend_from_slice(&[9, 0x45, 3, 0, b'x', b'/', b'*', 1, 1, 2, 0, b'p', b'/']);
        let mut h = [0u8; 9];
        while s.read_exact(&mut h).is_ok() {
            let mut payload = vec![0; u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize - 5];
            s.read_exact(&mut payload).unwrap();
            let body: Vec<u8> = match h[8] {
                1 => [12u32.to_le_bytes(), [0; 4]].concat(),
                31 => user.clone(),
                38 => [&[1u8, 1, 0, 0, 0][..], &user].concat(), // the last chunk, one user
                _ => Vec::new(),
            };
            let mut reply = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            reply.extend_from_slice(&h[4..8]);
            reply.extend_from_slice(&[0, 0, 0, 0, 0]);
            reply.extend_from_slice(&body);
            if s.write_all(&reply).is_err() {
                break;
            }
        }
    });
    let mut c = Client::connect(&addr).unwrap();
    for u in [c.whoami().unwrap(), c.user_list().unwrap().remove(0), c.whoami().unwrap()] {
        assert_eq!(u.name, "u");
        assert!(matches!(&u.grants[0].selector, Selector::Other { kind: 9, text } if text == "x/*"));
        assert_eq!(u.grants[0].selector.to_string(), "kind#9:x/*");
        assert_eq!(u.grants[0].rights.to_string(), "read,delete,0x40");
        assert_eq!(u.grants[0].rights.to_string().parse::<Rights>().unwrap(), u.grants[0].rights);
        assert_eq!(u.grants[1].selector, Selector::Prefix("p/".into()));
    }
    let other = c.whoami().unwrap().grants[0].selector.clone();
    assert!(matches!(c.grant("u", &other, Rights::READ), Err(Error::InvalidArgument(_))));
    drop(c);
    h.join().unwrap();
}
