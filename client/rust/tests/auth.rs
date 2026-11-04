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
    assert_eq!(c.protocol_version().unwrap(), 12);
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
    assert!(matches!(Client::connect_opts(srv.addr(), &opts).unwrap_err(), Error::Io(_)));
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
