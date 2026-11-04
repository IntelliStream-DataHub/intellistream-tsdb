//! The key cache: PBKDF2 runs once per process for a user, salt and password, not once per connection, also
//! across the connections of a Pool. A test binary of its own, so no other test's login moves the counter.

mod common;

use common::Server;
use tsdb_client::test_hooks::pbkdf2_runs;
use tsdb_client::{Client, ConnectOptions, Error, Pool};

#[test]
fn pbkdf2_runs_once_per_process() {
    let srv = Server::start_auth("cache-pw");
    let opts = ConnectOptions::new().user("root").password("cache-pw");
    let before = pbkdf2_runs();
    let mut clients = Vec::new();
    for _ in 0..5 {
        clients.push(Client::connect_opts(srv.addr(), &opts).unwrap());
    }
    let pool = Pool::connect_opts(srv.addr(), 4, opts.clone()).unwrap();
    pool.map(8, |c, _| c.whoami().map(|_| ())).unwrap();
    assert_eq!(pbkdf2_runs() - before, 1, "PBKDF2 once for 9 logins");

    // another password is another entry (and fails); the right one is still cached
    let wrong = ConnectOptions::new().user("root").password("other");
    assert!(matches!(Client::connect_opts(srv.addr(), &wrong).unwrap_err(), Error::Auth(_)));
    assert_eq!(pbkdf2_runs() - before, 2);
    Client::connect_opts(srv.addr(), &opts).unwrap();
    assert_eq!(pbkdf2_runs() - before, 2);
}
