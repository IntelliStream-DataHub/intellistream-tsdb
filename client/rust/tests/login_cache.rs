// SPDX-License-Identifier: Apache-2.0
//! The key cache: PBKDF2 runs once per process for a user, salt and password, not once per connection, also
//! across the connections of a Pool that start together (REVIEW3 #1). A test binary of its own, so no other test's
//! login moves the counter.

mod common;

use common::Server;
use tsdb_client::test_hooks::pbkdf2_runs;
use tsdb_client::{Client, ConnectOptions, Error, Pool};

fn opts(user: &str, pw: &str) -> ConnectOptions {
    ConnectOptions::new().user(user).password(pw)
}

/// The PBKDF2 runs of one login as `user`.
fn runs_of_login(srv: &Server, user: &str, pw: &str) -> u64 {
    let before = pbkdf2_runs();
    Client::connect_opts(srv.addr(), &opts(user, pw)).unwrap();
    pbkdf2_runs() - before
}

#[test]
fn pbkdf2_runs_once_per_process() {
    let srv = Server::start_auth("cache-pw");
    let root = opts("root", "cache-pw");
    let before = pbkdf2_runs();
    let mut clients = Vec::new();
    for _ in 0..5 {
        clients.push(Client::connect_opts(srv.addr(), &root).unwrap());
    }
    let pool = Pool::connect_opts(srv.addr(), 4, root.clone()).unwrap();
    pool.map(8, |c, _| c.whoami().map(|_| ())).unwrap();
    assert_eq!(pbkdf2_runs() - before, 1, "PBKDF2 once for 9 logins");

    // another password is another entry (and fails); the right one is still cached
    let wrong = opts("root", "other");
    assert!(matches!(Client::connect_opts(srv.addr(), &wrong).unwrap_err(), Error::Auth(_)));
    assert_eq!(pbkdf2_runs() - before, 2);
    Client::connect_opts(srv.addr(), &root).unwrap();
    assert_eq!(pbkdf2_runs() - before, 2);

    // iterations high enough that the connections of a pool all miss before the first is done
    clients[0].user_create("pool", "pool-pw", false, Some(100_000)).unwrap();
    clients[0].user_create("pool2", "pool2-pw", false, Some(100_000)).unwrap();
    // a cold pool of 24: one PBKDF2, the others wait for it
    let start = pbkdf2_runs();
    let pool = Pool::connect_opts(srv.addr(), 24, opts("pool", "pool-pw")).unwrap();
    pool.map(24, |c, _| c.whoami().map(|_| ())).unwrap();
    assert_eq!(pbkdf2_runs() - start, 1, "a cold pool of 24 runs PBKDF2 once");
    // another user's cold pool of 16 leaves root's and pool's entries in the cache
    let start = pbkdf2_runs();
    let _pool2 = Pool::connect_opts(srv.addr(), 16, opts("pool2", "pool2-pw")).unwrap();
    assert_eq!(pbkdf2_runs() - start, 1, "a cold pool of 16 runs PBKDF2 once");
    assert_eq!(runs_of_login(&srv, "root", "cache-pw"), 0, "root's entry survived the cold pool");
    assert_eq!(runs_of_login(&srv, "pool", "pool-pw"), 0, "pool's entry survived the cold pool");
}
