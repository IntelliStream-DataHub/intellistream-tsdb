// SPDX-License-Identifier: Apache-2.0
use tsdb_client::{Client, ConnectOptions, QueryOpts, Rights, Selector};

fn main() -> Result<(), tsdb_client::Error> {
    // Log in as the superuser of the quick start: the address, the user and its password file.
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let user = args.next().unwrap_or_else(|| "admin".to_string());
    let password_file = args.next().unwrap_or_else(|| "/tmp/tsdb-demo/password".to_string());
    let mut admin = Client::connect_opts(addr.as_str(), &ConnectOptions::new().user(user).password_file(password_file))?;

    // A user for a collector of the tenant "acme": it may add rows to the series whose names start with
    // "acme/", and do nothing else.
    admin.user_create("acme/ingest", "collector-password-5", false, None)?;
    admin.grant("acme/ingest", &Selector::Prefix("acme/".into()), Rights::INSERT)?;
    for u in admin.user_list()? {
        let grants: Vec<String> = u.grants.iter().map(|g| format!("{} on {}", g.rights, g.selector)).collect();
        println!("{}: {}", u.name, if u.superuser { "superuser".to_string() } else { grants.join(", ") });
    }

    // The collector logs in with its own password and writes a row.
    let login = ConnectOptions::new().user("acme/ingest").password("collector-password-5");
    let mut collector = Client::connect_opts(addr.as_str(), &login)?;
    let pump = collector.series("acme/pump1")?;
    collector.insert_series(pump, &[1_700_000_000_000], &[1.5])?;

    // It may not read the row back, nor see a series of another tenant.
    if let Err(e) = collector.query_points(pump, .., QueryOpts::asc()) {
        println!("read acme/pump1: {e}");
    }
    if let Err(e) = collector.series("globex/pump1") {
        println!("globex/pump1: {e}");
    }
    collector.close()?;

    // Drop the user again, so that the example can run once more.
    admin.user_drop("acme/ingest")?;
    admin.close()
}
