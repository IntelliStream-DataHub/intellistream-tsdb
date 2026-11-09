// SPDX-License-Identifier: Apache-2.0
//! The README's quick start is `examples/quickstart.rs`, and it runs against a live server that requires a login.

mod common;

use std::process::Command;

use common::Server;

/// An example's source without its SPDX line: the README shows the code only.
fn example(name: &str) -> String {
    let path = format!("{}/examples/{name}.rs", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(path).unwrap();
    match text.split_once('\n') {
        Some((first, rest)) if first.starts_with("// SPDX-License-Identifier:") => rest.to_string(),
        _ => text,
    }
}

#[test]
fn readme_quick_start_is_the_example() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{dir}/README.md")).unwrap();
    let example = example("quickstart");
    let start = readme.find("## Quick start").expect("a Quick start section");
    let code = readme[start..].split("```rust\n").nth(1).and_then(|s| s.split("\n```").next()).unwrap();
    assert_eq!(code.trim_end(), example.trim_end(), "README.md quick start differs from examples/quickstart.rs");
}

/// Runs an example against a server that requires a login, set up as the README's step 1 does (the superuser
/// `admin`, added by the CLI), logged in as `admin` through a password file; returns what it printed and whether it
/// succeeded.
fn run_logged_in(example: &str) -> std::process::Output {
    let dir = common::test_dir("tsdb-readme-auth");
    common::user_cli(&dir, Some("example-pw"), &["add", "admin", "--superuser"]);
    let srv = Server::spawn(dir, true);
    let pw_dir = common::test_dir("tsdb-readme-pw");
    std::fs::create_dir_all(&pw_dir).unwrap();
    let password_file = pw_dir.join("password");
    std::fs::write(&password_file, "example-pw\n").unwrap();
    let out = Command::new(env!("CARGO"))
        .args(["run", "--quiet", "--example", example, "--", &srv.addr(), "admin"])
        .arg(&password_file)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    std::fs::remove_dir_all(pw_dir).ok();
    out
}

#[test]
fn quick_start_runs() {
    let out = run_logged_in("quickstart");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "quickstart failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    let count = |p: &str| stdout.lines().filter(|l| l.starts_with(p)).count();
    assert_eq!((count("raw "), count("hour "), count("day ")), (3, 6, 2), "{stdout}");
    assert!(stdout.contains("raw  1700085540000 59"), "{stdout}");
    // Oslo days begin at 23:00 UTC in November
    assert!(
        stdout.ends_with("day  1699916400000 count 60 sum 1770\nday  1700002800000 count 1380 sum 40710\n"),
        "{stdout}"
    );
}

#[test]
fn readme_analytics_section_is_the_example() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{dir}/README.md")).unwrap();
    let example = example("analytics");
    let start = readme.find("## Time-weighted averages").expect("an analytics section");
    let section = &readme[start..];
    let code = section.split("```rust\n").nth(1).and_then(|s| s.split("\n```").next()).unwrap();
    assert_eq!(code.trim_end(), example.trim_end(), "README.md analytics differs from examples/analytics.rs");
    let printed = section.split("```text\n").nth(1).and_then(|s| s.split("```").next()).unwrap();
    assert_eq!(printed, analytics_output(), "README.md shows other output than the example prints");
}

fn analytics_output() -> &'static str {
    "hour 1699999200000 avg 40.0 twa 25.0\nhour 1700002800000 avg 70.0 twa 70.0\nspread range 60 stddev 28.72\n\
     chart 100 of 10000 points\n"
}

#[test]
fn analytics_example_runs() {
    let out = run_logged_in("analytics");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "analytics failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(stdout, analytics_output());
}

#[test]
fn readme_users_section_is_the_example() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{dir}/README.md")).unwrap();
    let example = example("users");
    let start = readme.find("## Users and rights").expect("a Users and rights section");
    let section = &readme[start..];
    let code = section.split("```rust\n").nth(1).and_then(|s| s.split("\n```").next()).unwrap();
    assert_eq!(code.trim_end(), example.trim_end(), "README.md users section differs from examples/users.rs");
    let printed = section.split("```text\n").nth(1).and_then(|s| s.split("```").next()).unwrap();
    assert_eq!(printed, users_output(), "README.md shows other output than the example prints");
}

fn users_output() -> &'static str {
    "admin: superuser\nacme/ingest: insert on prefix:acme/\nread acme/pump1: server error -16: permission denied\n\
     globex/pump1: server error -16: permission denied\n"
}

#[test]
fn users_example_runs() {
    let out = run_logged_in("users");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "users failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(stdout, users_output());
}
