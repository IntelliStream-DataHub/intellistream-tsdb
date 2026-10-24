//! The README's quick start is `examples/quickstart.rs`, and it runs against a live server.

mod common;

use std::process::Command;

use common::Server;

#[test]
fn readme_quick_start_is_the_example() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{dir}/README.md")).unwrap();
    let example = std::fs::read_to_string(format!("{dir}/examples/quickstart.rs")).unwrap();
    let start = readme.find("## Quick start").expect("a Quick start section");
    let code = readme[start..].split("```rust\n").nth(1).and_then(|s| s.split("\n```").next()).unwrap();
    assert_eq!(code.trim_end(), example.trim_end(), "README.md quick start differs from examples/quickstart.rs");
}

#[test]
fn quick_start_runs() {
    let srv = Server::start();
    let out = Command::new(env!("CARGO"))
        .args(["run", "--quiet", "--example", "quickstart", "--", &srv.addr()])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "quickstart failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    let count = |p: &str| stdout.lines().filter(|l| l.starts_with(p)).count();
    assert_eq!((count("raw "), count("hour "), count("day ")), (3, 6, 2), "{stdout}");
    assert!(stdout.contains("raw  1700085540000 59"), "{stdout}");
    assert!(stdout.contains("rows [Some(3), None, Some(5)]\nint  count 2 isum 8\nmode off\n"), "{stdout}");
}
