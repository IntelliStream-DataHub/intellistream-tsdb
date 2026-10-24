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
    // Oslo days begin at 23:00 UTC in November
    assert!(
        stdout.ends_with("day  1699916400000 count 60 sum 1770\nday  1700002800000 count 1380 sum 40710\n"),
        "{stdout}"
    );
}

#[test]
fn readme_embedded_section_is_the_example() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{dir}/README.md")).unwrap();
    let example = std::fs::read_to_string(format!("{dir}/examples/embedded_quickstart.rs")).unwrap();
    let start = readme.find("## Embedded").expect("an Embedded section");
    let code = readme[start..].split("```rust\n").nth(1).and_then(|s| s.split("\n```").next()).unwrap();
    assert_eq!(code.trim_end(), example.trim_end(), "README.md Embedded differs from examples/embedded_quickstart.rs");
}

#[cfg(feature = "embedded")]
#[test]
fn embedded_example_runs() {
    let dir = common::test_dir("tsdb-readme-embedded");
    let out = Command::new(env!("CARGO"))
        .args(["run", "--quiet", "--features", "embedded", "--example", "embedded_quickstart", "--"])
        .arg(&dir)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "embedded_quickstart failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    // Oslo days begin at 23:00 UTC in November
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines,
        [
            "raw  1700000000000 0",
            "raw  1700000060000 1",
            "raw  1700000120000 2",
            "day  1699916400000 47 1081",
            "day  1700002800000 1393 41399"
        ]
    );
    std::fs::remove_dir_all(dir).ok();
}
