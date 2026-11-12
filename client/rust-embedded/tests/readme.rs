// SPDX-License-Identifier: AGPL-3.0-only
//! The README's embedded section is `examples/embedded_quickstart.rs`, and the example runs.

mod common;

use std::process::Command;

#[test]
fn readme_embedded_section_is_the_example() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{dir}/README.md")).unwrap();
    let example = std::fs::read_to_string(format!("{dir}/examples/embedded_quickstart.rs")).unwrap();
    // the README shows the code, not the SPDX line that heads the file
    let example = example.strip_prefix("// SPDX-License-Identifier: AGPL-3.0-only\n").unwrap_or(&example);
    let start = readme.find("## Embedded").expect("an Embedded section");
    let code = readme[start..].split("```rust\n").nth(1).and_then(|s| s.split("\n```").next()).unwrap();
    assert_eq!(code.trim_end(), example.trim_end(), "README.md Embedded differs from examples/embedded_quickstart.rs");
}

#[test]
fn embedded_example_runs() {
    let dir = common::test_dir("tsdb-readme-embedded");
    let out = Command::new(env!("CARGO"))
        .args(["run", "--quiet", "--example", "embedded_quickstart", "--"])
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
