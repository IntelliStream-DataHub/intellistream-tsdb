// SPDX-License-Identifier: AGPL-3.0-only
//! Builds the tsdb engine into the crate, or links a prebuilt libtsdb.so (feature `system`).
//!
//! The static build mirrors the Makefile: every `src/*.c` plus `client/tsdb_calendar.c` (the
//! client-side calendar that `tsdb_query_group_local` and `tsdb_calendar_bounds` need), with the
//! same language and floating-point flags, because the bytes a codec writes must not depend on
//! who compiled it. Portable by default; on x86-64 the files of `src/levels.h` are compiled for
//! the baseline, x86-64-v3 and x86-64-v4 and `src/cpu.c` picks one set at startup, as in
//! `make dist`. Feature `native` compiles everything with `-march=native` instead.

use std::env;
use std::path::{Path, PathBuf};

/// The files compiled once per CPU level (Makefile `LV_SRC`).
const LEVEL_SRC: [&str; 5] = ["codec", "codec_alp", "codec_typed", "codec_xor", "values"];

fn main() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    println!("cargo:rerun-if-env-changed=TSDB_SRC_DIR");
    println!("cargo:rerun-if-env-changed=TSDB_LIB_DIR");
    // the repository around the crate, else the copy vendor.sh made for a package (the Python sdist)
    let root = env::var_os("TSDB_SRC_DIR").map(PathBuf::from).unwrap_or_else(|| {
        let repo = manifest.join("../..");
        if repo.join("include/tsdb.h").exists() { repo } else { manifest.join("csrc") }
    });
    let include = root.join("include");
    assert!(include.join("tsdb.h").exists(), "{} has no include/tsdb.h: set TSDB_SRC_DIR", root.display());
    println!("cargo:include={}", include.display());

    // layout.c only needs the header: it is built in both modes, for tests/layout.rs
    println!("cargo:rerun-if-changed=layout.c");
    println!("cargo:rerun-if-changed={}", include.display());
    cc::Build::new().file("layout.c").include(&include).compile("tsdb_sys_layout");

    if env::var_os("CARGO_FEATURE_SYSTEM").is_some() {
        let dir = env::var_os("TSDB_LIB_DIR").map(PathBuf::from).unwrap_or_else(|| root.join("build"));
        println!("cargo:rustc-link-search=native={}", dir.display());
        println!("cargo:rustc-link-lib=dylib=tsdb");
        return;
    }

    let src = root.join("src");
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-changed={}", root.join("client/tsdb_calendar.c").display());
    let entries: Vec<PathBuf> = std::fs::read_dir(&src).expect("read src/").map(|e| e.unwrap().path()).collect();
    // headers and the kernel bodies simd.c includes (*.inc) are inputs too
    for f in entries.iter().filter(|p| p.extension().is_some_and(|x| x == "c" || x == "h" || x == "inc")) {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    let mut files: Vec<PathBuf> = entries.into_iter().filter(|p| p.extension().is_some_and(|x| x == "c")).collect();
    files.sort();
    files.push(root.join("client/tsdb_calendar.c"));

    let native = env::var_os("CARGO_FEATURE_NATIVE").is_some();
    let x86_64 = env::var("CARGO_CFG_TARGET_ARCH").unwrap() == "x86_64";
    let levels = x86_64 && !native;
    // zstd-sys (feature bundled-zstd) names its headers, ';'-separated; else the system's zstd.h
    let zstd_include: Vec<PathBuf> =
        env::var("DEP_ZSTD_INCLUDE").map(|s| s.split(';').map(PathBuf::from).collect()).unwrap_or_default();
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());

    let base = |dir: &str, extra: &[&str]| {
        let mut b = cc::Build::new();
        b.include(&include).include(&src).out_dir(out.join(dir)).opt_level(3).warnings(false);
        for f in ["-std=gnu11", "-fno-math-errno", "-ffp-contract=off", "-fPIC"] {
            b.flag(f);
        }
        if native {
            b.flag("-march=native");
        }
        if levels {
            b.define("TSDB_LEVELS", None).define("TSDB_HAVE_v3", None).define("TSDB_HAVE_v4", None);
        }
        b.includes(&zstd_include);
        for f in extra {
            b.flag(f);
        }
        b
    };
    let is_level = |p: &Path| LEVEL_SRC.iter().any(|n| p.file_stem().is_some_and(|s| s == *n));

    let mut objects =
        base("common", &[]).files(files.iter().filter(|p| !levels || !is_level(p))).compile_intermediates();
    if levels {
        let lv: Vec<&PathBuf> = files.iter().filter(|p| is_level(p)).collect();
        for (name, flags) in [
            ("base", &[][..]),
            ("v3", &["-march=x86-64-v3"][..]),
            ("v4", &["-march=x86-64-v4", "-mprefer-vector-width=256"][..]),
        ] {
            let mut b = base(name, flags);
            b.define("TSDB_LV", name).files(lv.iter().copied());
            objects.extend(b.compile_intermediates());
        }
    }
    let objects = hide_internal_symbols(&out, objects);
    cc::Build::new().objects(objects).out_dir(&out).compile("tsdb");
    println!("cargo:rustc-link-lib=m");
    if zstd_include.is_empty() {
        println!("cargo:rustc-link-lib=zstd");
    }
}

/// Links the engine's objects into one and keeps only the `tsdb_*` symbols global, as
/// tools/libtsdb.map does for libtsdb.so. Otherwise every internal name (pool_new, wal_open,
/// xxh64, ...) lands in the user's binary, where another library defining the same name breaks
/// the link or, through an archive, silently takes tsdb's calls. ELF targets (`cc -r` and
/// objcopy), and Apple targets (`ld -r` with an exported-symbols list); elsewhere the objects are archived as they are.
fn hide_internal_symbols(out: &Path, objects: Vec<PathBuf>) -> Vec<PathBuf> {
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let apple = matches!(os.as_str(), "macos" | "ios");
    if !apple && !matches!(os.as_str(), "linux" | "android" | "freebsd" | "netbsd" | "openbsd" | "dragonfly") {
        return objects;
    }
    let merged = out.join("tsdb_engine.o");
    let cc = cc::Build::new().get_compiler();
    if apple {
        // Mach-O: `ld -r` with an exported-symbols list turns every other global into a local symbol
        // (they are private externs, which -r makes local unless -keep_private_externs is given)
        // The list names every public symbol the objects define, written out: ld64 has no exclusion pattern, and it
        // refuses -exported_symbols_list together with -unexported_symbols_list. The tsdb_test_* hooks (they
        // reach into the engine and can corrupt a database) are left out. Untested on a Mac (docs/tasks/macos.md).
        let list = out.join("tsdb_exports.txt");
        let nm = std::process::Command::new("nm").args(["-gUj"]).args(&objects).output().expect("run nm");
        assert!(nm.status.success(), "nm failed to list the engine's symbols");
        let names: std::collections::BTreeSet<&str> = std::str::from_utf8(&nm.stdout)
            .expect("nm output")
            .lines()
            .filter(|n| n.starts_with("_tsdb_") && !n.starts_with("_tsdb_test_"))
            .collect();
        assert!(names.contains("_tsdb_open"), "nm listed no _tsdb_open: the exported symbols list would be empty");
        let text: String = names.iter().map(|n| format!("{n}\n")).collect();
        std::fs::write(&list, text).expect("write the exported symbols list");
        let mut cmd = cc.to_command();
        cmd.args(["-r", "-nostdlib", "-Wl,-exported_symbols_list"]).arg(format!("-Wl,{}", list.display()));
        let status = cmd.arg("-o").arg(&merged).args(&objects).status();
        assert!(status.is_ok_and(|s| s.success()), "{:?} -r failed to link the engine's objects", cc.path());
        return vec![merged];
    }
    let status = cc.to_command().args(["-r", "-nostdlib", "-o"]).arg(&merged).args(&objects).status();
    assert!(status.is_ok_and(|s| s.success()), "{:?} -r failed to link the engine's objects", cc.path());
    println!("cargo:rerun-if-env-changed=OBJCOPY");
    let tools = env::var("OBJCOPY").map(|o| vec![o]).unwrap_or_else(|_| vec!["objcopy".into(), "llvm-objcopy".into()]);
    for tool in &tools {
        // The test hooks match tsdb_* as well: they are localized by name (tools/libtsdb.map does the same for
        // libtsdb.so), whatever the order in which the tool applies the two options.
        let status = std::process::Command::new(tool)
            .args(["--wildcard", "--keep-global-symbol=tsdb_*", "--localize-symbol=tsdb_test_*"])
            .arg(&merged)
            .status();
        match status {
            Ok(s) if s.success() => {
                assert_no_test_hooks(&merged);
                return vec![merged];
            }
            Ok(s) => panic!("{tool} failed ({s}) to localize the engine's internal symbols"),
            Err(_) => continue,
        }
    }
    panic!("no objcopy found (tried {tools:?}; set OBJCOPY) to localize the engine's internal symbols");
}

/// Fails the build when the merged object still exports a `tsdb_test_*` hook (they can corrupt a database).
/// ELF only (the Apple list excludes them by pattern); skipped with a warning when no `nm` is installed.
fn assert_no_test_hooks(merged: &Path) {
    println!("cargo:rerun-if-env-changed=NM");
    let tools = env::var("NM").map(|o| vec![o]).unwrap_or_else(|_| vec!["nm".into(), "llvm-nm".into()]);
    for tool in &tools {
        let Ok(out) = std::process::Command::new(tool).args(["-g", "--defined-only"]).arg(merged).output() else {
            continue;
        };
        assert!(out.status.success(), "{tool} failed to list the symbols of the engine object");
        let text = String::from_utf8_lossy(&out.stdout);
        let hooks: Vec<&str> =
            text.lines().filter_map(|l| l.split_whitespace().last()).filter(|n| n.starts_with("tsdb_test_")).collect();
        assert!(hooks.is_empty(), "the engine object still exports test hooks: {hooks:?}");
        assert!(text.contains(" tsdb_open"), "the engine object exports no tsdb_open: the symbol check read nothing");
        return;
    }
    println!("cargo:warning=no nm found (set NM): the test hooks were not checked");
}
