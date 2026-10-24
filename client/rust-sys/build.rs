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
    let root = env::var_os("TSDB_SRC_DIR").map(PathBuf::from).unwrap_or_else(|| manifest.join("../.."));
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
    let mut files: Vec<PathBuf> = std::fs::read_dir(&src)
        .expect("read src/")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "c"))
        .collect();
    files.sort();
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    for h in ["internal.h", "levels.h", "bits.h", "bitpack.h"] {
        println!("cargo:rerun-if-changed={}", src.join(h).display());
    }
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
    cc::Build::new().objects(objects).out_dir(&out).compile("tsdb");
    println!("cargo:rustc-link-lib=m");
    if zstd_include.is_empty() {
        println!("cargo:rustc-link-lib=zstd");
    }
}
