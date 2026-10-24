//! The Rust calendar bounds against the C client's: `fixtures/calendar_bounds.txt`
//! holds what `tsdb_calendar_bounds` gives for about 45 zones (DST at
//! midnight, half- and quarter-hour offsets, skipped days, negative DST,
//! POSIX rules beyond the last transition), every unit, several counts and
//! ranges from 1800 to 2100 and beyond. Both read the same zone files, copied
//! into `fixtures/zoneinfo`; `fixtures/gen_calendar_fixtures.sh` regenerates
//! the lot.
//!
//! The only test in this binary: it points `$TZDIR` at the fixture files
//! before any zone is loaded.

use std::path::PathBuf;

use tsdb_client::{Unit, calendar_bounds};

fn unit(code: &str) -> Unit {
    match code {
        "1" => Unit::Second,
        "2" => Unit::Minute,
        "3" => Unit::Hour,
        "4" => Unit::Day,
        "5" => Unit::Week,
        "6" => Unit::Month,
        "7" => Unit::Quarter,
        "8" => Unit::Year,
        u => panic!("unit {u}"),
    }
}

fn fnv1a32(bounds: &[i64]) -> u32 {
    let mut h = 0x811c_9dc5u32;
    for b in bounds {
        for byte in b.to_le_bytes() {
            h ^= u32::from(byte);
            h = h.wrapping_mul(0x0100_0193);
        }
    }
    h
}

#[test]
fn bounds_match_the_c_client() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    // SAFETY: the only test of this binary, before anything reads the environment
    unsafe { std::env::set_var("TZDIR", dir.join("zoneinfo")) };
    let text = std::fs::read_to_string(dir.join("calendar_bounds.txt")).unwrap();
    let (mut cases, mut errors, mut periods, mut failures) = (0, 0, 0, Vec::new());
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let f: Vec<&str> = line.split(' ').collect();
        let zone = if f[0] == "-" { "" } else { f[0] };
        let (count, start, end): (u32, i64, i64) =
            (f[2].parse().unwrap(), f[3].parse().unwrap(), f[4].parse().unwrap());
        let got = calendar_bounds(zone, unit(f[1]), count, start..end);
        cases += 1;
        if f[5] == "ERR" {
            errors += 1;
            if let Ok(b) = got {
                failures.push(format!("{line}: got {} bounds", b.len()));
            }
            continue;
        }
        let b = match got {
            Ok(b) => b,
            Err(e) => {
                failures.push(format!("{line}: {e}"));
                continue;
            }
        };
        periods += b.len();
        let n: usize = f[5].parse().unwrap();
        let hash = u32::from_str_radix(f[6], 16).unwrap();
        let listed: Vec<i64> = f[7..].iter().map(|s| s.parse().unwrap()).collect();
        if b.len() != n || fnv1a32(&b) != hash || (!listed.is_empty() && listed != b) {
            let head: Vec<i64> = b.iter().copied().take(4).collect();
            failures.push(format!("{line}: got {} bounds, hash {:08x}, from {head:?}", b.len(), fnv1a32(&b)));
        }
    }
    assert!(cases > 10_000 && errors > 100 && periods > 10_000_000, "{cases} cases, {errors} errors, {periods}");
    assert!(
        failures.is_empty(),
        "{} of {cases} cases differ from the C client:\n{}",
        failures.len(),
        failures[..failures.len().min(30)].join("\n")
    );
}
