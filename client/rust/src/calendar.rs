// SPDX-License-Identifier: Apache-2.0
//! Calendar periods in a time zone, as UTC bucket bounds: a port of
//! `client/tsdb_calendar.c`, which defines them.
//!
//! The server knows UTC only (protocol version 4). For "per day in
//! Europe/Oslo" the client computes the UTC instant each local day begins (23
//! or 25 hours apart around DST changes) and the server groups rows by those
//! bounds ([`Client::query_bounds`](crate::Client::query_bounds)).
//!
//! The arithmetic follows the C file step by step, so both clients give the
//! same bounds; `tests/calendar_fixtures.rs` checks that against output of the
//! C function. One deliberate difference: a compact offset such as "+0530" is
//! 5:30 here, where the C reader takes it for 530 hours. Zones are UTC, fixed offsets and IANA names read from the
//! system's zoneinfo (TZif versions 1 to 4, `$TZDIR` or `/usr/share/zoneinfo`).
//! Instants after the last transition in a file follow the POSIX rule in its
//! footer, so DST keeps working in future years with "slim" files too. Loaded
//! zones are cached for the life of the process.
//!
//! Ported rather than built on a time zone crate: the C file is the
//! definition, and beyond what such a crate offers it fixes the standard
//! offset seconds to hours are aligned in (the TZif footer's), the instant a
//! skipped local midnight maps to (the transition), fixed offsets such as
//! "+0530", and ranges of 200,000 years either way. A crate would need most
//! of this file around it, and a dependency besides.

use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{Error, Result, Status};
use crate::types::Unit;

/// Most periods one call returns (`MAX_BOUNDS` in the C file).
pub(crate) const MAX_BOUNDS: usize = 64 << 20;

/// Beyond a few hundred thousand years the arithmetic would overflow.
const LIMIT: i64 = 1000 * 86400 * 366 * 200_000;

/// A POSIX TZ rule `Mm.w.d[/time]`.
#[derive(Debug, Clone, Copy, Default)]
struct Rule {
    month: i64,
    week: i64,
    wday: i64,
    /// Local time of day of the change, seconds.
    secs: i64,
}

/// A loaded zone (`struct tz`).
#[derive(Debug, Default)]
pub(crate) struct Zone {
    /// UTC seconds of each transition.
    trans: Vec<i64>,
    /// Offset (seconds east) from that transition on.
    after: Vec<i32>,
    /// Offset before the first transition.
    before: i32,
    has_rule: bool,
    rule_dst: bool,
    std_off: i32,
    dst_off: i32,
    /// DST start (in standard time) and end (in DST).
    start: Rule,
    end: Rule,
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::InvalidArgument(msg.into())
}

fn floor_div(a: i64, b: i64) -> i64 {
    a.div_euclid(b) // b > 0
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// (year, month, day) of a day number.
fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

// ------------------------------------------------------------ POSIX rule

/// A zone abbreviation, `<...>` or at least three letters; the rest of `s`.
fn parse_name(s: &[u8]) -> Option<&[u8]> {
    if s.first() == Some(&b'<') {
        let close = s.iter().position(|&c| c == b'>')?;
        return Some(&s[close + 1..]);
    }
    let n = s.iter().take_while(|c| c.is_ascii_alphabetic()).count();
    (n >= 3).then(|| &s[n..])
}

fn digits(s: &[u8]) -> (i64, &[u8]) {
    let n = s.iter().take_while(|c| c.is_ascii_digit()).count();
    let v = s[..n].iter().fold(0i64, |v, &c| v.saturating_mul(10).saturating_add(i64::from(c - b'0')));
    (v, &s[n..])
}

/// `[+-]hh[:mm[:ss]]` as seconds; the rest of `s`.
fn parse_hms(s: &[u8]) -> Option<(i64, &[u8])> {
    let (sign, s) = match s.first() {
        Some(b'-') => (-1, &s[1..]),
        Some(b'+') => (1, &s[1..]),
        _ => (1, s),
    };
    if !s.first().is_some_and(u8::is_ascii_digit) {
        return None;
    }
    let (h, mut s) = digits(s);
    let (mut m, mut sec) = (0, 0);
    if s.first() == Some(&b':') {
        (m, s) = digits(&s[1..]);
        if s.first() == Some(&b':') {
            (sec, s) = digits(&s[1..]);
        }
    }
    Some((sign * (h.saturating_mul(3600).saturating_add(m * 60).saturating_add(sec)), s))
}

/// An optionally signed decimal, as `%d` in sscanf reads it.
fn parse_int(s: &[u8]) -> Option<(i64, &[u8])> {
    let (sign, rest) = match s.first() {
        Some(b'-') => (-1, &s[1..]),
        Some(b'+') => (1, &s[1..]),
        _ => (1, s),
    };
    if !rest.first().is_some_and(u8::is_ascii_digit) {
        return None;
    }
    let (v, rest) = digits(rest);
    Some((sign * v, rest))
}

/// `Mm.w.d[/time]`; the Jn and n forms are not used by current zones.
fn parse_rule(s: &[u8]) -> Option<(Rule, &[u8])> {
    let s = s.strip_prefix(b"M")?;
    let (month, s) = parse_int(s)?;
    let (week, s) = parse_int(s.strip_prefix(b".")?)?;
    let (wday, s) = parse_int(s.strip_prefix(b".")?)?;
    let skip = s.iter().take_while(|&&c| c != b'/' && c != b',').count();
    let mut s = &s[skip..];
    let mut secs = 7200;
    if s.first() == Some(&b'/') {
        (secs, s) = parse_hms(&s[1..])?;
    }
    Some((Rule { month, week, wday, secs }, s))
}

/// Offsets that fit no `i32` are of no real zone; they make the footer unusable.
fn off32(v: i64) -> Option<i32> {
    i32::try_from(v).ok()
}

fn parse_footer(z: &mut Zone, s: &[u8]) {
    let Some(p) = parse_name(s) else { return };
    let Some((off, p)) = parse_hms(p) else { return };
    let Some(std_off) = off32(-off) else { return }; // POSIX offsets are west of UTC
    z.std_off = std_off;
    z.has_rule = true;
    if p.is_empty() {
        return; // no DST
    }
    let Some(mut p) = parse_name(p) else { return };
    z.dst_off = z.std_off.saturating_add(3600);
    if !p.is_empty() && p[0] != b',' {
        let Some((off, rest)) = parse_hms(p) else { return };
        let Some(dst_off) = off32(-off) else {
            z.has_rule = false;
            return;
        };
        z.dst_off = dst_off;
        p = rest;
    }
    let rules = p
        .strip_prefix(b",")
        .and_then(parse_rule)
        .and_then(|(start, p)| Some((start, parse_rule(p.strip_prefix(b",")?)?.0)));
    match rules {
        Some((start, end)) => {
            z.start = start;
            z.end = end;
            z.rule_dst = true;
        }
        None => z.has_rule = false,
    }
}

/// UTC second at which rule `r` fires in year `y`, given the offset in force before it.
fn rule_utc(r: &Rule, y: i64, off_before: i32) -> i64 {
    let first = days_from_civil(y, r.month, 1);
    let wd_first = (first % 7 + 11) % 7; // 1970-01-01 was a Thursday (4)
    let mut day = first + (r.wday - wd_first + 7) % 7 + (r.week - 1) * 7;
    let month_end = if r.month == 12 { days_from_civil(y + 1, 1, 1) } else { days_from_civil(y, r.month + 1, 1) };
    if day >= month_end {
        day -= (day - month_end) / 7 * 7 + 7; // week 5 means the last one
    }
    day * 86400 + r.secs - i64::from(off_before)
}

impl Zone {
    fn rule_offset(&self, t: i64) -> i32 {
        if !self.rule_dst {
            return self.std_off;
        }
        let (y, _, _) = civil(floor_div(t, 86400));
        let on = rule_utc(&self.start, y, self.std_off);
        let off = rule_utc(&self.end, y, self.dst_off);
        let dst = if on < off { t >= on && t < off } else { !(t >= off && t < on) };
        if dst { self.dst_off } else { self.std_off }
    }

    /// Offset in seconds east of UTC at UTC second `t`.
    fn offset(&self, t: i64) -> i32 {
        let n = self.trans.len();
        if n == 0 || t < self.trans[0] {
            return if n == 0 && self.has_rule { self.rule_offset(t) } else { self.before };
        }
        if t >= self.trans[n - 1] && self.has_rule {
            return self.rule_offset(t);
        }
        // the last transition <= t
        let i = self.trans.partition_point(|&x| x <= t) - 1;
        self.after[i]
    }

    /// The UTC second at which local time `local` (seconds) begins. Repeated
    /// local times (DST ends) give the first; skipped ones (DST starts) give
    /// the moment the clock jumps past them.
    fn utc(&self, local: i64) -> i64 {
        let a = self.offset(local - 2 * 86400);
        let b = self.offset(local + 2 * 86400);
        let (lo_off, hi_off) = (i64::from(a.min(b)), i64::from(a.max(b)));
        // the earliest consistent instant: a larger offset means an earlier instant
        let u = local - hi_off;
        if i64::from(self.offset(u)) == hi_off {
            return u;
        }
        let u = local - lo_off;
        if i64::from(self.offset(u)) == lo_off {
            return u;
        }
        // skipped: the transition lies in (local - hi_off, local - lo_off]
        let (mut lo, mut hi) = (local - hi_off, local - lo_off);
        let before = self.offset(lo);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if self.offset(mid) == before {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        hi
    }

    /// The offset fixed-length units (seconds to hours) are aligned in: standard time.
    fn fixed_offset(&self) -> i32 {
        if self.has_rule { self.std_off } else { self.before }
    }
}

// ----------------------------------------------------------------- TZif

fn be(b: &[u8], n: usize) -> i64 {
    let v = b[..n].iter().fold(0u64, |v, &x| v << 8 | u64::from(x));
    if n == 4 { i64::from(v as u32 as i32) } else { v as i64 }
}

fn corrupt(name: &str) -> Error {
    invalid(format!("time zone file of {name:?} is not valid TZif"))
}

fn load_tzif(name: &str, b: &[u8]) -> Result<Zone> {
    if b.len() < 44 || &b[..4] != b"TZif" {
        return Err(corrupt(name));
    }
    let version = if b[4] != 0 { i64::from(b[4]) - i64::from(b'0') } else { 1 };
    let mut h = 0usize;
    let mut tsize = 4usize;
    for pass in 0..2 {
        if h + 44 > b.len() {
            return Err(corrupt(name));
        }
        let field = |i: usize| be(&b[h + i..], 4);
        let (isutc, isstd, leap) = (field(20), field(24), field(28));
        let (timecnt, typecnt, charcnt) = (field(32), field(36), field(40));
        if [isutc, isstd, leap, timecnt, typecnt, charcnt].iter().any(|&c| c < 0) || typecnt == 0 {
            return Err(corrupt(name));
        }
        let ts = tsize as i64;
        let body = (timecnt * ts + timecnt + typecnt * 6 + charcnt + leap * (ts + 4) + isstd + isutc) as usize;
        if h + 44 + body > b.len() {
            return Err(corrupt(name));
        }
        if version >= 2 && pass == 0 {
            // skip the 32-bit data: the 64-bit copy follows
            h += 44 + body;
            tsize = 8;
            continue;
        }
        let (timecnt, typecnt) = (timecnt as usize, typecnt as usize);
        let tt = h + 44;
        let idx = tt + timecnt * tsize;
        let types = idx + timecnt;
        let mut z = Zone { trans: Vec::with_capacity(timecnt), after: Vec::with_capacity(timecnt), ..Zone::default() };
        for i in 0..timecnt {
            z.trans.push(be(&b[tt + i * tsize..], tsize));
            let k = b[idx + i] as usize;
            if k >= typecnt {
                return Err(corrupt(name));
            }
            z.after.push(be(&b[types + k * 6..], 4) as i32);
        }
        // before the first transition: the first standard type, or type 0
        z.before = be(&b[types..], 4) as i32;
        if let Some(k) = (0..typecnt).find(|k| b[types + k * 6 + 4] == 0) {
            z.before = be(&b[types + k * 6..], 4) as i32;
        }
        if version >= 2 {
            let foot = &b[h + 44 + body..];
            if foot.first() == Some(&b'\n')
                && let Some(e) = foot[1..].iter().position(|&c| c == b'\n')
            {
                parse_footer(&mut z, &foot[1..1 + e.min(127)]);
            }
        }
        return Ok(z);
    }
    unreachable!("the second pass returns")
}

/// `+hh:mm`-style and `+hhmm` offsets.
fn fixed_offset(name: &str) -> Option<i64> {
    let b = name.as_bytes();
    if !matches!(b.first(), Some(b'+' | b'-')) {
        return None;
    }
    // +hhmm first: read as hours, "+0530" would be 530 of them
    if b.len() == 5 && b[1..].iter().all(u8::is_ascii_digit) {
        let compact = format!("{}:{}", &name[..3], &name[3..]);
        return parse_hms(compact.as_bytes()).filter(|(_, rest)| rest.is_empty()).map(|(off, _)| off);
    }
    parse_hms(b).filter(|(_, rest)| rest.is_empty()).map(|(off, _)| off)
}

fn load_zone(name: &str) -> Result<Zone> {
    if matches!(name, "UTC" | "Z" | "GMT" | "Etc/UTC") {
        return Ok(Zone::default());
    }
    if let Some(off) = fixed_offset(name) {
        let before = off32(off).ok_or_else(|| invalid(format!("time zone offset {name:?} is too large")))?;
        return Ok(Zone { before, ..Zone::default() });
    }
    if name.contains("..") || name.starts_with('/') || name.contains('\0') {
        return Err(invalid(format!("time zone {name:?} is not a zone name")));
    }
    let dir = std::env::var_os("TZDIR").unwrap_or_else(|| "/usr/share/zoneinfo".into());
    let path = std::path::Path::new(&dir).join(name);
    let unknown = || invalid(format!("unknown time zone {name:?} (no file {})", path.display()));
    let meta = std::fs::metadata(&path).map_err(|_| unknown())?;
    if !meta.is_file() || meta.len() == 0 || meta.len() >= 1 << 20 {
        return Err(unknown());
    }
    let bytes = std::fs::read(&path).map_err(|_| unknown())?;
    load_tzif(name, &bytes)
}

/// The zone called `name` (empty: UTC), loaded once per process.
pub(crate) fn zone(name: &str) -> Result<Arc<Zone>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Zone>>>> = OnceLock::new();
    let name = if name.is_empty() { "UTC" } else { name };
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    if let Some(z) = cache.get(name) {
        return Ok(z.clone());
    }
    let z = Arc::new(load_zone(name)?);
    cache.insert(name.to_owned(), z.clone());
    Ok(z)
}

// ------------------------------------------------------------------ bounds

fn unit_ms(unit: Unit) -> i64 {
    match unit {
        Unit::Second => 1000,
        Unit::Minute => 60_000,
        _ => 3_600_000,
    }
}

/// The index of the local calendar unit holding `ts` (days, ISO weeks,
/// months, quarters, years since 1970).
fn cal_index(z: &Zone, unit: Unit, ts: i64) -> i64 {
    let s = floor_div(ts, 1000);
    let day = floor_div(s + i64::from(z.offset(s)), 86400);
    match unit {
        Unit::Day => day,
        Unit::Week => floor_div(day + 3, 7), // 1970-01-01 was a Thursday
        _ => {
            let (y, m, _) = civil(day);
            let month = (y - 1970) * 12 + (m - 1);
            match unit {
                Unit::Month => month,
                Unit::Quarter => floor_div(month, 3),
                _ => y - 1970,
            }
        }
    }
}

/// Local seconds at which calendar unit `idx` begins; `None` where that is
/// beyond what the arithmetic can hold.
fn cal_local_start(unit: Unit, idx: i64) -> Option<i64> {
    let days = match unit {
        Unit::Day => idx,
        Unit::Week => idx.checked_mul(7)?.checked_sub(3)?,
        Unit::Month | Unit::Quarter => {
            let month = if unit == Unit::Month { idx } else { idx.checked_mul(3)? };
            let y = floor_div(month, 12);
            days_from_civil(1970i64.checked_add(y)?, month - y * 12 + 1, 1)
        }
        _ => days_from_civil(1970i64.checked_add(idx)?, 1, 1),
    };
    days.checked_mul(86400)
}

/// The UTC start (ms since the epoch) of every period of `count` `unit`s
/// that overlaps `range`, in time zone `zone`. These are the bounds
/// [`Client::query_group`](crate::Client::query_group) sends to the server
/// for a time zone, and what [`Client::query_bounds`](crate::Client::query_bounds) takes.
///
/// - `zone`: `""` or `"UTC"`, a fixed offset (`"+02:00"`, `"-0530"`), or an
///   IANA name such as `"Europe/Oslo"`, read from the system's zoneinfo
///   (`/usr/share/zoneinfo`, or the directory in `$TZDIR`).
/// - Days, weeks (ISO, from Monday), months, quarters and years are local
///   periods: a day across a DST change lasts 23 or 25 hours. Seconds,
///   minutes and hours have a fixed length and are aligned in the zone's
///   standard time.
/// - `count` units make one period (0 counts as 1). Periods are counted from
///   1970 in local time, as ClickHouse's `toStartOfInterval` does: with
///   `count` 2, days 0-1, 2-3, ... since 1970-01-01.
/// - A period runs from its first instant to the next period's first
///   instant, and the bounds strictly increase. A local midnight that occurs
///   twice (the clock is set back across it) begins its day the first time;
///   one that is skipped (the clock jumps over it) begins it when the clock
///   jumps.
///
/// The bounds are exactly those of `tsdb_calendar_bounds` in the C client.
///
/// # Errors
///
/// [`Error::InvalidArgument`] for an unknown zone or a range beyond about
/// 200,000 years either side of 1970 (an unbounded one included);
/// [`Status::TooManyBuckets`] (as [`Error::Server`], the C calendar's status) for
/// more than 64 Mi periods. An empty range gives no bounds.
///
/// # Examples
///
/// ```
/// use tsdb_client::{Unit, calendar_bounds};
///
/// // 2024-03-30 to 2024-04-01 UTC: three local days in Oslo, the middle one 23 hours long
/// let b = calendar_bounds("Europe/Oslo", Unit::Day, 1, 1_711_756_800_000..1_711_929_600_000)?;
/// # if std::path::Path::new("/usr/share/zoneinfo/Europe/Oslo").exists() {
/// assert_eq!(b, [1_711_753_200_000, 1_711_839_600_000, 1_711_922_400_000]);
/// # }
/// # Ok::<(), tsdb_client::Error>(())
/// ```
pub fn calendar_bounds(zone: &str, unit: Unit, count: u32, range: impl RangeBounds<i64>) -> Result<Vec<i64>> {
    let (start, end) = crate::client::bounds(&range);
    if start >= end {
        return Ok(Vec::new());
    }
    if start < -LIMIT || end > LIMIT {
        return Err(invalid(format!("range [{start}, {end}) is too far from 1970 for calendar bounds")));
    }
    let z = self::zone(zone)?;
    bounds_in(&z, unit, count, start, end)
}

/// [`calendar_bounds`] for a loaded zone and a range checked against [`LIMIT`].
fn bounds_in(z: &Zone, unit: Unit, count: u32, start: i64, end: i64) -> Result<Vec<i64>> {
    let c = i64::from(count.max(1));
    let fixed = matches!(unit, Unit::Second | Unit::Minute | Unit::Hour);
    let (interval, shift) = (unit_ms(unit) * c, i64::from(z.fixed_offset()) * 1000);
    let (k0, k1) = if fixed {
        (floor_div(start + shift, interval), floor_div(end - 1 + shift, interval))
    } else {
        (floor_div(cal_index(z, unit, start), c), floor_div(cal_index(z, unit, end - 1), c))
    };
    if k1 - k0 + 1 > MAX_BOUNDS as i64 {
        // the engine's and the C calendar's status, so that every client fails an over-wide range alike
        return Err(Error::Server(Status::TooManyBuckets));
    }
    let mut b: Vec<i64> = Vec::with_capacity((k1 - k0 + 1) as usize);
    for k in k0..=k1 {
        let t = if fixed {
            k * interval - shift
        } else {
            k.checked_mul(c)
                .and_then(|i| cal_local_start(unit, i))
                .and_then(|local| z.utc(local).checked_mul(1000))
                .ok_or_else(|| invalid("calendar period too far from 1970"))?
        };
        if b.last().is_none_or(|&last| t > last) {
            // bounds strictly increase
            b.push(t);
        }
    }
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trips() {
        for d in [-1_000_000, -719_468, -1, 0, 1, 59, 365, 10_957, 2_932_896] {
            let (y, m, dd) = civil(d);
            assert_eq!(days_from_civil(y, m, dd), d);
        }
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(-1), (1969, 12, 31));
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
    }

    #[test]
    fn fixed_offsets() {
        assert_eq!(fixed_offset("+02:00"), Some(7200));
        assert_eq!(fixed_offset("-05:30"), Some(-19_800));
        assert_eq!(fixed_offset("+5"), Some(18_000));
        // compact hhmm: 5 h 30, where the C reader reads 530 hours
        assert_eq!(fixed_offset("+0530"), Some(19_800));
        assert_eq!(fixed_offset("-0530"), Some(-19_800));
        assert_eq!(fixed_offset("Europe/Oslo"), None);
        assert_eq!(fixed_offset("+"), None);
        assert_eq!(fixed_offset("+02:00x"), None);
        let b = calendar_bounds("-0530", Unit::Day, 1, 0..86_400_000).unwrap();
        assert_eq!(b, [-66_600_000, 19_800_000]);
    }

    #[test]
    fn footers() {
        let mut z = Zone::default();
        parse_footer(&mut z, b"CET-1CEST,M3.5.0,M10.5.0/3");
        assert!(z.has_rule && z.rule_dst);
        assert_eq!((z.std_off, z.dst_off), (3600, 7200));
        assert_eq!((z.start.month, z.start.week, z.start.wday, z.start.secs), (3, 5, 0, 7200));
        assert_eq!(z.end.secs, 10_800);
        let mut z = Zone::default();
        parse_footer(&mut z, b"<-02>2<-01>,M3.5.0/-1,M10.5.0/0"); // America/Nuuk
        assert_eq!((z.std_off, z.dst_off, z.start.secs, z.end.secs), (-7200, -3600, -3600, 0));
        let mut z = Zone::default();
        parse_footer(&mut z, b"<+0545>-5:45");
        assert!(z.has_rule && !z.rule_dst);
        assert_eq!(z.std_off, 20_700);
        let mut z = Zone::default();
        parse_footer(&mut z, b"EST5EDT,J60,J300"); // a form the reader does not know: no rule
        assert!(!z.has_rule);
    }

    #[test]
    fn rules_fire_on_the_right_day() {
        let last_sunday_march = Rule { month: 3, week: 5, wday: 0, secs: 7200 };
        // 2024-03-31 01:00 UTC is 02:00 CET
        assert_eq!(rule_utc(&last_sunday_march, 2024, 3600), 1_711_846_800);
        let second_sunday_march = Rule { month: 3, week: 2, wday: 0, secs: 7200 };
        // 2024-03-10 07:00 UTC is 02:00 EST
        assert_eq!(rule_utc(&second_sunday_march, 2024, -18_000), 1_710_054_000);
    }

    #[test]
    fn utc_and_errors() {
        assert_eq!(calendar_bounds("", Unit::Day, 0, 1..86_400_001).unwrap(), [0, 86_400_000]);
        assert_eq!(calendar_bounds("UTC", Unit::Hour, 2, 3_600_000..3_600_001).unwrap(), [0]);
        assert!(calendar_bounds("UTC", Unit::Day, 1, 5..5).unwrap().is_empty());
        assert!(calendar_bounds("UTC", Unit::Day, 1, ..).is_err());
        assert!(calendar_bounds("UTC", Unit::Second, 1, 0..86_400_000 * 1000).is_err());
        assert!(calendar_bounds("Mars/Olympus_Mons", Unit::Day, 1, 0..1).is_err());
        assert!(calendar_bounds("../etc/passwd", Unit::Day, 1, 0..1).is_err());
        assert!(calendar_bounds("Europe\0Oslo", Unit::Day, 1, 0..1).is_err());
        assert!(load_tzif("x", b"TZif2 not really").is_err());
    }
}
