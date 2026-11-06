//! The bodies of protocol 13 (`include/tsdb_wire.h`): why a request was refused (UNAVAILABLE), where a volume went
//! (MOVED), what a frame spanning volumes did (PARTIAL), the drop watermark (DROPPED) and TOPOLOGY. The decoders
//! never trust a count: an entry count the bytes cannot hold, or bytes that are left over, make the body
//! malformed, and [`ErrorInfo::has_body`] stays false (the status alone still tells what to do).

use crate::error::{Error, Result};
use crate::protocol::Cursor;

/// Most entries a body may hold (`TSDB_UNAVAIL_MAX`); a decoder refuses more.
const UNAVAIL_MAX: usize = 1024;
/// The longest address in a body (`TSDB_ADDR_MAX`).
const ADDR_MAX: usize = 255;
/// Entries of an UNAVAILABLE body (`TSDB_UNAVAIL_ENTRY`).
const UNAVAIL_ENTRY: usize = 28;
/// The `vol` of an entry that names no one volume (`TSDB_VOL_ANY`).
pub const VOL_ANY: u16 = 0xffff;
/// MOVED flag: a clean release is under way (`TSDB_MOVED_HANDOFF`).
pub const MOVED_HANDOFF: u8 = 1;
/// The time budget of one request's retries, by default (`TSDB_BUDGET_MS`).
pub const DEFAULT_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_millis(30_000);

/// One volume a request was refused for (an entry of UNAVAILABLE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unavailable {
    /// The volume, or [`VOL_ANY`].
    pub vol: u16,
    /// Why (`TSDB_WHY_*`).
    pub reason: u8,
    /// Entry flags.
    pub flags: u8,
    /// The volume's epoch.
    pub epoch: u32,
    /// When the outage began, ms since the epoch of the server's clock.
    pub since_ms: i64,
    /// How long to wait before asking again, ms.
    pub retry_after_ms: u32,
    /// The outage's id.
    pub outage_id: u64,
}

/// One volume another node owns (an entry of MOVED).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Moved {
    /// The volume.
    pub vol: u16,
    /// Its epoch.
    pub epoch: u32,
    /// Entry flags ([`MOVED_HANDOFF`]).
    pub flags: u8,
    /// The owner's node id.
    pub node_id: u32,
    /// The owner's address, empty when the body does not name one.
    pub addr: String,
}

/// A volume's own status in a PARTIAL reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialVol {
    /// The volume.
    pub vol: u16,
    /// Its status: 0, or `TSDB_ERR_UNAVAILABLE`, `_MOVED`, `_DROPPED`.
    pub status: i16,
}

/// The body of the latest UNAVAILABLE, MOVED, PARTIAL or DROPPED reply (see [`Client::last_error`](crate::Client::last_error)).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ErrorInfo {
    /// The status (`TSDB_ERR_*`) of the latest such reply, 0 before any.
    pub status: i32,
    /// False: the server sent no body (protocol 12) or it was malformed; only `status` is known.
    pub has_body: bool,
    /// UNAVAILABLE entries (a PARTIAL's refused volumes too).
    pub unavailable: Vec<Unavailable>,
    /// MOVED entries.
    pub moved: Vec<Moved>,
    /// The `resend_from` of an UNAVAILABLE or MOVED for a frame that carried a seq.
    pub resend_from: Option<u64>,
    /// PARTIAL: each volume's status.
    pub volumes: Vec<PartialVol>,
    /// PARTIAL: the rows of the frame.
    pub rows: u32,
    /// PARTIAL: bit i set means row i was not applied.
    pub not_applied: Vec<u8>,
    /// DROPPED: the volume.
    pub dropped_vol: u16,
    /// DROPPED: rows before this timestamp (ms) are never accepted.
    pub dropped_before: i64,
}

impl ErrorInfo {
    /// How long the server asks to wait: the largest `retry_after_ms` of the entries, 0 without any.
    pub fn retry_after_ms(&self) -> u32 {
        self.unavailable.iter().map(|e| e.retry_after_ms).max().unwrap_or(0)
    }

    /// PARTIAL: whether row `i` was not applied.
    pub fn row_not_applied(&self, i: usize) -> bool {
        self.not_applied.get(i / 8).is_some_and(|b| b >> (i % 8) & 1 != 0)
    }
}

/// Where the body of an error reply starts, after the op's own empty reply (`tsdb_wire_error_shape`): -1 for none.
pub(crate) fn error_shape(op: u8) -> usize {
    use crate::protocol::Op;
    match op {
        x if x == Op::QueryRaw as u8 => 5,
        x if x == Op::QueryValues as u8 || x == Op::QueryLttb as u8 || x == Op::QueryMulti as u8 => 8,
        _ => 0,
    }
}

fn bad() -> Error {
    Error::Protocol("malformed HA body".into())
}

fn unavail_entries(r: &mut Cursor<'_>) -> Result<Vec<Unavailable>> {
    let n = r.u16()? as usize;
    if n > UNAVAIL_MAX || r.remaining() < n * UNAVAIL_ENTRY {
        return Err(bad());
    }
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let vol = r.u16()?;
        let reason = r.u8()?;
        let flags = r.u8()?;
        let epoch = r.u32()?;
        let since_ms = r.i64()?;
        let retry_after_ms = r.u32()?;
        let outage_id = r.u64()?;
        v.push(Unavailable { vol, reason, flags, epoch, since_ms, retry_after_ms, outage_id });
    }
    Ok(v)
}

fn resend(r: &mut Cursor<'_>) -> Result<Option<u64>> {
    match r.remaining() {
        0 => Ok(None),
        8 => Ok(Some(r.u64()?)),
        _ => Err(bad()),
    }
}

fn addr(r: &mut Cursor<'_>, n: usize) -> Result<String> {
    if n > ADDR_MAX {
        return Err(bad());
    }
    Ok(String::from_utf8_lossy(r.take(n)?).into_owned())
}

fn decode(status: i32, body: &[u8]) -> Result<ErrorInfo> {
    let mut e = ErrorInfo { status, ..ErrorInfo::default() };
    let mut r = Cursor::new(body);
    match status {
        -11 => {
            e.unavailable = unavail_entries(&mut r)?;
            e.resend_from = resend(&mut r)?;
        }
        -10 => {
            let n = r.u16()? as usize;
            if n > UNAVAIL_MAX || r.remaining() < n * 13 {
                return Err(bad());
            }
            for _ in 0..n {
                let (vol, epoch, flags, node_id) = (r.u16()?, r.u32()?, r.u8()?, r.u32()?);
                let al = r.u16()? as usize;
                e.moved.push(Moved { vol, epoch, flags, node_id, addr: addr(&mut r, al)? });
            }
            e.resend_from = resend(&mut r)?;
        }
        -12 => {
            let nv = r.u16()? as usize;
            if nv > UNAVAIL_MAX || r.remaining() < nv * 4 + 4 {
                return Err(bad());
            }
            for _ in 0..nv {
                let (vol, st) = (r.u16()?, r.u16()?);
                e.volumes.push(PartialVol { vol, status: st as i16 });
            }
            e.rows = r.u32()?;
            if e.rows > crate::protocol::MAX_FRAME as u32 {
                return Err(bad());
            }
            e.not_applied = r.take((e.rows as usize).div_ceil(8))?.to_vec();
            e.unavailable = unavail_entries(&mut r)?;
            if r.remaining() != 0 {
                return Err(bad());
            }
        }
        -13 => {
            e.dropped_vol = r.u16()?;
            e.dropped_before = r.i64()?;
            if r.remaining() != 0 {
                return Err(bad()); // exactly 10 bytes, as the C codec reads it
            }
        }
        _ => return Ok(e),
    }
    e.has_body = true;
    Ok(e)
}

/// The info of an error reply whose payload (after the status) is `payload`, for a request of op `op`.
pub(crate) fn error_info(status: i32, payload: &[u8], op: u8) -> ErrorInfo {
    let off = error_shape(op);
    let body = payload.get(off..).unwrap_or(&[]);
    decode(status, body).unwrap_or(ErrorInfo { status, ..ErrorInfo::default() })
}

/// A node of the cluster, as TOPOLOGY lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopoNode {
    /// The node's id.
    pub node_id: u32,
    /// Whether the cluster sees it alive.
    pub alive: bool,
    /// Its address; empty: the node this answer came from.
    pub addr: String,
}

/// A volume, as TOPOLOGY lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopoVol {
    /// The volume's number.
    pub vol: u16,
    /// `TSDB_ROLE_*`.
    pub role: u8,
    /// `TSDB_HEALTH_*`.
    pub health: u8,
    /// The volume's epoch.
    pub epoch: u32,
    /// `TSDB_WHY_*` when not available.
    pub reason: u8,
    /// The storage domain, 0xffff when unknown.
    pub domain: u16,
    /// Since when the health has held, ms.
    pub since_ms: i64,
    /// The owner's node id.
    pub owner: u32,
    /// The server (a node's index).
    pub server: u16,
    /// The owner's address, empty: this node.
    pub addr: String,
    /// The volume's directory; empty unless the user is a superuser.
    pub path: String,
}

/// The reply of [`Client::topology`](crate::Client::topology).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    /// A generation number that changes when the answer does.
    pub generation: u64,
    /// The nodes.
    pub nodes: Vec<TopoNode>,
    /// The volumes.
    pub volumes: Vec<TopoVol>,
}

pub(crate) fn decode_topology(body: &[u8]) -> Result<Topology> {
    let mut r = Cursor::new(body);
    let generation = r.u64()?;
    let nn = r.u32()? as usize;
    if nn > 4096 || r.remaining() < nn * 7 {
        return Err(bad());
    }
    let mut nodes = Vec::with_capacity(nn);
    for _ in 0..nn {
        let node_id = r.u32()?;
        let alive = r.u8()? != 0;
        let al = r.u16()? as usize;
        nodes.push(TopoNode { node_id, alive, addr: addr(&mut r, al)? });
    }
    let nv = r.u32()? as usize;
    if nv > 65535 || r.remaining() < nv * 30 {
        return Err(bad());
    }
    let mut volumes = Vec::with_capacity(nv);
    for _ in 0..nv {
        let vol = r.u16()?;
        let (role, health) = (r.u8()?, r.u8()?);
        let epoch = r.u32()?;
        let reason = r.u8()?;
        r.u8()?;
        let domain = r.u16()?;
        let since_ms = r.i64()?;
        let owner = r.u32()?;
        let server = r.u16()?;
        let al = r.u16()? as usize;
        let a = addr(&mut r, al)?;
        let pl = r.u16()? as usize;
        if pl > 4096 {
            return Err(bad());
        }
        let path = String::from_utf8_lossy(r.take(pl)?).into_owned();
        volumes.push(TopoVol { vol, role, health, epoch, reason, domain, since_ms, owner, server, addr: a, path });
    }
    if r.remaining() != 0 {
        return Err(bad());
    }
    Ok(Topology { generation, nodes, volumes })
}

/// "host:port", "[v6]:port" or "host" (port 7070) into "host:port" for connecting; None if it makes no sense.
pub(crate) fn parse_node(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let port_ok = |p: &str| p.parse::<u32>().is_ok_and(|v| (1..=65535).contains(&v));
    if let Some(rest) = s.strip_prefix('[') {
        let (host, tail) = rest.split_once(']')?;
        if host.is_empty() {
            return None;
        }
        return match tail {
            "" => Some(format!("[{host}]:{}", crate::protocol::DEFAULT_PORT)),
            t => {
                let p = t.strip_prefix(':')?;
                port_ok(p).then(|| format!("[{host}]:{p}"))
            }
        };
    }
    match s.matches(':').count() {
        0 => Some(format!("{s}:{}", crate::protocol::DEFAULT_PORT)),
        1 => {
            let (h, p) = s.split_once(':')?;
            (!h.is_empty() && port_ok(p)).then(|| s.to_string())
        }
        // a bare IPv6 address has no port to read
        _ => Some(format!("[{s}]:{}", crate::protocol::DEFAULT_PORT)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodes() {
        assert_eq!(parse_node("a:1").as_deref(), Some("a:1"));
        assert_eq!(parse_node(" a ").as_deref(), Some("a:7070"));
        assert_eq!(parse_node("[::1]:9").as_deref(), Some("[::1]:9"));
        assert_eq!(parse_node("[::1]").as_deref(), Some("[::1]:7070"));
        assert_eq!(parse_node("::1").as_deref(), Some("[::1]:7070"));
        assert_eq!(parse_node("a:0"), None);
        assert_eq!(parse_node("a:x"), None);
        assert_eq!(parse_node(""), None);
        assert_eq!(parse_node("[::1"), None);
    }

    /// The bytes the C codec makes (tests/data/wire13-vectors.txt, `test_wire --emit`): the same values the other
    /// languages' tests expect.
    fn vector(name: &str) -> Vec<u8> {
        let text = include_str!("../../../tests/data/wire13-vectors.txt");
        let hex = text
            .lines()
            .find_map(|l| l.strip_prefix(name).and_then(|r| r.strip_prefix(' ')))
            .unwrap_or_else(|| panic!("no vector {name}"));
        (0..hex.len() / 2).map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn vectors() {
        let e = error_info(-11, &vector("unavailable"), 4);
        assert!(e.has_body && e.unavailable.len() == 2 && e.resend_from.is_none());
        assert_eq!(
            e.unavailable[0],
            Unavailable { vol: 3, reason: 1, flags: 0, epoch: 7, since_ms: 1_700_000_000_123, retry_after_ms: 300, outage_id: 0x1122334455667788 }
        );
        assert_eq!((e.unavailable[1].vol, e.unavailable[1].since_ms, e.unavailable[1].retry_after_ms), (VOL_ANY, -5, 60000));
        assert_eq!(e.retry_after_ms(), 60000);
        let e = error_info(-11, &vector("unavailable-resend"), 4);
        assert_eq!((e.unavailable.len(), e.resend_from), (1, Some(0xA1B2C3D4E5F60718)));
        let e = error_info(-10, &vector("moved"), 4);
        assert!(e.has_body);
        assert_eq!(e.moved[0], Moved { vol: 1, epoch: 9, flags: MOVED_HANDOFF, node_id: 4, addr: "db2.example:7070".into() });
        assert_eq!((e.moved[1].vol, e.moved[1].addr.as_str()), (2, ""));
        let e = error_info(-12, &vector("partial"), 4);
        assert!(e.has_body);
        assert_eq!(e.volumes, [PartialVol { vol: 1, status: 0 }, PartialVol { vol: 3, status: -11 }]);
        assert_eq!(e.rows, 10);
        assert_eq!((0..10).filter(|&i| e.row_not_applied(i)).collect::<Vec<_>>(), [2, 5, 8]);
        assert_eq!(e.unavailable.len(), 2);
        let e = error_info(-13, &vector("dropped"), 4);
        assert_eq!((e.has_body, e.dropped_vol, e.dropped_before), (true, 5, 1_700_000_000_999));
        let t = decode_topology(&vector("topology")).unwrap();
        assert_eq!((t.generation, t.nodes.len(), t.volumes.len()), (0xDEADBEEF, 2, 2));
        assert_eq!(t.nodes[1], TopoNode { node_id: 2, alive: false, addr: "10.0.0.2:7070".into() });
        assert_eq!((t.volumes[0].role, t.volumes[0].domain, t.volumes[0].path.as_str()), (1, 0xffff, "/srv/a"));
        assert_eq!((t.volumes[1].health, t.volumes[1].owner, t.volumes[1].addr.as_str()), (2, 2, "10.0.0.2:7070"));
        // every prefix of every body is malformed, never a panic
        for n in ["unavailable", "moved", "partial", "dropped"] {
            let v = vector(n);
            for st in [-10, -11, -12, -13] {
                for k in 0..v.len() {
                    let _ = error_info(st, &v[..k], 4);
                }
            }
        }
        let v = vector("topology");
        for k in 0..v.len() {
            assert!(decode_topology(&v[..k]).is_err(), "{k}");
        }
    }

    #[test]
    fn truncated_bodies_are_malformed() {
        // two UNAVAILABLE entries but bytes for one
        let mut b = vec![2, 0];
        b.extend_from_slice(&[0; UNAVAIL_ENTRY]);
        assert!(!error_info(-11, &b, 4).has_body);
        let mut ok = vec![1, 0];
        ok.extend_from_slice(&[0; UNAVAIL_ENTRY]);
        assert!(error_info(-11, &ok, 4).has_body);
        // trailing bytes that are not a resend_from
        ok.push(1);
        assert!(!error_info(-11, &ok, 4).has_body);
    }

    #[test]
    fn a_dropped_body_is_exactly_ten_bytes() {
        let d = vector("dropped");
        assert_eq!(d.len(), 10);
        assert!(error_info(-13, &d, 4).has_body);
        let mut longer = d.clone();
        longer.push(0); // the C codec refuses it too
        assert!(!error_info(-13, &longer, 4).has_body);
    }
}
