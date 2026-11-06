//! Protocol 13 (docs/ha.md 9 and 15.4): the bodies of UNAVAILABLE, MOVED, PARTIAL and DROPPED, retry after
//! `retry_after` within a time budget, MOVED followed to the node it names, several nodes in a connect string,
//! TOPOLOGY. The refusals come from fake servers (a script per test); the outage is real: tsdbd on a storage
//! domain that outagefs takes away (`build/outage_server`, tests/outage_server.c).

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tsdb_client::{Client, ConnectOptions, Error, QueryOpts, Status};

const WHY_NFS_DOWN: u8 = 1;
const OP_FLUSH: u8 = 8;
const OP_INSERT_VALUES: u8 = 14;

// ------------------------------------------------------------------ bodies

fn unavail_entry(vol: u16, retry_after_ms: u32) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&vol.to_le_bytes());
    b.push(WHY_NFS_DOWN);
    b.push(0);
    b.extend_from_slice(&7u32.to_le_bytes());
    b.extend_from_slice(&123_456i64.to_le_bytes());
    b.extend_from_slice(&retry_after_ms.to_le_bytes());
    b.extend_from_slice(&99u64.to_le_bytes());
    b
}

fn unavailable_body(vol: u16, retry_after_ms: u32) -> Vec<u8> {
    let mut b = 1u16.to_le_bytes().to_vec();
    b.extend(unavail_entry(vol, retry_after_ms));
    b
}

fn moved_body(addr: &str) -> Vec<u8> {
    let mut b = 1u16.to_le_bytes().to_vec();
    b.extend_from_slice(&2u16.to_le_bytes()); // vol
    b.extend_from_slice(&5u32.to_le_bytes()); // epoch
    b.push(0);
    b.extend_from_slice(&7u32.to_le_bytes()); // node
    b.extend_from_slice(&(addr.len() as u16).to_le_bytes());
    b.extend_from_slice(addr.as_bytes());
    b
}

// ------------------------------------------------------------------ a fake server

/// One request as the fake server read it.
#[derive(Clone, Debug)]
struct Req {
    #[allow(dead_code)]
    conn: usize,
    op: u8,
    id: u32,
    /// how many requests of this op came before, on every connection
    nth: usize,
}

/// What the script answers: a status and the bytes after it.
struct Reply(i32, Vec<u8>);

struct Fake {
    addr: SocketAddr,
    log: Arc<Mutex<Vec<Req>>>,
}

impl Fake {
    /// Listens on a port of its own; every connection is answered by `script` (HELLO excepted: protocol 13,
    /// no login). The threads end with the process.
    fn start(script: impl Fn(&Req) -> Reply + Send + Sync + 'static) -> Fake {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::<Req>::new()));
        let script = Arc::new(script);
        let lg = Arc::clone(&log);
        thread::spawn(move || {
            for (conn, s) in l.incoming().enumerate() {
                let (script, log) = (Arc::clone(&script), Arc::clone(&lg));
                thread::spawn(move || serve(conn, s.unwrap(), &*script, &log));
            }
        });
        Fake { addr, log }
    }

    fn seen(&self, op: u8) -> Vec<Req> {
        self.log.lock().unwrap().iter().filter(|r| r.op == op).cloned().collect()
    }
}

fn serve(conn: usize, mut s: TcpStream, script: &(dyn Fn(&Req) -> Reply + Send + Sync), log: &Mutex<Vec<Req>>) {
    loop {
        let mut h = [0u8; 9];
        if s.read_exact(&mut h).is_err() {
            return;
        }
        let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
        let id = u32::from_le_bytes(h[4..8].try_into().unwrap());
        let op = h[8];
        let mut payload = vec![0u8; len - 5];
        if s.read_exact(&mut payload).is_err() {
            return;
        }
        let reply = if op == 1 {
            // HELLO: u32 version, u16 flags, u16 mechanisms
            let mut b = 13u32.to_le_bytes().to_vec();
            b.extend_from_slice(&[0, 0, 0, 0]);
            Reply(0, b)
        } else {
            let req = {
                let mut g = log.lock().unwrap();
                let nth = g.iter().filter(|r| r.op == op).count();
                let r = Req { conn, op, id, nth };
                g.push(r.clone());
                r
            };
            script(&req)
        };
        let mut f = ((9 + reply.1.len()) as u32).to_le_bytes().to_vec();
        f.extend_from_slice(&id.to_le_bytes());
        f.push(0);
        f.extend_from_slice(&reply.0.to_le_bytes());
        f.extend_from_slice(&reply.1);
        if s.write_all(&f).is_err() {
            return;
        }
    }
}

fn ok() -> Reply {
    Reply(0, Vec::new())
}

fn unavailable(retry_after_ms: u32) -> Reply {
    Reply(-11, unavailable_body(1, retry_after_ms))
}

// ------------------------------------------------------------------ retry

#[test]
fn unavailable_is_retried_after_retry_after() {
    let f = Fake::start(|r| if r.nth == 0 { unavailable(300) } else { ok() });
    let mut c = Client::connect(f.addr).unwrap();
    let t0 = Instant::now();
    c.flush().unwrap();
    // the server asked for 300 ms, so the request goes again no sooner (mutation "ignore retry_after")
    assert!(t0.elapsed() >= Duration::from_millis(280), "retried after {:?}", t0.elapsed());
    assert!(t0.elapsed() < Duration::from_secs(5));
    let seen = f.seen(OP_FLUSH);
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].id, seen[1].id, "the same request again");
    // the body of the refusal is kept
    let e = c.last_error();
    assert_eq!(e.status, -11);
    assert!(e.has_body, "the body was dropped");
    assert_eq!(e.unavailable.len(), 1);
    assert_eq!((e.unavailable[0].vol, e.unavailable[0].reason, e.unavailable[0].epoch), (1, WHY_NFS_DOWN, 7));
    assert_eq!((e.unavailable[0].since_ms, e.unavailable[0].outage_id), (123_456, 99));
    assert_eq!(e.retry_after_ms(), 300);
    // and the connection goes on with the next request
    c.flush().unwrap();
}

#[test]
fn budget_runs_out() {
    let f = Fake::start(|_| unavailable(400));
    let mut c = Client::connect(f.addr).unwrap();
    c.set_retry(Some(Duration::from_millis(1500)));
    let t0 = Instant::now();
    let e = c.flush().unwrap_err();
    assert_eq!(e.status(), Some(Status::Unavailable), "{e}");
    assert!(t0.elapsed() >= Duration::from_millis(1000) && t0.elapsed() < Duration::from_secs(4), "{:?}", t0.elapsed());
    let n = f.seen(OP_FLUSH).len();
    assert!((3..=5).contains(&n), "{n} tries in 1.5 s at 400 ms");
    assert_eq!(c.last_error().unavailable[0].retry_after_ms, 400);
    // the refusal is an answer: the connection is fine
    assert!(!c.is_broken());
}

#[test]
fn retry_off_returns_the_first_refusal() {
    let f = Fake::start(|_| unavailable(5));
    let mut c = Client::connect(f.addr).unwrap();
    c.set_retry(None);
    assert_eq!(c.flush().unwrap_err().status(), Some(Status::Unavailable));
    assert_eq!(f.seen(OP_FLUSH).len(), 1);
    assert!(c.last_error().has_body);
}

#[test]
fn a_refusal_without_a_body_waits_a_second() {
    // protocol 12 shape: the status alone
    let f = Fake::start(|r| if r.nth == 0 { Reply(-11, Vec::new()) } else { ok() });
    let mut c = Client::connect(f.addr).unwrap();
    let t0 = Instant::now();
    c.flush().unwrap();
    assert!(t0.elapsed() >= Duration::from_millis(950), "{:?}", t0.elapsed());
    assert!(!c.last_error().has_body);
    assert_eq!(c.last_error().status, -11);
}

#[test]
fn a_malformed_body_is_only_a_status() {
    // two entries promised, one present
    let mut b = 2u16.to_le_bytes().to_vec();
    b.extend(unavail_entry(1, 10));
    let f = Fake::start(move |r| if r.nth == 0 { Reply(-11, b.clone()) } else { ok() });
    let mut c = Client::connect(f.addr).unwrap();
    c.flush().unwrap();
    assert!(!c.last_error().has_body);
}

#[test]
fn streamed_reply_refused_before_it_begins_is_retried() {
    // QUERY_RAW: the empty chunk (last 1, n 0) comes first in an error reply, the body after it
    let f = Fake::start(|r| {
        if r.nth == 0 {
            let mut b = vec![1, 0, 0, 0, 0];
            b.extend(unavailable_body(1, 50));
            Reply(-11, b)
        } else {
            Reply(0, vec![1, 0, 0, 0, 0])
        }
    });
    let mut c = Client::connect(f.addr).unwrap();
    let p = c.query_points(0x1_0000_0001, .., QueryOpts::default()).unwrap();
    assert_eq!(p.len(), 0);
    assert_eq!(f.seen(5).len(), 2);
    assert!(c.last_error().has_body);
}

#[test]
fn pipelined_inserts_go_again_in_order_from_the_first_refused() {
    // frames 0..4 of INSERT_VALUES; the second is refused once: it and every frame after it are sent again, in order
    let f = Fake::start(|r| if r.nth == 1 { unavailable(50) } else { ok() });
    let mut c = Client::connect(f.addr).unwrap();
    for i in 0..4i64 {
        c.insert_series_async(0x1_0000_0001, &[i], &[i as f32]).unwrap();
    }
    c.wait().unwrap();
    let ids: Vec<u32> = f.seen(OP_INSERT_VALUES).iter().map(|r| r.id).collect();
    assert_eq!(ids.len(), 4 + 3, "{ids:?}");
    let first = ids[0];
    assert_eq!(ids, [first, first + 1, first + 2, first + 3, first + 1, first + 2, first + 3], "{ids:?}");
    assert_eq!(c.pending(), 0);
}

#[test]
fn a_refused_pipeline_past_the_budget_fails_the_wait() {
    let f = Fake::start(|_| unavailable(500));
    let mut c = Client::connect(f.addr).unwrap();
    c.set_retry(Some(Duration::from_millis(700)));
    c.insert_series_async(0x1_0000_0001, &[1], &[1.0]).unwrap();
    assert_eq!(c.wait().unwrap_err().status(), Some(Status::Unavailable));
    assert!(!c.is_broken());
}

// ------------------------------------------------------------------ MOVED, nodes

#[test]
fn moved_is_followed_to_the_node_it_names() {
    let b = Fake::start(|_| ok());
    let baddr = b.addr.to_string();
    let a = Fake::start(move |_| Reply(-10, moved_body(&baddr)));
    let mut c = Client::connect(a.addr).unwrap();
    c.flush().unwrap();
    assert_eq!(a.seen(OP_FLUSH).len(), 1);
    assert_eq!(b.seen(OP_FLUSH).len(), 1, "the request went to the new owner");
    let e = c.last_error();
    assert_eq!(e.status, -10);
    assert!(e.has_body);
    assert_eq!((e.moved[0].vol, e.moved[0].epoch, e.moved[0].node_id), (2, 5, 7));
    assert_eq!(e.moved[0].addr, b.addr.to_string());
    // later requests stay on B
    c.flush().unwrap();
    assert_eq!(a.seen(OP_FLUSH).len(), 1);
    assert_eq!(b.seen(OP_FLUSH).len(), 2);
}

#[test]
fn moved_without_an_address_goes_to_the_next_node() {
    let b = Fake::start(|_| ok());
    let a = Fake::start(|_| Reply(-10, moved_body("")));
    let nodes = format!("{},{}", a.addr, b.addr);
    // the list is shuffled: start from whichever the client picked, the other one is the owner
    let mut c = Client::connect_nodes(&nodes, &ConnectOptions::new()).unwrap();
    let r = c.flush();
    let (na, nb) = (a.seen(OP_FLUSH).len(), b.seen(OP_FLUSH).len());
    if na == 0 {
        // the client started on B, which owns: nothing moved
        assert!(r.is_ok());
        assert_eq!(nb, 1);
    } else {
        assert!(r.is_ok(), "{r:?}");
        assert_eq!((na, nb), (1, 1));
    }
}

#[test]
fn moved_with_nowhere_to_go_is_the_answer() {
    let a = Fake::start(|_| Reply(-10, moved_body("")));
    let mut c = Client::connect(a.addr).unwrap();
    let e = c.flush().unwrap_err();
    assert_eq!(e.status(), Some(Status::Moved));
    assert!(!c.is_broken());
}

#[test]
fn connect_nodes_skips_a_dead_node() {
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let b = Fake::start(|_| ok());
    for _ in 0..4 {
        let mut c = Client::connect_nodes(&format!("{dead}, {}", b.addr), &ConnectOptions::new()).unwrap();
        c.flush().unwrap();
    }
    assert_eq!(b.seen(OP_FLUSH).len(), 4);
    assert!(Client::connect_nodes(&dead.to_string(), &ConnectOptions::new()).is_err());
    assert!(matches!(Client::connect_nodes("", &ConnectOptions::new()), Err(Error::InvalidArgument(_))));
    assert!(matches!(Client::connect_nodes("a:0", &ConnectOptions::new()), Err(Error::InvalidArgument(_))));
}

// ------------------------------------------------------------------ PARTIAL and DROPPED

#[test]
fn partial_and_dropped_are_decoded() {
    let f = Fake::start(|r| {
        if r.nth == 0 {
            let mut b = 2u16.to_le_bytes().to_vec();
            for (v, st) in [(1u16, 0i16), (3, -11)] {
                b.extend_from_slice(&v.to_le_bytes());
                b.extend_from_slice(&st.to_le_bytes());
            }
            b.extend_from_slice(&10u32.to_le_bytes());
            b.extend_from_slice(&[0x24, 0x01]);
            b.extend(unavailable_body(3, 100));
            Reply(-12, b)
        } else {
            let mut b = 5u16.to_le_bytes().to_vec();
            b.extend_from_slice(&1_700_000_000_999i64.to_le_bytes());
            Reply(-13, b)
        }
    });
    let mut c = Client::connect(f.addr).unwrap();
    // neither is retried: PARTIAL is partly applied, DROPPED is final
    assert_eq!(c.flush().unwrap_err().status(), Some(Status::Partial));
    let e = c.last_error().clone();
    assert!(e.has_body);
    assert_eq!(e.volumes.len(), 2);
    assert_eq!((e.volumes[1].vol, e.volumes[1].status), (3, -11));
    assert_eq!(e.rows, 10);
    let rows: Vec<usize> = (0..10).filter(|&i| e.row_not_applied(i)).collect();
    assert_eq!(rows, [2, 5, 8]);
    assert_eq!(e.unavailable[0].retry_after_ms, 100);
    assert_eq!(c.flush().unwrap_err().status(), Some(Status::Dropped));
    let e = c.last_error();
    assert!(e.has_body);
    assert_eq!((e.dropped_vol, e.dropped_before), (5, 1_700_000_000_999));
    assert_eq!(f.seen(OP_FLUSH).len(), 2);
}

// ------------------------------------------------------------------ tsdbd

#[test]
fn topology_of_a_single_server() {
    let s = common::Server::start();
    let mut c = s.client();
    let t = c.topology().unwrap();
    assert_eq!(t.nodes.len(), 1);
    assert!(t.nodes[0].alive);
    assert!(!t.volumes.is_empty());
    for v in &t.volumes {
        assert_eq!(v.role, 1, "owner");
        assert_eq!(v.health, 0, "available");
    }
    let g = t.generation;
    assert_eq!(c.topology().unwrap().generation, g, "the generation names the answer");
    // --no-auth: STATUS is a superuser's, and --no-auth has none
    let st = c.server_status();
    assert!(st.is_ok() || st.unwrap_err().status() == Some(Status::Denied));
}

/// tests/outage_server.c: tsdbd with volume A on a domain that outagefs can take away.
struct Outage {
    child: Child,
    stdin: std::process::ChildStdin,
    out: BufReader<std::process::ChildStdout>,
    port: u16,
    sid_a: u64,
    sid_root: u64,
    vol_a: u16,
}

impl Outage {
    fn start() -> Option<Outage> {
        let bin = common::tsdbd().with_file_name("outage_server");
        let lib = common::tsdbd().with_file_name("liboutagefs.so");
        if !bin.exists() || !lib.exists() {
            eprintln!("skipped: {} or {} is missing (make builds them on Linux)", bin.display(), lib.display());
            return None;
        }
        let dir = common::test_dir("tsdb-rust-outage");
        std::fs::create_dir_all(&dir).unwrap();
        let mut child = Command::new(bin)
            .arg(&dir)
            .env("OUTAGEFS_LIB", lib)
            .env("TSDBD", common::tsdbd())
            .env_remove("LD_PRELOAD")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let mut out = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        out.read_line(&mut line).unwrap();
        let f: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(f.first(), Some(&"READY"), "outage_server said {line:?}");
        let o = Outage {
            port: f[1].parse().unwrap(),
            sid_a: f[2].parse().unwrap(),
            sid_root: f[3].parse().unwrap(),
            vol_a: f[4].parse().unwrap(),
            child,
            stdin,
            out,
        };
        Some(o)
    }

    fn command(&mut self, cmd: &str) {
        writeln!(self.stdin, "{cmd}").unwrap();
        let mut line = String::new();
        self.out.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "ok", "{cmd}");
    }

    fn client(&self) -> Client {
        Client::connect(("127.0.0.1", self.port)).unwrap()
    }
}

impl Drop for Outage {
    fn drop(&mut self) {
        let _ = writeln!(self.stdin, "quit");
        let mut rest = String::new();
        let _ = self.out.read_to_string(&mut rest);
        let st = self.child.wait().unwrap();
        if !thread::panicking() {
            assert!(st.success(), "outage_server: {rest}");
        }
    }
}

#[test]
fn an_outage_gives_unavailable_with_its_body_and_the_client_retries_then_succeeds() {
    let Some(mut o) = Outage::start() else { return };
    let mut c = o.client();
    c.set_retry(None);
    c.insert_series(o.sid_a, &[1000], &[1.0]).unwrap();

    // the topology of a healthy server
    let t = c.topology().unwrap();
    assert!(t.volumes.iter().all(|v| v.health == 0));

    o.command("down");
    let e = c.insert_series(o.sid_a, &[2000], &[2.0]).unwrap_err();
    assert_eq!(e.status(), Some(Status::Unavailable), "{e}");
    let info = c.last_error().clone();
    assert!(info.has_body, "the body was dropped");
    assert_eq!(info.unavailable[0].vol, o.vol_a);
    assert!(info.unavailable[0].reason != 0 && info.unavailable[0].retry_after_ms > 0 && info.unavailable[0].since_ms > 0);
    // the root answers while A is away
    c.insert_series(o.sid_root, &[2001], &[2.0]).unwrap();
    let t = c.topology().unwrap();
    let a = t.volumes.iter().find(|v| v.vol == o.vol_a).unwrap();
    assert_eq!((a.health, a.reason != 0), (2, true));
    assert!(t.volumes.iter().filter(|v| v.vol != o.vol_a).all(|v| v.health == 0));

    // a budget shorter than the outage: the status comes back after about the budget
    let mut c2 = o.client();
    c2.set_retry(Some(Duration::from_millis(1200)));
    let t0 = Instant::now();
    assert_eq!(c2.insert_series(o.sid_a, &[3000], &[3.0]).unwrap_err().status(), Some(Status::Unavailable));
    assert!(t0.elapsed() < Duration::from_secs(4), "{:?}", t0.elapsed());

    // the default budget: the call waits, the volume heals, the call succeeds (once)
    let (port, sid_a) = (o.port, o.sid_a);
    let h = thread::spawn(move || {
        let mut c3 = Client::connect(("127.0.0.1", port)).unwrap();
        let t0 = Instant::now();
        c3.insert_series(sid_a, &[4000], &[4.0]).unwrap(); // mutation "no retry": UNAVAILABLE
        t0.elapsed()
    });
    thread::sleep(Duration::from_millis(1500));
    o.command("heal");
    let took = h.join().unwrap();
    assert!(took >= Duration::from_millis(1200) && took < Duration::from_secs(25), "{took:?}");

    // rows 1000 and 4000, not the refused 2000 and 3000
    let mut n = 0;
    for _ in 0..100 {
        match c.query_points(o.sid_a, .., QueryOpts::default()) {
            Ok(p) => {
                n = p.len();
                break;
            }
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    }
    assert_eq!(n, 2);
}
