//! Request timeouts and keepalive, against fake servers that stall.
//!
//! No tsdbd here: each test runs a listener that accepts and then misbehaves
//! in one way, and checks that the client's call fails with a timed-out I/O
//! error within bounds, and that the client is unusable afterwards.

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tsdb_client::{Client, Error, KeepAlive};

const TIMEOUT: Duration = Duration::from_millis(300);
/// How long a call may take at most before the test calls it a hang.
const BOUND: Duration = Duration::from_secs(5);

/// Fails the whole test binary if a test is still running after this long: a
/// regression in the timeouts shows up as a hang, which must not stall CI.
const WATCHDOG: Duration = Duration::from_secs(60);

/// Armed at the start of a test, disarmed when the test returns.
struct Watchdog(Option<mpsc::Sender<()>>);

fn watchdog() -> Watchdog {
    let (tx, rx) = mpsc::channel::<()>();
    let name = thread::current().name().unwrap_or("?").to_string();
    thread::spawn(move || {
        if rx.recv_timeout(WATCHDOG) == Err(mpsc::RecvTimeoutError::Timeout) {
            eprintln!("test {name} hangs: no timeout fired within {WATCHDOG:?}");
            std::process::exit(101);
        }
    });
    Watchdog(Some(tx))
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.0.take());
    }
}

/// A server running `behave` on the first connection; dropping the handle
/// releases it, which closes the connection.
struct Fake {
    addr: SocketAddr,
    release: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Fake {
    fn start(behave: impl FnOnce(TcpStream, mpsc::Receiver<()>) + Send + 'static) -> Fake {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            if let Ok((s, _)) = l.accept() {
                behave(s, rx);
            }
        });
        Fake { addr, release: Some(tx), thread: Some(thread) }
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        drop(self.release.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Reads one request frame: (id, op).
fn read_request(s: &mut TcpStream) -> std::io::Result<(u32, u8)> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len)?;
    let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut body)?;
    Ok((u32::from_le_bytes(body[0..4].try_into().unwrap()), body[4]))
}

/// A reply with status 0 and no payload.
fn ack(id: u32) -> [u8; 13] {
    let mut r = [0u8; 13];
    r[0..4].copy_from_slice(&9u32.to_le_bytes());
    r[4..8].copy_from_slice(&id.to_le_bytes());
    r
}

/// Holds the connection open until the test is over.
fn hold(rx: mpsc::Receiver<()>) {
    let _ = rx.recv();
}

fn connect(f: &Fake) -> Client {
    let mut c = Client::connect_with(f.addr, Some(TIMEOUT)).unwrap();
    c.set_compression(0).unwrap(); // no HELLO before the first insert
    c
}

fn is_timeout(e: &Error) -> bool {
    matches!(e, Error::Io(io) if io.kind() == ErrorKind::TimedOut)
}

/// The call timed out within bounds and the client now fails at once.
fn assert_timed_out(c: &mut Client, started: Instant, r: Result<(), Error>) {
    let took = started.elapsed();
    let e = r.expect_err("the call must fail");
    assert!(is_timeout(&e), "expected a timed-out I/O error, got {e:?}");
    assert_eq!(e.code(), -1);
    assert!(took >= TIMEOUT - Duration::from_millis(50), "returned after {took:?}, before the timeout");
    assert!(took < BOUND, "took {took:?}");
    assert!(c.is_broken());
    let again = Instant::now();
    let e = c.series("x").expect_err("a broken client refuses calls");
    assert!(matches!(e, Error::Io(_)), "{e:?}");
    assert!(again.elapsed() < Duration::from_millis(100), "a broken client must fail fast");
    assert!(c.wait().is_err());
}

#[test]
fn server_that_never_answers() {
    let _w = watchdog();
    let f = Fake::start(|mut s, rx| {
        let _ = read_request(&mut s); // takes the request, says nothing
        hold(rx);
    });
    let mut c = connect(&f);
    assert_eq!(c.timeout(), Some(TIMEOUT));
    let t = Instant::now();
    let r = c.series("a").map(|_| ());
    assert_timed_out(&mut c, t, r);
}

#[test]
fn server_that_accepts_and_never_reads() {
    let _w = watchdog();
    let f = Fake::start(|_s, rx| hold(rx));
    let mut c = connect(&f);
    let t = Instant::now();
    let r = c.series("a").map(|_| ());
    assert_timed_out(&mut c, t, r);
}

#[test]
fn server_that_answers_half_a_frame() {
    let _w = watchdog();
    let f = Fake::start(|mut s, rx| {
        if let Ok((id, _)) = read_request(&mut s) {
            let _ = s.write_all(&ack(id)[..6]);
        }
        hold(rx);
    });
    let mut c = connect(&f);
    let t = Instant::now();
    let r = c.series("a").map(|_| ());
    assert_timed_out(&mut c, t, r);
}

#[test]
fn server_that_answers_the_header_but_not_the_payload() {
    let _w = watchdog();
    let f = Fake::start(|mut s, rx| {
        if let Ok((id, _)) = read_request(&mut s) {
            let mut r = ack(id);
            r[0..4].copy_from_slice(&(9u32 + 4).to_le_bytes()); // announces 4 payload bytes
            let _ = s.write_all(&r);
        }
        hold(rx);
    });
    let mut c = connect(&f);
    let t = Instant::now();
    let r = c.series("a").map(|_| ());
    assert_timed_out(&mut c, t, r);
}

#[test]
fn server_that_stops_answering_among_pipelined_inserts() {
    let _w = watchdog();
    // acknowledges the first 5 of 20 small inserts, then goes quiet
    let f = Fake::start(|mut s, rx| {
        for _ in 0..5 {
            match read_request(&mut s) {
                Ok((id, _)) => {
                    let _ = s.write_all(&ack(id));
                }
                Err(_) => return,
            }
        }
        hold(rx);
    });
    let mut c = connect(&f);
    for _ in 0..20 {
        c.insert_series_async(1, &[1, 2, 3], &[1.0f32, 2.0, 3.0]).unwrap();
    }
    assert_eq!(c.pending(), 20);
    let t = Instant::now();
    let r = c.wait();
    assert_timed_out(&mut c, t, r);
}

#[test]
fn server_that_stops_reading_among_pipelined_inserts() {
    let _w = watchdog();
    // takes 10 inserts, then stops reading: the socket buffers fill up and a
    // send blocks, which the timeout must bound too
    let f = Fake::start(|mut s, rx| {
        for _ in 0..10 {
            match read_request(&mut s) {
                Ok((id, _)) => {
                    let _ = s.write_all(&ack(id));
                }
                Err(_) => return,
            }
        }
        hold(rx);
    });
    let mut c = connect(&f);
    let (ts, values): (Vec<i64>, Vec<f32>) = (0..200_000).map(|i| (i as i64, i as f32)).unzip();
    let t = Instant::now();
    let mut sent = 0;
    let err = loop {
        match c.insert_series_async(1, &ts, &values) {
            Ok(()) => sent += 1,
            Err(e) => break e,
        }
        assert!(
            sent < 1000 && t.elapsed() < BOUND,
            "{sent} inserts of 2.4 MB were accepted by a server that stopped reading"
        );
    };
    assert!(sent >= 10, "the first inserts fit in the buffers: {sent}");
    assert!(is_timeout(&err), "{err:?}");
    assert!(t.elapsed() < BOUND);
    assert!(c.is_broken());
    assert!(c.wait().is_err());
}

#[test]
fn a_slow_but_live_server_is_not_cut_off() {
    let _w = watchdog();
    // the timeout bounds each wait: replies slower than nothing but faster than the timeout pass
    let f = Fake::start(|mut s, rx| {
        while let Ok((id, _)) = read_request(&mut s) {
            thread::sleep(TIMEOUT / 3);
            let r = ack(id);
            let _ = s.write_all(&r[..6]);
            thread::sleep(TIMEOUT / 3);
            let _ = s.write_all(&r[6..]);
        }
        hold(rx);
    });
    let c = connect(&f);
    let mut c = c;
    for _ in 0..3 {
        c.flush().unwrap();
    }
    assert!(!c.is_broken());
}

#[test]
fn no_timeout_by_default_and_it_can_be_set_and_cleared() {
    let _w = watchdog();
    let f = Fake::start(|_s, rx| hold(rx));
    let c = Client::connect(f.addr).unwrap();
    assert_eq!(c.timeout(), None);
    c.set_timeout(Some(Duration::from_secs(7))).unwrap();
    assert_eq!(c.timeout(), Some(Duration::from_secs(7)));
    c.set_timeout(None).unwrap();
    assert_eq!(c.timeout(), None);
    assert!(c.set_timeout(Some(Duration::ZERO)).is_err());
}

#[cfg(not(target_os = "linux"))]
#[test]
fn connect_with_gives_up() {
    let _w = watchdog();
    // 10.255.255.1 is not routed: the connect either hangs (and must time out) or fails at once
    let t = Instant::now();
    let r = Client::connect_with("10.255.255.1:7070", Some(Duration::from_millis(300)));
    assert!(r.is_err());
    assert!(t.elapsed() < BOUND, "{:?}", t.elapsed());
}

/// A listener whose accept queue is full: further SYNs are dropped, so a
/// connect hangs on any Linux machine, whatever the routing.
#[cfg(target_os = "linux")]
#[test]
fn connect_with_gives_up() {
    use std::os::fd::FromRawFd;
    let _w = watchdog();
    // SAFETY: plain socket calls on a descriptor owned here and handed to TcpListener.
    let l = unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        assert!(fd >= 0);
        let mut a: libc::sockaddr_in = std::mem::zeroed();
        a.sin_family = libc::AF_INET as libc::sa_family_t;
        a.sin_addr.s_addr = u32::from_ne_bytes([127, 0, 0, 1]);
        assert_eq!(libc::bind(fd, (&raw const a).cast(), size_of::<libc::sockaddr_in>() as u32), 0);
        assert_eq!(libc::listen(fd, 0), 0);
        TcpListener::from_raw_fd(fd)
    };
    let addr = l.local_addr().unwrap();
    // fill the queue (backlog 0 holds one connection); the last attempt hangs
    let held: Vec<TcpStream> =
        (0..4).filter_map(|_| TcpStream::connect_timeout(&addr, Duration::from_millis(200)).ok()).collect();
    assert!(!held.is_empty() && held.len() < 4, "the accept queue must fill up, {} connected", held.len());
    let t = Instant::now();
    let Err(e) = Client::connect_with(addr, Some(Duration::from_millis(300))) else { panic!("the connect must fail") };
    let took = t.elapsed();
    assert!(matches!(&e, Error::Io(io) if io.kind() == ErrorKind::TimedOut), "{e:?}");
    assert!(took >= Duration::from_millis(250) && took < BOUND, "{took:?}");
}

#[test]
fn connecting_to_a_closed_port_fails_at_once() {
    let _w = watchdog();
    assert!(Client::connect_with("127.0.0.1:1", Some(TIMEOUT)).is_err());
}

#[cfg(target_os = "linux")]
mod keepalive {
    use super::*;
    use std::os::fd::AsRawFd;

    fn opt(s: &TcpStream, level: i32, name: i32) -> i32 {
        let mut v: libc::c_int = -1;
        let mut len = size_of::<libc::c_int>() as libc::socklen_t;
        let rc = unsafe { libc::getsockopt(s.as_raw_fd(), level, name, (&raw mut v).cast(), &mut len) };
        assert_eq!(rc, 0);
        v
    }

    #[test]
    fn on_by_default_with_short_probes_and_adjustable() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let s = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let probe = s.try_clone().unwrap(); // shares the socket: reads its options
        let c = Client::from_stream(s).unwrap();
        let get = |n| opt(&probe, libc::IPPROTO_TCP, n);
        assert_eq!(opt(&probe, libc::SOL_SOCKET, libc::SO_KEEPALIVE), 1);
        assert_eq!((get(libc::TCP_KEEPIDLE), get(libc::TCP_KEEPINTVL), get(libc::TCP_KEEPCNT)), (30, 10, 3));

        c.set_keepalive(Some(KeepAlive {
            idle: Duration::from_secs(5),
            interval: Duration::from_millis(1500),
            count: 2,
        }))
        .unwrap();
        assert_eq!((get(libc::TCP_KEEPIDLE), get(libc::TCP_KEEPINTVL), get(libc::TCP_KEEPCNT)), (5, 2, 2));

        c.set_keepalive(None).unwrap();
        assert_eq!(opt(&probe, libc::SOL_SOCKET, libc::SO_KEEPALIVE), 0);
    }
}
