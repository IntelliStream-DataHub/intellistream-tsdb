//! Starts `build/tsdbd` for one test: a free port and a fresh data directory.
//!
//! The binary is `$TSDBD` or `../../build/tsdbd` (build it with `make` in the
//! repository root); data goes under `$TSDB_TEST_DIR` or the system temp
//! directory. The server is ready once it prints its "serving" line, which it
//! does after binding: a successful connect alone could reach another server
//! that already holds the port.

#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use tsdb_client::Client;

/// Milliseconds on a minute boundary (2023-11-14 22:13 UTC), so buckets are whole.
pub const T0: i64 = 1_699_999_980_000;

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}
const SIGTERM: i32 = 15;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A fresh directory path (not created) under `$TSDB_TEST_DIR` or the system temp directory.
pub fn test_dir(prefix: &str) -> PathBuf {
    let base = std::env::var_os("TSDB_TEST_DIR").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("{prefix}-{}-{}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

pub struct Server {
    child: Option<Child>,
    pub port: u16,
    pub dir: PathBuf,
}

impl Server {
    pub fn start() -> Server {
        Server::start_in(test_dir("tsdb-rust"))
    }

    /// Starts tsdbd on `dir`, which may already hold a database.
    pub fn start_in(dir: PathBuf) -> Server {
        let bin = std::env::var_os("TSDBD")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../build/tsdbd"));
        assert!(bin.exists(), "{} is missing: run make in the repository root", bin.display());
        let mut log = String::new();
        for _ in 0..10 {
            let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
            let mut child = Command::new(&bin)
                .arg("--dir")
                .arg(&dir)
                .args(["--port", &port.to_string(), "--no-fsync", "--workers", "4"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn tsdbd");
            let mut stderr = BufReader::new(child.stderr.take().unwrap());
            let mut line = String::new();
            loop {
                line.clear();
                if stderr.read_line(&mut line).unwrap_or(0) == 0 {
                    break; // exited, most likely because the port was taken
                }
                log.push_str(&line);
                if line.contains("serving") {
                    std::thread::spawn(move || {
                        for l in stderr.lines().map_while(Result::ok) {
                            eprintln!("{l}");
                        }
                    });
                    return Server { child: Some(child), port, dir };
                }
            }
            let _ = child.wait();
        }
        panic!("tsdbd did not start:\n{log}");
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn client(&self) -> Client {
        Client::connect(self.addr()).expect("connect")
    }

    /// Stops the server with SIGTERM and returns its exit status.
    pub fn stop(&mut self) -> ExitStatus {
        let mut child = self.child.take().expect("running");
        unsafe { kill(child.id() as i32, SIGTERM) };
        child.wait().expect("wait for tsdbd")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.is_some() {
            let st = self.stop();
            if !std::thread::panicking() {
                assert!(st.success(), "tsdbd exited with {st}");
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_dir_all(self.dir.with_extension("volume"));
    }
}
