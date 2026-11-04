//! Starts `build/tsdbd` for one test: a port tsdbd chose and a fresh data directory.
//!
//! The binary is `$TSDBD` or `../../build/tsdbd` (build it with `make` in the
//! repository root); data goes under `$TSDB_TEST_DIR` or the system temp
//! directory. The server is ready once it has written its port file
//! (`--port 0 --port-file`: "PORT PID"); the pid must be the child's, so a
//! test never talks to a server it did not start.

#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
        // tsdbd picks its port (--port 0) and writes "PORT PID" to this file once it serves: no port is
        // computed here, and the pid in the file must be the child started here.
        let pf = dir.with_extension("port");
        let _ = std::fs::remove_file(&pf);
        let mut child = Command::new(&bin)
            .arg("--dir")
            .arg(&dir)
            .args(["--port", "0", "--port-file"])
            .arg(&pf)
            .args(["--no-fsync", "--workers", "4", "--no-auth"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            // The sanitizer runs (asan/tsan of the embedded engine) preload their runtime into the test binary;
            // tsdbd is not instrumented, and under a preloaded TSan its SIGTERM handler waits for an intercepted
            // call that its io_uring_enter never makes, so the server would never stop.
            .env_remove("LD_PRELOAD")
            .env_remove("TSAN_OPTIONS")
            .env_remove("ASAN_OPTIONS")
            .spawn()
            .expect("spawn tsdbd");
        let log = Arc::new(Mutex::new(String::new()));
        let stderr = BufReader::new(child.stderr.take().unwrap());
        let sink = Arc::clone(&log);
        std::thread::spawn(move || {
            for l in stderr.lines().map_while(Result::ok) {
                eprintln!("{l}");
                let mut g = sink.lock().unwrap();
                g.push_str(&l);
                g.push('\n');
            }
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some(st) = child.try_wait().expect("wait for tsdbd") {
                panic!("tsdbd exited with {st} before it listened:\n{}", log.lock().unwrap());
            }
            if let Ok(text) = std::fs::read_to_string(&pf) {
                let mut it = text.split_whitespace().map(|x| x.parse::<u32>());
                if let (Some(Ok(port)), Some(Ok(pid))) = (it.next(), it.next()) {
                    let _ = std::fs::remove_file(&pf);
                    if pid != child.id() {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("{} names pid {pid}, but the server started here is pid {}", pf.display(), child.id());
                    }
                    return Server { child: Some(child), port: port as u16, dir };
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("tsdbd did not write {} in 30 s:\n{}", pf.display(), log.lock().unwrap());
    }

    /// Adds volume `path` to the running server's database, as an operator does: `tsdb <dir> volume add`
    /// (tsdbd never creates a volume for a client). The CLI is the `tsdb` next to the tsdbd in use.
    pub fn add_volume(&self, path: &std::path::Path) {
        let tsdbd = std::env::var_os("TSDBD")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../build/tsdbd"));
        let cli = tsdbd.with_file_name("tsdb");
        let out = Command::new(&cli)
            .arg(&self.dir)
            .args(["volume", "add"])
            .arg(path)
            .env_remove("LD_PRELOAD")
            .output()
            .unwrap_or_else(|e| panic!("{}: {e}", cli.display()));
        assert!(out.status.success(), "volume add {}: {}", path.display(), String::from_utf8_lossy(&out.stderr));
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
