//! TCP keepalive for the client's socket.
//!
//! std has no keepalive, so the options are set with `setsockopt` on Unix. A
//! peer that vanished without a reset (power loss, a cut cable, a NAT that
//! forgot the connection) would otherwise leave an idle connection looking
//! healthy for hours; with keepalive the next read or write fails once the
//! probes go unanswered.

use std::io;
use std::net::TcpStream;
use std::time::Duration;

/// Keepalive probe settings of a connection.
///
/// The default (see [`Default`]) starts probing after 30 s of silence, probes
/// every 10 s and gives up after 3 unanswered probes: a dead peer is noticed
/// about a minute after the last traffic. Keepalive only watches an idle
/// connection; use [`Client::set_timeout`](crate::Client::set_timeout) to bound
/// a request that is waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeepAlive {
    /// Silence before the first probe (rounded up to whole seconds).
    pub idle: Duration,
    /// Time between probes (rounded up to whole seconds).
    pub interval: Duration,
    /// Unanswered probes before the connection is dropped.
    pub count: u32,
}

impl Default for KeepAlive {
    fn default() -> KeepAlive {
        KeepAlive { idle: Duration::from_secs(30), interval: Duration::from_secs(10), count: 3 }
    }
}

/// The socket options for idle time, probe interval and probe count. Only the
/// targets whose libc binding has them are listed: OpenBSD and others lack the
/// constants, and naming them would stop the crate from building there.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
const PROBE_OPTIONS: Option<(i32, i32, i32)> = Some((libc::TCP_KEEPIDLE, libc::TCP_KEEPINTVL, libc::TCP_KEEPCNT));
#[cfg(any(target_os = "macos", target_os = "ios"))]
const PROBE_OPTIONS: Option<(i32, i32, i32)> = Some((libc::TCP_KEEPALIVE, libc::TCP_KEEPINTVL, libc::TCP_KEEPCNT));
#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "macos",
        target_os = "ios"
    ))
))]
const PROBE_OPTIONS: Option<(i32, i32, i32)> = None;

#[cfg(unix)]
pub(crate) fn apply(stream: &TcpStream, ka: Option<KeepAlive>) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    fn set(fd: i32, level: i32, name: i32, value: i32) -> io::Result<()> {
        // SAFETY: `value` is a live c_int for the whole call and the length passed is its size.
        let rc = unsafe {
            libc::setsockopt(fd, level, name, (&raw const value).cast(), size_of::<libc::c_int>() as libc::socklen_t)
        };
        if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }
    let secs = |d: Duration| (d.as_secs() + u64::from(d.subsec_nanos() > 0)).clamp(1, i32::MAX as u64) as i32;
    let fd = stream.as_raw_fd();
    let Some(ka) = ka else {
        return set(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 0);
    };
    set(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1)?;
    // The probe settings are a refinement: a platform without one of them
    // keeps its (long) system default rather than failing the connection.
    if let Some((idle, interval, count)) = PROBE_OPTIONS {
        let _ = set(fd, libc::IPPROTO_TCP, idle, secs(ka.idle));
        let _ = set(fd, libc::IPPROTO_TCP, interval, secs(ka.interval));
        let _ = set(fd, libc::IPPROTO_TCP, count, ka.count.clamp(1, 127) as i32);
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn apply(_stream: &TcpStream, _ka: Option<KeepAlive>) -> io::Result<()> {
    Ok(())
}
