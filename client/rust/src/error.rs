//! Errors: server statuses (`TSDB_ERR_*` in `tsdb.h`), I/O, protocol violations
//! and arguments rejected before anything is sent.

use std::fmt;
use std::io;

/// A status code returned by the server, one of the negative `TSDB_ERR_*`
/// codes in `include/tsdb.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Status {
    /// `TSDB_ERR_IO` (-1): an I/O error on the server.
    Io,
    /// `TSDB_ERR_NOMEM` (-2): the server ran out of memory.
    NoMem,
    /// `TSDB_ERR_CORRUPT` (-3): corrupt data or a malformed request.
    Corrupt,
    /// `TSDB_ERR_INVALID` (-4): an invalid argument, such as a negative interval.
    Invalid,
    /// `TSDB_ERR_LOCKED` (-5): the database is locked by another process.
    Locked,
    /// `TSDB_ERR_NOTFOUND` (-6): no such series.
    NotFound,
    /// `TSDB_ERR_TOO_MANY_BUCKETS` (-7): an aggregate would return too many buckets.
    TooManyBuckets,
    /// `TSDB_ERR_STOPPED` (-8): the query was stopped, or the database is closing.
    Stopped,
    /// `TSDB_ERR_READONLY` (-9): the server serves a read-only database.
    ReadOnly,
    /// `TSDB_ERR_MOVED` (-10): another node owns the volume; definitely not applied. The body is in
    /// [`Client::last_error`](crate::Client::last_error); the client follows it by itself unless retries are off.
    Moved,
    /// `TSDB_ERR_UNAVAILABLE` (-11): the volume's storage is unavailable; definitely not applied. The client
    /// retries it after the server's `retry_after` within its time budget; this is what is left when the budget
    /// ran out. The body is in [`Client::last_error`](crate::Client::last_error).
    Unavailable,
    /// `TSDB_ERR_PARTIAL` (-12): a frame spanning volumes was applied for some of them only.
    Partial,
    /// `TSDB_ERR_DROPPED` (-13): below the volume's drop watermark; never applied, never to be sent again.
    Dropped,
    /// `TSDB_ERR_DENIED` (-16): the user holds no right for this, or the series does not exist (tsdbd answers
    /// both alike, so no user learns what other tenants hold).
    Denied,
    /// `TSDB_ERR_AUTH` (-17): the login failed (unknown user or wrong password look alike), or the server
    /// requires one first.
    Auth,
    /// A code this client does not know.
    Unknown(i32),
}

impl Status {
    /// Maps a negative status code to a [`Status`].
    pub fn from_code(code: i32) -> Status {
        match code {
            -1 => Status::Io,
            -2 => Status::NoMem,
            -3 => Status::Corrupt,
            -4 => Status::Invalid,
            -5 => Status::Locked,
            -6 => Status::NotFound,
            -7 => Status::TooManyBuckets,
            -8 => Status::Stopped,
            -9 => Status::ReadOnly,
            -10 => Status::Moved,
            -11 => Status::Unavailable,
            -12 => Status::Partial,
            -13 => Status::Dropped,
            -16 => Status::Denied,
            -17 => Status::Auth,
            c => Status::Unknown(c),
        }
    }

    /// The numeric `TSDB_ERR_*` code.
    pub fn code(self) -> i32 {
        match self {
            Status::Io => -1,
            Status::NoMem => -2,
            Status::Corrupt => -3,
            Status::Invalid => -4,
            Status::Locked => -5,
            Status::NotFound => -6,
            Status::TooManyBuckets => -7,
            Status::Stopped => -8,
            Status::ReadOnly => -9,
            Status::Moved => -10,
            Status::Unavailable => -11,
            Status::Partial => -12,
            Status::Dropped => -13,
            Status::Denied => -16,
            Status::Auth => -17,
            Status::Unknown(c) => c,
        }
    }

    /// A short description, the same text as `tsdbc_strerror`.
    pub fn description(self) -> &'static str {
        strerror(self.code())
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())
    }
}

/// The text for a `TSDB_ERR_*` code (0 is "ok"), as `tsdbc_strerror` in the C client.
pub fn strerror(code: i32) -> &'static str {
    match code {
        0 => "ok",
        -1 => "I/O error (or connection lost)",
        -2 => "out of memory",
        -3 => "corrupt data or protocol error",
        -4 => "invalid argument",
        -5 => "database is locked by another process",
        -6 => "not found",
        -7 => "too many buckets",
        -8 => "stopped",
        -9 => "database is read only",
        -10 => "the volume is owned by another node",
        -11 => "the volume is unavailable (storage down or recovering); not applied",
        -12 => "applied for some volumes only",
        -13 => "below the volume's drop watermark; never applied",
        -16 => "permission denied",
        -17 => "login failed or required",
        _ => "unknown error",
    }
}

/// Everything a [`Client`](crate::Client) call can fail with.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The server executed the request and answered with an error status; for an embedded
    /// [`Db`](crate::Db), the engine's status.
    Server(Status),
    /// The connection failed (or was already broken by an earlier failure).
    Io(io::Error),
    /// The server's reply did not follow the protocol; the connection is unusable.
    Protocol(String),
    /// The arguments were rejected before anything was sent.
    InvalidArgument(String),
    /// Entry `index` of a batch call (a query or an insert of `*_many`) failed with `error`.
    /// It is the lowest failing entry: every entry below it ran and succeeded.
    Batch {
        /// The failing entry's position in the batch.
        index: usize,
        /// Its error.
        error: Box<Error>,
    },
    /// The login at connect failed or was refused, by the server ([`Status::Auth`]: unknown user or wrong
    /// password, which look alike) or by this client: the server did not prove it holds the user's verifier,
    /// it asks for a login and none was given, or it asks for none and the options require one.
    Auth(String),
}

impl Error {
    /// The `TSDB_ERR_*` code closest to this error: the server's status, or
    /// -1 (I/O), -3 (protocol) or -4 (invalid argument) for client-side errors.
    pub fn code(&self) -> i32 {
        match self {
            Error::Server(s) => s.code(),
            Error::Io(_) => -1,
            Error::Protocol(_) => -3,
            Error::InvalidArgument(_) => -4,
            Error::Auth(_) => Status::Auth.code(),
            Error::Batch { error, .. } => error.code(),
        }
    }

    /// The server status, if the server reported this error; [`Status::Auth`] for every failed login.
    pub fn status(&self) -> Option<Status> {
        match self {
            Error::Server(s) => Some(*s),
            Error::Auth(_) => Some(Status::Auth),
            Error::Batch { error, .. } => error.status(),
            _ => None,
        }
    }

    /// For an error of a batch call, the failing entry's position in the batch.
    pub fn batch_index(&self) -> Option<usize> {
        match self {
            Error::Batch { index, .. } => Some(*index),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Server(s) => write!(f, "server error {}: {}", s.code(), s.description()),
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Auth(m) => write!(f, "login failed: {m}"),
            Error::Batch { index, error } => write!(f, "batch entry {index}: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Batch { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

/// The result type of this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;
