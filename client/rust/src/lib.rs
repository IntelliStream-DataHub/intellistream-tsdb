//! A blocking client for tsdbd, the TCP server of the tsdb time series database.
//!
//! The protocol is the one in `include/tsdb_protocol.h`; this crate mirrors the
//! C client in `client/tsdb_client.h` and has no dependencies beyond `std`.
//!
//! - Timestamps are `i64` milliseconds since the epoch. A series has a
//!   [`ValueType`] (float32, float64, int64, bool or state); values travel as
//!   typed [`Column`]s with an optional validity [`Bitmap`] for nulls
//!   ([`Client::insert_series_values`], [`Client::query_rows`]). Float32
//!   series also have the version 1 fast path with plain `f32` slices
//!   ([`Client::insert_series`], [`Client::query_points`]).
//! - Every call returns [`Result`]; a server status arrives as
//!   [`Error::Server`] with a [`Status`], and [`Error::code`] gives the
//!   `TSDB_ERR_*` number.
//! - Inserts can be pipelined with [`Client::insert_series_async`] /
//!   [`Client::insert_async`] and collected with [`Client::wait`]. The server
//!   runs one connection's requests in order, so the last write still wins.
//!
//! ```no_run
//! use tsdb_client::{Client, QueryOpts};
//!
//! let mut c = Client::connect(("127.0.0.1", tsdb_client::DEFAULT_PORT))?;
//! let id = c.series("tank.level_bar")?;
//! c.insert_series(id, &[1_700_000_000_000], &[4.2])?;
//! for (ts, v) in c.query_points(id, .., QueryOpts::asc())?.iter() {
//!     println!("{ts} {v}");
//! }
//! # Ok::<(), tsdb_client::Error>(())
//! ```
#![warn(missing_docs)]

mod client;
mod error;
mod protocol;
mod types;
mod value;

pub use client::Client;
pub use error::{Error, Result, Status, strerror};
pub use protocol::{DEFAULT_PORT, PROTOCOL_VERSION};
pub use types::{Bucket, Group, Order, Points, QueryOpts, Rows, Stats, Unit};
pub use value::{Bitmap, Column, ColumnRef, NULL_F32_BITS, NULL_F64_BITS, Native, Value, ValueType, Values, ValuesRef};
