//! A client for tsdbd, the network server of the tsdb time series database.
//!
//! One [`Client`] is one TCP connection. With it you create series, insert
//! rows and read them back, raw or aggregated into buckets.
//!
//! ```no_run
//! use tsdb_client::{Client, Group, QueryOpts, Unit};
//!
//! let mut c = Client::connect(("127.0.0.1", tsdb_client::DEFAULT_PORT))?;
//!
//! // a series has a name and a numeric id
//! let id = c.series("tank.level_bar")?;
//!
//! // insert rows: timestamps in ms since 1970-01-01 UTC, and values
//! c.insert_series(id, &[1_700_000_000_000, 1_700_000_060_000], &[4.2, 4.3])?;
//!
//! // read them back, newest first
//! for (ts, v) in c.query_points(id, .., QueryOpts::desc())?.iter() {
//!     println!("{ts} {v}");
//! }
//!
//! // one bucket per day in Oslo, with count, sum, min, max
//! let days = Group::new(Unit::Day).timezone("Europe/Oslo");
//! for b in c.query_group(id, .., Some(&days), QueryOpts::asc())? {
//!     println!("{} {} {}", b.start, b.count, b.avg());
//! }
//! # Ok::<(), tsdb_client::Error>(())
//! ```
//!
//! # Basics
//!
//! - **Time** is `i64` milliseconds since the epoch. A time range is any Rust
//!   range of those: `t0..t1` includes `t0` and stops before `t1`; `..` is
//!   everything.
//! - **Value types.** A series has a [`ValueType`], fixed when it is created:
//!   float32 (what [`Client::series`] creates), float64, int64, bool or state
//!   (codes of names, see [`Client::state`]). Typed values travel as a
//!   [`Column`], whose validity [`Bitmap`] marks nulls
//!   ([`Client::insert_series_values`], [`Client::query_rows`]). Float32
//!   series also have a fast path with plain `f32` slices
//!   ([`Client::insert_series`], [`Client::query_points`]).
//! - **Last write wins.** Writing a timestamp again replaces its value.
//! - **Aggregates** ([`Client::query_agg`], [`Client::query_group`],
//!   [`Client::query_bounds`]) return [`Bucket`]s with count, sum, min and
//!   max; they skip nulls.
//! - **Time zones.** The server knows UTC only. For local days, weeks,
//!   months, quarters or years, the client computes the UTC instants where
//!   the periods begin ([`calendar_bounds`]) from the system's time zone
//!   files, and the server groups by those. [`Client::query_group`] does
//!   this for you.
//! - **Errors.** Every call returns [`Result`]. An error status from the
//!   server is [`Error::Server`] with a [`Status`]; [`Error::code`] gives the
//!   `TSDB_ERR_*` number. After an I/O or protocol error the client refuses
//!   further calls: connect again.
//! - **Speed.** Inserts are compressed with zstd for servers of protocol
//!   version 3 or later ([`Client::set_compression`]; level 1 by default, 0
//!   turns it off). Many small inserts go faster sent without waiting for
//!   each answer ([`Client::insert_series_async`], then [`Client::wait`]).
//!
//! The protocol is described in `include/tsdb_protocol.h` of the tsdb
//! repository; this crate mirrors the C client in `client/tsdb_client.h`.
//! Its dependencies are `zstd`, `rayon` (the thread pool of the batch calls: [`Pool`] and the
//! `*_many` calls of `Db`) and, on Unix, `libc` (for the TCP keepalive options); with the
//! `embedded` feature also `tsdb-sys`, the engine.
#![warn(missing_docs)]

mod batch;
mod calendar;
mod client;
mod database;
#[cfg(feature = "embedded")]
mod embedded;
mod error;
mod keepalive;
mod pack;
mod protocol;
mod types;
mod value;

pub use batch::{AggQuery, InsertBatch, Pool, RangeQuery, Target};
pub use calendar::calendar_bounds;
pub use client::{CONNECT_TIMEOUT, Client, DEFAULT_COMPRESSION};
pub use database::Database;
#[cfg(feature = "embedded")]
pub use embedded::{ChunkRef, Db, Options, PartInfo, RawValues, ValueCodec};
pub use error::{Error, Result, Status, strerror};
pub use keepalive::KeepAlive;
pub use protocol::{DEFAULT_PORT, PROTOCOL_VERSION};
pub use types::{Bucket, Group, Order, Points, QueryOpts, QueryStats, Rows, Stats, Unit};
pub use value::{Bitmap, Column, ColumnRef, NULL_F32_BITS, NULL_F64_BITS, Native, Value, ValueType, Values, ValuesRef};
