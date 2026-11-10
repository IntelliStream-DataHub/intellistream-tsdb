// SPDX-License-Identifier: AGPL-3.0-only
//! `tsdb_embedded`: the tsdb engine inside the Python process (`Db`), built with maturin. AGPL-3.0-only.
//!
//! The network client is the separate package `tsdb_client` (Apache-2.0). This extension raises that package's
//! exceptions (`tsdb_client.TsdbError` and its subclasses are imported, not redefined, so one `except` catches
//! errors of a `Client` and of a `Db`); its result classes (`Buckets`, `Stats`, ...) are its own types of the
//! same shape, named `tsdb_embedded.*`.

use pyo3::prelude::*;

use ::tsdb_client as tc;
use tsdb_client_python_common::add_result_classes;

mod calls;
mod embedded;

/// The tsdb database inside your Python process: Db, with the C engine built in.
///
/// Licence: AGPL-3.0-only (or a commercial licence from IntelliStream AS). The network client, tsdb_client, is a
/// separate package under Apache-2.0; Db has its methods, raises its exceptions (tsdb_client.TsdbError and its
/// subclasses) and returns result classes of the same shape, named tsdb_embedded.Buckets and so on.
///
/// Example:
///     >>> from tsdb_embedded import Db
///     >>> with Db("/tmp/tsdb-embedded") as db:
///     ...     s = db.series("pump7.flow")
///     ...     db.insert_series(s, [1_700_000_000_000], [4.5])
///     ...     db.query_raw(s, numpy=False)
///     ([1700000000000], [4.5])
#[pymodule]
fn tsdb_embedded(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<embedded::Db>()?;
    add_result_classes(m)?;
    m.add("PROTOCOL_VERSION", tc::PROTOCOL_VERSION)?;
    Ok(())
}
