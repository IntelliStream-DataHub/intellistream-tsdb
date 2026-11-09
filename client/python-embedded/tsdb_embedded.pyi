# SPDX-License-Identifier: AGPL-3.0-only
"""The tsdb database inside your Python process: Db, with the C engine built in.

Licence: AGPL-3.0-only (or a commercial licence from IntelliStream AS). The network client, tsdb_client, is a
separate package under Apache-2.0; Db has its methods, raises its exceptions (tsdb_client.TsdbError and its
subclasses) and returns result classes of the same shape, named tsdb_embedded.Buckets and so on.

Example:
    >>> from tsdb_embedded import Db
    >>> with Db("/tmp/tsdb-embedded") as db:
    ...     s = db.series("pump7.flow")
    ...     db.insert_series(s, [1_700_000_000_000], [4.5])
    ...     db.query_raw(s, numpy=False)
    ([1700000000000], [4.5])
"""

import os
import pathlib
from collections.abc import Sequence
from types import TracebackType
from typing import Any, Literal, final, overload
from zoneinfo import ZoneInfo

from tsdb_client import TsdbError
from typing_extensions import Buffer

PROTOCOL_VERSION: int
"""The protocol version of the network client this Db mirrors."""


"""The value types: ["float32", "float64", "int64", "bool", "state"]."""

_Order = Literal["asc", "desc"]
_Unit = Literal["second", "minute", "hour", "day", "week", "month", "quarter", "year"]
_TwaMethod = Literal["step", "linear"]
# Columns: a numpy array or array.array of the right element type (copied
# without iterating), or any sequence of numbers.
_Timestamps = Buffer | Sequence[int]
_Values = Buffer | Sequence[float]
# Series ids are ints of 64 bits, handed out by the database: the volume in the high half, the series' index
# in the volume in the low half, so every id is at least 2**32 (an id cut to 32 bits is refused). They are not
# dense: look them up by name or list them with Db.series_ids(). An id column is a uint64 or int64 array (a
# negative id raises ValueError) or a sequence of ints; a 32-bit array (uint32, int32) raises TypeError, since it
# cannot hold an id.
_SeriesIds = Buffer | Sequence[int]
_ValueType = Literal["float32", "float64", "int64", "bool", "state"]
# Typed values: a numpy array or masked array (masked = null), a buffer, or a
# sequence with None for nulls.
_TypedValues = Buffer | Sequence[float | int | bool | None]
_TimeZone = str | ZoneInfo


@final
class Bucket:
    """One aggregate bucket, as returned by query_agg, query_group and query_bounds.

    Aggregates skip nulls. Buckets without rows are not returned; a bucket
    whose rows are all null is returned with count 0. Two buckets are equal
    when all their fields are (floats compared bit for bit).
    """

    @property
    def start(self) -> int:
        """Start of the bucket, ms since the epoch (UTC). For a single bucket
        over the whole range, the start of the range.
        """
    @property
    def count(self) -> int:
        """Number of known (not null) values in the bucket."""
    @property
    def sum(self) -> float:
        """Sum of the values, as a float. A NaN sum, min or max is always the quiet NaN
        0x7ff8000000000000, whatever NaNs were stored, so results compare bit for bit."""
    @property
    def min(self) -> float:
        """Smallest value (NaN if there is none)."""
    @property
    def max(self) -> float:
        """Largest value (NaN if there is none)."""
    @property
    def isum(self) -> int:
        """Exact sum for int64 and bool series (true counts 1); saturates at the
        int64 limits.
        """
    @property
    def imin(self) -> int:
        """Exact smallest value for int64 and bool series."""
    @property
    def imax(self) -> int:
        """Exact largest value for int64 and bool series."""
    @property
    def avg(self) -> float:
        """The mean, sum / count (0 for an empty bucket)."""
    @property
    def range(self) -> float:
        """max - min (spread queries only; AttributeError otherwise)."""
    @property
    def var_pop(self) -> float:
        """Population variance (spread queries only). NaN when count is 0."""
    @property
    def var_samp(self) -> float:
        """Sample variance (spread queries only). NaN when count is below 2."""
    @property
    def stddev_pop(self) -> float:
        """Population standard deviation (spread queries only)."""
    @property
    def stddev_samp(self) -> float:
        """Sample standard deviation (spread queries only)."""

@final
class Buckets(Sequence[Bucket]):
    """The buckets of an aggregate query, a column per field.

    It is a sequence of Bucket (a collections.abc.Sequence: len(), indexing,
    slicing, iteration, index(), count(), + and == with a list of Bucket), so
    code written for the list it replaces keeps working. Each field is also a
    column over all buckets, a numpy array (a list without numpy): start, sum,
    min, max, avg, isum, imin, imax, and counts for the counts (count() is the
    sequence method): ``b = db.query_agg(s, start, end, 60_000);
    b.counts.sum(); b.max.max()``. Reading a column copies it out of the
    result; keep it in a variable rather than reading it in a loop. The
    buckets of the spread queries (query_spread*()) have five more columns:
    range, var_pop, var_samp, stddev_pop and stddev_samp; on other buckets
    reading them raises AttributeError.
    """

    @property
    def start(self) -> Any:
        """Bucket starts, ms since the epoch (int64)."""
    @property
    def counts(self) -> Any:
        """Number of known values per bucket (uint64); count() is the sequence method."""
    @property
    def sum(self) -> Any:
        """Sums (float64)."""
    @property
    def min(self) -> Any:
        """Smallest values (float64)."""
    @property
    def max(self) -> Any:
        """Largest values (float64)."""
    @property
    def isum(self) -> Any:
        """Exact sums of int64 and bool series (int64)."""
    @property
    def imin(self) -> Any:
        """Exact minimums of int64 and bool series (int64)."""
    @property
    def imax(self) -> Any:
        """Exact maximums of int64 and bool series (int64)."""
    @property
    def avg(self) -> Any:
        """Means, sum / count (0 for a bucket of nulls only) (float64)."""
    @property
    def range(self) -> Any:
        """max - min (float64; spread queries only)."""
    @property
    def var_pop(self) -> Any:
        """Population variances (float64; spread queries only)."""
    @property
    def var_samp(self) -> Any:
        """Sample variances (float64; spread queries only)."""
    @property
    def stddev_pop(self) -> Any:
        """Population standard deviations (float64; spread queries only)."""
    @property
    def stddev_samp(self) -> Any:
        """Sample standard deviations (float64; spread queries only)."""
    def count(self, value: Any) -> int:
        """Number of buckets equal to `value` (the sequence method; the column is counts)."""
    def index(self, value: Any, start: int = 0, stop: int = ...) -> int:
        """Position of the first bucket equal to `value`; ValueError if there is none."""
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, i: int) -> Bucket: ...
    @overload
    def __getitem__(self, i: slice) -> Buckets: ...
    def __add__(self, other: list[Bucket] | Buckets) -> list[Bucket]: ...
    def __radd__(self, other: list[Bucket]) -> list[Bucket]: ...

@final
class BucketsMany(Sequence[Buckets]):
    """The buckets of many aggregate queries (query_agg_many()).

    result[i] is the Buckets of query i, and len(result) the number of
    queries. For whole-batch numpy work, result.buckets holds every query's
    buckets concatenated (columns start, count, sum, ...) and result.offsets
    (int64, one more than the queries) where each query's begin:
    ``b.buckets.counts[b.offsets[i]:b.offsets[i + 1]]`` are query i's counts.
    Without numpy the columns and offsets are lists. It is a
    collections.abc.Sequence of Buckets: a slice gives a list of them.
    """

    @property
    def buckets(self) -> Buckets:
        """Every query's buckets, concatenated in query order."""
    @property
    def offsets(self) -> Any:
        """Where each query's buckets begin in buckets (int64, queries + 1 entries)."""
    def count(self, value: Any) -> int:
        """Number of queries whose buckets equal `value` (the sequence method)."""
    def index(self, value: Any, start: int = 0, stop: int = ...) -> int:
        """Position of the first query whose buckets equal `value`; ValueError if there is none."""
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, i: int) -> Buckets: ...
    @overload
    def __getitem__(self, i: slice) -> list[Buckets]: ...

@final
class Many(Sequence[Any]):
    """The answer of a multi-series call (query_*_multi()).

    len(r) is the number of series, r.status their statuses (int32, 0 for
    success, else the TSDB_ERR_* code) and r.offsets (int64, one more than
    the series) where each series' rows or buckets begin in the columns,
    which hold every series' results concatenated in series order. r[i] is
    series i's result as the single call returns it, and raises TsdbError
    for a series that failed. Columns: r.ts and r.values for the values and
    LTTB calls; r.buckets (a Buckets or TwaBuckets) for the bucket calls,
    whose columns read straight from r too (r.counts, r.sum, r.twa, ...).
    Without numpy the columns are lists.
    """

    @property
    def kind(self) -> Literal["values", "buckets", "spread", "twa", "lttb"]:
        """The call that answered: "values", "buckets", "spread", "twa" or "lttb"."""
    @property
    def series(self) -> list[int]:
        """The series ids, in the order asked."""
    @property
    def status(self) -> Any:
        """Each series' status: 0, or the TSDB_ERR_* code it failed with (int32)."""
    @property
    def offsets(self) -> Any:
        """Where each series' rows or buckets begin in the columns (int64, series + 1 entries)."""
    @property
    def ok(self) -> bool:
        """True when every series succeeded."""
    def error_of(self, i: int) -> TsdbError | None:
        """The TsdbError of series i, or None if it succeeded."""
    @property
    def ts(self) -> Any:
        """Every series' timestamps, concatenated (the values and LTTB calls)."""
    @property
    def values(self) -> Any:
        """Every series' values, concatenated; TypeError when the series have different types (use r[i])."""
    @property
    def buckets(self) -> Buckets | TwaBuckets:
        """Every series' buckets, concatenated: a Buckets (TwaBuckets for query_twa_multi())."""
    def __getattr__(self, name: str) -> Any: ...
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, i: int) -> Any: ...
    @overload
    def __getitem__(self, i: slice) -> list[Any]: ...

@final
class LatestMany(Sequence[Any]):
    """The answers of one latest call, in the order asked. `r[i]` is `(ts, value)` for series i's newest value,
    `(ts, None)` for a null (with_nulls=True), `None` when the series has no row, and raises TsdbError when
    its status is not 0 (NOTFOUND or DENIED).
    """
    @property
    def series(self) -> list[int]:
        """The series ids, in the order asked (empty for latest_by_name())."""
    @property
    def names(self) -> list[str] | None:
        """The names asked (latest_by_name()), else None."""
    @property
    def status(self) -> Any:
        """Each answer's status: 0, or a TSDB_ERR_* code (NOTFOUND, DENIED, INVALID)."""
    @property
    def kind(self) -> Any:
        """Each answer's kind: 0 a value, 1 a null (with_nulls=True), 2 no row (or a status)."""
    @property
    def ts(self) -> Any:
        """Each row's timestamp (ms); 0 where kind is 2."""
    @property
    def values(self) -> list[Any]:
        """Each value as a Python object (float, int or bool; a state is its code); None where kind is not 0."""
    @property
    def ok(self) -> bool:
        """True when every series was answered (no NOTFOUND or DENIED)."""
    def error_of(self, i: int) -> TsdbError | None:
        """The error of series i (a TsdbError, not raised), or None when it was answered."""
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, i: int) -> tuple[int, Any] | None: ...
    @overload
    def __getitem__(self, i: slice) -> list[tuple[int, Any] | None]: ...

@final
class TwaBucket:
    """One bucket of a time-weighted average (query_twa*()).

    The value is weighted by how long it held: area is its integral over
    time (value x ms), covered_ms the time the segments cover, and twa
    area / covered_ms (NaN when nothing is covered). count is the number of
    known readings in the bucket. Equal when every field is (floats bit for bit).
    """

    @property
    def start(self) -> int:
        """Start of the bucket, ms since the epoch (UTC); for a single bucket over
        the whole range, the start of the range.
        """
    @property
    def twa(self) -> float:
        """The time-weighted average, area / covered_ms (NaN when nothing is covered)."""
    @property
    def area(self) -> float:
        """The integral of the value over time, in value x ms."""
    @property
    def covered_ms(self) -> int:
        """The time covered by segments, in ms."""
    @property
    def count(self) -> int:
        """Known readings in the bucket (and in the query's range)."""

@final
class TwaBuckets(Sequence[TwaBucket]):
    """The buckets of a time-weighted average query, a column per field.

    A sequence of TwaBucket (len(), indexing, slicing, iteration, == with
    another TwaBuckets or a list of TwaBucket), and a numpy array per field
    over all buckets (a list without numpy): start, twa, area, covered_ms
    and counts (count() is the sequence method).
    """

    @property
    def start(self) -> Any:
        """Bucket starts, ms since the epoch (int64)."""
    @property
    def twa(self) -> Any:
        """Time-weighted averages (float64; NaN where nothing is covered)."""
    @property
    def area(self) -> Any:
        """Integrals of the value over time, value x ms (float64)."""
    @property
    def covered_ms(self) -> Any:
        """Time covered by segments, ms (int64)."""
    @property
    def counts(self) -> Any:
        """Known readings per bucket (uint64)."""
    def count(self, value: Any) -> int:
        """Number of buckets equal to `value` (the sequence method; the column is counts)."""
    def index(self, value: Any, start: int = 0, stop: int = ...) -> int:
        """Position of the first bucket equal to `value`; ValueError if there is none."""
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, i: int) -> TwaBucket: ...
    @overload
    def __getitem__(self, i: slice) -> TwaBuckets: ...

@final
class Stats:
    """Database statistics, as returned by Client.stats()."""

    @property
    def parts(self) -> int:
        """Parts (files of sorted rows) on disk."""
    @property
    def partitions(self) -> int:
        """Partitions (time ranges, one day each by default)."""
    @property
    def rows(self) -> int:
        """Rows stored in parts."""
    @property
    def bytes(self) -> int:
        """Bytes of the parts on disk."""
    @property
    def merges(self) -> int:
        """Merges of parts done since the server started."""
    @property
    def merged_rows(self) -> int:
        """Rows written by those merges."""
    @property
    def passthrough_granules(self) -> int:
        """Blocks of rows that merges copied without decompressing them."""
    @property
    def memtable_rows(self) -> int:
        """Rows held in memory (and in the write-ahead log), not yet in parts."""

@final
class QueryStats:
    """What the server did for this connection's previous query, as returned by
    Client.last_query_stats().

    The server stores rows in blocks ("granules") of up to 8192 rows, each
    with its count, sum, min and max. An aggregate over whole blocks reads
    those numbers instead of decompressing the rows.
    """

    @property
    def elapsed_s(self) -> float:
        """Seconds the server spent on the query."""
    @property
    def rows(self) -> int:
        """Rows covered: rows_decoded + rows_from_stats + memtable_rows."""
    @property
    def rows_decoded(self) -> int:
        """Rows decompressed (whole granules)."""
    @property
    def rows_from_stats(self) -> int:
        """Rows answered from granule statistics, without decompressing them."""
    @property
    def memtable_rows(self) -> int:
        """Rows read from memory (not yet in parts)."""
    @property
    def bytes(self) -> int:
        """Uncompressed size of the rows covered (timestamp and value)."""
    @property
    def bytes_read(self) -> int:
        """Compressed bytes of the granules decompressed."""
    @property
    def parts(self) -> int:
        """Parts with rows of the series in the range."""
    @property
    def granules(self) -> int:
        """Granules of those parts in the range."""
    @property
    def granules_from_stats(self) -> int:
        """Granules answered from their statistics."""
    @property
    def result_rows(self) -> int:
        """Rows, or buckets, returned."""

@final
class Db:
    """A tsdb database opened in this process: no server, no network.

    The methods are those of Client, so code written for one runs with the
    other. Use it as a context manager (``with Db(path) as db:``) or call
    close(); one process at a time may open a directory for writing.
    The engine works with the GIL released, and threads sharing one Db
    run in parallel.

    Args:
        path: The database directory; created if missing (its parent must exist).
        read_only: Open without writing, merging or locking exclusively.
        follow: Read only, and keep following a writer of the same directory.
        fsync: fsync parts before publishing them (default True).
        wal_sync_ms: 0: every small write waits for the WAL fsync (the
            default); N > 0: the WAL is synced every N ms; -1: no WAL.
        merge_threads: Background merge threads; 0 disables merges.
        query_threads: Query threads of the database, shared by all queries
            (default: every CPU).
        request_threads: Threads one query or one multi-series call uses at
            most, its caller included (default: query_threads).
        partition_classes: Slow series get partitions of 7, 28 or 364 base
            partitions, chosen from their rate (default True; fixed when the
            database is created).
        insert_zstd_level: zstd level of the parts inserts write (default 0:
            none; merges compress them at zstd_level); -1: zstd_level.
        stats, stats_sweep_mb_s: statistics of what is stored (-1 off, 0 lazy,
            1 from the open; the sweeper's MB/s cap), as tsdb_options.
        latest_cache: bytes of memory for the latest tables of the process
            (default RAM / 16; 0: no table, the same answers, slower).
        The other keywords are the engine's tsdb_options fields of the same
        name; None keeps the engine's default. partition_ms and granule_rows
        are fixed when the database is created. value_codec is "auto",
        "gorilla" or "chimp".

    Raises:
        TsdbError: ERR_LOCKED if another process has the directory open for
            writing; ERR_IO if it cannot be created.
        ValueError: Unknown value_codec.
    """

    def __new__(
        cls,
        path: str | os.PathLike[str],
        *,
        read_only: bool = False,
        follow: bool = False,
        fsync: bool | None = None,
        wal_sync_ms: int | None = None,
        merge_threads: int | None = None,
        query_threads: int | None = None,
        partition_ms: int | None = None,
        granule_rows: int | None = None,
        zstd_level: int | None = None,
        value_codec: Literal["auto", "gorilla", "chimp"] | None = None,
        buffer_rows: int | None = None,
        flush_interval_ms: int | None = None,
        wal_max_batch_rows: int | None = None,
        max_part_bytes: int | None = None,
        refresh_ms: int | None = None,
        merge_idle_s: int | None = None,
        obsolete_part_lifetime_s: int | None = None,
        verify_checksums: bool | None = None,
        retention_merge_s: int | None = None,
        request_threads: int | None = None,
        partition_classes: bool | None = None,
        pin_index: bool | None = None,
        insert_zstd_level: int | None = None,
        stats: int | None = None,
        stats_sweep_mb_s: int | None = None,
        latest_cache: int | None = None,
    ) -> Db: ...
    def close(self) -> None:
        """Flushes the memtable and closes the database.

        Waits for calls other threads are making on it. Closing twice is
        fine; any later call raises TsdbError (ERR_IO).
        """
    def __enter__(self) -> Db:
        """Returns the database itself, to use in a with statement."""
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """Closes the database at the end of a with block."""
    @staticmethod
    def simd_level() -> str:
        """Returns the SIMD kernels the engine picked for this CPU, such as "avx2"."""
    def series(self, name: str, create: bool = True) -> int:
        """Returns the id of a series, creating it (as float32) if needed.

        Raises:
            TsdbError: ERR_NOTFOUND if create is False and there is no such series.
        """
    def series_at(self, name: str, path: str) -> int:
        """Returns the id of a series, creating it (as float32) in a volume if needed."""
    def series_create(self, name: str, value_type: _ValueType = "float32", path: str | None = None) -> int:
        """Returns the id of a series of a given value type, creating it if needed.

        Raises:
            TsdbError: ERR_INVALID if the series exists with another type.
            ValueError: Unknown value_type.
        """
    def series_type(self, id: int) -> _ValueType:
        """Returns the value type of a series: "float32", "float64", "int64", "bool" or "state"."""
    def series_name(self, id: int) -> str:
        """Returns the name of a series."""
    def series_path(self, id: int) -> str:
        """Returns the volume (directory) a series is stored in."""
    def series_count(self) -> int:
        """Returns the number of series. Ids are not 0 .. series_count() - 1: list them with series_ids()."""
    def series_ids(self) -> list[int]:
        """Returns the ids of every series: those of one volume in creation order, the volumes one after another."""
    def volumes(self) -> list[str]:
        """Returns the directories of the volumes, the database directory first."""
    def state(self, series: int, name: str) -> int:
        """Returns the code of a state name, creating it if needed."""
    def state_lookup(self, series: int, name: str) -> int:
        """Returns the code of an existing state name (ERR_NOTFOUND if it has none)."""
    def state_name(self, series: int, code: int) -> str:
        """Returns the name of a state code (ERR_NOTFOUND for an unknown code)."""
    def state_names(self, series: int, codes: _TypedValues) -> list[str | None]:
        """Returns the names of many state codes; None and masked entries give None."""
    def insert_series(self, series: int, ts: _Timestamps, values: _Values) -> None:
        """Inserts float32 rows into one series. The last write wins.

        Args:
            series: The series id.
            ts: Timestamps, ms since the epoch: a list of ints or an int64 array.
            values: One float per timestamp: a list or a float32/float64 array.
        """
    def insert(self, series: _SeriesIds, ts: _Timestamps, values: _Values) -> None:
        """Inserts float32 rows into many series, all or nothing."""
    def insert_series_async(self, series: int, ts: _Timestamps, values: _Values) -> None:
        """The same as insert_series(): an embedded insert has no round trip to hide."""
    def insert_async(self, series: _SeriesIds, ts: _Timestamps, values: _Values) -> None:
        """The same as insert()."""
    def insert_series_values(
        self, series: int, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Inserts values of any type, with nulls, into one series.

        Args:
            series: The series id.
            ts: Timestamps, ms since the epoch.
            values: A list with None for nulls, a numpy array, or a numpy
                masked array (masked = null).
            value_type: The series' type; None looks it up.
        """
    def insert_values(
        self, series: _SeriesIds, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Inserts values of any type into many series of that type, all or nothing."""
    def insert_series_values_async(
        self, series: int, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """The same as insert_series_values()."""
    def insert_values_async(
        self, series: _SeriesIds, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """The same as insert_values()."""
    def wait(self) -> None:
        """Nothing to wait for: embedded inserts are done when they return."""
    def query_raw(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
        numpy: bool | None = None,
    ) -> tuple[Any, Any]:
        """Returns the float32 rows of a series as (timestamps, values).

        Args:
            start, end: The half-open range [start, end) in ms; None is unbounded.
            order: "asc" or "desc".
            limit: At most this many rows; None is all.
            numpy: True for numpy arrays, False for lists, None for numpy if
                it can be imported.
        """
    def query_points(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
        numpy: bool | None = None,
    ) -> tuple[Any, Any]:
        """The same as query_raw()."""
    def query_values(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
        numpy: bool | None = None,
    ) -> tuple[Any, Any]:
        """Returns the rows of a series of any type as (timestamps, values).

        values is a numpy masked array (masked = null) of the series' type,
        or a list with None for nulls; the arguments are those of query_raw().
        """
    def query_agg(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        interval_ms: int = 0,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """Aggregates a series: one bucket per interval_ms, or one for the whole
        range when interval_ms is 0, as Buckets (a sequence of Bucket with
        numpy columns), oldest first or newest first with order="desc", at
        most limit of them. Empty buckets are left out.
        """
    def query_group(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: _TimeZone | None = None,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """Aggregates a series by calendar units, as Client.query_group() does.

        Args:
            unit: "second", "minute", "hour", "day", "week", "month",
                "quarter" or "year"; None is one bucket for the whole range.
            count: Units per bucket.
            timezone: A zone name or tzinfo; None is UTC.
        """
    def query_bounds(
        self,
        series: int,
        bounds: _Timestamps,
        start: int | None = None,
        end: int | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """Aggregates a series into buckets between ascending bounds (ms)."""
    def query_spread(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        interval_ms: int = 0,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """Count, sum, min and max like query_agg(), and the spread of the values
        in each bucket: range (max - min), population and sample variance and
        standard deviation.

        Takes the arguments of query_agg(). The variance is computed from the
        granule statistics where whole granules fall in a bucket, so it costs
        about what query_agg() does.

        Returns:
            Buckets with the columns range, var_pop, var_samp, stddev_pop and
            stddev_samp beside the usual ones (and Bucket objects with those
            fields). Variances are NaN for a bucket of nulls only, the sample
            ones also for a single value.

        Raises:
            TsdbError: ERR_INVALID for a negative interval.
        """
    def query_spread_group(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: _TimeZone | None = None,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """The spread by calendar periods: query_group() with the columns of
        query_spread(). Takes the arguments of query_group().
        """
    def query_spread_bounds(
        self,
        series: int,
        bounds: _Timestamps,
        start: int | None = None,
        end: int | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """The spread in buckets that start at the given times: query_bounds()
        with the columns of query_spread(). Takes the arguments of query_bounds().
        """
    def query_twa(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        interval_ms: int = 0,
        *,
        method: _TwaMethod = "step",
        max_gap_ms: int = 0,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> TwaBuckets:
        """Time-weighted averages: each reading weighted by how long it held.

        A reading made once an hour counts as much as an hour of readings made
        every second. Use it for values sampled irregularly or on change.

        Args:
            series: The series id.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            interval_ms: Bucket length in ms; 0 gives one bucket for the range.
            method: "step" (a value holds until the next reading) or "linear"
                (a straight line between readings).
            max_gap_ms: A gap between readings longer than this covers nothing
                (the value is stale there); 0 for no limit.
            order: "asc" (oldest first) or "desc" (newest first).
            limit: At most this many buckets; None for all, 0 for none.

        Returns:
            TwaBuckets: a sequence of TwaBucket with numpy columns start,
            twa, area, covered_ms and counts.

        Raises:
            TsdbError: ERR_INVALID for a negative interval.
            ValueError: Unknown method or order.
        """
    def query_twa_group(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: _TimeZone | None = None,
        method: _TwaMethod = "step",
        max_gap_ms: int = 0,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> TwaBuckets:
        """Time-weighted averages by calendar periods (days in a time zone, ...).
        Takes the arguments of query_group() and method and max_gap_ms of query_twa().
        """
    def query_twa_bounds(
        self,
        series: int,
        bounds: _Timestamps,
        start: int | None = None,
        end: int | None = None,
        *,
        method: _TwaMethod = "step",
        max_gap_ms: int = 0,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> TwaBuckets:
        """Time-weighted averages in buckets that start at the given times.
        Takes the arguments of query_bounds() and method and max_gap_ms of query_twa().
        """
    def query_lttb(
        self,
        series: int,
        threshold: int,
        start: int | None = None,
        end: int | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
        numpy: bool | None = None,
    ) -> tuple[Any, Any]:
        """Downsamples a series for a chart: at most threshold rows that keep the
        shape of the line (Largest-Triangle-Three-Buckets).

        The first and last rows are always kept; a range of threshold rows or
        fewer comes back whole.

        Args:
            series: The series id.
            threshold: How many rows to keep at most, at least 3.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            order: "asc" (oldest first) or "desc" (newest first): the same
                rows, in that order.
            limit: At most this many of the chosen rows; None for all.
            numpy: As for query_values().

        Returns:
            (timestamps, values), as query_values() returns them.
        """
    def delete_range(self, series: int, start: int | None, end: int | None) -> None:
        """Deletes the rows of a series with start <= ts < end; None is unbounded.
        A row inserted after the call returned is kept.
        """
    def replace_range(
        self,
        series: int,
        start: int | None,
        end: int | None,
        ts: _Timestamps,
        values: Any,
        *,
        value_type: _ValueType | None = None,
    ) -> None:
        """Replaces the rows of a series with start <= ts < end by ts and values,
        atomically, as Client.replace_range() does. No rows make it a delete.

        Raises:
            TsdbError: ERR_INVALID for a timestamp outside the range or a
                value_type other than the series'.
        """
    def set_retention(self, series: int, limit_ms: int) -> None:
        """Sets how long a series keeps its rows, in ms; 0 keeps them for ever.
        Rows older than the clock minus limit_ms disappear from queries at
        once and from disk as merges get to them.
        """
    def retention(self, series: int) -> int:
        """Returns the retention limit of a series in ms, 0 for none."""
    def query_raw_many(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> list[tuple[Any, Any]]:
        """Runs many raw float32 queries at once, in parallel on every CPU.

        One call answers a query per entry of series, with the GIL released
        for all of them: a single Python thread uses every core, and the
        interpreter is crossed once per call rather than once per query.

        Args:
            series: Series ids: a list or a numpy array (uint64 or int64), one per query.
            start, end: None (unbounded), one timestamp for every query, or a
                list or int64 array with one per query; [start, end) in ms.
            order: "asc" or "desc".
            limit: At most this many rows per query; None is all.

        Returns:
            A list with (timestamps, values) of each query, in query order: an
            int64 and a float32 numpy array, as query_raw() returns them.
            Needs numpy (ImportError without it).

        Afterwards last_query_stats() holds the totals of the batch's queries,
        with the whole batch's time as elapsed_s; so does query_agg_many().

        Raises:
            TsdbError: The lowest failing query's error; its index attribute is
                that query's position, and every query below it succeeded.
        """
    def query_agg_many(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        interval_ms: int | _Timestamps | None = None,
    ) -> BucketsMany:
        """Runs many aggregate queries at once, in parallel on every CPU.

        Args:
            series: Series ids, one per query (a list, or a uint64 or int64 array).
            start, end: As in query_raw_many().
            interval_ms: Bucket length in ms: one for every query, or a list or
                int64 array of one per query; 0 (the default) is one bucket for
                the query's range.

        Returns:
            BucketsMany: result[i] is the Buckets of query i; result.buckets
            holds every bucket (numpy columns) and result.offsets where each
            query's begin. Without numpy, columns and offsets are lists.

        Raises:
            TsdbError: The lowest failing query's error; its index attribute is
                that query's position, and every query below it succeeded.
        """
    def insert_many(
        self, batches: Sequence[tuple[int | _SeriesIds, _Timestamps, _TypedValues]], *, value_type: _ValueType | None = None
    ) -> None:
        """Runs many inserts at once, in parallel on every CPU.

        Each insert is all or nothing on its own. Inserts that share a series
        run one after another in list order, so where two write the same row,
        the later one in the list wins, as if they ran one at a time.

        Args:
            batches: A list of (series, ts, values) tuples: series is one id or
                a column of ids (one per row), ts int64 timestamps, values one
                per timestamp. numpy arrays are copied once, without the GIL.
            value_type: The type of every batch's values; None takes each
                batch's from its series (the first row's for a column of ids),
                as insert_series_values() does, converting the values to it
                (float64 into a float32 series, a list into an int64 series).

        Raises:
            TsdbError: The lowest failing insert's error; its index attribute is
                that insert's position. Every insert below it was committed;
                later ones may have been. Errors found before anything is
                inserted name their entry too ("batch entry 3: ...", index 3),
                and then nothing was: arguments that do not convert (TypeError,
                ValueError), and without value_type a series that does not
                exist (its type is looked up first).
        """
    def commit(self) -> None:
        """Makes every insert so far durable (syncs the WAL)."""
    def flush(self) -> None:
        """Writes the memtable into parts."""
    def optimize(self) -> None:
        """Flushes, then merges every partition down to as few parts as it can."""
    def drop_before(self, ts: int) -> None:
        """Drops whole partitions that end at or before ts."""
    def refresh(self) -> None:
        """A read-only database: picks up what the writer published since the last refresh."""
    def stats(self) -> Stats:
        """Returns the storage statistics."""
    def latest(
        self,
        series: _SeriesIds,
        end: int | _Timestamps | None = None,
        *,
        with_nulls: bool = False,
        threads: int = 0,
        errors: Literal["raise", "status"] = "raise",
        numpy: bool | None = None,
    ) -> LatestMany:
        """As Client.latest(), in this process."""

    def latest_by_name(
        self,
        names: Sequence[str],
        end: int | _Timestamps | None = None,
        *,
        with_nulls: bool = False,
        threads: int = 0,
        errors: Literal["raise", "status"] = "raise",
        numpy: bool | None = None,
    ) -> LatestMany:
        """As Client.latest_by_name(), in this process."""
    def query_values_multi(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
        threads: int = 0,
        max_rows: int = 0,
        errors: Literal["raise", "status"] = "raise",
        numpy: bool | None = None,
    ) -> Many:
        """As Client.query_values_multi(), in this process."""
    def query_buckets_multi(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        interval_ms: int | _Timestamps | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: _TimeZone | None = None,
        bounds: _Timestamps | None = None,
        order: _Order = "asc",
        limit: int | None = None,
        threads: int = 0,
        max_rows: int = 0,
        errors: Literal["raise", "status"] = "raise",
    ) -> Many:
        """As Client.query_buckets_multi(), in this process."""
    def query_spread_multi(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        interval_ms: int | _Timestamps | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: _TimeZone | None = None,
        bounds: _Timestamps | None = None,
        order: _Order = "asc",
        limit: int | None = None,
        threads: int = 0,
        max_rows: int = 0,
        errors: Literal["raise", "status"] = "raise",
    ) -> Many:
        """As Client.query_spread_multi(), in this process."""
    def query_twa_multi(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        interval_ms: int | _Timestamps | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: _TimeZone | None = None,
        bounds: _Timestamps | None = None,
        method: _TwaMethod = "step",
        max_gap_ms: int = 0,
        order: _Order = "asc",
        limit: int | None = None,
        threads: int = 0,
        max_rows: int = 0,
        errors: Literal["raise", "status"] = "raise",
    ) -> Many:
        """As Client.query_twa_multi(), in this process."""
    def query_lttb_multi(
        self,
        series: _SeriesIds,
        threshold: int,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
        threads: int = 0,
        max_rows: int = 0,
        errors: Literal["raise", "status"] = "raise",
        numpy: bool | None = None,
    ) -> Many:
        """As Client.query_lttb_multi(), in this process."""
    def last_query_stats(self) -> QueryStats:
        """Returns what the last query through this Db read (all zero before the first).

        After a batch call (query_raw_many, query_agg_many) the totals of its
        queries, with the whole batch's time as elapsed_s. With several
        threads querying, the query (or batch) that finished last.
        """
    def failed(self) -> str | None:
        """None while healthy; after a storage error that leaves it unknown what
        reached the disk, what failed (writes then fail until reopened).
        """
    @property
    def path(self) -> pathlib.Path:
        """The database directory."""
    @property
    def pending(self) -> int:
        """Always 0: embedded inserts are done when they return."""
