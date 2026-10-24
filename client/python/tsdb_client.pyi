"""Client for tsdbd, the TCP server of the tsdb time series database.

Connect with Client, create series, insert rows and read them back, raw
or aggregated into buckets. Timestamps are int milliseconds since
1970-01-01 UTC. Time ranges are [start, end): start is included, end is
not, and None means no limit.

Example:
    >>> from tsdb_client import Client
    >>> with Client("127.0.0.1", 7070) as c:
    ...     s = c.series("pump7.flow")
    ...     c.insert_series(s, [1_700_000_000_000], [4.5])
    ...     c.query_raw(s, numpy=False)
    ([1700000000000], [4.5])

Errors are raised as TsdbError; its code is one of the ERR_* numbers.
"""

import os
import pathlib
from collections.abc import Sequence
from types import TracebackType
from typing import Any, Literal, final
from zoneinfo import ZoneInfo

from typing_extensions import Buffer

DEFAULT_PORT: int
"""The port tsdbd listens on by default (7070)."""
PROTOCOL_VERSION: int
"""The protocol version this client speaks."""
DEFAULT_COMPRESSION: int
"""The zstd level a new client compresses inserts with (1)."""
ERR_IO: int
ERR_NOMEM: int
ERR_CORRUPT: int
ERR_INVALID: int
ERR_LOCKED: int
ERR_NOTFOUND: int
ERR_TOO_MANY_BUCKETS: int
ERR_STOPPED: int
ERR_READONLY: int
VALUE_TYPES: list[str]
"""The value types: ["float32", "float64", "int64", "bool", "state"]."""

_Order = Literal["asc", "desc"]
_Unit = Literal["second", "minute", "hour", "day", "week", "month", "quarter", "year"]
# Columns: a numpy array or array.array of the right element type (copied
# without iterating), or any sequence of numbers.
_Timestamps = Buffer | Sequence[int]
_Values = Buffer | Sequence[float]
_SeriesIds = Buffer | Sequence[int]
_ValueType = Literal["float32", "float64", "int64", "bool", "state"]
# Typed values: a numpy array or masked array (masked = null), a buffer, or a
# sequence with None for nulls.
_TypedValues = Buffer | Sequence[float | int | bool | None]
_TimeZone = str | ZoneInfo

class TsdbError(Exception):
    """An error from tsdbd or the client; `code` is the TSDB_ERR_* number (negative)."""

    code: int
    """The TSDB_ERR_* number (negative), such as ERR_NOTFOUND."""

def strerror(code: int) -> str:
    """Returns the text for a TSDB_ERR_* code, such as ERR_NOTFOUND."""

def calendar_bounds(timezone: _TimeZone, unit: _Unit, start: int, end: int, count: int = 1) -> list[int]:
    """Returns the UTC times at which local calendar periods begin.

    These are the bucket starts query_group() uses for a time zone, and what
    query_bounds() takes. The result is the same as the C client's
    tsdb_calendar_bounds.

    Args:
        timezone: "UTC", a fixed offset such as "+02:00" or "-0530", an IANA
            name such as "Europe/Oslo" (read from /usr/share/zoneinfo, or
            $TZDIR), or a zoneinfo.ZoneInfo.
        unit: "second", "minute", "hour", "day", "week", "month", "quarter"
            or "year". Days and longer follow the local calendar; seconds to
            hours have a fixed length, aligned in the zone's standard time.
        start: Start of the range, ms since the epoch.
        end: End of the range (not included), ms since the epoch.
        count: Units per period, counted from 1970 in local time (0 counts
            as 1).

    Returns:
        The start (ms, UTC) of every period that overlaps [start, end),
        strictly increasing. A local midnight that happens twice begins its
        day the first time; one that is skipped begins it when the clock
        jumps.

    Raises:
        TsdbError: ERR_INVALID for an unknown time zone or more than 64 Mi
            periods.
        ValueError: Unknown unit.
    """

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
class Client:
    """One TCP connection to tsdbd.

    Use it as a context manager (``with Client(...) as c:``) or call close().
    Network calls release the GIL: threads can share one client (they take
    turns) or open one each.

    Args:
        host: Server name or address.
        port: Server port (7070 by default).
        timeout: Seconds to wait for the connection and for every later read
            or write; None (the default) waits forever. A call that times out
            raises TsdbError (ERR_IO) and closes the client.
        keepalive: Turn TCP keepalive on (the default): after 30 s of silence
            the client probes every 10 s and drops the connection after 3
            unanswered probes, where the platform allows.

    Raises:
        TsdbError: The connection failed.
        ValueError: timeout is not a positive number.
    """

    def __new__(
        cls, host: str = "127.0.0.1", port: int = 7070, timeout: float | None = None, keepalive: bool = True
    ) -> Client: ...
    def set_timeout(self, timeout: float | None) -> None:
        """Changes how long a call waits for the server, in seconds; None waits forever.

        The limit applies to each wait for the server (sending or receiving), not
        to a whole call. A call that times out raises TsdbError (ERR_IO) and closes
        the client: connect again.

        Raises:
            ValueError: timeout is not a positive number.
            TsdbError: The client is closed.
        """
    @property
    def timeout(self) -> float | None:
        """The timeout in seconds set at connect or by set_timeout; None waits forever.

        Raises:
            TsdbError: The client is closed.
        """
    def close(self) -> None:
        """Waits for pending inserts, then closes the connection.

        Closing twice is fine. Any later call raises TsdbError (ERR_IO).

        Raises:
            TsdbError: The first error among the pending inserts.
        """
    def __enter__(self) -> Client:
        """Returns the client itself, to use in a with statement."""
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """Closes the client at the end of a with block.

        If the block succeeded, this waits for pending inserts as close()
        does; if it raised, the connection is dropped at once.
        """
    def protocol_version(self) -> int:
        """Returns the protocol version the server speaks.

        Version 3 added compressed inserts, version 4 grouping in time zones
        by the client (query_bounds).
        """
    def series(self, name: str, create: bool = True) -> int:
        """Returns the id of a series, creating it (as float32) if needed.

        Args:
            name: The series name, such as "pump7.flow".
            create: If False, do not create the series.

        Raises:
            TsdbError: ERR_NOTFOUND if create is False and there is no such series.
        """
    def series_at(self, name: str, path: str) -> int:
        """Returns the id of a series, creating it (as float32) in a volume if needed.

        Args:
            name: The series name.
            path: A directory on the server for the new series (a volume);
                it only matters when the series is created.
        """
    def series_create(self, name: str, value_type: _ValueType = "float32", path: str | None = None) -> int:
        """Returns the id of a series of a given value type, creating it if needed.

        Args:
            name: The series name.
            value_type: "float32", "float64", "int64", "bool" or "state".
            path: A directory on the server for the new series (a volume);
                None means the database directory.

        Raises:
            TsdbError: ERR_INVALID if the series exists with another type.
            ValueError: Unknown value_type.
        """
    def series_type(self, id: int) -> _ValueType:
        """Returns the value type of a series: "float32", "float64", "int64", "bool" or "state".

        Raises:
            TsdbError: ERR_NOTFOUND if there is no such series.
        """
    def series_name(self, id: int) -> str:
        """Returns the name of a series.

        Raises:
            TsdbError: ERR_NOTFOUND if there is no such series.
        """
    def state(self, series: int, name: str) -> int:
        """Returns the code of a state name, creating it if needed.

        State series store small integer codes; the server keeps their names.
        Codes start at 1 and never change.

        Args:
            series: A series id; its volume holds the names.
            name: The state name, such as "running".
        """
    def state_lookup(self, series: int, name: str) -> int:
        """Returns the code of an existing state name.

        Raises:
            TsdbError: ERR_NOTFOUND if the name has no code.
        """
    def state_name(self, series: int, code: int) -> str:
        """Returns the name of a state code.

        Raises:
            TsdbError: ERR_NOTFOUND for an unknown code.
        """
    def state_names(self, series: int, codes: _TypedValues) -> list[str | None]:
        """Returns the names of many state codes.

        Args:
            series: A series id; its volume holds the names.
            codes: A list, array or masked array of codes, as query_values
                returns them. None and masked entries give None.

        Returns:
            A list of names. Names are cached in the client.
        """
    def insert_series(self, series: int, ts: _Timestamps, values: _Values) -> None:
        """Inserts float32 rows into one series and waits until the server has them.

        Writing a timestamp that already has a value replaces it: the last
        write wins.

        Args:
            series: The series id.
            ts: Timestamps, ms since the epoch: a list of ints or an int64 array.
            values: One float per timestamp: a list or a float32/float64 array.

        Raises:
            TsdbError: The server refused the rows (ERR_INVALID for a series
                that is not float32).
        """
    def insert(self, series: _SeriesIds, ts: _Timestamps, values: _Values) -> None:
        """Inserts float32 rows into many series and waits.

        Args:
            series: The series id of each row: a list of ints or a uint32 array.
            ts: The timestamp of each row, ms since the epoch.
            values: The value of each row.
        """
    def insert_series_async(self, series: int, ts: _Timestamps, values: _Values) -> None:
        """Sends float32 rows of one series without waiting for the answer.

        Faster for many small inserts. An error is raised by wait(), or by
        the next call that waits for the server.
        """
    def insert_async(self, series: _SeriesIds, ts: _Timestamps, values: _Values) -> None:
        """Sends float32 rows of many series without waiting; see insert_series_async()."""
    def insert_series_values(
        self, series: int, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Inserts values of any type, with nulls, into one series and waits.

        Args:
            series: The series id.
            ts: Timestamps, ms since the epoch.
            values: A list with None for nulls, a numpy array, or a numpy
                masked array (masked = null).
            value_type: The series' type. None asks the server once and
                remembers it.

        Raises:
            TsdbError: ERR_INVALID if value_type is not the series' type.
        """
    def insert_values(
        self, series: _SeriesIds, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Inserts values of any type into many series of that type and waits.

        As insert_series_values(), with a series id per row.
        """
    def insert_series_values_async(
        self, series: int, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Sends values of any type into one series without waiting; see insert_series_async()."""
    def insert_values_async(
        self, series: _SeriesIds, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Sends values of any type into many series without waiting; see insert_series_async()."""
    def wait(self) -> None:
        """Waits for the answers to all inserts sent without waiting.

        Raises:
            TsdbError: The first error among them.
        """
    @property
    def pending(self) -> int:
        """Number of inserts sent but not yet answered."""
    def set_compression(self, level: int) -> None:
        """Sets how much inserts are compressed (zstd level).

        Args:
            level: 0 turns compression off; 1 (the default) is fast, higher
                levels compress more, negative levels are faster still. Servers
                older than protocol version 3 always get uncompressed inserts.

        Raises:
            TsdbError: ERR_INVALID for a level zstd does not have.
        """
    @property
    def compression(self) -> int:
        """The zstd level of inserts (0: off)."""
    @property
    def bytes_sent(self) -> int:
        """Bytes this client has written to the network so far."""
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
        """Returns the raw rows of a float32 series.

        Args:
            series: The series id.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            order: "asc" (oldest first) or "desc" (newest first).
            limit: At most this many rows; None for all, 0 for none.
            numpy: True for numpy arrays, False for lists, None for numpy
                arrays if numpy is installed.

        Returns:
            (timestamps, values): int64 and float32 arrays, or lists of int and
            float. Nulls read as NaN.

        Raises:
            TsdbError: ERR_INVALID for a series that is not float32 (use
                query_values()).
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
        """The same as query_raw() (its name in the Rust and C clients)."""
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
        """Returns the raw rows of a series of any type, with nulls.

        Takes the same arguments as query_raw().

        Returns:
            (timestamps, values). With numpy: an int64 array and a
            numpy.ma.MaskedArray (masked = null) of float32, float64, int64,
            bool or uint32 (state codes). Without: two lists, None for nulls.
        """
    def query_agg(
        self, series: int, start: int | None = None, end: int | None = None, interval_ms: int = 0
    ) -> list[Bucket]:
        """Aggregates a series into buckets of a fixed length.

        Args:
            series: The series id.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            interval_ms: Bucket length in ms; buckets start at multiples of it
                since the epoch. 0 gives one bucket for the whole range.

        Returns:
            A list of Bucket, oldest first; empty buckets are left out.

        Raises:
            TsdbError: ERR_INVALID for a negative interval, ERR_TOO_MANY_BUCKETS
                if the range holds too many buckets.
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
    ) -> list[Bucket]:
        """Aggregates a series by calendar periods, such as days in a time zone.

        Days, weeks (from Monday), months, quarters and years follow the local
        calendar: a day across a daylight saving change lasts 23 or 25 hours.
        Seconds, minutes and hours have a fixed length. For a zone other than
        UTC the client computes where the local periods begin and sends those
        times (query_bounds()); a server older than protocol version 4 gets
        the zone itself.

        Args:
            series: The series id.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            unit: "second", "minute", "hour", "day", "week", "month",
                "quarter" or "year"; None gives one bucket for the whole range.
            count: Units per bucket, counted from 1970 (count=15 with
                unit="minute" gives quarter hours).
            timezone: "UTC" (or None), a fixed offset such as "+02:00", an
                IANA name such as "Europe/Oslo", or a zoneinfo.ZoneInfo.
            order: "asc" (oldest first) or "desc" (newest first).
            limit: At most this many buckets; None for all.

        Returns:
            A list of Bucket; empty buckets are left out. A bucket's start is
            the UTC time its local period begins.

        Raises:
            TsdbError: ERR_INVALID for an unknown time zone.
            ValueError: Unknown unit or order.
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
    ) -> list[Bucket]:
        """Aggregates a series into buckets that start at the given times.

        Bucket i holds the rows from bounds[i] up to bounds[i + 1]; the last
        one holds the rows up to end. Rows before bounds[0] are left out.
        calendar_bounds() computes such bounds for local days, weeks, months,
        quarters or years; query_group() does both steps. Needs a server of
        protocol version 4.

        Args:
            series: The series id.
            bounds: Bucket starts, ms since the epoch, strictly increasing: a
                list of ints or an int64 array.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            order: "asc" (oldest first) or "desc" (newest first).
            limit: At most this many buckets; None for all.

        Returns:
            A list of Bucket; empty buckets are left out.

        Raises:
            TsdbError: ERR_INVALID if bounds is empty or does not increase, or
                from a server older than version 4.
        """
    def delete_range(self, series: int, start: int | None, end: int | None) -> None:
        """Deletes the rows of a series with start <= ts < end (protocol version 7).

        A delete is a write like any other: a row inserted after it returned
        is kept. start and end are required; None is unbounded, so
        delete_range(id, None, None) deletes every row of the series.

        Raises:
            TsdbError: ERR_INVALID from a server older than version 7 (the
                client asks HELLO first and does not send the request).
        """
    def replace_range(
        self,
        series: int,
        start: int | None,
        end: int | None,
        ts: _Timestamps,
        values: _TypedValues,
        *,
        value_type: _ValueType | None = None,
    ) -> None:
        """Replaces the rows of a series with start <= ts < end by ts and values,
        atomically: a reader sees the old rows or the new ones, never a mix
        (protocol version 7).

        Args:
            series: The series id.
            start, end: The range; None is unbounded.
            ts: Timestamps, ms since the epoch, each in the range.
            values: As for insert_series_values(); no rows make it a delete.
            value_type: The series' type. None asks the server once and
                remembers it.

        Raises:
            TsdbError: ERR_INVALID for a timestamp outside the range, a
                value_type other than the series', rows that do not fit in
                one request (256 MiB), or a server older than version 7.
        """
    def set_retention(self, series: int, limit_ms: int) -> None:
        """Sets how long a series keeps its rows, in ms (protocol version 7):
        rows older than the server's clock minus limit_ms disappear from
        queries at once and from disk as merges get to them. 0 keeps them.

        Raises:
            TsdbError: ERR_INVALID for a negative limit or a server older
                than version 7.
        """
    def retention(self, series: int) -> int:
        """Returns the retention limit of a series in ms, 0 for none (protocol version 7)."""
    def commit(self) -> None:
        """Makes everything written so far durable on the server's disk."""
    def flush(self) -> None:
        """Commits, then writes the rows the server holds in memory to parts on disk."""
    def optimize(self) -> None:
        """Merges the parts of every partition into one (slow on big databases)."""
    def stats(self) -> Stats:
        """Returns database statistics (a Stats)."""
    def last_query_stats(self) -> QueryStats:
        """Returns what the server did for this connection's previous query (a QueryStats).

        Rows covered, decompressed or answered from block statistics, bytes
        and time. All zero before the first query.
        """


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
        query_threads: Threads for one query (default: every CPU).
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
        """Returns the number of series."""
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
        self, series: int, start: int | None = None, end: int | None = None, interval_ms: int = 0
    ) -> list[Bucket]:
        """Aggregates a series: one Bucket per interval_ms, or one for the whole
        range when interval_ms is 0. Empty buckets are left out.
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
    ) -> list[Bucket]:
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
    ) -> list[Bucket]:
        """Aggregates a series into buckets between ascending bounds (ms)."""
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
    def last_query_stats(self) -> QueryStats:
        """Returns what the last query through this Db read (all zero before the first)."""
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
