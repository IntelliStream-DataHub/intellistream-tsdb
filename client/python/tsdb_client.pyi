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
from typing import Any, Literal, final, overload
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
ERR_DENIED: int
"""The user may not do this (no grant covers the series, or not its admin)."""
ERR_AUTH: int
"""The login failed or is required (an unknown user and a wrong password look alike)."""
RIGHT_READ: int
"""Reading the series' rows, aggregates and metadata (1)."""
RIGHT_INSERT: int
"""Inserting rows (2)."""
RIGHT_DELETE: int
"""Deleting and replacing rows (4)."""
RIGHT_ADMIN: int
"""Managing users and grants inside the selector (8)."""
RIGHT_WRITE: int
"""RIGHT_INSERT | RIGHT_DELETE (6)."""
SCRAM_ITERATIONS: int
"""The PBKDF2 iterations of a new verifier when none are given (4096)."""
PASSWORD_MAX: int
"""The longest password in bytes (1023); a password is at least 1 byte, without NUL, in every client and the CLI."""
VALUE_TYPES: list[str]
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

class TsdbError(Exception):
    """An error from tsdbd or the client; `code` is the TSDB_ERR_* number (negative)."""

    code: int
    """The TSDB_ERR_* number (negative), such as ERR_NOTFOUND."""

    index: int | None
    """For an error of a batch call (*_many), the failing entry's position in
    the batch: the lowest one that failed; every entry below it ran and
    succeeded. For a series of a multi-series call (query_*_multi), that
    series' position in the list. None for other calls."""

    result: Many | None
    """For a series of a multi-series call that failed with errors="raise",
    the whole answer (the other series' results and statuses). None for
    other calls."""

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
class User:
    """A user of the server, from whoami() or user_list()."""

    @property
    def name(self) -> str:
        """The login name; "" from a server without login (--no-auth)."""
    @property
    def superuser(self) -> bool:
        """Every right on every series, user management and the database-wide operations."""
    @property
    def grants(self) -> list[tuple[str, str]]:
        """The grants as (selector, rights) pairs of strings, e.g. ("prefix:acme/", "read,insert"). A user's rights
        on a series are the union of those whose selector matches its name. The rights read back in grant(); a
        selector of a kind this client does not know (a later server's) shows as "kind#N:TEXT" and does not."""

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
            or write; None (the default) gives up on the connect after 10 s
            and lets every later call wait forever. A call that times out
            raises TsdbError (ERR_IO) and closes the client.
        keepalive: Turn TCP keepalive on (the default): after 30 s of silence
            the client probes every 10 s and drops the connection after 3
            unanswered probes, where the platform allows.
        user: The user to log in as (SCRAM-SHA-256), when the server asks
            for a login. The password never travels, and the connect fails
            unless the server proves it holds the user's verifier. PBKDF2
            runs once per process for a user and password: later connects,
            a Pool's included, reuse the keys.
        password: The user's password (its UTF-8 bytes, without
            normalization; ASCII is safest): 1 to PASSWORD_MAX (1023) bytes
            without NUL, in every client and the CLI.
        password_file: Read the password from the first line of this file
            instead, without the LF or CR LF that ends it, at every login.
        require_auth: Refuse (ERR_AUTH) a server that does not ask for a
            login (--no-auth). Otherwise such a server is used without one.

    Raises:
        TsdbError: The connection failed; ERR_AUTH when the login failed (an
            unknown user and a wrong password look alike), the server did not
            prove its signature, it asks for a login and none was given, or
            require_auth is set and it asks for none; ERR_INVALID, naming
            the file, when password_file cannot be read or its first line is
            not a password (a mistake to fix, not a network error to retry).
        ValueError: timeout is not a positive number, a user has neither
            password nor password_file, or password breaks the rule above.
    """

    def __new__(
        cls,
        host: str = "127.0.0.1",
        port: int = 7070,
        timeout: float | None = None,
        keepalive: bool = True,
        *,
        user: str | None = None,
        password: str | None = None,
        password_file: str | os.PathLike[str] | None = None,
        require_auth: bool = False,
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

        The id is a 64-bit int that names the series' volume: keep it as it is (do not store it in 32 bits)
        and do not count on ids being 0, 1, 2 and so on.

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
            path: A volume on the server for the new series, one the operator
                added (`tsdb <dir> volume add`; else ERR_NOTFOUND); it only
                matters when the series is created.
        """
    def series_create(self, name: str, value_type: _ValueType = "float32", path: str | None = None) -> int:
        """Returns the id of a series of a given value type, creating it if needed.

        Args:
            name: The series name.
            value_type: "float32", "float64", "int64", "bool" or "state".
            path: A volume on the server for the new series, one the operator
                added (`tsdb <dir> volume add`; else ERR_NOTFOUND); None
                means the database directory.

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
            series: The series id of each row: a list of ints, or a uint64 or int64 array.
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
                levels compress more, negative levels are faster still.

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
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        interval_ms: int = 0,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> Buckets:
        """Aggregates a series into buckets of a fixed length.

        Args:
            series: The series id.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            interval_ms: Bucket length in ms; buckets start at multiples of it
                since the epoch. 0 gives one bucket for the whole range.
            order: "asc" (oldest first) or "desc" (newest first).
            limit: At most this many buckets; None for all, 0 for none.

        Returns:
            Buckets: a sequence of Bucket with a numpy array per field
            (b.counts, b.sum, ...); empty buckets are left out.

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
    ) -> Buckets:
        """Aggregates a series by calendar periods, such as days in a time zone.

        Days, weeks (from Monday), months, quarters and years follow the local
        calendar: a day across a daylight saving change lasts 23 or 25 hours.
        Seconds, minutes and hours have a fixed length. For a zone other than
        UTC the client computes where the local periods begin and sends those
        times (query_bounds()).

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
            Buckets (a sequence of Bucket, with numpy columns); empty buckets
            are left out. A bucket's start is
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
    ) -> Buckets:
        """Aggregates a series into buckets that start at the given times.

        Bucket i holds the rows from bounds[i] up to bounds[i + 1]; the last
        one holds the rows up to end. Rows before bounds[0] are left out.
        calendar_bounds() computes such bounds for local days, weeks, months,
        quarters or years; query_group() does both steps.

        Args:
            series: The series id.
            bounds: Bucket starts, ms since the epoch, strictly increasing: a
                list of ints or an int64 array.
            start: First timestamp to include (ms); None for no lower limit.
            end: Timestamp to stop before (ms); None for no upper limit.
            order: "asc" (oldest first) or "desc" (newest first).
            limit: At most this many buckets; None for all.

        Returns:
            Buckets (a sequence of Bucket, with numpy columns); empty buckets
            are left out.

        Raises:
            TsdbError: ERR_INVALID if bounds is empty or does not increase.
        """
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
        """Time-weighted averages: each reading weighted by how long it held
        (order and limit as in query_agg()).

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

        Raises:
            TsdbError: ERR_INVALID for a threshold below 3 or a state series.
        """
    def delete_range(self, series: int, start: int | None, end: int | None) -> None:
        """Deletes the rows of a series with start <= ts < end.

        A delete is a write like any other: a row inserted after it returned
        is kept. start and end are required; None is unbounded, so
        delete_range(id, None, None) deletes every row of the series.
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
        atomically: a reader sees the old rows or the new ones, never a mix.

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
                one request (256 MiB).
        """
    def set_retention(self, series: int, limit_ms: int) -> None:
        """Sets how long a series keeps its rows, in ms:
        rows older than the server's clock minus limit_ms disappear from
        queries at once and from disk as merges get to them. 0 keeps them.

        Raises:
            TsdbError: ERR_INVALID for a negative limit.
        """
    def retention(self, series: int) -> int:
        """Returns the retention limit of a series in ms, 0 for none."""
    def commit(self) -> None:
        """Makes everything written so far durable on the server's disk."""
    def flush(self) -> None:
        """Commits, then writes the rows the server holds in memory to parts on disk."""
    def optimize(self) -> None:
        """Merges the parts of every partition into one (slow on big databases)."""
    def stats(self) -> Stats:
        """Returns database statistics (a Stats)."""
    def whoami(self) -> User:
        """The user this connection is logged in as, with its grants (a User);
        name "" with every right (superuser) on a server started with --no-auth."""
    def user_list(self) -> list[User]:
        """The users this one may see (a list of User): itself, and the users it
        manages (every user, for a superuser).

        Raises:
            TsdbError: ERR_DENIED on a server started with --no-auth (users are
                managed there with the offline `tsdb DIR user` verbs).
        """
    def user_create(self, name: str, password: str, *, superuser: bool = False, iterations: int | None = None) -> None:
        """Creates user name with password, a superuser if superuser.

        The password does not travel: the client sends its verifier (a fresh
        salt, StoredKey and ServerKey) computed with iterations of PBKDF2
        (None: SCRAM_ITERATIONS, 4096; at least 4096, at most 100,000,000).

        Raises:
            TsdbError: ERR_DENIED unless this user is a superuser or holds admin
                on a prefix that name starts with (only a superuser creates
                superusers), and on a --no-auth server; ERR_INVALID for a name
                that exists or that the server refuses; ERR_INVALID for
                iterations out of range.
            ValueError: password is not 1 to PASSWORD_MAX (1023) bytes without
                NUL (the rule of every client and the CLI).
        """
    def user_password(self, name: str, password: str, *, iterations: int | None = None) -> None:
        """Sets the password of user name (its own, or one of a user it manages),
        sent as user_create() sends it.

        Raises:
            TsdbError: As user_create(); ERR_DENIED also for a user holding a
                grant outside this one's admin selectors.
        """
    def user_drop(self, name: str) -> None:
        """Drops user name; its open connections are answered ERR_AUTH and closed.

        Raises:
            TsdbError: ERR_DENIED for a user this one does not manage, and for
                the last superuser.
        """
    def user_alter(self, name: str, *, superuser: bool) -> None:
        """Makes user name a superuser or takes that away (superusers only).

        Raises:
            TsdbError: ERR_DENIED unless this user is a superuser, and for
                taking it from the last superuser.
        """
    def grant(self, name: str, selector: str, rights: str | int) -> None:
        """Gives user name rights on the series selector matches. Grants only add.

        selector is written as the CLI writes it: "all", "prefix:P" (a byte
        prefix of the name: tenants end theirs in "/"), "name:N" or "glob:G"
        (* any run of bytes, ? one byte, \\ makes the next one literal). rights
        are names ("read,insert"; "write" is insert and delete) or RIGHT_* bits.
        Names are separated by commas, with spaces around them allowed, as
        tsdbd and the CLI read them; "none" and "0xHH" (bits a later server
        may name) read too, and the server refuses rights it cannot grant.

        Example:
            >>> c.grant("acme/ingest", "prefix:acme/", "insert")
            >>> c.grant("acme/dash", "glob:acme/*/temp", RIGHT_READ)

        Raises:
            ValueError: A selector or rights it cannot read.
            TsdbError: ERR_DENIED unless this user is a superuser or selector
                lies inside its own admin selectors and it manages name;
                ERR_INVALID for a selector the server refuses (a glob over 256
                bytes, a 65th glob grant).
        """
    def revoke(self, name: str, selector: str, rights: str | int) -> None:
        """Takes rights on selector from user name (the grant with exactly this
        selector), as grant() writes them.

        Raises:
            ValueError: A selector or rights it cannot read.
            TsdbError: As grant().
        """
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
        """Reads the rows of many series in one request.

        The server reads the series in parallel, from one consistent point in
        time, and answers them in the order asked. Lists of more than 65,536
        series go as several requests.

        Args:
            series: Series ids: a list of ints, or a uint64 or int64 array.
            start: First timestamp to include (ms): one for every series, or a
                list or int64 array with one per series; None for no limit.
            end: Timestamp to stop before, like start.
            order: "asc" or "desc", for each series.
            limit: At most this many rows of each series; None for all.
            threads: Threads the request uses at most; 0 for the server's
                --request-threads (Db: the database's request_threads).
            max_rows: Rows of all series together, at most (ERR_TOO_MANY_BUCKETS
                past it); 0 for the server's --multi-max-rows (Db: no limit).
            errors: "raise" raises TsdbError (with .index and .result, the
                Many) for the first series that failed; "status" returns the
                Many, whose .status tells which failed.
            numpy: As for query_values().

        Returns:
            Many: r.ts and r.values hold every series' rows, r.offsets where
            each begins; r[i] is series i's (timestamps, values).

        Raises:
            TsdbError: A series failed (errors="raise"), the whole request
                failed.
        """
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
        """Aggregates many series in one request.

        Each series is bucketed as query_agg() (interval_ms), query_group()
        (unit, count, timezone) or query_bounds() (bounds) would; with none of
        them every series gets one bucket over its range. For a zone other than
        UTC the client computes the local periods' bounds over all the series.

        Args:
            series, start, end, order, limit, threads, max_rows, errors: As for
                query_values_multi(); limit counts buckets.
            interval_ms: Bucket length in ms: one for all, or one per series.
            unit, count, timezone: Calendar periods, as for query_group().
            bounds: Bucket starts for every series, as for query_bounds().

        Returns:
            Many: r.buckets (a Buckets) holds every series' buckets, whose
            columns also read from r (r.counts, r.sum, ...); r[i] is series i's
            Buckets.

        Raises:
            TsdbError: As for query_values_multi().
            ValueError: More than one of interval_ms, unit and bounds.
        """
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
        """The spread aggregates (query_spread()) of many series in one request.

        Arguments and result as for query_buckets_multi(); the buckets have the
        spread columns (range, var_pop, ...).
        """
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
        """Time-weighted averages (query_twa()) of many series in one request.

        Arguments as for query_buckets_multi(), with method and max_gap_ms as
        for query_twa(). r.buckets is a TwaBuckets; r[i] is series i's.
        """
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
        """Downsamples many series with LTTB (query_lttb()) in one request.

        Arguments and result as for query_values_multi(), with threshold as for
        query_lttb().
        """
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

@final
class Pool:
    """A pool of connections to tsdbd for batch calls that use them all at once.

    One call runs many queries (or inserts) spread over the connections, in
    parallel and with the GIL released, so a single Python thread can keep
    the server's cores busy. Use it as a context manager or call close().

    Args:
        host: Server name or address.
        port: Server port.
        connections: Number of connections (default: the number of CPUs here).
        timeout: Seconds for the connect and every later read or write, as in
            Client; None waits forever after a 10 s connect.
        keepalive: TCP keepalive, as in Client.
        user, password, password_file, require_auth: The login, as in Client;
            every connection logs in, a replacement too.

    A connection that breaks (an I/O error or a timeout) is replaced by a new
    one before its next query, and so is every other connection made before
    it broke (after a server restart all of them are dead). A query whose
    connection broke runs once more on a new connection; an insert does not
    (the server may have applied it), so after a restart one insert_many()
    can fail with ERR_IO and the next works. Errors of batch calls name the
    lowest failing entry (TsdbError.index), as Db's do. A Pool has no
    last_query_stats(); a Client has.

    Raises:
        TsdbError: A connection failed.
        ValueError: connections is 0 or timeout is not positive.
    """

    def __new__(
        cls,
        host: str = "127.0.0.1",
        port: int = 7070,
        connections: int | None = None,
        *,
        timeout: float | None = None,
        keepalive: bool = True,
        user: str | None = None,
        password: str | None = None,
        password_file: str | os.PathLike[str] | None = None,
        require_auth: bool = False,
    ) -> Pool: ...
    @property
    def connections(self) -> int:
        """Number of connections."""
    def close(self) -> None:
        """Closes every connection; later calls raise TsdbError (ERR_IO)."""
    def __enter__(self) -> Pool:
        """Returns the pool itself, to use in a with statement."""
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """Closes the pool at the end of a with block."""
    def query_raw_many(
        self,
        series: _SeriesIds,
        start: int | _Timestamps | None = None,
        end: int | _Timestamps | None = None,
        *,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> list[tuple[Any, Any]]:
        """Runs many raw float32 queries over the connections at once.

        Takes and returns what Db.query_raw_many() does: series (one id per
        query), start and end (None, one timestamp, or one per query), order,
        limit; a list of (timestamps, values) numpy arrays, one per query.
        Needs numpy (ImportError without it).

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
        """Runs many aggregate queries over the connections at once.

        As Db.query_agg_many(): a bucket length for every query or one per
        query (0: one bucket for the range); returns a BucketsMany (lists
        instead of numpy columns without numpy).

        Raises:
            TsdbError: The lowest failing query's error; its index attribute is
                that query's position, and every query below it succeeded.
        """
    def insert_many(
        self, batches: Sequence[tuple[int | _SeriesIds, _Timestamps, _TypedValues]], *, value_type: _ValueType | None = None
    ) -> None:
        """Runs many inserts over the connections at once.

        As Db.insert_many(): a list of (series, ts, values) tuples, each all or
        nothing on its own; inserts that share a series run in list order (on
        one connection); without value_type each takes its series' type.

        Raises:
            TsdbError: The lowest failing insert's error, with its index; every
                insert below it was committed, later ones may have been (errors
                found before inserting, as in Db.insert_many(), insert nothing).
                An insert whose connection broke is not retried.
        """
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
        """As Client.query_values_multi() (on one of the connections)."""
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
        """As Client.query_buckets_multi() (on one of the connections)."""
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
        """As Client.query_spread_multi() (on one of the connections)."""
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
        """As Client.query_twa_multi() (on one of the connections)."""
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
        """As Client.query_lttb_multi() (on one of the connections)."""
