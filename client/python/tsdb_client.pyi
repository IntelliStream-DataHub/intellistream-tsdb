"""Client for tsdbd, the TCP server of the tsdb time series database.

Timestamps are int milliseconds since the epoch, values 32-bit floats. Time
ranges are [start, end); None means unbounded.
"""

from collections.abc import Sequence
from types import TracebackType
from typing import Any, Literal, final

from typing_extensions import Buffer

DEFAULT_PORT: int
PROTOCOL_VERSION: int
DEFAULT_COMPRESSION: int
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

class TsdbError(Exception):
    """An error from tsdbd or the client."""

    code: int
    """The TSDB_ERR_* number (negative), e.g. ERR_NOTFOUND."""

def strerror(code: int) -> str:
    """The text for a TSDB_ERR_* code."""

@final
class Bucket:
    """One aggregate bucket; only non-empty buckets are returned."""

    @property
    def start(self) -> int:
        """Bucket start, ms since the epoch (the query start for a single bucket)."""
    @property
    def count(self) -> int: ...
    @property
    def sum(self) -> float: ...
    @property
    def min(self) -> float: ...
    @property
    def max(self) -> float: ...
    @property
    def isum(self) -> int:
        """Exact sum for int64 and bool series (saturating)."""
    @property
    def imin(self) -> int: ...
    @property
    def imax(self) -> int: ...
    @property
    def avg(self) -> float:
        """sum / count (0 for an empty bucket)."""

@final
class Stats:
    @property
    def parts(self) -> int: ...
    @property
    def partitions(self) -> int: ...
    @property
    def rows(self) -> int: ...
    @property
    def bytes(self) -> int: ...
    @property
    def merges(self) -> int: ...
    @property
    def merged_rows(self) -> int: ...
    @property
    def passthrough_granules(self) -> int: ...
    @property
    def memtable_rows(self) -> int:
        """Rows in the WAL-backed memtable, not yet in parts."""

@final
class QueryStats:
    """What the server did for this connection's previous query."""

    @property
    def elapsed_s(self) -> float: ...
    @property
    def rows(self) -> int:
        """Rows covered: rows_decoded + rows_from_stats + memtable_rows."""
    @property
    def rows_decoded(self) -> int:
        """Rows decompressed (whole granules)."""
    @property
    def rows_from_stats(self) -> int:
        """Rows answered from granule statistics, without decoding."""
    @property
    def memtable_rows(self) -> int: ...
    @property
    def bytes(self) -> int:
        """Uncompressed size of the rows covered."""
    @property
    def bytes_read(self) -> int:
        """Compressed bytes of the granules decoded."""
    @property
    def parts(self) -> int: ...
    @property
    def granules(self) -> int: ...
    @property
    def granules_from_stats(self) -> int: ...
    @property
    def result_rows(self) -> int:
        """Rows, or buckets, returned."""

@final
class Client:
    """One TCP connection to tsdbd. Network calls release the GIL; threads
    sharing a client take turns."""

    def __new__(cls, host: str = "127.0.0.1", port: int = 7070, timeout: float | None = None) -> Client:
        """Connects. `timeout` (seconds) bounds the connect and every later
        socket read or write; a call that times out closes the client."""
    def close(self) -> None:
        """Waits for pending inserts, then closes. Closing twice is fine."""
    def __enter__(self) -> Client: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...
    def protocol_version(self) -> int: ...
    def series(self, name: str, create: bool = True) -> int:
        """The id of series `name`, created if needed; with create=False,
        TsdbError(ERR_NOTFOUND) if it does not exist."""
    def series_at(self, name: str, path: str) -> int:
        """The id of series `name`, created in the volume `path` on the server if needed."""
    def series_name(self, id: int) -> str: ...
    def insert_series(self, series: int, ts: _Timestamps, values: _Values) -> None:
        """Inserts rows of one series and waits. The last write of a timestamp wins."""
    def insert(self, series: _SeriesIds, ts: _Timestamps, values: _Values) -> None:
        """Inserts rows of many series (series[i] is the series of row i) and waits."""
    def insert_series_async(self, series: int, ts: _Timestamps, values: _Values) -> None:
        """Sends without waiting; errors come from wait() or the next call that reads a reply."""
    def insert_async(self, series: _SeriesIds, ts: _Timestamps, values: _Values) -> None: ...
    def series_create(self, name: str, value_type: _ValueType = "float32", path: str | None = None) -> int:
        """The id of a typed series, created if needed; another type is TsdbError(ERR_INVALID)."""
    def series_type(self, id: int) -> _ValueType: ...
    def state(self, series: int, name: str) -> int:
        """The code of state `name` in the series' volume dictionary, created if needed."""
    def state_lookup(self, series: int, name: str) -> int: ...
    def state_name(self, series: int, code: int) -> str: ...
    def state_names(self, series: int, codes: _TypedValues) -> list[str | None]:
        """Names of codes (None and masked entries give None); cached."""
    def insert_series_values(
        self, series: int, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None:
        """Inserts typed values (None or masked = null) and waits. value_type
        defaults to the series' type, asked once and cached."""
    def insert_values(
        self, series: _SeriesIds, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None: ...
    def insert_series_values_async(
        self, series: int, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None: ...
    def insert_values_async(
        self, series: _SeriesIds, ts: _Timestamps, values: _TypedValues, *, value_type: _ValueType | None = None
    ) -> None: ...
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
        """(timestamps, values) of any type: an int64 array and a
        numpy.ma.MaskedArray (masked = null) with numpy, else lists with None
        for nulls."""
    def wait(self) -> None:
        """Collects pending insert acknowledgements; raises the first error."""
    def set_compression(self, level: int) -> None:
        """zstd level of inserts: 0 sends plain arrays; others (1 by default)
        pack them for servers of protocol version 3 or later."""
    @property
    def compression(self) -> int: ...
    @property
    def bytes_sent(self) -> int:
        """Bytes written to the socket by this client so far."""
    @property
    def pending(self) -> int:
        """Inserts sent but not yet acknowledged."""
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
        """(timestamps, values): numpy int64/float32 arrays if numpy is
        importable (numpy=None) or required (numpy=True), else lists of int
        and float. limit=0 returns nothing."""
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
        """Same as query_raw."""
    def query_agg(
        self, series: int, start: int | None = None, end: int | None = None, interval_ms: int = 0
    ) -> list[Bucket]:
        """Buckets of interval_ms aligned to the epoch; 0 gives one bucket for the range."""
    def query_group(
        self,
        series: int,
        start: int | None = None,
        end: int | None = None,
        *,
        unit: _Unit | None = None,
        count: int = 1,
        timezone: str | None = None,
        order: _Order = "asc",
        limit: int | None = None,
    ) -> list[Bucket]:
        """Buckets of `count` calendar units in `timezone` ("UTC", "+02:00",
        "Europe/Oslo"; None is UTC). unit=None gives one bucket for the range."""
    def commit(self) -> None: ...
    def flush(self) -> None: ...
    def optimize(self) -> None: ...
    def stats(self) -> Stats: ...
    def last_query_stats(self) -> QueryStats:
        """What the server did for this connection's previous query."""
