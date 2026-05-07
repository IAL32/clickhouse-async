"""High-level Client — the user-facing API on top of `Connection`.

A `Client` wraps one `Connection`. The lifecycle is owned by
`async with`:

    import clickhouse_async as ch

    async with ch.connect("clickhouse://default:@localhost:9000/default") as client:
        rows = await client.fetch_all("SELECT 1")

`connect(dsn)` is a sync factory that returns an unopened `Client`;
the actual TCP open + Hello handshake happens in `__aenter__`. Users
who don't want a context manager can call `await client.open()`
explicitly and pair it with `await client.close()`.
"""

from __future__ import annotations

import contextlib
import ssl as _ssl_module
import time
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from clickhouse_async.connection import Connection, State, TransportFactory
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.errors import ProtocolError, QueryCancellationError
from clickhouse_async.protocol.block import Block, BlockInfo, ColumnSpec
from clickhouse_async.protocol.server_packets import ProfileInfo, ProgressInfo

if TYPE_CHECKING:
    from types import TracebackType

    from clickhouse_async.protocol.compression import CompressionMethod
    from clickhouse_async.protocol.handshake import ServerInfo


@dataclass
class QueryResult:
    """Outcome of an `execute()` call.

    `columns` carries the server's column metadata (taken from the
    header block); `rows` is a row-major list of tuples assembled by
    transposing each `DATA` block. `progress` is the *last*
    Progress packet received (each carries increments since the
    previous one), and `profile_info` is the single ProfileInfo
    packet emitted near end-of-stream. `elapsed` is the wall-clock
    duration from `send_query` to `EndOfStream`.

    `written_rows` is the **server-confirmed** count: the sum of
    every `Progress.written_rows` increment the server sent during
    the query. For SELECTs this stays at 0; for INSERTs it tracks
    what the server actually wrote (which may differ from
    `client_sent_rows` when a CHECK / DEDUP / partial-write filter
    drops rows server-side). `client_sent_rows` carries the
    matching client-side count: for `insert()` it's how many rows
    we shipped over the wire, for `execute()` it stays at 0.
    """

    columns: list[ColumnSpec] = field(default_factory=list)
    rows: list[tuple[object, ...]] = field(default_factory=list)
    written_rows: int = 0
    client_sent_rows: int = 0
    elapsed: float = 0.0
    progress: ProgressInfo = field(
        default_factory=lambda: ProgressInfo(
            read_rows=0, read_bytes=0, total_rows_to_read=0
        )
    )
    profile_info: ProfileInfo | None = None

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True)
class ColumnarBlock:
    """One server block yielded by `iter_column_blocks`, column-major.

    `data[col_idx]` is the decoded value list for that column — the
    native shape the wire delivers, with no row-tuple transpose. When
    `column_factories` are active, individual elements may not be
    lists (e.g. `numpy.ndarray`); the factory owns the element type.
    """

    columns: tuple[ColumnSpec, ...]
    data: list[Any]  # data[col_idx] — list by default; factory output otherwise
    n_rows: int


@dataclass(frozen=True)
class ColumnarResult:
    """Full query result from `fetch_columns`, column-major.

    `data[col_idx]` is the complete value list for that column
    concatenated across all server blocks. `rows` is the total row
    count (`len(data[0])` when at least one column is present).
    `bytes_read` and `rows_read` come from the last Progress packet.
    When `column_factories` are active, individual elements may not
    be lists; the factory owns the element type.
    """

    columns: tuple[ColumnSpec, ...]
    data: list[Any]  # data[col_idx] — list by default; factory output otherwise
    rows: int
    written_rows: int
    elapsed: float
    bytes_read: int
    rows_read: int


class Client:
    """User-facing connection wrapper. One `Client` owns one
    `Connection`; concurrent calls on the same client raise
    `ConcurrentQueryError` (the protocol does not multiplex)."""

    def __init__(
        self,
        dsn: str | DSN,
        *,
        ssl_context: _ssl_module.SSLContext | None = None,
        transport_factory: TransportFactory | None = None,
        on_host_attempt: Callable[[tuple[str, int], BaseException | None], None]
        | None = None,
        column_factories: dict[str, Callable[[list[Any]], Any]] | None = None,
        json_nested: bool = False,
        compression: CompressionMethod | None = None,
        connect_timeout: float | None = None,
    ) -> None:
        parsed = dsn if isinstance(dsn, DSN) else parse_dsn(dsn)
        self._dsn: DSN = parsed
        # If the DSN says secure but no ssl_context was passed, fall
        # back to the stdlib default. Users who want pinned certs /
        # custom CAs hand us a configured context.
        if parsed.secure and ssl_context is None:
            ssl_context = _ssl_module.create_default_context()
        # Stash the resolved config so `kill_query` can mint a fresh
        # side-channel `Client` when the primary connection is busy.
        self._ssl_context = ssl_context
        self._transport_factory = transport_factory
        self._on_host_attempt = on_host_attempt
        self._column_factories = column_factories
        self._connect_timeout = connect_timeout
        # Explicit kwarg overrides DSN; None in either falls through to
        # _default_compression() inside Connection.__init__.
        effective_compression = (
            compression if compression is not None else parsed.compression
        )
        self._conn = Connection(
            parsed.hosts,
            ssl_context=ssl_context,
            compression=effective_compression,
            transport_factory=transport_factory,
            on_host_attempt=on_host_attempt,
            json_nested=json_nested,
            connect_timeout=connect_timeout,
        )

    # ---- lifecycle -------------------------------------------------------

    async def open(self) -> None:
        """Open the underlying transport and run the Hello handshake."""
        await self._conn.open(
            user=self._dsn.user,
            password=self._dsn.password,
            database=self._dsn.database,
        )

    async def close(self) -> None:
        """Close the underlying transport. Idempotent."""
        await self._conn.close()

    async def __aenter__(self) -> Client:
        await self.open()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        await self.close()

    # ---- introspection ---------------------------------------------------

    async def ping(self) -> None:
        """Round-trip a `Ping`/`Pong` to verify liveness."""
        await self._conn.ping()

    @property
    def server_info(self) -> ServerInfo:
        """Server identity captured during the Hello handshake.

        Raises if accessed before `__aenter__` / `open()`.
        """
        return self._conn.server_info

    @property
    def dsn(self) -> DSN:
        return self._dsn

    @property
    def is_alive(self) -> bool:
        """`True` iff the underlying connection is in `READY` —
        i.e. usable for the next query. Used by the pool's release
        path to decide whether to recycle a returned client."""
        return self._conn.state == State.READY

    # ---- queries ---------------------------------------------------------

    async def execute(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> QueryResult:
        """Run `sql`, drain the server response, and return a
        `QueryResult` carrying the columns, the rows row-major, and
        the final progress/profile-info packets.

        Concurrent calls on the same client raise
        `ConcurrentQueryError` (the wire protocol does not multiplex).
        Server-reported errors propagate as `ServerError` and leave
        the connection in `READY` (reusable for the next query).
        """
        start = time.monotonic()
        await self._conn.send_query(
            sql, query_id=query_id, settings=settings, params=params
        )

        columns: list[ColumnSpec] = []
        rows: list[tuple[object, ...]] = []
        captured_progress: ProgressInfo | None = None
        captured_profile: ProfileInfo | None = None
        # Progress packets carry *increments* since the previous one;
        # accumulate the per-call totals instead of overwriting so
        # `QueryResult.written_rows` reflects the server's view.
        total_written_rows = 0

        prior_progress = self._conn.on_progress
        prior_profile = self._conn.on_profile_info

        def _on_progress(p: ProgressInfo) -> None:
            nonlocal captured_progress, total_written_rows
            captured_progress = p
            total_written_rows += p.written_rows
            if prior_progress is not None:
                prior_progress(p)

        def _on_profile(pi: ProfileInfo) -> None:
            nonlocal captured_profile
            captured_profile = pi
            if prior_profile is not None:
                prior_profile(pi)

        self._conn.on_progress = _on_progress
        self._conn.on_profile_info = _on_profile
        try:
            async for streamed in self._conn.iter_packets():
                # execute() ignores totals/extremes — those land on the
                # streaming surface (iter_blocks / iter_rows).
                if streamed.kind != "data":
                    continue
                block = streamed.block
                if not columns and block.columns:
                    columns = block.columns
                if block.n_rows == 0:
                    continue
                # Transpose column-major block.data into row-major tuples
                # via the C-level `zip` rather than a Python comprehension —
                # this materialises one tuple per row in a single C call
                # and is the difference between ~150ms and ~30ms on a
                # 1M-row mixed-type block.
                rows.extend(zip(*block.data, strict=False))
        finally:
            self._conn.on_progress = prior_progress
            self._conn.on_profile_info = prior_profile

        elapsed = time.monotonic() - start
        # Materialise both fields once so ty's flow analysis doesn't
        # over-narrow the inline conditionals (the callbacks run inside
        # the async-for loop, which ty doesn't track for closure
        # mutation).
        final_progress: ProgressInfo
        if captured_progress is not None:
            final_progress = captured_progress
        else:
            final_progress = ProgressInfo(
                read_rows=0, read_bytes=0, total_rows_to_read=0
            )
        return QueryResult(
            columns=columns,
            rows=rows,
            written_rows=total_written_rows,
            elapsed=elapsed,
            progress=final_progress,
            profile_info=captured_profile,
        )

    async def fetch_all(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> list[tuple[object, ...]]:
        """Run `sql` and return all rows. Convenience over `execute`
        for callers that don't need the surrounding metadata."""
        result = await self.execute(
            sql, params=params, settings=settings, query_id=query_id
        )
        return result.rows

    async def fetch_one(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> tuple[object, ...] | None:
        """Run `sql` and return the first row, or `None` if empty."""
        rows = await self.fetch_all(
            sql, params=params, settings=settings, query_id=query_id
        )
        return rows[0] if rows else None

    # ---- streaming -------------------------------------------------------

    async def iter_blocks(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> AsyncGenerator[Block, None]:
        """Async-iterate the result of `sql` block-by-block.

        Header-only blocks (`n_rows == 0`) are filtered out; only
        data-bearing blocks are yielded. Totals / Extremes are not
        yielded — those need their own typed surface and aren't part
        of the v0 streaming API.

        The generator holds the underlying connection until exhausted.
        To break out early without leaking the connection, wrap with
        `contextlib.aclosing` so the cleanup (Cancel + drain) runs
        deterministically at the `async with` exit:

            from contextlib import aclosing
            async with aclosing(client.iter_blocks("SELECT …")) as blocks:
                async for block in blocks:
                    if some_condition:
                        break

        Without `aclosing`, Python defers async-generator cleanup to
        GC time — a subsequent operation on the same client may fire
        before cancel completes.
        """
        await self._conn.send_query(
            sql, query_id=query_id, settings=settings, params=params
        )
        try:
            async for streamed in self._conn.iter_packets():
                if streamed.kind != "data":
                    continue
                if streamed.block.n_rows == 0:
                    continue
                yield streamed.block
        finally:
            # If the user broke out before EndOfStream, cancel and drain
            # so the connection is reusable for the next operation.
            # `cancel()` always raises `QueryCancellationError` with a
            # reason — we only need its side effects here.
            if self._conn.state == State.IN_FLIGHT:
                with contextlib.suppress(QueryCancellationError):
                    await self._conn.cancel()

    # ---- columnar surface -----------------------------------------------

    def _apply_factories(
        self,
        columns: tuple[ColumnSpec, ...],
        raw: list[Any],
    ) -> list[Any]:
        """Apply per-column factories (if any) to `raw` column data.

        Factories are keyed on `ColumnSpec.type_spec` — shallow match,
        so a factory for `"UInt64"` does not fire for `"Array(UInt64)"`.
        Columns with no registered factory are returned unchanged.
        """
        if not self._column_factories:
            return raw
        out: list[Any] = []
        for col, values in zip(columns, raw, strict=False):
            factory = self._column_factories.get(col.type_spec)
            out.append(factory(values) if factory else values)
        return out

    async def fetch_columns(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> ColumnarResult:
        """Run `sql` and return the result in column-major order.

        Unlike `execute`, no row-tuple transpose is performed —
        `ColumnarResult.data[i]` is the decoded list for column `i`,
        exactly as the codec produced it across all server blocks.
        Useful for analytics workloads that pass columns directly to
        numpy / polars / pyarrow without an intermediate row representation.
        """
        start = time.monotonic()
        await self._conn.send_query(
            sql, query_id=query_id, settings=settings, params=params
        )

        columns: list[ColumnSpec] = []
        accum: list[list[Any]] = []
        total_rows = 0
        total_written_rows = 0
        captured_progress: ProgressInfo | None = None

        prior_progress = self._conn.on_progress

        def _on_progress(p: ProgressInfo) -> None:
            nonlocal captured_progress, total_written_rows
            captured_progress = p
            total_written_rows += p.written_rows
            if prior_progress is not None:
                prior_progress(p)

        self._conn.on_progress = _on_progress
        try:
            async for streamed in self._conn.iter_packets():
                if streamed.kind != "data":
                    continue
                block = streamed.block
                if not columns and block.columns:
                    columns = list(block.columns)
                    accum = [[] for _ in columns]
                if block.n_rows == 0:
                    continue
                for i, col_data in enumerate(block.data):
                    accum[i].extend(col_data)
                total_rows += block.n_rows
        finally:
            self._conn.on_progress = prior_progress

        elapsed = time.monotonic() - start
        # Materialise once so ty's flow analysis doesn't over-narrow the
        # inline conditional (the callback runs inside the async-for loop,
        # which ty doesn't track for closure mutation).
        final_progress: ProgressInfo = captured_progress or ProgressInfo(
            read_rows=0, read_bytes=0, total_rows_to_read=0
        )
        col_tuple = tuple(columns)
        return ColumnarResult(
            columns=col_tuple,
            data=self._apply_factories(col_tuple, accum),
            rows=total_rows,
            written_rows=total_written_rows,
            elapsed=elapsed,
            bytes_read=final_progress.read_bytes,
            rows_read=final_progress.read_rows,
        )

    async def iter_column_blocks(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> AsyncGenerator[ColumnarBlock, None]:
        """Async-iterate the result of `sql` as `ColumnarBlock` values.

        Each block is yielded column-major with no row-tuple transpose.
        Header-only blocks (`n_rows == 0`) are filtered out.

        The same `contextlib.aclosing` recommendation from `iter_blocks`
        applies: wrap with `aclosing` for deterministic cleanup on early exit.
        """
        await self._conn.send_query(
            sql, query_id=query_id, settings=settings, params=params
        )
        try:
            async for streamed in self._conn.iter_packets():
                if streamed.kind != "data":
                    continue
                block = streamed.block
                if block.n_rows == 0:
                    continue
                col_tuple = tuple(block.columns)
                yield ColumnarBlock(
                    columns=col_tuple,
                    data=self._apply_factories(col_tuple, block.data),
                    n_rows=block.n_rows,
                )
        finally:
            if self._conn.state == State.IN_FLIGHT:
                with contextlib.suppress(QueryCancellationError):
                    await self._conn.cancel()

    # ---- inserts --------------------------------------------------------

    async def insert(
        self,
        sql: str,
        *,
        rows: Iterable[Sequence[object]] | AsyncIterable[Sequence[object]],
        column_names: Sequence[str],
        insert_block_size: int = 65536,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> int:
        """Run `INSERT INTO t [ (col, …) ] VALUES` and stream `rows` to
        the server in batches of `insert_block_size`.

        `rows` is a sync iterable, async iterable, or a single iterable
        of tuples / sequences. Each row's length must match
        `column_names`. The codec for each column is taken from the
        server's INSERT header block — we don't infer types client-side.

        Validation runs against the server's header before any DATA
        bytes leave the wire:

        - `column_names` is compared to the server's column list
          (case-sensitive, ordered). Mismatch → `ValueError`,
          query cancelled cleanly, no rows sent.
        - Each row's length is checked against the header column
          count as the row arrives. Mismatch → `ValueError` naming
          the offending row index. Earlier batches that already
          flushed are committed server-side; the query is cancelled
          before the failing row goes out so callers don't end up
          with a half-block of partial data.

        Returns the **server-confirmed** `written_rows` — the sum
        of every `Progress.written_rows` increment the server
        emitted during the drain phase. Normally equals
        `client_sent_rows`; can diverge under CHECK constraints,
        DEDUP, or partial-batch failures.
        """
        await self._conn.send_query(sql, query_id=query_id, settings=settings)

        # Drain server packets until the header DATA arrives. The server
        # may emit Progress / Log / TimezoneUpdate before the header in
        # some configurations; iter_packets dispatches those through
        # the existing callback hooks while we wait.
        iterator = self._conn.iter_packets()
        header: Block | None = None
        async for streamed in iterator:
            if streamed.kind == "data":
                header = streamed.block
                break
        if header is None:
            raise ProtocolError(
                "INSERT did not receive a header DATA block from the server"
            )

        # Validate column-name alignment. A mismatch is a programmer
        # error, not a transport failure, so we cancel cleanly and
        # surface a ValueError with both lists named.
        server_names = [c.name for c in header.columns]
        if list(column_names) != server_names:
            with contextlib.suppress(QueryCancellationError):
                await self._conn.cancel()
            raise ValueError(
                f"INSERT column names mismatch: passed {list(column_names)!r}, "
                f"server expects {server_names!r}"
            )

        # Hook in the same accumulator `execute` uses so the server's
        # `written_rows` increments roll up across the drain phase.
        prior_progress = self._conn.on_progress
        total_written_rows = 0

        def _on_progress(p: ProgressInfo) -> None:
            nonlocal total_written_rows
            total_written_rows += p.written_rows
            if prior_progress is not None:
                prior_progress(p)

        self._conn.on_progress = _on_progress

        client_sent_rows = 0
        batch: list[Sequence[object]] = []
        n_columns = len(header.columns)

        async def _flush(batch: list[Sequence[object]]) -> int:
            block = _build_insert_block(header.columns, batch)  # type: ignore[union-attr]
            await self._conn.send_data(block)
            return len(batch)

        try:
            # Sync iterables get a fast path that batches in pure
            # Python without yielding through an async generator on
            # every row — at 100k rows that's ~100 ms saved on the
            # insert benchmark (one async-yield costs ~1 µs).
            if isinstance(rows, AsyncIterable):
                async for row_index, row in _enumerate_async(
                    cast("AsyncIterable[Sequence[object]]", rows)
                ):
                    if len(row) != n_columns:
                        with contextlib.suppress(QueryCancellationError):
                            await self._conn.cancel()
                        raise ValueError(
                            f"INSERT row {row_index} has {len(row)} columns; "
                            f"expected {n_columns} (column names: {server_names!r})"
                        )
                    batch.append(row)
                    if len(batch) >= insert_block_size:
                        client_sent_rows += await _flush(batch)
                        batch = []
            else:
                for row_index, row in enumerate(
                    cast("Iterable[Sequence[object]]", rows)
                ):
                    if len(row) != n_columns:
                        with contextlib.suppress(QueryCancellationError):
                            await self._conn.cancel()
                        raise ValueError(
                            f"INSERT row {row_index} has {len(row)} columns; "
                            f"expected {n_columns} (column names: {server_names!r})"
                        )
                    batch.append(row)
                    if len(batch) >= insert_block_size:
                        client_sent_rows += await _flush(batch)
                        batch = []
            if batch:
                client_sent_rows += await _flush(batch)

            # Empty terminator block tells the server the INSERT payload is
            # complete; the server then emits Progress / EndOfStream.
            await self._conn.send_data(None)

            # Drain remaining packets (Progress / EndOfStream / etc.).
            async for _ in iterator:
                pass
        finally:
            self._conn.on_progress = prior_progress

        # Surface the server's view. We fall back to the client-side
        # count when the server emitted no `Progress.written_rows`
        # increments — older servers / specific engines (e.g. Memory
        # with no replication target) sometimes skip them. The two
        # are typically equal; CHECK constraints / DEDUP / partial
        # writes are where they diverge.
        return total_written_rows or client_sent_rows

    async def iter_rows(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> AsyncGenerator[tuple[object, ...], None]:
        """Async-iterate the result of `sql` row-by-row.

        A thin transpose around `iter_blocks`; the same
        `contextlib.aclosing` recommendation applies for
        deterministic cleanup on early exit.
        """
        # Hold the inner generator explicitly so we can aclose() it on
        # GeneratorExit — Python doesn't propagate aclose through a
        # `async for` loop, so a break-out at the iter_rows level would
        # otherwise leave iter_blocks dangling and the connection stuck
        # in IN_FLIGHT.
        inner = self.iter_blocks(
            sql, params=params, settings=settings, query_id=query_id
        )
        try:
            async for block in inner:
                for i in range(block.n_rows):
                    yield tuple(col[i] for col in block.data)
        finally:
            await inner.aclose()

    # ---- query control --------------------------------------------------

    async def kill_query(self, query_id: str, *, sync: bool = True) -> int:
        """Cancel a running query identified by `query_id` from a
        side channel.

        Issues `KILL QUERY WHERE query_id = {qid:String}` (with
        `SYNC` appended when `sync=True`, the default) over a
        separate connection from the one that started the query. The
        return value is the number of queries the server confirmed it
        targeted — each row in the `KILL QUERY` result corresponds
        to one targeted query.

        `sync=True` (default) waits for the server to actually
        cancel the target query before returning. `sync=False`
        drops the `SYNC` keyword and returns as soon as the server
        has accepted the request; the original task may briefly keep
        running.

        Permissions: requires either the same user that issued the
        query or a user with the `KILL QUERY` access right.

        If the current `Client`'s connection is `READY`, the kill
        runs over it directly. Otherwise — typically because *this*
        client is the one mid-query — a fresh side-channel `Client`
        is opened against the same DSN for the duration of the call
        and closed before returning.
        """
        if not query_id or not query_id.strip():
            raise ValueError(
                f"query_id must be a non-empty, non-whitespace string; got {query_id!r}"
            )

        sql = "KILL QUERY WHERE query_id = {qid:String}" + (" SYNC" if sync else "")
        params: Mapping[str, object] = {"qid": query_id}

        if self._conn.state == State.READY:
            result = await self.execute(sql, params=params)
            return result.row_count

        # Primary connection is busy / broken / closed — open a fresh
        # side-channel client with the same DSN config for this call
        # only, then drop it.
        async with Client(
            self._dsn,
            ssl_context=self._ssl_context,
            transport_factory=self._transport_factory,
            on_host_attempt=self._on_host_attempt,
            connect_timeout=self._connect_timeout,
        ) as fresh:
            result = await fresh.execute(sql, params=params)
            return result.row_count


async def _async_rows(
    rows: AsyncIterable[Sequence[object]],
) -> AsyncGenerator[Sequence[object], None]:
    async for row in rows:
        yield row


async def _sync_rows(
    rows: Iterable[Sequence[object]],
) -> AsyncGenerator[Sequence[object], None]:
    for row in rows:
        yield row


def _normalise_row_source(
    rows: Iterable[Sequence[object]] | AsyncIterable[Sequence[object]],
) -> AsyncGenerator[Sequence[object], None]:
    """Wrap a sync or async iterable in a uniform async-generator
    interface so the insert loop can `async for` over either shape.

    Split into two single-purpose helpers because narrowing a
    sync-or-async-iterable union inside one function loses the
    parametric element type at the iteration site (ty observes
    `object` rather than `Sequence[object]`). The `cast` calls
    re-attach the parameter ty's isinstance narrowing strips off."""
    if isinstance(rows, AsyncIterable):
        return _async_rows(cast("AsyncIterable[Sequence[object]]", rows))
    return _sync_rows(cast("Iterable[Sequence[object]]", rows))


async def _enumerate_async(
    source: AsyncIterable[Sequence[object]],
) -> AsyncGenerator[tuple[int, Sequence[object]], None]:
    """Async `enumerate` over an async row source. Used by
    `Client.insert` so per-row validation errors can name the
    offending row index."""
    index = 0
    async for row in source:
        yield index, row
        index += 1


def _build_insert_block(
    specs: Sequence[ColumnSpec], rows: Sequence[Sequence[object]]
) -> Block:
    """Transpose row-major `rows` into a column-major Block matching
    `specs`. Raises ValueError naming the offending row index when a
    row's arity doesn't match."""
    n_cols = len(specs)
    n_rows = len(rows)
    if n_rows == 0:
        return Block(
            info=BlockInfo(),
            columns=list(specs),
            n_rows=0,
            data=[[] for _ in range(n_cols)],
        )
    # Caller validates row arity before getting here, but a defensive
    # spot-check protects against `zip` silently truncating. We sample
    # the first row; `Client.insert` does the per-row check during the
    # async iteration so a stray short row will already have raised.
    first_len = len(rows[0])
    if first_len != n_cols:  # pragma: no cover — caller validates first
        raise ValueError(
            f"INSERT row 0 has {first_len} columns; expected {n_cols} "
            f"(column names: {[s.name for s in specs]})"
        )
    # `zip(*rows)` materialises the columns in one C-level call rather
    # than n_rows*n_cols Python `list.append` operations.
    columns_data = [list(col) for col in zip(*rows, strict=False)]
    return Block(
        info=BlockInfo(),
        columns=list(specs),
        n_rows=n_rows,
        data=columns_data,
    )


def connect(
    dsn: str | DSN,
    *,
    ssl_context: _ssl_module.SSLContext | None = None,
    transport_factory: TransportFactory | None = None,
    column_factories: dict[str, Callable[[list[Any]], Any]] | None = None,
    json_nested: bool = False,
    compression: CompressionMethod | None = None,
    connect_timeout: float | None = None,
) -> Client:
    """Build an unopened `Client`. The handshake happens when you
    enter the `async with` block (or call `await client.open()`).

    `column_factories` maps ClickHouse type-spec strings (e.g.
    `"UInt64"`, `"Nullable(Float32)"`) to callables that transform
    a plain `list` into any type the caller wants (e.g.
    `numpy.array`). Applied in `fetch_columns` and
    `iter_column_blocks`; row-major paths are unaffected.

    `json_nested=True` makes every `JSON` column in the session
    return nested dicts (`{"user": {"id": 7}}`) instead of flat
    dotted-path dicts (`{"user.id": 7}`). Write path always accepts
    both shapes transparently.

    `compression` overrides the DSN's compression setting. `None`
    (the default) defers to the DSN, or auto-detects LZ4 when the
    `[compression]` extra is installed and the DSN omits
    `?compression=`. Pass `CompressionMethod.NONE` to force off.

    `connect_timeout` limits how long each per-host TCP connect +
    Hello handshake may take (seconds). `None` means no limit. On
    timeout the host counts as failed and the next candidate is tried.

    `transport_factory` is a test-only injection point for the
    underlying socket pair; production callers should leave it unset.
    """
    return Client(
        dsn,
        ssl_context=ssl_context,
        transport_factory=transport_factory,
        column_factories=column_factories,
        json_nested=json_nested,
        compression=compression,
        connect_timeout=connect_timeout,
    )
