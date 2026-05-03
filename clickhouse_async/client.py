"""High-level Client — the user-facing API on top of ``Connection``.

A ``Client`` wraps one ``Connection``. The lifecycle is owned by
``async with``:

    import clickhouse_async as ch

    async with ch.connect("clickhouse://default:@localhost:9000/default") as client:
        rows = await client.fetch_all("SELECT 1")

``connect(dsn)`` is a sync factory that returns an unopened ``Client``;
the actual TCP open + Hello handshake happens in ``__aenter__``. Users
who don't want a context manager can call ``await client.open()``
explicitly and pair it with ``await client.close()``.
"""

from __future__ import annotations

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
from types import TracebackType
from typing import TYPE_CHECKING, cast

from clickhouse_async.connection import Connection, State, TransportFactory
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.errors import ProtocolError, QueryCancellationError
from clickhouse_async.protocol.block import Block, BlockInfo, ColumnSpec
from clickhouse_async.protocol.server_packets import ProfileInfo, ProgressInfo

if TYPE_CHECKING:
    from clickhouse_async.protocol.handshake import ServerInfo


@dataclass
class QueryResult:
    """Outcome of an ``execute()`` call.

    ``columns`` carries the server's column metadata (taken from the
    header block); ``rows`` is a row-major list of tuples assembled by
    transposing each ``DATA`` block. ``progress`` and ``profile_info``
    capture the last respective server packet, or default-constructed
    instances if none arrived. ``elapsed`` is the wall-clock duration
    from ``send_query`` to ``EndOfStream``.
    """

    columns: list[ColumnSpec] = field(default_factory=list)
    rows: list[tuple[object, ...]] = field(default_factory=list)
    written_rows: int = 0
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


class Client:
    """User-facing connection wrapper. One ``Client`` owns one
    ``Connection``; concurrent calls on the same client raise
    ``ConcurrentQueryError`` (the protocol does not multiplex)."""

    def __init__(
        self,
        dsn: str | DSN,
        *,
        ssl_context: _ssl_module.SSLContext | None = None,
        transport_factory: TransportFactory | None = None,
        on_host_attempt: Callable[[tuple[str, int], BaseException | None], None]
        | None = None,
    ) -> None:
        parsed = dsn if isinstance(dsn, DSN) else parse_dsn(dsn)
        self._dsn: DSN = parsed
        # If the DSN says secure but no ssl_context was passed, fall
        # back to the stdlib default. Users who want pinned certs /
        # custom CAs hand us a configured context.
        if parsed.secure and ssl_context is None:
            ssl_context = _ssl_module.create_default_context()
        self._conn = Connection(
            parsed.hosts,
            ssl_context=ssl_context,
            compression=parsed.compression,
            transport_factory=transport_factory,
            on_host_attempt=on_host_attempt,
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
        """Round-trip a ``Ping``/``Pong`` to verify liveness."""
        await self._conn.ping()

    @property
    def server_info(self) -> ServerInfo:
        """Server identity captured during the Hello handshake.

        Raises if accessed before ``__aenter__`` / ``open()``.
        """
        return self._conn.server_info

    @property
    def dsn(self) -> DSN:
        return self._dsn

    @property
    def is_alive(self) -> bool:
        """``True`` iff the underlying connection is in ``READY`` —
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
        """Run ``sql``, drain the server response, and return a
        ``QueryResult`` carrying the columns, the rows row-major, and
        the final progress/profile-info packets.

        Concurrent calls on the same client raise
        ``ConcurrentQueryError`` (the wire protocol does not multiplex).
        Server-reported errors propagate as ``ServerError`` and leave
        the connection in ``READY`` (reusable for the next query).
        """
        start = time.monotonic()
        await self._conn.send_query(
            sql, query_id=query_id, settings=settings, params=params
        )

        columns: list[ColumnSpec] = []
        rows: list[tuple[object, ...]] = []
        captured_progress: ProgressInfo | None = None
        captured_profile: ProfileInfo | None = None

        prior_progress = self._conn.on_progress
        prior_profile = self._conn.on_profile_info

        def _on_progress(p: ProgressInfo) -> None:
            nonlocal captured_progress
            captured_progress = p
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
                # Transpose column-major block.data into row-major tuples.
                for i in range(block.n_rows):
                    rows.append(tuple(col[i] for col in block.data))
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
            written_rows=final_progress.written_rows,
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
        """Run ``sql`` and return all rows. Convenience over ``execute``
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
        """Run ``sql`` and return the first row, or ``None`` if empty."""
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
        """Async-iterate the result of ``sql`` block-by-block.

        Header-only blocks (``n_rows == 0``) are filtered out; only
        data-bearing blocks are yielded. Totals / Extremes are not
        yielded — those need their own typed surface and aren't part
        of the v0 streaming API.

        The generator holds the underlying connection until exhausted.
        To break out early without leaking the connection, wrap with
        ``contextlib.aclosing`` so the cleanup (Cancel + drain) runs
        deterministically at the ``async with`` exit:

            from contextlib import aclosing
            async with aclosing(client.iter_blocks("SELECT …")) as blocks:
                async for block in blocks:
                    if some_condition:
                        break

        Without ``aclosing``, Python defers async-generator cleanup to
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
            if self._conn.state == State.IN_FLIGHT:
                try:
                    await self._conn.cancel()
                except QueryCancellationError:
                    # cancel() always raises with a reason field; we
                    # only need its side effects here.
                    pass

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
        """Run ``INSERT INTO t [ (col, …) ] VALUES`` and stream ``rows`` to
        the server in batches of ``insert_block_size``.

        ``rows`` is a sync iterable, async iterable, or a single iterable
        of tuples / sequences. Each row's length must match
        ``column_names``. The codec for each column is taken from the
        server's INSERT header block — we don't infer types client-side.

        Returns the total number of rows we shipped to the server. A
        column-name mismatch between ``column_names`` and the server's
        header raises ``ValueError`` after cancelling the query
        cleanly; the connection remains reusable.
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
            try:
                await self._conn.cancel()
            except QueryCancellationError:
                pass
            raise ValueError(
                f"INSERT column names mismatch: passed {list(column_names)!r}, "
                f"server expects {server_names!r}"
            )

        total_rows = 0
        batch: list[Sequence[object]] = []

        async def _flush(batch: list[Sequence[object]]) -> int:
            block = _build_insert_block(header.columns, batch)  # type: ignore[union-attr]
            await self._conn.send_data(block)
            return len(batch)

        async for row in _normalise_row_source(rows):
            batch.append(row)
            if len(batch) >= insert_block_size:
                total_rows += await _flush(batch)
                batch = []
        if batch:
            total_rows += await _flush(batch)

        # Empty terminator block tells the server the INSERT payload is
        # complete; the server then emits Progress / EndOfStream.
        await self._conn.send_data(None)

        # Drain remaining packets (Progress / EndOfStream / etc.).
        async for _ in iterator:
            pass

        return total_rows

    async def iter_rows(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> AsyncGenerator[tuple[object, ...], None]:
        """Async-iterate the result of ``sql`` row-by-row.

        A thin transpose around ``iter_blocks``; the same
        ``contextlib.aclosing`` recommendation applies for
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
    interface so the insert loop can ``async for`` over either shape.

    Split into two single-purpose helpers because narrowing a
    sync-or-async-iterable union inside one function loses the
    parametric element type at the iteration site (ty observes
    ``object`` rather than ``Sequence[object]``). The ``cast`` calls
    re-attach the parameter ty's isinstance narrowing strips off."""
    if isinstance(rows, AsyncIterable):
        return _async_rows(cast(AsyncIterable[Sequence[object]], rows))
    return _sync_rows(cast(Iterable[Sequence[object]], rows))


def _build_insert_block(
    specs: Sequence[ColumnSpec], rows: Sequence[Sequence[object]]
) -> Block:
    """Transpose row-major ``rows`` into a column-major Block matching
    ``specs``. Raises ValueError naming the offending row index when a
    row's arity doesn't match."""
    n_cols = len(specs)
    n_rows = len(rows)
    columns_data: list[list[object]] = [[] for _ in range(n_cols)]
    for i, row in enumerate(rows):
        if len(row) != n_cols:
            raise ValueError(
                f"INSERT row {i} has {len(row)} columns; expected {n_cols} "
                f"(column names: {[s.name for s in specs]})"
            )
        for c, val in enumerate(row):
            columns_data[c].append(val)
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
) -> Client:
    """Build an unopened ``Client``. The handshake happens when you
    enter the ``async with`` block (or call ``await client.open()``).

    ``transport_factory`` is a test-only injection point for the
    underlying socket pair; production callers should leave it unset.
    """
    return Client(dsn, ssl_context=ssl_context, transport_factory=transport_factory)
