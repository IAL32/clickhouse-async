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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING

from clickhouse_async.connection import Connection, _WriterLike
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.protocol.block import ColumnSpec
from clickhouse_async.protocol.server_packets import ProfileInfo, ProgressInfo

if TYPE_CHECKING:
    import asyncio

    from clickhouse_async.protocol.handshake import ServerInfo


_TransportFactory = Callable[
    [str, int, _ssl_module.SSLContext | None],
    Awaitable[tuple["asyncio.StreamReader", _WriterLike]],
]


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
        transport_factory: _TransportFactory | None = None,
    ) -> None:
        parsed = dsn if isinstance(dsn, DSN) else parse_dsn(dsn)
        self._dsn: DSN = parsed
        # If the DSN says secure but no ssl_context was passed, fall
        # back to the stdlib default. Users who want pinned certs /
        # custom CAs hand us a configured context.
        if parsed.secure and ssl_context is None:
            ssl_context = _ssl_module.create_default_context()
        self._conn = Connection(
            host=parsed.host,
            port=parsed.port,
            ssl_context=ssl_context,
            compression=parsed.compression,
            transport_factory=transport_factory,
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
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
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
                # 07b ignores totals/extremes — they're part of the
                # streaming surface in 07c.
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
        return QueryResult(
            columns=columns,
            rows=rows,
            written_rows=(
                captured_progress.written_rows if captured_progress else 0
            ),
            elapsed=elapsed,
            progress=captured_progress
            or ProgressInfo(read_rows=0, read_bytes=0, total_rows_to_read=0),
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


def connect(
    dsn: str | DSN,
    *,
    ssl_context: _ssl_module.SSLContext | None = None,
    transport_factory: _TransportFactory | None = None,
) -> Client:
    """Build an unopened ``Client``. The handshake happens when you
    enter the ``async with`` block (or call ``await client.open()``).

    ``transport_factory`` is a test-only injection point for the
    underlying socket pair; production callers should leave it unset.
    """
    return Client(
        dsn, ssl_context=ssl_context, transport_factory=transport_factory
    )
