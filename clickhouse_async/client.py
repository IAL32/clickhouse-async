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
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import TYPE_CHECKING

from clickhouse_async.connection import Connection, _WriterLike
from clickhouse_async.dsn import DSN, parse_dsn

if TYPE_CHECKING:
    import asyncio

    from clickhouse_async.protocol.handshake import ServerInfo


_TransportFactory = Callable[
    [str, int, _ssl_module.SSLContext | None],
    Awaitable[tuple["asyncio.StreamReader", _WriterLike]],
]


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
