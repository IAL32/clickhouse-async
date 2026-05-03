"""A scripted transport for unit-testing ``Connection`` without a socket.

Acts as the ``transport_factory`` argument to ``Connection``, so tests
can drive the connection against pre-recorded server bytes and assert
the exact bytes the connection wrote in response — handshake, query,
packet loop, cancel, compression — without spinning up a real socket.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ssl

    from clickhouse_async.connection import _WriterLike


class _ScriptedWriter:
    """Captures everything the connection writes and exposes a
    ``StreamWriter``-shaped surface for the rest of the methods."""

    def __init__(self, buf: bytearray) -> None:
        self._buf = buf
        self._closed = False

    def write(self, data: bytes) -> None:
        if self._closed:
            raise ConnectionResetError("scripted writer already closed")
        self._buf.extend(data)

    def close(self) -> None:
        self._closed = True

    def is_closing(self) -> bool:
        return self._closed

    async def drain(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class ScriptedTransport:
    """Hands a ``Connection`` a scripted reader/writer pair.

    Tests:
    - call ``feed(bytes)`` to enqueue server-to-client bytes,
    - drive whatever the connection does next (which will read from
      the queued bytes and write to ``written()``),
    - inspect ``written()`` for byte-layout assertions.

    Use as the ``transport_factory=`` argument when constructing
    ``Connection``. The factory is the bound ``__call__`` so the
    instance can be passed directly.
    """

    def __init__(self) -> None:
        self.reader = asyncio.StreamReader()
        self._buf = bytearray()
        self._writer = _ScriptedWriter(self._buf)
        self.opens = 0

    def feed(self, data: bytes) -> None:
        self.reader.feed_data(data)

    def feed_eof(self) -> None:
        self.reader.feed_eof()

    def written(self) -> bytes:
        return bytes(self._buf)

    def writer_closed(self) -> bool:
        return self._writer.is_closing()

    async def __call__(
        self,
        _host: str,
        _port: int,
        _ssl_context: ssl.SSLContext | None,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        self.opens += 1
        return self.reader, self._writer


__all__ = ["ScriptedTransport"]
