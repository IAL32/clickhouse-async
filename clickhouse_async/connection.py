"""The native-protocol Connection.

A ``Connection`` owns one TCP socket and one protocol state machine. It
is the layer the high-level ``Client`` is thin over. Substep 06a landed
the lifecycle skeleton; 06b adds the Hello handshake that promotes
``CONNECTING → READY``; 06c adds ``send_query`` and ``iter_packets`` for
the minimal SELECT round-trip. INSERT (06e), parameters (06f),
cancellation (06g), and compression (06h) sit on top.

Single-task model: the connection never spawns a background reader
task. Every read happens on the calling task, so cancellation lives
in one place and there's no "who owns the buffer" problem when a user
breaks out of a streamed iterator. Adding a reader task later would
undo the cancellation reasoning we get for free here.
"""

from __future__ import annotations

import asyncio
import logging
import ssl as _ssl_module
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from enum import IntEnum
from typing import Protocol

from clickhouse_async.errors import ConcurrentQueryError, ProtocolError
from clickhouse_async.protocol.block import Block, read_block
from clickhouse_async.protocol.exception_packet import read_exception_body
from clickhouse_async.protocol.handshake import (
    ServerInfo,
    read_server_hello,
    write_client_hello,
)
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import OUR_REVISION, ServerPacket
from clickhouse_async.protocol.query_packet import write_query_packet

_logger = logging.getLogger(__name__)


class State(IntEnum):
    IDLE = 0
    CONNECTING = 1
    READY = 2
    IN_FLIGHT = 3
    BROKEN = 4
    CLOSED = 5


class _WriterLike(Protocol):
    """The slice of ``asyncio.StreamWriter`` we actually call.

    A test ``ScriptedTransport`` matches this protocol structurally so
    we can drive the connection without a real socket.
    """

    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...
    def is_closing(self) -> bool: ...
    async def drain(self) -> None: ...
    async def wait_closed(self) -> None: ...


TransportFactory = Callable[
    [str, int, _ssl_module.SSLContext | None],
    Awaitable[tuple[asyncio.StreamReader, _WriterLike]],
]


async def _default_transport_factory(
    host: str,
    port: int,
    ssl_context: _ssl_module.SSLContext | None,
) -> tuple[asyncio.StreamReader, _WriterLike]:
    return await asyncio.open_connection(host, port, ssl=ssl_context)


class Connection:
    """Native-protocol connection skeleton.

    For 06a, ``open()`` takes the transport up to ``CONNECTING`` and
    stops. The Hello exchange that promotes ``CONNECTING → READY`` is
    06b's job. ``close()`` is idempotent and safe from any state.
    """

    def __init__(
        self,
        host: str,
        port: int = 9000,
        *,
        ssl_context: _ssl_module.SSLContext | None = None,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._ssl_context = ssl_context
        self._transport_factory: TransportFactory = (
            transport_factory or _default_transport_factory
        )
        self._state: State = State.IDLE
        self._reader: AsyncBinaryReader | None = None
        self._writer: _WriterLike | None = None
        self._server_info: ServerInfo | None = None
        self._negotiated_revision: int = 0
        self._user: str = ""
        # (from_state, to_state, reason) per transition — load-bearing for
        # tests and a useful debugging breadcrumb in production logs.
        self._transitions: list[tuple[State, State, str]] = []

    # ---- introspection ---------------------------------------------------

    @property
    def state(self) -> State:
        return self._state

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def transitions(self) -> list[tuple[State, State, str]]:
        """Read-only view of the state transitions this connection has
        gone through. Tests use this to assert the right reasons fired."""
        return list(self._transitions)

    @property
    def server_info(self) -> ServerInfo:
        """Server identity captured during the Hello handshake.

        Raises if the connection hasn't completed the handshake yet —
        otherwise we'd hand callers a half-populated value.
        """
        if self._server_info is None:
            raise RuntimeError(
                f"server_info is not available in state {self._state.name}"
            )
        return self._server_info

    @property
    def negotiated_revision(self) -> int:
        """``min(OUR_REVISION, server_revision)`` — the revision used to
        gate every wire-format decision past the handshake."""
        return self._negotiated_revision

    # ---- lifecycle -------------------------------------------------------

    async def open(
        self,
        *,
        user: str = "default",
        password: str = "",
        database: str = "default",
    ) -> None:
        """Open the transport and run the Hello handshake.

        On success the connection ends in ``READY`` with ``server_info``
        populated and ``negotiated_revision`` set to
        ``min(OUR_REVISION, server.revision)``.

        On a server-side rejection (server replies ``Exception`` to our
        Hello) the underlying ``ServerError`` propagates and the
        connection is BROKEN. On any other handshake failure (incl.
        cancellation, IO error, malformed packet) the connection is
        also BROKEN and the writer is closed.
        """
        if self._state != State.IDLE:
            raise RuntimeError(
                f"open() requires IDLE state, got {self._state.name}"
            )
        self._user = user
        self._transition(State.CONNECTING, "open()")
        try:
            reader, writer = await self._transport_factory(
                self._host, self._port, self._ssl_context
            )
        except BaseException as exc:
            self._transition(State.BROKEN, f"transport open failed: {exc!r}")
            raise
        self._reader = AsyncBinaryReader(reader)
        self._writer = writer
        try:
            await self._do_handshake(
                user=user, password=password, database=database
            )
        except BaseException as exc:
            self._transition(State.BROKEN, f"handshake failed: {exc!r}")
            await self._cleanup_writer()
            raise

    async def _do_handshake(
        self, *, user: str, password: str, database: str
    ) -> None:
        assert self._reader is not None and self._writer is not None
        # Send client Hello as one buffered write — the protocol expects
        # the whole packet to arrive together.
        out = BinaryWriter()
        write_client_hello(out, user=user, password=password, database=database)
        self._writer.write(out.getvalue())
        await self._writer.drain()

        # Read the server's response. The packet id determines whether
        # the server accepted us (HELLO) or rejected us (EXCEPTION).
        packet_id = await self._reader.read_varuint()
        if packet_id == ServerPacket.HELLO:
            info = await read_server_hello(self._reader)
            self._server_info = info
            self._negotiated_revision = min(OUR_REVISION, info.revision)
            self._transition(
                State.READY,
                f"handshake complete (server={info.name!r} "
                f"revision={info.revision}, negotiated={self._negotiated_revision})",
            )
        elif packet_id == ServerPacket.EXCEPTION:
            raise await read_exception_body(self._reader)
        else:
            raise ProtocolError(
                f"unexpected packet id {packet_id} during handshake "
                f"(expected HELLO={ServerPacket.HELLO.value} or "
                f"EXCEPTION={ServerPacket.EXCEPTION.value})"
            )

    # ---- queries ---------------------------------------------------------

    async def send_query(
        self,
        sql: str,
        *,
        query_id: str = "",
        settings: Mapping[str, str] | None = None,
    ) -> None:
        """Send a Query packet for ``sql``. Transitions ``READY → IN_FLIGHT``.

        For 06c, ``settings`` is forwarded as-is and ``parameters`` /
        compression aren't exposed yet — those come in 06d/06f/06h.
        Concurrent calls on the same connection raise
        ``ConcurrentQueryError`` rather than queueing or fanning out.
        """
        if self._state == State.IN_FLIGHT:
            raise ConcurrentQueryError(
                "another query is already in flight on this connection"
            )
        if self._state != State.READY:
            raise RuntimeError(
                f"send_query() requires READY state, got {self._state.name}"
            )
        assert self._writer is not None  # READY implies handshake completed

        out = BinaryWriter()
        write_query_packet(
            out,
            sql=sql,
            query_id=query_id,
            user=self._user,
            revision=self._negotiated_revision,
            settings=settings,
        )
        self._writer.write(out.getvalue())
        await self._writer.drain()
        self._transition(
            State.IN_FLIGHT,
            f"send_query(query_id={query_id!r}, len(sql)={len(sql)})",
        )

    async def iter_packets(self) -> AsyncIterator[Block]:
        """Yield each result block until ``EndOfStream`` or ``Exception``.

        06c handles the minimum dispatch — ``DATA`` (yielded), ``EXCEPTION``
        (raises ``ServerError``, returns to READY), ``END_OF_STREAM``
        (terminates, returns to READY). Anything else mid-query is a
        ``ProtocolError`` and marks the connection BROKEN. The fuller
        packet roster (Progress / ProfileInfo / Log / …) lands in 06d.
        """
        if self._state != State.IN_FLIGHT:
            raise RuntimeError(
                f"iter_packets() requires IN_FLIGHT state, got {self._state.name}"
            )
        assert self._reader is not None
        while True:
            packet_id = await self._reader.read_varuint()
            if packet_id == ServerPacket.DATA:
                # External-table name on every Data packet (empty for the
                # main result table).
                _ = await self._reader.read_string()
                block = await read_block(
                    self._reader, revision=self._negotiated_revision
                )
                yield block
            elif packet_id == ServerPacket.END_OF_STREAM:
                self._transition(State.READY, "EndOfStream")
                return
            elif packet_id == ServerPacket.EXCEPTION:
                err = await read_exception_body(self._reader)
                self._transition(
                    State.READY, f"server exception: {err.name}"
                )
                raise err
            else:
                self._transition(
                    State.BROKEN, f"unexpected packet id {packet_id}"
                )
                raise ProtocolError(
                    f"unexpected packet id {packet_id} during query "
                    f"(06c handles DATA={ServerPacket.DATA.value}, "
                    f"END_OF_STREAM={ServerPacket.END_OF_STREAM.value}, "
                    f"EXCEPTION={ServerPacket.EXCEPTION.value}; "
                    f"more packet types arrive in 06d)"
                )

    async def _cleanup_writer(self) -> None:
        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is None or writer.is_closing():
            return
        writer.close()
        try:
            await writer.wait_closed()
        except (OSError, asyncio.CancelledError):
            pass

    async def close(self) -> None:
        """Close the transport and transition to ``CLOSED``.

        Idempotent and safe to call from any state, including mid-open.
        """
        if self._state == State.CLOSED:
            return
        self._transition(State.CLOSED, "close()")
        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is not None and not writer.is_closing():
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, asyncio.CancelledError):
                # Already-closed sockets and cancellation during shutdown
                # aren't load-bearing — we're closing anyway.
                pass

    # ---- internals -------------------------------------------------------

    def _transition(self, new_state: State, reason: str) -> None:
        old = self._state
        self._state = new_state
        self._transitions.append((old, new_state, reason))
        _logger.debug(
            "connection state %s -> %s (%s)", old.name, new_state.name, reason
        )
