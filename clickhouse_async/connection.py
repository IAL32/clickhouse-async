"""The native-protocol Connection.

A ``Connection`` owns one TCP socket and one protocol state machine. It
is the layer the high-level ``Client`` is thin over. Substep 06a in
``.plans/06-connection.md`` lands the lifecycle and the state machine
only — handshake, query/insert, packet loop, cancellation, and
compression land in 06b through 06h on top of this skeleton.

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
from collections.abc import Awaitable, Callable
from enum import IntEnum
from typing import Protocol

from clickhouse_async.protocol.io import AsyncBinaryReader

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

    # ---- lifecycle -------------------------------------------------------

    async def open(self) -> None:
        """Establish the TCP transport.

        06a stops at ``CONNECTING`` — the Hello exchange that promotes
        to ``READY`` is added in 06b.
        """
        if self._state != State.IDLE:
            raise RuntimeError(
                f"open() requires IDLE state, got {self._state.name}"
            )
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
