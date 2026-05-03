"""Lifecycle and state-machine tests for ``Connection``.

``open()`` brings the transport up *and* runs the Hello handshake, so
tests that reach an open connection feed a scripted Hello reply via
``encode_server_hello``.
"""

from __future__ import annotations

import ssl
from typing import Never

import pytest

from clickhouse_async.connection import Connection, State

from ._mock_transport import ScriptedTransport
from ._scripted_packets import encode_server_hello

# ---- starts in IDLE -----------------------------------------------------


async def test_freshly_constructed_connection_is_idle() -> None:
    # BEGIN: a brand-new Connection over a scripted transport
    transport = ScriptedTransport()
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN / THEN: the connection is in IDLE and has not opened the transport
    assert conn.state == State.IDLE
    assert transport.opens == 0


# ---- open() — IDLE → READY (after handshake) ----------------------------


async def test_open_completes_handshake_and_reaches_ready() -> None:
    # BEGIN: an IDLE connection over a scripted transport with a Hello
    #        reply queued
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: opening the transport
    await conn.open()

    # THEN: the connection ends in READY, the factory ran exactly once,
    #       and the transition log records both phases
    assert conn.state == State.READY
    assert transport.opens == 1
    transitions_to = [t[1] for t in conn.transitions]
    assert transitions_to == [State.CONNECTING, State.READY]


async def test_open_from_non_idle_state_raises() -> None:
    # BEGIN: a connection already in READY from a successful handshake
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()

    # WHEN: calling open() a second time
    # THEN: a RuntimeError surfaces, naming the current state
    with pytest.raises(RuntimeError, match="READY"):
        await conn.open()


async def test_transport_factory_failure_marks_connection_broken() -> None:
    # BEGIN: a transport factory that raises on the first call
    async def boom(
        _host: str, _port: int, _ssl_context: ssl.SSLContext | None
    ) -> Never:
        raise ConnectionRefusedError("nope")

    conn = Connection([("h", 9000)], transport_factory=boom)

    # WHEN: opening fails
    # THEN: the underlying error propagates, the connection is BROKEN, and
    #       the transition log records the failure reason
    with pytest.raises(ConnectionRefusedError):
        await conn.open()
    assert conn.state == State.BROKEN
    last = conn.transitions[-1]
    assert last[0] == State.CONNECTING
    assert last[1] == State.BROKEN
    assert "transport open failed" in last[2]


# ---- close() ------------------------------------------------------------


async def test_close_from_ready_transitions_to_closed_and_closes_writer() -> None:
    # BEGIN: an open connection in READY (handshake complete)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()

    # WHEN: closing the connection
    await conn.close()

    # THEN: the connection is CLOSED and the underlying writer was closed
    assert conn.state == State.CLOSED
    assert transport.writer_closed()


async def test_close_is_idempotent() -> None:
    # BEGIN: a connection that has already been closed once
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()
    await conn.close()

    # WHEN: closing again
    await conn.close()

    # THEN: still CLOSED, no new transition was logged
    assert conn.state == State.CLOSED
    assert sum(1 for _, to, _ in conn.transitions if to == State.CLOSED) == 1


async def test_close_from_idle_is_safe() -> None:
    # BEGIN: a brand-new connection that never opened
    transport = ScriptedTransport()
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: closing without opening
    await conn.close()

    # THEN: the transition still happens (IDLE → CLOSED) and there's no
    #       writer to interact with
    assert conn.state == State.CLOSED
    assert conn.transitions[-1][0] == State.IDLE
    assert conn.transitions[-1][1] == State.CLOSED


async def test_close_from_broken_does_not_resurrect_writer() -> None:
    # BEGIN: a connection that hit a transport error and is BROKEN
    async def boom(
        _host: str, _port: int, _ssl_context: ssl.SSLContext | None
    ) -> Never:
        raise ConnectionRefusedError("nope")

    conn = Connection([("h", 9000)], transport_factory=boom)
    with pytest.raises(ConnectionRefusedError):
        await conn.open()
    assert conn.state == State.BROKEN

    # WHEN: closing the broken connection
    await conn.close()

    # THEN: it transitions BROKEN → CLOSED without raising
    assert conn.state == State.CLOSED


# ---- mock transport sanity check ---------------------------------------


async def test_mock_transport_carries_handshake_traffic_both_directions() -> None:
    # BEGIN: a connection ready to handshake against a scripted server
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: running the handshake
    await conn.open(user="u", password="p", database="db")

    # THEN: the scripted server consumed the queued Hello reply and the
    #       transport captured what we wrote (the client Hello packet);
    #       a non-trivial number of bytes flowed in each direction
    assert conn.state == State.READY
    assert len(transport.written()) > 0
    # First written byte is the ClientPacket.HELLO varuint id (0x00).
    assert transport.written()[0] == 0


