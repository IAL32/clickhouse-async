"""Cancellation tests for ``Connection.cancel()``."""

from __future__ import annotations

import asyncio

import pytest

from clickhouse_async.connection import Connection, State
from clickhouse_async.errors import QueryCancellationError
from clickhouse_async.protocol.io import AsyncBinaryReader
from clickhouse_async.protocol.packets import ClientPacket

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_end_of_stream,
    encode_server_exception,
    encode_server_hello,
    encode_server_progress,
)


async def _connect_in_flight(transport: ScriptedTransport) -> Connection:
    """Open a connection past handshake and dispatch a SELECT so it
    sits in IN_FLIGHT, ready to be cancelled."""
    transport.feed(encode_server_hello())
    conn = Connection("h", 9000, transport_factory=transport)
    await conn.open()
    await conn.send_query("SELECT 1")
    return conn


def _bytes_at_or_after(haystack: bytes, needle: bytes, *, after: int) -> bool:
    return haystack.find(needle, after) != -1


# ---- READY: no-op -------------------------------------------------------


async def test_cancel_from_ready_is_a_silent_noop() -> None:
    # BEGIN: a connected READY connection (no query in flight)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection("h", 9000, transport_factory=transport)
    await conn.open()
    assert conn.state == State.READY
    pre = len(transport.written())

    # WHEN: calling cancel
    await conn.cancel()

    # THEN: no exception, state unchanged, no bytes written
    assert conn.state == State.READY
    assert len(transport.written()) == pre


# ---- IN_FLIGHT: clean drain --------------------------------------------


async def test_cancel_mid_flight_drains_and_returns_to_ready() -> None:
    # BEGIN: a connection mid-query with EndOfStream queued as the
    #        server's cancel-ack
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)
    transport.feed(encode_server_end_of_stream())
    pre = len(transport.written())

    # WHEN: cancelling
    with pytest.raises(QueryCancellationError) as exc_info:
        await conn.cancel()

    # THEN: outcome is "drained", connection is READY, and we wrote at
    #       least one byte (the Cancel packet id) after the previous
    #       captured offset
    assert exc_info.value.reason == "drained"
    assert conn.state == State.READY
    assert len(transport.written()) > pre


async def test_cancel_packet_emitted_is_a_bare_varuint_id() -> None:
    # BEGIN: a connection mid-query with EndOfStream queued
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)
    transport.feed(encode_server_end_of_stream())
    pre = len(transport.written())

    # WHEN: cancelling
    with pytest.raises(QueryCancellationError):
        await conn.cancel()

    # THEN: the very first byte written after the Query packet is
    #       ClientPacket.CANCEL, with no following body bytes belonging
    #       to it (the next bytes would be a follow-up packet, but in
    #       this test there are none).
    written = transport.written()[pre:]
    rdr = AsyncBinaryReader(_eof_stream(written))
    assert await rdr.read_varuint() == ClientPacket.CANCEL


async def test_cancel_swallows_server_exception_as_clean_ack() -> None:
    # BEGIN: a connection mid-query where the server replies to Cancel
    #        with an Exception (e.g. QUERY_WAS_CANCELLED) instead of
    #        EndOfStream — both are clean-drain paths
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)
    transport.feed(
        encode_server_exception(
            code=394,
            name="QUERY_WAS_CANCELLED",
            display_text="Query was cancelled.",
        )
    )

    # WHEN: cancelling
    with pytest.raises(QueryCancellationError) as exc_info:
        await conn.cancel()

    # THEN: the outcome is "drained" (not the ServerError), and the
    #       connection is READY
    assert exc_info.value.reason == "drained"
    assert conn.state == State.READY


async def test_cancel_drain_processes_progress_callbacks() -> None:
    # BEGIN: a connection mid-query whose post-Cancel drain queues a
    #        Progress packet ahead of the EndOfStream
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)
    progress_log: list[int] = []
    conn.on_progress = lambda p: progress_log.append(p.read_rows)
    transport.feed(encode_server_progress(read_rows=99))
    transport.feed(encode_server_end_of_stream())

    # WHEN: cancelling
    with pytest.raises(QueryCancellationError):
        await conn.cancel()

    # THEN: the progress callback fired during the drain; the cancel
    #       didn't suppress side effects of post-Cancel packets
    assert progress_log == [99]
    assert conn.state == State.READY


# ---- IN_FLIGHT: timeout path ------------------------------------------


async def test_cancel_drain_timeout_marks_connection_broken() -> None:
    # BEGIN: a connection mid-query with no server response queued — the
    #        drain will block until the timeout fires
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)

    # WHEN: cancelling with a short timeout
    with pytest.raises(QueryCancellationError) as exc_info:
        await conn.cancel(drain_timeout=0.05)

    # THEN: outcome is "timeout", connection is BROKEN, writer is closed
    assert exc_info.value.reason == "timeout"
    assert conn.state == State.BROKEN
    assert transport.writer_closed()


# ---- already_cancelled --------------------------------------------------


async def test_concurrent_cancel_raises_already_cancelled() -> None:
    # BEGIN: a connection mid-query; first cancel will block waiting for
    #        the drain (no bytes queued yet)
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)

    first = asyncio.create_task(conn.cancel(drain_timeout=10.0))
    # Yield control so first reaches the drain await
    await asyncio.sleep(0)

    # WHEN: a second cancel runs while first is awaiting drain
    with pytest.raises(QueryCancellationError) as exc_info:
        await conn.cancel()

    # THEN: the second sees "already_cancelled", first is still in flight
    assert exc_info.value.reason == "already_cancelled"
    assert not first.done()

    # Cleanup: feed EOS so first completes, then await it
    transport.feed(encode_server_end_of_stream())
    with pytest.raises(QueryCancellationError) as first_exc:
        await first
    assert first_exc.value.reason == "drained"


# ---- BROKEN / CLOSED ----------------------------------------------------


async def test_cancel_from_broken_raises_not_in_flight() -> None:
    # BEGIN: a connection that was forced into BROKEN by a transport-time
    #        cancel timeout
    transport = ScriptedTransport()
    conn = await _connect_in_flight(transport)
    with pytest.raises(QueryCancellationError):
        await conn.cancel(drain_timeout=0.05)
    assert conn.state == State.BROKEN

    # WHEN / THEN: cancelling the BROKEN connection raises with reason
    #             "not_in_flight" naming the current state
    with pytest.raises(QueryCancellationError) as exc_info:
        await conn.cancel()
    assert exc_info.value.reason == "not_in_flight"
    assert "BROKEN" in str(exc_info.value)


async def test_cancel_from_idle_raises_not_in_flight() -> None:
    # BEGIN: a brand-new IDLE connection (never opened)
    transport = ScriptedTransport()
    conn = Connection("h", 9000, transport_factory=transport)
    assert conn.state == State.IDLE

    # WHEN / THEN: cancelling raises with reason "not_in_flight"
    with pytest.raises(QueryCancellationError) as exc_info:
        await conn.cancel()
    assert exc_info.value.reason == "not_in_flight"
    assert "IDLE" in str(exc_info.value)


# ---- helpers -----------------------------------------------------------


def _eof_stream(data: bytes) -> asyncio.StreamReader:
    s = asyncio.StreamReader()
    s.feed_data(data)
    s.feed_eof()
    return s
