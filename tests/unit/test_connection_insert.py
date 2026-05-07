"""Tests for the INSERT data path.

The Connection-level surface is just `send_data(block | None)` plus
the existing `iter_packets` / `send_query` machinery. Orchestration
(consume header → send batches → terminator → drain) lives at the
Client layer; these tests cover the wire bytes and state behaviour
of the building blocks.
"""

from __future__ import annotations

import pytest

from clickhouse_async.connection import Connection, State
from clickhouse_async.protocol.block import (
    Block,
    BlockInfo,
    make_column,
    read_block,
    write_block,
)
from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.protocol.packets import OUR_REVISION, ClientPacket

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
)


def _reader_over(data: bytes) -> SyncBinaryReader:
    return SyncBinaryReader(bytes(data))


async def _open_in_flight(transport: ScriptedTransport) -> Connection:
    """Open a connection and put it in IN_FLIGHT for an INSERT-shaped
    test. Server replies with a header block as the first response so
    callers can immediately consume it via `iter_packets`."""
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()
    await conn.send_query("INSERT INTO t VALUES")
    return conn


# ---- send_data byte layout --------------------------------------------


async def test_send_data_writes_packet_id_empty_name_then_block() -> None:
    # BEGIN: a connection in IN_FLIGHT and a known block
    transport = ScriptedTransport()
    conn = await _open_in_flight(transport)
    pre = len(transport.written())
    spec, vals = make_column("id", "Int32", [1, 2, 3])
    block = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[vals])

    # WHEN: sending the block
    await conn.send_data(block)
    after = transport.written()[pre:]

    # THEN: the bytes start with ClientPacket.DATA (varuint), an empty
    #       external-table-name string, then a write_block round-trip
    rdr = _reader_over(after)
    assert rdr.read_varuint() == ClientPacket.DATA
    assert rdr.read_string() == ""
    decoded = read_block(rdr, revision=OUR_REVISION)
    assert decoded.n_rows == 3
    assert decoded.data == [[1, 2, 3]]
    assert decoded.columns[0].name == "id"


async def test_send_data_none_writes_empty_terminator_block() -> None:
    # BEGIN: a connection in IN_FLIGHT
    transport = ScriptedTransport()
    conn = await _open_in_flight(transport)
    pre = len(transport.written())

    # WHEN: sending the terminator
    await conn.send_data(None)
    after = transport.written()[pre:]

    # THEN: the suffix matches an empty-Block Data packet exactly —
    #       same shape as the trailing block in the Query packet
    expected = BinaryWriter()
    expected.write_varuint(ClientPacket.DATA)
    expected.write_string("")
    write_block(
        expected,
        Block(info=BlockInfo(), columns=[], n_rows=0, data=[]),
        revision=conn.negotiated_revision,
    )
    assert after == expected.getvalue()


# ---- state guards -----------------------------------------------------


async def test_send_data_from_ready_raises() -> None:
    # BEGIN: a connection that just completed handshake but has no query in flight
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()
    assert conn.state == State.READY

    # WHEN: trying to send_data without a query in flight
    # THEN: a RuntimeError surfaces naming the current state
    with pytest.raises(RuntimeError, match="IN_FLIGHT"):
        await conn.send_data(None)


async def test_send_data_from_idle_raises() -> None:
    # BEGIN: a brand-new connection that never opened
    transport = ScriptedTransport()
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN / THEN: send_data without opening
    with pytest.raises(RuntimeError, match="IDLE"):
        await conn.send_data(None)


async def test_send_data_does_not_change_state() -> None:
    # BEGIN: a connection in IN_FLIGHT
    transport = ScriptedTransport()
    conn = await _open_in_flight(transport)

    # WHEN: sending several data blocks plus the terminator
    spec, vals = make_column("id", "Int32", [1])
    block = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[vals])
    await conn.send_data(block)
    await conn.send_data(block)
    await conn.send_data(None)

    # THEN: the connection stays IN_FLIGHT — state moves to READY only
    #       when the iterator sees EndOfStream
    assert conn.state == State.IN_FLIGHT


# ---- end-to-end INSERT-like sequence ----------------------------------


async def test_full_insert_sequence_returns_to_ready() -> None:
    # BEGIN: a connection where the server sends a header block, then
    #        (after our data + terminator) emits EndOfStream
    transport = ScriptedTransport()
    conn = await _open_in_flight(transport)
    spec, _ = make_column("id", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_end_of_stream())

    # WHEN: orchestrating the INSERT — consume header, send a batch,
    #       send terminator, drain remaining packets
    iterator = conn.iter_packets()
    first = await anext(iterator)
    assert first.kind == "data"
    assert first.block.n_rows == 0
    assert first.block.columns[0].name == "id"

    spec_send, vals = make_column("id", "Int32", [10, 20])
    batch = Block(info=BlockInfo(), columns=[spec_send], n_rows=2, data=[vals])
    await conn.send_data(batch)
    await conn.send_data(None)

    remaining = [s async for s in iterator]

    # THEN: no more block packets after the header, the connection
    #       returns to READY (reusable for the next query)
    assert remaining == []
    assert conn.state == State.READY


async def test_send_data_block_round_trips_via_scripted_loopback() -> None:
    # BEGIN: a connection in IN_FLIGHT, a non-trivial block to send, and
    #        a fresh reader over the captured bytes
    transport = ScriptedTransport()
    conn = await _open_in_flight(transport)
    pre = len(transport.written())

    s1, v1 = make_column("id", "UInt64", [1, 2, 3])
    s2, v2 = make_column("name", "String", ["alpha", "beta", "gamma"])
    block = Block(info=BlockInfo(), columns=[s1, s2], n_rows=3, data=[v1, v2])

    # WHEN: sending the block and decoding what we wrote
    await conn.send_data(block)
    rdr = _reader_over(transport.written()[pre:])
    assert rdr.read_varuint() == ClientPacket.DATA
    assert rdr.read_string() == ""
    decoded = read_block(rdr, revision=conn.negotiated_revision)

    # THEN: the bytes round-trip back to a block with the same columns
    #       and values
    assert [c.name for c in decoded.columns] == ["id", "name"]
    assert decoded.data == [[1, 2, 3], ["alpha", "beta", "gamma"]]
