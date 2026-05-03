"""Tests for the steady-state packet-loop dispatch.

``iter_packets`` handles every server packet a SELECT can produce —
yields ``DATA`` / ``TOTALS`` / ``EXTREMES`` as ``StreamedBlock``,
fires the matching callback for non-yielded packets (Progress,
ProfileInfo, Log, TableColumns, TimezoneUpdate, ProfileEvents), and
terminates on ``END_OF_STREAM`` (READY) or ``EXCEPTION`` (raises).
"""

from __future__ import annotations

import pytest

from clickhouse_async.connection import Connection, State, StreamedBlock
from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.block import Block, BlockInfo, make_column
from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS,
    OUR_REVISION,
    ServerPacket,
)
from clickhouse_async.protocol.server_packets import ProfileInfo, ProgressInfo

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_block_packet,
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
    encode_server_profile_info,
    encode_server_progress,
    encode_server_table_columns,
    encode_server_timezone_update,
)


async def _connect_and_send_query(transport: ScriptedTransport) -> Connection:
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()
    await conn.send_query("SELECT 1")
    return conn


# ---- Progress callback ---------------------------------------------------


async def test_progress_callback_fires_with_decoded_values() -> None:
    # BEGIN: a connected connection with a Progress packet queued before
    #        EndOfStream, plus a callback recording every fired value
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    received: list[ProgressInfo] = []
    conn.on_progress = received.append
    transport.feed(
        encode_server_progress(
            read_rows=100,
            read_bytes=1234,
            total_rows_to_read=1000,
            total_bytes_to_read=12340,
            written_rows=10,
            written_bytes=200,
            elapsed_ns=5_000_000,
        )
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining the iterator
    blocks = [s async for s in conn.iter_packets()]

    # THEN: no blocks were yielded, the callback fired once with every
    #       field populated at OUR_REVISION's gates, and the connection
    #       returned to READY
    assert blocks == []
    assert len(received) == 1
    p = received[0]
    assert p.read_rows == 100
    assert p.read_bytes == 1234
    assert p.total_rows_to_read == 1000
    assert p.total_bytes_to_read == 12340
    assert p.written_rows == 10
    assert p.written_bytes == 200
    assert p.elapsed_ns == 5_000_000
    assert conn.state == State.READY


async def test_progress_at_pre_total_bytes_revision_omits_late_fields() -> None:
    # BEGIN: a connection negotiated at a revision below
    #        TOTAL_BYTES_IN_PROGRESS (54463) — queue a Progress encoded
    #        for that older shape
    older_revision = DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS - 1
    transport = ScriptedTransport()
    transport.feed(encode_server_hello(revision=older_revision))
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()
    await conn.send_query("SELECT 1")
    received: list[ProgressInfo] = []
    conn.on_progress = received.append
    transport.feed(
        encode_server_progress(
            revision=older_revision,
            read_rows=7,
            read_bytes=70,
            total_rows_to_read=42,
        )
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    [s async for s in conn.iter_packets()]

    # THEN: the callback fires with the older-shape fields populated and
    #       the gated fields default to zero
    p = received[0]
    assert p.read_rows == 7
    assert p.total_rows_to_read == 42
    assert p.total_bytes_to_read == 0
    assert p.written_rows == 0
    assert p.elapsed_ns == 0


async def test_progress_without_callback_is_silently_consumed() -> None:
    # BEGIN: a connection with no progress callback and a Progress
    #        packet queued
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    transport.feed(encode_server_progress(read_rows=5))
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining the iterator
    blocks = [s async for s in conn.iter_packets()]

    # THEN: the loop completes cleanly — the Progress body was consumed
    #       even without a callback to receive it
    assert blocks == []
    assert conn.state == State.READY


# ---- ProfileInfo callback ----------------------------------------------


async def test_profile_info_callback_fires_with_decoded_values() -> None:
    # BEGIN: a connection with a ProfileInfo packet queued before EOS
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    received: list[ProfileInfo] = []
    conn.on_profile_info = received.append
    transport.feed(
        encode_server_profile_info(
            rows=42,
            blocks=3,
            bytes_=1024,
            applied_limit=True,
            rows_before_limit=100,
            applied_aggregation=True,
            rows_before_aggregation=200,
        )
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    [s async for s in conn.iter_packets()]

    # THEN: the callback fired with every documented field
    assert len(received) == 1
    pi = received[0]
    assert pi.rows == 42
    assert pi.blocks == 3
    assert pi.bytes == 1024
    assert pi.applied_limit is True
    assert pi.rows_before_limit == 100
    assert pi.applied_aggregation is True
    assert pi.rows_before_aggregation == 200


# ---- Block-bearing callback packets (Log / ProfileEvents) -------------


async def test_log_callback_receives_block() -> None:
    # BEGIN: a connection with a LOG packet queued (a 0-row block stands
    #        in for an empty log batch — the wire shape is the same)
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    received: list[Block] = []
    conn.on_log = received.append
    spec, _ = make_column("event_time", "Int64", [])
    log_block = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    transport.feed(
        encode_server_block_packet(ServerPacket.LOG, log_block)
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    blocks = [s async for s in conn.iter_packets()]

    # THEN: nothing yielded, the callback received the log block
    assert blocks == []
    assert len(received) == 1
    assert received[0].columns[0].name == "event_time"


async def test_profile_events_callback_receives_block() -> None:
    # BEGIN: a connection with a PROFILE_EVENTS packet queued
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    received: list[Block] = []
    conn.on_profile_events = received.append
    spec, _ = make_column("name", "String", [])
    pe_block = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    transport.feed(
        encode_server_block_packet(ServerPacket.PROFILE_EVENTS, pe_block)
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    [s async for s in conn.iter_packets()]

    # THEN: callback fired with the events block
    assert len(received) == 1
    assert received[0].columns[0].name == "name"


# ---- TableColumns / TimezoneUpdate -------------------------------------


async def test_table_columns_callback_fires_with_two_strings() -> None:
    # BEGIN: a connection with a TABLE_COLUMNS packet queued
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    received: list[tuple[str, str]] = []
    conn.on_table_columns = lambda name, cols: received.append((name, cols))
    transport.feed(
        encode_server_table_columns(
            default_table_name="",
            columns="`id` UInt64\n`name` String",
        )
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    [s async for s in conn.iter_packets()]

    # THEN: the callback received both strings as documented
    assert len(received) == 1
    name, cols = received[0]
    assert name == ""
    assert "UInt64" in cols
    assert "String" in cols


async def test_timezone_update_callback_fires_with_string() -> None:
    # BEGIN: a connection with a TIMEZONE_UPDATE packet queued
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    received: list[str] = []
    conn.on_timezone_update = received.append
    transport.feed(encode_server_timezone_update("Europe/Madrid"))
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    [s async for s in conn.iter_packets()]

    # THEN: the callback received the tz string verbatim
    assert received == ["Europe/Madrid"]


# ---- Totals / Extremes are yielded with kind tags ---------------------


async def test_totals_and_extremes_yield_streamed_blocks_with_their_kind() -> None:
    # BEGIN: a connection queued to emit DATA → TOTALS → EXTREMES → EOS
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    spec, _ = make_column("x", "Int32", [])
    data_block = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[1]])
    totals_block = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[42]])
    extremes_block = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[0, 99]])
    transport.feed(encode_server_data(data_block))
    transport.feed(
        encode_server_block_packet(ServerPacket.TOTALS, totals_block)
    )
    transport.feed(
        encode_server_block_packet(ServerPacket.EXTREMES, extremes_block)
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    streamed = [s async for s in conn.iter_packets()]

    # THEN: each block is tagged with its kind, in declared order
    assert [s.kind for s in streamed] == ["data", "totals", "extremes"]
    assert streamed[1].block.data == [[42]]
    assert streamed[2].block.data == [[0, 99]]


# ---- Mixed real-world flow ----------------------------------------------


async def test_realistic_query_flow_routes_packets_through_callbacks() -> None:
    # BEGIN: a connection scripted with a realistic sequence — a
    #        TimezoneUpdate, a header DATA block, a Progress, the data
    #        DATA, a ProfileInfo, then EndOfStream
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    progress_log: list[ProgressInfo] = []
    profile_log: list[ProfileInfo] = []
    tz_log: list[str] = []
    conn.on_progress = progress_log.append
    conn.on_profile_info = profile_log.append
    conn.on_timezone_update = tz_log.append

    spec, _ = make_column("number", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[7]])
    transport.feed(encode_server_timezone_update("UTC"))
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_progress(read_rows=1))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_profile_info(rows=1, blocks=2, bytes_=64))
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining the iterator
    streamed = [s async for s in conn.iter_packets()]

    # THEN: only DATA blocks come out; every callback fired once with
    #       the right values; the connection returns to READY
    assert [s.kind for s in streamed] == ["data", "data"]
    assert streamed[1].block.data == [[7]]
    assert tz_log == ["UTC"]
    assert len(progress_log) == 1 and progress_log[0].read_rows == 1
    assert len(profile_log) == 1 and profile_log[0].bytes == 64
    assert conn.state == State.READY


# ---- Distributed-read packets are rejected -----------------------------


@pytest.mark.parametrize(
    "packet_id",
    [
        ServerPacket.PART_UUIDS,
        ServerPacket.READ_TASK_REQUEST,
        ServerPacket.MERGE_TREE_ALL_RANGES_ANNOUNCEMENT,
        ServerPacket.MERGE_TREE_READ_TASK_REQUEST,
    ],
)
async def test_distributed_read_packets_break_the_connection(
    packet_id: ServerPacket,
) -> None:
    # BEGIN: a connection emitting a packet that only initial-query
    #        connections shouldn't see — initial-query connections
    #        don't drive distributed reads
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    w = BinaryWriter()
    w.write_varuint(packet_id)
    transport.feed(w.getvalue())

    # WHEN / THEN: the connection refuses to skip the body and is BROKEN
    with pytest.raises(ProtocolError, match="distributed-read"):
        async for _ in conn.iter_packets():
            pass
    assert conn.state == State.BROKEN


# ---- Streamed iteration over a callback-only stream returns READY ------


async def test_callback_only_stream_returns_to_ready_with_no_yields() -> None:
    # BEGIN: a connection emitting only callback-style packets before EOS
    transport = ScriptedTransport()
    conn = await _connect_and_send_query(transport)
    transport.feed(encode_server_progress())
    transport.feed(encode_server_progress())
    transport.feed(encode_server_profile_info())
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    streamed: list[StreamedBlock] = [s async for s in conn.iter_packets()]

    # THEN: nothing yielded, connection back to READY ready for the next
    #       query (no half-broken state because we saw no DATA)
    assert streamed == []
    assert conn.state == State.READY


# ---- Negotiated revision is used for block decoding -------------------


async def test_iter_packets_passes_negotiated_revision_to_block_reader() -> None:
    # BEGIN: a connection where server claimed a revision below us so
    #        the negotiated revision is the older one — the encoded
    #        Data packet must use the same revision for the block to
    #        round-trip
    older_revision = OUR_REVISION - 30
    transport = ScriptedTransport()
    transport.feed(encode_server_hello(revision=older_revision))
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()
    await conn.send_query("SELECT 1")
    spec, vals = make_column("x", "Int32", [1, 2, 3])
    block = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[vals])
    transport.feed(encode_server_data(block, revision=older_revision))
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    streamed = [s async for s in conn.iter_packets()]

    # THEN: the block round-tripped at the negotiated revision (would
    #       have desynced if iter_packets had used OUR_REVISION instead)
    assert conn.negotiated_revision == older_revision
    assert streamed[0].block.data == [[1, 2, 3]]
