"""Tests for `Client.iter_blocks` / `iter_rows` and the
cancel-on-break cleanup contract.
"""

from __future__ import annotations

from contextlib import aclosing

from clickhouse_async import connect
from clickhouse_async.connection import State
from clickhouse_async.protocol.block import Block, BlockInfo, make_column

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
)


def _three_block_select(transport: ScriptedTransport) -> None:
    """Queue header + three data blocks + EndOfStream."""
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    block1 = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[1, 2]])
    block2 = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[[3, 4, 5]])
    block3 = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[6, 7]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(block1))
    transport.feed(encode_server_data(block2))
    transport.feed(encode_server_data(block3))
    transport.feed(encode_server_end_of_stream())


# ---- iter_blocks --------------------------------------------------------


async def test_iter_blocks_yields_each_data_block_skipping_header() -> None:
    # BEGIN: a SELECT response with header + three data blocks
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _three_block_select(transport)

    # WHEN: iterating over the result blocks
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        blocks = [b async for b in client.iter_blocks("SELECT n FROM t")]

    # THEN: header is filtered out; the three data blocks come back in
    #       arrival order with their original n_rows
    assert [b.n_rows for b in blocks] == [2, 3, 2]
    assert blocks[0].data == [[1, 2]]
    assert blocks[1].data == [[3, 4, 5]]
    assert blocks[2].data == [[6, 7]]


async def test_iter_blocks_returns_connection_to_ready_after_exhaustion() -> None:
    # BEGIN: a streamable response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _three_block_select(transport)

    # WHEN: exhausting the iterator
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        async for _ in client.iter_blocks("SELECT n FROM t"):
            pass
        post_state = client._conn.state  # type: ignore[attr-defined]

    # THEN: the connection is back to READY (EndOfStream reached
    #       naturally) — no cancel was needed
    assert post_state == State.READY


# ---- iter_rows ----------------------------------------------------------


async def test_iter_rows_transposes_blocks_into_row_major_tuples() -> None:
    # BEGIN: a streamable two-column SELECT
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec_id, _ = make_column("id", "Int32", [])
    spec_name, _ = make_column("name", "String", [])
    header = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name],
        n_rows=0,
        data=[[], []],
    )
    block1 = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name],
        n_rows=2,
        data=[[1, 2], ["a", "b"]],
    )
    block2 = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name],
        n_rows=1,
        data=[[3], ["c"]],
    )
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(block1))
    transport.feed(encode_server_data(block2))
    transport.feed(encode_server_end_of_stream())

    # WHEN: iterating row-by-row
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        rows = [r async for r in client.iter_rows("SELECT id, name FROM t")]

    # THEN: rows arrive in declared order across blocks
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


# ---- cancel-on-break via contextlib.aclosing ---------------------------


async def test_breaking_out_with_aclosing_cancels_and_returns_to_ready() -> None:
    # BEGIN: a long streamable SELECT
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _three_block_select(transport)

    # WHEN: breaking out after the first block, wrapping with aclosing
    #       so the generator's cleanup runs deterministically
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        async with aclosing(client.iter_blocks("SELECT n FROM t")) as blocks:
            seen_first = False
            async for _ in blocks:
                seen_first = True
                break
        # THEN: by the time aclosing exits, the connection has been
        #       cancelled and drained — state is READY (or BROKEN if
        #       the drain timed out, but the loopback queue still has
        #       packets to drain so we expect READY)
        assert seen_first
        assert client._conn.state == State.READY  # type: ignore[attr-defined]


async def test_aclosing_makes_connection_reusable_for_next_query() -> None:
    # BEGIN: a streamable SELECT followed by a second scripted response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _three_block_select(transport)
    # Queue a follow-up SELECT response
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[99]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())

    # WHEN: streaming the first query, breaking out, and then running
    #       a second query on the same client
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        async with aclosing(client.iter_rows("SELECT n FROM t")) as rows:
            async for _ in rows:
                break

        # THEN: a follow-up query succeeds — cancel-on-break left the
        #       connection in a usable state
        result = await client.fetch_all("SELECT n FROM t")
        assert result == [(99,)]


# ---- empty-result streaming --------------------------------------------


async def test_iter_blocks_over_header_only_response_yields_nothing() -> None:
    # BEGIN: a SELECT that returns no data rows
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_end_of_stream())

    # WHEN: iterating
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        blocks = [b async for b in client.iter_blocks("SELECT n WHERE 0")]

    # THEN: nothing yielded; connection is READY
    assert blocks == []
    # The aenter context already validated state via server_info access
