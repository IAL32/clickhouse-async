"""Tests for Client.fetch_columns and Client.iter_column_blocks."""

from __future__ import annotations

import clickhouse_async as ch
from clickhouse_async import ColumnarBlock, ColumnarResult, connect
from clickhouse_async.pool import create_pool
from clickhouse_async.protocol.block import Block, BlockInfo, make_column

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
)


def _two_col_response(transport: ScriptedTransport) -> None:
    """Queue a header + one data block with 2 columns and 3 rows."""
    spec_id, _ = make_column("id", "UInt32", [])
    spec_name, _ = make_column("name", "String", [])
    header = Block(
        info=BlockInfo(), columns=[spec_id, spec_name], n_rows=0, data=[[], []]
    )
    data = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name],
        n_rows=3,
        data=[[1, 2, 3], ["a", "b", "c"]],
    )
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())


# ---- fetch_columns() returns a ColumnarResult ---------------------------


async def test_fetch_columns_returns_columnar_result() -> None:
    # BEGIN: a scripted two-column SELECT
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_col_response(transport)

    # WHEN: running fetch_columns()
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.fetch_columns("SELECT id, name FROM t")

    # THEN: result is ColumnarResult with column-major data — no row transpose
    assert isinstance(result, ColumnarResult)
    assert [c.name for c in result.columns] == ["id", "name"]
    assert result.data[0] == [1, 2, 3]
    assert result.data[1] == ["a", "b", "c"]
    assert result.rows == 3


async def test_fetch_columns_skips_empty_header_block() -> None:
    # BEGIN: a response whose first block has n_rows=0
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int8", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[7, 8]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running fetch_columns()
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.fetch_columns("SELECT n FROM t")

    # THEN: rows counts only data rows, not the header block
    assert result.rows == 2
    assert result.data[0] == [7, 8]


async def test_fetch_columns_concatenates_multiple_blocks() -> None:
    # BEGIN: three data blocks for the same column
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("x", "Float32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    block1 = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[1.0, 2.0]])
    block2 = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[3.0]])
    block3 = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[[4.0, 5.0, 6.0]])
    for b in (header, block1, block2, block3):
        transport.feed(encode_server_data(b))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running fetch_columns()
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.fetch_columns("SELECT x FROM t")

    # THEN: all three blocks concatenated into a single column list
    assert result.rows == 6
    assert result.data[0] == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]


async def test_fetch_columns_data_is_not_row_major() -> None:
    # BEGIN: a two-column response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_col_response(transport)

    # WHEN: running fetch_columns()
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.fetch_columns("SELECT id, name FROM t")

    # THEN: data is a list of per-column lists, not a list of row tuples
    assert isinstance(result.data, list)
    assert all(isinstance(col, list) for col in result.data)
    assert not any(isinstance(v, tuple) for col in result.data for v in col)


# ---- iter_column_blocks() yields ColumnarBlock --------------------------


async def test_iter_column_blocks_yields_one_per_block() -> None:
    # BEGIN: three data blocks with varying row counts
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    block1 = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[1, 2]])
    block2 = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[3]])
    block3 = Block(info=BlockInfo(), columns=[spec], n_rows=4, data=[[4, 5, 6, 7]])
    for b in (header, block1, block2, block3):
        transport.feed(encode_server_data(b))
    transport.feed(encode_server_end_of_stream())

    # WHEN: iterating with iter_column_blocks()
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        blocks: list[ColumnarBlock] = [
            b async for b in client.iter_column_blocks("SELECT n FROM t")
        ]

    # THEN: header excluded; three ColumnarBlock values with correct row counts
    assert len(blocks) == 3
    assert all(isinstance(b, ColumnarBlock) for b in blocks)
    assert [b.n_rows for b in blocks] == [2, 1, 4]


async def test_iter_column_blocks_data_is_column_major() -> None:
    # BEGIN: a two-column, one-block response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_col_response(transport)

    # WHEN: consuming the single data block
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        blocks: list[ColumnarBlock] = [
            b async for b in client.iter_column_blocks("SELECT id, name FROM t")
        ]

    # THEN: block.data[0] is the id column, block.data[1] is the name column
    assert len(blocks) == 1
    block = blocks[0]
    assert [c.name for c in block.columns] == ["id", "name"]
    assert block.data[0] == [1, 2, 3]
    assert block.data[1] == ["a", "b", "c"]


async def test_iter_column_blocks_columns_is_tuple() -> None:
    # BEGIN: a one-column response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("v", "Int8", [])
    transport.feed(
        encode_server_data(Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]]))
    )
    transport.feed(
        encode_server_data(
            Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[42]])
        )
    )
    transport.feed(encode_server_end_of_stream())

    # WHEN: consuming the block
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        async for block in client.iter_column_blocks("SELECT v FROM t"):
            # THEN: columns is a tuple (immutable), not a list
            assert isinstance(block.columns, tuple)


# ---- pool pass-throughs -------------------------------------------------


async def test_pool_fetch_columns_pass_through() -> None:
    # BEGIN: pool backed by a scripted transport
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_col_response(transport)

    # WHEN: calling fetch_columns on the pool (one-shot acquire+release)
    async with create_pool(
        "clickhouse://default:@host/db",
        max_size=1,
        transport_factory=transport,
        enable_reaper=False,
    ) as pool:
        result = await pool.fetch_columns("SELECT id, name FROM t")

    # THEN: result is a ColumnarResult from the acquired client
    assert isinstance(result, ColumnarResult)
    assert result.rows == 3
    assert result.data[0] == [1, 2, 3]
    assert result.data[1] == ["a", "b", "c"]


async def test_pool_fetch_columns_is_in_public_api() -> None:
    # THEN: fetch_columns and iter_column_blocks are accessible from the
    #       top-level package so callers need only `import clickhouse_async`
    assert hasattr(ch, "ColumnarBlock")
    assert hasattr(ch, "ColumnarResult")
    assert hasattr(ch.Pool, "fetch_columns")
    assert hasattr(ch.Pool, "iter_column_blocks")


async def test_pool_iter_column_blocks_pass_through() -> None:
    # BEGIN: pool backed by a scripted transport with two data blocks
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    block1 = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[10, 20]])
    block2 = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[30]])
    for b in (header, block1, block2):
        transport.feed(encode_server_data(b))
    transport.feed(encode_server_end_of_stream())

    # WHEN: streaming column blocks through the pool
    async with create_pool(
        "clickhouse://default:@host/db",
        max_size=1,
        transport_factory=transport,
        enable_reaper=False,
    ) as pool:
        blocks: list[ColumnarBlock] = [
            b async for b in pool.iter_column_blocks("SELECT n FROM t")
        ]

    # THEN: both data blocks yielded with correct n_rows
    assert len(blocks) == 2
    assert blocks[0].n_rows == 2
    assert blocks[0].data[0] == [10, 20]
    assert blocks[1].n_rows == 1
    assert blocks[1].data[0] == [30]
