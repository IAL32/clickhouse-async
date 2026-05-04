"""Tests for the column_factories hook on Client and Pool."""

from __future__ import annotations

from clickhouse_async import connect
from clickhouse_async.pool import create_pool
from clickhouse_async.protocol.block import Block, BlockInfo, make_column

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
)


def _uint32_block(values: list[int]) -> Block:
    spec, _ = make_column("n", "UInt32", [])
    return Block(
        info=BlockInfo(),
        columns=[spec],
        n_rows=len(values),
        data=[values],
    )


def _header_block() -> Block:
    spec, _ = make_column("n", "UInt32", [])
    return Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])


# ---- fetch_columns applies factory to matching column -------------------


async def test_fetch_columns_applies_factory_to_matching_column() -> None:
    # BEGIN: a one-column UInt32 response with factory registered
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_data(_uint32_block([10, 20, 30])))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running fetch_columns with a tuple factory for UInt32
    async with connect(
        "clickhouse://default:@host/db",
        transport_factory=transport,
        column_factories={"UInt32": tuple},
    ) as client:
        result = await client.fetch_columns("SELECT n FROM t")

    # THEN: the column data is a tuple, not a list
    assert isinstance(result.data[0], tuple)
    assert result.data[0] == (10, 20, 30)


async def test_fetch_columns_no_factory_leaves_list() -> None:
    # BEGIN: a one-column response with no factories registered
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_data(_uint32_block([1, 2, 3])))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running fetch_columns without column_factories
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.fetch_columns("SELECT n FROM t")

    # THEN: the column data remains a plain list
    assert isinstance(result.data[0], list)
    assert result.data[0] == [1, 2, 3]


async def test_factory_for_unknown_type_is_silently_skipped() -> None:
    # BEGIN: a UInt32 column with a factory only for a different type
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_data(_uint32_block([5, 6])))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running fetch_columns with factories that don't match UInt32
    async with connect(
        "clickhouse://default:@host/db",
        transport_factory=transport,
        column_factories={"Int64": tuple},
    ) as client:
        result = await client.fetch_columns("SELECT n FROM t")

    # THEN: unmatched column stays as list
    assert isinstance(result.data[0], list)
    assert result.data[0] == [5, 6]


# ---- iter_column_blocks applies factory per block -----------------------


async def test_iter_column_blocks_applies_factory_per_block() -> None:
    # BEGIN: two data blocks with a tuple factory
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_data(_uint32_block([1, 2])))
    transport.feed(encode_server_data(_uint32_block([3, 4, 5])))
    transport.feed(encode_server_end_of_stream())

    calls: list[list] = []

    def tracking_tuple(values: list) -> tuple:
        calls.append(values)
        return tuple(values)

    # WHEN: iterating with iter_column_blocks and a tracking factory
    async with connect(
        "clickhouse://default:@host/db",
        transport_factory=transport,
        column_factories={"UInt32": tracking_tuple},
    ) as client:
        blocks = [b async for b in client.iter_column_blocks("SELECT n FROM t")]

    # THEN: factory called once per block (2 blocks x 1 column = 2 calls)
    assert len(calls) == 2
    assert all(isinstance(b.data[0], tuple) for b in blocks)
    assert blocks[0].data[0] == (1, 2)
    assert blocks[1].data[0] == (3, 4, 5)


# ---- pool propagates factories to client --------------------------------


async def test_pool_propagates_factories_to_client() -> None:
    # BEGIN: pool created with a tuple factory for UInt32
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_data(_uint32_block([7, 8, 9])))
    transport.feed(encode_server_end_of_stream())

    # WHEN: calling fetch_columns through the pool
    async with create_pool(
        "clickhouse://default:@host/db",
        max_size=1,
        transport_factory=transport,
        enable_reaper=False,
        column_factories={"UInt32": tuple},
    ) as pool:
        result = await pool.fetch_columns("SELECT n FROM t")

    # THEN: factory applied via the acquired client — column is a tuple
    assert isinstance(result.data[0], tuple)
    assert result.data[0] == (7, 8, 9)
