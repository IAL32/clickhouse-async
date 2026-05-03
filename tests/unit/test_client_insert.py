"""Tests for ``Client.insert``."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from clickhouse_async import connect
from clickhouse_async.connection import State
from clickhouse_async.protocol.block import Block, BlockInfo, make_column

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _insert_response(
    transport: ScriptedTransport,
    *,
    column_specs: list[tuple[str, str]] | None = None,
) -> None:
    """Queue an INSERT-shaped server flow: header block + EndOfStream."""
    if column_specs is None:
        column_specs = [("id", "Int32"), ("name", "String")]
    specs = [make_column(name, type_, [])[0] for name, type_ in column_specs]
    header = Block(
        info=BlockInfo(),
        columns=specs,
        n_rows=0,
        data=[[] for _ in specs],
    )
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_end_of_stream())


# ---- happy path: sync iterable -----------------------------------------


async def test_insert_sync_iterable_round_trips() -> None:
    # BEGIN: an INSERT-shaped server response and a sync list of rows
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _insert_response(transport)
    rows = [(1, "alice"), (2, "bob"), (3, "carol")]

    # WHEN: running insert with a plain list as the rows source
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        n = await client.insert(
            "INSERT INTO t (id, name) VALUES",
            rows=rows,
            column_names=["id", "name"],
        )
        # THEN (inside the with block, before close): every row shipped,
        #     connection back to READY (drain after terminator saw EOS)
        assert n == 3
        assert client._conn.state == State.READY  # type: ignore[attr-defined]


# ---- async iterable ----------------------------------------------------


async def test_insert_async_iterable_round_trips() -> None:
    # BEGIN: an INSERT-shaped response and an async generator of rows
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _insert_response(transport)

    async def _gen() -> AsyncIterator[tuple[int, str]]:
        for i in range(5):
            yield (i, f"row-{i}")

    # WHEN: passing the async generator as the rows source
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        n = await client.insert(
            "INSERT INTO t VALUES",
            rows=cast("AsyncIterator[tuple[object, ...]]", _gen()),
            column_names=["id", "name"],
        )

    # THEN: every async-yielded row was shipped
    assert n == 5


# ---- batching ----------------------------------------------------------


async def test_insert_batches_rows_at_insert_block_size() -> None:
    # BEGIN: an INSERT-shaped response and rows at exactly batch size + 1
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _insert_response(transport)
    rows = [(i, f"row-{i}") for i in range(7)]

    # WHEN: running insert with a tiny insert_block_size to force two
    #       batches
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        n = await client.insert(
            "INSERT INTO t VALUES",
            rows=rows,
            column_names=["id", "name"],
            insert_block_size=3,
        )

    # THEN: all rows were shipped (across 3+3+1 = 3 Data packets +
    #       terminator)
    assert n == 7


# ---- column-name mismatch ----------------------------------------------


async def test_insert_column_name_mismatch_raises_and_keeps_client_usable() -> None:
    # BEGIN: an INSERT-shaped response whose header carries different
    #        column names from what the user passed
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _insert_response(transport)
    # Queue a follow-up SELECT response so we can verify the client
    # is still usable
    spec, _ = make_column("n", "Int32", [])
    follow_header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    follow_data = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[42]])
    transport.feed(encode_server_data(follow_header))
    transport.feed(encode_server_data(follow_data))
    transport.feed(encode_server_end_of_stream())

    # WHEN: insert is called with the wrong column names
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        with pytest.raises(ValueError, match="column names mismatch"):
            await client.insert(
                "INSERT INTO t VALUES",
                rows=[(1,)],
                column_names=["bogus"],
            )

        # THEN: the client is still usable for the next query
        result = await client.fetch_all("SELECT n FROM t")
        assert result == [(42,)]


# ---- row arity validation ----------------------------------------------


async def test_insert_row_with_wrong_arity_raises() -> None:
    # BEGIN: an INSERT-shaped response and a row with the wrong number
    #        of columns
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _insert_response(transport)

    # WHEN / THEN: inserting raises ValueError naming the row index and
    #              expected/actual column count
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        with pytest.raises(ValueError, match="row 0 has 1 columns"):
            await client.insert(
                "INSERT INTO t VALUES",
                rows=[(1,)],  # only 1 element; header expects 2
                column_names=["id", "name"],
            )


# ---- empty rows --------------------------------------------------------


async def test_insert_with_no_rows_still_terminates_cleanly() -> None:
    # BEGIN: an INSERT-shaped response and an empty rows iterable
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _insert_response(transport)

    # WHEN: inserting zero rows
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        n = await client.insert(
            "INSERT INTO t VALUES",
            rows=[],
            column_names=["id", "name"],
        )
        # THEN (inside the with block): zero rows shipped, connection
        #     back to READY (we still sent the empty terminator)
        assert n == 0
        assert client._conn.state == State.READY  # type: ignore[attr-defined]
