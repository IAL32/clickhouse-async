"""Tests for ``Client.execute`` / ``fetch_all`` / ``fetch_one``."""

from __future__ import annotations

import pytest

from clickhouse_async import QueryResult, connect
from clickhouse_async.errors import ServerError
from clickhouse_async.protocol.block import Block, BlockInfo, make_column

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_exception,
    encode_server_hello,
    encode_server_profile_info,
    encode_server_progress,
)


def _two_row_select_response(transport: ScriptedTransport) -> None:
    """Queue a typical SELECT response: header block + data block + EOS."""
    spec, _ = make_column("number", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(
        info=BlockInfo(),
        columns=[spec],
        n_rows=2,
        data=[[10, 20]],
    )
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())


# ---- execute() returns a QueryResult -----------------------------------


async def test_execute_returns_columns_and_row_major_rows() -> None:
    # BEGIN: a scripted SELECT response with 2 rows over 1 column
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_row_select_response(transport)

    # WHEN: running execute()
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.execute("SELECT number FROM numbers LIMIT 2")

    # THEN: the result has the column metadata from the header and
    #       the rows transposed into row-major tuples
    assert isinstance(result, QueryResult)
    assert [c.name for c in result.columns] == ["number"]
    assert result.rows == [(10,), (20,)]
    assert result.row_count == 2


async def test_execute_handles_multi_column_blocks_in_order() -> None:
    # BEGIN: a SELECT that returns two columns over two rows
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec_id, _ = make_column("id", "UInt32", [])
    spec_name, _ = make_column("name", "String", [])
    header = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name],
        n_rows=0,
        data=[[], []],
    )
    data = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name],
        n_rows=2,
        data=[[1, 2], ["alpha", "beta"]],
    )
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running the query
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.execute("SELECT id, name FROM t")

    # THEN: rows are tuples in declared column order, columns track names
    assert [c.name for c in result.columns] == ["id", "name"]
    assert result.rows == [(1, "alpha"), (2, "beta")]


async def test_execute_concatenates_multiple_data_blocks() -> None:
    # BEGIN: a SELECT response split across three data blocks
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int8", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    block1 = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[[1, 2]])
    block2 = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[[3, 4, 5]])
    block3 = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[6]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(block1))
    transport.feed(encode_server_data(block2))
    transport.feed(encode_server_data(block3))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running execute
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.execute("SELECT n FROM t")

    # THEN: rows from every block are concatenated in arrival order
    assert [r[0] for r in result.rows] == [1, 2, 3, 4, 5, 6]


async def test_execute_captures_final_progress_and_profile_info() -> None:
    # BEGIN: a SELECT response interleaving Progress and ProfileInfo
    #        with the data blocks before EndOfStream
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[42]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_progress(read_rows=10, read_bytes=200))
    transport.feed(encode_server_data(data))
    transport.feed(
        encode_server_progress(read_rows=20, read_bytes=400, written_rows=1)
    )
    transport.feed(encode_server_profile_info(rows=1, blocks=2, bytes_=64))
    transport.feed(encode_server_end_of_stream())

    # WHEN: running execute
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.execute("SELECT 42")

    # THEN: the final Progress and ProfileInfo packets are surfaced
    #       through QueryResult; row data still round-trips
    assert result.rows == [(42,)]
    assert result.progress.read_rows == 20
    assert result.progress.read_bytes == 400
    assert result.written_rows == 1
    assert result.profile_info is not None
    assert result.profile_info.rows == 1
    assert result.profile_info.blocks == 2


async def test_execute_records_elapsed_wall_clock_time() -> None:
    # BEGIN: a quick SELECT response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_row_select_response(transport)

    # WHEN: running the query
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        result = await client.execute("SELECT number")

    # THEN: elapsed is non-negative and bounded by a generous ceiling —
    #       the in-memory loopback should be sub-second
    assert result.elapsed >= 0.0
    assert result.elapsed < 1.0


async def test_execute_passes_params_through_to_send_query() -> None:
    # BEGIN: a SELECT that the server-side parameter parser will resolve
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_row_select_response(transport)

    # WHEN: running execute() with typed parameters
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        await client.execute(
            "SELECT number WHERE n = {n:Int32}", params={"n": 42}
        )

    # THEN: the captured Query packet's tail carries the parameter
    #       block (we just check the bytes contain the value as text)
    written = transport.written()
    assert b"42" in written  # the formatted parameter value


# ---- ServerError propagation -------------------------------------------


async def test_execute_raises_server_error_and_leaves_client_reusable() -> None:
    # BEGIN: a server that rejects the query with an Exception
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(
        encode_server_exception(
            code=60, name="UNKNOWN_TABLE", display_text="Table foo doesn't exist"
        )
    )
    # Queue a second successful response for a follow-up query
    _two_row_select_response(transport)

    # WHEN: the first query fails, then the second succeeds
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        with pytest.raises(ServerError, match="UNKNOWN_TABLE"):
            await client.execute("SELECT * FROM foo")
        # THEN: the client is still usable — Exception isn't a transport
        #       failure
        result = await client.execute("SELECT number")
        assert result.row_count == 2


# ---- fetch_all / fetch_one ---------------------------------------------


async def test_fetch_all_returns_just_the_rows() -> None:
    # BEGIN: a 2-row SELECT
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_row_select_response(transport)

    # WHEN: fetching all rows
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        rows = await client.fetch_all("SELECT number")

    # THEN: rows come back as a plain list of tuples
    assert rows == [(10,), (20,)]


async def test_fetch_one_returns_first_row() -> None:
    # BEGIN: a 2-row SELECT
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _two_row_select_response(transport)

    # WHEN: fetching just the first row
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        row = await client.fetch_one("SELECT number")

    # THEN: it's the first tuple
    assert row == (10,)


async def test_fetch_one_returns_none_for_empty_result() -> None:
    # BEGIN: a SELECT response with header only (no data rows)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    spec, _ = make_column("n", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_end_of_stream())

    # WHEN: fetching one row
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        row = await client.fetch_one("SELECT number WHERE 0")

    # THEN: it's None (rather than an empty tuple, which would be
    #       indistinguishable from a 0-column row)
    assert row is None
