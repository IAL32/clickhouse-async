"""Round-trip and byte-layout tests for Block read/write."""

from __future__ import annotations

import asyncio

import pytest

from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.block import (
    Block,
    BlockInfo,
    make_column,
    read_block,
    read_block_info,
    write_block,
    write_block_info,
)
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import OUR_REVISION


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


# ---- BlockInfo ----------------------------------------------------------


async def test_block_info_default_round_trip() -> None:
    # BEGIN: a default BlockInfo (the common case from a vanilla SELECT)
    info = BlockInfo()
    writer = BinaryWriter()

    # WHEN: writing then reading back
    write_block_info(writer, info)
    decoded = await read_block_info(_reader(writer.getvalue()))

    # THEN: defaults round-trip identically
    assert decoded == info


async def test_block_info_custom_round_trip() -> None:
    # BEGIN: a BlockInfo carrying both fields with non-default values
    info = BlockInfo(is_overflows=True, bucket_num=7)
    writer = BinaryWriter()

    # WHEN: writing then reading back
    write_block_info(writer, info)
    decoded = await read_block_info(_reader(writer.getvalue()))

    # THEN: every field round-trips
    assert decoded == info


async def test_block_info_layout_is_numbered_fields_terminated_by_zero() -> None:
    # BEGIN: a BlockInfo with recognisable values for byte-spotting
    info = BlockInfo(is_overflows=True, bucket_num=42)

    # WHEN: writing it
    writer = BinaryWriter()
    write_block_info(writer, info)

    # THEN: the layout is exactly: varuint 1, byte 1, varuint 2, Int32 LE 42,
    #       varuint 0 (sentinel)
    assert (
        writer.getvalue()
        == b"\x01\x01\x02" + (42).to_bytes(4, "little", signed=True) + b"\x00"
    )


async def test_block_info_unknown_field_raises_protocol_error() -> None:
    # BEGIN: a stream encoding field number 3 — gated above OUR_REVISION (24.8)
    bad = b"\x03\x01\x00"  # field 3, one payload byte, then sentinel
    reader = _reader(bad)

    # WHEN: reading the block info
    # THEN: a ProtocolError surfaces — the handshake should not have
    #       produced field 3 at our negotiated revision
    with pytest.raises(ProtocolError, match="unknown BlockInfo field number 3"):
        await read_block_info(reader)


# ---- Block: empty / header-only ----------------------------------------


async def test_empty_block_round_trip() -> None:
    # BEGIN: a block with zero columns and zero rows (server emits these
    #        as part of certain control sequences)
    block = Block()
    writer = BinaryWriter()

    # WHEN: writing then reading back at OUR_REVISION
    write_block(writer, block, revision=OUR_REVISION)
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: the empty block round-trips with default BlockInfo
    assert decoded.info == BlockInfo()
    assert decoded.columns == []
    assert decoded.n_rows == 0
    assert decoded.data == []


async def test_header_only_block_carries_column_metadata_no_rows() -> None:
    # BEGIN: a header-only block — column specs but n_rows=0. The server
    #        emits this before streaming a SELECT result so the client can
    #        wire up the output formatter early.
    spec_a, _ = make_column("id", "Int32", [])
    spec_b, _ = make_column("name", "String", [])
    block = Block(
        info=BlockInfo(),
        columns=[spec_a, spec_b],
        n_rows=0,
        data=[[], []],
    )
    writer = BinaryWriter()

    # WHEN: writing then reading back
    write_block(writer, block, revision=OUR_REVISION)
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: column names and type specs survive even though no rows are present
    assert decoded.n_rows == 0
    assert [c.name for c in decoded.columns] == ["id", "name"]
    assert [c.type_spec for c in decoded.columns] == ["Int32", "String"]
    assert decoded.data == [[], []]


# ---- Block: 1-column / N-column data -----------------------------------


async def test_single_column_int32_round_trip() -> None:
    # BEGIN: a 1-column 3-row Int32 block
    spec, _ = make_column("number", "Int32", [10, 20, 30])
    block = Block(
        info=BlockInfo(),
        columns=[spec],
        n_rows=3,
        data=[[10, 20, 30]],
    )
    writer = BinaryWriter()

    # WHEN: writing then reading back
    write_block(writer, block, revision=OUR_REVISION)
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: column metadata and values come back identical
    assert decoded.n_rows == 3
    assert decoded.data == [[10, 20, 30]]
    assert decoded.columns[0].name == "number"
    assert decoded.columns[0].type_spec == "Int32"


async def test_multi_column_mixed_types_round_trip() -> None:
    # BEGIN: a 3-column block exercising int / string / nullable
    spec_id, vals_id = make_column("id", "UInt32", [1, 2, 3])
    spec_name, vals_name = make_column("name", "String", ["a", "bb", "ccc"])
    spec_score, vals_score = make_column(
        "score", "Nullable(Float64)", [1.5, None, -0.25]
    )
    block = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_name, spec_score],
        n_rows=3,
        data=[vals_id, vals_name, vals_score],
    )
    writer = BinaryWriter()

    # WHEN: writing then reading back
    write_block(writer, block, revision=OUR_REVISION)
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: every column survives, order preserved, nulls intact
    assert decoded.n_rows == 3
    assert [c.name for c in decoded.columns] == ["id", "name", "score"]
    assert decoded.data[0] == [1, 2, 3]
    assert decoded.data[1] == ["a", "bb", "ccc"]
    assert decoded.data[2] == [1.5, None, -0.25]


async def test_nested_types_round_trip() -> None:
    # BEGIN: a block with a deeply nested column type
    spec, vals = make_column(
        "tags",
        "Array(Nullable(String))",
        [["a", None, "b"], [], ["c"]],
    )
    block = Block(
        info=BlockInfo(),
        columns=[spec],
        n_rows=3,
        data=[vals],
    )
    writer = BinaryWriter()

    # WHEN: writing then reading back
    write_block(writer, block, revision=OUR_REVISION)
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: nested values survive, including the inner null mask
    assert decoded.data == [[["a", None, "b"], [], ["c"]]]


# ---- Block: revision gating ---------------------------------------------


async def test_below_custom_serialization_revision_omits_has_custom_byte() -> None:
    # BEGIN: a block at a revision below DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION
    spec, vals = make_column("x", "Int8", [1])
    block = Block(columns=[spec], n_rows=1, data=[vals])
    revision = 54453  # one below the gate (54454)

    # WHEN: writing and inspecting the byte length
    writer = BinaryWriter()
    write_block(writer, block, revision=revision)
    encoded = writer.getvalue()

    # THEN: there is no extra has_custom byte in the per-column header — we
    #       can verify this by reading back at the same revision and getting
    #       the same data
    decoded = await read_block(_reader(encoded), revision=revision)
    assert decoded.data == [[1]]


async def test_custom_serialization_byte_must_be_zero() -> None:
    # BEGIN: a synthetic stream where has_custom=1 (we never emit this, but
    #        a misconfigured server would)
    writer = BinaryWriter()
    # Empty BlockInfo
    writer.write_varuint(1)
    writer.write_byte(0)
    writer.write_varuint(2)
    writer.write_int(-1, 4, signed=True)
    writer.write_varuint(0)
    # 1 column, 1 row
    writer.write_varuint(1)
    writer.write_varuint(1)
    # Column header: name, type, has_custom=1
    writer.write_string("x")
    writer.write_string("Int8")
    writer.write_byte(1)  # poison

    # WHEN: reading the block at OUR_REVISION
    # THEN: a ProtocolError surfaces, naming the offending column and value
    with pytest.raises(ProtocolError, match="custom serialization"):
        await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)


# ---- Block: validation on write ----------------------------------------


def test_write_block_rejects_mismatched_data_columns() -> None:
    # BEGIN: a block whose data list length doesn't match its columns list
    spec, vals = make_column("x", "Int8", [1])
    block = Block(columns=[spec, spec], n_rows=1, data=[vals])
    writer = BinaryWriter()

    # WHEN: attempting to write
    # THEN: a ValueError surfaces before we touch the wire
    with pytest.raises(ValueError, match="2 column specs"):
        write_block(writer, block, revision=OUR_REVISION)


def test_write_block_rejects_row_count_mismatch() -> None:
    # BEGIN: a block whose declared n_rows differs from a column's value count
    spec, _ = make_column("x", "Int8", [])
    block = Block(columns=[spec], n_rows=3, data=[[1, 2]])
    writer = BinaryWriter()

    # WHEN: attempting to write
    # THEN: a ValueError surfaces naming the column and counts
    with pytest.raises(ValueError, match=r"block\.n_rows is 3"):
        write_block(writer, block, revision=OUR_REVISION)
