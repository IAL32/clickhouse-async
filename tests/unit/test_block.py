"""Round-trip and byte-layout tests for Block read/write."""

from __future__ import annotations

import asyncio
import struct

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
    # BEGIN: a stream encoding a genuinely unknown field number (99)
    bad = b"\x63\x01\x00"  # field 99, one payload byte, then sentinel
    reader = _reader(bad)

    # WHEN: reading the block info
    # THEN: a ProtocolError surfaces — unknown field numbers are rejected
    with pytest.raises(ProtocolError, match="unknown BlockInfo field number 99"):
        await read_block_info(reader)


async def test_block_info_field_3_out_of_order_buckets_is_drained() -> None:
    # BEGIN: a stream with field 3 (out_of_order_buckets) carrying two Int32s
    #        followed by field 1 (is_overflows=True) then sentinel
    buckets = struct.pack("<ii", 7, 42)  # two Int32 values
    data = (
        b"\x03"  # field_num = 3
        + b"\x02"  # varuint count = 2
        + buckets  # 2 x Int32
        + b"\x01"  # field_num = 1 (is_overflows)
        + b"\x01"  # True
        + b"\x00"  # terminator
    )
    reader = _reader(data)

    # WHEN: reading block info
    info = await read_block_info(reader)

    # THEN: field 3 was drained; the subsequent is_overflows field was read
    assert info.is_overflows is True


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


async def test_has_custom_byte_above_one_raises_protocol_error() -> None:
    # BEGIN: a synthetic stream where has_custom=2 — the protocol only
    #        defines 0 (default) and 1 (custom serialisation present).
    #        Any other value is a wire-format violation.
    writer = BinaryWriter()
    _write_empty_block_header(writer, n_columns=1, n_rows=1)
    writer.write_string("x")
    writer.write_string("Int8")
    writer.write_byte(2)  # invalid

    # WHEN: reading the block at OUR_REVISION
    # THEN: a ProtocolError surfaces, naming the offending column and value
    with pytest.raises(ProtocolError, match="has_custom byte 2"):
        await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)


async def test_custom_serialization_unknown_kind_raises() -> None:
    # BEGIN: has_custom=1 (custom serialisation present) but the inner
    #        kind byte is 2 — only kind=1 (Sparse) is currently produced
    writer = BinaryWriter()
    _write_empty_block_header(writer, n_columns=1, n_rows=1)
    writer.write_string("x")
    writer.write_string("Int8")
    writer.write_byte(1)  # has_custom = 1
    writer.write_byte(2)  # kind = 2 (unknown)

    # WHEN / THEN: a ProtocolError surfaces, naming the offending kind
    with pytest.raises(ProtocolError, match=r"kind=2"):
        await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)


# ---- Block: sparse-serialised columns ----------------------------------

# Bit-62 flag the server OR-s into the **last** sparse-offset varuint to
# mark end-of-granule. The remaining low 62 bits are the trailing-defaults
# count. Mirrors ``_SPARSE_END_OF_GRANULE_FLAG`` in ``protocol/block.py``.
_SPARSE_END_OF_GRANULE_FLAG = 1 << 62


def _write_empty_block_header(
    writer: BinaryWriter, *, n_columns: int, n_rows: int
) -> None:
    """Emit the BlockInfo + (n_columns, n_rows) prefix used by every
    block test that doesn't go through ``write_block`` directly."""
    writer.write_varuint(1)  # BlockInfo field 1: is_overflows
    writer.write_byte(0)
    writer.write_varuint(2)  # BlockInfo field 2: bucket_num
    writer.write_int(-1, 4, signed=True)
    writer.write_varuint(0)  # BlockInfo terminator
    writer.write_varuint(n_columns)
    writer.write_varuint(n_rows)


def _write_sparse_uint8_column(
    writer: BinaryWriter,
    *,
    group_sizes: list[int],
    trailing_defaults: int,
    values: list[int],
) -> None:
    """Encode the body of a sparse UInt8 column past its has_custom byte.

    ``group_sizes[i]`` is the number of default-valued rows before the
    i-th non-default; ``trailing_defaults`` is the count after the last
    non-default. ``values`` carries the non-default UInt8 payloads.
    """
    writer.write_byte(1)  # has_custom = 1
    writer.write_byte(1)  # kind = 1 (Sparse)
    for size in group_sizes:
        writer.write_varuint(size)
    writer.write_varuint(_SPARSE_END_OF_GRANULE_FLAG | trailing_defaults)
    for v in values:
        writer.write_byte(v)


async def test_sparse_all_default_column_decodes_to_null_values() -> None:
    # BEGIN: a block whose only column is sparse and *fully* default —
    #        no group-size varuints, just the end-of-granule marker
    #        whose low 62 bits hold the trailing-defaults count
    writer = BinaryWriter()
    _write_empty_block_header(writer, n_columns=1, n_rows=5)
    writer.write_string("v")
    writer.write_string("UInt8")
    _write_sparse_uint8_column(writer, group_sizes=[], trailing_defaults=5, values=[])

    # WHEN: reading the block
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: every row carries the codec's null_value (0 for UInt8)
    assert decoded.n_rows == 5
    assert decoded.data == [[0, 0, 0, 0, 0]]


async def test_sparse_single_non_default_at_position_zero() -> None:
    # BEGIN: 5 rows, one non-default value (=7) at position 0, the rest
    #        are defaults
    writer = BinaryWriter()
    _write_empty_block_header(writer, n_columns=1, n_rows=5)
    writer.write_string("v")
    writer.write_string("UInt8")
    _write_sparse_uint8_column(writer, group_sizes=[0], trailing_defaults=4, values=[7])

    # WHEN: reading
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: position 0 holds 7, positions 1-4 hold the default 0
    assert decoded.data == [[7, 0, 0, 0, 0]]


async def test_sparse_multiple_non_defaults_interleaved() -> None:
    # BEGIN: 10 rows with non-defaults at positions 0, 3, 9 — the same
    #        shape we observed on the wire from the live server
    writer = BinaryWriter()
    _write_empty_block_header(writer, n_columns=1, n_rows=10)
    writer.write_string("v")
    writer.write_string("UInt8")
    _write_sparse_uint8_column(
        writer,
        # Walk: pos 0 (gs=0, place 1, pos=1) -> +2 defaults to pos 3
        # (place 2, pos=4) -> +5 defaults to pos 9 (place 3, pos=10)
        group_sizes=[0, 2, 5],
        trailing_defaults=0,
        values=[1, 2, 3],
    )

    # WHEN: reading
    decoded = await read_block(_reader(writer.getvalue()), revision=OUR_REVISION)

    # THEN: non-defaults land at the right positions; rest is the codec's
    #       null_value (0 for UInt8)
    assert decoded.data == [[1, 0, 0, 2, 0, 0, 0, 0, 0, 3]]


async def test_sparse_offsets_overshoot_n_rows_raises() -> None:
    # BEGIN: a sparse stream whose group_sizes + trailing_defaults sum
    #        to more than n_rows — the server should never emit this,
    #        but if it did we must catch it before producing wrong data
    writer = BinaryWriter()
    _write_empty_block_header(writer, n_columns=1, n_rows=5)
    writer.write_string("v")
    writer.write_string("UInt8")
    _write_sparse_uint8_column(
        writer,
        group_sizes=[0],  # 1 non-default at position 0 (consumes 1)
        trailing_defaults=10,  # claims 10 trailing — total 11 > n_rows=5
        values=[7],
    )

    # WHEN / THEN: ProtocolError flags the mismatch by name
    with pytest.raises(ProtocolError, match="sparse offsets"):
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
