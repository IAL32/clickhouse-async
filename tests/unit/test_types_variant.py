"""Round-trip and wire-format tests for `Variant` and `Dynamic`.

The codec round-trips a closed tagged union (`Variant`) and an open
one (`Dynamic`) — both share the same body shape (8B version,
`n_rows` discriminator bytes, per-arm bodies in declared order),
`Dynamic` adds a per-block prefix declaring the active types.
"""

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.variant import Dynamic, Variant


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


# ---- parser surface -----------------------------------------------------


def test_variant_parses_into_codec_with_named_arms() -> None:
    # BEGIN / WHEN: a three-arm Variant
    codec = parse_type("Variant(Int64, String, Date)")

    # THEN: a Variant codec comes back with the arms in declared order
    #       and `codec.name` round-trips the spec verbatim
    assert isinstance(codec, Variant)
    assert codec.name == "Variant(Int64, String, Date)"
    assert len(codec.components) == 3


def test_variant_rejects_empty_arm_list() -> None:
    # BEGIN / WHEN / THEN: `Variant()` is meaningless (no arm to pick)
    with pytest.raises(ValueError, match="one or more type parameters"):
        parse_type("Variant()")


def test_dynamic_bare_form_parses_with_default_max_types() -> None:
    # BEGIN / WHEN: a bare `Dynamic` (no parens)
    codec = parse_type("Dynamic")

    # THEN: a Dynamic codec with no max_types cap; `codec.name`
    #       matches the bare spelling
    assert isinstance(codec, Dynamic)
    assert codec.max_types is None
    assert codec.name == "Dynamic"


def test_dynamic_named_int_form_parses() -> None:
    # BEGIN / WHEN: the parametric `Dynamic(max_types=N)` form
    codec = parse_type("Dynamic(max_types=42)")

    # THEN: a Dynamic codec with the cap stored, and the rendering
    #       round-trips the spec verbatim
    assert isinstance(codec, Dynamic)
    assert codec.max_types == 42
    assert codec.name == "Dynamic(max_types=42)"


def test_dynamic_rejects_unknown_named_param() -> None:
    # BEGIN / WHEN / THEN: only `max_types` is accepted
    with pytest.raises(ValueError, match="max_types"):
        parse_type("Dynamic(unknown=1)")


# ---- Variant round-trips -------------------------------------------------


async def test_variant_round_trip_mixed_rows_recovers_each_arm() -> None:
    # BEGIN: a 3-arm Variant and four rows, each landing in a
    #        different arm (Int64, String, Date, NULL).
    codec = parse_type("Variant(Int64, String, Date)")
    values: list[object] = [
        42,
        "hello",
        date(2026, 5, 3),
        None,
    ]

    # WHEN: writing then reading back through the codec
    writer = BinaryWriter()
    codec.write(writer, values)
    decoded = await codec.read(_reader(writer.getvalue()), len(values))

    # THEN: every row's value comes back identical (and the discriminator
    #       was inferred correctly from the Python type per arm)
    assert decoded == values


async def test_variant_all_null_column_round_trips() -> None:
    # BEGIN: every row is NULL → discriminator stream is all 0xFF and
    #        no arm bodies are written
    codec = parse_type("Variant(Int64, String)")
    values: list[object] = [None, None, None]

    # WHEN: round-tripping
    writer = BinaryWriter()
    codec.write(writer, values)
    encoded = writer.getvalue()
    decoded = await codec.read(_reader(encoded), len(values))

    # THEN: every row is None and the encoded body is exactly the
    #       8-byte version + 3 x 0xFF discriminator (no arm bodies)
    assert decoded == values
    assert encoded == bytes(8) + b"\xff\xff\xff"


async def test_variant_tag_forces_arm_when_inference_would_pick_first_match() -> None:
    # BEGIN: a Variant where two arms share a Python type — both Int32
    #        and Int64 surface as `int`, so default resolution picks
    #        Int32 (declared first). Variant.tag overrides to arm 1.
    codec = parse_type("Variant(Int32, Int64)")
    values: list[object] = [
        7,  # → arm 0 (Int32) by default
        Variant.tag(7, 1),  # → arm 1 (Int64) explicitly
    ]

    # WHEN: round-tripping
    writer = BinaryWriter()
    codec.write(writer, values)
    encoded = writer.getvalue()
    decoded = await codec.read(_reader(encoded), len(values))

    # THEN: both rows surface the integer 7 on read (the discriminator
    #       lives in the codec only) but the on-wire discriminator
    #       stream proves the tag pinned arm 1 for row 1
    assert decoded == [7, 7]
    # 8B version + 1B discriminator per row + arm bodies
    discriminators = encoded[8 : 8 + len(values)]
    assert discriminators == bytes([0, 1])


def test_variant_tag_rejects_out_of_range_arm_index() -> None:
    # BEGIN: a 2-arm Variant — index 2 is out of range
    codec = parse_type("Variant(Int64, String)")

    # WHEN / THEN: `Variant.tag` resolution flags the bad index at
    #              write time with a clear error
    with pytest.raises(ValueError, match=r"out of range"):
        codec.write(BinaryWriter(), [Variant.tag(1, 2)])


def test_variant_write_rejects_value_with_no_matching_arm() -> None:
    # BEGIN: a Variant whose arms don't cover `float` (no Float arm)
    codec = parse_type("Variant(Int64, String)")

    # WHEN / THEN: the resolver names the offending Python type and
    #              points at the workaround
    with pytest.raises(ValueError, match=r"Variant\.tag"):
        codec.write(BinaryWriter(), [3.14])


async def test_variant_empty_column_reads_and_writes_zero_bytes() -> None:
    # BEGIN / WHEN: an empty Variant column — no version byte, no
    #               discriminators, no arm bodies
    codec = parse_type("Variant(Int64, String)")
    writer = BinaryWriter()
    codec.write(writer, [])
    encoded = writer.getvalue()
    decoded = await codec.read(_reader(encoded), 0)

    # THEN: exactly zero bytes round-trip
    assert encoded == b""
    assert decoded == []


# ---- Variant wire-format pin --------------------------------------------


async def test_variant_wire_format_pin_for_int32_string() -> None:
    # BEGIN: a hand-built `Variant(Int32, String)` payload exercising
    #        all three discriminator states (arm 0, arm 1, NULL).
    #        Layout: 8B version (0) + 4 x 1B discriminator + Int32 body
    #        for the rows where disc == 0 (one row: 7) + String body
    #        for the rows where disc == 1 (two rows: "x", "y").
    payload = (
        bytes(8)  # version 0
        + bytes([0, 1, 0xFF, 1])  # disc: int32, str, null, str
        + (7).to_bytes(4, "little", signed=True)  # Int32 arm body
        + b"\x01x\x01y"  # String arm body: 'x' then 'y'
    )
    codec = parse_type("Variant(Int32, String)")

    # WHEN: decoding the payload
    decoded = await codec.read(_reader(payload), 4)

    # THEN: the values land in the rows their discriminators point at,
    #       in the original row order (NOT in arm-body order)
    assert decoded == [7, "x", None, "y"]


async def test_variant_unsupported_version_raises_protocol_error() -> None:
    # BEGIN: a payload whose first 8 bytes claim a non-zero
    #        discriminator-stream version (e.g. COMPACT mode 1)
    codec = parse_type("Variant(Int32, String)")
    payload = (1).to_bytes(8, "little", signed=False) + b"\x00"

    # WHEN / THEN: the codec surfaces the version with a clear error
    #              instead of silently misinterpreting bytes
    with pytest.raises(ProtocolError, match="version"):
        await codec.read(_reader(payload), 1)


async def test_variant_out_of_range_discriminator_raises() -> None:
    # BEGIN: a 2-arm Variant payload claiming discriminator 5 — the
    #        codec must reject rather than reading past the end
    codec = parse_type("Variant(Int32, String)")
    payload = bytes(8) + bytes([5])

    # WHEN / THEN: a discriminator that doesn't index any arm raises
    with pytest.raises(ProtocolError, match="out of range"):
        await codec.read(_reader(payload), 1)


# ---- Dynamic round-trips -----------------------------------------------


async def test_dynamic_two_active_types_round_trip() -> None:
    # BEGIN: a Dynamic block with two active arms (Int64, String) and
    #        a NULL row — the codec writes both arms to the prefix.
    codec = parse_type("Dynamic")
    values: list[object] = [42, "hello", None, 99]

    # WHEN: round-tripping
    writer = BinaryWriter()
    codec.write(writer, values)
    decoded = await codec.read(_reader(writer.getvalue()), len(values))

    # THEN: every row's value survives, and the per-block prefix
    #       declared exactly the active arms
    assert decoded == values


async def test_dynamic_three_active_types_round_trip() -> None:
    # BEGIN: a Dynamic block with three active arms (Int64, String,
    #        Date) — exercises growing the prefix across rows
    codec = parse_type("Dynamic")
    values: list[object] = [
        1,
        "alpha",
        date(2026, 5, 3),
        2,
        "beta",
    ]

    # WHEN: round-tripping
    decoded = await _round_trip_dynamic(codec, values)

    # THEN: every row round-trips
    assert decoded == values


async def test_dynamic_per_block_prefix_declares_only_used_types() -> None:
    # BEGIN: a Dynamic block that only uses Int64 (and NULL) — the
    #        per-block type list should contain exactly one entry
    codec = parse_type("Dynamic")
    values: list[object] = [1, 2, None, 3]

    writer = BinaryWriter()
    codec.write(writer, values)
    encoded = writer.getvalue()

    # The prefix layout (V1 form per upstream
    # `SerializationDynamic::DynamicSerializationVersion`): 8B
    # version + varuint duplicate `max_dynamic_types` slot + varuint
    # actual `num_dynamic_types` + one length-prefixed type-spec
    # string. `Int64` encodes as varuint length (5) + 5 ASCII bytes.
    # The implicit `SharedVariant` arm is *not* in the declared list
    # — it's only appended on the read side.
    # WHEN: peek at the prefix
    expected_prefix = (
        b"\x01\x00\x00\x00\x00\x00\x00\x00"  # UInt64 LE: version V1 = 1
        + b"\x01"  # varuint: max_dynamic_types slot (we mirror count)
        + b"\x01"  # varuint: actual num_dynamic_types = 1
        + b"\x05Int64"  # length-prefixed type-spec
    )

    # THEN: only `Int64` was declared — NULL doesn't take an arm
    assert encoded[: len(expected_prefix)] == expected_prefix


async def test_dynamic_tag_pins_explicit_type_spec() -> None:
    # BEGIN: a value whose Python type maps to `DateTime` by default,
    #        but we want it stored as `Date` instead — Dynamic.tag
    #        forces the type spec
    codec = parse_type("Dynamic")
    values: list[object] = [
        Dynamic.tag(date(2026, 5, 3), "Date"),
        Dynamic.tag(date(2026, 5, 4), "Date"),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip_dynamic(codec, values)

    # THEN: the values come back as plain `date` (the Dynamic.tag
    #       wrapper unwraps to its underlying value on read)
    assert decoded == [date(2026, 5, 3), date(2026, 5, 4)]


async def test_dynamic_max_types_cap_exceeded_raises() -> None:
    # BEGIN: a Dynamic with a cap of 1 type but two distinct types in
    #        the block
    codec = parse_type("Dynamic(max_types=1)")
    values: list[object] = [1, "x"]

    # WHEN / THEN: the write path surfaces the cap with a clear error
    with pytest.raises(ValueError, match="max_types"):
        codec.write(BinaryWriter(), values)


async def test_dynamic_all_null_block_writes_zero_active_types() -> None:
    # BEGIN: every row is NULL → no active types, but the
    #        discriminator stream still has one byte per row
    codec = parse_type("Dynamic")
    values: list[object] = [None, None]

    # WHEN: round-tripping
    decoded = await _round_trip_dynamic(codec, values)

    # THEN: the all-NULL block round-trips and the prefix declares
    #       zero active arms
    assert decoded == values


async def test_dynamic_unknown_python_type_points_at_tag() -> None:
    # BEGIN: a value with no inference rule (an empty list)
    codec = parse_type("Dynamic")

    # WHEN / THEN: the inference helper raises with a pointer at
    #              `Dynamic.tag`
    with pytest.raises(ValueError, match=r"Dynamic\.tag"):
        codec.write(BinaryWriter(), [[1, 2, 3]])


# ---- name round-tripping -------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "Variant(Int32, String)",
        "Variant(Int64, String, Date)",
        "Variant(Array(Int32), Nullable(String))",
        "Variant(Tuple(Int32, String), UInt64)",
        "Dynamic",
        "Dynamic(max_types=8)",
    ],
)
def test_codec_name_round_trips_through_parser(spec: str) -> None:
    # BEGIN: the canonical type spec
    # WHEN: parsing it
    codec = parse_type(spec)

    # THEN: the codec's `name` reproduces the spec verbatim
    assert codec.name == spec


# ---- helpers -------------------------------------------------------------


async def _round_trip_dynamic(codec: ColumnCodec, values: list[object]) -> list[object]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return await codec.read(_reader(writer.getvalue()), len(values))
