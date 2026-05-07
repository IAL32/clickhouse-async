"""Round-trip and byte-layout tests for `Enum8`, `Enum16`,
`LowCardinality(T)`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.composite import LowCardinality
from clickhouse_async.types.enums import Enum8, Enum16

if TYPE_CHECKING:
    from collections.abc import Sequence


def _reader(data: bytes) -> SyncBinaryReader:
    return SyncBinaryReader(bytes(data))


async def _round_trip(codec: ColumnCodec, values: Sequence[Any]) -> list[Any]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return codec.read(_reader(writer.getvalue()), len(values))


# ---- Enum8 / Enum16 -----------------------------------------------------


async def test_enum8_round_trip_via_parser() -> None:
    # BEGIN: an Enum8 spec parsed from the canonical block-header form
    codec = parse_type("Enum8('red' = 1, 'green' = 2, 'blue' = -3)")
    values = ["red", "blue", "green", "red"]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every label survives, the codec is Enum8
    assert isinstance(codec, Enum8)
    assert decoded == values


async def test_enum16_round_trip_via_parser() -> None:
    # BEGIN: an Enum16 spec with a value range that doesn't fit in Int8
    codec = parse_type("Enum16('a' = -1000, 'b' = 0, 'c' = 32000)")
    values = ["c", "a", "b"]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: large signed values survive
    assert isinstance(codec, Enum16)
    assert decoded == values


async def test_enum8_byte_layout_is_signed_int8_per_row() -> None:
    # BEGIN: an Enum8 with a known mapping
    codec = parse_type("Enum8('a' = 1, 'b' = -3)")

    # WHEN: encoding rows
    writer = BinaryWriter()
    codec.write(writer, ["a", "b", "a"])

    # THEN: each row is the signed Int8 value of its label
    assert writer.getvalue() == bytes([1, 0xFD, 1])  # -3 as Int8 → 0xFD


def test_enum_rejects_unknown_label_on_write() -> None:
    # BEGIN: an Enum8 codec
    codec = Enum8({"a": 1, "b": 2})

    # WHEN: writing a label not in the mapping
    # THEN: a ValueError surfaces with the offending label
    writer = BinaryWriter()
    with pytest.raises(ValueError, match="unknown"):
        codec.write(writer, ["c"])


async def test_enum_rejects_unknown_value_on_read() -> None:
    # BEGIN: an Enum8 codec and a stream containing an unmapped Int8 value
    codec = Enum8({"a": 1, "b": 2})

    # WHEN: reading bytes that don't correspond to any mapped value
    # THEN: a ProtocolError surfaces, naming the offending row
    with pytest.raises(ProtocolError, match="unknown"):
        codec.read(_reader(bytes([99])), 1)


def test_enum_rejects_duplicate_values() -> None:
    # BEGIN: a mapping with two labels sharing a value
    # WHEN: constructing
    # THEN: a ValueError surfaces
    with pytest.raises(ValueError, match="duplicate values"):
        Enum8({"a": 1, "b": 1})


def test_enum_parser_recognises_label_value_pairs() -> None:
    # BEGIN: a parsed enum codec
    codec = parse_type("Enum8('hi there' = 5, 'with, comma' = 6)")

    # WHEN: inspecting its mapping
    # THEN: labels with embedded spaces and commas survive parsing
    assert isinstance(codec, Enum8)
    assert codec.mapping == {"hi there": 5, "with, comma": 6}


# ---- LowCardinality ----------------------------------------------------


async def test_low_cardinality_round_trip_dedupes() -> None:
    # BEGIN: a LowCardinality(String) codec and rows with heavy repetition
    codec = parse_type("LowCardinality(String)")
    values = ["red", "blue", "red", "green", "red", "blue"]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row comes back identical, dedupe is internal
    assert isinstance(codec, LowCardinality)
    assert decoded == values


async def test_low_cardinality_picks_smallest_index_width() -> None:
    # BEGIN: a small dictionary (3 unique values) with many rows
    codec = parse_type("LowCardinality(Int32)")
    values = [1, 2, 3] * 100

    # WHEN: encoding
    writer = BinaryWriter()
    codec.write(writer, values)
    encoded = writer.getvalue()

    # THEN: serialisation_type's low byte is 0 (UInt8 indices), since the
    #       dictionary fits in 256 entries; the indices region occupies
    #       len(values) bytes
    sertype = int.from_bytes(encoded[8:16], "little", signed=False)
    assert sertype & 0xFF == 0


async def test_low_cardinality_round_trip_int_values() -> None:
    # BEGIN: a LowCardinality(UInt32) codec
    codec = parse_type("LowCardinality(UInt32)")
    values = [10, 20, 10, 30, 20, 30, 10]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: hashable scalars round-trip through the dictionary
    assert decoded == values


async def test_low_cardinality_nullable_string_round_trips_mixed_values() -> None:
    # BEGIN: a LowCardinality(Nullable(String)) codec
    codec = parse_type("LowCardinality(Nullable(String))")
    assert codec.name == "LowCardinality(Nullable(String))"
    values: list[str | None] = [
        "alpha",
        None,
        "beta",
        "alpha",
        None,
        "gamma",
    ]

    # WHEN: round-tripping through the codec
    decoded = await _round_trip(codec, values)

    # THEN: every row comes back identically — Nones map to dictionary
    #       slot 0, deduped non-null values to slots 1+
    assert decoded == values


async def test_low_cardinality_nullable_all_null_round_trips() -> None:
    # BEGIN: an all-null column
    codec = parse_type("LowCardinality(Nullable(String))")
    values: list[str | None] = [None] * 5

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row decodes back to None — index 0 mapped, dictionary
    #       holds only the placeholder
    assert decoded == values


async def test_low_cardinality_nullable_all_non_null_round_trips() -> None:
    # BEGIN: a column with no nulls; the placeholder at index 0 still
    #        travels on the wire but is never indexed
    codec = parse_type("LowCardinality(Nullable(Int32))")
    values: list[int | None] = [1, 2, 3, 1, 2]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: integers come back unchanged
    assert decoded == values


async def test_low_cardinality_nullable_wire_format_pin() -> None:
    # BEGIN / WHEN: hand-encode a known mixed-values column then decode
    #     the captured bytes — pins the wire layout so a future codec
    #     change can't silently desync
    codec = parse_type("LowCardinality(Nullable(String))")
    values: list[str | None] = ["x", None, "y", "x"]

    writer = BinaryWriter()
    codec.write(writer, values)
    written = writer.getvalue()

    # First 24 bytes are version + sertype + dict_size — all UInt64 LE.
    version = int.from_bytes(written[:8], "little")
    sertype = int.from_bytes(written[8:16], "little")
    dict_size = int.from_bytes(written[16:24], "little")
    # THEN: key_version is 1 (SharedDictionariesWithAdditionalKeys);
    #       sertype carries HasAdditionalKeys | NeedGlobalDictionary |
    #       NeedUpdateDictionary plus the UInt8 index-width tag (0);
    #       the dictionary holds the placeholder + 2 distinct strings.
    assert version == 1
    assert sertype == 0x0000_0000_0000_0600
    assert dict_size == 3

    # And the body decodes back to the same values
    decoded = codec.read(SyncBinaryReader(written), len(values))
    assert decoded == values


@pytest.mark.parametrize(
    "spec",
    [
        "Enum8('a' = 1)",
        "Enum16('a' = -1, 'b' = 1)",
        "LowCardinality(String)",
        "LowCardinality(Int32)",
    ],
)
async def test_empty_batch_round_trip(spec: str) -> None:
    # BEGIN: a parsed codec
    codec = parse_type(spec)

    # WHEN: round-tripping zero rows
    decoded = await _round_trip(codec, [])

    # THEN: nothing is read or written, and an empty list comes back
    assert decoded == []


# ---- name round-tripping -------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "Enum8('a' = 1, 'b' = 2)",
        "Enum16('alpha' = 100, 'beta' = -200)",
        "LowCardinality(String)",
        "LowCardinality(Int64)",
    ],
)
def test_codec_name_round_trips_through_parser(spec: str) -> None:
    # BEGIN: a canonical type spec
    # WHEN: parsing it
    codec = parse_type(spec)

    # THEN: the codec's `name` reproduces the spec verbatim
    assert codec.name == spec
