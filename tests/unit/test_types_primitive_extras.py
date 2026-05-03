"""Round-trip tests for the rest of the v0 primitive matrix:

- All signed/unsigned ints (Int8/16/32/64/128/256, UInt variants), Bool,
  Float32/64.
- FixedString(N) — including padding and over-length rejection.
- Decimal{32,64,128,256}(S) and the Decimal(P, S) dispatcher.
- Date, Date32, DateTime, DateTime('TZ'), DateTime64(p), DateTime64(p, 'TZ').
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal as PyDecimal
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.datetime import (
    DateTime,
    DateTime64,
    HighPrecisionTimestamp,
)
from clickhouse_async.types.decimal import (
    Decimal32,
    Decimal64,
    Decimal128,
    make_decimal,
)
from clickhouse_async.types.string import FixedString

if TYPE_CHECKING:
    from collections.abc import Sequence


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


async def _round_trip(codec: ColumnCodec, values: Sequence[Any]) -> list[Any]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return await codec.read(_reader(writer.getvalue()), len(values))


# ---- integer matrix ------------------------------------------------------


@pytest.mark.parametrize(
    "spec,values",
    [
        ("Int8", [-128, -1, 0, 1, 127]),
        ("Int16", [-(2**15), -1, 0, 2**15 - 1]),
        ("Int32", [-(2**31), -1, 0, 2**31 - 1]),
        ("Int64", [-(2**63), -1, 0, 2**63 - 1]),
        ("Int128", [-(2**127), -1, 0, 2**127 - 1]),
        ("Int256", [-(2**255), -1, 0, 2**255 - 1]),
        ("UInt8", [0, 1, 255]),
        ("UInt16", [0, 1, 2**16 - 1]),
        ("UInt32", [0, 1, 2**32 - 1]),
        ("UInt64", [0, 1, 2**64 - 1]),
        ("UInt128", [0, 1, 2**128 - 1]),
        ("UInt256", [0, 1, 2**256 - 1]),
    ],
)
async def test_integer_round_trip(spec: str, values: list[int]) -> None:
    # BEGIN: a parsed codec for the spec and edge-aligned values for its width
    codec = parse_type(spec)

    # WHEN: writing then reading back through the codec
    decoded = await _round_trip(codec, values)

    # THEN: every value round-trips identically
    assert decoded == values


# ---- floats --------------------------------------------------------------


async def test_float64_round_trip_preserves_precision() -> None:
    # BEGIN: a Float64 codec and values spanning the IEEE 754 range
    codec = parse_type("Float64")
    values = [0.0, -0.0, 1.5, -1.5, 1e-300, 1e300, 1.0 / 3.0]

    # WHEN: writing then reading back
    decoded = await _round_trip(codec, values)

    # THEN: every value round-trips bit-for-bit (including signed zero)
    assert decoded == values
    assert str(decoded[1]) == "-0.0"


async def test_float32_round_trip_within_precision() -> None:
    # BEGIN: a Float32 codec and values that survive 32-bit rounding cleanly
    codec = parse_type("Float32")
    values = [0.0, 1.0, -1.0, 0.5, -0.5, 2.5]

    # WHEN: writing then reading back
    decoded = await _round_trip(codec, values)

    # THEN: clean halves and integers survive exactly
    assert decoded == values


# ---- bool ----------------------------------------------------------------


async def test_bool_round_trip() -> None:
    # BEGIN: a Bool codec and a mix of True/False
    codec = parse_type("Bool")
    values = [True, False, True, True, False]

    # WHEN: writing then reading back
    decoded = await _round_trip(codec, values)

    # THEN: every value is a Python bool (not int) and round-trips
    assert decoded == values
    assert all(isinstance(v, bool) for v in decoded)


# ---- FixedString ---------------------------------------------------------


async def test_fixed_string_round_trip_pads_short_inputs_with_nul() -> None:
    # BEGIN: a FixedString(5) codec and inputs of varying length ≤ 5
    codec = parse_type("FixedString(5)")
    values = [b"hello", b"hi", b"", b"\xff\x00\xab\xcd\xef"]

    # WHEN: writing then reading back
    decoded = await _round_trip(codec, values)

    # THEN: short inputs come back NUL-padded to length 5 (not their original
    #       length) — the on-wire format is fixed, no length is preserved
    assert decoded == [
        b"hello",
        b"hi\x00\x00\x00",
        b"\x00\x00\x00\x00\x00",
        b"\xff\x00\xab\xcd\xef",
    ]


def test_fixed_string_rejects_over_length_input() -> None:
    # BEGIN: a FixedString(3) codec
    codec = FixedString(3)
    writer = BinaryWriter()

    # WHEN: writing a value longer than the declared capacity
    # THEN: a ValueError surfaces — silent truncation would lose data
    with pytest.raises(ValueError, match="exceeds capacity"):
        codec.write(writer, [b"too long"])


# ---- Decimal -------------------------------------------------------------


@pytest.mark.parametrize(
    "spec,codec_cls",
    [
        ("Decimal32(4)", Decimal32),
        ("Decimal64(4)", Decimal64),
        ("Decimal128(4)", Decimal128),
        ("Decimal(9, 4)", Decimal32),  # precision dispatch
        ("Decimal(18, 4)", Decimal64),
        ("Decimal(38, 4)", Decimal128),
    ],
)
async def test_decimal_dispatches_to_correct_storage_size(
    spec: str, codec_cls: type
) -> None:
    # BEGIN: a parsed decimal spec exercising both spelling conventions
    codec = parse_type(spec)

    # WHEN: round-tripping a representative value at the declared scale
    values = [PyDecimal("0"), PyDecimal("3.1416"), PyDecimal("-99.0001")]
    decoded = await _round_trip(codec, values)

    # THEN: the codec is the storage-size class implied by precision, and
    #       the decimal values round-trip exactly at the declared scale
    assert isinstance(codec, codec_cls)
    assert decoded == values


async def test_decimal_round_trips_at_full_scale_negative_values() -> None:
    # BEGIN: a Decimal64(8) codec, max-scale negative values
    codec = make_decimal(precision=18, scale=8)

    values = [
        PyDecimal("0.00000001"),
        PyDecimal("-0.00000001"),
        PyDecimal("9999999999.99999999"),
        PyDecimal("-9999999999.99999999"),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: all eight digits of scale survive, signed values too
    assert decoded == values


def test_decimal_rejects_out_of_range_precision() -> None:
    # BEGIN: an out-of-range precision (ClickHouse caps at 76)
    # WHEN: constructing the dispatcher
    # THEN: a ValueError surfaces with the offending value
    with pytest.raises(ValueError, match="precision out of range"):
        make_decimal(precision=77, scale=0)


# ---- Date / Date32 ------------------------------------------------------


async def test_date_round_trip() -> None:
    # BEGIN: a Date codec and dates inside its 1970-2149 range
    codec = parse_type("Date")
    values = [
        date(1970, 1, 1),
        date(2026, 5, 2),
        date(2149, 6, 6),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every date survives
    assert decoded == values


async def test_date32_round_trip_pre_epoch() -> None:
    # BEGIN: a Date32 codec and dates spanning before and after 1970
    codec = parse_type("Date32")
    values = [
        date(1900, 1, 1),
        date(1969, 12, 31),
        date(1970, 1, 1),
        date(1970, 1, 2),
        date(2026, 5, 2),
        date(2299, 12, 31),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: pre-epoch dates (negative day count) survive too
    assert decoded == values


# ---- DateTime -----------------------------------------------------------


async def test_datetime_naive_treated_as_utc() -> None:
    # BEGIN: a bare DateTime codec (no tz) and naive datetimes
    codec = parse_type("DateTime")
    values = [
        datetime(1970, 1, 1, 0, 0, 0),
        datetime(2026, 5, 2, 12, 34, 56),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: datetimes come back naive but represent the same instant when
    #       interpreted as UTC
    assert decoded == values
    assert all(d.tzinfo is None for d in decoded)


async def test_datetime_with_named_timezone_preserves_zone() -> None:
    # BEGIN: a DateTime('Europe/Madrid') codec and an aware datetime
    codec = parse_type("DateTime('Europe/Madrid')")
    madrid = ZoneInfo("Europe/Madrid")
    values = [datetime(2026, 5, 2, 12, 0, 0, tzinfo=madrid)]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: the value is still aware and represents the same instant
    assert isinstance(codec, DateTime)
    assert decoded[0] == values[0]
    assert decoded[0].tzinfo is not None
    assert decoded[0].utcoffset() == values[0].utcoffset()


async def test_datetime_naming_round_trips() -> None:
    # BEGIN: codecs with and without an explicit timezone
    bare = parse_type("DateTime")
    aware = parse_type("DateTime('UTC')")

    # WHEN: reading the codec name
    # THEN: the name spells the type the same way the server spelled it
    assert bare.name == "DateTime"
    assert aware.name == "DateTime('UTC')"


# ---- DateTime64 ---------------------------------------------------------


@pytest.mark.parametrize(
    "precision,sub_second_micro",
    [
        (0, 0),  # whole seconds only
        (3, 123_000),  # millisecond — 123 ticks → 123_000 microseconds
        (6, 123_456),  # microsecond — exact match for Python's resolution
    ],
)
async def test_datetime64_round_trip_at_supported_precisions(
    precision: int, sub_second_micro: int
) -> None:
    # BEGIN: a DateTime64(p, 'UTC') codec at a precision Python can hold exactly
    codec = parse_type(f"DateTime64({precision}, 'UTC')")
    values = [
        datetime(2026, 5, 2, 12, 0, 0, tzinfo=UTC),
        datetime(2026, 5, 2, 12, 0, 0, microsecond=sub_second_micro, tzinfo=UTC),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: values come back identical, including any sub-second component
    #       that fits in the codec's precision
    assert isinstance(codec, DateTime64)
    assert decoded == values


def test_datetime64_rejects_out_of_range_precision() -> None:
    # BEGIN: a DateTime64 with an unsupported precision
    # WHEN: constructing
    # THEN: a ValueError surfaces with the offending value
    with pytest.raises(ValueError, match="precision must be in"):
        DateTime64(precision=10)


# ---- DateTime64 high precision (p > 6) -----------------------------------


@pytest.mark.parametrize("precision", [7, 8, 9])
async def test_datetime64_high_precision_round_trip_preserves_ticks(
    precision: int,
) -> None:
    # BEGIN: a DateTime64(p) codec at a precision Python's `datetime`
    #        can't hold — `precision > 6` defaults to high_precision=True
    codec = parse_type(f"DateTime64({precision})")
    assert isinstance(codec, DateTime64)
    assert codec.high_precision is True
    # A representative tick count with the *full* precision populated
    # so a lossy datetime conversion would drop digits.
    ticks = 1_756_812_345_000_000_000 + (1 if precision == 9 else 0)
    values: list[HighPrecisionTimestamp] = [
        HighPrecisionTimestamp(ticks=ticks, scale=precision),
        HighPrecisionTimestamp(ticks=ticks + 7, scale=precision),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: ticks survive byte-for-byte
    assert decoded == values


async def test_datetime64_high_precision_false_falls_back_to_datetime() -> None:
    # BEGIN: opt out of high_precision at p=9 — the codec returns
    #        ``datetime`` values (lossy) for callers that prefer the
    #        v0 shape
    codec = DateTime64(precision=9, timezone="UTC", high_precision=False)
    values = [datetime(2026, 5, 2, 12, 0, 0, microsecond=123_456, tzinfo=UTC)]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: the plain `datetime` round-trips (sub-microsecond zeroed
    #       since the input had none anyway)
    assert decoded == values


def test_high_precision_timestamp_to_datetime_is_lossy_above_micro() -> None:
    # BEGIN: a HighPrecisionTimestamp at scale 9 (nanoseconds)
    ts = HighPrecisionTimestamp(ticks=1_756_812_345_123_456_789, scale=9)

    # WHEN: converting to ``datetime``
    dt = ts.to_datetime()

    # THEN: the ``datetime`` carries microseconds (123_456) — the bottom
    #       three nanosecond digits truncate at the Python boundary
    assert dt.microsecond == 123_456
    # Round-trip from datetime back to a HighPrecisionTimestamp at the
    # same scale fills sub-microsecond ticks with zeros.
    rebuilt = HighPrecisionTimestamp.from_datetime(dt, scale=9)
    assert rebuilt.ticks == 1_756_812_345_123_456_000


def test_datetime64_write_rejects_mismatched_high_precision_scale() -> None:
    # BEGIN: a HighPrecisionTimestamp at the wrong scale
    codec = DateTime64(precision=9)

    # WHEN / THEN: the write path refuses rather than silently
    #              re-scaling
    with pytest.raises(ValueError, match=r"scale .* does not match"):
        codec.write(BinaryWriter(), [HighPrecisionTimestamp(ticks=1, scale=6)])


# ---- DateTime / DateTime64 session_timezone fallback ---------------------


def test_bare_datetime_uses_session_timezone_fallback() -> None:
    # BEGIN: a bare ``DateTime`` parsed with a session_timezone
    codec = parse_type("DateTime", session_timezone="Europe/Berlin")
    assert isinstance(codec, DateTime)

    # WHEN / THEN: the codec carries the session zone in its effective
    #              tz; ``codec.name`` still round-trips the bare form
    assert codec.timezone_name == "Europe/Berlin"
    assert codec.name == "DateTime"


def test_explicit_datetime_timezone_beats_session_timezone() -> None:
    # BEGIN: a parametric DateTime('UTC') with a session tz that differs
    codec = parse_type("DateTime('UTC')", session_timezone="Europe/Berlin")

    # WHEN / THEN: the explicit tz wins; the type-spec form survives
    assert isinstance(codec, DateTime)
    assert codec.timezone_name == "UTC"
    assert codec.name == "DateTime('UTC')"


def test_bare_datetime64_uses_session_timezone_fallback() -> None:
    # BEGIN: a bare DateTime64(3) parsed with a session_timezone
    codec = parse_type("DateTime64(3)", session_timezone="Europe/Berlin")
    assert isinstance(codec, DateTime64)

    # WHEN / THEN: the codec carries the session zone; ``.name`` still
    #              round-trips the bare form (without timezone)
    assert codec.timezone_name == "Europe/Berlin"
    assert codec.name == "DateTime64(3)"


async def test_datetime_session_timezone_decodes_aware() -> None:
    # BEGIN: a bare ``DateTime`` codec with a session zone of Berlin —
    #        the wire payload is UTC seconds; the decode should land
    #        in the Berlin offset
    codec = parse_type("DateTime", session_timezone="Europe/Berlin")
    assert isinstance(codec, DateTime)
    # 2026-05-02 12:00:00 UTC = 14:00:00 Berlin (CEST, +02:00 in May)
    values_in = [datetime(2026, 5, 2, 12, 0, 0, tzinfo=UTC)]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values_in)

    # THEN: the read value is aware in Europe/Berlin
    assert len(decoded) == 1
    out = decoded[0]
    assert out.tzinfo is not None
    assert out.utcoffset() == timedelta(hours=2)
    assert out.hour == 14


# ---- empty-batch invariants for every new codec -------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "Int8",
        "UInt256",
        "Float32",
        "Float64",
        "Bool",
        "FixedString(5)",
        "Decimal32(2)",
        "Decimal(38, 4)",
        "Date",
        "Date32",
        "DateTime",
        "DateTime('UTC')",
        "DateTime64(3)",
        "DateTime64(6, 'UTC')",
    ],
)
async def test_empty_batch_round_trip(spec: str) -> None:
    # BEGIN: a parsed codec
    codec = parse_type(spec)

    # WHEN: round-tripping zero rows
    decoded = await _round_trip(codec, [])

    # THEN: nothing is read or written, and an empty list comes back
    assert decoded == []
