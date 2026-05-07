"""Codecs for `Decimal{32,64,128,256}(scale)` and the dispatch wrapper
`Decimal(precision, scale)`.

A ClickHouse decimal is stored as a fixed-width signed integer holding
`value * 10**scale`. Width is determined by precision (the number of
significant decimal digits):

  P ≤ 9   → 4 bytes (Decimal32)
  P ≤ 18  → 8 bytes (Decimal64)
  P ≤ 38  → 16 bytes (Decimal128)
  P ≤ 76  → 32 bytes (Decimal256)

Block headers can spell decimals either as `Decimal(P, S)` (we dispatch
to the right size) or as the explicit `Decimal32(S)`/`…(S)` form
(direct lookup). Both work.
"""

from __future__ import annotations

from decimal import Decimal as PyDecimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter

# Storage-width breakpoints for the `Decimal(P, S)` dispatcher.
# ClickHouse caps total decimal precision at 76 digits.
_DECIMAL32_MAX_PRECISION = 9
_DECIMAL64_MAX_PRECISION = 18
_DECIMAL128_MAX_PRECISION = 38
_MAX_DECIMAL_PRECISION = 76


class _DecimalCodec:
    """Decimal codec parameterised by its byte width."""

    null_value: PyDecimal = PyDecimal(0)
    python_type: type = PyDecimal
    _size: int = 0

    def __init__(self, scale: int) -> None:
        if scale < 0:
            raise ValueError(f"Decimal scale must be non-negative, got {scale}")
        self.scale = scale
        self._scale_factor = PyDecimal(10) ** scale
        self.name = f"Decimal{self._size * 8}({scale})"

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[PyDecimal]:
        if n_rows == 0:
            return []
        size = self._size
        scale_factor = self._scale_factor
        data = await reader.read_exact(size * n_rows)
        out: list[PyDecimal] = []
        for i in range(n_rows):
            raw = int.from_bytes(data[i * size : (i + 1) * size], "little", signed=True)
            out.append(PyDecimal(raw) / scale_factor)
        return out

    def write(self, writer: BinaryWriter, values: Sequence[PyDecimal]) -> None:
        if not values:
            return
        size = self._size
        scale_factor = self._scale_factor
        out = bytearray()
        for v in values:
            scaled = int(PyDecimal(v) * scale_factor)
            out.extend(scaled.to_bytes(size, "little", signed=True))
        writer.write_raw(bytes(out))


class Decimal32(_DecimalCodec):
    _size = 4


class Decimal64(_DecimalCodec):
    _size = 8


class Decimal128(_DecimalCodec):
    _size = 16


class Decimal256(_DecimalCodec):
    _size = 32


def make_decimal(precision: int, scale: int) -> _DecimalCodec:
    """Dispatcher for the `Decimal(P, S)` block-header spelling.

    Returns the appropriate fixed-width codec for the precision; the
    resulting codec's `name` reads `Decimal(P, S)` rather than
    `Decimal32(S)` so round-tripping the spec preserves the form the
    server sent.
    """

    if precision < 1 or precision > _MAX_DECIMAL_PRECISION:
        raise ValueError(
            f"Decimal precision out of range [1, {_MAX_DECIMAL_PRECISION}], "
            f"got {precision}"
        )
    if precision <= _DECIMAL32_MAX_PRECISION:
        codec: _DecimalCodec = Decimal32(scale)
    elif precision <= _DECIMAL64_MAX_PRECISION:
        codec = Decimal64(scale)
    elif precision <= _DECIMAL128_MAX_PRECISION:
        codec = Decimal128(scale)
    else:
        codec = Decimal256(scale)
    codec.name = f"Decimal({precision}, {scale})"
    return codec
