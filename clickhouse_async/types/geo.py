"""Codecs for ClickHouse's geo aliases.

The four geo types are pure aliases over the existing ``Tuple`` /
``Array`` shapes; the wire format is identical to the desugared
spelling. Each class here is a thin wrapper that forwards ``read`` /
``write`` / ``null_value`` to an inner codec and overrides ``name``
to the alias spelling so server-emitted headers and client-built
type strings match.

| Alias          | Underlying shape            | Python representation                    |
| -------------- | --------------------------- | ---------------------------------------- |
| ``Point``      | ``Tuple(Float64, Float64)`` | ``tuple[float, float]``                  |
| ``Ring``       | ``Array(Point)``            | ``list[tuple[float, float]]``            |
| ``Polygon``    | ``Array(Ring)``             | ``list[list[tuple[float, float]]]``      |
| ``MultiPolygon`` | ``Array(Polygon)``        | ``list[list[list[tuple[float, float]]]]`` |

ClickHouse may emit either spelling in block headers depending on the
query and version; the parser produces ``Point`` / ``Ring`` / etc. for
the alias names and falls back to ``Tuple(...)`` / ``Array(...)`` for
the desugared forms.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from clickhouse_async.types.composite import Array, Tuple
from clickhouse_async.types.primitive import Float64

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class Point:
    """``Point`` — alias for ``Tuple(Float64, Float64)``."""

    name = "Point"
    null_value: tuple[float, float] = (0.0, 0.0)

    def __init__(self) -> None:
        self._inner: Tuple = Tuple(Float64(), Float64())

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[tuple[float, float]]:
        return await self._inner.read(reader, n_rows)

    def write(self, writer: BinaryWriter, values: Sequence[Sequence[Any]]) -> None:
        self._inner.write(writer, values)


class Ring:
    """``Ring`` — alias for ``Array(Point)``."""

    name = "Ring"
    null_value: list[tuple[float, float]] = []  # noqa: RUF012

    def __init__(self) -> None:
        self._inner: Array = Array(Point())

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[list[tuple[float, float]]]:
        return await self._inner.read(reader, n_rows)

    def write(self, writer: BinaryWriter, values: Sequence[Sequence[Any]]) -> None:
        self._inner.write(writer, values)


class Polygon:
    """``Polygon`` — alias for ``Array(Ring)``."""

    name = "Polygon"
    null_value: list[list[tuple[float, float]]] = []  # noqa: RUF012

    def __init__(self) -> None:
        self._inner: Array = Array(Ring())

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[list[list[tuple[float, float]]]]:
        return await self._inner.read(reader, n_rows)

    def write(self, writer: BinaryWriter, values: Sequence[Sequence[Any]]) -> None:
        self._inner.write(writer, values)


class MultiPolygon:
    """``MultiPolygon`` — alias for ``Array(Polygon)``."""

    name = "MultiPolygon"
    null_value: list[list[list[tuple[float, float]]]] = []  # noqa: RUF012

    def __init__(self) -> None:
        self._inner: Array = Array(Polygon())

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[list[list[list[tuple[float, float]]]]]:
        return await self._inner.read(reader, n_rows)

    def write(self, writer: BinaryWriter, values: Sequence[Sequence[Any]]) -> None:
        self._inner.write(writer, values)
