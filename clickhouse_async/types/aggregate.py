"""Codec for ``AggregateFunction(func, arg1, arg2, …)`` state columns.

These are the opaque-state columns ClickHouse uses for materialised
views and ``-State`` aggregate suffixes. The wire format is **per-row
bytes**, where each row's structure is determined by ``func``'s own
``serialize`` / ``deserialize`` implementation server-side. There's
no generic length prefix at the column level — upstream
``DataTypeAggregateFunction::serializeBinaryBulk`` simply concatenates
``function->serialize(buf, place)`` outputs — so the client has to
know the per-row structure for each function it wants to round-trip.

v0.2 ships with per-function readers for a small, common set of
aggregates — enough to round-trip the single most-used MV pipeline
(``avgState`` → ``avgMerge``) — and raises a clear
``NotImplementedError`` for anything outside the table. Adding a new
function is a few lines: register a reader in ``_READERS``.

Use case: a SELECT against an MV's state column produces opaque
bytes; feed those bytes directly into an INSERT into another
``AggregateFunction(...)`` column with the same ``func`` to
copy / repartition state. The bytes are never introspectable at
the Python level.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
    from clickhouse_async.types.base import ColumnCodec


_StateReader = Callable[["AsyncBinaryReader"], Awaitable[bytes]]

# LEB128 varuint continuation marker — bit 7 = "more bytes follow".
# Mirrored from ``clickhouse_async.protocol.io`` to keep this module
# self-contained (the io module only exposes a decoded-int reader,
# not a raw-bytes reader).
_VARUINT_CONTINUATION_BIT = 0x80


async def _read_varuint_bytes(reader: AsyncBinaryReader) -> bytes:
    """Read a LEB128 varuint and return its raw byte encoding.

    We round-trip the bytes verbatim back into INSERTs, so we need
    the *encoding* (not the decoded integer) to keep state
    re-inserts exact.
    """
    out = bytearray()
    while True:
        b = await reader.read_byte()
        out.append(b)
        if b < _VARUINT_CONTINUATION_BIT:
            return bytes(out)


async def _read_avg_state(reader: AsyncBinaryReader) -> bytes:
    """``avg`` state: ``Float64 numerator`` + ``varuint denominator``.

    The total length is variable (the denominator is varuint-encoded
    in upstream ``AggregateFunctionAvgBase::serialize``), so we read
    each piece by hand and return the concatenated bytes.
    """
    numerator = await reader.read_exact(8)
    denom_bytes = await _read_varuint_bytes(reader)
    return bytes(numerator) + denom_bytes


async def _read_count_state(reader: AsyncBinaryReader) -> bytes:
    """``count`` state: a single ``UInt64`` row counter (8 bytes)."""
    return bytes(await reader.read_exact(8))


# Per-function readers. Keys are the bare function names (no parens
# / literal args). Adding a new aggregate is a one-line registration
# of a callable that knows how to bound one row's state on the wire.
_READERS: dict[str, _StateReader] = {
    "avg": _read_avg_state,
    "count": _read_count_state,
}


class AggregateFunction:
    """``AggregateFunction(func, arg1, arg2, …)`` — opaque per-row state.

    ``read`` returns ``list[bytes]``; ``write`` accepts the same and
    writes the bytes back verbatim — server-side ``deserialize``
    sees identical bytes to what its own ``serialize`` would have
    produced. Unknown functions raise ``NotImplementedError`` with
    the function name so the user can either upgrade clickhouse-async
    or run the merge server-side via ``-Merge`` without round-tripping
    the bytes through Python.
    """

    null_value: bytes = b""

    def __init__(
        self,
        function_call: str,
        arg_types: list[ColumnCodec],
    ) -> None:
        # ``function_call`` is the verbatim leading-param string —
        # ``avg``, ``quantilesTDigest(0.5, 0.9)`` etc. We don't try to
        # parse the parametric args; only the bare function name
        # (everything before any ``(``) drives the reader lookup.
        self.function_call = function_call.strip()
        self.arg_types = arg_types
        # Bare function name — the lookup key for the readers table.
        paren = self.function_call.find("(")
        self.function_name = (
            self.function_call[:paren] if paren != -1 else self.function_call
        ).strip()
        arg_part = ", ".join(t.name for t in arg_types)
        if arg_part:
            self.name = f"AggregateFunction({self.function_call}, {arg_part})"
        else:
            self.name = f"AggregateFunction({self.function_call})"

    def _reader(self) -> _StateReader:
        """Return the per-row state reader for this function or raise
        ``NotImplementedError`` naming the function and the workaround."""
        rd = _READERS.get(self.function_name)
        if rd is None:
            raise NotImplementedError(
                f"AggregateFunction({self.function_call}, …): the wire "
                f"format for '{self.function_name}' state is not "
                f"implemented in clickhouse-async yet. Known aggregates: "
                f"{sorted(_READERS)}. Workaround: run the matching "
                f"'-Merge' aggregate server-side and select the merged "
                f"value instead of round-tripping the raw state bytes "
                f"through Python."
            )
        return rd

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[bytes]:
        if n_rows == 0:
            return []
        rd = self._reader()
        return [await rd(reader) for _ in range(n_rows)]

    def write(self, writer: BinaryWriter, values: Sequence[bytes]) -> None:
        if not values:
            return
        # Even though we just write the bytes verbatim, refuse on an
        # unknown aggregate so the user gets the same diagnostic on
        # both sides of the round-trip.
        self._reader()
        for v in values:
            writer.write_raw(v)
