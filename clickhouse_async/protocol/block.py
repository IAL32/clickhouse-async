"""Read and write the columnar Block format.

Every result row batch and every ``INSERT`` payload is wrapped in a
Block. The wire layout (mirrors upstream ``Formats/NativeReader.cpp``
and ``NativeWriter.cpp``):

1. **BlockInfo** — numbered TLV-style fields ending in a varuint ``0``
   sentinel (see ``Core/BlockInfo.cpp`` for the canonical layout). At
   ``OUR_REVISION`` the live fields are ``is_overflows`` (bool, field
   1) and ``bucket_num`` (Int32, field 2). Higher-numbered fields
   (e.g. ``out_of_order_buckets``) are gated above our revision and we
   treat them as a protocol error if a server emits them — the
   handshake contract says it shouldn't.
2. **Varuint ``n_columns``**, then **varuint ``n_rows``**.
3. For each column, in declared order:
   - Length-prefixed UTF-8 column name.
   - Length-prefixed UTF-8 type spec (``Array(Nullable(String))`` etc.).
   - At ``revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION``: a
     1-byte custom-serialization flag. v0 only emits/accepts ``0``;
     non-zero raises ``ProtocolError``.
   - The column body — ``codec.read(reader, n_rows)`` /
     ``codec.write(writer, values)``.

Empty blocks and header-only blocks (``n_rows = 0``, any column count)
are valid and used by the server to signal the column metadata of a
``SELECT`` before streaming any rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.packets import (
    DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION,
)
from clickhouse_async.types import ColumnCodec, parse_type

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter

# BlockInfo TLV field numbers (canonical layout, ``Core/BlockInfo.cpp``).
_BLOCK_INFO_FIELD_TERMINATOR = 0
_BLOCK_INFO_FIELD_IS_OVERFLOWS = 1
_BLOCK_INFO_FIELD_BUCKET_NUM = 2


@dataclass
class BlockInfo:
    """The numbered metadata fields preceding every Block on the wire.

    Defaults match upstream's defaults; an empty block info (defaults
    only) round-trips through ``write_block_info`` /
    ``read_block_info`` with the same byte layout the server emits.
    """

    is_overflows: bool = False
    bucket_num: int = -1


@dataclass
class ColumnSpec:
    """The per-column header inside a Block: its name, type-spec string
    (verbatim from the wire), and the codec instantiated from that
    spec."""

    name: str
    type_spec: str
    codec: ColumnCodec


@dataclass
class Block:
    """A columnar batch — header metadata plus the per-column data."""

    info: BlockInfo = field(default_factory=BlockInfo)
    columns: list[ColumnSpec] = field(default_factory=list)
    n_rows: int = 0
    data: list[list[Any]] = field(default_factory=list)


# ---- BlockInfo -------------------------------------------------------------


async def read_block_info(reader: AsyncBinaryReader) -> BlockInfo:
    info = BlockInfo()
    while True:
        field_num = await reader.read_varuint()
        if field_num == _BLOCK_INFO_FIELD_TERMINATOR:
            return info
        if field_num == _BLOCK_INFO_FIELD_IS_OVERFLOWS:
            info.is_overflows = (await reader.read_byte()) != 0
        elif field_num == _BLOCK_INFO_FIELD_BUCKET_NUM:
            info.bucket_num = await reader.read_int(4, signed=True)
        else:
            raise ProtocolError(
                f"unknown BlockInfo field number {field_num} at offset "
                f"{reader.position}; the negotiated revision should not "
                f"have produced it"
            )


def write_block_info(writer: BinaryWriter, info: BlockInfo) -> None:
    writer.write_varuint(_BLOCK_INFO_FIELD_IS_OVERFLOWS)
    writer.write_byte(1 if info.is_overflows else 0)
    writer.write_varuint(_BLOCK_INFO_FIELD_BUCKET_NUM)
    writer.write_int(info.bucket_num, 4, signed=True)
    writer.write_varuint(_BLOCK_INFO_FIELD_TERMINATOR)


# ---- Block -----------------------------------------------------------------


async def read_block(
    reader: AsyncBinaryReader,
    *,
    revision: int,
    session_timezone: str | None = None,
) -> Block:
    """Decode one block from the reader.

    ``revision`` is the connection's negotiated protocol revision —
    ``min(OUR_REVISION, server_revision)`` from the handshake — and
    governs which optional fields are on the wire.

    ``session_timezone`` (when set) becomes the fallback timezone for
    any bare ``DateTime`` / ``DateTime64(p)`` codec parsed from this
    block's column specs. The Connection plumbs it down from the
    ``TIMEZONE_UPDATE`` packet so naive datetimes land in the
    server's negotiated session zone instead of silently UTC.
    """

    info = await read_block_info(reader)
    n_columns = await reader.read_varuint()
    n_rows = await reader.read_varuint()
    columns: list[ColumnSpec] = []
    data: list[list[Any]] = []
    for _ in range(n_columns):
        name = await reader.read_string()
        type_spec = await reader.read_string()
        codec = parse_type(type_spec, session_timezone=session_timezone)
        if revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION:
            has_custom = await reader.read_byte()
            if has_custom != 0:
                raise ProtocolError(
                    f"custom serialization not supported: column {name!r} "
                    f"of type {type_spec!r} carries has_custom={has_custom}"
                )
        column_data = await codec.read(reader, n_rows)
        columns.append(ColumnSpec(name=name, type_spec=type_spec, codec=codec))
        data.append(column_data)
    return Block(info=info, columns=columns, n_rows=n_rows, data=data)


def write_block(writer: BinaryWriter, block: Block, *, revision: int) -> None:
    """Encode one block to the writer.

    Caller is responsible for ``len(block.data) == len(block.columns)``
    and each column's data list having ``block.n_rows`` entries.
    """

    if len(block.data) != len(block.columns):
        raise ValueError(
            f"block data has {len(block.data)} columns but "
            f"{len(block.columns)} column specs"
        )
    for spec, values in zip(block.columns, block.data, strict=True):
        if len(values) != block.n_rows:
            raise ValueError(
                f"column {spec.name!r} has {len(values)} values but "
                f"block.n_rows is {block.n_rows}"
            )

    write_block_info(writer, block.info)
    writer.write_varuint(len(block.columns))
    writer.write_varuint(block.n_rows)
    for spec, values in zip(block.columns, block.data, strict=True):
        writer.write_string(spec.name)
        writer.write_string(spec.type_spec)
        if revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION:
            writer.write_byte(0)  # has_custom = 0; v0 only emits standard codecs
        spec.codec.write(writer, values)


# ---- helpers ---------------------------------------------------------------


def make_column(
    name: str, type_spec: str, values: Sequence[Any]
) -> tuple[ColumnSpec, list[Any]]:
    """Build a (spec, values) pair from a type-spec string. Convenience
    for tests and for users assembling INSERT blocks by hand."""

    return ColumnSpec(
        name=name, type_spec=type_spec, codec=parse_type(type_spec)
    ), list(values)
