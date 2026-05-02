"""Decoders for the non-data server packets observed during a query.

The packet id (varuint ``ServerPacket.*``) has already been consumed by
the caller; each function reads only the packet body. Block-bearing
packets (Data, Totals, Extremes, Log, ProfileEvents) share the
``[external table name string][block]`` shape — see
``read_block_packet_body``.

Layouts pinned against upstream:

- **Progress** — ``IO/Progress.cpp::ProgressValues::write``
- **ProfileInfo** — ``QueryPipeline/ProfileInfo.cpp::ProfileInfo::write``
- **Log / ProfileEvents / Totals / Extremes / Data** — ``Server/TCPHandler.cpp``
- **TableColumns** — id + ``""`` + columns description string
- **TimezoneUpdate** — id + tz string
"""

from __future__ import annotations

from dataclasses import dataclass

from clickhouse_async.protocol.block import Block, read_block
from clickhouse_async.protocol.io import AsyncBinaryReader
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS,
    DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO,
    DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION,
)


@dataclass
class ProgressInfo:
    """Server-emitted progress increment.

    ClickHouse emits Progress packets multiple times during a query,
    each carrying *increments since the last Progress packet* (per
    upstream's ``fetchValuesAndResetPiecewiseAtomically``). Callers
    that want cumulative totals accumulate themselves.
    """

    read_rows: int
    read_bytes: int
    total_rows_to_read: int
    total_bytes_to_read: int = 0
    written_rows: int = 0
    written_bytes: int = 0
    elapsed_ns: int = 0


@dataclass
class ProfileInfo:
    """Server-emitted aggregate profile info, sent once near end-of-query."""

    rows: int
    blocks: int
    bytes: int
    applied_limit: bool
    rows_before_limit: int
    applied_aggregation: bool = False
    rows_before_aggregation: int = 0


# ---- Progress / ProfileInfo ----------------------------------------------


async def read_progress(
    reader: AsyncBinaryReader, *, revision: int
) -> ProgressInfo:
    read_rows = await reader.read_varuint()
    read_bytes = await reader.read_varuint()
    total_rows_to_read = await reader.read_varuint()

    total_bytes_to_read = 0
    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS:
        total_bytes_to_read = await reader.read_varuint()

    written_rows = 0
    written_bytes = 0
    if revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO:
        written_rows = await reader.read_varuint()
        written_bytes = await reader.read_varuint()

    elapsed_ns = 0
    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS:
        elapsed_ns = await reader.read_varuint()

    return ProgressInfo(
        read_rows=read_rows,
        read_bytes=read_bytes,
        total_rows_to_read=total_rows_to_read,
        total_bytes_to_read=total_bytes_to_read,
        written_rows=written_rows,
        written_bytes=written_bytes,
        elapsed_ns=elapsed_ns,
    )


async def read_profile_info(
    reader: AsyncBinaryReader, *, revision: int
) -> ProfileInfo:
    rows = await reader.read_varuint()
    blocks = await reader.read_varuint()
    bytes_ = await reader.read_varuint()
    applied_limit = (await reader.read_byte()) != 0
    rows_before_limit = await reader.read_varuint()
    # `unused_obsolete_field` (UInt8) — historically `calculated_rows_before_limit`,
    # now consumed and discarded by upstream.
    await reader.read_byte()

    applied_aggregation = False
    rows_before_aggregation = 0
    if revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION:
        applied_aggregation = (await reader.read_byte()) != 0
        rows_before_aggregation = await reader.read_varuint()

    return ProfileInfo(
        rows=rows,
        blocks=blocks,
        bytes=bytes_,
        applied_limit=applied_limit,
        rows_before_limit=rows_before_limit,
        applied_aggregation=applied_aggregation,
        rows_before_aggregation=rows_before_aggregation,
    )


# ---- block-bearing packets ---------------------------------------------


async def read_block_packet_body(
    reader: AsyncBinaryReader, *, revision: int
) -> tuple[str, Block]:
    """Read the body shared by Data / Totals / Extremes / Log / ProfileEvents.

    Layout: ``string external_table_name`` (often empty) + ``Block``.
    """

    table_name = await reader.read_string()
    block = await read_block(reader, revision=revision)
    return table_name, block


# ---- TableColumns / TimezoneUpdate -------------------------------------


async def read_table_columns(
    reader: AsyncBinaryReader,
) -> tuple[str, str]:
    """``TableColumns`` body: ``string default_table_name`` (typically "")
    + ``string columns_description`` (the full ``CREATE TABLE`` columns
    DDL fragment)."""

    default_table_name = await reader.read_string()
    columns_description = await reader.read_string()
    return default_table_name, columns_description


async def read_timezone_update(
    reader: AsyncBinaryReader,
) -> str:
    """``TimezoneUpdate`` body: a single string carrying the server's
    session timezone."""

    return await reader.read_string()
