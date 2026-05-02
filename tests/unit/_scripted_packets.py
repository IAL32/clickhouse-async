"""Helpers for encoding server-to-client packets in unit tests.

Each helper builds the byte sequence the server would emit (including
the leading varuint packet id), so tests can ``transport.feed(…)`` it
and drive ``Connection`` against a known scenario. The helpers are
shared across every substep that needs scripted server behaviour.
"""

from __future__ import annotations

from clickhouse_async.protocol.block import Block, write_block
from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS,
    DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO,
    DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION,
    DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
    DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    OUR_REVISION,
    ServerPacket,
)


def encode_server_hello(
    *,
    name: str = "ClickHouse",
    version_major: int = 24,
    version_minor: int = 8,
    revision: int = OUR_REVISION,
    timezone: str | None = "UTC",
    display_name: str | None = "test-server",
    version_patch: int = 1,
) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.HELLO``.

    Optional fields ``timezone``, ``display_name``, and ``version_patch``
    are emitted only at revisions where the protocol gates them on; if
    a caller passes ``timezone="UTC"`` against a sub-54058 revision the
    field is silently omitted (the server wouldn't have written it).
    """

    w = BinaryWriter()
    w.write_varuint(ServerPacket.HELLO)
    w.write_string(name)
    w.write_varuint(version_major)
    w.write_varuint(version_minor)
    w.write_varuint(revision)
    if revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE and timezone is not None:
        w.write_string(timezone)
    if (
        revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
        and display_name is not None
    ):
        w.write_string(display_name)
    if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH:
        w.write_varuint(version_patch)
    return w.getvalue()


def encode_server_exception(
    *,
    code: int = 1,
    name: str = "TEST_ERROR",
    display_text: str = "test error",
    stack_trace: str = "",
    nested: bytes | None = None,
) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.EXCEPTION``.

    To test nested exceptions, pass another encoded body via ``nested``;
    the helper sets ``has_nested = 1`` and appends it.
    """

    w = BinaryWriter()
    w.write_varuint(ServerPacket.EXCEPTION)
    _append_exception_body(
        w,
        code=code,
        name=name,
        display_text=display_text,
        stack_trace=stack_trace,
        nested=nested,
    )
    return w.getvalue()


def encode_exception_body_only(
    *,
    code: int = 1,
    name: str = "TEST_ERROR",
    display_text: str = "test error",
    stack_trace: str = "",
    nested: bytes | None = None,
) -> bytes:
    """The Exception body without the leading packet id — used as the
    ``nested`` argument for stacking errors."""

    w = BinaryWriter()
    _append_exception_body(
        w,
        code=code,
        name=name,
        display_text=display_text,
        stack_trace=stack_trace,
        nested=nested,
    )
    return w.getvalue()


def _append_exception_body(
    w: BinaryWriter,
    *,
    code: int,
    name: str,
    display_text: str,
    stack_trace: str,
    nested: bytes | None,
) -> None:
    w.write_int(code, 4, signed=True)
    w.write_string(name)
    w.write_string(display_text)
    w.write_string(stack_trace)
    w.write_byte(1 if nested is not None else 0)
    if nested is not None:
        w.write_raw(nested)


def encode_server_data(
    block: Block, *, revision: int = OUR_REVISION, table_name: str = ""
) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.DATA`` —
    packet id, external-table name (empty for the main result), and the
    block at the given revision."""

    w = BinaryWriter()
    w.write_varuint(ServerPacket.DATA)
    w.write_string(table_name)
    write_block(w, block, revision=revision)
    return w.getvalue()


def encode_server_end_of_stream() -> bytes:
    """Build the bytes a server would emit for
    ``ServerPacket.END_OF_STREAM``."""

    w = BinaryWriter()
    w.write_varuint(ServerPacket.END_OF_STREAM)
    return w.getvalue()


def encode_server_progress(
    *,
    revision: int = OUR_REVISION,
    read_rows: int = 0,
    read_bytes: int = 0,
    total_rows_to_read: int = 0,
    total_bytes_to_read: int = 0,
    written_rows: int = 0,
    written_bytes: int = 0,
    elapsed_ns: int = 0,
) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.PROGRESS``
    at the given revision (gates determine which trailing fields are
    emitted)."""

    w = BinaryWriter()
    w.write_varuint(ServerPacket.PROGRESS)
    w.write_varuint(read_rows)
    w.write_varuint(read_bytes)
    w.write_varuint(total_rows_to_read)
    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS:
        w.write_varuint(total_bytes_to_read)
    if revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO:
        w.write_varuint(written_rows)
        w.write_varuint(written_bytes)
    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS:
        w.write_varuint(elapsed_ns)
    return w.getvalue()


def encode_server_profile_info(
    *,
    revision: int = OUR_REVISION,
    rows: int = 0,
    blocks: int = 0,
    bytes_: int = 0,
    applied_limit: bool = False,
    rows_before_limit: int = 0,
    applied_aggregation: bool = False,
    rows_before_aggregation: int = 0,
) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.PROFILE_INFO``."""

    w = BinaryWriter()
    w.write_varuint(ServerPacket.PROFILE_INFO)
    w.write_varuint(rows)
    w.write_varuint(blocks)
    w.write_varuint(bytes_)
    w.write_byte(1 if applied_limit else 0)
    w.write_varuint(rows_before_limit)
    # `unused_obsolete_field` (UInt8 bool) — upstream still emits it
    w.write_byte(0)
    if revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION:
        w.write_byte(1 if applied_aggregation else 0)
        w.write_varuint(rows_before_aggregation)
    return w.getvalue()


def encode_server_block_packet(
    packet_id: ServerPacket,
    block: Block,
    *,
    revision: int = OUR_REVISION,
    table_name: str = "",
) -> bytes:
    """Build a server packet whose body is ``[string][block]`` (Totals,
    Extremes, Log, ProfileEvents)."""

    w = BinaryWriter()
    w.write_varuint(packet_id)
    w.write_string(table_name)
    write_block(w, block, revision=revision)
    return w.getvalue()


def encode_server_table_columns(
    *, default_table_name: str = "", columns: str = ""
) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.TABLE_COLUMNS``."""

    w = BinaryWriter()
    w.write_varuint(ServerPacket.TABLE_COLUMNS)
    w.write_string(default_table_name)
    w.write_string(columns)
    return w.getvalue()


def encode_server_timezone_update(tz: str) -> bytes:
    """Build the bytes a server would emit for ``ServerPacket.TIMEZONE_UPDATE``."""

    w = BinaryWriter()
    w.write_varuint(ServerPacket.TIMEZONE_UPDATE)
    w.write_string(tz)
    return w.getvalue()


__all__ = [
    "encode_exception_body_only",
    "encode_server_block_packet",
    "encode_server_data",
    "encode_server_end_of_stream",
    "encode_server_exception",
    "encode_server_hello",
    "encode_server_profile_info",
    "encode_server_progress",
    "encode_server_table_columns",
    "encode_server_timezone_update",
]
