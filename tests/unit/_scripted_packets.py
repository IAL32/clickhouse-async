"""Helpers for encoding server-to-client packets in unit tests.

Each helper builds the byte sequence the server would emit (including
the leading varuint packet id), so tests can ``transport.feed(…)`` it
and drive ``Connection`` against a known scenario. The helpers are
shared across every substep that needs scripted server behaviour.
"""

from __future__ import annotations

from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.packets import (
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


__all__ = [
    "encode_exception_body_only",
    "encode_server_exception",
    "encode_server_hello",
]
