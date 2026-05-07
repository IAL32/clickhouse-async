"""Decode the body of a server `Exception` packet into a `ServerError`.

Shared between the handshake (server may reject Hello with an
Exception) and the steady-state packet loop (Exception is the
end-of-query failure path). The packet id (varuint
`ServerPacket.EXCEPTION`) is consumed by the caller; this function
reads only the body.

Wire format (per upstream `Common/Exception.cpp::writeException`):

- Int32 code (signed, little-endian)
- String name
- String display_text
- String stack_trace
- UInt8 has_nested (0 or 1)
- if has_nested: nested Exception body (recursive)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_async.errors import ServerError

if TYPE_CHECKING:
    from clickhouse_async.protocol.io import AsyncBinaryReader


async def read_exception_body(reader: AsyncBinaryReader) -> ServerError:
    code = await reader.read_int(4, signed=True)
    name = await reader.read_string()
    display_text = await reader.read_string()
    stack_trace = await reader.read_string()
    has_nested = await reader.read_byte()
    nested: ServerError | None = None
    if has_nested:
        nested = await read_exception_body(reader)
    return ServerError(
        code=code,
        name=name,
        display_text=display_text,
        stack_trace=stack_trace,
        nested=nested,
    )
