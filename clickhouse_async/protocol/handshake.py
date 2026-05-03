"""Hello handshake — client and server packet codecs plus ``ServerInfo``.

Wire layout, in order:

**Client → server (varuint ``ClientPacket.HELLO``):**
- string ``client_name``
- varuint ``client_version_major``
- varuint ``client_version_minor``
- varuint ``client_revision`` = ``OUR_REVISION``
- string ``default_database``
- string ``user``
- string ``password``

**Server → client (varuint ``ServerPacket.HELLO``):**
- string ``server_name``
- varuint ``version_major``
- varuint ``version_minor``
- varuint ``revision``
- if ``revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE``: string ``timezone``
- if ``revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME``: string ``display_name``
- if ``revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH``: varuint ``version_patch``
- if ``revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES``:
  varuint ``n_rules`` followed by ``n_rules`` pairs of (string
  ``pattern``, string ``message``) — informational; v0 reads and
  discards them.
- if ``revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2``: 8-byte
  little-endian unsigned ``nonce`` — used for inter-server auth, which
  v0 doesn't drive; we read and discard.

Both ``read_server_hello`` and ``read_exception_body`` (in
``exception_packet``) operate on the body — the caller has already
consumed the leading packet id so it can dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass

import clickhouse_async
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES,
    DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2,
    DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
    DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    OUR_REVISION,
    ClientPacket,
)

CLIENT_NAME = "clickhouse-async"


def _split_version(v: str) -> tuple[int, int]:
    """Parse ``"0.1.0"`` → ``(0, 1)``. Patch is folded into the minor's
    submission; we send only major/minor at handshake."""
    parts = v.split(".")
    major = int(parts[0]) if len(parts) >= 1 and parts[0].isdigit() else 0
    minor = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 0
    return major, minor


CLIENT_VERSION_MAJOR, CLIENT_VERSION_MINOR = _split_version(
    clickhouse_async.__version__
)


@dataclass
class ServerInfo:
    """The server identity captured from its Hello reply."""

    name: str
    version_major: int
    version_minor: int
    revision: int
    timezone: str | None = None
    display_name: str | None = None
    version_patch: int = 0


def write_client_hello(
    writer: BinaryWriter,
    *,
    user: str,
    password: str,
    database: str,
) -> None:
    """Append the client Hello packet (id + body) to ``writer``."""
    writer.write_varuint(ClientPacket.HELLO)
    writer.write_string(CLIENT_NAME)
    writer.write_varuint(CLIENT_VERSION_MAJOR)
    writer.write_varuint(CLIENT_VERSION_MINOR)
    writer.write_varuint(OUR_REVISION)
    writer.write_string(database)
    writer.write_string(user)
    writer.write_string(password)


async def read_server_hello(reader: AsyncBinaryReader) -> ServerInfo:
    """Read the body of a server Hello packet (the packet id has already
    been consumed)."""
    name = await reader.read_string()
    version_major = await reader.read_varuint()
    version_minor = await reader.read_varuint()
    revision = await reader.read_varuint()

    timezone: str | None = None
    if revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
        timezone = await reader.read_string()

    display_name: str | None = None
    if revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME:
        display_name = await reader.read_string()

    version_patch = 0
    if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH:
        version_patch = await reader.read_varuint()

    # Password complexity rules — informational, the client doesn't use
    # them. Drained so the wire stays in sync.
    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES:
        n_rules = await reader.read_varuint()
        for _ in range(n_rules):
            await reader.read_string()  # pattern
            await reader.read_string()  # message

    # Interserver-secret-v2 nonce — used for inter-server auth. v0
    # doesn't drive that path; we read and discard.
    if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2:
        await reader.read_int(8, signed=False)

    return ServerInfo(
        name=name,
        version_major=version_major,
        version_minor=version_minor,
        revision=revision,
        timezone=timezone,
        display_name=display_name,
        version_patch=version_patch,
    )
