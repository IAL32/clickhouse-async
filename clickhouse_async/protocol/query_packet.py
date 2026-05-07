"""Query packet writer and the `ClientInfo` block it embeds.

Wire layout (from upstream `Client/Connection.cpp::sendQuery` and
mirrored in `.plans/06-connection.md`):

1. varuint `ClientPacket.QUERY`
2. string `query_id`
3. (revision ≥ `DBMS_MIN_REVISION_WITH_CLIENT_INFO`) `ClientInfo` block
4. settings — `[name, flags-varuint, value]*` then empty-name terminator
5. (revision ≥ `DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET`) interserver
   secret string — empty for `InitialQuery` (which we always are in v0)
6. varuint query stage = `Complete` (2)
7. varuint compression flag (0 or 1)
8. string SQL
9. (revision ≥ `DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS`) parameters —
   same shape as settings, with terminator
10. trailing empty Data packet — `Client.Data` + empty external-table
    name + empty block

`write_query_packet` accepts settings, parameters, and a compression
flag; each is gated on the matching protocol revision and emitted in
the documented order.
"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

from clickhouse_async.protocol._client_env import _safe_hostname, _safe_os_user
from clickhouse_async.protocol.block import Block, BlockInfo
from clickhouse_async.protocol.compression import CompressionMethod, write_block_framed
from clickhouse_async.protocol.handshake import (
    CLIENT_NAME,
    CLIENT_VERSION_MAJOR,
    CLIENT_VERSION_MINOR,
)
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH,
    DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME,
    DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS,
    DBMS_MIN_REVISION_WITH_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
    DBMS_MIN_REVISION_WITH_OPENTELEMETRY,
    DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS,
    DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS,
    DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER,
    OUR_REVISION,
    ClientPacket,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from clickhouse_async.protocol.io import BinaryWriter


class QueryStage(IntEnum):
    """Stages a query is processed up to. v0 always sends `COMPLETE`."""

    FETCH_COLUMNS = 0
    WITH_MERGEABLE_STATE = 1
    COMPLETE = 2
    WITH_MERGEABLE_STATE_AFTER_AGGREGATION = 3
    WITH_MERGEABLE_STATE_AFTER_AGGREGATION_AND_LIMIT = 4


class _Interface(IntEnum):
    TCP = 1


class _QueryKind(IntEnum):
    NO_QUERY = 0
    INITIAL_QUERY = 1
    SECONDARY_QUERY = 2


# BaseSettings flag bits (per upstream Core/BaseSettings.h::Flags). Settings
# we send are flagged IMPORTANT so the server doesn't silently drop
# names it doesn't recognise; query parameters live in the same wire
# format but use CUSTOM since they're user-defined names rather than
# built-in settings.
_SETTING_FLAG_IMPORTANT = 0x01
_PARAM_FLAG_CUSTOM = 0x02


def _write_client_info(
    writer: BinaryWriter,
    *,
    revision: int,
    query_id: str,
    user: str,
) -> None:
    """Append the `ClientInfo` block (caller has already gated on
    `revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO`)."""

    writer.write_byte(_QueryKind.INITIAL_QUERY)
    # initial_user / initial_query_id / initial_address
    writer.write_string(user)
    writer.write_string(query_id)
    # initial_address: ClickHouse parses this through Poco's SocketAddress
    # which asserts non-empty input. We don't have a public IP to report
    # for an initial query, so we send a documented sentinel that parses
    # as "no address" without tripping the assertion.
    writer.write_string("0.0.0.0:0")

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME:
        # Int64 microseconds since the Unix epoch — 0 means unset
        writer.write_int(0, 8, signed=True)

    writer.write_byte(_Interface.TCP)

    # TCP-specific block
    writer.write_string(_safe_os_user())
    writer.write_string(_safe_hostname())
    writer.write_string(CLIENT_NAME)
    writer.write_varuint(CLIENT_VERSION_MAJOR)
    writer.write_varuint(CLIENT_VERSION_MINOR)
    writer.write_varuint(OUR_REVISION)

    if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO:
        writer.write_string("")  # quota_key

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH:
        writer.write_varuint(0)  # distributed_depth

    if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH:
        writer.write_varuint(0)  # client_version_patch

    if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY:
        writer.write_byte(0)  # has_otel = 0

    if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS:
        writer.write_varuint(0)  # collaborate_with_initiator
        writer.write_varuint(0)  # obsolete_count_participating_replicas
        writer.write_varuint(0)  # number_of_current_replica

    if revision >= DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS:
        writer.write_varuint(0)  # script_query_number
        writer.write_varuint(0)  # script_line_number

    if revision >= DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER:
        writer.write_byte(0)  # have_jwt = 0 (no JWT token)


def write_query_packet(
    writer: BinaryWriter,
    *,
    sql: str,
    query_id: str,
    user: str,
    revision: int,
    settings: Mapping[str, str] | None = None,
    parameters: Mapping[str, str] | None = None,
    compression: CompressionMethod = CompressionMethod.NONE,
) -> None:
    """Append a complete Query packet plus the trailing empty data block.

    Settings and parameters values are strings on the wire — type
    coercion happens at the call site. The trailing empty Data block is
    framed with the same `compression` method as the rest of the
    connection — the server expects all client-to-server blocks to be
    compressed whenever compression is negotiated.
    """

    writer.write_varuint(ClientPacket.QUERY)
    writer.write_string(query_id)

    if revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO:
        _write_client_info(writer, revision=revision, query_id=query_id, user=user)

    if settings:
        for name, value in settings.items():
            writer.write_string(name)
            writer.write_varuint(_SETTING_FLAG_IMPORTANT)
            writer.write_string(value)
    writer.write_string("")  # terminator

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES:
        writer.write_string("")  # extra_roles — empty for non-interserver queries

    if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET:
        # Empty hash for INITIAL_QUERY — which is the only kind v0 issues.
        writer.write_string("")

    writer.write_varuint(QueryStage.COMPLETE)
    writer.write_varuint(0 if compression == CompressionMethod.NONE else 1)
    writer.write_string(sql)

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS:
        if parameters:
            for name, value in parameters.items():
                writer.write_string(name)
                writer.write_varuint(_PARAM_FLAG_CUSTOM)
                writer.write_string(value)
        writer.write_string("")  # terminator

    # Trailing empty Data packet — signals "no inline data" for SELECTs;
    # for INSERTs the caller follows it with real Data packets.
    # Must be framed with the connection's compression when compression is
    # on — the server expects all client-to-server blocks to be compressed.
    writer.write_varuint(ClientPacket.DATA)
    writer.write_string("")  # external table name (empty = main table)
    write_block_framed(
        writer,
        Block(info=BlockInfo(), columns=[], n_rows=0, data=[]),
        revision=revision,
        compression=compression,
    )
