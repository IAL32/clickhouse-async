"""ClickHouse native-protocol packet codes and revision gates.

Values mirror upstream byte-for-byte:

- `src/Core/Protocol.h`         — `ClientPacket` and `ServerPacket`.
- `src/Core/ProtocolDefines.h`  — revision gates and `OUR_REVISION`.

`OUR_REVISION` is what this client claims in `Hello`. It's pinned to a
specific upstream `DBMS_TCP_PROTOCOL_VERSION` (see `.clickhouse-version`
for the matching server image); the negotiated revision against any modern
server is exactly what we implement, and newer servers downshift at
handshake. Bumping `OUR_REVISION` must come paired with implementing the
fields the higher revision introduces — lifting this number alone produces
subtly broken handshakes.

Every constant declared below is enforced to be `≤ OUR_REVISION` by
`tests/unit/test_protocol_packets.py`. Add a constant only when this
client gates a behaviour on it.

Values cross-checked against ClickHouse 26.5.1 source (`ProtocolDefines.h`
and `Protocol.h`). Backward-compat with 24.8 LTS is maintained through
revision-gated logic — older servers negotiate down to their own revision.
"""

from __future__ import annotations

from enum import IntEnum


class ClientPacket(IntEnum):
    """Packet types this client sends to the server.

    Values are LEB128-encoded on the wire as the first varuint of every
    client→server packet. Names mirror upstream `Protocol::Client::Enum`.
    """

    HELLO = 0
    QUERY = 1
    DATA = 2
    CANCEL = 3
    PING = 4
    TABLES_STATUS_REQUEST = 5
    KEEP_ALIVE = 6
    SCALAR = 7
    IGNORED_PART_UUIDS = 8
    READ_TASK_RESPONSE = 9
    MERGE_TREE_READ_TASK_RESPONSE = 10
    SSH_CHALLENGE_REQUEST = 11
    SSH_CHALLENGE_RESPONSE = 12


class ServerPacket(IntEnum):
    """Packet types the server sends to this client.

    Values are LEB128-encoded on the wire as the first varuint of every
    server→client packet. Names mirror upstream `Protocol::Server::Enum`.
    """

    HELLO = 0
    DATA = 1
    EXCEPTION = 2
    PROGRESS = 3
    PONG = 4
    END_OF_STREAM = 5
    PROFILE_INFO = 6
    TOTALS = 7
    EXTREMES = 8
    TABLES_STATUS_RESPONSE = 9
    LOG = 10
    TABLE_COLUMNS = 11
    PART_UUIDS = 12
    READ_TASK_REQUEST = 13
    PROFILE_EVENTS = 14
    MERGE_TREE_ALL_RANGES_ANNOUNCEMENT = 15
    MERGE_TREE_READ_TASK_REQUEST = 16
    TIMEZONE_UPDATE = 17
    SSH_CHALLENGE = 18


# ---- Revision gates --------------------------------------------------------
#
# Names match upstream ProtocolDefines.h exactly. Add an entry here only
# when this client gates behaviour on it.

DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032
DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058
DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372
DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401
DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420
DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET = 54441
DBMS_MIN_REVISION_WITH_OPENTELEMETRY = 54442
DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH = 54448
DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME = 54449
DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS = 54451
DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS = 54453
DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION = 54454
DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY = 54458
DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM = 54458
DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS = 54459
DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS = 54460
DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES = 54461
DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 = 54462
DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS = 54463
DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES = 54464
DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION = 54469
DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS = 54470
DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL = 54471
DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES = 54472
DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION = 54473
DBMS_MIN_REVISION_WITH_SERVER_SETTINGS = 54474
DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS = 54475
DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER = 54476
DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION = 54477
DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL = 54479
DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION = 54480
DBMS_MIN_REVISION_WITH_COMPRESSED_LOGS_PROFILE_EVENTS_COLUMNS = 54481
DBMS_MIN_REVISION_WITH_NULLABLE_SPARSE_SERIALIZATION = 54483

# The protocol revision this client claims in Hello. Pinned to the
# upstream DBMS_TCP_PROTOCOL_VERSION matching the server image declared
# in .clickhouse-version; bumping is a paired edit on both files.
OUR_REVISION = 54483
