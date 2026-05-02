"""ClickHouse native-protocol packet codes and revision gates.

Values mirror upstream byte-for-byte:

- ``src/Core/Protocol.h``         — ``ClientPacket`` and ``ServerPacket``.
- ``src/Core/ProtocolDefines.h``  — revision gates and ``OUR_REVISION``.

``OUR_REVISION`` is what this client claims in ``Hello``. We pin it to the
ClickHouse 24.8 LTS ``DBMS_TCP_PROTOCOL_VERSION`` (54469) so the negotiated
revision against any modern server is exactly what we implement; newer
servers downshift to ours at handshake. Bumping ``OUR_REVISION`` must come
paired with implementing the fields the higher revision introduces — lifting
this number alone produces subtly broken handshakes.

Every constant declared below is enforced to be ``≤ OUR_REVISION`` by
``tests/unit/test_protocol_packets.py``. Add a constant only when this
client gates a behaviour on it.

Cross-checked against the local ClickHouse master clone at SHA
``564e37f5088e3a27ba9f866655898ed4e2845df1``; the values for every gate
listed here are identical on the 24.8 branch.
"""

from __future__ import annotations

from enum import IntEnum


class ClientPacket(IntEnum):
    """Packet types this client sends to the server.

    Values are LEB128-encoded on the wire as the first varuint of every
    client→server packet. Names mirror upstream ``Protocol::Client::Enum``.
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
    server→client packet. Names mirror upstream ``Protocol::Server::Enum``.
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
DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401
DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET = 54441
DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH = 54448
DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS = 54451
DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS = 54459
DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES = 54464

# The protocol revision this client claims in Hello. Pinned to ClickHouse
# 24.8 LTS DBMS_TCP_PROTOCOL_VERSION. Keep in sync with .clickhouse-version.
OUR_REVISION = 54469
