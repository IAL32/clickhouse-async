"""ClickHouse protocol constants."""

from enum import IntEnum


class ClientCodes(IntEnum):
    """Client packet codes."""

    HELLO = 0
    QUERY = 1
    DATA = 2
    CANCEL = 3
    PING = 4


class ServerCodes(IntEnum):
    """Server packet codes."""

    HELLO = 0
    DATA = 1
    EXCEPTION = 2
    PROGRESS = 3
    PONG = 4
    END_OF_STREAM = 5
    PROFILE_INFO = 6
    TOTALS = 7
    EXTREMES = 8
    TABLES_STATUS = 9
    LOG = 10
    TABLE_COLUMNS = 11
    PART_UUIDS = 12
    READ_TASK_REQUEST = 13
    PROFILE_EVENTS = 14


class CompressionMethod(IntEnum):
    """Compression methods."""

    NONE = -1
    LZ4 = 1
    ZSTD = 2


class CompressionState(IntEnum):
    """Compression state."""

    DISABLE = 0
    ENABLE = 1


class Stages(IntEnum):
    """Query processing stages."""

    COMPLETE = 2


class ClickHouseProtocol:
    """Protocol version constants."""

    # Minimum revisions for various features
    MIN_REVISION_WITH_TEMPORARY_TABLES = 50264
    MIN_REVISION_WITH_BLOCK_INFO = 51903
    MIN_REVISION_WITH_CLIENT_INFO = 54032
    MIN_REVISION_WITH_SERVER_TIMEZONE = 54058
    MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
    MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372
    MIN_REVISION_WITH_VERSION_PATCH = 54401
    MIN_REVISION_WITH_SERVER_LOGS = 54406
    MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420
    MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429
    MIN_REVISION_WITH_INTERSERVER_SECRET = 54441
    MIN_REVISION_WITH_OPENTELEMETRY = 54442
    MIN_REVISION_WITH_DISTRIBUTED_DEPTH = 54448
    MIN_REVISION_WITH_INITIAL_QUERY_START_TIME = 54449
    MIN_REVISION_WITH_PARALLEL_REPLICAS = 54453
    MIN_REVISION_WITH_CUSTOM_SERIALIZATION = 54454
    MIN_PROTOCOL_VERSION_WITH_ADDENDUM = 54458
    MIN_PROTOCOL_VERSION_WITH_PARAMETERS = 54459

    # Current revision
    DBMS_VERSION_MAJOR = 24
    DBMS_VERSION_MINOR = 12
    DBMS_VERSION_PATCH = 0
    DBMS_PROTOCOL_VERSION = (
        MIN_PROTOCOL_VERSION_WITH_PARAMETERS  # Current protocol version
    )
    DBMS_NAME = "ClickHouse"
