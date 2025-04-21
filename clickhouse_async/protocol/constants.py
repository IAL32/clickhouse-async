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
    PROFILE_EVENTS = 12


class CompressionMethod(IntEnum):
    """Compression methods."""

    NONE = -1
    LZ4 = 1
    ZSTD = 2


class CompressionState(IntEnum):
    """Compression state."""

    DISABLE = 0
    ENABLE = 1


class Protocol:
    """Protocol version constants."""

    # Minimum revisions for various features
    MIN_REVISION_WITH_CLIENT_INFO = 54032
    MIN_REVISION_WITH_SERVER_TIMEZONE = 54058
    MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
    MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372
    MIN_REVISION_WITH_VERSION_PATCH = 54401
    MIN_REVISION_WITH_SERVER_LOGS = 54406
    MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420
    MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429

    # Current revision
    DBMS_VERSION_MAJOR = 1
    DBMS_VERSION_MINOR = 0
    DBMS_VERSION_PATCH = 0
    DBMS_PROTOCOL_VERSION = 54429  # Current protocol version
    DBMS_NAME = "ClickHouse"
