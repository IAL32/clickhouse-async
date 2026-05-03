"""Async Python client for ClickHouse."""

# Set the version *before* the re-exports below: ``protocol/handshake.py``
# reads ``clickhouse_async.__version__`` at import time to populate the
# Hello packet's client_version_{major,minor}. Re-ordering here would
# trigger a circular import that fails attribute lookup.
__version__ = "0.1.0"

from clickhouse_async.client import Client, QueryResult, connect
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.errors import (
    ClickHouseError,
    ConcurrentQueryError,
    MissingExtraError,
    ProtocolError,
    QueryCancellationError,
    ServerError,
    UnsupportedFeatureError,
)
from clickhouse_async.protocol.compression import CompressionMethod

__all__ = [
    "DSN",
    "ClickHouseError",
    "Client",
    "CompressionMethod",
    "ConcurrentQueryError",
    "MissingExtraError",
    "ProtocolError",
    "QueryCancellationError",
    "QueryResult",
    "ServerError",
    "UnsupportedFeatureError",
    "__version__",
    "connect",
    "parse_dsn",
]
