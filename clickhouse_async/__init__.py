"""Async Python client for ClickHouse."""

# Set the version *before* the re-exports below: ``protocol/handshake.py``
# reads ``clickhouse_async.__version__`` at import time to populate the
# Hello packet's client_version_{major,minor}. Re-ordering here would
# trigger a circular import that fails attribute lookup.
__version__ = "0.3.0"

from clickhouse_async.client import (
    Client,
    ColumnarBlock,
    ColumnarResult,
    QueryResult,
    connect,
)
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.errors import (
    ClickHouseError,
    ConcurrentQueryError,
    ConnectError,
    MissingExtraError,
    PoolClosedError,
    PoolError,
    PoolTimeoutError,
    ProtocolError,
    QueryCancellationError,
    ServerError,
    UnsupportedFeatureError,
)
from clickhouse_async.pool import Pool, create_pool
from clickhouse_async.protocol.compression import CompressionMethod

__all__ = [
    "DSN",
    "ClickHouseError",
    "Client",
    "ColumnarBlock",
    "ColumnarResult",
    "CompressionMethod",
    "ConcurrentQueryError",
    "ConnectError",
    "MissingExtraError",
    "Pool",
    "PoolClosedError",
    "PoolError",
    "PoolTimeoutError",
    "ProtocolError",
    "QueryCancellationError",
    "QueryResult",
    "ServerError",
    "UnsupportedFeatureError",
    "__version__",
    "connect",
    "create_pool",
    "parse_dsn",
]
