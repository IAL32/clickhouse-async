"""Parse a ClickHouse connection-string DSN.

Format: ``clickhouse[s]://[user[:password]]@host[:port][/database][?k=v&...]``

Examples:

- ``clickhouse://localhost``
- ``clickhouse://default:@localhost:9000/default``
- ``clickhouse://alice:secret@db.example:9000/analytics?compression=lz4``
- ``clickhouses://alice@db.example/`` (TLS, default port 9440)
- ``clickhouse://user@[::1]:9000/db`` (IPv6 host literal)

Recognised query parameters:

- ``secure=true|false`` — overrides the scheme. ``clickhouses://`` implies
  ``secure=true``.
- ``compression=none|lz4|zstd``
- ``connect_timeout=<float seconds>``

Anything else in the query string lands in ``settings`` as a ``str → str``
map and is forwarded to the server alongside each query.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote, urlparse

from clickhouse_async.protocol.compression import CompressionMethod

DEFAULT_PORT = 9000
DEFAULT_SECURE_PORT = 9440
DEFAULT_USER = "default"
DEFAULT_DATABASE = "default"
DEFAULT_CONNECT_TIMEOUT = 10.0


@dataclass(frozen=True)
class DSN:
    """Parsed DSN. ``parse_dsn`` is the only documented constructor."""

    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool = False
    compression: CompressionMethod = CompressionMethod.NONE
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    settings: dict[str, str] = field(default_factory=dict)


def parse_dsn(dsn: str) -> DSN:
    """Parse a ``clickhouse://`` DSN string into a ``DSN`` dataclass.

    Raises ``ValueError`` for unsupported schemes, missing host, or
    malformed query-parameter values (bool / float / compression).
    """
    parsed = urlparse(dsn)
    if parsed.scheme not in ("clickhouse", "clickhouses"):
        raise ValueError(
            f"unsupported DSN scheme {parsed.scheme!r} "
            f"(expected 'clickhouse' or 'clickhouses')"
        )
    if not parsed.hostname:
        raise ValueError(f"DSN missing host: {dsn!r}")

    user = unquote(parsed.username) if parsed.username else DEFAULT_USER
    password = unquote(parsed.password) if parsed.password else ""
    host = parsed.hostname

    path = parsed.path.lstrip("/")
    database = unquote(path) if path else DEFAULT_DATABASE

    query = parse_qs(parsed.query, keep_blank_values=True)

    secure = parsed.scheme == "clickhouses" or _parse_bool(
        _take_one(query, "secure", "false")
    )

    port = parsed.port
    if port is None:
        port = DEFAULT_SECURE_PORT if secure else DEFAULT_PORT

    compression = _parse_compression(_take_one(query, "compression", "none"))

    connect_timeout_str = _take_one(
        query, "connect_timeout", str(DEFAULT_CONNECT_TIMEOUT)
    )
    try:
        connect_timeout = float(connect_timeout_str)
    except ValueError as exc:
        raise ValueError(
            f"invalid connect_timeout {connect_timeout_str!r}: {exc}"
        ) from exc
    if connect_timeout <= 0:
        raise ValueError(
            f"connect_timeout must be positive, got {connect_timeout}"
        )

    consumed = {"secure", "compression", "connect_timeout"}
    settings = {k: v[-1] for k, v in query.items() if k not in consumed}

    return DSN(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        secure=secure,
        compression=compression,
        connect_timeout=connect_timeout,
        settings=settings,
    )


# ---- helpers --------------------------------------------------------------


def _take_one(query: dict[str, list[str]], key: str, default: str) -> str:
    """Take the last value for ``key`` (a multi-valued query is ambiguous;
    last-value matches what most parsers do). Returns ``default`` if the
    key is missing."""
    values = query.get(key)
    if not values:
        return default
    return values[-1]


def _parse_bool(s: str) -> bool:
    lowered = s.strip().lower()
    if lowered in ("true", "1", "yes", "on"):
        return True
    if lowered in ("false", "0", "no", "off", ""):
        return False
    raise ValueError(f"invalid bool value {s!r}")


def _parse_compression(s: str) -> CompressionMethod:
    lowered = s.strip().lower()
    if lowered in ("none", "off", "false", ""):
        return CompressionMethod.NONE
    if lowered == "lz4":
        return CompressionMethod.LZ4
    if lowered == "zstd":
        return CompressionMethod.ZSTD
    raise ValueError(
        f"unsupported compression {s!r} (expected 'none', 'lz4', or 'zstd')"
    )
