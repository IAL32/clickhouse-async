"""Parse a ClickHouse connection-string DSN.

Format: ``clickhouse[s]://[user[:password]]@host[:port][,host2[:port2]...][/database][?k=v&...]``

Multiple hosts are comma-separated; bare hosts inherit the scheme's
default port (``9000`` plain / ``9440`` secure). The first connection
attempt walks the list in order; ``Pool`` rotates the start position
across acquires so a single dead replica doesn't dominate.

Examples:

- ``clickhouse://localhost``
- ``clickhouse://default:@localhost:9000/default``
- ``clickhouse://alice:secret@db.example:9000/analytics?compression=lz4``
- ``clickhouses://alice@db.example/`` (TLS, default port 9440)
- ``clickhouse://user@[::1]:9000/db`` (IPv6 host literal)
- ``clickhouse://user@h1:9000,h2:9000,h3/db`` (failover list)

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
    """Parsed DSN. ``parse_dsn`` is the only documented constructor.

    ``hosts`` is the canonical multi-host candidate list. ``host`` /
    ``port`` are read-only shortcuts pointing at the first entry — the
    one a single-host caller would expect.
    """

    hosts: tuple[tuple[str, int], ...]
    user: str
    password: str
    database: str
    secure: bool = False
    compression: CompressionMethod = CompressionMethod.NONE
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    settings: dict[str, str] = field(default_factory=dict)

    @property
    def host(self) -> str:
        """First host in the candidate list."""
        return self.hosts[0][0]

    @property
    def port(self) -> int:
        """Port of the first host in the candidate list."""
        return self.hosts[0][1]


def parse_dsn(dsn: str) -> DSN:
    """Parse a ``clickhouse://`` DSN string into a ``DSN`` dataclass.

    Raises ``ValueError`` for unsupported schemes, missing host, malformed
    host:port pieces, or malformed query-parameter values.
    """
    parsed = urlparse(dsn)
    if parsed.scheme not in ("clickhouse", "clickhouses"):
        raise ValueError(
            f"unsupported DSN scheme {parsed.scheme!r} "
            f"(expected 'clickhouse' or 'clickhouses')"
        )

    query = parse_qs(parsed.query, keep_blank_values=True)

    secure = parsed.scheme == "clickhouses" or _parse_bool(
        _take_one(query, "secure", "false")
    )
    default_port = DEFAULT_SECURE_PORT if secure else DEFAULT_PORT

    # urlparse's `.hostname` / `.port` choke on multi-host netlocs (it
    # splits on the first colon and trips on the comma). We re-derive
    # the host portion by hand: strip the userinfo prefix off the
    # netloc, then split the remainder on top-level commas.
    netloc = parsed.netloc
    host_part = netloc.rsplit("@", 1)[-1] if "@" in netloc else netloc
    if not host_part:
        raise ValueError(f"DSN missing host: {dsn!r}")
    pieces = _split_host_pieces(host_part)
    hosts: list[tuple[str, int]] = []
    for piece in pieces:
        if not piece.strip():
            raise ValueError(f"DSN has an empty host entry: {dsn!r}")
        hosts.append(_parse_host_piece(piece, default_port=default_port))
    if not hosts:
        raise ValueError(f"DSN missing host: {dsn!r}")

    user = unquote(parsed.username) if parsed.username else DEFAULT_USER
    password = unquote(parsed.password) if parsed.password else ""

    path = parsed.path.lstrip("/")
    database = unquote(path) if path else DEFAULT_DATABASE

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
        raise ValueError(f"connect_timeout must be positive, got {connect_timeout}")

    consumed = {"secure", "compression", "connect_timeout"}
    settings = {k: v[-1] for k, v in query.items() if k not in consumed}

    return DSN(
        hosts=tuple(hosts),
        user=user,
        password=password,
        database=database,
        secure=secure,
        compression=compression,
        connect_timeout=connect_timeout,
        settings=settings,
    )


# ---- helpers --------------------------------------------------------------


def _split_host_pieces(s: str) -> list[str]:
    """Split a multi-host string on top-level commas, respecting ``[...]``
    brackets so an IPv6 literal's internal colons / commas don't get
    confused for a separator."""
    pieces: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in s:
        if ch == "[":
            depth += 1
            current.append(ch)
        elif ch == "]":
            if depth == 0:
                raise ValueError(f"unbalanced ']' in DSN host list {s!r}")
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            pieces.append("".join(current))
            current = []
        else:
            current.append(ch)
    if depth != 0:
        raise ValueError(f"unbalanced '[' in DSN host list {s!r}")
    pieces.append("".join(current))
    return pieces


def _parse_host_piece(piece: str, *, default_port: int) -> tuple[str, int]:
    """Parse a single ``host[:port]`` (or ``[ipv6][:port]``) into
    ``(host, port)`` with the scheme's default port if no port is given."""
    piece = piece.strip()
    if piece.startswith("["):
        end = piece.find("]")
        if end == -1:
            raise ValueError(f"unterminated IPv6 host literal in DSN: {piece!r}")
        host = piece[1:end]
        rest = piece[end + 1 :]
        if not rest:
            port = default_port
        elif rest.startswith(":"):
            port = _parse_port(rest[1:], piece)
        else:
            raise ValueError(f"unexpected text after IPv6 host literal: {piece!r}")
    elif ":" in piece:
        host, port_str = piece.rsplit(":", 1)
        port = _parse_port(port_str, piece)
    else:
        host = piece
        port = default_port
    host = unquote(host)
    if not host:
        raise ValueError(f"DSN host piece is empty: {piece!r}")
    return host, port


def _parse_port(s: str, piece: str) -> int:
    try:
        port = int(s)
    except ValueError as exc:
        raise ValueError(f"invalid port {s!r} in DSN host {piece!r}") from exc
    if not 0 < port < 65536:
        raise ValueError(f"port out of range in DSN host {piece!r}: {port}")
    return port


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
