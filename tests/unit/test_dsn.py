"""Tests for ``parse_dsn`` — every documented edge case in the DSN
grammar."""

from __future__ import annotations

import pytest

from clickhouse_async.dsn import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_DATABASE,
    DEFAULT_PORT,
    DEFAULT_SECURE_PORT,
    DEFAULT_USER,
    parse_dsn,
)
from clickhouse_async.protocol.compression import CompressionMethod

# ---- minimal DSN forms --------------------------------------------------


def test_minimal_dsn_uses_documented_defaults() -> None:
    # BEGIN: the smallest valid DSN — host only
    dsn = parse_dsn("clickhouse://localhost")

    # WHEN / THEN: every other field gets its documented default
    assert dsn.host == "localhost"
    assert dsn.port == DEFAULT_PORT
    assert dsn.user == DEFAULT_USER
    assert dsn.password == ""
    assert dsn.database == DEFAULT_DATABASE
    assert dsn.secure is False
    assert dsn.compression == CompressionMethod.NONE
    assert dsn.connect_timeout == DEFAULT_CONNECT_TIMEOUT
    assert dsn.settings == {}


def test_user_and_password_extracted_from_userinfo() -> None:
    # BEGIN / WHEN: parsing a DSN with credentials
    dsn = parse_dsn("clickhouse://alice:secret@host:9000/db")

    # THEN: user and password come through as-is
    assert dsn.user == "alice"
    assert dsn.password == "secret"
    assert dsn.host == "host"
    assert dsn.port == 9000
    assert dsn.database == "db"


def test_user_only_no_password() -> None:
    # BEGIN / WHEN: a DSN with user but no password
    dsn = parse_dsn("clickhouse://alice@host")

    # THEN: password is empty string, not None
    assert dsn.user == "alice"
    assert dsn.password == ""


def test_canonical_localhost_dsn_round_trips() -> None:
    # BEGIN / WHEN: the canonical default DSN from CLAUDE.md
    dsn = parse_dsn(
        "clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse"
    )

    # THEN: it parses to the values we expect for the test container
    assert dsn.host == "localhost"
    assert dsn.port == 9000
    assert dsn.user == "clickhouse"
    assert dsn.password == "clickhouse"
    assert dsn.database == "clickhouse"


# ---- percent-encoding ---------------------------------------------------


def test_percent_encoded_credentials_are_decoded() -> None:
    # BEGIN / WHEN: a DSN whose password contains an `@` literal
    #               (must be percent-encoded as %40 in the userinfo)
    dsn = parse_dsn("clickhouse://alice:p%40ss@host")

    # THEN: the parsed password is the decoded form
    assert dsn.password == "p@ss"


def test_percent_encoded_database_is_decoded() -> None:
    # BEGIN / WHEN: a database with a percent-encoded space
    dsn = parse_dsn("clickhouse://host/my%20db")

    # THEN: the parsed database is the decoded form
    assert dsn.database == "my db"


# ---- IPv6 hosts ---------------------------------------------------------


def test_ipv6_host_literal_is_unwrapped_from_brackets() -> None:
    # BEGIN / WHEN: an IPv6 literal in URL form requires bracketing
    dsn = parse_dsn("clickhouse://[::1]:9000/db")

    # THEN: the host comes through without brackets (urlparse strips them)
    assert dsn.host == "::1"
    assert dsn.port == 9000


# ---- trailing slash -----------------------------------------------------


def test_trailing_slash_path_falls_back_to_default_database() -> None:
    # BEGIN / WHEN: a DSN whose path is just "/"
    dsn = parse_dsn("clickhouse://host/")

    # THEN: database is the default (server-side default db)
    assert dsn.database == DEFAULT_DATABASE


# ---- secure / TLS -------------------------------------------------------


def test_clickhouses_scheme_implies_secure_and_default_secure_port() -> None:
    # BEGIN / WHEN: the clickhouses:// scheme without explicit port
    dsn = parse_dsn("clickhouses://host")

    # THEN: secure is on and the port defaults to the secure port
    assert dsn.secure is True
    assert dsn.port == DEFAULT_SECURE_PORT


def test_secure_query_param_promotes_default_port() -> None:
    # BEGIN / WHEN: clickhouse:// scheme with secure=true in the query
    dsn = parse_dsn("clickhouse://host?secure=true")

    # THEN: secure is on and the secure default port is picked
    assert dsn.secure is True
    assert dsn.port == DEFAULT_SECURE_PORT


def test_explicit_port_wins_over_secure_default() -> None:
    # BEGIN / WHEN: secure on but the user gave a custom port
    dsn = parse_dsn("clickhouse://host:1234?secure=true")

    # THEN: the explicit port is preserved
    assert dsn.secure is True
    assert dsn.port == 1234


# ---- compression --------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("none", CompressionMethod.NONE),
        ("off", CompressionMethod.NONE),
        ("false", CompressionMethod.NONE),
        ("lz4", CompressionMethod.LZ4),
        ("LZ4", CompressionMethod.LZ4),
        ("zstd", CompressionMethod.ZSTD),
        ("ZSTD", CompressionMethod.ZSTD),
    ],
)
def test_compression_query_param(value: str, expected: CompressionMethod) -> None:
    # BEGIN / WHEN: a DSN with a compression query param of any spelling
    dsn = parse_dsn(f"clickhouse://host?compression={value}")

    # THEN: it maps to the documented CompressionMethod (case-insensitive)
    assert dsn.compression == expected


# ---- connect_timeout ----------------------------------------------------


def test_connect_timeout_query_param_is_a_float() -> None:
    # BEGIN / WHEN: a DSN with a numeric connect_timeout
    dsn = parse_dsn("clickhouse://host?connect_timeout=2.5")

    # THEN: the timeout is parsed as float
    assert dsn.connect_timeout == 2.5


def test_connect_timeout_zero_or_negative_rejected() -> None:
    # BEGIN / WHEN / THEN: zero or negative timeout raises ValueError
    with pytest.raises(ValueError, match="must be positive"):
        parse_dsn("clickhouse://host?connect_timeout=0")
    with pytest.raises(ValueError, match="must be positive"):
        parse_dsn("clickhouse://host?connect_timeout=-1")


# ---- settings passthrough -----------------------------------------------


def test_unknown_query_params_become_settings() -> None:
    # BEGIN / WHEN: a DSN with unknown query params alongside known ones
    dsn = parse_dsn(
        "clickhouse://host?compression=lz4&max_block_size=65536&use_nulls=1"
    )

    # THEN: only the unknown params land in settings; known params are
    #       parsed away, not echoed
    assert dsn.compression == CompressionMethod.LZ4
    assert dsn.settings == {"max_block_size": "65536", "use_nulls": "1"}


# ---- error paths --------------------------------------------------------


def test_unsupported_scheme_raises() -> None:
    # BEGIN / WHEN / THEN: any non-clickhouse[s] scheme is rejected
    with pytest.raises(ValueError, match="unsupported DSN scheme"):
        parse_dsn("http://host")
    with pytest.raises(ValueError, match="unsupported DSN scheme"):
        parse_dsn("postgres://host")


def test_missing_host_raises() -> None:
    # BEGIN / WHEN / THEN: a DSN without a host fails fast
    with pytest.raises(ValueError, match="missing host"):
        parse_dsn("clickhouse://")


def test_invalid_compression_value_raises() -> None:
    # BEGIN / WHEN / THEN: a compression value we don't recognise raises
    with pytest.raises(ValueError, match="unsupported compression"):
        parse_dsn("clickhouse://host?compression=brotli")


def test_invalid_secure_value_raises() -> None:
    # BEGIN / WHEN / THEN: a non-boolean secure value raises
    with pytest.raises(ValueError, match="invalid bool"):
        parse_dsn("clickhouse://host?secure=maybe")
