"""Integration tests for LZ4 / ZSTD compressed connections.

These tests exercise the full round-trip path:
  client → compressed frame (CityHash128 + method header) → server
  server → compressed frame → client (decompress + checksum verify)

They are guarded by `pytest.importorskip` so a `bare` install
(no extras) skips cleanly rather than errors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch
from clickhouse_async.connection import _default_compression
from clickhouse_async.protocol.compression import CompressionMethod

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


pytestmark = pytest.mark.integration


@pytest.fixture(
    params=[CompressionMethod.LZ4, CompressionMethod.ZSTD],
    ids=["lz4", "zstd"],
)
def compression(request: pytest.FixtureRequest) -> CompressionMethod:
    """Parametrize over LZ4 and ZSTD, skipping each if the matching
    extra is not installed."""
    method: CompressionMethod = request.param
    if method == CompressionMethod.LZ4:
        pytest.importorskip("lz4")
    elif method == CompressionMethod.ZSTD:
        pytest.importorskip("zstandard")
    pytest.importorskip("clickhouse_cityhash")
    return method


@pytest.fixture
async def compressed_client(
    dsn: str,
    compression: CompressionMethod,
) -> AsyncIterator[ch.Client]:
    """A Client opened with the given compression method."""
    async with ch.connect(dsn, compression=compression) as client:
        yield client


# ---- tests ------------------------------------------------------------


async def test_compressed_select_one_round_trips(
    compressed_client: ch.Client,
) -> None:
    # BEGIN: a client connected with LZ4 or ZSTD compression
    # WHEN: running the universal smoke query
    rows = await compressed_client.fetch_all("SELECT 1")

    # THEN: a single row containing the integer 1 comes back
    assert rows == [(1,)]


async def test_compressed_select_many_rows_round_trips(
    compressed_client: ch.Client,
) -> None:
    # BEGIN: a client with compression enabled
    # WHEN: selecting 10 000 rows — forces multi-block response
    rows = await compressed_client.fetch_all(
        "SELECT number FROM system.numbers LIMIT 10000"
    )

    # THEN: all 10 000 rows arrived in order without checksum errors
    assert len(rows) == 10000
    assert rows[0] == (0,)
    assert rows[-1] == (9999,)


async def test_compressed_insert_and_select_round_trips(
    dsn: str,
    compression: CompressionMethod,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: an empty Memory-engine table; client uses the tested
    #        compression method for both INSERT and SELECT
    table = "test_compression_insert_select"
    await fresh_table(table, "(id UInt64, name String) ENGINE = Memory")

    rows_in: list[tuple[object, ...]] = [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]

    async with ch.connect(dsn, compression=compression) as client:
        # WHEN: inserting rows over a compressed connection
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "name"],
        )

    # THEN: the server confirmed the correct row count
    assert n == 3

    async with ch.connect(dsn, compression=compression) as client:
        # WHEN: reading back over a compressed connection
        rows_out = await client.fetch_all(f"SELECT id, name FROM {table} ORDER BY id")

    # THEN: every row round-tripped through the compressed wire
    assert rows_out == rows_in


async def test_default_compression_auto_detects_lz4(dsn: str) -> None:
    # BEGIN: lz4 + cityhash extras installed; no explicit compression= arg
    pytest.importorskip("lz4")
    pytest.importorskip("clickhouse_cityhash")

    # WHEN: creating a Client without an explicit compression argument
    async with ch.connect(dsn) as client:
        rows = await client.fetch_all("SELECT 1")

    # THEN: _default_compression() resolved to LZ4 (the installed extra)
    #       and the query succeeded end-to-end
    assert _default_compression() == CompressionMethod.LZ4
    assert rows == [(1,)]


async def test_none_compression_is_always_available(dsn: str) -> None:
    # BEGIN: explicit CompressionMethod.NONE — no extras required
    # WHEN: connecting with compression disabled
    async with ch.connect(dsn, compression=CompressionMethod.NONE) as client:
        rows = await client.fetch_all("SELECT 1")

    # THEN: query succeeds without any compression library
    assert rows == [(1,)]
