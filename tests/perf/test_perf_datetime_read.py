"""Micro-benchmarks for ``DateTime`` / ``Date`` / ``Date32`` decode.

Per profiling, ``DateTime.read`` is the largest remaining slice of
the 1M-row mixed-type read benchmark — the per-row
``datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None)`` call is
two object allocations through the public Python API. This file
pins the per-codec cost so future C-extension work can be measured
against a stable baseline.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.types.datetime import Date, Date32, DateTime

from .conftest import build_date_column, build_datetime_column

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_datetime_read_naive(benchmark: BenchmarkFixture, n_rows: int) -> None:
    """Naive (no timezone) ``DateTime`` — the path that goes through
    ``_naive_utc_from_ts`` per row."""
    body = build_datetime_column(n_rows)
    codec = DateTime()

    def run() -> int:
        reader = SyncBinaryReader(body)
        rows = codec.read(reader, n_rows)
        return len(rows)

    decoded = benchmark(run)
    assert decoded == n_rows


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_datetime_read_aware(benchmark: BenchmarkFixture, n_rows: int) -> None:
    """Timezone-aware ``DateTime('UTC')`` — the path that goes
    through ``datetime.fromtimestamp(ts, tz=tz)`` per row."""
    body = build_datetime_column(n_rows)
    codec = DateTime("UTC")

    def run() -> int:
        reader = SyncBinaryReader(body)
        rows = codec.read(reader, n_rows)
        return len(rows)

    decoded = benchmark(run)
    assert decoded == n_rows


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_date_read(benchmark: BenchmarkFixture, n_rows: int) -> None:
    """``Date`` — UInt16 days since epoch decoded into ``date``."""
    body = build_date_column(n_rows)
    codec = Date()

    def run() -> int:
        reader = SyncBinaryReader(body)
        rows = codec.read(reader, n_rows)
        return len(rows)

    decoded = benchmark(run)
    assert decoded == n_rows


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_date32_read(benchmark: BenchmarkFixture, n_rows: int) -> None:
    """``Date32`` — Int32 days since epoch decoded into ``date``.
    Same per-row work as ``Date`` but a wider on-wire integer."""
    body = build_date_column(n_rows)
    # Date32 reads 4 bytes per row; widen the column body accordingly.
    wide = struct.pack(
        f"<{n_rows}i",
        *struct.unpack(f"<{n_rows}H", body),
    )
    codec = Date32()

    def run() -> int:
        reader = SyncBinaryReader(wide)
        rows = codec.read(reader, n_rows)
        return len(rows)

    decoded = benchmark(run)
    assert decoded == n_rows
