"""Micro-benchmarks for ``String`` column decode.

The canonical hot loop. The 1M-row read scenario in ``benchmarks/``
spends ~45% of its wall time inside ``String.read``; this file lets
us track the per-codec cost in isolation across PRs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.types.string import String

from .conftest import build_string_column

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize("length", [8, 64], ids=["len8", "len64"])
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_string_read_sync(
    benchmark: BenchmarkFixture, n_rows: int, length: int
) -> None:
    """Sync codec read directly off an in-memory buffer — no event loop."""
    body = build_string_column(n_rows, length)
    codec = String()

    def run() -> int:
        reader = SyncBinaryReader(body)
        rows = codec.read(reader, n_rows)
        return len(rows)

    decoded = benchmark(run)
    assert decoded == n_rows
