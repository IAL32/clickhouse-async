"""Micro-benchmarks for ``String`` column decode.

The canonical hot loop. The 1M-row read scenario in ``benchmarks/``
spends ~45% of its wall time inside ``String.read``; this file lets
us track the per-codec cost in isolation across PRs.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader
from clickhouse_async.types.string import String

from .conftest import build_string_column

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize("length", [8, 64], ids=["len8", "len64"])
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_string_read_async(
    benchmark: BenchmarkFixture, n_rows: int, length: int
) -> None:
    """Current implementation — async reader, await per byte for the
    varuint length and per row for the body.

    Each ``benchmark`` round wraps the whole read in
    ``asyncio.run`` so the measurement includes the event-loop
    set-up / tear-down. For 100k-row workloads the per-call work
    is ~tens of ms; the ~1ms ``asyncio.run`` floor is in the noise."""
    body = build_string_column(n_rows, length)
    codec = String()

    def run() -> int:
        async def _inner() -> int:
            reader = AsyncBinaryReader.from_bytes(body)
            rows = await codec.read(reader, n_rows)
            return len(rows)

        return asyncio.run(_inner())

    decoded = benchmark(run)
    assert decoded == n_rows
