"""Micro-benchmark for whole-block decode.

Mirrors the column shape the cross-library benchmark's
``read_throughput`` uses — ``(UInt64, String, DateTime)`` over
``n_rows`` rows. A full block walk catches per-codec overhead plus
the ``read_block`` framing work (BlockInfo + per-column header
parsing). Compared against the per-codec micro-benchmarks, the
delta tells us how much the framing layer costs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.block import read_block
from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.protocol.packets import OUR_REVISION

from .conftest import build_block_body

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_block_read_sync(benchmark: BenchmarkFixture, n_rows: int) -> None:
    """Decode one whole block of ``(UInt64, String, DateTime)``."""
    body = build_block_body(n_rows)

    def run() -> int:
        reader = SyncBinaryReader(body)
        block = read_block(reader, revision=OUR_REVISION)
        return block.n_rows

    decoded = benchmark(run)
    assert decoded == n_rows
