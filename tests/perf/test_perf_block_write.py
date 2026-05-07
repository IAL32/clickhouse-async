"""Micro-benchmark for whole-block encode.

Mirrors the column shape the cross-library benchmark's
``insert_throughput`` uses — ``(UInt64, String, Float64)`` over
``n_rows`` rows. Captures per-codec write overhead plus the
``write_block`` framing work.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.block import Block, BlockInfo, make_column, write_block
from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.packets import OUR_REVISION

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_block_write_sync(benchmark: BenchmarkFixture, n_rows: int) -> None:
    """Encode one whole block of ``(UInt64, String, Float64)``."""
    spec_id, vals_id = make_column("id", "UInt64", list(range(n_rows)))
    spec_label, vals_label = make_column(
        "label", "String", [f"label-{i % 1000}" for i in range(n_rows)]
    )
    spec_val, vals_val = make_column("val", "Float64", [i * 0.5 for i in range(n_rows)])
    block = Block(
        info=BlockInfo(),
        columns=[spec_id, spec_label, spec_val],
        n_rows=n_rows,
        data=[vals_id, vals_label, vals_val],
    )

    def run() -> int:
        writer = BinaryWriter()
        write_block(writer, block, revision=OUR_REVISION)
        return len(writer)

    nbytes = benchmark(run)
    assert nbytes > 0
