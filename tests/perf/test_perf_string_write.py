"""Micro-benchmarks for ``String`` column encode.

Insert-side counterpart to ``test_perf_string_read``. The 100k-row
insert benchmark spends most of its codec time inside
``String.write`` since varuint length + UTF-8 encode + bytearray
append per row is the per-cell shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.types.string import String

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize("length", [8, 64], ids=["len8", "len64"])
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_string_write_sync(
    benchmark: BenchmarkFixture, n_rows: int, length: int
) -> None:
    """Encode ``n_rows`` strings into a fresh ``BinaryWriter``."""
    values = ["x" * length] * n_rows
    codec = String()

    def run() -> int:
        writer = BinaryWriter()
        codec.write(writer, values)
        return len(writer)

    nbytes = benchmark(run)
    # Sanity: each row is varuint(length) + length bytes. For length=8
    # the varuint is 1 byte, so total = n_rows * (1+8) = 9*n_rows.
    assert nbytes >= n_rows * length
