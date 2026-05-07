"""Micro-benchmarks for fixed-width primitive column decode.

``_StructCodec.read`` uses a single ``struct.unpack`` over the whole
buffer, so per-row Python work is bounded — this benchmark mostly
captures the reader-layer overhead (one ``read_exact(n_rows * size)``
per call) and gives us a baseline for the wider-int / float codecs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.types.primitive import Float64, Int32, Int64, UInt64

from .conftest import build_fixed_int_column

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.perf
@pytest.mark.parametrize(
    ("codec_factory", "fmt"),
    [
        (Int32, "i"),
        (Int64, "q"),
        (UInt64, "Q"),
        (Float64, "d"),
    ],
    ids=["Int32", "Int64", "UInt64", "Float64"],
)
@pytest.mark.parametrize("n_rows", [10_000, 100_000], ids=["n10k", "n100k"])
def test_fixed_width_read_sync(
    benchmark: BenchmarkFixture, n_rows: int, codec_factory: type, fmt: str
) -> None:
    """Read ``n_rows`` of a primitive in one bulk ``struct.unpack``."""
    body = build_fixed_int_column(n_rows, fmt)
    codec = codec_factory()

    def run() -> int:
        reader = SyncBinaryReader(body)
        values = codec.read(reader, n_rows)
        return len(values)

    decoded = benchmark(run)
    assert decoded == n_rows
