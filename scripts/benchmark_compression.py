#!/usr/bin/env python3
"""Benchmark LZ4 / ZSTD / no-compression on a representative query.

Usage:
    uv run python scripts/benchmark_compression.py
    uv run python scripts/benchmark_compression.py --localdb
    uv run python scripts/benchmark_compression.py --localdb=clickhouse://user:pass@host:9000/db
    uv run python scripts/benchmark_compression.py --rows 500000

Starts a ClickHouse container by default (requires docker + testcontainers).
Pass --localdb to hit a locally running server instead.

The script creates a temporary table, inserts representative data
(integers, floats, strings, timestamps), then runs three SELECT passes —
one per compression setting — and prints a summary table.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

# Make sure the project root is importable when run from the repo.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import clickhouse_async as ch
from clickhouse_async.protocol.compression import CompressionMethod

_DEFAULT_DSN = "clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse"
_TABLE = "benchmark_compression_tmp"
_BATCH = 65_536


async def _setup(client: ch.Client, n_rows: int) -> None:
    await client.execute(f"DROP TABLE IF EXISTS {_TABLE}")
    await client.execute(f"""
        CREATE TABLE {_TABLE} (
            id        UInt64,
            val       Float64,
            category  LowCardinality(String),
            label     String,
            ts        DateTime
        ) ENGINE = Memory
    """)

    categories = ["alpha", "beta", "gamma", "delta", "epsilon"]
    base_ts = datetime(2026, 1, 1, tzinfo=UTC)
    total_sent = 0
    batch_num = 0
    while total_sent < n_rows:
        count = min(_BATCH, n_rows - total_sent)
        rows = [
            (
                total_sent + i,
                (total_sent + i) * 0.123456,
                categories[(total_sent + i) % len(categories)],
                f"label-{(total_sent + i) % 1000:04d}",
                base_ts,
            )
            for i in range(count)
        ]
        await client.insert(
            f"INSERT INTO {_TABLE} (id, val, category, label, ts) VALUES",
            rows=rows,
            column_names=("id", "val", "category", "label", "ts"),
        )
        total_sent += count
        batch_num += 1

    print(f"  Inserted {total_sent:,} rows in {batch_num} batches.")


async def _bench_one(
    dsn: str, method: CompressionMethod, label: str, n_rows: int, deadline: float
) -> tuple[str, float, int]:
    """Run one SELECT pass and return (label, elapsed_s, bytes_read)."""
    async with ch.connect(dsn, compression=method) as client:
        start = time.monotonic()
        async with asyncio.timeout(deadline):
            result = await client.execute(
                f"SELECT id, val, category, label, ts FROM {_TABLE}"
            )
        elapsed = time.monotonic() - start
    assert result.row_count == n_rows, f"expected {n_rows} rows, got {result.row_count}"
    result_bytes = result.profile_info.bytes if result.profile_info else 0
    return label, elapsed, result_bytes


async def _teardown(dsn: str) -> None:
    async with ch.connect(dsn) as client:
        await client.execute(f"DROP TABLE IF EXISTS {_TABLE}")


async def _run(dsn: str, n_rows: int, deadline: float) -> None:
    print(f"\nSetting up {n_rows:,} rows …")
    async with ch.connect(dsn, compression=CompressionMethod.NONE) as client:
        await _setup(client, n_rows)

    methods = [
        (CompressionMethod.NONE, "none"),
        (CompressionMethod.LZ4, "lz4"),
        (CompressionMethod.ZSTD, "zstd"),
    ]

    results: list[tuple[str, float, int]] = []
    for method, label in methods:
        print(f"  Benchmarking {label} …", end=" ", flush=True)
        try:
            row = await _bench_one(dsn, method, label, n_rows, deadline)
            results.append(row)
            print(f"{row[1]:.3f}s")
        except TimeoutError:
            print(f"SKIP (timed out after {deadline:.0f}s)")
        except Exception as exc:
            print(f"SKIP ({exc})")

    await _teardown(dsn)

    print("\n" + "=" * 60)
    print(f"{'compression':<14} {'elapsed':>10} {'rows/s':>12} {'MB result':>10}")
    print("-" * 60)
    for label, elapsed, read_bytes in results:
        rows_per_s = n_rows / elapsed if elapsed > 0 else float("inf")
        mb = read_bytes / 1_048_576
        print(f"{label:<14} {elapsed:>10.3f}s {rows_per_s:>11,.0f} {mb:>9.1f} MB")
    print("=" * 60)

    if len(results) >= 2:
        none_elapsed = next((e for lbl, e, _ in results if lbl == "none"), None)
        for label, elapsed, _ in results:
            if label != "none" and none_elapsed:
                speedup = none_elapsed / elapsed
                print(f"  {label} vs none: {speedup:.2f}x speedup")
    print()


def _get_dsn(args: argparse.Namespace) -> str:
    if args.localdb is None:
        return _DEFAULT_DSN
    if args.localdb == "":
        return _DEFAULT_DSN
    return args.localdb


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--localdb",
        nargs="?",
        const="",
        default=None,
        metavar="DSN",
        help="Use a local ClickHouse server instead of a container. "
        "Omit a value to use the default DSN.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000,
        help="Number of rows to benchmark (default: 200000).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-method query timeout in seconds (default: 60).",
    )
    args = parser.parse_args()

    if args.localdb is not None:
        dsn = _get_dsn(args)
        asyncio.run(_run(dsn, args.rows, args.timeout))
    else:
        # Container path — import testcontainers lazily.
        try:
            from tests.containers.clickhouse import ClickHouseContainer
        except ImportError:
            print(
                "testcontainers not available. "
                "Run with --localdb to benchmark against a local server.",
                file=sys.stderr,
            )
            sys.exit(1)

        with ClickHouseContainer() as container:
            dsn = container.dsn
            asyncio.run(_run(dsn, args.rows, args.timeout))


if __name__ == "__main__":
    main()
