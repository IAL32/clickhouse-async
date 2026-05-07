"""KPI 3 — bulk-insert throughput.

A ``Memory``-engine table is created once at startup and truncated
between runs. Each measured run inserts ``--rows`` rows of
``(UInt64, String, Float64)``; the row source is precomputed so
neither generation cost nor the GC of throwaway tuples leaks into
the timing.
"""

from __future__ import annotations

import asyncio
import time

from adapters import get_adapter

import clickhouse_async as ch

from .common import RunResult, base_arg_parser, emit, library_label

_TABLE = "bench_insert_throughput"


def _generate_rows(n: int) -> list[tuple[int, str, float]]:
    """Mix of integers, strings, and floats — the everyday wide-row
    shape an ETL pipeline would push through. Pre-built once outside
    the timed window."""
    return [(i, f"label-{i % 1000}", i * 0.5) for i in range(n)]


async def _setup_and_truncate(dsn: str) -> None:
    """Create the destination table from a clickhouse-async client so
    the bench doesn't depend on whichever adapter is being measured to
    do administrative DDL. Truncates between runs to keep block
    layout deterministic."""
    async with ch.connect(dsn) as admin:
        await admin.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        await admin.execute(
            f"CREATE TABLE {_TABLE} "
            f"(id UInt64, label String, val Float64) "
            f"ENGINE = Memory"
        )


async def _truncate(dsn: str) -> None:
    async with ch.connect(dsn) as admin:
        await admin.execute(f"TRUNCATE TABLE {_TABLE}")


async def _main() -> None:
    parser = base_arg_parser("insert_throughput", default_runs=10, default_warmup=3)
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Rows per measured run (default 100_000)",
    )
    args = parser.parse_args()

    rows = _generate_rows(args.rows)
    columns = ("id", "label", "val")

    await _setup_and_truncate(args.dsn_native)
    adapter = get_adapter(args.library, args.dsn_native, args.dsn_http)
    async with adapter.connect() as client:
        for _ in range(args.warmup):
            await client.insert_rows(_TABLE, rows, columns)
            await _truncate(args.dsn_native)
        for run_idx in range(args.runs):
            t0 = time.perf_counter()
            n = await client.insert_rows(_TABLE, rows, columns)
            t1 = time.perf_counter()
            emit(
                RunResult(
                    library=library_label(args.library),
                    scenario="insert_throughput",
                    run=run_idx,
                    elapsed_ms=(t1 - t0) * 1000.0,
                    rows=n if n else len(rows),
                )
            )
            await _truncate(args.dsn_native)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
