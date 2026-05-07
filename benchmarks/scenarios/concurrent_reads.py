"""KPI 4 — concurrent-read throughput.

Each measured run fans out ``--concurrency`` parallel SELECTs over
``asyncio.gather``. We record total wall time and the concurrency
factor so the report can compute aggregate rows/sec and tail latency.

For ``ca`` and ``asynch`` we open one connection per concurrent task
(the recommended pattern for these libraries — both forbid concurrent
queries on a single connection). For ``cc`` (HTTP) we share one
``AsyncClient`` since clickhouse-connect's async client multiplexes
requests over its connection pool internally.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from adapters import get_adapter

from .common import RunResult, base_arg_parser, emit, library_label

if TYPE_CHECKING:
    import argparse

    from adapters.base import Adapter

_QUERY = "SELECT count() FROM numbers(100000)"


def _coerce_count(row: object) -> int:
    if row is None:
        return 0
    if hasattr(row, "__getitem__"):
        return int(row[0])  # type: ignore[index]
    return int(row)  # type: ignore[arg-type]


async def _run_native_fanout(
    args: argparse.Namespace, adapter: Adapter
) -> tuple[int, float]:
    """For native libs each task gets its own connection."""

    async def one() -> int:
        async with adapter.connect() as c:
            return _coerce_count(await c.select_one(_QUERY))

    t0 = time.perf_counter()
    counts = await asyncio.gather(*(one() for _ in range(args.concurrency)))
    t1 = time.perf_counter()
    return sum(counts), (t1 - t0) * 1000.0


async def _run_shared_client_fanout(
    args: argparse.Namespace, adapter: Adapter
) -> tuple[int, float]:
    """For clickhouse-connect we share one client across all tasks —
    the async client's internal pool handles concurrency."""
    async with adapter.connect() as client:
        # Warmup is per-fanout already; do a single ping to confirm the
        # client is hot before timing.
        await client.select_one("SELECT 1")

        async def one() -> int:
            return _coerce_count(await client.select_one(_QUERY))

        t0 = time.perf_counter()
        counts = await asyncio.gather(*(one() for _ in range(args.concurrency)))
        t1 = time.perf_counter()
        return sum(counts), (t1 - t0) * 1000.0


async def _main() -> None:
    parser = base_arg_parser("concurrent_reads", default_runs=10, default_warmup=3)
    parser.add_argument("--concurrency", type=int, default=16)
    args = parser.parse_args()

    adapter = get_adapter(args.library, args.dsn_native, args.dsn_http)
    runner = _run_shared_client_fanout if args.library == "cc" else _run_native_fanout

    for _ in range(args.warmup):
        await runner(args, adapter)
    for run_idx in range(args.runs):
        rows_seen, elapsed = await runner(args, adapter)
        emit(
            RunResult(
                library=library_label(args.library),
                scenario="concurrent_reads",
                run=run_idx,
                elapsed_ms=elapsed,
                rows=rows_seen,
                extra={"concurrency": args.concurrency},
            )
        )


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
