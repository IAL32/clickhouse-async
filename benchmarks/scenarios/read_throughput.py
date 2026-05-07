"""KPI 2 — large-result throughput.

Each measured run reads ``--rows`` rows of ``(UInt64, String, DateTime)``
from ``system.numbers`` and reports the wall time. The triple covers
one numeric, one variable-length string, and one date — exercising
each library's row-tuple decode path across the common type families.
"""

from __future__ import annotations

import asyncio
import time

from adapters import get_adapter

from .common import RunResult, base_arg_parser, emit, library_label


def _query(n_rows: int) -> str:
    # ``toString(number)`` gives a non-trivial String column whose
    # size scales with row index; ``now()`` gives a constant DateTime
    # so per-row encoding cost dominates over server-side computation.
    return f"SELECT number, toString(number), now() FROM system.numbers LIMIT {n_rows}"


async def _main() -> None:
    parser = base_arg_parser("read_throughput", default_runs=10, default_warmup=3)
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Rows per measured run (default 1_000_000)",
    )
    args = parser.parse_args()

    adapter = get_adapter(args.library, args.dsn_native, args.dsn_http)
    async with adapter.connect() as client:
        sql = _query(args.rows)
        for _ in range(args.warmup):
            await client.select_rows(sql)
        for run_idx in range(args.runs):
            t0 = time.perf_counter()
            n = await client.select_rows(sql)
            t1 = time.perf_counter()
            emit(
                RunResult(
                    library=library_label(args.library),
                    scenario="read_throughput",
                    run=run_idx,
                    elapsed_ms=(t1 - t0) * 1000.0,
                    rows=n,
                )
            )


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
