"""KPI 1 — round-trip latency for a single ``SELECT 1``.

Each measured run is one ``SELECT 1``. We do many runs (default 200)
to get enough samples for p50 / p95 / p99 to be meaningful. The number
of warmup iterations is small (default 20) — the curve flattens out
fast for native protocols and we want HTTP's connection-pool warmup
to be visible in the data.
"""

from __future__ import annotations

import asyncio
import time

from adapters import get_adapter

from .common import RunResult, base_arg_parser, emit, library_label


async def _main() -> None:
    parser = base_arg_parser("ping_latency", default_runs=200, default_warmup=20)
    args = parser.parse_args()

    adapter = get_adapter(args.library, args.dsn_native, args.dsn_http)
    async with adapter.connect() as client:
        for _ in range(args.warmup):
            await client.select_one("SELECT 1")
        for run_idx in range(args.runs):
            t0 = time.perf_counter()
            await client.select_one("SELECT 1")
            t1 = time.perf_counter()
            emit(
                RunResult(
                    library=library_label(args.library),
                    scenario="ping_latency",
                    run=run_idx,
                    elapsed_ms=(t1 - t0) * 1000.0,
                    rows=1,
                )
            )


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
