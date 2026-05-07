"""KPI 5 — RSS over time during a 5M-row read.

A peak-RSS number alone hides the shape of a library's memory
trajectory. Two libraries that both peak at the same MiB look
identical on a bar chart, but one might have buffered the entire
result before yielding it (sharp spike) while the other streams
(flat, low). The scenario therefore samples the process's resident
set every ``--sample-ms`` (default 50 ms) on a background thread
while the read runs, and emits the full time series on the JSON line
so the report can render an overlay line chart.

Each scenario invocation is already its own subprocess (driven by
``run.sh``), so the baseline sample is cleanly isolated from the
other scenarios — no prior-connection state inflates the curve.
"""

from __future__ import annotations

import asyncio
import threading
import time

import psutil
from adapters import get_adapter

from .common import RunResult, base_arg_parser, emit, library_label

_DEFAULT_ROWS = 5_000_000


class _RssSampler:
    """Sample the process's resident-set size on a background thread.

    Stores ``(t_ms, rss_mib)`` tuples relative to the sampler's start
    time. Daemon thread so a crash in the main coroutine doesn't hang
    the process; ``stop()`` joins cleanly when the read completes.
    """

    def __init__(self, interval_s: float) -> None:
        self._proc = psutil.Process()
        self._interval = interval_s
        self._samples: list[dict[str, float]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._t0 = 0.0

    def __enter__(self) -> _RssSampler:
        self._t0 = time.perf_counter()
        # Capture an explicit baseline at t=0 so the time series starts
        # at the read's true entry point even if the first scheduled
        # tick happens slightly later.
        self._record()
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join()
        # Final sample after the read returns — guarantees the trailing
        # value reflects post-read state regardless of where the last
        # tick landed.
        self._record()

    def _record(self) -> None:
        t_ms = (time.perf_counter() - self._t0) * 1000.0
        rss_mib = self._proc.memory_info().rss / (1024.0 * 1024.0)
        self._samples.append({"t_ms": round(t_ms, 2), "rss_mib": round(rss_mib, 2)})

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._record()
            # ``Event.wait`` doubles as the sleep + early-cancel signal,
            # so ``stop()`` returns promptly instead of waiting out the
            # full interval.
            if self._stop.wait(self._interval):
                return

    @property
    def samples(self) -> list[dict[str, float]]:
        return self._samples


def _summarise(samples: list[dict[str, float]]) -> dict[str, float]:
    """Pull the headline numbers out of the time series so the report
    can render a table even when not unfolding the full curve."""
    if not samples:
        return {}
    rss_values = [s["rss_mib"] for s in samples]
    baseline = rss_values[0]
    peak = max(rss_values)
    return {
        "rss_baseline_mib": round(baseline, 2),
        "rss_peak_mib": round(peak, 2),
        "rss_delta_mib": round(peak - baseline, 2),
    }


async def _main() -> None:
    parser = base_arg_parser("memory_ceiling", default_runs=1, default_warmup=0)
    parser.add_argument(
        "--rows",
        type=int,
        default=_DEFAULT_ROWS,
        help=f"Rows to scan in one go (default {_DEFAULT_ROWS:_})",
    )
    parser.add_argument(
        "--sample-ms",
        type=float,
        default=50.0,
        help="RSS sampler interval in milliseconds (default 50)",
    )
    args = parser.parse_args()

    adapter = get_adapter(args.library, args.dsn_native, args.dsn_http)
    sql = f"SELECT number FROM system.numbers LIMIT {args.rows}"
    interval_s = args.sample_ms / 1000.0
    async with adapter.connect() as client:
        with _RssSampler(interval_s) as sampler:
            t0 = time.perf_counter()
            n = await client.select_rows(sql)
            t1 = time.perf_counter()

    extra: dict[str, object] = {
        "rss_samples": sampler.samples,
        "sample_interval_ms": args.sample_ms,
        **_summarise(sampler.samples),
    }
    emit(
        RunResult(
            library=library_label(args.library),
            scenario="memory_ceiling",
            run=0,
            elapsed_ms=(t1 - t0) * 1000.0,
            rows=n,
            extra=extra,
        )
    )


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
