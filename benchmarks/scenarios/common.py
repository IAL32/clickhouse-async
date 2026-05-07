"""Shared bits for the per-scenario CLIs.

Every scenario follows the same pattern:

1. Parse ``--library``, ``--runs``, ``--warmup`` and any scenario-
   specific knobs.
2. Open one connection via the chosen adapter.
3. Run ``warmup`` warmup iterations (results discarded).
4. Run ``runs`` measured iterations, emitting one JSON line per run.

The JSON shape is fixed so ``benchmarks/report.py`` can ingest output
from every scenario without per-scenario parsing logic.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

# DSN defaults match ``scripts/clickhouse.sh up`` so the orchestrator
# can ``./scripts/clickhouse.sh up`` and the bench picks up the same
# server with no extra plumbing.
DEFAULT_DSN_NATIVE = "clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse"
DEFAULT_DSN_HTTP = "http://clickhouse:clickhouse@localhost:8123/clickhouse"


@dataclass
class RunResult:
    """One JSON-line worth of measurement.

    ``elapsed_ms`` is wall time the scenario measured for one run;
    ``rows`` is whatever row count the scenario considers a unit (one
    SELECT 1 → ``rows=1``; a 1M-row scan → ``rows=1_000_000``). The
    report uses ``rows / elapsed_ms`` to compute throughput where it
    makes sense.

    ``extra`` is a free-form dict for scenario-specific fields (e.g.
    concurrent fan-out factor, peak RSS for memory_ceiling).
    """

    library: str
    scenario: str
    run: int
    elapsed_ms: float
    rows: int
    extra: dict[str, Any] = field(default_factory=dict)


def emit(result: RunResult) -> None:
    """Print one JSON line to stdout. ``flush=True`` so the orchestrator
    can pipe-tee live without waiting for the process to exit."""
    sys.stdout.write(json.dumps(asdict(result)))
    sys.stdout.write("\n")
    sys.stdout.flush()


def base_arg_parser(
    scenario: str, *, default_runs: int, default_warmup: int
) -> argparse.ArgumentParser:
    """Common CLI flags. Scenario modules add their own on top."""
    parser = argparse.ArgumentParser(
        description=f"Benchmark scenario: {scenario}",
    )
    parser.add_argument(
        "--library",
        choices=("ca", "asynch_pypi", "asynch_tacto", "cc", "cc_async"),
        required=True,
        help=(
            "ca = clickhouse-async, asynch_pypi = long2ice/asynch from "
            "PyPI, asynch_tacto = nils-borrmann-tacto/asynch fork, "
            "cc = clickhouse-connect 0.15.x (thread-pool async), "
            "cc_async = clickhouse-connect 1.0.0rc2 (native async)"
        ),
    )
    parser.add_argument("--runs", type=int, default=default_runs)
    parser.add_argument("--warmup", type=int, default=default_warmup)
    parser.add_argument(
        "--dsn-native",
        default=DEFAULT_DSN_NATIVE,
        help="Native-protocol DSN for the ca / asynch adapters",
    )
    parser.add_argument(
        "--dsn-http",
        default=DEFAULT_DSN_HTTP,
        help="HTTP DSN for the cc adapter",
    )
    return parser


def library_label(short: str) -> str:
    """Pretty name for the JSON output. Stable across runs so the
    report's grouping is self-consistent — the two asynch flavours get
    distinct labels even though they share the import module name."""
    return {
        "ca": "clickhouse-async",
        "asynch_pypi": "asynch (PyPI)",
        "asynch_tacto": "asynch (tacto fork)",
        "cc": "clickhouse-connect (thread-pool)",
        "cc_async": "clickhouse-connect (native async)",
    }[short]
