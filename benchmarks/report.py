"""Aggregate raw JSON-line results into a CSV, a markdown report, and
one PNG bar chart per KPI.

Input  — ``results/raw.jsonl`` (one JSON object per line, schema in
         ``scenarios/common.py::RunResult``).
Output — ``results/results.csv``     (flattened, one row per measured run)
         ``results/report.md``       (machine info + per-KPI tables)
         ``results/<scenario>.png``  (median bar + p95 error whisker)

The script is run by ``benchmarks/run.sh`` after every scenario has
finished. It can also be re-run on a previous ``raw.jsonl`` to
regenerate the report without re-collecting data.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # no display needed; we only save PNGs
import matplotlib.pyplot as plt

# Order the libraries are presented in (across tables, charts, the lot).
# Pinned so successive reports are visually consistent.
_LIB_ORDER = (
    "clickhouse-async",
    "asynch (PyPI)",
    "asynch (tacto fork)",
    "clickhouse-connect (thread-pool)",
    "clickhouse-connect (native async)",
)
_LIB_COLOURS = {
    "clickhouse-async": "#1f77b4",
    "asynch (PyPI)": "#ff7f0e",
    "asynch (tacto fork)": "#d62728",
    "clickhouse-connect (thread-pool)": "#2ca02c",
    "clickhouse-connect (native async)": "#9467bd",
}

# Per-scenario presentation metadata. Throughput-style scenarios show
# rows/sec; latency-style show ms; memory shows MiB.
_SCENARIO_META: dict[str, dict[str, str]] = {
    "ping_latency": {
        "title": "Ping latency (`SELECT 1`)",
        "metric": "Latency (ms, lower is better)",
        "aggregate": "latency_ms",
    },
    "read_throughput": {
        "title": "Read throughput (1M rows scanned)",
        "metric": "Throughput (rows/sec, higher is better)",
        "aggregate": "rows_per_sec",
    },
    "insert_throughput": {
        "title": "Insert throughput (100k bulk INSERT)",
        "metric": "Throughput (rows/sec, higher is better)",
        "aggregate": "rows_per_sec",
    },
    "concurrent_reads": {
        "title": "Concurrent reads (16-way fan-out)",
        "metric": "Total wall time (ms, lower is better)",
        "aggregate": "latency_ms",
    },
    "memory_ceiling": {
        "title": "RSS over time (5M-row read)",
        "metric": "Resident set size (MiB) over wall-clock time (ms)",
        "aggregate": "rss_timeseries",
    },
}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            rows.append(json.loads(stripped))
    return rows


def _read_environment(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _write_csv(rows: list[dict[str, Any]], out: Path) -> None:
    """Flatten the nested ``extra`` dict into top-level columns so the
    CSV is consumable by a spreadsheet tool without further parsing."""
    fields = sorted(
        {k for r in rows for k in r}
        | {f"extra.{k}" for r in rows for k in r.get("extra", {})}
    )
    fields = [f for f in fields if f != "extra"]
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            row = {k: v for k, v in r.items() if k != "extra"}
            for k, v in r.get("extra", {}).items():
                row[f"extra.{k}"] = v
            writer.writerow(row)


def _summarise(samples: list[float]) -> dict[str, float]:
    """p50/p95/p99/min/max/mean for a sample series. Falls back
    gracefully when the run count is too low for percentiles to be
    meaningful — they're computed by linear interpolation, so 3
    samples still yields a valid (if noisy) p95."""
    if not samples:
        return {}
    sorted_samples = sorted(samples)
    return {
        "n": len(samples),
        "min": sorted_samples[0],
        "p50": statistics.median(sorted_samples),
        "p95": _percentile(sorted_samples, 0.95),
        "p99": _percentile(sorted_samples, 0.99),
        "max": sorted_samples[-1],
        "mean": statistics.fmean(sorted_samples),
    }


def _percentile(sorted_samples: list[float], q: float) -> float:
    if not sorted_samples:
        return float("nan")
    if len(sorted_samples) == 1:
        return sorted_samples[0]
    rank = q * (len(sorted_samples) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_samples) - 1)
    frac = rank - lo
    return sorted_samples[lo] * (1 - frac) + sorted_samples[hi] * frac


def _aggregate(rows: list[dict[str, Any]], aggregate: str) -> list[float]:
    """Convert each raw run's elapsed_ms / rows / extra into the
    aggregation metric the scenario reports."""
    series: list[float] = []
    for r in rows:
        elapsed = r["elapsed_ms"]
        if aggregate == "latency_ms":
            series.append(elapsed)
        elif aggregate == "rows_per_sec":
            if elapsed > 0:
                series.append(r["rows"] / (elapsed / 1000.0))
        elif aggregate == "rss_timeseries":
            # The summary table for memory_ceiling shows peak RSS so
            # it stays comparable with the line chart; the chart
            # itself renders the full time series via _render_chart.
            peak = r.get("extra", {}).get("rss_peak_mib")
            if peak is not None:
                series.append(peak)
    return series


def _format_value(value: float, aggregate: str) -> str:
    if aggregate == "rows_per_sec":
        return f"{value:>12,.0f}"
    if aggregate == "rss_timeseries":
        return f"{value:>10.1f}"
    return f"{value:>10.3f}"


def _render_bar_chart(
    scenario: str, by_lib: dict[str, dict[str, float]], out: Path
) -> None:
    meta = _SCENARIO_META[scenario]
    libs = [lib for lib in _LIB_ORDER if lib in by_lib]
    medians = [by_lib[lib]["p50"] for lib in libs]
    # Error whiskers go from p50 down to min and up to p95 — gives the
    # reader a quick sense of "is the median representative".
    err_low = [by_lib[lib]["p50"] - by_lib[lib]["min"] for lib in libs]
    err_high = [by_lib[lib]["p95"] - by_lib[lib]["p50"] for lib in libs]
    colours = [_LIB_COLOURS[lib] for lib in libs]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(
        libs,
        medians,
        yerr=[err_low, err_high],
        capsize=6,
        color=colours,
    )
    ax.set_title(meta["title"])
    ax.set_ylabel(meta["metric"])
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    # Library labels are long ("clickhouse-connect (thread-pool)" etc.);
    # at five-up they overlap if drawn flat. 45°-right keeps them
    # legible without tipping into 90°-vertical territory.
    ax.set_xticks(range(len(libs)))
    ax.set_xticklabels(libs, rotation=45, ha="right", rotation_mode="anchor")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def _render_rss_timeseries_chart(
    scenario: str, by_lib_rows: dict[str, list[dict[str, Any]]], out: Path
) -> None:
    """One line per library showing RSS-over-time during the read.

    Each library's last run contributes its full sample series. Peak
    RSS is surfaced as a small annotation next to each line for quick
    cross-comparison.
    """
    meta = _SCENARIO_META[scenario]

    fig, ax = plt.subplots(figsize=(9, 5))
    plotted = 0
    for lib in _LIB_ORDER:
        rows = by_lib_rows.get(lib)
        if not rows:
            continue
        # Take the last run's samples — for memory_ceiling each
        # invocation is a single run by default, so this just means
        # "the run we have".
        samples = rows[-1].get("extra", {}).get("rss_samples") or []
        if not samples:
            continue
        xs = [s["t_ms"] for s in samples]
        ys = [s["rss_mib"] for s in samples]
        colour = _LIB_COLOURS.get(lib)
        ax.plot(xs, ys, label=lib, color=colour, linewidth=1.6)
        peak = max(ys)
        ax.annotate(
            f"{peak:.0f} MiB",
            xy=(xs[ys.index(peak)], peak),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color=colour or "black",
        )
        plotted += 1

    ax.set_title(meta["title"])
    ax.set_xlabel("Wall-clock time during read (ms)")
    ax.set_ylabel("Resident set size (MiB)")
    ax.grid(axis="both", linestyle=":", alpha=0.5)
    if plotted:
        ax.legend(loc="best", fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def _render_chart(
    scenario: str,
    by_lib: dict[str, dict[str, float]],
    by_lib_rows: dict[str, list[dict[str, Any]]],
    out: Path,
) -> None:
    """Dispatch by scenario aggregate type. The bar-chart layout works
    for any single-number-per-run scenario; ``rss_timeseries`` needs
    the full sample series so it gets its own renderer."""
    if _SCENARIO_META[scenario]["aggregate"] == "rss_timeseries":
        _render_rss_timeseries_chart(scenario, by_lib_rows, out)
    else:
        _render_bar_chart(scenario, by_lib, out)


def _environment_block(env: dict[str, Any]) -> str:
    if not env:
        return "_environment.json not found — re-run via `run.sh` to capture._\n"
    rows = [
        ("Run timestamp (UTC)", env.get("timestamp", "?")),
        ("OS", env.get("os", "?")),
        ("CPU", env.get("cpu", "?")),
        ("RAM", env.get("ram", "?")),
        ("Power source", env.get("power", "n/a") or "n/a"),
        ("Python", env.get("python", "?")),
        ("Docker", env.get("docker", "?")),
        ("ClickHouse server", env.get("clickhouse_server", "?")),
        ("clickhouse-async", env.get("ca_version", "?")),
        ("asynch (PyPI)", env.get("asynch_pypi_version", "?")),
        ("asynch (tacto fork)", env.get("asynch_tacto_version", "?")),
        ("clickhouse-connect (thread-pool)", env.get("cc_version", "?")),
        ("clickhouse-connect (native async)", env.get("cc_async_version", "?")),
        ("Repo SHA", env.get("git_sha", "?")),
        (
            "Working tree",
            "dirty" if env.get("git_dirty") else "clean",
        ),
    ]
    lines = ["| Field | Value |", "|---|---|"]
    lines.extend(f"| {k} | {v} |" for k, v in rows)
    return "\n".join(lines) + "\n"


def _render_table(
    scenario: str,
    by_lib: dict[str, dict[str, float]],
    aggregate: str,
) -> str:
    headers = ("Library", "n", "p50", "p95", "p99", "min", "max", "mean")
    lines = [f"### {_SCENARIO_META[scenario]['title']}\n"]
    lines.append(f"_{_SCENARIO_META[scenario]['metric']}_\n")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for lib in _LIB_ORDER:
        if lib not in by_lib:
            continue
        s = by_lib[lib]
        lines.append(
            "| "
            + " | ".join(
                [
                    lib,
                    str(int(s["n"])),
                    _format_value(s["p50"], aggregate).strip(),
                    _format_value(s["p95"], aggregate).strip(),
                    _format_value(s["p99"], aggregate).strip(),
                    _format_value(s["min"], aggregate).strip(),
                    _format_value(s["max"], aggregate).strip(),
                    _format_value(s["mean"], aggregate).strip(),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "raw",
        nargs="?",
        default="results/raw.jsonl",
        help="Path to the JSON-line input (default results/raw.jsonl)",
    )
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Directory to write report.md / *.png / results.csv into",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    raw_path = Path(args.raw)
    rows = _read_jsonl(raw_path)
    if not rows:
        print(f"no rows in {raw_path}", file=sys.stderr)
        sys.exit(1)

    _write_csv(rows, results_dir / "results.csv")

    # Group rows by (scenario, library)
    by_scenario: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for r in rows:
        by_scenario.setdefault(r["scenario"], {}).setdefault(r["library"], []).append(r)

    md_lines = ["# Benchmark report\n"]
    md_lines.append("## Environment\n")
    env = _read_environment(results_dir / "environment.json")
    md_lines.append(_environment_block(env))
    md_lines.append("\n## Results\n")

    for scenario, meta in _SCENARIO_META.items():
        if scenario not in by_scenario:
            continue
        by_lib_summary: dict[str, dict[str, float]] = {}
        for lib, lib_rows in by_scenario[scenario].items():
            samples = _aggregate(lib_rows, meta["aggregate"])
            if samples:
                by_lib_summary[lib] = _summarise(samples)
        if not by_lib_summary:
            continue
        md_lines.append(_render_table(scenario, by_lib_summary, meta["aggregate"]))
        chart_path = results_dir / f"{scenario}.png"
        _render_chart(scenario, by_lib_summary, by_scenario[scenario], chart_path)
        md_lines.append(f"![{scenario}]({chart_path.name})\n\n")

    (results_dir / "report.md").write_text("".join(md_lines))
    print(f"wrote {results_dir / 'report.md'}")


if __name__ == "__main__":
    main()
