# Benchmarks

Head-to-head comparison of the async-capable Python ClickHouse clients
in current circulation:

| Short name | Library | Source | Protocol | Surface |
|---|---|---|---|---|
| `ca` | [`clickhouse-async`](https://github.com/IAL32/clickhouse-async) | this repo (editable) | Native TCP `:9000` | async-only |
| `asynch_pypi` | [`asynch`](https://github.com/long2ice/asynch) | PyPI release | Native TCP `:9000` | async-only |
| `asynch_tacto` | [`asynch`](https://github.com/nils-borrmann-tacto/asynch) (fork) | git, `main` | Native TCP `:9000` | async-only |
| `cc` | [`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect) `0.15.x` | PyPI release | HTTP `:8123` | async = thread-pool wrapper |
| `cc_async` | [`clickhouse-connect[async]`](https://github.com/ClickHouse/clickhouse-connect) `1.0.0rc2` | PyPI prerelease | HTTP `:8123` | async = native |

Two pairs install the **same** Python module from different sources or
versions and therefore can't coexist in one venv:

- `asynch_pypi` / `asynch_tacto` — both import `asynch`
- `cc` / `cc_async` — both import `clickhouse_connect`

The pyproject.toml declares each pair as `[tool.uv].conflicts` so
`uv lock` resolves them separately. `run.sh` reinstalls when flipping
between members of a conflict group.

The goal is one reproducible report a reader can scroll once and walk
away with an informed library choice.

## What it measures (KPIs)

| # | KPI | Workload | What it tells you |
|---|---|---|---|
| 1 | **Round-trip latency** | `SELECT 1` × 200 sequentially | Cost of one request — viable for interactive paths? |
| 2 | **Read throughput** | `SELECT number, toString(number), now() FROM system.numbers LIMIT 1_000_000` | Analytics / dashboard speed |
| 3 | **Insert throughput** | Bulk INSERT 100 000 rows of `(UInt64, String, Float64)` | ETL pipelines |
| 4 | **Concurrent throughput** | 16-way fan-out `SELECT count() FROM numbers(100_000)` | Pool / event-loop behaviour under contention |
| 5 | **RSS over time** | Single-process scan of 5M rows, sampled every 50 ms via `psutil` | Memory trajectory — buffered (sharp spike) vs streaming (flat) |

Each scenario does 3 warmup runs followed by 10 measured runs (200 / 20
for the ping scenario where each "run" is one query). Percentiles
(p50/p95/p99), min / max / mean are computed across the measured runs.
The report renders one matplotlib bar chart per KPI with median bars +
p95 error whiskers.

## How to run

```bash
cd benchmarks
./run.sh                           # full run, 4 labels, ~5 min
./run.sh --quick                   # smaller dataset, ~1 min
./run.sh --library ca              # one library only
./run.sh --library asynch_tacto    # one asynch flavour only
./run.sh --down                    # stop the container after
```

`run.sh` calls `uv sync --extra <lib>` for each requested label
(reinstalling `asynch` between the two `asynch_*` passes), so you do
**not** need to pre-sync — the orchestrator handles it.

If you want to install manually for ad-hoc debugging:

```bash
uv sync --extra ca --extra cc_async --extra asynch_tacto    # 3 of 5
uv run python -m scenarios.ping_latency --library asynch_tacto --runs 50
```

Output lands in `results/`:

| File | Contents |
|---|---|
| `results/raw.jsonl` | One JSON record per measured run (full sample log) |
| `results/results.csv` | Same data flattened for spreadsheet tools |
| `results/environment.json` | Machine / version snapshot for the run |
| `results/report.md` | Human-readable summary with embedded charts |
| `results/<scenario>.png` | One bar chart per KPI |

## Important — read before quoting numbers

These benchmarks are **opinionated** by necessity. The compromises and
their direction of bias are documented inline so you can reason about
which numbers translate to your environment and which don't.

### Compromises

1. **HTTP vs native is structurally unequal.** clickhouse-connect rides
   ClickHouse's HTTP server on port 8123; the others ride the native
   TCP server on port 9000. We benchmark **what users observe**
   (request → response wall time), not raw protocol efficiency. Real
   WAN latency compresses the gap; localhost amplifies it.
2. **Async-only.** clickhouse-connect supports both sync and async; we
   use `clickhouse_connect.get_async_client` for both labels. The
   `cc` label benchmarks the stable 0.15.x line — its async client is
   a thread-pool wrapper around the sync driver. The `cc_async` label
   benchmarks the 1.0.0rc2 prerelease's native async client, which
   ClickHouse upstream calls a "drop-in replacement with the same API
   surface". Both are reported so users can see what each install
   gives them today.
3. **Default compression per library.** clickhouse-async ships LZ4-on
   when the `[compression]` extra is installed (it is in the bench
   venv); clickhouse-connect's HTTP transport defaults to LZ4; asynch
   is off by default for both versions. We do **not** override per
   library — those defaults are what users land on.
4. **Localhost Docker, warm cache.** Single-host eliminates network
   jitter; warm-cache reads are page-cache served. Real-world WAN
   latency narrows the native-vs-HTTP gap, and cold-cache reads are
   disk-bound — both shift relative numbers.
5. **Single-host pools.** Multi-host failover and replica fan-out are
   features only `clickhouse-async` and `asynch` support. Out of scope
   here — they'd change the question from "which is faster" to "which
   has what features".
6. **asynch insert quirk.** Both PyPI `asynch` 0.3.1 and the tacto
   fork ship a `process_insert_query` that does not drain to
   `END_OF_STREAM`. The benchmark adapter (`adapters/asynch_adapter.py`)
   manually drains after every INSERT so the connection stays usable.
   Without that workaround the second insert raises
   `ProgrammingError`. The drain cost is small but real and is
   reflected in the numbers.
7. **Type-system breadth is qualitative.** JSON / Variant / Dynamic /
   Map / Tuple / AggregateFunction support varies by library and is a
   real deciding factor — but speed alone misleads if you can't even
   read the column. Consult each library's docs for type coverage in
   addition to these speed numbers.

### Operator discipline (not script-enforced)

- Run on AC power.
- Close other heavy workloads (browsers, IDEs indexing, Docker builds).
- Allow the laptop to reach steady-state thermals — no benchmarks 30 s
  after unsuspending.
- The memory KPI is now sampled in-process via `psutil` — no system
  `time(1)` dependency, no `brew install gnu-time` needed.

## Refreshing the baseline (intentional version bumps)

`asynch` (PyPI), the tacto fork, and `clickhouse-connect` are
**pinned** in `pyproject.toml`; `uv.lock` is committed. To
intentionally bump:

```bash
cd benchmarks
uv lock --upgrade-package asynch --upgrade-package clickhouse-connect
./run.sh                           # re-collect under the new versions
git add pyproject.toml uv.lock results/
git commit -m "bump: refresh benchmark baseline"
```

`pyproject.toml` next to each pin names the version verbatim so a diff
makes the change legible.

## CI policy

These benchmarks **do not** run in CI. Timing on shared runners is
flaky enough that the signal is dominated by noise, and a flaky CI is
worse than a manual one. Maintainers run the suite locally before
releases or after performance-sensitive changes.

## Layout

```
benchmarks/
  pyproject.toml          # base + one extra per library (5 labels: ca / asynch_pypi / asynch_tacto / cc / cc_async)
  run.sh                  # orchestrator — handles the asynch flip
  adapters/               # one module per library, uniform async interface
  scenarios/              # one module per KPI
  report.py               # raw.jsonl → report.md + PNGs
  results/                # outputs (gitignored except .gitkeep)
```

## Adding a new library or scenario

- **New library**: add an extra to `pyproject.toml`, an
  `adapters/<x>_adapter.py` implementing the `Adapter` /
  `AdapterClient` Protocol from `adapters/base.py`, register the
  short name in `adapters/__init__.py::get_adapter` and
  `scenarios/common.py::library_label`, append the short name to
  `ALL_LIBS` and `extras_for` in `run.sh`, and add a label to
  `_LIB_ORDER` / `_LIB_COLOURS` in `report.py`.
- **New scenario**: add `scenarios/<x>.py` following the shape of the
  existing modules (use `common.base_arg_parser` and `common.emit`),
  wire it into `run_all_scenarios_for_lib` in `run.sh`, and add
  metadata to `_SCENARIO_META` in `report.py` so the table and chart
  render.
