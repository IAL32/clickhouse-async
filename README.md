# clickhouse-async

An async Python client for ClickHouse that speaks the native TCP protocol on
port `9000` (or `9440` for TLS).

> **Status:** v0.1. Native protocol, types, client, pool, multi-host
> failover, idle reaper, cross-connection cancel, and the most-common
> v0 type gaps (LowCardinality(Nullable(T)), named Tuples) all land here.
> Not yet on PyPI — installable via VCS until the first release artefact
> ships.

## Why another ClickHouse client?

- **Native protocol, not HTTP.** The binary protocol on `:9000` is columnar,
  supports streaming, server-side progress, and query cancellation. It's
  also what most non-Python ClickHouse clients use.
- **Async end-to-end.** Built on `asyncio`. No threads, no `run_in_executor`
  hops on the hot path, no sync I/O hidden inside an `async def`.
- **Streaming by default.** Results arrive block by block — large queries
  do not require loading the full resultset into memory.
- **Multi-host failover, built in.** A DSN can list comma-separated
  candidates; `Client.open()` walks them in order and `Pool` rotates
  the start position with a per-host cooldown so a single dead replica
  doesn't dominate.
- **Cross-connection cancel.** `Pool.kill_query(query_id)` opens a fresh
  side-channel connection so you can cancel a long-running query
  without touching the connection that issued it.
- **Small surface.** A `Client` for one-shot or single-session use and a
  `Pool` with idle reaper + min-size warm for production workloads,
  both with `async with` semantics.
- **Typed.** `ty`-clean, public API fully annotated.

## Installation

> Not yet published. Once released:

```bash
uv add clickhouse-async
# or
pip install clickhouse-async
```

Requires Python 3.11+.

Since v0.4 the package ships a small Rust extension (built with
[`maturin`](https://www.maturin.rs/) + [PyO3](https://pyo3.rs/)) for
the hot read paths. Installing from a published wheel needs no extra
tools; **building from source needs a Rust toolchain** (`cargo` 1.74+).

```bash
# install Rust if you don't have it (rustup is the official installer)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# then either pip will build automatically, or for development:
uv sync                  # installs deps including maturin
uv run maturin develop --uv   # build the Rust extension into the venv
```

## Quick start

### One-shot client

```python
import clickhouse_async as ch

async with ch.connect("clickhouse://default:@localhost:9000/default") as client:
    rows = await client.fetch_all("SELECT number FROM system.numbers LIMIT 10")
```

### Connection pool

```python
async with ch.create_pool(
    "clickhouse://default:@localhost:9000/default",
    min_size=4,
    max_size=32,
    max_idle_time=300.0,        # idle reaper closes entries past this
    idle_check_interval=30.0,   # reaper sweep period
) as pool:
    async with pool.acquire() as client:
        await client.execute("INSERT INTO events VALUES", rows=batch)

    # one-shot pass-through
    rows = await pool.fetch_all("SELECT count() FROM events")

    # cancel a query running on another connection
    await pool.kill_query("query-id-from-system.processes")
```

### Multi-host failover

A DSN can list comma-separated candidates. `Connection.open()` walks them
in order on each open; `Pool` rotates the start position across acquires
and cools-down hosts that just failed for `host_failover_cooldown` (default
5 s).

```python
async with ch.create_pool(
    "clickhouse://user:pass@replica-a:9000,replica-b:9000,replica-c/db",
    host_failover_cooldown=5.0,
) as pool:
    rows = await pool.fetch_all("SELECT 1")
```

### Streaming a large result

```python
async for block in client.iter_blocks(
    "SELECT * FROM events WHERE day = {d:Date}",
    params={"d": "2026-05-01"},
):
    process(block)
```

### Inserts

```python
await client.insert(
    "INSERT INTO events (id, ts, payload) VALUES",
    rows=rows_iter,             # iterable or async iterable of tuples
    column_names=("id", "ts", "payload"),
)
```

Parameters are bound server-side via ClickHouse's `{name:Type}` syntax,
not interpolated client-side.

## Compression

LZ4 compression is **on by default** when the `[compression]` extra is
installed. It costs almost nothing CPU-wise on modern hardware and cuts
wire bytes by 3–10× on typical numeric/columnar data.

```bash
pip install clickhouse-async[compression]   # or uv add clickhouse-async[compression]
```

Once installed, every connection uses LZ4 automatically. To opt out:

```python
from clickhouse_async.protocol.compression import CompressionMethod

# per-connection override
async with ch.connect("clickhouse://...", compression=CompressionMethod.NONE) as client:
    ...

# via DSN query string
async with ch.connect("clickhouse://host?compression=none") as client:
    ...
```

Set the environment variable `CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION=off` to
disable the auto-detect globally without changing code (useful when the
extra is installed but compression is undesired, e.g. during debugging).

ZSTD is also supported (`compression=CompressionMethod.ZSTD` /
`?compression=zstd`) for higher compression ratios at the cost of more CPU.

## Development

### Setup

```bash
git clone https://github.com/IAL32/clickhouse-async
cd clickhouse-async
uv sync
uv run maturin develop --uv       # build the Rust extension into the venv
```

The `maturin develop` step is required after every `git pull` or
edit under `src/` (the Rust crate). Pure-Python edits don't need a
rebuild.

### Day-to-day

```bash
uv run ruff check && uv run ruff format
uv run ty check
uv run pytest                       # unit tests only
uv run pytest tests/integration     # integration tests (starts a container)
```

### Running ClickHouse locally

A small wrapper around `docker run` is included for keeping a dev server
around between test runs:

```bash
./scripts/clickhouse.sh up           # start (uses the version in .clickhouse-version)
./scripts/clickhouse.sh up 25.3      # pin a specific version for this run
./scripts/clickhouse.sh status
./scripts/clickhouse.sh shell        # open clickhouse-client inside the container
./scripts/clickhouse.sh logs -f
./scripts/clickhouse.sh down
```

The default version lives in `.clickhouse-version` and is the single
source of truth shared with the testcontainers subclass.

### Integration tests against a local server

Integration tests start a ClickHouse container via `testcontainers` by
default. To skip the container and run against a server you already have
running locally (e.g. one started with the script above), use `--localdb`:

```bash
# default creds: clickhouse / clickhouse @ localhost:9000/clickhouse
uv run pytest tests/integration --localdb

# custom DSN
uv run pytest tests/integration --localdb=clickhouse://user:pass@host:9000/db
```

## Performance

A reproducible head-to-head benchmark suite against
[`asynch`](https://github.com/long2ice/asynch) and
[`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect)
lives under [`benchmarks/`](./benchmarks/). The committed
`benchmarks/results/report.md` is the reference baseline (M3 Pro,
ClickHouse 26.3) and shows the five KPIs the suite tracks: ping
latency, read throughput, insert throughput, concurrent reads, and
RSS-over-time during a 5M-row scan.

```bash
cd benchmarks
./run.sh                           # full run, ~5 min
./run.sh --quick                   # smaller dataset, ~1 min
```

The benchmarks pin specific versions of each library (see
`benchmarks/pyproject.toml`) so re-runs across machines are
comparable. The report's first section captures the host and
package versions for the run.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

[Apache 2.0](./LICENSE).
