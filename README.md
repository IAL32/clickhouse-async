# clickhouse-async

An async Python client for ClickHouse that speaks the native TCP protocol on
port `9000` (or `9440` for TLS).

> **Status:** pre-v0. The codebase is intentionally empty while the API and
> conventions land. Not yet on PyPI.

## Why another ClickHouse client?

- **Native protocol, not HTTP.** The binary protocol on `:9000` is columnar,
  supports streaming, server-side progress, and query cancellation. It's
  also what most non-Python ClickHouse clients use.
- **Async end-to-end.** Built on `asyncio`. No threads, no `run_in_executor`
  hops on the hot path, no sync I/O hidden inside an `async def`.
- **Streaming by default.** Results arrive block by block — large queries
  do not require loading the full resultset into memory.
- **Small surface.** A `Client` for one-shot or single-session use and a
  `Pool` for production workloads, both with `async with` semantics.
- **Typed.** `ty`-clean, public API fully annotated.

## Installation

> Not yet published. Once released:

```bash
uv add clickhouse-async
# or
pip install clickhouse-async
```

Requires Python 3.11+.

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
) as pool:
    async with pool.acquire() as client:
        await client.execute("INSERT INTO events VALUES", rows=batch)

    # one-shot pass-through
    rows = await pool.fetch_all("SELECT count() FROM events")
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

## Development

### Setup

```bash
git clone https://github.com/IAL32/clickhouse-async
cd clickhouse-async
uv sync
```

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

### Project conventions

A few rules for v0:

- **Type checker is `ty`.** No `mypy`, no parallel mypy config.
- **Integration tests go through a project-local
  `ClickHouseContainer(DockerContainer)`** at `tests/containers/clickhouse.py`.
  No hand-rolled `docker run`, no `docker-compose.yml`. The subclass owns
  image pinning, port exposure, default credentials, and debug helpers.
- **`asyncio` end-to-end.** No threads on the hot path.
- **Canonical default DSN:**
  `clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse` — used by
  both the test container and `--localdb` defaults.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

[Apache 2.0](./LICENSE).
