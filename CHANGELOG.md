# Changelog

All notable changes to `clickhouse-async`. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [SemVer](https://semver.org/).

## [0.1.0] — 2026-05-03

The first tagged release. Picks up everything that landed during the v0
build-out plus the v0.1 hardening cycle (plans 01–05).

### Added

- **Native TCP protocol client.** Hello handshake, single-task packet
  loop, INSERT data path, server-side parameter binding via the
  `{name:Type}` placeholder syntax, and per-block LZ4 / ZSTD framing.
  HTTP transport on `:8123` is intentionally out of scope.
- **Type system.** `Int{8,16,32,64,128,256}`, `UInt*` mirror, `Float32/64`,
  `Bool`, `String`, `FixedString`, `Decimal{32,64,128,256}`,
  `Date`/`Date32`, `DateTime`, `DateTime64(precision[, tz])`, `UUID`,
  `IPv4`/`IPv6`, `Enum8`/`Enum16`, `Array`, `Tuple` (named **and**
  unnamed), `Map`, `Nullable`, `LowCardinality(T)` and
  `LowCardinality(Nullable(T))`.
- **`Client`** with `execute` / `fetch_all` / `fetch_one`,
  block-streaming `iter_blocks` / row-streaming `iter_rows`,
  `insert(rows=…)` (sync and async row sources), `ping`, and
  `kill_query`.
- **`Pool`** with lazy fill, FIFO acquire, bounded `max_size`,
  per-acquire health checks, per-release lifetime caps, the
  pass-through one-shots `execute` / `fetch_all` / `fetch_one`, and
  the new `kill_query`. Backed by a `deque` + `asyncio.Condition` so
  the background reaper can scan free entries by `last_returned_at`.
- **Pool idle reaper + `min_size` warm.** Background task sweeps every
  `idle_check_interval` (30 s default), closes entries idle past
  `max_idle_time` (5 min default) while keeping the population at
  ≥ `min_size`, and warms fresh connections back up after the close
  pass. Lazy on first `acquire()`; cancelled in `close()`. Opt out
  with `enable_reaper=False`.
- **Multi-host DSN with failover.** DSNs may list comma-separated
  hosts (`clickhouse://u:p@h1:9000,h2:9000,h3/db`). `Connection.open()`
  walks them in order; `Pool` rotates the start position per acquire
  and skips hosts in cooldown. A new `ConnectError` aggregates per-host
  failures when every candidate misses.
- **Cross-connection query cancel.** `Client.kill_query(query_id)` and
  `Pool.kill_query(query_id)` issue
  `KILL QUERY WHERE query_id = {qid:String}` over a side-channel
  connection. Defaults to `SYNC`; pass `sync=False` to fire-and-forget.

### Tooling

- **CI** runs lint + types + a Python 3.11 / 3.12 / 3.13 matrix of
  unit + integration tests in two install shapes (bare + compression
  extra).
- **Local-CI loop via `act`.** `./scripts/act.sh full` runs the full
  workflow inside a local Linux runner so contributors can iterate
  without burning CI round-trips. See [`CONTRIBUTING.md`][CONTRIBUTING]
  for the full setup.
- **`scripts/clickhouse.sh`** is the single way to keep a dev
  ClickHouse running between test runs. Reads the version from
  `.clickhouse-version`, mirrors the testcontainers fixture, and
  waits for the *real* server (not the entrypoint init phase) before
  reporting ready.
- **`prek`** wraps `pre-commit` for hook execution; the hooks
  themselves run `ruff check`, `ruff format --check`, and `ty check`
  via `uv run`.

### Documentation

- README covers the new multi-host DSN, idle reaper, and cross-conn
  cancel surface.
- TODO.md trims the v0 limitations and v0.2 type-system entries that
  v0.1 closed.
- `.plans/` index marks every numbered plan landed.

### Notes

- The bare-install footprint stays import-clean: `import
  clickhouse_async` does not require `lz4` / `zstandard` / cityhash.
  Those imports are lazy and live behind the `[lz4]`, `[zstd]`, and
  `[compression]` extras.

[CONTRIBUTING]: ./CONTRIBUTING.md
