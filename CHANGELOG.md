# Changelog

All notable changes to `clickhouse-async`. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [SemVer](https://semver.org/).

## [0.2.0] — 2026-05-04

Theme: **type-system completeness + production polish.** After this
release a `SELECT * FROM system.columns` on a stock ClickHouse 24.8
install parses every column type without an `"unknown type"` error.

### Added

- **`DateTime64` full sub-second precision.** Precisions 7–9 now
  round-trip at the correct nanosecond / 100-nanosecond scale instead
  of silently truncating to microseconds. `DateTime` respects the
  connection's session timezone for encode/decode rather than pinning
  to UTC.
- **INSERT header validation.** `Client.insert` verifies the
  column-name list against the header `DATA` block the server echoes
  back and surfaces a clear `ValueError` on mismatch. The server's
  `Progress.written_rows` counter is now returned as
  `QueryResult.written_rows` (previously always zero).
- **`Nested(name T, …)` type.** Reads and writes as a
  `list[dict[str, Any]]` — each dict is one nested row. De-sugars
  to `Array(Tuple(…))` on the wire.
- **Geo type aliases.** `Point`, `Ring`, `Polygon`, and
  `MultiPolygon` are now registered codec aliases on top of the
  existing tuple/array codecs. A `SELECT * FROM system.columns` no
  longer raises on any of the four geo shapes.
- **`AggregateFunction` state columns.** `AggregateFunction(fn, T)`
  round-trips the opaque state bytes. Per-row state readers ship for
  `avg` and `count`; other functions pass bytes through and raise
  `NotImplementedError` on state access (adding a function is a
  one-line registration).
- **`Variant(T1, T2, …)` and `Dynamic`.** `Variant` decodes each
  row to the matching Python type for its discriminator; `Dynamic`
  reads the type tag per value and delegates to the appropriate codec.
  Writes are supported (Python value → best-fit variant slot).
- **`JSON` read/write.** Full round-trip against ClickHouse 24.8:
  reads produce `dict[str, Any]` keyed by dotted path; writes accept
  the same shape. The shared-data substream is read but its values are
  merged into the per-row dict at the codec level (paths that spill
  past `max_dynamic_paths` are silently dropped on write — see
  `TODO.md`).

### Changed

- **Connection transport hardening.** `Connection` now propagates
  OS-level socket errors (broken pipe, connection reset) as structured
  `BROKEN` state transitions instead of propagating raw `OSError`
  through the packet loop. The pool's `verify_or_discard` path
  correctly disposes of stale connections opened before a server
  restart.

### Tooling

- **Coverage floor.** `pytest-cov` is now a dev dependency.
  `[tool.coverage.run]` enables branch coverage; `[tool.coverage.report]`
  sets `fail_under = 94`. The `unit-bare` CI job enforces the floor on
  every push and uploads an HTML artifact on Python 3.11.
  `./scripts/coverage.sh` runs the same check locally and opens the
  HTML report.
- **Integration resilience tests.** `tests/integration/test_resilience.py`
  uses a TCP proxy to simulate FIN (graceful close) and RST (hard
  reset) mid-query and post-query; verifies the pool reconnects cleanly.

### Fixed

- `DateTime` codec no longer pins to UTC when the server negotiates a
  session timezone via `TimezoneUpdate` — the connection now threads
  `session_timezone` through every block read path.
- `LowCardinality(Nullable(T))` null entries no longer corrupt the
  index on subsequent rows in the same block.

### Documentation

- `TODO.md` §1 drops the DateTime64 precision and naive-UTC entries;
  §2 drops the type-system entries for `Nested`, geo aliases,
  `AggregateFunction`, `Variant`, `Dynamic`, `JSON`, and the INSERT
  read-receipt entry — all landed.
- `CONTRIBUTING.md` gains a "Coverage" section.
- `.plans/` index marks all eight v0.2 plans as landed.

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

## v0.3.1 (2026-05-04)

### Feat

- **connection**: add connect_timeout parameter to Connection, Client, and Pool
- bump OUR_REVISION to 54483, add ClickHouse 26.3 LTS support

### Fix

- **protocol**: decompress TABLE_COLUMNS body at revision >= 54481
- **ci**: prevent integration test hang on connect and in Docker asyncio
- **tests**: replace system.numbers() table-function calls with system.numbers LIMIT n

## v0.3.0 (2026-05-04)

### Feat

- **v0.4-05**: add example scenario tests against real public datasets
- **v0.3-04**: enable LZ4 compression by default when extra is installed
- **v0.3-03**: JSON ergonomics — nested-dict mode and shared-data overflow
- **v0.3-02**: add column_factories hook for columnar surface
- **v0.3-01**: add columnar retrieval surface

### Refactor

- extract _nest/_flatten to _json_helpers.py

## v0.2.0 (2026-05-04)

### Feat

- full JSON read/write + Dynamic alphabetical-sort fix (v0.2 plan 07)
- JSON parser-only stub + Dynamic V1 wire format (v0.2 plan 07)
- Variant + Dynamic types (v0.2 plan 06)
- AggregateFunction state columns (v0.2 plan 05)
- geo aliases (Point, Ring, Polygon, MultiPolygon) (v0.2 plan 04)
- Nested(name T, ...) type (v0.2 plan 03)
- INSERT header validation + server-confirmed written_rows (v0.2 plan 02)
- DateTime64 full precision + session timezone plumbing (v0.2 plan 01)

### Fix

- harden Connection against transport-level failures + add resilience tests

### Refactor

- extract parse_type + registry into types/_parser.py

## v0.1.0 (2026-05-03)

### Feat

- Tuple named-field syntax (plan 05) + v0.1 release prep
- LowCardinality(Nullable(T)) + fix v0 LC wire format (plan 04)
- cross-connection query cancel via kill_query (v0.1 plan 03)
- **pool**: make the idle reaper opt-out via enable_reaper
- **pool**: idle reaper + min_size warm (v0.1 plan 02)
- multi-host DSN with failover (v0.1 plan 01)
- **tests**: integration suite + handshake / parameter wire fixes
- **pool**: health checks, lifetime cap, pass-through one-shots (08b)
- **pool**: lazy-fill bounded pool with FIFO acquire (08a)
- **client**: insert with sync + async row sources (07d)
- **client**: iter_blocks / iter_rows with cancel-on-break (07c)
- **client**: execute / fetch_all / fetch_one + QueryResult (07b)
- **client**: DSN parser + Client lifecycle (07a)
- **connection**: LZ4 / ZSTD compressed-frame support (06h)
- **connection**: cooperative cancel + bounded drain (06g)
- **connection**: server-side query parameters (06f)
- **connection**: send_data for the INSERT data path (06e)
- **connection**: full packet-loop dispatch + callback hooks (06d)
- **connection**: Query packet + minimal SELECT round-trip (06c)
- **connection**: Hello handshake + ServerInfo
- **connection**: state machine + lifecycle skeleton
- **protocol**: add Block read/write
- **types**: add Enum8/Enum16 and LowCardinality(T)
- **types**: add Array, Tuple, Map composite codecs
- **types**: add UUID, IPv4, IPv6 codecs
- **types**: full primitive matrix, decimals, dates, datetimes
- **types**: scaffold column codec system with Int32, String, Nullable
- **protocol**: define packet ids and revision gates
- **protocol**: add async binary I/O primitives
- migrate to `uv` (#3)
- Add first protocol implementations and integration tests (#1)

### Fix

- **ci**: drop listen-host hack; wait for real server's Ready line
- **ci**: grep server log file, not docker logs
- **ci**: wait for real server's 'Ready for connections' log line
- **ci**: listen_host=0.0.0.0 + rotation no-failure short-clock fix
- **ci**: pin integration tests to 127.0.0.1 + dump server logs
- **scripts**: probe host-side TCP after in-container readiness
- **ci**: drop nonexistent --no-extras flag from sync invocations
