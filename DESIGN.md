# clickhouse-async — Design Proposal

A pure-Python, fully `asyncio`-native client for ClickHouse over the **native TCP
protocol** (default port `9000` / `9440` for TLS).

This document describes the design through v0.2 (shipped) and v0.3 (in
progress): the client, the connection pool, the layers underneath, and the
trajectory the type-system buildout is following. It is deliberately scoped —
the goal is to ship a small, correct, idiomatic async client first, and grow
features behind a stable surface. Anything tagged "v0" below describes the
constraints that informed the original build; anything tagged "v0.x" is the
current state.

---

## 1. Goals and non-goals

### Goals
- **Native protocol only.** Talk directly to ClickHouse on `:9000`. The native
  protocol is binary, columnar, supports server-side progress, profile info,
  query cancellation, and is materially faster than HTTP for large result sets.
- **Truly async.** Use `asyncio` end-to-end. No threads, no `run_in_executor`
  hops on the hot path, no sync I/O hiding inside an `async def`.
- **Small, ergonomic surface.** A `Client` for one-shot or single-session use
  and a `Pool` for production workloads — both with `async with` semantics.
- **Streaming by default.** Results are returned block-by-block; large queries
  should not require loading the whole resultset into memory.
- **Correctness over feature breadth.** Handshake, query, insert, cancel,
  errors, progress, and the common scalar/composite types. Niche server
  features can wait until v0 is proven solid.
- **Typed and strict.** `ty` clean, public API fully annotated.

### Non-goals (for v0)
- The HTTP interface (`:8123`). A separate transport could be added later, but
  the native protocol is the focus.
- ORM-style query building. We expose parameterised SQL, not a query DSL.
- Distributed-table awareness, sharding logic, or routing — the server handles
  that. The client is "dumb pipe + types".
- Drop-in compatibility with `clickhouse-driver` or `asynch`. We will borrow
  good ideas, but the API is designed fresh for `asyncio`.
- A C extension. Pure Python first; we can profile and add a Cython/Rust hot
  loop later if blocks become CPU-bound.

---

## 2. Architecture at a glance

Five layers, each unit-testable in isolation:

```
                ┌──────────────────────────────────────────┐
   public API   │  Pool                                    │
                │   └─ acquire() ──► Client                │
                │                     ├─ execute()         │
                │                     ├─ iter_blocks()     │
                │                     └─ insert()          │
                └──────────────────────────────────────────┘
                ┌──────────────────────────────────────────┐
   session      │  Connection (handshake, server caps,     │
                │  per-query state machine, cancel token)  │
                └──────────────────────────────────────────┘
                ┌──────────────────────────────────────────┐
   framing      │  Packet codec (client/server packet ids, │
                │  varint, length-prefixed strings, blocks)│
                └──────────────────────────────────────────┘
                ┌──────────────────────────────────────────┐
   types        │  Column codecs (Int*, Float*, String,    │
                │  FixedString, Date*, Decimal, UUID, IPv*,│
                │  Array, Tuple, Map, Nullable, LowCard.,  │
                │  Enum, Bool)                             │
                └──────────────────────────────────────────┘
                ┌──────────────────────────────────────────┐
   transport    │  asyncio Stream{Reader,Writer} +         │
                │  optional LZ4/ZSTD compression frame     │
                └──────────────────────────────────────────┘
```

The split matters because the wire protocol mixes three concerns —
packet framing, per-column type encoding, and the request/response state
machine — that other drivers tangle together. Keeping them separate lets us
fuzz-test the codec without a server, swap the transport (TLS, mock, in-memory
loopback) without touching types, and reason about cancellation in one place.

---

## 3. Wire protocol notes (what v0 must implement)

The native protocol is documented in the ClickHouse source tree (mainly
`src/Client/Connection.cpp`, `src/IO/`, `src/Core/Protocol.h`). The minimum
surface for a working client:

**Client → server packets**
- `Hello` — protocol version, client name/version, default database, user,
  password.
- `Query` — query id, settings, stage, compression flag, SQL string, optional
  parameters, optional inline data block (for `INSERT ... VALUES`).
- `Data` — block payload (for `INSERT`).
- `Cancel` — cooperative query cancellation.
- `Ping` — pool keepalive / liveness probe.

**Server → client packets**
- `Hello` — server name, version, server timezone, display name, revision.
- `Data` — a result block.
- `Exception` — server-side error chain.
- `Progress` — incremental rows/bytes read on the server side.
- `Pong`, `EndOfStream`, `ProfileInfo`, `TableColumns`, `ProfileEvents`.

A few protocol invariants we will encode in types and assert at runtime:
- The server can interleave `Progress`, `ProfileInfo`, and `Data` packets while
  a query is executing. A connection in "query in flight" state must keep
  reading until `EndOfStream` or `Exception`.
- `Exception` is terminal — the connection is reusable, but the query is over.
- Compression, when negotiated, is **per block** (not per stream): each
  compressed block carries its own CityHash128 checksum. We honour that.

We will pin a known-good protocol revision (e.g. the one shipped by ClickHouse
24.x) and grow forward from there. Older servers fall back to a lower revision
during the `Hello` exchange.

---

## 4. Public API — `Client`

The client is the unit of one logical session: one TCP connection, one
in-flight query at a time. It is an `async` context manager.

```python
import clickhouse_async as ch

async with ch.connect("clickhouse://default:@localhost:9000/default") as client:
    rows = await client.fetch_all("SELECT number FROM system.numbers LIMIT 10")

    async for block in client.iter_blocks(
        "SELECT * FROM events WHERE day = {d:Date}",
        params={"d": "2026-05-01"},
    ):
        process(block)

    await client.insert(
        "INSERT INTO events (id, ts, payload) VALUES",
        rows=rows_iter,             # iterable or async iterable of tuples
        column_names=("id", "ts", "payload"),
    )
```

### Surface

```python
class Client:
    # lifecycle
    async def __aenter__(self) -> "Client": ...
    async def __aexit__(self, *exc) -> None: ...
    async def close(self) -> None: ...
    async def ping(self) -> None: ...

    # queries returning rows
    async def execute(self, sql: str, *, params: Mapping[str, Any] | None = None,
                      settings: Mapping[str, Any] | None = None,
                      query_id: str | None = None) -> QueryResult: ...
    async def fetch_all(self, sql: str, **kw) -> list[tuple]: ...
    async def fetch_one(self, sql: str, **kw) -> tuple | None: ...
    def iter_blocks(self, sql: str, **kw) -> AsyncIterator[Block]: ...
    def iter_rows(self, sql: str, **kw)   -> AsyncIterator[tuple]: ...

    # inserts
    async def insert(self, sql: str, *, rows: Iterable | AsyncIterable,
                     column_names: Sequence[str],
                     column_types: Sequence[str] | None = None) -> int: ...

    # introspection
    @property
    def server_info(self) -> ServerInfo: ...
    @property
    def in_transaction(self) -> bool: ...   # always False for v0
```

### Key choices

- **One in-flight query per client.** The native protocol does not multiplex.
  Concurrent calls on the same `Client` raise `ConcurrentQueryError`; users
  who want concurrency use a `Pool`. This keeps the state machine simple and
  makes cancellation correct.
- **Parameters are server-side bound.** We use ClickHouse's `{name:Type}`
  parameter syntax and the `Query` packet's parameter table; we do **not**
  do client-side string interpolation. This eliminates an entire category of
  injection bugs and matches how the server expects modern drivers to behave.
- **Streaming returns are the default.** `iter_blocks` / `iter_rows` are
  `AsyncIterator`s, and `fetch_all` is just `[r async for r in iter_rows(...)]`
  with a memory cap. A `max_rows` setting on the client stops accidental
  unbounded loads.
- **Inserts accept any iterable.** Sync iterables are fine; async iterables
  are awaited transparently. Rows are batched into blocks of a configurable
  size (`insert_block_size`, default 65 536) before being sent.
- **`QueryResult` is small.** It carries column metadata, row count, server
  progress, and a buffer of blocks if the query was non-streaming. It does
  **not** hold a reference to the connection — once `execute` returns, the
  conn is idle and pool-returnable.

### Cancellation

- An `asyncio.CancelledError` raised in user code while iterating a result
  triggers a `Cancel` packet, then drains the connection until `EndOfStream`
  with a bounded timeout. If draining exceeds the timeout, we close the
  socket and mark the connection bad — the pool will discard it.
- A per-query `timeout` parameter installs an `asyncio.timeout()` around
  the same logic.
- We never silently swallow a cancel. The cancel either returns control to
  the caller or surfaces a `QueryCancellationError` describing what we did.

---

## 5. Public API — `Pool`

The pool is what production code should use. It owns a fixed-shape population
of connections and hands them out under `async with`.

```python
async with ch.create_pool(
    "clickhouse://default:@localhost:9000/default",
    min_size=4,
    max_size=32,
    max_idle_time=60.0,
    max_lifetime=600.0,
    acquire_timeout=5.0,
) as pool:
    async with pool.acquire() as client:
        await client.fetch_all("SELECT 1")

    # convenience pass-through, acquires + releases for one call
    rows = await pool.fetch_all("SELECT 1")
```

### Semantics
- **Bounded.** `max_size` is a hard cap; `acquire()` waits up to
  `acquire_timeout` for a slot, then raises `PoolTimeoutError`.
- **Min-size warm.** The reaper opens connections up to `min_size`
  on the next pass after first use — never eagerly at `create_pool`
  time, since that would turn a misconfigured DSN into an
  import-time failure. Opt out via `enable_reaper=False` (see below).
- **Idle reaper.** A background task lazily started on first
  `acquire()` evicts connections whose idle time exceeds
  `max_idle_time` while keeping the pool above `min_size`. Cancelled
  and awaited in `pool.close()`. Set `enable_reaper=False` to
  disable; that path also forbids `min_size > 0` since the reaper
  is what enforces it.
- **Lifetime cap.** `max_lifetime` forces recycle of long-lived connections,
  which dodges server-side `max_session_timeout` surprises and helps DNS
  changes propagate.
- **Health checks on acquire.** A connection borrowed from the pool gets a
  cheap `Ping` if it has been idle longer than `health_check_after`. Failed
  pings drop the connection and we transparently open a fresh one — but
  only on `acquire`, never mid-query.
- **Fairness.** A FIFO waiter queue, implemented as a `deque` plus
  `asyncio.Condition`. The Condition gives the reaper a way to scan
  free entries by `last_returned_at` without consuming them, which
  `asyncio.Queue` couldn't.
- **Multi-host failover.** DSNs may list comma-separated host
  candidates; `Connection.open` walks them in order, and `Pool`
  rotates the start position per acquire (`_HostRotation`) with a
  per-host failure cooldown (`host_failover_cooldown`, default 5 s).
  Read-replica vs primary routing and health-aware load balancing
  (least-conns, RTT) are still roadmap.
- **Cross-connection cancel.** `Pool.kill_query(query_id)` opens a
  fresh side-channel connection through the rotation and issues
  `KILL QUERY WHERE query_id = …`. Returns the server-confirmed
  killed-row count; defaults to `SYNC`.
- **Connection identity.** The pool returns `Client` instances; the user
  never sees a raw `Connection`. On release we reset transient session state
  (current database, query settings) so the next borrower gets a clean slate.

### What the pool deliberately does NOT do
- **No automatic query retry.** ClickHouse INSERTs are not idempotent and
  SELECTs may be expensive; silently re-running them is a footgun. Retry is
  the caller's choice. Connection-level reconnection on acquire is fine —
  that is invisible and safe.
- **No multiplexing.** One connection, one query at a time. If you need 32
  parallel queries, use a pool of 32.

---

## 6. Connection lifecycle

A `Connection` is a small state machine:

```
   IDLE ──open()──► CONNECTING ──hello/handshake──► READY
     ▲                                                │
     │                                          send_query()
     │                                                ▼
     │                                        QUERY_IN_FLIGHT
     │                                                │
     │             ┌───── EndOfStream ────────────────┤
     └─────────────┤                                  │
                   ├───── Exception ──────────────────┤
                   │                                  │
                   └───── Cancel + drain ─────────────┘
                                  │
                                  ▼ (on protocol error / timeout)
                              BROKEN ──close()──► CLOSED
```

Invariants enforced in code:
- A connection in `BROKEN` is never returned to the pool — it is closed and
  replaced.
- `send_query` requires `READY`; calling it from `QUERY_IN_FLIGHT` is a
  `ConcurrentQueryError`, not a hang.
- All transitions log the reason; protocol errors include the last few
  packet headers seen. This makes the next bug bisectable.

---

## 7. Type system and blocks

A `Block` is a columnar batch: `{column_name: (type, values)}` plus a row
count. Each `ColumnCodec` knows how to read and write its values to a binary
buffer given a row count.

### Types shipped (through v0.1)
- **Numeric:** `Int8/16/32/64/128/256`, `UInt8/16/32/64/128/256`,
  `Float32/64`, `Bool`.
- **Decimal:** `Decimal32/64/128/256` with scale.
- **String:** `String`, `FixedString(N)`.
- **Time:** `Date`, `Date32`, `DateTime` (with optional timezone),
  `DateTime64(precision[, tz])`. The connection's session timezone is
  plumbed in as a fallback for bare `DateTime` / `DateTime64`, so naive
  reads land in the server's session zone instead of silently UTC.
  `DateTime64(p > 6)` returns `HighPrecisionTimestamp(ticks, scale)`
  by default so nanosecond ticks survive Python's microsecond
  ceiling; pass `high_precision=False` to opt back into the lossy
  `datetime` shape.
- **Net/UUID:** `UUID`, `IPv4`, `IPv6`.
- **Composite:** `Array(T)`, `Tuple(T...)` (named **and** unnamed),
  `Map(K, V)`, `Nullable(T)`, `LowCardinality(T)` and
  `LowCardinality(Nullable(T))`, `Enum8/16`.

### Added in v0.2
- `AggregateFunction(fn, T)` — per-row state bytes; readers for `avg`
  and `count`; other functions pass through and raise on state access.
- `Nested(name T, …)` — reads/writes as `list[dict[str, Any]]`.
- Geo aliases: `Point`, `Ring`, `Polygon`, `MultiPolygon`.
- `Variant(T1, …)` and `Dynamic(max_types=N)` — tagged-union types.
- `JSON` (ClickHouse 24.x) — full read/write; flat dotted-path `dict`.
  Shared-data substream is decoded on read but always written empty
  (overflow paths are dropped) — fixed in v0.3 plan 03.

### Planned for v0.3
- **`column_factories` hook.** Per-type override for the Python
  construction of each column (e.g. `"UInt64": numpy.array`). Applied
  after codec decode in the columnar retrieval path. Foundations for
  adapter packages.

### Python representation
- Sane defaults: `int`, `float`, `Decimal`, `str`, `bytes`, `datetime`,
  `date`, `uuid.UUID`, `ipaddress.IPv4Address`/`IPv6Address`, `list`, `tuple`,
  `dict`, `None`.
- A `column_factories` hook lets users override per-type construction
  (e.g. polars/pyarrow/numpy adapters in a separate package later).

### Encoding strategy
Each codec is an object with `read(buf, n_rows) -> Sequence[T]` and
`write(buf, values) -> None`. They compose: `Nullable(Array(String))` is just
three codecs stacked. Buffers are `memoryview`-backed to avoid copies on the
read path.

---

## 8. Errors

A flat hierarchy under `clickhouse_async.errors`:

```
ClickHouseError
├── ConnectionError              (network / DNS / TLS)
├── ProtocolError                (we got a packet we cannot make sense of)
├── ServerError                  (Exception packet from the server)
│     attributes: code, name, display_text, stack_trace, nested
├── QueryCancellationError
├── ConcurrentQueryError
├── PoolError
│     ├── PoolTimeoutError
│     └── PoolClosedError
└── TypeError                    (round-trip / column-codec mismatch)
```

`ServerError.code` is the canonical numeric code; we will not invent our own.

---

## 9. Compression and TLS

- **Compression:** opt-in via DSN/option. v0 supports **LZ4** (fast, the
  default in upstream drivers) and **ZSTD**. The compressed-block framing
  with CityHash128 checksums is implemented in the codec layer, so the
  connection layer is unaware. Disabled by default until benchmarked.
- **TLS:** standard `ssl.SSLContext` plumbed through `asyncio.open_connection`.
  No custom certificate handling — users hand us a context.

---

## 10. Concurrency model

- The connection runs on a single asyncio task per call. There is no internal
  reader task pumping packets into a queue; `execute` and `iter_blocks` read
  packets directly. This avoids the "who owns the buffer when the user
  cancels" problem that bites every driver that adds a reader task.
- Cancellation safety is the single hardest property to get right, and we
  test it explicitly: every public coroutine has a test that cancels it at
  every `await` point and asserts the connection is either reusable or
  cleanly broken — never half-broken.
- The pool is the only place we use background tasks (idle reaper). It is
  cancelled and awaited on `pool.close()`.

---

## 11. Testing strategy

Three tiers:

1. **Codec unit tests.** Hex-fixture round-trips for every type: encode known
   value → expected bytes; decode expected bytes → known value. These are
   fast, deterministic, and catch most regressions.
2. **Protocol unit tests.** Drive `Connection` against an in-memory mock
   transport that scripts server packets. Verifies the state machine,
   cancellation, error handling, and progress callback delivery without
   needing Docker.
3. **Integration tests.** Spin up ClickHouse via `testcontainers` with a
   project-local `ClickHouseContainer(DockerContainer)` subclass that pins
   the image, exposes `:9000` and `:8123`, and surfaces helpers for log
   tailing and exec'ing into the container while debugging. A pytest flag
   `--localdb` skips the container and points the suite at a developer's
   local server (`--localdb` alone uses
   `clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse`;
   `--localdb=clickhouse://...` overrides the DSN). Integration tests are
   marked with `@pytest.mark.integration` so unit runs stay fast.

CI matrix: Python 3.11/3.12/3.13, ClickHouse stable + latest LTS.

---

## 12. Repository layout

```
clickhouse_async/
    __init__.py          # connect(), create_pool(), version, public re-exports
    client.py            # Client, QueryResult, ColumnarResult (v0.3)
    pool.py              # Pool
    connection.py        # Connection state machine
    dsn.py               # connection-string parsing
    errors.py
    _host_rotation.py    # round-robin + per-host failure cooldown
    protocol/
        packets.py       # packet ids, protocol revision constants
        block.py         # Block, Column, read_block, write_block
        io.py            # AsyncBinaryReader, BinaryWriter
        handshake.py     # client Hello / server Hello codec
        query_packet.py  # Query packet + ClientInfo
        compression.py   # LZ4 / ZSTD compressed-block framing
        server_packets.py
        parameters.py    # {name:Type} parameter formatting
        exception_packet.py
    types/
        __init__.py      # registry: name → codec; parse_type()
        _parser.py       # type-spec string parser
        base.py          # ColumnCodec Protocol
        primitive.py     # Int*, UInt*, Float*, Bool
        decimal.py
        datetime.py      # Date, Date32, DateTime, DateTime64, HighPrecisionTimestamp
        string.py        # String, FixedString
        composite.py     # Array, Tuple, Map, Nullable, LowCardinality, Enum
        net.py           # UUID, IPv4, IPv6
        aggregate.py     # AggregateFunction
        enums.py         # Enum8, Enum16
        geo.py           # Point, Ring, Polygon, MultiPolygon aliases
        variant.py       # Variant, Dynamic
        json_type.py     # JSON (ClickHouse 24.x)
tests/
    unit/
    integration/
    containers/          # project-local ClickHouseContainer subclass
```

---

## 13. Roadmap

### Landed in v0.1

1. **Multi-host DSN with failover.** Comma-separated candidate list;
   `Connection.open` walks them in order; `Pool` rotates the start
   position with a per-host failure cooldown.
2. **Pool idle reaper + `min_size` warm.** Background task closes
   idle connections past `max_idle_time` while keeping the pool above
   `min_size`; opt out via `enable_reaper=False`.
3. **Cross-connection query cancel.** `Pool.kill_query(query_id)` and
   `Client.kill_query(query_id)` open a fresh side-channel connection
   to issue `KILL QUERY WHERE query_id = …`.
4. **`LowCardinality(Nullable(T))` and `Tuple(name T, …)`.** The two
   real-world type shapes v0 rejected; both round-trip end-to-end.

### v0.2 (shipped — 2026-05-04)

1. **Type-system completeness.** `AggregateFunction(fn, T)`, `Nested`,
   geo aliases (`Point`, `Ring`, `Polygon`, `MultiPolygon`),
   `Variant(T1, …)`, `Dynamic(max_types=N)`, `JSON` (ClickHouse 24.x).
2. **`DateTime64(7..9)` + session timezone.** Nanosecond precision via
   `HighPrecisionTimestamp`; bare `DateTime` honours the server's
   session timezone.
3. **INSERT block-header validation + server-confirmed `written_rows`.**
   Schema typo fails fast with a named-column diagnostic.
4. **Connection transport hardening.** OS-level socket errors propagate
   as structured `BROKEN` transitions; pool `verify_or_discard` handles
   stale connections.
5. **Test coverage floor.** `pytest-cov` + `branch=true`; CI enforces
   ≥ 94 % branch coverage; current suite sits at 95.1 %.

### v0.3 (in progress)

1. **Column-major retrieval surface.** `Client.fetch_columns(sql) ->
   ColumnarResult` and `Client.iter_column_blocks(sql)` avoid the
   per-row tuple transpose. `Pool` gets the same pass-throughs.
2. **`column_factories` hook.** `column_factories: dict[str, Callable]`
   kwarg on `connect()` / `create_pool()` lets callers replace the
   default `list` with any type (numpy, polars, pyarrow) per column.
   Applied post-decode in the columnar path; row-major paths unaffected.
3. **JSON ergonomics.** `json_nested=True` mode reconstructs `{"user":
   {"id": 7}}` from dotted-path keys; write accepts both flat and
   nested inputs. Shared-data write substream correctly populated for
   overflow paths (closes the v0.2 caveat).
4. **Compression default on.** LZ4 is the default when the
   `[compression]` extra is installed; `CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION=off`
   env var and `compression=None` kwarg opt out.

### v0.4+

1. **`pyarrow` / `polars` adapter packages** (`clickhouse-async-arrow` /
   `-polars`). Separate extras; build on the `column_factories` hook
   from v0.3. Each block becomes an Arrow `RecordBatch` / Polars
   `DataFrame` with no row-tuple intermediate.
2. **Read-only / write-only pool routing.** Primary-only writes,
   replica-fanout reads — builds on multi-host DSN.
3. **`AggregateFunction` allow-list expansion.** Adding `quantile`,
   `uniq`, etc. is a one-line registration in `aggregate.py`; deferred
   until a real workload drives priority.

### v1

1. **OpenTelemetry instrumentation** around `execute` / `acquire` /
   packet send/receive, gated behind an extra so the bare install stays
   import-clean. Pinned to v1 so span shapes don't churn while the API
   is still settling.

---

## 14. Open questions

- **Parameter binding fallback.** *Resolved (v0):* on too-old servers
  we refuse with `UnsupportedFeatureError` rather than silently
  emitting client-side substitution. Silent fallback would undermine
  the safety claim of server-bound parameters.
- **Block-as-DataFrame.** *Resolved (v0.3+):* `.to_arrow()` /
  `.to_polars()` live in extras packages
  (`clickhouse-async-arrow` / `-polars`) wrapping a forthcoming
  column-major retrieval surface (`Client.fetch_columns` /
  `iter_column_blocks`), not core. Core stays lean.
- **Default compression.** *Still open.* LZ4 is essentially free
  CPU-wise and saves a lot of bytes on large result sets. Default
  stays off until a benchmark suite proves the trade-off on
  multi-block payloads; revisit in v0.3.
