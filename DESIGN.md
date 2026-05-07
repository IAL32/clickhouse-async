# clickhouse-async — Design Proposal

A pure-Python, fully `asyncio`-native client for ClickHouse over the **native TCP
protocol** (default port `9000` / `9440` for TLS).

This document describes the design through v0.2 (shipped), v0.3 (in
progress), and v0.4 (planned): the client, the connection pool, the layers underneath, and the
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
                      query_id: str | None = None,
                      timeout: float | None = None,
                      max_rows: int | None = None) -> QueryResult: ...
    async def fetch_all(self, sql: str, **kw) -> list[tuple]: ...
    async def fetch_one(self, sql: str, **kw) -> tuple | None: ...
    def iter_blocks(self, sql: str, **kw) -> AsyncIterator[Block]: ...
    def iter_rows(self, sql: str, **kw)   -> AsyncIterator[tuple]: ...

    # inserts
    async def insert(self, sql: str, *, rows: Iterable | AsyncIterable,
                     column_names: Sequence[str],
                     column_types: Sequence[str] | None = None,
                     timeout: float | None = None,
                     deduplication_token: str | None = None) -> int: ...

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
  with a memory cap. A `max_rows` parameter on `execute`/`fetch_all` raises
  `ResultTooLargeError` (subclass of `ClickHouseError`) before the process
  can OOM — **not yet implemented, tracked in §15**.
- **Inserts accept any iterable.** Sync iterables are fine; async iterables
  are awaited transparently. Rows are batched into blocks of a configurable
  size (`insert_block_size`, default 65 536) before being sent.
- **`QueryResult` is small.** It carries column metadata, row count, server
  progress, and a buffer of blocks if the query was non-streaming. It does
  **not** hold a reference to the connection — once `execute` returns, the
  conn is idle and pool-returnable.

### Cancellation and timeouts

- An `asyncio.CancelledError` raised in user code while iterating a result
  triggers a `Cancel` packet, then drains the connection until `EndOfStream`
  with a bounded timeout. If draining exceeds the timeout, we close the
  socket and mark the connection bad — the pool will discard it.
- A per-query `timeout: float | None` parameter wraps the entire
  packet-iteration loop with `asyncio.timeout()`, providing client-side
  enforcement independent of the server's `max_execution_time` setting.
  This protects against network partitions and hung server processes —
  **not yet implemented, tracked in §15**.
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
Each codec is an object with `read(reader, n_rows) -> Sequence[T]` and
`write(writer, values) -> None`. They compose: `Nullable(Array(String))` is
just three codecs stacked.

`read` is **synchronous** — it consumes from a `SyncBinaryReader` over an
in-memory bytes buffer rather than awaiting on a stream per primitive. The
async work happens at the transport boundary: `read_block_buffered` drains
either a generous initial socket chunk (uncompressed) or one compressed
frame at a time, hands the resulting buffer to the sync block parser, and
pushes any unconsumed bytes back to the async reader for the next packet.
On `BufferUnderflow` the parse retries from the start with a topped-up
buffer. Removing the per-row `await` is the single biggest factor in v0.4's
read-throughput jump; see §13 v0.4.

### `_fast_read` C extension

The `String` and `DateTime` read paths route through the `_fast_read`
extension. It exposes two pure functions:

- `decode_strings(buf, pos, n_rows) -> (list[str], int)` — walks
  varuint length + UTF-8 over the buffer in a tight C loop, calling
  `PyUnicode_DecodeUTF8` per row. Raises `BufferUnderflow` on a
  short buffer — same sentinel the rest of the read path raises, so
  the outer `read_block_buffered` retry loop is unaffected.
- `decode_datetime(buf, n_rows, tzinfo) -> list[datetime]` — naive
  case goes `gmtime_r` → `datetime(...)` directly, skipping the
  `fromtimestamp(ts, UTC).replace(tzinfo=None)` two-call dance pure
  Python is forced into. Aware case defers to `datetime.fromtimestamp`
  for correctness on DST boundaries; the win there is the lack of an
  interpreter frame per row.

The extension is **required**. The codecs that import it have no
pure-Python fallback — the constant-factor gap was big enough that
maintaining two parallel decoders wasn't worth it. Source installs
without a working C compiler will fail; binary wheels ship for the
common platforms (see §13 v0.5.1) so `pip install` works without a
compiler in the typical case.

Build shape: ABI3 (`Py_LIMITED_API = 0x030B0000`) plus the
`bdist_wheel` `py_limited_api = "cp311"` setting means one
`cp311-abi3` wheel covers Python 3.11+ across each platform.

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

- **Compression:** auto-detected. v0 supports **LZ4** (fast, the default
  in upstream drivers) and **ZSTD**. The compressed-block framing with
  CityHash128 checksums is implemented in the codec layer, so the connection
  layer is unaware. **Enabled by default (LZ4) when the `[compression]`
  extra is installed;** falls back to uncompressed when the extra is absent.
  Set `CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION=off` to opt out globally, or
  pass `compression=CompressionMethod.NONE` / `?compression=none` in the
  DSN to opt out per-connection.
- **TLS:** `clickhouses://` / `secure=true` in the DSN creates
  `ssl.create_default_context()` (system CA store, hostname verification
  on). Users who need custom CA bundles, client certificates, or disabled
  verification pass an explicit `ssl_context` kwarg to `connect()` /
  `create_pool()`. There is no implicit cert pinning or HPKP — that is the
  caller's responsibility.
- **Socket idle read timeout:** after the TCP+TLS handshake, individual
  `asyncio.StreamReader.read()` calls can block indefinitely if the network
  goes dark mid-stream (TCP connection alive but no data flowing — common
  behind load balancers and in container environments). A configurable
  per-read timeout would wrap every `readexactly` call and transition the
  connection to `BROKEN` on expiry — **not yet implemented, tracked in §15**.

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
        io.py            # AsyncBinaryReader (transport), BinaryWriter
        io_sync.py       # SyncBinaryReader (in-memory codec input)
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

### v0.3 (shipped — 2026-05-04)

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

### v0.4

1. **Example scenario tests.** *(shipped — 2026-05-04.)* Three public
   ClickHouse datasets (COVID-19 epidemiology, OpenCelliD cell towers,
   Hacker News) loaded into an ephemeral server in a dedicated
   `scenarios` CI job. Proves the client works against realistic
   schemas — `Enum8`, `Float64`, `DateTime`, `Array(UInt32)`,
   aggregation, date math, multi-block streaming. No production code
   changes; tests and CI only.
2. **Read-throughput refactor.** *(shipped — 2026-05-07.)* Codec
   `read` is now synchronous, consuming a `SyncBinaryReader` over an
   in-memory buffer instead of awaiting on a stream per primitive.
   Async work survives only at the transport boundary
   (`read_block_buffered` drains and sync-parses, with pushback for
   over-drained bytes). Hot-path codecs (`String`, `DateTime`,
   `Enum`, `UUID`, `Array` offsets, `Decimal32/64`) bulk-unpack with
   `struct` rather than per-row `int.from_bytes`; `String.read`
   inlines varuint + UTF-8 decode against the underlying buffer; the
   `Client` row-tuple transpose uses C-level `zip(*data)`. End-to-end
   the 1M-row `read_throughput` benchmark closes most of the gap to
   `clickhouse-connect`'s thread-pool baseline. The
   `pytest-benchmark` suite under `tests/perf/` tracks the codec-level
   numbers across PRs.
3. **`pyarrow` / `polars` adapter packages** (`clickhouse-async-arrow` /
   `-polars`). Separate extras; build on the `column_factories` hook
   from v0.3. Each block becomes an Arrow `RecordBatch` / Polars
   `DataFrame` with no row-tuple intermediate.
4. **Read-only / write-only pool routing.** Primary-only writes,
   replica-fanout reads — builds on multi-host DSN.
5. **`AggregateFunction` allow-list expansion.** Adding `quantile`,
   `uniq`, etc. is a one-line registration in `aggregate.py`; deferred
   until a real workload drives priority.

### v0.5 (shipped — 2026-05-07)

1. **`_fast_read` C extension.** ABI3 setuptools-built extension
   hosting `decode_datetime` and `decode_strings`. The String and
   DateTime read codecs route through it. See §7 "`_fast_read` C
   extension" for the function surface and the build-shape rationale.
2. **`Date` / `Date32` `date.fromordinal` swap.** Pure-Python win
   surfaced while building the missing per-codec micro-bench:
   replacing `_EPOCH_DATE + timedelta(days=d)` with
   `date.fromordinal(epoch_ord + d)` cuts those codecs' per-row work
   by 4.3x.
3. **Read-throughput on the cross-library benchmark.** 1M-row
   `(UInt64, String, DateTime)` read goes from 1.47 M r/s (v0.4.1) to
   3.79 M r/s — within 1.34x of `clickhouse-connect`'s native-async
   client and roughly 5.6x faster than the `asynch` forks.

### v0.5.1 (shipped — 2026-05-07)

1. **Pure-Python fallback removed.** v0.5.0 carried inlined
   pure-Python implementations alongside the C path so installs
   without a compiler kept working. The maintenance cost outweighed
   the benefit — the C build is well-supported on every platform we
   target, and the pure-Python branch was hard to keep in lockstep
   with the C semantics. The codecs now require the extension; source
   installs without a C compiler fail explicitly rather than
   silently picking the slow path.
2. **cibuildwheel matrix.** A new `wheels.yml` workflow builds
   binary wheels for the common platforms (linux/x86_64,
   linux/aarch64, linux/x86_64-musl, macos/arm64, windows/AMD64) on
   every push and attaches them to GitHub Releases on tag pushes.
   ABI3 means one wheel per platform covers Python 3.11+, so the
   matrix is platforms only. Intel macOS users build from sdist —
   GitHub's last x86_64 macOS runner image is on borrowed time and
   the cibuildwheel CI cost wasn't worth carrying for a shrinking
   audience.

### v1

1. **OpenTelemetry instrumentation** around `execute` / `acquire` /
   packet send/receive, gated behind an extra so the bare install stays
   import-clean. Pinned to v1 so span shapes don't churn while the API
   is still settling.

---

## 15. Production readiness gaps

Features that are either planned but not yet implemented, or entirely
missing, that block recommending this client for production use. Each item
here has a corresponding entry in `TODO.md §2`. Once all items in this
section are shipped, the version should advance to **v1.0**.

### 1. Client-side per-query timeout

**Status:** Designed (§4) but not implemented. No `timeout` parameter
exists on `execute`, `fetch_all`, `iter_blocks`, `insert`, or any other
public method.

**Risk:** If ClickHouse is overloaded and stops sending packets, or if a
network partition occurs mid-stream, the calling coroutine blocks forever.
Server-side `max_execution_time` does not protect against this because it
only fires if the server is still running — a dead TCP-alive-but-silent
connection (common behind load balancers) silently hangs the client.

**Design:** `timeout: float | None = None` on every query method. If set,
wraps the packet-iteration loop with `asyncio.timeout(timeout)`. Expiry
sends a `Cancel` packet, drains with `drain_timeout`, and raises
`QueryCancellationError(reason="client_timeout")`. If the cancel drain
itself times out, marks the connection `BROKEN`.

### 2. Socket idle read timeout

**Status:** Not implemented. `connect_timeout` covers only the TCP+TLS
handshake.

**Risk:** Independent of (1). After connection, individual
`asyncio.StreamReader.readexactly()` calls have no deadline. A half-open
TCP connection (server rebooted, load balancer dropped the session, network
partition) silently blocks every read forever, independent of whether the
user set a per-query `timeout`.

**Design:** `read_timeout: float | None = None` on `Connection` / DSN.
Wraps each `readexactly` in an `asyncio.wait_for`. Timeout transitions the
connection to `BROKEN` and raises `ConnectionError`. Distinct from
per-query timeout: per-query timeout caps total query duration; read timeout
caps the gap between successive packets.

### 3. `fetch_all` / `execute` result-size guard

**Status:** Described in §4 ("a `max_rows` setting on the client stops
accidental unbounded loads") but not implemented in code.

**Risk:** `fetch_all("SELECT * FROM large_table")` accumulates all rows
into a Python list. A 100 M-row result set will exhaust memory with no
warning or circuit-breaker.

**Design:** `max_rows: int | None = None` on `execute` / `fetch_all`.
After accumulating each block, check `len(rows) >= max_rows` and raise
`ResultTooLargeError` (new `ClickHouseError` subclass). Does not send a
Cancel — the error marks the connection `BROKEN` since partial drain
would be ambiguous. Callers who want large results should use `iter_blocks`
with an explicit consumer loop. A default can be set at the `Client` or
`Pool` level via `default_max_rows`.

### 4. Structured query logging

**Status:** `logging.getLogger("clickhouse_async")` is instantiated in
`connection.py` and `pool.py` but used almost nowhere: one `DEBUG` call
in `connection.py`, four in `pool.py`. No log emitted on query start,
query end, connection open/close, pool acquire/release, or health-check
result.

**Risk:** Debugging production incidents requires replaying what queries
ran, on which connections, how long they took, and what pool events
preceded a failure. Without structured logs, operators are blind.

**Design:** Emit to `logging.getLogger("clickhouse_async")` at these
points, with structured extra fields (never interpolated into the message
so log aggregators can filter):
- `INFO`: connection opened/closed, pool acquire/release (with
  `query_id`, `host`, `elapsed_ms`).
- `DEBUG`: query started (sql truncated to 200 chars, `query_id`,
  `params` redacted if a `redact_params` option is set), query finished
  (elapsed, rows, bytes).
- `WARNING`: health-check ping failed, connection discarded, acquire
  timeout imminent.
- `ERROR`: unhandled pool reaper exception (already present).

SQL is truncated, not redacted by default — operators may opt in to
`log_queries=False` if queries contain sensitive literals.

### 5. Graceful pool drain on shutdown

**Status:** `pool.close()` closes all connections immediately, including
those backing in-flight queries. Callers mid-`async with pool.acquire()`
get their client torn away.

**Risk:** Zero-downtime deploys require finishing in-flight queries before
recycling the process. Abrupt close means any query in flight when the
application shuts down fails with a transport error, even if it was
milliseconds from completion.

**Design:** `pool.drain(timeout: float = 30.0)` — a new method that:
1. Sets a "draining" flag so new `acquire()` calls raise `PoolClosedError`
   immediately.
2. Waits until all currently-acquired clients are returned (via
   `asyncio.Condition`), bounded by `timeout`.
3. Then closes idle connections and returns. Callers chain
   `await pool.drain(); await pool.close()` in their shutdown handler.

`pool.close()` gains a `drain_timeout: float = 0.0` parameter as a
shortcut: `0.0` (default) means "close immediately" (current behaviour);
positive value means "drain first".

### 6. Lightweight instrumentation hooks

**Status:** No callback or hook surface for observing query latency, pool
utilisation, or error rates — short of attaching a `logging.Handler` or
waiting for OTel (v1).

**Risk:** Before OTel lands, operators cannot alert on slow queries, count
errors, or graph pool wait times without monkey-patching internal methods.

**Design:** Two sync callbacks, both optional and `None` by default,
accepted at `create_pool()` and `connect()`:

```python
on_query_start: Callable[[QueryEvent], None] | None = None
on_query_end:   Callable[[QueryEvent], None] | None = None
```

`QueryEvent` is a small dataclass: `query_id`, `sql` (truncated),
`host`, `started_at`, `elapsed` (only on end), `rows`, `error`.
Callbacks are sync (no `await`), called with the GIL held. Users bridge
to async or external systems themselves (e.g., `loop.call_soon`). These
are intentionally minimal — OTel replaces them at v1 with proper spans.

### 7. PyPI release and versioned wheels

**Status:** The package is installable via `pip install git+https://…`
but has not been published to PyPI. README notes "Not yet on PyPI."

**Risk:** VCS installs are not reproducible across environments (git ref
can be force-pushed), are not accepted by some security-conscious
dependency scanners, and cannot be pinned in a `uv.lock` / `pip-compile`
workflow the same way as a versioned wheel.

**Design:** Publish to PyPI at v0.3 or v0.4 (whichever ships first after
the API surface stabilises enough not to need a major-version bump on the
first public release). Use `uv build` + `twine upload`. CI gains a
`publish` job gated on tags matching `v*`. Enforce that `pyproject.toml`
`[project.version]` matches the git tag before publish.

### 8. INSERT deduplication token

**Status:** ClickHouse's `insert_deduplication_token` setting can be
passed today via `settings={"insert_deduplication_token": "..."}` but is
undocumented and has no first-class API surface.

**Risk:** Without a documented deduplication story, callers who implement
retry logic cannot achieve at-most-once INSERT semantics. An insert that
succeeds but whose confirmation is lost (network partition before the
`await insert(...)` returns) will be retried and duplicated.

**Design:** Add `deduplication_token: str | None = None` to `Client.insert()`.
When set, it is injected into `settings` as `insert_deduplication_token`.
The recommended pattern — generate a deterministic token from the content
or operation ID, pass it on every retry attempt — is documented alongside
the parameter. The pool's `insert()` pass-through surfaces the same kwarg.

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
