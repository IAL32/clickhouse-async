# clickhouse-async — Design Proposal

A pure-Python, fully `asyncio`-native client for ClickHouse over the **native TCP
protocol** (default port `9000` / `9440` for TLS).

This document describes the v0 design: the client, the connection pool, and the
layers underneath that make them work. It is deliberately scoped — the goal is
to ship a small, correct, idiomatic async client first, and grow features
behind a stable surface.

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
- **Min-size warm.** `min_size` connections are created lazily on first use
  but kept alive afterwards. We do **not** open them eagerly at `create_pool`
  time — that turns a misconfigured DSN into an import-time failure.
- **Idle reaper.** A background task evicts connections whose idle time
  exceeds `max_idle_time` while keeping the pool above `min_size`.
- **Lifetime cap.** `max_lifetime` forces recycle of long-lived connections,
  which dodges server-side `max_session_timeout` surprises and helps DNS
  changes propagate.
- **Health checks on acquire.** A connection borrowed from the pool gets a
  cheap `Ping` if it has been idle longer than `health_check_after`. Failed
  pings drop the connection and we transparently open a fresh one — but
  only on `acquire`, never mid-query.
- **Fairness.** A FIFO waiter queue. Asyncio's default `Queue` gives this
  for free.
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
- **No load balancing across hosts in v0.** A list of hosts in the DSN is
  on the roadmap (round-robin + failover), but v0 connects to exactly one.

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

### Types in v0
- **Numeric:** `Int8/16/32/64/128/256`, `UInt8/16/32/64/128/256`,
  `Float32/64`, `Bool`.
- **Decimal:** `Decimal32/64/128/256` with scale.
- **String:** `String`, `FixedString(N)`.
- **Time:** `Date`, `Date32`, `DateTime` (with optional timezone),
  `DateTime64(precision, tz)`.
- **Net/UUID:** `UUID`, `IPv4`, `IPv6`.
- **Composite:** `Array(T)`, `Tuple(T...)`, `Map(K, V)`, `Nullable(T)`,
  `LowCardinality(T)`, `Enum8/16`.

### Deferred
- `AggregateFunction(...)` state columns, `Nested`, `Geo`, `JSON` (the new
  variant), `Variant`, `Dynamic`. These need careful round-tripping and
  deserve their own pass after v0 ships.

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
    client.py            # Client
    pool.py              # Pool
    connection.py        # Connection state machine
    dsn.py               # connection-string parsing
    errors.py
    protocol/
        packets.py       # packet ids, hello/query/data structs
        reader.py        # async packet reader
        writer.py        # async packet writer
        compression.py   # LZ4 / ZSTD frame
    types/
        __init__.py      # registry: name → codec
        primitive.py
        decimal.py
        datetime.py
        string.py
        composite.py     # Array, Tuple, Map, Nullable, LowCardinality
        net.py           # UUID, IPv4, IPv6
tests/
    unit/
    integration/
    fixtures/            # hex captures from a real server
```

---

## 13. Roadmap after v0

In rough priority order, gated on v0 being stable:

1. Multi-host DSN with round-robin + failover.
2. `AggregateFunction(...)` state columns and the new `JSON` type.
3. Optional `pyarrow` / `polars` zero-copy adapters as separate extras.
4. A C/Cython hot path for the int/float/string codecs if profiling shows
   the pure-Python encoders are the bottleneck on large inserts.
5. Server-side query cancellation by `query_id` (cancelling from a different
   connection than the one running the query).
6. Optional opentelemetry span emission around `execute` / `acquire`.

---

## 14. Open questions

- **Parameter binding fallback.** The server's parameter feature requires a
  recent revision; do we silently fall back to client-side substitution for
  older servers, or refuse? Leaning toward refuse + clear error — silent
  fallback to string substitution would undermine the safety claim above.
- **Block-as-DataFrame.** Should `Block` expose a `.to_arrow()` / `.to_polars()`
  in core, or live in an extras package? Core stays light → extras package.
- **Default compression.** LZ4 is essentially free CPU-wise and saves a lot
  of bytes on large result sets. Tempting to enable by default, but it hides
  protocol-level bugs during early development. Default off for v0; revisit
  after we have a benchmark suite.
