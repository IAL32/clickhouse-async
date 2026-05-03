# TODO

Tracks features intentionally deferred and the post-v0 roadmap. Anything
"shipped but with a caveat" goes in §1; anything not yet written goes
in §2. Out-of-scope for v0 by design lives in
[`DESIGN.md` §1](./DESIGN.md) — this file is for what we *will* do, not
what we won't.

When a deferral lands, delete the entry. If a deferral grows into a
real product question, escalate it to [`DESIGN.md` §14 (open
questions)](./DESIGN.md).

---

## 1. v0 known limitations

Already-shipped code with documented caveats. Each entry names the
codec/module that carries the limitation so a grep finds the on-ramp.

### Type system (`clickhouse_async/types/`)

- **`LowCardinality(Nullable(T))` rejected at construction.** ClickHouse
  reserves dictionary index 0 for the null placeholder, with all data
  indices shifted by +1, when the inner is `Nullable`. We don't
  implement that yet — the codec raises `ValueError` with a hint to use
  `Nullable(LowCardinality(T))` instead.
  *Code:* `composite.py::LowCardinality.__init__`.
- **`DateTime64(p)` for `p > 6` truncates the lowest digits.** Python's
  `datetime` carries microsecond resolution (10⁻⁶ s). Round-tripping is
  exact at `p ≤ 6`; `p ∈ {7, 8, 9}` loses precision at the Python
  boundary. We accept and write at the codec's native scale; the
  truncation happens when constructing the `datetime`.
  *Code:* `datetime.py::DateTime64.read/write`.
- **`Tuple` named-field syntax not parsed.** ClickHouse server can emit
  `Tuple(name String, age Int32)`. We only handle the unnamed form
  `Tuple(String, Int32)`. Tables with named tuples will fail to parse
  the column type spec.
  *Code:* `__init__.py::_make_tuple`.
- **No `AggregateFunction`, `JSON`, `Variant`, `Dynamic`, `Nested`,
  geo types.** Listed as out-of-scope in `DESIGN.md §7` (deferred). A
  query returning any of these will fail at the type-spec parser
  ("unknown type"). Round-tripping these requires careful work; it's a
  v0.x feature.
- **Naive `datetime` is interpreted as UTC.** A bare `DateTime` codec
  (no timezone parameter) returns naive `datetime` objects whose
  underlying instant is UTC. ClickHouse drives its display via the
  session's timezone setting; v0 doesn't plumb the session timezone
  from the connection layer into the codec, so naive ≠ "session
  timezone". Plumbing this is a connection-layer task.
  *Code:* `datetime.py::DateTime.read`, `_naive_utc_from_ts`.

### Protocol primitives

- **Single-quoted strings in type specs don't support escapes.** The
  parser reads up to the next single quote — server-emitted type
  strings shouldn't contain doubled quotes or backslash escapes, but
  user-authored CREATE TABLE syntax can. If we ever parse user input
  through this, extend `_read_quoted_string`.
  *Code:* `types/__init__.py::_Parser._read_quoted_string`.
- **`Connection.send_data` does not validate the block's columns
  against the server's INSERT header.** Misaligned columns surface as
  a `ServerError` from the next `iter_packets` read instead of a
  fast, named-column-vs-named-column diagnostic. Header validation
  belongs at the Client layer (07) where the header→block flow is
  owned; recorded here so the gap is visible until 07 lands.
  *Code:* `connection.py::Connection.send_data`.
- **No background idle reaper on `Pool`.** v0 implements per-acquire
  health checks (`health_check_after`) and per-release lifetime caps
  (`max_lifetime`), so stale connections are recycled when they're
  next touched. But there's no asynchronous background task that
  evicts long-idle connections proactively while keeping the pool
  above `min_size`. For workloads with bursty acquire patterns and
  long quiet periods, idle connections sit in the queue until the
  next acquire-or-release evicts them. The `min_size`-warm aspect
  also relies on something opening connections proactively, which
  v0's lazy fill doesn't do; the parameter is accepted at
  `create_pool` for forward compatibility but isn't enforced.
  *Code:* `pool.py::Pool` — see ``DESIGN.md §5`` for the original
  contract.
- **No automatic cancel-on-break for raw `Connection.iter_packets`.**
  Breaking out of `async for s in conn.iter_packets()` does not
  eagerly send a `Cancel` — Python defers async-generator finalisation
  to GC time. The high-level `Client.iter_blocks` / `iter_rows` wrap
  the iteration with a try/finally that runs `cancel()` on early
  exit, so the recommended user-facing pattern is
  `async with aclosing(client.iter_blocks(...))`. Direct callers of
  `Connection.iter_packets` still need to invoke `conn.cancel()`
  themselves; the higher-level Pool (08) will own this orchestration
  for the rare cases where users hold a raw Connection.
  *Code:* `connection.py::Connection.iter_packets`,
  `client.py::Client.iter_blocks`.

---

## 2. Roadmap (post-v0)

Things we haven't written yet, ordered by approximate priority. Items
already on `DESIGN.md §13` are repeated here so this file is the single
"what's next" reference.

### Connection / protocol

- **Multi-host DSN with round-robin + failover.** v0 connects to
  exactly one host. The DSN parser will need to accept comma-separated
  hosts; the connection logic gets a "try next on connect failure"
  loop. Pool integration is the trickier part — failed acquire-time
  health checks should mark a host bad for some cooldown, not just
  rotate forever.
- **Cancel a query by `query_id` from a different connection.** v0's
  cancel goes through the same connection that issued the query.
  Issuing `KILL QUERY WHERE query_id = ...` from a side channel is the
  cross-connection path; needs a small helper API on the pool.
- **Compression default on.** Currently off. Once we have a benchmark
  suite and an integration test that exercises multi-block compressed
  payloads, flip the default for connections opened with the
  `compression` extra installed.
- **Parameter-binding fallback policy.** Open question in `DESIGN.md
  §14`. We currently raise on too-old servers; decide whether to keep
  raising or silently emit client-side substitution (probably keep
  raising — silent fallback undermines the safety claim).

### Type system

- **`LowCardinality(Nullable(T))`.** Implement the null-at-index-0
  layout with shifted indices.
- **`AggregateFunction(...)` state columns.** Used by materialised
  views; needed for any non-trivial analytics workload.
- **`JSON` type** (the new ClickHouse 24.x JSON, not the deprecated
  String-backed Object).
- **`Variant`, `Dynamic`, `Nested`, geo types.**
- **Tuple named fields.** `Tuple(name T, ...)` parsing — tracked in
  §1 as a limitation; the implementation belongs here.
- **Custom `column_factories` hook.** Per-type override for Python
  representation (e.g. polars/pyarrow/numpy adapters) — the protocol
  is described in `DESIGN.md §7`. Default factories ship in core; the
  adapters live in extras.

### Client / pool

- **No automatic query retry.** Documented as a deliberate
  non-feature in `DESIGN.md §5`; surfaced here because users will
  ask. Connection-level reconnect on `acquire()` is fine and lands
  with the pool. Query-level retry is the caller's problem.
- **Read-only / write-only pool variants.** Multi-host opens this up
  — primary-only writes, replica-fanout reads.
- **Read-receipt for `INSERT`.** Surface server-confirmed
  `written_rows` separately from "we sent N rows".

### Adapters / extras

- **`pyarrow` zero-copy adapter** as a separate extras package
  (`clickhouse-async-arrow`). Not in core to keep the bare install
  small.
- **`polars` adapter.** Same shape.
- **C/Cython hot path** for the int/float/string codecs *only if*
  profiling shows pure-Python encoders are the bottleneck on large
  inserts. Don't pre-optimise.

### Observability

- **OpenTelemetry spans** around `execute` / `acquire` / packet
  send/receive. Optional dep; instrumented via a hook so users
  without OTel pay nothing.

---

## 3. Open design questions

These have no obvious right answer; they need a decision before the
relevant code lands. Mirrors `DESIGN.md §14`.

- **Parameter-binding fallback for old servers.** Refuse with a clear
  error (current lean) vs. silently substitute. Decision: refuse.
  Locking it in here means we can stop revisiting.
- **`Block.to_arrow()` / `.to_polars()` location.** Core vs. extras.
  Decision: extras package, keep core lean.
- **Default compression on/off.** Currently off. Will revisit once a
  benchmark suite exists.
