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

- **No `AggregateFunction`, `JSON`, `Variant`, `Dynamic` types.**
  Listed as out-of-scope in `DESIGN.md §7` (deferred). A query
  returning any of these will fail at the type-spec parser
  ("unknown type"). Round-tripping these requires careful work;
  it's a v0.x feature.

### Protocol primitives

- **Single-quoted strings in type specs don't support escapes.** The
  parser reads up to the next single quote — server-emitted type
  strings shouldn't contain doubled quotes or backslash escapes, but
  user-authored CREATE TABLE syntax can. If we ever parse user input
  through this, extend `_read_quoted_string`.
  *Code:* `types/__init__.py::_Parser._read_quoted_string`.
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

- **Compression default on.** Currently off. Once we have a benchmark
  suite and an integration test that exercises multi-block compressed
  payloads, flip the default for connections opened with the
  `compression` extra installed.
- **Parameter-binding fallback policy.** Open question in `DESIGN.md
  §14`. We currently raise on too-old servers; decide whether to keep
  raising or silently emit client-side substitution (probably keep
  raising — silent fallback undermines the safety claim).

### Type system

- **`AggregateFunction(...)` state columns.** Used by materialised
  views; needed for any non-trivial analytics workload.
- **`JSON` type** (the new ClickHouse 24.x JSON, not the deprecated
  String-backed Object).
- **`Variant`, `Dynamic` types.**
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
- **Column-major retrieval surface (v0.3).** ClickHouse blocks
  arrive column-major on the wire, but `Client.execute` /
  `fetch_all` / `iter_rows` transpose into row-major tuples for
  every block. For wide / numeric SELECTs the transpose dominates
  decode time and allocates one Python tuple per row needlessly.
  Plan: keep the row-major surface as the default for ergonomics,
  add a parallel `Client.fetch_columns(sql, …) -> ColumnarResult`
  that returns the columns as a list of per-column lists (already
  the `Block.data` shape under the hood) and an
  `iter_column_blocks(...)` for streaming. Doubles as the
  zero-copy entry point for the `pyarrow` / `polars` adapters
  below — those wrap `iter_column_blocks` and convert each block
  to an Arrow `RecordBatch` / Polars `DataFrame` without ever
  materialising row tuples.
  *Code:* `client.py::Client.execute` is the only place rows get
  transposed today; `Block.data` already holds column-major
  values, so the new surface is mostly plumbing.

### Adapters / extras

- **`pyarrow` zero-copy adapter** as a separate extras package
  (`clickhouse-async-arrow`). Not in core to keep the bare install
  small. Builds on the column-major retrieval surface above —
  each `iter_column_blocks` block becomes an Arrow `RecordBatch`
  with no row-tuple intermediate.
- **`polars` adapter.** Same shape.
- **C/Cython hot path** for the int/float/string codecs *only if*
  profiling shows pure-Python encoders are the bottleneck on large
  inserts. Don't pre-optimise.

### Observability (v1)

- **OpenTelemetry spans** around `execute` / `acquire` / packet
  send/receive. Optional dep; instrumented via a hook so users
  without OTel pay nothing. Pencilled in for v1 once the type system
  is complete and the API surface is stable enough that span shapes
  won't churn.

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
