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

- **`AggregateFunction(...)` only round-trips a small allow-list of
  known aggregates.** v0.2 ships per-row state readers for ``avg``
  and ``count``; everything else parses but raises
  ``NotImplementedError`` on read/write. Adding a function is a
  one-line registration in ``types/aggregate.py::_READERS``.

- **`JSON` shared-data substream on write.** Reads decode the
  ``Array(Tuple(String, String))`` shared-data substream and merge
  values into the per-row dict, but writes always emit an empty
  substream — paths that spill past ``max_dynamic_paths`` are silently
  dropped. Wire it up so the per-row dict picks up shared paths on
  write.
  *Code:* ``types/json_type.py``.

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
  themselves.
  *Code:* `connection.py::Connection.iter_packets`,
  `client.py::Client.iter_blocks`.

---

## 2. Roadmap

Things we haven't written yet, ordered by approximate priority.

### v0.3 — Columnar surface + JSON completeness + compression on

*See `.plans/README.md` for the full dependency-ordered plan list.*

- **Column-major retrieval surface.** `Client.fetch_columns(sql) ->
  ColumnarResult` and `Client.iter_column_blocks(sql)` avoid the
  per-row tuple transpose. `Pool` gets the same pass-throughs.
  *Code:* `client.py::Client.execute` is the only place rows get
  transposed; `Block.data` already holds column-major values.
- **`column_factories` hook.** `column_factories: dict[str, Callable]`
  kwarg on `connect()` / `create_pool()` lets callers replace the
  default `list` with any type (numpy, polars, pyarrow) per column.
  Depends on the columnar surface above.
- **`JSON` nested dict ergonomics.** `json_nested=True` mode reconstructs
  nested dicts from dotted-path keys on read; write accepts either flat
  or nested input. Shared-data write substream correctly populated for
  overflow paths beyond `max_dynamic_paths`.
  *Code:* `types/json_type.py`.
- **Compression default on.** `_default_compression()` helper auto-enables
  LZ4 when the `[compression]` extra is installed; `CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION=off`
  opts out globally; `compression=None` opts out per-connection.

### v0.4+ — Adapters and extended type support

- **`JSON` typed paths.** `JSON(SKIP path)` and `JSON(SKIP REGEXP 'rx')`
  parse but the codec doesn't reflect typed-path columns yet.
- **`pyarrow` zero-copy adapter** (`clickhouse-async-arrow`). Builds on
  the columnar surface — each `iter_column_blocks` block becomes an
  Arrow `RecordBatch` with no row-tuple intermediate.
- **`polars` adapter.** Same shape as the Arrow adapter.
- **Read-only / write-only pool variants.** Multi-host opens this up —
  primary-only writes, replica-fanout reads.
- **C/Cython hot path** for int/float/string codecs *only if* profiling
  shows pure-Python encoders are the bottleneck on large inserts.

### v1 — Observability and API stability

- **OpenTelemetry spans** around `execute` / `acquire` / packet
  send/receive. Optional dep; instrumented via a hook so users
  without OTel pay nothing.
- **Parameter-binding fallback policy.** Currently we raise on too-old
  servers. Decision stands: refuse — silent fallback undermines the
  safety claim.
- **No automatic query retry.** Documented as a deliberate non-feature;
  surfaced here because users will ask. Connection-level reconnect on
  `acquire()` is fine. Query-level retry is the caller's problem.

---

## 3. Open design questions

These have no obvious right answer; they need a decision before the
relevant code lands.

- **`Block.to_arrow()` / `.to_polars()` location.** Core vs. extras.
  Decision: extras package, keep core lean.
