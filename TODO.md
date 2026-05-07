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
  known aggregates.** v0.2 ships per-row state readers for `avg`
  and `count`; everything else parses but raises
  `NotImplementedError` on read/write. Adding a function is a
  one-line registration in `types/aggregate.py::_READERS`.

- **`JSON` shared-data substream on write.** Reads decode the
  `Array(Tuple(String, String))` shared-data substream and merge
  values into the per-row dict, but writes always emit an empty
  substream — paths that spill past `max_dynamic_paths` are silently
  dropped. Wire it up so the per-row dict picks up shared paths on
  write.
  *Code:* `types/json_type.py`.

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

### v0.3 — Columnar surface + JSON completeness + compression on (shipped)

All four items landed in v0.3 — see `DESIGN.md §13`.

### v0.4 — Example scenarios (shipped)

### Pre-v1 production requirements

These block recommending the client for production use. They are not
feature additions — they are correctness and operability gaps. Every item
here has a detailed design in `DESIGN.md §15`. All must ship before the
version advances to 1.0.

- **Client-side per-query timeout.** No `timeout` parameter exists on any
  public query method. A half-open TCP connection or an overloaded server
  that stops sending packets will block the caller forever. Server-side
  `max_execution_time` does not protect against network partitions.
  Design: `timeout: float | None = None` on `execute`, `fetch_all`,
  `iter_blocks`, `insert`; wraps packet iteration with
  `asyncio.timeout()`; expiry triggers cancel-and-drain.
  *Code:* `client.py`.

- **Socket idle read timeout.** `connect_timeout` covers only the TCP/TLS
  handshake. After connection, `asyncio.StreamReader.readexactly()` can
  block forever if the server or network goes silent mid-stream (common
  behind load balancers). Design: `read_timeout: float | None = None` on
  `Connection`; wraps every `readexactly` call; expiry transitions to
  `BROKEN`.
  *Code:* `connection.py`.

- **Structured query logging.** Exactly one `_logger.debug()` call exists
  in `connection.py`. Production operations require: query start/end with
  `query_id`, `host`, and `elapsed`; pool acquire/release; health-check
  results; connection lifecycle events. Design: emit to
  `logging.getLogger("clickhouse_async")` at `INFO` (lifecycle) and
  `DEBUG` (query start/end with truncated SQL).
  *Code:* `connection.py`, `pool.py`, `client.py`.

- **Graceful pool drain on shutdown.** `pool.close()` tears connections
  immediately, dropping in-flight queries. Design: `pool.drain(timeout)`
  sets a draining flag (new `acquire()` → `PoolClosedError`) and waits
  for all acquired clients to be returned before closing idle connections.
  `pool.close(drain_timeout=30.0)` as a shortcut.
  *Code:* `pool.py`.

- **Lightweight instrumentation hooks.** No callback surface for observing
  query latency or error rates before OTel (v1). Design: `on_query_start`
  and `on_query_end` sync callbacks on `connect()` / `create_pool()`,
  receiving a small `QueryEvent` dataclass. OTel replaces these at v1.
  *Code:* `client.py`, `pool.py`.

- **PyPI release and versioned wheels.** Installable via VCS only. VCS
  installs are not reproducible, fail security scanners, and can't be
  pinned in lockfiles. Publish to PyPI at v0.3 or v0.4; gate on a tag
  matching `v*` in CI. Since v0.4 the package contains a Rust
  extension built via maturin, so the wheel matrix is per-OS × per-
  arch (with `abi3-py311` flattening Python-minor variants). Use
  `cibuildwheel` for the linux/macos/windows × x86_64/aarch64 build
  matrix; `maturin publish` once for sdist.
  *Code:* `pyproject.toml`, `.github/workflows/`.


### v0.5+ — Adapters and extended type support

- **`JSON` typed paths.** `JSON(SKIP path)` and `JSON(SKIP REGEXP 'rx')`
  parse but the codec doesn't reflect typed-path columns yet.
- **`pyarrow` zero-copy adapter** (`clickhouse-async-arrow`). Builds on
  the columnar surface — each `iter_column_blocks` block becomes an
  Arrow `RecordBatch` with no row-tuple intermediate.
- **`polars` adapter.** Same shape as the Arrow adapter.
- **Read-only / write-only pool variants.** Multi-host opens this up —
  primary-only writes, replica-fanout reads.
- **Additional Rust hot paths.** v0.4 ships one Rust function
  (`decode_strings`) that replaces `String.read`'s per-row UTF-8 +
  PyUnicode loop. Two further candidates were prototyped during v0.4
  and **abandoned** because CPython's intrinsics are already C-fast
  for those shapes (transpose: `tuple()` + comprehension, big-int
  decode: `int.from_bytes`) — see `.plans/04-transpose.md` and
  `.plans/05-decode-big-int.md` for the postmortems. The v0.5+
  candidates worth re-measuring against the same discriminator
  ("does the inner loop touch raw bytes, or already-Pythonised
  objects?"):
  - **AggregateFunction state per-row decode** (`aggregate.py:144`):
    has a per-row `await`, so the async-dispatch saving may exceed
    CPython's baseline. Worth a prototype if AggregateFunction
    performance matters.
  - **Long-string columns** (logs, JSON-as-text, English text):
    `decode_strings`'s 4 % win on the short-string benchmark grows
    when the per-row UTF-8 work dominates over async dispatch.
    Worth a dedicated benchmark scenario before deciding.
  - **Nullable mask + values combine** in `composite.py`: per-row
    Python conditional that may benefit from a vectorised Rust
    pass.

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
