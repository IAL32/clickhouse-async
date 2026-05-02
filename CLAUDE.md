# CLAUDE.md

Guide for working in this repo. Read this first; for depth, see the docs
referenced below.

## Scope
An async Python client for ClickHouse over the **native TCP protocol**
(`:9000` / `:9440`). HTTP (`:8123`) is out of scope. Pre-v0.

## Where to read
- `README.md` — install, quick-start, conventions for humans.
- `DESIGN.md` — architecture, protocol layering, type system, roadmap.
  Sections §1 and §13 list what is intentionally **not** in scope.
- `.plans/` — step-by-step v0 build plan (gitignored, local notes).
  Numbered by dependency order; `.plans/README.md` is the index.

## Tooling (use these, not alternatives)
- `uv` for everything: `uv sync`, `uv lock`, `uv run <cmd>`.
- `ty` for types: `uv run ty check`. **Do not add mypy.**
- `ruff` for lint+format: `uv run ruff check && uv run ruff format`.
- `pytest` (+ `pytest-asyncio`, `asyncio_mode = "auto"`).
- `prek` for git hooks (drop-in `pre-commit` replacement; same
  `.pre-commit-config.yaml`). Install hooks with `uv run prek install`.
  **Do not use `pre-commit` directly.**

## Code rules
- `asyncio` end-to-end. No threads, no `run_in_executor` on the hot
  path, no sync I/O hidden inside `async def`.
- One in-flight query per `Connection` / `Client`; concurrent calls
  raise `ConcurrentQueryError`. For concurrency, use `Pool`.
- Parameter binding is **server-side** via ClickHouse's `{name:Type}`
  syntax. Never client-side string interpolation. If the server is too
  old to support it, raise — don't silently fall back.
- Compression libs (`lz4`, `zstandard`, cityhash) are **optional
  extras**, never core deps. Their imports are **lazy** (inside the
  codec's first call). `import clickhouse_async` on a bare install must
  not raise. Missing extra at runtime → `MissingExtraError` with the
  exact `pip install clickhouse-async[<extra>]` command.
- Strict typing on the public API. No `Any` in public signatures, no
  `# type: ignore` without a comment explaining why.

## Test rules
- Split test bodies with BDD comments. Each marker carries an inline
  description: `# BEGIN: <setup>`, `# WHEN: <action>`, `# THEN:
  <assertion>`. Order is fixed; bare markers (no colon, no context) are
  not allowed. `# WHEN` holds a single action — if two, it's two tests.
  Skip phases that don't apply. For `pytest.raises`, write `# WHEN:` and
  `# THEN:` as two adjacent comments above the `with` block.
- **Integration tests clean up before they run, not after.** Each test
  drops and recreates the tables/databases it owns at entry. Teardown
  is a nice-to-have, not the contract — a previously crashed test must
  not poison the next run.
- Integration tests use the project's `ClickHouseContainer` subclass at
  `tests/containers/clickhouse.py`. **No hand-rolled `docker run`, no
  `docker-compose.yml`, no direct upstream `testcontainers.clickhouse`.**
- `--localdb` (bare → default DSN; `--localdb=clickhouse://...` →
  custom) skips the container in favour of a local server.
- Default `pytest` runs unit only. Integration is opt-in via
  `tests/integration` or `-m integration`.
- CI must run two install shapes: `bare` (`uv sync --no-extras`,
  catches accidental top-level imports of optional libs) and `full`
  (`uv sync --extra compression`, runs integration parameterized over
  `compression ∈ {None, lz4, zstd}`).

## Don'ts
- Don't add `mypy`, mypy-only ignores, or a parallel mypy config.
- Don't use `pre-commit` directly; use `prek` (it reads the same config).
- Don't add hand-rolled docker setups outside `tests/containers/`.
- Don't put `lz4` / `zstandard` / cityhash in core `[project.dependencies]`.
- Don't add automatic query retry to the pool. Connection-level
  reconnect on `acquire` is fine; query-level retry is the caller's.
- Don't add HTTP transport, multi-host DSN, JSON/Variant/Dynamic types,
  or arrow adapters in v0 — those are roadmap (`DESIGN.md §13`).

## Canonical default DSN
`clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse` — used
by both the test container and the `--localdb` default. If one
changes, both must.
