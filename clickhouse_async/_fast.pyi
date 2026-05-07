"""Type stubs for the Rust-built ``clickhouse_async._fast`` extension.

The actual module is a binary ``.so`` / ``.pyd`` produced by maturin
from the Rust crate at the repo root. This stub file is the canonical
documentation of the function surface for IDEs, ``ty``, and any other
static analysis the project runs.

Hand-maintained — changes to the Rust pymodule signatures must be
reflected here in the same PR.
"""

def decode_strings(buf: bytes, n_rows: int) -> list[str]:
    """Decode ``n_rows`` varuint-prefixed UTF-8 strings from ``buf``.

    ``buf`` is the on-wire layout of a String column body —
    ``[varuint length, body bytes] x n_rows`` packed end-to-end. Rust
    walks the buffer once, parsing each varuint and constructing one
    ``str`` per row.

    Raises :class:`ValueError` (with a precise row index) on:

    - varuint truncation or > 64-bit overflow
    - body bytes referencing offsets beyond ``buf``
    - invalid UTF-8 within any row's body
    """
    ...
