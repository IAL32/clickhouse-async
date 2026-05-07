"""Type stub for the ``_fast_read`` C extension.

The implementation lives in ``_fast_read.c`` and is built into a
``.abi3.so`` at install time. This stub gives ``ty`` and other
type-checkers a source to read against — without it the codecs
that import ``_fast_read`` would surface ``no member`` errors.

Keep the signatures in lockstep with the ``PyMethodDef`` table in
``_fast_read.c``.
"""

from __future__ import annotations

from datetime import datetime, tzinfo

__version__: str

def available() -> bool:
    """Return ``True``. Smoke test that the extension was built and loaded."""

def decode_strings(
    buf: bytes,
    pos: int,
    n_rows: int,
) -> tuple[list[str], int]:
    """Walk ``n_rows`` varuint-prefixed UTF-8 strings starting at
    ``buf[pos]``. Returns the list of strings together with the byte
    position right after the last string consumed.

    Raises ``clickhouse_async.protocol.io_sync.BufferUnderflow`` on a
    short buffer — same sentinel the rest of the read path raises.
    """

def decode_datetime(
    buf: bytes,
    n_rows: int,
    tz: tzinfo | None,
) -> list[datetime]:
    """Decode ``n_rows`` UInt32 LE Unix timestamps into ``datetime``
    objects. Pass ``None`` for naive UTC, a ``tzinfo`` for aware.
    """
