"""Force-fallback tests for the codecs that route through ``_fast_read``.

When the C extension is built, the unit suite naturally exercises the
fast path — the pure-Python branches in ``String.read`` and
``DateTime.read`` are skipped, dragging coverage. These tests
monkey-patch ``_fast.module`` to ``None`` for the duration of one
codec call so the pure-Python implementation runs against the same
inputs the rest of the suite already validates.

The point isn't to re-validate correctness (the codec tests do that
on whichever path is active); it's to make sure the fallback path
stays compiled and tested even on a developer machine where the C
extension is present.
"""

from __future__ import annotations

import struct
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from clickhouse_async import _fast
from clickhouse_async.protocol.io_sync import BufferUnderflow, SyncBinaryReader
from clickhouse_async.types.datetime import DateTime
from clickhouse_async.types.string import String


@pytest.fixture
def _force_pure_python(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hide ``_fast.module`` for the duration of a test so the codec
    pure-Python fallback runs."""
    monkeypatch.setattr(_fast, "module", None)


def _encode_varuint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


# ---- String.read pure-Python fallback ----------------------------------


@pytest.mark.usefixtures("_force_pure_python")
async def test_string_read_pure_python_decodes_short_strings() -> None:
    # BEGIN: a buffer with three varuint-prefixed UTF-8 strings
    payload = bytearray()
    for s in ("alpha", "beta", "gamma"):
        body = s.encode("utf-8")
        payload.extend(_encode_varuint(len(body)))
        payload.extend(body)

    # WHEN: reading via the pure-Python path
    rows = String().read(SyncBinaryReader(bytes(payload)), 3)

    # THEN: every row matches and `_fast.module` was actually None
    assert rows == ["alpha", "beta", "gamma"]
    assert _fast.module is None


@pytest.mark.usefixtures("_force_pure_python")
async def test_string_read_pure_python_handles_multibyte_varuint() -> None:
    # BEGIN: a single string with a body length that needs two varuint bytes
    body = b"x" * 200  # 200 > 127 forces a 2-byte varuint
    payload = _encode_varuint(len(body)) + body

    # WHEN: reading via the pure-Python path
    rows = String().read(SyncBinaryReader(payload), 1)

    # THEN: the multi-byte varuint walks correctly
    assert rows == ["x" * 200]


@pytest.mark.usefixtures("_force_pure_python")
async def test_string_read_pure_python_raises_buffer_underflow_mid_varuint() -> None:
    # BEGIN: a buffer that ends mid-varuint (continuation bit set, no follow-up)
    payload = bytes([0x80])

    # WHEN: reading the pure-Python path
    # THEN: BufferUnderflow surfaces with the same shape the C path raises
    with pytest.raises(BufferUnderflow):
        String().read(SyncBinaryReader(payload), 1)


@pytest.mark.usefixtures("_force_pure_python")
async def test_string_read_pure_python_raises_buffer_underflow_short_body() -> None:
    # BEGIN: a varuint claiming 100 body bytes but only 5 are present
    payload = _encode_varuint(100) + b"hello"

    # WHEN / THEN: short body raises BufferUnderflow with `needed=100`
    with pytest.raises(BufferUnderflow) as exc_info:
        String().read(SyncBinaryReader(payload), 1)
    assert exc_info.value.needed == 100
    assert exc_info.value.available == 5


# ---- DateTime.read pure-Python fallback --------------------------------


@pytest.mark.usefixtures("_force_pure_python")
async def test_datetime_read_pure_python_naive() -> None:
    # BEGIN: three UInt32 LE timestamps and a naive DateTime codec
    timestamps = [1_700_000_000, 1_700_000_060, 1_700_000_120]
    payload = struct.pack(f"<{len(timestamps)}I", *timestamps)
    codec = DateTime()  # naive

    # WHEN: reading via the pure-Python path
    rows = codec.read(SyncBinaryReader(payload), len(timestamps))

    # THEN: every row decodes to the matching naive UTC datetime
    expected = [
        datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None) for ts in timestamps
    ]
    assert rows == expected
    assert all(r.tzinfo is None for r in rows)


@pytest.mark.usefixtures("_force_pure_python")
async def test_datetime_read_pure_python_aware() -> None:
    # BEGIN: a DateTime('Europe/Madrid') codec and one timestamp
    timestamps = [1_700_000_000]
    payload = struct.pack(f"<{len(timestamps)}I", *timestamps)
    codec = DateTime("Europe/Madrid")
    madrid = ZoneInfo("Europe/Madrid")

    # WHEN: reading via the pure-Python path
    rows = codec.read(SyncBinaryReader(payload), len(timestamps))

    # THEN: the value is aware and represents the same instant
    assert len(rows) == 1
    assert rows[0].tzinfo is not None
    assert rows[0] == datetime.fromtimestamp(timestamps[0], tz=madrid)
