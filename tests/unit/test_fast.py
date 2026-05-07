"""Unit tests for the Rust-built ``clickhouse_async._fast`` extension.

The hot decode paths that previously lived in pure Python now call
into Rust. The end-to-end ``test_types_*.py`` suite already covers
those swaps via the public API; this file pins down the Rust
function's edge-case contract directly so a parity bug is named in
isolation rather than buried in a higher-level failure.
"""

from __future__ import annotations

import pytest

from clickhouse_async._fast import decode_strings


def _encode_varuint(buf: bytearray, value: int) -> None:
    """Mirror of ``BinaryWriter.write_varuint`` for assembling test
    fixtures that match the on-wire layout the Rust function expects."""
    while value >= 0x80:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value)


def _wire_encode(rows: list[bytes]) -> bytes:
    """Assemble ``[varuint len, body] x N`` — the on-wire String column body."""
    buf = bytearray()
    for body in rows:
        _encode_varuint(buf, len(body))
        buf.extend(body)
    return bytes(buf)


# ---- decode_strings -----------------------------------------------------


def test_decode_strings_empty_buf_zero_rows_returns_empty_list() -> None:
    # BEGIN / WHEN / THEN: zero rows is the trivial case the codec hits
    #     for an empty result block; must return an empty list and
    #     never raise.
    assert decode_strings(b"", 0) == []


def test_decode_strings_single_ascii_row_round_trips() -> None:
    # BEGIN: one row of 5 ASCII chars
    # WHEN: decoding with n_rows=1
    # THEN: yields the expected single-element list
    assert decode_strings(_wire_encode([b"alpha"]), 1) == ["alpha"]


def test_decode_strings_multiple_mixed_length_rows() -> None:
    # BEGIN: a wire-format buffer packing four rows of varying lengths
    rows = [b"alpha", b"beta", b"", b"bba"]

    # WHEN: decoding
    decoded = decode_strings(_wire_encode(rows), len(rows))

    # THEN: per-row slices come back in declared order; empty row is
    #     the empty string, not a missing entry
    assert decoded == ["alpha", "beta", "", "bba"]


def test_decode_strings_multibyte_utf8_round_trips() -> None:
    # BEGIN: rows holding Cyrillic + emoji + a 4-byte CJK char so the
    #     length-vs-codepoint distinction is exercised
    rows = ["Привет".encode(), "🚀".encode(), "漢字".encode()]

    # WHEN / THEN: each row decodes to its original Python string
    assert decode_strings(_wire_encode(rows), len(rows)) == ["Привет", "🚀", "漢字"]


def test_decode_strings_invalid_utf8_raises_with_row_index() -> None:
    # BEGIN: a body whose second row is not valid UTF-8
    buf = bytearray()
    _encode_varuint(buf, 2)
    buf.extend(b"ok")
    _encode_varuint(buf, 2)
    buf.extend(b"\xff\xfe")  # invalid UTF-8

    # WHEN / THEN: the error message names the offending row index so
    #     a caller can locate the problem column without re-decoding.
    with pytest.raises(ValueError, match="row 1: invalid UTF-8"):
        decode_strings(bytes(buf), 2)


def test_decode_strings_buf_too_short_for_declared_length_raises() -> None:
    # BEGIN: a wire-format buffer whose declared length runs past the
    #     end of ``buf`` (a wire-format mismatch — should never happen
    #     in practice but must be caught)
    buf = bytearray()
    _encode_varuint(buf, 4)
    buf.extend(b"abc")  # only 3 bytes — declared 4

    # WHEN / THEN: ValueError surfaces naming the row, no partial decode
    with pytest.raises(ValueError, match="row 0: needs 4 bytes"):
        decode_strings(bytes(buf), 1)


def test_decode_strings_truncated_varuint_raises_with_row_index() -> None:
    # BEGIN: a buffer whose first varuint never terminates (every byte
    #     has the high bit set)
    buf = bytes([0x80] * 5)  # all continuation bits, no terminator

    # WHEN / THEN: error names the row whose varuint failed to parse
    with pytest.raises(ValueError, match=r"row 0:.*varuint"):
        decode_strings(buf, 1)


def test_decode_strings_zero_length_row_is_empty_string() -> None:
    # BEGIN: a stand-alone zero-length row in the middle of the column
    rows = [b"left", b"", b"right"]

    # WHEN / THEN: the empty row materialises as an empty Python string
    assert decode_strings(_wire_encode(rows), len(rows)) == ["left", "", "right"]


def test_decode_strings_thousand_rows_stress() -> None:
    # BEGIN: a thousand short rows — exercises the inner loop without
    #     stressing memory; catches any per-row leak or corruption
    rows = [f"row-{i:04d}" for i in range(1_000)]
    buf = _wire_encode([r.encode() for r in rows])

    # WHEN / THEN: every row survives byte-for-byte
    assert decode_strings(buf, len(rows)) == rows
