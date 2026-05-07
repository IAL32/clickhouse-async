"""Shared helpers for ``tests/perf/`` micro-benchmarks.

The benchmarks operate on synthetic in-memory byte buffers that
match the on-wire format, so each test is hermetic — no server,
no docker, no network. That keeps the numbers reproducible across
laptops and CI runners.
"""

from __future__ import annotations

import struct

from clickhouse_async.protocol.io import _VARUINT_CONTINUATION_BIT


def encode_varuint(value: int) -> bytes:
    """LEB128 encoder mirroring ``BinaryWriter.write_varuint``."""
    out = bytearray()
    while value >= _VARUINT_CONTINUATION_BIT:
        out.append((value & 0x7F) | _VARUINT_CONTINUATION_BIT)
        value >>= 7
    out.append(value)
    return bytes(out)


def build_string_column(n_rows: int, length: int) -> bytes:
    """Body of a String column on the wire: ``[varuint length, body] x n_rows``.

    ``length`` is the length of *every* row's body — keeps the
    benchmark's row count and per-row work decoupled.
    """
    body = b"x" * length
    prefix = encode_varuint(length)
    buf = bytearray()
    for _ in range(n_rows):
        buf.extend(prefix)
        buf.extend(body)
    return bytes(buf)


def build_fixed_int_column(n_rows: int, fmt: str) -> bytes:
    """Body of a fixed-width primitive column. ``fmt`` is a ``struct``
    format char — ``'q'`` for Int64, ``'i'`` for Int32, etc."""
    return struct.pack(f"<{n_rows}{fmt}", *range(n_rows))


def build_block_body(n_rows: int) -> bytes:
    """A whole-block body with mixed types — the same shape the
    1M-row read benchmark uses ``(UInt64, String, DateTime)``.

    Wire layout:

    - BlockInfo: ``varuint 1, byte 0, varuint 2, int32 -1, varuint 0``
    - varuint n_columns (= 3)
    - varuint n_rows
    - per column: name + type-spec (string each) + has_custom byte + body
    """
    buf = bytearray()
    # BlockInfo: is_overflows=False, bucket_num=-1, terminator
    buf.extend(encode_varuint(1))
    buf.append(0)
    buf.extend(encode_varuint(2))
    buf.extend((-1).to_bytes(4, "little", signed=True))
    buf.extend(encode_varuint(0))
    # n_columns, n_rows
    buf.extend(encode_varuint(3))
    buf.extend(encode_varuint(n_rows))

    def column_header(name: str, type_spec: str) -> None:
        name_bytes = name.encode()
        type_bytes = type_spec.encode()
        buf.extend(encode_varuint(len(name_bytes)))
        buf.extend(name_bytes)
        buf.extend(encode_varuint(len(type_bytes)))
        buf.extend(type_bytes)
        buf.append(0)  # has_custom = 0

    # number UInt64
    column_header("number", "UInt64")
    buf.extend(struct.pack(f"<{n_rows}Q", *range(n_rows)))

    # toString(number) — the slow column
    column_header("text", "String")
    for i in range(n_rows):
        s = str(i).encode()
        buf.extend(encode_varuint(len(s)))
        buf.extend(s)

    # now() DateTime — same UInt32 epoch for every row (ClickHouse
    # DateTime is u32 seconds since epoch). DateTime header includes
    # an optional timezone string after the type spec; keep it
    # implicit (no tz) to match the simplest path.
    column_header("ts", "DateTime")
    buf.extend(struct.pack(f"<{n_rows}I", *([1_700_000_000] * n_rows)))

    return bytes(buf)
