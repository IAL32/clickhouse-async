"""Composite codecs that wrap or combine other codecs.

- ``Nullable(T)`` — 1-byte-per-row null mask, then the inner body.
- ``Array(T)``    — UInt64 cumulative-offsets row, then the inner body
                    holding the flattened values.
- ``Tuple(T1, T2, …)`` — each component's full column body in order
                    (n_rows of T1, then n_rows of T2, …).
- ``Map(K, V)``   — same wire format as ``Array(Tuple(K, V))``.
- ``LowCardinality(T)`` — dictionary-encoded column. See the codec's
                    docstring for the wire layout.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types.base import ColumnCodec


class Nullable:
    """Wraps another codec, prefixing the column body with a 1-byte-per-row
    null mask (``0`` = not null, ``1`` = null).

    The on-wire format requires the inner codec to write *every* row even
    when the row is null, so on writes we substitute ``inner.null_value``
    in place of ``None`` before delegating.
    """

    null_value: None = None

    def __init__(self, inner: ColumnCodec) -> None:
        self.inner = inner
        self.name = f"Nullable({inner.name})"

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[Any]:
        if n_rows == 0:
            return []
        mask = await reader.read_exact(n_rows)
        values = await self.inner.read(reader, n_rows)
        return [None if mask[i] else values[i] for i in range(n_rows)]

    def write(self, writer: BinaryWriter, values: Sequence[Any]) -> None:
        n = len(values)
        if n == 0:
            return
        writer.write_raw(bytes(1 if v is None else 0 for v in values))
        replaced: list[Any] = [
            self.inner.null_value if v is None else v for v in values
        ]
        self.inner.write(writer, replaced)


class Array:
    """``Array(T)`` — cumulative UInt64 offsets followed by the flattened
    inner column.

    The offsets row holds ``n_rows`` UInt64 values; offsets[i] is the
    running total of array lengths from row 0 through row i (inclusive).
    The inner body then holds ``offsets[-1]`` flat values to be sliced
    back into per-row arrays.
    """

    null_value: list[Any]

    def __init__(self, inner: ColumnCodec) -> None:
        self.inner = inner
        self.name = f"Array({inner.name})"
        self.null_value = []

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[list[Any]]:
        if n_rows == 0:
            return []
        # Cumulative offsets — one UInt64 per row.
        offsets_data = await reader.read_exact(8 * n_rows)
        offsets = [
            int.from_bytes(offsets_data[i * 8 : (i + 1) * 8], "little", signed=False)
            for i in range(n_rows)
        ]
        total = offsets[-1]
        flat = await self.inner.read(reader, total)
        out: list[list[Any]] = []
        prev = 0
        for end in offsets:
            out.append(list(flat[prev:end]))
            prev = end
        return out

    def write(self, writer: BinaryWriter, values: Sequence[Sequence[Any]]) -> None:
        n = len(values)
        if n == 0:
            return
        # Cumulative offsets.
        offsets = bytearray()
        running = 0
        for v in values:
            running += len(v)
            offsets.extend(running.to_bytes(8, "little", signed=False))
        writer.write_raw(bytes(offsets))
        # Flat inner body.
        flat: list[Any] = [item for row in values for item in row]
        self.inner.write(writer, flat)


class Tuple:
    """``Tuple(T1, T2, …)`` — each component's full column body in order.

    The wire layout is *not* row-major; each component contributes its
    ``n_rows``-long body sequentially. We read each component's column,
    then ``zip`` them into Python tuples.

    ClickHouse also supports a *named* form, ``Tuple(id Int32, name
    String)``. Names live only in the type-spec string carried by the
    block header, never in the column body — wire-format-wise the
    named and unnamed forms are identical. Pass ``names=...`` to
    construct a named tuple; a future Client could surface those names
    as a ``NamedTuple`` row representation, but for now ``read()``
    still yields plain ``tuple`` values.
    """

    null_value: tuple[Any, ...]

    def __init__(
        self,
        *components: ColumnCodec,
        names: tuple[str, ...] | None = None,
    ) -> None:
        if not components:
            raise ValueError("Tuple requires at least one component")
        if names is not None and len(names) != len(components):
            raise ValueError(
                f"Tuple names length ({len(names)}) must match components "
                f"length ({len(components)})"
            )
        self.components = components
        self.names = names
        if names is None:
            self.name = f"Tuple({', '.join(c.name for c in components)})"
        else:
            self.name = "Tuple({})".format(
                ", ".join(
                    f"{n} {c.name}" for n, c in zip(names, components, strict=True)
                )
            )
        self.null_value = tuple(c.null_value for c in components)

    @property
    def named(self) -> bool:
        """``True`` iff this Tuple was constructed with field names."""
        return self.names is not None

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[tuple[Any, ...]]:
        if n_rows == 0:
            return []
        columns: list[list[Any]] = []
        for component in self.components:
            columns.append(await component.read(reader, n_rows))
        return [
            tuple(columns[c][i] for c in range(len(self.components)))
            for i in range(n_rows)
        ]

    def write(self, writer: BinaryWriter, values: Sequence[Sequence[Any]]) -> None:
        n = len(values)
        if n == 0:
            return
        for c, component in enumerate(self.components):
            component.write(writer, [row[c] for row in values])


class Map:
    """``Map(K, V)`` — same wire format as ``Array(Tuple(K, V))``.

    The Python representation is a ``dict[K, V]`` per row. Repeated keys
    in the on-wire payload are not preserved by the dict conversion;
    ClickHouse Map columns aren't supposed to contain duplicate keys.
    """

    null_value: dict[Any, Any]

    def __init__(self, key: ColumnCodec, value: ColumnCodec) -> None:
        self.key = key
        self.value = value
        self.name = f"Map({key.name}, {value.name})"
        # Reuse Array(Tuple(K, V)) for the actual wire work.
        self._inner: Array = Array(Tuple(key, value))
        self.null_value = {}

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[dict[Any, Any]]:
        rows = await self._inner.read(reader, n_rows)
        return [dict(row) for row in rows]

    def write(self, writer: BinaryWriter, values: Sequence[dict[Any, Any]]) -> None:
        rows = [list(d.items()) for d in values]
        self._inner.write(writer, rows)


class LowCardinality:
    """``LowCardinality(T)`` — dictionary-encoded column.

    Wire layout (per upstream ``SerializationLowCardinality``):

    1. ``UInt64`` key version. Currently always ``1``
       (``SharedDictionariesWithAdditionalKeys``) — the only key
       version ClickHouse currently accepts.
    2. ``UInt64`` serialization-type bitfield. Low byte is the index
       width tag (``0``=UInt8, ``1``=UInt16, ``2``=UInt32, ``3``=UInt64);
       bits ``0x600`` are ``HasAdditionalKeysBit | NeedUpdateDictionary``
       which we always set on writes.
    3. ``UInt64`` dictionary size.
    4. Dictionary body — ``dict_size`` rows.
    5. ``UInt64`` indices count (must equal ``n_rows``).
    6. Indices body — ``n_rows`` little-endian unsigned ints at the
       declared width.

    ``LowCardinality(Nullable(T))`` reuses the same wire skeleton with
    a single twist: dictionary index 0 is **reserved for null** and the
    dictionary body is encoded at the *unwrapped* ``T`` (not at
    ``Nullable(T)``). Indices in the data stream point at 0 for null
    rows and 1+ for the corresponding ``T`` value. The dictionary block
    therefore prepends a placeholder (the unwrapped codec's
    ``null_value``) at slot 0; that slot is never referenced by
    non-null rows, so the placeholder's exact byte representation is
    irrelevant — only its presence matters for offset accounting.
    """

    null_value: Any

    # Per upstream ``KeysSerializationVersion``, ``1`` =
    # ``SharedDictionariesWithAdditionalKeys`` — the only version
    # ClickHouse currently accepts.
    _VERSION = 1
    # Bit 9 (``HasAdditionalKeysBit``, 0x200) + bit 10
    # (``NeedUpdateDictionary``, 0x400). These bit positions come
    # from upstream ``IndexesSerializationType``: bit 8 is the
    # global-dict flag (server-only), bit 9 is HasAdditionalKeys,
    # bit 10 is NeedUpdateDictionary. The low byte is the index-width
    # tag and gets OR'd in per write.
    _SERIALIZATION_BASE = 0x0000_0000_0000_0600

    def __init__(self, inner: ColumnCodec) -> None:
        self.inner = inner
        # Detect Nullable inner so the read/write paths can switch to
        # the null-at-index-0 layout. Flag is load-bearing for both
        # branches; we never inspect ``isinstance(inner, Nullable)``
        # again past construction.
        self._inner_is_nullable: bool = isinstance(inner, Nullable)
        self.name = f"LowCardinality({inner.name})"
        # When the inner is Nullable, the column's null sentinel is the
        # Python ``None`` (matches the wire's index-0 mapping). Otherwise
        # we surface the inner type's own null_value so callers that
        # default-fill a column see something type-correct.
        self.null_value = None if self._inner_is_nullable else inner.null_value

    @staticmethod
    def _index_tag_for_size(dict_size: int) -> tuple[int, int]:
        """Return ``(tag, byte_width)`` for the smallest unsigned int that
        can index ``dict_size`` entries."""
        if dict_size <= 2**8:
            return 0, 1
        if dict_size <= 2**16:
            return 1, 2
        if dict_size <= 2**32:
            return 2, 4
        return 3, 8

    @staticmethod
    def _byte_width_for_tag(tag: int) -> int:
        return {0: 1, 1: 2, 2: 4, 3: 8}[tag]

    def _dictionary_codec(self) -> ColumnCodec:
        """Codec used for the dictionary body on the wire.

        For ``LowCardinality(Nullable(T))`` the dictionary stores the
        unwrapped ``T`` — the null mapping is implicit at index 0 and
        not encoded in the body itself.
        """
        if self._inner_is_nullable:
            # mypy / ty know self.inner is a ColumnCodec; the inner of
            # a Nullable is itself a ColumnCodec. Cast keeps types tight.
            return cast("Nullable", self.inner).inner
        return self.inner

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[Any]:
        if n_rows == 0:
            return []
        version = await reader.read_int(8, signed=False)
        if version != self._VERSION:
            raise ProtocolError(f"unsupported LowCardinality version: {version}")
        sertype = await reader.read_int(8, signed=False)
        index_tag = sertype & 0xFF
        if index_tag not in (0, 1, 2, 3):
            raise ProtocolError(f"invalid LowCardinality index tag: {index_tag}")
        index_size = self._byte_width_for_tag(index_tag)

        dict_size = await reader.read_int(8, signed=False)
        dict_body = await self._dictionary_codec().read(reader, dict_size)
        # Map slot 0 to None so a downstream lookup with index 0 yields
        # null. The placeholder bytes the server wrote at slot 0 are
        # ignored — we never expose them.
        dictionary: list[Any] = (
            [None, *dict_body[1:]] if self._inner_is_nullable else dict_body
        )

        idx_count = await reader.read_int(8, signed=False)
        if idx_count != n_rows:
            raise ProtocolError(
                f"LowCardinality indices count {idx_count} != n_rows {n_rows}"
            )
        idx_data = await reader.read_exact(index_size * n_rows)
        return [
            dictionary[
                int.from_bytes(
                    idx_data[i * index_size : (i + 1) * index_size],
                    "little",
                    signed=False,
                )
            ]
            for i in range(n_rows)
        ]

    def write(self, writer: BinaryWriter, values: Sequence[Any]) -> None:
        n = len(values)
        if n == 0:
            return

        dictionary, indices = self._build_dictionary(values)

        index_tag, index_size = self._index_tag_for_size(len(dictionary))
        sertype = self._SERIALIZATION_BASE | index_tag

        writer.write_int(self._VERSION, 8, signed=False)
        writer.write_int(sertype, 8, signed=False)
        writer.write_int(len(dictionary), 8, signed=False)
        self._dictionary_codec().write(writer, dictionary)
        writer.write_int(n, 8, signed=False)
        idx_buf = bytearray()
        for i in indices:
            idx_buf.extend(i.to_bytes(index_size, "little", signed=False))
        writer.write_raw(bytes(idx_buf))

    def _build_dictionary(self, values: Sequence[Any]) -> tuple[list[Any], list[int]]:
        """Deduplicate ``values`` in first-seen order and return
        ``(dictionary, per_row_indices)`` for the wire encoding.

        For ``LowCardinality(Nullable(T))``, dictionary slot 0 is
        reserved for null: ``None`` rows map to index 0, non-null
        values dedupe into ``[1, 1+k)``. The slot-0 placeholder is the
        unwrapped codec's ``null_value`` — it travels through the
        wire to keep offsets aligned but is never indexed by a non-null
        row.
        """
        if self._inner_is_nullable:
            placeholder = self._dictionary_codec().null_value
            seen: dict[Any, int] = {}
            dictionary: list[Any] = [placeholder]
            indices: list[int] = []
            for v in values:
                if v is None:
                    indices.append(0)
                    continue
                idx = seen.get(v)
                if idx is None:
                    idx = len(dictionary)
                    seen[v] = idx
                    dictionary.append(v)
                indices.append(idx)
            return dictionary, indices

        seen = {}
        dictionary = []
        indices = []
        for v in values:
            idx = seen.get(v)
            if idx is None:
                idx = len(dictionary)
                seen[v] = idx
                dictionary.append(v)
            indices.append(idx)
        return dictionary, indices
