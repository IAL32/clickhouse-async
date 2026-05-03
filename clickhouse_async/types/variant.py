"""Codecs for ``Variant(T1, T2, …)`` and ``Dynamic[(max_types=N)]``.

Both are ClickHouse 24.x tagged unions. ``Variant`` is *closed* — the
arms are declared up front and the discriminator is a small unsigned
integer indexing into them. ``Dynamic`` is *open* — each block's
active arms are declared in a per-block prefix, then encoded via the
same Variant body.

Wire format (mirrors upstream
``SerializationVariant::serializeBinaryBulk{StatePrefix,
WithMultipleStreams}`` in mode ``BASIC = 0``):

| Bytes                    | Meaning                                                |
| ------------------------ | ------------------------------------------------------ |
| 8                        | discriminator-stream version (always ``0`` here)       |
| ``n_rows`` x 1           | per-row discriminator: ``0..k-1`` for an active arm,   |
|                          | ``0xFF`` (``NULL_DISCRIMINATOR``) for NULL             |
| per-arm bodies           | each arm's column body sliced to the rows whose        |
|                          | discriminator matches its index, in declared order     |

``Dynamic`` prepends a per-block prefix:

| Bytes                                | Meaning                                |
| ------------------------------------ | -------------------------------------- |
| 8                                    | dynamic-stream version (always ``0``)  |
| varuint                              | ``n_active_types`` for this block      |
| ``n_active_types`` length-prefixed   | type-spec strings, parsed via          |
|                                      | ``parse_type``                         |
| (then the Variant payload above)     |                                        |

Python representation: a row is the value itself — the discriminator
is implicit in the Python type. ``None`` maps to ``NULL``. When the
declared arms are ambiguous for a given Python value (``int`` matches
both ``Int32`` and ``Int64``), the *first declared* arm wins; pin a
specific arm with ``Variant.tag(value, type_index)``. ``Dynamic``
exposes a parallel ``Dynamic.tag(value, type_spec)`` helper for cases
where Python-type inference would pick the wrong ClickHouse type.
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import ipaddress as _ip
import uuid as _uuid
from typing import TYPE_CHECKING, Any, ClassVar

from clickhouse_async.errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
    from clickhouse_async.types.base import ColumnCodec


# ``BASIC`` discriminator-stream mode. ``COMPACT`` (mode 1) skips the
# per-row discriminator stream when an entire block belongs to a
# single arm — we don't emit it on writes; if the server emits it on
# reads we surface the version with a clear error rather than silently
# misinterpreting bytes.
_VARIANT_VERSION_BASIC = 0
# Reserved discriminator marking a NULL row. Upstream defines this
# as ``ColumnVariant::NULL_DISCRIMINATOR``.
_NULL_DISCRIMINATOR = 0xFF
# Per upstream ``ColumnVariant::MAX_NESTED_COLUMNS``. Variants beyond
# this don't fit in a single discriminator byte (the slot at 0xFF is
# reserved for NULL, leaving 0..254 for arms).
_MAX_VARIANTS = 255

# Dynamic's top-level structure version. Upstream declares this in
# ``SerializationDynamic::DynamicSerializationVersion`` — ``V1 = 1``
# is the legacy form (writes ``max_dynamic_types`` then
# ``num_dynamic_types`` to the wire); ``V2 = 2`` drops the duplicate
# and is the default ClickHouse 24.x emits. We accept both on read
# and always emit V2 on write.
_DYNAMIC_VERSION_V1 = 1
_DYNAMIC_VERSION_V2 = 2

# ClickHouse's default ``max_dynamic_types`` for a ``Dynamic`` column.
# Upstream constant: ``ColumnDynamic::DEFAULT_MAX_DYNAMIC_TYPES``. Used
# in the V1 wire format (the slot is a column-policy hint, not a wire
# constraint — the server's reader just skips it past — but we still
# emit a sensible default so wire dumps don't have garbage in that
# field).
_DEFAULT_MAX_DYNAMIC_TYPES = 32

# Every ``Dynamic`` column on the wire silently carries one extra
# variant arm at the end — ``SharedVariant`` (a ``String``) — used
# server-side to spill values whose type exceeds ``max_dynamic_types``.
# Upstream omits it from the declared types list (``num_dynamic_types
# = variant_names.size() - 1``) but always *deserializes* with it
# appended, so an inner ``Variant`` of a Dynamic actually has
# ``num_dynamic_types + 1`` arms. We never use SharedVariant on
# writes (always declare the actual type), so its on-wire body is
# always 0 rows; we only need it on the read path so the inner
# ``Variant._read_body`` knows the arm count.
_SHARED_VARIANT_TYPE_SPEC = "String"


class _VariantTag:
    """Sentinel wrapper produced by ``Variant.tag(value, type_index)``.

    Holds a value alongside an explicit arm index; the codec's resolver
    sees the wrapper and skips Python-type matching, picking the arm
    the caller asked for. Sub-class of ``object`` only — never seen by
    user code unless they unwrap it themselves."""

    __slots__ = ("type_index", "value")

    def __init__(self, value: object, type_index: int) -> None:
        self.value = value
        self.type_index = type_index

    def __repr__(self) -> str:
        return f"Variant.tag({self.value!r}, {self.type_index})"


class _DynamicTag:
    """Sentinel wrapper produced by ``Dynamic.tag(value, type_spec)``.

    Holds a value with an explicit ClickHouse type spec to be used as
    its arm in the block's dynamic type list. Necessary when Python
    type inference (``int → Int64``, ``str → String`` …) would pick
    the wrong arm — e.g. forcing a ``Date`` instead of letting a
    ``datetime.date`` collapse into ``DateTime``."""

    __slots__ = ("type_spec", "value")

    def __init__(self, value: object, type_spec: str) -> None:
        self.value = value
        self.type_spec = type_spec

    def __repr__(self) -> str:
        return f"Dynamic.tag({self.value!r}, {self.type_spec!r})"


class Variant:
    """``Variant(T1, T2, …)`` — closed tagged union (up to 255 arms).

    Construct directly with ``Variant(Int64(), String())`` or via the
    parser (``parse_type("Variant(Int64, String)")``). On read each
    row is the value itself (``None`` for NULL); the discriminator is
    implicit in the Python type. On write, the codec resolves the
    arm via ``isinstance`` against each arm's ``python_type`` in
    declared order — first match wins. Use ``Variant.tag(v, i)`` to
    force arm ``i`` when inference picks the wrong one.
    """

    null_value: ClassVar[None] = None
    python_type: ClassVar[type] = object

    def __init__(self, *components: ColumnCodec) -> None:
        if not components:
            raise ValueError("Variant requires at least one component")
        if len(components) > _MAX_VARIANTS:
            raise ValueError(
                f"Variant supports up to {_MAX_VARIANTS} components "
                f"(0xFF is reserved for NULL); got {len(components)}"
            )
        self.components = components
        self.name = f"Variant({', '.join(c.name for c in components)})"

    @staticmethod
    def tag(value: object, type_index: int) -> _VariantTag:
        """Force a particular arm for ``value`` regardless of how
        Python-type inference would resolve it.

        Use when several arms share a Python representation
        (``Int32`` and ``Int64`` both surface ``int``) and the
        first-declared default is wrong for this row.
        """
        return _VariantTag(value, type_index)

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[Any]:
        if n_rows == 0:
            return []
        version = await reader.read_int(8, signed=False)
        if version != _VARIANT_VERSION_BASIC:
            raise ProtocolError(
                f"unsupported Variant discriminator-stream version {version}; "
                f"only BASIC (0) is supported"
            )
        return await self._read_body(reader, n_rows, self.components)

    @staticmethod
    async def _read_body(
        reader: AsyncBinaryReader,
        n_rows: int,
        components: Sequence[ColumnCodec],
    ) -> list[Any]:
        """Read the body (``n_rows`` discriminators + per-arm column
        bodies) for a Variant whose mode/version byte has already been
        consumed.

        Shared between ``Variant.read`` and ``Dynamic.read`` so the
        Variant inner reuses the same path — Dynamic just hands in the
        per-block-resolved component list.
        """
        disc = await reader.read_exact(n_rows)
        # Count rows per arm in declared order so we know how many
        # values to ask each arm's codec for.
        counts = [0] * len(components)
        for d in disc:
            if d == _NULL_DISCRIMINATOR:
                continue
            if d >= len(components):
                raise ProtocolError(
                    f"Variant discriminator {d} out of range for "
                    f"{len(components)} arms at offset {reader.position}"
                )
            counts[d] += 1
        bodies = [
            await component.read(reader, counts[i])
            for i, component in enumerate(components)
        ]
        # Reassemble row order: walk discriminators, pull next value
        # from the matching arm's body queue.
        cursors = [0] * len(components)
        out: list[Any] = []
        for d in disc:
            if d == _NULL_DISCRIMINATOR:
                out.append(None)
            else:
                out.append(bodies[d][cursors[d]])
                cursors[d] += 1
        return out

    def write(self, writer: BinaryWriter, values: Sequence[Any]) -> None:
        if not values:
            return
        # Resolve each row's (discriminator, payload) pair. NULL
        # rows keep ``None`` as payload — variant codecs never see
        # them since their per-arm body skips NULL slots entirely.
        rows = [self._resolve(v) for v in values]
        writer.write_int(_VARIANT_VERSION_BASIC, 8, signed=False)
        self._write_body(writer, self.components, rows)

    @staticmethod
    def _write_body(
        writer: BinaryWriter,
        components: Sequence[ColumnCodec],
        rows: Sequence[tuple[int, Any]],
    ) -> None:
        """Emit ``n_rows`` discriminators + per-arm column bodies for
        a sequence of pre-resolved ``(disc, payload)`` rows.

        Shared between ``Variant.write`` and ``Dynamic.write`` —
        Dynamic resolves arms per-block from the values themselves and
        hands the resolved rows in here so the wire shape is identical
        below the prefix.
        """
        n = len(rows)
        discriminators = bytearray(n)
        per_arm: list[list[Any]] = [[] for _ in components]
        for i, (disc, payload) in enumerate(rows):
            discriminators[i] = disc
            if disc != _NULL_DISCRIMINATOR:
                per_arm[disc].append(payload)
        writer.write_raw(bytes(discriminators))
        for component, payload in zip(components, per_arm, strict=True):
            component.write(writer, payload)

    def _resolve(self, value: object) -> tuple[int, Any]:
        """Return ``(discriminator, payload)`` for one row.

        ``None`` → NULL discriminator. ``_VariantTag`` pins the arm
        explicitly. Otherwise the row is matched against each arm's
        ``python_type`` via ``isinstance`` in declared order — first
        match wins.
        """
        if value is None:
            return _NULL_DISCRIMINATOR, None
        if isinstance(value, _VariantTag):
            if not 0 <= value.type_index < len(self.components):
                raise ValueError(
                    f"Variant.tag type_index {value.type_index} out of range "
                    f"for {len(self.components)} arms in {self.name}"
                )
            return value.type_index, value.value
        for i, component in enumerate(self.components):
            pt = getattr(component, "python_type", None)
            if pt is not None and isinstance(value, pt):
                return i, value
        raise ValueError(
            f"no arm in {self.name} matches Python type "
            f"{type(value).__name__!r}; tag explicitly with "
            f"Variant.tag(value, type_index)"
        )


class Dynamic:
    """``Dynamic[(max_types=N)]`` — open tagged union.

    Each block carries its own list of active types in a prefix, so
    the on-wire arms can vary block-to-block. Python rows are values
    themselves (``None`` for NULL); on writes, the codec collects
    each value's inferred (or tagged) ClickHouse type spec, builds a
    per-block arm list, and writes the block as a Variant body.

    ``max_types`` is informational (matches upstream's
    ``Dynamic(max_types=N)`` cap on how many active arms a block may
    declare). The codec doesn't enforce it on reads — the server is
    the source of truth — but raises on writes if the inferred arm
    count exceeds the cap (when set).

    .. note::

        v0.2 caveat: ``Dynamic`` round-trips with itself in unit tests
        but does not yet pass real-server INSERT round-trips on
        ClickHouse 24.8 LTS. The wire format (V1 ``8B version + varuint
        max_dynamic_types + varuint num_dynamic_types + per-arm type
        names + 8B variant mode + n_rows discriminators + per-arm
        bodies + implicit ``SharedVariant`` (String) tail-arm`) is
        believed correct from upstream source-reading but a real
        24.8.14 server still rejects writes with "Unknown type code:
        0x68" for reasons we haven't pinned down. Use only inside
        unit tests that exercise the codec against itself; real-server
        ``Dynamic`` and ``JSON`` (which is built on Dynamic) round-trips
        are tracked as v0.3 follow-ups in ``TODO.md``.
    """

    null_value: ClassVar[None] = None
    python_type: ClassVar[type] = object

    def __init__(self, max_types: int | None = None) -> None:
        if max_types is not None and max_types <= 0:
            raise ValueError(f"Dynamic max_types must be positive; got {max_types}")
        self.max_types = max_types
        self.name = (
            f"Dynamic(max_types={max_types})" if max_types is not None else "Dynamic"
        )

    @staticmethod
    def tag(value: object, type_spec: str) -> _DynamicTag:
        """Pin a ClickHouse type spec for ``value`` instead of relying
        on the Python-type → type-spec inference. Necessary when the
        default mapping (``int → Int64``, ``str → String`` …) picks
        the wrong arm — e.g. forcing ``Date`` over ``DateTime`` for a
        ``datetime.date`` value.
        """
        return _DynamicTag(value, type_spec)

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[Any]:
        if n_rows == 0:
            return []
        # ``parse_type`` lives in ``types/__init__.py`` which already
        # imports this module — a top-level import here would create a
        # circular import. Deferring it to first call is intentional;
        # PLC0415 is silenced for that reason only.
        from clickhouse_async.types import parse_type  # noqa: PLC0415

        version = await reader.read_int(8, signed=False)
        if version not in (_DYNAMIC_VERSION_V1, _DYNAMIC_VERSION_V2):
            raise ProtocolError(
                f"unsupported Dynamic structure version {version}; "
                f"only V1 ({_DYNAMIC_VERSION_V1}) and V2 ({_DYNAMIC_VERSION_V2}) "
                f"are supported"
            )
        if version == _DYNAMIC_VERSION_V1:
            # V1 emits ``max_dynamic_types`` before the actual count; we
            # ignore the cap (server-side policy, not a wire constraint).
            await reader.read_varuint()
        n_types = await reader.read_varuint()
        components: list[ColumnCodec] = [
            parse_type(await reader.read_string()) for _ in range(n_types)
        ]
        # Append the implicit ``SharedVariant`` arm before reading the
        # inner Variant body. Upstream ``ColumnDynamic`` always carries
        # this arm at the tail (used to spill rare types over
        # ``max_dynamic_types``); the wire format hides it from the
        # declared list but the inner Variant *does* count it as an
        # arm and reserves a discriminator slot for it.
        components.append(parse_type(_SHARED_VARIANT_TYPE_SPEC))
        # The Variant payload carries its own state-prefix version byte.
        inner_version = await reader.read_int(8, signed=False)
        if inner_version != _VARIANT_VERSION_BASIC:
            raise ProtocolError(
                f"unsupported Dynamic Variant version {inner_version}; "
                f"only BASIC (0) is supported"
            )
        return await Variant._read_body(reader, n_rows, components)

    def write(self, writer: BinaryWriter, values: Sequence[Any]) -> None:
        if not values:
            return
        # Same circular-import deferral as ``read``; PLC0415 silenced
        # for that reason only.
        from clickhouse_async.types import parse_type  # noqa: PLC0415

        # Walk values once: pick a type spec per row (explicit tag or
        # inferred from Python type), accumulate active specs in
        # first-seen order, and remember each row's (disc, payload).
        type_specs: list[str] = []
        spec_to_idx: dict[str, int] = {}
        rows: list[tuple[int, Any]] = []
        for v in values:
            if v is None:
                rows.append((_NULL_DISCRIMINATOR, None))
                continue
            if isinstance(v, _DynamicTag):
                spec = v.type_spec
                payload: Any = v.value
            else:
                spec = _infer_dynamic_type_spec(v)
                payload = v
            idx = spec_to_idx.get(spec)
            if idx is None:
                idx = len(type_specs)
                spec_to_idx[spec] = idx
                type_specs.append(spec)
            rows.append((idx, payload))

        if self.max_types is not None and len(type_specs) > self.max_types:
            raise ValueError(
                f"Dynamic block has {len(type_specs)} active types but "
                f"max_types is {self.max_types}; tag values to collapse arms "
                f"or raise the cap"
            )

        # Write V1: server-side ``DynamicSerializationVersion::checkVersion``
        # accepts both V1 and V2, but ClickHouse 24.8 LTS gates V2 behind
        # ``DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION`` (54473).
        # Our negotiated revision (54469) is below that, so the server's
        # codec is configured for V1; we mirror that on writes for
        # symmetry. V1 layout (per upstream
        # ``SerializationDynamic::serializeBinaryBulkStatePrefix``):
        # ``8B version + varuint max_dynamic_types + varuint
        # num_dynamic_types + n type-spec strings``. The reader skips the
        # ``max_dynamic_types`` slot (it's a column-policy hint, not a
        # wire constraint), so we just write our cap when the user set
        # one and the default ``32`` otherwise.
        writer.write_int(_DYNAMIC_VERSION_V1, 8, signed=False)
        writer.write_varuint(len(type_specs))
        writer.write_varuint(len(type_specs))
        for spec in type_specs:
            writer.write_string(spec)
        # Append the implicit ``SharedVariant`` arm so the inner
        # ``Variant._write_body`` reserves a per-arm body slot for it.
        # Our writes never assign to SharedVariant — its body is always
        # 0 rows — but the inner Variant still needs to know it exists
        # to match the discriminator stream the server expects.
        components = [parse_type(spec) for spec in type_specs]
        components.append(parse_type(_SHARED_VARIANT_TYPE_SPEC))
        # The Variant body carries its own version + discriminators +
        # per-arm bodies. An all-NULL block still emits both versions
        # plus the n_rows x 0xFF discriminator stream.
        writer.write_int(_VARIANT_VERSION_BASIC, 8, signed=False)
        Variant._write_body(writer, components, rows)


# ---- Python-type → ClickHouse-type inference for Dynamic writes ---------


def _infer_dynamic_type_spec(value: object) -> str:
    """Map a Python value to its default ClickHouse type spec for a
    ``Dynamic`` block-prefix declaration.

    Conservative: covers the common scalar shapes only. ``bool`` is
    checked before ``int`` (Python's ``bool`` is a subclass of ``int``,
    so the order matters). Anything outside this table raises with a
    pointer to ``Dynamic.tag`` for explicit typing.
    """
    if isinstance(value, bool):
        return "Bool"
    if isinstance(value, int):
        return "Int64"
    if isinstance(value, float):
        return "Float64"
    if isinstance(value, str):
        return "String"
    if isinstance(value, bytes):
        return "String"
    if isinstance(value, _dt.datetime):
        return "DateTime"
    if isinstance(value, _dt.date):
        return "Date"
    if isinstance(value, _uuid.UUID):
        return "UUID"
    if isinstance(value, _ip.IPv4Address):
        return "IPv4"
    if isinstance(value, _ip.IPv6Address):
        return "IPv6"
    if isinstance(value, _decimal.Decimal):
        # Without a precision/scale hint we can't pick a Decimal width;
        # force the caller to tag explicitly.
        raise ValueError(
            f"cannot infer Dynamic type for Decimal {value!r}; tag with "
            f"Dynamic.tag(value, 'Decimal(P, S)')"
        )
    raise ValueError(
        f"cannot infer Dynamic type for Python value of type "
        f"{type(value).__name__!r}; tag with Dynamic.tag(value, type_spec)"
    )
