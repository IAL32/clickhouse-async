"""Codec for ClickHouse 24.x's ``JSON`` type.

ClickHouse's new JSON column stores schema-on-read documents:
each unique JSON path becomes its own ``SerializationDynamic``
sub-column server-side, plus a single shared ``Array(Tuple(String,
String))`` substream that holds rare paths spilled outside the
``max_dynamic_paths`` limit. The wire format reflects that
sub-column model — the column body is a concatenation of:

| Bytes                | Meaning                                                |
| -------------------- | ------------------------------------------------------ |
| 8                    | ``ObjectSerializationVersion`` (``V1 = 0`` at our      |
|                      | negotiated revision; ``V2 = 2`` once we negotiate      |
|                      | ``DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON``)        |
| varuint              | (V1 only) ``max_dynamic_paths`` — column-policy hint   |
|                      | the reader skips past                                  |
| varuint              | ``num_dynamic_paths`` — number of declared paths in    |
|                      | this block                                             |
| n length-prefixed    | sorted dotted path names (``user``, ``user.id``, …)    |
| (then each path's ``Dynamic`` body in *path-name-sorted* order — see          |
|  ``types/variant.py::Dynamic`` for the per-Dynamic layout. Each path is its   |
|  own self-contained Dynamic sub-column with version + max_dynamic_types +     |
|  num_dynamic_types + type names + Variant body)                               |
| (then the shared data column: an ``Array(Tuple(String, String))`` body for    |
|  ``n_rows``. Empty on our writes — we never spill paths to shared.)           |

Python representation: each row is a flat ``dict[str, Any]`` keyed by
the dotted path. We don't auto-nest on reads or auto-flatten nested
dicts on writes — callers can wrap that ergonomics layer themselves;
keeping the codec's representation flat means a row's path set is
exactly what the server materialised, and round-tripping is byte
loss-less. ``codec.name`` round-trips ``JSON`` and ``JSON(hint, …)``
specs verbatim so server-emitted block headers can be re-INSERTed.
"""

from __future__ import annotations

import json as _json
from collections import Counter
from typing import TYPE_CHECKING, Any, ClassVar

from clickhouse_async.errors import ProtocolError
from clickhouse_async.types._parser import _JSONHint, parse_type
from clickhouse_async.types.variant import (
    _DEFAULT_MAX_DYNAMIC_TYPES,
    _DYNAMIC_VERSION_V1,
    _DYNAMIC_VERSION_V2,
    _NULL_DISCRIMINATOR,
    _SHARED_VARIANT_NAME,
    _SHARED_VARIANT_TYPE_SPEC,
    _VARIANT_VERSION_BASIC,
    Variant,
    _DynamicTag,
    _infer_dynamic_type_spec,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


# ``ObjectSerializationVersion`` constants are *different* from
# ``DynamicSerializationVersion`` despite the similar names — Object's
# V1 is ``0``, Dynamic's V1 is ``1``. Keep the constants disambiguated.
_OBJECT_VERSION_V1 = 0
_OBJECT_VERSION_STRING = 1
_OBJECT_VERSION_V2 = 2

# ClickHouse's default ``max_dynamic_paths`` (per upstream
# ``DataTypeObject.h::DEFAULT_MAX_SEPARATELY_STORED_PATHS``). Used in
# the V1 wire format slot — the reader skips it past, so any
# reasonable value works on writes; we mirror upstream's default for
# wire-trace symmetry.
_DEFAULT_MAX_DYNAMIC_PATHS = 1024


# ---- nested / flat helpers -------------------------------------------------


def _nest(flat: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a nested dict from dotted-path keys.

    ``{"user.id": 7, "user.name": "alice"}`` → ``{"user": {"id": 7, "name": "alice"}}``.
    Keys with no dot pass through unchanged.
    """
    out: dict[str, Any] = {}
    for dotted_key, value in flat.items():
        parts = dotted_key.split(".")
        node = out
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return out


def _flatten(d: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested dict to dotted-path keys.

    ``{"user": {"id": 7}}`` → ``{"user.id": 7}``. Already-flat dicts
    (no values that are ``dict``) pass through unchanged — calling this
    on a flat dict is a no-op.
    """
    out: dict[str, Any] = {}
    for k, v in d.items():
        full = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, full))
        else:
            out[full] = v
    return out


class JSON:
    """``JSON[(hint, hint, …)]`` — schema-on-read JSON column.

    Read path: returns one ``dict[str, Any]`` per row, keyed by dotted
    path; values are whatever the per-path ``Dynamic`` codec yields.
    Missing paths are simply absent from the row's dict (NULL on the
    Dynamic discriminator stream → no entry rather than ``{path:
    None}``). Empty objects come back as ``{}``.

    Write path: accepts a sequence of ``dict[str, Any]`` rows, walks
    them to discover the path set, and emits each path as its own
    Dynamic sub-column. Values must be Python types our ``Dynamic``
    inference covers (``int → Int64``, ``str → String``, etc.) or
    explicitly tagged via ``Dynamic.tag(value, type_spec)``.
    """

    null_value: ClassVar[None] = None
    python_type: ClassVar[type] = dict

    def __init__(
        self,
        hints: list[_JSONHint] | None = None,
        *,
        json_nested: bool = False,
    ) -> None:
        self.hints = hints or []
        self._nested = json_nested
        if self.hints:
            self.name = "JSON({})".format(", ".join(h.text for h in self.hints))
        else:
            self.name = "JSON"

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[dict[str, Any]]:
        if n_rows == 0:
            return []

        # 1. ObjectSerializationVersion + (V1) max_dynamic_paths slot.
        version = await reader.read_int(8, signed=False)
        if version == _OBJECT_VERSION_STRING:
            # STRING mode: each row is one length-prefixed JSON-text
            # string. We don't decode — return the raw strings under a
            # well-known key so callers can re-parse if they want.
            raise ProtocolError(
                "JSON column emitted in STRING serialization mode "
                "(output_format_native_write_json_as_string=1) is not "
                "supported on read; disable that setting on the SELECT"
            )
        if version not in (_OBJECT_VERSION_V1, _OBJECT_VERSION_V2):
            raise ProtocolError(
                f"unsupported JSON Object structure version {version}; "
                f"expected V1 ({_OBJECT_VERSION_V1}) or V2 ({_OBJECT_VERSION_V2})"
            )
        if version == _OBJECT_VERSION_V1:
            await reader.read_varuint()  # max_dynamic_paths slot
        # 2. Sorted path-name list.
        n_paths = await reader.read_varuint()
        path_names = [await reader.read_string() for _ in range(n_paths)]

        # 3. Per-path Dynamic STATE PREFIX, all consumed before any
        #    bulk body. This mirrors upstream ``SerializationObject``,
        #    which calls each path's ``deserializeBinaryBulkStatePrefix``
        #    inside ``deserializeBinaryBulkStatePrefix`` *before*
        #    looping ``deserializeBinaryBulkWithMultipleStreams`` for
        #    bulk reads. For single-path columns the interleaving is
        #    invisible, but with two or more paths the prefix-bulk
        #    boundary moves and you'd read the wrong bytes.
        per_path_components: list[list[Any]] = []
        for _ in path_names:
            dyn_version = await reader.read_int(8, signed=False)
            if dyn_version not in (_DYNAMIC_VERSION_V1, _DYNAMIC_VERSION_V2):
                raise ProtocolError(
                    f"unsupported Dynamic structure version {dyn_version} "
                    f"inside JSON path"
                )
            if dyn_version == _DYNAMIC_VERSION_V1:
                await reader.read_varuint()  # max_dynamic_types slot
            n_types = await reader.read_varuint()
            declared_specs = [await reader.read_string() for _ in range(n_types)]
            # Sort declared specs together with the implicit
            # ``SharedVariant`` arm — same rule as standalone Dynamic.
            sorted_specs = sorted([*declared_specs, _SHARED_VARIANT_NAME])
            components = [
                parse_type(_SHARED_VARIANT_TYPE_SPEC)
                if spec == _SHARED_VARIANT_NAME
                else parse_type(spec)
                for spec in sorted_specs
            ]
            inner_version = await reader.read_int(8, signed=False)
            if inner_version != _VARIANT_VERSION_BASIC:
                raise ProtocolError(
                    f"unsupported Variant mode {inner_version} inside JSON "
                    f"Dynamic; only BASIC (0) is supported"
                )
            per_path_components.append(components)

        # 4. shared_data state prefix — ``Array → Tuple → String x2``
        #    chain is all base-class no-op. Zero bytes consumed.

        # 5. Per-path Dynamic BULK BODY (discs + per-arm bodies in
        #    sorted variant order).
        per_path_values: list[list[Any]] = [
            await Variant._read_body(reader, n_rows, components)
            for components in per_path_components
        ]

        # 6. shared_data bulk — ``Array(Tuple(String, String))`` body.
        #    Each per-row element is a list of (dotted_path, json_str)
        #    pairs for paths that spilled past max_dynamic_paths.
        shared_data_codec = parse_type("Array(Tuple(String, String))")
        shared_data: list[list[Any]] = await shared_data_codec.read(reader, n_rows)

        # 7. Reassemble per-row dicts. NULL on a path's Dynamic stream
        #    means "this row had no value for this path" — represent
        #    that as the path being absent from the row's dict, not
        #    ``{path: None}``.
        result: list[dict[str, Any]] = [{} for _ in range(n_rows)]
        for path, values in zip(path_names, per_path_values, strict=True):
            for i, v in enumerate(values):
                if v is not None:
                    result[i][path] = v
        # Merge overflow paths from shared-data back in.
        for i, spilled in enumerate(shared_data):
            for path, json_str in spilled:
                result[i][path] = _json.loads(json_str)
        if self._nested:
            return [_nest(row) for row in result]
        return result

    def write(self, writer: BinaryWriter, values: Sequence[dict[str, Any]]) -> None:
        if not values:
            return

        # Flatten nested dicts to dotted-path keys first. Already-flat
        # rows pass through unchanged (_flatten is a no-op on flat dicts).
        flat_values: list[dict[str, Any]] = [_flatten(row) for row in values]

        # Count path occurrences across the batch to decide which paths
        # get their own sub-column vs. spill to shared-data. Sort
        # alphabetically — upstream ``SerializationObject`` sorts
        # ``object_state->sorted_dynamic_paths`` before writing.
        path_counts: Counter[str] = Counter()
        for row in flat_values:
            path_counts.update(row.keys())
        all_paths = sorted(path_counts.keys())

        if len(all_paths) <= _DEFAULT_MAX_DYNAMIC_PATHS:
            dynamic_paths = all_paths
            overflow_paths: list[str] = []
        else:
            # Most-frequent paths earn their own sub-column; the rest
            # spill to the shared-data Array(Tuple(String, String)) column.
            most_common = [
                p for p, _ in path_counts.most_common(_DEFAULT_MAX_DYNAMIC_PATHS)
            ]
            dynamic_paths = sorted(most_common)
            overflow_set = set(all_paths) - set(dynamic_paths)
            overflow_paths = sorted(overflow_set)

        sorted_paths = dynamic_paths

        # For each dynamic path, resolve every row to a ``(spec, payload)``
        # pair (or ``(None, None)`` for missing). We need this resolution
        # twice (once for the state prefix to compute declared types,
        # once for the bulk body to compute discriminators), so cache it
        # up front.
        per_path_resolved: list[list[tuple[str | None, Any]]] = []
        per_path_specs: list[list[str]] = []
        per_path_sorted_components: list[list[Any]] = []
        for path in sorted_paths:
            seen: set[str] = set()
            specs: list[str] = []
            resolved: list[tuple[str | None, Any]] = []
            for row in flat_values:
                v = row.get(path)
                if v is None:
                    resolved.append((None, None))
                    continue
                if isinstance(v, _DynamicTag):
                    spec = v.type_spec
                    payload: Any = v.value
                else:
                    spec = _infer_dynamic_type_spec(v)
                    payload = v
                if spec not in seen:
                    seen.add(spec)
                    specs.append(spec)
                resolved.append((spec, payload))
            per_path_resolved.append(resolved)
            per_path_specs.append(specs)
            sorted_names = sorted([*specs, _SHARED_VARIANT_NAME])
            per_path_sorted_components.append(
                [
                    parse_type(_SHARED_VARIANT_TYPE_SPEC)
                    if name == _SHARED_VARIANT_NAME
                    else parse_type(name)
                    for name in sorted_names
                ]
            )

        # 1. ObjectStructure prefix: V1 + max + num + path names.
        writer.write_int(_OBJECT_VERSION_V1, 8, signed=False)
        writer.write_varuint(_DEFAULT_MAX_DYNAMIC_PATHS)
        writer.write_varuint(len(sorted_paths))
        for path in sorted_paths:
            writer.write_string(path)

        # 2. Per-path Dynamic STATE PREFIX (interleaved with the
        #    matching shared-data state prefix below). Upstream
        #    ``SerializationObject`` writes all dynamic-path state
        #    prefixes BEFORE any bulk body — for multi-path JSON the
        #    naive "for each path: prefix+bulk" interleaving produces
        #    wrong bytes since the receiver expects all prefixes first.
        for specs in per_path_specs:
            # 8B Dyn V1 + max + num + declared type names (sorted, but
            # *excluding* the implicit SharedVariant — only the variant
            # body slots include it). The receiver re-appends
            # SharedVariant after parsing this list.
            writer.write_int(_DYNAMIC_VERSION_V1, 8, signed=False)
            writer.write_varuint(_DEFAULT_MAX_DYNAMIC_TYPES)
            writer.write_varuint(len(specs))
            for name in specs:
                writer.write_string(name)
            # 8B variant mode (BASIC = 0). Each variant arm's
            # state prefix is no-op for the primitive types we
            # encode; nothing more to write here.
            writer.write_int(_VARIANT_VERSION_BASIC, 8, signed=False)

        # 3. shared_data state prefix is the chain
        #    ``Array → Tuple → String x2`` — all of which have
        #    base-class no-op state prefixes. Zero bytes. We don't
        #    even need a placeholder write.

        # 4. Per-path Dynamic BULK BODY. We build the inner-Variant
        #    body inline rather than recursing into ``Variant._write_body``
        #    because the discriminator sort key already picks the right
        #    sorted index for each row.
        for resolved, sorted_components, specs in zip(
            per_path_resolved,
            per_path_sorted_components,
            per_path_specs,
            strict=True,
        ):
            sorted_names = sorted([*specs, _SHARED_VARIANT_NAME])
            spec_to_disc = {name: i for i, name in enumerate(sorted_names)}
            rows_with_disc: list[tuple[int, Any]] = [
                (_NULL_DISCRIMINATOR, None)
                if spec is None
                else (spec_to_disc[spec], payload)
                for spec, payload in resolved
            ]
            Variant._write_body(writer, sorted_components, rows_with_disc)

        # 5. shared_data bulk — ``Array(Tuple(String, String))`` body.
        #    Each element is a list of (dotted_path, json_encoded_value)
        #    pairs for overflow paths present in that row. Empty when all
        #    paths fit within _DEFAULT_MAX_DYNAMIC_PATHS.
        if overflow_paths:
            shared_rows: list[list[tuple[str, str]]] = []
            for row in flat_values:
                spilled: list[tuple[str, str]] = []
                for path in overflow_paths:
                    v = row.get(path)
                    if v is not None:
                        spilled.append((path, _json.dumps(v, default=str)))
                shared_rows.append(spilled)
        else:
            shared_rows = [[] for _ in flat_values]
        shared_data_codec = parse_type("Array(Tuple(String, String))")
        shared_data_codec.write(writer, shared_rows)
