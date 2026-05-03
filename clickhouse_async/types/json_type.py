"""Codec stub for ClickHouse 24.x's ``JSON`` type.

v0.2 ships *parser-only* support: ``parse_type("JSON")`` and
``parse_type("JSON(max_dynamic_paths=N, max_dynamic_types=N, SKIP path,
SKIP REGEXP 'pattern')")`` succeed and round-trip ``codec.name`` so
queries that mention ``JSON`` columns don't crash at the type-spec
parse, but ``codec.read`` and ``codec.write`` raise
``NotImplementedError`` with a diagnostic pointing at the v0.3 plan.

The skipped read/write path is non-trivial: upstream
``SerializationObject`` concatenates five substreams in one stream
(``ObjectStructure`` prefix → typed-path state prefix → dynamic-path
state prefix → shared-data state prefix → typed-path body →
dynamic-path body → shared-data body), each ``Dynamic`` sub-column
itself nests a ``Variant`` body, and the format is gated by a
revision-dependent V1/V2 layout that's still settling on the server
side. Plan 06 confirmed our standalone ``Dynamic`` codec doesn't yet
match the real server wire format, so JSON's bulk path waits for that
to be production-tested first.

Module name is ``json_type`` rather than ``json`` to avoid shadowing
the stdlib module — Python's import resolution looks at sibling
modules of ``clickhouse_async.types`` and the bare ``json`` name there
would mask anything that does ``import json`` from inside this
package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class _JSONHint:
    """One declared parameter of a ``JSON(...)`` spec.

    The block-header form mixes named-int caps (``max_dynamic_paths=N``,
    ``max_dynamic_types=N``) with ``SKIP path`` and ``SKIP REGEXP
    'pattern'`` clauses. We only store the kind + raw token strings
    needed to round-trip ``codec.name`` — semantic meaning lives
    server-side.
    """

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        # The verbatim spec fragment, e.g. ``max_dynamic_paths=512`` or
        # ``SKIP user.email`` or ``SKIP REGEXP 'tmp_.*'``. Storing the
        # raw text keeps the renderer trivial — no need to know which
        # hint kind we have.
        self.text = text


class JSON:
    """``JSON[(hint, hint, …)]`` — schema-on-read JSON column.

    Parser support only in v0.2. Both ``read`` and ``write`` raise
    ``NotImplementedError`` with a diagnostic pointing at the v0.3
    follow-up. ``codec.name`` round-trips the spec verbatim so the
    server's block header can be re-emitted on INSERT.
    """

    null_value: ClassVar[None] = None
    python_type: ClassVar[type] = dict

    def __init__(self, hints: list[_JSONHint] | None = None) -> None:
        self.hints = hints or []
        if self.hints:
            self.name = "JSON({})".format(", ".join(h.text for h in self.hints))
        else:
            self.name = "JSON"

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[dict[str, object]]:
        # ``reader`` is unused on purpose — we never consume any bytes
        # because the v0.2 stub raises before reading. Keep the
        # signature aligned with ``ColumnCodec`` so the parser registry
        # type-checks.
        del reader
        if n_rows == 0:
            return []
        raise NotImplementedError(
            "JSON column body decoding is not implemented in v0.2; the "
            "ClickHouse 24.x SerializationObject layout (5 concatenated "
            "substreams) is too involved to ship before Dynamic round-trips "
            "are server-verified. Tracked as a v0.3 follow-up; see TODO.md "
            "section 2 ('JSON type'). Workaround: cast to String "
            "server-side (``SELECT toJSONString(j) FROM …``) and parse the "
            "JSON in Python."
        )

    def write(self, writer: BinaryWriter, values: Sequence[object]) -> None:
        # Symmetric with ``read`` — never write bytes because the stub
        # raises before reaching the writer. ``del`` makes the
        # unused-arg lint happy.
        del writer
        if not values:
            return
        raise NotImplementedError(
            "JSON column body encoding is not implemented in v0.2; see "
            "the matching ``read`` diagnostic. Workaround for inserts: "
            "send a ``String`` column on the wire and ``CAST(s AS JSON)`` "
            "server-side."
        )
