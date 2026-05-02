"""Error hierarchy. See DESIGN.md §8."""

from __future__ import annotations


class ClickHouseError(Exception):
    """Base class for all clickhouse-async errors."""


class ProtocolError(ClickHouseError):
    """Wire data violated the ClickHouse protocol."""


class ServerError(ClickHouseError):
    """An error reported by the server over the protocol's Exception packet.

    Carries the structured fields the server emits — numeric code, error
    name, the human-readable display text, and the stack trace if any —
    plus an optional ``nested`` chain when the server stacks multiple
    causes. ``code`` is the canonical upstream numeric code; we do not
    invent our own.
    """

    def __init__(
        self,
        code: int,
        name: str,
        display_text: str,
        stack_trace: str = "",
        nested: ServerError | None = None,
    ) -> None:
        super().__init__(f"{name} (code {code}): {display_text}")
        self.code = code
        self.name = name
        self.display_text = display_text
        self.stack_trace = stack_trace
        self.nested = nested
