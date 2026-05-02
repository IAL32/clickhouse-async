"""Error hierarchy. See DESIGN.md §8."""

from __future__ import annotations


class ClickHouseError(Exception):
    """Base class for all clickhouse-async errors."""


class ProtocolError(ClickHouseError):
    """Wire data violated the ClickHouse protocol."""


class ConcurrentQueryError(ClickHouseError):
    """A second query was issued on a connection while another is in flight.

    The native protocol doesn't multiplex; one in-flight query per
    connection is the contract. Callers wanting concurrency should use a
    ``Pool``.
    """


class QueryCancellationError(ClickHouseError):
    """Outcome of a ``Connection.cancel()`` call.

    ``reason`` distinguishes the path the cancel took:

    - ``"drained"``: cancel succeeded; the connection returned to
      ``READY``. The previous query is over and the connection is
      reusable.
    - ``"timeout"``: the post-Cancel drain exceeded ``drain_timeout``;
      the socket was closed and the connection is ``BROKEN``.
    - ``"already_cancelled"``: another cancel is in flight on this
      connection (the second call is rejected; the first is still
      working).
    - ``"not_in_flight"``: cancel was called from a state other than
      ``IN_FLIGHT`` (and not from ``READY``, where it's a no-op).
    """

    reason: str

    def __init__(self, *, reason: str, message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason


class UnsupportedFeatureError(ClickHouseError):
    """A feature was requested but the negotiated protocol revision
    doesn't support it.

    Raised, for example, when the user passes ``params`` to ``send_query``
    against a server below
    ``DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS``. We refuse rather than
    silently fall back to client-side string interpolation — that would
    undermine the safety claim of server-bound parameters.
    """


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
