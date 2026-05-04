"""Pure dict helpers for dotted-path ↔ nested-dict conversion.

Used by the JSON codec to accept nested-dict input on write and
optionally reconstruct nested dicts on read. Extracted here so they
can be tested and reused independently of the wire-protocol codec.
"""

from __future__ import annotations

from typing import Any


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
