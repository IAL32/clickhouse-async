"""Round-robin host rotation with per-host failure cooldowns.

Used by ``Pool`` to spread acquires across the candidate list of a
multi-host DSN. Every ``next_candidates()`` call advances the start
position so concurrent acquires don't all hammer the same host;
hosts that just failed are skipped for a short cooldown so a single
dead replica doesn't burn every acquire on a connect-and-fail loop.

The rotation is purely synchronous — operations are O(n) over the
candidate list and never await — so a single asyncio event loop
serialises them naturally without an explicit lock.
"""

from __future__ import annotations

import time
from collections.abc import Sequence


class _HostRotation:
    """Track the candidate host list, rotation pointer, and per-host
    failure timestamps for the pool's open path."""

    def __init__(
        self,
        hosts: Sequence[tuple[str, int]],
        *,
        cooldown: float = 5.0,
    ) -> None:
        if not hosts:
            raise ValueError("rotation requires at least one host")
        if cooldown < 0:
            raise ValueError(f"cooldown must be ≥ 0, got {cooldown}")
        self._hosts: tuple[tuple[str, int], ...] = tuple(hosts)
        self._cooldown = cooldown
        self._failures: dict[tuple[str, int], float] = {}
        self._next_start: int = 0

    @property
    def hosts(self) -> tuple[tuple[str, int], ...]:
        return self._hosts

    @property
    def cooldown(self) -> float:
        return self._cooldown

    def next_candidates(self) -> tuple[tuple[str, int], ...]:
        """Return the candidate list ordered for the next open() attempt.

        - Rotates the start position by 1 so successive acquires hit
          different first-choice hosts.
        - Drops hosts whose last failure is within the cooldown window.
        - If every host is in cooldown (every replica is recently dead),
          returns the full rotated list anyway — the cooldown is best
          effort, not a hard wait.
        """
        now = time.monotonic()
        n = len(self._hosts)
        rotated: list[tuple[str, int]] = [
            self._hosts[(self._next_start + i) % n] for i in range(n)
        ]
        self._next_start = (self._next_start + 1) % n

        eligible: list[tuple[str, int]] = [
            host
            for host in rotated
            if (now - self._failures.get(host, 0.0)) >= self._cooldown
        ]
        return tuple(eligible) if eligible else tuple(rotated)

    def record_failure(self, host: tuple[str, int]) -> None:
        """Mark ``host`` as recently-failed. The next ``cooldown``
        seconds of rotations will skip it (unless every host is in
        cooldown)."""
        if host not in set(self._hosts):
            # Unknown host — defensive; the caller is the Connection
            # which only sees what we hand it, so this shouldn't happen.
            return
        self._failures[host] = time.monotonic()

    def record_success(self, host: tuple[str, int]) -> None:
        """Clear ``host``'s failure timestamp — a successful connect
        proves the replica is live again."""
        self._failures.pop(host, None)
