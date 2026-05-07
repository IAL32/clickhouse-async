"""Codecs for `Date`, `Date32`, `DateTime[(tz)]` and
`DateTime64(precision[, tz])`.

On-wire layouts:

- `Date`:        UInt16, days since 1970-01-01 (unsigned).
- `Date32`:      Int32,  days since 1970-01-01 (signed; covers 1900-2299).
- `DateTime`:    UInt32, seconds since the Unix epoch (UTC). Optional
                   timezone parameter is *display-only* — storage is UTC.
- `DateTime64`:  Int64,  ticks since the Unix epoch where 1 tick =
                   `10**-precision` seconds. Optional timezone is also
                   display-only.

Returned Python types:

- `Date` and `Date32`                     → `datetime.date`.
- `DateTime`                                → naive `datetime` if no
                                                timezone, aware `datetime`
                                                if a timezone parameter is
                                                present.
- `DateTime64(p ≤ 6)`                       → `datetime` (microsecond
                                                resolution covers it).
- `DateTime64(p ∈ {7, 8, 9})`               → `HighPrecisionTimestamp`
                                                so sub-microsecond ticks
                                                survive the Python boundary.
                                                Pass `high_precision=False`
                                                to opt out and accept the
                                                lossy `datetime` form.

A bare `DateTime` codec without an explicit timezone parameter honours
the connection's session timezone when one is plumbed through
`parse_type(..., session_timezone=...)`; without it, naive datetimes
fall back to UTC interpretation (the v0 behaviour).
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from clickhouse_async.types._datetime_helpers import _naive_utc_from_ts, _resolve_tz

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import BinaryWriter
    from clickhouse_async.protocol.io_sync import SyncBinaryReader

_EPOCH_DATE = date(1970, 1, 1)

# Python's `datetime` only carries microsecond resolution. Anything past
# scale 6 (microseconds) needs `HighPrecisionTimestamp`; anything at or
# below survives a `datetime` round-trip exactly.
_MICROSECOND_SCALE = 6
# ClickHouse caps DateTime64 precision at 9 (nanoseconds).
_MAX_DATETIME64_PRECISION = 9


# ---- HighPrecisionTimestamp ---------------------------------------------


@dataclass(frozen=True)
class HighPrecisionTimestamp:
    """Sub-microsecond timestamp surfaced by `DateTime64(p > 6)`.

    Python's `datetime` only carries microsecond resolution (10⁻⁶ s),
    so `DateTime64(7..9)` would lose the bottom digits if forced
    through it. This dataclass keeps the raw integer `ticks` against
    the codec's `scale` (`10⁻ᵖ` seconds per tick) — round-trips
    are byte-for-byte exact.

    Use `to_datetime()` for a lossy conversion to a `datetime`
    (truncates anything below microsecond), or
    `HighPrecisionTimestamp.from_datetime(dt, scale)` to build one
    from a `datetime` rounded to the codec's scale.
    """

    ticks: int
    scale: int  # power-of-ten exponent: 1 tick == 10**-scale seconds

    def to_datetime(self) -> datetime:
        """Convert to a naive UTC `datetime`. Lossy when
        `scale > _MICROSECOND_SCALE` — sub-microsecond ticks are
        truncated."""
        if self.scale <= _MICROSECOND_SCALE:
            us_per_tick = 10 ** (_MICROSECOND_SCALE - self.scale)
            us_total = self.ticks * us_per_tick
        else:
            ticks_per_us = 10 ** (self.scale - _MICROSECOND_SCALE)
            us_total = self.ticks // ticks_per_us
        seconds, microseconds = divmod(us_total, 1_000_000)
        return _naive_utc_from_ts(seconds).replace(microsecond=microseconds)

    @classmethod
    def from_datetime(cls, dt: datetime, scale: int) -> HighPrecisionTimestamp:
        """Build a `HighPrecisionTimestamp` from a `datetime` at
        `scale`. Naive `dt` is treated as UTC; sub-microsecond
        digits are zero (Python doesn't carry them)."""
        if dt.tzinfo is None:
            seconds = int(dt.replace(tzinfo=UTC).timestamp())
        else:
            seconds = int(dt.timestamp())
        microseconds = dt.microsecond
        if scale <= _MICROSECOND_SCALE:
            ticks = seconds * (10**scale) + microseconds // (
                10 ** (_MICROSECOND_SCALE - scale)
            )
        else:
            ticks = seconds * (10**scale) + microseconds * (
                10 ** (scale - _MICROSECOND_SCALE)
            )
        return cls(ticks=ticks, scale=scale)


# ---- Date / Date32 -------------------------------------------------------


class Date:
    name = "Date"
    null_value: date = _EPOCH_DATE
    python_type: type = date

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[date]:
        if n_rows == 0:
            return []
        data = reader.read_exact(2 * n_rows)
        days_arr = struct.unpack(f"<{n_rows}H", data)
        epoch = _EPOCH_DATE
        td = timedelta
        return [epoch + td(days=d) for d in days_arr]

    def write(self, writer: BinaryWriter, values: Sequence[date]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            days = (v - _EPOCH_DATE).days
            out.extend(days.to_bytes(2, "little", signed=False))
        writer.write_raw(bytes(out))


class Date32:
    name = "Date32"
    null_value: date = _EPOCH_DATE
    python_type: type = date

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[date]:
        if n_rows == 0:
            return []
        data = reader.read_exact(4 * n_rows)
        days_arr = struct.unpack(f"<{n_rows}i", data)
        epoch = _EPOCH_DATE
        td = timedelta
        return [epoch + td(days=d) for d in days_arr]

    def write(self, writer: BinaryWriter, values: Sequence[date]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            days = (v - _EPOCH_DATE).days
            out.extend(days.to_bytes(4, "little", signed=True))
        writer.write_raw(bytes(out))


# ---- DateTime ------------------------------------------------------------


class DateTime:
    null_value: datetime
    python_type: type = datetime

    def __init__(
        self,
        timezone: str | None = None,
        *,
        session_timezone: str | None = None,
    ) -> None:
        # `timezone` (the type-spec argument) takes precedence;
        # `session_timezone` is the fallback the connection plumbs in
        # so naive `DateTime` reads land in the server's session zone
        # rather than silently UTC.
        self.explicit_timezone = timezone
        effective_tz = timezone if timezone is not None else session_timezone
        self.timezone_name = effective_tz
        self._tz = _resolve_tz(effective_tz)
        # `codec.name` round-trips the *type-spec* form, not the
        # session-augmented form. `DateTime('UTC')` stays
        # `DateTime('UTC')`; a bare `DateTime` stays `DateTime`
        # even when an aware datetime gets surfaced via session_tz.
        self.name = f"DateTime('{timezone}')" if timezone else "DateTime"
        self.null_value = (
            datetime(1970, 1, 1, tzinfo=self._tz)
            if self._tz is not None
            else datetime(1970, 1, 1)
        )

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[datetime]:
        if n_rows == 0:
            return []
        data = reader.read_exact(4 * n_rows)
        # Bulk-unpack all timestamps in one C-level call rather than
        # per-row `int.from_bytes`, then drive the per-row datetime
        # construction off the resulting tuple.
        timestamps = struct.unpack(f"<{n_rows}I", data)
        tz = self._tz
        if tz is not None:
            from_ts = datetime.fromtimestamp
            return [from_ts(ts, tz=tz) for ts in timestamps]
        # Naive UTC: do the resolution-then-strip dance once per row.
        # `_naive_utc_from_ts` is hoisted to a local for the loop.
        naive = _naive_utc_from_ts
        return [naive(ts) for ts in timestamps]

    def write(self, writer: BinaryWriter, values: Sequence[datetime]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            if v.tzinfo is None:
                # Treat naive as UTC seconds (matches read semantics).
                ts = int(v.replace(tzinfo=UTC).timestamp())
            else:
                ts = int(v.timestamp())
            out.extend(ts.to_bytes(4, "little", signed=False))
        writer.write_raw(bytes(out))


# ---- DateTime64 ----------------------------------------------------------


class DateTime64:
    null_value: datetime | HighPrecisionTimestamp
    # `DateTime64` may surface `datetime` (low-precision) or
    # `HighPrecisionTimestamp` (high-precision) values. We declare
    # `datetime` as the variant-resolution type since it's the
    # common case; high-precision callers can pin via `Variant.tag`
    # if they need `HighPrecisionTimestamp` to land in this arm.
    python_type: type = datetime

    def __init__(
        self,
        precision: int,
        timezone: str | None = None,
        *,
        high_precision: bool | None = None,
        session_timezone: str | None = None,
    ) -> None:
        if precision < 0 or precision > _MAX_DATETIME64_PRECISION:
            raise ValueError(
                f"DateTime64 precision must be in [0, "
                f"{_MAX_DATETIME64_PRECISION}], got {precision}"
            )
        self.precision = precision
        self.explicit_timezone = timezone
        # Resolve display tz: explicit beats session beats none.
        effective_tz = timezone if timezone is not None else session_timezone
        self.timezone_name = effective_tz
        self._tz = _resolve_tz(effective_tz)
        self._scale = 10**precision
        if timezone:
            self.name = f"DateTime64({precision}, '{timezone}')"
        else:
            self.name = f"DateTime64({precision})"
        # `high_precision` defaults to True iff the codec carries
        # sub-microsecond resolution; flip it explicitly to opt out
        # (forcing `datetime` returns at the cost of truncation).
        self.high_precision: bool = (
            (precision > _MICROSECOND_SCALE)
            if high_precision is None
            else high_precision
        )
        if self.high_precision:
            self.null_value = HighPrecisionTimestamp(ticks=0, scale=precision)
        else:
            self.null_value = (
                datetime(1970, 1, 1, tzinfo=self._tz)
                if self._tz is not None
                else datetime(1970, 1, 1)
            )

    def read(
        self, reader: SyncBinaryReader, n_rows: int
    ) -> list[datetime | HighPrecisionTimestamp]:
        if n_rows == 0:
            return []
        data = reader.read_exact(8 * n_rows)
        if self.high_precision:
            return self._read_high_precision(data, n_rows)
        return self._read_datetime(data, n_rows)

    def _read_datetime(
        self, data: bytes, n_rows: int
    ) -> list[datetime | HighPrecisionTimestamp]:
        out: list[datetime | HighPrecisionTimestamp] = []
        scale = self._scale
        for i in range(n_rows):
            ticks = int.from_bytes(data[i * 8 : (i + 1) * 8], "little", signed=True)
            seconds, fraction = divmod(ticks, scale)
            # Map fraction (10**-precision seconds) into microseconds (10**-6).
            if self.precision <= _MICROSECOND_SCALE:
                microseconds = fraction * (10 ** (_MICROSECOND_SCALE - self.precision))
            else:
                microseconds = fraction // (10 ** (self.precision - _MICROSECOND_SCALE))
            if self._tz is not None:
                base = datetime.fromtimestamp(seconds, tz=self._tz)
            else:
                base = _naive_utc_from_ts(seconds)
            out.append(base.replace(microsecond=microseconds))
        return out

    def _read_high_precision(
        self, data: bytes, n_rows: int
    ) -> list[datetime | HighPrecisionTimestamp]:
        precision = self.precision
        return [
            HighPrecisionTimestamp(
                ticks=int.from_bytes(data[i * 8 : (i + 1) * 8], "little", signed=True),
                scale=precision,
            )
            for i in range(n_rows)
        ]

    def write(
        self,
        writer: BinaryWriter,
        values: Sequence[datetime | HighPrecisionTimestamp],
    ) -> None:
        if not values:
            return
        out = bytearray()
        scale = self._scale
        for v in values:
            if isinstance(v, HighPrecisionTimestamp):
                if v.scale != self.precision:
                    raise ValueError(
                        f"HighPrecisionTimestamp scale {v.scale} does not "
                        f"match codec precision {self.precision}"
                    )
                ticks = v.ticks
            else:
                if v.tzinfo is None:
                    seconds = int(v.replace(tzinfo=UTC).timestamp())
                else:
                    seconds = int(v.timestamp())
                microseconds = v.microsecond
                if self.precision <= _MICROSECOND_SCALE:
                    fraction = microseconds // (
                        10 ** (_MICROSECOND_SCALE - self.precision)
                    )
                else:
                    fraction = microseconds * (
                        10 ** (self.precision - _MICROSECOND_SCALE)
                    )
                ticks = seconds * scale + fraction
            out.extend(ticks.to_bytes(8, "little", signed=True))
        writer.write_raw(bytes(out))
