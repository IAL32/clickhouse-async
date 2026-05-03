"""Codecs for ``Date``, ``Date32``, ``DateTime[(tz)]`` and
``DateTime64(precision[, tz])``.

On-wire layouts:

- ``Date``:        UInt16, days since 1970-01-01 (unsigned).
- ``Date32``:      Int32,  days since 1970-01-01 (signed; covers 1900-2299).
- ``DateTime``:    UInt32, seconds since the Unix epoch (UTC). Optional
                   timezone parameter is *display-only* — storage is UTC.
- ``DateTime64``:  Int64,  ticks since the Unix epoch where 1 tick =
                   ``10**-precision`` seconds. Optional timezone is also
                   display-only.

Returned Python types:

- ``Date`` and ``Date32``       → ``datetime.date``.
- ``DateTime`` and ``DateTime64`` → naive ``datetime.datetime`` if no
                                    timezone, aware ``datetime.datetime``
                                    if a timezone parameter is present.

Note: Python's ``datetime`` carries microsecond precision (10**-6 s), so
``DateTime64(p)`` for ``p > 6`` truncates the lowest digits at the Python
boundary. Round-tripping at p ≤ 6 is exact.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter

_EPOCH_DATE = date(1970, 1, 1)


def _resolve_tz(name: str | None) -> timezone | ZoneInfo | None:
    if name is None:
        return None
    if name == "UTC":
        return UTC
    return ZoneInfo(name)


def _naive_utc_from_ts(ts: int) -> datetime:
    """Naive datetime representing the UTC instant at ``ts`` Unix seconds.

    ``datetime.utcfromtimestamp`` is deprecated; we get the same value
    by resolving in UTC and stripping the tzinfo.
    """
    return datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None)


# ---- Date / Date32 -------------------------------------------------------


class Date:
    name = "Date"
    null_value: date = _EPOCH_DATE

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[date]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(2 * n_rows)
        return [
            _EPOCH_DATE
            + timedelta(
                days=int.from_bytes(data[i * 2 : (i + 1) * 2], "little", signed=False)
            )
            for i in range(n_rows)
        ]

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

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[date]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(4 * n_rows)
        return [
            _EPOCH_DATE
            + timedelta(
                days=int.from_bytes(data[i * 4 : (i + 1) * 4], "little", signed=True)
            )
            for i in range(n_rows)
        ]

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

    def __init__(self, timezone: str | None = None) -> None:
        self.timezone_name = timezone
        self._tz = _resolve_tz(timezone)
        self.name = f"DateTime('{timezone}')" if timezone else "DateTime"
        self.null_value = (
            datetime(1970, 1, 1, tzinfo=self._tz)
            if self._tz is not None
            else datetime(1970, 1, 1)
        )

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[datetime]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(4 * n_rows)
        out: list[datetime] = []
        for i in range(n_rows):
            ts = int.from_bytes(data[i * 4 : (i + 1) * 4], "little", signed=False)
            if self._tz is not None:
                out.append(datetime.fromtimestamp(ts, tz=self._tz))
            else:
                # Naive: interpret as UTC seconds, present without tz
                out.append(_naive_utc_from_ts(ts))
        return out

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
    null_value: datetime

    def __init__(self, precision: int, timezone: str | None = None) -> None:
        if precision < 0 or precision > 9:
            raise ValueError(f"DateTime64 precision must be in [0, 9], got {precision}")
        self.precision = precision
        self.timezone_name = timezone
        self._tz = _resolve_tz(timezone)
        self._scale = 10**precision
        if timezone:
            self.name = f"DateTime64({precision}, '{timezone}')"
        else:
            self.name = f"DateTime64({precision})"
        self.null_value = (
            datetime(1970, 1, 1, tzinfo=self._tz)
            if self._tz is not None
            else datetime(1970, 1, 1)
        )

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[datetime]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(8 * n_rows)
        out: list[datetime] = []
        scale = self._scale
        for i in range(n_rows):
            ticks = int.from_bytes(data[i * 8 : (i + 1) * 8], "little", signed=True)
            seconds, fraction = divmod(ticks, scale)
            # Map fraction (10**-precision seconds) into microseconds (10**-6).
            if self.precision <= 6:
                microseconds = fraction * (10 ** (6 - self.precision))
            else:
                microseconds = fraction // (10 ** (self.precision - 6))
            if self._tz is not None:
                base = datetime.fromtimestamp(seconds, tz=self._tz)
            else:
                base = _naive_utc_from_ts(seconds)
            out.append(base.replace(microsecond=microseconds))
        return out

    def write(self, writer: BinaryWriter, values: Sequence[datetime]) -> None:
        if not values:
            return
        out = bytearray()
        scale = self._scale
        for v in values:
            if v.tzinfo is None:
                seconds = int(v.replace(tzinfo=UTC).timestamp())
            else:
                seconds = int(v.timestamp())
            microseconds = v.microsecond
            if self.precision <= 6:
                fraction = microseconds // (10 ** (6 - self.precision))
            else:
                fraction = microseconds * (10 ** (self.precision - 6))
            ticks = seconds * scale + fraction
            out.extend(ticks.to_bytes(8, "little", signed=True))
        writer.write_raw(bytes(out))
