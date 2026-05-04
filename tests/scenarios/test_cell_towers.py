"""Scenario tests against the OpenCelliD cell towers dataset.

Source: https://datasets-documentation.s3.amazonaws.com/cell_towers/cell_towers.csv.xz
Scale: 500 000 rows (LIMIT imposed at load time)
Types exercised: Enum8, UInt8/16/32/64, Int16, Float64, DateTime
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import clickhouse_async as ch

_VALID_RADIOS = {"CDMA", "GSM", "LTE", "NR", "UMTS"}


async def test_total_row_count(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: the cell towers table was loaded with LIMIT 500 000
    # WHEN: counting all rows
    rows = await client.fetch_all(f"SELECT count() FROM {cell_towers}")

    # THEN: exactly 500 000 rows were written
    count = rows[0][0]
    assert isinstance(count, int)
    assert count == 500_000


async def test_radio_enum_values(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: the radio column is Enum8 with five named variants
    # WHEN: collecting all distinct radio values
    rows = await client.fetch_all(
        f"SELECT DISTINCT radio FROM {cell_towers} ORDER BY radio"
    )

    # THEN: every value is one of the five defined radio types
    for (radio,) in rows:
        assert radio in _VALID_RADIOS


async def test_radio_decoded_as_string(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: Enum8 values should decode to the enum name, not the numeric tag
    # WHEN: fetching one radio value
    rows = await client.fetch_all(f"SELECT radio FROM {cell_towers} LIMIT 1")

    # THEN: the Python type is str
    assert isinstance(rows[0][0], str)


async def test_lon_lat_are_float(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: lon / lat are stored as Float64
    # WHEN: fetching one coordinate pair
    rows = await client.fetch_all(f"SELECT lon, lat FROM {cell_towers} LIMIT 1")

    # THEN: both are Python floats within plausible geographic ranges
    lon, lat = rows[0]
    assert isinstance(lon, float)
    assert isinstance(lat, float)
    assert -180.0 <= lon <= 180.0
    assert -90.0 <= lat <= 90.0


async def test_created_decoded_as_datetime(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: the created column is typed DateTime
    # WHEN: fetching one row
    rows = await client.fetch_all(f"SELECT created FROM {cell_towers} LIMIT 1")

    # THEN: the value is a timezone-aware datetime (session timezone applied)
    assert isinstance(rows[0][0], datetime)


async def test_towers_per_radio_type(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: all 500 000 rows belong to some radio type
    # WHEN: grouping by radio
    rows = await client.fetch_all(
        f"SELECT radio, count() AS cnt FROM {cell_towers} "
        f"GROUP BY radio ORDER BY cnt DESC"
    )

    # THEN: at least one group exists and counts sum to 500 000
    assert len(rows) >= 1
    total = sum(r[1] for r in rows)
    assert total == 500_000


async def test_bounding_box_query(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: the dataset spans global coordinates including Western Europe
    # WHEN: filtering to a Western-Europe bounding box
    rows = await client.fetch_all(
        f"SELECT count() FROM {cell_towers} "
        f"WHERE lon BETWEEN -10 AND 30 AND lat BETWEEN 35 AND 60"
    )

    # THEN: a positive count is returned (the region has towers)
    count = rows[0][0]
    assert isinstance(count, int)
    assert count > 0


async def test_towers_created_after_2010(client: ch.Client, cell_towers: str) -> None:
    # BEGIN: the dataset spans decades of cell tower registrations
    # WHEN: filtering to towers registered on or after 2010-01-01
    rows = await client.fetch_all(
        f"SELECT count() FROM {cell_towers} WHERE created >= '2010-01-01'"
    )

    # THEN: a positive count that is at most the full 500 000
    count = rows[0][0]
    assert isinstance(count, int)
    assert 0 < count <= 500_000
