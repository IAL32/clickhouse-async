"""Scenario tests against the COVID-19 Open Data epidemiology dataset.

Source: https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv
Scale: ~12.5 M rows (no LIMIT — full load)
Types exercised: Date, Int32, LowCardinality(String)
"""

from __future__ import annotations

from datetime import date, datetime

import clickhouse_async as ch
from clickhouse_async import ColumnarResult


async def test_total_row_count(client: ch.Client, covid19: str) -> None:
    # BEGIN: the full COVID-19 dataset is loaded
    # WHEN: counting all rows
    rows = await client.fetch_all(f"SELECT count() FROM {covid19}")

    # THEN: the dataset contains at least 1 M epidemiology records
    count = rows[0][0]
    assert isinstance(count, int)
    assert count >= 1_000_000


async def test_location_key_is_nonempty_string(client: ch.Client, covid19: str) -> None:
    # BEGIN: the COVID-19 table has a LowCardinality(String) location_key column
    # WHEN: sampling 10 distinct location keys
    rows = await client.fetch_all(
        f"SELECT DISTINCT location_key FROM {covid19} LIMIT 10"
    )

    # THEN: exactly 10 non-empty strings are returned
    assert len(rows) == 10
    for (key,) in rows:
        assert isinstance(key, str)
        assert key != ""


async def test_date_column_decoded_as_date(client: ch.Client, covid19: str) -> None:
    # BEGIN: the date column is typed Date on the server
    # WHEN: fetching one row
    rows = await client.fetch_all(f"SELECT date FROM {covid19} LIMIT 1")

    # THEN: the value is a Python date (not datetime)
    assert len(rows) == 1
    val = rows[0][0]
    assert isinstance(val, date)
    assert not isinstance(val, datetime)


async def test_top_locations_by_cumulative_confirmed(
    client: ch.Client, covid19: str
) -> None:
    # BEGIN: the dataset has cumulative_confirmed per location
    # WHEN: grouping and taking the top 5
    rows = await client.fetch_all(
        f"SELECT location_key, max(cumulative_confirmed) AS mx "
        f"FROM {covid19} GROUP BY location_key ORDER BY mx DESC LIMIT 5"
    )

    # THEN: 5 rows with int values in descending order
    assert len(rows) == 5
    for loc, mx in rows:
        assert isinstance(loc, str)
        assert isinstance(mx, int)
    values = [r[1] for r in rows]
    assert values == sorted(values, reverse=True)


async def test_daily_new_cases_for_known_location(
    client: ch.Client, covid19: str
) -> None:
    # BEGIN: 'US' is a location key present in the dataset
    # WHEN: fetching 30 days of new_confirmed for the US, ordered by date
    rows = await client.fetch_all(
        f"SELECT date, new_confirmed FROM {covid19} "
        f"WHERE location_key = 'US' ORDER BY date LIMIT 30"
    )

    # THEN: 30 rows with strictly non-decreasing dates
    assert len(rows) == 30
    dates = [r[0] for r in rows]
    assert dates == sorted(dates)


async def test_aggregate_returns_columnar_result(
    client: ch.Client, covid19: str
) -> None:
    # BEGIN: the columnar retrieval surface is available
    # WHEN: calling fetch_columns
    result = await client.fetch_columns(
        f"SELECT date, location_key FROM {covid19} LIMIT 100"
    )

    # THEN: a ColumnarResult with correctly-typed column-major data
    assert isinstance(result, ColumnarResult)
    assert [c.name for c in result.columns] == ["date", "location_key"]
    assert result.rows == 100
    first_date = result.data[0][0]
    assert isinstance(first_date, date)
    assert not isinstance(first_date, datetime)
