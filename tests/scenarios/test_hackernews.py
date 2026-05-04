"""Scenario tests against the Hacker News dataset.

Source: https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet
Scale: 200 000 rows (LIMIT imposed at load time)
Types exercised: UInt32, UInt8, LowCardinality(String), DateTime, String,
                 Int32, Array(UInt32), and the ARRAY JOIN table function.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import clickhouse_async as ch


async def test_total_row_count(client: ch.Client, hackernews: str) -> None:
    # BEGIN: the hackernews table was loaded with LIMIT 200 000
    # WHEN: counting all rows
    rows = await client.fetch_all(f"SELECT count() FROM {hackernews}")

    # THEN: exactly 200 000 rows were written
    assert rows[0][0] == 200_000


async def test_post_types(client: ch.Client, hackernews: str) -> None:
    # BEGIN: HN posts have several types (story, comment, job, …)
    # WHEN: collecting all distinct types
    rows = await client.fetch_all(
        f"SELECT DISTINCT type FROM {hackernews} ORDER BY type"
    )

    # THEN: at least 'story' or 'comment' is present
    types = {r[0] for r in rows}
    assert types & {"story", "comment"}


async def test_time_decoded_as_datetime(client: ch.Client, hackernews: str) -> None:
    # BEGIN: the time column is typed DateTime
    # WHEN: fetching one story timestamp
    rows = await client.fetch_all(
        f"SELECT time FROM {hackernews} WHERE type = 'story' LIMIT 1"
    )

    # THEN: the value is a datetime instance
    assert len(rows) == 1
    assert isinstance(rows[0][0], datetime)


async def test_top_authors(client: ch.Client, hackernews: str) -> None:
    # BEGIN: the by column holds submitter names (non-empty for active posts)
    # WHEN: finding the 10 most prolific authors
    rows = await client.fetch_all(
        f"SELECT by, count() FROM {hackernews} "
        f"WHERE by != '' GROUP BY by ORDER BY 2 DESC LIMIT 10"
    )

    # THEN: 10 rows with non-empty string author names
    assert len(rows) == 10
    for by, _cnt in rows:
        assert isinstance(by, str)
        assert by != ""


async def test_kids_is_list(client: ch.Client, hackernews: str) -> None:
    # BEGIN: kids is Array(UInt32) — child comment IDs
    # WHEN: selecting posts that have at least one child
    rows = await client.fetch_all(
        f"SELECT kids FROM {hackernews} WHERE length(kids) > 0 LIMIT 5"
    )

    # THEN: each value is a Python list of ints
    assert len(rows) > 0
    for (kids,) in rows:
        assert isinstance(kids, list)
        assert all(isinstance(k, int) for k in kids)


async def test_array_length_filter(client: ch.Client, hackernews: str) -> None:
    # BEGIN: some posts have many child comment IDs
    # WHEN: finding the post with the most children
    rows = await client.fetch_all(
        f"SELECT id, length(kids) FROM {hackernews} ORDER BY length(kids) DESC LIMIT 1"
    )

    # THEN: the length is a positive int
    assert len(rows) == 1
    assert isinstance(rows[0][1], int)
    assert rows[0][1] > 0


async def test_array_join_expands_rows(client: ch.Client, hackernews: str) -> None:
    # BEGIN: ARRAY JOIN flattens the kids array into individual rows
    # WHEN: ARRAY JOIN-ing the first 10 parent-child pairs
    rows = await client.fetch_all(
        f"SELECT id, k FROM {hackernews} "
        f"ARRAY JOIN kids AS k WHERE length(kids) > 0 LIMIT 10"
    )

    # THEN: 10 rows where each kid ID is an int
    assert len(rows) == 10
    for _id, k in rows:
        assert isinstance(k, int)


async def test_monthly_activity(client: ch.Client, hackernews: str) -> None:
    # BEGIN: posts span several calendar months
    # WHEN: grouping by start-of-month and ordering chronologically
    rows = await client.fetch_all(
        f"SELECT toStartOfMonth(time) AS month, count() "
        f"FROM {hackernews} GROUP BY month ORDER BY month"
    )

    # THEN: months are in ascending order and typed as date (not datetime)
    assert len(rows) > 0
    months = [r[0] for r in rows]
    assert months == sorted(months)
    assert isinstance(months[0], date)
    assert not isinstance(months[0], datetime)


async def test_score_is_int(client: ch.Client, hackernews: str) -> None:
    # BEGIN: score is typed Int32 for story posts
    # WHEN: computing the min and max score across stories
    rows = await client.fetch_all(
        f"SELECT min(score), max(score) FROM {hackernews} WHERE type = 'story'"
    )

    # THEN: both are ints and min ≤ max
    min_s, max_s = rows[0]
    assert isinstance(min_s, int)
    assert isinstance(max_s, int)
    assert min_s <= max_s
