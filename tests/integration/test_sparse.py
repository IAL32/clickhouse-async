"""End-to-end sparse-column reads against a real ClickHouse server.

ClickHouse's MergeTree engine auto-enables sparse serialisation when the
ratio of default-valued rows in a column hits
`ratio_of_defaults_for_sparse_serialization` (0.95 by default). The
decision is part-merge-time, not query-time — so the test inserts
default-valued rows, runs `OPTIMIZE TABLE … FINAL` to force the merge,
then reads back via both the row-major and columnar surfaces. Without
the sparse decoder, every assertion below would surface a
`ProtocolError` instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import clickhouse_async as ch


async def test_sparse_uint8_column_round_trips_via_fetch_all_and_fetch_columns(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a MergeTree table whose `v` column has a DEFAULT and a
    #        load shape that trips the sparse threshold (≥ 95 % defaults
    #        triggers the encoding when the part is merged)
    table = "test_sparse_uint8"
    await fresh_table(
        table,
        "(id UInt64, v UInt8 DEFAULT 0) ENGINE = MergeTree ORDER BY id",
    )
    async with pool.acquire() as client:
        await client.insert(
            f"INSERT INTO {table} (id, v) VALUES",
            rows=[(i, 0) for i in range(100)],
            column_names=("id", "v"),
        )
        await client.insert(
            f"INSERT INTO {table} (id, v) VALUES",
            rows=[(1000 + i, 1) for i in range(5)],
            column_names=("id", "v"),
        )
        # FINAL is mandatory — sparse is decided at part-merge time, not
        # at insert time, so without it the read may still hit the dense
        # codec and the test would silently pass against the wrong path.
        await client.execute(f"OPTIMIZE TABLE {table} FINAL")

    async with pool.acquire() as client:
        # WHEN: reading the column via the row-major surface
        rows = await client.fetch_all(f"SELECT v FROM {table} ORDER BY id")

        # THEN: every default row is decoded as 0; the 5 non-defaults
        #       land at positions 100-104 (sorted by id, the non-default
        #       ids 1000-1004 sort to the end)
        assert len(rows) == 105
        assert all(r[0] == 0 for r in rows[:100])
        assert [r[0] for r in rows[100:]] == [1, 1, 1, 1, 1]

        # WHEN: reading via the columnar surface — same codec, different
        #       call site; the sparse path must satisfy both
        cols = await client.fetch_columns(f"SELECT v FROM {table} ORDER BY id")

        # THEN: identical reconstruction
        assert list(cols.data[0]) == [0] * 100 + [1] * 5

        # WHEN: reusing the same connection for a follow-up query
        followup = await client.fetch_all(f"SELECT count() FROM {table} WHERE v = 0")

        # THEN: the connection stayed READY past the sparse read — under
        #       the v0.3.1 ProtocolError path it would have flipped to
        #       BROKEN here and the next query would never reach the wire
        assert followup == [(100,)]


async def test_sparse_uint8_column_with_interleaved_non_defaults(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a layout where non-defaults sit at the start, middle, and
    #        end of the sorted block — exercises the group-size walk
    #        rather than the trivial all-at-the-end case
    table = "test_sparse_interleaved"
    await fresh_table(
        table,
        "(id UInt64, v UInt8 DEFAULT 0) ENGINE = MergeTree ORDER BY id",
    )
    default_ids = list(range(1, 50)) + list(range(51, 99))
    non_default_ids = [0, 50, 99]
    async with pool.acquire() as client:
        await client.insert(
            f"INSERT INTO {table} (id, v) VALUES",
            rows=[(i, 0) for i in default_ids],
            column_names=("id", "v"),
        )
        await client.insert(
            f"INSERT INTO {table} (id, v) VALUES",
            rows=[(i, 1) for i in non_default_ids],
            column_names=("id", "v"),
        )
        await client.execute(f"OPTIMIZE TABLE {table} FINAL")

    # WHEN: reading the column
    rows = await pool.fetch_all(f"SELECT v FROM {table} ORDER BY id")

    # THEN: non-defaults appear at sorted positions 0, 50, 99 — the
    #       reconstruction walks group_sizes correctly, not just appends
    assert len(rows) == 100
    assert [i for i, r in enumerate(rows) if r[0] != 0] == [0, 50, 99]
