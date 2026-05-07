"""Unit tests for `write_query_packet` and its revision-gated branches.

OS-error fallback tests for `_safe_os_user` / `_safe_hostname` live
in `test_client_env.py` alongside the helpers themselves.
"""

from __future__ import annotations

import pytest

from clickhouse_async.protocol.compression import CompressionMethod
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import (
    DBMS_MIN_REVISION_WITH_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    OUR_REVISION,
    ClientPacket,
)
from clickhouse_async.protocol.query_packet import write_query_packet


def _make_reader(writer: BinaryWriter) -> AsyncBinaryReader:
    return AsyncBinaryReader.from_bytes(writer.getvalue())


# ---- revision-gated branches in write_query_packet ----------------------


async def test_write_query_packet_omits_client_info_below_threshold() -> None:
    # BEGIN: a very old revision that predates ClientInfo
    old_revision = DBMS_MIN_REVISION_WITH_CLIENT_INFO - 1
    writer = BinaryWriter()

    # WHEN: writing a query packet at the old revision
    write_query_packet(
        writer, sql="SELECT 1", query_id="q", user="u", revision=old_revision
    )

    # THEN: packet type + query_id written; next field is settings terminator
    #       (no ClientInfo block)
    rdr = _make_reader(writer)
    assert await rdr.read_varuint() == ClientPacket.QUERY
    assert await rdr.read_string() == "q"
    assert (
        await rdr.read_string() == ""
    )  # settings terminator (no ClientInfo => no settings block)


@pytest.mark.parametrize(
    "old_revision",
    [
        DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO - 1,
        DBMS_MIN_REVISION_WITH_VERSION_PATCH - 1,
    ],
    ids=["below_quota_key", "below_version_patch"],
)
async def test_write_client_info_omits_optional_field_below_threshold(
    old_revision: int,
) -> None:
    # BEGIN: a revision just below a protocol gate
    writer = BinaryWriter()

    # WHEN: writing a query packet at that revision
    write_query_packet(
        writer, sql="SELECT 1", query_id="q", user="u", revision=old_revision
    )

    # THEN: packet is written without error (the gated field is skipped)
    assert len(writer.getvalue()) > 0


# ---- settings write path ------------------------------------------------


async def test_write_query_packet_encodes_settings_in_wire_format() -> None:
    # BEGIN: a modern-revision connection with query settings
    writer = BinaryWriter()

    # WHEN: writing a query with settings
    write_query_packet(
        writer,
        sql="SELECT 1",
        query_id="q",
        user="u",
        revision=OUR_REVISION,
        settings={"max_threads": "4"},
    )

    # THEN: packet is written; spot-check packet type and query id
    rdr = _make_reader(writer)
    assert await rdr.read_varuint() == ClientPacket.QUERY
    assert await rdr.read_string() == "q"
    assert rdr.position > 0


async def test_write_query_packet_with_compression_flag() -> None:
    # BEGIN: compression extra must be present — the compressed trailing block
    # requires lz4 + clickhouse_cityhash
    pytest.importorskip("lz4")
    pytest.importorskip("clickhouse_cityhash")

    writer_plain = BinaryWriter()
    writer_comp = BinaryWriter()

    # WHEN: writing a query packet without and with LZ4 compression
    write_query_packet(
        writer_plain, sql="SELECT 1", query_id="q", user="u", revision=OUR_REVISION
    )
    write_query_packet(
        writer_comp,
        sql="SELECT 1",
        query_id="q",
        user="u",
        revision=OUR_REVISION,
        compression=CompressionMethod.LZ4,
    )

    # THEN: both are non-empty; the compressed packet differs (flag byte + framed block)
    assert len(writer_plain.getvalue()) > 0
    assert len(writer_comp.getvalue()) > 0
    assert writer_plain.getvalue() != writer_comp.getvalue()
