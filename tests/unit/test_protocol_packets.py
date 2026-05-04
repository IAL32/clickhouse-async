"""Tests for the protocol packet codes and revision gates."""

from __future__ import annotations

import inspect

from clickhouse_async.protocol import packets
from clickhouse_async.protocol.packets import (
    OUR_REVISION,
    ClientPacket,
    ServerPacket,
)


def test_client_packet_values_match_upstream() -> None:
    # BEGIN: the canonical client packet ids from upstream Protocol.h
    expected = {
        "HELLO": 0,
        "QUERY": 1,
        "DATA": 2,
        "CANCEL": 3,
        "PING": 4,
        "TABLES_STATUS_REQUEST": 5,
        "KEEP_ALIVE": 6,
        "SCALAR": 7,
        "IGNORED_PART_UUIDS": 8,
        "READ_TASK_RESPONSE": 9,
        "MERGE_TREE_READ_TASK_RESPONSE": 10,
        "SSH_CHALLENGE_REQUEST": 11,
        "SSH_CHALLENGE_RESPONSE": 12,
    }

    # WHEN: collecting the members of ClientPacket
    actual = {m.name: m.value for m in ClientPacket}

    # THEN: every name and value matches upstream byte-for-byte
    assert actual == expected


def test_server_packet_values_match_upstream() -> None:
    # BEGIN: the canonical server packet ids from upstream Protocol.h
    expected = {
        "HELLO": 0,
        "DATA": 1,
        "EXCEPTION": 2,
        "PROGRESS": 3,
        "PONG": 4,
        "END_OF_STREAM": 5,
        "PROFILE_INFO": 6,
        "TOTALS": 7,
        "EXTREMES": 8,
        "TABLES_STATUS_RESPONSE": 9,
        "LOG": 10,
        "TABLE_COLUMNS": 11,
        "PART_UUIDS": 12,
        "READ_TASK_REQUEST": 13,
        "PROFILE_EVENTS": 14,
        "MERGE_TREE_ALL_RANGES_ANNOUNCEMENT": 15,
        "MERGE_TREE_READ_TASK_REQUEST": 16,
        "TIMEZONE_UPDATE": 17,
        "SSH_CHALLENGE": 18,
    }

    # WHEN: collecting the members of ServerPacket
    actual = {m.name: m.value for m in ServerPacket}

    # THEN: every name and value matches upstream byte-for-byte
    assert actual == expected


def test_our_revision_is_at_least_every_declared_gate() -> None:
    # BEGIN: every revision gate declared in packets.py
    gates = {
        name: value
        for name, value in inspect.getmembers(packets)
        if name.startswith(("DBMS_MIN_REVISION_", "DBMS_MIN_PROTOCOL_VERSION_"))
        and isinstance(value, int)
    }

    # WHEN: collecting any gate that exceeds OUR_REVISION
    too_high = {n: v for n, v in gates.items() if v > OUR_REVISION}

    # THEN: no gate is higher than the revision we claim — otherwise the
    #       handshake would advertise features we don't implement
    assert not too_high, (
        f"OUR_REVISION ({OUR_REVISION}) is below: {too_high}. "
        "Bump OUR_REVISION when adding new gates, or remove the unused gate."
    )


def test_our_revision_pinned_to_tcp_protocol_version() -> None:
    # BEGIN: the upstream DBMS_TCP_PROTOCOL_VERSION from ClickHouse 26.5.1
    upstream_tcp_protocol_version = 54483

    # WHEN: reading OUR_REVISION
    pinned = OUR_REVISION

    # THEN: the pin matches; bumping requires touching .clickhouse-version too
    assert pinned == upstream_tcp_protocol_version
