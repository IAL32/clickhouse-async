"""Unit tests for `_default_compression()` and the compression auto-detect path."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from clickhouse_async.connection import _default_compression
from clickhouse_async.protocol.compression import CompressionMethod


def test_default_compression_is_none_without_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # BEGIN: simulate a bare install where lz4 is not importable
    monkeypatch.delenv("CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION", raising=False)
    # Remove any cached lz4 module and force ImportError on next import
    lz4_backup = sys.modules.pop("lz4", None)
    lz4_block_backup = sys.modules.pop("lz4.block", None)
    try:
        with patch.dict(sys.modules, {"lz4": None, "lz4.block": None}):
            # WHEN: _default_compression is called without lz4 installed
            result = _default_compression()

        # THEN: falls back to NONE
        assert result == CompressionMethod.NONE
    finally:
        if lz4_backup is not None:
            sys.modules["lz4"] = lz4_backup
        if lz4_block_backup is not None:
            sys.modules["lz4.block"] = lz4_block_backup


@pytest.mark.parametrize(
    "env_value", ["off", "none", "false", "0", "OFF", "None", "FALSE"]
)
def test_env_var_suppresses_auto_lz4(
    monkeypatch: pytest.MonkeyPatch, env_value: str
) -> None:
    # BEGIN: env var set to opt out of compression
    monkeypatch.setenv("CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION", env_value)

    # WHEN: _default_compression is called (even if lz4 would be importable)
    result = _default_compression()

    # THEN: always NONE regardless of lz4 availability
    assert result == CompressionMethod.NONE


def test_default_compression_is_lz4_with_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    # BEGIN: simulate lz4 being installed by injecting a fake module
    monkeypatch.delenv("CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION", raising=False)
    fake_lz4_block = MagicMock()
    with patch.dict(sys.modules, {"lz4": MagicMock(), "lz4.block": fake_lz4_block}):
        # WHEN: _default_compression is called with lz4 available
        result = _default_compression()

    # THEN: returns LZ4
    assert result == CompressionMethod.LZ4
