"""Smoke tests for the ``_fast_read`` C extension.

The extension is a hard requirement now (no pure-Python fallback).
These tests pin the contract:

- ``_fast_read`` imports cleanly as a submodule of ``clickhouse_async``.
- It exposes ``__version__`` and a working ``available()`` callable.
- ``decode_strings`` and ``decode_datetime`` are exposed and callable.

Round-trip parity with the codecs is covered by the regular type-suite
tests; these tests are the canary that the wheel actually shipped the
extension.
"""

from __future__ import annotations

from clickhouse_async import _fast_read


def test_fast_read_imports() -> None:
    # WHEN / THEN: the module loads and has both decode entry points
    assert hasattr(_fast_read, "decode_strings")
    assert hasattr(_fast_read, "decode_datetime")


def test_fast_read_smoke() -> None:
    # WHEN: invoking the no-op smoke-test callable
    result = _fast_read.available()

    # THEN: it returns True and a `__version__` is exposed
    assert result is True
    assert isinstance(_fast_read.__version__, str)
    assert _fast_read.__version__  # non-empty
