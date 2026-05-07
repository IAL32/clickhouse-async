"""Tests for the optional ``_fast_read`` C extension scaffolding.

The C extension is built when a working C compiler is available and
``setup.py`` ran during install; otherwise it's absent and the Python
codecs fall back. The contract these tests pin:

- ``import clickhouse_async`` succeeds either way (bare-install discipline).
- ``clickhouse_async._fast`` exposes ``module`` (the extension) or
  ``None``; ``is_available()`` mirrors that boolean.
- When loaded, the module exposes ``__version__`` and a working
  ``available()`` callable. Codec-level fast paths in subsequent
  commits will key off these.
"""

from __future__ import annotations

import importlib

import pytest

from clickhouse_async import _fast


def test_import_clickhouse_async_does_not_require_fast_extension() -> None:
    # BEGIN: the package itself
    # WHEN: importing it
    importlib.import_module("clickhouse_async")
    # THEN: the import succeeds whether or not the .abi3.so was built
    #       — the extension is wrapped in a try/except in `_fast.py`.


def test_fast_module_is_module_or_none() -> None:
    # BEGIN: the lazy-import shim
    # WHEN / THEN: `module` is either the loaded extension or `None`
    assert _fast.module is None or hasattr(_fast.module, "available")


def test_is_available_matches_module_presence() -> None:
    # BEGIN: the lazy-import shim
    # WHEN / THEN: `is_available()` agrees with `module is not None`
    assert _fast.is_available() is (_fast.module is not None)


@pytest.mark.skipif(
    _fast.module is None,
    reason="C extension not built — exercising the fallback path instead",
)
def test_fast_module_smoke() -> None:
    # BEGIN: the loaded C extension
    assert _fast.module is not None  # type: narrowing for ty / mypy

    # WHEN: invoking its smoke-test callable
    result = _fast.module.available()

    # THEN: the no-op stub returns True and a `__version__` is exposed
    assert result is True
    assert isinstance(_fast.module.__version__, str)
    assert _fast.module.__version__  # non-empty
