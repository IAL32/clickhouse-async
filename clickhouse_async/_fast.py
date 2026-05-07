"""Single import site for the optional ``_fast_read`` C extension.

Codecs that want the C-accelerated path read ``_fast.module`` and
fall back to their pure-Python implementation when it's ``None``.
Centralising the ``try/except ImportError`` here keeps the per-codec
files clean and makes the bare-install discipline easy to audit:
one grep for ``_fast.module`` finds every fast-path call site.

The extension is built optionally (``setup.py`` declares
``optional=True``); installs without a working C compiler still
succeed, and ``module is None`` exercises the pure-Python fallback.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

# Resolve the extension at module import time and keep a private
# reference. ``importlib`` lets ty stay quiet about a module it
# can't see in source — the C extension is built dynamically and
# may or may not exist at type-check time.
module: ModuleType | None
try:
    module = importlib.import_module("clickhouse_async._fast_read")
except ImportError:  # pragma: no cover — exercised when the extension is missing
    module = None


def is_available() -> bool:
    """Return ``True`` if the C extension was built and loaded."""
    return module is not None
