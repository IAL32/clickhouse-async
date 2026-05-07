"""Build configuration for the ``_fast_read`` C extension.

Package metadata, Python sources, dependencies, and tooling config
live in ``pyproject.toml``. This file exists for one reason:
setuptools declarative TOML config doesn't yet cover ``ext_modules``,
so the C extension declaration has to live in ``setup.py``.

The extension is **required** — the codecs that route through it
(``String.read``, ``DateTime.read``) no longer carry pure-Python
fallbacks. Source builds without a working C compiler will fail; the
PyPI wheel matrix (built via cibuildwheel) covers the common
platforms so binary installs don't need a compiler at all.
``py_limited_api=True`` plus ``Py_LIMITED_API = 0x030B0000`` means
one ``cp311-abi3`` wheel per platform covers Python 3.11+.
"""

from __future__ import annotations

from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            name="clickhouse_async._fast_read",
            sources=["clickhouse_async/_fast_read.c"],
            py_limited_api=True,
            define_macros=[("Py_LIMITED_API", "0x030B0000")],
        ),
    ],
)
