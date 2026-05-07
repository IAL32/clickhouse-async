"""Build configuration for the optional ``_fast_read`` C extension.

Package metadata, Python sources, dependencies, and tooling config
live in ``pyproject.toml``. This file exists for one reason: setuptools
declarative TOML config doesn't yet cover ``ext_modules``, so the C
extension declaration has to live in ``setup.py``.

``optional=True`` means the extension is best-effort — installs without
a working C compiler still succeed and the codecs fall back to their
pure-Python implementations. Combined with ``py_limited_api=True``, one
``.abi3.so`` wheel covers Python 3.11+ across the supported platforms.
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
            optional=True,
        ),
    ],
)
