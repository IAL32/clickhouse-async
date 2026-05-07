"""Per-library async adapters with a uniform interface.

The scenarios import :func:`get_adapter` to look up an adapter by
short name (``ca`` / ``asynch_pypi`` / ``asynch_tacto`` / ``cc`` /
``cc_async``); each adapter exposes a single ``connect()`` async
context manager that yields an :class:`AdapterClient` with the
methods the scenarios call.

Adapters are intentionally thin — they document each library's
"recommended way" to do the same operation, so the comparison stays
honest. If a library has a bulk-insert helper that's faster than its
DBAPI cursor, the adapter uses the helper.
"""

from __future__ import annotations

from .base import Adapter, AdapterClient

__all__ = ["Adapter", "AdapterClient", "get_adapter"]


def get_adapter(name: str, dsn_native: str, dsn_http: str) -> Adapter:
    """Return the adapter for one of the five library short names.

    - ``ca``           — clickhouse-async (this project)
    - ``asynch_pypi``  — asynch from PyPI (``long2ice/asynch``)
    - ``asynch_tacto`` — asynch tacto fork (``nils-borrmann-tacto/asynch``)
    - ``cc``           — clickhouse-connect 0.15.x (HTTP, async = thread-pool wrapper)
    - ``cc_async``     — clickhouse-connect 1.0.0rc2 with ``[async]`` (HTTP, native async)

    Pairs that share a Python module — ``asynch_pypi`` /
    ``asynch_tacto`` share ``asynch``; ``cc`` / ``cc_async`` share
    ``clickhouse-connect`` — can only have one member installed at a
    time. The short name is a label only; both members of each pair
    go through the same adapter class. Native-protocol adapters get
    ``dsn_native``; HTTP adapters get ``dsn_http``. Lazy imports keep
    the missing-library case from blocking the others.
    """
    if name == "ca":
        from .ca_adapter import ClickhouseAsyncAdapter

        return ClickhouseAsyncAdapter(dsn_native)
    if name in ("asynch_pypi", "asynch_tacto"):
        from .asynch_adapter import AsynchAdapter

        return AsynchAdapter(dsn_native)
    if name in ("cc", "cc_async"):
        from .cc_adapter import ClickhouseConnectAdapter

        return ClickhouseConnectAdapter(dsn_http)
    raise ValueError(
        f"unknown adapter {name!r}; choose from "
        f"ca / asynch_pypi / asynch_tacto / cc / cc_async"
    )
