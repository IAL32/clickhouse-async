"""Best-effort OS/environment queries used to populate ClientInfo.

Extracted from query_packet.py so they can be tested independently.
ClickHouse records empty strings for fields it can't populate, so all
helpers here swallow OS errors and return a safe fallback.
"""

from __future__ import annotations

import getpass
import socket


def _safe_os_user() -> str:
    """Best-effort current OS user. ClickHouse just records "" if absent."""
    try:
        return getpass.getuser()
    except (OSError, KeyError):
        return ""


def _safe_hostname() -> str:
    """Best-effort local hostname. ClickHouse just records "" if absent."""
    try:
        return socket.gethostname()
    except OSError:
        return ""
