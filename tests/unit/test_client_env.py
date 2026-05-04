"""Tests for the OS/environment helpers in _client_env.py."""

from __future__ import annotations

import getpass
import socket

import pytest

from clickhouse_async.protocol._client_env import _safe_hostname, _safe_os_user


@pytest.mark.parametrize("exc_type", [OSError, KeyError])
def test_safe_os_user_returns_empty_string_on_getuser_error(
    monkeypatch: pytest.MonkeyPatch,
    exc_type: type[Exception],
) -> None:
    # BEGIN: an environment where getpass.getuser raises an OS-level error
    monkeypatch.setattr(getpass, "getuser", lambda: (_ for _ in ()).throw(exc_type()))

    # WHEN: _safe_os_user is called
    result = _safe_os_user()

    # THEN: falls back to empty string
    assert result == ""


def test_safe_hostname_returns_empty_string_on_os_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # BEGIN: an environment where socket.gethostname raises OSError
    monkeypatch.setattr(socket, "gethostname", lambda: (_ for _ in ()).throw(OSError()))

    # WHEN: _safe_hostname is called
    result = _safe_hostname()

    # THEN: falls back to empty string
    assert result == ""
