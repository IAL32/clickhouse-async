"""Project-local ``ClickHouseContainer`` subclass.

The only path the integration tests use to spin up a server. Owns:

- Image pinning (``DEFAULT_VERSION`` is read from ``.clickhouse-version``
  at the repo root, the same source ``scripts/clickhouse.sh`` reads —
  bumping the file is the single edit that updates both).
- Port exposure: the native protocol on ``:9000`` and HTTP on ``:8123``
  (the latter is convenient for healthchecks / debugging, not used by
  the client itself).
- Default credentials matching the canonical project DSN:
  user / password / db all ``"clickhouse"``.
- A few debug helpers — ``tail_logs``, ``exec_sql``, ``shell`` — so a
  flaky test can be inspected without re-deriving the docker invocations.
- A ``dsn`` property that produces the connection string the rest of
  the suite hands to ``ch.connect`` / ``ch.create_pool``.

Per project conventions: integration tests **do not** invoke
``docker run`` directly, do **not** depend on ``docker-compose``, and
do **not** use the upstream ``testcontainers.clickhouse`` module. The
subclass is the single source of truth.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

# `parents[2]` walks: clickhouse.py → containers/ → tests/ → repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_VERSION_FILE = _REPO_ROOT / ".clickhouse-version"
DEFAULT_VERSION = _VERSION_FILE.read_text().strip()
CLICKHOUSE_IMAGE = f"clickhouse/clickhouse-server:{DEFAULT_VERSION}"

NATIVE_PORT = 9000
HTTP_PORT = 8123

DEFAULT_USER = "clickhouse"
DEFAULT_PASSWORD = "clickhouse"
DEFAULT_DATABASE = "clickhouse"


class ClickHouseContainer(DockerContainer):
    """A pinned ClickHouse server in a Docker container."""

    def __init__(self, image: str = CLICKHOUSE_IMAGE) -> None:
        super().__init__(image)
        self.with_exposed_ports(NATIVE_PORT, HTTP_PORT)
        self.with_env("CLICKHOUSE_USER", DEFAULT_USER)
        self.with_env("CLICKHOUSE_PASSWORD", DEFAULT_PASSWORD)
        self.with_env("CLICKHOUSE_DB", DEFAULT_DATABASE)
        # CH refuses to start with the default soft nofile on macOS / some
        # Linux distros.
        self.with_kwargs(
            ulimits=[{"name": "nofile", "soft": 262144, "hard": 262144}]
        )

    def start(self) -> ClickHouseContainer:
        super().start()
        # CH writes "Ready for connections" once the TCP listeners are up;
        # 60 s is generous for the cold-start image pull on a slow disk.
        wait_for_logs(self, "Ready for connections", timeout=60)
        return self

    @property
    def dsn(self) -> str:
        """A ``clickhouse://`` DSN pointing at the running container."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(NATIVE_PORT)
        return (
            f"clickhouse://{DEFAULT_USER}:{DEFAULT_PASSWORD}"
            f"@{host}:{port}/{DEFAULT_DATABASE}"
        )

    # ---- debug helpers --------------------------------------------------

    def tail_logs(self, lines: int = 200) -> str:
        """Return the last ``lines`` of the container's stdout/stderr.

        Useful when a query fails in a way the client surfaces as
        ``ServerError`` and the developer wants the server's
        perspective without leaving pytest.
        """
        result = subprocess.run(
            [
                "docker",
                "logs",
                "--tail",
                str(lines),
                self.get_wrapped_container().id,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return (result.stdout or "") + (result.stderr or "")

    def exec_sql(self, sql: str) -> str:
        """Run ``sql`` via the in-container ``clickhouse-client`` binary
        and return its stdout. Used by tests that need to inspect
        server-side state (e.g. ``system.query_log``) without going
        through the async client under test."""
        cmd = [
            "clickhouse-client",
            "--user",
            DEFAULT_USER,
            "--password",
            DEFAULT_PASSWORD,
            "--database",
            DEFAULT_DATABASE,
            "--query",
            sql,
        ]
        exit_code, output = self.exec(cmd)
        if exit_code != 0:
            raise RuntimeError(
                f"clickhouse-client exited {exit_code}: {output!r}"
            )
        if isinstance(output, bytes):
            return output.decode("utf-8", errors="replace")
        return str(output)

    def shell(self) -> int:
        """Drop into an interactive ``clickhouse-client`` shell inside
        the container.

        Returns the exit status. Intended for ad-hoc debugging during
        an interactive ``pytest --pdb`` session — not from automated
        tests.
        """
        return os.system(
            f"docker exec -it {self.get_wrapped_container().id} "
            f"clickhouse-client --user {DEFAULT_USER} "
            f"--password {DEFAULT_PASSWORD} "
            f"--database {DEFAULT_DATABASE}"
        )


__all__ = ["CLICKHOUSE_IMAGE", "DEFAULT_VERSION", "ClickHouseContainer"]
