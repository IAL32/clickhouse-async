#!/usr/bin/env bash
# Run a bare ClickHouse server for local development.
#
# Matches the canonical DSN used by --localdb and tests/containers/clickhouse.py:
#   clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse
#
# Version resolution (highest priority first):
#   1. positional arg:           ./scripts/clickhouse.sh up 25.3
#   2. CLICKHOUSE_VERSION env:   CLICKHOUSE_VERSION=25.3 ./scripts/clickhouse.sh up
#   3. .clickhouse-version file at the repo root
#   4. fallback: 24.8

set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-clickhouse-async-dev}"
NATIVE_PORT="${NATIVE_PORT:-9000}"
HTTP_PORT="${HTTP_PORT:-8123}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="$ROOT/.clickhouse-version"

resolve_version() {
    if [[ -n "${CLICKHOUSE_VERSION:-}" ]]; then
        printf '%s' "$CLICKHOUSE_VERSION"
    elif [[ -f "$VERSION_FILE" ]]; then
        tr -d '[:space:]' < "$VERSION_FILE"
    else
        printf '24.8'
    fi
}

dsn() {
    printf 'clickhouse://clickhouse:clickhouse@localhost:%s/clickhouse' "$NATIVE_PORT"
}

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [version]

Commands:
  up [version]    Start ClickHouse (default: $(resolve_version))
  down            Stop and remove the container
  restart [ver]   down + up
  logs [-f]       Tail container logs (pass -f to follow)
  shell           Open clickhouse-client inside the container
  status          Show container status (default if no command given)
  version         Print the version that would be used by 'up'

Env overrides:
  CLICKHOUSE_VERSION   override version
  CONTAINER_NAME       container name (default: clickhouse-async-dev)
  NATIVE_PORT          host port for :9000 (default: 9000)
  HTTP_PORT            host port for :8123 (default: 8123)

Connection (when running):
  $(dsn)
EOF
}

is_running() {
    [[ -n "$(docker ps --filter "name=^/${CONTAINER_NAME}$" --filter "status=running" --quiet)" ]]
}

exists() {
    [[ -n "$(docker ps -a --filter "name=^/${CONTAINER_NAME}$" --quiet)" ]]
}

require_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "error: docker is not installed or not on PATH" >&2
        exit 1
    fi
}

up() {
    require_docker
    local version="${1:-$(resolve_version)}"

    if is_running; then
        local current_image
        current_image=$(docker inspect --format '{{.Config.Image}}' "$CONTAINER_NAME")
        echo "ClickHouse already running ($current_image)"
        echo "DSN: $(dsn)"
        return 0
    fi

    if exists; then
        echo "Removing stopped container '$CONTAINER_NAME'..."
        docker rm "$CONTAINER_NAME" >/dev/null
    fi

    echo "Starting clickhouse/clickhouse-server:${version} on :${NATIVE_PORT} (native), :${HTTP_PORT} (http)..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        --ulimit nofile=262144:262144 \
        -e CLICKHOUSE_USER=clickhouse \
        -e CLICKHOUSE_PASSWORD=clickhouse \
        -e CLICKHOUSE_DB=clickhouse \
        -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
        -p "${NATIVE_PORT}:9000" \
        -p "${HTTP_PORT}:8123" \
        "clickhouse/clickhouse-server:${version}" >/dev/null

    printf 'Waiting for server'
    for _ in $(seq 1 30); do
        if docker exec "$CONTAINER_NAME" \
                clickhouse-client --user clickhouse --password clickhouse \
                --query "SELECT 1" >/dev/null 2>&1; then
            printf '\nReady. DSN: %s\n' "$(dsn)"
            return 0
        fi
        printf '.'
        sleep 1
    done
    printf '\n'
    echo "error: server did not become ready within 30s" >&2
    echo "       check logs with: $(basename "$0") logs" >&2
    return 1
}

down() {
    require_docker
    if ! exists; then
        echo "No container named '$CONTAINER_NAME'"
        return 0
    fi
    docker rm -f "$CONTAINER_NAME" >/dev/null
    echo "Stopped and removed '$CONTAINER_NAME'"
}

logs() {
    require_docker
    docker logs "$@" "$CONTAINER_NAME"
}

shell() {
    require_docker
    if ! is_running; then
        echo "error: container is not running. Start with: $(basename "$0") up" >&2
        exit 1
    fi
    docker exec -it "$CONTAINER_NAME" \
        clickhouse-client --user clickhouse --password clickhouse --database clickhouse
}

status() {
    require_docker
    if is_running; then
        local image
        image=$(docker inspect --format '{{.Config.Image}}' "$CONTAINER_NAME")
        echo "running  ($image)"
        echo "DSN: $(dsn)"
    elif exists; then
        echo "stopped  (run: $(basename "$0") up)"
    else
        echo "not started  (run: $(basename "$0") up)"
    fi
}

cmd="${1:-status}"
shift || true

case "$cmd" in
    up)             up "$@" ;;
    down|stop)      down ;;
    restart)        down; up "$@" ;;
    logs)           logs "$@" ;;
    shell|cli)      shell ;;
    status)         status ;;
    version)        resolve_version; echo ;;
    -h|--help|help) usage ;;
    *)              echo "error: unknown command '$cmd'" >&2; usage; exit 2 ;;
esac
