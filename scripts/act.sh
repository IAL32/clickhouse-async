#!/usr/bin/env bash
# Run the GitHub Actions workflows locally with `act`.
#
# Why: GitHub CI takes ~25–60 s per round-trip (image pull, runner
# allocation, cache restore). Running the same workflow locally with
# `act` cuts the loop to ~10 s for unit tests and ~30 s including the
# integration suite, so we can iterate on workflow / test changes
# without burning CI minutes or waiting on remote runners.
#
# Usage:
#   ./scripts/act.sh full            # run the full job (unit + integration)
#                                    # for a single Python version (3.12)
#   ./scripts/act.sh full all        # run the full matrix (3.11/3.12/3.13)
#                                    # WARNING: matrix jobs share the host's
#                                    # Docker daemon; the parallel jobs will
#                                    # race on the ClickHouse container name
#                                    # (act-specific limitation, not real CI).
#   ./scripts/act.sh unit            # run only the bare unit job
#   ./scripts/act.sh lint            # run only the lint+types job
#   ./scripts/act.sh scenarios       # run the example scenarios job
#   ./scripts/act.sh prek            # run only the prek workflow
#
# Requirements:
#   - act        (brew install act)
#   - docker     (running, with the user's socket accessible)
#
# Configuration lives in `.actrc` at the repo root — see that file for
# the explanation of each flag.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: $1 is not installed or not on PATH" >&2
        echo "       install with: $2" >&2
        exit 1
    fi
}

require act "brew install act"
require docker "https://docs.docker.com/get-docker/"

# Make sure no stale ClickHouse container survives from a previous run —
# matrix jobs in act share the host Docker daemon, and a leftover
# container with the same name would short-circuit the up() probe and
# mask a real failure.
docker rm -f clickhouse-async-dev >/dev/null 2>&1 || true

cmd="${1:-full}"
shift || true

case "$cmd" in
    full)
        if [[ "${1:-}" == "all" ]]; then
            exec act push -W .github/workflows/tests.yml -j full "$@"
        fi
        # Default: pin to one Python version for fast iteration.
        py="${1:-3.12}"
        exec act push -W .github/workflows/tests.yml -j full \
            --matrix "python:${py}"
        ;;
    unit)
        py="${1:-3.12}"
        exec act push -W .github/workflows/tests.yml -j unit-bare \
            --matrix "python:${py}"
        ;;
    lint)
        exec act push -W .github/workflows/tests.yml -j lint
        ;;
    scenarios)
        exec act push -W .github/workflows/tests.yml -j scenarios
        ;;
    prek)
        exec act push -W .github/workflows/prek.yml
        ;;
    -h|--help|help)
        # Print only the leading comment block of this script (the
        # usage docs above), stripping the leading ``# `` markers.
        sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed -E 's/^# ?//'
        ;;
    *)
        echo "error: unknown command '$cmd'" >&2
        echo "       run '$(basename "$0") --help' for usage" >&2
        exit 2
        ;;
esac
