#!/usr/bin/env bash
#
# Top-level benchmark runner — clickhouse-async / asynch / clickhouse-connect.
#
# Usage:
#   ./run.sh                           # full run, all 4 labels
#   ./run.sh --quick                   # smaller dataset (~1 min)
#   ./run.sh --library ca              # one short name only
#   ./run.sh --down                    # stop the container at the end
#
# Library short names: ca / asynch_pypi / asynch_tacto / cc.
# Each library lives behind its own pyproject extra so syncing one
# doesn't drag in the rest. asynch_pypi and asynch_tacto share the
# Python module name ``asynch``; this script handles the
# install-and-rerun dance when both are requested.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"

QUICK=0
DOWN=0
ALL_LIBS=("ca" "cc" "cc_async" "asynch_pypi" "asynch_tacto")
LIBS=("${ALL_LIBS[@]}")

# Libraries that share a Python package and therefore can't co-exist
# in one venv. Each conflict-group key maps to the package name to
# force-reinstall when flipping between members. The phase logic at
# the bottom of the file uses this to insert a re-sync between
# conflicting members.
conflict_group_for() {
    case "$1" in
        asynch_pypi|asynch_tacto) printf 'asynch' ;;
        cc|cc_async)              printf 'clickhouse-connect' ;;
        *)                         printf '' ;;
    esac
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)    QUICK=1; shift ;;
        --down)     DOWN=1;  shift ;;
        --library)  LIBS=("$2"); shift 2 ;;
        -h|--help)
            sed -n '3,15p' "$0"
            exit 0
            ;;
        *)
            echo "unknown flag: $1" >&2
            exit 2
            ;;
    esac
done

mkdir -p "$RESULTS"
RAW="$RESULTS/raw.jsonl"
ENV_JSON="$RESULTS/environment.json"
: > "$RAW"

if [[ $QUICK -eq 1 ]]; then
    PING_RUNS=50;     PING_WARMUP=10
    READ_RUNS=3;      READ_WARMUP=1; READ_ROWS=200000
    INSERT_RUNS=3;    INSERT_WARMUP=1; INSERT_ROWS=20000
    CONC_RUNS=3;      CONC_WARMUP=1; CONC_FANOUT=8
    MEM_ROWS=500000
else
    PING_RUNS=200;    PING_WARMUP=20
    READ_RUNS=10;     READ_WARMUP=3; READ_ROWS=1000000
    INSERT_RUNS=10;   INSERT_WARMUP=3; INSERT_ROWS=100000
    CONC_RUNS=10;     CONC_WARMUP=3; CONC_FANOUT=16
    MEM_ROWS=5000000
fi

# --- ensure the venv has the right extras synced -----------------------------
#
# Runs ``uv sync`` with whichever extras are needed for this invocation.
# Builds a unique extra-set so we sync once per asynch variant.
extras_for() {
    local lib="$1"
    case "$lib" in
        ca|asynch_pypi|asynch_tacto|cc|cc_async) printf '%s' "$lib" ;;
        *)
            echo "internal error: unknown library $lib" >&2
            exit 2
            ;;
    esac
}

sync_for_libs() {
    local extras=()
    local _arg
    for _arg in "$@"; do
        extras+=( "--extra" "$(extras_for "$_arg")" )
    done
    # Force-reinstall the conflict-group packages so a flip between
    # mutually-exclusive extras (e.g. asynch_pypi → asynch_tacto, or
    # cc → cc_async) actually swaps the on-disk package version. Both
    # flags are no-ops when the version hasn't changed.
    ( cd "$HERE" && uv sync --quiet \
        --reinstall-package asynch \
        --reinstall-package clickhouse-connect \
        "${extras[@]}" )
}

echo "==> ensuring ClickHouse is running"
"$ROOT/scripts/clickhouse.sh" up >/dev/null

DSN_NATIVE="clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse"
DSN_HTTP="http://clickhouse:clickhouse@localhost:8123/clickhouse"

# --- machine characteristics --------------------------------------------------

OS_NAME="$(uname -s)"
collect_env() {
    # Caller passes the libs whose versions to record this phase; the
    # rest of the per-label slots are left untouched (or seeded with
    # ``?`` on first run).
    local phase_libs_csv="$1"
    local cpu ram power docker_v ch_image ch_version python_v ca_v asynch_v cc_v git_sha git_dirty
    case "$OS_NAME" in
        Darwin)
            cpu="$(sysctl -n machdep.cpu.brand_string) — $(sysctl -n hw.physicalcpu)P/$(sysctl -n hw.logicalcpu)L cores"
            local mem_bytes; mem_bytes="$(sysctl -n hw.memsize)"
            ram="$(awk -v b="$mem_bytes" 'BEGIN{printf "%.1f GiB", b/1024/1024/1024}')"
            if command -v pmset >/dev/null 2>&1; then
                power="$(pmset -g batt | head -1 | sed 's/^[^(]*//;s/[ ]*$//')"
            else
                power="n/a"
            fi
            ;;
        Linux)
            cpu="$(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //') — $(nproc --all 2>/dev/null || nproc) cores"
            local mem_kb; mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
            ram="$(awk -v k="$mem_kb" 'BEGIN{printf "%.1f GiB", k/1024/1024}')"
            power="n/a"
            ;;
        *)
            cpu="unknown"; ram="unknown"; power="n/a"
            ;;
    esac

    docker_v="$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo 'unavailable')"
    if [[ -f "$ROOT/.clickhouse-version" ]]; then
        ch_image="$(tr -d '[:space:]' < "$ROOT/.clickhouse-version")"
    else
        ch_image="${CLICKHOUSE_VERSION:-24.8}"
    fi
    ch_version="$(docker exec clickhouse-async-dev clickhouse-client --query 'SELECT version()' 2>/dev/null || echo 'unavailable')"

    python_v="$( ( cd "$HERE" && uv run python -c 'import sys; print(sys.version.split()[0])' ) 2>/dev/null || echo '?')"
    ca_v="$( ( cd "$HERE" && uv pip show clickhouse-async 2>/dev/null | awk '/^Version/{print $2}' ) || echo '?')"
    asynch_v="$( ( cd "$HERE" && uv pip show asynch 2>/dev/null | awk '/^Version/{print $2}' ) || echo '?')"
    cc_v="$( ( cd "$HERE" && uv pip show clickhouse-connect 2>/dev/null | awk '/^Version/{print $2}' ) || echo '?')"

    git_sha="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo '?')"
    if [[ -n "$(git -C "$ROOT" status --porcelain 2>/dev/null)" ]]; then
        git_dirty=true
    else
        git_dirty=false
    fi

    # Static (machine-level) fields are merged-then-overwritten on
    # every phase. Per-label version slots are only updated for the
    # labels in this phase's ``phase_libs_csv``; other slots are
    # preserved from prior phases (or seeded with ``?`` on first run).
    # Without this scoping, the cc / cc_async slots would clobber each
    # other since they share the ``clickhouse-connect`` package name.
    OS_NAME_TXT="$(uname -srm)"
    DIRTY_PY="$( [[ "$git_dirty" == "true" ]] && echo "True" || echo "False" )"
    NOW_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    python3 - "$ENV_JSON" "$phase_libs_csv" <<PY
import json, os, sys
path, phase_csv = sys.argv[1], sys.argv[2]
phase_libs = set(phase_csv.split(",")) if phase_csv else set()
existing = {}
if os.path.exists(path):
    with open(path) as f:
        existing = json.load(f)

# Static fields — overwritten each phase. ``ram`` and ``power`` etc.
# don't change between phases on the same machine.
existing.update({
    "timestamp":         "$NOW_UTC",
    "os":                "$OS_NAME_TXT",
    "cpu":               "$cpu",
    "ram":               "$ram",
    "power":             "$power",
    "python":            "$python_v",
    "docker":            "$docker_v",
    "clickhouse_image":  "$ch_image",
    "clickhouse_server": "$ch_version",
    "git_sha":           "$git_sha",
    "git_dirty":         $DIRTY_PY,
})

# Per-label version slots. ``ca_version`` is always the editable
# clickhouse-async install. The asynch / cc slots are filled per phase
# from whichever extra was just synced.
existing.setdefault("ca_version", "$ca_v")
if "ca" in phase_libs:
    existing["ca_version"] = "$ca_v"
asynch_in_phase = "asynch_pypi" in phase_libs or "asynch_tacto" in phase_libs
if "asynch_pypi" in phase_libs and "$asynch_v" and "$asynch_v" != "?":
    existing["asynch_pypi_version"] = "$asynch_v"
if "asynch_tacto" in phase_libs and "$asynch_v" and "$asynch_v" != "?":
    existing["asynch_tacto_version"] = "$asynch_v"
if "cc" in phase_libs and "$cc_v" and "$cc_v" != "?":
    existing["cc_version"] = "$cc_v"
if "cc_async" in phase_libs and "$cc_v" and "$cc_v" != "?":
    existing["cc_async_version"] = "$cc_v"

# Seed any never-filled slot with ``?`` so the report renders the row.
for key in (
    "asynch_pypi_version",
    "asynch_tacto_version",
    "cc_version",
    "cc_async_version",
):
    existing.setdefault(key, "?")

with open(path, "w") as f:
    json.dump(existing, f, indent=2)
PY
}

# --- per-scenario runners -----------------------------------------------------

run_scenario_basic() {
    local scenario="$1" lib="$2"; shift 2
    echo "    -> $scenario / $lib" >&2
    ( cd "$HERE" && uv run python -m "scenarios.$scenario" \
        --library "$lib" \
        --dsn-native "$DSN_NATIVE" --dsn-http "$DSN_HTTP" \
        "$@" ) >> "$RAW"
}

run_all_scenarios_for_lib() {
    local lib="$1"
    echo "==> running scenarios for library: $lib"
    run_scenario_basic ping_latency      "$lib" --runs "$PING_RUNS"   --warmup "$PING_WARMUP"
    run_scenario_basic read_throughput   "$lib" --runs "$READ_RUNS"   --warmup "$READ_WARMUP"   --rows "$READ_ROWS"
    run_scenario_basic insert_throughput "$lib" --runs "$INSERT_RUNS" --warmup "$INSERT_WARMUP" --rows "$INSERT_ROWS"
    run_scenario_basic concurrent_reads  "$lib" --runs "$CONC_RUNS"   --warmup "$CONC_WARMUP"   --concurrency "$CONC_FANOUT"
    # memory_ceiling samples RSS in-process via psutil; no ``time -v``
    # wrapper needed any more.
    run_scenario_basic memory_ceiling    "$lib" --rows "$MEM_ROWS"
}

# --- main loop ---------------------------------------------------------------
#
# Some libraries can't co-exist (asynch_pypi vs asynch_tacto, cc vs
# cc_async — same package name, different sources/versions). We group
# LIBS into "phases" where each phase contains at most one member of
# each conflict group; flush_phase syncs and runs them together. A
# new phase begins when we hit a second member of any conflict group.
#
# macOS' default ``bash`` is 3.2 (no associative arrays), so the
# claim-set is two flat strings — one per conflict group. Add a new
# pair here when introducing a new conflict group; ``conflict_group_for``
# above returns the bucket name we look up below.

phase_libs=()
claimed_asynch=""
claimed_cc=""

flush_phase() {
    if (( ${#phase_libs[@]} == 0 )); then return; fi
    sync_for_libs "${phase_libs[@]}"
    # Re-capture environment after every sync. ``collect_env`` only
    # touches per-label version slots for libs that were just synced,
    # so cc and cc_async (which share the clickhouse-connect package)
    # both end up with their own line in environment.json.
    local _csv
    _csv="$(IFS=','; printf '%s' "${phase_libs[*]}")"
    echo "==> capturing environment → $ENV_JSON"
    collect_env "$_csv"
    # Distinct loop variable so we don't clobber the outer for-loop's
    # ``lib``. Bash for-loop variables share the enclosing scope unless
    # explicitly declared ``local``.
    local _bench_lib
    for _bench_lib in "${phase_libs[@]}"; do
        run_all_scenarios_for_lib "$_bench_lib"
    done
    phase_libs=()
    claimed_asynch=""
    claimed_cc=""
}

claim_or_flush() {
    local lib="$1" group="$2"
    case "$group" in
        asynch)
            if [[ -n "$claimed_asynch" && "$claimed_asynch" != "$lib" ]]; then
                flush_phase
            fi
            claimed_asynch="$lib"
            ;;
        clickhouse-connect)
            if [[ -n "$claimed_cc" && "$claimed_cc" != "$lib" ]]; then
                flush_phase
            fi
            claimed_cc="$lib"
            ;;
    esac
}

for lib in "${LIBS[@]}"; do
    group="$(conflict_group_for "$lib")"
    if [[ -n "$group" ]]; then
        claim_or_flush "$lib" "$group"
    fi
    phase_libs+=("$lib")
done
flush_phase

echo "==> generating report"
( cd "$HERE" && uv run python report.py "$RAW" --results-dir "$RESULTS" )

if [[ $DOWN -eq 1 ]]; then
    echo "==> stopping ClickHouse"
    "$ROOT/scripts/clickhouse.sh" down >/dev/null
fi

echo "==> done. open $RESULTS/report.md"
