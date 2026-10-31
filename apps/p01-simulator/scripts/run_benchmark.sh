#!/usr/bin/env bash
# Run a benchmark profile and collect its evidence bundle.
#
# Section 9.4 is the contract this script implements: a number without its
# environment is not a result. Every profile records the git SHA, interpreter,
# SQLite version, host CPU/memory, dataset and seed into artifacts/, and the
# evidence collector marks any benchmark lacking that manifest as
# INVALID_RESULT rather than publishing it (WF-115).
#
# The provisional SLO targets in section 9.2 are for the 4 vCPU Linux reference
# host. Off that host this script *measures and records* rather than asserting,
# so a convenient laptop number can never be mistaken for a met SLO. Set
# SIM_BENCH_ENFORCE_SLO=1 on the reference host to turn the latency and
# throughput targets into assertions.
#
# Usage:
#   scripts/run_benchmark.sh [--profile smoke|baseline-100k|stress-1m] [--enforce-slo]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROFILE="smoke"
ENFORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --enforce-slo) ENFORCE=1; shift ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$PROFILE" in
  smoke)
    # Small enough for CI on every pre-release build.
    export SIM_BENCH_DEALS=2500 SIM_BENCH_ACCOUNTS=120
    ;;
  baseline-100k)
    # Dataset A from section 9.1.
    export SIM_BENCH_DEALS=100000 SIM_BENCH_ACCOUNTS=1000
    ;;
  stress-1m)
    # Dataset B from section 9.1. P1 only: run after the baseline is met, and
    # do not publish it as a baseline result.
    export SIM_BENCH_DEALS=1000000 SIM_BENCH_ACCOUNTS=2000
    ;;
  *)
    echo "unknown profile: $PROFILE (expected smoke, baseline-100k or stress-1m)" >&2
    exit 2
    ;;
esac

if [[ "$ENFORCE" == "1" ]]; then
  export SIM_BENCH_ENFORCE_SLO=1
fi

PYTHON="${PYTHON:-python}"
if [[ -x "$ROOT/../.venv/Scripts/python.exe" ]]; then
  PYTHON="$ROOT/../.venv/Scripts/python.exe"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON="$ROOT/.venv/bin/python"
fi

echo "=== benchmark profile: $PROFILE ==="
echo "    deals=$SIM_BENCH_DEALS accounts=$SIM_BENCH_ACCOUNTS enforce_slo=${SIM_BENCH_ENFORCE_SLO:-0}"
echo "    python: $("$PYTHON" --version)"
echo "    host:   $(uname -a 2>/dev/null || echo "$OS")"
echo

# Three repetitions, as section 9.4 point 7 requires: report the median and the
# spread, never the best run.
REPEAT="${SIM_BENCH_REPEAT:-3}"
for attempt in $(seq 1 "$REPEAT"); do
  echo "--- repetition $attempt of $REPEAT ---"
  "$PYTHON" -m pytest tests/performance -q -p no:randomly
  mkdir -p "artifacts/repetition-$attempt"
  cp artifacts/benchmark_*.json "artifacts/repetition-$attempt/" 2>/dev/null || true
done

echo
echo "=== evidence ==="
"$PYTHON" scripts/collect_evidence.py || true

echo
echo "Recorded artefacts:"
ls -1 artifacts/*.json artifacts/repetition-*/ 2>/dev/null || true
echo
echo "Reminder: these numbers are measurements, not a claim that the section 9.2"
echo "SLO targets were met, unless this ran on the reference host with"
echo "--enforce-slo. The scorecard in artifacts/<run_id>/scorecard.json records"
echo "which of the two it was."
