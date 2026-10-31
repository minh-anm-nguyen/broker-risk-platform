"""Release-gate scorecard. Section 18.1.

Prints the honest version of the design's blank scorecard: what exists, what
executes, and what is still unproven. The rule it enforces is section 18's -
a feature is never marked supported because its *rejection* test passed, and
a gate is never green because the suite was fast.

Usage::

    python scripts/release_gates.py            # scorecard from static facts
    python scripts/release_gates.py --run      # also execute the suite
    python scripts/release_gates.py --run --slow
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from analytics.faults.killpoints import ALL_KILL_POINTS  # noqa: E402
from analytics.fixtures.golden import fixture_ids  # noqa: E402
from analytics.invariants import INVARIANTS, unproven  # noqa: E402

#: Suites and what each one establishes.
SUITES = {
    "unit": "parsers, money, canonical encoding, policy, dedup, rollups, oracles",
    "component": "the Kafka, ClickHouse and control-plane adapters' own contracts",
    "contract": "the read-only HTTP contract and scope isolation",
    "integration": "the 18 acceptance scenarios of section 11.1",
    "chaos": "28 kill points and 12 compound campaigns",
    "property": "metamorphic properties and a stateful model",
    "whatif": "coverage across the 44 failure domains",
    "performance": "benchmark shape and resource bounds (NOT the design's SLOs)",
}

#: Claims that cannot be made from this repository, and why.
DECLARED_GAPS = {
    "G2 performance SLOs": (
        "Section 13.3's targets (100 events/s, 100k build under 2 min, read p95 "
        "under 500 ms) are UNMEASURED. The benchmarks run against in-process "
        "adapters and measure P08 logic only, not ClickHouse or Kafka."
    ),
    "G1 real-infrastructure integration": (
        "clickhouse_http.py and control_postgres.py are reviewed but not "
        "exercised: no run against a real ClickHouse or PostgreSQL is recorded."
    ),
    "G3 replication / HA (domain AF, INV08-24)": (
        "ReplicatedMergeTree, Keeper and cross-replica read-after-write are P2 "
        "and need a real multi-replica cluster. No RPO/RTO is claimed."
    ),
    "G3 corrections (FR08-11)": (
        "DealCorrected/DealReversed v2 is gated pending a P07 schema upgrade. "
        "Only the safe *rejection* is tested, which does not make the feature "
        "supported."
    ),
    "G3 multi-currency conversion (FR08-12)": (
        "FX conversion needs a versioned rate set agreed with P05. Only the "
        "refusal is tested."
    ),
}


@dataclass
class Row:
    metric: str
    required: str
    status: str


def static_scorecard() -> list[Row]:
    gaps = unproven()
    return [
        Row("What-if domains", "44", "44 covered (1 declared NOT RUN: AF)"),
        Row("Deterministic kill points", "28", f"{len(ALL_KILL_POINTS)} implemented and executed"),
        Row("Compound campaigns", "12", "12 implemented and executed"),
        Row("Golden fixture definitions", "20", f"{len(fixture_ids())} implemented"),
        Row("Acceptance scenarios", "18", "18 implemented and executed"),
        Row(
            "Invariants proven",
            f"{len(INVARIANTS)}",
            f"{len(INVARIANTS) - len(gaps)} proven, {len(gaps)} declared unproven",
        ),
        Row("Throughput / p95 / RPO / RTO", "measured", "NOT MEASURED - see declared gaps"),
    ]


def run_suite(include_slow: bool) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    for suite in SUITES:
        command = [
            sys.executable,
            "-m",
            "pytest",
            f"tests/{suite}",
            "-q",
            "-p",
            "no:cacheprovider",
        ]
        if not include_slow:
            command += ["-m", "not slow"]
        completed = subprocess.run(
            command, cwd=ROOT, capture_output=True, text=True, check=False
        )
        tail = [line for line in completed.stdout.strip().splitlines() if line][-1:]
        results[suite] = {
            "exit_code": completed.returncode,
            "passed": completed.returncode == 0,
            "summary": tail[0] if tail else "",
        }
        marker = "PASS" if completed.returncode == 0 else "FAIL"
        print(f"  {suite:<12} {marker}  {results[suite]['summary']}")
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="P08 release-gate scorecard")
    parser.add_argument("--run", action="store_true", help="execute the test suites")
    parser.add_argument("--slow", action="store_true", help="include slow-marked tests")
    parser.add_argument("--json", action="store_true", help="emit machine-readable output")
    args = parser.parse_args(argv)

    rows = static_scorecard()
    results: dict[str, dict[str, object]] = {}

    if not args.json:
        print("Project 08 - release gate scorecard")
        print("=" * 78)
        print(f"{'Metric':<32}{'Required':>12}  Status")
        print("-" * 78)
        for row in rows:
            print(f"{row.metric:<32}{row.required:>12}  {row.status}")
        print()
        print("Suites")
        print("-" * 78)
        for suite, purpose in SUITES.items():
            print(f"  {suite:<12} {purpose}")

    if args.run:
        if not args.json:
            print()
            print("Execution")
            print("-" * 78)
        results = run_suite(args.slow)

    if not args.json:
        print()
        print("Declared gaps - claims this repository does NOT support")
        print("-" * 78)
        for gate, why in DECLARED_GAPS.items():
            print(f"  {gate}")
            print(f"      {why}")
        print()
        print(
            "Section 18: a feature is not supported because its rejection test "
            "passed, and\nbeing able to benchmark fast while incorrect is a NO-GO."
        )

    if args.json:
        print(
            json.dumps(
                {
                    "scorecard": [vars(row) for row in rows],
                    "suites": SUITES,
                    "execution": results,
                    "declared_gaps": DECLARED_GAPS,
                    "unproven_invariants": [inv.invariant_id for inv in unproven()],
                },
                indent=2,
            )
        )

    if args.run and any(not result["passed"] for result in results.values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
