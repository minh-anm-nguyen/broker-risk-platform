#!/usr/bin/env python
"""Collect the evidence bundle for a run (SYSTEM_DESIGN section 10.6).

Writes ``artifacts/<run_id>/`` containing the manifest, scenario, fault rules,
invariant results, the canonical digest and a scorecard.

The one rule this script exists to enforce: **it never invents a PASS.** An
invariant that was skipped is reported as skipped, a harness-scope invariant is
reported as proven-elsewhere with the name of the test, and a benchmark with no
recorded environment is reported as ``INVALID_RESULT`` rather than published
(WF-115). The scorecard it emits is therefore safe to paste into a release
gate, because every cell says how it was obtained.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from simulator import GENERATOR_VERSION, SCHEMA_VERSION, __version__  # noqa: E402
from simulator.domain import invariants as inv  # noqa: E402
from simulator.domain.accounting import ACCOUNTING_POLICY_VERSION  # noqa: E402
from simulator.faults import rules as fault_rules  # noqa: E402
from simulator.persistence import event_store as es  # noqa: E402
from simulator.persistence.connection import open_connection  # noqa: E402
from simulator.settings import Settings  # noqa: E402

#: Benchmark artefacts the performance suite writes, if it has been run.
_BENCHMARKS = (
    "benchmark_bootstrap",
    "benchmark_read_mix",
    "benchmark_overload",
    "benchmark_restart",
)

#: Highest what-if case id in the section 7 matrix.
_WF_COUNT = 127

#: Cases this project does **not** test, with the reason. Stated explicitly so
#: the coverage map never has to guess, and so "we did not test this" is a
#: recorded decision rather than an absence someone later reads as a pass.
#:
#: Every entry here is P1 or P2. All 77 P0 cases are referenced by at least one
#: test, which is what release gate G1 requires.
_NOT_TESTED: dict[str, str] = {
    "WF-010": "P1. Needs a multi-interpreter CI matrix; the frozen golden digests are the mechanism, but only one runtime was exercised.",
    "WF-058": "P1. A client holding a connection open for minutes needs a real socket-level client; admission control is tested, the slow-client shape is not.",
    "WF-070": "P1. TCP reset and half-open connections need a network proxy. A middleware cannot produce them (section 6.4).",
    "WF-071": "P1. DNS failure and connection refused need an external harness.",
    "WF-094": "P1. 1M-deal stress. Profile exists (scripts/run_benchmark.sh --profile stress-1m); not run.",
    "WF-095": "P1. One-hour soak for FD and RSS growth; not run.",
    "WF-097": "P1. Hot-account skew (one account holding most deals); not run.",
    "WF-098": "P1. Very wide time-range filter with EXPLAIN QUERY PLAN review; not run.",
    "WF-100": "P2. Eight-hour mixed read/write/admin saturation; not run.",
    "WF-116": "P1. Behaviour when the metrics exporter itself fails; not exercised.",
    "WF-117": "P1. Host clock skew of seconds. Deadlines use the monotonic clock and timestamps are UTC, but no test steps the host clock.",
    "WF-125": "P1. Container memory cap behaviour needs a constrained container runtime.",
    "WF-126": "P1. v1/v2 coexistence; there is no v2 yet.",
    "WF-127": "P2. Slow or network filesystem; needs that filesystem.",
}

#: Fields a benchmark result must carry to be publishable (section 9.4).
_REQUIRED_MANIFEST_FIELDS = frozenset(
    {
        "git_sha",
        "python",
        "sqlite",
        "platform",
        "logical_cpus",
        "total_memory_bytes",
        "generator_version",
        "targets",
    }
)


def git_sha() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=str(ROOT),
        )
        return completed.stdout.strip() or "not-a-git-repository"
    except Exception:  # noqa: BLE001 - absence of git is itself a fact to record
        return "git-unavailable"


def build_manifest(run: es.RunSummary) -> dict[str, Any]:
    return {
        "collected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "git_sha": git_sha(),
        "simulator_version": __version__,
        "generator_version": GENERATOR_VERSION,
        "accounting_policy_version": ACCOUNTING_POLICY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "python": sys.version.split()[0],
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "run": {
            "run_id": run.run_id,
            "generation": run.generation,
            "seed": run.seed,
            "scenario_name": run.scenario_name,
            "spec_hash": run.spec_hash,
            "status": run.status.value,
            "committed_seq": run.committed_seq,
        },
    }


def load_benchmarks(artifacts: Path) -> dict[str, Any]:
    """Read benchmark artefacts, marking any without an environment invalid."""
    results: dict[str, Any] = {}
    for name in _BENCHMARKS:
        path = artifacts / f"{name}.json"
        if not path.exists():
            results[name] = {"status": "NOT_RUN"}
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        missing = sorted(_REQUIRED_MANIFEST_FIELDS - set(payload))
        if missing:
            results[name] = {
                "status": "INVALID_RESULT",
                "reason": f"manifest is missing {missing}",
            }
            continue
        results[name] = {
            "status": "RECORDED",
            "is_reference_environment": payload.get("is_reference_environment", False),
            "slo_enforced": payload.get("slo_enforced", False),
            "measured": payload.get("measured", {}),
            "verdicts": payload.get("verdicts", {}),
        }
    return results


def build_what_if_map() -> dict[str, Any]:
    """Map every what-if case to the tests that reference it (section 10.6).

    The mapping is *mechanical*: it scans the test suite for ``WF-nnn``
    references rather than asserting coverage from a hand-maintained list. That
    keeps it honest in the direction that matters - it cannot claim a case is
    covered by a test that does not mention it - at the cost of missing cases a
    test covers without naming. Those show up as ``NOT_MAPPED``, which is
    explicitly *not* a pass.

    The statuses mean:
      REFERENCED_BY_TEST  a test names this case; see the files listed.
      NOT_TESTED          a recorded decision, with a reason.
      NOT_MAPPED          nobody decided. Treat as untested.
    """
    import re

    references: dict[str, set[str]] = {}
    tests_root = ROOT / "tests"
    for path in sorted(tests_root.rglob("*.py")):
        relative = path.relative_to(ROOT).as_posix()
        for number in set(re.findall(r"WF-(\d{3})", path.read_text(encoding="utf-8"))):
            references.setdefault(f"WF-{number}", set()).add(relative)

    cases: dict[str, Any] = {}
    for index in range(1, _WF_COUNT + 1):
        case = f"WF-{index:03d}"
        if case in references:
            cases[case] = {
                "status": "REFERENCED_BY_TEST",
                "tests": sorted(references[case]),
            }
        elif case in _NOT_TESTED:
            cases[case] = {"status": "NOT_TESTED", "reason": _NOT_TESTED[case]}
        else:
            cases[case] = {
                "status": "NOT_MAPPED",
                "reason": "no test names this case and no decision was recorded; treat as untested",
            }

    counted = {
        status: sum(1 for entry in cases.values() if entry["status"] == status)
        for status in ("REFERENCED_BY_TEST", "NOT_TESTED", "NOT_MAPPED")
    }
    return {
        "total_cases": _WF_COUNT,
        "counts": counted,
        "method": (
            "mechanical scan of tests/ for WF-nnn references; a reference means a "
            "test names the case, not that the case is exhaustively covered"
        ),
        "caveat": (
            "A finite matrix does not enumerate every possible failure. Property-based, "
            "stateful, metamorphic and mutation testing extend this; the matrix is a "
            "structured inventory, not a proof of fault tolerance."
        ),
        "not_mapped": sorted(
            case for case, entry in cases.items() if entry["status"] == "NOT_MAPPED"
        ),
        "cases": cases,
    }


def build_scorecard(
    summary: dict[str, Any],
    results: list[inv.InvariantResult],
    benchmarks: dict[str, Any],
    what_if: dict[str, Any],
) -> dict[str, Any]:
    """The release-gate view. Every cell states how it was obtained."""
    by_id = {item.invariant_id: item for item in results}
    storage_total = sum(1 for item in results if item.scope == inv.STORAGE_SCOPE)
    measured = [
        name
        for name, payload in benchmarks.items()
        if payload.get("status") == "RECORDED"
    ]
    reference = [
        name
        for name in measured
        if benchmarks[name].get("is_reference_environment") is True
    ]
    return {
        "invariants_storage_scope": f"{summary['passed']} / {storage_total}",
        "invariants_failed": summary["failed_ids"] or "none",
        "invariants_skipped": summary["skipped_ids"] or "none",
        "invariants_proven_by_tests": {
            item.invariant_id: item.covered_by
            for item in results
            if item.scope == inv.HARNESS_SCOPE
        },
        "what_if_referenced_by_tests": (
            f"{what_if['counts']['REFERENCED_BY_TEST']} / {what_if['total_cases']}"
        ),
        "what_if_not_tested_by_decision": what_if["counts"]["NOT_TESTED"],
        "what_if_not_mapped": what_if["not_mapped"] or "none",
        "benchmarks_recorded": measured or "NOT_RUN",
        # Section 9.2: the provisional targets are for the 4 vCPU Linux
        # reference host. A measurement from anywhere else is data, not a
        # verdict on the SLO.
        "benchmarks_on_reference_environment": reference or "NONE",
        "slo_status": (
            "MEASURED_ON_REFERENCE"
            if reference
            else ("MEASURED_OFF_REFERENCE" if measured else "NOT_RUN")
        ),
        "connector_end_to_end_resilience": "NOT_TESTED (requires Project 02)",
        "power_loss_durability": "NOT_TESTED (kill -9 is not a power cut, WF-087)",
        "gate_g0_data_correctness": (
            "PASS" if summary["storage_scope_ok"] and not summary["failed"] else "FAIL"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", default=None, help="run id; defaults to the active run")
    parser.add_argument(
        "--output", default=str(ROOT / "artifacts"), help="artifacts directory"
    )
    arguments = parser.parse_args(argv)

    settings = Settings()
    artifacts = Path(arguments.output)
    connection = open_connection(settings, writable=False)
    try:
        run_id = arguments.run or es.load_active_run_id(connection)
        if run_id is None:
            print("no run to collect evidence for", file=sys.stderr)
            return 2
        run = es.load_run_summary(connection, run_id)
        if run is None:
            print(f"run {run_id!r} does not exist", file=sys.stderr)
            return 2

        results = inv.run_all(connection, run_id, scope=inv.FULL_SCOPE)
        summary = inv.summarize(results)
        digest, counts = inv.canonical_state_digest(connection, run_id)
        rules = fault_rules.load_rules(connection)
    finally:
        connection.close()

    target = artifacts / run_id
    target.mkdir(parents=True, exist_ok=True)
    benchmarks = load_benchmarks(artifacts)

    def write(name: str, payload: Any) -> None:
        (target / name).write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )

    write("manifest.json", build_manifest(run))
    write("scenario.json", run.spec_payload)
    write(
        "fault_rules.json",
        {
            "count": len(rules),
            "items": [rule.to_payload() for rule in rules],
            "note": (
                "Activation counts here are whatever was persisted. The "
                "authoritative audit trail for a chaos run is "
                "sim_fault_activations_total plus the structured log."
            ),
        },
    )
    write(
        "invariant_results.json",
        {"summary": summary, "results": [item.to_payload() for item in results]},
    )
    write("record_counts.json", counts)
    write("benchmarks.json", benchmarks)
    what_if = build_what_if_map()
    write("what_if_results.json", what_if)
    write("scorecard.json", build_scorecard(summary, results, benchmarks, what_if))
    (target / "canonical_state.sha256").write_text(digest + "\n", encoding="utf-8")

    print(f"evidence written to {target}")
    print(f"  canonical digest  {digest}")
    print(f"  record counts     {counts}")
    print(
        f"  invariants        passed={summary['passed']} failed={summary['failed']} "
        f"skipped={summary['skipped']} not_applicable={summary['not_applicable']}"
    )
    print(f"  fault rules       {len(rules)}")
    print(
        f"  what-if cases     referenced_by_test="
        f"{what_if['counts']['REFERENCED_BY_TEST']} "
        f"not_tested={what_if['counts']['NOT_TESTED']} "
        f"not_mapped={what_if['counts']['NOT_MAPPED']}"
    )
    for name, payload in benchmarks.items():
        print(f"  {name:<22} {payload.get('status')}")

    if summary["failed"]:
        print(f"INVARIANTS FAILED: {summary['failed_ids']}", file=sys.stderr)
        return 1
    if not summary["storage_scope_ok"]:
        print(
            f"INCOMPLETE: storage-scope invariants did not all run "
            f"(skipped={summary['skipped_ids']})",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
