"""Report the what-if matrix coverage honestly.

The tool exists to make one failure impossible: a table of 360 cases presented
as 360 results. It reads the evidence artifact the test suite writes and prints
coverage *against the full matrix*, so an untouched domain appears as a gap.

Rules it enforces:

* A case with no record is ``NOT_EXECUTED``. It is never counted as passing,
  and it is listed by id under ``--uncovered``.
* ``INVALID_TEST`` is reported separately from ``FAIL``. A fault-injection case
  whose fault never fired has not passed and has not failed -- it has not been
  a test (WF5-295).
* The P0 release gate of section 18.1 requires all 240 P0 cases executed and
  passing. ``--gate`` exits non-zero until that holds, and prints exactly which
  cases are missing.

Usage::

    python -m risk_tools.report
    python -m risk_tools.report --group F --verbose
    python -m risk_tools.report --uncovered
    python -m risk_tools.report --gate
    python -m risk_tools.report --json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

from risk_tools.evidence import default_artifact_path
from risk_tools.matrix import CASES, DOMAINS, TOTAL_CASES, CaseStatus, Priority

__all__ = ["load_artifact", "build_summary", "main"]

_PASSING = {str(CaseStatus.PASS), str(CaseStatus.SKIPPED_WITH_APPROVED_EXCEPTION)}


def load_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"no evidence artifact at {path}. Run the test suite first; "
            "a report without execution evidence would be fiction."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def build_summary(artifact: dict[str, Any]) -> dict[str, Any]:
    """Fold the records into per-case and per-group views.

    Where several tests cover one case, the worst outcome wins. A case is only
    as good as its weakest evidence.
    """
    severity = {
        str(CaseStatus.FAIL): 0,
        str(CaseStatus.INVALID_TEST): 1,
        str(CaseStatus.NOT_EXECUTED): 2,
        str(CaseStatus.SKIPPED_WITH_APPROVED_EXCEPTION): 3,
        str(CaseStatus.PASS): 4,
    }

    per_case: dict[str, dict[str, Any]] = {}
    for record in artifact.get("records", []):
        case_id = record["case_id"]
        status = record.get("test_status", str(CaseStatus.NOT_EXECUTED))
        existing = per_case.get(case_id)
        if existing is None or severity.get(status, 2) < severity.get(existing["status"], 2):
            per_case[case_id] = {
                "status": status,
                "oracle": record.get("oracle"),
                "injection_confirmed": record.get("injection_confirmed", False),
                "nodeid": record.get("test_nodeid"),
                "observed_status": record.get("observed_status"),
                "observed_reason_codes": record.get("observed_reason_codes", []),
            }

    groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "executed": 0, "passed": 0, "failed": 0, "invalid": 0}
    )
    by_priority: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "executed": 0, "passed": 0, "failed": 0, "invalid": 0}
    )
    uncovered: list[str] = []
    failed: list[str] = []
    invalid: list[str] = []

    for case_id, item in sorted(CASES.items()):
        record = per_case.get(case_id)
        group = groups[item.group]
        priority = by_priority[str(item.priority)]
        group["total"] += 1
        priority["total"] += 1

        if record is None:
            uncovered.append(case_id)
            continue

        group["executed"] += 1
        priority["executed"] += 1
        status = record["status"]
        if status in _PASSING:
            group["passed"] += 1
            priority["passed"] += 1
        elif status == str(CaseStatus.FAIL):
            group["failed"] += 1
            priority["failed"] += 1
            failed.append(case_id)
        elif status == str(CaseStatus.INVALID_TEST):
            group["invalid"] += 1
            priority["invalid"] += 1
            invalid.append(case_id)

    p0 = by_priority[str(Priority.P0)]
    gate_ok = p0["total"] == p0["passed"] and not failed and not invalid

    return {
        "environment_id": artifact.get("summary", {}).get("environment_id"),
        "generated_at": artifact.get("summary", {}).get("generated_at"),
        "matrix_total": TOTAL_CASES,
        "records": len(artifact.get("records", [])),
        "per_case": per_case,
        "groups": dict(groups),
        "by_priority": dict(by_priority),
        "uncovered": uncovered,
        "failed": failed,
        "invalid": invalid,
        "p0_gate_pass": gate_ok,
    }


def _bar(passed: int, total: int, width: int = 20) -> str:
    filled = 0 if total == 0 else int(round(width * passed / total))
    return "#" * filled + "." * (width - filled)


def _render(summary: dict[str, Any], *, group_filter: str | None, verbose: bool) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("Project 05 -- what-if matrix execution report")
    lines.append("=" * 78)
    lines.append(f"environment   {summary['environment_id']}")
    lines.append(f"generated     {summary['generated_at']}")
    lines.append(f"records       {summary['records']} test outcomes")
    lines.append("")
    lines.append(f"{'grp':<4} {'domain':<52} {'cov':>7}  {'pass':>5}")
    lines.append("-" * 78)

    titles = dict(DOMAINS)
    for group, _title in DOMAINS:
        if group_filter and group != group_filter:
            continue
        stats = summary["groups"].get(group, {"total": 0, "executed": 0, "passed": 0})
        coverage = f"{stats['executed']}/{stats['total']}"
        lines.append(
            f"{group:<4} {titles[group][:52]:<52} {coverage:>7}  {_bar(stats['passed'], stats['total'])}"
        )

    lines.append("-" * 78)
    for priority in ("P0", "P1", "P2"):
        stats = summary["by_priority"].get(
            priority, {"total": 0, "executed": 0, "passed": 0, "failed": 0, "invalid": 0}
        )
        lines.append(
            f"{priority}  total {stats['total']:>3}   executed {stats['executed']:>3}   "
            f"passed {stats['passed']:>3}   failed {stats['failed']:>3}   "
            f"invalid {stats['invalid']:>3}"
        )

    lines.append("")
    not_executed = len(summary["uncovered"])
    lines.append(
        f"NOT_EXECUTED  {not_executed} of {summary['matrix_total']} cases have no execution evidence"
    )
    if summary["failed"]:
        lines.append(f"FAIL          {', '.join(summary['failed'])}")
    if summary["invalid"]:
        lines.append(f"INVALID_TEST  {', '.join(summary['invalid'])}")

    gate = "PASS" if summary["p0_gate_pass"] else "NOT MET"
    lines.append("")
    lines.append(f"P0 release gate (section 18.1): {gate}")
    if not summary["p0_gate_pass"]:
        p0_missing = [
            case_id
            for case_id in summary["uncovered"]
            if CASES[case_id].priority is Priority.P0
        ]
        lines.append(
            f"  {len(p0_missing)} P0 cases are still NOT_EXECUTED; "
            "run with --uncovered to list them."
        )
        lines.append(
            "  An unexecuted case is not a passing case. This report will not"
        )
        lines.append("  claim the gate is met until every P0 case has evidence.")

    if verbose and group_filter:
        lines.append("")
        lines.append(f"Group {group_filter} detail")
        lines.append("-" * 78)
        for case_id, item in sorted(CASES.items()):
            if item.group != group_filter:
                continue
            record = summary["per_case"].get(case_id)
            if record is None:
                lines.append(f"  {case_id}  {item.priority!s:<3}  NOT_EXECUTED")
                continue
            injection = "injected" if record["injection_confirmed"] else "no-injection"
            lines.append(
                f"  {case_id}  {item.priority!s:<3}  {record['status']:<12} "
                f"{record['oracle']:<22} {injection}"
            )
            if record.get("nodeid"):
                lines.append(f"              {record['nodeid']}")

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m risk_tools.report",
        description="Report what-if matrix coverage from recorded test evidence.",
    )
    parser.add_argument("--path", type=Path, default=None, help="evidence artifact path")
    parser.add_argument("--group", help="restrict the report to one domain letter")
    parser.add_argument("--verbose", action="store_true", help="list each case in the group")
    parser.add_argument("--uncovered", action="store_true", help="list the NOT_EXECUTED case ids")
    parser.add_argument("--json", action="store_true", help="emit the summary as JSON")
    parser.add_argument(
        "--gate",
        action="store_true",
        help="exit non-zero unless the P0 release gate is met",
    )
    args = parser.parse_args(argv)

    path = args.path or default_artifact_path()
    try:
        artifact = load_artifact(path)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    summary = build_summary(artifact)

    if args.json:
        printable = {k: v for k, v in summary.items() if k != "per_case"}
        print(json.dumps(printable, indent=2, sort_keys=True))
    elif args.uncovered:
        print(f"{len(summary['uncovered'])} cases with no execution evidence:")
        for case_id in summary["uncovered"]:
            item = CASES[case_id]
            print(f"  {case_id}  {item.priority!s:<3}  {item.group:<3} {item.domain}")
    else:
        print(_render(summary, group_filter=args.group, verbose=args.verbose))

    if args.gate and not summary["p0_gate_pass"]:
        return 1
    if summary["failed"] or summary["invalid"]:
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
