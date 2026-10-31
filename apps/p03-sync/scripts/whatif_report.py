"""Merge test results with the design's what-if matrix into a gate report.

The case list is parsed out of the design document rather than transcribed
here. Transcribing 252 ids and priorities by hand would drift from the design
the moment either changed, and a coverage report that disagrees with its own
source of truth is worse than no report.

The important behaviour is what happens to a case *no test exercised*: it is
reported ``NOT_TESTED``, counted against the gate, and never silently dropped.
A release gate that only sees the cases somebody remembered to write is not a
gate (design 22.1, WF3-242).

Usage:
    python scripts/whatif_report.py [--results artifacts/what_if_results.json]
                                    [--design ../docs/Project_03_....md]
                                    [--out artifacts/whatif_coverage.json]
                                    [--require-p0]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import defaultdict

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]

_DESIGN_RELATIVE = (
    pathlib.Path("docs") / "system-design"
    / "Project_03_Trading_Data_Synchronization_System_Design.md"
)


def _locate_design() -> pathlib.Path:
    """Find the design document by walking up to the repository root.

    A fixed number of ``.parent`` hops assumes this component's depth in the
    tree and silently resolves to the wrong directory when that changes.
    Searching upwards is layout-independent; ``--design`` still overrides it.
    """
    for candidate in PROJECT_ROOT.parents:
        design = candidate / _DESIGN_RELATIVE
        if design.is_file():
            return design
    # Returned rather than raised: the caller may pass --design, and failing at
    # import time would make that impossible.
    return PROJECT_ROOT.parent / _DESIGN_RELATIVE


DEFAULT_DESIGN = _locate_design()
DEFAULT_RESULTS = PROJECT_ROOT / "artifacts" / "what_if_results.json"
DEFAULT_OUT = PROJECT_ROOT / "artifacts" / "whatif_coverage.json"

#: Matrix rows look like: | WF3-053 | P0 | <fault> | <expected> | <oracle> |
_ROW = re.compile(
    r"^\|\s*(?P<case>WF3-\d{3})\s*\|\s*(?P<priority>P[012])\s*\|(?P<rest>.*)$"
)
#: Group headings look like: ### E. Batch COMMIT, receipts & ... - 12 tinh huong
_GROUP = re.compile(r"^###\s+(?P<letter>[A-U])\.\s+(?P<title>.+?)\s+[-\u2014]")


def parse_design(path: pathlib.Path) -> dict[str, dict[str, str]]:
    cases: dict[str, dict[str, str]] = {}
    group_letter = ""
    group_title = ""
    for line in path.read_text(encoding="utf-8").splitlines():
        heading = _GROUP.match(line)
        if heading:
            group_letter = heading.group("letter")
            group_title = heading.group("title").strip()
            continue
        row = _ROW.match(line)
        if row:
            columns = [cell.strip() for cell in row.group("rest").split("|")]
            cases[row.group("case")] = {
                "priority": row.group("priority"),
                "group": group_letter,
                "group_title": group_title,
                "fault": columns[0] if columns else "",
            }
    return cases


def build_report(
    cases: dict[str, dict[str, str]], results: dict[str, object]
) -> dict[str, object]:
    executed = {entry["case_id"]: entry for entry in results.get("cases", [])}  # type: ignore[union-attr]
    rows = []
    by_priority: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for case_id in sorted(cases):
        meta = cases[case_id]
        found = executed.get(case_id)
        status = found["status"] if found else "NOT_TESTED"
        by_priority[meta["priority"]][status] += 1
        rows.append(
            {
                "case_id": case_id,
                "priority": meta["priority"],
                "group": meta["group"],
                "status": status,
                "tests": (found or {}).get("tests", []),
            }
        )

    # Cases a test claimed that the design does not define: a typo in a marker
    # would otherwise inflate coverage invisibly.
    unknown = sorted(set(executed) - set(cases))

    p0 = by_priority["P0"]
    p0_total = sum(p0.values())
    gate = {
        "p0_total": p0_total,
        "p0_pass": p0.get("PASS", 0),
        # Only PASS counts. NOT_CONCLUSIVE and BLOCKED_ENVIRONMENT are each a
        # kind of "we do not know" and must not be read as success (design 15.6).
        "p0_blocking": p0_total - p0.get("PASS", 0),
        "satisfied": p0_total > 0 and p0.get("PASS", 0) == p0_total,
    }
    return {
        "schema": "trading-data-sync/whatif-coverage/v1",
        "design_cases": len(cases),
        "by_priority": {
            priority: dict(statuses) for priority, statuses in sorted(by_priority.items())
        },
        "p0_gate": gate,
        "unknown_case_ids": unknown,
        "cases": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--design", type=pathlib.Path, default=DEFAULT_DESIGN)
    parser.add_argument("--results", type=pathlib.Path, default=DEFAULT_RESULTS)
    parser.add_argument("--out", type=pathlib.Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--require-p0",
        action="store_true",
        help="exit non-zero unless every P0 case is PASS",
    )
    args = parser.parse_args(argv)

    if not args.design.exists():
        print(f"design document not found: {args.design}", file=sys.stderr)
        return 2
    cases = parse_design(args.design)
    if not cases:
        print("no what-if rows parsed from the design document", file=sys.stderr)
        return 2
    results = (
        json.loads(args.results.read_text(encoding="utf-8"))
        if args.results.exists()
        else {"cases": []}
    )
    report = build_report(cases, results)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    gate = report["p0_gate"]
    print(f"design cases parsed : {report['design_cases']}")
    for priority, statuses in report["by_priority"].items():
        rendered = ", ".join(f"{k}={v}" for k, v in sorted(statuses.items()))
        print(f"  {priority}: {rendered}")
    print(f"P0 gate             : {gate['p0_pass']}/{gate['p0_total']} PASS")
    if report["unknown_case_ids"]:
        print(f"unknown case ids    : {report['unknown_case_ids']}")
    print(f"report written      : {args.out}")

    if args.require_p0 and not gate["satisfied"]:
        print(
            f"P0 gate NOT satisfied: {gate['p0_blocking']} case(s) are not PASS",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
