"""Run the suite and emit one evidence record per what-if case.

The design's rule for this file is strict and worth restating, because it is
what makes the output worth reading at all (section 14.15):

* A case nothing exercises is ``NOT_TESTED``. It is **not** a pass, and the
  absence of a test is reported rather than inferred from a green suite.
* A case whose test could not reach a conclusion - skipped, or xfailed
  because the host cannot measure what it needs - is ``NOT_CONCLUSIVE``.
* A case that needs a capability this build or this environment does not have
  (an authorised Manager SDK, a network-layer proxy, an 8-hour soak) is
  ``NOT_SUPPORTED``, with the reason stated. Silence here would read as a
  pass by omission.
* Nothing is ever filled in by hand.

The catalogue comes from the design document itself, so a case added there and
not tested here shows up as ``NOT_TESTED`` on the next run rather than
disappearing.

Usage::

    python scripts/run_whatif_suite.py [--design PATH] [--out DIR] [-k EXPR]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

_DESIGN_RELATIVE = (
    Path("docs") / "system-design"
    / "Project_02_MT5_Integration_Connector_System_Design.md"
)


def _locate_design() -> Path:
    """Find the design document by walking up to the repository root.

    A fixed number of ``.parent`` hops assumes this component's depth in the
    tree and silently resolves to the wrong directory when that changes.
    Searching upwards is layout-independent; ``--design`` still overrides it.
    """
    for candidate in ROOT.parents:
        design = candidate / _DESIGN_RELATIVE
        if design.is_file():
            return design
    # Returned rather than raised: the caller may pass --design, and failing at
    # import time would make that impossible.
    return ROOT.parent / _DESIGN_RELATIVE


DEFAULT_DESIGN = _locate_design()

#: Every case id mentioned anywhere, in the design's own format.
CASE_PATTERN = re.compile(r"WF2-[A-R]-\d{2}")

#: A row of the design's what-if tables: `WF2-A-01` | P0 | ...
ROW_PATTERN = re.compile(
    r"^\|\s*`(WF2-[A-R]-\d{2})`\s*\|\s*(P[012])\s*\|\s*(.+?)\s*\|", re.MULTILINE
)

ALLOWED_STATUSES = ("PASS", "FAIL", "NOT_TESTED", "NOT_SUPPORTED", "NOT_CONCLUSIVE")

#: Cases this build or this environment cannot exercise, each with the reason.
#:
#: Written out rather than left to fall through to ``NOT_TESTED`` because the
#: two mean different things to a reviewer: "nobody wrote a test" is a gap in
#: the suite, while "this needs a Windows host with an authorised Manager SDK"
#: is a gap in the environment, and only the first one is fixable here.
NOT_SUPPORTED: dict[str, str] = {
    # Native Manager SDK. No authorised SDK, endpoint, credential or
    # documented field mapping exists; the driver refuses to pretend
    # (drivers/manager_sdk_windows.py, design section 2).
    "WF2-E-02": "needs an authorised native Manager SDK session",
    "WF2-E-03": "needs an authorised native Manager SDK session",
    "WF2-E-05": "needs native SDK callbacks after logout",
    "WF2-E-07": "needs the real SDK's numeric representation",
    "WF2-E-08": "needs a real native wheel to mismatch an ABI against",
    "WF2-E-09": "needs native object ownership semantics",
    "WF2-E-10": "needs the real SDK's string encoding",
    "WF2-E-11": "needs a real native session to leak",
    "WF2-E-12": "needs a vendor enum or unit change to observe",
    "WF2-O-09": "needs a Windows host missing the SDK DLL dependencies",
    "WF2-O-12": "needs two gateways against one authorised native session",
    "WF2-R-07": "needs a real SDK that lacks a stable snapshot",
    "WF2-R-08": "needs a real source with non-deterministic ordering",
    "WF2-R-09": "needs native SDK callbacks during a worker death",
    "WF2-R-12": "needs an undocumented vendor behaviour change",
    # Transport-layer faults. An HTTP 503 cannot stand in for a TCP reset or a
    # half-open socket; these need a network proxy or iptables, which the
    # design puts at fault layer L2 (section 12.1) and which this lab does not
    # run.
    "WF2-C-01": "needs DNS-level fault injection (fault layer L2)",
    "WF2-C-02": "needs TCP-level fault injection (fault layer L2)",
    "WF2-C-03": "needs a SYN blackhole (fault layer L2)",
    "WF2-C-04": "needs a TCP reset mid-body (fault layer L2)",
    "WF2-C-05": "needs a half-open TCP socket (fault layer L2)",
    "WF2-C-06": "needs an expired TLS certificate (fault layer L6)",
    "WF2-C-07": "needs a mismatched TLS hostname (fault layer L6)",
    "WF2-C-08": "needs repeated network flaps (fault layer L2)",
    "WF2-C-09": "needs byte-level response pacing (fault layer L2)",
    "WF2-C-10": "needs bandwidth shaping (fault layer L2)",
    "WF2-C-12": "needs MTU and packet reordering control (fault layer L2)",
    "WF2-O-10": "needs certificate rotation during a live session",
    # Host and operating-system faults, and long soaks.
    "WF2-M-10": "needs a full artifact volume (fault layer L4)",
    "WF2-M-11": "needs CPU throttling to 0.5 core (fault layer L4)",
    "WF2-M-12": "needs an 8-hour soak run",
    "WF2-O-04": "needs a misconfigured container network",
    "WF2-O-11": "needs a container OOM kill",
    "WF2-N-12": "needs host-level memory-dump analysis",
    # Secret-store and clock infrastructure this lab does not run.
    "WF2-B-08": "needs an unavailable secret store",
    "WF2-B-10": "needs host clock skew against an auth service",
    "WF2-B-11": "needs two overlapping token versions from a rotation system",
    # Cross-entity referential integrity. A deal naming an account that does
    # not exist is only visible from a joined view of both entities, and this
    # connector reads each entity as its own snapshot (capability
    # ``stable_snapshot_across_entities`` is false). Detecting it belongs to
    # the reconciliation project; what Project 02 owes is the per-entity
    # provenance that makes the comparison possible, which it emits
    # (sections 1.3 and 7.1).
    "WF2-L-05": (
        "needs a joined view across entities; cross-entity referential integrity "
        "is Project 04's reconciliation, and this connector emits the per-entity "
        "provenance it needs rather than guessing"
    ),
    # Supply chain and host pressure: real infrastructure, not code.
    "WF2-N-10": "needs a vulnerability database and a CI dependency scanner",
    "WF2-R-10": "needs host disk pressure and a degraded logging path",
    # Two-host and source-side-operation cases.
    "WF2-F-12": "needs a Windows worker host rebooting independently of the gateway",
    "WF2-I-12": (
        "needs the source to rotate its cursor signing key, which the contract "
        "double does not expose"
    ),
    "WF2-J-12": "needs a backup-and-restore of an older source snapshot",
}


@dataclass
class CaseRecord:
    case_id: str
    priority: str
    description: str
    tests: list[str] = field(default_factory=list)
    status: str = "NOT_TESTED"
    reason: str | None = None

    def as_dict(self, *, run_id: str, git_sha: str, worker_mode: str) -> dict[str, Any]:
        return {
            "test_case_id": self.case_id,
            "priority": self.priority,
            "status": self.status,
            "test_run_id": run_id,
            "git_sha": git_sha,
            "worker_mode": worker_mode,
            "expected_oracle": self.description,
            "evidence_paths": sorted(self.tests),
            "reason_if_not_pass": self.reason,
        }


def load_catalogue(design: Path) -> dict[str, CaseRecord]:
    if not design.exists():
        raise SystemExit(f"design document not found: {design}")
    text = design.read_text(encoding="utf-8")
    catalogue: dict[str, CaseRecord] = {}
    for case_id, priority, description in ROW_PATTERN.findall(text):
        catalogue[case_id] = CaseRecord(
            case_id=case_id, priority=priority, description=description.strip()
        )
    if not catalogue:
        raise SystemExit(
            "no what-if rows parsed from the design document; the table format changed"
        )
    return catalogue


def map_tests_to_cases(tests_dir: Path) -> dict[str, set[str]]:
    """Which test functions cite which case ids.

    Resolution is by enclosing ``def test_...``, so a citation in a class
    docstring or a module docstring attaches to every test below it - which is
    how a file-level "Covers WF2-..." header is meant to read.
    """
    import ast

    precise: dict[str, set[str]] = defaultdict(set)
    headers: list[tuple[set[str], list[str]]] = []

    for path in sorted(tests_dir.rglob("test_*.py")):
        source = path.read_text(encoding="utf-8")
        if not CASE_PATTERN.search(source):
            continue
        try:
            rel = path.relative_to(ROOT).as_posix()
        except ValueError:
            # A tests directory outside the project root, which is how this
            # function is exercised in isolation. The absolute path is still a
            # usable node-id prefix.
            rel = path.as_posix()
        try:
            tree = ast.parse(source)
        except SyntaxError:  # pragma: no cover - a broken test file
            continue
        lines = source.splitlines()

        functions: list[tuple[int, int, str]] = []
        for node in ast.walk(tree):
            if isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)
            ) and node.name.startswith("test_"):
                functions.append((node.lineno, node.end_lineno or node.lineno, node.name))
        functions.sort()
        first_test_line = functions[0][0] if functions else len(lines) + 1

        # Precise citations: a case named inside a test function, or in a
        # docstring immediately above one.
        for start, end, name in functions:
            body = "\n".join(lines[start - 1 : end])
            for case_id in CASE_PATTERN.findall(body):
                precise[case_id].add(f"{rel}::{name}")
        for index, line in enumerate(lines, start=1):
            if index < first_test_line:
                continue
            if any(start <= index <= end for start, end, _ in functions):
                continue
            following = [name for start, _, name in functions if start > index]
            if not following:
                continue
            for case_id in CASE_PATTERN.findall(line):
                precise[case_id].add(f"{rel}::{following[0]}")

        header_cases: set[str] = set()
        for line in lines[: first_test_line - 1]:
            header_cases.update(CASE_PATTERN.findall(line))
        if header_cases and functions:
            headers.append((header_cases, [f"{rel}::{name}" for _, _, name in functions]))

    mapping: dict[str, set[str]] = {
        case_id: set(names) for case_id, names in precise.items()
    }
    # A module-level "Covers WF2-..." header is a coarse citation: it names
    # what a file is about, not which test proves what. It applies only to
    # cases that no individual test anywhere claims - otherwise one
    # inconclusive test would drag down a case that has its own passing test
    # in another file.
    for header_cases, names in headers:
        for case_id in header_cases:
            if case_id in precise:
                continue
            mapping.setdefault(case_id, set()).update(names)
    return mapping


class OutcomeCollector:
    """A pytest plugin that records the outcome of every test function."""

    def __init__(self) -> None:
        #: node id without parameters -> worst outcome seen
        self.outcomes: dict[str, str] = {}
        self.totals: dict[str, int] = defaultdict(int)

    @staticmethod
    def _base(nodeid: str) -> str:
        head = nodeid.split("[", 1)[0]
        path, _, rest = head.partition("::")
        name = rest.split("::")[-1] if rest else ""
        return f"{path}::{name}"

    def pytest_runtest_logreport(self, report: Any) -> None:  # noqa: D401
        if report.when != "call" and not (report.when == "setup" and report.skipped):
            return
        base = self._base(report.nodeid)
        if report.failed:
            outcome = "FAIL"
        elif report.skipped:
            # xfail and skip both mean "no conclusion was reached here".
            outcome = "NOT_CONCLUSIVE"
        else:
            outcome = "PASS"
        self.totals[outcome] += 1
        # Worst outcome wins across parameter sets.
        rank = {"PASS": 0, "NOT_CONCLUSIVE": 1, "FAIL": 2}
        current = self.outcomes.get(base)
        if current is None or rank[outcome] > rank[current]:
            self.outcomes[base] = outcome


def git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--design", type=Path, default=DEFAULT_DESIGN)
    parser.add_argument("--out", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--tests", type=Path, default=ROOT / "tests")
    parser.add_argument("-k", dest="keyword", default=None, help="pytest -k expression")
    parser.add_argument(
        "--include-slow",
        action="store_true",
        help="also run the chaos and performance lanes",
    )
    args = parser.parse_args(argv)

    import pytest

    catalogue = load_catalogue(args.design)
    mapping = map_tests_to_cases(args.tests)

    run_id = time.strftime("run-%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.out / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    pytest_args = [str(args.tests), "-q", "-p", "no:cacheprovider"]
    if not args.include_slow:
        pytest_args += ["-m", "not slow"]
    if args.keyword:
        pytest_args += ["-k", args.keyword]

    collector = OutcomeCollector()
    env_path = os.environ.get("PYTHONPATH", "")
    src = str(ROOT / "src")
    if src not in env_path.split(os.pathsep):
        sys.path.insert(0, src)
    exit_code = pytest.main(pytest_args, plugins=[collector])

    sha = git_sha()
    worker_mode = os.environ.get("CONNECTOR_WORKER_MODE", "thread")

    for case_id, record in catalogue.items():
        record.tests = sorted(mapping.get(case_id, set()))
        if case_id in NOT_SUPPORTED:
            record.status = "NOT_SUPPORTED"
            record.reason = NOT_SUPPORTED[case_id]
            continue
        if not record.tests:
            record.status = "NOT_TESTED"
            record.reason = "no test in this suite cites this case id"
            continue
        observed = [collector.outcomes.get(node) for node in record.tests]
        if any(item == "FAIL" for item in observed):
            record.status = "FAIL"
            record.reason = "at least one citing test failed"
        elif any(item is None for item in observed):
            # Cited by a test that this invocation did not run - a filtered or
            # deselected lane. Not a pass.
            record.status = "NOT_TESTED"
            missing = [
                node for node, item in zip(record.tests, observed) if item is None
            ]
            record.reason = f"citing test not executed in this run: {missing[0]}"
        elif any(item == "NOT_CONCLUSIVE" for item in observed):
            record.status = "NOT_CONCLUSIVE"
            # Named, not just counted: a reviewer has to be able to go and
            # look at the measurement that could not decide.
            inconclusive = [
                node for node, item in zip(record.tests, observed)
                if item == "NOT_CONCLUSIVE"
            ]
            record.reason = (
                "a citing test ran and could not conclude: " + ", ".join(inconclusive)
            )
        else:
            record.status = "PASS"

    records = [
        catalogue[case_id].as_dict(run_id=run_id, git_sha=sha, worker_mode=worker_mode)
        for case_id in sorted(catalogue)
    ]
    counts: dict[str, dict[str, int]] = {
        priority: defaultdict(int) for priority in ("P0", "P1", "P2")
    }
    for record in records:
        counts[record["priority"]][record["status"]] += 1

    results_path = out_dir / "what_if_results.json"
    results_path.write_text(
        json.dumps(
            {
                "test_run_id": run_id,
                "git_sha": sha,
                "worker_mode": worker_mode,
                "pytest_exit_code": int(exit_code),
                "allowed_statuses": list(ALLOWED_STATUSES),
                "totals_by_priority": {
                    priority: dict(sorted(values.items()))
                    for priority, values in counts.items()
                },
                "test_outcome_totals": dict(sorted(collector.totals.items())),
                "cases": records,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "test_run_id": run_id,
                "git_sha": sha,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "processor": platform.processor(),
                "cpu_count": os.cpu_count(),
                "worker_mode": worker_mode,
                "pytest_args": pytest_args,
                "source": (
                    "Project 01 contract double (tests/fakesim); the Project 01 "
                    "service itself was not run, so cross-service interoperability "
                    "is NOT_TESTED"
                ),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(f"what-if evidence: {results_path}")
    for priority in ("P0", "P1", "P2"):
        summary = ", ".join(
            f"{status}={count}" for status, count in sorted(counts[priority].items())
        )
        print(f"  {priority}: {summary}")
    # The suite's own exit code is what gates a release; this script reports.
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
