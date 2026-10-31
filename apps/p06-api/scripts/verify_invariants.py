#!/usr/bin/env python
"""Static checks for invariants that a test cannot easily observe.

These are structural: they read the source tree rather than run the service.
Each one encodes a boundary from the design doc that is easy to breach by
accident during a refactor.

    python scripts/verify_invariants.py

Exits non-zero and prints every breach, so one run tells you everything.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "risk_api"
ENGINE = ROOT.parents[1] / "packages" / "risk-engine" / "src" / "risk_engine"


def _py_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def check_engine_has_no_web_imports() -> list[str]:
    """INV: the Project 05 library must not depend on a web framework."""
    banned = re.compile(r"^\s*(?:from|import)\s+(fastapi|starlette|uvicorn|httpx|pydantic)\b", re.M)
    breaches = []
    for path in _py_files(ENGINE):
        for match in banned.finditer(path.read_text(encoding="utf-8")):
            breaches.append(f"{path.relative_to(ENGINE.parents[2])}: engine imports {match.group(1)}")
    return breaches


def check_no_float_conversion_of_money() -> list[str]:
    """INV06-05: nothing may cast a money value to float."""
    banned = re.compile(r"\bfloat\(\s*(?:amount|total|gross|net|commission|swap|price|lots)\b")
    breaches = []
    for path in _py_files(SRC) + _py_files(ENGINE):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if banned.search(line):
                breaches.append(f"{path.name}:{number}: float() applied to a money value")
    return breaches


def check_routers_do_no_arithmetic() -> list[str]:
    """Design doc section 0.1: no financial formula in an endpoint handler."""
    suspicious = re.compile(r"Decimal\(|\*\s*contract_size|open_price\s*[-+]|lots\s*\*")
    breaches = []
    for path in _py_files(SRC / "api"):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if suspicious.search(line):
                breaches.append(f"{path.name}:{number}: arithmetic in the HTTP layer")
    return breaches


def check_no_sql_string_building() -> list[str]:
    """Design doc section 8.2: no SQL assembled from request values."""
    banned = re.compile(r"(?i)(select\s+.+\s+from|insert\s+into|update\s+\w+\s+set)\s*[\"']?\s*(?:\+|%|\.format|f\")")
    breaches = []
    for path in _py_files(SRC):
        text = path.read_text(encoding="utf-8")
        if banned.search(text):
            breaches.append(f"{path.name}: SQL appears to be built from a string")
    return breaches


def check_no_mutating_statements_against_upstream() -> list[str]:
    """INV06-20: Project 06 never writes to a Project 03/04 table."""
    banned = re.compile(r"(?i)\b(insert\s+into|update\s+|delete\s+from|truncate)\b")
    allowed_files = {"audit_store.py"}
    breaches = []
    for path in _py_files(SRC):
        if path.name in allowed_files:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith(("#", '"', "'")):
                continue
            if banned.search(line):
                breaches.append(f"{path.name}:{number}: mutating statement outside the audit store")
    return breaches


def check_problem_responses_are_the_only_error_path() -> list[str]:
    """Every refusal must be an ApiProblem so the taxonomy and headers apply."""
    banned = re.compile(r"raise\s+HTTPException")
    breaches = []
    for path in _py_files(SRC):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if banned.search(line):
                breaches.append(f"{path.name}:{number}: raises HTTPException instead of ApiProblem")
    return breaches


def check_no_auth_bypass_switch() -> list[str]:
    """There must be no configuration that skips authentication."""
    banned = re.compile(r"(?i)(skip_auth|disable_auth|auth_bypass|allow_anonymous|no_auth)")
    breaches = []
    for path in _py_files(SRC):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if banned.search(line):
                breaches.append(f"{path.name}:{number}: looks like an authentication bypass")
    return breaches


def check_faults_are_not_reachable_from_routers() -> list[str]:
    """Design doc section 11.1: injection is a harness concern, not a route."""
    breaches = []
    for path in _py_files(SRC / "api"):
        text = path.read_text(encoding="utf-8")
        if re.search(r"\.arm\(|\.arm_barrier\(|\.arm_once\(", text):
            breaches.append(f"{path.name}: a router arms a fault")
    return breaches


def check_no_print_statements_in_src() -> list[str]:
    """Logs go through the redactor; a print bypasses it."""
    breaches = []
    for path in _py_files(SRC):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.match(r"\s*print\(", line):
                breaches.append(f"{path.name}:{number}: print() bypasses the redacting logger")
    return breaches


CHECKS = {
    "engine has no web framework imports": check_engine_has_no_web_imports,
    "money is never cast to float": check_no_float_conversion_of_money,
    "routers do no financial arithmetic": check_routers_do_no_arithmetic,
    "no SQL built from strings": check_no_sql_string_building,
    "no mutating statements against upstream": check_no_mutating_statements_against_upstream,
    "errors always go through ApiProblem": check_problem_responses_are_the_only_error_path,
    "no authentication bypass switch": check_no_auth_bypass_switch,
    "routers cannot arm faults": check_faults_are_not_reachable_from_routers,
    "no print() in application code": check_no_print_statements_in_src,
}


def main() -> int:
    failures = 0
    for name, check in CHECKS.items():
        breaches = check()
        if breaches:
            failures += 1
            sys.stdout.write(f"FAIL  {name}\n")
            for breach in breaches:
                sys.stdout.write(f"        {breach}\n")
        else:
            sys.stdout.write(f"ok    {name}\n")
    if failures:
        sys.stdout.write(f"\n{failures} invariant check(s) failed\n")
        return 1
    sys.stdout.write(f"\nall {len(CHECKS)} invariant checks passed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
