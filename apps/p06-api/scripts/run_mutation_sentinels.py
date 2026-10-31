#!/usr/bin/env python
"""Mutation sentinels: prove the suite would catch a removed safety check.

Design doc section 13.5. For each sentinel this script edits one line of source
to disable a protection, runs a narrow set of tests, and requires them to FAIL.
A sentinel the suite tolerates is a coverage gap, so a green run for a mutation
is reported as a failure of this script.

Every mutation is reverted in a ``finally`` block, including on interrupt. The
script refuses to start if the working tree is not clean, so an aborted run can
never be mistaken for intended source.

    python scripts/run_mutation_sentinels.py [--list] [--only NAME]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "risk_api"
ENGINE = ROOT.parents[1] / "packages" / "risk-engine" / "src" / "risk_engine"


@dataclass(frozen=True, slots=True)
class Sentinel:
    """One disabled protection, and the tests that must notice."""

    name: str
    description: str
    path: Path
    before: str
    after: str
    tests: tuple[str, ...]


SENTINELS: tuple[Sentinel, ...] = (
    Sentinel(
        name="drop-scope-intersection",
        description="stop denying accounts outside the authorized scope",
        path=SRC / "authz" / "scope_policy.py",
        before="    outside = [account for account in requested if account not in permitted]",
        after="    outside = []  # MUTATION: scope intersection removed",
        tests=(
            "tests/integration/test_authz_matrix_and_isolation.py",
            "tests/contracts/test_risk_endpoint.py",
        ),
    ),
    Sentinel(
        name="flip-allow-partial-default",
        description="make partial reports the default instead of opt-in",
        path=SRC / "settings.py",
        before="    strict_composite_default: bool = True",
        after="    strict_composite_default: bool = False  # MUTATION",
        tests=(
            "tests/integration/test_strict_partial_and_quality.py",
            "tests/unit/test_settings_and_versioning.py",
        ),
    ),
    Sentinel(
        name="skip-final-cut-check",
        description="release a result without re-checking that its cut still holds",
        path=SRC / "app" / "cut_capture.py",
        before="        changed = pinned.differs_from(current)",
        after="        changed = ()  # MUTATION: final validity check disabled",
        tests=("tests/integration/test_cut_races_and_audit.py",),
    ),
    Sentinel(
        name="skip-audit-requirement",
        description="return a financial report even when the audit sink is down",
        path=SRC / "settings.py",
        before="    audit_required: bool = True",
        after="    audit_required: bool = False  # MUTATION",
        tests=("tests/integration/test_cut_races_and_audit.py",),
    ),
    Sentinel(
        name="cast-money-to-float",
        description="serialize money through float instead of a decimal string",
        path=ENGINE / "decimals.py",
        before='    text = format(quantized, "f")',
        after='    text = repr(float(quantized))  # MUTATION',
        tests=(
            "tests/unit/test_money_and_decimals.py",
            "tests/contracts/test_risk_endpoint.py",
        ),
    ),
    Sentinel(
        name="drop-epoch-from-cursor-check",
        description="stop binding a pagination cursor to its source epoch",
        path=SRC / "app" / "pagination.py",
        before="        if payload.source_id != scope.source_id or payload.epoch_id != scope.epoch_id:",
        after="        if False:  # MUTATION: cursor epoch binding removed",
        tests=("tests/unit/test_cursor_codec.py",),
    ),
    Sentinel(
        name="ignore-scan-fence",
        description="stitch position pages across a republished scan",
        path=SRC / "app" / "pagination.py",
        before="        if payload.published_scan_id != published_scan_id:",
        after="        if False:  # MUTATION: scan fence removed",
        tests=(
            "tests/unit/test_cursor_codec.py",
            "tests/integration/test_cut_races_and_audit.py",
        ),
    ),
    Sentinel(
        name="accept-unverified-result",
        description="serialize an engine result without checking its contract",
        path=SRC / "app" / "report_quality.py",
        before="    if result.currency != expected.currency:",
        after="    if False:  # MUTATION: output currency check removed",
        tests=(
            "tests/unit/test_report_quality_guard.py",
            "tests/integration/test_strict_partial_and_quality.py",
        ),
    ),
    Sentinel(
        name="allow-unsigned-tokens",
        description="accept a token whose header says alg none",
        path=SRC / "authn" / "jwt_verifier.py",
        before='        if algorithm.lower() == "none":',
        after="        if False:  # MUTATION: alg none accepted",
        tests=("tests/unit/test_jwt_verifier.py",),
    ),
    Sentinel(
        name="ignore-unknown-query-keys",
        description="silently ignore a query parameter the endpoint does not declare",
        path=SRC / "contracts" / "requests.py",
        before='    unknown = sorted({key for key in query_keys if key not in allowed})',
        after="    unknown = []  # MUTATION: unknown query keys ignored",
        tests=(
            "tests/unit/test_request_contracts.py",
            "tests/contracts/test_routing_and_auth_http.py",
        ),
    ),
    Sentinel(
        name="unbounded-admission-queue",
        description="let the heavy report queue grow past its ceiling",
        path=SRC / "admission" / "scheduler.py",
        before="        if self.stats.queued_heavy >= self._max_heavy_queued and self._no_slot_free():",
        after="        if False:  # MUTATION: queue ceiling removed",
        tests=("tests/unit/test_admission_budgets_quotas.py",),
    ),
    Sentinel(
        name="allow-account-id-in-metric-label",
        description="permit a concrete account id as a metric label value",
        path=SRC / "observability" / "metrics.py",
        before="            if segment and _ACCOUNT_LIKE_RE.fullmatch(segment):",
        after="            if False:  # MUTATION: label policy relaxed",
        tests=("tests/unit/test_observability_policy.py",),
    ),
)


def working_tree_is_clean() -> bool:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", str(SRC), str(ENGINE)],
            capture_output=True,
            text=True,
            cwd=ROOT,
            check=False,
        )
    except FileNotFoundError:
        # No git available: the caller takes responsibility for the tree.
        return True
    if result.returncode != 0:
        return True
    return result.stdout.strip() == ""


def run_tests(tests: tuple[str, ...]) -> int:
    process = subprocess.run(
        [sys.executable, "-m", "pytest", *tests, "-q", "-x", "-p", "no:cacheprovider"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return process.returncode


def apply_sentinel(sentinel: Sentinel) -> str:
    original = sentinel.path.read_text(encoding="utf-8")
    occurrences = original.count(sentinel.before)
    if occurrences != 1:
        raise RuntimeError(
            f"{sentinel.name}: anchor matched {occurrences} times in "
            f"{sentinel.path.name}; the sentinel needs updating"
        )
    sentinel.path.write_text(original.replace(sentinel.before, sentinel.after), encoding="utf-8")
    return original


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="list the sentinels and exit")
    parser.add_argument("--only", help="run a single sentinel by name")
    args = parser.parse_args()

    if args.list:
        for sentinel in SENTINELS:
            sys.stdout.write(f"{sentinel.name:36} {sentinel.description}\n")
        return 0

    selected = [s for s in SENTINELS if args.only is None or s.name == args.only]
    if not selected:
        sys.stderr.write(f"no sentinel named {args.only!r}\n")
        return 2

    if not working_tree_is_clean():
        sys.stderr.write(
            "refusing to run: the source tree has uncommitted changes, and this "
            "script edits source in place. Commit or stash first.\n"
        )
        return 2

    survived: list[str] = []
    broken: list[str] = []

    for sentinel in selected:
        sys.stdout.write(f"\n--- {sentinel.name}: {sentinel.description}\n")
        try:
            original = apply_sentinel(sentinel)
        except RuntimeError as exc:
            sys.stdout.write(f"ERROR {exc}\n")
            broken.append(sentinel.name)
            continue
        try:
            code = run_tests(sentinel.tests)
        finally:
            sentinel.path.write_text(original, encoding="utf-8")

        if code == 0:
            sys.stdout.write(
                f"SURVIVED  the suite still passed with this protection removed\n"
                f"          tests run: {', '.join(sentinel.tests)}\n"
            )
            survived.append(sentinel.name)
        else:
            sys.stdout.write("killed    at least one test failed, as required\n")

    sys.stdout.write("\n" + "=" * 70 + "\n")
    sys.stdout.write(f"sentinels run: {len(selected)}\n")
    sys.stdout.write(f"killed:        {len(selected) - len(survived) - len(broken)}\n")
    sys.stdout.write(f"survived:      {len(survived)}\n")
    if broken:
        sys.stdout.write(f"anchors stale: {len(broken)} ({', '.join(broken)})\n")
    if survived:
        sys.stdout.write(
            "\nFAIL: these protections can be removed without failing a test, so the "
            f"suite gives false confidence for them: {', '.join(survived)}\n"
        )
        return 1
    if broken:
        sys.stdout.write("\nFAIL: update the stale sentinel anchors above\n")
        return 1
    sys.stdout.write("\nall sentinels killed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
