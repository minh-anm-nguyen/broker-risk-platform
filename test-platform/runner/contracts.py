#!/usr/bin/env python
"""Contract freeze gate for the platform (P09 ticket T09-01, section 1.2).

This checks the frozen contract in ``contracts/manifest.yaml`` against what the
repository actually contains, and fails the build when the two disagree. It is
deliberately read-only: P09's job is to *detect* drift, never to edit a
producer or a consumer so that a test goes green (P09 section 1.2).

What it verifies
----------------
1. Every component declared in the manifest has its directory on disk.
2. Every declared package name is present under that component's ``src``.
3. Every declared entrypoint names a real module and a callable.
4. Every schema referenced by an interface exists and is parseable JSON.
5. Declared ``env_prefix`` and ``http_port`` values appear in the component's
   own code or configuration, so the manifest cannot silently rot.

What it will NOT do
-------------------
Validate event or response *payloads* against the JSON Schemas. That needs the
``jsonschema`` package and live data, and belongs to the independent oracle
(T09-07/08). This gate only proves the schemas are present and well-formed.

Exit codes
----------
``0`` contract holds. ``1`` a declared fact is false. Open ``gaps`` are printed
as warnings and do not fail the default run: they are recorded, owned defects,
and failing on them would only tempt someone to delete the record. Pass
``--strict-gaps`` at release gate G1, where an unscoped gap is a blocker.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - a missing dependency is its own message
    sys.stderr.write("PyYAML is required: pip install pyyaml\n")
    raise SystemExit(2) from None

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "contracts" / "manifest.yaml"

_SKIP_PARTS = {"__pycache__", ".hypothesis", "artifacts", ".pytest_cache"}


@dataclass
class Report:
    """Collected findings. Failures block; warnings are recorded."""

    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks: int = 0

    def ok(self) -> None:
        self.checks += 1

    def fail(self, message: str) -> None:
        self.checks += 1
        self.failures.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)


def _declared_packages(spec: dict[str, Any]) -> list[str]:
    """Return the package names a component declares, singular or plural form."""
    if "package" in spec:
        return [spec["package"]]
    return list(spec.get("packages", ()))


def _declared_entrypoints(spec: dict[str, Any]) -> list[str]:
    if "entrypoint" in spec:
        return [spec["entrypoint"]]
    return list(spec.get("entrypoints", ()))


def _grep(root: Path, needle: str) -> bool:
    """Cheap textual search over a component's own sources and configuration."""
    patterns = ("*.py", "*.yml", "*.yaml", "*.toml", "*.example", "Dockerfile")
    for pattern in patterns:
        for candidate in root.rglob(pattern):
            if _SKIP_PARTS.intersection(candidate.parts):
                continue
            try:
                if needle in candidate.read_text(encoding="utf-8", errors="ignore"):
                    return True
            except OSError:
                continue
    return False


def check_components(manifest: dict[str, Any], report: Report) -> None:
    for name, spec in manifest.get("components", {}).items():
        path = REPO_ROOT / spec["path"]
        if not path.is_dir():
            report.fail(f"{name}: declared path {spec['path']} does not exist")
            continue
        report.ok()

        src = path / "src"
        for package in _declared_packages(spec):
            # Presence under src/ is the check. Importing for real would run the
            # component's code, which a contract gate has no business doing.
            if (src / package).is_dir() or (src / f"{package}.py").is_file():
                report.ok()
            else:
                report.fail(
                    f"{name}: declared package '{package}' not found under {spec['path']}/src"
                )

        for entrypoint in _declared_entrypoints(spec):
            module, _, attribute = entrypoint.partition(":")
            relative = Path(*module.split("."))
            if (src / relative).is_dir() or (src / f"{relative}.py").is_file():
                report.ok()
            else:
                report.fail(f"{name}: entrypoint '{entrypoint}' has no module {module}")
            if not attribute:
                report.fail(f"{name}: entrypoint '{entrypoint}' names no callable")

        has_dockerfile = (path / "Dockerfile").is_file()
        if spec.get("deployable") is True and not has_dockerfile:
            report.fail(f"{name}: declared deployable but has no Dockerfile")
        elif spec.get("deployable") is False and has_dockerfile:
            report.warn(
                f"{name}: has a Dockerfile but the manifest says deployable=false - "
                "update the manifest or the claim is stale"
            )
        else:
            report.ok()

        prefix = spec.get("env_prefix")
        if prefix and not _grep(path, prefix):
            report.fail(
                f"{name}: declared env_prefix '{prefix}' appears nowhere in {spec['path']}"
            )
        elif prefix:
            report.ok()

        port = spec.get("http_port")
        if port and not _grep(path, str(port)):
            report.warn(
                f"{name}: declared http_port {port} appears nowhere in {spec['path']} - "
                "the port may have moved"
            )
        elif port:
            report.ok()


def check_schemas(manifest: dict[str, Any], report: Report) -> None:
    contracts_dir = MANIFEST.parent
    referenced: set[str] = set()

    for interface, spec in manifest.get("interfaces", {}).items():
        schema_name = spec.get("schema")
        if not schema_name:
            continue
        referenced.add(schema_name)
        schema_path = contracts_dir / schema_name
        if not schema_path.is_file():
            report.fail(f"interface {interface}: references missing schema {schema_name}")
            continue
        try:
            document = json.loads(schema_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            report.fail(f"schema {schema_name}: is not valid JSON ({exc})")
            continue
        if "$schema" not in document or "title" not in document:
            report.fail(f"schema {schema_name}: missing $schema or title")
        else:
            report.ok()

    for orphan in sorted(contracts_dir.glob("*.schema.json")):
        if orphan.name not in referenced:
            report.warn(f"schema {orphan.name}: present but no interface references it")


def check_identity(manifest: dict[str, Any], report: Report) -> None:
    """Every identity field must name one owner, so two components cannot both
    believe they define it (P09 section 1.1)."""
    components = set(manifest.get("components", {}))
    allowed_owners = components | {"deployment", "product", "unassigned"}
    for field_name, spec in manifest.get("identity", {}).items():
        owner = spec.get("owner")
        if owner not in allowed_owners:
            report.fail(
                f"identity '{field_name}': owner '{owner}' is not a declared component"
            )
        else:
            report.ok()


#: A gap with one of these statuses is closed and is not reported.
_CLOSED_STATUSES = frozenset({"accepted-by-design", "resolved"})

#: A gap that has been decided but not closed. It still blocks, because a
#: decision is not a verification: `implemented-unverified` means the code
#: exists and has never run against the thing it has to work against, and
#: `scoped`/`deferred` mean a release carrying it needs the scope label that
#: P09 section 19.1 requires. Reported with the status visible, so nobody has
#: to open the manifest to learn that the gap was thought about.
_DECIDED_STATUSES = frozenset({"implemented-unverified", "scoped", "deferred"})


def report_gaps(manifest: dict[str, Any], report: Report) -> list[str]:
    open_gaps: list[str] = []
    for gap_id, gap in manifest.get("gaps", {}).items():
        status = gap.get("status")
        if status in _CLOSED_STATUSES:
            continue
        open_gaps.append(gap_id)
        severity = str(gap.get("severity", "unknown")).upper()
        owner = gap.get("owner", "unassigned")
        if status in _DECIDED_STATUSES:
            label = f"{severity}, {status}, owner={owner}"
        elif status:
            label = f"{severity}, {status}, owner={owner}"
        else:
            label = f"{severity}, UNDECIDED, owner={owner}"
        report.warn(f"GAP {gap_id} [{label}]: {gap.get('title')}")
    return open_gaps


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Platform contract freeze gate (T09-01).")
    parser.add_argument(
        "--strict-gaps",
        action="store_true",
        help="treat any open contract gap as a failure (use at release gate G1)",
    )
    args = parser.parse_args(argv)

    if not MANIFEST.is_file():
        sys.stderr.write(f"contract manifest not found: {MANIFEST}\n")
        return 2

    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    report = Report()

    check_components(manifest, report)
    check_schemas(manifest, report)
    check_identity(manifest, report)
    open_gaps = report_gaps(manifest, report)

    print(f"contract_version: {manifest.get('contract_version')}")
    print(f"reviewed:         {manifest.get('reviewed')}")
    print(f"checks run:       {report.checks}")

    for warning in report.warnings:
        print(f"WARN  {warning}")
    for failure in report.failures:
        print(f"FAIL  {failure}")

    if report.failures:
        print(f"\nCONTRACT BROKEN: {len(report.failures)} declared fact(s) are false.")
        return 1
    if args.strict_gaps and open_gaps:
        print(f"\nCONTRACT HOLDS but {len(open_gaps)} gap(s) are open: {', '.join(open_gaps)}")
        print("G1 cannot be claimed while an unscoped gap is open.")
        return 1

    print(f"\nCONTRACT HOLDS ({len(open_gaps)} open gap(s) recorded as warnings).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
