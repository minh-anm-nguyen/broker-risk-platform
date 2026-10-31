"""Evidence artifacts for test and campaign runs (section 10.7).

Section 10.7 specifies an artifact manifest, and section 10.6 adds a rule that
matters more than the format: **the runner must confirm the fault actually
fired.** A case whose fault never activated did not test what it claims, so
``fault_activation_confirmed`` is recorded and a case that expected a fault and
did not get one is marked ``INVALID_TEST`` rather than ``PASS``.

The result vocabulary is the design's, and it is used honestly:

* ``PASS`` -- the expected (possibly deliberately failed) outcome occurred and
  an independent oracle agrees.
* ``FAIL`` -- it did not.
* ``INCONCLUSIVE`` -- the run could not establish the outcome. **Never**
  counted as a pass (section 9 "test run result").
* ``NOT_RUN`` -- the case was not executed, for example because the fault
  cannot be produced in this environment.
* ``INVALID_TEST`` -- the case ran but did not test its own premise.

Artifacts are written outside the tables the service under test writes, so a
run cannot overwrite its own evidence (section 10.7).
"""

from __future__ import annotations

import json
import platform
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from .canonical import HASH_VERSION, SCHEMA_VERSION, content_hash


class CaseResult(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    NOT_RUN = "NOT_RUN"
    INVALID_TEST = "INVALID_TEST"


#: Only ``PASS`` counts towards coverage. Spelled out as a constant so a
#: reporting bug cannot quietly promote ``INCONCLUSIVE``.
PASSING_RESULTS: frozenset[CaseResult] = frozenset({CaseResult.PASS})


@dataclass
class EvidenceManifest:
    """One case's evidence record (section 10.7's JSON shape)."""

    test_id: str
    result: CaseResult
    domain: str = ""
    priority: str = "P0"
    description: str = ""
    policy_hash: str = ""
    source_id: str = "sim_local"
    source_epoch: str | None = None
    source_snapshot_seq: int | None = None
    target_watermark: int | None = None
    source_manifest_complete: bool | None = None
    target_manifest_complete: bool | None = None
    fault_injected: str | None = None
    fault_activation_confirmed: bool | None = None
    expected_outcome: str = ""
    actual_outcome: str = ""
    oracle_result: str = ""
    reason_code: str | None = None
    resource_metrics: Mapping[str, Any] = field(default_factory=dict)
    artifacts: Sequence[str] = field(default_factory=list)
    notes: Sequence[str] = field(default_factory=list)

    def validate(self) -> CaseResult:
        """Downgrade a result that its own evidence does not support.

        A case that named a fault but cannot confirm it activated becomes
        ``INVALID_TEST``: it may have passed for the wrong reason, and
        section 10.6 says that is not a pass.
        """
        if self.result is CaseResult.PASS and self.fault_injected:
            if self.fault_activation_confirmed is not True:
                self.notes = [
                    *self.notes,
                    "declared a fault but could not confirm activation",
                ]
                self.result = CaseResult.INVALID_TEST
        return self.result

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        body = {
            "test_id": self.test_id,
            "domain": self.domain,
            "priority": self.priority,
            "description": self.description,
            "result": self.result.value,
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "p04_schema_version": SCHEMA_VERSION,
            "p04_hash_version": HASH_VERSION,
            "p04_policy_hash": self.policy_hash,
            # Stated rather than implied: this build runs on SQLite in WAL
            # mode, so a reader never mistakes these results for PostgreSQL
            # measurements (section 6.2, 11.1).
            "db_engine": "sqlite-wal",
            "db_durability_mode": "synchronous=FULL;journal_mode=WAL",
            "source_id": self.source_id,
            "source_epoch": self.source_epoch,
            "source_snapshot_seq": self.source_snapshot_seq,
            "target_watermark": self.target_watermark,
            "source_manifest_complete": self.source_manifest_complete,
            "target_manifest_complete": self.target_manifest_complete,
            "fault_injected": self.fault_injected,
            "fault_activation_confirmed": self.fault_activation_confirmed,
            "expected_outcome": self.expected_outcome,
            "actual_outcome": self.actual_outcome,
            "oracle_result": self.oracle_result,
            "reason_code": self.reason_code,
            "resource_metrics": dict(self.resource_metrics),
            "artifacts": list(self.artifacts),
            "notes": list(self.notes),
        }
        body["evidence_hash"] = content_hash(body)
        return body

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{self.test_id}.json"
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path


@dataclass
class CampaignReport:
    """Aggregate over many cases, with honest arithmetic."""

    campaign_id: str
    manifests: list[EvidenceManifest] = field(default_factory=list)

    def add(self, manifest: EvidenceManifest) -> EvidenceManifest:
        manifest.validate()
        self.manifests.append(manifest)
        return manifest

    def tally(self) -> dict[str, int]:
        counts = {result.value: 0 for result in CaseResult}
        for manifest in self.manifests:
            counts[manifest.result.value] += 1
        return counts

    def by_domain(self) -> dict[str, dict[str, int]]:
        grouped: dict[str, dict[str, int]] = {}
        for manifest in self.manifests:
            bucket = grouped.setdefault(
                manifest.domain or "?", {result.value: 0 for result in CaseResult}
            )
            bucket[manifest.result.value] += 1
        return grouped

    @property
    def executed(self) -> int:
        return sum(
            1 for m in self.manifests if m.result not in (CaseResult.NOT_RUN,)
        )

    @property
    def passed(self) -> int:
        return sum(1 for m in self.manifests if m.result in PASSING_RESULTS)

    @property
    def failed(self) -> int:
        return sum(1 for m in self.manifests if m.result is CaseResult.FAIL)

    @property
    def green(self) -> bool:
        """True only if nothing failed and nothing was invalid.

        ``NOT_RUN`` does not fail a campaign -- it reduces coverage, which the
        report states separately. Hiding uncovered cases inside a pass rate is
        the dishonesty section 9 warns about.
        """
        return self.failed == 0 and not any(
            m.result is CaseResult.INVALID_TEST for m in self.manifests
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "totals": self.tally(),
            "by_domain": self.by_domain(),
            "executed": self.executed,
            "passed": self.passed,
            "failed": self.failed,
            "green": self.green,
            "cases": [m.to_dict() for m in self.manifests],
        }

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{self.campaign_id}.json"
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def render_table(self) -> str:
        lines = [f"campaign {self.campaign_id}", ""]
        lines.append(f"{'case':<28} {'domain':<8} {'pri':<4} {'result':<13} reason")
        lines.append("-" * 88)
        for manifest in self.manifests:
            lines.append(
                f"{manifest.test_id:<28} {manifest.domain:<8} {manifest.priority:<4} "
                f"{manifest.result.value:<13} {manifest.reason_code or ''}"
            )
        totals = self.tally()
        lines.append("")
        lines.append(
            "totals: "
            + "  ".join(f"{key}={value}" for key, value in totals.items() if value)
        )
        lines.append(f"green: {self.green}")
        return "\n".join(lines)
