"""Evidence records: what a test must produce to count as executed.

Section 10.1 of the design document lists eleven fields a scenario has to emit
before its result means anything. The reason is the one failure the whole
project is organised against: a green test suite that proves nothing because
the fault never fired, the seed was not recorded, or the expected value was
computed by the code under test.

So a record carries, for every case:

* the environment and the dataset seed, so it can be re-run;
* ``injection_confirmed``, so a fault-injection case that failed to inject is
  ``INVALID_TEST`` rather than ``PASS`` (WF5-295, WF5-296);
* the oracle's *kind*, so a reviewer can see at a glance whether the expected
  value came from an independent source (WF5-289);
* the observed status and digest next to the expected ones.
"""

from __future__ import annotations

import json
import os
import platform
import sys
from dataclasses import dataclass, field, asdict
from datetime import UTC, datetime
from enum import StrEnum, unique
from pathlib import Path
from threading import Lock
from typing import Any, Iterable

from risk_tools.matrix import CASES, CaseStatus, Priority, case

__all__ = [
    "OracleKind",
    "EvidenceRecord",
    "EvidenceLog",
    "environment_id",
    "SPEC_VERSION",
]

#: Version of the design document this harness implements.
SPEC_VERSION = "1.0"


@unique
class OracleKind(StrEnum):
    """Which of the four independent oracle classes a case used.

    ``ENGINE_SELF`` is deliberately in the enum and deliberately disqualifying:
    naming it makes a circular oracle visible in the report instead of hiding
    inside a passing assertion.
    """

    GOLDEN_REFERENCE = "GOLDEN_REFERENCE"
    STATE_PROVENANCE = "STATE_PROVENANCE"
    PROPERTY_METAMORPHIC = "PROPERTY_METAMORPHIC"
    OPERATIONAL = "OPERATIONAL"
    ENGINE_SELF = "ENGINE_SELF"


def environment_id() -> str:
    """A reproducibility fingerprint for the host the suite ran on."""
    return (
        f"{platform.system().lower()}-py{sys.version_info.major}{sys.version_info.minor}"
        f"-sqlite-reference"
    )


@dataclass
class EvidenceRecord:
    """One executed case."""

    case_id: str
    spec_version: str = SPEC_VERSION
    test_status: CaseStatus = CaseStatus.NOT_EXECUTED
    injection_confirmed: bool = False
    expected_status: str | None = None
    observed_status: str | None = None
    expected_reason_codes: tuple[str, ...] = ()
    observed_reason_codes: tuple[str, ...] = ()
    expected_values: dict[str, str] = field(default_factory=dict)
    observed_values: dict[str, str] = field(default_factory=dict)
    oracle: OracleKind = OracleKind.GOLDEN_REFERENCE
    dataset_seed: int | None = None
    fixture_id: str | None = None
    input_digest: str | None = None
    output_digest: str | None = None
    elapsed_ms: int | None = None
    rss_peak_mib: float | None = None
    environment_id: str = field(default_factory=environment_id)
    recorded_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    notes: tuple[str, ...] = ()
    test_nodeid: str | None = None

    def __post_init__(self) -> None:
        case(self.case_id)  # rejects an id outside the matrix

    @property
    def priority(self) -> Priority:
        return case(self.case_id).priority

    def as_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["test_status"] = str(self.test_status)
        payload["oracle"] = str(self.oracle)
        payload["priority"] = str(self.priority)
        payload["group"] = case(self.case_id).group
        payload["domain"] = case(self.case_id).domain
        for key in (
            "expected_reason_codes",
            "observed_reason_codes",
            "notes",
        ):
            payload[key] = list(payload[key])
        return payload


class EvidenceLog:
    """Thread-safe collector that writes one JSON artifact per run."""

    def __init__(self, path: Path | None = None) -> None:
        self._records: dict[tuple[str, str | None], EvidenceRecord] = {}
        self._lock = Lock()
        self._path = path

    def record(self, record: EvidenceRecord) -> EvidenceRecord:
        """Store a record.

        Keyed by ``(case_id, test_nodeid)`` so that several tests may cover one
        case from different angles without overwriting each other, while a
        re-run of the same test replaces its own previous record.
        """
        with self._lock:
            self._records[(record.case_id, record.test_nodeid)] = record
        return record

    def extend(self, records: Iterable[EvidenceRecord]) -> None:
        for item in records:
            self.record(item)

    @property
    def records(self) -> tuple[EvidenceRecord, ...]:
        with self._lock:
            return tuple(
                sorted(self._records.values(), key=lambda r: (r.case_id, r.test_nodeid or ""))
            )

    def covered_case_ids(self) -> frozenset[str]:
        return frozenset(r.case_id for r in self.records)

    def summary(self) -> dict[str, Any]:
        """Coverage and outcome counts, measured against the full matrix."""
        by_status: dict[str, int] = {}
        by_priority_status: dict[str, dict[str, int]] = {}
        for record in self.records:
            key = str(record.test_status)
            by_status[key] = by_status.get(key, 0) + 1
            bucket = by_priority_status.setdefault(str(record.priority), {})
            bucket[key] = bucket.get(key, 0) + 1

        covered = self.covered_case_ids()
        executed_p0 = {c for c in covered if CASES[c].priority is Priority.P0}
        failed = [r.case_id for r in self.records if r.test_status is CaseStatus.FAIL]
        invalid = [r.case_id for r in self.records if r.test_status is CaseStatus.INVALID_TEST]

        total_p0 = sum(1 for c in CASES.values() if c.priority is Priority.P0)
        return {
            "spec_version": SPEC_VERSION,
            "environment_id": environment_id(),
            "generated_at": datetime.now(UTC).isoformat(),
            "matrix_total": len(CASES),
            "cases_covered": len(covered),
            "cases_not_executed": len(CASES) - len(covered),
            "p0_total": total_p0,
            "p0_covered": len(executed_p0),
            "records": len(self.records),
            "by_status": by_status,
            "by_priority": by_priority_status,
            "failed_cases": sorted(set(failed)),
            "invalid_cases": sorted(set(invalid)),
            "uncovered_case_ids": sorted(set(CASES) - covered),
            "coverage_by_group": self._coverage_by_group(covered),
        }

    def _coverage_by_group(self, covered: frozenset[str]) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for item in CASES.values():
            bucket = out.setdefault(item.group, {"total": 0, "covered": 0})
            bucket["total"] += 1
            if item.case_id in covered:
                bucket["covered"] += 1
        return out

    def write(self, path: Path | None = None) -> Path:
        target = path or self._path
        if target is None:
            raise ValueError("no artifact path configured for the evidence log")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "summary": self.summary(),
            "records": [r.as_payload() for r in self.records],
        }
        target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return target


def default_artifact_path() -> Path:
    """Where the suite writes its evidence, overridable for CI."""
    override = os.environ.get("RISK_EVIDENCE_PATH")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "artifacts" / "test_matrix_results.json"
