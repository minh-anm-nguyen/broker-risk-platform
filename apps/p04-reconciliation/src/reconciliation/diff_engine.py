"""Classify set-diff rows into findings with evidence (sections 4.3, 5.6, 6.2).

The diff itself is a set operation; the judgement is here. Three rules shape
every line of this module:

* **Absence is an observation, not a deletion.** A key present in the target
  and missing from the source becomes ``ABSENT_AT_SOURCE_*``, never "closed"
  or "deleted". Section 5.3 is explicit: only a source that reports
  ``status=CLOSED`` closes a position, and F-05/F-06 exist to catch the code
  that forgets it.

* **Money conflicts are terminal, not correctable.** Two observations of the
  same immutable deal id with different canonical content is
  ``IMMUTABLE_DEAL_CONFLICT`` at ``CRITICAL``, and section 8.1 forbids any
  automatic action for it. There is no authoritative value to pick, so
  picking one would be inventing financial data.

* **Confidence follows the alignment gate, not the data.** If the
  preconditions in section 5.1 did not hold, no finding may claim ``PROVEN``,
  regardless of how obvious the difference looks. A hash difference across two
  different cuts is expected behaviour (section 5.3).

A count-only check is never sufficient and is never used: section 6.2 caveat 4
rules out ``COUNT(*)`` or a root hash alone as proof of coverage, so counts
here only drive *extra* safety alerts such as the mass-drop guard.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .canonical import content_hash
from .config import Settings
from .db.repository import RunRecord
from .dto import AlignmentDecision, DiffRow
from .errors import Confidence, FindingCode, ReasonCode, Severity

#: ``(entity, diff_type) -> (finding_code, severity, explanation_code)``.
#: This table *is* the repair action matrix in section 4.3, in executable
#: form; ``policy.py`` reads the same codes to decide what may be proposed.
CLASSIFICATION_MATRIX: Mapping[tuple[str, str], tuple[FindingCode, Severity, str]] = {
    # Deals: immutable history. Nothing here is ever auto-corrected.
    ("deals", "MISSING_TARGET"): (
        FindingCode.MISSING_TARGET_DEAL,
        Severity.ERROR,
        "SOURCE_DEAL_ABSENT_FROM_TARGET_AT_PROVEN_CUT",
    ),
    ("deals", "EXTRA_TARGET"): (
        FindingCode.EXTRA_TARGET_DEAL,
        Severity.CRITICAL,
        "TARGET_DEAL_NOT_IN_SOURCE_AT_PROVEN_CUT_NEVER_DELETE",
    ),
    ("deals", "FIELD_MISMATCH"): (
        FindingCode.IMMUTABLE_DEAL_CONFLICT,
        Severity.CRITICAL,
        "SAME_DEAL_ID_DIFFERENT_CANONICAL_CONTENT_QUARANTINE",
    ),
    # Accounts: mutable projection, repairable by a full P03 re-sync.
    ("accounts", "MISSING_TARGET"): (
        FindingCode.MISSING_TARGET_ACCOUNT,
        Severity.ERROR,
        "SOURCE_ACCOUNT_ABSENT_FROM_TARGET_PROJECTION",
    ),
    ("accounts", "EXTRA_TARGET"): (
        FindingCode.ABSENT_AT_SOURCE_ACCOUNT,
        Severity.WARN,
        "ACCOUNT_ABSENT_FROM_SOURCE_LISTING_OBSERVATION_ONLY",
    ),
    ("accounts", "FIELD_MISMATCH"): (
        FindingCode.ACCOUNT_FIELD_MISMATCH,
        Severity.ERROR,
        "ACCOUNT_FIELDS_DIFFER_AT_EQUAL_CUT",
    ),
    # Positions: mutable projection. Absence is never a close.
    ("positions", "MISSING_TARGET"): (
        FindingCode.MISSING_TARGET_POSITION,
        Severity.ERROR,
        "SOURCE_POSITION_ABSENT_FROM_TARGET_PROJECTION",
    ),
    ("positions", "EXTRA_TARGET"): (
        FindingCode.ABSENT_AT_SOURCE_POSITION,
        Severity.WARN,
        "POSITION_ABSENT_FROM_SOURCE_LISTING_NOT_CLOSED",
    ),
    ("positions", "FIELD_MISMATCH"): (
        FindingCode.POSITION_FIELD_MISMATCH,
        Severity.ERROR,
        "POSITION_FIELDS_DIFFER_AT_EQUAL_CUT",
    ),
}


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """Findings plus the summary the run status is derived from."""

    findings: Sequence[Mapping[str, Any]]
    counts: Mapping[str, int] = field(default_factory=dict)
    mass_drift: bool = False
    large_count_drop: bool = False
    highest_severity: Severity | None = None

    @property
    def has_proven_findings(self) -> bool:
        return any(f["confidence"] == Confidence.PROVEN.value for f in self.findings)


class DiffClassifier:
    """Turns diff rows into evidence-bearing findings."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def classify(
        self,
        *,
        run: RunRecord,
        rows: Sequence[DiffRow],
        decision: AlignmentDecision,
    ) -> ClassificationResult:
        """Classify every diff row under the alignment decision.

        When strict comparison is not allowed, the per-item differences are
        *not* emitted as item findings at all. One run-level finding carrying
        the blocking reason is emitted instead, so nothing downstream can
        mistake "we could not compare" for a list of defects (section 5.1).
        """
        if not decision.strict_compare_allowed:
            return self._blocked_result(run=run, rows=rows, decision=decision)

        findings: list[dict[str, Any]] = []
        counts: dict[str, int] = {}

        for row in rows:
            if row.diff_type == "MATCHED":
                continue
            key = (run.entity, row.diff_type)
            mapped = CLASSIFICATION_MATRIX.get(key)
            if mapped is None:
                # An unmapped combination is a programming error, not a data
                # problem; recording it as INCONCLUSIVE keeps the run honest.
                findings.append(
                    self._finding(
                        run=run,
                        item_key=row.item_key,
                        code=FindingCode.INCONCLUSIVE_SOURCE,
                        severity=Severity.ERROR,
                        confidence=Confidence.INCONCLUSIVE,
                        explanation="UNMAPPED_DIFF_TYPE",
                        source_hash=row.source_hash,
                        target_hash=row.target_hash,
                        source_seq=row.source_seq,
                        decision=decision,
                    )
                )
                counts["UNMAPPED"] = counts.get("UNMAPPED", 0) + 1
                continue

            code, severity, explanation = mapped
            confidence = self._confidence_for(code)
            findings.append(
                self._finding(
                    run=run,
                    item_key=row.item_key,
                    code=code,
                    severity=severity,
                    confidence=confidence,
                    explanation=explanation,
                    source_hash=row.source_hash,
                    target_hash=row.target_hash,
                    source_seq=row.source_seq,
                    decision=decision,
                )
            )
            counts[code.value] = counts.get(code.value, 0) + 1

        mass_drift = self._is_mass_drift(run, counts)
        large_drop = self._is_large_count_drop(run)

        if mass_drift:
            findings.append(
                self._finding(
                    run=run,
                    item_key=None,
                    code=FindingCode.LARGE_SOURCE_COUNT_DROP
                    if large_drop
                    else FindingCode.PROVISIONAL_CROSS_ENTITY,
                    severity=Severity.CRITICAL,
                    confidence=Confidence.PROVEN,
                    explanation="MASS_DRIFT_EXCEEDS_BLAST_RADIUS_CAP",
                    source_hash=run.source_root_hash,
                    target_hash=run.target_root_hash,
                    source_seq=None,
                    decision=decision,
                )
            )
            counts["MASS_DRIFT"] = 1

        if large_drop:
            findings.append(
                self._finding(
                    run=run,
                    item_key=None,
                    code=FindingCode.LARGE_SOURCE_COUNT_DROP,
                    severity=Severity.CRITICAL,
                    confidence=Confidence.PROVEN,
                    explanation="SOURCE_COUNT_DROPPED_SHARPLY_MANUAL_REVIEW",
                    source_hash=run.source_root_hash,
                    target_hash=run.target_root_hash,
                    source_seq=None,
                    decision=decision,
                )
            )
            counts[FindingCode.LARGE_SOURCE_COUNT_DROP.value] = (
                counts.get(FindingCode.LARGE_SOURCE_COUNT_DROP.value, 0) + 1
            )

        # Deduplicate the (code, item_key) pairs the two guards above may
        # both have produced, matching the UNIQUE constraint on the table.
        unique: dict[tuple[str, str | None], dict[str, Any]] = {}
        for finding in findings:
            unique[(finding["finding_code"], finding["item_key"])] = finding
        deduped = list(unique.values())

        return ClassificationResult(
            findings=deduped,
            counts=counts,
            mass_drift=mass_drift,
            large_count_drop=large_drop,
            highest_severity=self._highest(deduped),
        )

    # ---- internals -------------------------------------------------------

    def _confidence_for(self, code: FindingCode) -> Confidence:
        """Absence claims are only ever as strong as the listing behind them.

        The alignment gate has already proven the source scan was complete and
        unfiltered, which is what section 5.3 requires before ``ABSENT`` may
        be asserted at all. The claim still stays ``PROVISIONAL``: it is a
        proven *observation* of absence, not proof that the entity ceased to
        exist, and section 4.3 allows only investigation for it.
        """
        if code in (FindingCode.ABSENT_AT_SOURCE_ACCOUNT, FindingCode.ABSENT_AT_SOURCE_POSITION):
            return Confidence.PROVISIONAL
        return Confidence.PROVEN

    def _blocked_result(
        self, *, run: RunRecord, rows: Sequence[DiffRow], decision: AlignmentDecision
    ) -> ClassificationResult:
        reason = decision.reason_code or ReasonCode.POLICY_BLOCK
        code = {
            ReasonCode.DIFFERENT_CUT: FindingCode.DIFFERENT_CUT,
            ReasonCode.EPOCH_MISMATCH: FindingCode.EPOCH_MISMATCH,
            ReasonCode.SOURCE_IDENTITY_CHANGED: FindingCode.EPOCH_MISMATCH,
        }.get(reason, FindingCode.INCONCLUSIVE_SOURCE)

        finding = self._finding(
            run=run,
            item_key=None,
            code=code,
            severity=Severity.WARN,
            confidence=Confidence.INCONCLUSIVE,
            explanation=f"STRICT_COMPARE_BLOCKED_{reason.value}",
            source_hash=run.source_root_hash,
            target_hash=run.target_root_hash,
            source_seq=None,
            decision=decision,
            observed_differences=len([r for r in rows if r.diff_type != "MATCHED"]),
        )
        return ClassificationResult(
            findings=[finding],
            counts={code.value: 1},
            mass_drift=False,
            large_count_drop=False,
            highest_severity=Severity.WARN,
        )

    def _finding(
        self,
        *,
        run: RunRecord,
        item_key: str | None,
        code: FindingCode,
        severity: Severity,
        confidence: Confidence,
        explanation: str,
        source_hash: str | None,
        target_hash: str | None,
        source_seq: int | None,
        decision: AlignmentDecision,
        **extra: Any,
    ) -> dict[str, Any]:
        """Build one finding, including the digest that binds it to its run.

        The evidence hash covers the run's provenance, the policy in force and
        the alignment checks, so a stored finding cannot later be reinterpreted
        under a different policy without the digest changing (FR04-07,
        INV04-16).
        """
        evidence = {
            "recon_run_id": run.recon_run_id,
            "source_id": run.source_id,
            "source_epoch_id": run.source_epoch_id,
            "entity": run.entity,
            "scope": run.scope,
            "schema_version": run.schema_version,
            "hash_version": run.hash_version,
            "policy_hash": run.policy_hash,
            "source_snapshot_seq": run.source_snapshot_seq,
            "target_snapshot_seq": run.target_snapshot_seq,
            "target_deal_watermark": run.target_deal_watermark,
            "target_published_scan_id": run.target_published_scan_id,
            "source_root_hash": run.source_root_hash,
            "target_root_hash": run.target_root_hash,
            "item_key": item_key,
            "finding_code": code.value,
            "confidence": confidence.value,
            "source_hash": source_hash,
            "target_hash": target_hash,
            "alignment_checks": dict(sorted(decision.checks.items())),
            **extra,
        }
        return {
            "item_key": item_key,
            "finding_code": code.value,
            "severity": severity.value,
            "confidence": confidence.value,
            "source_hash": source_hash,
            "target_hash": target_hash,
            "source_seq": source_seq,
            "evidence_hash": content_hash(evidence),
            "explanation_code": explanation,
        }

    def _is_mass_drift(self, run: RunRecord, counts: Mapping[str, int]) -> bool:
        """Blast-radius cap on mutable projections (section 8.1, F-12).

        Deliberately not applied to deals: a backfill of many missing
        historical deals is a legitimate shape of repair, whereas a fifth of
        all account balances changing at once needs a human first.
        """
        if run.entity == "deals":
            return False
        total = run.source_count or 0
        if total < self._settings.mass_drift_min_entities:
            return False
        affected = sum(
            value
            for key, value in counts.items()
            if key
            in (
                FindingCode.ACCOUNT_FIELD_MISMATCH.value,
                FindingCode.POSITION_FIELD_MISMATCH.value,
                FindingCode.MISSING_TARGET_ACCOUNT.value,
                FindingCode.MISSING_TARGET_POSITION.value,
                FindingCode.ABSENT_AT_SOURCE_ACCOUNT.value,
                FindingCode.ABSENT_AT_SOURCE_POSITION.value,
            )
        )
        return affected > total * self._settings.mass_drift_fraction

    def _is_large_count_drop(self, run: RunRecord) -> bool:
        """Guard against an empty or truncated source listing (F-16).

        A source that suddenly reports far fewer rows than the target holds is
        the signature of a truncated listing, and the FMEA entry for "zero
        record count from source" rates it critical. Never a deletion.
        """
        source = run.source_count
        target = run.target_count
        if source is None or target is None or target == 0:
            return False
        if target < self._settings.mass_drift_min_entities:
            return False
        return source <= target * (1.0 - self._settings.large_count_drop_fraction)

    def _highest(self, findings: Sequence[Mapping[str, Any]]) -> Severity | None:
        order = [Severity.INFO, Severity.WARN, Severity.ERROR, Severity.CRITICAL]
        worst: Severity | None = None
        for finding in findings:
            current = Severity(finding["severity"])
            if worst is None or order.index(current) > order.index(worst):
                worst = current
        return worst
