"""Classification and policy decisions (sections 4.3, 8.1, 5.4).

These tests work on constructed ``RunRecord`` and ``DiffRow`` values so each
decision boundary is examined on its own, without a database or a simulator in
the way. The point is to pin the *table* in section 4.3 and the guardrails in
section 8.1 as executable behaviour.
"""

from __future__ import annotations

import pytest

from reconciliation.alignment import cross_entity_tier
from reconciliation.canonical import HASH_VERSION, SCHEMA_VERSION
from reconciliation.config import load_settings
from reconciliation.db.repository import RunRecord
from reconciliation.diff_engine import CLASSIFICATION_MATRIX, DiffClassifier
from reconciliation.dto import AlignmentDecision, DiffRow, SourceCapabilities
from reconciliation.errors import (
    ActionType,
    Confidence,
    FindingCode,
    ReasonCode,
    Severity,
)
from reconciliation.policy import ACTION_FOR_FINDING, BLOCKING_FINDINGS, RepairPolicy

pytestmark = [pytest.mark.unit]


def make_run(
    *,
    entity: str = "deals",
    source_count: int = 100,
    target_count: int = 100,
    source_complete: bool = True,
    target_complete: bool = True,
) -> RunRecord:
    return RunRecord(
        recon_run_id="run-1",
        source_id="sim_local",
        source_epoch_id="epoch-1",
        entity=entity,
        scope="UNFILTERED",
        scope_hash="s" * 64,
        policy_hash="p" * 64,
        schema_version=SCHEMA_VERSION,
        hash_version=HASH_VERSION,
        status="DIFFING",
        worker_id="worker-A",
        source_run_id="run_0001",
        source_generation=1,
        source_snapshot_seq=1000,
        target_snapshot_seq=1000,
        target_published_scan_id="scan-1",
        target_deal_watermark=1000,
        source_complete=source_complete,
        target_complete=target_complete,
        source_count=source_count,
        target_count=target_count,
        source_root_hash="a" * 64,
        target_root_hash="b" * 64,
        reason_code=None,
        verifies_plan_id=None,
        started_at="2026-01-01T00:00:00.000000Z",
        deadline_at="2026-01-01T01:00:00.000000Z",
        ended_at=None,
    )


ALLOWED = AlignmentDecision(
    strict_compare_allowed=True,
    reason_code=None,
    confidence=Confidence.PROVEN.value,
    checks={"compatible_snapshot_cut": True},
)


def blocked(reason: ReasonCode) -> AlignmentDecision:
    return AlignmentDecision(
        strict_compare_allowed=False,
        reason_code=reason,
        confidence=Confidence.INCONCLUSIVE.value,
        checks={"compatible_snapshot_cut": False},
    )


@pytest.fixture
def settings(tmp_path):
    return load_settings(
        env="test",
        db_path=tmp_path / "x.db",
        source_id="sim_local",
        approval_required=True,
        allow_deal_backfill=False,
        mass_drift_fraction=0.20,
        mass_drift_min_entities=10,
    )


# ---------------------------------------------------------------------------
# The section 4.3 matrix
# ---------------------------------------------------------------------------


def test_every_entity_and_diff_type_combination_is_mapped() -> None:
    """An unmapped combination would fall through to INCONCLUSIVE silently."""
    for entity in ("deals", "accounts", "positions"):
        for diff_type in ("MISSING_TARGET", "EXTRA_TARGET", "FIELD_MISMATCH"):
            assert (entity, diff_type) in CLASSIFICATION_MATRIX


@pytest.mark.parametrize(
    ("entity", "diff_type", "expected_code", "expected_severity"),
    [
        ("deals", "MISSING_TARGET", FindingCode.MISSING_TARGET_DEAL, Severity.ERROR),
        ("deals", "EXTRA_TARGET", FindingCode.EXTRA_TARGET_DEAL, Severity.CRITICAL),
        ("deals", "FIELD_MISMATCH", FindingCode.IMMUTABLE_DEAL_CONFLICT, Severity.CRITICAL),
        ("accounts", "FIELD_MISMATCH", FindingCode.ACCOUNT_FIELD_MISMATCH, Severity.ERROR),
        ("accounts", "EXTRA_TARGET", FindingCode.ABSENT_AT_SOURCE_ACCOUNT, Severity.WARN),
        ("positions", "EXTRA_TARGET", FindingCode.ABSENT_AT_SOURCE_POSITION, Severity.WARN),
        ("positions", "FIELD_MISMATCH", FindingCode.POSITION_FIELD_MISMATCH, Severity.ERROR),
    ],
)
def test_classification_follows_the_design_matrix(
    settings, entity, diff_type, expected_code, expected_severity
) -> None:
    classifier = DiffClassifier(settings)
    result = classifier.classify(
        run=make_run(entity=entity),
        rows=[DiffRow("K-1", diff_type, "a" * 64, "b" * 64)],
        decision=ALLOWED,
    )
    assert len(result.findings) == 1
    assert result.findings[0]["finding_code"] == expected_code.value
    assert result.findings[0]["severity"] == expected_severity.value


def test_absence_findings_are_provisional_not_proven(settings) -> None:
    """Section 5.3: absence from a listing is evidence of absence only."""
    classifier = DiffClassifier(settings)
    result = classifier.classify(
        run=make_run(entity="positions"),
        rows=[DiffRow("P-1", "EXTRA_TARGET", None, "b" * 64)],
        decision=ALLOWED,
    )
    assert result.findings[0]["confidence"] == Confidence.PROVISIONAL.value


def test_money_conflicts_are_proven_and_critical(settings) -> None:
    classifier = DiffClassifier(settings)
    result = classifier.classify(
        run=make_run(entity="deals"),
        rows=[DiffRow("D-1", "FIELD_MISMATCH", "a" * 64, "b" * 64)],
        decision=ALLOWED,
    )
    finding = result.findings[0]
    assert finding["confidence"] == Confidence.PROVEN.value
    assert finding["severity"] == Severity.CRITICAL.value


def test_matched_rows_produce_no_findings(settings) -> None:
    classifier = DiffClassifier(settings)
    result = classifier.classify(
        run=make_run(),
        rows=[DiffRow("D-1", "MATCHED", "a" * 64, "a" * 64)],
        decision=ALLOWED,
    )
    assert result.findings == []


# ---------------------------------------------------------------------------
# Blocked alignment collapses to one run-level finding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (ReasonCode.DIFFERENT_CUT, FindingCode.DIFFERENT_CUT),
        (ReasonCode.EPOCH_MISMATCH, FindingCode.EPOCH_MISMATCH),
        (ReasonCode.SOURCE_IDENTITY_CHANGED, FindingCode.EPOCH_MISMATCH),
        (ReasonCode.TARGET_NOT_CAUGHT_UP, FindingCode.INCONCLUSIVE_SOURCE),
    ],
)
def test_blocked_alignment_emits_one_inconclusive_finding(
    settings, reason, expected
) -> None:
    """Per-item differences must not be published as defects (IT04-02)."""
    classifier = DiffClassifier(settings)
    rows = [DiffRow(f"K-{i}", "FIELD_MISMATCH", "a" * 64, "b" * 64) for i in range(25)]

    result = classifier.classify(run=make_run(entity="accounts"), rows=rows, decision=blocked(reason))

    assert len(result.findings) == 1
    assert result.findings[0]["finding_code"] == expected.value
    assert result.findings[0]["confidence"] == Confidence.INCONCLUSIVE.value
    assert result.findings[0]["item_key"] is None
    assert result.has_proven_findings is False


# ---------------------------------------------------------------------------
# Mass drift and count-drop guards
# ---------------------------------------------------------------------------


def test_mass_drift_is_flagged_above_the_configured_fraction(settings) -> None:
    classifier = DiffClassifier(settings)
    rows = [DiffRow(f"A-{i}", "FIELD_MISMATCH", "a" * 64, "b" * 64) for i in range(30)]

    result = classifier.classify(
        run=make_run(entity="accounts", source_count=100, target_count=100),
        rows=rows,
        decision=ALLOWED,
    )

    assert result.mass_drift is True


def test_mass_drift_is_not_flagged_below_the_fraction(settings) -> None:
    classifier = DiffClassifier(settings)
    rows = [DiffRow(f"A-{i}", "FIELD_MISMATCH", "a" * 64, "b" * 64) for i in range(5)]

    result = classifier.classify(
        run=make_run(entity="accounts", source_count=100, target_count=100),
        rows=rows,
        decision=ALLOWED,
    )

    assert result.mass_drift is False


def test_mass_drift_does_not_apply_to_deal_backfills(settings) -> None:
    """Many missing historical deals is a legitimate repair shape."""
    classifier = DiffClassifier(settings)
    rows = [DiffRow(f"D-{i}", "MISSING_TARGET", "a" * 64, None) for i in range(50)]

    result = classifier.classify(
        run=make_run(entity="deals", source_count=100, target_count=50),
        rows=rows,
        decision=ALLOWED,
    )

    assert result.mass_drift is False


def test_large_source_count_drop_is_flagged(settings) -> None:
    classifier = DiffClassifier(settings)
    result = classifier.classify(
        run=make_run(entity="deals", source_count=2, target_count=100),
        rows=[DiffRow(f"D-{i}", "EXTRA_TARGET", None, "b" * 64) for i in range(98)],
        decision=ALLOWED,
    )
    assert result.large_count_drop is True
    assert any(
        f["finding_code"] == FindingCode.LARGE_SOURCE_COUNT_DROP.value for f in result.findings
    )


def test_small_target_sets_do_not_trigger_the_drop_guard(settings) -> None:
    """A two-row dataset shrinking is not statistically a mass event."""
    classifier = DiffClassifier(settings)
    result = classifier.classify(
        run=make_run(entity="deals", source_count=0, target_count=2),
        rows=[DiffRow("D-1", "EXTRA_TARGET", None, "b" * 64)],
        decision=ALLOWED,
    )
    assert result.large_count_drop is False


# ---------------------------------------------------------------------------
# Policy guardrails (section 8.1)
# ---------------------------------------------------------------------------


def test_every_finding_code_has_a_policy_action() -> None:
    """A missing mapping would silently fall back to no action at all."""
    for code in FindingCode:
        assert code in ACTION_FOR_FINDING, f"{code} has no policy action"


def test_no_finding_maps_to_a_destructive_action() -> None:
    assert set(ACTION_FOR_FINDING.values()) <= set(ActionType)
    assert ActionType.MANUAL_INVESTIGATION in set(ACTION_FOR_FINDING.values())


def _result(findings):
    from reconciliation.diff_engine import ClassificationResult

    return ClassificationResult(findings=findings, counts={})


def _finding(code: FindingCode, *, confidence=Confidence.PROVEN, severity=Severity.ERROR):
    return {
        "item_key": "K-1",
        "finding_code": code.value,
        "severity": severity.value,
        "confidence": confidence.value,
        "evidence_hash": "e" * 64,
        "explanation_code": "X",
    }


def test_inconclusive_evidence_makes_the_run_ineligible(settings) -> None:
    policy = RepairPolicy(settings)
    decision = policy.evaluate(
        run=make_run(),
        result=_result([_finding(FindingCode.MISSING_TARGET_DEAL, confidence=Confidence.INCONCLUSIVE)]),
    )
    assert decision.eligible is False
    assert decision.is_dispatchable is False
    assert "INCONCLUSIVE_EVIDENCE_PRESENT" in decision.escalations


def test_incomplete_manifest_makes_the_run_ineligible(settings) -> None:
    policy = RepairPolicy(settings)
    decision = policy.evaluate(
        run=make_run(source_complete=False),
        result=_result([_finding(FindingCode.ACCOUNT_FIELD_MISMATCH)]),
    )
    assert decision.eligible is False
    assert decision.reason_code is ReasonCode.SOURCE_INCOMPLETE
    assert decision.actions == ()


@pytest.mark.parametrize("code", sorted(BLOCKING_FINDINGS, key=lambda c: c.value))
def test_blocking_findings_yield_investigation_only(settings, code) -> None:
    policy = RepairPolicy(settings)
    decision = policy.evaluate(run=make_run(), result=_result([_finding(code)]))

    assert decision.eligible is True
    assert decision.is_dispatchable is False
    assert {a.action_type for a in decision.actions} == {ActionType.MANUAL_INVESTIGATION}
    assert code.value in decision.escalations


def test_account_mismatch_is_dispatchable_as_a_resync(settings) -> None:
    policy = RepairPolicy(settings)
    decision = policy.evaluate(
        run=make_run(entity="accounts"),
        result=_result([_finding(FindingCode.ACCOUNT_FIELD_MISMATCH)]),
    )
    assert decision.eligible is True
    assert decision.is_dispatchable is True
    assert {a.action_type for a in decision.actions} == {ActionType.REQUEST_FULL_ACCOUNT_RESYNC}
    assert decision.requires_approval is True


def test_deal_backfill_is_downgraded_when_disabled(settings) -> None:
    policy = RepairPolicy(settings)
    decision = policy.evaluate(
        run=make_run(entity="deals"),
        result=_result([_finding(FindingCode.MISSING_TARGET_DEAL)]),
    )
    assert {a.action_type for a in decision.actions} == {ActionType.MANUAL_INVESTIGATION}
    assert "DEAL_BACKFILL_DISABLED_BY_POLICY" in decision.escalations


def test_deal_backfill_is_offered_when_enabled(tmp_path) -> None:
    settings = load_settings(
        env="test",
        db_path=tmp_path / "y.db",
        source_id="sim_local",
        approval_required=True,
        allow_deal_backfill=True,
    )
    policy = RepairPolicy(settings)
    decision = policy.evaluate(
        run=make_run(entity="deals"),
        result=_result([_finding(FindingCode.MISSING_TARGET_DEAL)]),
    )
    actions = {a.action_type for a in decision.actions}
    assert actions == {ActionType.REQUEST_DEAL_BACKFILL}
    assert decision.requires_approval is True
    params = decision.actions[0].parameters
    # Provenance and bounds only; no values to write.
    assert params["entity"] == "deals"
    assert params["upper_bound_seq"] == 1000
    assert not any(key in params for key in ("balance", "commission", "swap", "price"))


def test_action_parameters_never_carry_monetary_values(settings) -> None:
    policy = RepairPolicy(settings)
    for code in (
        FindingCode.ACCOUNT_FIELD_MISMATCH,
        FindingCode.POSITION_FIELD_MISMATCH,
        FindingCode.MISSING_TARGET_ACCOUNT,
    ):
        decision = policy.evaluate(run=make_run(entity="accounts"), result=_result([_finding(code)]))
        for action in decision.actions:
            for key in action.parameters:
                assert key not in {"balance", "credit", "commission", "swap", "price", "volume_lots"}


def test_evidence_freshness_cap_is_enforced(settings) -> None:
    policy = RepairPolicy(settings)
    assert policy.check_evidence_freshness(evidence_age_s=1) is None
    assert (
        policy.check_evidence_freshness(evidence_age_s=settings.evidence_max_age_s + 1)
        is ReasonCode.EVIDENCE_STALE
    )


def test_empty_findings_produce_no_plan(settings) -> None:
    policy = RepairPolicy(settings)
    decision = policy.evaluate(run=make_run(), result=_result([]))
    assert decision.eligible is False
    assert decision.actions == ()


# ---------------------------------------------------------------------------
# Cross-entity tier resolution (section 5.4)
# ---------------------------------------------------------------------------


def test_configuration_cannot_grant_a_tier_the_source_lacks() -> None:
    """Section 0.4: no global cut is claimed without a tested contract."""
    without = SourceCapabilities(global_cut_across_entities=False)
    assert cross_entity_tier(without, "T2") == "T1"
    assert cross_entity_tier(without, "T1") == "T1"
    assert cross_entity_tier(without, "T0") == "T0"


def test_tier_two_requires_the_capability() -> None:
    with_cut = SourceCapabilities(global_cut_across_entities=True)
    assert cross_entity_tier(with_cut, "T2") == "T2"
    assert cross_entity_tier(with_cut, "T1") == "T1"
