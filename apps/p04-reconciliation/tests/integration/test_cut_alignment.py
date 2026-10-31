"""Cut alignment and false-positive guards: IT04-02, F-04, F-05, F-06.

These tests cover the failure the design rates most dangerous after a false
``MATCHED``: calling a legitimate change "corruption" and then repairing it.

The rules under test:

* **Mutable projections need the same cut.** Source read at S+1 versus a
  target published at S is ``DIFFERENT_CUT`` -- a freshness mismatch, not a
  defect (section 5.3, IT04-02).
* **Deals do not need equal snapshot numbers.** They need ``W >= S``. A
  watermark behind the source snapshot is ``TARGET_NOT_CAUGHT_UP``, which is
  explicitly *not* a missing-data finding (INV04-04).
* **Absence is an observation.** A position in the target and absent from a
  complete source listing is ``ABSENT_AT_SOURCE_POSITION`` at ``PROVISIONAL``
  confidence, never ``CLOSED`` (F-05).
* **A filtered listing cannot support an absence claim at all** (F-06).
"""

from __future__ import annotations

import pytest

from reconciliation.db.engine import Role
from reconciliation.errors import (
    ActionType,
    Confidence,
    DataIntegrityError,
    FindingCode,
    ReasonCode,
)
from reconciliation.scheduler import ReconciliationScheduler
from tests.conftest import SyncedWorld

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# IT04-02 / F-04: account changed after publication is not corruption
# ---------------------------------------------------------------------------


def test_account_changed_after_publish_is_different_cut_not_corruption(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_admin, db
) -> None:
    """IT04-02 / INV04-03: source at S+n, target at S -> DIFFERENT_CUT.

    INV04-03 is the "no CORRUPT from a mismatched Account/Position cut" rule:
    a freshness difference must never be reported as a defect.
    """
    before = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    assert before.status == "MATCHED"

    # Move the source forward: new events change account state and S.
    p01_admin.advance(synced_world.run_id, events=12)
    moved = p01_admin.meta()
    assert int(moved["snapshot_seq"]) > synced_world.snapshot_seq

    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.DIFFERENT_CUT
    # Exactly one run-level finding, carrying the blocking reason -- not a
    # list of per-account "defects".
    assert outcome.finding_count == 1
    finding = outcome.findings[0]
    assert finding["finding_code"] == FindingCode.DIFFERENT_CUT.value
    assert finding["confidence"] == Confidence.INCONCLUSIVE.value
    assert finding["item_key"] is None
    assert "STRICT_COMPARE_BLOCKED_DIFFERENT_CUT" in finding["explanation_code"]

    # No account field mismatch was claimed, and no plan may be built.
    assert not any(
        f["finding_code"] == FindingCode.ACCOUNT_FIELD_MISMATCH.value for f in outcome.findings
    )
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None


def test_realigning_the_cut_restores_matched(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_admin, harness
) -> None:
    """The documented remedy for DIFFERENT_CUT: re-sync, then re-audit."""
    p01_admin.advance(synced_world.run_id, events=10)
    blocked = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    assert blocked.reason_code is ReasonCode.DIFFERENT_CUT

    harness.publish_snapshot(synced_world.epoch_id, "accounts")
    realigned = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)

    assert realigned.status == "MATCHED", (realigned.reason_code, realigned.detail)
    assert realigned.finding_count == 0


# ---------------------------------------------------------------------------
# INV04-04: W < S is not a missing deal
# ---------------------------------------------------------------------------


def test_watermark_behind_source_is_not_a_missing_deal_finding(
    scheduler: ReconciliationScheduler, p01_admin, harness, source, clock, db
) -> None:
    """INV04-04: an un-caught-up target makes no missing-data claims.

    The target genuinely lacks most deals here. Reporting them as missing
    would be the classic false positive: P03 simply has not synced them yet.
    """
    total = 40
    p01_admin.create_run(accounts=4, deals=total, max_open_positions=4)
    epoch_id = harness.bootstrap_epoch()

    # A small page size so one page really is a fraction of the data; the
    # default 100 would swallow the whole set and make the fixture vacuous.
    from recon_harness.p03_sync import P03SyncHarness

    partial = P03SyncHarness(
        db=db, source=source, clock=clock, source_id="sim_local", page_size=8
    )
    partial.sync_deals(epoch_id, max_pages=1, advance_watermark=False)
    assert harness.deal_watermark(epoch_id) == 0

    conn = db.connect(Role.P03_SYNC)
    try:
        stored = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE epoch_id = ?", (epoch_id,)
        ).fetchone()[0]
    finally:
        conn.close()
    assert 0 < stored < total, "fixture must leave the target genuinely incomplete"

    outcome = scheduler.run_audit(entity="deals", epoch_id=epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.TARGET_NOT_CAUGHT_UP
    assert not any(
        f["finding_code"] == FindingCode.MISSING_TARGET_DEAL.value for f in outcome.findings
    )
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None


def test_deals_match_when_watermark_passes_source_snapshot(
    scheduler: ReconciliationScheduler, p01_admin, harness
) -> None:
    """The deal rule is W >= S, not S_source == S_target."""
    p01_admin.create_run(accounts=4, deals=30, max_open_positions=4)
    epoch_id = harness.bootstrap_epoch()
    harness.sync_deals(epoch_id)

    outcome = scheduler.run_audit(entity="deals", epoch_id=epoch_id)

    assert outcome.status == "MATCHED"
    assert outcome.decision is not None
    cut = outcome.decision.detail["cut"]
    assert cut["rule"] == "immutable_deal_history_requires_W_ge_S"
    assert cut["target_deal_watermark"] >= cut["source_snapshot_seq"]


# ---------------------------------------------------------------------------
# F-05: absent from source listing is not CLOSED
# ---------------------------------------------------------------------------


def test_position_absent_from_source_is_an_observation_not_a_close(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    """F-05: evidence of absence, explicitly not evidence of closure."""
    synced_world.harness.corrupt_for_test(
        """
        INSERT INTO positions_current (epoch_id, position_id, account_id, symbol, side,
                                       volume_lots, open_price, status, published_scan_id, payload_hash)
        VALUES (?, 'P-77777777', 'A-000001', 'EURUSD', 'BUY', '0.50', '1.10000', 'OPEN', ?, 'x')
        """,
        (synced_world.epoch_id, synced_world.position_scan_id),
    )

    outcome = scheduler.run_audit(entity="positions", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    finding = next(
        f
        for f in outcome.findings
        if f["finding_code"] == FindingCode.ABSENT_AT_SOURCE_POSITION.value
    )
    assert finding["item_key"] == "P-77777777"
    # Proven that it is absent from a complete listing; still only an
    # observation about existence, so PROVISIONAL.
    assert finding["confidence"] == Confidence.PROVISIONAL.value
    assert "NOT_CLOSED" in finding["explanation_code"]

    # The only permitted action is a human looking at it.
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    assert {a.action_type for a in plan.actions} == {ActionType.MANUAL_INVESTIGATION}

    # The row is untouched: nothing was marked CLOSED or deleted.
    conn = db.connect(Role.P03_SYNC)
    try:
        row = conn.execute(
            "SELECT status FROM positions_current WHERE epoch_id = ? AND position_id = 'P-77777777'",
            (synced_world.epoch_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None and row["status"] == "OPEN"


# ---------------------------------------------------------------------------
# F-06: a filtered scan cannot support a full-set comparison
# ---------------------------------------------------------------------------


def test_filtered_scope_is_refused_at_the_source_port(source) -> None:
    """F-06: the port refuses a filtered scan outright.

    Rejecting at the boundary means no downstream code can receive a filtered
    manifest and mistake its omissions for absences (section 5.3).
    """
    with pytest.raises(DataIntegrityError) as exc:
        list(source.iter_pages("positions", scope="OPEN_ONLY"))
    assert exc.value.reason is ReasonCode.SOURCE_SCAN_FILTERED


def test_filtered_scope_run_is_inconclusive_not_matched(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld
) -> None:
    outcome = scheduler.run_audit(
        entity="positions", epoch_id=synced_world.epoch_id, scope="OPEN_ONLY"
    )
    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.SOURCE_SCAN_FILTERED


# ---------------------------------------------------------------------------
# Account field mismatch at an equal cut is a real, repairable finding
# ---------------------------------------------------------------------------


def test_account_mismatch_at_equal_cut_plans_a_full_resync(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    """The contrast case: same cut, so a difference really is a defect."""
    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()["account_id"]
    finally:
        conn.close()

    synced_world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '1.23' WHERE epoch_id = ? AND account_id = ?",
        (synced_world.epoch_id, victim),
    )

    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    finding = next(
        f for f in outcome.findings if f["finding_code"] == FindingCode.ACCOUNT_FIELD_MISMATCH.value
    )
    assert finding["item_key"] == victim
    assert finding["confidence"] == Confidence.PROVEN.value

    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    assert {a.action_type for a in plan.actions} == {ActionType.REQUEST_FULL_ACCOUNT_RESYNC}
    # A re-sync request, not a balance assignment.
    assert "balance" not in plan.actions[0].parameters
    assert plan.actions[0].parameters["entity"] == "accounts"
