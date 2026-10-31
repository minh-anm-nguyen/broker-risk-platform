"""Approval, fencing, dispatch and verification: IT04-01, IT04-04, IT04-09, F-09.

The lifecycle invariant from section 4.2 drives every test here:

    APPROVED is not APPLIED
    ACCEPTED is not COMMITTED
    COMMITTED is not VERIFIED

So the suite checks each arrow separately, and specifically that the chain
cannot be short-circuited: no approval means no dispatch, a lost ACK means
``UNKNOWN`` rather than a retry, and a P03 receipt saying "committed" does not
by itself produce the word ``VERIFIED``.
"""

from __future__ import annotations

import pytest

from reconciliation.approvals import Role as ApprovalRole
from reconciliation.db.engine import Role
from reconciliation.errors import ActionType, ReasonCode
from reconciliation.scheduler import ReconciliationScheduler
from tests.conftest import SyncedWorld

pytestmark = pytest.mark.integration

AUTHOR = "auditor-1"
APPROVER = "approver-2"


def _corrupt_one_account(world: SyncedWorld, db) -> str:
    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? ORDER BY account_id LIMIT 1",
            (world.epoch_id,),
        ).fetchone()["account_id"]
    finally:
        conn.close()
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '0.01' WHERE epoch_id = ? AND account_id = ?",
        (world.epoch_id, victim),
    )
    return victim


@pytest.fixture
def account_plan(scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db):
    """A real, approved-pending plan for a genuine account mismatch."""
    victim = _corrupt_one_account(synced_world, db)
    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    assert outcome.status == "FINDINGS", (outcome.reason_code, outcome.detail)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by=AUTHOR)
    assert plan is not None
    assert {a.action_type for a in plan.actions} == {ActionType.REQUEST_FULL_ACCOUNT_RESYNC}
    assert plan.status == "PENDING_APPROVAL"
    return plan, victim, outcome


# ---------------------------------------------------------------------------
# Approval gate (FR04-09, INV04-06)
# ---------------------------------------------------------------------------


def test_dispatch_without_approval_is_refused(
    scheduler: ReconciliationScheduler, account_plan
) -> None:
    plan, _victim, _outcome = account_plan

    result = scheduler.dispatch(plan)

    assert result.outcomes == ()
    assert result.reason_code is ReasonCode.APPROVAL_MISSING
    # The plan is untouched, still awaiting a decision.
    assert scheduler.approvals.verify_for_dispatch(
        plan_id=plan.plan_id, expected_plan_hash=plan.plan_hash
    ).allowed is False


def test_self_approval_is_refused(scheduler: ReconciliationScheduler, account_plan) -> None:
    """Separation of duties: the author may not authorise their own plan."""
    plan, _victim, _outcome = account_plan

    outcome = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id=AUTHOR,  # same identity that created it
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )

    assert outcome.allowed is False
    assert outcome.reason_code is ReasonCode.APPROVAL_SELF


@pytest.mark.parametrize("role", [ApprovalRole.AUDITOR, ApprovalRole.SERVICE])
def test_non_approving_roles_cannot_approve(
    scheduler: ReconciliationScheduler, account_plan, role: ApprovalRole
) -> None:
    plan, _victim, _outcome = account_plan

    outcome = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="someone-else",
        role=role,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )

    assert outcome.allowed is False
    assert outcome.reason_code is ReasonCode.APPROVAL_ROLE_INVALID


def test_approving_a_different_plan_hash_is_refused(
    scheduler: ReconciliationScheduler, account_plan
) -> None:
    """INV04-06: the approval binds to the content the approver saw."""
    plan, _victim, _outcome = account_plan

    outcome = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id=APPROVER,
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash="0" * 64,
    )

    assert outcome.allowed is False
    assert outcome.reason_code is ReasonCode.PLAN_DIGEST_CHANGED


def test_rejected_plan_cannot_later_be_approved(
    scheduler: ReconciliationScheduler, account_plan
) -> None:
    plan, _victim, _outcome = account_plan

    rejected = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id=APPROVER,
        role=ApprovalRole.APPROVER,
        decision="REJECT",
        plan_hash=plan.plan_hash,
    )
    assert rejected.allowed is True

    retried = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="approver-3",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )
    assert retried.allowed is False

    result = scheduler.dispatch(plan)
    assert result.outcomes == ()
    assert result.reason_code is ReasonCode.POLICY_BLOCK


def test_expired_plan_cannot_be_approved(
    scheduler: ReconciliationScheduler, account_plan, settings, clock
) -> None:
    plan, _victim, _outcome = account_plan

    clock.advance(settings.plan_ttl_s + 60)

    outcome = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id=APPROVER,
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )
    assert outcome.allowed is False
    assert outcome.reason_code is ReasonCode.APPROVAL_EXPIRED


def test_stale_evidence_blocks_dispatch_of_an_approved_plan(
    scheduler: ReconciliationScheduler, account_plan, settings, clock
) -> None:
    """Section 8.1: evidence older than the cap makes the plan undispatchable.

    Approval was valid when given; the world has since moved on. Applying an
    old decision to new data is how a correct judgement becomes a wrong act.
    """
    plan, _victim, _outcome = account_plan
    approved = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id=APPROVER,
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )
    assert approved.allowed is True

    clock.advance(settings.evidence_max_age_s + 30)

    result = scheduler.dispatch(plan)
    assert result.outcomes == ()
    assert result.reason_code is ReasonCode.EVIDENCE_STALE


# ---------------------------------------------------------------------------
# Happy path: COMMITTED then independently VERIFIED
# ---------------------------------------------------------------------------


def _approve(scheduler: ReconciliationScheduler, plan) -> None:
    outcome = scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id=APPROVER,
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )
    assert outcome.allowed is True, outcome.detail


def test_approved_resync_commits_then_verifies_independently(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, db, repository
) -> None:
    """The full controlled-recovery path, with VERIFIED earned by a new audit."""
    plan, victim, _outcome = account_plan
    _approve(scheduler, plan)

    result = scheduler.dispatch(plan)

    assert result.all_committed, [o.status for o in result.outcomes]
    # COMMITTED moves the plan to VERIFYING, never straight to VERIFIED.
    assert result.plan_status == "VERIFYING"
    assert repository.get_plan(plan.plan_id)["status"] == "VERIFYING"

    # P03 actually re-derived the account from the source.
    conn = db.connect(Role.P03_SYNC)
    try:
        balance = conn.execute(
            "SELECT balance FROM accounts_current WHERE epoch_id = ? AND account_id = ?",
            (synced_world.epoch_id, victim),
        ).fetchone()["balance"]
    finally:
        conn.close()
    assert balance != "0.01"

    verification = scheduler.verify(plan, entity="accounts")

    assert verification.verified is True
    assert verification.plan_status == "VERIFIED"
    assert verification.verification_run_id is not None

    # The verification run is a real, separate audit linked to the plan.
    verify_run = repository.get_run(verification.verification_run_id)
    assert verify_run is not None
    assert verify_run.verifies_plan_id == plan.plan_id
    assert verify_run.status == "MATCHED"
    assert verify_run.recon_run_id != plan.recon_run_id


def test_verified_requires_a_matched_audit_not_just_a_receipt(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, db, repository
) -> None:
    """INV04-12: a committed receipt plus a still-broken target is FAILED.

    P03 commits the re-sync, and then the target is corrupted again before
    verification. The receipt still says ``COMMITTED``; the plan must not be
    ``VERIFIED``.
    """
    plan, victim, _outcome = account_plan
    _approve(scheduler, plan)
    result = scheduler.dispatch(plan)
    assert result.all_committed

    # Break it again, after the commit but before verification.
    synced_world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '0.02' WHERE epoch_id = ? AND account_id = ?",
        (synced_world.epoch_id, victim),
    )

    verification = scheduler.verify(plan, entity="accounts")

    assert verification.verified is False
    assert verification.plan_status == "FAILED"
    assert repository.get_plan(plan.plan_id)["status"] == "FAILED"


# ---------------------------------------------------------------------------
# IT04-09: acceptance is not verification
# ---------------------------------------------------------------------------


def test_accepted_then_failed_never_becomes_verified(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, harness, repository
) -> None:
    """IT04-09: P03 accepts the intent then fails during source capture."""
    plan, victim, _outcome = account_plan
    _approve(scheduler, plan)
    harness.faults.accept_then_fail.arm(1)

    result = scheduler.dispatch(plan)

    assert harness.faults.accept_then_fail.fired, "the fault must have activated"
    assert result.plan_status == "FAILED"
    assert result.reason_code is ReasonCode.REPAIR_FAILED
    assert repository.get_plan(plan.plan_id)["status"] == "FAILED"

    # Verification refuses to run on a plan that was never committed.
    verification = scheduler.verify(plan, entity="accounts")
    assert verification.verified is False
    assert verification.plan_status == "FAILED"


def test_rejected_intent_is_recorded_and_not_retried_blindly(
    scheduler: ReconciliationScheduler, account_plan, harness, repository
) -> None:
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)
    harness.faults.reject_intent.arm(1)

    result = scheduler.dispatch(plan)

    assert harness.faults.reject_intent.fired
    assert result.plan_status == "FAILED"
    assert result.reason_code is ReasonCode.REPAIR_REJECTED

    receipt = harness.repair_intent_status(plan.actions[0].intent_id)
    assert receipt is not None and receipt.status == "REJECTED"
    assert receipt.execution_count == 0, "a rejected intent must not have executed work"


# ---------------------------------------------------------------------------
# F-09 / IT04-01: lost ACK after commit
# ---------------------------------------------------------------------------


def test_lost_ack_yields_unknown_and_resolves_without_double_execution(
    scheduler: ReconciliationScheduler, account_plan, harness, repository, db, synced_world
) -> None:
    """F-09: P03 committed, the response vanished.

    The dangerous responses are "retry" (runs the work twice) and "failed"
    (loses a committed change). The correct one is ``UNKNOWN`` followed by a
    receipt query on the same intent id (INV04-09).
    """
    plan, victim, _outcome = account_plan
    _approve(scheduler, plan)
    harness.faults.lose_repair_ack.arm(1)

    result = scheduler.dispatch(plan)

    assert harness.faults.lose_repair_ack.fired, "the lost-ACK fault must have activated"
    assert result.plan_status == "UNKNOWN"
    assert result.reason_code is ReasonCode.REPAIR_UNKNOWN
    assert result.outcomes[0].is_unknown

    intent_id = plan.actions[0].intent_id
    receipt = harness.repair_intent_status(intent_id)
    assert receipt is not None and receipt.is_committed
    assert receipt.execution_count == 1

    # Resolve by asking about the same intent id. No new intent is created.
    from reconciliation.executor import RepairExecutor

    executor = RepairExecutor(
        repository=repository,
        approvals=scheduler.approvals,
        leases=scheduler.leases,
        read_port=harness,
        repair_port=harness,
        settings=scheduler._settings,
        clock=scheduler._clock,
    )
    resolved = executor.resolve_unknown(action_id=plan.actions[0].action_id, intent_id=intent_id)

    assert resolved.status == "COMMITTED"
    assert resolved.intent_id == intent_id
    # Still exactly one execution: resolving did not re-run the work.
    assert harness.repair_intent_status(intent_id).execution_count == 1


def test_replaying_the_same_intent_id_executes_the_work_once(
    scheduler: ReconciliationScheduler, account_plan, harness
) -> None:
    """INV04-08: P03 dedups on ``intent_id``, so a replay is a no-op.

    Asserted at the port, which is where the guarantee lives: after a lost
    ACK the worker may legitimately resend the identical request, and the
    work must not run a second time.
    """
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)
    action = plan.actions[0]

    first = scheduler.dispatch(plan)
    assert first.all_committed
    assert harness.repair_intent_status(action.intent_id).execution_count == 1

    # The identical request, presented again exactly as the executor would.
    replayed = harness.request_full_resync(
        intent_id=action.intent_id,
        source_id="sim_local",
        entity="accounts",
        expected_epoch_id=plan.expected_epoch_id,
        expected_revision=plan.expected_target_scan_id,
        expected_lease_version=plan.expected_lease_version,
        approved_plan_hash=plan.plan_hash,
        parameters=action.parameters,
    )

    assert replayed.is_committed
    assert replayed.execution_count == 1, "a replayed intent must not execute again"
    assert harness.repair_intent_status(action.intent_id).execution_count == 1


def test_an_already_committed_plan_is_not_dispatchable_again(
    scheduler: ReconciliationScheduler, account_plan, harness, repository
) -> None:
    """A plan awaiting verification is past its dispatch window.

    Re-dispatching would be a second repair attempt for a question already
    answered, so the approval gate refuses it (section 4.2).
    """
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)
    assert scheduler.dispatch(plan).plan_status == "VERIFYING"

    second = scheduler.dispatch(plan)

    assert second.outcomes == ()
    assert second.reason_code is ReasonCode.POLICY_BLOCK
    assert repository.get_plan(plan.plan_id)["status"] == "VERIFYING"
    assert harness.repair_intent_status(plan.actions[0].intent_id).execution_count == 1


def test_unknown_with_no_p03_record_permits_a_same_id_retry(
    scheduler: ReconciliationScheduler, account_plan, harness, repository
) -> None:
    """The other branch of section 7.6: P03 never saw the intent at all."""
    plan, _victim, _outcome = account_plan
    from reconciliation.executor import RepairExecutor

    executor = RepairExecutor(
        repository=repository,
        approvals=scheduler.approvals,
        leases=scheduler.leases,
        read_port=harness,
        repair_port=harness,
        settings=scheduler._settings,
        clock=scheduler._clock,
    )

    resolved = executor.resolve_unknown(
        action_id=plan.actions[0].action_id, intent_id="never-dispatched-intent"
    )

    assert resolved.status == "ABSENT"
    assert "same intent id is safe" in resolved.detail


# ---------------------------------------------------------------------------
# IT04-04: stale lease and stale revision
# ---------------------------------------------------------------------------


def test_worker_that_lost_its_lease_cannot_dispatch(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, settings
) -> None:
    """INV04-10: a fenced worker is told it lost, and changes nothing."""
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)

    scope_hash = settings.scope_hash("accounts")
    worker_a = scheduler.leases.acquire(
        epoch_id=synced_world.epoch_id, entity="accounts", scope_hash=scope_hash
    )
    # Worker B takes over, bumping the version and fencing A.
    worker_b = scheduler.leases.steal(
        epoch_id=synced_world.epoch_id,
        entity="accounts",
        scope_hash=scope_hash,
        owner_id="recon-worker-B",
    )
    assert worker_b.version > worker_a.version

    result = scheduler.dispatch(plan, lease=worker_a)

    assert result.plan_status == "STALE"
    assert result.reason_code is ReasonCode.LEASE_LOST
    assert result.outcomes == ()


def test_plan_goes_stale_when_p03_publishes_a_newer_scan(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, harness, repository
) -> None:
    """IT04-04's second clause: the expected revision moved.

    The plan's evidence describes published scan X. P03 has since published Y,
    so the plan is answering a question about a projection that no longer
    exists.
    """
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)

    harness.publish_snapshot(synced_world.epoch_id, "accounts")  # scan Y

    result = scheduler.dispatch(plan)

    assert result.plan_status == "STALE"
    assert result.reason_code is ReasonCode.PLAN_STALE
    assert result.outcomes == ()
    assert repository.get_plan(plan.plan_id)["status"] == "STALE"


def test_source_reset_cancels_an_approved_plan_before_p03_notices(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, p01_admin, harness
) -> None:
    """INV04-11: a regenerated source cancels plans for the old epoch.

    The subtle part is the timing. P03's ``source_epochs`` row stays `ACTIVE`
    until somebody re-bootstraps, so a check that only read P03's state would
    happily dispatch a repair against a dataset that no longer exists. The
    guard therefore compares the *source's* live ``(run_id, generation)``
    against the pair recorded for the plan's epoch.
    """
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)

    # The source regenerates. P03 has not been told yet.
    p01_admin.reset_run(synced_world.run_id, expected_generation=synced_world.generation)
    status = harness.sync_status("sim_local", synced_world.epoch_id, "accounts")
    assert status.epoch_status == "ACTIVE", (
        "fixture precondition: P03 must still believe the old epoch is active"
    )

    result = scheduler.dispatch(plan)

    assert result.plan_status == "STALE"
    assert result.reason_code is ReasonCode.EPOCH_MISMATCH
    assert result.outcomes == ()
    assert harness.repair_intent_status(plan.actions[0].intent_id) is None, (
        "no intent may reach P03 for a regenerated source"
    )


def test_quarantined_epoch_blocks_dispatch(
    scheduler: ReconciliationScheduler, account_plan, synced_world: SyncedWorld, harness
) -> None:
    """Section 7.7: a reset source blocks plans from the old epoch."""
    plan, _victim, _outcome = account_plan
    _approve(scheduler, plan)

    harness.quarantine_epoch(synced_world.epoch_id)

    result = scheduler.dispatch(plan)

    assert result.plan_status == "STALE"
    assert result.reason_code is ReasonCode.EPOCH_MISMATCH


# ---------------------------------------------------------------------------
# Investigation-only plans
# ---------------------------------------------------------------------------


def test_investigation_only_plan_dispatches_nothing(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db, harness
) -> None:
    """A money conflict yields a plan with no dispatchable action at all."""
    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT deal_id FROM deals WHERE epoch_id = ? LIMIT 1", (synced_world.epoch_id,)
        ).fetchone()["deal_id"]
    finally:
        conn.close()
    harness.corrupt_for_test(
        "UPDATE deals SET swap = '-77.00' WHERE epoch_id = ? AND deal_id = ?",
        (synced_world.epoch_id, victim),
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by=AUTHOR)
    assert plan is not None and plan.dispatchable_actions == ()

    result = scheduler.dispatch(plan)

    # Nothing to dispatch, so the decision is complete as recorded.
    assert result.outcomes == ()
    assert result.plan_status == "VERIFIED"
    # And no repair intent was ever created.
    assert harness.repair_intent_status(plan.actions[0].intent_id) is None
