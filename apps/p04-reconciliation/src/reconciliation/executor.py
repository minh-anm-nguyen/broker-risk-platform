"""Fenced repair dispatch and ambiguous-outcome handling (sections 7.5, 7.6).

This is the only module that asks Project 03 to change anything, and it is
written around one fact: **a missing response is not a failure.** If the
network drops after P03 committed, retrying the request would run the work
twice; treating it as failed would leave a committed change unrecorded. Both
are wrong. Section 0.2 decision 7 names the correct answer: the outcome is
``UNKNOWN``, and the only way out is to ask P03 about the *same* intent id.

The dispatch sequence, with the reason each step exists:

1. **Validate the lease.** A worker that lost its lease during a pause must
   not dispatch (INV04-10).
2. **Pass the approval gate**, re-checked now rather than at approval time
   (section 4.2: ``APPROVED`` is not ``APPLIED``).
3. **Re-verify the preconditions** against P03's current state. The epoch must
   still be active and the expected revision/watermark must still hold, or the
   plan is ``STALE`` (section 7.5 "approval does not last forever").
4. **Record the attempt before dispatching.** If the process dies mid-call,
   the durable attempt row is what tells the recovering worker that an intent
   may be in flight (section 7.6).
5. **Dispatch, then classify the outcome**, never assuming success from the
   absence of an exception.

``COMMITTED`` from P03 moves the plan to ``VERIFYING``, not ``VERIFIED``. Only
``verifier.py``, after an independent audit at a compatible cut, may use that
word (INV04-12).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from .approvals import ApprovalService
from .audit import AuditEvent, AuditLog
from .clock import Clock
from .config import Settings
from .db.repository import ReconRepository
from .dto import RepairReceipt
from .errors import (
    ActionType,
    ApprovalError,
    LeaseLostError,
    PlanStaleError,
    ReasonCode,
    ReconError,
    SourceError,
)
from .leases import Lease, LeaseManager
from .planning import RepairPlan
from .ports.p03 import P03ReadPort, P03RepairPort
from .ports.source import SourcePort


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    """What happened to one action."""

    action_id: str
    intent_id: str
    status: str
    reason_code: ReasonCode | None = None
    receipt: RepairReceipt | None = None
    detail: str = ""

    @property
    def is_unknown(self) -> bool:
        return self.status == "UNKNOWN"


@dataclass(frozen=True, slots=True)
class PlanDispatchResult:
    plan_id: str
    plan_status: str
    outcomes: tuple[DispatchOutcome, ...]
    reason_code: ReasonCode | None = None

    @property
    def all_committed(self) -> bool:
        return bool(self.outcomes) and all(
            outcome.status == "COMMITTED" for outcome in self.outcomes
        )


class RepairExecutor:
    """Dispatches approved plans to P03 under fencing and idempotency."""

    def __init__(
        self,
        *,
        repository: ReconRepository,
        approvals: ApprovalService,
        leases: LeaseManager,
        read_port: P03ReadPort,
        repair_port: P03RepairPort,
        settings: Settings,
        clock: Clock,
        source: SourcePort | None = None,
    ) -> None:
        self._repo = repository
        self._approvals = approvals
        self._leases = leases
        self._read = read_port
        self._repair = repair_port
        self._settings = settings
        self._clock = clock
        #: Used to re-check the *source's* identity at dispatch time. Without
        #: it, a source that regenerated its data after the plan was approved
        #: would still be repaired against, because P03's epoch row stays
        #: ACTIVE until P03 re-bootstraps (INV04-11).
        self._source = source

    # ---- public API ------------------------------------------------------

    def dispatch_plan(self, plan: RepairPlan, *, lease: Lease | None = None) -> PlanDispatchResult:
        """Dispatch every dispatchable action in order."""
        stored = self._repo.get_plan(plan.plan_id)
        if stored is None:
            raise ReconError("plan is not persisted", plan_id=plan.plan_id)

        actions = plan.dispatchable_actions
        if not actions:
            # An investigation-only plan is complete the moment it is recorded:
            # there is nothing to dispatch and nothing to verify.
            self._repo.set_plan_status(plan.plan_id, "VERIFIED", reason_code=None)
            return PlanDispatchResult(plan.plan_id, "VERIFIED", ())

        # ---- guards ------------------------------------------------------
        try:
            if lease is not None:
                self._leases.validate(lease)
            self._approvals.require_dispatchable(
                plan_id=plan.plan_id, expected_plan_hash=plan.plan_hash
            )
            self._assert_preconditions(plan)
        except LeaseLostError as exc:
            self._repo.set_plan_status(plan.plan_id, "STALE", reason_code=ReasonCode.LEASE_LOST)
            return PlanDispatchResult(plan.plan_id, "STALE", (), ReasonCode.LEASE_LOST)
        except PlanStaleError as exc:
            self._repo.set_plan_status(plan.plan_id, "STALE", reason_code=exc.reason)
            return PlanDispatchResult(plan.plan_id, "STALE", (), exc.reason)
        except ApprovalError as exc:
            # Not STALE: the plan may still become dispatchable once a valid
            # approval exists, so it stays where it is.
            self._audit_refusal(plan, exc.reason)
            return PlanDispatchResult(plan.plan_id, str(stored["status"]), (), exc.reason)

        self._repo.set_plan_status(plan.plan_id, "DISPATCHING")

        outcomes: list[DispatchOutcome] = []
        for action in actions:
            outcome = self._dispatch_action(plan, action)
            outcomes.append(outcome)
            self._repo.set_action_status(
                action.action_id, outcome.status, reason_code=outcome.reason_code
            )
            if outcome.status in ("REJECTED", "FAILED", "STALE"):
                break

        plan_status, reason = self._plan_status_from(outcomes)
        self._repo.set_plan_status(plan.plan_id, plan_status, reason_code=reason)
        return PlanDispatchResult(plan.plan_id, plan_status, tuple(outcomes), reason)

    def resolve_unknown(self, *, action_id: str, intent_id: str) -> DispatchOutcome:
        """Resolve an ``UNKNOWN`` by asking P03 about the same intent id.

        Never re-dispatches with a new identity. If P03 has no record at all,
        a retry of the *same* intent id is safe and is reported as such; if it
        does, its receipt is authoritative (section 7.6, INV04-09).
        """
        receipt = self._repair.repair_intent_status(intent_id)
        if receipt is None:
            return DispatchOutcome(
                action_id=action_id,
                intent_id=intent_id,
                status="ABSENT",
                reason_code=None,
                detail="P03 has no record of this intent; retrying the same intent id is safe",
            )
        status = "COMMITTED" if receipt.is_committed else receipt.status
        reason = None if receipt.is_committed else ReasonCode.REPAIR_FAILED
        self._repo.set_action_status(action_id, status, reason_code=reason)
        return DispatchOutcome(
            action_id=action_id,
            intent_id=intent_id,
            status=status,
            reason_code=reason,
            receipt=receipt,
            detail=f"resolved from P03 receipt: {receipt.status}",
        )

    # ---- internals -------------------------------------------------------

    def _assert_preconditions(self, plan: RepairPlan) -> None:
        """Re-verify P03-side preconditions immediately before dispatch."""
        entity = "deals" if plan.expected_deal_w is not None else self._entity_of(plan)
        status = self._read.sync_status(self._settings.source_id, plan.expected_epoch_id, entity)

        if status.epoch_status != "ACTIVE":
            raise PlanStaleError(
                "source epoch is no longer active",
                reason=ReasonCode.EPOCH_MISMATCH,
                epoch_status=status.epoch_status,
            )

        # INV04-11: a source reset must cancel plans for the old epoch, and it
        # must do so *before* P03 notices. P03's epoch row stays ACTIVE until
        # someone re-bootstraps, so checking only the row above would let a
        # repair be dispatched against a dataset that no longer exists.
        self._assert_source_identity_unchanged(plan)
        if (
            plan.expected_lease_version is not None
            and status.lease_version != plan.expected_lease_version
        ):
            raise PlanStaleError(
                "P03 sync lease version moved since the plan was built",
                reason=ReasonCode.PLAN_STALE,
                expected=plan.expected_lease_version,
                actual=status.lease_version,
            )
        if (
            plan.expected_target_scan_id is not None
            and status.published_scan_id is not None
            and status.published_scan_id != plan.expected_target_scan_id
        ):
            # P03 published a newer scan; the evidence this plan rests on
            # describes a projection that no longer exists (IT04-04).
            raise PlanStaleError(
                "P03 published a newer scan than the plan expected",
                reason=ReasonCode.PLAN_STALE,
                expected=plan.expected_target_scan_id,
                actual=status.published_scan_id,
            )
        if plan.expected_deal_w is not None and status.watermark_seq != plan.expected_deal_w:
            raise PlanStaleError(
                "Deal watermark moved since the plan was built",
                reason=ReasonCode.PLAN_STALE,
                expected=plan.expected_deal_w,
                actual=status.watermark_seq,
            )

    def _assert_source_identity_unchanged(self, plan: RepairPlan) -> None:
        """Confirm the source still holds the dataset this plan is about.

        Compares the source's live ``(run_id, generation)`` against the pair
        recorded on ``source_epochs`` for the plan's epoch. A mismatch means
        the source regenerated, so every piece of evidence behind the plan
        describes a world that is gone (section 7.7).

        Skipped when no source port was injected, which keeps older callers
        working; the scheduler always injects one.
        """
        if self._source is None:
            return
        with self._repo.session() as conn:
            row = conn.execute(
                "SELECT source_run_id, source_generation FROM source_epochs WHERE epoch_id = ?",
                (plan.expected_epoch_id,),
            ).fetchone()
        if row is None:
            raise PlanStaleError(
                "the plan's epoch is unknown to P03",
                reason=ReasonCode.EPOCH_MISMATCH,
                epoch_id=plan.expected_epoch_id,
            )
        try:
            identity = self._source.read_identity()
        except SourceError as exc:
            # A source we cannot reach cannot be confirmed unchanged, and an
            # unconfirmed source is not a basis for mutating anything.
            raise PlanStaleError(
                "cannot confirm the source identity before dispatching",
                reason=ReasonCode.SOURCE_TRANSPORT_FAILED,
                detail=str(exc),
            ) from exc

        if (
            identity.run_id != row["source_run_id"]
            or identity.generation != int(row["source_generation"])
        ):
            raise PlanStaleError(
                "the source was regenerated after this plan was built",
                reason=ReasonCode.EPOCH_MISMATCH,
                expected=(row["source_run_id"], int(row["source_generation"])),
                actual=(identity.run_id, identity.generation),
            )

    def _entity_of(self, plan: RepairPlan) -> str:
        for action in plan.actions:
            entity = action.parameters.get("entity")
            if isinstance(entity, str):
                return entity
        return "accounts"

    def _dispatch_action(self, plan: RepairPlan, action: Any) -> DispatchOutcome:
        attempt_id = str(uuid.uuid4())
        # The request id is derived, so a replay of the same attempt cannot
        # create a second attempt row (UNIQUE on request_id).
        request_id = f"{action.intent_id}:{self._attempt_ordinal(action.action_id)}"
        self._repo.create_attempt(
            attempt_id=attempt_id,
            action_id=action.action_id,
            request_id=request_id,
            status="DISPATCHING",
        )
        self._audit(AuditEvent.DISPATCH_STARTED, plan=plan, intent_id=action.intent_id,
                    action_type=action.action_type.value)

        try:
            receipt = self._call_port(plan, action)
        except SourceError as exc:
            if exc.reason is ReasonCode.REPAIR_UNKNOWN:
                # Committed or not -- we genuinely cannot tell from here.
                self._repo.resolve_attempt(
                    attempt_id, status="UNKNOWN", error_code=ReasonCode.REPAIR_UNKNOWN.value
                )
                self._audit(
                    AuditEvent.DISPATCH_UNKNOWN,
                    plan=plan,
                    intent_id=action.intent_id,
                    detail="response lost; outcome must be resolved by receipt query",
                )
                return DispatchOutcome(
                    action_id=action.action_id,
                    intent_id=action.intent_id,
                    status="UNKNOWN",
                    reason_code=ReasonCode.REPAIR_UNKNOWN,
                    detail=str(exc),
                )
            self._repo.resolve_attempt(attempt_id, status="FAILED", error_code=exc.reason.value)
            return DispatchOutcome(
                action_id=action.action_id,
                intent_id=action.intent_id,
                status="FAILED",
                reason_code=exc.reason,
                detail=str(exc),
            )

        self._repo.resolve_attempt(
            attempt_id,
            status=receipt.status,
            receipt_hash=receipt.receipt_hash,
            error_code=receipt.reason_code,
        )
        if receipt.status == "REJECTED":
            return DispatchOutcome(
                action_id=action.action_id,
                intent_id=action.intent_id,
                status="REJECTED",
                reason_code=ReasonCode.REPAIR_REJECTED,
                receipt=receipt,
                detail=receipt.reason_code or "",
            )
        if receipt.status == "FAILED":
            return DispatchOutcome(
                action_id=action.action_id,
                intent_id=action.intent_id,
                status="FAILED",
                reason_code=ReasonCode.REPAIR_FAILED,
                receipt=receipt,
                detail=receipt.reason_code or "",
            )
        return DispatchOutcome(
            action_id=action.action_id,
            intent_id=action.intent_id,
            status=receipt.status,
            receipt=receipt,
        )

    def _attempt_ordinal(self, action_id: str) -> int:
        return len(self._repo.attempts_for(action_id))

    def _call_port(self, plan: RepairPlan, action: Any) -> RepairReceipt:
        if action.action_type in (
            ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
            ActionType.REQUEST_FULL_POSITION_RESYNC,
        ):
            entity = (
                "accounts"
                if action.action_type is ActionType.REQUEST_FULL_ACCOUNT_RESYNC
                else "positions"
            )
            return self._repair.request_full_resync(
                intent_id=action.intent_id,
                source_id=self._settings.source_id,
                entity=entity,
                expected_epoch_id=plan.expected_epoch_id,
                expected_revision=plan.expected_target_scan_id,
                expected_lease_version=plan.expected_lease_version,
                approved_plan_hash=plan.plan_hash,
                parameters=action.parameters,
            )
        if action.action_type is ActionType.REQUEST_DEAL_BACKFILL:
            if not self._settings.allow_deal_backfill:
                raise ReconError(
                    "deal backfill is disabled by policy",
                    reason=ReasonCode.FORBIDDEN_ACTION,
                    intent_id=action.intent_id,
                )
            return self._repair.request_deal_backfill(
                intent_id=action.intent_id,
                source_id=self._settings.source_id,
                expected_epoch_id=plan.expected_epoch_id,
                expected_watermark=plan.expected_deal_w,
                approved_plan_hash=plan.plan_hash,
                parameters=action.parameters,
            )
        raise ReconError(
            "action type is not dispatchable", reason=ReasonCode.FORBIDDEN_ACTION,
            action_type=str(action.action_type),
        )

    def _plan_status_from(
        self, outcomes: list[DispatchOutcome]
    ) -> tuple[str, ReasonCode | None]:
        if any(outcome.is_unknown for outcome in outcomes):
            return "UNKNOWN", ReasonCode.REPAIR_UNKNOWN
        if any(outcome.status == "REJECTED" for outcome in outcomes):
            return "FAILED", ReasonCode.REPAIR_REJECTED
        if any(outcome.status == "FAILED" for outcome in outcomes):
            return "FAILED", ReasonCode.REPAIR_FAILED
        if all(outcome.status == "COMMITTED" for outcome in outcomes):
            # Committed is not verified. An independent audit decides that.
            return "VERIFYING", None
        return "RUNNING", None

    def _audit(self, event: str, *, plan: RepairPlan, **details: Any) -> None:
        with self._repo.session() as conn:
            from .db.engine import transaction as _tx

            with _tx(conn):
                AuditLog(conn, self._clock).append(
                    event,
                    recon_run_id=plan.recon_run_id,
                    plan_id=plan.plan_id,
                    actor_id=self._settings.worker_id,
                    **details,
                )

    def _audit_refusal(self, plan: RepairPlan, reason: ReasonCode) -> None:
        self._audit(AuditEvent.APPROVAL_REFUSED, plan=plan, reason_code=reason.value)
