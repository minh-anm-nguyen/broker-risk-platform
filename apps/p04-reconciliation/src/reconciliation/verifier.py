"""Independent post-repair verification (sections 7.5, 4.2, INV04-12).

The rule this module exists to enforce: **a receipt is not verification.**

Section 4.2 states the chain plainly -- ``APPROVED`` is not ``APPLIED``,
``ACCEPTED`` is not ``COMMITTED``, and ``COMMITTED`` is not ``VERIFIED``. A
200 response, or even a durable P03 receipt saying the sync committed, says
only that P03 believes it did something. It says nothing about whether the
target now agrees with the source.

So verification here means exactly one thing: run a **brand new audit** --
fresh source capture, fresh target capture, fresh alignment -- and mark the
plan ``VERIFIED`` only if that independent run comes back ``MATCHED`` at a
compatible cut. Anything else (findings remain, cut incompatible, source
inconclusive) leaves the plan ``FAILED`` or re-queued, never ``VERIFIED``.

The verification run is a first-class ``recon_runs`` row with
``verifies_plan_id`` set, so the evidence trail shows which audit justified
the word "verified" (INV04-16).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol

from .audit import AuditEvent, AuditLog
from .clock import Clock
from .config import Settings
from .db.engine import transaction
from .db.repository import ReconRepository
from .errors import ReasonCode
from .planning import RepairPlan


class AuditOutcomeLike(Protocol):
    """The shape of an audit result the verifier needs to judge."""

    status: str
    reason_code: ReasonCode | None
    recon_run_id: str
    finding_count: int


class AuditRunner(Protocol):
    """Runs a complete independent audit.

    Injected rather than imported so the verifier does not depend on the
    scheduler that drives it.
    """

    def __call__(
        self, *, entity: str, epoch_id: str, request_id: str, verifies_plan_id: str | None = None
    ) -> AuditOutcomeLike: ...


@dataclass(frozen=True, slots=True)
class VerificationResult:
    verified: bool
    plan_status: str
    reason_code: ReasonCode | None
    verification_run_id: str | None
    detail: str = ""


class PostRepairVerifier:
    """Decides whether a dispatched plan may be called ``VERIFIED``."""

    def __init__(
        self,
        *,
        repository: ReconRepository,
        audit_runner: AuditRunner,
        settings: Settings,
        clock: Clock,
    ) -> None:
        self._repo = repository
        self._run_audit = audit_runner
        self._settings = settings
        self._clock = clock

    def verify(self, plan: RepairPlan, *, entity: str | None = None) -> VerificationResult:
        """Run a fresh audit and decide.

        ``entity`` defaults to the entity the plan's actions targeted, because
        verifying a position re-sync by auditing accounts would prove nothing.
        """
        stored = self._repo.get_plan(plan.plan_id)
        if stored is None:
            return VerificationResult(
                False, "FAILED", ReasonCode.INTERNAL_ERROR, None, "plan is not persisted"
            )

        if stored["status"] not in ("VERIFYING", "RUNNING", "UNKNOWN"):
            # Verification only applies to a plan P03 reported as committed.
            return VerificationResult(
                False,
                str(stored["status"]),
                ReasonCode.POLICY_BLOCK,
                None,
                f"plan status {stored['status']} is not awaiting verification",
            )

        target_entity = entity or self._entity_of(plan)
        self._audit(plan, AuditEvent.VERIFICATION_STARTED, entity=target_entity)

        outcome = self._run_audit(
            entity=target_entity,
            epoch_id=plan.expected_epoch_id,
            request_id=f"verify:{plan.plan_id}:{uuid.uuid4()}",
            verifies_plan_id=plan.plan_id,
        )

        if outcome.status == "MATCHED":
            self._repo.set_plan_status(plan.plan_id, "VERIFIED")
            self._audit(
                plan,
                AuditEvent.VERIFICATION_RESULT,
                entity=target_entity,
                result="VERIFIED",
                verification_run_id=outcome.recon_run_id,
            )
            return VerificationResult(
                True,
                "VERIFIED",
                None,
                outcome.recon_run_id,
                "independent audit matched at a compatible cut",
            )

        # Every other outcome is a refusal to claim success. An INCONCLUSIVE
        # verification is explicitly *not* a pass: we could not prove the
        # repair worked, so the plan does not get to say it did.
        reason = outcome.reason_code or ReasonCode.VERIFICATION_FAILED
        self._repo.set_plan_status(plan.plan_id, "FAILED", reason_code=reason)
        self._audit(
            plan,
            AuditEvent.VERIFICATION_RESULT,
            entity=target_entity,
            result=outcome.status,
            reason_code=reason.value,
            verification_run_id=outcome.recon_run_id,
            finding_count=outcome.finding_count,
        )
        return VerificationResult(
            False,
            "FAILED",
            reason,
            outcome.recon_run_id,
            f"independent audit returned {outcome.status}",
        )

    # ---- internals -------------------------------------------------------

    def _entity_of(self, plan: RepairPlan) -> str:
        for action in plan.actions:
            entity = action.parameters.get("entity")
            if isinstance(entity, str):
                return entity
        return "deals" if plan.expected_deal_w is not None else "accounts"

    def _audit(self, plan: RepairPlan, event: str, **details: object) -> None:
        with self._repo.session() as conn, transaction(conn):
            AuditLog(conn, self._clock).append(
                event,
                recon_run_id=plan.recon_run_id,
                plan_id=plan.plan_id,
                actor_id=self._settings.worker_id,
                **details,
            )
