"""Approval, RBAC and separation of duties (sections 8.1, 12.4, FR04-09).

Four rules, each of which exists because of a specific way approval gates fail
in practice:

* **Approve the hash, not the plan id.** An approval records the
  ``plan_hash`` it saw. If the plan changes, the recorded hash no longer
  matches and the approval is void (``PLAN_DIGEST_CHANGED``). Approving "plan
  7" and then letting plan 7's contents change is the classic bypass
  (INV04-06).

* **The approver must not be the author.** ``require_distinct_approver``
  rejects self-approval by actor identity, because a single compromised or
  mistaken actor should not be able to both propose and authorise a change to
  financial data (section 12.4).

* **Approvals expire.** A plan has a TTL and its evidence has a maximum age.
  An approval that outlives either is refused at dispatch, not honoured
  (section 8.1 "evidence age > 5 minutes").

* **A rejection is final for that plan.** It cannot be overridden by a later
  approval on the same plan; a new plan with fresh evidence is required.

``verify_for_dispatch`` is the single gate ``executor.py`` must pass through,
so there is exactly one place where "may this be dispatched?" is decided.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from .clock import Clock, from_iso
from .config import Settings
from .db.repository import ReconRepository
from .errors import ApprovalError, ReasonCode


class Role(StrEnum):
    """Least-privilege roles (section 12.4)."""

    #: May trigger audits and read evidence. Cannot approve.
    AUDITOR = "AUDITOR"
    #: May approve a repair plan. Cannot author one.
    APPROVER = "APPROVER"
    #: Break-glass: may approve and dispatch. Recorded loudly.
    INCIDENT_COMMANDER = "INCIDENT_COMMANDER"
    #: Automation identity. Never allowed to approve anything.
    SERVICE = "SERVICE"


#: Roles whose approval counts. ``SERVICE`` is absent on purpose: a service
#: account approving its own proposals is not separation of duties.
APPROVING_ROLES: frozenset[Role] = frozenset({Role.APPROVER, Role.INCIDENT_COMMANDER})


@dataclass(frozen=True, slots=True)
class ApprovalOutcome:
    allowed: bool
    reason_code: ReasonCode | None
    detail: str = ""


class ApprovalService:
    """Records decisions and answers whether a plan may be dispatched."""

    def __init__(self, *, repository: ReconRepository, settings: Settings, clock: Clock) -> None:
        self._repo = repository
        self._settings = settings
        self._clock = clock

    # ---- recording -------------------------------------------------------

    def decide(
        self,
        *,
        plan_id: str,
        actor_id: str,
        role: Role | str,
        decision: str,
        plan_hash: str,
    ) -> ApprovalOutcome:
        """Record an approve/reject decision, refusing invalid ones.

        The actor's role and the decision verb are validated *before* the plan
        is looked up, so an unrecognised role is always reported as such. If
        the plan lookup came first, a bad role aimed at a non-existent plan
        would be reported as "unknown plan", hiding the more important fact
        that the caller is not a recognised principal.
        """
        if decision not in ("APPROVE", "REJECT"):
            return ApprovalOutcome(False, ReasonCode.APPROVAL_ROLE_INVALID, "unknown decision")
        try:
            resolved_role = role if isinstance(role, Role) else Role(role)
        except ValueError:
            return ApprovalOutcome(
                False, ReasonCode.APPROVAL_ROLE_INVALID, f"unrecognised role {role!r}"
            )

        plan = self._repo.get_plan(plan_id)
        if plan is None:
            return ApprovalOutcome(False, ReasonCode.APPROVAL_MISSING, "unknown plan")

        if decision == "APPROVE":
            if resolved_role not in APPROVING_ROLES:
                return ApprovalOutcome(
                    False,
                    ReasonCode.APPROVAL_ROLE_INVALID,
                    f"role {resolved_role} may not approve",
                )
            if plan["plan_hash"] != plan_hash:
                # The approver is looking at a different plan than the one
                # stored. Refuse rather than approve the stored one.
                return ApprovalOutcome(
                    False,
                    ReasonCode.PLAN_DIGEST_CHANGED,
                    "submitted plan hash does not match the stored plan",
                )
            if self._settings.require_distinct_approver and plan["created_by"] == actor_id:
                return ApprovalOutcome(
                    False, ReasonCode.APPROVAL_SELF, "author may not approve their own plan"
                )
            if plan["status"] == "REJECTED":
                return ApprovalOutcome(
                    False, ReasonCode.POLICY_BLOCK, "a rejected plan cannot be approved"
                )
            if plan["status"] not in ("PENDING_APPROVAL", "DRAFT"):
                return ApprovalOutcome(
                    False,
                    ReasonCode.POLICY_BLOCK,
                    f"plan in status {plan['status']} is not awaiting approval",
                )
            if self._clock.now() > from_iso(plan["expires_at"]):
                return ApprovalOutcome(
                    False, ReasonCode.APPROVAL_EXPIRED, "plan expired before approval"
                )

        self._repo.record_approval(
            approval_id=str(uuid.uuid4()),
            plan_id=plan_id,
            actor_id=actor_id,
            role=resolved_role.value,
            decision=decision,
            plan_hash=plan_hash,
        )
        self._repo.set_plan_status(
            plan_id,
            "APPROVED" if decision == "APPROVE" else "REJECTED",
            reason_code=None if decision == "APPROVE" else ReasonCode.POLICY_BLOCK,
            actor_id=actor_id,
        )
        return ApprovalOutcome(True, None, f"{decision} recorded by {actor_id}")

    # ---- dispatch gate ---------------------------------------------------

    def verify_for_dispatch(self, *, plan_id: str, expected_plan_hash: str) -> ApprovalOutcome:
        """The only gate ``executor.py`` consults before dispatching.

        Re-checks everything at dispatch time rather than trusting that it was
        true at approval time: section 7.5 requires the preconditions to still
        hold *now*, because approval is not application (section 4.2).
        """
        plan = self._repo.get_plan(plan_id)
        if plan is None:
            return ApprovalOutcome(False, ReasonCode.APPROVAL_MISSING, "unknown plan")

        if plan["plan_hash"] != expected_plan_hash:
            return ApprovalOutcome(
                False, ReasonCode.PLAN_DIGEST_CHANGED, "plan content changed since approval"
            )

        now = self._clock.now()
        if now > from_iso(plan["expires_at"]):
            return ApprovalOutcome(False, ReasonCode.APPROVAL_EXPIRED, "plan TTL elapsed")

        evidence_age = (now - from_iso(plan["evidence_captured_at"])).total_seconds()
        if evidence_age > self._settings.evidence_max_age_s:
            return ApprovalOutcome(
                False,
                ReasonCode.EVIDENCE_STALE,
                f"evidence is {evidence_age:.0f}s old, cap is {self._settings.evidence_max_age_s}s",
            )

        if plan["status"] == "REJECTED":
            return ApprovalOutcome(False, ReasonCode.POLICY_BLOCK, "plan was rejected")

        if not self._settings.approval_required:
            # Only reachable in a test configuration; prod forbids it in
            # ``config.py``. Recorded here so the relaxation is visible.
            return ApprovalOutcome(True, None, "approval not required by configuration")

        approvals = self._repo.approvals_for(plan_id)
        if any(row["decision"] == "REJECT" for row in approvals):
            return ApprovalOutcome(False, ReasonCode.POLICY_BLOCK, "plan has a rejection on record")

        valid = [
            row
            for row in approvals
            if row["decision"] == "APPROVE"
            and row["plan_hash"] == expected_plan_hash
            and Role(row["role"]) in APPROVING_ROLES
            and not (self._settings.require_distinct_approver and row["actor_id"] == plan["created_by"])
        ]
        if not valid:
            return ApprovalOutcome(
                False, ReasonCode.APPROVAL_MISSING, "no valid approval for this plan hash"
            )
        if plan["status"] not in ("APPROVED", "DISPATCHING", "UNKNOWN"):
            return ApprovalOutcome(
                False, ReasonCode.POLICY_BLOCK, f"plan status {plan['status']} is not dispatchable"
            )
        return ApprovalOutcome(True, None, f"approved by {valid[0]['actor_id']}")

    def require_dispatchable(self, *, plan_id: str, expected_plan_hash: str) -> None:
        """Raise ``ApprovalError`` unless the plan may be dispatched."""
        outcome = self.verify_for_dispatch(plan_id=plan_id, expected_plan_hash=expected_plan_hash)
        if not outcome.allowed:
            raise ApprovalError(
                outcome.detail or "plan is not dispatchable",
                reason=outcome.reason_code or ReasonCode.APPROVAL_MISSING,
                plan_id=plan_id,
            )

    def approvals(self, plan_id: str) -> Sequence[object]:
        return self._repo.approvals_for(plan_id)
