"""Immutable repair plans and dry-run previews (sections 4.2, 7.5, 6.2).

A plan is a *statement about a specific world*: this epoch, this published
scan, this watermark, this policy, these findings. Three consequences follow,
and all three are implemented here:

* **The plan is content-addressed.** ``plan_hash`` covers the run, the
  expected preconditions, the policy hash and every action's parameters. An
  approval is recorded against that hash, so editing anything about the plan
  after approval invalidates the approval rather than silently inheriting it
  (INV04-06).

* **Intent ids are derived, not random.** ``intent_id =
  SHA256(plan_id + action_order + action_type + parameters_hash)`` exactly as
  section 7.5 specifies. The same action therefore always presents the same
  identity to P03, which is what makes a retry after a lost ACK idempotent
  instead of a second execution (INV04-08).

* **A preview changes nothing.** ``preview`` builds the whole plan, including
  hashes, and persists none of it, so an operator can see precisely what would
  be dispatched before anything exists (FR04-08).

Forbidden actions cannot be smuggled in: the action type is an ``ActionType``
member and the enum has no destructive verb, but ``_assert_permitted`` checks
anyway, because defence in depth is cheap and a future enum addition should
fail loudly here.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .canonical import canonical_json_bytes, content_hash
from .clock import Clock, to_iso
from .config import Settings
from .db.repository import ReconRepository, RunRecord
from .dto import SyncStatus
from .errors import (
    ActionType,
    ForbiddenActionError,
    HIGH_RISK_ACTIONS,
    PolicyViolation,
    ReasonCode,
)
from .policy import PolicyDecision, ProposedAction

#: Actions this service may ever dispatch. Anything outside the set is a bug,
#: and is refused before a plan can be persisted (section 8.1).
PERMITTED_ACTIONS: frozenset[ActionType] = frozenset(
    {
        ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
        ActionType.REQUEST_FULL_POSITION_RESYNC,
        ActionType.REQUEST_DEAL_BACKFILL,
        ActionType.MANUAL_INVESTIGATION,
    }
)


def derive_intent_id(
    *, plan_id: str, action_order: int, action_type: ActionType, parameters_hash: str
) -> str:
    """Section 7.5's intent identity, byte for byte."""
    material = f"{plan_id}|{action_order}|{action_type.value}|{parameters_hash}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PlannedAction:
    action_id: str
    action_order: int
    action_type: ActionType
    intent_id: str
    parameters: Mapping[str, Any]
    parameters_hash: str
    justification: Sequence[str]

    def as_row(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "action_order": self.action_order,
            "action_type": self.action_type.value,
            "intent_id": self.intent_id,
            "parameters": dict(self.parameters),
            "parameters_hash": self.parameters_hash,
            "status": "PLANNED",
        }


@dataclass(frozen=True, slots=True)
class RepairPlan:
    """A complete, content-addressed plan. Immutable by construction."""

    plan_id: str
    recon_run_id: str
    plan_hash: str
    expected_epoch_id: str
    expected_target_scan_id: str | None
    expected_deal_w: int | None
    expected_lease_version: int | None
    policy_hash: str
    evidence_captured_at: str
    status: str
    actions: Sequence[PlannedAction]
    escalations: Sequence[str]
    requires_approval: bool

    @property
    def dispatchable_actions(self) -> tuple[PlannedAction, ...]:
        return tuple(
            action
            for action in self.actions
            if action.action_type is not ActionType.MANUAL_INVESTIGATION
        )

    def preview_text(self) -> str:
        """Human-readable dry run, for ``reconctl plan create --dry-run``."""
        lines = [
            f"plan {self.plan_id}",
            f"  run           {self.recon_run_id}",
            f"  plan_hash     {self.plan_hash}",
            f"  epoch         {self.expected_epoch_id}",
            f"  expect scan   {self.expected_target_scan_id}",
            f"  expect W      {self.expected_deal_w}",
            f"  policy_hash   {self.policy_hash}",
            f"  status        {self.status}",
            f"  approval      {'required' if self.requires_approval else 'not required'}",
        ]
        if self.escalations:
            lines.append(f"  escalations   {', '.join(self.escalations)}")
        for action in self.actions:
            lines.append(
                f"  [{action.action_order}] {action.action_type.value}"
                f" intent={action.intent_id[:16]} because={','.join(action.justification)}"
            )
        if not self.dispatchable_actions:
            lines.append("  (no dispatchable action: this plan only records a human decision)")
        return "\n".join(lines)


class RepairPlanner:
    """Builds plans from a policy decision, and persists them unchanged."""

    def __init__(self, *, repository: ReconRepository, settings: Settings, clock: Clock) -> None:
        self._repo = repository
        self._settings = settings
        self._clock = clock

    # ---- construction ----------------------------------------------------

    def preview(
        self,
        *,
        run: RunRecord,
        decision: PolicyDecision,
        status: SyncStatus,
        findings_digest: str,
        plan_id: str | None = None,
    ) -> RepairPlan:
        """Build a plan without persisting anything (FR04-08)."""
        if not decision.eligible:
            raise PolicyViolation(
                "policy did not find this run eligible for a plan",
                reason=decision.reason_code or ReasonCode.POLICY_BLOCK,
                recon_run_id=run.recon_run_id,
            )

        resolved_plan_id = plan_id or str(uuid.uuid4())
        actions = self._build_actions(resolved_plan_id, decision.actions)

        requires_approval = bool(
            decision.requires_approval
            or self._settings.approval_required
            or any(action.action_type in HIGH_RISK_ACTIONS for action in actions)
        )
        plan_status = "PENDING_APPROVAL" if requires_approval else "APPROVED"

        expected_scan = status.published_scan_id if run.entity != "deals" else None
        expected_w = status.watermark_seq if run.entity == "deals" else None

        plan_hash = self._plan_hash(
            run=run,
            actions=actions,
            expected_epoch_id=run.source_epoch_id,
            expected_target_scan_id=expected_scan,
            expected_deal_w=expected_w,
            expected_lease_version=status.lease_version,
            findings_digest=findings_digest,
        )

        return RepairPlan(
            plan_id=resolved_plan_id,
            recon_run_id=run.recon_run_id,
            plan_hash=plan_hash,
            expected_epoch_id=run.source_epoch_id,
            expected_target_scan_id=expected_scan,
            expected_deal_w=expected_w,
            expected_lease_version=status.lease_version,
            policy_hash=self._settings.policy_hash,
            evidence_captured_at=to_iso(self._clock.now()),
            status=plan_status,
            actions=actions,
            escalations=tuple(decision.escalations),
            requires_approval=requires_approval,
        )

    def create(
        self,
        *,
        run: RunRecord,
        decision: PolicyDecision,
        status: SyncStatus,
        findings_digest: str,
        created_by: str,
    ) -> RepairPlan:
        """Build and persist the plan in its final, immutable form."""
        plan = self.preview(
            run=run, decision=decision, status=status, findings_digest=findings_digest
        )
        self._repo.insert_plan(
            plan_id=plan.plan_id,
            run_id=plan.recon_run_id,
            plan_hash=plan.plan_hash,
            expected_epoch_id=plan.expected_epoch_id,
            expected_target_scan_id=plan.expected_target_scan_id,
            expected_deal_w=plan.expected_deal_w,
            expected_lease_version=plan.expected_lease_version,
            evidence_captured_at=plan.evidence_captured_at,
            status=plan.status,
            created_by=created_by,
            actions=[action.as_row() for action in plan.actions],
        )
        return plan

    # ---- internals -------------------------------------------------------

    def _build_actions(
        self, plan_id: str, proposed: Sequence[ProposedAction]
    ) -> tuple[PlannedAction, ...]:
        actions: list[PlannedAction] = []
        for order, item in enumerate(proposed):
            self._assert_permitted(item.action_type)
            parameters_hash = hashlib.sha256(canonical_json_bytes(dict(item.parameters))).hexdigest()
            actions.append(
                PlannedAction(
                    action_id=str(uuid.uuid4()),
                    action_order=order,
                    action_type=item.action_type,
                    intent_id=derive_intent_id(
                        plan_id=plan_id,
                        action_order=order,
                        action_type=item.action_type,
                        parameters_hash=parameters_hash,
                    ),
                    parameters=dict(item.parameters),
                    parameters_hash=parameters_hash,
                    justification=tuple(item.justification),
                )
            )
        return tuple(actions)

    def _assert_permitted(self, action_type: ActionType) -> None:
        if action_type not in PERMITTED_ACTIONS:
            raise ForbiddenActionError(
                "action type is not on the permitted allowlist",
                action_type=str(action_type),
            )

    def _plan_hash(
        self,
        *,
        run: RunRecord,
        actions: Sequence[PlannedAction],
        expected_epoch_id: str,
        expected_target_scan_id: str | None,
        expected_deal_w: int | None,
        expected_lease_version: int | None,
        findings_digest: str,
    ) -> str:
        """Everything that makes this plan *this* plan.

        ``plan_id`` is included because intent ids derive from it, so two
        plans with identical content still dispatch distinguishable intents.
        ``findings_digest`` is included so a plan cannot be approved and then
        re-pointed at a different set of findings.
        """
        return content_hash(
            {
                "plan_id": actions[0].intent_id if actions else None,
                "recon_run_id": run.recon_run_id,
                "source_id": run.source_id,
                "entity": run.entity,
                "scope": run.scope,
                "expected_epoch_id": expected_epoch_id,
                "expected_target_scan_id": expected_target_scan_id,
                "expected_deal_w": expected_deal_w,
                "expected_lease_version": expected_lease_version,
                "policy_hash": self._settings.policy_hash,
                "schema_version": run.schema_version,
                "hash_version": run.hash_version,
                "source_root_hash": run.source_root_hash,
                "target_root_hash": run.target_root_hash,
                "findings_digest": findings_digest,
                "actions": [
                    {
                        "order": action.action_order,
                        "type": action.action_type.value,
                        "parameters_hash": action.parameters_hash,
                        "intent_id": action.intent_id,
                    }
                    for action in actions
                ],
            }
        )


def findings_digest(findings: Sequence[Mapping[str, Any]]) -> str:
    """Order-independent digest of the findings a plan answers."""
    return content_hash(
        sorted(
            [
                [str(f["finding_code"]), f.get("item_key"), str(f["evidence_hash"])]
                for f in findings
            ]
        )
    )
