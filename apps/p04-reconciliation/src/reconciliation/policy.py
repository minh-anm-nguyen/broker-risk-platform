"""Repair eligibility and safety guardrails (SYSTEM_DESIGN section 8.1).

This module answers one question: given proven findings, what is this service
*allowed* to propose? The defaults are the design's defaults, and they are
conservative on purpose:

| Situation | Allowed |
|---|---|
| Direct write to a P03 table | never -- not even representable (``ActionType``) |
| Deal money conflict | investigate only; no value is authoritative |
| Extra deal in target | investigate only; never a delete |
| Missing deal at a proven cut | bounded backfill, and only when enabled (P1) |
| Account/Position field mismatch at equal cut | full P03 re-sync |
| Absent from source listing | investigate only; absence is not a close |
| Mass drift beyond the blast-radius cap | escalate to a human |
| Any ``INCONCLUSIVE`` evidence | nothing |

Two properties matter more than the table itself:

* **Nothing proposable can corrupt money.** The action allowlist contains no
  verb that edits a value; the strongest thing a plan can say is "P03, please
  re-derive this from the source". That is what makes section 8.1's "automatic
  write patch: never" structurally true rather than a review rule.

* **Insufficient evidence yields an empty plan, not a smaller one.** If any
  finding in the run is ``INCONCLUSIVE``, or the alignment gate blocked strict
  comparison, the whole run is ineligible. Repairing the subset we happen to
  be sure about, while the run as a whole is untrustworthy, is how a partial
  manifest turns into a destructive action (IT04-05).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .config import Settings
from .db.repository import RunRecord
from .diff_engine import ClassificationResult
from .errors import ActionType, Confidence, FindingCode, ReasonCode, Severity

#: The strongest action each finding may justify (section 4.3 "possible safe
#: recovery"). ``MANUAL_INVESTIGATION`` means "record it, tell a human, change
#: nothing".
ACTION_FOR_FINDING: Mapping[FindingCode, ActionType] = {
    FindingCode.MISSING_TARGET_DEAL: ActionType.REQUEST_DEAL_BACKFILL,
    FindingCode.EXTRA_TARGET_DEAL: ActionType.MANUAL_INVESTIGATION,
    FindingCode.IMMUTABLE_DEAL_CONFLICT: ActionType.MANUAL_INVESTIGATION,
    FindingCode.ACCOUNT_FIELD_MISMATCH: ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
    FindingCode.MISSING_TARGET_ACCOUNT: ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
    FindingCode.POSITION_FIELD_MISMATCH: ActionType.REQUEST_FULL_POSITION_RESYNC,
    FindingCode.MISSING_TARGET_POSITION: ActionType.REQUEST_FULL_POSITION_RESYNC,
    FindingCode.ABSENT_AT_SOURCE_ACCOUNT: ActionType.MANUAL_INVESTIGATION,
    FindingCode.ABSENT_AT_SOURCE_POSITION: ActionType.MANUAL_INVESTIGATION,
    FindingCode.DIFFERENT_CUT: ActionType.MANUAL_INVESTIGATION,
    FindingCode.PROVISIONAL_CROSS_ENTITY: ActionType.MANUAL_INVESTIGATION,
    FindingCode.EPOCH_MISMATCH: ActionType.MANUAL_INVESTIGATION,
    FindingCode.INCONCLUSIVE_SOURCE: ActionType.MANUAL_INVESTIGATION,
    FindingCode.LARGE_SOURCE_COUNT_DROP: ActionType.MANUAL_INVESTIGATION,
}

#: Findings that block the whole plan from containing any orchestration
#: action, because the right next step is a human looking at the source
#: (section 8.1 "source epoch reset: block until re-bootstrap").
BLOCKING_FINDINGS: frozenset[FindingCode] = frozenset(
    {
        FindingCode.EPOCH_MISMATCH,
        FindingCode.IMMUTABLE_DEAL_CONFLICT,
        FindingCode.EXTRA_TARGET_DEAL,
        FindingCode.LARGE_SOURCE_COUNT_DROP,
    }
)


@dataclass(frozen=True, slots=True)
class ProposedAction:
    """One action a plan may contain, before it becomes immutable."""

    action_type: ActionType
    parameters: Mapping[str, Any]
    justification: Sequence[str]


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """What policy permits for this run."""

    eligible: bool
    reason_code: ReasonCode | None
    actions: Sequence[ProposedAction]
    requires_approval: bool
    escalations: Sequence[str] = ()

    @property
    def is_dispatchable(self) -> bool:
        return self.eligible and any(
            action.action_type not in (ActionType.MANUAL_INVESTIGATION,) for action in self.actions
        )


class RepairPolicy:
    """Applies section 8.1's guardrails to a classified run."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, *, run: RunRecord, result: ClassificationResult) -> PolicyDecision:
        findings = list(result.findings)
        if not findings:
            return PolicyDecision(
                eligible=False, reason_code=None, actions=(), requires_approval=False
            )

        codes = {FindingCode(f["finding_code"]) for f in findings}
        escalations: list[str] = []

        # ---- hard blocks -------------------------------------------------
        if any(f["confidence"] == Confidence.INCONCLUSIVE.value for f in findings):
            return PolicyDecision(
                eligible=False,
                reason_code=ReasonCode.POLICY_BLOCK,
                actions=self._investigation_only(codes),
                requires_approval=False,
                escalations=["INCONCLUSIVE_EVIDENCE_PRESENT"],
            )

        if not run.source_complete or not run.target_complete:
            # Belt and braces: the alignment gate already covers this, but a
            # partial manifest must never reach a plan through any path
            # (section 8.1 "source complete=false: no diff, no repair").
            return PolicyDecision(
                eligible=False,
                reason_code=ReasonCode.SOURCE_INCOMPLETE,
                actions=(),
                requires_approval=False,
                escalations=["INCOMPLETE_MANIFEST"],
            )

        blocking = codes & BLOCKING_FINDINGS
        if blocking:
            return PolicyDecision(
                eligible=True,
                reason_code=ReasonCode.POLICY_BLOCK,
                actions=self._investigation_only(codes),
                requires_approval=True,
                escalations=sorted(code.value for code in blocking),
            )

        if result.mass_drift:
            escalations.append("MASS_DRIFT_EXCEEDS_CAP")
            return PolicyDecision(
                eligible=True,
                reason_code=ReasonCode.MASS_DRIFT_ESCALATION,
                actions=self._investigation_only(codes),
                requires_approval=True,
                escalations=escalations,
            )

        # ---- build the strongest permitted action set --------------------
        actions: list[ProposedAction] = []
        wanted: dict[ActionType, list[str]] = {}
        for code in sorted(codes, key=lambda c: c.value):
            action_type = ACTION_FOR_FINDING.get(code, ActionType.MANUAL_INVESTIGATION)
            if action_type is ActionType.REQUEST_DEAL_BACKFILL and not self._settings.allow_deal_backfill:
                # Section 8.1 keeps historical Deal backfill behind an
                # explicit switch and a human; downgrade rather than refuse,
                # so the finding is still recorded and visible.
                action_type = ActionType.MANUAL_INVESTIGATION
                escalations.append("DEAL_BACKFILL_DISABLED_BY_POLICY")
            wanted.setdefault(action_type, []).append(code.value)

        for action_type, justification in sorted(wanted.items(), key=lambda kv: kv[0].value):
            actions.append(
                ProposedAction(
                    action_type=action_type,
                    parameters=self._parameters_for(action_type, run),
                    justification=tuple(sorted(justification)),
                )
            )

        requires_approval = self._settings.approval_required or any(
            action.action_type is not ActionType.MANUAL_INVESTIGATION for action in actions
        )
        return PolicyDecision(
            eligible=True,
            reason_code=None,
            actions=tuple(actions),
            requires_approval=requires_approval,
            escalations=tuple(escalations),
        )

    # ---- internals -------------------------------------------------------

    def _investigation_only(self, codes: set[FindingCode]) -> tuple[ProposedAction, ...]:
        return (
            ProposedAction(
                action_type=ActionType.MANUAL_INVESTIGATION,
                parameters={"reason": "POLICY_REQUIRES_HUMAN_REVIEW"},
                justification=tuple(sorted(code.value for code in codes)),
            ),
        )

    def _parameters_for(self, action_type: ActionType, run: RunRecord) -> Mapping[str, Any]:
        """Parameters are provenance, never values.

        An action says *which* epoch and scope to re-derive, never what the
        result should be. That is the whole difference between orchestrating a
        sync and patching a row (section 0.2 decision 3).
        """
        base: dict[str, Any] = {
            "source_id": run.source_id,
            "epoch_id": run.source_epoch_id,
            "scope": "UNFILTERED",
        }
        if action_type is ActionType.REQUEST_FULL_ACCOUNT_RESYNC:
            base["entity"] = "accounts"
        elif action_type is ActionType.REQUEST_FULL_POSITION_RESYNC:
            base["entity"] = "positions"
        elif action_type is ActionType.REQUEST_DEAL_BACKFILL:
            base["entity"] = "deals"
            base["upper_bound_seq"] = run.source_snapshot_seq
            base["expected_watermark"] = run.target_deal_watermark
        else:
            base["entity"] = run.entity
        return base

    # ---- dispatch-time guards -------------------------------------------

    def check_evidence_freshness(self, *, evidence_age_s: float) -> ReasonCode | None:
        """Section 8.1: evidence older than the cap makes a plan ``STALE``.

        A plan is a statement about a world that existed when the evidence was
        captured. Applying it to a world that has since moved is how a correct
        decision becomes a wrong action.
        """
        if evidence_age_s > self._settings.evidence_max_age_s:
            return ReasonCode.EVIDENCE_STALE
        return None

    def highest_severity_requires_human(self, severity: Severity | None) -> bool:
        return severity in (Severity.CRITICAL,)
