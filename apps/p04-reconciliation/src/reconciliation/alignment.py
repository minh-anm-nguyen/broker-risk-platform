"""Comparison-precondition gate (SYSTEM_DESIGN sections 5.1, 5.2, 5.3, 2.3).

Every precondition in section 5.1 is evaluated here, and the result decides
whether the run may emit ``PROVEN`` findings at all:

```text
same source_id && same source_epoch_id
&& complete_unfiltered_source_scan
&& source_run_generation_consistent
&& supported_schema_and_hash_version
&& consistent_target_capture
&& compatible_snapshot_cut
&& compare_scope_is_identical
&& no revoked provenance / tampered evidence
```

The subtle rule -- and the one that causes real production incidents -- is
what "compatible cut" means per entity:

* **Deals are immutable within an epoch.** Compatibility is therefore
  ``W >= S``: once P03's durable watermark has passed the source snapshot, the
  set of target deals with ``source_seq <= S`` is comparable with the source
  set at ``S``. The two snapshot numbers do *not* need to be equal.

* **Accounts and Positions are mutable latest-state projections.** Here
  compatibility requires the *same* cut. If the source was read at S=1501 and
  P03 published at S=1500, a row that differs is almost certainly a legitimate
  change in between, not corruption. Section 5.3 exists entirely to stop that
  false positive, and section 8.1 forbids auto-repair from such a run.

A failed precondition yields ``INCONCLUSIVE``/``DIFFERENT_CUT``, never
``MATCHED`` and never a repair (section 5.1 "fail-closed").
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .canonical import HASH_VERSION, SCHEMA_VERSION
from .config import Settings
from .db.repository import RunRecord
from .dto import AlignmentDecision, SourceCapabilities, SourceIdentity, SyncStatus
from .errors import Confidence, ReasonCode

#: Entities whose rows may change under us, so strict compare needs an
#: identical cut (section 5.3).
MUTABLE_ENTITIES: frozenset[str] = frozenset({"accounts", "positions"})


@dataclass(frozen=True, slots=True)
class AlignmentInputs:
    """Everything the gate needs, gathered before it is asked to decide."""

    run: RunRecord
    identity: SourceIdentity
    capabilities: SourceCapabilities
    status: SyncStatus
    expected_epoch_id: str
    expected_source_id: str
    evidence_tampered: bool = False


class AlignmentEngine:
    """Decides whether a strict comparison is permitted, and says why not."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def decide(self, inputs: AlignmentInputs) -> AlignmentDecision:
        run = inputs.run
        checks: dict[str, bool] = {}
        detail: dict[str, Any] = {}

        # ---- identity and epoch -----------------------------------------
        checks["same_source_id"] = (
            run.source_id == inputs.expected_source_id == inputs.identity.source_id
        )
        checks["same_source_epoch_id"] = (
            run.source_epoch_id == inputs.expected_epoch_id == inputs.status.epoch_id
        )
        checks["source_epoch_active"] = inputs.status.epoch_status == "ACTIVE"
        checks["source_run_generation_consistent"] = (
            run.source_run_id == inputs.identity.run_id
            and run.source_generation == inputs.identity.generation
        )
        detail["source_identity"] = {
            "run_id": inputs.identity.run_id,
            "generation": inputs.identity.generation,
        }

        # ---- evidence completeness --------------------------------------
        checks["complete_unfiltered_source_scan"] = bool(run.source_complete)
        checks["compare_scope_is_identical"] = (
            run.scope == "UNFILTERED" and inputs.status.filter_hash in (None, "UNFILTERED")
        )
        checks["consistent_target_capture"] = bool(run.target_complete)

        # ---- schema and hash versions -----------------------------------
        checks["supported_schema_and_hash_version"] = (
            run.schema_version == SCHEMA_VERSION and run.hash_version == HASH_VERSION
        )
        checks["policy_unchanged"] = run.policy_hash == self._settings.policy_hash

        # ---- provenance --------------------------------------------------
        checks["evidence_untampered"] = not inputs.evidence_tampered

        # ---- capability --------------------------------------------------
        # Section 2.3: without stable snapshot pagination and durable epoch
        # identity there is no full-set compare to be had, only observations.
        checks["source_supports_full_set_compare"] = (
            inputs.capabilities.stable_snapshot_pagination
            and inputs.capabilities.durable_epoch_identity
            and inputs.capabilities.full_unfiltered_listing
        )
        checks["identical_financial_semantics"] = inputs.capabilities.identical_financial_semantics

        # ---- cut compatibility ------------------------------------------
        cut_ok, cut_reason, cut_detail = self._check_cut(run, inputs)
        checks["compatible_snapshot_cut"] = cut_ok
        detail["cut"] = cut_detail

        reason = self._first_failure(checks, cut_reason)
        allowed = reason is None
        return AlignmentDecision(
            strict_compare_allowed=allowed,
            reason_code=reason,
            confidence=Confidence.PROVEN.value if allowed else Confidence.INCONCLUSIVE.value,
            checks=checks,
            detail=detail,
        )

    # ---- internals -------------------------------------------------------

    def _check_cut(
        self, run: RunRecord, inputs: AlignmentInputs
    ) -> tuple[bool, ReasonCode | None, dict[str, Any]]:
        source_s = run.source_snapshot_seq
        detail: dict[str, Any] = {
            "entity": run.entity,
            "source_snapshot_seq": source_s,
            "target_snapshot_seq": run.target_snapshot_seq,
            "target_deal_watermark": run.target_deal_watermark,
        }
        if source_s is None:
            return False, ReasonCode.SOURCE_INCOMPLETE, detail

        if run.entity == "deals":
            watermark = run.target_deal_watermark
            if watermark is None:
                return False, ReasonCode.TARGET_NO_PUBLISHED_SCAN, detail
            if watermark < source_s:
                return False, ReasonCode.TARGET_NOT_CAUGHT_UP, detail
            detail["rule"] = "immutable_deal_history_requires_W_ge_S"
            return True, None, detail

        # Mutable projection: the cuts must be the same one.
        target_s = run.target_snapshot_seq
        if target_s is None:
            return False, ReasonCode.TARGET_NO_PUBLISHED_SCAN, detail
        detail["rule"] = "mutable_projection_requires_equal_S"
        if target_s != source_s:
            return False, ReasonCode.DIFFERENT_CUT, detail
        return True, None, detail

    def _first_failure(
        self, checks: dict[str, bool], cut_reason: ReasonCode | None
    ) -> ReasonCode | None:
        """Map the first failed precondition to its reason code.

        Ordered most-fundamental first so the reported reason is the root
        cause: an epoch mismatch explains everything downstream of it, and
        reporting ``DIFFERENT_CUT`` in that situation would send an operator
        to the wrong runbook.
        """
        if not checks["same_source_id"]:
            return ReasonCode.SOURCE_IDENTITY_CHANGED
        if not checks["same_source_epoch_id"] or not checks["source_run_generation_consistent"]:
            return ReasonCode.EPOCH_MISMATCH
        if not checks["source_epoch_active"]:
            return ReasonCode.EPOCH_MISMATCH
        if not checks["supported_schema_and_hash_version"]:
            return ReasonCode.INCOMPARABLE_SCHEMA
        if not checks["identical_financial_semantics"]:
            return ReasonCode.INCOMPARABLE_SCHEMA
        if not checks["source_supports_full_set_compare"]:
            return ReasonCode.UNSUPPORTED_CAPABILITY
        if not checks["compare_scope_is_identical"]:
            return ReasonCode.SOURCE_SCAN_FILTERED
        if not checks["complete_unfiltered_source_scan"]:
            return ReasonCode.SOURCE_INCOMPLETE
        if not checks["consistent_target_capture"]:
            return ReasonCode.TARGET_INCOMPLETE
        if not checks["evidence_untampered"]:
            return ReasonCode.POLICY_BLOCK
        if not checks["policy_unchanged"]:
            return ReasonCode.EVIDENCE_STALE
        if cut_reason is not None:
            return cut_reason
        return None


def cross_entity_tier(capabilities: SourceCapabilities, configured: str) -> str:
    """Resolve the cross-entity evidence tier actually available (section 5.4).

    Configuration cannot grant a tier the source does not support: asking for
    ``T2`` without a tested coordinated global cut returns ``T1``. Section 0.4
    forbids claiming a global Account+Position+Deal snapshot when upstream
    provides three independent ones, and a config flag is not a contract.
    """
    if configured == "T2" and capabilities.global_cut_across_entities:
        return "T2"
    if configured == "T2":
        return "T1"
    return configured
