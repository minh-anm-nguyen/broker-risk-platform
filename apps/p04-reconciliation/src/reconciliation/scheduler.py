"""Run orchestration: the full reconciliation sequence (section 7.1).

This module is the executable form of the sequence diagram in section 7.1 and
the pseudocode in section 7.2. The order of operations is not incidental --
each step is positioned so that a failure at that point cannot be mistaken for
a different kind of failure:

1. **Admission and lease first.** Bounded concurrency before any work, so a
   burst of triggers cannot starve the sync service being audited (FR04-12).
2. **Identity before capture.** The epoch is checked against the source's
   ``(run_id, generation)`` up front, so a reset source is rejected rather
   than half-scanned (section 7.2).
3. **Source capture, then completeness proof.** No target work happens until
   the source manifest is provably complete; otherwise an incomplete source
   would make the target look like it holds extra rows.
4. **Pre-check P03 status, then capture the target.** The pre-check gives an
   early, cheap ``TARGET_NOT_CAUGHT_UP``; the authoritative read happens
   inside the capture transaction (section 7.3).
5. **Alignment before classification.** The gate decides whether findings may
   claim ``PROVEN`` at all, so classification never has to guess.
6. **Findings persisted before the status changes**, so a crash between them
   leaves evidence rather than a verdict with nothing behind it (section 7.6).

The honest-outcome rule runs through all of it: any missing precondition ends
the run in ``INCONCLUSIVE`` with a reason code. ``MATCHED`` is only ever
reached by proving every precondition and finding zero differences.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .alignment import AlignmentEngine, AlignmentInputs, cross_entity_tier
from .approvals import ApprovalService
from .audit import AuditEvent, AuditLog
from .clock import Clock
from .config import Settings
from .db.engine import CaptureStrategy, Database, transaction
from .db.repository import ReconRepository, RunRecord
from .diff_engine import ClassificationResult, DiffClassifier
from .dto import AlignmentDecision, SourceIdentity, SyncStatus
from .errors import (
    AdmissionRejected,
    LeaseLostError,
    ReasonCode,
    ReconError,
    SourceError,
)
from .leases import Lease, LeaseManager
from .metrics import Metrics
from .planning import RepairPlan, RepairPlanner, findings_digest
from .policy import RepairPolicy
from .ports.p03 import P03ReadPort, P03RepairPort
from .ports.source import SourcePort
from .source_capture import SourceCaptureWorker
from .target_capture import TargetCaptureWorker


@dataclass(frozen=True, slots=True)
class AuditOutcome:
    """The result of one complete audit run."""

    recon_run_id: str
    status: str
    reason_code: ReasonCode | None
    finding_count: int = 0
    findings: Sequence[Mapping[str, Any]] = ()
    decision: AlignmentDecision | None = None
    source_count: int | None = None
    target_count: int | None = None
    cross_entity_tier: str = "T1"
    classification: ClassificationResult | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    @property
    def matched(self) -> bool:
        return self.status == "MATCHED"

    @property
    def inconclusive(self) -> bool:
        return self.status == "INCONCLUSIVE"


class ReconciliationScheduler:
    """Drives audits, plans, dispatch and verification."""

    def __init__(
        self,
        *,
        db: Database,
        repository: ReconRepository,
        source: SourcePort,
        read_port: P03ReadPort,
        repair_port: P03RepairPort,
        settings: Settings,
        clock: Clock,
        metrics: Metrics | None = None,
        capture_strategy: CaptureStrategy = CaptureStrategy.IMMEDIATE,
    ) -> None:
        self._db = db
        self._repo = repository
        self._source = source
        self._read = read_port
        self._repair = repair_port
        self._settings = settings
        self._clock = clock
        self._metrics = metrics or Metrics()

        self._leases = LeaseManager(repository=repository, settings=settings, clock=clock)
        self._alignment = AlignmentEngine(settings)
        self._classifier = DiffClassifier(settings)
        self._policy = RepairPolicy(settings)
        self._planner = RepairPlanner(repository=repository, settings=settings, clock=clock)
        self._approvals = ApprovalService(repository=repository, settings=settings, clock=clock)
        self._source_capture = SourceCaptureWorker(
            repository=repository, source=source, settings=settings, clock=clock
        )
        self._target_capture = TargetCaptureWorker(
            db=db,
            repository=repository,
            settings=settings,
            clock=clock,
            strategy=capture_strategy,
        )

    # ---- accessors used by the CLI and tests -----------------------------

    @property
    def leases(self) -> LeaseManager:
        return self._leases

    @property
    def approvals(self) -> ApprovalService:
        return self._approvals

    @property
    def planner(self) -> RepairPlanner:
        return self._planner

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    # ---- the audit -------------------------------------------------------

    def run_audit(
        self,
        *,
        entity: str,
        epoch_id: str,
        request_id: str | None = None,
        scope: str = "UNFILTERED",
        verifies_plan_id: str | None = None,
        hold_lease: bool = True,
    ) -> AuditOutcome:
        """Execute one audit run end to end."""
        request = request_id or str(uuid.uuid4())
        scope_hash = self._settings.scope_hash(entity, scope)

        # Resolve the epoch before anything is persisted. A run row carries a
        # foreign key to source_epochs, so an unknown epoch would otherwise
        # surface as a raw driver integrity error instead of a named refusal.
        # It is also not an inconclusive *audit*: there is nothing to audit,
        # so there is no run to record.
        self._epoch_identity(epoch_id)

        if self._repo.active_run_count() >= self._settings.max_concurrent_runs:
            self._metrics.inc("recon_admission_rejected_total", entity=entity)
            raise AdmissionRejected(
                "too many audit runs are already active",
                active=self._repo.active_run_count(),
                limit=self._settings.max_concurrent_runs,
            )

        lease: Lease | None = None
        if hold_lease:
            lease = self._leases.acquire(epoch_id=epoch_id, entity=entity, scope_hash=scope_hash)

        run = self._repo.create_run(
            request_id=request,
            source_id=self._settings.source_id,
            epoch_id=epoch_id,
            entity=entity,
            scope=scope,
            verifies_plan_id=verifies_plan_id,
        )
        if run.status != "CREATED":
            # A recovered run from a replayed trigger. Report its stored
            # outcome rather than redoing the work (section 7.6).
            return self._outcome_from_stored(run)

        self._metrics.inc("recon_runs_started_total", entity=entity)
        timer = self._metrics.timer("recon_run_duration_seconds", entity=entity)

        try:
            return self._execute(run, entity=entity, epoch_id=epoch_id, scope=scope, lease=lease)
        except AdmissionRejected:
            raise
        except LeaseLostError as exc:
            self._repo.finish_inconclusive(run.recon_run_id, ReasonCode.LEASE_LOST)
            self._metrics.inc("recon_runs_inconclusive_total", reason=ReasonCode.LEASE_LOST.value)
            return AuditOutcome(run.recon_run_id, "INCONCLUSIVE", ReasonCode.LEASE_LOST,
                                detail={"error": str(exc)})
        except ReconError as exc:
            self._repo.finish_inconclusive(run.recon_run_id, exc.reason)
            self._metrics.inc("recon_runs_inconclusive_total", reason=exc.reason.value)
            return AuditOutcome(run.recon_run_id, "INCONCLUSIVE", exc.reason,
                                detail={"error": str(exc)})
        finally:
            timer.stop()
            if lease is not None:
                try:
                    self._leases.release(lease)
                except LeaseLostError:  # pragma: no cover - already fenced
                    pass

    # ---- audit stages ----------------------------------------------------

    def _execute(
        self,
        run: RunRecord,
        *,
        entity: str,
        epoch_id: str,
        scope: str,
        lease: Lease | None,
    ) -> AuditOutcome:
        run_id = run.recon_run_id

        # -- identity and epoch -------------------------------------------
        expected = self._epoch_identity(epoch_id)
        identity = self._source.read_identity()
        capabilities = self._source.read_capabilities()

        if (
            identity.run_id != expected["source_run_id"]
            or identity.generation != expected["source_generation"]
        ):
            # The source regenerated its data. Nothing captured under the old
            # epoch can be compared with it (section 7.7).
            self._repo.finish_inconclusive(run_id, ReasonCode.EPOCH_MISMATCH)
            self._metrics.inc("recon_runs_inconclusive_total", reason=ReasonCode.EPOCH_MISMATCH.value)
            return AuditOutcome(
                run_id,
                "INCONCLUSIVE",
                ReasonCode.EPOCH_MISMATCH,
                detail={
                    "expected": (expected["source_run_id"], expected["source_generation"]),
                    "actual": (identity.run_id, identity.generation),
                },
            )

        # -- source capture ------------------------------------------------
        self._repo.set_status(run_id, "CAPTURING_SOURCE")
        capture = self._source_capture.capture(
            run_id, entity=entity, expected_identity=identity, scope=scope
        )
        if not capture.complete:
            reason = capture.reason_code or ReasonCode.SOURCE_INCOMPLETE
            self._repo.finish_inconclusive(run_id, reason)
            self._metrics.inc("recon_runs_inconclusive_total", reason=reason.value)
            self._metrics.inc("recon_source_capture_failed_total", reason=reason.value)
            return AuditOutcome(
                run_id, "INCONCLUSIVE", reason,
                detail={"pages": capture.pages, "items": capture.items},
            )
        self._metrics.observe("recon_source_pages", capture.pages, entity=entity)

        run = self._require_run(run_id)
        source_s = run.source_snapshot_seq
        assert source_s is not None  # guaranteed by a complete capture

        # -- P03 pre-check -------------------------------------------------
        status = self._read.sync_status(self._settings.source_id, epoch_id, entity)
        early = self._precheck(status, entity=entity, source_snapshot_seq=source_s)
        if early is not None:
            self._repo.finish_inconclusive(run_id, early)
            self._metrics.inc("recon_runs_inconclusive_total", reason=early.value)
            return AuditOutcome(run_id, "INCONCLUSIVE", early,
                                detail={"source_snapshot_seq": source_s,
                                        "watermark_seq": status.watermark_seq})

        # -- target capture ------------------------------------------------
        self._repo.set_status(run_id, "CAPTURING_TARGET")
        target = self._target_capture.capture(
            run_id, entity=entity, epoch_id=epoch_id, source_snapshot_seq=source_s
        )
        if not target.complete:
            reason = target.reason_code or ReasonCode.TARGET_INCOMPLETE
            self._repo.finish_inconclusive(run_id, reason)
            self._metrics.inc("recon_runs_inconclusive_total", reason=reason.value)
            self._metrics.inc("recon_target_capture_failed_total", reason=reason.value)
            return AuditOutcome(run_id, "INCONCLUSIVE", reason)

        if lease is not None:
            # Both manifests are captured; confirm we are still the rightful
            # auditor before publishing any verdict (INV04-10).
            self._leases.validate(lease)

        # -- alignment -----------------------------------------------------
        run = self._require_run(run_id)
        decision = self._alignment.decide(
            AlignmentInputs(
                run=run,
                identity=identity,
                capabilities=capabilities,
                status=self._read.sync_status(self._settings.source_id, epoch_id, entity),
                expected_epoch_id=epoch_id,
                expected_source_id=self._settings.source_id,
            )
        )
        tier = cross_entity_tier(capabilities, self._settings.cross_entity_tier)
        self._record_alignment(run_id, decision, tier)

        # -- diff and classify ---------------------------------------------
        self._repo.set_status(run_id, "DIFFING")
        rows = self._repo.compute_diff(run_id)
        result = self._classifier.classify(run=run, rows=rows, decision=decision)

        if result.findings:
            self._repo.record_findings(run_id, result.findings)

        if not decision.strict_compare_allowed:
            reason = decision.reason_code or ReasonCode.DIFFERENT_CUT
            self._repo.finish_inconclusive(run_id, reason)
            self._metrics.inc("recon_runs_inconclusive_total", reason=reason.value)
            return AuditOutcome(
                run_id, "INCONCLUSIVE", reason,
                finding_count=len(result.findings), findings=result.findings,
                decision=decision, source_count=run.source_count, target_count=run.target_count,
                cross_entity_tier=tier, classification=result,
            )

        if not result.findings:
            self._repo.set_status(run_id, "MATCHED")
            self._metrics.inc("recon_runs_matched_total", entity=entity)
            return AuditOutcome(
                run_id, "MATCHED", None, decision=decision,
                source_count=run.source_count, target_count=run.target_count,
                cross_entity_tier=tier, classification=result,
            )

        self._repo.set_status(run_id, "FINDINGS")
        self._metrics.inc("recon_runs_findings_total", entity=entity)
        self._metrics.observe("recon_findings_per_run", len(result.findings), entity=entity)
        return AuditOutcome(
            run_id, "FINDINGS", None,
            finding_count=len(result.findings), findings=result.findings,
            decision=decision, source_count=run.source_count, target_count=run.target_count,
            cross_entity_tier=tier, classification=result,
        )

    def _precheck(
        self, status: SyncStatus, *, entity: str, source_snapshot_seq: int
    ) -> ReasonCode | None:
        """Cheap early exits before paying for a target capture."""
        if status.epoch_status != "ACTIVE":
            return ReasonCode.EPOCH_MISMATCH
        if entity == "deals":
            # Order matters: section 5.6 maps "Deal W < S" to
            # TARGET_NOT_CAUGHT_UP, and that is the code whose runbook says
            # "wait for P03". Reporting NO_PUBLISHED_SCAN for a watermark that
            # simply has not advanced yet would send the operator to the wrong
            # procedure, so the watermark is checked first.
            if status.watermark_seq < source_snapshot_seq:
                return ReasonCode.TARGET_NOT_CAUGHT_UP
            if not status.published:
                return ReasonCode.TARGET_NO_PUBLISHED_SCAN
        elif status.published_scan_id is None:
            return ReasonCode.TARGET_NO_PUBLISHED_SCAN
        return None

    # ---- planning, dispatch, verification --------------------------------

    def plan_repair(
        self, run_id: str, *, created_by: str, dry_run: bool = False
    ) -> RepairPlan | None:
        """Build a plan for a run that produced findings.

        Returns ``None`` when policy finds the run ineligible, which is the
        common and correct answer for an inconclusive or blocked run.
        """
        run = self._require_run(run_id)
        findings = [dict(row) for row in (self._repo.findings_for(run_id) or [])]
        if not findings:
            return None

        normalised = [
            {
                "finding_code": row["finding_code"],
                "item_key": row["item_key"],
                "severity": row["severity"],
                "confidence": row["confidence"],
                "evidence_hash": row["evidence_hash"],
            }
            for row in findings
        ]
        result = ClassificationResult(findings=normalised, counts=self._repo.finding_counts(run_id))
        decision = self._policy.evaluate(run=run, result=result)
        if not decision.eligible:
            return None

        status = self._read.sync_status(self._settings.source_id, run.source_epoch_id, run.entity)
        digest = findings_digest(normalised)
        if dry_run:
            return self._planner.preview(
                run=run, decision=decision, status=status, findings_digest=digest
            )
        plan = self._planner.create(
            run=run,
            decision=decision,
            status=status,
            findings_digest=digest,
            created_by=created_by,
        )
        self._repo.set_status(run_id, "PLANNED")
        self._metrics.inc("recon_plans_created_total", entity=run.entity)
        return plan

    def dispatch(self, plan: RepairPlan, *, lease: Lease | None = None):
        """Dispatch an approved plan (see ``executor.RepairExecutor``)."""
        from .executor import RepairExecutor

        executor = RepairExecutor(
            repository=self._repo,
            approvals=self._approvals,
            leases=self._leases,
            read_port=self._read,
            repair_port=self._repair,
            settings=self._settings,
            clock=self._clock,
            source=self._source,
        )
        outcome = executor.dispatch_plan(plan, lease=lease)
        self._metrics.inc("recon_dispatch_total", status=outcome.plan_status)
        return outcome

    def verify(self, plan: RepairPlan, *, entity: str | None = None):
        """Run an independent post-repair audit (see ``verifier``)."""
        from .verifier import PostRepairVerifier

        verifier = PostRepairVerifier(
            repository=self._repo,
            audit_runner=self._verification_audit,
            settings=self._settings,
            clock=self._clock,
        )
        result = verifier.verify(plan, entity=entity)
        self._metrics.inc("recon_verification_total", verified=str(result.verified).lower())
        return result

    def _verification_audit(
        self, *, entity: str, epoch_id: str, request_id: str, verifies_plan_id: str | None = None
    ) -> AuditOutcome:
        """Audit runner handed to the verifier.

        ``hold_lease=False`` because the caller already owns the scope lease
        for this epoch/entity; re-acquiring it here would deadlock against
        ourselves.
        """
        return self.run_audit(
            entity=entity,
            epoch_id=epoch_id,
            request_id=request_id,
            verifies_plan_id=verifies_plan_id,
            hold_lease=False,
        )

    # ---- helpers ---------------------------------------------------------

    def _require_run(self, run_id: str) -> RunRecord:
        run = self._repo.get_run(run_id)
        if run is None:
            raise ReconError("run disappeared", recon_run_id=run_id)
        return run

    def _epoch_identity(self, epoch_id: str) -> Mapping[str, Any]:
        """Read the source identity P03 bound to this epoch (read-only)."""
        with self._repo.session() as conn:
            row = conn.execute(
                "SELECT source_run_id, source_generation, status FROM source_epochs WHERE epoch_id = ?",
                (epoch_id,),
            ).fetchone()
        if row is None:
            raise SourceError(
                "epoch is unknown to P03", reason=ReasonCode.EPOCH_MISMATCH, epoch_id=epoch_id
            )
        return {
            "source_run_id": row["source_run_id"],
            "source_generation": int(row["source_generation"]),
            "status": row["status"],
        }

    def _record_alignment(self, run_id: str, decision: AlignmentDecision, tier: str) -> None:
        with self._repo.session() as conn, transaction(conn):
            AuditLog(conn, self._clock).append(
                AuditEvent.ALIGNMENT_DECIDED,
                recon_run_id=run_id,
                actor_id=self._settings.worker_id,
                strict_compare_allowed=decision.strict_compare_allowed,
                reason_code=decision.reason_code.value if decision.reason_code else None,
                cross_entity_tier=tier,
                failed_checks=sorted(k for k, v in decision.checks.items() if not v),
            )

    def _outcome_from_stored(self, run: RunRecord) -> AuditOutcome:
        findings = self._repo.findings_for(run.recon_run_id)
        return AuditOutcome(
            recon_run_id=run.recon_run_id,
            status=run.status,
            reason_code=ReasonCode(run.reason_code) if run.reason_code else None,
            finding_count=len(findings),
            source_count=run.source_count,
            target_count=run.target_count,
            detail={"recovered": True},
        )
