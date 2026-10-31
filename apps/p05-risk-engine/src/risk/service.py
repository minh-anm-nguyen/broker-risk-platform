"""The orchestrator: capture, gate, compute, fence, audit, respond.

Order is the whole design. Each step either narrows what is possible or proves
something, and the steps that prove things come *after* the work they protect:

1. **Admit.** Bounded concurrency; overflow is rejected in milliseconds.
2. **Authorize.** Narrow the request filter against the granted scope. No
   database query happens for an unauthorized account.
3. **Capture.** One read snapshot: pointers, then facts, then release.
4. **Pin.** Quote snapshot and catalog version, both immutable for the request.
5. **Gate.** Per-component verdicts; fail closed.
6. **Compute.** Pure functions over the frozen bundle.
7. **Fence.** Re-read epoch and reconciliation revision. If the source reset or
   a blocking finding appeared while the arithmetic ran, the result is
   discarded -- it describes a world that no longer exists (K05-12, WF5-208).
8. **Audit.** Durable receipt. If the policy says mandatory and it fails, the
   result is not returned.

Step 7 before step 8 before the response, and never a shortcut from step 6 to
the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from risk.adapters.p03_reader import P03Reader
from risk.adapters.quotes import QuoteProvider
from risk.audit import AuditSink, make_idempotency_key, new_receipt, record_with_retry
from risk.contract import (
    ComponentPayload,
    RiskRequest,
    RiskResult,
    composite_payload,
    exposure_payload,
    realized_payload,
    unrealized_payload,
)
from risk.domain.classification import select_eligible_deals
from risk.domain.exposure import compute_exposure
from risk.domain.facts import AuthorizedScope, CoverageReport, RiskCut, RiskInputBundle
from risk.domain.realized import compute_realized
from risk.domain.unrealized import compute_unrealized
from risk.errors import AuditError, ReasonCode, RiskError
from risk.gates import evaluate_gates
from risk.limits import AdmissionController, Deadline
from risk.manifest import build_input_manifest, classify_exactness, digest, scope_hash
from risk.observability import MetricsRegistry, Timer, log_fields, reason_family
from risk.policies.loader import RiskPolicy, load_catalog, load_policy
from risk.status import (
    Component,
    ComponentStatus,
    ExactnessClass,
    GlobalStatus,
    derive_global_status,
)

__all__ = ["RiskService", "ServiceDependencies"]


@dataclass
class ServiceDependencies:
    reader: P03Reader
    quote_provider: QuoteProvider | None
    audit_sink: AuditSink
    metrics: MetricsRegistry


class RiskService:
    """Computes risk reports from the Project 03 read model."""

    def __init__(
        self,
        deps: ServiceDependencies,
        policy: RiskPolicy | None = None,
        *,
        clock: object | None = None,
    ) -> None:
        self._deps = deps
        self._policy = policy or load_policy()
        self._admission = AdmissionController(self._policy.limits)
        # Injectable so that freshness and deadline behaviour are testable
        # without sleeping.
        self._now = clock if callable(clock) else (lambda: datetime.now(UTC))

    @property
    def policy(self) -> RiskPolicy:
        return self._policy

    @property
    def admission(self) -> AdmissionController:
        return self._admission

    async def compute(self, request: RiskRequest, scope: AuthorizedScope) -> RiskResult:
        """Run one calculation end to end.

        Returns a :class:`RiskResult` for every outcome that the engine can
        describe, including ``BLOCKED`` and ``UNSUPPORTED``. Only genuinely
        exceptional conditions -- an unauthorized scope, an overload, a blown
        deadline -- propagate as exceptions, because those have no meaningful
        per-component answer.
        """
        timer = Timer()
        async with self._admission.admit():
            return await self._compute_admitted(request, scope, timer)

    async def _compute_admitted(
        self, request: RiskRequest, scope: AuthorizedScope, timer: Timer
    ) -> RiskResult:
        policy = self._resolve_policy(request)
        deadline = Deadline(policy.limits.wall_clock_deadline_seconds)

        # Narrow before reading. An account outside the grant must not reach a
        # WHERE clause, so that the audit log of the database never shows the
        # engine asking about it (WF5-193).
        account_ids = scope.narrow(request.account_ids)

        needs_positions = bool(
            {Component.EXPOSURE, Component.UNREALIZED} & request.components
        )
        needs_deals = Component.REALIZED in request.components

        captured = await self._deps.reader.capture(
            scope=scope,
            epoch_id=request.epoch_id,
            account_ids=account_ids,
            period_start=request.period_start,
            period_end=request.period_end,
            need_positions=needs_positions,
            need_deals=needs_deals,
            deadline=deadline,
        )
        deadline.check(stage="post_capture")

        catalog = load_catalog(request.symbol_catalog_version)

        quotes = None
        quote_error: RiskError | None = None
        if Component.UNREALIZED in request.components or request.include_quote_notional:
            quotes, quote_error = await self._pin_quotes(request, captured, deadline)

        valuation_as_of = request.valuation_as_of or (
            quotes.captured_at if quotes is not None else self._now()
        )

        coverage = CoverageReport(
            account_scan_published=captured.coverage.account_scan_published,
            position_scan_published=captured.coverage.position_scan_published,
            position_scan_terminal=captured.coverage.position_scan_terminal,
            deal_watermark_terminal=captured.coverage.deal_watermark_terminal,
            deal_watermark_unfiltered=captured.coverage.deal_watermark_unfiltered,
            epoch_active=captured.coverage.epoch_active,
            quote_snapshot_pinned=quotes is not None,
            fee_fields_available=policy.fee_fields_available,
            position_row_count=captured.coverage.position_row_count,
            deal_row_count=captured.coverage.deal_row_count,
            notes=captured.coverage.notes,
        )

        cut = RiskCut(
            source_id=captured.cut.source_id,
            epoch_id=captured.cut.epoch_id,
            account_scan_id=captured.cut.account_scan_id,
            position_scan_id=captured.cut.position_scan_id,
            deal_watermark_seq=captured.cut.deal_watermark_seq,
            quote_snapshot_id=None if quotes is None else quotes.snapshot_id,
            fx_rate_set_id=None,
            symbol_catalog_version=catalog.catalog_version,
            recon_revision=captured.cut.recon_revision,
            cut_proof_id=captured.cut.cut_proof_id,
            source_snapshot_seq=captured.cut.source_snapshot_seq,
        )

        bundle = RiskInputBundle(
            cut=cut,
            scope=AuthorizedScope(
                tenant_id=scope.tenant_id,
                source_id=scope.source_id,
                account_ids=account_ids,
                may_read_audit=scope.may_read_audit,
            ),
            accounts=captured.accounts,
            positions=captured.positions,
            deals=captured.deals,
            catalog=catalog,
            quotes=quotes,
            recon=captured.recon,
            coverage=coverage,
            valuation_as_of=valuation_as_of,
            period_start=request.period_start,
            period_end=request.period_end,
        )

        manifest = build_input_manifest(
            bundle,
            risk_policy_version=policy.policy_version,
            rounding_mode=policy.rounding_mode,
            source_capability=str(policy.source_capability),
            requested_components=[str(c) for c in request.components],
            strict_composite=request.strict_composite,
        )
        input_digest = manifest.input_digest()

        gate_report = evaluate_gates(
            bundle,
            policy,
            requested=request.components,
            strict_composite=request.strict_composite,
            historical=request.historical,
        )
        deadline.check(stage="post_gate")

        payloads, realized_result, unrealized_result = self._compute_components(
            bundle, policy, request, gate_report, quote_error
        )

        # --- final fence -----------------------------------------------------
        extra_reasons: list[ReasonCode] = []
        epoch_status, generation = await self._deps.reader.current_epoch_fence(request.epoch_id)
        if generation != captured.epoch_generation or (
            epoch_status != captured.epoch_status and epoch_status != "ACTIVE"
        ):
            raise RiskError(
                ReasonCode.EPOCH_CHANGED,
                "source epoch changed while the calculation was running",
                before=captured.epoch_status,
                after=epoch_status,
            )
        latest_recon = await self._deps.reader.current_recon_revision(request.epoch_id)
        if latest_recon is not None and captured.cut.recon_revision is not None:
            if latest_recon != captured.cut.recon_revision:
                # New evidence landed mid-flight. Whether it clears or blocks,
                # this result was computed without it.
                extra_reasons.append(ReasonCode.RECON_BLOCKING)
                payloads = {
                    name: ComponentPayload(
                        ComponentStatus.BLOCKED,
                        tuple(dict.fromkeys([*payload.reason_codes, str(ReasonCode.RECON_BLOCKING)])),
                    )
                    for name, payload in payloads.items()
                }
                realized_result = None
                unrealized_result = None

        statuses = {name: payload.status for name, payload in payloads.items()}
        status = derive_global_status(statuses, strict_composite=request.strict_composite)

        composite = None
        exactness: ExactnessClass = classify_exactness(
            cut, composite_requested=len(request.components) > 1
        )
        if (
            gate_report.composite_allowed
            and not extra_reasons
            and Component.REALIZED in request.components
            and Component.UNREALIZED in request.components
        ):
            composite = composite_payload(
                realized_result, unrealized_result, rounding=policy.rounding_mode
            )

        reason_codes = tuple(
            dict.fromkeys(
                [str(code) for code in gate_report.all_reason_codes()]
                + [str(code) for code in extra_reasons]
            )
        )

        output_core = {
            "status": str(status),
            "components": {str(k): v.as_payload() for k, v in sorted(payloads.items())},
            "composite_total": composite,
            "cuts": cut.as_manifest(),
            "currency": self._reporting_currency(bundle),
            "risk_policy_version": policy.policy_version,
            "symbol_catalog_version": catalog.catalog_version,
        }
        output_digest = digest(output_core)

        idempotency_key = make_idempotency_key(
            input_digest=input_digest,
            risk_policy_version=policy.policy_version,
            requested_components=tuple(sorted(str(c) for c in request.components)),
            strict_composite=request.strict_composite,
        )
        receipt = new_receipt(
            idempotency_key=idempotency_key,
            epoch_id=request.epoch_id,
            account_scope_hash=manifest.scope_hash,
            risk_policy_version=policy.policy_version,
            symbol_catalog_version=catalog.catalog_version,
            cut_manifest=cut.as_manifest(),
            input_digest=input_digest,
            output_digest=output_digest,
            status=status,
            reason_codes=reason_codes,
            duration_ms=timer.elapsed_ms,
            now=self._now(),
        )

        try:
            stored = await record_with_retry(
                self._deps.audit_sink, receipt, attempts=policy.audit_retry_attempts
            )
        except AuditError:
            if policy.audit_mandatory_for_ready and status in (
                GlobalStatus.READY,
                GlobalStatus.DEGRADED,
            ):
                # Fail closed. The arithmetic succeeded; the control did not.
                self._record_metrics(GlobalStatus.BLOCKED, payloads, timer)
                raise
            stored = receipt

        result = RiskResult(
            calculation_id=stored.calculation_id,
            status=status,
            reason_codes=reason_codes,
            currency=self._reporting_currency(bundle),
            exactness=exactness,
            scope={
                "source_id": scope.source_id,
                "epoch_id": request.epoch_id,
                "scope_hash": manifest.scope_hash,
                "account_count": len(account_ids),
            },
            cuts=cut.as_manifest(),
            components=payloads,
            composite_total=composite,
            input_digest=input_digest,
            output_digest=output_digest,
            risk_policy_version=policy.policy_version,
            symbol_catalog_version=catalog.catalog_version,
            duration_ms=timer.elapsed_ms,
            audit_receipt_id=stored.calculation_id,
            lineage=manifest.as_payload(),
        )

        self._record_metrics(status, payloads, timer)
        self._deps.metrics.set_gauge("risk_inflight_jobs", self._admission.running, instance="local")
        self.last_log_record = log_fields(
            calculation_id=result.calculation_id,
            scope_hash=manifest.scope_hash,
            source_id=scope.source_id,
            epoch_id=request.epoch_id,
            status=str(status),
            reason_codes=reason_codes,
            risk_policy_version=policy.policy_version,
            symbol_catalog_version=catalog.catalog_version,
            duration_ms=timer.elapsed_ms,
        )
        return result

    # ------------------------------------------------------------------ helpers

    def _resolve_policy(self, request: RiskRequest) -> RiskPolicy:
        """Pin exactly one policy for the whole calculation.

        Loaded once, here. Re-reading it later in the request would allow a
        deployment switch to mix two rounding modes into one number (WF5-209).
        """
        if request.risk_policy_version == self._policy.policy_version:
            return self._policy
        return load_policy(request.risk_policy_version)

    async def _pin_quotes(
        self, request: RiskRequest, captured: object, deadline: Deadline
    ) -> tuple[object | None, RiskError | None]:
        """Fetch and pin a quote snapshot, converting failure into a reason.

        A quote outage must not fail the whole request: realized P&L does not
        depend on a mark, and blocking it too would be an availability loss
        with no correctness benefit (WF5-020/K05-20).
        """
        if self._deps.quote_provider is None:
            from risk.errors import QualityGateError

            return None, QualityGateError(
                ReasonCode.QUOTE_MISSING, "no quote provider is configured"
            )
        symbols = sorted({p.symbol for p in getattr(captured, "positions", ()) if p.is_open})
        deadline.check(stage="pin_quotes")
        try:
            snapshot = await self._deps.quote_provider.snapshot(
                symbols=symbols,
                as_of=request.valuation_as_of or self._now(),
                snapshot_id=request.quote_snapshot_id,
            )
        except RiskError as exc:
            self._deps.metrics.increment(
                "risk_quote_stale_total", symbol_group="all"
            )
            return None, exc
        return snapshot, None

    def _reporting_currency(self, bundle: RiskInputBundle) -> str | None:
        try:
            return bundle.single_account_currency()
        except RiskError:
            # Multiple or missing currencies: the envelope says ``null`` and a
            # reason code explains it, rather than naming one arbitrarily.
            return None

    def _compute_components(
        self,
        bundle: RiskInputBundle,
        policy: RiskPolicy,
        request: RiskRequest,
        gate_report: object,
        quote_error: RiskError | None,
    ) -> tuple[dict[Component, ComponentPayload], object | None, object | None]:
        payloads: dict[Component, ComponentPayload] = {}
        realized_result = None
        unrealized_result = None
        currency = self._reporting_currency(bundle)

        for component in sorted(request.components, key=str):
            verdict = gate_report.verdict(component)  # type: ignore[attr-defined]
            if not verdict.computable:
                reasons = tuple(str(c) for c in verdict.reason_codes)
                if component is Component.UNREALIZED and quote_error is not None:
                    reasons = tuple(dict.fromkeys([*reasons, str(quote_error.reason)]))
                payloads[component] = ComponentPayload(verdict.status, reasons)
                continue

            try:
                if component is Component.EXPOSURE:
                    result = compute_exposure(
                        bundle.positions,
                        bundle.catalog,
                        quotes=bundle.quotes,
                        include_quote_notional=request.include_quote_notional,
                    )
                    payloads[component] = ComponentPayload(
                        verdict.status,
                        tuple(str(c) for c in verdict.reason_codes),
                        exposure_payload(result),
                    )
                elif component is Component.REALIZED:
                    assert currency is not None
                    eligibility = select_eligible_deals(
                        bundle.deals,
                        capability=policy.source_capability,
                        period_start=bundle.period_start,
                        period_end=bundle.period_end,
                        deal_watermark_seq=bundle.cut.deal_watermark_seq,
                    )
                    realized_result = compute_realized(
                        eligibility,
                        currency=currency,
                        fee_fields_available=policy.fee_fields_available,
                    )
                    payloads[component] = ComponentPayload(
                        verdict.status,
                        tuple(str(c) for c in verdict.reason_codes),
                        realized_payload(realized_result, rounding=policy.rounding_mode),
                    )
                else:
                    assert currency is not None
                    assert bundle.quotes is not None
                    assert bundle.valuation_as_of is not None
                    unrealized_result = compute_unrealized(
                        bundle.positions,
                        bundle.catalog,
                        bundle.quotes,
                        valuation_as_of=bundle.valuation_as_of,
                        max_quote_age_seconds=policy.quotes.threshold_for(
                            historical=request.historical
                        ),
                        reporting_currency=currency,
                    )
                    payloads[component] = ComponentPayload(
                        verdict.status,
                        tuple(str(c) for c in verdict.reason_codes),
                        unrealized_payload(unrealized_result, rounding=policy.rounding_mode),
                    )
            except RiskError as exc:
                # A calculator refusing its input is a per-component block, not
                # a request failure. The reason code is the calculator's, so
                # the caller learns which rule stopped it.
                status = (
                    ComponentStatus.UNSUPPORTED
                    if exc.reason
                    in (
                        ReasonCode.UNSUPPORTED_CALC_MODE,
                        ReasonCode.UNSUPPORTED_SYMBOL,
                        ReasonCode.UNSUPPORTED_POSITION_STATUS,
                    )
                    else ComponentStatus.BLOCKED
                )
                payloads[component] = ComponentPayload(status, (str(exc.reason),))
                if component is Component.REALIZED:
                    realized_result = None
                elif component is Component.UNREALIZED:
                    unrealized_result = None

        return payloads, realized_result, unrealized_result

    def _record_metrics(
        self,
        status: GlobalStatus,
        payloads: dict[Component, ComponentPayload],
        timer: Timer,
    ) -> None:
        metrics = self._deps.metrics
        for component, payload in payloads.items():
            family = (
                reason_family(payload.reason_codes[0]) if payload.reason_codes else "NONE"
            )
            metrics.increment(
                "risk_calculation_total",
                component=str(component),
                status=str(payload.status),
                reason_family=family,
            )
            metrics.observe(
                "risk_calculation_duration_seconds",
                timer.elapsed_seconds,
                component=str(component),
                status=str(payload.status),
            )
        metrics.increment(
            "risk_calculation_envelope_total", status=str(status), outcome="final"
        )
