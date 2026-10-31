"""The report application service.

One ordered pipeline, used by every financial endpoint:

    capture cut -> compute (bounded) -> validate output contract
    -> final validity check -> durable audit -> serialize

The order is the contract. Nothing financial is serialized before the output
guard passes, the cut is re-checked and a durable audit receipt exists. If the
audit sink cannot accept the record, the request fails closed with ``503``
rather than returning an unaudited report.

The service returns a plain dict plus metadata; the routers own HTTP concerns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from risk_engine import RiskRequest, RiskResult, format_money, format_units

from ..admission.budgets import Deadline, RequestBudget
from ..authz.principal_context import AuthorizedScope
from ..contracts.money import FloatInPayloadError, assert_no_float_in
from ..contracts.problem_details import ApiProblem
from ..faults import FaultPoint, FaultRegistry
from ..observability.metrics import Metrics
from ..repositories.audit_store import (
    AuditCommitAmbiguous,
    AuditRecord,
    AuditStore,
    AuditUnavailable,
)
from .cut_capture import CapturedCut, CutCapturer
from .engine_port import RiskEnginePort
from .report_quality import ExpectedResultShape, decide_outcome, reason_family, validate_engine_result

__all__ = ["ReportEnvelope", "ReportRequest", "RiskReportService"]


@dataclass(frozen=True, slots=True)
class ReportRequest:
    """A fully authorized, fully validated report request."""

    endpoint_id: str
    scope: AuthorizedScope
    components: tuple[str, ...]
    allow_partial: bool
    risk_policy_version: str
    symbol_catalog_version: str
    api_schema_version: str
    result_contract_version: str
    request_id: str
    period_start_utc: str | None = None
    period_end_utc: str | None = None
    valuation_as_of: str | None = None
    workload: str = "heavy"


@dataclass(frozen=True, slots=True)
class ReportEnvelope:
    """What the router needs in order to answer."""

    payload: dict[str, object]
    http_status: int
    partial: bool
    result: RiskResult
    captured: CapturedCut
    audit_id: str | None


class RiskReportService:
    """Coordinates capture, computation, verification and audit."""

    def __init__(
        self,
        *,
        capturer: CutCapturer,
        engine: RiskEnginePort,
        audit: AuditStore,
        metrics: Metrics,
        budget: RequestBudget,
        audit_required: bool,
        max_response_bytes: int,
        reporting_currency: str = "USD",
        faults: FaultRegistry | None = None,
        max_recapture_attempts: int = 1,
    ) -> None:
        self._reporting_currency_code = reporting_currency
        self._capturer = capturer
        self._engine = engine
        self._audit = audit
        self._metrics = metrics
        self._budget = budget
        self._audit_required = audit_required
        self._max_response_bytes = max_response_bytes
        self._faults = faults or FaultRegistry()
        self._max_recapture_attempts = max(0, max_recapture_attempts)

    # ------------------------------------------------------------------ main

    async def produce(self, request: ReportRequest, *, deadline: Deadline) -> ReportEnvelope:
        """Run the pipeline and return a verified, audited report."""
        attempts = 0
        last_problem: ApiProblem | None = None

        while attempts <= self._max_recapture_attempts:
            attempts += 1
            captured = await self._capturer.capture(
                scope=request.scope,
                components=request.components,
                period_start_utc=request.period_start_utc,
                period_end_utc=request.period_end_utc,
                symbol_catalog_version=request.symbol_catalog_version,
                deadline=deadline,
            )
            result = await self._compute(request, captured, deadline=deadline)
            try:
                await self._capturer.final_check(
                    scope=request.scope, pinned=captured.revisions, deadline=deadline
                )
            except ApiProblem as problem:
                self._metrics.observe_cut_revalidation_failure(
                    source_kind="projection", failure_family=problem.code.lower()
                )
                last_problem = problem
                if problem.code in ("CUT_CHANGED",) and attempts <= self._max_recapture_attempts:
                    # One bounded recapture, if the budget allows it. Never a loop.
                    if deadline.remaining() > self._budget.final_check_seconds:
                        continue
                await self._record_audit(
                    request, captured, result=None, decision="BLOCK", denial=problem.code
                )
                raise

            outcome = decide_outcome(result, allow_partial=request.allow_partial)
            # Serialize first: a report that cannot be built must not leave an
            # ALLOW receipt behind claiming that it was.
            payload = self._serialize(request, captured, result, audit_id=None)
            audit_id = await self._record_audit(
                request, captured, result=result, decision="ALLOW", denial=None
            )
            payload["audit_id"] = audit_id
            self._metrics.observe_financial_status(
                status=result.status, reason_family=reason_family(result.reason_codes)
            )
            return ReportEnvelope(
                payload=payload,
                http_status=outcome.http_status,
                partial=outcome.partial,
                result=result,
                captured=captured,
                audit_id=audit_id,
            )

        # Unreachable in practice; the loop either returns or raises.
        raise last_problem or ApiProblem("CUT_CHANGED", "the data cut kept changing")

    # --------------------------------------------------------------- compute

    async def _compute(
        self, request: ReportRequest, captured: CapturedCut, *, deadline: Deadline
    ) -> RiskResult:
        engine_request = RiskRequest(
            source_id=request.scope.source_id,
            epoch_id=request.scope.epoch_id,
            account_ids=request.scope.accounts,
            components=request.components,  # type: ignore[arg-type]
            risk_policy_version=request.risk_policy_version,
            symbol_catalog_version=request.symbol_catalog_version,
            period_start_utc=captured.period_start_utc or request.period_start_utc,
            period_end_utc=captured.period_end_utc or request.period_end_utc,
            valuation_as_of=request.valuation_as_of,
            strict_composite=not request.allow_partial,
        )
        try:
            result = await self._engine.compute(
                engine_request,
                captured.bundle(),
                deadline=deadline,
                compute_budget_seconds=self._budget.compute_seconds,
                request_id=request.request_id,
            )
        except ApiProblem as problem:
            self._metrics.observe_engine(outcome="refused")
            await self._record_audit(
                request, captured, result=None, decision="BLOCK", denial=problem.code
            )
            raise
        self._metrics.observe_engine(outcome="completed")

        await self._faults.wait_if_barrier(FaultPoint.BARRIER_AFTER_COMPUTE)

        expected = ExpectedResultShape(
            source_id=request.scope.source_id,
            epoch_id=request.scope.epoch_id,
            accounts=request.scope.accounts,
            components=request.components,
            currency=self._reporting_currency_code,
            risk_policy_version=request.risk_policy_version,
            symbol_catalog_version=request.symbol_catalog_version,
            position_scan_id=captured.cut.position_scan_id,
            account_scan_id=captured.cut.account_scan_id,
            deal_watermark_seq=captured.cut.deal_watermark_seq,
            quote_snapshot_id=captured.cut.quote_snapshot_id,
        )
        try:
            validate_engine_result(result, expected)
        except ApiProblem as problem:
            self._metrics.observe_engine(outcome="contract_violation")
            await self._record_audit(
                request, captured, result=None, decision="ERROR", denial=problem.code
            )
            raise
        return result

    # The expected currency comes from configuration, not from the result, so
    # a result that changed currency fails validation instead of defining it.

    # ----------------------------------------------------------------- audit

    async def _record_audit(
        self,
        request: ReportRequest,
        captured: CapturedCut | None,
        *,
        result: RiskResult | None,
        decision: str,
        denial: str | None,
    ) -> str | None:
        """Persist the access record, proving durability before any payload."""
        await self._faults.wait_if_barrier(FaultPoint.BARRIER_BEFORE_AUDIT)
        record = AuditRecord(
            audit_id=AuditRecord.new_id(),
            request_id=request.request_id,
            principal_scope_hash=request.scope.audit_scope_hash(),
            endpoint_id=request.endpoint_id,
            decision=decision,  # type: ignore[arg-type]
            result_status=None if result is None else result.status,
            denial_family=denial,
            input_digest=None if result is None else result.input_digest,
            output_digest=None if result is None else result.output_digest,
            data_cut_digest=None if captured is None else captured.digest(),
            risk_policy_version=request.risk_policy_version,
            recon_revision=None if captured is None else captured.recon_revision,
            evidence=("final_check_passed",) if decision == "ALLOW" else (),
        )
        try:
            receipt = await self._audit.append(record)
        except AuditCommitAmbiguous as ambiguous:
            # The write may have landed. Prove it by id, or fail closed.
            proven = await self._prove_receipt(ambiguous.audit_id)
            if proven is None:
                self._metrics.observe_audit(result=decision.lower(), failed=True)
                raise ApiProblem(
                    "AUDIT_UNAVAILABLE",
                    "the mandatory access audit could not be proven durable",
                    reason_codes=("AUDIT_PERSISTENCE_FAILED",),
                ) from ambiguous
            self._metrics.observe_audit(result=decision.lower())
            return proven
        except AuditUnavailable as unavailable:
            self._metrics.observe_audit(result=decision.lower(), failed=True)
            if self._audit_required:
                raise ApiProblem(
                    "AUDIT_UNAVAILABLE",
                    "the mandatory access audit sink is unavailable",
                    reason_codes=("AUDIT_PERSISTENCE_FAILED",),
                ) from unavailable
            return None

        self._metrics.observe_audit(result=decision.lower())
        return receipt.audit_id

    async def _prove_receipt(self, audit_id: str) -> str | None:
        try:
            found = await self._audit.find(audit_id)
        except AuditUnavailable:
            return None
        return None if found is None else found.audit_id

    # ------------------------------------------------------------- serialize

    def _serialize(
        self,
        request: ReportRequest,
        captured: CapturedCut,
        result: RiskResult,
        *,
        audit_id: str | None,
    ) -> dict[str, object]:
        """Build the wire payload with money as strings only."""
        currency = result.currency
        payload: dict[str, object] = {
            "request_id": request.request_id,
            "calculation_id": result.calculation_id,
            "status": result.status,
            "reason_codes": list(result.reason_codes),
            "currency": currency,
            "scope": {
                "source_id": request.scope.source_id,
                "epoch_id": request.scope.epoch_id,
                "account_ids": list(request.scope.accounts),
            },
            "cuts": {
                "account_scan_id": captured.cut.account_scan_id,
                "position_scan_id": captured.cut.position_scan_id,
                "deal_watermark_seq": captured.cut.deal_watermark_seq,
                "quote_snapshot_id": captured.cut.quote_snapshot_id,
                "recon_revision": captured.recon_revision,
                "consistency": captured.cut.consistency,
                "captured_at_utc": captured.captured_at_utc,
            },
            "period": (
                None
                if captured.period_start_utc is None or captured.period_end_utc is None
                else {
                    "start_utc": captured.period_start_utc,
                    "end_utc": captured.period_end_utc,
                }
            ),
            "versions": {
                "risk_policy": result.risk_policy_version,
                "symbol_catalog": result.symbol_catalog_version,
                "api_schema": request.api_schema_version,
                "result_contract": request.result_contract_version,
            },
            "coverage": {
                "deals_complete": captured.coverage.deals_complete,
                "fee_coverage": captured.coverage.fee_coverage,
                "quote_cut_proven": captured.coverage.quote_cut_proven,
                "accounts_published": captured.coverage.accounts_published,
                "positions_published": captured.coverage.positions_published,
            },
            "components": self.components_payload(result, currency),
            "composite_total": (
                None if result.composite_total is None else format_money(result.composite_total, currency)
            ),
            "input_digest": result.input_digest,
            "output_digest": result.output_digest,
            "audit_id": audit_id,
            "partial_allowed": request.allow_partial,
        }
        self._guard_payload(payload, endpoint_class=request.endpoint_id)
        return payload

    @staticmethod
    def components_payload(result: RiskResult, currency: str) -> dict[str, object]:
        """Project the engine components into wire shape."""
        components: dict[str, object] = {}
        if result.exposure is not None:
            components["exposure"] = {
                "status": result.exposure.status,
                "reason_code": result.exposure.reason_code,
                "positions_counted": result.exposure.positions_counted,
                "by_symbol": {
                    symbol: {
                        "unit_currency": bucket.unit_currency,
                        "long_units": format_units(bucket.long_units),
                        "short_units": format_units(bucket.short_units),
                        "gross_units": format_units(bucket.gross_units),
                        "net_units": format_units(bucket.net_units),
                    }
                    for symbol, bucket in sorted(result.exposure.by_symbol.items())
                },
            }
        if result.realized is not None:
            components["realized"] = RiskReportService.realized_payload(result, currency)
        if result.unrealized is not None:
            unrealized = result.unrealized
            components["unrealized"] = {
                "status": unrealized.status,
                "reason_code": unrealized.reason_code,
                "currency": unrealized.currency,
                "total_usd": (
                    None if unrealized.total is None else format_money(unrealized.total, currency)
                ),
                "by_symbol": {
                    symbol: format_money(amount, currency)
                    for symbol, amount in sorted(unrealized.by_symbol.items())
                },
                "quote_snapshot_id": unrealized.quote_snapshot_id,
                "valuation_as_of": unrealized.valuation_as_of,
            }
        return components

    @staticmethod
    def realized_payload(result: RiskResult, currency: str) -> dict[str, object]:
        """Realized members, keeping the fee-coverage warning in the field name."""
        realized = result.realized
        if realized is None:  # pragma: no cover - callers check first
            raise ValueError("no realized component to serialize")

        def money(value: object) -> str | None:
            return None if value is None else format_money(value, currency)  # type: ignore[arg-type]

        return {
            "status": realized.status,
            "reason_code": realized.reason_code,
            "currency": realized.currency,
            "gross_usd": money(realized.gross),
            "commission_usd": money(realized.commission),
            "swap_usd": money(realized.swap),
            "net_excluding_fee_usd": money(realized.net_excluding_fee),
            "fee_coverage": realized.fee_coverage,
            "deals_counted": realized.deals_counted,
        }

    def _guard_payload(self, payload: Mapping[str, object], *, endpoint_class: str) -> None:
        """Last line of defence before serialization."""
        if self._faults.active(FaultPoint.RESPONSE_CORRUPT_DECIMAL_TYPE) is not None:
            # Simulate a serializer regression so the guard is proven to fire.
            payload = {**payload, "composite_total": 81.0}
        try:
            assert_no_float_in(dict(payload))
        except FloatInPayloadError as exc:
            # A controlled internal error, never a float on the wire.
            raise ApiProblem(
                "INTERNAL_ERROR", "the report failed its serialization guard"
            ) from exc
        encoded = _estimate_size(payload)
        if encoded > self._max_response_bytes:
            raise ApiProblem(
                "RESULT_TOO_LARGE",
                "the report exceeds the configured response size budget",
            )
        self._metrics.observe_payload(endpoint_class=endpoint_class, size_bytes=encoded)


def _estimate_size(payload: Mapping[str, object]) -> int:
    import json

    return len(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"))


def positions_page_payload(
    *,
    request_id: str,
    scope: AuthorizedScope,
    captured: CapturedCut,
    rows: Sequence[object],
    page_size: int,
    next_cursor: str | None,
    versions: Mapping[str, str],
) -> dict[str, object]:
    """Serialize a position page. Money and lots stay strings."""
    items = []
    for row in rows:
        items.append(
            {
                "position_id": row.position_id,  # type: ignore[attr-defined]
                "account_id": row.account_id,  # type: ignore[attr-defined]
                "symbol": row.symbol,  # type: ignore[attr-defined]
                "side": row.side,  # type: ignore[attr-defined]
                "volume_lots": format_units(row.lots),  # type: ignore[attr-defined]
                "open_price": format_units(row.open_price),  # type: ignore[attr-defined]
                "source_status": row.source_status,  # type: ignore[attr-defined]
                "presence": row.presence,  # type: ignore[attr-defined]
            }
        )
    payload: dict[str, object] = {
        "request_id": request_id,
        "scope": {
            "source_id": scope.source_id,
            "epoch_id": scope.epoch_id,
            "account_ids": list(scope.accounts),
        },
        "cuts": {
            "account_scan_id": captured.cut.account_scan_id,
            "position_scan_id": captured.cut.position_scan_id,
            "deal_watermark_seq": captured.cut.deal_watermark_seq,
            "quote_snapshot_id": captured.cut.quote_snapshot_id,
            "recon_revision": captured.recon_revision,
            "consistency": captured.cut.consistency,
            "captured_at_utc": captured.captured_at_utc,
        },
        "versions": dict(versions),
        "items": items,
        "page_size": page_size,
        "next_cursor": next_cursor,
        "has_more": next_cursor is not None,
    }
    assert_no_float_in(payload)
    return payload
