"""Reference Project 05 risk calculation engine.

Scope for P0 (design doc section 0.3 / 5.4): linear FX instruments whose profit
currency is USD. Anything outside that profile is refused with an explicit
reason code; it is never approximated.

The engine is deliberately synchronous and pure: given the same immutable
bundle and request it always produces the same digests. Blocking and CPU
isolation concerns belong to the caller.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from typing import Mapping

from .canonical import digest
from .decimals import CALC_PRECISION, format_money, format_units, parse_decimal, quantize_money
from .errors import (
    IncompatibleCutError,
    InvalidDecimalError,
    RiskEngineError,
    UnsupportedCalcModeError,
)
from .types import (
    ALL_COMPONENTS,
    ExposureComponent,
    InstrumentSpec,
    RealizedComponent,
    RiskCut,
    RiskInputBundle,
    RiskRequest,
    RiskResult,
    Status,
    SymbolExposure,
    UnrealizedComponent,
)

__all__ = ["RiskEngine", "RISK_POLICY_VERSION", "RESULT_CONTRACT_VERSION", "parse_rfc3339"]

RISK_POLICY_VERSION = "risk-policy-v1"
RESULT_CONTRACT_VERSION = "risk-result-v1"

_OPEN_STATUSES = frozenset({"OPEN"})
_PRESENT = "PRESENT"
_SUPPORTED_CALC_MODES = frozenset({"FOREX_LINEAR"})
_REPORTING_CURRENCY = "USD"


def parse_rfc3339(value: str, *, field: str) -> datetime:
    """Parse a strict RFC3339 UTC instant. A trailing Z is required."""
    if not isinstance(value, str) or not value.endswith("Z"):
        raise InvalidDecimalError(f"{field}: RFC3339 UTC instant with trailing Z required")
    if len(value) < 20 or value[10] != "T":
        raise InvalidDecimalError(f"{field}: date and time must be separated by T")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidDecimalError(f"{field}: undecodable RFC3339 instant") from exc
    if parsed.tzinfo is None:
        raise InvalidDecimalError(f"{field}: naive instant rejected")
    return parsed.astimezone(timezone.utc)


class RiskEngine:
    """Pure calculator over an already-captured, already-authorized bundle."""

    def __init__(
        self,
        *,
        risk_policy_version: str = RISK_POLICY_VERSION,
        reporting_currency: str = _REPORTING_CURRENCY,
    ) -> None:
        self.risk_policy_version = risk_policy_version
        self.reporting_currency = reporting_currency

    # ------------------------------------------------------------------ public

    def compute(
        self,
        request: RiskRequest,
        bundle: RiskInputBundle,
        *,
        calculation_id: str | None = None,
    ) -> RiskResult:
        """Compute the requested components and the composite decision."""
        self._validate_versions(request, bundle)
        scope_accounts = self._scope_accounts(request)
        instruments = {spec.symbol: spec for spec in bundle.instruments}

        reasons: list[str] = []
        exposure = realized = unrealized = None

        if "EXPOSURE" in request.components:
            exposure = self._exposure(bundle, scope_accounts, instruments, reasons)
        if "REALIZED" in request.components:
            realized = self._realized(request, bundle, scope_accounts, reasons)
        if "UNREALIZED" in request.components:
            unrealized = self._unrealized(request, bundle, scope_accounts, instruments, reasons)

        status, composite, composite_reasons = self._composite(
            request, bundle.cut, exposure, realized, unrealized
        )
        for reason in composite_reasons:
            if reason not in reasons:
                reasons.append(reason)

        input_digest = digest(self._input_manifest(request, bundle, scope_accounts))

        scope = {
            "source_id": request.source_id,
            "epoch_id": request.epoch_id,
            "account_ids": sorted(scope_accounts),
        }
        output_manifest = {
            "status": status,
            "reason_codes": sorted(set(reasons)),
            "currency": self.reporting_currency,
            "risk_policy_version": self.risk_policy_version,
            "symbol_catalog_version": bundle.symbol_catalog_version,
            "scope": scope,
            "cuts": bundle.cut.to_manifest(),
            "exposure": self._exposure_manifest(exposure),
            "realized": self._realized_manifest(realized),
            "unrealized": self._unrealized_manifest(unrealized),
            "composite_total": (
                None if composite is None else format_money(composite, self.reporting_currency)
            ),
            "input_digest": input_digest,
            "strict_composite": request.strict_composite,
        }

        return RiskResult(
            calculation_id=calculation_id or str(uuid.uuid4()),
            status=status,
            reason_codes=tuple(sorted(set(reasons))),
            currency=self.reporting_currency,
            risk_policy_version=self.risk_policy_version,
            symbol_catalog_version=bundle.symbol_catalog_version,
            scope=scope,
            cuts=bundle.cut,
            exposure=exposure,
            realized=realized,
            unrealized=unrealized,
            composite_total=composite,
            input_digest=input_digest,
            output_digest=digest(output_manifest),
            strict_composite=request.strict_composite,
        )

    # ------------------------------------------------------------- validation

    def _validate_versions(self, request: RiskRequest, bundle: RiskInputBundle) -> None:
        if request.risk_policy_version != self.risk_policy_version:
            raise RiskEngineError(
                f"risk policy {request.risk_policy_version!r} is not installed",
                reason_codes=("POLICY_VERSION_CHANGED",),
            )
        if request.symbol_catalog_version != bundle.symbol_catalog_version:
            raise RiskEngineError(
                "symbol catalog version mismatch between request and captured bundle",
                reason_codes=("POLICY_VERSION_CHANGED",),
            )
        if not request.components:
            raise RiskEngineError(
                "at least one component is required", reason_codes=("INVALID_DECIMAL",)
            )
        unknown = [c for c in request.components if c not in ALL_COMPONENTS]
        if unknown:
            raise UnsupportedCalcModeError(f"unknown components: {sorted(unknown)}")
        if bundle.cut.source_id != request.source_id or bundle.cut.epoch_id != request.epoch_id:
            raise IncompatibleCutError("captured cut does not match the requested source/epoch")

    @staticmethod
    def _scope_accounts(request: RiskRequest) -> frozenset[str]:
        """Accounts the calculation runs over.

        An empty filter never means every account: the caller must name them.
        """
        if not request.account_ids:
            raise RiskEngineError(
                "account filter must be explicit", reason_codes=("UNAUTHORIZED_SCOPE",)
            )
        return frozenset(request.account_ids)

    # -------------------------------------------------------------- exposure

    def _exposure(
        self,
        bundle: RiskInputBundle,
        scope: frozenset[str],
        instruments: Mapping[str, InstrumentSpec],
        reasons: list[str],
    ) -> ExposureComponent:
        if not bundle.coverage.positions_published:
            reasons.append("P03_UNPUBLISHED")
            return ExposureComponent(status="BLOCKED", reason_code="P03_UNPUBLISHED")

        longs: dict[str, Decimal] = {}
        shorts: dict[str, Decimal] = {}
        units_currency: dict[str, str] = {}
        counted = 0
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            for position in bundle.positions:
                if position.account_id not in scope:
                    continue
                if position.presence != _PRESENT:
                    # Absence is not CLOSED; it is simply not part of this cut.
                    continue
                if position.source_status not in _OPEN_STATUSES:
                    continue
                spec = instruments.get(position.symbol)
                if spec is None or spec.calc_mode not in _SUPPORTED_CALC_MODES:
                    reasons.append("UNSUPPORTED_CALC_MODE")
                    return ExposureComponent(status="UNSUPPORTED", reason_code="UNSUPPORTED_CALC_MODE")
                lots = parse_decimal(position.lots, field=f"position[{position.position_id}].lots")
                contract_size = parse_decimal(
                    spec.contract_size, field=f"spec[{spec.symbol}].contract_size"
                )
                units = lots * contract_size
                units_currency.setdefault(position.symbol, spec.base_currency)
                if units_currency[position.symbol] != spec.base_currency:
                    raise RiskEngineError(
                        "conflicting unit currency for one symbol",
                        reason_codes=("DATA_INTEGRITY_CONFLICT",),
                    )
                if position.side == "BUY":
                    longs[position.symbol] = longs.get(position.symbol, Decimal(0)) + units
                else:
                    shorts[position.symbol] = shorts.get(position.symbol, Decimal(0)) + units
                counted += 1

            result: dict[str, SymbolExposure] = {}
            for symbol in sorted(units_currency):
                long_units = longs.get(symbol, Decimal(0))
                short_units = shorts.get(symbol, Decimal(0))
                result[symbol] = SymbolExposure(
                    unit_currency=units_currency[symbol],
                    long_units=long_units,
                    short_units=short_units,
                    gross_units=long_units + short_units,
                    net_units=long_units - short_units,
                )
        return ExposureComponent(status="READY", by_symbol=result, positions_counted=counted)

    # -------------------------------------------------------------- realized

    def _realized(
        self,
        request: RiskRequest,
        bundle: RiskInputBundle,
        scope: frozenset[str],
        reasons: list[str],
    ) -> RealizedComponent:
        if request.period_start_utc is None or request.period_end_utc is None:
            raise RiskEngineError(
                "realized component requires an explicit half-open period",
                reason_codes=("INVALID_DECIMAL",),
            )
        start = parse_rfc3339(request.period_start_utc, field="period_start_utc")
        end = parse_rfc3339(request.period_end_utc, field="period_end_utc")
        if end <= start:
            raise RiskEngineError(
                "period must be a non-empty half-open interval",
                reason_codes=("INVALID_DECIMAL",),
            )
        if not bundle.coverage.deals_complete:
            reasons.append("DEAL_WATERMARK_UNPROVEN")
            return RealizedComponent(status="BLOCKED", reason_code="DEAL_WATERMARK_UNPROVEN")

        gross = Decimal(0)
        commission = Decimal(0)
        swap = Decimal(0)
        counted = 0
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            for deal in bundle.deals:
                if deal.account_id not in scope:
                    continue
                executed = parse_rfc3339(
                    deal.executed_at_utc, field=f"deal[{deal.deal_id}].executed_at_utc"
                )
                if not (start <= executed < end):
                    continue
                gross += parse_decimal(deal.gross_profit, field=f"deal[{deal.deal_id}].gross_profit")
                commission += parse_decimal(deal.commission, field=f"deal[{deal.deal_id}].commission")
                swap += parse_decimal(deal.swap, field=f"deal[{deal.deal_id}].swap")
                counted += 1
            # Signed amounts: a cost is already negative, abs() would hide it.
            net = gross + commission + swap

        if bundle.coverage.fee_coverage == "NOT_AVAILABLE":
            reasons.append("FEE_COVERAGE_UNAVAILABLE")

        return RealizedComponent(
            status="READY",
            currency=self.reporting_currency,
            gross=quantize_money(gross, self.reporting_currency),
            commission=quantize_money(commission, self.reporting_currency),
            swap=quantize_money(swap, self.reporting_currency),
            net_excluding_fee=quantize_money(net, self.reporting_currency),
            fee_coverage=bundle.coverage.fee_coverage,
            deals_counted=counted,
        )

    # ------------------------------------------------------------ unrealized

    def _unrealized(
        self,
        request: RiskRequest,
        bundle: RiskInputBundle,
        scope: frozenset[str],
        instruments: Mapping[str, InstrumentSpec],
        reasons: list[str],
    ) -> UnrealizedComponent:
        if not bundle.coverage.positions_published:
            reasons.append("P03_UNPUBLISHED")
            return UnrealizedComponent(status="BLOCKED", reason_code="P03_UNPUBLISHED")
        if not bundle.coverage.quote_cut_proven or bundle.cut.quote_snapshot_id is None:
            reasons.append("QUOTE_CUT_UNPROVEN")
            return UnrealizedComponent(status="BLOCKED", reason_code="QUOTE_CUT_UNPROVEN")

        max_age = bundle.coverage.quote_max_age_seconds
        age = bundle.coverage.quote_age_seconds
        if max_age is not None and age is not None and age > max_age:
            reasons.append("STALE_MARK")
            return UnrealizedComponent(status="BLOCKED", reason_code="STALE_MARK")

        quotes = {quote.symbol: quote for quote in bundle.quotes}
        by_symbol: dict[str, Decimal] = {}
        total = Decimal(0)
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            for position in bundle.positions:
                if position.account_id not in scope:
                    continue
                if position.presence != _PRESENT or position.source_status not in _OPEN_STATUSES:
                    continue
                spec = instruments.get(position.symbol)
                if spec is None or spec.calc_mode not in _SUPPORTED_CALC_MODES:
                    reasons.append("UNSUPPORTED_CALC_MODE")
                    return UnrealizedComponent(
                        status="UNSUPPORTED", reason_code="UNSUPPORTED_CALC_MODE"
                    )
                if spec.profit_currency != self.reporting_currency:
                    # P0 refuses conversion rather than inventing an FX path.
                    reasons.append("FX_RATE_MISSING")
                    return UnrealizedComponent(status="BLOCKED", reason_code="FX_RATE_MISSING")
                quote = quotes.get(position.symbol)
                if quote is None or quote.snapshot_id != bundle.cut.quote_snapshot_id:
                    reasons.append("QUOTE_CUT_UNPROVEN")
                    return UnrealizedComponent(status="BLOCKED", reason_code="QUOTE_CUT_UNPROVEN")
                lots = parse_decimal(position.lots, field="lots")
                units = lots * parse_decimal(spec.contract_size, field="contract_size")
                open_price = parse_decimal(position.open_price, field="open_price")
                bid = parse_decimal(quote.bid, field="quote.bid")
                ask = parse_decimal(quote.ask, field="quote.ask")
                if bid > ask:
                    raise RiskEngineError(
                        "crossed quote rejected", reason_codes=("DATA_INTEGRITY_CONFLICT",)
                    )
                if position.side == "BUY":
                    pnl = (bid - open_price) * units
                else:
                    pnl = (open_price - ask) * units
                by_symbol[position.symbol] = by_symbol.get(position.symbol, Decimal(0)) + pnl
                total += pnl

        return UnrealizedComponent(
            status="READY",
            currency=self.reporting_currency,
            total=quantize_money(total, self.reporting_currency),
            by_symbol={
                symbol: quantize_money(value, self.reporting_currency)
                for symbol, value in sorted(by_symbol.items())
            },
            quote_snapshot_id=bundle.cut.quote_snapshot_id,
            valuation_as_of=request.valuation_as_of,
        )

    # -------------------------------------------------------------- composite

    def _composite(
        self,
        request: RiskRequest,
        cut: RiskCut,
        exposure: ExposureComponent | None,
        realized: RealizedComponent | None,
        unrealized: UnrealizedComponent | None,
    ) -> tuple[Status, Decimal | None, tuple[str, ...]]:
        """Decide the overall status and whether a composite total may exist."""
        present = [c for c in (exposure, realized, unrealized) if c is not None]
        statuses = {c.status for c in present}
        reasons: list[str] = []

        if "UNSUPPORTED" in statuses:
            return "UNSUPPORTED", None, tuple(reasons)

        if statuses != {"READY"}:
            # Something could not be proven. A strict caller gets a blocker.
            return ("BLOCKED" if request.strict_composite else "DEGRADED"), None, tuple(reasons)

        money_components = [c for c in (realized, unrealized) if c is not None]
        if len(money_components) < 2:
            # A single money component needs no cross-cut sum.
            return "READY", None, tuple(reasons)

        if cut.consistency != "COMPATIBLE":
            # Each component is individually proven but they are not on one
            # logical cut, so a cross-component sum would be fiction.
            reasons.append("INCOMPATIBLE_CUT")
            return ("BLOCKED" if request.strict_composite else "DEGRADED"), None, tuple(reasons)

        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            total = Decimal(0)
            if realized is not None and realized.net_excluding_fee is not None:
                total += realized.net_excluding_fee
            if unrealized is not None and unrealized.total is not None:
                total += unrealized.total
        return "READY", quantize_money(total, self.reporting_currency), tuple(reasons)

    # -------------------------------------------------------------- manifests

    def _input_manifest(
        self, request: RiskRequest, bundle: RiskInputBundle, scope: frozenset[str]
    ) -> dict[str, object]:
        return {
            "request": request.to_manifest(),
            "cut": bundle.cut.to_manifest(),
            "coverage": bundle.coverage.to_manifest(),
            "symbol_catalog_version": bundle.symbol_catalog_version,
            "risk_policy_version": self.risk_policy_version,
            "accounts": sorted(
                (
                    {"account_id": a.account_id, "currency": a.currency, "presence": a.presence}
                    for a in bundle.accounts
                    if a.account_id in scope
                ),
                key=lambda row: row["account_id"],
            ),
            "positions": sorted(
                (
                    {
                        "position_id": p.position_id,
                        "account_id": p.account_id,
                        "symbol": p.symbol,
                        "side": p.side,
                        "lots": p.lots,
                        "open_price": p.open_price,
                        "source_status": p.source_status,
                        "presence": p.presence,
                    }
                    for p in bundle.positions
                    if p.account_id in scope
                ),
                key=lambda row: row["position_id"],
            ),
            "deals": sorted(
                (
                    {
                        "deal_id": d.deal_id,
                        "account_id": d.account_id,
                        "source_seq": d.source_seq,
                        "entry_kind": d.entry_kind,
                        "gross_profit": d.gross_profit,
                        "commission": d.commission,
                        "swap": d.swap,
                        "executed_at_utc": d.executed_at_utc,
                    }
                    for d in bundle.deals
                    if d.account_id in scope
                ),
                key=lambda row: (row["source_seq"], row["deal_id"]),
            ),
            "quotes": sorted(
                (
                    {
                        "symbol": q.symbol,
                        "bid": q.bid,
                        "ask": q.ask,
                        "market_time_utc": q.market_time_utc,
                        "quote_id": q.quote_id,
                        "snapshot_id": q.snapshot_id,
                    }
                    for q in bundle.quotes
                ),
                key=lambda row: row["symbol"],
            ),
        }

    @staticmethod
    def _exposure_manifest(component: ExposureComponent | None) -> object:
        if component is None:
            return None
        return {
            "status": component.status,
            "reason_code": component.reason_code,
            "positions_counted": component.positions_counted,
            "by_symbol": {
                symbol: {
                    "unit_currency": value.unit_currency,
                    "long_units": format_units(value.long_units),
                    "short_units": format_units(value.short_units),
                    "gross_units": format_units(value.gross_units),
                    "net_units": format_units(value.net_units),
                }
                for symbol, value in sorted(component.by_symbol.items())
            },
        }

    def _money(self, value: Decimal | None) -> str | None:
        return None if value is None else format_money(value, self.reporting_currency)

    def _realized_manifest(self, component: RealizedComponent | None) -> object:
        if component is None:
            return None
        return {
            "status": component.status,
            "reason_code": component.reason_code,
            "currency": component.currency,
            "gross": self._money(component.gross),
            "commission": self._money(component.commission),
            "swap": self._money(component.swap),
            "net_excluding_fee": self._money(component.net_excluding_fee),
            "fee_coverage": component.fee_coverage,
            "deals_counted": component.deals_counted,
        }

    def _unrealized_manifest(self, component: UnrealizedComponent | None) -> object:
        if component is None:
            return None
        return {
            "status": component.status,
            "reason_code": component.reason_code,
            "currency": component.currency,
            "total": self._money(component.total),
            "by_symbol": {
                symbol: self._money(value) for symbol, value in sorted(component.by_symbol.items())
            },
            "quote_snapshot_id": component.quote_snapshot_id,
            "valuation_as_of": component.valuation_as_of,
        }
