"""Canonical calculation inputs/outputs (design doc sections 2.4 and 7).

Everything here is frozen: once a cut is captured, the inputs a calculation ran
against must not be mutable by a later caller.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Mapping, Protocol

__all__ = [
    "Side",
    "Consistency",
    "Status",
    "Component",
    "FeeCoverage",
    "RiskCut",
    "InstrumentSpec",
    "AccountFact",
    "PositionFact",
    "QuoteFact",
    "DealFact",
    "CoverageDeclaration",
    "RiskInputBundle",
    "RiskRequest",
    "SymbolExposure",
    "ExposureComponent",
    "RealizedComponent",
    "UnrealizedComponent",
    "RiskResult",
    "RiskInputReader",
    "ALL_COMPONENTS",
]

Side = Literal["BUY", "SELL"]
Consistency = Literal["COMPATIBLE", "INDEPENDENT", "UNPROVEN"]
Status = Literal["READY", "DEGRADED", "BLOCKED", "UNSUPPORTED", "INCONCLUSIVE"]
Component = Literal["EXPOSURE", "REALIZED", "UNREALIZED"]
FeeCoverage = Literal["COVERED", "NOT_AVAILABLE"]

ALL_COMPONENTS: tuple[Component, ...] = ("EXPOSURE", "REALIZED", "UNREALIZED")


@dataclass(frozen=True, slots=True)
class RiskCut:
    """The cut vector a result is valid for. There is no single global timestamp."""

    source_id: str
    epoch_id: str
    account_scan_id: str | None = None
    position_scan_id: str | None = None
    deal_watermark_seq: int | None = None
    quote_snapshot_id: str | None = None
    fx_rate_set_id: str | None = None
    source_snapshot_seq: int | None = None
    consistency: Consistency = "UNPROVEN"

    def to_manifest(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "epoch_id": self.epoch_id,
            "account_scan_id": self.account_scan_id,
            "position_scan_id": self.position_scan_id,
            "deal_watermark_seq": self.deal_watermark_seq,
            "quote_snapshot_id": self.quote_snapshot_id,
            "fx_rate_set_id": self.fx_rate_set_id,
            "source_snapshot_seq": self.source_snapshot_seq,
            "consistency": self.consistency,
        }


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    symbol: str
    base_currency: str
    quote_currency: str
    profit_currency: str
    calc_mode: str
    contract_size: Decimal
    tick_size: Decimal
    volume_step: Decimal
    spec_version: str


@dataclass(frozen=True, slots=True)
class AccountFact:
    account_id: str
    currency: str
    presence: str
    published_scan_id: str


@dataclass(frozen=True, slots=True)
class PositionFact:
    position_id: str
    account_id: str
    symbol: str
    side: Side
    lots: Decimal
    open_price: Decimal
    source_status: str
    presence: str
    published_scan_id: str | None = None


@dataclass(frozen=True, slots=True)
class QuoteFact:
    symbol: str
    bid: Decimal
    ask: Decimal
    market_time_utc: str
    quote_id: str
    snapshot_id: str


@dataclass(frozen=True, slots=True)
class DealFact:
    deal_id: str
    account_id: str
    source_seq: int
    entry_kind: str
    gross_profit: Decimal
    commission: Decimal
    swap: Decimal
    executed_at_utc: str
    deal_type: str | None = None


@dataclass(frozen=True, slots=True)
class CoverageDeclaration:
    """What the upstream projection is willing to *prove*, not what it hopes."""

    deals_complete: bool = False
    fee_coverage: FeeCoverage = "NOT_AVAILABLE"
    quote_cut_proven: bool = False
    accounts_published: bool = False
    positions_published: bool = False
    quote_max_age_seconds: int | None = None
    quote_age_seconds: int | None = None

    def to_manifest(self) -> dict[str, object]:
        return {
            "deals_complete": self.deals_complete,
            "fee_coverage": self.fee_coverage,
            "quote_cut_proven": self.quote_cut_proven,
            "accounts_published": self.accounts_published,
            "positions_published": self.positions_published,
            "quote_max_age_seconds": self.quote_max_age_seconds,
            "quote_age_seconds": self.quote_age_seconds,
        }


@dataclass(frozen=True, slots=True)
class RiskInputBundle:
    cut: RiskCut
    accounts: tuple[AccountFact, ...]
    positions: tuple[PositionFact, ...]
    deals: tuple[DealFact, ...]
    quotes: tuple[QuoteFact, ...]
    instruments: tuple[InstrumentSpec, ...]
    coverage: CoverageDeclaration
    symbol_catalog_version: str


@dataclass(frozen=True, slots=True)
class RiskRequest:
    """Library-level request. ``account_ids`` is a *filter*, never an authorization."""

    source_id: str
    epoch_id: str
    account_ids: tuple[str, ...]
    components: tuple[Component, ...]
    risk_policy_version: str
    symbol_catalog_version: str
    period_start_utc: str | None = None
    period_end_utc: str | None = None
    valuation_as_of: str | None = None
    strict_composite: bool = True

    def to_manifest(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "epoch_id": self.epoch_id,
            "account_ids": list(self.account_ids),
            "components": list(self.components),
            "risk_policy_version": self.risk_policy_version,
            "symbol_catalog_version": self.symbol_catalog_version,
            "period_start_utc": self.period_start_utc,
            "period_end_utc": self.period_end_utc,
            "valuation_as_of": self.valuation_as_of,
            "strict_composite": self.strict_composite,
        }


@dataclass(frozen=True, slots=True)
class SymbolExposure:
    unit_currency: str
    long_units: Decimal
    short_units: Decimal
    gross_units: Decimal
    net_units: Decimal


@dataclass(frozen=True, slots=True)
class ExposureComponent:
    status: Status
    by_symbol: Mapping[str, SymbolExposure] = field(default_factory=dict)
    reason_code: str | None = None
    positions_counted: int = 0


@dataclass(frozen=True, slots=True)
class RealizedComponent:
    status: Status
    currency: str | None = None
    gross: Decimal | None = None
    commission: Decimal | None = None
    swap: Decimal | None = None
    net_excluding_fee: Decimal | None = None
    fee_coverage: FeeCoverage | None = None
    deals_counted: int = 0
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class UnrealizedComponent:
    status: Status
    currency: str | None = None
    total: Decimal | None = None
    by_symbol: Mapping[str, Decimal] = field(default_factory=dict)
    quote_snapshot_id: str | None = None
    valuation_as_of: str | None = None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class RiskResult:
    calculation_id: str
    status: Status
    reason_codes: tuple[str, ...]
    currency: str
    risk_policy_version: str
    symbol_catalog_version: str
    scope: Mapping[str, object]
    cuts: RiskCut
    exposure: ExposureComponent | None
    realized: RealizedComponent | None
    unrealized: UnrealizedComponent | None
    composite_total: Decimal | None
    input_digest: str
    output_digest: str
    strict_composite: bool

    def component(self, name: Component):
        return {"EXPOSURE": self.exposure, "REALIZED": self.realized, "UNREALIZED": self.unrealized}[name]


class RiskInputReader(Protocol):
    """Capture port implemented by the caller (Project 06 or a trusted CLI)."""

    async def capture(self, *, authorized_scope: object, cut_request: object) -> RiskInputBundle: ...
