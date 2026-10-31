"""Strict response contracts.

Every monetary field is typed ``str`` and validated against the canonical
decimal shape. A ``float`` cannot reach the wire through these models, and the
response validator re-checks the serialized tree before it is sent.

Field naming note: the P0 profile reports in USD only (design doc section 5.4),
and the published contract names the realized members ``gross_usd``,
``commission_usd``, ``swap_usd`` and ``net_excluding_fee_usd``. The suffix is
part of the agreed wire contract and the ``currency`` member repeats it
explicitly. ``net_excluding_fee_usd`` is never shortened to ``net_usd``: the
longer name is the warning that fee coverage may be missing.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator
from risk_engine import is_canonical_decimal_string

__all__ = [
    "ScopeBlock",
    "CutsBlock",
    "VersionsBlock",
    "SymbolExposureOut",
    "ExposureComponentOut",
    "RealizedComponentOut",
    "UnrealizedComponentOut",
    "ComponentsBlock",
    "RiskResponse",
    "PnlResponse",
    "PeriodBlock",
    "PositionOut",
    "PositionsPage",
    "ExposureResponse",
    "DailyReport",
    "CoverageBlock",
    "ProblemResponse",
    "StatusName",
]

StatusName = Literal["READY", "DEGRADED", "BLOCKED", "UNSUPPORTED", "INCONCLUSIVE"]
ConsistencyName = Literal["COMPATIBLE", "INDEPENDENT", "UNPROVEN"]
FeeCoverageName = Literal["COVERED", "NOT_AVAILABLE"]
Money = Annotated[str, StringConstraints(min_length=1, max_length=40)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


def _validate_money(value: str | None) -> str | None:
    if value is None:
        return None
    if not is_canonical_decimal_string(value):
        raise ValueError("monetary values must be canonical decimal strings")
    return value


class ScopeBlock(_Strict):
    source_id: str
    epoch_id: str
    account_ids: tuple[str, ...]


class CutsBlock(_Strict):
    """The cut vector the payload is valid for. There is no single global time."""

    account_scan_id: str | None = None
    position_scan_id: str | None = None
    deal_watermark_seq: int | None = None
    quote_snapshot_id: str | None = None
    recon_revision: str | None = None
    consistency: ConsistencyName = "UNPROVEN"
    captured_at_utc: str | None = None


class VersionsBlock(_Strict):
    risk_policy: str
    symbol_catalog: str
    api_schema: str
    result_contract: str


class SymbolExposureOut(_Strict):
    unit_currency: str
    long_units: Money
    short_units: Money
    gross_units: Money
    net_units: Money

    @field_validator("long_units", "short_units", "gross_units", "net_units")
    @classmethod
    def _units_are_decimal_strings(cls, value: str) -> str:
        return _validate_money(value)  # type: ignore[return-value]


class ExposureComponentOut(_Strict):
    status: StatusName
    reason_code: str | None = None
    positions_counted: int = 0
    by_symbol: dict[str, SymbolExposureOut] = {}


class RealizedComponentOut(_Strict):
    status: StatusName
    reason_code: str | None = None
    currency: str | None = None
    gross_usd: Money | None = None
    commission_usd: Money | None = None
    swap_usd: Money | None = None
    net_excluding_fee_usd: Money | None = None
    fee_coverage: FeeCoverageName | None = None
    deals_counted: int = 0

    @field_validator("gross_usd", "commission_usd", "swap_usd", "net_excluding_fee_usd")
    @classmethod
    def _money_strings(cls, value: str | None) -> str | None:
        return _validate_money(value)


class UnrealizedComponentOut(_Strict):
    status: StatusName
    reason_code: str | None = None
    currency: str | None = None
    total_usd: Money | None = None
    by_symbol: dict[str, Money] = {}
    quote_snapshot_id: str | None = None
    valuation_as_of: str | None = None

    @field_validator("total_usd")
    @classmethod
    def _total_money(cls, value: str | None) -> str | None:
        return _validate_money(value)

    @field_validator("by_symbol")
    @classmethod
    def _symbol_money(cls, value: dict[str, str]) -> dict[str, str]:
        for amount in value.values():
            _validate_money(amount)
        return value


class ComponentsBlock(_Strict):
    exposure: ExposureComponentOut | None = None
    realized: RealizedComponentOut | None = None
    unrealized: UnrealizedComponentOut | None = None


class PeriodBlock(_Strict):
    """Half-open ``[start, end)`` interval over realized deal execution time."""

    start_utc: str
    end_utc: str


class CoverageBlock(_Strict):
    deals_complete: bool
    fee_coverage: FeeCoverageName
    quote_cut_proven: bool
    accounts_published: bool
    positions_published: bool


class RiskResponse(_Strict):
    request_id: str
    calculation_id: str
    status: StatusName
    reason_codes: tuple[str, ...]
    currency: str
    scope: ScopeBlock
    cuts: CutsBlock
    versions: VersionsBlock
    coverage: CoverageBlock
    components: ComponentsBlock
    composite_total: Money | None = None
    input_digest: str
    output_digest: str
    audit_id: str | None = None
    partial_allowed: bool = False

    @field_validator("composite_total")
    @classmethod
    def _composite_money(cls, value: str | None) -> str | None:
        return _validate_money(value)


class PnlResponse(_Strict):
    request_id: str
    calculation_id: str
    status: StatusName
    reason_codes: tuple[str, ...]
    currency: str
    scope: ScopeBlock
    period: PeriodBlock
    cuts: CutsBlock
    versions: VersionsBlock
    coverage: CoverageBlock
    realized: RealizedComponentOut
    input_digest: str
    output_digest: str
    audit_id: str | None = None
    partial_allowed: bool = False


class PositionOut(_Strict):
    position_id: str
    account_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    volume_lots: Money
    open_price: Money
    source_status: str
    presence: str

    @field_validator("volume_lots", "open_price")
    @classmethod
    def _decimal_strings(cls, value: str) -> str:
        return _validate_money(value)  # type: ignore[return-value]


class PositionsPage(_Strict):
    """A page from one verified published scan. Pages are never stitched across scans."""

    request_id: str
    scope: ScopeBlock
    cuts: CutsBlock
    versions: VersionsBlock
    items: tuple[PositionOut, ...]
    page_size: int
    next_cursor: str | None = None
    has_more: bool = False


class ExposureResponse(_Strict):
    request_id: str
    calculation_id: str
    status: StatusName
    reason_codes: tuple[str, ...]
    currency: str
    scope: ScopeBlock
    cuts: CutsBlock
    versions: VersionsBlock
    coverage: CoverageBlock
    exposure: ExposureComponentOut
    group_count: int
    input_digest: str
    output_digest: str
    audit_id: str | None = None
    partial_allowed: bool = False


class DailyReport(_Strict):
    """Realized deal activity for one UTC calendar day.

    This report knows nothing about the end-of-day position snapshot and never
    implies it does.
    """

    request_id: str
    calculation_id: str
    status: StatusName
    reason_codes: tuple[str, ...]
    currency: str
    report_date: str
    scope: ScopeBlock
    period: PeriodBlock
    cuts: CutsBlock
    versions: VersionsBlock
    coverage: CoverageBlock
    realized: RealizedComponentOut
    input_digest: str
    output_digest: str
    audit_id: str | None = None
    partial_allowed: bool = False


class ProblemResponse(_Strict):
    """RFC 9457 problem document, declared so it appears in the OpenAPI schema."""

    type: str
    title: str
    status: int
    detail: str
    instance: str
    code: str
    request_id: str
    retryable: bool
    reason_codes: tuple[str, ...] = ()
