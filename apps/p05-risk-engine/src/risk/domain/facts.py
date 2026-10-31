"""Validated input facts, the cut vector, and the immutable input bundle.

Nothing reaches the calculators as a raw dictionary. Each fact type validates
itself on construction, which gives one place to answer "could this row ever
produce a wrong number?" and makes the calculators small enough to review.

The cut vector (:class:`RiskCut`) is the load-bearing idea of the whole design.
Project 03 keeps accounts, positions and deals in separate projections with
separate publication points; there is no single ``as_of`` that describes all of
them. So a result carries the identity of *each* cut, plus a statement about
whether anything proves they belong together -- and a composite total is only
offered when something does (section 4.1).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Iterable, Mapping, Sequence

from risk.domain.instruments import InstrumentSpec, SymbolCatalog
from risk.domain.money import Money, MoneySemantic, parse_money, validate_currency
from risk.errors import ContractError, QualityGateError, ReasonCode
from risk.status import Consistency, Presence, Side

__all__ = [
    "parse_utc",
    "AccountFact",
    "PositionFact",
    "DealFact",
    "QuoteFact",
    "QuoteSnapshot",
    "ReconEvidence",
    "RiskCut",
    "AuthorizedScope",
    "RiskInputBundle",
    "CoverageReport",
    "MAX_ACCOUNT_ID_LENGTH",
]

#: Account identifiers are opaque broker strings. They are bounded so that a
#: hostile 10 MiB identifier cannot be allocated, logged or used as a map key
#: (WF5-199).
MAX_ACCOUNT_ID_LENGTH = 64
MAX_ID_LENGTH = 128

#: Position states this build knows how to interpret. Anything else blocks the
#: affected scope: an unrecognised state must not be quietly skipped, because
#: skipping it understates exposure (WF5-053).
KNOWN_POSITION_STATUSES = frozenset({"OPEN", "CLOSED", "PENDING"})


def parse_utc(raw: object, *, field_name: str) -> datetime:
    """Parse a timezone-aware instant and normalise it to UTC.

    A naive timestamp is rejected outright. Guessing the server's local zone
    would move a deal across a reporting boundary and nobody would see it
    happen (WF5-160).
    """
    if isinstance(raw, datetime):
        value = raw
    elif isinstance(raw, str):
        text = raw.strip()
        if len(text) > 64:
            raise ContractError(ReasonCode.INVALID_TIMESTAMP, "timestamp literal too long", field=field_name)
        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            value = datetime.fromisoformat(candidate)
        except ValueError:
            # Covers malformed offsets such as +99:00 (WF5-071).
            raise ContractError(
                ReasonCode.INVALID_TIMESTAMP, "not a valid ISO-8601 instant", field=field_name
            ) from None
    else:
        raise ContractError(
            ReasonCode.INVALID_TIMESTAMP,
            "timestamp must be an ISO-8601 string or datetime",
            field=field_name,
            got=type(raw).__name__,
        )

    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ContractError(
            ReasonCode.NAIVE_TIMESTAMP_REJECTED,
            "timestamp must carry an explicit offset",
            field=field_name,
        )
    return value.astimezone(UTC)


def _validate_id(raw: object, *, field_name: str, max_length: int = MAX_ID_LENGTH) -> str:
    if not isinstance(raw, str) or not raw:
        raise ContractError(
            ReasonCode.CONTRACT_MISSING_FIELD, "identifier must be a non-empty string", field=field_name
        )
    if len(raw) > max_length:
        raise ContractError(
            ReasonCode.CONTRACT_MISSING_FIELD, "identifier too long", field=field_name, length=len(raw)
        )
    # Control characters would corrupt log lines and digest inputs alike.
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw):
        raise ContractError(
            ReasonCode.CONTRACT_MISSING_FIELD,
            "identifier contains control characters",
            field=field_name,
        )
    return raw


@dataclass(frozen=True, slots=True)
class AccountFact:
    """One row of the Project 03 ``accounts_current`` projection."""

    account_id: str
    currency: str
    epoch_id: str
    published_scan_id: str
    presence: Presence = Presence.PRESENT

    def __post_init__(self) -> None:
        _validate_id(self.account_id, field_name="account_id", max_length=MAX_ACCOUNT_ID_LENGTH)
        _validate_id(self.epoch_id, field_name="epoch_id")
        _validate_id(self.published_scan_id, field_name="published_scan_id")
        if not isinstance(self.currency, str) or not self.currency:
            # An empty account currency cannot be defaulted to USD: every
            # downstream amount would carry a fabricated label (WF5-002).
            raise ContractError(
                ReasonCode.INVALID_ACCOUNT_CURRENCY,
                "account currency is missing",
                account_scope="redacted",
            )
        try:
            validate_currency(self.currency)
        except ContractError as exc:
            raise ContractError(
                ReasonCode.INVALID_ACCOUNT_CURRENCY, "account currency is malformed"
            ) from exc
        if not isinstance(self.presence, Presence):
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "presence must be a Presence member")


@dataclass(frozen=True, slots=True)
class PositionFact:
    """One row of the Project 03 ``positions_current`` projection."""

    position_id: str
    account_id: str
    symbol: str
    side: Side
    lots: Decimal
    open_price: Decimal
    source_status: str
    presence: Presence
    epoch_id: str
    published_scan_id: str

    def __post_init__(self) -> None:
        _validate_id(self.position_id, field_name="position_id")
        _validate_id(self.account_id, field_name="account_id", max_length=MAX_ACCOUNT_ID_LENGTH)
        _validate_id(self.symbol, field_name="symbol", max_length=32)
        _validate_id(self.epoch_id, field_name="epoch_id")
        _validate_id(self.published_scan_id, field_name="published_scan_id")
        if not isinstance(self.side, Side):
            raise ContractError(
                ReasonCode.UNSUPPORTED_POSITION_SIDE, "side must be a Side member", symbol=self.symbol
            )
        if not isinstance(self.presence, Presence):
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "presence must be a Presence member")
        if not isinstance(self.lots, Decimal) or not self.lots.is_finite():
            raise ContractError(ReasonCode.INVALID_POSITION_VOLUME, "lots must be a finite Decimal")
        if self.lots <= 0:
            # Zero lots with an OPEN status is a corrupted row, and a negative
            # volume is an attempt to encode direction twice (WF5-056, WF5-151).
            raise ContractError(
                ReasonCode.INVALID_POSITION_VOLUME,
                "open position volume must be strictly positive",
                symbol=self.symbol,
            )
        if not isinstance(self.open_price, Decimal) or not self.open_price.is_finite():
            raise ContractError(ReasonCode.INVALID_POSITION_PRICE, "open_price must be a finite Decimal")
        if self.open_price <= 0:
            raise ContractError(
                ReasonCode.INVALID_POSITION_PRICE,
                "open price must be strictly positive",
                symbol=self.symbol,
            )
        if not isinstance(self.source_status, str) or not self.source_status:
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "source_status is required")

    @property
    def is_open(self) -> bool:
        return self.presence is Presence.PRESENT and self.source_status == "OPEN"

    def require_known_status(self) -> None:
        """Reject states outside the approved mapping."""
        if self.source_status not in KNOWN_POSITION_STATUSES:
            raise ContractError(
                ReasonCode.UNSUPPORTED_POSITION_STATUS,
                "position status is outside the approved mapping",
                symbol=self.symbol,
                source_status=self.source_status,
            )

    def signed_units(self, spec: InstrumentSpec) -> Decimal:
        units = spec.units_for(self.lots)
        return units if self.side is Side.BUY else -units


@dataclass(frozen=True, slots=True)
class DealFact:
    """One immutable row of the Project 03 ``deals`` table.

    ``gross_profit``, ``commission`` and ``swap`` are kept as separate signed
    :class:`Money` values. They are never collapsed on ingress, and ``abs()`` is
    never applied: a positive commission is a rebate, and flipping its sign
    turns a credit into a charge (WF5-112, WF5-135).
    """

    deal_id: str
    account_id: str
    source_seq: int
    entry_kind: str
    deal_type: str | None
    gross_profit: Money
    commission: Money
    swap: Money
    executed_at: datetime
    epoch_id: str
    payload_hash: str = ""

    def __post_init__(self) -> None:
        _validate_id(self.deal_id, field_name="deal_id")
        _validate_id(self.account_id, field_name="account_id", max_length=MAX_ACCOUNT_ID_LENGTH)
        _validate_id(self.epoch_id, field_name="epoch_id")
        if not isinstance(self.source_seq, int) or isinstance(self.source_seq, bool):
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "source_seq must be an int")
        if self.source_seq < 0:
            raise ContractError(ReasonCode.DATA_INTEGRITY_CONFLICT, "source_seq must be non-negative")
        if not isinstance(self.entry_kind, str) or not self.entry_kind:
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "entry_kind is required")
        if self.executed_at.tzinfo is None:
            raise ContractError(ReasonCode.NAIVE_TIMESTAMP_REJECTED, "executed_at must be tz-aware")
        expected = {
            "gross_profit": MoneySemantic.REALIZED_GROSS,
            "commission": MoneySemantic.COMMISSION,
            "swap": MoneySemantic.SWAP,
        }
        currencies = set()
        for name, semantic in expected.items():
            value = getattr(self, name)
            if not isinstance(value, Money):
                raise ContractError(
                    ReasonCode.MONEY_FIELD_MISSING, "deal money field is missing", field=name
                )
            if value.semantic is not semantic:
                raise ContractError(
                    ReasonCode.UNIT_MISMATCH, "deal money field has the wrong semantic", field=name
                )
            currencies.add(value.currency)
        if len(currencies) != 1:
            # One deal reporting profit in EUR and commission in USD needs a
            # conversion policy before any net can be stated (WF5-144).
            raise ContractError(
                ReasonCode.CURRENCY_MISMATCH,
                "deal money fields disagree on currency",
                currencies=sorted(currencies),
            )

    @property
    def currency(self) -> str:
        return self.gross_profit.currency


@dataclass(frozen=True, slots=True)
class QuoteFact:
    """A validated two-sided market mark pinned to a snapshot."""

    symbol: str
    bid: Decimal
    ask: Decimal
    market_time: datetime
    quote_id: str
    snapshot_id: str
    feed_status: str = "ACTIVE"

    def __post_init__(self) -> None:
        _validate_id(self.symbol, field_name="symbol", max_length=32)
        _validate_id(self.quote_id, field_name="quote_id")
        _validate_id(self.snapshot_id, field_name="snapshot_id")
        for name in ("bid", "ask"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ContractError(ReasonCode.INVALID_QUOTE, "quote price must be a finite Decimal",
                                    field=name, symbol=self.symbol)
            if value <= 0:
                raise ContractError(
                    ReasonCode.INVALID_QUOTE, "quote price must be positive", field=name, symbol=self.symbol
                )
        if self.bid > self.ask:
            raise ContractError(
                ReasonCode.INVALID_QUOTE, "inverted spread: bid above ask", symbol=self.symbol
            )
        if self.market_time.tzinfo is None:
            raise ContractError(ReasonCode.NAIVE_TIMESTAMP_REJECTED, "market_time must be tz-aware")

    @property
    def mid(self) -> Decimal:
        """Mid price, used only for informational exposure notional."""
        return (self.bid + self.ask) / Decimal(2)

    def closing_price_for(self, side: Side) -> Decimal:
        """The price at which an open position of ``side`` would be closed.

        A long is closed by selling into the bid; a short is closed by buying
        from the ask. Using the wrong side understates the cost of the spread
        and makes every open position look slightly profitable (WF5-061/062).
        """
        return self.bid if side is Side.BUY else self.ask


@dataclass(frozen=True)
class QuoteSnapshot:
    """An immutable, versioned set of quotes used for one calculation.

    Pinning matters even inside a single request: valuing position 1 at one
    tick and position 10 000 at a later tick produces a portfolio that never
    existed at any instant (WF5-129).
    """

    snapshot_id: str
    quotes: Mapping[str, QuoteFact]
    captured_at: datetime
    source_id: str = ""
    bound_position_scan_id: str | None = None

    def __post_init__(self) -> None:
        _validate_id(self.snapshot_id, field_name="snapshot_id")
        for symbol, quote in self.quotes.items():
            if symbol != quote.symbol:
                # A quote filed under another symbol's key would price the
                # wrong instrument (WF5-068, WF5-076).
                raise ContractError(
                    ReasonCode.QUOTE_SYMBOL_MISMATCH,
                    "quote is keyed under a different symbol",
                    key=symbol,
                    quote_symbol=quote.symbol,
                )
            if quote.snapshot_id != self.snapshot_id:
                raise ContractError(
                    ReasonCode.QUOTE_IMMUTABILITY_CONFLICT,
                    "quote belongs to a different snapshot",
                    symbol=symbol,
                )

    def get(self, symbol: str) -> QuoteFact:
        try:
            return self.quotes[symbol]
        except KeyError:
            raise QualityGateError(
                ReasonCode.QUOTE_MISSING,
                "no pinned quote for this symbol",
                symbol=symbol,
                snapshot_id=self.snapshot_id,
            ) from None

    def __contains__(self, symbol: object) -> bool:
        return symbol in self.quotes


@dataclass(frozen=True, slots=True)
class ReconEvidence:
    """Project 04 verification state, bound to the scope it actually covers.

    ``VERIFIED`` is not a blanket pardon. It applies to the accounts, the epoch
    and the watermark it was produced for; reused outside that scope it becomes
    a claim nobody made (WF5-035, WF5-178).
    """

    status: str
    revision: int
    epoch_id: str
    covered_accounts: frozenset[str]
    covered_deal_watermark: int | None
    blocking_accounts: frozenset[str] = frozenset()
    blocking_symbols: frozenset[str] = frozenset()
    scope_known: bool = True

    #: Statuses that permit a READY result for a covered scope.
    CLEAN_STATUSES = frozenset({"VERIFIED", "MATCHED"})
    #: Statuses that block. Anything *unknown* also blocks (WF5-271).
    BLOCKING_STATUSES = frozenset({"INCONCLUSIVE", "BLOCKED", "DISPUTED", "APPROVED", "COMMITTED"})

    def __post_init__(self) -> None:
        if not isinstance(self.status, str) or not self.status:
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "recon status is required")
        if not isinstance(self.revision, int) or self.revision < 0:
            raise ContractError(
                ReasonCode.CONTRACT_MISSING_FIELD, "recon revision must be a non-negative int"
            )

    def covers(self, account_ids: Iterable[str]) -> bool:
        return self.scope_known and set(account_ids).issubset(self.covered_accounts)


@dataclass(frozen=True, slots=True)
class RiskCut:
    """The identity of every data cut that contributed to a calculation.

    ``consistency`` is never inferred from the fact that all the ids happen to
    be populated. It is ``COMPATIBLE`` only when ``cut_proof_id`` names an
    upstream freeze that produced them together (section 4.1, WF5-030).
    """

    source_id: str
    epoch_id: str
    account_scan_id: str | None = None
    position_scan_id: str | None = None
    deal_watermark_seq: int | None = None
    quote_snapshot_id: str | None = None
    fx_rate_set_id: str | None = None
    symbol_catalog_version: str | None = None
    recon_revision: int | None = None
    cut_proof_id: str | None = None
    source_snapshot_seq: int | None = None

    def __post_init__(self) -> None:
        _validate_id(self.source_id, field_name="source_id")
        _validate_id(self.epoch_id, field_name="epoch_id")

    @property
    def consistency(self) -> Consistency:
        if self.cut_proof_id:
            return Consistency.COMPATIBLE
        if self.position_scan_id and self.deal_watermark_seq is not None:
            return Consistency.INDEPENDENT
        return Consistency.UNPROVEN

    def as_manifest(self) -> dict[str, object]:
        """Flat, hashable description of the cut."""
        return {
            "source_id": self.source_id,
            "epoch_id": self.epoch_id,
            "account_scan_id": self.account_scan_id,
            "position_scan_id": self.position_scan_id,
            "deal_watermark_seq": self.deal_watermark_seq,
            "quote_snapshot_id": self.quote_snapshot_id,
            "fx_rate_set_id": self.fx_rate_set_id,
            "symbol_catalog_version": self.symbol_catalog_version,
            "recon_revision": self.recon_revision,
            "cut_proof_id": self.cut_proof_id,
            "source_snapshot_seq": self.source_snapshot_seq,
            "consistency": str(self.consistency),
        }


@dataclass(frozen=True, slots=True)
class AuthorizedScope:
    """What the *authenticated* caller may read.

    This never comes from request JSON. The request may narrow the scope with a
    filter; it may not widen it. Keeping the two apart is what makes a tampered
    ``source_id`` in a request body harmless (WF5-195).
    """

    tenant_id: str
    source_id: str
    account_ids: frozenset[str]
    may_read_audit: bool = False

    def __post_init__(self) -> None:
        _validate_id(self.tenant_id, field_name="tenant_id")
        _validate_id(self.source_id, field_name="source_id")
        if not self.account_ids:
            raise ContractError(ReasonCode.UNAUTHORIZED_SCOPE, "authorized scope cannot be empty")
        for account_id in self.account_ids:
            _validate_id(account_id, field_name="account_id", max_length=MAX_ACCOUNT_ID_LENGTH)

    def narrow(self, requested: Sequence[str] | None) -> frozenset[str]:
        """Intersect a requested filter with the authorization.

        Raises:
            AuthorizationError-equivalent ``ContractError`` with
            ``UNAUTHORIZED_SCOPE`` when the request names an account outside the
            grant. Silently dropping it would let a caller probe which account
            ids exist by watching totals change.
        """
        from risk.errors import AuthorizationError

        if requested is None:
            return self.account_ids
        asked = {_validate_id(a, field_name="account_id", max_length=MAX_ACCOUNT_ID_LENGTH)
                 for a in requested}
        outside = asked - self.account_ids
        if outside:
            raise AuthorizationError(
                ReasonCode.UNAUTHORIZED_SCOPE,
                "request names accounts outside the authorized scope",
                outside_count=len(outside),
            )
        if not asked:
            raise AuthorizationError(ReasonCode.UNAUTHORIZED_SCOPE, "empty account filter")
        return frozenset(asked)


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Proof -- or absence of proof -- that the captured input is complete.

    Every flag defaults to the pessimistic value. A bundle built without
    explicitly asserting completeness cannot produce a ``READY`` component, so
    forgetting to set a flag fails closed rather than open (WF5-014 in spirit:
    absence of evidence is not evidence).
    """

    account_scan_published: bool = False
    position_scan_published: bool = False
    position_scan_terminal: bool = False
    deal_watermark_terminal: bool = False
    deal_watermark_unfiltered: bool = False
    epoch_active: bool = False
    quote_snapshot_pinned: bool = False
    fee_fields_available: bool = False
    position_row_count: int = 0
    deal_row_count: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RiskInputBundle:
    """Everything one calculation is allowed to look at, frozen.

    The bundle is deliberately boring: no lazy database handles, no callables,
    no clock. Given the same bundle the calculators must produce the same
    result on any host, which is what makes the output digest a usable oracle
    (INV05-10).
    """

    cut: RiskCut
    scope: AuthorizedScope
    accounts: tuple[AccountFact, ...]
    positions: tuple[PositionFact, ...]
    deals: tuple[DealFact, ...]
    catalog: SymbolCatalog
    quotes: QuoteSnapshot | None
    recon: ReconEvidence | None
    coverage: CoverageReport
    valuation_as_of: datetime | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None

    def __post_init__(self) -> None:
        # INV05-01: not one row may belong to another epoch, source or account.
        for account in self.accounts:
            if account.epoch_id != self.cut.epoch_id:
                raise ContractError(
                    ReasonCode.EPOCH_IDENTITY_CONFLICT, "account row from another epoch"
                )
            if account.account_id not in self.scope.account_ids:
                raise ContractError(
                    ReasonCode.UNAUTHORIZED_SCOPE, "account row outside the authorized scope"
                )
        # Uniqueness is checked here, at the edge of the pure core, because a
        # reader that delivers a row twice is broken and every component built
        # on the bundle would be wrong in the same way. Catching it per
        # component would report it as three unrelated failures (WF5-224).
        seen_positions: set[str] = set()
        for position in self.positions:
            if position.epoch_id != self.cut.epoch_id:
                raise ContractError(
                    ReasonCode.EPOCH_IDENTITY_CONFLICT, "position row from another epoch"
                )
            if position.account_id not in self.scope.account_ids:
                raise ContractError(
                    ReasonCode.UNAUTHORIZED_SCOPE, "position row outside the authorized scope"
                )
            if position.position_id in seen_positions:
                raise ContractError(
                    ReasonCode.DATA_INTEGRITY_CONFLICT,
                    "duplicate position_id in the captured input set",
                    position_id=position.position_id,
                )
            seen_positions.add(position.position_id)

        seen_deals: set[str] = set()
        for deal in self.deals:
            if deal.epoch_id != self.cut.epoch_id:
                raise ContractError(ReasonCode.EPOCH_IDENTITY_CONFLICT, "deal row from another epoch")
            if deal.account_id not in self.scope.account_ids:
                raise ContractError(
                    ReasonCode.UNAUTHORIZED_SCOPE, "deal row outside the authorized scope"
                )
            if deal.deal_id in seen_deals:
                raise ContractError(
                    ReasonCode.DATA_INTEGRITY_CONFLICT,
                    "duplicate deal_id in the captured input set",
                    deal_id=deal.deal_id,
                )
            seen_deals.add(deal.deal_id)
        if self.period_start and self.period_end and self.period_start >= self.period_end:
            raise ContractError(
                ReasonCode.INVALID_TIME_WINDOW, "period_start must be strictly before period_end"
            )
        if self.recon is not None and self.recon.epoch_id != self.cut.epoch_id:
            # Repair evidence from a previous source generation says nothing
            # about this one (WF5-174).
            raise ContractError(
                ReasonCode.RECON_EVIDENCE_STALE, "reconciliation evidence belongs to another epoch"
            )

    @property
    def account_currencies(self) -> frozenset[str]:
        return frozenset(a.currency for a in self.accounts)

    def single_account_currency(self) -> str:
        """The one reporting currency, or a refusal to invent one."""
        currencies = self.account_currencies
        if len(currencies) == 1:
            return next(iter(currencies))
        if not currencies:
            raise QualityGateError(
                ReasonCode.INVALID_ACCOUNT_CURRENCY, "no account rows to establish a reporting currency"
            )
        raise QualityGateError(
            ReasonCode.CURRENCY_MISMATCH,
            "multiple account currencies in one request need an approved FX rate set",
            currencies=sorted(currencies),
        )


def build_money_triplet(
    *,
    currency: str,
    gross: object,
    commission: object,
    swap: object,
) -> tuple[Money, Money, Money]:
    """Validate the three signed deal money fields together.

    A missing field is an error, never a zero. ``COALESCE(swap, 0)`` at the SQL
    boundary is the single most convincing way to report a wrong net figure
    (WF5-005, WF5-113).
    """
    if gross is None:
        raise ContractError(ReasonCode.MONEY_FIELD_MISSING, "realized_pnl_gross is required")
    if commission is None:
        raise ContractError(ReasonCode.MONEY_FIELD_MISSING, "commission is required")
    if swap is None:
        raise ContractError(ReasonCode.MONEY_FIELD_MISSING, "swap is required")
    return (
        parse_money(gross, currency, MoneySemantic.REALIZED_GROSS, field="realized_pnl_gross"),
        parse_money(commission, currency, MoneySemantic.COMMISSION, field="commission"),
        parse_money(swap, currency, MoneySemantic.SWAP, field="swap"),
    )
