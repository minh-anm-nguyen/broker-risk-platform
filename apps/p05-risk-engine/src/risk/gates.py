"""Quality gates: the decision layer between captured inputs and arithmetic.

The calculators are deliberately naive -- give them positions and a quote and
they will produce a number. Everything that decides whether they *should* be
given those inputs lives here, in one place, so that the question "why is this
BLOCKED?" has exactly one file to read.

Three properties define the layer:

*Per-component, not global.* A stale quote has nothing to do with whether last
month's realized P&L is correct. Collapsing them into one boolean means either
blocking a good number or publishing a bad one, so each component gets its own
verdict (section 4.2).

*Fail closed, including on the unexpected.* An unknown reconciliation status
blocks. A coverage flag nobody set blocks. The gate never reasons "no evidence
of a problem" into "no problem" (WF5-271).

*Reasons, always.* Every non-READY verdict carries at least one
:class:`~risk.errors.ReasonCode`. A blocked component with no reason is itself
a contract violation (WF5-283).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from risk.domain.facts import RiskInputBundle
from risk.domain.instruments import SUPPORTED_CALC_MODES
from risk.domain.unrealized import quote_age_seconds
from risk.errors import ReasonCode
from risk.policies.loader import RiskPolicy
from risk.status import Component, ComponentStatus, Consistency

__all__ = [
    "ComponentVerdict",
    "GateReport",
    "evaluate_gates",
]


@dataclass(frozen=True)
class ComponentVerdict:
    """Whether one component may be computed, and why not if it may not."""

    component: Component
    status: ComponentStatus
    reason_codes: tuple[ReasonCode, ...] = ()

    @property
    def computable(self) -> bool:
        """Whether the arithmetic may run.

        ``STALE`` is computable: the number is produced and then branded. That
        is the whole point -- a stale valuation is information, a fabricated
        zero is not (section 9.1).
        """
        return self.status in (ComponentStatus.READY, ComponentStatus.STALE)

    def as_payload(self) -> dict[str, object]:
        return {
            "status": str(self.status),
            "reason_codes": [str(code) for code in self.reason_codes],
        }


@dataclass(frozen=True)
class GateReport:
    """Verdicts for every requested component plus the composite decision."""

    verdicts: dict[Component, ComponentVerdict]
    composite_allowed: bool
    composite_reason_codes: tuple[ReasonCode, ...] = ()
    envelope_reason_codes: tuple[ReasonCode, ...] = field(default_factory=tuple)

    def verdict(self, component: Component) -> ComponentVerdict:
        return self.verdicts[component]

    def all_reason_codes(self) -> tuple[ReasonCode, ...]:
        """Every distinct reason, in a stable order for the response envelope."""
        seen: dict[ReasonCode, None] = {}
        for verdict in self.verdicts.values():
            for code in verdict.reason_codes:
                seen.setdefault(code, None)
        for code in self.composite_reason_codes:
            seen.setdefault(code, None)
        for code in self.envelope_reason_codes:
            seen.setdefault(code, None)
        return tuple(seen)


def _scope_blockers(bundle: RiskInputBundle, policy: RiskPolicy) -> list[ReasonCode]:
    """Blockers that poison every component equally."""
    blockers: list[ReasonCode] = []
    coverage = bundle.coverage

    if policy.gates.require_epoch_active and not coverage.epoch_active:
        blockers.append(ReasonCode.SOURCE_NOT_ACTIVE)

    recon = bundle.recon
    if policy.gates.require_recon_evidence:
        if recon is None:
            # No evidence is not clean evidence. Project 04 exists precisely
            # because the projections can disagree with the source.
            blockers.append(ReasonCode.RECON_EVIDENCE_SCOPE_UNKNOWN)
        elif not recon.scope_known:
            blockers.append(ReasonCode.RECON_EVIDENCE_SCOPE_UNKNOWN)
        elif not recon.covers(bundle.scope.account_ids):
            # Verification of a different set of accounts says nothing here.
            blockers.append(ReasonCode.RECON_EVIDENCE_SCOPE_UNKNOWN)
        elif recon.status in recon.BLOCKING_STATUSES:
            blockers.append(
                ReasonCode.RECON_INCONCLUSIVE
                if recon.status == "INCONCLUSIVE"
                else ReasonCode.RECON_BLOCKING
            )
        elif recon.status not in recon.CLEAN_STATUSES:
            # An unrecognised status from a newer Project 04 build blocks by
            # default rather than being optimistically ignored (WF5-271).
            if policy.gates.unknown_recon_status_blocks:
                blockers.append(ReasonCode.RECON_BLOCKING)
        elif recon.blocking_accounts & bundle.scope.account_ids:
            blockers.append(ReasonCode.RECON_BLOCKING)

    if not bundle.accounts:
        blockers.append(ReasonCode.P03_UNPUBLISHED)
    elif policy.gates.require_position_scan_published and not coverage.account_scan_published:
        blockers.append(ReasonCode.P03_UNPUBLISHED)
    else:
        currencies = bundle.account_currencies
        if len(currencies) > 1:
            blockers.append(ReasonCode.CURRENCY_MISMATCH)
        elif not policy.supports_currency(next(iter(currencies))):
            blockers.append(ReasonCode.INVALID_ACCOUNT_CURRENCY)

    return blockers


def _position_component_blockers(bundle: RiskInputBundle, policy: RiskPolicy) -> list[ReasonCode]:
    """Blockers specific to anything derived from the position scan."""
    blockers: list[ReasonCode] = []
    coverage = bundle.coverage
    if policy.gates.require_position_scan_published and not coverage.position_scan_published:
        blockers.append(ReasonCode.P03_UNPUBLISHED)
    if not coverage.position_scan_terminal:
        # An empty scan is a valid zero only when its last page was received.
        # Without the terminal receipt, "no positions" and "pagination died"
        # are indistinguishable (WF5-031 vs WF5-032).
        blockers.append(ReasonCode.P03_UNPUBLISHED)

    return blockers


def _position_unsupported_reasons(bundle: RiskInputBundle) -> list[ReasonCode]:
    """Symbols among the open positions that this build cannot value.

    Separate from the blockers because the distinction is reported: ``BLOCKED``
    means the data is untrustworthy, ``UNSUPPORTED`` means the data is fine and
    the feature is not built. Conflating them tells an operator to go looking
    for a data problem that does not exist (section 4.2).
    """
    reasons: list[ReasonCode] = []
    open_symbols = {p.symbol for p in bundle.positions if p.is_open}
    unknown = sorted(s for s in open_symbols if s not in bundle.catalog)
    if unknown:
        reasons.append(ReasonCode.UNSUPPORTED_SYMBOL)
    unsupported_mode = sorted(
        s
        for s in open_symbols
        if s in bundle.catalog
        and bundle.catalog.get(s).calc_mode not in SUPPORTED_CALC_MODES
    )
    mismatched_profit = sorted(
        s
        for s in open_symbols
        if s in bundle.catalog
        and bundle.catalog.get(s).calc_mode in SUPPORTED_CALC_MODES
        and bundle.catalog.get(s).profit_currency != bundle.catalog.get(s).quote_currency
    )
    if unsupported_mode or mismatched_profit:
        reasons.append(ReasonCode.UNSUPPORTED_CALC_MODE)
    return reasons


def _realized_blockers(bundle: RiskInputBundle, policy: RiskPolicy) -> list[ReasonCode]:
    blockers: list[ReasonCode] = []
    coverage = bundle.coverage
    if policy.gates.require_deal_watermark_terminal and not coverage.deal_watermark_terminal:
        blockers.append(ReasonCode.DEAL_WATERMARK_UNPROVEN)
    if policy.gates.require_deal_watermark_unfiltered and not coverage.deal_watermark_unfiltered:
        # A watermark derived from an account-filtered scan cannot support a
        # claim of global completeness (WF5-004).
        blockers.append(ReasonCode.DEAL_WATERMARK_UNPROVEN)
    if bundle.cut.deal_watermark_seq is None:
        blockers.append(ReasonCode.DEAL_WATERMARK_UNPROVEN)
    return blockers


def _unrealized_verdict(
    bundle: RiskInputBundle,
    policy: RiskPolicy,
    *,
    scope_blockers: list[ReasonCode],
    position_blockers: list[ReasonCode],
    unsupported_reasons: list[ReasonCode],
    historical: bool,
) -> ComponentVerdict:
    reasons: list[ReasonCode] = [*scope_blockers, *position_blockers]
    quotes = bundle.quotes

    # Unsupported outranks a missing quote: there is no point reporting that a
    # price is absent for an instrument this build could not value anyway.
    if unsupported_reasons:
        return ComponentVerdict(
            Component.UNREALIZED,
            ComponentStatus.UNSUPPORTED,
            tuple(dict.fromkeys([*reasons, *unsupported_reasons])),
        )

    if quotes is None or not bundle.coverage.quote_snapshot_pinned:
        reasons.append(ReasonCode.QUOTE_MISSING)
        return ComponentVerdict(Component.UNREALIZED, ComponentStatus.BLOCKED, tuple(dict.fromkeys(reasons)))

    open_symbols = {p.symbol for p in bundle.positions if p.is_open}
    missing = sorted(open_symbols - set(quotes.quotes))
    if missing:
        # One un-priced symbol makes the portfolio total incomplete. A partial
        # total labelled READY is worse than no total (WF5-127).
        reasons.append(ReasonCode.QUOTE_MISSING)

    # The quote snapshot has to be tied to the position scan it values. Without
    # that binding the two are simply two unrelated observations (WF5-029).
    if policy.gates.require_cut_proof_for_composite and open_symbols:
        bound = quotes.bound_position_scan_id
        if bound is not None and bound != bundle.cut.position_scan_id:
            reasons.append(ReasonCode.QUOTE_CUT_UNPROVEN)
        elif bound is None and bundle.cut.cut_proof_id is None:
            reasons.append(ReasonCode.QUOTE_CUT_UNPROVEN)

    if reasons:
        return ComponentVerdict(Component.UNREALIZED, ComponentStatus.BLOCKED, tuple(dict.fromkeys(reasons)))

    as_of = bundle.valuation_as_of
    if as_of is None:
        return ComponentVerdict(
            Component.UNREALIZED, ComponentStatus.BLOCKED, (ReasonCode.QUOTE_CUT_UNPROVEN,)
        )

    threshold = policy.quotes.threshold_for(historical=historical)
    stale = False
    for symbol in sorted(open_symbols):
        quote = quotes.quotes[symbol]
        age = quote_age_seconds(quote, as_of=as_of)
        if age < 0 and policy.quotes.reject_lookahead:
            return ComponentVerdict(
                Component.UNREALIZED, ComponentStatus.BLOCKED, (ReasonCode.QUOTE_LOOKAHEAD,)
            )
        if quote.feed_status != "ACTIVE":
            stale = True
        if threshold is not None and age > threshold:
            stale = True

    if stale:
        return ComponentVerdict(Component.UNREALIZED, ComponentStatus.STALE, (ReasonCode.STALE_MARK,))
    return ComponentVerdict(Component.UNREALIZED, ComponentStatus.READY)


def evaluate_gates(
    bundle: RiskInputBundle,
    policy: RiskPolicy,
    *,
    requested: frozenset[Component],
    strict_composite: bool,
    historical: bool = False,
) -> GateReport:
    """Decide, per component, whether the arithmetic may run.

    Args:
        bundle: The captured, validated inputs.
        policy: The pinned policy whose gate switches apply.
        requested: Components the caller asked for.
        strict_composite: Whether the caller demands a portfolio-level answer.
        historical: Whether this is an as-of report rather than a live one;
            selects the historical freshness policy.

    Returns:
        A :class:`GateReport`. Components not requested are absent from it.
    """
    scope_blockers = _scope_blockers(bundle, policy)
    position_blockers = _position_component_blockers(bundle, policy)
    unsupported_reasons = _position_unsupported_reasons(bundle)
    verdicts: dict[Component, ComponentVerdict] = {}

    if Component.EXPOSURE in requested:
        # An unsupported instrument among the open positions makes the whole
        # exposure report unsupported rather than silently short of one symbol:
        # an understated book reads as a safe book (WF5-156).
        if unsupported_reasons and not (scope_blockers or position_blockers):
            verdicts[Component.EXPOSURE] = ComponentVerdict(
                Component.EXPOSURE,
                ComponentStatus.UNSUPPORTED,
                tuple(dict.fromkeys(unsupported_reasons)),
            )
        else:
            reasons = tuple(dict.fromkeys([*scope_blockers, *position_blockers]))
            verdicts[Component.EXPOSURE] = ComponentVerdict(
                Component.EXPOSURE,
                ComponentStatus.BLOCKED if reasons else ComponentStatus.READY,
                reasons,
            )

    if Component.REALIZED in requested:
        reasons = tuple(dict.fromkeys([*scope_blockers, *_realized_blockers(bundle, policy)]))
        verdicts[Component.REALIZED] = ComponentVerdict(
            Component.REALIZED,
            ComponentStatus.BLOCKED if reasons else ComponentStatus.READY,
            reasons,
        )

    if Component.UNREALIZED in requested:
        verdicts[Component.UNREALIZED] = _unrealized_verdict(
            bundle,
            policy,
            scope_blockers=scope_blockers,
            position_blockers=position_blockers,
            unsupported_reasons=unsupported_reasons,
            historical=historical,
        )

    # A composite total needs more than healthy components: it needs proof that
    # they describe one instant. Without a cut proof the cuts are INDEPENDENT,
    # and adding numbers across independent cuts invents a portfolio.
    composite_reasons: list[ReasonCode] = []
    composite_allowed = all(v.status is ComponentStatus.READY for v in verdicts.values())
    if composite_allowed and len(requested) > 1:
        if policy.gates.require_cut_proof_for_composite:
            if bundle.cut.consistency is not Consistency.COMPATIBLE:
                composite_allowed = False
                composite_reasons.append(ReasonCode.INCOMPATIBLE_CUT)
    elif not composite_allowed:
        composite_reasons.append(ReasonCode.INCOMPATIBLE_CUT)

    envelope: list[ReasonCode] = []
    if not bundle.coverage.fee_fields_available:
        # Disclosure, not a blocker: the realized figures are correct for what
        # they cover, and the envelope states what they do not cover.
        envelope.append(ReasonCode.FEE_COVERAGE_UNAVAILABLE)

    if strict_composite and not composite_allowed and ReasonCode.INCOMPATIBLE_CUT not in composite_reasons:
        composite_reasons.append(ReasonCode.INCOMPATIBLE_CUT)

    return GateReport(
        verdicts=verdicts,
        composite_allowed=composite_allowed,
        composite_reason_codes=tuple(dict.fromkeys(composite_reasons)),
        envelope_reason_codes=tuple(envelope),
    )

