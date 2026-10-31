"""The P05 accounting boundary. Section 7, what-if domains M, N, Y."""

from __future__ import annotations

from decimal import Decimal

import pytest

from analytics.contracts.policy import (
    AccountingPolicy,
    FeeCoverage,
    PolicyError,
    SymbolCatalog,
    SymbolSpec,
    default_p0_policy,
)
from analytics.errors import Reason

pytestmark = pytest.mark.unit


@pytest.fixture
def policy() -> AccountingPolicy:
    return default_p0_policy()


class TestCurrencyPolicy:
    def test_accepts_declared_currencies(self, policy):
        assert policy.require_currency("USD") == "USD"
        assert policy.require_currency("EUR") == "EUR"

    def test_rejects_undeclared_currency(self, policy):
        with pytest.raises(PolicyError) as excinfo:
            policy.require_currency("JPY")
        assert excinfo.value.reason is Reason.CURRENCY_UNSUPPORTED

    def test_p0_refuses_fx_conversion(self, policy):
        """FR08-12: conversion needs a versioned rate set, which P0 lacks."""
        with pytest.raises(PolicyError) as excinfo:
            policy.convert(Decimal("1.00"), "EUR", "USD")
        assert excinfo.value.reason is Reason.FX_CONVERSION_UNSUPPORTED

    def test_identity_conversion_is_a_no_op(self, policy):
        assert policy.convert(Decimal("1.00"), "USD", "USD") == Decimal("1.00")

    def test_enabling_the_flag_still_does_not_invent_a_rate(self):
        """A rate-set id is not a rate. P08-M."""
        enabled = AccountingPolicy(
            version="p1-fx",
            fee_coverage=FeeCoverage.COMMISSION_AND_SWAP_ONLY,
            supported_currencies=frozenset({"USD", "EUR"}),
            symbol_catalog=default_p0_policy().symbol_catalog,
            fx_conversion_enabled=True,
            fx_rate_set_id="rates-2026-10-30",
        )
        with pytest.raises(PolicyError) as excinfo:
            enabled.convert(Decimal("1.00"), "EUR", "USD")
        assert excinfo.value.reason is Reason.FX_CONVERSION_UNSUPPORTED

    def test_fx_flag_without_rate_set_is_a_configuration_error(self):
        with pytest.raises(PolicyError):
            AccountingPolicy(
                version="broken",
                fee_coverage=FeeCoverage.GROSS_ONLY,
                supported_currencies=frozenset({"USD"}),
                symbol_catalog=default_p0_policy().symbol_catalog,
                fx_conversion_enabled=True,
                fx_rate_set_id=None,
            )


class TestSymbolCatalog:
    def test_unknown_symbol_is_refused(self, policy):
        with pytest.raises(PolicyError) as excinfo:
            policy.symbol_catalog.require("DOGEUSD")
        assert excinfo.value.reason is Reason.SYMBOL_UNKNOWN

    def test_disabled_symbol_is_refused(self):
        catalog = SymbolCatalog(
            version="v1",
            symbols={"OFF": SymbolSpec("OFF", "USD", "LOT", enabled=False)},
        )
        with pytest.raises(PolicyError) as excinfo:
            catalog.require("OFF")
        assert excinfo.value.reason is Reason.SYMBOL_UNKNOWN

    def test_quote_currency_is_not_inferred_from_the_name(self, policy):
        """``EURUSD`` does not imply the Deal settles in USD.

        Section 8.4 forbids the "assume USD" shortcut, so the currency comes
        from the catalogue entry and the Deal, never from string slicing.
        """
        spec = policy.symbol_catalog.require("USDJPY")
        assert spec.quote_currency == "JPY"


class TestFeeCoverage:
    def test_unknown_coverage_blocks_any_net(self):
        policy = AccountingPolicy(
            version="unknown-fees",
            fee_coverage=FeeCoverage.UNKNOWN,
            supported_currencies=frozenset({"USD"}),
            symbol_catalog=default_p0_policy().symbol_catalog,
        )
        net, label = policy.net_realized(Decimal("10"), Decimal("-1"), Decimal("0"))
        assert net is None
        assert label == "NET_UNAVAILABLE_COVERAGE_UNKNOWN"

    def test_gross_only_blocks_any_net(self):
        policy = AccountingPolicy(
            version="gross-only",
            fee_coverage=FeeCoverage.GROSS_ONLY,
            supported_currencies=frozenset({"USD"}),
            symbol_catalog=default_p0_policy().symbol_catalog,
        )
        net, label = policy.net_realized(Decimal("10"), Decimal("-1"), Decimal("0"))
        assert net is None
        assert label == "NET_UNAVAILABLE_FEES_NOT_MODELLED"

    def test_partial_coverage_is_labelled_not_called_net(self, policy):
        """A components subtotal must not be presentable as an economic net."""
        net, label = policy.net_realized(Decimal("12.30"), Decimal("-0.50"), Decimal("-0.10"))
        assert net == Decimal("11.70")
        assert label == "NET_COMPONENTS_COVERED"
        assert label != "NET_FULL"

    def test_full_coverage_is_labelled_net_full(self):
        policy = AccountingPolicy(
            version="all-fees",
            fee_coverage=FeeCoverage.ALL_FEES_INCLUDED,
            supported_currencies=frozenset({"USD"}),
            symbol_catalog=default_p0_policy().symbol_catalog,
        )
        net, label = policy.net_realized(Decimal("12.30"), Decimal("-0.50"), Decimal("-0.10"))
        assert net == Decimal("11.70")
        assert label == "NET_FULL"

    def test_event_cannot_claim_broader_coverage_than_the_policy(self, policy):
        """G08-20 inverse: one event must not widen every total it joins."""
        with pytest.raises(PolicyError) as excinfo:
            policy.validate_declared_coverage(str(FeeCoverage.ALL_FEES_INCLUDED))
        assert excinfo.value.reason is Reason.FEE_COVERAGE_UNKNOWN

    def test_event_declaring_unknown_is_refused(self, policy):
        with pytest.raises(PolicyError) as excinfo:
            policy.validate_declared_coverage(str(FeeCoverage.UNKNOWN))
        assert excinfo.value.reason is Reason.FEE_COVERAGE_UNKNOWN

    def test_unrecognised_coverage_string_is_refused(self, policy):
        with pytest.raises(PolicyError) as excinfo:
            policy.validate_declared_coverage("MOSTLY_COMPLETE")
        assert excinfo.value.reason is Reason.FEE_COVERAGE_UNKNOWN

    def test_matching_coverage_is_accepted(self, policy):
        assert (
            policy.validate_declared_coverage(str(FeeCoverage.COMMISSION_AND_SWAP_ONLY))
            is FeeCoverage.COMMISSION_AND_SWAP_ONLY
        )

    def test_narrower_declaration_is_accepted(self, policy):
        assert (
            policy.validate_declared_coverage(str(FeeCoverage.GROSS_ONLY))
            is FeeCoverage.GROSS_ONLY
        )

    @pytest.mark.parametrize(
        "coverage,full,partial",
        [
            (FeeCoverage.ALL_FEES_INCLUDED, True, True),
            (FeeCoverage.COMMISSION_AND_SWAP_ONLY, False, True),
            (FeeCoverage.GROSS_ONLY, False, False),
            (FeeCoverage.UNKNOWN, False, False),
        ],
    )
    def test_coverage_permissions(self, coverage, full, partial):
        assert coverage.permits_full_net is full
        assert coverage.permits_partial_net is partial


class TestP0Policy:
    def test_p0_defaults_match_the_design(self, policy):
        """Section 0.2: USD/EUR kept apart, no conversion, fees limited."""
        assert policy.version == "usd-fx-synthetic-v1"
        assert policy.fee_coverage is FeeCoverage.COMMISSION_AND_SWAP_ONLY
        assert policy.supported_currencies == frozenset({"USD", "EUR"})
        assert policy.fx_conversion_enabled is False
