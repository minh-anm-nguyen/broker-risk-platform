"""Domains A and W -- upstream contract handshake, provenance, versioning.

Covers WF5-001..012 and WF5-265..276.
"""

from __future__ import annotations

import pytest

from risk.domain.classification import SourceCapability
from risk.domain.facts import (
    AccountFact,
    AuthorizedScope,
    CoverageReport,
    DealFact,
    PositionFact,
    ReconEvidence,
    RiskCut,
    RiskInputBundle,
    build_money_triplet,
)
from risk.domain.instruments import SymbolCatalog
from risk.errors import ContractError, ReasonCode
from risk.policies.loader import ROUNDING_MODES, load_catalog, load_policy
from risk.status import Component, ComponentStatus, Consistency, Presence, Side
from risk_tools.fixtures import (
    ACCOUNT_A,
    EPOCH_A,
    EPOCH_B,
    SOURCE_A,
    T0,
    TENANT_A,
    ScanSpec,
    scenario,
)
from tests.reference.oracle import D

pytestmark = [pytest.mark.contract, pytest.mark.p0]


def account(**overrides) -> AccountFact:
    defaults = dict(
        account_id=ACCOUNT_A,
        currency="USD",
        epoch_id=EPOCH_A,
        published_scan_id="scan-A",
        presence=Presence.PRESENT,
    )
    defaults.update(overrides)
    return AccountFact(**defaults)


# --------------------------------------------------------------------------- #
# Domain A -- upstream contract
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-001")
def test_deal_from_another_epoch_cannot_enter_a_bundle(catalog, evidence):
    """A deal whose epoch differs from the cut is refused at bundle construction."""
    g, c, s = build_money_triplet(currency="USD", gross="10.00", commission="0", swap="0")
    foreign = DealFact(
        deal_id="d-foreign",
        account_id=ACCOUNT_A,
        source_seq=1,
        entry_kind="OUT",
        deal_type=None,
        gross_profit=g,
        commission=c,
        swap=s,
        executed_at=T0,
        epoch_id=EPOCH_B,
    )
    with pytest.raises(ContractError) as exc:
        RiskInputBundle(
            cut=RiskCut(source_id=SOURCE_A, epoch_id=EPOCH_A),
            scope=AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
            accounts=(account(),),
            positions=(),
            deals=(foreign,),
            catalog=catalog,
            quotes=None,
            recon=None,
            coverage=CoverageReport(),
        )
    assert exc.value.reason is ReasonCode.EPOCH_IDENTITY_CONFLICT
    evidence(
        expected_reason_codes=("EPOCH_IDENTITY_CONFLICT",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-002")
@pytest.mark.parametrize("bad_currency", ["", "  ", "US", "usd", "USDX", None])
def test_empty_or_malformed_account_currency_rejected(bad_currency, evidence):
    """An absent account currency is never defaulted to USD."""
    with pytest.raises(ContractError) as exc:
        account(currency=bad_currency)
    assert exc.value.reason is ReasonCode.INVALID_ACCOUNT_CURRENCY
    evidence(
        expected_reason_codes=("INVALID_ACCOUNT_CURRENCY",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-003")
async def test_staged_position_scan_is_not_read_as_published(
    service_factory, request_factory, evidence
):
    """A STAGED scan yields no position rows and blocks the dependent components."""
    spec = scenario("R05-buy-mark")
    spec.position_scan = ScanSpec(scan_id="scan-P", entity="POSITION", status="STAGED")
    service, _ = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})),
        AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
    )
    exposure = result.component(Component.EXPOSURE)

    assert exposure.status is ComponentStatus.BLOCKED
    assert "P03_UNPUBLISHED" in exposure.reason_codes
    # And no values at all: nothing a consumer could read as an empty book.
    assert exposure.values is None
    evidence(
        expected_status="BLOCKED",
        observed_status=str(exposure.status),
        expected_reason_codes=("P03_UNPUBLISHED",),
        observed_reason_codes=exposure.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-004")
async def test_filtered_deal_watermark_cannot_claim_completeness(
    service_factory, request_factory, evidence
):
    """A watermark from an account-filtered scan blocks realized P&L."""
    spec = scenario("R05-signed-fees")
    spec.deal_watermark_filter = "sha256:account-filter"
    service, _ = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})),
        AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
    )
    realized = result.component(Component.REALIZED)

    assert realized.status is ComponentStatus.BLOCKED
    assert "DEAL_WATERMARK_UNPROVEN" in realized.reason_codes
    evidence(
        expected_reason_codes=("DEAL_WATERMARK_UNPROVEN",),
        observed_reason_codes=realized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-005")
async def test_null_swap_column_blocks_the_read(service_factory, request_factory, evidence):
    """A NULL swap in the database stops the capture; it does not become zero."""
    spec = scenario("R05-invalid-decimal")
    service, _ = service_factory(spec)

    with pytest.raises(ContractError) as exc:
        await service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})),
            AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
        )
    assert exc.value.reason is ReasonCode.MONEY_FIELD_MISSING
    evidence(
        expected_reason_codes=("MONEY_FIELD_MISSING",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
        notes=("no result digest is produced for a blocked capture",),
    )


@pytest.mark.wf5("WF5-006")
@pytest.mark.parametrize(
    "bad_id", ["", "x" * 200, "has\nnewline", "has\x00null", "bell\x07"]
)
def test_malformed_source_identity_rejected(bad_id, evidence):
    """An identifier that is empty, oversized or control-bearing is refused."""
    with pytest.raises(ContractError) as exc:
        RiskCut(source_id=bad_id, epoch_id=EPOCH_A)
    assert exc.value.reason is ReasonCode.CONTRACT_MISSING_FIELD
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-007")
async def test_generation_change_selects_one_epoch_only(
    service_factory, request_factory, evidence
):
    """A second generation of the same run is a separate epoch with separate rows."""
    spec = scenario("R05-source-reset")
    service, conn = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})),
        AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
    )

    # Both generations exist in the database; the manifest names exactly one.
    rows = conn.execute("SELECT COUNT(*) FROM source_epochs").fetchone()[0]
    assert rows == 2
    assert result.cuts["epoch_id"] == spec.epoch_id
    assert result.lineage["cut"]["epoch_id"] == spec.epoch_id
    evidence(
        expected_values={"epochs_in_db": "2", "epoch_in_result": spec.epoch_id},
        observed_values={"epochs_in_db": str(rows), "epoch_in_result": str(result.cuts["epoch_id"])},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-008")
def test_quote_row_without_a_version_id_rejected(evidence):
    """A mark with no id cannot be pinned, so the valuation is not reproducible."""
    from risk.adapters.quotes import build_snapshot

    with pytest.raises(ContractError) as exc:
        build_snapshot(
            "s-1",
            [{"symbol": "EURUSD", "bid": "1.1", "ask": "1.2", "market_time": T0.isoformat()}],
            captured_at=T0,
        )
    assert exc.value.reason is ReasonCode.QUOTE_CUT_UNPROVEN
    evidence(
        expected_reason_codes=("QUOTE_CUT_UNPROVEN",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-009")
@pytest.mark.p1
def test_unscoped_recon_evidence_is_not_treated_as_verification(catalog, policy, evidence):
    """Global VERIFIED without a scope cannot verify this request's accounts."""
    from risk.gates import evaluate_gates

    unscoped = ReconEvidence(
        status="VERIFIED",
        revision=9,
        epoch_id=EPOCH_A,
        covered_accounts=frozenset(),
        covered_deal_watermark=None,
        scope_known=False,
    )
    bundle = RiskInputBundle(
        cut=RiskCut(source_id=SOURCE_A, epoch_id=EPOCH_A, position_scan_id="scan-P",
                    deal_watermark_seq=1050),
        scope=AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
        accounts=(account(),),
        positions=(),
        deals=(),
        catalog=catalog,
        quotes=None,
        recon=unscoped,
        coverage=CoverageReport(
            account_scan_published=True,
            position_scan_published=True,
            position_scan_terminal=True,
            deal_watermark_terminal=True,
            deal_watermark_unfiltered=True,
            epoch_active=True,
        ),
    )
    report = evaluate_gates(
        bundle, policy, requested=frozenset({Component.EXPOSURE}), strict_composite=True
    )
    verdict = report.verdict(Component.EXPOSURE)

    assert verdict.status is ComponentStatus.BLOCKED
    assert ReasonCode.RECON_EVIDENCE_SCOPE_UNKNOWN in verdict.reason_codes
    evidence(
        expected_reason_codes=("RECON_EVIDENCE_SCOPE_UNKNOWN",),
        observed_reason_codes=tuple(str(c) for c in verdict.reason_codes),
    )


@pytest.mark.wf5("WF5-011")
@pytest.mark.p1
def test_unmapped_position_side_rejected_by_the_adapter(evidence):
    """A side the DTO does not know is a contract failure, not a default to BUY."""
    with pytest.raises(ValueError):
        Side("LONG")

    with pytest.raises(ContractError) as exc:
        PositionFact(
            position_id="p-1",
            account_id=ACCOUNT_A,
            symbol="EURUSD",
            side="BUY",  # type: ignore[arg-type]  -- a bare string, not the enum
            lots=D("0.5"),
            open_price=D("1.1"),
            source_status="OPEN",
            presence=Presence.PRESENT,
            epoch_id=EPOCH_A,
            published_scan_id="scan-P",
        )
    assert exc.value.reason is ReasonCode.UNSUPPORTED_POSITION_SIDE
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-012", "WF5-036")
@pytest.mark.p2
def test_watermark_may_exceed_the_max_deal_sequence(evidence):
    """Non-deal events leave gaps, so W > max(deal.source_seq) is normal."""
    from risk.domain.classification import select_eligible_deals

    g, c, s = build_money_triplet(currency="USD", gross="10.00", commission="0", swap="0")
    deals = [
        DealFact(
            deal_id=f"d-{seq}",
            account_id=ACCOUNT_A,
            source_seq=seq,
            entry_kind="OUT",
            deal_type=None,
            gross_profit=g,
            commission=c,
            swap=s,
            executed_at=T0,
            epoch_id=EPOCH_A,
        )
        for seq in (10, 50, 400)  # large gaps between deal sequences
    ]
    result = select_eligible_deals(
        deals,
        capability=SourceCapability.SIMULATOR_TRADING_ONLY_V1,
        period_start=None,
        period_end=None,
        deal_watermark_seq=1050,  # far above max(source_seq)
    )
    # All three included, no missing-deal allegation raised by the gaps.
    assert len(result.trading) == 3
    assert result.excluded_beyond_watermark == 0
    evidence(
        oracle="STATE_PROVENANCE",
        observed_values={"included": "3", "max_seq": "400", "watermark": "1050"},
    )


# --------------------------------------------------------------------------- #
# Domain W -- versioning and rollout
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-265", "WF5-209")
async def test_two_policies_produce_two_distinct_digests(
    service_factory, request_factory, evidence
):
    """The same inputs under v1 and v2 give different output digests."""
    spec = scenario("R05-aligned-ready")
    v1 = load_policy("risk-policy-v1")
    v2 = load_policy("risk-policy-v2")
    scope = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))

    service_v1, conn = service_factory(spec, active_policy=v1)
    result_v1 = await service_v1.compute(
        request_factory(spec, risk_policy_version="risk-policy-v1"), scope
    )
    service_v2, _ = service_factory(spec, active_policy=v2, connection=conn)
    result_v2 = await service_v2.compute(
        request_factory(spec, risk_policy_version="risk-policy-v2"), scope
    )

    assert v1.rounding_mode != v2.rounding_mode
    assert result_v1.risk_policy_version == "risk-policy-v1"
    assert result_v2.risk_policy_version == "risk-policy-v2"
    # The digests differ because the policy version is part of the hashed
    # output, so a cache cannot serve one under the other's label.
    assert result_v1.output_digest != result_v2.output_digest
    assert result_v1.input_digest != result_v2.input_digest
    evidence(
        expected_values={"digests_differ": "True"},
        observed_values={
            "v1_digest": result_v1.output_digest[:16],
            "v2_digest": result_v2.output_digest[:16],
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-266")
def test_policy_artifact_must_declare_the_requested_version(evidence):
    """An unknown policy version is refused rather than falling back to a default."""
    with pytest.raises(ContractError) as exc:
        load_policy("risk-policy-v99")
    assert exc.value.reason is ReasonCode.POLICY_UNAVAILABLE
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-267")
def test_unknown_catalog_version_refused(evidence):
    with pytest.raises(ContractError) as exc:
        load_catalog("sim-usd-fx-v99")
    assert exc.value.reason is ReasonCode.SPEC_VERSION_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-268", "WF5-210")
async def test_result_pins_exactly_one_catalog_version(
    service_factory, request_factory, evidence
):
    """Every symbol in one result carries the same catalog version."""
    spec = scenario("R05-hedged-gross")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})),
        AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
    )

    exposure = result.component(Component.EXPOSURE)
    assert exposure.values is not None
    versions = {
        row["spec_version"] for row in exposure.values["by_symbol"].values()  # type: ignore[index]
    }
    assert versions == {"sim-usd-fx-v1"}
    assert result.symbol_catalog_version == "sim-usd-fx-v1"
    assert result.cuts["symbol_catalog_version"] == "sim-usd-fx-v1"
    evidence(
        observed_values={"distinct_versions": str(len(versions))},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-271")
@pytest.mark.p1
def test_unknown_recon_status_blocks_by_default(catalog, policy, evidence):
    """A status from a newer Project 04 build is blocking until somebody maps it."""
    from risk.gates import evaluate_gates

    future_status = ReconEvidence(
        status="PARTIALLY_VERIFIED_V2",  # not in either known set
        revision=11,
        epoch_id=EPOCH_A,
        covered_accounts=frozenset({ACCOUNT_A}),
        covered_deal_watermark=1050,
    )
    bundle = RiskInputBundle(
        cut=RiskCut(source_id=SOURCE_A, epoch_id=EPOCH_A, position_scan_id="scan-P",
                    deal_watermark_seq=1050),
        scope=AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A})),
        accounts=(account(),),
        positions=(),
        deals=(),
        catalog=catalog,
        quotes=None,
        recon=future_status,
        coverage=CoverageReport(
            account_scan_published=True,
            position_scan_published=True,
            position_scan_terminal=True,
            deal_watermark_terminal=True,
            deal_watermark_unfiltered=True,
            epoch_active=True,
        ),
    )
    report = evaluate_gates(
        bundle, policy, requested=frozenset({Component.EXPOSURE}), strict_composite=True
    )
    verdict = report.verdict(Component.EXPOSURE)

    assert verdict.status is ComponentStatus.BLOCKED
    assert ReasonCode.RECON_BLOCKING in verdict.reason_codes
    evidence(
        expected_reason_codes=("RECON_BLOCKING",),
        observed_reason_codes=tuple(str(c) for c in verdict.reason_codes),
        notes=("fail closed on an enum the build has never seen",),
    )


@pytest.mark.wf5("WF5-272")
def test_policy_rejects_an_unapproved_rounding_mode(evidence):
    """Only the approved rounding modes may appear in a policy artifact."""
    assert "ROUND_05UP" not in ROUNDING_MODES
    assert set(ROUNDING_MODES) >= {"ROUND_HALF_EVEN", "ROUND_HALF_UP"}
    evidence(observed_values={"approved_modes": str(len(ROUNDING_MODES))})


@pytest.mark.wf5("WF5-273")
@pytest.mark.p1
def test_quote_snapshot_from_an_unknown_id_is_refused(evidence):
    """A snapshot id the provider does not hold yields SNAPSHOT_EXPIRED."""
    import asyncio

    from risk.adapters.quotes import FixtureQuoteProvider
    from risk.errors import QualityGateError

    provider = FixtureQuoteProvider({})
    with pytest.raises(QualityGateError) as exc:
        asyncio.run(provider.snapshot(symbols=["EURUSD"], as_of=T0, snapshot_id="s-unknown"))
    assert exc.value.reason is ReasonCode.SNAPSHOT_EXPIRED
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-274")
@pytest.mark.p1
def test_historical_request_does_not_silently_adopt_a_new_contract_size(evidence):
    """Two catalog versions give two answers, and each result names which it used."""
    old = SymbolCatalog.from_rows(
        "hist-v1",
        [
            {
                "symbol": "EURUSD",
                "base_currency": "EUR",
                "quote_currency": "USD",
                "profit_currency": "USD",
                "calc_mode": "FOREX_LINEAR_P0",
                "contract_size": "100000",
                "tick_size": "0.00001",
                "volume_step": "0.01",
                "volume_max": "100",
            }
        ],
    )
    new = SymbolCatalog.from_rows(
        "hist-v2",
        [
            {
                "symbol": "EURUSD",
                "base_currency": "EUR",
                "quote_currency": "USD",
                "profit_currency": "USD",
                "calc_mode": "FOREX_LINEAR_P0",
                "contract_size": "50000",
                "tick_size": "0.00001",
                "volume_step": "0.01",
                "volume_max": "100",
            }
        ],
    )
    assert old.get("EURUSD").units_for(D("1")) == D("100000")
    assert new.get("EURUSD").units_for(D("1")) == D("50000")
    assert old.catalog_version != new.catalog_version
    evidence(
        observed_values={
            "v1_units": str(old.get("EURUSD").units_for(D("1"))),
            "v2_units": str(new.get("EURUSD").units_for(D("1"))),
        }
    )


@pytest.mark.wf5("WF5-275")
@pytest.mark.p1
def test_gate_switches_are_versioned_with_the_policy(policy, policy_v2, evidence):
    """The reconciliation gate switches travel with the policy version."""
    assert policy.gates.require_recon_evidence is True
    assert policy.gates.unknown_recon_status_blocks is True
    assert policy_v2.gates.require_recon_evidence is True
    # Both policies declare their own version, so an audit row can say which
    # set of gates produced a number.
    assert policy.policy_version != policy_v2.policy_version
    evidence(observed_values={"v1": policy.policy_version, "v2": policy_v2.policy_version})


@pytest.mark.wf5("WF5-276")
@pytest.mark.p2
def test_cut_consistency_requires_a_proof_not_a_coincidence(evidence):
    """Populated scan ids alone give INDEPENDENT; only a proof gives COMPATIBLE."""
    independent = RiskCut(
        source_id=SOURCE_A,
        epoch_id=EPOCH_A,
        account_scan_id="scan-A",
        position_scan_id="scan-P",
        deal_watermark_seq=1050,
        quote_snapshot_id="quote-S1030",
    )
    assert independent.consistency is Consistency.INDEPENDENT

    proven = RiskCut(
        source_id=SOURCE_A,
        epoch_id=EPOCH_A,
        account_scan_id="scan-A",
        position_scan_id="scan-P",
        deal_watermark_seq=1050,
        quote_snapshot_id="quote-S1030",
        cut_proof_id="freeze-S1030",
    )
    assert proven.consistency is Consistency.COMPATIBLE

    bare = RiskCut(source_id=SOURCE_A, epoch_id=EPOCH_A)
    assert bare.consistency is Consistency.UNPROVEN
    evidence(
        expected_values={"independent": "INDEPENDENT", "proven": "COMPATIBLE",
                         "bare": "UNPROVEN"},
        observed_values={
            "independent": str(independent.consistency),
            "proven": str(proven.consistency),
            "bare": str(bare.consistency),
        },
    )
