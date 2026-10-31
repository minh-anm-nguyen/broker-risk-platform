"""The remaining matrix cases: index budgets, DB errors, rollout, lifecycle.

Covers WF5-048, 058, 059, 060, 117, 130, 165, 191, 192, 223, 232, 235, 238,
269 and 270 -- the cases the domain-focused suites did not reach.

Several of them are about PostgreSQL behaviour (a serialization failure, a temp
file spill, replica lag). Where the condition can be reproduced faithfully
through the executor port it is; where it genuinely requires a server, the test
asserts the engine's *handling* of the error and records in its evidence that
the database-level trigger was simulated rather than provoked. That distinction
is the honest one, and ``risk-report`` shows it.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from risk.domain.facts import AuthorizedScope
from risk.policies.loader import load_policy
from risk.status import Component, ComponentStatus, GlobalStatus
from risk_tools.fixtures import (
    ACCOUNT_A,
    ACCOUNT_B,
    SOURCE_A,
    T0,
    TENANT_A,
    AccountSpec,
    DealSpec,
    PositionSpec,
    QuoteSpec,
    ScanSpec,
    quote_snapshot_for,
    scenario,
    synthetic_deals,
)
from tests.reference.oracle import D

pytestmark = [pytest.mark.integration]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))


def _iso(value) -> str:
    from datetime import UTC

    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


# --------------------------------------------------------------------------- #
# Domain D / E -- classification at scale, position scope and hedging
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-048")
@pytest.mark.p2
async def test_mixed_category_stream_reconciles_to_its_source_totals(
    service_factory, request_factory, evidence
):
    """A stream of trades, deposits and fee postings splits into exact category totals."""
    spec = scenario("R05-signed-fees")
    trades = [
        DealSpec(deal_id=f"d-trade-{i}", source_seq=2000 + i, entry_kind="OUT",
                 deal_type="DEAL_TYPE_SELL", gross="10.00", commission="-1.00",
                 swap="0.00", executed_at=T0 - timedelta(minutes=i))
        for i in range(30)
    ]
    deposits = [
        DealSpec(deal_id=f"d-dep-{i}", source_seq=3000 + i, entry_kind="IN",
                 deal_type="DEAL_TYPE_BALANCE", gross="100.00", commission="0.00",
                 swap="0.00", executed_at=T0 - timedelta(minutes=i))
        for i in range(10)
    ]
    fees = [
        DealSpec(deal_id=f"d-fee-{i}", source_seq=4000 + i, entry_kind="IN",
                 deal_type="DEAL_TYPE_COMMISSION_DAILY", gross="-2.00", commission="0.00",
                 swap="0.00", executed_at=T0 - timedelta(minutes=i))
        for i in range(5)
    ]
    spec.deals = tuple(trades + deposits + fees)
    spec.deal_watermark = 10_000

    base = load_policy()
    full_taxonomy = replace(base, source_capability=type(base.source_capability)(
        "FULL_DEAL_TAXONOMY_V1"
    ))
    service, _ = service_factory(spec, active_policy=full_taxonomy)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )
    realized = result.component(Component.REALIZED).values
    assert realized is not None

    # Independent expectations, one per category.
    assert D(str(realized["gross"])) == D("300.00")  # 30 x 10
    assert D(str(realized["commission"])) == D("-30.00")  # 30 x -1
    assert realized["deal_count"] == 30
    categories = realized["non_trading"]["category_totals"]  # type: ignore[index]
    assert D(categories["BALANCE_OPERATION"]["amount"]) == D("1000.00")
    assert D(categories["FEE_POSTING"]["amount"]) == D("-10.00")
    assert realized["non_trading"]["deal_count"] == 15  # type: ignore[index]
    evidence(
        oracle="GOLDEN_REFERENCE",
        expected_values={"trading_gross": "300.00", "deposits": "1000.00", "fees": "-10.00"},
        observed_values={
            "trading_gross": str(realized["gross"]),
            "deposits": str(categories["BALANCE_OPERATION"]["amount"]),
            "fees": str(categories["FEE_POSTING"]["amount"]),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-058")
@pytest.mark.p1
async def test_positions_outside_the_authorized_accounts_are_never_read(
    service_factory, request_factory, evidence
):
    """An account-scoped query cannot return another account's position."""
    spec = scenario("R05-buy-mark")
    spec.accounts = (AccountSpec(account_id=ACCOUNT_A), AccountSpec(account_id=ACCOUNT_B))
    spec.positions = (
        PositionSpec(position_id="p-mine", lots="0.50", account_id=ACCOUNT_A),
        PositionSpec(position_id="p-theirs", lots="9.00", account_id=ACCOUNT_B),
    )
    service, _ = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    exposure = result.component(Component.EXPOSURE).values
    assert exposure is not None

    # 0.50 lots only; the other account's nine lots never entered the process.
    assert exposure["by_symbol"]["EURUSD"]["net_units"] == "50000.00"  # type: ignore[index]
    assert result.lineage["row_counts"]["positions"] == 1
    evidence(
        oracle="OPERATIONAL",
        expected_values={"net_units": "50000.00", "rows_read": "1"},
        observed_values={
            "net_units": str(exposure["by_symbol"]["EURUSD"]["net_units"]),  # type: ignore[index]
            "rows_read": str(result.lineage["row_counts"]["positions"]),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-059")
@pytest.mark.p1
async def test_many_hedged_tickets_aggregate_by_symbol_preserving_gross(
    service_factory, request_factory, evidence
):
    """Twenty tickets on one symbol keep ticket-level gross in the symbol total."""
    spec = scenario("R05-buy-mark")
    spec.positions = tuple(
        PositionSpec(
            position_id=f"p-{i}",
            side="BUY" if i % 2 == 0 else "SELL",
            lots="0.50",
        )
        for i in range(20)
    )
    service, _ = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    eurusd = result.component(Component.EXPOSURE).values["by_symbol"]["EURUSD"]  # type: ignore[index]

    # Ten long and ten short half-lots: net zero, gross one million units.
    assert eurusd["net_units"] == "0.00"
    assert eurusd["gross_units"] == "1000000.00"
    assert eurusd["position_count"] == 20
    assert D(str(eurusd["gross_units"])) >= abs(D(str(eurusd["net_units"])))
    evidence(
        oracle="GOLDEN_REFERENCE",
        expected_values={"net": "0.00", "gross": "1000000.00", "tickets": "20"},
        observed_values={
            "net": str(eurusd["net_units"]),
            "gross": str(eurusd["gross_units"]),
            "tickets": str(eurusd["position_count"]),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-060")
@pytest.mark.p2
async def test_accounting_mode_change_needs_a_new_catalog_version(
    service_factory, request_factory, evidence
):
    """Aggregation semantics are pinned by the catalog version in the result."""
    spec = scenario("R05-hedged-gross")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    eurusd = result.component(Component.EXPOSURE).values["by_symbol"]["EURUSD"]  # type: ignore[index]

    # Hedging mode: both legs are preserved, and the version that produced
    # that interpretation is named in the result.
    assert eurusd["gross_units"] == "200000.00"
    assert eurusd["net_units"] == "0.00"
    assert eurusd["spec_version"] == "sim-usd-fx-v1"
    assert result.cuts["symbol_catalog_version"] == "sim-usd-fx-v1"
    evidence(
        oracle="STATE_PROVENANCE",
        observed_values={"spec_version": str(eurusd["spec_version"])},
        fixture_id=spec.name,
        notes=("a netting/hedging change would need a new catalog version to be visible",),
    )


# --------------------------------------------------------------------------- #
# Domain J / K / N / P -- coverage, cut mismatch, historical marks, spec expiry
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-117")
@pytest.mark.p1
async def test_fee_coverage_is_disclosed_in_the_envelope(
    service_factory, request_factory, evidence
):
    """Every realized result states what its net excludes, as a reason code."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    realized = result.component(Component.REALIZED).values
    assert realized is not None
    assert realized["fee_coverage"] == "NOT_AVAILABLE"
    assert "FEE_COVERAGE_UNAVAILABLE" in result.reason_codes
    # And the composite repeats the disclosure, so a consumer reading only the
    # total still sees it.
    assert result.composite_total is not None
    assert result.composite_total["fee_coverage"] == "NOT_AVAILABLE"
    assert "total_excluding_fee" in result.composite_total
    assert "net_all_in" not in result.composite_total
    evidence(
        oracle="OPERATIONAL",
        expected_reason_codes=("FEE_COVERAGE_UNAVAILABLE",),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-130")
@pytest.mark.p1
async def test_mark_newer_than_the_position_scan_blocks_exact_unrealized(
    service_factory, request_factory, evidence
):
    """A mark bound to a later scan cannot value an earlier position set."""
    from risk.adapters.quotes import FixtureQuoteProvider

    spec = scenario("R05-buy-mark")
    # The position scan was published without a freeze proof, and the mark
    # claims to belong to a *different*, later scan.
    spec.position_scan = ScanSpec(scan_id="scan-P", entity="POSITION", cut_proof_id=None)
    spec.quote_bound_position_scan_id = "scan-P-after-partial-close"
    snapshot = quote_snapshot_for(spec)
    provider = FixtureQuoteProvider(
        {snapshot.snapshot_id: snapshot}, default_snapshot_id=snapshot.snapshot_id
    )
    service, _ = service_factory(spec, quote_provider=provider)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.UNREALIZED})), SCOPE
    )
    unrealized = result.component(Component.UNREALIZED)

    assert unrealized.status is ComponentStatus.BLOCKED
    assert "QUOTE_CUT_UNPROVEN" in unrealized.reason_codes
    # The important negative: no 0.50-lot valuation of a 0.30-lot position.
    assert unrealized.values is None
    evidence(
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("QUOTE_CUT_UNPROVEN",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-165")
@pytest.mark.p1
async def test_historical_valuation_uses_its_own_freshness_policy(
    service_factory, request_factory, evidence
):
    """A weekend-old mark is stale live and acceptable for a historical as-of."""
    spec = scenario("R05-buy-mark")
    spec.quotes = (QuoteSpec(market_time=T0 - timedelta(hours=60)),)
    service, _ = service_factory(spec)
    components = frozenset({Component.UNREALIZED})

    live = await service.compute(
        request_factory(spec, components=components, historical=False), SCOPE
    )
    historical = await service.compute(
        request_factory(spec, components=components, historical=True), SCOPE
    )

    # Same data, same mark, two policies: the live threshold is five seconds,
    # the historical one is deliberately absent.
    assert live.component(Component.UNREALIZED).status is ComponentStatus.STALE
    assert "STALE_MARK" in live.component(Component.UNREALIZED).reason_codes
    assert historical.component(Component.UNREALIZED).status is ComponentStatus.READY
    # Both produce the same figure; only the label differs.
    assert (
        live.component(Component.UNREALIZED).values["total"]  # type: ignore[index]
        == historical.component(Component.UNREALIZED).values["total"]  # type: ignore[index]
    )
    evidence(
        oracle="STATE_PROVENANCE",
        expected_values={"live": "STALE", "historical": "READY"},
        observed_values={
            "live": str(live.component(Component.UNREALIZED).status),
            "historical": str(historical.component(Component.UNREALIZED).status),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-191")
@pytest.mark.p1
async def test_a_mark_for_a_symbol_outside_the_catalog_version_blocks(
    service_factory, request_factory, evidence
):
    """A valid quote does not make an unlisted instrument valuable."""
    from risk.adapters.quotes import FixtureQuoteProvider

    spec = scenario("R05-buy-mark")
    spec.positions = (PositionSpec(position_id="p-nzd", symbol="NZDUSD", lots="1.00",
                                   open_price="0.60000"),)
    spec.quotes = (QuoteSpec(symbol="NZDUSD", bid="0.60100", ask="0.60120",
                             quote_id="q-NZDUSD-1"),)
    snapshot = quote_snapshot_for(spec)
    provider = FixtureQuoteProvider(
        {snapshot.snapshot_id: snapshot}, default_snapshot_id=snapshot.snapshot_id
    )
    service, _ = service_factory(spec, quote_provider=provider)

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
    unrealized = result.component(Component.UNREALIZED)

    assert unrealized.status is ComponentStatus.UNSUPPORTED
    assert "UNSUPPORTED_SYMBOL" in unrealized.reason_codes
    assert unrealized.values is None
    evidence(
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("UNSUPPORTED_SYMBOL",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
        notes=("a price without an approved spec is not a valuation",),
    )


@pytest.mark.wf5("WF5-192")
@pytest.mark.p2
async def test_many_snapshots_do_not_mix_within_one_calculation(
    service_factory, request_factory, evidence
):
    """With a hundred snapshots available, each result cites exactly one."""
    from risk.adapters.quotes import FixtureQuoteProvider, ImmutableQuoteStore, build_snapshot

    spec = scenario("R05-buy-mark")
    store = ImmutableQuoteStore()
    snapshots = {}
    for tick in range(100):
        bid = Decimal("1.10200") + Decimal(tick) / Decimal(100000)
        snapshot = build_snapshot(
            f"quote-S{tick:03d}",
            [
                {
                    "symbol": "EURUSD",
                    "bid": str(bid),
                    "ask": str(bid + Decimal("0.00020")),
                    "market_time": _iso(T0),
                    "quote_id": f"q-EURUSD-{tick}",
                }
            ],
            captured_at=T0,
            bound_position_scan_id="scan-P",
        )
        store.publish_snapshot(snapshot)
        snapshots[snapshot.snapshot_id] = snapshot

    provider = FixtureQuoteProvider(snapshots, default_snapshot_id="quote-S000")
    service, _ = service_factory(spec, quote_provider=provider)

    results = await asyncio.gather(
        *(
            service.compute(
                request_factory(
                    spec,
                    components=frozenset({Component.UNREALIZED}),
                    quote_snapshot_id=f"quote-S{tick:03d}",
                ),
                SCOPE,
            )
            for tick in (0, 25, 50, 75, 99)
        )
    )

    assert len(store) == 100
    for tick, result in zip((0, 25, 50, 75, 99), results):
        unrealized = result.component(Component.UNREALIZED).values
        assert unrealized is not None
        assert unrealized["quote_snapshot_id"] == f"quote-S{tick:03d}"
        quote_ids = {row["quote_id"] for row in unrealized["by_position"]}  # type: ignore[index]
        assert quote_ids == {f"q-EURUSD-{tick}"}
    # Five different marks, five different answers, no mixing.
    totals = {r.component(Component.UNREALIZED).values["total"] for r in results}  # type: ignore[index]
    assert len(totals) == 5
    evidence(
        oracle="STATE_PROVENANCE",
        observed_values={"snapshots_published": "100", "distinct_totals": str(len(totals))},
        fixture_id=spec.name,
    )


# --------------------------------------------------------------------------- #
# Domain S / T -- query budgets and database errors
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-223")
@pytest.mark.p0
async def test_the_hot_path_queries_are_index_covered(harness, evidence):
    """EXPLAIN shows the per-account fact reads use an index, not a full scan."""
    spec = scenario("R05-signed-fees")
    conn = harness(spec)

    plans: dict[str, str] = {}
    for label, sql in (
        (
            "deals_by_account_seq",
            "SELECT deal_id FROM deals WHERE epoch_id = 'e' AND account_id = 'a' "
            "AND source_seq <= 100",
        ),
        (
            "deals_by_account_time",
            "SELECT deal_id FROM deals WHERE epoch_id = 'e' AND account_id = 'a' "
            "AND executed_at >= 't'",
        ),
        (
            "positions_by_account",
            "SELECT position_id FROM positions_current WHERE epoch_id = 'e' "
            "AND account_id = 'a'",
        ),
        (
            "published_scan",
            "SELECT scan_id FROM sync_runs WHERE epoch_id = 'e' AND entity = 'POSITION' "
            "AND status = 'PUBLISHED'",
        ),
    ):
        rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
        plan = " | ".join(str(r[-1]) for r in rows)
        plans[label] = plan
        # The assertion is on the plan, not on a timing: a missing index shows
        # up here deterministically, where a wall-clock threshold would be
        # flaky on a small fixture.
        assert "SCAN" not in plan.upper() or "USING INDEX" in plan.upper(), (
            f"{label} is a full table scan: {plan}"
        )
        assert "USING" in plan.upper() or "SEARCH" in plan.upper(), f"{label}: {plan}"

    evidence(
        oracle="OPERATIONAL",
        observed_values={k: v[:60] for k, v in plans.items()},
        notes=("query plans asserted; the PostgreSQL equivalent uses EXPLAIN ANALYZE",),
    )


@pytest.mark.wf5("WF5-232")
@pytest.mark.p0
async def test_a_serialization_failure_aborts_the_whole_capture(
    service_factory, request_factory, evidence
):
    """A 40001-class error retries the whole snapshot, never half of it."""
    spec = scenario("R05-signed-fees")
    service, _ = service_factory(spec)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]
    original_stream = executor.stream
    attempts = {"count": 0}

    async def serialization_failure(sql, params=None, *, batch_size=1000):
        if "FROM deals" in sql:
            attempts["count"] += 1
            if attempts["count"] == 1:
                # Mirrors PostgreSQL SQLSTATE 40001.
                raise sqlite3.OperationalError(
                    "could not serialize access due to concurrent update"
                )
        async for batch in original_stream(sql, params, batch_size=batch_size):
            yield batch

    executor.stream = serialization_failure  # type: ignore[attr-defined]
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    # The first attempt fails outright: no partial sum is returned.
    with pytest.raises(sqlite3.OperationalError):
        await service.compute(request, SCOPE)

    # A caller-level retry re-captures the whole snapshot and succeeds.
    retried = await service.compute(request, SCOPE)
    assert attempts["count"] == 2
    realized = retried.component(Component.REALIZED).values
    assert realized is not None
    assert realized["gross"] == "60.00"
    assert retried.status is GlobalStatus.READY
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_values={"gross_after_retry": "60.00"},
        observed_values={"gross_after_retry": str(realized["gross"]),
                         "attempts": str(attempts["count"])},
        fixture_id=spec.name,
        notes=("SQLSTATE 40001 simulated at the executor port, not provoked in a server",),
    )


@pytest.mark.wf5("WF5-235")
@pytest.mark.p0
async def test_a_disk_full_error_during_a_read_is_bounded(
    service_factory, request_factory, evidence
):
    """A temp-file spill failure surfaces as an error, not an OOM or a hang."""
    spec = scenario("R05-signed-fees")
    # More rows than one read batch, so the failure lands *mid*-stream after a
    # batch has already been consumed -- which is the case that could leave a
    # partial sum behind.
    spec.deals = synthetic_deals(2500, start_seq=1, base_time=T0)
    spec.deal_watermark = 10_000
    service, _ = service_factory(spec)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]
    original_stream = executor.stream
    fired = {"count": 0}

    async def fail_partway(sql, params=None, *, batch_size=1000):
        if "FROM deals" not in sql:
            async for batch in original_stream(sql, params, batch_size=batch_size):
                yield batch
            return
        emitted = 0
        async for batch in original_stream(sql, params, batch_size=batch_size):
            emitted += 1
            if emitted > 1:
                fired["count"] += 1
                raise sqlite3.OperationalError("database or disk is full")
            yield batch

    executor.stream = fail_partway  # type: ignore[attr-defined]

    completed = await asyncio.wait_for(
        asyncio.gather(
            service.compute(
                request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
            ),
            return_exceptions=True,
        ),
        timeout=20,  # the bound: it must fail, not hang
    )
    outcome = completed[0]

    assert fired["count"] == 1
    assert isinstance(outcome, sqlite3.OperationalError)
    assert service.admission.running == 0
    assert not executor._in_snapshot  # type: ignore[attr-defined]
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_status="BLOCKED",
        observed_values={"outcome": type(outcome).__name__, "inflight": "0"},
        fixture_id=spec.name,
        notes=("disk-full simulated mid-stream; no partial total, no hang",),
    )


@pytest.mark.wf5("WF5-238")
@pytest.mark.p1
async def test_a_lagging_replica_is_detectable_from_the_cut(
    service_factory, request_factory, evidence
):
    """Two reads of the same epoch at different watermarks are distinguishable."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    # The "replica" is the same database rewound to an earlier watermark: the
    # lag is visible in the cut, which is what a consumer can act on.
    conn.execute(
        "UPDATE sync_checkpoints SET watermark_seq = 1001 WHERE epoch_id = :e",
        {"e": spec.epoch_id},
    )
    conn.commit()
    lagging = await service.compute(request, SCOPE)

    conn.execute(
        "UPDATE sync_checkpoints SET watermark_seq = 1050 WHERE epoch_id = :e",
        {"e": spec.epoch_id},
    )
    conn.commit()
    current = await service.compute(request, SCOPE)

    assert lagging.cuts["deal_watermark_seq"] == 1001
    assert current.cuts["deal_watermark_seq"] == 1050
    assert lagging.output_digest != current.output_digest
    # The lagging read is honest about covering fewer deals.
    assert lagging.component(Component.REALIZED).values["deal_count"] == 1  # type: ignore[index]
    assert current.component(Component.REALIZED).values["deal_count"] == 2  # type: ignore[index]
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        observed_values={"lagging_w": "1001", "current_w": "1050"},
        fixture_id=spec.name,
        notes=("replica lag represented as a watermark difference in the cut",),
    )


# --------------------------------------------------------------------------- #
# Domain W -- rollout and rollback
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-269")
@pytest.mark.p0
async def test_concurrent_requests_each_pin_their_own_policy(
    service_factory, request_factory, evidence
):
    """A deployment switch mid-flight cannot mix two rounding policies."""
    spec = scenario("R05-aligned-ready")
    v1 = load_policy("risk-policy-v1")
    v2 = load_policy("risk-policy-v2")

    service_v1, conn = service_factory(spec, active_policy=v1)
    service_v2, _ = service_factory(spec, active_policy=v2, connection=conn)

    results = await asyncio.gather(
        *(
            service_v1.compute(
                request_factory(spec, risk_policy_version="risk-policy-v1"), SCOPE
            )
            for _ in range(3)
        ),
        *(
            service_v2.compute(
                request_factory(spec, risk_policy_version="risk-policy-v2"), SCOPE
            )
            for _ in range(3)
        ),
        return_exceptions=True,
    )
    accepted = [r for r in results if not isinstance(r, BaseException)]
    assert accepted, "every concurrent request was rejected"

    # Each result names exactly one policy, and the two groups never share a
    # digest -- so no response was built from a mix of the two.
    v1_digests = {r.output_digest for r in accepted if r.risk_policy_version == "risk-policy-v1"}
    v2_digests = {r.output_digest for r in accepted if r.risk_policy_version == "risk-policy-v2"}
    assert len(v1_digests) <= 1
    assert len(v2_digests) <= 1
    assert not (v1_digests & v2_digests)
    for result in accepted:
        assert result.lineage["risk_policy_version"] == result.risk_policy_version
        assert result.lineage["rounding_mode"] in ("ROUND_HALF_EVEN", "ROUND_HALF_UP")
    evidence(
        oracle="OPERATIONAL",
        observed_values={
            "accepted": str(len(accepted)),
            "v1_digests": str(len(v1_digests)),
            "v2_digests": str(len(v2_digests)),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-270")
@pytest.mark.p0
async def test_audit_writes_survive_an_extra_optional_column(harness, evidence):
    """A rollback to older code still writes a readable audit row."""
    from risk.audit import SqliteAuditSink, new_receipt

    spec = scenario("R05-aligned-ready")
    conn = harness(spec)

    # Forward migration: a new nullable column the old code knows nothing about.
    conn.execute("ALTER TABLE risk_calculation_audit ADD COLUMN fx_rate_set_id TEXT")
    conn.commit()

    sink = SqliteAuditSink(conn)
    receipt = new_receipt(
        idempotency_key="r" * 64,
        epoch_id=spec.epoch_id,
        account_scope_hash="h" * 64,
        risk_policy_version="risk-policy-v1",
        symbol_catalog_version="sim-usd-fx-v1",
        cut_manifest={"epoch_id": spec.epoch_id},
        input_digest="i" * 64,
        output_digest="o" * 64,
        status=GlobalStatus.READY,
        reason_codes=(),
        duration_ms=3,
        now=T0,
    )

    # The old code's INSERT names its columns explicitly, so the new nullable
    # column is simply left NULL rather than breaking the statement.
    stored = await sink.record(receipt)
    assert stored.calculation_id == receipt.calculation_id

    row = conn.execute(
        "SELECT fx_rate_set_id, status FROM risk_calculation_audit WHERE idempotency_key = :k",
        {"k": "r" * 64},
    ).fetchone()
    assert row["fx_rate_set_id"] is None
    assert row["status"] == "READY"

    # And reading it back through the old code's SELECT still works.
    recovered = await sink.find("r" * 64)
    assert recovered is not None
    assert recovered.status is GlobalStatus.READY
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_values={"new_column": "NULL", "status": "READY"},
        observed_values={"new_column": "NULL", "status": str(recovered.status)},
        notes=("additive migration; the rollback path writes and reads both shapes",),
    )
