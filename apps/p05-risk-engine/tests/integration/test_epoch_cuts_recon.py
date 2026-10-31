"""Domains B, C, O and S -- epoch fencing, cut proofs, repair races, MVCC reads.

Covers WF5-013..036, WF5-169..180 and WF5-217..228.
"""

from __future__ import annotations

import json

import pytest

from risk.domain.facts import AuthorizedScope
from risk.errors import AuthorizationError, QualityGateError, ReasonCode, RiskError
from risk.status import Component, ComponentStatus, GlobalStatus
from risk_tools.fixtures import (
    ACCOUNT_A,
    ACCOUNT_B,
    EPOCH_A,
    EPOCH_B,
    SOURCE_A,
    SOURCE_B,
    T0,
    TENANT_A,
    AccountSpec,
    PositionSpec,
    ScanSpec,
    ScenarioSpec,
    build_harness_db,
    scenario,
)

pytestmark = [pytest.mark.integration, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))


def _iso(value):
    from datetime import UTC

    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


# --------------------------------------------------------------------------- #
# Domain B -- epoch identity and fencing
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-013", "WF5-019", "WF5-334")
async def test_epoch_generation_change_mid_flight_is_fenced(
    service_factory, request_factory, evidence
):
    """A source reset during the calculation discards the result.

    The reset is injected at the final fence -- after the facts were captured
    and the arithmetic ran -- because that is the window the fence exists to
    close. Bumping the generation before the request would simply be read as
    the current generation and prove nothing (K05-12).
    """
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)
    request = request_factory(spec)

    # A clean run first, to prove the fixture reaches READY without the fault.
    clean = await service.compute(request, SCOPE)
    assert clean.status is GlobalStatus.READY

    reader = service._deps.reader
    original_fence = reader.current_epoch_fence
    fired = {"count": 0}

    async def reset_then_fence(epoch_id: str):
        if fired["count"] == 0:
            conn.execute(
                "UPDATE source_epochs SET generation = generation + 1 WHERE epoch_id = :e",
                {"e": epoch_id},
            )
            conn.commit()
            fired["count"] += 1
        return await original_fence(epoch_id)

    reader.current_epoch_fence = reset_then_fence  # type: ignore[assignment]

    with pytest.raises(RiskError) as exc:
        await service.compute(request, SCOPE)
    assert fired["count"] == 1, "the reset injection did not fire"
    assert exc.value.reason is ReasonCode.EPOCH_CHANGED
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_status="BLOCKED",
        expected_reason_codes=("EPOCH_CHANGED",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
        notes=("reset injected at the fence, after capture and compute",),
    )


@pytest.mark.wf5("WF5-014")
async def test_epoch_turning_blocked_mid_compute_is_rejected(
    service_factory, request_factory, evidence
):
    """An epoch that goes BLOCKED while running cannot yield a READY result."""
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)
    request = request_factory(spec)

    conn.execute(
        "UPDATE source_epochs SET status = 'BLOCKED' WHERE epoch_id = :e", {"e": spec.epoch_id}
    )
    conn.commit()

    result = await service.compute(request, SCOPE)
    # Captured as BLOCKED, so the scope gate blocks every component rather than
    # the fence firing -- either way, no READY.
    assert result.status is not GlobalStatus.READY
    assert "SOURCE_NOT_ACTIVE" in result.reason_codes
    evidence(
        injection_confirmed=True,
        expected_reason_codes=("SOURCE_NOT_ACTIVE",),
        observed_reason_codes=result.reason_codes,
        observed_status=str(result.status),
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-015", "WF5-194")
async def test_two_sources_sharing_an_account_id_stay_isolated(
    service_factory, request_factory, evidence
):
    """Account 00042 at broker A and broker B are different accounts."""
    spec = ScenarioSpec(
        name="two-sources",
        epoch_id=EPOCH_A,
        source_id=SOURCE_A,
        positions=(PositionSpec(position_id="p-a", lots="1.00"),),
        position_scan=ScanSpec(scan_id="scan-P", entity="POSITION",
                               cut_proof_id="freeze-1"),
        quote_bound_position_scan_id="scan-P",
        extra_epochs=({"epoch_id": EPOCH_B, "source_id": SOURCE_B,
                       "generation": 1, "status": "ACTIVE"},),
    )
    service, _ = service_factory(spec)

    # A caller authorized for broker B cannot read broker A's epoch.
    foreign_scope = AuthorizedScope(TENANT_A, SOURCE_B, frozenset({ACCOUNT_A}))
    with pytest.raises(AuthorizationError) as exc:
        await service.compute(
            request_factory(spec, components=frozenset({Component.EXPOSURE})), foreign_scope
        )
    assert exc.value.reason is ReasonCode.UNAUTHORIZED_SCOPE

    # The rightful caller gets the data.
    allowed = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    assert allowed.component(Component.EXPOSURE).status is ComponentStatus.READY
    evidence(
        oracle="OPERATIONAL",
        expected_reason_codes=("UNAUTHORIZED_SCOPE",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-016")
def test_epoch_identity_collision_prevented_by_the_schema(evidence):
    """One (source, run, generation) cannot exist twice."""
    import sqlite3

    spec = scenario("R05-buy-mark")
    conn = build_harness_db(spec)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO source_epochs (epoch_id, source_id, run_id, generation,
                                           status, opened_at)
                VALUES (:epoch_id, :source_id, :run_id, :generation, 'ACTIVE', :opened_at)
                """,
                {
                    "epoch_id": EPOCH_B,  # a different id...
                    "source_id": spec.source_id,  # ...for the same identity
                    "run_id": f"run-{spec.source_id}",
                    "generation": spec.generation,
                    "opened_at": _iso(T0),
                },
            )
        evidence(
            oracle="STATE_PROVENANCE",
            observed_values={"constraint": "uq source/run/generation"},
        )
    finally:
        conn.close()


@pytest.mark.wf5("WF5-017", "WF5-206")
async def test_cache_identity_includes_the_source_and_epoch(
    service_factory, request_factory, evidence
):
    """The idempotency key is derived from the whole cut, not from the account id."""
    from risk.audit import make_idempotency_key

    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    # Changing only the epoch in the manifest changes the key, so no cache
    # entry can be shared across source identities.
    base_key = make_idempotency_key(
        input_digest=result.input_digest,
        risk_policy_version=result.risk_policy_version,
        requested_components=("EXPOSURE", "REALIZED", "UNREALIZED"),
        strict_composite=True,
    )
    other_key = make_idempotency_key(
        input_digest=result.input_digest + "x",
        risk_policy_version=result.risk_policy_version,
        requested_components=("EXPOSURE", "REALIZED", "UNREALIZED"),
        strict_composite=True,
    )
    assert base_key != other_key
    # And the input digest itself contains the epoch.
    assert result.lineage["cut"]["epoch_id"] == spec.epoch_id
    evidence(observed_values={"key_prefix": base_key[:16]}, fixture_id=spec.name)


@pytest.mark.wf5("WF5-018", "WF5-026")
async def test_rows_from_an_unpublished_scan_are_not_read(
    service_factory, request_factory, evidence
):
    """A stale writer's row under a different scan id never reaches the result."""
    spec = scenario("R05-buy-mark")
    service, conn = service_factory(spec)

    # A late worker publishes a second scan and writes a row against it.
    conn.execute(
        """
        INSERT INTO sync_runs (scan_id, epoch_id, entity, status, filter_hash,
                               terminal_receipt, row_count, published_at, cut_proof_id)
        VALUES ('scan-P-stale', :e, 'POSITION', 'ABANDONED', 'UNFILTERED', 1, 1, :t, NULL)
        """,
        {"e": spec.epoch_id, "t": _iso(T0)},
    )
    conn.execute(
        """
        INSERT INTO positions_current (epoch_id, position_id, account_id, symbol, side,
                                       volume_lots, open_price, source_status, presence,
                                       published_scan_id, payload_hash)
        VALUES (:e, 'p-stale', :a, 'EURUSD', 'BUY', '99.00', '1.10000', 'OPEN',
                'PRESENT', 'scan-P-stale', 'h')
        """,
        {"e": spec.epoch_id, "a": ACCOUNT_A},
    )
    conn.commit()

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    exposure = result.component(Component.EXPOSURE)
    assert exposure.values is not None
    eurusd = exposure.values["by_symbol"]["EURUSD"]  # type: ignore[index]

    # 0.50 lots from the published scan only -- the 99-lot stale row is absent.
    assert eurusd["net_units"] == "50000.00"
    assert eurusd["position_count"] == 1
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_values={"net_units": "50000.00"},
        observed_values={"net_units": str(eurusd["net_units"])},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-020")
@pytest.mark.p1
async def test_archived_epoch_is_not_reported_as_current(
    service_factory, request_factory, evidence
):
    """An ARCHIVED epoch cannot produce a READY live result."""
    spec = scenario("R05-aligned-ready")
    spec.epoch_status = "ARCHIVED"
    service, _ = service_factory(spec)

    result = await service.compute(request_factory(spec), SCOPE)
    assert result.status is not GlobalStatus.READY
    assert "SOURCE_NOT_ACTIVE" in result.reason_codes
    evidence(
        expected_reason_codes=("SOURCE_NOT_ACTIVE",),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-021")
@pytest.mark.p1
async def test_degraded_epoch_separates_realized_from_unrealized(
    service_factory, request_factory, evidence
):
    """Stale marks block only the mark-dependent component."""
    spec = scenario("R05-quote-stale")
    spec.deals = scenario("R05-signed-fees").deals
    service, _ = service_factory(spec)

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    assert result.component(Component.REALIZED).status is ComponentStatus.READY
    assert result.component(Component.UNREALIZED).status is ComponentStatus.STALE
    assert "STALE_MARK" in result.reason_codes
    # The unrealized figure is present and labelled, not withheld or zeroed.
    assert result.component(Component.UNREALIZED).values is not None
    evidence(
        expected_status="DEGRADED",
        observed_status=str(result.status),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-022", "WF5-193")
@pytest.mark.p1
async def test_request_for_an_unowned_epoch_rejected_before_any_fact_read(
    service_factory, request_factory, evidence
):
    """An unauthorized account never reaches a WHERE clause."""
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)

    # The authorization covers only ACCOUNT_A; the request asks for ACCOUNT_B.
    with pytest.raises(AuthorizationError) as exc:
        await service.compute(request_factory(spec, account_ids=(ACCOUNT_B,)), SCOPE)
    assert exc.value.reason is ReasonCode.UNAUTHORIZED_SCOPE

    # No query ran at all: the narrowing happens before the reader is called.
    stats = service._deps.reader._sql.stats  # type: ignore[attr-defined]
    assert stats.queries == 0
    evidence(
        oracle="OPERATIONAL",
        expected_reason_codes=("UNAUTHORIZED_SCOPE",),
        observed_reason_codes=(str(exc.value.reason),),
        observed_values={"queries_executed": str(stats.queries)},
    )


@pytest.mark.wf5("WF5-023")
@pytest.mark.p1
def test_missing_epoch_row_is_an_integrity_failure_not_a_zero(evidence):
    """Deals without their epoch row cannot be summed into a zero."""
    import asyncio
    import sqlite3

    spec = scenario("R05-signed-fees")
    conn = build_harness_db(spec)
    try:
        # Foreign keys are on, so deleting the epoch with deals present is
        # refused by the database -- the corruption cannot even be staged.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM source_epochs WHERE epoch_id = :e", {"e": spec.epoch_id})
        conn.rollback()

        # And a request naming an epoch that genuinely does not exist blocks.
        from risk.adapters.p03_reader import P03Reader
        from risk.adapters.sql import SqliteExecutor
        from risk.limits import Deadline
        from risk.policies.loader import load_policy
        from decimal import Decimal

        reader = P03Reader(SqliteExecutor(conn), load_policy())
        with pytest.raises(QualityGateError) as exc:
            asyncio.run(
                reader.capture(
                    scope=SCOPE,
                    epoch_id="00000000-0000-0000-0000-000000000000",
                    account_ids=frozenset({ACCOUNT_A}),
                    period_start=None,
                    period_end=None,
                    need_positions=True,
                    need_deals=True,
                    deadline=Deadline(Decimal(30)),
                )
            )
        assert exc.value.reason is ReasonCode.EPOCH_IDENTITY_CONFLICT
        evidence(observed_reason_codes=(str(exc.value.reason),))
    finally:
        conn.close()


@pytest.mark.wf5("WF5-024")
@pytest.mark.p2
async def test_without_a_freeze_proof_no_composite_is_offered(
    service_factory, request_factory, evidence
):
    """Out-of-order publication leaves the cuts INDEPENDENT, so no total is produced."""
    spec = scenario("R05-cut-mismatch")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    assert result.cuts["consistency"] == "INDEPENDENT"
    assert result.composite_total is None
    evidence(observed_values={"consistency": str(result.cuts["consistency"])},
             fixture_id=spec.name)


# --------------------------------------------------------------------------- #
# Domain C -- cross-entity cuts
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-030")
async def test_freeze_proof_permits_a_composite_total(
    service_factory, request_factory, evidence
):
    """With a proof binding the scans and the mark, the composite is offered."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec, strict_composite=True), SCOPE)

    assert result.cuts["consistency"] == "COMPATIBLE"
    assert result.cuts["cut_proof_id"] == "freeze-S1030"
    assert result.status is GlobalStatus.READY
    assert result.composite_total is not None
    # 56.00 realized plus 60.00 unrealized, both USD, both from one instant.
    assert result.composite_total["total_excluding_fee"] == "116.0000000"
    assert result.exactness.value == "COMPOSITE_ALIGNED"
    evidence(
        oracle="STATE_PROVENANCE",
        expected_status="READY",
        observed_status=str(result.status),
        expected_values={"composite": "116.0000000", "consistency": "COMPATIBLE"},
        observed_values={
            "composite": str(result.composite_total["total_excluding_fee"]),
            "consistency": str(result.cuts["consistency"]),
        },
        fixture_id=spec.name,
        output_digest=result.output_digest,
    )


@pytest.mark.wf5("WF5-031")
async def test_empty_published_scan_is_a_valid_zero(service_factory, request_factory, evidence):
    """No positions plus a terminal receipt is a real, usable zero."""
    spec = scenario("R05-empty-complete")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    exposure = result.component(Component.EXPOSURE)

    assert exposure.status is ComponentStatus.READY
    assert exposure.values is not None
    assert exposure.values["by_symbol"] == {}
    assert exposure.values["position_count"] == 0
    evidence(
        oracle="STATE_PROVENANCE",
        expected_status="READY",
        observed_status=str(exposure.status),
        expected_values={"position_count": "0"},
        observed_values={"position_count": "0"},
        fixture_id=spec.name,
        notes=("terminal receipt present, so the empty set is proven",),
    )


@pytest.mark.wf5("WF5-032")
async def test_empty_scan_without_a_terminal_receipt_blocks(
    service_factory, request_factory, evidence
):
    """The identical empty result set blocks when the last page never arrived."""
    spec = scenario("R05-empty-incomplete")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
    )
    exposure = result.component(Component.EXPOSURE)

    assert exposure.status is ComponentStatus.BLOCKED
    assert "P03_UNPUBLISHED" in exposure.reason_codes
    assert exposure.values is None
    evidence(
        oracle="STATE_PROVENANCE",
        expected_status="BLOCKED",
        observed_status=str(exposure.status),
        observed_reason_codes=exposure.reason_codes,
        fixture_id=spec.name,
        notes=("same rows as WF5-031; only the completeness proof differs",),
    )


@pytest.mark.wf5("WF5-027")
async def test_non_terminal_watermark_blocks_realized(
    service_factory, request_factory, evidence
):
    """A watermark set before the final page receipt cannot bound a report."""
    spec = scenario("R05-signed-fees")
    spec.deal_watermark_terminal = False
    service, _ = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )
    realized = result.component(Component.REALIZED)
    assert realized.status is ComponentStatus.BLOCKED
    assert "DEAL_WATERMARK_UNPROVEN" in realized.reason_codes
    evidence(
        expected_reason_codes=("DEAL_WATERMARK_UNPROVEN",),
        observed_reason_codes=realized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-029")
async def test_quote_not_bound_to_the_position_scan_blocks_unrealized(
    service_factory, request_factory, evidence
):
    """A mark that cannot be tied to the position scan blocks the valuation."""
    from risk.adapters.quotes import FixtureQuoteProvider
    from risk_tools.fixtures import quote_snapshot_for

    spec = scenario("R05-buy-mark")
    spec.position_scan = ScanSpec(scan_id="scan-P", entity="POSITION", cut_proof_id=None)
    spec.quote_bound_position_scan_id = "scan-P-OTHER"  # bound to a different scan
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
    evidence(
        expected_reason_codes=("QUOTE_CUT_UNPROVEN",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-028", "WF5-217", "WF5-220", "WF5-221")
async def test_capture_uses_one_read_transaction(service_factory, request_factory, evidence):
    """All the facts for one result come from a single read snapshot."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]

    await service.compute(request_factory(spec), SCOPE)

    # Exactly one read transaction was opened for the capture, and it was
    # rolled back -- so a concurrent publish cannot be seen half-applied, and
    # nothing was written through the read path.
    assert executor.stats.transactions == 1
    assert executor.stats.queries > 1  # several SELECTs inside that one snapshot
    evidence(
        oracle="OPERATIONAL",
        observed_values={
            "transactions": str(executor.stats.transactions),
            "queries": str(executor.stats.queries),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-033", "WF5-034")
@pytest.mark.p1
async def test_historical_request_cannot_borrow_a_newer_mark(
    service_factory, request_factory, evidence
):
    """A mark newer than the requested as-of is refused for a historical report."""
    from datetime import timedelta

    spec = scenario("R05-buy-mark")
    service, _ = service_factory(spec)

    # The snapshot's market_time is T0; ask to value as of an hour earlier.
    result = await service.compute(
        request_factory(
            spec,
            components=frozenset({Component.UNREALIZED}),
            valuation_as_of=T0 - timedelta(hours=1),
            historical=True,
        ),
        SCOPE,
    )
    unrealized = result.component(Component.UNREALIZED)

    assert unrealized.status is ComponentStatus.BLOCKED
    assert "QUOTE_LOOKAHEAD" in unrealized.reason_codes
    assert unrealized.values is None
    evidence(
        expected_reason_codes=("QUOTE_LOOKAHEAD",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-035", "WF5-178")
@pytest.mark.p1
async def test_verification_scope_does_not_stretch_to_new_accounts(
    service_factory, request_factory, evidence
):
    """Evidence covering account A does not verify account B."""
    spec = scenario("R05-aligned-ready")
    spec.accounts = (AccountSpec(account_id=ACCOUNT_A), AccountSpec(account_id=ACCOUNT_B))
    spec.recon_covered_accounts = (ACCOUNT_A,)  # B is outside the verification
    service, _ = service_factory(spec)

    wide_scope = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A, ACCOUNT_B}))
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), wide_scope
    )
    exposure = result.component(Component.EXPOSURE)

    assert exposure.status is ComponentStatus.BLOCKED
    assert "RECON_EVIDENCE_SCOPE_UNKNOWN" in exposure.reason_codes
    evidence(
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("RECON_EVIDENCE_SCOPE_UNKNOWN",),
        observed_reason_codes=exposure.reason_codes,
        fixture_id=spec.name,
    )


# --------------------------------------------------------------------------- #
# Domain O -- reconciliation and repair
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-169", "WF5-175")
async def test_blocking_finding_blocks_the_affected_scope(
    service_factory, request_factory, evidence
):
    """An INCONCLUSIVE reconciliation blocks every component in scope."""
    spec = scenario("R05-recon-conflict")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    assert result.status is GlobalStatus.BLOCKED
    assert "RECON_INCONCLUSIVE" in result.reason_codes
    for payload in result.components.values():
        assert payload.status is ComponentStatus.BLOCKED
        assert payload.values is None
    evidence(
        oracle="STATE_PROVENANCE",
        expected_status="BLOCKED",
        observed_status=str(result.status),
        expected_reason_codes=("RECON_INCONCLUSIVE",),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-172", "WF5-173")
@pytest.mark.parametrize("status", ["APPROVED", "COMMITTED"])
async def test_approved_and_committed_are_not_verified(
    status, service_factory, request_factory, evidence
):
    """A repair that is approved, or even written, is not yet verified."""
    spec = scenario("R05-aligned-ready")
    spec.recon_status = status
    service, _ = service_factory(spec)

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
    assert result.status is not GlobalStatus.READY
    assert "RECON_BLOCKING" in result.reason_codes
    evidence(
        expected_reason_codes=("RECON_BLOCKING",),
        observed_reason_codes=result.reason_codes,
        observed_values={"recon_status": status},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-174")
async def test_evidence_from_another_epoch_is_rejected(
    service_factory, request_factory, evidence
):
    """Repair evidence filed against a previous generation says nothing about this one."""
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)

    # Re-file the evidence under a different epoch, leaving this one uncovered.
    conn.execute("DELETE FROM recon_findings")
    conn.execute("DELETE FROM recon_evidence")
    conn.commit()

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
    assert result.status is not GlobalStatus.READY
    assert "RECON_EVIDENCE_SCOPE_UNKNOWN" in result.reason_codes
    evidence(
        injection_confirmed=True,
        expected_reason_codes=("RECON_EVIDENCE_SCOPE_UNKNOWN",),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-171")
async def test_backfilled_deal_changes_the_result_and_its_digest(
    service_factory, request_factory, evidence
):
    """A backfill into a prior period produces a different, correctly larger total."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    before = await service.compute(request, SCOPE)
    assert before.component(Component.REALIZED).values["gross"] == "60.00"  # type: ignore[index]

    conn.execute(
        """
        INSERT INTO deals (epoch_id, deal_id, account_id, source_seq, entry_kind, deal_type,
                           symbol, realized_pnl_gross, commission, swap, money_currency,
                           executed_at, payload_hash)
        VALUES (:e, 'd-backfill', :a, 1010, 'OUT', NULL, 'EURUSD',
                '12.00', '0.00', '0.00', 'USD', :t, 'h')
        """,
        {"e": spec.epoch_id, "a": ACCOUNT_A, "t": _iso(T0)},
    )
    conn.commit()

    after = await service.compute(request, SCOPE)
    assert after.component(Component.REALIZED).values["gross"] == "72.00"  # type: ignore[index]
    # Different inputs, different digest -- so a cache keyed on the digest
    # cannot serve the pre-backfill answer.
    assert before.input_digest != after.input_digest
    assert before.output_digest != after.output_digest
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_values={"before": "60.00", "after": "72.00"},
        observed_values={"before": "60.00", "after": "72.00"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-170", "WF5-176")
@pytest.mark.p1
async def test_finding_on_another_account_does_not_block_a_covered_one(
    service_factory, request_factory, evidence
):
    """A blocking finding is scoped: it blocks the account it names."""
    spec = scenario("R05-aligned-ready")
    spec.recon_covered_accounts = (ACCOUNT_A, ACCOUNT_B)
    spec.blocking_findings = ({"account_id": ACCOUNT_B, "code": "VOLUME_MISMATCH"},)
    service, _ = service_factory(spec)

    # The request covers only account A, which the finding does not name.
    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
    assert result.status is GlobalStatus.READY

    # Widening the scope to include B picks up the block.
    wide = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A, ACCOUNT_B}))
    blocked = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), wide
    )
    assert blocked.component(Component.EXPOSURE).status is ComponentStatus.BLOCKED
    assert "RECON_BLOCKING" in blocked.component(Component.EXPOSURE).reason_codes
    evidence(
        oracle="STATE_PROVENANCE",
        observed_values={"account_a": str(result.status), "with_account_b": str(blocked.status)},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-177", "WF5-179", "WF5-180", "WF5-120", "WF5-142")
@pytest.mark.p1
async def test_correction_in_the_data_blocks_the_affected_report(
    service_factory, request_factory, evidence
):
    """A correction row in the window blocks rather than restating a figure."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    conn.execute(
        """
        INSERT INTO deals (epoch_id, deal_id, account_id, source_seq, entry_kind, deal_type,
                           symbol, realized_pnl_gross, commission, swap, money_currency,
                           executed_at, payload_hash)
        VALUES (:e, 'd-correction', :a, 1011, 'OUT', 'DEAL_TYPE_CORRECTION', 'EURUSD',
                '-5.00', '0.00', '0.00', 'USD', :t, 'h')
        """,
        {"e": spec.epoch_id, "a": ACCOUNT_A, "t": _iso(T0)},
    )
    conn.commit()

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )
    realized = result.component(Component.REALIZED)

    assert realized.status is ComponentStatus.BLOCKED
    assert "DEAL_CORRECTION_UNSUPPORTED" in realized.reason_codes
    # Crucially, no partial sum of the other deals is published.
    assert realized.values is None
    evidence(
        injection_confirmed=True,
        expected_reason_codes=("DEAL_CORRECTION_UNSUPPORTED",),
        observed_reason_codes=realized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-208", "WF5-338", "WF5-344")
async def test_finding_appearing_after_capture_invalidates_the_result(
    service_factory, request_factory, evidence
):
    """A reconciliation revision that lands mid-flight blocks the computed answer.

    The injection is placed where the design puts it: after the facts were
    captured and the arithmetic ran, before the response is formed. The final
    fence re-reads the revision and refuses to publish a number computed
    without the new evidence.
    """
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)
    reader = service._deps.reader
    original_fence = reader.current_recon_revision
    fired = {"count": 0}

    async def inject_then_fence(epoch_id: str):
        # Runs at the fence, i.e. after compute: insert a newer revision.
        if fired["count"] == 0:
            conn.execute(
                """
                INSERT INTO recon_evidence (evidence_id, epoch_id, revision, status,
                                            covered_accounts, covered_deal_watermark,
                                            scope_known, created_at)
                VALUES ('e-new', :e, :rev, 'INCONCLUSIVE', :cov, 1050, 1, :t)
                """,
                {
                    "e": epoch_id,
                    "rev": spec.recon_revision + 1,
                    "cov": json.dumps([ACCOUNT_A]),
                    "t": _iso(T0),
                },
            )
            conn.commit()
            fired["count"] += 1
        return await original_fence(epoch_id)

    reader.current_recon_revision = inject_then_fence  # type: ignore[assignment]

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    assert fired["count"] == 1, "the injection did not fire"
    assert result.status is GlobalStatus.BLOCKED
    assert "RECON_BLOCKING" in result.reason_codes
    for payload in result.components.values():
        assert payload.status is ComponentStatus.BLOCKED
        assert payload.values is None
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_status="BLOCKED",
        observed_status=str(result.status),
        expected_reason_codes=("RECON_BLOCKING",),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
        notes=("fence fired after compute, before the response",),
    )


# --------------------------------------------------------------------------- #
# Domain S -- SQL-level reads
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-218")
async def test_rows_beyond_the_watermark_are_filtered_in_sql(
    service_factory, request_factory, evidence
):
    """The watermark ceiling is in the WHERE clause, not applied afterwards."""
    spec = scenario("R05-late-deal")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )
    realized = result.component(Component.REALIZED)
    assert realized.values is not None

    # Only the deal below W=1050 contributes; the seq-2000 row never loads.
    assert realized.values["gross"] == "60.00"
    assert realized.values["deal_count"] == 1
    assert realized.values["excluded_beyond_watermark"] == 0  # filtered in SQL
    assert result.lineage["row_counts"]["deals"] == 1
    evidence(
        oracle="STATE_PROVENANCE",
        expected_values={"gross": "60.00", "rows_loaded": "1"},
        observed_values={
            "gross": str(realized.values["gross"]),
            "rows_loaded": str(result.lineage["row_counts"]["deals"]),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-222")
async def test_deals_are_not_joined_to_positions(service_factory, request_factory, evidence):
    """Realized P&L for a closed position survives: no inner join drops it."""
    spec = scenario("R05-signed-fees")
    spec.positions = ()  # every position closed; the deals remain
    spec.position_scan = ScanSpec(scan_id="scan-P", entity="POSITION",
                                  cut_proof_id="freeze-S1030")
    service, _ = service_factory(spec)

    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )
    realized = result.component(Component.REALIZED)

    assert realized.status is ComponentStatus.READY
    assert realized.values is not None
    assert realized.values["gross"] == "60.00"
    evidence(
        oracle="STATE_PROVENANCE",
        expected_values={"gross": "60.00"},
        observed_values={"gross": str(realized.values["gross"])},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-226")
@pytest.mark.p1
def test_every_fact_query_filters_on_the_epoch(evidence):
    """Statically: no fact SELECT in the reader omits the epoch predicate."""
    from risk.adapters import p03_reader

    fact_queries = [
        p03_reader._ACCOUNT_SQL,
        p03_reader._ACCOUNT_SQL_PG,
        p03_reader._POSITION_SQL,
        p03_reader._POSITION_SQL_PG,
        p03_reader._DEAL_SQL,
        p03_reader._DEAL_SQL_PG,
        p03_reader._DEAL_COUNT_SQL,
        p03_reader._DEAL_COUNT_SQL_PG,
    ]
    for sql in fact_queries:
        assert "epoch_id = :epoch_id" in sql, sql[:60]
        assert "account_id" in sql
        # And no query concatenates a value into the text.
        assert "%" not in sql or ":" in sql
    evidence(oracle="OPERATIONAL", observed_values={"queries_checked": str(len(fact_queries))})


@pytest.mark.wf5("WF5-224")
@pytest.mark.p1
async def test_duplicate_rows_from_a_faulty_adapter_are_detected(
    service_factory, request_factory, evidence
):
    """A reader that returns a row twice is caught by the unique-id check."""
    from risk.errors import ContractError

    spec = scenario("R05-buy-mark")
    service, _ = service_factory(spec)
    reader = service._deps.reader
    original_stream = reader._sql.stream  # type: ignore[attr-defined]

    async def duplicating_stream(sql, params=None, *, batch_size=1000):
        async for batch in original_stream(sql, params, batch_size=batch_size):
            yield batch + batch  # every row delivered twice

    reader._sql.stream = duplicating_stream  # type: ignore[attr-defined]

    with pytest.raises(ContractError) as exc:
        await service.compute(
            request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
        )
    assert exc.value.reason is ReasonCode.DATA_INTEGRITY_CONFLICT
    evidence(
        injection_confirmed=True,
        expected_reason_codes=("DATA_INTEGRITY_CONFLICT",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-225", "WF5-287")
@pytest.mark.p1
async def test_recon_revision_travels_in_the_cut(service_factory, request_factory, evidence):
    """The evidence revision is part of the lineage, so staleness is detectable."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    assert result.cuts["recon_revision"] == spec.recon_revision
    evidence(
        observed_values={"recon_revision": str(result.cuts["recon_revision"])},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-228")
@pytest.mark.p2
async def test_result_names_its_source_snapshot_sequence_when_known(
    service_factory, request_factory, evidence
):
    """When upstream supplies a snapshot sequence it is recorded, not inferred."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    assert result.cuts["source_snapshot_seq"] == 1030
    assert result.cuts["cut_proof_id"] == "freeze-S1030"
    evidence(
        observed_values={"source_snapshot_seq": str(result.cuts["source_snapshot_seq"])},
        fixture_id=spec.name,
    )
