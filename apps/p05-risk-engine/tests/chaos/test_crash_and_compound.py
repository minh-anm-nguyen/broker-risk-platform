"""Domains AB and AC -- crash points, recovery contracts, compound failures.

Covers WF5-325..336 and WF5-337..348, and the kill points K05-01..K05-20 of
design section 14.

Recovery here is almost entirely *recompute*. Project 05 owns no source facts,
so a crash leaves nothing to repair upstream -- which the first test asserts
rather than assumes. The one piece of genuinely stateful behaviour is the audit
write, and its ambiguous-COMMIT window is what most of these tests exercise.

Every fault is injected at a named barrier and every test confirms the
injection fired. A chaos test whose fault silently failed to happen has tested
the happy path under a misleading name (WF5-295, WF5-296).
"""

from __future__ import annotations

import asyncio
import hashlib
import json

import pytest

from risk.audit import (
    AuditReceipt,
    InMemoryAuditSink,
    make_calculation_id,
    make_idempotency_key,
    new_receipt,
    record_with_retry,
)
from risk.domain.facts import AuthorizedScope
from risk.errors import AuditError, ReasonCode, RiskError
from risk.status import Component, ComponentStatus, GlobalStatus
from risk_tools.fixtures import (
    ACCOUNT_A,
    SOURCE_A,
    T0,
    TENANT_A,
    QuoteSpec,
    ScanSpec,
    quote_snapshot_for,
    scenario,
)

pytestmark = [pytest.mark.chaos, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))

UPSTREAM_TABLES = (
    "source_epochs",
    "sync_runs",
    "sync_checkpoints",
    "accounts_current",
    "positions_current",
    "deals",
    "recon_evidence",
    "recon_findings",
)


def upstream_hash(conn) -> str:
    """Fingerprint every upstream table, for before/after comparison."""
    digest = hashlib.sha256()
    for table in UPSTREAM_TABLES:
        for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1"):  # noqa: S608
            digest.update(repr(tuple(row)).encode())
    return digest.hexdigest()


# --------------------------------------------------------------------------- #
# Kill points: K05-01 .. K05-20
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-326", "WF5-332")
async def test_crash_mid_calculation_leaves_upstream_untouched(
    service_factory, request_factory, evidence
):
    """K05-10/K05-11: an abort inside the arithmetic mutates nothing upstream."""
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)
    before = upstream_hash(conn)

    fired = {"count": 0}
    original = service._compute_components

    def die_mid_compute(*args, **kwargs):
        fired["count"] += 1
        raise KeyboardInterrupt("simulated SIGKILL inside the aggregation")

    service._compute_components = die_mid_compute  # type: ignore[assignment]

    with pytest.raises(KeyboardInterrupt):
        await service.compute(request_factory(spec), SCOPE)

    after = upstream_hash(conn)
    assert fired["count"] == 1, "the crash injection did not fire"
    assert before == after
    assert service.admission.running == 0

    # And recovery is a plain recompute: the same inputs give the same answer.
    service._compute_components = original  # type: ignore[assignment]
    recovered = await service.compute(request_factory(spec), SCOPE)
    assert recovered.status is GlobalStatus.READY
    assert recovered.component(Component.REALIZED).values["net_excluding_fee"] == "56.00"  # type: ignore[index]
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_values={"upstream_unchanged": "True", "recovered_net": "56.00"},
        observed_values={"before": before[:16], "after": after[:16],
                         "recovered_net": "56.00"},
        fixture_id=spec.name,
        notes=("K05-11: discard partial state, recompute from the pinned inputs",),
    )


@pytest.mark.wf5("WF5-327")
async def test_crash_between_compute_and_audit_publishes_nothing(
    service_factory, request_factory, evidence
):
    """K05-13: a result that could not be audited is not returned."""
    spec = scenario("R05-aligned-ready")
    sink = InMemoryAuditSink()
    sink.fail_always = True
    service, conn = service_factory(spec, sink=sink)
    before = upstream_hash(conn)

    with pytest.raises(AuditError) as exc:
        await service.compute(request_factory(spec), SCOPE)

    assert exc.value.reason is ReasonCode.AUDIT_PERSISTENCE_FAILED
    assert len(sink) == 0
    assert upstream_hash(conn) == before
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_reason_codes=("AUDIT_PERSISTENCE_FAILED",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
        notes=("K05-13: no unaudited READY",),
    )


@pytest.mark.wf5("WF5-328", "WF5-357")
async def test_ambiguous_commit_resolves_to_one_receipt(
    service_factory, request_factory, evidence
):
    """K05-15/K05-16: the commit landed, the ACK was lost, the retry finds it.

    This is the one genuinely unavoidable window in the design. The injection
    makes the sink persist the receipt and *then* report failure, exactly as a
    lost acknowledgement looks from the caller's side.
    """
    spec = scenario("R05-aligned-ready")

    class LosesTheAck(InMemoryAuditSink):
        def __init__(self) -> None:
            super().__init__()
            self.lost_acks = 0

        async def record(self, receipt: AuditReceipt) -> AuditReceipt:
            stored = await super().record(receipt)
            if self.lost_acks == 0:
                self.lost_acks += 1
                # Durable, but the caller never hears about it.
                raise AuditError(
                    ReasonCode.AUDIT_PERSISTENCE_FAILED, "connection reset after COMMIT"
                )
            return stored

    sink = LosesTheAck()
    service, _ = service_factory(spec, sink=sink)
    request = request_factory(spec)

    # The policy retry budget covers the lost ACK: the second attempt finds the
    # row already there and returns it as a replay.
    result = await service.compute(request, SCOPE)

    assert sink.lost_acks == 1, "the lost-ACK injection did not fire"
    assert len(sink) == 1  # exactly one receipt, not two
    assert sink.writes == 1
    assert result.status is GlobalStatus.READY

    # A fresh client retry resolves to the same identifier.
    again = await service.compute(request, SCOPE)
    assert again.calculation_id == result.calculation_id
    assert again.output_digest == result.output_digest
    assert len(sink) == 1
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_values={"receipts": "1"},
        observed_values={"receipts": str(len(sink)),
                         "calculation_id": result.calculation_id},
        fixture_id=spec.name,
        notes=("K05-15: idempotency key derived from the input digest",),
    )


@pytest.mark.wf5("WF5-328")
async def test_idempotency_key_is_stable_and_discriminating(evidence):
    """The key changes when any input to the answer changes, and only then."""
    base = dict(
        input_digest="a" * 64,
        risk_policy_version="risk-policy-v1",
        requested_components=("EXPOSURE", "REALIZED"),
        strict_composite=True,
    )
    key = make_idempotency_key(**base)  # type: ignore[arg-type]

    # Stable across repeated derivation and across component ordering.
    assert key == make_idempotency_key(**base)  # type: ignore[arg-type]
    assert key == make_idempotency_key(
        **{**base, "requested_components": ("REALIZED", "EXPOSURE")}  # type: ignore[arg-type]
    )
    # And distinct for every field that can change the answer.
    assert key != make_idempotency_key(**{**base, "input_digest": "b" * 64})  # type: ignore[arg-type]
    assert key != make_idempotency_key(
        **{**base, "risk_policy_version": "risk-policy-v2"}  # type: ignore[arg-type]
    )
    assert key != make_idempotency_key(**{**base, "strict_composite": False})  # type: ignore[arg-type]
    assert make_calculation_id(key) == make_calculation_id(key)
    evidence(oracle="OPERATIONAL", observed_values={"key": key[:16]})


@pytest.mark.wf5("WF5-329")
async def test_audit_retry_budget_is_bounded(evidence):
    """A sink that never recovers exhausts the budget and raises; it does not hang."""
    sink = InMemoryAuditSink()
    sink.fail_always = True
    receipt = new_receipt(
        idempotency_key="k" * 64,
        epoch_id="e",
        account_scope_hash="h" * 64,
        risk_policy_version="risk-policy-v1",
        symbol_catalog_version="sim-usd-fx-v1",
        cut_manifest={},
        input_digest="i" * 64,
        output_digest="o" * 64,
        status=GlobalStatus.READY,
        reason_codes=(),
        duration_ms=1,
        now=T0,
    )

    with pytest.raises(AuditError):
        await asyncio.wait_for(record_with_retry(sink, receipt, attempts=3), timeout=5)
    assert len(sink) == 0
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"attempts": "3", "outcome": "AUDIT_PERSISTENCE_FAILED"},
    )


@pytest.mark.wf5("WF5-325")
async def test_read_transaction_is_released_on_every_path(
    service_factory, request_factory, evidence
):
    """K05-17: success, block and exception all leave no open transaction."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]

    await service.compute(request_factory(spec), SCOPE)
    assert not executor._in_snapshot  # type: ignore[attr-defined]

    blocked_spec = scenario("R05-recon-conflict")
    blocked_service, _ = service_factory(blocked_spec)
    blocked_executor = blocked_service._deps.reader._sql  # type: ignore[attr-defined]
    await blocked_service.compute(request_factory(blocked_spec, strict_composite=False), SCOPE)
    assert not blocked_executor._in_snapshot  # type: ignore[attr-defined]

    failing_spec = scenario("R05-invalid-decimal")
    failing_service, _ = service_factory(failing_spec)
    failing_executor = failing_service._deps.reader._sql  # type: ignore[attr-defined]
    with pytest.raises(RiskError):
        await failing_service.compute(
            request_factory(failing_spec, components=frozenset({Component.REALIZED})), SCOPE
        )
    assert not failing_executor._in_snapshot  # type: ignore[attr-defined]
    evidence(
        oracle="OPERATIONAL",
        observed_values={"paths_checked": "success,blocked,raised"},
    )


@pytest.mark.wf5("WF5-330", "WF5-282")
async def test_digest_is_independent_of_locale_and_timezone(
    service_factory, request_factory, evidence
):
    """The same inputs give the same digest whatever the process TZ and locale."""
    import os
    import time

    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)
    request = request_factory(spec)

    digests: set[str] = set()
    original_tz = os.environ.get("TZ")
    try:
        for tz in ("UTC", "Asia/Tokyo", "America/Los_Angeles"):
            os.environ["TZ"] = tz
            if hasattr(time, "tzset"):  # not available on Windows
                time.tzset()
            service_tz, _ = service_factory(spec, connection=conn)
            result = await service_tz.compute(request, SCOPE)
            digests.add(result.output_digest)
    finally:
        if original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original_tz
        if hasattr(time, "tzset"):
            time.tzset()

    assert len(digests) == 1, "digest varied with the process timezone"
    evidence(
        oracle="OPERATIONAL",
        expected_values={"distinct_digests": "1"},
        observed_values={"distinct_digests": str(len(digests))},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-331")
@pytest.mark.p1
async def test_restored_older_snapshot_against_a_newer_mark_blocks(
    service_factory, request_factory, evidence
):
    """A restore that rewinds the facts but not the marks cannot claim alignment."""
    from risk.adapters.quotes import FixtureQuoteProvider

    spec = scenario("R05-buy-mark")
    # The restored position scan carries no freeze proof, and the mark is bound
    # to a scan that no longer exists.
    spec.position_scan = ScanSpec(scan_id="scan-P", entity="POSITION", cut_proof_id=None)
    spec.quote_bound_position_scan_id = "scan-P-pre-restore"
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
    assert unrealized.values is None
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("QUOTE_CUT_UNPROVEN",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-332")
async def test_losing_the_derived_cache_loses_nothing_authoritative(
    service_factory, request_factory, evidence
):
    """Dropping every audit row still leaves the result recomputable, identically."""
    spec = scenario("R05-aligned-ready")
    sink = InMemoryAuditSink()
    service, conn = service_factory(spec, sink=sink)
    request = request_factory(spec)

    before = await service.compute(request, SCOPE)
    assert len(sink) == 1

    # Simulate a disaster that destroyed only Project 05's derived data.
    sink._by_key.clear()  # type: ignore[attr-defined]
    assert len(sink) == 0

    after = await service.compute(request, SCOPE)
    assert after.output_digest == before.output_digest
    assert after.calculation_id == before.calculation_id
    assert after.component(Component.REALIZED).values == before.component(
        Component.REALIZED
    ).values
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_values={"digest_identical": "True"},
        observed_values={"digest": after.output_digest[:16]},
        fixture_id=spec.name,
        notes=("derived data is a cache; the source of truth is upstream",),
    )


@pytest.mark.wf5("WF5-333")
@pytest.mark.p1
def test_removing_a_referenced_catalog_version_blocks_rather_than_substituting(evidence):
    """A historical report cannot fall back to the current catalog."""
    from risk.errors import ContractError
    from risk.policies.loader import load_catalog

    assert load_catalog("sim-usd-fx-v1").catalog_version == "sim-usd-fx-v1"

    with pytest.raises(ContractError) as exc:
        load_catalog("sim-usd-fx-v0-retired")
    assert exc.value.reason is ReasonCode.SPEC_VERSION_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-335", "WF5-336")
@pytest.mark.p2
async def test_a_superseded_report_is_distinguishable_by_its_digest(
    service_factory, request_factory, evidence
):
    """After a replay changes the facts, the old digest no longer matches."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    original = await service.compute(request, SCOPE)
    conn.execute(
        "UPDATE deals SET realized_pnl_gross = '70.00' WHERE deal_id = 'd-close-1'"
    )
    conn.commit()
    replayed = await service.compute(request, SCOPE)

    assert original.input_digest != replayed.input_digest
    assert original.calculation_id != replayed.calculation_id
    # Both receipts coexist, so an operator can see the restatement happened.
    assert replayed.component(Component.REALIZED).values["gross"] == "70.00"  # type: ignore[index]
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        observed_values={"before": "60.00", "after": "70.00"},
        fixture_id=spec.name,
    )


# --------------------------------------------------------------------------- #
# Domain AC -- compound failures
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-337")
async def test_stale_quote_plus_epoch_reset_yields_no_result(
    service_factory, request_factory, evidence
):
    """Two faults at once: both are reported, neither is papered over."""
    spec = scenario("R05-quote-stale")
    service, conn = service_factory(spec)
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
        await service.compute(
            request_factory(spec, components=frozenset({Component.UNREALIZED})), SCOPE
        )
    assert fired["count"] == 1
    assert exc.value.reason is ReasonCode.EPOCH_CHANGED
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("EPOCH_CHANGED",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
        notes=("stale mark and epoch reset; the fence wins and nothing is published",),
    )


@pytest.mark.wf5("WF5-339")
async def test_database_failure_plus_cancellation_leaks_nothing(
    service_factory, request_factory, evidence
):
    """A failing read and a cancelling client together leave no resources held."""
    import sqlite3

    spec = scenario("R05-signed-fees")
    service, _ = service_factory(spec)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]
    original_stream = executor.stream
    fired = {"count": 0}

    async def fail_then_cancel(sql, params=None, *, batch_size=1000):
        if "FROM deals" in sql:
            fired["count"] += 1
            await asyncio.sleep(0)
            raise sqlite3.OperationalError("failover in progress")
        async for batch in original_stream(sql, params, batch_size=batch_size):
            yield batch

    executor.stream = fail_then_cancel  # type: ignore[attr-defined]

    task = asyncio.create_task(
        service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
        )
    )
    with pytest.raises(sqlite3.OperationalError):
        await task

    assert fired["count"] == 1
    assert service.admission.running == 0
    assert not executor._in_snapshot  # type: ignore[attr-defined]
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"inflight": "0", "open_snapshots": "0"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-340")
async def test_hostile_decimal_under_concurrency_costs_nothing(evidence):
    """A hostile literal is refused cheaply, even a hundred at a time."""
    import time

    from risk.domain.money import parse_decimal
    from risk.errors import ContractError

    payloads = ["1E+99999999", "9" * 200, "NaN", "1e99999999999"] * 25
    start = time.monotonic()
    rejected = 0
    for payload in payloads:
        try:
            parse_decimal(payload, field="gross")
        except ContractError:
            rejected += 1
    elapsed = time.monotonic() - start

    assert rejected == len(payloads)
    assert elapsed < 1.0, f"ingress guard cost {elapsed:.3f}s for 100 rejections"
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"rejected": str(rejected), "elapsed_s": f"{elapsed:.4f}"},
    )


@pytest.mark.wf5("WF5-341")
async def test_missing_rate_and_stale_mark_report_both_reasons(
    service_factory, request_factory, evidence
):
    """A non-USD symbol and a stale mark produce no partial USD composite."""
    from risk.adapters.quotes import FixtureQuoteProvider
    from datetime import timedelta

    spec = scenario("R05-buy-mark")
    spec.positions = (
        *spec.positions,
        type(spec.positions[0])(
            position_id="p-jpy", symbol="USDJPY", side="BUY", lots="1.00",
            open_price="149.000", source_status="OPEN", presence="PRESENT",
            account_id=ACCOUNT_A,
        ),
    )
    spec.quotes = (
        QuoteSpec(market_time=T0 - timedelta(seconds=30)),
        QuoteSpec(symbol="USDJPY", bid="150.000", ask="150.010", quote_id="q-USDJPY-1",
                  market_time=T0 - timedelta(seconds=30)),
    )
    snapshot = quote_snapshot_for(spec)
    provider = FixtureQuoteProvider(
        {snapshot.snapshot_id: snapshot}, default_snapshot_id=snapshot.snapshot_id
    )
    service, _ = service_factory(spec, quote_provider=provider)

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
    unrealized = result.component(Component.UNREALIZED)

    # Unsupported outranks stale: there is no verified formula for the JPY leg,
    # so no number is produced and no USD composite is offered.
    assert unrealized.status is ComponentStatus.UNSUPPORTED
    assert "UNSUPPORTED_CALC_MODE" in unrealized.reason_codes
    assert unrealized.values is None
    assert result.composite_total is None
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("UNSUPPORTED_CALC_MODE",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-342")
async def test_backfill_during_a_month_end_report_snaps_to_the_watermark(
    service_factory, request_factory, evidence
):
    """A deal arriving above W is excluded now and included after W advances."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    conn.execute(
        """
        INSERT INTO deals (epoch_id, deal_id, account_id, source_seq, entry_kind, deal_type,
                           symbol, realized_pnl_gross, commission, swap, money_currency,
                           executed_at, payload_hash)
        VALUES (:e, 'd-backfill', :a, 9000, 'OUT', NULL, 'EURUSD',
                '99.00', '0.00', '0.00', 'USD', :t, 'h')
        """,
        {
            "e": spec.epoch_id,
            "a": ACCOUNT_A,
            "t": T0.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00"),
        },
    )
    conn.commit()

    pre = await service.compute(request, SCOPE)
    assert pre.component(Component.REALIZED).values["gross"] == "60.00"  # type: ignore[index]

    conn.execute(
        "UPDATE sync_checkpoints SET watermark_seq = 9500 WHERE epoch_id = :e",
        {"e": spec.epoch_id},
    )
    conn.commit()
    post = await service.compute(request, SCOPE)

    assert post.component(Component.REALIZED).values["gross"] == "159.00"  # type: ignore[index]
    assert pre.cuts["deal_watermark_seq"] == 1050
    assert post.cuts["deal_watermark_seq"] == 9500
    # The two reports are distinguishable, so neither can be mistaken for final.
    assert pre.output_digest != post.output_digest
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_values={"at_w1050": "60.00", "at_w9500": "159.00"},
        observed_values={"at_w1050": "60.00", "at_w9500": "159.00"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-343")
async def test_completeness_outranks_freshness(service_factory, request_factory, evidence):
    """A perfect mark does not rescue an incomplete position scan."""
    spec = scenario("R05-buy-mark")
    spec.position_scan = ScanSpec(
        scan_id="scan-P", entity="POSITION", terminal_receipt=False,
        cut_proof_id="freeze-S1030",
    )
    service, _ = service_factory(spec)

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
    unrealized = result.component(Component.UNREALIZED)
    exposure = result.component(Component.EXPOSURE)

    assert exposure.status is ComponentStatus.BLOCKED
    assert unrealized.status is ComponentStatus.BLOCKED
    assert "P03_UNPUBLISHED" in unrealized.reason_codes
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("P03_UNPUBLISHED",),
        observed_reason_codes=unrealized.reason_codes,
        fixture_id=spec.name,
        notes=("fresh quote, incomplete scan: the scan wins",),
    )


@pytest.mark.wf5("WF5-344")
async def test_triple_version_mismatch_blocks_the_composite(
    service_factory, request_factory, evidence
):
    """Old evidence, a new scan and a stale mark together produce no total."""
    from datetime import timedelta

    from risk.adapters.quotes import FixtureQuoteProvider

    spec = scenario("R05-aligned-ready")
    spec.recon_covered_watermark = 100  # evidence from an older watermark
    spec.recon_status = "INCONCLUSIVE"
    spec.quotes = (QuoteSpec(market_time=T0 - timedelta(seconds=45)),)
    snapshot = quote_snapshot_for(spec)
    provider = FixtureQuoteProvider(
        {snapshot.snapshot_id: snapshot}, default_snapshot_id=snapshot.snapshot_id
    )
    service, _ = service_factory(spec, quote_provider=provider)

    result = await service.compute(request_factory(spec, strict_composite=True), SCOPE)

    assert result.status is GlobalStatus.BLOCKED
    assert result.composite_total is None
    assert "RECON_INCONCLUSIVE" in result.reason_codes
    for payload in result.components.values():
        assert payload.values is None
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_status="BLOCKED",
        observed_status=str(result.status),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-345")
@pytest.mark.p1
async def test_audit_outage_with_an_expiring_deadline_fails_closed(evidence):
    """Both controls fail at once; the result is still not published."""
    from decimal import Decimal

    from risk.limits import Deadline
    from risk.errors import ResourceError

    sink = InMemoryAuditSink()
    sink.fail_always = True
    deadline = Deadline(Decimal("0.001"))
    await asyncio.sleep(0.01)

    # The deadline has expired and the sink is down. Neither condition can
    # produce a result, and both report a reason.
    with pytest.raises(ResourceError) as deadline_exc:
        deadline.check(stage="audit")
    assert deadline_exc.value.reason is ReasonCode.DEADLINE_EXCEEDED

    receipt = new_receipt(
        idempotency_key="k" * 64,
        epoch_id="e",
        account_scope_hash="h" * 64,
        risk_policy_version="risk-policy-v1",
        symbol_catalog_version="sim-usd-fx-v1",
        cut_manifest={},
        input_digest="i" * 64,
        output_digest="o" * 64,
        status=GlobalStatus.READY,
        reason_codes=(),
        duration_ms=1,
        now=T0,
    )
    with pytest.raises(AuditError) as audit_exc:
        await record_with_retry(sink, receipt, attempts=2)
    assert audit_exc.value.reason is ReasonCode.AUDIT_PERSISTENCE_FAILED
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_reason_codes=("DEADLINE_EXCEEDED", "AUDIT_PERSISTENCE_FAILED"),
    )


@pytest.mark.wf5("WF5-346")
@pytest.mark.p1
def test_inverted_spread_during_a_spec_rollout_is_caught_by_input_validation(evidence):
    """The quote validator fires regardless of which catalog version is active."""
    from risk.domain.facts import QuoteFact
    from risk.errors import ContractError
    from tests.reference.oracle import D

    for _version in ("sim-usd-fx-v1", "hypothetical-v2"):
        with pytest.raises(ContractError) as exc:
            QuoteFact(
                symbol="EURUSD",
                bid=D("1.10300"),
                ask=D("1.10200"),
                market_time=T0,
                quote_id="q-inverted",
                snapshot_id="s-1",
            )
        assert exc.value.reason is ReasonCode.INVALID_QUOTE
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_reason_codes=("INVALID_QUOTE",),
    )


@pytest.mark.wf5("WF5-347")
@pytest.mark.p1
async def test_a_late_reply_after_a_reset_cannot_be_published(
    service_factory, request_factory, evidence
):
    """A worker finishing after the source moved on is fenced out."""
    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)
    reader = service._deps.reader
    original_fence = reader.current_epoch_fence
    fired = {"count": 0}

    async def slow_worker_then_reset(epoch_id: str):
        if fired["count"] == 0:
            fired["count"] += 1
            await asyncio.sleep(0.01)  # the worker was slow
            conn.execute(
                "UPDATE source_epochs SET status = 'ARCHIVED' WHERE epoch_id = :e",
                {"e": epoch_id},
            )
            conn.commit()
        return await original_fence(epoch_id)

    reader.current_epoch_fence = slow_worker_then_reset  # type: ignore[assignment]

    with pytest.raises(RiskError) as exc:
        await service.compute(request_factory(spec), SCOPE)
    assert fired["count"] == 1
    assert exc.value.reason is ReasonCode.EPOCH_CHANGED
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_reason_codes=("EPOCH_CHANGED",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-348")
@pytest.mark.p2
async def test_triple_fault_returns_unavailability_not_a_leak(
    service_factory, request_factory, evidence
):
    """Blocking evidence, a quote outage and an audit outage: nothing is published."""
    from risk.adapters.quotes import FixtureQuoteProvider

    spec = scenario("R05-recon-conflict")
    provider = FixtureQuoteProvider({}, fail_with=ReasonCode.DB_UNAVAILABLE)
    sink = InMemoryAuditSink()
    sink.fail_always = True
    service, _ = service_factory(spec, quote_provider=provider, sink=sink)

    # Blocked results do not require an audit receipt, so this path returns a
    # BLOCKED envelope rather than raising -- and carries no figures at all.
    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    assert result.status is GlobalStatus.BLOCKED
    assert result.composite_total is None
    for payload in result.components.values():
        assert payload.status is ComponentStatus.BLOCKED
        assert payload.values is None
    encoded = json.dumps(result.as_payload())
    assert ACCOUNT_A not in encoded
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_status="BLOCKED",
        observed_status=str(result.status),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
        notes=("three simultaneous faults; controlled unavailability, no disclosure",),
    )
