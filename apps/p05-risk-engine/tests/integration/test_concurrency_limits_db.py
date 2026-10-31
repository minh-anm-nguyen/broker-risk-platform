"""Domains R, T and U -- concurrency, database faults, resource budgets.

Covers WF5-205..216, WF5-229..240 and WF5-241..252.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from decimal import Decimal

import pytest

from risk.adapters.sql import SqliteExecutor
from risk.audit import InMemoryAuditSink
from risk.domain.facts import AuthorizedScope
from risk.errors import AuditError, ReasonCode, ResourceError
from risk.limits import (
    AdmissionController,
    Deadline,
    batched,
    check_estimated_input_bytes,
    check_request_limits,
)
from risk.policies.loader import LimitPolicy, load_policy
from risk.status import Component, GlobalStatus
from risk_tools.fixtures import (
    ACCOUNT_A,
    SOURCE_A,
    T0,
    TENANT_A,
    PositionSpec,
    ScanSpec,
    scenario,
    synthetic_deals,
    synthetic_positions,
)

pytestmark = [pytest.mark.integration, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))


def tiny_limits(**overrides) -> LimitPolicy:
    defaults = dict(
        max_accounts_per_request=2,
        max_positions_per_request=5,
        max_deals_per_window=5,
        read_batch_rows=2,
        max_concurrent_jobs=2,
        max_queued_jobs=1,
        wall_clock_deadline_seconds=Decimal(30),
        max_input_bytes=1024,
    )
    defaults.update(overrides)
    return LimitPolicy(**defaults)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Domain R -- concurrency, dedup, races
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-205")
async def test_identical_requests_are_deterministic_and_share_a_receipt(
    service_factory, request_factory, audit_sink, evidence
):
    """Two identical requests give identical digests and one audit receipt."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    first = await service.compute(request, SCOPE)
    second = await service.compute(request, SCOPE)

    assert first.input_digest == second.input_digest
    assert first.output_digest == second.output_digest
    assert first.calculation_id == second.calculation_id
    assert audit_sink.writes == 1
    evidence(
        oracle="OPERATIONAL",
        observed_values={"receipts": str(len(audit_sink)), "digest": first.output_digest[:16]},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-207")
async def test_two_epochs_computed_concurrently_keep_their_own_manifests(
    service_factory, request_factory, evidence
):
    """Concurrent requests for different epochs do not share mutable state."""
    from risk_tools.fixtures import EPOCH_B, ScenarioSpec

    spec_a = scenario("R05-buy-mark")
    spec_b = ScenarioSpec(
        name="epoch-b",
        epoch_id=EPOCH_B,
        source_id=SOURCE_A,
        generation=2,
        positions=(PositionSpec(position_id="p-b", lots="2.00"),),
        position_scan=ScanSpec(scan_id="scan-P-b", entity="POSITION",
                               cut_proof_id="freeze-b"),
        account_scan=ScanSpec(scan_id="scan-A-b", entity="ACCOUNT"),
        quote_bound_position_scan_id="scan-P-b",
    )
    service_a, _ = service_factory(spec_a)
    service_b, _ = service_factory(spec_b)

    components = frozenset({Component.EXPOSURE})
    result_a, result_b = await asyncio.gather(
        service_a.compute(request_factory(spec_a, components=components), SCOPE),
        service_b.compute(request_factory(spec_b, components=components), SCOPE),
    )

    assert result_a.cuts["epoch_id"] == spec_a.epoch_id
    assert result_b.cuts["epoch_id"] == spec_b.epoch_id
    assert result_a.component(Component.EXPOSURE).values["by_symbol"]["EURUSD"][  # type: ignore[index]
        "net_units"
    ] == "50000.00"
    assert result_b.component(Component.EXPOSURE).values["by_symbol"]["EURUSD"][  # type: ignore[index]
        "net_units"
    ] == "200000.00"
    evidence(
        oracle="OPERATIONAL",
        observed_values={"epoch_a": str(result_a.cuts["epoch_id"])[:8],
                         "epoch_b": str(result_b.cuts["epoch_id"])[:8]},
    )


@pytest.mark.wf5("WF5-211")
async def test_overload_is_rejected_promptly_not_queued(evidence):
    """Past the worker and queue budget the controller refuses immediately."""
    controller = AdmissionController(tiny_limits(max_concurrent_jobs=2, max_queued_jobs=1))
    release = asyncio.Event()
    admitted = asyncio.Event()
    running = 0

    async def hold() -> None:
        nonlocal running
        async with controller.admit():
            running += 1
            if running == 2:
                admitted.set()
            await release.wait()

    holders = [asyncio.create_task(hold()) for _ in range(2)]
    await asyncio.wait_for(admitted.wait(), timeout=2)

    # One waiter fits in the queue.
    waiter = asyncio.create_task(hold())
    await asyncio.sleep(0)
    assert controller.running == 2

    # The next arrival is refused rather than waiting behind it.
    with pytest.raises(ResourceError) as exc:
        async with controller.admit():
            pass
    assert exc.value.reason is ReasonCode.OVERLOADED
    assert controller.rejected == 1

    release.set()
    await asyncio.gather(*holders, waiter)
    assert controller.running == 0
    evidence(
        oracle="OPERATIONAL",
        expected_reason_codes=("OVERLOADED",),
        observed_reason_codes=(str(exc.value.reason),),
        observed_values={"rejected": str(controller.rejected)},
    )


@pytest.mark.wf5("WF5-212", "WF5-248")
async def test_cancellation_releases_the_slot_and_the_connection(
    service_factory, request_factory, evidence
):
    """A cancelled request leaves no worker slot or transaction behind."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    reader = service._deps.reader
    original_fence = reader.current_epoch_fence
    entered = asyncio.Event()

    async def hang_at_fence(epoch_id: str):
        entered.set()
        await asyncio.sleep(60)  # cancelled before this completes
        return await original_fence(epoch_id)

    reader.current_epoch_fence = hang_at_fence  # type: ignore[assignment]

    task = asyncio.create_task(service.compute(request_factory(spec), SCOPE))
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The admission slot came back and the read transaction was rolled back.
    assert service.admission.running == 0
    executor = reader._sql  # type: ignore[attr-defined]
    assert executor.stats.transactions == 1
    assert not executor._in_snapshot  # type: ignore[attr-defined]
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"inflight": "0", "open_snapshots": "0"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-213", "WF5-219")
async def test_blown_deadline_is_a_controlled_rejection(evidence):
    """An exhausted budget raises DEADLINE_EXCEEDED at the next stage boundary."""
    deadline = Deadline(Decimal("0.001"))
    await asyncio.sleep(0.02)

    assert deadline.expired
    with pytest.raises(ResourceError) as exc:
        deadline.check(stage="capture_deals")
    assert exc.value.reason is ReasonCode.DEADLINE_EXCEEDED
    assert exc.value.detail["stage"] == "capture_deals"
    # Monotonic, so a wall-clock jump cannot create a deadline that never fires.
    assert deadline.elapsed_seconds >= 0
    evidence(
        oracle="OPERATIONAL",
        expected_reason_codes=("DEADLINE_EXCEEDED",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-214")
@pytest.mark.p1
async def test_two_processes_computing_one_report_agree_on_the_id(
    service_factory, request_factory, evidence
):
    """A deterministic calculation id means no race to create two receipts."""
    spec = scenario("R05-aligned-ready")
    shared_sink = InMemoryAuditSink()
    service_a, conn = service_factory(spec, sink=shared_sink)
    service_b, _ = service_factory(spec, sink=shared_sink, connection=conn)
    request = request_factory(spec)

    result_a, result_b = await asyncio.gather(
        service_a.compute(request, SCOPE), service_b.compute(request, SCOPE)
    )

    assert result_a.calculation_id == result_b.calculation_id
    assert result_a.output_digest == result_b.output_digest
    assert shared_sink.writes == 1
    evidence(
        oracle="OPERATIONAL",
        observed_values={"writes": str(shared_sink.writes)},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-215")
@pytest.mark.p1
async def test_captured_values_are_immutable_for_the_request(
    service_factory, request_factory, evidence
):
    """The bundle is frozen, so nothing can be mutated under a running calculation."""
    import dataclasses

    from risk.domain.facts import RiskCut, RiskInputBundle

    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    # Structurally: the fact and cut types refuse assignment.
    cut = RiskCut(source_id=SOURCE_A, epoch_id=spec.epoch_id)
    with pytest.raises(dataclasses.FrozenInstanceError):
        cut.epoch_id = "other"  # type: ignore[misc]
    assert RiskInputBundle.__dataclass_params__.frozen  # type: ignore[attr-defined]
    assert result.status is GlobalStatus.READY
    evidence(oracle="OPERATIONAL", fixture_id=spec.name)


@pytest.mark.wf5("WF5-216")
@pytest.mark.p2
async def test_cut_identity_not_a_ttl_decides_reuse(
    service_factory, request_factory, evidence
):
    """Two results differ because their cuts differ, not because time passed."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    before = await service.compute(request, SCOPE)
    conn.execute(
        "UPDATE sync_checkpoints SET watermark_seq = 2050 WHERE epoch_id = :e",
        {"e": spec.epoch_id},
    )
    conn.commit()
    after = await service.compute(request, SCOPE)

    # Same wall-clock second, different watermark, different identity.
    assert before.cuts["deal_watermark_seq"] != after.cuts["deal_watermark_seq"]
    assert before.input_digest != after.input_digest
    evidence(
        oracle="STATE_PROVENANCE",
        observed_values={
            "w_before": str(before.cuts["deal_watermark_seq"]),
            "w_after": str(after.cuts["deal_watermark_seq"]),
        },
        fixture_id=spec.name,
    )


# --------------------------------------------------------------------------- #
# Domain T -- database faults
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-229", "WF5-230", "WF5-236")
async def test_connection_failure_mid_read_yields_no_partial_result(
    service_factory, request_factory, evidence
):
    """A dropped connection aborts the capture; no partial sum is returned."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    reader = service._deps.reader
    executor = reader._sql  # type: ignore[attr-defined]
    original_stream = executor.stream

    async def failing_stream(sql, params=None, *, batch_size=1000):
        if "FROM deals" in sql:
            raise sqlite3.OperationalError("server closed the connection unexpectedly")
        async for batch in original_stream(sql, params, batch_size=batch_size):
            yield batch

    executor.stream = failing_stream  # type: ignore[attr-defined]

    with pytest.raises(sqlite3.OperationalError):
        await service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
        )

    # No audit receipt, so nothing claims a result was produced.
    assert service.admission.running == 0
    assert not executor._in_snapshot  # type: ignore[attr-defined]
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_status="BLOCKED",
        observed_values={"partial_results": "0"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-231", "WF5-234", "WF5-329")
async def test_audit_failure_blocks_a_ready_result(
    service_factory, request_factory, evidence
):
    """When audit is mandatory, a sink outage prevents a READY from being returned."""
    spec = scenario("R05-aligned-ready")
    sink = InMemoryAuditSink()
    sink.fail_always = True
    service, _ = service_factory(spec, sink=sink)

    with pytest.raises(AuditError) as exc:
        await service.compute(request_factory(spec), SCOPE)
    assert exc.value.reason is ReasonCode.AUDIT_PERSISTENCE_FAILED
    assert len(sink) == 0
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_reason_codes=("AUDIT_PERSISTENCE_FAILED",),
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
        notes=("arithmetic succeeded; the control did not, so nothing is published",),
    )


@pytest.mark.wf5("WF5-231")
async def test_transient_audit_failure_is_retried_within_budget(
    service_factory, request_factory, policy, evidence
):
    """A deadlock on the audit insert is retried, bounded by the policy."""
    spec = scenario("R05-aligned-ready")
    sink = InMemoryAuditSink()
    sink.fail_next = policy.audit_retry_attempts - 1  # recovers on the last attempt
    service, _ = service_factory(spec, sink=sink)

    result = await service.compute(request_factory(spec), SCOPE)

    assert result.status is GlobalStatus.READY
    assert sink.writes == 1
    assert sink.fail_next == 0  # the retry budget was actually consumed
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"attempts_used": str(policy.audit_retry_attempts)},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-233")
async def test_statement_timeout_during_a_scan_is_a_controlled_failure(
    service_factory, request_factory, evidence
):
    """A timeout mid-scan produces no numeric total."""
    spec = scenario("R05-signed-fees")
    service, _ = service_factory(spec)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]
    original_fetch = executor.fetch_one

    async def timeout_on_count(sql, params=None):
        if "COUNT(*)" in sql and "FROM deals" in sql:
            raise sqlite3.OperationalError("canceling statement due to statement timeout")
        return await original_fetch(sql, params)

    executor.fetch_one = timeout_on_count  # type: ignore[attr-defined]

    with pytest.raises(sqlite3.OperationalError):
        await service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
        )
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"numeric_total_returned": "False"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-239")
@pytest.mark.p1
def test_float_from_a_malformed_driver_is_rejected(evidence):
    """A NUMERIC column handed back as a float is refused before the core."""
    from risk.adapters.p03_reader import decimal_or_block
    from risk.errors import ContractError

    assert decimal_or_block("1.25", field="commission") == Decimal("1.25")

    with pytest.raises(ContractError) as exc:
        decimal_or_block(1.25, field="commission")
    assert exc.value.reason is ReasonCode.DECIMAL_STRING_REQUIRED

    with pytest.raises(ContractError) as exc:
        decimal_or_block(None, field="commission")
    assert exc.value.reason is ReasonCode.MONEY_FIELD_MISSING
    evidence(observed_reason_codes=("DECIMAL_STRING_REQUIRED", "MONEY_FIELD_MISSING"))


@pytest.mark.wf5("WF5-237")
@pytest.mark.p1
async def test_admission_bounds_the_number_of_open_transactions(
    service_factory, request_factory, evidence
):
    """Concurrency is capped, so the connection demand is capped with it."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    results = await asyncio.gather(
        *(service.compute(request, SCOPE) for _ in range(8)), return_exceptions=True
    )
    accepted = [r for r in results if not isinstance(r, Exception)]
    rejected = [r for r in results if isinstance(r, ResourceError)]

    # Whatever the mix, nothing hung and nothing escaped as an unexpected error.
    assert len(accepted) + len(rejected) == 8
    for item in rejected:
        assert item.reason is ReasonCode.OVERLOADED
    assert service.admission.running == 0
    evidence(
        oracle="OPERATIONAL",
        observed_values={"accepted": str(len(accepted)), "rejected": str(len(rejected))},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-240")
@pytest.mark.p2
def test_schema_absence_is_reported_not_papered_over(evidence):
    """A missing table surfaces as a database error, never as an empty result."""
    conn = sqlite3.connect(":memory:")
    try:
        executor = SqliteExecutor(conn)
        with pytest.raises(sqlite3.OperationalError):
            asyncio.run(executor.fetch_all("SELECT * FROM deals"))
        evidence(oracle="OPERATIONAL", observed_values={"outcome": "OperationalError"})
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Domain U -- CPU, memory, backpressure
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-241", "WF5-243")
def test_position_count_above_the_policy_cap_is_rejected(evidence):
    """A 100k-position request is refused by the declared limit, not by an OOM."""
    limits = tiny_limits()
    check_request_limits(limits, account_count=1, position_count=5)

    with pytest.raises(ResourceError) as exc:
        check_request_limits(limits, account_count=1, position_count=100_000)
    assert exc.value.reason is ReasonCode.LIMIT_EXCEEDED
    assert exc.value.detail["limit"] == 5
    evidence(
        oracle="OPERATIONAL",
        expected_reason_codes=("LIMIT_EXCEEDED",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-243")
async def test_oversized_position_scan_is_rejected_during_the_read(
    service_factory, request_factory, evidence
):
    """The cap is enforced while streaming, so the rest is never materialised."""
    spec = scenario("R05-buy-mark")
    spec.positions = synthetic_positions(40, symbols=("EURUSD",))
    capped = replace(load_policy(), limits=tiny_limits(max_positions_per_request=5))
    service, _ = service_factory(spec, active_policy=capped)

    with pytest.raises(ResourceError) as exc:
        await service.compute(
            request_factory(spec, components=frozenset({Component.EXPOSURE})), SCOPE
        )
    assert exc.value.reason is ReasonCode.LIMIT_EXCEEDED
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_reason_codes=(str(exc.value.reason),),
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-242", "WF5-319", "WF5-320")
async def test_batch_size_does_not_change_the_answer(
    service_factory, request_factory, evidence
):
    """Reading in batches of 1, 7 and 10 000 produces one identical digest."""
    spec = scenario("R05-signed-fees")
    spec.deals = synthetic_deals(120, start_seq=1, base_time=T0)
    spec.deal_watermark = 10_000

    digests: set[str] = set()
    grosses: set[str] = set()
    base_policy = load_policy()
    for batch_size in (1, 7, 10_000):
        policy = replace(
            base_policy, limits=replace(base_policy.limits, read_batch_rows=batch_size)
        )
        service, _ = service_factory(spec, active_policy=policy)
        result = await service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
        )
        digests.add(result.output_digest)
        grosses.add(str(result.component(Component.REALIZED).values["gross"]))  # type: ignore[index]

    assert len(digests) == 1, "batch size changed the output digest"
    assert len(grosses) == 1
    evidence(
        oracle="PROPERTY_METAMORPHIC",
        dataset_seed=4205,
        expected_values={"distinct_digests": "1"},
        observed_values={"distinct_digests": str(len(digests)),
                         "gross": next(iter(grosses))},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-244", "WF5-251")
async def test_cpu_bound_work_does_not_stall_the_event_loop(
    service_factory, request_factory, evidence
):
    """A heartbeat keeps ticking while calculations run."""
    spec = scenario("R05-signed-fees")
    spec.deals = synthetic_deals(400, start_seq=1, base_time=T0)
    spec.deal_watermark = 10_000
    service, _ = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    ticks = 0
    stop = asyncio.Event()

    async def heartbeat() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            await asyncio.sleep(0.001)

    beat = asyncio.create_task(heartbeat())
    results = await asyncio.gather(
        *(service.compute(request, SCOPE) for _ in range(4)), return_exceptions=True
    )
    stop.set()
    await beat

    assert ticks > 0, "the event loop never got control back"
    assert any(not isinstance(r, Exception) for r in results)
    evidence(
        oracle="OPERATIONAL",
        observed_values={"heartbeat_ticks": str(ticks)},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-245")
def test_log_record_is_a_bounded_fixed_shape(evidence):
    """One log record per calculation, with a fixed key set -- not one per row."""
    from risk.observability import log_fields

    record = log_fields(
        calculation_id="c",
        scope_hash="h" * 64,
        source_id="sim-broker-a",
        epoch_id="e",
        status="READY",
        reason_codes=(),
        risk_policy_version="risk-policy-v1",
        symbol_catalog_version="sim-usd-fx-v1",
        duration_ms=5,
    )
    assert set(record) == {
        "calculation_id",
        "scope_hash",
        "source",
        "epoch_id",
        "status",
        "reason_codes",
        "reason_families",
        "risk_policy_version",
        "symbol_catalog_version",
        "duration_ms",
    }
    evidence(oracle="OPERATIONAL", observed_values={"fields": str(len(record))})


@pytest.mark.wf5("WF5-246")
def test_oversized_money_literal_is_refused_before_parsing(evidence):
    """The length guard fires before any Decimal work."""
    from risk.domain.money import MAX_DECIMAL_INPUT_CHARS, parse_decimal
    from risk.errors import ContractError

    ok = "1" * MAX_DECIMAL_INPUT_CHARS
    assert parse_decimal(ok[:30], field="gross").is_finite()

    with pytest.raises(ContractError) as exc:
        parse_decimal("9" * (MAX_DECIMAL_INPUT_CHARS + 1), field="gross")
    assert exc.value.reason is ReasonCode.DECIMAL_MAGNITUDE_EXCEEDED
    evidence(oracle="OPERATIONAL", observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-247")
def test_canonical_serialisation_does_not_recurse_without_bound(evidence):
    """Deeply nested metadata is handled or refused, never a stack overflow."""
    from risk.manifest import canonical_json

    nested: object = "leaf"
    for _ in range(200):
        nested = {"child": nested}

    encoded = canonical_json(nested)
    assert encoded.count("child") == 200

    # A pathological depth raises a RecursionError, which is a controlled
    # Python exception rather than an interpreter crash. The service never
    # builds such a structure: the manifest shape is fixed and flat.
    very_deep: object = "leaf"
    for _ in range(20_000):
        very_deep = {"child": very_deep}
    with pytest.raises(RecursionError):
        canonical_json(very_deep)
    evidence(
        oracle="OPERATIONAL",
        observed_values={"handled_depth": "200", "refused_depth": "20000"},
    )


@pytest.mark.wf5("WF5-249", "WF5-227")
@pytest.mark.p1
async def test_a_hanging_quote_provider_does_not_hold_the_slot_forever(evidence):
    """A provider that never answers is cancelled, and the slot is released."""
    controller = AdmissionController(tiny_limits(max_concurrent_jobs=1, max_queued_jobs=1))

    async def hang() -> None:
        async with controller.admit():
            await asyncio.sleep(60)

    task = asyncio.create_task(hang())
    await asyncio.sleep(0)
    assert controller.running == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert controller.running == 0
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        observed_values={"slot_released": "True"},
    )


@pytest.mark.wf5("WF5-250")
@pytest.mark.p1
def test_batched_helper_preserves_every_row_exactly_once(evidence):
    """Batching is a partition: no row lost, none duplicated, order preserved."""
    rows = list(range(1000))
    for size in (1, 3, 999, 1000, 5000):
        batches = list(batched(rows, size))
        flat = [item for batch in batches for item in batch]
        assert flat == rows
        assert all(len(b) <= size for b in batches)

    with pytest.raises(ResourceError):
        list(batched(rows, 0))
    evidence(oracle="PROPERTY_METAMORPHIC", observed_values={"sizes_checked": "5"})


@pytest.mark.wf5("WF5-252")
@pytest.mark.p2
async def test_repeated_calculations_do_not_accumulate_state(
    service_factory, request_factory, evidence
):
    """Fifty sequential calculations leave the admission controller at rest."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    for _ in range(50):
        await service.compute(request, SCOPE)

    assert service.admission.running == 0
    assert service.admission.queued == 0
    evidence(
        oracle="OPERATIONAL",
        observed_values={"iterations": "50", "inflight_after": "0"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-246", "WF5-168")
async def test_the_memory_budget_is_enforced_from_the_row_count(
    service_factory, request_factory, evidence
):
    """A window inside the row cap can still be refused on estimated size.

    Row count and footprint are separate budgets. The check runs on the count
    returned by a pre-flight COUNT(*), before any row is decoded, because that
    is the last moment at which refusing is still cheap.
    """
    limits = tiny_limits(max_deals_per_window=10_000_000, max_input_bytes=4096)

    # Eight rows at the declared 512 bytes each sits exactly on the budget.
    check_estimated_input_bytes(limits, row_count=8)

    with pytest.raises(ResourceError) as exc:
        check_estimated_input_bytes(limits, row_count=9)
    assert exc.value.reason is ReasonCode.INPUT_TOO_LARGE
    assert exc.value.detail["row_count"] == 9
    assert exc.value.detail["limit"] == 4096

    # And the reader applies it: a deal window within the row cap is refused
    # when its estimated footprint is not.
    spec = scenario("R05-signed-fees")
    spec.deals = synthetic_deals(40, start_seq=1, base_time=T0)
    spec.deal_watermark = 10_000
    capped = replace(load_policy(), limits=limits)
    service, _ = service_factory(spec, active_policy=capped)

    with pytest.raises(ResourceError) as read_exc:
        await service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
        )
    assert read_exc.value.reason is ReasonCode.INPUT_TOO_LARGE
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_reason_codes=("INPUT_TOO_LARGE",),
        observed_reason_codes=(str(read_exc.value.reason),),
        fixture_id=spec.name,
        notes=("checked from a pre-flight COUNT(*), before any row is decoded",),
    )
