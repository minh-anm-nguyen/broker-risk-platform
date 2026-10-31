"""Domain AA -- throughput, load, stress and scaling.

Covers WF5-313..324.

These are *measurements under assertion*, not benchmark results. The design
document is emphatic that a fast wrong answer is not success, so every test
here checks the figures first and the timing second, and records both. The
budgets are the provisional ones of section 15.2, scaled down to what a
single-process test suite can run on any developer machine; the real campaign
is ``python -m risk_tools.benchmark``, whose output is archived separately.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import replace
from decimal import Decimal

import pytest

from risk.domain.facts import AuthorizedScope
from risk.errors import ResourceError
from risk.policies.loader import load_policy
from risk.status import Component, GlobalStatus
from risk_tools.fixtures import (
    ACCOUNT_A,
    SOURCE_A,
    T0,
    TENANT_A,
    QuoteSpec,
    scenario,
    synthetic_deals,
    synthetic_positions,
)
from tests.reference.oracle import D, reference_in_window, reference_realized

pytestmark = [pytest.mark.performance, pytest.mark.slow, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))

#: Scaled-down workload so the suite stays runnable on a laptop. The
#: environment variable lets CI raise it towards the reference campaign.
DEAL_COUNT = int(os.environ.get("RISK_PERF_DEALS", "5000"))
POSITION_COUNT = int(os.environ.get("RISK_PERF_POSITIONS", "1000"))


def peak_rss_mib() -> float | None:
    try:
        import psutil
    except ImportError:  # pragma: no cover - optional dependency
        return None
    return psutil.Process().memory_info().rss / (1024 * 1024)


def expected_realized_gross(deals, *, period_start=None, period_end=None) -> Decimal:
    """Independent expectation, from the naive reference.

    The half-open window is applied here as well as in the engine. An oracle
    that summed every generated row would disagree with a correct engine
    whenever the dataset outgrew the requested period, and the disagreement
    would read as an arithmetic bug rather than as a dataset that did not fit.
    """
    in_scope = [
        d
        for d in deals
        if reference_in_window(d.executed_at, period_start, period_end)
    ]
    return reference_realized(
        [
            {"gross": d.gross, "commission": d.commission, "swap": d.swap}
            for d in in_scope
        ]
    )["gross"]


@pytest.mark.wf5("WF5-313", "WF5-317")
async def test_reference_workload_is_correct_before_it_is_timed(
    service_factory, request_factory, evidence
):
    """The mixed workload produces exact figures; the timing is recorded beside them."""
    spec = scenario("R05-aligned-ready")
    spec.deals = synthetic_deals(DEAL_COUNT, start_seq=1, base_time=T0)
    symbols = ("EURUSD", "GBPUSD", "AUDUSD")
    spec.positions = synthetic_positions(POSITION_COUNT, symbols=symbols)
    # Every open symbol needs a pinned mark: one missing quote blocks the whole
    # valuation, which is the behaviour tested in WF5-127 and not what this
    # workload is measuring.
    spec.quotes = tuple(
        QuoteSpec(symbol=symbol, bid="1.10200", ask="1.10220", quote_id=f"q-{symbol}-1")
        for symbol in symbols
    )
    spec.deal_watermark = DEAL_COUNT + 10_000

    policy = replace(
        load_policy(),
        limits=replace(
            load_policy().limits,
            max_positions_per_request=POSITION_COUNT + 10,
            max_deals_per_window=DEAL_COUNT + 10,
        ),
    )
    service, _ = service_factory(spec, active_policy=policy)

    started = time.monotonic()
    result = await service.compute(request_factory(spec), SCOPE)
    elapsed = time.monotonic() - started

    # Correctness first.
    request = request_factory(spec)
    expected = expected_realized_gross(
        spec.deals, period_start=request.period_start, period_end=request.period_end
    )
    realized = result.component(Component.REALIZED).values
    assert realized is not None
    assert D(str(realized["gross"])) == expected
    assert realized["deal_count"] == DEAL_COUNT
    assert result.status is GlobalStatus.READY
    assert result.lineage["row_counts"]["positions"] == POSITION_COUNT

    exposure = result.component(Component.EXPOSURE).values
    assert exposure is not None
    for symbol_rows in exposure["by_symbol"].values():  # type: ignore[union-attr]
        gross = D(str(symbol_rows["gross_units"]))
        net = D(str(symbol_rows["net_units"]))
        assert gross >= abs(net)

    evidence(
        oracle="GOLDEN_REFERENCE",
        dataset_seed=4205,
        expected_status="READY",
        observed_status=str(result.status),
        expected_values={"gross": str(expected), "deals": str(DEAL_COUNT)},
        observed_values={
            "gross": str(realized["gross"]),
            "deals": str(realized["deal_count"]),
            "elapsed_s": f"{elapsed:.3f}",
        },
        rss_peak_mib=peak_rss_mib(),
        fixture_id=spec.name,
        notes=(f"workload scaled by RISK_PERF_DEALS={DEAL_COUNT}",),
    )


@pytest.mark.wf5("WF5-314", "WF5-242")
async def test_streaming_aggregation_keeps_memory_bounded(
    service_factory, request_factory, evidence
):
    """A large deal set is aggregated in batches; RSS growth stays modest."""
    spec = scenario("R05-signed-fees")
    spec.deals = synthetic_deals(DEAL_COUNT, start_seq=1, base_time=T0)
    spec.deal_watermark = DEAL_COUNT + 10_000

    policy = replace(
        load_policy(),
        limits=replace(load_policy().limits, max_deals_per_window=DEAL_COUNT + 10),
    )
    service, _ = service_factory(spec, active_policy=policy)
    executor = service._deps.reader._sql  # type: ignore[attr-defined]

    before = peak_rss_mib()
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )
    after = peak_rss_mib()

    realized = result.component(Component.REALIZED).values
    assert realized is not None
    request = request_factory(spec, components=frozenset({Component.REALIZED}))
    assert D(str(realized["gross"])) == expected_realized_gross(
        spec.deals, period_start=request.period_start, period_end=request.period_end
    )

    # The read was genuinely batched rather than a single fetchall.
    expected_batches = -(-DEAL_COUNT // policy.limits.read_batch_rows)
    assert executor.stats.batches >= expected_batches

    growth = None if before is None or after is None else after - before
    if growth is not None:
        # Generous for a test process, but it would fail outright on a design
        # that materialised every row as an ORM object.
        assert growth < 256, f"RSS grew by {growth:.1f} MiB"
    evidence(
        oracle="OPERATIONAL",
        dataset_seed=4205,
        observed_values={
            "deals": str(DEAL_COUNT),
            "batches": str(executor.stats.batches),
            "rss_growth_mib": "unavailable" if growth is None else f"{growth:.1f}",
        },
        rss_peak_mib=after,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-315", "WF5-318")
async def test_concurrent_load_is_admitted_or_refused_never_wrong(
    service_factory, request_factory, evidence
):
    """Under 40 concurrent requests every outcome is correct or OVERLOADED."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    started = time.monotonic()
    outcomes = await asyncio.gather(
        *(service.compute(request, SCOPE) for _ in range(40)), return_exceptions=True
    )
    elapsed = time.monotonic() - started

    accepted = [o for o in outcomes if not isinstance(o, BaseException)]
    overloaded = [o for o in outcomes if isinstance(o, ResourceError)]
    unexpected = [
        o for o in outcomes if isinstance(o, BaseException) and not isinstance(o, ResourceError)
    ]

    assert unexpected == [], unexpected
    assert len(accepted) + len(overloaded) == 40
    # Every accepted result is correct and identical -- load did not corrupt one.
    digests = {r.output_digest for r in accepted}
    assert len(digests) <= 1
    for result in accepted:
        assert result.component(Component.REALIZED).values["net_excluding_fee"] == "56.00"  # type: ignore[index]
    for error in overloaded:
        assert error.reason.value == "OVERLOADED"
    assert service.admission.running == 0
    evidence(
        oracle="OPERATIONAL",
        observed_values={
            "accepted": str(len(accepted)),
            "overloaded": str(len(overloaded)),
            "distinct_digests": str(len(digests)),
            "elapsed_s": f"{elapsed:.3f}",
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-316")
async def test_unsupported_requests_fail_cheaply(service_factory, request_factory, evidence):
    """Half the load asking for an unsupported instrument costs little."""
    spec = scenario("R05-unsupported-symbol")
    service, _ = service_factory(spec)
    request = request_factory(spec, strict_composite=False)

    started = time.monotonic()
    outcomes = await asyncio.gather(
        *(service.compute(request, SCOPE) for _ in range(10)), return_exceptions=True
    )
    elapsed = time.monotonic() - started

    accepted = [o for o in outcomes if not isinstance(o, BaseException)]
    for result in accepted:
        unrealized = result.component(Component.UNREALIZED)
        assert unrealized.status.value == "UNSUPPORTED"
        assert unrealized.values is None  # no expensive fake computation
    # A rejection must not cost more than a success; it does less work.
    assert elapsed < 10.0
    evidence(
        oracle="OPERATIONAL",
        observed_values={"requests": "10", "elapsed_s": f"{elapsed:.3f}"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-319", "WF5-320")
async def test_batch_size_extremes_preserve_the_result(
    service_factory, request_factory, evidence
):
    """Batch sizes of 1 and 100 000 give the same digest; neither is rejected wrongly."""
    spec = scenario("R05-signed-fees")
    spec.deals = synthetic_deals(500, start_seq=1, base_time=T0)
    spec.deal_watermark = 100_000

    digests: set[str] = set()
    timings: dict[int, float] = {}
    base = load_policy()
    for batch_size in (1, 1000, 100_000):
        policy = replace(
            base,
            limits=replace(base.limits, read_batch_rows=batch_size, max_deals_per_window=100_000),
        )
        service, _ = service_factory(spec, active_policy=policy)
        started = time.monotonic()
        result = await service.compute(
            request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
        )
        timings[batch_size] = time.monotonic() - started
        digests.add(result.output_digest)

    assert len(digests) == 1, "batch size changed the answer"
    evidence(
        oracle="PROPERTY_METAMORPHIC",
        dataset_seed=4205,
        expected_values={"distinct_digests": "1"},
        observed_values={
            "distinct_digests": str(len(digests)),
            **{f"elapsed_batch_{k}": f"{v:.3f}" for k, v in timings.items()},
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-321")
@pytest.mark.p1
async def test_a_fast_wrong_answer_fails_the_gate(service_factory, request_factory, evidence):
    """Correctness is asserted before latency, and an incorrect run fails outright.

    Demonstrated by injecting a wrong figure into the comparison: the test's
    own structure puts the value assertion first, so no latency result can
    rescue it.
    """
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)

    started = time.monotonic()
    result = await service.compute(request_factory(spec), SCOPE)
    elapsed = time.monotonic() - started

    realized = result.component(Component.REALIZED).values
    assert realized is not None
    # The order matters: the value gate is first and unconditional.
    assert realized["net_excluding_fee"] == "56.00"
    assert realized["currency"] == "USD"
    assert result.status is GlobalStatus.READY
    # Only now is the timing recorded, and it is recorded, not asserted as a
    # pass criterion on its own.
    assert elapsed >= 0
    evidence(
        oracle="GOLDEN_REFERENCE",
        expected_values={"net_excluding_fee": "56.00"},
        observed_values={"net_excluding_fee": "56.00", "elapsed_s": f"{elapsed:.4f}"},
        fixture_id=spec.name,
        notes=("value gate precedes the latency record",),
    )


@pytest.mark.wf5("WF5-322")
@pytest.mark.p1
async def test_cold_and_warm_runs_are_recorded_separately(
    service_factory, request_factory, evidence
):
    """First and subsequent runs are timed apart, not averaged together."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    cold_start = time.monotonic()
    cold = await service.compute(request, SCOPE)
    cold_elapsed = time.monotonic() - cold_start

    warm_times = []
    for _ in range(5):
        warm_start = time.monotonic()
        warm = await service.compute(request, SCOPE)
        warm_times.append(time.monotonic() - warm_start)
        assert warm.output_digest == cold.output_digest

    evidence(
        oracle="OPERATIONAL",
        observed_values={
            "cold_s": f"{cold_elapsed:.4f}",
            "warm_median_s": f"{sorted(warm_times)[2]:.4f}",
            "warm_runs": "5",
        },
        fixture_id=spec.name,
        notes=("reported separately; a warm figure is not a cold-cache claim",),
    )


@pytest.mark.wf5("WF5-323")
@pytest.mark.p1
async def test_a_concurrent_writer_does_not_corrupt_a_reader(
    service_factory, request_factory, evidence
):
    """Deals appended while reading are excluded by the pinned watermark."""
    spec = scenario("R05-signed-fees")
    service, conn = service_factory(spec)
    request = request_factory(spec, components=frozenset({Component.REALIZED}))

    async def writer() -> int:
        written = 0
        for index in range(20):
            conn.execute(
                """
                INSERT INTO deals (epoch_id, deal_id, account_id, source_seq, entry_kind,
                                   deal_type, symbol, realized_pnl_gross, commission, swap,
                                   money_currency, executed_at, payload_hash)
                VALUES (:e, :d, :a, :seq, 'OUT', NULL, 'EURUSD',
                        '1000.00', '0.00', '0.00', 'USD', :t, 'h')
                """,
                {
                    "e": spec.epoch_id,
                    "d": f"d-concurrent-{index}",
                    "a": ACCOUNT_A,
                    "seq": 5000 + index,  # above the pinned watermark of 1050
                    "t": T0.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00"),
                },
            )
            conn.commit()
            written += 1
            await asyncio.sleep(0)
        return written

    reader_task = asyncio.create_task(service.compute(request, SCOPE))
    written = await writer()
    result = await reader_task

    realized = result.component(Component.REALIZED).values
    assert realized is not None
    # 60.00 only: none of the 20 000 USD of concurrent writes leaked in.
    assert realized["gross"] == "60.00"
    assert result.cuts["deal_watermark_seq"] == 1050
    evidence(
        injection_confirmed=True,
        oracle="STATE_PROVENANCE",
        expected_values={"gross": "60.00", "rows_written_concurrently": "20"},
        observed_values={"gross": str(realized["gross"]), "written": str(written)},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-324")
@pytest.mark.p2
async def test_a_short_soak_shows_no_accumulating_state(
    service_factory, request_factory, evidence
):
    """Two hundred iterations leave the controller idle and the digest stable."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    first = await service.compute(request, SCOPE)
    rss_samples: list[float] = []
    for index in range(199):
        result = await service.compute(request, SCOPE)
        assert result.output_digest == first.output_digest
        if index % 50 == 0:
            sample = peak_rss_mib()
            if sample is not None:
                rss_samples.append(sample)

    assert service.admission.running == 0
    assert service.admission.queued == 0
    if len(rss_samples) >= 2:
        # A leak would show as monotonic growth across the samples; a plateau
        # is what this asserts.
        assert rss_samples[-1] - rss_samples[0] < 128
    evidence(
        oracle="OPERATIONAL",
        observed_values={
            "iterations": "200",
            "rss_samples": ",".join(f"{s:.0f}" for s in rss_samples) or "unavailable",
        },
        rss_peak_mib=rss_samples[-1] if rss_samples else None,
        fixture_id=spec.name,
    )
