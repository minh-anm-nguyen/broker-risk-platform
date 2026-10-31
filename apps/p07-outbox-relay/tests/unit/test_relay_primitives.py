"""Backoff, the circuit breaker, admission limits, settings and the taxonomy.

Small pieces, but each one guards a specific FMEA row, so each gets a test that
names it. The settings tests are the ones to keep: a configuration that cannot
hold -- a lease shorter than the delivery deadline, an in-flight cap the buffer
cannot carry -- must fail at startup, because the alternative is discovering it
during an outage.
"""

from __future__ import annotations

import random

import pytest

from outbox.clock import ManualClock, from_wire, to_wire
from outbox.errors import (
    DISPOSITIONS,
    BrokerError,
    ConfigurationError,
    Disposition,
    ErrorCode,
    MessageTooLarge,
    ProducerQueueFull,
    classify,
)
from outbox.persistence.ports import AttemptOutcome
from outbox.relay.backoff import CircuitBreaker, compute_delay
from outbox.relay.limits import Admission, InFlightLimits, StoragePolicy
from outbox.settings import load_settings

pytestmark = pytest.mark.unit


class TestBackoff:
    def test_doubles_from_the_base(self) -> None:
        delays = [
            compute_delay(attempt=attempt, base_seconds=0.5, max_seconds=60, jitter_ratio=0.0)
            for attempt in range(1, 6)
        ]
        assert delays == [0.5, 1.0, 2.0, 4.0, 8.0]

    def test_is_capped(self) -> None:
        assert (
            compute_delay(attempt=40, base_seconds=0.5, max_seconds=60, jitter_ratio=0.0) == 60
        )

    def test_a_huge_attempt_count_does_not_compute_a_huge_power(self) -> None:
        # The exponent is clamped before the shift, so this returns rather than
        # building a 300-bit integer on its way to being capped anyway.
        assert compute_delay(attempt=10_000, base_seconds=1, max_seconds=30, jitter_ratio=0.0) == 30

    def test_jitter_only_ever_subtracts(self) -> None:
        rng = random.Random(7)
        for attempt in range(1, 30):
            delay = compute_delay(
                attempt=attempt, base_seconds=1, max_seconds=10, jitter_ratio=0.5, rng=rng
            )
            # An additive jitter would quietly raise the cap.
            assert 0 < delay <= 10

    def test_jitter_spreads_identical_attempts(self) -> None:
        rng = random.Random(1)
        delays = {
            compute_delay(attempt=5, base_seconds=1, max_seconds=60, jitter_ratio=0.25, rng=rng)
            for _ in range(50)
        }
        # Without this, ten thousand rows that failed together become due
        # together and the recovery is the next outage.
        assert len(delays) > 40

    @pytest.mark.parametrize(
        ("attempt", "base", "cap", "jitter"),
        [(0, 1, 10, 0.0), (1, 0, 10, 0.0), (1, 10, 1, 0.0), (1, 1, 10, 1.5), (1, 1, 10, -0.1)],
    )
    def test_invalid_parameters_raise(self, attempt: int, base: float, cap: float, jitter: float) -> None:
        with pytest.raises(ValueError):
            compute_delay(attempt=attempt, base_seconds=base, max_seconds=cap, jitter_ratio=jitter)


class TestCircuitBreaker:
    def test_opens_after_the_threshold(self) -> None:
        breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=10)
        assert not breaker.record_failure(monotonic=0)
        assert not breaker.record_failure(monotonic=1)
        assert breaker.record_failure(monotonic=2)
        assert breaker.is_open(monotonic=2)
        assert breaker.opened_count == 1

    def test_a_success_resets_the_run(self) -> None:
        breaker = CircuitBreaker(failure_threshold=2, cooldown_seconds=10)
        breaker.record_failure(monotonic=0)
        breaker.record_success()
        assert not breaker.record_failure(monotonic=1)
        assert not breaker.is_open(monotonic=1)

    def test_half_opens_after_the_cooldown(self) -> None:
        breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=10)
        breaker.record_failure(monotonic=0)
        assert breaker.is_open(monotonic=5)
        assert breaker.seconds_until_probe(monotonic=5) == 5
        assert not breaker.is_open(monotonic=10)

    def test_a_failed_probe_re_arms_the_cooldown(self) -> None:
        breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=10)
        breaker.record_failure(monotonic=0)
        breaker.record_failure(monotonic=10)
        # Without re-arming, the circuit would flap open and closed once per cycle.
        assert breaker.is_open(monotonic=12)


class TestInFlightLimits:
    def test_admits_until_the_count_cap(self) -> None:
        limits = InFlightLimits(max_inflight=2, max_buffer_bytes=1_000_000, max_event_bytes=1000)
        assert limits.admit() is Admission.OK
        limits.acquire(aggregate_key="A", size_bytes=10)
        limits.acquire(aggregate_key="B", size_bytes=10)
        assert limits.admit() is Admission.INFLIGHT_FULL
        limits.release(aggregate_key="A", size_bytes=10)
        assert limits.admit() is Admission.OK

    def test_admits_against_the_worst_case_event_size(self) -> None:
        # Judging on average size lets a run of large events overshoot the
        # budget: 600 bytes are held and the cap is 1500, so a further *average*
        # event would fit while a further worst-case one would not.
        limits = InFlightLimits(max_inflight=100, max_buffer_bytes=1500, max_event_bytes=1000)
        limits.acquire(aggregate_key="A", size_bytes=600)
        assert limits.admit() is Admission.BUFFER_FULL

    def test_headroom_is_the_tighter_of_the_two_caps(self) -> None:
        limits = InFlightLimits(max_inflight=50, max_buffer_bytes=5000, max_event_bytes=1000)
        assert limits.headroom() == 5

    def test_a_second_record_for_one_aggregate_is_a_bug_not_a_wait(self) -> None:
        limits = InFlightLimits(max_inflight=10, max_buffer_bytes=1_000_000, max_event_bytes=1000)
        limits.acquire(aggregate_key="A-42", size_bytes=10)
        with pytest.raises(AssertionError, match="already has a record in flight"):
            limits.acquire(aggregate_key="A-42", size_bytes=10)

    def test_double_release_cannot_drive_the_counters_negative(self) -> None:
        limits = InFlightLimits(max_inflight=10, max_buffer_bytes=1000, max_event_bytes=100)
        limits.acquire(aggregate_key="A", size_bytes=10)
        limits.release(aggregate_key="A", size_bytes=10)
        limits.release(aggregate_key="A", size_bytes=10)
        assert limits.inflight == 0 and limits.buffered_bytes == 0


class TestStoragePolicy:
    @pytest.mark.parametrize(
        ("used", "expected"),
        [(0, "OK"), (79, "OK"), (80, "WARN"), (90, "THROTTLE"), (95, "FAIL_CLOSED"), (200, "FAIL_CLOSED")],
    )
    def test_levels(self, used: int, expected: str) -> None:
        policy = StoragePolicy(
            budget_bytes=100, warn_ratio=0.8, throttle_ratio=0.9, fail_closed_ratio=0.95
        )
        assert policy.level(used) == expected

    def test_fail_closed_stops_new_business_writes(self) -> None:
        policy = StoragePolicy(
            budget_bytes=100, warn_ratio=0.8, throttle_ratio=0.9, fail_closed_ratio=0.95
        )
        assert policy.accepts_new_business_writes(90)
        assert not policy.accepts_new_business_writes(96)


class TestErrorTaxonomy:
    def test_every_error_code_has_a_disposition(self) -> None:
        # A new code without a chosen disposition would silently inherit
        # whatever the lookup defaulted to.
        for code in ErrorCode:
            assert code in DISPOSITIONS

    def test_an_unclassified_exception_fails_closed(self) -> None:
        classification = classify(RuntimeError("something new"))
        assert classification.code is ErrorCode.UNKNOWN
        assert classification.disposition is Disposition.QUARANTINE

    def test_message_too_large_is_never_retried(self) -> None:
        # Auto-truncating a financial field to make it fit is not an option.
        assert classify(MessageTooLarge("too big")).disposition is Disposition.QUARANTINE

    def test_a_full_producer_queue_is_backpressure_not_a_failure(self) -> None:
        assert classify(ProducerQueueFull("full")).disposition is Disposition.BACKPRESSURE

    def test_auth_denied_opens_the_circuit(self) -> None:
        # Looping on a rejected ACL is a denial of service against ourselves.
        assert (
            classify(BrokerError("denied", code=ErrorCode.AUTH_DENIED)).disposition
            is Disposition.CIRCUIT_OPEN
        )

    def test_ambiguous_acks_are_retryable(self) -> None:
        assert classify(BrokerError("?", code=ErrorCode.BROKER_ACK_AMBIGUOUS)).retryable

    def test_attempt_outcomes_are_distinct(self) -> None:
        assert len({str(outcome) for outcome in AttemptOutcome}) == len(list(AttemptOutcome))


class TestClock:
    def test_wire_format_is_fixed_width(self) -> None:
        clock = ManualClock()
        text = to_wire(clock.now())
        assert text == "2026-10-30T06:00:00.000000Z"
        assert from_wire(text) == clock.now()

    def test_fixed_width_means_lexicographic_order_is_chronological(self) -> None:
        clock = ManualClock()
        stamps = [to_wire(clock.advance(0.5)) for _ in range(20)]
        assert stamps == sorted(stamps)

    def test_a_naive_datetime_is_refused(self) -> None:
        from datetime import datetime

        with pytest.raises(ValueError, match="naive"):
            to_wire(datetime(2026, 10, 30, 6, 0, 0))

    @pytest.mark.parametrize("text", ["2026-10-30T06:00:00Z", "2026-10-30T06:00:00.000000", "", "nope"])
    def test_malformed_timestamps_are_refused(self, text: str) -> None:
        with pytest.raises(ValueError):
            from_wire(text)

    def test_manual_clock_advances_both_readings_together(self) -> None:
        clock = ManualClock()
        before = clock.monotonic()
        clock.advance(5)
        assert clock.monotonic() == before + 5


class TestSettings:
    def test_defaults_are_coherent(self) -> None:
        settings = load_settings()
        assert settings.store_backend == "sqlite"
        assert settings.lease_seconds > settings.delivery_deadline_seconds

    def test_a_lease_shorter_than_the_delivery_deadline_is_refused(self) -> None:
        # Otherwise a standby reclaims rows whose produce is still outstanding
        # and duplicates become the normal case rather than a crash case.
        with pytest.raises(ConfigurationError, match="lease_seconds must exceed"):
            load_settings(lease_seconds=5.0, delivery_deadline_seconds=10.0)

    def test_an_unreachable_inflight_cap_is_refused(self) -> None:
        with pytest.raises(ConfigurationError, match="exceeds"):
            load_settings(max_inflight=64, max_event_wire_bytes=1 << 20, producer_buffer_max_bytes=1 << 20)

    def test_claim_batch_may_not_exceed_the_inflight_cap(self) -> None:
        with pytest.raises(ConfigurationError, match="claim_batch_size"):
            load_settings(claim_batch_size=100, max_inflight=10)

    def test_an_empty_allowlist_is_refused(self) -> None:
        with pytest.raises(ConfigurationError, match="at least one source_id"):
            load_settings(source_allowlist=())

    def test_duplicate_sources_are_refused(self) -> None:
        with pytest.raises(ConfigurationError, match="duplicates"):
            load_settings(source_allowlist=("a", "a"))

    def test_storage_ratios_must_increase(self) -> None:
        with pytest.raises(ConfigurationError, match="warn < throttle < fail_closed"):
            load_settings(storage_warn_ratio=0.9, storage_throttle_ratio=0.8)

    def test_postgres_backend_requires_a_dsn(self) -> None:
        with pytest.raises(ConfigurationError, match="requires pg_dsn"):
            load_settings(store_backend="postgres", pg_dsn=None)

    def test_unknown_backend_is_refused(self) -> None:
        with pytest.raises(ConfigurationError, match="must be 'sqlite' or 'postgres'"):
            load_settings(store_backend="mysql")

    def test_csv_environment_values_are_split(self) -> None:
        settings = load_settings(source_allowlist="a, b ,c", supported_schema_versions="1")
        assert settings.source_allowlist == ("a", "b", "c")
        assert settings.supported_schema_versions == (1,)

    def test_settings_are_frozen(self) -> None:
        settings = load_settings()
        with pytest.raises(Exception):
            settings.max_inflight = 1  # type: ignore[misc]
