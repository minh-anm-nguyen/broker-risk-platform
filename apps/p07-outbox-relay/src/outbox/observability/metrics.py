"""Metrics, with cardinality bounded by construction.

Design section 11.1 lists the signals and then adds the constraint that shapes
this module: no ``deal_id``, ``event_id``, ``account_id`` or ``claim_token``
labels. Those are unbounded in production, and a metrics backend that receives
them degrades until the dashboards you need during an incident are the ones
that stop working. So the label sets here are closed enums -- outcome, error
class, group and topic -- and there is no API that accepts an arbitrary label
value.

Everything lives on an injectable ``CollectorRegistry`` rather than the global
default, so a test can assert on exact counter values without inheriting
whatever another test left behind.

One naming decision is deliberate and slightly awkward:
``outbox_created_to_ack_seconds``, not ``commit_to_ack``. The row's
``created_at`` comes from ``clock_timestamp()`` *inside* the transaction, which
fires before the commit record is durable, so the interval it measures is
missing the commit window. Calling the metric what it actually measures is
better than publishing an SLO against a number that is quietly optimistic.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from ..errors import ErrorCode
from ..persistence.ports import AttemptOutcome

#: Buckets in seconds. Dense below one second because the draft target is a p99
#: under five seconds at 100 events/s, and a 5s bucket boundary with nothing
#: between 1s and 10s cannot tell you whether you met it.
_ACK_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0)


@dataclass(slots=True)
class Metrics:
    """The relay's and consumer's instrumentation."""

    registry: CollectorRegistry

    outbox_pending_total: Gauge
    outbox_claimed_total: Gauge
    outbox_quarantined_total: Gauge
    outbox_oldest_pending_age_seconds: Gauge
    pg_outbox_storage_bytes: Gauge
    producer_buffer_used_bytes: Gauge
    producer_inflight_records: Gauge
    kafka_consumer_lag_records: Gauge

    outbox_claimed_expired_total: Counter
    outbox_publish_attempts_total: Counter
    outbox_ack_ambiguous_total: Counter
    producer_errors_total: Counter
    consumer_duplicates_total: Counter
    consumer_version_gap_total: Counter
    consumer_quarantined_total: Counter
    consumer_applied_total: Counter
    source_epoch_publication_block_total: Counter
    relay_circuit_open_total: Counter
    relay_backpressure_total: Counter

    outbox_created_to_ack_seconds: Histogram

    @classmethod
    def build(cls, registry: CollectorRegistry | None = None) -> Metrics:
        reg = registry if registry is not None else CollectorRegistry()
        return cls(
            registry=reg,
            outbox_pending_total=Gauge(
                "outbox_pending_total", "Outbox rows awaiting publication", registry=reg
            ),
            outbox_claimed_total=Gauge(
                "outbox_claimed_total", "Outbox rows currently leased to a relay", registry=reg
            ),
            outbox_quarantined_total=Gauge(
                "outbox_quarantined_total",
                "Quarantined outbox rows. Not success: each one needs an owner",
                registry=reg,
            ),
            outbox_oldest_pending_age_seconds=Gauge(
                "outbox_oldest_pending_age_seconds",
                "Age of the oldest unpublished row. Backlog age, which is not backlog count",
                registry=reg,
            ),
            pg_outbox_storage_bytes=Gauge(
                "pg_outbox_storage_bytes", "Bytes used by the outbox tables and indexes", registry=reg
            ),
            producer_buffer_used_bytes=Gauge(
                "producer_buffer_used_bytes", "Bytes queued in the local producer buffer", registry=reg
            ),
            producer_inflight_records=Gauge(
                "producer_inflight_records", "Records enqueued but not yet acknowledged", registry=reg
            ),
            kafka_consumer_lag_records=Gauge(
                "kafka_consumer_lag_records",
                "Committed offset distance behind the high watermark",
                ["group", "topic"],
                registry=reg,
            ),
            outbox_claimed_expired_total=Counter(
                "outbox_claimed_expired_total",
                "Claims reclaimed after a lease expired, i.e. a crashed or stalled owner",
                registry=reg,
            ),
            outbox_publish_attempts_total=Counter(
                "outbox_publish_attempts_total", "Publish attempts by outcome", ["outcome"], registry=reg
            ),
            outbox_ack_ambiguous_total=Counter(
                "outbox_ack_ambiguous_total",
                "Attempts where the broker may hold the record but did not confirm it",
                registry=reg,
            ),
            producer_errors_total=Counter(
                "producer_errors_total", "Producer errors by class", ["code"], registry=reg
            ),
            consumer_duplicates_total=Counter(
                "consumer_duplicates_total",
                "Records recognised as already applied. Expected during crash campaigns",
                registry=reg,
            ),
            consumer_version_gap_total=Counter(
                "consumer_version_gap_total",
                "Aggregates paused because a required version had not arrived",
                registry=reg,
            ),
            consumer_quarantined_total=Counter(
                "consumer_quarantined_total", "Records quarantined by the consumer", ["code"], registry=reg
            ),
            consumer_applied_total=Counter(
                "consumer_applied_total", "Records applied to the projection", registry=reg
            ),
            source_epoch_publication_block_total=Counter(
                "source_epoch_publication_block_total",
                "Event creations refused by an epoch, lease or policy gate",
                registry=reg,
            ),
            relay_circuit_open_total=Counter(
                "relay_circuit_open_total", "Times the publish circuit opened", registry=reg
            ),
            relay_backpressure_total=Counter(
                "relay_backpressure_total", "Claim cycles skipped because local buffers were full", registry=reg
            ),
            outbox_created_to_ack_seconds=Histogram(
                "outbox_created_to_ack_seconds",
                "Row creation to broker acknowledgement. Excludes the commit window, "
                "so it is a lower bound on true commit-to-ack latency",
                buckets=_ACK_BUCKETS,
                registry=reg,
            ),
        )

    # --- typed recording helpers -------------------------------------------
    def record_attempt(self, outcome: AttemptOutcome) -> None:
        self.outbox_publish_attempts_total.labels(outcome=str(outcome)).inc()

    def record_producer_error(self, code: ErrorCode) -> None:
        self.producer_errors_total.labels(code=str(code)).inc()

    def record_consumer_quarantine(self, code: ErrorCode) -> None:
        self.consumer_quarantined_total.labels(code=str(code)).inc()

    def observe_ack_latency(self, seconds: float) -> None:
        # A negative interval means the clocks disagree, not that delivery was
        # instantaneous. Recording 0 keeps the histogram honest without
        # inventing a bucket for impossible values.
        self.outbox_created_to_ack_seconds.observe(max(0.0, seconds))

    def value(self, name: str, **labels: str) -> float:
        """Read one sample back. For tests and the CLI's status output."""
        sample = self.registry.get_sample_value(name, labels or None)
        return 0.0 if sample is None else float(sample)
