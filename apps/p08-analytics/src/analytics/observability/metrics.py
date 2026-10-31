"""The metric set of section 8.3, with cardinality enforced in code.

Section 8.3 bans ``deal_id``, ``account_id``, ``event_id`` and ``tenant_id``
as metric labels. That ban is easy to agree with and easy to violate six
months later, so :meth:`Metrics._check_labels` refuses the forbidden names
outright: a Prometheus scrape that melts under a million label values is an
outage, and finding out in production is worse than a failing unit test.

The metric *taxonomy* also carries design meaning (INV08-23). Raw
observations, unique event ids and canonical facts are three different
numbers, and a dashboard that shows one of them as "deals" will eventually be
read as all three. They are therefore separate series with names that cannot
be confused.
"""

from __future__ import annotations

import threading
from typing import Mapping

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

#: Labels that must never appear on a metric. Unbounded in production.
FORBIDDEN_LABELS: frozenset[str] = frozenset(
    {"deal_id", "account_id", "event_id", "tenant_id", "build_id", "cursor", "symbol"}
)

#: Latency buckets covering an interactive read (section 13.3's p95 target of
#: 500 ms) through to a timeout, so the histogram can show the tail rather
#: than saturating in its last bucket.
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


def metrics_registry() -> CollectorRegistry:
    return CollectorRegistry()


class Metrics:
    """Owns every P08 series. One instance per process."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or metrics_registry()
        self._lock = threading.Lock()

        # -- ingestion: three distinct counts, deliberately (INV08-23) ----
        self.raw_rows_total = Counter(
            "p08_ingest_raw_rows_total",
            "Physical raw observations written to ClickHouse",
            ["topic"],
            registry=self.registry,
        )
        self.unique_event_ids_total = Counter(
            "p08_ingest_unique_event_ids_total",
            "Distinct event_id values accepted at least once",
            ["topic"],
            registry=self.registry,
        )
        self.records_rejected_total = Counter(
            "p08_ingest_records_rejected_total",
            "Records refused by validation, by reason",
            ["reason"],
            registry=self.registry,
        )
        self.raw_insert_ambiguous_total = Counter(
            "p08_raw_insert_ambiguous_total",
            "Insert acknowledgements that could not be proven",
            registry=self.registry,
        )
        self.offset_commits_total = Counter(
            "p08_offset_commits_total",
            "Successful Kafka offset commits",
            ["topic"],
            registry=self.registry,
        )
        self.offset_commit_blocked_total = Counter(
            "p08_offset_commit_blocked_total",
            "Commits withheld because a lower offset was unresolved",
            ["reason"],
            registry=self.registry,
        )

        # -- integrity: any non-zero value here is an incident ------------
        self.conflicting_event_ids_total = Counter(
            "p08_conflicting_event_ids_total",
            "Same event_id observed with different payload hashes",
            registry=self.registry,
        )
        self.semantic_conflicts_total = Counter(
            "p08_semantic_conflicts_total",
            "Same Deal identity observed with different content hashes",
            registry=self.registry,
        )
        self.missing_deal_identity_count = Gauge(
            "p08_missing_deal_identity_count",
            "Deals present in the source manifest but absent from the build",
            ["report_family"],
            registry=self.registry,
        )
        self.unexpected_deal_identity_count = Gauge(
            "p08_unexpected_deal_identity_count",
            "Deals in the build that the source manifest does not list",
            ["report_family"],
            registry=self.registry,
        )

        # -- quarantine ----------------------------------------------------
        self.quarantine_pending = Gauge(
            "p08_quarantine_pending",
            "Unresolved poison receipts",
            registry=self.registry,
        )
        self.partitions_paused = Gauge(
            "p08_partitions_paused",
            "Partitions paused on unresolved poison",
            registry=self.registry,
        )

        # -- lag -----------------------------------------------------------
        self.kafka_partition_lag = Gauge(
            "p08_kafka_partition_lag",
            "High watermark minus committed offset",
            ["topic", "partition"],
            registry=self.registry,
        )
        self.oldest_event_age_seconds = Gauge(
            "p08_oldest_event_age_seconds",
            "Age of the oldest un-ingested event",
            ["topic"],
            registry=self.registry,
        )

        # -- builds --------------------------------------------------------
        self.build_duration_seconds = Histogram(
            "p08_build_duration_seconds",
            "Wall time of a full build attempt",
            ["report_family", "outcome"],
            buckets=(1, 5, 15, 30, 60, 120, 300, 900, 1800),
            registry=self.registry,
        )
        self.build_validation_failed_total = Counter(
            "p08_build_validation_failed_total",
            "Candidate builds that failed an oracle",
            ["reason"],
            registry=self.registry,
        )
        self.builds_published_total = Counter(
            "p08_builds_published_total",
            "Pointer flips that completed",
            ["report_family"],
            registry=self.registry,
        )
        self.publication_age_seconds = Gauge(
            "p08_publication_age_seconds",
            "Age of the currently published build",
            ["report_family"],
            registry=self.registry,
        )

        # -- queries -------------------------------------------------------
        self.query_latency_seconds = Histogram(
            "p08_query_latency_seconds",
            "Analytics read latency",
            ["route", "status"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.query_rejected_total = Counter(
            "p08_query_rejected_total",
            "Reads refused, by reason",
            ["reason"],
            registry=self.registry,
        )
        self.query_in_flight = Gauge(
            "p08_query_in_flight", "Reads currently executing", registry=self.registry
        )
        self.unauthorized_access_total = Counter(
            "p08_unauthorized_access_total",
            "Requests refused by authentication or scope checks",
            ["kind"],
            registry=self.registry,
        )

        # -- storage -------------------------------------------------------
        self.ch_parts_active = Gauge(
            "p08_ch_parts_active", "Active parts per table", ["table"], registry=self.registry
        )
        self.ch_disk_free_bytes = Gauge(
            "p08_ch_disk_free_bytes", "Free bytes on the ClickHouse volume", registry=self.registry
        )
        self.ch_memory_peak_bytes = Gauge(
            "p08_ch_memory_peak_bytes", "Peak query memory observed", registry=self.registry
        )

    @staticmethod
    def _check_labels(labels: Mapping[str, str]) -> None:
        offenders = sorted(set(labels) & FORBIDDEN_LABELS)
        if offenders:
            raise ValueError(
                "high-cardinality labels are forbidden on metrics (section 8.3): "
                + ", ".join(offenders)
            )

    def observe_storage(self, stats) -> None:
        """Mirror a :class:`StorageStats` snapshot into the gauges."""
        for table, parts in stats.active_parts.items():
            self.ch_parts_active.labels(table=table).set(parts)
        self.ch_disk_free_bytes.set(stats.disk_free_bytes)
        self.ch_memory_peak_bytes.set(stats.peak_memory_bytes)

    def observe_lag(self, topic: str, partition: int, lag: int) -> None:
        self._check_labels({"topic": topic})
        self.kafka_partition_lag.labels(topic=topic, partition=str(partition)).set(lag)

    def snapshot(self) -> dict[str, float]:
        """Flatten the registry for assertions and evidence bundles.

        Tests need to prove an alert *would* fire - "any conflicting event id
        pages the owner" is a claim about a number, so the number has to be
        readable without scraping an HTTP endpoint.
        """
        out: dict[str, float] = {}
        for metric in self.registry.collect():
            for sample in metric.samples:
                key = sample.name
                if sample.labels:
                    label_text = ",".join(f"{k}={v}" for k, v in sorted(sample.labels.items()))
                    key = f"{key}{{{label_text}}}"
                out[key] = sample.value
        return out
