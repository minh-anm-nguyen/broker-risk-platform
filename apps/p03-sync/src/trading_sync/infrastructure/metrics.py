"""Prometheus metrics.

Label choice is a correctness concern here, not a style one. Two rules hold:

**No unbounded label values.** ``deal_id``, ``account_id``, ``position_id``,
``scan_id`` and SQL text are never labels. Each would create one time series
per record, and account ids in particular would publish customer identifiers
into a metrics store that has none of the access controls the database has
(design 12.1, WF3-198, WF3-204). ``tests/unit/test_metrics.py`` asserts this
rather than trusting the convention.

**Names say what they measure.** ``sync_checkpoint_source_seq`` is an *event*
sequence, and the distance between it and the source's committed sequence
counts non-deal events too. It is not a count of missing deals, and presenting
it as one would turn a healthy system into a false alarm (design 12.1,
WF3-195).
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

__all__ = ["FORBIDDEN_LABELS", "SyncMetrics"]

#: Asserted against every registered metric by the unit suite.
FORBIDDEN_LABELS = frozenset(
    {
        "deal_id",
        "account_id",
        "position_id",
        "scan_id",
        "cursor",
        "query",
        "sql",
        "token",
        "lease_token",
        "payload",
        "run_id",
    }
)


class SyncMetrics:
    """Metric handles bound to one registry, so tests stay isolated."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        reg = self.registry

        self.scan_total = Counter(
            "sync_scan_total", "Scans by entity and outcome", ["entity", "outcome"], registry=reg
        )
        self.pages_committed_total = Counter(
            "sync_pages_committed_total", "Durable page transactions", ["entity"], registry=reg
        )
        self.deals_inserted_total = Counter(
            "sync_deals_inserted_total", "Newly inserted distinct deals", ["source_group"], registry=reg
        )
        self.deals_duplicate_total = Counter(
            "sync_deals_duplicate_total", "Identical replays absorbed", ["entity"], registry=reg
        )
        self.data_integrity_conflicts_total = Counter(
            "sync_data_integrity_conflicts_total",
            "Immutable content conflicts",
            ["entity", "reason_code"],
            registry=reg,
        )
        self.boundary_anomalies_total = Counter(
            "sync_source_boundary_anomalies_total",
            "Records at or below the start watermark, i.e. an inclusive after_seq",
            ["entity"],
            registry=reg,
        )
        self.checkpoint_source_seq = Gauge(
            "sync_checkpoint_source_seq",
            "Durable deal watermark as a source event sequence, not a deal count",
            ["source_id"],
            registry=reg,
        )
        self.last_success_age_seconds = Gauge(
            "sync_last_success_age_seconds",
            "Seconds since the last successful publication",
            ["source_id", "entity"],
            registry=reg,
        )
        self.snapshot_seq = Gauge(
            "sync_snapshot_seq",
            "Last published source snapshot sequence",
            ["source_id", "entity"],
            registry=reg,
        )
        self.scan_duration_seconds = Histogram(
            "sync_scan_duration_seconds", "Wall time per scan", ["entity", "outcome"], registry=reg
        )
        self.db_tx_duration_seconds = Histogram(
            "sync_db_tx_duration_seconds", "Database transaction latency", ["operation"], registry=reg
        )
        self.db_pool_in_use = Gauge(
            "sync_db_pool_in_use", "Connections checked out", ["service"], registry=reg
        )
        self.lease_takeovers_total = Counter(
            "sync_lease_takeovers_total", "Leases taken after expiry", ["entity"], registry=reg
        )
        self.staging_rows = Gauge(
            "sync_staging_rows", "Rows held in staging", ["entity", "state"], registry=reg
        )
        self.scan_inflight = Gauge(
            "sync_scan_inflight", "Scans currently running", ["entity"], registry=reg
        )
        self.upstream_errors_total = Counter(
            "sync_upstream_errors_total", "Connector faults by code", ["error_code"], registry=reg
        )
        self.queue_wait_seconds = Histogram(
            "sync_queue_wait_seconds", "Admission queue wait", ["operation"], registry=reg
        )
        self.quarantine_items_total = Counter(
            "sync_quarantine_items_total", "Quarantined items", ["entity", "reason_code"], registry=reg
        )
