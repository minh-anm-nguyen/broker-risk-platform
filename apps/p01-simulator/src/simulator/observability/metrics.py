"""Prometheus metrics (section 11.1).

Cardinality discipline: ``route`` is always the *route template*
(``/v1/deals``), never the resolved URL, and no metric carries an account,
deal or request id. Those belong in structured logs. A metric labelled by
account id would produce one time series per account and take the metrics
backend down long before it helped anyone.

The registry is per-application instance rather than the library's global
default, so a test suite that builds fifty apps does not hit duplicate
registration errors.
"""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

#: Latency buckets chosen around the provisional SLO targets (p95 <= 300 ms,
#: p99 <= 1000 ms) so the histogram can actually answer whether they were met.
_HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0, 2.0, 5.0, 10.0)
_COMMIT_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0)


class Metrics:
    """Owns one registry and every collector the service publishes."""

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.registry = CollectorRegistry()
        reg = self.registry

        self.http_requests = Counter(
            "sim_http_requests_total",
            "HTTP requests by route template, status code and plane.",
            ["route", "status", "plane"],
            registry=reg,
        )
        self.http_duration = Histogram(
            "sim_http_duration_seconds",
            "Server-side request duration by route template and method.",
            ["route", "method"],
            buckets=_HTTP_BUCKETS,
            registry=reg,
        )
        self.requests_in_flight = Gauge(
            "sim_requests_in_flight",
            "Requests currently being served, by plane.",
            ["plane"],
            registry=reg,
        )
        self.requests_rejected = Counter(
            "sim_requests_rejected_total",
            "Requests rejected before handling, by reason (admission, rate limit, body size).",
            ["reason", "plane"],
            registry=reg,
        )

        self.writer_queue_depth = Gauge(
            "sim_writer_queue_depth",
            "Pending items in the single-writer queue.",
            registry=reg,
        )
        self.writer_queue_rejected = Counter(
            "sim_writer_queue_rejected_total",
            "Writes rejected because the bounded writer queue was full.",
            registry=reg,
        )
        self.writer_commits = Counter(
            "sim_writer_commit_total",
            "Committed domain events by event type.",
            ["event_type"],
            registry=reg,
        )
        self.writer_batches = Counter(
            "sim_writer_batch_total",
            "Writer transactions by outcome (committed, retried_individually, failed).",
            ["outcome"],
            registry=reg,
        )
        self.writer_commit_duration = Histogram(
            "sim_writer_commit_duration_seconds",
            "Duration of one writer transaction.",
            buckets=_COMMIT_BUCKETS,
            registry=reg,
        )
        self.last_committed_seq = Gauge(
            "sim_last_committed_seq",
            "Highest durably committed event sequence, by run.",
            ["run_id"],
            registry=reg,
        )
        self.domain_rejections = Counter(
            "sim_domain_rejections_total",
            "Domain operations rejected by the engine, by error code.",
            ["code"],
            registry=reg,
        )

        self.generation_jobs = Counter(
            "sim_generation_job_processed_total",
            "Generation jobs by terminal status.",
            ["status"],
            registry=reg,
        )
        self.generation_job_duration = Histogram(
            "sim_generation_job_duration_seconds",
            "Wall-clock duration of a generation job.",
            buckets=(0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600),
            registry=reg,
        )

        self.fault_activations = Counter(
            "sim_fault_activations_total",
            "Fault rules that fired, by rule and mode. Intentional, not an error rate.",
            ["rule_id", "mode"],
            registry=reg,
        )
        self.fault_matches = Counter(
            "sim_fault_matches_total",
            "Requests a fault rule matched, whether or not it activated.",
            ["rule_id"],
            registry=reg,
        )
        self.fault_errors = Counter(
            "sim_fault_errors_total",
            "Failures inside the fault engine itself (a bug, not an injected fault).",
            ["mode"],
            registry=reg,
        )

        self.snapshot_active = Gauge(
            "sim_snapshot_active_count",
            "Snapshot leases currently held for multi-page scans.",
            registry=reg,
        )
        self.snapshot_expired = Counter(
            "sim_snapshot_expired_total",
            "Snapshot leases that ended, by reason (ttl, run_replaced, evicted).",
            ["reason"],
            registry=reg,
        )
        self.snapshot_rejected = Counter(
            "sim_snapshot_rejected_total",
            "New snapshots refused because the active-snapshot limit was reached.",
            registry=reg,
        )

        self.sqlite_busy = Counter(
            "sim_sqlite_busy_total",
            "SQLITE_BUSY / lock timeout occurrences.",
            registry=reg,
        )
        self.storage_errors = Counter(
            "sim_storage_errors_total",
            "Storage failures by class (disk_full, readonly, other).",
            ["kind"],
            registry=reg,
        )
        self.sqlite_wal_bytes = Gauge(
            "sim_sqlite_wal_bytes", "Size of the WAL file in bytes.", registry=reg
        )
        self.db_file_bytes = Gauge(
            "sim_db_file_bytes", "Size of the main database file in bytes.", registry=reg
        )

        self.invariant_failures = Counter(
            "sim_invariant_failures_total",
            "Invariant checks that failed, by invariant id.",
            ["invariant_id"],
            registry=reg,
        )
        self.recovery_required = Gauge(
            "sim_recovery_required",
            "1 when projections disagree with the event log and operator repair is needed.",
            registry=reg,
        )
        self.resident_memory_bytes = Gauge(
            "sim_resident_memory_bytes",
            "Resident set size of this process in bytes.",
            registry=reg,
        )

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST


class NullMetrics:
    """No-op stand-in used by pure unit tests that do not assert on metrics."""

    enabled = False

    def __getattr__(self, name: str) -> "NullMetrics":
        return self

    def __call__(self, *args: object, **kwargs: object) -> "NullMetrics":
        return self

    def labels(self, *args: object, **kwargs: object) -> "NullMetrics":
        return self

    def inc(self, *args: object, **kwargs: object) -> None:
        return None

    def dec(self, *args: object, **kwargs: object) -> None:
        return None

    def set(self, *args: object, **kwargs: object) -> None:
        return None

    def observe(self, *args: object, **kwargs: object) -> None:
        return None

    def render(self) -> tuple[bytes, str]:
        return b"", CONTENT_TYPE_LATEST
