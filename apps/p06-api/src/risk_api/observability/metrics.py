"""Metrics with deliberately bounded label cardinality.

Design doc section 10.1. The label allowlist is enforced in code, not just
documented: ``_check_labels`` refuses an account id, principal id or epoch as
a label value. A metrics endpoint that leaks account numbers is a data leak
with a monitoring label on it.

``api_unauthorized_data_access_total`` is the critical signal. It must stay at
zero; a test asserts exactly that.
"""

from __future__ import annotations

import re
from typing import Final, Iterable

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

__all__ = ["Metrics", "LabelPolicyError"]

# Route templates, not concrete paths: /accounts/{account_id}/risk never
# becomes /accounts/00042/risk.
_ROUTE_TEMPLATE_RE: Final = re.compile(r"^[a-z0-9/{}_.-]{1,80}$")
_ACCOUNT_LIKE_RE: Final = re.compile(r"^[0-9]{3,}$")
_DIGEST_LIKE_RE: Final = re.compile(r"^(sha256:)?[0-9a-f]{16,}$")

_LATENCY_BUCKETS: Final = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class LabelPolicyError(ValueError):
    """A label value that would blow up cardinality or leak data."""


def _check_labels(values: Iterable[str]) -> None:
    for value in values:
        if not isinstance(value, str) or value == "":
            raise LabelPolicyError("label values must be non-empty strings")
        if len(value) > 80:
            raise LabelPolicyError("label value is too long to be low cardinality")
        if _ACCOUNT_LIKE_RE.fullmatch(value):
            raise LabelPolicyError("numeric identifiers are not permitted as label values")
        if _DIGEST_LIKE_RE.fullmatch(value):
            raise LabelPolicyError("digests are not permitted as label values")


class Metrics:
    """One registry per app instance, so tests never share counters."""

    def __init__(self, *, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()

        self.http_requests = Counter(
            "api_http_requests_total",
            "HTTP requests by route template, method and status class.",
            labelnames=("route", "method", "status_class"),
            registry=self.registry,
        )
        self.http_duration = Histogram(
            "api_http_duration_seconds",
            "HTTP request duration by route template and status class.",
            labelnames=("route", "status_class"),
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.scope_denials = Counter(
            "api_scope_denials_total",
            "Authorization denials by family and route template.",
            labelnames=("denial_family", "route"),
            registry=self.registry,
        )
        self.financial_status = Counter(
            "api_financial_status_total",
            "Financial result statuses by reason family.",
            labelnames=("status", "reason_family"),
            registry=self.registry,
        )
        self.cut_revalidation_failures = Counter(
            "api_cut_revalidation_failures_total",
            "Final validity checks that rejected a computed result.",
            labelnames=("source_kind", "failure_family"),
            registry=self.registry,
        )
        self.admission_rejections = Counter(
            "api_admission_rejections_total",
            "Requests refused by admission control.",
            labelnames=("scope", "reason"),
            registry=self.registry,
        )
        self.inflight_requests = Gauge(
            "api_inflight_requests",
            "Requests currently being served by this instance.",
            registry=self.registry,
        )
        self.heavy_queue_depth = Gauge(
            "api_heavy_queue_depth",
            "Heavy reports waiting for an execution slot.",
            registry=self.registry,
        )
        self.db_pool_wait = Histogram(
            "api_db_pool_wait_seconds",
            "Time spent waiting for a projection-store session.",
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.3, 1.0),
            registry=self.registry,
        )
        self.db_errors = Counter(
            "api_db_errors_total",
            "Projection-store read failures by type.",
            labelnames=("type",),
            registry=self.registry,
        )
        self.worker_cpu_seconds = Counter(
            "api_worker_cpu_seconds",
            "CPU seconds spent in bounded engine workers.",
            labelnames=("worker_kind",),
            registry=self.registry,
        )
        self.worker_heartbeat_lag = Gauge(
            "api_worker_heartbeat_lag",
            "Observed event-loop scheduling delay, p99 in seconds.",
            labelnames=("worker_kind",),
            registry=self.registry,
        )
        self.audit_receipts = Counter(
            "api_audit_receipts_total",
            "Durable audit receipts by result.",
            labelnames=("result",),
            registry=self.registry,
        )
        self.audit_failures = Counter(
            "api_audit_failures_total",
            "Audit write failures by result.",
            labelnames=("result",),
            registry=self.registry,
        )
        self.response_payload_bytes = Histogram(
            "api_response_payload_bytes",
            "Serialized response size by endpoint class.",
            labelnames=("endpoint_class",),
            buckets=(256, 1024, 4096, 16384, 65536, 262144, 1048576),
            registry=self.registry,
        )
        self.engine_invocations = Counter(
            "api_engine_invocations_total",
            "Risk engine invocations by outcome.",
            labelnames=("outcome",),
            registry=self.registry,
        )
        self.unauthorized_data_access = Counter(
            "api_unauthorized_data_access_total",
            "Critical test signal. Must remain zero.",
            labelnames=("surface",),
            registry=self.registry,
        )

    # ------------------------------------------------------------ recording

    def observe_request(
        self, *, route: str, method: str, status: int, duration_seconds: float
    ) -> None:
        if _ROUTE_TEMPLATE_RE.fullmatch(route) is None:
            raise LabelPolicyError(f"route label is not a template: {route!r}")
        for segment in route.split("/"):
            # A concrete id in a path segment is unbounded cardinality and, for
            # an account number, a leak. The template placeholder is required.
            if segment and _ACCOUNT_LIKE_RE.fullmatch(segment):
                raise LabelPolicyError(f"route label contains a concrete id: {route!r}")
        status_class = f"{status // 100}xx"
        _check_labels((route, method, status_class))
        self.http_requests.labels(route=route, method=method, status_class=status_class).inc()
        self.http_duration.labels(route=route, status_class=status_class).observe(duration_seconds)

    def observe_denial(self, *, denial_family: str, route: str) -> None:
        _check_labels((denial_family, route))
        self.scope_denials.labels(denial_family=denial_family, route=route).inc()

    def observe_financial_status(self, *, status: str, reason_family: str) -> None:
        _check_labels((status, reason_family))
        self.financial_status.labels(status=status, reason_family=reason_family).inc()

    def observe_cut_revalidation_failure(self, *, source_kind: str, failure_family: str) -> None:
        _check_labels((source_kind, failure_family))
        self.cut_revalidation_failures.labels(
            source_kind=source_kind, failure_family=failure_family
        ).inc()

    def observe_admission_rejection(self, *, scope: str, reason: str) -> None:
        _check_labels((scope, reason))
        self.admission_rejections.labels(scope=scope, reason=reason).inc()

    def observe_audit(self, *, result: str, failed: bool = False) -> None:
        _check_labels((result,))
        if failed:
            self.audit_failures.labels(result=result).inc()
        else:
            self.audit_receipts.labels(result=result).inc()

    def observe_payload(self, *, endpoint_class: str, size_bytes: int) -> None:
        _check_labels((endpoint_class,))
        self.response_payload_bytes.labels(endpoint_class=endpoint_class).observe(size_bytes)

    def observe_engine(self, *, outcome: str) -> None:
        _check_labels((outcome,))
        self.engine_invocations.labels(outcome=outcome).inc()

    def observe_db_error(self, *, kind: str) -> None:
        _check_labels((kind,))
        self.db_errors.labels(type=kind).inc()

    def flag_unauthorized_data_access(self, *, surface: str) -> None:
        """Only a bug or a test fixture should ever reach this."""
        _check_labels((surface,))
        self.unauthorized_data_access.labels(surface=surface).inc()

    def set_gauges(self, *, inflight: int, heavy_queue_depth: int, heartbeat_p99: float) -> None:
        self.inflight_requests.set(inflight)
        self.heavy_queue_depth.set(heavy_queue_depth)
        self.worker_heartbeat_lag.labels(worker_kind="engine").set(heartbeat_p99)

    def add_worker_cpu(self, seconds: float) -> None:
        self.worker_cpu_seconds.labels(worker_kind="engine").inc(max(0.0, seconds))

    # ---------------------------------------------------------------- export

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def counter_value(self, name: str, **labels: str) -> float:
        """Read one sample back. Used by tests and the readiness probe."""
        value = self.registry.get_sample_value(name, labels or None)
        return 0.0 if value is None else value
