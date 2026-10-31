"""Metrics, structured logs with redaction, and the event-loop monitor.

Three rules, each written against a specific way observability goes wrong:

* **Metric labels are bounded.** ``account_id``, ``deal_id``, ``request_id``
  and raw exception text are never labels. A Prometheus label with unbounded
  cardinality does not degrade gracefully - it takes the metrics backend down,
  and it does so during the incident the metrics were meant to explain
  (WF2-P-09, section 10.1).
* **Redaction is central and testable.** Secrets are registered once and a
  logging filter rewrites every record, so a token cannot leak through a
  stack trace, a repr, or an httpx exception that happens to include a URL.
  A test plants a canary and asserts it is absent from every artefact
  (WF2-B-06, WF2-N-07, INV-15).
* **Liveness measures the loop, not the source.** The heartbeat is an
  ``asyncio.sleep`` whose overshoot *is* the lag. If a blocking call were
  running on the loop, this is the number that would move - which is why the
  isolation claim is checked with it rather than argued (INV-01, WF2-F-01).

Log records are JSON and every value is scalar. A control character in an
inbound identifier would otherwise break the framing of the log stream, which
is a log-injection primitive, so identifiers are sanitised before they are
written (WF2-N-06, WF2-N-05).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Final, Iterable

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

__all__ = [
    "REDACTED",
    "SecretRegistry",
    "RedactingFilter",
    "JsonFormatter",
    "configure_logging",
    "Metrics",
    "EventLoopMonitor",
    "sanitize_token_text",
]

REDACTED: Final[str] = "[REDACTED]"

#: Control characters and anything that would break a single JSON log line.
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")

#: Patterns that look like a credential even when the exact value is unknown -
#: a rotated token still present in a long-lived buffer, for instance.
_CREDENTIAL_SHAPED: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"(?i)\b(authorization)\s*[:=]\s*\S+"),
    re.compile(r"(?i)\b(token|secret|password|passwd|api[_-]?key)\s*[:=]\s*\S+"),
    # A query string carrying a credential. The parameter name survives so the
    # log still says what was stripped.
    re.compile(r"(?i)([?&](?:token|access_token|key|secret)=)[^&\s]+"),
)


def sanitize_token_text(value: str) -> str:
    """Strip control characters and cap the length of free text.

    Applied to anything that came from outside - an identifier, a source
    code, a reason - before it reaches a log line or a metric label.
    """
    cleaned = _CONTROL.sub("", value)
    return cleaned[:256]


class SecretRegistry:
    """The set of literal secrets to scrub from output.

    Short values are not registered: a two-character "secret" would match
    everywhere and redact the logs into uselessness. The threshold is a
    deliberate trade - a credential short enough to fall below it is not
    protectable by string matching anyway.
    """

    _MIN_LENGTH: Final[int] = 8

    def __init__(self) -> None:
        self._secrets: set[str] = set()

    def add(self, *values: str | None) -> None:
        for value in values:
            if value and len(value) >= self._MIN_LENGTH:
                self._secrets.add(value)

    def clear(self) -> None:
        self._secrets.clear()

    @property
    def count(self) -> int:
        return len(self._secrets)

    def scrub(self, text: str) -> str:
        if not text:
            return text
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, REDACTED)
        for pattern in _CREDENTIAL_SHAPED:
            text = pattern.sub(lambda m: f"{m.group(1)} {REDACTED}", text)
        return text


#: Process-wide registry. A module-level singleton on purpose: redaction must
#: apply to every log record including those from dependencies, and a filter
#: cannot be threaded through third-party logging calls.
SECRETS = SecretRegistry()


class RedactingFilter(logging.Filter):
    """Scrubs every record's message, arguments and exception text."""

    def __init__(self, secrets: SecretRegistry | None = None) -> None:
        super().__init__()
        self._secrets = secrets or SECRETS

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self._secrets.scrub(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    key: self._scrub_value(value) for key, value in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(self._scrub_value(value) for value in record.args)
        if record.exc_info and record.exc_info[1] is not None:
            # The formatted traceback is built from this, so scrubbing the
            # exception's own arguments is what keeps a URL out of it.
            exc = record.exc_info[1]
            scrubbed = tuple(self._scrub_value(arg) for arg in exc.args)
            try:
                exc.args = scrubbed
            except (AttributeError, TypeError):  # pragma: no cover - exotic exceptions
                pass
        for key, value in list(record.__dict__.items()):
            if key.startswith("_") or key in _LOG_RESERVED:
                continue
            record.__dict__[key] = self._scrub_value(value)
        return True

    def _scrub_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._secrets.scrub(value)
        return value


_LOG_RESERVED: Final[frozenset[str]] = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with the design's field names."""

    def __init__(self, *, service: str = "mt5-connector") -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": self._service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _LOG_RESERVED or key.startswith("_"):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                payload[key] = value
            else:
                # Collapsed rather than dropped: a non-scalar extra is usually
                # a mistake, and seeing its type is how it gets fixed.
                payload[key] = f"<{type(value).__name__}>"
        if record.exc_info:
            payload["exception_type"] = (
                record.exc_info[0].__name__ if record.exc_info[0] else "unknown"
            )
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def configure_logging(
    *, level: int = logging.INFO, secrets: SecretRegistry | None = None
) -> logging.Logger:
    """Install the JSON formatter and the redacting filter on the root logger.

    The filter goes on the *handler* as well as the connector's own logger so
    that records from dependencies pass through it too.
    """
    redactor = RedactingFilter(secrets)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(redactor)

    root = logging.getLogger()
    root.setLevel(level)
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)

    logger = logging.getLogger("mt5_connector")
    logger.addFilter(redactor)
    return logger


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class Metrics:
    """Prometheus collectors with deliberately small label sets.

    Each instance owns its registry so tests are independent and a second
    connector in one process does not collide with the first.
    """

    def __init__(self, *, source_id: str, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self._source = sanitize_token_text(source_id)

        self.requests_total = Counter(
            "connector_requests_total",
            "Read requests by operation and outcome.",
            ["source_id", "operation", "outcome", "error_class"],
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "connector_request_duration_seconds",
            "End-to-end request duration measured from admission.",
            ["source_id", "operation", "outcome"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
            registry=self.registry,
        )
        self.queue_wait = Histogram(
            "connector_queue_wait_seconds",
            "Time between admission and dispatch.",
            ["source_id", "operation"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 3.0),
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "connector_queue_depth",
            "Requests waiting in the session mailbox.",
            ["source_id"],
            registry=self.registry,
        )
        self.sdk_call_duration = Histogram(
            "connector_sdk_call_duration_seconds",
            "How long a driver call actually occupied the session slot.",
            ["source_id", "operation"],
            buckets=(0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
            registry=self.registry,
        )
        self.sdk_active_calls = Gauge(
            "connector_sdk_active_calls",
            "Driver calls in flight for this session. Must never exceed 1.",
            ["source_id"],
            registry=self.registry,
        )
        self.worker_epoch = Gauge(
            "connector_worker_epoch",
            "Current session incarnation.",
            ["source_id"],
            registry=self.registry,
        )
        self.event_loop_lag = Histogram(
            "connector_event_loop_lag_seconds",
            "Overshoot of a fixed-interval heartbeat sleep.",
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0),
            registry=self.registry,
        )
        self.retry_total = Counter(
            "connector_retry_total",
            "Retries by error class and attempt bucket.",
            ["source_id", "error_class", "attempt"],
            registry=self.registry,
        )
        self.circuit_state = Gauge(
            "connector_circuit_state",
            "Breaker state: 0 closed, 1 half-open, 2 open.",
            ["source_id"],
            registry=self.registry,
        )
        self.reconnect_total = Counter(
            "connector_reconnect_total",
            "Reconnect attempts by reason.",
            ["source_id", "reason"],
            registry=self.registry,
        )
        self.snapshot_restart_total = Counter(
            "connector_snapshot_restart_total",
            "Scans abandoned and in need of a restart, by reason.",
            ["source_id", "reason"],
            registry=self.registry,
        )
        self.validation_error_total = Counter(
            "connector_validation_error_total",
            "Validation failures by field class. Never carries a field value.",
            ["source_id", "entity", "field"],
            registry=self.registry,
        )
        self.quarantine_total = Counter(
            "connector_quarantine_total",
            "Sessions quarantined, by reason.",
            ["source_id", "reason"],
            registry=self.registry,
        )
        self.response_bytes = Histogram(
            "connector_response_bytes",
            "Response size per page.",
            ["source_id", "operation"],
            buckets=(1024, 8192, 65536, 262144, 1048576, 4194304, 16777216),
            registry=self.registry,
        )
        self.readiness = Gauge(
            "connector_readiness",
            "1 when the connector will serve reads, 0 otherwise.",
            ["source_id"],
            registry=self.registry,
        )
        self.inflight_oldest_age = Gauge(
            "connector_inflight_oldest_age_seconds",
            "Age of the oldest in-flight driver call.",
            ["source_id"],
            registry=self.registry,
        )
        self.stale_completions_total = Counter(
            "connector_stale_completions_total",
            "Driver completions discarded by the fence, by reason.",
            ["source_id", "reason"],
            registry=self.registry,
        )
        self.abandoned_workers = Gauge(
            "connector_abandoned_workers",
            "Worker threads this process could not reclaim.",
            ["source_id"],
            registry=self.registry,
        )

    # -- helpers ------------------------------------------------------------

    def observe_request(
        self, *, operation: str, outcome: str, error_class: str, duration_s: float
    ) -> None:
        self.requests_total.labels(
            self._source, operation, outcome, error_class
        ).inc()
        self.request_duration.labels(self._source, operation, outcome).observe(duration_s)

    def observe_retry(self, *, error_class: str, attempt: int) -> None:
        # Bucketed rather than raw: attempt counts are bounded by policy, but
        # bucketing keeps the series stable if that policy changes.
        bucket = "1" if attempt <= 1 else "2" if attempt == 2 else "3+"
        self.retry_total.labels(self._source, error_class, bucket).inc()

    def set_gauge(self, gauge: Gauge, value: float) -> None:
        gauge.labels(self._source).set(value)

    def observe_validation_error(self, *, entity: str, field_name: str) -> None:
        self.validation_error_total.labels(
            self._source, sanitize_token_text(entity), sanitize_token_text(field_name)
        ).inc()

    def render(self) -> bytes:
        return generate_latest(self.registry)


# ---------------------------------------------------------------------------
# Event-loop monitor
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class LagSummary:
    samples: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass(slots=True)
class EventLoopMonitor:
    """Samples event-loop responsiveness with a fixed-interval sleep.

    The measurement is the *overshoot*: ask for 50 ms, record how much longer
    it took. If something blocking were running on the loop, the overshoot
    would be the length of that call. This is the instrument behind INV-01,
    and it is also why a noisy CI machine must report ``NOT_CONCLUSIVE``
    rather than a pass - a loaded host overshoots for reasons that have
    nothing to do with the connector (section 14.3).
    """

    interval_s: float = 0.05
    capacity: int = 4096
    metrics: Metrics | None = None
    _samples: list[float] = field(default_factory=list)
    _task: asyncio.Task[None] | None = None
    _running: bool = False

    @property
    def samples(self) -> tuple[float, ...]:
        return tuple(self._samples)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.get_running_loop().create_task(
            self._loop(), name="event-loop-heartbeat"
        )

    async def stop(self) -> None:
        self._running = False
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while self._running:
            started = time.perf_counter()
            await asyncio.sleep(self.interval_s)
            lag = max(0.0, (time.perf_counter() - started) - self.interval_s)
            self.record(lag)

    def record(self, lag_s: float) -> None:
        self._samples.append(lag_s)
        if len(self._samples) > self.capacity:
            # A ring rather than unbounded growth: a soak run would otherwise
            # accumulate a sample every 50 ms for eight hours.
            del self._samples[: len(self._samples) - self.capacity]
        if self.metrics is not None:
            self.metrics.event_loop_lag.observe(lag_s)

    def summary(self) -> LagSummary:
        if not self._samples:
            return LagSummary(0, 0.0, 0.0, 0.0, 0.0)
        ordered = sorted(self._samples)
        return LagSummary(
            samples=len(ordered),
            p50_ms=_quantile(ordered, 0.50) * 1000.0,
            p95_ms=_quantile(ordered, 0.95) * 1000.0,
            p99_ms=_quantile(ordered, 0.99) * 1000.0,
            max_ms=ordered[-1] * 1000.0,
        )

    def reset(self) -> None:
        self._samples.clear()


def _quantile(ordered: Iterable[float], q: float) -> float:
    values = list(ordered)
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, int(round(q * (len(values) - 1)))))
    return values[index]
