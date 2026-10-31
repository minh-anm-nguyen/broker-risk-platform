"""Metrics and structured logging with enforced label discipline.

A metrics backend will happily accept ``account_id`` as a label and then fall
over three weeks later when the series count passes a million. Worse, the
account identifiers are now in a system with different access controls than the
database they came from. So this module refuses high-cardinality and sensitive
labels at registration time rather than hoping reviewers catch them
(WF5-200, WF5-279).

Durations come from a monotonic clock. A wall-clock delta can be negative after
an NTP step, and a negative duration in a histogram is a silent data error
(WF5-284).
"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Final, Mapping

from risk.errors import ContractError, ReasonCode

__all__ = [
    "MetricsRegistry",
    "FORBIDDEN_LABELS",
    "ALLOWED_LABELS",
    "redact",
    "log_fields",
    "Timer",
]

#: Labels that must never appear on a metric, whatever the intent.
FORBIDDEN_LABELS: Final[frozenset[str]] = frozenset(
    {
        "account_id",
        "account",
        "accounts",
        "deal_id",
        "position_id",
        "balance",
        "amount",
        "profit",
        "pnl",
        "tenant_id",
        "customer",
        "email",
        "dsn",
        "password",
        "secret",
        "token",
    }
)

#: The complete set of permitted label names. An allowlist rather than a
#: denylist, so a new sensitive field is rejected by default.
ALLOWED_LABELS: Final[frozenset[str]] = frozenset(
    {
        "component",
        "status",
        "reason_family",
        "entity_pair",
        "symbol_group",
        "field_class",
        "instance",
        "stage",
        "dialect",
        "outcome",
    }
)

_SECRET_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"(?i)(password|passwd|pwd)\s*=\s*[^\s;]+"),
    re.compile(r"(?i)(secret|token|api[_-]?key)\s*[=:]\s*[^\s;]+"),
    re.compile(r"(?i)postgres(?:ql)?://[^\s]+"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._\-]+"),
)


def redact(text: str) -> str:
    """Strip credential-shaped substrings from a message.

    Defence in depth, not a licence to log connection strings: the engine never
    puts a DSN in an error on purpose, and this catches the case where a driver
    embeds one in an exception message (WF5-201).
    """
    out = text
    for pattern in _SECRET_PATTERNS:
        out = pattern.sub("[REDACTED]", out)
    return out


def reason_family(reason_code: str) -> str:
    """Collapse a reason code into a bounded family for metric labels.

    There are a few dozen reason codes and the set grows; a label with one value
    per code is still bounded, but a family keeps dashboards readable and
    survives new codes without a dashboard edit.
    """
    code = str(reason_code)
    if code.startswith(("QUOTE_", "STALE_")):
        return "MARKET_DATA"
    if code.startswith("FX_"):
        return "FX"
    if code.startswith(("DECIMAL_", "INVALID_DECIMAL", "NONFINITE_", "MONEY_")):
        return "NUMERIC"
    if code.startswith(("RECON_",)):
        return "RECONCILIATION"
    if code.startswith(("EPOCH_", "SOURCE_", "P03_", "DEAL_WATERMARK_", "SCAN_")):
        return "UPSTREAM"
    if code.startswith(("UNAUTHORIZED",)):
        return "AUTHORIZATION"
    if code in ("OVERLOADED", "DEADLINE_EXCEEDED", "LIMIT_EXCEEDED", "INPUT_TOO_LARGE", "CANCELLED"):
        return "RESOURCE"
    if code.startswith(("UNSUPPORTED_", "SPEC_", "VOLUME_", "PRICE_")):
        return "UNSUPPORTED"
    if code in ("DB_UNAVAILABLE", "AUDIT_PERSISTENCE_FAILED"):
        return "INFRASTRUCTURE"
    if code in ("INCOMPATIBLE_CUT", "HISTORICAL_CUT_UNPROVEN", "SNAPSHOT_EXPIRED"):
        return "LINEAGE"
    return "OTHER"


@dataclass
class MetricsRegistry:
    """A tiny in-process metrics sink with label validation.

    Not a Prometheus client. It records the same series a Prometheus client
    would, so the label discipline and the counter semantics are testable
    without taking on an exporter dependency in the domain package.
    """

    counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    observations: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = field(default_factory=dict)

    @staticmethod
    def _validate(labels: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
        for key in labels:
            lowered = key.lower()
            if lowered in FORBIDDEN_LABELS:
                raise ContractError(
                    ReasonCode.UNAUTHORIZED_SCOPE,
                    "label is forbidden on metrics",
                    label=lowered,
                )
            if lowered not in ALLOWED_LABELS:
                raise ContractError(
                    ReasonCode.CONTRACT_MISSING_FIELD,
                    "label is not in the metrics allowlist",
                    label=lowered,
                )
        return tuple(sorted((k, str(v)) for k, v in labels.items()))

    def increment(self, name: str, *, amount: int = 1, **labels: Any) -> None:
        self.counters[(name, self._validate(labels))] += amount

    def observe(self, name: str, value: float, **labels: Any) -> None:
        if value < 0:
            # A negative duration means the clock moved, not that the work took
            # negative time. Recording it would corrupt every percentile.
            raise ContractError(
                ReasonCode.INVALID_TIMESTAMP, "refusing a negative observation", metric=name
            )
        self.observations[(name, self._validate(labels))].append(value)

    def set_gauge(self, name: str, value: float, **labels: Any) -> None:
        self.gauges[(name, self._validate(labels))] = value

    def counter_value(self, name: str, **labels: Any) -> int:
        return self.counters.get((name, self._validate(labels)), 0)


class Timer:
    """Monotonic stopwatch."""

    def __init__(self) -> None:
        self._start = time.monotonic()

    @property
    def elapsed_seconds(self) -> float:
        return max(0.0, time.monotonic() - self._start)

    @property
    def elapsed_ms(self) -> int:
        return int(self.elapsed_seconds * 1000)


def log_fields(
    *,
    calculation_id: str,
    scope_hash: str,
    source_id: str,
    epoch_id: str,
    status: str,
    reason_codes: tuple[str, ...],
    risk_policy_version: str,
    symbol_catalog_version: str,
    duration_ms: int,
) -> dict[str, object]:
    """Build the structured log record for one calculation.

    ``source_id`` is masked and the account list appears only as a hash. The
    record is designed to be greppable by an operator without granting them
    sight of which accounts were queried (section 8.3).
    """
    masked_source = source_id[:4] + "***" if len(source_id) > 4 else "***"
    return {
        "calculation_id": calculation_id,
        "scope_hash": scope_hash[:16],
        "source": masked_source,
        "epoch_id": epoch_id,
        "status": status,
        "reason_codes": list(reason_codes),
        "reason_families": sorted({reason_family(c) for c in reason_codes}),
        "risk_policy_version": risk_policy_version,
        "symbol_catalog_version": symbol_catalog_version,
        "duration_ms": duration_ms,
    }
