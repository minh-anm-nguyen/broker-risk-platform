"""Minimal in-process metrics (SYSTEM_DESIGN section 12.1).

Deliberately dependency-free: the service must be observable in a test run
without a metrics server, and the campaign runner needs to read the numbers
back as data to put them in an evidence artifact. ``render_prometheus``
produces a scrape-compatible text exposition for the deployed case.

The metric set is the "minimum metrics" list from section 12.1: run outcomes
by reason, capture failures, dispatch and verification outcomes, plus
durations and sizes. Every counter is labelled with the reason code where one
exists, because a count of failures without the reason cannot drive a runbook.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


def _label_key(labels: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


@dataclass
class Histogram:
    """Unbucketed summary: count, sum, min, max and the observed values.

    Values are retained so a benchmark can compute real percentiles rather
    than interpolating from buckets. Section 11.5 asks for p50/p95/p99 from
    measured data, and bucket interpolation is not measurement.
    """

    values: list[float] = field(default_factory=list)

    def observe(self, value: float) -> None:
        self.values.append(float(value))

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def total(self) -> float:
        return sum(self.values)

    def percentile(self, fraction: float) -> float | None:
        if not self.values:
            return None
        ordered = sorted(self.values)
        index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
        return ordered[index]

    def summary(self) -> dict[str, Any]:
        if not self.values:
            return {"count": 0}
        return {
            "count": self.count,
            "sum": self.total,
            "min": min(self.values),
            "max": max(self.values),
            "p50": self.percentile(0.50),
            "p95": self.percentile(0.95),
            "p99": self.percentile(0.99),
        }


class _Timer:
    def __init__(self, metrics: Metrics, name: str, labels: Mapping[str, Any]) -> None:
        self._metrics = metrics
        self._name = name
        self._labels = labels
        self._start = time.perf_counter()
        self._stopped = False

    def stop(self) -> float:
        if self._stopped:
            return 0.0
        self._stopped = True
        elapsed = time.perf_counter() - self._start
        self._metrics.observe(self._name, elapsed, **self._labels)
        return elapsed

    def __enter__(self) -> _Timer:
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()


class Metrics:
    """Thread-safe counters and histograms."""

    def __init__(self) -> None:
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[tuple[str, tuple[tuple[str, str], ...]], Histogram] = {}
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._lock = Lock()

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        key = (name, _label_key(labels))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def set_gauge(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            self._gauges[(name, _label_key(labels))] = float(value)

    def observe(self, name: str, value: float, **labels: Any) -> None:
        key = (name, _label_key(labels))
        with self._lock:
            self._histograms.setdefault(key, Histogram()).observe(value)

    def timer(self, name: str, **labels: Any) -> _Timer:
        return _Timer(self, name, labels)

    def counter_value(self, name: str, **labels: Any) -> float:
        return self._counters.get((name, _label_key(labels)), 0.0)

    def counter_total(self, name: str) -> float:
        return sum(value for (metric, _), value in self._counters.items() if metric == name)

    def histogram(self, name: str, **labels: Any) -> Histogram | None:
        return self._histograms.get((name, _label_key(labels)))

    def snapshot(self) -> dict[str, Any]:
        """Plain data, for an evidence artifact (section 10.7)."""
        with self._lock:
            return {
                "counters": [
                    {"name": name, "labels": dict(labels), "value": value}
                    for (name, labels), value in sorted(self._counters.items())
                ],
                "gauges": [
                    {"name": name, "labels": dict(labels), "value": value}
                    for (name, labels), value in sorted(self._gauges.items())
                ],
                "histograms": [
                    {"name": name, "labels": dict(labels), **histogram.summary()}
                    for (name, labels), histogram in sorted(self._histograms.items())
                ],
            }

    def render_prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            for (name, labels), value in sorted(self._counters.items()):
                lines.append(f"{name}{_render_labels(labels)} {value}")
            for (name, labels), value in sorted(self._gauges.items()):
                lines.append(f"{name}{_render_labels(labels)} {value}")
            for (name, labels), histogram in sorted(self._histograms.items()):
                summary = histogram.summary()
                lines.append(f"{name}_count{_render_labels(labels)} {summary['count']}")
                if summary["count"]:
                    lines.append(f"{name}_sum{_render_labels(labels)} {summary['sum']}")
        return "\n".join(lines) + "\n"


def _render_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    body = ",".join(f'{key}="{value}"' for key, value in labels)
    return "{" + body + "}"
