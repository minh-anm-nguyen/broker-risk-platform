"""Benchmark harness: measure, but assert correctness first.

The discipline of design section 15.4 is built into the shape of this tool. A
profile run does three things in order:

1. Computes the expected figures from the independent reference arithmetic.
2. Runs the workload and **fails the profile** if any figure disagrees.
3. Only then reports latency, throughput and resident memory.

A profile that produced a wrong number reports ``correctness: FAIL`` and its
timings are marked invalid, because a fast wrong answer is not a result
(ADR05-010). Cold and warm runs are reported separately rather than averaged,
and every run records the dataset seed, the policy version, the catalog version
and the host, so the numbers can be reproduced or disputed.

Usage::

    python -m risk_tools.benchmark --profile reference-5k --seed 4205
    python -m risk_tools.benchmark --profile offline-100k --seed 4205 --out run.json
    python -m risk_tools.benchmark --list
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from risk.adapters.p03_reader import P03Reader
from risk.adapters.quotes import FixtureQuoteProvider
from risk.adapters.sql import SqliteExecutor
from risk.audit import InMemoryAuditSink
from risk.contract import RiskRequest
from risk.domain.facts import AuthorizedScope
from risk.observability import MetricsRegistry
from risk.policies.loader import load_policy
from risk.service import RiskService, ServiceDependencies
from risk.status import Component, GlobalStatus
from risk_tools.fixtures import (
    ACCOUNT_A,
    SOURCE_A,
    T0,
    TENANT_A,
    QuoteSpec,
    build_harness_db,
    quote_snapshot_for,
    scenario,
    synthetic_deals,
    synthetic_positions,
)

__all__ = ["Profile", "PROFILES", "BenchmarkResult", "run_profile", "main"]

SYMBOLS = ("EURUSD", "GBPUSD", "AUDUSD")


@dataclass(frozen=True)
class Profile:
    """A named, reproducible workload."""

    name: str
    deals: int
    positions: int
    #: Separate budget for offline batch work: the 30-second on-demand
    #: deadline of section 8.1 is not the right bound for a 1M-deal job.
    deadline_seconds: int
    repetitions: int = 3
    description: str = ""


PROFILES: dict[str, Profile] = {
    "smoke": Profile("smoke", deals=200, positions=50, deadline_seconds=30, repetitions=3,
                     description="fast sanity profile for a pre-commit hook"),
    "reference-5k": Profile("reference-5k", deals=5_000, positions=1_000,
                            deadline_seconds=30, repetitions=3,
                            description="on-demand reference workload"),
    "reference-100k": Profile("reference-100k", deals=100_000, positions=10_000,
                              deadline_seconds=120, repetitions=3,
                              description="section 15.2 streaming workload"),
    "offline-1m": Profile("offline-1m", deals=1_000_000, positions=10_000,
                          deadline_seconds=600, repetitions=1,
                          description="offline batch; explicitly not a 30s budget"),
}


@dataclass
class BenchmarkResult:
    """One profile run, with its environment and its verdict."""

    profile: str
    seed: int
    deals: int
    positions: int
    correctness: str = "NOT_RUN"
    expected_gross: str | None = None
    observed_gross: str | None = None
    expected_status: str = "READY"
    observed_status: str | None = None
    output_digest: str | None = None
    cold_seconds: float | None = None
    warm_seconds: list[float] = field(default_factory=list)
    warm_p50: float | None = None
    warm_p95: float | None = None
    build_seconds: float | None = None
    rss_peak_mib: float | None = None
    read_batches: int | None = None
    read_rows: int | None = None
    queries: int | None = None
    risk_policy_version: str = ""
    symbol_catalog_version: str = ""
    read_batch_rows: int | None = None
    host: dict[str, Any] = field(default_factory=dict)
    started_at: str = ""
    notes: list[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        """Timings mean nothing unless the figures were right."""
        return self.correctness == "PASS"

    def as_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["valid"] = self.valid
        return payload


def _host_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown",
    }
    try:
        import os

        info["cpu_count"] = os.cpu_count()
    except Exception:  # pragma: no cover - defensive
        info["cpu_count"] = None
    try:
        import psutil

        info["total_ram_mib"] = round(psutil.virtual_memory().total / (1024 * 1024))
    except ImportError:
        info["total_ram_mib"] = None
    return info


def _rss_mib() -> float | None:
    try:
        import psutil
    except ImportError:
        return None
    return psutil.Process().memory_info().rss / (1024 * 1024)


def _reference_gross(
    deal_specs: Sequence[Any], *, period_start: Any, period_end: Any
) -> Decimal:
    """Expected realized gross, from a sum that does not touch the engine.

    Deliberately written out here rather than imported from the engine: this is
    the independent expectation the run is judged against.

    The window is applied here too, with the same half-open ``[start, end)``
    rule. An oracle that summed every generated row would disagree with a
    correct engine the moment the dataset outgrew the request -- and the
    disagreement would look like an arithmetic bug.
    """
    total = Decimal(0)
    for spec in deal_specs:
        if period_start is not None and spec.executed_at < period_start:
            continue
        if period_end is not None and spec.executed_at >= period_end:
            continue
        total += Decimal(spec.gross)
    return total


async def run_profile(profile: Profile, *, seed: int) -> BenchmarkResult:
    """Build the dataset, verify the figures, then measure."""
    result = BenchmarkResult(
        profile=profile.name,
        seed=seed,
        deals=profile.deals,
        positions=profile.positions,
        host=_host_info(),
        started_at=datetime.now(UTC).isoformat(),
        notes=[profile.description] if profile.description else [],
    )

    build_started = time.monotonic()
    spec = scenario("R05-aligned-ready")
    spec.seed = seed
    spec.deals = synthetic_deals(profile.deals, seed=seed, start_seq=1, base_time=T0)
    spec.positions = synthetic_positions(profile.positions, seed=seed, symbols=SYMBOLS)
    spec.quotes = tuple(
        QuoteSpec(symbol=symbol, bid="1.10200", ask="1.10220", quote_id=f"q-{symbol}-1")
        for symbol in SYMBOLS
    )
    spec.deal_watermark = profile.deals + 10_000

    connection = build_harness_db(spec)
    result.build_seconds = round(time.monotonic() - build_started, 3)

    base = load_policy()
    policy = replace(
        base,
        limits=replace(
            base.limits,
            max_positions_per_request=profile.positions + 10,
            max_deals_per_window=profile.deals + 10,
            wall_clock_deadline_seconds=Decimal(profile.deadline_seconds),
        ),
    )
    result.risk_policy_version = policy.policy_version
    result.read_batch_rows = policy.limits.read_batch_rows

    snapshot = quote_snapshot_for(spec)
    executor = SqliteExecutor(connection)
    deps = ServiceDependencies(
        reader=P03Reader(executor, policy),
        quote_provider=FixtureQuoteProvider(
            {snapshot.snapshot_id: snapshot}, default_snapshot_id=snapshot.snapshot_id
        ),
        audit_sink=InMemoryAuditSink(),
        metrics=MetricsRegistry(),
    )
    service = RiskService(deps, policy, clock=lambda: T0)
    scope = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))
    from datetime import timedelta

    request = RiskRequest(
        epoch_id=spec.epoch_id,
        components=frozenset(Component),
        period_start=T0 - timedelta(days=365),
        period_end=T0 + timedelta(days=1),
        valuation_as_of=T0,
        quote_snapshot_id=spec.quote_snapshot_id,
        strict_composite=True,
    )

    # --- step 1: the independent expectation, computed before the run --------
    expected_gross = _reference_gross(
        spec.deals, period_start=request.period_start, period_end=request.period_end
    )
    result.expected_gross = f"{expected_gross:f}"

    try:
        # --- step 2: cold run, checked for correctness -----------------------
        cold_started = time.monotonic()
        first = await service.compute(request, scope)
        result.cold_seconds = round(time.monotonic() - cold_started, 4)

        realized = first.component(Component.REALIZED).values
        result.observed_status = str(first.status)
        result.output_digest = first.output_digest
        result.symbol_catalog_version = first.symbol_catalog_version

        if realized is None:
            result.correctness = "FAIL"
            result.notes.append("realized component carried no values")
            return result

        result.observed_gross = str(realized["gross"])
        if Decimal(str(realized["gross"])) != expected_gross:
            result.correctness = "FAIL"
            result.notes.append("realized gross disagreed with the reference sum")
            return result
        if first.status is not GlobalStatus.READY:
            result.correctness = "FAIL"
            result.notes.append(f"status was {first.status}, expected READY")
            return result
        result.correctness = "PASS"

        # --- step 3: warm runs, measured and digest-checked -----------------
        for _ in range(max(0, profile.repetitions - 1)):
            warm_started = time.monotonic()
            warm = await service.compute(request, scope)
            result.warm_seconds.append(round(time.monotonic() - warm_started, 4))
            if warm.output_digest != first.output_digest:
                result.correctness = "FAIL"
                result.notes.append("a warm run produced a different digest")
                return result

        if result.warm_seconds:
            result.warm_p50 = round(statistics.median(result.warm_seconds), 4)
            ordered = sorted(result.warm_seconds)
            index = max(0, int(round(0.95 * (len(ordered) - 1))))
            result.warm_p95 = ordered[index]

        result.rss_peak_mib = None if _rss_mib() is None else round(_rss_mib() or 0, 1)
        result.read_batches = executor.stats.batches
        result.read_rows = executor.stats.rows
        result.queries = executor.stats.queries
    finally:
        connection.close()

    return result


def _format_report(results: list[BenchmarkResult]) -> str:
    lines = ["", "Benchmark report", "=" * 72]
    for item in results:
        verdict = "PASS" if item.valid else "FAIL / INVALID"
        lines.append(f"profile              {item.profile}   [{verdict}]")
        lines.append(f"  dataset            {item.deals} deals, {item.positions} positions, seed {item.seed}")
        lines.append(f"  policy / catalog   {item.risk_policy_version} / {item.symbol_catalog_version}")
        lines.append(f"  correctness        {item.correctness}")
        lines.append(f"    expected gross   {item.expected_gross}")
        lines.append(f"    observed gross   {item.observed_gross}")
        lines.append(f"    status           {item.observed_status}")
        if item.valid:
            lines.append(f"  cold run           {item.cold_seconds} s")
            lines.append(f"  warm p50 / p95     {item.warm_p50} s / {item.warm_p95} s")
            lines.append(f"  rss peak           {item.rss_peak_mib} MiB")
            lines.append(
                f"  read              {item.read_rows} rows in {item.read_batches} batches,"
                f" {item.queries} queries, batch size {item.read_batch_rows}"
            )
        else:
            lines.append("  timings            withheld: a wrong figure is not a result")
        for note in item.notes:
            lines.append(f"  note               {note}")
        lines.append("-" * 72)
    lines.append(
        "Cold and warm runs are reported separately. These are measurements on"
    )
    lines.append(
        "the host named above, not a claim about any other environment."
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m risk_tools.benchmark",
        description="Run a reproducible risk-engine benchmark profile.",
    )
    parser.add_argument("--profile", default="reference-5k", help="profile name")
    parser.add_argument("--seed", type=int, default=4205, help="dataset seed")
    parser.add_argument("--out", type=Path, help="write the raw JSON result here")
    parser.add_argument("--list", action="store_true", help="list the profiles and exit")
    args = parser.parse_args(argv)

    if args.list:
        for name, profile in PROFILES.items():
            print(
                f"{name:<16} {profile.deals:>9} deals  {profile.positions:>6} positions  "
                f"deadline {profile.deadline_seconds}s  -- {profile.description}"
            )
        return 0

    profile = PROFILES.get(args.profile)
    if profile is None:
        print(f"unknown profile {args.profile!r}; try --list", file=sys.stderr)
        return 2

    result = asyncio.run(run_profile(profile, seed=args.seed))
    print(_format_report([result]))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result.as_payload(), indent=2), encoding="utf-8")
        print(f"raw result written to {args.out}")

    # A failed correctness check is a failed benchmark, and the exit code says so.
    return 0 if result.valid else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
