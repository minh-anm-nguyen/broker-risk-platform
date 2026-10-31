"""Shared test helpers and the *independent* oracle.

In its own module rather than in ``conftest.py`` so test files can import it
by name. ``conftest`` is loaded by pytest as a plugin, not as a normal
package member, so importing from it couples tests to pytest's import mode.

The oracle in :func:`expected_daily` is deliberately written from scratch
against :class:`DealSpec` using plain :mod:`decimal`. It imports nothing from
``analytics.build``, and it must not: section 9.1 calls a circular oracle a
release blocker, and the only way to keep that honest is for the expected
values to come from the fixture definition rather than from the code under
test. The duplicated day-bucketing and summation logic *is* the test.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from analytics.clock import ManualClock
from analytics.contracts.manifest import (
    CutScope,
    ReconciliationTrust,
    SourceCutManifest,
    build_source_manifest,
)
from analytics.fixtures.golden import (
    DEFAULT_TOPIC,
    DealSpec,
    deal_envelope,
    fixture,
    generate_deals,
    specs_for,
)
from analytics.main import Stack

READER_TOKEN = "lab-reader-token"
OPERATOR_TOKEN = "lab-operator-token"


# ---------------------------------------------------------------------------
# The independent oracle
# ---------------------------------------------------------------------------


def dedup_specs(specs: Iterable[DealSpec]) -> dict[tuple[str, str, str, str], DealSpec]:
    """One spec per Deal identity, raising on a genuine conflict.

    Mirrors section 6.6's reference implementation: a repeated identity with
    the same content is a harmless duplicate, and one with different content
    is an error the oracle refuses to average away.
    """
    seen: dict[tuple[str, str, str, str], DealSpec] = {}
    for spec in specs:
        key = spec.identity
        previous = seen.get(key)
        if previous is not None and previous.deal_payload_hash() != spec.deal_payload_hash():
            raise AssertionError(f"conflicting source spec for {key}")
        seen[key] = spec
    return seen


def expected_daily(specs: Iterable[DealSpec]) -> dict[tuple[date, str, str, str], dict[str, Any]]:
    """Expected daily aggregates, computed only from the source specs."""
    daily: dict[tuple[date, str, str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "deal_count": 0,
            "realized_pnl_gross": Decimal("0"),
            "commission": Decimal("0"),
            "swap": Decimal("0"),
            "volume_lots": Decimal("0"),
        }
    )
    for spec in dedup_specs(specs).values():
        # UTC day of execution, never of ingestion.
        key = (
            spec.executed_at.astimezone(UTC).date(),
            spec.account_id,
            spec.symbol,
            spec.money_currency,
        )
        bucket = daily[key]
        bucket["deal_count"] += 1
        bucket["realized_pnl_gross"] += Decimal(spec.realized_pnl_gross)
        bucket["commission"] += Decimal(spec.commission)
        bucket["swap"] += Decimal(spec.swap)
        bucket["volume_lots"] += Decimal(spec.volume_lots)
    return dict(sorted(daily.items()))


def expected_currency_totals(specs: Iterable[DealSpec]) -> dict[str, dict[str, Decimal]]:
    """Per-currency totals. No grand total is offered, by design."""
    totals: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "deal_count": Decimal("0"),
            "realized_pnl_gross": Decimal("0"),
            "commission": Decimal("0"),
            "swap": Decimal("0"),
            "volume_lots": Decimal("0"),
        }
    )
    for spec in dedup_specs(specs).values():
        bucket = totals[spec.money_currency]
        bucket["deal_count"] += 1
        bucket["realized_pnl_gross"] += Decimal(spec.realized_pnl_gross)
        bucket["commission"] += Decimal(spec.commission)
        bucket["swap"] += Decimal(spec.swap)
        bucket["volume_lots"] += Decimal(spec.volume_lots)
    return dict(totals)


def expected_identities(specs: Iterable[DealSpec]) -> set[tuple[str, str]]:
    return {(spec.deal_id, spec.deal_payload_hash()) for spec in dedup_specs(specs).values()}


# ---------------------------------------------------------------------------
# Source evidence stub (stands in for P03 and P04)
# ---------------------------------------------------------------------------


@dataclass
class StubSource:
    """A controllable :class:`SourceEvidenceProvider`.

    Every attribute is something a scenario needs to move: the Deal set, the
    completeness flag, the trust revision and whether it has been revoked.
    ``capture_calls`` and ``trust_calls`` are recorded so a test can assert
    the worker re-read the cut mid-build rather than trusting its first look.
    """

    specs: list[DealSpec]
    clock: ManualClock
    revision: str = "recon-verified-1"
    verified: bool = True
    revoked: bool = False
    findings: tuple[str, ...] = ()
    complete: bool = True
    incomplete_reason: str | None = None
    watermark: int | None = None
    retention_horizon: datetime | None = None
    kafka_cut: Mapping[str, int] | None = None
    capture_calls: int = 0
    trust_calls: int = 0
    #: Revoke the trust record after this many trust reads (0 = never).
    revoke_after_trust_reads: int = 0
    #: Replace the Deal set after this many capture calls (0 = never).
    mutate_after_captures: int = 0
    mutated_specs: list[DealSpec] | None = None

    def capture_cut(self, scope: CutScope) -> SourceCutManifest:
        self.capture_calls += 1
        specs = self.specs
        if (
            self.mutate_after_captures
            and self.capture_calls > self.mutate_after_captures
            and self.mutated_specs is not None
        ):
            specs = self.mutated_specs
        return build_source_manifest(
            scope,
            [(s.deal_id, s.deal_payload_hash(), s.source_seq) for s in specs],
            captured_at=self.clock.now(),
            deal_watermark=self.watermark,
            kafka_cut=self.kafka_cut,
            complete=self.complete,
            incomplete_reason=self.incomplete_reason,
            retention_horizon=self.retention_horizon,
        )

    def reconciliation_trust(self, scope: CutScope) -> ReconciliationTrust:
        self.trust_calls += 1
        revoked = self.revoked
        revision = self.revision
        if self.revoke_after_trust_reads and self.trust_calls > self.revoke_after_trust_reads:
            revoked = True
            revision = f"{self.revision}-revoked"
        return ReconciliationTrust(
            revision=revision,
            scope=scope,
            verified=self.verified,
            blocking_findings=self.findings,
            revoked=revoked,
            evaluated_at=self.clock.now(),
        )


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------


def produce(
    stack: Stack,
    envelopes: Sequence[Mapping[str, Any]],
    *,
    partition: int | None = 0,
    key: str | None = None,
) -> list[int]:
    """Publish envelopes to the lab broker, returning their offsets."""
    offsets = []
    for envelope in envelopes:
        body = json.dumps(envelope, sort_keys=True).encode("utf-8")
        offsets.append(stack.broker.produce(DEFAULT_TOPIC, body, key=key, partition=partition))
    return offsets


def produce_specs(
    stack: Stack, specs: Sequence[DealSpec], *, partition: int | None = 0, keyed: bool = False
) -> list[int]:
    offsets = []
    for spec in specs:
        body = json.dumps(deal_envelope(spec), sort_keys=True).encode("utf-8")
        offsets.append(
            stack.broker.produce(
                DEFAULT_TOPIC,
                body,
                key=spec.account_id if keyed else None,
                partition=None if keyed else partition,
            )
        )
    return offsets


def ingest_all(stack: Stack, *, max_cycles: int = 500) -> list:
    return stack.ingest_worker().run_until_idle(max_cycles=max_cycles)


def restart_consumer(stack: Stack):
    """Replace the stack's consumer, as a process restart would.

    A consumer's read position lives in the process. Reusing the same client
    object after a simulated crash would model a worker that kept its
    in-memory position across death, which is not what happens: the new
    process joins the group and seeks to the *committed* offset. Several
    kill-point assertions depend on that redelivery, so the restart has to be
    explicit rather than implied by constructing a new ``IngestWorker``.
    """
    from analytics.adapters.kafka_memory import InMemoryConsumer

    topic = stack.settings.kafka_topic_allowlist[0]
    stack.consumer = InMemoryConsumer(
        stack.broker,
        group_id=stack.settings.kafka_group_id,
        assignment=stack.broker.partitions_of(topic),
        faults=stack.faults,
    )
    return stack.ingest_worker()


def stub_source(stack: Stack, specs: Sequence[DealSpec], **kwargs) -> StubSource:
    return StubSource(list(specs), stack.clock, **kwargs)


def run_pipeline(
    stack: Stack,
    specs: Sequence[DealSpec],
    cut: CutScope,
    *,
    source: StubSource | None = None,
    publish: bool = True,
    keyed: bool = False,
):
    """Produce, ingest and build in one call. Returns ``(outcome, source)``."""
    produce_specs(stack, specs, keyed=keyed)
    ingest_all(stack)
    provider = source or stub_source(stack, specs)
    outcome = stack.build_worker(provider).run(cut, publish=publish)
    return outcome, provider


def rollup_map(stack: Stack, outcome, scope: CutScope):
    rows = stack.clickhouse.read_daily_rollups(
        build_id=outcome.build_id,
        tenant_id=scope.tenant_id,
        source_id=scope.source_id,
        epoch_id=scope.epoch_id,
    )
    return {(row.trade_day, row.account_id, row.symbol, row.money_currency): row for row in rows}


def assert_matches_oracle(stack: Stack, outcome, scope: CutScope, specs: Sequence[DealSpec]) -> None:
    """Compare the build's rollups against the independent oracle."""
    actual = rollup_map(stack, outcome, scope)
    want = expected_daily(specs)
    assert set(actual) == set(want), "rollup grouping keys differ from the oracle"
    for key, values in want.items():
        row = actual[key]
        assert row.deal_count == values["deal_count"], f"deal_count at {key}"
        assert row.realized_pnl_gross == values["realized_pnl_gross"], f"gross at {key}"
        assert row.commission == values["commission"], f"commission at {key}"
        assert row.swap == values["swap"], f"swap at {key}"


def published_pointer(stack: Stack, scope: CutScope):
    return stack.control.get_publication(
        tenant_id=scope.tenant_id,
        source_id=scope.source_id,
        epoch_id=scope.epoch_id,
        report_family=scope.report_family,
    )


def with_epoch(specs: Sequence[DealSpec], epoch_id: str) -> list[DealSpec]:
    return [replace(spec, epoch_id=epoch_id) for spec in specs]


def golden_envelopes(fixture_id: str):
    return fixture(fixture_id).envelopes


def many_specs(count: int, **kwargs) -> list[DealSpec]:
    return generate_deals(count, **kwargs)


def three_usd_specs() -> tuple[DealSpec, ...]:
    return specs_for("G08-01")
