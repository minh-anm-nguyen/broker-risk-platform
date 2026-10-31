"""Produce an evidence bundle in the layout of section 11.3.

Runs one scenario end to end and writes ``artifacts/p08/<run_id>/`` with the
files the design names. The bundle is the point: a claim that a build was
verified has to be re-checkable by somebody who was not there, which means
recording the *inputs* (which manifest, which trust revision, which write
marker) and not only the verdict.

``verdict.json`` can say ``NOT_RUN``. Section 11 is explicit that a scenario
whose fault never fired is not a pass, so if the plan scheduled a fault that
did not fire, this script says so rather than writing ``PASS``.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from analytics.clock import ManualClock  # noqa: E402
from analytics.contracts.manifest import (  # noqa: E402
    CutScope,
    ReconciliationTrust,
    build_source_manifest,
)
from analytics.errors import AmbiguousWriteError, BuildState  # noqa: E402
from analytics.faults.killpoints import FaultPlan, FaultSite, KillPoint  # noqa: E402
from analytics.fixtures.golden import (  # noqa: E402
    DEFAULT_EPOCH,
    DEFAULT_POLICY_VERSION,
    DEFAULT_REPORT_FAMILY,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DEFAULT_TOPIC,
    deal_envelope,
    generate_deals,
    specs_for,
)
from analytics.main import build_lab_stack  # noqa: E402
from analytics.money import format_money  # noqa: E402
from analytics.settings import Settings  # noqa: E402

#: Scenarios this script can produce evidence for.
SCENARIOS = {
    "happy": "G08-01 clean build and publication",
    "duplicate": "G08-02 duplicate delivery at the ACK boundary (KP08-05)",
    "conflict": "G08-03 one event_id with two payload hashes",
    "incomplete": "G08-09 source lists 100 Deals, 99 reach the sink",
}


class _Source:
    def __init__(self, specs, clock, revision="recon-verified-evidence"):
        self._specs = list(specs)
        self._clock = clock
        self.revision = revision

    def capture_cut(self, scope):
        return build_source_manifest(
            scope,
            [(s.deal_id, s.deal_payload_hash(), s.source_seq) for s in self._specs],
            captured_at=self._clock.now(),
        )

    def reconciliation_trust(self, scope):
        return ReconciliationTrust(
            revision=self.revision, scope=scope, verified=True, evaluated_at=self._clock.now()
        )


def _independent_totals(specs) -> dict[str, dict[str, str]]:
    """Expected totals from the specs alone. Never from P08's output."""
    seen = {spec.identity: spec for spec in specs}
    totals: dict[str, dict[str, Decimal]] = {}
    for spec in seen.values():
        bucket = totals.setdefault(
            spec.money_currency,
            {
                "deal_count": Decimal(0),
                "realized_pnl_gross": Decimal(0),
                "commission": Decimal(0),
                "swap": Decimal(0),
            },
        )
        bucket["deal_count"] += 1
        bucket["realized_pnl_gross"] += Decimal(spec.realized_pnl_gross)
        bucket["commission"] += Decimal(spec.commission)
        bucket["swap"] += Decimal(spec.swap)
    return {
        currency: {key: format_money(value) for key, value in measures.items()}
        for currency, measures in sorted(totals.items())
    }


def _write(directory: Path, name: str, payload) -> None:
    path = directory / name
    if name.endswith(".jsonl"):
        path.write_text(
            "\n".join(json.dumps(entry, sort_keys=True, default=str) for entry in payload),
            encoding="utf-8",
        )
    else:
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def collect(scenario: str, out_root: Path, deals: int) -> int:
    run_id = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{scenario}-{uuid.uuid4().hex[:8]}"
    directory = out_root / "p08" / run_id
    directory.mkdir(parents=True, exist_ok=True)

    clock = ManualClock()
    faults = FaultPlan()
    settings = Settings(
        env="test",
        tenant_map=(f"{DEFAULT_TOPIC}:{DEFAULT_SOURCE}={DEFAULT_TENANT}",),
        cursor_signing_key="evidence-cursor-key-0123456789",
    )

    expected_faults: list[str] = []
    if scenario == "duplicate":
        faults.raise_at(FaultSite.CH_INSERT_AMBIGUOUS, AmbiguousWriteError(), times=1)
        expected_faults.append(str(FaultSite.CH_INSERT_AMBIGUOUS))

    stack = build_lab_stack(settings=settings, clock=clock, faults=faults)
    started = time.perf_counter()
    try:
        scope = CutScope(
            tenant_id=DEFAULT_TENANT,
            source_id=DEFAULT_SOURCE,
            epoch_id=DEFAULT_EPOCH,
            report_family=DEFAULT_REPORT_FAMILY,
            cut_id="cut-evidence-1",
            policy_version=DEFAULT_POLICY_VERSION,
        )

        if scenario == "conflict":
            from analytics.fixtures.golden import fixture

            golden = fixture("G08-03")
            envelopes = list(golden.envelopes)
            source_specs = list(golden.source_deals)
        elif scenario == "incomplete":
            source_specs = list(generate_deals(deals or 100, seed=9009))
            envelopes = [deal_envelope(spec) for spec in source_specs[:-1]]
        else:
            source_specs = (
                list(generate_deals(deals, seed=20261030)) if deals else list(specs_for("G08-01"))
            )
            envelopes = [deal_envelope(spec) for spec in source_specs]

        for envelope in envelopes:
            stack.broker.produce(
                DEFAULT_TOPIC,
                json.dumps(envelope, sort_keys=True).encode("utf-8"),
                partition=0,
            )

        worker = stack.ingest_worker()
        outcomes = worker.run_until_idle(max_cycles=10_000)

        if scenario == "duplicate":
            for offset in range(len(envelopes)):
                stack.broker.redeliver((DEFAULT_TOPIC, 0), offset)
            outcomes += worker.run_until_idle(max_cycles=10_000)

        source = _Source(source_specs, clock)
        build = stack.build_worker(source).run(scope)
        elapsed = time.perf_counter() - started

        # -- the bundle ---------------------------------------------------
        _write(
            directory,
            "environment.json",
            {
                "python": sys.version,
                "platform": platform.platform(),
                "machine": platform.machine(),
                "cpu_count": __import__("os").cpu_count(),
                "clickhouse_version_pin": settings.ch_server_version_pin,
                "adapters": "in-process deterministic models (not a real cluster)",
                "measurement_scope": "P08 logic only; section 13.3 targets remain UNMEASURED",
                "collected_at": datetime.now(UTC).isoformat(),
            },
        )
        _write(
            directory,
            "scenario_manifest.json",
            {
                "run_id": run_id,
                "scenario": scenario,
                "description": SCENARIOS[scenario],
                "scope": scope.as_wire(),
                "expected_faults": expected_faults,
                "envelopes_produced": len(envelopes),
                "source_deals_claimed": len(source_specs),
            },
        )
        _write(directory, "fault_timeline.jsonl", faults.timeline())
        _write(
            directory,
            "kafka_offsets.json",
            {
                "committed": {
                    f"{topic}/{partition}": offset
                    for (topic, partition), offset in stack.consumer.committed(
                        sorted(stack.consumer.assignment())
                    ).items()
                },
                "watermarks": {
                    f"{topic}/{partition}": stack.broker.watermarks((topic, partition))
                    for (topic, partition) in sorted(stack.consumer.assignment())
                },
                "assignment_epoch": stack.consumer.assignment_epoch,
                "batches": [
                    {
                        "polled": o.polled,
                        "accepted": o.accepted,
                        "raw_rows_written": o.raw_rows_written,
                        "quarantined": o.quarantined,
                        "ambiguous": o.ambiguous,
                        "committed": {f"{t}/{p}": v for (t, p), v in o.committed.items()},
                    }
                    for o in outcomes
                ],
            },
        )
        manifest = source.capture_cut(scope)
        _write(
            directory,
            "p03_source_manifest.json",
            {
                "scope": scope.as_wire(),
                "deal_watermark": manifest.deal_watermark,
                "expected_unique_deals": manifest.expected_unique_deals,
                "identity_digest": manifest.identity_digest(),
                "manifest_sha256": manifest.manifest_sha256(),
                "complete": manifest.complete,
            },
        )
        trust = source.reconciliation_trust(scope)
        _write(
            directory,
            "p04_verified_cut.json",
            {
                "revision": trust.revision,
                "verified": trust.verified,
                "revoked": trust.revoked,
                "blocking_findings": list(trust.blocking_findings),
            },
        )
        _write(
            directory,
            "ch_raw_summary.json",
            {
                "physical_observations": stack.clickhouse.raw_row_count(),
                "unique_event_ids": len(
                    {row.event_id for row in stack.clickhouse.all_raw()}
                ),
                "event_id_conflicts": [
                    {"event_id": g.event_id, "distinct_hashes": g.distinct_hashes}
                    for g in stack.clickhouse.event_id_conflicts(
                        tenant_id=scope.tenant_id,
                        source_id=scope.source_id,
                        epoch_id=scope.epoch_id,
                        max_write_marker=10**9,
                    )
                    if g.distinct_hashes > 1
                ],
                "storage": asdict(stack.clickhouse.storage_stats()),
            },
        )
        _write(
            directory,
            "ch_build_summary.json",
            {
                "build_id": build.build_id,
                "state": str(build.state),
                "status": str(build.status),
                "reason": str(build.reason) if build.reason else None,
                "message": build.message,
                "attempt_no": build.attempt_no,
                "canonical_digest": build.canonical_digest,
                "evidence_digest": build.evidence_digest,
                "fact_rows_written": build.fact_rows_written,
                "rollup_rows_written": build.rollup_rows_written,
                "pinned_write_marker": build.pinned_write_marker,
                "details": dict(build.details),
            },
        )
        _write(
            directory,
            "pg_publication_history.json",
            [
                {
                    "action": entry.action,
                    "build_id": entry.build_id,
                    "previous_build_id": entry.previous_build_id,
                    "publication_revision": entry.publication_revision,
                    "actor": entry.actor,
                    "note": entry.note,
                    "recorded_at": entry.recorded_at.isoformat(),
                }
                for entry in stack.control.publication_history(
                    tenant_id=scope.tenant_id,
                    source_id=scope.source_id,
                    epoch_id=scope.epoch_id,
                    report_family=scope.report_family,
                )
            ],
        )
        expected = _independent_totals(source_specs)
        _write(
            directory,
            "decimal_oracle.json",
            {
                "independent_expected": expected,
                "build_measured": (
                    {k: dict(v) for k, v in build.oracle.currency_totals.items()}
                    if build.oracle
                    else None
                ),
                "oracle_detail": build.oracle.as_wire() if build.oracle else None,
            },
        )
        _write(
            directory,
            "performance.json",
            {
                "wall_seconds": round(elapsed, 4),
                "envelopes": len(envelopes),
                "raw_rows": stack.clickhouse.raw_row_count(),
                "note": "in-process adapters; not a cluster benchmark",
            },
        )
        _write(directory, "metrics.json", stack.metrics.snapshot())
        _write(
            directory,
            "quarantine.json",
            [
                {
                    "receipt_id": r.receipt_id,
                    "topic": r.topic,
                    "partition": r.partition,
                    "offset": r.offset,
                    "reason": r.reason,
                    "resolved": r.resolved,
                }
                for r in stack.control.list_quarantine(unresolved_only=False)
            ],
        )

        # -- the verdict --------------------------------------------------
        missing_faults = [
            site for site in expected_faults if faults.fired.get(site, 0) == 0
        ]
        if missing_faults:
            # Section 11: a fault that never fired is NOT_RUN, not PASS.
            verdict, reason = "NOT_RUN", f"scheduled faults never fired: {missing_faults}"
        elif scenario in ("happy", "duplicate"):
            ok = build.state is BuildState.PUBLISHED and build.oracle is not None
            agrees = ok and all(
                expected[currency][measure]
                == build.oracle.currency_totals.get(currency, {}).get(measure)
                for currency in expected
                for measure in expected[currency]
            )
            verdict = "PASS" if agrees else "FAIL"
            reason = (
                "published and agreeing with the independent oracle"
                if agrees
                else f"state={build.state} reason={build.reason}"
            )
        elif scenario == "conflict":
            blocked = build.state is BuildState.QUARANTINED
            verdict = "PASS" if blocked else "FAIL"
            reason = f"state={build.state} reason={build.reason}"
        else:  # incomplete
            incomplete = build.state is BuildState.FAILED and build.reason is not None
            verdict = "PASS" if incomplete else "FAIL"
            reason = f"state={build.state} reason={build.reason}"

        _write(
            directory,
            "verdict.json",
            {
                "run_id": run_id,
                "scenario": scenario,
                "verdict": verdict,
                "reason": reason,
                "faults_scheduled": expected_faults,
                "faults_fired": dict(faults.fired),
                "faults_pending": faults.pending(),
                "review": "machine-generated; requires human review before release",
            },
        )

        print(f"evidence written to {directory}")
        print(f"verdict: {verdict} - {reason}")
        return 0 if verdict == "PASS" else 1
    finally:
        stack.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect a P08 evidence bundle")
    parser.add_argument(
        "scenario", nargs="?", default="happy", choices=sorted(SCENARIOS), help="scenario to run"
    )
    parser.add_argument("--out", default=str(ROOT / "artifacts"), help="artifacts root")
    parser.add_argument("--deals", type=int, default=0, help="generate N deals")
    parser.add_argument("--all", action="store_true", help="collect every scenario")
    args = parser.parse_args(argv)

    if args.all:
        failures = 0
        for scenario in sorted(SCENARIOS):
            failures += collect(scenario, Path(args.out), args.deals)
        return 1 if failures else 0
    return collect(args.scenario, Path(args.out), args.deals)


if __name__ == "__main__":
    raise SystemExit(main())
