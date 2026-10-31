"""Operator and demo CLI.

The command that matters is ``demo``, which runs section 0.1's end-to-end
slice on the deterministic stack:

    P07 DealRecorded -> Kafka -> raw -> canonical build -> daily report
    -> VERIFIED publication -> read-only query

with a duplicate delivery injected at the ACK boundary. It prints the
independent oracle's totals next to the published ones, because a demo that
only shows "it worked" is exactly the sort of evidence the design spends its
length warning against.

The operator commands (``quarantine``, ``gc``) require an actor or a note.
That is not ceremony: resolving poison or collecting a build without a
recorded reason is how an incident becomes untraceable.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from decimal import Decimal
from typing import Sequence

from .clock import ManualClock
from .contracts.manifest import CutScope, ReconciliationTrust, build_source_manifest
from .errors import AnalyticsError, BuildState, ReportStatus
from .fixtures.golden import (
    DEFAULT_EPOCH,
    DEFAULT_POLICY_VERSION,
    DEFAULT_REPORT_FAMILY,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DEFAULT_TOPIC,
    deal_envelope,
    fixture,
    fixture_ids,
    generate_deals,
    specs_for,
)
from .main import build_lab_stack
from .money import format_money
from .settings import Settings


def _lab_scope(cut_id: str = "cut-1") -> CutScope:
    return CutScope(
        tenant_id=DEFAULT_TENANT,
        source_id=DEFAULT_SOURCE,
        epoch_id=DEFAULT_EPOCH,
        report_family=DEFAULT_REPORT_FAMILY,
        cut_id=cut_id,
        policy_version=DEFAULT_POLICY_VERSION,
    )


class _LabSource:
    """Stands in for P03's manifest and P04's verdict in the demo."""

    def __init__(self, specs, clock) -> None:
        self._specs = list(specs)
        self._clock = clock

    def capture_cut(self, scope: CutScope):
        return build_source_manifest(
            scope,
            [(s.deal_id, s.deal_payload_hash(), s.source_seq) for s in self._specs],
            captured_at=self._clock.now(),
        )

    def reconciliation_trust(self, scope: CutScope) -> ReconciliationTrust:
        return ReconciliationTrust(
            revision="recon-verified-demo",
            scope=scope,
            verified=True,
            evaluated_at=self._clock.now(),
        )


def _independent_totals(specs) -> dict[str, dict[str, str]]:
    """Per-currency totals from the specs alone, via plain Decimal.

    Re-derived here rather than read from the build, so the demo shows two
    independently computed numbers rather than one number twice (section 9.1).
    """
    seen = {spec.identity: spec for spec in specs}
    totals: dict[str, dict[str, Decimal]] = {}
    for spec in seen.values():
        bucket = totals.setdefault(
            spec.money_currency,
            {
                "deal_count": Decimal(0),
                "gross": Decimal(0),
                "commission": Decimal(0),
                "swap": Decimal(0),
            },
        )
        bucket["deal_count"] += 1
        bucket["gross"] += Decimal(spec.realized_pnl_gross)
        bucket["commission"] += Decimal(spec.commission)
        bucket["swap"] += Decimal(spec.swap)
    return {
        currency: {key: format_money(value) for key, value in measures.items()}
        for currency, measures in sorted(totals.items())
    }


def cmd_demo(args: argparse.Namespace) -> int:
    """Run the end-to-end vertical slice of section 0.1."""
    clock = ManualClock()
    settings = Settings(
        env="test",
        tenant_map=(f"{DEFAULT_TOPIC}:{DEFAULT_SOURCE}={DEFAULT_TENANT}",),
        cursor_signing_key="demo-cursor-signing-key-0123456789",
    )
    stack = build_lab_stack(settings=settings, clock=clock)
    try:
        specs = (
            list(generate_deals(args.deals, seed=args.seed))
            if args.deals
            else list(specs_for("G08-01"))
        )
        scope = _lab_scope()

        print(f"1. producing {len(specs)} DealRecorded.v1 events to {DEFAULT_TOPIC}")
        for spec in specs:
            stack.broker.produce(
                DEFAULT_TOPIC,
                json.dumps(deal_envelope(spec), sort_keys=True).encode("utf-8"),
                partition=0,
            )

        print("2. ingesting to raw (append-only observations)")
        stack.ingest_worker().run_until_idle(max_cycles=5_000)
        print(f"   raw observations: {stack.clickhouse.raw_row_count()}")

        if args.duplicate:
            print("3. injecting a duplicate delivery of every record (at-least-once)")
            for offset in range(len(specs)):
                stack.broker.redeliver((DEFAULT_TOPIC, 0), offset)
            stack.ingest_worker().run_until_idle(max_cycles=5_000)
            print(f"   raw observations after replay: {stack.clickhouse.raw_row_count()}")

        print("4. building and publishing a verified immutable build")
        outcome = stack.build_worker(_LabSource(specs, clock)).run(scope)
        if outcome.state is not BuildState.PUBLISHED:
            print(f"   BUILD NOT PUBLISHED: {outcome.state} / {outcome.reason}")
            print(f"   {outcome.message}")
            return 1
        print(f"   build_id={outcome.build_id} revision={outcome.published_revision}")
        print(f"   canonical digest={outcome.canonical_digest}")
        print(
            f"   canonical Deals={len(outcome.dedup.deals)} "
            f"from {outcome.dedup.physical_observations} observations"
        )

        print("5. independent Decimal oracle vs published totals")
        independent = _independent_totals(specs)
        published = {
            currency: {
                "deal_count": measures["deal_count"],
                "gross": measures["realized_pnl_gross"],
                "commission": measures["commission"],
                "swap": measures["swap"],
            }
            for currency, measures in outcome.oracle.currency_totals.items()
        }
        for currency in sorted(independent):
            left = independent[currency]
            right = published.get(currency, {})
            agree = all(left[key] == right.get(key) for key in left)
            print(f"   {currency}: oracle={left}")
            print(f"   {currency}: build ={right}  -> {'AGREE' if agree else 'DISAGREE'}")
            if not agree:
                return 1

        print("6. reading the published report through the read-only API")
        reader = stack.authenticator.authenticate("lab-reader-token")
        # The range is derived from the corpus and clamped to the configured
        # day bound, because an over-wide request is refused as
        # RANGE_TOO_LARGE - and a demo must not ask for something the service
        # is right to reject.
        executed_days = sorted(spec.executed_at.date() for spec in specs)
        from_day = executed_days[0]
        to_day = min(
            executed_days[-1],
            from_day.replace(year=from_day.year + 1)
            if settings.query_max_day_range >= 366
            else executed_days[-1],
        )
        report = stack.query.daily(
            reader,
            source_id=DEFAULT_SOURCE,
            epoch_id=DEFAULT_EPOCH,
            from_day=from_day,
            to_day=to_day,
        )
        print(f"   range={from_day} .. {to_day}")
        print(f"   status={report.envelope.status} build_id={report.envelope.build_id}")
        print(f"   fee_coverage={report.envelope.fee_coverage} rows={len(report.items)}")
        if report.envelope.status is not ReportStatus.READY:
            # A demo that prints OK while the read was refused is exactly the
            # false evidence this project exists to prevent.
            print(
                f"   READ NOT READY: {report.envelope.reason} - {report.envelope.message}",
                file=sys.stderr,
            )
            return 1
        if report.envelope.build_id != outcome.build_id:
            print("   READ SERVED A DIFFERENT BUILD THAN WAS PUBLISHED", file=sys.stderr)
            return 1
        if not report.items:
            print("   READ RETURNED NO ROWS FOR A PUBLISHED BUILD", file=sys.stderr)
            return 1
        for item in report.items[: args.show]:
            print(
                f"     {item['date']} {item['account_id']:<8} {item['symbol']:<7} "
                f"{item['currency']} count={item['deal_count']:<5} "
                f"gross={item['realized_pnl_gross']:>12} "
                f"net={item['net_realized']} ({item['net_label']})"
            )
        if len(report.items) > args.show:
            print(f"     ... {len(report.items) - args.show} more rows")

        print("\nDEMO OK - published totals agree with the independent oracle")
        return 0
    finally:
        stack.close()


def cmd_fixtures(args: argparse.Namespace) -> int:
    """List the golden fixtures and their expected outcomes."""
    for fixture_id in fixture_ids():
        item = fixture(fixture_id)
        reason = f" ({item.expected_reason})" if item.expected_reason else ""
        print(f"{fixture_id}  {item.expected_outcome:<12}{reason}")
        print(f"            {item.description}")
        if args.verbose and item.notes:
            print(f"            note: {item.notes}")
    return 0


def cmd_killpoints(_args: argparse.Namespace) -> int:
    """List the 28 deterministic injection sites."""
    from .faults.killpoints import kill_point_catalog

    for entry in kill_point_catalog():
        print(f"{entry['id']}  {entry['site']}")
    return 0


def cmd_invariants(args: argparse.Namespace) -> int:
    """Print the invariant register with the suite that proves each one."""
    from .invariants import INVARIANTS, unproven

    for invariant in INVARIANTS:
        marker = " " if invariant.proven else "!"
        print(f"{marker} {invariant.invariant_id}  {invariant.statement}")
        if args.verbose or not invariant.proven:
            print(f"              proof: {invariant.proof}")
    gaps = unproven()
    if gaps:
        print(f"\n{len(gaps)} invariant(s) are NOT PROVEN in this repository:")
        for invariant in gaps:
            print(f"  {invariant.invariant_id}")
    return 0


def cmd_quarantine(args: argparse.Namespace) -> int:
    """Inspect or resolve the poison ledger."""
    stack = build_lab_stack(control_path=args.control_db)
    try:
        if args.resolve:
            if not args.note:
                print("--note is required to resolve a receipt", file=sys.stderr)
                return 2
            record = stack.control.resolve_quarantine(receipt_id=args.resolve, note=args.note)
            print(f"resolved {record.receipt_id}: {record.resolution_note}")
            return 0
        records = stack.control.list_quarantine(unresolved_only=not args.all)
        if not records:
            print("no quarantine receipts")
            return 0
        for record in records:
            print(
                f"{record.receipt_id}  {record.topic}/{record.partition}@{record.offset}  "
                f"{record.reason}  resolved={record.resolved}"
            )
        return 0
    finally:
        stack.close()


def cmd_gc(args: argparse.Namespace) -> int:
    """Plan, or execute, garbage collection of abandoned builds."""
    stack = build_lab_stack(control_path=args.control_db)
    try:
        plan = stack.gc.plan(tenant_id=args.tenant)
        print(f"collectable: {list(plan.collectable)}")
        for build_id, reason in sorted(plan.protected.items()):
            print(f"protected  {build_id}  {reason}")
        if args.execute:
            if not args.actor:
                print("--actor is required to execute GC", file=sys.stderr)
                return 2
            deleted = stack.gc.collect(tenant_id=args.tenant, plan=plan, actor=args.actor)
            print(f"deleted rows per build: {deleted}")
        return 0
    finally:
        stack.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p08", description="Project 08 - ClickHouse Broker Analytics"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="run the end-to-end vertical slice")
    demo.add_argument("--deals", type=int, default=0, help="generate N deals instead of G08-01")
    demo.add_argument("--seed", type=int, default=20261030)
    demo.add_argument("--show", type=int, default=10, help="rows to print")
    demo.add_argument(
        "--no-duplicate",
        dest="duplicate",
        action="store_false",
        help="skip the duplicate-delivery injection",
    )
    demo.set_defaults(func=cmd_demo, duplicate=True)

    fixtures = sub.add_parser("fixtures", help="list the golden fixtures")
    fixtures.add_argument("-v", "--verbose", action="store_true")
    fixtures.set_defaults(func=cmd_fixtures)

    kill = sub.add_parser("killpoints", help="list the 28 injection sites")
    kill.set_defaults(func=cmd_killpoints)

    inv = sub.add_parser("invariants", help="list the invariant register")
    inv.add_argument("-v", "--verbose", action="store_true")
    inv.set_defaults(func=cmd_invariants)

    quarantine = sub.add_parser("quarantine", help="inspect or resolve poison receipts")
    quarantine.add_argument("--control-db", default="./data/p08_control.db")
    quarantine.add_argument("--all", action="store_true", help="include resolved receipts")
    quarantine.add_argument("--resolve", help="receipt id to resolve")
    quarantine.add_argument("--note", help="operator note, required to resolve")
    quarantine.set_defaults(func=cmd_quarantine)

    gc_cmd = sub.add_parser("gc", help="plan or run build garbage collection")
    gc_cmd.add_argument("--control-db", default="./data/p08_control.db")
    gc_cmd.add_argument("--tenant", default=DEFAULT_TENANT)
    gc_cmd.add_argument("--execute", action="store_true")
    gc_cmd.add_argument("--actor", help="operator identity, required to execute")
    gc_cmd.set_defaults(func=cmd_gc)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except AnalyticsError as exc:
        print(f"error [{exc.reason}]: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
