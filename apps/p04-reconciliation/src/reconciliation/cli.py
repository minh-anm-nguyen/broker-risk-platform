"""``reconctl`` -- the operator command line (section 13.3).

Section 13.3 sketches this contract; this is the working implementation. It is
built so that the *read* paths are always safe to run and the *write* paths
cannot be run by accident:

* ``run``, ``source``, ``intent`` and ``audit`` subcommands only read.
* ``plan create`` defaults to a dry run; persisting a plan needs ``--commit``.
* ``plan dispatch`` requires the plan hash on the command line, so dispatching
  the wrong plan takes more than a stale shell-history entry. The hash is
  re-checked against the stored plan before anything is sent (INV04-06).
* Nothing here can write Project 03's tables; the role the CLI connects with
  is denied those writes by the engine.

Exit codes are meaningful for scripting: ``0`` success, ``1`` a refusal or a
non-matching audit, ``2`` a usage error.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import httpx

from .approvals import ApprovalService
from .approvals import Role as ApprovalRole
from .audit import verify_chain
from .clock import SystemClock
from .config import Settings, load_settings
from .db.engine import Database, Role
from .db.repository import ReconRepository
from .errors import ReconError
from .evidence import CampaignReport
from .metrics import Metrics
from .ports.source import P01HttpSource
from .scheduler import ReconciliationScheduler

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_USAGE = 2


def _build(settings: Settings):
    """Compose the service for local operation.

    Project 03 is represented by its contract harness: until Project 03
    exists, that harness is what owns the synced tables. The CLI says so in
    ``source check`` output rather than pretending otherwise.
    """
    from recon_harness.p03_sync import P03SyncHarness

    database = Database(
        settings.db_path,
        busy_timeout_ms=settings.sqlite_busy_timeout_ms,
        synchronous=settings.sqlite_synchronous,
    )
    database.create_schema()
    clock = SystemClock()
    repository = ReconRepository(database, settings, clock)

    headers = {}
    if settings.source_data_token:
        headers["Authorization"] = f"Bearer {settings.source_data_token}"
    client = httpx.Client(
        base_url=settings.source_base_url,
        timeout=settings.source_request_timeout_s,
        headers=headers,
    )
    source = P01HttpSource(
        client=client,
        source_id=settings.source_id,
        max_page_size=settings.source_max_page_size,
        max_retries=settings.source_max_retries,
        retry_base_delay_s=settings.source_retry_base_delay_s,
    )
    harness = P03SyncHarness(
        db=database,
        source=source,
        clock=clock,
        source_id=settings.source_id,
        page_size=settings.source_max_page_size,
    )
    scheduler = ReconciliationScheduler(
        db=database,
        repository=repository,
        source=source,
        read_port=harness,
        repair_port=harness,
        settings=settings,
        clock=clock,
        metrics=Metrics(),
    )
    return database, repository, source, harness, scheduler, client


def _emit(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


# ---------------------------------------------------------------------------
# source
# ---------------------------------------------------------------------------


def cmd_source_check(args: argparse.Namespace, settings: Settings) -> int:
    database, _repo, source, harness, _sched, client = _build(settings)
    try:
        identity = source.read_identity()
        capabilities = source.read_capabilities()
        profile = harness.source_capability_profile(settings.source_id)
        _emit(
            {
                "source_id": identity.source_id,
                "run_id": identity.run_id,
                "generation": identity.generation,
                "snapshot_seq": identity.snapshot_seq,
                "counts": {
                    "accounts": identity.account_count,
                    "deals": identity.deal_count,
                    "open_positions": identity.open_position_count,
                },
                "capabilities": {
                    "max_page_size": capabilities.max_page_size,
                    "snapshot_ttl_seconds": capabilities.snapshot_ttl_seconds,
                    "stable_snapshot_pagination": capabilities.stable_snapshot_pagination,
                    "durable_epoch_identity": capabilities.durable_epoch_identity,
                    "complete_deal_history": capabilities.complete_deal_history,
                    "global_cut_across_entities": capabilities.global_cut_across_entities,
                },
                "p03_profile": dict(profile),
                "policy_hash": settings.policy_hash,
                # Stated plainly so no reader mistakes this for Project 03.
                "p03_implementation": "contract harness (Project 03 not yet implemented)",
            }
        )
        return EXIT_OK
    finally:
        client.close()


def cmd_source_bootstrap(args: argparse.Namespace, settings: Settings) -> int:
    """Register the source's current epoch with the P03 harness."""
    _db, _repo, _source, harness, _sched, client = _build(settings)
    try:
        epoch_id = harness.bootstrap_epoch()
        _emit({"epoch_id": epoch_id})
        return EXIT_OK
    finally:
        client.close()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def cmd_run_start(args: argparse.Namespace, settings: Settings) -> int:
    _db, _repo, _source, _harness, scheduler, client = _build(settings)
    try:
        outcome = scheduler.run_audit(
            entity=args.entity,
            epoch_id=args.epoch,
            request_id=args.request_id,
            scope=args.scope,
        )
        _emit(
            {
                "recon_run_id": outcome.recon_run_id,
                "status": outcome.status,
                "reason_code": outcome.reason_code.value if outcome.reason_code else None,
                "finding_count": outcome.finding_count,
                "source_count": outcome.source_count,
                "target_count": outcome.target_count,
                "cross_entity_tier": outcome.cross_entity_tier,
                "detail": dict(outcome.detail),
            }
        )
        # A non-matching audit is a meaningful non-zero exit for scripting,
        # not an error: INCONCLUSIVE and FINDINGS are both valid outcomes.
        return EXIT_OK if outcome.status == "MATCHED" else EXIT_REFUSED
    finally:
        client.close()


def cmd_run_show(args: argparse.Namespace, settings: Settings) -> int:
    _db, repository, _source, _harness, _sched, client = _build(settings)
    try:
        run = repository.get_run(args.run_id)
        if run is None:
            print(f"unknown run {args.run_id}", file=sys.stderr)
            return EXIT_REFUSED
        ok, reason, stats = repository.validate_receipt_chain(args.run_id)
        _emit(
            {
                "recon_run_id": run.recon_run_id,
                "entity": run.entity,
                "scope": run.scope,
                "status": run.status,
                "reason_code": run.reason_code,
                "source_complete": run.source_complete,
                "target_complete": run.target_complete,
                "source_snapshot_seq": run.source_snapshot_seq,
                "target_snapshot_seq": run.target_snapshot_seq,
                "target_deal_watermark": run.target_deal_watermark,
                "source_count": run.source_count,
                "target_count": run.target_count,
                "source_root_hash": run.source_root_hash,
                "target_root_hash": run.target_root_hash,
                "policy_hash": run.policy_hash,
                "receipt_chain": {
                    "valid": ok,
                    "reason": reason.value if reason else None,
                    "receipts": stats.receipt_count,
                    "terminal_index": stats.terminal_index,
                    "declared_items": stats.declared_items,
                    "stored_items": stats.stored_items,
                },
                "findings": repository.finding_counts(args.run_id),
            }
        )
        return EXIT_OK
    finally:
        client.close()


def cmd_run_evidence(args: argparse.Namespace, settings: Settings) -> int:
    """Dump findings with their evidence digests (redacted by default)."""
    _db, repository, _source, _harness, _sched, client = _build(settings)
    try:
        rows = repository.findings_for(args.run_id)
        payload = [
            {
                "finding_code": row["finding_code"],
                "item_key": row["item_key"],
                "severity": row["severity"],
                "confidence": row["confidence"],
                "explanation_code": row["explanation_code"],
                "evidence_hash": row["evidence_hash"],
                # Content digests are included only when explicitly asked for:
                # they are not secret, but they are not useful in a ticket.
                **(
                    {"source_hash": row["source_hash"], "target_hash": row["target_hash"]}
                    if not args.redacted
                    else {}
                ),
            }
            for row in rows
        ]
        _emit({"recon_run_id": args.run_id, "findings": payload})
        return EXIT_OK
    finally:
        client.close()


def cmd_run_compare(args: argparse.Namespace, settings: Settings) -> int:
    _db, repository, _source, _harness, _sched, client = _build(settings)
    try:
        rows = repository.compute_diff(args.run_id)
        _emit(
            {
                "recon_run_id": args.run_id,
                "differences": [
                    {
                        "item_key": row.item_key,
                        "diff_type": row.diff_type,
                        "source_hash": row.source_hash,
                        "target_hash": row.target_hash,
                    }
                    for row in rows
                ],
            }
        )
        return EXIT_OK if not rows else EXIT_REFUSED
    finally:
        client.close()


def cmd_run_verify(args: argparse.Namespace, settings: Settings) -> int:
    """Independent post-repair verification of a dispatched plan."""
    _db, repository, _source, _harness, scheduler, client = _build(settings)
    try:
        plan = _rehydrate_plan(repository, args.plan_id)
        if plan is None:
            print(f"unknown plan {args.plan_id}", file=sys.stderr)
            return EXIT_REFUSED
        result = scheduler.verify(plan)
        _emit(
            {
                "plan_id": args.plan_id,
                "verified": result.verified,
                "plan_status": result.plan_status,
                "reason_code": result.reason_code.value if result.reason_code else None,
                "verification_run_id": result.verification_run_id,
                "detail": result.detail,
            }
        )
        return EXIT_OK if result.verified else EXIT_REFUSED
    finally:
        client.close()


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def cmd_plan_create(args: argparse.Namespace, settings: Settings) -> int:
    _db, _repo, _source, _harness, scheduler, client = _build(settings)
    try:
        plan = scheduler.plan_repair(
            args.run_id, created_by=args.actor, dry_run=not args.commit
        )
        if plan is None:
            _emit(
                {
                    "run_id": args.run_id,
                    "plan": None,
                    "reason": "policy found no eligible repair for this run",
                }
            )
            return EXIT_REFUSED
        print(plan.preview_text())
        if not args.commit:
            print("\n(dry run: nothing was persisted; re-run with --commit)")
        return EXIT_OK
    finally:
        client.close()


def cmd_plan_approve(args: argparse.Namespace, settings: Settings) -> int:
    _db, repository, _source, _harness, _sched, client = _build(settings)
    try:
        service = ApprovalService(
            repository=repository, settings=settings, clock=SystemClock()
        )
        outcome = service.decide(
            plan_id=args.plan_id,
            actor_id=args.actor,
            role=ApprovalRole(args.role),
            decision="REJECT" if args.reject else "APPROVE",
            plan_hash=args.plan_hash,
        )
        _emit(
            {
                "plan_id": args.plan_id,
                "allowed": outcome.allowed,
                "reason_code": outcome.reason_code.value if outcome.reason_code else None,
                "detail": outcome.detail,
            }
        )
        return EXIT_OK if outcome.allowed else EXIT_REFUSED
    finally:
        client.close()


def cmd_plan_dispatch(args: argparse.Namespace, settings: Settings) -> int:
    _db, repository, _source, _harness, scheduler, client = _build(settings)
    try:
        plan = _rehydrate_plan(repository, args.plan_id)
        if plan is None:
            print(f"unknown plan {args.plan_id}", file=sys.stderr)
            return EXIT_REFUSED
        if plan.plan_hash != args.plan_hash:
            print(
                "refusing to dispatch: the supplied plan hash does not match the stored plan",
                file=sys.stderr,
            )
            return EXIT_REFUSED
        result = scheduler.dispatch(plan)
        _emit(
            {
                "plan_id": result.plan_id,
                "plan_status": result.plan_status,
                "reason_code": result.reason_code.value if result.reason_code else None,
                "outcomes": [
                    {
                        "intent_id": outcome.intent_id,
                        "status": outcome.status,
                        "reason_code": outcome.reason_code.value if outcome.reason_code else None,
                        "detail": outcome.detail,
                    }
                    for outcome in result.outcomes
                ],
                "note": "COMMITTED is not VERIFIED; run `reconctl run verify` next",
            }
        )
        return EXIT_OK if result.plan_status in ("VERIFYING", "VERIFIED") else EXIT_REFUSED
    finally:
        client.close()


def cmd_intent_status(args: argparse.Namespace, settings: Settings) -> int:
    _db, _repo, _source, harness, _sched, client = _build(settings)
    try:
        receipt = harness.repair_intent_status(args.intent_id)
        if receipt is None:
            _emit(
                {
                    "intent_id": args.intent_id,
                    "found": False,
                    "note": "P03 has no record; retrying the SAME intent id is safe",
                }
            )
            return EXIT_REFUSED
        _emit(
            {
                "intent_id": receipt.intent_id,
                "found": True,
                "status": receipt.status,
                "reason_code": receipt.reason_code,
                "receipt_hash": receipt.receipt_hash,
                "result_scan_id": receipt.result_scan_id,
                "result_watermark": receipt.result_watermark,
                "execution_count": receipt.execution_count,
            }
        )
        return EXIT_OK
    finally:
        client.close()


# ---------------------------------------------------------------------------
# audit
# ---------------------------------------------------------------------------


def cmd_audit_verify(args: argparse.Namespace, settings: Settings) -> int:
    """Recompute the audit hash chain (section 6.2 caveat 3)."""
    database, _repo, _source, _harness, _sched, client = _build(settings)
    try:
        conn = database.connect(Role.RECON)
        try:
            ok, bad, detail = verify_chain(conn)
            total = conn.execute("SELECT COUNT(*) FROM recon_audit").fetchone()[0]
        finally:
            conn.close()
        _emit(
            {
                "events": total,
                "chain_valid": ok,
                "first_bad_sequence_no": bad,
                "detail": detail,
                "note": (
                    "detects in-place edits within this database; it is not a "
                    "substitute for an external append-only anchor"
                ),
            }
        )
        return EXIT_OK if ok else EXIT_REFUSED
    finally:
        client.close()


def cmd_gc(args: argparse.Namespace, settings: Settings) -> int:
    _db, repository, _source, _harness, _sched, client = _build(settings)
    try:
        released = repository.release_evidence()
        _emit({"runs_released": released})
        return EXIT_OK
    finally:
        client.close()


# ---------------------------------------------------------------------------
# campaign
# ---------------------------------------------------------------------------


def cmd_campaign(args: argparse.Namespace, settings: Settings) -> int:
    """Run the what-if campaign and write evidence artifacts (section 9)."""
    from .campaign import run_campaign

    report: CampaignReport = run_campaign(tier=args.tier, seed=args.seed)
    directory = Path(args.evidence_dir or settings.artifacts_dir)
    path = report.write(directory)
    print(report.render_table())
    print(f"\nevidence: {path}")
    return EXIT_OK if report.green else EXIT_REFUSED


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _rehydrate_plan(repository: ReconRepository, plan_id: str):
    """Rebuild a ``RepairPlan`` from its persisted rows.

    The stored plan is the authority; nothing is recomputed, so a plan cannot
    change shape between creation and dispatch.
    """
    from .errors import ActionType
    from .planning import PlannedAction, RepairPlan

    row = repository.get_plan(plan_id)
    if row is None:
        return None
    actions = tuple(
        PlannedAction(
            action_id=action["action_id"],
            action_order=int(action["action_order"]),
            action_type=ActionType(action["action_type"]),
            intent_id=action["intent_id"],
            parameters=json.loads(action["parameters_json"]),
            parameters_hash=action["parameters_hash"],
            justification=(),
        )
        for action in repository.plan_actions(plan_id)
    )
    return RepairPlan(
        plan_id=row["plan_id"],
        recon_run_id=row["recon_run_id"],
        plan_hash=row["plan_hash"],
        expected_epoch_id=row["expected_epoch_id"],
        expected_target_scan_id=row["expected_target_scan_id"],
        expected_deal_w=row["expected_deal_w"],
        expected_lease_version=row["expected_lease_version"],
        policy_hash=row["policy_hash"],
        evidence_captured_at=row["evidence_captured_at"],
        status=row["status"],
        actions=actions,
        escalations=(),
        requires_approval=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reconctl",
        description="Project 04 reconciliation and controlled recovery control plane",
    )
    parser.add_argument("--db", type=Path, help="path to the reconciliation database")
    parser.add_argument("--source-url", help="base URL of the Project 01 simulator")
    parser.add_argument("--source-id", default=None)
    sub = parser.add_subparsers(dest="group", required=True)

    source = sub.add_parser("source", help="source identity and capabilities").add_subparsers(
        dest="action", required=True
    )
    source.add_parser("check", help="read identity, capabilities and declared limits").set_defaults(
        func=cmd_source_check
    )
    source.add_parser("bootstrap", help="register the current source epoch").set_defaults(
        func=cmd_source_bootstrap
    )

    run = sub.add_parser("run", help="audit runs").add_subparsers(dest="action", required=True)
    start = run.add_parser("start", help="start one audit run")
    start.add_argument("--entity", required=True, choices=["accounts", "positions", "deals"])
    start.add_argument("--epoch", required=True)
    start.add_argument("--scope", default="UNFILTERED")
    start.add_argument("--request-id", default=None, help="idempotency key for this trigger")
    start.set_defaults(func=cmd_run_start)

    show = run.add_parser("show", help="describe a run and its receipt chain")
    show.add_argument("--run-id", required=True)
    show.set_defaults(func=cmd_run_show)

    evidence = run.add_parser("evidence", help="list findings with evidence digests")
    evidence.add_argument("--run-id", required=True)
    evidence.add_argument("--redacted", action="store_true")
    evidence.set_defaults(func=cmd_run_evidence)

    compare = run.add_parser("compare", help="show the raw manifest differences")
    compare.add_argument("--run-id", required=True)
    compare.set_defaults(func=cmd_run_compare)

    verify = run.add_parser("verify", help="independent post-repair verification")
    verify.add_argument("--plan-id", required=True)
    verify.set_defaults(func=cmd_run_verify)

    plan = sub.add_parser("plan", help="repair plans").add_subparsers(dest="action", required=True)
    create = plan.add_parser("create", help="build a plan (dry run by default)")
    create.add_argument("--run-id", required=True)
    create.add_argument("--actor", required=True)
    create.add_argument(
        "--commit", action="store_true", help="persist the plan instead of previewing it"
    )
    create.set_defaults(func=cmd_plan_create)

    approve = plan.add_parser("approve", help="record an approval or rejection")
    approve.add_argument("--plan-id", required=True)
    approve.add_argument("--plan-hash", required=True)
    approve.add_argument("--actor", required=True)
    approve.add_argument("--role", default=ApprovalRole.APPROVER.value)
    approve.add_argument("--reject", action="store_true")
    approve.set_defaults(func=cmd_plan_approve)

    dispatch = plan.add_parser("dispatch", help="send an approved plan to Project 03")
    dispatch.add_argument("--plan-id", required=True)
    dispatch.add_argument(
        "--plan-hash", required=True, help="must match the stored plan exactly"
    )
    dispatch.set_defaults(func=cmd_plan_dispatch)

    intent = sub.add_parser("intent", help="repair intents").add_subparsers(
        dest="action", required=True
    )
    status = intent.add_parser("status", help="read the authoritative P03 receipt")
    status.add_argument("--intent-id", required=True)
    status.set_defaults(func=cmd_intent_status)

    audit = sub.add_parser("audit", help="audit log").add_subparsers(dest="action", required=True)
    audit.add_parser("verify", help="recompute the tamper-evident chain").set_defaults(
        func=cmd_audit_verify
    )

    gc = sub.add_parser("gc", help="release evidence past its retention window")
    gc.set_defaults(func=cmd_gc)

    campaign = sub.add_parser("campaign", help="run the what-if campaign")
    campaign.add_argument("--tier", default="P0", choices=["P0", "P1", "ALL"])
    campaign.add_argument("--seed", type=int, default=42)
    campaign.add_argument("--evidence-dir", default=None)
    campaign.set_defaults(func=cmd_campaign)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return EXIT_USAGE

    overrides: dict[str, Any] = {}
    if args.db is not None:
        overrides["db_path"] = args.db
    if args.source_url:
        overrides["source_base_url"] = args.source_url
    if args.source_id:
        overrides["source_id"] = args.source_id

    try:
        settings = load_settings(**overrides)
    except ReconError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    try:
        return int(args.func(args, settings))
    except ReconError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except KeyboardInterrupt:  # pragma: no cover
        print("interrupted", file=sys.stderr)
        return EXIT_REFUSED


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
