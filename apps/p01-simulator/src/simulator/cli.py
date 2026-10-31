"""Operator CLI.

``generate``, ``verify`` and ``digest`` run in-process rather than over HTTP on
purpose: seeding a dataset and auditing it are operator tasks, and routing them
through the network would add an authentication and rate-limiting path that has
nothing to do with the job. They still go through the same writer and the same
invariant suite, so there is no second code path for correctness.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

from . import GENERATOR_VERSION, __version__
from .canonical import parse_timestamp
from .domain import accounting as acc
from .domain import invariants as inv
from .generation.coordinator import ScenarioCoordinator
from .generation.generator import ScenarioSpec
from .observability.logging import configure_logging, get_logger
from .observability.metrics import Metrics
from .persistence.connection import (
    ProcessLock,
    ReadConnectionPool,
    initialize_database,
    verify_integrity,
    open_connection,
)
from .persistence.failpoints import controller_from_settings
from .persistence.writer import WriterService
from .settings import Settings

logger = get_logger("cli")


def _load_spec(path: Path) -> ScenarioSpec:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ScenarioSpec(
        scenario_name=payload["scenario_name"],
        seed=int(payload["seed"]),
        start_virtual_time=parse_timestamp(payload["start_virtual_time"]),
        account_count=int(payload["account_count"]),
        deal_count=int(payload["deal_count"]),
        max_open_positions=int(payload.get("max_open_positions", 0)),
        symbols=tuple(payload["symbols"]),
        initial_balance_usd=acc.parse_decimal(
            payload["initial_balance_usd"], field="initial_balance_usd"
        ),
        generator_version=str(payload.get("generator_version", GENERATOR_VERSION)),
        execution_mode=str(payload.get("execution_mode", "bulk_generate")),
        fault_profile=str(payload.get("fault_profile", "none")),
    )


class _Runtime:
    """Minimal in-process runtime: writer, pool and coordinator, nothing else."""

    def __init__(self, settings: Settings, *, lock: bool = True) -> None:
        self.settings = settings
        self.schema_version = initialize_database(settings)
        self._lock = ProcessLock(settings.lock_path) if lock else None
        if self._lock is not None:
            self._lock.acquire()
        self.metrics = Metrics()
        self.pool = ReadConnectionPool(settings)
        self.writer = WriterService(
            settings,
            metrics=self.metrics,
            failpoints=controller_from_settings(
                settings.failpoint, settings.failpoint_at_seq, settings.failpoint_mode
            ),
        )
        self.writer.start()
        self.coordinator = ScenarioCoordinator(
            settings, self.writer, self.pool, metrics=self.metrics
        )
        self.coordinator.startup()

    def close(self) -> None:
        self.coordinator.shutdown()
        self.writer.stop(drain=True, timeout=30.0)
        self.pool.close()
        if self._lock is not None:
            self._lock.release()


async def _generate(settings: Settings, spec: ScenarioSpec, *, reset: bool) -> int:
    runtime = _Runtime(settings)
    try:
        existing = runtime.coordinator.active_run()
        if existing is not None and not existing.status.is_terminal:
            if not reset:
                print(
                    f"an active run already exists ({existing.run_id}, generation "
                    f"{existing.generation}). Pass --reset to start a new generation.",
                    file=sys.stderr,
                )
                return 2
            result = await runtime.coordinator.reset(
                spec=spec,
                expected_generation=existing.generation,
                idempotency_key=None,
                request_hash="cli-reset",
            )
        else:
            result = await runtime.coordinator.create_run(
                spec, idempotency_key=None, request_hash="cli-create"
            )

        job_id = result["job_id"]
        run_id = result["run_id"]
        print(f"run {run_id} generation {result['generation']} job {job_id}")
        started = time.perf_counter()
        last_reported = -1
        while True:
            job = runtime.coordinator.job(job_id)
            if job is None:
                print("job disappeared", file=sys.stderr)
                return 1
            if job.processed_count != last_reported:
                last_reported = job.processed_count
                print(
                    f"  {job.status.value:<10} {job.processed_count}/{job.target_count} {job.unit}",
                    flush=True,
                )
            if job.status.is_terminal:
                break
            await asyncio.sleep(0.25)

        elapsed = time.perf_counter() - started
        run = runtime.coordinator.run(run_id)
        assert run is not None
        print(
            f"job {job.status.value} in {elapsed:.2f}s | committed_seq={run.committed_seq} "
            f"accounts={run.account_count} deals={run.deal_count} open={run.open_position_count}"
        )
        if job.error_code:
            print(f"error: {job.error_code}: {job.error_message}", file=sys.stderr)
            return 1
        return 0
    finally:
        runtime.close()


def _verify(settings: Settings, run_id: str | None, *, integrity: bool) -> int:
    conn = open_connection(settings, writable=False, check_same_thread=True)
    try:
        if integrity:
            verify_integrity(conn)
            print("integrity_check: ok")
        from .persistence import event_store as es

        target = run_id or es.load_active_run_id(conn)
        if target is None:
            print("no run to verify", file=sys.stderr)
            return 2
        results = inv.run_all(conn, target)
        digest, counts = inv.canonical_state_digest(conn, target)
        summary = inv.summarize(results)
        width = max(len(item.title) for item in results)
        for item in results:
            print(f"{item.status:<15} {item.invariant_id}  {item.title:<{width}}  {item.detail}")
        print()
        print(f"run              {target}")
        print(f"canonical digest {digest}")
        print(f"record counts    {counts}")
        print(
            f"summary          passed={summary['passed']} failed={summary['failed']} "
            f"skipped={summary['skipped']} not_applicable={summary['not_applicable']}"
        )
        if summary["failed"]:
            print(f"FAILED: {summary['failed_ids']}", file=sys.stderr)
            return 1
        if not summary["storage_scope_ok"]:
            # Reaching here means a storage-scope check did not run. Reporting
            # success would hand a release gate a green light nobody earned.
            print(
                f"INCOMPLETE: storage-scope invariants did not all run "
                f"(skipped={summary['skipped_ids']})",
                file=sys.stderr,
            )
            return 1
        # NOT_APPLICABLE is never counted as a pass: the harness-scope
        # invariants are proven by the named tests, not by this command.
        print(
            "storage-scope invariants passed. Harness-scope invariants "
            "(INV-09/10/11/12/16) are proven by their named tests, not here."
        )
        return 0
    finally:
        conn.close()


def _digest(settings: Settings, run_id: str | None) -> int:
    conn = open_connection(settings, writable=False, check_same_thread=True)
    try:
        from .persistence import event_store as es

        target = run_id or es.load_active_run_id(conn)
        if target is None:
            print("no run", file=sys.stderr)
            return 2
        digest, counts = inv.canonical_state_digest(conn, target)
        print(json.dumps({"run_id": target, "digest": digest, "counts": counts}, indent=2))
        return 0
    finally:
        conn.close()


def _serve(settings: Settings) -> int:
    import uvicorn

    from .main import create_app

    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        # One worker, always. The writer queue and seq allocation are only
        # correct with a single process (WF-120).
        workers=1,
        log_config=None,
        access_log=False,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simulator", description="MT5 Broker Simulator operator CLI"
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="create or verify the database schema")
    sub.add_parser("serve", help="run the HTTP service")

    generate = sub.add_parser("generate", help="seed a dataset from a scenario file")
    generate.add_argument("--scenario", required=True, type=Path)
    generate.add_argument(
        "--reset",
        action="store_true",
        help="replace the active run with a new generation instead of failing",
    )

    verify = sub.add_parser("verify", help="run the storage-scope invariant suite")
    verify.add_argument("--run", dest="run_id", default=None)
    verify.add_argument(
        "--integrity", action="store_true", help="also run PRAGMA integrity_check"
    )

    digest = sub.add_parser("digest", help="print the canonical content digest of a run")
    digest.add_argument("--run", dest="run_id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level, settings.log_format)

    if args.command == "migrate":
        version = initialize_database(settings)
        print(f"schema version {version} at {settings.db_path}")
        return 0
    if args.command == "serve":
        return _serve(settings)
    if args.command == "generate":
        return asyncio.run(_generate(settings, _load_spec(args.scenario), reset=args.reset))
    if args.command == "verify":
        return _verify(settings, args.run_id, integrity=args.integrity)
    if args.command == "digest":
        return _digest(settings, args.run_id)
    return 2  # pragma: no cover - argparse enforces the choices


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
