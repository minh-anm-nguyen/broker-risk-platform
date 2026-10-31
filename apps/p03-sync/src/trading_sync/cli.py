"""Operational CLI (design 19.3).

Implements the commands the design sketched. Two safety properties are built
in rather than documented:

``approve-epoch`` is a deliberate act.
    Approving a *new* epoch after a source reset means a full re-read and a
    second copy of history, so it is a human decision with an audited reason,
    never something a retry loop performs (design 8.3 item 4, WF3-188).

Nothing here can set a watermark.
    There is no ``set-watermark`` command, because a watermark without a
    receipt chain behind it is an unverifiable claim. Catching a lagging
    source up by hand is the shortcut design 12.3 lists as forbidden, and the
    publication guard would reject it anyway (WF3-057).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Sequence

from trading_sync.application.coordinator import SyncCoordinator
from trading_sync.application.source_epoch_guard import SourceEpochGuard
from trading_sync.application.sync_deals import DealSyncService
from trading_sync.application.sync_snapshot import SnapshotSyncService
from trading_sync.config import Settings, load_settings
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import SyncError
from trading_sync.infrastructure.db import create_engine, session_factory
from trading_sync.infrastructure.logging import configure_logging
from trading_sync.infrastructure.metrics import SyncMetrics
from trading_sync.infrastructure.repository import PostgresSink
from trading_sync.infrastructure.source_adapter import HttpConnectorSource

__all__ = ["main", "build_parser"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trading-sync", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    enroll = sub.add_parser("enroll", help="register the configured source")
    enroll.add_argument("--source-id", default=None)

    approve = sub.add_parser(
        "approve-epoch",
        help="activate a discovered source epoch (requires an audited reason)",
    )
    approve.add_argument("--source-id", default=None)
    approve.add_argument("--epoch-id", required=True)
    approve.add_argument(
        "--reason", required=True, help="recorded in sync_audit; required"
    )

    run = sub.add_parser("run", help="run one scan for one entity, or all entities")
    run.add_argument(
        "--entity", choices=[*(str(e) for e in Entity), "all"], default="all"
    )
    run.add_argument("--source-id", default=None)

    status = sub.add_parser("status", help="print per-entity freshness as JSON")
    status.add_argument("--source-id", default=None)

    epochs = sub.add_parser("epochs", help="list known epochs for a source")
    epochs.add_argument("--source-id", default=None)
    return parser


class _Context:
    """Wires the object graph for one CLI invocation."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.metrics = SyncMetrics()
        self.engine = create_engine(settings)
        self.sessions = session_factory(self.engine)
        self.sink = PostgresSink(self.sessions, settings, metrics=self.metrics)
        self.guard = SourceEpochGuard(
            self.sink,
            source_id=settings.source.source_id,
            expected_kind=settings.source.kind,
            require_run_id_and_generation=settings.source.require_run_id_and_generation,
        )
        self.deals = DealSyncService(
            self.sink, settings, self.guard, metrics=self.metrics
        )
        self.snapshots = SnapshotSyncService(
            self.sink, settings, self.guard, metrics=self.metrics
        )
        self.coordinator = SyncCoordinator(
            self.sink, settings, self.deals, self.snapshots, metrics=self.metrics
        )

    async def aclose(self) -> None:
        await self.engine.dispose()


async def _run(args: argparse.Namespace) -> int:
    settings = load_settings()
    configure_logging(
        service=settings.telemetry.service_name, level=settings.telemetry.log_level
    )
    source_id = args.source_id or settings.source.source_id
    context = _Context(settings)
    try:
        if args.command == "enroll":
            await context.sink.enroll_source(source_id, str(settings.source.kind))
            _emit({"enrolled": source_id, "kind": str(settings.source.kind)})
            return 0

        if args.command == "approve-epoch":
            import uuid

            epoch = await context.sink.approve_epoch(
                uuid.UUID(args.epoch_id), reason=args.reason
            )
            _emit({"approved": str(epoch.epoch_id), "epoch": epoch.label})
            return 0

        if args.command == "epochs":
            epochs = await context.sink.list_epochs(source_id)
            _emit(
                {
                    "source_id": source_id,
                    "epochs": [
                        {
                            "epoch_id": str(item.epoch_id),
                            "label": item.label,
                            "status": str(item.status),
                        }
                        for item in epochs
                    ],
                }
            )
            return 0

        if args.command == "status":
            from trading_sync.status.api import freshness_report

            epoch = await context.sink.get_active_epoch(source_id)
            if epoch is None:
                _emit({"source_id": source_id, "state": "NO_ACTIVE_EPOCH"})
                return 1
            checkpoints = await context.sink.read_checkpoints(epoch.epoch_id)
            _emit(
                freshness_report(
                    source_id=source_id,
                    epoch_label=epoch.label,
                    epoch_status=epoch.status,
                    checkpoints=checkpoints,
                )
            )
            return 0

        if args.command == "run":
            async with HttpConnectorSource(settings, metrics=context.metrics) as source:
                entities = (
                    [Entity.ACCOUNTS, Entity.POSITIONS, Entity.DEALS]
                    if args.entity == "all"
                    else [Entity(args.entity)]
                )
                results = []
                for entity in entities:
                    result = await context.coordinator.sync_entity(source, entity)
                    results.append(_describe(result))
                _emit({"source_id": source_id, "results": results})
                # Non-zero when any entity failed, so a scheduler notices. A
                # partial success is still a failure to report (WF3-221).
                return 0 if all(item["ok"] for item in results) else 1

        raise AssertionError(f"unhandled command {args.command!r}")
    except SyncError as exc:
        _emit({"error": exc.reason_code, "detail": exc.message, **exc.safe_details})
        return 2
    finally:
        await context.aclose()


def _describe(result: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "entity": str(result.entity),
        "ok": result.ok,
        "attempts": result.attempts,
    }
    if result.outcome is not None:
        outcome = result.outcome
        payload.update(
            {
                "status": str(outcome.status),
                "pages": outcome.pages,
                "records": outcome.records,
                "duplicates": outcome.duplicates,
                "watermark_before": outcome.watermark_before,
                "watermark_after": outcome.watermark_after,
                "advanced": outcome.advanced,
            }
        )
        if outcome.boundary_anomalies:
            payload["source_boundary_anomalies"] = outcome.boundary_anomalies
    if result.error is not None:
        payload["reason_code"] = result.reason_code
    return payload


def _emit(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
