"""Command line entry points: ``serve``, ``probe`` and ``scan``.

Configuration comes from ``CONNECTOR_*`` environment variables only. There is
no ``--base-url`` or ``--token`` flag, deliberately: a credential on a command
line lands in shell history, in ``ps`` output and in CI logs, and a host flag
would reintroduce the arbitrary-destination problem that
:meth:`mt5_connector.config.Settings.url_for` exists to prevent
(WF2-N-01, section 9.1).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from typing import Any

from .client import Connector
from .config import load_settings
from .errors import ConnectorError
from .ports import Entity, ReadPlan
from .scan import Scanner
from .telemetry import configure_logging

__all__ = ["main"]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mt5-connector",
        description="Project 02 - MT5 Integration Connector (read-only).",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the health and metrics surface.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    probe = sub.add_parser("probe", help="Handshake once and print the negotiated contract.")
    probe.add_argument("--timeout-ms", type=int, default=5_000)

    scan = sub.add_parser("scan", help="Run one full scan and print its outcome.")
    scan.add_argument("--entity", default="deals", choices=[item.value for item in Entity])
    scan.add_argument("--limit", type=int, default=None)
    scan.add_argument("--after-seq", type=int, default=None)
    scan.add_argument("--account-id", default=None)
    scan.add_argument("--symbol", default=None)
    scan.add_argument("--deadline-ms", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging(level=getattr(logging, args.log_level))
    try:
        settings = load_settings()
    except ConnectorError as error:
        # A configuration failure exits before anything is allocated: no
        # thread, no session, no upstream request (WF2-A-01).
        print(json.dumps(error.to_envelope(), indent=2), file=sys.stderr)
        return 78  # EX_CONFIG

    if args.command == "serve":
        import uvicorn

        from .app import create_app

        connector = Connector(settings)
        uvicorn.run(
            create_app(connector),
            host=args.host,
            port=args.port,
            log_config=None,
            access_log=False,
        )
        return 0

    if args.command == "probe":
        return asyncio.run(_probe(settings, timeout_ms=args.timeout_ms))
    return asyncio.run(
        _scan(
            settings,
            entity=Entity(args.entity),
            limit=args.limit,
            after_seq=args.after_seq,
            account_id=args.account_id,
            symbol=args.symbol,
            deadline_ms=args.deadline_ms,
        )
    )


async def _probe(settings: Any, *, timeout_ms: int) -> int:
    connector = Connector(settings)
    try:
        identity = await connector.connect()
        capabilities = await connector.capabilities()
    except ConnectorError as error:
        print(json.dumps(error.to_envelope(), indent=2), file=sys.stderr)
        return 1
    finally:
        await connector.close()
    print(
        json.dumps(
            {
                "source_id": identity.source_id,
                "source_kind": identity.source_kind,
                "run_id": identity.run_id,
                "generation": identity.generation,
                "worker_epoch": identity.worker_epoch,
                "contract_version": capabilities.contract_version,
                "capabilities": {
                    "supported_entities": sorted(
                        entity.value for entity in capabilities.supported_entities
                    ),
                    "snapshot_cursor": capabilities.snapshot_cursor,
                    "incremental_deals_after_seq": capabilities.incremental_deals_after_seq,
                    "stable_snapshot_across_entities": (
                        capabilities.stable_snapshot_across_entities
                    ),
                    "max_page_size": capabilities.max_page_size,
                },
            },
            indent=2,
        )
    )
    return 0


async def _scan(
    settings: Any,
    *,
    entity: Entity,
    limit: int | None,
    after_seq: int | None,
    account_id: str | None,
    symbol: str | None,
    deadline_ms: int | None,
) -> int:
    connector = Connector(settings)
    pages = 0
    try:
        await connector.connect()
        plan = ReadPlan(
            entity=entity,
            limit=settings.resolve_limit(limit),
            after_seq=after_seq if entity is Entity.DEALS else None,
            account_id=account_id,
            symbol=symbol,
        )
        scanner = Scanner(connector)

        async def sink(page: Any) -> None:
            nonlocal pages
            pages += 1

        result = await scanner.run(plan, sink=sink, deadline_ms=deadline_ms)
    except ConnectorError as error:
        print(json.dumps(error.to_envelope(), indent=2), file=sys.stderr)
        return 1
    finally:
        await connector.close()

    candidate = result.checkpoint_candidate
    print(
        json.dumps(
            {
                "outcome": result.outcome.value,
                "entity": result.entity.value,
                "pages_seen": result.pages_seen,
                "items_seen": result.items_seen,
                "snapshot_seq": result.snapshot_seq,
                "error_code": result.error_code,
                "reason": result.reason,
                "checkpoint_candidate": (
                    None
                    if candidate is None
                    else {
                        "snapshot_seq": candidate.snapshot_seq,
                        "run_id": candidate.run_id,
                        "generation": candidate.generation,
                        "filter_digest": candidate.filter_digest,
                        "items_seen": candidate.items_seen,
                        "note": candidate.note,
                    }
                ),
            },
            indent=2,
        )
    )
    return 0 if result.complete else 2


if __name__ == "__main__":  # pragma: no cover - entry point
    raise SystemExit(main())
