"""Read-only health, freshness and metrics surface (design 12.4).

The distinctions here are the point of the module.

**Liveness is not readiness, and readiness is not synced.** ``/health/live``
answers "is this process running" and deliberately never touches the database
or the source -- tying liveness to upstream availability means a source outage
gets the pod restarted, which helps nothing. ``/health/ready`` answers "can
this process work safely". Neither claims the data is current (WF3-193).

**Freshness is per entity.** A successful deal scan says nothing about whether
the position snapshot is current, so they are reported separately with their
own sequences and ages. One combined number would be a fiction (design 7.5).

**``cross_entity_consistent`` is hard-coded false.** There is no coordinated
cut across the three entities, so the flag cannot be true. It exists so a
consumer must acknowledge that rather than assume otherwise (ADR-005,
WF3-217).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from trading_sync.config import Settings
from trading_sync.domain.snapshots import EpochStatus
from trading_sync.infrastructure.metrics import SyncMetrics
from trading_sync.infrastructure.repository import PostgresSink

__all__ = ["build_app", "freshness_report"]

#: Beyond this, an entity is reported stale. A target to tune after
#: measurement, not a validated SLO (design 16.2).
DEFAULT_STALE_AFTER_SECONDS = 900


def build_app(
    sink: PostgresSink,
    settings: Settings,
    sessions: Any,
    metrics: SyncMetrics,
    *,
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
) -> FastAPI:
    app = FastAPI(title="trading-data-sync status", docs_url=None, redoc_url=None)

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        """Process-only liveness. No database, no source, no query."""
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready(response: Response) -> dict[str, Any]:
        """Can this process work safely right now?

        The probe is deliberately cheap -- ``SELECT 1`` plus a migration-version
        lookup. A readiness check that counted rows in a million-row table
        would itself become the outage it is meant to detect (design 9.3).
        """
        detail: dict[str, Any] = {"status": "ready", "checks": {}}
        try:
            async with sessions() as session:
                await session.execute(text("SELECT 1"))
                version = (
                    await session.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar()
            detail["checks"]["database"] = "ok"
            detail["checks"]["schema_version"] = version
            if version is None:
                # Running against an unmigrated schema would mean DML against
                # tables that may not match the models (WF3-164).
                detail["status"] = "not_ready"
                detail["checks"]["database"] = "schema not migrated"
        except Exception as exc:  # noqa: BLE001 - reported, never raised onward
            detail["status"] = "not_ready"
            detail["checks"]["database"] = type(exc).__name__
        if detail["status"] != "ready":
            response.status_code = 503
        return detail

    @app.get("/sync/status")
    async def sync_status(
        source_id: str = Query(..., description="an explicitly authorized source id"),
    ) -> dict[str, Any]:
        """Per-entity freshness for one source.

        ``source_id`` is required rather than defaulted. An unscoped status
        endpoint would let a caller enumerate every source the service touches
        (design 11.5, WF3-183).
        """
        if settings.security.require_source_scope and source_id != settings.source.source_id:
            raise HTTPException(status_code=403, detail="source is not authorized here")
        epoch = await sink.get_active_epoch(source_id)
        if epoch is None:
            raise HTTPException(status_code=404, detail="no active epoch for this source")
        checkpoints = await sink.read_checkpoints(epoch.epoch_id)
        return freshness_report(
            source_id=source_id,
            epoch_label=epoch.label,
            epoch_status=epoch.status,
            checkpoints=checkpoints,
            stale_after_seconds=stale_after_seconds,
        )

    @app.get("/metrics")
    async def prometheus() -> Response:
        return Response(
            content=generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST
        )

    return app


def freshness_report(
    *,
    source_id: str,
    epoch_label: str,
    epoch_status: EpochStatus,
    checkpoints: list[dict[str, Any]],
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
) -> dict[str, Any]:
    """Assemble the status payload from durable checkpoint rows.

    Derived from the database rather than from in-process counters, so a
    worker that logged success before its COMMIT failed cannot make the status
    endpoint lie (WF3-196).
    """
    entities: dict[str, Any] = {}
    any_stale = False
    for row in checkpoints:
        last_success = row.get("last_success_at")
        now = row.get("db_now") or datetime.now(timezone.utc)
        age = None if last_success is None else (now - last_success).total_seconds()
        stale = age is None or age > stale_after_seconds
        any_stale = any_stale or stale
        entities[str(row["entity"])] = {
            "watermark_seq": int(row["watermark"]),
            "published": row.get("published_scan_id") is not None,
            "last_success_age_seconds": None if age is None else round(age, 3),
            "stale": stale,
            "lease_version": int(row.get("lease_version", 0)),
            "scope": row.get("filter_hash"),
        }
    degraded = any_stale or epoch_status is not EpochStatus.ACTIVE
    return {
        "source_id": source_id,
        "epoch": epoch_label,
        "epoch_status": str(epoch_status),
        "entities": entities,
        # No coordinated cut exists across entities, so this cannot be true.
        # Project 04 owns reconciliation; see ADR-005.
        "cross_entity_consistent": False,
        "reconciliation_required": True,
        "state": "DEGRADED" if degraded else "ACTIVE",
    }
