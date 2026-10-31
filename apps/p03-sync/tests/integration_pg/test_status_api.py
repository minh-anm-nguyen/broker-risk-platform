"""Status surface: liveness, readiness and per-entity freshness (group Q)."""

from __future__ import annotations

import httpx
import pytest

from trading_sync.domain.snapshots import Entity
from trading_sync.status.api import build_app

pytestmark = pytest.mark.integration


@pytest.fixture
def client(sink, settings, sessions, metrics) -> httpx.AsyncClient:
    app = build_app(sink, settings, sessions, metrics)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://status"
    )


@pytest.mark.whatif("WF3-193")
async def test_liveness_ignores_the_source_and_the_database(client) -> None:
    """Liveness answers only "is this process running".

    Coupling it to upstream availability means a source outage gets the process
    restarted, which fixes nothing and loses the in-flight scan's lease early.
    """
    async with client as http:
        response = await http.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


@pytest.mark.whatif("WF3-164")
async def test_readiness_reports_the_migrated_schema_version(client) -> None:
    """Readiness is a cheap bounded probe, not a data-quality check.

    It confirms the schema is migrated, because DML against an incompatible
    schema is the failure WF3-164 describes. It does not count rows: a
    readiness check that scanned a million-row table would become the outage
    it is meant to detect.
    """
    async with client as http:
        response = await http.get("/health/ready")
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "ready"
    assert body["checks"]["schema_version"] == "0001_initial"


@pytest.mark.whatif("WF3-193", "WF3-194")
async def test_status_reports_staleness_rather_than_a_clean_bill(
    client, sink, epoch
) -> None:
    """A source that has never synced is DEGRADED, not ACTIVE.

    Reporting "no errors" for an entity that has never succeeded is the false
    all-green WF3-193 warns about; absence of a failure is not evidence of
    freshness.
    """
    async with client as http:
        response = await http.get("/sync/status", params={"source_id": "sim_local"})
    body = response.json()
    assert response.status_code == 200
    assert body["state"] == "DEGRADED"
    for entity in ("accounts", "positions", "deals"):
        assert body["entities"][entity]["stale"] is True
        assert body["entities"][entity]["published"] is False
        assert body["entities"][entity]["last_success_age_seconds"] is None


@pytest.mark.whatif("WF3-217", "WF3-221")
async def test_status_exposes_each_entity_separately_and_never_claims_a_global_cut(
    client, deal_service, snapshot_service, source
) -> None:
    """Three cuts are reported as three cuts.

    ``cross_entity_consistent`` cannot be true because no coordinated read
    exists across the entities. It is present so a consumer has to acknowledge
    that rather than assume the three numbers describe one instant (ADR-005).
    """
    source.set_account("A1")
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    source.add_deal("D1")
    await deal_service.run_once(source)

    async with client as http:
        body = (
            await http.get("/sync/status", params={"source_id": "sim_local"})
        ).json()

    assert body["cross_entity_consistent"] is False
    assert body["reconciliation_required"] is True
    assert body["entities"]["accounts"]["published"] is True
    assert body["entities"]["deals"]["published"] is True
    assert body["entities"]["positions"]["published"] is False
    assert (
        body["entities"]["deals"]["watermark_seq"]
        > body["entities"]["accounts"]["watermark_seq"]
    )


@pytest.mark.whatif("WF3-183")
async def test_status_refuses_an_unauthorized_source(client) -> None:
    """No unscoped status: a caller must name a source it is allowed to see.

    An endpoint that defaulted to "all sources" would let one tenant enumerate
    another's (design 11.5).
    """
    async with client as http:
        response = await http.get("/sync/status", params={"source_id": "someone_else"})
    assert response.status_code == 403


@pytest.mark.whatif("WF3-204")
async def test_metrics_endpoint_exposes_no_per_record_series(client, deal_service, source) -> None:
    """Scraping must not publish one series per deal or per account."""
    source.add_deal("D1")
    await deal_service.run_once(source)
    async with client as http:
        body = (await http.get("/metrics")).text
    assert "sync_checkpoint_source_seq" in body
    for forbidden in ("deal_id=", "account_id=", "scan_id="):
        assert forbidden not in body
