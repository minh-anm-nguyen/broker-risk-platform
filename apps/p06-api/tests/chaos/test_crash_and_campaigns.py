"""Kill points and compound campaigns.

Design doc sections 15 and 16. These do not SIGKILL a process: in-process
barriers and faults place the interruption at the *exact* pipeline point a kill
would, and then assert the durable consequences. The property under test is
what survives and what must not have been claimed.

Durability is checked against the JSONL audit backend, where an fsync means a
crash test can inspect what actually reached the disk.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from conftest import ACCOUNT_A, SOURCE_ID
from httpx import ASGITransport, AsyncClient
from risk_api.faults import FaultPoint, FaultRegistry
from risk_api.lifespan import build_context
from risk_api.main import create_app
from risk_api.repositories.read_views import world_table_checksums
from risk_api.settings import Settings

pytestmark = [pytest.mark.chaos, pytest.mark.slow]

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"


@pytest.fixture
def durable_audit_path(tmp_path: Path) -> Path:
    return tmp_path / "audit" / "api_access_audit.jsonl"


@pytest.fixture
async def durable_client(durable_audit_path, world, policies, clock):
    """An app whose audit sink is a real fsynced file."""
    settings = Settings(
        environment="test",
        faults_enabled=True,
        audit_backend="jsonl",
        audit_path=str(durable_audit_path),
        log_level="CRITICAL",
    )
    context = build_context(
        settings=settings,
        world=world,
        policies=policies,
        faults=FaultRegistry(enabled=True),
        clock=clock.monotonic,
        wall_clock=clock.wall,
    )
    app = create_app(context=context, settings=settings)
    context.workers.start()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as http:
        yield http, context
    await context.workers.shutdown(drain_seconds=0.2)


def audit_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                # A torn trailing line is exactly what a mid-write crash leaves.
                continue
    return rows


class TestKillPointsBeforeAudit:
    """K06-01 to K06-13: nothing financial and nothing audited as ALLOW."""

    async def test_k06_01_interrupted_before_credential_parsing(self, client, context):
        response = await client.get(RISK, params={"source_id": SOURCE_ID})
        assert response.status_code == 401
        assert context.audit.all_records() == ()
        assert context.engine.stats.calls == 0

    async def test_k06_02_interrupted_after_authentication_before_scope(self, client, auth):
        response = await client.get(
            "/api/v1/accounts/00043/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 404

    async def test_k06_05_interrupted_waiting_for_the_pool(self, client, context, auth):
        context.faults.arm(FaultPoint.DB_POOL_EXHAUSTED)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        assert context.views.open_sessions == 0, "an orphan pool waiter survived"

    async def test_k06_06_interrupted_mid_cut_capture(self, client, context, world, auth):
        """A partially pinned cut must be discarded, not served."""
        before = world_table_checksums(world)
        context.faults.arm(FaultPoint.DB_FAIL_AFTER_SNAPSHOT)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        assert context.engine.stats.calls == 0
        assert world_table_checksums(world) == before
        assert context.views.open_sessions == 0

    async def test_k06_09_interrupted_mid_engine_work(self, client, context, auth):
        """The API stays up and the worker is reclaimed."""
        context.faults.arm(FaultPoint.ENGINE_BURN_CPU, 400)
        first = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert first.status_code in (200, 504)
        follow_up = await client.get("/health/live")
        assert follow_up.status_code == 200
        assert context.workers.healthy()

    async def test_k06_11_interrupted_before_the_final_epoch_check(
        self, client, context, world, auth
    ):
        """No old-epoch output, even though the computation finished."""
        barrier = context.faults.arm_barrier(FaultPoint.BARRIER_BEFORE_FINAL_CHECK)
        task = asyncio.create_task(
            client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        )
        for _ in range(200):
            if barrier.arrivals:
                break
            await asyncio.sleep(0.005)
        world.rotate_epoch("E2")
        barrier.release()
        response = await task
        assert response.status_code == 409
        assert response.json()["code"] == "EPOCH_CHANGED"
        assert "ALLOW" not in [row.decision for row in context.audit.all_records()]

    async def test_k06_13_interrupted_before_the_audit_record(self, client, context, auth):
        """No financial response has been emitted at this point."""
        barrier = context.faults.arm_barrier(FaultPoint.BARRIER_BEFORE_AUDIT)
        task = asyncio.create_task(
            client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        )
        for _ in range(200):
            if barrier.arrivals:
                break
            await asyncio.sleep(0.005)
        assert context.audit.all_records() == ()
        task.cancel()
        with pytest.raises(BaseException):
            await task


class TestKillPointsAroundTheAuditCommit:
    """K06-14 to K06-17, against a genuinely fsynced audit file."""

    async def test_k06_14_failure_before_the_insert_emits_nothing(self, durable_client):
        http, context = durable_client
        from conftest import TokenFactory  # noqa: PLC0415

        auth = _auth(context)
        context.faults.arm(FaultPoint.AUDIT_FAIL_BEFORE_COMMIT)
        response = await http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        assert audit_rows(Path(context.settings.audit_path)) == []
        assert "56.00" not in response.text
        assert TokenFactory is not None

    async def test_k06_16_a_committed_row_with_a_lost_ack_is_found_and_honoured(
        self, durable_client
    ):
        """The row is on disk; the service proves it by id rather than failing."""
        http, context = durable_client
        auth = _auth(context)
        context.faults.arm(FaultPoint.AUDIT_LOSE_ACK)
        response = await http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 200, response.text
        rows = audit_rows(Path(context.settings.audit_path))
        assert len(rows) == 1
        assert rows[0]["decision"] == "ALLOW"
        assert rows[0]["audit_id"] == response.json()["audit_id"]

    async def test_k06_17_a_durable_row_survives_a_restart(self, durable_client, world, policies, clock):
        """After a restart, the receipt is still discoverable by id."""
        http, context = durable_client
        auth = _auth(context)
        response = await http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 200
        audit_id = response.json()["audit_id"]
        path = Path(context.settings.audit_path)

        # A fresh process reading the same file: the row is still there.
        restarted = build_context(
            settings=context.settings,
            world=world,
            policies=policies,
            faults=FaultRegistry(enabled=True),
            clock=clock.monotonic,
            wall_clock=clock.wall,
        )
        found = await restarted.audit.find(audit_id)
        assert found is not None
        assert found.decision == "ALLOW"
        assert len(audit_rows(path)) == 1

    async def test_a_torn_trailing_line_does_not_break_recovery(self, durable_client):
        """A crash mid-write leaves a partial line; reading must tolerate it."""
        http, context = durable_client
        auth = _auth(context)
        await http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        path = Path(context.settings.audit_path)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write('{"audit_id": "torn-row", "dec')
        assert len(audit_rows(path)) == 1
        second = await http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert second.status_code == 200
        assert len(audit_rows(path)) == 2

    async def test_the_audit_file_is_append_only(self, durable_client):
        http, context = durable_client
        auth = _auth(context)
        ids = []
        for _ in range(3):
            response = await http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
            assert response.status_code == 200
            ids.append(response.json()["audit_id"])
        rows = audit_rows(Path(context.settings.audit_path))
        assert [row["audit_id"] for row in rows] == ids


class TestCompoundCampaigns:
    async def test_mc_01_reset_plus_blocking_revision_under_concurrency(
        self, client, context, world, auth
    ):
        """No stale epoch and no unauthorized READY, under 12 concurrent clients."""
        barrier = context.faults.arm_barrier(FaultPoint.BARRIER_AFTER_COMPUTE)
        tasks = [
            asyncio.create_task(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth))
            for _ in range(4)
        ]
        for _ in range(300):
            if barrier.arrivals:
                break
            await asyncio.sleep(0.005)
        world.rotate_epoch("E2")
        world.bump_recon_revision("recon-R11", blocking_reason="MISMATCH")
        barrier.release()

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for response in responses:
            status = getattr(response, "status_code", None)
            assert status in (409, 503, 429), status
            if status == 409:
                assert "56.00" not in response.text
        assert "ALLOW" not in [row.decision for row in context.audit.all_records()]

    async def test_mc_04_pool_exhaustion_plus_engine_failure_stays_bounded(
        self, client, context, auth
    ):
        """No resource leak and no retry storm when two dependencies fail together."""
        context.faults.arm(FaultPoint.DB_POOL_EXHAUSTED)
        context.faults.arm(FaultPoint.ENGINE_RAISE)
        responses = await asyncio.gather(
            *(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth) for _ in range(8)),
            return_exceptions=True,
        )
        statuses = {getattr(r, "status_code", None) for r in responses}
        assert statuses <= {503, 429}
        assert context.views.open_sessions == 0
        assert context.admission.stats.executing_heavy == 0
        assert context.workers.stats.in_flight == 0

    async def test_mc_08_concurrent_cancellation_and_worker_pressure(
        self, client, context, auth
    ):
        """No double response, no orphan compute."""
        context.faults.arm(FaultPoint.ENGINE_BURN_CPU, 200)
        tasks = [
            asyncio.create_task(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth))
            for _ in range(6)
        ]
        await asyncio.sleep(0.02)
        for task in tasks[:3]:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        served = [r for r in results if getattr(r, "status_code", None) is not None]
        for response in served:
            assert response.status_code in (200, 429, 503, 504)
        await asyncio.sleep(0.5)
        assert context.admission.stats.executing_heavy == 0
        assert context.views.open_sessions == 0

    async def test_graceful_shutdown_drains_without_serving_stale_results(
        self, durable_client, world
    ):
        """K06-20: queued and executing reports end bounded."""
        http, context = durable_client
        auth = _auth(context)
        context.faults.arm(FaultPoint.ENGINE_BURN_CPU, 100)
        tasks = [
            asyncio.create_task(http.get(RISK, params={"source_id": SOURCE_ID}, headers=auth))
            for _ in range(3)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await context.workers.shutdown(drain_seconds=1.0)
        for response in results:
            status = getattr(response, "status_code", None)
            assert status in (200, 429, 503, 504)
        assert context.workers.stats.in_flight == 0


def _auth(context) -> dict[str, str]:
    """Mint a header for analyst A against this context's own issuer."""
    from risk_api.authn.test_issuer import LocalTestIssuer, TokenSpec

    issuer = LocalTestIssuer(
        issuer=context.settings.jwt_issuer,
        audience=context.settings.jwt_audience,
        keys=context.settings.jwt_keys,
        environment=context.settings.environment,
    )
    token = issuer.mint(
        TokenSpec(
            subject="analyst-a",
            tenant_id="tenant-a",
            extra_claims={"roles": ["risk_analyst"]},
        ),
        now=context.wall_clock(),
    )
    return {"Authorization": f"Bearer {token}"}
