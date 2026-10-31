"""Credentials, plane separation, SSRF and secret containment, over real TCP.

Covers WF2-A-05, WF2-A-07, WF2-B-01, WF2-B-02, WF2-B-06, WF2-B-09, WF2-H-06,
WF2-N-01, WF2-N-02, WF2-N-03, WF2-N-04, WF2-D-11, INV-15 and INV-16.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from mt5_connector.client import Connector, build_driver_factory
from mt5_connector.config import Settings
from mt5_connector.connection_fsm import SessionState
from mt5_connector.errors import (
    AuthenticationError,
    ConfigurationError,
    PermissionDeniedError,
    SourceContractError,
)
from mt5_connector.ports import Entity, ReadPlan
from mt5_connector.telemetry import SECRETS, JsonFormatter, RedactingFilter

from ..conftest import SECRET_CANARY, make_settings
from ..fakesim import ADMIN_TOKEN, DATA_TOKEN

pytestmark = [pytest.mark.contract]


async def connector_for(settings: Settings, token: str) -> Connector:
    return Connector(settings, driver_factory=build_driver_factory(settings, token=token))


class TestCredentials:
    async def test_an_unknown_token_disables_the_source_without_a_retry_storm(
        self, sim_settings
    ) -> None:
        """WF2-B-01, WF2-H-06: one classification, not 300 attempts."""
        connector = await connector_for(sim_settings, "not-a-real-token-zzzz")
        try:
            with pytest.raises(AuthenticationError):
                await connector.connect()
            assert connector.supervisor.state is SessionState.DISABLED
            attempts_after_first = connector.supervisor.connect_attempts_total

            # Ninety-nine more callers arrive. None of them reaches the source.
            for _ in range(99):
                with pytest.raises(AuthenticationError):
                    await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
            assert connector.supervisor.connect_attempts_total == attempts_after_first
            ready, reasons = connector.supervisor.ready()
            assert ready is False
            assert "session_state_disabled" in reasons
        finally:
            await connector.close()

    async def test_a_revoked_credential_disables_a_ready_session(
        self, sim_settings, sim_server, data_token_env
    ) -> None:
        """WF2-B-09."""
        connector = await connector_for(sim_settings, data_token_env)
        try:
            await connector.connect()
            await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
            sim_server.sim.reject_credentials = True
            with pytest.raises(AuthenticationError):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
            assert connector.supervisor.ready()[0] is False
        finally:
            await connector.close()

    async def test_reconfiguring_is_the_only_way_out_of_disabled(
        self, sim_settings, sim_server
    ) -> None:
        """Section 6.4: rotation recovers; a retry does not."""
        connector = await connector_for(sim_settings, "wrong-token-aaaaaaaa")
        try:
            with pytest.raises(AuthenticationError):
                await connector.connect()
            assert connector.supervisor.state is SessionState.DISABLED
            connector.supervisor.reconfigured()
            assert connector.supervisor.state in (
                SessionState.BACKOFF,
                SessionState.CONNECTING,
            )
        finally:
            await connector.close()

    async def test_a_missing_token_stops_the_driver_factory(self, sim_settings) -> None:
        """WF2-A-07: no unauthenticated reads are issued."""
        with pytest.raises(ConfigurationError) as excinfo:
            build_driver_factory(sim_settings, token=None)
        assert excinfo.value.details["token_env"] == "SIMULATOR_DATA_TOKEN"


class TestPlaneSeparation:
    async def test_an_admin_token_in_the_connector_is_still_data_plane_only(
        self, sim_settings
    ) -> None:
        """WF2-N-02, INV-16: privilege separation is about the *path*, too.

        Even handed an admin credential, the connector has no route to the
        control plane: :meth:`Settings.url_for` composes only data-plane
        paths, so there is nothing to send it to.
        """
        connector = await connector_for(sim_settings, ADMIN_TOKEN)
        try:
            await connector.connect()
            for name in ("faults", "reset", "/admin/v1/faults", "/control/reset"):
                with pytest.raises(ConfigurationError):
                    sim_settings.url_for(name)
        finally:
            await connector.close()

    async def test_the_data_token_is_refused_on_the_control_plane(
        self, sim_server, data_token_env
    ) -> None:
        """WF2-B-02, INV-16: asserted against the source, not just our config."""
        import httpx

        with httpx.Client(timeout=5.0) as client:
            denied = client.post(
                f"{sim_server.base_url}/control/reset",
                headers={"Authorization": f"Bearer {DATA_TOKEN}"},
                json={},
            )
            assert denied.status_code == 403
            assert denied.json()["error"]["code"] == "FORBIDDEN"

            allowed = client.post(
                f"{sim_server.base_url}/control/reset",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                json={},
            )
            assert allowed.status_code == 200

    async def test_a_403_is_classified_as_permanent(self, sim_settings, sim_server) -> None:
        connector = await connector_for(sim_settings, DATA_TOKEN)
        try:
            await connector.connect()
            sim_server.sim.add_fault(
                "http_error", path="/v1/deals", status=503, max_activations=1
            )
            sim_server.sim.clear_faults()
            # Model a 403 from the data plane directly.
            from mt5_connector.drivers.base import DriverResponse
            from mt5_connector.validation import map_error_response

            error = map_error_response(
                DriverResponse(
                    status=403,
                    payload={"error": {"code": "FORBIDDEN"}},
                    byte_count=32,
                    content_type="application/json",
                ),
                source_id="broker-sim-01",
                operation="fetch_deals",
                retry_after_cap_s=2.0,
            )
            assert isinstance(error, PermissionDeniedError)
            assert error.retryable is False
        finally:
            await connector.close()


class TestSsrf:
    def test_a_caller_cannot_supply_a_destination(self) -> None:
        """WF2-N-01: prevented by having no mechanism, not by a filter."""
        settings = make_settings(base_url="http://127.0.0.1:8000")
        hostile = [
            "http://169.254.169.254/latest/meta-data/",
            "http://metadata.google.internal/",
            "file:///etc/passwd",
            "../admin",
            "//evil.example.com/v1/deals",
        ]
        for candidate in hostile:
            with pytest.raises(ConfigurationError):
                settings.url_for(candidate)

    def test_a_metadata_address_is_refused_at_open(self) -> None:
        """WF2-C-11, WF2-N-08: the name passed; the address must too."""
        settings = make_settings(
            base_url="http://127.0.0.1:8000",
            source_kind="simulator",
            driver="simulator_sync_http",
        )
        from mt5_connector.drivers import simulator_sync_http as module

        original = module.socket.getaddrinfo
        try:
            module.socket.getaddrinfo = lambda *a, **k: [  # type: ignore[assignment]
                (2, 1, 6, "", ("169.254.169.254", 80))
            ]
            driver = module.SimulatorSyncHttpDriver(settings, token="x" * 12)
            with pytest.raises(ConfigurationError) as excinfo:
                driver.open()
            assert excinfo.value.details["reason"] == "address_not_permitted"
        finally:
            module.socket.getaddrinfo = original  # type: ignore[assignment]

    async def test_a_redirect_is_not_followed(self, sim_settings, sim_server) -> None:
        """WF2-D-11: following it would resend the credential elsewhere."""
        connector = await connector_for(sim_settings, DATA_TOKEN)
        try:
            await connector.connect()
            rule = sim_server.sim.add_fault(
                "redirect",
                path="/v1/deals",
                max_activations=10,
                location="http://169.254.169.254/v1/deals",
            )
            with pytest.raises(SourceContractError) as excinfo:
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
            assert excinfo.value.details["reason"] == "redirect"
            assert rule.activations >= 1
        finally:
            await connector.close()


class TestSecretContainment:
    async def test_the_canary_never_reaches_a_log_record(
        self, sim_settings, sim_server, monkeypatch
    ) -> None:
        """WF2-B-06, WF2-N-07, WF2-P-05, INV-15."""
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        handler.addFilter(RedactingFilter(SECRETS))
        logger = logging.getLogger("mt5_connector")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        token = SECRET_CANARY
        monkeypatch.setenv("SIMULATOR_DATA_TOKEN", token)
        # The source will reject this token, which is the interesting case:
        # the failure path is where a credential usually leaks.
        connector = await connector_for(sim_settings, token)
        try:
            with pytest.raises(AuthenticationError):
                await connector.connect()
            logger.warning("handshake failed for %s", token)
            logger.error("url", extra={"detail": f"GET /v1/meta?token={token}"})
        finally:
            await connector.close()
            logger.removeHandler(handler)

        output = stream.getvalue()
        assert output
        assert token not in output
        assert "[REDACTED]" in output
        for line in output.strip().splitlines():
            json.loads(line)  # every record is still one valid JSON object

    async def test_an_error_envelope_carries_no_credential(
        self, sim_settings, monkeypatch
    ) -> None:
        monkeypatch.setenv("SIMULATOR_DATA_TOKEN", SECRET_CANARY)
        connector = await connector_for(sim_settings, SECRET_CANARY)
        try:
            with pytest.raises(AuthenticationError) as excinfo:
                await connector.connect()
            rendered = json.dumps(excinfo.value.to_envelope())
            assert SECRET_CANARY not in rendered
            assert "Bearer" not in rendered
        finally:
            await connector.close()

    def test_the_settings_repr_contains_no_credential(self) -> None:
        settings = make_settings()
        assert SECRET_CANARY not in repr(settings)
        assert SECRET_CANARY not in json.dumps(settings.redacted())


class TestTransportPolicy:
    def test_tls_verification_cannot_be_disabled_for_a_remote_source(self) -> None:
        """WF2-N-04, WF2-C-06: no verify=False escape hatch."""
        with pytest.raises(ConfigurationError):
            make_settings(base_url="https://broker.example.com", tls_verify=False)

    def test_only_the_four_data_paths_are_reachable(self) -> None:
        from mt5_connector.config import DATA_PLANE_PATHS

        assert set(DATA_PLANE_PATHS.values()) == {
            "/v1/meta",
            "/v1/accounts",
            "/v1/positions",
            "/v1/deals",
        }
        # Read-only by construction: nothing here writes, resets or injects.
        assert all(path.startswith("/v1/") for path in DATA_PLANE_PATHS.values())
