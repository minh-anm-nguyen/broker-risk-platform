"""Configuration safety and version negotiation.

WF06-AB and WF06-AD. The settings object is a security boundary: it refuses to
start a non-local environment on development key material, refuses an unsigned
algorithm, and refuses sub-budgets that cannot fit the deadline. The version
resolver refuses to serve a request under a policy it did not ask for.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from risk_api.contracts.problem_details import ApiProblem
from risk_api.contracts.versioning import (
    READ_VIEW_CONTRACT_VERSIONS,
    CapabilityHandshake,
    assert_read_view_capability,
    resolve_policy_versions,
)
from risk_api.settings import DEV_CURSOR_KEY, DEV_JWT_KEYS, Settings

pytestmark = pytest.mark.unit


class TestDefaults:
    def test_strict_composite_is_the_default(self):
        """A partial report must always be something the caller asked for."""
        assert Settings(environment="test").strict_composite_default is True

    def test_the_documented_limits_are_the_defaults(self):
        settings = Settings(environment="test")
        assert settings.max_body_bytes == 64 * 1024
        assert settings.max_accounts_per_request == 100
        assert settings.max_positions_page == 100
        assert settings.max_aggregate_symbols == 200
        assert settings.max_pnl_window_days == 31
        assert settings.max_response_bytes == 1024 * 1024
        assert settings.cursor_ttl_seconds == 120

    def test_the_documented_budgets_are_the_defaults(self):
        settings = Settings(environment="test")
        assert settings.compute_budget_seconds == 3.0
        assert settings.request_deadline_seconds == 5.0
        assert settings.max_heavy_executing == 4
        assert settings.max_heavy_queued == 16
        assert settings.max_cheap_concurrent == 32
        assert settings.db_pool_size == 5
        assert settings.db_max_overflow == 0
        assert settings.rate_limit_per_second == 20.0
        assert settings.rate_limit_burst == 40.0

    def test_epoch_pinning_is_off_by_default(self):
        """A client must not be able to select an old epoch out of the box."""
        assert Settings(environment="test").allow_pinned_epoch_requests is False

    def test_audit_is_mandatory_by_default(self):
        assert Settings(environment="test").audit_required is True

    def test_fault_injection_is_off_by_default(self):
        assert Settings(environment="test").faults_enabled is False

    def test_settings_are_frozen(self):
        settings = Settings(environment="test")
        with pytest.raises(ValidationError):
            settings.max_body_bytes = 1  # type: ignore[misc]

    def test_unknown_settings_are_refused(self):
        with pytest.raises(ValidationError):
            Settings(environment="test", definitely_not_a_setting=1)  # type: ignore[call-arg]


class TestProductionGuards:
    def test_development_jwt_keys_are_refused_outside_local(self):
        with pytest.raises(ValidationError, match="development JWT key"):
            Settings(environment="production", cursor_signing_key="a-real-production-key")

    def test_the_development_cursor_key_is_refused_outside_local(self):
        with pytest.raises(ValidationError, match="development cursor key"):
            Settings(environment="staging", jwt_keys={"prod-1": "a-real-production-secret"})

    def test_fault_injection_cannot_be_enabled_outside_local(self):
        with pytest.raises(ValidationError, match="fault injection"):
            Settings(
                environment="production",
                jwt_keys={"prod-1": "secret"},
                cursor_signing_key="another-real-key",
                faults_enabled=True,
            )

    def test_a_properly_configured_production_instance_starts(self):
        settings = Settings(
            environment="production",
            jwt_keys={"prod-1": "a-real-production-secret"},
            cursor_signing_key="a-real-production-cursor-key",
        )
        assert settings.environment == "production"
        assert settings.jwt_keys != DEV_JWT_KEYS
        assert settings.cursor_signing_key != DEV_CURSOR_KEY


class TestAlgorithmPolicy:
    @pytest.mark.parametrize("algorithm", ["none", "None", "", "RS256", "ES256", "PS256"])
    def test_unsigned_and_unsupported_algorithms_are_refused(self, algorithm: str):
        with pytest.raises(ValidationError):
            Settings(environment="test", jwt_allowed_algorithms=(algorithm,))

    def test_an_empty_algorithm_list_is_refused(self):
        with pytest.raises(ValidationError):
            Settings(environment="test", jwt_allowed_algorithms=())

    def test_hmac_algorithms_are_accepted(self):
        settings = Settings(environment="test", jwt_allowed_algorithms=("HS256", "HS512"))
        assert settings.jwt_allowed_algorithms == ("HS256", "HS512")


class TestCoherence:
    def test_sub_budgets_must_fit_the_deadline(self):
        with pytest.raises(ValidationError, match="sub-budgets"):
            Settings(
                environment="test",
                request_deadline_seconds=2.0,
                compute_budget_seconds=3.0,
            )

    def test_the_default_page_cannot_exceed_the_maximum(self):
        with pytest.raises(ValidationError, match="default page size"):
            Settings(environment="test", default_positions_page=500, max_positions_page=100)

    def test_burst_below_the_sustained_rate_is_refused(self):
        with pytest.raises(ValidationError, match="burst"):
            Settings(environment="test", rate_limit_per_second=20.0, rate_limit_burst=10.0)

    @pytest.mark.parametrize("path", ["api/v1", "/api/v1/", ""])
    def test_a_malformed_base_path_is_refused(self, path: str):
        with pytest.raises(ValidationError):
            Settings(environment="test", api_base_path=path)


class TestEnvironmentOverrides:
    def test_values_come_from_the_prefixed_environment(self, monkeypatch):
        monkeypatch.setenv("RISK_API_ENVIRONMENT", "test")
        monkeypatch.setenv("RISK_API_MAX_POSITIONS_PAGE", "25")
        monkeypatch.setenv("RISK_API_DEFAULT_POSITIONS_PAGE", "10")
        monkeypatch.setenv("RISK_API_STRICT_COMPOSITE_DEFAULT", "false")
        settings = Settings()
        assert settings.max_positions_page == 25
        assert settings.strict_composite_default is False


class TestCapabilityHandshake:
    def test_a_matching_provider_is_compatible(self):
        handshake = CapabilityHandshake(
            read_views=dict(READ_VIEW_CONTRACT_VERSIONS), provider_kind="test"
        )
        assert handshake.is_compatible()
        assert_read_view_capability(handshake)

    def test_a_missing_view_fails_at_startup(self):
        """Detecting a P03/P04 contract drift mid-request would be too late."""
        views = dict(READ_VIEW_CONTRACT_VERSIONS)
        views.pop("v_risk_reconciliation")
        handshake = CapabilityHandshake(read_views=views, provider_kind="test")
        assert handshake.missing() == ("v_risk_reconciliation",)
        with pytest.raises(RuntimeError, match="incompatible read-view contract"):
            assert_read_view_capability(handshake)

    def test_a_drifted_version_fails_at_startup(self):
        views = dict(READ_VIEW_CONTRACT_VERSIONS)
        views["v_risk_published_cut"] = "v2"
        handshake = CapabilityHandshake(read_views=views, provider_kind="test")
        assert handshake.mismatched() == ("v_risk_published_cut",)
        with pytest.raises(RuntimeError):
            assert_read_view_capability(handshake)


class TestPolicyVersionResolution:
    def resolve(self, **overrides):
        base = {
            "requested_risk_policy": None,
            "requested_symbol_catalog": None,
            "installed_risk_policy": "risk-policy-v1",
            "installed_symbol_catalog": "sim-usd-fx-v1",
            "allowed_risk_policies": ("risk-policy-v1",),
            "allowed_symbol_catalogs": ("sim-usd-fx-v1",),
        }
        base.update(overrides)
        return resolve_policy_versions(**base)  # type: ignore[arg-type]

    def test_omitted_versions_resolve_to_what_is_installed(self):
        assert self.resolve() == ("risk-policy-v1", "sim-usd-fx-v1")

    def test_the_installed_version_may_be_requested_explicitly(self):
        assert self.resolve(requested_risk_policy="risk-policy-v1")[0] == "risk-policy-v1"

    def test_an_uninstalled_policy_is_refused(self):
        """A version string must never select code to load."""
        with pytest.raises(ApiProblem) as caught:
            self.resolve(requested_risk_policy="risk-policy-v99")
        assert caught.value.code == "UNSUPPORTED_CALC_MODE"
        assert caught.value.status == 422

    def test_an_uninstalled_catalog_is_refused(self):
        with pytest.raises(ApiProblem):
            self.resolve(requested_symbol_catalog="attacker-catalog")

    def test_an_allowlisted_but_not_installed_version_is_still_refused(self):
        with pytest.raises(ApiProblem):
            self.resolve(
                requested_risk_policy="risk-policy-v2",
                allowed_risk_policies=("risk-policy-v1", "risk-policy-v2"),
            )
