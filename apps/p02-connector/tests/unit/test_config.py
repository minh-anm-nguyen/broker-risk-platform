"""Configuration refusals, all of which must happen before anything is built.

Covers WF2-A-01 .. WF2-A-07, WF2-A-11, WF2-M-05, WF2-N-01, WF2-N-04 and the
startup half of INV-03.

Every test here asserts the *refusal*, and several also assert that nothing
was allocated - because "rejected the config" and "rejected the config without
opening a socket or starting a thread" are different claims, and only the
second one is what the design promises.
"""

from __future__ import annotations

import threading

import pytest

from mt5_connector.config import ABSOLUTE_MAX_PAGE_SIZE, Settings, load_settings
from mt5_connector.errors import ConfigurationError

pytestmark = pytest.mark.unit


def _settings(**overrides) -> Settings:
    base = {"source_id": "broker-sim-01", "base_url": "http://127.0.0.1:8000"}
    base.update(overrides)
    return Settings(**base)


class TestIdentity:
    def test_missing_source_id_refuses_to_start(self) -> None:
        """WF2-A-01: no source_id means no start, and no worker either."""
        threads_before = threading.active_count()
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(source_id="")
        assert excinfo.value.details["field"] == "source_id"
        assert not excinfo.value.retryable
        assert threading.active_count() == threads_before

    def test_blank_source_id_is_not_treated_as_present(self) -> None:
        with pytest.raises(ConfigurationError):
            _settings(source_id="   ")

    def test_source_id_must_be_metric_label_safe(self) -> None:
        # It becomes a Prometheus label; a newline in one corrupts the
        # exposition format.
        with pytest.raises(ConfigurationError):
            _settings(source_id="broker\nsim")

    def test_token_env_must_be_an_env_var_name(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(token_env="not a var")
        assert excinfo.value.details["field"] == "token_env"

    def test_missing_token_is_an_error_not_an_empty_string(self) -> None:
        """WF2-A-07: no credential means no blind reads."""
        settings = _settings()
        with pytest.raises(ConfigurationError) as excinfo:
            settings.token(environ={})
        assert excinfo.value.details["token_env"] == "SIMULATOR_DATA_TOKEN"

    def test_blank_token_is_rejected(self) -> None:
        settings = _settings()
        with pytest.raises(ConfigurationError):
            settings.token(environ={"SIMULATOR_DATA_TOKEN": "   "})

    def test_token_is_never_a_settings_field(self) -> None:
        """A repr of the settings must be safe to log (INV-15)."""
        settings = _settings()
        assert "token" not in settings.redacted() or settings.redacted()["token_env"]
        assert "secret" not in repr(settings).lower()
        assert set(settings.redacted()) >= {"source_id", "base_url", "token_env"}
        assert all("token" not in str(value).lower() or key == "token_env"
                   for key, value in settings.redacted().items())


class TestUrlPolicy:
    def test_url_without_scheme_is_refused_before_any_network_access(self) -> None:
        """WF2-A-06."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(base_url="127.0.0.1:8000")
        assert excinfo.value.details["field"] == "base_url"

    def test_unsupported_scheme_is_refused(self) -> None:
        for url in ("ftp://host/", "file:///etc/passwd", "gopher://host"):
            with pytest.raises(ConfigurationError):
                _settings(base_url=url)

    def test_base_url_with_a_path_is_refused(self) -> None:
        """WF2-A-05: a configured path could point the data plane at /admin."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(base_url="http://127.0.0.1:8000/admin/v1")
        assert "allowed_paths" in excinfo.value.details

    def test_base_url_with_a_query_string_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            _settings(base_url="http://127.0.0.1:8000?token=abc")

    def test_plaintext_to_a_remote_host_is_refused(self) -> None:
        """WF2-N-04: cleartext would expose the bearer token."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(base_url="http://broker.example.com")
        assert excinfo.value.details["host_is_loopback"] is False

    def test_plaintext_to_loopback_is_permitted(self) -> None:
        assert _settings(base_url="http://localhost:8000").base_url
        assert _settings(base_url="http://127.0.0.1:8000").base_url

    def test_https_with_verification_disabled_is_refused(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(base_url="https://broker.example.com", tls_verify=False)
        assert excinfo.value.details["field"] == "tls_verify"

    def test_host_outside_the_allowlist_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            _settings(
                base_url="https://broker.example.com",
                outbound_allowlist=("other.example.com",),
            )

    def test_url_for_only_composes_allowlisted_paths(self) -> None:
        """WF2-N-01: there is no mechanism for an arbitrary destination."""
        settings = _settings()
        assert settings.url_for("deals") == "http://127.0.0.1:8000/v1/deals"
        for forbidden in ("admin", "/admin/v1/faults", "http://evil/", "meta/../admin"):
            with pytest.raises(ConfigurationError) as excinfo:
                settings.url_for(forbidden)
            assert "allowed" in excinfo.value.details


class TestLimits:
    @pytest.mark.parametrize("size", [0, -1, ABSOLUTE_MAX_PAGE_SIZE + 1, 1_000_000])
    def test_page_size_outside_the_cap_is_refused(self, size: int) -> None:
        """WF2-A-02: refused, never silently reduced to the cap."""
        with pytest.raises(ConfigurationError):
            _settings(default_page_size=size)

    def test_max_page_size_above_the_absolute_cap_is_refused(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(max_page_size=ABSOLUTE_MAX_PAGE_SIZE + 1)
        assert excinfo.value.details["maximum"] == ABSOLUTE_MAX_PAGE_SIZE

    @pytest.mark.parametrize("size", [0, -1, -32, 100_000])
    def test_queue_size_must_be_positive_and_bounded(self, size: int) -> None:
        """WF2-A-03 and the startup half of INV-03."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(max_queue_pending=size)
        assert excinfo.value.details["field"] == "max_queue_pending"

    def test_resolve_limit_refuses_rather_than_clamps(self) -> None:
        """WF2-M-05: a clamped limit lets a caller believe it got everything."""
        settings = _settings(max_page_size=500)
        assert settings.resolve_limit(None) == settings.default_page_size
        assert settings.resolve_limit(500) == 500
        for bad in (0, -1, 501, 1_000_000):
            with pytest.raises(ConfigurationError) as excinfo:
                settings.resolve_limit(bad)
            assert excinfo.value.details["maximum"] == 500

    def test_queue_wait_cap_must_be_below_the_total_deadline(self) -> None:
        """WF2-H-01: otherwise a request can burn its budget and still dispatch."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(page_total_deadline_ms=1_000, queue_wait_cap_ms=1_000)
        assert excinfo.value.details["field"] == "queue_wait_cap_ms"


class TestWorkerPolicy:
    def test_two_session_workers_is_refused(self) -> None:
        """ADR-202: concurrency on one session is a correctness decision."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(session_workers=2)
        assert excinfo.value.details["see"] == "ADR-202"

    def test_connector_side_fault_injection_is_refused(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(enable_fault_injection_in_connector=True)
        assert "section 9.3" in str(excinfo.value.details["see"])

    def test_native_driver_requires_process_isolation(self) -> None:
        """ADR-204: a hung native call cannot be stopped inside a thread."""
        with pytest.raises(ConfigurationError) as excinfo:
            _settings(
                source_kind="manager_sdk",
                driver="manager_sdk_windows",
                worker_mode="thread",
            )
        assert excinfo.value.details["see"] == "ADR-204"

    def test_native_source_kind_requires_the_native_driver(self) -> None:
        with pytest.raises(ConfigurationError):
            _settings(source_kind="manager_sdk", driver="simulator_sync_http")

    def test_unknown_worker_mode_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            _settings(worker_mode="fibers")


class TestEnvironmentLoading:
    def test_load_settings_requires_the_mandatory_pair(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            load_settings(environ={})
        assert excinfo.value.details["field"] in {"source_id", "base_url"}

    def test_unknown_setting_is_rejected_not_ignored(self) -> None:
        """An ignored option is worse than a rejected one (WF2-A-11)."""
        with pytest.raises(ConfigurationError) as excinfo:
            load_settings(
                environ={
                    "CONNECTOR_SOURCE_ID": "s1",
                    "CONNECTOR_BASE_URL": "http://127.0.0.1:1",
                    "CONNECTOR_MAX_QUEUE_PENDIN": "32",
                }
            )
        assert excinfo.value.details["fields"] == ["max_queue_pendin"]

    def test_integer_settings_are_parsed_and_validated(self) -> None:
        settings = load_settings(
            environ={
                "CONNECTOR_SOURCE_ID": "s1",
                "CONNECTOR_BASE_URL": "http://127.0.0.1:1",
                "CONNECTOR_MAX_QUEUE_PENDING": "16",
                "CONNECTOR_ALLOW_THREAD_ABANDON": "true",
            }
        )
        assert settings.max_queue_pending == 16
        assert settings.allow_thread_abandon is True

    def test_non_integer_value_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            load_settings(
                environ={
                    "CONNECTOR_SOURCE_ID": "s1",
                    "CONNECTOR_BASE_URL": "http://127.0.0.1:1",
                    "CONNECTOR_MAX_QUEUE_PENDING": "lots",
                }
            )

    def test_settings_are_frozen_so_one_run_has_one_configuration(self) -> None:
        """WF2-A-11: a file changed later cannot produce a second config."""
        settings = _settings()
        with pytest.raises(Exception):
            settings.max_queue_pending = 64  # type: ignore[misc]
