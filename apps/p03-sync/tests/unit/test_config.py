"""Configuration guards that prevent unsafe-by-construction deployments."""

from __future__ import annotations

import pytest

from trading_sync.config import load_settings
from trading_sync.errors import ConfigurationError

pytestmark = pytest.mark.unit


def test_defaults_match_the_design_budgets() -> None:
    settings = load_settings(database_url="postgresql+asyncpg://u@h/db")
    assert settings.sync.scan_max_duration_seconds == 720
    assert settings.sync.max_pages_per_scan == 2500
    assert settings.source.page_size == 500
    assert settings.postgres.max_connections == 7


@pytest.mark.whatif("WF3-186")
def test_admin_token_on_a_data_plane_source_is_refused() -> None:
    """The sync worker has no business holding fault-injection authority.

    Fault injection belongs to the test harness on a separate path; a worker
    that could reach the admin plane is a privilege-escalation path from a
    data-plane compromise (design 3.2 item 3).
    """
    with pytest.raises(ConfigurationError):
        load_settings(
            database_url="postgresql+asyncpg://u@h/db",
            source={"admin_token": "super-secret"},
        )


@pytest.mark.whatif("WF3-047")
def test_scan_budget_must_leave_margin_under_the_cursor_ttl() -> None:
    """A scan allowed to run past the source's cursor TTL dies at its terminal
    page every time, wasting the whole attempt (design 16.2)."""
    with pytest.raises(ConfigurationError):
        load_settings(
            database_url="postgresql+asyncpg://u@h/db",
            source={"cursor_ttl_seconds": 900},
            sync={"scan_max_duration_seconds": 880},
        )


@pytest.mark.whatif("WF3-125")
def test_heartbeat_must_fit_inside_the_lease_ttl() -> None:
    """One missed heartbeat must not forfeit the lease."""
    with pytest.raises(ConfigurationError):
        load_settings(
            database_url="postgresql+asyncpg://u@h/db",
            lease={"ttl_seconds": 30, "heartbeat_seconds": 20},
        )


@pytest.mark.whatif("WF3-042")
def test_page_size_above_the_contract_ceiling_is_refused() -> None:
    with pytest.raises(ConfigurationError):
        load_settings(
            database_url="postgresql+asyncpg://u@h/db", source={"page_size": 501}
        )


@pytest.mark.whatif("WF3-185")
def test_database_url_is_held_as_a_secret() -> None:
    """So an accidental repr or log of the settings object cannot leak it."""
    settings = load_settings(database_url="postgresql+asyncpg://u:pw@h/db")
    assert "pw" not in repr(settings)
    assert settings.sqlalchemy_url == "postgresql+asyncpg://u:pw@h/db"


def test_missing_database_url_fails_loudly_when_used() -> None:
    with pytest.raises(ConfigurationError):
        load_settings().sqlalchemy_url


def test_session_timeouts_are_applied_per_connection() -> None:
    """Per connection rather than per statement, so a query that forgets to
    opt in is still bounded (design 9.3)."""
    settings = load_settings(database_url="postgresql+asyncpg://u@h/db")
    server_settings = settings.connect_args()["server_settings"]
    assert server_settings["statement_timeout"] == "15000"
    assert server_settings["lock_timeout"] == "3000"
    assert server_settings["idle_in_transaction_session_timeout"] == "30000"
