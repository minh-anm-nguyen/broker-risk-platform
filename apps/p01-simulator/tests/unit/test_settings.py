"""Configuration must fail closed.

Every case here is a deployment mistake that would otherwise be discovered in
production: an unauthenticated control plane on a public interface, several
workers racing over one sequence counter, a cursor signing key regenerated on
every restart.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError as PydanticValidationError

from simulator.settings import (
    ConfigurationError,
    Settings,
    assert_single_worker_configuration,
)

pytestmark = pytest.mark.unit


def base(tmp_path: Path, **overrides) -> dict:
    return {"db_path": str(tmp_path / "sim.db"), **overrides}


def test_loopback_defaults_are_usable_without_secrets(tmp_path: Path) -> None:
    """A local test double that refused to boot would be useless."""
    settings = Settings(**base(tmp_path))
    assert settings.is_loopback_only
    assert settings.effective_data_token
    assert settings.effective_admin_token
    assert settings.effective_data_token != settings.effective_admin_token


@pytest.mark.parametrize("host", ["0.0.0.0", "192.168.1.10", "::"])
def test_non_loopback_bind_requires_every_secret(tmp_path: Path, host: str) -> None:
    """WF-108, WF-122: refuse to expose an unauthenticated control plane."""
    with pytest.raises(PydanticValidationError) as excinfo:
        Settings(**base(tmp_path, host=host))
    message = str(excinfo.value)
    assert "SIM_DATA_TOKEN" in message
    assert "SIM_ADMIN_TOKEN" in message
    assert "SIM_CURSOR_SIGNING_KEY" in message


def test_non_loopback_bind_with_secrets_is_accepted(tmp_path: Path) -> None:
    settings = Settings(
        **base(
            tmp_path,
            host="0.0.0.0",
            data_token="d" * 32,
            admin_token="a" * 32,
            cursor_signing_key="c" * 32,
        )
    )
    assert not settings.is_loopback_only


def test_identical_tokens_break_plane_separation(tmp_path: Path) -> None:
    """Sharing one token would make the data plane an admin plane."""
    with pytest.raises(PydanticValidationError) as excinfo:
        Settings(
            **base(
                tmp_path,
                host="0.0.0.0",
                data_token="same-token",
                admin_token="same-token",
                cursor_signing_key="c" * 32,
            )
        )
    assert "must differ" in str(excinfo.value)


@pytest.mark.parametrize("variable", ["WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS"])
def test_multiple_workers_are_refused(variable: str) -> None:
    """WF-120: two writers would break seq contiguity without corrupting a file.

    That is the dangerous version of the bug, because the data would look fine.
    """
    with pytest.raises(ConfigurationError) as excinfo:
        assert_single_worker_configuration({variable: "4"})
    assert "single writer" in str(excinfo.value)


@pytest.mark.parametrize("value", ["1", "", None])
def test_single_or_unset_worker_count_is_accepted(value: str | None) -> None:
    environment = {} if value is None else {"WEB_CONCURRENCY": value}
    assert_single_worker_configuration(environment)


def test_non_integer_worker_count_is_refused() -> None:
    with pytest.raises(ConfigurationError):
        assert_single_worker_configuration({"WEB_CONCURRENCY": "many"})


def test_cursor_key_is_stable_across_instances(tmp_path: Path) -> None:
    """Section 4.2: a key regenerated per restart silently voids every cursor."""
    first = Settings(**base(tmp_path)).effective_cursor_key
    second = Settings(**base(tmp_path)).effective_cursor_key
    assert first == second
    # And it is tied to the dataset, not a shared constant.
    other = Settings(db_path=str(tmp_path / "other.db")).effective_cursor_key
    assert other != first


def test_signing_keys_expose_the_rotation_overlap(tmp_path: Path) -> None:
    settings = Settings(
        **base(
            tmp_path,
            cursor_signing_key="new",
            cursor_signing_key_id="k2",
            cursor_signing_key_previous="old",
            cursor_signing_key_previous_id="k1",
        )
    )
    keys = settings.signing_keys()
    assert [key_id for key_id, _ in keys] == ["k2", "k1"]
    # The first entry is the signing key; later entries verify only.
    assert keys[0][1] == "new"


def test_a_previous_key_without_its_id_is_ignored(tmp_path: Path) -> None:
    """Half-configured rotation must not silently sign with an unnamed key."""
    settings = Settings(**base(tmp_path, cursor_signing_key_previous="old"))
    assert len(settings.signing_keys()) == 1


def test_page_size_defaults_must_not_exceed_the_maximum(tmp_path: Path) -> None:
    with pytest.raises(PydanticValidationError):
        Settings(**base(tmp_path, default_page_size=200, max_page_size=100))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_page_size", 0),
        ("max_pending_events", 0),
        ("snapshot_ttl_sec", 0),
        ("max_data_requests_in_flight", 0),
        ("rate_limit_rps", 0),
        ("virtual_time_step_ms", 0),
        ("writer_batch_size", 0),
        # Two writers are not a configuration option.
        ("max_active_generators", 2),
    ],
)
def test_every_bound_rejects_a_nonsensical_value(tmp_path: Path, field: str, value: object) -> None:
    with pytest.raises(PydanticValidationError):
        Settings(**base(tmp_path, **{field: value}))


def test_there_is_no_unlimited_mode(tmp_path: Path) -> None:
    """Section 9.3: every resource knob has a finite ceiling."""
    settings = Settings(**base(tmp_path))
    for field in (
        "max_page_size",
        "max_request_body_bytes",
        "max_pending_events",
        "max_data_requests_in_flight",
        "max_active_snapshots",
        "snapshot_ttl_sec",
        "max_fault_delay_ms",
        "max_fault_rules",
        "max_deals_per_run",
        "max_advance_events",
        "max_json_depth",
        "request_deadline_ms",
    ):
        assert getattr(settings, field) > 0, field


def test_lock_path_sits_beside_the_database(tmp_path: Path) -> None:
    settings = Settings(**base(tmp_path))
    assert settings.lock_path.parent == settings.db_path.parent
    assert settings.lock_path != settings.db_path


def test_settings_are_immutable(tmp_path: Path) -> None:
    """Configuration that can change at runtime is configuration you cannot trust."""
    settings = Settings(**base(tmp_path))
    with pytest.raises(PydanticValidationError):
        settings.max_page_size = 10_000  # type: ignore[misc]


def test_unknown_failpoint_names_are_refused() -> None:
    """WF-119: a typo would create a test that passes with nothing injected."""
    from simulator.persistence.failpoints import controller_from_settings

    with pytest.raises(ValueError):
        controller_from_settings("just_before_commmit", 0)  # note the typo
    assert not controller_from_settings("", 0).enabled
    assert controller_from_settings("just_before_commit", 5).enabled
