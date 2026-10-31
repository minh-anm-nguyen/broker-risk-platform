"""Typed settings with fail-fast validation.

Design rules enforced here (SYSTEM_DESIGN sections 1.4, 4.1, 9.3, 12):

* The MVP supports exactly one writer process and one active run. A
  multi-worker deployment must fail at startup rather than silently corrupt
  ``seq`` allocation (WF-120).
* A non-loopback bind requires real, distinct tokens; otherwise startup fails
  closed instead of exposing an unauthenticated control plane (WF-108, WF-122).
* The cursor signing key must be supplied and durable. Regenerating it on each
  restart would silently invalidate every outstanding cursor (section 4.2).
* Every resource knob has an explicit bound. There is no "unlimited" mode.
"""

from __future__ import annotations

import ipaddress
import os
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationError(RuntimeError):
    """Raised when configuration is unsafe or internally inconsistent."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SIM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # ---- Deployment ------------------------------------------------------
    env: Literal["dev", "test", "docker", "prod"] = "dev"
    host: str = "127.0.0.1"
    port: int = Field(default=8001, ge=1, le=65535)

    # ---- Storage ---------------------------------------------------------
    db_path: Path = Field(default=Path("./data/simulator.db"))
    sqlite_busy_timeout_ms: int = Field(default=5_000, ge=100, le=60_000)
    sqlite_synchronous: Literal["OFF", "NORMAL", "FULL"] = "FULL"
    read_pool_size: int = Field(default=8, ge=1, le=64)
    integrity_check_on_start: bool = False

    # ---- Auth ------------------------------------------------------------
    data_token: str = ""
    admin_token: str = ""
    cursor_signing_key: str = ""
    cursor_signing_key_id: str = "k1"
    # A previous key stays valid for verification during rotation so the
    # overlap window can cover at least one snapshot TTL (section 4.2).
    cursor_signing_key_previous: str = ""
    cursor_signing_key_previous_id: str = ""

    # ---- Bounded resources (section 9.3) ---------------------------------
    default_page_size: int = Field(default=100, ge=1, le=500)
    max_page_size: int = Field(default=500, ge=1, le=5_000)
    max_request_body_bytes: int = Field(default=256 * 1024, ge=1024, le=8 * 1024 * 1024)
    max_pending_events: int = Field(default=10_000, ge=1, le=1_000_000)
    max_active_generators: int = Field(default=1, ge=1, le=1)
    max_data_requests_in_flight: int = Field(default=128, ge=1, le=4_096)
    max_active_snapshots: int = Field(default=100, ge=1, le=10_000)
    snapshot_ttl_sec: int = Field(default=900, ge=1, le=86_400)
    max_fault_delay_ms: int = Field(default=30_000, ge=0, le=600_000)
    max_fault_rules: int = Field(default=100, ge=0, le=10_000)
    max_deals_per_run: int = Field(default=1_000_000, ge=0, le=100_000_000)
    max_accounts_per_run: int = Field(default=100_000, ge=0, le=10_000_000)
    max_open_positions_per_run: int = Field(default=100_000, ge=0, le=10_000_000)
    max_advance_events: int = Field(default=100_000, ge=1, le=10_000_000)
    max_json_depth: int = Field(default=32, ge=2, le=256)
    writer_batch_size: int = Field(default=500, ge=1, le=10_000)
    # How long admission may wait for an in-flight slot before rejecting.
    # Queueing without a deadline is how a service dies quietly.
    admission_timeout_ms: int = Field(default=250, ge=0, le=30_000)
    request_deadline_ms: int = Field(default=30_000, ge=100, le=600_000)

    # ---- Rate limiting ---------------------------------------------------
    rate_limit_enabled: bool = True
    rate_limit_rps: float = Field(default=500.0, gt=0, le=1_000_000)
    rate_limit_burst: int = Field(default=1_000, ge=1, le=1_000_000)

    # ---- Clock / determinism --------------------------------------------
    virtual_time_step_ms: int = Field(default=1_000, ge=1, le=86_400_000)
    # A mark older than this (in *virtual* time) is reported with stale=true.
    # The simulator flags staleness rather than hiding it, so a consumer can
    # refuse to present a stale valuation as current (WF-030).
    mark_stale_after_ms: int = Field(default=60_000, ge=0, le=86_400_000)
    # Tolerated wall-clock skew for a cursor that outlived the process which
    # issued it and therefore has no in-process monotonic deadline (WF-086).
    cursor_clock_skew_tolerance_sec: int = Field(default=60, ge=0, le=3_600)

    # ---- Observability ---------------------------------------------------
    metrics_enabled: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "text"] = "json"

    # ---- Test-only hooks -------------------------------------------------
    # Deterministic crash points for IT-03 / WF-076 / WF-077. The process
    # terminates hard when a failpoint fires, so never enable this in a
    # deployment anything else depends on.
    failpoint: str = ""
    failpoint_at_seq: int = 0
    #: ``kill`` emulates SIGKILL (the only honest way to test WAL recovery);
    #: ``raise`` fails the transaction instead, for storage-error tests.
    failpoint_mode: Literal["kill", "raise"] = "kill"
    # Lets the test suite run many isolated app instances inside one pytest
    # process without fighting over the single-writer lock file.
    enforce_single_process_lock: bool = True

    # ---- Validators ------------------------------------------------------
    @field_validator("db_path")
    @classmethod
    def _expand_db_path(cls, value: Path) -> Path:
        return Path(os.path.expandvars(str(value))).expanduser()

    @model_validator(mode="after")
    def _validate_consistency(self) -> "Settings":
        if self.default_page_size > self.max_page_size:
            raise ValueError("default_page_size must not exceed max_page_size")

        if self.is_loopback_only:
            # A loopback-only dev/test run may derive secrets locally: nothing
            # is reachable off-host, and refusing to boot would make the
            # simulator useless as a local test double.
            return self

        missing = [
            name
            for name, value in (
                ("SIM_DATA_TOKEN", self.data_token),
                ("SIM_ADMIN_TOKEN", self.admin_token),
                ("SIM_CURSOR_SIGNING_KEY", self.cursor_signing_key),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Non-loopback bind requires explicit secrets; missing: "
                + ", ".join(missing)
                + ". Refusing to expose an unauthenticated control plane."
            )
        if self.data_token == self.admin_token:
            raise ValueError("data_token and admin_token must differ: plane separation is mandatory")
        return self

    # ---- Derived ---------------------------------------------------------
    @property
    def is_loopback_only(self) -> bool:
        if self.host in {"localhost", ""}:
            return True
        try:
            return ipaddress.ip_address(self.host).is_loopback
        except ValueError:
            return False

    @property
    def lock_path(self) -> Path:
        return self.db_path.with_suffix(self.db_path.suffix + ".writer.lock")

    @property
    def effective_cursor_key(self) -> str:
        if self.cursor_signing_key:
            return self.cursor_signing_key
        if not self.is_loopback_only:  # pragma: no cover - blocked by the validator
            raise ConfigurationError("cursor_signing_key is required for non-loopback deployments")
        # Derived from the absolute database path so it stays stable across
        # restarts of the same local dataset without being a shared constant.
        return "dev-insecure-cursor-key::" + str(self.db_path.resolve())

    @property
    def effective_data_token(self) -> str:
        return self.data_token or "dev-data-token"

    @property
    def effective_admin_token(self) -> str:
        return self.admin_token or "dev-admin-token"

    def signing_keys(self) -> list[tuple[str, str]]:
        """Return ``(key_id, key)`` pairs accepted for cursor verification.

        The first entry is the active signing key; later entries verify only.
        """
        keys = [(self.cursor_signing_key_id, self.effective_cursor_key)]
        if self.cursor_signing_key_previous and self.cursor_signing_key_previous_id:
            keys.append((self.cursor_signing_key_previous_id, self.cursor_signing_key_previous))
        return keys


_WORKER_ENV_VARS = ("WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS")


def assert_single_worker_configuration(environ: dict[str, str] | None = None) -> None:
    """Fail fast when the process is configured for multiple workers (WF-120).

    SQLite ``seq`` allocation and the single-writer queue are only correct with
    one writer process. Hoping SQLite's own locking is "probably enough" is not
    a design, it is a silent corruption risk.
    """
    env = os.environ if environ is None else environ
    for name in _WORKER_ENV_VARS:
        raw = env.get(name)
        if not raw:
            continue
        try:
            workers = int(raw)
        except ValueError as exc:
            raise ConfigurationError(f"{name}={raw!r} is not an integer") from exc
        if workers > 1:
            raise ConfigurationError(
                f"{name}={workers} is unsupported: Project 01 requires a single writer "
                "process. Run one worker, or use separate databases."
            )
