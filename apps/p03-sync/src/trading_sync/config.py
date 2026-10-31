"""Typed configuration.

Mirrors the illustrative YAML in design 19.2, with the values that matter for
safety expressed as validated fields rather than conventions. The defaults are
the design's *provisional* numbers: they are starting points to be tuned after
benchmarking, not measured results (design 16.2).

Three validators here encode rules that are easy to break by accident and
expensive to discover in production:

* a lease heartbeat must be meaningfully shorter than the lease TTL, or the
  owner loses its lease while believing it still holds it (WF3-125);
* a scan's wall-clock budget must leave margin under the source's cursor TTL,
  or every large scan dies at its terminal page (WF3-047);
* an admin credential must never be configured into the data-plane source,
  because the sync worker has no business holding fault-injection authority
  (design 3.2 item 3, WF3-186).
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import SourceKind
from trading_sync.domain.validation import DEFAULT_MAX_PAGE_ITEMS
from trading_sync.errors import ConfigurationError

__all__ = [
    "LeaseConfig",
    "PostgresConfig",
    "SecurityConfig",
    "Settings",
    "SourceConfig",
    "SyncConfig",
    "TelemetryConfig",
    "load_settings",
]

PositiveInt = Annotated[int, Field(gt=0)]


class SourceConfig(BaseModel):
    """Identity and transport settings for one upstream source."""

    #: Configured, never taken from a request. A caller-supplied source id
    #: would let one tenant write into another's epoch (AS-01, WF3-183).
    source_id: str = "sim_local"
    kind: SourceKind = SourceKind.SIMULATOR
    connector_endpoint: str = "http://localhost:8080"
    api_token: SecretStr | None = None
    #: Admin/fault-injection credentials belong to the test harness, not here.
    admin_token: SecretStr | None = None
    require_run_id_and_generation: bool = True
    page_size: Annotated[int, Field(gt=0, le=DEFAULT_MAX_PAGE_ITEMS)] = 500
    deal_checkpoint_scope: str = FILTER_UNFILTERED
    #: ``None`` disables the allowlist and keeps only the format check.
    allowed_currencies: tuple[str, ...] | None = None
    request_timeout_seconds: Annotated[float, Field(gt=0)] = 15.0
    #: Refused while reading, so an oversized body never lands in memory.
    max_response_bytes: PositiveInt = 8 * 1024 * 1024
    #: The source's documented snapshot/cursor lifetime, used to size budgets.
    cursor_ttl_seconds: PositiveInt = 900

    @model_validator(mode="after")
    def _reject_admin_credentials(self) -> SourceConfig:
        if self.admin_token is not None:
            raise ValueError(
                "admin_token must not be configured on a data-plane source; "
                "fault injection belongs to the test harness"
            )
        return self


class SyncConfig(BaseModel):
    """Scan budgets and retry policy."""

    worker_concurrency: PositiveInt = 1
    scan_queue_capacity: PositiveInt = 8
    scan_max_duration_seconds: PositiveInt = 720
    max_pages_per_scan: PositiveInt = 2500
    #: Scan-level retries only. Transport retries live in the connector, and
    #: nesting the two multiplies attempts (design 9.2).
    scan_retry_attempts: Annotated[int, Field(ge=0, le=5)] = 2
    scan_retry_base_seconds: Annotated[float, Field(gt=0)] = 2.0
    scan_retry_max_seconds: Annotated[float, Field(gt=0)] = 60.0
    deal_batch_size: Annotated[int, Field(gt=0, le=DEFAULT_MAX_PAGE_ITEMS)] = 500
    stable_snapshot_required: bool = True
    #: Write a Project 07 ``event_outbox`` row inside the same transaction as
    #: each newly inserted Deal.
    #:
    #: False is the standalone posture: Project 03 works on its own, and
    #: ``event_outbox`` need not exist. It also means a committed Deal produces
    #: no event, so the Project 03 -> 07 -> Kafka -> 08 chain does not exist.
    #: That is a declared capability gap, not a silent one: readiness reports
    #: it, and nothing downstream may be described as fed by this service while
    #: it is False (contract gap CG-08).
    #:
    #: True requires ``outbox migrate`` to have created the outbox tables in
    #: this same database. Emission then fails closed - if an event cannot be
    #: built, the page transaction rolls back and the Deal is not committed
    #: either, so there is no state where a Deal exists without its event.
    emit_outbox_events: bool = False
    #: Fraction of the source cursor TTL a scan may consume (design 16.2 wants
    #: at least 20% margin).
    cursor_ttl_margin: Annotated[float, Field(gt=0, lt=1)] = 0.8


class PostgresConfig(BaseModel):
    """Sink connectivity and the timeouts that keep a stuck query bounded."""

    pool_size: PositiveInt = 5
    max_overflow: Annotated[int, Field(ge=0)] = 2
    pool_timeout_seconds: Annotated[float, Field(gt=0)] = 5.0
    statement_timeout_ms: PositiveInt = 15_000
    lock_timeout_ms: PositiveInt = 3_000
    idle_in_transaction_session_timeout_ms: PositiveInt = 30_000
    #: Only for the whitelisted SQLSTATEs 40001 and 40P01 (design 9.2).
    db_tx_retry_attempts: Annotated[int, Field(ge=0, le=5)] = 3
    echo_sql: bool = False

    @property
    def max_connections(self) -> int:
        return self.pool_size + self.max_overflow


class LeaseConfig(BaseModel):
    ttl_seconds: PositiveInt = 30
    heartbeat_seconds: PositiveInt = 10

    @model_validator(mode="after")
    def _heartbeat_fits_inside_ttl(self) -> LeaseConfig:
        if self.heartbeat_seconds * 2 > self.ttl_seconds:
            raise ValueError(
                "heartbeat_seconds must be at most half of ttl_seconds so that a "
                "single missed heartbeat does not forfeit the lease"
            )
        return self


class SecurityConfig(BaseModel):
    allow_admin_fault_api: bool = False
    #: Readers must name an authorized source; no unscoped scans (design 11.5).
    require_source_scope: bool = True


class TelemetryConfig(BaseModel):
    #: Raw source payloads are never logged in any environment by default.
    payload_logging: bool = False
    service_name: str = "trading-data-sync"
    log_level: str = "INFO"


class Settings(BaseSettings):
    """Process configuration, assembled from the environment.

    Nested fields use a double underscore, so the source id is
    ``SYNC_SOURCE__SOURCE_ID``. The database URL is read from
    ``SYNC_DATABASE_URL`` and held as a ``SecretStr`` so it cannot be printed
    into a log or an exception by accident (WF3-185).
    """

    model_config = SettingsConfigDict(
        env_prefix="SYNC_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    database_url: SecretStr = SecretStr("")
    #: Optional second DSN owning DDL. When unset, migrations use the app DSN,
    #: which is acceptable locally but not the production posture (design 11.1).
    migration_database_url: SecretStr | None = None

    source: SourceConfig = Field(default_factory=SourceConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    lease: LeaseConfig = Field(default_factory=LeaseConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)

    #: Test-only crash barriers (design 17.3). Enabling these in a deployed
    #: process would let a request-shaped input terminate a transaction.
    enable_killpoints: bool = False

    @model_validator(mode="after")
    def _scan_budget_leaves_cursor_margin(self) -> Settings:
        allowed = self.source.cursor_ttl_seconds * self.sync.cursor_ttl_margin
        if self.sync.scan_max_duration_seconds > allowed:
            raise ValueError(
                "scan_max_duration_seconds leaves no margin under the source "
                f"cursor TTL: {self.sync.scan_max_duration_seconds}s requested, "
                f"{allowed:.0f}s allowed by cursor_ttl_margin"
            )
        return self

    @model_validator(mode="after")
    def _page_size_fits_batch(self) -> Settings:
        if self.source.page_size > self.sync.deal_batch_size:
            raise ValueError(
                "source.page_size must not exceed sync.deal_batch_size; a page "
                "is committed in one transaction"
            )
        return self

    @property
    def sqlalchemy_url(self) -> str:
        url = self.database_url.get_secret_value()
        if not url:
            raise ConfigurationError(
                "SYNC_DATABASE_URL is not set", setting="database_url"
            )
        return url

    @property
    def migration_url(self) -> str:
        if self.migration_database_url is not None:
            secret = self.migration_database_url.get_secret_value()
            if secret:
                return secret
        return self.sqlalchemy_url

    def connect_args(self) -> dict[str, object]:
        """asyncpg server settings applied to every connection in the pool.

        Setting these per connection rather than per statement means a query
        that forgets to opt in is still bounded, which is the behaviour the
        resource-containment requirement depends on (design 9.3, WF3-116).
        """
        return {
            "server_settings": {
                "statement_timeout": str(self.postgres.statement_timeout_ms),
                "lock_timeout": str(self.postgres.lock_timeout_ms),
                "idle_in_transaction_session_timeout": str(
                    self.postgres.idle_in_transaction_session_timeout_ms
                ),
                "application_name": self.telemetry.service_name,
            }
        }


def load_settings(**overrides: object) -> Settings:
    """Build :class:`Settings`, converting validation failures into our taxonomy."""
    try:
        return Settings(**overrides)  # type: ignore[arg-type]
    except Exception as exc:  # pydantic ValidationError and friends
        raise ConfigurationError("invalid configuration", detail=str(exc)) from exc
