"""Configuration, validated at startup rather than discovered at 3am.

Every bound in design section 16.2 -- claim batch, in-flight cap, wire size,
pool size, storage budget -- is a *configuration proposal subject to
benchmark*, not a measured result. They live here so a benchmark can change one
number without touching the relay, and so a combination that cannot possibly
hold (in-flight larger than the producer buffer can carry, a lease shorter than
the delivery deadline) is refused at process start.

The lease-versus-deadline check is the one worth reading twice: if a lease can
expire while a produce request is still outstanding, a standby will take the row
over and publish a second copy *by design*. That is legal at-least-once
behaviour, but it should be a deliberate operational choice, not an accident of
two defaults that were never compared.
"""

from __future__ import annotations

from pydantic import Field, ValidationError as PydanticValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .canonical import DEFAULT_MAX_WIRE_BYTES
from .errors import ConfigurationError


class Settings(BaseSettings):
    """Relay, store and consumer configuration, immutable once loaded."""

    model_config = SettingsConfigDict(
        env_prefix="OUTBOX_",
        env_file=".env",
        extra="ignore",
        frozen=True,
    )

    # --- store ---------------------------------------------------------------
    store_backend: str = "sqlite"
    sqlite_path: str = "./outbox.sqlite3"
    pg_dsn: str | None = None
    relay_pg_pool_max: int = Field(default=8, ge=1, le=64)
    storage_budget_bytes: int = Field(default=1 << 30, ge=1 << 20)

    # --- tenancy -------------------------------------------------------------
    source_allowlist: tuple[str, ...] = ("sim-broker-01",)

    # --- relay ---------------------------------------------------------------
    relay_owner: str = "relay-1"
    claim_batch_size: int = Field(default=50, ge=1, le=1000)
    lease_seconds: float = Field(default=30.0, gt=0, le=3600)
    max_inflight: int = Field(default=64, ge=1, le=4096)
    delivery_deadline_seconds: float = Field(default=10.0, gt=0, le=600)
    max_event_wire_bytes: int = Field(default=DEFAULT_MAX_WIRE_BYTES, ge=1024, le=8 << 20)
    max_attempts: int = Field(default=12, ge=1, le=1000)
    backoff_base_seconds: float = Field(default=0.5, gt=0, le=60)
    backoff_max_seconds: float = Field(default=60.0, gt=0, le=3600)
    backoff_jitter_ratio: float = Field(default=0.25, ge=0.0, le=1.0)
    circuit_failure_threshold: int = Field(default=5, ge=1, le=1000)
    circuit_cooldown_seconds: float = Field(default=15.0, gt=0, le=3600)
    producer_buffer_max_bytes: int = Field(default=8 << 20, ge=64 << 10)
    poll_idle_sleep_seconds: float = Field(default=0.05, ge=0.0, le=10.0)

    # --- consumer ------------------------------------------------------------
    consumer_name: str = "reference-pg-sink"
    consumer_group: str = "reference-pg-sink"
    consumer_max_poll_records: int = Field(default=100, ge=1, le=10_000)
    supported_schema_versions: tuple[int, ...] = (1,)

    # --- storage pressure policy (section 16.4) ------------------------------
    # Illustrative policy candidates, not validated thresholds.
    storage_warn_ratio: float = Field(default=0.80, gt=0, lt=1)
    storage_throttle_ratio: float = Field(default=0.90, gt=0, lt=1)
    storage_fail_closed_ratio: float = Field(default=0.95, gt=0, le=1)

    @field_validator("source_allowlist", "supported_schema_versions", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        """Accept ``a,b,c`` from the environment as well as a real sequence."""
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split(",") if part.strip())
        return value

    @field_validator("store_backend")
    @classmethod
    def _known_backend(cls, value: str) -> str:
        if value not in ("sqlite", "postgres"):
            raise ValueError(f"store_backend {value!r} must be 'sqlite' or 'postgres'")
        return value

    @field_validator("source_allowlist")
    @classmethod
    def _non_empty_allowlist(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        # An empty allowlist would route nothing, which looks like an outage
        # rather than like a misconfiguration. Refuse it at startup instead.
        if not value:
            raise ValueError("source_allowlist must name at least one source_id")
        if len(set(value)) != len(value):
            raise ValueError("source_allowlist contains duplicates")
        return value

    @model_validator(mode="after")
    def _coherent(self) -> Settings:
        if self.store_backend == "postgres" and not self.pg_dsn:
            raise ValueError("store_backend=postgres requires pg_dsn")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds must be >= backoff_base_seconds")
        if not (self.storage_warn_ratio < self.storage_throttle_ratio < self.storage_fail_closed_ratio):
            raise ValueError("storage ratios must increase: warn < throttle < fail_closed")
        if self.lease_seconds <= self.delivery_deadline_seconds:
            raise ValueError(
                "lease_seconds must exceed delivery_deadline_seconds, otherwise a standby "
                "reclaims rows whose produce request is still outstanding and duplicates "
                "become the normal case rather than a crash-recovery case"
            )
        # One claimed row may sit in the producer buffer at up to the wire limit.
        if self.max_inflight * self.max_event_wire_bytes > self.producer_buffer_max_bytes:
            raise ValueError(
                f"max_inflight*max_event_wire_bytes = "
                f"{self.max_inflight * self.max_event_wire_bytes} exceeds "
                f"producer_buffer_max_bytes = {self.producer_buffer_max_bytes}; "
                "the in-flight cap would be unreachable and backpressure would come "
                "from the buffer instead of from the cap"
            )
        if self.claim_batch_size > self.max_inflight:
            raise ValueError("claim_batch_size must not exceed max_inflight")
        return self

    @property
    def effective_max_payload_bytes(self) -> int:
        """Room left for the payload after envelope overhead is accounted for.

        Rough but conservative: the envelope's fixed fields and hex digests cost
        well under 1 KiB, and reserving a round kilobyte means a payload that
        validates here cannot fail the wire check after wrapping.
        """
        return max(1024, self.max_event_wire_bytes - 1024)


def load_settings(**overrides: object) -> Settings:
    """Build settings, converting pydantic's error into our own type.

    Callers (CLI, tests, the relay entry point) should see
    :class:`ConfigurationError` regardless of which validator tripped, so that
    "bad configuration" is one thing to catch and report.
    """
    try:
        return Settings(**overrides)  # type: ignore[arg-type]
    except PydanticValidationError as exc:
        details = "; ".join(
            f"{'.'.join(str(part) for part in error['loc']) or '<model>'}: {error['msg']}"
            for error in exc.errors()
        )
        raise ConfigurationError(f"invalid configuration: {details}") from exc
