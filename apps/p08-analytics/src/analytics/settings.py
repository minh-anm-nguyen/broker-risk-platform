"""Typed settings with fail-fast validation.

Rules the design makes non-negotiable, enforced at boot rather than hoped for:

* **No unbounded knob.** Every queue, batch, retry, connection and timeout has
  a declared ceiling (FR08-08, INV08-20). "Unlimited" is not a value.
* **``auto_offset_reset`` must be ``none``.** Section 16.2. Resetting to
  ``latest`` after a retention loss converts missing money into a clean
  dashboard, which is the exact failure INV08-13 exists to prevent.
* **Tenant mapping is configuration.** An empty mapping is fatal, because the
  fallback would be a default tenant, and a default tenant is a cross-tenant
  leak with extra steps (INV08-17).
* **Synchronous insert ACK is required.** Async insert durability is
  version-dependent (section 6.1 step 4); enabling it without pinning a tested
  server version is refused.
* **A pinned ClickHouse version.** Section 13.1. The placeholder from the
  design is rejected explicitly so a deployment cannot inherit it.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .errors import ConfigurationError

#: The literal placeholder in the design's config sample. Carrying it into a
#: running deployment would mean nobody chose a version.
VERSION_PLACEHOLDER = "SET_ME_TO_TESTED_VERSION"


def _split_csv(value: object) -> object:
    """Accept a comma-separated string or a sequence, in either shape.

    An environment variable arrives as one string, while code constructing
    ``Settings`` directly tends to pass a tuple. Splitting only the string
    form left a tuple element like ``("a=1,b=2",)`` silently intact, so the
    second entry became part of the first one's value - a misconfiguration
    that would hand a whole source to a tenant whose name contained a comma.
    Normalising both shapes removes the ambiguity.
    """
    if isinstance(value, str):
        items: list[str] = value.split(",")
    elif isinstance(value, (list, tuple)):
        items = []
        for entry in value:
            items.extend(str(entry).split(","))
    else:
        return value
    return tuple(item.strip() for item in items if item.strip())


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="P08_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # ---- Deployment ------------------------------------------------------
    env: Literal["dev", "test", "docker", "prod"] = "dev"
    host: str = "127.0.0.1"
    port: int = Field(default=8008, ge=1, le=65535)

    # ---- Kafka (section 16.2) -------------------------------------------
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic_allowlist: tuple[str, ...] = ("broker.deals.v1",)
    kafka_group_id: str = "broker-analytics-v1"
    kafka_auto_offset_reset: Literal["none"] = "none"
    kafka_auto_offset_commit: Literal[False] = False
    kafka_max_batch_records: int = Field(default=2_000, ge=1, le=50_000)
    kafka_max_inflight_batches_per_partition: int = Field(default=1, ge=1, le=1)
    kafka_poll_timeout_ms: int = Field(default=1_000, ge=10, le=60_000)
    kafka_max_envelope_bytes: int = Field(default=256 * 1024, ge=1_024, le=8 * 1024 * 1024)

    # ---- ClickHouse ------------------------------------------------------
    ch_url: str = "http://localhost:8123"
    ch_database: str = "broker_analytics"
    ch_synchronous_insert_ack_required: bool = True
    ch_server_version_pin: str = VERSION_PLACEHOLDER
    ch_max_write_connections: int = Field(default=4, ge=1, le=64)
    ch_max_read_queries: int = Field(default=16, ge=1, le=256)
    ch_max_query_duration_ms: int = Field(default=5_000, ge=100, le=600_000)
    ch_insert_deadline_ms: int = Field(default=5_000, ge=100, le=600_000)
    ch_fact_batch_rows: int = Field(default=50_000, ge=1, le=1_000_000)
    ch_disk_capacity_rows: int = Field(default=5_000_000, ge=1_000)

    # ---- Control plane ---------------------------------------------------
    control_dsn: str = "sqlite:///./data/p08_control.db"
    control_statement_timeout_ms: int = Field(default=5_000, ge=100, le=120_000)

    # ---- Builds ----------------------------------------------------------
    build_max_concurrent_per_source_epoch: int = Field(default=1, ge=1, le=1)
    build_source_manifest_required: bool = True
    build_reconciliation_policy: Literal["REQUIRE_COMPATIBLE_CUT", "REQUIRE_VERIFIED"] = (
        "REQUIRE_COMPATIBLE_CUT"
    )
    build_fail_on_money_currency_unknown: bool = True
    build_fail_on_semantic_hash_conflict: bool = True
    build_lease_seconds: int = Field(default=300, ge=5, le=86_400)
    build_max_deals: int = Field(default=2_000_000, ge=1)
    publication_strategy: Literal["PG_FENCED_ATOMIC_POINTER"] = "PG_FENCED_ATOMIC_POINTER"

    # ---- Query service ---------------------------------------------------
    query_default_page_size: int = Field(default=100, ge=1, le=1_000)
    query_max_page_size: int = Field(default=1_000, ge=1, le=10_000)
    query_max_day_range: int = Field(default=366, ge=1, le=3_660)
    query_max_in_flight: int = Field(default=64, ge=1, le=4_096)
    query_freshness_target_seconds: int = Field(default=300, ge=1, le=86_400)
    cursor_signing_key: str = ""
    cursor_signing_key_id: str = "k1"

    # ---- Auth ------------------------------------------------------------
    reader_token: str = ""
    operator_token: str = ""
    #: ``topic:source_id=tenant_id`` entries. Section 6.1 step 1.
    tenant_map: tuple[str, ...] = ()
    require_audit_for_reads: bool = False

    # ---- Observability ---------------------------------------------------
    metrics_redact_pii: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    artifacts_dir: str = "./artifacts"

    @field_validator("kafka_topic_allowlist", "tenant_map", mode="before")
    @classmethod
    def _normalise_csv(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("kafka_topic_allowlist")
    @classmethod
    def _require_topics(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("kafka_topic_allowlist must not be empty")
        return value

    @model_validator(mode="after")
    def _validate_consistency(self) -> "Settings":
        if self.query_max_page_size < self.query_default_page_size:
            raise ConfigurationError(
                "query_max_page_size must be at least query_default_page_size",
                default=self.query_default_page_size,
                maximum=self.query_max_page_size,
            )
        if not self.ch_synchronous_insert_ack_required:
            # Section 6.1 step 4: async insert durability and dedup semantics
            # are version- and settings-dependent, and must be proven
            # separately before this can be turned off.
            raise ConfigurationError(
                "ch_synchronous_insert_ack_required must stay true until async insert "
                "durability is tested against a pinned server version"
            )
        if self.env in ("docker", "prod"):
            if self.ch_server_version_pin == VERSION_PLACEHOLDER:
                raise ConfigurationError(
                    "ch_server_version_pin must name the tested ClickHouse version",
                    env=self.env,
                )
            if not self.tenant_map:
                raise ConfigurationError(
                    "tenant_map must be configured; there is no default tenant", env=self.env
                )
            if not self.reader_token or not self.operator_token:
                raise ConfigurationError(
                    "reader_token and operator_token are required outside dev/test", env=self.env
                )
            if self.reader_token == self.operator_token:
                raise ConfigurationError(
                    "reader and operator tokens must differ; a reader must not hold write scope"
                )
            if not self.cursor_signing_key:
                raise ConfigurationError(
                    "cursor_signing_key must be supplied and durable", env=self.env
                )
            if self.host not in ("127.0.0.1", "localhost") and len(self.reader_token) < 32:
                raise ConfigurationError(
                    "a non-loopback bind requires a reader token of at least 32 characters"
                )
        return self

    def tenant_mapping(self) -> dict[tuple[str, str], str]:
        """Parse ``topic:source=tenant`` entries into a lookup table."""
        mapping: dict[tuple[str, str], str] = {}
        for entry in self.tenant_map:
            if "=" not in entry or ":" not in entry.split("=", 1)[0]:
                raise ConfigurationError(
                    "tenant_map entries must look like 'topic:source_id=tenant_id'", entry=entry
                )
            left, tenant = entry.split("=", 1)
            topic, source_id = left.split(":", 1)
            key = (topic.strip(), source_id.strip())
            tenant_id = tenant.strip()
            if not topic.strip() or not source_id.strip() or not tenant_id:
                raise ConfigurationError("tenant_map entry has an empty component", entry=entry)
            if key in mapping and mapping[key] != tenant_id:
                raise ConfigurationError(
                    "tenant_map maps one topic/source to two tenants", entry=entry
                )
            mapping[key] = tenant_id
        return mapping

    def tenant_resolver(self):
        """Return a ``(topic, source_id) -> tenant_id | None`` callable."""
        mapping = self.tenant_mapping()

        def resolve(topic: str, source_id: str) -> str | None:
            return mapping.get((topic, source_id))

        return resolve

    @property
    def allowed_topics(self) -> frozenset[str]:
        return frozenset(self.kafka_topic_allowlist)
