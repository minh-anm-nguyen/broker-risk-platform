"""Typed settings with fail-fast validation (SYSTEM_DESIGN sections 8.1, 11).

Design rules enforced here:

* **Every resource has an explicit bound.** There is no "unlimited" mode for
  pages, run duration, scan quota or concurrent runs. An audit that can grow
  without limit will eventually starve the sync service it is auditing, which
  turns a correctness tool into an availability incident (FR04-12, FMEA "audit
  monopolises DB").
* **Safety parameters are configuration, not constants.** The 20% mass-drift
  cap and the 5-minute evidence age in section 8.1 are explicitly described by
  the design as values to tune per environment, so they live here and are
  recorded in the policy hash of every run.
* **The policy hash covers everything that can change a verdict.** If an
  operator loosens a threshold, runs taken before and after are no longer
  comparable, and the stored hash is what makes that visible (FR04-02).
* **Auto-dispatch is off by default.** Section 8.1 makes every money-affecting
  action forbidden by default; a config flag cannot grant more than the action
  allowlist in ``errors.ActionType`` permits.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .canonical import HASH_VERSION, SCHEMA_VERSION, content_hash
from .errors import ConfigurationError


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RECON_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # ---- Deployment ------------------------------------------------------
    env: Literal["dev", "test", "docker", "prod"] = "dev"
    worker_id: str = Field(default="recon-worker-1", min_length=1, max_length=64)

    # ---- Storage ---------------------------------------------------------
    #: The reference deployment targets PostgreSQL (section 6.2). This build
    #: runs on SQLite in WAL mode, whose read transactions provide the same
    #: snapshot isolation the target-capture protocol depends on. The DDL in
    #: ``database/migrations`` keeps the PostgreSQL form authoritative.
    db_path: Path = Field(default=Path("./data/recon.db"))
    sqlite_busy_timeout_ms: int = Field(default=5_000, ge=100, le=60_000)
    sqlite_synchronous: Literal["OFF", "NORMAL", "FULL"] = "FULL"
    statement_timeout_ms: int = Field(default=30_000, ge=100, le=600_000)

    # ---- Source contract (P01 declared limits, section 2.1) --------------
    source_base_url: str = Field(default="http://127.0.0.1:8001")
    source_id: str = Field(default="sim_local", min_length=1, max_length=64)
    source_data_token: str = ""
    source_max_page_size: int = Field(default=500, ge=1, le=500)
    source_snapshot_ttl_s: int = Field(default=900, ge=1, le=86_400)
    source_request_timeout_s: float = Field(default=10.0, gt=0, le=120)
    source_max_retries: int = Field(default=3, ge=0, le=10)
    source_retry_base_delay_s: float = Field(default=0.05, ge=0, le=5)

    # ---- Run bounds (FR04-12) --------------------------------------------
    max_pages_per_run: int = Field(default=10_000, ge=1, le=1_000_000)
    max_items_per_run: int = Field(default=2_000_000, ge=1)
    run_deadline_s: int = Field(default=1_800, ge=1, le=86_400)
    max_concurrent_runs: int = Field(default=2, ge=1, le=32)
    max_concurrent_repairs_per_source: int = Field(default=1, ge=1, le=8)
    lease_ttl_s: int = Field(default=120, ge=5, le=3_600)

    # ---- Safety policy (section 8.1 -- proposals, tune per environment) --
    mass_drift_fraction: float = Field(default=0.20, gt=0, le=1.0)
    mass_drift_min_entities: int = Field(default=10, ge=1)
    large_count_drop_fraction: float = Field(default=0.50, gt=0, le=1.0)
    evidence_max_age_s: int = Field(default=300, ge=1, le=86_400)
    plan_ttl_s: int = Field(default=3_600, ge=60, le=604_800)
    approval_required: bool = True
    allow_auto_dispatch: bool = False
    allow_deal_backfill: bool = False
    require_distinct_approver: bool = True

    # ---- Cross-entity capability (section 5.4) ---------------------------
    #: Raising this above ``T1`` is a claim about the upstream contract. It is
    #: refused unless the source capability profile actually advertises a
    #: coordinated global cut, so configuration alone cannot manufacture a
    #: stronger guarantee than the source provides.
    cross_entity_tier: Literal["T0", "T1", "T2"] = "T1"
    cross_entity_grace_s: int = Field(default=60, ge=0, le=3_600)

    # ---- Evidence retention (INV04-15) -----------------------------------
    evidence_retention_s: int = Field(default=604_800, ge=60)
    artifacts_dir: Path = Field(default=Path("./artifacts"))

    @field_validator("source_base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("source_base_url must be an http(s) URL")
        return value.rstrip("/")

    @model_validator(mode="after")
    def _validate_coherence(self) -> Settings:
        if self.max_items_per_run < self.max_pages_per_run:
            raise ConfigurationError(
                "max_items_per_run must allow at least one item per permitted page",
                max_items_per_run=self.max_items_per_run,
                max_pages_per_run=self.max_pages_per_run,
            )
        # A plan whose evidence is already stale when it is created can never
        # be dispatched, so the combination is a configuration error rather
        # than a runtime surprise (section 8.1 "evidence age > 5 minutes").
        if self.plan_ttl_s < self.evidence_max_age_s:
            raise ConfigurationError(
                "plan_ttl_s below evidence_max_age_s would expire every plan on creation",
                plan_ttl_s=self.plan_ttl_s,
                evidence_max_age_s=self.evidence_max_age_s,
            )
        if self.env == "prod":
            if self.allow_auto_dispatch:
                raise ConfigurationError(
                    "auto dispatch without human approval is not permitted in prod",
                    hint="section 8.1 makes every money-affecting action forbidden by default",
                )
            if not self.approval_required:
                raise ConfigurationError("approval_required cannot be disabled in prod")
            if not self.source_data_token:
                raise ConfigurationError("source_data_token is required outside local runs")
        if self.allow_auto_dispatch and self.approval_required:
            raise ConfigurationError(
                "allow_auto_dispatch and approval_required are contradictory",
                hint="disable approval_required explicitly if a test needs unattended dispatch",
            )
        return self

    # ---- Derived fingerprints -------------------------------------------

    def policy_payload(self) -> dict[str, Any]:
        """Exactly the knobs that can change a verdict.

        Deployment details such as ``worker_id`` and the database path are
        excluded: two workers applying the same policy must produce the same
        ``policy_hash``, otherwise runs become incomparable for no reason
        (FR04-02).
        """
        return {
            "schema_version": SCHEMA_VERSION,
            "hash_version": HASH_VERSION,
            "source_max_page_size": self.source_max_page_size,
            "source_snapshot_ttl_s": self.source_snapshot_ttl_s,
            "max_pages_per_run": self.max_pages_per_run,
            "max_items_per_run": self.max_items_per_run,
            "run_deadline_s": self.run_deadline_s,
            "mass_drift_fraction": self.mass_drift_fraction,
            "mass_drift_min_entities": self.mass_drift_min_entities,
            "large_count_drop_fraction": self.large_count_drop_fraction,
            "evidence_max_age_s": self.evidence_max_age_s,
            "plan_ttl_s": self.plan_ttl_s,
            "approval_required": self.approval_required,
            "allow_auto_dispatch": self.allow_auto_dispatch,
            "allow_deal_backfill": self.allow_deal_backfill,
            "require_distinct_approver": self.require_distinct_approver,
            "cross_entity_tier": self.cross_entity_tier,
            "cross_entity_grace_s": self.cross_entity_grace_s,
        }

    @property
    def policy_hash(self) -> str:
        return content_hash(self.policy_payload())

    def scope_payload(self, entity: str, scope: str) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "entity": entity,
            "scope": scope,
            "schema_version": SCHEMA_VERSION,
        }

    def scope_hash(self, entity: str, scope: str = "UNFILTERED") -> str:
        return content_hash(self.scope_payload(entity, scope))


def load_settings(**overrides: Any) -> Settings:
    """Construct settings, turning pydantic errors into ConfigurationError."""
    try:
        return Settings(**overrides)
    except ConfigurationError:
        raise
    except Exception as exc:  # pydantic ValidationError and friends
        raise ConfigurationError("invalid reconciliation configuration", detail=str(exc)) from exc
