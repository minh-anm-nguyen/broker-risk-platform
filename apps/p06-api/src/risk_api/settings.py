"""Configuration for the Risk Reporting API.

Every limit in design doc section 4.3 and every budget in section 7.3 is a
named setting here, so a test can assert behaviour at L-1, L and L+1 without
patching module internals.

Secrets are read from the environment. The bundled development key material is
only usable when ``environment`` is ``local`` or ``test``; a non-local
environment that does not supply its own keys refuses to start.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "get_settings", "reset_settings_cache"]

Environment = Literal["local", "test", "staging", "production"]

# Development-only material. Deliberately obvious so it cannot be mistaken for
# a production secret, and rejected outright outside local/test.
DEV_JWT_KEYS: dict[str, str] = {"local-test-key-1": "dev-only-insecure-jwt-signing-key-0001"}
DEV_CURSOR_KEY = "dev-only-insecure-cursor-signing-key-0001"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RISK_API_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
    )

    environment: Environment = "local"
    api_base_path: str = "/api/v1"
    service_name: str = "risk-reporting-api"

    # ------------------------------------------------------------- identity
    jwt_issuer: str = "https://issuer.local.test/risk"
    jwt_audience: str = "risk-reporting-api"
    jwt_allowed_algorithms: tuple[str, ...] = ("HS256",)
    jwt_keys: dict[str, str] = Field(default_factory=lambda: dict(DEV_JWT_KEYS))
    jwt_clock_skew_seconds: int = 30
    jwt_max_lifetime_seconds: int = 3600

    # ------------------------------------------------------- report policy
    strict_composite_default: bool = True
    reporting_currency: str = "USD"
    risk_policy_version: str = "risk-policy-v1"
    symbol_catalog_version: str = "sim-usd-fx-v1"
    allowed_risk_policy_versions: tuple[str, ...] = ("risk-policy-v1",)
    allowed_symbol_catalog_versions: tuple[str, ...] = ("sim-usd-fx-v1",)
    api_schema_version: str = "risk-response-v1"
    result_contract_version: str = "risk-result-v1"
    allow_pinned_epoch_requests: bool = False

    # --------------------------------------------------------------- limits
    max_body_bytes: int = 64 * 1024
    max_url_chars: int = 2048
    max_query_chars: int = 1024
    max_json_depth: int = 12
    max_accounts_per_request: int = 100
    max_positions_page: int = 100
    default_positions_page: int = 50
    max_aggregate_symbols: int = 200
    max_pnl_window_days: int = 31
    default_realized_window_days: int = 31
    max_response_bytes: int = 1024 * 1024
    cursor_ttl_seconds: int = 120
    reject_content_encoding: bool = True

    # -------------------------------------------------------------- budgets
    compute_budget_seconds: float = 3.0
    request_deadline_seconds: float = 5.0
    capture_budget_seconds: float = 1.0
    final_check_budget_seconds: float = 0.5
    max_recapture_attempts: int = 1

    # ------------------------------------------------------------ admission
    max_heavy_executing: int = 4
    max_heavy_queued: int = 16
    max_cheap_concurrent: int = 32
    rate_limit_per_second: float = 20.0
    rate_limit_burst: float = 40.0
    heavy_request_weight: float = 4.0
    cheap_request_weight: float = 1.0
    retry_after_seconds: int = 2

    # ------------------------------------------------------------- database
    db_pool_size: int = 5
    db_max_overflow: int = 0
    db_pool_timeout_seconds: float = 0.3
    db_statement_timeout_ms: int = 1000

    # ---------------------------------------------------------------- audit
    audit_required: bool = True
    audit_backend: Literal["memory", "jsonl"] = "memory"
    audit_path: str = "./var/audit/api_access_audit.jsonl"
    audit_commit_timeout_seconds: float = 1.0

    # --------------------------------------------------------------- cursor
    cursor_signing_key: str = DEV_CURSOR_KEY
    cursor_key_id: str = "cursor-key-1"

    # -------------------------------------------------------- observability
    expose_internal_metrics: bool = True
    log_level: str = "INFO"
    trusted_proxy_request_id_header: str | None = None

    # ------------------------------------------------------------ test only
    faults_enabled: bool = False

    @field_validator("api_base_path")
    @classmethod
    def _base_path_shape(cls, value: str) -> str:
        if not value.startswith("/") or value.endswith("/"):
            raise ValueError("api_base_path must start with / and must not end with /")
        return value

    @field_validator("jwt_allowed_algorithms")
    @classmethod
    def _no_unsigned_algorithms(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("at least one signing algorithm must be allowed")
        for algorithm in value:
            if algorithm.lower() in ("none", ""):
                raise ValueError("unsigned tokens are never acceptable")
            if algorithm not in ("HS256", "HS384", "HS512"):
                raise ValueError(f"algorithm {algorithm!r} is not supported by this build")
        return value

    @model_validator(mode="after")
    def _guard_development_secrets(self) -> "Settings":
        if self.environment in ("local", "test"):
            return self
        if any(self.jwt_keys.get(kid) == secret for kid, secret in DEV_JWT_KEYS.items()):
            raise ValueError("development JWT key material is not allowed outside local/test")
        if self.cursor_signing_key == DEV_CURSOR_KEY:
            raise ValueError("development cursor key is not allowed outside local/test")
        if self.faults_enabled:
            raise ValueError("fault injection must stay disabled outside local/test")
        return self

    @model_validator(mode="after")
    def _guard_budget_coherence(self) -> "Settings":
        overhead = self.capture_budget_seconds + self.final_check_budget_seconds
        if self.compute_budget_seconds + overhead > self.request_deadline_seconds:
            raise ValueError(
                "capture + compute + final-check sub-budgets must fit inside request_deadline_seconds"
            )
        if self.default_positions_page > self.max_positions_page:
            raise ValueError("default page size cannot exceed the maximum page size")
        if self.rate_limit_burst < self.rate_limit_per_second:
            raise ValueError("burst must be at least the sustained rate")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings, cached so FastAPI dependencies stay cheap."""
    return Settings()


def reset_settings_cache() -> None:
    """Drop the cached settings. Tests use this after changing the environment."""
    get_settings.cache_clear()
