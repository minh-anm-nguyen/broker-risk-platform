"""Load and validate the approved policy and catalog artifacts.

Policies are data, but they are not *trusted* data: a typo in a YAML file can
change every number the engine publishes. So loading is strict -- unknown
rounding modes, missing sections and non-string decimals are rejected at load
time rather than discovered mid-calculation.

Loaded objects are frozen and cached by path, because every result pins exactly
one policy version and one catalog version. Two positions in one calculation
must never be valued under two catalogs (WF5-210).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import (
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
    Decimal,
)
from functools import lru_cache
from pathlib import Path
from typing import Any, Final, Mapping

import yaml

from risk.domain.classification import SourceCapability
from risk.domain.instruments import CalcMode, SymbolCatalog
from risk.domain.money import parse_decimal
from risk.errors import ContractError, ReasonCode

__all__ = [
    "RiskPolicy",
    "QuotePolicy",
    "GatePolicy",
    "LimitPolicy",
    "load_policy",
    "load_catalog",
    "POLICY_DIR",
    "DEFAULT_POLICY_VERSION",
    "DEFAULT_CATALOG_VERSION",
    "ROUNDING_MODES",
]

POLICY_DIR: Final[Path] = Path(__file__).parent
DEFAULT_POLICY_VERSION: Final[str] = "risk-policy-v1"
DEFAULT_CATALOG_VERSION: Final[str] = "sim-usd-fx-v1"

#: Only these rounding modes may appear in a policy. Allowing an arbitrary
#: string would let a policy select a mode the goldens were never checked
#: against.
ROUNDING_MODES: Final[dict[str, str]] = {
    "ROUND_HALF_EVEN": ROUND_HALF_EVEN,
    "ROUND_HALF_UP": ROUND_HALF_UP,
    "ROUND_HALF_DOWN": ROUND_HALF_DOWN,
    "ROUND_DOWN": ROUND_DOWN,
    "ROUND_UP": ROUND_UP,
    "ROUND_FLOOR": ROUND_FLOOR,
    "ROUND_CEILING": ROUND_CEILING,
}

_CATALOG_FILES: Final[dict[str, str]] = {"sim-usd-fx-v1": "symbol_catalog_v1.yaml"}
_POLICY_FILES: Final[dict[str, str]] = {
    "risk-policy-v1": "risk_policy_v1.yaml",
    "risk-policy-v2": "risk_policy_v2.yaml",
}


def _require(mapping: Mapping[str, Any], key: str, *, where: str) -> Any:
    if key not in mapping:
        raise ContractError(
            ReasonCode.POLICY_UNAVAILABLE, "policy section is missing a key", section=where, key=key
        )
    return mapping[key]


@dataclass(frozen=True, slots=True)
class QuotePolicy:
    max_age_seconds_live: Decimal | None
    max_age_seconds_historical: Decimal | None
    reject_lookahead: bool
    exposure_valuation_basis: str

    def threshold_for(self, *, historical: bool) -> Decimal | None:
        return self.max_age_seconds_historical if historical else self.max_age_seconds_live


@dataclass(frozen=True, slots=True)
class GatePolicy:
    require_epoch_active: bool
    require_position_scan_published: bool
    require_deal_watermark_terminal: bool
    require_deal_watermark_unfiltered: bool
    require_recon_evidence: bool
    require_cut_proof_for_composite: bool
    unknown_recon_status_blocks: bool


@dataclass(frozen=True, slots=True)
class LimitPolicy:
    max_accounts_per_request: int
    max_positions_per_request: int
    max_deals_per_window: int
    read_batch_rows: int
    max_concurrent_jobs: int
    max_queued_jobs: int
    wall_clock_deadline_seconds: Decimal
    max_input_bytes: int


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    """An immutable, versioned set of calculation decisions."""

    policy_version: str
    rounding_mode: str
    internal_precision: int
    max_scale: int
    max_digits: int
    supported_calc_modes: frozenset[CalcMode]
    reporting_currencies: frozenset[str]
    source_capability: SourceCapability
    quotes: QuotePolicy
    gates: GatePolicy
    limits: LimitPolicy
    fee_fields_available: bool
    non_trading_reported_separately: bool
    audit_mandatory_for_ready: bool
    audit_retry_attempts: int

    def supports_currency(self, currency: str) -> bool:
        return currency in self.reporting_currencies

    def supports_calc_mode(self, mode: CalcMode) -> bool:
        return mode in self.supported_calc_modes


def _parse_policy(raw: Mapping[str, Any]) -> RiskPolicy:
    policy_version = _require(raw, "policy_version", where="root")
    if not isinstance(policy_version, str) or not policy_version:
        raise ContractError(ReasonCode.POLICY_UNAVAILABLE, "policy_version must be a non-empty string")

    money = _require(raw, "money", where="root")
    rounding_name = _require(money, "rounding_mode", where="money")
    if rounding_name not in ROUNDING_MODES:
        raise ContractError(
            ReasonCode.POLICY_UNAVAILABLE, "unapproved rounding mode", rounding_mode=str(rounding_name)
        )

    profile = _require(raw, "supported_profile", where="root")
    modes: set[CalcMode] = set()
    for name in _require(profile, "calc_modes", where="supported_profile"):
        try:
            modes.add(CalcMode(name))
        except ValueError:
            raise ContractError(
                ReasonCode.UNSUPPORTED_CALC_MODE, "policy names an unknown calc mode", calc_mode=str(name)
            ) from None
    try:
        capability = SourceCapability(_require(profile, "source_capability", where="supported_profile"))
    except ValueError:
        raise ContractError(
            ReasonCode.POLICY_UNAVAILABLE, "policy names an unknown source capability"
        ) from None

    quotes_raw = _require(raw, "quotes", where="root")
    live = quotes_raw.get("max_age_seconds_live")
    historical = quotes_raw.get("max_age_seconds_historical")
    quotes = QuotePolicy(
        max_age_seconds_live=None if live is None else Decimal(int(live)),
        max_age_seconds_historical=None if historical is None else Decimal(int(historical)),
        reject_lookahead=bool(_require(quotes_raw, "reject_lookahead", where="quotes")),
        exposure_valuation_basis=str(
            _require(quotes_raw, "exposure_valuation_basis", where="quotes")
        ),
    )

    gates_raw = _require(raw, "gates", where="root")
    gates = GatePolicy(
        require_epoch_active=bool(_require(gates_raw, "require_epoch_active", where="gates")),
        require_position_scan_published=bool(
            _require(gates_raw, "require_position_scan_published", where="gates")
        ),
        require_deal_watermark_terminal=bool(
            _require(gates_raw, "require_deal_watermark_terminal", where="gates")
        ),
        require_deal_watermark_unfiltered=bool(
            _require(gates_raw, "require_deal_watermark_unfiltered", where="gates")
        ),
        require_recon_evidence=bool(_require(gates_raw, "require_recon_evidence", where="gates")),
        require_cut_proof_for_composite=bool(
            _require(gates_raw, "require_cut_proof_for_composite", where="gates")
        ),
        unknown_recon_status_blocks=bool(
            _require(gates_raw, "unknown_recon_status_blocks", where="gates")
        ),
    )

    limits_raw = _require(raw, "limits", where="root")
    limits = LimitPolicy(
        max_accounts_per_request=int(_require(limits_raw, "max_accounts_per_request", where="limits")),
        max_positions_per_request=int(_require(limits_raw, "max_positions_per_request", where="limits")),
        max_deals_per_window=int(_require(limits_raw, "max_deals_per_window", where="limits")),
        read_batch_rows=int(_require(limits_raw, "read_batch_rows", where="limits")),
        max_concurrent_jobs=int(_require(limits_raw, "max_concurrent_jobs", where="limits")),
        max_queued_jobs=int(_require(limits_raw, "max_queued_jobs", where="limits")),
        wall_clock_deadline_seconds=Decimal(
            int(_require(limits_raw, "wall_clock_deadline_seconds", where="limits"))
        ),
        max_input_bytes=int(_require(limits_raw, "max_input_bytes", where="limits")),
    )
    for name, value in (
        ("max_accounts_per_request", limits.max_accounts_per_request),
        ("max_positions_per_request", limits.max_positions_per_request),
        ("read_batch_rows", limits.read_batch_rows),
        ("max_concurrent_jobs", limits.max_concurrent_jobs),
    ):
        if value <= 0:
            raise ContractError(ReasonCode.POLICY_UNAVAILABLE, "limit must be positive", key=name)

    realized = _require(raw, "realized", where="root")
    audit = _require(raw, "audit", where="root")

    return RiskPolicy(
        policy_version=policy_version,
        rounding_mode=ROUNDING_MODES[rounding_name],
        internal_precision=int(_require(money, "internal_precision", where="money")),
        max_scale=int(_require(money, "max_scale", where="money")),
        max_digits=int(_require(money, "max_digits", where="money")),
        supported_calc_modes=frozenset(modes),
        reporting_currencies=frozenset(
            str(c) for c in _require(profile, "reporting_currencies", where="supported_profile")
        ),
        source_capability=capability,
        quotes=quotes,
        gates=gates,
        limits=limits,
        fee_fields_available=bool(_require(realized, "fee_fields_available", where="realized")),
        non_trading_reported_separately=bool(
            _require(realized, "non_trading_reported_separately", where="realized")
        ),
        audit_mandatory_for_ready=bool(_require(audit, "mandatory_for_ready", where="audit")),
        audit_retry_attempts=int(_require(audit, "retry_attempts", where="audit")),
    )


@lru_cache(maxsize=8)
def load_policy(version: str = DEFAULT_POLICY_VERSION) -> RiskPolicy:
    """Load an approved policy by version name."""
    filename = _POLICY_FILES.get(version)
    if filename is None:
        raise ContractError(
            ReasonCode.POLICY_UNAVAILABLE, "no such approved policy version", requested=version
        )
    path = POLICY_DIR / filename
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ContractError(
            ReasonCode.POLICY_UNAVAILABLE, "policy artifact could not be read", requested=version
        ) from exc
    if not isinstance(raw, dict):
        raise ContractError(ReasonCode.POLICY_UNAVAILABLE, "policy artifact is not a mapping")
    policy = _parse_policy(raw)
    if policy.policy_version != version:
        # The file's own declared version wins the argument, so a mislabelled
        # artifact cannot masquerade as an approved one.
        raise ContractError(
            ReasonCode.POLICY_VERSION_CHANGED,
            "policy artifact declares a different version than requested",
            requested=version,
            declared=policy.policy_version,
        )
    return policy


@lru_cache(maxsize=8)
def load_catalog(version: str = DEFAULT_CATALOG_VERSION) -> SymbolCatalog:
    """Load an approved symbol catalog by version name."""
    filename = _CATALOG_FILES.get(version)
    if filename is None:
        raise ContractError(
            ReasonCode.SPEC_VERSION_MISMATCH, "no such approved catalog version", requested=version
        )
    path = POLICY_DIR / filename
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ContractError(
            ReasonCode.SPEC_VERSION_MISMATCH, "catalog artifact could not be read", requested=version
        ) from exc
    if not isinstance(raw, dict):
        raise ContractError(ReasonCode.SPEC_VERSION_MISMATCH, "catalog artifact is not a mapping")
    declared = raw.get("catalog_version")
    if declared != version:
        raise ContractError(
            ReasonCode.SPEC_VERSION_MISMATCH,
            "catalog artifact declares a different version than requested",
            requested=version,
            declared=str(declared),
        )
    rows = raw.get("symbols")
    if not isinstance(rows, list) or not rows:
        raise ContractError(ReasonCode.SPEC_VERSION_MISMATCH, "catalog has no symbol rows")
    effective_from = raw.get("effective_from")
    if effective_from is not None and not isinstance(effective_from, (str, datetime)):
        raise ContractError(ReasonCode.SPEC_VERSION_MISMATCH, "effective_from must be a timestamp")
    # Decimal fields must already be strings in the artifact: a YAML float would
    # have lost precision before this code ever sees it.
    for row in rows:
        for key in ("contract_size", "tick_size", "volume_step", "volume_max"):
            if isinstance(row.get(key), float):
                raise ContractError(
                    ReasonCode.DECIMAL_STRING_REQUIRED,
                    "catalog decimals must be quoted strings",
                    symbol=str(row.get("symbol")),
                    key=key,
                )
            parse_decimal(row.get(key), field=key)
    return SymbolCatalog.from_rows(version, rows)
