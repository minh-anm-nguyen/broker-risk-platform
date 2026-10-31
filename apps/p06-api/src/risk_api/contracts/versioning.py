"""API and dependency version negotiation.

A version string from a client never selects code to load. It is checked
against an installed allowlist; anything else is refused.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Mapping

from .problem_details import ApiProblem

__all__ = [
    "API_SCHEMA_VERSION",
    "RESULT_CONTRACT_VERSION",
    "READ_VIEW_CONTRACT_VERSIONS",
    "CapabilityHandshake",
    "resolve_policy_versions",
    "assert_read_view_capability",
]

API_SCHEMA_VERSION: Final = "risk-response-v1"
RESULT_CONTRACT_VERSION: Final = "risk-result-v1"

# Versioned read-side interfaces this build knows how to consume. A drift in
# any of them must be detected at startup, not inside a financial request.
READ_VIEW_CONTRACT_VERSIONS: Final[Mapping[str, str]] = {
    "v_risk_epoch_status": "v1",
    "v_risk_published_cut": "v1",
    "v_risk_reconciliation": "v1",
}


@dataclass(frozen=True, slots=True)
class CapabilityHandshake:
    """What the data provider claims it can serve."""

    read_views: Mapping[str, str]
    provider_kind: str

    def missing(self) -> tuple[str, ...]:
        return tuple(
            sorted(name for name in READ_VIEW_CONTRACT_VERSIONS if name not in self.read_views)
        )

    def mismatched(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                name
                for name, expected in READ_VIEW_CONTRACT_VERSIONS.items()
                if name in self.read_views and self.read_views[name] != expected
            )
        )

    def is_compatible(self) -> bool:
        return not self.missing() and not self.mismatched()


def assert_read_view_capability(handshake: CapabilityHandshake) -> None:
    """Fail fast when the provider cannot prove the contract this build needs."""
    if handshake.is_compatible():
        return
    problems = {"missing": handshake.missing(), "mismatched": handshake.mismatched()}
    raise RuntimeError(f"incompatible read-view contract: {problems}")


def resolve_policy_versions(
    *,
    requested_risk_policy: str | None,
    requested_symbol_catalog: str | None,
    installed_risk_policy: str,
    installed_symbol_catalog: str,
    allowed_risk_policies: tuple[str, ...],
    allowed_symbol_catalogs: tuple[str, ...],
) -> tuple[str, str]:
    """Resolve the policy/catalog pair for a request.

    An omitted version means "use what is installed". A supplied version must
    be both allowlisted and actually installed; otherwise the request is
    refused rather than served under a different policy than it asked for.
    """
    risk_policy = requested_risk_policy or installed_risk_policy
    symbol_catalog = requested_symbol_catalog or installed_symbol_catalog

    if risk_policy not in allowed_risk_policies or risk_policy != installed_risk_policy:
        raise ApiProblem(
            "UNSUPPORTED_CALC_MODE",
            "requested risk policy version is not installed on this instance",
            reason_codes=("POLICY_VERSION_CHANGED",),
        )
    if symbol_catalog not in allowed_symbol_catalogs or symbol_catalog != installed_symbol_catalog:
        raise ApiProblem(
            "UNSUPPORTED_CALC_MODE",
            "requested symbol catalog version is not installed on this instance",
            reason_codes=("POLICY_VERSION_CHANGED",),
        )
    return risk_policy, symbol_catalog
