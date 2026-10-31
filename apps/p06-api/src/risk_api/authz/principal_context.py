"""Typed identity and scope context.

The distinct types here exist to make a category error impossible: a
``RequestedFilters`` can never be passed where an ``AuthorizedScope`` is
expected, so a client-supplied account list cannot reach a query by accident.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Final, Mapping

__all__ = [
    "Permission",
    "ROLE_PERMISSIONS",
    "Principal",
    "ScopePolicy",
    "RequestedFilters",
    "AuthorizedScope",
    "scope_hash",
]


class Permission:
    """Endpoint-level permissions. Roles are bundles of these."""

    READ_ACCOUNT_RISK: Final = "risk:read_account"
    READ_ACCOUNT_PNL: Final = "risk:read_pnl"
    READ_POSITIONS: Final = "risk:read_positions"
    READ_AGGREGATE: Final = "risk:read_aggregate"
    READ_DAILY_REPORT: Final = "risk:read_daily"
    SUBMIT_CALCULATION: Final = "risk:submit_calculation"
    READ_INTERNAL_METRICS: Final = "ops:read_metrics"

    ALL: Final = (
        READ_ACCOUNT_RISK,
        READ_ACCOUNT_PNL,
        READ_POSITIONS,
        READ_AGGREGATE,
        READ_DAILY_REPORT,
        SUBMIT_CALCULATION,
        READ_INTERNAL_METRICS,
    )


# Roles from design doc section 6.1. The least privileged role cannot reach
# aggregates or internal metrics.
ROLE_PERMISSIONS: Final[Mapping[str, tuple[str, ...]]] = {
    "risk_viewer": (
        Permission.READ_ACCOUNT_RISK,
        Permission.READ_ACCOUNT_PNL,
        Permission.READ_POSITIONS,
    ),
    "risk_analyst": (
        Permission.READ_ACCOUNT_RISK,
        Permission.READ_ACCOUNT_PNL,
        Permission.READ_POSITIONS,
        Permission.READ_AGGREGATE,
        Permission.READ_DAILY_REPORT,
        Permission.SUBMIT_CALCULATION,
    ),
    "service_reporter": (
        Permission.READ_ACCOUNT_RISK,
        Permission.READ_ACCOUNT_PNL,
        Permission.READ_POSITIONS,
        Permission.READ_AGGREGATE,
        Permission.READ_DAILY_REPORT,
        Permission.SUBMIT_CALCULATION,
    ),
    # Policy administration happens outside this API; the role gets no data reads.
    "risk_admin": (),
    "ops_reader": (Permission.READ_INTERNAL_METRICS,),
}


def scope_hash(*parts: object) -> str:
    """Stable hash used for audit and cursor binding.

    Hashing rather than storing raw identifiers keeps account numbers out of
    cursors, metrics and audit rows while still proving two scopes match.
    """
    joined = "\x1f".join("" if part is None else str(part) for part in parts)
    return "sha256:" + hashlib.sha256(joined.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class Principal:
    """The authenticated caller, as the server understands it."""

    principal_id: str
    tenant_id: str
    roles: tuple[str, ...]
    token_scopes: tuple[str, ...]
    key_id: str
    token_id: str | None = None

    @property
    def permissions(self) -> frozenset[str]:
        """Permissions granted by the server-side role record.

        Token scopes narrow this set but can never widen it: a token asking for
        ``risk:read_aggregate`` on a viewer role still gets nothing.
        """
        granted: set[str] = set()
        for role in self.roles:
            granted.update(ROLE_PERMISSIONS.get(role, ()))
        if self.token_scopes:
            granted &= set(self.token_scopes)
        return frozenset(granted)

    def has(self, permission: str) -> bool:
        return permission in self.permissions


@dataclass(frozen=True, slots=True)
class ScopePolicy:
    """Server-side record of what a subject may read. Never client supplied."""

    principal_id: str
    tenant_id: str
    roles: tuple[str, ...]
    revision: str
    accounts_by_source: Mapping[str, frozenset[str]] = field(default_factory=dict)
    aggregate_sources: frozenset[str] = frozenset()
    symbol_allowlist: Mapping[str, frozenset[str]] = field(default_factory=dict)

    @property
    def sources(self) -> frozenset[str]:
        return frozenset(self.accounts_by_source)

    def accounts_for(self, source_id: str) -> frozenset[str]:
        return self.accounts_by_source.get(source_id, frozenset())

    def allows_aggregate(self, source_id: str) -> bool:
        return source_id in self.aggregate_sources

    def symbols_for(self, source_id: str) -> frozenset[str] | None:
        """``None`` means every symbol in the source is permitted."""
        return self.symbol_allowlist.get(source_id)


@dataclass(frozen=True, slots=True)
class RequestedFilters:
    """What the client asked for. Untrusted by construction."""

    source_id: str
    account_ids: tuple[str, ...]
    symbol: str | None = None
    epoch_id: str | None = None


@dataclass(frozen=True, slots=True)
class AuthorizedScope:
    """The server's decision. Only this type may reach a query or the engine."""

    principal_id: str
    tenant_id: str
    source_id: str
    epoch_id: str
    accounts: tuple[str, ...]
    auth_scope_revision: str
    symbol: str | None = None
    permitted_symbols: frozenset[str] | None = None

    def __post_init__(self) -> None:
        if not self.accounts:
            raise ValueError("an authorized scope always names at least one account")
        if tuple(sorted(self.accounts)) != self.accounts:
            raise ValueError("authorized accounts must be canonically sorted")

    @property
    def tenant_scope_hash(self) -> str:
        return scope_hash(self.tenant_id, self.principal_id, self.source_id)

    @property
    def account_scope_hash(self) -> str:
        return scope_hash(self.tenant_id, self.source_id, *self.accounts)

    def audit_scope_hash(self) -> str:
        return scope_hash(
            self.tenant_id,
            self.principal_id,
            self.source_id,
            self.epoch_id,
            self.auth_scope_revision,
            *self.accounts,
        )
