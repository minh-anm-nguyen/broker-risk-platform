"""Scope resolution and object-level authorization.

Design doc section 6.2 in code. The order is deliberate and tested:
authenticate, resolve the server-side policy, check the endpoint permission,
then intersect the requested objects. Nothing reads data before all four pass.

Masking: an account outside the caller's scope produces the same 404 as an
account that does not exist, so a prober cannot enumerate the broker. A bulk
request that names any unauthorized account is denied as a whole, because
returning the authorized subset would itself disclose membership.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

from ..contracts.problem_details import ApiProblem
from .principal_context import AuthorizedScope, Principal, RequestedFilters, ScopePolicy

__all__ = [
    "ScopePolicyRepository",
    "InMemoryScopePolicyRepository",
    "EpochResolver",
    "authorize_scope",
    "require_permission",
    "AuthorizationDecision",
]


class ScopePolicyRepository(Protocol):
    """Read-only policy lookup port."""

    async def resolve(self, subject: str) -> ScopePolicy | None: ...

    async def revision(self, subject: str) -> str | None: ...


class EpochResolver(Protocol):
    """Resolves the epoch a request runs against."""

    async def active_epoch(self, source_id: str) -> str | None: ...

    async def epoch_status(self, source_id: str, epoch_id: str) -> str | None: ...


@dataclass(slots=True)
class InMemoryScopePolicyRepository:
    """Deterministic policy store.

    ``bump_revision`` models a policy change landing mid-request, which the
    final authorization gate must notice.
    """

    policies: dict[str, ScopePolicy]

    async def resolve(self, subject: str) -> ScopePolicy | None:
        return self.policies.get(subject)

    async def revision(self, subject: str) -> str | None:
        policy = self.policies.get(subject)
        return None if policy is None else policy.revision

    def put(self, subject: str, policy: ScopePolicy) -> None:
        self.policies[subject] = policy

    def bump_revision(self, subject: str, revision: str) -> None:
        policy = self.policies[subject]
        self.policies[subject] = ScopePolicy(
            principal_id=policy.principal_id,
            tenant_id=policy.tenant_id,
            roles=policy.roles,
            revision=revision,
            accounts_by_source=policy.accounts_by_source,
            aggregate_sources=policy.aggregate_sources,
            symbol_allowlist=policy.symbol_allowlist,
        )

    def revoke_account(self, subject: str, source_id: str, account_id: str, *, revision: str) -> None:
        policy = self.policies[subject]
        remaining = policy.accounts_for(source_id) - {account_id}
        accounts = dict(policy.accounts_by_source)
        accounts[source_id] = remaining
        self.policies[subject] = ScopePolicy(
            principal_id=policy.principal_id,
            tenant_id=policy.tenant_id,
            roles=policy.roles,
            revision=revision,
            accounts_by_source=accounts,
            aggregate_sources=policy.aggregate_sources,
            symbol_allowlist=policy.symbol_allowlist,
        )


@dataclass(frozen=True, slots=True)
class AuthorizationDecision:
    """What was decided, for the audit trail. Carries no account identifiers."""

    allowed: bool
    denial_family: str | None
    scope_hash: str | None


def require_permission(principal: Principal, permission: str, *, denial_family: str) -> None:
    """Check an endpoint-level permission before any data access."""
    if not principal.has(permission):
        raise ApiProblem(
            "ACCESS_DENIED",
            "the authenticated principal is not permitted to use this endpoint",
            reason_codes=(denial_family,),
        )


async def authorize_scope(
    *,
    principal: Principal,
    policy: ScopePolicy,
    filters: RequestedFilters,
    epochs: EpochResolver,
    allow_pinned_epoch: bool,
    aggregate: bool = False,
    max_accounts: int = 100,
) -> AuthorizedScope:
    """Intersect requested filters with the server-side policy.

    Raises a masked ``RESOURCE_NOT_FOUND`` for anything out of scope on a
    single-object route, and ``ACCESS_DENIED`` for a bulk request the role may
    not perform at all.
    """
    if policy.tenant_id != principal.tenant_id:
        # The token's tenant and the policy record must agree. A mismatch is a
        # configuration fault, not something to resolve in the caller's favour.
        raise ApiProblem("ACCESS_DENIED", "principal tenant does not match its policy record")

    if filters.source_id not in policy.sources:
        raise ApiProblem(
            "RESOURCE_NOT_FOUND",
            "the requested resource does not exist or is not visible to this principal",
            reason_codes=("SOURCE_OUT_OF_SCOPE",),
        )

    permitted = policy.accounts_for(filters.source_id)
    requested = tuple(sorted(set(filters.account_ids)))

    if not requested:
        # An empty account list is never a wildcard.
        raise ApiProblem(
            "INVALID_REQUEST",
            "at least one account must be named; an empty selection is not a wildcard",
            reason_codes=("EMPTY_ACCOUNT_SELECTION",),
        )
    if len(requested) > max_accounts:
        raise ApiProblem(
            "ACCOUNT_SCOPE_TOO_LARGE",
            f"at most {max_accounts} accounts may be requested",
        )
    if aggregate and not policy.allows_aggregate(filters.source_id):
        raise ApiProblem(
            "ACCESS_DENIED",
            "this principal may not request aggregate reports for the source",
            reason_codes=("AGGREGATE_NOT_PERMITTED",),
        )

    outside = [account for account in requested if account not in permitted]
    if outside:
        if aggregate or len(requested) > 1:
            # Bulk mismatch: deny the whole request and name nothing.
            raise ApiProblem(
                "ACCESS_DENIED",
                "the requested account selection includes accounts outside this scope",
                reason_codes=("ACCOUNT_OUT_OF_SCOPE",),
            )
        raise ApiProblem(
            "RESOURCE_NOT_FOUND",
            "the requested resource does not exist or is not visible to this principal",
            reason_codes=("ACCOUNT_OUT_OF_SCOPE",),
        )

    epoch_id = await _resolve_epoch(
        filters=filters, epochs=epochs, allow_pinned_epoch=allow_pinned_epoch
    )

    return AuthorizedScope(
        principal_id=principal.principal_id,
        tenant_id=policy.tenant_id,
        source_id=filters.source_id,
        epoch_id=epoch_id,
        accounts=requested,
        auth_scope_revision=policy.revision,
        symbol=filters.symbol,
        permitted_symbols=policy.symbols_for(filters.source_id),
    )


async def _resolve_epoch(
    *, filters: RequestedFilters, epochs: EpochResolver, allow_pinned_epoch: bool
) -> str:
    """Choose the epoch. The server decides unless pinning is explicitly enabled."""
    active = await epochs.active_epoch(filters.source_id)
    if filters.epoch_id is None:
        if active is None:
            raise ApiProblem(
                "DATA_NOT_READY",
                "the source has no active epoch to report on",
                reason_codes=("SOURCE_NOT_ACTIVE",),
            )
        return active

    if filters.epoch_id == active:
        return active
    if not allow_pinned_epoch:
        # A client cannot select an archived epoch to dodge a new finding.
        raise ApiProblem(
            "ACCESS_DENIED",
            "pinning a historical epoch is not enabled on this instance",
            reason_codes=("EPOCH_PINNING_NOT_PERMITTED",),
        )
    status = await epochs.epoch_status(filters.source_id, filters.epoch_id)
    if status != "ACTIVE":
        raise ApiProblem(
            "EPOCH_CHANGED",
            "the requested epoch is no longer reportable",
            reason_codes=("SOURCE_NOT_ACTIVE",),
        )
    return filters.epoch_id


def permissions_snapshot(policies: Mapping[str, ScopePolicy]) -> dict[str, tuple[str, ...]]:
    """Debug helper: subject to roles. Used by tests and the readiness probe."""
    return {subject: policy.roles for subject, policy in sorted(policies.items())}
