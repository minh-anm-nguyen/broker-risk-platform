"""Authentication and tenant scope. Section 6, INV08-17.

The central type is :class:`Scope`, and its only interesting method is
:meth:`Scope.narrow`. A request may restrict what it asks for and may never
extend it, so the operation is intersection - there is no method that adds a
source or an account to a scope. Code that wanted to widen access would have
to construct a new :class:`Scope`, which only the authenticator does.

Token comparison uses :func:`hmac.compare_digest`. The timing leak from
``==`` on a short token is small, but there is no reason to accept it.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Mapping, Sequence

from ..errors import AuthenticationError, AuthorizationError, Reason


@dataclass(frozen=True, slots=True)
class Scope:
    """What one caller may see. ``None`` on a set means "all within tenant"."""

    tenant_id: str
    source_ids: frozenset[str] | None = None
    epoch_ids: frozenset[str] | None = None
    account_ids: frozenset[str] | None = None
    report_families: frozenset[str] | None = None
    #: Operator scope permits rollback, GC and quarantine resolution.
    operator: bool = False

    def allows_source(self, source_id: str) -> bool:
        return self.source_ids is None or source_id in self.source_ids

    def allows_epoch(self, epoch_id: str) -> bool:
        return self.epoch_ids is None or epoch_id in self.epoch_ids

    def allows_report_family(self, family: str) -> bool:
        return self.report_families is None or family in self.report_families

    def require_source(self, source_id: str) -> str:
        if not self.allows_source(source_id):
            raise AuthorizationError(
                Reason.FORBIDDEN_SCOPE, "source is outside the caller's scope", source_id=source_id
            )
        return source_id

    def require_epoch(self, epoch_id: str) -> str:
        if not self.allows_epoch(epoch_id):
            raise AuthorizationError(
                Reason.FORBIDDEN_SCOPE, "epoch is outside the caller's scope", epoch_id=epoch_id
            )
        return epoch_id

    def require_report_family(self, family: str) -> str:
        if not self.allows_report_family(family):
            raise AuthorizationError(
                Reason.FORBIDDEN_SCOPE,
                "report family is outside the caller's scope",
                report_family=family,
            )
        return family

    def require_operator(self) -> None:
        if not self.operator:
            raise AuthorizationError(
                Reason.FORBIDDEN_SCOPE, "this operation requires operator scope"
            )

    def narrow_accounts(self, requested: Sequence[str] | None) -> frozenset[str] | None:
        """Intersect requested accounts with the permitted set.

        An account the caller may not see is refused rather than dropped from
        the filter: silently returning the other accounts would make a
        forbidden query look like an empty result.
        """
        if requested is None:
            return self.account_ids
        asked = frozenset(requested)
        if not asked:
            raise AuthorizationError(Reason.FORBIDDEN_SCOPE, "empty account filter")
        if self.account_ids is None:
            return asked
        forbidden = asked - self.account_ids
        if forbidden:
            raise AuthorizationError(
                Reason.FORBIDDEN_SCOPE,
                "one or more accounts are outside the caller's scope",
                count=len(forbidden),
            )
        return asked

    def cache_key(self) -> tuple:
        """Key that includes every dimension of the scope.

        P08-T-09: a cache keyed on less than this serves one tenant's rows to
        another, or one epoch's rows after a reset.
        """
        return (
            self.tenant_id,
            tuple(sorted(self.source_ids)) if self.source_ids else None,
            tuple(sorted(self.epoch_ids)) if self.epoch_ids else None,
            tuple(sorted(self.account_ids)) if self.account_ids else None,
            tuple(sorted(self.report_families)) if self.report_families else None,
            self.operator,
        )


@dataclass(frozen=True, slots=True)
class AuthContext:
    """An authenticated caller."""

    principal: str
    scope: Scope

    @property
    def tenant_id(self) -> str:
        return self.scope.tenant_id


@dataclass(frozen=True, slots=True)
class _TokenRecord:
    principal: str
    scope: Scope


class Authenticator:
    """Static bearer tokens mapped to scopes.

    Deliberately minimal: P08 is an internal service behind P06's auth
    boundary (section 2.2), and the thing worth testing here is the *scope*
    logic, not a second identity provider.
    """

    def __init__(self, tokens: Mapping[str, _TokenRecord] | None = None) -> None:
        self._tokens: dict[str, _TokenRecord] = dict(tokens or {})

    def register(self, token: str, *, principal: str, scope: Scope) -> None:
        if not token or len(token) < 8:
            raise AuthenticationError(
                Reason.UNAUTHENTICATED, "a token must be at least 8 characters"
            )
        self._tokens[token] = _TokenRecord(principal=principal, scope=scope)

    def authenticate(self, token: str | None) -> AuthContext:
        if not token:
            raise AuthenticationError(Reason.UNAUTHENTICATED, "a bearer token is required")
        for candidate, record in self._tokens.items():
            if hmac.compare_digest(candidate, token):
                return AuthContext(principal=record.principal, scope=record.scope)
        raise AuthenticationError(Reason.UNAUTHENTICATED, "unknown token")

    @staticmethod
    def from_settings(settings) -> "Authenticator":
        """Build the two standard principals from configuration.

        The reader gets no operator scope and the operator is a separate
        token, because section 8.2 requires mutations to run under a distinct
        audited role.
        """
        auth = Authenticator()
        mapping = settings.tenant_mapping()
        tenants = frozenset(mapping.values())
        if settings.reader_token:
            for tenant in tenants:
                auth.register(
                    settings.reader_token if len(tenants) == 1
                    else f"{settings.reader_token}:{tenant}",
                    principal=f"reader@{tenant}",
                    scope=Scope(tenant_id=tenant, operator=False),
                )
        if settings.operator_token:
            for tenant in tenants:
                auth.register(
                    settings.operator_token if len(tenants) == 1
                    else f"{settings.operator_token}:{tenant}",
                    principal=f"operator@{tenant}",
                    scope=Scope(tenant_id=tenant, operator=True),
                )
        return auth
