"""RFC 9457 Problem Details and the stable public error taxonomy.

The catalog below is the contract: design doc section 4.7 is encoded as data so
tests assert the mapping instead of re-deriving it. A problem body carries only
what a client needs to act. It never carries SQL, stack traces, tokens, account
identifiers or internal hostnames.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Mapping

__all__ = [
    "PROBLEM_TYPE_BASE",
    "PROBLEM_CATALOG",
    "ENGINE_REASON_TO_PUBLIC_CODE",
    "ProblemSpec",
    "ApiProblem",
    "problem_body",
    "public_code_for_reasons",
]

# Documentation origin for `type`. Must be replaced with a controlled origin
# before any real deployment; it is intentionally a reserved test domain.
PROBLEM_TYPE_BASE: Final = "https://docs.example.test/problems"


@dataclass(frozen=True, slots=True)
class ProblemSpec:
    status: int
    title: str
    slug: str
    retryable: bool = False


def _spec(status: int, title: str, slug: str, retryable: bool = False) -> ProblemSpec:
    return ProblemSpec(status=status, title=title, slug=slug, retryable=retryable)


PROBLEM_CATALOG: Final[Mapping[str, ProblemSpec]] = {
    # ---- identity and authorization
    "UNAUTHENTICATED": _spec(401, "Authentication required", "unauthenticated"),
    "ACCESS_DENIED": _spec(403, "Access denied for this principal", "access-denied"),
    "RESOURCE_NOT_FOUND": _spec(404, "Resource not found", "resource-not-found"),
    # ---- request shape
    "METHOD_NOT_ALLOWED": _spec(405, "Method not allowed", "method-not-allowed"),
    "NOT_ACCEPTABLE": _spec(406, "No acceptable representation", "not-acceptable"),
    "REQUEST_TOO_LARGE": _spec(413, "Request body too large", "request-too-large"),
    "URI_TOO_LONG": _spec(414, "Request URI too long", "uri-too-long"),
    "UNSUPPORTED_MEDIA_TYPE": _spec(415, "Unsupported media type", "unsupported-media-type"),
    "INVALID_REQUEST": _spec(422, "Invalid request", "invalid-request"),
    "INVALID_PAYLOAD": _spec(422, "Invalid payload", "invalid-payload"),
    "INVALID_CURSOR": _spec(422, "Invalid pagination cursor", "invalid-cursor"),
    "INVALID_TIME_WINDOW": _spec(422, "Invalid time window", "invalid-time-window"),
    "LIMIT_EXCEEDED": _spec(422, "Requested page limit exceeded", "limit-exceeded"),
    "ACCOUNT_SCOPE_TOO_LARGE": _spec(422, "Too many accounts requested", "account-scope-too-large"),
    "HISTORICAL_WINDOW_TOO_LARGE": _spec(
        422, "Historical window too large for an interactive request", "historical-window-too-large"
    ),
    "RESULT_TOO_LARGE": _spec(422, "Result exceeds the response size budget", "result-too-large"),
    "UNSUPPORTED_CALC_MODE": _spec(
        422, "Calculation profile not supported", "unsupported-calc-mode"
    ),
    # ---- data quality conflicts
    "COMPOSITE_UNPROVEN": _spec(
        409, "Consistent composite result unavailable", "composite-unproven"
    ),
    "DATA_NOT_READY": _spec(409, "Required source data is not ready", "data-not-ready"),
    "EPOCH_CHANGED": _spec(409, "Source epoch changed", "epoch-changed", retryable=True),
    "CUT_CHANGED": _spec(409, "Data cut changed before delivery", "cut-changed", retryable=True),
    "SCAN_CHANGED": _spec(409, "Published scan changed", "scan-changed", retryable=True),
    "CURSOR_EXPIRED": _spec(409, "Pagination cursor expired", "cursor-expired", retryable=True),
    # ---- capacity and dependencies
    "RATE_LIMITED": _spec(429, "Principal rate limit exceeded", "rate-limited", retryable=True),
    "OVERLOADED": _spec(503, "Service admission capacity exhausted", "overloaded", retryable=True),
    "DB_POOL_BUSY": _spec(503, "Database pool busy", "db-pool-busy", retryable=True),
    "DB_UNAVAILABLE": _spec(503, "Source database unavailable", "db-unavailable", retryable=True),
    "AUDIT_UNAVAILABLE": _spec(
        503, "Mandatory audit sink unavailable", "audit-unavailable", retryable=True
    ),
    "NOT_READY": _spec(503, "Instance not ready", "not-ready", retryable=True),
    "INVALID_RISK_RESULT": _spec(502, "Upstream risk result failed its contract", "invalid-risk-result"),
    "DEADLINE_EXCEEDED": _spec(504, "Server deadline exceeded", "deadline-exceeded", retryable=True),
    "INTERNAL_ERROR": _spec(500, "Internal error", "internal-error"),
}

# Project 05 reason code -> P06 public code. Domain blockers keep a typed cause
# instead of collapsing into a blanket 500.
ENGINE_REASON_TO_PUBLIC_CODE: Final[Mapping[str, str]] = {
    "UNAUTHORIZED_SCOPE": "RESOURCE_NOT_FOUND",
    "EPOCH_CHANGED": "EPOCH_CHANGED",
    "SOURCE_NOT_ACTIVE": "DATA_NOT_READY",
    "P03_UNPUBLISHED": "DATA_NOT_READY",
    "DEAL_WATERMARK_UNPROVEN": "DATA_NOT_READY",
    "RECON_BLOCKING": "DATA_NOT_READY",
    "INCOMPATIBLE_CUT": "COMPOSITE_UNPROVEN",
    "QUOTE_CUT_UNPROVEN": "COMPOSITE_UNPROVEN",
    "STALE_MARK": "COMPOSITE_UNPROVEN",
    "FEE_COVERAGE_UNAVAILABLE": "COMPOSITE_UNPROVEN",
    "MONEY_FIELD_MISSING": "COMPOSITE_UNPROVEN",
    "FX_RATE_MISSING": "UNSUPPORTED_CALC_MODE",
    "UNSUPPORTED_CALC_MODE": "UNSUPPORTED_CALC_MODE",
    "DEAL_TYPE_UNCLASSIFIED": "DATA_NOT_READY",
    "INVALID_DECIMAL": "DATA_NOT_READY",
    "DATA_INTEGRITY_CONFLICT": "DATA_NOT_READY",
    "POLICY_VERSION_CHANGED": "DATA_NOT_READY",
    "OVERLOADED": "OVERLOADED",
    "DEADLINE_EXCEEDED": "DEADLINE_EXCEEDED",
    "DB_UNAVAILABLE": "DB_UNAVAILABLE",
    "AUDIT_PERSISTENCE_FAILED": "AUDIT_UNAVAILABLE",
}

# When several reasons arrive at once, the most specific blocker wins so the
# client is told the thing it can actually act on.
_REASON_PRIORITY: Final[tuple[str, ...]] = (
    "UNAUTHORIZED_SCOPE",
    "RECON_BLOCKING",
    "EPOCH_CHANGED",
    "SOURCE_NOT_ACTIVE",
    "P03_UNPUBLISHED",
    "DEAL_WATERMARK_UNPROVEN",
    "UNSUPPORTED_CALC_MODE",
    "FX_RATE_MISSING",
    "STALE_MARK",
    "QUOTE_CUT_UNPROVEN",
    "INCOMPATIBLE_CUT",
    "DATA_INTEGRITY_CONFLICT",
    "INVALID_DECIMAL",
    "POLICY_VERSION_CHANGED",
    "FEE_COVERAGE_UNAVAILABLE",
)


def public_code_for_reasons(reasons: tuple[str, ...]) -> str:
    """Pick the public code for a set of engine reason codes."""
    for candidate in _REASON_PRIORITY:
        if candidate in reasons:
            return ENGINE_REASON_TO_PUBLIC_CODE[candidate]
    for reason in reasons:
        mapped = ENGINE_REASON_TO_PUBLIC_CODE.get(reason)
        if mapped is not None:
            return mapped
    return "COMPOSITE_UNPROVEN"


@dataclass(slots=True)
class ApiProblem(Exception):
    """A deliberate, documented refusal.

    Raising this is how every layer says no. ``detail`` is public text, so it
    must stay free of internal identifiers.
    """

    code: str
    detail: str = ""
    reason_codes: tuple[str, ...] = ()
    headers: dict[str, str] = field(default_factory=dict)
    status_override: int | None = None

    def __post_init__(self) -> None:
        if self.code not in PROBLEM_CATALOG:
            raise KeyError(f"unknown problem code: {self.code}")
        Exception.__init__(self, f"{self.code}: {self.detail}")

    @property
    def spec(self) -> ProblemSpec:
        return PROBLEM_CATALOG[self.code]

    @property
    def status(self) -> int:
        return self.status_override or self.spec.status

    @property
    def retryable(self) -> bool:
        return self.spec.retryable


def problem_body(problem: ApiProblem, request_id: str) -> dict[str, object]:
    """Build the RFC 9457 member set for a problem response."""
    spec = problem.spec
    return {
        "type": f"{PROBLEM_TYPE_BASE}/{spec.slug}",
        "title": spec.title,
        "status": problem.status,
        "detail": problem.detail or spec.title,
        "instance": f"urn:request:{request_id}",
        "code": problem.code,
        "request_id": request_id,
        "retryable": problem.retryable,
        "reason_codes": list(problem.reason_codes),
    }
