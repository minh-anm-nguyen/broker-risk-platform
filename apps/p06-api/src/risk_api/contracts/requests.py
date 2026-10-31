"""Strict request contracts.

Two rules drive this module:

* An input the service does not understand is an error, not something to
  ignore. A silently dropped ``bypass_cut=true`` would be indistinguishable
  from a honoured one.
* Parsing happens before any expensive work, and within hard size/shape bounds.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Annotated, Any, Final, Iterable, Literal, Sequence

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator, model_validator

from .problem_details import ApiProblem

__all__ = [
    "ACCOUNT_ID_PATTERN",
    "SOURCE_ID_PATTERN",
    "SYMBOL_PATTERN",
    "AccountId",
    "SourceId",
    "Symbol",
    "ComponentName",
    "RiskCalculationRequest",
    "parse_components",
    "parse_rfc3339_utc",
    "parse_utc_date",
    "enforce_query_allowlist",
    "decode_json_body",
    "json_depth",
]

ACCOUNT_ID_PATTERN: Final = r"^[0-9A-Za-z_-]{1,32}$"
SOURCE_ID_PATTERN: Final = r"^[a-z0-9][a-z0-9-]{0,62}$"
SYMBOL_PATTERN: Final = r"^[A-Z0-9._]{1,24}$"

# Account identifiers keep their exact string form: "00042" is not 42.
AccountId = Annotated[str, StringConstraints(pattern=ACCOUNT_ID_PATTERN)]
SourceId = Annotated[str, StringConstraints(pattern=SOURCE_ID_PATTERN)]
Symbol = Annotated[str, StringConstraints(pattern=SYMBOL_PATTERN)]
ComponentName = Literal["EXPOSURE", "REALIZED", "UNREALIZED"]

_VALID_COMPONENTS: Final = ("EXPOSURE", "REALIZED", "UNREALIZED")


def parse_components(raw: str | None, *, default: Sequence[str] = _VALID_COMPONENTS) -> tuple[str, ...]:
    """Parse the ``components`` query value into a canonical tuple.

    An unknown member fails the whole request; a partially understood
    enumeration must not trigger a partial computation.
    """
    if raw is None or raw == "":
        return tuple(default)
    parts = raw.split(",")
    if any(part == "" for part in parts):
        raise ApiProblem("INVALID_REQUEST", "components must not contain empty members")
    if any(part != part.strip() for part in parts):
        # Trimming would be a silent normalisation of something the caller
        # did not actually send.
        raise ApiProblem(
            "INVALID_REQUEST", "components must not carry surrounding whitespace"
        )
    unknown = sorted({part for part in parts if part not in _VALID_COMPONENTS})
    if unknown:
        raise ApiProblem(
            "INVALID_REQUEST",
            f"unknown components requested: {', '.join(unknown)}",
        )
    seen: list[str] = []
    for part in parts:
        if part not in seen:
            seen.append(part)
    # Canonical order keeps digests and cache keys stable regardless of input order.
    return tuple(component for component in _VALID_COMPONENTS if component in seen)


def parse_rfc3339_utc(value: str, *, field: str) -> datetime:
    """Parse a strict RFC3339 UTC instant with a mandatory trailing ``Z``."""
    if not isinstance(value, str) or not value.endswith("Z") or len(value) > 30:
        raise ApiProblem("INVALID_REQUEST", f"{field} must be an RFC3339 UTC instant ending in Z")
    if len(value) < 20 or value[10] != "T":
        # fromisoformat would accept a space separator; RFC3339 does not.
        raise ApiProblem("INVALID_REQUEST", f"{field} must separate date and time with T")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiProblem("INVALID_REQUEST", f"{field} is not a valid RFC3339 instant") from exc
    if parsed.tzinfo is None:
        raise ApiProblem("INVALID_REQUEST", f"{field} must carry the Z offset")
    return parsed.astimezone(timezone.utc)


def parse_utc_date(value: str, *, field: str) -> date:
    """Parse a strict ``YYYY-MM-DD`` calendar date."""
    if not isinstance(value, str) or len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise ApiProblem("INVALID_REQUEST", f"{field} must be formatted YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ApiProblem("INVALID_REQUEST", f"{field} is not a valid calendar date") from exc


def enforce_query_allowlist(query_keys: Iterable[str], allowed: frozenset[str]) -> None:
    """Reject any query key the endpoint does not declare."""
    unknown = sorted({key for key in query_keys if key not in allowed})
    if unknown:
        raise ApiProblem(
            "INVALID_REQUEST",
            f"unsupported query parameters: {', '.join(unknown)}",
        )


def json_depth(value: Any, *, _depth: int = 1) -> int:
    """Maximum nesting depth of a decoded JSON tree."""
    if isinstance(value, dict):
        if not value:
            return _depth
        return max(json_depth(item, _depth=_depth + 1) for item in value.values())
    if isinstance(value, list):
        if not value:
            return _depth
        return max(json_depth(item, _depth=_depth + 1) for item in value)
    return _depth


def _reject_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Object hook that refuses duplicate JSON member names.

    A body carrying ``allow_partial`` twice with opposing values is ambiguous.
    Taking the last value would let a caller smuggle a policy change past a
    reviewer who read the first one.
    """
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ApiProblem("INVALID_PAYLOAD", f"duplicate JSON member: {key}")
        seen[key] = value
    return seen


def _reject_constant(name: str) -> Any:
    """Refuse JSON extensions such as ``NaN`` and ``Infinity``."""
    raise ApiProblem("INVALID_PAYLOAD", f"non-finite JSON constant rejected: {name}")


def decode_json_body(raw: bytes, *, max_bytes: int, max_depth: int) -> Any:
    """Decode a request body under explicit size, encoding and depth bounds."""
    if len(raw) > max_bytes:
        raise ApiProblem("REQUEST_TOO_LARGE", "request body exceeds the configured limit")
    if not raw.strip():
        raise ApiProblem("INVALID_PAYLOAD", "request body must contain a JSON object")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ApiProblem("INVALID_PAYLOAD", "request body must be valid UTF-8") from exc
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_members,
            parse_constant=_reject_constant,
        )
    except ApiProblem:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise ApiProblem("INVALID_PAYLOAD", "request body is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise ApiProblem("INVALID_PAYLOAD", "request body must be a JSON object")
    if json_depth(decoded) > max_depth:
        raise ApiProblem("INVALID_PAYLOAD", "request body nesting is too deep")
    return decoded


class RiskCalculationRequest(BaseModel):
    """Body of ``POST /api/v1/risk-calculations``.

    ``account_ids`` is a requested *filter*. Authorization intersects it
    server-side; nothing here widens access.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    source_id: SourceId
    account_ids: list[AccountId]
    components: list[ComponentName]
    period_start_utc: str | None = None
    period_end_utc: str | None = None
    valuation_as_of: str | None = None
    allow_partial: bool = False
    risk_policy_version: str | None = None
    symbol_catalog_version: str | None = None
    epoch_id: str | None = None

    @field_validator("account_ids")
    @classmethod
    def _accounts_non_empty(cls, value: list[str]) -> list[str]:
        if not value:
            # An empty list never means every account in the broker.
            raise ValueError("account_ids must name at least one account")
        if len(set(value)) != len(value):
            raise ValueError("account_ids must not repeat an account")
        return value

    @field_validator("components")
    @classmethod
    def _components_non_empty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("components must name at least one component")
        if len(set(value)) != len(value):
            raise ValueError("components must not repeat a member")
        return value

    @model_validator(mode="after")
    def _period_required_for_realized(self) -> "RiskCalculationRequest":
        if "REALIZED" in self.components:
            if self.period_start_utc is None or self.period_end_utc is None:
                raise ValueError("REALIZED requires period_start_utc and period_end_utc")
        return self

    def canonical_accounts(self) -> tuple[str, ...]:
        return tuple(sorted(self.account_ids))

    def canonical_components(self) -> tuple[str, ...]:
        return tuple(component for component in _VALID_COMPONENTS if component in self.components)
