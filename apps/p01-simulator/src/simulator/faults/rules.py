"""Fault rule model, validation and persistence.

Scope boundary, stated once so it is not quietly widened later: this module
configures **response-layer** faults only - HTTP status codes, delays and
mutations of the outgoing payload (layers F2 and F3 of section 6.1).

Transport faults (TCP reset, half-open connections, DNS failure) and
storage/process faults (disk full, read-only mount, SIGKILL) are *not*
configurable here. A middleware cannot produce a real TCP reset, and a fault
rule that claimed to would produce a test that passes while proving nothing
(section 6.4, WF-119). Those live in the proxy harness and in
:mod:`simulator.persistence.failpoints` respectively.

Every mutating mode carries mandatory bounds - ``max_activations``,
``ttl_seconds`` and, for delays, a ceiling - so a mistyped rule cannot wedge
the service for hours (WF-106).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from ..canonical import format_timestamp, parse_timestamp
from ..domain.accounting import parse_decimal
from ..domain.models import ValidationError


class FaultLayer(StrEnum):
    RESPONSE = "response"


class FaultMode(StrEnum):
    HTTP_ERROR = "http_error"
    DELAY = "delay_before_response"
    DROP_ONE_ITEM = "drop_one_item"
    DUPLICATE_ONE_ITEM = "duplicate_one_item"
    SHUFFLE_ITEMS = "shuffle_items"
    OMIT_FIELD = "omit_field"
    REPLACE_FIELD_VALUE = "replace_field_value"
    MALFORMED_JSON = "malformed_json"
    WRONG_CONTENT_TYPE = "wrong_content_type"
    TRUNCATE_RESPONSE = "truncate_response"
    STALE_SNAPSHOT = "stale_snapshot"


#: Modes that alter the response body or framing. At most one may apply to a
#: single request: composing two body mutations produces a state the test
#: author did not describe and cannot assert on (section 6.3).
TERMINAL_MODES = frozenset(
    {
        FaultMode.HTTP_ERROR,
        FaultMode.DROP_ONE_ITEM,
        FaultMode.DUPLICATE_ONE_ITEM,
        FaultMode.SHUFFLE_ITEMS,
        FaultMode.OMIT_FIELD,
        FaultMode.REPLACE_FIELD_VALUE,
        FaultMode.MALFORMED_JSON,
        FaultMode.WRONG_CONTENT_TYPE,
        FaultMode.TRUNCATE_RESPONSE,
        FaultMode.STALE_SNAPSHOT,
    }
)

#: Status codes a fault may emit. Restricted to codes the error contract
#: already defines so a connector's retry logic is exercised, not confused.
ALLOWED_FAULT_STATUSES = frozenset({429, 500, 502, 503, 504})

#: Never faulted. Liveness must stay answerable while faults are active, and
#: the control plane must stay reachable to turn them off (section 6.3).
PROTECTED_PATH_PREFIXES: tuple[str, ...] = ("/health", "/admin", "/openapi.json", "/docs")


class TriggerKind(StrEnum):
    ALWAYS = "always"
    NTH_MATCHING_REQUEST = "nth_matching_request"
    PROBABILITY = "probability"


@dataclass(frozen=True, slots=True)
class FaultScope:
    """What a rule applies to. ``None`` means "do not constrain on this".

    ``run_id`` is recorded but is deliberately *not* a matching dimension: the
    middleware runs before the handler resolves a run, and reading the active
    run from storage on every request would put a database round trip in front
    of the data plane. It is used instead to expire a run's rules on reset.
    """

    method: str | None = None
    path: str | None = None
    account_id: str | None = None
    symbol: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "path": self.path,
            "account_id": self.account_id,
            "symbol": self.symbol,
        }


@dataclass(frozen=True, slots=True)
class FaultTrigger:
    kind: TriggerKind
    n: int = 0
    probability: Decimal = Decimal(0)
    max_activations: int = 1
    ttl_seconds: int = 600

    def to_payload(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "n": self.n,
            "probability": str(self.probability),
            "max_activations": self.max_activations,
            "ttl_seconds": self.ttl_seconds,
        }


@dataclass(frozen=True, slots=True)
class FaultAction:
    mode: FaultMode
    parameters: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {"mode": self.mode.value, "parameters": self.parameters}


@dataclass(slots=True)
class FaultRule:
    rule_id: str
    run_id: str | None
    enabled: bool
    layer: FaultLayer
    priority: int
    scope: FaultScope
    trigger: FaultTrigger
    action: FaultAction
    expires_at: datetime | None
    created_at: datetime
    #: Requests whose scope matched *while the rule was live*, whether or not
    #: it fired. An exhausted or expired rule is skipped before matching, so
    #: this counts considerations rather than every request that would have
    #: matched had the rule still been active - which also keeps the hot path
    #: free of dead rules (WF-099).
    match_count: int = 0
    #: Requests the rule actually altered.
    activations: int = 0

    @property
    def exhausted(self) -> bool:
        return self.activations >= self.trigger.max_activations

    def expired(self, now: datetime | None = None) -> bool:
        if self.expires_at is None:
            return False
        return (now or datetime.now(UTC)) >= self.expires_at

    def to_payload(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "run_id": self.run_id,
            "enabled": self.enabled,
            "layer": self.layer.value,
            "priority": self.priority,
            "scope": self.scope.to_payload(),
            "trigger": self.trigger.to_payload(),
            "action": self.action.to_payload(),
            "expires_at": format_timestamp(self.expires_at) if self.expires_at else None,
            "created_at": format_timestamp(self.created_at),
            "match_count": self.match_count,
            "activations": self.activations,
            "exhausted": self.exhausted,
        }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def build_rule(
    payload: dict[str, Any],
    *,
    rule_id: str,
    run_id: str | None,
    max_delay_ms: int,
    max_ttl_seconds: int = 86_400,
) -> FaultRule:
    """Validate an admin payload into a bounded rule.

    Every bound is checked here rather than at activation time: a rule that
    would be refused later is better refused now, while an operator is still
    looking at the response.
    """
    layer_value = str(payload.get("layer", FaultLayer.RESPONSE.value))
    if layer_value != FaultLayer.RESPONSE.value:
        raise ValidationError(
            f"layer {layer_value!r} is not configurable through this API",
            details={
                "field": "layer",
                "supported": [FaultLayer.RESPONSE.value],
                "reason": "transport and storage faults require the proxy or container harness",
            },
        )

    scope_payload = payload.get("scope") or {}
    if not isinstance(scope_payload, dict):
        raise ValidationError("scope must be an object", details={"field": "scope"})
    path = scope_payload.get("path")
    if path is not None:
        path = str(path)
        if not path.startswith("/"):
            raise ValidationError("scope.path must start with '/'", details={"field": "scope.path"})
        if path.startswith(PROTECTED_PATH_PREFIXES):
            raise ValidationError(
                "faults may not target health or control-plane paths",
                details={"field": "scope.path", "protected": list(PROTECTED_PATH_PREFIXES)},
            )
    method = scope_payload.get("method")
    if method is not None:
        method = str(method).upper()
        if method not in {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValidationError("scope.method is not a valid HTTP method", details={"field": "scope.method"})
    scope = FaultScope(
        method=method,
        path=path,
        account_id=_optional_str(scope_payload.get("account_id"), "scope.account_id"),
        symbol=_optional_str(scope_payload.get("symbol"), "scope.symbol"),
    )

    trigger = _build_trigger(payload.get("trigger") or {}, max_ttl_seconds=max_ttl_seconds)
    action = _build_action(payload.get("action") or {}, max_delay_ms=max_delay_ms)

    priority = payload.get("priority", 100)
    if not isinstance(priority, int) or isinstance(priority, bool) or not 0 <= priority <= 10_000:
        raise ValidationError(
            "priority must be an integer between 0 and 10000", details={"field": "priority"}
        )

    now = datetime.now(UTC)
    return FaultRule(
        rule_id=rule_id,
        run_id=run_id,
        enabled=bool(payload.get("enabled", True)),
        layer=FaultLayer.RESPONSE,
        priority=priority,
        scope=scope,
        trigger=trigger,
        action=action,
        expires_at=now + timedelta(seconds=trigger.ttl_seconds),
        created_at=now,
    )


def _optional_str(value: Any, field: str) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text or len(text) > 64:
        raise ValidationError(f"{field} must be 1-64 characters", details={"field": field})
    return text


def _build_trigger(payload: dict[str, Any], *, max_ttl_seconds: int) -> FaultTrigger:
    if not isinstance(payload, dict):
        raise ValidationError("trigger must be an object", details={"field": "trigger"})
    kind_value = str(payload.get("kind", TriggerKind.ALWAYS.value))
    try:
        kind = TriggerKind(kind_value)
    except ValueError as exc:
        raise ValidationError(
            f"trigger.kind {kind_value!r} is not supported",
            details={"field": "trigger.kind", "supported": [item.value for item in TriggerKind]},
        ) from exc

    n = payload.get("n", 0)
    if kind is TriggerKind.NTH_MATCHING_REQUEST:
        if not isinstance(n, int) or isinstance(n, bool) or n < 1 or n > 1_000_000:
            raise ValidationError(
                "trigger.n must be an integer >= 1 for nth_matching_request",
                details={"field": "trigger.n"},
            )
    else:
        n = 0

    probability = Decimal(0)
    if kind is TriggerKind.PROBABILITY:
        probability = parse_decimal(payload.get("probability", "0"), field="trigger.probability")
        if not 0 <= probability <= 1:
            raise ValidationError(
                "trigger.probability must be between 0 and 1",
                details={"field": "trigger.probability"},
            )

    max_activations = payload.get("max_activations", 1)
    if (
        not isinstance(max_activations, int)
        or isinstance(max_activations, bool)
        or not 1 <= max_activations <= 1_000_000
    ):
        raise ValidationError(
            "trigger.max_activations must be an integer between 1 and 1000000",
            details={"field": "trigger.max_activations"},
        )

    ttl_seconds = payload.get("ttl_seconds", 600)
    if (
        not isinstance(ttl_seconds, int)
        or isinstance(ttl_seconds, bool)
        or not 1 <= ttl_seconds <= max_ttl_seconds
    ):
        raise ValidationError(
            f"trigger.ttl_seconds must be an integer between 1 and {max_ttl_seconds}",
            details={"field": "trigger.ttl_seconds", "maximum": max_ttl_seconds},
        )

    return FaultTrigger(
        kind=kind,
        n=n,
        probability=probability,
        max_activations=max_activations,
        ttl_seconds=ttl_seconds,
    )


def _build_action(payload: dict[str, Any], *, max_delay_ms: int) -> FaultAction:
    if not isinstance(payload, dict):
        raise ValidationError("action must be an object", details={"field": "action"})
    mode_value = str(payload.get("mode", ""))
    try:
        mode = FaultMode(mode_value)
    except ValueError as exc:
        raise ValidationError(
            f"action.mode {mode_value!r} is not supported",
            details={"field": "action.mode", "supported": [item.value for item in FaultMode]},
        ) from exc
    raw = payload.get("parameters") or {}
    if not isinstance(raw, dict):
        raise ValidationError("action.parameters must be an object", details={"field": "action.parameters"})
    parameters = dict(raw)

    if mode is FaultMode.HTTP_ERROR:
        status = parameters.get("status", 503)
        if status not in ALLOWED_FAULT_STATUSES:
            raise ValidationError(
                "action.parameters.status is not an allowed fault status",
                details={"field": "action.parameters.status", "allowed": sorted(ALLOWED_FAULT_STATUSES)},
            )
        retry_after = parameters.get("retry_after_seconds")
        if retry_after is not None:
            if not isinstance(retry_after, int) or isinstance(retry_after, bool) or not 0 <= retry_after <= 3_600:
                raise ValidationError(
                    "retry_after_seconds must be an integer between 0 and 3600",
                    details={"field": "action.parameters.retry_after_seconds"},
                )
        parameters["status"] = int(status)
    elif mode is FaultMode.DELAY:
        delay_ms = parameters.get("delay_ms", 0)
        if not isinstance(delay_ms, int) or isinstance(delay_ms, bool) or delay_ms < 0:
            raise ValidationError(
                "delay_ms must be a non-negative integer", details={"field": "action.parameters.delay_ms"}
            )
        if delay_ms > max_delay_ms:
            raise ValidationError(
                "delay_ms exceeds the configured maximum",
                details={"field": "action.parameters.delay_ms", "maximum": max_delay_ms},
            )
        parameters["delay_ms"] = delay_ms
    elif mode in {FaultMode.DROP_ONE_ITEM, FaultMode.DUPLICATE_ONE_ITEM}:
        selection = str(parameters.get("selection", "last"))
        if selection not in {"first", "last"}:
            raise ValidationError(
                "selection must be 'first' or 'last'",
                details={"field": "action.parameters.selection"},
            )
        parameters["selection"] = selection
    elif mode in {FaultMode.OMIT_FIELD, FaultMode.REPLACE_FIELD_VALUE}:
        field_name = parameters.get("field")
        if not field_name or not isinstance(field_name, str) or len(field_name) > 64:
            raise ValidationError(
                "field is required and must be 1-64 characters",
                details={"field": "action.parameters.field"},
            )
        selection = str(parameters.get("selection", "first"))
        if selection not in {"first", "last", "all"}:
            raise ValidationError(
                "selection must be 'first', 'last' or 'all'",
                details={"field": "action.parameters.selection"},
            )
        parameters["selection"] = selection
        if mode is FaultMode.REPLACE_FIELD_VALUE and "value" not in parameters:
            raise ValidationError(
                "value is required for replace_field_value",
                details={"field": "action.parameters.value"},
            )
    elif mode is FaultMode.TRUNCATE_RESPONSE:
        keep = parameters.get("keep_bytes", 32)
        if not isinstance(keep, int) or isinstance(keep, bool) or not 0 <= keep <= 1_000_000:
            raise ValidationError(
                "keep_bytes must be an integer between 0 and 1000000",
                details={"field": "action.parameters.keep_bytes"},
            )
        parameters["keep_bytes"] = keep
    elif mode is FaultMode.STALE_SNAPSHOT:
        delta = parameters.get("seq_delta", 1)
        if not isinstance(delta, int) or isinstance(delta, bool) or not 1 <= delta <= 1_000_000_000:
            raise ValidationError(
                "seq_delta must be an integer >= 1",
                details={"field": "action.parameters.seq_delta"},
            )
        parameters["seq_delta"] = delta

    return FaultAction(mode=mode, parameters=parameters)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def insert_rule(conn: sqlite3.Connection, rule: FaultRule) -> None:
    conn.execute(
        "INSERT INTO fault_rules (rule_id, run_id, enabled, layer, priority, scope_json, "
        " trigger_json, action_json, match_count, activations, expires_at, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            rule.rule_id,
            rule.run_id,
            int(rule.enabled),
            rule.layer.value,
            rule.priority,
            json.dumps(rule.scope.to_payload(), separators=(",", ":"), sort_keys=True),
            json.dumps(rule.trigger.to_payload(), separators=(",", ":"), sort_keys=True),
            json.dumps(rule.action.to_payload(), separators=(",", ":"), sort_keys=True),
            rule.match_count,
            rule.activations,
            format_timestamp(rule.expires_at) if rule.expires_at else None,
            format_timestamp(rule.created_at),
        ),
    )


def delete_rule(conn: sqlite3.Connection, rule_id: str) -> int:
    return conn.execute("DELETE FROM fault_rules WHERE rule_id = ?", (rule_id,)).rowcount


def delete_all_rules(conn: sqlite3.Connection) -> int:
    return conn.execute("DELETE FROM fault_rules").rowcount


def update_counters(conn: sqlite3.Connection, rule: FaultRule) -> None:
    conn.execute(
        "UPDATE fault_rules SET match_count = ?, activations = ?, enabled = ? WHERE rule_id = ?",
        (rule.match_count, rule.activations, int(rule.enabled), rule.rule_id),
    )


def load_rules(conn: sqlite3.Connection) -> list[FaultRule]:
    rules: list[FaultRule] = []
    for row in conn.execute("SELECT * FROM fault_rules ORDER BY priority, rule_id"):
        scope_payload = json.loads(row["scope_json"])
        trigger_payload = json.loads(row["trigger_json"])
        action_payload = json.loads(row["action_json"])
        rules.append(
            FaultRule(
                rule_id=row["rule_id"],
                run_id=row["run_id"],
                enabled=bool(row["enabled"]),
                layer=FaultLayer(row["layer"]),
                priority=int(row["priority"]),
                scope=FaultScope(
                    method=scope_payload.get("method"),
                    path=scope_payload.get("path"),
                    account_id=scope_payload.get("account_id"),
                    symbol=scope_payload.get("symbol"),
                ),
                trigger=FaultTrigger(
                    kind=TriggerKind(trigger_payload["kind"]),
                    n=int(trigger_payload.get("n", 0)),
                    probability=Decimal(str(trigger_payload.get("probability", "0"))),
                    max_activations=int(trigger_payload.get("max_activations", 1)),
                    ttl_seconds=int(trigger_payload.get("ttl_seconds", 600)),
                ),
                action=FaultAction(
                    mode=FaultMode(action_payload["mode"]),
                    parameters=dict(action_payload.get("parameters") or {}),
                ),
                expires_at=parse_timestamp(row["expires_at"]) if row["expires_at"] else None,
                created_at=parse_timestamp(row["created_at"]),
                match_count=int(row["match_count"]),
                activations=int(row["activations"]),
            )
        )
    return rules
