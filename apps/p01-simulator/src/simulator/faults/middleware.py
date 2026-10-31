"""ASGI middleware that applies a fault decision to a data-plane response.

The invariant this file exists to preserve: **a response-layer fault mutates a
copy of the outgoing payload and never the canonical store** (ADR-006,
INV-12). Everything here operates on bytes already produced by the handler.
Nothing in this module can reach the database.

Two implementation notes that matter:

* The delay is ``await asyncio.sleep``, never ``time.sleep``. A blocking sleep
  in an async handler would stall the event loop and make every other request,
  including ``/health/live``, slow for a reason unrelated to the fault being
  tested (section 6.2, WF-093).
* An activation is only counted once the fault was actually applied. Counting
  at selection time would report a fault as injected when the handler happened
  to return an error and the mutation never happened - and a chaos result
  whose activation count is a guess proves nothing (WF-112, WF-119).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Iterable
from urllib.parse import parse_qs

from ..observability.logging import get_logger
from ..observability.metrics import Metrics, NullMetrics
from ..settings import Settings
from .matcher import FaultEngine, RequestContext
from .rules import PROTECTED_PATH_PREFIXES, FaultAction, FaultMode

logger = get_logger("fault_middleware")

_JSON_CONTENT_TYPE = b"application/json"


class FaultInjectionMiddleware:
    """Pure ASGI middleware. Buffers the response only when a fault applies."""

    def __init__(
        self,
        app: Any,
        *,
        engine_getter: Callable[[], FaultEngine],
        settings: Settings,
        metrics_getter: Callable[[], Metrics | NullMetrics] | None = None,
    ) -> None:
        self.app = app
        # Resolved per request rather than captured: the engine is built in the
        # application lifespan, which runs after the middleware stack is
        # assembled.
        self._engine_getter = engine_getter
        self._settings = settings
        self._metrics_getter = metrics_getter or (lambda: NullMetrics())

    @property
    def _engine(self) -> FaultEngine:
        return self._engine_getter()

    @property
    def _metrics(self) -> Metrics | NullMetrics:
        return self._metrics_getter()

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path: str = scope.get("path", "")
        if path.startswith(PROTECTED_PATH_PREFIXES):
            # Health and the control plane are never faulted: liveness has to
            # stay answerable, and the operator has to be able to disarm.
            await self.app(scope, receive, send)
            return

        decision = self._engine.select(self._context(scope, path))
        if decision.is_noop:
            await self.app(scope, receive, send)
            return

        if decision.delay_ms and decision.delay_rule_id:
            await asyncio.sleep(decision.delay_ms / 1000.0)
            self._engine.commit(decision.delay_rule_id)

        action = decision.action
        if action is None:
            await self.app(scope, receive, send)
            return

        if action.mode is FaultMode.HTTP_ERROR:
            await self._send_injected_error(scope, send, action)
            if decision.rule_id:
                self._engine.commit(decision.rule_id)
            return

        await self._mutate_response(scope, receive, send, action, decision.rule_id)

    # -- context -----------------------------------------------------------

    @staticmethod
    def _context(scope: dict[str, Any], path: str) -> RequestContext:
        query = parse_qs(
            (scope.get("query_string") or b"").decode("latin-1"), keep_blank_values=False
        )
        return RequestContext(
            method=scope.get("method", "GET"),
            path=path,
            account_id=(query.get("account_id") or [None])[0],
            symbol=(query.get("symbol") or [None])[0],
        )

    # -- injected error ----------------------------------------------------

    async def _send_injected_error(
        self, scope: dict[str, Any], send: Any, action: FaultAction
    ) -> None:
        from ..api.errors import problem_payload

        status = int(action.parameters.get("status", 503))
        retry_after = action.parameters.get("retry_after_seconds")
        code = {
            429: "RATE_LIMITED",
            500: "INTERNAL_ERROR",
            502: "SIMULATED_UPSTREAM_ERROR",
            503: "SERVICE_NOT_READY",
            504: "SIMULATED_UPSTREAM_TIMEOUT",
        }.get(status, "INTERNAL_ERROR")
        payload = problem_payload(
            code=code,
            message="injected fault response",
            request_id=scope.get("state", {}).get("request_id"),
            retryable=status in {429, 500, 502, 503, 504},
            details={"injected": True},
        )
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", _JSON_CONTENT_TYPE),
            (b"content-length", str(len(body)).encode("ascii")),
            (b"cache-control", b"no-store"),
            (b"x-fault-injected", b"1"),
        ]
        if retry_after is not None:
            headers.append((b"retry-after", str(int(retry_after)).encode("ascii")))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body, "more_body": False})

    # -- body mutation -----------------------------------------------------

    async def _mutate_response(
        self,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
        action: FaultAction,
        rule_id: str | None,
    ) -> None:
        status = 200
        headers: list[tuple[bytes, bytes]] = []
        chunks: list[bytes] = []
        started = False

        async def capture(message: dict[str, Any]) -> None:
            nonlocal status, headers, started
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = list(message.get("headers") or [])
                started = True
                return
            if message["type"] == "http.response.body":
                chunks.append(message.get("body") or b"")
                return
            await send(message)

        await self.app(scope, receive, capture)

        if not started:  # pragma: no cover - a handler that sent nothing
            return

        body = b"".join(chunks)
        applied = False
        if 200 <= status < 300:
            try:
                body, headers = _apply_action(action, body, headers)
                applied = True
            except Exception as exc:  # noqa: BLE001 - a fault must not 500 the service
                self._metrics.fault_errors.labels(mode=action.mode.value).inc()
                logger.error(
                    "fault engine failed to mutate a response; sending it unchanged",
                    extra={"mode": action.mode.value, "rule_id": rule_id},
                    exc_info=exc,
                )
                body = b"".join(chunks)
        else:
            # The handler already failed for its own reasons. Mutating that
            # response would make the fault report a lie.
            logger.info(
                "fault not applied: downstream response was not a success",
                extra={"mode": action.mode.value, "rule_id": rule_id, "status": status},
            )

        if applied:
            headers.append((b"x-fault-injected", b"1"))
            if rule_id:
                self._engine.commit(rule_id)

        headers = _set_content_length(headers, len(body))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body, "more_body": False})


# ---------------------------------------------------------------------------
# Payload mutations
# ---------------------------------------------------------------------------


def _apply_action(
    action: FaultAction, body: bytes, headers: list[tuple[bytes, bytes]]
) -> tuple[bytes, list[tuple[bytes, bytes]]]:
    mode = action.mode
    parameters = action.parameters

    if mode is FaultMode.WRONG_CONTENT_TYPE:
        return body, _replace_header(headers, b"content-type", b"text/plain; charset=utf-8")

    if mode is FaultMode.MALFORMED_JSON:
        # Deterministically break the JSON grammar without changing the length
        # class of the payload, so the client hits a parse error rather than a
        # schema error.
        stripped = body.rstrip()
        broken = (stripped[:-1] + b',}') if stripped.endswith(b"}") else stripped + b"{"
        return broken, headers

    if mode is FaultMode.TRUNCATE_RESPONSE:
        keep = int(parameters.get("keep_bytes", 32))
        # Content-length is corrected to the truncated size so the fault is
        # observable in-process. A genuine wire-level framing violation needs
        # the proxy harness (section 6.4).
        return body[:keep], headers

    document = json.loads(body.decode("utf-8"))
    if not isinstance(document, dict):
        return body, headers

    if mode is FaultMode.STALE_SNAPSHOT:
        delta = int(parameters.get("seq_delta", 1))
        if isinstance(document.get("snapshot_seq"), int):
            document["snapshot_seq"] = max(0, document["snapshot_seq"] - delta)
    else:
        items = document.get("items")
        if isinstance(items, list) and items:
            document["items"] = _mutate_items(mode, parameters, items)

    return json.dumps(document, separators=(",", ":")).encode("utf-8"), headers


def _mutate_items(mode: FaultMode, parameters: dict[str, Any], items: list[Any]) -> list[Any]:
    selection = str(parameters.get("selection", "last"))
    index = 0 if selection == "first" else len(items) - 1

    if mode is FaultMode.DROP_ONE_ITEM:
        return [item for position, item in enumerate(items) if position != index]
    if mode is FaultMode.DUPLICATE_ONE_ITEM:
        duplicated = list(items)
        duplicated.insert(index, items[index])
        return duplicated
    if mode is FaultMode.SHUFFLE_ITEMS:
        # Reversal rather than a random shuffle: the reorder is observable and
        # identical on every replay, which is what a reproducible fault needs.
        return list(reversed(items))
    if mode is FaultMode.OMIT_FIELD:
        field = str(parameters["field"])
        return _edit_items(items, selection, lambda item: _without(item, field))
    if mode is FaultMode.REPLACE_FIELD_VALUE:
        field = str(parameters["field"])
        value = parameters.get("value")
        return _edit_items(items, selection, lambda item: _with(item, field, value))
    return items


def _edit_items(
    items: list[Any], selection: str, edit: Callable[[Any], Any]
) -> list[Any]:
    if selection == "all":
        return [edit(item) for item in items]
    target = 0 if selection == "first" else len(items) - 1
    return [edit(item) if position == target else item for position, item in enumerate(items)]


def _without(item: Any, field: str) -> Any:
    if not isinstance(item, dict):
        return item
    clone = dict(item)
    clone.pop(field, None)
    return clone


def _with(item: Any, field: str, value: Any) -> Any:
    if not isinstance(item, dict):
        return item
    clone = dict(item)
    clone[field] = value
    return clone


def _replace_header(
    headers: list[tuple[bytes, bytes]], name: bytes, value: bytes
) -> list[tuple[bytes, bytes]]:
    filtered = [(key, item) for key, item in headers if key.lower() != name]
    filtered.append((name, value))
    return filtered


def _set_content_length(
    headers: list[tuple[bytes, bytes]], length: int
) -> list[tuple[bytes, bytes]]:
    return _replace_header(headers, b"content-length", str(length).encode("ascii"))
