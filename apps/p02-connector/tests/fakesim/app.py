"""A contract-fidelity double of the Project 01 data plane, plus fault control.

**What this is and is not.** It is a test double written against the Project 01
``v1`` contract as published by that project's ``api/data.py``,
``api/schemas.py``, ``api/errors.py`` and ``security/auth.py``: the same
envelope fields, the same error codes and statuses, the same money-as-string
rule, the same exclusive ``after_seq``, the same no-clamp limit policy, the
same signed opaque cursor and the same 410 pair. It is **not** the Project 01
service. Evidence gathered against it supports the connector's behaviour
against that contract; it does not demonstrate interoperability with the real
process, and the scorecard records that distinction rather than blurring it
(section 14.1: evidence levels do not add up).

It is a real ASGI app served over a real TCP socket, which matters: the
connector's driver is synchronous and must perform genuine blocking socket
I/O, so an in-process transport would quietly remove the thing under test.

The fault surface lives under ``/control`` rather than ``/admin``, and the
connector's own configuration refuses a base URL with a path, so the connector
cannot reach it even by mistake. Faults are counted, and a test that asserts a
fault *fired* is the difference between a pass and a vacuous pass (WF2-P-03).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response

from .dataset import Dataset, DatasetSpec, build_dataset

__all__ = ["FakeSimulator", "create_fake_simulator", "DATA_TOKEN", "ADMIN_TOKEN"]

DATA_TOKEN = "data-token-canary-AAAA1111"
ADMIN_TOKEN = "admin-token-canary-BBBB2222"
_CURSOR_KEY = b"fake-simulator-cursor-signing-key"

#: Statuses a fault may emit, matching the Project 01 allowlist plus the two
#: snapshot-lifecycle codes, which that project produces from real state
#: rather than from a fault rule. Being able to inject them directly is what
#: lets a test hit page 18 of a scan with a 410 deterministically.
_FAULT_STATUSES = frozenset({410, 429, 500, 502, 503, 504})

_ERROR_STATUS: dict[str, int] = {
    "INVALID_CURSOR": 400,
    "INVALID_FILTER": 400,
    "UNAUTHORIZED": 401,
    "FORBIDDEN": 403,
    "NOT_FOUND": 404,
    "SNAPSHOT_EXPIRED": 410,
    "RUN_REPLACED": 410,
    "PAYLOAD_TOO_LARGE": 413,
    "VALIDATION_FAILED": 422,
    "RATE_LIMITED": 429,
    "INTERNAL_ERROR": 500,
    "SERVICE_NOT_READY": 503,
    "STORAGE_ERROR": 503,
    "SIMULATED_UPSTREAM_TIMEOUT": 504,
}


def _error(
    code: str,
    message: str,
    *,
    retryable: bool = False,
    details: dict[str, Any] | None = None,
    run_id: str | None = None,
    status: int | None = None,
    retry_after: str | None = None,
) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    resolved = status if status is not None else _ERROR_STATUS.get(code, 500)
    if resolved in (429, 503) and retry_after is not None:
        headers["Retry-After"] = retry_after
    return JSONResponse(
        status_code=resolved,
        headers=headers,
        content={
            "error": {
                "code": code,
                "message": message,
                "retryable": retryable,
                "request_id": None,
                "run_id": run_id,
                "details": details or {},
            }
        },
    )


# ---------------------------------------------------------------------------
# Cursors
# ---------------------------------------------------------------------------


def _filter_digest(filters: dict[str, Any]) -> str:
    payload = json.dumps(filters, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _encode_cursor(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    signature = hmac.new(_CURSOR_KEY, body, hashlib.sha256).digest()[:16]
    return base64.urlsafe_b64encode(signature + body).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str) -> dict[str, Any] | None:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (ValueError, UnicodeEncodeError):
        return None
    if len(raw) <= 16:
        return None
    signature, body = raw[:16], raw[16:]
    expected = hmac.new(_CURSOR_KEY, body, hashlib.sha256).digest()[:16]
    # Constant time: the signature is what makes the cursor opaque rather
    # than merely obscure, and a timing oracle would undo that.
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


# ---------------------------------------------------------------------------
# Fault rules
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FaultRule:
    rule_id: str
    mode: str
    parameters: dict[str, Any] = field(default_factory=dict)
    path: str | None = None
    nth: int = 0
    max_activations: int = 1
    matches: int = 0
    activations: int = 0
    enabled: bool = True

    @property
    def exhausted(self) -> bool:
        return self.activations >= self.max_activations

    def should_fire(self, path: str) -> bool:
        if not self.enabled or self.exhausted:
            return False
        if self.path is not None and not path.startswith(self.path):
            return False
        self.matches += 1
        if self.nth and self.matches != self.nth:
            return False
        self.activations += 1
        return True

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "mode": self.mode,
            "parameters": self.parameters,
            "path": self.path,
            "nth": self.nth,
            "max_activations": self.max_activations,
            "matches": self.matches,
            "activations": self.activations,
            "enabled": self.enabled,
            "exhausted": self.exhausted,
        }


class FakeSimulator:
    """Holds the dataset, the run identity, the snapshots and the faults."""

    def __init__(self, dataset: Dataset | None = None, *, snapshot_ttl_s: float = 300.0) -> None:
        self.dataset = dataset or build_dataset(DatasetSpec())
        self.snapshot_ttl_s = snapshot_ttl_s
        self.faults: dict[str, FaultRule] = {}
        self.armed = True
        self.request_log: list[dict[str, Any]] = []
        #: Snapshot leases, keyed by (run_id, generation, entity, snapshot_seq).
        #: A lease that is revoked makes its cursors answer 410.
        self.revoked_snapshots: set[tuple[str, int, str, int]] = set()
        self.expire_all_cursors = False
        self._fault_counter = 0
        self.max_page_size = 500
        self.default_page_size = 100
        #: Set to answer every data-plane request with 401, as a revoked
        #: credential would.
        self.reject_credentials = False

    # -- control ------------------------------------------------------------

    def add_fault(
        self,
        mode: str,
        *,
        path: str | None = None,
        nth: int = 0,
        max_activations: int = 1,
        **parameters: Any,
    ) -> FaultRule:
        self._fault_counter += 1
        rule = FaultRule(
            rule_id=f"fault-{self._fault_counter:03d}",
            mode=mode,
            parameters=parameters,
            path=path,
            nth=nth,
            max_activations=max_activations,
        )
        self.faults[rule.rule_id] = rule
        return rule

    def clear_faults(self) -> None:
        self.faults.clear()

    def reset_run(self, *, new_run_id: str | None = None) -> Dataset:
        """Start a new generation, as the Project 01 admin reset does."""
        generation = self.dataset.generation + 1
        self.dataset = self.dataset.with_generation(
            run_id=new_run_id, generation=generation
        )
        # Every cursor issued against the previous generation is now invalid.
        self.expire_all_cursors = True
        return self.dataset

    def append_deals(self, count: int) -> None:
        """Commit more deals, as a generator running during a scan would.

        Used to prove a scan pinned to a snapshot does not see them
        (WF2-I-01).
        """
        from decimal import Decimal

        sequence = self.dataset.committed_seq
        start = len(self.dataset.deals) + 1
        for index in range(start, start + count):
            sequence += 2
            self.dataset.deals.append(
                {
                    "deal_id": f"deal-{index:06d}",
                    "source_seq": sequence,
                    "account_id": self.dataset.accounts[0]["account_id"],
                    "position_id": self.dataset.positions[0]["position_id"],
                    "symbol": "EURUSD",
                    "entry": "IN",
                    "side": "BUY",
                    "volume_lots": "0.10",
                    "price": "1.08500",
                    "realized_pnl_gross": "0.00",
                    "commission": str(-(Decimal("0.70"))),
                    "swap": "0.00",
                    "executed_at": "2026-10-02T00:00:00.000Z",
                }
            )
        self.dataset.committed_seq = sequence

    def fault_activations(self, mode: str | None = None) -> int:
        return sum(
            rule.activations
            for rule in self.faults.values()
            if mode is None or rule.mode == mode
        )

    # -- paging -------------------------------------------------------------

    def _rows(self, entity: str) -> list[dict[str, Any]]:
        if entity == "accounts":
            return self.dataset.accounts
        if entity == "positions":
            return self.dataset.positions
        return self.dataset.deals

    def _key_of(self, entity: str, row: dict[str, Any]) -> list[Any]:
        if entity == "accounts":
            return [row["account_id"]]
        if entity == "positions":
            return [row["position_id"]]
        return [row["source_seq"], row["deal_id"]]

    def page(
        self,
        entity: str,
        *,
        filters: dict[str, Any],
        snapshot_seq: int,
        last_key: list[Any] | None,
        limit: int,
    ) -> tuple[list[dict[str, Any]], list[Any] | None]:
        rows = [row for row in self._rows(entity) if self._visible(entity, row, snapshot_seq)]
        if entity == "deals":
            after = filters.get("after_seq")
            if after is not None:
                rows = [row for row in rows if row["source_seq"] > int(after)]
        account = filters.get("account_id")
        if account is not None:
            rows = [row for row in rows if row.get("account_id") == account]
        symbol = filters.get("symbol")
        if symbol is not None:
            rows = [row for row in rows if row.get("symbol") == symbol]
        if last_key is not None:
            rows = [row for row in rows if self._key_of(entity, row) > list(last_key)]
        # Over-fetch by one to learn whether a next page exists, so a cursor
        # is minted only when there is something after this page.
        window = rows[: limit + 1]
        if len(window) > limit:
            page_rows = window[:limit]
            return page_rows, self._key_of(entity, page_rows[-1])
        return window, None

    def _visible(self, entity: str, row: dict[str, Any], snapshot_seq: int) -> bool:
        if entity != "deals":
            return True
        return int(row["source_seq"]) <= snapshot_seq


# ---------------------------------------------------------------------------
# The app
# ---------------------------------------------------------------------------


def create_fake_simulator(state: FakeSimulator | None = None) -> tuple[FastAPI, FakeSimulator]:
    sim = state or FakeSimulator()
    app = FastAPI(title="Project 01 contract double", version="1.0.0")
    app.state.sim = sim

    def _authorize(request: Request, *, admin: bool) -> Response | None:
        header = request.headers.get("authorization") or ""
        parts = header.split(None, 1)
        token = parts[1].strip() if len(parts) == 2 and parts[0].lower() == "bearer" else None
        if token is None:
            return _error("UNAUTHORIZED", "a bearer token is required")
        if token == ADMIN_TOKEN:
            return None
        if token == DATA_TOKEN:
            if admin:
                # 403 rather than 404: hiding a route from an *authenticated*
                # caller buys nothing and makes the failure harder to read.
                return _error(
                    "FORBIDDEN", "this token is not permitted on the control plane"
                )
            if sim.reject_credentials:
                return _error("UNAUTHORIZED", "the presented token has been revoked")
            return None
        return _error("UNAUTHORIZED", "the presented token is not recognized")

    def _resolve_limit(raw: str | None) -> int | Response:
        if raw is None:
            return sim.default_page_size
        try:
            value = int(raw)
        except ValueError:
            return _error(
                "VALIDATION_FAILED", "limit must be an integer", details={"field": "limit"}
            )
        if value < 1 or value > sim.max_page_size:
            # Refused, not clamped: a clamped limit lets a client believe it
            # received a whole page when it received part of one.
            return _error(
                "VALIDATION_FAILED",
                "limit is outside the permitted range",
                details={"field": "limit", "minimum": 1, "maximum": sim.max_page_size},
            )
        return value

    def _apply_fault(path: str, build: Callable[[], Response]) -> Response:
        if not sim.armed:
            return build()
        for rule in list(sim.faults.values()):
            if not rule.should_fire(path):
                continue
            response = _fault_response(sim, rule, build)
            if response is not None:
                return response
        return build()

    # -- data plane ---------------------------------------------------------

    @app.get("/v1/meta")
    async def meta(request: Request) -> Response:
        denied = _authorize(request, admin=False)
        if denied is not None:
            return denied
        data = sim.dataset
        return JSONResponse(
            {
                "run_id": data.run_id,
                "generation": data.generation,
                "scenario_name": data.spec.scenario_name,
                "seed": data.spec.seed,
                "spec_hash": hashlib.sha256(
                    f"{data.run_id}:{data.generation}:{data.spec.seed}".encode()
                ).hexdigest()[:16],
                "status": "RUNNING",
                "snapshot_seq": data.committed_seq,
                "virtual_time": "2026-10-02T00:00:00.000Z",
                "start_virtual_time": "2026-10-01T00:00:00.000Z",
                "account_count": len(data.accounts),
                "deal_count": len(data.deals),
                "open_position_count": sum(
                    1 for row in data.positions if row["status"] == "OPEN"
                ),
                "capabilities": {
                    "api_version": "v1",
                    "generator_version": "1.0.0",
                    "accounting_policy_version": "1.0.0",
                    "symbol_catalog_version": "1.0.0",
                    "supported_symbols": ["EURUSD", "GBPUSD", "XAUUSD"],
                    "account_currency": "USD",
                    "default_page_size": sim.default_page_size,
                    "max_page_size": sim.max_page_size,
                    "snapshot_ttl_seconds": int(sim.snapshot_ttl_s),
                    "after_seq_is_exclusive": True,
                    "cursor_is_opaque_and_signed": True,
                    "time_range_convention": "[from_time, to_time)",
                    "simulator_specific_fields": [
                        "run_id",
                        "generation",
                        "snapshot_seq",
                        "source_seq",
                    ],
                    "wire_compatible_with_mt5_manager_api": False,
                },
            }
        )

    def _list(entity: str, request: Request) -> Response:
        denied = _authorize(request, admin=False)
        if denied is not None:
            return denied
        params = request.query_params
        cursor = params.get("cursor")
        limit = _resolve_limit(params.get("limit"))
        if isinstance(limit, Response):
            return limit

        data = sim.dataset
        filters: dict[str, Any] = {"limit": limit}
        if entity != "accounts":
            filters["account_id"] = params.get("account_id")
            filters["symbol"] = params.get("symbol")
        if entity == "deals":
            raw_after = params.get("after_seq")
            if raw_after is not None:
                try:
                    filters["after_seq"] = int(raw_after)
                except ValueError:
                    return _error(
                        "VALIDATION_FAILED",
                        "after_seq must be an integer",
                        details={"field": "after_seq"},
                    )
            else:
                filters["after_seq"] = None

        if cursor is None:
            snapshot_seq = data.committed_seq
            last_key = None
            effective = filters
        else:
            payload = _decode_cursor(cursor)
            if payload is None:
                return _error(
                    "INVALID_CURSOR",
                    "the cursor is not a valid signed token",
                    details={"field": "cursor"},
                )
            if payload.get("entity") != entity:
                return _error(
                    "INVALID_CURSOR",
                    "the cursor belongs to a different entity",
                    details={"field": "cursor"},
                )
            # Only filters the client explicitly sent are compared. Sending a
            # cursor alone is normal pagination; sending it *with* a changed
            # filter is a different scan, and answering it would silently
            # reinterpret the request.
            explicit = set(params.keys()) - {"cursor"}
            mismatched = sorted(
                name
                for name in explicit
                if name in filters
                and str(filters[name]) != str((payload.get("filters") or {}).get(name))
            )
            if mismatched:
                return _error(
                    "INVALID_CURSOR",
                    "request filters do not match the cursor",
                    details={
                        "mismatched_filters": mismatched,
                        "hint": "start a new scan when changing any filter, including limit",
                    },
                )
            if payload.get("run_id") != data.run_id or payload.get("generation") != data.generation:
                return _error(
                    "RUN_REPLACED",
                    "the run this cursor belongs to has been replaced",
                    details={
                        "cursor_run_id": payload.get("run_id"),
                        "active_run_id": data.run_id,
                    },
                    run_id=data.run_id,
                )
            issued = float(payload.get("issued_at") or 0.0)
            lease = (
                str(payload.get("run_id")),
                int(payload.get("generation") or 0),
                entity,
                int(payload.get("snapshot_seq") or 0),
            )
            if (
                sim.expire_all_cursors
                or lease in sim.revoked_snapshots
                or (time.time() - issued) > sim.snapshot_ttl_s
            ):
                return _error(
                    "SNAPSHOT_EXPIRED",
                    "the snapshot this cursor referred to has expired",
                    details={"snapshot_seq": lease[3]},
                    run_id=data.run_id,
                )
            snapshot_seq = int(payload.get("snapshot_seq") or 0)
            last_key = payload.get("last_key")
            effective = dict(payload.get("filters") or {})
            effective["limit"] = int(effective.get("limit", limit))

        rows, next_key = sim.page(
            entity,
            filters=effective,
            snapshot_seq=snapshot_seq,
            last_key=last_key,
            limit=int(effective["limit"]),
        )
        next_cursor = (
            None
            if next_key is None
            else _encode_cursor(
                {
                    "entity": entity,
                    "run_id": data.run_id,
                    "generation": data.generation,
                    "snapshot_seq": snapshot_seq,
                    "filters": effective,
                    "filter_digest": _filter_digest(effective),
                    "last_key": next_key,
                    "issued_at": time.time(),
                }
            )
        )
        sim.request_log.append(
            {
                "entity": entity,
                "query": dict(params),
                "snapshot_seq": snapshot_seq,
                "items": len(rows),
            }
        )
        return JSONResponse(
            {
                "run_id": data.run_id,
                "generation": data.generation,
                "snapshot_seq": snapshot_seq,
                "next_cursor": next_cursor,
                "items": [dict(row) for row in rows],
            }
        )

    @app.get("/v1/accounts")
    async def accounts(request: Request) -> Response:
        return _apply_fault("/v1/accounts", lambda: _list("accounts", request))

    @app.get("/v1/positions")
    async def positions(request: Request) -> Response:
        return _apply_fault("/v1/positions", lambda: _list("positions", request))

    @app.get("/v1/deals")
    async def deals(request: Request) -> Response:
        return _apply_fault("/v1/deals", lambda: _list("deals", request))

    # -- control plane ------------------------------------------------------

    @app.get("/admin/v1/runs/current")
    async def current_run(request: Request) -> Response:
        denied = _authorize(request, admin=True)
        if denied is not None:
            return denied
        return JSONResponse(
            {"run_id": sim.dataset.run_id, "generation": sim.dataset.generation}
        )

    @app.post("/control/reset")
    async def control_reset(request: Request) -> Response:
        denied = _authorize(request, admin=True)
        if denied is not None:
            return denied
        body = await _json_body(request)
        data = sim.reset_run(new_run_id=body.get("run_id"))
        return JSONResponse({"run_id": data.run_id, "generation": data.generation})

    @app.post("/control/faults")
    async def control_fault(request: Request) -> Response:
        denied = _authorize(request, admin=True)
        if denied is not None:
            return denied
        body = await _json_body(request)
        rule = sim.add_fault(
            str(body.get("mode")),
            path=body.get("path"),
            nth=int(body.get("nth") or 0),
            max_activations=int(body.get("max_activations") or 1),
            **(body.get("parameters") or {}),
        )
        return JSONResponse(rule.as_dict(), status_code=201)

    @app.get("/control/faults")
    async def control_list_faults(request: Request) -> Response:
        denied = _authorize(request, admin=True)
        if denied is not None:
            return denied
        return JSONResponse({"items": [rule.as_dict() for rule in sim.faults.values()]})

    @app.delete("/control/faults")
    async def control_clear_faults(request: Request) -> Response:
        denied = _authorize(request, admin=True)
        if denied is not None:
            return denied
        sim.clear_faults()
        return JSONResponse({"cleared": True})

    @app.get("/health/live")
    async def live() -> Response:
        return JSONResponse({"status": "alive"})

    return app, sim


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - a control plane may be called sloppily
        return {}
    return body if isinstance(body, dict) else {}


def _fault_response(
    sim: FakeSimulator, rule: FaultRule, build: Callable[[], Response]
) -> Response | None:
    """Turn a fired rule into a response, or ``None`` to fall through."""
    mode = rule.mode
    params = rule.parameters

    if mode == "http_error":
        status = int(params.get("status", 503))
        if status not in _FAULT_STATUSES:
            return None
        code = str(
            params.get(
                "code",
                {
                    410: "SNAPSHOT_EXPIRED",
                    429: "RATE_LIMITED",
                    500: "INTERNAL_ERROR",
                    502: "SERVICE_NOT_READY",
                    503: "SERVICE_NOT_READY",
                    504: "SIMULATED_UPSTREAM_TIMEOUT",
                }[status],
            )
        )
        return _error(
            code,
            "a deliberate fault was injected",
            retryable=status in (429, 500, 502, 503, 504),
            status=status,
            retry_after=params.get("retry_after"),
            details={"injected": True, "rule_id": rule.rule_id},
            run_id=sim.dataset.run_id,
        )

    if mode == "delay_before_response":
        time.sleep(float(params.get("delay_ms", 0)) / 1000.0)
        return build()

    if mode == "malformed_json":
        body = build()
        raw = bytes(body.body)
        # Truncated mid-document: well-formed JSON with the tail removed is
        # the payload a streaming parser is most likely to accept by accident.
        return Response(
            content=raw[: max(1, len(raw) // 2)],
            status_code=200,
            media_type="application/json",
        )

    if mode == "truncate_response":
        body = build()
        keep = int(params.get("keep_bytes", 32))
        return Response(
            content=bytes(body.body)[:keep],
            status_code=200,
            media_type="application/json",
        )

    if mode == "wrong_content_type":
        return PlainTextResponse(
            content="<html><body>gateway error</body></html>",
            status_code=200,
            media_type="text/html",
        )

    if mode == "oversize_body":
        size = int(params.get("bytes", 4 * 1024 * 1024))
        return Response(
            content=b'{"filler":"' + b"x" * size + b'"}',
            status_code=200,
            media_type="application/json",
        )

    if mode == "declare_huge_content_length":
        body = build()
        return Response(
            content=bytes(body.body),
            status_code=200,
            media_type="application/json",
            headers={"Content-Length": str(int(params.get("declared", 64 * 1024 * 1024)))},
        )

    if mode == "deep_nesting":
        depth = int(params.get("depth", 10_000))
        return Response(
            content=(b"[" * depth) + (b"]" * depth),
            status_code=200,
            media_type="application/json",
        )

    if mode == "duplicate_json_key":
        return Response(
            content=b'{"run_id":"a","run_id":"b","generation":1,"snapshot_seq":1,'
            b'"next_cursor":null,"items":[]}',
            status_code=200,
            media_type="application/json",
        )

    if mode == "non_finite_number":
        return Response(
            content=b'{"run_id":"run_0001","generation":1,"snapshot_seq":1,'
            b'"next_cursor":null,"items":[{"commission":NaN}]}',
            status_code=200,
            media_type="application/json",
        )

    if mode == "redirect":
        return RedirectResponse(url=str(params.get("location", "http://example.invalid/v1/deals")))

    if mode == "no_content":
        return Response(status_code=204)

    if mode in {
        "drop_one_item",
        "duplicate_one_item",
        "duplicate_replace_item",
        "shuffle_items",
        "omit_field",
        "replace_field_value",
        "float_money",
        "stale_snapshot",
        "mixed_generation",
        "mixed_snapshot_seq",
        "overlong_page",
        "regress_source_seq",
    }:
        return _mutate_body(sim, rule, build)

    return None


def _mutate_body(
    sim: FakeSimulator, rule: FaultRule, build: Callable[[], Response]
) -> Response:
    """Mutate a *valid* page, so the connector sees 200 with wrong content.

    This is the dangerous family: the framing is perfect and the status is
    200, so only domain validation can catch it (section 15.6).
    """
    original = build()
    try:
        body = json.loads(bytes(original.body).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):  # pragma: no cover - error page
        return original
    if not isinstance(body, dict) or "items" not in body:
        return original
    items = body["items"]
    params = rule.parameters
    mode = rule.mode

    if mode == "drop_one_item" and items:
        del items[int(params.get("index", 0)) % len(items)]
    elif mode == "duplicate_one_item" and items:
        items.append(dict(items[int(params.get("index", 0)) % len(items)]))
    elif mode == "duplicate_replace_item" and len(items) > 1:
        # The page keeps its declared size, so the page-size cap does not
        # fire first and the duplicate itself is what has to be caught.
        clone = dict(items[0])
        clone["source_seq"] = items[-1]["source_seq"]
        items[-1] = clone
    elif mode == "shuffle_items" and len(items) > 1:
        items.reverse()
    elif mode == "omit_field" and items:
        items[0].pop(str(params.get("field", "deal_id")), None)
    elif mode == "replace_field_value" and items:
        items[0][str(params.get("field", "commission"))] = params.get("value", "0.00")
    elif mode == "float_money" and items:
        field_name = str(params.get("field", "price"))
        if field_name in items[0]:
            items[0][field_name] = float(items[0][field_name])
    elif mode == "stale_snapshot":
        body["snapshot_seq"] = max(0, int(body["snapshot_seq"]) - int(params.get("delta", 5)))
    elif mode == "mixed_generation":
        body["generation"] = int(body["generation"]) + int(params.get("delta", 1))
    elif mode == "mixed_snapshot_seq":
        body["snapshot_seq"] = int(body["snapshot_seq"]) + int(params.get("delta", 7))
    elif mode == "overlong_page" and items:
        extra = int(params.get("extra", 5))
        for index in range(extra):
            clone = dict(items[0])
            for key in ("deal_id", "account_id", "position_id"):
                if key in clone:
                    clone[key] = f"{clone[key]}-x{index}"
            if "source_seq" in clone:
                clone["source_seq"] = int(clone["source_seq"]) + 1 + index
            items.append(clone)
    elif mode == "regress_source_seq" and len(items) > 1:
        items[-1]["source_seq"] = int(items[0]["source_seq"])
        items[-1]["deal_id"] = f"{items[0]['deal_id']}-dup"

    return JSONResponse(body)
