"""Authentication, plane separation, injection resistance and log redaction.

The threat model is modest - synthetic data, a test double - but the failures
these tests prevent are not: a system under test that can reset its own data
source invalidates every result produced with it (WF-101).
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.main import create_app
from simulator.observability.logging import JsonFormatter, redact
from simulator.persistence.connection import ProcessLock
from simulator.security.auth import Authenticator, ForbiddenError, Plane, UnauthorizedError
from simulator.settings import ConfigurationError
from tests.conftest import ADMIN_TOKEN, DATA_TOKEN, create_run, make_settings

pytestmark = pytest.mark.contract

DATA_ROUTES = ["/v1/meta", "/v1/accounts", "/v1/positions", "/v1/deals", "/v1/marks"]
ADMIN_ROUTES = [
    "/admin/v1/runs/current",
    "/admin/v1/faults",
    "/admin/v1/metrics",
    "/admin/v1/diagnostics/invariants",
]


@pytest.mark.parametrize("route", DATA_ROUTES + ADMIN_ROUTES)
def test_no_token_is_401(client: TestClient, route: str) -> None:
    """WF-102."""
    response = client.get(route)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHORIZED"


@pytest.mark.parametrize(
    "header",
    [
        "",
        "Bearer",
        "Bearer ",
        "Basic dXNlcjpwYXNz",
        f"{DATA_TOKEN}",  # no scheme
        f"bearer {DATA_TOKEN}extra",
        "Bearer " + "x" * 600,
    ],
)
def test_malformed_or_wrong_tokens_are_401(client: TestClient, header: str) -> None:
    response = client.get("/v1/meta", headers={"Authorization": header})
    assert response.status_code == 401


def test_the_bearer_scheme_is_case_insensitive(client: TestClient) -> None:
    assert client.get("/v1/meta", headers={"Authorization": f"bearer {DATA_TOKEN}"}).status_code == 200
    assert client.get("/v1/meta", headers={"Authorization": f"BEARER {DATA_TOKEN}"}).status_code == 200


def test_an_unauthorized_response_reveals_nothing_about_the_token(
    client: TestClient,
) -> None:
    """A distinguishable error is an oracle for guessing."""
    unknown = client.get("/v1/meta", headers={"Authorization": "Bearer totally-wrong"})
    truncated = client.get("/v1/meta", headers={"Authorization": f"Bearer {DATA_TOKEN[:-1]}"})
    assert unknown.status_code == truncated.status_code == 401
    assert unknown.json()["error"]["message"] == truncated.json()["error"]["message"]
    body = unknown.text
    assert DATA_TOKEN not in body
    assert ADMIN_TOKEN not in body


@pytest.mark.parametrize("route", ADMIN_ROUTES)
def test_a_data_token_cannot_reach_the_control_plane(
    client: TestClient, data_headers: dict[str, str], route: str
) -> None:
    """WF-101: 403, not 404 - an authenticated caller gains nothing from hiding."""
    response = client.get(route, headers=data_headers)
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"


def test_a_data_token_cannot_create_or_reset_a_run(
    client: TestClient, data_headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    create_run(client, admin_headers)
    before = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    for route, body in (
        ("/admin/v1/runs", {"scenario_name": "x", "seed": 1, "start_virtual_time": "2026-10-30T00:00:00Z", "account_count": 1, "deal_count": 1}),
        ("/admin/v1/runs/run_0001/reset", {}),
        ("/admin/v1/runs/run_0001/stop", {}),
        ("/admin/v1/faults", {"action": {"mode": "http_error", "parameters": {"status": 503}}}),
    ):
        response = client.post(
            route, json=body, headers={**data_headers, "Idempotency-Key": "x"}
        )
        assert response.status_code == 403, route
    after = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert after == before


def test_an_admin_token_may_read_the_data_plane(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """Documented: admin is a superset, so debugging needs one token."""
    assert client.get("/v1/meta", headers=admin_headers).status_code == 200


def test_health_endpoints_need_no_token(client: TestClient) -> None:
    """A probe that needs a secret is a probe nobody configures right."""
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 200


def test_authenticator_uses_constant_time_comparison() -> None:
    """Not timing-measurable here; asserted structurally instead."""
    import inspect

    source = inspect.getsource(Authenticator.authenticate)
    assert "compare_digest" in source
    assert "==" not in source.replace("!=", "").replace("==None", "")


def test_authenticator_rejects_and_authorizes_as_documented(tmp_path) -> None:
    settings = make_settings(tmp_path)
    authenticator = Authenticator(settings)
    with pytest.raises(UnauthorizedError):
        authenticator.authenticate(None, required=Plane.DATA)
    with pytest.raises(UnauthorizedError):
        authenticator.authenticate("Bearer nope", required=Plane.DATA)
    with pytest.raises(ForbiddenError):
        authenticator.authenticate(f"Bearer {DATA_TOKEN}", required=Plane.ADMIN)
    data = authenticator.authenticate(f"Bearer {DATA_TOKEN}", required=Plane.DATA)
    admin = authenticator.authenticate(f"Bearer {ADMIN_TOKEN}", required=Plane.ADMIN)
    assert data.plane is Plane.DATA and not data.is_admin
    assert admin.plane is Plane.ADMIN and admin.is_admin
    # Only a short digest is retained, never the token.
    assert DATA_TOKEN not in data.token_id
    assert len(data.token_id) == 12


# ---------------------------------------------------------------------------
# Injection and hostile payloads
# ---------------------------------------------------------------------------


SQL_PAYLOADS = [
    "A-000001' OR '1'='1",
    "'; DROP TABLE deals; --",
    "A-000001'; UPDATE account_versions SET balance_dec='999999.00'; --",
    "1 UNION SELECT * FROM idempotency_records",
    "../../etc/passwd",
    "A-000001%00truncated",
]


@pytest.mark.parametrize("payload", SQL_PAYLOADS)
def test_sql_injection_payloads_are_data_not_code(
    client: TestClient, data_headers: dict[str, str], admin_headers: dict[str, str], payload: str
) -> None:
    """WF-103: every statement is parameterized, so these are just filters."""
    create_run(client, admin_headers)
    before = client.get("/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers).json()

    response = client.get(f"/v1/deals?account_id={payload}", headers=data_headers)
    assert response.status_code in {200, 422}
    if response.status_code == 200:
        assert response.json()["items"] == []

    after = client.get("/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers).json()
    assert after["canonical_digest"] == before["canonical_digest"]
    assert after["summary"]["failed"] == 0
    # The schema is intact.
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 200


def test_a_deeply_nested_payload_is_refused_not_recursed(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-110: hostile JSON must not turn into unbounded recursion."""
    nested: Any = {"a": 1}
    for _ in range(2_000):
        nested = {"a": nested}
    response = client.post(
        "/admin/v1/runs",
        json=nested,
        headers={**admin_headers, "Idempotency-Key": "deep"},
    )
    assert response.status_code in {413, 422}
    assert client.get("/health/live").status_code == 200


def test_an_oversized_body_is_refused_before_processing(
    client: TestClient, admin_headers: dict[str, str], settings
) -> None:
    """WF-091."""
    oversized = {"scenario_name": "x" * (settings.max_request_body_bytes + 1_000)}
    response = client.post(
        "/admin/v1/runs",
        json=oversized,
        headers={**admin_headers, "Idempotency-Key": "big"},
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"


def test_unicode_and_control_characters_do_not_crash_the_service(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-075.

    A lone surrogate such as ``\\ud800`` is deliberately absent: it is not
    encodable as UTF-8, so no HTTP client can put it in a URL. Testing it would
    only assert that the test client refuses to send it.
    """
    for payload in ["‮", "\u0000", "😀" * 20, "%00", "\r\nX-Injected: 1", "\x1b[2J"]:
        response = client.get("/v1/deals", params={"account_id": payload}, headers=data_headers)
        assert response.status_code in {200, 400, 422}
        # Header injection through a query parameter must be impossible.
        assert "x-injected" not in {key.lower() for key in response.headers}


def test_a_cursor_from_another_run_is_refused(
    client: TestClient, data_headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    """WF-047, WF-104: fail closed, never leak another run's rows."""
    create_run(client, admin_headers)
    cursor = client.get("/v1/deals?limit=5", headers=data_headers).json()["next_cursor"]
    assert cursor

    client.post(
        "/admin/v1/runs/run_0001/reset",
        json={"expected_generation": 1},
        headers={**admin_headers, "Idempotency-Key": "reset-iso"},
    )
    response = client.get(f"/v1/deals?cursor={cursor}", headers=data_headers)
    assert response.status_code == 410
    assert response.json()["error"]["code"] == "RUN_REPLACED"


def test_a_cursor_signed_with_another_key_is_refused(tmp_path) -> None:
    """A cursor from a different deployment must not be honoured."""
    from simulator.persistence.cursors import CursorCodec

    foreign = CursorCodec([("k1", "someone-elses-key")])
    from tests.unit.test_cursors import make_payload

    token = foreign.encode(make_payload())

    settings = make_settings(tmp_path / "iso")
    app = create_app(settings, lock_process=False)
    with TestClient(app) as isolated:
        create_run(isolated, {"Authorization": f"Bearer {ADMIN_TOKEN}"})
        response = isolated.get(
            f"/v1/deals?cursor={token}", headers={"Authorization": f"Bearer {DATA_TOKEN}"}
        )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "INVALID_CURSOR"


# ---------------------------------------------------------------------------
# Log redaction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    ["authorization", "token", "admin_token", "data_token", "secret", "signing_key", "cookie"],
)
def test_sensitive_fields_are_redacted_by_the_formatter(field: str) -> None:
    """WF-107: redaction lives in the formatter, not at call sites."""
    record = logging.LogRecord("simulator.test", logging.INFO, __file__, 1, "msg", None, None)
    setattr(record, field, "super-secret-value")
    rendered = json.loads(JsonFormatter().format(record))
    assert rendered[field] == "[redacted]"
    assert "super-secret-value" not in json.dumps(rendered)


def test_a_bearer_header_value_is_redacted_wherever_it_appears() -> None:
    assert redact("Bearer abc123") == "[redacted]"
    assert redact({"headers": {"Authorization": "Bearer abc123"}}) == {
        "headers": {"Authorization": "[redacted]"}
    }


def test_redaction_is_depth_bounded() -> None:
    """A cyclic or hostile structure must not recurse without limit."""
    nested: Any = {"a": "b"}
    for _ in range(50):
        nested = {"a": nested}
    assert "[truncated]" in json.dumps(redact(nested))


def test_log_output_from_a_real_request_contains_no_token(
    client: TestClient, admin_headers: dict[str, str], caplog
) -> None:
    with caplog.at_level(logging.DEBUG, logger="simulator"):
        create_run(client, admin_headers)
        client.get("/v1/meta", headers=admin_headers)
    text = "\n".join(record.getMessage() + str(record.__dict__) for record in caplog.records)
    assert ADMIN_TOKEN not in text
    assert DATA_TOKEN not in text


# ---------------------------------------------------------------------------
# Single-process enforcement
# ---------------------------------------------------------------------------


def test_no_response_or_fixture_carries_anything_resembling_real_data(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """WF-111: the data policy is synthetic-only, so it is checked, not assumed.

    Scans the shipped scenario fixtures and a live response for the shapes that
    would indicate a real record leaked in: an email address, a long digit run
    that could be a card or account number, an IBAN-like token, or an MT5
    credential field. The patterns are coarse on purpose - this is a tripwire
    for an accident, not a DLP product.
    """
    import re
    from pathlib import Path

    create_run(client, admin_headers)
    payloads = [
        client.get("/v1/deals?limit=50", headers=data_headers).text,
        client.get("/v1/accounts?limit=50", headers=data_headers).text,
        client.get("/v1/positions?limit=50", headers=data_headers).text,
        client.get("/v1/marks", headers=data_headers).text,
        client.get("/admin/v1/runs/current", headers=admin_headers).text,
    ]
    scenarios = Path(__file__).resolve().parents[2] / "scenarios"
    payloads += [path.read_text(encoding="utf-8") for path in scenarios.glob("*.json")]

    patterns = {
        "email": re.compile(r"[\w.+-]+@[\w-]+\.[\w.]{2,}"),
        "long_digit_run": re.compile(r"\d{13,}"),
        "iban_like": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,}\b"),
        "mt5_credential": re.compile(
            r"(?i)\b(mt5[_-]?(login|password|server)|manager[_-]?(login|password))\b"
        ),
    }
    for index, text in enumerate(payloads):
        for name, pattern in patterns.items():
            match = pattern.search(text)
            assert match is None, f"payload {index}: {name} matched {match.group()!r}"

    # And the identifiers really are the synthetic ordinal form.
    deals = client.get("/v1/deals?limit=5", headers=data_headers).json()["items"]
    for deal in deals:
        assert re.fullmatch(r"D-\d{8}", deal["deal_id"]), deal["deal_id"]
        assert re.fullmatch(r"A-\d{6}", deal["account_id"]), deal["account_id"]


def test_a_second_process_cannot_attach_to_one_database(tmp_path) -> None:
    """WF-120: two writers would break seq contiguity, not the file."""
    settings = make_settings(tmp_path)
    first = ProcessLock(settings.lock_path)
    first.acquire()
    try:
        second = ProcessLock(settings.lock_path)
        with pytest.raises(ConfigurationError) as excinfo:
            second.acquire()
        assert "single writer" in str(excinfo.value)
    finally:
        first.release()
    # Released, so the next process may take it.
    third = ProcessLock(settings.lock_path)
    third.acquire()
    third.release()


def test_an_app_with_the_lock_enabled_refuses_a_second_instance(tmp_path) -> None:
    settings = make_settings(tmp_path, enforce_single_process_lock=True)
    app_one = create_app(settings, lock_process=True)
    with TestClient(app_one):
        app_two = create_app(settings, lock_process=True)
        with pytest.raises(ConfigurationError):
            with TestClient(app_two):
                pass
