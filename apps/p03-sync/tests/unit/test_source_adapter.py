"""HTTP adapter: status mapping, byte caps and field-presence rules (group L).

Driven through ``httpx.MockTransport`` so each upstream behaviour is produced
exactly, including the ones a cooperative server would never send.
"""

from __future__ import annotations

import json

import httpx
import pytest

from trading_sync.config import load_settings
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import (
    AuthenticationError,
    AuthorizationError,
    OverloadedError,
    ResponseTooLargeError,
    SchemaError,
    SnapshotExpiredError,
    SourceContractError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from trading_sync.infrastructure.source_adapter import HttpConnectorSource

pytestmark = pytest.mark.unit


def make_source(handler, **overrides) -> HttpConnectorSource:
    settings = load_settings(
        database_url="postgresql+asyncpg://u@h/db",
        source={"source_id": "sim_local", **overrides},
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://connector"
    )
    return HttpConnectorSource(settings, client=client)


def json_response(payload: dict, status: int = 200, headers: dict | None = None):
    return httpx.Response(status, content=json.dumps(payload), headers=headers or {})


def meta_body(**overrides) -> dict:
    body = {
        "run_id": "run_0001",
        "generation": 0,
        "source_kind": "simulator",
        "committed_seq": 10,
    }
    body.update(overrides)
    return body


def page_body(**overrides) -> dict:
    body = {
        "run_id": "run_0001",
        "generation": 0,
        "snapshot_seq": 10,
        "items": [],
        "next_cursor": None,
    }
    body.update(overrides)
    return body


@pytest.mark.whatif("WF3-001", "WF3-002")
@pytest.mark.parametrize(
    "body",
    [
        {"generation": 0},
        {"run_id": "run_0001"},
        meta_body(run_id=""),
        meta_body(run_id="   "),
        meta_body(generation="zero"),
        meta_body(generation=True),
    ],
)
async def test_unusable_meta_identity_is_refused(body: dict) -> None:
    """No synthesised identity, ever.

    A generated ``run_id`` would let two different simulator runs share a
    watermark, and the resulting mix is undetectable afterwards.
    """
    source = make_source(lambda request: json_response(body))
    with pytest.raises(SourceContractError):
        await source.read_meta("sim_local")
    await source.aclose()


@pytest.mark.whatif("WF3-008")
async def test_a_non_simulator_source_defaults_to_no_incremental_support() -> None:
    """The safe default matters.

    Assuming support and finding out otherwise means a watermark has already
    been written over a sequence nobody guaranteed to be monotonic.
    """
    source = make_source(
        lambda request: json_response(meta_body(source_kind="manager_sdk"))
    )
    identity = await source.read_meta("sim_local")
    assert identity.supports_incremental_deals is False
    await source.aclose()


@pytest.mark.whatif("WF3-003")
async def test_an_unknown_schema_version_is_refused() -> None:
    """Present-but-unknown is the dangerous case, not absent.

    A mapper applied to a schema it does not know could read a renamed field as
    missing, or a repurposed one as its old meaning.
    """
    source = make_source(lambda r: json_response(meta_body(schema_version="99")))
    with pytest.raises(SchemaError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.safe_details["schema_version"] == "99"
    await source.aclose()


@pytest.mark.whatif("WF3-009")
async def test_a_known_schema_version_is_accepted() -> None:
    source = make_source(lambda r: json_response(meta_body(schema_version="v1")))
    assert (await source.read_meta("sim_local")).run_id == "run_0001"
    await source.aclose()


@pytest.mark.whatif("WF3-135")
async def test_http_410_becomes_a_snapshot_expiry() -> None:
    source = make_source(lambda r: httpx.Response(410, content=b"gone"))
    with pytest.raises(SnapshotExpiredError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.retryable is True
    assert caught.value.checkpoint_safe is False
    await source.aclose()


@pytest.mark.whatif("WF3-136")
async def test_http_429_carries_the_retry_after_hint() -> None:
    """Parsed here, clamped by the retry policy -- never trusted raw."""
    source = make_source(
        lambda r: httpx.Response(429, content=b"", headers={"Retry-After": "12"})
    )
    with pytest.raises(OverloadedError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.retry_after_seconds == 12.0
    await source.aclose()


@pytest.mark.whatif("WF3-136")
@pytest.mark.parametrize("header", [None, "later", "-5", "0"])
async def test_an_unusable_retry_after_is_ignored(header: str | None) -> None:
    headers = {} if header is None else {"Retry-After": header}
    source = make_source(lambda r: httpx.Response(429, content=b"", headers=headers))
    with pytest.raises(OverloadedError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.retry_after_seconds is None
    await source.aclose()


@pytest.mark.whatif("WF3-138")
async def test_http_401_is_not_retryable() -> None:
    """Retrying wrong credentials cannot fix them, and a retry loop against an
    auth endpoint looks like an attack."""
    source = make_source(lambda r: httpx.Response(401, content=b""))
    with pytest.raises(AuthenticationError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.retryable is False
    await source.aclose()


@pytest.mark.whatif("WF3-139")
async def test_http_403_is_not_retryable() -> None:
    source = make_source(lambda r: httpx.Response(403, content=b""))
    with pytest.raises(AuthorizationError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.retryable is False
    await source.aclose()


@pytest.mark.whatif("WF3-137")
async def test_http_503_is_transient_but_never_checkpoint_safe() -> None:
    source = make_source(lambda r: httpx.Response(503, content=b""))
    with pytest.raises(UpstreamUnavailableError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.retryable is True
    assert caught.value.checkpoint_safe is False
    await source.aclose()


@pytest.mark.whatif("WF3-140")
async def test_an_undecodable_body_is_refused_before_any_write() -> None:
    source = make_source(lambda r: httpx.Response(200, content=b"<html>nope</html>"))
    with pytest.raises(SchemaError):
        await source.read_meta("sim_local")
    await source.aclose()


@pytest.mark.whatif("WF3-133")
async def test_a_timeout_is_classified_as_transient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    source = make_source(handler)
    with pytest.raises(UpstreamTimeoutError):
        await source.read_meta("sim_local")
    await source.aclose()


@pytest.mark.whatif("WF3-147")
async def test_an_oversized_response_is_abandoned_while_reading() -> None:
    """The cap is enforced during the read, not after.

    Checking afterwards would mean the oversized body is already resident,
    which is precisely the exhaustion the cap exists to prevent.
    """
    payload = b'{"junk": "' + b"x" * 200_000 + b'"}'
    source = make_source(
        lambda r: httpx.Response(200, content=payload), max_response_bytes=4096
    )
    with pytest.raises(ResponseTooLargeError) as caught:
        await source.read_meta("sim_local")
    assert caught.value.safe_details["max_bytes"] == 4096
    await source.aclose()


@pytest.mark.whatif("WF3-044")
async def test_the_adapter_reports_cursor_field_presence_separately() -> None:
    """Absent field versus explicit null, carried faithfully to the validator.

    ``body.get("next_cursor")`` would collapse the two and let a truncated
    response end a scan.
    """
    missing = page_body()
    del missing["next_cursor"]
    source = make_source(lambda r: json_response(missing))
    page = await source.iter_deals("sim_local", 0).__anext__()
    assert page.cursor_present is False
    await source.aclose()

    explicit = page_body(next_cursor=None)
    source = make_source(lambda r: json_response(explicit))
    page = await source.iter_deals("sim_local", 0).__anext__()
    assert page.cursor_present is True and page.next_cursor is None
    await source.aclose()


@pytest.mark.whatif("WF3-146", "WF3-211")
async def test_pages_are_fetched_only_as_the_consumer_asks() -> None:
    """No prefetch: the consumer's commit paces the source.

    Requests are counted, so a later background-fetch optimisation that broke
    the backpressure bound fails here rather than at a million deals.
    """
    requests: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        requests.append(str(req.url))
        index = len(requests)
        terminal = index >= 3
        return json_response(
            page_body(items=[], next_cursor=None if terminal else f"cur-{index}")
        )

    source = make_source(handler)
    pages = source.iter_deals("sim_local", 0)
    await pages.__anext__()
    assert len(requests) == 1, "the adapter fetched ahead of the consumer"
    await pages.__anext__()
    assert len(requests) == 2
    await pages.aclose()
    assert len(requests) == 2, "closing the stream still triggered a fetch"
    await source.aclose()


@pytest.mark.whatif("WF3-004")
async def test_items_pass_through_untouched_for_the_domain_to_judge() -> None:
    """The adapter never coerces payloads; the domain validator decides.

    Keeping parsing in one place is what makes "reject a JSON float" one
    enforced rule rather than two implementations that can drift apart.
    """
    body = page_body(items=[{"deal_id": "D1", "commission": 0.1}])
    source = make_source(lambda r: json_response(body))
    page = await source.iter_deals("sim_local", 0).__anext__()
    assert page.items[0]["commission"] == 0.1
    assert page.entity is Entity.DEALS
    await source.aclose()


@pytest.mark.whatif("WF3-010")
async def test_the_configured_page_size_is_sent_on_every_request() -> None:
    seen: list[dict[str, str]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(dict(req.url.params))
        index = len(seen)
        return json_response(
            page_body(next_cursor=None if index >= 2 else "cur-1")
        )

    source = make_source(handler, page_size=100)
    async for _page in source.iter_deals("sim_local", 42):
        pass
    assert seen[0]["limit"] == "100"
    assert seen[0]["after_seq"] == "42"
    assert seen[1]["cursor"] == "cur-1"
    await source.aclose()
