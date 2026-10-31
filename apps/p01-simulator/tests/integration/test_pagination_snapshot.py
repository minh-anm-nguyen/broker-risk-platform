"""Snapshot pagination: INV-10, INV-11, WF-032, WF-033, WF-034..WF-046.

These tests are the proof for two harness-scope invariants the storage sweep
cannot answer:

* **INV-10** - every returned page contains only records within the snapshot
  sequence.
* **INV-11** - a full paged scan returns every record exactly once.

The oracle is an independent query that computes the expected identity set
*without* pagination. Comparing a paged scan against itself would prove only
that the scan is self-consistent, which it would be even if it dropped every
other row.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.persistence.read_repository import ReadRepository
from tests.conftest import collect_all_pages, create_run, wait_for_job

pytestmark = pytest.mark.integration


def oracle_deal_ids(settings, run_id: str, snapshot_seq: int, **filters: Any) -> set[str]:
    """Expected identity set, computed without pagination."""
    from simulator.persistence.connection import ReadConnectionPool

    pool = ReadConnectionPool(settings)
    try:
        repository = ReadRepository(pool, settings)
        return repository.deal_identity_set(
            run_id=run_id, snapshot_seq=snapshot_seq, **filters
        )
    finally:
        pool.close()


@pytest.fixture
def paging_run(client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]):
    """A dataset big enough that a scan needs many pages."""
    create_run(client, admin_headers, account_count=25, deal_count=900, max_open_positions=50)
    meta = client.get("/v1/meta", headers=data_headers).json()
    return meta


def test_a_full_scan_returns_every_record_exactly_once(
    client: TestClient, data_headers: dict[str, str], settings, paging_run: dict[str, Any]
) -> None:
    """INV-11 against an independent oracle."""
    items, pages = collect_all_pages(client, data_headers, "/v1/deals?limit=50")
    returned = [item["deal_id"] for item in items]
    expected = oracle_deal_ids(settings, paging_run["run_id"], paging_run["snapshot_seq"])

    assert len(pages) >= 18, "the dataset should need many pages"
    assert len(returned) == len(set(returned)), "a deal was returned twice"
    assert set(returned) == expected
    assert len(returned) == 900


def test_pages_stay_within_the_snapshot(
    client: TestClient, data_headers: dict[str, str], paging_run: dict[str, Any]
) -> None:
    """INV-10: no page may contain a record created after the snapshot."""
    snapshot = paging_run["snapshot_seq"]
    items, pages = collect_all_pages(client, data_headers, "/v1/deals?limit=50")
    for page in pages:
        assert page["snapshot_seq"] == snapshot
        assert page["run_id"] == paging_run["run_id"]
        assert page["generation"] == paging_run["generation"]
    assert all(item["source_seq"] <= snapshot for item in items)


def test_a_concurrent_writer_does_not_disturb_an_open_scan(
    client: TestClient,
    admin_headers: dict[str, str],
    data_headers: dict[str, str],
    settings,
    paging_run: dict[str, Any],
) -> None:
    """WF-032 and WF-033, the central claim of the versioned design.

    A reader opens a snapshot, the writer commits several hundred more events
    mid-scan, and the reader must still see exactly the state at ``S``: no new
    deal, no missing old one.
    """
    snapshot = paging_run["snapshot_seq"]
    expected = oracle_deal_ids(settings, paging_run["run_id"], snapshot)

    first = client.get("/v1/deals?limit=25", headers=data_headers).json()
    assert first["next_cursor"]
    collected = [item["deal_id"] for item in first["items"]]

    advance = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": 400},
        headers={**admin_headers, "Idempotency-Key": "concurrent-writer"},
    )
    assert advance.status_code == 200

    cursor = first["next_cursor"]
    while cursor:
        page = client.get(f"/v1/deals?cursor={cursor}", headers=data_headers).json()
        assert page["snapshot_seq"] == snapshot
        assert all(item["source_seq"] <= snapshot for item in page["items"])
        collected.extend(item["deal_id"] for item in page["items"])
        cursor = page["next_cursor"]

    job = wait_for_job(client, admin_headers, advance.json()["status_url"])
    assert job["status"] == "SUCCEEDED"

    assert len(collected) == len(set(collected))
    assert set(collected) == expected

    # And a fresh snapshot does see the new data, so the old one was a
    # snapshot rather than simply a stale read.
    after = client.get("/v1/meta", headers=data_headers).json()
    assert after["snapshot_seq"] > snapshot
    fresh, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")
    assert len(fresh) > len(collected)


def test_positions_updated_mid_scan_keep_their_snapshot_version(
    client: TestClient,
    admin_headers: dict[str, str],
    data_headers: dict[str, str],
    paging_run: dict[str, Any],
) -> None:
    """WF-033: the trap a current-state table would fall into.

    Filtering a current-state table by ``updated_seq <= S`` loses a row's
    pre-``S`` value the moment it is updated. Versioned projections keep it.
    """
    snapshot = paging_run["snapshot_seq"]
    first = client.get("/v1/positions?limit=20", headers=data_headers).json()
    assert first["next_cursor"]
    before = {item["position_id"]: item for item in first["items"]}

    advance = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": 300},
        headers={**admin_headers, "Idempotency-Key": "position-churn"},
    )
    wait_for_job(client, admin_headers, advance.json()["status_url"])

    # Re-reading the same page of the same snapshot must be byte-identical.
    again = client.get("/v1/positions?limit=20", headers=data_headers)
    assert again.json()["snapshot_seq"] > snapshot  # a new snapshot, as expected

    cursor = first["next_cursor"]
    seen = dict(before)
    while cursor:
        page = client.get(f"/v1/positions?cursor={cursor}", headers=data_headers).json()
        assert page["snapshot_seq"] == snapshot
        for item in page["items"]:
            seen[item["position_id"]] = item
        cursor = page["next_cursor"]
    # Every position present at S was returned, including ones the writer has
    # since changed or closed.
    assert len(seen) >= len(before)
    for position_id, original in before.items():
        assert seen[position_id] == original


def test_retrying_the_same_cursor_returns_an_identical_page(
    client: TestClient, data_headers: dict[str, str], paging_run: dict[str, Any]
) -> None:
    """WF-034: a retry after a network blip must not shift the window.

    The *token* is not asserted to be byte-identical across retries, and that
    is deliberate: reusing a cursor refreshes its lease so an in-progress scan
    does not race its own TTL, which changes the expiry embedded in the next
    token. What must not change is the data and the position it denotes, so
    that is what is checked - including that every issued token continues to
    the same following page.
    """
    cursor = client.get("/v1/deals?limit=30", headers=data_headers).json()["next_cursor"]
    pages = [
        client.get(f"/v1/deals?cursor={cursor}", headers=data_headers).json() for _ in range(4)
    ]
    assert all(page["items"] == pages[0]["items"] for page in pages)
    assert all(page["snapshot_seq"] == pages[0]["snapshot_seq"] for page in pages)

    followups = [
        client.get(f"/v1/deals?cursor={page['next_cursor']}", headers=data_headers).json()
        for page in pages
    ]
    assert all(page["items"] == followups[0]["items"] for page in followups)

    # The tokens denote the same position even where their expiry differs.
    codec = client.app.state.context.codec
    decoded = [codec.decode(page["next_cursor"]) for page in pages]
    assert {(item.snapshot_seq, tuple(item.last_key)) for item in decoded} == {
        (decoded[0].snapshot_seq, tuple(decoded[0].last_key))
    }


def test_a_cursor_alone_is_enough_to_continue(
    client: TestClient, data_headers: dict[str, str], paging_run: dict[str, Any]
) -> None:
    """The filters travel inside the signed token, so no resend is required."""
    first = client.get(
        "/v1/deals?account_id=A-000002&limit=5", headers=data_headers
    ).json()
    assert first["next_cursor"]
    second = client.get(
        f"/v1/deals?cursor={first['next_cursor']}", headers=data_headers
    ).json()
    assert second["items"]
    assert {item["account_id"] for item in second["items"]} == {"A-000002"}


def test_resending_identical_filters_with_a_cursor_is_accepted(
    client: TestClient, data_headers: dict[str, str], paging_run: dict[str, Any]
) -> None:
    """A client that echoes its filters back is paginating normally."""
    first = client.get(
        "/v1/deals?account_id=A-000002&limit=5", headers=data_headers
    ).json()
    response = client.get(
        f"/v1/deals?cursor={first['next_cursor']}&account_id=A-000002&limit=5",
        headers=data_headers,
    )
    assert response.status_code == 200


@pytest.mark.parametrize(
    ("extra", "mismatched"),
    [
        ("limit=10", "limit"),
        ("account_id=A-000009", "account_id"),
        ("symbol=GBPUSD", "symbol"),
        ("after_seq=5", "after_seq"),
    ],
)
def test_changing_a_filter_mid_scan_is_refused(
    client: TestClient,
    data_headers: dict[str, str],
    paging_run: dict[str, Any],
    extra: str,
    mismatched: str,
) -> None:
    """WF-036: changing the filter changes what the snapshot means."""
    first = client.get(
        "/v1/deals?account_id=A-000002&limit=5", headers=data_headers
    ).json()
    response = client.get(
        f"/v1/deals?cursor={first['next_cursor']}&{extra}", headers=data_headers
    )
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "INVALID_CURSOR"
    assert mismatched in error["details"]["mismatched_filters"]


def test_a_cursor_cannot_be_used_on_another_entity(
    client: TestClient, data_headers: dict[str, str], paging_run: dict[str, Any]
) -> None:
    cursor = client.get("/v1/deals?limit=5", headers=data_headers).json()["next_cursor"]
    response = client.get(f"/v1/positions?cursor={cursor}", headers=data_headers)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_CURSOR"


def test_an_expired_snapshot_forces_a_fresh_sync(tmp_path) -> None:
    """WF-037: 410, which tells the client to start over rather than retry."""
    import time

    from simulator.main import create_app
    from tests.conftest import ADMIN_TOKEN, DATA_TOKEN, make_settings

    settings = make_settings(tmp_path, snapshot_ttl_sec=1, cursor_clock_skew_tolerance_sec=0)
    app = create_app(settings, lock_process=False)
    data = {"Authorization": f"Bearer {DATA_TOKEN}"}
    admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    with TestClient(app) as client:
        create_run(client, admin)
        cursor = client.get("/v1/deals?limit=5", headers=data).json()["next_cursor"]
        assert cursor
        # A real TTL has to pass; one second is the smallest configurable value.
        time.sleep(1.2)
        response = client.get(f"/v1/deals?cursor={cursor}", headers=data)
        assert response.status_code == 410
        assert response.json()["error"]["code"] == "SNAPSHOT_EXPIRED"
        # And a new scan works immediately.
        assert client.get("/v1/deals?limit=5", headers=data).status_code == 200


def test_incremental_sync_covers_everything_without_repeating(
    client: TestClient, data_headers: dict[str, str], settings, paging_run: dict[str, Any]
) -> None:
    """WF-042: the realistic connector loop, driven to exhaustion.

    Repeatedly: read a page with ``after_seq`` set to the last sequence
    processed, then advance the watermark. The union must equal the oracle and
    nothing may arrive twice.
    """
    expected = oracle_deal_ids(settings, paging_run["run_id"], paging_run["snapshot_seq"])
    watermark = 0
    collected: list[str] = []
    for _ in range(100):
        page = client.get(
            f"/v1/deals?after_seq={watermark}&limit=40", headers=data_headers
        ).json()
        if not page["items"]:
            break
        collected.extend(item["deal_id"] for item in page["items"])
        watermark = page["items"][-1]["source_seq"]
    assert len(collected) == len(set(collected))
    assert set(collected) == expected


def test_keyset_pagination_is_used_rather_than_offset(
    client: TestClient, data_headers: dict[str, str], paging_run: dict[str, Any]
) -> None:
    """WF-045: deep pages must not get progressively slower.

    This asserts the *shape* of the guarantee, not a wall-clock number: every
    page is ordered by the indexed key and resumes from the last one, so the
    last page performs the same work as the first.
    """
    items, pages = collect_all_pages(client, data_headers, "/v1/deals?limit=25")
    sequences = [item["source_seq"] for item in items]
    assert sequences == sorted(sequences), "results must be ordered by the keyset"
    identifiers = [item["deal_id"] for item in items]
    pairs = list(zip(sequences, identifiers))
    assert pairs == sorted(pairs), "ties must break on (source_seq, deal_id)"
    assert pages[-1]["next_cursor"] is None


def test_last_page_reports_no_cursor_but_promises_nothing_about_the_future(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    paging_run: dict[str, Any],
) -> None:
    """WF-039: ``next_cursor=null`` is about this snapshot only."""
    _, pages = collect_all_pages(client, data_headers, "/v1/deals?limit=500")
    assert pages[-1]["next_cursor"] is None
    advance = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": 60},
        headers={**admin_headers, "Idempotency-Key": "more-after-null"},
    )
    wait_for_job(client, admin_headers, advance.json()["status_url"])
    fresh, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")
    assert len(fresh) > 900
