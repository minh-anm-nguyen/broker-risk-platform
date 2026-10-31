"""The invariant suite itself, and the honesty of its scorecard.

A suite that reports green because it did not look is worse than no suite, so
these tests check two different things:

* every storage-scope invariant really does pass on real datasets, and
* the suite *detects* a violation when one is introduced deliberately.

The second half matters more. An invariant check that cannot fail has never
been shown to check anything (WF-114).
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.domain import invariants as inv
from simulator.persistence.connection import open_connection
from tests.conftest import create_run, wait_for_job

pytestmark = pytest.mark.integration

STORAGE_IDS = [
    spec.invariant_id for spec in inv.INVARIANTS if spec.scope == inv.STORAGE_SCOPE
]
HARNESS_IDS = [
    spec.invariant_id for spec in inv.INVARIANTS if spec.scope == inv.HARNESS_SCOPE
]


def full_report(client: TestClient, admin_headers: dict[str, str]) -> dict[str, Any]:
    response = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_every_storage_invariant_passes_on_a_generated_dataset(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    report = full_report(client, admin_headers)
    results = {item["invariant_id"]: item for item in report["results"]}
    assert set(results) == {spec.invariant_id for spec in inv.INVARIANTS}
    for invariant_id in STORAGE_IDS:
        assert results[invariant_id]["status"] == "PASS", results[invariant_id]
    assert report["summary"]["storage_scope_ok"] is True
    assert report["summary"]["failed"] == 0


def test_harness_scope_invariants_are_never_reported_as_passing(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """The release-gate rule: NOT_APPLICABLE is not a pass.

    Each one also has to name the test that does prove it, so a reader of the
    scorecard can go and check.
    """
    report = full_report(client, admin_headers)
    results = {item["invariant_id"]: item for item in report["results"]}
    for invariant_id in HARNESS_IDS:
        item = results[invariant_id]
        assert item["status"] == "NOT_APPLICABLE"
        assert item["covered_by"], invariant_id
        assert item["covered_by"].startswith("tests/")
    assert report["summary"]["not_applicable"] == len(HARNESS_IDS)
    assert report["summary"]["passed"] == len(STORAGE_IDS)


def test_a_fast_sweep_refuses_to_claim_the_money_was_checked(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """A fast sweep is useful but must not produce a green gate."""
    fast = client.get(
        "/admin/v1/diagnostics/invariants?scope=fast", headers=admin_headers
    ).json()
    assert fast["scope"] == "fast"
    assert fast["canonical_digest"] == ""  # not computed, so not reported
    assert fast["summary"]["storage_scope_ok"] is False
    assert set(fast["summary"]["skipped_ids"]) == {"INV-05", "INV-06", "INV-07", "INV-08"}
    results = {item["invariant_id"]: item for item in fast["results"]}
    for invariant_id in fast["summary"]["skipped_ids"]:
        assert results[invariant_id]["status"] == "SKIPPED"
        assert "scope=full" in results[invariant_id]["detail"]


def test_the_report_records_what_it_actually_counted(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    report = full_report(client, admin_headers)
    counts = report["record_counts"]
    assert counts["deals"] == 120
    assert counts["accounts"] == 10
    assert counts["events"] == report["committed_seq"]
    assert counts["marks"] == 3
    assert report["canonical_digest"].startswith("sha256:")


def test_invariants_hold_across_a_long_mixed_workload(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """Generation, advances, injected events, reads and faults together."""
    create_run(client, admin_headers, account_count=12, deal_count=400)
    for index in range(3):
        advance = client.post(
            "/admin/v1/runs/run_0001/advance",
            json={"events": 120},
            headers={**admin_headers, "Idempotency-Key": f"mix-{index}"},
        )
        wait_for_job(client, admin_headers, advance.json()["status_url"])
        client.get("/v1/deals?limit=100", headers=data_headers)
        client.post(
            "/admin/v1/runs/run_0001/events",
            json={
                "kind": "ADJUST_ACCOUNT",
                "account_id": f"A-{index + 1:06d}",
                "amount": "-12.34",
                "reason": "mixed workload",
            },
            headers={**admin_headers, "Idempotency-Key": f"adj-{index}"},
        )

    report = full_report(client, admin_headers)
    assert report["summary"]["failed"] == 0
    assert report["summary"]["storage_scope_ok"] is True
    # INV-07 in particular: adjustments must be in the recomputation.
    results = {item["invariant_id"]: item for item in report["results"]}
    assert results["INV-07"]["status"] == "PASS"
    assert results["INV-07"]["observed"]["accounts_checked"] == 12


def test_repeated_open_and_close_on_one_account_replays_exactly(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """WF-023: many cycles on one account, replayed from the deal ledger.

    Driven through the API with explicit amounts rather than generated data, so
    the expected balance is arithmetic a reader can check by hand rather than
    something the implementation told us.
    """
    from decimal import Decimal

    create_run(client, admin_headers, account_count=1, deal_count=0)
    client.post(
        "/admin/v1/runs/run_0001/events",
        json={"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10000", "ask": "1.10010"},
        headers={**admin_headers, "Idempotency-Key": "wf023-mark"},
    )

    expected = Decimal("10000.00")
    for cycle in range(12):
        open_price = Decimal("1.10000")
        close_price = Decimal("1.10000") + Decimal("0.00010") * cycle
        volume = Decimal("0.10")
        commission = Decimal("-0.50")
        swap = Decimal("-0.10")

        opened = client.post(
            "/admin/v1/runs/run_0001/events",
            json={
                "kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
                "side": "BUY", "volume_lots": str(volume), "price": str(open_price),
                "commission": str(commission), "swap": "0.00",
            },
            headers={**admin_headers, "Idempotency-Key": f"wf023-open-{cycle}"},
        )
        assert opened.status_code == 201, opened.text
        expected += commission

        closed = client.post(
            "/admin/v1/runs/run_0001/events",
            json={
                "kind": "CLOSE_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
                "volume_lots": str(volume), "price": str(close_price),
                "commission": str(commission), "swap": str(swap),
            },
            headers={**admin_headers, "Idempotency-Key": f"wf023-close-{cycle}"},
        )
        assert closed.status_code == 201, closed.text
        gross = (close_price - open_price) * volume * Decimal("100000")
        expected += gross.quantize(Decimal("0.01")) + commission + swap

    published = client.get("/v1/accounts/A-000001", headers=data_headers).json()
    assert published["balance"] == str(expected.quantize(Decimal("0.01")))

    report = full_report(client, admin_headers)
    assert report["summary"]["failed"] == 0
    results = {item["invariant_id"]: item for item in report["results"]}
    assert results["INV-07"]["status"] == "PASS"
    # 24 deals, and each cycle opened a fresh position after the full close.
    assert report["record_counts"]["deals"] == 24
    assert report["record_counts"]["positions"] == 12


def test_an_unknown_run_is_404_rather_than_an_empty_pass(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    response = client.get(
        "/admin/v1/diagnostics/invariants?run_id=run_9999", headers=admin_headers
    )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Mutation testing: the suite must detect a deliberate violation
# ---------------------------------------------------------------------------


@pytest.fixture
def tampered(client: TestClient, admin_headers: dict[str, str], settings):
    """Seed a run, then hand the test a direct writable connection.

    Tampering goes around the service on purpose: the engine would refuse
    every one of these mutations, which is why they have to be introduced
    behind its back to test the detector.
    """
    create_run(client, admin_headers, account_count=5, deal_count=60)
    report = full_report(client, admin_headers)
    assert report["summary"]["failed"] == 0
    connection = open_connection(settings, writable=True)
    connection.execute("PRAGMA foreign_keys=OFF")  # so the tamper can land
    try:
        yield connection
    finally:
        connection.close()


def check(settings, invariant_id: str) -> inv.InvariantResult:
    connection = open_connection(settings, writable=False)
    try:
        from simulator.persistence import event_store as es

        run_id = es.load_active_run_id(connection)
        results = {
            item.invariant_id: item for item in inv.run_all(connection, run_id, scope=inv.FULL_SCOPE)
        }
        return results[invariant_id]
    finally:
        connection.close()


def test_inv01_detects_a_missing_event(tampered: sqlite3.Connection, settings) -> None:
    """A gap in the sequence is the signature of a lost commit."""
    tampered.execute(
        "DELETE FROM event_log WHERE run_id = (SELECT run_id FROM active_run) "
        "AND seq = (SELECT committed_seq / 2 FROM runs LIMIT 1)"
    )
    tampered.commit()
    result = check(settings, "INV-01")
    assert result.status == "FAIL"
    assert "does not equal committed_seq" in result.detail


def test_inv02_detects_a_duplicated_deal(tampered: sqlite3.Connection, settings) -> None:
    """WF-014: two deals sharing a source event."""
    row = tampered.execute(
        "SELECT * FROM deals LIMIT 1"
    ).fetchone()
    columns = row.keys()
    values = [row[name] for name in columns]
    values[columns.index("deal_id")] = "D-CLONE01"
    placeholders = ", ".join("?" for _ in columns)
    tampered.execute(
        f"INSERT INTO deals ({', '.join(columns)}) VALUES ({placeholders})", values
    )
    tampered.commit()
    result = check(settings, "INV-02")
    assert result.status == "FAIL"
    assert "source_seq" in result.detail


def test_inv03_detects_an_orphaned_deal(tampered: sqlite3.Connection, settings) -> None:
    """WF-013: a deal pointing at a position that does not exist."""
    tampered.execute("UPDATE deals SET position_id = 'P-GHOST' WHERE deal_id = 'D-00000001'")
    tampered.commit()
    result = check(settings, "INV-03")
    assert result.status == "FAIL"
    assert result.observed["orphan_position_refs"] >= 1


def test_inv04_detects_overlapping_versions(tampered: sqlite3.Connection, settings) -> None:
    """Two versions visible at one sequence breaks every as-of read."""
    tampered.execute(
        "UPDATE account_versions SET valid_to_seq = valid_to_seq + 1000 "
        "WHERE valid_to_seq IS NOT NULL AND account_id = 'A-000001'"
    )
    tampered.commit()
    result = check(settings, "INV-04")
    assert result.status == "FAIL"
    assert "overlapping" in result.detail


def test_inv05_detects_a_closed_position_with_volume(
    tampered: sqlite3.Connection, settings
) -> None:
    tampered.execute(
        "UPDATE position_versions SET volume_dec = '1.00' WHERE status = 'CLOSED'"
    )
    tampered.commit()
    result = check(settings, "INV-05")
    assert result.status == "FAIL"
    assert result.observed["closed_with_volume"]


def test_inv06_detects_over_closing(tampered: sqlite3.Connection, settings) -> None:
    """WF-016 from the other direction: detect it if it ever happened."""
    tampered.execute(
        "UPDATE deals SET volume_dec = '99.00' WHERE entry = 'OUT' AND deal_id = "
        "(SELECT deal_id FROM deals WHERE entry = 'OUT' LIMIT 1)"
    )
    tampered.commit()
    result = check(settings, "INV-06")
    assert result.status == "FAIL"


def test_inv07_detects_a_tampered_balance(tampered: sqlite3.Connection, settings) -> None:
    """The one that matters most: a balance that does not match the ledger."""
    tampered.execute(
        "UPDATE account_versions SET balance_dec = '999999.00' "
        "WHERE account_id = 'A-000001' AND valid_to_seq IS NULL"
    )
    tampered.commit()
    result = check(settings, "INV-07")
    assert result.status == "FAIL"
    assert result.observed["mismatches"]
    assert result.observed["mismatches"][0]["projected"] == "999999.00"


def test_inv07_detects_a_single_cent_of_drift(
    tampered: sqlite3.Connection, settings
) -> None:
    """A cent is enough. Anything coarser would miss real rounding bugs."""
    current = tampered.execute(
        "SELECT balance_dec FROM account_versions "
        "WHERE account_id = 'A-000002' AND valid_to_seq IS NULL"
    ).fetchone()[0]
    from decimal import Decimal

    drifted = str(Decimal(current) + Decimal("0.01"))
    tampered.execute(
        "UPDATE account_versions SET balance_dec = ? "
        "WHERE account_id = 'A-000002' AND valid_to_seq IS NULL",
        (drifted,),
    )
    tampered.commit()
    result = check(settings, "INV-07")
    assert result.status == "FAIL"


def test_inv14_detects_an_event_beyond_the_watermark(
    tampered: sqlite3.Connection, settings
) -> None:
    tampered.execute("UPDATE runs SET committed_seq = committed_seq - 5")
    tampered.commit()
    result = check(settings, "INV-14")
    assert result.status == "FAIL"
    assert result.observed["events_beyond"] == 5
    assert result.observed["max_event_seq"] == result.observed["committed_seq"] + 5


def test_inv15_detects_an_unmarked_replaced_run(
    tampered: sqlite3.Connection, settings
) -> None:
    tampered.execute("UPDATE runs SET status = 'REPLACED', replaced_at = NULL")
    tampered.commit()
    result = check(settings, "INV-15")
    assert result.status == "FAIL"


def test_a_tampered_dataset_changes_the_canonical_digest(
    tampered: sqlite3.Connection, settings
) -> None:
    """INV-09's detector: any change to the data changes the digest."""
    connection = open_connection(settings, writable=False)
    try:
        from simulator.persistence import event_store as es

        run_id = es.load_active_run_id(connection)
        before, _ = inv.canonical_state_digest(connection, run_id)
    finally:
        connection.close()

    tampered.execute(
        "UPDATE deals SET commission_dec = '-0.01' WHERE deal_id = 'D-00000001'"
    )
    tampered.commit()

    connection = open_connection(settings, writable=False)
    try:
        after, _ = inv.canonical_state_digest(connection, run_id)
    finally:
        connection.close()
    assert after != before


def test_the_cli_verify_command_fails_on_a_tampered_dataset(
    tampered: sqlite3.Connection, settings, monkeypatch, capsys
) -> None:
    """The release gate runs through the CLI, so the CLI must exit non-zero."""
    from simulator import cli

    tampered.execute(
        "UPDATE account_versions SET balance_dec = '1.00' "
        "WHERE account_id = 'A-000001' AND valid_to_seq IS NULL"
    )
    tampered.commit()

    monkeypatch.setattr(cli, "Settings", lambda: settings)
    exit_code = cli.main(["verify"])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "FAILED" in captured.err
    assert "INV-07" in captured.err


def test_the_cli_verify_command_passes_on_a_sound_dataset(
    client: TestClient, admin_headers: dict[str, str], settings, monkeypatch, capsys
) -> None:
    from simulator import cli

    create_run(client, admin_headers, account_count=4, deal_count=40)
    monkeypatch.setattr(cli, "Settings", lambda: settings)
    exit_code = cli.main(["verify", "--integrity"])
    captured = capsys.readouterr()
    assert exit_code == 0, captured.err
    assert "integrity_check: ok" in captured.out
    assert "canonical digest" in captured.out
    # The CLI says plainly which invariants it did not prove.
    assert "INV-09/10/11/12/16" in captured.out
