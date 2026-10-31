"""Privilege separation, redaction and adversarial inputs (sections 12.2, 12.4).

Four things are checked here, each corresponding to a way an audit tool can
become the thing that damages the data it was meant to protect:

* **Privilege.** The reconciliation role cannot write any Project 03 table,
  cannot advance a checkpoint, and cannot alter the schema. Enforced by the
  engine, so these are observations rather than promises (INV04-07, INV04-18).
* **Redaction.** Audit records and log details must not become a second copy
  of financial data. Money-bearing keys are stripped before storage.
* **Adversarial input.** A hostile source payload -- SQL in an identifier, a
  path traversal, a huge string, a float where money belongs -- is refused at
  the canonical boundary and cannot reach the database as data or as SQL.
* **Tamper evidence.** Editing a stored audit row breaks the hash chain at a
  reportable position.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from reconciliation.approvals import APPROVING_ROLES, ApprovalService
from reconciliation.approvals import Role as ApprovalRole
from reconciliation.audit import AuditLog, redact, verify_chain
from reconciliation.canonical import item_key, payload_hash
from reconciliation.db.engine import Role, transaction
from reconciliation.db.schema import P03_OWNED_TABLES, P04_OWNED_TABLES
from reconciliation.errors import CanonicalizationError, ReasonCode

pytestmark = [pytest.mark.security]


# ---------------------------------------------------------------------------
# Privilege separation
# ---------------------------------------------------------------------------


def test_recon_role_cannot_write_any_p03_table(db) -> None:
    conn = db.connect(Role.RECON)
    try:
        for table in sorted(P03_OWNED_TABLES):
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                conn.execute(f"DELETE FROM {table}")
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                conn.execute(f"UPDATE {table} SET rowid = rowid")
    finally:
        conn.close()


def test_recon_role_can_read_every_p03_table(db) -> None:
    """Reading is the whole job; only writing is denied (section 3.2)."""
    conn = db.connect(Role.RECON)
    try:
        for table in sorted(P03_OWNED_TABLES):
            conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    finally:
        conn.close()


def test_recon_role_cannot_advance_the_deal_checkpoint(db) -> None:
    """INV04-18: an audit must never move P03's watermark."""
    conn = db.connect(Role.RECON)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            conn.execute("UPDATE sync_checkpoints SET watermark_seq = 999999999")
    finally:
        conn.close()


def test_recon_role_cannot_change_the_schema(db) -> None:
    """A role that can DROP the table it cannot write has no separation."""
    conn = db.connect(Role.RECON)
    try:
        for statement in (
            "DROP TABLE deals",
            "ALTER TABLE deals RENAME TO deals_old",
            "CREATE TABLE smuggled (x TEXT)",
            "CREATE INDEX idx_smuggled ON deals(deal_id)",
        ):
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                conn.execute(statement)
    finally:
        conn.close()


def test_p03_role_cannot_write_p04_evidence_tables(db) -> None:
    """The separation runs both ways: P03 cannot rewrite the audit's findings."""
    conn = db.connect(Role.P03_SYNC)
    try:
        for table in sorted(P04_OWNED_TABLES):
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                conn.execute(f"DELETE FROM {table}")
    finally:
        conn.close()


def test_the_two_ownership_sets_are_disjoint() -> None:
    assert not (P03_OWNED_TABLES & P04_OWNED_TABLES)


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_redact_strips_money_and_secret_bearing_keys() -> None:
    safe = redact(
        {
            "account_id": "A-000001",
            "balance": "10019.94",
            "commission": "-2.05",
            "lease_token": "secret-token",
            "nested": {"swap": "1.00", "count": 3},
            "count": 7,
        }
    )
    assert safe["account_id"] == "A-000001"
    assert safe["count"] == 7
    assert safe["balance"] == "[redacted]"
    assert safe["commission"] == "[redacted]"
    assert safe["lease_token"] == "[redacted]"
    assert safe["nested"]["swap"] == "[redacted]"
    assert safe["nested"]["count"] == 3


def test_audit_rows_never_store_raw_money(db, clock) -> None:
    """An audit export must not become a copy of the ledger (section 12.2)."""
    conn = db.connect(Role.RECON)
    try:
        with transaction(conn):
            AuditLog(conn, clock).append(
                "TEST_EVENT",
                actor_id="tester",
                account_id="A-000001",
                balance="10019.94",
                commission="-2.05",
            )
        stored = conn.execute("SELECT safe_details FROM recon_audit").fetchone()[0]
    finally:
        conn.close()

    assert "10019.94" not in stored
    assert "-2.05" not in stored
    assert "A-000001" in stored
    assert "[redacted]" in stored


def test_source_token_is_not_present_in_settings_repr(settings) -> None:
    """A credential must not leak through a log line or a traceback."""
    rendered = repr(settings.policy_payload())
    assert "source_data_token" not in rendered


# ---------------------------------------------------------------------------
# Tamper evidence
# ---------------------------------------------------------------------------


def test_audit_chain_detects_an_edited_row(db, clock) -> None:
    conn = db.connect(Role.RECON)
    try:
        with transaction(conn):
            log = AuditLog(conn, clock)
            for index in range(5):
                log.append("TEST_EVENT", actor_id="tester", index=index)
        assert verify_chain(conn)[0] is True

        # Tamper with the middle event's payload.
        with transaction(conn):
            conn.execute(
                "UPDATE recon_audit SET safe_details = '{\"index\":99}' WHERE sequence_no = 3"
            )

        ok, bad_sequence, detail = verify_chain(conn)
    finally:
        conn.close()

    assert ok is False
    assert bad_sequence == 3
    assert "event_hash" in (detail or "")


def test_audit_chain_detects_a_deleted_row(db, clock) -> None:
    conn = db.connect(Role.RECON)
    try:
        with transaction(conn):
            log = AuditLog(conn, clock)
            for index in range(4):
                log.append("TEST_EVENT", actor_id="tester", index=index)
        with transaction(conn):
            conn.execute("DELETE FROM recon_audit WHERE sequence_no = 2")
        ok, bad_sequence, detail = verify_chain(conn)
    finally:
        conn.close()

    assert ok is False
    assert bad_sequence == 3
    assert "contiguous" in (detail or "") or "prev_event_hash" in (detail or "")


# ---------------------------------------------------------------------------
# Adversarial source input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    [
        "A-1'; DROP TABLE deals;--",
        "A-1 OR 1=1",
        "../../etc/passwd",
        "A-1\x00truncated",
        "A-1\n\rinjected",
        "<script>alert(1)</script>",
        "A" * 5000,
        "A-1; DELETE FROM accounts_current",
        "${jndi:ldap://evil/x}",
        "A-1‮RTL",
    ],
)
def test_hostile_identifiers_are_refused_at_the_canonical_boundary(hostile: str) -> None:
    """Refused by policy before they can be stored or interpolated."""
    record = {"account_id": hostile, "currency": "USD", "balance": "1.00", "credit": "0.00"}
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", record)
    with pytest.raises(CanonicalizationError):
        item_key("accounts", record)


def test_hostile_item_key_cannot_alter_the_database_even_if_stored(db, repository, clock) -> None:
    """Defence in depth: parameterised SQL makes a stored key inert.

    The canonical layer already refuses these, so this test inserts one
    directly to prove the storage layer would not execute it either.
    """
    epoch_id = str(uuid.uuid4())
    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute("INSERT OR IGNORE INTO sync_sources VALUES('sim_local','local')")
            conn.execute(
                "INSERT INTO source_epochs VALUES(?,?,?,?,?,?)",
                (epoch_id, "sim_local", "run_0001", 1, "ACTIVE", "2026-01-01T00:00:00.000000Z"),
            )
    finally:
        conn.close()

    run = repository.create_run(
        request_id="hostile-1", source_id="sim_local", epoch_id=epoch_id, entity="deals"
    )
    hostile = "D-1'); DROP TABLE deals;--"
    with repository.session() as conn, transaction(conn):
        repository.record_target_manifest(conn, run.recon_run_id, [(hostile, "a" * 64, 1)])

    conn = db.connect(Role.RECON)
    try:
        # The table still exists and the hostile text is inert data.
        assert conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0] == 0
        stored = conn.execute(
            "SELECT item_key FROM recon_target_items WHERE recon_run_id = ?", (run.recon_run_id,)
        ).fetchone()[0]
        assert stored == hostile
    finally:
        conn.close()


def test_float_money_from_a_hostile_source_cannot_enter_a_manifest() -> None:
    """A float is refused, so it can never reach a comparison (section 6.4)."""
    with pytest.raises(CanonicalizationError):
        payload_hash(
            "accounts",
            {"account_id": "A-1", "currency": "USD", "balance": 0.1, "credit": "0.00"},
        )


def test_oversized_and_unknown_fields_are_refused() -> None:
    base = {"account_id": "A-1", "currency": "USD", "balance": "1.00", "credit": "0.00"}
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", {**base, "injected_field": "x" * 100_000})


# ---------------------------------------------------------------------------
# Approval roles
# ---------------------------------------------------------------------------


def test_service_identity_is_not_an_approving_role() -> None:
    """Automation must not be able to authorise its own proposals."""
    assert ApprovalRole.SERVICE not in APPROVING_ROLES
    assert ApprovalRole.AUDITOR not in APPROVING_ROLES
    assert ApprovalRole.APPROVER in APPROVING_ROLES


def test_unknown_role_string_is_rejected(repository, settings, clock) -> None:
    """An unrecognised principal is refused on its own terms.

    Reported as ``APPROVAL_ROLE_INVALID`` rather than "unknown plan", so an
    attempt to approve with an invented role is visible in the audit trail as
    exactly that.
    """
    service = ApprovalService(repository=repository, settings=settings, clock=clock)
    outcome = service.decide(
        plan_id="whatever",
        actor_id="x",
        role="ROOT",
        decision="APPROVE",
        plan_hash="0" * 64,
    )
    assert outcome.allowed is False
    assert outcome.reason_code is ReasonCode.APPROVAL_ROLE_INVALID


def test_unknown_decision_verb_is_rejected(repository, settings, clock) -> None:
    service = ApprovalService(repository=repository, settings=settings, clock=clock)
    outcome = service.decide(
        plan_id="whatever",
        actor_id="x",
        role=ApprovalRole.APPROVER,
        decision="MAYBE",
        plan_hash="0" * 64,
    )
    assert outcome.allowed is False
    assert outcome.reason_code is ReasonCode.APPROVAL_ROLE_INVALID


def test_approval_for_an_unknown_plan_is_refused(repository, settings, clock) -> None:
    service = ApprovalService(repository=repository, settings=settings, clock=clock)
    outcome = service.decide(
        plan_id=str(uuid.uuid4()),
        actor_id="approver-2",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash="0" * 64,
    )
    assert outcome.allowed is False
