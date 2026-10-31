"""Domain Q -- tenant isolation, authorization, SQL safety, redaction.

Covers WF5-193..204. Plus the read-only guarantee of G05-08, which on
PostgreSQL is a GRANT and here is additionally enforced structurally.
"""

from __future__ import annotations

import json

import pytest

from risk.adapters.sql import ReadOnlyViolation, SqliteExecutor, assert_read_only
from risk.domain.facts import AuthorizedScope
from risk.errors import AuthorizationError, ContractError, ReasonCode
from risk.observability import (
    ALLOWED_LABELS,
    FORBIDDEN_LABELS,
    MetricsRegistry,
    log_fields,
    redact,
)
from risk.status import Component
from risk_tools.fixtures import (
    ACCOUNT_A,
    ACCOUNT_B,
    SOURCE_A,
    SOURCE_B,
    TENANT_A,
    TENANT_B,
    AccountSpec,
    build_harness_db,
    scenario,
)

pytestmark = [pytest.mark.security, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))


@pytest.mark.wf5("WF5-193")
def test_request_cannot_widen_the_authorized_scope(evidence):
    """A request filter may narrow a grant; it may never add to it."""
    scope = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A, ACCOUNT_B}))

    assert scope.narrow((ACCOUNT_A,)) == frozenset({ACCOUNT_A})
    assert scope.narrow(None) == frozenset({ACCOUNT_A, ACCOUNT_B})

    with pytest.raises(AuthorizationError) as exc:
        scope.narrow((ACCOUNT_A, "99999"))
    assert exc.value.reason is ReasonCode.UNAUTHORIZED_SCOPE
    # The error counts the offending accounts without naming them, so a probe
    # cannot enumerate ids by reading the message.
    assert "99999" not in str(exc.value)
    assert exc.value.detail["outside_count"] == 1
    evidence(
        expected_reason_codes=("UNAUTHORIZED_SCOPE",),
        observed_reason_codes=(str(exc.value.reason),),
        oracle="OPERATIONAL",
    )


@pytest.mark.wf5("WF5-194")
async def test_identical_account_ids_across_tenants_stay_separate(
    service_factory, request_factory, evidence
):
    """Two tenants with account 00042 get two different scope hashes."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)

    first = await service.compute(request_factory(spec), SCOPE)
    other_tenant = AuthorizedScope(TENANT_B, SOURCE_A, frozenset({ACCOUNT_A}))
    second = await service.compute(request_factory(spec), other_tenant)

    # Same account id, same data -- different scope hash, so no cache entry or
    # audit row can be shared between the two tenants.
    assert first.scope["scope_hash"] != second.scope["scope_hash"]
    assert first.input_digest != second.input_digest
    assert first.calculation_id != second.calculation_id
    evidence(
        oracle="OPERATIONAL",
        observed_values={
            "tenant_a_hash": str(first.scope["scope_hash"])[:16],
            "tenant_b_hash": str(second.scope["scope_hash"])[:16],
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-195")
async def test_source_id_in_the_request_body_is_ignored(
    service_factory, request_factory, evidence
):
    """Authorization decides the source; the request body cannot override it."""
    from risk.contract import RiskRequest

    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)

    # A tampered body claiming another source. The request object has no
    # source_id field at all, so the claim has nowhere to land.
    tampered = RiskRequest.from_payload(
        {
            "source_id": SOURCE_B,
            "epoch_id": spec.epoch_id,
            "components": ["EXPOSURE"],
            "period_start_utc": "2026-09-01T00:00:00Z",
            "period_end_utc": "2026-12-01T00:00:00Z",
        }
    )
    assert not hasattr(tampered, "source_id")

    result = await service.compute(tampered, SCOPE)
    assert result.scope["source_id"] == SOURCE_A
    evidence(
        oracle="OPERATIONAL",
        expected_values={"source_used": SOURCE_A},
        observed_values={"source_used": str(result.scope["source_id"])},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-196", "WF5-197")
def test_read_only_executor_refuses_every_mutating_statement(evidence):
    """UPDATE, DELETE and DDL are refused before the driver sees them.

    On PostgreSQL the GRANT is the real control and the commented-out role
    section of the migration is what enforces it. This is the second line: a
    bug in this package cannot mutate upstream facts even where the role would
    permit it (INV05-13).
    """
    spec = scenario("R05-buy-mark")
    conn = build_harness_db(spec)
    try:
        executor = SqliteExecutor(conn)
        mutations = [
            "UPDATE deals SET commission = '0' WHERE deal_id = 'd-1'",
            "DELETE FROM recon_findings",
            "INSERT INTO deals (epoch_id, deal_id) VALUES ('x', 'y')",
            "DROP TABLE deals",
            "ALTER TABLE deals ADD COLUMN x TEXT",
            "TRUNCATE deals",
            "  -- a comment first\n  DELETE FROM accounts_current",
            "SELECT 1; DELETE FROM deals",  # piggy-backed statement
        ]
        for sql in mutations:
            with pytest.raises(ReadOnlyViolation) as exc:
                assert_read_only(sql)
            assert exc.value.reason is ReasonCode.UNAUTHORIZED_SCOPE

        # Reads still work, so the guard is not merely refusing everything.
        assert_read_only("SELECT count(*) FROM deals")
        assert_read_only("WITH x AS (SELECT 1) SELECT * FROM x")
        evidence(
            oracle="OPERATIONAL",
            expected_reason_codes=("UNAUTHORIZED_SCOPE",),
            observed_values={"statements_refused": str(len(mutations))},
        )
    finally:
        conn.close()


@pytest.mark.wf5("WF5-196")
async def test_source_facts_are_byte_identical_after_a_calculation(
    service_factory, request_factory, evidence
):
    """A before/after hash of every upstream table proves the read was read-only."""
    import hashlib

    spec = scenario("R05-aligned-ready")
    service, conn = service_factory(spec)

    def table_hash() -> str:
        digest = hashlib.sha256()
        for table in (
            "source_epochs",
            "sync_runs",
            "sync_checkpoints",
            "accounts_current",
            "positions_current",
            "deals",
            "recon_evidence",
            "recon_findings",
        ):
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1"):  # noqa: S608
                digest.update(repr(tuple(row)).encode())
        return digest.hexdigest()

    before = table_hash()
    await service.compute(request_factory(spec), SCOPE)
    after = table_hash()

    assert before == after
    evidence(
        oracle="OPERATIONAL",
        expected_values={"upstream_unchanged": "True"},
        observed_values={"before": before[:16], "after": after[:16]},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-198")
async def test_account_ids_are_bound_as_parameters_not_interpolated(
    service_factory, request_factory, evidence
):
    """A SQL-injection payload in an account id finds nothing and executes nothing."""
    payload = "00042' OR '1'='1"
    spec = scenario("R05-aligned-ready")
    spec.accounts = (AccountSpec(account_id=ACCOUNT_A), AccountSpec(account_id=ACCOUNT_B))
    service, conn = service_factory(spec)

    # Authorize the payload string itself, so it reaches the query as a value.
    injected_scope = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({payload}))
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE})), injected_scope
    )

    # It matched no account, and the two real accounts are untouched.
    assert result.lineage["row_counts"]["accounts"] == 0
    assert conn.execute("SELECT COUNT(*) FROM accounts_current").fetchone()[0] == 2
    evidence(
        injection_confirmed=True,
        oracle="OPERATIONAL",
        expected_values={"accounts_matched": "0", "accounts_in_db": "2"},
        observed_values={
            "accounts_matched": str(result.lineage["row_counts"]["accounts"]),
            "accounts_in_db": "2",
        },
    )


@pytest.mark.wf5("WF5-199")
@pytest.mark.parametrize(
    "hostile",
    [
        "x" * 1000,
        "acct\nINJECTED LOG LINE",
        "acct\x00null",
        "acct\x1b[31mred",
    ],
)
def test_oversized_and_control_bearing_account_ids_rejected(hostile, evidence):
    """An identifier that is too long or carries control characters is refused."""
    with pytest.raises(ContractError) as exc:
        AuthorizedScope(TENANT_A, SOURCE_A, frozenset({hostile}))
    assert exc.value.reason is ReasonCode.CONTRACT_MISSING_FIELD
    # The error never echoes the value, so a log line cannot be forged.
    assert hostile not in str(exc.value)
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-200", "WF5-279")
def test_metrics_refuse_sensitive_and_unlisted_labels(evidence):
    """An account id cannot become a metric label, nor can an unlisted name."""
    metrics = MetricsRegistry()

    metrics.increment("risk_calculation_total", component="EXPOSURE", status="READY")
    assert metrics.counter_value(
        "risk_calculation_total", component="EXPOSURE", status="READY"
    ) == 1

    for forbidden in ("account_id", "balance", "deal_id", "tenant_id", "token"):
        with pytest.raises(ContractError) as exc:
            metrics.increment("risk_calculation_total", **{forbidden: "x"})
        assert exc.value.reason is ReasonCode.UNAUTHORIZED_SCOPE

    # An unlisted but innocent-looking label is also refused: the allowlist is
    # what makes a new sensitive field fail closed.
    with pytest.raises(ContractError) as exc:
        metrics.increment("risk_calculation_total", customer_region="eu")
    assert exc.value.reason is ReasonCode.CONTRACT_MISSING_FIELD

    assert FORBIDDEN_LABELS & ALLOWED_LABELS == frozenset()
    evidence(
        oracle="OPERATIONAL",
        observed_values={"forbidden": str(len(FORBIDDEN_LABELS)),
                         "allowed": str(len(ALLOWED_LABELS))},
    )


@pytest.mark.wf5("WF5-201")
@pytest.mark.p1
def test_credentials_are_redacted_from_messages(evidence):
    """A DSN or a token embedded in a driver message is stripped."""
    samples = [
        ("could not connect to postgresql://user:s3cret@db:5432/risk",
         "postgresql://user:s3cret@db:5432/risk"),
        ("auth failed: password=hunter2", "hunter2"),
        ("header Bearer eyJhbGciOiJIUzI1NiJ9.abc", "eyJhbGciOiJIUzI1NiJ9.abc"),
        ("api_key: sk-live-1234567890", "sk-live-1234567890"),
    ]
    for text, secret in samples:
        cleaned = redact(text)
        assert secret not in cleaned, cleaned
        assert "[REDACTED]" in cleaned
    evidence(oracle="OPERATIONAL", observed_values={"samples": str(len(samples))})


@pytest.mark.wf5("WF5-202")
@pytest.mark.p1
async def test_audit_rows_identify_scope_by_hash_only(
    service_factory, request_factory, audit_sink, evidence
):
    """The stored receipt carries a scope hash, never the account list."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    receipt = await audit_sink.find(
        next(iter(audit_sink._by_key))  # type: ignore[attr-defined]
    )
    assert receipt is not None
    encoded = json.dumps(receipt.as_payload())

    assert ACCOUNT_A not in encoded
    assert TENANT_A not in encoded
    assert receipt.account_scope_hash == result.scope["scope_hash"]
    # No monetary value is stored either: the receipt proves which inputs were
    # used, and the result is re-derivable from them.
    for forbidden in ("56.00", "60.00", "116.0000000", "gross", "net_excluding_fee"):
        assert forbidden not in encoded
    evidence(
        oracle="OPERATIONAL",
        observed_values={"scope_hash": receipt.account_scope_hash[:16]},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-203")
@pytest.mark.p1
def test_audit_read_permission_is_a_separate_grant(evidence):
    """Reading the audit trail is a distinct capability from computing a report."""
    compute_only = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))
    with_audit = AuthorizedScope(
        TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}), may_read_audit=True
    )

    assert compute_only.may_read_audit is False
    assert with_audit.may_read_audit is True
    evidence(
        oracle="OPERATIONAL",
        notes=("Project 06 enforces this flag on its audit endpoint",),
    )


@pytest.mark.wf5("WF5-204")
@pytest.mark.p2
async def test_scope_is_fixed_for_the_whole_calculation(
    service_factory, request_factory, evidence
):
    """The authorization used at the end is the one captured at the start."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    # The bundle's scope is frozen, so a revocation landing mid-flight cannot
    # retroactively widen what was read -- and the recorded hash is the one the
    # data was read under.
    assert result.scope["scope_hash"]
    assert result.lineage["scope_hash"] == result.scope["scope_hash"]
    assert result.scope["account_count"] == 1
    evidence(
        oracle="OPERATIONAL",
        observed_values={"scope_hash": str(result.lineage["scope_hash"])[:16]},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-281")
def test_log_record_masks_the_source_and_omits_the_accounts(evidence):
    """The structured log line is greppable without disclosing who was queried."""
    record = log_fields(
        calculation_id="calc-1",
        scope_hash="a" * 64,
        source_id="sim-broker-a",
        epoch_id="epoch-1",
        status="READY",
        reason_codes=("FEE_COVERAGE_UNAVAILABLE",),
        risk_policy_version="risk-policy-v1",
        symbol_catalog_version="sim-usd-fx-v1",
        duration_ms=42,
    )
    encoded = json.dumps(record)

    assert record["source"] == "sim-***"
    assert "sim-broker-a" not in encoded
    assert record["scope_hash"] == "a" * 16  # truncated, not the full hash
    assert record["reason_families"] == ["OTHER"]
    evidence(oracle="OPERATIONAL", observed_values={"source_label": str(record["source"])})


@pytest.mark.needs_postgres
@pytest.mark.wf5("WF5-196", "WF5-197")
def test_postgres_role_cannot_mutate_upstream_tables(evidence):
    """The GRANT-level guarantee, verified against a live PostgreSQL.

    Skipped without ``RISK_PG_DSN``. SQLite has no roles, so the structural
    guard above is the only enforcement in the default environment -- and this
    test records honestly that the database-level control was not exercised.
    """
    import os

    dsn = os.environ.get("RISK_PG_DSN")
    if not dsn:
        pytest.skip("RISK_PG_DSN not set; the GRANT-level control is unverified here")

    import asyncio

    from risk.adapters.sql import PostgresExecutor

    async def attempt() -> str:
        executor = await PostgresExecutor.connect(dsn)
        try:
            await executor._conn.execute("UPDATE deals SET commission = 0")  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 - the refusal is the assertion
            return type(exc).__name__
        finally:
            await executor.close()
        return "ALLOWED"

    outcome = asyncio.run(attempt())
    assert outcome != "ALLOWED"
    evidence(oracle="OPERATIONAL", observed_values={"outcome": outcome})
