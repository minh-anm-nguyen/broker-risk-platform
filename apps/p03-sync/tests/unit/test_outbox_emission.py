"""The Project 03 to Project 07 seam, tested without a database.

Event construction is pure, so everything about the contract between the two
projects can be asserted here: field projection, money representation, event
identity, routing, and - most importantly - what happens to a Deal whose money
the event contract will not carry.

What these tests do NOT cover is the insert itself. That needs PostgreSQL and
lives in ``tests/integration_pg/test_outbox_atomicity.py``, which reports
BLOCKED_ENVIRONMENT without a DSN rather than passing (contract gap CG-08).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from platform_events.errors import MoneyPolicyError, ValidationError

from trading_sync.errors import OutboxEmissionError

from trading_sync.application.outbox_emission import (
    DEAL_SCHEMA_VERSION,
    build_deal_event,
    deal_payload,
    outbox_row,
)
from trading_sync.domain.entities import DealRecord

pytestmark = pytest.mark.unit

SOURCE = "sim_local"
EPOCH = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
OTHER_EPOCH = "3f2504e0-4f89-11d3-9a0c-0305e82c3302"
CREATED = datetime(2026, 10, 31, 6, 0, tzinfo=UTC)
ALLOWLIST = (SOURCE,)


def make_deal(**overrides: object) -> DealRecord:
    payload: dict[str, object] = {
        "deal_id": "D-100",
        "source_seq": 42,
        "account_id": "00042",
        "position_id": "P-9",
        "symbol": "EURUSD",
        "entry_kind": "OUT",
        "side": "BUY",
        "volume_lots": "0.10",
        "price": "1.09876",
        "realized_pnl_gross": "12.50",
        "commission": "-0.70",
        "swap": "0.00",
        "executed_at": "2026-10-30T06:00:00.000000Z",
    }
    payload.update(overrides)
    return DealRecord.from_payload(payload)


def emit(record: DealRecord, *, version: int = 1):
    return build_deal_event(
        record,
        source_id=SOURCE,
        epoch_id=EPOCH,
        aggregate_version=version,
        created_at=CREATED,
        source_allowlist=ALLOWLIST,
    )


# ------------------------------------------------------------------- payload


def test_every_money_field_travels_as_a_string() -> None:
    """A JSON number would arrive as a float on the far side of the wire. The
    whole platform money policy rests on this one property."""
    payload = deal_payload(make_deal())
    for field in ("volume_lots", "price", "realized_pnl_gross", "commission", "swap"):
        assert isinstance(payload[field], str), field


def test_the_currency_is_reported_unproven_rather_than_guessed() -> None:
    """Project 03 receives no per-deal currency. Filling in the account
    currency would be an inference presented as a fact, and a consumer
    aggregating by currency would act on it."""
    payload = deal_payload(make_deal())
    assert payload["money_currency"] is None
    assert payload["money_currency_status"] == "UNPROVEN"


def test_there_are_two_money_representations_and_the_envelope_holds_the_wire_one() -> None:
    """Worth knowing before reading the assertions below.

    ``deal_payload`` projects with Project 03 canonical form, which trims
    trailing fraction zeros: ``Decimal("0.70")`` becomes ``"0.7"``. The event
    contract then re-normalises to its own two-decimal money scale, so the
    validated envelope carries ``"0.70"``. Both are the same amount; only the
    envelope value goes on the wire, so that is what a consumer sees.
    """
    record = make_deal(commission="0.70")
    assert deal_payload(record)["commission"] == "0.7"
    assert emit(record).envelope.payload["commission"] == "0.70"


@pytest.mark.parametrize(
    ("given", "on_the_wire"),
    [
        ("-0.70", "-0.70"),
        ("0.70", "0.70"),
        ("0.00", "0.00"),
        ("-12.5", "-12.50"),
    ],
)
def test_commission_keeps_its_sign_and_magnitude_on_the_wire(
    given: str, on_the_wire: str
) -> None:
    """Commission may legitimately be either sign and nothing re-signs it.

    Zero is included because a sign-normalising bug often shows up first as
    ``-0.00``, which is the same money but different bytes, and therefore a
    different digest and a failed golden comparison.
    """
    assert emit(make_deal(commission=given)).envelope.payload["commission"] == on_the_wire


def test_a_null_position_id_survives_as_null() -> None:
    """A historical Deal may reference a position that has left the open set.
    Dropping the field, or substituting an empty string, would lose that."""
    assert deal_payload(make_deal(position_id=None))["position_id"] is None


# ------------------------------------------------------------------ identity


def test_event_identity_is_derived_from_the_deal_and_is_stable() -> None:
    """A retry must produce the same event id, so the outbox unique index
    rejects it instead of letting one business change become two events."""
    first = emit(make_deal())
    second = emit(make_deal())
    assert first.envelope.event_id == second.envelope.event_id
    assert first.envelope.dedupe_key == second.envelope.dedupe_key


def test_two_different_deals_get_different_identities() -> None:
    left = emit(make_deal(deal_id="D-100")).envelope.event_id
    right = emit(make_deal(deal_id="D-101")).envelope.event_id
    assert left != right


def test_the_same_deal_in_a_different_epoch_is_a_different_event() -> None:
    """A source reset can reuse a deal id. Two epochs must never collapse into
    one event, or a replayed epoch would silently overwrite history."""
    first = emit(make_deal())
    second = build_deal_event(
        make_deal(),
        source_id=SOURCE,
        epoch_id=OTHER_EPOCH,
        aggregate_version=1,
        created_at=CREATED,
        source_allowlist=ALLOWLIST,
    )
    assert first.envelope.event_id != second.envelope.event_id
    assert first.envelope.dedupe_key != second.envelope.dedupe_key


def test_the_aggregate_is_the_account_not_the_deal() -> None:
    """Ordering is per account, so the relay version guard protects an account
    event sequence rather than each deal in isolation."""
    event = emit(make_deal(account_id="00042"))
    assert event.envelope.aggregate_type == "account"
    assert event.envelope.aggregate_id == "00042"
    assert event.aggregate_key == "00042"


def test_event_time_is_the_execution_time_not_now() -> None:
    """A correction posted today for last week fill carries the old timestamp.
    Stamping it with `now` would misplace it for any consumer that orders by
    event time."""
    event = emit(make_deal(executed_at="2026-10-30T06:00:00.000000Z"))
    assert event.envelope.event_time.startswith("2026-10-30T06:00:00")
    assert event.envelope.created_at.startswith("2026-10-31T06:00:00")


def test_schema_version_is_declared_not_implied() -> None:
    assert emit(make_deal()).envelope.schema_version == DEAL_SCHEMA_VERSION == 1


# ------------------------------------------------------------------- routing


def test_deals_route_to_the_deal_topic_with_a_scoped_partition_key() -> None:
    event = emit(make_deal())
    assert event.topic == "broker.deals.v1"
    # Source and epoch are in the key, so two brokers that both have account
    # "00042" cannot share a partition lane.
    assert SOURCE in event.partition_key
    assert EPOCH in event.partition_key


def test_a_source_outside_the_allowlist_is_refused() -> None:
    """Routing authorises the source. A misconfigured source id must not be
    able to publish into another broker topic lane."""
    with pytest.raises(Exception):
        build_deal_event(
            make_deal(),
            source_id="someone-elses-broker",
            epoch_id=EPOCH,
            aggregate_version=1,
            created_at=CREATED,
            source_allowlist=ALLOWLIST,
        )


# ------------------------------------------- fails closed, never rounds (CG-10)


@pytest.mark.parametrize("field", ["realized_pnl_gross", "commission", "swap"])
def test_money_finer_than_the_contract_carries_is_refused_not_rounded(field: str) -> None:
    """Project 03 stores money at twelve decimal places; the event contract
    carries two and refuses the rest.

    This is the right refusal in the wrong place, recorded as contract gap
    CG-10. What matters here is that it *raises*: a rounded commission nobody
    was told about is worse than a halted scan, because the halted scan is
    visible.
    """
    record = make_deal(**{field: "0.005"})
    assert getattr(record, field) == Decimal("0.005")
    with pytest.raises(OutboxEmissionError) as excinfo:
        emit(record)

    # The message has to be usable by whoever reads it during an incident:
    # which deal, which field, both scales, and what the ways out are.
    message = str(excinfo.value)
    assert record.deal_id in message
    assert field in message
    assert "3 decimal places" in message
    assert "carries 2" in message


def test_out_of_scale_money_is_still_refused_by_the_contract_itself() -> None:
    """Defence in depth.

    ``assert_emittable`` is a better error message, not the safety mechanism.
    If someone bypasses it, the shared contract must still refuse the value -
    so this asserts the underlying refusal directly.
    """
    from platform_events.money import normalize_money

    with pytest.raises((MoneyPolicyError, ValidationError)):
        normalize_money("0.005", field="commission")


def test_a_naive_executed_at_cannot_be_emitted() -> None:
    """A timestamp whose zone is assumed is a timestamp that is wrong
    somewhere, so the wire projection refuses it."""
    record = make_deal()
    object.__setattr__(record, "executed_at", datetime(2026, 10, 30, 6, 0))
    with pytest.raises(ValueError):
        deal_payload(record)


# ---------------------------------------------------------------- outbox row


def test_the_row_carries_everything_the_relay_needs_to_publish() -> None:
    row = outbox_row(emit(make_deal()), created_at=CREATED)
    for column in (
        "event_id",
        "source_id",
        "epoch_id",
        "dedupe_key",
        "aggregate_type",
        "aggregate_key",
        "aggregate_version",
        "event_type",
        "schema_version",
        "topic",
        "partition_key",
        "payload",
        "payload_sha256",
        "wire_bytes",
        "created_at",
        "next_attempt_at",
    ):
        assert column in row, column


def test_the_producer_never_sets_the_relay_lifecycle_columns() -> None:
    """``state``, ``claim_owner``, ``claim_token``, ``lease_until`` and
    ``published_at`` belong to Project 07. A producer that set them could
    insert a row that already looks claimed, or already published."""
    row = outbox_row(emit(make_deal()), created_at=CREATED)
    for forbidden in (
        "state",
        "attempt_count",
        "claim_owner",
        "claim_token",
        "claim_version",
        "lease_until",
        "published_at",
        "kafka_partition",
        "kafka_offset",
    ):
        assert forbidden not in row, forbidden


def test_the_row_is_immediately_eligible_for_the_relay() -> None:
    row = outbox_row(emit(make_deal()), created_at=CREATED)
    assert row["next_attempt_at"] == row["created_at"] == CREATED


def test_wire_bytes_and_payload_hash_describe_the_same_payload() -> None:
    """The hash is what a consumer compares two deliveries of one event by. If
    it did not cover the bytes actually sent, a mutated duplicate could pass as
    a harmless one."""
    event = emit(make_deal())
    row = outbox_row(event, created_at=CREATED)
    assert row["payload_sha256"] == event.envelope.payload_sha256
    assert isinstance(row["wire_bytes"], bytes)
    assert len(row["payload_sha256"]) == 64


def test_the_wire_size_limit_is_enforced_when_one_is_given() -> None:
    with pytest.raises(Exception):
        outbox_row(emit(make_deal()), created_at=CREATED, max_wire_bytes=16)
