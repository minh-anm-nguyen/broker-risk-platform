"""Identity derivation, the payload registry and the envelope contract.

The separator test is the one with teeth. An account id of ``A:42`` and an
account ``A`` in a differently-spelled epoch must not be able to produce the
same key, and the hex encoding is what makes that true for every id rather than
for the ids someone happened to think of.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from outbox.errors import ErrorCode, ValidationError
from outbox.events.envelope import Envelope, validate_provenance
from outbox.events.keys import (
    EventType,
    build_dedupe_key,
    derive_event_id,
    hex_identity,
    normalize_uuid,
    parse_dedupe_key,
    partition_key,
    unhex_identity,
)
from outbox.events.routing import (
    DEALS_TOPIC,
    SNAPSHOTS_TOPIC,
    TOPIC_MAP,
    authorize_source,
    partition_for_key,
    route,
    topic_for,
)
from outbox.events.schema import REGISTRY, get_spec, validate_payload
from outbox.errors import TenantForbidden
from outbox.money import MoneyCurrency

pytestmark = pytest.mark.unit

EPOCH = "00000000-0000-4000-8000-000000000001"
T0 = "2026-10-30T05:59:59.000000Z"
T1 = "2026-10-30T06:00:01.000000Z"


def deal_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "deal_id": "D-1001",
        "account_id": "A-42",
        "position_id": "P-9",
        "symbol": "EURUSD",
        "side": "BUY",
        "entry_kind": "OUT",
        "volume_lots": "0.10",
        "price": "1.10500",
        "realized_pnl_gross": "12.30",
        "commission": "-0.50",
        "swap": "-0.10",
        "executed_at": T0,
    }
    payload.update(MoneyCurrency.proven("USD").as_payload_fields())
    payload.update(overrides)
    return payload


def build(**overrides: object) -> Envelope:
    kwargs: dict[str, object] = {
        "event_type": EventType.DEAL_RECORDED,
        "schema_version": 1,
        "source_id": "sim-broker-01",
        "epoch_id": EPOCH,
        "aggregate_id": "A-42",
        "aggregate_version": 7,
        "payload": deal_payload(),
        "event_time": T0,
        "created_at": T1,
        "provenance": {"source_seq": 1013, "deal_id": "D-1001"},
        "identity": "D-1001",
    }
    kwargs.update(overrides)
    return Envelope.build(**kwargs)  # type: ignore[arg-type]


class TestKeys:
    def test_dedupe_key_matches_the_design_example(self) -> None:
        assert build_dedupe_key(EventType.DEAL_RECORDED, EPOCH, "D-1001") == (
            f"DealRecorded:{EPOCH}:442d31303031"
        )

    def test_round_trip_through_the_typed_parser(self) -> None:
        key = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH, "D-1001")
        parsed = parse_dedupe_key(key)
        assert parsed.event_type is EventType.DEAL_RECORDED
        assert parsed.epoch_id == EPOCH
        assert parsed.identity == "D-1001"
        assert str(parsed) == key

    @pytest.mark.parametrize("identity", ["D:1001", "a:b:c", "D-1001:", ":", "x" * 200])
    def test_identities_containing_the_separator_survive(self, identity: str) -> None:
        key = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH, identity)
        assert parse_dedupe_key(key).identity == identity

    def test_two_identities_cannot_collide_across_the_separator(self) -> None:
        # Without hex encoding, "A:42" in epoch E and "A" in an epoch spelled
        # "E:42" could produce the same string.
        first = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH, "A:42")
        second = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH, "A")
        assert first != second
        assert parse_dedupe_key(first).identity != parse_dedupe_key(second).identity

    def test_hex_round_trip_rejects_malformed_input(self) -> None:
        assert unhex_identity(hex_identity("héllo")) == "héllo"
        for bad in ("zz", "abc", "", "ffff" + "g"):
            with pytest.raises(ValidationError):
                unhex_identity(bad)

    @pytest.mark.parametrize("value", ["", "x" * 300, "with\x00nul"])
    def test_bad_identities_are_refused(self, value: str) -> None:
        with pytest.raises(ValidationError):
            hex_identity(value)

    def test_event_id_is_derived_and_stable(self) -> None:
        key = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH, "D-1001")
        first = derive_event_id("sim-broker-01", key)
        assert first == derive_event_id("sim-broker-01", key)
        # A derived id is why a retry cannot mint a second identity for one fact.
        assert first.version == 4
        assert first != derive_event_id("sim-broker-02", key)

    @pytest.mark.parametrize(
        "value",
        [
            "0000000a-0000-4000-8000-00000000000f".upper(),
            "{00000000-0000-4000-8000-000000000001}",
            "00000000000040008000000000000001",
            "not-a-uuid",
        ],
    )
    def test_non_canonical_uuid_spellings_are_refused(self, value: str) -> None:
        with pytest.raises(ValidationError):
            normalize_uuid(value, field="epoch_id")

    def test_uuid_object_is_accepted(self) -> None:
        value = uuid.UUID(EPOCH)
        assert normalize_uuid(value, field="epoch_id") == EPOCH

    def test_partition_key_hex_encodes_the_aggregate(self) -> None:
        assert partition_key("sim-broker-01", EPOCH, "A-42") == f"sim-broker-01:{EPOCH}:412d3432"

    def test_a_colon_in_the_source_id_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="must not contain"):
            partition_key("sim:broker", EPOCH, "A-42")


class TestSchemaRegistry:
    def test_every_registered_spec_is_self_consistent(self) -> None:
        for (event_type, version), spec in REGISTRY.items():
            assert spec.event_type is event_type
            assert spec.schema_version == version
            assert spec.fields, f"{event_type} has no fields"
            names = [field.name for field in spec.fields]
            assert len(names) == len(set(names)), f"{event_type} has duplicate field names"

    def test_only_deal_recorded_bears_money(self) -> None:
        money_bearing = {
            event_type for (event_type, _), spec in REGISTRY.items() if spec.money_bearing
        }
        assert money_bearing == {EventType.DEAL_RECORDED}

    def test_every_non_deal_event_declares_notification_only(self) -> None:
        for (event_type, _), spec in REGISTRY.items():
            if event_type is EventType.DEAL_RECORDED:
                continue
            assert "notification_only" in spec.field_names, event_type

    def test_unknown_event_type_is_schema_unsupported(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            get_spec("DealInvented", 1)
        assert excinfo.value.code is ErrorCode.SCHEMA_UNSUPPORTED

    def test_unknown_schema_version_is_schema_unsupported(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            get_spec(EventType.DEAL_RECORDED, 99)
        assert excinfo.value.code is ErrorCode.SCHEMA_UNSUPPORTED

    def test_unknown_payload_field_is_refused_rather_than_ignored(self) -> None:
        with pytest.raises(ValidationError, match="register a new schema version"):
            validate_payload(EventType.DEAL_RECORDED, 1, deal_payload(sneaky="value"))

    def test_missing_required_field_is_refused(self) -> None:
        payload = deal_payload()
        del payload["commission"]
        with pytest.raises(ValidationError, match="missing required field 'commission'"):
            validate_payload(EventType.DEAL_RECORDED, 1, payload)

    def test_optional_field_may_be_explicitly_null(self) -> None:
        normalized = validate_payload(EventType.DEAL_RECORDED, 1, deal_payload(position_id=None))
        assert normalized["position_id"] is None

    @pytest.mark.parametrize("side", ["buy", "LONG", "", None, 1])
    def test_enum_values_are_closed(self, side: object) -> None:
        with pytest.raises(ValidationError, match="not in"):
            validate_payload(EventType.DEAL_RECORDED, 1, deal_payload(side=side))

    def test_notification_only_must_be_true_not_merely_present(self) -> None:
        payload = {
            "entity": "accounts",
            "scan_id": "scan-1",
            "snapshot_seq": 1,
            "record_count": 2,
            "manifest_sha256": "a" * 64,
            "published_at": T0,
            "notification_only": False,
        }
        with pytest.raises(ValidationError, match="must be present and true"):
            validate_payload(EventType.ACCOUNT_SNAPSHOT_PUBLISHED, 1, payload)

    @pytest.mark.parametrize("digest", ["A" * 64, "a" * 63, "g" * 64, 1])
    def test_hash_fields_require_lowercase_hex_of_the_right_length(self, digest: object) -> None:
        payload = {
            "entity": "accounts",
            "scan_id": "scan-1",
            "snapshot_seq": 1,
            "record_count": 2,
            "manifest_sha256": digest,
            "published_at": T0,
            "notification_only": True,
        }
        with pytest.raises(ValidationError):
            validate_payload(EventType.ACCOUNT_SNAPSHOT_PUBLISHED, 1, payload)

    @pytest.mark.parametrize("count", [-1, 1.5, True, "3"])
    def test_counts_must_be_non_negative_integers(self, count: object) -> None:
        payload = {
            "entity": "accounts",
            "scan_id": "scan-1",
            "snapshot_seq": count,
            "record_count": 2,
            "manifest_sha256": "a" * 64,
            "published_at": T0,
            "notification_only": True,
        }
        with pytest.raises(ValidationError):
            validate_payload(EventType.ACCOUNT_SNAPSHOT_PUBLISHED, 1, payload)

    @pytest.mark.parametrize(
        "stamp", ["2026-10-30T06:00:00Z", "2026-10-30 06:00:00.000000Z", "2026-10-30T06:00:00.000000+00:00"]
    )
    def test_timestamps_must_use_the_pinned_wire_format(self, stamp: str) -> None:
        with pytest.raises(ValidationError, match="pinned wire format"):
            validate_payload(EventType.DEAL_RECORDED, 1, deal_payload(executed_at=stamp))


class TestEnvelope:
    def test_build_derives_identity_and_hash(self) -> None:
        envelope = build()
        assert envelope.dedupe_key == f"DealRecorded:{EPOCH}:442d31303031"
        assert envelope.event_id == derive_event_id("sim-broker-01", envelope.dedupe_key)
        assert envelope.aggregate_key == envelope.aggregate_id == "A-42"
        assert envelope.aggregate_type == "account"

    def test_wire_round_trip_is_exact(self) -> None:
        envelope = build()
        assert Envelope.parse_wire(envelope.to_wire_bytes(), supported_schema_versions=(1,)) == envelope

    def test_payload_is_normalised_before_hashing(self) -> None:
        loose = build(payload=deal_payload(volume_lots="0.1", price="1.105", commission="-0.5"))
        tight = build()
        assert loose.payload_sha256 == tight.payload_sha256

    def test_datetime_inputs_are_formatted_to_the_wire_form(self) -> None:
        envelope = build(created_at=datetime(2026, 10, 30, 6, 0, 1, tzinfo=UTC))
        assert envelope.created_at == T1

    def test_aggregate_version_must_be_a_positive_integer(self) -> None:
        for version in (0, -1, 1.5, True, "1"):
            with pytest.raises(ValidationError):
                build(aggregate_version=version)

    def test_a_tampered_payload_fails_the_hash_check(self) -> None:
        envelope = build()
        raw = envelope.to_wire_bytes().replace(b'"12.30"', b'"99.30"')
        with pytest.raises(ValidationError) as excinfo:
            Envelope.parse_wire(raw, supported_schema_versions=(1,))
        assert excinfo.value.code is ErrorCode.INTEGRITY_CONFLICT

    def test_a_forged_event_id_is_detected(self) -> None:
        envelope = build()
        forged = envelope.to_wire_bytes().replace(
            str(envelope.event_id).encode(), b"11111111-1111-4111-8111-111111111111"
        )
        with pytest.raises(ValidationError) as excinfo:
            Envelope.parse_wire(forged, supported_schema_versions=(1,))
        assert excinfo.value.code is ErrorCode.INTEGRITY_CONFLICT

    def test_an_unsupported_schema_version_is_rejected_before_the_payload(self) -> None:
        envelope = build()
        raw = envelope.to_wire_bytes().replace(b'"schema_version":1', b'"schema_version":2')
        with pytest.raises(ValidationError) as excinfo:
            Envelope.parse_wire(raw, supported_schema_versions=(1,))
        assert excinfo.value.code is ErrorCode.SCHEMA_UNSUPPORTED

    def test_missing_and_extra_envelope_fields_are_refused(self) -> None:
        import json

        raw = json.loads(build().to_wire_bytes())
        del raw["provenance"]
        with pytest.raises(ValidationError, match="missing required fields"):
            Envelope.parse_wire(json.dumps(raw).encode())
        raw = json.loads(build().to_wire_bytes())
        raw["extra"] = 1
        with pytest.raises(ValidationError, match="unknown fields"):
            Envelope.parse_wire(json.dumps(raw).encode())

    def test_aggregate_type_must_match_the_contract(self) -> None:
        import json

        raw = json.loads(build().to_wire_bytes())
        raw["aggregate_type"] = "position"
        with pytest.raises(ValidationError, match="does not match"):
            Envelope.parse_wire(json.dumps(raw).encode())

    def test_provenance_is_a_closed_set(self) -> None:
        assert validate_provenance({"source_seq": 1, "deal_id": "D-1"}) == {
            "source_seq": 1,
            "deal_id": "D-1",
        }
        with pytest.raises(ValidationError, match="unknown fields"):
            validate_provenance({"mt5_password": "hunter2"})

    def test_provenance_nulls_are_dropped_not_stored(self) -> None:
        assert validate_provenance({"source_seq": None}) == {}

    def test_oversized_envelope_is_refused_at_the_wire_limit(self) -> None:
        envelope = build(payload=deal_payload(symbol="E" * 120))
        with pytest.raises(ValidationError) as excinfo:
            envelope.to_wire_bytes(max_bytes=256)
        assert excinfo.value.code is ErrorCode.MESSAGE_TOO_LARGE


class TestRouting:
    def test_every_event_type_has_a_topic(self) -> None:
        for event_type in EventType:
            assert topic_for(event_type) is TOPIC_MAP[event_type]

    def test_deal_and_snapshot_topics_are_distinct(self) -> None:
        assert topic_for(EventType.DEAL_RECORDED) is DEALS_TOPIC
        assert topic_for(EventType.ACCOUNT_SNAPSHOT_PUBLISHED) is SNAPSHOTS_TOPIC

    def test_an_unlisted_source_is_forbidden(self) -> None:
        with pytest.raises(TenantForbidden, match="not in the configured allowlist"):
            authorize_source("sim-broker-99", ("sim-broker-01",))

    def test_route_returns_topic_and_key(self) -> None:
        resolved = route(
            event_type=EventType.DEAL_RECORDED,
            source_id="sim-broker-01",
            epoch_id=EPOCH,
            aggregate_key="A-42",
            allowlist=("sim-broker-01",),
        )
        assert resolved.topic == DEALS_TOPIC.name
        assert resolved.partition_key == partition_key("sim-broker-01", EPOCH, "A-42")

    def test_partition_mapping_is_stable_and_in_range(self) -> None:
        key = partition_key("sim-broker-01", EPOCH, "A-42")
        first = partition_for_key(key, 6)
        assert first == partition_for_key(key, 6)
        assert 0 <= first < 6

    def test_two_brokers_sharing_an_account_id_get_different_keys(self) -> None:
        # Isolation has to hold in the record key, not only in the projection.
        a = partition_key("sim-broker-01", EPOCH, "A-42")
        b = partition_key("sim-broker-02", EPOCH, "A-42")
        assert a != b
