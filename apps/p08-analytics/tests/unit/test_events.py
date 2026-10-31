"""P07 envelope validation. Section 2.1, what-if domains B, C, G, K."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from analytics.canonical import digest_of
from analytics.contracts.events import (
    ACCOUNT_SNAPSHOT_PUBLISHED,
    CANONICAL_DEAL_FIELDS,
    DEAL_RECORDED,
    SUPPORTED_SCHEMA_VERSIONS,
    canonical_deal_fingerprint,
    parse_envelope,
)
from analytics.errors import PoisonError, Reason
from analytics.fixtures.golden import (
    DEFAULT_EPOCH,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DEFAULT_TOPIC,
    deal_envelope,
    snapshot_marker_envelope,
    specs_for,
)

pytestmark = pytest.mark.unit

ALLOWED = frozenset({DEFAULT_TOPIC})


def resolver(topic: str, source_id: str) -> str | None:
    if (topic, source_id) == (DEFAULT_TOPIC, DEFAULT_SOURCE):
        return DEFAULT_TENANT
    return None


def parse(record, **kwargs):
    defaults = dict(
        topic=DEFAULT_TOPIC,
        partition=0,
        offset=0,
        tenant_resolver=resolver,
        allowed_topics=ALLOWED,
    )
    defaults.update(kwargs)
    return parse_envelope(record, **defaults)


@pytest.fixture
def spec():
    return specs_for("G08-01")[0]


class TestHappyPath:
    def test_accepts_a_well_formed_deal(self, spec):
        envelope = parse(deal_envelope(spec))
        assert envelope.event_type == DEAL_RECORDED
        assert envelope.tenant_id == DEFAULT_TENANT
        assert envelope.deal_id == spec.deal_id
        assert envelope.deal is not None
        assert envelope.deal.money_currency == "USD"
        assert envelope.scope == (DEFAULT_TENANT, DEFAULT_SOURCE, DEFAULT_EPOCH)
        assert envelope.deal_key == (DEFAULT_TENANT, DEFAULT_SOURCE, DEFAULT_EPOCH, spec.deal_id)

    def test_accepts_a_snapshot_marker(self):
        envelope = parse(snapshot_marker_envelope())
        assert envelope.event_type == ACCOUNT_SNAPSHOT_PUBLISHED
        assert envelope.deal is None
        assert not envelope.is_deal

    def test_fingerprint_matches_the_fixture_side(self, spec):
        envelope = parse(deal_envelope(spec))
        assert canonical_deal_fingerprint(envelope.deal) == spec.deal_payload_hash()

    def test_money_written_two_ways_hashes_alike(self, spec):
        """``12.30`` and ``12.300`` are one amount, so one fingerprint.

        Without the decimal normalisation a producer that trimmed a trailing
        zero would look like a payload conflict (INV08-02) rather than a
        redelivery.
        """
        padded = replace(spec, realized_pnl_gross=spec.realized_pnl_gross + "0")
        a = parse(deal_envelope(spec))
        b = parse(deal_envelope(padded))
        assert a.deal_payload_hash == b.deal_payload_hash
        assert a.payload_sha256 == b.payload_sha256


class TestTenantResolution:
    def test_unmapped_source_is_refused(self, spec):
        """There is no default tenant. Section 6.1 step 1."""
        unknown = replace(spec, source_id="sim-broker-99")
        with pytest.raises(PoisonError) as excinfo:
            parse(deal_envelope(unknown))
        assert excinfo.value.reason is Reason.TENANT_UNMAPPED

    def test_payload_cannot_declare_its_own_tenant(self, spec):
        record = dict(deal_envelope(spec))
        payload = dict(record["payload"])
        payload["tenant_id"] = "rival-tenant"
        record["payload"] = payload
        record["payload_sha256"] = digest_of(payload)
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.TENANT_UNMAPPED

    def test_envelope_tenant_must_match_the_mapping(self, spec):
        record = dict(deal_envelope(spec))
        record["tenant_id"] = "rival-tenant"
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.TENANT_UNMAPPED

    def test_matching_envelope_tenant_is_accepted(self, spec):
        record = dict(deal_envelope(spec))
        record["tenant_id"] = DEFAULT_TENANT
        assert parse(record).tenant_id == DEFAULT_TENANT


class TestTopicAndType:
    def test_non_allowlisted_topic_is_refused(self, spec):
        with pytest.raises(PoisonError) as excinfo:
            parse(deal_envelope(spec), topic="broker.other.v1")
        assert excinfo.value.reason is Reason.TOPIC_NOT_ALLOWED

    def test_unknown_event_type_is_refused(self, spec):
        record = dict(deal_envelope(spec))
        record["event_type"] = "DealCorrected"
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.EVENT_TYPE_NOT_ALLOWED

    def test_v2_schema_is_refused_until_the_correction_contract_exists(self, spec):
        """Section 7.3: an unapproved correction must not rewrite money."""
        record = dict(deal_envelope(spec, schema_version=2))
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.SCHEMA_VERSION_UNSUPPORTED

    def test_only_v1_is_supported(self):
        assert SUPPORTED_SCHEMA_VERSIONS == frozenset({1})


class TestHashIntegrity:
    def test_payload_hash_mismatch_is_poison(self, spec):
        with pytest.raises(PoisonError) as excinfo:
            parse(deal_envelope(spec, corrupt_payload_hash=True))
        assert excinfo.value.reason is Reason.HASH_MISMATCH

    def test_deal_hash_mismatch_is_poison(self, spec):
        with pytest.raises(PoisonError) as excinfo:
            parse(deal_envelope(spec, corrupt_deal_hash=True))
        assert excinfo.value.reason is Reason.HASH_MISMATCH

    @pytest.mark.parametrize("bad", ["", "abc", "A" * 64, "g" * 64, "a" * 63])
    def test_malformed_hash_shape_is_poison(self, spec, bad):
        record = dict(deal_envelope(spec))
        record["payload_sha256"] = bad
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.HASH_MALFORMED

    def test_provenance_deal_id_must_match_payload(self, spec):
        record = dict(deal_envelope(spec))
        provenance = dict(record["provenance"])
        provenance["deal_id"] = "D-OTHER"
        record["provenance"] = provenance
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.FIELD_INVALID

    def test_canonical_field_set_is_pinned(self):
        """Adding a field here is a cross-project contract change."""
        assert CANONICAL_DEAL_FIELDS == (
            "account_id",
            "commission",
            "deal_id",
            "entry_kind",
            "executed_at",
            "money_currency",
            "realized_pnl_gross",
            "side",
            "swap",
            "symbol",
            "volume_lots",
        )


class TestFieldValidation:
    @pytest.mark.parametrize(
        "field",
        [
            "event_id",
            "dedupe_key",
            "event_type",
            "schema_version",
            "source_id",
            "epoch_id",
            "aggregate_type",
            "aggregate_id",
            "aggregate_version",
            "created_at",
            "payload",
            "payload_sha256",
            "provenance",
        ],
    )
    def test_missing_envelope_field_is_poison(self, spec, field):
        record = dict(deal_envelope(spec))
        record.pop(field)
        with pytest.raises(PoisonError):
            parse(record)

    @pytest.mark.parametrize(
        "field",
        [
            "deal_id",
            "account_id",
            "symbol",
            "side",
            "entry_kind",
            "executed_at",
            "volume_lots",
            "realized_pnl_gross",
            "commission",
            "swap",
            "money_currency",
            "fee_coverage",
        ],
    )
    def test_missing_payload_field_is_poison(self, spec, field):
        record = dict(deal_envelope(spec))
        payload = dict(record["payload"])
        payload.pop(field)
        record["payload"] = payload
        record["payload_sha256"] = digest_of(_decimalise(payload))
        with pytest.raises(PoisonError):
            parse(record)

    def test_null_field_is_poison(self, spec):
        record = dict(deal_envelope(spec))
        record["event_id"] = None
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.FIELD_MISSING

    @pytest.mark.parametrize("value", ["not-a-uuid", "", "12345", "x" * 40])
    def test_bad_uuid_is_poison(self, spec, value):
        record = dict(deal_envelope(spec))
        record["event_id"] = value
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason in (Reason.FIELD_INVALID, Reason.FIELD_MISSING)

    @pytest.mark.parametrize("value", [-1, -(2**63), 2**63, True, "5", 1.5])
    def test_bad_integer_coordinate_is_poison(self, spec, value):
        """P08-E-12: negative or wrapped integers are refused at the type edge."""
        record = dict(deal_envelope(spec))
        record["aggregate_version"] = value
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.FIELD_INVALID

    def test_unknown_side_is_poison(self, spec):
        record = _with_payload(spec, {"side": "HOLD"})
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.SIDE_UNKNOWN

    def test_unknown_entry_kind_is_poison(self, spec):
        record = _with_payload(spec, {"entry_kind": "SIDEWAYS"})
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.ENTRY_KIND_UNKNOWN

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_blank_symbol_is_poison_not_defaulted(self, spec, value):
        """P08-U-08: a null symbol is never mapped to a default instrument."""
        record = _with_payload(spec, {"symbol": value})
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason in (Reason.SYMBOL_UNKNOWN, Reason.FIELD_MISSING)

    @pytest.mark.parametrize("value", ["0.00", "-1.00"])
    def test_non_positive_volume_is_poison(self, spec, value):
        record = _with_payload(spec, {"volume_lots": value})
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.FIELD_INVALID

    def test_naive_executed_at_is_poison(self, spec):
        record = _with_payload(spec, {"executed_at": "2026-10-30T09:00:00"})
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.FIELD_INVALID

    def test_unsupported_currency_shape_is_poison(self, spec):
        record = _with_payload(spec, {"money_currency": "usd"})
        with pytest.raises(PoisonError) as excinfo:
            parse(record)
        assert excinfo.value.reason is Reason.CURRENCY_UNSUPPORTED


class TestEnvelopeSize:
    def test_oversized_envelope_is_refused_before_parsing(self, spec):
        with pytest.raises(PoisonError) as excinfo:
            parse(deal_envelope(spec), raw_size=10_000, max_bytes=1_000)
        assert excinfo.value.reason is Reason.ENVELOPE_TOO_LARGE

    def test_non_mapping_record_is_refused(self):
        with pytest.raises(PoisonError) as excinfo:
            parse(["not", "an", "object"])
        assert excinfo.value.reason is Reason.ENVELOPE_MALFORMED


def _decimalise(payload):
    from decimal import Decimal

    from analytics.canonical import parse_timestamp

    out = dict(payload)
    for key in ("realized_pnl_gross", "commission", "swap", "volume_lots"):
        if key in out:
            out[key] = Decimal(str(out[key]))
    if isinstance(out.get("executed_at"), str):
        out["executed_at"] = parse_timestamp(out["executed_at"], "executed_at")
    return out


def _with_payload(spec, overrides):
    """Build an envelope whose declared hashes match the overridden payload.

    Overriding the payload without recomputing the hash would make every such
    test fail on HASH_MISMATCH and never reach the field validation it means
    to exercise.
    """
    record = dict(deal_envelope(spec))
    payload = dict(record["payload"])
    payload.update(overrides)
    record["payload"] = payload
    try:
        record["payload_sha256"] = digest_of(_decimalise(payload))
    except Exception:
        # The override is unhashable (a null symbol, say). The envelope hash
        # then cannot match, but validation rejects the field first.
        record["payload_sha256"] = "0" * 64
    return record
