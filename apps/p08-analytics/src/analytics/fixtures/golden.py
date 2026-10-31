"""Golden fixture builders for the 20 scenarios of section 9.3.

The envelopes here are built the way a *producer* builds them - money as
decimal strings, hashes computed over the canonical encoding - because a
fixture that skipped the hash would let a validation bug pass unnoticed.

Two deliberate properties:

* **A fixture can be invalid on purpose.** G08-03 carries an ``event_id`` with
  two different payload hashes, and G08-20 declares unknown fee coverage.
  Their expected outcome is a refusal, and a fixture builder that quietly
  repaired them would delete the test.
* **Nothing derives from P08's own output.** Expected totals come from the
  :class:`DealSpec` list via :mod:`decimal`, never from a rollup query
  (section 9.1, P08-R-09).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from ..canonical import digest_of, format_timestamp
from ..contracts.manifest import CutScope
from ..contracts.policy import FeeCoverage

DEFAULT_TENANT = "lab-tenant"
DEFAULT_SOURCE = "sim-broker-01"
DEFAULT_EPOCH = "00000000-0000-4000-8000-000000000001"
ALT_EPOCH = "00000000-0000-4000-8000-000000000002"
DEFAULT_TOPIC = "broker.deals.v1"
DEFAULT_REPORT_FAMILY = "daily_deal_v1"
DEFAULT_POLICY_VERSION = "usd-fx-synthetic-v1"
SYMBOL_CATALOG_VERSION = "sym-catalog-v1"

#: Fixed base instant. Section 13.1 insists a reproducible run pins its
#: inputs; a fixture that used ``now()`` would not be reproducible.
BASE_TIME = datetime(2026, 10, 30, 9, 0, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class DealSpec:
    """One Deal as the *source* describes it. The oracle's only input."""

    deal_id: str
    account_id: str
    symbol: str
    side: str
    entry_kind: str
    executed_at: datetime
    volume_lots: str
    realized_pnl_gross: str
    commission: str
    swap: str
    money_currency: str
    source_seq: int
    fee_coverage: str = str(FeeCoverage.COMMISSION_AND_SWAP_ONLY)
    symbol_catalog_version: str = SYMBOL_CATALOG_VERSION
    accounting_policy_version: str = DEFAULT_POLICY_VERSION
    epoch_id: str = DEFAULT_EPOCH
    source_id: str = DEFAULT_SOURCE
    tenant_id: str = DEFAULT_TENANT

    def payload(self) -> dict[str, Any]:
        """The P07 payload: every amount a decimal string."""
        return {
            "deal_id": self.deal_id,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "side": self.side,
            "entry_kind": self.entry_kind,
            "executed_at": format_timestamp(self.executed_at),
            "volume_lots": self.volume_lots,
            "realized_pnl_gross": self.realized_pnl_gross,
            "commission": self.commission,
            "swap": self.swap,
            "money_currency": self.money_currency,
            "fee_coverage": self.fee_coverage,
            "symbol_catalog_version": self.symbol_catalog_version,
            "accounting_policy_version": self.accounting_policy_version,
        }

    def canonical_deal_fields(self) -> dict[str, Any]:
        """The subset covered by ``deal_payload_hash``.

        Mirrors ``CANONICAL_DEAL_FIELDS`` in the contract. Built independently
        here so a change on one side shows up as a hash mismatch in the tests
        rather than passing silently on both.
        """
        return {
            "account_id": self.account_id,
            "commission": Decimal(self.commission),
            "deal_id": self.deal_id,
            "entry_kind": self.entry_kind,
            "executed_at": self.executed_at,
            "money_currency": self.money_currency,
            "realized_pnl_gross": Decimal(self.realized_pnl_gross),
            "side": self.side,
            "swap": Decimal(self.swap),
            "symbol": self.symbol,
            "volume_lots": Decimal(self.volume_lots),
        }

    def deal_payload_hash(self) -> str:
        return digest_of(self.canonical_deal_fields())

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (self.tenant_id, self.source_id, self.epoch_id, self.deal_id)


def _payload_hash(payload: Mapping[str, Any]) -> str:
    """Envelope ``payload_sha256`` over the decimalised payload.

    Must match ``_decimalize_money_fields`` in the contract: money strings and
    the timestamp are normalised before hashing, so ``"12.30"`` and
    ``"12.300"`` agree.
    """
    normalized = dict(payload)
    for key in ("realized_pnl_gross", "commission", "swap", "volume_lots"):
        if key in normalized:
            normalized[key] = Decimal(str(normalized[key]))
    if isinstance(normalized.get("executed_at"), str):
        from ..canonical import parse_timestamp

        normalized["executed_at"] = parse_timestamp(normalized["executed_at"], "executed_at")
    return digest_of(normalized)


def _event_uuid(seed: str) -> str:
    """Deterministic UUIDv4-shaped id from a seed string."""
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return (
        f"{digest[0:8]}-{digest[8:12]}-4{digest[13:16]}-"
        f"8{digest[17:20]}-{digest[20:32]}"
    )


def deal_envelope(
    spec: DealSpec,
    *,
    event_id: str | None = None,
    aggregate_version: int | None = None,
    created_at: datetime | None = None,
    payload_overrides: Mapping[str, Any] | None = None,
    corrupt_payload_hash: bool = False,
    corrupt_deal_hash: bool = False,
    schema_version: int = 1,
    event_type: str = "DealRecorded",
) -> dict[str, Any]:
    """Build a P07 ``DealRecorded.v1`` envelope for ``spec``.

    The ``corrupt_*`` switches exist for the conflict fixtures, which need an
    envelope whose declared hash disagrees with its content.
    """
    payload = spec.payload()
    if payload_overrides:
        payload.update(payload_overrides)
    declared_payload_hash = _payload_hash(payload)
    if corrupt_payload_hash:
        declared_payload_hash = "0" * 64
    declared_deal_hash = spec.deal_payload_hash()
    if corrupt_deal_hash:
        declared_deal_hash = "1" * 64
    elif payload_overrides:
        # Recompute from the overridden payload so a deliberately different
        # money value produces a genuinely different fingerprint.
        declared_deal_hash = digest_of(
            {
                "account_id": payload["account_id"],
                "commission": Decimal(str(payload["commission"])),
                "deal_id": payload["deal_id"],
                "entry_kind": payload["entry_kind"],
                "executed_at": _as_datetime(payload["executed_at"]),
                "money_currency": payload["money_currency"],
                "realized_pnl_gross": Decimal(str(payload["realized_pnl_gross"])),
                "side": payload["side"],
                "swap": Decimal(str(payload["swap"])),
                "symbol": payload["symbol"],
                "volume_lots": Decimal(str(payload["volume_lots"])),
            }
        )
    return {
        "event_id": event_id or _event_uuid(f"{spec.identity}/{spec.source_seq}"),
        "dedupe_key": f"deal:{spec.deal_id}:{spec.epoch_id}",
        "event_type": event_type,
        "schema_version": schema_version,
        "source_id": spec.source_id,
        "epoch_id": spec.epoch_id,
        "aggregate_type": "Account",
        "aggregate_id": spec.account_id,
        "aggregate_version": aggregate_version if aggregate_version is not None else spec.source_seq,
        "created_at": format_timestamp(created_at or (spec.executed_at + timedelta(seconds=1))),
        "payload": payload,
        "payload_sha256": declared_payload_hash,
        "provenance": {
            "source_seq": spec.source_seq,
            "deal_id": payload["deal_id"],
            "deal_payload_hash": declared_deal_hash,
        },
    }


def snapshot_marker_envelope(
    *,
    source_id: str = DEFAULT_SOURCE,
    epoch_id: str = DEFAULT_EPOCH,
    account_id: str = "A-1",
    source_seq: int = 1,
    snapshot_seq: int = 7,
) -> dict[str, Any]:
    """An ``AccountSnapshotPublished`` marker. G08-08.

    It carries no holdings, which is the point: the marker proves P03
    published a snapshot and nothing else (INV08-16).
    """
    payload = {"account_id": account_id, "snapshot_seq": snapshot_seq, "published": True}
    return {
        "event_id": _event_uuid(f"snapshot/{source_id}/{epoch_id}/{snapshot_seq}"),
        "dedupe_key": f"snapshot:{account_id}:{snapshot_seq}",
        "event_type": "AccountSnapshotPublished",
        "schema_version": 1,
        "source_id": source_id,
        "epoch_id": epoch_id,
        "aggregate_type": "Account",
        "aggregate_id": account_id,
        "aggregate_version": snapshot_seq,
        "created_at": format_timestamp(BASE_TIME),
        "payload": payload,
        "payload_sha256": digest_of(payload),
        "provenance": {"source_seq": source_seq, "snapshot_seq": snapshot_seq},
    }


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    from ..canonical import parse_timestamp

    return parse_timestamp(str(value), "executed_at")


# --------------------------------------------------------------------------
# Deterministic bulk generation
# --------------------------------------------------------------------------

_SYMBOLS = ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD")
_SIDES = ("BUY", "SELL")


class _Lcg:
    """Small linear congruential generator.

    A fixed, self-contained PRNG rather than :mod:`random`, so a fixture's
    content cannot change because something else in the process reseeded the
    global generator, and so the same seed gives the same corpus on any
    Python version (P01 makes the same choice for the same reason).
    """

    __slots__ = ("_state",)

    def __init__(self, seed: int) -> None:
        self._state = (seed * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)

    def next_int(self, bound: int) -> int:
        self._state = (self._state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        return (self._state >> 17) % bound


def generate_deals(
    count: int,
    *,
    seed: int = 20261030,
    accounts: int = 25,
    days: int = 7,
    currencies: Sequence[str] = ("USD",),
    start: datetime = BASE_TIME,
    tenant_id: str = DEFAULT_TENANT,
    source_id: str = DEFAULT_SOURCE,
    epoch_id: str = DEFAULT_EPOCH,
    account_skew: float = 0.0,
    first_source_seq: int = 1,
) -> list[DealSpec]:
    """Generate ``count`` unique, deterministic Deals.

    ``account_skew`` concentrates that fraction of records on a single
    account, which is what B08-07 needs to show a hot key.
    """
    if count < 0:
        raise ValueError("count must not be negative")
    rng = _Lcg(seed)
    specs: list[DealSpec] = []
    skew_cutoff = int(count * account_skew)
    for index in range(count):
        if index < skew_cutoff:
            account = "A-HOT"
        else:
            account = f"A-{rng.next_int(accounts) + 1}"
        symbol = _SYMBOLS[rng.next_int(len(_SYMBOLS))]
        side = _SIDES[rng.next_int(len(_SIDES))]
        currency = currencies[rng.next_int(len(currencies))]
        day_offset = rng.next_int(days)
        second_offset = rng.next_int(86_400)
        gross_cents = rng.next_int(400_001) - 200_000
        commission_cents = -(rng.next_int(1_500) + 1)
        swap_cents = rng.next_int(301) - 150
        specs.append(
            DealSpec(
                deal_id=f"D-{index + 1:09d}",
                account_id=account,
                symbol=symbol,
                side=side,
                entry_kind="OUT",
                executed_at=start + timedelta(days=day_offset, seconds=second_offset),
                volume_lots=_decimal_str(rng.next_int(500) + 1, 2),
                realized_pnl_gross=_decimal_str(gross_cents, 2),
                commission=_decimal_str(commission_cents, 2),
                swap=_decimal_str(swap_cents, 2),
                money_currency=currency,
                source_seq=first_source_seq + index,
                tenant_id=tenant_id,
                source_id=source_id,
                epoch_id=epoch_id,
            )
        )
    return specs


def _decimal_str(units: int, scale: int) -> str:
    """Render an integer number of minor units as a decimal string."""
    sign = "-" if units < 0 else ""
    digits = str(abs(units)).rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}" if scale else f"{sign}{digits}"


# --------------------------------------------------------------------------
# The 20 named fixtures
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GoldenFixture:
    """A named scenario: wire records, the source truth, and what must happen."""

    fixture_id: str
    description: str
    #: Envelopes in delivery order. May contain duplicates by design.
    envelopes: tuple[Mapping[str, Any], ...]
    #: Deals the *source* says exist. The oracle's input, not P08's output.
    source_deals: tuple[DealSpec, ...]
    #: What a build over this fixture must conclude.
    expected_outcome: str
    expected_reason: str | None = None
    #: Deals listed by the source but intentionally never emitted (G08-09).
    withheld_deals: tuple[DealSpec, ...] = ()
    notes: str = ""
    scope: CutScope | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def record_values(self) -> list[bytes]:
        import json

        return [json.dumps(env, sort_keys=True).encode("utf-8") for env in self.envelopes]


def _scope(cut_id: str = "cut-1", epoch_id: str = DEFAULT_EPOCH) -> CutScope:
    return CutScope(
        tenant_id=DEFAULT_TENANT,
        source_id=DEFAULT_SOURCE,
        epoch_id=epoch_id,
        report_family=DEFAULT_REPORT_FAMILY,
        cut_id=cut_id,
        policy_version=DEFAULT_POLICY_VERSION,
    )


def _three_usd_deals() -> list[DealSpec]:
    """G08-01: signed fees, two accounts, one day. Hand-checkable totals."""
    return [
        DealSpec(
            deal_id="D-1",
            account_id="A-1",
            symbol="EURUSD",
            side="BUY",
            entry_kind="OUT",
            executed_at=BASE_TIME,
            volume_lots="1.00",
            realized_pnl_gross="12.30",
            commission="-0.50",
            swap="-0.10",
            money_currency="USD",
            source_seq=1,
        ),
        DealSpec(
            deal_id="D-2",
            account_id="A-1",
            symbol="EURUSD",
            side="SELL",
            entry_kind="OUT",
            executed_at=BASE_TIME + timedelta(hours=1),
            volume_lots="2.00",
            realized_pnl_gross="-4.75",
            commission="-1.00",
            swap="0.25",
            money_currency="USD",
            source_seq=2,
        ),
        DealSpec(
            deal_id="D-3",
            account_id="A-2",
            symbol="GBPUSD",
            side="BUY",
            entry_kind="OUT",
            executed_at=BASE_TIME + timedelta(hours=2),
            volume_lots="0.50",
            realized_pnl_gross="100.00",
            commission="-2.00",
            swap="0.00",
            money_currency="USD",
            source_seq=3,
        ),
    ]


def _build_fixtures() -> dict[str, GoldenFixture]:
    out: dict[str, GoldenFixture] = {}
    base = _three_usd_deals()

    out["G08-01"] = GoldenFixture(
        fixture_id="G08-01",
        description="Three USD Deals with signed commission and swap",
        envelopes=tuple(deal_envelope(spec) for spec in base),
        source_deals=tuple(base),
        expected_outcome="READY",
        scope=_scope(),
        notes="Day totals: A-1 gross 7.55, A-2 gross 100.00",
    )

    # G08-02: byte-identical redelivery at a second offset.
    dup = deal_envelope(base[0])
    out["G08-02"] = GoldenFixture(
        fixture_id="G08-02",
        description="Same Kafka event delivered twice at different offsets",
        envelopes=(dup, *(deal_envelope(s) for s in base[1:]), dup),
        source_deals=tuple(base),
        expected_outcome="READY",
        scope=_scope(),
        notes="Raw observations >= 4; canonical Deals exactly 3",
    )

    # G08-03: one event_id, two different payload hashes.
    conflict_a = deal_envelope(base[0], event_id=_event_uuid("conflict-03"))
    conflict_b = deal_envelope(
        base[0],
        event_id=_event_uuid("conflict-03"),
        payload_overrides={"realized_pnl_gross": "999.99"},
    )
    out["G08-03"] = GoldenFixture(
        fixture_id="G08-03",
        description="One event_id observed with two different payload hashes",
        envelopes=(conflict_a, conflict_b, *(deal_envelope(s) for s in base[1:])),
        source_deals=tuple(base),
        expected_outcome="BLOCKED",
        expected_reason="EVENT_ID_HASH_CONFLICT",
        scope=_scope(),
    )

    # G08-04: same deal_id under two event_ids with different money.
    semantic_a = deal_envelope(base[0], event_id=_event_uuid("semantic-04-a"))
    semantic_b = deal_envelope(
        base[0],
        event_id=_event_uuid("semantic-04-b"),
        payload_overrides={"realized_pnl_gross": "77.77"},
    )
    out["G08-04"] = GoldenFixture(
        fixture_id="G08-04",
        description="Same Deal under two event_ids with conflicting money",
        envelopes=(semantic_a, semantic_b, *(deal_envelope(s) for s in base[1:])),
        source_deals=tuple(base),
        expected_outcome="BLOCKED",
        expected_reason="DEAL_SEMANTIC_CONFLICT",
        scope=_scope(),
    )

    # G08-05: an epoch reset reuses Deal ids legitimately.
    epoch2 = [replace(spec, epoch_id=ALT_EPOCH, source_seq=spec.source_seq) for spec in base]
    out["G08-05"] = GoldenFixture(
        fixture_id="G08-05",
        description="Epoch reset reuses the same Deal ids",
        envelopes=tuple(
            deal_envelope(spec) for spec in (*base, *epoch2)
        ),
        source_deals=tuple(base),
        expected_outcome="READY",
        scope=_scope(),
        extra={"second_epoch_deals": tuple(epoch2), "second_epoch_scope": _scope(epoch_id=ALT_EPOCH)},
        notes="Each epoch builds separately; no cross-epoch merge",
    )

    # G08-06: USD and EUR in one day.
    eur = replace(
        base[0], deal_id="D-EUR-1", money_currency="EUR", source_seq=10, realized_pnl_gross="5.00"
    )
    multi = [*base, eur]
    out["G08-06"] = GoldenFixture(
        fixture_id="G08-06",
        description="USD and EUR Deals on the same day",
        envelopes=tuple(deal_envelope(spec) for spec in multi),
        source_deals=tuple(multi),
        expected_outcome="READY",
        scope=_scope(),
        notes="Separate currency rows; no combined total",
    )

    # G08-07: executed months before ingestion.
    late = replace(
        base[0],
        deal_id="D-LATE-1",
        executed_at=BASE_TIME - timedelta(days=120),
        source_seq=11,
    )
    late_set = [*base, late]
    out["G08-07"] = GoldenFixture(
        fixture_id="G08-07",
        description="Valid Deal executed long before it was ingested",
        envelopes=tuple(deal_envelope(spec) for spec in late_set),
        source_deals=tuple(late_set),
        expected_outcome="READY",
        scope=_scope(),
        notes="trade_day follows executed_at, not ingestion time",
    )

    out["G08-08"] = GoldenFixture(
        fixture_id="G08-08",
        description="Account snapshot marker with no holdings payload",
        envelopes=(snapshot_marker_envelope(), *(deal_envelope(s) for s in base)),
        source_deals=tuple(base),
        expected_outcome="READY",
        scope=_scope(),
        notes="Account/position metrics stay UNSUPPORTED; Deal report unaffected",
    )

    # G08-09: source lists 100, only 99 emitted.
    hundred = generate_deals(100, seed=9009)
    out["G08-09"] = GoldenFixture(
        fixture_id="G08-09",
        description="Source manifest lists 100 Deals, only 99 reach the sink",
        envelopes=tuple(deal_envelope(spec) for spec in hundred[:-1]),
        source_deals=tuple(hundred),
        withheld_deals=(hundred[-1],),
        expected_outcome="INCOMPLETE",
        expected_reason="SOURCE_DEALS_MISSING",
        scope=_scope(),
        notes="Kafka lag of zero does not prove completeness",
    )

    out["G08-10"] = GoldenFixture(
        fixture_id="G08-10",
        description="Build killed after 98% of facts are written",
        envelopes=tuple(deal_envelope(spec) for spec in hundred),
        source_deals=tuple(hundred),
        expected_outcome="FAILED",
        scope=_scope(),
        notes="Old publication must remain current and unchanged",
    )

    out["G08-11"] = GoldenFixture(
        fixture_id="G08-11",
        description="Identical pinned cut built twice",
        envelopes=tuple(deal_envelope(spec) for spec in base),
        source_deals=tuple(base),
        expected_outcome="READY",
        scope=_scope(),
        notes="Both builds must produce the same canonical digest",
    )

    out["G08-12"] = GoldenFixture(
        fixture_id="G08-12",
        description="Reconciliation revision changes mid-build",
        envelopes=tuple(deal_envelope(spec) for spec in base),
        source_deals=tuple(base),
        expected_outcome="BLOCKED",
        expected_reason="RECONCILIATION_REVOKED",
        scope=_scope(),
    )

    hundred_k = 100_000
    out["G08-13"] = GoldenFixture(
        fixture_id="G08-13",
        description="100k synthetic Deals, exact set and sums",
        envelopes=(),
        source_deals=(),
        expected_outcome="READY",
        scope=_scope(),
        notes="Generated on demand; see extra['generator']",
        extra={"generator": {"count": hundred_k, "seed": 13013}},
    )

    out["G08-14"] = GoldenFixture(
        fixture_id="G08-14",
        description="1M Deals with account skew",
        envelopes=(),
        source_deals=(),
        expected_outcome="READY",
        scope=_scope(),
        notes="Resource fairness profile; generated on demand",
        extra={"generator": {"count": 1_000_000, "seed": 14014, "account_skew": 0.9}},
    )

    # G08-15: DST and midnight boundaries, all expressed in UTC.
    boundary = [
        replace(
            base[0],
            deal_id="D-TZ-1",
            executed_at=datetime(2026, 3, 29, 0, 59, 59, tzinfo=UTC),
            source_seq=21,
        ),
        replace(
            base[0],
            deal_id="D-TZ-2",
            executed_at=datetime(2026, 3, 29, 1, 0, 0, tzinfo=UTC),
            source_seq=22,
        ),
        replace(
            base[0],
            deal_id="D-TZ-3",
            executed_at=datetime(2026, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            source_seq=23,
        ),
        replace(
            base[0],
            deal_id="D-TZ-4",
            executed_at=datetime(2027, 1, 1, 0, 0, 0, tzinfo=UTC),
            source_seq=24,
        ),
    ]
    out["G08-15"] = GoldenFixture(
        fixture_id="G08-15",
        description="Midnight, DST and year-boundary executions",
        envelopes=tuple(deal_envelope(spec) for spec in boundary),
        source_deals=tuple(boundary),
        expected_outcome="READY",
        scope=_scope(),
        notes="UTC day assignment; four Deals across four distinct days",
    )

    out["G08-16"] = GoldenFixture(
        fixture_id="G08-16",
        description="Kafka retention expired below the committed offset",
        envelopes=tuple(deal_envelope(spec) for spec in base),
        source_deals=tuple(base),
        expected_outcome="INCOMPLETE",
        expected_reason="RETENTION_GAP",
        scope=_scope(),
    )

    # G08-17: poison at offset 1, valid record directly after it.
    poison = {"event_type": "DealRecorded", "schema_version": 1, "not": "an envelope"}
    out["G08-17"] = GoldenFixture(
        fixture_id="G08-17",
        description="Poison record followed by a valid one",
        envelopes=(deal_envelope(base[0]), poison, deal_envelope(base[1])),
        source_deals=(base[0], base[1]),
        expected_outcome="QUARANTINED",
        expected_reason="FIELD_MISSING",
        scope=_scope(),
        notes="No commit may pass the unresolved offset until its receipt is durable",
    )

    out["G08-18"] = GoldenFixture(
        fixture_id="G08-18",
        description="Overlapping old and new consumer assignments",
        envelopes=tuple(deal_envelope(spec) for spec in base),
        source_deals=tuple(base),
        expected_outcome="READY",
        scope=_scope(),
        notes="Stale generation may not commit; no offset skipped",
    )

    out["G08-19"] = GoldenFixture(
        fixture_id="G08-19",
        description="GC attempts to delete the currently published build",
        envelopes=tuple(deal_envelope(spec) for spec in base),
        source_deals=tuple(base),
        expected_outcome="READY",
        expected_reason="GC_PROTECTED",
        scope=_scope(),
    )

    # G08-20: fee coverage unknown, so no net may be published.
    unknown_fee = [replace(spec, fee_coverage=str(FeeCoverage.UNKNOWN)) for spec in base]
    out["G08-20"] = GoldenFixture(
        fixture_id="G08-20",
        description="Fee coverage unknown while a net figure is requested",
        envelopes=tuple(deal_envelope(spec) for spec in unknown_fee),
        source_deals=tuple(unknown_fee),
        expected_outcome="BLOCKED",
        expected_reason="FEE_COVERAGE_UNKNOWN",
        scope=_scope(),
        notes="Components may still be reported; net must be withheld",
    )

    return out


GOLDEN_FIXTURES: Mapping[str, GoldenFixture] = _build_fixtures()


def fixture(fixture_id: str) -> GoldenFixture:
    try:
        return GOLDEN_FIXTURES[fixture_id]
    except KeyError as exc:
        raise KeyError(
            f"unknown golden fixture {fixture_id!r}; known: {sorted(GOLDEN_FIXTURES)}"
        ) from exc


def fixture_ids() -> list[str]:
    return sorted(GOLDEN_FIXTURES)


def specs_for(fixture_id: str) -> tuple[DealSpec, ...]:
    """Source Deals of a fixture, generating the large ones on demand."""
    item = fixture(fixture_id)
    generator = item.extra.get("generator")
    if generator:
        return tuple(generate_deals(**generator))
    return item.source_deals


def envelopes_for(fixture_id: str) -> list[Mapping[str, Any]]:
    item = fixture(fixture_id)
    generator = item.extra.get("generator")
    if generator:
        return [deal_envelope(spec) for spec in generate_deals(**generator)]
    return list(item.envelopes)
