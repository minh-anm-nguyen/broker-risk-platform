"""Independent oracles O1-O8.

Design section 14.2 states the rule these exist to obey: never use the system
under test as its own expected result. So nothing here calls the writer, the
relay or the consumer. Each oracle reads raw rows and raw broker records and
recomputes the expected relationship from a different direction -- usually in
SQL, and where not, by re-deriving an identity from the event's own content
rather than from the index that was built for it.

The two that are easiest to write wrongly, and are therefore written carefully:

**O1 is scoped to a test epoch.** A left join of every Deal against every outbox
row would fail on any database that contains Deals from before the outbox
integration was enabled, and would then be "fixed" by loosening it until it
proved nothing. The scope is a parameter, and the design calls for an
``integration_enabled_at`` manifest for a real backfill.

**O4 compares distinct event ids, not record counts.** Kafka may legally hold
the same event more than once, so a count comparison would report a failure for
correct behaviour. What must match is the *set* of identities -- and separately,
what the consumer applied must not be missing any of them, "even if Kafka lag
says 0".
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .broker.embedded import EmbeddedKafka
from .canonical import payload_hash
from .events.keys import EventType, build_dedupe_key, parse_dedupe_key
from .faults.hooks import HookRegistry


@dataclass(slots=True)
class OracleResult:
    """One oracle's verdict, with enough detail to act on a failure."""

    name: str
    ok: bool
    summary: str
    details: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.ok

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "summary": self.summary, "details": self.details[:50]}


def _ok(name: str, summary: str) -> OracleResult:
    return OracleResult(name=name, ok=True, summary=summary)


def _fail(name: str, summary: str, details: list[str]) -> OracleResult:
    return OracleResult(name=name, ok=False, summary=summary, details=details)


# --------------------------------------------------------------------------- #
# O1 - every committed Deal has exactly one logical DealRecorded
# --------------------------------------------------------------------------- #

def o1_deal_to_outbox_set_equality(store: Any, *, epoch_id: str) -> OracleResult:
    """Deals in ``epoch_id`` with no matching ``DealRecorded`` outbox row.

    The join reconstructs the dedupe key in SQL from the Deal's own columns,
    which is the same thing design section 14.2 does with
    ``encode(convert_to(deal_id,'UTF8'),'hex')``. Rebuilding it independently is
    the point: joining on a stored foreign key would only prove the writer is
    self-consistent.
    """
    missing: list[str] = []
    for row in store.query(
        "SELECT deal_id FROM deals WHERE epoch_id = ? ORDER BY source_seq, deal_id", (epoch_id,)
    ):
        deal_id = row["deal_id"]
        expected_key = build_dedupe_key(EventType.DEAL_RECORDED, epoch_id, deal_id)
        found = store.query(
            "SELECT event_id FROM event_outbox WHERE epoch_id = ? AND event_type = 'DealRecorded' "
            "AND dedupe_key = ?",
            (epoch_id, expected_key),
        )
        if not found:
            missing.append(deal_id)
        elif len(found) > 1:
            missing.append(f"{deal_id} (has {len(found)} outbox rows)")
    if missing:
        return _fail(
            "O1",
            f"{len(missing)} committed Deal(s) in {epoch_id} lack exactly one DealRecorded row",
            missing,
        )
    return _ok("O1", f"every Deal in {epoch_id} has exactly one logical DealRecorded row")


# --------------------------------------------------------------------------- #
# O2 - no orphan outbox Deal event
# --------------------------------------------------------------------------- #

def o2_no_orphan_outbox_deal(store: Any, *, epoch_id: str) -> OracleResult:
    """Every ``DealRecorded`` row maps back to a Deal with matching content.

    The Deal identity is recovered by *parsing* the dedupe key through the typed
    mapping, not by splitting the string: design section 14.2 warns that a naive
    split breaks when an id contains the separator, and the hex encoding is what
    makes the parse total.
    """
    problems: list[str] = []
    for row in store.query(
        "SELECT dedupe_key, payload, payload_sha256, source_id FROM event_outbox "
        "WHERE epoch_id = ? AND event_type = 'DealRecorded'",
        (epoch_id,),
    ):
        key = parse_dedupe_key(row["dedupe_key"])
        deal = store.query(
            "SELECT realized_pnl_gross, commission, swap, source_seq, payload_hash, account_id "
            "FROM deals WHERE epoch_id = ? AND deal_id = ?",
            (epoch_id, key.identity),
        )
        if not deal:
            problems.append(f"outbox event for deal {key.identity!r} has no Deal row")
            continue
        payload = json.loads(row["payload"])
        stored = deal[0]
        if payload["account_id"] != stored["account_id"]:
            problems.append(
                f"deal {key.identity!r}: event says account {payload['account_id']!r}, "
                f"Deal row says {stored['account_id']!r}"
            )
        for field_name in ("realized_pnl_gross", "commission", "swap"):
            if Decimal(payload[field_name]) != Decimal(str(stored[field_name])):
                problems.append(
                    f"deal {key.identity!r}: {field_name} event={payload[field_name]} "
                    f"db={stored[field_name]}"
                )
        if payload_hash(payload) != row["payload_sha256"]:
            problems.append(f"deal {key.identity!r}: stored payload does not match stored hash")
    if problems:
        return _fail("O2", f"{len(problems)} orphan or mismatched DealRecorded row(s)", problems)
    return _ok("O2", f"every DealRecorded row in {epoch_id} maps to a matching Deal")


# --------------------------------------------------------------------------- #
# O3 - published scans and markers correspond exactly
# --------------------------------------------------------------------------- #

def o3_scan_marker_equivalence(store: Any, *, epoch_id: str) -> OracleResult:
    """``sync_runs.status='PUBLISHED'`` matches the set of marker events.

    Checked in both directions, because the two failures are different bugs: a
    published scan with no marker loses a notification, and a marker for a
    ``STAGED`` or ``ABORTED`` scan announces a snapshot that was never made
    current.
    """
    problems: list[str] = []
    for row in store.query(
        "SELECT scan_id, entity, status FROM sync_runs WHERE epoch_id = ?", (epoch_id,)
    ):
        event_type = (
            EventType.ACCOUNT_SNAPSHOT_PUBLISHED
            if row["entity"] == "accounts"
            else EventType.POSITION_SNAPSHOT_PUBLISHED
        )
        key = build_dedupe_key(event_type, epoch_id, row["scan_id"])
        markers = store.query(
            "SELECT event_id FROM event_outbox WHERE epoch_id = ? AND dedupe_key = ?",
            (epoch_id, key),
        )
        if row["status"] == "PUBLISHED" and len(markers) != 1:
            problems.append(
                f"scan {row['scan_id']!r} is PUBLISHED but has {len(markers)} marker(s)"
            )
        if row["status"] != "PUBLISHED" and markers:
            problems.append(
                f"scan {row['scan_id']!r} is {row['status']} yet a marker exists"
            )
    if problems:
        return _fail("O3", f"{len(problems)} scan/marker mismatch(es)", problems)
    return _ok("O3", f"scan statuses and snapshot markers correspond in {epoch_id}")


# --------------------------------------------------------------------------- #
# O4 - broker contents and consumer effects
# --------------------------------------------------------------------------- #

def o4_broker_and_consumer_sets(
    store: Any,
    broker: EmbeddedKafka,
    consumer_store: Any,
    *,
    consumer_name: str,
    require_consumed: bool = True,
) -> OracleResult:
    """Compare identity *sets* across outbox, broker log and consumer inbox.

    Three relationships, each a different failure:

    * a broker record whose event id is not in the outbox would be a fabricated
      event,
    * a ``PUBLISHED`` row with no broker record is a false ACK, and
    * an id in the broker that the consumer never processed is a missing
      business effect -- the one the design insists is critical "even if Kafka
      lag says 0".
    """
    published = {
        str(row["event_id"])
        for row in store.query("SELECT event_id FROM event_outbox WHERE state = 'PUBLISHED'")
    }
    all_rows = {
        str(row["event_id"]): row["state"]
        for row in store.query("SELECT event_id, state FROM event_outbox")
    }
    broker_ids = {
        json.loads(record.value.decode("utf-8"))["event_id"] for record in broker.records()
    }
    processed = consumer_store.processed_event_ids(consumer_name)

    problems: list[str] = []
    for event_id in sorted(broker_ids - set(all_rows)):
        problems.append(f"broker holds {event_id} which is in no outbox row")
    for event_id in sorted(published - broker_ids):
        problems.append(f"{event_id} is PUBLISHED but absent from the broker log")
    if require_consumed:
        for event_id in sorted(broker_ids - processed):
            problems.append(f"{event_id} is in the broker but was never processed by the consumer")

    if problems:
        return _fail(
            "O4",
            f"{len(problems)} set mismatch(es) across outbox/broker/consumer",
            problems,
        )
    return _ok(
        "O4",
        f"{len(broker_ids)} distinct event id(s) in the broker, all accounted for "
        f"({broker.record_count()} physical records, duplicates are legal)",
    )


# --------------------------------------------------------------------------- #
# O5 - order
# --------------------------------------------------------------------------- #

def o5_order(consumer_store: Any, *, consumer_name: str) -> OracleResult:
    """Applied versions are contiguous from 1, or the aggregate is blocked.

    A gap is only acceptable when it is *declared*: the aggregate carries a
    ``blocked_reason``. An aggregate that has applied 1, 2 and 4 while claiming
    to be healthy has lost version 3's effect silently, which is the failure
    this oracle exists to find.
    """
    problems: list[str] = []
    for row in consumer_store.version_ledger(consumer_name):
        applied = [
            int(record["aggregate_version"])
            for record in consumer_store.query(
                "SELECT aggregate_version FROM consumer_processed_events "
                "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND aggregate_key = ? "
                "ORDER BY aggregate_version",
                (consumer_name, row["source_id"], row["epoch_id"], row["aggregate_key"]),
            )
        ]
        last = int(row["last_applied_version"])
        contiguous = list(range(1, last + 1))
        seen_up_to_last = sorted({version for version in applied if version <= last})
        if seen_up_to_last != contiguous:
            problems.append(
                f"{row['aggregate_key']!r}: last_applied={last} but processed versions "
                f"{seen_up_to_last} are not 1..{last}"
            )
        ahead = sorted({version for version in applied if version > last})
        if ahead and not row["blocked_reason"]:
            problems.append(
                f"{row['aggregate_key']!r}: processed versions {ahead} are ahead of "
                f"last_applied={last} with no blocked_reason recorded"
            )
    if problems:
        return _fail("O5", f"{len(problems)} ordering problem(s)", problems)
    return _ok("O5", "applied versions are contiguous or explicitly blocked")


# --------------------------------------------------------------------------- #
# O6 - money identity
# --------------------------------------------------------------------------- #

def o6_money_identity(
    store: Any, consumer_store: Any, *, consumer_name: str, source_id: str, epoch_id: str
) -> OracleResult:
    """Sum the source Deals independently and compare with the projection.

    The expected totals are computed here from the ``deals`` table with
    ``Decimal``, not read from anything the consumer wrote. Deals whose currency
    was never proven are excluded from the currency totals and counted
    separately, matching the rule that an unproven amount must not be
    aggregated.
    """
    expected: dict[str, dict[str, Any]] = {}
    for row in store.query(
        "SELECT account_id, realized_pnl_gross, commission, swap, money_currency "
        "FROM deals WHERE epoch_id = ?",
        (epoch_id,),
    ):
        bucket = expected.setdefault(
            row["account_id"],
            {
                "realized_pnl_gross": Decimal("0"),
                "commission": Decimal("0"),
                "swap": Decimal("0"),
                "deal_count": 0,
                "unproven": 0,
                "currency": row["money_currency"],
            },
        )
        bucket["deal_count"] += 1
        if row["money_currency"] is None:
            bucket["unproven"] += 1
            continue
        for field_name in ("realized_pnl_gross", "commission", "swap"):
            bucket[field_name] += Decimal(str(row[field_name]))

    problems: list[str] = []
    actual = {
        projection.account_id: projection
        for projection in consumer_store.projection(consumer_name, source_id, epoch_id)
    }
    for account_id, bucket in expected.items():
        projection = actual.get(account_id)
        if projection is None:
            problems.append(f"account {account_id!r} has Deals but no projection row")
            continue
        if projection.deal_count != bucket["deal_count"]:
            problems.append(
                f"account {account_id!r}: projection counted {projection.deal_count} Deals, "
                f"source has {bucket['deal_count']}"
            )
        if projection.unproven_currency_deal_count != bucket["unproven"]:
            problems.append(
                f"account {account_id!r}: projection counted {projection.unproven_currency_deal_count} "
                f"unproven-currency Deals, source has {bucket['unproven']}"
            )
        for field_name in ("realized_pnl_gross", "commission", "swap"):
            projected = Decimal(getattr(projection, field_name))
            if projected != bucket[field_name]:
                problems.append(
                    f"account {account_id!r}: {field_name} projection={projected} "
                    f"source={bucket[field_name]}"
                )
    for account_id in set(actual) - set(expected):
        problems.append(f"projection has account {account_id!r} with no Deals in the source")

    if problems:
        return _fail("O6", f"{len(problems)} money discrepancy/ies", problems)
    return _ok("O6", f"projection totals match the source Deals for {len(expected)} account(s)")


# --------------------------------------------------------------------------- #
# O7 - boundedness
# --------------------------------------------------------------------------- #

@dataclass(frozen=True, slots=True)
class ResourceBudget:
    max_rss_bytes: int | None = None
    max_inflight: int | None = None
    max_buffer_bytes: int | None = None
    max_claimed_rows: int | None = None
    max_storage_bytes: int | None = None


def o7_boundedness(
    store: Any,
    *,
    budget: ResourceBudget,
    inflight: int | None = None,
    buffer_bytes: int | None = None,
    now: Any = None,
) -> OracleResult:
    """Resource caps held. Correct money with an OOM is not a pass."""
    from datetime import UTC, datetime

    problems: list[str] = []
    stats = store.stats(now=now or datetime.now(UTC))
    if budget.max_claimed_rows is not None and stats.claimed_total > budget.max_claimed_rows:
        problems.append(f"{stats.claimed_total} rows claimed, cap is {budget.max_claimed_rows}")
    if budget.max_storage_bytes is not None and stats.storage_bytes > budget.max_storage_bytes:
        problems.append(f"store holds {stats.storage_bytes} bytes, cap is {budget.max_storage_bytes}")
    if budget.max_inflight is not None and inflight is not None and inflight > budget.max_inflight:
        problems.append(f"{inflight} records in flight, cap is {budget.max_inflight}")
    if (
        budget.max_buffer_bytes is not None
        and buffer_bytes is not None
        and buffer_bytes > budget.max_buffer_bytes
    ):
        problems.append(f"producer buffer holds {buffer_bytes} bytes, cap is {budget.max_buffer_bytes}")
    if budget.max_rss_bytes is not None:
        rss = _rss_bytes()
        if rss is None:
            problems.append("an RSS budget was set but psutil is unavailable to measure it")
        elif rss > budget.max_rss_bytes:
            problems.append(f"RSS is {rss} bytes, cap is {budget.max_rss_bytes}")
    if problems:
        return _fail("O7", f"{len(problems)} resource bound(s) exceeded", problems)
    return _ok("O7", "all declared resource bounds held")


def _rss_bytes() -> int | None:
    try:
        import psutil  # noqa: PLC0415 - optional, only needed when a budget is set
    except ModuleNotFoundError:  # pragma: no cover
        return None
    return int(psutil.Process().memory_info().rss)


# --------------------------------------------------------------------------- #
# O8 - evidence honesty
# --------------------------------------------------------------------------- #

def o8_evidence_honesty(
    hooks: HookRegistry, *, required_kill_points: tuple[str, ...]
) -> OracleResult:
    """Confirm each declared fault actually fired.

    Design section 14.2: a case is ``PASS`` only when the fault hook fired. A
    crash test whose kill point was never reached has proved that the happy path
    works, which is not what it claims to prove, so this oracle is what turns
    that into ``INCONCLUSIVE`` instead of a quiet pass.
    """
    never = [point for point in required_kill_points if hooks.fired(point) == 0]
    if never:
        return _fail(
            "O8",
            f"{len(never)} declared fault(s) never fired; the result is INCONCLUSIVE, not PASS",
            list(never),
        )
    fired = {point: hooks.fired(point) for point in required_kill_points}
    return _ok("O8", f"all declared faults fired: {fired}")


# --------------------------------------------------------------------------- #
# Suite
# --------------------------------------------------------------------------- #

def run_core_suite(
    store: Any,
    broker: EmbeddedKafka,
    consumer_store: Any,
    *,
    source_id: str,
    epoch_id: str,
    consumer_name: str,
    budget: ResourceBudget | None = None,
    require_consumed: bool = True,
) -> list[OracleResult]:
    """O1-O7 over one epoch. O8 needs the test's own kill-point list."""
    results = [
        o1_deal_to_outbox_set_equality(store, epoch_id=epoch_id),
        o2_no_orphan_outbox_deal(store, epoch_id=epoch_id),
        o3_scan_marker_equivalence(store, epoch_id=epoch_id),
        o4_broker_and_consumer_sets(
            store,
            broker,
            consumer_store,
            consumer_name=consumer_name,
            require_consumed=require_consumed,
        ),
        o5_order(consumer_store, consumer_name=consumer_name),
        o6_money_identity(
            store,
            consumer_store,
            consumer_name=consumer_name,
            source_id=source_id,
            epoch_id=epoch_id,
        ),
    ]
    if budget is not None:
        results.append(o7_boundedness(store, budget=budget))
    return results


def assert_all(results: list[OracleResult]) -> None:
    """Raise with every failure, not just the first.

    A failing campaign usually trips several oracles, and the combination is the
    diagnosis -- "O1 and O6 failed" means Deals are missing events, while "only
    O6 failed" means the events exist and the projection is wrong.
    """
    failed = [result for result in results if not result.ok]
    if failed:
        lines = []
        for result in failed:
            lines.append(f"{result.name}: {result.summary}")
            lines.extend(f"    - {detail}" for detail in result.details[:10])
        raise AssertionError("oracle failures:\n" + "\n".join(lines))
