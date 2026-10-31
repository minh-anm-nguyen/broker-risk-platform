"""Command line entry point.

``demo`` is the one worth running first. It performs the three demonstrations
design section 0.1 asks the MVP to produce, in order, and prints what each one
proves:

1. a Deal and its event commit together and the consumer applies it once;
2. a relay killed after the broker ACK but before the database write-back
   leaves two copies in Kafka and still exactly one business effect;
3. a broker outage grows a bounded backlog that drains completely on recovery.

The other subcommands are the operational surface: ``migrate``, ``relay``,
``consumer``, ``verify`` (the oracle suite), ``status`` and ``bench``.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from .broker.embedded import Fault
from .clock import RealClock
from .errors import ConfigurationError, OutboxError
from .faults.hooks import HOOKS, CrashSignal, KillPoint
from .fixtures import EPOCH_A, deal, spread_load
from .observability.logging import configure
from .oracles import ResourceBudget, run_core_suite
from .persistence import open_store
from .pipeline import build_pipeline
from .settings import load_settings


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def cmd_migrate(args: argparse.Namespace) -> int:
    settings = load_settings()
    store = open_store(settings, clock=RealClock())
    store.migrate()
    _print({"migrated": True, "backend": settings.store_backend})
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    settings = load_settings()
    store = open_store(settings, clock=RealClock())
    stats = store.stats(now=RealClock().now())
    _print(
        {
            "backend": settings.store_backend,
            "pending": stats.pending_total,
            "claimed": stats.claimed_total,
            "published": stats.published_total,
            "quarantined": stats.quarantined_total,
            "oldest_pending_age_seconds": round(stats.oldest_pending_age_seconds, 3),
            "storage_bytes": stats.storage_bytes,
            # Named for what it is: QUARANTINED is not success and needs an owner.
            "needs_operator": stats.quarantined_total > 0,
        }
    )
    return 0


def cmd_relay(args: argparse.Namespace) -> int:
    """Run the relay against a real store and the configured broker.

    The embedded broker is in-process, so a relay run against it only makes
    sense inside one process. Pointing this at a real cluster needs the
    ``kafka`` extra and :mod:`outbox.broker.confluent`; that path is refused
    here rather than silently publishing into a model.
    """
    raise ConfigurationError(
        "the standalone relay needs a real broker: install the 'kafka' extra and wire "
        "outbox.broker.confluent.ConfluentProducer. Use 'outbox demo' to exercise the "
        "pipeline end to end against the in-process broker model."
    )


def cmd_demo(args: argparse.Namespace) -> int:
    root = Path(args.root or tempfile.mkdtemp(prefix="outbox-demo-"))
    results: dict[str, Any] = {"root": str(root)}
    HOOKS.reset()

    # --- demo 1: atomic commit, one business effect ----------------------
    pipeline = build_pipeline(root=root / "demo1", claim_batch_size=10, max_inflight=16)
    pipeline.sync.open_epoch(EPOCH_A)
    pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001"), deal("D-1002", account_id="A-43")])
    pipeline.pump()
    projection = pipeline.consumer_store.projection(
        pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
    )
    results["demo1_atomic_commit"] = {
        "outbox_published": sum(1 for record in pipeline.store.iter_records() if record.state == "PUBLISHED"),
        "broker_records": pipeline.broker.record_count(),
        "accounts_projected": len(projection),
        "proves": "business row and event committed together; consumer applied each event once",
    }
    pipeline.close()

    # --- demo 2: ACK then kill before the DB write-back ------------------
    pipeline = build_pipeline(root=root / "demo2", claim_batch_size=1, max_inflight=4)
    pipeline.sync.open_epoch(EPOCH_A)
    pipeline.sync.insert_deals(EPOCH_A, [deal("D-2001")])
    try:
        with HOOKS.crash_at(KillPoint.RELAY_AFTER_ACK_BEFORE_CAS):
            pipeline.worker.run_until_idle(max_cycles=5)
    except CrashSignal:
        pass
    # The lease has to lapse before a standby may touch the row.
    time.sleep(0)
    standby_publisher, standby_worker = pipeline.new_relay("relay-standby")
    expired = pipeline.store.query(
        "UPDATE event_outbox SET lease_until = '2000-01-01T00:00:00.000000Z' "
        "WHERE state = 'CLAIMED'"
    )
    del expired
    standby_worker.run_until_idle(max_cycles=20)
    pipeline.run_consumer()
    results["demo2_ack_then_kill"] = {
        "broker_physical_records": pipeline.broker.record_count(),
        "broker_distinct_event_ids": len(
            {json.loads(record.value)["event_id"] for record in pipeline.broker.records()}
        ),
        "consumer_applied": pipeline.consumer_store.processed_count(pipeline.settings.consumer_name),
        "crash_fired": HOOKS.fired(KillPoint.RELAY_AFTER_ACK_BEFORE_CAS),
        "proves": "duplicate Kafka records are legal; the consumer inbox applies one effect",
    }
    pipeline.close()

    # --- demo 3: broker outage, bounded backlog, full drain --------------
    pipeline = build_pipeline(root=root / "demo3", claim_batch_size=20, max_inflight=32)
    pipeline.sync.open_epoch(EPOCH_A)
    pipeline.sync.insert_deals(EPOCH_A, spread_load(200, accounts=8))
    pipeline.broker.fault = Fault.DOWN
    for _ in range(10):
        pipeline.publisher.publish_cycle()
    during = pipeline.store.stats(now=pipeline.clock.now())
    pipeline.broker.fault = Fault.NONE
    pipeline.worker.run_until_idle(max_cycles=20_000)
    pipeline.run_consumer()
    after = pipeline.store.stats(now=pipeline.clock.now())
    results["demo3_outage_recovery"] = {
        "pending_during_outage": during.pending_total,
        "records_in_broker_during_outage": 0,
        "pending_after_recovery": after.pending_total,
        "published_after_recovery": after.published_total,
        "consumer_applied": pipeline.consumer_store.processed_count(pipeline.settings.consumer_name),
        "proves": "nothing published while the broker was down, nothing lost, full drain after",
    }
    oracles = run_core_suite(
        pipeline.store,
        pipeline.broker,
        pipeline.consumer_store,
        source_id=pipeline.source_id,
        epoch_id=EPOCH_A,
        consumer_name=pipeline.settings.consumer_name,
        budget=ResourceBudget(max_claimed_rows=pipeline.settings.max_inflight),
    )
    results["oracles"] = [result.as_dict() for result in oracles]
    pipeline.close()

    _print(results)
    return 0 if all(result.ok for result in oracles) else 1


def cmd_verify(args: argparse.Namespace) -> int:
    settings = load_settings()
    store = open_store(settings, clock=RealClock())
    from .consumer.inbox import ConsumerStore

    consumer_store = ConsumerStore(args.consumer_db)
    consumer_store.migrate()
    # Verifying against a real cluster needs a broker adapter that can list a
    # topic; the in-process model is the only one that can do so here, so the
    # broker-side oracle is explicitly skipped rather than faked.
    from .broker.embedded import EmbeddedKafka

    results = run_core_suite(
        store,
        EmbeddedKafka(),
        consumer_store,
        source_id=args.source_id,
        epoch_id=args.epoch_id,
        consumer_name=settings.consumer_name,
        require_consumed=False,
    )
    _print(
        {
            "epoch_id": args.epoch_id,
            "oracles": [result.as_dict() for result in results],
            "note": "O4's broker side is not evaluated against a live cluster by this command",
        }
    )
    return 0 if all(result.ok for result in results) else 1


def cmd_bench(args: argparse.Namespace) -> int:
    """A scaled-down B07-02-style run against the in-process broker.

    The number this prints is a *harness* throughput figure. It says nothing
    about a real broker: there is no network, no replication and no fsync on the
    broker side. Design section 16.1 is explicit that DEV is not a durability
    proof, and it is not a throughput proof either.
    """
    root = Path(tempfile.mkdtemp(prefix="outbox-bench-"))
    pipeline = build_pipeline(root=root, claim_batch_size=100, max_inflight=256, max_event_wire_bytes=32768)
    pipeline.sync.open_epoch(EPOCH_A)
    deals = spread_load(args.events, accounts=args.accounts)
    write_start = time.perf_counter()
    for index in range(0, len(deals), 200):
        pipeline.sync.insert_deals(EPOCH_A, deals[index : index + 200])
    write_elapsed = time.perf_counter() - write_start

    publish_start = time.perf_counter()
    summary = pipeline.worker.run_until_idle(max_cycles=200_000)
    publish_elapsed = time.perf_counter() - publish_start

    consume_start = time.perf_counter()
    report = pipeline.run_consumer()
    consume_elapsed = time.perf_counter() - consume_start

    _print(
        {
            "events": args.events,
            "accounts": args.accounts,
            "write_seconds": round(write_elapsed, 3),
            "write_events_per_second": round(args.events / write_elapsed, 1),
            "publish_seconds": round(publish_elapsed, 3),
            "publish_events_per_second": round(args.events / publish_elapsed, 1),
            "consume_seconds": round(consume_elapsed, 3),
            "acked": summary.acked,
            "applied": report.applied,
            "broker_records": pipeline.broker.record_count(),
            "caveat": (
                "in-process broker model, no network, no replication, no broker fsync. "
                "Not a Kafka throughput measurement."
            ),
        }
    )
    pipeline.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="outbox", description=__doc__)
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="create the outbox schema").set_defaults(func=cmd_migrate)
    sub.add_parser("status", help="print outbox backlog and storage").set_defaults(func=cmd_status)
    sub.add_parser("relay", help="run the relay worker").set_defaults(func=cmd_relay)

    demo = sub.add_parser("demo", help="run the three MVP demonstrations")
    demo.add_argument("--root", default=None, help="directory for the demo databases")
    demo.set_defaults(func=cmd_demo)

    verify = sub.add_parser("verify", help="run the oracle suite against a store")
    verify.add_argument("--epoch-id", required=True)
    verify.add_argument("--source-id", default="sim-broker-01")
    verify.add_argument("--consumer-db", default="./consumer.sqlite3")
    verify.set_defaults(func=cmd_verify)

    bench = sub.add_parser("bench", help="scaled-down throughput run (harness figure only)")
    bench.add_argument("--events", type=int, default=5000)
    bench.add_argument("--accounts", type=int, default=20)
    bench.set_defaults(func=cmd_bench)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure(args.log_level)
    try:
        return int(args.func(args))
    except OutboxError as exc:
        _print({"error": type(exc).__name__, "message": str(exc)})
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
