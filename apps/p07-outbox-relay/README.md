# Project 07 — PostgreSQL Outbox & Kafka

A runnable implementation of `docs/Project_07_PostgreSQL_Outbox_and_Kafka_System_Design.md`:
a transactional outbox written inside the business transaction, an at-least-once
relay to Kafka with lease fencing, and an idempotent reference consumer whose
business effect lands exactly once per logical event.

```
P03 sync TX ──┬─► deals / sync_runs          (business tables)
              └─► event_outbox               ONE commit, both rows
                      │
                 claim + lease
                      ▼
              relay ──► produce ──► broker ──► reference consumer
                      ▲            (log)          │
                 delivery ACK                     ├─► inbox (event_id)
                      │                           ├─► version ledger
                 CAS by claim token               └─► projection
```

## What this proves, and what it does not

This is the part to read before quoting any number from it.

**Genuinely exercised.** Atomic business + outbox commit, rollback at every
kill point, the semantic-key uniqueness that makes a retry a no-op, the
per-aggregate version certificate, claim/lease/CAS fencing between competing
owners, the consumer's inbox and version guard, offset-after-database ordering,
quarantine and audited replay. These are properties of ACID transactions and
SQL constraints, and they run against a real transactional database.

**Modelled, not proven.** The default broker is an in-process Kafka *model*
(`outbox.broker.embedded`). It reproduces enqueue-then-callback, per-partition
append order, manual offset commit, retention truncation and a fault set that
includes the genuinely ambiguous ACK — which is what makes every interleaving in
the kill-point inventory reachable in milliseconds. It says **nothing** about
replication, ISR behaviour under leader election, or the durability of an
acknowledged record. Design section 16.1 is explicit that a single broker is not
a durability proof; a model in the same process is less. Those claims need the
`HA-LIKE` lab (3 brokers, RF=3, `min.insync.replicas=2`) and
`outbox.broker.confluent`.

**Not measured.** Every throughput and latency figure the design lists is a
*target to be measured*, not a result. `outbox bench` and `tests/performance`
print harness rates with no network, no replication and no broker fsync. They
assert *shape* — the backlog drains completely, resources stay inside their
caps, cost does not grow superlinearly — never magnitude.

**Substituted.** Project 03 is a separate mini-project and is not implemented
here, so `outbox.p03.sync_service` is a stand-in call site: real transactions
and real kill points, no paging, connector or checkpoint protocol. The
PostgreSQL store (`outbox.persistence.postgres_store`) carries the design's DDL
and the `FOR UPDATE SKIP LOCKED` claim, and runs only when `OUTBOX_PG_DSN` is
set and the `postgres` extra is installed.

## Quick start

```bash
pip install -e '.[test]'
outbox demo              # the three MVP demonstrations from design section 0.1
outbox bench --events 5000
pytest -q                # the whole suite (~400 tests, a couple of minutes)
pytest -q -m 'not slow'  # the per-commit smoke suite
python scripts/collect_evidence.py --include-slow   # per-layer release verdict
```

No install is strictly required — `pythonpath` in `pyproject.toml` puts `src` on
the path — so `pytest -q` works in a fresh checkout. `make help` lists the rest.

`outbox demo` runs, in order:

1. **Atomic commit.** A Deal and its event commit together; the consumer applies
   it once.
2. **ACK then kill.** The relay is killed after the broker acknowledged but
   before it wrote that back. Two physical records appear in the log under one
   event id, and the projection moves once.
3. **Outage and recovery.** Nothing is published while the broker is down, the
   backlog stays bounded, and it drains completely afterwards.

It finishes by running oracles O1–O7 and exits non-zero if any fails.

## Layout

| Path | What lives there |
|---|---|
| `events/` | Envelope v1, identity derivation, the payload registry, topic routing |
| `canonical.py`, `money.py`, `clock.py` | The one canonical encoding, decimal-string money, injectable time |
| `writer/` | `OutboxWriter` — runs *inside* P03's transaction, cannot open its own |
| `persistence/` | DDL in both dialects, the claim query, the fencing CAS |
| `relay/` | Admission limits, backoff, circuit breaker, the publish cycle, the worker loop |
| `broker/` | Producer/consumer ports, the in-process model, the `confluent-kafka` adapter |
| `consumer/` | The reference sink: inbox, version ledger, projection, quarantine |
| `oracles.py` | O1–O8, computed independently of the system under test |
| `faults/` | The 24 named kill points and their fire counts |
| `ops/` | Quarantine inspection, sanitised poison notices, audited replay |
| `p03/` | The Project 03 call-site stand-in |

## The decisions worth knowing

**`produce()` is an enqueue, not an ACK.** The broker port has no call that both
sends and confirms. A record is enqueued with a callback that only runs while
`poll()` is being called, so a relay that forgets to poll stalls visibly instead
of reporting a healthy publish rate while losing events.

**`PUBLISHED` requires a delivery callback *and* a successful CAS.** Not an
enqueue, not the absence of an error.

**A failed CAS is not a delivery failure.** If the broker acknowledged and a
standby has since taken the row, the event *is* in Kafka. It is counted as a
stale ACK and left alone; rescheduling it would be a lie in the metrics and a
second publish in the log.

**The event id is derived, not random.** `event_id = f(source_id, dedupe_key)`,
shaped as a v4 UUID. Stronger than the design requires: even a writer that
bypassed the unique index could not mint a second identity for one fact.

**Every source identifier is hex-encoded inside a composite key.** An account id
of `A:42` cannot be made to collide with account `A` in an epoch spelled `42`.

**Money is decimal strings at a pinned scale, and excess precision is rejected.**
`"0.1"` and `"0.10"` hash identically; `"0.105"` into a scale-2 field raises
rather than rounding.

**An unknown currency stays unknown.** `money_currency_status=UNPROVEN` means the
amounts are exact and the unit is not established. The projection counts those
Deals and never adds them to a currency total. Nothing invents USD because the
simulator uses USD.

**The CAS predicate is the claim token and version, not the lease deadline.**
A reclaim is what bumps the version, so the token pair already catches every
case where someone else holds the row. Checking the lease too would reject a
late write from the *current* owner and manufacture a duplicate to prevent a
conflict that does not exist. Reasoning is recorded at
`SqliteOutboxStore._cas`.

**A version gap pauses the aggregate and rolls back entirely.** No inbox row, no
projection change — the event is deferred, not processed, and an inbox row would
later make it look applied.

**Backlog gauges are read every cycle; the storage figure is not.** Counts come
from an index on `state`; `PRAGMA page_count` / `pg_total_relation_size` runs on
a 5-second cadence, because storage grows at the speed of a backlog rather than
of a poll loop.

**A quarantined record blocks its aggregate by name where identity is legible.**
A hash mismatch still tells us the source, epoch and aggregate, so one account
stops rather than every account that hashes onto the same partition.

## Test layers

396 tests, one skipped (the PostgreSQL parity test, which needs `OUTBOX_PG_DSN`).

| Marker | Test functions | What it covers |
|---|---|---|
| `unit` | 116 | Canonical encoding, money policy, identity, schema registry, envelope, routing, backoff, admission limits, settings, error taxonomy, clock |
| `contract` | 31 | IT07-01 … IT07-20 from design section 14.4, one test per clause |
| `integration` | 90 | Claim/lease/CAS and concurrent owners, SQL constraints and the immutability trigger, the broker model's own semantics, ops/health/metrics/redaction |
| `chaos` | 36 | KP07-01 … KP07-24 individually, plus compound campaigns C07-A … C07-D |
| `property` | 14 | A Hypothesis state machine over the real pipeline, plus metamorphic equivalences |
| `mutation` | 15 | Deliberately broken implementations the suite must reject |
| `performance` | 8 | Scaled-down B07 suites; shape assertions and recorded measurements |

Counts are test functions; parametrised cases expand beyond them.

`tests/chaos/test_kill_points.py::test_every_kill_point_is_covered` fails if a
site in the inventory has no test naming it, so the inventory cannot drift into
a list of good intentions.

`tests/mutation/` is the suite testing itself. Each case builds one of the
mutants design section 15.2 names — a skipped outbox insert, `PUBLISHED` before
the callback, a missing inbox key, the offset committed before the database, a
flipped fee sign, the epoch dropped from the dedupe key, a skipped stale-worker
CAS, versions applied out of order — and asserts an oracle catches it. A
surviving mutant means the corresponding real test is decorative.

## Honest status against the design's gates

| Gate | Status here |
|---|---|
| G07-0 Contract | Schema, identity and topic policy pinned in code; currency status explicit |
| G07-1 Atomicity | Executed. KP07-01…10 and oracles O1–O3 pass against a real transactional store |
| G07-2 Delivery | Executed against the broker *model*: KP07-11…23, duplicate/reorder/offset oracles pass |
| G07-3 Security | Tenant allowlist, epoch/source binding, ACL paths, log and DLQ redaction tested. No TLS/SASL — there is no real broker here |
| G07-4 Faults | 24 kill points and 4 compound campaigns executed with O8 evidence. **Not** the full 320 P0 matrix |
| G07-5 Performance | **Not assessed.** Harness figures only |
| G07-6 Operational readiness | Quarantine, audited replay, drain estimate, storage policy, health semantics tested. No backup/DR drill |
| G07-7 Production-like | **Not reached.** Needs the 3-broker lab |

Unimplemented by design, and listed so nothing above is read as covering them:
P1 multi-relay sharding, schema registry, the P04 atomic verification hook, the
P08 ClickHouse handoff, and the P2 CDC/Debezium option.
