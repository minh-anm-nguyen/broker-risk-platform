# Project 08 — ClickHouse Broker Analytics

Kafka `DealRecorded.v1` → append-only raw observations → immutable verified
analytics builds → one fenced publication pointer → read-only queries.

Implementation of `docs/Project_08_ClickHouse_Broker_Analytics_System_Design.md`.
Section references throughout the code and this file point at that document.

```
P01 Simulator → P02 Connector → P03 Sync → P07 Outbox → Kafka
                                                          │
                                       ┌──────────────────┘
                                       ▼
                    ingest (validate → quarantine → insert → commit)
                                       ▼
                        broker_events_raw   (append-only, duplicates kept)
                                       ▼
                    build (pin cut → scan conflicts → canonicalise)
                                       ▼
                 analytics_deals_build    (exactly one row per Deal)
                                       ▼
                 analytics_daily_build    (grouped per currency)
                                       ▼
          oracles: source set equality + independent Decimal recomputation
                                       ▼
          PostgreSQL publication pointer (fenced CAS, one TX, auditable)
                                       ▼
                       read-only API, every row from one build
```

## The one rule everything follows

> A slow answer, a `STALE` answer or a refusal is acceptable.
> A believable wrong monetary total is not.

Every design choice below is downstream of that asymmetry. When the two
conflict, this system refuses to answer.

## Quick start

```bash
make install          # the package plus test extras; no Kafka/ClickHouse needed
make demo             # the end-to-end slice, with a duplicate delivery injected
make smoke            # the full offline suite minus the slow tier
make gates            # the release scorecard, including what is NOT supported
```

`make demo` prints the independent oracle's totals beside the published ones
and exits non-zero if they disagree — or if the read comes back anything other
than `READY` from the build that was just published.

## How the pieces fit

| Package | Responsibility |
|---|---|
| `contracts/` | Frozen agreements with P03, P04, P05, P07. Nothing here may be relaxed to make a test pass. |
| `ports/` | Protocols for Kafka, ClickHouse and the control plane. |
| `adapters/` | Deterministic in-process models (used by every test) and the real drivers. |
| `ingest/` | Validate → durably quarantine → insert → commit the highest *contiguous* offset. |
| `build/` | Pin the cut, detect conflicts, canonicalise, roll up, prove, publish. |
| `publication/` | Pointer resolution, rollback, and GC with hard protections. |
| `query/` | Read-only API: scope narrowing, build binding, signed cursors. |
| `faults/` | The 28 named kill points of section 12. |
| `fixtures/` | The 20 golden fixtures of section 9.3, generated deterministically. |

### Why the tests need no Kafka or ClickHouse

The hard parts of this project are *timing* properties: an ambiguous insert
ACK, a partition assignment lost mid-flight, a lease expiring during a long
aggregation, a retention truncation. Reproducing those against real brokers
means sleeping and hoping. So the ports have in-process implementations that
model the same observable contract — append-only storage with no uniqueness
constraint, offsets that are "next to read", generations that fence commits,
retention that truncates — and faults are *scheduled* rather than waited for.

The production adapters satisfy the same protocols, so a passing test says
something about the logic that ships. What it does **not** cover is the driver
integration itself; see *Declared gaps*.

## Tests

587 tests, no external services, deterministic.

| Suite | Tests | What it establishes |
|---|---:|---|
| `unit` | 267 | Money, canonical encoding, envelope validation, policy, dedup, rollups, oracles |
| `component` | 102 | Each adapter's own contract (offsets, fencing, retention, CAS, leases) |
| `contract` | 33 | The HTTP contract and scope isolation |
| `integration` | 47 | The 18 acceptance scenarios plus golden-fixture conformance |
| `chaos` | 42 | All 28 kill points and all 12 compound campaigns |
| `property` | 11 | Metamorphic properties plus a stateful model |
| `whatif` | 72 | Coverage across the 44 failure domains |
| `performance` | 13 | Benchmark shape and resource bounds |

```bash
make test        # everything, including the 100k-Deal build
make chaos       # just the kill points and campaigns
make perf        # benchmarks, excluding the 100k tier
make evidence    # evidence bundles in the layout of section 11.3
```

### Three testing decisions worth knowing

**The oracle is written twice.** `tests/helpers.py` recomputes every expected
total from the fixture definitions using plain `decimal`, importing nothing
from `analytics.build`. The duplicated day-bucketing and summation logic *is*
the test; an oracle that read P08's own rollups would agree by construction
and prove nothing (section 9.1). `verify_rollups` additionally refuses a call
whose two sides are not genuinely two sides.

**A fault that did not fire is `NOT_RUN`, not `PASS`.** `FaultPlan` records
every site it was asked about and every fault it delivered, and
`assert_fired()` fails a scenario whose injection silently missed. `CrashError`
inherits `BaseException` so the `except Exception` handlers in the pipeline
cannot swallow a simulated process death.

**Mutation is tested directly.** `CC08-12` takes a verified build, mutates the
aggregation seven ways (a cent on the gross, a flipped fee sign, a doubled
count, a dropped swap, a relabelled currency, a shifted day, a relabelled
account) and requires the independent oracle to catch every one. If that test
ever passes vacuously, the rest of the suite means nothing.

## What the code refuses to do

These are not defensive extras; each one is a failure mode the design names.

- **Add USD to EUR.** The currency is part of the rollup key and
  `CurrencyBuckets` offers no grand-total method. There is no code path that
  could sum across currencies (INV08-07).
- **Accept a JSON float for money.** A fractional JSON number is poison at the
  decode boundary. `0.1` is not 0.1, and letting it through would make every
  later sum wrong by an invisible amount (section 7.2).
- **Pick a winner between conflicting observations.** Two payload hashes for
  one `event_id`, or two content hashes for one Deal, quarantine the build. No
  `any()`, no `argMax()`, no "latest wins" (INV08-02, INV08-03).
- **Commit a Kafka offset on an unproven insert.** An ambiguous ACK means the
  rows may well be durable; replaying is harmless and committing is not
  (ADR08-09).
- **Skip a record it could not parse.** Poison gets a durable receipt before
  the stream advances past it, or the partition pauses (INV08-21).
- **Treat `count()` as completeness.** A cut missing one Deal and duplicating
  another has the right count. The proof is a two-way set difference against
  P03's manifest (section 6.3).
- **Infer a tenant.** An unmapped source is refused. A default tenant is a
  cross-tenant leak with extra steps (INV08-17).
- **Publish without evidence.** The control plane's pointer transaction
  re-checks state, evidence rows, the live lease, the trust revision and the
  expected pointer revision — all inside one transaction (section 6.5).
- **Report a net P&L it cannot stand behind.** With commission-and-swap-only
  coverage the figure is labelled `NET_COMPONENTS_COVERED`, never `NET_FULL`
  (INV08-15).
- **Convert currencies.** FX needs a versioned rate set agreed with P05.
  Enabling the flag still raises (FR08-12).

## Declared gaps

Section 18 is explicit that a feature is not supported because its rejection
test passed. `make gates` prints this list; it is part of the deliverable.

| Gate | Not supported, and why |
|---|---|
| **G1** real-infrastructure integration | `clickhouse_http.py` and `control_postgres.py` are reviewed but **not exercised** — no run against a real ClickHouse or PostgreSQL is recorded. The guard *logic* they share with the SQLite adapter is tested; the driver integration is not. |
| **G2** performance SLOs | Section 13.3's targets (100 events/s sustained, a 100k build inside 2 minutes, read p95 under 500 ms, RPO/RTO) are **UNMEASURED**. The benchmarks measure P08's own logic against in-process adapters and must not be quoted as evidence for them. |
| **G3** replication / HA | `ReplicatedMergeTree`, Keeper and cross-replica read-after-write are P2 (what-if domain **AF**, **INV08-24**). Single-node read visibility is proven; no HA or RPO/RTO claim is made. |
| **G3** corrections | `DealCorrected`/`DealReversed` v2 is gated pending a P07 schema upgrade. Only the safe rejection is tested. |
| **G3** multi-currency conversion | Needs a P05 rate-set contract. Only the refusal is tested. |

`p08 invariants` lists all 24 invariants with the suite that proves each one,
and marks the one (INV08-24) this repository cannot prove.

## Running against real infrastructure

```bash
make docker-up     # pinned ClickHouse 24.8, PostgreSQL 16.4, Kafka 3.8 (KRaft)
make migrate       # apply both schemas
make docker-down
```

`P08_CH_SERVER_VERSION_PIN` must name the version actually deployed; the app
refuses to start on the design's placeholder (section 13.1). Single-node Kafka
and single-node ClickHouse are **not** high availability.

## Operator commands

```bash
p08 quarantine                                  # unresolved poison receipts
p08 quarantine --resolve <id> --note "<why>"    # a note is required
p08 gc --tenant lab-tenant                      # plan: collectable vs protected
p08 gc --tenant lab-tenant --execute --actor me # an actor is required
p08 fixtures / p08 killpoints / p08 invariants
```

Resolving poison and collecting builds require a note or an actor. That is not
ceremony: an incident without a recorded reason becomes untraceable, and GC is
the one maintenance task that can destroy a published report.

## Configuration

`.env.example` documents every key. The settings module fails at startup
rather than in production, so outside `dev`/`test` it requires a pinned
ClickHouse version, a non-empty tenant map, distinct reader and operator
tokens, and a durable cursor signing key. `auto.offset.reset` is pinned to
`none` and synchronous insert ACK cannot be turned off.

## Status

Implemented and passing offline: ingestion with contiguous-offset discipline,
semantic dedup, immutable verified builds, fenced publication with rollback,
the read-only API, GC protections, the 20 golden fixtures, the 18 acceptance
scenarios, the 28 kill points, the 12 compound campaigns, metamorphic and
stateful property tests, and 44-domain what-if coverage.

Not established: everything under *Declared gaps*. A resilient system is a
claim supported by repeatable results, not by the number of scenarios listed.
