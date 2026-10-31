# Broker Risk Platform

A monorepo assembling nine mini-projects that reconstruct a **Broker Risk
Reporting and MT5 Integration** system: eight components that build the
product, plus a test platform whose job is to disbelieve them.

Layout, gate names and separation-of-duty rules follow
[Project 09 §17 and §19](docs/system-design/Project_09_Integration_Testing_and_Performance_System_Design.md).

```
broker-risk-platform/
├── apps/
│   ├── p01-simulator/        P01  deterministic synthetic broker source + fault injection
│   ├── p02-connector/        P02  typed MT5 adapter, bounded blocking-call isolation
│   ├── p03-sync/             P03  authoritative PostgreSQL Deal store, watermark, snapshots
│   ├── p04-reconciliation/   P04  read-only evidence, findings, fenced repair proposals
│   ├── p05-risk-engine/      P05  full engine: domain + DB adapters + policies + tooling
│   ├── p06-api/              P06  thin authorized HTTP read layer
│   ├── p07-outbox-relay/     P07  same-transaction outbox, fenced relay, reference consumer
│   └── p08-analytics/        P08  raw ingest, canonical build, immutable verified publication
├── packages/
│   ├── platform-events/            shared event contract: envelope, identity,
│   │                               schema, routing, money. No DB, no broker.
│   └── risk-engine/          P05  pure Decimal domain library (what P06 actually imports)
├── contracts/                      frozen interface manifest + JSON Schemas (T09-01)
├── infrastructure/
│   ├── compose.test.yml            T1 integration topology (T09-03)
│   └── docker-compose.yml          superseded narrow P06 lab
├── test-platform/            P09  orchestration, oracles, fault injection, release gates
├── docs/system-design/             the nine design documents
└── .github/workflows/              platform.yml + per-component pipelines
```

## Directory renames from the pre-assembly layout

| Was | Now |
|---|---|
| `mt5-broker-simulator/` | `apps/p01-simulator/` |
| `mt5-connector/` | `apps/p02-connector/` |
| `trading-data-sync/` | `apps/p03-sync/` |
| `mt5-reconciliation-service/` | `apps/p04-reconciliation/` |
| `broker-risk-engine/` | `apps/p05-risk-engine/` |
| `apps/risk-reporting-api/` | `apps/p06-api/` |
| `postgres-outbox-kafka/` | `apps/p07-outbox-relay/` |
| `broker-analytics-clickhouse/` | `apps/p08-analytics/` |
| `docs/*.md` | `docs/system-design/*.md` |

Path references in `infrastructure/`, `.github/workflows/` and
`apps/p06-api/Dockerfile` were rewritten to match. The old
`.github/workflows/risk-reporting-api.yml` is now `p06-api.yml`.

### Path arithmetic the move broke

Four places located files by counting `.parent` hops, which assumed each
component sat directly under the repository root. Adding the `apps/` level
broke all four. Each now searches upwards for the target instead, so the next
layout change does not repeat this:

| File | Was looking for | Symptom |
|---|---|---|
| `apps/p03-sync/tests/unit/test_harness_integrity.py` | the P03 design document | 4 tests failed with `FileNotFoundError` |
| `apps/p03-sync/scripts/whatif_report.py` | the P03 design document | wrong `--design` default |
| `apps/p02-connector/scripts/run_whatif_suite.py` | the P02 design document | wrong `--design` default |
| `apps/p04-reconciliation/tests/conftest.py` | the P01 simulator `src/` | 93 collection errors, `No module named 'simulator'` |
| `apps/p04-reconciliation/src/reconciliation/campaign.py` | the P01 simulator `src/` | **silent** — the guard was `if simulator_src.exists()`, so a campaign booted with no simulator on the path and failed later somewhere unrelated. It now raises. |

The last one is the instructive case: it is exactly the failure mode the design
documents keep warning about. The code did not report that it could not find
its source; it carried on and produced a result attributed to a source that
never ran.

## Inventory

Counted from the tree, not estimated:

| Component | src LOC | src files | test LOC | test files |
|---|---:|---:|---:|---:|
| p01-simulator | 11,321 | 40 | 8,620 | 24 |
| p02-connector | 9,119 | 29 | 8,580 | 22 |
| p03-sync | 6,640 | 30 | 7,284 | 31 |
| p04-reconciliation | 11,220 | 31 | 6,557 | 14 |
| p05-risk-engine | 8,158 | 31 | 9,500 | 15 |
| p06-api | 7,430 | 46 | 6,738 | 23 |
| p07-outbox-relay | 9,112 | 45 | 5,526 | 14 |
| p08-analytics | 13,112 | 48 | 7,401 | 18 |
| packages/risk-engine | 1,236 | 6 | **0** | **0** |
| **Total** | **77,348** | 306 | **60,206** | 161 |

## Verified test status

`make test` across every component (markers `not slow and not performance`),
Python 3.14 on Windows:

| Component | Result |
|---|---|
| p01-simulator | 508 passed, 25 deselected |
| p02-connector | 510 passed, 19 deselected |
| p03-sync | 255 passed, 123 skipped, 5 deselected |
| p04-reconciliation | 268 passed, 28 deselected |
| p05-risk-engine | 360 passed, 1 skipped, 9 deselected |
| p06-api | 699 passed, 3 skipped, 25 deselected |
| p07-outbox-relay | 379 passed, 1 skipped, 16 deselected |
| p08-analytics | 574 passed, 13 deselected |
| packages/platform-events | 7 passed |
| packages/risk-engine | 87 passed |
| **Total** | **3,647 passed, 0 failed, 128 skipped, 140 deselected** |

Read the right-hand numbers, not just the first. 123 of P03's skips are
database-backed cases, including the six that would prove the outbox
atomicity invariant; without `SYNC_TEST_DATABASE_URL` they are
`BLOCKED_ENVIRONMENT`, which by the project's own rule is not a pass. The 140
deselected are the slow and performance suites, which need a declared resource
envelope to mean anything.

`make test` reports an empty suite as `NOT_RUN` on its own line rather than
folding it into a green summary. That is not decoration: it is how
`packages/risk-engine` was found to have no tests at all, and later how a
phantom entry in the component list was found - both would otherwise have
looked like success.

## Quick start

```bash
make help              # every target
make contracts         # contract freeze gate - the fastest real signal
make test              # fast suite across every component
make test-one C=apps/p03-sync
make up / make down    # T1 topology (requires Docker)
```

The bundled `.venv` is Python 3.14; components declare `>=3.12`.

## What is actually established

**Verified in this repository**

- All eight components are implemented, with their own suites, Makefiles and
  `.env.example` files. No placeholder files remain.
- The contract freeze gate runs: `make contracts` performs 71 checks over
  component paths, package names, entrypoints, Dockerfile claims, env prefixes,
  schema references and identity ownership, and reports every gap with its
  decision status.
- `packages/platform-events` holds the shared event contract and is proven to
  import with no dependency on any component package - checked both through
  `sys.modules` and by reading the source, so a lazily imported component is
  caught too.
- `infrastructure/compose.test.yml` parses, and every build context, Dockerfile
  and bind mount it names resolves on disk.

**Implemented but never executed against the thing it must work against**

- **The P03 outbox write.** P03 builds a `DealRecorded` and inserts it on its
  own session inside the deal-page transaction. The pure half is fully tested;
  the insert needs PostgreSQL, so the six atomicity cases report
  `BLOCKED_ENVIRONMENT`. That file is also the first thing that would apply
  P07's outbox DDL over P03's migrated schema, so the schema-compatibility
  question is open with it.
- `compose.test.yml` has not been brought up - no Docker in this environment.
- `.github/workflows/platform.yml` has never run; the repository has two local
  commits and no remote.

**Not built**

- 31 of 32 P09 tickets. No independent oracle, no golden fixtures, no fault
  injectors, no scenario registry, no evidence store, no gate evaluator. See
  [`test-platform/README.md`](test-platform/README.md).
- Therefore **0 of 600 what-if cases, 0 of 40 crash points, 0 of 20 E2E flows,
  0 of 16 benchmarks executed. Release gates G0-G5 are NOT EVALUATED.**

No throughput, latency, HA, RPO or RTO figure has been measured.

## The eight seams

The thing worth knowing about this codebase. Every component's own suite passed
while four of the eight seams between them were broken, because each component
was tested against its own idea of its neighbour: P07 against a 336-line
stand-in P03 it wrote itself, P06 against a reference engine, P05's reader
against SQLite shaped like P03. Each was internally consistent, and together
they did not form a system.

| # | Seam | State |
|---|---|---|
| 1 | P01 → P02 | real - `simulator_sync_http` driver over HTTP |
| 2 | P02 → P03 | real - `source_adapter.py` over httpx |
| 3 | P03 → P04 | real - P04 reads P03's tables and boots P01 in-process |
| 4 | P03/P04 → P05 | real - `p03_reader.py` is dialect-aware for both SQLite and PostgreSQL |
| 5 | P05 → P06 | **scoped** - reference library, not the full engine (CG-01) |
| 6 | P03 → P07 | **implemented, unproven** - was absent entirely (CG-08) |
| 7 | P07 schema ownership | fixed - P07 no longer creates P03's tables (CG-07) |
| 8 | P07 → P08 | real - topics match |

## Contract gaps

`make contracts` prints these on every run with their decision status. They are
integration defects found by reading and then wiring the code - P09 §1.2 makes
detecting drift P09's job and patching production schemas explicitly not its
job. Nothing is left `UNDECIDED`.

**Closed**

| ID | What it was | How it closed |
|---|---|---|
| CG-02 | P07 emits five event types; the design document froze three | `interfaces.p03_to_p07.event_types` in the manifest now lists all five, and the manifest is what the gate checks |
| CG-03 | P02's compose used `SIMULATOR_*` and port 8000; P01 reads `SIM_*` on 8001 | Wiring corrected, including the cursor signing key P01 fails closed without |
| CG-05 | `fee_coverage` is structurally `NOT_AVAILABLE` | Accepted by design, recorded so nobody "fixes" it with a zero |
| CG-06 | The shared P05 money library had an empty test directory | 87 tests over the decimal primitives, every expectation taken from running the code |
| CG-07 | P07's `migrate()` created stand-in copies of five P03-owned tables on the production path | `include_p03_standin` keyword, default False; `outbox migrate --with-p03-standin` reports which ownership was chosen |

**Open, with a recorded decision**

| ID | Sev | Status | Position |
|---|---|---|---|
| CG-08 | critical | implemented-unverified | P03 now builds and inserts the outbox row on its own session inside `persist_deal_page`. 26 unit tests cover the pure emitter; the six database cases report `BLOCKED_ENVIRONMENT` because no PostgreSQL was available. **The chain is implemented, not proven.** |
| CG-10 | critical | scoped | P03 stores money at 12 decimal places; the event contract carries 2 and refuses the rest. Kept at 2; emission fails closed with a typed error naming the deal, the field and both scales. Residual risk: one incompatible deal halts its scan. |
| CG-01 | high | scoped | P06 serves from the 1.2k-line reference library, not the 8.2k-line full P05. A P0 scope decision, now guarded by `tests/unit/test_p05_boundary.py`, which fails if the full engine is half-wired in **or** if the arithmetic moves into the API. |
| CG-04 | high | deferred | No platform-wide tenant model. Unsupported feature named explicitly per §19.1; not a P0 compromise with one tenant, becomes a blocker on the second. |

## Rules that bind the whole tree

Common to all nine design documents, and the reason the gates are shaped the
way they are:

1. **Correct-or-explicitly-unknown.** No `NULL → 0`, no missing quote silently
   reusing the previous price, no unknown currency defaulting to USD.
2. **Money is never a float.** Canonical decimal strings end to end, with
   currency and unit attached. Commission and swap keep their sign.
3. **At-least-once, not exactly-once.** PG → Kafka → ClickHouse can redeliver.
   Every business effect must be idempotent by stable event id.
4. **Fail closed.** Missing quote, currency, policy, authorization, cut or
   provenance means refuse or mark incomplete — never guess.
5. **Bounded everything.** Queues, retries, pools, workers, memory, deadlines
   and cardinality all have measured ceilings.
6. **`health != readiness != correctness`.** A live process proves nothing about
   the data; HTTP 200 does not mean `READY`.
7. **Independent oracles.** Never use the system under test as its own oracle.
   `pg_count == ch_count` is insufficient — both can lose the same Deal.
8. **`NOT_RUN` is not a pass.** An empty suite, a skipped case or a fault that
   never fired is reported as such. `make test` prints these on their own line.

## Housekeeping

- A duplicate `trading-data-sync/` directory may remain at the repository root.
  Its contents were copied to `apps/p03-sync/` and verified identical by
  aggregate SHA-256 before the original was removed; the OS held a handle on the
  directory, so deletion may not have completed. It is safe to delete manually.
- `git init` has not been run. CI, evidence provenance and the `git_sha` field
  required by P09 §1.1 all depend on it.

## Where to start

Per P09 §18, before any attempt at the 600-case matrix, build one vertical
slice: `GF09-07` + a `DealRecorded` duplicate after Kafka ACK ambiguity + a P08
verified publication + an independent money oracle. That single scenario
exercises the contract, a fault injector, all three truth planes and the gate
evaluator at once.

Closing CG-01 is the other high-value move: until it is resolved or explicitly
scoped, "P06 serves P05 results" is true only of the reference library.
