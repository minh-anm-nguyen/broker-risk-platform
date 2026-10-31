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

Full sweep after the assembly, `make test` equivalent (markers
`not slow and not performance`), on Python 3.14 / Windows:

| Component | Result |
|---|---|
| p01-simulator | 508 passed, 25 deselected |
| p02-connector | 510 passed, 19 deselected |
| p03-sync | 229 passed, 117 skipped, 5 deselected |
| p04-reconciliation | 268 passed, 28 deselected |
| p05-risk-engine | 360 passed, 1 skipped, 9 deselected |
| p06-api | 697 passed, 2 skipped, 25 deselected |
| p07-outbox-relay | 379 passed, 1 skipped, 16 deselected |
| p08-analytics | 574 passed, 13 deselected |
| packages/risk-engine | **NOT_RUN — 0 tests collected** (CG-06) |
| **Total** | **3,525 passed, 0 failed, 121 skipped, 140 deselected** |

Read the three right-hand numbers, not just the first. 117 of P03's cases skip
because `SYNC_TEST_DATABASE_URL` is unset — by its own design those cases are
`BLOCKED_ENVIRONMENT`, never `PASS`. The 140 deselected are the slow and
performance suites, which need a declared resource envelope to mean anything.
`packages/risk-engine` collected nothing, and `make test` reports that as
`NOT_RUN` rather than folding it into a green line.

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

**Working and verified in this repository**

- All eight components are implemented, with their own suites, Makefiles and
  `.env.example` files. No placeholder files remain.
- The contract freeze gate runs: `make contracts` performs 68 checks over
  component paths, package names, entrypoints, Dockerfile claims, env prefixes,
  schema references and identity ownership.
- `infrastructure/compose.test.yml` parses, and every build context, Dockerfile
  and bind mount it names resolves on disk.

**Written but never executed**

- `compose.test.yml` has not been brought up. Docker is not available in the
  environment this was assembled in, so the topology is validated structurally
  and nothing more.
- `.github/workflows/platform.yml` has never run: this tree is **not a git
  repository yet**. `git init` is the first step before CI means anything.

**Not built**

- 31 of 32 P09 tickets. No independent oracle, no golden fixtures, no fault
  injectors, no scenario registry, no evidence store, no gate evaluator. See
  [`test-platform/README.md`](test-platform/README.md) for the slot-by-slot
  state.
- Therefore: **0 of 600 what-if cases, 0 of 40 crash points, 0 of 20 E2E flows,
  0 of 16 benchmarks executed. Release gates G0–G5 are NOT EVALUATED.**

No throughput, latency, HA, RPO or RTO figure has been measured. Every such
number in the design documents is a target.

## Open contract gaps

`make contracts` prints these on every run. They are integration defects found
by reading the code, recorded rather than patched over — P09 §1.2 makes
detecting drift P09's job and fixing production schemas explicitly not its job.

| ID | Severity | Gap |
|---|---|---|
| CG-01 | high | Two P05 surfaces exist and are not API-compatible. P06 imports the 1.2k-line `risk_engine` library, not the 8.2k-line `risk` package. A P06 response does not exercise the full P05 policy, gate and adapter code. |
| CG-04 | high | `tenant_id` is required by the P09 identity vector and by INV09-14, but there is no platform-wide tenant model. Cross-tenant isolation cannot be tested end to end. |
| CG-06 | high | `packages/risk-engine/tests/` is empty. The money library every financial response flows through has no suite of its own. |
| CG-02 | medium | P07 emits five event types; the P09 design manifest froze three. A consumer written to the document would miss `SourceEpochChanged` and `ReconciliationVerified`. |
| CG-03 | medium | `apps/p02-connector/docker-compose.yml` uses `SIMULATOR_*` and port 8000; P01 reads `SIM_*` and listens on 8001. That file cannot produce a working lab. `compose.test.yml` has the correct wiring. |
| CG-05 | low | `fee_coverage` is structurally `NOT_AVAILABLE` because P03 carries no `fee` field. Correct by design; recorded so nobody "fixes" it with a zero. |

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
