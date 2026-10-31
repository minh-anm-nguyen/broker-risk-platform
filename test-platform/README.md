# Project 09 — Integration Testing & Performance platform

The control plane that verifies Projects 01–08. It sits **outside** the system
under test and is designed to disbelieve it: P01–P08 build the product, P09
decides whether a claim about the product is supported by evidence.

Design document: [`Project_09_Integration_Testing_and_Performance_System_Design.md`](../docs/system-design/Project_09_Integration_Testing_and_Performance_System_Design.md)

## What exists today

| Piece | §17 path | State |
|---|---|---|
| Contract freeze gate (T09-01) | `runner/contracts.py` | **Working.** 71 checks, run via `make contracts`. Reports every gap with its decision status, so an undecided gap is visible as `UNDECIDED` |
| Contract manifest + schemas | `../contracts/` | **Working.** 1 manifest, 3 JSON Schemas, 9 gaps - 5 closed, 4 open with recorded decisions |
| T1 topology (T09-03) | `../infrastructure/compose.test.yml` | **Written, unexecuted.** No Docker in the authoring environment, so it has never been brought up |
| Scenario registry (T09-16) | `scenarios/` | Empty — 0 of 600 cases written |
| Independent oracle (T09-07/08) | `oracle/` | Empty — not started |
| Golden fixtures (T09-05/06) | `fixtures/` | Empty — 0 of 24 |
| Fault injectors (T09-10..15) | `fault-injectors/` | Empty — not started |
| Crash points (T09-23) | `crash-points.yaml` | Absent — 0 of 40 |
| Compound campaigns (T09-30) | `campaigns/` | Empty — 0 of 20 |
| Benchmarks (T09-25..28) | `benchmarks/` | Empty — 0 of 16 |
| Evidence store + gate evaluator (T09-32) | `reporting/` | Empty — not started |
| Runbooks (T09-31) | `runbooks/` | Empty — not started |

**1 of 32 tickets implemented.** Everything else here is a named, empty slot.
That is deliberate: an empty directory is honest, and a stub test that passes
without asserting anything is the one failure mode this project exists to
prevent (INV09-33, §19.3).

### What the one working ticket turned out to be worth

T09-01 was meant to be bookkeeping. It found that four of the eight seams
between P01–P08 were broken while all 3,500-odd component tests were passing,
because every component had been tested against its own stand-in for its
neighbour. That is the argument for this whole project in one sentence, and it
came from the cheapest ticket in it.

It also caught a false finding of its own: a reported SQLite-only P05 reader
that turned out to be dialect-aware. A gate is only useful if its output can be
wrong and get withdrawn, so that one was withdrawn from the manifest.

## Scorecard

Carried forward from design §19.2, unchanged because nothing has been executed:

| Item | Planned | Executed |
|---|---:|---|
| What-if cases | 600 | 0 |
| P0 cases | 400 | 0 |
| Golden fixtures | 24 | 0 |
| E2E flows | 20 | 0 |
| Crash/kill points | 40 | 0 |
| Compound campaigns | 20 | 0 |
| Benchmarks | 16 | 0 |
| Tickets | 32 | 1 (T09-01) |
| Release gates G0–G5 | 6 | **NOT EVALUATED** |

Real throughput, p95/p99, HA, RPO and RTO: **not measured.** Any number quoted
for those today is a target from a design document, not a result.

## Running what works

```bash
make contracts          # from the repository root
make contracts-strict   # an open gap fails; this is the G1 posture
```

The gate is read-only by design. When it reports drift, the fix belongs in the
component or in the manifest — never in the gate (§1.2).

## Rules that bind anything added here

1. **Independence.** The oracle must not import P05 finance code and must not
   trust P08's build status. It recomputes expected values from the fixture
   manifest with its own `Decimal` arithmetic (§17, T09-08).
2. **A fault must actually fire.** `NOT_TRIGGERED` is a distinct outcome and a
   NO-GO for its gate. It is never counted as a pass (INV09-33, §19.3).
3. **Seven outcome states, no collapsing.** `PASSED`, `FAILED`,
   `INVALID_SETUP`, `NOT_TRIGGERED`, `SKIPPED_INCOMPATIBLE`,
   `BLOCKED_DEPENDENCY`, `NOT_RUN`. Only `PASSED` counts toward coverage (§2.4).
4. **Three truth planes must agree.** Source manifest ↔ persisted PostgreSQL
   rows ↔ served P06/P08 output. `pg_count == ch_count` is insufficient: both
   can lose the same Deal or share the same wrong amount (§2.3).
5. **Full denominators.** Offered, completed, failed, rejected, dropped and
   timed-out requests are all reported. A p95 computed over successes only is
   an invalid benchmark (INV09-34, §12.1).
6. **Evidence is append-only.** A `FAILED` result cannot be overwritten
   (INV09-32).
7. **Fault endpoints are test-namespace only** and must be disabled in any
   image something else depends on (INV09-39).

## Suggested first vertical slice

From design §18, before any attempt at the 600-case matrix:

> `GF09-07` + a `DealRecorded` duplicate after Kafka ACK ambiguity + a P08
> verified publication + an independent money oracle.

That single scenario exercises the contract, the fault injector, the three
truth planes and the gate evaluator at once. It is worth more than a hundred
cases that only call healthy endpoints.
