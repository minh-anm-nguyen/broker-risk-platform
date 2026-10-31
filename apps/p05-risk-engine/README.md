# Project 05 — Broker Risk Calculation Engine

Implementation of `docs/Project_05_Broker_Risk_Calculation_Engine_System_Design.md`:
a pure-`Decimal` risk core that computes **exposure**, **realized P&L** and
**unrealized P&L** over the Project 03 read model, or refuses to, with a reason.

The engine's operating principle is *correct-or-explicitly-unknown*. Returning
a wrong amount is treated as a worse outcome than returning no amount, so every
path that cannot prove its inputs fails closed with a machine-readable reason
code rather than substituting a zero, a default currency or a nearby quote.

---

## What it computes, and what it refuses

| Component | Produced when | Refused when |
|---|---|---|
| **Exposure** | A published, terminal position scan and an approved catalog entry for every open symbol | Scan unpublished or non-terminal, unknown position status, off-grid volume, symbol outside the whitelist |
| **Realized P&L** | A terminal, unfiltered deal watermark and a fully classified deal set | Missing money column, unclassified deal type, correction row, cross-currency deal set |
| **Unrealized P&L** | A pinned quote snapshot bound to the position scan, inside the freshness policy | Missing or inverted quote, look-ahead mark, unbound snapshot, non-USD profit currency |
| **Composite total** | All of the above **and** an upstream freeze proof binding the cuts | Anything else — there is no laxer total for callers who would rather have a number |

Worked example from design section 5.6, reproduced exactly by the test suite:

```
BUY 0.50 EURUSD @ 1.10000, commission -2.50
CLOSE 0.20 @ 1.10300, commission -1.00, swap -0.50
remaining 0.30 lots, bid 1.10200 / ask 1.10220

realized gross            +60.00 USD
commission                 -3.50 USD
swap                       -0.50 USD
net_excluding_fee          +56.00 USD      fee_coverage = NOT_AVAILABLE
unrealized gross           +60.00 USD      (1.10200 - 1.10000) * 30 000
exposure                30 000 EUR net,  30 000 EUR gross
composite_total           +116.00 USD      only because the cut is proven
```

---

## The five ideas that shape the code

**1. The cut vector, not a timestamp.** Project 03 publishes accounts,
positions and deals as separate projections with separate publication points.
There is no single `as_of` that describes all three, so a result carries the
identity of *each* cut — `account_scan_id`, `position_scan_id`,
`deal_watermark_seq`, `quote_snapshot_id`, `symbol_catalog_version`,
`recon_revision` — plus a `consistency` verdict. That verdict is `COMPATIBLE`
only when a `cut_proof_id` names an upstream freeze that produced them
together. Populated ids that happen to agree are `INDEPENDENT`, and a composite
total over independent cuts invents a portfolio that existed at no instant.

**2. Typed money.** Every amount is a `Money`: an exact `Decimal` welded to a
currency *and* an aggregation semantic. `usd_gross + eur_gross` raises; so does
`usd_gross + usd_commission`. Money enters as a string — a `float` is rejected
rather than converted, because by the time a price has been through binary
floating point the precision is gone and no later conversion recovers it.
Rounding happens once, at the reporting boundary: summing 1 000 commissions of
`0.005` yields `5.00`, not the `0.00` that per-row rounding produces.

**3. Fail closed, per component.** A stale quote says nothing about last
month's realized P&L, so each component gets its own verdict and its own reason
codes. `BLOCKED` and `UNSUPPORTED` components carry **no value keys at all** —
not `null`, which is one `|| 0` away from a flat line at zero in a dashboard.
`UNSUPPORTED` is reported separately from `BLOCKED`: the first means the feature
is not built, the second means the data cannot be trusted, and conflating them
sends an operator hunting a problem that does not exist.

**4. A whitelist for arithmetic.** `FOREX_LINEAR_P0` is the only calculation
mode with a verified formula, so every other mode — including ones that look
like simple multiplication, such as a CFD with a broker multiplier — resolves
to `UNSUPPORTED_CALC_MODE`. Unit exposure is still reported for those
instruments, flagged `calc_mode_supported: false`, because a lot count derives
from the contract size alone; a money value does not.

**5. Read-only, and provably so.** The role needs `SELECT` and nothing more.
The executor refuses any statement that is not a read before the driver sees
it, the read transaction is always rolled back, and a test hashes every
upstream table before and after a calculation to show the bytes did not move.

---

## Layout

```
broker-risk-engine/
├── src/risk/
│   ├── errors.py            reason-code taxonomy (design section 7.3)
│   ├── status.py            component / global status, cut consistency
│   ├── domain/              pure, I/O-free calculation core
│   │   ├── money.py         Decimal ingress, Money, Quantity
│   │   ├── instruments.py   versioned specs, supported-mode whitelist
│   │   ├── facts.py         validated facts, RiskCut, RiskInputBundle
│   │   ├── classification.py  trading / non-trading / unclassified
│   │   ├── exposure.py      long / short / gross / net by symbol
│   │   ├── realized.py      signed aggregation + coverage disclosure
│   │   ├── unrealized.py    side-aware mark-to-market
│   │   └── fx.py            P1 conversion graph, bounded and side-aware
│   ├── policies/            risk-policy-v1/v2, sim-usd-fx-v1, loader
│   ├── adapters/            SQL port, P03 reader, quote providers
│   ├── gates.py             the fail-closed decision layer
│   ├── manifest.py          canonical hashing and lineage
│   ├── audit.py             idempotent receipts
│   ├── limits.py            admission control and monotonic deadlines
│   ├── contract.py          the library surface Project 06 will wrap
│   └── service.py           capture → gate → compute → fence → audit
├── src/risk_tools/          matrix registry, evidence, fixtures, CLIs
├── database/migrations/     P03/P04 harness schema + P05-owned DDL
└── tests/                   golden, contract, integration, property,
                             security, chaos, performance
```

---

## Running it

```bash
make install          # pip install -e ".[test]"
make test             # the whole suite
make gate             # suite, then enforce the P0 release gate
make report           # matrix coverage from the recorded evidence
make bench            # correctness-gated reference benchmark
```

Without `make`:

```bash
PYTHONPATH=src python -m pytest
PYTHONPATH=src python -m risk_tools.report
PYTHONPATH=src python -m risk_tools.benchmark --profile reference-5k --seed 4205
```

A minimal calculation:

```python
from risk.contract import RiskRequest
from risk.domain.facts import AuthorizedScope
from risk.service import RiskService, ServiceDependencies
from risk.status import Component

scope = AuthorizedScope(                 # from the authenticated context,
    tenant_id="tenant-a",                # never from the request body
    source_id="sim-broker-a",
    account_ids=frozenset({"00042"}),
)
request = RiskRequest(
    epoch_id="3cecd819-fd61-420b-8e35-019c70203b4e",
    components=frozenset(Component),
    period_start=..., period_end=...,    # half-open [start, end)
    valuation_as_of=...,
    quote_snapshot_id="quote-S1030",
    strict_composite=True,
)
result = await service.compute(request, scope)
result.status                            # READY / DEGRADED / BLOCKED / ...
result.component(Component.REALIZED).values["net_excluding_fee"]   # "56.00"
result.composite_total                   # None unless the cut is proven
```

`account_ids` on the request is a **filter**, not an authorization. It can
narrow the granted scope and never widen it, which is what makes a tampered
`source_id` in a request body harmless.

---

## Test strategy

```
tests/golden/        reviewed arithmetic vs an independent oracle
tests/contract/      upstream handshake, versioning, the P06 boundary
tests/integration/   SQL read model, cuts, reconciliation, concurrency
tests/property/      algebraic invariants, metamorphic relations, fuzz
tests/security/      isolation, authorization, SQL safety, redaction
tests/chaos/         crash points K05-01..20, recovery, compound faults
tests/performance/   load, streaming memory, concurrent correctness
```

Four things the suite does deliberately:

**The oracle is independent.** `tests/reference/oracle.py` re-derives the
formulas from the design document using only the standard library and imports
nothing from `risk`. A test asserts that structurally, so a future import
cannot quietly make the expectations circular.

**Mutants are hunted, not described.** Two property tests re-implement the
most tempting single-line bugs — gross computed as `long - short`, and a
swapped bid/ask — and assert the property catches them. A hedged book reports
`gross = 0` under the first, and *gains* the spread under the second.

**Every case has a case id.** Tests carry `@pytest.mark.wf5("WF5-061")`, and
the ids are checked against the 360-case registry at collection time. The
suite writes `artifacts/test_matrix_results.json`, and `risk-report` prints
coverage against the full matrix so an untouched domain shows as a gap.

**A fault that did not fire is not a pass.** A chaos test must confirm its
injection fired; without that confirmation the evidence log downgrades it to
`INVALID_TEST`, which is reported separately from both `PASS` and `FAIL`.

### Measured state

All 360 cases execute and pass, so `make gate` exits zero:

```
P0  total 240   executed 240   passed 240   failed 0   invalid 0
P1  total  90   executed  90   passed  90   failed 0   invalid 0
P2  total  30   executed  30   passed  30   failed 0   invalid 0
NOT_EXECUTED 0 of 360
```

Regenerate with `make test && make report`. Nothing is counted as passing
without a record in the artifact.

### Measured benchmarks

Correctness-gated runs on one development host (Windows, Python 3.14, SQLite
substrate, seed 4205, `risk-policy-v1` / `sim-usd-fx-v1`). These are
observations, not SLOs, and they do not transfer to another environment:

| Profile | Deals | Positions | Correctness | Cold | Warm p50 | Peak RSS | Read |
|---|---:|---:|---|---:|---:|---:|---|
| `smoke` | 200 | 50 | PASS | 0.06 s | 0.05 s | 40 MiB | 6 batches |
| `reference-5k` | 5 000 | 1 000 | PASS | 0.84 s | 0.85 s | 54 MiB | 18 batches |
| `reference-100k` | 100 000 | 10 000 | PASS | 40.7 s | 49.3 s | 185 MiB | 330 batches |

Two things worth stating plainly. The 100k run is **slower** than design
section 15.2's provisional 15-second budget — the budget was written for
PostgreSQL on a 4-vCPU Linux reference host, and this is SQLite in a single
Python process, so the figures are not comparable and the budget is not
claimed. Memory is the result that does transfer: 185 MiB peak against a
512 MiB cap, with the row count read in 330 bounded batches, is evidence the
aggregation streams rather than materialising the set.

The 100k profile also caught a real defect during development, which is the
reason the gate is shaped the way it is. The generator produced timestamps
running *forwards* from the reference instant, so a 100 000-row dataset
overflowed the caller's one-day window; the engine correctly excluded the
overflow and the benchmark's oracle — which summed every generated row —
reported a mismatch. The engine was right and the harness was wrong. Both were
fixed: the generator now runs backwards from the reference instant, and the
oracle applies the same half-open window independently.

---

## Honest limits

These are the places where the implementation is narrower than the design
document, or where a guarantee is weaker than it may appear.

**Project 02, 03 and 04 are not running.** They are design documents. This
project ships `database/migrations/` with the Project 03-shaped read model and
the Project 04 evidence tables so an environment can be stood up, and the
reader is written against that schema — but the end-to-end integration with
those services has not been exercised, because they do not exist yet.

**Most DB tests run on SQLite, not PostgreSQL.** The reader targets a `SqlExecutor`
port with two adapters. The PostgreSQL adapter is real code and uses
`REPEATABLE READ READ ONLY`, which is what gives a capture one database
revision; the suite runs on SQLite so it needs no server. The properties that
transfer — row-set identity, watermark scoping, bounded streaming,
parameterised queries, transaction release — are tested. The ones that do not
are marked `needs_postgres` and **skip** without `RISK_PG_DSN`: specifically
the GRANT-level proof that the role cannot `UPDATE deals`. Structurally the
executor refuses mutating statements, but the database-level control is
unverified here, and the report says so.

**Some database errors are simulated at the port.** A serialization failure
(SQLSTATE 40001), a statement timeout and a disk-full error are injected into
the executor rather than provoked in a server. The tests check the engine's
*handling* — no partial sums, no leaked transactions, bounded retry — and their
evidence records note that the trigger was simulated.

**Benchmarks are measurements on one host, not SLOs.** Figures come from
`artifacts/benchmark-*.json` with the host, seed, policy and catalog version
recorded. Cold and warm runs are reported separately rather than averaged. A
profile whose figures disagree with the reference arithmetic reports
`correctness: FAIL` and **withholds its timings**, because a fast wrong answer
is not a result. The design document's provisional budgets are not claimed as
achieved.

**FX is P1 and not on the P0 path.** `risk.domain.fx` implements a bounded,
side-aware, cycle-safe conversion graph and is tested, but the P0 profile never
calls it: a non-USD profit currency is `UNSUPPORTED_CALC_MODE`.

**`fee_coverage` is permanently `NOT_AVAILABLE` in this build.** Project 03
carries no `DEAL_FEE` column, so there is no all-in net to publish. The
payload has no field named `net`, `net_all_in` or `total` — only
`net_excluding_fee`, named after what it excludes. A test asserts those keys
are absent.

**There is no YAML fault-campaign runner.** Design section 13.6 sketches one.
The chaos suite injects at named barriers in Python instead, which keeps the
injection point, the confirmation and the assertion in one reviewable place and
lets the evidence log downgrade an unconfirmed injection automatically. A YAML
layer would add a format to maintain without adding a check.

**Caching is designed, not built.** `risk_materialized_results` and the cache
key of design section 8.2 are in the migration and the key derivation exists as
the audit idempotency key, but no cache is read on the hot path. Section 8.2's
reasoning applies: invalidation complexity during a correctness build buys
nothing.

**No margin, VaR, liquidation or equity.** Out of scope by design, and the
engine will not add `balance + unrealized` and call it equity.

---

## Reason codes

The full taxonomy is `risk.errors.ReasonCode` (design section 7.3). The ones
most likely to appear in practice:

| Code | Meaning |
|---|---|
| `P03_UNPUBLISHED` | Scan not published, or published without a terminal receipt |
| `DEAL_WATERMARK_UNPROVEN` | Watermark non-terminal, or derived from a filtered scan |
| `RECON_INCONCLUSIVE` / `RECON_BLOCKING` | Project 04 evidence blocks this scope |
| `RECON_EVIDENCE_SCOPE_UNKNOWN` | Evidence exists but does not cover these accounts |
| `INCOMPATIBLE_CUT` | Components are individually valid but not provably aligned |
| `QUOTE_CUT_UNPROVEN` | Mark cannot be tied to the position scan it would value |
| `STALE_MARK` | Quote older than the policy threshold — figures kept, labelled |
| `QUOTE_LOOKAHEAD` | Mark dated after the valuation instant |
| `MONEY_FIELD_MISSING` | A required money column was NULL; never coalesced to zero |
| `UNSUPPORTED_CALC_MODE` | No verified formula for this instrument |
| `DEAL_TYPE_UNCLASSIFIED` | Deal category has no approved mapping |
| `FEE_COVERAGE_UNAVAILABLE` | Disclosure: the net excludes uncovered charges |
| `EPOCH_CHANGED` | Source reset between capture and the final fence |
| `AUDIT_PERSISTENCE_FAILED` | Mandatory audit write failed; no unaudited READY |
| `OVERLOADED` / `DEADLINE_EXCEEDED` | Admission or time budget exhausted |

`RETRYABLE_CODES` names the subset a bounded client retry may clear.
`UNAUTHORIZED_SCOPE` is deliberately absent from it: blind retry of a rejected
authorization is an attack pattern, not a recovery strategy.
