# Project 04 — Snapshot Reconciliation & Recovery

An implementation of `docs/Project_04_Snapshot_Reconciliation_and_Recovery_System_Design.md`:
an evidence-first audit of Project 03's synced state against an independent
source oracle, plus fenced, approval-gated recovery.

The service has two separate halves, exactly as the design requires:

- **Reconciliation (read-only).** Capture source and target evidence at a
  known *epoch* and *snapshot cut*, classify differences with a confidence
  level, and persist durable findings.
- **Recovery (controlled).** Emit only permitted intents to Project 03, under
  preconditions, fencing, idempotency and audit — then **re-check
  independently** before anything is called `VERIFIED`.

---

## What this build actually proves, and what it does not

This section is first because the design makes truthfulness a release gate
(sections 0.4, 1.2, 9) and the environment imposes real limits.

### Measured state of this build

| Suite | Count | Result |
|---|---|---|
| `tests/unit` + `tests/property_based` + `tests/security` | 157 | pass |
| `tests/contract` + `tests/integration` | 95 | pass |
| `tests/chaos` (14 crash windows + a real process kill) | 15 | pass |
| `tests/campaigns` (registry and reporting honesty) | 16 | pass |
| G0 + G1 release gates over a populated database | 20 checks | pass |
| `tests/performance` | 4 | pass |
| What-if campaign, P0 tier | 52 of 208 executed | 52 pass, 0 fail, 156 `NOT_RUN` |

Measured on SQLite in this environment (`artifacts/benchmark_*.json`), with
fsync disabled for the fixture database — **not** PostgreSQL figures, and they
validate no target in section 11.4:

- a full audit of 1,200 deals over 12 pages: **6.2 s**, ~193 rows/s;
- RSS delta for that audit: **136 KiB**, about 116 bytes per row, which is the
  evidence that manifests stream rather than materialise (section 5.2 rule 6).


The campaign's 52/208 is the honest figure: every unexecuted case is
registered with the specific reason it cannot run here (no PostgreSQL, no real
Project 03, no OS-level network fault injection), and `coverage_summary()`
reports implemented-versus-declared per domain so the gap cannot be mistaken
for a pass rate.

Three defects in this service were found by its own tests during development
and fixed: `FINDINGS` was wrongly treated as terminal, blocking section 4.1's
`FINDINGS → PLANNED` transition; INV04-11 was unenforced, so an approved plan
could dispatch against a regenerated source because P03's epoch row stays
`ACTIVE` until re-bootstrap; and an audit of an unknown epoch died on a raw
foreign-key error instead of a named refusal. The third was caught by the
campaign rather than by the unit and integration suites.

### Proven here, by executable tests

| Claim | Where |
|---|---|
| A clean world audits as `MATCHED` and agrees with an independent P01 read | `tests/integration/test_clean_baseline.py` |
| A source scan cannot be called complete without a contiguous, single-cut, terminal receipt chain | `db/repository.py::mark_source_complete`, `tests/chaos/test_crash_windows.py` |
| Mutable projections at different cuts yield `DIFFERENT_CUT`, never "corrupt" | `tests/integration/test_cut_alignment.py` |
| `W < S` yields `TARGET_NOT_CAUGHT_UP`, never a missing-deal finding | same |
| Deal money conflicts quarantine and produce no automatic correction | `tests/integration/test_deal_findings.py` |
| Absence from a listing is never treated as a close or a delete | same, plus `test_cut_alignment.py` |
| Target capture is one consistent cut, or it fails closed | `tests/integration/test_target_capture_consistency.py` |
| Project 04's DB role cannot write any Project 03 table | `tests/security/test_privilege_and_inputs.py` |
| A lost ACK becomes `UNKNOWN` and resolves without double execution | `tests/integration/test_repair_lifecycle.py`, `tests/chaos/test_crash_windows.py` |
| `COMMITTED` never implies `VERIFIED` | same |
| A fenced worker and a stale plan cannot dispatch | same |
| All 14 crash windows of section 7.6 recover per their stated rule | `tests/chaos/test_crash_windows.py` |
| The 12 metamorphic rules of section 10.4 hold | `tests/property_based/test_metamorphic.py` |
| A lying source does not cause a deletion, and the independent oracle catches it | `tests/integration/test_source_faults.py::...truncated_source...` |

### Explicitly **not** proven here

1. **Nothing about Project 03.** Project 03 does not exist in this repository.
   Its two contracts are implemented by `src/recon_harness/p03_sync.py`, a
   clearly-labelled contract harness. Every result is evidence about Project
   04 only.
2. **Nothing about PostgreSQL.** No PostgreSQL server is available in this
   environment. The service runs on SQLite in WAL mode, whose read
   transactions give the snapshot isolation section 7.3 depends on, and whose
   `SQLITE_BUSY_SNAPSHOT` gives the fail-closed behaviour IT04-07 requires.
   `database/migrations/0001_recon_schema.sql` holds the authoritative
   PostgreSQL schema **and has not been executed**.
3. **DB role grants are emulated.** INV04-07/INV04-18 are enforced here by a
   SQLite connection authoriser that denies writes at the engine level. That
   is a genuine engine-level denial and a genuine test, but production must
   use the `GRANT`/`REVOKE` block in the reference migration.
4. **No performance target is validated.** Section 11's numbers are proposals.
   `tests/performance/` measures and records what it can on SQLite with fsync
   disabled; those numbers are not PostgreSQL measurements and validate no
   target in section 11.4.
5. **Not all 312 what-if cases are executed.** 52 are implemented as real
   executable cases with real faults and independent oracles; the remainder
   are registered as `NOT_RUN` with the specific reason each cannot run here.
   `reconctl campaign` reports implemented-versus-declared per domain so the
   gap is visible rather than implied. A coverage claim that hid the skips
   would be the dishonesty section 9 warns about.
6. **No global cross-entity cut.** Upstream provides three independent
   snapshots, so the cross-entity tier is capped at `T1` regardless of
   configuration (section 0.4).

---

## Layout

```text
mt5-reconciliation-service/
├── src/reconciliation/
│   ├── errors.py            reason codes, finding codes, closed action allowlist
│   ├── canonical.py         versioned canonical mapping + content hashing (§6.4)
│   ├── config.py            fail-fast settings; every bound explicit (§8.1)
│   ├── clock.py             injectable time, so expiry is decided not awaited
│   ├── dto.py               typed records crossing the source trust boundary
│   ├── db/
│   │   ├── schema.py        SQLite DDL + table-ownership sets
│   │   ├── engine.py        role-scoped connections, capture transactions (§7.3)
│   │   └── repository.py    recon_* persistence; validated completeness CAS
│   ├── ports/
│   │   ├── source.py        P01 HTTP driver + programmable fake (§2.1)
│   │   └── p03.py           the two Project 03 contracts (§2.2)
│   ├── source_capture.py    page receipts, resume after lost ACK (§7.4)
│   ├── target_capture.py    one consistent database cut (§7.3)
│   ├── alignment.py         the §5.1 precondition gate
│   ├── diff_engine.py       classification + confidence (§4.3)
│   ├── policy.py            guardrails and repair eligibility (§8.1)
│   ├── planning.py          immutable, content-addressed plans (§7.5)
│   ├── approvals.py         RBAC, separation of duties, expiry (§12.4)
│   ├── leases.py            scheduler fencing by lease version (INV04-10)
│   ├── executor.py          fenced dispatch, UNKNOWN handling (§7.5, §7.6)
│   ├── verifier.py          independent post-repair audit (INV04-12)
│   ├── scheduler.py         the §7.1 sequence, end to end
│   ├── audit.py             hash-chained, redacted audit log (§12.2)
│   ├── metrics.py           counters and real-percentile histograms (§12.1)
│   ├── evidence.py          §10.7 artifact manifests, honest result vocabulary
│   ├── campaign.py          the §9 what-if runner and case registry
│   └── cli.py               reconctl (§13.3)
├── src/recon_harness/
│   └── p03_sync.py          Project 03 CONTRACT HARNESS (not Project 03)
├── database/migrations/     authoritative PostgreSQL schema + GRANTs (not run)
├── docs/runbooks/           RB04-01..07 decision trees (§15)
└── tests/{unit,contract,integration,chaos,property_based,security,performance,campaigns}
```

---

## Running it

The simulator from Project 01 is the source. Nothing needs installing beyond
the shared virtualenv; `pythonpath` in `pyproject.toml` puts `src/` on the
path.

```bash
# 1. start the Project 01 simulator (separate shell, from mt5-broker-simulator)
python -m simulator.cli serve          # or: uvicorn simulator.main:create_app --factory

# 2. check what the source promises before trusting anything it says
reconctl source check --source-url http://127.0.0.1:8001

# 3. register its current epoch with the Project 03 harness
reconctl source bootstrap

# 4. audit
reconctl run start --entity deals --epoch <EPOCH-UUID>
reconctl run show  --run-id <RUN-UUID>
reconctl run evidence --run-id <RUN-UUID> --redacted

# 5. propose a repair (dry run by default -- nothing is persisted)
reconctl plan create --run-id <RUN-UUID> --actor auditor-1
reconctl plan create --run-id <RUN-UUID> --actor auditor-1 --commit

# 6. a *different* person approves the exact plan hash
reconctl plan approve --plan-id <PLAN> --plan-hash <SHA256> --actor approver-2

# 7. dispatch, then verify independently -- dispatch is not verification
reconctl plan dispatch --plan-id <PLAN> --plan-hash <SHA256>
reconctl run verify --plan-id <PLAN>

# auxiliary
reconctl intent status --intent-id <ID>     # authoritative P03 receipt
reconctl audit verify                       # recompute the audit hash chain
reconctl campaign --tier P0 --evidence-dir ./artifacts
```

Exit codes: `0` success, `1` a refusal or non-matching audit, `2` usage error.
A non-zero exit from `run start` is normal — `INCONCLUSIVE` and `FINDINGS` are
valid outcomes, not errors.

### Verifying a build

Two commands check the service end to end without needing a separate server;
both boot Project 01 in-process.

```bash
# The full lifecycle, including every refusal (section 18's demo).
# Leaves a populated database behind for the gate check below.
python scripts/demo_end_to_end.py --db ./data/recon.db

# The G0/G1 release invariants, as queries over that database (section 14).
python scripts/verify_release_gates.py --db ./data/recon.db
```

The demo walks: a clean `MATCHED` audit of all three entities; a corrupted
account detected, planned, refused for self-approval, refused for missing
approval, approved by a second actor, dispatched, and independently
`VERIFIED`; a deal money conflict that yields zero dispatchable actions; a
moved source cut reported as `DIFFERENT_CUT`; a lost ACK resolved to
`COMMITTED` with `execution_count == 1`; and a stale plan refused at dispatch.
It exits non-zero if any of those outcomes — including any of the refusals —
does not occur.

The gate script runs 10 G0 checks (data correctness) and 10 G1 checks
(controlled recovery) against every row the database has ever held, including
"no plan is `VERIFIED` without an independent `MATCHED` verification run" and
"no action records more than one committed attempt". A violation there is a
correctness defect, not a flaky test: it means the service recorded a verdict
its own evidence does not support.

G2 (what-if campaigns) is reported by `reconctl campaign`. G3 (performance and
operations) needs the measurement rig in section 11.5 and is **not** verified
by either script.

### Configuration

Environment variables, prefixed `RECON_` (see `config.py` for all bounds):

| Variable | Default | Meaning |
|---|---|---|
| `RECON_DB_PATH` | `./data/recon.db` | reconciliation database |
| `RECON_SOURCE_BASE_URL` | `http://127.0.0.1:8001` | Project 01 base URL |
| `RECON_SOURCE_DATA_TOKEN` | — | required outside local runs |
| `RECON_APPROVAL_REQUIRED` | `true` | cannot be disabled in `prod` |
| `RECON_ALLOW_AUTO_DISPATCH` | `false` | refused in `prod` |
| `RECON_ALLOW_DEAL_BACKFILL` | `false` | P1; touches immutable history |
| `RECON_MASS_DRIFT_FRACTION` | `0.20` | blast-radius cap (§8.1, tune) |
| `RECON_EVIDENCE_MAX_AGE_S` | `300` | plan goes stale past this (§8.1, tune) |
| `RECON_MAX_PAGES_PER_RUN` | `10000` | hard scan budget |
| `RECON_RUN_DEADLINE_S` | `1800` | hard run budget |
| `RECON_CROSS_ENTITY_TIER` | `T1` | cannot exceed what the source supports |

Every knob that can change a verdict is hashed into `policy_hash` and stored
on each run, so two runs taken under different thresholds are visibly
incomparable rather than silently so.

---

## Testing

```bash
python -m pytest tests/unit tests/property_based tests/security -q   # fast
python -m pytest tests/integration -q                               # needs P01
python -m pytest tests/chaos -q                                     # slow
python -m pytest -q -m "not slow"                                   # commit suite
```

Markers: `unit`, `contract`, `integration`, `chaos`, `property`, `security`,
`performance`, `campaign`, `slow`.

Two testing decisions are worth knowing about:

**The source is the real simulator.** Integration tests boot Project 01
in-process and drive its real pagination, cursor signing, snapshot semantics
and fault-injection API. A hand-rolled stand-in would mostly prove that two of
my own assumptions agree (section 10.1).

**The oracle is independent where it can be.** The canonical hash tests build
the expected digest by assembling canonical JSON **as a hand-written string**,
not by calling the encoder under test, and also pin exact digests — so the
encoder and the oracle cannot drift together unnoticed. Integration tests
re-read P01 directly and recompute root hashes outside the capture path.

**Fault activation is asserted, not assumed.** Where a test injects a fault it
checks the injector's own activation counter (P01's for transport faults, the
harness's for repair faults). A case that declares a fault and cannot confirm
it fired is downgraded to `INVALID_TEST` by `evidence.py`, never counted as a
pass (section 10.6).

---

## Design decisions worth knowing before changing anything

**The action vocabulary is closed.** `errors.ActionType` has four members and
none of them edits a value or deletes a row. Adding `UPDATE_DEAL` would not be
a feature; it would remove the structural reason this service cannot corrupt
money. Section 8.1's "automatic write patch to P03 tables: never" is true here
because it is unrepresentable, not because a reviewer is watching.

**`INCONCLUSIVE` is a success path.** Most of this codebase exists to reach it
correctly. The worst bug this service can have is reporting agreement it did
not prove, so every missing precondition ends the run with a reason code
rather than a verdict.

**Confidence follows the alignment gate, not the data.** A hash difference
across two different cuts is *expected*. `diff_engine.py` therefore refuses to
emit per-item findings at all when `alignment.py` blocked strict comparison; it
emits one run-level finding carrying the blocking reason.

**Hashes are always recomputed from business fields.** Project 03's stored
`payload_hash` is never compared directly, because section 6.4 only permits
that once both encoders are confirmed identical. The harness deliberately uses
a *different* hash scheme so any code that shortcuts to comparing stored hashes
fails the tests instead of passing by luck.

**`source_seq` is provenance, not content.** It travels beside the digest and
is excluded from it, so the same immutable deal hashes identically no matter
which scan observed it.

**Decimals come from strings, and scale normalisation is lossless or refused.**
`Decimal("1.0")` and `Decimal("1.00")` hash equal under the declared USD scale.
`Decimal("1.004")` is an error, not a rounded `1.00`: rounding would invent a
value the source never sent.

---

## Operator runbooks

`docs/runbooks/` implements the decision trees in section 15:

| ID | Situation |
|---|---|
| RB04-01 | Source disconnected during an audit |
| RB04-02 | Source generation reset |
| RB04-03 | Deal conflict in amount, commission or swap |
| RB04-04 | Project 03 commit ACK lost, status `UNKNOWN` |
| RB04-05 | Repeated cursor expiry on a large scan |
| RB04-06 | Database disk full or failover |
| RB04-07 | Post-repair verification failed |

---

## Handoff to Projects 05–06

Consumers must be given the metadata in section 13.4 and must display it:
`source_epoch_id`, each entity's `published_scan_id`/`snapshot_seq`, the Deal
watermark, the latest reconciliation result and
`unresolved_critical_findings`. A `MATCHED` deal audit does **not** mean the
broker is globally consistent, and `cross_entity_consistent` is `false` here
because no global cut exists to justify `true`.
