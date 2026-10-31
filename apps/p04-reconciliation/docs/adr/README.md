# Architecture Decision Records

Written for: a developer about to change this service, who needs to know which
decisions are load-bearing and why.

ADR-001 to ADR-008 implement the decisions proposed in SYSTEM_DESIGN section
16.1. ADR-009 to ADR-012 record decisions this build had to make that the
design did not specify — chiefly because no PostgreSQL server and no Project
03 exist in this environment. Those four are the ones most likely to be wrong
for a real deployment, so they state their limits explicitly.

---

## ADR-001 — Project 04 is never a second writer for synced data

**Status:** accepted · **Drivers:** section 0.2 decision 3, INV04-07, INV04-18

**Decision.** Project 03 owns `deals`, `accounts_current`, `positions_current`
and `sync_checkpoints`. Project 04 reads them and orchestrates Project 03; it
never writes them.

**Why.** Two writers for financial projections means two sources of truth for
"what did the broker say?", and no way to attribute a wrong value afterwards.
An audit tool that can also mutate what it audits cannot be trusted to report
on itself.

**How it is enforced, not just intended.** The action vocabulary
(`errors.ActionType`) has four members and none edits a value or deletes a
row, so a destructive action is *unrepresentable*. Independently, the database
connection denies those writes at the engine level (ADR-010).

**Consequence.** Repairing anything requires Project 03 to cooperate. When
Project 03 is down, nothing can be repaired — correctly. Adding
`UPDATE_DEAL` would not be a feature; it would remove the structural reason
this service cannot corrupt money.

---

## ADR-002 — `INCONCLUSIVE` is a first-class outcome

**Status:** accepted · **Drivers:** sections 0.2 decision 8, 5.1, INV04-01

**Decision.** Any unmet comparison precondition ends the run in
`INCONCLUSIVE` with a reason code. It is never promoted to `MATCHED`, and a
terminal run cannot be moved out of that state.

**Why.** The worst defect this service can have is reporting agreement it did
not prove. A false `MATCHED` lets a real discrepancy reach Projects 05–06 as
trusted data; a false `INCONCLUSIVE` costs a retry.

**Consequence.** Most of the code exists to reach this state correctly, and a
non-zero CLI exit for `INCONCLUSIVE` is normal rather than an error.

---

## ADR-003 — Cut compatibility is per entity, not global

**Status:** accepted · **Drivers:** sections 5.2, 5.3, INV04-03, INV04-04

**Decision.** Deals, being immutable within an epoch, are comparable when
`W >= S`. Mutable projections (accounts, positions) require the *same* cut;
otherwise the run is `DIFFERENT_CUT`.

**Why.** Requiring equal snapshot numbers for deals would make most audits
impossible for no safety gain. Accepting unequal cuts for mutable rows would
report every legitimate in-between change as corruption — and then "repair"
it.

**Consequence.** `alignment.py` is the only place that decides this, and
`diff_engine.py` refuses to emit per-item findings when it says no.

---

## ADR-004 — Absence is an observation, never a deletion or a close

**Status:** accepted · **Drivers:** sections 4.3, 5.3, F-05, F-06

**Decision.** A key present in the target and missing from a complete,
unfiltered source listing yields `ABSENT_AT_SOURCE_*` at `PROVISIONAL`
confidence. Only a source that reports `status=CLOSED` closes a position.
A filtered listing cannot support an absence claim at all.

**Why.** "I did not see it" and "it does not exist" differ, and the gap
between them is where filtered listings and truncated scans live. Treating
absence as deletion turns a source bug into permanent data loss.

**Consequence.** Absence findings never produce a dispatchable action, and the
source port refuses a filtered scan outright.

---

## ADR-005 — Money conflicts quarantine; they are never corrected

**Status:** accepted · **Drivers:** sections 4.3, 8.1, INV04-05, INV04-13

**Decision.** The same `(epoch_id, deal_id)` with different canonical content
is `IMMUTABLE_DEAL_CONFLICT` at `CRITICAL`. No automatic action exists.

**Why.** There is no authoritative value to choose. Last-write-wins, "trust
the source", and "trust the target" are all guesses, and a guess about money
is worse than a documented unresolved conflict.

**Consequence.** These incidents need a human and RB04-03. A full re-sync is
*not* a fix and may hide the conflict.

---

## ADR-006 — A receipt is not verification

**Status:** accepted · **Drivers:** sections 4.2, 7.5, INV04-12

**Decision.** `APPROVED` is not `APPLIED`, `ACCEPTED` is not `COMMITTED`, and
`COMMITTED` is not `VERIFIED`. Only a fresh, independent audit that returns
`MATCHED` at a compatible cut may mark a plan `VERIFIED`.

**Why.** A receipt says Project 03 believes it did something. It says nothing
about whether the target now agrees with the source — and a repair that
committed against a stale source is exactly the case that looks successful.

**Consequence.** Every repair costs a second full audit. The verification run
is a real `recon_runs` row with `verifies_plan_id` set, so "verified" is
traceable to the evidence that justified it.

---

## ADR-007 — An ambiguous outcome is `UNKNOWN`, resolved by receipt query

**Status:** accepted · **Drivers:** sections 0.2 decision 7, 7.5, 7.6, INV04-09

**Decision.** A lost response after dispatch yields `UNKNOWN`. Resolution is
to query Project 03 for the *same* deterministic `intent_id`. Retrying blind
and failing fast are both forbidden.

**Why.** Retrying risks executing the work twice; treating it as failed loses
a committed change. Only Project 03 knows, so we ask it.

**Consequence.** `intent_id = SHA256(plan_id + order + type + parameters_hash)`
is derived, never random, so it survives a process restart.

---

## ADR-008 — Evidence age bounds a plan's validity

**Status:** accepted · **Drivers:** section 8.1, FMEA "stale worker"

**Decision.** A plan carries the preconditions it was built under (epoch,
published scan, watermark, lease version, policy hash) and is re-checked at
dispatch. Stale evidence, a moved revision, or a lost lease makes it `STALE`.

**Why.** A plan is a statement about a world that existed when the evidence was
captured. Applying a correct decision to a world that has moved is how a good
judgement becomes a wrong action.

**Consequence.** Long approval queues produce stale plans. That is intended:
re-plan on fresh evidence rather than re-approving an old one.

---

## ADR-009 — Project 03 is represented by a contract harness

**Status:** accepted, provisional · **Driver:** Project 03 does not exist yet

**Decision.** Project 04 depends on two narrow ports
(`ports/p03.py`). `src/recon_harness/p03_sync.py` implements them with the
semantics Project 03's design specifies, and owns the P03 tables.

**Why.** The alternative — waiting for Project 03 — would leave this service
untested. Defining the dependency as two small ports means the harness can be
replaced without touching the audit logic.

**Limits, stated plainly.** No result obtained against the harness is evidence
about Project 03. The harness is deliberately *stricter* than a mock in the
places that matter (the watermark only advances after a terminal page,
publications are atomic, intents dedup), because a lenient counterparty would
let real bugs pass.

**Revisit when:** Project 03 exists. The contract tests in
`tests/contract/test_source_contract.py` are the checklist its ports must
satisfy.

---

## ADR-010 — Privilege separation via the SQLite authoriser

**Status:** accepted, environment-forced · **Drivers:** INV04-07, INV04-18

**Decision.** `db/engine.py` installs a connection authoriser that returns
`SQLITE_DENY` for INSERT/UPDATE/DELETE on every table in
`P03_OWNED_TABLES`, and for all schema changes outside the migrator role.

**Why.** The design requires a database role that *cannot* write Project 03's
tables. No PostgreSQL server is available here, so `GRANT`/`REVOKE` cannot be
executed. The authoriser is the closest genuine equivalent: it denies inside
the engine, so a hand-written `UPDATE deals` raises `not authorized` exactly
as `permission denied for table deals` would.

**Limits.** This is an emulation of the `GRANT` block in
`database/migrations/0001_recon_schema.sql`, not a substitute for it.
Production must use real roles; the authoriser protects a single process, not
a database.

**Why it is still worth having.** It makes INV04-07 a *tested fact* in this
build rather than a convention, and it would catch an accidental write
introduced by a future change.

---

## ADR-011 — SQLite in WAL mode as the runnable engine

**Status:** accepted, environment-forced · **Driver:** section 7.3

**Decision.** The service runs on SQLite with `journal_mode=WAL`. The
PostgreSQL schema in `database/migrations/` stays authoritative and unexecuted.

**Why.** Section 7.3's target-capture protocol needs a transaction-scoped
read snapshot and must fail closed when the cut moves. PostgreSQL provides
that with `REPEATABLE READ`; SQLite's WAL read transactions provide the same
property, and a capture whose snapshot went stale is refused with
`SQLITE_BUSY_SNAPSHOT`. Both are mapped to `TARGET_CAPTURE_CONFLICT`.

**What this genuinely tests.** The hybrid-read failure mode is real here, not
simulated: `tests/integration/test_target_capture_consistency.py` publishes
concurrently from another thread and asserts every completed capture belongs
to exactly one publication.

**What it does not test.** PostgreSQL MVCC specifics — vacuum behaviour,
serialisation failures, long-transaction bloat, WAL exhaustion, failover.
Domains D and Q of the what-if matrix are mostly `NOT_RUN` for this reason.
No performance figure here transfers to PostgreSQL.

---

## ADR-012 — Target hashes are always recomputed, never copied

**Status:** accepted · **Driver:** section 6.4

**Decision.** `target_capture.py` recomputes each row's digest from its
business fields using Project 04's canonical mapper. Project 03's stored
`payload_hash` is never compared directly.

**Why.** Section 6.4 permits comparing stored hashes only once both encoders
are confirmed identical — and they are not. Comparing them unverified would
report either every row as mismatched, or (worse, if the encoders happened to
agree on a shared bug) nothing at all.

**How the rule is kept honest.** The harness hashes rows with a deliberately
*different* scheme (`p03-storage-v1`). Any future shortcut to comparing stored
hashes therefore fails the contract tests instead of passing by luck.

**Cost.** The capture computes hashes in Python rather than in SQL, so the
whole capture holds one transaction instead of being a single statement. That
is still one consistent cut, which is what section 7.3 actually requires.

---

## Open questions carried forward (section 16.2)

These are unresolved and deliberately not decided here:

1. **Multi-currency money policy.** Only USD has a declared scale. A second
   currency needs an explicit scale and conversion policy before any
   cross-currency comparison is meaningful.
2. **Coordinated global cut.** Strong cross-entity invariants need a tested
   upstream contract that provides one. Until then the tier is capped at `T1`,
   and `cross_entity_consistent` is reported as `false`.
3. **Deal backfill protocol.** Enabled by a flag but not exercised against a
   real Project 03. The immutability guarantee under replay needs its own
   review before this is used outside tests.
4. **Retention and partitioning at scale.** Manifests for millions of keys
   need partitioning and a measured retention policy; the current sweep is
   correct but not sized.
5. **External audit anchor.** The hash chain detects in-place edits within
   this database. A privileged operator who rewrites every row and recomputes
   the chain is undetectable without an append-only external store.
