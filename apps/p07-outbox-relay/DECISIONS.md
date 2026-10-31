# Decisions

The design document ends with five decisions "still to lock before coding"
(section 20) and seven required integration ADRs (section 2). Writing code
forced answers to all of them. This file records what was chosen, where it is
enforced, and what it costs — so the next person can disagree with the decision
rather than reverse-engineer it from the code.

---

## The five open decisions

### 1. Which P03 source scope starts with the outbox enabled

**Chosen: greenfield test epoch only.**

An event is created only for a Deal that P03 *newly inserts* while the writer is
active. There is no backfill path, and `oracles.o1_deal_to_outbox_set_equality`
takes an `epoch_id` parameter precisely so it cannot be pointed at a database
containing pre-integration Deals and then "fixed" by loosening it until it
proves nothing.

**Cost:** adopting this on a legacy database needs a separate manifest and
provenance plan that does not exist here. Until that exists, pointing the writer
at an epoch with history would leave older Deals permanently without events, and
O1 would correctly report them.

*Enforced at:* `oracles.o1_deal_to_outbox_set_equality`, `p03.SyncService._insert_one_deal`.

---

### 2. Whether a `DealRecorded` payload may carry money without a proven currency

**Chosen: yes, with a truthful status, and no aggregation.**

The payload carries `money_currency_status` of `PROVEN` or `UNPROVEN`.
`UNPROVEN` means the amounts are exact and verifiable by hash while their unit
is unknown. `money.MoneyCurrency` makes the two states unforgeable: there is no
way to spell `PROVEN` without supplying a code, and a payload claiming `PROVEN`
with no code — or `UNPROVEN` with one — is rejected on parse rather than
resolved in the payload's favour, because either half could be the lie.

The reference consumer counts unproven-currency Deals in
`unproven_currency_deal_count` and never adds them to a currency total.
Suppressing the amounts entirely was the alternative; it was rejected because
the amounts are genuine facts and withholding them would force P08 to re-fetch
from P03, trading a clear "unit unknown" signal for a hidden dependency.

**Cost:** a downstream consumer that ignores the status field will read zeros
where amounts exist. The status is required rather than optional so that
ignoring it is a visible omission.

*Enforced at:* `money.MoneyCurrency`, `events.schema` (cross-field rule),
`consumer.inbox.apply_deal`.

---

### 3. Which aggregate key preserves business order

**Chosen: per-account Deal stream.**

`aggregate_key` is the account id, so the ordering stream a consumer projects is
the account's Deal stream. The alternative — a per-Deal stream, where every Deal
is its own aggregate at version 1 — would eliminate head contention entirely and
also eliminate the ordering guarantee, leaving the consumer with no basis for
detecting a missing event.

**Cost:** one head row per account is a contention point, and a hot account
serialises on it. `fixtures.skewed_load` exists so the benchmark measures that
case rather than an even spread, and
`test_b07_04_a_hot_account_keeps_a_contiguous_contended_stream` asserts 300
events on one account still come out contiguous.

*Enforced at:* `writer.OutboxWriter.record_deal`, the
`UNIQUE (source_id, epoch_id, aggregate_key, aggregate_version)` index.

---

### 4. Whether a snapshot marker is a notification or a snapshot image

**Chosen: notification only.**

The marker carries `scan_id`, `snapshot_seq`, `record_count`, a manifest digest
and `notification_only: true`. It does not carry the accounts or the positions.
A consumer that receives one re-reads the P03 read model.

`notification_only` is a required field that must be literally `true` (schema
kind `FLAG_TRUE`), rather than a convention, so a consumer cannot mistake a
marker for the data — which is ADR07-06's actual requirement.

**Cost:** a consumer needs read access to P03 to act on a marker. Embedding the
snapshot would remove that coupling and would put a whole account set on an
analytics topic, with the size and privacy consequences that implies.

*Enforced at:* `events.schema._SNAPSHOT_FIELDS`,
`writer.OutboxWriter.record_snapshot_published`, `consumer.reference._project`.

---

### 5. Topic isolation and retention model

**Chosen: shared topics per event class, with isolation in the key, the ACL and
the consumer scope.**

Four topics by event class (`deals`, `snapshots`, `lifecycle`, `poison`), not
one per broker. Isolation is enforced in three independent places: the partition
key includes the source and epoch, the broker ACL restricts which identities may
produce or consume, and the reference consumer re-checks `source_id` against its
own scope before any side effect.

Three layers rather than one because each fails differently: a key collision
would merge ordering streams, an ACL gap would expose data, and a consumer
scoped too widely would apply another tenant's events to its own projection.
`test_it07_14_a_foreign_tenant_event_is_never_applied` exercises the third with
the same account id in two tenants.

**Cost:** a shared topic needs stronger per-consumer filtering than a
broker-specific topic would, and retention is uniform across tenants. A tenant
needing different retention needs its own topic, which is a migration.

*Enforced at:* `events.keys.partition_key`, `events.routing.authorize_source`,
`consumer.reference._handle`, `broker.embedded` ACL fields.

---

## The seven integration ADRs

| ADR | Decision | Enforced at |
|---|---|---|
| **07-01** Do not own P03's schema | The outbox tables are additive. `p03/` is a declared stand-in, not a reimplementation, and its DDL is kept separate from the outbox DDL in `schema_sql.py` | `persistence.schema_sql` (two DDL blocks), `p03/__init__.py` |
| **07-02** An event only for a genuinely new Deal | `INSERT ... ON CONFLICT DO NOTHING`; an identical re-fetch emits nothing and a conflicting one raises `IntegrityIncident` | `p03.SyncService._insert_one_deal`, `writer.OutboxWriter._emit` |
| **07-03** No marker for a partial scan | The writer reads `sync_runs` *inside the transaction* and refuses anything not `PUBLISHED`. A caller cannot describe a staged scan as published | `writer.OutboxWriter.record_snapshot_published` |
| **07-04** `source_seq` has gaps | Nothing derives completeness from it. Fixture `G07-04` uses sequences 5, 9, 20 and asserts no Deal is synthesised for the gaps | `fixtures.G07_04`, `oracles.o1_*` (keyed on deal id, never on sequence) |
| **07-05** P04 markers fence on the revision | The reconciliation revision *is* the event identity, so one revision yields one event however many times the hook runs | `writer.OutboxWriter.record_reconciliation_verified` |
| **07-06** Markers are self-describing | `notification_only` is required and must be `true` | `events.schema`, `test_it07_05` |
| **07-07** Topic config is pinned before benchmarking | Topic names, partition counts and retention are code constants, so a change is a reviewable diff. Routing is resolved at *write* time and stored, so a pending row keeps the destination it was written for | `events.routing.TopicSpec`, `writer.OutboxWriter._emit` |

---

## Decisions the design left open that the code had to settle

### The CAS predicate excludes the lease deadline

`mark_published_if_owner` conditions on `state='CLAIMED' AND claim_token=? AND
claim_version=?`, and deliberately **not** on `lease_until > now`, although
design section 7.3 mentions an "active lease".

A reclaim is what increments `claim_version`, so the token pair already catches
every case where another worker holds the row. If nobody has taken it over, an
expired lease means only that this worker is late — and rejecting its write
would send a row that was already delivered back for another publish,
manufacturing a duplicate to guard against a conflict that does not exist.

*Recorded at:* `SqliteOutboxStore._cas`, with both cases tested
(`test_an_expired_lease_with_no_takeover_can_still_complete` and IT07-07).

### `event_id` is derived, not random

`event_id = f(source_id, dedupe_key)`, shaped as a v4 UUID. Stronger than the
design asks for: even a writer that somehow bypassed the unique index could not
mint a second identity for one fact. The cost is that the id is not opaque — it
is reproducible by anyone who knows the key — which is acceptable because the
key is already derived from the business identity.

*Enforced at:* `events.keys.derive_event_id`, re-verified on parse in
`Envelope.parse_wire`.

### Every source identifier is hex-encoded inside a composite key

Not a convention to remember but the only encoding available: an account id of
`A:42` cannot be made to collide with account `A` in an epoch spelled `42`,
because neither ever appears raw inside a key. This applies to the dedupe key
and to the partition key, since a partition key is also an ordering identity.

*Enforced at:* `events.keys.hex_identity`, `events.keys.partition_key`.

### Excess decimal precision is rejected, not rounded

`"0.1"` and `"0.10"` normalise to one string and therefore one hash, which is
the metamorphic property section 14.1 asks for. `"0.105"` into a scale-2 field
raises instead of rounding, because padding loses nothing and truncation loses
money.

*Enforced at:* `money.normalize`.

### A quarantined record blocks its aggregate by name where identity is legible

A hash mismatch still tells us the source, epoch and aggregate — only the
payload is untrustworthy. `OutboxError.context` carries those identifiers out of
the failure so the consumer blocks one account rather than halting every account
that happens to hash onto the same partition. Where identity is *not* legible (a
malformed envelope), the partition halts, which is the wider blast radius the
design acknowledges.

*Enforced at:* `events.envelope.parse_wire` (context), `consumer.reference._quarantine`.

### A version gap rolls the transaction back entirely

Design section 8.1 permits buffering out-of-order events within strict limits.
This implementation pauses instead: no inbox row, no projection change, a
`blocked_reason` on the aggregate. Buffering trades a visible stop for an
invisible one, and only the visible stop gives an operator something to act on.
No buffering setting is exposed, deliberately -- a knob that does nothing is
worse than no knob, because someone will tune it and expect an effect.

*Enforced at:* `consumer.reference._apply`.
