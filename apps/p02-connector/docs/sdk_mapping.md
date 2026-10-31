# Native MT5 Manager SDK mapping — deliberately empty

**Status: does not exist.** This document records what a mapping would have to
contain, and why writing one now would be worse than not having one.

The code agrees with it: `drivers/manager_sdk_windows.py` refuses to construct
on an unqualified host, raises on every read even on a qualified one, and
carries this list as `REQUIRED_EVIDENCE`. Every capability it would negotiate
is `False`.

---

## Why there is nothing here

Three layers are routinely conflated, and only the first is in this
repository:

1. **The Project 01 REST simulator.** A test double designed for this
   project. It defines `run_id`, `generation`, `snapshot_seq`, `source_seq`
   and an opaque signed cursor. These are *its* semantics and carry no claim
   about any vendor API — it says so itself, in
   `capabilities.wire_compatible_with_mt5_manager_api: false`.
2. **The MetaTrader 5 terminal Python integration.** The MQL5-documented
   `initialize` / `account_info` / `positions_get` / `history_deals_get`
   family. It connects through a *terminal*, not through a broker's Manager
   API — a different thing with a confusingly similar name.
3. **The MetaQuotes `mt5manager` distribution.** At the time of writing it
   publishes Windows x86-64 wheels for several CPython versions. Package
   metadata says nothing about thread safety, cancellation, snapshot
   semantics, or whether any particular broker will accept a connection.

A mapping written against assumed semantics would produce a connector that
*looks* integrated. Code returning plausible numbers from an unverified source
is the most expensive failure available here, because nothing downstream can
tell it from the real thing.

---

## What has to be answered first

Each item is a question for vendor documentation, an authorised environment or
a conformance test — not a line of code.

### Access and platform

| # | Question | Why it blocks |
|---|---|---|
| 1 | Is there an authorised broker endpoint and a read-only role that can reach the data we need? | Without it there is nothing to test against, and a least-privilege claim is unverifiable. |
| 2 | Does a wheel whose platform tag matches the target host import successfully *on that host*? | Installing a Windows wheel into a Linux container does not work. The build must fail, not the runtime. |
| 3 | Which API version is in use, and does it have a stability policy? | Needed before any golden fixture means anything. |

### Concurrency and lifecycle

| # | Question | Why it blocks |
|---|---|---|
| 4 | May two calls share one session? | The actor design assumes **no** (ADR-202). Evidence either confirms that or unlocks concurrency. |
| 5 | Can a call be aborted, or does it hang indefinitely? | Decides whether process isolation is mandatory or merely prudent (ADR-204). |
| 6 | On which thread do callbacks arrive, and what happens to one that fires after logout? | A callback mutating state after a session ends is a use-after-free in waiting. |
| 7 | What is the object ownership model — who frees what, and is a double close defined? | A double free in a wrapper is undefined behaviour, not an exception. |

### Data semantics

| # | Question | Why it blocks |
|---|---|---|
| 8 | For every money field: units, scale and sign convention. Is a commission negative? | A flipped sign turns every cost into revenue, and nothing downstream would notice. |
| 9 | For every timestamp: timezone, precision, and whether it is execution time or receive time. | Receive time recorded as execution time books a trade in the wrong hour. |
| 10 | Are numeric fields delivered as floats? | If so, precision is already lost upstream and a policy decision is required — not a silent cast. |
| 11 | Enum translations for side, entry and position status, with test vectors. | An unmapped value forwarded downstream makes a consumer branch on something nobody defined. |
| 12 | String encoding for identifiers and symbols. | Silent replacement characters in a key create duplicates that look like distinct records. |
| 13 | Hedging versus netting: what identifies a position, and may two ever be merged? | This connector never coalesces positions; the source's model has to be known before anyone does. |

### Sync strategy

| # | Question | Why it blocks |
|---|---|---|
| 14 | Is there any stable snapshot, cursor or incremental sequence? | Without one the paging model in `scan.py` does not apply and `snapshot_cursor` stays `False`. |
| 15 | Are deal ids immutable, and are reads ordered stably? | Overlapping-window sync needs both; neither can be assumed. |
| 16 | Can a record change after it has been read? | Decides whether reconciliation is periodic or continuous. |

---

## If the answers are unfavourable

The design's fallbacks (section 7.5), usable **only** once verified:

1. **Immutable deal ids within a time range.** Read overlapping windows,
   deduplicate on `(source_id, deal_id)`, track late arrivals, reconcile
   periodically. Costs re-reads; gains no dependence on a cursor.
2. **Point-in-time snapshots without a global cut.** Snapshot each entity
   separately with its own `observed_at`, and say plainly that the view is
   eventually consistent. Do not present it as an atomic broker-wide cut.
3. **No stable ordering or pagination.** Bounded key or time partitions with
   measured overlap, plus reconciliation to catch what moved. If the
   guarantees are not there, the honest answer is `CAPABILITY_UNSUPPORTED` —
   not an invented cursor.

What none of them permits: fabricating a `source_seq` or a `snapshot_seq` and
then claiming a gap-free sync. A synthetic sequence makes the claim
unfalsifiable, which is worse than not making it.

---

## The template a real mapping must fill

```
field: DealDTO.commission
  source_call:        <documented SDK accessor>
  source_type:        <as documented, e.g. double / int64 / struct field>
  units:              <currency minor units? account currency? per lot?>
  scale:              <fixed point? how many places?>
  sign_convention:    <negative = charge to the account? verified how?>
  null_semantics:     <absent vs zero, and which the source means>
  precision_policy:   <exact, or rejected; never silently rounded>
  test_vectors:       <at least one positive, one negative, one zero>
  verified_by:        <document section, ticket, or conformance test id>
```

A row with `verified_by: UNKNOWN` for any money, time or identity field means
the mapping is not ready. Release gate G3 says so, and the scorecard reports
the E-series cases as `NOT_SUPPORTED` with the reason stated — rather than as
untested, which would read as an oversight instead of a decision.

---

## References

- MetaQuotes `mt5manager` distribution: <https://pypi.org/project/mt5manager/>
- MQL5 terminal Python integration (**not** the Manager API):
  <https://www.mql5.com/en/docs/python_metatrader5>
- `drivers/manager_sdk_windows.py` — the guard, and `REQUIRED_EVIDENCE`
- [ADR-211](adr/README.md#adr-211) — an unverified capability is refused
- Design sections 2, 7.5 and 15.5
