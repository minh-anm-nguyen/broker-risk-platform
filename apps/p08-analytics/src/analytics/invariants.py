"""The 24 invariants of section 4, with the test that proves each one.

This register exists so the claim "INV08-xx holds" is checkable rather than
asserted. ``p08 invariants`` prints it, the release-gate script reads it, and
a test asserts the register is complete and that no entry claims a proof it
does not have.

An entry whose ``proof`` begins with ``NOT PROVEN`` is a declared gap. The
design is explicit that an untested guarantee must not be claimed (section
18), so a gap here is visible rather than quietly absent.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Invariant:
    invariant_id: str
    statement: str
    proof: str

    @property
    def proven(self) -> bool:
        return not self.proof.startswith("NOT PROVEN")


INVARIANTS: tuple[Invariant, ...] = (
    Invariant(
        "INV08-01",
        "Kafka redelivery changes the number of raw observations, not canonical Deal effects",
        "property.test_metamorphic.TestDuplicateInvariance; integration.IT08-02",
    ),
    Invariant(
        "INV08-02",
        "The same event_id with a different payload hash is quarantined, never resolved by choice",
        "unit.test_dedup_rollup_oracle.TestConflictScan; integration.IT08-03",
    ),
    Invariant(
        "INV08-03",
        "The same Deal identity with inconsistent content hashes is quarantined",
        "unit.test_dedup_rollup_oracle.TestConflictScan; integration.IT08-04",
    ),
    Invariant(
        "INV08-04",
        "A distinct epoch may reuse a Deal id without collision",
        "property.test_metamorphic.TestScopeSeparation; integration.IT08-13",
    ),
    Invariant(
        "INV08-05",
        "A build contains exactly one row for every Deal in the proven cut",
        "unit.test_dedup_rollup_oracle.TestIdentityOracle; integration.IT08-07; "
        "property.test_stateful invariant",
    ),
    Invariant(
        "INV08-06",
        "Report sums equal the canonical fact sums per currency and policy",
        "unit.test_dedup_rollup_oracle.TestMoneyOracle; property.test_metamorphic",
    ),
    Invariant(
        "INV08-07",
        "No cross-currency arithmetic is disguised as a common unit",
        "unit.test_money.TestCurrencyBuckets; integration.IT08-14",
    ),
    Invariant(
        "INV08-08",
        "Snapshots and Deal cuts are never silently combined across incompatible versions",
        "whatif.TestDomainsNAndO; whatif.TestDomainsXAndY.test_x04",
    ),
    Invariant(
        "INV08-09",
        "PUBLISHED follows only from VERIFIED with complete proof and visible rows",
        "component.test_control_plane.TestPublication; integration.IT08-18; "
        "property.test_stateful invariant",
    ),
    Invariant(
        "INV08-10",
        "An abandoned or incomplete build is never queryable as current",
        "chaos.test_kill_points KP08-16/17/19/20; property.test_stateful invariant",
    ),
    Invariant(
        "INV08-11",
        "Losing an active lease cannot overwrite the publication pointer",
        "component.test_control_plane.TestLeases; component.test_kafka_adapter."
        "TestGenerationFencing; integration.IT08-12",
    ),
    Invariant(
        "INV08-12",
        "A Kafka offset is committed only after an eligible insert ACK or a durable quarantine",
        "chaos.test_kill_points KP08-03/04/05; integration.IT08-05/IT08-06",
    ),
    Invariant(
        "INV08-13",
        "A missing retention horizon triggers rebootstrap or INCOMPLETE, never a silent skip",
        "component.test_kafka_adapter.TestRetention; chaos.CC08-04; whatif.TestDomainsACADAM",
    ),
    Invariant(
        "INV08-14",
        "The event-time period is the validated UTC executed_at, not the ingestion timestamp",
        "unit.test_canonical.TestTradeDay; whatif.TestDomainsPAndV; integration.IT08-08",
    ),
    Invariant(
        "INV08-15",
        "A missing or unsupported money, fee or FX policy cannot produce a monetary READY",
        "unit.test_policy.TestFeeCoverage; whatif.TestDomainsNAndO.test_n02",
    ),
    Invariant(
        "INV08-16",
        "AccountSnapshotPublished is not interpreted as a complete Account fact",
        "integration.IT08-09; whatif.TestDomainsNAndO.test_o01; contract.test_http_api",
    ),
    Invariant(
        "INV08-17",
        "Every reader query binds tenant, source, epoch and publication",
        "contract.test_http_api.TestScopeIsolation; whatif.TestDomainB; chaos.CC08-08/09",
    ),
    Invariant(
        "INV08-18",
        "A rollback restores an earlier verified pointer without mutating history",
        "component.test_control_plane.TestRollback; chaos.CC08-11",
    ),
    Invariant(
        "INV08-19",
        "Retention and GC never delete the current published build or required audit source",
        "chaos.test_kill_points KP08-28; whatif.TestDomainsACADAM; "
        "property.test_stateful invariant",
    ),
    Invariant(
        "INV08-20",
        "Resource use stays within declared budgets or fails in a controlled way",
        "component.test_clickhouse_adapter.TestResourceBounds; integration.IT08-17; "
        "performance.test_benchmarks.TestResourceBounds",
    ),
    Invariant(
        "INV08-21",
        "Ingestion does not hide permanent poison behind a Kafka lag of zero",
        "integration.IT08-06; whatif.TestDomainK; chaos.CC08-02",
    ),
    Invariant(
        "INV08-22",
        "A rebuild on an identical pinned cut and policy yields an identical canonical hash",
        "property.test_metamorphic.TestRebuildDeterminism; whatif.TestDomainJ.test_j03",
    ),
    Invariant(
        "INV08-23",
        "Raw, event and projection counts are distinguished; physical duplicates are expected",
        "component.test_clickhouse_adapter.TestFactCounts; "
        "whatif.TestDomainsALANAQ.test_an03",
    ),
    Invariant(
        "INV08-24",
        "A production replica claim follows only from tested cross-replica visibility/restore",
        "NOT PROVEN - ReplicatedMergeTree/Keeper failover is P2 and needs a real "
        "multi-replica cluster (design section 0, point 12; what-if domain AF). "
        "Single-node read visibility is proven by integration.IT08-18.",
    ),
)

INVARIANTS_BY_ID = {invariant.invariant_id: invariant for invariant in INVARIANTS}


def unproven() -> tuple[Invariant, ...]:
    """Invariants this repository does not prove. Declared, not hidden."""
    return tuple(invariant for invariant in INVARIANTS if not invariant.proven)
