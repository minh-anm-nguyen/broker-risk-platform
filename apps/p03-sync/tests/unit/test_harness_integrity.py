"""Tests of the test harness (design group U).

Unusual to test a harness, but the design makes it a release gate, and for
good reason: every correctness claim in this project rests on the harness
telling the truth about what it actually exercised. A fault injector that
silently failed to fire, or a case id that nothing covers quietly vanishing
from the report, would turn the whole suite into decoration.

So the properties asserted here are:

* an injector that did not fire makes the run inconclusive, never a pass;
* a barrier that was not crossed does the same;
* faults and barriers do not leak into the next case;
* a case no test covers is reported ``NOT_TESTED`` and counted against the gate;
* a case id a test claims but the design does not define is surfaced, not
  absorbed as coverage.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from scripts.whatif_report import build_report, parse_design
from tests.fakes.fake_source import FakeSimulatorSource, Fault
from trading_sync.application.killpoints import KILL_POINTS, Barriers, KillPointCrash
from trading_sync.domain.snapshots import Entity

pytestmark = pytest.mark.unit

_DESIGN_RELATIVE = (
    pathlib.Path("docs") / "system-design"
    / "Project_03_Trading_Data_Synchronization_System_Design.md"
)


def _locate_design() -> pathlib.Path:
    """Find the design document by walking up to the repository root.

    Counting ``.parent`` hops breaks the moment the component moves in the
    tree, and it breaks as a confusing FileNotFoundError in four unrelated
    tests rather than as one clear message. Searching upwards survives the
    monorepo layout and whatever comes after it.
    """
    here = pathlib.Path(__file__).resolve()
    for candidate in here.parents:
        design = candidate / _DESIGN_RELATIVE
        if design.is_file():
            return design
    raise FileNotFoundError(
        f"could not locate {_DESIGN_RELATIVE} in any parent of {here}. "
        "These tests read the design document as the authority for the what-if "
        "case list, so they cannot run without it."
    )


DESIGN = _locate_design()


@pytest.mark.whatif("WF3-241")
def test_an_injector_that_never_fired_fails_the_case() -> None:
    """``assert_fired`` is the difference between PASS and NOT_CONCLUSIVE.

    Without it, a test whose fault silently failed to arm would assert the
    happy path and report success -- the most expensive kind of green.
    """
    source = FakeSimulatorSource()
    source.inject(Fault(mode="snapshot_expired", at_page=5, entity=Entity.DEALS))
    with pytest.raises(AssertionError) as caught:
        source.assert_fired("snapshot_expired")
    assert "NOT_CONCLUSIVE" in str(caught.value)


@pytest.mark.whatif("WF3-241")
async def test_a_fault_that_did_fire_is_recorded() -> None:
    source = FakeSimulatorSource(page_size=1)
    source.add_deal("D1")
    source.add_deal("D2")
    source.inject(Fault(mode="timeout", at_page=1, entity=Entity.DEALS))
    pages = source.iter_deals("sim_local", 0)
    await pages.__anext__()
    with pytest.raises(Exception):
        await pages.__anext__()
    source.assert_fired("timeout")
    assert source.fired("timeout") == 1


@pytest.mark.whatif("WF3-241")
def test_a_barrier_that_was_not_crossed_fails_the_case() -> None:
    """Arming a barrier is not evidence that the code reached it.

    A kill point placed in a branch the test never takes would otherwise look
    like a crash-window test that passed.
    """
    barriers = Barriers(enabled=True)
    barriers.arm_crash("K06_after_commit_before_ack")
    with pytest.raises(AssertionError) as caught:
        barriers.assert_crossed("K06_after_commit_before_ack")
    assert "proves nothing" in str(caught.value)


@pytest.mark.whatif("WF3-248")
async def test_barriers_count_crossings_and_clear_between_cases() -> None:
    """A rule left armed would make the next case unreproducible."""
    barriers = Barriers(enabled=True)
    barriers.arm_crash("K05_after_receipt_before_commit")
    with pytest.raises(KillPointCrash):
        await barriers.reach("K05_after_receipt_before_commit")
    barriers.assert_crossed("K05_after_receipt_before_commit")

    barriers.clear()
    assert barriers.crossed("K05_after_receipt_before_commit") == 0
    # Disarmed: reaching it again is a no-op rather than a second crash.
    await barriers.reach("K05_after_receipt_before_commit")


@pytest.mark.whatif("WF3-248")
async def test_clearing_faults_disarms_everything() -> None:
    source = FakeSimulatorSource(page_size=1)
    source.add_deal("D1")
    source.inject(Fault(mode="timeout", at_page=0, entity=Entity.DEALS))
    source.clear_faults()
    pages = [page async for page in source.iter_deals("sim_local", 0)]
    assert len(pages) == 1
    assert source.fired("timeout") == 0


def test_barriers_cannot_be_armed_when_disabled() -> None:
    """Production safety: a barrier armed at runtime would be a way to
    terminate a transaction on demand."""
    barriers = Barriers(enabled=False)
    with pytest.raises(RuntimeError):
        barriers.arm_crash("K06_after_commit_before_ack")


def test_only_known_kill_points_can_be_armed() -> None:
    """A typo would arm nothing and the test would silently pass."""
    barriers = Barriers(enabled=True)
    with pytest.raises(KeyError):
        barriers.arm_crash("K99_imaginary")
    assert len(KILL_POINTS) == 17


@pytest.mark.whatif("WF3-242", "WF3-243")
def test_a_case_no_test_covers_is_reported_not_tested() -> None:
    """Silence is not success.

    A report listing only the cases somebody remembered to write is not a gate.
    Every design case appears, and anything short of PASS counts against it.
    """
    cases = parse_design(DESIGN)
    assert len(cases) == 252, f"expected 252 design cases, parsed {len(cases)}"

    report = build_report(cases, {"cases": [{"case_id": "WF3-001", "status": "PASS"}]})
    statuses = {row["case_id"]: row["status"] for row in report["cases"]}
    assert statuses["WF3-001"] == "PASS"
    assert statuses["WF3-053"] == "NOT_TESTED"
    assert report["p0_gate"]["satisfied"] is False
    assert report["p0_gate"]["p0_total"] == 169


@pytest.mark.whatif("WF3-242")
def test_blocked_environment_does_not_count_as_a_pass() -> None:
    """Skipping because no database was configured is not evidence.

    This is the case that would otherwise make a laptop run look like a
    release-ready result (WF3-247).
    """
    cases = parse_design(DESIGN)
    report = build_report(
        cases, {"cases": [{"case_id": "WF3-053", "status": "BLOCKED_ENVIRONMENT"}]}
    )
    gate = report["p0_gate"]
    assert gate["p0_pass"] == 0
    assert gate["satisfied"] is False


@pytest.mark.whatif("WF3-243")
def test_a_case_id_the_design_does_not_define_is_surfaced() -> None:
    """A typo in a marker would otherwise inflate coverage invisibly."""
    cases = parse_design(DESIGN)
    report = build_report(cases, {"cases": [{"case_id": "WF3-999", "status": "PASS"}]})
    assert report["unknown_case_ids"] == ["WF3-999"]


@pytest.mark.whatif("WF3-250")
def test_the_design_priority_split_is_what_the_report_uses() -> None:
    """The report's own totals must match the design's stated 169/61/22."""
    cases = parse_design(DESIGN)
    counts: dict[str, int] = {}
    for meta in cases.values():
        counts[meta["priority"]] = counts.get(meta["priority"], 0) + 1
    assert counts == {"P0": 169, "P1": 61, "P2": 22}
