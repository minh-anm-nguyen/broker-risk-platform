"""The evidence pipeline itself: status classification and report honesty.

Covers WF2-P-03, WF2-P-11 and WF2-R-11.

These test the thing that decides whether every other test counts. A runner
that silently reports an unexercised case as a pass, or a benchmark report
that cannot distinguish a baseline from a chaos run, would make the whole
suite's output untrustworthy - so the runner gets tests of its own.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def load_script(name: str):
    """Import a script by path.

    Registered in ``sys.modules`` before execution because ``@dataclass``
    resolves annotations through ``sys.modules[cls.__module__]``, and a module
    that is not there yet makes the decorator fail.
    """
    import sys

    module_name = f"_script_{name}"
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def whatif():
    return load_script("run_whatif_suite")


@pytest.fixture(scope="module")
def scorecard():
    return load_script("generate_scorecard")


@pytest.fixture(scope="module")
def benchmark():
    return load_script("run_benchmark")


class TestCatalogue:
    def test_the_catalogue_is_read_from_the_design_document(self, whatif) -> None:
        """A case added to the design and not tested must surface, not vanish."""
        if not whatif.DEFAULT_DESIGN.exists():
            pytest.skip(f"design document not present at {whatif.DEFAULT_DESIGN}")
        catalogue = whatif.load_catalogue(whatif.DEFAULT_DESIGN)
        # The design states 216 cases: P0 130, P1 67, P2 19.
        assert len(catalogue) == 216
        counts = {"P0": 0, "P1": 0, "P2": 0}
        for record in catalogue.values():
            counts[record.priority] += 1
        assert counts == {"P0": 130, "P1": 67, "P2": 19}

    def test_every_case_starts_as_not_tested(self, whatif) -> None:
        """WF2-P-03: a pass has to be earned, never defaulted to."""
        if not whatif.DEFAULT_DESIGN.exists():
            pytest.skip("design document not present")
        catalogue = whatif.load_catalogue(whatif.DEFAULT_DESIGN)
        assert {record.status for record in catalogue.values()} == {"NOT_TESTED"}

    def test_a_missing_design_document_is_a_hard_failure(self, whatif, tmp_path) -> None:
        with pytest.raises(SystemExit):
            whatif.load_catalogue(tmp_path / "nope.md")

    def test_the_statuses_are_exactly_the_five_the_design_allows(self, whatif) -> None:
        assert set(whatif.ALLOWED_STATUSES) == {
            "PASS",
            "FAIL",
            "NOT_TESTED",
            "NOT_SUPPORTED",
            "NOT_CONCLUSIVE",
        }

    def test_every_not_supported_entry_carries_a_reason(self, whatif) -> None:
        """A bare exclusion would read as a pass by omission."""
        assert whatif.NOT_SUPPORTED
        for case_id, reason in whatif.NOT_SUPPORTED.items():
            assert whatif.CASE_PATTERN.fullmatch(case_id), case_id
            assert len(reason) > 20, f"{case_id} needs a stated reason"
            assert "need" in reason


class TestCaseToTestMapping:
    def test_a_function_level_citation_wins_over_a_file_header(
        self, whatif, tmp_path
    ) -> None:
        """One inconclusive test must not drag down its whole file's cases."""
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "test_sample.py").write_text(
            '"""Covers WF2-A-01, WF2-A-02."""\n'
            "\n"
            "def test_one():\n"
            '    """WF2-A-01: specific."""\n'
            "    pass\n"
            "\n"
            "def test_two():\n"
            "    pass\n",
            encoding="utf-8",
        )
        mapping = whatif.map_tests_to_cases(tests_dir)
        # A-01 is claimed precisely, so only that test counts for it.
        assert len(mapping["WF2-A-01"]) == 1
        assert next(iter(mapping["WF2-A-01"])).endswith("::test_one")
        # A-02 appears only in the header, so it falls back to the whole file.
        assert len(mapping["WF2-A-02"]) == 2

    def test_a_file_without_any_citation_is_ignored(self, whatif, tmp_path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "test_plain.py").write_text(
            "def test_nothing():\n    pass\n", encoding="utf-8"
        )
        assert whatif.map_tests_to_cases(tests_dir) == {}

    def test_the_real_suite_cites_every_p0_case(self, whatif) -> None:
        """Not a coverage claim - a claim that nothing is silently unclaimed."""
        if not whatif.DEFAULT_DESIGN.exists():
            pytest.skip("design document not present")
        catalogue = whatif.load_catalogue(whatif.DEFAULT_DESIGN)
        mapping = whatif.map_tests_to_cases(ROOT / "tests")
        unclaimed = [
            case_id
            for case_id, record in catalogue.items()
            if record.priority == "P0"
            and case_id not in mapping
            and case_id not in whatif.NOT_SUPPORTED
        ]
        assert unclaimed == [], (
            "every P0 case must either be cited by a test or listed as "
            f"NOT_SUPPORTED with a reason; unclaimed: {unclaimed}"
        )


class TestScorecard:
    @staticmethod
    def _results() -> dict:
        def case(case_id: str, priority: str, status: str) -> dict:
            return {
                "test_case_id": case_id,
                "priority": priority,
                "status": status,
                "reason_if_not_pass": None if status == "PASS" else "a stated reason",
            }

        # The ids are assembled rather than written as literals. This file is
        # about the citation scanner, and a literal case id in fixture *data*
        # would be picked up as this test citing that case - which would make
        # a case look inconclusive because a scorecard test used it as an
        # example.
        def cid(series: str, number: int) -> str:
            return f"WF2-{series}-{number:02d}"

        return {
            "test_run_id": "run-test",
            "pytest_exit_code": 0,
            "cases": [
                case(cid("A", 1), "P0", "PASS"),
                case(cid("C", 1), "P0", "NOT_SUPPORTED"),
                case(cid("E", 1), "P0", "NOT_SUPPORTED"),
                case(cid("F", 1), "P0", "NOT_CONCLUSIVE"),
                case(cid("K", 1), "P0", "PASS"),
                case(cid("Q", 1), "P0", "PASS"),
                case(cid("R", 11), "P2", "NOT_TESTED"),
            ],
        }

    def test_the_report_keeps_the_five_statuses_separate(self, scorecard) -> None:
        """Section 20: a percentage would hide the distinction that matters."""
        text = scorecard.render(self._results(), {"test_run_id": "run-test"})
        for status in ("PASS", "NOT_SUPPORTED", "NOT_CONCLUSIVE", "NOT_TESTED"):
            assert status in text
        assert "None of the three is a pass." in text

    def test_an_inconclusive_p0_case_fails_the_resilience_gate(self, scorecard) -> None:
        text = scorecard.render(self._results(), {})
        gate_line = next(line for line in text.splitlines() if line.startswith("| G1 "))
        assert "NOT MET" in gate_line

    def test_the_native_gate_states_what_blocks_it(self, scorecard) -> None:
        text = scorecard.render(self._results(), {})
        assert "G3 is blocked on" in text
        assert "authorised Manager SDK" in text

    def test_the_combination_axes_are_reported_with_coverage(self, scorecard) -> None:
        """WF2-R-11: an unexercised axis must be visible, not absent."""
        text = scorecard.render(self._results(), {})
        assert "Fault-combination axes" in text
        for axis in scorecard.COMBINATION_AXES:
            assert f"| {axis} |" in text
        # The network axis has only a NOT_SUPPORTED case, so it reports none.
        network_line = next(
            line for line in text.splitlines() if line.startswith("| network ")
        )
        assert "none" in network_line

    def test_the_report_states_what_it_does_not_claim(self, scorecard) -> None:
        text = scorecard.render(self._results(), {})
        assert "What this scorecard does not claim" in text
        assert "contract double" in text
        assert "not SLOs" in text

    def test_rendering_a_run_from_disk(self, scorecard, tmp_path) -> None:
        run_dir = tmp_path / "run-1"
        run_dir.mkdir()
        (run_dir / "what_if_results.json").write_text(
            json.dumps(self._results()), encoding="utf-8"
        )
        (run_dir / "manifest.json").write_text(
            json.dumps({"test_run_id": "run-1", "python": "3.12.0"}), encoding="utf-8"
        )
        assert scorecard.main(["--from", str(tmp_path), "--quiet"]) == 0
        rendered = (run_dir / "scorecard.md").read_text(encoding="utf-8")
        assert "3.12.0" in rendered

    def test_no_results_is_a_hard_failure(self, scorecard, tmp_path) -> None:
        with pytest.raises(SystemExit):
            scorecard.main(["--from", str(tmp_path)])


class TestBenchmarkReport:
    def test_a_fault_profile_must_be_named(self, benchmark) -> None:
        """WF2-P-11: a run under an unnamed fault is uninterpretable."""
        with pytest.raises(SystemExit) as excinfo:
            benchmark.main(["--fault-profile", "something-undocumented"])
        assert "fault-profile" in str(excinfo.value)

    def test_the_baseline_flag_is_derived_not_asserted(self, benchmark) -> None:
        """A report cannot claim to be a baseline while naming a fault."""
        assert "none" in benchmark.KNOWN_FAULT_PROFILES
        assert len(benchmark.KNOWN_FAULT_PROFILES) > 1
        source = (ROOT / "scripts" / "run_benchmark.py").read_text(encoding="utf-8")
        assert '"is_baseline": args.fault_profile == "none"' in source

    def test_every_design_profile_has_a_selector(self, benchmark) -> None:
        for name in ("B0", "B1", "B2", "B3", "LARGE"):
            assert name in benchmark.PROFILES
