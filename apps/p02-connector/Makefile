# Project 02 - MT5 Integration Connector.
#
# PY is the interpreter. Point it at the project venv, or export it:
#   make PY=../.venv/Scripts/python.exe smoke
PY ?= python
PYTEST = PYTHONPATH=src $(PY) -m pytest -p no:cacheprovider

.PHONY: help install smoke test unit contract integration chaos property \
        performance whatif scorecard bench serve probe scan lint clean

help:
	@echo "make smoke        - the per-commit suite (unit + contract + integration)"
	@echo "make test         - everything except the slow lanes"
	@echo "make chaos        - process kill and hard-hang recovery (slow)"
	@echo "make performance  - benchmark profiles B0-B3 and the large scan (slow)"
	@echo "make whatif       - run the suite and emit what_if_results.json"
	@echo "make scorecard    - render scorecard.md from the latest what-if run"
	@echo "make bench        - one benchmark profile with a named fault profile"
	@echo "make serve        - run the health and metrics surface"

install:
	$(PY) -m pip install -e ".[test]"

smoke:
	$(PYTEST) tests/unit tests/contract tests/integration -q -m "not slow"

test:
	$(PYTEST) tests -q -m "not slow"

unit:
	$(PYTEST) tests/unit -q

contract:
	$(PYTEST) tests/contract -q

integration:
	$(PYTEST) tests/integration -q

chaos:
	$(PYTEST) tests/chaos -q

property:
	$(PYTEST) tests/property -q

performance:
	$(PYTEST) tests/performance -q -s

# The full evidence run. --include-slow is what makes the chaos and
# performance cases report PASS rather than NOT_TESTED.
whatif:
	PYTHONPATH=src $(PY) scripts/run_whatif_suite.py --include-slow

scorecard:
	PYTHONPATH=src $(PY) scripts/generate_scorecard.py --quiet

# PROFILE and FAULT_PROFILE are explicit: a run under a fault reported as a
# baseline is the failure WF2-P-11 describes.
PROFILE ?= ALL
FAULT_PROFILE ?= none
bench:
	PYTHONPATH=src $(PY) scripts/run_benchmark.py --profile $(PROFILE) --fault-profile $(FAULT_PROFILE)

serve:
	PYTHONPATH=src $(PY) -m mt5_connector.cli serve

probe:
	PYTHONPATH=src $(PY) -m mt5_connector.cli probe

scan:
	PYTHONPATH=src $(PY) -m mt5_connector.cli scan --entity deals

lint:
	$(PY) -m ruff check src tests scripts

clean:
	rm -rf artifacts .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
