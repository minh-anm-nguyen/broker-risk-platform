# Project 04 - Snapshot Reconciliation & Recovery
PY ?= python
# src layout without an install step; mirrors pytest's pythonpath setting.
export PYTHONPATH := src

.PHONY: help smoke test unit property security contract integration chaos perf campaign gates lint all

help:
	@echo "smoke       - the per-commit suite (fast, no slow markers)"
	@echo "test        - everything except performance"
	@echo "unit        - canonical, policy, config"
	@echo "property    - the 12 metamorphic rules (section 10.4)"
	@echo "security    - privilege, redaction, adversarial input"
	@echo "contract    - P01 wire semantics and the P03 ports"
	@echo "integration - full audit/repair lifecycle against live P01"
	@echo "chaos       - the 14 crash windows (section 7.6)"
	@echo "perf        - measured throughput; records, does not assert targets"
	@echo "campaign    - the what-if matrix, with honest NOT_RUN reporting"
	@echo "gates       - verify G0/G1 release invariants over a database"

# The commit gate: everything that is fast enough to run on every change.
smoke:
	$(PY) -m pytest -q -m "not slow"

test:
	$(PY) -m pytest -q -m "not performance"

unit:
	$(PY) -m pytest tests/unit -q

property:
	$(PY) -m pytest tests/property_based -q

security:
	$(PY) -m pytest tests/security -q

contract:
	$(PY) -m pytest tests/contract -q

integration:
	$(PY) -m pytest tests/integration -q

chaos:
	$(PY) -m pytest tests/chaos -q

perf:
	$(PY) -m pytest tests/performance -q

campaign:
	$(PY) -m reconciliation.cli campaign --tier P0 --evidence-dir ./artifacts

# A violation here is a correctness defect, not a flaky test.
gates:
	$(PY) scripts/verify_release_gates.py --db ./data/recon.db

lint:
	-ruff check src tests scripts
	-ruff format --check src tests scripts

all: smoke contract integration chaos gates
