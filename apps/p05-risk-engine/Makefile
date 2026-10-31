PY ?= python
PYTEST := $(PY) -m pytest
export PYTHONPATH := src

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show the available targets
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-22s %s\n", $$1, $$2}'

# --- install ---------------------------------------------------------------

.PHONY: install
install: ## Install the package with its test extra
	$(PY) -m pip install -e ".[test]"

.PHONY: install-postgres
install-postgres: ## Add the optional psycopg dependency
	$(PY) -m pip install -e ".[test,postgres]"

# --- test ------------------------------------------------------------------

.PHONY: test
test: ## Run every suite (the full 360-case matrix)
	$(PYTEST)

.PHONY: smoke
smoke: ## Fast pre-commit suite: arithmetic and contracts only
	$(PYTEST) -m "golden or contract" -q

.PHONY: unit
unit: ## Reviewed arithmetic vectors against the independent oracle
	$(PYTEST) tests/golden -q

.PHONY: contract
contract: ## Upstream handshake, versioning and the Project 06 boundary
	$(PYTEST) tests/contract -q

.PHONY: integration
integration: ## SQL read model, cuts, reconciliation, concurrency
	$(PYTEST) tests/integration -q

.PHONY: property
property: ## Property-based, metamorphic and fuzz suites
	$(PYTEST) tests/property -q

.PHONY: security
security: ## Isolation, authorization, SQL safety and redaction
	$(PYTEST) tests/security -q

.PHONY: chaos
chaos: ## Crash points, recovery contracts and compound faults
	$(PYTEST) tests/chaos -q

.PHONY: performance
performance: ## Load, streaming memory and concurrency measurements
	$(PYTEST) tests/performance -q

.PHONY: p0
p0: ## Only the release-blocking P0 cases
	$(PYTEST) -m p0 -q

# --- evidence and gates ----------------------------------------------------

.PHONY: report
report: ## Matrix coverage report from the recorded evidence
	$(PY) -m risk_tools.report

.PHONY: gate
gate: test ## Run the suite, then enforce the P0 release gate
	$(PY) -m risk_tools.report --gate

.PHONY: uncovered
uncovered: ## List the matrix cases with no execution evidence
	$(PY) -m risk_tools.report --uncovered

# --- benchmark -------------------------------------------------------------

.PHONY: bench
bench: ## On-demand reference workload (correctness gated)
	$(PY) -m risk_tools.benchmark --profile reference-5k --seed 4205 \
		--out artifacts/benchmark-reference-5k.json

.PHONY: bench-100k
bench-100k: ## Section 15.2 streaming workload
	$(PY) -m risk_tools.benchmark --profile reference-100k --seed 4205 \
		--out artifacts/benchmark-reference-100k.json

.PHONY: bench-1m
bench-1m: ## Offline batch workload; minutes, not seconds
	$(PY) -m risk_tools.benchmark --profile offline-1m --seed 4205 \
		--out artifacts/benchmark-offline-1m.json

# --- housekeeping ----------------------------------------------------------

.PHONY: lint
lint: ## Ruff, if it is installed
	@$(PY) -m ruff check src tests 2>/dev/null || \
		echo "ruff not installed; skipping (pip install ruff)"

.PHONY: clean
clean: ## Remove caches and generated artifacts
	rm -rf .pytest_cache .hypothesis
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -f artifacts/test_matrix_results.json artifacts/benchmark-*.json
