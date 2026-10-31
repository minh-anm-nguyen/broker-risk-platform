# MT5 Broker Simulator - developer entry points.
#
# The test targets mirror the suites in SYSTEM_DESIGN section 10.5, so "which
# suite gates what" is visible here rather than only in CI configuration.

PYTHON ?= $(shell test -x ../.venv/Scripts/python.exe && echo ../.venv/Scripts/python.exe \
            || (test -x .venv/bin/python && echo .venv/bin/python) || echo python)
PYTEST := $(PYTHON) -m pytest

.DEFAULT_GOAL := help
.PHONY: help install migrate serve generate verify digest evidence \
        test test-all unit-smoke p0-integration contract property chaos \
        baseline-benchmark stress-soak lint openapi up down logs clean

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the package and test extras
	$(PYTHON) -m pip install -e '.[test]'

# ---- Running ---------------------------------------------------------------

migrate: ## Create or verify the database schema
	$(PYTHON) -m simulator.cli migrate

serve: ## Run the HTTP service (single worker, by design)
	$(PYTHON) -m simulator.cli serve

generate: ## Seed a dataset: make generate SCENARIO=scenarios/baseline_100k.json
	$(PYTHON) -m simulator.cli generate --scenario $(or $(SCENARIO),scenarios/baseline_100k.json) $(ARGS)

verify: ## Run the storage-scope invariant suite (exits non-zero on failure)
	$(PYTHON) scripts/verify_invariants.py $(ARGS)

digest: ## Print the canonical content digest of the active run
	$(PYTHON) -m simulator.cli digest

evidence: ## Collect the evidence bundle into artifacts/<run_id>/
	$(PYTHON) scripts/collect_evidence.py $(ARGS)

# ---- Test suites (section 10.5) --------------------------------------------

unit-smoke: ## Every commit. Blocks merge on failure.
	$(PYTEST) tests/unit -q

contract: ## Every commit. HTTP contract and error envelope.
	$(PYTEST) tests/contract -q

p0-integration: ## Every PR. Blocks merge on failure.
	$(PYTEST) tests/integration -q

property: ## Every PR. Model-based and metamorphic properties.
	$(PYTEST) tests/property -q

chaos: ## Nightly / pre-release. Crash boundaries and storage faults.
	$(PYTEST) tests/chaos -q

baseline-benchmark: ## Pre-release. Measures and records; see scripts/run_benchmark.sh.
	bash scripts/run_benchmark.sh --profile smoke

stress-soak: ## Pre-release (P1). 1M deals; never published as a baseline result.
	bash scripts/run_benchmark.sh --profile stress-1m

test: ## Everything except the slow suites
	$(PYTEST) tests -q -m 'not slow'

test-all: ## Everything, including chaos and benchmarks
	$(PYTEST) tests -q

lint: ## Byte-compile every module as a syntax and import check
	$(PYTHON) -m compileall -q src tests scripts

openapi: ## Write the OpenAPI schema to artifacts/openapi.json
	@mkdir -p artifacts
	$(PYTHON) -c "import json, pathlib, sys; sys.path.insert(0, 'src'); \
	from simulator.main import create_app; \
	pathlib.Path('artifacts/openapi.json').write_text(json.dumps(create_app(lock_process=False).openapi(), indent=2), encoding='utf-8'); \
	print('artifacts/openapi.json')"

# ---- Docker ----------------------------------------------------------------

up: ## Start the container stack (requires the three SIM_* secrets)
	docker compose up --build -d
	@echo "service on http://127.0.0.1:8001 - readiness: /health/ready"

down: ## Stop the stack, keeping the data volume
	docker compose down

logs: ## Follow container logs
	docker compose logs -f simulator

clean: ## Remove local datasets, artefacts and caches (not the Docker volume)
	rm -rf data artifacts .pytest_cache .hypothesis .ruff_cache
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
