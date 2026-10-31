# Project 08 - ClickHouse Broker Analytics
#
# The test targets are split the way the design splits its evidence: `smoke`
# is what runs on every commit, `test` is the full offline suite, and `gates`
# prints the honest scorecard including the claims this repository does NOT
# support.

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
PYTEST_FLAGS ?= -q -p no:cacheprovider

.DEFAULT_GOAL := help
.PHONY: help install install-infra demo demo-100k test smoke unit component contract \
        integration chaos property whatif perf perf-slow gates gates-run evidence \
        fixtures killpoints invariants lint clean docker-up docker-down migrate

help: ## Show the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install: ## Install the package and its test extras (no infrastructure drivers)
	$(PYTHON) -m pip install -e ".[test]"

install-infra: ## Additionally install the real ClickHouse/Kafka/PostgreSQL drivers
	$(PYTHON) -m pip install -e ".[test,infra]"

# ---------------------------------------------------------------------------
# The demo
# ---------------------------------------------------------------------------

demo: ## Run the end-to-end vertical slice of design section 0.1
	$(PYTHON) -m analytics.cli demo

demo-100k: ## Run the demo over a 100k-Deal corpus
	$(PYTHON) -m analytics.cli demo --deals 100000 --show 5

# ---------------------------------------------------------------------------
# Tests. None of these need Kafka, ClickHouse or PostgreSQL: the
# deterministic adapters model them, which is what makes the chaos suite
# repeatable (see src/analytics/ports/__init__.py).
# ---------------------------------------------------------------------------

smoke: ## Per-commit suite: everything except the slow tier
	$(PYTEST) tests $(PYTEST_FLAGS) -m "not slow"

test: ## Full offline suite, including the slow tier
	$(PYTEST) tests $(PYTEST_FLAGS)

unit: ## Parsers, money, canonical encoding, policy, dedup, rollups, oracles
	$(PYTEST) tests/unit $(PYTEST_FLAGS)

component: ## The Kafka, ClickHouse and control-plane adapters' own contracts
	$(PYTEST) tests/component $(PYTEST_FLAGS)

contract: ## The read-only HTTP contract and scope isolation
	$(PYTEST) tests/contract $(PYTEST_FLAGS)

integration: ## The 18 acceptance scenarios of section 11.1
	$(PYTEST) tests/integration $(PYTEST_FLAGS)

chaos: ## 28 kill points and 12 compound campaigns
	$(PYTEST) tests/chaos $(PYTEST_FLAGS)

property: ## Metamorphic properties and the stateful model
	$(PYTEST) tests/property $(PYTEST_FLAGS)

whatif: ## Coverage across the 44 failure domains
	$(PYTEST) tests/whatif $(PYTEST_FLAGS)

perf: ## Benchmarks, excluding the 100k tier
	$(PYTEST) tests/performance $(PYTEST_FLAGS) -m "not slow" -s

perf-slow: ## Benchmarks including the 100k-Deal build
	$(PYTEST) tests/performance $(PYTEST_FLAGS) -s

# ---------------------------------------------------------------------------
# Evidence and gates
# ---------------------------------------------------------------------------

gates: ## Print the release-gate scorecard and the declared gaps
	$(PYTHON) scripts/release_gates.py

gates-run: ## Scorecard plus an execution of every suite
	$(PYTHON) scripts/release_gates.py --run

evidence: ## Collect evidence bundles for every scenario (section 11.3)
	$(PYTHON) scripts/collect_evidence.py --all

fixtures: ## List the 20 golden fixtures and their expected outcomes
	$(PYTHON) -m analytics.cli fixtures

killpoints: ## List the 28 deterministic injection sites
	$(PYTHON) -m analytics.cli killpoints

invariants: ## List the invariant register and any unproven entries
	$(PYTHON) -m analytics.cli invariants

lint: ## Ruff, if installed
	@$(PYTHON) -m ruff check src tests scripts || \
	  echo "ruff is not installed; skipping (pip install ruff)"

clean: ## Remove caches and generated artifacts
	rm -rf .pytest_cache .hypothesis .ruff_cache artifacts/p08 data
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

# ---------------------------------------------------------------------------
# Real infrastructure. Everything below needs Docker, and nothing in the
# offline suite depends on it. Release gates G1 and G2 stay open until a run
# against these services is recorded.
# ---------------------------------------------------------------------------

docker-up: ## Start ClickHouse, Kafka and PostgreSQL
	docker compose up -d

docker-down: ## Stop them and remove the volumes
	docker compose down -v

migrate: ## Apply both schemas to the running services
	docker compose exec -T clickhouse clickhouse-client --multiquery \
	  < database/clickhouse/migrations/0001_init.sql
	docker compose exec -T postgres psql -U postgres -d broker_platform \
	  -f /migrations/0001_control_plane.sql
