# Broker Risk Platform - monorepo entry point.
#
# Layout and gate names follow Project 09 sections 17 and 19. Each component
# keeps its own Makefile; this one composes them and owns the cross-component
# gates (contract freeze, full test sweep, T1 topology).
#
# Python: components declare >=3.12. The bundled venv is 3.14.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Windows and POSIX disagree on both the venv layout and the PYTHONPATH
# separator. Getting the separator wrong is silent: Python treats
# "src:/other/path" as one nonexistent directory, every import fails, and the
# suite looks like it collected nothing rather than like it was misconfigured.
ifeq ($(OS),Windows_NT)
  VENV_PY := .venv/Scripts/python.exe
  PATHSEP := ;
else
  VENV_PY := .venv/bin/python
  PATHSEP := :
endif

# Absolute, because every test target runs from inside a component directory.
# Falls back to whatever python is on PATH when the bundled venv is absent,
# which is the case on a CI runner.
PY := $(shell test -x "$(VENV_PY)" && echo "$(CURDIR)/$(VENV_PY)" || echo python)

COMPOSE := docker compose -f infrastructure/compose.test.yml
# Both shared packages, for every component. P06 imports risk_engine; P03, P07
# and P08 import platform_events. Leaving one out does not fail at collection -
# it fails inside the suite, which reads as the component being broken rather
# than the path being short.
RISK_ENGINE_SRC := $(CURDIR)/packages/risk-engine/src
PLATFORM_EVENTS_SRC := $(CURDIR)/packages/platform-events/src
P07_SRC := $(CURDIR)/apps/p07-outbox-relay/src
PYPATH := src$(PATHSEP)$(RISK_ENGINE_SRC)$(PATHSEP)$(PLATFORM_EVENTS_SRC)$(PATHSEP)$(P07_SRC)

# Every component with a tests/ directory, in dependency order.
COMPONENTS := \
	apps/p01-simulator \
	apps/p02-connector \
	apps/p03-sync \
	apps/p04-reconciliation \
	apps/p05-risk-engine \
	apps/p06-api \
	apps/p07-outbox-relay \
	apps/p08-analytics \
	packages/platform-events \
	packages/risk-engine \
	test-platform

# Excluded from the default sweep: these take minutes and need a declared
# resource envelope to mean anything (P09 section 12.1).
FAST_MARKERS := not slow and not performance

.PHONY: help
help:
	@echo "Broker Risk Platform (P01-P08 plus the P09 test platform)"
	@echo
	@echo "  make contracts         Contract freeze gate - detects drift between the"
	@echo "                         manifest and the code (P09 T09-01)"
	@echo "  make contracts-strict  Same, but an open contract gap fails. Use at G1."
	@echo "  make test              Fast test sweep across every component"
	@echo "  make oracle            Independent oracle against a live platform"
	@echo "  make test-integrated   Same sweep with real PostgreSQL - see"
	@echo "                         infrastructure/local-postgres.md"
	@echo "  make test-one C=apps/p03-sync   One component's fast suite"
	@echo "  make test-slow         Include slow and performance suites"
	@echo "  make up / make down    T1 integration topology (needs Docker)"
	@echo "  make ps / make logs    Inspect the running topology"
	@echo "  make clean             Remove caches and build artifacts"
	@echo
	@echo "Gates G0-G5 are defined in docs/system-design/Project_09_*.md section 19."
	@echo "No gate is claimed by this Makefile; it only runs the checks."

# ---------------------------------------------------------------- contracts ---

.PHONY: contracts
contracts:
	@$(PY) test-platform/runner/contracts.py

.PHONY: contracts-strict
contracts-strict:
	@$(PY) test-platform/runner/contracts.py --strict-gaps

# --------------------------------------------------------------------- tests ---

# Each component is run from its own directory so its pyproject settings
# (markers, asyncio mode, filterwarnings) apply. The risk-engine source is on
# the path throughout because P06 imports it.
# pytest exit 5 means "collected nothing". That is NOT_RUN, and P09 section
# 19.3 is explicit that NOT_RUN must never be counted as a pass, so it is
# reported on its own line instead of disappearing into a green summary.
.PHONY: test
test:
	@failed=""; notrun=""; \
	for c in $(COMPONENTS); do \
		if [ ! -d "$$c/tests" ]; then notrun="$$notrun $${c}:no-tests-dir"; continue; fi; \
		echo "--- $$c"; \
		( cd "$$c" && PYTHONPATH="$(PYPATH)" "$(PY)" -m pytest \
			-q --no-header -m "$(FAST_MARKERS)" ); \
		rc=$$?; \
		if [ $$rc -eq 5 ]; then notrun="$$notrun $${c}:empty-suite"; \
		elif [ $$rc -ne 0 ]; then failed="$$failed $$c"; fi; \
	done; \
	echo; \
	if [ -n "$$notrun" ]; then echo "NOT_RUN (no coverage, not a pass):$$notrun"; fi; \
	if [ -n "$$failed" ]; then echo "FAILED:$$failed"; exit 1; fi; \
	echo "every component suite that exists passed"

.PHONY: test-one
test-one:
	@test -n "$(C)" || { echo "usage: make test-one C=apps/p03-sync"; exit 2; }
	@cd "$(C)" && PYTHONPATH="$(PYPATH)" "$(PY)" -m pytest \
		-q --no-header -m "$(FAST_MARKERS)"

.PHONY: test-slow
test-slow:
	@for c in $(COMPONENTS); do \
		[ -d "$$c/tests" ] || continue; \
		echo "--- $$c (including slow)"; \
		( cd "$$c" && PYTHONPATH="$(PYPATH)" "$(PY)" -m pytest \
			-q --no-header ) || exit 1; \
	done

# -------------------------------------------------------------- integrated ---

# The same sweep, with the PostgreSQL-backed cases actually running instead of
# reporting BLOCKED_ENVIRONMENT. See infrastructure/local-postgres.md for the
# databases, the two roles and why they are separate. SYNC_DATABASE_URL is
# deliberately NOT set: P03 has a case that asserts the loud failure when it is
# absent, and setting it would fail that case for the wrong reason.
PG_HOST ?= 127.0.0.1
PG_PORT ?= 55432

.PHONY: test-integrated
test-integrated:
	@command -v pg_isready >/dev/null 2>&1 && pg_isready -h $(PG_HOST) -p $(PG_PORT) -q \
		|| echo "note: could not confirm PostgreSQL on $(PG_HOST):$(PG_PORT); DB-backed cases will report BLOCKED_ENVIRONMENT"
	@SYNC_TEST_DATABASE_URL="postgresql+asyncpg://postgres@$(PG_HOST):$(PG_PORT)/trading_sync_test" \
		OUTBOX_PG_DSN="postgresql://postgres@$(PG_HOST):$(PG_PORT)/broker_platform" \
		RECON_TARGET_PG_DSN="postgresql://postgres@$(PG_HOST):$(PG_PORT)/broker_risk" \
		RISK_PG_DSN="postgresql://risk_reader@$(PG_HOST):$(PG_PORT)/broker_risk" \
		$(MAKE) --no-print-directory test
# ----------------------------------------------------------------- oracle ---

# The independent oracle (T09-07, T09-08) against a live platform. Read-only:
# every call it makes is a GET or a SELECT. It imports no component package,
# which is the point - an oracle sharing a library with what it checks cannot
# detect a wrong rounding rule or a missing translation.
ORACLE_SOURCE ?= http://127.0.0.1:8001
ORACLE_DSN    ?= postgresql://postgres@127.0.0.1:55432/broker_risk

.PHONY: oracle
oracle:
	@$(PY) test-platform/oracle/check.py \
		--source-url "$(ORACLE_SOURCE)" --dsn "$(ORACLE_DSN)"
# ---------------------------------------------------------------- topology ---

.PHONY: up
up:
	$(COMPOSE) up -d --build
	@echo
	@echo "Started. Service readiness is NOT data readiness: a healthy container"
	@echo "does not mean a registered source epoch, a migrated schema or a"
	@echo "reachable topic (P09 section 4.2). The runner asserts that separately."

.PHONY: down
down:
	$(COMPOSE) down -v

.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: logs
logs:
	$(COMPOSE) logs -f --tail=100

.PHONY: config
config:
	$(COMPOSE) config --quiet && echo "compose.test.yml is valid"

# ------------------------------------------------------------------- hygiene ---

.PHONY: clean
clean:
	@find . -path ./.venv -prune -o -name '__pycache__' -type d -print0 2>/dev/null \
		| xargs -0 rm -rf 2>/dev/null || true
	@find . -path ./.venv -prune -o -name '*.egg-info' -type d -print0 2>/dev/null \
		| xargs -0 rm -rf 2>/dev/null || true
	@find . -path ./.venv -prune -o -name '.pytest_cache' -type d -print0 2>/dev/null \
		| xargs -0 rm -rf 2>/dev/null || true
	@echo "caches removed (sources, artifacts/ and .hypothesis/ databases untouched)"
