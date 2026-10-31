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
PYPATH := src$(PATHSEP)$(RISK_ENGINE_SRC)$(PATHSEP)$(PLATFORM_EVENTS_SRC)

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
	packages/risk-engine

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
