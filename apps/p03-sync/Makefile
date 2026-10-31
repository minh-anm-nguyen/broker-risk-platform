# Project 03 - Trading Data Synchronization
#
# The database-backed targets need SYNC_TEST_DATABASE_URL. Without it those
# suites skip and their what-if cases report BLOCKED_ENVIRONMENT rather than
# PASS, so a green run on a machine with no database is not a satisfied gate.

PY ?= python
PYTHONPATH_VALUE ?= src;.
PYTEST ?= PYTHONPATH="$(PYTHONPATH_VALUE)" $(PY) -m pytest

.PHONY: help install migrate test test-unit test-p0 test-slow bench soak report gate lint clean-artifacts

help:
	@echo "install          install the package with its dev extras"
	@echo "migrate          alembic upgrade head against SYNC_DATABASE_URL"
	@echo "test             the whole suite"
	@echo "test-unit        pure-unit only; no database required"
	@echo "test-p0          the suites behind the P0 release gate"
	@echo "bench            performance measurements into artifacts/"
	@echo "soak             60-minute soak (WF3-210); shorter runs do not certify it"
	@echo "report           merge results with the design matrix"
	@echo "gate             report, and fail unless every P0 case passes"

install:
	$(PY) -m pip install -e ".[dev]"

migrate:
	PYTHONPATH=src $(PY) -m alembic upgrade head

test:
	$(PYTEST) -q

test-unit:
	$(PYTEST) -q -m unit

# Correctness, checkpoint, atomic publication, epoch safety, concurrency and
# DB resilience: gates G1-G8 of design 22.2.
test-p0:
	$(PYTEST) -q -m "unit or integration or crash or e2e or property"

test-slow:
	$(PYTEST) -q -m slow

bench:
	$(PYTEST) -q -m performance

# Anything shorter than an hour is a smoke run; the test refuses to certify it.
soak:
	SYNC_SOAK_SECONDS=3600 $(PYTEST) -q -m slow tests/performance/test_soak.py

report:
	$(PY) scripts/whatif_report.py

gate:
	$(PY) scripts/whatif_report.py --require-p0

lint:
	$(PY) -m compileall -q src tests scripts

clean-artifacts:
	@echo "artifacts/ holds test evidence and benchmark manifests."
	@echo "Remove it by hand if you are sure; it is not deleted automatically."
