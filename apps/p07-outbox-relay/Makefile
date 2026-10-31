# Project 07 — PostgreSQL Outbox & Kafka
#
# `make smoke` is the per-commit gate; `make verify` is the full evidence run.
# Nothing here claims a performance or durability result: see README.md.

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest

.DEFAULT_GOAL := help
.PHONY: help install smoke test unit contract integration chaos property mutation perf \
        demo bench evidence lint clean ha-up ha-down pg-test

help:
	@echo "install     install the package with test extras"
	@echo "smoke       fast per-commit suite (excludes slow)"
	@echo "test        the whole suite, slow layers included"
	@echo "unit contract integration chaos property mutation perf   one layer"
	@echo "demo        the three MVP demonstrations from design section 0.1"
	@echo "bench       scaled-down throughput run (harness figure only)"
	@echo "evidence    per-layer run with the section 14.5 result contract"
	@echo "ha-up       start the 3-broker lab (the only place durability is testable)"
	@echo "pg-test     run the PostgreSQL-backed tests against OUTBOX_PG_DSN"

install:
	$(PYTHON) -m pip install -e '.[test]'

smoke:
	$(PYTEST) -q -m "not slow"

test:
	$(PYTEST) -q

unit:
	$(PYTEST) -q -m unit

contract:
	$(PYTEST) -q -m contract

integration:
	$(PYTEST) -q -m integration

chaos:
	$(PYTEST) -q -m chaos

property:
	$(PYTEST) -q -m property

mutation:
	$(PYTEST) -q -m mutation

perf:
	$(PYTEST) -q -m performance -s

demo:
	$(PYTHON) -m outbox.cli demo

bench:
	$(PYTHON) -m outbox.cli bench --events 5000 --accounts 20

evidence:
	$(PYTHON) scripts/collect_evidence.py --include-slow

lint:
	@command -v ruff >/dev/null 2>&1 && ruff check src tests scripts || \
		echo "ruff is not installed; skipping"

clean:
	rm -rf .pytest_cache .hypothesis evidence *.sqlite3 *.sqlite3-wal *.sqlite3-shm
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

# --- the HA-LIKE lab -------------------------------------------------------
# RF=3 with min.insync.replicas=2 and acks=all is the configuration the
# durability claims are stated relative to. A single broker cannot produce that
# evidence no matter how the tests are written.
ha-up:
	docker compose -f docker-compose.ha.yml up -d
	@echo "brokers on localhost:19092,19093,19094; postgres on localhost:15432"

ha-down:
	docker compose -f docker-compose.ha.yml down -v

pg-test:
	@test -n "$(OUTBOX_PG_DSN)" || (echo "set OUTBOX_PG_DSN first" && exit 1)
	$(PYTEST) -q -m postgres
