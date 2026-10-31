# Risk Reporting API developer tasks.
#
# PY points at the repository virtualenv. Override it for another interpreter:
#   make smoke PY=python
PY ?= ../../.venv/Scripts/python.exe
PYTEST = $(PY) -m pytest

.PHONY: help install smoke test unit contract integration adversarial chaos perf property \
        invariants sentinels recipes openapi openapi-check token serve clean

help:
	@echo "smoke         unit + contract, the per-commit gate"
	@echo "test          the whole suite including the slow markers"
	@echo "unit          validation, authz, decimal, cursor, admission, engine"
	@echo "contract      HTTP routing, schemas, headers, health, openapi"
	@echo "integration   cut races, audit lifecycle, authz matrix, isolation"
	@echo "adversarial   fuzz, tamper and abuse probing"
	@echo "chaos         kill points and compound campaigns (slow)"
	@echo "perf          saturation and responsiveness, prints measurements (slow)"
	@echo "property      hypothesis properties and the state machine"
	@echo "invariants    static boundary checks over the source tree"
	@echo "sentinels     each removed safety check must break a test"
	@echo "recipes       run the fault recipes in recipes/"
	@echo "openapi       export openapi/risk-reporting-v1.json"
	@echo "token         mint a local development bearer token"
	@echo "serve         run the API on 127.0.0.1:8060"

install:
	$(PY) -m pip install -e ../../packages/risk-engine
	$(PY) -m pip install -e ".[test]"

smoke:
	$(PYTEST) tests/unit tests/contracts -q

test:
	$(PYTEST) tests -q

unit:
	$(PYTEST) tests/unit -q

contract:
	$(PYTEST) tests/contracts -q

integration:
	$(PYTEST) tests/integration -q

adversarial:
	$(PYTEST) tests/adversarial -q

chaos:
	$(PYTEST) tests/chaos -q

perf:
	$(PYTEST) tests/performance -q -s

property:
	$(PYTEST) tests/property -q

invariants:
	$(PY) scripts/verify_invariants.py

sentinels:
	$(PY) scripts/run_mutation_sentinels.py

recipes:
	$(PY) scripts/run_fault_recipe.py \
		recipes/IT06-04-repair-race-v1.yaml \
		recipes/IT06-07-audit-lost-ack-v1.yaml \
		recipes/IT06-11-invalid-engine-result-v1.yaml

openapi:
	$(PY) scripts/export_openapi.py

openapi-check:
	$(PY) scripts/export_openapi.py --check

token:
	$(PY) scripts/mint_dev_token.py --header

serve:
	RISK_API_ENVIRONMENT=local $(PY) -m uvicorn risk_api.main:asgi_app \
		--factory --host 127.0.0.1 --port 8060 --reload

clean:
	rm -rf .pytest_cache .ruff_cache artifacts tests/_evidence.jsonl
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
