# Risk Reporting API. Runs as a non-root user and ships no development secret.
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

# The Project 05 engine is a sibling package, so the build context is the
# repository root rather than this directory.
COPY packages/risk-engine /srv/packages/risk-engine
COPY apps/p06-api/pyproject.toml /srv/apps/p06-api/pyproject.toml
COPY apps/p06-api/src /srv/apps/p06-api/src

RUN pip install --no-cache-dir /srv/packages/risk-engine \
 && pip install --no-cache-dir /srv/apps/p06-api

RUN useradd --create-home --shell /usr/sbin/nologin riskapi \
 && mkdir -p /srv/var/audit \
 && chown -R riskapi:riskapi /srv/var
USER riskapi

ENV RISK_API_ENVIRONMENT=staging \
    RISK_API_AUDIT_BACKEND=jsonl \
    RISK_API_AUDIT_PATH=/srv/var/audit/api_access_audit.jsonl

EXPOSE 8060

# Liveness only; it must not depend on the projection store.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8060/health/live', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "risk_api.main:asgi_app", "--factory", "--host", "0.0.0.0", "--port", "8060"]
