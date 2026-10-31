# Linux image for the MVP.
#
# The native Manager SDK driver is deliberately not installable here: its
# wheels are Windows x86-64, and a build that pretended otherwise would turn a
# deployment mistake into a runtime mystery. The driver's preflight refuses to
# construct on this platform rather than failing later with something vague
# (WF2-E-01, WF2-O-01).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir .

# Unprivileged: the connector is read-only and needs nothing from the host.
RUN useradd --create-home --shell /usr/sbin/nologin connector
USER connector

EXPOSE 8080

# Liveness only. Readiness depends on the *source*, and gating the container's
# restart policy on it would turn a source outage into a restart loop - the
# wrong remediation, applied automatically (INV-13).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health/live', timeout=2).status == 200 else 1)"

ENTRYPOINT ["python", "-m", "mt5_connector.cli"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8080"]
