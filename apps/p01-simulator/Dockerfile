# MT5 Broker Simulator.
#
# Single writer process by design: `seq` allocation and the writer queue are
# only correct with one process per database, so there is no gunicorn and no
# worker count to tune. A second worker would not corrupt the file - it would
# break sequence contiguity, which is worse, because the data would look fine
# (WF-120).

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first so a source change does not invalidate the layer.
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY scenarios ./scenarios
COPY scripts ./scripts

# The database lives on a volume. Without one, a container restart loses the
# dataset and WF-121 ("restart, persistent volume intact") cannot be tested.
RUN mkdir -p /data && chown -R 65532:65532 /data /app
VOLUME ["/data"]

ENV SIM_DB_PATH=/data/simulator.db \
    SIM_HOST=0.0.0.0 \
    SIM_PORT=8001 \
    SIM_ENV=docker \
    SIM_LOG_FORMAT=json

# Binding 0.0.0.0 means the three secrets are mandatory; startup fails closed
# without them rather than exposing an unauthenticated control plane
# (WF-108, WF-122). They are deliberately not defaulted here.
#   SIM_DATA_TOKEN, SIM_ADMIN_TOKEN, SIM_CURSOR_SIGNING_KEY

USER 65532:65532

EXPOSE 8001

# Liveness only: it performs no query and holds no lock, so it still answers
# while every data slot is occupied (WF-056). Readiness is a separate endpoint
# and is what an orchestrator should gate traffic on.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8001/health/live', timeout=2).status==200 else 1)"

ENTRYPOINT ["python", "-m", "simulator.cli"]
CMD ["serve"]
