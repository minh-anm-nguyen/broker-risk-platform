# Project 08 - ClickHouse Broker Analytics
#
# One image, three entrypoints (consumer, build worker, query service), as in
# design section 16.1. Installing the `infra` extra here is what distinguishes
# this image from a test run: the offline suite needs no drivers at all.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Build tooling for the driver wheels, purged in the same layer so it does not
# ship in the runtime image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY src ./src

RUN pip install --upgrade pip \
 && pip install ".[infra]" \
 && apt-get purge -y --auto-remove build-essential

COPY database ./database
COPY scripts ./scripts
COPY scenarios ./scenarios

# A non-root user, because the analytics service needs no privileges and a
# container that can write outside its own volume is a liability.
RUN useradd --create-home --uid 10008 p08 \
 && mkdir -p /app/artifacts /app/data \
 && chown -R p08:p08 /app
USER p08

EXPOSE 8008

# The readiness probe touches storage rather than returning a constant: a
# process that reports ready while ClickHouse is unreachable only attracts
# traffic it cannot serve.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8008/health/ready || exit 1

# The default role is the read-only query service. Override the command to run
# the consumer or the build worker from the same image.
CMD ["uvicorn", "analytics.main:app", "--factory", \
     "--host", "0.0.0.0", "--port", "8008"]
