# Project 07 - PostgreSQL Outbox & Kafka relay
#
# Build context is the repository root, because this component depends on the
# shared event contract in packages/platform-events. That dependency is the
# point: the relay must agree with the producer (Project 03) and the consumer
# (Project 08) about envelope, identity and money, and one shared package is
# the only way to guarantee that rather than three copies that drift.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY packages/platform-events ./packages/platform-events
COPY apps/p07-outbox-relay/pyproject.toml ./
COPY apps/p07-outbox-relay/src ./src

RUN pip install --upgrade pip \
 && pip install ./packages/platform-events \
 && (pip install ".[postgres,kafka]" || pip install ".") \
 && apt-get purge -y --auto-remove build-essential

RUN useradd --create-home --uid 10007 p07 \
 && chown -R p07:p07 /app
USER p07

ENV OUTBOX_STORE_BACKEND=postgres

# `outbox migrate` deliberately does NOT create Project 03's tables unless
# asked with --with-p03-standin. Against a shared database the stand-in DDL is
# CREATE TABLE IF NOT EXISTS, so it would be accepted silently rather than
# rejected, taking ownership of a schema Project 03 owns (contract gap CG-07).
#
# No default role either. `relay` publishes to Kafka and `consume` applies
# downstream effects; starting either by accident would move real events.
ENTRYPOINT ["outbox"]
CMD ["--help"]
