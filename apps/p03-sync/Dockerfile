# Non-root, pinned base, no build toolchain in the final image (design 11.9).
FROM python:3.12-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

RUN pip install --no-cache-dir . && \
    adduser --system --no-create-home --uid 10001 sync

# Migrations are never run at startup: a container restart must not be able to
# alter the schema (design 11.7). Run `alembic upgrade head` as a deliberate
# step with the migration role.
USER 10001

ENTRYPOINT ["trading-sync"]
CMD ["status"]
