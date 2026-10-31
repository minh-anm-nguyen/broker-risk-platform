# Project 05 - Broker Risk Calculation Engine
#
# The engine is a library; Project 06 serves it. This image exists for the two
# operator tools - `risk-report` and `risk-benchmark` - and as the reproducible
# environment a benchmark number must be attributable to. A measurement taken
# outside a declared envelope is not comparable to anything (P09 section 12.1),
# so the base image is pinned and the CPU and memory limits belong in compose.
#
# No server, no port. If something here grows an HTTP surface, it has taken
# over Project 06's job.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY apps/p05-risk-engine/pyproject.toml ./
COPY apps/p05-risk-engine/src ./src

# The postgres extra is what lets the reader reach Project 03. Without it the
# engine still runs, but only against SQLite, which is the offline test path.
# The fallback is deliberate rather than sloppy: an image that cannot build
# without a driver extra is an image nobody can build offline.
RUN pip install --upgrade pip \
 && (pip install ".[postgres]" || pip install ".") \
 && apt-get purge -y --auto-remove build-essential

COPY apps/p05-risk-engine/database ./database

RUN useradd --create-home --uid 10005 p05 \
 && mkdir -p /app/artifacts \
 && chown -R p05:p05 /app
USER p05

# No default action. The tools take an explicit cut, policy version and output
# path; defaulting any of them would produce a report whose inputs nobody chose.
ENTRYPOINT ["risk-report"]
CMD ["--help"]
