# Project 04 - Snapshot Reconciliation & Recovery
#
# A control-plane image, not a server: `reconctl` collects evidence, proposes
# repairs and waits for approval. There is no port to expose and no loop by
# default, because a reconciliation that starts itself is a reconciliation
# nobody approved (design decision 6).
#
# The default command prints help rather than acting, so an accidental
# `docker run` cannot begin a repair.
#
# Build context is the repository root, for symmetry with the other components
# whose dependencies live outside their own directory.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY apps/p04-reconciliation/pyproject.toml ./
COPY apps/p04-reconciliation/src ./src

RUN pip install --upgrade pip && pip install .

# Evidence lives on a volume, outside the image. Design section 17 requires a
# findings database to survive the container that produced it: an audit trail
# inside a layer disappears on redeploy.
RUN useradd --create-home --uid 10004 p04 \
 && mkdir -p /srv/var/recon \
 && chown -R p04:p04 /srv/var/recon /app
USER p04
VOLUME ["/srv/var/recon"]

ENV RECON_DB_PATH=/srv/var/recon/recon.db

ENTRYPOINT ["reconctl"]
CMD ["--help"]
