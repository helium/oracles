# Runtime image for the mobile_dbt project.
#
# Separate from the repo-root Dockerfile, which builds Rust binaries and takes a
# PACKAGE build-arg. Nothing is shared between them, so they are two images with
# two cache keys -- see the `dbt-*` jobs in .github/workflows/dbt.yml.
#
# Python 3.12: see the note in requirements.txt about 3.14.
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/dbt \
    # dbt writes logs/ and target/ relative to the project dir; keep them off
    # the read-only project copy so the image can run with a read-only root fs.
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_TARGET_PATH=/tmp/dbt/target

WORKDIR /dbt

# Dependencies first: requirements change far less often than models, so a model
# edit reuses this layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# `dbt deps` is resolved at build time from the committed lock file, so the
# image never reaches out to the package hub at run time.
COPY packages.yml package-lock.yml ./
COPY dbt_project.yml profiles.yml ./
RUN dbt deps

COPY macros/ ./macros/
COPY models/ ./models/
COPY tests/ ./tests/

# Only the connection probe -- the rest of scripts/ drives docker compose and
# is for local development, not for inside the image.
COPY scripts/wait_for_trino.py ./scripts/
COPY docker-entrypoint.sh ./

# Fail the build rather than the first run if the project does not parse. Needs
# no warehouse.
RUN dbt parse

# The entrypoint waits for Trino to be awake and serving before handing over to
# dbt; see its header. No default target and no default command: the profile
# takes its target from DBT_TARGET, every connection value comes from the
# environment (see profiles.yml), and no arguments means the full
# `dbt run` + `dbt test` cycle.
ENTRYPOINT ["/dbt/docker-entrypoint.sh"]
