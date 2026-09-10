#!/usr/bin/env bash
#
# Register the local Polaris warehouse as a Trino catalog so dbt can reach it.
#
# The local Trino runs with `catalog.management=dynamic` and
# `catalog.store=memory` (infra/trino/etc/config.properties), which means the
# static catalog files mounted at /etc/trino/catalog are NOT loaded -- a fresh
# Trino has only the `system` catalog, and everything else is registered at
# runtime with `CREATE CATALOG`. That is how helium_iceberg's test harness gets
# its per-test catalogs (see `register_trino_catalog` in
# helium_iceberg/src/test_harness.rs); this script does the same thing once, for
# a stable catalog dbt can point at.
#
# Because the store is in memory, the registration does not survive a Trino
# restart. Re-run this after `docker compose up`; it is idempotent.
#
# The connector properties below intentionally match
# infra/trino/etc/catalog/iceberg.properties. That file is inert under dynamic
# catalog management -- if you change it, change this too, or local Trino and
# local dbt will disagree about where the data is.
set -euo pipefail

PROJECT="${COMPOSE_PROJECT_NAME:-oracles}"
CATALOG="${MOBILE_CATALOG:-iceberg}"
WAREHOUSE="${ICEBERG_WAREHOUSE:-iceberg}"
CREDENTIAL="${ICEBERG_CREDENTIAL:-root:s3cr3t}"
SCOPE="${ICEBERG_SCOPE:-PRINCIPAL_ROLE:ALL}"

# Docker-internal hostnames: the statement is executed by Trino, inside the
# compose network, so it must not use the host-published ports.
REST_URI="${ICEBERG_CATALOG_URI_INTERNAL:-http://polaris:8181/api/catalog}"
S3_ENDPOINT="${S3_ENDPOINT_INTERNAL:-http://rustfs:9000}"

read -r -d '' SQL <<SQL || true
CREATE CATALOG IF NOT EXISTS "${CATALOG}" USING iceberg WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = '${REST_URI}',
    "iceberg.rest-catalog.security" = 'OAUTH2',
    "iceberg.rest-catalog.oauth2.credential" = '${CREDENTIAL}',
    "iceberg.rest-catalog.oauth2.scope" = '${SCOPE}',
    "iceberg.rest-catalog.warehouse" = '${WAREHOUSE}',
    "iceberg.file-format" = 'parquet',
    "fs.native-s3.enabled" = 'true',
    "s3.endpoint" = '${S3_ENDPOINT}',
    "s3.region" = '${S3_REGION:-us-east-1}',
    "s3.aws-access-key" = '${S3_ACCESS_KEY:-admin}',
    "s3.aws-secret-key" = '${S3_SECRET_KEY:-admin}',
    "s3.path-style-access" = 'true'
)
SQL

echo "registering Trino catalog '${CATALOG}' -> Polaris warehouse '${WAREHOUSE}'"
docker compose -p "${PROJECT}" exec -T trino trino --execute "${SQL}"
docker compose -p "${PROJECT}" exec -T trino trino --execute 'SHOW CATALOGS'
