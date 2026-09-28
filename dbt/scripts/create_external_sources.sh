#!/usr/bin/env bash
#
# Apply scripts/external_sources.sql to the local Trino, substituting the
# catalog name. See that file's header for what it is and why it is a copy.
#
# Run after register_trino_catalog.sh. Idempotent (CREATE ... IF NOT EXISTS).
set -euo pipefail

PROJECT="${COMPOSE_PROJECT_NAME:-oracles}"
CATALOG="${MOBILE_CATALOG:-iceberg}"

SQL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/external_sources.sql"

echo "creating external source mirrors in catalog '${CATALOG}'"
# Only ${CATALOG} is substituted; a bare `envsubst` would also eat the
# Trino-legitimate `$` in any future column default or string literal.
sed "s|\${CATALOG}|${CATALOG}|g" "${SQL_FILE}" \
  | docker compose -p "${PROJECT}" exec -T trino trino -f /dev/stdin
