#!/usr/bin/env bash
#
# Load scripts/fixtures.sql into the local Trino, substituting the catalog name.
# Safe to re-run: the fixture script empties each table before filling it.
set -euo pipefail

PROJECT="${COMPOSE_PROJECT_NAME:-oracles}"
CATALOG="${MOBILE_CATALOG:-iceberg}"

SQL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fixtures.sql"

echo "loading fixtures into catalog '${CATALOG}'"
# Only ${CATALOG} is substituted; a bare `envsubst` would also eat any
# Trino-legitimate `$` in a string literal.
sed "s|\${CATALOG}|${CATALOG}|g" "${SQL_FILE}" \
  | docker compose -p "${PROJECT}" exec -T trino trino -f /dev/stdin
