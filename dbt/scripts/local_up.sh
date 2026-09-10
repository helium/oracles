#!/usr/bin/env bash
#
# Stand up everything the dbt project needs locally, in one command.
#
#   ./dbt/scripts/local_up.sh              # bring up / top up
#   ./dbt/scripts/local_up.sh --reset      # wipe the warehouse first
#   ./dbt/scripts/local_up.sh --no-fixtures
#
# Steps, in the order they have to happen:
#
#   1. compose up      Trino + Polaris + RustFS + Postgres, waited on.
#   2. catalog         `CREATE CATALOG`, because local Trino keeps its catalog
#                      store in memory and loses the registration on restart.
#   3. external DDL    the mirrored DDL for the source this repo does not
#                      produce.
#   4. fixtures        rows, so the data tests run against something real.
#
# Every step is idempotent, so re-running is safe and is the normal way to top
# up a stack that has been restarted.
#
# USE `--reset` WHEN YOU WANT A CLEAN WAREHOUSE. Plain `docker compose down`
# keeps the Iceberg data in the RustFS volume, so a stack that looks brand new
# can still hold yesterday's rows. `--reset` passes `-v` and removes them.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

PROJECT="${COMPOSE_PROJECT_NAME:-oracles}"
export COMPOSE_PROJECT_NAME="${PROJECT}"

RESET=false
FIXTURES=true
for arg in "$@"; do
  case "${arg}" in
    --reset)       RESET=true ;;
    --no-fixtures) FIXTURES=false ;;
    -h|--help)     sed -n '2,30p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *)             echo "unknown option: ${arg}" >&2; exit 2 ;;
  esac
done

step() { printf '\n== %s\n' "$1"; }

if [ "${RESET}" = true ]; then
  step "removing the existing stack and its volumes"
  docker compose -p "${PROJECT}" down -v
fi

step "starting Trino and its dependencies"
docker compose -p "${PROJECT}" up -d --wait trino

step "registering the Trino catalog"
./dbt/scripts/register_trino_catalog.sh

step "creating external source mirrors"
./dbt/scripts/create_external_sources.sh

if [ "${FIXTURES}" = true ]; then
  step "loading fixtures"
  ./dbt/scripts/load_fixtures.sh
fi

cat <<'DONE'

== ready

  cd dbt && dbt run && dbt test

Trino is on localhost:8080. To query it directly:

  docker compose -p oracles exec trino trino
DONE
