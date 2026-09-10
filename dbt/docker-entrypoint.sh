#!/bin/sh
#
# Wake Trino, then run dbt.
#
# The production cluster sleeps (Railway app sleeping), and dbt-trino does not
# retry a connection that fails, so the very first query of a scheduled run
# would land on a suspended service and the run would just fail. The wait has
# to happen before dbt starts, and putting it in the entrypoint rather than in
# the scheduler's command means it cannot be left out by whatever invokes the
# image.
#
#   docker run IMAGE                     # wait, then `dbt run` and `dbt test`
#   docker run IMAGE run --select foo    # wait, then that command
#   DBT_SKIP_TRINO_WAIT=1 docker run …   # straight to dbt (parse, deps, --version)
#
# `dbt run` then `dbt test` rather than `dbt build`: build runs a model's unit
# tests before the model, and the unit test that supplies `this` needs the
# relation to exist to read its column types, so `build` fails on a warehouse
# where the models have never been created. See models/marts/_unit_tests.yml.
set -e

if [ -z "${DBT_SKIP_TRINO_WAIT}" ]; then
  python /dbt/scripts/wait_for_trino.py
fi

if [ "$#" -eq 0 ]; then
  dbt run
  exec dbt test
fi

exec dbt "$@"
