#!/bin/sh
#
# Wake Trino, load the seeds, then run dbt.
#
# The production cluster sleeps (Railway app sleeping), and dbt-trino does not
# retry a connection that fails, so the very first query of a scheduled run
# would land on a suspended service and the run would just fail. The wait has
# to happen before dbt starts, and putting it in the entrypoint rather than in
# the scheduler's command means it cannot be left out by whatever invokes the
# image.
#
# The seeds (the offload eligibility rules) are loaded before every command
# for the same reason: the models that read them fail without them, and a
# scheduler command such as `run --select +tag:1h` never loads them itself,
# because `dbt run` builds models only. Loading here also keeps the reload
# inside the job, so a run never reads a seed another job is half-way through
# replacing (dbt-trino reloads a seed by dropping and refilling its table).
#
#   docker run IMAGE                     # wait, `dbt seed`, then `dbt run` and `dbt test`
#   docker run IMAGE run --select foo    # wait, `dbt seed`, then that command
#   DBT_SKIP_TRINO_WAIT=1 docker run …   # straight to dbt, no seed (parse, deps, --version)
#
# `dbt run` then `dbt test` rather than `dbt build`: build runs a model's unit
# tests before the model, and the unit test that supplies `this` needs the
# relation to exist to read its column types, so `build` fails on a warehouse
# where the models have never been created. See models/marts/_unit_tests.yml.
set -e

# Without a warehouse there is nothing to wait for and nowhere to seed.
if [ -z "${DBT_SKIP_TRINO_WAIT}" ]; then
  python /dbt/scripts/wait_for_trino.py
  dbt seed
fi

if [ "$#" -eq 0 ]; then
  dbt run
  exec dbt test
fi

exec dbt "$@"
