# mobile_dbt

dbt transformations over the Iceberg tables in the `mobile` catalog. Public,
like the oracles themselves.

## Why it lives here

Most of this catalog is written by the Rust oracles a few directories away, and
the schema of each of those tables is a `table_definition()` function rather
than a migration file. That matters for how a dbt source declaration stays
honest: the only way to check one is to build against a table created by that
same function, which CI can do here and could not do from another repository.
A column added in `session.rs` and the model that consumes it land in one pull
request, reviewed together.

Today the project does not exercise that. It has one model, over the one source
in this catalog that the repo does *not* produce, so nothing here reads an
oracle table and no Rust runs in CI. The argument above is why this is the right
home for the models that will, not a description of what it is currently doing.

## Scope

**The `mobile` catalog, and nothing else.** A model that needs data from
outside it does not belong here. Keeping to that line is what makes the
project's boundary a rule rather than a habit.

Within the catalog, **a model lands in the schema of the data it describes** --
`hotspots.enabled_carriers_inventory` sits beside the
`hotspots.enabled_carriers_history` it summarises, rather than in a separate
`analytics` bucket. So each model declares its own `schema=` and the project
sets no default; `macros/generate_schema_name.sql` keeps dbt from prefixing it,
because a name encoding whose target built it is not something a consumer can
depend on and would not sit beside anything. Consumers declare
`mobile.<namespace>.<model>` as a source and treat the mobile catalog as
authoritative for this data.

Two things follow from that choice. dbt needs write access to schemas whose raw
tables it does not produce, and its output shares a namespace with them -- so
model names must not collide with source names, and the grain tests in
`_marts.yml` are the guard against a model quietly becoming something else.

The one declared source comes from the S3 binary-file ingest pipeline, which
lives elsewhere, so CI creates it from a hand-maintained copy of its production
DDL in `scripts/external_sources.sql`. Drift there fails CI as a wrong or
missing column rather than as a silently wrong mart — weaker than deriving the
schema from its own definition, and the best available for a table this repo
does not own. Refresh that file with `SHOW CREATE TABLE` when upstream moves.

The oracle-written tables (`data_transfer`, `poc`, `rewards`, `tokens`) are
deliberately not declared. Nothing models them, so declaring them verified only
their own accuracy — and paid for it with a full `mobile-verifier` compile in
CI, to create the tables to check them against. Declare one when a model needs
it, and bring that check back with it.

Where the rest of the catalog's schemas live, for when a model needs one:

| namespace | tables | defined in |
| --- | --- | --- |
| `hotspots` | 1 — **declared** | *external* — mirrored in `scripts/external_sources.sql` |
| `data_transfer` | 4 | `helium_iceberg_oracles/src/data_transfer/` |
| `poc` | 8 | `mobile_verifier/src/iceberg/` |
| `rewards` | 3 | `mobile_verifier/src/iceberg/` |
| `tokens` | 1 | `price/src/iceberg/` |

## Layout

```
models/sources.yml   the one source, mirrored from its production DDL
models/marts/        the published models
macros/              incremental window, UTC day, schema naming
tests/               singular tests asserting cross-column invariants (empty)
scripts/             stack setup, catalog registration, source DDL, fixtures
```

## Running it locally

Python 3.12. dbt-trino does not import on 3.14 — see `requirements.txt`.

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

From the repo root, one command stands up everything:

```bash
./dbt/scripts/local_up.sh
```

That waits for Trino, registers the catalog, applies the source DDL, and loads
fixtures. Docker only — no Rust toolchain needed.
Every step is idempotent, so re-running it is the normal way to top up a stack
that has been restarted. Two flags:

```bash
./dbt/scripts/local_up.sh --reset        # wipe the warehouse first (down -v)
./dbt/scripts/local_up.sh --no-fixtures  # schemas only, no rows
```

**Use `--reset` when you want a clean warehouse.** Plain `docker compose down`
leaves the Iceberg data in the RustFS volume, so a stack that looks brand new
can still hold yesterday's rows — which shows up as the source's uniqueness
test failing and reads like a code defect. `--reset` passes `-v`.

Then, from `dbt/`:

```bash
dbt deps && dbt run && dbt test
```

**`dbt run && dbt test`, not `dbt build`.** `dbt build` runs a model's unit
tests before the model itself, and the unit test that supplies `this` needs the
relation to exist so dbt can read its column types — so against a fresh
warehouse `build` errors out before creating anything. Once the models exist
`dbt build` is fine, but the two-step always works and is what CI does.

dbt picks up `profiles.yml` from the working directory, so no `DBT_PROFILES_DIR`
is needed as long as you run from `dbt/`. Set it if you run from anywhere else:

```bash
DBT_PROFILES_DIR=/path/to/oracles/dbt dbt build --project-dir /path/to/oracles/dbt
```

### Fixtures vs unit tests

Two different jobs, and it is worth not confusing them.

**`scripts/fixtures.sql`** is rows in the warehouse, for looking at the thing by
hand and for giving the data tests something real to run against. It is
idempotent — every table is emptied before it is filled. If an oracle-written
table ever needs rows, write them through its Rust row structs rather than as
INSERTs; SQL fixtures re-encode column order and types, which is the
duplication the rest of this project avoids, and is acceptable for the one
external table only because there is no definition here to reuse.

**`models/marts/_unit_tests.yml`** is what actually pins the logic. Rows are
supplied inline, so the tests need no fixtures and no warehouse data and run
green against an empty warehouse — including in CI on every PR. They cover the
first-run branch and, more importantly, carry the regression guard for the
watermark: switch the model's window back to the device-supplied `timestamp` and
`watermark_ignores_reported_clock` fails. That was verified by doing it, not
assumed.

Query the warehouse directly with
`docker compose -p oracles exec trino trino`.

### Two things about the local stack that will bite you

Trino runs with `catalog.management=dynamic` and `catalog.store=memory`
(`infra/trino/etc/config.properties`), which means:

1. **The catalog files under `infra/trino/etc/catalog/` are inert.** A fresh
   Trino has only the `system` catalog. `scripts/register_trino_catalog.sh`
   creates the `iceberg` one at runtime, mirroring what `helium_iceberg`'s test
   harness does for its per-test catalogs.
2. **The registration does not survive a Trino restart.** Re-run the script
   after every `docker compose up`. It is idempotent.

## Conventions that are not negotiable

**Filter on the raw partition column.** Every source table is partitioned on a
`day(<timestamp>)` transform, and Trino prunes those partitions only for a plain
range predicate over the column itself. Wrapping it — `cast(ts as date) >= …`,
`date_trunc(…)` — returns identical rows and reads the entire table. Derive a
day column for grouping if you need one, but keep the raw column for the
predicate. `macros/incremental_window.sql` exists so no model has to get this
right by hand.

**Watermark on the oracle's clock, never the reporter's.** Several sources carry
two timestamps: one the oracle assigned on receipt and one supplied by the
device or payer. Which rows an incremental run *considers* must key off the
former — it is monotonic with ingest, so "received since the last run" is
exactly the set of new rows, and it is the partition key, so the predicate
prunes. A watermark on a reported clock fails both ways, and the failure is
silent: see the header of `models/marts/enabled_carriers_inventory.sql` for the
worked case where one device with a misconfigured clock stops every other
hotspot from ever being picked up again.

Which row *wins* within a key is a different question, and a reported clock can
legitimately be the answer there — it is the moment being described. Keep the
two decisions separate and say which is which.

**Grain is asserted, not assumed.** Every mart pins its grain with a test
(`unique`, or `unique_combination_of_columns` for a compound one). A source
change that fans one row into many then fails loudly instead of quietly
doubling every count downstream.

## Deployment

The image is built and pushed on `dbt-v*` tags only, so shipping models is not
tied to the release cadence of the Rust binaries. Everything the container
needs comes from the environment (`TRINO_HOST`, `TRINO_JWT_TOKEN`,
`MOBILE_CATALOG`, `DBT_TARGET=prod`); see `profiles.yml`.

```bash
docker run IMAGE                     # wait for Trino, then `dbt run` + `dbt test`
docker run IMAGE run --select foo    # wait, then that command
DBT_SKIP_TRINO_WAIT=1 docker run …   # straight to dbt (parse, deps, --version)
```

### The cluster sleeps

The production Trino runs on Railway with app sleeping, so a scheduled run can
fire at a suspended cluster. Railway wakes a service on an inbound HTTP request
but does not serve the request that did the waking, and dbt-trino does not retry
a failed connection — so dbt's first query would simply fail the run.

`docker-entrypoint.sh` therefore runs `scripts/wait_for_trino.py` before handing
over to dbt. Putting it in the entrypoint rather than in the scheduler's command
means it cannot be left out by whatever invokes the image. Two phases:

1. Poll `/v1/info` until the coordinator reports `starting=false`. Any HTTP
   response proves it is awake — a 401 counts. 502/503/504 mean still coming up;
   a connection error means still asleep.
2. `SELECT 1` over the real credentials, because Trino accepts connections
   before it accepts queries. If phase 1 saw `starting=false` this is a single
   confirming attempt, so a credential or catalog problem surfaces immediately
   instead of being buried under retries. If `/v1/info` was behind auth and
   `starting` could not be read, it retries for the remaining budget.

Budget is `TRINO_WAIT_TIMEOUT` (default 300s — a JVM cold start is slow) polled
every `TRINO_WAIT_INTERVAL` (default 5s). It uses the same environment the dbt
profile does, so there is nothing extra to configure.

To exercise it against the local stack:

```bash
docker compose -p oracles stop trino
TRINO_HOST=localhost TRINO_PORT=8080 TRINO_HTTP_SCHEME=http \
  TRINO_USER=dbt MOBILE_CATALOG=iceberg python scripts/wait_for_trino.py &
docker compose -p oracles start trino
```

## Known gaps

- **No source freshness in CI.** `dbt source freshness` thresholds are declared
  in `sources.yml` but nothing runs them on a schedule yet.
- **One mart so far.** The sources are all declared and tested, so adding
  models is additive from here.
- **`tags=['15m']` on the mart records intended cadence but nothing acts on
  it.** Whatever schedules the container has to honour it — and the cadence is
  inherited rather than chosen, so it is worth confirming carrier enablement
  needs quarter-hour freshness.
- **Waking the cluster does not restore a dynamic catalog.** The wait proves
  Trino is serving queries, not that the catalog dbt needs is registered. That
  is fine for a coordinator with static catalog files, and NOT fine for one
  running `catalog.store=memory` like the local stack, which loses its catalogs
  on every restart. Worth confirming which the Railway cluster is.
- **No Iceberg table maintenance.** An upsert model run every 15 minutes
  produces ~96 snapshots a day and a trail of small files. `expire_snapshots`
  and `optimize` need scheduling against the marts; nothing here does it.
