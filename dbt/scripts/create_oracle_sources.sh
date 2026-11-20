#!/usr/bin/env bash
#
# Create the oracle-written tables dbt reads, from their Rust
# `table_definition()`s, in the local Iceberg catalog. See
# mobile_verifier/examples/create_dbt_sources.rs and
# icicle/examples/create_dbt_sources.rs.
#
# Talks to Polaris directly from the host (localhost:8181), so it does not need
# the Trino catalog registered first -- but Trino will only see the tables once
# register_trino_catalog.sh has run. Idempotent: existing tables are left alone.
#
# Needs a Rust toolchain. The first run compiles both oracles, which takes a
# few minutes; later runs reuse the target directory.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

echo "creating oracle source tables from their Rust definitions"
cargo run --quiet --locked -p mobile-verifier --example create_dbt_sources
cargo run --quiet --locked -p icicle --example create_dbt_sources
