//! End-to-end test of [`epoch_reward_info::epoch_statement`] against the real
//! Postgres-backed tables it reads in production.
//!
//! Production reads `solana.public.dao_epoch_infos` / `sub_dao_epoch_infos`
//! through a Trino postgresql-connector catalog. Unlike the iceberg-backed
//! `rewarder_epoch_reward_info_trino` test, here the tables are created in the
//! `#[sqlx::test]` database with their real Postgres column types, and a
//! dynamic Trino catalog is registered against that database so the statement
//! runs through the same connector as production.

use helium_iceberg::HarnessConfig;
use mobile_verifier::rewarder::epoch_reward_info;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use trino_rust_client::Trino;

// Real on-chain addresses from the prod Solana indexer. IOT is the decoy
// sub-DAO that must be ignored.
const DAO: &str = "BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie";
const MOBILE_SUB_DAO: &str = "Gm9xDCJawDEKDrrQW6haw94gABaYzQwCq4ZQU8h8bd22";
const IOT_SUB_DAO: &str = "39Lw1RH6zt8AJvKn3BTxmUDofzduCM2J3kSaGDZ8L7Sk";

/// Postgres as seen from inside the Trino container (the `postgres` service in
/// both docker-compose and CI).
const TRINO_POSTGRES_HOST: &str = "postgres:5432";

/// Mirrors the `SELECT ... AS` aliases of `epoch_statement`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Trino)]
struct EpochRow {
    deployer_cap_hnt: String,
    dc_burned: String,
    hnt_rewards_issued: String,
    delegation_rewards_issued: String,
    rewards_issued_at: String,
    epoch_address: String,
}

#[sqlx::test(migrations = false)]
async fn epoch_statement_reads_from_postgres(pool: PgPool) -> anyhow::Result<()> {
    create_tables(&pool).await?;
    seed(&pool).await?;

    let trino =
        trino_client::Client::from_client(HarnessConfig::default().trino_client_builder().build()?);
    let catalog = create_catalog(&pool, &trino).await?;

    let result = trino
        .get_all(
            epoch_reward_info::epoch_statement(
                &format!("{catalog}.public"),
                20012,
                DAO,
                MOBILE_SUB_DAO,
            )
            .typed::<EpochRow>(),
        )
        .await;

    assert_eq!(
        result?,
        vec![EpochRow {
            deployer_cap_hnt: "5197507584000".into(),
            dc_burned: "433125632".into(),
            hnt_rewards_issued: "0".into(),
            delegation_rewards_issued: "4931506849316".into(),
            rewards_issued_at: "1729123215".into(),
            epoch_address: "6uh3n1msjxESnXa6QXYtiN76aWo8Jtza2FGs8pyj6f3i".into(),
        }]
    );

    // Only reached on success: a failed run keeps its catalog for inspection,
    // like sqlx keeps its database, and `create_catalog` clears it next run.
    trino
        .execute_raw(format!(r#"DROP CATALOG "{catalog}""#))
        .await?;

    Ok(())
}

async fn create_tables(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE public.sub_dao_epoch_infos (
            address character varying(255) NOT NULL,
            epoch numeric,
            sub_dao character varying(255),
            dc_burned numeric,
            vehnt_at_epoch_start numeric,
            vehnt_in_closing_positions numeric,
            fall_rates_from_closing_positions numeric,
            delegation_rewards_issued numeric,
            utility_score numeric,
            rewards_issued_at numeric,
            bump_seed integer,
            initialized boolean,
            dc_onboarding_fees_paid numeric,
            refreshed_at timestamp with time zone,
            created_at timestamp with time zone NOT NULL,
            hnt_rewards_issued numeric,
            previous_percentage numeric
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE public.dao_epoch_infos (
            address character varying(255) NOT NULL,
            epoch numeric,
            dao character varying(255),
            deployer_cap_hnt numeric
        )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn seed(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO public.sub_dao_epoch_infos (
            address, epoch, sub_dao, dc_burned, vehnt_at_epoch_start,
            vehnt_in_closing_positions, fall_rates_from_closing_positions,
            delegation_rewards_issued, utility_score, rewards_issued_at, bump_seed,
            initialized, dc_onboarding_fees_paid, refreshed_at, created_at,
            hnt_rewards_issued, previous_percentage
        ) VALUES
            ('6uh3n1msjxESnXa6QXYtiN76aWo8Jtza2FGs8pyj6f3i', 20012, $1, 433125632,
             62843306519648125, 0, 0, 4931506849316, 902123282859819773199123,
             1729123215, 253, true, 22636000000, '2025-01-29 20:51:55.516+00',
             '2024-11-14 00:29:28.143+00', 0, 0),
            ('EjTzQSLcfwxwJNcNYNsA7TMgS4xkU5v4Uk76FaAqrjN', 20076, $2, 13573367,
             57923413641283752, 0, 0, 5342465753425, 414196714244783572215317,
             1734653887, 251, true, 1419171300000, '2025-01-29 20:46:12.45+00',
             '2024-11-14 00:29:28.143+00', 0, 0)
        "#,
    )
    .bind(MOBILE_SUB_DAO)
    .bind(IOT_SUB_DAO)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO public.dao_epoch_infos (address, epoch, dao, deployer_cap_hnt)
        VALUES ('daoEpochInfo20012', 20012, $1, 5197507584000)
        "#,
    )
    .bind(DAO)
    .execute(pool)
    .await?;

    Ok(())
}

/// Register a Trino postgresql catalog against this test's database: plain
/// connection properties plus
/// `unsupported-type-handling = CONVERT_TO_VARCHAR` so the unbounded `numeric`
/// columns are exposed (as varchar) instead of silently dropped. Returns the
/// catalog name.
async fn create_catalog(pool: &PgPool, trino: &trino_client::Client) -> anyhow::Result<String> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;
    let catalog = format!("pg{}", database.to_lowercase());

    // The name is stable across runs (sqlx derives the database name from the
    // test path), so clear a catalog left behind by a failed run.
    trino
        .execute_raw(format!(r#"DROP CATALOG IF EXISTS "{catalog}""#))
        .await?;

    trino
        .execute_raw(format!(
            r#"
            CREATE CATALOG "{catalog}" USING postgresql WITH (
                "connection-url" = 'jdbc:postgresql://{TRINO_POSTGRES_HOST}/{database}',
                "connection-user" = 'postgres',
                "connection-password" = 'postgres',
                "unsupported-type-handling" = 'CONVERT_TO_VARCHAR'
            )
            "#
        ))
        .await?;

    Ok(catalog)
}
