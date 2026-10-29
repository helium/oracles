use helium_iceberg::{FieldDefinition, IcebergTestHarness, TableDefinition};
use mobile_packet_verifier::{daemon::run_migrations, iceberg};
use sqlx::PgPool;

use crate::common;

#[sqlx::test]
async fn migrations_ignore_ones_from_a_newer_binary(pool: PgPool) -> anyhow::Result<()> {
    // What a newer binary leaves behind before this one is rolled back to.
    sqlx::query(
        r#"
        INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
        VALUES (99999, 'from a newer binary', true, '\x00', 0)
        "#,
    )
    .execute(&pool)
    .await?;

    run_migrations(&pool).await?;

    Ok(())
}

#[tokio::test]
async fn writers_start_when_session_tables_have_session_keys() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;

    iceberg::get_writers(&harness.as_settings()).await?;

    Ok(())
}

#[tokio::test]
async fn writers_refuse_session_tables_without_session_keys() -> anyhow::Result<()> {
    // A live table created before file_ts and record_index existed, with the
    // ALTER TABLE statements not yet run.
    let harness = IcebergTestHarness::new().await?;
    harness
        .create_table(
            TableDefinition::builder(iceberg::NAMESPACE, iceberg::session::TABLE_NAME)
                .with_fields([FieldDefinition::required_string("request_pub_key")])
                .build()?,
        )
        .await?;

    let Err(err) = iceberg::get_writers(&harness.as_settings()).await else {
        panic!("writers started against a table missing file_ts and record_index");
    };
    assert!(
        err.to_string()
            .contains(r#"missing columns ["file_ts", "record_index"]"#),
        "unexpected error: {err:#}"
    );

    Ok(())
}
