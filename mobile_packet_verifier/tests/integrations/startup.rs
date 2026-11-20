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
async fn burned_sessions_still_write_once_the_next_binary_adds_signature() -> anyhow::Result<()> {
    // What the next binary leaves behind: a signature column on
    // burned_sessions, added by the ALTER TABLE run before it deploys. This
    // binary knows nothing of it, and has to keep writing burned sessions
    // while that deploy waits and after a rollback to this one. The column
    // takes a null.
    use trino_rust_client::Trino;

    #[derive(Debug, serde::Serialize, serde::Deserialize, Trino)]
    struct Row {
        signature: Option<String>,
    }

    let harness = common::setup_iceberg().await?;
    let table = format!(
        "{}.{}",
        iceberg::NAMESPACE,
        iceberg::burned_session::TABLE_NAME
    );
    harness
        .trino()
        .execute(format!("ALTER TABLE {table} ADD COLUMN signature VARCHAR"))
        .await?;

    let writers = iceberg::get_writers(&harness.as_settings()).await?;
    let now = chrono::Utc::now().fixed_offset();
    writers
        .burned_session
        .write(vec![
            iceberg::burned_session::IcebergBurnedDataTransferSession {
                pub_key: "hotspot".to_string(),
                payer: "payer".to_string(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                rewardable_bytes: 2_000,
                num_dcs: 1,
                multiplier: None,
                first_timestamp: now,
                last_timestamp: now,
                burn_timestamp: now,
            },
        ])
        .await?;

    let rows = harness
        .trino()
        .get_all::<Row>(format!("SELECT signature FROM {table}"))
        .await?
        .into_vec();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].signature, None);

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
