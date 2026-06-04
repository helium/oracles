use chrono::{Duration, SubsecRound, Utc};
use file_store_oracles::speedtest::CellSpeedtestIngestReport;
use helium_crypto::PublicKeyBinary;
use mobile_verifier::iceberg::{self, speedtest::IcebergSpeedtest};

fn make_iceberg_speedtest(seed: u8) -> IcebergSpeedtest {
    let pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let now = Utc::now().trunc_subsecs(3);
    let received = now + Duration::seconds(1);
    let report = CellSpeedtestIngestReport {
        received_timestamp: received,
        report: file_store_oracles::speedtest::CellSpeedtest {
            pubkey,
            serial: "test-serial".to_string(),
            timestamp: now,
            upload_speed: 10_000_000,
            download_speed: 100_000_000,
            latency: 25,
        },
    };
    IcebergSpeedtest::from(&report)
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_speedtest() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_speedtests() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let records: Vec<IcebergSpeedtest> = (1u8..=5).map(make_iceberg_speedtest).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}
