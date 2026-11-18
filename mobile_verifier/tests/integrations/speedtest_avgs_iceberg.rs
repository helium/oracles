use chrono::{SubsecRound, Utc};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::iceberg::{self, speedtest_avg::IcebergSpeedtestAvg};

fn make_iceberg_speedtest_avg(seed: u8) -> IcebergSpeedtestAvg {
    let pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let now = Utc::now().trunc_subsecs(3);
    IcebergSpeedtestAvg {
        hotspot_pubkey: pubkey.to_string(),
        upload_speed_avg_bps: 10_000_000,
        download_speed_avg_bps: 100_000_000,
        latency_avg_ms: 25,
        reward_multiplier: 1.0,
        sample_count: 2,
        timestamp: now.into(),
        speedtests: vec![],
    }
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_speedtest_avg() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtestAvg>(iceberg::speedtest_avg::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest_avg(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_speedtest_avgs() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtestAvg>(iceberg::speedtest_avg::TABLE_NAME)
        .await?;

    let records: Vec<IcebergSpeedtestAvg> = (1u8..=5).map(make_iceberg_speedtest_avg).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtestAvg>(iceberg::speedtest_avg::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest_avg(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}
