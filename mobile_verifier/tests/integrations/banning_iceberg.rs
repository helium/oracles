use chrono::{DateTime, Duration, SubsecRound, Utc};
use file_store_oracles::mobile_ban::{
    proto::{BanReason, VerifiedBanIngestReportStatus},
    BanAction, BanDetails, BanReport, BanRequest, BanType, VerifiedBanReport,
};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::iceberg::{self, ban::IcebergBan};

fn truncated_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(3)
}

fn make_iceberg_ban(seed: u8) -> IcebergBan {
    let hotspot_pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let ban_pubkey = PublicKeyBinary::from(vec![seed + 1, seed + 1]);
    let now = truncated_now();

    let report = VerifiedBanReport {
        verified_timestamp: now,
        report: BanReport {
            received_timestamp: now,
            report: BanRequest {
                hotspot_pubkey,
                timestamp: now - Duration::minutes(5),
                ban_pubkey,
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: format!("SN-{seed}"),
                    message: format!("ban {seed}"),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::Poc,
                    expiration_timestamp: None,
                }),
            },
        },
        status: VerifiedBanIngestReportStatus::Valid,
    };
    IcebergBan::from(&report)
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_ban() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergBan>(iceberg::ban::TABLE_NAME)
        .await?;

    let record = make_iceberg_ban(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_bans() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergBan>(iceberg::ban::TABLE_NAME)
        .await?;

    let records: Vec<IcebergBan> = (1u8..=5).map(make_iceberg_ban).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergBan>(iceberg::ban::TABLE_NAME)
        .await?;

    let record = make_iceberg_ban(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}
