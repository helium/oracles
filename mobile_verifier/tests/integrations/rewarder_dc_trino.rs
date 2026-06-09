use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use mobile_verifier::iceberg::burned_session;
use serde::Serialize;

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const PAYER: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

/// Mirror of `mobile_packet_verifier`'s `IcebergBurnedDataTransferSession` — the
/// shape written into `data_transfer.burned_sessions`.
#[derive(Clone, Debug, Serialize)]
struct BurnedSessionRow {
    pub_key: String,
    payer: String,
    upload_bytes: u64,
    download_bytes: u64,
    rewardable_bytes: u64,
    num_dcs: u64,
    first_timestamp: DateTime<FixedOffset>,
    last_timestamp: DateTime<FixedOffset>,
    burn_timestamp: DateTime<FixedOffset>,
}

fn row(pub_key: &str, rewardable_bytes: u64, num_dcs: u64, burn: DateTime<Utc>) -> BurnedSessionRow {
    BurnedSessionRow {
        pub_key: pub_key.to_string(),
        payer: PAYER.to_string(),
        upload_bytes: rewardable_bytes,
        download_bytes: 0,
        rewardable_bytes,
        num_dcs,
        first_timestamp: burn.into(),
        last_timestamp: burn.into(),
        burn_timestamp: burn.into(),
    }
}

fn at(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, 0, 0).unwrap()
}

async fn write_rows(harness: &IcebergTestHarness, id: &str, rows: Vec<BurnedSessionRow>) {
    let writer = harness
        .get_table_writer_in::<BurnedSessionRow>(burned_session::NAMESPACE, burned_session::TABLE_NAME)
        .await
        .expect("burned_sessions writer");
    writer
        .write_idempotent(id, rows)
        .await
        .expect("write burned sessions");
}

fn key(s: &str) -> PublicKeyBinary {
    s.parse().unwrap()
}

#[tokio::test]
async fn aggregate_sums_per_hotspot_and_respects_epoch_bounds() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let client = trino_client::Client::from_inner(harness.owned_trino().await?);

    let epoch = at(2024, 6, 1, 0)..at(2024, 6, 2, 0);

    write_rows(
        &harness,
        "epoch",
        vec![
            // Two in-range sessions for HOTSPOT_1 — should sum.
            row(HOTSPOT_1, 1_000, 100, at(2024, 6, 1, 6)),
            row(HOTSPOT_1, 500, 50, at(2024, 6, 1, 18)),
            // One in-range session for HOTSPOT_2.
            row(HOTSPOT_2, 2_000, 200, at(2024, 6, 1, 12)),
            // After the epoch end — must be excluded from the aggregate.
            row(HOTSPOT_1, 9_999, 999, at(2024, 6, 2, 12)),
        ],
    )
    .await;

    let map = burned_session::aggregate_hotspot_data_sessions_to_dc(&client, &epoch).await?;

    assert_eq!(map.len(), 2, "out-of-range row should be excluded");

    let h1 = map.get(&key(HOTSPOT_1)).expect("hotspot 1 present");
    assert_eq!(h1.rewardable_dc, 150);
    assert_eq!(h1.rewardable_bytes, 1_500);

    let h2 = map.get(&key(HOTSPOT_2)).expect("hotspot 2 present");
    assert_eq!(h2.rewardable_dc, 200);
    assert_eq!(h2.rewardable_bytes, 2_000);

    Ok(())
}

#[tokio::test]
async fn empty_table_yields_empty_map_and_flags_missing_data() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let client = trino_client::Client::from_inner(harness.owned_trino().await?);

    let epoch = at(2024, 6, 1, 0)..at(2024, 6, 2, 0);

    // Nothing written yet.
    let map = burned_session::aggregate_hotspot_data_sessions_to_dc(&client, &epoch).await?;
    assert!(map.is_empty());
    assert!(
        burned_session::no_burned_sessions(&client, &epoch).await?,
        "no data at all -> pipeline not current"
    );

    Ok(())
}

#[tokio::test]
async fn no_burned_sessions_detects_data_past_period_end() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let client = trino_client::Client::from_inner(harness.owned_trino().await?);

    let epoch = at(2024, 6, 1, 0)..at(2024, 6, 2, 0);

    // Only data within the period: nothing past the end yet.
    write_rows(
        &harness,
        "within",
        vec![row(HOTSPOT_1, 1_000, 100, at(2024, 6, 1, 12))],
    )
    .await;
    assert!(
        burned_session::no_burned_sessions(&client, &epoch).await?,
        "data only within period -> not current past end"
    );

    // A session burned at/after the period end signals the pipeline is current.
    write_rows(
        &harness,
        "past_end",
        vec![row(HOTSPOT_2, 2_000, 200, at(2024, 6, 2, 1))],
    )
    .await;
    assert!(
        !burned_session::no_burned_sessions(&client, &epoch).await?,
        "data past period end -> current"
    );

    Ok(())
}
