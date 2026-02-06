use std::str::FromStr;

use chrono::Utc;
use file_store::file_sink::{FileSinkClient, MessageReceiver};
use file_store_oracles::mobile_session::{
    DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, VerifiedDataTransferIngestReportV1,
};
use mobile_packet_verifier::{accumulate::accumulate_sessions, banning, dc_to_bytes};
use sqlx::{types::Uuid, PgPool};

use crate::common::{setup_iceberg, TestMobileConfig};

#[sqlx::test]
async fn burn_metric_reports_0_after_successful_accumulate_and_burn(
    pool: PgPool,
) -> anyhow::Result<()> {
    let harness = setup_iceberg().await?;

    let payer_key =
        PublicKeyBinary::from_str("112c85vbMr7afNc88QhTginpDEVNC5miouLWJstsX6mCaLxf8WRa")?;

    let mk_dt = |rewardable_bytes: u64| DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes,
            pub_key: PublicKeyBinary::from(vec![1]),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: PublicKeyBinary::from(vec![1]),
                upload_bytes: 0,
                download_bytes: 0,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: Uuid::new_v4().to_string(),
                payer: payer_key.clone(),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    };

    // Fill reports with rewardable_byte values that are just over and under the rounding limit.
    let mut reports = vec![];
    for _ in 0..1000 {
        reports.push(mk_dt(dc_to_bytes(100) + 2));
        reports.push(mk_dt(dc_to_bytes(150) - 2_000));
    }

    let metrics = TestMetrics::new();

    // accumulate and burn
    run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;
    run_burner(&pool, &payer_key, Some(harness.trino())).await?;

    metrics.assert_pending_dc_burn(&payer_key, 0).await?;

    Ok(())
}

async fn run_accumulate_sessions(
    pool: &PgPool,
    reports: Vec<DataTransferSessionIngestReport>,
    mobile_config: TestMobileConfig,
) -> anyhow::Result<MessageReceiver<VerifiedDataTransferIngestReportV1>> {
    let mut txn = pool.begin().await?;

    let (verified_sessions_tx, verified_sessions_rx) = tokio::sync::mpsc::channel(999_999);
    let verified_sessions = FileSinkClient::new(verified_sessions_tx, "test");

    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;
    accumulate_sessions(
        &mobile_config,
        banned_radios,
        &mut txn,
        &verified_sessions,
        Utc::now(),
        futures::stream::iter(reports),
    )
    .await?;
    txn.commit().await?;

    Ok(verified_sessions_rx)
}

async fn run_burner(
    pool: &PgPool,
    payer_key: &PublicKeyBinary,
    trino: Option<&trino_rust_client::Client>,
) -> anyhow::Result<()> {
    let (valid_sessions_tx, _valid_sessions_rx) = tokio::sync::mpsc::channel(999_999);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let solana_network = solana::burn::TestSolanaClientMap::default();
    solana_network.insert(payer_key, 900_000_000).await;
    mobile_packet_verifier::burner::Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
    )
    .burn(pool, trino)
    .await?;

    Ok(())
}

#[derive(Clone)]
struct TestMetrics {
    addr: String,
}

impl TestMetrics {
    fn new() -> Self {
        let addr = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("tcp listener");
            listener.local_addr().expect("local address")
        };

        poc_metrics::start_metrics(&poc_metrics::Settings { endpoint: addr })
            .expect("install prometheus");
        TestMetrics {
            addr: format!("http://{addr}"),
        }
    }

    async fn assert_pending_dc_burn(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> anyhow::Result<()> {
        let res = reqwest::get(self.addr.clone()).await?;
        let body = res.text().await?;
        if body.is_empty() {
            anyhow::bail!("metrics body is empty")
        }

        let expected = format!(r#"pending_dc_burn{{payer="{payer}"}} {amount}"#);
        if !body.contains(&expected) {
            anyhow::bail!("expected: {expected} in:\n{body}");
        }

        Ok(())
    }
}
