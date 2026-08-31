use std::str::FromStr;

use chrono::{Duration, Utc};
use file_store::file_sink::{FileSinkClient, MessageReceiver};
use file_store_oracles::mobile_session::{
    DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq,
};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, VerifiedDataTransferIngestReportV1,
};
use mobile_packet_verifier::{
    banning, daemon::handle_data_transfer_session_file, dc_to_bytes, iceberg, routing::RoutingKeys,
};
use sqlx::{types::Uuid, PgPool};

use crate::common::hotspot_inventory::MobileHotspotInventory;

/// `pending_dc_burn` reads 0 for payers that burned, and their real debt for a
/// payer that did not.
///
/// That second half is the point of the gauge: leftover DC should be visible.
/// The burn sets each payer's gauge from the priced total before it checks
/// balances, so a payer that cannot pay keeps a gauge showing exactly what it
/// owes, and that figure grows every cycle it fails to burn.
///
/// The gauge is set rather than decremented because the two sides do not count
/// the same way. `accumulate` sums bytes per payer and converts once; the burn
/// converts per hotspot group, and multiplies. Subtracting one from the other
/// walks the gauge negative, which is what it did before this was measured.
///
/// All three payers share one test because `TestMetrics` installs a global
/// recorder: a second instance in the same process leaves the first serving the
/// data and the second serving nothing.
#[sqlx::test]
async fn burn_metric_reports_0_for_burned_payers_and_the_debt_for_stuck_ones(
    pool: PgPool,
) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;
    let burn_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

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

    let metrics = TestMetrics::shared();

    // accumulate and burn
    run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![PublicKeyBinary::from(vec![1])],
        vec![PublicKeyBinary::from(vec![1])],
        Some(session_writer),
    )
    .await?;
    // ...and a second payer whose hotspot carries a 1.5 multiplier.
    let multiplied_gateway = PublicKeyBinary::from(vec![2]);
    let multiplied_payer = PublicKeyBinary::from(vec![9]);
    grant_multiplier(&pool, &multiplied_gateway).await?;
    run_accumulate_sessions(
        &pool,
        &harness,
        vec![mk_dt_for(
            &multiplied_gateway,
            &multiplied_payer,
            dc_to_bytes(200),
        )],
        vec![multiplied_gateway.clone()],
        vec![multiplied_gateway.clone()],
        None,
    )
    .await?;

    // ...and a third who cannot afford what it owes. Its hotspot carries a
    // multiplier, so the debt on the gauge can only have come from the priced
    // total -- `accumulate` would have incremented the smaller, unmultiplied
    // figure.
    let broke_gateway = PublicKeyBinary::from(vec![3]);
    let broke_payer = PublicKeyBinary::from(vec![8]);
    grant_multiplier(&pool, &broke_gateway).await?;
    run_accumulate_sessions(
        &pool,
        &harness,
        vec![mk_dt_for(&broke_gateway, &broke_payer, dc_to_bytes(500))],
        vec![broke_gateway.clone()],
        vec![broke_gateway.clone()],
        None,
    )
    .await?;

    run_burner(
        &pool,
        &[&payer_key, &multiplied_payer],
        &[(&broke_payer, 10)],
        Some(burn_writer),
    )
    .await?;

    metrics.assert_pending_dc_burn(&payer_key, 0).await?;
    // 200 DC accumulated, 300 burned at 1.5x. Set rather than subtracted, so
    // this is 0 and not -100.
    metrics.assert_pending_dc_burn(&multiplied_payer, 0).await?;
    // 500 DC of bytes at 1.5x is 750 owed, against a balance of 10. Nothing
    // burns, and the gauge shows the real debt -- not the 500 `accumulate` put
    // there.
    metrics.assert_pending_dc_burn(&broke_payer, 750).await?;

    // A second cycle with more traffic: the debt grows rather than resetting,
    // which is what makes a stuck payer visible over time.
    run_accumulate_sessions(
        &pool,
        &harness,
        vec![mk_dt_for(&broke_gateway, &broke_payer, dc_to_bytes(300))],
        vec![broke_gateway.clone()],
        vec![broke_gateway.clone()],
        None,
    )
    .await?;
    run_burner(&pool, &[], &[(&broke_payer, 10)], None).await?;
    // 800 DC of bytes now, still at 1.5x.
    metrics.assert_pending_dc_burn(&broke_payer, 1200).await?;

    let trino = harness.trino();
    let all_sessions = iceberg::session::get_all(trino).await?;
    let all_burns = iceberg::burned_session::get_all(trino).await?;

    assert_eq!(all_sessions.len(), 2000, "individual sessions");
    assert_eq!(all_burns.len(), 2, "one combined burn per payer");
    assert!(
        all_burns.iter().any(|b| b.num_dcs == 300),
        "the multiplied payer burned 200 DC at 1.5x"
    );

    Ok(())
}

/// `burned_dc_by_multiplier` splits a payer's burn by the multiplier behind it.
///
/// `pending_dc_burn` tells you sessions are piling up and burns are happening,
/// but it is fed by `accumulate`, which has no multiplier to apply. This is the
/// metric that shows the real rate, and where it comes from.
#[sqlx::test]
async fn burned_dc_is_split_by_multiplier(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let burn_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

    // Its own payer, because the exporter is shared with the other tests here.
    let payer_key = PublicKeyBinary::from(vec![7]);
    let plain = PublicKeyBinary::from(vec![20]);
    let boosted = PublicKeyBinary::from(vec![21]);

    grant_multiplier(&pool, &boosted).await?;

    let metrics = TestMetrics::shared();

    run_accumulate_sessions(
        &pool,
        &harness,
        vec![
            mk_dt_for(&plain, &payer_key, dc_to_bytes(100)),
            mk_dt_for(&boosted, &payer_key, dc_to_bytes(200)),
        ],
        vec![plain.clone(), boosted.clone()],
        vec![plain.clone(), boosted.clone()],
        None,
    )
    .await?;
    run_burner(&pool, &[&payer_key], &[], Some(burn_writer)).await?;

    // 100 DC at 1x, and 200 DC at 1.5x charged as 300.
    metrics.assert_burned_at(&payer_key, "1", 100).await?;
    metrics.assert_burned_at(&payer_key, "1.5", 300).await?;

    // The split adds back up to what the transaction charged.
    metrics
        .assert_line(&format!(
            r#"burned{{payer="{payer_key}",success="true"}} 400"#
        ))
        .await?;

    Ok(())
}

/// A report of `rewardable_bytes` from `gateway`, billed to `payer`.
fn mk_dt_for(
    gateway: &PublicKeyBinary,
    payer: &PublicKeyBinary,
    rewardable_bytes: u64,
) -> DataTransferSessionIngestReport {
    DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes,
            pub_key: gateway.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: gateway.clone(),
                upload_bytes: 0,
                download_bytes: 0,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: Uuid::new_v4().to_string(),
                payer: payer.clone(),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }
}

async fn grant_multiplier(pool: &PgPool, gateway: &PublicKeyBinary) -> anyhow::Result<()> {
    use file_store_oracles::mobile::data_transfer_multiplier::DataTransferMultiplier;
    use mobile_packet_verifier::multiplier::db::{self, GrantedMultiplier};

    let mut txn = pool.begin().await?;
    db::save(
        &mut txn,
        &[GrantedMultiplier {
            hotspot_pubkey: gateway.clone(),
            multiplier: DataTransferMultiplier::new(rust_decimal::dec!(1.5))?,
            effective_timestamp: Utc::now() - Duration::hours(1),
        }],
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

async fn run_accumulate_sessions(
    pool: &PgPool,
    harness: &IcebergTestHarness,
    reports: Vec<DataTransferSessionIngestReport>,
    known_gateways: Vec<PublicKeyBinary>,
    routing_keys: Vec<PublicKeyBinary>,
    iceberg_writer: Option<iceberg::DataTransferWriter>,
) -> anyhow::Result<MessageReceiver<VerifiedDataTransferIngestReportV1>> {
    let seed_ts = Utc::now() - Duration::hours(1);
    let rows = known_gateways
        .iter()
        .map(|gw| MobileHotspotInventory::known(gw, seed_ts))
        .collect();
    crate::common::hotspot_inventory::seed(harness, rows).await?;
    let resolver = crate::common::gateway_resolver(harness).await?;
    let routing_keys: RoutingKeys = routing_keys.into_iter().collect();

    let mut txn = pool.begin().await?;

    let ts = Utc::now();

    let (verified_sessions_tx, verified_sessions_rx) = tokio::sync::mpsc::channel(999_999);
    let verified_sessions = FileSinkClient::new(verified_sessions_tx, "test");

    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;
    handle_data_transfer_session_file(
        &mut txn,
        iceberg_writer.as_ref(),
        None,
        "test_write_id",
        banned_radios,
        &resolver,
        &routing_keys,
        &verified_sessions,
        ts,
        futures::stream::iter(reports),
    )
    .await?;

    txn.commit().await?;

    Ok(verified_sessions_rx)
}

/// `funded` gets enough to burn anything; `underfunded` gets the balance given,
/// which is how a payer is made to fail the balance check.
async fn run_burner(
    pool: &PgPool,
    funded: &[&PublicKeyBinary],
    underfunded: &[(&PublicKeyBinary, u64)],
    iceberg_writer: Option<iceberg::BurnedDataTransferWriter>,
) -> anyhow::Result<()> {
    let (valid_sessions_tx, _valid_sessions_rx) = tokio::sync::mpsc::channel(999_999);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let solana_network = solana::burn::TestSolanaClientMap::default();
    // `burn` visits every payer with pending rows, and asks each for a balance.
    for payer_key in funded {
        solana_network.insert(payer_key, 900_000_000).await;
    }
    for (payer_key, balance) in underfunded {
        solana_network.insert(payer_key, *balance).await;
    }
    mobile_packet_verifier::burner::Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
        iceberg_writer,
    )
    .burn(pool)
    .await?;

    Ok(())
}

#[derive(Clone)]
/// The Prometheus exporter, shared by every test in this binary.
///
/// `start_metrics` installs a *global* recorder, so a second one would leave the
/// first serving the data and the second serving nothing. Tests take this one
/// and keep to their own payer keys so they cannot read each other's writes.
struct TestMetrics {
    addr: String,
}

static METRICS: std::sync::OnceLock<TestMetrics> = std::sync::OnceLock::new();

impl TestMetrics {
    fn shared() -> &'static TestMetrics {
        METRICS.get_or_init(|| {
            let addr = {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("tcp listener");
                listener.local_addr().expect("local address")
            };

            // On its own runtime, on its own thread. The exporter's HTTP server
            // is spawned onto whatever runtime installs it, and each
            // `#[sqlx::test]` brings up and tears down its own -- so installing
            // from inside a test would take the endpoint down with it and leave
            // every later test unable to scrape.
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("metrics runtime");
                rt.block_on(async move {
                    poc_metrics::start_metrics(&poc_metrics::Settings { endpoint: addr })
                        .expect("install prometheus");
                    ready_tx.send(()).expect("signal ready");
                    std::future::pending::<()>().await;
                });
            });
            ready_rx.recv().expect("metrics endpoint started");

            TestMetrics {
                addr: format!("http://{addr}"),
            }
        })
    }

    async fn scrape(&self) -> anyhow::Result<String> {
        let body = reqwest::get(self.addr.clone()).await?.text().await?;
        if body.is_empty() {
            anyhow::bail!("metrics body is empty")
        }
        Ok(body)
    }

    /// Assert an exact line is present, e.g.
    /// `pending_dc_burn{payer="..."} 0`.
    async fn assert_line(&self, expected: &str) -> anyhow::Result<()> {
        let body = self.scrape().await?;
        if !body.contains(expected) {
            anyhow::bail!("expected: {expected} in:\n{body}");
        }
        Ok(())
    }

    async fn assert_pending_dc_burn(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> anyhow::Result<()> {
        self.assert_line(&format!(r#"pending_dc_burn{{payer="{payer}"}} {amount}"#))
            .await
    }

    /// Labels come out in the order they were given, not alphabetically.
    async fn assert_burned_at(
        &self,
        payer: &PublicKeyBinary,
        multiplier: &str,
        dc: u64,
    ) -> anyhow::Result<()> {
        self.assert_line(&format!(
            r#"burned_dc_by_multiplier{{payer="{payer}",multiplier="{multiplier}"}} {dc}"#
        ))
        .await
    }
}
