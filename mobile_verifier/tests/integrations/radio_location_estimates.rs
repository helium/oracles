use crate::common::MockAuthorizationClient;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    radio_location_estimates::{Entity, RadioLocationEstimate, RadioLocationEstimatesReq},
    radio_location_estimates_ingest_report::RadioLocationEstimatesIngestReport,
    FileInfo,
};
use h3o::{CellIndex, LatLng};
use helium_crypto::{KeyTag, Keypair, PublicKeyBinary};
use mobile_verifier::radio_location_estimates::{
    clear_invalidated, hash_key, RadioLocationEstimatesDaemon,
};
use rand::rngs::OsRng;
use rust_decimal::prelude::FromPrimitive;
use sqlx::{PgPool, Pool, Postgres, Row};

#[sqlx::test]
async fn verifier_test(pool: PgPool) -> anyhow::Result<()> {
    let task_pool = pool.clone();
    let (reports_tx, reports_rx) = tokio::sync::mpsc::channel(10);
    let (sink_tx, _sink_rx) = tokio::sync::mpsc::channel(10);
    let (trigger, listener) = triggered::trigger();

    tokio::spawn(async move {
        let deamon = RadioLocationEstimatesDaemon::new(
            task_pool,
            MockAuthorizationClient::new(),
            reports_rx,
            FileSinkClient::new(sink_tx, "metric"),
        );

        deamon.run(listener).await.expect("failed to complete task");
    });

    // Sending reports as if they are coming from ingestor
    let (fis, reports) = file_info_stream();
    reports_tx.send(fis).await?;

    let mut retry = 0;
    const MAX_RETRIES: u32 = 3;
    const RETRY_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

    let mut expected_n = 0;
    for report in &reports {
        expected_n += report.report.estimates.len();
    }

    while retry <= MAX_RETRIES {
        let saved_estimates = select_radio_location_estimates(&pool).await?;

        // Check that we have expected (2) number of estimates saved in DB
        // 1 should be invalidated and other should be valid
        // We know the order (invalid first becase we order by in select_radio_location_estimates)
        if expected_n == saved_estimates.len() {
            compare_report_and_estimate(&reports[0], &saved_estimates[0], false);
            compare_report_and_estimate(&reports[1], &saved_estimates[1], true);
            break;
        } else {
            retry += 1;
            tokio::time::sleep(RETRY_WAIT).await;
        }
    }

    assert!(
        retry <= MAX_RETRIES,
        "Exceeded maximum retries: {}",
        MAX_RETRIES
    );

    // Now clear invalidated estimates there should be only 1 left in DB
    let mut tx = pool.begin().await?;
    clear_invalidated(&mut tx, &Utc::now()).await?;
    tx.commit().await?;

    let leftover_estimates = select_radio_location_estimates(&pool).await?;
    assert_eq!(1, leftover_estimates.len());
    // Check that we have the right estimate left over
    compare_report_and_estimate(&reports[1], &leftover_estimates[0], true);

    trigger.trigger();

    Ok(())
}

fn file_info_stream() -> (
    FileInfoStream<RadioLocationEstimatesIngestReport>,
    Vec<RadioLocationEstimatesIngestReport>,
) {
    let file_info = FileInfo {
        key: "test_file_info".to_string(),
        prefix: "verified_mapping_event".to_string(),
        timestamp: Utc::now(),
        size: 0,
    };

    let carrier_key_pair = generate_keypair();
    let carrier_public_key_binary: PublicKeyBinary =
        carrier_key_pair.public_key().to_owned().into();

    let hotspot_key_pair = generate_keypair();
    let hotspot_public_key_binary: PublicKeyBinary =
        hotspot_key_pair.public_key().to_owned().into();

    let entity = Entity::WifiPubKey(hotspot_public_key_binary);

    let reports = vec![
        RadioLocationEstimatesIngestReport {
            received_timestamp: Utc::now() - Duration::hours(1),
            report: RadioLocationEstimatesReq {
                entity: entity.clone(),
                estimates: vec![RadioLocationEstimate {
                    hex: LatLng::new(0.1, 0.1)
                        .unwrap()
                        .to_cell(h3o::Resolution::Twelve),
                    grid_distance: 2,
                    confidence: rust_decimal::Decimal::from_f32(0.1).unwrap(),
                }],
                timestamp: Utc::now() - Duration::hours(1),
                carrier_key: carrier_public_key_binary.clone(),
            },
        },
        RadioLocationEstimatesIngestReport {
            received_timestamp: Utc::now(),
            report: RadioLocationEstimatesReq {
                entity: entity.clone(),
                estimates: vec![RadioLocationEstimate {
                    hex: LatLng::new(0.2, 0.2)
                        .unwrap()
                        .to_cell(h3o::Resolution::Twelve),
                    grid_distance: 2,
                    confidence: rust_decimal::Decimal::from_f32(0.2).unwrap(),
                }],
                timestamp: Utc::now(),
                carrier_key: carrier_public_key_binary.clone(),
            },
        },
    ];
    (
        FileInfoStream::new("default".to_string(), file_info, reports.clone()),
        reports,
    )
}

fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}

fn timestamp_match(dt1: DateTime<Utc>, dt2: DateTime<Utc>) -> bool {
    let difference = dt1.signed_duration_since(dt2);
    difference.num_seconds().abs() < 1
}

fn compare_report_and_estimate(
    report: &RadioLocationEstimatesIngestReport,
    estimate: &RadioLocationEstimateDB,
    should_be_valid: bool,
) {
    assert_eq!(
        hash_key(
            &report.report.entity,
            report.received_timestamp,
            report.report.estimates[0].hex,
            report.report.estimates[0].grid_distance,
        ),
        estimate.hashed_key
    );

    assert_eq!(report.report.entity.to_string(), estimate.radio_key);
    assert!(timestamp_match(
        report.received_timestamp,
        estimate.received_timestamp
    ));
    assert_eq!(report.report.estimates[0].hex, estimate.hex);
    assert_eq!(
        report.report.estimates[0].grid_distance,
        estimate.grid_distance
    );
    assert_eq!(report.report.estimates[0].confidence, estimate.confidence);

    if should_be_valid {
        assert!(estimate.invalidated_at.is_none());
    } else {
        assert!(estimate.invalidated_at.is_some());
    }
}

#[derive(Debug)]
pub struct RadioLocationEstimateDB {
    pub hashed_key: String,
    pub radio_key: String,
    pub received_timestamp: DateTime<Utc>,
    pub hex: CellIndex,
    pub grid_distance: u32,
    pub confidence: rust_decimal::Decimal,
    pub invalidated_at: Option<DateTime<Utc>>,
}

pub async fn select_radio_location_estimates(
    pool: &Pool<Postgres>,
) -> anyhow::Result<Vec<RadioLocationEstimateDB>> {
    let rows = sqlx::query(
        r#"
        SELECT hashed_key, radio_key, hashed_key, received_timestamp, hex, grid_distance, confidence, invalidated_at
        FROM radio_location_estimates
        ORDER BY received_timestamp ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let estimates = rows
        .into_iter()
        .map(|row| {
            let hex = row.get::<i64, _>("hex") as u64;
            let hex = CellIndex::try_from(hex).expect("valid Cell Index");
            RadioLocationEstimateDB {
                hashed_key: row.get("hashed_key"),
                radio_key: row.get("radio_key"),
                received_timestamp: row.get("received_timestamp"),
                hex,
                grid_distance: row.get::<i64, _>("grid_distance") as u32,
                confidence: row.get("confidence"),
                invalidated_at: row.try_get("invalidated_at").ok(),
            }
        })
        .collect();

    Ok(estimates)
}
