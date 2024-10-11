use crate::common::MockAuthorizationClient;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    radio_location_estimates::{
        RadioLocationEstimate, RadioLocationEstimateEvent, RadioLocationEstimatesReq,
    },
    radio_location_estimates_ingest_report::RadioLocationEstimatesIngestReport,
    FileInfo,
};
use helium_crypto::{KeyTag, Keypair, PublicKeyBinary};
use mobile_verifier::radio_location_estimates::{
    clear_invalided, hash_key, RadioLocationEstimatesDaemon,
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
    let (fis, reports, _public_key_binary) = file_info_stream();
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
    clear_invalided(&mut tx, &Utc::now()).await?;
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
    PublicKeyBinary,
) {
    let file_info = FileInfo {
        key: "test_file_info".to_string(),
        prefix: "verified_mapping_event".to_string(),
        timestamp: Utc::now(),
        size: 0,
    };

    let key_pair = generate_keypair();
    let public_key_binary: PublicKeyBinary = key_pair.public_key().to_owned().into();

    let reports = vec![
        RadioLocationEstimatesIngestReport {
            received_timestamp: Utc::now() - Duration::hours(1),
            report: RadioLocationEstimatesReq {
                radio_id: "radio_1".to_string(),
                estimates: vec![RadioLocationEstimate {
                    radius: rust_decimal::Decimal::from_f32(0.1).unwrap(),
                    lat: rust_decimal::Decimal::from_f32(0.1).unwrap(),
                    long: rust_decimal::Decimal::from_f32(-0.1).unwrap(),
                    confidence: rust_decimal::Decimal::from_f32(0.1).unwrap(),
                    events: vec![RadioLocationEstimateEvent {
                        id: "event_1".to_string(),
                        timestamp: Utc::now() - Duration::hours(1),
                    }],
                }],
                timestamp: Utc::now() - Duration::hours(1),
                carrier_key: public_key_binary.clone(),
            },
        },
        RadioLocationEstimatesIngestReport {
            received_timestamp: Utc::now(),
            report: RadioLocationEstimatesReq {
                radio_id: "radio_1".to_string(),
                estimates: vec![RadioLocationEstimate {
                    radius: rust_decimal::Decimal::from_f32(0.2).unwrap(),
                    lat: rust_decimal::Decimal::from_f32(0.2).unwrap(),
                    long: rust_decimal::Decimal::from_f32(-0.2).unwrap(),
                    confidence: rust_decimal::Decimal::from_f32(0.2).unwrap(),
                    events: vec![RadioLocationEstimateEvent {
                        id: "event_1".to_string(),
                        timestamp: Utc::now(),
                    }],
                }],
                timestamp: Utc::now(),
                carrier_key: public_key_binary.clone(),
            },
        },
    ];
    (
        FileInfoStream::new("default".to_string(), file_info, reports.clone()),
        reports,
        public_key_binary,
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
            report.report.radio_id.clone(),
            report.received_timestamp,
            report.report.estimates[0].radius,
            report.report.estimates[0].lat,
            report.report.estimates[0].long
        ),
        estimate.hashed_key
    );

    assert_eq!(report.report.radio_id, estimate.radio_id);
    assert!(timestamp_match(
        report.received_timestamp,
        estimate.received_timestamp
    ));
    assert_eq!(report.report.estimates[0].radius, estimate.radius);
    assert_eq!(report.report.estimates[0].lat, estimate.lat);
    assert_eq!(report.report.estimates[0].long, estimate.long);
    assert_eq!(report.report.estimates[0].confidence, estimate.confidence);

    if should_be_valid {
        assert!(estimate.invalided_at.is_none());
    } else {
        assert!(estimate.invalided_at.is_some());
    }
}

#[derive(Debug)]
pub struct RadioLocationEstimateDB {
    pub hashed_key: String,
    pub radio_id: String,
    pub received_timestamp: DateTime<Utc>,
    pub radius: rust_decimal::Decimal,
    pub lat: rust_decimal::Decimal,
    pub long: rust_decimal::Decimal,
    pub confidence: rust_decimal::Decimal,
    pub invalided_at: Option<DateTime<Utc>>,
}

pub async fn select_radio_location_estimates(
    pool: &Pool<Postgres>,
) -> anyhow::Result<Vec<RadioLocationEstimateDB>> {
    let rows = sqlx::query(
        r#"
        SELECT hashed_key, radio_id, received_timestamp, radius, lat, long, confidence, invalided_at
        FROM radio_location_estimates
        ORDER BY received_timestamp ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let estimates = rows
        .into_iter()
        .map(|row| RadioLocationEstimateDB {
            hashed_key: row.get("hashed_key"),
            radio_id: row.get("radio_id"),
            received_timestamp: row.get("received_timestamp"),
            radius: row.get("radius"),
            lat: row.get("lat"),
            long: row.get("long"),
            confidence: row.get("confidence"),
            invalided_at: row.try_get("invalided_at").ok(),
        })
        .collect();

    Ok(estimates)
}
