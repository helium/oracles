use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    subscriber_verified_mapping_event::SubscriberVerifiedMappingEvent,
    subscriber_verified_mapping_event_ingest_report::SubscriberVerifiedMappingEventIngestReport,
    FileInfo,
};
use helium_crypto::{KeyTag, Keypair, PublicKeyBinary};
use helium_proto::services::poc_mobile::VerifiedSubscriberVerifiedMappingEventIngestReportV1;
use mobile_verifier::subscriber_verified_mapping_event::{
    aggregate_verified_mapping_events, SubscriberVerifiedMappingEventDaemon,
    VerifiedSubscriberVerifiedMappingEventShare, VerifiedSubscriberVerifiedMappingEventShares,
};
use prost::Message;
use rand::rngs::OsRng;
use sqlx::{PgPool, Pool, Postgres, Row};
use std::{collections::HashMap, ops::Range};
use tokio::time::timeout;

use crate::common::{MockAuthorizationClient, MockEntityClient};

#[sqlx::test]
async fn main_test(pool: PgPool) -> anyhow::Result<()> {
    let (reports_tx, reports_rx) = tokio::sync::mpsc::channel(10);
    let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel(10);
    let (trigger, listener) = triggered::trigger();
    let task_pool = pool.clone();

    tokio::spawn(async move {
        let deamon = SubscriberVerifiedMappingEventDaemon::new(
            task_pool,
            MockAuthorizationClient::new(),
            MockEntityClient::new(),
            reports_rx,
            FileSinkClient::new(sink_tx, "metric"),
        );

        deamon.run(listener).await.expect("failed to complete task");
    });

    // Sending reports as if they are coming from ingestor
    let (fis, mut reports, public_key_binary) = file_info_stream();
    reports_tx.send(fis).await?;

    // Testing that each report we sent made it into DB
    let mut retry = 0;
    const MAX_RETRIES: u32 = 10;
    const RETRY_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
    while retry <= MAX_RETRIES {
        let mut saved_vsmes = select_events(&pool).await?;

        if reports.len() == saved_vsmes.len() {
            let now = Utc::now();

            for vsme in &mut saved_vsmes {
                // We have to do this because we do not store carrier_mapping_key in DB
                vsme.carrier_mapping_key = public_key_binary.clone();
                // We also update timestamp because github action return a different timestamp
                // just few nanoseconds later
                vsme.timestamp = now;
                println!("vsme    {:?}", vsme);
            }

            assert!(reports.iter_mut().all(|r| {
                r.report.timestamp = now;
                println!("report {:?}", r.report);
                saved_vsmes.contains(&r.report)
            }));
            break;
        } else {
            tracing::debug!("wrong saved_vsmes.len() {}", saved_vsmes.len());
            retry += 1;
            tokio::time::sleep(RETRY_WAIT).await;
        }
    }

    assert!(
        retry <= MAX_RETRIES,
        "Exceeded maximum retries: {}",
        MAX_RETRIES
    );

    // Testing that each report we sent made it on verified report FileSink
    for expected_report in reports.clone() {
        match timeout(std::time::Duration::from_secs(2), sink_rx.recv()).await {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => panic!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => panic!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => {
                    let proto_verified_report = VerifiedSubscriberVerifiedMappingEventIngestReportV1::decode(data.as_slice())
                        .expect("unable to decode into VerifiedSubscriberVerifiedMappingEventIngestReportV1");

                    let rcv_report: SubscriberVerifiedMappingEventIngestReport =
                        proto_verified_report.report.unwrap().try_into()?;

                    assert!(timestamp_match(
                        expected_report.received_timestamp,
                        rcv_report.received_timestamp
                    ));
                    assert_eq!(
                        expected_report.report.subscriber_id,
                        rcv_report.report.subscriber_id
                    );
                    assert_eq!(
                        expected_report.report.total_reward_points,
                        rcv_report.report.total_reward_points
                    );
                    assert_eq!(
                        expected_report.report.carrier_mapping_key,
                        rcv_report.report.carrier_mapping_key
                    );

                    assert!(timestamp_match(
                        expected_report.received_timestamp,
                        rcv_report.report.timestamp
                    ));
                }
            },
            Ok(None) => panic!("got none"),
            Err(reason) => panic!("got error {reason}"),
        }
    }

    // Testing aggregate_verified_mapping_events now
    let reward_period = Range {
        start: Utc::now() - Duration::days(1),
        end: Utc::now(),
    };
    let mut shares_from_reports = reports_to_shares(reports.clone());
    shares_from_reports.sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id));

    let mut shares = aggregate_verified_mapping_events(&pool, &reward_period).await?;
    shares.sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id));

    assert_eq!(shares_from_reports, shares);

    trigger.trigger();

    Ok(())
}

fn timestamp_match(dt1: DateTime<Utc>, dt2: DateTime<Utc>) -> bool {
    let difference = dt1.signed_duration_since(dt2);
    difference.num_seconds().abs() < 1
}

fn file_info_stream() -> (
    FileInfoStream<SubscriberVerifiedMappingEventIngestReport>,
    Vec<SubscriberVerifiedMappingEventIngestReport>,
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
        SubscriberVerifiedMappingEventIngestReport {
            received_timestamp: Utc::now(),
            report: SubscriberVerifiedMappingEvent {
                subscriber_id: vec![0],
                total_reward_points: 100,
                timestamp: Utc::now(),
                carrier_mapping_key: public_key_binary.clone(),
            },
        },
        SubscriberVerifiedMappingEventIngestReport {
            received_timestamp: Utc::now() - Duration::seconds(10),
            report: SubscriberVerifiedMappingEvent {
                subscriber_id: vec![1],
                total_reward_points: 101,
                timestamp: Utc::now() - Duration::seconds(10),
                carrier_mapping_key: public_key_binary.clone(),
            },
        },
        SubscriberVerifiedMappingEventIngestReport {
            received_timestamp: Utc::now(),
            report: SubscriberVerifiedMappingEvent {
                subscriber_id: vec![1],
                total_reward_points: 99,
                timestamp: Utc::now(),
                carrier_mapping_key: public_key_binary.clone(),
            },
        },
    ];
    (
        FileInfoStream::new("default".to_string(), file_info, reports.clone()),
        reports,
        public_key_binary,
    )
}

fn reports_to_shares(
    reports: Vec<SubscriberVerifiedMappingEventIngestReport>,
) -> VerifiedSubscriberVerifiedMappingEventShares {
    let mut reward_map: HashMap<Vec<u8>, i64> = HashMap::new();

    for report in reports {
        let event = report.report;
        let entry = reward_map.entry(event.subscriber_id).or_insert(0);
        *entry += event.total_reward_points as i64;
    }

    reward_map
        .into_iter()
        .map(
            |(subscriber_id, total_reward_points)| VerifiedSubscriberVerifiedMappingEventShare {
                subscriber_id,
                total_reward_points,
            },
        )
        .collect()
}

async fn select_events(
    pool: &Pool<Postgres>,
) -> anyhow::Result<Vec<SubscriberVerifiedMappingEvent>> {
    let rows = sqlx::query(
        r#"
            SELECT 
                subscriber_id,
                total_reward_points,
                received_timestamp
            FROM subscriber_verified_mapping_event
        "#,
    )
    .fetch_all(pool)
    .await?;

    let events = rows
        .into_iter()
        .map(|row| SubscriberVerifiedMappingEvent {
            subscriber_id: row.get::<Vec<u8>, _>("subscriber_id"),
            total_reward_points: row.get::<i64, _>("total_reward_points") as u64,
            timestamp: row.get::<DateTime<Utc>, _>("received_timestamp"),
            carrier_mapping_key: vec![].into(),
        })
        .collect::<Vec<SubscriberVerifiedMappingEvent>>();

    Ok(events)
}

fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}
