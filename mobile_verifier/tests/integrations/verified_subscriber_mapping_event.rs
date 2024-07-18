use chrono::{Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    verified_subscriber_mapping_event::VerifiedSubscriberMappingEvent,
    verified_subscriber_mapping_event_ingest_report::VerifiedSubscriberMappingEventIngestReport,
    FileInfo,
};
use helium_crypto::{KeyTag, Keypair, PublicKeyBinary};
use mobile_verifier::verified_subscriber_mapping_event::{
    aggregate_verified_mapping_events, VerifiedMappingEventShare, VerifiedMappingEventShares,
    VerifiedSubscriberMappingEventDeamon,
};
use rand::rngs::OsRng;
use sqlx::{PgPool, Pool, Postgres};
use std::{collections::HashMap, ops::Range};

#[sqlx::test]
async fn main_test(pool: PgPool) -> anyhow::Result<()> {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (trigger, listener) = triggered::trigger();
    let task_pool = pool.clone();

    tokio::spawn(async move {
        let deamon = VerifiedSubscriberMappingEventDeamon::new(task_pool, rx);
        deamon.run(listener).await.expect("failed to complete task");
    });

    let (fis, mut reports) = file_info_stream();
    tx.send(fis).await?;

    let mut retry = 0;
    const MAX_RETRIES: u32 = 10;
    const RETRY_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
    while retry <= MAX_RETRIES {
        let saved_vmes = select_events(&pool).await?;
        if reports.len() == saved_vmes.len() {
            assert!(reports.iter_mut().all(|r| {
                // We ahve to do this because we do not store verification_mapping_pubkey in DB
                r.report.verification_mapping_pubkey = vec![].into();
                saved_vmes.contains(&r.report)
            }));
            break;
        } else {
            tracing::debug!("wrong saved_vmes.len() {}", saved_vmes.len());
            retry += 1;
            tokio::time::sleep(RETRY_WAIT).await;
        }
    }

    assert!(
        retry <= MAX_RETRIES,
        "Exceeded maximum retries: {}",
        MAX_RETRIES
    );

    // Testing aggregate_verified_mapping_events now
    let reward_period = Range {
        start: Utc::now() - Duration::days(1),
        end: Utc::now(),
    };
    let mut shares_from_reports = reports_to_shares(reports);
    shares_from_reports.sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id));

    let mut shares = aggregate_verified_mapping_events(&pool, &reward_period).await?;
    shares.sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id));

    assert_eq!(shares_from_reports, shares);

    trigger.trigger();

    Ok(())
}

fn file_info_stream() -> (
    FileInfoStream<VerifiedSubscriberMappingEventIngestReport>,
    Vec<VerifiedSubscriberMappingEventIngestReport>,
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
        VerifiedSubscriberMappingEventIngestReport {
            received_timestamp: Utc::now(),
            report: VerifiedSubscriberMappingEvent {
                subscriber_id: vec![0],
                total_reward_points: 100,
                timestamp: Utc::now(),
                verification_mapping_pubkey: public_key_binary.clone(),
            },
        },
        VerifiedSubscriberMappingEventIngestReport {
            received_timestamp: Utc::now() - Duration::seconds(10),
            report: VerifiedSubscriberMappingEvent {
                subscriber_id: vec![1],
                total_reward_points: 101,
                timestamp: Utc::now() - Duration::seconds(10),
                verification_mapping_pubkey: public_key_binary.clone(),
            },
        },
        VerifiedSubscriberMappingEventIngestReport {
            received_timestamp: Utc::now(),
            report: VerifiedSubscriberMappingEvent {
                subscriber_id: vec![1],
                total_reward_points: 99,
                timestamp: Utc::now(),
                verification_mapping_pubkey: public_key_binary.clone(),
            },
        },
    ];
    (
        FileInfoStream::new("default".to_string(), file_info, reports.clone()),
        reports,
    )
}

fn reports_to_shares(
    reports: Vec<VerifiedSubscriberMappingEventIngestReport>,
) -> VerifiedMappingEventShares {
    let mut reward_map: HashMap<Vec<u8>, i64> = HashMap::new();

    for report in reports {
        let event = report.report;
        let entry = reward_map.entry(event.subscriber_id).or_insert(0);
        *entry += event.total_reward_points as i64;
    }

    reward_map
        .into_iter()
        .map(
            |(subscriber_id, total_reward_points)| VerifiedMappingEventShare {
                subscriber_id,
                total_reward_points,
            },
        )
        .collect()
}

async fn select_events(
    pool: &Pool<Postgres>,
) -> anyhow::Result<Vec<VerifiedSubscriberMappingEvent>> {
    Ok(sqlx::query_as::<_, VerifiedSubscriberMappingEvent>(
        r#"
            SELECT 
                subscriber_id,
                total_reward_points,
                timestamp
            FROM verified_mapping_event
        "#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect::<Vec<VerifiedSubscriberMappingEvent>>())
}

fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}
