use chrono::{Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream, verified_mapping_event::VerifiedSubscriberMappingEvent,
    FileInfo,
};
use mobile_verifier::subscriber_mapping_event::{
    aggregate_verified_mapping_events, SubscriberMappingEventDeamon, VerifiedMappingEventShare,
    VerifiedMappingEventShares,
};
use sqlx::{PgPool, Pool, Postgres};
use std::{collections::HashMap, ops::Range};

#[sqlx::test]
async fn main_test(pool: PgPool) -> anyhow::Result<()> {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (trigger, listener) = triggered::trigger();
    let task_pool = pool.clone();

    tokio::spawn(async move {
        let deamon = SubscriberMappingEventDeamon::new(task_pool, rx);
        deamon.run(listener).await.expect("failed to complete task");
    });

    let (fis, vsmes) = file_info_stream();
    tx.send(fis).await?;

    let mut retry = 0;
    const MAX_RETRIES: u32 = 10;
    const RETRY_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
    while retry <= MAX_RETRIES {
        let saved_vmes = select_events(&pool).await?;
        if vsmes.len() == saved_vmes.len() {
            assert!(vsmes.iter().all(|e| saved_vmes.contains(e)));
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
    let mut avmes = aggregate_verified_mapping_events(&pool, &reward_period).await?;

    assert_eq!(
        vsmes_to_shares(vsmes).sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id)),
        avmes.sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id))
    );

    trigger.trigger();

    Ok(())
}

fn file_info_stream() -> (
    FileInfoStream<VerifiedSubscriberMappingEvent>,
    Vec<VerifiedSubscriberMappingEvent>,
) {
    let file_info = FileInfo {
        key: "test_file_info".to_string(),
        prefix: "verified_mapping_event".to_string(),
        timestamp: Utc::now(),
        size: 0,
    };

    let vsmes = vec![
        VerifiedSubscriberMappingEvent {
            subscriber_id: vec![0],
            total_reward_points: 100,
            timestamp: Utc::now(),
        },
        VerifiedSubscriberMappingEvent {
            subscriber_id: vec![1],
            total_reward_points: 101,
            timestamp: Utc::now() - Duration::seconds(10),
        },
        VerifiedSubscriberMappingEvent {
            subscriber_id: vec![1],
            total_reward_points: 99,
            timestamp: Utc::now(),
        },
    ];
    (
        FileInfoStream::new("default".to_string(), file_info, vsmes.clone()),
        vsmes,
    )
}

fn vsmes_to_shares(vsmes: Vec<VerifiedSubscriberMappingEvent>) -> VerifiedMappingEventShares {
    let mut reward_map: HashMap<Vec<u8>, i64> = HashMap::new();

    for vsme in vsmes {
        let entry = reward_map.entry(vsme.subscriber_id).or_insert(0);
        *entry += vsme.total_reward_points as i64;
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
