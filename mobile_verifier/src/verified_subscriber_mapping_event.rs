use crate::Settings;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_source,
    verified_subscriber_mapping_event::VerifiedSubscriberMappingEvent,
    verified_subscriber_mapping_event_ingest_report::VerifiedSubscriberMappingEventIngestReport,
    FileStore, FileType,
};
use futures::{stream::StreamExt, TryStreamExt};
use sqlx::{Pool, Postgres, Transaction};
use std::ops::Range;
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct VerifiedSubscriberMappingEventDeamon {
    pool: Pool<Postgres>,
    events: Receiver<FileInfoStream<VerifiedSubscriberMappingEventIngestReport>>,
}

impl VerifiedSubscriberMappingEventDeamon {
    pub fn new(
        pool: Pool<Postgres>,
        events: Receiver<FileInfoStream<VerifiedSubscriberMappingEventIngestReport>>,
    ) -> Self {
        Self { pool, events }
    }

    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_store: FileStore,
    ) -> anyhow::Result<impl ManagedTask> {
        let (events, event_server) =
            file_source::continuous_source::<VerifiedSubscriberMappingEventIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::VerifiedSubscriberMappingEventIngestReportV1.to_string())
                .create()
                .await?;

        let task = Self::new(pool, events);

        Ok(TaskManager::builder()
            .add_task(event_server)
            .add_task(task)
            .build())
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting sme deamon");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("sme deamon shutting down");
                    break;
                }
                Some(file) = self.events.recv() => {
                    self.process_file(file).await?;
                }
            }
        }
        Ok(())
    }

    async fn process_file(
        &self,
        file: FileInfoStream<VerifiedSubscriberMappingEventIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Processing Verified Mapping Event file {}",
            file.file_info.key
        );
        let mut transaction = self.pool.begin().await?;
        let mut reports = file.into_stream(&mut transaction).await?;

        while let Some(report) = reports.next().await {
            save_event(&report.report, &mut transaction).await?;
        }

        transaction.commit().await?;
        Ok(())
    }
}

impl ManagedTask for VerifiedSubscriberMappingEventDeamon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

async fn save_event(
    event: &VerifiedSubscriberMappingEvent,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into verified_mapping_event (subscriber_id, total_reward_points, timestamp)
        values ($1, $2, $3)
        on conflict (subscriber_id, timestamp) do nothing
        "#,
    )
    .bind(&event.subscriber_id)
    .bind(event.total_reward_points as i64)
    .bind(event.timestamp)
    .execute(exec)
    .await?;

    Ok(())
}

const SUBSCRIBER_REWARD_PERIOD_IN_DAYS: i64 = 1;
pub type VerifiedMappingEventShares = Vec<VerifiedMappingEventShare>;

#[derive(sqlx::FromRow, PartialEq, Debug)]
pub struct VerifiedMappingEventShare {
    pub subscriber_id: Vec<u8>,
    pub total_reward_points: i64,
}

pub async fn aggregate_verified_mapping_events(
    db: impl sqlx::PgExecutor<'_> + Copy,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<VerifiedMappingEventShares, sqlx::Error> {
    let mut rows = sqlx::query_as::<_, VerifiedMappingEventShare>(
        "SELECT 
            subscriber_id, 
            SUM(total_reward_points) AS total_reward_points
        FROM 
            verified_mapping_event
        WHERE timestamp >= $1 AND timestamp < $2
        GROUP BY 
            subscriber_id;",
    )
    .bind(reward_period.end - Duration::days(SUBSCRIBER_REWARD_PERIOD_IN_DAYS))
    .bind(reward_period.end)
    .fetch(db);

    let mut vme_shares = VerifiedMappingEventShares::new();
    while let Some(share) = rows.try_next().await? {
        vme_shares.push(share)
    }

    Ok(vme_shares)
}
