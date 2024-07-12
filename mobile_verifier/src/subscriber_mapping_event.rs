use crate::Settings;
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_source,
    verified_mapping_event::VerifiedSubscriberMappingEvent,
    FileStore, FileType,
};
use futures::stream::StreamExt;
use sqlx::{Pool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct SubscriberMappingEventDeamon {
    pool: Pool<Postgres>,
    events: Receiver<FileInfoStream<VerifiedSubscriberMappingEvent>>,
}

impl SubscriberMappingEventDeamon {
    pub fn new(
        pool: Pool<Postgres>,
        events: Receiver<FileInfoStream<VerifiedSubscriberMappingEvent>>,
    ) -> Self {
        Self { pool, events }
    }

    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_store: FileStore,
    ) -> anyhow::Result<impl ManagedTask> {
        let (events, event_server) =
            file_source::continuous_source::<VerifiedSubscriberMappingEvent, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::VerifiedSubscriberMappingEvent.to_string())
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
        file: FileInfoStream<VerifiedSubscriberMappingEvent>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Processing Verified Mapping Event file {}",
            file.file_info.key
        );
        let mut transaction = self.pool.begin().await?;
        let mut events = file.into_stream(&mut transaction).await?;

        while let Some(event) = events.next().await {
            save_event(&event, &mut transaction).await?;
        }

        transaction.commit().await?;
        Ok(())
    }
}

impl ManagedTask for SubscriberMappingEventDeamon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

pub async fn save_event(
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
