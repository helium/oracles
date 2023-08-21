use crate::{traits::MsgDecode, Error, FileInfo, FileStore, FileType, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use derive_builder::Builder;
use futures::{future::LocalBoxFuture, stream::BoxStream, StreamExt, TryFutureExt};
use retainer::Cache;
use std::marker::PhantomData;
use task_manager::ManagedTask;
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender};

const DEFAULT_POLL_DURATION_SECS: i64 = 30;
const DEFAULT_POLL_DURATION: std::time::Duration =
    std::time::Duration::from_secs(DEFAULT_POLL_DURATION_SECS as u64);
const CLEAN_DURATION: std::time::Duration = std::time::Duration::from_secs(12 * 60 * 60);
const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(3 * 60 * 60);

type MemoryFileCache = Cache<String, bool>;

pub struct FileInfoStream<T> {
    pub file_info: FileInfo,
    stream: BoxStream<'static, T>,
}

impl<T> FileInfoStream<T>
where
    T: Send,
{
    pub async fn into_stream(
        self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<BoxStream<'static, T>> {
        db::insert(transaction, self.file_info).await?;
        Ok(self.stream)
    }
}

#[derive(Debug, Clone)]
pub enum LookbackBehavior {
    StartAfter(DateTime<Utc>),
    Max(Duration),
}

#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
pub struct FileInfoPollerConfig<T> {
    #[builder(default = "Duration::seconds(DEFAULT_POLL_DURATION_SECS)")]
    poll_duration: Duration,
    db: sqlx::Pool<sqlx::Postgres>,
    store: FileStore,
    file_type: FileType,
    lookback: LookbackBehavior,
    #[builder(default = "Duration::minutes(10)")]
    offset: Duration,
    #[builder(default = "20")]
    queue_size: usize,
    #[builder(setter(skip))]
    p: PhantomData<T>,
}

#[derive(Debug, Clone)]
pub struct FileInfoPollerServer<T> {
    config: FileInfoPollerConfig<T>,
    sender: Sender<FileInfoStream<T>>,
}

impl<T> FileInfoPollerConfigBuilder<T>
where
    T: Clone,
{
    pub fn create(self) -> Result<(Receiver<FileInfoStream<T>>, FileInfoPollerServer<T>)> {
        let config = self.build()?;
        let (sender, receiver) = tokio::sync::mpsc::channel(config.queue_size);
        Ok((receiver, FileInfoPollerServer { config, sender }))
    }
}

impl<T> ManagedTask for FileInfoPollerServer<T>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));

        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<T> FileInfoPollerServer<T>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    pub async fn start(
        self,
        shutdown: triggered::Listener,
    ) -> Result<impl std::future::Future<Output = Result>> {
        let join_handle = tokio::spawn(async move { self.run(shutdown).await });
        Ok(async move {
            match join_handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(Error::from(err)),
            }
        })
    }

    async fn run(self, shutdown: triggered::Listener) -> Result {
        let cache = create_cache();
        let mut poll_trigger = tokio::time::interval(self.poll_duration());
        let mut cleanup_trigger = tokio::time::interval(CLEAN_DURATION);

        let mut latest_ts = db::latest_ts(&self.config.db, self.config.file_type).await?;
        tracing::info!(
            "starting FileInfoPoller for file type {}",
            self.config.file_type
        );

        loop {
            let after = self.after(latest_ts);
            let before = Utc::now();

            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("stopping FileInfoPoller for file type {}", self.config.file_type);
                    break;
                }
                _ = cleanup_trigger.tick() => self.clean(&cache).await?,
                _ = poll_trigger.tick() => {
                    let files = self.config.store.list_all(self.config.file_type.to_str(), after, before).await?;
                    for file in files {
                        if !is_already_processed(&self.config.db, &cache, &file).await? {
                            if send_stream(&self.sender, &self.config.store, file.clone()).await? {
                                latest_ts = Some(file.timestamp);
                                cache_file(&cache, &file).await;
                            } else {
                                tracing::info!("FileInfoPoller: channel full");
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn after(&self, latest: Option<DateTime<Utc>>) -> DateTime<Utc> {
        let latest_offset = latest.map(|lt| lt - self.config.offset);
        match self.config.lookback {
            LookbackBehavior::StartAfter(start_after) => latest_offset.unwrap_or(start_after),
            LookbackBehavior::Max(max_lookback) => {
                let max_ts = Utc::now() - max_lookback;
                latest_offset.map(|lt| lt.max(max_ts)).unwrap_or(max_ts)
            }
        }
    }

    async fn clean(&self, cache: &MemoryFileCache) -> Result {
        cache.purge(4, 0.25).await;
        db::clean(&self.config.db, &self.config.file_type).await?;
        Ok(())
    }

    fn poll_duration(&self) -> std::time::Duration {
        self.config
            .poll_duration
            .to_std()
            .unwrap_or(DEFAULT_POLL_DURATION)
    }
}

async fn send_stream<T>(
    sender: &Sender<FileInfoStream<T>>,
    store: &FileStore,
    file: FileInfo,
) -> Result<bool>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    let stream = store
        .stream_file(file.clone())
        .await?
        .filter_map(|msg| async {
            msg.map_err(|err| {
                tracing::error!(
                    "Error streaming entry in file of type {}: {err:?}",
                    std::any::type_name::<T>()
                );
                err
            })
            .ok()
        })
        .filter_map(|msg| async {
            <T as MsgDecode>::decode(msg)
                .map_err(|err| {
                    tracing::error!(
                        "Error in decoding message of type {}: {err:?}",
                        std::any::type_name::<T>()
                    );
                    err
                })
                .ok()
        })
        .boxed();

    let incoming_data_stream = FileInfoStream {
        file_info: file,
        stream,
    };

    match sender.try_send(incoming_data_stream) {
        Ok(_) => Ok(true),
        Err(TrySendError::Full(_)) => Ok(false),
        Err(TrySendError::Closed(_)) => Err(Error::channel()),
    }
}

fn create_cache() -> MemoryFileCache {
    Cache::new()
}

async fn is_already_processed(
    db: impl sqlx::PgExecutor<'_>,
    cache: &MemoryFileCache,
    file_info: &FileInfo,
) -> Result<bool> {
    if cache.get(&file_info.key).await.is_some() {
        Ok(true)
    } else {
        db::exists(db, file_info).await
    }
}

async fn cache_file(cache: &MemoryFileCache, file_info: &FileInfo) {
    cache.insert(file_info.key.clone(), true, CACHE_TTL).await;
}

mod db {
    use super::*;

    pub async fn latest_ts(
        db: impl sqlx::PgExecutor<'_>,
        file_type: FileType,
    ) -> Result<Option<DateTime<Utc>>> {
        let default = Utc.timestamp_opt(0, 0).single().unwrap();

        let result = sqlx::query_scalar::<_, DateTime<Utc>>(
            r#"
        SELECT COALESCE(MAX(file_timestamp), $1) FROM files_processed where file_type = $2
        "#,
        )
        .bind(default)
        .bind(file_type.to_str())
        .fetch_one(db)
        .await?;

        if result == default {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    pub async fn exists(db: impl sqlx::PgExecutor<'_>, file_info: &FileInfo) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            r#"
        SELECT EXISTS(SELECT 1 from files_processed where file_name = $1)
        "#,
        )
        .bind(file_info.key.clone())
        .fetch_one(db)
        .await?)
    }

    pub async fn insert(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        file_info: FileInfo,
    ) -> Result {
        sqlx::query(r#"
        INSERT INTO files_processed(file_name, file_type, file_timestamp, processed_at) VALUES($1, $2, $3, $4)
        "#)
    .bind(file_info.key)
    .bind(&file_info.prefix)
    .bind(file_info.timestamp)
    .bind(Utc::now())
    .execute(tx)
    .await?;

        Ok(())
    }

    pub async fn clean(db: impl sqlx::PgExecutor<'_>, file_type: &FileType) -> Result {
        sqlx::query(
            r#"
        DELETE FROM files_processed where file_name in (
            SELECT file_name
            FROM files_processed
            WHERE file_type = $1
            ORDER BY file_timestamp DESC
            OFFSET 100
        )
        "#,
        )
        .bind(file_type.to_str())
        .execute(db)
        .await?;

        Ok(())
    }
}
