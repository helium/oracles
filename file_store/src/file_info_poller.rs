use crate::{traits::MsgDecode, Error, FileInfo, FileStore, Result};
use chrono::{DateTime, Duration, Utc};
use derive_builder::Builder;
use futures::{future::LocalBoxFuture, stream::BoxStream, StreamExt};
use futures_util::TryFutureExt;
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

#[async_trait::async_trait]
pub trait FileInfoPollerState: Send + Sync + 'static {
    async fn latest_timestamp(
        &self,
        process_name: &str,
        file_type: &str,
    ) -> Result<Option<DateTime<Utc>>>;

    async fn exists(&self, process_name: &str, file_info: &FileInfo) -> Result<bool>;

    async fn clean(&self, process_name: &str, file_type: &str) -> Result;
}

#[async_trait::async_trait]
pub trait FileInfoPollerStateRecorder {
    async fn record(self, process_name: &str, file_info: &FileInfo) -> Result;
}

pub struct FileInfoStream<T> {
    pub file_info: FileInfo,
    process_name: String,
    stream: BoxStream<'static, T>,
}

impl<T> FileInfoStream<T>
where
    T: Send,
{
    pub fn new(process_name: String, file_info: FileInfo, stream: BoxStream<'static, T>) -> Self {
        Self {
            file_info,
            process_name,
            stream,
        }
    }

    pub async fn into_stream(
        self,
        recorder: impl FileInfoPollerStateRecorder,
    ) -> Result<BoxStream<'static, T>> {
        recorder.record(&self.process_name, &self.file_info).await?;
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
pub struct FileInfoPollerConfig<T, S> {
    #[builder(default = "Duration::seconds(DEFAULT_POLL_DURATION_SECS)")]
    poll_duration: Duration,
    state: S,
    store: FileStore,
    prefix: String,
    lookback: LookbackBehavior,
    #[builder(default = "Duration::minutes(10)")]
    offset: Duration,
    #[builder(default = "20")]
    queue_size: usize,
    #[builder(default = r#""default".to_string()"#)]
    process_name: String,
    #[builder(setter(skip))]
    p: PhantomData<T>,
}

#[derive(Debug, Clone)]
pub struct FileInfoPollerServer<T, S> {
    config: FileInfoPollerConfig<T, S>,
    sender: Sender<FileInfoStream<T>>,
}

type FileInfoStreamReceiver<T> = Receiver<FileInfoStream<T>>;
impl<T, S> FileInfoPollerConfigBuilder<T, S>
where
    T: Clone,
{
    pub fn create(self) -> Result<(FileInfoStreamReceiver<T>, FileInfoPollerServer<T, S>)> {
        let config = self.build()?;
        let (sender, receiver) = tokio::sync::mpsc::channel(config.queue_size);
        Ok((receiver, FileInfoPollerServer { config, sender }))
    }
}

impl<T, S> ManagedTask for FileInfoPollerServer<T, S>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
    S: FileInfoPollerState,
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

impl<T, S> FileInfoPollerServer<T, S>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
    S: FileInfoPollerState,
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
        let process_name = self.config.process_name.clone();

        let mut latest_ts = self
            .config
            .state
            .latest_timestamp(&self.config.process_name, &self.config.prefix)
            .await?;
        tracing::info!(
            r#type = self.config.prefix,
            %process_name,
            "starting FileInfoPoller",
        );

        loop {
            let after = self.after(latest_ts);
            let before = Utc::now();

            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!(r#type = self.config.prefix, %process_name, "stopping FileInfoPoller");
                    break;
                }
                _ = cleanup_trigger.tick() => self.clean(&cache).await?,
                _ = poll_trigger.tick() => {
                    let files = self.config.store.list_all(&self.config.prefix, after, before).await?;
                    for file in files {
                        if !is_already_processed(&self.config.state, &cache, &process_name, &file).await? {
                            if send_stream(&self.sender, &self.config.store, process_name.clone(), file.clone()).await? {
                                latest_ts = Some(file.timestamp);
                                cache_file(&cache, &file).await;
                            } else {
                                tracing::info!(r#type = self.config.prefix, %process_name, "FileInfoPoller: channel full");
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
        self.config
            .state
            .clean(&self.config.process_name, &self.config.prefix)
            .await?;
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
    process_name: String,
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

    let incoming_data_stream = FileInfoStream::new(process_name, file, stream);
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
    state: &impl FileInfoPollerState,
    cache: &MemoryFileCache,
    process_name: &str,
    file_info: &FileInfo,
) -> Result<bool> {
    if cache.get(&file_info.key).await.is_some() {
        Ok(true)
    } else {
        state.exists(process_name, file_info).await
    }
}

async fn cache_file(cache: &MemoryFileCache, file_info: &FileInfo) {
    cache.insert(file_info.key.clone(), true, CACHE_TTL).await;
}

#[cfg(feature = "sqlx-postgres")]
#[async_trait::async_trait]
impl FileInfoPollerStateRecorder for &mut sqlx::Transaction<'_, sqlx::Postgres> {
    async fn record(self, process_name: &str, file_info: &FileInfo) -> Result {
        sqlx::query(
            r#"
                INSERT INTO files_processed(process_name, file_name, file_type, file_timestamp, processed_at) VALUES($1, $2, $3, $4, $5)
            "#)
            .bind(process_name)
            .bind(&file_info.key)
            .bind(&file_info.prefix)
            .bind(file_info.timestamp)
            .bind(Utc::now())
            .execute(self)
            .await
            .map(|_| ())
            .map_err(Error::from)
    }
}

#[cfg(feature = "sqlx-postgres")]
#[async_trait::async_trait]
impl FileInfoPollerState for sqlx::Pool<sqlx::Postgres> {
    async fn latest_timestamp(
        &self,
        process_name: &str,
        file_type: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            r#"
                SELECT MAX(file_timestamp) FROM files_processed where process_name = $1 and file_type = $2
            "#,
            )
            .bind(process_name)
            .bind(file_type)
            .fetch_one(self)
            .await
            .map_err(Error::from)
    }

    async fn exists(&self, process_name: &str, file_info: &FileInfo) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
                SELECT EXISTS(SELECT 1 from files_processed where process_name = $1 and file_name = $2)
            "#,
            )
            .bind(process_name)
            .bind(&file_info.key)
            .fetch_one(self)
            .await
            .map_err(Error::from)
    }

    async fn clean(&self, process_name: &str, file_type: &str) -> Result {
        sqlx::query(
            r#"
                DELETE FROM files_processed where file_name in (
                    SELECT file_name
                    FROM files_processed
                    WHERE process_name = $1 and file_type = $2
                    ORDER BY file_timestamp DESC
                    OFFSET 100
                )
            "#,
        )
        .bind(process_name)
        .bind(file_type)
        .execute(self)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }
}
