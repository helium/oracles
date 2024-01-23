use crate::{traits::MsgDecode, Error, FileInfo, FileStore, Result};
use chrono::{DateTime, Duration, Utc};
use derive_builder::Builder;
use futures::{future::LocalBoxFuture, stream::BoxStream, StreamExt};
use futures_util::TryFutureExt;
use retainer::Cache;
use std::{collections::VecDeque, marker::PhantomData, sync::Arc};
use task_manager::ManagedTask;
use tokio::sync::mpsc::{Receiver, Sender};

const DEFAULT_POLL_DURATION_SECS: i64 = 30;
const DEFAULT_POLL_DURATION: std::time::Duration =
    std::time::Duration::from_secs(DEFAULT_POLL_DURATION_SECS as u64);
const CLEAN_DURATION: std::time::Duration = std::time::Duration::from_secs(12 * 60 * 60);
const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(3 * 60 * 60);

type MemoryFileCache = Arc<Cache<String, bool>>;

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
    data: Vec<T>,
}

impl<T> FileInfoStream<T>
where
    T: Send,
{
    pub fn new(process_name: String, file_info: FileInfo, data: Vec<T>) -> Self {
        Self {
            file_info,
            process_name,
            data,
        }
    }

    pub async fn into_stream(
        self,
        recorder: impl FileInfoPollerStateRecorder,
    ) -> Result<BoxStream<'static, T>>
    where
        T: 'static,
    {
        recorder.record(&self.process_name, &self.file_info).await?;
        Ok(futures::stream::iter(self.data.into_iter()).boxed())
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
    #[builder(default = "5")]
    queue_size: usize,
    #[builder(default = r#""default".to_string()"#)]
    process_name: String,
    #[builder(setter(skip))]
    p: PhantomData<T>,
}

#[derive(Clone)]
pub struct FileInfoPollerServer<T, S> {
    config: FileInfoPollerConfig<T, S>,
    sender: Sender<FileInfoStream<T>>,
    file_queue: VecDeque<FileInfo>,
    latest_file_timestamp: Option<DateTime<Utc>>,
    cache: MemoryFileCache,
}

type FileInfoStreamReceiver<T> = Receiver<FileInfoStream<T>>;
impl<T, S> FileInfoPollerConfigBuilder<T, S>
where
    T: Clone,
    S: FileInfoPollerState,
{
    pub async fn create(self) -> Result<(FileInfoStreamReceiver<T>, FileInfoPollerServer<T, S>)> {
        let config = self.build()?;
        let (sender, receiver) = tokio::sync::mpsc::channel(config.queue_size);
        let latest_file_timestamp = config
            .state
            .latest_timestamp(&config.process_name, &config.prefix)
            .await?;

        Ok((
            receiver,
            FileInfoPollerServer {
                config,
                sender,
                file_queue: VecDeque::new(),
                latest_file_timestamp,
                cache: create_cache(),
            },
        ))
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

    async fn get_next_file(&mut self) -> Result<FileInfo> {
        loop {
            if let Some(file_info) = self.file_queue.pop_front() {
                return Ok(file_info);
            }

            let after = self.after(self.latest_file_timestamp);
            let before = Utc::now();
            let files = self
                .config
                .store
                .list_all(&self.config.prefix, after, before)
                .await?;

            for file in files {
                if !self.is_already_processed(&file).await? {
                    self.latest_file_timestamp = Some(file.timestamp);
                    self.file_queue.push_back(file);
                }
            }

            if self.file_queue.is_empty() {
                tokio::time::sleep(self.poll_duration()).await;
            }
        }
    }

    async fn run(mut self, shutdown: triggered::Listener) -> Result {
        let mut cleanup_trigger = tokio::time::interval(CLEAN_DURATION);
        let process_name = self.config.process_name.clone();

        tracing::info!(
            r#type = self.config.prefix,
            %process_name,
            "starting FileInfoPoller",
        );

        let sender = self.sender.clone();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!(r#type = self.config.prefix, %process_name, "stopping FileInfoPoller");
                    break;
                }
                _ = cleanup_trigger.tick() => self.clean(&self.cache).await?,
                result = futures::future::try_join(sender.reserve().map_err(Error::from), self.get_next_file()) => {
                    let (permit, file) = result?;
                    let data = parse_file(&self.config.store, process_name.clone(), file.clone()).await?;
                    permit.send(data);
                    cache_file(&self.cache, &file).await;
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

    async fn is_already_processed(&self, file_info: &FileInfo) -> Result<bool> {
        if self.cache.get(&file_info.key).await.is_some() {
            Ok(true)
        } else {
            self.config
                .state
                .exists(&self.config.process_name, file_info)
                .await
        }
    }
}

async fn parse_file<T>(
    store: &FileStore,
    process_name: String,
    file: FileInfo,
) -> Result<FileInfoStream<T>>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    let stream: Vec<T> = store
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
        .collect()
        .await;

    Ok(FileInfoStream::new(process_name, file, stream))
}

fn create_cache() -> MemoryFileCache {
    Arc::new(Cache::new())
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
