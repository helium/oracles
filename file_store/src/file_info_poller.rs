use crate::{file_store, traits::MsgDecode, Error, FileInfo, FileStore, Result};
use aws_sdk_s3::types::ByteStream;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use futures::{future::LocalBoxFuture, stream::BoxStream, StreamExt};
use futures_util::TryFutureExt;
use retainer::Cache;
use std::{collections::VecDeque, marker::PhantomData, sync::Arc, time::Duration};
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

    // Returns number of items cleaned
    async fn clean(
        &self,
        process_name: &str,
        file_type: &str,
        offset: DateTime<Utc>,
    ) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait FileInfoPollerParser<T>: Send + Sync + 'static {
    async fn parse(&self, stream: ByteStream) -> Result<Vec<T>>;
}

#[async_trait::async_trait]
pub trait FileInfoPollerStateRecorder {
    async fn record(&mut self, process_name: &str, file_info: &FileInfo) -> Result;
}

#[async_trait::async_trait]
pub trait FileInfoPollerStore: Send + Sync + 'static {
    async fn list_all<A, B>(&self, file_type: &str, after: A, before: B) -> Result<Vec<FileInfo>>
    where
        A: Into<Option<DateTime<Utc>>> + Send + Sync + Copy,
        B: Into<Option<DateTime<Utc>>> + Send + Sync + Copy;

    async fn get_raw<K>(&self, key: K) -> Result<ByteStream>
    where
        K: Into<String> + Send + Sync;
}

#[derive(Debug)]
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
        recorder: &mut impl FileInfoPollerStateRecorder,
    ) -> Result<BoxStream<'static, T>>
    where
        T: 'static,
    {
        let latency = Utc::now() - self.file_info.timestamp;
        metrics::gauge!(
            "file-processing-latency",
            "file-type" => self.file_info.prefix.clone(),
            "process-name" => self.process_name.clone(),
        )
        .set(latency.num_seconds() as f64);

        metrics::gauge!(
            "file-processing-timestamp",
            "file-type" => self.file_info.prefix.clone(),
            "process-name" => self.process_name.clone(),
        )
        .set(self.file_info.timestamp.timestamp_millis() as f64);

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
pub struct FileInfoPollerConfig<Message, State, Store, Parser> {
    #[builder(default = "DEFAULT_POLL_DURATION")]
    poll_duration: Duration,
    state: State,
    store: Store,
    prefix: String,
    parser: Parser,
    lookback: LookbackBehavior,
    #[builder(default = "Duration::from_secs(10 * 60)")]
    offset: Duration,
    #[builder(default = "5")]
    queue_size: usize,
    #[builder(default = r#""default".to_string()"#)]
    process_name: String,
    #[builder(setter(skip))]
    p: PhantomData<Message>,
}

#[derive(Clone)]
pub struct FileInfoPollerServer<
    Message,
    State,
    Store = FileStore,
    Parser = MsgDecodeFileInfoPollerParser,
> {
    config: FileInfoPollerConfig<Message, State, Store, Parser>,
    sender: Sender<FileInfoStream<Message>>,
    file_queue: VecDeque<FileInfo>,
    latest_file_timestamp: Option<DateTime<Utc>>,
    cache: MemoryFileCache,
}

type FileInfoStreamReceiver<T> = Receiver<FileInfoStream<T>>;

impl<Message, State, Store, Parser> FileInfoPollerConfigBuilder<Message, State, Store, Parser>
where
    Message: Clone,
    State: FileInfoPollerState,
    Parser: FileInfoPollerParser<Message>,
    Store: FileInfoPollerStore,
{
    pub async fn create(
        self,
    ) -> Result<(
        FileInfoStreamReceiver<Message>,
        FileInfoPollerServer<Message, State, Store, Parser>,
    )> {
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

impl<Message, State, Parser> ManagedTask for FileInfoPollerServer<Message, State, FileStore, Parser>
where
    Message: Send + Sync + 'static,
    State: FileInfoPollerState,
    Parser: FileInfoPollerParser<Message>,
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

impl<Message, State, Store, Parser> FileInfoPollerServer<Message, State, Store, Parser>
where
    Message: Send + Sync + 'static,
    State: FileInfoPollerState,
    Parser: FileInfoPollerParser<Message>,
    Store: FileInfoPollerStore + Send + Sync + 'static,
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
                    let byte_stream = self.config.store.get_raw(file.clone()).await?;
                    let data = self.config.parser.parse(byte_stream).await?;
                    let file_info_stream = FileInfoStream::new(process_name.clone(), file.clone(), data);

                    permit.send(file_info_stream);
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
        let cache_before = cache.len().await;
        cache.purge(4, 0.25).await;
        let cache_after = cache.len().await;

        let db_removed = self
            .config
            .state
            .clean(
                &self.config.process_name,
                &self.config.prefix,
                self.after(self.latest_file_timestamp),
            )
            .await?;

        tracing::info!(
            cache_removed = cache_before - cache_after,
            db_removed,
            "cache clean"
        );

        Ok(())
    }

    fn poll_duration(&self) -> std::time::Duration {
        self.config.poll_duration
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

pub struct MsgDecodeFileInfoPollerParser;

#[async_trait::async_trait]
impl<T> FileInfoPollerParser<T> for MsgDecodeFileInfoPollerParser
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    async fn parse(&self, byte_stream: ByteStream) -> Result<Vec<T>> {
        Ok(file_store::stream_source(byte_stream)
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
            .await)
    }
}

pub struct ProstFileInfoPollerParser;

#[async_trait::async_trait]
impl<T> FileInfoPollerParser<T> for ProstFileInfoPollerParser
where
    T: helium_proto::Message + Default,
{
    async fn parse(&self, byte_stream: ByteStream) -> Result<Vec<T>> {
        Ok(file_store::stream_source(byte_stream)
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
                <T as helium_proto::Message>::decode(msg)
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
            .await)
    }
}

fn create_cache() -> MemoryFileCache {
    Arc::new(Cache::new())
}

async fn cache_file(cache: &MemoryFileCache, file_info: &FileInfo) {
    cache.insert(file_info.key.clone(), true, CACHE_TTL).await;
}

#[async_trait::async_trait]
impl FileInfoPollerStore for FileStore {
    async fn list_all<A, B>(&self, file_type: &str, after: A, before: B) -> Result<Vec<FileInfo>>
    where
        A: Into<Option<DateTime<Utc>>> + Send + Sync + Copy,
        B: Into<Option<DateTime<Utc>>> + Send + Sync + Copy,
    {
        self.list_all(file_type, after, before).await
    }

    async fn get_raw<K>(&self, key: K) -> Result<ByteStream>
    where
        K: Into<String> + Send + Sync,
    {
        self.get_raw(key).await
    }
}

#[cfg(feature = "sqlx-postgres")]
pub mod sqlx_postgres {
    use super::*;

    use sqlx::postgres::PgQueryResult;

    #[async_trait::async_trait]
    impl FileInfoPollerStateRecorder for sqlx::Transaction<'_, sqlx::Postgres> {
        async fn record(&mut self, process_name: &str, file_info: &FileInfo) -> Result {
            sqlx::query(
            r#"
                INSERT INTO files_processed(process_name, file_name, file_type, file_timestamp, processed_at) VALUES($1, $2, $3, $4, $5)
            "#)
            .bind(process_name)
            .bind(&file_info.key)
            .bind(&file_info.prefix)
            .bind(file_info.timestamp)
            .bind(Utc::now())
            .execute(&mut **self)
            .await
            .map(|_| ())
            .map_err(Error::from)
        }
    }

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

        async fn clean(
            &self,
            process_name: &str,
            file_type: &str,
            offset: DateTime<Utc>,
        ) -> Result<u64> {
            let t100_timestamp: Option<DateTime<Utc>> = sqlx::query_scalar(
                r#"
                    SELECT file_timestamp
                    FROM files_processed
                    WHERE process_name = $1
                        AND file_type = $2
                    ORDER BY file_timestamp DESC
                    LIMIT 1 OFFSET 100;
                "#,
            )
            .bind(process_name)
            .bind(file_type)
            .fetch_optional(self)
            .await?;

            let Some(t100) = t100_timestamp else {
                // The cleaning limit has not been reached, remove nothing.
                return Ok(0);
            };

            // To keep from reprocessing files, we need to make sure rows that exist
            // within the offset window are not removed.
            let older_than_limit = t100.min(offset);

            let query_result: PgQueryResult = sqlx::query(
                r#"
                    DELETE FROM files_processed
                    WHERE process_name = $1
                        AND file_type = $2
                        AND file_timestamp < $3
                "#,
            )
            .bind(process_name)
            .bind(file_type)
            .bind(older_than_limit)
            .execute(self)
            .await
            .map_err(Error::from)?;

            Ok(query_result.rows_affected())
        }
    }

    #[cfg(test)]
    mod tests {

        use sqlx::{Executor, PgPool};
        use std::time::Duration;
        use tokio::time::timeout;

        use super::*;

        struct TestParser;
        struct TestStore(Vec<FileInfo>);

        #[async_trait::async_trait]
        impl FileInfoPollerParser<String> for TestParser {
            async fn parse(&self, _byte_stream: ByteStream) -> Result<Vec<String>> {
                Ok(vec![])
            }
        }

        #[async_trait::async_trait]
        impl FileInfoPollerStore for TestStore {
            async fn list_all<A, B>(
                &self,
                _file_type: &str,
                after: A,
                before: B,
            ) -> Result<Vec<FileInfo>>
            where
                A: Into<Option<DateTime<Utc>>> + Send + Sync + Copy,
                B: Into<Option<DateTime<Utc>>> + Send + Sync + Copy,
            {
                let after = after.into();
                let before = before.into();

                Ok(self
                    .0
                    .clone()
                    .into_iter()
                    .filter(|file_info| after.is_none_or(|v| file_info.timestamp > v))
                    .filter(|file_info| before.is_none_or(|v| file_info.timestamp <= v))
                    .collect())
            }

            async fn get_raw<K>(&self, _key: K) -> Result<ByteStream>
            where
                K: Into<String> + Send + Sync,
            {
                Ok(ByteStream::default())
            }
        }

        #[sqlx::test]
        async fn do_not_reprocess_files_when_offset_exceeds_earliest_file(
            pool: PgPool,
        ) -> anyhow::Result<()> {
            // Cleaning the files_processed table should not cause files within the
            // `FileInfoPoller.config.offset` window to be reprocessed.

            // There is no auto-migration for tests in this lib workspace.
            pool.execute(
                r#"
                CREATE TABLE files_processed (
                    process_name TEXT NOT NULL DEFAULT 'default',
                    file_name VARCHAR PRIMARY KEY,
                    file_type VARCHAR NOT NULL,
                    file_timestamp TIMESTAMPTZ NOT NULL,
                    processed_at TIMESTAMPTZ NOT NULL
                );
                "#,
            )
            .await?;

            // The important aspect of this test is that all the files to be
            // processed happen _within_ the lookback offset.
            const EXPECTED_FILE_COUNT: i64 = 150;
            let mut infos = vec![];
            for seconds in 0..EXPECTED_FILE_COUNT {
                let file_info = FileInfo {
                    key: format!("key-{seconds}"),
                    prefix: "file_type".to_string(),
                    timestamp: Utc::now() - chrono::Duration::seconds(seconds),
                    size: 42,
                };
                infos.push(file_info);
            }

            // To simulate a restart, we're going to make a new FileInfoPoller.
            // This closure is to ensure they have the same settings.
            let file_info_builder = || {
                let six_hours = chrono::Duration::hours(6).to_std().unwrap();
                FileInfoPollerConfigBuilder::<String, _, TestStore, _>::default()
                    .parser(TestParser)
                    .state(pool.clone())
                    .store(TestStore(infos.clone()))
                    .lookback(LookbackBehavior::Max(six_hours))
                    .prefix("file_type".to_string())
                    .offset(six_hours)
                    .create()
            };

            // The first startup of the file info poller, there is nothing to clean.
            // And all file_infos will be returned to be processed.
            let (mut receiver, ingest_server) = file_info_builder().await?;
            let (trigger, shutdown) = triggered::trigger();
            tokio::spawn(async move {
                if let Err(status) = ingest_server.run(shutdown).await {
                    println!("ingest server went down unexpectedly: {status:?}");
                }
            });

            // "process" all the files. They are not recorded into the database
            // until the file is consumed as a stream.
            let mut processed = 0;
            while processed < EXPECTED_FILE_COUNT {
                match timeout(Duration::from_secs(1), receiver.recv()).await? {
                    Some(msg) => {
                        processed += 1;
                        let mut txn = pool.begin().await?;
                        let _x = msg.into_stream(&mut txn).await?;
                        txn.commit().await?;
                    }
                    err => panic!("something went wrong: {err:?}"),
                };
            }

            // Shutdown the ingest server, we're going to create a new one and start it.
            trigger.trigger();

            // The second startup of the file info poller, there are 100+ files that
            // have been processed. The initial clean should not remove processed
            // files in a way that causes us to re-receive any files within our
            // offset for processing.
            let (mut receiver, ingest_server) = file_info_builder().await?;
            let (trigger, shutdown) = triggered::trigger();
            let _handle = tokio::spawn(async move {
                if let Err(status) = ingest_server.run(shutdown).await {
                    println!("ingest server went down unexpectedly: {status:?}");
                }
            });

            // Attempting to recieve files for processing. The timeout should fire,
            // because all the files we have setup exist within the offset, and
            // should still be in the database.
            match timeout(Duration::from_secs(1), receiver.recv()).await {
                Err(_err) => (),
                Ok(msg) => {
                    panic!("we got something when we expected nothing.: {msg:?}");
                }
            }

            // Shut down for great good
            trigger.trigger();

            Ok(())
        }
    }
}
