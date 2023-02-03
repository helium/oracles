use std::marker::PhantomData;

use crate::{traits::MsgDecode, Error, FileInfo, FileStore, FileType, Result};
use chrono::{DateTime, Duration, Utc};
use derive_builder::Builder;
use futures::{stream::BoxStream, StreamExt};
use retainer::Cache;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

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
        db_insert(transaction, self.file_info).await?;
        Ok(self.stream)
    }
}

#[derive(Debug, Clone, Builder)]
pub struct FileInfoPoller<T> {
    #[builder(default = "Duration::seconds(DEFAULT_POLL_DURATION_SECS)")]
    poll_duration: Duration,
    db: sqlx::Pool<sqlx::Postgres>,
    store: FileStore,
    file_type: FileType,
    start_after: DateTime<Utc>,
    #[builder(default = "None")]
    max_lookback: Option<Duration>,
    #[builder(default = "Duration::minutes(10)")]
    offset: Duration,
    #[builder(setter(skip))]
    p: PhantomData<T>,
}

impl<T> FileInfoPoller<T>
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    pub async fn start(
        self,
        shutdown: triggered::Listener,
    ) -> Result<(Receiver<FileInfoStream<T>>, JoinHandle<Result>)> {
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let join_handle = tokio::spawn(async move { self.run(shutdown, sender).await });

        Ok((receiver, join_handle))
    }

    fn after(&self, latest: DateTime<Utc>) -> DateTime<Utc> {
        self.max_lookback
            .map(|max_lookback| Utc::now() - max_lookback)
            .map(|max_ts| max_ts.max(latest))
            .unwrap_or(latest)
    }

    async fn run(self, shutdown: triggered::Listener, sender: Sender<FileInfoStream<T>>) -> Result {
        let cache = create_cache();
        let mut poll_trigger = tokio::time::interval(self.poll_duration());
        let mut cleanup_trigger = tokio::time::interval(CLEAN_DURATION);

        let mut latest_ts = db_latest_ts(&self.db, self.start_after, self.file_type).await?;

        loop {
            let after = self.after(latest_ts - self.offset);
            let before = Utc::now();
            let shutdown = shutdown.clone();

            tokio::select! {
                _ = shutdown => break,
                _ = cleanup_trigger.tick() => self.clean(&cache).await?,
                _ = poll_trigger.tick() => {
                    let files = self.store.list_all(self.file_type, after, before).await?;
                    for file in files {
                        if !is_already_processed(&self.db, &cache, &file).await? {
                            latest_ts = file.timestamp;
                            cache_file(&cache, &file).await;
                            send_stream(&sender, &self.store, file).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn clean(&self, cache: &MemoryFileCache) -> Result {
        cache.purge(4, 0.25).await;
        db_clean(&self.db, &self.file_type).await?;
        Ok(())
    }

    fn poll_duration(&self) -> std::time::Duration {
        self.poll_duration
            .to_std()
            .unwrap_or_else(|_| DEFAULT_POLL_DURATION)
    }
}

async fn send_stream<T>(
    sender: &Sender<FileInfoStream<T>>,
    store: &FileStore,
    file: FileInfo,
) -> Result
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

    let _ = sender.send(incoming_data_stream).await;
    Ok(())
}

fn create_cache() -> MemoryFileCache {
    Cache::new()
}

async fn is_already_processed(
    db: impl sqlx::PgExecutor<'_>,
    cache: &MemoryFileCache,
    file_info: &FileInfo,
) -> Result<bool> {
    if let Some(_) = cache.get(&file_info.key).await {
        Ok(true)
    } else {
        db_exists(db, file_info).await
    }
}

async fn cache_file(cache: &MemoryFileCache, file_info: &FileInfo) {
    cache.insert(file_info.key.clone(), true, CACHE_TTL).await;
}

async fn db_latest_ts(
    db: impl sqlx::PgExecutor<'_>,
    start_after: DateTime<Utc>,
    file_type: FileType,
) -> Result<DateTime<Utc>> {
    Ok(sqlx::query_scalar::<_, DateTime<Utc>>(
        r#"
        SELECT COALESCE(MAX(file_timestamp), $1) FROM incoming_files where file_type = $2
        "#,
    )
    .bind(start_after)
    .bind(file_type.to_str())
    .fetch_one(db)
    .await?)
}

async fn db_exists(db: impl sqlx::PgExecutor<'_>, file_info: &FileInfo) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(SELECT 1 from incoming_files where file_name = $1)
        "#,
    )
    .bind(file_info.key.clone())
    .fetch_one(db)
    .await?)
}

async fn db_insert(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, file_info: FileInfo) -> Result {
    sqlx::query(r#"
        INSERT INTO incoming_files(file_name, file_type, file_timestamp, processed_at) VALUES($1, $2, $3, $4)
        "#)
    .bind(file_info.key)
    .bind(file_info.file_type.to_str())
    .bind(file_info.timestamp)
    .bind(Utc::now())
    .execute(tx)
    .await?;

    Ok(())
}

async fn db_clean(db: impl sqlx::PgExecutor<'_>, file_type: &FileType) -> Result {
    sqlx::query(
        r#"
        DELETE FROM incoming_files where file_name in (
            SELECT file_name
            FROM incoming_files
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

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use sqlx::postgres::PgPoolOptions;

    use crate::iot_beacon_report::IotBeaconIngestReport;
    use crate::file_source::continuous_source;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stuff() {
        let (_shutdown_trigger, shutdown_listener) = triggered::trigger();
        let pool = PgPoolOptions::new()
            .connect("psql://postgres:password@localhost/iot_packet_verifier")
            .await
            .expect("db connection");

        let settings = crate::Settings {
            bucket: "devnet-iot-ingest".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
        };

        let file_store = FileStore::from_settings(&settings)
            .await
            .expect("File Store");

        let (mut receiver, _join_handle) = continuous_source::<IotBeaconIngestReport>()
            .db(pool.clone())
            .store(file_store)
            .file_type(FileType::IotBeaconIngestReport)
            .start_after(Utc.timestamp_millis(1675407541306))
            .poll_duration(Duration::seconds(1))
            .build()
            .expect("Poller")
            .start(shutdown_listener)
            .await
            .expect("Start poller");

        loop {
            tokio::select! {
                msg = receiver.recv() => match msg {
                    Some(msg) => {
                        let mut tx = pool.begin().await.expect("TX BEGIN");
                        let file_info = msg.file_info.clone();
                        let count = msg.into_stream(&mut tx).await.expect("Stream")
                            .fold(0, |total, _| async move { total + 1 })
                            .await;

                        println!("{:?} --> {count}", file_info);
                        // msg.mark_done(&mut tx).await.expect("MARK DONE");

                        tx.commit().await.expect("TX COMMIT");
                    }
                    None => println!("WTF"),
                }
            }
        }
    }
}
