use std::marker::PhantomData;

use crate::{traits::MsgDecode, Error, FileInfo, FileStore, FileType, Result};
use chrono::{DateTime, Duration, Utc};
use derive_builder::Builder;
use futures::{stream::BoxStream, StreamExt};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

const DEFAULT_POLL_DURATION_SECS: i64 = 30;
const CLEAN_DURATION_SECS: u64 = 12 * 60 * 60;

pub struct IncomingDataStream<T> {
    pub file_info: FileInfo,
    pub stream: BoxStream<'static, T>,
    sender: tokio::sync::oneshot::Sender<bool>,
}

#[derive(Debug, Clone, Builder)]
pub struct IncomingDataPoller<T> {
    #[builder(default = "Duration::seconds(DEFAULT_POLL_DURATION_SECS)")]
    poll_duration: Duration,
    db: sqlx::Pool<sqlx::Postgres>,
    store: FileStore,
    file_type: FileType,
    start_after: DateTime<Utc>,
    #[builder(default = "Duration::minutes(10)")]
    offset: Duration,
    #[builder(setter(skip))]
    p: PhantomData<T>,
}

impl<T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static> IncomingDataPoller<T> {
    pub async fn start(
        self,
        shutdown: triggered::Listener,
    ) -> Result<(Receiver<IncomingDataStream<T>>, JoinHandle<Result>)> {
        let (sender, receiver) = tokio::sync::mpsc::channel(50);
        let join_handle = tokio::spawn(async move { self.run(shutdown, sender).await });

        Ok((receiver, join_handle))
    }

    async fn run(
        self,
        shutdown: triggered::Listener,
        sender: Sender<IncomingDataStream<T>>,
    ) -> Result {
        let duration = self
            .poll_duration
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::from_secs(DEFAULT_POLL_DURATION_SECS as u64));

        let mut trigger = tokio::time::interval(duration);
        let mut cleanup_trigger =
            tokio::time::interval(std::time::Duration::from_secs(CLEAN_DURATION_SECS));

        let mut latest_ts = db_latest_ts(&self.db, self.start_after, self.file_type).await?;
        println!("Latest_ts: {latest_ts:?}");

        loop {
            let after = latest_ts - self.offset;
            let before = Utc::now();
            let shutdown = shutdown.clone();

            tokio::select! {
                _ = shutdown => break,
                _ = cleanup_trigger.tick() => db_clean(&self.db, &self.file_type).await?,
                _ = trigger.tick() => {
                    let files = self.store.list_all(self.file_type, after, before).await?;
                    for file in files {
                        if !db_exists(&self.db, &file).await? {
                            latest_ts = file.timestamp;
                            send_stream(&sender, &self.store, file).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<T> IncomingDataStream<T> {
    pub async fn mark_done(
        self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result {
        db_insert(transaction, self.file_info).await?;
        let _ = self.sender.send(true);
        Ok(())
    }
}

async fn send_stream<T>(
    sender: &Sender<IncomingDataStream<T>>,
    store: &FileStore,
    file: FileInfo,
) -> Result
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    let (os_sender, os_receiver) = tokio::sync::oneshot::channel();
    let stream = store
        .stream_file(file.clone())
        .await?
        .filter_map(|msg| async {
            msg.map_err(|err| {
                tracing::error!("Error in processing binary");
                err
            })
            .ok()
        })
        .filter_map(|msg| async {
            <T as MsgDecode>::decode(msg)
                .map_err(|err| {
                    tracing::error!("Error in decoding message");
                    err
                })
                .ok()
        })
        .boxed();

    let incoming_data_stream = IncomingDataStream {
        file_info: file,
        stream,
        sender: os_sender,
    };

    let _ = sender.send(incoming_data_stream).await;
    let _ = os_receiver.await;
    Ok(())
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
            OFFSET 3 
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

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test() {
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
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

        let poller = IncomingDataPollerBuilder::<IotBeaconIngestReport>::default()
            .db(pool.clone())
            .store(file_store)
            .file_type(FileType::IotBeaconIngestReport)
            .start_after(Utc.timestamp_millis(1675299181384))
            .poll_duration(Duration::seconds(1))
            .build()
            .expect("Poller");

        let (mut receiver, join_handle) =
            poller.start(shutdown_listener).await.expect("start poller");

        let mut tx = pool.begin().await.expect("TX BEGIN");

        tokio::select! {
            msg = receiver.recv() => match msg {
                Some(mut msg) => {
                    println!("{:?}", &msg.file_info);
                    let x = msg.stream.next().await;
                    dbg!(x);
                    msg.mark_done(&mut tx).await.expect("MARK DONE");
                }
                None => println!("WTF"),
            }
        }

        tx.commit().await.expect("TX COMMIT");

        shutdown_trigger.trigger();

        // let x = join_handle.await;
        // dbg!(x);

        assert!(false);
    }
}
