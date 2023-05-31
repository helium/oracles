use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, mobile_transfer::ValidDataTransferSession};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::PublicKeyBinary;
use rust_decimal::prelude::*;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use std::ops::Range;
use tokio::sync::mpsc::Receiver;

pub struct DataSessionIngestor {
    pub pool: PgPool,
}

pub type SubscriberMap = HashMap<Vec<u8>, Decimal>;
pub type HotspotMap = HashMap<PublicKeyBinary, Decimal>;

impl DataSessionIngestor {
    pub async fn run(
        self,
        mut receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
        shutdown: triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("starting DataSessionIngestor");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.clone() => {
                        tracing::info!("DataSessionIngestor shutting down");
                        break;
                    }
                    Some(file) = receiver.recv() => self.process_file(file).await?,
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<ValidDataTransferSession>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "handling valid data transfer file {:?}",
            file_info_stream.file_info.key
        );
        let mut transaction = self.pool.begin().await?;
        let file_ts = file_info_stream.file_info.timestamp;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                // if we have a subscriber id, then we know this is
                // a data transfer event from a mobile subscriber
                match report.subscriber_id.clone() {
                    Some(subscriber_id) => {
                        let data_session = SubscriberDataSession::from_valid_data_session(
                            report,
                            subscriber_id,
                            file_ts,
                        );
                        data_session.save(&mut transaction).await?;
                        metrics::increment_counter!(
                            "oracles_mobile_verifier_ingest_subscriber_data_session"
                        )
                    }

                    None => {
                        let data_session =
                            HotspotDataSession::from_valid_data_session(report, file_ts);
                        data_session.save(&mut transaction).await?;
                        metrics::increment_counter!(
                            "oracles_mobile_verifier_ingest_hotspot_data_session"
                        )
                    }
                }

                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
pub struct SubscriberDataSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub subscriber_id: Vec<u8>,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub reward_timestamp: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
pub struct HotspotDataSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub num_dcs: i64,
    pub reward_timestamp: DateTime<Utc>,
}

#[derive(thiserror::Error, Debug)]
#[error("data session error: {0}")]
pub struct DataSessionError(#[from] sqlx::Error);

impl SubscriberDataSession {
    pub async fn save(self, db: &mut Transaction<'_, Postgres>) -> Result<(), DataSessionError> {
        sqlx::query(
            r#"
            INSERT INTO subscriber_data_transfer_sessions (pub_key, subscriber_id, payer, upload_bytes, download_bytes, reward_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(self.pub_key)
        .bind(self.subscriber_id)
        // .bind("subscriber_x".to_string().as_bytes().to_vec())  // TODO: used for local debugging, remove when done
        .bind(self.payer)
        .bind(self.upload_bytes)
        .bind(self.download_bytes)
        .bind(self.reward_timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }
    fn from_valid_data_session(
        v: ValidDataTransferSession,
        subscriber_id: Vec<u8>,
        reward_timestamp: DateTime<Utc>,
    ) -> SubscriberDataSession {
        Self {
            pub_key: v.pub_key,
            payer: v.payer,
            subscriber_id,
            upload_bytes: v.upload_bytes as i64,
            download_bytes: v.download_bytes as i64,
            reward_timestamp,
        }
    }
}

impl HotspotDataSession {
    pub async fn save(self, db: &mut Transaction<'_, Postgres>) -> Result<(), DataSessionError> {
        sqlx::query(
            r#"
            INSERT INTO hotspot_data_transfer_sessions (pub_key, payer, upload_bytes, download_bytes, num_dcs, reward_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(self.pub_key)
        .bind(self.payer)
        .bind(self.upload_bytes)
        .bind(self.download_bytes)
        .bind(self.num_dcs)
        .bind(self.reward_timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }
    fn from_valid_data_session(
        v: ValidDataTransferSession,
        reward_timestamp: DateTime<Utc>,
    ) -> HotspotDataSession {
        Self {
            pub_key: v.pub_key,
            payer: v.payer,
            upload_bytes: v.upload_bytes as i64,
            download_bytes: v.download_bytes as i64,
            num_dcs: v.num_dcs as i64,
            reward_timestamp,
        }
    }
}

pub async fn aggregate_mobile_hotspot_data_sessions_to_dc<'a>(
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
    epoch: &'a Range<DateTime<Utc>>,
) -> Result<HotspotMap, sqlx::Error> {
    let stream = sqlx::query_as::<_, HotspotDataSession>(
        r#"
        SELECT *
        FROM hotspot_data_transfer_sessions
        WHERE reward_timestamp >= $1 and reward_timestamp < $2
        "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch(exec);
    data_sessions_to_dc(stream).await
}

pub async fn data_sessions_to_dc<'a>(
    stream: impl Stream<Item = Result<HotspotDataSession, sqlx::Error>>,
) -> Result<HotspotMap, sqlx::Error> {
    tokio::pin!(stream);
    let mut map = HotspotMap::new();
    while let Some(session) = stream.try_next().await? {
        *map.entry(session.pub_key).or_default() += Decimal::from(session.num_dcs)
    }
    Ok(map)
}

pub async fn aggregate_mobile_subscriber_data_sessions_to_bytes<'a>(
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
    epoch: &Range<DateTime<Utc>>,
) -> Result<SubscriberMap, sqlx::Error> {
    // TODO: to determine if a mobile subscriber has transferred data
    // confirm it is downloaded data we are interested in and not
    // either uploaded data or a sum of both
    let stream = sqlx::query_as::<_, SubscriberDataSession>(
        r#"
            SELECT *
            FROM subscriber_data_transfer_sessions
            WHERE reward_timestamp >= $1 and reward_timestamp < $2
            "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch(exec);
    data_sessions_to_bytes(stream).await
}

pub async fn data_sessions_to_bytes<'a>(
    stream: impl Stream<Item = Result<SubscriberDataSession, sqlx::Error>>,
) -> Result<SubscriberMap, sqlx::Error> {
    tokio::pin!(stream);
    let mut map = SubscriberMap::new();
    while let Some(session) = stream.try_next().await? {
        *map.entry(session.subscriber_id).or_default() += Decimal::from(session.download_bytes)
    }
    Ok(map)
}

pub async fn clear_subscriber_data_sessions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from subscriber_data_transfer_sessions where reward_timestamp <= $1")
        .bind(reward_period.end)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
