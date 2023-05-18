use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, mobile_subscriber::SubscriberLocationIngestReport,
};
use futures::{StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::mpsc::Receiver;

pub struct SubscriberLocationLoader {
    pub pool: PgPool,
    pub carrier_keys: Vec<PublicKeyBinary>,
    reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
}

impl SubscriberLocationLoader {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        carrier_keys: Vec<PublicKeyBinary>,
        reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
    ) -> Self {
        Self {
            pool,
            carrier_keys,
            reports_receiver,
        }
    }
    pub async fn run(&mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        // TODO: add tick to periodaclly refresh the carrier keys from mobile config service
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                msg = self.reports_receiver.recv() => if let Some(stream) =  msg {
                    self.handle_report(stream).await?;
                }
            }
        }
        tracing::info!("stopping subscriber location reports handler");
        Ok(())
    }

    async fn handle_report(
        &self,
        file_info_stream: FileInfoStream<SubscriberLocationIngestReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                if self.verify_known_carrier_key(&report.report.pubkey) {
                    self.save(
                        report.report.subscriber_id.clone(),
                        report.received_timestamp, // TODO: we want the recv timestamp or the location report timestamp ?
                        &mut transaction,
                    )
                    .await?;
                    metrics::increment_counter!(
                        "oracles_mobile_verifier_valid_subscriber_location_report"
                    );
                } else {
                    metrics::increment_counter!(
                        "oracles_mobile_verifier_invalid_subscriber_location_report"
                    );
                }
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }

    pub async fn save(
        &self,
        subscriber_id: Vec<u8>,
        timestamp: DateTime<Utc>,
        db: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
                INSERT INTO subscriber_loc (subscriber_id, date_bucket, hour_bucket, recv_timestamp)
                VALUES ($1, DATE_TRUNC('hour', $2), DATE_PART('hour', $2), $2)
                ON CONFLICT DO NOTHING;
                "#,
        )
        .bind(subscriber_id)
        .bind(timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }

    fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> bool {
        self.carrier_keys.contains(public_key)
    }

    // TODO: finish this
    fn refresh_carrier_keys(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
