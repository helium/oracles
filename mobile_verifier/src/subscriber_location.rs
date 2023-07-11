use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    mobile_subscriber::{
        SubscriberLocationIngestReport, SubscriberLocationReq,
        VerifiedSubscriberLocationIngestReport,
    },
};
use futures::{StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use helium_proto::services::poc_mobile::{
    SubscriberReportVerificationStatus, VerifiedSubscriberLocationIngestReportV1,
};
use mobile_config::client::{AuthorizationClient, EntityClient};
use sqlx::{PgPool, Postgres, Transaction};
use std::ops::Range;
use tokio::sync::mpsc::Receiver;

pub type SubscriberValidatedLocations = Vec<Vec<u8>>;

pub struct SubscriberLocationIngestor {
    pub pool: PgPool,
    auth_client: AuthorizationClient,
    entity_client: EntityClient,
    reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
    verified_report_sink: FileSinkClient,
}

impl SubscriberLocationIngestor {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        auth_client: AuthorizationClient,
        entity_client: EntityClient,
        reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
        verified_report_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            auth_client,
            entity_client,
            reports_receiver,
            verified_report_sink,
        }
    }
    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                msg = self.reports_receiver.recv() => if let Some(stream) =  msg {
                    match self.process_file(stream).await {
                        Ok(()) => {
                            self.verified_report_sink.commit().await?;
                        },
                        Err(err) => { return Err(err)}
                    }

                }
            }
        }
        tracing::info!("stopping subscriber location reports handler");
        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<SubscriberLocationIngestReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(
                transaction,
                |mut transaction, loc_ingest_report| async move {
                    // verifiy the report
                    let verified_report_status =
                        self.verify_report(&loc_ingest_report.report).await;

                    // if the report is valid then save to the db
                    // and thus available to be rewarded
                    if verified_report_status == SubscriberReportVerificationStatus::Valid {
                        self.save(&loc_ingest_report, &mut transaction).await?;
                    }

                    // write out paper trail of verified report, valid or invalid
                    let verified_report_proto: VerifiedSubscriberLocationIngestReportV1 =
                        VerifiedSubscriberLocationIngestReport {
                            report: loc_ingest_report,
                            status: verified_report_status,
                            timestamp: Utc::now(),
                        }
                        .into();
                    self.verified_report_sink
                        .write(
                            verified_report_proto,
                            &[("status", verified_report_status.as_str_name())],
                        )
                        .await?;

                    Ok(transaction)
                },
            )
            .await?
            .commit()
            .await?;
        Ok(())
    }

    pub async fn save(
        &self,
        loc_ingest_report: &SubscriberLocationIngestReport,
        db: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
                INSERT INTO subscriber_loc_verified (subscriber_id, received_timestamp)
                VALUES ($1, $2)
                "#,
        )
        .bind(loc_ingest_report.report.subscriber_id.clone())
        .bind(loc_ingest_report.received_timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }

    async fn verify_report(
        &self,
        report: &SubscriberLocationReq,
    ) -> SubscriberReportVerificationStatus {
        if !self.verify_known_carrier_key(&report.carrier_pub_key).await {
            return SubscriberReportVerificationStatus::InvalidCarrierKey;
        };
        if !self.verify_subscriber_id(&report.subscriber_id).await {
            return SubscriberReportVerificationStatus::InvalidSubscriberId;
        };
        SubscriberReportVerificationStatus::Valid
    }

    async fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> bool {
        match self
            .auth_client
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
        {
            Ok(res) => res,
            Err(_err) => false,
        }
    }

    async fn verify_subscriber_id(&self, subscriber_id: &Vec<u8>) -> bool {
        match self
            .entity_client
            .verify_rewardable_entity(subscriber_id)
            .await
        {
            Ok(res) => res,
            Err(_err) => false,
        }
    }
}

#[derive(sqlx::FromRow)]
pub struct SubscriberLocationShare {
    pub subscriber_id: Vec<u8>,
}

pub async fn aggregate_location_shares(
    db: impl sqlx::PgExecutor<'_> + Copy,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<SubscriberValidatedLocations, sqlx::Error> {
    let mut rows = sqlx::query_as::<_, SubscriberLocationShare>(
        "select distinct(subscriber_id) from subscriber_loc_verified where received_timestamp >= $1 and received_timestamp < $2",
    )
    .bind(reward_period.start)
    .bind(reward_period.end)
    .fetch(db);
    let mut location_shares = SubscriberValidatedLocations::new();
    while let Some(share) = rows.try_next().await? {
        location_shares.push(share.subscriber_id)
    }
    Ok(location_shares)
}

pub async fn clear_location_shares(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from subscriber_loc_verified where received_timestamp < $1")
        .bind(reward_period.end)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
