use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::{self, FileSinkClient},
    file_source,
    file_upload::FileUpload,
    mobile_subscriber::{
        SubscriberLocationIngestReport, SubscriberLocationReq,
        VerifiedSubscriberLocationIngestReport,
    },
    FileStore, FileType,
};
use futures::{StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use helium_proto::services::poc_mobile::{
    SubscriberReportVerificationStatus, VerifiedSubscriberLocationIngestReportV1,
};
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, entity_client::EntityVerifier,
};
use sqlx::{PgPool, Pool, Postgres, Transaction};
use std::{ops::Range, time::Instant};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::Settings;

const SUBSCRIBER_REWARD_PERIOD_IN_DAYS: i64 = 1;

pub type SubscriberValidatedLocations = Vec<Vec<u8>>;

pub struct SubscriberLocationIngestor<AV, EV> {
    pub pool: PgPool,
    authorization_verifier: AV,
    entity_verifier: EV,
    reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
    verified_report_sink: FileSinkClient,
}

impl<AV, EV> SubscriberLocationIngestor<AV, EV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    EV: EntityVerifier + Send + Sync + 'static,
{
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: FileStore,
        authorization_verifier: AV,
        entity_verifier: EV,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_subscriber_location, verified_subscriber_location_server) =
            file_sink::FileSinkBuilder::new(
                FileType::VerifiedSubscriberLocationIngestReport,
                settings.store_base_path(),
                file_upload.clone(),
                concat!(env!("CARGO_PKG_NAME"), "_verified_subscriber_location"),
            )
            .auto_commit(false)
            .create()
            .await?;

        let (subscriber_location_ingest, subscriber_location_ingest_server) =
            file_source::continuous_source::<SubscriberLocationIngestReport, _>()
                .state(pool.clone())
                .store(file_store.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::SubscriberLocationIngestReport.to_string())
                .create()
                .await?;

        let subscriber_location_ingestor = SubscriberLocationIngestor::new(
            pool,
            authorization_verifier,
            entity_verifier,
            subscriber_location_ingest,
            verified_subscriber_location,
        );

        Ok(TaskManager::builder()
            .add_task(verified_subscriber_location_server)
            .add_task(subscriber_location_ingest_server)
            .add_task(subscriber_location_ingestor)
            .build())
    }

    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        authorization_verifier: AV,
        entity_verifier: EV,
        reports_receiver: Receiver<FileInfoStream<SubscriberLocationIngestReport>>,
        verified_report_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            authorization_verifier,
            entity_verifier,
            reports_receiver,
            verified_report_sink,
        }
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                Some(file) = self.reports_receiver.recv() => {
                    let start = Instant::now();
                    self.process_file(file).await?;
                    metrics::histogram!("subscriber_location_processing_time")
                        .record(start.elapsed());
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
                        save(&loc_ingest_report, &mut transaction).await?;
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
                            &[("report_status", verified_report_status.as_str_name())],
                        )
                        .await?;

                    Ok(transaction)
                },
            )
            .await?
            .commit()
            .await?;
        self.verified_report_sink.commit().await?;
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
            .authorization_verifier
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
        {
            Ok(res) => res,
            Err(_err) => false,
        }
    }

    async fn verify_subscriber_id(&self, subscriber_id: &[u8]) -> bool {
        match self
            .entity_verifier
            .verify_rewardable_entity(subscriber_id)
            .await
        {
            Ok(res) => res,
            Err(_err) => false,
        }
    }
}

impl<AV, EV> ManagedTask for SubscriberLocationIngestor<AV, EV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    EV: EntityVerifier + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

pub async fn save(
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
    .bind(reward_period.end - Duration::days(SUBSCRIBER_REWARD_PERIOD_IN_DAYS))
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
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from subscriber_loc_verified where received_timestamp < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
