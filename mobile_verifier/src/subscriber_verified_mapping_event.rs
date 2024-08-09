use crate::Settings;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::{self, FileSinkClient},
    file_source,
    file_upload::FileUpload,
    subscriber_verified_mapping_event::SubscriberVerifiedMappingEvent,
    subscriber_verified_mapping_event_ingest_report::SubscriberVerifiedMappingEventIngestReport,
    verified_subscriber_verified_mapping_event_ingest_report::VerifiedSubscriberVerifiedMappingEventIngestReport,
    FileStore, FileType,
};
use futures::{stream::StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        SubscriberVerifiedMappingEventVerificationStatus,
        VerifiedSubscriberVerifiedMappingEventIngestReportV1,
    },
};
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, entity_client::EntityVerifier,
};
use sqlx::{Pool, Postgres, Transaction};
use std::ops::Range;
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct SubscriberVerifiedMappingEventDeamon<AV, EV> {
    pool: Pool<Postgres>,
    authorization_verifier: AV,
    entity_verifier: EV,
    reports_receiver: Receiver<FileInfoStream<SubscriberVerifiedMappingEventIngestReport>>,
    verified_report_sink: FileSinkClient,
}

impl<AV, EV> SubscriberVerifiedMappingEventDeamon<AV, EV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    EV: EntityVerifier + Send + Sync + 'static,
{
    pub fn new(
        pool: Pool<Postgres>,
        authorization_verifier: AV,
        entity_verifier: EV,
        reports_receiver: Receiver<FileInfoStream<SubscriberVerifiedMappingEventIngestReport>>,
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

    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        authorization_verifier: AV,
        entity_verifier: EV,
        file_store: FileStore,
        file_upload: FileUpload,
    ) -> anyhow::Result<impl ManagedTask> {
        let (reports_receiver, reports_receiver_server) =
            file_source::continuous_source::<SubscriberVerifiedMappingEventIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::SubscriberVerifiedMappingEventIngestReport.to_string())
                .create()
                .await?;

        let (verified_report_sink, verified_report_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::VerifiedSubscriberVerifiedMappingEventIngestReport,
            settings.store_base_path(),
            file_upload.clone(),
            concat!(
                env!("CARGO_PKG_NAME"),
                "_verified_subscriber_verified_mapping_event_ingest_report"
            ),
        )
        .auto_commit(false)
        .create()
        .await?;

        let task = Self::new(
            pool,
            authorization_verifier,
            entity_verifier,
            reports_receiver,
            verified_report_sink,
        );

        Ok(TaskManager::builder()
            .add_task(reports_receiver_server)
            .add_task(verified_report_sink_server)
            .add_task(task)
            .build())
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting sme deamon");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("sme deamon shutting down");
                    break;
                }
                Some(file) = self.reports_receiver.recv() => {
                    self.process_file(file).await?;
                }
            }
        }
        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<SubscriberVerifiedMappingEventIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Processing Verified Mapping Event file {}",
            file_info_stream.file_info.key
        );

        let mut transaction = self.pool.begin().await?;

        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report: SubscriberVerifiedMappingEventIngestReport| async move {
                // verifiy the report
                let verified_report_status = self.verify_event(&report.report).await;

                // if the report is valid then save to the db
                // and thus available to be rewarded
                if verified_report_status
                    == SubscriberVerifiedMappingEventVerificationStatus::SvmeValid
                {
                    save_to_db(&report, &mut transaction).await?;
                }

                // write out paper trail of verified report, valid or invalid
                let verified_report_proto: VerifiedSubscriberVerifiedMappingEventIngestReportV1 =
                    VerifiedSubscriberVerifiedMappingEventIngestReport {
                        report,
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
            })
            .await?
            .commit()
            .await?;

        self.verified_report_sink.commit().await?;

        Ok(())
    }

    async fn verify_event(
        &self,
        event: &SubscriberVerifiedMappingEvent,
    ) -> SubscriberVerifiedMappingEventVerificationStatus {
        if !self
            .verify_known_carrier_key(&event.carrier_mapping_key)
            .await
        {
            return SubscriberVerifiedMappingEventVerificationStatus::SvmeInvalidCarrierKey;
        }

        if !self.verify_subscriber_id(&event.subscriber_id).await {
            return SubscriberVerifiedMappingEventVerificationStatus::SvmeInvalidSubscriberId;
        }

        SubscriberVerifiedMappingEventVerificationStatus::SvmeValid
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

impl<AV, EV> ManagedTask for SubscriberVerifiedMappingEventDeamon<AV, EV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    EV: EntityVerifier + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

async fn save_to_db(
    report: &SubscriberVerifiedMappingEventIngestReport,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO subscriber_verified_mapping_event (subscriber_id, total_reward_points, received_timestamp)
        VALUES ($1, $2, $3)
        ON CONFLICT (subscriber_id, received_timestamp) DO NOTHING
        "#,
    )
    .bind(&report.report.subscriber_id)
    .bind(report.report.total_reward_points as i64)
    .bind(report.received_timestamp)
    .execute(exec)
    .await?;

    Ok(())
}

const SUBSCRIBER_REWARD_PERIOD_IN_DAYS: i64 = 1;
pub type VerifiedSubscriberVerifiedMappingEventShares =
    Vec<VerifiedSubscriberVerifiedMappingEventShare>;

#[derive(sqlx::FromRow, PartialEq, Debug)]
pub struct VerifiedSubscriberVerifiedMappingEventShare {
    pub subscriber_id: Vec<u8>,
    pub total_reward_points: i64,
}

pub async fn aggregate_verified_mapping_events(
    db: impl sqlx::PgExecutor<'_> + Copy,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<VerifiedSubscriberVerifiedMappingEventShares, sqlx::Error> {
    let vsme_shares = sqlx::query_as::<_, VerifiedSubscriberVerifiedMappingEventShare>(
        "SELECT 
            subscriber_id, 
            SUM(total_reward_points) AS total_reward_points
        FROM 
            subscriber_verified_mapping_event
        WHERE received_timestamp >= $1 AND received_timestamp < $2
        GROUP BY 
            subscriber_id;",
    )
    .bind(reward_period.start - Duration::days(SUBSCRIBER_REWARD_PERIOD_IN_DAYS))
    .bind(reward_period.end)
    .fetch_all(db)
    .await?;

    Ok(vsme_shares)
}

pub async fn clear(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM subscriber_verified_mapping_event WHERE received_timestamp < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
