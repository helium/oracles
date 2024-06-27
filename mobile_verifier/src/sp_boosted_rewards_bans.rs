use chrono::{DateTime, TimeZone, Utc};
use file_store::{
    file_info_poller::{
        FileInfoPollerConfigBuilder, FileInfoStream, LookbackBehavior, ProstFileInfoPollerParser,
    },
    file_sink::{self, FileSinkClient},
    file_upload::FileUpload,
    FileStore, FileType,
};
use futures::{prelude::future::LocalBoxFuture, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        service_provider_boosted_rewards_banned_radio_req_v1::{
            KeyType, SpBoostedRewardsBannedRadioReason,
        },
        ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
        ServiceProviderBoostedRewardsBannedRadioVerificationStatus,
        VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{PgPool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::Settings;

const CLEANUP_DAYS: i64 = 7;
const CLEANUP_INTERVAL_SECS: u64 = 12 * 60 * 60;

struct BannedRadioReport {
    received_timestamp: DateTime<Utc>,
    pubkey: PublicKeyBinary,
    radio_type: String,
    radio_key: String,
    until: DateTime<Utc>,
    reason: SpBoostedRewardsBannedRadioReason,
}

impl TryFrom<ServiceProviderBoostedRewardsBannedRadioIngestReportV1> for BannedRadioReport {
    type Error = anyhow::Error;

    fn try_from(
        value: ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    ) -> Result<Self, Self::Error> {
        let report = value
            .report
            .ok_or_else(|| anyhow::anyhow!("invalid ingest report"))?;

        let reason = report.reason();

        let (radio_type, radio_key) = match report.key_type {
            Some(KeyType::CbsdId(cbsd_id)) => ("cbrs", cbsd_id),
            Some(KeyType::HotspotKey(bytes)) => ("wifi", PublicKeyBinary::from(bytes).to_string()),
            None => anyhow::bail!("Invalid keytype"),
        };

        Ok(Self {
            received_timestamp: Utc
                .timestamp_millis_opt(value.received_timestamp as i64)
                .single()
                .ok_or_else(|| {
                    anyhow::anyhow!("invalid received timestamp, {}", value.received_timestamp)
                })?,
            pubkey: report.pub_key.into(),
            radio_type: radio_type.to_string(),
            radio_key,
            until: Utc
                .timestamp_opt(report.until as i64, 0)
                .single()
                .ok_or_else(|| anyhow::anyhow!("invalid until: {}", report.until))?,
            reason,
        })
    }
}

pub struct ServiceProviderBoostedRewardsBanIngestor<AV> {
    pool: PgPool,
    authorization_verifier: AV,
    receiver: Receiver<FileInfoStream<ServiceProviderBoostedRewardsBannedRadioIngestReportV1>>,
    verified_sink: FileSinkClient,
}

impl<AV> ManagedTask for ServiceProviderBoostedRewardsBanIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    AV::Error: std::error::Error + Send + Sync + 'static,
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

impl<AV> ServiceProviderBoostedRewardsBanIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    AV::Error: std::error::Error + Send + Sync + 'static,
{
    pub async fn create_managed_task(
        pool: PgPool,
        file_upload: FileUpload,
        file_store: FileStore,
        authorization_verifier: AV,
        settings: &Settings,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_sink, verified_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::VerifiedServiceProviderBoostedRewardsBannedRadioIngestReport,
            settings.store_base_path(),
            file_upload,
            concat!(env!("CARGO_PKG_NAME"), "_verified_sp_boosted_rewards_ban"),
        )
        .auto_commit(false)
        .create()
        .await?;

        let (receiver, ingest_server) = FileInfoPollerConfigBuilder::<
            ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
            _,
            _,
        >::default()
        .parser(ProstFileInfoPollerParser)
        .state(pool.clone())
        .store(file_store)
        .lookback(LookbackBehavior::StartAfter(settings.start_after))
        .prefix(FileType::ServiceProviderBoostedRewardsBannedRadioIngestReport.to_string())
        .create()
        .await?;

        let ingestor = Self {
            pool,
            authorization_verifier,
            receiver,
            verified_sink,
        };

        Ok(TaskManager::builder()
            .add_task(verified_sink_server)
            .add_task(ingest_server)
            .add_task(ingestor)
            .build())
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("service provider boosted rewards ban ingestor starting");

        let mut cleanup_interval =
            tokio::time::interval(std::time::Duration::from_secs(CLEANUP_INTERVAL_SECS));

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = cleanup_interval.tick() => {
                    db::cleanup(&self.pool).await?;
                }
                Some(file) = self.receiver.recv() => {
                    self.process_file(file).await?;
                }
            }
        }
        tracing::info!("stopping service provider boosted rewards ban ingestor");

        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<ServiceProviderBoostedRewardsBannedRadioIngestReportV1>,
    ) -> anyhow::Result<()> {
        tracing::info!(file = %file_info_stream.file_info.key, "processing sp boosted rewards ban file");
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut tx, ingest| async move {
                self.process_ingest_report(&mut tx, ingest).await?;
                Ok(tx)
            })
            .await?
            .commit()
            .await?;

        self.verified_sink.commit().await?;

        Ok(())
    }

    async fn process_ingest_report(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        ingest: ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    ) -> anyhow::Result<()> {
        let report = BannedRadioReport::try_from(ingest.clone())?;
        let is_authorized = self.is_authorized(&report.pubkey).await?;

        if is_authorized {
            db::update_report(transaction, report).await?;
        }

        let status = match is_authorized {
                    true => ServiceProviderBoostedRewardsBannedRadioVerificationStatus::SpBoostedRewardsBanValid,
                    false => ServiceProviderBoostedRewardsBannedRadioVerificationStatus::SpBoostedRewardsBanInvalidCarrierKey,
                };

        let verified_report = VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
            report: Some(ingest),
            status: status.into(),
            timestamp: Utc::now().timestamp_millis() as u64,
        };

        self.verified_sink
            .write(verified_report, &[("status", status.as_str_name())])
            .await?;

        Ok(())
    }

    async fn is_authorized(&self, pubkey: &PublicKeyBinary) -> anyhow::Result<bool> {
        self.authorization_verifier
            .verify_authorized_key(pubkey, NetworkKeyRole::MobileCarrier)
            .await
            .map_err(anyhow::Error::from)
    }
}

mod db {
    use chrono::Duration;

    use super::*;

    pub(super) async fn cleanup(pool: &PgPool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM sp_boosted_rewards_bans
            WHERE until < $1 or invalidated_at < $1
        "#,
        )
        .bind(Utc::now() - Duration::days(CLEANUP_DAYS))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }

    pub(super) async fn update_report(
        transaction: &mut Transaction<'_, Postgres>,
        report: BannedRadioReport,
    ) -> anyhow::Result<()> {
        match report.reason {
            SpBoostedRewardsBannedRadioReason::Unbanned => {
                invalidate_all_before(transaction, report).await
            }
            _ => save(transaction, report).await,
        }
    }

    async fn save(
        transaction: &mut Transaction<'_, Postgres>,
        report: BannedRadioReport,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
        INSERT INTO sp_boosted_rewards_bans(radio_type, radio_key, received_timestamp, until)
        VALUES($1,$2,$3,$4)
    "#,
        )
        .bind(report.radio_type)
        .bind(report.radio_key)
        .bind(report.received_timestamp)
        .bind(report.until)
        .execute(transaction)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }

    async fn invalidate_all_before(
        transaction: &mut Transaction<'_, Postgres>,
        report: BannedRadioReport,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE sp_boosted_rewards_bans
            SET invalidated_at = now()
            WHERE radio_type = $1
                AND radio_key = $2
                AND received_timestamp <= $3
        "#,
        )
        .bind(report.radio_type)
        .bind(report.radio_key)
        .bind(report.received_timestamp)
        .execute(transaction)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}
