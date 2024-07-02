use std::collections::HashSet;

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
            KeyType as ProtoKeyType, SpBoostedRewardsBannedRadioReason,
        },
        SeniorityUpdateReason, ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
        ServiceProviderBoostedRewardsBannedRadioVerificationStatus,
        VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{PgPool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    heartbeats::OwnedKeyType,
    seniority::{Seniority, SeniorityUpdate, SeniorityUpdateAction},
    Settings,
};

const CLEANUP_DAYS: i64 = 7;

struct BannedRadioReport {
    received_timestamp: DateTime<Utc>,
    pubkey: PublicKeyBinary,
    key: OwnedKeyType,
    until: DateTime<Utc>,
    reason: SpBoostedRewardsBannedRadioReason,
}

impl BannedRadioReport {
    fn radio_type(&self) -> &'static str {
        match self.key {
            OwnedKeyType::Cbrs(_) => "cbrs",
            OwnedKeyType::Wifi(_) => "wifi",
        }
    }
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

        let key = match report.key_type {
            Some(ProtoKeyType::CbsdId(cbsd_id)) => OwnedKeyType::Cbrs(cbsd_id),
            Some(ProtoKeyType::HotspotKey(bytes)) => {
                OwnedKeyType::Wifi(PublicKeyBinary::from(bytes))
            }
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
            key,
            until: Utc
                .timestamp_opt(report.until as i64, 0)
                .single()
                .ok_or_else(|| anyhow::anyhow!("invalid until: {}", report.until))?,
            reason,
        })
    }
}

#[derive(Debug, Default)]
pub struct BannedRadios {
    wifi: HashSet<PublicKeyBinary>,
    cbrs: HashSet<String>,
}

impl BannedRadios {
    pub fn insert_wifi(&mut self, pubkey: PublicKeyBinary) {
        self.wifi.insert(pubkey);
    }

    pub fn insert_cbrs(&mut self, cbsd_id: String) {
        self.cbrs.insert(cbsd_id);
    }

    pub fn contains(&self, pubkey: &PublicKeyBinary, cbsd_id_opt: Option<&str>) -> bool {
        match cbsd_id_opt {
            Some(cbsd_id) => self.cbrs.contains(cbsd_id),
            None => self.wifi.contains(pubkey),
        }
    }
}

pub struct ServiceProviderBoostedRewardsBanIngestor<AV> {
    pool: PgPool,
    authorization_verifier: AV,
    receiver: Receiver<FileInfoStream<ServiceProviderBoostedRewardsBannedRadioIngestReportV1>>,
    verified_sink: FileSinkClient,
    seniority_update_sink: FileSinkClient,
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
        seniority_update_sink: FileSinkClient,
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
            seniority_update_sink,
        };

        Ok(TaskManager::builder()
            .add_task(verified_sink_server)
            .add_task(ingest_server)
            .add_task(ingestor)
            .build())
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("service provider boosted rewards ban ingestor starting");

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
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
        self.seniority_update_sink.commit().await?;

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
            db::update_report(transaction, &report).await?;
            self.update_seniority(transaction, &report).await?;
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

    async fn update_seniority(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        report: &BannedRadioReport,
    ) -> anyhow::Result<()> {
        if let Some(current_seniority) =
            Seniority::fetch_latest(report.key.to_ref(), transaction).await?
        {
            let seniority_update = SeniorityUpdate::new(
                report.key.to_ref(),
                current_seniority.last_heartbeat,
                current_seniority.uuid,
                SeniorityUpdateAction::Insert {
                    new_seniority: Utc::now(),
                    update_reason: SeniorityUpdateReason::ServiceProviderBan,
                },
            );

            seniority_update.write(&self.seniority_update_sink).await?;
            seniority_update.execute(transaction).await?;
        }

        Ok(())
    }
}

pub async fn clear_bans(
    transaction: &mut Transaction<'_, Postgres>,
    before: DateTime<Utc>,
) -> anyhow::Result<()> {
    db::cleanup(transaction, before).await
}

pub mod db {
    use std::str::FromStr;

    use chrono::Duration;
    use sqlx::Row;

    use super::*;

    pub async fn get_banned_radios(
        pool: &PgPool,
        date_time: DateTime<Utc>,
    ) -> anyhow::Result<BannedRadios> {
        sqlx::query(
            r#"
                SELECT radio_type, radio_key
                FROM sp_boosted_rewards_bans
                WHERE until < $1 or invalidated_at < $1
            "#,
        )
        .bind(date_time)
        .fetch(pool)
        .map_err(anyhow::Error::from)
        .try_fold(BannedRadios::default(), |mut set, row| async move {
            let radio_type = row.get::<&str, &str>("radio_type");
            let radio_key = row.get::<String, &str>("radio_key");
            match radio_type {
                "wifi" => set.insert_wifi(PublicKeyBinary::from_str(&radio_key)?),
                "cbrs" => set.insert_cbrs(radio_key),
                _ => anyhow::bail!("Inavlid radio type: {}", radio_type),
            };

            Ok(set)
        })
        .await
    }

    pub(super) async fn cleanup(
        transaction: &mut Transaction<'_, Postgres>,
        before: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                DELETE FROM sp_boosted_rewards_bans
                WHERE until < $1 or invalidated_at < $1
            "#,
        )
        .bind(before - Duration::days(CLEANUP_DAYS))
        .execute(transaction)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }

    pub(super) async fn update_report(
        transaction: &mut Transaction<'_, Postgres>,
        report: &BannedRadioReport,
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
        report: &BannedRadioReport,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                INSERT INTO sp_boosted_rewards_bans(radio_type, radio_key, received_timestamp, until)
                VALUES($1,$2,$3,$4)
            "#,
        )
        .bind(report.radio_type())
        .bind(&report.key)
        .bind(report.received_timestamp)
        .bind(report.until)
        .execute(transaction)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }

    async fn invalidate_all_before(
        transaction: &mut Transaction<'_, Postgres>,
        report: &BannedRadioReport,
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
        .bind(report.radio_type())
        .bind(&report.key)
        .bind(report.received_timestamp)
        .execute(transaction)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}
