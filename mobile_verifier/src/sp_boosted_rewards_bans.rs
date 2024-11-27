use std::{collections::HashSet, sync::Arc};

use chrono::{DateTime, TimeZone, Utc};
use file_store::{
    file_info_poller::{
        FileInfoPollerConfigBuilder, FileInfoStream, LookbackBehavior, ProstFileInfoPollerParser,
    },
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileStore, FileType,
};
use futures::{prelude::future::LocalBoxFuture, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        service_provider_boosted_rewards_banned_radio_req_v1::{
            KeyType as ProtoKeyType, SpBoostedRewardsBannedRadioBanType,
            SpBoostedRewardsBannedRadioReason,
        },
        SeniorityUpdate as SeniorityUpdateProto, SeniorityUpdateReason,
        ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
        ServiceProviderBoostedRewardsBannedRadioVerificationStatus,
        VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{PgPool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    heartbeats::{HbType, OwnedKeyType},
    seniority::{Seniority, SeniorityUpdate, SeniorityUpdateAction},
    Settings,
};

const CLEANUP_DAYS: i64 = 7;

pub struct BannedRadioReport {
    received_timestamp: DateTime<Utc>,
    pubkey: PublicKeyBinary,
    key: OwnedKeyType,
    until: DateTime<Utc>,
    reason: SpBoostedRewardsBannedRadioReason,
    ban_type: SpBoostedRewardsBannedRadioBanType,
}

impl BannedRadioReport {
    fn radio_type(&self) -> HbType {
        match self.key {
            OwnedKeyType::Cbrs(_) => HbType::Cbrs,
            OwnedKeyType::Wifi(_) => HbType::Wifi,
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
        let ban_type = report.ban_type();

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
            pubkey: report.pubkey.into(),
            key,
            until: Utc
                .timestamp_opt(report.until as i64, 0)
                .single()
                .ok_or_else(|| anyhow::anyhow!("invalid until: {}", report.until))?,
            reason,
            ban_type,
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

pub struct ServiceProviderBoostedRewardsBanIngestor {
    pool: PgPool,
    authorization_verifier: Arc<dyn AuthorizationVerifier>,
    receiver: Receiver<FileInfoStream<ServiceProviderBoostedRewardsBannedRadioIngestReportV1>>,
    verified_sink: FileSinkClient<VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1>,
    seniority_update_sink: FileSinkClient<SeniorityUpdateProto>,
}

impl ManagedTask for ServiceProviderBoostedRewardsBanIngestor {
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

impl ServiceProviderBoostedRewardsBanIngestor {
    pub async fn create_managed_task(
        pool: PgPool,
        file_upload: FileUpload,
        file_store: FileStore,
        authorization_verifier: Arc<dyn AuthorizationVerifier>,
        settings: &Settings,
        seniority_update_sink: FileSinkClient<SeniorityUpdateProto>,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_sink, verified_sink_server) =
            VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1::file_sink(
                settings.store_base_path(),
                file_upload,
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (receiver, ingest_server) = FileInfoPollerConfigBuilder::<
            ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
            _,
            _,
            _,
        >::default()
        .parser(ProstFileInfoPollerParser)
        .state(pool.clone())
        .store(file_store)
        .lookback(LookbackBehavior::StartAfter(settings.start_after))
        .prefix(FileType::SPBoostedRewardsBannedRadioIngestReport.to_string())
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

        let status = if is_authorized {
            ServiceProviderBoostedRewardsBannedRadioVerificationStatus::SpBoostedRewardsBanValid
        } else {
            ServiceProviderBoostedRewardsBannedRadioVerificationStatus::SpBoostedRewardsBanInvalidCarrierKey
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
        ban_type: SpBoostedRewardsBannedRadioBanType,
        date_time: DateTime<Utc>,
    ) -> anyhow::Result<BannedRadios> {
        sqlx::query(
            r#"
                SELECT distinct radio_type, radio_key
                FROM sp_boosted_rewards_bans
                WHERE ban_type = $1
                    AND received_timestamp <= $2
                    AND until > $2 
                    AND COALESCE(invalidated_at > $2, TRUE)
            "#,
        )
        .bind(ban_type.as_str_name())
        .bind(date_time)
        .fetch(pool)
        .map_err(anyhow::Error::from)
        .try_fold(BannedRadios::default(), |mut set, row| async move {
            let radio_type = row.get::<HbType, &str>("radio_type");
            let radio_key = row.get::<String, &str>("radio_key");
            match radio_type {
                HbType::Wifi => set.insert_wifi(PublicKeyBinary::from_str(&radio_key)?),
                HbType::Cbrs => set.insert_cbrs(radio_key),
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

    pub async fn update_report(
        transaction: &mut Transaction<'_, Postgres>,
        report: &BannedRadioReport,
    ) -> anyhow::Result<()> {
        match report.reason {
            SpBoostedRewardsBannedRadioReason::Unbanned => {
                invalidate_all_before(transaction, report).await
            }
            _ => {
                invalidate_all_before(transaction, report).await?;
                save(transaction, report).await
            }
        }
    }

    async fn save(
        transaction: &mut Transaction<'_, Postgres>,
        report: &BannedRadioReport,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                INSERT INTO sp_boosted_rewards_bans(ban_type, radio_type, radio_key, received_timestamp, until)
                VALUES($1,$2,$3,$4,$5)
            "#,
        )
        .bind(report.ban_type.as_str_name())
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
                AND ban_type = $3
                AND received_timestamp <= $4
        "#,
        )
        .bind(report.radio_type())
        .bind(&report.key)
        .bind(report.ban_type.as_str_name())
        .bind(report.received_timestamp)
        .execute(transaction)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use file_store::file_sink::Message;
    use helium_crypto::{KeyTag, Keypair, PublicKey};
    use helium_proto::services::poc_mobile::{
        SeniorityUpdate as SeniorityUpdateProto, ServiceProviderBoostedRewardsBannedRadioReqV1,
    };
    use mobile_config::client::ClientError;
    use rand::rngs::OsRng;
    use tokio::sync::mpsc;

    use crate::heartbeats::KeyType;

    use super::*;

    struct AllVerified;

    #[async_trait::async_trait]
    impl AuthorizationVerifier for AllVerified {
        async fn verify_authorized_key(
            &self,
            _pubkey: &PublicKeyBinary,
            _role: helium_proto::services::mobile_config::NetworkKeyRole,
        ) -> Result<bool, ClientError> {
            Ok(true)
        }
    }

    struct TestSetup {
        ingestor: ServiceProviderBoostedRewardsBanIngestor,
        _verified_receiver:
            Receiver<Message<VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1>>,
        _seniority_receiver: Receiver<Message<SeniorityUpdateProto>>,
    }

    impl TestSetup {
        fn create(pool: PgPool, verifier: Arc<dyn AuthorizationVerifier>) -> Self {
            let (_fip_sender, fip_receiver) = mpsc::channel(1);
            let (verified_sender, verified_receiver) = mpsc::channel(5);
            let (seniority_sender, seniority_receiver) = mpsc::channel(5);

            let verified_sink = FileSinkClient::new(verified_sender, "verified");
            let seniority_sink = FileSinkClient::new(seniority_sender, "seniority");

            let ingestor = ServiceProviderBoostedRewardsBanIngestor {
                pool,
                authorization_verifier: verifier,
                receiver: fip_receiver,
                verified_sink,
                seniority_update_sink: seniority_sink,
            };

            Self {
                ingestor,
                _verified_receiver: verified_receiver,
                _seniority_receiver: seniority_receiver,
            }
        }
    }

    fn wifi_ban_report(
        key: &PublicKey,
        until: DateTime<Utc>,
        reason: SpBoostedRewardsBannedRadioReason,
    ) -> ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
        let signer_keypair = generate_keypair();

        ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
            received_timestamp: Utc::now().timestamp_millis() as u64,
            report: Some(ServiceProviderBoostedRewardsBannedRadioReqV1 {
                pubkey: signer_keypair.public_key().into(),
                reason: reason as i32,
                until: until.timestamp() as u64,
                signature: vec![],
                key_type: Some(ProtoKeyType::HotspotKey(key.into())),
                ban_type: SpBoostedRewardsBannedRadioBanType::BoostedHex as i32,
            }),
        }
    }

    fn cbrs_ban_report(
        cbsd_id: String,
        until: DateTime<Utc>,
        reason: SpBoostedRewardsBannedRadioReason,
    ) -> ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
        let signer_keypair = generate_keypair();

        ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
            received_timestamp: Utc::now().timestamp_millis() as u64,
            report: Some(ServiceProviderBoostedRewardsBannedRadioReqV1 {
                pubkey: signer_keypair.public_key().into(),
                reason: reason as i32,
                until: until.timestamp() as u64,
                signature: vec![],
                key_type: Some(ProtoKeyType::CbsdId(cbsd_id)),
                ban_type: SpBoostedRewardsBannedRadioBanType::BoostedHex as i32,
            }),
        }
    }

    #[sqlx::test]
    async fn wifi_radio_can_get_banned_and_unbanned(pool: PgPool) -> anyhow::Result<()> {
        let setup = TestSetup::create(pool.clone(), Arc::new(AllVerified));
        let keypair = generate_keypair();
        let cbsd_id = "cbsd-id-1".to_string();

        let report = cbrs_ban_report(
            cbsd_id.clone(),
            Utc::now() + Duration::days(7),
            SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation,
        );

        let mut transaction = pool.begin().await?;
        setup
            .ingestor
            .process_ingest_report(&mut transaction, report)
            .await?;
        transaction.commit().await?;

        let banned_radios = db::get_banned_radios(
            &pool,
            SpBoostedRewardsBannedRadioBanType::BoostedHex,
            Utc::now(),
        )
        .await?;
        let result =
            banned_radios.contains(&keypair.public_key().to_owned().into(), Some(&cbsd_id));

        assert!(result);

        let report = cbrs_ban_report(
            cbsd_id.clone(),
            Utc::now() - Duration::days(7),
            SpBoostedRewardsBannedRadioReason::Unbanned,
        );

        let mut transaction = pool.begin().await?;
        setup
            .ingestor
            .process_ingest_report(&mut transaction, report)
            .await?;
        transaction.commit().await?;

        let banned_radios = db::get_banned_radios(
            &pool,
            SpBoostedRewardsBannedRadioBanType::BoostedHex,
            Utc::now(),
        )
        .await?;
        let result =
            banned_radios.contains(&keypair.public_key().to_owned().into(), Some(&cbsd_id));

        assert!(!result);

        Ok(())
    }

    #[sqlx::test]
    async fn cbrs_radio_can_get_banned_and_unbanned(pool: PgPool) -> anyhow::Result<()> {
        let setup = TestSetup::create(pool.clone(), Arc::new(AllVerified));
        let keypair = generate_keypair();

        let report = wifi_ban_report(
            keypair.public_key(),
            Utc::now() + Duration::days(7),
            SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation,
        );

        let mut transaction = pool.begin().await?;
        setup
            .ingestor
            .process_ingest_report(&mut transaction, report)
            .await?;
        transaction.commit().await?;

        let banned_radios = db::get_banned_radios(
            &pool,
            SpBoostedRewardsBannedRadioBanType::BoostedHex,
            Utc::now(),
        )
        .await?;
        let result = banned_radios.contains(&keypair.public_key().to_owned().into(), None);

        assert!(result);

        let report = wifi_ban_report(
            keypair.public_key(),
            Utc::now() - Duration::days(7),
            SpBoostedRewardsBannedRadioReason::Unbanned,
        );

        let mut transaction = pool.begin().await?;
        setup
            .ingestor
            .process_ingest_report(&mut transaction, report)
            .await?;
        transaction.commit().await?;

        let banned_radios = db::get_banned_radios(
            &pool,
            SpBoostedRewardsBannedRadioBanType::BoostedHex,
            Utc::now(),
        )
        .await?;
        let result = banned_radios.contains(&keypair.public_key().to_owned().into(), None);

        assert!(!result);

        Ok(())
    }

    #[sqlx::test]
    async fn getting_banned_reset_seniority(pool: PgPool) -> anyhow::Result<()> {
        let setup = TestSetup::create(pool.clone(), Arc::new(AllVerified));
        let keypair = generate_keypair();
        let pubkey = PublicKeyBinary::from(keypair.public_key().to_owned());

        let last_heartbeat_ts = Utc::now() - Duration::hours(5);
        let uuid = uuid::Uuid::new_v4();
        let key_type = KeyType::Wifi(&pubkey);

        let seniority_update = SeniorityUpdate::new(
            key_type,
            last_heartbeat_ts,
            uuid,
            SeniorityUpdateAction::Insert {
                new_seniority: last_heartbeat_ts,
                update_reason: SeniorityUpdateReason::NewCoverageClaimTime,
            },
        );

        let report = wifi_ban_report(
            keypair.public_key(),
            Utc::now() + Duration::days(7),
            SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation,
        );

        let mut transaction = pool.begin().await?;
        seniority_update.execute(&mut transaction).await?;

        setup
            .ingestor
            .process_ingest_report(&mut transaction, report)
            .await?;

        let seniority = Seniority::fetch_latest(key_type, &mut transaction)
            .await?
            .unwrap();
        transaction.commit().await?;

        assert!(seniority.seniority_ts > last_heartbeat_ts);
        assert_eq!(
            seniority.update_reason,
            SeniorityUpdateReason::ServiceProviderBan as i32
        );

        Ok(())
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }
}
