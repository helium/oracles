use std::collections::{HashMap, HashSet};

use chrono::Utc;
use file_store::{
    file_upload::FileUpload,
    mobile_ban::{
        self, BanReport, BanReportSource, BanReportStream, BanType, VerifiedBanIngestReportStatus,
        VerifiedBanReport, VerifiedBanReportSink,
    },
    traits::{FileSinkCommitStrategy, FileSinkRollTime},
    FileStore,
};
use futures::{StreamExt, TryFutureExt};

use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, AuthorizationClient, ClientError,
};
use sqlx::{PgConnection, PgPool};
use task_manager::{ManagedTask, TaskManager};

use crate::Settings;

pub struct BanIngestor {
    pool: PgPool,
    auth_verifier: AuthorizationClient,
    report_rx: BanReportSource,
    verified_sink: VerifiedBanReportSink,
}

pub async fn create_managed_task(
    pool: PgPool,
    file_upload: FileUpload,
    file_store: FileStore,
    auth_verifier: AuthorizationClient,
    settings: &Settings,
) -> anyhow::Result<impl ManagedTask> {
    let (verified_sink, verified_sink_server) = mobile_ban::verified_report_sink(
        settings.store_base_path(),
        file_upload,
        FileSinkCommitStrategy::Manual,
        FileSinkRollTime::Default,
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let (report_rx, ingest_server) =
        mobile_ban::report_source(pool.clone(), file_store, settings.start_after).await?;

    let ingestor = BanIngestor {
        pool,
        auth_verifier,
        report_rx,
        verified_sink,
    };

    Ok(TaskManager::builder()
        .add_task(verified_sink_server)
        .add_task(ingest_server)
        .add_task(ingestor)
        .build())
}

impl ManagedTask for BanIngestor {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl BanIngestor {
    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!(" ban ingestor starting");

        loop {
            tokio::select! {
                _= &mut shutdown => break,
                msg = self.report_rx.recv() => {
                    let Some(file_info_stream) = msg else {
                        tracing::warn!(" ban report rx dropped");
                        break;
                    };
                    self.process_file(file_info_stream).await?;
                }
            }
        }

        tracing::info!(" ban ingestor stopping");

        Ok(())
    }

    async fn process_file(&self, file_info_stream: BanReportStream) -> anyhow::Result<()> {
        let file = &file_info_stream.file_info.key;
        tracing::info!(file, "processing");

        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        while let Some(report) = stream.next().await {
            let verified_report = process_ban_report(&mut txn, &self.auth_verifier, report).await?;
            self.verified_sink.write(verified_report, &[]).await?;
        }

        self.verified_sink.commit().await?;
        txn.commit().await?;

        Ok(())
    }
}

async fn process_ban_report(
    conn: &mut PgConnection,
    auth_verifier: &impl AuthorizationVerifier<Error = ClientError>,
    report: BanReport,
) -> anyhow::Result<VerifiedBanReport> {
    let status = get_verified_status(auth_verifier, &report.report.ban_key).await?;

    let verified_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        report,
        status,
    };

    if verified_report.is_verified() {
        db::update_hotspot_ban(conn, &verified_report).await?;
    }
    Ok(verified_report)
}

async fn get_verified_status(
    auth_verifier: &impl AuthorizationVerifier<Error = ClientError>,
    pubkey: &helium_crypto::PublicKeyBinary,
) -> anyhow::Result<VerifiedBanIngestReportStatus> {
    let is_authorized = auth_verifier
        .verify_authorized_key(pubkey, NetworkKeyRole::BanKey)
        .await?;
    let status = match is_authorized {
        true => VerifiedBanIngestReportStatus::Valid,
        false => VerifiedBanIngestReportStatus::InvalidBanKey,
    };
    Ok(status)
}

#[derive(Debug, Default)]
pub struct BannedRadios {
    banned: HashMap<PublicKeyBinary, BanType>,
    sp_banned: HashSet<PublicKeyBinary>,
}

impl BannedRadios {
    pub async fn new(pool: &PgPool, date_time: chrono::DateTime<Utc>) -> anyhow::Result<Self> {
        let mut me = db::get_banned_radios(pool, date_time).await?;

        use helium_proto::services::poc_mobile::service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType;
        let poc_banned_radios = crate::sp_boosted_rewards_bans::db::get_banned_radios(
            pool,
            SpBoostedRewardsBannedRadioBanType::Poc,
            date_time,
        )
        .await?;

        // let mut me = Self::default();
        me.sp_banned = poc_banned_radios.wifi;

        Ok(me)
    }

    pub fn insert_sp_banned(&mut self, hotspot_pubkey: PublicKeyBinary) {
        self.sp_banned.insert(hotspot_pubkey);
    }

    pub fn is_sp_banned(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
        self.sp_banned.contains(hotspot_pubkey)
    }

    pub fn is_poc_banned(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
        self.is_banned(hotspot_pubkey, BanType::Poc)
    }

    // IMPORTANT:
    //
    // This function should not be provided.
    // DataTransferSessions that are written by the mobile-packet-verifier
    // must be output in the rewards file. By this time, the DC for those rewards
    // have already been burnt.
    //
    // There is a matching banned radio status in mobile-packet-verifier
    // that prevents DC from being burnt for banned radios, resulting in
    // DataTransferSessions not being output for this servive to output them.
    //
    // fn is_data_banned(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
    //     self.is_banned(hotspot_pubkey, BanType::Data)
    // }

    fn is_banned(&self, hotspot_pubkey: &PublicKeyBinary, ban_type: BanType) -> bool {
        match self.banned.get(hotspot_pubkey) {
            Some(BanType::All) => true,
            Some(ban) => ban == &ban_type,
            None => false,
        }
    }
}

pub mod db {
    use std::str::FromStr;

    use chrono::{DateTime, Utc};
    use file_store::mobile_ban::{BanAction, BanDetails, BanType, UnbanDetails, VerifiedBanReport};
    use futures::TryStreamExt;
    use helium_crypto::PublicKeyBinary;
    use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};

    use super::BannedRadios;

    pub async fn get_banned_radios(
        pool: &PgPool,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<BannedRadios> {
        sqlx::query(
            r#"
            SELECT hotspot_pubkey, ban_type
            FROM hotspot_bans
            WHERE 
                expiration_timestamp IS NULL 
                OR expiration_timestamp >= $1
            "#,
        )
        .bind(timestamp)
        .fetch(pool)
        .map_err(anyhow::Error::from)
        .try_fold(BannedRadios::default(), |mut set, row| async move {
            let pubkey = PublicKeyBinary::from_str(row.get("hotspot_pubkey"))?;
            let ban_type = BanType::from_str(row.get("ban_type"))?;
            set.banned.insert(pubkey, ban_type);
            Ok(set)
        })
        .await
    }

    pub async fn cleanup_bans(
        txn: &mut Transaction<'_, Postgres>,
        before: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM hotspot_bans WHERE expiration_timestamp < $1")
            .bind(before)
            .execute(txn)
            .await?;

        Ok(())
    }

    pub async fn update_hotspot_ban(
        conn: &mut PgConnection,
        ban_report: &VerifiedBanReport,
    ) -> anyhow::Result<()> {
        match &ban_report.report.report.ban_action {
            // TODO: ignore `ban_type = Poc` when mobile_packet_verifier is updated
            BanAction::Ban(BanDetails {
                ban_type,
                expiration_timestamp,
                ..
            }) => {
                insert_ban(
                    conn,
                    ban_report.hotspot_pubkey(),
                    ban_report.report.received_timestamp,
                    *expiration_timestamp,
                    ban_type,
                )
                .await?
            }
            BanAction::Unban(UnbanDetails { .. }) => {
                remove_ban(conn, ban_report.hotspot_pubkey()).await?
            }
        }

        Ok(())
    }

    async fn insert_ban(
        conn: &mut PgConnection,
        hotspot_pubkey: &PublicKeyBinary,
        received_timestamp: DateTime<Utc>,
        expiration_timestamp: Option<DateTime<Utc>>,
        ban_type: &BanType,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO hotspot_bans
                (hotspot_pubkey, received_timestamp, expiration_timestamp, ban_type)
            VALUES
                ($1, $2, $3, $4)
            ON CONFLICT (hotspot_pubkey) DO UPDATE SET
                received_timestamp = EXCLUDED.received_timestamp,
                expiration_timestamp = EXCLUDED.expiration_timestamp,
                ban_type = EXCLUDED.ban_type
            "#,
        )
        .bind(hotspot_pubkey)
        .bind(received_timestamp)
        .bind(expiration_timestamp)
        .bind(ban_type.as_str_name())
        .execute(conn)
        .await?;

        Ok(())
    }

    async fn remove_ban(
        conn: &mut PgConnection,
        hotspot_pubkey: &PublicKeyBinary,
    ) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM hotspot_bans WHERE hotspot_pubkey = $1")
            .bind(hotspot_pubkey)
            .execute(conn)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::mobile_config::NetworkKeyRole;
    use mobile_ban::{proto::BanReason, BanAction, BanDetails, BanRequest, BanType, UnbanDetails};

    use super::*;

    struct AllVerified;

    #[async_trait::async_trait]
    impl AuthorizationVerifier for AllVerified {
        type Error = ClientError;

        async fn verify_authorized_key(
            &self,
            _pubkey: &PublicKeyBinary,
            _role: NetworkKeyRole,
        ) -> Result<bool, Self::Error> {
            Ok(true)
        }
    }

    async fn test_get_current_banned_radios(pool: &PgPool) -> anyhow::Result<BannedRadios> {
        db::get_banned_radios(pool, Utc::now()).await
    }

    #[sqlx::test]
    async fn ban_unban(pool: PgPool) -> anyhow::Result<()> {
        let mut conn = pool.acquire().await?;
        let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

        let ban_report = BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: hotspot_pubkey.clone(),
                sent_timestamp: Utc::now(),
                ban_key: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    notes: "test-ban".to_string(),
                    reason: BanReason::Gaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        };

        let unban_report = BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: hotspot_pubkey.clone(),
                sent_timestamp: Utc::now(),
                ban_key: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Unban(UnbanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    notes: "test-unban".to_string(),
                }),
            },
        };

        // Ban radio
        process_ban_report(&mut conn, &AllVerified, ban_report).await?;
        let banned = test_get_current_banned_radios(&pool).await?;
        assert_eq!(true, banned.is_poc_banned(&hotspot_pubkey));

        // Unban radio
        process_ban_report(&mut conn, &AllVerified, unban_report).await?;
        let banned = test_get_current_banned_radios(&pool).await?;
        assert_eq!(false, banned.is_poc_banned(&hotspot_pubkey));

        Ok(())
    }

    #[sqlx::test]
    async fn new_ban_replaces_old_ban(pool: PgPool) -> anyhow::Result<()> {
        let mut conn = pool.acquire().await?;
        let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

        let mk_ban_report = |ban_type: BanType| BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: hotspot_pubkey.clone(),
                sent_timestamp: Utc::now(),
                ban_key: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    notes: "test-ban".to_string(),
                    reason: BanReason::Gaming,
                    ban_type,
                    expiration_timestamp: None,
                }),
            },
        };

        process_ban_report(&mut conn, &AllVerified, mk_ban_report(BanType::All)).await?;
        let banned = test_get_current_banned_radios(&pool).await?;
        assert_eq!(true, banned.is_poc_banned(&hotspot_pubkey));

        process_ban_report(&mut conn, &AllVerified, mk_ban_report(BanType::Data)).await?;
        let banned = test_get_current_banned_radios(&pool).await?;
        assert_eq!(false, banned.is_poc_banned(&hotspot_pubkey));

        Ok(())
    }

    #[sqlx::test]
    async fn expired_bans_are_not_used(pool: PgPool) -> anyhow::Result<()> {
        let mut conn = pool.acquire().await?;
        let expired_hotspot_pubkey = PublicKeyBinary::from(vec![1]);
        let banned_hotspot_pubkey = PublicKeyBinary::from(vec![2]);

        let expired_ban_report = BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: expired_hotspot_pubkey.clone(),
                sent_timestamp: Utc::now() - chrono::Duration::hours(6),
                ban_key: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    notes: "test-ban".to_string(),
                    reason: BanReason::Gaming,
                    ban_type: BanType::All,
                    expiration_timestamp: Some(Utc::now() - chrono::Duration::hours(5)),
                }),
            },
        };

        let ban_report = BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: banned_hotspot_pubkey.clone(),
                sent_timestamp: Utc::now(),
                ban_key: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    notes: "test-ban".to_string(),
                    reason: BanReason::Gaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        };

        process_ban_report(&mut conn, &AllVerified, expired_ban_report).await?;
        process_ban_report(&mut conn, &AllVerified, ban_report).await?;

        let banned = test_get_current_banned_radios(&pool).await?;
        assert_eq!(false, banned.is_poc_banned(&expired_hotspot_pubkey));

        assert_eq!(true, banned.is_poc_banned(&banned_hotspot_pubkey));

        Ok(())
    }

    #[sqlx::test]
    async fn unverified_requests_are_not_written_to_db(pool: PgPool) -> anyhow::Result<()> {
        struct NoneVerified;

        #[async_trait::async_trait]
        impl AuthorizationVerifier for NoneVerified {
            type Error = ClientError;

            async fn verify_authorized_key(
                &self,
                _pubkey: &PublicKeyBinary,
                _role: NetworkKeyRole,
            ) -> Result<bool, Self::Error> {
                Ok(false)
            }
        }

        let mut conn = pool.acquire().await?;
        let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

        let ban_report = BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: hotspot_pubkey.clone(),
                sent_timestamp: Utc::now(),
                ban_key: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    notes: "test-ban".to_string(),
                    reason: BanReason::Gaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        };

        // Unverified Ban radio
        process_ban_report(&mut conn, &NoneVerified, ban_report).await?;
        let banned = test_get_current_banned_radios(&pool).await?;
        assert_eq!(false, banned.is_poc_banned(&hotspot_pubkey));

        Ok(())
    }
}
