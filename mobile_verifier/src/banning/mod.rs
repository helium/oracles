use std::collections::HashSet;

use chrono::Utc;
use file_store::{
    file_upload::FileUpload,
    mobile_ban,
    traits::{FileSinkCommitStrategy, FileSinkRollTime},
    FileStore,
};
use helium_crypto::PublicKeyBinary;
use ingester::BanIngester;
use mobile_config::client::AuthorizationClient;
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

use crate::Settings;

pub mod db;
pub mod ingester;

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

    let ingestor = BanIngester::new(pool, auth_verifier, report_rx, verified_sink);

    Ok(TaskManager::builder()
        .add_task(verified_sink_server)
        .add_task(ingest_server)
        .add_task(ingestor)
        .build())
}

#[derive(Debug, Default)]
pub struct BannedRadios {
    banned: HashSet<PublicKeyBinary>,
    sp_banned: HashSet<PublicKeyBinary>,
}

impl BannedRadios {
    pub async fn new(pool: &PgPool, date_time: chrono::DateTime<Utc>) -> anyhow::Result<Self> {
        let banned = db::get_banned_radios(pool, date_time).await?;

        use helium_proto::services::poc_mobile::service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType;
        let poc_banned_radios = crate::sp_boosted_rewards_bans::db::get_banned_radios(
            pool,
            SpBoostedRewardsBannedRadioBanType::Poc,
            date_time,
        )
        .await?;

        Ok(BannedRadios {
            banned,
            sp_banned: poc_banned_radios.wifi,
        })
    }

    pub fn insert_sp_banned(&mut self, hotspot_pubkey: PublicKeyBinary) {
        self.sp_banned.insert(hotspot_pubkey);
    }

    pub fn is_sp_banned(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
        self.sp_banned.contains(hotspot_pubkey)
    }

    pub fn is_poc_banned(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
        self.banned.contains(hotspot_pubkey)
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
}
