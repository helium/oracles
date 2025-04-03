use std::collections::HashSet;

use chrono::{DateTime, Utc};
use file_store::{file_sink::FileSinkClient, file_upload::FileUpload, FileStore};
use helium_crypto::PublicKeyBinary;
use ingestor::BanIngestor;
use mobile_config::client::AuthorizationClient;
use sp_boosted_rewards_bans::ServiceProviderBoostedRewardsBanIngestor;
use sqlx::{PgConnection, PgPool};
use task_manager::{ManagedTask, TaskManager};

mod proto {
    pub use helium_proto::services::poc_mobile::SeniorityUpdate;
}

use crate::Settings;

pub mod db;
pub mod ingestor;
pub mod sp_boosted_rewards_bans;

pub const BAN_CLEANUP_DAYS: i64 = 7;

pub async fn create_managed_task(
    pool: PgPool,
    file_upload: FileUpload,
    file_store: FileStore,
    auth_verifier: AuthorizationClient,
    settings: &Settings,
    seniority_update_sink: FileSinkClient<proto::SeniorityUpdate>,
) -> anyhow::Result<impl ManagedTask> {
    let ban_ingestor = BanIngestor::create_managed_task(
        pool.clone(),
        file_upload.clone(),
        file_store.clone(),
        auth_verifier.clone(),
        settings,
    )
    .await?;

    let sp_ban_ingestor = ServiceProviderBoostedRewardsBanIngestor::create_managed_task(
        pool,
        file_upload,
        file_store,
        auth_verifier,
        settings,
        seniority_update_sink,
    )
    .await?;

    Ok(TaskManager::builder()
        .add_task(ban_ingestor)
        .add_task(sp_ban_ingestor)
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
        let poc_banned_radios = sp_boosted_rewards_bans::db::get_banned_radios(
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

pub async fn clear_bans(conn: &mut PgConnection, before: DateTime<Utc>) -> anyhow::Result<()> {
    db::clear_bans(conn, before).await?;
    sp_boosted_rewards_bans::clear_bans(conn, before).await?;

    Ok(())
}
