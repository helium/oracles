use std::collections::HashSet;

use chrono::{DateTime, Utc};
use file_store::file_upload::FileUpload;
use file_store::BucketClient;
use helium_crypto::PublicKeyBinary;
use ingestor::BanIngestor;
use mobile_config::client::AuthorizationClient;
use sqlx::{PgConnection, PgPool};
use task_manager::ManagedTask;

use crate::Settings;

pub mod db;
pub mod ingestor;

pub const BAN_CLEANUP_DAYS: i64 = 7;

pub async fn create_managed_task(
    pool: PgPool,
    file_upload: FileUpload,
    bucket_client: BucketClient,
    auth_verifier: AuthorizationClient,
    settings: &Settings,
) -> anyhow::Result<impl ManagedTask> {
    BanIngestor::create_managed_task(pool, file_upload, bucket_client, auth_verifier, settings)
        .await
}

#[derive(Debug, Default)]
pub struct BannedRadios {
    banned: HashSet<PublicKeyBinary>,
}

impl BannedRadios {
    pub async fn new(pool: &PgPool, before: chrono::DateTime<Utc>) -> anyhow::Result<Self> {
        let banned = db::get_banned_radios(pool, before).await?;
        Ok(BannedRadios { banned })
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
    // DataTransferSessions not being output for this servive to reward them.
    //
    // fn is_data_banned(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
    //     unimplimented!("do not provide this method")
    // }
}

pub async fn clear_bans(conn: &mut PgConnection, before: DateTime<Utc>) -> anyhow::Result<()> {
    db::clear_bans(conn, before).await?;
    Ok(())
}
