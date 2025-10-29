use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
use file_store::file_source;
use file_store_oracles::FileType;
use helium_crypto::PublicKeyBinary;
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

pub mod db;
pub mod ingestor;
pub mod purger;

pub use db::get_banned_radios;
pub use ingestor::handle_verified_ban_report;

pub const BAN_CLEANUP_DAYS: i64 = 7;

#[derive(Debug, Deserialize, Serialize)]
pub struct BanSettings {
    /// Where do we look in s3 for ban files
    pub input_bucket: String,
    /// How often to purge expired bans
    #[serde(with = "humantime_serde", default = "default_purge_interval")]
    pub purge_interval: Duration,
    /// How far back should we be reading ban files
    #[serde(default = "default_ingest_start_after")]
    pub start_after: DateTime<Utc>,
}

fn default_purge_interval() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_ingest_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

pub async fn create_managed_task(
    pool: PgPool,
    client: file_store::Client,
    settings: &BanSettings,
) -> anyhow::Result<impl ManagedTask> {
    let (ban_report_rx, ban_report_server) = file_source::continuous_source()
        .state(pool.clone())
        .file_store(client, settings.input_bucket.clone())
        .lookback_start_after(settings.start_after)
        .prefix(FileType::VerifiedMobileBanReport.to_string())
        .create()
        .await?;

    let ingestor = ingestor::BanIngestor::new(pool.clone(), ban_report_rx);
    let purger = purger::BanPurger::new(pool, settings.purge_interval);

    Ok(TaskManager::builder()
        .add_task(ban_report_server)
        .add_task(ingestor)
        .add_task(purger)
        .build())
}

#[derive(Debug, Default)]
pub struct BannedRadios {
    banned: HashSet<PublicKeyBinary>,
}

impl BannedRadios {
    pub fn contains(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
        self.banned.contains(hotspot_pubkey)
    }
}
