use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
use file_store::{mobile_ban, FileStore};
use helium_crypto::PublicKeyBinary;
use humantime_serde::re::humantime;
use serde::Deserialize;
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

pub mod db;
pub mod ingester;
pub mod purger;

pub use db::get_banned_radios;

#[derive(Debug, Deserialize)]
pub struct BanSettings {
    /// Where do we look in s3 for ban files
    pub input_bucket: file_store::Settings,
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
    settings: &BanSettings,
) -> anyhow::Result<impl ManagedTask> {
    let verifier_file_store = FileStore::from_settings(&settings.input_bucket).await?;

    let (ban_report_rx, ban_report_server) =
        mobile_ban::verified_report_source(pool.clone(), verifier_file_store, settings.start_after)
            .await?;

    let ingester = ingester::BanIngestor::new(pool.clone(), ban_report_rx);
    let purger = purger::BanPurger::new(pool, settings.purge_interval);

    Ok(TaskManager::builder()
        .add_task(ban_report_server)
        .add_task(ingester)
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
