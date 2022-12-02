use crate::{reward_index, Error, Result, Settings};
use db_store::meta;
use file_store::{traits::TimestampDecode, FileInfo, FileStore, FileType};
use futures::{stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKey;
use helium_proto::{services::poc_mobile::RadioRewardShare, Message, RewardManifest};
use poc_metrics::record_duration;
use sqlx::{Pool, Postgres};
use std::{collections::HashMap, str::FromStr};
use tokio::time;

pub struct Indexer {
    pool: Pool<Postgres>,
    interval: time::Duration,
    verifier_store: FileStore,
}

impl Indexer {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(10).await?;
        Ok(Self {
            interval: settings.interval(),
            verifier_store: FileStore::from_settings(&settings.verifier).await?,
            pool,
        })
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting mobile index");

        let mut interval_timer = tokio::time::interval(self.interval);

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping mobile indexer");
                return Ok(());
            }

            tokio::select! {
                _ = shutdown.clone() => (),
                _ = interval_timer.tick() => {
                    record_duration!(
                        "reward_index_duration",
                        self.handle_rewards().await?
                    )
                }
            }
        }
    }

    async fn handle_rewards(&mut self) -> Result {
        tracing::info!("Checking for reward manifest");

        let last_reward_manifest: u64 = meta::fetch(&self.pool, "last_reward_manifest").await?;

        let next_manifest = self
            .verifier_store
            .list_all(
                FileType::RewardManifest,
                last_reward_manifest.to_timestamp_millis()?,
                None,
            )
            .await?;

        let Some(manifest_file) = next_manifest.first().cloned() else {
            tracing::info!("No new manifest found");
            return Ok(());
        };

        let Some(manifest_buff) = self.verifier_store.get(manifest_file.clone()).await?
            .next()
            .await else {
                tracing::error!("Empty manifest");
                return Ok(());
            };

        tracing::info!("Manifest found, indexing rewards");

        let manifest = RewardManifest::decode(manifest_buff.map_err(Error::from)?)?;
        let manifest_time = manifest.end_timestamp.to_timestamp()?;

        let reward_files = stream::iter(
            manifest
                .written_files
                .into_iter()
                .map(|file_name| FileInfo::from_str(&file_name)),
        )
        .boxed();

        let mut reward_shares = self.verifier_store.source_unordered(5, reward_files);

        let mut hotspot_rewards: HashMap<Vec<u8>, u64> = HashMap::new();

        while let Some(msg) = reward_shares.try_next().await? {
            let radio_reward_share = RadioRewardShare::decode(msg)?;
            *hotspot_rewards
                .entry(radio_reward_share.hotspot_key)
                .or_default() += radio_reward_share.amount;
        }

        // Begin a transaction to write all the hotspot rewards
        let mut txn = self.pool.begin().await?;

        for (address, amount) in hotspot_rewards {
            let pub_key = PublicKey::try_from(address)?;
            reward_index::insert(&mut txn, &pub_key, amount, &manifest_time).await?;
        }

        // Include the last reward manifest in the transaction to avoid failures
        // updating the last handled manifest
        meta::store(
            &mut txn,
            "last_reward_manifest",
            manifest_file.timestamp.timestamp_millis(),
        )
        .await?;

        txn.commit().await?;

        Ok(())
    }
}
