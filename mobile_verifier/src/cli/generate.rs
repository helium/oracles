use crate::{
    reward_shares::{PocShares, TransferRewards},
    speedtests::EmptyDatabase,
    verifier::{VerifiedEpoch, Verifier},
    Settings,
};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use futures::TryStreamExt;
use helium_crypto::PublicKey;
use mobile_config::Client;
use serde_json::json;
use std::collections::HashMap;

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    start: NaiveDateTime,
    #[clap(long)]
    end: NaiveDateTime,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let Self { start, end } = self;

        let start = DateTime::from_utc(start, Utc);
        let end = DateTime::from_utc(end, Utc);

        tracing::info!("Verifying shares from the following time range: {start} to {end}");
        let epoch = start..end;

        let (config_client, config_client_handle) = Client::from_settings(&settings.config_client)?;
        let file_store = FileStore::from_settings(&settings.ingest).await?;
        let verifier = Verifier::new(config_client, file_store);

        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = verifier.verify_epoch(EmptyDatabase, &epoch).await?;

        let reward_shares = PocShares::aggregate(
            heartbeats.try_collect().await?,
            speedtests.try_collect().await?,
        )
        .await;

        let mut total_rewards = 0_u64;
        let mut owner_rewards = HashMap::<_, u64>::new();
        let transfer_rewards = TransferRewards::empty();
        for (reward, _) in reward_shares.into_rewards(&transfer_rewards, &epoch) {
            total_rewards += reward.amount;
            *owner_rewards
                .entry(PublicKey::try_from(reward.owner_key)?)
                .or_default() += reward.amount;
        }

        let rewards: Vec<_> = owner_rewards.into_iter().collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "rewards": rewards,
                "total_rewards": total_rewards,
            }))?
        );
        config_client_handle.abort();

        Ok(())
    }
}
